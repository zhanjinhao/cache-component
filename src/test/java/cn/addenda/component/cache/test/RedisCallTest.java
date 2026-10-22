package cn.addenda.component.cache.test;

import cn.addenda.component.cache.CacheIllegalArgumentException;
import cn.addenda.component.cache.CacheNetworkException;
import cn.addenda.component.cache.CacheUnsupportedOperationException;
import cn.addenda.component.cache.LettuceRedisKVCache;
import cn.addenda.component.cache.RedissonRedisKVCache;
import cn.addenda.component.cache.StringRedisTemplateRedisKVCache;
import cn.addenda.component.cache.TestRedisCallAccess;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * {@code RedisCall}（包级可见）的翻译口径：四个 redis 客户端的异常在这里收敛成
 * {@link CacheUnsupportedOperationException}（服务端不支持该命令）、
 * {@link CacheIllegalArgumentException}（组件不允许该 TTL 穿透到存储层；redis 侧是下发后
 * 把存储层的 {@code invalid expire time} 归一成这个口径）与
 * {@link CacheNetworkException}（连不上、超时），其它异常原样抛出。
 * <p>
 * 分两段：前面的用例<b>离线</b>跑，用抓下来的原文钉住各客户端的异常形状；后面的
 * {@code testReal...} 用例<b>需要真实服务端</b>，把命令真发给客户端，断言它们抛出来的东西
 * 确实被翻译了。后者才是"换客户端 / 换服务端版本时会不会失效"的哨兵 ——
 * 前者只是把文案抄了一遍，文案变了它不会响。
 * <p>
 * 本组件用到的四个客户端里，lettuce 的两个实现（standalone / cluster）抛的是同一套异常，
 * 因此真实链路这边用 standalone 代表。
 * <p>
 * 上游触发条件：{@code GETDEL} 需要 Redis ≥ 6.2；TTL 64 位溢出由
 * {@code ExpiredKVCache.ttlMillis} 原样下发 {@link Long#MAX_VALUE} 触发。
 *
 * @author addenda
 */
@Slf4j
public class RedisCallTest {

  /**
   * 服务端一定不认识的命令名。真实链路那几条用例用它触发"unknown command"。
   */
  private static final String BOGUS_COMMAND = "CACHEHELPERBOGUSCMD";

  /**
   * 一定触发服务端拒绝的 TTL：{@code ttlMillis} 会原样下发 {@link Long#MAX_VALUE} 毫秒。
   */
  private static final long OVERFLOWING_TTL_MILLIS = Long.MAX_VALUE;

  // ------------------------------------------------------------------
  // 离线：unknown command
  // ------------------------------------------------------------------

  /**
   * 直接发一条不存在的命令（lettuce 抛出，反引号是服务端的原文）。
   */
  @Test
  public void testUnknownCommandIsTranslated() {
    RedisCommandExecutionException client = new RedisCommandExecutionException(
            "ERR unknown command `GETDEL`, with args beginning with: ");

    CacheUnsupportedOperationException translated = assertUnsupported(client);
    Assert.assertTrue("消息里要说清是哪个命令：" + translated.getMessage(),
            translated.getMessage().contains("GETDEL"));
  }

  /**
   * 命令写在脚本里时回文完全不同（这份来自 redis 7 的 Lua 报错），同样要认出来。
   */
  @Test
  public void testScriptStyleExceptionIsTranslated() {
    RedisException client = new RedisException(
            "ERR Error running script (call to f_5e5bab6f): @user_script:1: "
                    + "@user_script: 1: Unknown Redis command called from Lua script");

    assertUnsupported(client);
  }

  /**
   * spring-data-redis 在外面又包了一层，原始回文在 cause 上 —— 必须逐层找。
   */
  @Test
  public void testWrappedExceptionIsTranslated() {
    RedisSystemException client = new RedisSystemException(
            "Error in execution; nested exception is io.lettuce.core.RedisCommandExecutionException:"
                    + " ERR unknown command `GETDEL`, with args beginning with: ",
            new RedisCommandExecutionException("ERR unknown command `GETDEL`, with args beginning with: "));

    assertUnsupported(client);
  }

  // ------------------------------------------------------------------
  // 离线：invalid expire time
  // ------------------------------------------------------------------

  /**
   * lettuce：{@code PSETEX} 被存储层拒绝时的原文。这是"参数已经下发"的那条路径 ——
   * 组件把它归一成自己的拒绝，消息里要带上 redis-cli 形式的命令与参数。
   */
  @Test
  public void testLettuceInvalidExpireTimeIsTranslated() {
    RedisCommandExecutionException client =
            new RedisCommandExecutionException("ERR invalid expire time in psetex");

    CacheIllegalArgumentException translated = assertInvalidTtl(client,
            TestRedisCallAccess.command("PSETEX", "user:1", OVERFLOWING_TTL_MILLIS));
    Assert.assertTrue("消息里要带上换算后的毫秒值：" + translated.getMessage(),
            translated.getMessage().contains(String.valueOf(OVERFLOWING_TTL_MILLIS)));
  }

  /**
   * redisson：同一句回文，但异常消息后面还挂着连接信息。
   */
  @Test
  public void testRedissonInvalidExpireTimeIsTranslated() {
    RedisException client = new RedisException(
            "ERR invalid expire time in psetex. channel: [id: 0x6257f99f, L:/192.168.31.86:12943"
                    + " - R:47.106.87.163/47.106.87.163:30002]");

    assertInvalidTtl(client, TestRedisCallAccess.command("PSETEX", "user:1", OVERFLOWING_TTL_MILLIS));
  }

  /**
   * spring-data-redis：它在 lettuce 的异常外面再包一层，原始回文在 cause 上 —— 必须逐层找。
   */
  @Test
  public void testWrappedInvalidExpireTimeIsTranslated() {
    RedisSystemException client = new RedisSystemException(
            "Error in execution; nested exception is io.lettuce.core.RedisCommandExecutionException:"
                    + " ERR invalid expire time in psetex",
            new RedisCommandExecutionException("ERR invalid expire time in psetex"));

    assertInvalidTtl(client, TestRedisCallAccess.command("PSETEX", "user:1", OVERFLOWING_TTL_MILLIS));
  }

  /**
   * <b>不带 TTL 的调用不能打印 TTL 数值。</b>这类调用根本没有 TTL 参数，消息里拼出
   * {@code "TTL 换算后是 null"} 只会误导调用方 —— 它该看到的是"组件拒绝本次调用"，
   * 而不是一个不存在的 TTL。
   */
  @Test
  public void testInvalidExpireTimeWithoutTtlDoesNotPrintNullTtl() {
    RedisCommandExecutionException client =
            new RedisCommandExecutionException("ERR invalid expire time in set");

    try {
      TestRedisCallAccess.call(TestRedisCallAccess.command("SET", "user:1", "v"), () -> {
        throw client;
      });
      Assert.fail("应该被翻译成 CacheIllegalArgumentException");
    } catch (CacheIllegalArgumentException e) {
      Assert.assertFalse("没有 TTL 参数的调用不该出现 null TTL：" + e.getMessage(),
              e.getMessage().contains("null"));
      Assert.assertFalse("没有 TTL 参数的调用不该给出换算后的 TTL：" + e.getMessage(),
              e.getMessage().contains("resolves to"));
      Assert.assertTrue("仍要说清是哪个命令：" + e.getMessage(), e.getMessage().contains("SET"));
      Assert.assertSame("原始异常要挂在 cause 上", client, e.getCause());
    }
  }

  // ------------------------------------------------------------------
  // 离线：网络 / 超时
  // ------------------------------------------------------------------

  /**
   * 网络故障按异常类与文案两种方式识别：lettuce 的 {@link RedisConnectionException} /
   * {@link RedisCommandTimeoutException}，以及 spring 包装后的
   * {@link RedisConnectionFailureException}。
   */
  @Test
  public void testNetworkFailuresAreTranslated() {
    RuntimeException[] failures = {
            new RedisConnectionException("Unable to connect to 127.0.0.1:6379"),
            new RedisCommandTimeoutException("Command timed out"),
            new RedisConnectionFailureException("Unable to connect to Redis"),
            new RedisSystemException("Unable to connect to 127.0.0.1:6379", new RuntimeException("refused")),
    };
    for (RuntimeException failure : failures) {
      try {
        TestRedisCallAccess.call(TestRedisCallAccess.command("GET", "user:1"), () -> {
          throw failure;
        });
        Assert.fail("应该被翻译成 CacheNetworkException：" + failure.getMessage());
      } catch (CacheNetworkException e) {
        Assert.assertSame("原始异常要挂在 cause 上", failure, e.getCause());
        Assert.assertTrue("消息里要带上命令与参数：" + e.getMessage(), e.getMessage().contains("GET user:1"));
      }
    }
  }

  // ------------------------------------------------------------------
  // 离线：其它异常原样抛出
  // ------------------------------------------------------------------

  /**
   * <b>只翻译上面三类</b>（未知命令 / 非法 TTL / 网络超时）：其它错误原样抛出，连实例都不换。
   * <p>
   * 最后两条是干扰项：{@code unknown subcommand} 与 {@code invalid command name} 文字上很接近，
   * 意思却分别是"调用方用错了子命令"和"命令名非法"，不能混进来。
   */
  @Test
  public void testOtherExceptionsAreRethrownUntouched() {
    RuntimeException[] others = {
            new RedisCommandExecutionException("WRONGTYPE Operation against a key holding the wrong kind of value"),
            new RedisException("ERR unknown subcommand for this key"),
            new RedisCommandExecutionException("ERR invalid command name"),
    };
    for (RuntimeException other : others) {
      try {
        TestRedisCallAccess.call(TestRedisCallAccess.command("GETDEL", "user:1"), () -> {
          throw other;
        });
        Assert.fail("应该原样抛出：" + other.getMessage());
      } catch (CacheUnsupportedOperationException e) {
        Assert.fail("不该被当成\"不支持\"：" + other.getMessage());
      } catch (CacheNetworkException e) {
        Assert.fail("不该被当成\"网络故障\"：" + other.getMessage());
      } catch (RuntimeException e) {
        Assert.assertSame("要原样抛出，不换实例", other, e);
      }
    }
  }

  /**
   * <b>命令串必须能直接粘到 redis-cli 执行。</b>普通 token 原样输出；含空格、引号、反斜杠、
   * 换行的参数按 redis-cli 的双引号规则转义，空串输出为 {@code ""}。
   */
  @Test
  public void testCommandIsRedisCliReady() {
    Assert.assertEquals("GET user:1", TestRedisCallAccess.command("GET", "user:1"));
    Assert.assertEquals("PSETEX user:1 60000",
            TestRedisCallAccess.command("PSETEX", "user:1", 60000L));
    Assert.assertEquals("SET user:1 v NX PX 60000",
            TestRedisCallAccess.command("SET", "user:1", "v", "NX", "PX", 60000L));
    Assert.assertEquals("SET \"user 1\" \"hello world\" NX",
            TestRedisCallAccess.command("SET", "user 1", "hello world", "NX"));
    Assert.assertEquals("SET \"a\\\"b\" \"c\\\\d\"",
            TestRedisCallAccess.command("SET", "a\"b", "c\\d"));
    Assert.assertEquals("SET \"line1\\nline2\" \"\"",
            TestRedisCallAccess.command("SET", "line1\nline2", ""));
  }

  /**
   * <b>拼命令串本身不能抛异常。</b>它在真正的 redis 调用之前求值，一旦抛出，异常会落在
   * {@code RedisCall} 的翻译逻辑之外，并让后面的 redis 调用根本不发生。格式化失败时
   * 降级为 {@code UNKNOWN Command}，让真正的调用照常发生。
   */
  @Test
  public void testCommandDegradesToUnknownWhenArgumentCannotBeFormatted() {
    Object broken = new Object() {
      @Override
      public String toString() {
        throw new IllegalStateException("toString failed");
      }
    };
    Assert.assertEquals("UNKNOWN Command",
            TestRedisCallAccess.command("SET", "user:1", broken));
    Assert.assertEquals("UNKNOWN Command",
            TestRedisCallAccess.command("SET", (Object[]) null));
  }

  /**
   * 正常返回时什么都不做。
   */
  @Test
  public void testValueIsReturnedAsIs() {
    Assert.assertEquals("v", TestRedisCallAccess.call(TestRedisCallAccess.command("GETDEL", "user:1"), () -> "v"));
    Assert.assertNull(TestRedisCallAccess.call(TestRedisCallAccess.command("GETDEL", "user:1"), () -> null));
    Assert.assertEquals("v",
            TestRedisCallAccess.callWithTtl(
                    TestRedisCallAccess.command("PSETEX", "user:1", 1000L), 1000L, () -> "v"));
    Assert.assertNull(TestRedisCallAccess.callWithTtl(
            TestRedisCallAccess.command("PSETEX", "user:1", 1000L), 1000L, () -> null));
  }

  // ------------------------------------------------------------------
  // 真实链路：需要服务端
  // 离线那几条抄的是"当时"的文案，文案变了它们不会响；下面这几条会。
  // ------------------------------------------------------------------

  /**
   * lettuce：直接让服务端不认识一条命令。
   */
  @Test
  public void testRealLettuceUnknownCommandIsTranslated() throws Exception {
    RedisClient redisClient = RedisClientBaseTest.redisClient();
    StatefulRedisConnection<String, String> connection = redisClient.connect();
    try {
      assertTranslatedByRealClient(() -> connection.sync()
              .eval("return redis.call('" + BOGUS_COMMAND + "')", ScriptOutputType.VALUE));
    } finally {
      connection.close();
      redisClient.shutdown();
    }
  }

  /**
   * spring-data-redis：它会在 lettuce 的异常外面再包一层。
   */
  @Test
  public void testRealSpringUnknownCommandIsTranslated() throws Exception {
    LettuceConnectionFactory factory = StringRedisTemplateBaseTest.lettuceConnectionFactory();
    try {
      StringRedisTemplate template = StringRedisTemplateBaseTest.stringRedisTemplate(factory);
      assertTranslatedByRealClient(
              () -> template.execute((RedisCallback<Object>) c -> c.execute(BOGUS_COMMAND)));
    } finally {
      factory.destroy();
    }
  }

  /**
   * redisson：它把命令写在脚本里下发。
   */
  @Test
  public void testRealRedissonUnknownCommandIsTranslated() throws Exception {
    RedissonClient redissonClient = RedissonClientBaseTest.redissonClient();
    try {
      assertTranslatedByRealClient(() -> redissonClient.getScript().eval(RScript.Mode.READ_WRITE,
              "return redis.call('" + BOGUS_COMMAND + "')", RScript.ReturnType.VALUE));
    } finally {
      redissonClient.shutdown();
    }
  }

  /**
   * lettuce（standalone 代表两个 lettuce 实现，cluster 只差连接类型）。
   */
  @Test
  public void testRealLettuceInvalidExpireTimeIsTranslated() throws Exception {
    RedisClient redisClient = RedisClientBaseTest.redisClient();
    LettuceRedisKVCache cache = new LettuceRedisKVCache(redisClient);
    try {
      assertRealTtlRejected(() -> cache.set(uniqueKey(), "v", OVERFLOWING_TTL_MILLIS, TimeUnit.MILLISECONDS));
      assertRealTtlRejected(() -> cache.setnx(uniqueKey(), "v", OVERFLOWING_TTL_MILLIS, TimeUnit.MILLISECONDS));
    } finally {
      cache.close();
      redisClient.shutdown();
    }
  }

  /**
   * spring-data-redis：它会把 lettuce 的异常包一层。
   */
  @Test
  public void testRealSpringInvalidExpireTimeIsTranslated() throws Exception {
    LettuceConnectionFactory factory = StringRedisTemplateBaseTest.lettuceConnectionFactory();
    try {
      StringRedisTemplate template = StringRedisTemplateBaseTest.stringRedisTemplate(factory);
      StringRedisTemplateRedisKVCache cache = new StringRedisTemplateRedisKVCache(template);
      assertRealTtlRejected(() -> cache.set(uniqueKey(), "v", OVERFLOWING_TTL_MILLIS, TimeUnit.MILLISECONDS));
      assertRealTtlRejected(() -> cache.setnx(uniqueKey(), "v", OVERFLOWING_TTL_MILLIS, TimeUnit.MILLISECONDS));
    } finally {
      factory.destroy();
    }
  }

  /**
   * redisson。
   */
  @Test
  public void testRealRedissonInvalidExpireTimeIsTranslated() throws Exception {
    RedissonClient redissonClient = RedissonClientBaseTest.redissonClient();
    try {
      RedissonRedisKVCache cache = new RedissonRedisKVCache(redissonClient);
      assertRealTtlRejected(() -> cache.set(uniqueKey(), "v", OVERFLOWING_TTL_MILLIS, TimeUnit.MILLISECONDS));
      assertRealTtlRejected(() -> cache.setnx(uniqueKey(), "v", OVERFLOWING_TTL_MILLIS, TimeUnit.MILLISECONDS));
    } finally {
      redissonClient.shutdown();
    }
  }

  /**
   * 把一次真实的调用交给被测的翻译逻辑，断言它被翻译了。
   * <p>
   * 失败时把客户端原始异常打进日志 —— 换客户端、换服务端版本导致判定失效时，
   * 日志里就是需要照着改的那条新文案。
   */
  private void assertTranslatedByRealClient(Supplier<Object> realCall) {
    try {
      TestRedisCallAccess.call(BOGUS_COMMAND, realCall);
      Assert.fail("服务端不认识这条命令，应该被翻译成 CacheUnsupportedOperationException");
    } catch (CacheUnsupportedOperationException e) {
      Throwable cause = e.getCause();
      Assert.assertNotNull("原始异常要挂在 cause 上", cause);
      log.info("真实客户端异常已翻译：{} :: {}", cause.getClass().getName(), cause.getMessage());
    }
  }

  private void assertRealTtlRejected(Runnable realCall) {
    try {
      realCall.run();
      Assert.fail("组件不接受这个 TTL，应该被翻译成 CacheIllegalArgumentException");
    } catch (CacheIllegalArgumentException e) {
      Throwable cause = e.getCause();
      Assert.assertNotNull("原始异常要挂在 cause 上", cause);
      log.info("真实客户端异常已翻译：{} :: {}", cause.getClass().getName(), cause.getMessage());
    }
  }

  private CacheUnsupportedOperationException assertUnsupported(RuntimeException client) {
    return assertUnsupported(client, () -> TestRedisCallAccess.call(
            TestRedisCallAccess.command("GETDEL", "user:1"), () -> {
              throw client;
            }));
  }

  private CacheIllegalArgumentException assertInvalidTtl(RuntimeException client, String context) {
    try {
      TestRedisCallAccess.callWithTtl(context, OVERFLOWING_TTL_MILLIS, () -> {
        throw client;
      });
      Assert.fail("应该被翻译成 CacheIllegalArgumentException");
      return null;
    } catch (CacheIllegalArgumentException e) {
      Assert.assertSame("原始异常要挂在 cause 上", client, e.getCause());
      return e;
    }
  }

  private CacheUnsupportedOperationException assertUnsupported(RuntimeException client, Runnable call) {
    try {
      call.run();
      Assert.fail("应该被翻译成 CacheUnsupportedOperationException");
      return null;
    } catch (CacheUnsupportedOperationException e) {
      Assert.assertSame("原始异常要挂在 cause 上", client, e.getCause());
      return e;
    }
  }

  private String uniqueKey() {
    return "RedisCallTest:" + UUID.randomUUID().toString().replace("-", "");
  }

}
