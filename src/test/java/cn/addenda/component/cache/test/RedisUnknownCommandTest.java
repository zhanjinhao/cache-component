package cn.addenda.component.cache.test;

import cn.addenda.component.cache.LettuceRedisClusterKVCache;
import cn.addenda.component.cache.TestRedisUnknownCommandAccess;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.function.Supplier;

/**
 * {@code RedisUnknownCommand}（包级可见）的翻译口径。
 * <p>
 * 分两段：前面的用例<b>离线</b>跑，用抓下来的原文钉住三种形状的判定；后面的
 * {@code testReal...} 用例<b>需要真实服务端</b>，把同一条命令真发给三个客户端，断言它们
 * 抛出来的东西确实被翻译了。后者才是"换客户端 / 换服务端版本时会不会失效"的哨兵 ——
 * 前者只是把文案抄了一遍，文案哪天变了它不会响。
 * <p>
 * 本组件用到的四个客户端里，lettuce 的两个实现（standalone / cluster）抛的是同一套异常，
 * 因此真实链路这边用 standalone 代表；{@link LettuceRedisClusterKVCache} 与它只差连接类型。
 *
 * @author addenda
 */
@Slf4j
public class RedisUnknownCommandTest {

  /**
   * 服务端一定不认识的命令名。真实链路那几条用例用它触发"unknown command"。
   */
  private static final String BOGUS_COMMAND = "CACHEHELPERBOGUSCMD";

  /**
   * 直接发一条不存在的命令（lettuce 抛出，反引号是服务端的原文）。
   */
  @Test
  public void testUnknownCommandIsTranslated() {
    RedisCommandExecutionException client = new RedisCommandExecutionException(
            "ERR unknown command `GETDEL`, with args beginning with: ");

    assertTranslated(client);
  }

  /**
   * 命令写在脚本里时回文完全不同（这份来自 redis 7 的 Lua 报错），同样要认出来。
   */
  @Test
  public void testScriptStyleExceptionIsTranslated() {
    RedisException client = new RedisException(
            "ERR Error running script (call to f_5e5bab6f): @user_script:1: "
                    + "@user_script: 1: Unknown Redis command called from Lua script");

    assertTranslated(client);
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

    assertTranslated(client);
  }

  /**
   * <b>只翻译这一种</b>：其它错误原样抛出，连实例都不换 ——
   * 连不上、超时、类型错都不是"不支持"。
   * <p>
   * 第三条是 redis 的官方回文之一（子命令写错时给出），文字上很接近但意思完全不同，
   * 必须靠"成词"判定挡住 —— 它曾经真的被误判过。
   */
  @Test
  public void testOtherExceptionsAreRethrownUntouched() {
    RuntimeException[] others = {
            new RedisSystemException("Unable to connect to 127.0.0.1:6379", new RuntimeException("refused")),
            new RedisCommandExecutionException("WRONGTYPE Operation against a key holding the wrong kind of value"),
            new RedisException("ERR unknown subcommand for this key"),
    };
    for (RuntimeException other : others) {
      try {
        TestRedisUnknownCommandAccess.call("GETDEL", () -> {
          throw other;
        });
        Assert.fail("应该原样抛出：" + other.getMessage());
      } catch (UnsupportedOperationException e) {
        Assert.fail("不该被当成\"不支持\"：" + other.getMessage());
      } catch (RuntimeException e) {
        Assert.assertSame("要原样抛出，不换实例", other, e);
      }
    }
  }

  /**
   * 正常返回时什么都不做。
   */
  @Test
  public void testValueIsReturnedAsIs() {
    Assert.assertEquals("v", TestRedisUnknownCommandAccess.call("GETDEL", () -> "v"));
    Assert.assertNull(TestRedisUnknownCommandAccess.call("GETDEL", () -> null));
  }

  // ------------------------------------------------------------------
  // 真实链路：需要服务端
  // 离线那几条抄的是"当时"的文案，文案变了它们不会响；下面这三条会。
  // ------------------------------------------------------------------

  /**
   * lettuce：直接让服务端不认识一条命令。
   */
  @Test
  public void testRealLettuceExceptionIsTranslated() throws Exception {
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
  public void testRealSpringExceptionIsTranslated() throws Exception {
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
  public void testRealRedissonExceptionIsTranslated() throws Exception {
    RedissonClient redissonClient = RedissonClientBaseTest.redissonClient();
    try {
      assertTranslatedByRealClient(() -> redissonClient.getScript().eval(RScript.Mode.READ_WRITE,
              "return redis.call('" + BOGUS_COMMAND + "')", RScript.ReturnType.VALUE));
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
      TestRedisUnknownCommandAccess.call(BOGUS_COMMAND, realCall);
      Assert.fail("服务端不认识这条命令，应该被翻译成 UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      Throwable cause = e.getCause();
      Assert.assertNotNull("原始异常要挂在 cause 上", cause);
      log.info("真实客户端异常已翻译：{} :: {}", cause.getClass().getName(), cause.getMessage());
    }
  }

  /**
   * 断言它被翻译成了 {@link UnsupportedOperationException}，且原始异常挂在 cause 上。
   */
  private void assertTranslated(RuntimeException client) {
    try {
      TestRedisUnknownCommandAccess.call("GETDEL", () -> {
        throw client;
      });
      Assert.fail("应该被翻译成 UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      Assert.assertSame("原始异常要挂在 cause 上", client, e.getCause());
      Assert.assertTrue("消息里要说清是哪个命令：" + e.getMessage(),
              e.getMessage().contains("GETDEL"));
    }
  }

}
