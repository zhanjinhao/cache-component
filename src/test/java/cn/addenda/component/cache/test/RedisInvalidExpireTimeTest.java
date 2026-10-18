package cn.addenda.component.cache.test;

import cn.addenda.component.cache.LettuceRedisKVCache;
import cn.addenda.component.cache.RedissonRedisKVCache;
import cn.addenda.component.cache.StringRedisTemplateRedisKVCache;
import cn.addenda.component.cache.TestRedisInvalidExpireTimeAccess;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * {@code RedisInvalidExpireTime}（包级可见）的翻译口径。
 * <p>
 * 分两段：前面的用例<b>离线</b>跑，用真实服务端抓下来的回文钉住三种客户端的异常形状；
 * 后面的 {@code testReal...} 用例<b>需要真实服务端</b>，直接打四个 redis 实现的
 * {@code set(k, v, ttl)} / {@code setnx(k, v, ttl)}。后者才是"换客户端 / 换服务端版本时
 * 会不会失效"的哨兵 —— 前者只是把文案抄了一遍，文案变了它不会响。
 * <p>
 * 上游触发条件只有一个：服务端算 {@code 当前时刻 + TTL} 时 64 位溢出。非正 TTL 不会走到这里，
 * {@code ExpiredKVCache.ttlMillis} 已把它们取整为 1ms。
 *
 * @author addenda
 */
@Slf4j
public class RedisInvalidExpireTimeTest {

  /**
   * 一定触发服务端拒绝的 TTL：{@code ttlMillis} 会原样下发 {@link Long#MAX_VALUE} 毫秒。
   */
  private static final long OVERFLOWING_TTL_MILLIS = Long.MAX_VALUE;

  /**
   * lettuce：{@code PSETEX} 被服务端拒绝时的原文。
   */
  @Test
  public void testLettuceStyleExceptionIsTranslated() {
    RedisCommandExecutionException client =
            new RedisCommandExecutionException("ERR invalid expire time in psetex");

    IllegalArgumentException translated = assertTranslated(client);
    Assert.assertTrue("消息里要带上公开方法名：" + translated.getMessage(),
            translated.getMessage().contains("set(key, value, timeout, unit)"));
    Assert.assertTrue("消息里要带上换算后的毫秒值：" + translated.getMessage(),
            translated.getMessage().contains(String.valueOf(OVERFLOWING_TTL_MILLIS)));
  }

  /**
   * redisson：同一句回文，但异常消息后面还挂着连接信息。
   */
  @Test
  public void testRedissonStyleExceptionIsTranslated() {
    RedisException client = new RedisException(
            "ERR invalid expire time in psetex. channel: [id: 0x6257f99f, L:/192.168.31.86:12943"
                    + " - R:47.106.87.163/47.106.87.163:30002]");

    assertTranslated(client);
  }

  /**
   * spring-data-redis：它在 lettuce 的异常外面再包一层，原始回文在 cause 上 —— 必须逐层找。
   */
  @Test
  public void testWrappedExceptionIsTranslated() {
    RedisSystemException client = new RedisSystemException(
            "Error in execution; nested exception is io.lettuce.core.RedisCommandExecutionException:"
                    + " ERR invalid expire time in psetex",
            new RedisCommandExecutionException("ERR invalid expire time in psetex"));

    assertTranslated(client);
  }

  /**
   * <b>只翻译这一种</b>：其它错误原样抛出，连实例都不换。
   * <p>
   * 最后一条是 {@code invalid} 家族的干扰项：只按单词判定会把它误吞，
   * 必须靠"invalid expire time"整句挡住。
   */
  @Test
  public void testOtherExceptionsAreRethrownUntouched() {
    RuntimeException[] others = {
            new RedisSystemException("Unable to connect to 127.0.0.1:6379", new RuntimeException("refused")),
            new RedisCommandExecutionException("WRONGTYPE Operation against a key holding the wrong kind of value"),
            new RedisCommandExecutionException("ERR unknown command `GETDEL`, with args beginning with: "),
            new RedisCommandExecutionException("ERR invalid command name"),
    };
    for (RuntimeException other : others) {
      try {
        TestRedisInvalidExpireTimeAccess.call("set(k, v, ttl)", OVERFLOWING_TTL_MILLIS, () -> {
          throw other;
        });
        Assert.fail("应该原样抛出：" + other.getMessage());
      } catch (IllegalArgumentException e) {
        Assert.fail("不该被当成\"TTL 溢出\"：" + other.getMessage());
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
    Assert.assertEquals("v", TestRedisInvalidExpireTimeAccess.call("set(k, v, ttl)", 1000L, () -> "v"));
    Assert.assertNull(TestRedisInvalidExpireTimeAccess.call("set(k, v, ttl)", 1000L, () -> null));
  }

  // ------------------------------------------------------------------
  // 真实链路：需要服务端
  // 离线那几条抄的是"当时"的文案，文案变了它们不会响；下面这几条会。
  // ------------------------------------------------------------------

  /**
   * lettuce（standalone 代表两个 lettuce 实现，cluster 只差连接类型）。
   */
  @Test
  public void testRealLettuceInvalidExpireTimeIsTranslated() throws Exception {
    RedisClient redisClient = RedisClientBaseTest.redisClient();
    LettuceRedisKVCache cache = new LettuceRedisKVCache(redisClient);
    try {
      assertRealRejected(() -> cache.set(uniqueKey(), "v", OVERFLOWING_TTL_MILLIS, TimeUnit.MILLISECONDS));
      assertRealRejected(() -> cache.setnx(uniqueKey(), "v", OVERFLOWING_TTL_MILLIS, TimeUnit.MILLISECONDS));
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
      assertRealRejected(() -> cache.set(uniqueKey(), "v", OVERFLOWING_TTL_MILLIS, TimeUnit.MILLISECONDS));
      assertRealRejected(() -> cache.setnx(uniqueKey(), "v", OVERFLOWING_TTL_MILLIS, TimeUnit.MILLISECONDS));
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
      assertRealRejected(() -> cache.set(uniqueKey(), "v", OVERFLOWING_TTL_MILLIS, TimeUnit.MILLISECONDS));
      assertRealRejected(() -> cache.setnx(uniqueKey(), "v", OVERFLOWING_TTL_MILLIS, TimeUnit.MILLISECONDS));
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
  private void assertRealRejected(Runnable realCall) {
    try {
      realCall.run();
      Assert.fail("服务端不接受这个 TTL，应该被翻译成 IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Throwable cause = e.getCause();
      Assert.assertNotNull("原始异常要挂在 cause 上", cause);
      log.info("真实客户端异常已翻译：{} :: {}", cause.getClass().getName(), cause.getMessage());
    }
  }

  /**
   * 断言它被翻译成了 {@link IllegalArgumentException}，且原始异常挂在 cause 上。
   */
  private IllegalArgumentException assertTranslated(RuntimeException client) {
    try {
      TestRedisInvalidExpireTimeAccess.call("set(key, value, timeout, unit)", OVERFLOWING_TTL_MILLIS,
              () -> {
                throw client;
              });
      Assert.fail("应该被翻译成 IllegalArgumentException");
      return null;
    } catch (IllegalArgumentException e) {
      Assert.assertSame("原始异常要挂在 cause 上", client, e.getCause());
      return e;
    }
  }

  private String uniqueKey() {
    return "RedisInvalidExpireTimeTest:" + UUID.randomUUID().toString().replace("-", "");
  }

}
