package cn.addenda.component.cache.test;

import cn.addenda.component.base.util.SleepUtils;
import cn.addenda.component.cache.LettuceRedisKVCache;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author addenda
 */
public class LettuceRedisKVCacheTest {

  private RedisClient redisClient;

  private LettuceRedisKVCache kvCache;

  @Before
  @SneakyThrows
  public void before() {
    redisClient = RedisClientBaseTest.redisClient();
    kvCache = new LettuceRedisKVCache(redisClient);
  }

  @After
  public void after() {
    kvCache.close();
    if (redisClient != null) {
      redisClient.shutdown();
    }
  }

  /**
   * 每个用例使用独立的key，避免共用redis实例时用例之间相互影响
   */
  private String uniqueKey() {
    return "LettuceRedisKVCacheTest:" + UUID.randomUUID().toString().replace("-", "");
  }

  @Test
  public void testSetAndGet() {
    String key = uniqueKey();
    kvCache.set(key, "v1");
    Assert.assertEquals("v1", kvCache.get(key));

    kvCache.set(key, "v2");
    Assert.assertEquals("v2", kvCache.get(key));
  }

  @Test
  public void testGetAbsentKey() {
    Assert.assertNull(kvCache.get(uniqueKey()));
  }

  @Test
  public void testSetWithoutTimeoutNeverExpires() {
    String key = uniqueKey();
    kvCache.set(key, "v");
    Assert.assertNotNull(kvCache.get(key));

    SleepUtils.sleep(TimeUnit.MILLISECONDS, 200);
    Assert.assertEquals("v", kvCache.get(key));
  }

  @Test
  public void testSetWithTimeoutExpires() {
    String key = uniqueKey();
    kvCache.set(key, "v", 1, TimeUnit.SECONDS);
    Assert.assertEquals("v", kvCache.get(key));

    SleepUtils.sleep(TimeUnit.MILLISECONDS, 1200);
    Assert.assertNull(kvCache.get(key));
    Assert.assertFalse(kvCache.containsKey(key));
  }

  /**
   * lettuce 的 {@code setex} 只支持整秒，实现里用的是 {@code psetex}。
   * 这里用 <b>1500ms</b>（非整秒）验证 ttl 的毫秒精度：若被按秒处理，
   * 它会变成 1 秒或 2 秒，下面两个时间点的断言就会对不上。
   */
  @Test
  public void testSetWithMillisecondPrecisionTimeout() {
    String key = uniqueKey();
    kvCache.set(key, "v", 1500, TimeUnit.MILLISECONDS);

    SleepUtils.sleep(TimeUnit.MILLISECONDS, 700);
    Assert.assertEquals("v", kvCache.get(key));

    SleepUtils.sleep(TimeUnit.MILLISECONDS, 1800);
    Assert.assertNull(kvCache.get(key));
  }

  /**
   * redis不接受非正的过期时间，实现里会把它们转换成1ms，而不是抛出异常
   */
  @Test
  public void testSetWithNonPositiveTimeout() {
    String zeroKey = uniqueKey();
    kvCache.set(zeroKey, "v", 0, TimeUnit.MILLISECONDS);

    String negativeKey = uniqueKey();
    kvCache.set(negativeKey, "v", -1, TimeUnit.MILLISECONDS);

    SleepUtils.sleep(TimeUnit.MILLISECONDS, 100);
    Assert.assertNull(kvCache.get(zeroKey));
    Assert.assertNull(kvCache.get(negativeKey));
  }

  @Test
  public void testContainsKey() {
    String key = uniqueKey();
    Assert.assertFalse(kvCache.containsKey(key));

    kvCache.set(key, "v");
    Assert.assertTrue(kvCache.containsKey(key));
  }

  @Test
  public void testDelete() {
    String key = uniqueKey();
    kvCache.set(key, "v");

    Assert.assertTrue(kvCache.delete(key));
    Assert.assertNull(kvCache.get(key));
    Assert.assertFalse(kvCache.containsKey(key));
  }

  @Test
  public void testDeleteAbsentKey() {
    Assert.assertFalse(kvCache.delete(uniqueKey()));
  }

  /**
   * set(k, null) 等价于删除 —— redis 里没有 java null 值。
   * <p>
   * 不对齐的话 lettuce 会把 null 编码成<b>空串</b>，get 返回 ""，
   * 等于把业务写进去的 null 静默换成了另一个值。Redisson 和内存实现都不会这样。
   */
  @Test
  public void testSetNullMeansDelete() {
    String key = uniqueKey();
    kvCache.set(key, "v");
    Assert.assertTrue(kvCache.containsKey(key));

    kvCache.set(key, null);
    Assert.assertNull("写 null 之后 get 应该返回 null", kvCache.get(key));
    Assert.assertFalse("写 null 等价于删除", kvCache.containsKey(key));
  }

  /**
   * 带 ttl 的重载行为一致：写 null 也当删除
   */
  @Test
  public void testSetNullWithTimeoutMeansDelete() {
    String key = uniqueKey();
    kvCache.set(key, "v", 1, TimeUnit.MINUTES);
    Assert.assertTrue(kvCache.containsKey(key));

    kvCache.set(key, null, 1, TimeUnit.MINUTES);
    Assert.assertNull(kvCache.get(key));
    Assert.assertFalse(kvCache.containsKey(key));
  }

  @Test
  public void testRemove() {
    String key = uniqueKey();
    kvCache.set(key, "v");

    Assert.assertEquals("v", kvCache.remove(key));
    Assert.assertNull(kvCache.get(key));
  }

  /**
   * computeIfAbsent 明确不支持：它是原子的 check-then-act，redis 表达不了 ——
   * get 与 set 两次往返之间必然有窗口，mappingFunction 又是本地代码、服务端不参与求值。
   * <p>
   * 与 {@link #testSize()} 一样，宁可抛 {@link UnsupportedOperationException}，
   * 也不给一个并发下会重复计算、互相覆盖的版本。两个重载的行为一致，
   * 完整覆盖见 {@code ExpiredKVCacheContractTest#testComputeIfAbsentNotSupported}。
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testComputeIfAbsentUnsupported() {
    kvCache.computeIfAbsent(uniqueKey(), k -> "v", 1, TimeUnit.MINUTES);
  }

  /**
   * capacity() 与 {@link #testSize()} 同口径：redis 答不上来（容量由服务端的
   * {@code maxmemory-policy} 全局决定），就抛异常，而不是返回一个编造的
   * {@code Long.MAX_VALUE} 让 {@code size() <= capacity()} 看起来成立。
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testCapacityUnsupported() {
    kvCache.capacity();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testSize() {
    kvCache.size();
  }

  /**
   * 外部传入的连接，close时不应该被关闭
   */
  @Test
  public void testExternalConnectionNotClosed() {
    StatefulRedisConnection<String, String> connection = redisClient.connect();
    try {
      LettuceRedisKVCache externalKvCache = new LettuceRedisKVCache(connection);
      externalKvCache.set(uniqueKey(), "v");
      externalKvCache.close();
      Assert.assertTrue(connection.isOpen());
    } finally {
      connection.close();
    }
  }

}
