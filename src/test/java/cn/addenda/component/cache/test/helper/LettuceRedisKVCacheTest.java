package cn.addenda.component.cache.test.helper;

import cn.addenda.component.base.util.SleepUtils;
import cn.addenda.component.cache.helper.LettuceRedisKVCache;
import cn.addenda.component.cache.test.RedisClientBaseTest;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    kvCache.set(key, "v", 200, TimeUnit.MILLISECONDS);
    Assert.assertEquals("v", kvCache.get(key));

    SleepUtils.sleep(TimeUnit.MILLISECONDS, 500);
    Assert.assertNull(kvCache.get(key));
    Assert.assertFalse(kvCache.containsKey(key));
  }

  /**
   * setex只支持秒级ttl，实现里使用的是psetex。这里验证亚秒级的ttl是精确生效的。
   */
  @Test
  public void testSetWithSubSecondTimeout() {
    String key = uniqueKey();
    kvCache.set(key, "v", 2000, TimeUnit.MILLISECONDS);

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

  @Test
  public void testRemove() {
    String key = uniqueKey();
    kvCache.set(key, "v");

    Assert.assertEquals("v", kvCache.remove(key));
    Assert.assertNull(kvCache.get(key));
  }

  @Test
  public void testComputeIfAbsent() {
    String key = uniqueKey();
    AtomicInteger counter = new AtomicInteger();

    String first = kvCache.computeIfAbsent(key, k -> "v" + counter.incrementAndGet(), 1, TimeUnit.MINUTES);
    String second = kvCache.computeIfAbsent(key, k -> "v" + counter.incrementAndGet(), 1, TimeUnit.MINUTES);

    Assert.assertEquals("v1", first);
    Assert.assertEquals("v1", second);
    Assert.assertEquals(1, counter.get());
  }

  @Test
  public void testCapacity() {
    Assert.assertEquals(Long.MAX_VALUE, kvCache.capacity());
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
