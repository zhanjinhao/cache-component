package cn.addenda.component.cache.test.helper;

import cn.addenda.component.base.util.SleepUtils;
import cn.addenda.component.cache.helper.LettuceRedisClusterKVCache;
import cn.addenda.component.cache.test.RedisClusterClientBaseTest;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link LettuceRedisKVCacheTest} 的集群版，逐条对齐以证明行为一致。
 *
 * @author addenda
 */
public class LettuceRedisClusterKVCacheTest {

  private RedisClusterClient redisClusterClient;

  private LettuceRedisClusterKVCache kvCache;

  @Before
  @SneakyThrows
  public void before() {
    redisClusterClient = RedisClusterClientBaseTest.redisClusterClient();
    kvCache = new LettuceRedisClusterKVCache(redisClusterClient);
  }

  @After
  public void after() {
    kvCache.close();
    if (redisClusterClient != null) {
      redisClusterClient.shutdown();
    }
  }

  /**
   * 每个用例使用独立的key，避免共用redis实例时用例之间相互影响
   */
  private String uniqueKey() {
    return "LettuceRedisClusterKVCacheTest:" + UUID.randomUUID().toString().replace("-", "");
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

  /**
   * set(k, null) 等价于删除 —— 和单机版及其它 ExpiredKVCache 实现对齐。
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
   * 带 ttl 的重载行为一致
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
    StatefulRedisClusterConnection<String, String> connection = redisClusterClient.connect();
    try {
      LettuceRedisClusterKVCache externalKvCache = new LettuceRedisClusterKVCache(connection);
      externalKvCache.set(uniqueKey(), "v");
      externalKvCache.close();
      Assert.assertTrue(connection.isOpen());
    } finally {
      connection.close();
    }
  }

}
