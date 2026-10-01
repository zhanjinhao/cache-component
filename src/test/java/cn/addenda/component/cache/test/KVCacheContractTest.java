package cn.addenda.component.cache.test;

import cn.addenda.component.cache.ExpiredHashMapKVCache;
import cn.addenda.component.cache.ExpiredKVCache;
import cn.addenda.component.cache.helper.LettuceRedisClusterKVCache;
import cn.addenda.component.cache.helper.LettuceRedisKVCache;
import cn.addenda.component.cache.helper.RedissonRedisKVCache;
import cn.addenda.component.cache.helper.StringRedisTemplateRedisKVCache;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KVCache / ExpiredKVCache 的契约测试：同一组断言跑遍全部实现。
 * <p/>
 * 目前覆盖：
 * <ul>
 *   <li>{@code set(k, null)} 一律当删除 —— 调用后 get 返回 null、containsKey 返回 false</li>
 *   <li>{@code computeIfAbsent} 惰性求值（命中缓存时不调 mappingFunction），结果为 null 时不写入</li>
 * </ul>
 * <p/>
 * 为什么要跑遍实现而不是只测一个：这些契约最容易在某个实现上破功，
 * 而且破法都很隐蔽 —— "静默把值换掉"、"命中缓存还白算一次"，都不报错。
 * redis 那几家（lettuce / redisson / spring-data-redis）对 null 的天然行为各不相同，
 * 尤其容易走偏。
 *
 * @author addenda
 */
@Slf4j
public class KVCacheContractTest {

  private final List<ExpiredKVCache<String, String>> caches = new ArrayList<>();

  private final List<AutoCloseable> resources = new ArrayList<>();

  @Before
  @SneakyThrows
  public void before() {
    caches.add(new ExpiredHashMapKVCache<String, String>() {
    });

    RedisClient redisClient = RedisClientBaseTest.redisClient();
    resources.add(redisClient::shutdown);
    caches.add(new LettuceRedisKVCache(redisClient));

    RedisClusterClient clusterClient = RedisClusterClientBaseTest.redisClusterClient();
    resources.add(clusterClient::shutdown);
    caches.add(new LettuceRedisClusterKVCache(clusterClient));

    RedissonClient redissonClient = RedissonClientBaseTest.redissonClient();
    resources.add(redissonClient::shutdown);
    caches.add(new RedissonRedisKVCache(redissonClient));

    LettuceConnectionFactory factory = newConnectionFactory();
    resources.add(factory::destroy);
    StringRedisTemplate template = new StringRedisTemplate(factory);
    template.afterPropertiesSet();
    caches.add(new StringRedisTemplateRedisKVCache(template));
  }

  @After
  public void after() {
    for (AutoCloseable resource : resources) {
      try {
        resource.close();
      } catch (Exception e) {
        log.error("释放资源失败", e);
      }
    }
  }

  @Test
  public void testSetNullMeansDelete() {
    for (ExpiredKVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      cache.set(key, "v");
      Assert.assertTrue(name + ": 前置条件，先写进去一个值", cache.containsKey(key));

      cache.set(key, null);
      Assert.assertNull(name + ": set(k, null) 之后 get 应该返回 null", cache.get(key));
      Assert.assertFalse(name + ": set(k, null) 应该等价于删除", cache.containsKey(key));

      cache.delete(key);
    }
  }

  @Test
  public void testSetNullWithTimeoutMeansDelete() {
    for (ExpiredKVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      cache.set(key, "v", 1, TimeUnit.MINUTES);
      Assert.assertTrue(name + ": 前置条件", cache.containsKey(key));

      cache.set(key, null, 1, TimeUnit.MINUTES);
      Assert.assertNull(name + ": 带 ttl 的 set(k, null) 之后 get 应该返回 null", cache.get(key));
      Assert.assertFalse(name + ": 带 ttl 的 set(k, null) 也应该等价于删除", cache.containsKey(key));

      cache.delete(key);
    }
  }

  /**
   * computeIfAbsent 命中缓存时不应该再调 mappingFunction —— 和 {@code Map.computeIfAbsent} 一致。
   * <p/>
   * 之前 {@code ExpiredHashMapKVCache} 的实现是无条件先算再放进 map，
   * 命中缓存时业务侧的计算函数照样执行，代价白付。
   */
  @Test
  public void testComputeIfAbsentIsLazy() {
    for (ExpiredKVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);
      AtomicInteger invoked = new AtomicInteger();

      Assert.assertEquals(name + ": 第一次应该计算",
              "v1", cache.computeIfAbsent(key, k -> "v" + invoked.incrementAndGet(), 1, TimeUnit.MINUTES));
      Assert.assertEquals(name + ": 第二次应该命中缓存，不再计算",
              "v1", cache.computeIfAbsent(key, k -> "v" + invoked.incrementAndGet(), 1, TimeUnit.MINUTES));
      Assert.assertEquals(name + ": mappingFunction 只应该被调用一次", 1, invoked.get());

      // 不带 ttl 的重载同样命中
      Assert.assertEquals(name + ": 不带 ttl 的重载命中同一个 key",
              "v1", cache.computeIfAbsent(key, k -> "v" + invoked.incrementAndGet()));
      Assert.assertEquals(name + ": mappingFunction 仍然只被调用一次", 1, invoked.get());

      cache.delete(key);
    }
  }

  /**
   * computeIfAbsent 的计算结果为 null 时不应该写入 —— 否则又变成"存了个 null"，
   * containsKey 返回 true 而 get 返回 null，和 KVCache#set 的契约打架。
   */
  @Test
  public void testComputeIfAbsentDoesNotStoreNull() {
    for (ExpiredKVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      Assert.assertNull(name + ": 应该返回 null",
              cache.computeIfAbsent(key, k -> null, 1, TimeUnit.MINUTES));
      Assert.assertFalse(name + ": 结果为 null 时不应该写入", cache.containsKey(key));

      Assert.assertNull(name + ": 2 参重载同样",
              cache.computeIfAbsent(key, k -> null));
      Assert.assertFalse(name + ": 2 参重载同样不写入", cache.containsKey(key));

      cache.delete(key);
    }
  }

  private String uniqueKey(String implName) {
    return "KVCacheContractTest:" + implName + ":" + UUID.randomUUID().toString().replace("-", "");
  }

  @SneakyThrows
  private LettuceConnectionFactory newConnectionFactory() {
    Properties properties = new Properties();
    try (InputStream in = new FileInputStream(KVCacheContractTest.class.getClassLoader()
            .getResource("redis_standalone.properties").getPath())) {
      properties.load(in);
    }

    LettuceConnectionFactory factory = new LettuceConnectionFactory(
            properties.getProperty("host"), Integer.parseInt(properties.getProperty("port")));
    factory.setPassword(properties.getProperty("password"));
    factory.afterPropertiesSet();
    return factory;
  }

}
