package cn.addenda.component.cache.test;

import cn.addenda.component.base.util.SleepUtils;
import cn.addenda.component.cache.ExpiredHashMapKVCache;
import cn.addenda.component.cache.ExpiredKVCache;
import cn.addenda.component.cache.helper.LettuceRedisClusterKVCache;
import cn.addenda.component.cache.helper.LettuceRedisKVCache;
import cn.addenda.component.cache.helper.RedissonRedisKVCache;
import cn.addenda.component.cache.SynchronizedExpiredKVCache;
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
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
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

  /**
   * 底层存储本身线程安全的那些 —— 按 {@link cn.addenda.component.cache.KVCache} 上的约定，
   * 它们自己也必须线程安全，所以会被并发用例覆盖。
   * <p/>
   * {@code ExpiredHashMapKVCache} 不在这里：它底层是普通 HashMap，本来就不承诺线程安全。
   */
  private final List<ExpiredKVCache<String, String>> threadSafeCaches = new ArrayList<>();

  private final List<AutoCloseable> resources = new ArrayList<>();

  @Before
  @SneakyThrows
  public void before() {
    caches.add(new ExpiredHashMapKVCache<String, String>() {
    });

    RedisClient redisClient = RedisClientBaseTest.redisClient();
    resources.add(redisClient::shutdown);
    addThreadSafe(new LettuceRedisKVCache(redisClient));

    RedisClusterClient clusterClient = RedisClusterClientBaseTest.redisClusterClient();
    resources.add(clusterClient::shutdown);
    addThreadSafe(new LettuceRedisClusterKVCache(clusterClient));

    RedissonClient redissonClient = RedissonClientBaseTest.redissonClient();
    resources.add(redissonClient::shutdown);
    addThreadSafe(new RedissonRedisKVCache(redissonClient));

    LettuceConnectionFactory factory = newConnectionFactory();
    resources.add(factory::destroy);
    StringRedisTemplate template = new StringRedisTemplate(factory);
    template.afterPropertiesSet();
    addThreadSafe(new StringRedisTemplateRedisKVCache(template));
  }

  private void addThreadSafe(ExpiredKVCache<String, String> cache) {
    caches.add(cache);
    threadSafeCaches.add(cache);
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

  /**
   * 线程安全：底层存储线程安全的实现，KVCache 也必须线程安全。
   * <p/>
   * 每个线程用自己的 key 反复"写 → 原样读回来 → 查存在 → 删"，同时在一个共享 key 上
   * 做并发的写/删/条件写。要求：不抛异常、本线程写的值必须原样读回来。
   * <p/>
   * 这个用例能抓到的是客户端侧的状态问题（共享可变字段、非线程安全的代理等），
   * 抓不到 redis 服务端的问题（那是 redis 自己的事）。
   */
  @Test
  public void testThreadSafety() throws Exception {
    int threadCount = 4;
    int rounds = 20;

    for (ExpiredKVCache<String, String> cache : threadSafeCaches) {
      String name = cache.getClass().getSimpleName();
      String sharedKey = uniqueKey(name) + ":shared";

      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threadCount);
      List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
      AtomicInteger completedRounds = new AtomicInteger();
      AtomicInteger skippedRounds = new AtomicInteger();

      for (int t = 0; t < threadCount; t++) {
        final int threadId = t;
        Thread thread = new Thread(() -> {
          String ownKey = sharedKey + ":t" + threadId;
          String ownValue = "v" + threadId;
          try {
            start.await();
            for (int i = 0; i < rounds; i++) {
              try {
                cache.set(ownKey, ownValue, 1, TimeUnit.MINUTES);

                // 本线程写的值必须原样读回来 —— 读回来别人的值说明客户端串号了
                String read = cache.get(ownKey);
                if (!ownValue.equals(read)) {
                  throw new AssertionError(name + ": 本线程写的值应该原样读回来. 期望[" + ownValue
                          + "] 实际[" + read + "] key=" + ownKey);
                }

                // 共享 key 上并发写删，只要求不抛异常
                cache.set(sharedKey, ownValue, 1, TimeUnit.MINUTES);
                cache.get(sharedKey);
                cache.containsKey(sharedKey);
                cache.delete(sharedKey);

                // 到这里 ownKey 必须还在（1 分钟 ttl，一轮才几十毫秒）
                if (!cache.containsKey(ownKey)) {
                  throw new AssertionError(name + ": ownKey 在连续操作之间消失了，key=" + ownKey
                          + " 再次 get=" + cache.get(ownKey));
                }
                completedRounds.incrementAndGet();
              } catch (Throwable e) {
                // 公网那台 redis 时不时 5s 命令超时 / 连不上，那是环境问题，
                // 不是这个用例要测的"客户端侧线程安全"，跳过这一轮
                if (isTransientServerError(e)) {
                  skippedRounds.incrementAndGet();
                } else {
                  throw e;
                }
              }
            }
          } catch (Throwable e) {
            failures.add(new IllegalStateException(name + ": 线程 " + threadId + " 失败", e));
          } finally {
            try {
              cache.delete(ownKey);
            } catch (Throwable ignored) {
              // 收尾失败无所谓
            }
            done.countDown();
          }
        });
        thread.setDaemon(true);
        thread.start();
      }

      start.countDown();
      Assert.assertTrue(name + ": 并发执行超时", done.await(120, TimeUnit.SECONDS));

      for (Throwable failure : failures) {
        log.error("{}", name, failure);
      }
      Assert.assertTrue(name + ": 并发执行不应该有任何异常", failures.isEmpty());
      if (skippedRounds.get() > 0) {
        log.error("{}: 有 {} 轮因为 redis 抖动被跳过", name, skippedRounds.get());
      }
      Assert.assertTrue(name + ": 大多数轮次应该跑完，否则这个用例等于没测",
              completedRounds.get() >= threadCount * rounds / 2);

      cache.delete(sharedKey);
    }
  }

  /**
   * {@code ExpiredHashMapKVCache} <b>不保证线程安全</b> —— 底层是普通 HashMap。
   * <p/>
   * 并发写下不只是丢数据，还会因为桶结构损坏而<b>卡死</b>。实测 8 线程 × 3000 次写：
   * 10 轮里 9 轮丢数据（丢 5%~67%）、1 轮直接卡死 —— 正常轮次只要 5~22ms，
   * 所以卡死不是"太慢"，是真的卡住了。
   * <p/>
   * 断言写成 {@code lost != 0}：丢数据（{@code lost > 0}）和卡死（{@code lost == -1}）
   * 都算证明了它不线程安全。反过来，如果哪天它变成 0，说明实现被换成了线程安全的，
   * 这个用例就该删掉。
   */
  @Test
  public void testThreadUnSafe() throws Exception {
    long lost = concurrentWrite(new ExpiredHashMapKVCache<String, String>() {
    }, 8, 3000);

    log.error("[ExpiredHashMapKVCache] 并发写 24000 个 key：{}",
            lost == -1 ? "卡死（10s 没跑完）" : ("丢了 " + lost + " 个"));

    Assert.assertTrue(
            "并发写 24000 个 key 应该丢数据或卡死；如果这里真的 0 丢失，说明它已经变成线程安全的了",
            lost != 0);
  }

  /**
   * 对照组：包一层 {@link SynchronizedExpiredKVCache} 之后必须一条不丢。
   * <p/>
   * 这就是 {@link cn.addenda.component.cache.KVCache} 上写的"底层不线程安全时怎么办"的答案。
   */
  @Test
  public void testSynchronizedWrapperIsThreadSafe() throws Exception {
    long lost = concurrentWrite(
            SynchronizedExpiredKVCache.<String, String>synchronize(new ExpiredHashMapKVCache<String, String>() {
            }), 8, 3000);

    Assert.assertEquals("SynchronizedExpiredKVCache 包装之后不应该丢任何数据", 0, lost);
  }

  /**
   * n 个线程并发往同一个 cache 里写各自独占的 key，然后校验每个 key 都还在。
   *
   * @return 丢失的 key 数量；<b>-1 表示超时</b>（多半是底层结构被并发写坏、线程卡死了）
   */
  @SneakyThrows
  private long concurrentWrite(ExpiredKVCache<String, String> cache, int threads, int perThread) {
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);

    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      Thread thread = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < perThread; i++) {
            cache.set("race:" + threadId + ":" + i, "v");
          }
        } catch (Throwable ignored) {
          // 丢数据 / 卡死才是这次要看的结果
        } finally {
          done.countDown();
        }
      });
      thread.setDaemon(true);
      thread.start();
    }

    start.countDown();
    if (!done.await(10, TimeUnit.SECONDS)) {
      return -1;
    }

    long lost = 0;
    for (int t = 0; t < threads; t++) {
      for (int i = 0; i < perThread; i++) {
        if (!cache.containsKey("race:" + t + ":" + i)) {
          lost++;
        }
      }
    }
    return lost;
  }

  /**
   * {@code computeIfAbsent} 在并发下<b>不是原子的</b> —— 这是接口契约层面的，不是实现缺陷。
   * <p/>
   * {@code KVCache} 的默认实现是 check-then-act：
   * <pre>
   * if ((v = get(key)) == null) {        // ← 检查
   *     if ((newValue = mappingFunction.apply(key)) != null) {
   *         set(key, newValue);          // ← 执行
   *     }
   * }
   * </pre>
   * 多个线程可以同时通过"检查"这一步，于是 {@code mappingFunction} 被调用多次，
   * 最后一次写入胜出。单线程下只调一次（见 {@code testComputeIfAbsentIsLazy}），并发下不成立。
   * <p/>
   * 需要原子性的话由上层加锁 —— {@code CacheHelper} 就是这么做的
   * （{@code lockAllocator} + 令牌桶限流，见 {@code LettuceRedisCacheHelperTest.testConcurrentQueryHitsCache}）。
   * <p/>
   * 同时验证：并发下<b>不抛异常、不死循环/死锁</b>（用带超时的 await 兜住）。
   */
  @Test
  public void testComputeIfAbsentIsNotAtomicUnderConcurrency() throws Exception {
    int threadCount = 8;

    for (ExpiredKVCache<String, String> cache : threadSafeCaches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      AtomicInteger invoked = new AtomicInteger();
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threadCount);
      List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

      for (int t = 0; t < threadCount; t++) {
        Thread thread = new Thread(() -> {
          try {
            start.await();
            cache.computeIfAbsent(key, k -> {
              invoked.incrementAndGet();
              // 把 check 和 act 之间的窗口拉大，让并发暴露出来
              SleepUtils.sleep(TimeUnit.MILLISECONDS, 100);
              return "computed";
            }, 1, TimeUnit.MINUTES);
          } catch (Throwable e) {
            failures.add(new IllegalStateException(name + ": 并发 computeIfAbsent 抛异常", e));
          } finally {
            done.countDown();
          }
        });
        thread.setDaemon(true);
        thread.start();
      }

      start.countDown();
      // 带超时的 await：真的死循环/死锁的话这里会失败，而不是把测试挂住
      Assert.assertTrue(name + ": 不应该死循环或死锁", done.await(30, TimeUnit.SECONDS));

      log.error("{}: {} 个线程并发 computeIfAbsent，mappingFunction 被调用了 {} 次",
              name, threadCount, invoked.get());

      for (Throwable failure : failures) {
        log.error("{}", name, failure);
      }
      Assert.assertTrue(name + ": 并发下不应该抛异常", failures.isEmpty());
      Assert.assertTrue(name + ": 并发下 mappingFunction 应该被调用多次，说明 computeIfAbsent 不是原子的",
              invoked.get() > 1);

      cache.delete(key);
    }
  }

  /**
   * 判断是不是"服务器/网络抖动"这类环境问题，而不是客户端线程安全问题。
   * <p/>
   * 不按具体异常类型判（lettuce / redisson / spring 各不相同），按文案判，
   * 这样五个实现共用一份逻辑。
   */
  private boolean isTransientServerError(Throwable t) {
    for (Throwable c = t; c != null && c != c.getCause(); c = c.getCause()) {
      if (c instanceof AssertionError) {
        return false;
      }
      String message = c.getMessage();
      if (message == null) {
        continue;
      }
      if (message.contains("timed out") || message.contains("Unable to connect")
              || message.contains("Connection refused")) {
        return true;
      }
    }
    return false;
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
