package cn.addenda.component.cache.test;

import cn.addenda.component.base.util.SleepUtils;
import cn.addenda.component.cache.CacheUnsupportedOperationException;
import cn.addenda.component.cache.ConcurrentHashMapKVCache;
import cn.addenda.component.cache.ExpiredConcurrentHashMapKVCache;
import cn.addenda.component.cache.ExpiredHashMapKVCache;
import cn.addenda.component.cache.ExpiredKVCache;
import cn.addenda.component.cache.HashMapKVCache;
import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.TreeMapKVCache;
import cn.addenda.component.cache.SynchronizedExpiredKVCache;
import cn.addenda.component.cache.SynchronizedKVCache;
import cn.addenda.component.cache.SynchronizedSortedKVCache;
import cn.addenda.component.cache.LettuceRedisClusterKVCache;
import cn.addenda.component.cache.LettuceRedisKVCache;
import cn.addenda.component.cache.RedissonRedisKVCache;
import cn.addenda.component.cache.StringRedisTemplateRedisKVCache;
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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * <b>L2 契约矩阵</b>：把"声明为 L1 + L2"的实现<b>全部</b>收在一张表里，
 * 逐个验证它们对得起这个声明。
 * <p>
 * 要钉的是同一条规则（见 {@link KVCache} 的 L2 层）：<b>原子性不可部分承诺</b> ——
 * 复合操作要么做成原子的，要么<b>明确拒绝</b>（抛 {@link CacheUnsupportedOperationException}），
 * 不允许悄悄退回接口默认实现。默认实现由 get 与写两步组成，多线程可以同时通过"检查"环节，
 * 而且<b>从签名上完全看不出来</b>：调用方以为拿到了原子操作，实际拿到的是竞态。
 * <p>
 * 为什么值得单独一个文件：这条规则此前<b>只写在注释里</b>，没有任何用例会发现"某个自称
 * L1 + L2 的实现偷偷用了默认实现"。各实现自己的用例是分头写的 ——
 * {@code KVCacheContractTest} 管内存那几家、{@code ExpiredKVCacheContractTest} 管带 TTL 的那几家，
 * 各测各的"正向"或"反向"，没有一处把"声明了 L2 的全部实现"当成一个整体来对账。
 * 于是漏掉一个实现、或者它悄悄退化，都不会有人发现。
 * <p>
 * 六条断言：
 * <ol>
 *   <li>{@link #testRegistryMatchesTheDeclaredL2Implementations()} —— 表本身要对，
 *       少了谁、多了谁都要红（矩阵漏跑比没有矩阵更糟）；</li>
 *   <li>{@link #testNoDeclaredL2ImplUsesTheInterfaceDefaultForCompositeOps()} ——
 *       <b>结构性</b>：两个复合默认方法由实现自己声明，不许直接用接口默认实现；</li>
 *   <li>{@link #testComputeIfAbsentIsAtomicOrRefused()}、{@link #testSetnxIsAtomicOrRefused()}、
 *       {@link #testTtlComputeIfAbsentIsAtomicOrRefused()}、{@link #testTtlSetnxIsAtomicOrRefused()}
 *       —— <b>行为性</b>：并发下要么只算一次 / 只成功一次，要么当初就明确拒绝。
 *       这条不依赖"实现有没有覆盖"，只认最终行为。后两条是带 TTL 的重载，
 *       只跑表里带 TTL 的实现（{@code instanceof ExpiredKVCache}），其余跳过。</li>
 * </ol>
 * 两条路子必须都有：结构性那条能一眼指出问题出在哪个方法上，但它假设"覆盖了就是原子的"；
 * 行为性那条不假设任何实现细节。
 * <p>
 * <b>四条行为性断言的鉴别力不一样，别当成等强：</b>
 * {@code computeIfAbsent} 那两条能把窗口<b>确定性地拉开</b> —— {@code mappingFunction} 是本地
 * 代码，让它先睡 100ms，"检查"与"写入"之间的竞态必然暴露；{@code setnx} 那两条做不到，
 * 窗口完全在实现内部，只能靠 8 个线程自然撞 —— 一次"先查后写"的实现只要落在同一个哈希桶上、
 * 又足够快，就可能一次都不撞上。实测：把 {@code ExpiredHashMapKVCache} 临时注册进来，
 * 两条 {@code computeIfAbsent} 都稳定报 {@code expected:<1> but was:<8>}，
 * 而两条 {@code setnx} 不红。
 * <p>
 * <b>边界：本矩阵对 {@code remove} 只做结构性检查（第 2 条），不做行为性检查。</b>
 * 行为性检查要能确定性复现"读到旧值 → 别人换值 → 删掉新值"这个窗口，而唯一的办法是给实现
 * 塞一个可控的底层存储；只有"可控 delegate + 包装层"这一条路径测得出来，通用不了。
 * 因此这里只保证"没退回接口默认实现"，挡不住"自己写了个貌似原子的 remove"——
 * {@code KVCacheContractTest#testDefaultRemoveIsNotAtomic} 从反向钉着默认实现。
 * <p>
 * <b>这套断言不是空跑的</b>：把 {@link HashMapKVCache}（"安全级别：无"，{@code computeIfAbsent}
 * 用的正是接口默认实现）临时注册进 {@link #declaredL2}，第 1、2、3 条会一起变红 ——
 * 清单报出多出来的那个名字、结构性那条指出它"直接用了接口默认实现（声明类 KVCache）"、
 * 行为性那条报 {@code mappingFunction} 被调用了 8 次而不是 1 次。
 * 第 4 条不该红，也确实没红：{@code HashMapKVCache.setnx} 本来就是原子的。
 *
 * @author addenda
 */
@Slf4j
public class L2ContractMatrixTest {

  /**
   * 声明为 L1 + L2 的实现，键是类的简单名。
   * <p>
   * <b>手工维护</b>，和仓库里其他契约测试的实现清单一样 —— 新增一个 L1 + L2 的实现就要加进来，
   * 漏了由 {@link #testRegistryMatchesTheDeclaredL2Implementations()} 负责报错。
   */
  private final Map<String, KVCache<String, String>> declaredL2 = new LinkedHashMap<>();

  /**
   * 并发用例的线程数：8 是仓库里其他并发用例的既有取值（{@code KVCacheContractTest}、
   * {@code ExpiredKVCacheContractTest} 都用它）。
   */
  private static final int THREAD_COUNT = 8;

  private final List<AutoCloseable> resources = new ArrayList<>();

  @Before
  @SneakyThrows
  public void before() {
    // ---- 靠底层存储本身达到 L1 + L2 的两家 ----
    register(new ConcurrentHashMapKVCache<String, String>());
    register(new ExpiredConcurrentHashMapKVCache<String, String>());

    // ---- 靠一把锁把底层补到 L1 + L2 的三家 ----
    // 被包装的都是"安全级别：无"的实现，正好说明 L1 / L2 是包装层补上来的
    register(SynchronizedKVCache.synchronize(new HashMapKVCache<String, String>()));
    register(SynchronizedExpiredKVCache.synchronize(new ExpiredHashMapKVCache<String, String>()));
    register(SynchronizedSortedKVCache.synchronize(new TreeMapKVCache<>()));

    // ---- 靠服务端命令原子性达到 L1 + L2 的四家 ----
    RedisClient redisClient = RedisClientBaseTest.redisClient();
    resources.add(redisClient::shutdown);
    register(new LettuceRedisKVCache(redisClient));

    RedisClusterClient clusterClient = RedisClusterClientBaseTest.redisClusterClient();
    resources.add(clusterClient::shutdown);
    register(new LettuceRedisClusterKVCache(clusterClient));

    RedissonClient redissonClient = RedissonClientBaseTest.redissonClient();
    resources.add(redissonClient::shutdown);
    register(new RedissonRedisKVCache(redissonClient));

    LettuceConnectionFactory factory = StringRedisTemplateBaseTest.lettuceConnectionFactory();
    resources.add(factory::destroy);
    StringRedisTemplate template = StringRedisTemplateBaseTest.stringRedisTemplate(factory);
    register(new StringRedisTemplateRedisKVCache(template));
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

  private void register(KVCache<String, String> cache) {
    declaredL2.put(cache.getClass().getSimpleName(), cache);
  }

  /**
   * <b>表要对。</b>矩阵是一份"谁声明了 L1 + L2"的清单，清单本身错了，后面几条断言跑得再认真也没意义。
   * <p>
   * 新增一个声明 L1 + L2 的实现时，这里会红 —— 提醒作者把它加进 {@link #declaredL2}，
   * 而不是让它绕开整套矩阵。
   */
  @Test
  public void testRegistryMatchesTheDeclaredL2Implementations() {
    TreeSet<String> expected = new TreeSet<>(Arrays.asList(
            "ConcurrentHashMapKVCache",
            "ExpiredConcurrentHashMapKVCache",
            "SynchronizedKVCache",
            "SynchronizedExpiredKVCache",
            "SynchronizedSortedKVCache",
            "LettuceRedisKVCache",
            "LettuceRedisClusterKVCache",
            "RedissonRedisKVCache",
            "StringRedisTemplateRedisKVCache"));
    Assert.assertEquals("声明为 L1 + L2 的实现有变动：新增/删除实现必须同步改这里，"
            + "否则它会绕过整套矩阵 —— 漏跑比没有矩阵更糟",
            expected, new TreeSet<>(declaredL2.keySet()));
  }

  /**
   * <b>结构性：复合操作不许退回接口默认实现。</b>
   * <p>
   * {@link KVCache#remove(Object)} 与 {@link KVCache#computeIfAbsent(Object, Function)}
   * 是接口上仅有的两个默认实现，而它们<b>不属于 L2</b>（见 {@link KVCache} 的 L2 层）。
   * 所以：声明了 L1 + L2 的实现，这两个方法必须由自己（或自己的某个父类）声明出来，
   * 声明类不能是接口本身。
   * <p>
   * 不校验 {@code setnx}：它在 {@link KVCache} 上是<b>抽象</b>方法，没有默认实现可退，
   * 谁都得自己声明。它由下面那条行为性断言覆盖。
   */
  @Test
  public void testNoDeclaredL2ImplUsesTheInterfaceDefaultForCompositeOps() {
    for (Map.Entry<String, KVCache<String, String>> entry : declaredL2.entrySet()) {
      assertNotInterfaceDefault(entry.getKey(), entry.getValue(),
              "remove", Object.class);
      assertNotInterfaceDefault(entry.getKey(), entry.getValue(),
              "computeIfAbsent", Object.class, Function.class);
    }
  }

  private void assertNotInterfaceDefault(String name, KVCache<String, String> cache,
                                         String methodName, Class<?>... parameterTypes) {
    Method method;
    try {
      method = cache.getClass().getMethod(methodName, parameterTypes);
    } catch (NoSuchMethodException e) {
      Assert.fail(name + " 上找不到 public 的 " + methodName + " 方法");
      return;
    }
    Class<?> declaringClass = method.getDeclaringClass();
    Assert.assertFalse(name + " 的 " + methodName + " 直接用了接口默认实现（声明类 "
                    + declaringClass.getSimpleName() + "）。"
                    + "接口默认实现由 get 与写两步组成、达不到 L2 —— "
                    + "做不到原子的就明确拒绝（抛 CacheUnsupportedOperationException），不要静默降级",
            declaringClass == KVCache.class || declaringClass == ExpiredKVCache.class);
  }

  /**
   * <b>行为性：{@code computeIfAbsent} 要么原子、要么明确拒绝。</b>
   * <p>
   * 不预设实现有没有覆盖它：先探一次，抛 {@link CacheUnsupportedOperationException} 的就是"明确拒绝"，
   * 合法（四个 redis 实现即如此）；不抛的就必须扛住并发 —— 8 个线程同时打同一个 key，
   * {@code mappingFunction} 只能被调用一次。默认实现那条"先 get 再 set"的路径会被调用多次，
   * 这是已有用例（{@code ExpiredKVCacheContractTest#testComputeIfAbsentIsNotAtomicUnderConcurrency}）
   * 的镜像断言：那边断 {@code > 1}，这边断 {@code == 1}。
   */
  @Test
  public void testComputeIfAbsentIsAtomicOrRefused() throws Exception {
    for (Map.Entry<String, KVCache<String, String>> entry : declaredL2.entrySet()) {
      String name = entry.getKey();
      KVCache<String, String> cache = entry.getValue();

      String probeKey = uniqueKey(name);
      try {
        cache.computeIfAbsent(probeKey, k -> "probe");
      } catch (CacheUnsupportedOperationException refused) {
        log.info("{} 明确拒绝了 computeIfAbsent（L2 允许，做不到就拒绝）", name);
        continue;
      } finally {
        cache.delete(probeKey);
      }

      String key = uniqueKey(name);
      ConcurrentResult result = concurrentComputeIfAbsent(cache, key);
      log.info("{}: 8 线程并发 computeIfAbsent，mappingFunction 被调用 {} 次", name, result.invoked);
      Assert.assertEquals(name + " 没有拒绝 computeIfAbsent，那它就必须是原子的 —— "
              + "mappingFunction 被调用了多次说明它走的是接口默认实现（或者自己实现成了非原子的）",
              1, result.invoked);
      assertAllThreadsGotTheValue(name, result);
      cache.delete(key);
    }
  }

  /**
   * <b>行为性：{@code setnx} 要么原子、要么明确拒绝。</b>
   * <p>
   * {@code setnx} 也是 check-then-act（"不存在才写"），同一个道理：并发抢同一个 key 只能有一个成功。
   * 明确拒绝的实现（如 {@code LruKKVCache}）合法，但那个不在这张表里 ——
   * 它声明的是"安全级别：无"。
   */
  @Test
  public void testSetnxIsAtomicOrRefused() throws Exception {
    for (Map.Entry<String, KVCache<String, String>> entry : declaredL2.entrySet()) {
      String name = entry.getKey();
      KVCache<String, String> cache = entry.getValue();

      String probeKey = uniqueKey(name);
      try {
        cache.setnx(probeKey, "probe");
      } catch (CacheUnsupportedOperationException refused) {
        log.info("{} 明确拒绝了 setnx（L2 允许，做不到就拒绝）", name);
        continue;
      } finally {
        cache.delete(probeKey);
      }

      String key = uniqueKey(name);
      int success = concurrentSetnx(cache, key);
      log.info("{}: 8 线程并发 setnx，成功 {} 次", name, success);
      Assert.assertEquals(name + " 的 setnx 并发下只能成功一次", 1, success);
      cache.delete(key);
    }
  }

  /**
   * <b>行为性：带 TTL 的 {@code computeIfAbsent} 要么原子、要么明确拒绝。</b>
   * <p>
   * 与 {@link #testComputeIfAbsentIsAtomicOrRefused()} 是同名的另一个重载，但底层走的路径
   * 完全不同（这个要在写入的同时设过期），所以分开各跑一遍。
   * 只跑表里带 TTL 的实现 —— 这个重载声明在 {@link ExpiredKVCache} 上。
   */
  @Test
  public void testTtlComputeIfAbsentIsAtomicOrRefused() throws Exception {
    for (Map.Entry<String, KVCache<String, String>> entry : declaredL2.entrySet()) {
      if (!(entry.getValue() instanceof ExpiredKVCache)) {
        continue;
      }
      String name = entry.getKey();
      ExpiredKVCache<String, String> cache = (ExpiredKVCache<String, String>) entry.getValue();

      String probeKey = uniqueKey(name);
      try {
        cache.computeIfAbsent(probeKey, k -> "probe", 1, TimeUnit.MINUTES);
      } catch (CacheUnsupportedOperationException refused) {
        log.info("{} 明确拒绝了带 TTL 的 computeIfAbsent（L2 允许，做不到就拒绝）", name);
        continue;
      } finally {
        cache.delete(probeKey);
      }

      String key = uniqueKey(name);
      ConcurrentResult result = concurrentTtlComputeIfAbsent(cache, key);
      log.info("{}: 8 线程并发带 TTL 的 computeIfAbsent，mappingFunction 被调用 {} 次",
              name, result.invoked);
      Assert.assertEquals(name + " 没有拒绝带 TTL 的 computeIfAbsent，那它就必须是原子的",
              1, result.invoked);
      assertAllThreadsGotTheValue(name, result);
      cache.delete(key);
    }
  }

  /**
   * <b>行为性：带 TTL 的 {@code setnx} 要么原子、要么明确拒绝。</b>
   * <p>
   * 同 {@link #testSetnxIsAtomicOrRefused()} 的另一个重载 —— redis 上一个是 {@code SET NX}、
   * 一个是 {@code SET NX PX}，是两条不同的命令，所以分开各跑一遍。
   */
  @Test
  public void testTtlSetnxIsAtomicOrRefused() throws Exception {
    for (Map.Entry<String, KVCache<String, String>> entry : declaredL2.entrySet()) {
      if (!(entry.getValue() instanceof ExpiredKVCache)) {
        continue;
      }
      String name = entry.getKey();
      ExpiredKVCache<String, String> cache = (ExpiredKVCache<String, String>) entry.getValue();

      String probeKey = uniqueKey(name);
      try {
        cache.setnx(probeKey, "probe", 1, TimeUnit.MINUTES);
      } catch (CacheUnsupportedOperationException refused) {
        log.info("{} 明确拒绝了带 TTL 的 setnx（L2 允许，做不到就拒绝）", name);
        continue;
      } finally {
        cache.delete(probeKey);
      }

      String key = uniqueKey(name);
      int success = concurrentTtlSetnx(cache, key);
      log.info("{}: 8 线程并发带 TTL 的 setnx，成功 {} 次", name, success);
      Assert.assertEquals(name + " 带 TTL 的 setnx 并发下只能成功一次", 1, success);
      cache.delete(key);
    }
  }

  /**
   * 一次并发点算的结果。
   * <p>
   * 除了次数，还要把<b>各线程拿到的返回值</b>带出来：只算一次不等于每个线程都拿到了那个值 ——
   * 理论上"算一次、其余线程拿到 null"是可能的，这条断言原来只存在于
   * {@code ConcurrentHashMapKVCacheTest}，现在并到这里。
   */
  private static final class ConcurrentResult {

    private final int invoked;
    private final List<String> values;

    private ConcurrentResult(int invoked, List<String> values) {
      this.invoked = invoked;
      this.values = values;
    }

  }

  /**
   * 只算一次还不够，每个线程都得拿到算出来的那个值。
   */
  private void assertAllThreadsGotTheValue(String name, ConcurrentResult result) {
    for (String v : result.values) {
      Assert.assertEquals(name + ": 并发下每个线程都应该拿到算出来的值", "computed", v);
    }
  }

  /**
   * @return 8 个线程并发调用后，{@code mappingFunction} 被调用的次数，以及各线程拿到的返回值。
   * 原子的实现必然只算一次；走"先 get 再 set"的实现会被调用多次。
   */
  private ConcurrentResult concurrentComputeIfAbsent(KVCache<String, String> cache, String key)
          throws Exception {
    AtomicInteger invoked = new AtomicInteger();
    List<String> values = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(THREAD_COUNT);

    for (int t = 0; t < THREAD_COUNT; t++) {
      Thread thread = new Thread(() -> {
        try {
          start.await();
          values.add(cache.computeIfAbsent(key, k -> {
            invoked.incrementAndGet();
            // 把 check 与 act 之间的窗口拉大，让非原子的实现暴露出来
            SleepUtils.sleep(TimeUnit.MILLISECONDS, 100);
            return "computed";
          }));
        } catch (Throwable e) {
          log.error("{}: 并发 computeIfAbsent 抛异常（本用例只看次数，不断言异常）", key, e);
        } finally {
          done.countDown();
        }
      });
      thread.setDaemon(true);
      thread.start();
    }

    start.countDown();
    Assert.assertTrue("不应该死循环或死锁", done.await(60, TimeUnit.SECONDS));
    return new ConcurrentResult(invoked.get(), values);
  }

  /**
   * @return 8 个线程并发 {@code setnx} 同一个 key，成功的次数。原子的实现必然返回 1。
   */
  private int concurrentSetnx(KVCache<String, String> cache, String key) throws Exception {
    AtomicInteger success = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(THREAD_COUNT);

    for (int t = 0; t < THREAD_COUNT; t++) {
      Thread thread = new Thread(() -> {
        try {
          start.await();
          if (cache.setnx(key, "v")) {
            success.incrementAndGet();
          }
        } catch (Throwable e) {
          log.error("{}: 并发 setnx 抛异常（本用例只看次数，不断言异常）", key, e);
        } finally {
          done.countDown();
        }
      });
      thread.setDaemon(true);
      thread.start();
    }

    start.countDown();
    Assert.assertTrue("不应该死循环或死锁", done.await(60, TimeUnit.SECONDS));
    return success.get();
  }

  /**
   * 同 {@link #concurrentComputeIfAbsent(KVCache, String)}，调的是带 TTL 的重载。
   * <p>
   * 参数类型是 {@link ExpiredKVCache}：那个重载只声明在它上面，这样调用处不必强转。
   */
  private ConcurrentResult concurrentTtlComputeIfAbsent(ExpiredKVCache<String, String> cache, String key)
          throws Exception {
    AtomicInteger invoked = new AtomicInteger();
    List<String> values = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(THREAD_COUNT);

    for (int t = 0; t < THREAD_COUNT; t++) {
      Thread thread = new Thread(() -> {
        try {
          start.await();
          values.add(cache.computeIfAbsent(key, k -> {
            invoked.incrementAndGet();
            // 把 check 与 act 之间的窗口拉大，让非原子的实现暴露出来
            SleepUtils.sleep(TimeUnit.MILLISECONDS, 100);
            return "computed";
          }, 1, TimeUnit.MINUTES));
        } catch (Throwable e) {
          log.error("{}: 并发带 TTL 的 computeIfAbsent 抛异常（本用例只看次数，不断言异常）", key, e);
        } finally {
          done.countDown();
        }
      });
      thread.setDaemon(true);
      thread.start();
    }

    start.countDown();
    Assert.assertTrue("不应该死循环或死锁", done.await(60, TimeUnit.SECONDS));
    return new ConcurrentResult(invoked.get(), values);
  }

  /**
   * 同 {@link #concurrentSetnx(KVCache, String)}，调的是带 TTL 的重载。
   */
  private int concurrentTtlSetnx(ExpiredKVCache<String, String> cache, String key) throws Exception {
    AtomicInteger success = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(THREAD_COUNT);

    for (int t = 0; t < THREAD_COUNT; t++) {
      Thread thread = new Thread(() -> {
        try {
          start.await();
          if (cache.setnx(key, "v", 1, TimeUnit.MINUTES)) {
            success.incrementAndGet();
          }
        } catch (Throwable e) {
          log.error("{}: 并发带 TTL 的 setnx 抛异常（本用例只看次数，不断言异常）", key, e);
        } finally {
          done.countDown();
        }
      });
      thread.setDaemon(true);
      thread.start();
    }

    start.countDown();
    Assert.assertTrue("不应该死循环或死锁", done.await(60, TimeUnit.SECONDS));
    return success.get();
  }

  private String uniqueKey(String implName) {
    return "L2ContractMatrixTest:" + implName + ":"
            + UUID.randomUUID().toString().replace("-", "");
  }

}
