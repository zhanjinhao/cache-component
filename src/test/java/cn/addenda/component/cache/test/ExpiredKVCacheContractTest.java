package cn.addenda.component.cache.test;

import cn.addenda.component.base.util.SleepUtils;
import cn.addenda.component.cache.CacheIllegalArgumentException;
import cn.addenda.component.cache.CacheUnsupportedOperationException;
import cn.addenda.component.cache.ExpiredConcurrentHashMapKVCache;
import cn.addenda.component.cache.ExpiredHashMapKVCache;
import cn.addenda.component.cache.ExpiredKVCache;
import cn.addenda.component.cache.LettuceRedisClusterKVCache;
import cn.addenda.component.cache.LettuceRedisKVCache;
import cn.addenda.component.cache.RedissonRedisKVCache;
import cn.addenda.component.cache.SynchronizedExpiredKVCache;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * KVCache / ExpiredKVCache 的契约测试：同一组断言跑遍全部实现。
 * <p>
 * 目前覆盖：
 * <ul>
 *   <li>{@code set(k, null)} 一律当删除 —— 调用后 get 返回 null、containsKey 返回 false</li>
 *   <li>{@code computeIfAbsent} 惰性求值（命中缓存时不调 mappingFunction），结果为 null 时不写入</li>
 * </ul>
 * <p>
 * 为什么要跑遍实现而不是只测一个：这些契约最容易在某个实现上破功，
 * 而且破法都很隐蔽 —— "静默把值换掉"、"命中缓存还白算一次"，都不报错。
 * redis 那几家（lettuce / redisson / spring-data-redis）对 null 的天然行为各不相同，
 * 尤其容易走偏。
 *
 * @author addenda
 */
@Slf4j
public class ExpiredKVCacheContractTest {

  private final List<ExpiredKVCache<String, String>> caches = new ArrayList<>();

  /**
   * 底层存储本身线程安全的那些 —— 按 {@link cn.addenda.component.cache.KVCache} 上的约定，
   * 它们自己也必须线程安全，所以会被并发用例覆盖。
   * <p>
   * {@code ExpiredHashMapKVCache} 不在这里：它底层是普通 HashMap，本来就不承诺线程安全。
   */
  private final List<ExpiredKVCache<String, String>> threadSafeCaches = new ArrayList<>();

  /**
   * 每个线程在 L1 用例里写多少个独占 key，见 {@link #testThreadSafety()}。
   * <p>
   * 内存实现必须与 {@link #testThreadUnSafe()} 的反例同参（8×3000）—— "同一个探针在不安全的
   * 实现上能红"这句话，只有参数一致时才证明得了这条正例。redis 实现受公网 RTT 限制只能给小档，
   * 那一档是<b>客户端侧冒烟</b>（共享连接/代理不串号、不炸），不是结构压力。
   * <p>
   * 用 {@link IdentityHashMap}：键的语义是"哪一个实例"，不该被将来可能出现的
   * {@code equals} 实现悄悄合并。
   */
  private final Map<ExpiredKVCache<String, String>, Integer> l1PerThread = new IdentityHashMap<>();

  /**
   * 内存实现。过期时刻由 {@link ExpiredKVCache#deadline} 自己算，溢出时饱和成"永不过期"，
   * 因此超大 TTL 与 redis 那组口径相反，见 {@link #testHugeTtlSaturatesOnInMemoryImpls()}。
   */
  private final List<ExpiredKVCache<String, String>> memoryCaches = new ArrayList<>();

  /**
   * 基于 redis 的实现。它们的过期时间由服务端最终裁决，因此超大同一条回文
   * （{@code ERR invalid expire time}）在四个客户端上都会出现，见
   * {@link #testHugeTtlIsRejectedUniformlyOnRedisImpls()}。
   */
  private final List<ExpiredKVCache<String, String>> redisCaches = new ArrayList<>();

  /**
   * 支持 {@code computeIfAbsent} 的那些（原子的与非原子的都在里面）。
   */
  private final List<ExpiredKVCache<String, String>> computeIfAbsentSupportedCaches = new ArrayList<>();

  /**
   * {@link #computeIfAbsentSupportedCaches} 里把 {@code computeIfAbsent} 做成原子操作的那些。
   * <p>
   * 单独分一组的原因：<b>原子性不是线程安全的必然结果</b> —— 线程安全只保证结构不坏，
   * 要把 check-then-act 做成原子得额外覆盖 {@code computeIfAbsent}。它与
   * {@link #computeIfAbsentNonAtomicCaches} 的断言正好相反（一组 {@code == 1}，一组 {@code > 1}），
   * 必须分开跑。
   */
  private final List<ExpiredKVCache<String, String>> computeIfAbsentAtomicCaches = new ArrayList<>();

  /**
   * 支持 {@code computeIfAbsent}、但没把它做成原子操作的那些 —— 即走接口默认实现
   * （get + set 两步）的那几个。当前只有 {@code ExpiredHashMapKVCache}。
   * <p>
   * 这一组在非线程安全的实现上做并发观察是刻意的，见
   * {@link #testComputeIfAbsentIsNotAtomicUnderConcurrency()}。
   */
  private final List<ExpiredKVCache<String, String>> computeIfAbsentNonAtomicCaches = new ArrayList<>();

  /**
   * 明确拒绝提供 {@code computeIfAbsent} 的实现（两个重载都抛
   * {@link CacheUnsupportedOperationException}）。
   * <p>
   * 基于 redis 的那四个：它们做不到原子的"不存在则计算并写入"，于是选择不给非原子版本。
   * 这是它们能声明 L2 的前提 —— 对外暴露的方法全部原子，做不到的那个直接拒绝。
   */
  private final List<ExpiredKVCache<String, String>> computeIfAbsentUnsupportedCaches = new ArrayList<>();

  private final List<AutoCloseable> resources = new ArrayList<>();

  @Before
  @SneakyThrows
  public void before() {
    // 内存实现：底层是 HashMap，不承诺线程安全；computeIfAbsent 走接口默认实现（非原子）。
    //
    // 注意 TreeMapKVCache 加不进来 —— 它是 SortedKVCache（只继承 KVCache），
    // 不是 ExpiredKVCache，没有带 ttl 的 set/setnx。
    ExpiredKVCache<String, String> hashCache = new ExpiredHashMapKVCache<String, String>() {
    };
    register(hashCache, false, true, false, 0);
    memoryCaches.add(hashCache);

    // 内存实现：底层是 ConcurrentHashMap，自身线程安全，且覆盖了 computeIfAbsent（原子）。
    // 3000 与 testThreadUnSafe 的反例同参，见 l1PerThread 的说明。
    ExpiredKVCache<String, String> concurrentHashMapCache = new ExpiredConcurrentHashMapKVCache<>();
    register(concurrentHashMapCache, true, true, true, 3000);
    memoryCaches.add(concurrentHashMapCache);

    // 四个 redis 实现：自身线程安全；做不到原子的 computeIfAbsent，于是明确拒绝。
    // L1 用例给 20（8 线程共 160 个 key）：公网 RTT 决定了上限，这一档看的是客户端侧不炸/不串号。
    RedisClient redisClient = RedisClientBaseTest.redisClient();
    resources.add(redisClient::shutdown);
    registerRedis(new LettuceRedisKVCache(redisClient));

    RedisClusterClient clusterClient = RedisClusterClientBaseTest.redisClusterClient();
    resources.add(clusterClient::shutdown);
    registerRedis(new LettuceRedisClusterKVCache(clusterClient));

    RedissonClient redissonClient = RedissonClientBaseTest.redissonClient();
    resources.add(redissonClient::shutdown);
    registerRedis(new RedissonRedisKVCache(redissonClient));

    LettuceConnectionFactory factory = StringRedisTemplateBaseTest.lettuceConnectionFactory();
    resources.add(factory::destroy);
    StringRedisTemplate template = StringRedisTemplateBaseTest.stringRedisTemplate(factory);
    registerRedis(new StringRedisTemplateRedisKVCache(template));
  }

  /**
   * 四个 redis 实现共有的性质一次登记完，免得在 {@link #before()} 里重复四遍：
   * 自身线程安全、拒绝 {@code computeIfAbsent}、L1 用例每线程 20 个 key（公网 RTT 决定）。
   */
  private void registerRedis(ExpiredKVCache<String, String> cache) {
    register(cache, true, false, false, 20);
    redisCaches.add(cache);
  }

  /**
   * 登记一个实现，顺便声明它的性质 —— 用例按性质挑组跑，不必在别处再算一遍。
   *
   * @param threadSafe               它自身是否承诺线程安全
   * @param computeIfAbsentSupported 它是否支持 {@code computeIfAbsent}
   *                                 （{@code false} 即"明确拒绝"，
   *                                 抛 {@link CacheUnsupportedOperationException}）
   * @param computeIfAbsentAtomic    它是否把 {@code computeIfAbsent} 做成了原子的（不支持时就无所谓了）
   * @param l1PerThread              L1 并发用例里每个线程写多少个独占 key；不承诺线程安全的传 0
   */
  private void register(ExpiredKVCache<String, String> cache, boolean threadSafe,
                        boolean computeIfAbsentSupported, boolean computeIfAbsentAtomic,
                        int l1PerThread) {
    caches.add(cache);
    if (threadSafe) {
      threadSafeCaches.add(cache);
      this.l1PerThread.put(cache, l1PerThread);
    }
    if (computeIfAbsentSupported) {
      computeIfAbsentSupportedCaches.add(cache);
      if (computeIfAbsentAtomic) {
        computeIfAbsentAtomicCaches.add(cache);
      } else {
        computeIfAbsentNonAtomicCaches.add(cache);
      }
    } else {
      computeIfAbsentUnsupportedCaches.add(cache);
    }
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
   * <p>
   * 之前 {@code ExpiredHashMapKVCache} 的实现是无条件先算再放进 map，
   * 命中缓存时业务侧的计算函数照样执行，代价白付。
   * <p>
   * 这里 TTL 给的是 1 分钟，覆盖的是"<b>还没到期</b>"那一半；真到期之后必须重算，
   * 见 {@link #testComputeIfAbsentRecomputesAfterExpiry()}。
   */
  @Test
  public void testComputeIfAbsentIsLazy() {
    for (ExpiredKVCache<String, String> cache : computeIfAbsentSupportedCaches) {
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
   * <b>到期之后 {@code computeIfAbsent} 会重算 —— "过期的 key 等同于没有值"在它身上同样成立。</b>
   * <p>
   * {@link #testComputeIfAbsentIsLazy()} 钉的是"命中就不算"，但那边 TTL 是 1 分钟，
   * 只覆盖"还没到期"那一半。真过了那一分钟再调，必须重算 —— 否则就是拿旧值糊弄调用方，
   * TTL 也就白设了。这里用 1 秒 TTL + 睡过它来钉，不依赖那一分钟。
   * <p>
   * 两个重载各一份：带 ttl 的，以及不带 ttl 的（后者遇到过期条目同样要重算，
   * 并顺手把它换成"永不过期"的新值）。
   * <p>
   * 只跑支持 {@code computeIfAbsent} 的内存实现 —— redis 那四个明确拒绝该方法。
   */
  @Test
  public void testComputeIfAbsentRecomputesAfterExpiry() throws Exception {
    for (ExpiredKVCache<String, String> cache : computeIfAbsentSupportedCaches) {
      String name = cache.getClass().getSimpleName();
      AtomicInteger invoked = new AtomicInteger();

      // 带 ttl 的重载：到期后重算
      String ttlKey = uniqueKey(name);
      Assert.assertEquals(name + ": 第一次应该计算", "v1",
              cache.computeIfAbsent(ttlKey, k -> "v" + invoked.incrementAndGet(), 1, TimeUnit.SECONDS));

      SleepUtils.sleep(TimeUnit.MILLISECONDS, 1200);

      Assert.assertEquals(name + ": 到期之后应该重算，不能把旧值糊弄回来", "v2",
              cache.computeIfAbsent(ttlKey, k -> "v" + invoked.incrementAndGet(), 1, TimeUnit.SECONDS));
      Assert.assertEquals(name + ": mappingFunction 应该被调用两次", 2, invoked.get());

      // 不带 ttl 的重载：同样要认出过期条目
      String plainKey = uniqueKey(name);
      Assert.assertEquals(name + ": 第一次应该计算", "v3",
              cache.computeIfAbsent(plainKey, k -> "v" + invoked.incrementAndGet(), 1, TimeUnit.SECONDS));

      SleepUtils.sleep(TimeUnit.MILLISECONDS, 1200);

      Assert.assertEquals(name + ": 不带 ttl 的重载也要重算", "v4",
              cache.computeIfAbsent(plainKey, k -> "v" + invoked.incrementAndGet()));
      Assert.assertEquals(name + ": mappingFunction 一共被调用四次", 4, invoked.get());

      cache.delete(ttlKey);
      cache.delete(plainKey);
    }
  }

  /**
   * computeIfAbsent 的计算结果为 null 时不应该写入 —— 否则又变成"存了个 null"，
   * containsKey 返回 true 而 get 返回 null，和 KVCache#set 的契约打架。
   */
  @Test
  public void testComputeIfAbsentDoesNotStoreNull() {
    for (ExpiredKVCache<String, String> cache : computeIfAbsentSupportedCaches) {
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
   * <b>L1 结构安全（正面断言）</b>：底层存储线程安全的实现，并发调用任意方法都必须满足
   * 「不抛异常、不损坏内部结构、不丢失已确认写入的数据」。
   * <p>
   * 与 {@link #testThreadUnSafe()} 共用同一个探针（{@link #concurrentL1Probe}），方向相反：
   * 那边断言 {@code lost != 0}（它达不到 L1），这边断言 {@code lost == 0}。
   * <b>反例那边能红，这条结论才有意义</b> —— 所以内存实现的规模必须与反例一致（8×3000），
   * 见 {@link #l1PerThread}。
   * <p>
   * redis 那四家的规模只能给小档（公网 RTT 决定的），它们在组里起的是<b>客户端侧冒烟</b>作用：
   * 共享连接 / 代理被并发调用时不炸、不串号。redis 服务端自身的并发正确性不归本组件保证。
   * <p>
   * 与 L2 的分界：本用例只压<b>单命令</b>的结构安全。{@code setnx} / {@code computeIfAbsent}
   * 的原子性分别由 {@link #testSetnxIsAtomic()}、{@link #testComputeIfAbsentIsAtomicUnderConcurrency()}
   * 负责 —— 原子性不是 L1 的必然结果。
   */
  @Test
  public void testThreadSafety() throws Exception {
    int threads = 8;

    for (ExpiredKVCache<String, String> cache : threadSafeCaches) {
      String name = cache.getClass().getSimpleName();
      int perThread = l1PerThread.get(cache);

      L1ProbeResult result = concurrentL1Probe(cache, uniqueKey(name), threads, perThread,
              this::isTransientServerError, 120);

      log.error("{}: L1 探针 {} 线程 × {} 个独占 key：校验 {} 个，丢失/写错 {} 个，异常 {} 个",
              name, threads, perThread, result.verified, result.lost, result.errors.size());
      for (Throwable error : result.errors) {
        log.error("{}", name, error);
      }

      Assert.assertEquals(name + ": L1 —— 已确认写入的数据不该丢失，也不该被换成别的值",
              0L, result.lost);
      Assert.assertTrue(name + ": L1 —— 并发调用不该抛异常（瞬时抖动已剔除）：" + result.errors,
              result.errors.isEmpty());
      Assert.assertTrue(name + ": 至少一半的 key 要被真正校验过，否则这个用例等于没测。verified="
              + result.verified, result.verified >= threads * perThread / 2);
    }
  }

  /**
   * <b>L1 的反例</b>：{@code ExpiredHashMapKVCache} 达不到 L1 —— 底层是普通 HashMap。
   * <p>
   * 并发写下不只是丢数据，还会因为桶结构损坏而<b>卡死</b>。实测 8 线程 × 3000 次写：
   * 10 轮里 9 轮丢数据（丢 5%~67%）、1 轮直接卡死 —— 正常轮次只要 5~22ms，
   * 所以卡死不是"太慢"，是真的卡住了。
   * <p>
   * 断言写成 {@code lost != 0}：丢数据（{@code lost > 0}，含"值被写错"）和卡死
   * （{@code lost == -1}）都算证明了它达不到 L1。它同时是 {@link #testThreadSafety()}
   * 的控制实验 —— 同一个探针、同一组参数在不安全的实现上能红，那条正例才有意义。
   * <p>
   * 反过来，如果哪天它变成 0，说明实现被换成了线程安全的，这个用例就该删掉。
   */
  @Test
  public void testThreadUnSafe() throws Exception {
    L1ProbeResult result = concurrentL1Probe(new ExpiredHashMapKVCache<String, String>() {
    }, uniqueKey("threadUnSafe"), 8, 3000, e -> false, 20);

    log.error("[ExpiredHashMapKVCache] 并发写 24000 个 key：{}",
            result.lost == -1 ? "卡死（20s 没跑完）" : ("丢了或写错了 " + result.lost + " 个"));

    Assert.assertTrue(
            "并发写 24000 个 key 应该丢数据 / 写错 / 卡死；如果这里真的 0，说明它已经变成线程安全的了",
            result.lost != 0);
  }

  /**
   * 对照组：包一层 {@link SynchronizedExpiredKVCache} 之后必须一条不丢、一个异常不抛。
   * <p>
   * 这就是 {@link cn.addenda.component.cache.KVCache} 上写的"底层不线程安全时怎么办"的答案。
   * 与 {@link #testThreadUnSafe()} 同参：底层没换，只加了一层锁。
   */
  @Test
  public void testSynchronizedWrapperIsThreadSafe() throws Exception {
    L1ProbeResult result = concurrentL1Probe(
            SynchronizedExpiredKVCache.<String, String>synchronize(
                    new ExpiredHashMapKVCache<String, String>() {
                    }), uniqueKey("synchronizedWrapper"), 8, 3000, e -> false, 20);

    Assert.assertEquals("SynchronizedExpiredKVCache 包装之后不应该丢任何数据", 0L, result.lost);
    Assert.assertTrue("包装层自己也不该抛异常：" + result.errors, result.errors.isEmpty());
  }

  /**
   * L1 的并发探针：{@code threads} 个线程各写 {@code perThread} 个独占 key，
   * <b>每个 key 只写一次、写自己的唯一值</b>（丢了下一轮不会补上）；同时在一个共享 key 上
   * 搅动 set / get / containsKey / delete —— L1 说的是"并发调用<b>任意</b>方法"。
   * 结束后逐个校验：这个 key 还在，且值没被换掉。
   * <p>
   * 三种失败信号正好对应 L1 的三条细则：
   * <ul>
   *   <li>{@code lost > 0}：已确认写入的数据丢了，或被写成了别的值（③）；</li>
   *   <li>{@code lost == -1}：超时，多半是内部结构被并发写坏、线程卡死（②）；</li>
   *   <li>{@code errors}：非瞬时异常（①）—— 由调用方决定是否断言为空。</li>
   * </ul>
   *
   * @param keyPrefix      本次运行必须唯一：redis 上的历史残留会让"丢了"看起来"还在"
   * @param ignorable      可以跳过的异常（redis 的瞬时抖动）。被跳过的 key 不计入 {@code lost}，
   *                       也不计入 {@code verified}；"至少一半校验过"那条断言负责兜住"全跳过了"
   * @param timeoutSeconds 等线程结束的上限；等不到即判为卡死，见 {@code lost == -1}
   */
  private L1ProbeResult concurrentL1Probe(ExpiredKVCache<String, String> cache, String keyPrefix,
                                          int threads, int perThread,
                                          Predicate<Throwable> ignorable, long timeoutSeconds)
          throws InterruptedException {
    L1ProbeResult result = new L1ProbeResult();
    String sharedKey = keyPrefix + ":shared";
    Set<String> legalSharedValues = new HashSet<>();
    for (int t = 0; t < threads; t++) {
      legalSharedValues.add("v" + t);
    }

    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);

    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      String ownValue = "v" + threadId;
      Thread thread = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < perThread; i++) {
            String key = keyPrefix + ":" + threadId + ":" + i;
            try {
              // 每个 key 只写一次：丢写不会被下一轮掩盖
              cache.set(key, ownValue + ":" + i, 1, TimeUnit.MINUTES);
            } catch (Throwable e) {
              if (ignorable.test(e)) {
                // 没写成：校验时跳过它，免得把环境抖动算成"丢了"
                result.skipped.add(key);
              } else {
                // 非瞬时：这个 key 不会落地，校验时会自行计入 lost
                result.errors.add(e);
              }
              continue;
            }

            // 共享 key 上搅动其余方法；读到的值必须是某个线程写过的，或 null
            try {
              cache.set(sharedKey, ownValue, 1, TimeUnit.MINUTES);
              String sharedRead = cache.get(sharedKey);
              if (sharedRead != null && !legalSharedValues.contains(sharedRead)) {
                result.errors.add(new AssertionError(
                        "共享 key 读到了从未写入的值：" + sharedRead));
              }
              cache.containsKey(sharedKey);
              cache.delete(sharedKey);
            } catch (Throwable e) {
              // 共享 key 只是搅动：瞬时抖动不影响自己的 key 是否落地
              if (!ignorable.test(e)) {
                result.errors.add(e);
              }
            }
          }
        } catch (Throwable e) {
          result.errors.add(e);
        } finally {
          done.countDown();
        }
      });
      thread.setDaemon(true);
      thread.start();
    }

    start.countDown();
    if (!done.await(timeoutSeconds, TimeUnit.SECONDS)) {
      result.lost = -1;
      return result;
    }

    // 并发结束后逐个校验：还在、且值没被换掉
    for (int t = 0; t < threads; t++) {
      for (int i = 0; i < perThread; i++) {
        String key = keyPrefix + ":" + t + ":" + i;
        if (result.skipped.contains(key)) {
          continue;
        }
        try {
          String actual = cache.get(key);
          result.verified++;
          if (!("v" + t + ":" + i).equals(actual)) {
            result.lost++;
          }
        } catch (Throwable e) {
          // 这次校验没做成：不计 verified，也不算丢失；非瞬时异常照样记下
          if (!ignorable.test(e)) {
            result.errors.add(e);
          }
        }
      }
    }
    return result;
  }

  /**
   * {@link #concurrentL1Probe} 的结果。三个字段分别对应 L1 的三条细则，见该方法的说明。
   */
  private static final class L1ProbeResult {
    /**
     * 丢失或被换成别的值的 key 数；<b>-1 表示超时</b>（多半是结构被写坏、线程卡死）
     */
    private long lost = 0;
    /**
     * 真正校验过的 key 数（不含因瞬时抖动没写成的）
     */
    private int verified = 0;
    /**
     * 非瞬时异常
     */
    private final List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
    /**
     * 因瞬时抖动没写成、因而不参与校验的 key
     */
    private final Set<String> skipped =
            Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  }

  /**
   * {@code computeIfAbsent} 在并发下<b>不是原子的</b> —— 这是接口契约层面的，不是实现缺陷。
   * <p>
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
   * <p>
   * 需要原子性的话由上层加锁 —— {@code CacheHelper} 就是这么做的
   * （{@code lockAllocator} + 令牌桶限流，见 {@code LettuceRedisCacheHelperTest.testConcurrentQueryHitsCache}）。
   * <p>
   * 同时验证：并发下<b>不抛异常、不死循环/死锁</b>（用带超时的 await 兜住）。
   * <p>
   * 只跑"支持 computeIfAbsent 但用默认实现"的那几个：覆盖成原子实现的断言正好相反
   * （见 {@link #testComputeIfAbsentIsAtomicUnderConcurrency()}），
   * 明确拒绝提供的那几个根本不能调（见 {@link #testComputeIfAbsentNotSupported()}）。
   */
  @Test
  public void testComputeIfAbsentIsNotAtomicUnderConcurrency() throws Exception {
    int threadCount = 8;

    for (ExpiredKVCache<String, String> cache : computeIfAbsentNonAtomicCaches) {
      String name = cache.getClass().getSimpleName();

      int invoked = concurrentComputeIfAbsent(cache, uniqueKey(name), threadCount);

      log.error("{}: {} 个线程并发 computeIfAbsent，mappingFunction 被调用了 {} 次",
              name, threadCount, invoked);

      Assert.assertTrue(name + ": 并发下 mappingFunction 应该被调用多次，说明 computeIfAbsent 不是原子的",
              invoked > 1);
    }
  }

  /**
   * <b>覆盖了 computeIfAbsent 的实现是原子的</b>：并发抢同一个 key，{@code mappingFunction} 只算一次。
   * <p>
   * 与 {@link #testComputeIfAbsentIsNotAtomicUnderConcurrency()} 是同一个场景的两组断言 ——
   * 那边 {@code invoked > 1}，这边 {@code invoked == 1}。谁进哪一组由
   * {@link #threadSafeCaches} / {@link #computeIfAbsentAtomicCaches} 两个登记表决定。
   * <p>
   * 目前只有内存实现做得到：{@link ExpiredConcurrentHashMapKVCache} 把整段 check-then-act
   * 收敛到 ConcurrentHashMap 的 {@code compute} 上。基于 redis 的实现做不到 ——
   * {@code mappingFunction} 是本地代码，服务端无法参与它的求值，两次往返之间必然有窗口；
   * 那类实现要原子性只能靠 {@code CacheHelper} 的 lockAllocator 加锁。
   */
  @Test
  public void testComputeIfAbsentIsAtomicUnderConcurrency() throws Exception {
    int threadCount = 8;

    for (ExpiredKVCache<String, String> cache : computeIfAbsentAtomicCaches) {
      String name = cache.getClass().getSimpleName();

      int invoked = concurrentComputeIfAbsent(cache, uniqueKey(name), threadCount);

      log.error("{}: {} 个线程并发 computeIfAbsent，mappingFunction 被调用了 {} 次",
              name, threadCount, invoked);

      Assert.assertEquals(name + ": 覆盖了 computeIfAbsent，并发下同一个 key 只应该算一次",
              1, invoked);
    }
  }

  /**
   * <b>包一层 {@link SynchronizedExpiredKVCache} 之后，带 TTL 的 computeIfAbsent 也是原子的。</b>
   * <p>
   * 这个重载由 {@link ExpiredKVCache} 的接口默认实现提供（get + set 两次委托），
   * 包装类必须把它<b>显式覆盖</b>才能整段持锁；漏掉覆盖的话，两次委托之间不持锁，
   * 并发下 {@code mappingFunction} 会被调用多次 —— 本用例就是钉这件事
   * （去掉 {@code SynchronizedExpiredKVCache} 里那个重载，这里会从 1 变成 8）。
   * <p>
   * 底层用 {@link ExpiredHashMapKVCache}：它自己的 computeIfAbsent 不是原子的，
   * 原子性完全来自包装层。
   */
  @Test
  public void testSynchronizedWrapperMakesTtlComputeIfAbsentAtomic() throws Exception {
    int threadCount = 8;

    ExpiredKVCache<String, String> cache = SynchronizedExpiredKVCache.synchronize(
            new ExpiredHashMapKVCache<String, String>() {
            });

    int invoked = concurrentComputeIfAbsent(cache, uniqueKey("SynchronizedExpiredKVCache"), threadCount);

    log.error("SynchronizedExpiredKVCache: {} 个线程并发 computeIfAbsent(ttl)，mappingFunction 被调用了 {} 次",
            threadCount, invoked);

    Assert.assertEquals("整段 check-then-act 在同一把锁里，mappingFunction 只应该被算一次",
            1, invoked);
  }

  /**
   * 并发抢同一个 key 调带 TTL 的 {@code computeIfAbsent}，返回 {@code mappingFunction} 被调用的次数。
   * <p>
   * {@code mappingFunction} 里 sleep 100ms 把 check 与 act 之间的窗口拉大，让并发暴露出来；
   * 带超时的 await 保证真的死循环/死锁时是断言失败，而不是把测试挂住。
   */
  private int concurrentComputeIfAbsent(ExpiredKVCache<String, String> cache, String key,
                                        int threadCount) throws Exception {
    String name = cache.getClass().getSimpleName();
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
    Assert.assertTrue(name + ": 不应该死循环或死锁", done.await(30, TimeUnit.SECONDS));

    for (Throwable failure : failures) {
      log.error("{}", name, failure);
    }
    Assert.assertTrue(name + ": 并发下不应该抛异常", failures.isEmpty());

    cache.delete(key);
    return invoked.get();
  }

  /**
   * <b>TTL 边界的契约矩阵：六个实现 × 各档 timeout，期望口径必须一致。</b>
   * <table border="1">
   *   <caption>TTL 取值与期望</caption>
   *   <tr><th>timeout</th><th>期望</th><th>为什么</th></tr>
   *   <tr><td>{@code 0ms} / {@code -1ms}</td><td>立即过期，且<b>不抛异常</b></td>
   *       <td>非正表示"立即过期"；redis 拒绝非正，所以实现必须统一取整为 1ms</td></tr>
   *   <tr><td>{@code 999µs}</td><td>立即过期</td><td>亚毫秒换算成毫秒后是 0，落在非正那一档</td></tr>
   *   <tr><td>{@code 1ms}</td><td>只断言"不抛异常"</td>
   *       <td>1ms 与"写入到读取之间耗时"同量级，读到或读不到都合法，不能断言</td></tr>
   *   <tr><td>{@code 5s} / {@code 60s}</td><td>读得到</td><td>足够长，任何实现上都还没到期</td></tr>
   * </table>
   * <b>矩阵里刻意没有 {@code Long.MAX_VALUE}</b>：这是一处<b>合法差异</b>（过期由服务端说了算），
   * 两侧各自单独覆盖 —— 内存侧 {@link #testHugeTtlSaturatesOnInMemoryImpls()}，
   * redis 侧 {@link #testHugeTtlIsRejectedUniformlyOnRedisImpls()}。
   * <p>
   * 差异的精确边界不是"数值大"，而是<b>服务端算 {@code 当前时刻 + TTL} 时 64 位溢出</b>：
   * <ul>
   *   <li>内存实现：{@link ExpiredKVCache#deadline} 溢出时饱和到 {@link Long#MAX_VALUE}，
   *       即"永不过期"，写入成功；</li>
   *   <li>redis 实现：参数下发后服务端回 {@code ERR invalid expire time}，实现把存储层的拒绝
   *       归一成 {@link CacheIllegalArgumentException}（组件不允许该参数穿透），<b>且不写入</b>
   *       （无半写状态）。</li>
   * </ul>
   * 所以 {@code Long.MAX_VALUE} 毫秒/天都会被拒绝（{@code ttlMillis} 饱和到
   * {@link Long#MAX_VALUE}）；而 {@code Long.MAX_VALUE} <b>微秒</b>换算后只有
   * 9.2e15 ms（约 29 万年），服务端接受 —— 这也是为什么矩阵不能按"输入有多大"来分档。
   * <p>
   * 这条用例的前身是"只测非正 TTL"的单点断言（{@link #testNonPositiveTtlIsImmediatelyExpiredOnAllImpls()}）；
   * 那个仍然保留，因为它对"不抛异常"这一点给的失败信息更直接。
   */
  @Test
  public void testTtlBoundaryMatrix() throws Exception {
    Object[][] matrix = {
            {"0ms", 0L, TimeUnit.MILLISECONDS, EXPIRED},
            {"-1ms", -1L, TimeUnit.MILLISECONDS, EXPIRED},
            {"999µs（换算后为 0）", 999L, TimeUnit.MICROSECONDS, EXPIRED},
            {"1ms", 1L, TimeUnit.MILLISECONDS, NO_THROW_ONLY},
            {"5s", 5L, TimeUnit.SECONDS, READABLE},
            {"60s", 60L, TimeUnit.SECONDS, READABLE},
    };

    for (Object[] row : matrix) {
      String desc = (String) row[0];
      long timeout = (Long) row[1];
      TimeUnit unit = (TimeUnit) row[2];
      int expect = (Integer) row[3];

      for (ExpiredKVCache<String, String> cache : caches) {
        String name = cache.getClass().getSimpleName() + " / " + desc;
        String key = uniqueKey(name);

        try {
          cache.set(key, "v", timeout, unit);
        } catch (RuntimeException e) {
          Assert.fail(name + "：不该抛异常（TTL 换算没走统一下限？），实际 "
                  + e.getClass().getSimpleName() + ": " + e.getMessage());
        }

        if (expect == EXPIRED) {
          // 非正 TTL 取整为 1ms，睡过 2ms 才确定跨过到期时刻
          SleepUtils.sleep(TimeUnit.MILLISECONDS, 5);
          Assert.assertNull(name + "：应立即过期", cache.get(key));
          Assert.assertFalse(name + "：应立即查不到", cache.containsKey(key));
        } else if (expect == READABLE) {
          Assert.assertEquals(name + "：应读得到", "v", cache.get(key));
        }
        // NO_THROW_ONLY：只要求上面那次 set 没抛

        cache.delete(key);
      }
    }
  }

  private static final int EXPIRED = 0;
  private static final int NO_THROW_ONLY = 1;
  private static final int READABLE = 2;

  /**
   * <b>null key 的契约矩阵：六个实现 × 带 TTL 的 {@code computeIfAbsent}。</b>
   * <p>
   * 期望分两组，正是本组件对"参数校验"与"明确拒绝"这两件事的取舍：
   * <ul>
   *   <li><b>支持该操作的实现</b>：{@code key} 为 null 一律 {@link CacheIllegalArgumentException}，
   *       且 {@code computeIfAbsent(null, null, ttl)} 报的也必须是 key 的问题，
   *       <b>不是</b> {@link NullPointerException} —— 两个参数都错时，key 的错优先；</li>
   *   <li><b>明确拒绝该操作的实现</b>（四个 redis 实现）：直接抛
   *       {@link CacheUnsupportedOperationException}，<b>不校验参数</b> ——
   *       整个操作都不支持时，"null 该怎么处理"不再适用。
   *       同一个口径也适用于 {@code LruKKVCache}（不在本组，它属于 {@code KVCache} 那一层）：
   *       它的 {@code setnx} / {@code computeIfAbsent} 同样不校验参数。
   *       唯一保留"先校验 key"的明确拒绝场景是依赖服务端能力的 {@code remove}
   *       （{@code GETDEL} 要求较新的服务端版本），见 {@link KVCache} 的异常口径。</li>
   * </ul>
   */
  @Test
  public void testNullKeyMatrixOnTtlComputeIfAbsent() {
    List<ExpiredKVCache<String, String>> supported = computeIfAbsentSupportedCaches;

    for (ExpiredKVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();

      if (supported.contains(cache)) {
        CacheAssertions.assertRejected(name + ": computeIfAbsent(null, fn, ttl)",
                () -> cache.computeIfAbsent(null, k -> "v", 1, TimeUnit.MINUTES));
        CacheAssertions.assertRejected(name + ": computeIfAbsent(null, null, ttl) 报 key 的问题，不是 NPE",
                () -> cache.computeIfAbsent(null, null, 1, TimeUnit.MINUTES));
      } else {
        CacheAssertions.assertNotSupported(
                name + ": 拒绝组直接抛 CacheUnsupportedOperationException，不校验参数",
                () -> cache.computeIfAbsent(null, k -> "v", 1, TimeUnit.MINUTES));
        CacheAssertions.assertNotSupported(
                name + ": 两个参数都 null 也是 CacheUnsupportedOperationException",
                () -> cache.computeIfAbsent(null, null, 1, TimeUnit.MINUTES));
      }
    }
  }

  /**
   * <b>redis 那四个实现明确拒绝提供 computeIfAbsent</b>：两个重载都抛
   * {@link CacheUnsupportedOperationException}。
   * <p>
   * 这是它们能声明 L2 的前提 —— {@code computeIfAbsent} 的语义是原子的 check-then-act，
   * 而 redis 的 get 与 set 是两次往返，中间必然有窗口（{@code mappingFunction} 是本地代码，
   * 服务端不参与它的求值）。宁可拒绝，也不给一个并发下会重复计算、互相覆盖的版本。
   * <p>
   * 需要该语义时走 {@code CacheHelper} 的 {@code queryWithPpf}（内部用 lockAllocator 加锁）。
   * <p>
   * 另外一个口径：拒绝组<b>不校验参数</b>，{@code computeIfAbsent(null, null, ttl)} 抛的也是
   * {@link CacheUnsupportedOperationException} 而不是 {@link CacheIllegalArgumentException}/NPE ——
   * 整个操作都不支持时，"null 该怎么处理"不再适用。
   * 见 {@link #testNullKeyMatrixOnTtlComputeIfAbsent()}。
   */
  @Test
  public void testComputeIfAbsentNotSupported() {
    for (ExpiredKVCache<String, String> cache : computeIfAbsentUnsupportedCaches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      CacheAssertions.assertNotSupported(name + ": computeIfAbsent",
              () -> cache.computeIfAbsent(key, k -> "v"));
      CacheAssertions.assertNotSupported(name + ": computeIfAbsent(ttl)",
              () -> cache.computeIfAbsent(key, k -> "v", 1, TimeUnit.MINUTES));
    }
  }

  /**
   * setnx：key 不存在才写入，返回是否写入成功。
   */
  @Test
  public void testSetnx() {
    for (ExpiredKVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      Assert.assertTrue(name + ": key 不存在时应该写入成功", cache.setnx(key, "v1"));
      Assert.assertEquals(name + ": 值应该写进去了", "v1", cache.get(key));

      Assert.assertFalse(name + ": key 已存在时应该写入失败", cache.setnx(key, "v2"));
      Assert.assertEquals(name + ": 已存在的值不应该被覆盖", "v1", cache.get(key));

      cache.delete(key);
      Assert.assertTrue(name + ": 删掉之后又能写了", cache.setnx(key, "v3"));
      Assert.assertEquals("v3", cache.get(key));

      cache.delete(key);
    }
  }

  /**
   * setnx 带 ttl：写入成功，且重复调用不会覆盖已有的值。
   * <p>
   * "重复调用"覆盖<b>两个重载</b>：同重载（再带 TTL 写）与跨重载（不带 TTL 写）。两者在实现里
   * 走不同路径（redis 上是 {@code SET NX PX} 与 {@code SET NX}），nx 语义不应随重载变化。
   * <p>
   * TTL 刻意给大（1 分钟）—— 本用例验证的是"不覆盖"，与到期无关。
   * 到期验证拆到 {@link #testSetnxWithTimeoutExpires()}。
   */
  @Test
  public void testSetnxWithTimeoutDoesNotOverwrite() {
    for (ExpiredKVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      Assert.assertTrue(name + ": 应该写入成功", cache.setnx(key, "v1", 1, TimeUnit.MINUTES));
      Assert.assertFalse(name + ": 已存在就不该再写", cache.setnx(key, "v2", 1, TimeUnit.MINUTES));
      Assert.assertFalse(name + ": 已存在就不该再写", cache.setnx(key, "v3"));
      Assert.assertEquals(name + ": 值不应该被覆盖", "v1", cache.get(key));

      cache.delete(key);
    }
  }

  /**
   * setnx 带 ttl：到点照常过期。
   * <p>
   * 这两件事原先挤在同一个用例里、TTL 只有 1 秒，于是"写 → 断言值还在"那两步只要被
   * 网络上的一次抖动拖过 1 秒，值就已经过期 —— 断言以一个看不出原因的形式假红
   * （真因是延迟，不是逻辑）。
   * <p>
   * 拆开之后本用例<b>不断言"写完立刻读得到"</b>，只断言"最终读不到"：到期时刻由服务端
   * 在写入时确定，写完再睡过 TTL，它一定已经过期，这两步都不受往返延迟影响。
   */
  @Test
  public void testSetnxWithTimeoutExpires() throws Exception {
    for (ExpiredKVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      Assert.assertTrue(name + ": 应该写入成功", cache.setnx(key, "v1", 1, TimeUnit.SECONDS));

      SleepUtils.sleep(TimeUnit.MILLISECONDS, 1200);
      Assert.assertNull(name + ": ttl 到了应该过期", cache.get(key));
      Assert.assertFalse(name + ": ttl 到了应该过期", cache.containsKey(key));

      cache.delete(key);
    }
  }

  /**
   * <b>set 和 setnx 对 null 的处理必须一致</b> —— value 为 null 在两边都表示"没有值"：
   * set 是删除，setnx 是无事可做（不写入、返回 false），都不抛异常。
   * <p>
   * 这样调用方才写得出不特判 null 的泛型代码：
   * <pre>
   * cache.set(key, serialize(obj));
   * cache.setnx(key, serialize(obj));
   * </pre>
   */
  @Test
  public void testNullValueMeansNoValue() {
    for (ExpiredKVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      // key 不存在时
      cache.set(key, null);
      Assert.assertFalse(name + ": set(key, null) 之后不应该存在", cache.containsKey(key));
      Assert.assertFalse(name + ": setnx(key, null) 应该返回 false", cache.setnx(key, null));
      Assert.assertFalse(name + ": setnx(key, null) 不应该写入", cache.containsKey(key));
      Assert.assertFalse(name + ": setnx(key, null, ttl) 应该返回 false",
              cache.setnx(key, null, 1, TimeUnit.MINUTES));
      Assert.assertFalse(name + ": setnx(key, null, ttl) 不应该写入", cache.containsKey(key));

      // key 已存在时，setnx 的 null 不能把已有的值弄丢
      cache.set(key, "v");
      Assert.assertFalse(name + ": setnx(key, null) 应该返回 false", cache.setnx(key, null));
      Assert.assertEquals(name + ": setnx(key, null) 不应该动已有值", "v", cache.get(key));

      cache.delete(key);
    }
  }

  /**
   * <b>非 ASCII 的 value 要原样往返。</b>
   * <p>
   * 各家的编码路径完全不同（lettuce 的 StringCodec、redisson 的 Kryo5、spring 的
   * StringRedisSerializer、内存实现根本不编码），只要有一家默认字符集不是 UTF-8，
   * 表现就是"写进去、读出来是乱码"，而且不报任何错。
   * <p>
   * 这条覆盖原先只存在于 {@code TestRedissonRedisKVCache} 的检查 —— 那边只测了 redisson，
   * 而且断言走的是裸 {@code RBucket}、根本没经过 KVCache；放到这里才对全部实现生效。
   */
  @Test
  public void testNonAsciiValueRoundTrips() {
    String[] values = {"占", "中文值", "emoji 😀", "mixed 混合 123"};
    for (ExpiredKVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      for (String value : values) {
        String key = uniqueKey(name);
        cache.set(key, value);
        Assert.assertEquals(name + ": 非 ASCII 值要原样往返[" + value + "]", value, cache.get(key));
        cache.delete(key);
      }
    }
  }

  /**
   * key 为 null 一律拒绝 —— 和 value 不同，null key 没有语义，纯粹是调用方笔误。
   * <p>
   * 不统一拒绝的话各实现表现各异：HashMap 收下、TreeMap 抛 NPE、redis 编码成空 key，
   * 调用方看到的是三个不同的 bug。
   */
  @Test
  public void testNullKeyRejected() {
    for (ExpiredKVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();

      CacheAssertions.assertRejected(name + ": set", () -> cache.set(null, "v"));
      CacheAssertions.assertRejected(name + ": set(ttl)", () -> cache.set(null, "v", 1, TimeUnit.MINUTES));
      CacheAssertions.assertRejected(name + ": setnx", () -> cache.setnx(null, "v"));
      CacheAssertions.assertRejected(name + ": setnx(ttl)", () -> cache.setnx(null, "v", 1, TimeUnit.MINUTES));
      CacheAssertions.assertRejected(name + ": get", () -> cache.get(null));
      CacheAssertions.assertRejected(name + ": containsKey", () -> cache.containsKey(null));
      CacheAssertions.assertRejected(name + ": delete", () -> cache.delete(null));
      CacheAssertions.assertRejected(name + ": remove", () -> cache.remove(null));
    }
  }

  /**
   * <b>remove = 取值并删除</b>：返回值就是被删掉的那个值，key 不存在时返回 null。
   * <p>
   * 接口的默认实现是 get + delete 两步；redis 那四个（单命令 {@code GETDEL}）与
   * {@link ExpiredConcurrentHashMapKVCache}（单操作 {@code map.remove}）覆盖成了原子的版本。
   * 单线程下两种实现语义一致，并发下的差别（默认实现可能把并发写入的新值删掉、
   * 却返回先前读到的旧值）钉在 {@code KVCacheContractTest#testDefaultRemoveIsNotAtomic}。
   */
  @Test
  public void testRemove() {
    for (ExpiredKVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      cache.set(key, "v");
      Assert.assertEquals(name + ": remove 应该返回被删掉的值", "v", cache.remove(key));
      Assert.assertFalse(name + ": 删掉之后 key 应该不存在", cache.containsKey(key));
      Assert.assertNull(name + ": 删掉之后应该读不到", cache.get(key));
      Assert.assertNull(name + ": key 不存在时 remove 应该返回 null", cache.remove(key));
    }
  }

  /**
   * <b>setnx 是原子的</b>：并发抢同一个 key，只能有一个成功。
   * <p>
   * 这正是它和 {@link cn.addenda.component.cache.KVCache#computeIfAbsent} 的区别 ——
   * 后者（接口默认的 computeIfAbsent）存在非原子的 check-then-act 问题，并发下每个线程都会调一次 mappingFunction
   * （见 {@link #testComputeIfAbsentIsNotAtomicUnderConcurrency}）。
   * <p>
   * 顺带验证：并发下不抛异常、不死循环/死锁。
   */
  @Test
  public void testSetnxIsAtomic() throws Exception {
    int threadCount = 8;

    for (ExpiredKVCache<String, String> cache : threadSafeCaches) {
      String name = cache.getClass().getSimpleName();
      int successCount = concurrentSetnx(cache, uniqueKey(name), threadCount, true);

      log.error("{}: {} 个线程并发 setnx 同一个 key，成功了 {} 个",
              name, threadCount, successCount);

      Assert.assertEquals(name + ": setnx 是原子的，只能有一个线程抢到", 1, successCount);
    }
  }

  /**
   * <b>不带 TTL 的 setnx 同样是原子的</b>。
   * <p>
   * 与 {@link #testSetnxIsAtomic()} 是同一个语义的两个重载，但底层走的是不同命令
   * （redis 上是 {@code SET NX} 与 {@code SET NX PX}），所以分开各钉一遍。
   */
  @Test
  public void testSetnxWithoutTtlIsAtomic() throws Exception {
    int threadCount = 8;

    for (ExpiredKVCache<String, String> cache : threadSafeCaches) {
      String name = cache.getClass().getSimpleName();
      int successCount = concurrentSetnx(cache, uniqueKey(name), threadCount, false);

      log.error("{}: {} 个线程并发 setnx（不带 ttl）同一个 key，成功了 {} 个",
              name, threadCount, successCount);

      Assert.assertEquals(name + ": setnx 是原子的，只能有一个线程抢到", 1, successCount);
    }
  }

  /**
   * 并发抢同一个 key 调 {@code setnx}，返回抢到的线程数。
   * <p>
   * 顺带验证：并发下不抛异常、不死循环/死锁。
   */
  private int concurrentSetnx(ExpiredKVCache<String, String> cache, String key, int threadCount,
                              boolean withTtl) throws Exception {
    String name = cache.getClass().getSimpleName();
    AtomicInteger successCount = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);
    List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

    for (int t = 0; t < threadCount; t++) {
      final int threadId = t;
      Thread thread = new Thread(() -> {
        try {
          start.await();
          boolean written = withTtl
                  ? cache.setnx(key, "v" + threadId, 1, TimeUnit.MINUTES)
                  : cache.setnx(key, "v" + threadId);
          if (written) {
            successCount.incrementAndGet();
          }
        } catch (Throwable e) {
          if (!isTransientServerError(e)) {
            failures.add(e);
          }
        } finally {
          done.countDown();
        }
      });
      thread.setDaemon(true);
      thread.start();
    }

    start.countDown();
    Assert.assertTrue(name + ": 不应该死循环或死锁", done.await(30, TimeUnit.SECONDS));

    for (Throwable failure : failures) {
      log.error("{}", name, failure);
    }
    Assert.assertTrue(name + ": 并发下不应该抛异常", failures.isEmpty());

    cache.delete(key);
    return successCount.get();
  }

  /**
   * <b>过期即不存在</b>：过了 TTL 之后，<b>所有实现的口径必须一致</b> ——
   * {@code get} 读不到、{@code containsKey} 返回 false、{@code remove} 返回 null、
   * {@code delete} 返回 false（它没删掉"存在的数据"）、两个 {@code setnx} 都把过期条目当成不存在。
   * <p>
   * 全部实现一起跑（{@code caches}）：任何一家掉队本用例都会红。历史上
   * {@link ExpiredHashMapKVCache} 就是掉队的那一家 —— 它的 {@code containsKey} 是裸的
   * map 查询、{@code remove} 不看到期时刻、不带 TTL 的 {@code setnx} 用裸的
   * {@code putIfAbsent}，三点都与其余实现相反，且与它<b>自己的</b>带 TTL 重载相反。
   * <p>
   * 每个观察点用独立的 key：{@code get} / {@code containsKey} 会顺手清掉过期条目，
   * 共用 key 会互相干扰。
   * <p>
   * 写入与断言分两段、中间只睡一次：逐个实现"写→睡→断言"要多付 N 次 TTL 的等待。
   * 两段之间靠 {@link ExpiredKeys} 关联，key 按用途命名而不是按下标记。
   * <p>
   * TTL 取 1 秒而不是更短：这条链路上有远程 redis，取值太紧会在"确认写入成功"那一步压线。
   */
  @Test
  public void testExpiredEntrySemanticsAcrossImpls() throws Exception {
    Map<ExpiredKVCache<String, String>, ExpiredKeys> pending = new LinkedHashMap<>();
    for (ExpiredKVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      ExpiredKeys keys = new ExpiredKeys(
              uniqueKey(name) + ":get",
              uniqueKey(name) + ":containsKey",
              uniqueKey(name) + ":remove",
              uniqueKey(name) + ":delete",
              uniqueKey(name) + ":setnx",
              uniqueKey(name) + ":setnxTtl");
      for (String key : keys.all()) {
        cache.set(key, "v", 1, TimeUnit.SECONDS);
      }
      pending.put(cache, keys);
    }

    SleepUtils.sleep(TimeUnit.MILLISECONDS, 1200);

    for (Map.Entry<ExpiredKVCache<String, String>, ExpiredKeys> entry : pending.entrySet()) {
      ExpiredKVCache<String, String> cache = entry.getKey();
      String name = cache.getClass().getSimpleName();
      ExpiredKeys keys = entry.getValue();

      Assert.assertNull(name + ": 过期之后读不到", cache.get(keys.forGet));
      Assert.assertFalse(name + ": 过期之后查不到（不先 get 也一样）",
              cache.containsKey(keys.forContainsKey));
      Assert.assertNull(name + ": remove 对过期条目返回 null", cache.remove(keys.forRemove));

      Assert.assertFalse(name + ": delete 对过期条目返回 false —— 它没删掉'存在的数据'",
              cache.delete(keys.forDelete));
      Assert.assertFalse(name + ": 而且过期条目已被物理清掉", cache.containsKey(keys.forDelete));

      Assert.assertTrue(name + ": 不带 TTL 的 setnx 把过期条目当成不存在",
              cache.setnx(keys.forSetnx, "new"));
      Assert.assertEquals(name + ": 写进去的是新值", "new", cache.get(keys.forSetnx));
      Assert.assertTrue(name + ": 带 TTL 的 setnx 同样视为不存在",
              cache.setnx(keys.forSetnxTtl, "new", 1, TimeUnit.MINUTES));
      Assert.assertEquals(name + ": 带 TTL 的 setnx 写进去的同样可读",
              "new", cache.get(keys.forSetnxTtl));

      cache.delete(keys.forSetnx);
      cache.delete(keys.forSetnxTtl);
    }
  }

  /**
   * <b>超长 TTL 必须饱和成"永不过期"，而不是溢出成"立即过期"</b>。
   * <p>
   * {@code now + toMillis(timeout)} 会溢出：{@code Long.MAX_VALUE} 毫秒这类"近似永不过期"
   * 的入参让结果回绕成负数，条目一写进去就被判为已过期 —— 语义的最坏反转，且不报任何错。
   * <p>
   * 只跑 {@link #memoryCaches}：四个基于 redis 的实现把 TTL 交给服务端，超大值会被服务端
   * 直接拒绝，口径本来就不同 —— 那一侧见 {@link #testHugeTtlIsRejectedUniformlyOnRedisImpls()}。
   */
  @Test
  public void testHugeTtlSaturatesOnInMemoryImpls() {
    for (ExpiredKVCache<String, String> cache : memoryCaches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      cache.set(key, "v", Long.MAX_VALUE, TimeUnit.MILLISECONDS);
      Assert.assertNotNull(name + ": Long.MAX_VALUE 毫秒应该被当成永不过期", cache.get(key));
      cache.delete(key);

      cache.set(key, "v", Long.MAX_VALUE, TimeUnit.DAYS);
      Assert.assertNotNull(name + ": 以天为单位的饱和值同样", cache.get(key));
      cache.delete(key);

      Assert.assertTrue(name + ": 带 TTL 的 setnx 走同一条饱和路径",
              cache.setnx(key, "v", Long.MAX_VALUE, TimeUnit.MILLISECONDS));
      Assert.assertNotNull(name + ": setnx 写进去的也该读得到", cache.get(key));
      cache.delete(key);
    }
  }

  /**
   * <b>超大 TTL 在四个 redis 实现上统一拒绝成 {@link CacheIllegalArgumentException}，且不写入。</b>
   * <p>
   * 这条路径<b>不在入口校验</b>：参数会先下发给存储层，服务端算 {@code 当前时刻 + TTL} 时
   * 64 位溢出，回 {@code ERR invalid expire time in psetex/set}；四个客户端抛出的异常类型
   * 各不相同（lettuce / redisson / spring-data-redis 三套），由 {@code RedisCall} 把存储层的
   * 拒绝归一成组件的 {@link CacheIllegalArgumentException}，原始异常挂在 {@code cause} 上。
   * 对调用方而言，结论与入口校验一样：组件不允许这个参数穿透到存储层。
   * <p>
   * 分界的精确描述见 {@link #testTtlBoundaryMatrix()}；内存侧由
   * {@link #testHugeTtlSaturatesOnInMemoryImpls()} 覆盖。
   * <p>
   * 同时钉住"拒绝之后不写入"：校验发生在写入之前，不该出现半写状态。
   */
  @Test
  public void testHugeTtlIsRejectedUniformlyOnRedisImpls() {
    for (ExpiredKVCache<String, String> cache : redisCaches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      assertInvalidExpireTime(name + ": set",
              () -> cache.set(key, "v", Long.MAX_VALUE, TimeUnit.MILLISECONDS));
      Assert.assertFalse(name + ": 被拒绝之后不该写入", cache.containsKey(key));

      assertInvalidExpireTime(name + ": setnx",
              () -> cache.setnx(key, "v", Long.MAX_VALUE, TimeUnit.MILLISECONDS));
      Assert.assertFalse(name + ": setnx 同样不该写入", cache.containsKey(key));
    }
  }

  /**
   * 断言抛的是统一的 {@link CacheIllegalArgumentException}：客户端原始异常挂在 cause 上，
   * 消息里带上换算后的毫秒值（否则调用方分不清是 TTL 的问题还是别的问题）。
   */
  private void assertInvalidExpireTime(String what, Runnable invocation) {
    try {
      invocation.run();
      Assert.fail(what + "：超大 TTL 应该被组件拒绝（允许由存储层拒绝后归一）");
    } catch (CacheIllegalArgumentException expected) {
      Assert.assertNotNull(what + "：客户端原始异常应挂在 cause 上", expected.getCause());
      Assert.assertTrue(what + ": 消息里要带上换算后的毫秒值：" + expected.getMessage(),
              expected.getMessage().contains(String.valueOf(Long.MAX_VALUE)));
    }
  }

  /**
   * <b>非正 TTL 在六个实现上表现一致：不抛异常，且值立即过期。</b>
   * <p>
   * 这条以前是红的（在 redisson 与 spring-data-redis 上）：它们把非正 timeout 原样下发，
   * 服务端回一句 {@code ERR invalid expire time in psetex/set}，客户端包一层异常抛出来；
   * 而两个 lettuce 实现各自取整成 1ms、两个内存实现让到期时刻落在当下 ——
   * 同一份 {@code timeout = 0} 的配置，一半实现"立即过期"、一半直接报错。
   * 现在六个实现统一走 {@link ExpiredKVCache#ttlMillis}。
   * <p>
   * 断言分两段：<b>调用不得抛异常</b>（这才是旧行为真正违反的那条），
   * 以及睡过 1ms 之后读不到（1ms 是取整后的 TTL，同一毫秒内可能仍读得到，所以要睡）。
   */
  @Test
  public void testNonPositiveTtlIsImmediatelyExpiredOnAllImpls() throws Exception {
    for (ExpiredKVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();

      String zero = uniqueKey(name);
      String negative = uniqueKey(name);
      String nxZero = uniqueKey(name);

      try {
        cache.set(zero, "v", 0L, TimeUnit.MILLISECONDS);
        cache.set(negative, "v", -1L, TimeUnit.MILLISECONDS);
        cache.setnx(nxZero, "v", 0L, TimeUnit.MILLISECONDS);
      } catch (RuntimeException e) {
        Assert.fail(name + ": 非正 TTL 不该抛异常（服务端拒绝说明没走 ttlMillis 换算），实际 "
                + e.getClass().getSimpleName() + ": " + e.getMessage());
      }

      SleepUtils.sleep(TimeUnit.MILLISECONDS, 30);

      Assert.assertNull(name + ": timeout=0 应已过期", cache.get(zero));
      Assert.assertFalse(name + ": timeout=0 应查不到", cache.containsKey(zero));
      Assert.assertNull(name + ": timeout=-1 应已过期", cache.get(negative));
      Assert.assertNull(name + ": setnx 的 timeout=0 同样立即过期", cache.get(nxZero));

      // 过期条目等同于不存在：不该把新写入挡在门外
      Assert.assertTrue(name + ": 过期的旧值不该挡住 setnx",
              cache.setnx(zero, "new", 1, TimeUnit.MINUTES));
      Assert.assertEquals(name + ": 新值要真的写进去", "new", cache.get(zero));

      cache.delete(zero);
      cache.delete(negative);
      cache.delete(nxZero);
    }
  }

  /**
   * 判断是不是"服务器/网络抖动"这类环境问题，而不是客户端线程安全问题。
   * <p>
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
    return "ExpiredKVCacheContractTest:" + implName + ":" + UUID.randomUUID().toString().replace("-", "");
  }

  /**
   * {@link #testExpiredEntrySemanticsAcrossImpls()} 的六个观察点。
   * <p>
   * 每个观察点必须用独立的 key：{@code get} / {@code containsKey} 会顺手清掉过期条目，
   * 共用 key 会让后面的断言测不到"它自己是否把过期当不存在"。
   * <p>
   * 按用途命名而不是按下标：写入阶段与断言阶段隔着一次睡眠，下标会让两段的对应关系
   * 只能靠记忆维护。
   */
  private static final class ExpiredKeys {
    private final String forGet;
    private final String forContainsKey;
    private final String forRemove;
    private final String forDelete;
    private final String forSetnx;
    private final String forSetnxTtl;

    private ExpiredKeys(String forGet, String forContainsKey, String forRemove,
                        String forDelete, String forSetnx, String forSetnxTtl) {
      this.forGet = forGet;
      this.forContainsKey = forContainsKey;
      this.forRemove = forRemove;
      this.forDelete = forDelete;
      this.forSetnx = forSetnx;
      this.forSetnxTtl = forSetnxTtl;
    }

    /**
     * 全部 key，供写入阶段统一遍历。
     */
    private String[] all() {
      return new String[]{forGet, forContainsKey, forRemove, forDelete, forSetnx, forSetnxTtl};
    }
  }

}
