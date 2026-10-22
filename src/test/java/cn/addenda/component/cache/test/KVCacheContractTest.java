package cn.addenda.component.cache.test;

import cn.addenda.component.base.util.SleepUtils;
import cn.addenda.component.cache.ConcurrentHashMapKVCache;
import cn.addenda.component.cache.ExpiredConcurrentHashMapKVCache;
import cn.addenda.component.cache.ExpiredHashMapKVCache;
import cn.addenda.component.cache.HashMapKVCache;
import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.TreeMapKVCache;
import cn.addenda.component.cache.SynchronizedExpiredKVCache;
import cn.addenda.component.cache.SynchronizedKVCache;
import cn.addenda.component.cache.SynchronizedSortedKVCache;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link KVCache}（不带 ttl 的那一层）的契约测试。
 * <p>
 * 和 {@link ExpiredKVCacheContractTest} 分开的原因：那边测的是 {@code ExpiredKVCache}，
 * 纯 {@code KVCache} 的实现（比如 {@link TreeMapKVCache}）没有带 ttl 的方法，进不了那边的列表。
 * <p>
 * 每次 {@code before()} 都会覆盖<b>九个</b>实例：{@link HashMapKVCache}、{@link TreeMapKVCache}、
 * {@link ConcurrentHashMapKVCache}，包了 {@link SynchronizedKVCache} 的 HashMap / TreeMap
 * 与包了 {@link SynchronizedSortedKVCache} 的 TreeMap 各一个，外加三个带 TTL 的实现
 * （见 {@code before()} 里那段说明 —— 不走 TTL 入口时它们就是普通的 {@code KVCache}）。
 * <p>
 * <b>四个 redis 实现不在这里，是有意的。</b>它们同样是 {@code KVCache}，但 KVCache 层那组断言
 * （set / get / delete 往返、setnx 的四种输入组合、key 为 null 一律拒绝、remove 返回旧值……）
 * 已经由 {@link ExpiredKVCacheContractTest} 在同一批实现上逐条跑过 —— 那边为了测 TTL 必须注册
 * 它们，顺手就覆盖了继承来的那一层；再登记一遍，只是把同样的断言往同一批远程实例上重发一次。
 * <p>
 * 这是一条<b>靠人记着的边界</b>：真要按"每层一张清单"彻底重划分，该做的是把
 * {@code ExpiredKVCacheContractTest} 里那几条 KVCache 层的用例挪过来，而不是在这里补注册。
 *
 * @author addenda
 */
@Slf4j
public class KVCacheContractTest {

  /**
   * 不承诺线程安全
   */
  private final List<KVCache<String, String>> caches = new ArrayList<>();

  /**
   * 实例自身承诺线程安全的那些：底层是并发容器的，或者包了 {@link SynchronizedKVCache} 的。
   * <p>
   * 注意后者的底层（HashMap / TreeMap）<b>本身并不线程安全</b>，安全性来自包装层。
   */
  private final List<KVCache<String, String>> threadSafeCaches = new ArrayList<>();

  /**
   * 不限制条数的那些：{@code capacity()} 返回 {@link KVCache#UNLIMITED_CAPACITY} 哨兵。
   * <p>
   * "哨兵统一"这条只对它们成立 —— 带真实容量的淘汰实现不属于这一类（它的 {@code capacity()}
   * 就是构造时给的容量），将来加进来时按 {@code register(..., unbounded = false)} 登记即可。
   */
  private final List<KVCache<String, String>> unboundedCaches = new ArrayList<>();

  /**
   * 登记一个实现，顺便声明它的两个性质 —— 用例按性质挑组跑：
   * {@link #threadSafeCaches} 给线程安全用例，{@link #unboundedCaches} 给哨兵用例。
   *
   * @param threadSafe 它自身是否承诺线程安全
   * @param unbounded  它是否不限制条数
   */
  private void register(KVCache<String, String> cache, boolean threadSafe, boolean unbounded) {
    caches.add(cache);
    if (threadSafe) {
      threadSafeCaches.add(cache);
    }
    if (unbounded) {
      unboundedCaches.add(cache);
    }
  }

  @Before
  public void before() {
    // 自身不承诺线程安全的实现
    register(new TreeMapKVCache<>(), false, true);
    register(new HashMapKVCache<>(), false, true);

    // 自身就承诺线程安全的实现（不依赖 SynchronizedKVCache 包装）
    register(new ConcurrentHashMapKVCache<>(), true, true);

    // 有序表的线程安全包装：底层 TreeMap 不线程安全，安全性来自包装层
    register(SynchronizedSortedKVCache.synchronize(new TreeMapKVCache<>()), true, true);

    // 靠 SynchronizedKVCache 包装获得线程安全的实现（底层 HashMap / TreeMap 本身不线程安全）
    register(SynchronizedKVCache.synchronize(new HashMapKVCache<>()), true, true);
    register(SynchronizedKVCache.synchronize(new TreeMapKVCache<>()), true, true);

    // 带 TTL 的实现：不走 TTL 入口时（本类只走 {@code KVCache} 那几个方法），
    // 它们的 set / get / delete / setnx / remove 与上面几个口径完全一致 ——
    // 不带 TTL 的 set 就是"永不过期"，因此同样受本类的契约约束。
    register(new ExpiredHashMapKVCache<>(), false, true);
    register(new ExpiredConcurrentHashMapKVCache<>(), true, true);
    register(SynchronizedExpiredKVCache.synchronize(new ExpiredHashMapKVCache<>()), true, true);
  }

  @Test
  public void testSetGetDelete() {
    for (KVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      cache.set(key, "v1");
      Assert.assertEquals(name + ": 写进去应该能读出来", "v1", cache.get(key));
      Assert.assertTrue(name + ": containsKey 应该是 true", cache.containsKey(key));

      cache.set(key, "v2");
      Assert.assertEquals(name + ": 覆盖写", "v2", cache.get(key));

      Assert.assertTrue(name + ": delete 返回是否删掉了", cache.delete(key));
      Assert.assertFalse(name + ": 再删一次应该返回 false", cache.delete(key));
      Assert.assertNull(name + ": 删掉之后应该读不到", cache.get(key));
    }
  }

  /**
   * set(k, null) 一律当删除 —— 见 {@link KVCache#set} 上的契约。
   */
  @Test
  public void testSetNullMeansDelete() {
    for (KVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      cache.set(key, "v");
      Assert.assertTrue(cache.containsKey(key));

      cache.set(key, null);
      Assert.assertNull(name + ": 写 null 之后 get 应该返回 null", cache.get(key));
      Assert.assertFalse(name + ": 写 null 等价于删除", cache.containsKey(key));
    }
  }

  /**
   * setnx：key 不存在才写入
   */
  @Test
  public void testSetnx() {
    for (KVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      Assert.assertTrue(name + ": key 不存在时应该写入成功", cache.setnx(key, "v1"));
      Assert.assertEquals("v1", cache.get(key));

      Assert.assertFalse(name + ": key 已存在时应该写入失败", cache.setnx(key, "v2"));
      Assert.assertEquals(name + ": 已存在的值不应该被覆盖", "v1", cache.get(key));

      cache.delete(key);
      Assert.assertTrue(name + ": 删掉之后又能写了", cache.setnx(key, "v3"));
    }
  }

  /**
   * <b>set 和 setnx 对 null 的处理必须一致</b> —— value 为 null 在两边都表示"没有值"：
   * set 是删除，setnx 是无事可做（不写入、返回 false），都不抛异常。
   */
  @Test
  public void testNullValueMeansNoValue() {
    for (KVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      cache.set(key, null);
      Assert.assertFalse(name + ": set(key, null) 之后不应该存在", cache.containsKey(key));
      Assert.assertFalse(name + ": setnx(key, null) 应该返回 false", cache.setnx(key, null));
      Assert.assertFalse(name + ": setnx(key, null) 不应该写入", cache.containsKey(key));

      // key 已存在时，setnx 的 null 不能把已有的值弄丢
      cache.set(key, "v");
      Assert.assertFalse(name + ": setnx(key, null) 应该返回 false", cache.setnx(key, null));
      Assert.assertEquals(name + ": setnx(key, null) 不应该动已有值", "v", cache.get(key));

      cache.delete(key);
    }
  }

  /**
   * key 为 null 一律拒绝，各方法表现一致。
   */
  @Test
  public void testNullKeyRejected() {
    for (KVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();

      CacheAssertions.assertRejected(name + ": set", () -> cache.set(null, "v"));
      CacheAssertions.assertRejected(name + ": setnx", () -> cache.setnx(null, "v"));
      CacheAssertions.assertRejected(name + ": get", () -> cache.get(null));
      CacheAssertions.assertRejected(name + ": containsKey", () -> cache.containsKey(null));
      CacheAssertions.assertRejected(name + ": delete", () -> cache.delete(null));
    }
  }

  /**
   * <b>{@link KVCache} 的两个默认方法同样要拒绝 null key</b>，且口径与其它方法一致：
   * 一律 {@code CacheIllegalArgumentException}。
   * <p>
   * {@link KVCache#remove(Object)} 与 {@link KVCache#computeIfAbsent(Object, Function)}
   * 一度<b>没有</b>在入口校验，靠下游的 {@link KVCache#get(Object)} 兜住（{@code get(null)} 抛 CacheIllegalArgumentException）。
   * 那让"同一个非法 key 换一个方法就换一种异常"：{@code computeIfAbsent(null, null)} 在走默认
   * 实现的那些类上抛的是 {@link NullPointerException}（{@code requireNonNull} 排在前面），
   * 在覆盖成底层原子版本的 CHM 系上抛的是 CacheIllegalArgumentException。现在两个默认方法都在开头调
   * {@code assertKeyNotNull}，与"key 为 null 是本接口对所有方法一致约定的调用方错误"对齐 ——
   * 所以下面连"两个参数都是 null"这一格也一并断言。
   */
  @Test
  public void testNullKeyRejectedOnDefaultRemoveAndComputeIfAbsent() {
    for (KVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();

      CacheAssertions.assertRejected(name + ": remove", () -> cache.remove(null));
      CacheAssertions.assertRejected(name + ": computeIfAbsent（mappingFunction 非 null）",
              () -> cache.computeIfAbsent(null, k -> "v"));
      CacheAssertions.assertRejected(name + ": computeIfAbsent(null, null) 报 key 的问题，不是 NPE",
              () -> cache.computeIfAbsent(null, null));
    }
  }

  /**
   * <b>不限制条数的实现，{@code capacity()} 哨兵必须统一。</b>
   * <p>
   * 这些实现都不限制条数，"容量上限"只能用一个哨兵表示。此前四个 HashMap /
   * ConcurrentHashMap 系的实现返回 {@code 1 << 30}，而 {@link TreeMapKVCache} 返回
   * {@link KVCache#UNLIMITED_CAPACITY} —— 调用方写 {@code size() <= capacity()} 或
   * {@code capacity() - size()} 时，两类实现的边界不一样。
   * <p>
   * 统一到 {@link KVCache#UNLIMITED_CAPACITY}：它更接近"没有上限"这个语义
   * （{@code 1 << 30} 在超过 10 亿条时会真的"撞顶"）。哨兵是"答不上来"时给调用方的
   * 一个稳定口径，<b>不是</b>真实的容量约束 —— 这些实现根本没有淘汰机制。
   * <p>
   * 断言写的是那个常量本身，不是 {@code Integer.MAX_VALUE} 这个字面量：
   * 上限的数值将来可能变，这条用例钉的是"所有不限制条数的实现都用同一个口径"。
   * <p>
   * 只跑 {@link #unboundedCaches}：带真实容量的淘汰实现不在此列，见那个字段的说明。
   */
  @Test
  public void testMemoryImplCapacitySentinelIsUniform() {
    for (KVCache<String, String> cache : unboundedCaches) {
      String name = cache.getClass().getSimpleName();
      Assert.assertEquals(name + ": 不限制条数的实现，capacity() 哨兵应统一为 KVCache.UNLIMITED_CAPACITY",
              KVCache.UNLIMITED_CAPACITY, cache.capacity());
    }
  }

  /**
   * computeIfAbsent 命中缓存时不调 mappingFunction
   */
  @Test
  public void testComputeIfAbsentIsLazy() {
    for (KVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);
      AtomicInteger invoked = new AtomicInteger();

      Assert.assertEquals(name + ": 第一次应该计算",
              "v1", cache.computeIfAbsent(key, k -> "v" + invoked.incrementAndGet()));
      Assert.assertEquals(name + ": 第二次应该命中缓存",
              "v1", cache.computeIfAbsent(key, k -> "v" + invoked.incrementAndGet()));
      Assert.assertEquals(name + ": mappingFunction 只应该被调用一次", 1, invoked.get());
    }
  }

  /**
   * computeIfAbsent 的计算结果为 null 时不写入
   */
  @Test
  public void testComputeIfAbsentDoesNotStoreNull() {
    for (KVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      Assert.assertNull(name + ": 应该返回 null", cache.computeIfAbsent(key, k -> null));
      Assert.assertFalse(name + ": 结果为 null 时不应该写入", cache.containsKey(key));
    }
  }

  /**
   * remove = get & delete
   */
  @Test
  public void testRemove() {
    for (KVCache<String, String> cache : caches) {
      String name = cache.getClass().getSimpleName();
      String key = uniqueKey(name);

      cache.set(key, "v");
      Assert.assertEquals(name + ": remove 应该返回被删掉的值", "v", cache.remove(key));
      Assert.assertNull(name + ": 删掉之后应该读不到", cache.get(key));
      Assert.assertFalse(name + ": key 应该不存在了", cache.containsKey(key));
    }
  }

  /**
   * <b>接口默认的 {@code remove} 不是原子的</b>：它是 get 与 delete 两步，
   * 两步之间别人写进来的值会被这次删除带走，而方法返回的还是它先前读到的那个旧值。
   * <p>
   * 这是 {@link KVCache} 的 L2 层"接口默认实现不属于本层"的实证，也说明了为什么想达到 L2
   * 的实现必须覆盖 {@code remove}（如 {@link ConcurrentHashMapKVCache} 直接委托
   * ConcurrentHashMap 的原子 remove）。
   * <p>
   * 用可控的底层存储把窗口<b>钉死</b>，不靠并发碰运气：{@code remove} 读到 "old" 之后停住
   * → 主线程写入 "new" → 放行 → {@code remove} 删掉的其实是 "new"，返回的却是 "old"。
   * 换成原子的 remove，删除与返回是同一个瞬间，返回值必然是 "new"。
   */
  @Test
  public void testDefaultRemoveIsNotAtomic() throws Exception {
    PausableGetKVCache<String, String> cache = new PausableGetKVCache<>();
    cache.set("k", "old");

    AtomicReference<String> removed = new AtomicReference<>();
    Thread thread = new Thread(() -> removed.set(cache.remove("k")));
    thread.setDaemon(true);
    thread.start();

    Assert.assertTrue("等 remove 读完旧值", cache.readDone.await(30, TimeUnit.SECONDS));

    // remove 已经读到 "old" 但还没删，此时别人把值换了
    cache.set("k", "new");

    cache.resume.countDown();
    thread.join(TimeUnit.SECONDS.toMillis(30));

    Assert.assertEquals("删掉的是并发写入的 new，默认实现却把先前读到的旧值返回了",
            "old", removed.get());
    Assert.assertNull("并发写入的 new 被这次 remove 带走了", cache.get("k"));
  }

  /**
   * <b>{@link SynchronizedKVCache} 把复合操作的原子性补上了</b>（L2 的复合操作那一半）：
   * 整个 check-then-act 被锁住，
   * 所以 {@code computeIfAbsent} 变成原子的，并发下 {@code mappingFunction} 只调一次。
   * <p>
   * 对照 {@link ExpiredKVCacheContractTest#testComputeIfAbsentIsNotAtomicUnderConcurrency()}：
   * 同样是 8 线程并发，那边断言裸实现的 {@code mappingFunction} 被调用<b>多次</b>（只保证 &gt; 1），
   * 这边断言包一层之后只调 1 次。
   * <p>
   * 顺带验证并发下不抛异常、不死循环/死锁。
   */
  @Test
  public void testComputeIfAbsentBecomesAtomicAfterSynchronize() throws Exception {
    int threadCount = 8;

    int invoked = concurrentComputeIfAbsent(
            SynchronizedKVCache.synchronize(new TreeMapKVCache<>()), threadCount);

    log.error("SynchronizedKVCache: {} 个线程并发 computeIfAbsent，mappingFunction 被调用了 {} 次",
            threadCount, invoked);

    Assert.assertEquals("SynchronizedKVCache 锁住了整个 check-then-act，mappingFunction 只应该被调用一次",
            1, invoked);
  }

  /**
   * 包装后的实例线程安全：并发读写不抛异常、写的值原样读回来。
   */
  @Test
  public void testThreadSafety() throws Exception {
    int threadCount = 4;
    int rounds = 200;

    for (KVCache<String, String> cache : threadSafeCaches) {
      String name = cache.getClass().getSimpleName();
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threadCount);
      List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

      for (int t = 0; t < threadCount; t++) {
        final int threadId = t;
        Thread thread = new Thread(() -> {
          String ownKey = uniqueKey(name) + ":t" + threadId;
          String ownValue = "v" + threadId;
          try {
            start.await();
            for (int i = 0; i < rounds; i++) {
              cache.set(ownKey, ownValue);
              Assert.assertEquals(name + ": 本线程写的值应该原样读回来", ownValue, cache.get(ownKey));
              cache.setnx(ownKey, "should-not-win");
              Assert.assertEquals(name + ": setnx 不该覆盖已有值", ownValue, cache.get(ownKey));
            }
          } catch (Throwable e) {
            failures.add(new IllegalStateException(name + ": 线程 " + threadId + " 失败", e));
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
    }
  }

  /**
   * @return mappingFunction 被调用的次数
   */
  private int concurrentComputeIfAbsent(KVCache<String, String> cache, int threadCount) throws Exception {
    AtomicInteger invoked = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);

    for (int t = 0; t < threadCount; t++) {
      Thread thread = new Thread(() -> {
        try {
          start.await();
          cache.computeIfAbsent("race-key", k -> {
            invoked.incrementAndGet();
            // 把 check 和 act 之间的窗口拉大，让并发暴露出来
            SleepUtils.sleep(TimeUnit.MILLISECONDS, 100);
            return "computed";
          });
        } catch (Throwable ignored) {
          // 次数才是这次要看的结果
        } finally {
          done.countDown();
        }
      });
      thread.setDaemon(true);
      thread.start();
    }

    start.countDown();
    Assert.assertTrue("不应该死循环或死锁", done.await(30, TimeUnit.SECONDS));
    return invoked.get();
  }

  private String uniqueKey(String implName) {
    return "KVCacheContractTest:" + implName + ":" + UUID.randomUUID().toString().replace("-", "");
  }

  /**
   * 一个可控的底层存储：{@link #get(Object)} 读到值之后会停下来，等测试放行才返回。
   * <p>
   * 目的是把 {@link KVCache#remove(Object)} 默认实现里 get 与 delete 之间的窗口
   * 确定性地暴露出来。
   * <p>
   * <b>刻意不覆盖 {@code remove} / {@code computeIfAbsent}</b> —— 要测的就是接口的默认实现。
   */
  private static class PausableGetKVCache<K, V> implements KVCache<K, V> {

    private final KVCache<K, V> delegate = new HashMapKVCache<>();

    /**
     * get 已经读到值
     */
    private final CountDownLatch readDone = new CountDownLatch(1);

    /**
     * 放行 get 返回
     */
    private final CountDownLatch resume = new CountDownLatch(1);

    @Override
    public void set(K k, V v) {
      delegate.set(k, v);
    }

    @Override
    public boolean setnx(K k, V v) {
      return delegate.setnx(k, v);
    }

    @Override
    public boolean containsKey(K k) {
      return delegate.containsKey(k);
    }

    @Override
    public V get(K k) {
      V v = delegate.get(k);
      readDone.countDown();
      try {
        resume.await(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return v;
    }

    @Override
    public boolean delete(K k) {
      return delegate.delete(k);
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public int capacity() {
      return delegate.capacity();
    }

  }

}
