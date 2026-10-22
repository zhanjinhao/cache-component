package cn.addenda.component.cache.test.eviction;

import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.KVCacheWrapper;
import cn.addenda.component.cache.TestDelegateAccess;
import cn.addenda.component.cache.eviction.lfu.LfuHashMapKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuLruKVCache;
import cn.addenda.component.cache.eviction.lru.LruGenerationHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruKHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruKVCache;
import cn.addenda.component.cache.eviction.lru.LruKKVCache;
import cn.addenda.component.cache.test.CacheAssertions;
import cn.addenda.component.cache.test.TestCapacityLimit;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code EvictableKVCache} 的<b>容量边界</b>测试：容量的校验、写满、写超、
 * 删空再写、反复写满删空，以及"统计结构"和"底层存储"两条线对不对得上。
 * <p>
 * 抛开了每个策略"淘汰谁"的差异，只盯一条：<b>容量是硬约束</b>。
 * 不管什么淘汰策略，{@code size()} 都不该越过 {@code capacity()}，
 * 也不该和底层实际存着的条数对不上。
 * <p>
 * {@link cn.addenda.component.cache.eviction.lru.LruGenerationKVCache} 不在这组里：
 * 它有两代、两个容量，{@code capacity()} 报的是两代之和，
 * 但新写进来的数据只能先落老年代，所以"写满 capacity 条"这件事在它身上不成立。
 * 它有自己的一组用例。
 *
 * @author addenda
 */
public class EvictableKVCacheWrapperCapacityBoundaryTest {

  /**
   * @return 一组刚构造好的淘汰实现，容量都是 capacity
   */
  private List<KVCache<String, String>> newCaches(int capacity) {
    List<KVCache<String, String>> caches = new ArrayList<>();
    caches.add(new LruHashMapKVCache<>(capacity));
    caches.add(new LfuHashMapKVCache<>(capacity));
    caches.add(new LfuLruKVCache<>(capacity));
    // threshold 给 1：写过就进，这样"容量"才是唯一的约束
    caches.add(new LruKHashMapKVCache<>(capacity, 1));
    return caches;
  }

  private String name(KVCache<String, String> cache) {
    return cache.getClass().getSimpleName();
  }

  // ------------------------------------------------------------------
  // 构造器边界
  // ------------------------------------------------------------------

  /**
   * 容量必须 > 0 —— 0 和负数都要当场拒绝。
   */
  @Test
  public void testConstructorRejectsNonPositiveCapacity() {
    CacheAssertions.assertRejected("LruKVCache(0)",
            () -> new LruHashMapKVCache<>(0));
    CacheAssertions.assertRejected("LruKVCache(-1)",
            () -> new LruHashMapKVCache<>(-1));
    CacheAssertions.assertRejected("LfuHashMapKVCache(0)",
            () -> new LfuHashMapKVCache<>(0));
    CacheAssertions.assertRejected("LfuLruKVCache(0)",
            () -> new LfuLruKVCache<>(0));
    CacheAssertions.assertRejected("LruKKVCache(0)",
            () -> new LruKHashMapKVCache<>(0, 1));
    CacheAssertions.assertRejected("LruGenerationKVCache(0, 3)",
            () -> new LruGenerationHashMapKVCache<>(0, 3, 0));
    CacheAssertions.assertRejected("LruGenerationKVCache(3, 0)",
            () -> new LruGenerationHashMapKVCache<>(3, 0, 0));
  }

  /**
   * <b>容量的上界是 {@code KVCache.UNLIMITED_CAPACITY}</b>：它与"必须 &gt; 0"合起来给出
   * 完整值域 {@code [0, UNLIMITED_CAPACITY]}，见 {@link KVCache#UNLIMITED_CAPACITY}
   * —— 本组件的合法性判断一律以这个常量为准，不是以 {@code Integer.MAX_VALUE} 为准。
   * <p>
   * 边界值本身必须可用：{@code capacity == UNLIMITED_CAPACITY} 要构造得出来。
   * <p>
   * "超过上限"那一侧在取 {@code int} 上界时写不出来，所以这里<b>临时把上限调低</b>
   * （见 {@link #setUnlimitedCapacity(int)}），跑完在 {@code finally} 里恢复。上限不是
   * 编译期常量（见其声明），调用方读的是字段，调低才会对构造器里的校验生效。
   */
  @Test
  public void testConstructorRejectsCapacityOverUnlimited() {
    Assert.assertNotNull("恰好等于上限要能用",
            new LruHashMapKVCache<String, String>(KVCache.UNLIMITED_CAPACITY));
    Assert.assertNotNull("恰好等于上限要能用",
            new LfuHashMapKVCache<String, String>(KVCache.UNLIMITED_CAPACITY));

    int lowered = 10_000;
    int original = TestCapacityLimit.setUnlimitedCapacity(lowered);
    try {
      Assert.assertEquals("上限应已被临时调低，否则下面的输入仍构造得出来",
              lowered, KVCache.UNLIMITED_CAPACITY);

      int over = lowered + 1;
      CacheAssertions.assertRejected("LruKVCache 超过上限",
              () -> new LruHashMapKVCache<String, String>(over));
      CacheAssertions.assertRejected("LfuHashMapKVCache 超过上限",
              () -> new LfuHashMapKVCache<String, String>(over));
      CacheAssertions.assertRejected("LfuLruKVCache 超过上限",
              () -> new LfuLruKVCache<String, String>(over));
    } finally {
      TestCapacityLimit.setUnlimitedCapacity(original);
    }
  }

  /**
   * 底层存储不能为 null。
   * <p>
   * {@link LruKVCache} 是抽象基类，具体类 {@link LruHashMapKVCache} 不接受 delegate，
   * 因此这里用一个临时实现把那个构造器露出来。
   */
  @Test
  public void testConstructorRejectsNullDelegate() {
    CacheAssertions.assertRejected("LruKVCache(capacity, null)",
            () -> new LruOverDelegate<String, String>(3, null));
  }

  // ------------------------------------------------------------------
  // 容量的硬约束
  // ------------------------------------------------------------------

  /**
   * 容量 1：写第二条时第一条必须走，且 size 始终是 1。
   */
  @Test
  public void testCapacityOneHoldsExactlyOne() {
    for (KVCache<String, String> cache : newCaches(1)) {
      String name = name(cache);
      cache.set("a", "1");
      Assert.assertEquals(name + ": 写一条就是满", 1, cache.size());
      Assert.assertTrue(cache.containsKey("a"));

      cache.set("b", "2");
      Assert.assertEquals(name + ": 还是 1 条", 1, cache.size());
      Assert.assertFalse(name + ": a 必须被淘汰", cache.containsKey("a"));
      Assert.assertTrue(cache.containsKey("b"));
      Assert.assertEquals("2", cache.get("b"));
    }
  }

  /**
   * <b>刚好写满不该淘汰任何东西</b> —— 淘汰的触发点是"再写一条"。
   */
  @Test
  public void testFillingToExactCapacityEvictsNothing() {
    for (KVCache<String, String> cache : newCaches(3)) {
      String name = name(cache);
      cache.set("a", "1");
      cache.set("b", "2");
      cache.set("c", "3");

      Assert.assertEquals(name + ": 正好三条", 3, cache.size());
      Assert.assertTrue(name + ": a 不该被淘汰", cache.containsKey("a"));
      Assert.assertTrue(name + ": b 不该被淘汰", cache.containsKey("b"));
      Assert.assertTrue(name + ": c 不该被淘汰", cache.containsKey("c"));
    }
  }

  /**
   * 超出容量一条，只淘汰一条 —— 不能一次清掉一片。
   */
  @Test
  public void testOneOverCapacityEvictsExactlyOne() {
    for (KVCache<String, String> cache : newCaches(3)) {
      String name = name(cache);
      cache.set("a", "1");
      cache.set("b", "2");
      cache.set("c", "3");
      cache.set("d", "4");

      Assert.assertEquals(name + ": 还是三条", 3, cache.size());
      Assert.assertFalse(name + ": a 被淘汰", cache.containsKey("a"));
      Assert.assertTrue(name + ": b 还在", cache.containsKey("b"));
      Assert.assertTrue(name + ": c 还在", cache.containsKey("c"));
      Assert.assertTrue(name + ": d 写进去了", cache.containsKey("d"));
    }
  }

  /**
   * 删空之后容量完全还回来：再写满不该触发淘汰。
   */
  @Test
  public void testDeleteEverythingThenRefill() {
    for (KVCache<String, String> cache : newCaches(3)) {
      String name = name(cache);
      cache.set("a", "1");
      cache.set("b", "2");
      cache.set("c", "3");

      cache.delete("a");
      cache.delete("b");
      cache.delete("c");
      Assert.assertEquals(name + ": 删空了", 0, cache.size());

      cache.set("d", "4");
      cache.set("e", "5");
      cache.set("f", "6");
      Assert.assertEquals(name + ": 重新装满三条", 3, cache.size());
      Assert.assertTrue(name + ": d 不该被淘汰", cache.containsKey("d"));
      Assert.assertTrue(name + ": e 不该被淘汰", cache.containsKey("e"));
      Assert.assertTrue(name + ": f 不该被淘汰", cache.containsKey("f"));
    }
  }

  /**
   * 反复"写满 → 删空"，容量不允许有任何泄漏。
   */
  @Test
  public void testRepeatedFillAndDrain() {
    for (KVCache<String, String> cache : newCaches(3)) {
      String name = name(cache);
      for (int round = 0; round < 5; round++) {
        for (int i = 0; i < 3; i++) {
          cache.set("r" + round + "-" + i, "v");
        }
        Assert.assertEquals(name + ": 第 " + round + " 轮写满", 3, cache.size());

        for (int i = 0; i < 3; i++) {
          Assert.assertTrue(name + ": 第 " + round + " 轮删得起", cache.delete("r" + round + "-" + i));
        }
        Assert.assertEquals(name + ": 第 " + round + " 轮删空", 0, cache.size());
      }
    }
  }

  /**
   * 反复写同一个 key 不该把统计撑大 —— 队列里同一个 key 只能占一格。
   */
  @Test
  public void testOverwriteSameKeyDoesNotGrowSize() {
    for (KVCache<String, String> cache : newCaches(3)) {
      String name = name(cache);
      for (int i = 0; i < 100; i++) {
        cache.set("a", "v" + i);
      }
      Assert.assertEquals(name + ": 同一个 key 写 100 次还是 1 条", 1, cache.size());
      Assert.assertEquals("v99", cache.get("a"));
    }
  }

  /**
   * 反复 setnx 同一个 key 同样不该撑大 —— 只有第一次会写进去。
   * <p>
   * {@link LruKKVCache} 不在这组里：它明确不支持 {@code setnx}
   * （抛 {@code CacheUnsupportedOperationException}），
   * 见 {@link #testSetnxIsUnsupportedOnLruKKVCache()}。
   */
  @Test
  public void testSetnxSameKeyDoesNotGrowSize() {
    for (KVCache<String, String> cache : newSetnxCapableCaches(3)) {
      String name = name(cache);
      Assert.assertTrue(name + ": 第一次该成功", cache.setnx("a", "v0"));
      for (int i = 1; i < 100; i++) {
        Assert.assertFalse(name + ": 之后都该失败", cache.setnx("a", "v" + i));
      }
      Assert.assertEquals(name + ": 还是 1 条", 1, cache.size());
      Assert.assertEquals("v0", cache.get("a"));
    }
  }

  // ------------------------------------------------------------------
  // 统计 vs 存储
  // ------------------------------------------------------------------

  /**
   * <b>不变量：{@code size() <= capacity()}，且 {@code size()} 与底层条数一致</b>。
   * <p>
   * 用 7 个 key 轮着写 200 次，每一步都检查。7 > 容量 3，所以中间一定会反复触发淘汰。
   * <p>
   * 要看清这里能抓到什么：{@code size()} 就是底层存储的条数，等式两边<b>同一来源</b>、
   * 恒等 —— 它保证的是"写入一直落在正确的地方、条数没有越界"；
   * <b>账本残留它看不见</b>（残留不改变数据层条数）。账本那一侧见
   * {@code EvictableKVCacheWrapperConsistencyTest}，那里直接读队列 / 计数表。
   */
  @Test
  public void testStatisticsStayInSyncWithStorage() {
    for (KVCache<String, String> cache : newCaches(3)) {
      String name = name(cache);
      for (int i = 0; i < 200; i++) {
        cache.set("k" + (i % 7), "v" + i);

        Assert.assertTrue(name + ": size 越界了，" + cache.size() + " > " + cache.capacity(),
                cache.size() <= cache.capacity());
        Assert.assertEquals(name + ": 第 " + i + " 步，size() 与底层存储条数对不上",
                delegateSize(cache), cache.size());
      }
    }
  }

  /**
   * 淘汰之后只剩容量这么多条，且每条都读得到 —— 数一遍真实存活数来交叉核对。
   * <p>
   * 计数走 {@code containsKey}、条数走 {@code size()}，两者都取自数据层，因此
   * 第一步那条等式本身不构成证据；<b>有意义的是 {@code alive <= 2} 与
   * {@code size() == alive} 这两条</b>：条数和"逐个 key 查出来的存活数"对得上，
   * 说明没有多算也没有丢。账本（队列 / 计数表）里的幽灵条目这里同样看不见。
   */
  @Test
  public void testNoPhantomEntriesAfterEviction() {
    for (KVCache<String, String> cache : newCaches(2)) {
      String name = name(cache);
      for (int i = 0; i < 20; i++) {
        cache.set("k" + i, "v" + i);
      }

      Assert.assertEquals(name + ": 只有 2 条能活下来", delegateSize(cache), cache.size());

      // 数一遍真实存活数，和 size() 对一下
      int alive = 0;
      for (int i = 0; i < 20; i++) {
        if (cache.containsKey("k" + i)) {
          alive++;
        }
      }
      Assert.assertEquals(name + ": size() 要等于真实存活数", (long) alive, cache.size());
      Assert.assertTrue(name + ": 存活数不该超过容量", alive <= 2);
    }
  }

  /**
   * 所有淘汰实现都是 {@code KVCacheWrapper}，底层存储能直接看得到。
   */
  private long delegateSize(KVCache<String, String> cache) {
    return TestDelegateAccess.delegateOf((KVCacheWrapper<String, String>) cache).size();
  }

  /**
   * 去掉 {@link LruKKVCache} —— 它明确不支持 {@code setnx}，行为是抛
   * {@code CacheUnsupportedOperationException}，不能混在"setnx 通用契约"里跑。
   * <p>
   * 这跟"容量"无关，是语义对不上：LRU-K 的写入要过准入闸门，
   * 而 {@code setnx} 承诺"不存在就写、写了就有"。
   */
  private List<KVCache<String, String>> newSetnxCapableCaches(int capacity) {
    List<KVCache<String, String>> caches = newCaches(capacity);
    caches.removeIf(cache -> cache instanceof LruKKVCache);
    return caches;
  }

  /**
   * 单独钉住 {@link LruKKVCache} 的这条：{@code setnx} 抛
   * {@code CacheUnsupportedOperationException}，而不是返回一个需要猜的 false。
   */
  @Test
  public void testSetnxIsUnsupportedOnLruKKVCache() {
    CacheAssertions.assertNotSupported("LruKKVCache: setnx",
            () -> new LruKHashMapKVCache<String, String>(3, 1).setnx("a", "v"));
    CacheAssertions.assertNotSupported("LruKKVCache: setnx(threshold=2)",
            () -> new LruKHashMapKVCache<String, String>(3, 2).setnx("a", "v"));
  }

  /**
   * 仅供测试：暴露 {@link LruKVCache} 接受 delegate 的公开构造器
   */
  private static class LruOverDelegate<K, V> extends LruKVCache<K, V> {
    private LruOverDelegate(int capacity, KVCache<K, V> kvCacheDelegate) {
      super(capacity, kvCacheDelegate);
    }
  }

}
