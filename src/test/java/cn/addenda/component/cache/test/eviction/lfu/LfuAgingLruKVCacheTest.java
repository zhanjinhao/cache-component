package cn.addenda.component.cache.test.eviction.lfu;

import cn.addenda.component.cache.TestDelegateAccess;
import cn.addenda.component.cache.eviction.lfu.LfuAgingKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuAgingLruKVCache;
import cn.addenda.component.cache.eviction.lru.LruHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruKVCache;
import cn.addenda.component.cache.test.CacheAssertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedHashSet;

/**
 * {@link LfuAgingLruKVCache} 的行为测试：带衰减的 LFU 决定淘汰谁，底层再压一层 LRU。
 * <p>
 * 衰减逻辑本身继承自 {@code LfuAgingKVCache}，在 {@code LfuAgingHashMapKVCacheTest} 里测过了，
 * 这里只覆盖"底层换成一个会自行淘汰的 LRU"带来的差别：
 * 两层容量一致时内层不被触发，容量不一致时内层会留下残留计数，而衰减要能在这种状态下照常工作。
 * <p>
 * 需要自己指定底层 LRU 实例（尤其是容量与上层不一致）的用例，用的是本测试里的临时实现
 * {@link ExposedLfuAgingLruKVCache}：{@code LfuAgingLruKVCache} 的底层存储由类名确定，
 * 不再接受 delegate 参数。
 *
 * @author addenda
 */
public class LfuAgingLruKVCacheTest {

  private static final int CAPACITY = 2;

  private LfuAgingLruKVCache<String, String> cache;

  @Before
  public void before() {
    cache = new LfuAgingLruKVCache<>(CAPACITY, 1, 0.5);
  }

  /**
   * 默认构造器把两层的容量设成一样的，衰减参数对外可读。
   */
  @Test
  public void testDefaultDelegate() {
    Assert.assertEquals(CAPACITY, cache.capacity());
    Assert.assertEquals(1L, cache.getAgingInterval());
    Assert.assertEquals(0.5, cache.getDecayRatio(), 0.0);
    Assert.assertNotNull("底层是一个 LRU", TestDelegateAccess.delegateOf(cache));
  }

  /**
   * 淘汰行为和 {@code LfuAgingHashMapKVCache} 一致：次数低的先走；衰减照常发生。
   */
  @Test
  public void testEvictsLeastFrequentlyUsedAndDecays() {
    cache.set("a", "v");
    for (int i = 0; i < 3; i++) {
      cache.get("a");                      // a 的分数 = 3
    }
    cache.set("b", "v");

    cache.set("c", "v");                    // 满 → 淘汰 0 分的 b → 随后衰减一次

    Assert.assertEquals("淘汰过一次", 1L, cache.getEvictionCount());
    Assert.assertEquals("a 的 3 分被减半成 1", Long.valueOf(1L), cache.getKeyToVisitorCount().get("a"));
    Assert.assertEquals("a 分数最高，应该活下来", "v", cache.get("a"));
    Assert.assertNull("b 没被读过，该淘汰", cache.get("b"));
  }

  /**
   * <b>两层容量一致时，底层 LRU 不该自己触发淘汰</b>，淘汰完全由 LFU 决定，衰减在 LFU 侧推进。
   */
  @Test
  public void testUnderlyingLruIsNotTriggeredWhenCapacitiesMatch() {
    LruKVCache<String, String> delegate = new LruHashMapKVCache<>(CAPACITY);
    LfuAgingKVCache<String, String> layered = new ExposedLfuAgingLruKVCache<>(CAPACITY, 1, 0.5, delegate);

    layered.set("a", "v");
    layered.set("b", "v");
    Assert.assertEquals("两层条数一致", CAPACITY, delegate.size());

    layered.set("c", "v");

    Assert.assertEquals("LFU 淘汰 + 写入，底层还是正好满", CAPACITY, delegate.size());
    Assert.assertEquals("底层没自行淘汰，LFU 统计与底层一致", delegate.size(), layered.getKeyToVisitorCount().size());
    Assert.assertNull(layered.get("a"));
  }

  /**
   * <b>底层 LRU 自行淘汰留下残留计数时，衰减仍然照常工作</b>。
   * <p>
   * 外层 3 格、内层 2 格是有意构造的不一致：内层写满后会自行淘汰队头，
   * LFU 层收不到通知，计数与分数会残留下来。
   * 衰减遍历的是 LFU 自己的两张统计表，不碰底层存储，因此残留不会让它出错 ——
   * 这是本用例要确认的两点：<b>不抛异常</b>，且<b>两张统计表在衰减后仍然自洽</b>。
   * <p>
   * 残留本身由 {@code EvictableKVCache#containsKey} 的自愈路径清除，见
   * {@code LfuLruKVCacheTest#testStatisticsSelfHealWhenUnderlyingLruEvicts}。
   */
  @Test
  public void testDecayKeepsStatisticStructuresConsistentWithLruDelegate() {
    LruKVCache<String, String> delegate = new LruHashMapKVCache<>(2);
    LfuAgingKVCache<String, String> layered = new ExposedLfuAgingLruKVCache<>(3, 1, 0.5, delegate);

    layered.set("a", "v");
    for (int i = 0; i < 4; i++) {
      layered.get("a");                     // a 的分数 = 4
    }
    for (int i = 0; i < 6; i++) {
      layered.set("k" + i, "v");
      layered.get("k" + i);
    }

    Assert.assertTrue("期间触发过淘汰，也就触发过衰减", layered.getEvictionCount() > 0);

    // 两张统计表自洽：每个 key 的分数都等于它所在的档位，且两表规模一致
    long tierTotal = 0;
    for (Long count : layered.getVisitorCountToKeySet().keySet()) {
      LinkedHashSet<String> keys = layered.getVisitorCountToKeySet().get(count);
      for (String k : keys) {
        Assert.assertEquals("档位表里的每个 key，其分数必须指向当前档",
                count, layered.getKeyToVisitorCount().get(k));
      }
      tierTotal += keys.size();
    }
    Assert.assertEquals("两张统计表的规模一致", layered.getKeyToVisitorCount().size(), tierTotal);

    // 残留计数不影响读取：留着残留的 key 读到 null，并顺势自愈
    for (String k : new String[]{"a", "k0", "k1", "k2", "k3", "k4", "k5"}) {
      layered.get(k);
    }
    Assert.assertEquals("自愈之后统计与底层存储对齐", delegate.size(), layered.getKeyToVisitorCount().size());
  }

  /**
   * set / setnx 对 null 的处理，见 {@code KVCache} 上的契约：
   * key 一律拒绝，value 为 null 一律表示"没有值"。
   * <p>
   * 两层结构，删的时候要确认<b>两层都清了</b>：LFU 的统计和底层 LRU 的队列。
   */
  @Test
  public void testNullKeyAndNullValue() {
    LruKVCache<String, String> delegate = new LruHashMapKVCache<>(CAPACITY);
    LfuAgingKVCache<String, String> layered = new ExposedLfuAgingLruKVCache<>(CAPACITY, 1, 0.5, delegate);

    layered.set("a", "1");
    Assert.assertEquals(1, layered.getKeyToVisitorCount().size());
    Assert.assertEquals(1, delegate.size());

    // size() 紧跟在 set(k, null) 后面断言：get/containsKey 会触发
    // EvictableKVCache#containsKey 里的自愈，把残留的统计顺手清掉
    layered.set("a", null);
    Assert.assertEquals("LFU 统计要清", 0, layered.getKeyToVisitorCount().size());
    Assert.assertEquals("底层 LRU 队列也要清", 0, delegate.size());
    Assert.assertNull(layered.get("a"));
    Assert.assertFalse(layered.containsKey("a"));

    // setnx 的 value 为 null：两层都不该被碰上
    Assert.assertFalse(layered.setnx("b", null));
    Assert.assertFalse(layered.containsKey("b"));
    Assert.assertEquals(0, layered.getKeyToVisitorCount().size());
    Assert.assertEquals(0, delegate.size());

    // 已有值时不动它
    layered.set("b", "2");
    Assert.assertFalse(layered.setnx("b", null));
    Assert.assertEquals("不该动已有值", "2", layered.get("b"));

    CacheAssertions.assertRejected("LfuAgingLruKVCache: set", () -> layered.set(null, "v"));
    CacheAssertions.assertRejected("LfuAgingLruKVCache: set(k, null)", () -> layered.set(null, null));
    CacheAssertions.assertRejected("LfuAgingLruKVCache: setnx", () -> layered.setnx(null, "v"));
    CacheAssertions.assertRejected("LfuAgingLruKVCache: setnx(k, null)", () -> layered.setnx(null, null));
    CacheAssertions.assertRejected("LfuAgingLruKVCache: get", () -> layered.get(null));
    CacheAssertions.assertRejected("LfuAgingLruKVCache: containsKey", () -> layered.containsKey(null));
    CacheAssertions.assertRejected("LfuAgingLruKVCache: delete", () -> layered.delete(null));
  }

  /**
   * 仅供测试：暴露 {@code LfuAgingKVCache} 接受 delegate 的 protected 构造器。
   * <p>
   * {@code LfuAgingLruKVCache} 的底层存储由类名确定、不再接受 delegate 参数，
   * 而"内层 LRU 自行淘汰留下残留计数"这个场景需要刻意把两层的容量配成不一致，
   * 因此在这里构造一个临时实现来指定底层 LRU。
   */
  private static class ExposedLfuAgingLruKVCache<K, V> extends LfuAgingKVCache<K, V> {
    private ExposedLfuAgingLruKVCache(int capacity, long agingInterval, double decayRatio,
                                      LruKVCache<K, V> kvCacheDelegate) {
      super(capacity, agingInterval, decayRatio, true, kvCacheDelegate);
    }
  }

}
