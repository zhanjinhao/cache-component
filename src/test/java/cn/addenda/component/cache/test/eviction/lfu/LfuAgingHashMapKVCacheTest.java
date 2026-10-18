package cn.addenda.component.cache.test.eviction.lfu;

import cn.addenda.component.cache.HashMapKVCache;
import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.SortedKVCache;
import cn.addenda.component.cache.SortedMapKVCache;
import cn.addenda.component.cache.TreeMapKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuAgingHashMapKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuAgingKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuHashMapKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuKVCache;
import cn.addenda.component.cache.test.CacheAssertions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * {@link LfuAgingHashMapKVCache} 的行为测试：在 LFU 之上给访问次数加时间维度。
 * <p>
 * 衰减逻辑继承自 {@link LfuAgingKVCache}，这里以 HashMap 底层为观察对象覆盖它，
 * 重点两条：
 * <ul>
 *   <li>衰减的算术是否正确（每次把所有分数减半）；</li>
 *   <li>它要解决的问题是否真的被解决 —— 高分僵尸被衰减清掉，而不带衰减的 LFU 留不住位置。</li>
 * </ul>
 * LRU 底层的同类验证在 {@code LfuAgingLruKVCacheTest} 里。
 *
 * @author addenda
 */
public class LfuAgingHashMapKVCacheTest {

  /**
   * <b>每次衰减把所有分数减半</b>，档位表同步重建。
   * <p>
   * {@code agingInterval = 1} 表示每淘汰一次就衰减一次，因此一次淘汰即可精确断言结果。
   */
  @Test
  public void testDecayHalvesAllVisitorCounts() {
    LfuAgingKVCache<String, String> cache = new LfuAgingHashMapKVCache<>(3, 1, 0.5);

    cache.set("a", "v");
    for (int i = 0; i < 7; i++) {
      cache.get("a");                       // a 的分数 = 7
    }
    cache.set("b", "v");
    cache.set("c", "v");                    // 写满 3 格，此时尚未淘汰

    Assert.assertEquals("前置条件：a 的分数是 7", Long.valueOf(7L), cache.getKeyToVisitorCount().get("a"));
    Assert.assertEquals("前置条件：尚未触发淘汰", 0L, cache.getEvictionCount());

    cache.set("d", "v");                    // 满了 → 淘汰最小档最旧的 b → 随后衰减一次

    Assert.assertEquals("已经淘汰过一次", 1L, cache.getEvictionCount());
    Assert.assertNull("最小档最旧的 b 被淘汰", cache.getKeyToVisitorCount().get("b"));
    Assert.assertEquals("a 的 7 分被减半成 3", Long.valueOf(3L), cache.getKeyToVisitorCount().get("a"));
    Assert.assertEquals("0 分减半后仍是 0", Long.valueOf(0L), cache.getKeyToVisitorCount().get("c"));
    Assert.assertEquals("新来的 d 从 0 起步", Long.valueOf(0L), cache.getKeyToVisitorCount().get("d"));
  }

  /**
   * <b>衰减能把高分僵尸清出去</b> —— 这正是它存在的理由。
   * <p>
   * 两个缓存跑同一段序列：一个 key 先被读 20 次（攒出高分），之后彻底不再使用；
   * 随后写入 10 条新数据，每条只被读一次。
   * <ul>
   *   <li>不带衰减：僵尸的 20 分永远压着新数据，它一直占着 2 格里的 1 格；</li>
   *   <li>带衰减：每淘汰一次分数减半（20 → 10 → 5 → 2 → 1），几轮之后僵尸变成最小档，
   *       被正常淘汰。</li>
   * </ul>
   */
  @Test
  public void testHighScoreZombieIsAgedOut() {
    LfuHashMapKVCache<String, String> plain = new LfuHashMapKVCache<>(2);
    LfuAgingHashMapKVCache<String, String> aging = new LfuAgingHashMapKVCache<>(2, 1, 0.5);

    for (KVCache<String, String> cache : Arrays.asList(plain, aging)) {
      cache.set("zombie", "v");
      for (int i = 0; i < 20; i++) {
        cache.get("zombie");
      }
      for (int i = 0; i < 10; i++) {
        cache.set("k" + i, "v");
        cache.get("k" + i);
      }
    }

    Assert.assertNotNull("不带衰减：20 分的僵尸永远压着新数据",
            plain.get("zombie"));
    Assert.assertNull("带衰减：分数逐次减半，僵尸最终被淘汰",
            aging.get("zombie"));
  }

  /**
   * 衰减阈值必须为正 —— 传 0 或负数说明调用方搞错了语义（想关闭衰减应当把 {@code decayRatio} 设为 0）。
   */
  @Test
  public void testAgingIntervalMustBePositive() {
    CacheAssertions.assertRejected("agingInterval=0",
            () -> new LfuAgingHashMapKVCache<String, String>(3, 0, 0.5));
    CacheAssertions.assertRejected("agingInterval=-1",
            () -> new LfuAgingHashMapKVCache<String, String>(3, -1, 0.5));
  }

  /**
   * 衰减比例必须落在 {@code [0, 1]} 内。
   */
  @Test
  public void testDecayRatioMustBeInUnitInterval() {
    CacheAssertions.assertRejected("decayRatio=-0.1",
            () -> new LfuAgingHashMapKVCache<String, String>(3, 1, -0.1));
    CacheAssertions.assertRejected("decayRatio=1.1",
            () -> new LfuAgingHashMapKVCache<String, String>(3, 1, 1.1));
  }

  /**
   * <b>{@code decayRatio = 0} 时与 {@link LfuKVCache} 行为完全一致</b> —— 一分不减，
   * 僵尸分数照样永久占位。
   * <p>
   * 保留这个取值是为了让调用方能按环境开关衰减，而不必在
   * {@link LfuAgingKVCache} 与 {@link LfuKVCache} 之间切换类型。
   */
  @Test
  public void testDecayRatioZeroIsPlainLfu() {
    LfuHashMapKVCache<String, String> plain = new LfuHashMapKVCache<>(2);
    LfuAgingHashMapKVCache<String, String> noDecay = new LfuAgingHashMapKVCache<>(2, 1, 0);

    for (KVCache<String, String> cache : Arrays.asList(plain, noDecay)) {
      cache.set("zombie", "v");
      for (int i = 0; i < 20; i++) {
        cache.get("zombie");
      }
      for (int i = 0; i < 10; i++) {
        cache.set("k" + i, "v");
        cache.get("k" + i);
      }
    }

    // 先断言分数，再调 get —— get 命中本身会把分数加 1
    Assert.assertEquals("decayRatio=0：分数一分没减", Long.valueOf(20L), noDecay.getKeyToVisitorCount().get("zombie"));

    Assert.assertNotNull("普通 LFU：20 分的僵尸留下", plain.get("zombie"));
    Assert.assertNotNull("decayRatio=0：同样留下，行为一致", noDecay.get("zombie"));
  }

  /**
   * <b>{@code decayRatio = 1} 的边界：一次衰减把所有分数一次性抹平。</b>
   * <p>
   * 算术上就是 {@code (long) (count * (1 - 1)) = 0}，于是每一档都落到 0 档、
   * 分档表被压成一档。取值 1 是 {@code decayRatio} 的上界（构造器只接受 {@code [0, 1]}），
   * 语义是"淘汰一次就把历史清零"，与 {@code decayRatio = 0}（永不衰减）正好是两端。
   */
  @Test
  public void testDecayRatioOneClearsAllScores() {
    // agingInterval=1：每次淘汰都紧跟一次衰减
    LfuAgingHashMapKVCache<String, String> cache = new LfuAgingHashMapKVCache<>(2, 1, 1);

    cache.set("a", "1");
    cache.set("b", "2");
    cache.get("a");
    cache.get("a");     // a 攒到 2 分，b 仍是 0 分
    Assert.assertEquals("前置条件：a 攒到 2 分", Long.valueOf(2L), cache.getKeyToVisitorCount().get("a"));
    Assert.assertEquals("前置条件：b 还是 0 分", Long.valueOf(0L), cache.getKeyToVisitorCount().get("b"));

    // 写第三条触发淘汰：0 分档里最旧的 b 先被淘汰，随后的衰减把所有分数归零
    cache.set("c", "3");

    Assert.assertNull("被淘汰的是 0 分的 b", cache.getKeyToVisitorCount().get("b"));
    Assert.assertEquals("a 的 2 分被一次性归零", Long.valueOf(0L), cache.getKeyToVisitorCount().get("a"));
    Assert.assertEquals("新来的 c 落在 0 档", Long.valueOf(0L), cache.getKeyToVisitorCount().get("c"));
    Assert.assertEquals("分档被压平成只剩 0 档一档", 1, cache.getVisitorCountToKeySet().size());
  }

  /**
   * 衰减与"写不计分"可以叠加使用：前者让分数失效，后者不让分数被无成本地制造出来。
   */
  @Test
  public void testDecayWithWriteNotCounting() {
    LfuAgingKVCache<String, String> cache = new LfuAgingHashMapKVCache<>(2, 2, 0.5, false);

    cache.set("a", "v");
    for (int i = 0; i < 10; i++) {
      cache.set("a", "v");                  // 反复覆盖写，写不计分
    }
    Assert.assertEquals("写不计分：a 的分数仍是 0", Long.valueOf(0L), cache.getKeyToVisitorCount().get("a"));

    cache.set("b", "v");
    cache.get("b");                         // b 有真实读需求
    Assert.assertEquals("b 的分数是 1", Long.valueOf(1L), cache.getKeyToVisitorCount().get("b"));

    cache.set("c", "v");                    // 满 → 淘汰最小档最旧的 a

    Assert.assertNull("被反复写但没人读的 a 被淘汰", cache.get("a"));
    Assert.assertNotNull("有真实读需求的 b 留下", cache.get("b"));
  }

  /**
   * 两张统计表可以在子类中换成别的实现，衰减参数对外可读。
   * <p>
   * 注入统计表的构造器是 {@code protected}（两张表必须始终相互同步，
   * 这个前提无法在构造时校验，因此不对外公开），这里用一个临时实现把它暴露出来。
   */
  @Test
  public void testInjectedStatisticStructures() {
    KVCache<String, Long> keyToVisitorCount = new HashMapKVCache<>();
    SortedKVCache<Long, LinkedHashSet<String>> visitorCountToKeySet = new TreeMapKVCache<>();
    LfuAgingKVCache<String, String> injected =
            new ExposedLfuAgingHashMapKVCache<>(keyToVisitorCount, visitorCountToKeySet, 2, 3, 0.5);

    Assert.assertEquals("衰减参数对外可读", 3L, injected.getAgingInterval());
    Assert.assertEquals("衰减参数对外可读", 0.5, injected.getDecayRatio(), 0.0);

    injected.set("a", "v");
    Assert.assertEquals("统计表被外部对象观察得到", Long.valueOf(0L), keyToVisitorCount.get("a"));

    injected.get("a");
    Assert.assertEquals(Long.valueOf(1L), keyToVisitorCount.get("a"));
  }

  /**
   * <b>衰减不依赖 {@code keySet()} 的迭代顺序</b>。
   * <p>
   * 注入的 {@link SortedKVCache} 故意让 {@code keySet()} 逆序迭代。若衰减直接按
   * {@code keySet()} 的顺序处理档位，先处理的 7 分会把 key 合并进 3 分档，
   * 随后轮到 3 分档时它会被<b>再减一次</b>（7 → 3 → 1）；正确结果是每个 key 一次衰减只生效一次。
   * <p>
   * 因此 {@code decay()} 自己排了序，不依赖注入结构是否按契约升序迭代。
   */
  @Test
  public void testDecayDoesNotDependOnKeySetIterationOrder() {
    SortedKVCache<Long, LinkedHashSet<String>> reverseKeySet = new DescendingKeySetSortedKVCache<>();
    LfuAgingKVCache<String, String> cache =
            new ExposedLfuAgingHashMapKVCache<>(new HashMapKVCache<>(), reverseKeySet, 3, 1, 0.5);

    cache.set("a", "v");
    for (int i = 0; i < 7; i++) {
      cache.get("a");                       // a 的分数 = 7
    }
    cache.set("d", "v");
    for (int i = 0; i < 3; i++) {
      cache.get("d");                       // d 的分数 = 3
    }
    cache.set("e", "v");                    // 写满 3 格，尚未淘汰

    Assert.assertEquals(Long.valueOf(7L), cache.getKeyToVisitorCount().get("a"));
    Assert.assertEquals(Long.valueOf(3L), cache.getKeyToVisitorCount().get("d"));

    cache.set("f", "v");                    // 满 → 淘汰 0 分的 e → 随后衰减一次

    Assert.assertEquals("只衰减过一次", 1L, cache.getEvictionCount());
    Assert.assertEquals("7 分减半成 3，不能被减两次", Long.valueOf(3L), cache.getKeyToVisitorCount().get("a"));
    Assert.assertEquals("3 分减半成 1", Long.valueOf(1L), cache.getKeyToVisitorCount().get("d"));
    Assert.assertEquals("新来的 f 从 0 起步", Long.valueOf(0L), cache.getKeyToVisitorCount().get("f"));
  }

  /**
   * 仅供测试：暴露 {@code LfuAgingHashMapKVCache} 注入两张统计表的 protected 构造器
   */
  private static class ExposedLfuAgingHashMapKVCache<K, V> extends LfuAgingKVCache<K, V> {
    private ExposedLfuAgingHashMapKVCache(KVCache<K, Long> keyToVisitorCount,
                                          SortedKVCache<Long, LinkedHashSet<K>> visitorCountToKeySet,
                                          int capacity, long agingInterval, double decayRatio) {
      super(keyToVisitorCount, visitorCountToKeySet, capacity, agingInterval, decayRatio,
              true, new HashMapKVCache<>());
    }
  }

  /**
   * 仅供测试：{@code keySet()} <b>不</b>按升序迭代的 {@link SortedKVCache}。
   * <p>
   * 其余语义全部复用 {@link SortedMapKVCache}（{@code getFirst()} 仍返回最小 key），
   * 只把 {@code keySet()} 换成逆序，用来把"衰减依赖迭代顺序"这一条单独暴露出来。
   */
  private static class DescendingKeySetSortedKVCache<V> extends SortedMapKVCache<Long, V> {

    /**
     * 与 {@code super} 持有的是同一个实例：这里只是为了拿到 {@code descendingKeySet()}。
     */
    private final NavigableMap<Long, V> sortedMap;

    private DescendingKeySetSortedKVCache() {
      this(new TreeMap<>());
    }

    private DescendingKeySetSortedKVCache(NavigableMap<Long, V> sortedMap) {
      super(sortedMap);
      this.sortedMap = sortedMap;
    }

    @Override
    public Set<Long> keySet() {
      return sortedMap.descendingKeySet();
    }

  }

}
