package cn.addenda.component.cache.test.eviction.lfu;

import cn.addenda.component.cache.HashMapKVCache;
import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.MapKVCache;
import cn.addenda.component.cache.SortedKVCache;
import cn.addenda.component.cache.TreeMapKVCache;
import cn.addenda.component.cache.TestDelegateAccess;
import cn.addenda.component.cache.eviction.lfu.LfuAgingKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuKVCache;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;

/**
 * 两张统计表<b>短期失步</b>时，{@link LfuKVCache} 必须"不抛异常、也不死循环"。
 * <p>
 * {@code LfuKVCache} 的类注释把"两表不一致时清到账本低于容量为止、<b>绝不抛异常</b>"
 * 写成了承诺，而且 {@code calculateKeyUsage} 为这个状态专门做了兜底。但另外两条路径漏了：
 * <ul>
 *   <li>{@code clearKeyStatistic}：计数表说 k 在 n 档，而档位表里已经没有 n 档 —— 原来的
 *       {@code keyList.remove(k)} 会抛 NPE。它还会被
 *       {@code EvictableKVCache#containsKey} 的<b>自愈路径</b>调用，在那里抛异常尤其糟；</li>
 *   <li>{@code removeMinOldest}：最小档里没有 key（空集合残留）—— 原来的
 *       {@code keyList.iterator().next()} 会抛 {@code NoSuchElementException}。</li>
 * </ul>
 * <p>
 * <b>这两个状态正常路径构造不出来</b>（三个写入点都会顺手删掉空档），所以要靠注入统计结构
 * 直接摆出来 —— 注入用的构造器本来就是 {@code protected}（见其说明），这里用临时子类把它暴露出来，
 * 顺带拿到两张表的引用，可以手工制造不一致。
 * <p>
 * <b>{@code removeMinOldest} 那条还多一层要求</b>：调用方是
 * {@code setWhenNonContainsKey} 里的 while 循环，所以"提前返回"的分支必须先把取不到 key
 * 的那个档位删掉，否则循环条件一点没变 → 死循环。{@link #testSetDoesNotSpinWhenMinTierIsEmpty}
 * 就是钉这一条的，带了 timeout 兜底。
 *
 * @author addenda
 */
public class LfuKVCacheLedgerInconsistencyTest {

  private static final int CAPACITY = 1;

  /**
   * {@code clearKeyStatistic} 遇到"计数表有、档位表没有"时不该抛异常。
   * <p>
   * 路径：{@code delete(k)} 会先调 {@code clearKeyStatistic}。原来在
   * {@code keyList.remove(k)} 上抛 NPE（{@code keyList} 为 null）。
   */
  @Test
  public void testClearKeyStatisticToleratesMissingTier() {
    KVCache<String, Long> keyToVisitorCount = new HashMapKVCache<>();
    SortedKVCache<Long, LinkedHashSet<String>> visitorCountToKeySet =
            new TreeMapKVCache<>();
    ExposedLfuKVCache<String, String> cache =
            new ExposedLfuKVCache<>(keyToVisitorCount, visitorCountToKeySet, CAPACITY);

    cache.set("a", "1");
    Assert.assertEquals("前置条件：a 以 0 次入档", Long.valueOf(0L), keyToVisitorCount.get("a"));

    // 手工制造失步：档位表里的 0 档整档消失，计数表仍然说 a 在 0 档
    visitorCountToKeySet.delete(0L);
    Assert.assertNull("前置条件：0 档已经没了", visitorCountToKeySet.get(0L));
    Assert.assertEquals("前置条件：计数还在", Long.valueOf(0L), keyToVisitorCount.get("a"));

    Assert.assertTrue("删除应该照常成功，不该因为账本失步而抛异常", cache.delete("a"));
    Assert.assertNull("残留的计数要清掉", keyToVisitorCount.get("a"));
    Assert.assertNull("值也要真的删掉", cache.get("a"));
  }

  /**
   * 同样的失步状态走<b>自愈路径</b>（{@code containsKey} 发现底层没有该 key）时也不该抛异常。
   * <p>
   * 底层已经被清掉、账本还留着 —— 这正是 {@code EvictableKVCacheWrapper#containsKey}
   * 要负责修的那种不一致，可修的过程中自己先抛 NPE 就说不过去了。
   */
  @Test
  public void testContainsKeySelfHealingToleratesMissingTier() {
    KVCache<String, Long> keyToVisitorCount = new HashMapKVCache<>();
    SortedKVCache<Long, LinkedHashSet<String>> visitorCountToKeySet =
            new TreeMapKVCache<>();
    ExposedLfuKVCache<String, String> cache =
            new ExposedLfuKVCache<>(keyToVisitorCount, visitorCountToKeySet, CAPACITY);

    cache.set("a", "1");
    visitorCountToKeySet.delete(0L);
    TestDelegateAccess.delegateOf(cache).delete("a");   // 底层自己没了（过期 / 服务端淘汰）

    Assert.assertFalse("自愈路径不该抛异常", cache.containsKey("a"));
    Assert.assertNull("顺手把残留计数清掉", keyToVisitorCount.get("a"));
  }

  /**
   * 最小档是个空集合时，{@code removeMinOldest} 不该抛 {@code NoSuchElementException}，
   * 且必须把那个空档清掉（留给调用方的循环"腾出进度"）。
   * <p>
   * 顺带钉住返回值：这种"没淘汰到任何东西"的情况必须返回 {@code false} ——
   * {@code LfuAgingKVCache} 正是靠它判断"缓存有没有真的受压"，
   * 进而在空转时不推进衰减年龄。
   */
  @Test
  public void testRemoveMinOldestToleratesEmptyTier() {
    KVCache<String, Long> keyToVisitorCount = new HashMapKVCache<>();
    SortedKVCache<Long, LinkedHashSet<String>> visitorCountToKeySet =
            new TreeMapKVCache<>();
    ExposedLfuKVCache<String, String> cache =
            new ExposedLfuKVCache<>(keyToVisitorCount, visitorCountToKeySet, CAPACITY);

    cache.set("a", "1");
    keyToVisitorCount.set("a", 5L);                       // 把 a 挪到 5 档
    visitorCountToKeySet.delete(0L);
    visitorCountToKeySet.set(5L, new LinkedHashSet<>());  // 5 档是个空集合

    Assert.assertNull("空档位取不到 key，什么都没淘汰 -> null", cache.evict());

    Assert.assertFalse("空档位要被删掉，否则调用方的循环永远等不到进度",
            visitorCountToKeySet.containsKey(5L));
    Assert.assertEquals("一个 key 都没真正淘汰", Long.valueOf(5L), keyToVisitorCount.get("a"));
    Assert.assertEquals("数据层原样保留", "1", cache.get("a"));
  }

  /**
   * <b>返回值是被淘汰掉的那个 value</b> —— 与上面那条互补，避免"永远返回 null"也能通过。
   * <p>
   * 给出三种情况：真的淘汰到了值 / 账本已空 / 数据层没有这条 key（用一个就删不掉的
   * delegate 模拟"数据层不配合"）。
   */
  @Test
  public void testRemoveMinOldestReturnsTheEvictedValue() {
    KVCache<String, Long> keyToVisitorCount = new HashMapKVCache<>();
    SortedKVCache<Long, LinkedHashSet<String>> visitorCountToKeySet =
            new TreeMapKVCache<>();
    ExposedLfuKVCache<String, String> cache =
            new ExposedLfuKVCache<>(keyToVisitorCount, visitorCountToKeySet, CAPACITY);
    cache.set("a", "1");

    Assert.assertEquals("真淘汰到了值 -> 把值交出来", "1", cache.evict());
    Assert.assertNull("a 的计数记录删掉了", keyToVisitorCount.get("a"));
    Assert.assertEquals("计数表清空", 0, keyToVisitorCount.size());
    Assert.assertNull("0 档摘掉了", visitorCountToKeySet.get(0L));
    Assert.assertEquals("档位表清空", 0, visitorCountToKeySet.size());

    Assert.assertNull("账本已空 -> null", cache.evict());
    Assert.assertEquals("空转之后计数表仍为空", 0, keyToVisitorCount.size());
    Assert.assertEquals("空转之后档位表仍为空", 0, visitorCountToKeySet.size());

    // 对照：delegate 删不掉时，同一次调用只清账本、返回 null
    NoDeleteDelegate<String, String> noDelete = new NoDeleteDelegate<>();
    KVCache<String, Long> keyToVisitorCount2 = new HashMapKVCache<>();
    SortedKVCache<Long, LinkedHashSet<String>> visitorCountToKeySet2 = new TreeMapKVCache<>();
    ExposedLfuKVCache<String, String> cache2 = new ExposedLfuKVCache<>(
            keyToVisitorCount2, visitorCountToKeySet2, CAPACITY, noDelete);
    cache2.set("a", "1");
    Assert.assertNull("delegate 删不掉 -> null", cache2.evict());

    // 即使数据层不配合，账本也必须照常推进 —— 否则调用方的淘汰循环等不到进度
    Assert.assertNull("同一次调用仍然把计数删掉了", keyToVisitorCount2.get("a"));
    Assert.assertEquals("计数表清空", 0, keyToVisitorCount2.size());
    Assert.assertEquals("档位表清空", 0, visitorCountToKeySet2.size());
    Assert.assertEquals("而数据层其实还在：delegate 不配合", "1", noDelete.get("a"));
  }

  /**
   * <b>空档位不能让写入陷入死循环。</b>
   * <p>
   * {@code setWhenNonContainsKey} 的淘汰循环条件同时看"计数表条数 >= 容量"和"档位表非空"。
   * 如果 {@code removeMinOldest} 发现空档后直接 return，条件完全不变，循环会一直转下去 ——
   * 比原来抛 {@code NoSuchElementException} 更糟（异常至少能被发现）。
   * 所以这里用 {@code timeout} 兜底：真死循环时用例会失败而不是把构建挂死。
   */
  @Test(timeout = 10000)
  public void testSetDoesNotSpinWhenMinTierIsEmpty() {
    KVCache<String, Long> keyToVisitorCount = new HashMapKVCache<>();
    SortedKVCache<Long, LinkedHashSet<String>> visitorCountToKeySet =
            new TreeMapKVCache<>();
    ExposedLfuKVCache<String, String> cache =
            new ExposedLfuKVCache<>(keyToVisitorCount, visitorCountToKeySet, CAPACITY);

    cache.set("a", "1");
    // 容量 1、计数表已有 1 条，下一次写就会触发淘汰；此时最小档是个空集合
    visitorCountToKeySet.set(0L, new LinkedHashSet<>());

    cache.set("b", "1");

    Assert.assertTrue("这一轮没有真正淘汰任何数据，a 应该还在", cache.containsKey("a"));
    Assert.assertTrue("b 要正常写进来", cache.containsKey("b"));

    // 账本状态要在任何 get 之前断言：命中会计一次分，把 b 挪到 1 档。
    // 空档被摘掉、b 正常入档；a 的计数是已知残留 ——
    // 空档分支只删档位、不动计数表（否则又要判断"该清哪一条"），
    // 那份残留会在后续访问时由 calculateKeyUsage/自愈对齐。
    Assert.assertEquals("b 以 0 次入档", Long.valueOf(0L), keyToVisitorCount.get("b"));
    Assert.assertEquals("a 的残留计数还在", Long.valueOf(0L), keyToVisitorCount.get("a"));
    Assert.assertEquals("计数表两条：a 的残留 + b", 2, keyToVisitorCount.size());
    Assert.assertEquals("档位表只剩 0 档一档", 1, visitorCountToKeySet.size());
    Assert.assertEquals("0 档里只有 b", new LinkedHashSet<>(Arrays.asList("b")),
            visitorCountToKeySet.get(0L));

    Assert.assertEquals("值是原样写入的", "1", cache.get("b"));
  }

  /**
   * 暴露 {@code LfuKVCache} 注入两张统计表的 {@code protected} 构造器，并放出淘汰入口。
   */
  private static class ExposedLfuKVCache<K, V> extends LfuKVCache<K, V> {

    private ExposedLfuKVCache(KVCache<K, Long> keyToVisitorCount,
                              SortedKVCache<Long, LinkedHashSet<K>> visitorCountToKeySet,
                              int capacity) {
      this(keyToVisitorCount, visitorCountToKeySet, capacity, new HashMapKVCache<>());
    }

    private ExposedLfuKVCache(KVCache<K, Long> keyToVisitorCount,
                              SortedKVCache<Long, LinkedHashSet<K>> visitorCountToKeySet,
                              int capacity, KVCache<K, V> kvCacheDelegate) {
      super(keyToVisitorCount, visitorCountToKeySet, capacity, true, kvCacheDelegate);
    }

    private V evict() {
      return removeMinOldest();
    }

  }

  // ============================================================
  // 衰减的触发条件：只有"真的淘汰了数据"才算缓存受压
  // ============================================================

  /**
   * 底层存储拒绝删除：模拟"账本里的 key 已被数据层自行删掉"，淘汰只清账本、不动数据。
   * <p>
   * KV 语义全部复用 {@link MapKVCache}，只覆盖"删除"这一对入口。
   */
  private static class NoDeleteDelegate<K, V> extends MapKVCache<K, V> {

    private NoDeleteDelegate() {
      super(new HashMap<>());
    }

    /**
     * 删不掉：数据层不配合，账本以为删掉了。
     */
    @Override
    public boolean delete(K k) {
      return false;
    }

    /**
     * 与 delete 保持一致：这是一个"什么都拿不走"的数据层。
     * <p>
     * 必须覆盖 —— {@link MapKVCache#remove(Object)} 会真的把值删掉并返回它。
     */
    @Override
    public V remove(K k) {
      return null;
    }

    @Override
    public int capacity() {
      return 1 << 30;
    }

  }

  private static class ExposedLfuAgingKVCache<K, V> extends LfuAgingKVCache<K, V> {

    private ExposedLfuAgingKVCache(KVCache<K, Long> keyToVisitorCount,
                                   SortedKVCache<Long, LinkedHashSet<K>> visitorCountToKeySet,
                                   int capacity, long agingInterval, double decayRatio,
                                   KVCache<K, V> kvCacheDelegate) {
      super(keyToVisitorCount, visitorCountToKeySet, capacity, agingInterval, decayRatio,
              true, kvCacheDelegate);
    }

  }

  /**
   * <b>淘汰"空转"时不该推进衰减年龄。</b>
   * <p>
   * 触发一次淘汰不等于"缓存受了压"：{@code removeMinOldest} 挑出来的 key 可能早已被数据层
   * 自行删除（过期、服务端淘汰），那一次只把账本残留清掉，一条数据都没驱逐。
   * 原实现在这里无条件 {@code ++evictionCount}，于是"账本里的 key 被数据层删光"这种
   * 与压力无关的场景也会一点点耗掉 {@code agingInterval} 这个节流阀，让衰减在空闲时提前发生。
   * <p>
   * 用一个拒绝删除的 delegate 把"空转"钉死：淘汰发生、账本被清、但数据层一条都没少。
   */
  @Test
  public void testAgingDoesNotAdvanceOnShamEviction() {
    KVCache<String, Long> keyToVisitorCount = new HashMapKVCache<>();
    SortedKVCache<Long, LinkedHashSet<String>> visitorCountToKeySet =
            new TreeMapKVCache<>();
    NoDeleteDelegate<String, String> delegate = new NoDeleteDelegate<>();
    LfuAgingKVCache<String, String> cache = new ExposedLfuAgingKVCache<>(
            keyToVisitorCount, visitorCountToKeySet, 2, 1, 0.5, delegate);

    cache.set("a", "1");
    cache.set("b", "1");
    cache.get("a");
    cache.get("b");
    Assert.assertEquals("前置条件：a 的分数是 1", Long.valueOf(1L),
            cache.getKeyToVisitorCount().get("a"));

    // 容量 2 已满，这次写会触发一次淘汰；delegate 拒绝删除 -> 空转
    cache.set("c", "1");

    Assert.assertEquals("空转不该推进淘汰计数", 0L, cache.getEvictionCount());
    Assert.assertEquals("空转不该触发衰减，b 的分数要保持 1", Long.valueOf(1L),
            cache.getKeyToVisitorCount().get("b"));
    Assert.assertEquals("数据层一条都没删掉", 3, delegate.size());
  }

}
