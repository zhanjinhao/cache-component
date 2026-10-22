package cn.addenda.component.cache.test.eviction.lru;

import cn.addenda.component.cache.HashMapKVCache;
import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.MapKVCache;
import cn.addenda.component.cache.eviction.lru.LruKHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruKKVCache;
import cn.addenda.component.cache.test.CacheAssertions;
import cn.addenda.component.cache.test.TestCapacityLimit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

/**
 * {@link LruKKVCache} 的行为测试：LRU-K —— 访问够 {@code threshold} 次才准进主缓存。
 * <p>
 * 这个实现最反直觉的地方是"写进去的值不一定读得到"，所以下面的用例大都在钉这一条。
 * <b>准入规则本身</b>（计数从哪一刻开始、写为什么只记 1 分）在
 * {@link LruKKVCacheAdmissionTest} 里。
 *
 * @author addenda
 */
public class LruKKVCacheTest {

  private static final int CAPACITY = 3;
  private static final int THRESHOLD = 2;

  private LruKKVCache<String, String> cache;

  @Before
  public void before() {
    cache = new LruKHashMapKVCache<>(CAPACITY, THRESHOLD);
  }

  /**
   * <b>门槛没到之前，set 是空转的</b>：值不落地，get 读不到。
   * <p>
   * 这是 LRU-K 的设计（挡住只写一次的冷数据），不是写丢了。
   */
  @Test
  public void testSetBelowThresholdDoesNotLand() {
    cache.set("a", "1");

    Assert.assertNull("只写过一次，还进不了缓存", cache.get("a"));
    Assert.assertFalse(cache.containsKey("a"));
    Assert.assertEquals("也没占用容量", 0, cache.size());
  }

  /**
   * 攒够次数之后才真正落地。
   * <p>
   * 次数第一段由 set 记成 1，后面每 get 一次 +1；
   * 到 2 之后再 set 就会写进主缓存。
   */
  @Test
  public void testSetLandsAfterThreshold() {
    cache.set("a", "1");                      // 计数 = 1
    Assert.assertNull(cache.get("a"));        // 计数 = 2

    cache.set("a", "1");                      // 2 >= threshold，落地
    Assert.assertEquals("够次数了，该读得到", "1", cache.get("a"));
    Assert.assertTrue(cache.containsKey("a"));
    Assert.assertEquals(1, cache.size());
  }

  /**
   * <b>重复 set 不会累加次数</b> —— 写只建档记 1 分，之后的写只做准入判定，不加分。
   * <p>
   * 所以门槛只能靠<b>未命中</b>的 get 攒，反复写同一个 key 是刷不上去的。
   * 准入规则的完整说明见 {@link LruKKVCacheAdmissionTest}。
   */
  @Test
  public void testRepeatedSetDoesNotAccumulateCount() {
    for (int i = 0; i < 10; i++) {
      cache.set("a", "1");
    }

    Assert.assertNull("写十次也攒不出次数，还是进不去", cache.get("a"));
  }

  /**
   * <b>未命中的</b> get 会累加次数，攒到门槛再 set 就落地。
   */
  @Test
  public void testGetAccumulatesCount() {
    LruKKVCache<String, String> c = new LruKHashMapKVCache<>(CAPACITY, 5);

    c.set("a", "1");                 // 1
    Assert.assertNull(c.get("a"));   // 2
    Assert.assertNull(c.get("a"));   // 3
    Assert.assertNull(c.get("a"));   // 4

    c.set("a", "1");                 // 4 < 5，还不到
    Assert.assertNull("差一次", c.get("a"));   // 5

    c.set("a", "1");                 // 5 >= 5，落地
    Assert.assertEquals("1", c.get("a"));
  }

  /**
   * 已经是主缓存的 key，set 不再受门槛限制（走的是 setWhenContainsKey）。
   */
  @Test
  public void testSetOnCachedKeyIsNormalOverwrite() {
    cache.set("a", "1");
    Assert.assertNull(cache.get("a"));
    cache.set("a", "1");
    Assert.assertEquals("1", cache.get("a"));

    cache.set("a", "1-new");
    Assert.assertEquals("命中之后就是普通覆盖写", "1-new", cache.get("a"));
  }

  /**
   * 容量满了照常按 LRU 淘汰。
   */
  @Test
  public void testEvictsOldestWhenFull() {
    LruKKVCache<String, String> c = new LruKHashMapKVCache<>(2, 1);

    c.set("a", "1");
    c.set("b", "2");
    // 按 a、b 的顺序读一遍，把 LRU 顺序摆回 [a, b]，下面该淘汰的就是 a
    Assert.assertEquals("1", c.get("a"));
    Assert.assertEquals("2", c.get("b"));
    Assert.assertEquals(2, c.size());

    c.set("c", "3");

    Assert.assertNull("最旧的 a 被淘汰", c.get("a"));
    Assert.assertEquals("2", c.get("b"));
    Assert.assertEquals("3", c.get("c"));
  }

  /**
   * <b>显式删除会把访问历史一起清掉</b>，下次要从头攒次数。
   */
  @Test
  public void testDeleteClearsVisitHistory() {
    cache.set("a", "1");                // 计数 = 1
    Assert.assertNull(cache.get("a"));   // 计数 = 2
    cache.set("a", "1");                // 落地
    Assert.assertEquals("1", cache.get("a"));

    Assert.assertTrue(cache.delete("a"));
    Assert.assertNull(cache.get("a"));

    // 如果历史没被清掉，计数应该还 >= 2，这次 set 会直接落地
    cache.set("a", "1");
    Assert.assertNull("历史清掉了，要从头攒次数", cache.get("a"));
  }

  /**
   * 门槛为 1 时等价于普通 LRU：写过就进。
   */
  @Test
  public void testThresholdOneBehavesLikePlainLru() {
    LruKKVCache<String, String> c = new LruKHashMapKVCache<>(2, 1);

    c.set("a", "1");
    Assert.assertEquals("threshold=1，写过就能读到", "1", c.get("a"));
  }

  /**
   * set 对 null 的处理，见 {@code KVCache} 上的契约：
   * key 一律拒绝，value 为 null 一律表示"没有值"。
   * <p>
   * LRU-K 这边多一层：{@code set(k, null)} 走的是 {@code delete}，
   * 连<b>访问历史</b>也要一起清掉 —— 否则"置空"之后次数还留着，
   * 下次写进来会直接越过门槛，等于一次删除把进度送给了未来的写入。
   * <p>
   * {@code setnx} 不在这里测：这个实现不支持它，见
   * {@link #testSetnxIsUnsupported()}。
   */
  @Test
  public void testNullKeyAndNullValue() {
    // 先把 a 攒够次数送进缓存
    cache.set("a", "1");
    Assert.assertNull(cache.get("a"));
    cache.set("a", "1");
    Assert.assertEquals("1", cache.get("a"));

    // size() 紧跟在 set(k, null) 后面断言：get/containsKey 会触发
    // EvictableKVCache#containsKey 里的自愈，把残留的统计顺手清掉
    cache.set("a", null);
    Assert.assertEquals("队列要清", 0, cache.size());
    Assert.assertNull(cache.get("a"));
    Assert.assertFalse(cache.containsKey("a"));

    // 历史也该清了：没清的话这次 set 会直接落地
    cache.set("a", "1");
    Assert.assertNull("访问历史没清干净的话这里就能读到值了", cache.get("a"));

    // 对不存在的 key 置空也不该留下历史
    cache.set("b", null);
    cache.set("b", "2");
    Assert.assertNull("set(b, null) 不该留下历史帮 b 越过门槛", cache.get("b"));

    // key 为 null 一律拒绝
    CacheAssertions.assertRejected("LruKKVCache: set", () -> cache.set(null, "v"));
    CacheAssertions.assertRejected("LruKKVCache: set(k, null)", () -> cache.set(null, null));
    CacheAssertions.assertRejected("LruKKVCache: get", () -> cache.get(null));
    CacheAssertions.assertRejected("LruKKVCache: containsKey", () -> cache.containsKey(null));
    CacheAssertions.assertRejected("LruKKVCache: delete", () -> cache.delete(null));
  }

  /**
   * <b>{@code set(k, null)} 只清这个 key 已攒的次数，其它 key 的准入进度不受影响。</b>
   * <p>
   * 与 {@link #testDeleteClearsVisitHistory()} 的区别：那边清的是"已经落地、随后被删"的 key；
   * 这里清的是<b>还在攒次数、尚未落地</b>的 key —— 它留下的唯一痕迹就是计数表里的那一条。
   * <p>
   * 验证方式靠"下次写会不会落地"：门槛为 2。a 若没被清，旧计数 2 会让紧接着的 set 直接落地；
   * b 若被连带清掉，它后面就凑不够 2 次、写不进去。
   */
  @Test
  public void testSetNullClearsOnlyItsOwnAccumulatedCount() {
    // a 攒到门槛（1 次写 + 1 次读），但只有 set 能触发落地
    cache.set("a", "1");                 // a 计数 = 1
    Assert.assertNull(cache.get("a"));   // a 计数 = 2，仍未落地
    // b 攒 1 次
    cache.set("b", "1");                 // b 计数 = 1
    Assert.assertEquals("前置条件：两条都还没落地", 0, cache.size());

    cache.set("a", null);                // 清掉 a 的计数

    // a 若没被清，旧计数 2 会让这次 set 直接落地
    cache.set("a", "1");
    Assert.assertNull("set(a, null) 必须清掉 a 已攒的次数", cache.get("a"));

    // b 的进度还在：再读一次即到门槛，紧接着的 set 应该落地
    cache.get("b");                      // b 计数 = 2
    cache.set("b", "1");                 // 达标 → 落地
    Assert.assertEquals("别的 key 已攒的次数不该被 set(a, null) 清掉",
            "1", cache.get("b"));
  }

  /**
   * <b>{@code setnx} 明确不支持，一律抛 {@code CacheUnsupportedOperationException}。</b>
   * <p>
   * LRU-K 的写入要过准入闸门（攒够 {@code threshold} 次访问才落地），
   * 而 {@code setnx} 的语义是"不存在就写、写了就有"，两者直接对撞。
   * 门槛 >= 2 时它本来会恒返回 false，调用方分不清是"已存在"还是"没落地"，
   * 拿它做互斥会永远抢不到还不报错 —— 与其这样，不如明确说不支持。
   * <p>
   * 参数不校验：整个操作都不支持，null key 同样抛 {@code CacheUnsupportedOperationException}
   * （见 {@code KVCache} 的异常口径第 1 条）。
   */
  @Test
  public void testSetnxIsUnsupported() {
    CacheAssertions.assertNotSupported("LruKKVCache: setnx",
            () -> new LruKHashMapKVCache<String, String>(CAPACITY, 2).setnx("a", "v"));
    CacheAssertions.assertNotSupported("LruKKVCache: setnx(k, null)",
            () -> new LruKHashMapKVCache<String, String>(CAPACITY, 2).setnx("a", null));
    CacheAssertions.assertNotSupported("LruKKVCache: setnx(threshold=1)",
            () -> new LruKHashMapKVCache<String, String>(CAPACITY, 1).setnx("a", "v"));

    // 参数不校验：整个操作都不支持，null key 也抛 CacheUnsupportedOperationException
    CacheAssertions.assertNotSupported("LruKKVCache: setnx(null, v)",
            () -> new LruKHashMapKVCache<String, String>(CAPACITY, 2).setnx(null, "v"));
  }

  /**
   * <b>{@code clearKeyStatistic} 里"key 在队列里但底层取不到值"那条分支</b>：
   * 底层把值弄丢时，队列项连同"已准入 key 上可能残留的那份计数"一起清掉。
   * <p>
   * 这条分支在普通用例里<b>不可达</b>：默认 delegate 是普通 HashMap，不会自己丢数据；
   * 而且新设计下命中不记账，正常路径不会再给已准入的 key 建记录。这里做两件事造场景：
   * 换一个"可以按指令丢数据"的 delegate；再直接往注入的计数表里写一条残留记录
   * （并发下"读到未命中 → 别人恰好准入"的竞态仍可能留下它）。
   * <p>
   * 残留计数必须被自愈清掉 —— 否则它会带到下一次 {@code set}，让一个"其实已经不在缓存里"
   * 的 key 直接过闸落地。
   */
  @Test
  public void testClearKeyStatisticDropsCountWhenKeyLeftInQueueButValueIsGone() {
    DroppableKVCache delegate = new DroppableKVCache();
    // 自己持有计数表，才能注入"已准入 key 的残留计数"
    HashMapKVCache<String, Integer> visitorCount = new HashMapKVCache<>();
    LruKKVCache<String, String> c = new LruKOverDroppable<>(CAPACITY, THRESHOLD,
            visitorCount, delegate);

    // 准入：set 建档 1 分 → 未命中 get 到 2 分 → 再 set 才落地（threshold=2）
    c.set("a", "1");
    c.get("a");
    c.set("a", "1");
    Assert.assertEquals("准入之后值落地了", "1", c.get("a"));

    // 准入时计数已清零；命中不建档，所以账本里没有 a
    Assert.assertFalse("准入之后计数记录已删除", visitorCount.containsKey("a"));
    Assert.assertEquals("命中不会重新建档", 0, visitorCount.size());

    // 注入一条"已准入 key 的残留计数"（并发竞态可能留下），再让底层悄悄丢值
    visitorCount.set("a", THRESHOLD);
    delegate.drop("a");

    // 自愈：key 还在队列里 → 残留计数必须一并清掉
    Assert.assertFalse("自愈被触发：底层说没有", c.containsKey("a"));
    Assert.assertFalse("队列里的 key 的残留计数也要清掉", visitorCount.containsKey("a"));

    // 计数清掉之后，再来 set 只是"第一次接触"，不该过闸落地
    c.set("a", "1-again");
    Assert.assertNull("计数被清掉了，所以这次不落地 —— 若残留，它会直接过闸",
            c.get("a"));
  }

  /**
   * <b>{@code remove} 不经过会记账的读路径</b>：删一个 key，不该毁掉另一个 key
   * 攒下的准入进度。
   * <p>
   * 接口默认实现是"get 后再 delete"，而本类的 {@code get} <b>未命中时会记账</b>。
   * 删 k 时那次白做的读会给 k 补一份计数记录，账本恰好满时就挤掉队头那条 —— 那是
   * <b>另一个 key 攒了一半的准入进度</b>；随后 {@code delete(k)} 只清 k 自己那份，
   * 别人的进度回不来。本类因此覆盖了 remove：值直接读底层存储，不走记账的 get。
   * <p>
   * 下面两组唯一的差别，就是 P 攒分期间有没有插入那次 {@code remove}，
   * 而且删的是一个<b>从未存在过</b>的 key —— 把一个"看起来完全无关的调用"
   * 造成的影响单独暴露出来。
   */
  @Test
  public void testRemoveDoesNotDestroyAnotherKeysAdmissionProgress() {
    Assert.assertTrue("对照组：不做那次 remove，P 攒够分后写一次就落地",
            admissionSurvivesWithoutRemove());
    Assert.assertTrue("实验组：中间删一个从未存在过的 key，P 的进度不该受影响",
            admissionSurvivesAfterRemove());
  }

  /**
   * 主容量 1 → 账本容量 2；门槛 {@link #THRESHOLD} 分。
   * <p>
   * P 攒到门槛分之后，Q 再占掉账本最后一个槽位，账本恰好满 —— 此时任何一次
   * "会给新 key 记账"的读都会把队头（P）挤出去。
   *
   * @return P 写一次之后有没有落进缓存
   */
  private boolean admissionSurvivesWithoutRemove() {
    LruKHashMapKVCache<String, String> c = new LruKHashMapKVCache<>(1, THRESHOLD);
    c.get("P");
    c.get("P");     // P 攒到门槛分
    c.get("Q");     // 账本此时刚好满
    c.set("P", "p");
    return c.containsKey("P");
  }

  /**
   * 同上，只在 {@code set} 之前插入一次 {@code remove("Z")} —— Z 从未进过缓存。
   */
  private boolean admissionSurvivesAfterRemove() {
    LruKHashMapKVCache<String, String> c = new LruKHashMapKVCache<>(1, THRESHOLD);
    c.get("P");
    c.get("P");
    c.get("Q");
    Assert.assertNull("删一个从未存在过的 key 返回 null", c.remove("Z"));
    c.set("P", "p");
    return c.containsKey("P");
  }

  /**
   * 模拟"数据层自己把值弄丢了、而上层统计收不到通知"的 delegate。
   * <p>
   * <b>继承的规则</b>：KV 语义全部来自 {@link MapKVCache} —— null value 按删除、
   * {@code setnx} 不覆盖已有 key、不限制条数。
   * <p>
   * <b>自身差异</b>：多了 {@link #drop(String)}（绕过外层 LRU-K 直接删数据层，
   * 模拟过期 / 被外部删除）；{@link #capacity()} 报一个有限的大容量，表示"真实存储也有
   * 自己的上限" —— 本替身是普通 HashMap，不会因此主动淘汰，这个值在本用例里只是声明。
   * <p>
   * <b>安全级别</b>：无（数据层是普通 HashMap），只在单线程测试里使用。
   */
  private static class DroppableKVCache extends MapKVCache<String, String> {

    private DroppableKVCache() {
      super(new HashMap<>());
    }

    /**
     * 绕过外层 LRU-K，直接把值从数据层丢掉 —— 模拟过期 / 被外部删除。
     */
    private void drop(String key) {
      delete(key);
    }

    /**
     * 与 {@link MapKVCache} 不同：本替身给自己一个明确的容量，便于用例描述数据层上限。
     */
    @Override
    public int capacity() {
      return 1 << 20;
    }

  }

  /**
   * {@code threshold} 取 0 或负数要在<b>构造时</b>就被拒绝。
   * <p>
   * 理由是这个值<b>没有意义</b>，而不是"行为不好"：准入资格问的是"攒够几次访问"，
   * 而计数从第一次访问起就是 1，所以 1 是最小有意义取值；0 与负数会被第一次访问自带的
   * 那个 1 直接满足，参数本身读不成"门槛"。
   * <p>
   * <b>{@code threshold == 1} 必须仍然被接受</b>：它的行为与 0/负数<b>完全一样</b>
   * （第一次写就落地，即"写过即进"，等价于普通 LRU），但它是合法取值 ——
   * {@code LruKVCacheTest} 正是拿 {@code threshold=1} 把本类当普通 LRU 用的。
   * 所以校验的理由只能是"定义域"，不能是"行为是否与普通 LRU 相同"，否则会把 1 一起拒掉。
   */
  @Test
  public void testThresholdMustBeAtLeastOne() {
    CacheAssertions.assertRejected("threshold = 0", () -> new LruKHashMapKVCache<>(CAPACITY, 0));
    CacheAssertions.assertRejected("threshold = -1", () -> new LruKHashMapKVCache<>(CAPACITY, -1));
    CacheAssertions.assertRejected("threshold = Integer.MIN_VALUE",
            () -> new LruKHashMapKVCache<>(CAPACITY, Integer.MIN_VALUE));

    Assert.assertNotNull("threshold = 1 是合法的（虽然等价于普通 LRU）",
            new LruKHashMapKVCache<>(CAPACITY, 1));
  }

  /**
   * <b>{@code computeIfAbsent} 与 {@code setnx} 一样被明确拒绝。</b>
   * <p>
   * 两者的写动作最终都是 {@code set}，都要过准入闸门，因此结果可能并不落在缓存里。
   * 不拒绝的话调用方会拿到一个非 null 返回值、随后 {@code get} 却是 null：
   * <pre>
   * cache.computeIfAbsent("k", k -&gt; "outer")  -&gt;  "outer"
   * cache.get("k")                             -&gt;  null
   * </pre>
   * 也不能改成"没落地就返回 null"——{@code null} 在本方法上已经表示"算出来是 null、没有写入"，
   * 再让它兼任第二种含义，调用方就分不清了。
   */
  @Test
  public void testComputeIfAbsentIsNotSupported() {
    CacheAssertions.assertNotSupported("computeIfAbsent",
            () -> cache.computeIfAbsent("k", k -> "v"));

    // 门槛为 1 时行为等价于普通 LRU，但方法本身同样不支持 ——
    // 不应让同一方法的行为随调优参数在"可用"与"抛异常"之间切换
    LruKKVCache<String, String> thresholdOne = new LruKHashMapKVCache<>(CAPACITY, 1);
    CacheAssertions.assertNotSupported("threshold=1 时同样不支持",
            () -> thresholdOne.computeIfAbsent("k", k -> "v"));

    // 参数不校验：null key 同样抛 CacheUnsupportedOperationException（与 setnx 同口径）
    CacheAssertions.assertNotSupported("computeIfAbsent(null, fn)",
            () -> cache.computeIfAbsent(null, k -> "v"));
  }

  /**
   * <b>容量上限对"组合出来的容量"同样有效。</b>
   * <p>
   * {@code KVCache.UNLIMITED_CAPACITY} 是所有 cache 都要遵守的容量上限
   * （见其说明：合法性判断一律以它为准）；本类的访问计数账本是一个<b>独立的 cache 实例</b>、
   * 容量取主缓存的两倍，于是主缓存的容量被间接限制在上限的一半以内 ——
   * 不是"乘 2 不溢出就行"。抛 {@code CacheIllegalArgumentException} 而不是
   * {@link ArithmeticException}（{@code Math.multiplyExact} 那种）：这是"调用方给的参数
   * 不合法"，本组件里同类问题一律用 CacheIllegalArgumentException；消息也会说清是"乘 2 才超的"，
   * 而不是回绕成负数后被容量校验报成"缓存的大小需要 &gt; 0"。
   * <p>
   * "主缓存自己不合法"那一侧在取 {@code int} 上界时写不出来，这里<b>临时把上限调低</b>
   * （{@link TestCapacityLimit}）再构造，跑完在 {@code finally} 里恢复。
   * 注意本类的两倍校验在构造参数求值时先行，所以这个输入同样由它拦下。
   */
  @Test
  public void testCapacityOverUnlimitedIsRejected() {
    int maxMain = KVCache.UNLIMITED_CAPACITY / 2;

    CacheAssertions.assertRejected("主缓存没超，但账本的两倍超了",
            () -> new LruKHashMapKVCache<>(maxMain + 1, 1));

    // 边界：恰好一半时，账本容量恰好等于上限
    Assert.assertNotNull("恰好一半可用", new LruKHashMapKVCache<>(maxMain, 1));

    int lowered = 10_000;
    int original = TestCapacityLimit.setUnlimitedCapacity(lowered);
    try {
      Assert.assertEquals("上限应已被临时调低，否则下面的输入仍构造得出来",
              lowered, KVCache.UNLIMITED_CAPACITY);
      CacheAssertions.assertRejected("超过上限的容量同样拒绝",
              () -> new LruKHashMapKVCache<>(lowered + 1, 1));
    } finally {
      TestCapacityLimit.setUnlimitedCapacity(original);
    }
  }

  /**
   * 仅供测试：暴露 {@code LruKKVCache} 接受 delegate 的 protected 构造器。
   */
  private static class LruKOverDroppable<K, V> extends LruKKVCache<K, V> {
    private LruKOverDroppable(int capacity, int threshold,
                              KVCache<K, Integer> visitorCount, KVCache<K, V> kvCacheDelegate) {
      super(capacity, threshold, visitorCount, kvCacheDelegate);
    }
  }

}
