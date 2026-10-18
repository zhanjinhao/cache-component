package cn.addenda.component.cache.test.eviction;

import cn.addenda.component.cache.HashMapKVCache;
import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.KVCacheWrapper;
import cn.addenda.component.cache.MapKVCache;
import cn.addenda.component.cache.TestDelegateAccess;
import cn.addenda.component.cache.eviction.lfu.LfuAgingKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuHashMapKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuLruKVCache;
import cn.addenda.component.cache.eviction.lru.LruHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruKKVCache;
import cn.addenda.component.cache.eviction.lru.LruKVCache;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code EvictableKVCacheWrapper} 的两层关系测试：<b>数据层是唯一真相，统计层是它的派生视图</b>。
 * <p>
 * 三条要求，逐条钉住：
 * <ol>
 *   <li><b>对外只用数据层口径</b>：{@code size()}、{@code containsKey}、{@code get} 三者
 *       永远自洽，调用方看不到"查得到却算 0 条"这类矛盾；</li>
 *   <li><b>统计层懒对齐</b>：数据层多出来的 key（外部直接写入）在下次访问时补登记，
 *       数据层丢掉的 key（过期、服务端淘汰）在下次访问时摘掉残留；</li>
 *   <li><b>不一致不抛异常</b>：两层任何分歧都只降级、不失败 —— 代价是实际条数可能
 *       暂时超过 {@code capacity()}，随后逐步收敛。</li>
 * </ol>
 * 本文件里的每个用例都是"某个淘汰策略 + 一个我指定的数据层"（换数据层要继承抽象基类），
 * 这样才测得到两层分歧。契约与边界断言在 {@code EvictableKVCacheWrapperContractTest} 等文件里。
 *
 * @author addenda
 */
public class EvictableKVCacheWrapperConsistencyTest {

  private static final int CAPACITY = 3;

  /**
   * 参与横切断言的全部实现。每个实现都是"策略 + 由用例指定的数据层"。
   */
  private final List<Impl> impls = new ArrayList<>();

  private void register(String name, boolean setnxSupported, boolean computeIfAbsentSupported,
                        Factory factory) {
    impls.add(new Impl(name, setnxSupported, computeIfAbsentSupported, factory));
  }

  private void before() {
    impls.clear();
    // LruKKVCache 明确拒绝 setnx 与 computeIfAbsent（都抛 UOE）—— 它的写入要过准入闸门，
    // 与"不存在就写、写了就有"对不上。两个开关是分开的：以后可能出现只拒绝其中一个的实现。
    register("LruHashMapKVCache", true, true,
            delegate -> new LruOverDelegate<>(CAPACITY, delegate));
    register("LruKHashMapKVCache", false, false,
            delegate -> new LruKOverDelegate<>(CAPACITY, 1, delegate));
    register("LfuHashMapKVCache", true, true,
            delegate -> new LfuOverDelegate<>(CAPACITY, delegate));
    register("LfuAgingHashMapKVCache", true, true,
            delegate -> new LfuAgingOverDelegate<>(CAPACITY, 2, 0.5, delegate));
  }

  // ------------------------------------------------------------------
  // 一、对外只用数据层口径
  // ------------------------------------------------------------------

  /**
   * <b>数据层里有、统计层不知道的 key，对外照样查得到、算得进条数。</b>
   * <p>
   * 这正是"统计层是派生视图"的第一层含义：包装一个已有内容的存储（或存储被外部写入）时，
   * {@code containsKey} 与 {@code size()} 必须同时承认它，不能一个说有一个算 0 条。
   */
  @Test
  public void testDataLayerIsTheTruth() {
    before();
    for (Impl impl : impls) {
      HashMapKVCache<String, String> delegate = new HashMapKVCache<>();
      delegate.set("foreign", "v");
      KVCacheWrapper<String, String> cache = impl.newCache(delegate);

      // size() 必须在任何访问之前断言：get / containsKey 会顺手把统计层对齐，
      // 那之后再看就分不出"size() 到底取的是哪一层"了
      Assert.assertEquals(impl + ": size() 如实报告数据层的 1 条", 1, cache.size());
      Assert.assertTrue(impl + ": 数据层有的 key 应该查得到", cache.containsKey("foreign"));
      Assert.assertEquals(impl + ": 数据层有的 key 应该读得到", "v", cache.get("foreign"));
      Assert.assertEquals(impl + ": size() 与数据层条数相等", delegate.size(), cache.size());
    }
  }

  /**
   * <b>{@code containsKey} 与 {@code size()} 不允许自相矛盾</b>：前者说有的 key，
   * 后者必须算得进（{@code size() >= 1}）。
   * <p>
   * 走一遍"预置 → 写 → 读 → 数据层被外部删掉 → 再写"的序列，每一步都校验这两条不变量。
   */
  @Test
  public void testContainsKeyAndSizeNeverContradict() {
    before();
    for (Impl impl : impls) {
      HashMapKVCache<String, String> delegate = new HashMapKVCache<>();
      delegate.set("foreign", "v");
      KVCacheWrapper<String, String> cache = impl.newCache(delegate);

      cache.set("mine", "v");
      assertNoContradiction(impl, cache, delegate, "写完一条");
      cache.get("foreign");
      assertNoContradiction(impl, cache, delegate, "读过预置的 key");
      delegate.delete("mine");                       // 数据层被外部删掉
      assertNoContradiction(impl, cache, delegate, "数据层被外部删掉一条");
      cache.set("mine2", "v");
      assertNoContradiction(impl, cache, delegate, "再写一条");
    }
  }

  /**
   * 每一步都校验：{@code containsKey} 与 {@code get} 互为充要，且 {@code size()} 等于数据层条数。
   */
  private void assertNoContradiction(Impl impl, KVCache<String, String> cache,
                                     KVCache<String, String> delegate, String phase) {
    for (String k : new String[]{"foreign", "mine", "mine2"}) {
      if (cache.containsKey(k)) {
        Assert.assertNotNull(impl + "（" + phase + "）：" + k + " 查得到就必须读得到", cache.get(k));
        Assert.assertTrue(impl + "（" + phase + "）：" + k + " 查得到，size() 就得算得进",
                cache.size() >= 1);
      } else {
        Assert.assertNull(impl + "（" + phase + "）：" + k + " 查不到就必须读不到", cache.get(k));
      }
    }
    Assert.assertEquals(impl + "（" + phase + "）：size() 必须等于数据层条数",
            delegate.size(), cache.size());
  }

  /**
   * <b>数据层丢掉的 key 对外立即不可见</b>：{@code containsKey} 为 false、{@code get} 为 null、
   * {@code size()} 少了它 —— 哪怕统计层里还留着残留（残留由下次访问摘掉，见后面的对齐用例）。
   */
  @Test
  public void testExternalDeletionIsInvisibleToTheCaller() {
    before();
    for (Impl impl : impls) {
      KVCacheWrapper<String, String> cache = impl.newCache(new HashMapKVCache<>());

      cache.set("a", "1");
      cache.set("b", "2");
      Assert.assertEquals(impl + ": 前置条件", 2, cache.size());

      TestDelegateAccess.delegateOf(cache).delete("a");

      // 同理，size() 先断言：containsKey 发现底层没有就会把残留摘掉，之后再看就分不出来
      Assert.assertEquals(impl + ": size() 跟着数据层减一", 1, cache.size());
      Assert.assertEquals(impl + ": size() 始终等于数据层条数",
              TestDelegateAccess.delegateOf(cache).size(), cache.size());
      Assert.assertFalse(impl + ": 数据层没有的 key 查不到", cache.containsKey("a"));
      Assert.assertNull(impl + ": 数据层没有的 key 读不到", cache.get("a"));
    }
  }

  // ------------------------------------------------------------------
  // 二、不一致不抛异常
  // ------------------------------------------------------------------

  /**
   * <b>两层怎么分歧都不抛异常。</b>
   * <p>
   * 用一个"最多只留 2 条、满了挤掉最早的"存储，外加容量不同的缓存包装它，
   * 让统计层与数据层持续对不上，然后把完整的操作面过一遍。
   */
  @Test
  public void testNeverThrowsWhenTheTwoLayersDiverge() {
    before();
    for (Impl impl : impls) {
      KVCacheWrapper<String, String> cache = impl.newCache(new SelfDroppingDelegate(2));

      try {
        for (int i = 0; i < 8; i++) {
          cache.set("k" + i, "v" + i);
          cache.get("k" + i);
          cache.delete("k" + i);
          cache.set("k" + i, "v" + i);
          if (impl.computeIfAbsentSupported) {
            Assert.assertNotNull(impl + ": computeIfAbsent 不该返回 null（第 " + i + " 轮）",
                    cache.computeIfAbsent("c" + i, k -> "cv-" + k));
          }
          cache.remove("c" + i);
          cache.size();
          cache.capacity();
          if (impl.setnxSupported) {
            cache.setnx("s" + i, "sv" + i);
          }
        }
      } catch (RuntimeException e) {
        Assert.fail(impl + ": 两层不一致时不应抛异常，实际抛出 " + e.getClass().getSimpleName()
                + ": " + e.getMessage());
      }
    }
  }

  /**
   * <b>两层一致时容量上界严格成立</b>：这是"尽力而为"的基线 —— 数据层与统计层同步
   * （底层不会自行丢数据）时，{@code size()} 始终不超过 {@code capacity()}。
   */
  @Test
  public void testCapacityHoldsWhenTheLayersAgree() {
    before();
    for (Impl impl : impls) {
      KVCacheWrapper<String, String> cache = impl.newCache(new HashMapKVCache<>());

      for (int i = 0; i < 10; i++) {
        cache.set("k" + i, "v" + i);
        Assert.assertTrue(impl + ": 第 " + i + " 轮写完之后 size() 不应超过容量，实际 "
                + cache.size(), cache.size() <= CAPACITY);
        Assert.assertEquals(impl + ": size() 始终等于数据层条数",
                TestDelegateAccess.delegateOf(cache).size(), cache.size());
      }
    }
  }

  /**
   * <b>两层分歧时超容量是暂时的</b>：数据层自己丢数据、而统计层还没跟上时，
   * 淘汰可能挑不到可淘汰的对象，于是实际条数短暂超过 {@code capacity()}；
   * 把所有 key 访问一遍让统计层对齐之后，对外数字重新等于数据层。
   */
  @Test
  public void testOverCapacityIsTemporaryWhenTheLayersDiverge() {
    before();
    for (Impl impl : impls) {
      KVCacheWrapper<String, String> cache = impl.newCache(new SelfDroppingDelegate(1));

      for (int i = 0; i < 6; i++) {
        cache.set("k" + i, "v" + i);
      }
      Assert.assertEquals(impl + ": size() 始终等于数据层条数（允许暂时超过容量）",
              TestDelegateAccess.delegateOf(cache).size(), cache.size());

      for (int i = 0; i < 6; i++) {
        cache.get("k" + i);
      }
      Assert.assertEquals(impl + ": 访问一遍之后依然等于数据层条数",
              TestDelegateAccess.delegateOf(cache).size(), cache.size());
      Assert.assertTrue(impl + ": 数据层自己只留得住 1 条，实际 " + cache.size(),
              cache.size() <= 1);
    }
  }

  // ------------------------------------------------------------------
  // 三、统计层懒对齐（直接观察账本）
  // ------------------------------------------------------------------

  /**
   * <b>LRU：队列向数据层对齐。</b>
   * <ul>
   *   <li>数据层多出来的 key —— 访问时补登记；</li>
   *   <li>数据层丢掉的 key —— 访问时把队列里的残留摘掉。</li>
   * </ul>
   */
  @Test
  public void testLruQueueAlignsToTheDataLayer() {
    ExposedLruHashMapKVCache<String, String> cache = new ExposedLruHashMapKVCache<>(5);
    TestDelegateAccess.delegateOf(cache).set("a", "1");
    TestDelegateAccess.delegateOf(cache).set("b", "2");

    Assert.assertEquals("队列里还没有它们", 0L, cache.dequeSize());

    Assert.assertEquals("1", cache.get("a"));
    Assert.assertEquals("2", cache.get("b"));
    Assert.assertEquals("访问之后补登记进队列", 2L, cache.dequeSize());

    TestDelegateAccess.delegateOf(cache).delete("a");
    Assert.assertEquals("数据层丢了 a，队列还不知道", 2L, cache.dequeSize());
    Assert.assertFalse(cache.containsKey("a"));
    Assert.assertEquals("看一眼之后残留被摘掉", 1L, cache.dequeSize());
  }

  /**
   * <b>LRU：队列为空时不淘汰、也不抛异常。</b>
   * <p>
   * 数据层里的数据全是外部写入的，队列没登记过 —— 这时"满了"也挑不出淘汰对象，
   * 只能先写进去（实际条数暂时超出容量），等后续访问把队列喂起来再逐轮追平。
   */
  @Test
  public void testLruDoesNotEvictFromAnEmptyQueue() {
    ExposedLruHashMapKVCache<String, String> cache = new ExposedLruHashMapKVCache<>(2);
    TestDelegateAccess.delegateOf(cache).set("a", "1");
    TestDelegateAccess.delegateOf(cache).set("b", "2");
    Assert.assertEquals("队列是空的", 0L, cache.dequeSize());

    cache.set("c", "3");

    Assert.assertEquals("写进去了", 3, cache.size());
    Assert.assertEquals("暂时超过容量", 1, cache.size() - cache.capacity());
    Assert.assertEquals("队列里只登记了刚写的这条", 1L, cache.dequeSize());
  }

  /**
   * <b>LFU：计数表向数据层对齐</b>，与 LRU 的队列同理。
   */
  @Test
  public void testLfuCountTableAlignsToTheDataLayer() {
    LfuHashMapKVCache<String, String> cache = new LfuHashMapKVCache<>(5);
    TestDelegateAccess.delegateOf(cache).set("a", "1");
    TestDelegateAccess.delegateOf(cache).set("b", "2");

    Assert.assertEquals("计数表里还没有它们", 0L, ledgerSize(cache));

    Assert.assertEquals("1", cache.get("a"));
    Assert.assertEquals("2", cache.get("b"));
    Assert.assertEquals("访问之后补建档", 2L, ledgerSize(cache));

    TestDelegateAccess.delegateOf(cache).delete("a");
    Assert.assertEquals("数据层丢了 a，计数表还不知道", 2L, ledgerSize(cache));
    Assert.assertFalse(cache.containsKey("a"));
    Assert.assertEquals("看一眼之后残留被摘掉", 1L, ledgerSize(cache));
  }

  /**
   * <b>LFU：计数表为空时不淘汰、也不抛异常</b>（这条路径曾经会 NPE）。
   */
  @Test
  public void testLfuDoesNotThrowWithAnEmptyCountTable() {
    LfuHashMapKVCache<String, String> cache = new LfuHashMapKVCache<>(2);
    TestDelegateAccess.delegateOf(cache).set("a", "1");
    TestDelegateAccess.delegateOf(cache).set("b", "2");
    Assert.assertEquals("计数表是空的", 0L, ledgerSize(cache));

    cache.set("c", "3");

    Assert.assertEquals("写进去了", 3, cache.size());
    Assert.assertEquals("计数表只登记了刚写的这条", 1L, ledgerSize(cache));
  }

  // ------------------------------------------------------------------
  // 四、分层实现同样满足以上三条
  // ------------------------------------------------------------------

  /**
   * {@link LfuLruKVCache}（外层 LFU、内层 LRU）在<b>两层容量一致、不发生淘汰</b>的基线下，
   * 对外表现同样是数据层口径：{@code size()} 等于内层数据层的条数，{@code containsKey} /
   * {@code get} 与它不矛盾；把所有 key 读一遍（触发外层计分与内层重排）之后依然如此。
   * <p>
   * 两层容量<b>不一致</b>时（内层自行淘汰、残留计数靠访问自愈）是另一组场景，见
   * {@code LfuLruKVCacheTest}。
   */
  @Test
  public void testLayeredImplKeepsTheSameContract() {
    LfuLruKVCache<String, String> layered = new LfuLruKVCache<>(5);

    for (int i = 0; i < 5; i++) {
      layered.set("k" + i, "v" + i);
    }

    Assert.assertEquals("size() 等于内层数据层条数",
            TestDelegateAccess.delegateOf(layered).size(), layered.size());

    for (int i = 0; i < 5; i++) {
      if (layered.containsKey("k" + i)) {
        Assert.assertNotNull("查得到就必须读得到", layered.get("k" + i));
        Assert.assertTrue("查得到就必须算得进 size()", layered.size() >= 1);
      } else {
        Assert.assertNull("查不到就必须读不到", layered.get("k" + i));
      }
    }

    for (int i = 0; i < 5; i++) {
      layered.get("k" + i);
    }
    Assert.assertEquals("访问一遍之后 size() 依然等于数据层条数",
            TestDelegateAccess.delegateOf(layered).size(), layered.size());
  }

  /**
   * <b>{@code set(k, null)} 之后账本里也不能留。</b>
   * <p>
   * 这条必须直接读账本，不能拿 {@code size()} 当代理：{@code size()} 取自数据层，
   * 值一删它就归零了 —— 队列/计数表里留着幽灵条目，它照样报 0。
   * {@code LruKVCacheTest} 与 {@code EvictableKVCacheWrapperContractTest} 里同名用例的
   * {@code size()} 断言只覆盖"值删掉了"这一半，账本那一半由本用例补。
   * <p>
   * 后果不是"报数不准"，而是容量：淘汰触发条件是 {@code capacity() == 账本条数}，
   * 幽灵条目会被当成一条真实数据 —— 账本提前填满，活着的 key 被挤掉。
   */
  @Test
  public void testSetNullClearsTheLedger() {
    ExposedLruHashMapKVCache<String, String> lru = new ExposedLruHashMapKVCache<>(5);
    lru.set("a", "1");
    Assert.assertEquals("值写进去了", 1, lru.size());
    Assert.assertEquals("LRU 的账本（队列）里登记了", 1L, lru.dequeSize());

    lru.set("a", null);
    Assert.assertEquals("值删掉了", 0, lru.size());
    Assert.assertEquals("队列也要跟着清，否则容量会被幽灵条目撑满", 0L, lru.dequeSize());

    LfuHashMapKVCache<String, String> lfu = new LfuHashMapKVCache<>(5);
    lfu.set("a", "1");
    Assert.assertEquals("值写进去了", 1, lfu.size());
    Assert.assertEquals("LFU 的计数表里登记了", 1, lfu.getKeyToVisitorCount().size());

    lfu.set("a", null);
    Assert.assertEquals("值删掉了", 0, lfu.size());
    Assert.assertEquals("计数表也要跟着清", 0, lfu.getKeyToVisitorCount().size());
    Assert.assertEquals("档位表里那个空档也要摘掉", 0, lfu.getVisitorCountToKeySet().size());
  }

  /**
   * <b>{@code setnx(k, null)} 连账本都不碰</b>：不只数据层没写入，队列 / 计数表里也不能多出一条。
   * <p>
   * 与 {@link #testSetNullClearsTheLedger()} 是同一件事的两面：{@code set(k, null)} 是"删"，
   * 账本要跟着清；{@code setnx(k, null)} 是"无事可做"，账本压根不该动。
   * {@code EvictableKVCacheWrapperOperationBoundaryTest#testSetnxNullValueIsCompletelyNoOp()}
   * 只看得见数据层（{@code size()} 与底层条数同源），账本这一半只能在这里直接读。
   * <p>
   * 两个分支都钉：key 不存在时账本保持为空；key 已存在时返回 false，已有值与账本都不受影响。
   */
  @Test
  public void testSetnxNullDoesNotTouchTheLedger() {
    ExposedLruHashMapKVCache<String, String> lru = new ExposedLruHashMapKVCache<>(5);
    Assert.assertFalse("key 不存在：应该返回 false", lru.setnx("a", null));
    Assert.assertEquals("key 不存在：不该写入", 0, lru.size());
    Assert.assertEquals("LRU 的队列里也不该登记", 0L, lru.dequeSize());

    lru.set("b", "2");
    Assert.assertFalse("key 已存在：应该返回 false", lru.setnx("b", null));
    Assert.assertEquals("已有值不该被删", "2", lru.get("b"));
    Assert.assertEquals("已有值的队列项也不该被动", 1L, lru.dequeSize());

    LfuHashMapKVCache<String, String> lfu = new LfuHashMapKVCache<>(5);
    Assert.assertFalse("key 不存在：应该返回 false", lfu.setnx("a", null));
    Assert.assertEquals("key 不存在：不该写入", 0, lfu.size());
    Assert.assertEquals("计数表里也不该登记", 0, lfu.getKeyToVisitorCount().size());
    Assert.assertEquals("档位表里也不该多出空档", 0, lfu.getVisitorCountToKeySet().size());

    lfu.set("b", "2");
    Assert.assertFalse("key 已存在：应该返回 false", lfu.setnx("b", null));
    Assert.assertEquals("已有值不该被删", "2", lfu.get("b"));
    Assert.assertEquals("账本也不该被影响", 1, lfu.getKeyToVisitorCount().size());
    Assert.assertEquals("档位表也不该被影响", 1, lfu.getVisitorCountToKeySet().size());
  }

  /**
   * <b>包装一个"已经装着超过容量"的存储时，容量仍然收敛</b>。
   * <p/>
   * 这是"账本超过容量"那条路径：外部直接写进数据层的 key 会在下次访问时被补登记进账本
   * （设计明确支持"包装一个已有的存储"），账本因此可能超过 capacity。
   * 若触发条件写成 {@code capacity() == 账本条数}，那时等式<b>永不成立</b> ——
   * 该层此后再也不淘汰，写多少留多少，容量形同虚设。
   * <p/>
   * 断言的是对外可见的那条不变量：{@code size() <= capacity()}。
   */
  @Test
  public void testCapacityConvergesWhenDelegateIsPrefilledBeyondCapacity() {
    before();
    for (Impl impl : impls) {
      // 预置 2 倍容量的数据，模拟"包装一个已经有内容的存储"
      HashMapKVCache<String, String> storage = new HashMapKVCache<>();
      for (int i = 0; i < CAPACITY * 2; i++) {
        storage.set("ext" + i, "v" + i);
      }

      KVCache<String, String> cache = impl.newCache(storage);
      Assert.assertEquals(impl + ": 前置条件，数据层确实超容量", CAPACITY * 2, cache.size());

      // 逐个 get：按设计"下次访问时补登记"，账本被顶到超过容量
      for (int i = 0; i < CAPACITY * 2; i++) {
        Assert.assertNotNull(impl + ": 外部写入的值读得到", cache.get("ext" + i));
      }

      // get 只把数据层已有的 key 补登记进账本，不清理数据 ——
      // 淘汰要等到后面的写入触发，这里数据层必须一条不少
      Assert.assertEquals(impl + ": get 不该清理数据", CAPACITY * 2, cache.size());

      // 再写新 key：应把账本清到低于容量，而不是一条都不淘汰
      for (int i = 0; i < CAPACITY * 2; i++) {
        cache.set("mine" + i, "v" + i);
      }

      Assert.assertTrue(impl + ": 账本超过容量之后仍应收敛，size()=" + cache.size()
                      + " capacity=" + cache.capacity(),
              cache.size() <= cache.capacity());
    }
  }

  /**
   * <b>数据层多于统计层、且统计层完全没有这些数据时，淘汰只动统计层登记过的 key。</b>
   * <p>
   * 预置的数据只写进 delegate、没进过账本。淘汰按"账本里最该走的那个"挑，挑不到它们 ——
   * 于是外部数据在被访问（补登记）之前不会因为容量被淘汰。这既是"统计层是派生视图"的直接
   * 后果，也是"包装一个已有内容的存储"能安全工作的前提。
   * <p>
   * 与 {@link #testCapacityConvergesWhenDelegateIsPrefilledBeyondCapacity()} 的区别：那边先
   * 把外部数据逐个读一遍（补登记进账本）再写入，账本因此认识它们；这里<b>不读</b>，它们对
   * 淘汰完全不可见。
   */
  @Test
  public void testEvictionOnlyTouchesKeysTrackedByTheLedger() {
    before();
    for (Impl impl : impls) {
      // 前置：数据层有 CAPACITY*2 条外部数据，账本一条都不知道
      HashMapKVCache<String, String> storage = new HashMapKVCache<>();
      for (int i = 0; i < CAPACITY * 2; i++) {
        storage.set("ext" + i, "v" + i);
      }
      KVCache<String, String> cache = impl.newCache(storage);

      // 写足够多的新 key，必然触发多轮淘汰
      for (int i = 0; i < CAPACITY * 3; i++) {
        cache.set("mine" + i, "v" + i);
      }

      // 外部数据一条都不该被淘汰：它们不在账本里，淘汰挑不到
      for (int i = 0; i < CAPACITY * 2; i++) {
        Assert.assertEquals(impl + ": 外部数据不该被淘汰 ext" + i,
                "v" + i, storage.get("ext" + i));
      }

      // 账本只认得新写入的 key，只能留住容量那么多条
      int alive = 0;
      for (int i = 0; i < CAPACITY * 3; i++) {
        if (cache.containsKey("mine" + i)) {
          alive++;
        }
      }
      Assert.assertEquals(impl + ": 淘汰只发生在账本登记过的 key 上",
              CAPACITY, alive);

      // 数据层 = 外部 CAPACITY*2 条 + 账本里的 CAPACITY 条
      Assert.assertEquals(impl + ": 被淘汰的只有账本登记的 key",
              CAPACITY * 3, cache.size());
    }
  }

  private long ledgerSize(LfuKVCache<String, String> cache) {
    return cache.getKeyToVisitorCount().size();
  }

  /**
   * 造一个"策略 + 指定数据层"的实例。
   */
  private interface Factory {
    KVCacheWrapper<String, String> create(KVCache<String, String> delegate);
  }

  private static class Impl {

    private final String name;
    private final boolean setnxSupported;
    private final boolean computeIfAbsentSupported;
    private final Factory factory;

    private Impl(String name, boolean setnxSupported, boolean computeIfAbsentSupported,
                 Factory factory) {
      this.name = name;
      this.setnxSupported = setnxSupported;
      this.computeIfAbsentSupported = computeIfAbsentSupported;
      this.factory = factory;
    }

    private KVCacheWrapper<String, String> newCache(KVCache<String, String> delegate) {
      return factory.create(delegate);
    }

    @Override
    public String toString() {
      return name;
    }

  }

  /**
   * 模拟"存储会自行丢数据"：最多留 {@code capacity} 条，满了挤掉最早写入的。
   * <p>
   * 只提供一张带容量上限的 {@link LinkedHashMap}，KVCache 的语义全部复用
   * {@link MapKVCache}（它本来就负责"把 KV 约定翻译成 Map 操作"）。
   */
  private static class SelfDroppingDelegate extends MapKVCache<String, String> {

    private final int capacity;

    private SelfDroppingDelegate(int capacity) {
      super(new LinkedHashMap<String, String>(16, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
          return size() > capacity;
        }
      });
      this.capacity = capacity;
    }

    /**
     * 与 {@link MapKVCache} 不同：本实现真的有容量上限，如实报告。
     */
    @Override
    public int capacity() {
      return capacity;
    }

  }

  /**
   * 仅供测试：暴露抽象基类接受 delegate 的 protected 构造器，数据层由用例指定。
   */
  private static class LruOverDelegate<K, V> extends LruKVCache<K, V> {
    private LruOverDelegate(int capacity, KVCache<K, V> kvCacheDelegate) {
      super(capacity, kvCacheDelegate);
    }
  }

  private static class LruKOverDelegate<K, V> extends LruKKVCache<K, V> {
    private LruKOverDelegate(int capacity, int threshold, KVCache<K, V> kvCacheDelegate) {
      super(capacity, threshold, new LruHashMapKVCache<>(capacity * 2), kvCacheDelegate);
    }
  }

  private static class LfuOverDelegate<K, V> extends LfuKVCache<K, V> {
    private LfuOverDelegate(int capacity, KVCache<K, V> kvCacheDelegate) {
      super(capacity, kvCacheDelegate);
    }
  }

  private static class LfuAgingOverDelegate<K, V> extends LfuAgingKVCache<K, V> {
    private LfuAgingOverDelegate(int capacity, long agingInterval, double decayRatio,
                                 KVCache<K, V> kvCacheDelegate) {
      super(capacity, agingInterval, decayRatio, true, kvCacheDelegate);
    }
  }

  /**
   * 仅供测试：暴露 {@code LruHashMapKVCache} 的队列长度。
   * <p>
   * {@code size()} 取数据层之后，账本（队列）条数只能从子类里读 —— {@code getLruDeque()} 是 protected。
   */
  private static class ExposedLruHashMapKVCache<K, V> extends LruHashMapKVCache<K, V> {
    private ExposedLruHashMapKVCache(int capacity) {
      super(capacity);
    }

    private long dequeSize() {
      return getLruDeque().size();
    }
  }

}
