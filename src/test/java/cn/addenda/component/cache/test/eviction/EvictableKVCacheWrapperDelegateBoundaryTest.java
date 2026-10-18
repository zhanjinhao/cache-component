package cn.addenda.component.cache.test.eviction;

import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.KVCacheWrapper;
import cn.addenda.component.cache.TestDelegateAccess;
import cn.addenda.component.cache.eviction.EvictableKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuAgingHashMapKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuAgingLruKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuHashMapKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuLruKVCache;
import cn.addenda.component.cache.eviction.lru.LruHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruKHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruKVCache;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * {@code EvictableKVCache} 的<b>底层存储边界</b>测试。
 * <p>
 * 这个类把"淘汰策略的统计结构"和"真正存数据的 delegate"分成两条线，
 * 两边靠 {@code EvictableKVCache#containsKey} 的副作用<b>懒对齐</b>：
 * 发现 delegate 里没有这个 key，就顺手把统计清掉。
 * <p>
 * 于是有一条必须知道的性质：<b>统计只保证"访问过的部分"是对的</b>。
 * delegate 自己丢数据（过期、或者 delegate 自己也带淘汰）之后，
 * 在下次访问到那些 key 之前，<b>账本</b>条数会虚高；反过来，
 * delegate 是外部传进来的、本来就带着数据时，<b>账本</b>条数又会虚低。
 * 两种都靠后续访问慢慢追平，不会立刻一致。
 * <p>
 * 虚的始终是账本，不是 {@code size()}：{@code size()} 取自数据层，任何时候都等于
 * delegate 的真实条数。所以本类的断言要分两路读 —— 真实条数看 {@code size()}，
 * 账本看各实现自己的入口（LRU 是队列、LFU 是计数表，见 {@link LedgerView}）。
 * <p>
 * 这些用例就是把这个边界钉住 —— 后面要是拿这个类当基础做别的东西，
 * 得先知道"账本什么时候是准的"，以及 {@code size()} 为什么始终是准的。
 *
 * @author addenda
 */
public class EvictableKVCacheWrapperDelegateBoundaryTest {

  /**
   * 一个会自己丢数据的 delegate：最多留 {@code max} 条，超出就丢最旧的。
   * <p>
   * 用来模拟"底层存储自己也有淘汰/过期"的情况 —— 这时候上层统计不会收到任何通知。
   */
  private static class BoundedDelegate implements KVCache<String, String> {

    private final Map<String, String> map = new LinkedHashMap<>();
    private final int max;

    private BoundedDelegate(int max) {
      this.max = max;
    }

    @Override
    public void set(String k, String v) {
      map.put(k, v);
      while (map.size() > max) {
        Iterator<String> it = map.keySet().iterator();
        it.next();
        it.remove();
      }
    }

    @Override
    public boolean setnx(String k, String v) {
      if (map.containsKey(k)) {
        return false;
      }
      set(k, v);
      return true;
    }

    @Override
    public boolean containsKey(String k) {
      return map.containsKey(k);
    }

    @Override
    public String get(String k) {
      return map.get(k);
    }

    @Override
    public boolean delete(String k) {
      return map.remove(k) != null;
    }

    @Override
    public int size() {
      return map.size();
    }

    @Override
    public int capacity() {
      return max;
    }
  }

  // ------------------------------------------------------------------
  // delegate 自己丢数据 → 统计虚高 → 懒自愈
  // ------------------------------------------------------------------

  /**
   * 底层只剩 2 条了，而队列（本层账本）还留着 5 条 ——
   * <b>这个不一致是设计内的</b>，靠后续访问自愈。
   * <p>
   * {@code size()} 取自数据层，报的是真实条数；账本的条数要看队列。
   */
  @Test
  public void testDelegateThatDropsKeysLeavesQueueResidue() {
    LruOverDelegate<String, String> c = new LruOverDelegate<>(5, new BoundedDelegate(2));

    for (int i = 0; i < 5; i++) {
      c.set("k" + i, "v" + i);
    }

    Assert.assertEquals("数据层只剩 2 条，size() 如实报告", 2, c.size());
    Assert.assertEquals("底层确实只剩 2 条", 2, TestDelegateAccess.delegateOf(c).size());
    Assert.assertEquals("而队列里还留着 5 条，等访问时自愈", 5L, c.dequeSize());
  }

  /**
   * 访问一遍之后自愈：{@code containsKey} 发现底层没有就把统计清掉。
   */
  @Test
  public void testStatisticsSelfHealOnAccess() {
    LruKVCache<String, String> c = new LruOverDelegate<>(5, new BoundedDelegate(2));
    for (int i = 0; i < 5; i++) {
      c.set("k" + i, "v" + i);
    }

    int alive = 0;
    for (int i = 0; i < 5; i++) {
      if (c.containsKey("k" + i)) {
        alive++;
      }
    }

    Assert.assertEquals("底层只剩 2 条", 2, alive);
    Assert.assertEquals("统计追平了", (long) alive, c.size());
    Assert.assertEquals("和底层存储也对上了", TestDelegateAccess.delegateOf(c).size(), c.size());
  }

  /**
   * 自愈之后容量也回来了：虚高的统计会提前触发淘汰，
   * 被清干净之后就不该再误伤活着的 key。
   */
  @Test
  public void testSelfHealedStatisticsFreeCapacityAgain() {
    LruKVCache<String, String> c = new LruOverDelegate<>(3, new BoundedDelegate(1));
    c.set("a", "1");
    c.set("b", "2");
    c.set("c", "3");

    // 走一遍让它自愈
    c.containsKey("a");
    c.containsKey("b");
    c.containsKey("c");

    // 底层只留得住 1 条，统计应该已经收敛到 1
    Assert.assertEquals(1, c.size());
    Assert.assertEquals(TestDelegateAccess.delegateOf(c).size(), c.size());
  }

  /**
   * 两层 LRU 叠加：外层以为自己有 5 条，内层自己淘汰到只剩 2 条。
   */
  @Test
  public void testLayeredLruDiverges() {
    LruKVCache<String, String> inner = new LruHashMapKVCache<>(2);
    LruOverDelegate<String, String> outer = new LruOverDelegate<>(5, inner);

    for (int i = 0; i < 5; i++) {
      outer.set("k" + i, "v" + i);
    }

    Assert.assertEquals("数据层（内层）只剩 2 条", 2, outer.size());
    Assert.assertEquals("内层队列里也只剩 2 条", 2, inner.size());
    Assert.assertEquals("而外层队列还留着 5 条", 5L, outer.dequeSize());

    int alive = 0;
    for (int i = 0; i < 5; i++) {
      if (outer.containsKey("k" + i)) {
        alive++;
      }
    }
    Assert.assertEquals("真实存活 2 条", 2, alive);
    Assert.assertEquals("外层自愈到和内层一致", 2, outer.size());
  }

  // ------------------------------------------------------------------
  // delegate 本来就带着数据 → 统计虚低 → 访问时补登记
  // ------------------------------------------------------------------

  /**
   * 包装一个已经有数据的 cache：{@code size()} 如实报出数据层的那条，账本还是空的；
   * 访问之后自动补登记。
   * <p>
   * <b>同时是回归</b>：LFU 包装预置 delegate 时曾经直接在 {@code calculateKeyUsage} 里 NPE
   * （计数表里没有这些 key，拿 null 去拆箱）。LFU 的四个实现都在 {@link #newImpls()} 里。
   * <p>
   * 跑 {@link #newImpls()} 里的五个实现；{@link LruKHashMapKVCache} 的准入账本没有公开入口，
   * 只断言数据层。{@code LruGenerationHashMapKVCache} 不在表里：它的 delegate 是年轻代，
   * 而 {@code set} 写的是老年代，"预置 delegate"不是同一件事 —— 见类注释。
   */
  @Test
  public void testPrePopulatedDelegateIsRegisteredOnAccess() {
    for (Impl impl : newImpls()) {
      EvictableKVCache<String, String> c = impl.newCache();
      delegate(c).set("a", "1");

      Assert.assertTrue(impl.name + ": 存在数据", c.containsKey("a"));
      Assert.assertEquals(impl.name + ": 数据层有 1 条，size() 如实报告", 1, c.size());
      assertLedger(impl.name + ": 账本还没登记过它", impl, c, 0);

      Assert.assertEquals(impl.name + ": 读得到", "1", c.get("a"));
      assertLedger(impl.name + ": 访问之后补登记", impl, c, 1);
      Assert.assertEquals(impl.name + ": size() 还是数据层的 1 条", 1, c.size());
    }
  }

  /**
   * 统计虚低时写入一个<b>新</b> key：账本只登记这一条，数据层是"原有 1 条 + 新的 1 条"。
   * <p>
   * 两者此时<b>故意不同步</b>：{@code size()} 取自数据层（2），账本只有新写入的那条（1）。
   * 原有那条要等下次访问才补登记（见 {@link #testPrePopulatedDelegateIsRegisteredOnAccess()}）——
   * 本层只在"访问到它"时自愈，新写入不会顺手把旧数据补进账本。
   */
  @Test
  public void testPrePopulatedDelegatePlusNewKeyRegistersOnlyTheNewOne() {
    for (Impl impl : newImpls()) {
      EvictableKVCache<String, String> c = impl.newCache();
      delegate(c).set("a", "1");
      assertLedger(impl.name + ": 前置条件：账本还没登记原有那条", impl, c, 0);

      c.set("b", "2");

      Assert.assertEquals(impl.name + ": 数据层：原有 1 条 + 新写 1 条", 2, c.size());
      assertLedger(impl.name + ": 账本只登记新写入的那条，原有那条等访问时补", impl, c, 1);
      // 直接读 delegate，避免 get 触发补登记把上面的账本断言搅乱
      Assert.assertEquals(impl.name + ": 原有数据没被动", "1", delegate(c).get("a"));
      Assert.assertEquals(impl.name + ": 新数据写进去了", "2", delegate(c).get("b"));
    }
  }

  /**
   * 同上，但走 {@code set} 覆盖写：账本里也不该出现两条。
   */
  @Test
  public void testPrePopulatedDelegateOverwriteKeepsSync() {
    for (Impl impl : newImpls()) {
      EvictableKVCache<String, String> c = impl.newCache();
      delegate(c).set("a", "1");

      c.set("a", "1-new");

      Assert.assertEquals(impl.name + ": 值被覆盖", "1-new", c.get("a"));
      Assert.assertEquals(impl.name + ": 覆盖写之后数据层仍是 1 条", 1, c.size());
      assertLedger(impl.name + ": 不该被登记两次", impl, c, 1);
    }
  }

  /**
   * 第二组用例的实现清单：五个实现走同一组断言，账本的读法由 {@link LedgerView} 决定。
   * <ul>
   *   <li>{@link LruKHashMapKVCache} 的 {@code ledgerView} 为 {@code null}：数据层完全适用，
   *       但准入账本是私有的、没有公开入口 —— 这里不用反射去够它；</li>
   *   <li>{@code LruGenerationHashMapKVCache} 不登记：{@code delegateOf} 拿到的是年轻代，
   *       而 {@code set} 写入的是老年代（探针：预置年轻代后写新 key，新 key 不在这份 delegate 里），
   *       "预置 delegate → 写新 key"的状态布局与其它实现不同，不能共用断言。</li>
   * </ul>
   */
  private List<Impl> newImpls() {
    List<Impl> impls = new ArrayList<>();
    impls.add(new Impl("LruHashMapKVCache",
            () -> new ExposedLruHashMapKVCache(5), LedgerView.LRU_DEQUE));
    impls.add(new Impl("LfuHashMapKVCache",
            () -> new LfuHashMapKVCache<>(5), LedgerView.LFU_COUNT));
    impls.add(new Impl("LfuLruKVCache",
            () -> new LfuLruKVCache<>(5), LedgerView.LFU_COUNT));
    impls.add(new Impl("LfuAgingHashMapKVCache",
            () -> new LfuAgingHashMapKVCache<>(5, 100, 0.5), LedgerView.LFU_COUNT));
    impls.add(new Impl("LfuAgingLruKVCache",
            () -> new LfuAgingLruKVCache<>(5, 100, 0.5), LedgerView.LFU_COUNT));
    impls.add(new Impl("LruKHashMapKVCache",
            () -> new LruKHashMapKVCache<>(5, 1), null));
    return impls;
  }

  /**
   * 一次实现的登记：怎么造、账本怎么读。
   */
  private static final class Impl {
    private final String name;
    private final Supplier<EvictableKVCache<String, String>> factory;
    /**
     * {@code null} 表示没有公开的账本入口，只断言数据层
     */
    private final LedgerView ledgerView;

    private Impl(String name, Supplier<EvictableKVCache<String, String>> factory,
                 LedgerView ledgerView) {
      this.name = name;
      this.factory = factory;
      this.ledgerView = ledgerView;
    }

    private EvictableKVCache<String, String> newCache() {
      return factory.get();
    }
  }

  /**
   * 账本的观测口径：LRU 是队列长度，LFU 是计数表条数。
   */
  private enum LedgerView {
    LRU_DEQUE {
      @Override
      long sizeOf(EvictableKVCache<String, String> cache) {
        return ((ExposedLruHashMapKVCache) cache).ledgerSize();
      }
    },
    LFU_COUNT {
      @Override
      long sizeOf(EvictableKVCache<String, String> cache) {
        return ((LfuKVCache<String, String>) cache).getKeyToVisitorCount().size();
      }
    };

    abstract long sizeOf(EvictableKVCache<String, String> cache);
  }

  /**
   * 断言账本里登记了多少条。{@code ledgerView} 为 {@code null} 的实现只断言数据层 ——
   * 它的账本没有公开入口（LRU-K 的准入账本），不做反射。
   */
  private void assertLedger(String message, Impl impl, EvictableKVCache<String, String> cache,
                            long expected) {
    if (impl.ledgerView == null) {
      return;
    }
    Assert.assertEquals(message, expected, impl.ledgerView.sizeOf(cache));
  }

  private KVCache<String, String> delegate(EvictableKVCache<String, String> cache) {
    return TestDelegateAccess.delegateOf((KVCacheWrapper<String, String>) cache);
  }

  /**
   * 仅供测试：暴露命名实现 {@link LruHashMapKVCache} 的队列 —— {@code getLruDeque()} 是
   * protected，只对子类开放。
   */
  private static class ExposedLruHashMapKVCache extends LruHashMapKVCache<String, String> {
    private ExposedLruHashMapKVCache(int capacity) {
      super(capacity);
    }

    private long ledgerSize() {
      return getLruDeque().size();
    }
  }

  /**
   * 仅供测试：LRU 层 + 指定的底层存储。
   * <p>
   * {@link LruKVCache} 是抽象基类，{@code LruHashMapKVCache} 的底层存储由类名确定、
   * 不接受 delegate。要"包装一个已经有数据的缓存"、或包一个会自行丢数据的 delegate，
   * 就得继承基类自己传——这正是那个公开的扩展点。
   */
  private static class LruOverDelegate<K, V> extends LruKVCache<K, V> {
    private LruOverDelegate(int capacity, KVCache<K, V> kvCacheDelegate) {
      super(capacity, kvCacheDelegate);
    }

    /**
     * 队列长度（本层账本）。{@code size()} 取数据层之后，账本要从这里看。
     */
    private long dequeSize() {
      return getLruDeque().size();
    }
  }

}
