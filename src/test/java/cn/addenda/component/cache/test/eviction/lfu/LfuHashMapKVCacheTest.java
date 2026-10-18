package cn.addenda.component.cache.test.eviction.lfu;

import cn.addenda.component.base.util.SleepUtils;
import cn.addenda.component.cache.HashMapKVCache;
import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.KVCacheWrapper;
import cn.addenda.component.cache.SortedKVCache;
import cn.addenda.component.cache.TreeMapKVCache;
import cn.addenda.component.cache.TestDelegateAccess;
import cn.addenda.component.cache.eviction.lru.LruHashMapKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuHashMapKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuLruKVCache;
import cn.addenda.component.cache.test.CacheAssertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * LFU 淘汰策略的行为测试：淘汰由访问次数决定，次数相同时按进入该档的先后（FIFO）。
 * <p>
 * 淘汰逻辑继承自抽象基类 {@link LfuKVCache}，两个实现都不引入自己的淘汰规则。
 * 同一组断言跑这两个实现：
 * <ul>
 *   <li>{@link LfuHashMapKVCache}：底层是一张普通 HashMap；</li>
 *   <li>{@link LfuLruKVCache}：底层是一个 LRU，<b>两层容量给成一致</b>。</li>
 * </ul>
 * 之所以能跑同一组断言：两层容量一致时，LFU 层总是先腾出位置，底层 LRU 永远轮不到
 * 自行淘汰，因此对外表现与前者一致。这是 {@code LfuLruKVCache} 的推荐配置，
 * 也是本类的"等价"断言；其两层容量<b>不一致</b>时的行为（底层自行淘汰、统计残留、自愈）
 * 另有专门的文件 {@code LfuLruKVCacheTest}。
 * <p>
 * 循环里只使用两个实现都有定义的口径：{@code size()}（取自数据层，报底层真实条数）、
 * {@code capacity()}、{@code getKvCacheDelegate().size()}（底层存储条数，与前者相等）。
 * 加衰减的版本在 {@code LfuAgingHashMapKVCacheTest} / {@code LfuAgingLruKVCacheTest}；
 * 契约层面的断言（null 语义、null key）在 {@code EvictableKVCacheWrapperContractTest} 里。
 *
 * @author addenda
 */
public class LfuHashMapKVCacheTest {

  private static final int CAPACITY = 2;

  private static final String HASH_MAP_IMPL = "LfuHashMapKVCache";

  /**
   * 两层容量给成一致的 {@link LfuLruKVCache}
   */
  private static final String LRU_IMPL = "LfuLruKVCache(两层容量一致)";

  /**
   * 全部实现。key 为断言消息里的名字，失败时能直接看出是哪个实现。
   * <p>
   * 元素类型用 {@link KVCacheWrapper} 而非 {@code KVCache}：断言里要观察底层存储
   * （{@code getKvCacheDelegate()}），那个方法在包装类上，不在 {@code KVCache} 接口上。
   */
  private final Map<String, KVCacheWrapper<String, String>> caches = new LinkedHashMap<>();

  @Before
  public void before() {
    caches.clear();
    register(HASH_MAP_IMPL, new LfuHashMapKVCache<>(CAPACITY));
    register(LRU_IMPL, new LfuLruKVCache<>(CAPACITY));
  }

  private void register(String name, KVCacheWrapper<String, String> cache) {
    caches.put(name, cache);
  }

  @Test
  public void testSetGetDelete() {
    for (String name : caches.keySet()) {
      KVCacheWrapper<String, String> cache = caches.get(name);

      cache.set("a", "1");
      Assert.assertEquals(name + ": 写进去应该能读出来", "1", cache.get("a"));
      Assert.assertTrue(name + ": 写进去应该能查到", cache.containsKey("a"));
      Assert.assertEquals(name + ": 写一条 size 是 1", 1, cache.size());

      Assert.assertTrue(name + ": 删掉应该返回 true", cache.delete("a"));
      Assert.assertNull(name + ": 删掉之后再读应该是 null", cache.get("a"));
      Assert.assertFalse(name + ": 删掉之后再查应该是 false", cache.containsKey("a"));
      Assert.assertEquals(name + ": 删掉之后计数也要跟着减", 0, cache.size());
    }
  }

  /**
   * 容量即构造时传入的值；淘汰按访问次数，次数低的先走。
   */
  @Test
  public void testCapacityAndEvictsLeastFrequentlyUsed() {
    for (String name : caches.keySet()) {
      KVCacheWrapper<String, String> cache = caches.get(name);
      Assert.assertEquals(name + ": 容量即构造时传入的值", CAPACITY, cache.capacity());

      cache.set("a", "1");
      cache.set("b", "2");

      cache.get("a");
      cache.get("a");

      cache.set("c", "3");

      Assert.assertEquals(name + ": a 次数高，应该活下来", "1", cache.get("a"));
      Assert.assertNull(name + ": b 没被读过，该淘汰", cache.get("b"));
      Assert.assertEquals(name + ": c 写进来了", "3", cache.get("c"));
    }
  }

  /**
   * 底层没有 ttl，写进去的值不会自己过期。
   */
  @Test
  public void testNoExpiry() throws Exception {
    for (String name : caches.keySet()) {
      KVCacheWrapper<String, String> cache = caches.get(name);

      cache.set("a", "1");
      SleepUtils.sleep(TimeUnit.MILLISECONDS, 50);
      Assert.assertEquals(name + ": 没有传 ttl，不该过期", "1", cache.get("a"));
    }
  }

  /**
   * 次数相同时淘汰最旧的（这一档退化成 FIFO）。
   */
  @Test
  public void testEvictsOldestOnTie() {
    for (String name : caches.keySet()) {
      KVCacheWrapper<String, String> cache = caches.get(name);

      cache.set("a", "1");
      cache.set("b", "2");
      Assert.assertEquals(name + ": 写满容量", CAPACITY, cache.size());

      cache.set("c", "3");

      Assert.assertNull(name + ": 三条都是 0 次，淘汰最旧的 a", cache.get("a"));
      Assert.assertEquals(name + ": b 还在", "2", cache.get("b"));
      Assert.assertEquals(name + ": c 写进来了", "3", cache.get("c"));
    }
  }

  /**
   * 覆盖写也算一次访问，次数涨上去之后不会被淘汰。
   */
  @Test
  public void testSetOnExistingKeyCountsAsVisit() {
    for (String name : caches.keySet()) {
      KVCacheWrapper<String, String> cache = caches.get(name);

      cache.set("a", "1");
      cache.set("b", "2");

      cache.set("a", "1-new");
      cache.set("a", "1-new");

      cache.set("c", "3");

      Assert.assertEquals(name + ": a 被写过三次，不该被淘汰", "1-new", cache.get("a"));
      Assert.assertNull(name + ": 该被淘汰的是 b", cache.get("b"));
    }
  }

  /**
   * 新写进来的 key 从 0 次起步，所以容量一满它就是下一个候选 —— LFU 的正常表现。
   */
  @Test
  public void testNewKeyStartsAtZeroAndIsFirstOut() {
    for (String name : caches.keySet()) {
      KVCacheWrapper<String, String> cache = caches.get(name);

      cache.set("hot", "1");
      for (int i = 0; i < 5; i++) {
        cache.get("hot");
      }

      cache.set("cold", "2");
      Assert.assertEquals(name + ": cold 写进来了", "2", cache.get("cold"));

      cache.set("cold2", "3");

      Assert.assertEquals(name + ": hot 次数最高，稳坐", "1", cache.get("hot"));
      Assert.assertNull(name + ": cold 只有 0 次，被淘汰", cache.get("cold"));
      Assert.assertEquals(name + ": cold2 写进来了", "3", cache.get("cold2"));
    }
  }

  /**
   * 删掉一条就腾出一格，下一次写不会触发淘汰。
   */
  @Test
  public void testDeleteFreesCapacity() {
    for (String name : caches.keySet()) {
      KVCacheWrapper<String, String> cache = caches.get(name);

      cache.set("a", "1");
      cache.set("b", "2");

      Assert.assertTrue(name + ": 删掉 b", cache.delete("b"));

      cache.set("c", "3");
      Assert.assertEquals(name + ": 没触发淘汰，a 还在", "1", cache.get("a"));
      Assert.assertEquals(name + ": c 写进来了", "3", cache.get("c"));
      Assert.assertEquals(name + ": 正好两条", CAPACITY, cache.size());
    }
  }

  /**
   * <b>LFU 自己的账本与底层存储必须同步</b>：淘汰由 LFU 决定，底层跟着减；
   * 删除也两层一起减。
   * <p>
   * 这条是"两层容量一致"这个前提的直接体现：如果底层 LRU 自行淘汰了，
   * 它的条数会小于 LFU 的账本，而 LFU 收不到通知 —— 记账就分叉了。
   */
  @Test
  public void testBackingStoreStaysInSyncWithLfuStatistics() {
    for (String name : caches.keySet()) {
      KVCacheWrapper<String, String> cache = caches.get(name);

      cache.set("a", "1");
      cache.set("b", "2");
      Assert.assertEquals(name + ": 两层条数一致", CAPACITY, TestDelegateAccess.delegateOf(cache).size());

      cache.set("c", "3");        // 满 → LFU 先淘汰 a，再写入

      Assert.assertEquals(name + ": 淘汰由 LFU 做，底层仍与账本一致",
              cache.size(), TestDelegateAccess.delegateOf(cache).size());
      Assert.assertEquals(name + ": 写入后正好满", CAPACITY, TestDelegateAccess.delegateOf(cache).size());
      Assert.assertNull(name + ": a 被淘汰", cache.get("a"));
      Assert.assertEquals(name + ": b 还在", "2", cache.get("b"));
      Assert.assertEquals(name + ": c 写进来了", "3", cache.get("c"));

      Assert.assertTrue(name + ": 删掉 b", cache.delete("b"));
      Assert.assertEquals(name + ": LFU 账本减一", 1, cache.size());
      Assert.assertEquals(name + ": 底层也减一", 1, TestDelegateAccess.delegateOf(cache).size());
    }
  }

  /**
   * <b>{@code countWriteAsVisit = false} 时写不涨分</b>，反复覆盖写刷不出分数。
   * <p>
   * 场景：一个 key 被调用方反复覆盖写（写穿透 / 幂等重写），另一个 key 只有一次真实读需求。
   * 计分时前者分数更高，被挤走的是后者；不计分时前者分数停在 0，被挤走的才是它。
   */
  @Test
  public void testCountWriteAsVisitFlag() {
    assertCountWriteAsVisitFlag(HASH_MAP_IMPL,
            new LfuHashMapKVCache<>(2, true), new LfuHashMapKVCache<>(2, false));
    assertCountWriteAsVisitFlag(LRU_IMPL,
            new LfuLruKVCache<>(2, true), new LfuLruKVCache<>(2, false));
  }

  private void assertCountWriteAsVisitFlag(String name,
                                           KVCache<String, String> counting,
                                           KVCache<String, String> notCounting) {
    for (KVCache<String, String> c : Arrays.asList(counting, notCounting)) {
      c.set("cold", "v");
      for (int i = 0; i < 10; i++) {
        c.set("cold", "v");          // 反复覆盖写同一个 key
      }
      c.set("hot", "v");
      c.get("hot");                  // hot 有真实读需求，但只有一次
      c.set("new", "v");             // 满了，淘汰分数最低的
    }

    Assert.assertNull(name + ": 计分时：被写 11 次的 cold 分数更高，走的是 hot", counting.get("hot"));
    Assert.assertNotNull(name + ": 计分时：该留下的是被反复写的 cold", counting.get("cold"));

    Assert.assertNotNull(name + ": 不计分时：hot 有真实读需求，应该留下", notCounting.get("hot"));
    Assert.assertNull(name + ": 不计分时：被反复写的 cold 分数停在 0，该淘汰的是它", notCounting.get("cold"));
  }

  /**
   * 两张统计表可以在子类中换成别的实现，替换后淘汰照常工作。
   * <p>
   * 注入统计表的构造器是 {@code protected}（两张表必须始终相互同步，
   * 这个前提无法在构造时校验，因此不对外公开），这里用临时实现把它暴露出来，
   * 顺带就能从外面观察这两张表的内容。
   */
  @Test
  public void testInjectedStatisticStructures() {
    assertInjectedStatisticStructures(HASH_MAP_IMPL, injected(HASH_MAP_IMPL));
    assertInjectedStatisticStructures(LRU_IMPL, injected(LRU_IMPL));
  }

  private void assertInjectedStatisticStructures(String name, Injected injected) {
    KVCache<String, String> cache = injected.cache;
    KVCache<String, Long> keyToVisitorCount = injected.keyToVisitorCount;
    SortedKVCache<Long, LinkedHashSet<String>> visitorCountToKeySet = injected.visitorCountToKeySet;

    cache.set("a", "1");

    Assert.assertEquals(name + ": 新 key 以 0 次入档", Long.valueOf(0L), keyToVisitorCount.get("a"));
    Assert.assertTrue(name + ": 0 这一档里应该有 a", visitorCountToKeySet.containsKey(0L));
    Assert.assertTrue(name + ": 0 这一档里应该有 a", visitorCountToKeySet.get(0L).contains("a"));

    cache.get("a");
    Assert.assertEquals(name + ": 读过一次之后次数 +1", Long.valueOf(1L), keyToVisitorCount.get("a"));
    Assert.assertFalse(name + ": 旧档里不该再留着 a", visitorCountToKeySet.containsKey(0L));
    Assert.assertTrue(name + ": a 应该挪到 1 档", visitorCountToKeySet.get(1L).contains("a"));
    Assert.assertEquals(name + ": 值仍然读得到", "1", cache.get("a"));
  }

  /**
   * set / setnx 对 null 的处理，见 {@code KVCache} 上的契约：
   * key 一律拒绝，value 为 null 一律表示"没有值"。
   * <p>
   * LFU 这边除了值和 size，还要确认<b>两张统计表也清干净了</b> —— 计数残留会让<b>账本</b>虚高，
   * 而淘汰触发比的正是账本条数，跟着失准（{@code size()} 取自数据层，看不出这件事，
   * 所以下面断言的是注入进来的那两张表本身）。
   * 对底层是 LRU 的那个实现，还多一层要求：底层自己的队列也得清（见
   * {@code getKvCacheDelegate().size()} 的断言）。
   */
  @Test
  public void testNullKeyAndNullValue() {
    assertNullKeyAndNullValue(HASH_MAP_IMPL, injected(HASH_MAP_IMPL));
    assertNullKeyAndNullValue(LRU_IMPL, injected(LRU_IMPL));
  }

  private void assertNullKeyAndNullValue(String name, Injected injected) {
    KVCacheWrapper<String, String> c = injected.cache;
    KVCache<String, Long> keyToVisitorCount = injected.keyToVisitorCount;
    SortedKVCache<Long, LinkedHashSet<String>> visitorCountToKeySet = injected.visitorCountToKeySet;

    c.set("a", "1");
    c.get("a");
    Assert.assertEquals(name + ": 前置条件", Long.valueOf(1L), keyToVisitorCount.get("a"));

    // size() 紧跟在 set(k, null) 后面断言：get/containsKey 会触发
    // EvictableKVCache#containsKey 里的自愈，把残留的统计顺手清掉
    c.set("a", null);
    Assert.assertEquals(name + ": 计数要清掉", 0, c.size());
    Assert.assertNull(name + ": 第一张表里不该还留着 a", keyToVisitorCount.get("a"));
    Assert.assertFalse(name + ": 第二张表里的空档也要删掉", visitorCountToKeySet.containsKey(1L));
    Assert.assertEquals(name + ": 底层存储也要清", 0, TestDelegateAccess.delegateOf(c).size());
    Assert.assertNull(name + ": set(k, null) 之后读不到", c.get("a"));
    Assert.assertFalse(name + ": set(k, null) 之后也查不到", c.containsKey("a"));

    // setnx 的 value 为 null：不写入、返回 false
    Assert.assertFalse(name + ": setnx(k, null) 返回 false", c.setnx("b", null));
    Assert.assertFalse(name + ": 不该写入", c.containsKey("b"));
    Assert.assertEquals(name + ": 不该占位置", 0, c.size());
    Assert.assertNull(name + ": 不该留下计数", keyToVisitorCount.get("b"));
    Assert.assertFalse(name + ": 也不该在第二张表里建档", visitorCountToKeySet.containsKey(0L));

    // 已有值时不动它
    c.set("b", "2");
    Assert.assertFalse(name + ": key 已存在时 setnx(k, null) 返回 false", c.setnx("b", null));
    Assert.assertEquals(name + ": 不该动已有值", "2", c.get("b"));

    // 对一个从没写过的 key 置空，必须是彻底的空操作
    c.set("never-existed", null);
    Assert.assertEquals(name + ": 统计表不该被幽灵条目撑大", 1, c.size());
    Assert.assertEquals(name + ": 底层也不该被撑大", 1, TestDelegateAccess.delegateOf(c).size());
    Assert.assertEquals(name + ": b 没被挤掉", "2", c.get("b"));

    // key 为 null 一律拒绝
    CacheAssertions.assertRejected(name + ": set", () -> c.set(null, "v"));
    CacheAssertions.assertRejected(name + ": set(k, null)", () -> c.set(null, null));
    CacheAssertions.assertRejected(name + ": setnx", () -> c.setnx(null, "v"));
    CacheAssertions.assertRejected(name + ": setnx(k, null)", () -> c.setnx(null, null));
    CacheAssertions.assertRejected(name + ": get", () -> c.get(null));
    CacheAssertions.assertRejected(name + ": containsKey", () -> c.containsKey(null));
    CacheAssertions.assertRejected(name + ": delete", () -> c.delete(null));
  }

  /**
   * <b>两张统计表的观测入口是只读的</b>：外部拿到的东西改不动账本。
   * <p>
   * 这两个 getter 以前由 Lombok 生成、直接返回活对象，调用方一次 {@code set} / {@code delete}
   * 就能把账本改坏（账本与数据层分叉、活数据被提前淘汰），而且不报任何错。
   * 现在返回只读外壳：读实时转发到活账本，写立刻抛
   * {@link UnsupportedOperationException}。
   * <p>
   * {@code computeIfAbsent} 是<b>被覆盖成一律抛</b>的，不靠"默认实现的写动作最终会落到
   * 被禁掉的 set 上"：默认实现只在 key 不存在时才写入，命中时它读到一个值就直接返回 ——
   * 于是同一个方法会随 key 在不在而时灵时不灵，调用方从签名上看不出来。
   * <p>
   * 表里的集合是 {@code LinkedHashSet}（类而不是接口，包不成不可写视图），
   * 因此 {@code get(count)} 返回副本、{@code keySet()} 返回快照 —— 否则调用方一个
   * {@code clear()} 就能把整个档位清空。
   */
  @Test
  public void testStatisticObservationEntriesAreReadOnly() {
    // countWriteAsVisit=false：只有 get 计分，所以分数就是 1，档位表里只有 1 档
    LfuKVCache<String, String> cache = new LfuHashMapKVCache<>(CAPACITY, false);
    cache.set("a", "1");
    cache.get("a");

    KVCache<String, Long> counts = cache.getKeyToVisitorCount();
    Assert.assertEquals("读得到 a 的分数", Long.valueOf(1L), counts.get("a"));
    Assert.assertEquals(1, counts.size());

    CacheAssertions.assertNotSupported("计数表 set", () -> counts.set("a", 99L));
    CacheAssertions.assertNotSupported("计数表 delete", () -> counts.delete("a"));
    CacheAssertions.assertNotSupported("计数表 remove", () -> counts.remove("a"));
    // remove 已覆盖成无条件抛：不校验参数，null key 也是 UOE（与 delete 同口径）
    CacheAssertions.assertNotSupported("计数表 remove(null)", () -> counts.remove(null));
    // computeIfAbsent 被覆盖成一律抛：默认实现只在"key 不存在"时才写入，
    // 命中时会直接返回读到的值（见 ReadOnlyKVCache 的说明），同一个方法两种行为
    CacheAssertions.assertNotSupported("计数表 computeIfAbsent（命中）",
            () -> counts.computeIfAbsent("a", k -> 99L));
    CacheAssertions.assertNotSupported("计数表 computeIfAbsent（未命中）",
            () -> counts.computeIfAbsent("absent", k -> 99L));
    Assert.assertEquals("在外壳上写失败之后，活账本一分未动",
            Long.valueOf(1L), cache.getKeyToVisitorCount().get("a"));

    SortedKVCache<Long, LinkedHashSet<String>> tiers = cache.getVisitorCountToKeySet();
    Assert.assertEquals("a 在 1 档", new LinkedHashSet<>(Arrays.asList("a")), tiers.get(1L));

    CacheAssertions.assertNotSupported("档位表 set", () -> tiers.set(1L, new LinkedHashSet<>()));
    CacheAssertions.assertNotSupported("档位表 delete", () -> tiers.delete(1L));
    CacheAssertions.assertNotSupported("档位表 computeIfAbsent",
            () -> tiers.computeIfAbsent(1L, k -> new LinkedHashSet<>()));
    CacheAssertions.assertNotSupported("档位表 keySet 是快照，不能 remove",
            () -> tiers.keySet().remove(1L));

    LinkedHashSet<String> copy = tiers.get(1L);
    copy.clear();
    Assert.assertEquals("清空副本不影响活账本",
            new LinkedHashSet<>(Arrays.asList("a")), cache.getVisitorCountToKeySet().get(1L));
  }

  /**
   * 按实现造一份注入了统计表的实例，并把它和那两张表一起带出来观察。
   */
  private Injected injected(String impl) {
    KVCache<String, Long> keyToVisitorCount = new HashMapKVCache<>();
    SortedKVCache<Long, LinkedHashSet<String>> visitorCountToKeySet = new TreeMapKVCache<>();
    KVCacheWrapper<String, String> cache;
    if (HASH_MAP_IMPL.equals(impl)) {
      cache = new ExposedLfuHashMapKVCache<>(keyToVisitorCount, visitorCountToKeySet, CAPACITY);
    } else {
      cache = new ExposedLfuLruKVCache<>(keyToVisitorCount, visitorCountToKeySet, CAPACITY);
    }
    return new Injected(cache, keyToVisitorCount, visitorCountToKeySet);
  }

  private static class Injected {
    private final KVCacheWrapper<String, String> cache;
    private final KVCache<String, Long> keyToVisitorCount;
    private final SortedKVCache<Long, LinkedHashSet<String>> visitorCountToKeySet;

    private Injected(KVCacheWrapper<String, String> cache, KVCache<String, Long> keyToVisitorCount,
                     SortedKVCache<Long, LinkedHashSet<String>> visitorCountToKeySet) {
      this.cache = cache;
      this.keyToVisitorCount = keyToVisitorCount;
      this.visitorCountToKeySet = visitorCountToKeySet;
    }
  }

  /**
   * 仅供测试：暴露 {@code LfuHashMapKVCache} 注入两张统计表的 protected 构造器
   */
  private static class ExposedLfuHashMapKVCache<K, V> extends LfuKVCache<K, V> {
    private ExposedLfuHashMapKVCache(KVCache<K, Long> keyToVisitorCount,
                                     SortedKVCache<Long, LinkedHashSet<K>> visitorCountToKeySet,
                                     int capacity) {
      super(keyToVisitorCount, visitorCountToKeySet, capacity, true, new HashMapKVCache<>());
    }
  }

  /**
   * 仅供测试：暴露 {@code LfuLruKVCache} 注入两张统计表的 protected 构造器。
   * 底层 LRU 由该类自己按 capacity 构造，两层容量因此天然一致。
   */
  private static class ExposedLfuLruKVCache<K, V> extends LfuKVCache<K, V> {
    private ExposedLfuLruKVCache(KVCache<K, Long> keyToVisitorCount,
                                 SortedKVCache<Long, LinkedHashSet<K>> visitorCountToKeySet,
                                 int capacity) {
      super(keyToVisitorCount, visitorCountToKeySet, capacity, true, new LruHashMapKVCache<>(capacity));
    }
  }

}
