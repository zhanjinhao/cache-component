package cn.addenda.component.cache.test.eviction.lru;

import cn.addenda.component.cache.HashMapKVCache;
import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.eviction.lru.LruKHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LinkedHashSetLruDeque;
import cn.addenda.component.cache.eviction.lru.LruDeque;
import cn.addenda.component.cache.eviction.lru.LruKVCache;
import cn.addenda.component.cache.eviction.lru.LruKKVCache;
import cn.addenda.component.cache.test.CacheAssertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link LruKVCache} 的行为测试。
 * <p>
 * 同一组断言跑两个实现：
 * <ul>
 *   <li>{@link LruKVCache}：基础 LRU，写进来即入队；</li>
 *   <li>{@link LruKKVCache}（{@code threshold = 1}）：LRU-K 的准入闸门设为 1，
 *       第一次写即达到门槛，每次写都能落地，淘汰顺序与 {@link LruKVCache} 完全一致 ——
 *       即"LRU-K 在阈值为 1 时退化为 LRU"。</li>
 * </ul>
 * 两者的差别只有 {@code setnx}：{@link LruKKVCache} 明确不支持（抛
 * {@link UnsupportedOperationException}），因此涉及 {@code setnx} 的断言只跑
 * {@link LruKVCache}，其不支持的行为由
 * {@code EvictableKVCacheWrapperContractTest#testSetnxIsUnsupportedOnLruKKVCache} 单独钉。
 * <p>
 * 契约层面的断言（null 语义、null key）在 {@code EvictableKVCacheWrapperContractTest} 里，
 * 这里只测这个实现自己特有的东西：淘汰顺序、命中后变最新、队列和值的同步。
 *
 * @author addenda
 */
public class LruKVCacheTest {

  private static final int CAPACITY = 3;

  /**
   * 全部实现。key 为断言消息里的名字，失败时能直接看出是哪个实现。
   */
  private final Map<String, KVCache<String, String>> caches = new LinkedHashMap<>();

  /**
   * {@code setnx} 语义完整的那些。{@link LruKKVCache} 不在这里，见类注释。
   */
  private final Map<String, KVCache<String, String>> setnxCaches = new LinkedHashMap<>();

  @Before
  public void before() {
    caches.clear();
    setnxCaches.clear();
    register("LruHashMapKVCache", new LruHashMapKVCache<>(CAPACITY), true);
    register("LruKHashMapKVCache(threshold=1)", new LruKHashMapKVCache<>(CAPACITY, 1), false);
  }

  private void register(String name, KVCache<String, String> cache, boolean setnx) {
    caches.put(name, cache);
    if (setnx) {
      setnxCaches.put(name, cache);
    }
  }

  @Test
  public void testSetGetDelete() {
    for (String name : caches.keySet()) {
      KVCache<String, String> cache = caches.get(name);

      cache.set("a", "1");
      Assert.assertEquals(name + ": 写进去应该能读出来", "1", cache.get("a"));
      Assert.assertTrue(name + ": 写进去应该能查到", cache.containsKey("a"));
      Assert.assertEquals(name + ": 写一条 size 是 1", 1, cache.size());

      Assert.assertTrue(name + ": 删掉应该返回 true", cache.delete("a"));
      Assert.assertNull(name + ": 删掉之后再读应该是 null", cache.get("a"));
      Assert.assertFalse(name + ": 删掉之后再查应该是 false", cache.containsKey("a"));
      Assert.assertEquals(name + ": 删掉之后队列也要跟着减", 0, cache.size());
    }
  }

  /**
   * 满了淘汰队头 —— 最早写进来的那个。
   */
  @Test
  public void testEvictsOldestWhenFull() {
    for (String name : caches.keySet()) {
      KVCache<String, String> cache = caches.get(name);

      cache.set("a", "1");
      cache.set("b", "2");
      cache.set("c", "3");
      Assert.assertEquals(name + ": 写满容量", CAPACITY, cache.size());

      cache.set("d", "4");
      Assert.assertEquals(name + ": 容量不变", CAPACITY, cache.size());
      Assert.assertNull(name + ": 最早写进来的 a 应该被淘汰", cache.get("a"));
      Assert.assertEquals(name + ": b 还在", "2", cache.get("b"));
      Assert.assertEquals(name + ": c 还在", "3", cache.get("c"));
      Assert.assertEquals(name + ": d 写进来了", "4", cache.get("d"));
    }
  }

  /**
   * 命中会把 key 挪到队尾，下一个被淘汰的就轮不到它。
   * <p>
   * 这条是最容易写错的：{@code LinkedHashSetLruDeque.addLast} 对已存在的元素
   * 不挪位置，少写一个 remove，a 就会永远卡在队头。
   */
  @Test
  public void testGetMakesKeyMostRecent() {
    for (String name : caches.keySet()) {
      KVCache<String, String> cache = caches.get(name);

      cache.set("a", "1");
      cache.set("b", "2");
      cache.set("c", "3");

      Assert.assertEquals(name + ": 读得到", "1", cache.get("a"));

      cache.set("d", "4");
      Assert.assertEquals(name + ": 刚读过的 a 不该被淘汰", "1", cache.get("a"));
      Assert.assertNull(name + ": 该被淘汰的是 b", cache.get("b"));
    }
  }

  /**
   * 覆盖写已有 key 也算一次访问，同样要挪到队尾。
   */
  @Test
  public void testSetOnExistingKeyMakesItMostRecent() {
    for (String name : caches.keySet()) {
      KVCache<String, String> cache = caches.get(name);

      cache.set("a", "1");
      cache.set("b", "2");
      cache.set("c", "3");

      cache.set("a", "1-new");

      cache.set("d", "4");
      Assert.assertEquals(name + ": 刚写过的 a 不该被淘汰", "1-new", cache.get("a"));
      Assert.assertNull(name + ": 该被淘汰的是 b", cache.get("b"));
    }
  }

  /**
   * 覆盖写不能把 size 撑大 —— 队列里同一个 key 只能有一份。
   */
  @Test
  public void testOverwriteDoesNotGrowSize() {
    for (String name : caches.keySet()) {
      KVCache<String, String> cache = caches.get(name);

      cache.set("a", "1");
      cache.set("b", "2");
      cache.set("c", "3");

      cache.set("a", "1-new");
      cache.set("b", "2-new");

      Assert.assertEquals(name + ": 覆盖写之后还是 3 条", CAPACITY, cache.size());
      Assert.assertEquals(name + ": a 是新值", "1-new", cache.get("a"));
      Assert.assertEquals(name + ": b 是新值", "2-new", cache.get("b"));
    }
  }

  /**
   * 删掉一条就腾出一格，下一次写不会触发淘汰。
   */
  @Test
  public void testDeleteFreesCapacity() {
    for (String name : caches.keySet()) {
      KVCache<String, String> cache = caches.get(name);

      cache.set("a", "1");
      cache.set("b", "2");
      cache.set("c", "3");

      Assert.assertTrue(name + ": 删掉 b", cache.delete("b"));
      Assert.assertEquals(name + ": 删掉之后队列也要跟着减", 2, cache.size());

      cache.set("d", "4");
      Assert.assertEquals(name + ": 又回到满容量", CAPACITY, cache.size());
      Assert.assertEquals(name + ": 之前删掉 b 腾出的位置给了 d，a 不该被淘汰", "1", cache.get("a"));
      Assert.assertEquals(name + ": c 不该被淘汰", "3", cache.get("c"));
      Assert.assertEquals(name + ": d 在", "4", cache.get("d"));
    }
  }

  /**
   * 删掉之后再写回来，要重新排到队尾。
   */
  @Test
  public void testDeleteThenSetAgain() {
    for (String name : caches.keySet()) {
      KVCache<String, String> cache = caches.get(name);

      cache.set("a", "1");
      cache.set("b", "2");
      cache.set("c", "3");

      cache.delete("a");
      cache.set("a", "1-again");

      cache.set("d", "4");
      Assert.assertEquals(name + ": 重新写进来的 a 是最新的，不该被淘汰", "1-again", cache.get("a"));
      Assert.assertNull(name + ": 该被淘汰的是 b", cache.get("b"));
    }
  }

  /**
   * 可以换一个 {@link LruDeque} 实现 —— 队列长度对得上就说明用的确实是传进去那个。
   * <p>
   * 只跑 {@link LruKVCache}：{@link LruKKVCache} 的主队列也是 {@code LruKVCache} 的，
   * 但它另有 {@code visitorCount} 结构，队列不是它唯一的账本，断言口径不同。
   * <p>
   * {@code LruKVCache} 注入队列的构造器是 {@code protected}（队列必须与底层 cache 严格同步，
   * 这个前提无法在构造时校验，因此不对外公开），这里用一个临时实现把它暴露出来。
   */
  @Test
  public void testCustomLruDeque() {
    LruDeque<String> myDeque = new LinkedHashSetLruDeque<>();
    LruKVCache<String, String> custom = new LruOverDeque<>(2, myDeque);

    custom.set("a", "1");
    custom.set("b", "2");

    Assert.assertEquals("队列被外部对象观察得到", 2, myDeque.size());
    Assert.assertEquals("队头是最早写进来的", "a", myDeque.getFirst());

    custom.get("a");
    Assert.assertEquals("命中后 a 排到队尾", "b", myDeque.getFirst());
  }

  /**
   * set / setnx 对 null 的处理，见 {@code KVCache} 上的契约：
   * key 一律拒绝，value 为 null 一律表示"没有值"。
   * <p>
   * LRU 这边的关键是<b>队列也要跟着清</b>：只把 null 透传给底层的话，
   * {@code lruDeque} 里会留下一个没有值的条目。这个条目 {@code size()} 看不见
   * （它取自数据层），但在淘汰触发条件 {@code capacity() == lruDeque.size()} 眼里
   * 就是一条真实数据 —— 队列提前填满，活着的 key 被踢掉。
   */
  @Test
  public void testSetNullMeansDelete() {
    for (String name : caches.keySet()) {
      KVCache<String, String> cache = caches.get(name);

      // value 为 null 等于删除，值和队列都要清。
      // 下面这条 size() 断言只能证明"值删掉了"：size() 取自数据层，
      // 队列里留着幽灵条目它也照样报 0。队列是否清干净由
      // EvictableKVCacheWrapperConsistencyTest#testSetNullClearsTheLedger 直接读账本来钉。
      cache.set("a", "1");
      Assert.assertEquals(name + ": 先写进去", 1, cache.size());

      cache.set("a", null);
      Assert.assertEquals(name + ": set(k, null) 之后值没了", 0, cache.size());
      Assert.assertNull(name + ": set(k, null) 之后读不到", cache.get("a"));
      Assert.assertFalse(name + ": set(k, null) 之后也查不到", cache.containsKey("a"));
    }
  }

  /**
   * 对一个从没写过的 key 置空，必须是彻底的空操作。
   * <p>
   * 队列里塞进幽灵条目的话，容量会被撑满，活着的 key 会被挤掉换成它。
   */
  @Test
  public void testSetNullOnUnknownKeyDoesNotEvict() {
    for (String name : caches.keySet()) {
      KVCache<String, String> cache = caches.get(name);

      cache.set("b", "2");
      cache.set("c", "3");
      cache.set("d", "4");
      Assert.assertEquals(name + ": 满了", CAPACITY, cache.size());

      cache.set("never-existed", null);

      Assert.assertEquals(name + ": 幽灵条目不该占格子", CAPACITY, cache.size());
      Assert.assertEquals(name + ": b 不该被这个空操作挤掉", "2", cache.get("b"));
      Assert.assertEquals(name + ": c 不该被这个空操作挤掉", "3", cache.get("c"));
      Assert.assertEquals(name + ": d 不该被这个空操作挤掉", "4", cache.get("d"));
      Assert.assertFalse(name + ": 幽灵 key 本身也不该存在", cache.containsKey("never-existed"));
    }
  }

  /**
   * setnx 的 value 为 null：不写入、返回 false，且不动已有值、不占队列位置。
   * <p>
   * 只跑 {@link LruKVCache} —— {@link LruKKVCache} 的 {@code setnx} 整个都不支持。
   */
  @Test
  public void testSetnxNullValueMeansNoValue() {
    for (String name : setnxCaches.keySet()) {
      KVCache<String, String> cache = setnxCaches.get(name);

      Assert.assertFalse(name + ": key 不存在时 setnx(k, null) 返回 false", cache.setnx("b", null));
      Assert.assertFalse(name + ": 不该写入", cache.containsKey("b"));
      Assert.assertEquals(name + ": 也不该占用队列位置", 0, cache.size());

      cache.set("b", "2");
      Assert.assertFalse(name + ": key 已存在时 setnx(k, null) 返回 false", cache.setnx("b", null));
      Assert.assertEquals(name + ": 不该动已有值", "2", cache.get("b"));
    }
  }

  /**
   * key 为 null 一律拒绝。
   * <p>
   * 但拒绝的类型分两种：{@link LruKKVCache} 的 {@code setnx} 是<b>无条件拒绝</b>，
   * 不校验参数（见 {@code KVCache} 的异常口径第 1 条），null key 也抛 UOE；
   * 其余方法（以及支持 setnx 的实现）入口先校验 key，抛 IAE。
   */
  @Test
  public void testNullKeyRejected() {
    for (String name : caches.keySet()) {
      KVCache<String, String> cache = caches.get(name);

      CacheAssertions.assertRejected(name + ": set", () -> cache.set(null, "v"));
      CacheAssertions.assertRejected(name + ": set(k, null)", () -> cache.set(null, null));
      if (setnxCaches.containsKey(name)) {
        CacheAssertions.assertRejected(name + ": setnx", () -> cache.setnx(null, "v"));
        CacheAssertions.assertRejected(name + ": setnx(k, null)", () -> cache.setnx(null, null));
      } else {
        // 无条件拒绝：UOE 优先于参数校验
        CacheAssertions.assertNotSupported(name + ": setnx", () -> cache.setnx(null, "v"));
        CacheAssertions.assertNotSupported(name + ": setnx(k, null)", () -> cache.setnx(null, null));
      }
      CacheAssertions.assertRejected(name + ": get", () -> cache.get(null));
      CacheAssertions.assertRejected(name + ": containsKey", () -> cache.containsKey(null));
      CacheAssertions.assertRejected(name + ": delete", () -> cache.delete(null));
    }
  }

  /**
   * 仅供测试：暴露 {@code LruKVCache} 注入队列的 protected 构造器。
   * <p>
   * 存储由这个临时实现自己指定（{@link HashMapKVCache}）—— 具体类
   * {@code LruHashMapKVCache} 只接受容量，要换队列得自己继承抽象基类。
   */
  private static class LruOverDeque<K, V> extends LruKVCache<K, V> {
    private LruOverDeque(int capacity, LruDeque<K> lruDeque) {
      super(capacity, new HashMapKVCache<>(), lruDeque);
    }
  }

}
