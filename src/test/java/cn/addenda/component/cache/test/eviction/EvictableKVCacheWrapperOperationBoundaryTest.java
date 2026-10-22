package cn.addenda.component.cache.test.eviction;

import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.KVCacheWrapper;
import cn.addenda.component.cache.TestDelegateAccess;
import cn.addenda.component.cache.eviction.lfu.LfuHashMapKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuLruKVCache;
import cn.addenda.component.cache.eviction.lru.LruKHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruKKVCache;
import cn.addenda.component.cache.test.CacheAssertions;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@code EvictableKVCache} 的<b>操作边界</b>测试。
 * <p>
 * 前两组分别盯容量和底层存储，这一组盯的是<b>各个操作和淘汰策略的交互</b>：
 * {@code computeIfAbsent} / {@code remove} 这些接口上的默认实现，
 * 落在淘汰实现上会不会出现"多登记一次"、"少清一次"、"白占一格"之类的偏差。
 * <p>
 * 共同的不变量就一条：<b>不管走哪个入口，{@code size()} 都要等于底层真实条数</b>。
 * <p>
 * 这条不变量的强度要说清楚：{@code size()} 取自数据层，与"底层条数"是<b>同一来源</b>，
 * 断言因此恒等。它能挡住的是"某个入口把值写歪了、写丢了"，<b>挡不住账本残留</b>
 * ——残留只影响淘汰触发条件，不影响 {@code size()}。账本那一侧由
 * {@code EvictableKVCacheWrapperConsistencyTest} 直接读队列 / 计数表来钉。
 *
 * @author addenda
 */
@Slf4j
public class EvictableKVCacheWrapperOperationBoundaryTest {

  private static final int CAPACITY = 3;

  private List<KVCache<String, String>> newCaches() {
    List<KVCache<String, String>> caches = new ArrayList<>();
    caches.add(new LruHashMapKVCache<>(CAPACITY));
    caches.add(new LfuHashMapKVCache<>(CAPACITY));
    caches.add(new LfuLruKVCache<>(CAPACITY));
    caches.add(new LruKHashMapKVCache<>(CAPACITY, 1));
    return caches;
  }

  private String name(KVCache<String, String> cache) {
    return cache.getClass().getSimpleName();
  }

  private long delegateSize(KVCache<String, String> cache) {
    return TestDelegateAccess.delegateOf((KVCacheWrapper<String, String>) cache).size();
  }

  /**
   * 去掉 {@link LruKKVCache} —— 它明确不支持 {@code setnx}
   * （抛 {@code CacheUnsupportedOperationException}），
   * 不能混在"setnx 通用契约"里跑。理由见它自己的方法说明：
   * LRU-K 的写入要过准入闸门，而 {@code setnx} 承诺"不存在就写、写了就有"。
   */
  private List<KVCache<String, String>> newSetnxCapableCaches() {
    List<KVCache<String, String>> caches = newCaches();
    caches.removeIf(cache -> cache instanceof LruKKVCache);
    return caches;
  }

  /**
   * 同理去掉 {@link LruKKVCache} —— 它也不支持 {@code computeIfAbsent}
   * （它的写动作同样要过准入闸门，见该类的方法说明）。
   */
  private List<KVCache<String, String>> newComputeIfAbsentCapableCaches() {
    List<KVCache<String, String>> caches = newCaches();
    caches.removeIf(cache -> cache instanceof LruKKVCache);
    return caches;
  }

  /**
   * 单独钉住 {@link LruKKVCache} 的这条：{@code setnx} 抛
   * {@code CacheUnsupportedOperationException}，而不是返回一个需要猜的 false。
   * <p>
   * 具体行为（各门槛、key 在不在）在 {@code LruKKVCacheTest} 里。
   */
  @Test
  public void testSetnxIsUnsupportedOnLruKKVCache() {
    CacheAssertions.assertNotSupported("LruKKVCache: setnx",
            () -> new LruKHashMapKVCache<String, String>(CAPACITY, 1).setnx("a", "v"));
  }

  /**
   * 同理钉住 {@code computeIfAbsent}：抛 {@code CacheUnsupportedOperationException}，
   * 而不是返回一个"可能根本没进缓存"的值。
   * <p>
   * 这里用的是 {@code threshold = 1}（值确实会落地）也照样抛 —— 与 {@code setnx} 同一个理由：
   * 不应让同一方法的行为随一个调优参数在"可用"与"抛异常"之间切换。
   * 具体用例在 {@code LruKKVCacheTest#testComputeIfAbsentIsNotSupported}。
   */
  @Test
  public void testComputeIfAbsentIsUnsupportedOnLruKKVCache() {
    CacheAssertions.assertNotSupported("LruKKVCache: computeIfAbsent",
            () -> new LruKHashMapKVCache<String, String>(CAPACITY, 1).computeIfAbsent("a", k -> "v"));
  }

  // ------------------------------------------------------------------
  // 不占容量的操作
  // ------------------------------------------------------------------

  /**
   * <b>读未命中、查未命中，都不该占用容量。</b>
   * <p>
   * 如果 {@code get} 一条不在缓存里的 key 也会在统计里挂一笔，
   * 那么"缓存没命中时的大量查询"就会把容量悄悄吃光 ——
   * 报表场景里最容易踩到的就是这一条。
   */
  @Test
  public void testMissesDoNotConsumeCapacity() {
    for (KVCache<String, String> cache : newCaches()) {
      String name = name(cache);

      for (int i = 0; i < 100; i++) {
        Assert.assertNull(cache.get("miss" + i));
        Assert.assertFalse(cache.containsKey("miss" + i));
      }

      Assert.assertEquals(name + ": 查 200 次未命中之后还是空的", 0, cache.size());
      Assert.assertEquals(name + ": 底层也不该有东西", 0L, delegateSize(cache));

      // 容量一点没被吃掉：写满三条不该触发淘汰
      cache.set("a", "1");
      cache.set("b", "2");
      cache.set("c", "3");
      Assert.assertTrue(name + ": a 不该被淘汰", cache.containsKey("a"));
      Assert.assertEquals(CAPACITY, cache.size());
    }
  }

  /**
   * <b>{@code set(k, null)} 对不存在的 key 必须是彻底的空操作</b>：
   * 不写值、不占格、不淘汰别人。
   */
  @Test
  public void testSetNullOnMissingKeyIsCompletelyNoOp() {
    for (KVCache<String, String> cache : newCaches()) {
      String name = name(cache);

      cache.set("a", "1");
      cache.set("b", "2");
      cache.set("c", "3");

      cache.set("never-existed", null);

      Assert.assertEquals(name + ": 不该多出一格", CAPACITY, cache.size());
      Assert.assertEquals(name + ": 底层条数也不变", CAPACITY, delegateSize(cache));
      Assert.assertTrue(name + ": a 不该被挤掉", cache.containsKey("a"));
      Assert.assertTrue(name + ": b 不该被挤掉", cache.containsKey("b"));
      Assert.assertTrue(name + ": c 不该被挤掉", cache.containsKey("c"));
    }
  }

  /**
   * {@code setnx} 的 value 为 null 时同样什么都不做。
   * <p>
   * 本用例只看得见数据层：{@code size()} 与底层条数同源，"账本没被碰"这一半看不见 ——
   * 那一半由 {@code EvictableKVCacheWrapperConsistencyTest#testSetnxNullDoesNotTouchTheLedger}
   * 直接读队列 / 计数表来钉。
   * <p>
   * {@link LruKKVCache} 不在这组里 —— 它整个 {@code setnx} 都不支持，
   * 抛 {@code CacheUnsupportedOperationException}，
   * 所以"null 时返回 false"那条契约在它身上不适用。见
   * {@link #testSetnxIsUnsupportedOnLruKKVCache()}。
   */
  @Test
  public void testSetnxNullValueIsCompletelyNoOp() {
    for (KVCache<String, String> cache : newSetnxCapableCaches()) {
      String name = name(cache);

      Assert.assertFalse(name + ": 应该返回 false", cache.setnx("a", null));
      Assert.assertEquals(name + ": 不该占格", 0, cache.size());
      Assert.assertEquals(name + ": 底层也不该有", 0L, delegateSize(cache));
    }
  }

  // ------------------------------------------------------------------
  // computeIfAbsent
  // ------------------------------------------------------------------

  /**
   * 未命中时算一次、写进去；再查命中，不重复计算。
   */
  @Test
  public void testComputeIfAbsentPopulatesThenHits() {
    for (KVCache<String, String> cache : newComputeIfAbsentCapableCaches()) {
      String name = name(cache);
      AtomicInteger invoked = new AtomicInteger();

      Assert.assertEquals(name + ": 第一次应该计算",
              "v1", cache.computeIfAbsent("a", k -> "v" + invoked.incrementAndGet()));
      Assert.assertEquals(name + ": 第二次该命中缓存",
              "v1", cache.computeIfAbsent("a", k -> "v" + invoked.incrementAndGet()));

      Assert.assertEquals(name + ": mappingFunction 只该调一次", 1, invoked.get());
      Assert.assertEquals(name + ": 只登记一条", 1, cache.size());
      Assert.assertEquals(name + ": size() 与底层条数一致", delegateSize(cache), cache.size());
    }
  }

  /**
   * 计算结果为 null 时不该写入，也不该占格。
   */
  @Test
  public void testComputeIfAbsentNullResultWritesNothing() {
    for (KVCache<String, String> cache : newComputeIfAbsentCapableCaches()) {
      String name = name(cache);

      Assert.assertNull(name + ": 应该返回 null", cache.computeIfAbsent("a", k -> null));

      Assert.assertEquals(name + ": 不该占格", 0, cache.size());
      Assert.assertEquals(name + ": 底层也不该有", 0L, delegateSize(cache));
    }
  }

  /**
   * 缓存已满时 {@code computeIfAbsent} 要照常淘汰 —— 它内部走的就是 {@code set}。
   */
  @Test
  public void testComputeIfAbsentEvictsWhenFull() {
    for (KVCache<String, String> cache : newComputeIfAbsentCapableCaches()) {
      String name = name(cache);

      cache.set("a", "1");
      cache.set("b", "2");
      cache.set("c", "3");
      cache.computeIfAbsent("d", k -> "4");

      Assert.assertEquals(name + ": 不该超过容量", CAPACITY, cache.size());
      Assert.assertEquals(name + ": size() 与底层条数一致", delegateSize(cache), cache.size());
      Assert.assertEquals(name + ": d 写进去了", "4", cache.get("d"));
    }
  }

  // ------------------------------------------------------------------
  // remove
  // ------------------------------------------------------------------

  /**
   * {@code remove} = get + delete：返回旧值，并且把位置腾出来。
   */
  @Test
  public void testRemoveReturnsValueAndFreesSlot() {
    for (KVCache<String, String> cache : newCaches()) {
      String name = name(cache);

      cache.set("a", "1");
      cache.set("b", "2");
      cache.set("c", "3");

      Assert.assertEquals(name + ": remove 返回被删掉的值", "1", cache.remove("a"));
      Assert.assertEquals(name + ": 少了一条", 2, cache.size());
      Assert.assertEquals(name + ": size() 与底层条数一致", delegateSize(cache), cache.size());
      Assert.assertFalse(cache.containsKey("a"));

      // 腾出来的格子要能用
      cache.set("d", "4");
      Assert.assertEquals(name + ": b 不该被淘汰", "2", cache.get("b"));
      Assert.assertEquals(name + ": c 不该被淘汰", "3", cache.get("c"));
      Assert.assertEquals("4", cache.get("d"));
    }
  }

  /**
   * {@code remove} 一个不存在的 key：返回 null，且不改变任何状态。
   */
  @Test
  public void testRemoveMissingKeyChangesNothing() {
    for (KVCache<String, String> cache : newCaches()) {
      String name = name(cache);

      cache.set("a", "1");
      long before = cache.size();

      Assert.assertNull(name + ": 不存在的 key 返回 null", cache.remove("never-existed"));

      Assert.assertEquals(name + ": size 不变", before, cache.size());
      Assert.assertEquals(name + ": size() 与底层条数一致", delegateSize(cache), cache.size());
      Assert.assertEquals(name + ": 已有的值不受影响", "1", cache.get("a"));
    }
  }

  // ------------------------------------------------------------------
  // 淘汰之后要"忘干净"
  // ------------------------------------------------------------------

  /**
   * <b>被淘汰的 key 要像从来没出现过</b>：
   * 它的统计必须清干净，否则 {@code setnx} 会以为它还在、返回 false。
   */
  @Test
  public void testEvictedKeyCanBeWrittenAgainViaSetnx() {
    for (KVCache<String, String> cache : newSetnxCapableCaches()) {
      String name = name(cache);

      cache.set("a", "1");
      cache.set("b", "2");
      cache.set("c", "3");
      // 再写一条，a 被淘汰
      cache.set("d", "4");
      Assert.assertFalse(name + ": 前置条件：a 已经不在", cache.containsKey("a"));

      Assert.assertTrue(name + ": 被淘汰的 key 应该能重新 setnx 进去",
              cache.setnx("a", "1-again"));
      Assert.assertEquals("1-again", cache.get("a"));
      Assert.assertEquals(name + ": 容量仍然是硬约束", CAPACITY, cache.size());
      Assert.assertEquals(name + ": size() 与底层条数一致", delegateSize(cache), cache.size());
    }
  }

  /**
   * 删掉再写回来，不该在统计里留下重复登记。
   */
  @Test
  public void testDeleteThenSetDoesNotDoubleRegister() {
    for (KVCache<String, String> cache : newCaches()) {
      String name = name(cache);

      for (int i = 0; i < 20; i++) {
        cache.set("a", "v" + i);
        cache.delete("a");
      }
      cache.set("a", "final");

      Assert.assertEquals(name + ": 只该有一条", 1, cache.size());
      Assert.assertEquals(name + ": size() 与底层条数一致", delegateSize(cache), cache.size());
      Assert.assertEquals("final", cache.get("a"));
    }
  }

  /**
   * {@code setnx} 成功之后，那条记录要真的参与淘汰 ——
   * 不能像以前那样绕开统计、导致缓存插多少活多少。
   */
  @Test
  public void testSetnxWrittenKeysParticipateInEviction() {
    for (KVCache<String, String> cache : newSetnxCapableCaches()) {
      String name = name(cache);

      for (int i = 0; i < 50; i++) {
        cache.setnx("k" + i, "v" + i);
      }

      Assert.assertTrue(name + ": setnx 进来的也要计入 size", cache.size() > 0);
      Assert.assertTrue(name + ": 不能超过容量，实际 " + cache.size(),
              cache.size() <= cache.capacity());
      Assert.assertEquals(name + ": size() 与底层条数一致", delegateSize(cache), cache.size());
    }
  }

}
