package cn.addenda.component.cache.test.eviction;

import cn.addenda.component.base.util.SleepUtils;
import cn.addenda.component.cache.CacheIllegalArgumentException;
import cn.addenda.component.cache.CacheUnsupportedOperationException;
import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.KVCacheWrapper;
import cn.addenda.component.cache.SynchronizedKVCache;
import cn.addenda.component.cache.eviction.EvictableKVCache;
import cn.addenda.component.cache.eviction.EvictableKVCacheWrapper;
import cn.addenda.component.cache.eviction.lfu.LfuAgingHashMapKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuAgingLruKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuHashMapKVCache;
import cn.addenda.component.cache.eviction.lfu.LfuLruKVCache;
import cn.addenda.component.cache.eviction.lru.LruGenerationHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruKHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruGenerationKVCache;
import cn.addenda.component.cache.eviction.lru.LruKVCache;
import cn.addenda.component.cache.eviction.lru.LruKKVCache;
import cn.addenda.component.cache.test.CacheAssertions;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * {@link EvictableKVCache} 这一族的契约测试。
 * <p>
 * 单独写一组的原因：{@code KVCacheContractTest} / {@code ExpiredKVCacheContractTest} 覆盖的
 * 实现都是"一层"的（存哪儿就是哪儿），而这里每个实现除了存值，还要在{@code
 * lruDeque} / {@code visitorCount} 这类统计结构里登记。于是同一条契约在这里会多出一个
 * 影子问题：<b>值删了、统计没删</b>，或者<b>值写了、统计没写</b>。
 * 两种都只有 {@code size()} / 淘汰行为能看出来，单看 {@code get} 是正常的。
 * <p>
 * 之前没被发现是因为 {@code LRUTest}/{@code LFUTest} 用的是 LeetCode 式的包装类，
 * 只调 {@code put}/{@code get}，从不碰 {@code set(k, null)} 和 {@code setnx}。
 * <p>
 * <b>关于 {@link LruGenerationKVCache}：它只参与本类里的 null 相关断言，以及
 * {@link #testComputeIfAbsentHitIsEquivalentToOneGet()}（命中相当于一次读，与代无关）。</b>
 * 它不进淘汰顺序那组、也不进 setnx 那组：那两个组的口径是给单代实现准备的 ——
 * 本类是"两代 + 中途插入"，淘汰顺序是另一套，两代容量也不能用单个 {@code capacity()} 衡量，
 * 另有 {@code LruGenerationKVCacheTest} 覆盖。它的 setnx 语义本身是完整的
 * （新 key 落老年代、{@code containsKey} 两代都看）。
 *
 * @author addenda
 */
@Slf4j
public class EvictableKVCacheWrapperContractTest {

  private static final int CAPACITY = 3;

  /**
   * 并发用例里所有线程共同写的 key 个数：
   * 必须大于上面那个容量（4 &gt; 3），每轮才会真的触发淘汰、把队列搅起来。
   */
  private static final int KEY_COUNT = 4;

  /**
   * 全部实现，用于 null 相关的断言
   */
  private final List<KVCache<String, String>> caches = new ArrayList<>();

  /**
   * {@code set} 会无条件写进底层存储的那些，用于淘汰行为的断言。
   * {@link LruGenerationKVCache} 不在这里，见类注释。
   */
  private final List<KVCache<String, String>> plainEvictableCaches = new ArrayList<>();

  /**
   * {@code setnx} 语义完整的那些，用于 setnx 的通用契约断言。
   * <p>
   * {@link LruKKVCache} 不在这里 —— 它明确不支持 {@code setnx}（抛
   * {@link CacheUnsupportedOperationException}）：
   * LRU-K 的写入要过准入闸门，和"不存在就写、写了就有"对不上。
   * 它的行为在 {@link #testSetnxIsUnsupportedOnLruKKVCache()} 里单独钉。
   */
  private final List<KVCache<String, String>> setnxCaches = new ArrayList<>();

  /**
   * @param refusesUnsupportedOps 它的 {@code setnx} / {@code computeIfAbsent} 是否是
   *                              <b>无条件拒绝</b>。是的话这两个方法不校验参数，null key
   *                              也抛 {@link CacheUnsupportedOperationException}
   *                              （见 {@link KVCache} 的异常口径第 1 条）
   */
  private void register(String name, KVCache<String, String> cache, boolean plain, boolean setnx,
                        boolean refusesUnsupportedOps) {
    caches.add(new Named(name, cache, refusesUnsupportedOps));
    if (plain) {
      plainEvictableCaches.add(new Named(name, cache));
    }
    if (setnx) {
      setnxCaches.add(new Named(name, cache));
    }
  }

  private void before() {
    register("LruKVCache",
            new LruHashMapKVCache<>(CAPACITY), true, true, false);
    register("LfuHashMapKVCache",
            new LfuHashMapKVCache<>(CAPACITY), true, true, false);
    register("LfuLruKVCache",
            new LfuLruKVCache<>(CAPACITY), true, true, false);
    // setnx / computeIfAbsent 无条件拒绝：CacheUnsupportedOperationException 优先于参数校验
    register("LruKKVCache",
            new LruKHashMapKVCache<>(CAPACITY, 1), true, false, true);
    // 本实现只跑 null 相关断言（见下面两个 false），threshold 取多少都不影响它们。
    // 注意 0 不是"关闭提升"：它表示"任何一次读取都会提升（含同一毫秒）"；
    // 要关闭提升得给一个极大的值，见 LruGenerationKVCache#threshold。
    // 两个开关都给 false：它只跑 null 相关断言。淘汰顺序那两组是单代实现的口径
    // （本类是"两代 + 中途插入"），纳入只会得到一堆不适用的断言 ——
    // plain 那组因此根本不会遍历到它，顺序覆盖在 LruGenerationKVCacheTest 里。
    register("LruGenerationKVCache",
            new LruGenerationHashMapKVCache<>(CAPACITY, CAPACITY, 0), false, false, false);
  }

  /**
   * <b>set(k, null) 一律当删除</b> —— 值删掉之外，淘汰策略的统计里也不能再留着它。
   * <p>
   * 只删值不清统计的话，统计里会留下一个没有值的 key。{@code size()} 看不出这件事
   * （它取自数据层），但淘汰触发条件比的正是账本条数，会被这个幽灵条目撑满。
   * <p>
   * 因此本用例的 {@code size()} 断言只覆盖"值删掉了"这一半；账本那一半由
   * {@code EvictableKVCacheWrapperConsistencyTest#testSetNullClearsTheLedger} 直接读账本来钉。
   */
  @Test
  public void testSetNullMeansDelete() {
    before();
    for (KVCache<String, String> cache : caches) {
      String name = name(cache);
      String k = "k";

      cache.set(k, "v");
      Assert.assertEquals(name + ": 先写进去", "v", cache.get(k));

      cache.set(k, null);
      Assert.assertNull(name + ": set(k, null) 之后 get 应该是 null", cache.get(k));
      Assert.assertFalse(name + ": set(k, null) 等价于删除", cache.containsKey(k));
      Assert.assertEquals(name + ": 值没了，size() 跟着归零", 0, cache.size());
    }
  }

  /**
   * <b>对一个不在缓存里的 key 调 set(k, null)，必须是彻底的空操作</b>。
   * <p>
   * 这是上面那条契约的最狠的一种破法：统计结构里塞进一个没有值的"幽灵条目"，
   * 把容量撑满 —— 为了给这个不存在的 key 腾位置，会当场挤掉一条<b>活着</b>的记录。
   * 调用方只是想把一个本来就没有的东西置空，结果丢了一条真数据。
   */
  @Test
  public void testSetNullOnUnknownKeyDoesNotEvict() {
    before();
    for (KVCache<String, String> cache : caches) {
      String name = name(cache);
      // 先写满容量。按 b、c、d 的顺序读一遍，把 LRU 顺序摆回 [b, c, d]，
      // 这样下面要淘汰的话，第一个被盯上的一定是 b。
      cache.set("b", "2");
      cache.set("c", "3");
      cache.set("d", "4");
      Assert.assertEquals(name + ": 前置条件，b 应该在", "2", cache.get("b"));
      Assert.assertEquals(name + ": 前置条件，c 应该在", "3", cache.get("c"));
      Assert.assertEquals(name + ": 前置条件，d 应该在", "4", cache.get("d"));

      // "never-existed" 从来就没写进去过，置空它应该什么都不发生。
      // 统计结构里塞进这个幽灵条目的话，容量会被撑满，b 会被挤掉换成它。
      cache.set("never-existed", null);

      Assert.assertEquals(name + ": b 不该被这个空操作挤掉", "2", cache.get("b"));
      Assert.assertEquals(name + ": c 不该被这个空操作挤掉", "3", cache.get("c"));
      Assert.assertEquals(name + ": d 不该被这个空操作挤掉", "4", cache.get("d"));
      Assert.assertFalse(name + ": 幽灵 key 不该被写进去", cache.containsKey("never-existed"));
    }
  }

  /**
   * setnx 的 value 为 null 时和 set 一致：表示"没有值"，不写入、返回 false，且不动已有值。
   * <p>
   * {@link LruKKVCache} 不在这组里 —— 它的 {@code setnx} 整个都不支持，抛
   * {@link CacheUnsupportedOperationException}，
   * 所以"null 时返回 false"这条契约在它身上不适用。
   */
  @Test
  public void testSetnxNullValueMeansNoValue() {
    before();
    for (KVCache<String, String> cache : setnxCaches) {
      String name = name(cache);
      String k = "k";

      Assert.assertFalse(name + ": key 不存在时 setnx(k, null) 应该返回 false", cache.setnx(k, null));
      Assert.assertFalse(name + ": setnx(k, null) 不该写入", cache.containsKey(k));
      Assert.assertEquals(name + ": setnx(k, null) 不该占用容量", 0, cache.size());

      cache.set(k, "v");
      Assert.assertFalse(name + ": key 已存在时 setnx(k, null) 应该返回 false", cache.setnx(k, null));
      Assert.assertEquals(name + ": setnx(k, null) 不该动已有值", "v", cache.get(k));
    }
  }

  /**
   * key 为 null 一律拒绝。
   * <p>
   * 九条覆盖全部方法，含 {@link KVCache#remove(Object)} 与
   * {@link KVCache#computeIfAbsent(Object, java.util.function.Function)} 两个复合默认方法。
   * <p>
   * 但"拒绝"的类型要按 {@link KVCache} 的异常口径分：<b>无条件拒绝某操作的实现不校验参数</b>，
   * null key 也抛 {@link CacheUnsupportedOperationException}（{@link LruKKVCache} 的
   * {@code setnx} / {@code computeIfAbsent} 属于这一类，登记时标了
   * {@code refusesUnsupportedOps}）。{@code remove} 不在其中 —— {@code LruKKVCache} 没有
   * 覆盖它，走默认实现，先校验 key，因此仍是 CacheIllegalArgumentException。
   */
  @Test
  public void testNullKeyRejected() {
    before();
    for (KVCache<String, String> cache : caches) {
      String name = name(cache);
      boolean refusesUnsupportedOps = ((Named) cache).refusesUnsupportedOps;

      assertKeyRejected(name + ": set", () -> cache.set(null, "v"));
      assertKeyRejected(name + ": set(k, null)", () -> cache.set(null, null));
      if (refusesUnsupportedOps) {
        CacheAssertions.assertNotSupported(name + ": setnx",
                () -> cache.setnx(null, "v"));
      } else {
        assertKeyRejected(name + ": setnx", () -> cache.setnx(null, "v"));
      }
      assertKeyRejected(name + ": get", () -> cache.get(null));
      assertKeyRejected(name + ": containsKey", () -> cache.containsKey(null));
      assertKeyRejected(name + ": delete", () -> cache.delete(null));
      assertKeyRejected(name + ": remove", () -> cache.remove(null));

      // 两个复合默认方法同上：无条件拒绝时 CacheUnsupportedOperationException 优先，
      // 否则入口 assertKeyNotNull。
      if (refusesUnsupportedOps) {
        CacheAssertions.assertNotSupported(name + ": computeIfAbsent",
                () -> cache.computeIfAbsent(null, k -> "v"));
        CacheAssertions.assertNotSupported(
                name + ": computeIfAbsent(null, null) 仍是 CacheUnsupportedOperationException，不是 NPE",
                () -> cache.computeIfAbsent(null, null));
      } else {
        assertKeyRejected(name + ": computeIfAbsent",
                () -> cache.computeIfAbsent(null, k -> "v"));
        assertKeyRejected(name + ": computeIfAbsent(null, null) 报 key 的问题，不是 NPE",
                () -> cache.computeIfAbsent(null, null));
      }
    }
  }

  /**
   * <b>setnx 写进来的 key 也必须进淘汰账本</b>。
   * <p>
   * 实现如果把 {@code setnx} 直接转给底层 cache（{@link KVCacheWrapper} 也因此刻意不提供
   * "转发给底层"的默认实现），淘汰策略的账本就会一无所知 —— 账本条数是 0，
   * "账本条数 == 容量"这个触发条件永不成立，淘汰整个失效。表现是缓存插多少活多少，
   * 容量形同虚设。{@link EvictableKVCacheWrapper#setnx(Object, Object)} 因此复用
   * {@link EvictableKVCacheWrapper#set(Object, Object)} 的插入路径，把登记交给两个钩子。
   * <p>
   * 注意 {@code size()} 看不出这件事：它取自数据层，这些 key 照样算得进去，
   * 所以下面那条 {@code size()} 断言只覆盖"值真的写进去了"。
   */
  @Test
  public void testSetnxIsRecordedInEviction() {
    before();
    for (KVCache<String, String> cache : setnxCaches) {
      String name = name(cache);

      for (int i = 0; i < 10; i++) {
        cache.setnx("k" + i, "v" + i);
      }

      int alive = 0;
      for (int i = 0; i < 10; i++) {
        if (cache.get("k" + i) != null) {
          alive++;
        }
      }

      log.info("{}: 容量 {}，setnx 写入 10 个 key 之后 size()={}，实际存活 {}",
              name, CAPACITY, cache.size(), alive);

      Assert.assertTrue(name + ": setnx 进来的值要真的写进去", cache.size() > 0);
      Assert.assertTrue(name + ": size() 不能超过容量，实际 " + cache.size(), cache.size() <= CAPACITY);
      Assert.assertTrue(name + ": 淘汰必须生效，实际存活 " + alive, alive <= CAPACITY);
    }
  }

  /**
   * setnx 不能覆盖已有的值。
   */
  @Test
  public void testSetnxDoesNotOverwrite() {
    before();
    for (KVCache<String, String> cache : setnxCaches) {
      String name = name(cache);
      String k = "k";

      Assert.assertTrue(name + ": key 不存在时应该写入成功", cache.setnx(k, "v1"));
      Assert.assertEquals(name + ": 写进去应该能读出来", "v1", cache.get(k));

      Assert.assertFalse(name + ": key 已存在时应该写入失败", cache.setnx(k, "v2"));
      Assert.assertEquals(name + ": 已存在的值不应该被覆盖", "v1", cache.get(k));
    }
  }

  /**
   * <b>{@link LruKKVCache} 的 {@code setnx} 明确不支持</b>，抛
   * {@link CacheUnsupportedOperationException}。
   * <p>
   * 它和"攒够 {@code threshold} 次访问才落地"这套准入语义对不上：
   * 门槛 >= 2 时本来会恒返回 false，调用方分不清是"已存在"还是"没落地"。
   * 要"写过就进"请用 {@link LruKVCache}。
   */
  @Test
  public void testSetnxIsUnsupportedOnLruKKVCache() {
    CacheAssertions.assertNotSupported("LruKKVCache: setnx",
            () -> new LruKHashMapKVCache<String, String>(CAPACITY, 1).setnx("k", "v"));
    CacheAssertions.assertNotSupported("LruKKVCache: setnx(threshold=2)",
            () -> new LruKHashMapKVCache<String, String>(CAPACITY, 2).setnx("k", "v"));
  }

  /**
   * <b>{@code computeIfAbsent} 命中等价于一次 {@code get} —— 即便 {@code mappingFunction}
   * 一次都没执行。</b>
   * <p>
   * 本层没有覆盖 {@code computeIfAbsent}，走的是接口默认实现，第一步就是 {@code get(k)}；
   * 而 {@code get} 在本层是有副作用的（动访问顺序、动访问次数、分代实现还会触发提升）。
   * 于是"缓存里有，直接返回旧值"这条分支<b>并非无副作用</b>：它改变缓存后续的行为，
   * 改变的内容与直接调一次 {@code get} 完全相同。这条契约写在
   * {@link cn.addenda.component.cache.eviction.EvictableKVCacheWrapper} 的类注释里。
   * <p>
   * 每条实现都用<b>三份同构实例做差分</b>：A 走 {@code computeIfAbsent}（命中）、B 走
   * {@code get}、C 什么都不做，前后步骤三者完全一致。断言 ①A 的 mappingFunction 未被调用；
   * ②A 与 B 的存活集合相同（命中 = 一次 get）；③A 与 C 不同（这次命中确实改变了缓存，
   * ②不是在一个死场景上比空集）。
   * <p>
   * 场景按实现分两类：单代实现写满容量后再逼出一次淘汰，让"这次命中动没动顺序/次数"
   * 显形；分代实现要先把前两条提升进年轻代并把它填满，再看命中仍留在老年代的那条 ——
   * 那时提升得先给年轻代腾位置，会连带淘汰一条无关数据（本用例最狠的一种后果）。
   * <p>
   * {@link LruKKVCache} 不在本用例里：它明确拒绝 {@code computeIfAbsent}
   * （抛 {@link CacheUnsupportedOperationException}），
   * 根本走不到命中的分支。
   */
  @Test
  public void testComputeIfAbsentHitIsEquivalentToOneGet() {
    Consumer<KVCache<String, String>> fill = cache -> {
      cache.set("k0", "v0");
      cache.set("k1", "v1");
      cache.set("k2", "v2");
    };
    // 写满容量之后再写一条，逼出一次淘汰：命中若动过顺序/次数，被淘汰的就不是同一条
    Consumer<KVCache<String, String>> forceOneEviction = cache -> cache.set("new", "v");
    Consumer<KVCache<String, String>> nothing = cache -> {
    };
    List<String> singleGenerationKeys = Arrays.asList("k0", "k1", "k2", "new");

    assertHitIsEquivalentToOneGet("LruKVCache",
            () -> new LruHashMapKVCache<>(CAPACITY), fill, forceOneEviction,
            singleGenerationKeys, "k0");
    assertHitIsEquivalentToOneGet("LfuHashMapKVCache",
            () -> new LfuHashMapKVCache<>(CAPACITY), fill, forceOneEviction,
            singleGenerationKeys, "k0");
    assertHitIsEquivalentToOneGet("LfuLruKVCache",
            () -> new LfuLruKVCache<>(CAPACITY), fill, forceOneEviction,
            singleGenerationKeys, "k0");
    // agingInterval 给到 100：本例只淘汰一次，衰减不会触发，
    // 于是观测到的是"命中是否等于一次 get"这条基线，而不是衰减带来的额外变化
    assertHitIsEquivalentToOneGet("LfuAgingHashMapKVCache",
            () -> new LfuAgingHashMapKVCache<>(CAPACITY, 100, 0.5), fill, forceOneEviction,
            singleGenerationKeys, "k0");
    assertHitIsEquivalentToOneGet("LfuAgingLruKVCache",
            () -> new LfuAgingLruKVCache<>(CAPACITY, 100, 0.5), fill, forceOneEviction,
            singleGenerationKeys, "k0");

    // 分代实现：threshold=0 且睡过 5ms，get 会把够资格的老年代 key 提升进年轻代。
    // 前两条提升之后年轻代（容量 2）正好填满，此时命中仍在老年代的 k2，
    // 提升要先淘汰年轻代队头 k0 —— 一条与 k2 无关的数据。
    Consumer<KVCache<String, String>> fillYoungGeneration = cache -> {
      SleepUtils.sleep(TimeUnit.MILLISECONDS, 5);
      cache.get("k0");
      cache.get("k1");
    };
    Consumer<KVCache<String, String>> fillAndPromote = cache -> {
      fill.accept(cache);
      fillYoungGeneration.accept(cache);
    };
    assertHitIsEquivalentToOneGet("LruGenerationKVCache",
            () -> new LruGenerationHashMapKVCache<>(2, CAPACITY, 0), fillAndPromote, nothing,
            Arrays.asList("k0", "k1", "k2"), "k2");
  }

  /**
   * 用同一个工厂造三份同构实例，分别走"命中 {@code computeIfAbsent} / 一次 {@code get} /
   * 什么都不做"，然后比对三者后续的存活集合。
   *
   * @param before 前置步骤，三份实例都要原样走一遍
   * @param after  后置步骤（逼出淘汰用），同样三份都要走；没有后置步骤时传空实现
   * @param hitKey 被命中的 key —— 必须是这条场景下"命中会产生效果"的那一条
   */
  private void assertHitIsEquivalentToOneGet(String name,
                                             Supplier<KVCache<String, String>> factory,
                                             Consumer<KVCache<String, String>> before,
                                             Consumer<KVCache<String, String>> after,
                                             List<String> probeKeys,
                                             String hitKey) {
    KVCache<String, String> hit = factory.get();
    KVCache<String, String> read = factory.get();
    KVCache<String, String> untouched = factory.get();
    AtomicInteger invoked = new AtomicInteger();

    before.accept(hit);
    before.accept(read);
    before.accept(untouched);

    hit.computeIfAbsent(hitKey, k -> {
      invoked.incrementAndGet();
      return "SHOULD-NOT-BE-USED";
    });
    read.get(hitKey);

    after.accept(hit);
    after.accept(read);
    after.accept(untouched);

    Assert.assertEquals(name + ": key 已在缓存里，mappingFunction 一次都不该执行",
            0, invoked.get());
    Assert.assertEquals(name + ": 命中 = 一次 get，后续的存活集合应完全相同",
            survive(read, probeKeys), survive(hit, probeKeys));
    Assert.assertNotEquals(name + ": 这次命中本应改变缓存（上面的断言才有意义）",
            survive(untouched, probeKeys), survive(hit, probeKeys));
  }

  /**
   * 探一遍给定 key，返回还活着的那些（顺序即入参顺序）。
   * <p>
   * 用 {@code containsKey} 而不是 {@code get}：{@code get} 一边探一边又动一次顺序/次数，
   * 恰好会把要观测的差别抹掉。
   */
  private List<String> survive(KVCache<String, String> cache, List<String> probeKeys) {
    List<String> alive = new ArrayList<>();
    for (String key : probeKeys) {
      if (cache.containsKey(key)) {
        alive.add(key);
      }
    }
    return alive;
  }

  /**
   * <b>淘汰族的并发补救路径：包一层 {@link SynchronizedKVCache}。</b>
   * <p>
   * 本族一律声明"安全级别：无"（统计结构与底层存储是两块独立状态、且不加锁），
   * {@code EvictableKVCacheWrapper} 的类注释把"需要并发时包一层"写成了唯一出路 ——
   * 但在此之前，<b>全仓库没有任何一处真的把淘汰实现包进去验证过</b>，这条承诺是空的。
   * <p>
   * 容量给足（{@code threadCount * 4}）：本用例压的是"包一层之后并发不炸"，
   * 不是淘汰行为；容量不够时各线程的 key 会互相挤掉，"读回自己的值"就会假红。
   */
  @Test
  public void testSynchronizedWrapperIsThreadSafe() throws Exception {
    int threadCount = 4;
    int rounds = 200;

    KVCache<String, String> cache = SynchronizedKVCache.synchronize(new LruHashMapKVCache<>(CAPACITY));

    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);
    List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

    for (int t = 0; t < threadCount; t++) {
      final int threadId = t;
      Thread thread = new Thread(() -> {
        String ownValue = "v" + threadId;
        try {
          start.await();
          for (int i = 0; i < rounds; i++) {
            // 所有线程写同一批 key，且每轮都写满到触发淘汰 —— 队列（LinkedHashSet）
            // 的 add / remove / iterator 被并发改，这才是包装层要挡的地方。
            for (int k = 0; k < KEY_COUNT; k++) {
              String key = "EvictableKVCacheWrapperContractTest:sync:k" + k;
              cache.set(key, ownValue);
              // 读到 null 是合法的（刚写完就被别的线程挤掉了），断言的是"不能读到垃圾"：
              // 要么没写过，要么是某个线程写进去的那几个值之一。
              // 注意不能断言"写进去就一定读得回来"—— 那是淘汰语义，不是并发语义。
              String read = cache.get(key);
              Assert.assertTrue("读到的必须是写过的值或 null，实际：" + read,
                      read == null || read.startsWith("v"));
            }
          }
        } catch (Throwable e) {
          failures.add(new IllegalStateException("线程 " + threadId + " 失败", e));
        } finally {
          done.countDown();
        }
      });
      thread.setDaemon(true);
      thread.start();
    }

    start.countDown();
    Assert.assertTrue("不应该死循环或死锁", done.await(60, TimeUnit.SECONDS));
    for (Throwable failure : failures) {
      log.error("", failure);
    }
    Assert.assertTrue("包一层之后并发下不应该抛异常：" + failures, failures.isEmpty());
  }

  /**
   * 同一把锁也把 {@code computeIfAbsent} 的 check-then-act 补成原子的 ——
   * 淘汰族自己用的是接口默认实现（get + set 两步），不包一层的话并发下会被算多次。
   */
  @Test
  public void testSynchronizedWrapperMakesComputeIfAbsentAtomic() throws Exception {
    int threadCount = 8;
    String key = "EvictableKVCacheWrapperContractTest:sync:computeIfAbsent";

    KVCache<String, String> cache = SynchronizedKVCache.synchronize(
            new LruHashMapKVCache<>(threadCount));

    AtomicInteger invoked = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);

    for (int t = 0; t < threadCount; t++) {
      Thread thread = new Thread(() -> {
        try {
          start.await();
          cache.computeIfAbsent(key, k -> {
            invoked.incrementAndGet();
            // 把 check 与 act 之间的窗口拉大，让"没锁住"暴露出来
            SleepUtils.sleep(TimeUnit.MILLISECONDS, 100);
            return "computed";
          });
        } catch (Throwable e) {
          log.error("", e);
        } finally {
          done.countDown();
        }
      });
      thread.setDaemon(true);
      thread.start();
    }

    start.countDown();
    Assert.assertTrue("不应该死循环或死锁", done.await(60, TimeUnit.SECONDS));

    log.error("SynchronizedKVCache(LruKVCache): {} 个线程并发 computeIfAbsent，mappingFunction 被调用了 {} 次",
            threadCount, invoked.get());
    Assert.assertEquals("整段 check-then-act 在同一把锁里，只应该算一次", 1, invoked.get());
  }

  private void assertKeyRejected(String what, Runnable invocation) {
    try {
      invocation.run();
      Assert.fail(what + "(null, ...) 应该抛 CacheIllegalArgumentException");
    } catch (CacheIllegalArgumentException expected) {
      // ok
    }
  }

  private String name(KVCache<String, String> cache) {
    return ((Named) cache).name;
  }

  /**
   * 每个用例跑之前都要新造一批实例：淘汰策略是有状态的，跑完一个用例就脏了。
   * 这里用装饰类把名字带上，方便断言失败时定位是哪个实现。
   */
  private static class Named implements KVCache<String, String> {

    private final String name;
    private final KVCache<String, String> delegate;
    /**
     * {@code setnx} / {@code computeIfAbsent} 是否无条件拒绝（此时不校验参数）
     */
    private final boolean refusesUnsupportedOps;

    private Named(String name, KVCache<String, String> delegate) {
      this(name, delegate, false);
    }

    private Named(String name, KVCache<String, String> delegate, boolean refusesUnsupportedOps) {
      this.name = name;
      this.delegate = delegate;
      this.refusesUnsupportedOps = refusesUnsupportedOps;
    }

    /**
     * 两个复合方法必须显式转发：不覆盖的话调用会落到 {@link KVCache} 的默认实现上，
     * 测到的就是默认实现的行为（入口 {@code assertKeyNotNull}），而不是被测实现的
     * "无条件拒绝、不校验参数"。
     */
    @Override
    public String remove(String k) {
      return delegate.remove(k);
    }

    @Override
    public String computeIfAbsent(String key, Function<? super String, ? extends String> mappingFunction) {
      return delegate.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public void set(String k, String v) {
      delegate.set(k, v);
    }

    @Override
    public boolean setnx(String k, String v) {
      return delegate.setnx(k, v);
    }

    @Override
    public boolean containsKey(String k) {
      return delegate.containsKey(k);
    }

    @Override
    public String get(String k) {
      return delegate.get(k);
    }

    @Override
    public boolean delete(String k) {
      return delegate.delete(k);
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public int capacity() {
      return delegate.capacity();
    }

  }

}
