package cn.addenda.component.cache.test.eviction.lru;

import cn.addenda.component.base.util.SleepUtils;
import cn.addenda.component.cache.HashMapKVCache;
import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.eviction.lru.LruGenerationHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruGenerationKVCache;
import cn.addenda.component.cache.eviction.lru.LruGenerationKVCache.Entry;
import cn.addenda.component.cache.eviction.lru.LruHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruKVCache;
import cn.addenda.component.cache.test.CacheAssertions;
import cn.addenda.component.cache.test.TestCapacityLimit;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * {@link LruGenerationKVCache} 的行为测试。
 * <p>
 * 这个类的核心是<b>一道时间闸门</b>：key 进老年代时记下入队时刻，
 * 之后被读到的时候，只有"入队到现在"达到（{@code >=}）{@code threshold} 才提升到年轻代；
 * 窗口之内怎么读都不提升，也不会把窗口推后。
 * <p>
 * 测试里阈值一律显式给，用下面两个常量把两种情况钉死 ——
 * 随手传 0 或者随手传个大数都会让用例的实际语义变得看不出来。
 *
 * @author addenda
 */
public class LruGenerationKVCacheTest {

  private static final int NEW_CAPACITY = 3;
  private static final int OLD_CAPACITY = 2;

  /**
   * 观察期取 0：判定式是"入队至今 >= threshold"，所以任何一次读取都会提升（<b>含同一毫秒</b>）。
   * <p>
   * 用例仍在读之前调用 {@link #sleepPastWindow()}（睡 5 毫秒）：那只是让"出了窗口"在字面上
   * 也成立，行为并不依赖它 —— 0 的语义已经包含同一毫秒。
   */
  private static final int PROMOTE_THRESHOLD = 0;

  /**
   * 观察期长到读多少次都出不去：用来测"窗口内不提升"。
   */
  private static final int NEVER_PROMOTE_THRESHOLD = 1_000_000;

  /**
   * 报表场景用的观察期（毫秒）：一次扫描里对同一个 key 的重复访问都落在这个窗口内
   */
  private static final int SCAN_WINDOW = 1000;

  // ------------------------------------------------------------------
  // 基本行为
  // ------------------------------------------------------------------

  /**
   * 新 key 一律进老年代，但读得到、也数得到。
   */
  @Test
  public void testNewKeyGoesToOldGeneration() {
    LruGenerationKVCache<String, String> c = newCache(NEVER_PROMOTE_THRESHOLD);

    c.set("a", "1");

    Assert.assertEquals("新 key 进老年代，但要读得到", "1", c.get("a"));
    Assert.assertTrue("containsKey 必须和 get 一致", c.containsKey("a"));
    Assert.assertEquals("size 要算上老年代", 1, c.size());
  }

  /**
   * <b>老年代的容量独立生效</b>：只写不读的时候，能活下来的是 {@code oldCapacity} 条 ——
   * 新 key 都先落老年代，年轻代不会帮忙装。
   */
  @Test
  public void testOldGenerationCapacityLimitsNewWrites() {
    LruGenerationKVCache<String, String> c = newCache(NEVER_PROMOTE_THRESHOLD);

    c.set("a", "1");
    c.set("b", "2");
    Assert.assertEquals(2, c.size());

    c.set("c", "3");

    Assert.assertEquals("老年代只有 2 格", 2, c.size());
    Assert.assertNull("最旧的 a 被淘汰", c.get("a"));
    Assert.assertEquals("2", c.get("b"));
    Assert.assertEquals("3", c.get("c"));
  }

  /**
   * {@code capacity()} 报两代之和。
   */
  @Test
  public void testCapacityIsBothGenerations() {
    LruGenerationKVCache<String, String> c = newCache(NEVER_PROMOTE_THRESHOLD);
    Assert.assertEquals(NEW_CAPACITY + OLD_CAPACITY, c.capacity());
  }

  // ------------------------------------------------------------------
  // 闸门本身
  // ------------------------------------------------------------------

  /**
   * <b>出了观察期再读到 → 提升到年轻代</b>。
   * <p>
   * 验证方式：提升之后 {@code a} 就和老年代是两套容量了 ——
   * 接下来故意让老年代翻天覆地，{@code a} 也该毫发无损。
   * 如果它没被提升（还留在老年代），这一轮 churn 会把它清掉。
   */
  @Test
  public void testReadAfterWindowPromotesToYoungGeneration() throws Exception {
    LruGenerationKVCache<String, String> c = newCache(PROMOTE_THRESHOLD);

    c.set("a", "1");
    c.set("b", "2");

    sleepPastWindow();
    Assert.assertEquals("1", c.get("a"));      // 出窗口 → 提升

    // 把老年代整个换一遍血（老年代容量 2）
    c.set("c", "3");
    c.set("d", "4");
    c.set("e", "5");

    Assert.assertEquals("a 已经搬进年轻代，不受老年代淘汰影响", "1", c.get("a"));
    Assert.assertNull("b 一直在老年代，早被换掉了", c.get("b"));
  }

  /**
   * <b>threshold = 0：同一毫秒内读到也提升</b> —— 判定式是"入队至今 >= threshold"。
   * <p>
   * 观测方式是提升之后年轻代队列长度变为 1。写与读紧挨着，正常情况下落在同一毫秒；
   * 但 Java 里没有可注入的时钟，无法<b>强制</b>两次调用同毫秒 —— 若恰好跨了毫秒，旧实现
   * （{@code > threshold}）也会提升。所以这条用例是回归网，不是"同一毫秒"的确定性证明。
   */
  @Test
  public void testThresholdZeroPromotesOnImmediateRead() {
    ExposedLruGeneration<String, String> c = new ExposedLruGeneration<>(NEW_CAPACITY,
            new HashMapKVCache<String, String>(),
            new LruHashMapKVCache<String, Entry<String>>(OLD_CAPACITY), PROMOTE_THRESHOLD);

    c.set("a", "1");
    Assert.assertEquals("前置条件：刚写进去，年轻代还空着", 0L, c.dequeSize());

    Assert.assertEquals("读得到", "1", c.get("a"));
    Assert.assertEquals("threshold=0：紧接写入的读也应提升", 1L, c.dequeSize());
  }

  /**
   * <b>观察期内读到 → 留在老年代</b>，连访问顺序都不动。
   */
  @Test
  public void testReadWithinWindowStaysInOldGeneration() {
    LruGenerationKVCache<String, String> c = newCache(NEVER_PROMOTE_THRESHOLD);

    c.set("a", "1");
    c.set("b", "2");

    Assert.assertEquals("1", c.get("a"));      // 窗口内，不提升也不重排

    c.set("c", "3");

    Assert.assertNull("没被提升，老年代满了照样第一个淘汰它", c.get("a"));
    Assert.assertEquals("2", c.get("b"));
    Assert.assertEquals("3", c.get("c"));
  }

  /**
   * <b>窗口内重复读多少次都不会提升</b> —— 这就是防扫描的那条性质。
   * <p>
   * 读的间隔不重要，重要的是"距入队"还没出窗口。
   */
  @Test
  public void testRepeatedReadsWithinWindowNeverPromote() {
    LruGenerationKVCache<String, String> c = newCache(NEVER_PROMOTE_THRESHOLD);

    c.set("r", "v");
    for (int i = 0; i < 100; i++) {
      Assert.assertEquals("v", c.get("r"));
    }

    // 老年代依然把它当冷数据：再写两条就把它挤出去
    c.set("x", "1");
    c.set("y", "2");

    Assert.assertNull("窗口内读 100 次也不该被提升", c.get("r"));
    Assert.assertEquals("1", c.get("x"));
    Assert.assertEquals("2", c.get("y"));
  }

  /**
   * <b>覆盖写不提升</b>：这个设计里"变热"的触发点是读，不是写。
   */
  @Test
  public void testOverwriteDoesNotPromote() {
    LruGenerationKVCache<String, String> c = newCache(NEVER_PROMOTE_THRESHOLD);

    c.set("a", "1");
    c.set("b", "2");
    c.set("a", "1-new");       // 覆盖写，不该把它变成热的 → 重排到老年代队尾，old=[b,a]

    // 老年代只有 2 格，再写两条就够把 a 挤出去
    c.set("c", "3");           // 淘汰 b，old=[a,c]
    Assert.assertNotNull("a排到老年代队尾，淘汰的是b", c.get("a"));
    Assert.assertNull("a排到老年代队尾，淘汰的是b", c.get("b"));
    c.set("d", "4");           // 淘汰 a，old=[c,d]

    Assert.assertNull("覆盖写没有提升它，所以它跟着老年代一起被淘汰", c.get("a"));
    Assert.assertEquals("3", c.get("c"));
    Assert.assertEquals("4", c.get("d"));
  }

  /**
   * <b>覆盖写也不刷新"入队时刻"</b>。
   * <p>
   * 观察期是"从入队算起"，不是"从最后一次写算起"。如果每次重写都把入队时刻刷成当前时间，
   * 那一个"每隔一会儿就被重写一次"的 key 会永远停在窗口里，和热数据一样升不上去 ——
   * 等于把坑从读路径挪到了写路径。
   * <p>
   * 这里在出了窗口之后重写一次，再读：它照样该被提升。
   */
  @Test
  public void testOverwriteDoesNotResetEntryTime() throws Exception {
    // 观察期必须明显大于"覆盖写 → 读"之间的耗时：threshold=0 时 elapsed >= 0 恒成立，
    // 无论 enterTm 是否被刷新，紧接着的读都会提升 —— 那就测不出差别了。
    LruGenerationKVCache<String, String> c = newCache(SCAN_WINDOW);

    c.set("a", "1");
    sleepPastScanWindow();      // 此时距入队已经出了窗口

    c.set("a", "1-new");        // 覆盖写：值换新，但入队时刻不该动

    Assert.assertEquals("1-new", c.get("a"));   // 出窗口 → 该提升

    // 验证它真进了年轻代：把老年代换一遍
    c.set("b", "2");
    c.set("c", "3");
    c.set("d", "4");

    Assert.assertEquals("入队时刻没被重写推后，所以它升上去了", "1-new", c.get("a"));
  }

  /**
   * <b>覆盖写一个已经提升到年轻代的 key</b>：原地更新，不落回老年代，也不触发老年代淘汰。
   * <p>
   * 与上一条的区别：那条讲"入队时刻不被重写推后"，这条讲"覆盖写之后它属于哪一代"。
   * 年轻代中的覆盖写走父类路径（重排到队尾 + 更新 value），老年代完全不参与；
   * 验证手法与上一条一致 —— 把老年代整个换一遍血，看它是否还活着。
   */
  @Test
  public void testOverwritePromotedKeyKeepsItInYoungGeneration() throws Exception {
    LruGenerationKVCache<String, String> c = newCache(PROMOTE_THRESHOLD);

    c.set("a", "1");
    c.set("b", "2");
    sleepPastWindow();
    Assert.assertEquals("1", c.get("a"));            // 出窗口 → 提升到年轻代
    Assert.assertEquals("搬家不是复制", 2, c.size());

    c.set("a", "1-new");                             // 覆盖写：此时 a 在年轻代里
    Assert.assertEquals("值要更新", "1-new", c.get("a"));
    Assert.assertEquals("覆盖写不产生新条目", 2, c.size());

    // 把老年代整个换一遍血（老年代容量 2）：若 a 被写回了老年代，它会在这里被挤掉
    c.set("c", "3");
    c.set("d", "4");
    Assert.assertEquals("a 还在，说明覆盖写没有把它挪回老年代", "1-new", c.get("a"));
  }

  // ------------------------------------------------------------------
  // 搬家 / 淘汰 / 删除
  // ------------------------------------------------------------------

  /**
   * 提升是"搬家"不是"复制"：搬完 total size 不该变，delete 也要能一次清干净。
   */
  @Test
  public void testPromotionMovesInsteadOfCopying() throws Exception {
    LruGenerationKVCache<String, String> c = newCache(PROMOTE_THRESHOLD);

    c.set("a", "1");
    c.set("b", "2");

    sleepPastWindow();
    Assert.assertEquals("1", c.get("a"));      // 提升
    Assert.assertEquals("搬完总数不变，不能同时算在两代里", 2, c.size());

    Assert.assertTrue("提升过去的 key 也要删得掉", c.delete("a"));
    Assert.assertNull(c.get("a"));
    Assert.assertEquals(1, c.size());

    Assert.assertEquals("b 不受影响", "2", c.get("b"));
  }

  /**
   * 年轻代满了会淘汰自己的队头，而且只动年轻代。
   */
  @Test
  public void testYoungGenerationEvictsItsOwnOldest() throws Exception {
    LruGenerationKVCache<String, String> c = new LruGenerationHashMapKVCache<>(1, 2, PROMOTE_THRESHOLD);

    c.set("a", "1");
    c.set("b", "2");

    sleepPastWindow();
    Assert.assertEquals("1", c.get("a"));      // a 提升，年轻代 = [a]
    Assert.assertEquals("2", c.get("b"));      // b 提升，年轻代满了 → 淘汰 a

    Assert.assertNull("年轻代只有 1 格，a 被它自己淘汰", c.get("a"));
    Assert.assertEquals("2", c.get("b"));
  }

  /**
   * <b>{@code remove} 不经过会提升的读路径</b>：删一个老年代的 key，
   * 不该连带淘汰年轻代里那条与它无关的数据。
   * <p>
   * 接口默认实现是"get 后再 delete"。这里的 {@code get} 一旦命中老年代且已过观察期，
   * 就会把该 key <b>提升</b>进年轻代，而提升要先淘汰年轻代的队头 —— 紧接着同一个 key
   * 就被删掉了，提升纯属白做，<b>被挤掉的那个 key 却回不来</b>。
   * 本类因此覆盖了 remove：取值走"只读底层、不操作队列"的旁路
   * （见 {@code LruKVCache#onlyGet}），取值与删除之间不产生任何淘汰。
   * <p>
   * 对照见 {@link #testReadPromotionStillEvictsYoungGenerationHead()}：{@code get} 的
   * 提升（含那次淘汰）是它自己的既有语义，本用例保证的是 {@code remove} 不借用它。
   */
  @Test
  public void testRemoveDoesNotEvictAnUnrelatedKey() throws Exception {
    LruGenerationKVCache<String, String> c =
            new LruGenerationHashMapKVCache<>(2, 3, PROMOTE_THRESHOLD);

    c.set("a", "1");
    c.set("b", "2");
    c.set("c", "3");                      // 三个都在老年代

    sleepPastWindow();
    Assert.assertEquals("1", c.get("a"));  // a 提升，年轻代 = [a]
    sleepPastWindow();
    Assert.assertEquals("2", c.get("b"));  // b 提升，年轻代满了 = [a, b]

    Assert.assertEquals("remove 要返回老年代里那个值", "3", c.remove("c"));

    Assert.assertEquals("a 不该被 remove(c) 带走", "1", c.get("a"));
    Assert.assertEquals("b 照旧", "2", c.get("b"));
    Assert.assertNull("c 确实删掉了", c.get("c"));
    Assert.assertEquals("只少了一条", 2, c.size());
  }

  /**
   * <b>对照组：{@code get} 的提升确实会淘汰年轻代队头。</b>
   * <p>
   * 这不是缺陷而是 {@code get} 的既有语义 —— 提升要腾位置。钉住它是为了把边界划清：
   * 上一条用例保证的是"<b>remove</b> 不走这条路径"，而不是"提升不会淘汰"。
   * 哪天有人把提升改成不淘汰（或改成淘汰别的），这里会红。
   */
  @Test
  public void testReadPromotionStillEvictsYoungGenerationHead() throws Exception {
    LruGenerationKVCache<String, String> c =
            new LruGenerationHashMapKVCache<>(2, 3, PROMOTE_THRESHOLD);

    c.set("a", "1");
    c.set("b", "2");
    c.set("c", "3");

    sleepPastWindow();
    Assert.assertEquals("1", c.get("a"));
    sleepPastWindow();
    Assert.assertEquals("2", c.get("b"));

    Assert.assertEquals("读 c 会把它提升进年轻代，腾位置时挤掉队头 a", "3", c.get("c"));
    Assert.assertNull("a 被这次读带走 —— 这是 get 的语义，不是 remove 的", c.get("a"));
    Assert.assertEquals("b 还在", "2", c.get("b"));
  }

  /**
   * <b>delete 的返回值要对</b>：在老年代里也要返回 true。
   */
  @Test
  public void testDeleteReturnsWhetherItExisted() throws Exception {
    LruGenerationKVCache<String, String> c = newCache(PROMOTE_THRESHOLD);

    c.set("a", "1");
    Assert.assertTrue("老年代里的 key 删掉要返回 true", c.delete("a"));
    Assert.assertFalse("再删一次返回 false", c.delete("a"));
    Assert.assertFalse("压根没有的 key 返回 false", c.delete("never-existed"));

    // 提升到年轻代之后同样要返回 true
    c.set("b", "2");
    sleepPastWindow();
    Assert.assertEquals("2", c.get("b"));
    Assert.assertTrue("年轻代里的 key 删掉也要返回 true", c.delete("b"));
  }

  /**
   * 删掉之后再写回来，之前提升到年轻代的那份也要清掉，不能留个幽灵。
   */
  @Test
  public void testDeleteThenSetAgain() throws Exception {
    LruGenerationKVCache<String, String> c = newCache(PROMOTE_THRESHOLD);

    c.set("a", "1");
    sleepPastWindow();
    Assert.assertEquals("1", c.get("a"));      // 提升到年轻代

    c.delete("a");
    Assert.assertNull(c.get("a"));

    c.set("a", "1-again");
    Assert.assertEquals("重新写进来的是新入队的，读得到就行", "1-again", c.get("a"));
    Assert.assertEquals(1, c.size());
  }

  /**
   * 删掉老年代里的条目能腾出格子。
   * <p>
   * 删的是<b>队尾</b>那条（b），不是队头：只删队头的话，后续淘汰本来就会先挑它，
   * 测不出"任意位置都能腾出来"。同时断言被删的 key 真的没了、条数恢复 ——
   * 否则"只删了队列、没删数据层"这种实现也能蒙混过关。
   */
  @Test
  public void testDeleteFreesOldGenerationSlot() {
    LruGenerationKVCache<String, String> c = newCache(NEVER_PROMOTE_THRESHOLD);

    c.set("a", "1");
    c.set("b", "2");                  // 老年代满（容量 2），队头是 a、队尾是 b

    Assert.assertTrue(c.delete("b"));
    c.set("c", "3");

    Assert.assertEquals("a 不该被淘汰", "1", c.get("a"));
    Assert.assertEquals("c 补上了空位", "3", c.get("c"));
    Assert.assertNull("b 确实删掉了", c.get("b"));
    Assert.assertEquals("老年代恢复满员：a + c", 2, c.size());
  }

  // ------------------------------------------------------------------
  // 报表场景（这个类存在的理由）
  // ------------------------------------------------------------------

  /**
   * <b>报表扫一遍，核心数据不能掉</b>。
   * <p>
   * 报表的典型动作是 cache-aside：读未命中 → 回源 → 写回缓存 → 可能再取一次。
   * 这一整串都发生在毫秒级，全部落在 {@link #SCAN_WINDOW} 之内 ——
   * 所以报表数据只会待在被冲刷的老年代，一条也进不了年轻代。
   */
  @Test
  public void testReportScanDoesNotFlushCoreData() throws Exception {
    int capacity = 50;
    LruGenerationKVCache<String, String> c =
            new LruGenerationHashMapKVCache<>(capacity, capacity, SCAN_WINDOW);

    // 核心业务数据：写进来，过了观察期再读一次 → 全部进年轻代
    for (int i = 0; i < capacity; i++) {
      c.set("core" + i, "v");
    }
    sleepPastScanWindow();
    for (int i = 0; i < capacity; i++) {
      Assert.assertEquals("v", c.get("core" + i));
    }

    // 报表扫 500 条冷数据，每条都"读未命中 → 写回 → 再取一次"
    for (int i = 0; i < 500; i++) {
      c.get("r" + i);
      c.set("r" + i, "v");
      Assert.assertEquals("v", c.get("r" + i));
    }

    // 再把老年代整个冲一轮：只要核心数据真在年轻代里，这一轮就伤不到它；
    // 反过来，如果它还在老年代，这一轮会把它清空 —— 让对错两种情况的差距足够明显
    for (int i = 0; i < capacity; i++) {
      c.set("churn" + i, "v");
    }

    // 用 containsKey 数，不用 get —— get 会把老年代里的 key 提升走，数一次就改了被测状态
    int alive = 0;
    for (int i = 0; i < capacity; i++) {
      if (c.containsKey("core" + i)) {
        alive++;
      }
    }
    Assert.assertEquals("报表扫完，核心数据应该一条不少", capacity, alive);
  }

  /**
   * 对照组：报表数据自己也不该把年轻代占满 —— 它压根没进去过。
   */
  @Test
  public void testReportDataStaysInOldGeneration() {
    LruGenerationKVCache<String, String> c =
            new LruGenerationHashMapKVCache<>(NEW_CAPACITY, OLD_CAPACITY, SCAN_WINDOW);

    for (int i = 0; i < 100; i++) {
      c.set("r" + i, "v");
      c.get("r" + i);          // 紧接着读，还在窗口内
    }

    // 老年代都被报表数据churn过不知道多少轮了，但容量上限还是老年代那两格
    Assert.assertEquals("报表数据只占老年代", OLD_CAPACITY, c.size());
  }

  // ------------------------------------------------------------------
  // null 契约
  // ------------------------------------------------------------------

  /**
   * {@code set(k, null)} 走基类的 {@code delete(k)}，两代都会清。
   */
  @Test
  public void testSetNullDeletes() {
    LruGenerationKVCache<String, String> c = newCache(NEVER_PROMOTE_THRESHOLD);

    c.set("a", "1");
    c.set("b", "2");

    c.set("b", null);

    Assert.assertNull("set(b, null) 要能把老年代里的 b 删掉", c.get("b"));
    Assert.assertFalse(c.containsKey("b"));
    Assert.assertEquals(1, c.size());
    Assert.assertEquals("删掉 b 腾出的位置还在，a 不受影响", "1", c.get("a"));
  }

  /**
   * {@code set(k, null)} 对提升到年轻代的 key 同样要清干净。
   */
  @Test
  public void testSetNullDeletesFromYoungGeneration() throws Exception {
    LruGenerationKVCache<String, String> c = newCache(PROMOTE_THRESHOLD);

    c.set("a", "1");
    sleepPastWindow();
    Assert.assertEquals("1", c.get("a"));      // 提升
    Assert.assertEquals(1, c.size());

    c.set("a", null);

    Assert.assertNull(c.get("a"));
    Assert.assertFalse(c.containsKey("a"));
    Assert.assertEquals("年轻代那份也要清掉", 0, c.size());
  }

  /**
   * set / setnx 对 null 的处理，见 {@code KVCache} 上的契约。
   */
  @Test
  public void testNullKeyAndNullValue() {
    LruGenerationKVCache<String, String> c = newCache(NEVER_PROMOTE_THRESHOLD);

    c.set("a", "1");

    // setnx 的 value 为 null：不写入、返回 false
    Assert.assertFalse(c.setnx("b", null));
    Assert.assertNull("不该写入", c.get("b"));

    // 已有值时不动它
    c.set("b", "2");
    Assert.assertFalse(c.setnx("b", null));
    Assert.assertEquals("不该动已有值", "2", c.get("b"));

    // 对一个从没写过的 key 置空，必须是彻底的空操作：
    // 老年代只有 2 格，这里塞进幽灵条目的话 a 会被挤掉
    c.set("never-existed", null);
    Assert.assertEquals("a 不该被这个空操作挤掉", "1", c.get("a"));
    Assert.assertEquals("b 不该被这个空操作挤掉", "2", c.get("b"));
    Assert.assertFalse("幽灵 key 本身也不该存在", c.containsKey("never-existed"));

    // key 为 null 一律拒绝
    CacheAssertions.assertRejected("LruGenerationKVCache: set", () -> c.set(null, "v"));
    CacheAssertions.assertRejected("LruGenerationKVCache: set(k, null)", () -> c.set(null, null));
    CacheAssertions.assertRejected("LruGenerationKVCache: setnx", () -> c.setnx(null, "v"));
    CacheAssertions.assertRejected("LruGenerationKVCache: setnx(k, null)", () -> c.setnx(null, null));
    CacheAssertions.assertRejected("LruGenerationKVCache: get", () -> c.get(null));
    CacheAssertions.assertRejected("LruGenerationKVCache: containsKey", () -> c.containsKey(null));
    CacheAssertions.assertRejected("LruGenerationKVCache: delete", () -> c.delete(null));
  }

  // ------------------------------------------------------------------

  /**
   * <b>年轻代队列被外部数据顶到超过 {@code newCapacity} 时，容量仍应收敛</b>。
   * <p/>
   * 与 {@code EvictableKVCacheWrapperConsistencyTest#testCapacityConvergesWhenDelegateIsPrefilledBeyondCapacity}
   * 同源：年轻代的存储是外部传进来的，可以直接预置数据；这些 key 会在下次访问时被补登记进
   * 年轻代队列，把队列顶到超过 {@code newCapacity}。若提升时的触发条件写成
   * {@code newCapacity == 队列长度}，那时等式永不成立 —— 年轻代此后再也不淘汰。
   * 断言的是对外可见的不变量：{@code size() <= capacity()}。
   */
  @Test
  public void testYoungGenerationConvergesWhenDelegateIsPrefilledBeyondCapacity() throws Exception {
    HashMapKVCache<String, String> youngDelegate = new HashMapKVCache<>();
    for (int i = 0; i < NEW_CAPACITY * 2; i++) {
      youngDelegate.set("young" + i, "v" + i);
    }
    LruHashMapKVCache<String, Entry<String>> oldGeneration =
            new LruHashMapKVCache<>(OLD_CAPACITY);

    ExposedLruGeneration<String, String> c = new ExposedLruGeneration<>(
            NEW_CAPACITY, youngDelegate, oldGeneration, PROMOTE_THRESHOLD);

    // 逐个 get：按设计"下次访问时补登记"进年轻代队列
    for (int i = 0; i < NEW_CAPACITY * 2; i++) {
      Assert.assertNotNull("外部写入的值读得到", c.get("young" + i));
    }
    Assert.assertTrue("前置条件：年轻代队列已超容量，队列=" + c.dequeSize(),
            c.dequeSize() > NEW_CAPACITY);
    Assert.assertEquals("且底层存储确实有那么多条", NEW_CAPACITY * 2, youngDelegate.size());

    // 造一次提升：老年代里的 key 出了观察期再读到，就会提升进年轻代
    c.set("old", "v");
    sleepPastWindow();
    c.get("old");

    // 提升要把年轻代清到低于 newCapacity 再放入，队列与底层存储必须一起收敛。
    // 不能只看 size() <= capacity()（两代之和）：年轻代仍超容量、老年代恰好空着时，
    // 两代之和照样可能落在上限内，测不出"年轻代没收敛"。
    Assert.assertEquals("年轻代队列收敛到 newCapacity", NEW_CAPACITY, c.dequeSize());
    Assert.assertEquals("年轻代存储同步收敛", NEW_CAPACITY, youngDelegate.size());
    Assert.assertTrue("刚提升的 key 在年轻代里", youngDelegate.containsKey("old"));
    Assert.assertFalse("被挤掉的队头已从存储里删掉", youngDelegate.containsKey("young0"));
    Assert.assertEquals("old 已从老年代搬走", 0, oldGeneration.size());
    Assert.assertEquals("总条数 = 年轻代那几条", NEW_CAPACITY, c.size());
  }

  /**
   * 仅供测试：暴露 {@code LruGenerationKVCache} 接受"年轻代存储"的 protected 构造器，
   * 以及年轻代队列长度。
   */
  private static class ExposedLruGeneration<K, V> extends LruGenerationKVCache<K, V> {
    private ExposedLruGeneration(int newCapacity, KVCache<K, V> kvCacheDelegate,
                                 LruKVCache<K, Entry<V>> oldGenerationKVCache, int threshold) {
      super(newCapacity, kvCacheDelegate, oldGenerationKVCache, threshold);
    }

    /**
     * 年轻代队列长度
     */
    private long dequeSize() {
      return getLruDeque().size();
    }
  }

  /**
   * 观察期不允许为负 —— <b>但 0 必须仍然被接受</b>。
   * <p>
   * 负数没有意义：判定式是 {@code 入队至今 >= threshold}，而"入队至今"恒 {@code >= 0}，
   * 于是老年代里的 key 一被读到就立刻提升，闸门形同关闭，却和"的确设了 0"分不出来。
   * <p>
   * 0 是有明确含义的取值（"任何一次读取都视为合格，含同一毫秒"），本文件的
   * {@link #PROMOTE_THRESHOLD} 与 {@code EvictableKVCacheWrapperContractTest} 都在用它，
   * 所以校验不能收紧到 {@code >= 1}。
   */
  @Test
  public void testThresholdMustNotBeNegative() {
    CacheAssertions.assertRejected("threshold = -1",
            () -> new LruGenerationHashMapKVCache<>(NEW_CAPACITY, OLD_CAPACITY, -1));
    CacheAssertions.assertRejected("threshold = Integer.MIN_VALUE",
            () -> new LruGenerationHashMapKVCache<>(NEW_CAPACITY, OLD_CAPACITY, Integer.MIN_VALUE));

    Assert.assertNotNull("threshold = 0 是合法取值，不能被拒",
            new LruGenerationHashMapKVCache<>(NEW_CAPACITY, OLD_CAPACITY, 0));
  }

  /**
   * <b>两代容量之和也不得超过 {@code KVCache.UNLIMITED_CAPACITY}。</b>
   * <p>
   * {@link #capacity()} 报的是两代之和，它就是本实例的容量，因此同样要落在
   * {@code [0, UNLIMITED_CAPACITY]} 内（见 {@link KVCache#UNLIMITED_CAPACITY}：
   * 所有合法性判断都以它为准）—— 不是"相加不溢出即可"。
   * <p>
   * 单代那一侧由 {@code EvictableKVCacheWrapper} 把关；这里测的是"单代都合法、
   * 加起来却超了"这一种。单代自己超上限在取 {@code int} 上界时写不出来，因此这里
   * <b>临时把上限调低</b>（{@link TestCapacityLimit}）再构造，跑完在 {@code finally} 里恢复。
   */
  @Test
  public void testCapacitySumOverUnlimitedIsRejectedAtConstruction() {
    int lowered = 10_000;
    int original = TestCapacityLimit.setUnlimitedCapacity(lowered);
    try {
      int max = KVCache.UNLIMITED_CAPACITY;
      Assert.assertEquals("上限应已被临时调低，否则下面\"单代自己超上限\"造不出来",
              lowered, max);

      CacheAssertions.assertRejected("单代都合法，但两代之和超上限",
              () -> new LruGenerationHashMapKVCache<>(max, 1, PROMOTE_THRESHOLD));

      // 边界：两代之和恰好等于上限是允许的
      LruGenerationKVCache<String, String> edge =
              new LruGenerationHashMapKVCache<>(max - 1, 1, PROMOTE_THRESHOLD);
      Assert.assertEquals("两代之和恰好等于上限", max, edge.capacity());
      Assert.assertTrue("绝不能是负数", edge.capacity() > 0);

      CacheAssertions.assertRejected("年轻代容量自己超上限",
              () -> new LruGenerationHashMapKVCache<>(max + 1, 1, PROMOTE_THRESHOLD));
      CacheAssertions.assertRejected("老年代容量自己超上限",
              () -> new LruGenerationHashMapKVCache<>(NEW_CAPACITY, max + 1, PROMOTE_THRESHOLD));

      // 控制组：正常容量照常（在上限被调低的情况下同样适用）
      LruGenerationKVCache<String, String> c =
              new LruGenerationHashMapKVCache<>(NEW_CAPACITY, OLD_CAPACITY, PROMOTE_THRESHOLD);
      Assert.assertEquals(NEW_CAPACITY + OLD_CAPACITY, c.capacity());
      Assert.assertEquals(0, c.size());
    } finally {
      TestCapacityLimit.setUnlimitedCapacity(original);
    }
  }

  private LruGenerationKVCache<String, String> newCache(int threshold) {
    return new LruGenerationHashMapKVCache<>(NEW_CAPACITY, OLD_CAPACITY, threshold);
  }

  /**
   * 睡过观察期，让"距入队"确定地大于 threshold（这里是 {@link #PROMOTE_THRESHOLD} = 0）。
   */
  private void sleepPastWindow() throws Exception {
    SleepUtils.sleep(TimeUnit.MILLISECONDS, 5);
  }

  private void sleepPastScanWindow() throws Exception {
    SleepUtils.sleep(TimeUnit.MILLISECONDS, SCAN_WINDOW + 20);
  }

}
