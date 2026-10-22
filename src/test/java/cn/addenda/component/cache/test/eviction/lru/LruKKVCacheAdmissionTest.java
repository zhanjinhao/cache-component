package cn.addenda.component.cache.test.eviction.lru;

import cn.addenda.component.cache.eviction.lru.LruKHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruKKVCache;
import cn.addenda.component.cache.test.CacheAssertions;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link LruKKVCache} 的<b>准入规则</b>测试。
 * <p>
 * 计数规则：
 * <ul>
 *   <li><b>建档</b>：第一次读或第一次写都会建档，记 1 分；</li>
 *   <li><b>累加</b>：之后每次读 {@code +1}，<b>写不加分</b>；</li>
 *   <li><b>准入</b>：每次写的时候看档案值，{@code >= threshold} 才真正落地。</li>
 * </ul>
 * 这一组用例盯的是几个容易漏掉的点：
 * <ul>
 *   <li><b>准入判定只发生在写里</b> —— 光读够 {@code threshold} 次不会自己进来，
 *       要有一次带值的机会才落地；</li>
 *   <li><b>写不累加</b> —— 反复写同一个 key 刷不出分数；</li>
 *   <li><b>计数档案自己也会被淘汰</b> —— {@code visitorCount} 是个容量 {@code capacity * 2}
 *       的 LRU，扫的 key 太多会把先前的档案挤掉，那个 key 的进度就归零了。</li>
 * </ul>
 *
 * @author addenda
 */
public class LruKKVCacheAdmissionTest {

  /**
   * 给得比较宽：{@code visitorCount} 的容量是它的两倍，
   * 这样"扫 100 个 key"不会撞上计数档案自身的淘汰，干扰要验的东西。
   */
  private static final int CAPACITY = 100;

  private LruKKVCache<String, String> cache(int threshold) {
    return new LruKHashMapKVCache<>(CAPACITY, threshold);
  }

  private LruKKVCache<String, String> tinyCache(int capacity, int threshold) {
    return new LruKHashMapKVCache<>(capacity, threshold);
  }

  // ------------------------------------------------------------------
  // 建档：读、写都能建
  // ------------------------------------------------------------------

  /**
   * <b>第一次读就建档</b>：读两次攒到 2 分，这时一次写就落地。
   */
  @Test
  public void testFirstReadCreatesArchive() {
    LruKKVCache<String, String> c = cache(2);

    Assert.assertNull(c.get("a"));      // 建档，1 分
    Assert.assertNull(c.get("a"));      // 2 分
    Assert.assertFalse("攒够分本身不会让它进来", c.containsKey("a"));

    c.set("a", "v");                    // 写的时候判定，2 >= 2
    Assert.assertTrue("这一次写就落地", c.containsKey("a"));
  }

  /**
   * <b>第一次写也建档</b>：写一次是 1 分，再读一次到 2 分，第二次写才落地。
   */
  @Test
  public void testFirstWriteCreatesArchive() {
    LruKKVCache<String, String> c = cache(2);

    c.set("a", "v");                    // 建档，1 分
    Assert.assertFalse("只有 1 分，进不去", c.containsKey("a"));

    c.get("a");                         // 2 分
    c.set("a", "v");
    Assert.assertTrue("第二次写落地", c.containsKey("a"));
  }

  /**
   * 建档之后每次读都 {@code +1}。threshold=100 时精确地卡在 99 / 100 上。
   */
  @Test
  public void testReadsAccumulate() {
    LruKKVCache<String, String> c = cache(100);

    for (int i = 0; i < 99; i++) {
      c.get("a");
    }
    c.set("a", "v");                    // 99 分，还差 1
    Assert.assertFalse("还差一分，不落地", c.containsKey("a"));

    c.get("a");                         // 100 分
    c.set("a", "v");
    Assert.assertTrue("攒到 100 分，准入", c.containsKey("a"));
  }

  /**
   * <b>写不加分</b>：写 100 次也只有建档那 1 分。
   * <p>
   * 要是写也累加，调用方（比如正在回源的报表）多写几次就能自己把分刷够，
   * 准入闸门就形同虚设了。
   */
  @Test
  public void testWriteDoesNotAccumulate() {
    LruKKVCache<String, String> c = cache(2);

    for (int i = 0; i < 100; i++) {
      c.set("a", "v" + i);
    }

    Assert.assertFalse("写 100 次也只有建档那 1 分", c.containsKey("a"));
  }

  // ------------------------------------------------------------------
  // 落地只发生在写里
  // ------------------------------------------------------------------

  /**
   * <b>攒够分不会自己进来，要有一次写才落地。</b>
   * <p>
   * 扫描读 3 次把分攒到 3，但直到写的那一刻才真正占坑。
   */
  @Test
  public void testReadsAccumulateButNeedAWriteToLand() {
    LruKKVCache<String, String> c = cache(2);

    for (int i = 0; i < 100; i++) {
      for (int r = 0; r < 3; r++) {
        c.get("r" + i);
      }
    }

    Assert.assertFalse("光读不写，什么都不落地", c.containsKey("r0"));
    Assert.assertEquals("主缓存一直是空的", 0, c.size());

    c.set("r0", "v");
    Assert.assertTrue("分早就够 3 了，这一次写直接落地", c.containsKey("r0"));
  }

  /**
   * cache-aside（读未命中 → 回源 → 写回）每轮恰好贡献"一次写 + 一次读"，
   * 所以是 {@code threshold} 轮进缓存。
   */
  @Test
  public void testCacheAsideTakesThresholdRounds() {
    for (int threshold = 1; threshold <= 3; threshold++) {
      LruKKVCache<String, String> c = cache(threshold);

      for (int round = 1; round < threshold; round++) {
        c.get("a");
        c.set("a", "v");
        Assert.assertFalse("threshold=" + threshold + "：第 " + round + " 轮还不该进",
                c.containsKey("a"));
      }

      c.get("a");
      c.set("a", "v");
      Assert.assertTrue("threshold=" + threshold + "：第 " + threshold + " 轮该进",
              c.containsKey("a"));
    }
  }

  // ------------------------------------------------------------------
  // 计数档案自己也会被淘汰
  // ------------------------------------------------------------------

  /**
   * {@code visitorCount} 是个容量 {@code capacity * 2} 的 LRU。
   * 短时间扫过太多 key，会把先前 key 的档案挤出去 —— 那个 key 攒到一半的进度
   * <b>就归零了</b>，得从头再攒。
   * <p>
   * 这是"访问历史不无限增长"的代价，用之前得知道。
   */
  @Test
  public void testCounterArchivesAreEvictedToo() {
    // visitorCount 容量 = 2 * 2 = 4
    LruKKVCache<String, String> c = tinyCache(2, 2);

    c.get("a");                         // a 建档，1 分

    // 再扫 5 个 key，把 a 的档案挤出去
    for (int i = 0; i < 5; i++) {
      c.get("x" + i);
    }

    c.get("a");                         // 档案没了，这次是重新建档，不是 +1
    c.set("a", "v");

    Assert.assertFalse("档案被挤掉之后进度归零，只重新攒到 1 分",
            c.containsKey("a"));
  }

  // ------------------------------------------------------------------
  // setnx：明确不支持
  // ------------------------------------------------------------------

  /**
   * <b>{@code setnx} 在这个实现上不支持，一律抛 {@code CacheUnsupportedOperationException}。</b>
   * <p>
   * 它和"先攒够 {@code threshold} 次访问才落地"这套准入语义对不上：
   * <ul>
   *   <li>门槛 >= 2 时本来会恒返回 false（key 一旦真在缓存里，入口的
   *       {@code containsKey} 就先返回 false 了），调用方分不清是"已存在"
   *       还是"没落地"，拿它做互斥会永远抢不到还不报错；</li>
   *   <li>门槛 = 1 时倒是能用，但那是"等价于普通 LRU"的特殊形态，
   *       不该让同一个方法的行为随调优参数在"可用 / 抛异常"之间跳。</li>
   * </ul>
   * 要"写过就进"的语义请改用 {@code LruKVCache}。
   */
  @Test
  public void testSetnxIsUnsupported() {
    for (int threshold = 1; threshold <= 3; threshold++) {
      LruKKVCache<String, String> c = cache(threshold);
      int t = threshold;
      CacheAssertions.assertNotSupported("threshold=" + t + " 时的 setnx",
              () -> c.setnx("a", "v"));
    }

    // 无论 key 在不在缓存里，都抛
    LruKKVCache<String, String> c = cache(1);
    c.set("a", "v");
    CacheAssertions.assertNotSupported("key 已存在时也抛", () -> c.setnx("a", "v2"));
    Assert.assertEquals("不该被覆盖", "v", c.get("a"));
  }

}
