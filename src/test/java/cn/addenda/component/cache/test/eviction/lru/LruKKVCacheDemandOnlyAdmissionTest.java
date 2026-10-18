package cn.addenda.component.cache.test.eviction.lru;

import cn.addenda.component.cache.HashMapKVCache;
import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.eviction.lru.LruKHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruKKVCache;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link LruKKVCache} 在 {@code countFirstWriteAsVisit = false} 时的准入口径：
 * <b>准入凭证只由未命中的读签发，写只负责在拿到凭证后落地。</b>
 * <p>
 * 默认值（{@code true}）的"第一次写算一次引用、{@code threshold = 1} 时等价于普通 LRU"
 * 由 {@link LruKKVCacheAdmissionTest} 与 {@link LruKKVCacheTest} 覆盖；这里只钉关闭开关后的行为。
 *
 * @author addenda
 */
public class LruKKVCacheDemandOnlyAdmissionTest {

  private static final int CAPACITY = 3;

  private LruKKVCache<String, String> newCache(int threshold) {
    return new LruKHashMapKVCache<>(CAPACITY, threshold, false);
  }

  /**
   * <b>只写不读：写多少次都不会落地</b>，{@code threshold = 1} 也不例外 ——
   * 写不签发凭证。
   * <p>
   * 用 {@code containsKey} 检查而不是 {@code get}：{@code get} 自己会签发凭证。
   */
  @Test
  public void testWritesNeverAdmitWithoutAMiss() {
    LruKKVCache<String, String> c = newCache(1);

    for (int i = 0; i < 10; i++) {
      c.set("a", "1");
    }

    Assert.assertFalse("只写不读，threshold=1 也不该落地", c.containsKey("a"));
    Assert.assertEquals("数据层不该有东西", 0, c.size());
  }

  /**
   * 未命中的读签够凭证之后，写才会落地：{@code threshold = 2} 需要两次未命中读。
   */
  @Test
  public void testMissReadsIssueTheTicketThenWriteLands() {
    LruKKVCache<String, String> c = newCache(2);

    Assert.assertNull("未命中读：计数 1", c.get("a"));
    c.set("a", "1");
    Assert.assertFalse("1 < 2，不落地", c.containsKey("a"));

    Assert.assertNull("未命中读：计数 2", c.get("a"));
    c.set("a", "1");
    Assert.assertEquals("2 >= 2，落地", "1", c.get("a"));
  }

  /**
   * 命中不建档（与开关无关的一致性）：准入之后怎么读，账本都是空的。
   * <p>
   * 直接注入计数表来观察账本，避免只靠"是否落地"间接推断。
   */
  @Test
  public void testHitDoesNotCreateRecord() {
    HashMapKVCache<String, Integer> ledger = new HashMapKVCache<>();
    LruKKVCache<String, String> c = new LruKOverLedger<>(CAPACITY, 2, false,
            ledger, new HashMapKVCache<>());

    c.get("a");                      // 计数 1
    c.get("a");                      // 计数 2
    c.set("a", "1");                 // 落地并删除计数
    Assert.assertEquals("1", c.get("a"));
    Assert.assertFalse("准入之后计数记录已删除", ledger.containsKey("a"));

    c.get("a");
    c.get("a");
    Assert.assertEquals("命中不建档", 0, ledger.size());
  }

  /**
   * 开关本身：同一串操作在默认值下"写过就进"，关闭后必须先用未命中读换凭证。
   */
  @Test
  public void testSwitchDecidesWhetherWriteThroughWorks() {
    LruKKVCache<String, String> writeThrough = new LruKHashMapKVCache<>(CAPACITY, 1);
    writeThrough.set("a", "1");
    Assert.assertEquals("默认开关：写过就进", "1", writeThrough.get("a"));

    LruKKVCache<String, String> demandOnly = newCache(1);
    demandOnly.set("a", "1");
    Assert.assertFalse("关闭开关：只写不落地", demandOnly.containsKey("a"));

    Assert.assertNull("未命中读签发凭证（计数 1）", demandOnly.get("a"));
    demandOnly.set("a", "1");
    Assert.assertEquals("拿到凭证后写才落地", "1", demandOnly.get("a"));
  }

  /**
   * 仅供测试：注入计数表（用来直接观察账本），并关闭"第一次写建档"。
   */
  private static class LruKOverLedger<K, V> extends LruKKVCache<K, V> {
    private LruKOverLedger(int capacity, int threshold, boolean countFirstWriteAsVisit,
                           KVCache<K, Integer> visitorCount, KVCache<K, V> kvCacheDelegate) {
      super(capacity, threshold, countFirstWriteAsVisit, visitorCount, kvCacheDelegate);
    }
  }

}
