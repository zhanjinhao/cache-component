package cn.addenda.component.cache.test;

import cn.addenda.component.base.util.SleepUtils;
import cn.addenda.component.cache.ExpiredConcurrentHashMapKVCache;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link ExpiredConcurrentHashMapKVCache} 对 value 类型的要求。
 * <p>
 * 结论见该类的注释：{@code K} 必须正确实现 {@code equals} / {@code hashCode}（它是 map 的 key），
 * 而 {@code V} 不需要 —— 这两个方法<b>一次都不会被调用</b>：判定与清除都落在 K 的哈希桶上、
 * 按引用完成，没有一处比较过值的内容。
 * <p>
 * 这里用一个"两个都没重写"的值类型把这条钉住：一旦哪天实现改成依赖值的比较
 * （例如按值查找再删除），本文件会变红。
 *
 * @author addenda
 */
public class ExpiredConcurrentHashMapKVCacheTest {

  /**
   * 刻意不重写 {@code equals} / {@code hashCode} / {@code toString}，用 Object 的引用相等。
   */
  private static class PlainValue {

    private final String s;

    private PlainValue(String s) {
      this.s = s;
    }

  }

  @Test
  public void testValueTypeNeedsNoEqualsOrHashCode() throws Exception {
    ExpiredConcurrentHashMapKVCache<String, PlainValue> cache = new ExpiredConcurrentHashMapKVCache<>();
    PlainValue v1 = new PlainValue("v1");

    cache.set("k", v1);
    Assert.assertSame("存进去什么引用，读出来就是什么引用", v1, cache.get("k"));
    Assert.assertTrue(cache.containsKey("k"));
    Assert.assertFalse("已存在时 setnx 不该写入", cache.setnx("k", new PlainValue("v2")));
    Assert.assertSame("setnx 失败不该动原值", v1, cache.get("k"));

    // 过期 + 清除：get 的 compute 一步完成判定与清除，全程不碰 V 的 equals / hashCode
    cache.set("expired", new PlainValue("old"), 1, TimeUnit.SECONDS);
    SleepUtils.sleep(TimeUnit.MILLISECONDS, 1200);
    Assert.assertEquals("尚未被碰过：size() 按原始条数算，仍把过期的这一条计入",
            2, cache.size());
    Assert.assertNull("过期之后读不到，并顺手清掉这一条", cache.get("expired"));
    Assert.assertEquals("被 get 碰到之后才清掉，size() 少一条", 1, cache.size());
    Assert.assertFalse(cache.containsKey("expired"));

    // 过期条目等同于不存在：判定看的是到期时刻，不是值
    PlainValue v3 = new PlainValue("v3");
    Assert.assertTrue("过期条目不该把 setnx 挡在门外", cache.setnx("expired", v3));
    Assert.assertSame(v3, cache.get("expired"));

    Assert.assertSame("remove 返回被删掉的那个引用", v3, cache.remove("expired"));
    Assert.assertNull(cache.get("expired"));

    cache.delete("k");
    Assert.assertEquals("清干净之后条数为 0", 0, cache.size());
  }

  /**
   * <b>「已过期」对 computeIfAbsent 而言同样是「不存在」</b>：命中过期条目要<b>重新计算</b>，
   * 并用新值把它替换掉 —— 既不是原样返回过期值，也不是把过期条目留在 map 里。
   * <p>
   * 两个重载都是如此，{@code ExpiredHashMapKVCache} 的行为与之一致。
   * <p>
   * 这条测不到四个基于 redis 的实现：它们直接抛 {@link UnsupportedOperationException}。
   * 各实现在过期条目上的完整口径（含 {@code containsKey} / {@code remove} /
   * 不带 TTL 的 {@code setnx} 这三点）由
   * {@code ExpiredKVCacheContractTest#testExpiredEntrySemanticsAcrossImpls} 统一钉住 ——
   * 本类与 {@code ExpiredHashMapKVCache} 在这三点上已经对齐，不要再按"两者相反"理解。
   */
  @Test
  public void testComputeIfAbsentRecomputesWhenExpired() throws Exception {
    ExpiredConcurrentHashMapKVCache<String, String> cache = new ExpiredConcurrentHashMapKVCache<>();
    AtomicInteger invoked = new AtomicInteger();

    cache.set("plain", "old", 1, TimeUnit.SECONDS);
    cache.set("ttl", "old", 1, TimeUnit.SECONDS);
    SleepUtils.sleep(TimeUnit.MILLISECONDS, 1200);

    // 不带 TTL 的重载：重算，写进去的值永不过期
    Assert.assertEquals("过期条目等同于不存在，应该重算",
            "computed-1", cache.computeIfAbsent("plain", k -> "computed-" + invoked.incrementAndGet()));
    Assert.assertEquals("新值要真的写进去", "computed-1", cache.get("plain"));

    // 带 TTL 的重载：同样重算
    Assert.assertEquals("带 TTL 的重载同样重算",
            "computed-2", cache.computeIfAbsent("ttl", k -> "computed-" + invoked.incrementAndGet(),
                    1, TimeUnit.MINUTES));
    Assert.assertEquals("新值要真的写进去", "computed-2", cache.get("ttl"));

    Assert.assertEquals("两个 key 各算一次", 2, invoked.get());

    // 命中未过期的值时不计算
    Assert.assertEquals("命中就不该再算",
            "computed-1", cache.computeIfAbsent("plain", k -> "computed-" + invoked.incrementAndGet()));
    Assert.assertEquals("仍然只算过两次", 2, invoked.get());
  }

}
