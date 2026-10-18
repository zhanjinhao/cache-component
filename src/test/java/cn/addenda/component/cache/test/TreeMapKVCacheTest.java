package cn.addenda.component.cache.test;

import cn.addenda.component.cache.SortedMapKVCache;
import cn.addenda.component.cache.TreeMapKVCache;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * {@link TreeMapKVCache} 的行为测试。
 * <p>
 * 它自己只做两件事：自建底层 {@code TreeMap}、按给定的比较器排序。其余契约继承自抽象的
 * {@link SortedMapKVCache} —— 排序视图、空集取首尾返回 null、{@code keySet()} 是活视图、
 * {@code containsKey} 按"有没有值"判定，这些都一并用 {@link TreeMapKVCache} 这个真实入口来覆盖。
 * <p>
 * 只有两处例外走测试用子类（见文件末尾）：<b>构造器拒绝 null map</b>、以及
 * <b>"外部往底层 map 里塞 null 值"</b>这种状态 —— 后者只有把 map 交进来才构造得出，
 * 而 {@link TreeMapKVCache} 的 map 是它自己建的、外面拿不到。
 *
 * @author addenda
 */
public class TreeMapKVCacheTest {

  /**
   * {@code getFirst()} / {@code getLast()} / {@code keySet()} 都按 <b>key 的顺序</b>，
   * 与写入先后无关 —— 无参构造器用的是 key 的自然顺序。
   */
  @Test
  public void testOrderedViewFollowsKeyOrder() {
    TreeMapKVCache<String, String> cache = new TreeMapKVCache<>();

    cache.set("b", "2");
    cache.set("a", "1");
    cache.set("c", "3");

    Assert.assertEquals("首元素是最小的 key", "a", cache.getFirst());
    Assert.assertEquals("尾元素是最大的 key", "c", cache.getLast());
    Assert.assertEquals(new ArrayList<>(cache.keySet()), Arrays.asList("a", "b", "c"));
  }

  /**
   * <b>空集时 {@code getFirst()} / {@code getLast()} 返回 null，而不是抛异常。</b>
   * <p>
   * {@code TreeMap.firstKey()} 原生行为是抛 {@code NoSuchElementException}，
   * 照搬它会把"空集"变成一条必须靠异常处理的正常路径。{@code null} 在这里无歧义：
   * key 不允许为 null。
   */
  @Test
  public void testGetFirstAndGetLastReturnNullWhenEmpty() {
    TreeMapKVCache<String, String> cache = new TreeMapKVCache<>();

    Assert.assertNull("空集时首元素为 null", cache.getFirst());
    Assert.assertNull("空集时尾元素为 null", cache.getLast());

    cache.set("a", "1");
    Assert.assertEquals("a", cache.getFirst());
    Assert.assertEquals("a", cache.getLast());

    cache.delete("a");
    Assert.assertNull("清空之后又是 null", cache.getFirst());
    Assert.assertNull("清空之后又是 null", cache.getLast());
  }

  /**
   * 带比较器的构造器按给定顺序排列 —— 这正是底层 {@code TreeMap} 的能力。
   * 本类自建 map 之后，调用方无法再像用父类那样自己传一个排好序的 map，所以必须留这个入口。
   */
  @Test
  public void testComparatorOverridesNaturalOrder() {
    TreeMapKVCache<String, String> cache = new TreeMapKVCache<>(Comparator.reverseOrder());

    cache.set("a", "1");
    cache.set("c", "3");
    cache.set("b", "2");

    Assert.assertEquals("倒序时首元素是最大的 key", "c", cache.getFirst());
    Assert.assertEquals("尾元素是最小的 key", "a", cache.getLast());
    Assert.assertEquals(new ArrayList<>(cache.keySet()), Arrays.asList("c", "b", "a"));
  }

  /**
   * <b>{@code keySet()} 返回的是底层表的活视图，不是快照</b>：调用方能通过它改到缓存本身。
   * <p>
   * 这不是缺陷而是取向 —— 包装的是哪份 map 就暴露哪份。钉住它是为了让这个取向是<b>有意的</b>：
   * 哪天要在这条链上加防护，本用例会红。
   * <p>
   * 顺带钉住"底层 map 归本实例所有"：活视图改到的只有本对象，另一个实例不受影响
   * （对照 {@link SortedMapKVCache}：那条路径下 map 由调用方交进来，改谁都行）。
   */
  @Test
  public void testKeySetIsLiveViewOfOwnMap() {
    TreeMapKVCache<String, String> cache = new TreeMapKVCache<>();
    cache.set("a", "1");
    cache.set("b", "2");
    Assert.assertEquals(2, cache.keySet().size());

    Set<String> keys = cache.keySet();
    Assert.assertTrue("通过 keySet 删得掉", keys.remove("a"));
    Assert.assertEquals("活视图：改动落到了缓存上", 1, cache.size());
    Assert.assertNull("缓存里真的没了", cache.get("a"));
    Assert.assertFalse("查也查不到", cache.containsKey("a"));

    TreeMapKVCache<String, String> other = new TreeMapKVCache<>();
    other.set("a", "1");
    Assert.assertEquals("两个实例各持一份 map", "1", other.get("a"));
  }

  /**
   * <b>构造器拒绝 null 的 SortedMap</b>：构造期快速失败，而不是等到第一次调用才 NPE。
   */
  @Test
  public void testNullSortedMapRejectedAtConstruction() {
    CacheAssertions.assertRejected("传 null 构造",
            () -> new TestSortedMapKVCache<String, String>(null));
  }

  /**
   * <b>{@code containsKey} / {@code get} / {@code delete} 三者口径一致</b>，都按"有没有值"。
   * <p>
   * 差别只在"外部绕过本类、直接往 map 里塞了一个 null 值"时显现：裸的 {@code map.containsKey}
   * 会说"在"，而 {@code get} 返回 null、{@code delete} 返回 false —— 三个方法各说各话。
   * 父类的 {@code containsKey} 因此写成 {@code get != null}。
   */
  @SuppressWarnings("ConstantConditions")
  @Test
  public void testContainsKeyGetDeleteAgreeWhenValueIsExternallyNull() {
    TreeMap<String, String> raw = new TreeMap<>();
    SortedMapKVCache<String, String> cache = new TestSortedMapKVCache<>(raw);

    raw.put("k", null);

    Assert.assertFalse("containsKey 说没有（按值判定）", cache.containsKey("k"));
    Assert.assertNull("get 也说没有", cache.get("k"));
    Assert.assertFalse("delete 也说没删掉", cache.delete("k"));
  }

  /**
   * 仅供测试：{@link SortedMapKVCache} 的构造器是 {@code protected} 的，开个口子，
   * 用来构造上面那两个 {@link TreeMapKVCache} 构造不出来的状态。
   */
  private static class TestSortedMapKVCache<K, V> extends SortedMapKVCache<K, V> {

    private TestSortedMapKVCache(SortedMap<K, V> sortedMap) {
      super(sortedMap);
    }

  }

}
