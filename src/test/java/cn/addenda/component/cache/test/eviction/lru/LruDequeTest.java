package cn.addenda.component.cache.test.eviction.lru;

import cn.addenda.component.cache.eviction.lru.LinkedHashSetLruDeque;
import cn.addenda.component.cache.eviction.lru.LruDeque;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * {@link LruDeque} 的行为测试（用当前唯一实现 {@link LinkedHashSetLruDeque} 跑契约）。
 * <p/>
 * 重点是接口文档里那条最容易踩的约定：{@link LruDeque#addLast} 对<b>已经在队里</b>的元素不挪位置，
 * 所以"命中后变最新"必须写成 {@code remove} + {@code addLast} 两步 —— 只调 {@code addLast}
 * 会让命中的元素永远卡在队头等着被淘汰。
 *
 * @author addenda
 */
public class LruDequeTest {

  private LruDeque<String> deque;

  @Before
  public void before() {
    deque = new LinkedHashSetLruDeque<>();
  }

  @Test
  public void testEmpty() {
    Assert.assertNull("空队列 getFirst 返回 null", deque.getFirst());
    Assert.assertEquals(0L, deque.size());
    Assert.assertFalse(deque.contains("a"));
  }

  /**
   * 队头是最早进来的（下一个被淘汰的）。
   */
  @Test
  public void testGetFirstIsOldest() {
    deque.addLast("a");
    deque.addLast("b");
    deque.addLast("c");

    Assert.assertEquals(3L, deque.size());
    Assert.assertEquals("队头是最早进来的", "a", deque.getFirst());
  }

  /**
   * 删掉队头之后，队头要顺延
   */
  @Test
  public void testRemoveShiftsFirst() {
    deque.addLast("a");
    deque.addLast("b");
    deque.addLast("c");

    deque.remove("a");
    Assert.assertFalse(deque.contains("a"));
    Assert.assertEquals(2L, deque.size());
    Assert.assertEquals("b", deque.getFirst());

    deque.remove("b");
    Assert.assertEquals("c", deque.getFirst());

    deque.remove("c");
    Assert.assertEquals(0L, deque.size());
    Assert.assertNull(deque.getFirst());
  }

  /**
   * 删不存在的元素应该是空操作，不能抛异常，也不能影响队列。
   */
  @Test
  public void testRemoveAbsentElement() {
    deque.addLast("a");

    deque.remove("not-exists");

    Assert.assertEquals("删不存在的元素是空操作", 1L, deque.size());
    Assert.assertEquals("a", deque.getFirst());
  }

  /**
   * 关键契约：{@code addLast} 对已存在的元素<b>不挪位置</b>（底层 LinkedHashSet.add 返回 false）。
   */
  @Test
  public void testAddLastExistingElementDoesNotMoveIt() {
    deque.addLast("a");
    deque.addLast("b");

    deque.addLast("a");

    Assert.assertEquals("重复添加不增加 size", 2L, deque.size());
    Assert.assertEquals("addLast 不挪位置，a 仍卡在队头", "a", deque.getFirst());
  }

  /**
   * "命中后变最新"的正确写法：remove + addLast。写成两步之后队头才会顺延。
   */
  @Test
  public void testRemoveThenAddLastMakesItMostRecent() {
    deque.addLast("a");
    deque.addLast("b");

    deque.remove("a");
    deque.addLast("a");

    Assert.assertEquals(2L, deque.size());
    Assert.assertEquals("remove + addLast 后 a 变队尾，队头轮到 b", "b", deque.getFirst());

    deque.remove("b");
    Assert.assertEquals("a", deque.getFirst());
  }

  /**
   * 元素身份按 equals/hashCode 判定（集合语义），不是引用相等 —— 缓存里用等值的 key 对象也认。
   */
  @Test
  public void testElementIdentityIsEqualsBased() {
    deque.addLast("a");
    deque.addLast(new String("a"));

    Assert.assertEquals("等值对象算同一个元素", 1L, deque.size());
    Assert.assertTrue(deque.contains(new String("a")));
    Assert.assertEquals("a", deque.getFirst());
  }

  @Test
  public void testContainsAndSize() {
    Assert.assertEquals(0L, deque.size());

    deque.addLast("a");
    deque.addLast("b");
    Assert.assertTrue(deque.contains("a"));
    Assert.assertTrue(deque.contains("b"));
    Assert.assertFalse(deque.contains("c"));
    Assert.assertEquals(2L, deque.size());

    deque.remove("b");
    Assert.assertFalse(deque.contains("b"));
    Assert.assertEquals(1L, deque.size());
  }

}
