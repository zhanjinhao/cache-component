package cn.addenda.component.cache.eviction.lru;

/**
 * LRU 实现中用于存储新旧关系的双向链表。
 * <p>
 * 约定：队头最旧、队尾最新；{@link #getFirst()} 返回的对象即为下一个淘汰对象。
 * <p>
 * {@link #addLast} 只负责将元素放入队尾，<b>不保证把已在队中的元素移动到队尾</b>，
 * 因此"命中后变为最新"必须写成 {@code remove(p)} 与 {@code addLast(p)} 两步。
 * 已在队中的元素具体如何处理由实现决定，见子类 {@link LinkedHashSetLruDeque}。
 *
 * @author addenda
 * @since 2023/05/30
 */
public interface LruDeque<P> {

  /**
   * @return 队头元素（下一个被淘汰的对象）；队空时返回 null
   */
  P getFirst();

  /**
   * 将元素放入队尾。
   * <p>
   * 元素已在队中时是否改变其位置由实现决定，见 {@link LinkedHashSetLruDeque}；
   * 需要"移动到队尾"时应先调用 {@link #remove(Object)}。
   */
  void addLast(P p);

  void remove(P p);

  boolean contains(P p);

  int size();

}
