package cn.addenda.component.cache.eviction.lru;

import java.util.LinkedHashSet;

/**
 * 基于 {@link LinkedHashSet} 实现的 {@link LruDeque}。
 * <p>
 * <b>继承的规则</b>：全部来自 {@link LruDeque}（队头最旧、队尾最新，
 * {@link #getFirst()} 返回下一个淘汰对象，{@link #addLast} 只负责放入队尾）。
 * <p>
 * <b>自身差异</b>：借助 {@code LinkedHashSet} 的插入顺序，迭代器从最早的插入位置开始，
 * 因此 {@link #getFirst()} 返回最早插入的元素，天然对应队头；未构建真正的链表。
 * <p>
 * 注意 {@link #addLast} 内部直接调用 {@code LinkedHashSet.add}，
 * <b>元素已存在时不会将其移动到末尾</b>（返回 false，位置保持不变）。
 *
 * @author addenda
 * @since 2023/05/30
 */
public class LinkedHashSetLruDeque<P> implements LruDeque<P> {

  private final LinkedHashSet<P> linkedHashSet = new LinkedHashSet<>();

  @Override
  public P getFirst() {
    if (linkedHashSet.isEmpty()) {
      return null;
    }
    return linkedHashSet.iterator().next();
  }

  /**
   * 放入队尾。<b>元素已存在时位置保持不变</b>（{@code LinkedHashSet.add} 的行为）。
   */
  @Override
  public void addLast(P p) {
    linkedHashSet.add(p);
  }

  @Override
  public void remove(P p) {
    linkedHashSet.remove(p);
  }

  @Override
  public boolean contains(P p) {
    return linkedHashSet.contains(p);
  }

  @Override
  public int size() {
    return linkedHashSet.size();
  }

}
