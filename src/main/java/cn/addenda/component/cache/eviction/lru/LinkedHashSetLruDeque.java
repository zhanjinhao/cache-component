package cn.addenda.component.cache.eviction.lru;

import java.util.LinkedHashSet;

/**
 * 基于 {@link LinkedHashSet} 实现的 {@link LruDeque}。
 * <p/>
 * 借助 {@code LinkedHashSet} 的插入顺序：迭代器从最早的插入位置开始遍历，
 * 因此 {@link #getFirst()} 返回的是最早插入的元素，天然对应队头。
 * 本实现未构建真正的链表，仅包含 {@code add} 与 {@code remove} 两个 O(1) 操作。
 * <p/>
 * 注意 {@link #addLast} 的语义：内部直接调用 {@code LinkedHashSet.add}，
 * <b>元素已存在时不会将其移动到末尾</b>（返回 false，位置保持不变）。
 * {@link LruKVCache} 中"命中后变为最新"一律写成 remove 与 addLast 两步，即出于此原因。
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
  public long size() {
    return linkedHashSet.size();
  }

}
