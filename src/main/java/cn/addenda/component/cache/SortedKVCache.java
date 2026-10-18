package cn.addenda.component.cache;

import java.util.Set;

/**
 * key 有序的 {@link KVCache}：在 {@link KVCache} 之上增加按下标取首尾 key 与遍历 keySet 的能力。
 * <p>
 * 排序由底层实现决定（参考子类 {@link TreeMapKVCache}）。{@link #getFirst()} /
 * {@link #getLast()} 在空集时返回 {@code null} —— 不是异常，{@code null} 只可能表示"没有"
 * （key 不允许为 null）。{@link #keySet()} 是否可写、是否实时，同样由底层实现决定。
 *
 * @author addenda
 * @since 2023/05/30
 */
public interface SortedKVCache<K, V> extends KVCache<K, V> {

  /**
   * 排序最靠前的 key。
   * <p>
   * <b>空集时返回 {@code null}</b> —— 不是异常：key 不允许为 null，所以 {@code null} 只可能
   * 表示"没有"。与 {@code LruDeque#getFirst()} 的空集语义保持一致。
   */
  K getFirst();

  /**
   * 排序最靠后的 key；空集时返回 {@code null}，见 {@link #getFirst()}。
   */
  K getLast();

  /**
   * 全部 key 的视图。是否可写、是否实时，由实现决定（见各实现类说明）。
   */
  Set<K> keySet();

}
