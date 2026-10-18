package cn.addenda.component.cache;

import java.util.Comparator;
import java.util.TreeMap;

/**
 * 用 {@link TreeMap} 作为底层存储的 {@link SortedKVCache}。
 * <p>
 * <b>继承的规则</b>：{@link SortedMapKVCache} 的全部约定 —— 排序视图、{@link #keySet()} 的
 * 活视图语义、{@link #containsKey(Object)} 按"有没有值"判定、不限制条数，以及安全级别。
 * <p>
 * <b>自身差异</b>：只提供底层 map —— 由本类自己创建 {@link TreeMap}，外部拿不到它，
 * 因此不存在 {@link SortedMapKVCache} 类注释里那条"调用方绕过本类去写底层 map"的问题。
 *
 * @author addenda
 */
public class TreeMapKVCache<K, V> extends SortedMapKVCache<K, V> {

  /**
   * 按 key 的<b>自然顺序</b>排列，要求 key 实现 {@link Comparable}。
   */
  public TreeMapKVCache() {
    super(new TreeMap<>());
  }

  /**
   * 按传入的比较器排列。
   *
   * @param comparator key 的比较器，不允许为 null
   */
  public TreeMapKVCache(Comparator<? super K> comparator) {
    super(new TreeMap<>(comparator));
  }

}
