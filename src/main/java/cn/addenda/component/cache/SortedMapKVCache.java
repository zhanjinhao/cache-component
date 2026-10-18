package cn.addenda.component.cache;

import java.util.Set;
import java.util.SortedMap;

/**
 * {@link SortedKVCache} 的抽象实现：把全部操作转发给一个 {@link SortedMap}。
 * <p>
 * 不限制条数，{@link #capacity()} 返回 {@link KVCache#UNLIMITED_CAPACITY} 哨兵；
 * {@link #containsKey(Object)} 按"有没有值"判定（{@code get != null}）而非裸的
 * {@code containsKey}。底层用哪种 map、是否由自己创建，由子类决定，参考子类
 * {@link TreeMapKVCache}。
 * <p>
 * <b>安全级别</b>：无。底层 {@link SortedMap} 本身不线程安全，连 L1 都不承诺。并发访问时：
 * 要同时保住 {@link #getFirst()} / {@link #getLast()} / {@link #keySet()}，用
 * {@link SynchronizedSortedKVCache}；只当成 {@link KVCache} 用的话包一层
 * {@link SynchronizedKVCache}，但会丢掉那三个方法。
 * <p>
 * <b>保护不到的地方</b>：子类把外部传入的 map 交给本类时，本类只持有它的<b>引用</b>、不作拷贝，
 * 因此构造之后<b>调用方不得再直接访问那个 SortedMap</b> —— 绕过本类的写入不享受任何保护，
 * 且不会有任何报错。
 *
 * @author addenda
 * @since 2023/05/30
 */
public abstract class SortedMapKVCache<K, V> implements SortedKVCache<K, V> {

  private final SortedMap<K, V> sortedMap;

  /**
   * @param sortedMap 底层存储；由外部传入时<b>构造之后不得再被外部直接访问</b>，见类注释
   */
  protected SortedMapKVCache(SortedMap<K, V> sortedMap) {
    if (sortedMap == null) {
      throw new IllegalArgumentException("sortedMap 不允许为 null!");
    }
    this.sortedMap = sortedMap;
  }

  @Override
  public void set(K k, V v) {
    KVCache.assertKeyNotNull(k);
    // 见 KVCache#set：value 为 null 时一律按删除处理
    if (v == null) {
      sortedMap.remove(k);
      return;
    }
    sortedMap.put(k, v);
  }

  @Override
  public boolean setnx(K k, V v) {
    KVCache.assertKeyNotNull(k);
    // 见 KVCache#setnx：value 为 null 时表示"没有值"，本方法不写入任何内容
    if (v == null) {
      return false;
    }
    return sortedMap.putIfAbsent(k, v) == null;
  }

  /**
   * 按"有没有值"判定，而不是裸的 {@code sortedMap.containsKey(k)}。
   * <p>
   * 后者会与 {@link #get(Object)} / {@link #delete(Object)} 打架：外部直接往 map 里塞一个
   * null 值时，{@code containsKey} 说"在"、{@code get} 返回 null、{@code delete} 返回 false。
   * 改成 {@code get != null} 后三个方法口径一致，也与过期的内存实现一致。
   */
  @Override
  public boolean containsKey(K k) {
    KVCache.assertKeyNotNull(k);
    return sortedMap.get(k) != null;
  }

  @Override
  public V get(K k) {
    KVCache.assertKeyNotNull(k);
    return sortedMap.get(k);
  }

  @Override
  public boolean delete(K k) {
    KVCache.assertKeyNotNull(k);
    return sortedMap.remove(k) != null;
  }

  @Override
  public int size() {
    return sortedMap.size();
  }

  /**
   * @return {@link KVCache#UNLIMITED_CAPACITY} —— 本实现不限制条数，哨兵值，
   *         不是真实存在的容量约束。
   */
  @Override
  public int capacity() {
    return UNLIMITED_CAPACITY;
  }

  /**
   * @return 最小的 key；空集时返回 {@code null}，见 {@link SortedKVCache#getFirst()}。
   *         <b>不</b>直接返回 {@code sortedMap.firstKey()} —— 那个在空集上抛
   *         {@code NoSuchElementException}，会把"空集"变成一条必须靠异常处理的正常路径。
   */
  @Override
  public K getFirst() {
    return sortedMap.isEmpty() ? null : sortedMap.firstKey();
  }

  /**
   * @return 最大的 key；空集时返回 {@code null}，见 {@link SortedKVCache#getFirst()}。
   */
  @Override
  public K getLast() {
    return sortedMap.isEmpty() ? null : sortedMap.lastKey();
  }

  /**
   * @return 底层 {@code sortedMap} 的 {@code keySet()} —— <b>活视图</b>，随本 cache 的写入
   *         实时变化；是否可写由底层 map 决定。详见 {@link SortedKVCache#keySet()}。
   */
  @Override
  public Set<K> keySet() {
    return sortedMap.keySet();
  }

}
