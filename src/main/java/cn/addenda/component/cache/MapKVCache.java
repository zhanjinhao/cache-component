package cn.addenda.component.cache;

import java.util.Map;

/**
 * 用 {@link Map} 承载数据的 {@link KVCache} 抽象：把 KVCache 的约定翻译成对 Map 的操作。
 * <p>
 * <b>自身规则</b>：
 * <ul>
 *   <li>value 为 null 一律按删除处理（{@link #set(Object, Object)}）；{@link #setnx} 在 v 为 null
 *       时不写入并返回 false；</li>
 *   <li>不限制条数，{@link #capacity()} 返回 {@link KVCache#UNLIMITED_CAPACITY} 哨兵，
 *       它不是真实的容量约束；</li>
 *   <li>{@link #remove(Object)} 覆盖成一次 {@code map.remove} —— 接口默认实现是 get + delete
 *       两步，并发下会删掉别人刚写进来的值；</li>
 *   <li><b>{@code computeIfAbsent} 不覆盖</b>，沿用接口默认实现：对普通 Map 它本来就是
 *       check-then-act，<b>不承诺 L2</b>。需要原子的实现见 {@link ConcurrentMapKVCache}。</li>
 * </ul>
 * 底层 Map 由子类在构造时指定，例如 {@link HashMapKVCache} 传入 {@code HashMap}。
 * 需要 TTL 的实现见 {@link ExpiredMapKVCache}。
 * <p>
 * <b>安全级别</b>：由传入的 Map 决定，本类不做声明 —— 传入非线程安全的 Map 时连 L1 都没有，
 * 并发访问用 {@link SynchronizedKVCache} 包装，见 {@link HashMapKVCache}。
 *
 * @author addenda
 * @since 2026/10/17
 */
public abstract class MapKVCache<K, V> implements KVCache<K, V> {

  private final Map<K, V> map;

  protected MapKVCache(Map<K, V> map) {
    if (map == null) {
      throw new IllegalArgumentException("map is null!");
    }
    this.map = map;
  }

  @Override
  public void set(K k, V v) {
    KVCache.assertKeyNotNull(k);
    // 见 KVCache#set：value 为 null 时一律按删除处理
    if (v == null) {
      map.remove(k);
      return;
    }
    map.put(k, v);
  }

  @Override
  public boolean setnx(K k, V v) {
    KVCache.assertKeyNotNull(k);
    // 见 KVCache#setnx：value 为 null 时表示"没有值"，本方法不写入任何内容
    if (v == null) {
      return false;
    }
    return map.putIfAbsent(k, v) == null;
  }

  @Override
  public boolean containsKey(K k) {
    KVCache.assertKeyNotNull(k);
    return map.containsKey(k);
  }

  @Override
  public V get(K k) {
    KVCache.assertKeyNotNull(k);
    return map.get(k);
  }

  @Override
  public boolean delete(K k) {
    KVCache.assertKeyNotNull(k);
    return map.remove(k) != null;
  }

  @Override
  public int size() {
    return map.size();
  }

  /**
   * @return {@link KVCache#UNLIMITED_CAPACITY} —— 本层不限制条数，这个值是"实际上限"的哨兵，
   *         不是真实存在的容量约束，不要据此做内存规划
   */
  @Override
  public int capacity() {
    return UNLIMITED_CAPACITY;
  }

  @Override
  public V remove(K k) {
    KVCache.assertKeyNotNull(k);
    return map.remove(k);
  }

}
