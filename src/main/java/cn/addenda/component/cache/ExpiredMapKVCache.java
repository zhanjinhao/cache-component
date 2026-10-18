package cn.addenda.component.cache;

import cn.addenda.component.base.pojo.Binary;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 用 {@link Map} 承载"value + 到期时刻"的 {@link ExpiredKVCache} 抽象。
 * <p>
 * <b>自身规则</b>：
 * <ul>
 *   <li>TTL 一律走 {@link ExpiredKVCache#ttlMillis} / {@link ExpiredKVCache#deadline} 换算，
 *       与其它实现只有一份规则；不带 TTL 的 {@link #set(Object, Object)} 表示永不过期；</li>
 *   <li><b>过期的 key 等同于没有值</b>：{@link #get(Object)} / {@link #containsKey(Object)} /
 *       {@link #remove(Object)} 与两个 {@code setnx} 一律按"没有值"回答；
 *       {@link #delete(Object)} 会把过期条目物理清掉，但返回 {@code false}
 *       —— 它没删掉"存在的数据"，与 redis 的 {@code DEL} 返回 0 同口径；</li>
 *   <li>过期项<b>惰性清理</b>：只有被上述操作碰到才会被物理删掉，在那之前 {@link #size()}
 *       仍把它算进去；只写过一次、之后不再被触及的过期项永远不会被释放（本层没有后台线程）；</li>
 *   <li>两个 {@code computeIfAbsent} <b>都不覆盖</b>，沿用接口默认实现，<b>不承诺 L2</b>；
 *       需要原子的实现见 {@link ExpiredConcurrentMapKVCache}。</li>
 * </ul>
 * 底层 Map 由子类在构造时指定，例如 {@link ExpiredHashMapKVCache} 传入 {@code HashMap}。
 * <p>
 * <b>安全级别</b>：由传入的 Map 决定，本类不做声明 —— 传入非线程安全的 Map 时连 L1 都没有，
 * 并发访问用 {@link SynchronizedExpiredKVCache} 包装。
 *
 * @author addenda
 * @since 2026/10/17
 */
public abstract class ExpiredMapKVCache<K, V> implements ExpiredKVCache<K, V> {

  /**
   * value + 到期时刻（毫秒）；{@link Long#MAX_VALUE} 表示永不过期
   */
  private final Map<K, Binary<V, Long>> map;

  protected ExpiredMapKVCache(Map<K, Binary<V, Long>> map) {
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
    map.put(k, Binary.of(v, Long.MAX_VALUE));
  }

  @Override
  public void set(K k, V v, long timeout, TimeUnit timeunit) {
    KVCache.assertKeyNotNull(k);
    // 见 KVCache#set：value 为 null 时一律按删除处理
    if (v == null) {
      map.remove(k);
      return;
    }
    map.put(k, Binary.of(v, ExpiredKVCache.deadline(timeout, timeunit)));
  }

  /**
   * 不带 TTL 的 setnx：<b>已过期的旧值等同于不存在</b>，不能把新写入挡在门外 ——
   * 过期条目仍占着 map 里的位置，不能直接用会把它当"存在"的 {@code map.putIfAbsent}。
   */
  @Override
  public boolean setnx(K k, V v) {
    KVCache.assertKeyNotNull(k);
    // 见 KVCache#setnx：value 为 null 时表示"没有值"，本方法不写入任何内容
    if (v == null) {
      return false;
    }
    // get 会顺手清掉已过期的条目："读不到"即"不存在"
    if (get(k) != null) {
      return false;
    }
    return map.putIfAbsent(k, Binary.of(v, Long.MAX_VALUE)) == null;
  }

  @Override
  public boolean setnx(K k, V v, long timeout, TimeUnit timeunit) {
    KVCache.assertKeyNotNull(k);
    if (v == null) {
      return false;
    }
    // 已过期的旧值等同于不存在，get 调用会一并将其清除
    if (get(k) != null) {
      return false;
    }
    return map.putIfAbsent(k, Binary.of(v, ExpiredKVCache.deadline(timeout, timeunit))) == null;
  }

  /**
   * key 是否存在，<b>已过期的等同于不存在</b>。
   * <p>
   * 必须走 {@link #get(Object)}，不能是裸的 {@code map.containsKey}：过期条目仍占着 map 里的
   * 位置，裸查询会说"在"，与 {@link #get(Object)} 返回 null 自相矛盾。上层的自愈也依赖这个口径。
   */
  @Override
  public boolean containsKey(K k) {
    KVCache.assertKeyNotNull(k);
    return get(k) != null;
  }

  @Override
  public V get(K k) {
    KVCache.assertKeyNotNull(k);
    Binary<V, Long> binary = map.get(k);
    if (binary == null) {
      return null;
    }
    if (isExpired(binary)) {
      map.remove(k);
      return null;
    }
    return binary.getF1();
  }

  /**
   * 删除缓存，返回是否删除了一条<b>存在</b>的数据。
   * <p>
   * <b>已过期的条目等同于不存在</b>：照样把它物理清掉，但返回 {@code false} ——
   * 与 {@link #remove(Object)}（过期返回 null）及 redis 实现（{@code DEL} 返回 0）口径一致。
   */
  @Override
  public boolean delete(K k) {
    KVCache.assertKeyNotNull(k);
    Binary<V, Long> removed = map.remove(k);
    return removed != null && !isExpired(removed);
  }

  @Override
  public int size() {
    return map.size();
  }

  /**
   * @return {@link KVCache#UNLIMITED_CAPACITY} —— 本层不限制条数，这个值是"实际上限"的哨兵，
   *         不是真实存在的容量约束；本层也不主动回收过期项，更不能据此推断"最多只会占这么多"
   */
  @Override
  public int capacity() {
    return UNLIMITED_CAPACITY;
  }

  /**
   * 取值并删除。<b>已过期的等同于没有值</b>：返回 null，同时把过期条目清掉。
   * <p>
   * 只看 key 在不在就返回过期旧值，会与 {@link #get(Object)} 的"读不到"矛盾。
   */
  @Override
  public V remove(K k) {
    KVCache.assertKeyNotNull(k);
    Binary<V, Long> removed = map.remove(k);
    if (removed == null || isExpired(removed)) {
      return null;
    }
    return removed.getF1();
  }

  /**
   * 到期时刻不晚于当前时刻即为已过期：{@code deadline <= now}。
   * <p>
   * 用 {@code <=} 而不是 {@code <}：redis 的语义是 {@code now >= deadline} 即失效，
   * 两者在"恰好等于到期时刻"这一格上必须一致。判定基准是 {@link System#currentTimeMillis()}。
   */
  protected boolean isExpired(Binary<V, Long> binary) {
    return binary.getF2() <= System.currentTimeMillis();
  }

}
