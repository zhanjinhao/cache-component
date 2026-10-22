package cn.addenda.component.cache;

import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * 用 {@link ConcurrentMap} 承载数据的 {@link KVCache} 抽象。
 * <p>
 * <b>自身规则</b>：语义与 {@link MapKVCache} 相同 —— value 为 null 按删除、{@code setnx}
 * 不覆盖已有 key、不限制条数（{@link #capacity()} 返回 {@link KVCache#UNLIMITED_CAPACITY}
 * 哨兵）、{@link #remove(Object)} 是一次删除。
 * <p>
 * <b>为什么不继承 {@link MapKVCache}</b>：本类承诺 L1 + L2，而 L2 要求<b>每一个</b>方法整体
 * 原子。继承会把这条承诺押在父类的方法体上 —— 父类面向任意 Map，它哪天把某个方法改成两步
 * 实现，子类就会静默失去原子性。因此这里每个方法都直接落在 ConcurrentMap 上，承诺由本类
 * 自己持有。
 * <p>
 * {@code set} 的单次写、{@code setnx} 的 {@code putIfAbsent}、{@link #remove(Object)} 的
 * 单次删除本身就是原子的；{@link #computeIfAbsent(Object, Function)} 委托 ConcurrentMap 的
 * 原子版本。底层 Map 由子类指定，例如 {@link ConcurrentHashMapKVCache}。
 * <p>
 * <b>{@code mappingFunction} 在桶锁内执行，不得回头操作本 cache</b>：在 JDK 8 上会让线程
 * 永久挂死、不抛异常。读操作（{@code get} / {@code containsKey} / {@code size}）不进入桶锁，
 * 是安全的。
 * <p>
 * <b>安全级别</b>：L1 + L2 —— 自身线程安全，且所有方法整体原子，无需再用
 * {@link SynchronizedKVCache} 包装。
 *
 * @author addenda
 * @since 2026/10/17
 */
public abstract class ConcurrentMapKVCache<K, V> implements KVCache<K, V> {

  private final ConcurrentMap<K, V> map;

  protected ConcurrentMapKVCache(ConcurrentMap<K, V> map) {
    if (map == null) {
      throw new CacheIllegalArgumentException("map must not be null!");
    }
    this.map = map;
  }

  @Override
  public void set(K k, V v) {
    KVCache.assertKeyNotNull(k);
    // 见 KVCache#set：value 为 null 时按删除处理；ConcurrentMap 不接受 null value，必须显式分流
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
   * @return {@link KVCache#UNLIMITED_CAPACITY} —— 本实现不限制条数，这个值是"实际上限"的哨兵，
   *         不是真实存在的容量约束，不要据此做内存规划
   */
  @Override
  public int capacity() {
    return UNLIMITED_CAPACITY;
  }

  /**
   * 覆盖默认实现：一步原子地取出并删除。
   * <p>
   * 默认实现是"先 get 再 delete"两步，并发下会取到别人刚写进来的值再把它删掉；
   * 本实现由 ConcurrentMap 保证整体原子。
   */
  @Override
  public V remove(K k) {
    KVCache.assertKeyNotNull(k);
    return map.remove(k);
  }

  /**
   * 覆盖默认实现：整段 check-then-act 由 ConcurrentMap 的原子版本完成，并发下同一个 key 的
   * {@code mappingFunction} 只会被调用一次；计算结果为 null 时不写入。
   */
  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    KVCache.assertKeyNotNull(key);
    return map.computeIfAbsent(key, mappingFunction);
  }

}
