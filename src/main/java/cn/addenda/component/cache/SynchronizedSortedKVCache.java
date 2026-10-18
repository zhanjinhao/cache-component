package cn.addenda.component.cache;

import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * {@link SortedKVCache} 的线程安全包装：所有方法在同一把 {@link ReentrantLock} 下委托给底层。
 * <p>
 * <b>继承的规则</b>：从 {@link KVCacheWrapper} 继承"包装类不引入自己的存储、只叠加行为"
 * 这一约定 —— 本类不引入任何自己的存储，也不改变任何方法的语义；{@link SortedKVCache} 的
 * {@link #getFirst()} / {@link #getLast()} / {@link #keySet()} 与 {@link KVCache} 的语义
 * 都原样穿过包装层。
 * <p>
 * <b>自身差异</b>：与 {@link SynchronizedKVCache} 是同一件事，区别只在<b>保留哪个接口</b> ——
 * 本类实现 {@link SortedKVCache}，把 {@code getFirst} / {@code getLast} / {@code keySet}
 * 一起锁住并转发；用 {@link SynchronizedKVCache} 包会丢掉这三个方法。
 * <p>
 * <b>安全级别</b>：L1 + L2。L1：并发调用不会损坏底层结构（即使底层是 {@link TreeMapKVCache}）；
 * L2：单个操作全程持锁，{@link #remove(Object)} 与 {@link #computeIfAbsent(Object, Function)}
 * 的 check-then-act 也整段持锁。代价是所有操作串行，包括读。
 * <p>
 * <b>两处保护不到的地方</b>：{@link #keySet()} 只能原样转发底层的活视图，遍历或修改它发生在
 * 锁之外；被包装的实现若只持有外部 {@code SortedMap} 的引用（见 {@link SortedMapKVCache}），
 * 调用方直接写那个 map 同样绕过本类的锁 —— {@link TreeMapKVCache} 自建 map，没有这个问题。
 *
 * @author addenda
 * @since 2026/10/10
 */
public class SynchronizedSortedKVCache<K, V> extends KVCacheWrapper<K, V>
        implements SortedKVCache<K, V> {

  private final ReentrantLock lock = new ReentrantLock();

  protected SynchronizedSortedKVCache(SortedKVCache<K, V> kvCache) {
    super(kvCache);
  }

  public static <KK, VV> SynchronizedSortedKVCache<KK, VV> synchronize(SortedKVCache<KK, VV> kvCache) {
    return new SynchronizedSortedKVCache<>(kvCache);
  }

  /**
   * 收窄返回类型，方便本类转发 {@link SortedKVCache} 特有方法时不再强转。
   * 可见性保持 {@code protected}，理由见 {@link KVCacheWrapper} 上的说明。
   */
  @Override
  protected SortedKVCache<K, V> getKvCacheDelegate() {
    return (SortedKVCache<K, V>) super.getKvCacheDelegate();
  }

  @Override
  public void set(K k, V v) {
    lock.lock();
    try {
      getKvCacheDelegate().set(k, v);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean setnx(K k, V v) {
    lock.lock();
    try {
      return getKvCacheDelegate().setnx(k, v);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean containsKey(K k) {
    lock.lock();
    try {
      return getKvCacheDelegate().containsKey(k);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public V get(K k) {
    lock.lock();
    try {
      return getKvCacheDelegate().get(k);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean delete(K k) {
    lock.lock();
    try {
      return getKvCacheDelegate().delete(k);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int size() {
    lock.lock();
    try {
      return getKvCacheDelegate().size();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int capacity() {
    lock.lock();
    try {
      return getKvCacheDelegate().capacity();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public V remove(K k) {
    lock.lock();
    try {
      return getKvCacheDelegate().remove(k);
    } finally {
      lock.unlock();
    }
  }

  /**
   * 整段 check-then-act 持锁，见类注释里 L2 的复合操作条目。
   * <p>
   * {@code mappingFunction} 在锁内执行，要求与
   * {@link SynchronizedKVCache#computeIfAbsent(Object, Function)} 完全相同：必须短小、
   * 不得访问别的锁，也不要回头写本 cache。
   */
  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    lock.lock();
    try {
      return getKvCacheDelegate().computeIfAbsent(key, mappingFunction);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public K getFirst() {
    lock.lock();
    try {
      return getKvCacheDelegate().getFirst();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public K getLast() {
    lock.lock();
    try {
      return getKvCacheDelegate().getLast();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Set<K> keySet() {
    lock.lock();
    try {
      return getKvCacheDelegate().keySet();
    } finally {
      lock.unlock();
    }
  }

}
