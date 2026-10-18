package cn.addenda.component.cache;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * {@link ExpiredKVCache} 的线程安全包装：所有方法在同一把 {@link ReentrantLock} 下委托给底层。
 * <p>
 * <b>继承的规则</b>：从 {@link ExpiredKVCacheWrapper} 继承"数据只在 delegate 里、本类不复制
 * 存储"这一分工 —— 所有读写都落到 delegate，本类唯一自己的状态是这把锁；
 * {@link ExpiredKVCache} 的语义（TTL 换算、过期即不存在、null value 按删除等）由 delegate 决定，
 * 本类<b>不改写也不补齐</b>，只原样转交。
 * <p>
 * 注意两处不能照搬父类的字面描述：
 * <ul>
 *   <li>{@link #size()} <b>不是</b>"直接转发"—— 本类覆盖了它（与其余方法一样），先持锁再转交；</li>
 *   <li>带 TTL 的 {@code set} / {@code setnx} <b>不是</b>继承来的默认实现 ——
 *       {@link ExpiredKVCacheWrapper} 刻意不提供转发版，本类自己声明并在锁内转交。</li>
 * </ul>
 * <p>
 * <b>自身差异</b>：只把底层的安全级别提到 L1 + L2，与 {@link SynchronizedKVCache} 同理。
 * 带 TTL 的 {@code set} / {@code setnx} 由本类声明并持锁；带 TTL 的
 * {@link #computeIfAbsent(Object, Function, long, TimeUnit)} 覆盖掉接口默认实现并整段持锁 ——
 * 不覆盖的话，默认实现是 get + set 两次委托，两步之间不持锁。
 *
 * @author addenda
 * @since 2023/05/30
 */
public class SynchronizedExpiredKVCache<K, V> extends ExpiredKVCacheWrapper<K, V> {

  private final ReentrantLock lock = new ReentrantLock();

  protected SynchronizedExpiredKVCache(ExpiredKVCache<K, V> kvCache) {
    super(kvCache);
  }

  public static <KK, VV> SynchronizedExpiredKVCache<KK, VV> synchronize(ExpiredKVCache<KK, VV> kvCache) {
    return new SynchronizedExpiredKVCache<>(kvCache);
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
  public void set(K k, V v, long timeout, TimeUnit timeunit) {
    lock.lock();
    try {
      getKvCacheDelegate().set(k, v, timeout, timeunit);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean setnx(K k, V v, long timeout, TimeUnit timeunit) {
    lock.lock();
    try {
      return getKvCacheDelegate().setnx(k, v, timeout, timeunit);
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
  public int capacity() {
    lock.lock();
    try {
      return getKvCacheDelegate().capacity();
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
  public V remove(K k) {
    lock.lock();
    try {
      return getKvCacheDelegate().remove(k);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    lock.lock();
    try {
      return getKvCacheDelegate().computeIfAbsent(key, mappingFunction);
    } finally {
      lock.unlock();
    }
  }

  /**
   * 带 TTL 的重载同样整段持锁。
   * <p>
   * 必须显式覆盖：接口默认实现是"先 {@code get}、未命中再带 TTL {@code set}"两步，两次调用
   * 之间不持锁，并发下 {@code mappingFunction} 会被调用多次，达不到本类声明的 L2。
   */
  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction,
                           long timeout, TimeUnit timeunit) {
    lock.lock();
    try {
      return getKvCacheDelegate().computeIfAbsent(key, mappingFunction, timeout, timeunit);
    } finally {
      lock.unlock();
    }
  }

}
