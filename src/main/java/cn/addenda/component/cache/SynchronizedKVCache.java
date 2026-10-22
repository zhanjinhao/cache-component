package cn.addenda.component.cache;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * {@link KVCache} 的线程安全包装：所有方法在同一把 {@link ReentrantLock} 下委托给底层。
 * <p>
 * <b>继承的规则</b>：从 {@link KVCacheWrapper} 继承"数据只在 delegate 里、本类不复制存储"
 * 这一分工 —— 所有读写都落到 delegate，本类唯一自己的状态是这把锁；
 * {@link KVCache} 的语义（null key 报 {@link CacheIllegalArgumentException}、null value 按删除、
 * {@link #setnx(Object, Object)} 的四种输入组合等）由 delegate 决定，本类<b>不改写也不补齐</b>，
 * 只原样转交。
 * <p>
 * 注意两处不能照搬 {@link KVCacheWrapper} 的字面描述：
 * <ul>
 *   <li>{@link #size()} <b>不是</b>"直接转发"—— 本类覆盖了它（与其余方法一样），先持锁再转交；</li>
 *   <li>{@link #setnx(Object, Object)} <b>不是</b>继承来的默认实现 —— {@link KVCacheWrapper}
 *       刻意不提供"转发给底层"的默认实现，本类自己声明并在锁内转交。</li>
 * </ul>
 * <p>
 * <b>自身差异（把 L1 / L2 补到哪一层）</b>：本类不实现存储，只把底层的安全级别提到 L1 + L2 ——
 * 底层即使连 L1 都没有（如 {@link HashMapKVCache}），经本类包装后也满足：
 * <ul>
 *   <li>L1：并发调用不会损坏底层结构；</li>
 *   <li>L2：单个操作全程持锁；{@link #remove(Object)} 与
 *       {@link #computeIfAbsent(Object, Function)} 两个默认实现里的 check-then-act 也整段持锁，
 *       并发下 {@code mappingFunction} 只会被调用一次。</li>
 * </ul>
 * <b>代价</b>是所有操作串行，包括读。
 *
 * @author addenda
 * @since 2023/05/30
 */
public class SynchronizedKVCache<K, V> extends KVCacheWrapper<K, V> {

  private final ReentrantLock lock = new ReentrantLock();

  protected SynchronizedKVCache(KVCache<K, V> kvCache) {
    super(kvCache);
  }

  public static <KK, VV> SynchronizedKVCache<KK, VV> synchronize(KVCache<KK, VV> kvCache) {
    return new SynchronizedKVCache<>(kvCache);
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

  /**
   * 整段 check-then-act 持锁，见类注释里 L2 的复合操作条目。
   * <p>
   * {@code mappingFunction} 在锁内执行，运行期间本缓存的所有操作（<b>包括纯读</b>）都在等这把
   * 锁，所以它必须短小，且<b>不要在里面访问别的锁</b> —— 本方法用的是不可中断的 {@code lock()}，
   * 一旦锁顺序死锁，等待方既超时不了也中断不了。回头操作本 cache 不会挂死（{@link ReentrantLock}
   * 可重入），但回调里的写入会与外层随后的写入互相覆盖，仍不应这么写。
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

}
