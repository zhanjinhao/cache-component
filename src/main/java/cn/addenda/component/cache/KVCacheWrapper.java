package cn.addenda.component.cache;

import lombok.AccessLevel;
import lombok.Getter;

/**
 * kv-cache 包装类的抽象。
 * <p>
 * 本类只提供底层存储的持有与访问（{@link #size()} 转发底层），<b>{@link #setnx(Object, Object)}
 * 由子类各自实现</b>，这里不提供"直接转发给底层存储"的默认实现 —— 转发会绕过子类叠加的行为
 * （登记统计、控制准入、加锁等），而 {@code setnx} 的写入正是这些行为的作用点。
 * <p>
 * <b>安全级别</b>：由子类决定，本类不做声明 —— 本类自身不引入任何同步手段，除了
 * {@link #size()} 之外的成员全部由子类实现，因此既不承诺 L1 也不承诺 L2，见各子类注释里的
 * "安全级别"一节（例如 {@link SynchronizedKVCache} 把 L1 与 L2 一并补上）。
 *
 * @author addenda
 * @since 2023/05/30
 */
public abstract class KVCacheWrapper<K, V> implements KVCache<K, V> {

  /**
   * 底层存储，即实际存储数据的 cache。
   * <p>
   * 访问器是 {@code protected}，<b>不是 public</b>：拿到它意味着绕开本包装层的一切保护
   * （加锁、淘汰统计同步等），因此它是<b>给子类实现自己用的</b>。子类重写它可以收窄返回类型
   * （见 {@link ExpiredKVCacheWrapper}、{@link SynchronizedSortedKVCache}），但不能放宽回 public。
   */
  @Getter(AccessLevel.PROTECTED)
  private final KVCache<K, V> kvCacheDelegate;

  protected KVCacheWrapper(KVCache<K, V> kvCacheDelegate) {
    if (kvCacheDelegate == null) {
      throw new CacheIllegalArgumentException("kvCacheDelegate must not be null!");
    }
    this.kvCacheDelegate = kvCacheDelegate;
  }

  @Override
  public int size() {
    return kvCacheDelegate.size();
  }

}
