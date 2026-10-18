package cn.addenda.component.cache;

/**
 * {@link ExpiredKVCache} 的装饰器基类。
 * <p>
 * 与 {@link KVCacheWrapper} 一致，带过期时间的 {@code set} 与 {@code setnx} <b>均由子类各自
 * 实现</b>，这里不提供默认转发 —— 转发会绕过子类自身叠加的行为（登记统计、控制准入、加锁等）。
 * <p>
 * <b>安全级别</b>：与 {@link KVCacheWrapper} 一致 —— 由子类决定，本类不做声明
 * （见 {@link SynchronizedExpiredKVCache}）。
 *
 * @author addenda
 */
public abstract class ExpiredKVCacheWrapper<K, V> extends KVCacheWrapper<K, V>
        implements ExpiredKVCache<K, V> {

  protected ExpiredKVCacheWrapper(ExpiredKVCache<K, V> kvCacheDelegate) {
    super(kvCacheDelegate);
  }

  /**
   * 收窄返回类型，方便子类直接调用带 TTL 的重载。可见性保持 {@code protected}，
   * 理由见 {@link KVCacheWrapper} 上的说明。
   */
  @Override
  protected ExpiredKVCache<K, V> getKvCacheDelegate() {
    return (ExpiredKVCache<K, V>) super.getKvCacheDelegate();
  }

}
