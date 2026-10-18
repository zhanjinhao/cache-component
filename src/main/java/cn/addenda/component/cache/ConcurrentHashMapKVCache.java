package cn.addenda.component.cache;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 用 {@link ConcurrentHashMap} 承载的 {@link KVCache}。
 * <p>
 * <b>继承的规则</b>：Map 承载的通用语义来自 {@link MapKVCache}；L1 + L2 与原子的
 * {@code computeIfAbsent} 来自 {@link ConcurrentMapKVCache}。
 * <p>
 * <b>自身差异</b>：只有底层 Map 的选型 —— {@link ConcurrentHashMap}。
 * 需要按 key 设置过期时间时用 {@link ExpiredConcurrentHashMapKVCache}。
 * <p>
 * <b>安全级别</b>：L1 + L2，并发访问无需再用 {@link SynchronizedKVCache} 包装。
 * {@code mappingFunction} 在桶锁内执行，<b>不得回头操作本 cache</b>（JDK 8 上会永久挂死）。
 *
 * @author addenda
 * @since 2026/10/06
 */
public class ConcurrentHashMapKVCache<K, V> extends ConcurrentMapKVCache<K, V> {

  public ConcurrentHashMapKVCache() {
    super(new ConcurrentHashMap<>());
  }

}
