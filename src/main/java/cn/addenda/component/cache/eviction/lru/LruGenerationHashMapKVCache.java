package cn.addenda.component.cache.eviction.lru;

import cn.addenda.component.cache.HashMapKVCache;

/**
 * 分代 LRU 淘汰策略的最简形态。
 * <p>
 * <b>继承的规则</b>：分代规则（时间闸门、提升、两代分派等）全部来自
 * {@link LruGenerationKVCache}。
 * <p>
 * <b>自身差异</b>：唯一的不同是两代各自的存储选型 —— 年轻代的存储是 {@link HashMapKVCache}，
 * 老年代是一个 {@link LruHashMapKVCache}（容量为入参 {@code oldCapacity}）。
 * <p>
 * 需要换某一代的存储时，继承 {@link LruGenerationKVCache} 自己传。
 * <p>
 * <b>安全级别</b>：无，见 {@link cn.addenda.component.cache.eviction.EvictableKVCacheWrapper}。
 *
 * @author addenda
 * @since 2023/05/30
 */
public class LruGenerationHashMapKVCache<K, V> extends LruGenerationKVCache<K, V> {

  /**
   * @param newCapacity 年轻代容量
   * @param oldCapacity 老年代容量
   * @param threshold   观察期（毫秒），见 {@link LruGenerationKVCache#getWhenNonContainsKey}
   */
  public LruGenerationHashMapKVCache(int newCapacity, int oldCapacity, int threshold) {
    super(newCapacity, new HashMapKVCache<>(), new LruHashMapKVCache<>(oldCapacity), threshold);
  }

}
