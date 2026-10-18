package cn.addenda.component.cache.eviction.lru;

import cn.addenda.component.cache.HashMapKVCache;

/**
 * LRU 淘汰策略的最简形态。
 * <p>
 * <b>继承的规则</b>：淘汰策略与结构全部来自 {@link LruKVCache} —— 队列与底层存储的分工、
 * 淘汰触发条件，以及它声明的安全级别。
 * <p>
 * <b>自身差异</b>：唯一的不同是底层存储（delegate）用 {@link HashMapKVCache}。
 * <p>
 * 需要换一个底层存储（例如一个带过期能力的存储）时，继承 {@link LruKVCache} 自己传 delegate。
 * <p>
 * <b>安全级别</b>：无，见 {@link cn.addenda.component.cache.eviction.EvictableKVCacheWrapper}。
 *
 * @author addenda
 * @since 2023/05/30
 */
public class LruHashMapKVCache<K, V> extends LruKVCache<K, V> {

  public LruHashMapKVCache(int capacity) {
    super(capacity, new HashMapKVCache<>());
  }

}
