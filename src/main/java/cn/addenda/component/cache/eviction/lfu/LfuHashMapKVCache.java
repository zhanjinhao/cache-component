package cn.addenda.component.cache.eviction.lfu;

import cn.addenda.component.cache.HashMapKVCache;

/**
 * LFU 淘汰策略的最简形态。
 * <p>
 * <b>继承的规则</b>：全部来自 {@link LfuKVCache}（两张表的同步、淘汰触发条件、自愈口径等）。
 * <p>
 * <b>自身差异</b>：只有底层存储 —— delegate 是普通 {@link HashMapKVCache}，自身不具备淘汰
 * 策略，因此淘汰完全由访问次数决定，不存在第二层淘汰。需要底层再提供一层顺序保证时不要用
 * 本类，见 {@link LfuLruKVCache}。
 * <p>
 * <b>安全级别</b>：无，见 {@link cn.addenda.component.cache.eviction.EvictableKVCacheWrapper}。
 *
 * @author addenda
 * @since 2023/05/30
 */
public class LfuHashMapKVCache<K, V> extends LfuKVCache<K, V> {

  public LfuHashMapKVCache(int capacity) {
    super(capacity, new HashMapKVCache<>());
  }

  /**
   * @param countWriteAsVisit 覆盖写是否计为一次访问，见 {@link LfuKVCache#isCountWriteAsVisit()}
   */
  public LfuHashMapKVCache(int capacity, boolean countWriteAsVisit) {
    super(capacity, countWriteAsVisit, new HashMapKVCache<>());
  }

}
