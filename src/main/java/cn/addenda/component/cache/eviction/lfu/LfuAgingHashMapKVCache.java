package cn.addenda.component.cache.eviction.lfu;

import cn.addenda.component.cache.HashMapKVCache;

/**
 * 带衰减的 LFU 淘汰策略的最简形态。
 * <p>
 * <b>继承的规则</b>：全部来自 {@link LfuAgingKVCache}（在 {@link LfuKVCache} 的规则之上叠加
 * 衰减）。
 * <p>
 * <b>自身差异</b>：只有底层存储 —— delegate 是普通 {@link HashMapKVCache}，自身不具备淘汰
 * 策略，因此淘汰完全由访问次数决定，不存在第二层淘汰。需要底层再提供一层顺序保证时不要用
 * 本类，见 {@link LfuAgingLruKVCache}。
 * <p>
 * <b>安全级别</b>：无，见 {@link cn.addenda.component.cache.eviction.EvictableKVCacheWrapper}。
 *
 * @author addenda
 * @since 2026/10/02
 */
public class LfuAgingHashMapKVCache<K, V> extends LfuAgingKVCache<K, V> {

  /**
   * @param agingInterval 每多少次淘汰衰减一次，必须 &gt; 0
   * @param decayRatio    每次衰减缩减的比例，取值区间 {@code [0, 1]}；0 表示不衰减
   */
  public LfuAgingHashMapKVCache(int capacity, long agingInterval, double decayRatio) {
    super(capacity, agingInterval, decayRatio, true, new HashMapKVCache<>());
  }

  /**
   * @param countWriteAsVisit 覆盖写是否计为一次访问，见 {@link LfuKVCache#isCountWriteAsVisit()}
   */
  public LfuAgingHashMapKVCache(int capacity, long agingInterval, double decayRatio,
                                boolean countWriteAsVisit) {
    super(capacity, agingInterval, decayRatio, countWriteAsVisit, new HashMapKVCache<>());
  }

}
