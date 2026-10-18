package cn.addenda.component.cache.eviction.lfu;

import cn.addenda.component.cache.eviction.lru.LruHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruKVCache;

/**
 * 带衰减的 LFU 淘汰策略，底层存储（delegate）为一个 {@link LruKVCache}。
 * <p>
 * <b>继承的规则</b>：全部来自 {@link LfuAgingKVCache}（在 {@link LfuKVCache} 的规则之上叠加
 * 衰减）。
 * <p>
 * <b>自身差异</b>：delegate 是一个具备自身淘汰策略的 LRU，持有独立容量与队列，写入时若自身
 * 已满会独立淘汰其队头（{@link LfuAgingHashMapKVCache} 的 delegate 只是普通 HashMap，没这一
 * 层）。由此产生的两套统计结构与自愈路径，与 {@link LfuLruKVCache} 完全一致，详见该类说明。
 * <p>
 * <b>安全级别</b>：无，见 {@link cn.addenda.component.cache.eviction.EvictableKVCacheWrapper}。
 *
 * @author addenda
 * @since 2026/10/02
 */
public class LfuAgingLruKVCache<K, V> extends LfuAgingKVCache<K, V> {

  /**
   * @param agingInterval 每多少次淘汰衰减一次，必须 &gt; 0
   * @param decayRatio    每次衰减缩减的比例，取值区间 {@code [0, 1]}；0 表示不衰减
   */
  public LfuAgingLruKVCache(int capacity, long agingInterval, double decayRatio) {
    super(capacity, agingInterval, decayRatio, true,
            new LruHashMapKVCache<>(capacity));
  }

  /**
   * @param countWriteAsVisit 覆盖写是否计为一次访问，见 {@link LfuKVCache#isCountWriteAsVisit()}
   */
  public LfuAgingLruKVCache(int capacity, long agingInterval, double decayRatio,
                            boolean countWriteAsVisit) {
    super(capacity, agingInterval, decayRatio, countWriteAsVisit,
            new LruHashMapKVCache<>(capacity));
  }

}
