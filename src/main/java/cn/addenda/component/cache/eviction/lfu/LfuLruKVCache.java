package cn.addenda.component.cache.eviction.lfu;

import cn.addenda.component.cache.eviction.lru.LruHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruKVCache;

/**
 * LFU 与 LRU 的两层组合：淘汰对象由 {@link LfuKVCache} 按访问次数决定，底层存储（delegate）
 * 为一个 {@link LruKVCache}。
 * <p>
 * <b>继承的规则</b>：全部来自 {@link LfuKVCache}（两张表的同步、淘汰触发条件、自愈口径等）。
 * <p>
 * <b>自身差异</b>：delegate 是一个具备自身淘汰策略的 LRU，持有独立容量与队列，写入时若自身
 * 已满会独立淘汰其队头（{@link LfuHashMapKVCache} 的 delegate 只是普通 HashMap，没这一层）。
 * 正常情况下 LFU 先按 {@code keyToVisitorCount} 淘汰并腾出位置，底层存储不会触发淘汰。
 * <p>
 * 若底层 LRU 自行淘汰了 key，LFU 层<b>不会</b>收到通知，访问次数计数会残留，直到该 key 下次被
 * 访问时由 {@code EvictableKVCache#containsKey} 走到 {@code clearKeyStatistic} 清除。清除之前
 * <b>账本</b>条数大于真实存活数量 —— 被污染的只是"淘汰挑谁"的依据；{@link #size()} <b>不受
 * 影响</b>，它取自数据层。两层的 capacity 建议保持一致，否则"底层不应自行触发淘汰"的前提不成立。
 * <p>
 * <b>安全级别</b>：无，见 {@link cn.addenda.component.cache.eviction.EvictableKVCacheWrapper}；
 * 需要并发正确时在外层包装 {@link cn.addenda.component.cache.SynchronizedKVCache}。
 *
 * @author addenda
 * @since 2023/05/30
 */
public class LfuLruKVCache<K, V> extends LfuKVCache<K, V> {

  public LfuLruKVCache(int capacity) {
    super(capacity, new LruHashMapKVCache<>(capacity));
  }

  /**
   * @param countWriteAsVisit 覆盖写是否计为一次访问，见 {@link LfuKVCache#isCountWriteAsVisit()}
   */
  public LfuLruKVCache(int capacity, boolean countWriteAsVisit) {
    super(capacity, countWriteAsVisit, new LruHashMapKVCache<>(capacity));
  }

}
