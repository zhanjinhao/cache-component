package cn.addenda.component.cache.eviction.lfu;

import cn.addenda.component.cache.CacheIllegalArgumentException;
import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.SortedKVCache;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * 带衰减（Aging）的 LFU 抽象基类：在 {@link LfuKVCache} 的基础上给访问次数补上时间维度。
 * 底层存储由子类决定，见 {@link LfuAgingHashMapKVCache}、{@link LfuAgingLruKVCache}。
 * <p>
 * 解决的问题：{@link LfuKVCache} 的访问次数只增不减，历史高分 key 即使不再使用也长期占住
 * 容量，把新数据挤在最小那一档里轮换。
 * <p>
 * 做法：每发生 {@link #agingInterval} 次淘汰，把所有 key 的访问次数按 {@link #decayRatio}
 * 缩减一次（{@code 新分数 = 旧分数 × (1 - decayRatio)}）。以"淘汰次数"而非时间为触发点 ——
 * 淘汰意味着缓存正在受压，只有此时才需要区分新鲜的高分与陈旧的高分。
 * <p>
 * <b>{@link #decayRatio} 的取值</b>：{@code 0} 表示不衰减，行为与 {@link LfuKVCache}
 * <b>完全一致</b>（不触发衰减遍历、无额外开销）；常用值 {@code 0.5} 为每次减半；
 * 越接近 {@code 1} 越接近 FIFO。
 * <p>
 * 衰减只能让被刷高的分数慢慢回落，防"调用方反复写来刷分"仍需把
 * {@link #isCountWriteAsVisit()} 设为 {@code false}。
 * <p>
 * <b>安全级别</b>：无，见 {@link cn.addenda.component.cache.eviction.EvictableKVCacheWrapper}；
 * 需要并发正确性时使用 {@link cn.addenda.component.cache.SynchronizedKVCache} 包装。
 *
 * @author addenda
 * @since 2026/10/02
 */
@Getter
public abstract class LfuAgingKVCache<K, V> extends LfuKVCache<K, V> {

  /**
   * 每多少次淘汰触发一次衰减
   */
  private final long agingInterval;
  /**
   * 每次衰减缩减的比例，取值区间 {@code [0, 1]}；{@code 0} 表示不衰减，
   * 行为与 {@link LfuKVCache} 完全一致。
   */
  private final double decayRatio;
  /**
   * 累计淘汰次数
   */
  private long evictionCount = 0L;

  /**
   * @param agingInterval 每多少次淘汰衰减一次，必须 &gt; 0
   * @param decayRatio    每次衰减缩减的比例，必须在 {@code [0, 1]} 内；0 表示不衰减
   */
  protected LfuAgingKVCache(int capacity, long agingInterval, double decayRatio,
                            boolean countWriteAsVisit, KVCache<K, V> kvCacheDelegate) {
    super(capacity, countWriteAsVisit, kvCacheDelegate);
    assertAgingArgs(agingInterval, decayRatio);
    this.agingInterval = agingInterval;
    this.decayRatio = decayRatio;
  }

  /**
   * 供子类替换两张统计结构的实现，原因见 {@link LfuKVCache#LfuKVCache(KVCache, SortedKVCache, int, boolean, KVCache)}
   */
  protected LfuAgingKVCache(KVCache<K, Long> keyToVisitorCount,
                            SortedKVCache<Long, LinkedHashSet<K>> visitorCountToKeySet,
                            int capacity, long agingInterval, double decayRatio,
                            boolean countWriteAsVisit, KVCache<K, V> kvCacheDelegate) {
    super(keyToVisitorCount, visitorCountToKeySet, capacity, countWriteAsVisit, kvCacheDelegate);
    assertAgingArgs(agingInterval, decayRatio);
    this.agingInterval = agingInterval;
    this.decayRatio = decayRatio;
  }

  private static void assertAgingArgs(long agingInterval, double decayRatio) {
    if (agingInterval <= 0L) {
      throw new CacheIllegalArgumentException("agingInterval must be > 0.");
    }
    if (decayRatio < 0D || decayRatio > 1D) {
      throw new CacheIllegalArgumentException("decayRatio must be within [0, 1].");
    }
  }

  /**
   * 先执行父类定义的淘汰，再按需衰减（顺序上本次被淘汰的 key 已不在计数表里，无需为它衰减）。
   * <p>
   * <b>只统计"真的淘汰了数据"的那些次</b>：{@link LfuKVCache#removeMinOldest} 挑出的 key 可能
   * 已被数据层自行删除，那一次只清了账本残留 —— 缓存没有受压，不该推进年龄，否则
   * {@link #agingInterval} 这个节流阀会被与压力无关的场景耗掉。判据是父类返回的被淘汰值
   * （{@code null} 即空转），不是"本方法被调用了几次"。
   * <p>
   * {@link #decayRatio} 为 0 时不触发衰减，本方法与 {@link LfuKVCache} 的行为完全一致。
   */
  @Override
  protected V removeMinOldest() {
    V removed = super.removeMinOldest();
    if (removed == null) {
      // 空转：账本清了，数据层一条没删。缓存没受压，不推进衰减年龄
      return null;
    }
    long currentEvictionCount = ++evictionCount;
    if (decayRatio > 0D && currentEvictionCount % agingInterval == 0) {
      decay();
    }
    return removed;
  }

  /**
   * 把所有 key 的访问次数按 {@link #decayRatio} 缩减，并重建档位表。
   * <p>
   * 必须<b>升序</b>处理档位：衰减后的档位一定不大于当前档位，升序才能保证被合并进目标档的
   * key 不会再被减一次。升序由本方法自己排序保证，<b>不依赖</b> {@code keySet()} 的迭代顺序
   * （{@link SortedKVCache} 可注入，未必按 key 升序迭代）。
   */
  private void decay() {
    // 衰减要读写账本，必须拿活对象：visitorCountToKeySet() / keyToVisitorCount() 是 protected 的
    // 活对象访问器，而 getXxx() 是给外部的只读外壳，其 set / delete 会直接抛异常。
    List<Long> counts = new ArrayList<>(visitorCountToKeySet().keySet());
    Collections.sort(counts);
    for (Long count : counts) {
      long decayed = (long) (count * (1 - decayRatio));
      if (decayed == count) {
        // 0 分档不变，跳过
        continue;
      }
      LinkedHashSet<K> keys = visitorCountToKeySet().get(count);
      visitorCountToKeySet().delete(count);
      for (K k : keys) {
        keyToVisitorCount().set(k, decayed);
      }
      // 目标档位可能已有其它 key（例如减半时 2 分和 3 分都落到 1 档）；追加即可，
      // LinkedHashSet 保留各自档内原有先后
      visitorCountToKeySet().computeIfAbsent(decayed, s -> new LinkedHashSet<>()).addAll(keys);
    }
  }

}
