package cn.addenda.component.cache.eviction.lru;

import cn.addenda.component.base.string.Slf4jUtils;
import cn.addenda.component.cache.CacheIllegalArgumentException;
import cn.addenda.component.cache.HashMapKVCache;
import cn.addenda.component.cache.KVCache;

/**
 * LRU-K 淘汰策略的最简形态。
 * <p>
 * <b>继承的规则</b>：除两处存储的选型外，全部来自 {@link LruKKVCache} —— 准入闸门、
 * 计数规则、不支持 {@code setnx} / {@code computeIfAbsent} 等。
 * <p>
 * <b>自身差异</b>：两处存储的选型 —— 主缓存的存储是 {@link HashMapKVCache}；访问计数账本
 * （{@code visitorCount}）是一个 {@link LruHashMapKVCache}，容量取主缓存的 {@code 2} 倍。
 * 账本自身也具备 LRU 淘汰：短时间内扫描过多 key 会将先前 key 的记录挤出，该 key 已累积的
 * 准入进度随之归零。
 * <p>
 * 另外提供一个开关 {@code countFirstWriteAsVisit}：第一次写是否算一次准入引用。默认
 * {@code true}（写算一次引用，{@code threshold = 1} 时等价于普通 LRU）；置为
 * {@code false} 后，准入凭证只由<b>未命中的读</b>签发，只写不读的 key 永远不会进入缓存。
 * <p>
 * 换某一处存储（例如一个带过期能力的存储，或换一个不同容量的账本）时，继承
 * {@link LruKKVCache} 自己传。
 * <p>
 * <b>安全级别</b>：无，见 {@link cn.addenda.component.cache.eviction.EvictableKVCacheWrapper}。
 *
 * @author addenda
 * @since 2023/05/30
 */
public class LruKHashMapKVCache<K, V> extends LruKKVCache<K, V> {

  /**
   * 等价于 {@link #LruKHashMapKVCache(int, int, boolean)}，{@code countFirstWriteAsVisit = true}：
   * 访问计数账本的容量取主缓存的两倍，见类注释。
   */
  public LruKHashMapKVCache(int capacity, int threshold) {
    this(capacity, threshold, true);
  }

  /**
   * @param countFirstWriteAsVisit 第一次写是否为未建档的 key 建档（是否算一次准入引用）。
   *                               {@code false} 时准入凭证只由未命中的读签发，
   *                               {@code threshold = 1} 也不再"写过就进"
   */
  public LruKHashMapKVCache(int capacity, int threshold, boolean countFirstWriteAsVisit) {
    super(capacity, threshold, countFirstWriteAsVisit,
            new LruHashMapKVCache<>(visitorCountCapacity(capacity)), new HashMapKVCache<>());
  }

  /**
   * 访问计数账本的容量 = 主缓存的两倍。
   * <p>
   * 账本是一个<b>独立的 cache 实例</b>，其容量同样必须落在
   * {@code [0, KVCache.UNLIMITED_CAPACITY]} 内（见 {@link KVCache#UNLIMITED_CAPACITY}），
   * 于是主缓存的容量被间接限制为它的两倍以内。比较前先转 {@code long}：上限将来可能被调低或
   * 来自配置，不能靠"溢出回绕"兜底。
   * <p>
   * 在这里显式判、而不是把 {@code capacity * 2} 直接交给账本的构造器：后者抛出来的消息说的是
   * 账本的数字，而调用方传的是 {@code capacity}，数字对不上。
   */
  private static int visitorCountCapacity(int capacity) {
    if ((long) capacity * 2 > KVCache.UNLIMITED_CAPACITY) {
      throw new CacheIllegalArgumentException(Slf4jUtils.format(
              "Primary cache capacity is too large: the visitor-count ledger capacity is twice that value,"
                      + " and the ledger itself must not exceed {} (KVCache.UNLIMITED_CAPACITY);"
                      + " therefore the primary cache capacity must not exceed {}; current value: [{}].",
              KVCache.UNLIMITED_CAPACITY, KVCache.UNLIMITED_CAPACITY / 2, capacity));
    }
    return capacity * 2;
  }

}
