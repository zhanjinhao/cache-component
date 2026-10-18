package cn.addenda.component.cache.eviction.lru;

import cn.addenda.component.cache.KVCache;

import java.util.function.Function;

/**
 * LRU-K 缓存实现（基础 LRU 等价于 LRU-1）。
 * <p>
 * <b>继承的规则</b>：淘汰机制（队列结构、数据层与队列分工、线程安全口径等）全部来自
 * {@link LruKVCache}。
 * <p>
 * <b>自身差异</b>：多了<b>准入</b>闸门 —— 基础 LRU 写入时即进入队列，本类要求 key 先累积
 * {@link #threshold} 次<b>未命中</b>（或 {@link #countFirstWriteAsVisit} 为 {@code true} 时的
 * 第一次写）才具备进入队列的资格；未达标的 key 只把次数记录在 {@link #visitorCount} 中，
 * <b>value 不落地</b>。
 * <p>
 * <b>计数规则</b>：每个 key 在 {@link #visitorCount} 中最多一份记录。
 * <ul>
 *   <li>建档：<b>第一次未命中读</b>时建立，初值为 {@code 1}；{@link #countFirstWriteAsVisit}
 *       为 {@code true}（默认）时，<b>第一次写</b>也会建档并记 {@code 1} 分；</li>
 *   <li>累加：此后每次<b>未命中</b>的 {@link #get} 再加 {@code 1}；<b>命中不建档、不累加</b>，
 *       写也不累加；</li>
 *   <li>准入：每次 {@link #set} 时检查计数值，{@code >= threshold} 才真正写入主缓存，
 *       写入的同时删除该计数记录。</li>
 * </ul>
 * 累积资格由读完成、执行准入由写完成，两者不在同一步（{@code get} 未命中时不持有 value）。
 * <p>
 * <b>写算不算一次引用由 {@link #countFirstWriteAsVisit} 决定</b>：默认 {@code true}，
 * 于是 {@code threshold = 1} 时"写过就进"、等价于普通 LRU；置为 {@code false} 后准入只由
 * demand miss 驱动，只写不读的 key 永远不会进入缓存。
 * <p>
 * 计数是<b>一次性准入凭证</b>：只有在主缓存里读不到的 key 才需要它。已准入的 key 命中时
 * 不建档 —— 准入判断不看它们，建档只会占用账本槽位、挤掉待准入 key 的进度。
 * <p>
 * <b>安全级别</b>：无，见 {@link cn.addenda.component.cache.eviction.EvictableKVCacheWrapper}。
 * 需要并发正确性时使用 {@link cn.addenda.component.cache.SynchronizedKVCache} 包装。
 *
 * @author addenda
 * @since 2023/05/30
 */
public abstract class LruKKVCache<K, V> extends LruKVCache<K, V> {
  /**
   * 访问多少次可以入队
   */
  private final int threshold;
  /**
   * 准入计数：key 还差多少次未命中才能落地（一次性凭证，准入时删除）
   */
  private final KVCache<K, Integer> visitorCount;
  /**
   * 第一次写是否为"还没建档"的 key 建档并记 {@code 1} 分。
   * <p>
   * {@code true}（默认）：写也算一次引用，{@code threshold = 1} 时"写过就进"，等价于普通 LRU；
   * {@code false}：准入凭证只能由<b>未命中的读</b>签发，写只负责在拿到凭证后落地 ——
   * 只写不读的 key 永远不会进入缓存。
   * <p>
   * 只影响<b>第一次写</b>（建档的那一次）；对已建档 key 的后续写从来不加分。
   */
  private final boolean countFirstWriteAsVisit;

  /**
   * 默认 {@code countFirstWriteAsVisit = true}，行为与既往一致。
   *
   * @param visitorCount 记录 key 未命中次数的缓存，由子类指定
   */
  protected LruKKVCache(int capacity, int threshold,
                        KVCache<K, Integer> visitorCount, KVCache<K, V> kvCacheDelegate) {
    this(capacity, threshold, true, visitorCount, kvCacheDelegate);
  }

  /**
   * @param countFirstWriteAsVisit 第一次写是否为未建档的 key 建档，
   *                              见 {@link #countFirstWriteAsVisit}
   */
  protected LruKKVCache(int capacity, int threshold, boolean countFirstWriteAsVisit,
                        KVCache<K, Integer> visitorCount, KVCache<K, V> kvCacheDelegate) {
    super(capacity, kvCacheDelegate);
    assertThreshold(threshold);
    this.threshold = threshold;
    this.countFirstWriteAsVisit = countFirstWriteAsVisit;
    this.visitorCount = visitorCount;
  }

  /**
   * 供子类替换队列实现，原因见 {@link LruKVCache#LruKVCache(int, KVCache, LruDeque)}；
   * 默认 {@code countFirstWriteAsVisit = true}。
   */
  protected LruKKVCache(int capacity, int threshold, LruDeque<K> lruDeque,
                        KVCache<K, Integer> visitorCount, KVCache<K, V> kvCacheDelegate) {
    this(capacity, threshold, true, lruDeque, visitorCount, kvCacheDelegate);
  }

  /**
   * 供子类替换队列实现，见 {@link #countFirstWriteAsVisit}。
   */
  protected LruKKVCache(int capacity, int threshold, boolean countFirstWriteAsVisit,
                        LruDeque<K> lruDeque,
                        KVCache<K, Integer> visitorCount, KVCache<K, V> kvCacheDelegate) {
    super(capacity, kvCacheDelegate, lruDeque);
    assertThreshold(threshold);
    this.threshold = threshold;
    this.countFirstWriteAsVisit = countFirstWriteAsVisit;
    this.visitorCount = visitorCount;
  }

  /**
   * {@code threshold} 的定义域是 {@code [1, +∞)}。
   * <p>
   * 它的含义是"攒够几次访问才具备准入资格"，而计数<b>从第一份凭证签发起就是 1</b>
   * （未命中的读；{@link #countFirstWriteAsVisit} 为 {@code true} 时第一次写也算），
   * 所以 1 是最小的有意义取值。0 与负数落在定义域外：判定式 {@code count >= threshold}
   * 会被第一份凭证自带的那个 1 直接满足，不构成门槛。
   * <p>
   * 校验放在构造器里：参数错误应当在构造时暴露，而不是等到某个 key 攒够次数才显现。
   */
  private static void assertThreshold(int threshold) {
    if (threshold < 1) {
      throw new IllegalArgumentException(
              "threshold 需要 >= 1（准入资格是\"攒够几次访问\"，计数从第一份凭证起即为 1）；当前值："
                      + threshold + "。");
    }
  }

  /**
   * 取值；<b>只有读不到时</b>才记录一次准入计数。
   * <p>
   * 计数是"一次性准入凭证"：只有在主缓存里读不到的 key 才需要它。命中（含已准入后再次读到）
   * 不建档、不累加 —— 准入判断不看已准入的 key，建档只会占用账本槽位、挤掉待准入 key 的进度。
   * <p>
   * 顺序必须是<b>先读后计数</b>：{@code super.get} 会顺带自愈（底层已丢、队列还留着时清掉
   * 残留记录），若先计数再读，自愈可能把刚建的记录删掉，这次未命中的证据就白丢了。
   * <p>
   * 未命中只累积到阈值并不会使 key 进入缓存，落地动作只发生在 {@link #set} 中。
   */
  @Override
  public V get(K k) {
    KVCache.assertKeyNotNull(k);
    V v = super.get(k);
    if (v == null) {
      Integer count = visitorCount.get(k);
      visitorCount.set(k, count == null ? 1 : count + 1);
    }
    return v;
  }

  /**
   * k 不在主缓存中：检查计数记录，达到 {@code threshold} 次才真正入队。
   * <p>
   * 计数记录通常已由未命中的读建立（见 {@link #get}）。若不存在（本层第一次接触这个 key）：
   * <ul>
   *   <li>{@link #countFirstWriteAsVisit} 为 {@code true}（默认）：此处建档，并把本次写计为
   *       一次引用；</li>
   *   <li>为 {@code false}：<b>不建档、不产生任何效果</b> —— 准入凭证只由未命中的读签发，
   *       写只负责在拿到凭证后落地。</li>
   * </ul>
   * 已建档 key 的后续写不加分：能使次数增长的只有未命中的 {@link #get}。
   * <p>
   * 次数不足时本方法<b>不产生任何效果</b>：value 不落地、{@code get} 取不到值。
   */
  @Override
  protected void setWhenNonContainsKey(K k, V v) {
    Integer count = visitorCount.get(k);
    if (count == null) {
      if (!countFirstWriteAsVisit) {
        // 写不签发准入凭证：不建档，也不落地
        return;
      }
      // 第一次写这个 key：建档，写本身算一次引用
      visitorCount.set(k, 1);
      count = 1;
    }
    if (count >= threshold) {
      visitorCount.delete(k);
      super.setWhenNonContainsKey(k, v);
    }
  }

  /**
   * 自动过期的需要清理visitorCount
   * <p>
   * 本来就不存在的不需要清理visitorCount
   * <p>
   * 判断依据为"key 是否存在于 LRU 队列中"：
   * <ul>
   *   <li>存在于队列中但取不到值：该 key 已过期或被淘汰，其访问历史一并作废，需要清理；</li>
   *   <li>不存在于队列中：该 key 从未进入主缓存，其计数属于等待准入的访问历史，
   *       清理等同于将进度归零。</li>
   * </ul>
   */
  @Override
  protected void clearKeyStatistic(K k) {
    // lruDeque里如果存在k，说明k是自动过期的
    if (getLruDeque().contains(k)) {
      visitorCount.delete(k);
    }
    super.clearKeyStatistic(k);
  }

  /**
   * <b>不支持，一律抛出 {@link UnsupportedOperationException}。</b>
   * <p>
   * LRU-K 的写入需要经过准入闸门，与 {@code setnx}"不存在则写入、写入后即可读取"的语义直接
   * 冲突：门槛大于等于 2 时恒返回 false（key 已存在则入口直接返回 false；不存在则 {@code set}
   * 因次数不足不会落地，末尾的 {@code containsKey} 仍为 false），调用方无法区分该 false 表示
   * "已存在"还是"未落地"。需要"写过即进"的行为请改用 {@link LruKVCache}。
   * <p>
   * 参数不校验：整个操作都不支持时，"key / v 为 null 该怎么处理"不再适用
   * （见 {@link KVCache} 的异常口径第 1 条）。本判断同样<b>优先于</b>
   * "v 为 null 时返回 false"这条 {@code KVCache} 契约。
   */
  @Override
  public boolean setnx(K k, V v) {
    throw new UnsupportedOperationException(
            "LruKKVCache 不支持 setnx：本实现的写入要过 LRU-K 的准入闸门"
                    + "（攒够 threshold=" + threshold + " 次访问才落地），"
                    + "而 setnx 的语义是'不存在就写、写了就有'，两者对不上。"
                    + "要'写过就进'的语义请改用 LruKVCache。");
  }

  /**
   * <b>不支持，一律抛出 {@link UnsupportedOperationException}。</b>
   * <p>
   * 拒绝的理由与 {@link #setnx(Object, Object)} 完全相同：本方法的写动作最终也是 {@code set}，
   * 同样要过准入闸门，结果可能并不落在缓存里。{@code computeIfAbsent} 的约定是"返回缓存中的
   * 值"，返回值因此不可信；而 {@code null} 的含义已经用满（表示"算出来是 null、没有写入"），
   * 不能再兼任"写进去了但没落地"。需要"回源 + 写入"时请用 {@code EvictableKVCacheWrapper}
   * 那条 {@code setnx}（它有"值是否实际进入缓存"的判定），或者改用 {@link LruKVCache}。
   * <p>
   * 参数不校验：key 与 {@code mappingFunction} 为 null 时同样抛此异常 —— 整个操作都不支持，
   * 参数语义不再适用（见 {@link KVCache} 的异常口径第 1 条）。
   */
  @Override
  public V computeIfAbsent(K k, Function<? super K, ? extends V> mappingFunction) {
    throw new UnsupportedOperationException(
            "LruKKVCache 不支持 computeIfAbsent：本实现的写入要过 LRU-K 的准入闸门"
                    + "（攒够 threshold=" + threshold + " 次访问才落地），"
                    + "算出来的值可能压根没进缓存，返回值因此不可信；"
                    + "而 null 在本方法上已经表示'算出来是 null'，不能再兼任'没落地'。"
                    + "要'写过就进'的语义请改用 LruKVCache。");
  }

  /**
   * 显式删除需要连同访问历史一并清除。与 {@link #clearKeyStatistic} 的"保留进度"不同：
   * 调用方已明确要求删除，不应残留中间状态。
   */
  @Override
  public boolean delete(K k) {
    // 入口就先拦 null key，见 LruKVCache#delete 上的说明
    KVCache.assertKeyNotNull(k);
    boolean delete = super.delete(k);
    visitorCount.delete(k);
    return delete;
  }

  /**
   * 取值并删除，<b>不经过 {@link #get(Object)}</b>。
   * <p>
   * 不能沿用 {@link KVCache#remove(Object)} 的默认实现（那是"get 后再 delete"）：本类的
   * {@code get} 命不命中都<b>给该 key 记账</b>，而账本自身有容量上限。默认实现里这次白做的读
   * 会在账本恰好满时挤掉队头那条记录 —— 那是<b>另一个 key 攒了一半的准入进度</b>，
   * 随后 {@code delete(k)} 只清 k 自己那份。
   * <p>
   * 取值因此直接读底层存储：删除本身就会清掉全部计数（见 {@link #delete(Object)}）。
   */
  @Override
  public V remove(K k) {
    KVCache.assertKeyNotNull(k);
    V v = getKvCacheDelegate().get(k);
    delete(k);
    return v;
  }

}
