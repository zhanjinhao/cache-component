package cn.addenda.component.cache.eviction.lru;

import cn.addenda.component.cache.KVCache;
import lombok.*;

/**
 * 分代 LRU 缓存实现（对应 MySQL 淘汰页所使用的 LRU 实现）。
 * <p>
 * <b>继承的规则</b>：淘汰机制（队列结构、数据层与队列分工、线程安全口径等）全部来自
 * {@link LruKVCache}。
 * <p>
 * <b>自身差异</b>：两个代各自持有独立的容量与队列 —— <b>老年代</b>
 * （{@link #oldGenerationKVCache}，容量由构造时传入的老年代对象自身决定）存放低频数据，
 * <b>所有新写入的 key 均先落入这里</b>，对应 MySQL 的 mid-point insertion；<b>年轻代</b>
 * （{@link LruKVCache} 继承给本对象的那份队列与底层存储，容量 {@code newCapacity}）存放热数据，
 * <b>只能通过提升进入</b>，没有直接的写入入口。
 * <p>
 * <b>提升规则（时间闸门）</b>：读到老年代中的 k 时，检查其<b>入队</b>至今是否达到
 * {@link #threshold}；达到则从老年代移除并写入年轻代，未达到则<b>不执行任何操作</b>
 * （既不提升，也不修改其入队时刻）。因此 {@link Entry#enterTm} 仅在入队时赋值一次。
 * <p>
 * <b>一个 key 同一时刻只存在于一代中</b>，提升时先删除后写入保证了这一点。两代之间没有数据
 * 共享：{@link #size()} 为两代之和，老年代自身淘汰（按其自身容量）不会连带清理年轻代，
 * 只有 {@link #delete(Object)} 会同时清理两代。
 * <p>
 * <b>安全级别</b>：无，见 {@link cn.addenda.component.cache.eviction.EvictableKVCacheWrapper}。
 *
 * @author addenda
 * @since 2023/05/30x
 */
@ToString
public abstract class LruGenerationKVCache<K, V> extends LruKVCache<K, V> {

  /**
   * 年轻代容量。
   * <p>
   * 父类构造器已将其用作容量（{@code super(newCapacity, ...)}）；此处额外保存一份，用于在
   * {@link #promoteToYoungGeneration} 中判断年轻代是否已满 —— {@link #capacity()} 已被重写为
   * "两代之和"，不能再用作年轻代的容量。
   */
  private final int newCapacity;
  /**
   * 老年代的观察期（毫秒）：key 进入老年代后，需经过该时长（<b>含</b>）再被读到才具备
   * 提升到年轻代的资格。
   * <p>
   * 取值 0 表示任何一次读取都视为合格（<b>含同一毫秒</b>）；取值极大时条件
   * {@code 入队至今 >= threshold} 实际不会成立，等价于关闭提升。要阻挡报表或全表扫描，该值
   * 应<b>不小于</b>"一次扫描中同一个 key 从进入老年代、到它在该次扫描里最后一次被读到的整个
   * 时长" —— 比较的是"入队至今"，而不是相邻两次重复访问之间的间隔。
   */
  private final long threshold;
  /**
   * 老年代：用于存储低频数据
   */
  private final LruKVCache<K, Entry<V>> oldGenerationKVCache;

  /**
   * 年轻代的存储与整个老年代都由子类指定，因此本构造器只对子类开放；调用方应使用具体子类
   * （{@link LruGenerationHashMapKVCache}）。
   *
   * @param newCapacity          年轻代容量
   * @param kvCacheDelegate      年轻代的存储
   * @param oldGenerationKVCache 老年代（自身是一个 LRU），其容量即老年代容量，
   *                             见 {@link #capacity()}
   * @param threshold            观察期（毫秒），见 {@link #getWhenNonContainsKey}
   */
  protected LruGenerationKVCache(int newCapacity, KVCache<K, V> kvCacheDelegate,
                                 LruKVCache<K, Entry<V>> oldGenerationKVCache, int threshold) {
    super(newCapacity, kvCacheDelegate);
    assertThreshold(threshold);
    assertCapacitySum(newCapacity, oldGenerationKVCache);
    this.newCapacity = newCapacity;
    this.threshold = threshold;
    this.oldGenerationKVCache = oldGenerationKVCache;
  }

  /**
   * 供子类替换年轻代的队列实现，原因见 {@link LruKVCache#LruKVCache(int, KVCache, LruDeque)}
   */
  protected LruGenerationKVCache(LruDeque<K> lruDeque, int newCapacity, KVCache<K, V> kvCacheDelegate,
                                 LruKVCache<K, Entry<V>> oldGenerationKVCache, int threshold) {
    super(newCapacity, kvCacheDelegate, lruDeque);
    assertThreshold(threshold);
    assertCapacitySum(newCapacity, oldGenerationKVCache);
    this.newCapacity = newCapacity;
    this.threshold = threshold;
    this.oldGenerationKVCache = oldGenerationKVCache;
  }

  /**
   * 两代容量之和不得超过 {@link KVCache#UNLIMITED_CAPACITY}。
   * <p>
   * {@link #capacity()} 报的是<b>两代之和</b>，它就是本实例的容量，因此同样受该值域约定
   * （{@code 0 <= size() <= capacity() <= UNLIMITED_CAPACITY}）约束 —— 单代由
   * {@code EvictableKVCacheWrapper} 把关，这里补的是"单代都合法、和却超了"这一种。
   * 比较前先转 {@code long}：不预先假设上限是多少（它可能被调低或来自配置）。
   * <p>
   * 参数顺序上排在 {@code super(...)} 之后（Java 要求），但仍在任何字段赋值之前 ——
   * 异常抛出时对象不会外泄。
   */
  private static void assertCapacitySum(int newCapacity, LruKVCache<?, ?> oldGenerationKVCache) {
    int oldCapacity = oldGenerationKVCache.capacity();
    if ((long) newCapacity + oldCapacity > KVCache.UNLIMITED_CAPACITY) {
      throw new IllegalArgumentException(
              "两代容量之和不允许超过 " + KVCache.UNLIMITED_CAPACITY
                      + "（KVCache.UNLIMITED_CAPACITY）：newCapacity=[" + newCapacity
                      + "]，oldCapacity=[" + oldCapacity + "]。");
    }
  }

  /**
   * {@code threshold}（观察期，毫秒）的定义域是 {@code [0, +∞)}。
   * <p>
   * 0 是合法取值且有文档含义："任何一次读取都视为合格（含同一毫秒）"（见 {@link #threshold}），
   * 现有调用方也在用它。负数落在定义域外：观察期是一个<b>时长</b>，负时长没有含义；它也不是
   * "关闭提升"的写法（那要用一个极大的正数），按笔误处理、构造时就报错。
   */
  private static void assertThreshold(int threshold) {
    if (threshold < 0) {
      throw new IllegalArgumentException(
              "threshold(观察期) 需要 >= 0；当前值：" + threshold
                      + "。0 表示\"任何一次读取都视为合格（含同一毫秒）\"；要关闭提升请用一个极大的正数。");
    }
  }

  /**
   * 写入底层存储时一律存入老年代 —— 新 key 始终属于冷数据，入队时刻即为当前时间。
   */
  @Override
  protected void setWhenNonContainsKey(K k, V v) {
    oldGenerationKVCache.set(k, new Entry<>(v, System.currentTimeMillis()));
  }

  /**
   * k 已存在于某一代中，执行覆盖写。
   * <p>
   * 覆盖写<b>不触发提升</b>：本设计中"变热"的触发点是读，而非写，因此老年代中的 key 被重写后
   * 仍保留在老年代（{@code oldGenerationKVCache.set} 会同时将其重新排到老年代队尾）。
   * <p>
   * <b>但入队时刻必须保持不变</b>（含义是"该 key 从何时开始接受观察"，重写仅更新 value）：
   * 若每次重写都将其刷新为当前时间，一个"每隔一段时间被重写一次"的 key 会始终停留在观察期内，
   * 与热数据同样无法提升。确实需要重新开始观察时会走 {@link #setWhenNonContainsKey} 分支，
   * 自然取得新的时刻。
   */
  @Override
  protected void setWhenContainsKey(K k, V v) {
    Entry<V> inOldGeneration = oldGenerationKVCache.onlyGet(k);
    if (inOldGeneration != null) {
      oldGenerationKVCache.set(k, new Entry<>(v, inOldGeneration.getEnterTm()));
      return;
    }
    // 年轻代中的覆盖写：由父类负责重排到队尾并写入 value
    super.setWhenContainsKey(k, v);
  }

  /**
   * 取值分派：<b>按年轻代口径</b>，而非父类默认的 {@link #containsKey(Object)}（两代口径）。
   * <p>
   * 老年代的 key 不在年轻代的存储里，若按两代口径分派，它会被送到 {@link #getWhenContainsKey}
   * 从年轻代取值，得到 null。分代概念因此完全留在本类内，父类不感知。
   */
  @Override
  public V get(K k) {
    if (!super.containsKey(k)) {
      return getWhenNonContainsKey(k);
    }
    return getWhenContainsKey(k);
  }

  /**
   * 年轻代未命中，转查老年代：
   * <ul>
   *   <li>老年代也不存在：返回 null；</li>
   *   <li>存在，且 {@code 入队至今 &gt;= threshold}：已达到观察期，提升到年轻代
   *       （从老年代移除，写入年轻代）；</li>
   *   <li>存在，但仍在观察期内：<b>不执行任何操作</b>。既不提升，也不修改
   *       {@link Entry#enterTm}，因此窗口不会被本次访问推后。</li>
   * </ul>
   * 无论走哪条分支，返回的都是老年代中的 value。
   */
  @Override
  protected V getWhenNonContainsKey(K k) {
    Entry<V> entry = oldGenerationKVCache.onlyGet(k);
    if (entry == null) {
      return null;
    }

    if (System.currentTimeMillis() - entry.getEnterTm() >= threshold) {
      oldGenerationKVCache.delete(k);
      promoteToYoungGeneration(k, entry.getV());
    }
    return entry.getV();
  }

  /**
   * 将一个已达到观察期的老年代条目写入年轻代。
   * <p>
   * <b>本动作不能通过 {@link #set(Object, Object)} 完成</b>：本方法被调用时 k 在两代中都不
   * 存在，{@code set} 会路由到 {@link #setWhenNonContainsKey} 把它又写回老年代，提升不会发生。
   * 父类的两个写入钩子也不能直接使用 —— {@code setWhenNonContainsKey} 以 {@link #capacity()}
   * （两代之和）判断是否已满，{@code setWhenContainsKey} 不含容量检查，而提升不是覆盖写。
   * <p>
   * 因此这里手工执行三步：年轻代已达 {@code newCapacity} 时先淘汰其自身队头（走
   * {@link #delete(Object)} 而非 {@code clearKeyStatistic}，因为需要连同底层存储一起清除）；
   * 将 k 追加到年轻代队尾；用父类的 {@code onlySet} 写入底层存储（队列已在上一行处理完毕）。
   */
  private void promoteToYoungGeneration(K k, V v) {
    // 用 >= 而不是 ==，理由同 LruKVCache#setWhenNonContainsKey：
    // 年轻代队列可能因补登记外部写入的 key 而超过 newCapacity，那时 == 永不成立。
    while (newCapacity <= getLruDeque().size()) {
      K oldest = getLruDeque().getFirst();
      if (oldest == null) {
        break;
      }
      delete(oldest);
    }
    getLruDeque().addLast(k);
    onlySet(k, v);
  }

  /**
   * 同时检查两代。
   * <p>
   * {@code super.containsKey} 只覆盖年轻代，因此这里以 {@code ||} 补充老年代；短路求值保证
   * 年轻代命中时不会去查老年代。本方法只承担<b>对外查询</b>：{@link #get(Object)} 已由本类
   * 重写为按年轻代口径分派，不经过本方法。
   */
  @Override
  public boolean containsKey(K k) {
    KVCache.assertKeyNotNull(k);
    return super.containsKey(k) || oldGenerationKVCache.containsKey(k);
  }

  /**
   * 同时删除两代，返回值取两代结果的并集。
   */
  @Override
  public boolean delete(K k) {
    KVCache.assertKeyNotNull(k);
    boolean deletedFromOld = oldGenerationKVCache.delete(k);
    boolean deletedFromYoung = super.delete(k);
    return deletedFromOld || deletedFromYoung;
  }

  /**
   * 取值并删除，<b>不经过 {@link #get(Object)}</b>。
   * <p>
   * 不能沿用 {@link KVCache#remove(Object)} 的默认实现（那是"get 后再 delete"）：本类的
   * {@code get} 命中老年代且已过观察期时会<b>提升</b>该 key，而提升要先淘汰年轻代的队头 ——
   * 紧接着这里就把同一个 key 删掉了，提升纯属白做，<b>被它挤掉的另一个 key 却回不来</b>。
   * <p>
   * 取值因此走两条"只读底层、不操作队列"的旁路（见 {@link LruKVCache#onlyGet(Object)}）：
   * k 在老年代时读老年代那份，否则读年轻代的底层存储 —— 两代互斥，读到的就是 k 的值。
   */
  @Override
  public V remove(K k) {
    KVCache.assertKeyNotNull(k);
    Entry<V> inOldGeneration = oldGenerationKVCache.onlyGet(k);
    V v = inOldGeneration != null ? inOldGeneration.getV() : onlyGet(k);
    delete(k);
    return v;
  }

  /**
   * @return 两代之和。注意新写入的 key 都先落老年代，因此"刚写入 N 条"能存活的最多为
   * 老年代容量的条数；年轻代需通过提升才会被填充
   * <p>
   * 直接相加即可：两代<i>容量</i>之和已在构造时校验为不超过
   * {@link cn.addenda.component.cache.KVCache#UNLIMITED_CAPACITY}
   * （见 {@link #assertCapacitySum}），因此不会回绕成负数。
   */
  @Override
  public int size() {
    return super.size() + oldGenerationKVCache.size();
  }

  /**
   * @return 两代容量之和
   * <p>
   * 直接相加即可：两代之和已在构造时校验为不超过
   * {@link cn.addenda.component.cache.KVCache#UNLIMITED_CAPACITY}
   * （见 {@link #assertCapacitySum}），所以这里既不需要饱和、也不会回绕成负数。
   */
  @Override
  public int capacity() {
    return newCapacity + oldGenerationKVCache.capacity();
  }

  /**
   * 老年代中的一个条目：value 与入队时刻。
   * <p>
   * <b>不提供 setter 是有意为之</b>：{@link #enterTm} 自入队起即固定，任何读写都不应修改。
   */
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class Entry<V> {
    private V v;
    /**
     * 进入老年代的毫秒时刻，只赋值一次
     */
    private long enterTm;
  }

}
