package cn.addenda.component.cache.eviction.lru;

import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.eviction.EvictableKVCacheWrapper;

/**
 * LRU 淘汰策略的抽象基类，由队列与底层存储两部分组成。
 * <p>
 * 本类只承载淘汰策略本身，底层存储由子类决定，见两个构造器；最常用的形态是
 * {@link LruHashMapKVCache}。
 * <p>
 * {@link LruDeque} 中只保存 key，按访问先后排列（队头最旧、队尾最新）；value 保存在底层
 * cache 中。<b>数据层是唯一真相</b>，队列是它的派生视图，只负责给出淘汰顺序；
 * {@link #size()} 与 {@link #containsKey} 都取自数据层。两者可能短期不一致（外部直接写进
 * 数据层的 key 队列不认识，数据层自行删除的 key 队列仍留着），不一致期间淘汰清到队列低于
 * 容量为止，绝不因此抛异常。
 * <p>
 * 读取、以及写入命中已有 key 时，都要先 {@link #delete} 再追加到队尾，见
 * {@link LruDeque#addLast}。
 * <p>
 * <b>安全级别</b>：无。队列与底层 cache 是两块独立状态，无法合并为一条原子命令。
 * 需要并发正确性时使用 {@link cn.addenda.component.cache.SynchronizedKVCache} 包装。
 *
 * @author addenda
 * @since 2023/05/30
 */
public abstract class LruKVCache<K, V> extends EvictableKVCacheWrapper<K, V> {

  /**
   * 记录新旧关系的双向队列：尾节点最新头节点最旧
   */
  private final LruDeque<K> lruDeque;

  /**
   * 底层存储由子类指定，因此本构造器只对子类开放；调用方应使用具体子类
   * （{@link LruHashMapKVCache}），或按需要自行继承。
   */
  protected LruKVCache(int capacity, KVCache<K, V> kvCacheDelegate) {
    super(capacity, kvCacheDelegate);
    this.lruDeque = new LinkedHashSetLruDeque<>();
  }

  /**
   * 供子类替换队列实现。
   * <p>
   * 不对外公开：队列与底层 cache 必须严格同步（见类注释），外部传入的队列是否满足该前提
   * 无法在构造时校验。需要换队列的是子类，因此只对子类开放。
   */
  protected LruKVCache(int capacity, KVCache<K, V> kvCacheDelegate, LruDeque<K> lruDeque) {
    super(capacity, kvCacheDelegate);
    this.lruDeque = lruDeque;
  }

  /**
   * k 已存在：将 k 移动到队尾，并更新底层 cache。
   * <p>
   * 必须先 {@link #delete} 再追加队尾，不能简化为 {@code addLast(k)}：
   * {@link LruDeque#addLast} 对已存在的元素不保证移动位置。顺带执行的 delete 也会同步
   * 子类的统计结构，见 {@link LruKKVCache}。
   */
  @Override
  protected void setWhenContainsKey(K k, V v) {
    delete(k);

    lruDeque.addLast(k);
    getKvCacheDelegate().set(k, v);
  }

  /**
   * k 不存在：若<b>本层队列</b>已满则先淘汰，再将 k 追加到队尾并写入底层 cache。
   * <p>
   * 触发条件用队列自己的长度，而不是公开的 {@link #size()}：后者取自数据层（见类注释），
   * 而"我这一层要不要腾位置"是队列自己的事。
   * <p>
   * 队头可能已被数据层自行删除（过期、服务端淘汰）时，{@link #delete} 只把残留的队列项清掉、
   * 不腾位置，本次写入因此可能<b>暂时</b>超出容量。淘汰是<b>循环</b>的（清到队列低于容量）：
   * 队列可能因"补登记外部直接写进数据层的 key"而超过容量，只清一条永远追不回来。
   */
  @Override
  protected void setWhenNonContainsKey(K k, V v) {
    // 用 >= 而不是 ==：补登记外部写入的 key 后队列可能超过容量，== 永不成立就再也不淘汰了
    while (capacity() <= lruDeque.size()) {
      K oldest = lruDeque.getFirst();
      if (oldest == null) {
        // 队列为空却仍满足条件，只可能来自 capacity() 被子类重写成 0/负数，退出避免死循环
        break;
      }
      delete(oldest);
    }

    lruDeque.addLast(k);
    getKvCacheDelegate().set(k, v);
  }

  /**
   * 供子类同步统计结构的钩子：底层 cache 中已不存在 k。
   * <p>
   * {@code EvictableKVCache#containsKey} 发现底层不存在 k 时也会调用本方法，
   * 此时它承担"自愈"职责 —— 清除可能残留的统计。
   */
  @Override
  protected void clearKeyStatistic(K k) {
    lruDeque.remove(k);
  }

  /**
   * 如果cache不存在k，返回null;
   */
  @Override
  protected V getWhenNonContainsKey(K k) {
    return null;
  }

  /**
   * 如果cache存在k，返回v，同时将k移至尾节点。
   * <p>
   * 同样是 remove 与 addLast 两步，原因见类注释中对 {@link LruDeque#addLast} 的说明。
   */
  @Override
  protected V getWhenContainsKey(K k) {
    lruDeque.remove(k);
    lruDeque.addLast(k);
    return getKvCacheDelegate().get(k);
  }

  /**
   * 同时删除底层 cache 与队列中的数据。
   * <p>
   * 先清理统计再删除 value，与 {@code EvictableKVCache#containsKey} 中
   * "发现底层不存在即调用 clearKeyStatistic"的顺序保持一致。
   */
  @Override
  public boolean delete(K k) {
    // 入口处拦 null key：否则会先执行 lruDeque.remove(null)，随后才在底层 cache 上抛出异常
    KVCache.assertKeyNotNull(k);
    clearKeyStatistic(k);
    return getKvCacheDelegate().delete(k);
  }

  /**
   * 队列的<b>活对象</b>，供子类读写（{@link LruKKVCache} 判准入、{@link LruGenerationKVCache}
   * 进出队）。
   * <p>
   * 返回的必须是活对象而非副本：{@code addLast} / {@code remove} 落在副本上会被静默丢弃，
   * 队列停止更新。
   */
  protected LruDeque<K> getLruDeque() {
    return lruDeque;
  }

  /**
   * 供子类调用：直接写入底层 cache，不操作 LRU 队列（即不更新访问顺序）。
   */
  protected void onlySet(K k, V v) {
    getKvCacheDelegate().set(k, v);
  }

  /**
   * 供子类调用：直接读取底层 cache，不操作 LRU 队列（即不更新访问顺序）
   */
  protected V onlyGet(K k) {
    return getKvCacheDelegate().get(k);
  }

}
