package cn.addenda.component.cache.eviction.lfu;

import cn.addenda.component.cache.CacheUnsupportedOperationException;
import cn.addenda.component.cache.HashMapKVCache;
import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.KVCacheWrapper;
import cn.addenda.component.cache.SortedKVCache;
import cn.addenda.component.cache.TreeMapKVCache;
import cn.addenda.component.cache.eviction.EvictableKVCacheWrapper;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * LFU 淘汰策略的抽象基类，基于两张哈希表构建。底层存储由子类决定，见
 * {@link LfuHashMapKVCache}、{@link LfuLruKVCache}；包装已有或自定义存储时继承本类，
 * 见受保护的构造器。
 * <p>
 * <b>两张表</b>：{@link #keyToVisitorCount}（key → 访问次数）与 {@link #visitorCountToKeySet}
 * （访问次数 → 该档全部 key，用 {@code SortedKVCache} 以 O(log n) 取最小档，档内用
 * {@code LinkedHashSet} 按进入先后排列）。淘汰取"次数最小"档中"最旧"的 key，同档退化为 FIFO。
 * <p>
 * <b>两张表必须始终同步</b>：{@link #calculateKeyUsage} 负责移档、{@link #clearKeyStatistic}
 * 负责摘除。<b>数据层是唯一真相</b>，两张表只是它的派生视图，两者可能短期不一致（外部写入、
 * 数据层自行删除）；不一致时淘汰清到账本低于容量为止，全程不抛异常。
 * <p>
 * <b>不变量</b>：底层每个 key 都必须在 {@code keyToVisitorCount} 中有计数 ——
 * {@link #calculateKeyUsage} 对"命中但无计数"的 key 按 0 次补齐（外部写入即由此对齐）。
 * <p>
 * <b>已知局限</b>：次数只增不减，高分的历史 key 会长期占位；覆盖写默认也计分（见
 * {@link #countWriteAsVisit}）。前者见 {@link LfuAgingKVCache}，后者把该开关置 false 即可。
 * <p>
 * <b>安全级别</b>：无，见 {@link cn.addenda.component.cache.eviction.EvictableKVCacheWrapper}。
 * 需要并发正确性时使用 {@link cn.addenda.component.cache.SynchronizedKVCache} 包装。
 *
 * @author addenda
 * @since 2023/05/30
 */
public abstract class LfuKVCache<K, V> extends EvictableKVCacheWrapper<K, V> {
  /**
   * key → 访问次数
   */
  private final KVCache<K, Long> keyToVisitorCount;
  /**
   * 访问次数 → 该档全部 key
   */
  private final SortedKVCache<Long, LinkedHashSet<K>> visitorCountToKeySet;
  /**
   * 覆盖写是否计为一次访问。{@code true}（默认）：写与读等价，都使次数 +1；
   * {@code false}：写只更新 value、不推进分数，新 key 仍以 0 次入档。
   * <p>
   * 开关存在的理由：读的分数增长必须由外部请求驱动，写的分数增长只要调用方自己反复
   * {@link #set} 就能无成本地刷高 —— 在写穿透、幂等重写等"写而不读"的用法下会挤走真正有
   * 读需求的数据，这类用法应设为 {@code false}。
   */
  private final boolean countWriteAsVisit;

  /**
   * 覆盖写是否计为一次访问，取值来自构造参数。
   */
  public boolean isCountWriteAsVisit() {
    return countWriteAsVisit;
  }

  /**
   * 计数表的<b>活对象</b>，供本类与子类读写。不对外：与 {@link #getKeyToVisitorCount()}
   * 是同一份数据，交出去等于把账本交出去；子类需要它，见 {@link LfuAgingKVCache}。
   */
  protected KVCache<K, Long> keyToVisitorCount() {
    return keyToVisitorCount;
  }

  /**
   * 档位表的<b>活对象</b>，见 {@link #keyToVisitorCount()}。
   */
  protected SortedKVCache<Long, LinkedHashSet<K>> visitorCountToKeySet() {
    return visitorCountToKeySet;
  }

  /**
   * 计数表的<b>只读观测入口</b>：读操作实时转发到活账本；
   * {@code set} / {@code setnx} / {@code delete} / {@code computeIfAbsent} / {@code remove}
   * 一律抛 {@link CacheUnsupportedOperationException}。五个方法都<b>不校验参数</b>
   * （无条件拒绝，见 {@code KVCache} 的异常口径第 1 条）；{@code remove} 也显式覆盖了，
   * 否则接口默认实现会在入口先校验 key，{@code remove(null)} 就变成
   * {@link CacheIllegalArgumentException}（参数错误），与其它四个方法的口径不一致。
   * <p>
   * 返回外壳而非拷贝：{@link KVCache} 没有 keySet、这张表枚举不出来，且拷贝只是某一瞬间的
   * 数据。外壳零拷贝、实时，并把误写变成显式失败。
   */
  public KVCache<K, Long> getKeyToVisitorCount() {
    return new ReadOnlyKVCache<>(keyToVisitorCount);
  }

  /**
   * 档位表的<b>只读观测入口</b>，见 {@link #getKeyToVisitorCount()}。
   * <p>
   * 读方法另防两处：{@code keySet()} 返回快照，{@code get(count)} 返回集合副本
   * （否则拿到 {@code LinkedHashSet} 后一个 {@code clear()} 就能清空整个档位）。
   */
  public SortedKVCache<Long, LinkedHashSet<K>> getVisitorCountToKeySet() {
    return new ReadOnlySortedKVCache<Long, K>(visitorCountToKeySet);
  }

  /**
   * 底层存储由子类指定，只对子类开放；调用方应使用具体子类
   * （{@link LfuHashMapKVCache} / {@link LfuLruKVCache}）。
   */
  protected LfuKVCache(int capacity, KVCache<K, V> kvCacheDelegate) {
    this(capacity, true, kvCacheDelegate);
  }

  /**
   * 底层存储由子类指定，只对子类开放；调用方应使用具体子类
   * （{@link LfuHashMapKVCache} / {@link LfuLruKVCache}）。
   *
   * @param countWriteAsVisit 覆盖写是否计为一次访问，见 {@link #countWriteAsVisit}
   */
  protected LfuKVCache(int capacity, boolean countWriteAsVisit, KVCache<K, V> kvCacheDelegate) {
    this(new HashMapKVCache<>(), new TreeMapKVCache<>(),
            capacity, countWriteAsVisit, kvCacheDelegate);
  }

  /**
   * 供子类替换两张统计结构的实现。只对子类开放：两张表必须始终相互同步（见类注释），
   * 外部注入的结构是否满足该前提无法在构造时校验，一旦不满足即为静默的容量判断错乱。
   *
   * @param countWriteAsVisit 覆盖写是否计为一次访问，见 {@link #countWriteAsVisit}
   */
  protected LfuKVCache(KVCache<K, Long> keyToVisitorCount,
                       SortedKVCache<Long, LinkedHashSet<K>> visitorCountToKeySet,
                       int capacity, boolean countWriteAsVisit, KVCache<K, V> kvCacheDelegate) {
    super(capacity, kvCacheDelegate);
    this.keyToVisitorCount = keyToVisitorCount;
    this.visitorCountToKeySet = visitorCountToKeySet;
    this.countWriteAsVisit = countWriteAsVisit;
  }

  /**
   * k 已存在：按 {@link #countWriteAsVisit} 决定是否记一次访问，再写入 value。
   * 计分时"写"与"读"等价（都加 1）；不计分时写只更新 value，不影响分数与档位。
   */
  @Override
  protected void setWhenContainsKey(K k, V v) {
    if (countWriteAsVisit) {
      calculateKeyUsage(k);
    }
    getKvCacheDelegate().set(k, v);
  }

  /**
   * k 不存在：若<b>本层账本</b>已满则先淘汰一个，再以 0 次访问入档、写入 value。
   * <p>
   * 触发条件用账本自己的条数（{@code keyToVisitorCount.size()}）而不是 {@link #size()}
   * （后者取自数据层）—— "我这一层要不要腾位置"是账本自己的事。
   * <p>
   * 挑出的 key 可能已被数据层自行删除，{@link #delete} 只清残留、不腾位置，因此本次写入
   * 可能<b>暂时</b>超出容量，下一次写继续追；这也是此处不抛异常的前提。新 key 从 0 次起步、
   * 必然落在最小档，容量满时刚写入的数据即为下一个淘汰候选。
   */
  @Override
  protected void setWhenNonContainsKey(K k, V v) {
    // 用 >= 而非 ==：账本可能因补齐（见 calculateKeyUsage）而超过容量，那时 == 永不成立、淘汰失效。
    // 第二个条件兜住档位表为空的情况，否则 removeMinOldest 空转会让循环停不下来。
    while (capacity() <= keyToVisitorCount.size() && visitorCountToKeySet.size() > 0) {
      removeMinOldest();
    }

    keyToVisitorCount.set(k, 0L);
    visitorCountToKeySet.computeIfAbsent(0L, s -> new LinkedHashSet<>()).add(k);
    getKvCacheDelegate().set(k, v);
  }

  /**
   * 淘汰访问次数最少的那一档中最旧的一个 key；同档由 {@code LinkedHashSet} 的插入顺序决定
   * 先后，即退化为 FIFO。档位表为空时直接返回。
   * <p>
   * <b>本方法必须"腾出进度"</b>：每调用一次档位表条数必然严格下降，这是调用方
   * {@link #setWhenNonContainsKey} 的 while 循环不退化成死循环的前提 —— 因此任何提前返回的
   * 分支都必须先把取不到 key 的那个档位删掉。两张表短期失步时只清残留、不抛异常。
   * <p>
   * <b>返回值回答的是"这次淘汰是不是空转"</b>：返回被淘汰掉的 value，{@code null} 表示什么都
   * 没淘汰到（账本为空、只清了残留、或数据层本来就没有该 key）—— 判据可靠，因为数据层不存 null。
   * 供子类重写，见 {@link LfuAgingKVCache}。
   *
   * @return 被淘汰掉的 value；空转时返回 null
   */
  protected V removeMinOldest() {
    if (visitorCountToKeySet.size() == 0) {
      return null;
    }

    Long minVisitorCount = visitorCountToKeySet.getFirst();
    LinkedHashSet<K> keyList = visitorCountToKeySet.get(minVisitorCount);
    if (keyList == null || keyList.isEmpty()) {
      // 最小档取不到 key：两表短期失步或残留空集合。这里必须先删掉空档再返回，
      // 否则调用方 while 的条件完全没变，会空转成死循环；删掉后条数严格下降，循环必然退出。
      visitorCountToKeySet.delete(minVisitorCount);
      return null;
    }

    K oldest = keyList.iterator().next();
    keyList.remove(oldest);
    if (keyList.isEmpty()) {
      visitorCountToKeySet.delete(minVisitorCount);
    }

    keyToVisitorCount.delete(oldest);
    // 账本残留照样要清（不清容量就失效）；用 remove 而非 delete：一次调用同时拿到"值"
    // 和"数据层有没有这条数据"（返回 null 即没有），即本方法的返回值。
    return getKvCacheDelegate().remove(oldest);
  }

  @Override
  protected V getWhenNonContainsKey(K k) {
    return null;
  }

  /**
   * 命中：访问次数加 1，再取 value。
   */
  @Override
  protected V getWhenContainsKey(K k) {
    calculateKeyUsage(k);
    return getKvCacheDelegate().get(k);
  }

  /**
   * 将 k 从"访问次数 = n"档移动到"访问次数 = n + 1"档。
   * <p>
   * k 通常已由 {@link #setWhenNonContainsKey} 登记；对 delegate 自带数据（包装一个已有内容的
   * 缓存）而计数表中没有的 k，在此按"以 0 次入档"补齐，避免 {@code visitorCount + 1} 拆箱时
   * 抛 NPE。旧档中最后一个 key 被移出时删除整个档，避免残留空集合影响 {@code getFirst()}。
   */
  private void calculateKeyUsage(K k) {
    Long visitorCount = keyToVisitorCount.get(k);
    if (visitorCount == null) {
      keyToVisitorCount.set(k, 0L);
      visitorCountToKeySet.computeIfAbsent(0L, s -> new LinkedHashSet<>()).add(k);
      visitorCount = 0L;
    }
    keyToVisitorCount.set(k, visitorCount + 1);

    LinkedHashSet<K> keyList = visitorCountToKeySet.get(visitorCount);
    if (keyList == null) {
      // 同上的例外情况：两表不一致时补齐档位，避免 NPE
      keyList = new LinkedHashSet<>();
      visitorCountToKeySet.set(visitorCount, keyList);
    }
    keyList.remove(k);
    if (keyList.isEmpty()) {
      visitorCountToKeySet.delete(visitorCount);
    }

    visitorCountToKeySet.computeIfAbsent(visitorCount + 1, s -> new LinkedHashSet<>()).add(k);
  }

  /**
   * 摘除 k 在两个档位结构中的记录。
   * <p>
   * 必须先查 {@code keyToVisitorCount} 再删除 —— 访问次数是定位档位的依据。计数为 null
   * 表示 k 从未入档，直接返回（供 {@code EvictableKVCache#containsKey} 的自愈路径使用）；
   * 两张表短期失步时档位查不到也不抛异常，那条路径的职责正是把不一致修回来。
   */
  @Override
  protected void clearKeyStatistic(K k) {
    Long visitorCount = keyToVisitorCount.get(k);
    if (visitorCount != null) {
      keyToVisitorCount.delete(k);
      LinkedHashSet<K> keyList = visitorCountToKeySet.get(visitorCount);
      // 两表可能短期失步，档位表里已找不到该档（同 calculateKeyUsage 的兜底）。
      // 计数已删，档位没什么可清，但不能抛异常 —— 本方法还会被自愈路径调用。
      if (keyList != null) {
        keyList.remove(k);
        if (keyList.isEmpty()) {
          visitorCountToKeySet.delete(visitorCount);
        }
      }
    }
  }

  @Override
  public boolean delete(K k) {
    // 在入口处拦截 null key，见 LruKVCache#delete 上的说明
    KVCache.assertKeyNotNull(k);
    clearKeyStatistic(k);

    return getKvCacheDelegate().delete(k);
  }

  /**
   * 只读的 {@link KVCache}：读操作转发给被包装的 cache，写操作一律抛
   * {@link CacheUnsupportedOperationException}。用于把自己的内部结构交出去给别人看时防止被改。
   * <p>
   * 只读范围：{@link #set} / {@link #setnx} / {@link #delete} / {@link #computeIfAbsent} /
   * {@link #remove} 直接抛；{@link #get} / {@link #containsKey} / {@link #size} /
   * {@link #capacity} 原样转发。
   * <p>
   * 不复制数据，读操作实时转发到被包装的 cache。<b>值本身是否可变不在防护范围内</b>：
   * 被包装的 cache 若以可变对象为值，调用方仍可通过 {@link #get} 拿到内部对象并修改它。
   *
   * @param <K> key 类型
   * @param <V> value 类型
   */
  private static class ReadOnlyKVCache<K, V> extends KVCacheWrapper<K, V> {

    private ReadOnlyKVCache(KVCache<K, V> kvCacheDelegate) {
      super(kvCacheDelegate);
    }

    @Override
    public boolean containsKey(K k) {
      return getKvCacheDelegate().containsKey(k);
    }

    @Override
    public V get(K k) {
      return getKvCacheDelegate().get(k);
    }

    @Override
    public int capacity() {
      return getKvCacheDelegate().capacity();
    }

    @Override
    public void set(K k, V v) {
      throw readOnly();
    }

    @Override
    public boolean setnx(K k, V v) {
      throw readOnly();
    }

    @Override
    public boolean delete(K k) {
      throw readOnly();
    }

    /**
     * <b>一并覆盖成无条件抛</b>：接口默认实现是"get 后 delete"，它会在入口先校验 key，
     * 于是 {@code remove(null)} 抛 {@link CacheIllegalArgumentException}（key 为 null）、
     * {@code delete(null)} 抛 {@link CacheUnsupportedOperationException}（只读）—— 同一个只读外壳上
     * 出现两种口径。无条件拒绝的操作一律不校验参数，见 {@link KVCache} 的异常口径第 1 条。
     */
    @Override
    public V remove(K k) {
      throw readOnly();
    }

    /**
     * <b>一律抛出，不沿用接口默认实现</b>：默认实现只在 key 不存在时才写入，命中时它读到值就
     * 直接返回，同一个方法会随 key 在不在而时灵时不灵。只读外壳的写入口就该一律失败，
     * 与上面三个方法同口径。参数不校验 —— 整个操作都不支持时，"null 怎么处理"不再适用。
     */
    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      throw readOnly();
    }

    // size() 由 KVCacheWrapper 转发；remove 见上面的显式覆盖。

    private CacheUnsupportedOperationException readOnly() {
      return new CacheUnsupportedOperationException(
              "只读的 KVCache：本对象只提供读操作。");
    }

  }

  /**
   * 只读的 {@link SortedKVCache}，见 {@link ReadOnlyKVCache}。
   * <p>
   * 比父类多堵一处"读操作也能改数据"：这类表的值本身可变（{@link LinkedHashSet}），只禁表的
   * 写方法挡不住 {@code get(k).clear()}。因此 {@link #get(Object)} 返回<b>副本</b>、
   * {@link #keySet()} 返回<b>快照</b>并包成不可写（底层 {@code TreeMap.keySet()} 是活视图）。
   * <p>
   * 两个类型参数不能合并：表的键与集合元素不一定是同一个类型 ——
   * 例如"次数 → 一组 key"这类表，键是次数、元素是 key。
   *
   * @param <C> 表的键类型
   * @param <E> 集合元素类型
   */
  private static class ReadOnlySortedKVCache<C, E> extends ReadOnlyKVCache<C, LinkedHashSet<E>>
          implements SortedKVCache<C, LinkedHashSet<E>> {

    private ReadOnlySortedKVCache(SortedKVCache<C, LinkedHashSet<E>> kvCacheDelegate) {
      super(kvCacheDelegate);
    }

    @Override
    public C getFirst() {
      return sortedDelegate().getFirst();
    }

    @Override
    public C getLast() {
      return sortedDelegate().getLast();
    }

    @Override
    public Set<C> keySet() {
      return new ReadOnlySet<>(new LinkedHashSet<>(sortedDelegate().keySet()));
    }

    /**
     * @return 该键下全部元素的<b>副本</b>；键不存在时返回 null
     */
    @Override
    public LinkedHashSet<E> get(C c) {
      LinkedHashSet<E> values = sortedDelegate().get(c);
      return values == null ? null : new LinkedHashSet<>(values);
    }

    private SortedKVCache<C, LinkedHashSet<E>> sortedDelegate() {
      return (SortedKVCache<C, LinkedHashSet<E>>) getKvCacheDelegate();
    }

  }

  /**
   * 只读 {@link Set}：{@link java.util.Collections#unmodifiableSet} 的写方法抛的是 JDK
   * {@code UnsupportedOperationException}，与本组件"拒绝一律用
   * {@link CacheUnsupportedOperationException}"的口径不一致。这里把所有写入口（含
   * {@code iterator().remove()}）换成该类型。
   *
   * @param <E> 元素类型
   */
  private static class ReadOnlySet<E> extends AbstractSet<E> {

    private final Set<E> delegate;

    private ReadOnlySet(Set<E> delegate) {
      this.delegate = delegate;
    }

    @Override
    public Iterator<E> iterator() {
      Iterator<E> iterator = delegate.iterator();
      return new Iterator<E>() {
        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public E next() {
          return iterator.next();
        }

        @Override
        public void remove() {
          throw readOnly();
        }
      };
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public boolean contains(Object o) {
      return delegate.contains(o);
    }

    @Override
    public boolean add(E e) {
      throw readOnly();
    }

    @Override
    public boolean remove(Object o) {
      throw readOnly();
    }

    @Override
    public void clear() {
      throw readOnly();
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
      throw readOnly();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw readOnly();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw readOnly();
    }

    private static CacheUnsupportedOperationException readOnly() {
      return new CacheUnsupportedOperationException("只读的统计表视图：本对象只提供读操作。");
    }

  }

}
