package cn.addenda.component.cache.eviction;

import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.KVCacheWrapper;
import cn.addenda.component.cache.eviction.lfu.LfuLruKVCache;
import cn.addenda.component.cache.eviction.lru.LruGenerationKVCache;
import cn.addenda.component.cache.eviction.lru.LruKKVCache;
import cn.addenda.component.cache.eviction.lru.LruKVCache;

/**
 * {@link EvictableKVCache} 的默认实现载体：容量字段与四个模板方法都在这里。
 * <p>
 * 五个钩子保持 {@code protected} —— 它们是模板与实现类之间的协议，<b>不对外使用</b>。
 * 直接调用钩子会绕过模板的判断（容量检查、null 语义、分派与自愈），使统计结构错乱。
 * <p>
 * 本层有两块状态：<b>数据层</b>（{@code delegate} 指向的底层存储）与<b>统计层</b>
 * （各实现自己维护的访问顺序、访问次数等）。<b>数据层是唯一真相</b>：{@code size()} 与
 * {@code containsKey} 都取自它；统计层是数据层的派生视图，只负责挑选淘汰对象，
 * 并靠"下次访问时对齐"逐步追平 —— 数据层多出来的 key 补登记，数据层丢掉的 key 清残留。
 * 两者不一致时淘汰按"清到账本低于容量为止"处理（见各实现的 {@code setWhenNonContainsKey}），
 * <b>绝不抛异常</b>。之所以要清到低于容量而不是只清一条：账本可能因"补登记外部直接写进
 * 数据层的 key"而超过容量，此时只清一条会永远追不回来、容量形同虚设。
 * 这也是本层能架在"服务端会自行删除数据"的存储（如 Redis）之上而不失准的原因。
 * <p>
 * 代价：不一致期间实际条数可能<b>暂时</b>超过 {@code capacity()}（多出的部分来自外部
 * 直接写入数据层的数据，或账本里尚未清理的残留）。
 * <p>
 * 另一处连带：{@code size()} 既然取自数据层，底层不支持计数时它便跟着失败
 * （如基于 redis 的实现，见 {@link KVCache#size()}）；本层不做兜底 —— 账本口径的条数
 * 不是"现在到底有多少数据"，用它顶上只会重新引入那种自相矛盾。
 * <p>
 * <b>两个复合方法（{@code computeIfAbsent}、{@code remove}）本层不实现</b>，走
 * {@link KVCache} 的默认实现 —— 它们的第一步都是 {@link #get(Object)}。因此
 * <b>命中也是一次读</b>：key 已存在时 {@code computeIfAbsent} 直接返回旧值、
 * {@code mappingFunction} 一次都不执行，但它对缓存后续的影响与直接调一次 {@code get}
 * 完全相同（动访问顺序、访问次数；分代实现还会触发提升，而提升要先给年轻代腾位置，
 * 会连带淘汰一条无关数据）。两个方法的非原子性也来自这里。
 * {@link LruKKVCache} 是例外：它拒绝 {@code computeIfAbsent}，走不到这条路径。
 * <p>
 * <b>安全级别</b>：无。本类及其子类持有可变统计结构（访问顺序队列、访问次数表等）且不加锁，
 * 因此即使 delegate 本身线程安全（如基于 Redis 的实现），本层也不承诺 L1 —— 并发访问会
 * 损坏统计结构，进而让淘汰行为错乱。需要并发时把整个淘汰层用
 * {@link cn.addenda.component.cache.SynchronizedKVCache} 或
 * {@link cn.addenda.component.cache.SynchronizedExpiredKVCache} 包一层，
 * 并发正确性由那一层承担。各实现类只声明"安全级别：无，见本类"。
 *
 * @author addenda
 * @since 2023/05/30
 */
public abstract class EvictableKVCacheWrapper<K, V> extends KVCacheWrapper<K, V>
        implements EvictableKVCache<K, V> {
  /**
   * cache的大小
   */
  private final int capacity;

  protected EvictableKVCacheWrapper(int capacity, KVCache<K, V> kvCache) {
    super(kvCache);
    // 容量的值域是 [0, KVCache.UNLIMITED_CAPACITY]（见其说明：所有合法性判断都以它为准，
    // 不要拿 Integer.MAX_VALUE 去比）。构造参数这一层就由这里把关；由容量"组合"出来的
    // 值（两代之和、账本的两倍）不是构造参数，各自在自己的实现里判。
    // 本层额外收紧了 0：容量 N 的含义是"最多留 N 条"，0 会让淘汰逻辑失去意义。
    if (capacity <= 0) {
      throw new IllegalArgumentException("缓存的大小需要 > 0！");
    }
    if (capacity > KVCache.UNLIMITED_CAPACITY) {
      throw new IllegalArgumentException(
              "缓存的大小不允许超过 " + KVCache.UNLIMITED_CAPACITY
                      + "（KVCache.UNLIMITED_CAPACITY）；当前值：[" + capacity + "]。");
    }
    this.capacity = capacity;
  }

  /**
   * 写入缓存。
   * <p>
   * v 为 null 表示"没有值"，按删除处理（见 {@link KVCache#set}）。
   * 该分支不能仅将 null 透传给底层 Cache，实现自己的统计结构必须同步清理
   * （具体的统计结构见各实现）。
   * <p>
   * 若仅透传 null，会产生两种后果：
   * <ul>
   *   <li>统计结构中残留一个没有值的条目 —— 它<b>不会</b>被 {@link #size()} 计入
   *       （{@code size()} 取自数据层），却会被本层的<b>淘汰触发条件</b>当成一条真实数据。
   *       触发条件比的是实现自己账本的条数：账本虚高到恰好等于容量时会提前驱逐仍然存活的数据，
   *       虚高超过容量时那个"相等"条件再也不成立、该实现便不再自行淘汰；</li>
   *   <li>对底层不存在的 key 调用 {@code set(k, null)} 时，会先填满统计结构、
   *       驱逐一条真实数据，随后不写入任何内容。</li>
   * </ul>
   * {@link #delete(Object)} 在各子类中的语义即为"将该 key 从所有结构中移除"，
   * 因此 null 分支直接复用。
   * <p>
   * k 不允许为 null，见 {@link KVCache#assertKeyNotNull(Object)}。
   */
  @Override
  public void set(K k, V v) {
    KVCache.assertKeyNotNull(k);
    if (v == null) {
      delete(k);
      return;
    }
    if (containsKey(k)) {
      setWhenContainsKey(k, v);
      return;
    }
    setWhenNonContainsKey(k, v);
  }

  /**
   * key 不存在时才写入。
   * <p>
   * 必须复用 {@link #set(Object, Object)} 的插入路径，不能把请求直接转给底层 Cache：
   * 淘汰决策与统计登记都实现在 {@code setWhenContainsKey} 与
   * {@code setWhenNonContainsKey} 中，绕过它们会导致淘汰策略的<b>账本</b>漏登记这些 key，
   * "账本条数 == 容量"这个触发条件永不成立，淘汰机制完全失效。
   * 注意 {@link #size()} 看不出这件事 —— 它取自数据层，这些 key 照样算得进去。
   * <p>
   * 返回值口径为"值是否实际进入缓存"：写入完成后通过 {@link #containsKey(Object)}
   * 判断，而不假定"调用 set 必然写入成功"。
   * <p>
   * 本层没有"写入不落地"的实现，因此上面的兜底判断恒为 true；保留它是为了将来出现
   * "写入不落地"的实现时不至于静默返回错误结果。真正"写入不落地"的实现会自行覆盖
   * {@code setnx}：如 {@link LruKKVCache} 明确抛 {@link UnsupportedOperationException}
   * 而非返回一个含义歧义的 false，原因见其类说明。
   * <p>
   * 与底层 Cache 的 {@code setnx} 不同，本方法不保证并发原子性：淘汰策略的统计结构与
   * 底层存储是两块独立状态，无法合并为一条原子命令。本层本就不由实现保证
   * （见 {@link cn.addenda.component.cache.KVCache} 的说明），
   * 需要并发正确性时使用
   * {@link cn.addenda.component.cache.SynchronizedKVCache} 包装。
   *
   * @param k key，不允许为 null
   * @param v value，为 null 时不写入并返回 false
   * @return 值实际进入缓存返回 true；key 已存在、或 v 为 null 时返回 false。
   *         注意"淘汰策略阻止写入"这一类<b>不会</b>返回 false —— 它明确抛异常，见上文
   */
  @Override
  public boolean setnx(K k, V v) {
    KVCache.assertKeyNotNull(k);
    if (v == null) {
      // 见 KVCache#setnx：v 为 null 时表示"没有值"，本方法不写入任何内容
      return false;
    }
    if (containsKey(k)) {
      return false;
    }
    set(k, v);
    return containsKey(k);
  }

  protected abstract void setWhenContainsKey(K k, V v);

  protected abstract void setWhenNonContainsKey(K k, V v);

  /**
   * 读取缓存，按 {@link #containsKey(Object)} 分派。
   * <p>
   * 本层只有这一套存在性口径：{@link #set(Object, Object)}、{@link #setnx(Object, Object)}
   * 与本方法都按同一个谓词决定走哪个分支。
   * <p>
   * 子类重写 {@link #containsKey(Object)} 会同时改变本方法的分派口径。当"缓存中存在"
   * 与"本层这一块存储中存在"不是同一件事时（数据分散在多块存储中），
   * 子类需要自行重写本方法，把分派落回持有该 key 的那一块，见
   * {@link LruGenerationKVCache#get(Object)}。
   */
  @Override
  public V get(K k) {
    if (!containsKey(k)) {
      return getWhenNonContainsKey(k);
    }
    return getWhenContainsKey(k);
  }

  protected abstract V getWhenNonContainsKey(K k);

  protected abstract V getWhenContainsKey(K k);

  /**
   * 判断 key 是否存在于缓存中。
   * <p>
   * 默认实现为"底层存储（{@code delegate}）中存在"。这是本层唯一的存储口径：
   * 淘汰策略的统计数据（访问顺序、访问次数等）不存放在底层 Cache 中，
   * 但数据本身都存放在那里，因此"数据是否存在"等价于"底层存储中是否存在"。
   * <p>
   * <b>本方法具有副作用</b>：底层返回不存在时调用 {@link #clearKeyStatistic}，
   * 承担"自愈"职责 —— 统计结构中可能残留已被淘汰或已过期的 key，
   * 每次发现底层不存在即清理一次，使统计逐步与真实状态一致。
   * <p>
   * 之所以采用"下次访问时自愈"而非"淘汰时即时通知"，是因为底层 Cache 可能自行驱逐 key
   * （如数据过期，或底层存储自身带有淘汰策略时自行触发淘汰，见 {@link LfuLruKVCache}），
   * 没有回调上层统计的途径。
   * <p>
   * 副作用也意味着本方法不能当作纯查询使用：并发场景下先查后改，
   * 自愈的结果未必与后续状态对应。
   * <p>
   * 子类将数据分散在多块存储中时，重写本方法以覆盖全部存储
   * （各块存储的自愈由各自的 {@code containsKey} 负责）；此时
   * {@link #get(Object)} 的分派口径随之改变，需要一并处理，见
   * {@link LruGenerationKVCache}。
   */
  @Override
  public boolean containsKey(K k) {
    KVCache.assertKeyNotNull(k);
    boolean b = getKvCacheDelegate().containsKey(k);
    if (!b) {
      clearKeyStatistic(k);
    }
    return b;
  }

  /**
   * 由实现类将 k 从自身的统计结构中移除。
   * <p>
   * 两条路径会调用本方法：
   * <ul>
   *   <li>{@link #containsKey} 发现底层已不存在 k（自愈）；</li>
   *   <li>实现类的 {@code delete} 删除了对应的值（显式删除）。</li>
   * </ul>
   * 实现必须容忍 k 不在统计结构中：第一条路径可能对任意 key 调用。
   */
  protected abstract void clearKeyStatistic(K k);

  /**
   * 容量的语义是"该 cache 最多能容纳多少条数据"：稳态下实现保证
   * {@code size() <= capacity()}，调用方以此判断是否还能写入。唯一的例外是数据层与账本
   * 不一致的短期窗口（见类注释里的"代价"），那时实际条数可能<b>暂时</b>超过容量。
   * <p>
   * 多数实现返回构造时传入的数值；分代结构的 {@link LruGenerationKVCache} 会重写它，
   * 返回两代容量之和，见其说明。
   * <p>
   * 反过来，<b>实现内部判断自身是否已满时不应使用本方法</b>：它会被子类重写，
   * 取到的可能是"整体容量"而非"当前这块存储的容量"。需要判断"自身是否已满"时应自行持有容量值
   * （{@link LruKVCache} 与 {@link LruGenerationKVCache} 正是因此各自持有容量值，见它们各自的说明）。
   */
  @Override
  public int capacity() {
    return capacity;
  }

}
