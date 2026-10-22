package cn.addenda.component.cache;

import cn.addenda.component.base.pojo.Binary;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 用 {@link ConcurrentMap} 承载"value + 到期时刻"的 {@link ExpiredKVCache} 抽象。
 * <p>
 * <b>自身规则</b>：语义与 {@link ExpiredMapKVCache} 相同 —— TTL 走
 * {@link ExpiredKVCache#ttlMillis} / {@link ExpiredKVCache#deadline}；<b>过期的 key 等同于
 * 没有值</b>，{@link #get(Object)} / {@link #containsKey(Object)} / {@link #remove(Object)}
 * 与两个 {@code setnx} 一律按"没有值"回答；{@link #delete(Object)} 会把过期条目物理清掉但
 * 返回 {@code false}；过期项惰性清理，{@link #size()} 取 ConcurrentMap 的原始条数。
 * <p>
 * <b>为什么不继承 {@link ExpiredMapKVCache}</b>：本类承诺 L1 + L2，而 L2 要求<b>每一个</b>
 * 方法整体原子。继承会把这条承诺押在父类的方法体上 —— 父类面向任意 Map，它哪天把某个方法
 * 改成两步实现（例如先 get 再 remove），子类就会静默失去原子性。因此这里每个方法都直接落在
 * ConcurrentMap 上，承诺由本类自己持有。
 * <p>
 * <b>原子性由单条 {@code compute} / {@code remove} 命令承担</b>：两个 {@code setnx} 与
 * 两个 {@code computeIfAbsent} 用 {@code map.compute} 把"判定 + 写入"合成一步；
 * {@link #get(Object)} 用 {@code map.compute} 把"判定是否过期 + 保留或清除"合成一步；
 * {@code set} / {@code delete} / {@link #remove(Object)} 的单次委托本身也是原子的。
 * 底层 Map 由子类指定，例如 {@link ExpiredConcurrentHashMapKVCache}。
 * <p>
 * <b>{@code mappingFunction} 在桶锁内执行，不得回头操作本 cache</b>：本类的
 * {@link #get(Object)} 自身也用 {@code compute}，因此回调里连读同一个 key 都会挂死
 * （JDK 8 上永久空转、不抛异常）。回调必须短小：它执行期间该哈希桶上的其它操作全部阻塞。
 * <p>
 * <b>对 key / value 类型的要求</b>：{@code K} 必须正确实现 {@code equals} 与 {@code hashCode}；
 * {@code V} 不需要（{@code Binary} 只作 value 存放、从不作 key）；{@code V} 不会被复制 ——
 * {@link #get(Object)} 返回写进去的那个引用。
 * <p>
 * <b>安全级别</b>：L1 + L2 —— 自身线程安全，且所有方法整体原子，无需再用
 * {@link SynchronizedExpiredKVCache} 包装。
 *
 * @author addenda
 * @since 2026/10/17
 */
public abstract class ExpiredConcurrentMapKVCache<K, V> implements ExpiredKVCache<K, V> {

  /**
   * value + 到期时刻（毫秒）；{@link Long#MAX_VALUE} 表示永不过期
   */
  private final ConcurrentMap<K, Binary<V, Long>> map;

  protected ExpiredConcurrentMapKVCache(ConcurrentMap<K, Binary<V, Long>> map) {
    if (map == null) {
      throw new CacheIllegalArgumentException("map must not be null!");
    }
    this.map = map;
  }

  @Override
  public void set(K k, V v) {
    KVCache.assertKeyNotNull(k);
    // 见 KVCache#set：value 为 null 时一律按删除处理
    if (v == null) {
      map.remove(k);
      return;
    }
    map.put(k, Binary.of(v, Long.MAX_VALUE));
  }

  @Override
  public void set(K k, V v, long timeout, TimeUnit timeunit) {
    KVCache.assertKeyNotNull(k);
    // 见 KVCache#set：value 为 null 时一律按删除处理
    if (v == null) {
      map.remove(k);
      return;
    }
    map.put(k, Binary.of(v, ExpiredKVCache.deadline(timeout, timeunit)));
  }

  /**
   * 不带 TTL 的 setnx：<b>整段语义是原子的</b>，与带 TTL 的重载同形。
   * <p>
   * 不能用 {@code putIfAbsent}：过期条目仍占着 map 里的位置会被它挡住；先清再写又变成两步。
   * 用 {@code compute} 把"判定 + 写入"合成一步。
   */
  @Override
  public boolean setnx(K k, V v) {
    KVCache.assertKeyNotNull(k);
    // 见 KVCache#setnx：value 为 null 时表示"没有值"，本方法不写入任何内容
    if (v == null) {
      return false;
    }
    Binary<V, Long> fresh = Binary.of(v, Long.MAX_VALUE);
    Binary<V, Long> written = map.compute(k, (key, current) ->
            current == null || isExpired(current) ? fresh : current);
    return written == fresh;
  }

  /**
   * 带 TTL 的 setnx：<b>整段语义是原子的</b>。
   * <p>
   * "已过期的旧值等同于不存在"意味着判定不能只看 key 在不在，还要看到期时刻；用
   * {@code compute} 把"判定 + 写入"合成一步，返回的新值正是本次写入的那一份即表示成功。
   */
  @Override
  public boolean setnx(K k, V v, long timeout, TimeUnit timeunit) {
    KVCache.assertKeyNotNull(k);
    if (v == null) {
      return false;
    }
    Binary<V, Long> fresh = Binary.of(v, ExpiredKVCache.deadline(timeout, timeunit));
    Binary<V, Long> written = map.compute(k, (key, current) ->
            current == null || isExpired(current) ? fresh : current);
    return written == fresh;
  }

  /**
   * 判断 key 是否存在，<b>已过期的等同于不存在</b>。直接复用 {@link #get(Object)}。
   */
  @Override
  public boolean containsKey(K k) {
    KVCache.assertKeyNotNull(k);
    return get(k) != null;
  }

  /**
   * 一步取值：{@code compute} 在同一次调用里完成"判定是否过期 + 保留或清除"。
   * <p>
   * 不写成"先 {@code map.get}、发现过期再 {@code map.remove}"两步，是为了让返回值与清除
   * 发生在同一个原子步骤里，正确性不依赖任何关于值的假设。命中未过期的条目时原样返回
   * {@code current}，不增删节点、{@link #size()} 不变。代价是每次都要拿桶锁，读路径不再是
   * lock-free 的 {@code get}。
   */
  @Override
  public V get(K k) {
    KVCache.assertKeyNotNull(k);
    Binary<V, Long> alive = map.compute(k, (key, current) ->
            current == null || isExpired(current) ? null : current);
    return alive == null ? null : alive.getF1();
  }

  /**
   * 删除缓存，返回是否删除了一条<b>存在</b>的数据。
   * <p>
   * <b>已过期的条目等同于不存在</b>：照样把它物理清掉，但返回 {@code false}，
   * 与 {@link #remove(Object)}（过期返回 null）及 redis 实现（{@code DEL} 返回 0）口径一致。
   */
  @Override
  public boolean delete(K k) {
    KVCache.assertKeyNotNull(k);
    Binary<V, Long> removed = map.remove(k);
    return removed != null && !isExpired(removed);
  }

  /**
   * @return ConcurrentMap 的原始条数。<b>已过期但还没被访问过的条目仍计入</b>，
   *         因此是"存过的条数"的上界，而不是"当下可读的条数"
   */
  @Override
  public int size() {
    return map.size();
  }

  /**
   * @return {@link KVCache#UNLIMITED_CAPACITY} —— 本实现不限制条数，这个值是"实际上限"的哨兵，
   *         不是真实存在的容量约束；本实现也不主动回收过期项，更不能据此推断"最多只会占这么多"
   */
  @Override
  public int capacity() {
    return UNLIMITED_CAPACITY;
  }

  /**
   * 一步原子地取出并删除。与 {@link #get(Object)} 同口径：本次取到的值已过期时按"没有值"
   * 处理（返回 null）。
   */
  @Override
  public V remove(K k) {
    KVCache.assertKeyNotNull(k);
    Binary<V, Long> binary = map.remove(k);
    if (binary == null || isExpired(binary)) {
      return null;
    }
    return binary.getF1();
  }

  /**
   * 覆盖默认实现：整段 check-then-act 是原子的，收敛到 ConcurrentMap 的 {@code compute} 上，
   * 同一个 key 上同时只有一个线程能完成"检查 + 计算 + 写入"。
   * <p>
   * 与默认实现一致：命中未过期的值时直接返回、不调用 {@code mappingFunction}；计算结果为 null
   * 时不写入。本层特有：<b>已过期的旧值等同于不存在</b>，因此遇到过期的条目会调用
   * {@code mappingFunction} 并用新值替换它（返回 null 则删掉该过期条目）。
   * <p>
   * <b>{@code mappingFunction} 不得回头操作本 cache。</b>回调在桶锁内执行，此时再操作同一个 key
   * 在 JDK 8 上会让当前线程<b>永久空转、不抛异常</b>；本类的 {@link #get(Object)} /
   * {@link #containsKey(Object)} 自身也用 {@code compute}，因此连读同一个 key 都会挂死。
   * 回调必须短小：它执行期间该哈希桶上的其它操作全部阻塞。
   *
   * @param key             key，不允许为 null
   * @param mappingFunction 不存在（或已过期）时的取值函数，不允许为 null；<b>不得操作本 cache</b>
   */
  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    KVCache.assertKeyNotNull(key);
    Objects.requireNonNull(mappingFunction);
    // timeunit 为 null 表示不带 TTL 的重载：写进去的值永不过期
    return computeIfAbsentAtomically(key, mappingFunction, 0L, null);
  }

  /**
   * 覆盖默认实现：整段 check-then-act 是原子的，见
   * {@link #computeIfAbsent(Object, Function)}。
   * <p>
   * 与默认实现一致：命中未过期的值时既不调用 {@code mappingFunction}，也不刷新它已有的 TTL。
   *
   * @param key             key，不允许为 null
   * @param mappingFunction 不存在（或已过期）时的取值函数，不允许为 null
   * @param timeout         新值的过期时间
   * @param timeunit        过期时间单位
   */
  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction,
                           long timeout, TimeUnit timeunit) {
    KVCache.assertKeyNotNull(key);
    Objects.requireNonNull(mappingFunction);
    return computeIfAbsentAtomically(key, mappingFunction, timeout, timeunit);
  }

  /**
   * {@code timeunit} 为 null 表示"不带 TTL 的重载"，此时 {@code timeout} 不参与计算。
   * <p>
   * 不用 {@code AtomicReference} 之类的容器承接结果：{@code compute} 的返回值就是写入 map
   * 的那个对象。
   */
  private V computeIfAbsentAtomically(K key, Function<? super K, ? extends V> mappingFunction,
                                      long timeout, TimeUnit timeunit) {
    Binary<V, Long> computed = map.compute(key, (k, current) -> {
      // 命中未过期的值：直接返回，不调用 mappingFunction
      if (current != null && !isExpired(current)) {
        return current;
      }
      V value = mappingFunction.apply(k);
      // 与默认实现一致：计算结果为 null 时不写入；这里返回 null 即把过期的旧条目删掉
      if (value == null) {
        return null;
      }
      long deadline = timeunit == null
              ? Long.MAX_VALUE
              : ExpiredKVCache.deadline(timeout, timeunit);
      return Binary.of(value, deadline);
    });
    return computed == null ? null : computed.getF1();
  }

  /**
   * 到期时刻不晚于当前时刻即为已过期：{@code deadline <= now}。
   * <p>
   * 用 {@code <=} 与 redis 的 {@code now >= deadline} 对齐；判定基准是
   * {@link System#currentTimeMillis()}。
   */
  private boolean isExpired(Binary<V, Long> binary) {
    Long deadline = binary.getF2();
    return deadline != null && deadline <= System.currentTimeMillis();
  }

}
