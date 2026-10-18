package cn.addenda.component.cache;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 带<b>过期能力</b>的 kv-cache：在 {@link KVCache} 之上按 key 设置 TTL。
 * <p>
 * 与 {@link KVCache} 的关系是<b>纯增量</b>：三个 TTL 入口都是新增方法，继承来的方法
 * 签名与结果口径不变。
 * <p>
 * <b>核心语义：过期的 key 等同于没有值</b>，即与"压根没有这个 key"完全等价。因此
 * {@link #get(Object)} / {@link #containsKey(Object)} / {@link #remove(Object)} 与两个
 * {@code setnx} 一律按"没有值"回答：读不到、查不到、{@code remove} 返回 null、
 * {@code setnx} 不会被过期的旧值挡住。
 * <p>
 * <b>TTL 的换算规则只有一份</b>，即 {@link #ttlMillis}；实现不得另写一份。
 * <p>
 * <b>超大 TTL</b>：换算结果溢出时 {@link #ttlMillis} 饱和到 {@link Long#MAX_VALUE}。
 * 内存实现据此得到"永不过期"（见 {@link #deadline}）；redis 实现的服务端算不了
 * {@code 当前时刻 + TTL}（64 位溢出），会拒绝该 TTL，实现统一翻译成
 * {@link IllegalArgumentException} 且<b>不写入</b>（原始客户端异常在 {@code cause} 上）。
 * 这是两者之间唯一一处 TTL 语义差异 —— 需要"永不过期"时请使用不带 TTL 的重载。
 * <p>
 * <b>时钟语义</b>：到期判定与到期时刻计算都以 {@link System#currentTimeMillis()} 为准，
 * 即墙上时钟，没有单调时钟补偿 —— 时钟回拨会让条目活得更久，两台机器时钟有偏差时
 * 同一个 TTL 的实际存活时长也有偏差。本接口不提供可注入的时钟。
 *
 * @author addenda
 */
public interface ExpiredKVCache<K, V> extends KVCache<K, V> {

  /**
   * TTL 的下限（毫秒）：{@link #ttlMillis} 把任何非正 timeout 都规整到这个值。
   * <p>
   * 取 1 而不是 0，是因为有些存储拒绝非正的过期时间；1ms 后过期与"立即过期"没有区别。
   */
  long MIN_TTL_MILLIS = 1L;

  /**
   * <b>把 timeout 规整成 TTL 时长（毫秒）—— 本接口所有实现共用的唯一一条换算规则。</b>
   * <p>
   * 非正 timeout 一律取整为 {@link #MIN_TTL_MILLIS}（不原样下发是因为有些存储拒绝非正的
   * 过期时间；不必报错是因为非正 TTL 本来就表示"立即过期"），正 timeout 按单位换算，
   * 不做别的加工。
   * <p>
   * 本方法只处理"非正"：{@code toMillis} 自身饱和到 {@link Long#MAX_VALUE} 的大 TTL
   * 原样返回 —— 那是溢出问题，内存侧见 {@link #deadline}，redis 侧见类注释的"超大 TTL"。
   *
   * @param timeout  过期时间，允许为 0 或负数（表示"立即过期"）
   * @param timeunit 过期时间单位
   * @return 不小于 {@link #MIN_TTL_MILLIS} 的毫秒数
   */
  static long ttlMillis(long timeout, TimeUnit timeunit) {
    return Math.max(MIN_TTL_MILLIS, timeunit.toMillis(timeout));
  }

  /**
   * 计算到期时刻（毫秒）：{@code now + ttlMillis(timeout, timeunit)}。
   * <p>
   * 相加溢出时返回 {@link Long#MAX_VALUE}（"永不过期"），与不带 TTL 的
   * {@link #set(Object, Object)} 落在同一个哨兵上 —— 否则回绕成负数会让条目一写进去
   * 就被判为已过期。换算规则在 {@link #ttlMillis}。
   *
   * @param timeout  过期时间
   * @param timeunit 过期时间单位
   * @return 到期时刻（毫秒）；{@link Long#MAX_VALUE} 表示永不过期
   */
  static long deadline(long timeout, TimeUnit timeunit) {
    long millis = ttlMillis(timeout, timeunit);
    long now = System.currentTimeMillis();
    long deadline = now + millis;
    // 结果小于 now，说明发生了溢出（含 toMillis 自身饱和到 Long.MAX_VALUE 的情况）
    return deadline < now ? Long.MAX_VALUE : deadline;
  }

  /**
   * 写入 key 与 value，并为该 key 设置过期时间。
   * <p>
   * value 为 null 时按删除处理，语义同 {@link KVCache#set(Object, Object)}。
   *
   * @param k        key，不允许为 null
   * @param v        value，为 null 时等价于 {@link KVCache#delete(Object)}
   * @param timeout  过期时间
   * @param timeunit 过期时间单位
   */
  void set(K k, V v, long timeout, TimeUnit timeunit);

  /**
   * 仅在 key 不存在时写入 key 与 value，并为该 key 设置过期时间。
   * <p>
   * 原子性与 null 语义同 {@link KVCache#setnx(Object, Object)}。
   *
   * @param k        key，不允许为 null
   * @param v        value，为 null 时不写入并返回 false
   * @param timeout  过期时间
   * @param timeunit 过期时间单位
   * @return 写入成功返回 true；key 已存在或 v 为 null 时返回 false
   */
  boolean setnx(K k, V v, long timeout, TimeUnit timeunit);

  /**
   * 不存在（或已过期）时用 {@code mappingFunction} 计算、带 TTL 写入；计算结果为 null
   * 时不写入。命中未过期的值时既不计算、也不刷新它已有的 TTL。
   * <p>
   * 其余口径同 {@link KVCache#computeIfAbsent(Object, Function)}。本默认实现不属于 L2；
   * 参考子类 {@link ExpiredConcurrentHashMapKVCache}（原子）、
   * {@link SynchronizedExpiredKVCache}（持锁）。
   *
   * @param key             key，不允许为 null
   * @param mappingFunction 不存在（或已过期）时的取值函数，不允许为 null；<b>不得操作本 cache</b>
   * @param timeout         新值的过期时间
   * @param timeunit        过期时间单位
   * @throws IllegalArgumentException      key 为 null 时
   * @throws NullPointerException          mappingFunction 为 null 时
   * @throws UnsupportedOperationException 实现不支持时（此时不校验参数）
   */
  default V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long timeout, TimeUnit timeunit) {
    KVCache.assertKeyNotNull(key);
    Objects.requireNonNull(mappingFunction);
    V v;
    if ((v = get(key)) == null) {
      V newValue;
      if ((newValue = mappingFunction.apply(key)) != null) {
        set(key, newValue, timeout, timeunit);
        return newValue;
      }
    }
    return v;
  }

}
