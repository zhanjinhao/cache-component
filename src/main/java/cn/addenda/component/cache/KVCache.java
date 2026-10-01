package cn.addenda.component.cache;

import java.util.Objects;
import java.util.function.Function;

/**
 * 抽象kv-cache的功能。
 * <p/>
 * <b>"线程安全"这个词太笼统，缓存场景下要拆成四层看 —— 本接口只承诺 L1 和 L2。</b>
 * <ul>
 *   <li>
 *     <b>L1 结构安全</b>：任意多线程并发调用任意方法，不抛异常、不损坏内部结构、
 *     不丢已确认写入的数据。
 *     <br/>
 *     约定：<b>底层存储本身线程安全的实现，自己也必须是线程安全的</b> ——
 *     Lettuce / Redisson / spring-data-redis 都是线程安全的，所以基于它们的实现
 *     必须能被多线程共享，实现上只要保证<b>字段全是 final、自己不留任何可变状态</b>、
 *     把活全交给底层即可。
 *     底层本来就不线程安全的实现（比如基于 HashMap 的 {@link ExpiredHashMapKVCache}）
 *     不承诺这一层，需要并发访问时用 {@link SynchronizedKVCache} 包一层。
 *   </li>
 *   <li>
 *     <b>L2 单命令原子</b>：单个 {@code get} / {@code set} / {@code delete} 不会被撕裂。
 *     内存实现靠 JVM 的引用写入原子性，redis 实现靠单命令原子性，这一层所有实现都满足。
 *   </li>
 *   <li>
 *     <b>L3 复合操作原子</b>：由多个方法拼成的 check-then-act 整体作为原子。
 *     <b>本接口不承诺这一层</b> —— {@link #remove(Object)} 和
 *     {@link #computeIfAbsent(Object, Function)} 都是默认实现，由 get + 写两步拼成，
 *     多线程可以同时通过"检查"那一步。
 *     单线程下 {@code mappingFunction} 只调一次，<b>并发下会被调多次</b>。
 *     需要原子性由上层加锁，{@code CacheHelper} 的 lockAllocator + 令牌桶就是补这一层的。
 *   </li>
 *   <li>
 *     <b>L4 缓存一致性 / 击穿 / 穿透</b>：缓存与数据源之间的问题，
 *     <b>不属于本接口的范畴</b> —— 改库与删缓存之间的窗口、大量线程同时打库、
 *     反复查不存在的数据，这些即使每个操作都"线程安全"也照样发生，
 *     由 {@code CacheHelper} 这一层负责（延迟二次删除、{@code NULL_OBJECT} 占位等）。
 *   </li>
 * </ul>
 * 一句话：<b>L1/L2 管的是"这个对象自己会不会被并发搞坏"，
 * L4 管的是"缓存和数据库之间一不一致"。</b>
 *
 * @author addenda
 * @since 2023/05/30
 */
public interface KVCache<K, V> {

  /**
   * 写入缓存。
   * <p/>
   * <b>v 为 null 时表示"没有值"，一律当作删除这个 key 处理</b> —— 各实现都必须遵守，
   * 即调用之后 {@link #get(Object)} 返回 null、{@link #containsKey(Object)} 返回 false。
   * <p/>
   * 统一成这个语义的原因：redis 里没有 java null 值，让 null 落库各家行为都不一样
   * （lettuce 会把 null 编码成空串、spring-data-redis 直接抛异常），
   * 结果就是调用方读出来的东西和写进去的对不上。
   *
   * @param k key
   * @param v value，为 null 时等价于 {@link #delete(Object)}
   */
  void set(K k, V v);

  boolean containsKey(K k);

  V get(K k);

  boolean delete(K k);

  long size();

  long capacity();

  /**
   * get & delete
   */
  default V remove(K k) {
    V v = get(k);
    delete(k);
    return v;
  }

  default V computeIfAbsent(K key,
                            Function<? super K, ? extends V> mappingFunction) {
    Objects.requireNonNull(mappingFunction);
    V v;
    if ((v = get(key)) == null) {
      V newValue;
      if ((newValue = mappingFunction.apply(key)) != null) {
        set(key, newValue);
        return newValue;
      }
    }

    return v;
  }

}
