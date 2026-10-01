package cn.addenda.component.cache;

import java.util.Objects;
import java.util.function.Function;

/**
 * 抽象kv-cache的功能
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
