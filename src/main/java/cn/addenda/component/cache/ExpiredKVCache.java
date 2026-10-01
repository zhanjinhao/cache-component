package cn.addenda.component.cache;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public interface ExpiredKVCache<K, V> extends KVCache<K, V> {

  /**
   * 写入缓存并设置过期时间。
   * <p/>
   * <b>v 为 null 时表示"没有值"，一律当作删除这个 key 处理</b>，语义同 {@link KVCache#set(Object, Object)}。
   *
   * @param k        key
   * @param v        value，为 null 时等价于 {@link KVCache#delete(Object)}
   * @param timeout  过期时间
   * @param timeunit 过期时间单位
   */
  void set(K k, V v, long timeout, TimeUnit timeunit);

  default V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long timeout, TimeUnit timeunit) {
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
