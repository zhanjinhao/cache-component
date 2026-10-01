package cn.addenda.component.cache;

import cn.addenda.component.base.pojo.Binary;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 基于HashMap实现的KVCache
 *
 * @author addenda
 * @since 2023/05/30
 */
public class ExpiredHashMapKVCache<K, V> implements ExpiredKVCache<K, V> {

  private final Map<K, Binary<V, Long>> map = new HashMap<>();

  @Override
  public void set(K k, V v) {
    // 见 KVCache#set：写 null 一律当删除
    if (v == null) {
      map.remove(k);
      return;
    }
    map.put(k, Binary.of(v, Long.MAX_VALUE));
  }

  @Override
  public void set(K k, V v, long timeout, TimeUnit timeunit) {
    // 见 KVCache#set：写 null 一律当删除
    if (v == null) {
      map.remove(k);
      return;
    }
    long timeoutMills = timeunit.toMillis(timeout);
    map.put(k, Binary.of(v, System.currentTimeMillis() + timeoutMills));
  }

  @Override
  public boolean containsKey(K k) {
    return map.containsKey(k);
  }

  @Override
  public V get(K k) {
    Binary<V, Long> binary = map.get(k);
    if (binary == null) {
      return null;
    }
    if (binary.getF2() < System.currentTimeMillis()) {
      map.remove(k);
      return null;
    }
    return binary.getF1();
  }

  @Override
  public boolean delete(K k) {
    return map.remove(k) != null;
  }

  @Override
  public long size() {
    return map.size();
  }

  @Override
  public long capacity() {
    return 1 << 30;
  }

  @Override
  public V remove(K k) {
    Binary<V, Long> remove = map.remove(k);
    if (remove != null) {
      return remove.getF1();
    }
    return null;
  }

  // 不再 override computeIfAbsent，直接用 ExpiredKVCache/KVCache 的默认实现。
  //
  // 原来的实现有两个偏离：
  //   1. 无条件先调 mappingFunction.apply(key)，再去 map.computeIfAbsent
  //      —— 与 Map.computeIfAbsent "只在 key 不存在时才计算" 的约定相反；
  //      命中缓存时业务侧的计算函数照样会被执行，代价白白付出。
  //   2. 计算结果是 null 时会把 Binary(null, ...) 存进去
  //      —— 于是 containsKey 返回 true、get 返回 null，和 KVCache#set 那条
  //      "null 一律当删除" 的契约冲突。
  //
  // 默认实现是惰性的，且结果为 null 时不会写入，两个问题都没有。

}
