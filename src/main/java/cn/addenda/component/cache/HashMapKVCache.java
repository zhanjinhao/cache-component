package cn.addenda.component.cache;

import java.util.HashMap;

/**
 * 用 {@link HashMap} 承载的 {@link KVCache}。
 * <p>
 * <b>继承的规则</b>：Map 承载的通用语义全部来自 {@link MapKVCache} —— value 为 null 按删除、
 * {@code setnx} 不覆盖已有 key、不限制条数；{@code computeIfAbsent} 沿用接口默认实现，
 * <b>不承诺 L2</b>。
 * <p>
 * <b>自身差异</b>：只有底层 Map 的选型 —— {@link HashMap}。
 * 需要按 key 设置过期时间时用 {@link ExpiredHashMapKVCache}。
 * <p>
 * <b>安全级别</b>：无。普通 {@link HashMap} 连 L1 都不承诺，并发写入会损坏内部结构；
 * 并发访问时用 {@link SynchronizedKVCache} 包装。
 *
 * @author addenda
 * @since 2026/10/02
 */
public class HashMapKVCache<K, V> extends MapKVCache<K, V> {

  public HashMapKVCache() {
    super(new HashMap<>());
  }

}
