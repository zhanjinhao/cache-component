package cn.addenda.component.cache;

import java.util.HashMap;

/**
 * 用 {@link HashMap} 承载的 {@link ExpiredKVCache}。
 * <p>
 * <b>继承的规则</b>：TTL 换算、"过期即不存在"、{@code delete} 对过期条目返回 false、
 * 过期项的惰性清理等全部来自 {@link ExpiredMapKVCache}；两个 {@code computeIfAbsent}
 * 沿用接口默认实现，<b>不承诺 L2</b>。
 * <p>
 * <b>自身差异</b>：只有底层 Map 的选型 —— {@link HashMap}。
 * 需要线程安全时用 {@link ExpiredConcurrentHashMapKVCache}。
 * <p>
 * <b>安全级别</b>：无。普通 {@link HashMap} 连 L1 都不承诺，并发写入会损坏内部结构；
 * 并发访问时用 {@link SynchronizedExpiredKVCache} 包装。
 *
 * @author addenda
 * @since 2023/05/30
 */
public class ExpiredHashMapKVCache<K, V> extends ExpiredMapKVCache<K, V> {

  public ExpiredHashMapKVCache() {
    super(new HashMap<>());
  }

}
