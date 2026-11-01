package cn.addenda.component.cache;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 用 {@link ConcurrentHashMap} 承载的 {@link ExpiredKVCache}。
 * <p>
 * <b>继承的规则</b>：TTL 换算、"过期即不存在"、过期项的惰性清理来自
 * {@link ExpiredMapKVCache}；L2（结构安全）与原子化的
 * {@code setnx} / {@code get} / {@code computeIfAbsent} 来自
 * {@link ExpiredConcurrentMapKVCache}。
 * <p>
 * <b>自身差异</b>：只有底层 Map 的选型 —— {@link ConcurrentHashMap}。
 * <p>
 * <b>并发安全级别</b>：L2（结构安全），并发访问无需再用 {@link SynchronizedExpiredKVCache}
 * 包装；{@code multiGet} / {@code containsKeys} 为逐 key 读（整批一致快照属于 L4）。
 * {@code mappingFunction} 在桶锁内执行，<b>不得回头操作本 cache</b> —— 本类的 {@code get}
 * 自身也用 {@code compute}，回调里连读同一个 key 都会挂死。
 *
 * @author addenda
 * @since 2026/10/06
 */
public class ExpiredConcurrentHashMapKVCache<K, V> extends ExpiredConcurrentMapKVCache<K, V> {

  public ExpiredConcurrentHashMapKVCache() {
    super(new ConcurrentHashMap<>());
  }

}
