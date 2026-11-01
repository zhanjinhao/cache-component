package cn.addenda.component.cache;

import java.util.HashMap;

/**
 * 用 {@link HashMap} 承载的 {@link KVCache}。
 * <p>
 * <b>继承的规则</b>：Map 承载的通用语义全部来自 {@link MapKVCache} —— value 为 null 按删除、
 * {@code setnx} 不覆盖已有 key、不限制条数；{@code computeIfAbsent} 沿用接口默认实现，
 * 执行期间需要独占（本类只到 L1）。
 * <p>
 * <b>自身差异</b>：只有底层 Map 的选型 —— {@link HashMap}。
 * 需要按 key 设置过期时间时用 {@link ExpiredHashMapKVCache}。
 * <p>
 * <b>并发安全级别</b>：L1（只支持并发读）。{@code get} / {@code containsKey} / {@code size}
 * 只读 {@link HashMap}、不修改结构，多个线程可以同时调用；<b>写操作必须独占</b>。
 * 需要 L4 时用 {@link SynchronizedKVCache} 包装。
 *
 * @author addenda
 * @since 2026/10/02
 */
public class HashMapKVCache<K, V> extends MapKVCache<K, V> {

  public HashMapKVCache() {
    super(new HashMap<>());
  }

}
