package cn.addenda.component.cache.eviction;

import cn.addenda.component.cache.ExpiredKVCache;
import cn.addenda.component.cache.KVCache;

/**
 * 带<b>自驱逐能力</b>的缓存：触及自身的容量约束时会淘汰数据。
 * <p>
 * <b>由谁执行淘汰由实现决定</b>，目前有两条路径：
 * <ul>
 *   <li><b>存储自己完成</b>：淘汰由底层存储承担（服务端的 {@code maxmemory-policy}、
 *       key 的 TTL 等），这类实现直接 implements 本接口，不经由包装类。
 *       <b>本组件目前没有这样的实现</b>：基于 redis 的实现只 implements
 *       {@link ExpiredKVCache}，未声明"具备自驱逐能力"；</li>
 *   <li><b>应用层完成</b>：实现继承 {@link EvictableKVCacheWrapper}，由它提供容量字段、
 *       模板方法与钩子，再用额外的统计结构决定淘汰谁。现有的淘汰族都走这条路。</li>
 * </ul>
 * 本接口不新增方法，只<b>重新声明两个写入口</b>，把淘汰带来的语义差异写在方法上 ——
 * 见 {@link #set(Object, Object)} 与 {@link #setnx(Object, Object)}。
 * 其余方法的口径与 {@link KVCache} 完全一致，不再重复声明。
 * <p>
 * 模板方法与钩子只存在于 {@link EvictableKVCacheWrapper} 这条实现路径上，不进本接口：
 * 它们是模板与实现类之间的协议，需要保持 {@code protected}，而接口声明不了 protected 方法。
 *
 * @author addenda
 * @since 2023/05/30
 */
public interface EvictableKVCache<K, V> extends KVCache<K, V> {

  /**
   * <b>在 {@link KVCache#set(Object, Object)} 之上追加的语义：容量已满时会先淘汰一个
   * 调用方没删过的 key。</b>
   * <p>
   * 丢谁由实现采用的淘汰策略决定，调用方无法干预（策略见各实现）。
   *
   * @param k key，不允许为 null
   * @param v value，为 null 时等价于 {@link KVCache#delete(Object)}
   * @throws IllegalArgumentException k 为 null 时
   */
  @Override
  void set(K k, V v);

  /**
   * <b>在 {@link KVCache#setnx(Object, Object)} 之上追加的语义：入口同上 —— 写入落地时
   * 容量已满的话，同样会先淘汰一个调用方没删过的 key。</b>
   *
   * @param k key，不允许为 null
   * @param v value，为 null 时不写入并返回 false
   * @return 写入成功返回 true；key 已存在、或 v 为 null 时返回 false
   * @throws IllegalArgumentException k 为 null 时
   */
  @Override
  boolean setnx(K k, V v);

}
