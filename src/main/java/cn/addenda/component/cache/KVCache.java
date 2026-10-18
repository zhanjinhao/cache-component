package cn.addenda.component.cache;

import java.util.Objects;
import java.util.function.Function;

/**
 * 抽象 kv-cache 的功能。
 * <p>
 * <b>{@link #set(Object, Object)} 不承诺"这条数据会一直在"。</b>{@link #get(Object)} 返回 null
 * 有三种原因：没写过、被删掉了、<b>实现自己丢弃了它</b> —— 容量驱动的丢弃见
 * {@code EvictableKVCache}，过期驱动的丢弃见 {@code ExpiredKVCache}。调用方必须以
 * {@link #get(Object)} / {@link #containsKey(Object)} 的实际返回为准。
 * <p>
 * 同理，{@link #get(Object)} 也不承诺无副作用：实现可以借读操作维护内部结构。
 * <p>
 * <b>线程安全分三层；本接口只定义这三层的含义，不声明任何实现达到哪一层</b> ——
 * 各实现类的注释里都有一节"安全级别"：
 * <ul>
 *   <li><b>L1 结构安全</b>：并发调用任意方法，不抛异常、不损坏内部结构、不丢失已确认写入的数据。</li>
 *   <li><b>L2 原子</b>：<b>所有</b>方法都整体原子 —— 单命令不会执行到一半；由多个方法组成的
 *       check-then-act，即本接口的 {@link #remove(Object)} 与
 *       {@link #computeIfAbsent(Object, Function)}，也整体作为原子。
 * <p>
 *       <b>不可部分承诺</b>：做不到原子的方法必须明确拒绝（抛
 *       {@link UnsupportedOperationException}），不能拿一个非原子的版本顶上 ——
 *       调用方从签名上分辨不出哪一半可以依赖。这两个方法的<b>默认实现不属于本层</b>
 *       （它们由 get 与写两步组成）；实现要么覆盖成原子版本，要么拒绝。
 *       也可以由上层加锁补足，见 {@code CacheHelper}。</li>
 *   <li><b>L3 缓存一致性 / 击穿 / 穿透</b>：不属于本接口的职责，见 {@code CacheHelper}。</li>
 * </ul>
 * <h3>入参误用的异常口径</h3>
 * 同一个错误在不同方法上必须报同一种异常，否则调用方没法写出统一的兜底处理。
 * 多个错误同时存在时，按下列优先级判定：
 * <ol>
 *   <li><b>实现不支持该操作</b> → {@link UnsupportedOperationException}，且<b>不校验参数</b>：
 *       操作整体不可用时，"null 该怎么处理"不再适用。包括 {@code LruKKVCache} 的
 *       {@code setnx} / {@code computeIfAbsent}、四个 redis 实现的 {@code computeIfAbsent}、
 *       只读外壳的写方法。</li>
 *   <li><b>{@code key} 为 null</b> → {@link IllegalArgumentException}。key 为 null 是本接口对
 *       所有方法一致约定的调用方错误，因此<b>两个参数都为 null 时也报 key</b>，而不是换成
 *       别的异常。</li>
 *   <li><b>{@code mappingFunction} 为 null</b> → {@link NullPointerException}。
 *       只有 key 合法时才轮得到这一条。</li>
 * </ol>
 * <b>唯一不适用第 1 条的是"条件性不支持"</b>：实现本身提供该方法，只是当前环境做不到 ——
 * 例如四个 redis 实现的 {@code remove} 依赖 {@code GETDEL}，服务端版本不够时抛
 * {@link UnsupportedOperationException}。这类仍按常规顺序<b>先校验 key</b>：
 * 参数错误与能力缺失同时存在时，报参数错误。
 * <p>
 * 序列化类型不支持由 {@code CacheHelper} 层报 {@code CacheException}，不在本接口范围内。
 *
 * @author addenda
 * @since 2023/05/30
 */
public interface KVCache<K, V> {

  /**
   * <b>条数与容量的值域上界：任何实现都必须满足
   * {@code 0 <= size() <= capacity() <= UNLIMITED_CAPACITY}。</b>
   * <p>
   * 本组件所有"合法 / 不合法"的判断都以它为准，不要拿 {@link Integer#MAX_VALUE} 去比：
   * 它现在恰好等于后者，但那是取值、不是语义（将来可能被调低或由配置给出）。
   * <p>
   * 两个用途：不限制条数的实现用它作返回值（它<b>不是</b>会发生作用的额度）；淘汰族用它作
   * 上界并在构造时校验，包括由容量<b>组合</b>出来的值，见 {@code LruGenerationKVCache} 与
   * {@code LruKHashMapKVCache}。
   * <p>
   * <b>不要与 {@link Long#MAX_VALUE} 混用</b>：后者是<b>到期时刻</b>的"永不过期"哨兵
   * （见 {@link ExpiredKVCache#deadline}），是时间而不是容量。
   * <p>
   * 初始值刻意写成<b>非编译期常量</b>（{@code Integer.valueOf} 而不是字面量）：编译期常量会被
   * 调用方内联，那个值就改不动了。现在需要它可读到的场景是测试 —— 把上限临时调低，才能构造出
   * "超过上限"的输入；语义上它也是"取值"而不是"语法"，将来可能由配置给出。
   */
  int UNLIMITED_CAPACITY = Integer.valueOf(Integer.MAX_VALUE);

  /**
   * 写入缓存。
   * <p>
   * v 为 null 表示"没有值"，一律视为删除该 key：调用之后 {@link #get(Object)} 返回 null、
   * {@link #containsKey(Object)} 返回 false。各实现都必须遵守 —— 底层存储对 java null 的
   * 处理各不相同，逐家适配只会让语义分叉，各家的做法见实现说明。
   * <p>
   * 本方法只负责"这次写入生效"：容量满时可能淘汰别的 key，值本身可能到期，
   * 实现也可能拒绝让这次写入落地（参考子类 {@code LruKKVCache}）。
   *
   * @param k key，不允许为 null
   * @param v value，为 null 时等价于 {@link #delete(Object)}
   * @throws IllegalArgumentException k 为 null 时
   */
  void set(K k, V v);

  /**
   * key 不存在时才写入（SET if Not eXists）。
   * <p>
   * 四种输入组合中只有一种会实际写入 —— 底层不存在该 key 且 v 非 null；其余三种一律
   * 不修改底层数据并返回 false。<b>v 为 null 时不会退化为删除</b>（nx语义的优先级高于v是null的契约），这一点与
   * {@link #set(Object, Object)} 不同。
   *
   * @param k key，不允许为 null
   * @param v value，为 null 时不写入并返回 false
   * @return 写入成功返回 true；key 已存在、或 v 为 null 时返回 false
   * @throws IllegalArgumentException k 为 null 时
   */
  boolean setnx(K k, V v);

  /**
   * @param k key，不允许为 null，见 {@link #assertKeyNotNull(Object)}
   */
  boolean containsKey(K k);

  /**
   * @param k key，不允许为 null，见 {@link #assertKeyNotNull(Object)}
   */
  V get(K k);

  /**
   * @param k key，不允许为 null，见 {@link #assertKeyNotNull(Object)}
   */
  boolean delete(K k);

  /**
   * 当前条数。返回值一定落在 {@code [0, UNLIMITED_CAPACITY]} 内。
   * <p>
   * 不是每个实现都答得上来 —— 参考子类 {@code LettuceRedisKVCache}（拒绝）、
   * {@code EvictableKVCacheWrapper}（淘汰族的口径）。
   *
   * @throws UnsupportedOperationException 实现不支持时
   */
  int size();

  /**
   * 容量上限。返回值一定落在 {@code [0, UNLIMITED_CAPACITY]} 内。
   * <p>
   * 同样不是每个实现都答得上来 —— 参考子类 {@code LettuceRedisKVCache}（拒绝）、
   * {@code EvictableKVCacheWrapper}（由构造参数决定）。<b>不限制条数的实现统一返回
   * {@link #UNLIMITED_CAPACITY} 哨兵</b>，它们不会因为"到了这个数"而丢弃任何数据。
   *
   * @throws UnsupportedOperationException 实现不支持时
   */
  int capacity();

  /**
   * 取值并删除，返回被删掉的那个值。
   * <p>
   * 复合操作，本默认实现由 get 与 delete 两步组成，不属于 L2。参考子类
   * {@code ConcurrentHashMapKVCache}（原子）、{@code SynchronizedKVCache}（持锁）、
   * {@code LettuceRedisKVCache}（{@code GETDEL}，要求服务端 Redis ≥ 6.2）。
   *
   * @param k key，不允许为 null
   * @throws IllegalArgumentException      k 为 null 时
   * @throws UnsupportedOperationException 服务端或实现不支持时
   */
  default V remove(K k) {
    // 先拦 null：本方法有两个下游调用，报错要发生在动作开始之前
    assertKeyNotNull(k);
    V v = get(k);
    delete(k);
    return v;
  }

  /**
   * 不存在时用 {@code mappingFunction} 计算并写入；计算结果为 null 时不写入。
   * <p>
   * 与 {@link #remove(Object)} 同属复合操作，本默认实现不属于 L2；实现可以覆盖成底层
   * 存储的原子版本，也可以<b>拒绝提供</b>（参考子类 {@code LruKKVCache}）。
   * <p>
   * <b>{@code mappingFunction} 不得回头操作本 cache。</b>回调在哪种锁里执行由实现决定，
   * 违反的后果也各不相同 —— 参考子类 {@code ConcurrentHashMapKVCache}、
   * {@code ExpiredConcurrentHashMapKVCache}。需要"计算过程中写别的数据"时，
   * 把该动作放在调用本方法之前或之后。
   *
   * @param key             key，不允许为 null
   * @param mappingFunction 不存在时的取值函数，不允许为 null；<b>不得操作本 cache</b>
   * @throws IllegalArgumentException      key 为 null 时
   * @throws NullPointerException          mappingFunction 为 null 时
   * @throws UnsupportedOperationException 实现不支持时（此时不校验参数）
   */
  default V computeIfAbsent(K key,
                            Function<? super K, ? extends V> mappingFunction) {
    assertKeyNotNull(key);
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

  /**
   * key 不允许为 null 时的统一校验。各实现应直接调用，不要各自复制一份。
   * <p>
   * key 与 value 的处理不同：value 为 null 有确定含义（"没有值"，见
   * {@link #set(Object, Object)}），key 为 null 则是调用方错误 —— null key 标识不了
   * 一条缓存，各底层存储的处理也不一致。
   *
   * @param k key
   * @throws IllegalArgumentException k 为 null 时
   */
  static void assertKeyNotNull(Object k) {
    if (k == null) {
      throw new IllegalArgumentException("key 不允许为 null");
    }
  }

}
