package cn.addenda.component.cache;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 基于 Lettuce 实现的 {@link ExpiredKVCache}。
 * <p>
 * <b>继承的规则</b>：{@link ExpiredKVCache} 的全部约定 —— 过期的 key 等同于没有值、
 * TTL 换算走 {@link ExpiredKVCache#ttlMillis}、value 为 null 时 {@code set} 按删除处理而
 * {@code setnx} 不写入、key 为 null 抛 {@link IllegalArgumentException}。
 * <p>
 * <b>自身差异</b>：
 * <ul>
 *   <li>TTL 交给服务端维护：带 ttl 的 {@code set} 走 {@code PSETEX}，下发的是时长，
 *       本地不存到期时刻；</li>
 *   <li>{@link #remove(String)} 用单条命令 {@code GETDEL}（<b>要求服务端 Redis ≥ 6.2</b>），
 *       两个 {@code setnx} 用 {@code SET ... NX [PX ttl]} —— 都是单命令原子；</li>
 *   <li>{@code computeIfAbsent} 做不到原子（{@code mappingFunction} 是本地代码，服务端不参与
 *       求值），{@code size} / {@code capacity} 答不上来，三者一律拒绝，见各方法说明；</li>
 *   <li>连接生命周期：本对象创建的连接由 {@link #close()} 关闭，外部传入的不关。</li>
 * </ul>
 * <p>
 * <b>安全级别</b>：L1 + L2。字段全 final、不持有自有可变状态，操作全部委托给线程安全的
 * {@link StatefulRedisConnection}，因此实例可被多线程共享；对外暴露的方法全部是原子的。
 *
 * @author addenda
 * @since 2023/6/3 16:25
 */
public class LettuceRedisKVCache implements ExpiredKVCache<String, String> {

  private final StatefulRedisConnection<String, String> connection;

  private final RedisCommands<String, String> commands;

  /**
   * 连接是否由本对象创建。只有自己创建的才由自己关闭，外部传入的由外部管理。
   */
  private final boolean connectionOwned;

  /**
   * @param redisClient redis 客户端。由本对象创建的连接由本对象负责关闭。
   */
  public LettuceRedisKVCache(RedisClient redisClient) {
    this(redisClient.connect(), true);
  }

  /**
   * @param connection 由外部管理的连接，本对象不会关闭它。
   */
  public LettuceRedisKVCache(StatefulRedisConnection<String, String> connection) {
    this(connection, false);
  }

  private LettuceRedisKVCache(StatefulRedisConnection<String, String> connection, boolean connectionOwned) {
    this.connection = connection;
    this.commands = connection.sync();
    this.connectionOwned = connectionOwned;
  }

  @Override
  public void set(String key, String value) {
    KVCache.assertKeyNotNull(key);
    if (value == null) {
      // 必须删而不是写：lettuce 会把 null 编码成空串，get 会返回 ""，等于静默篡改数据
      commands.del(key);
      return;
    }
    commands.set(key, value);
  }

  @Override
  public void set(String key, String value, long timeout, TimeUnit unit) {
    KVCache.assertKeyNotNull(key);
    if (value == null) {
      // 与 set(key, value) 一致：写 null 即删除，带不带 ttl 都不改变该语义
      commands.del(key);
      return;
    }
    long ttlMillis = ExpiredKVCache.ttlMillis(timeout, unit);
    // 用 psetex 而非 setex：后者只支持秒级
    RedisInvalidExpireTime.run("set(key, value, timeout, unit)", ttlMillis,
            () -> commands.psetex(key, ttlMillis, value));
  }

  @Override
  public boolean setnx(String key, String value) {
    KVCache.assertKeyNotNull(key);
    // value 为 null 时不发命令：原样发给 lettuce 会被编码成空串写入，与 set 的"删除"口径不一致
    if (value == null) {
      return false;
    }
    return "OK".equals(commands.set(key, value, new SetArgs().nx()));
  }

  @Override
  public boolean setnx(String key, String value, long timeout, TimeUnit unit) {
    KVCache.assertKeyNotNull(key);
    if (value == null) {
      return false;
    }
    long ttlMillis = ExpiredKVCache.ttlMillis(timeout, unit);
    String result = RedisInvalidExpireTime.call("setnx(key, value, timeout, unit)", ttlMillis,
            () -> commands.set(key, value, new SetArgs().nx().px(ttlMillis)));
    return "OK".equals(result);
  }

  @Override
  public boolean containsKey(String key) {
    KVCache.assertKeyNotNull(key);
    Long exists = commands.exists(key);
    return exists != null && exists > 0;
  }

  @Override
  public String get(String key) {
    KVCache.assertKeyNotNull(key);
    return commands.get(key);
  }

  @Override
  public boolean delete(String key) {
    KVCache.assertKeyNotNull(key);
    Long deleted = commands.del(key);
    return deleted != null && deleted > 0;
  }

  /**
   * 原子地取值并删除，单条命令 {@code GETDEL}。
   * <p>
   * 覆盖接口默认实现（get + delete 两次往返，中间别人写进来的新值会被 delete 带走、
   * 返回的却是旧值）。
   * <p>
   * <b>要求服务端 Redis ≥ 6.2</b>；更低的版本上 {@link RedisUnknownCommand} 会把客户端
   * 抛出的 "unknown command" 统一翻译成 {@link UnsupportedOperationException}，本方法不降级、
   * 不兜底。
   *
   * @throws UnsupportedOperationException 服务端不支持 {@code GETDEL} 时
   */
  @Override
  public String remove(String key) {
    KVCache.assertKeyNotNull(key);
    return RedisUnknownCommand.call("GETDEL", () -> commands.getdel(key));
  }

  /**
   * <b>不提供</b>：{@code computeIfAbsent} 的语义是原子的 check-then-act，而 redis 无法原子
   * 表达 —— {@code mappingFunction} 是本地代码，服务端不参与求值，get 与 set 两次往返之间
   * 必然有窗口，并发下会重复计算、互相覆盖。
   * <p>
   * 本类声明 L1 + L2，而 L2 不允许"这一半原子、那一半不原子"，因此拒绝而非降级。
   * 需要该语义时用 {@code CacheHelper#queryWithPpf}（内部加锁），或自己用
   * {@code get} + {@code setnx} 拼。
   *
   * @throws UnsupportedOperationException 一律抛出
   */
  @Override
  public String computeIfAbsent(String key, Function<? super String, ? extends String> mappingFunction) {
    throw new UnsupportedOperationException(
            "redis 实现不支持 computeIfAbsent：它不是原子的复合操作，并发下会重复计算并互相覆盖。"
                    + "需要该语义时用 CacheHelper#queryWithPpf（内部加锁）。");
  }

  /**
   * 同 {@link #computeIfAbsent(String, Function)}。
   */
  @Override
  public String computeIfAbsent(String key, Function<? super String, ? extends String> mappingFunction,
                                long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException(
            "redis 实现不支持 computeIfAbsent：它不是原子的复合操作，并发下会重复计算并互相覆盖。"
                    + "需要该语义时用 CacheHelper#queryWithPpf（内部加锁）。");
  }

  /**
   * <b>不提供</b>：本实现拿不到"这个 cache 里现在有多少条"的口径。与 {@link #capacity()} 一样，
   * 答不上来就拒绝，不猜一个看起来成立的数字。
   *
   * @throws UnsupportedOperationException 一律抛出
   */
  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  /**
   * <b>不提供</b>：容量由服务端的 {@code maxmemory-policy} 全局决定，与某个 KVCache 实例无关。
   * 编造一个数字会让 {@code size() <= capacity()} 这类判断看起来成立，而 {@link #size()}
   * 恰恰是不支持的。
   *
   * @throws UnsupportedOperationException 一律抛出
   */
  @Override
  public int capacity() {
    throw new UnsupportedOperationException(
            "redis 实现不支持 capacity()：容量由服务端的 maxmemory-policy 全局决定，与单个 KVCache 实例无关。");
  }

  /**
   * 关闭由本对象创建的连接；外部传入的连接不会被关闭。
   */
  public void close() {
    if (connectionOwned) {
      connection.close();
    }
  }

}
