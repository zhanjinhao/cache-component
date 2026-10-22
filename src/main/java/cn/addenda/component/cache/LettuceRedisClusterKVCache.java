package cn.addenda.component.cache;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.SetArgs;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 基于 Lettuce 实现的 {@link ExpiredKVCache}，用于 redis cluster 模式。
 * <p>
 * <b>继承的规则</b>：{@link ExpiredKVCache} 的全部约定 —— 过期的 key 等同于没有值、
 * TTL 换算走 {@link ExpiredKVCache#ttlMillis}、value 为 null 时 {@code set} 按删除处理而
 * {@code setnx} 不写入、key 为 null 抛 {@link CacheIllegalArgumentException}。
 * <p>
 * <b>自身差异</b>：与 {@link LettuceRedisKVCache} 完全一致 —— TTL 交给服务端维护
 * （{@code PSETEX}）、{@link #remove(String)} 用 {@code GETDEL}、两个 {@code setnx} 用
 * {@code SET ... NX}、{@code computeIfAbsent} 与 {@code size} / {@code capacity} 一律拒绝，
 * 连接生命周期同 {@link #close()}。见该类的说明，此处不重复。
 * <p>
 * 唯一的区别是类型：{@code RedisClient -> RedisClusterClient}、
 * {@code StatefulRedisConnection -> StatefulRedisClusterConnection}、
 * {@code RedisCommands -> RedisAdvancedClusterCommands}。后两者继承同一套
 * {@code RedisStringCommands} / {@code RedisKeyCommands}，本类用到的方法签名一致。
 * {@link StatefulRedisClusterConnection} 是门面，背后按节点各持一条连接；
 * 单个命令仍按 key 的 slot 路由，不存在跨 slot 问题。
 * <p>
 * <b>安全级别</b>：L1 + L2，与 {@link LettuceRedisKVCache} 同理。
 *
 * @author addenda
 */
public class LettuceRedisClusterKVCache implements ExpiredKVCache<String, String> {

  private final StatefulRedisClusterConnection<String, String> connection;

  private final RedisAdvancedClusterCommands<String, String> commands;

  /**
   * 连接是否由本对象创建。只有自己创建的才由自己关闭，外部传入的由外部管理。
   */
  private final boolean connectionOwned;

  /**
   * @param redisClusterClient 集群客户端。由本对象创建的连接由本对象负责关闭。
   */
  public LettuceRedisClusterKVCache(RedisClusterClient redisClusterClient) {
    this(connect(redisClusterClient), true);
  }

  /**
   * 建连也可能因网络失败而抛异常，与后续命令同口径翻译。
   */
  private static StatefulRedisClusterConnection<String, String> connect(RedisClusterClient redisClusterClient) {
    return RedisCall.call("connect", redisClusterClient::connect);
  }

  /**
   * @param connection 由外部管理的连接，本对象不会关闭该连接。
   */
  public LettuceRedisClusterKVCache(StatefulRedisClusterConnection<String, String> connection) {
    this(connection, false);
  }

  private LettuceRedisClusterKVCache(StatefulRedisClusterConnection<String, String> connection,
                                     boolean connectionOwned) {
    this.connection = connection;
    this.commands = connection.sync();
    this.connectionOwned = connectionOwned;
  }

  @Override
  public void set(String key, String value) {
    KVCache.assertKeyNotNull(key);
    if (value == null) {
      // redis 中不存在 java null 值。写入 null 在语义上表示"没有值"，等价于删除该 key。
      // 该处理保证 get 返回 null，与其它 ExpiredKVCache 实现保持一致。
      // 若不删除，lettuce 会将 null 编码为空串，get 返回 ""，等同于静默篡改数据。
      RedisCall.run(RedisCall.command("DEL", key), () -> commands.del(key));
      return;
    }
    RedisCall.run(RedisCall.command("SET", key, value), () -> commands.set(key, value));
  }

  @Override
  public void set(String key, String value, long timeout, TimeUnit unit) {
    KVCache.assertKeyNotNull(key);
    if (value == null) {
      // 与 set(key, value) 一致：写入 null 即删除该 key，是否携带 ttl 均不改变该语义。
      RedisCall.run(RedisCall.command("DEL", key), () -> commands.del(key));
      return;
    }
    long ttlMillis = ExpiredKVCache.ttlMillis(timeout, unit);
    // lettuce 仅提供秒级的 setex，此处使用 psetex 以支持毫秒级的 ttl。
    RedisCall.runWithTtl(RedisCall.command("PSETEX", key, ttlMillis), ttlMillis,
            () -> commands.psetex(key, ttlMillis, value));
  }

  @Override
  public boolean setnx(String key, String value) {
    KVCache.assertKeyNotNull(key);
    // 参见 KVCache#setnx：value 为 null 表示"没有值"，此时不写入任何内容。
    // 原样发送给 lettuce 会将 null 编码为空串写入，get 返回 ""，与 set 的"删除"语义不一致。
    if (value == null) {
      return false;
    }
    // SET key value NX：redis 单命令原子操作。
    return "OK".equals(RedisCall.call(RedisCall.command("SET", key, value, "NX"),
            () -> commands.set(key, value, new SetArgs().nx())));
  }

  @Override
  public boolean setnx(String key, String value, long timeout, TimeUnit unit) {
    KVCache.assertKeyNotNull(key);
    if (value == null) {
      return false;
    }
    // SET key value NX PX ttl。
    long ttlMillis = ExpiredKVCache.ttlMillis(timeout, unit);
    String result = RedisCall.callWithTtl(RedisCall.command("SET", key, value, "NX", "PX", ttlMillis), ttlMillis,
            () -> commands.set(key, value, new SetArgs().nx().px(ttlMillis)));
    return "OK".equals(result);
  }

  @Override
  public boolean containsKey(String key) {
    KVCache.assertKeyNotNull(key);
    Long exists = RedisCall.call(RedisCall.command("EXISTS", key),
            () -> commands.exists(key));
    return exists != null && exists > 0;
  }

  @Override
  public String get(String key) {
    KVCache.assertKeyNotNull(key);
    return RedisCall.call(RedisCall.command("GET", key), () -> commands.get(key));
  }

  @Override
  public boolean delete(String key) {
    KVCache.assertKeyNotNull(key);
    Long deleted = RedisCall.call(RedisCall.command("DEL", key), () -> commands.del(key));
    return deleted != null && deleted > 0;
  }

  /**
   * 原子地取值并删除，单条命令 {@code GETDEL}，理由见 {@link LettuceRedisKVCache#remove(String)}。
   *
   * @throws CacheUnsupportedOperationException 服务端不支持 {@code GETDEL} 时，见
   *                                          {@link LettuceRedisKVCache#remove(String)}
   */
  @Override
  public String remove(String key) {
    KVCache.assertKeyNotNull(key);
    return RedisCall.call(RedisCall.command("GETDEL", key), () -> commands.getdel(key));
  }

  /**
   * <b>不提供</b>，与 {@link LettuceRedisKVCache} 同，
   * 见 {@link LettuceRedisKVCache#computeIfAbsent(String, Function)} 的说明。
   *
   * @throws CacheUnsupportedOperationException 一律抛出
   */
  @Override
  public String computeIfAbsent(String key, Function<? super String, ? extends String> mappingFunction) {
    throw new CacheUnsupportedOperationException(
            "The redis implementation does not support computeIfAbsent.");
  }

  @Override
  public String computeIfAbsent(String key, Function<? super String, ? extends String> mappingFunction,
                                long timeout, TimeUnit unit) {
    throw new CacheUnsupportedOperationException(
            "The redis implementation does not support computeIfAbsent.");
  }

  @Override
  public int size() {
    throw new CacheUnsupportedOperationException(
            "The redis implementation does not support size(): the server has no per-instance entry-count metric.");
  }

  /**
   * <b>不提供</b>，理由见 {@link LettuceRedisKVCache#capacity()}。
   *
   * @throws CacheUnsupportedOperationException 一律抛出
   */
  @Override
  public int capacity() {
    throw new CacheUnsupportedOperationException(
            "The redis implementation does not support capacity(): capacity is determined globally by the"
                    + " server's maxmemory-policy and is unrelated to a single KVCache instance.");
  }

  /**
   * 关闭由本对象创建的连接；外部传入的连接不会被关闭。
   */
  public void close() {
    if (connectionOwned) {
      RedisCall.run("close", connection::close);
    }
  }

}
