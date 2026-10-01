package cn.addenda.component.cache.helper;

import cn.addenda.component.cache.ExpiredKVCache;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.concurrent.TimeUnit;

/**
 * 基于Lettuce实现的KVCache。
 * <p/>
 * <b>线程安全</b>：字段全是 final、没有自有可变状态，所有操作都委托给线程安全的
 * {@link StatefulRedisConnection}（它内部把命令写进同一个 channel 的 FIFO 队列，
 * 响应按序回来，多线程可以共享一条连接），所以整个对象可以被多线程共享使用。
 *
 * @author addenda
 * @since 2023/6/3 16:25
 */
public class LettuceRedisKVCache implements ExpiredKVCache<String, String> {

  private final StatefulRedisConnection<String, String> connection;

  private final RedisCommands<String, String> commands;

  /**
   * 连接是否由当前对象创建。由当前对象创建的连接才由当前对象负责关闭，
   * 外部传入的连接生命周期由外部管理。
   */
  private final boolean connectionOwned;

  /**
   * @param redisClient redis客户端。创建的连接由当前对象负责关闭
   */
  public LettuceRedisKVCache(RedisClient redisClient) {
    this(redisClient.connect(), true);
  }

  /**
   * @param connection 外部管理的连接，不会被当前对象关闭
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
    if (value == null) {
      // redis 里没有 java null 值。写 null 语义上就是"没有值"，等价于把这个 key 删掉 ——
      // 这样 get 返回 null，和 {@link ExpiredHashMapKVCache}、RedissonRedisKVCache 对齐。
      // 不这么做的话 lettuce 会把 null 编码成空串，get 出来是 ""，等于静默篡改数据。
      commands.del(key);
      return;
    }
    commands.set(key, value);
  }

  @Override
  public void set(String key, String value, long timeout, TimeUnit unit) {
    if (value == null) {
      // 同上：写 null 就是删掉，带不带 ttl 都一样
      commands.del(key);
      return;
    }
    // lettuce只提供了秒级的setex，使用psetex以支持毫秒级的ttl
    commands.psetex(key, toRedisMillis(timeout, unit), value);
  }

  @Override
  public boolean containsKey(String key) {
    Long exists = commands.exists(key);
    return exists != null && exists > 0;
  }

  @Override
  public String get(String key) {
    return commands.get(key);
  }

  @Override
  public boolean delete(String key) {
    Long deleted = commands.del(key);
    return deleted != null && deleted > 0;
  }

  @Override
  public long size() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long capacity() {
    return Long.MAX_VALUE;
  }

  /**
   * 关闭由当前对象创建的连接。外部传入的连接不会被关闭。
   */
  public void close() {
    if (connectionOwned) {
      connection.close();
    }
  }

  /**
   * redis不接受非正的过期时间，这里向上取整成1ms。
   * 非正的过期时间表达的是"立刻过期"，1ms后过期具备相同的语义。
   */
  private long toRedisMillis(long timeout, TimeUnit unit) {
    return Math.max(1L, unit.toMillis(timeout));
  }

}
