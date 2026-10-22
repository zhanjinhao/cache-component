package cn.addenda.component.cache;

import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 基于 Redisson 实现的 {@link ExpiredKVCache}。
 * <p>
 * <b>继承的规则</b>：{@link ExpiredKVCache} 的全部约定 —— 过期的 key 等同于没有值、
 * TTL 换算走 {@link ExpiredKVCache#ttlMillis}、value 为 null 时 {@code set} 按删除处理而
 * {@code setnx} 不写入、key 为 null 抛 {@link CacheIllegalArgumentException}。
 * <p>
 * <b>自身差异</b>：
 * <ul>
 *   <li>TTL 交给服务端维护：带 ttl 的 {@code set} 走 {@code RBucket.set(value, ttl, MILLISECONDS)}，
 *       下发的是时长，本地不存到期时刻；</li>
 *   <li>{@link #remove(String)} 用单命令 {@code RBucket.getAndDelete}（等价 {@code GETDEL}，
 *       <b>要求服务端 Redis ≥ 6.2</b>），两个 {@code setnx} 用 {@code RBucket.trySet}
 *       （等价 {@code SETNX}）—— 都是单命令原子；</li>
 *   <li>{@code computeIfAbsent} 做不到原子，{@code size} / {@code capacity} 答不上来，
 *       三者一律拒绝，见各方法说明；</li>
 *   <li>{@link RedissonClient} 由外部传入，本类只借用、<b>不负责关闭</b>。</li>
 * </ul>
 * <p>
 * <b>安全级别</b>：L1 + L2。{@link RedissonClient} 本身线程安全，本类字段全为 final、
 * 不持有自有可变状态，操作全部委托给它，因此实例可被多线程共享。
 *
 * @author addenda
 * @since 2023/6/3 16:25
 */
public class RedissonRedisKVCache implements ExpiredKVCache<String, String> {

  private final RedissonClient redissonClient;

  public RedissonRedisKVCache(RedissonClient redissonClient) {
    this.redissonClient = redissonClient;
  }

  @Override
  public void set(String key, String value) {
    KVCache.assertKeyNotNull(key);
    RBucket<Object> bucket = redissonClient.getBucket(key);
    // 参见 KVCache#set：写入 null 一律视为删除。
    // redisson 的 bucket.set(null) 恰好也是删除，但此处显式写出删除操作，不依赖该实现细节。
    if (value == null) {
      RedisCall.run(RedisCall.command("DEL", key), bucket::delete);
      return;
    }
    RedisCall.run(RedisCall.command("SET", key, value), () -> bucket.set(value));
  }

  @Override
  public void set(String key, String value, long timeout, TimeUnit unit) {
    KVCache.assertKeyNotNull(key);
    RBucket<Object> bucket = redissonClient.getBucket(key);
    if (value == null) {
      RedisCall.run(RedisCall.command("DEL", key), bucket::delete);
      return;
    }
    // TTL 换算统一走 ExpiredKVCache.ttlMillis：redisson 自己不做校验，
    // 非正 timeout 若原样下发会被服务端以 "ERR invalid expire time" 打回来。
    long ttlMillis = ExpiredKVCache.ttlMillis(timeout, unit);
    RedisCall.runWithTtl(RedisCall.command("PSETEX", key, ttlMillis), ttlMillis,
            () -> bucket.set(value, ttlMillis, TimeUnit.MILLISECONDS));
  }

  @Override
  public boolean setnx(String key, String value) {
    KVCache.assertKeyNotNull(key);
    // 参见 KVCache#setnx：value 为 null 表示"没有值"，此时不写入任何内容。
    if (value == null) {
      return false;
    }
    // RBucket.trySet 等价于 SETNX。
    return RedisCall.call(RedisCall.command("SET", key, value, "NX"),
            () -> redissonClient.getBucket(key).trySet(value));
  }

  @Override
  public boolean setnx(String key, String value, long timeout, TimeUnit unit) {
    KVCache.assertKeyNotNull(key);
    if (value == null) {
      return false;
    }
    long ttlMillis = ExpiredKVCache.ttlMillis(timeout, unit);
    return RedisCall.callWithTtl(RedisCall.command("SET", key, value, "NX", "PX", ttlMillis), ttlMillis,
            () -> redissonClient.getBucket(key).trySet(value, ttlMillis, TimeUnit.MILLISECONDS));
  }

  @Override
  public boolean containsKey(String key) {
    KVCache.assertKeyNotNull(key);
    RBucket<Object> bucket = redissonClient.getBucket(key);
    return RedisCall.call(RedisCall.command("GET", key), bucket::get) != null;
  }

  @Override
  public String get(String key) {
    KVCache.assertKeyNotNull(key);
    RBucket<String> bucket = redissonClient.getBucket(key);
    return RedisCall.call(RedisCall.command("GET", key), bucket::get);
  }

  @Override
  public boolean delete(String key) {
    KVCache.assertKeyNotNull(key);
    RBucket<String> bucket = redissonClient.getBucket(key);
    return RedisCall.call(RedisCall.command("DEL", key), bucket::delete);
  }

  /**
   * 原子地取值并删除（Redisson 的 {@code RBucket.getAndDelete}），覆盖接口的默认实现
   * （get + delete 两次往返：两次之间别人写进来的新值会被 delete 带走，而返回的还是旧值）。
   * <p>
   * 与另外三个 redis 实现不同，<b>本方法不要求服务端 Redis ≥ 6.2</b>：客户端把取值与删除
   * 合成一段服务端脚本下发，由服务端整体执行，旧版本也能用。
   * <p>
   * 服务端拒绝该命令时，{@link RedisCall} 会把客户端异常统一翻译成
   * {@link CacheUnsupportedOperationException}。
   *
   * @throws CacheUnsupportedOperationException 服务端不认识本方法下发的命令时
   */
  @Override
  public String remove(String key) {
    KVCache.assertKeyNotNull(key);
    RBucket<String> bucket = redissonClient.getBucket(key);
    // context 用 redis 命令名而不是客户端 API 名：翻译出来的消息说的是"服务端不支持哪个命令"
    return RedisCall.call(RedisCall.command("GETDEL", key), bucket::getAndDelete);
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

}
