package cn.addenda.component.cache;

import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 基于 spring-data-redis 的 {@link StringRedisTemplate} 实现的 {@link ExpiredKVCache}。
 * <p>
 * <b>继承的规则</b>：{@link ExpiredKVCache} 的全部约定 —— 过期的 key 等同于没有值、
 * TTL 换算走 {@link ExpiredKVCache#ttlMillis}、value 为 null 时 {@code set} 按删除处理而
 * {@code setnx} 不写入、key 为 null 抛 {@link CacheIllegalArgumentException}。
 * <p>
 * <b>自身差异</b>：
 * <ul>
 *   <li>TTL 交给服务端维护：带 ttl 的 {@code set} 走
 *       {@code opsForValue().set(key, value, ttl, MILLISECONDS)}，下发的是时长；</li>
 *   <li>{@link #remove(String)} 用单命令 {@code getAndDelete}（等价 {@code GETDEL}，
 *       <b>要求服务端 Redis ≥ 6.2</b>），两个 {@code setnx} 用 {@code setIfAbsent}
 *       （{@code SET NX} / {@code SET NX PX}）—— 都是单命令原子；</li>
 *   <li>{@code computeIfAbsent} 做不到原子，{@code size} / {@code capacity} 答不上来，
 *       三者一律拒绝，见各方法说明；</li>
 *   <li>{@link StringRedisTemplate} 由外部传入，本类只借用、<b>不负责关闭</b>。</li>
 * </ul>
 * <p>
 * <b>安全级别</b>：L1 + L2。{@link StringRedisTemplate} 本身线程安全，本类字段全为 final、
 * 不持有自有可变状态，操作全部委托给它，因此实例可被多线程共享。
 *
 * @author addenda
 * @since 2023/6/3 16:25
 */
public class StringRedisTemplateRedisKVCache implements ExpiredKVCache<String, String> {

  private final StringRedisTemplate stringRedisTemplate;

  public StringRedisTemplateRedisKVCache(StringRedisTemplate stringRedisTemplate) {
    this.stringRedisTemplate = stringRedisTemplate;
  }

  @Override
  public void set(String key, String value) {
    KVCache.assertKeyNotNull(key);
    // 参见 KVCache#set：写入 null 一律视为删除。
    // 若直接调用 opsForValue().set(key, null)，spring-data-redis 会抛出
    // IllegalArgumentException("Value must not be null!")。
    if (value == null) {
      RedisCall.run(RedisCall.command("DEL", key), () -> stringRedisTemplate.delete(key));
      return;
    }
    RedisCall.run(RedisCall.command("SET", key, value),
            () -> stringRedisTemplate.opsForValue().set(key, value));
  }

  @Override
  public void set(String key, String value, long timeout, TimeUnit unit) {
    KVCache.assertKeyNotNull(key);
    // 与 set(key, value) 一致：写入 null 即删除该 key。
    if (value == null) {
      RedisCall.run(RedisCall.command("DEL", key), () -> stringRedisTemplate.delete(key));
      return;
    }
    // TTL 换算统一走 ExpiredKVCache.ttlMillis：spring-data-redis 自己不做校验，
    // 非正 timeout 若原样下发会被服务端以 "ERR invalid expire time" 打回来。
    long ttlMillis = ExpiredKVCache.ttlMillis(timeout, unit);
    RedisCall.runWithTtl(RedisCall.command("PSETEX", key, ttlMillis), ttlMillis,
            () -> stringRedisTemplate.opsForValue()
                    .set(key, value, ttlMillis, TimeUnit.MILLISECONDS));
  }

  @Override
  public boolean setnx(String key, String value) {
    KVCache.assertKeyNotNull(key);
    // 参见 KVCache#setnx：value 为 null 表示"没有值"，此时不写入任何内容。
    if (value == null) {
      return false;
    }
    // SETNX。
    return Boolean.TRUE.equals(RedisCall.call(RedisCall.command("SET", key, value, "NX"),
            () -> stringRedisTemplate.opsForValue().setIfAbsent(key, value)));
  }

  @Override
  public boolean setnx(String key, String value, long timeout, TimeUnit unit) {
    KVCache.assertKeyNotNull(key);
    if (value == null) {
      return false;
    }
    // SET key value NX EX/PX ttl。TTL 换算统一走 ExpiredKVCache.ttlMillis，见它的说明。
    long ttlMillis = ExpiredKVCache.ttlMillis(timeout, unit);
    Boolean written = RedisCall.callWithTtl(
            RedisCall.command("SET", key, value, "NX", "PX", ttlMillis), ttlMillis,
            () -> stringRedisTemplate.opsForValue()
                    .setIfAbsent(key, value, ttlMillis, TimeUnit.MILLISECONDS));
    return Boolean.TRUE.equals(written);
  }

  @Override
  public boolean containsKey(String s) {
    KVCache.assertKeyNotNull(s);
    return Boolean.TRUE.equals(RedisCall.call(RedisCall.command("EXISTS", s),
            () -> stringRedisTemplate.hasKey(s)));
  }

  @Override
  public String get(String key) {
    KVCache.assertKeyNotNull(key);
    return RedisCall.call(RedisCall.command("GET", key),
            () -> stringRedisTemplate.opsForValue().get(key));
  }

  @Override
  public boolean delete(String key) {
    KVCache.assertKeyNotNull(key);
    return Boolean.TRUE.equals(RedisCall.call(RedisCall.command("DEL", key),
            () -> stringRedisTemplate.delete(key)));
  }

  /**
   * 原子地取值并删除，单条命令 {@code GETDEL}（spring-data-redis 的
   * {@code ValueOperations.getAndDelete}）。
   * <p>
   * 覆盖接口的默认实现（get + delete 两次往返）：两次之间别人写进来的新值会被 delete 带走，
   * 而返回的还是先前读到的旧值。
   * <p>
   * <b>要求服务端 Redis ≥ 6.2</b>；更低的版本上 {@link RedisCall} 会把客户端抛出的
   * "unknown command" 统一翻译成 {@link CacheUnsupportedOperationException}，本方法不降级、不兜底。
   *
   * @throws CacheUnsupportedOperationException 服务端不支持 {@code GETDEL} 时
   */
  @Override
  public String remove(String key) {
    KVCache.assertKeyNotNull(key);
    return RedisCall.call(RedisCall.command("GETDEL", key),
            () -> stringRedisTemplate.opsForValue().getAndDelete(key));
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
