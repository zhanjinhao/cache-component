package cn.addenda.component.cache;

import java.util.function.Supplier;

/**
 * <b>仅供测试</b>：把包级可见的 {@link RedisCall} 暴露给 {@code cache.test.*}。
 * <p>
 * 该类只服务于四个 redis KVCache 实现，本就该是包级的，不该为了测试改成 public。
 * 这里刻意只做转发、不做任何包装，与 {@link TestDelegateAccess} 同一个套路 ——
 * 两者都必须待在 {@link RedisCall} 所在的包里，包级成员跨不过去。
 * <p>
 * 放在测试源码树下，不会进入产物。
 *
 * @author addenda
 */
public final class TestRedisCallAccess {

  private TestRedisCallAccess() {
  }

  /**
   * 见 {@link RedisCall#call(String, Supplier)}。
   */
  public static <T> T call(String context, Supplier<T> action) {
    return RedisCall.call(context, action);
  }

  /**
   * 见 {@link RedisCall#callWithTtl(String, long, Supplier)}。
   */
  public static <T> T callWithTtl(String context, long ttlMillis, Supplier<T> action) {
    return RedisCall.callWithTtl(context, ttlMillis, action);
  }

  /**
   * 见 {@link RedisCall#command(String, Object...)}。
   */
  public static String command(String command, Object... args) {
    return RedisCall.command(command, args);
  }

}
