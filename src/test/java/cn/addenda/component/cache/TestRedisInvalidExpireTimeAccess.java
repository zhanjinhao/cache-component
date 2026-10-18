package cn.addenda.component.cache;

import java.util.function.Supplier;

/**
 * <b>仅供测试</b>：把包级可见的 {@link RedisInvalidExpireTime} 暴露给 {@code cache.test.*}。
 * <p>
 * 与 {@link TestRedisUnknownCommandAccess} 同一个套路：被测类本就该是包级的，不为了测试改成
 * {@code public}，这里只做转发。必须待在 {@link RedisInvalidExpireTime} 所在的包里。
 * <p>
 * 放在测试源码树下，不会进入产物。
 *
 * @author addenda
 */
public final class TestRedisInvalidExpireTimeAccess {

  private TestRedisInvalidExpireTimeAccess() {
  }

  /**
   * 见 {@link RedisInvalidExpireTime#call(String, long, Supplier)}。
   */
  public static <T> T call(String api, long ttlMillis, Supplier<T> action) {
    return RedisInvalidExpireTime.call(api, ttlMillis, action);
  }

}
