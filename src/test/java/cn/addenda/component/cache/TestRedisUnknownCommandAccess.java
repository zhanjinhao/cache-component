package cn.addenda.component.cache;

import java.util.function.Supplier;

/**
 * <b>仅供测试</b>：把包级可见的 {@link RedisUnknownCommand} 暴露给 {@code cache.test.*}。
 * <p>
 * 该类只服务于四个 redis KVCache 实现，本就该是包级的，不该为了测试改成 public。
 * 这里刻意只做转发、不做任何包装，与 {@link TestDelegateAccess} 同一个套路 ——
 * 两者都必须待在 {@link RedisUnknownCommand} 所在的包里，包级成员跨不过去。
 * <p>
 * 放在测试源码树下，不会进入产物。
 *
 * @author addenda
 */
public final class TestRedisUnknownCommandAccess {

  private TestRedisUnknownCommandAccess() {
  }

  /**
   * 见 {@link RedisUnknownCommand#call(String, Supplier)}。
   *
   * @param command 命令名，用于拼错误消息
   * @param action  实际调用
   * @return {@code action} 的返回值
   */
  public static <T> T call(String command, Supplier<T> action) {
    return RedisUnknownCommand.call(command, action);
  }

}
