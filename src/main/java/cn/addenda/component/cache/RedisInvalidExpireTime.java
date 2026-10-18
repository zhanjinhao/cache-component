package cn.addenda.component.cache;

import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * 把"服务端不接受这个过期时间"翻译成统一的 {@link IllegalArgumentException}。
 * <p>
 * 触发条件只有一个：换算后的 TTL 太大，服务端算 {@code 当前时刻 + TTL} 时 64 位溢出。
 * 非正 TTL 不会走到这里 —— {@link ExpiredKVCache#ttlMillis} 已把它们取整为 1ms。
 * <p>
 * 四个 redis 客户端对同一条回文的异常类型各不相同（lettuce 的
 * {@code RedisCommandExecutionException}、redisson 的 {@code RedisException}、
 * spring-data-redis 的 {@code RedisSystemException}），调用方没法统一处理 —— 这里统一成
 * {@link IllegalArgumentException}（本组件表示"参数不合法"的类型），原始异常挂在
 * {@code cause} 上，消息里带上换算后的毫秒值。
 * <p>
 * <b>只翻译这一种</b>：连不上、超时、类型错等一律原样抛出。
 *
 * @author addenda
 */
final class RedisInvalidExpireTime {

  /**
   * <b>redis 改措辞时只改这一处</b>：改完跑 {@code RedisInvalidExpireTimeTest} 里的真实链路
   * 用例确认，它们会打日志给出当前文案。
   * <p>
   * 对着真实服务端抓的两种回文（同一句，命令名不同）：
   * <pre>
   * ERR invalid expire time in psetex
   * ERR invalid expire time in set
   * </pre>
   * 匹配整句而不是"invalid"单词：redis 还有别的 {@code ERR invalid ...} 回文，
   * 只有这一句代表"TTL 溢出"。
   */
  private static final Pattern INVALID_EXPIRE_TIME =
          Pattern.compile("\\binvalid\\s+expire\\s+time\\b", Pattern.CASE_INSENSITIVE);

  private RedisInvalidExpireTime() {
  }

  /**
   * 无返回值的版本。
   *
   * @param api       公开方法名，如 {@code set(key, value, timeout, unit)}，用于拼错误消息
   * @param ttlMillis 下发给服务端的 TTL（毫秒），用于拼错误消息
   * @param action    实际调用
   * @throws IllegalArgumentException 服务端拒绝 {@code ttlMillis} 时
   */
  static void run(String api, long ttlMillis, Runnable action) {
    call(api, ttlMillis, () -> {
      action.run();
      return null;
    });
  }

  /**
   * @param api       公开方法名，用于拼错误消息
   * @param ttlMillis 下发给服务端的 TTL（毫秒），用于拼错误消息
   * @param action    实际调用
   * @return {@code action} 的返回值
   * @throws IllegalArgumentException 服务端拒绝 {@code ttlMillis} 时
   */
  static <T> T call(String api, long ttlMillis, Supplier<T> action) {
    try {
      return action.get();
    } catch (RuntimeException e) {
      if (!isInvalidExpireTime(e)) {
        throw e;
      }
      throw new IllegalArgumentException(
              "服务端不接受这个过期时间：" + api + " 的 TTL 换算后是 " + ttlMillis
                      + " ms，服务端要求\"当前时刻 + TTL\"不溢出 64 位。"
                      + "需要\"永不过期\"时请使用不带 TTL 的重载。", e);
    }
  }

  /**
   * 逐层看 cause：spring-data-redis 会在客户端异常外再包一层，原始回文在更里面。
   */
  private static boolean isInvalidExpireTime(Throwable e) {
    Throwable t = e;
    while (t != null) {
      String message = t.getMessage();
      if (message != null && INVALID_EXPIRE_TIME.matcher(message).find()) {
        return true;
      }
      Throwable cause = t.getCause();
      t = cause == t ? null : cause;
    }
    return false;
  }

}
