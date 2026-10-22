package cn.addenda.component.cache;

import cn.addenda.component.base.string.Slf4jUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * 四个 redis 客户端调用命令时的统一异常翻译。
 * <p>
 * 同一件事在不同客户端上抛出的异常类型各不相同，业务侧无法统一处理。这里把它们收敛成
 * 本组件的三个异常：
 * <ul>
 *   <li>服务端不支持该命令 → {@link CacheUnsupportedOperationException}；</li>
 *   <li>过期时间等参数不被<b>本组件</b>允许穿透到存储层 → {@link CacheIllegalArgumentException}。
 *       这类参数有的在实现入口就拦下了，有的下发给服务端、由服务端拒绝，这里负责把后者的拒绝
 *       归一成组件自己的口径 —— 调用方看到的消息说的是"组件不接受"，而不是"服务端不接受"；</li>
 *   <li>连不上、连接被重置、命令超时等环境故障 → {@link CacheNetworkException}。</li>
 * </ul>
 * 其它异常（如 {@code WRONGTYPE} 等服务端业务错误）原样抛出，连实例都不换。
 * <p>
 * 两个"回文"特征是<b>唯一需要跟随 redis 措辞变化的地方</b>：改完跑
 * {@code RedisCallTest} 里的真实链路用例确认。
 *
 * @author addenda
 * @since 2026/10/19
 */
final class RedisCall {

  /**
   * "服务端不认识这条命令"。对着真实服务端抓的两种：
   * <pre>
   * ERR unknown command `GETDEL`, with args beginning with:
   * ... @user_script: 1: Unknown Redis command called from Lua script
   * </pre>
   * 两个词都必须<b>成词</b>：redis 还有 {@code ERR unknown subcommand ...}，文字很接近但意思是
   * "调用方用错了子命令"，不能混进来。
   */
  private static final Pattern UNKNOWN_COMMAND =
          Pattern.compile("\\bunknown\\b.*\\bcommand\\b", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /**
   * 存储层拒绝这个过期时间。抓到的两种回文（同一句，命令名不同）：
   * <pre>
   * ERR invalid expire time in psetex
   * ERR invalid expire time in set
   * </pre>
   * 存储层的拒绝只是<b>证据</b>，对外统一翻译成组件的
   * {@link CacheIllegalArgumentException}（"组件不允许该参数穿透"），不是"服务端不接受"。
   */
  private static final Pattern INVALID_EXPIRE_TIME =
          Pattern.compile("\\binvalid\\s+expire\\s+time\\b", Pattern.CASE_INSENSITIVE);

  private RedisCall() {
  }

  /**
   * 把命令与参数拼成<b>可直接粘贴到 redis-cli 执行</b>的形式：含空格、引号等特殊字符的参数
   * 用双引号包起来并转义，普通 token（key、{@code NX}/{@code PX} 等）原样输出。
   * <p>
   * 例：{@code command("SET", "user:1", "hello world", "NX")}
   * → {@code SET user:1 "hello world" NX}
   * <p>
   * <b>本方法必须不抛异常</b>：调用点写成
   * {@code RedisCall.run(RedisCall.command(...), () -> redisCall())}，上下文在真正的
   * redis 调用<b>之前</b>求值；这里一旦抛出，异常会落在 {@link #call} 的翻译逻辑之外，
   * 还会让后面的 redis 调用根本不发生。因此任何格式化失败都降级为
   * {@code UNKNOWN Command}，真正的问题仍由随后的 redis 调用及其异常暴露。
   *
   * @param command redis 命令名
   * @param args    命令参数（key、value、TTL、NX/PX 等）
   */
  static String command(String command, Object... args) {
    try {
      List<String> parts = new ArrayList<>(args.length + 1);
      parts.add(command);
      for (Object arg : args) {
        parts.add(arg instanceof Number ? String.valueOf(arg) : token(String.valueOf(arg)));
      }
      return String.join(" ", parts);
    } catch (Throwable e) {
      return "UNKNOWN Command";
    }
  }

  /**
   * 普通 token 原样输出；需要引用的参数按 redis-cli 的双引号规则转义。
   */
  private static String token(String value) {
    if (isPlainToken(value)) {
      return value;
    }
    StringBuilder sb = new StringBuilder(value.length() + 2).append('"');
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"':
          sb.append("\\\"");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        default:
          if (c < 0x20 || c == 0x7f) {
            sb.append(String.format("\\x%02x", (int) c));
          } else {
            sb.append(c);
          }
      }
    }
    return sb.append('"').toString();
  }

  private static boolean isPlainToken(String value) {
    if (value.isEmpty()) {
      return false;
    }
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      boolean safe = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
              || c == '_' || c == '-' || c == '.' || c == ':' || c == '@' || c == '/' || c == '+';
      if (!safe) {
        return false;
      }
    }
    return true;
  }

  /**
   * @param context redis-cli 形式的命令与参数（如 {@code GET user:1}、{@code SET user:1 v NX}），
   *                用于拼错误消息
   */
  static <T> T call(String context, Supplier<T> action) {
    return call(context, null, action);
  }

  /**
   * @param context   redis-cli 形式的命令与参数（如 {@code PSETEX user:1 60000}），用于拼错误消息
   * @param ttlMillis 下发给服务端的 TTL（毫秒），用于区分本次调用是否带 TTL
   */
  static <T> T callWithTtl(String context, long ttlMillis, Supplier<T> action) {
    return call(context, ttlMillis, action);
  }

  /**
   * @param context redis-cli 形式的命令与参数（如 {@code DEL user:1}），用于拼错误消息
   */
  static void run(String context, Runnable action) {
    call(context, null, () -> {
      action.run();
      return null;
    });
  }

  /**
   * @param context   redis-cli 形式的命令与参数（如 {@code PSETEX user:1 60000}），用于拼错误消息
   * @param ttlMillis 下发给服务端的 TTL（毫秒），用于区分本次调用是否带 TTL
   */
  static void runWithTtl(String context, long ttlMillis, Runnable action) {
    call(context, ttlMillis, () -> {
      action.run();
      return null;
    });
  }

  private static <T> T call(String context, Long ttlMillis, Supplier<T> action) {
    try {
      return action.get();
    } catch (RuntimeException e) {
      if (isUnknownCommand(e)) {
        throw new CacheUnsupportedOperationException(Slf4jUtils.format(
                "The server does not support this Redis command: {}. It usually requires a newer Redis"
                        + " version; the current server may be older.", context), e);
      }
      if (isInvalidExpireTime(e)) {
        throw new CacheIllegalArgumentException(invalidExpireTimeMessage(context, ttlMillis), e);
      }
      if (isNetworkFailure(e)) {
        throw new CacheNetworkException(
                Slf4jUtils.format("Failed to access Redis (network/timeout): {}", context), e);
      }
      throw e;
    }
  }

  /**
   * 只有<b>带 TTL 的调用</b>才有"换算后的毫秒数"可报。不带 TTL 的调用（{@code call/run}）
   * 没有这个参数，不能拼出 {@code "TTL 换算后是 null"} 这种误导性的消息。
   *
   * @param ttlMillis 下发给服务端的 TTL；{@code null} 表示本次调用没有 TTL 参数
   */
  private static String invalidExpireTimeMessage(String context, Long ttlMillis) {
    if (ttlMillis == null) {
      return Slf4jUtils.format("The cache component rejects this Redis call: {}. "
              + "The parameter is not allowed to reach the storage layer; "
              + "the storage layer returned 'invalid expire time'.", context);
    }
    return Slf4jUtils.format("The cache component rejects this Redis call: {}. "
                    + "The TTL is not allowed to reach the storage layer; "
                    + "the storage layer returned 'invalid expire time' "
                    + "(now + TTL must not overflow 64 bits). "
                    + "Use the overload without a TTL if you need a non-expiring entry.",
            context);
  }

  /**
   * 逐层看 cause：spring-data-redis 会在客户端异常外再包一层。
   */
  private static boolean isUnknownCommand(Throwable e) {
    return matches(e, UNKNOWN_COMMAND);
  }

  private static boolean isInvalidExpireTime(Throwable e) {
    return matches(e, INVALID_EXPIRE_TIME);
  }

  private static boolean matches(Throwable e, Pattern pattern) {
    for (Throwable t = e; t != null; t = next(t)) {
      String message = t.getMessage();
      if (message != null && pattern.matcher(message).find()) {
        return true;
      }
    }
    return false;
  }

  /**
   * 网络/超时故障。按<b>异常类的简单名</b>判（{@code Connection} / {@code Timeout}），
   * 覆盖四个客户端的全部类型，包括 spring-data-redis 的包装层；再按文案兜底。
   */
  private static boolean isNetworkFailure(Throwable e) {
    for (Throwable t = e; t != null; t = next(t)) {
      String simpleName = t.getClass().getSimpleName();
      if (simpleName.contains("Connection") || simpleName.contains("Timeout")) {
        return true;
      }
      String message = t.getMessage();
      if (message != null) {
        String lower = message.toLowerCase(Locale.ROOT);
        if (lower.contains("unable to connect") || lower.contains("connection refused")
                || lower.contains("connection reset") || lower.contains("broken pipe")
                || lower.contains("timed out")) {
          return true;
        }
      }
    }
    return false;
  }

  private static Throwable next(Throwable t) {
    Throwable cause = t.getCause();
    return cause == t ? null : cause;
  }

}
