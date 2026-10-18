package cn.addenda.component.cache;

import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * 把"服务端不认识这条命令"翻译成统一的 {@link UnsupportedOperationException}。
 * <p>
 * 四个 redis 客户端（lettuce、lettuce cluster、redisson、spring-data-redis）对同一条
 * {@code ERR unknown command} 抛出的异常类型各不相同，调用方没法统一处理 —— 这里统一成
 * {@link UnsupportedOperationException}（本组件表示"该操作不可用"的一贯类型），
 * 并把客户端原始异常挂在 {@code cause} 上。
 * <p>
 * <b>只翻译这一种</b>：连不上、超时、类型错等一律原样抛出 —— 它们不是"不支持"。
 *
 * @author addenda
 */
final class RedisUnknownCommand {

  /**
   * 判定用的回文特征。<b>redis 改措辞时只改这一处</b>：改完跑
   * {@code RedisUnknownCommandTest} 里那几条真实链路用例确认，它们会打日志给出当前文案。
   * <p>
   * 目前匹配的两种"不认识这条命令"（对着真实服务端抓的）：
   * <pre>
   * ERR unknown command `GETDEL`, with args beginning with:
   * ... @user_script: 1: Unknown Redis command called from Lua script
   * </pre>
   * 两个词都必须<b>成词</b>：redis 还有一条 {@code ERR unknown subcommand ...} 的官方回文
   * （子命令写错时给出），文字很接近但意思是"调用方用错了子命令"、不是"服务端不支持"，
   * 不能混进来。
   */
  private static final Pattern UNKNOWN_COMMAND =
          Pattern.compile("\\bunknown\\b.*\\bcommand\\b", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private RedisUnknownCommand() {
  }

  /**
   * @param command 命令名，如 {@code GETDEL}，用于拼错误消息
   * @param action  实际调用
   * @throws UnsupportedOperationException 服务端不认识 {@code command} 时
   */
  static <T> T call(String command, Supplier<T> action) {
    try {
      return action.get();
    } catch (RuntimeException e) {
      if (!isUnknownCommand(e)) {
        throw e;
      }
      throw new UnsupportedOperationException(
              "服务端不支持命令 " + command + "（本方法要求更新的 Redis 版本，当前服务端更旧）。", e);
    }
  }

  /**
   * 逐层看 cause：spring-data-redis 会在客户端异常外再包一层，原始回文在更里面。
   */
  private static boolean isUnknownCommand(Throwable e) {
    Throwable t = e;
    while (t != null) {
      String message = t.getMessage();
      if (message != null && UNKNOWN_COMMAND.matcher(message).find()) {
        return true;
      }
      Throwable cause = t.getCause();
      t = cause == t ? null : cause;
    }
    return false;
  }

}
