package cn.addenda.component.cache;

/**
 * 访问远端缓存（目前是 redis）时的<b>网络/超时</b>异常的统一类型。
 * <p>
 * 四个客户端（lettuce、lettuce cluster、redisson、spring-data-redis）对同一类环境故障
 * （连不上、连接被重置、命令超时等）抛出的异常类型各不相同；这里统一成
 * {@link CacheNetworkException}，原始客户端异常挂在 {@code cause} 上。
 * <p>
 * <b>只翻译这一类</b>：服务端不支持该命令仍抛 {@link CacheUnsupportedOperationException}；
 * 参数不被组件允许穿透时抛 {@link CacheIllegalArgumentException}。
 *
 * @author addenda
 * @since 2026/10/19
 */
public class CacheNetworkException extends CacheException {

  public CacheNetworkException() {
    super();
  }

  public CacheNetworkException(String message) {
    super(message);
  }

  public CacheNetworkException(String message, Throwable cause) {
    super(message, cause);
  }

  public CacheNetworkException(Throwable cause) {
    super(cause);
  }

}
