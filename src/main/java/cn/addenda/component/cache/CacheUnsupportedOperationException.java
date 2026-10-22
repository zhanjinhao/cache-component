package cn.addenda.component.cache;

/**
 * 缓存组件里"操作不可用"的统一异常。
 * <p>
 * 统一了原先使用的 JDK {@link UnsupportedOperationException}：它太通用，业务系统配全局
 * 异常处理器时无法判断错误是否来自本组件。本类型的语义是<b>操作整体不可用</b> —— 实现做不到
 * （如只读外壳的写方法、{@code LruKKVCache} 的 {@code setnx}），或服务端版本不够
 * （如 {@code GETDEL}）。<b>入参不合法</b>属于另一类错误，用
 * {@link CacheIllegalArgumentException}。
 * <p>
 * 两者的共同父类型是 {@link CacheException}，业务侧只需认这一棵继承树。
 *
 * @author addenda
 * @since 2026/10/19
 */
public class CacheUnsupportedOperationException extends CacheException {

  public CacheUnsupportedOperationException() {
    super();
  }

  public CacheUnsupportedOperationException(String message) {
    super(message);
  }

  public CacheUnsupportedOperationException(String message, Throwable cause) {
    super(message, cause);
  }

  public CacheUnsupportedOperationException(Throwable cause) {
    super(cause);
  }

}
