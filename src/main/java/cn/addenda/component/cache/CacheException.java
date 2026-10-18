package cn.addenda.component.cache;

import cn.addenda.component.base.exception.SystemException;

/**
 * 缓存模块的统一异常，继承自 {@link SystemException}。
 *
 * @author addenda
 * @since 2023/05/30 19:16
 */
public class CacheException extends SystemException {

  public CacheException() {
    super();
  }

  public CacheException(String message) {
    super(message);
  }

  public CacheException(String message, Throwable cause) {
    super(message, cause);
  }

  public CacheException(Throwable cause) {
    super(cause);
  }

  public CacheException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  @Override
  public String moduleName() {
    return "cache";
  }

  @Override
  public String componentName() {
    return "cache";
  }
}
