package cn.addenda.component.cache.helper;

/**
 * <b>仅供测试</b>：把 {@link CacheHelper} 里包级可见的超时推导暴露给 {@code cache.test.*}。
 * <p>
 * {@code deleteTaskTimeout} 与 {@code MIN_DELETE_TASK_TIMEOUT_MS} 都是包级的（给私有嵌套类
 * {@code DeleteTask} 用），测试在别的包够不到。这里刻意只做转发、不做任何包装：它就是
 * "把那个入口交出来"这一个动作，与 {@code cn.addenda.component.cache.TestDelegateAccess}
 * 同一个套路。
 * <p>
 * 为什么不干脆把这两个成员改成 public：那是拿产品 API 换测试位置的方便。
 * 本类放在测试源码树下，不会进入产物。
 *
 * @author addenda
 */
public final class TestCacheHelperAccess {

  private TestCacheHelperAccess() {
  }

  /**
   * @return {@link CacheHelper} 里那个下限常量
   */
  public static long minDeleteTaskTimeoutMs() {
    return CacheHelper.MIN_DELETE_TASK_TIMEOUT_MS;
  }

  /**
   * @param delay 单次删除的耗时
   * @return 延迟删除任务的执行超时
   */
  public static long deleteTaskTimeout(long delay) {
    return CacheHelper.deleteTaskTimeout(delay);
  }

}
