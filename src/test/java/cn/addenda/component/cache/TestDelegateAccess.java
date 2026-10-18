package cn.addenda.component.cache;

/**
 * <b>仅供测试</b>：读取包装类的底层存储。
 * <p>
 * {@link KVCacheWrapper#getKvCacheDelegate()} 是 {@code protected} 的 —— 拿到它就绕过了
 * 包装层的一切保护（加锁、统计同步），所以不能是公开 API，见该字段上的说明。
 * 但测试<b>恰恰需要</b>这层"绕过"：大量用例要断言"底层存储里到底还剩几条"
 * （{@code size()} 取自数据层，与账本可能不一致），这正是绕开包装层才能观察到的。
 * <p>
 * 测试都在 {@code cn.addenda.component.cache.test.*} 包，够不到 {@code protected}；
 * 而 {@code protected} 同时包含"同包可访问"，因此把入口开在与
 * {@link KVCacheWrapper} 同一个包里。放在测试源码树下，不会进入产物。
 * <p>
 * 这里刻意只做转发、不做任何包装：它就是"把底层存储交出来"这一个动作。
 */
public final class TestDelegateAccess {

  private TestDelegateAccess() {
  }

  /**
   * @param wrapper 任意包装类实例
   * @return 它的底层存储
   */
  public static <K, V> KVCache<K, V> delegateOf(KVCacheWrapper<K, V> wrapper) {
    return wrapper.getKvCacheDelegate();
  }

}
