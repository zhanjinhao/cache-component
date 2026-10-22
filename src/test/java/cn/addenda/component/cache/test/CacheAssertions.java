package cn.addenda.component.cache.test;

import cn.addenda.component.cache.CacheIllegalArgumentException;
import cn.addenda.component.cache.CacheUnsupportedOperationException;
import org.junit.Assert;

/**
 * 契约测试共用的断言。
 * <p>
 * 抽出来的原因：同一条断言要在六七个实现上各跑一遍，复制那么多份之后
 * 很容易只改了其中一份。
 *
 * @author addenda
 */
public final class CacheAssertions {

  private CacheAssertions() {
  }

  /**
   * 断言这次调用会被 {@link CacheIllegalArgumentException} 拒绝。
   * <p>
   * 用在两类入参上：null key（各实现都要拦，见 {@code KVCache#assertKeyNotNull}），
   * 以及构造器里的非法容量 / null delegate。
   *
   * @param what       出错的场景描述，拼进失败信息里，比如 {@code "LruKVCache: set"}
   * @param invocation 实际调用
   */
  public static void assertRejected(String what, Runnable invocation) {
    try {
      invocation.run();
      Assert.fail(what + " 应该抛 CacheIllegalArgumentException，但没有");
    } catch (CacheIllegalArgumentException expected) {
      // ok
    }
  }

  /**
   * 断言这个操作在这个实现上<b>明确不支持</b>。
   * <p>
   * 用在语义对不上的地方，比如 {@code LruKKVCache} 的 {@code setnx} ——
   * 它的写入要过 LRU-K 的准入闸门，和"不存在就写、写了就有"对不上，
   * 所以宁可抛 {@link CacheUnsupportedOperationException} 也不返回一个需要猜的 false。
   *
   * @param what       出错的场景描述
   * @param invocation 实际调用
   */
  public static void assertNotSupported(String what, Runnable invocation) {
    try {
      invocation.run();
      Assert.fail(what + " 应该抛 CacheUnsupportedOperationException，但没有");
    } catch (CacheUnsupportedOperationException expected) {
      // ok
    }
  }

}
