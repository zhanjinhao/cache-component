package cn.addenda.component.cache.test;

import cn.addenda.component.cache.KVCache;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * <b>仅供测试</b>：临时修改 {@link KVCache#UNLIMITED_CAPACITY}，用来构造"超过上限"的输入 ——
 * 上限取 {@code int} 上界时，"比它还大"根本写不出来。
 * <p>
 * 能改得动的前提是它<b>不是编译期常量</b>（声明处用的是 {@code Integer.valueOf}）：
 * 编译期常量会被调用方内联，运行时改字段不会生效。
 * <p>
 * 接口字段隐式 {@code static final}，先去掉 FINAL 修饰符再写（JDK 8 可用；9+ 对
 * {@code Field.modifiers} 的反射写入会受限）。
 * <p>
 * <b>这是全局副作用</b>：调用方必须 {@code try/finally} 恢复原值；测试串行执行。
 *
 * @author addenda
 */
public final class TestCapacityLimit {

  private TestCapacityLimit() {
  }

  /**
   * 把上限改成 {@code value}。
   *
   * @return 修改前的值，供调用方在 {@code finally} 里恢复
   */
  public static int setUnlimitedCapacity(int value) {
    try {
      Field field = KVCache.class.getField("UNLIMITED_CAPACITY");
      Field modifiers = Field.class.getDeclaredField("modifiers");
      modifiers.setAccessible(true);
      modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
      int old = field.getInt(null);
      field.setInt(null, value);
      return old;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("无法临时修改 KVCache.UNLIMITED_CAPACITY", e);
    }
  }

}
