package cn.addenda.component.cache.test.codec.scalar.external;

import cn.addenda.component.cache.codec.scalar.external.ScalarExternalTypeSupport;

/**
 * 测试专用：把 {@link ScalarExternalTypeSupport} 的 protected 解析方法暴露为 public。
 *
 * @author addenda
 */
public final class ExposedScalarExternalTypeSupport extends ScalarExternalTypeSupport {

  public boolean _parseBoolean(String text) {
    return super.parseBoolean(text);
  }

  public byte _parseByte(String text) {
    return super.parseByte(text);
  }

  public short _parseShort(String text) {
    return super.parseShort(text);
  }

  public int _parseInt(String text) {
    return super.parseInt(text);
  }

  public long _parseLong(String text) {
    return super.parseLong(text);
  }

  public float _parseFloat(String text) {
    return super.parseFloat(text);
  }

  public double _parseDouble(String text) {
    return super.parseDouble(text);
  }

  public Character _parseCharacter(String text) {
    return super.parseCharacter(text);
  }
}
