package cn.addenda.component.cache.test.codec;

import cn.addenda.component.cache.codec.CacheCodecException;
import cn.addenda.component.cache.codec.ScalarValueSupport;

/**
 * 测试专用：把 {@link ScalarValueSupport} 的 protected 方法暴露为 public。
 * <p>
 * 测试类与生产类不在同一包，匿名子类新增的 public 方法无法通过
 * {@code ScalarValueSupport} 变量调用；Java 8 又没有 {@code var}，
 * 因此需要一个有名字的子类来承载这些公开方法。
 *
 * @author addenda
 */
public final class ExposedScalarValueSupport extends ScalarValueSupport {

  public boolean _hasUtf8Prefix(byte[] bytes) {
    return super.hasUtf8Prefix(bytes);
  }

  public boolean _hasUtf8Prefix(byte[] bytes, int offset, int length) {
    return super.hasUtf8Prefix(bytes, offset, length);
  }

  public boolean _isUtf8Start(byte first) {
    return super.isUtf8Start(first);
  }

  public String _scalarTypePrefix(byte[] bytes) {
    return super.scalarTypePrefix(bytes);
  }

  public String _scalarTypePrefix(byte[] bytes, int offset, int length) {
    return super.scalarTypePrefix(bytes, offset, length);
  }

  public String _decodeUtf8(byte[] bytes) {
    return super.decodeUtf8(bytes);
  }

  public String _decodeUtf8(byte[] bytes, int offset, int length) {
    return super.decodeUtf8(bytes, offset, length);
  }

  public byte[] _encodeUtf8(String text) {
    return super.encodeUtf8(text);
  }

  public boolean _isPrintableAscii(int c) {
    return super.isPrintableAscii(c);
  }

  public String _escape(String text) {
    return super.escape(text);
  }

  public String _unescape(String text) {
    return super.unescape(text);
  }

  public CacheCodecException _corrupted(String text, String reason) {
    return super.corrupted(text, reason);
  }
}
