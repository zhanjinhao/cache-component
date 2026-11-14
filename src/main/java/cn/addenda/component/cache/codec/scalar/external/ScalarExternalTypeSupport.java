package cn.addenda.component.cache.codec.scalar.external;

import cn.addenda.component.cache.codec.ScalarValueSupport;

/**
 * ScalarExternalType / ScalarExternalArrayType 实现的公共工具基类。
 * <p>
 * 承载与具体类型无关的文本解析、转义和错误转换工具；类型注册由
 * ScalarExternalTypeRegistry 完成。
 */
public abstract class ScalarExternalTypeSupport extends ScalarValueSupport {

  /**
   * 把文本解析为 boolean；文本只能是 "true" 或 "false"。
   */
  protected boolean parseBoolean(String text) {
    requireText(text, "invalid boolean");
    if ("true".equals(text)) {
      return true;
    }
    if ("false".equals(text)) {
      return false;
    }
    throw corrupted(text, "invalid boolean");
  }

  /**
   * 把文本解析为 byte；非法数字抛 {@link cn.addenda.component.cache.codec.CacheCodecException}。
   */
  protected byte parseByte(String text) {
    requireText(text, "invalid byte");
    try {
      return Byte.parseByte(text);
    } catch (NumberFormatException e) {
      throw corrupted(text, "invalid byte");
    }
  }

  /**
   * 把文本解析为 short；非法数字抛 {@link cn.addenda.component.cache.codec.CacheCodecException}。
   */
  protected short parseShort(String text) {
    requireText(text, "invalid short");
    try {
      return Short.parseShort(text);
    } catch (NumberFormatException e) {
      throw corrupted(text, "invalid short");
    }
  }

  /**
   * 把文本解析为 int；非法数字抛 {@link cn.addenda.component.cache.codec.CacheCodecException}。
   */
  protected int parseInt(String text) {
    requireText(text, "invalid int");
    try {
      return Integer.parseInt(text);
    } catch (NumberFormatException e) {
      throw corrupted(text, "invalid int");
    }
  }

  /**
   * 把文本解析为 long；非法数字抛 {@link cn.addenda.component.cache.codec.CacheCodecException}。
   */
  protected long parseLong(String text) {
    requireText(text, "invalid long");
    try {
      return Long.parseLong(text);
    } catch (NumberFormatException e) {
      throw corrupted(text, "invalid long");
    }
  }

  /**
   * 把文本解析为 float；非法数字抛 {@link cn.addenda.component.cache.codec.CacheCodecException}。
   */
  protected float parseFloat(String text) {
    requireText(text, "invalid float");
    try {
      return Float.parseFloat(text);
    } catch (NumberFormatException e) {
      throw corrupted(text, "invalid float");
    }
  }

  /**
   * 把文本解析为 double；非法数字抛 {@link cn.addenda.component.cache.codec.CacheCodecException}。
   */
  protected double parseDouble(String text) {
    requireText(text, "invalid double");
    try {
      return Double.parseDouble(text);
    } catch (NumberFormatException e) {
      throw corrupted(text, "invalid double");
    }
  }

  /**
   * 把文本解析为单个 UTF-16 字符；文本先执行反转义，长度必须为 1。
   */
  protected Character parseCharacter(String text) {
    requireText(text, "character must contain exactly one UTF-16 code unit");
    String character = unescape(text);
    if (character.length() != 1) {
      throw corrupted(text, "character must contain exactly one UTF-16 code unit");
    }
    return character.charAt(0);
  }

  /**
   * 校验 payload 文本非 null；null 统一转换为对应解析方法的损坏异常。
   */
  private void requireText(String text, String reason) {
    if (text == null) {
      throw corrupted(null, reason);
    }
  }
}
