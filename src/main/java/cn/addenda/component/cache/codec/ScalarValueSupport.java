package cn.addenda.component.cache.codec;

import cn.addenda.component.base.string.Slf4jUtils;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;

/**
 * scalar 文本 wire 的 UTF-8 编解码工具。
 * <p>
 * {@link StringIdentityType}、ScalarExternalTypeRegistry、ScalarEmbeddedTypeRegistry
 * 共享这些方法。
 *
 * @author addenda
 */
public abstract class ScalarValueSupport {

  /**
   * escape/unescape 使用的转义引导字符。
   */
  private static final char BACKSLASH = '\\';

  /**
   * escape/unescape 需要保护的引号字符。
   */
  private static final char DOUBLE_QUOTE = '"';

  /**
   * 创建 scalar 文本工具基类。
   */
  protected ScalarValueSupport() {
  }

  /**
   * 只判断首字节是否可能是 UTF-8 文本的起始字节，不扫描整个数组。
   * <p>
   * 返回 true 表示首字节符合 UTF-8 起始字节范围；整段字节的合法性由
   * {@link #decodeUtf8(byte[])} 严格校验。
   */
  protected boolean hasUtf8Prefix(byte[] bytes) {
    return bytes != null && hasUtf8Prefix(bytes, 0, bytes.length);
  }

  /**
   * 只判断指定区间首字节是否可能是 UTF-8 文本的起始字节。
   */
  protected boolean hasUtf8Prefix(byte[] bytes, int offset, int length) {
    if (bytes == null || offset < 0 || length < 0
            || offset > bytes.length - length) {
      return false;
    }
    if (length == 0) {
      return true;
    }
    return isUtf8Start(bytes[offset]);
  }

  /**
   * 判断字节是否可能是 UTF-8 文本的首字节。
   * <p>
   * byte 是有符号的（-128..127）；方法内部按无符号值（0..255）判断：
   * ASCII（{@code 0x00..0x7F}）或合法的多字节起始字节（{@code 0xC2..0xF4}）。
   * 用于保证 bytes embedded 的 magic 不会与 String identity 的 wire 重叠。
   */
  protected boolean isUtf8Start(byte first) {
    int unsigned = first & 0xFF;
    return unsigned <= 0x7F || (unsigned >= 0xC2 && unsigned <= 0xF4);
  }

  /**
   * 判断字符或字节值是否是可打印 ASCII，即 {@code 0x20..0x7E}。
   * <p>
   * 参数使用 {@code int}，以同时接受 {@code char} 和已经通过 {@code & 0xFF}
   * 转成 0..255 的 byte 值，避免 byte 的符号扩展和 char 到 byte 的截断。
   */
  protected boolean isPrintableAscii(int c) {
    return c >= 0x20 && c <= 0x7E;
  }

  /**
   * 解析 {@code @<typePrefix>:} 中的 scalar 类型前缀，不解析也不校验 payload。
   * <p>
   * 类型前缀必须是可打印 ASCII，长度不超过 {@link TypeRegistry#SCALAR_PREFIX_MAX_LENGTH}；
   * 扫描最多覆盖该长度的前缀，超长文本在达到上限时直接返回 null。
   */
  protected String scalarTypePrefix(byte[] bytes) {
    return bytes == null ? null : scalarTypePrefix(bytes, 0, bytes.length);
  }

  /**
   * 解析指定区间中 {@code @<typePrefix>:} 的 scalar 类型前缀。
   */
  protected String scalarTypePrefix(byte[] bytes, int offset, int length) {
    // bytes 为 null 时无法解析出类型名
    // 长度小于 3 时无法同时容纳 '@'、至少一个类型名字符和 ':'
    // 首字节必须是 scalar 前缀 '@'
    if (bytes == null || offset < 0 || length < 3
            || offset > bytes.length - length
            || bytes[offset] != (byte) TypeRegistry.SCALAR_PREFIX) {
      // 返回 null 表示当前 wire 不属于 scalar 文本格式
      return null;
    }
    // 第二个字符同样是 '@' 时，wire 属于 String identity 的 '@@' 转义
    // todo 这里其实是有耦合的，依赖了子类StringIdentityType的规则
    if (bytes[offset + 1] == (byte) TypeRegistry.SCALAR_PREFIX) {
      // 返回 null 表示当前 wire 不属于 scalar 文本格式
      return null;
    }
    // 类型名从 '@' 之后的第一个字节开始
    int start = offset + 1;
    // index 从类型名起点开始，向冒号方向扫描
    int index = start;
    // 类型名最多允许 SCALAR_PREFIX_MAX_LENGTH 个字节，再加一个冒号位置
    int nameLimit = start + TypeRegistry.SCALAR_PREFIX_MAX_LENGTH + 1;
    // 实际扫描边界同时受区间末尾和类型名最大长度限制
    int scanLimit = Math.min(offset + length, nameLimit);
    // 在扫描边界内持续查找冒号
    while (index < scanLimit && bytes[index] != ':') {
      // byte 先与 0xFF 做按位与，得到无符号值（0..255），才能正确判断是否是可打印 ASCII
      int b = bytes[index] & 0xFF;
      // 类型名限定为可打印 ASCII 字符
      if (!isPrintableAscii(b)) {
        // 遇到不可打印字符时返回 null
        return null;
      }
      // 当前字节合法，继续扫描下一个字节
      index++;
    }
    // 类型名为空、扫描到区间末尾或达到最大长度仍未遇到冒号时，无法确定类型名
    if (index == start || index >= offset + length || index >= nameLimit) {
      // 返回 null 表示类型名格式不完整或超过最大长度
      return null;
    }
    // 用 SCALAR_CHARSET（UTF-8）还原类型名，起始位置为 start，有效长度为 index - start
    return new String(bytes, start, index - start, TypeRegistry.SCALAR_CHARSET);
  }

  /**
   * 用严格 UTF-8 解码整段字节；遇到非法字节时抛 {@link CacheCodecException}。
   */
  protected String decodeUtf8(byte[] bytes) {
    return bytes == null ? null : decodeUtf8(bytes, 0, bytes.length);
  }

  /**
   * 严格解码指定区间中的 UTF-8 字节。
   */
  protected String decodeUtf8(byte[] bytes, int offset, int length) {
    try {
      return TypeRegistry.SCALAR_CHARSET.newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .decode(ByteBuffer.wrap(bytes, offset, length))
              .toString();
    } catch (CharacterCodingException e) {
      throw new CacheCodecException(
              "Unsupported cache value bytes: not valid UTF-8.", e);
    }
  }

  /**
   * 把文本编码为 UTF-8 字节；text 为 null 时返回 null。
   */
  protected byte[] encodeUtf8(String text) {
    return text == null ? null : text.getBytes(TypeRegistry.SCALAR_CHARSET);
  }

  /**
   * 转义可读文本中的反斜杠、双引号、换行、回车、制表符和控制字符。
   * <p>
   * 文本不含需要转义的字符时返回原 String。
   */
  protected String escape(String text) {
    int firstEscaped = firstEscapedIndex(text);
    if (firstEscaped < 0) {
      // 数值和日期文本通常直接复用原 String。
      return text;
    }
    StringBuilder builder = new StringBuilder(text.length() + 8);
    builder.append(text, 0, firstEscaped);
    for (int i = firstEscaped; i < text.length(); i++) {
      char c = text.charAt(i);
      switch (c) {
        case BACKSLASH:
          builder.append(BACKSLASH).append(BACKSLASH);
          break;
        case DOUBLE_QUOTE:
          builder.append(BACKSLASH).append(DOUBLE_QUOTE);
          break;
        case '\n':
          builder.append("\\n");
          break;
        case '\r':
          builder.append("\\r");
          break;
        case '\t':
          builder.append("\\t");
          break;
        default:
          if (Character.isISOControl(c)) {
            builder.append(String.format("\\u%04x", (int) c));
          } else {
            builder.append(c);
          }
      }
    }
    return builder.toString();
  }

  /**
   * 返回第一个需要转义的字符下标；文本无需转义时返回 -1。
   */
  private int firstEscapedIndex(String text) {
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == BACKSLASH || c == DOUBLE_QUOTE || c == '\n' || c == '\r' || c == '\t'
              || Character.isISOControl(c)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * 还原 {@link #escape(String)} 产生的转义序列。
   * <p>
   * 入参必须且只能是 {@link #escape(String)} 的返回值。{@code escape} 只会产生
   * {@code \\}、{@code \"}、{@code \n}、{@code \r}、{@code \t} 以及 ISO 控制字符的
   * {@code \}{@code uXXXX}；本方法不保证能正确处理其它来源的文本。
   * <p>
   * 对不符合上述约定的输入，当前实现可能抛 {@link CacheCodecException}，也可能按
   * 宽松规则处理，这些行为不属于本方法的契约。
   */
  protected String unescape(String text) {
    StringBuilder builder = new StringBuilder(text.length());
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c != BACKSLASH) {
        builder.append(c);
        continue;
      }
      if (++i >= text.length()) {
        throw corrupted(text, "trailing escape character");
      }
      char escaped = text.charAt(i);
      switch (escaped) {
        case BACKSLASH:
          builder.append(BACKSLASH);
          break;
        case DOUBLE_QUOTE:
          builder.append(DOUBLE_QUOTE);
          break;
        case 'n':
          builder.append('\n');
          break;
        case 'r':
          builder.append('\r');
          break;
        case 't':
          builder.append('\t');
          break;
        case 'u':
          if (i + 4 >= text.length()) {
            throw corrupted(text, "incomplete unicode escape");
          }
          String hex = text.substring(i + 1, i + 5);
          try {
            builder.append((char) Integer.parseInt(hex, 16));
          } catch (NumberFormatException e) {
            throw corrupted(text, "invalid unicode escape");
          }
          i += 4;
          break;
        default:
          builder.append(escaped);
      }
    }
    return builder.toString();
  }

  /**
   * 构造 wire 文本损坏的异常，消息包含原始文本和损坏原因。
   */
  protected CacheCodecException corrupted(String text, String reason) {
    return new CacheCodecException(Slf4jUtils.format(
            "Corrupted Redis KVCache value [{}]: {}.", text, reason));
  }

}
