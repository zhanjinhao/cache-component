package cn.addenda.component.cache.codec;

import java.util.Arrays;

/**
 * wire 字节区间：底层数组 {@code bytes}、起始下标 {@code offset} 和长度 {@code length}。
 * <p>
 * 编码实现可以先写入一个容量大于实际长度的缓冲区，再用本对象返回实际有效区间，
 * 避免为了得到精确长度的 {@code byte[]} 而复制缓冲区。
 * <p>
 * 使用约定：
 * <ul>
 *   <li>{@link #bytes()} 返回底层数组，容量可能大于 {@link #length()}；</li>
 *   <li>调用方只能读取，不得修改底层数组；</li>
 *   <li>需要独立、精确长度的数组时调用 {@link #toByteArray()}。</li>
 * </ul>
 *
 * @author addenda
 */
public final class WireBytes {

  private final byte[] bytes;

  private final int offset;

  private final int length;

  private WireBytes(byte[] bytes, int offset, int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  /**
   * 包装完整数组；{@code bytes} 为 null 时返回 null。
   */
  public static WireBytes of(byte[] bytes) {
    return bytes == null ? null : new WireBytes(bytes, 0, bytes.length);
  }

  /**
   * 包装数组的指定区间；{@code bytes} 为 null 时返回 null。
   * <p>
   * {@code bytes} 非 null 时，{@code offset} 和 {@code length} 必须落在其范围内，
   * 否则抛 {@link IllegalArgumentException}。
   */
  public static WireBytes of(byte[] bytes, int offset, int length) {
    if (bytes == null) {
      return null;
    }
    if (offset < 0 || length < 0 || offset > bytes.length
            || length > bytes.length - offset) {
      throw new IllegalArgumentException("Invalid wire range: offset [" + offset
              + "], length [" + length + "], bytes length [" + bytes.length + "].");
    }
    return new WireBytes(bytes, offset, length);
  }

  /**
   * 返回底层数组；容量可能大于 {@link #length()}，调用方不得修改。
   */
  public byte[] bytes() {
    return bytes;
  }

  /**
   * 返回有效区间的起始下标。
   */
  public int offset() {
    return offset;
  }

  /**
   * 返回有效区间的长度。
   */
  public int length() {
    return length;
  }

  /**
   * 返回精确长度的字节数组。
   * <p>
   * 区间覆盖完整数组时直接返回底层数组，不复制；否则复制有效区间。
   */
  public byte[] toByteArray() {
    if (offset == 0 && length == bytes.length) {
      return bytes;
    }
    return Arrays.copyOfRange(bytes, offset, offset + length);
  }
}
