package cn.addenda.component.cache.codec;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * 一类 wire 编解码器的统一入口。
 * <p>
 * {@link #encodeWire(Object)} 返回可以直接写入 redis 的 wire 区间；
 * {@link #canDecode(byte[], int, int)} 判断字节区间是否属于当前 registry，
 * payload 的合法性由 {@link #decode(byte[], int, int)} 校验。
 *
 * @author addenda
 */
public interface TypeRegistry {

  /**
   * Scalar 文本 wire 的前缀标识符。String 自身使用该字符开头时写成两个该字符。
   */
  char SCALAR_PREFIX = '@';

  /**
   * scalar 文本 wire 的默认字符集；prefix 和 payload 都使用该字符集。
   */
  Charset SCALAR_CHARSET = StandardCharsets.UTF_8;

  /**
   * scalar 文本 wire 中类型名前缀的最大长度。
   * <p>
   * 解码识别类型名时最多扫描该长度，避免对超长文本做全数组扫描。
   * <p>
   * 内置最长前缀为 {@code java.util.concurrent.atomic.AtomicInteger[]}，长度 43；
   * 64 为该长度保留 21 个字符的扩展空间。
   */
  int SCALAR_PREFIX_MAX_LENGTH = 64;

  /**
   * 当前 registry 能够处理该对象时返回 true。
   */
  boolean canEncode(Object o);

  /**
   * 返回可以直接写入 redis 的 wire 区间。
   * <p>
   * 调用方应先通过 {@link #canEncode(Object)} 确认当前 registry 负责该对象。
   */
  WireBytes encodeWire(Object o);

  /**
   * 当前 registry 识别该 wire 区间时返回 true。
   * <p>
   * 该方法只判断 wire 归属，payload 内部合法性由
   * {@link #decode(byte[], int, int)} 校验。
   */
  boolean canDecode(byte[] bytes, int offset, int length);

  /**
   * 按当前 registry 的 wire 规则还原对象。
   */
  Object decode(byte[] bytes, int offset, int length);
}
