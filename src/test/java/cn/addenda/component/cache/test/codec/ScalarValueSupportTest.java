package cn.addenda.component.cache.test.codec;

import cn.addenda.component.cache.codec.CacheCodecException;
import cn.addenda.component.cache.codec.ScalarValueSupport;
import cn.addenda.component.cache.codec.TypeRegistry;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;

/**
 * 钉住 {@link ScalarValueSupport} 的 UTF-8 编解码、前缀识别、类型名解析、转义和
 * 标量编码工具。
 *
 * @author addenda
 */
public class ScalarValueSupportTest {

  private final ExposedScalarValueSupport support = new ExposedScalarValueSupport();

  // ------------------------------------------------------------------
  // hasUtf8Prefix
  // ------------------------------------------------------------------

  @Test
  public void testHasUtf8PrefixNullAndEmpty() {
    Assert.assertFalse(support._hasUtf8Prefix((byte[]) null));
    Assert.assertTrue(support._hasUtf8Prefix(new byte[0]));

    Assert.assertFalse(support._hasUtf8Prefix(null, 0, 0));
    Assert.assertTrue(support._hasUtf8Prefix(new byte[0], 0, 0));
  }

  @Test
  public void testHasUtf8PrefixFirstByteBoundaries() {
    assertHasUtf8PrefixRange(0x00, 0x7F, true);
    assertHasUtf8PrefixRange(0x80, 0xC1, false);
    assertHasUtf8PrefixRange(0xC2, 0xF4, true);
    assertHasUtf8PrefixRange(0xF5, 0xFF, false);
  }

  private void assertHasUtf8PrefixRange(int from, int to, boolean expected) {
    for (int value = from; value <= to; value++) {
      boolean actual = support._hasUtf8Prefix(new byte[]{(byte) value});
      String message = "first byte 0x" + Integer.toHexString(value);
      if (expected) {
        Assert.assertTrue(message, actual);
      } else {
        Assert.assertFalse(message, actual);
      }
    }
  }

  @Test
  public void testHasUtf8PrefixRange() {
    byte[] bytes = bytes(0x41, 0xF4, 0x80);

    Assert.assertTrue(support._hasUtf8Prefix(bytes, 0, 3));
    Assert.assertTrue(support._hasUtf8Prefix(bytes, 1, 2));
    Assert.assertFalse(support._hasUtf8Prefix(bytes, 2, 1));

    // 空区间不读取任何字节，始终视为合法前缀
    Assert.assertTrue(support._hasUtf8Prefix(bytes, 0, 0));
    Assert.assertTrue(support._hasUtf8Prefix(bytes, 2, 0));
    Assert.assertTrue(support._hasUtf8Prefix(bytes, 3, 0));
  }

  @Test
  public void testHasUtf8PrefixInvalidRange() {
    byte[] bytes = bytes(0x41, 0x42, 0x43);

    Assert.assertFalse(support._hasUtf8Prefix(bytes, -1, 1));
    Assert.assertFalse(support._hasUtf8Prefix(bytes, 0, -1));
    Assert.assertFalse(support._hasUtf8Prefix(bytes, 4, 0));
    Assert.assertFalse(support._hasUtf8Prefix(bytes, 3, 1));
    Assert.assertFalse(support._hasUtf8Prefix(bytes, 0, Integer.MAX_VALUE));
    Assert.assertFalse(support._hasUtf8Prefix(bytes, Integer.MAX_VALUE, 0));
  }

  // ------------------------------------------------------------------
  // isUtf8Start
  // ------------------------------------------------------------------

  @Test
  public void testIsUtf8StartBoundaries() {
    assertIsUtf8StartRange(0x00, 0x7F, true);
    assertIsUtf8StartRange(0x80, 0xC1, false);
    assertIsUtf8StartRange(0xC2, 0xF4, true);
    assertIsUtf8StartRange(0xF5, 0xFF, false);
  }

  private void assertIsUtf8StartRange(int from, int to, boolean expected) {
    for (int value = from; value <= to; value++) {
      boolean actual = support._isUtf8Start((byte) value);
      String message = "byte 0x" + Integer.toHexString(value);
      if (expected) {
        Assert.assertTrue(message, actual);
      } else {
        Assert.assertFalse(message, actual);
      }
    }
  }

  // ------------------------------------------------------------------
  // isPrintableAscii
  // ------------------------------------------------------------------

  @Test
  public void testIsPrintableAsciiBoundaries() {
    Assert.assertTrue(support._isPrintableAscii(0x20));
    Assert.assertTrue(support._isPrintableAscii(0x21));
    Assert.assertTrue(support._isPrintableAscii(0x7E));

    Assert.assertFalse(support._isPrintableAscii(-1));
    Assert.assertFalse(support._isPrintableAscii(0x00));
    Assert.assertFalse(support._isPrintableAscii(0x1F));
    Assert.assertFalse(support._isPrintableAscii(0x7F));
    Assert.assertFalse(support._isPrintableAscii(0x80));
    Assert.assertFalse(support._isPrintableAscii(0xFF));
    Assert.assertFalse(support._isPrintableAscii(0x100));
    Assert.assertFalse(support._isPrintableAscii(Integer.MIN_VALUE));
    Assert.assertFalse(support._isPrintableAscii(Integer.MAX_VALUE));
  }

  // ------------------------------------------------------------------
  // scalarTypePrefix
  // ------------------------------------------------------------------

  @Test
  public void testScalarTypePrefixNormal() {
    Assert.assertEquals("java.lang.Integer",
            support._scalarTypePrefix(utf8("@java.lang.Integer:1")));
    Assert.assertEquals("a", support._scalarTypePrefix(utf8("@a:")));
    Assert.assertEquals("a", support._scalarTypePrefix(utf8("@a:payload")));
    Assert.assertEquals("a", support._scalarTypePrefix(utf8("@a:b:c")));
    Assert.assertEquals("a b", support._scalarTypePrefix(utf8("@a b:x")));
    Assert.assertEquals(" ", support._scalarTypePrefix(utf8("@ :x")));

    // payload 不参与识别，因此 payload 中的非法 UTF-8、控制字符和冒号都不影响 typeName
    Assert.assertEquals("a", support._scalarTypePrefix(bytes('@', 'a', ':', 0xFF)));
    Assert.assertEquals("a", support._scalarTypePrefix(bytes('@', 'a', ':', 0x00)));
    Assert.assertEquals("a", support._scalarTypePrefix(
            bytes('@', 'a', ':', 0xE4, 0xB8, 0xAD)));
  }

  @Test
  public void testScalarTypePrefixRange() {
    byte[] wire = utf8("xx@a:yy");

    Assert.assertEquals("a", support._scalarTypePrefix(wire, 2, 4));
    Assert.assertEquals("a", support._scalarTypePrefix(wire, 2, 3));
    Assert.assertEquals("a", support._scalarTypePrefix(wire, 2, wire.length - 2));

    // 区间首字节不是 '@'
    Assert.assertNull(support._scalarTypePrefix(wire, 1, 5));
    Assert.assertNull(support._scalarTypePrefix(wire, 3, 4));
    // 长度不足
    Assert.assertNull(support._scalarTypePrefix(wire, 2, 2));
    Assert.assertNull(support._scalarTypePrefix(wire, 6, 1));
  }

  @Test
  public void testScalarTypePrefixMaxLengthBoundary() {
    String maxName = repeat('a', TypeRegistry.SCALAR_PREFIX_MAX_LENGTH);
    String tooLongName = repeat('a', TypeRegistry.SCALAR_PREFIX_MAX_LENGTH + 1);

    Assert.assertEquals(maxName,
            support._scalarTypePrefix(utf8("@" + maxName + ":x")));
    Assert.assertEquals(maxName,
            support._scalarTypePrefix(utf8("@" + maxName + ":")));

    Assert.assertNull(support._scalarTypePrefix(utf8("@" + tooLongName + ":x")));
    Assert.assertNull(support._scalarTypePrefix(utf8("@" + tooLongName + ":")));
    Assert.assertNull(support._scalarTypePrefix(utf8("@" + maxName)));

    // 带 offset 时仍以类型名长度而不是整个区间长度作为上限
    byte[] shifted = utf8("xx@" + maxName + ":v");
    Assert.assertEquals(maxName,
            support._scalarTypePrefix(shifted, 2, shifted.length - 2));
  }

  @Test
  public void testScalarTypePrefixInvalidFormat() {
    Assert.assertNull(support._scalarTypePrefix((byte[]) null));
    Assert.assertNull(support._scalarTypePrefix(new byte[0]));
    Assert.assertNull(support._scalarTypePrefix(utf8("@")));
    Assert.assertNull(support._scalarTypePrefix(utf8("@a")));
    Assert.assertNull(support._scalarTypePrefix(utf8("@ab")));
    Assert.assertNull(support._scalarTypePrefix(utf8("@:x")));
    Assert.assertNull(support._scalarTypePrefix(utf8("@@a:x")));
    Assert.assertNull(support._scalarTypePrefix(utf8("@@")));
    Assert.assertNull(support._scalarTypePrefix(utf8("abc")));

    // 类型名中的不可打印 ASCII 或非 ASCII 字节
    Assert.assertNull(support._scalarTypePrefix(bytes('@', 'a', '\t', ':', 'x')));
    Assert.assertNull(support._scalarTypePrefix(bytes('@', 'a', '\n', ':', 'x')));
    Assert.assertNull(support._scalarTypePrefix(bytes('@', 'a', 0x1F, ':', 'x')));
    Assert.assertNull(support._scalarTypePrefix(bytes('@', 'a', 0x7F, ':', 'x')));
    Assert.assertNull(support._scalarTypePrefix(bytes('@', 'a', 0x80, ':', 'x')));
    Assert.assertNull(support._scalarTypePrefix(bytes('@', 'a', 0xFF, ':', 'x')));
    Assert.assertNull(support._scalarTypePrefix(
            bytes('@', 0xE4, 0xB8, 0xAD, ':', 'x')));
  }

  @Test
  public void testScalarTypePrefixInvalidRange() {
    byte[] wire = utf8("@a:x");

    Assert.assertNull(support._scalarTypePrefix(null, 0, 4));
    Assert.assertNull(support._scalarTypePrefix(wire, -1, 4));
    Assert.assertNull(support._scalarTypePrefix(wire, 0, -1));
    Assert.assertNull(support._scalarTypePrefix(wire, 0, 2));
    Assert.assertNull(support._scalarTypePrefix(wire, 0, 5));
    Assert.assertNull(support._scalarTypePrefix(wire, 4, 0));
    Assert.assertNull(support._scalarTypePrefix(wire, 3, 2));
  }

  // ------------------------------------------------------------------
  // decodeUtf8
  // ------------------------------------------------------------------

  @Test
  public void testDecodeUtf8Normal() {
    Assert.assertNull(support._decodeUtf8((byte[]) null));
    Assert.assertEquals("", support._decodeUtf8(new byte[0]));
    Assert.assertEquals("hello", support._decodeUtf8(utf8("hello")));

    Assert.assertEquals("中", support._decodeUtf8(bytes(0xE4, 0xB8, 0xAD)));
    Assert.assertEquals("\uD83D\uDE00", support._decodeUtf8(bytes(0xF0, 0x9F, 0x98, 0x80)));
    Assert.assertEquals("\u0000", support._decodeUtf8(bytes(0x00)));
    Assert.assertEquals("\u0080", support._decodeUtf8(bytes(0xC2, 0x80)));
    Assert.assertEquals("\uFFFF", support._decodeUtf8(bytes(0xEF, 0xBF, 0xBF)));
    Assert.assertEquals("\uDBFF\uDFFF",
            support._decodeUtf8(bytes(0xF4, 0x8F, 0xBF, 0xBF)));
  }

  @Test
  public void testDecodeUtf8Range() {
    byte[] text = utf8("x中y");

    Assert.assertEquals("x中y", support._decodeUtf8(text, 0, text.length));
    Assert.assertEquals("中", support._decodeUtf8(text, 1, 3));
    Assert.assertEquals("y", support._decodeUtf8(text, 4, 1));
    Assert.assertEquals("", support._decodeUtf8(text, 0, 0));
    Assert.assertEquals("", support._decodeUtf8(text, text.length, 0));

    // 区间之外的非法字节不会被解码
    byte[] withInvalidPrefix = bytes(0xFF, 'a');
    Assert.assertEquals("a", support._decodeUtf8(withInvalidPrefix, 1, 1));
  }

  @Test
  public void testDecodeUtf8InvalidBytes() {
    assertInvalidUtf8(bytes(0x80));
    assertInvalidUtf8(bytes(0xBF));
    assertInvalidUtf8(bytes(0xFF));
    assertInvalidUtf8(bytes(0xC0, 0x80));
    assertInvalidUtf8(bytes(0xC1, 0xBF));
    assertInvalidUtf8(bytes(0xE0, 0x80, 0x80));
    assertInvalidUtf8(bytes(0xE4, 0xB8));
    assertInvalidUtf8(bytes(0xE4, 0x28, 0xA1));
    assertInvalidUtf8(bytes(0xED, 0xA0, 0x80));
    assertInvalidUtf8(bytes(0xF4, 0x90, 0x80, 0x80));
    assertInvalidUtf8(bytes(0xF5, 0x80, 0x80, 0x80));
    assertInvalidUtf8(bytes('a', 0xFF, 'b'));

    byte[] text = bytes('a', 0xFF);
    try {
      support._decodeUtf8(text, 1, 1);
      Assert.fail("区间内的非法 UTF-8 应抛 CacheCodecException");
    } catch (CacheCodecException expected) {
      Assert.assertTrue(expected.getMessage().contains("not valid UTF-8"));
    }
  }

  // ------------------------------------------------------------------
  // encodeUtf8
  // ------------------------------------------------------------------

  @Test
  public void testEncodeUtf8() {
    Assert.assertNull(support._encodeUtf8(null));
    Assert.assertArrayEquals(new byte[0], support._encodeUtf8(""));

    Assert.assertArrayEquals(bytes('a', 'b', 'c'), support._encodeUtf8("abc"));
    Assert.assertArrayEquals(bytes(0xE4, 0xB8, 0xAD),
            support._encodeUtf8("中"));
    Assert.assertArrayEquals(bytes(0xF0, 0x9F, 0x98, 0x80),
            support._encodeUtf8("\uD83D\uDE00"));
    Assert.assertArrayEquals(bytes(0xC2, 0x80), support._encodeUtf8("\u0080"));
    Assert.assertArrayEquals(bytes(0xEF, 0xBF, 0xBF),
            support._encodeUtf8("\uFFFF"));
  }

  @Test
  public void testEncodeDecodeUtf8RoundTrip() {
    String[] values = {
            "", "hello", "中", "\uD83D\uDE00", "\u0000", "\u0080", "\uFFFF",
            "\uDBFF\uDFFF", "line\nbreak", "a\"b\\c"
    };
    for (String value : values) {
      Assert.assertEquals(value,
              support._decodeUtf8(support._encodeUtf8(value)));
    }
  }

  @Test
  public void testEncodeUtf8ReplacesLoneSurrogate() {
    // UTF-8 无法表示孤立的 UTF-16 代理项，编码器默认用 '?' 替换。
    Assert.assertArrayEquals(bytes('?'), support._encodeUtf8("\uD800"));
    Assert.assertArrayEquals(bytes('?'), support._encodeUtf8("\uDC00"));
    Assert.assertArrayEquals(bytes('a', '?', 'b'),
            support._encodeUtf8("a\uD800b"));

    Assert.assertEquals("?", support._decodeUtf8(
            support._encodeUtf8("\uD800")));
  }

  // ------------------------------------------------------------------
  // escape
  // ------------------------------------------------------------------

  @Test
  public void testEscapeReturnsSameInstanceWhenNoEscapeNeeded() {
    String plain = "abc 123 ~ @ , [] 中文 \uD83D\uDE00";
    Assert.assertSame(plain, support._escape(plain));

    String empty = "";
    Assert.assertSame(empty, support._escape(empty));
  }

  @Test
  public void testEscapeSpecialCharacters() {
    Assert.assertEquals("\\\\", support._escape("\\"));
    Assert.assertEquals("\\\"", support._escape("\""));
    Assert.assertEquals("\\n", support._escape("\n"));
    Assert.assertEquals("\\r", support._escape("\r"));
    Assert.assertEquals("\\t", support._escape("\t"));
  }

  @Test
  public void testEscapeIsoControlBoundaries() {
    Assert.assertEquals("\\u0000", support._escape("\u0000"));
    Assert.assertEquals("\\u0001", support._escape("\u0001"));
    Assert.assertEquals("\\u000b", support._escape("\u000b"));
    Assert.assertEquals("\\u001f", support._escape("\u001f"));
    Assert.assertEquals("\\u007f", support._escape("\u007f"));
    Assert.assertEquals("\\u0080", support._escape("\u0080"));
    Assert.assertEquals("\\u009f", support._escape("\u009f"));

    // 边界之外的可打印/非控制字符保持原样
    Assert.assertEquals(" ", support._escape(" "));
    Assert.assertEquals("~", support._escape("~"));
    Assert.assertEquals("\u00a0", support._escape("\u00a0"));
  }

  @Test
  public void testEscapeMixedText() {
    String raw = "a\"b\\c\n\r\t\u0001\u007F中";
    String expected = "a\\\"b\\\\c\\n\\r\\t\\u0001\\u007f中";
    Assert.assertEquals(expected, support._escape(raw));

    Assert.assertEquals("abc\\u0000def", support._escape("abc\u0000def"));
    Assert.assertEquals("\\\"\\n", support._escape("\"\n"));
    Assert.assertEquals("\uD83D\uDE00\\n", support._escape("\uD83D\uDE00\n"));
  }

  @Test(expected = NullPointerException.class)
  public void testEscapeNull() {
    support._escape(null);
  }

  // ------------------------------------------------------------------
  // unescape
  // ------------------------------------------------------------------

  @Test
  public void testUnescapeNormal() {
    Assert.assertEquals("", support._unescape(""));
    Assert.assertEquals("plain", support._unescape("plain"));

    Assert.assertEquals("\\", support._unescape("\\\\"));
    Assert.assertEquals("\"", support._unescape("\\\""));
    Assert.assertEquals("\n", support._unescape("\\n"));
    Assert.assertEquals("\r", support._unescape("\\r"));
    Assert.assertEquals("\t", support._unescape("\\t"));

    // 未知转义按字面字符还原
    Assert.assertEquals("@", support._unescape("\\@"));
    Assert.assertEquals(",", support._unescape("\\,"));
    Assert.assertEquals("x", support._unescape("\\x"));
    Assert.assertEquals("0", support._unescape("\\0"));
    Assert.assertEquals("'", support._unescape("\\'"));
    Assert.assertEquals("xy", support._unescape("\\x\\y"));

    // 两个反斜杠后跟 n 表示字面量 \n，而不是换行
    Assert.assertEquals("\\n", support._unescape("\\\\n"));
  }

  @Test
  public void testUnescapeUnicode() {
    Assert.assertEquals("A", support._unescape("\\u0041"));
    Assert.assertEquals("中", support._unescape("\\u4e2d"));
    Assert.assertEquals("中", support._unescape("\\u4E2D"));
    Assert.assertEquals("\u0000", support._unescape("\\u0000"));
    Assert.assertEquals("\u007f", support._unescape("\\u007f"));
    Assert.assertEquals("\u009f", support._unescape("\\u009f"));
    Assert.assertEquals("\uFFFF", support._unescape("\\uFFFF"));
    Assert.assertEquals("AB", support._unescape("\\u0041\\u0042"));
    Assert.assertEquals("\uD83D\uDE00", support._unescape("\\uD83D\\uDE00"));

    // 正好消费 4 个十六进制字符，后续字符原样保留
    Assert.assertEquals("A1", support._unescape("\\u00411"));

    // 被转义的反斜杠不会再次触发 unicode 解析
    Assert.assertEquals("\\u0041", support._unescape("\\\\u0041"));
  }

  @Test
  public void testUnescapeCorrupted() {
    assertCorrupted(() -> support._unescape("\\"), "trailing escape character");
    assertCorrupted(() -> support._unescape("tail\\"), "trailing escape character");

    assertCorrupted(() -> support._unescape("\\u"), "incomplete unicode escape");
    assertCorrupted(() -> support._unescape("\\u1"), "incomplete unicode escape");
    assertCorrupted(() -> support._unescape("\\u12"), "incomplete unicode escape");
    assertCorrupted(() -> support._unescape("\\u123"), "incomplete unicode escape");

    assertCorrupted(() -> support._unescape("\\u123G"), "invalid unicode escape");
    assertCorrupted(() -> support._unescape("\\uZZZZ"), "invalid unicode escape");
    assertCorrupted(() -> support._unescape("\\u12G4"), "invalid unicode escape");
    assertCorrupted(() -> support._unescape("\\u 123"), "invalid unicode escape");
  }

  @Test(expected = NullPointerException.class)
  public void testUnescapeNull() {
    support._unescape(null);
  }

  @Test
  public void testEscapeUnescapeRoundTripForEveryChar() {
    for (int value = 0; value <= 0xFFFF; value++) {
      String raw = String.valueOf((char) value);
      Assert.assertEquals("U+" + Integer.toHexString(value),
              raw, support._unescape(support._escape(raw)));
    }
  }

  @Test
  public void testEscapeUnescapeRoundTripForMixedText() {
    String[] values = {
            "", "plain", "a\"b\\c", "line\nbreak", "\u0000\u0001\u007f\u009f",
            "中文 \uD83D\uDE00", "\\u0041", "a,b", "@java.lang.Integer:1"
    };
    for (String value : values) {
      Assert.assertEquals(value, support._unescape(support._escape(value)));
    }

    StringBuilder all = new StringBuilder();
    for (int value = 0; value <= 0xFFFF; value++) {
      all.append((char) value);
    }
    Assert.assertEquals(all.toString(),
            support._unescape(support._escape(all.toString())));
  }

  @Test
  public void testEscapeUnescapeRoundTripForEscapeLookalikes() {
    String bs = "\\";
    String quote = "\"";
    String[] values = {
            bs,
            bs + bs,
            bs + "n",
            bs + "r",
            bs + "t",
            bs + "u0041",
            bs + "uD800",
            bs + "u0000",
            bs + "u009f",
            bs + "uD83D" + bs + "uDE00",
            bs + bs + "n",
            bs + bs + bs + "n",
            quote,
            quote + quote,
            bs + quote,
            quote + bs,
            bs + quote + bs,
            "a" + bs + "n" + quote + "b",
            "n" + bs + "t" + bs + "u" + "0"
    };
    for (String value : values) {
      assertEscapeRoundTrip(value);
    }
  }

  @Test
  public void testEscapeUnescapeRoundTripForBackslashRuns() {
    for (int count = 0; count <= 10; count++) {
      StringBuilder run = new StringBuilder(count);
      for (int i = 0; i < count; i++) {
        run.append('\\');
      }
      String prefix = run.toString();
      assertEscapeRoundTrip(prefix);
      assertEscapeRoundTrip(prefix + "n");
      assertEscapeRoundTrip(prefix + "r");
      assertEscapeRoundTrip(prefix + "t");
      assertEscapeRoundTrip(prefix + "u0041");
      assertEscapeRoundTrip(prefix + "\"");
      assertEscapeRoundTrip(prefix + "\u0000");
    }
  }

  @Test
  public void testEscapeUnescapeRoundTripForUnicodeEscapeFollowedByHexDigits() {
    String[] values = {
            "\u0000" + "0123456789abcdefABCDEF",
            "\u0001" + "23456789abcdef",
            "\u000b" + "cdef",
            "\u001f" + "0123456789",
            "\u007f" + "f",
            "\u0080" + "0",
            "\u009f" + "a",
            "\u0000" + "u0041",
            "\u001f" + "\\u0020"
    };
    for (String value : values) {
      assertEscapeRoundTrip(value);
    }
  }

  @Test
  public void testEscapeUnescapeRoundTripForAllIsoControls() {
    StringBuilder all = new StringBuilder();
    for (int code = 0x00; code <= 0x1F; code++) {
      all.append((char) code);
    }
    for (int code = 0x7F; code <= 0x9F; code++) {
      all.append((char) code);
    }
    assertEscapeRoundTrip(all.toString());

    String prefix = "\\\"";
    String suffix = "\"\\";
    for (int code = 0x00; code <= 0x1F; code++) {
      assertControlRoundTrip((char) code, prefix, suffix);
    }
    for (int code = 0x7F; code <= 0x9F; code++) {
      assertControlRoundTrip((char) code, prefix, suffix);
    }
  }

  private void assertControlRoundTrip(char c, String prefix, String suffix) {
    assertEscapeRoundTrip("a" + c + "b");
    assertEscapeRoundTrip(prefix + c + "\\n" + c + suffix);
  }

  @Test
  public void testEscapeUnescapeRoundTripForControlBoundaries() {
    String[] values = {
            "\u001f", " ", "~", "\u007f", "\u0080", "\u009f", "\u00A0",
            "\u0085", "\u2028", "\u2029", "\u200B", "\uFEFF",
            "a\u001fb", " \u007f\u0080\u009f ", "\u00A0\u2028\u2029",
            "\\\u2028\"\u2029\\", "\\\uFEFF\"\u200B\\"
    };
    for (String value : values) {
      assertEscapeRoundTrip(value);
    }

    Assert.assertEquals("\\u001f", support._escape("\u001f"));
    Assert.assertEquals("\\u007f", support._escape("\u007f"));
    Assert.assertEquals("\\u0080", support._escape("\u0080"));
    Assert.assertEquals("\\u0085", support._escape("\u0085"));
    Assert.assertEquals("\\u009f", support._escape("\u009f"));

    Assert.assertSame("\u00A0", support._escape("\u00A0"));
    Assert.assertSame("\u2028", support._escape("\u2028"));
    Assert.assertSame("\u2029", support._escape("\u2029"));
    Assert.assertSame("\u200B", support._escape("\u200B"));
    Assert.assertSame("\uFEFF", support._escape("\uFEFF"));
  }

  @Test
  public void testEscapeUnescapeRoundTripForSupplementaryAndNoncharacterCodePoints() {
    String[] values = {
            "\uD800\uDC00", // U+10000
            "\uD83D\uDE00", // U+1F600
            "\uDBFF\uDFFF", // U+10FFFF
            "\uFDD0",
            "\uFDEF",
            "\uFFFE",
            "\uFFFF",
            "\uD83F\uDFFE", // U+1FFFE
            "\uD83F\uDFFF", // U+1FFFF
            "\uDBFF\uDFFE", // U+10FFFE
            "a\uD83D\uDE00b",
            "e\u0301",
            "\uFEFF",
            "\u200B",
            "\\\uFFFE\"\uFFFF\\\u0000\uD83F\uDFFE"
    };
    for (String value : values) {
      assertEscapeRoundTrip(value);
      Assert.assertEquals(value, support._decodeUtf8(support._encodeUtf8(value)));
    }
  }

  @Test
  public void testEscapeUnescapeRoundTripForLoneSurrogates() {
    String high = "\uD800";
    String low = "\uDC00";
    String[] values = {
            high,
            low,
            "a" + high + "b",
            "a" + low + "b",
            high + high,
            low + low,
            low + high,
            high + "A",
            "A" + low,
            "a" + high + low + "b",
            "\uD83D\uDE00" + high,
            low + "\uD83D\uDE00"
    };
    for (String value : values) {
      assertEscapeRoundTrip(value);
      Assert.assertSame(value, support._escape(value));
    }
  }

  @Test
  public void testLoneSurrogateIsLossyAcrossUtf8() {
    String high = "a\uD800b";
    String low = "a\uDC00b";

    Assert.assertEquals("a?b", support._decodeUtf8(support._encodeUtf8(high)));
    Assert.assertEquals("a?b", support._decodeUtf8(support._encodeUtf8(low)));
    Assert.assertNotEquals(high, support._decodeUtf8(support._encodeUtf8(high)));
    Assert.assertNotEquals(low, support._decodeUtf8(support._encodeUtf8(low)));

    String pair = "a\uD83D\uDE00b";
    Assert.assertEquals(pair, support._decodeUtf8(support._encodeUtf8(pair)));
  }

  @Test
  public void testEscapeUnescapeRoundTripForByteLikeCharacters() {
    StringBuilder builder = new StringBuilder(0x100);
    for (int value = 0; value <= 0xFF; value++) {
      builder.append((char) value);
    }
    String raw = builder.toString();

    assertEscapeRoundTrip(raw);
    Assert.assertEquals(raw, support._decodeUtf8(support._encodeUtf8(raw)));
  }

  @Test
  public void testEscapeUnescapeRoundTripForSpecialCharacterPairs() {
    char[] alphabet = {
            '\\', '"', 'n', 'r', 't', 'u', '0', '9', 'a', 'f', 'A', 'F',
            '\u0000', '\u001f', '\u007f', '\u0080', '\u009f', '\u00A0',
            '中', '\uD800', '\uDC00', '\uD83D', '\uDE00'
    };
    for (char first : alphabet) {
      for (char second : alphabet) {
        assertEscapeRoundTrip(new String(new char[]{first, second}));
      }
    }

    char[] middle = {
            '\\', '"', 'u', '0', '\u0000', '\u007f', '中', '\uD800', '\uDE00'
    };
    for (char first : alphabet) {
      for (char mid : middle) {
        for (char second : alphabet) {
          assertEscapeRoundTrip(new String(new char[]{first, mid, second}));
        }
      }
    }
  }

  @Test
  public void testEscapeOutputUsesOnlyKnownEscapeSequences() {
    StringBuilder raw = new StringBuilder();
    raw.append("a\\\"\n\r\t\u0000\u001f\u007f\u0080\u009f中");
    raw.append("\uD83D\uDE00\uD83F\uDFFE\uD800");
    for (int value = 0; value <= 0xFF; value++) {
      raw.append((char) value);
    }

    String escaped = support._escape(raw.toString());
    for (int i = 0; i < escaped.length(); i++) {
      char c = escaped.charAt(i);
      Assert.assertFalse("raw control char U+" + Integer.toHexString(c),
              Character.isISOControl(c));
      Assert.assertNotEquals("raw quote", '"', c);
      if (c != '\\') {
        continue;
      }

      Assert.assertTrue("trailing backslash", i + 1 < escaped.length());
      char next = escaped.charAt(++i);
      if (next == 'u') {
        for (int j = 0; j < 4; j++) {
          Assert.assertTrue("non-lowercase hex digit",
                  isLowerHex(escaped.charAt(++i)));
        }
      } else {
        Assert.assertTrue("unknown escape: \\" + next,
                next == '\\' || next == '"' || next == 'n'
                        || next == 'r' || next == 't');
      }
    }
  }

  @Test
  public void testEscapeUnescapeRoundTripForLongMixedText() {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      builder.append("\\\"\n\r\t\u0000\u001f\u007f\u0080\u009f中\uD83D\uDE00");
    }
    assertEscapeRoundTrip(builder.toString());
  }

  // ------------------------------------------------------------------
  // corrupted
  // ------------------------------------------------------------------

  @Test
  public void testCorruptedMessage() {
    CacheCodecException exception = support._corrupted("wire", "reason");
    Assert.assertEquals("Corrupted Redis KVCache value [wire]: reason.",
            exception.getMessage());
    Assert.assertNull(exception.getCause());
  }

  @Test
  public void testCorruptedNullParts() {
    Assert.assertEquals("Corrupted Redis KVCache value [null]: null.",
            support._corrupted(null, null).getMessage());
  }

  @Test
  public void testCorruptedDoesNotFormatPlaceholdersInArguments() {
    Assert.assertEquals("Corrupted Redis KVCache value [{}]: {}.",
            support._corrupted("{}", "{}").getMessage());
  }

  // ------------------------------------------------------------------
  // helpers
  // ------------------------------------------------------------------

  private void assertInvalidUtf8(byte[] bytes) {
    try {
      support._decodeUtf8(bytes);
      Assert.fail("非法 UTF-8 应抛 CacheCodecException");
    } catch (CacheCodecException expected) {
      Assert.assertTrue(expected.getMessage().contains("not valid UTF-8"));
      Assert.assertTrue(expected.getCause() instanceof CharacterCodingException);
    }
  }

  private void assertCorrupted(Runnable invocation, String message) {
    try {
      invocation.run();
      Assert.fail("应抛 CacheCodecException");
    } catch (CacheCodecException expected) {
      Assert.assertTrue("实际消息：" + expected.getMessage(),
              expected.getMessage().contains(message));
    }
  }

  private void assertEscapeRoundTrip(String value) {
    Assert.assertEquals(value, support._unescape(support._escape(value)));
  }

  private static boolean isLowerHex(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  }

  private static byte[] utf8(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] bytes(int... values) {
    byte[] result = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      result[i] = (byte) values[i];
    }
    return result;
  }

  private static String repeat(char c, int count) {
    StringBuilder builder = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      builder.append(c);
    }
    return builder.toString();
  }
}
