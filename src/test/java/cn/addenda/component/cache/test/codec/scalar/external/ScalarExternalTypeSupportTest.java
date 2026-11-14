package cn.addenda.component.cache.test.codec.scalar.external;

import cn.addenda.component.cache.codec.CacheCodecException;
import cn.addenda.component.cache.codec.scalar.external.ScalarExternalTypeSupport;

import org.junit.Assert;
import org.junit.Test;

/**
 * 钉住 {@link ScalarExternalTypeSupport} 的文本解析工具。
 *
 * @author addenda
 */
public class ScalarExternalTypeSupportTest {

  private final ExposedScalarExternalTypeSupport support =
          new ExposedScalarExternalTypeSupport();

  // ------------------------------------------------------------------
  // parseBoolean
  // ------------------------------------------------------------------

  @Test
  public void testParseBoolean() {
    Assert.assertTrue(support._parseBoolean("true"));
    Assert.assertFalse(support._parseBoolean("false"));
  }

  @Test
  public void testParseBooleanInvalidInput() {
    String[] invalid = {"TRUE", "True", " true", "true ", "1", "0", "", "yes"};
    for (String text : invalid) {
      assertCorrupted(() -> support._parseBoolean(text), "invalid boolean");
    }
    assertCorrupted(() -> support._parseBoolean(null), "invalid boolean");
  }

  // ------------------------------------------------------------------
  // parseByte
  // ------------------------------------------------------------------

  @Test
  public void testParseByte() {
    Assert.assertEquals((byte) 0, support._parseByte("0"));
    Assert.assertEquals((byte) 9, support._parseByte("9"));
    Assert.assertEquals((byte) -9, support._parseByte("-9"));
    Assert.assertEquals((byte) 9, support._parseByte("+9"));
    Assert.assertEquals(Byte.MIN_VALUE, support._parseByte("-128"));
    Assert.assertEquals(Byte.MAX_VALUE, support._parseByte("127"));
  }

  @Test
  public void testParseByteInvalidInput() {
    String[] invalid = {
            "128", "-129", "1.0", "0x10", " 1", "1 ", "", "x"
    };
    for (String text : invalid) {
      assertCorrupted(() -> support._parseByte(text), "invalid byte");
    }
    assertCorrupted(() -> support._parseByte(null), "invalid byte");
  }

  // ------------------------------------------------------------------
  // parseShort
  // ------------------------------------------------------------------

  @Test
  public void testParseShort() {
    Assert.assertEquals((short) 0, support._parseShort("0"));
    Assert.assertEquals((short) 9, support._parseShort("9"));
    Assert.assertEquals((short) -9, support._parseShort("-9"));
    Assert.assertEquals((short) 9, support._parseShort("+9"));
    Assert.assertEquals(Short.MIN_VALUE, support._parseShort("-32768"));
    Assert.assertEquals(Short.MAX_VALUE, support._parseShort("32767"));
  }

  @Test
  public void testParseShortInvalidInput() {
    String[] invalid = {
            "32768", "-32769", "1.0", "0x10", " 1", "1 ", "", "x"
    };
    for (String text : invalid) {
      assertCorrupted(() -> support._parseShort(text), "invalid short");
    }
    assertCorrupted(() -> support._parseShort(null), "invalid short");
  }

  // ------------------------------------------------------------------
  // parseInt
  // ------------------------------------------------------------------

  @Test
  public void testParseInt() {
    Assert.assertEquals(0, support._parseInt("0"));
    Assert.assertEquals(9, support._parseInt("9"));
    Assert.assertEquals(-9, support._parseInt("-9"));
    Assert.assertEquals(9, support._parseInt("+9"));
    Assert.assertEquals(Integer.MIN_VALUE,
            support._parseInt("-2147483648"));
    Assert.assertEquals(Integer.MAX_VALUE,
            support._parseInt("2147483647"));
  }

  @Test
  public void testParseIntInvalidInput() {
    String[] invalid = {
            "2147483648", "-2147483649", "1.0", "0x10", " 1", "1 ", "", "x"
    };
    for (String text : invalid) {
      assertCorrupted(() -> support._parseInt(text), "invalid int");
    }
    assertCorrupted(() -> support._parseInt(null), "invalid int");
  }

  // ------------------------------------------------------------------
  // parseLong
  // ------------------------------------------------------------------

  @Test
  public void testParseLong() {
    Assert.assertEquals(0L, support._parseLong("0"));
    Assert.assertEquals(9L, support._parseLong("9"));
    Assert.assertEquals(-9L, support._parseLong("-9"));
    Assert.assertEquals(9L, support._parseLong("+9"));
    Assert.assertEquals(Long.MIN_VALUE,
            support._parseLong("-9223372036854775808"));
    Assert.assertEquals(Long.MAX_VALUE,
            support._parseLong("9223372036854775807"));
  }

  @Test
  public void testParseLongInvalidInput() {
    String[] invalid = {
            "9223372036854775808", "-9223372036854775809",
            "1.0", "0x10", " 1", "1 ", "", "x",
            // Java 源码里的 long 字面量后缀不是 Long.parseLong 的语法
            "9L", "9l", "-9L", "+9L"
    };
    for (String text : invalid) {
      assertCorrupted(() -> support._parseLong(text), "invalid long");
    }
    assertCorrupted(() -> support._parseLong(null), "invalid long");
  }

  // ------------------------------------------------------------------
  // parseFloat
  // ------------------------------------------------------------------

  @Test
  public void testParseFloat() {
    Assert.assertEquals(0.0f, support._parseFloat("0"), 0);
    Assert.assertEquals(1.5f, support._parseFloat("1.5"), 0);
    Assert.assertEquals(-1.5f, support._parseFloat("-1.5"), 0);
    Assert.assertEquals(1.5f, support._parseFloat("+1.5"), 0);
    Assert.assertEquals(1.5f, support._parseFloat("1.5f"), 0);
    Assert.assertEquals(1.5f, support._parseFloat("1.5F"), 0);
    Assert.assertEquals(1500.0f, support._parseFloat("1.5e3"), 0);
    Assert.assertEquals(1.5f, support._parseFloat(" 1.5 "), 0);
    Assert.assertEquals(Float.MAX_VALUE,
            support._parseFloat(String.valueOf(Float.MAX_VALUE)), 0);
    Assert.assertEquals(Float.MIN_VALUE,
            support._parseFloat(String.valueOf(Float.MIN_VALUE)), 0);
  }

  @Test
  public void testParseFloatSpecialValues() {
    Assert.assertTrue(Float.isNaN(support._parseFloat("NaN")));
    Assert.assertTrue(Float.isInfinite(support._parseFloat("Infinity")));
    Assert.assertTrue(support._parseFloat("-Infinity") < 0);
    Assert.assertTrue(Float.isInfinite(support._parseFloat("1e50")));
    Assert.assertEquals(0.0f, support._parseFloat("1e-50"), 0);
    Assert.assertEquals(Float.floatToIntBits(-0.0f),
            Float.floatToIntBits(support._parseFloat("-0")));
  }

  @Test
  public void testParseFloatInvalidInput() {
    String[] invalid = {
            "1.2.3", "NaNx", "Infinityx", "  ", "", "x"
    };
    for (String text : invalid) {
      assertCorrupted(() -> support._parseFloat(text), "invalid float");
    }
    assertCorrupted(() -> support._parseFloat(null), "invalid float");
  }

  // ------------------------------------------------------------------
  // parseDouble
  // ------------------------------------------------------------------

  @Test
  public void testParseDouble() {
    Assert.assertEquals(0.0d, support._parseDouble("0"), 0);
    Assert.assertEquals(1.5d, support._parseDouble("1.5"), 0);
    Assert.assertEquals(-1.5d, support._parseDouble("-1.5"), 0);
    Assert.assertEquals(1.5d, support._parseDouble("+1.5"), 0);
    Assert.assertEquals(1.5d, support._parseDouble("1.5d"), 0);
    Assert.assertEquals(1.5d, support._parseDouble("1.5D"), 0);
    Assert.assertEquals(1500.0d, support._parseDouble("1.5e3"), 0);
    Assert.assertEquals(1.5d, support._parseDouble(" 1.5 "), 0);
    Assert.assertEquals(Double.MAX_VALUE,
            support._parseDouble(String.valueOf(Double.MAX_VALUE)), 0);
    Assert.assertEquals(Double.MIN_VALUE,
            support._parseDouble(String.valueOf(Double.MIN_VALUE)), 0);
  }

  @Test
  public void testParseDoubleSpecialValues() {
    Assert.assertTrue(Double.isNaN(support._parseDouble("NaN")));
    Assert.assertTrue(Double.isInfinite(support._parseDouble("Infinity")));
    Assert.assertTrue(support._parseDouble("-Infinity") < 0);
    Assert.assertTrue(Double.isInfinite(support._parseDouble("1e400")));
    Assert.assertEquals(0.0d, support._parseDouble("1e-400"), 0);
    Assert.assertEquals(Double.doubleToLongBits(-0.0d),
            Double.doubleToLongBits(support._parseDouble("-0")));
  }

  @Test
  public void testParseDoubleInvalidInput() {
    String[] invalid = {
            "1.2.3", "NaNx", "Infinityx", "  ", "", "x"
    };
    for (String text : invalid) {
      assertCorrupted(() -> support._parseDouble(text), "invalid double");
    }
    assertCorrupted(() -> support._parseDouble(null), "invalid double");
  }

  // ------------------------------------------------------------------
  // parseCharacter
  // ------------------------------------------------------------------

  @Test
  public void testParseCharacter() {
    Assert.assertEquals(Character.valueOf('a'), support._parseCharacter("a"));
    Assert.assertEquals(Character.valueOf(' '), support._parseCharacter(" "));
    Assert.assertEquals(Character.valueOf('中'), support._parseCharacter("中"));

    Assert.assertEquals(Character.valueOf('\n'),
            support._parseCharacter("\\n"));
    Assert.assertEquals(Character.valueOf('\r'),
            support._parseCharacter("\\r"));
    Assert.assertEquals(Character.valueOf('\t'),
            support._parseCharacter("\\t"));
    Assert.assertEquals(Character.valueOf('\\'),
            support._parseCharacter("\\\\"));
    Assert.assertEquals(Character.valueOf('"'),
            support._parseCharacter("\\\""));
    Assert.assertEquals(Character.valueOf('A'),
            support._parseCharacter("\\u0041"));
    Assert.assertEquals(Character.valueOf('中'),
            support._parseCharacter("\\u4e2d"));
    Assert.assertEquals(Character.valueOf('\u0000'),
            support._parseCharacter("\\u0000"));

    // 未知转义按字面字符还原
    Assert.assertEquals(Character.valueOf('x'),
            support._parseCharacter("\\x"));
  }

  @Test
  public void testParseCharacterBoundaries() {
    Assert.assertEquals(Character.valueOf('\u0000'), support._parseCharacter("\u0000"));
    Assert.assertEquals(Character.valueOf('\u007f'), support._parseCharacter("\u007f"));
    Assert.assertEquals(Character.valueOf('\u0080'), support._parseCharacter("\u0080"));
    Assert.assertEquals(Character.valueOf('\uFFFF'), support._parseCharacter("\uFFFF"));
    // 单个 UTF-16 code unit 的代理项也是合法 char
    Assert.assertEquals(Character.valueOf('\uD800'), support._parseCharacter("\uD800"));
  }

  @Test
  public void testParseCharacterWrongLength() {
    String[] invalid = {
            "", "ab", "😀", "\\uD83D\\uDE00", "\\u0041\\u0042"
    };
    for (String text : invalid) {
      assertCorrupted(() -> support._parseCharacter(text),
              "character must contain exactly one UTF-16 code unit");
    }
    assertCorrupted(() -> support._parseCharacter(null),
            "character must contain exactly one UTF-16 code unit");
  }

  @Test
  public void testParseCharacterPropagatesUnescapeErrors() {
    assertCorrupted(() -> support._parseCharacter("\\"),
            "trailing escape character");
    assertCorrupted(() -> support._parseCharacter("tail\\"),
            "trailing escape character");
    assertCorrupted(() -> support._parseCharacter("\\u"),
            "incomplete unicode escape");
    assertCorrupted(() -> support._parseCharacter("\\u123"),
            "incomplete unicode escape");
    assertCorrupted(() -> support._parseCharacter("\\uZZZZ"),
            "invalid unicode escape");
    assertCorrupted(() -> support._parseCharacter("\\u12G4"),
            "invalid unicode escape");
  }

  // ------------------------------------------------------------------
  // exception message
  // ------------------------------------------------------------------

  @Test
  public void testCorruptedMessageContainsTextAndReason() {
    CacheCodecException exception = assertCorrupted(
            () -> support._parseByte("x"), "invalid byte");
    Assert.assertEquals("Corrupted Redis KVCache value [x]: invalid byte.",
            exception.getMessage());

    CacheCodecException nullException = assertCorrupted(
            () -> support._parseBoolean(null), "invalid boolean");
    Assert.assertEquals("Corrupted Redis KVCache value [null]: invalid boolean.",
            nullException.getMessage());
  }

  private CacheCodecException assertCorrupted(Runnable invocation, String message) {
    try {
      invocation.run();
      Assert.fail("应抛 CacheCodecException");
      return null;
    } catch (CacheCodecException expected) {
      Assert.assertTrue("实际消息：" + expected.getMessage(),
              expected.getMessage().contains(message));
      return expected;
    }
  }
}
