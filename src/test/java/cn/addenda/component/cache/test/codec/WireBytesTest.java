package cn.addenda.component.cache.test.codec;

import cn.addenda.component.cache.codec.*;
import cn.addenda.component.cache.codec.bytes.embedded.*;
import cn.addenda.component.cache.codec.bytes.external.*;
import cn.addenda.component.cache.codec.scalar.embedded.*;
import cn.addenda.component.cache.codec.scalar.external.*;

import org.junit.Assert;
import org.junit.Test;

/**
 * 钉住 {@link WireBytes} 的区间访问、null 语义和按需复制。
 *
 * @author addenda
 */
public class WireBytesTest {

  @Test
  public void testWholeArrayDoesNotCopy() {
    byte[] bytes = {1, 2, 3};

    WireBytes wire = WireBytes.of(bytes);

    Assert.assertSame(bytes, wire.bytes());
    Assert.assertEquals(0, wire.offset());
    Assert.assertEquals(bytes.length, wire.length());
    Assert.assertSame(bytes, wire.toByteArray());
  }

  @Test
  public void testSubRangeCopiesOnDemand() {
    byte[] bytes = {1, 2, 3, 4};

    WireBytes wire = WireBytes.of(bytes, 1, 2);

    Assert.assertSame(bytes, wire.bytes());
    Assert.assertEquals(1, wire.offset());
    Assert.assertEquals(2, wire.length());
    byte[] copied = wire.toByteArray();
    Assert.assertNotSame(bytes, copied);
    Assert.assertArrayEquals(new byte[]{2, 3}, copied);
  }

  @Test
  public void testEmptyRangeAtEnd() {
    byte[] bytes = {1, 2, 3};

    WireBytes wire = WireBytes.of(bytes, bytes.length, 0);

    Assert.assertEquals(3, wire.offset());
    Assert.assertEquals(0, wire.length());
    Assert.assertEquals(0, wire.toByteArray().length);
  }

  @Test
  public void testEmptyArrayDoesNotCopy() {
    byte[] bytes = new byte[0];

    WireBytes wire = WireBytes.of(bytes);

    Assert.assertNotNull(wire);
    Assert.assertSame(bytes, wire.bytes());
    Assert.assertEquals(0, wire.offset());
    Assert.assertEquals(0, wire.length());
    Assert.assertSame(bytes, wire.toByteArray());
  }

  @Test
  public void testExplicitWholeRangeDoesNotCopy() {
    byte[] bytes = {1, 2, 3};

    WireBytes wire = WireBytes.of(bytes, 0, bytes.length);

    Assert.assertSame(bytes, wire.bytes());
    Assert.assertEquals(0, wire.offset());
    Assert.assertEquals(bytes.length, wire.length());
    Assert.assertSame(bytes, wire.toByteArray());
  }

  @Test
  public void testEmptyRangeOnEmptyArrayDoesNotCopy() {
    byte[] bytes = new byte[0];

    WireBytes wire = WireBytes.of(bytes, 0, 0);

    Assert.assertSame(bytes, wire.bytes());
    Assert.assertSame(bytes, wire.toByteArray());
  }

  @Test
  public void testRangeStartingAtZeroStillCopiesWhenShorter() {
    byte[] bytes = {1, 2, 3};

    WireBytes wire = WireBytes.of(bytes, 0, 2);
    byte[] copy = wire.toByteArray();

    Assert.assertSame(bytes, wire.bytes());
    Assert.assertNotSame(bytes, copy);
    Assert.assertArrayEquals(new byte[]{1, 2}, copy);
  }

  @Test
  public void testRangeToEndCopies() {
    byte[] bytes = {1, 2, 3, 4};

    WireBytes wire = WireBytes.of(bytes, 2, 2);
    byte[] copy = wire.toByteArray();

    Assert.assertSame(bytes, wire.bytes());
    Assert.assertNotSame(bytes, copy);
    Assert.assertArrayEquals(new byte[]{3, 4}, copy);
  }

  @Test
  public void testLastByteRange() {
    byte[] bytes = {1, 2, 3};

    WireBytes wire = WireBytes.of(bytes, 2, 1);

    Assert.assertEquals(2, wire.offset());
    Assert.assertEquals(1, wire.length());
    Assert.assertArrayEquals(new byte[]{3}, wire.toByteArray());
  }

  @Test
  public void testEmptyRangeOnNonEmptyArrayCopies() {
    byte[] bytes = {1, 2, 3};

    WireBytes wire = WireBytes.of(bytes, 0, 0);
    byte[] first = wire.toByteArray();
    byte[] second = wire.toByteArray();

    Assert.assertSame(bytes, wire.bytes());
    Assert.assertEquals(0, first.length);
    Assert.assertNotSame(bytes, first);
    Assert.assertNotSame(first, second);
  }

  @Test
  public void testSubRangeCopiesProduceIndependentArrays() {
    byte[] bytes = {1, 2, 3, 4};

    WireBytes wire = WireBytes.of(bytes, 1, 2);
    byte[] first = wire.toByteArray();
    byte[] second = wire.toByteArray();

    Assert.assertNotSame(first, second);
    Assert.assertArrayEquals(first, second);

    first[0] = 99;
    Assert.assertEquals(2, second[0]);
    Assert.assertEquals(2, bytes[1]);
  }

  @Test
  public void testToByteArrayPreservesUnsignedByteValues() {
    byte[] bytes = {(byte) 0x80, (byte) 0xFF, 0x00, 0x7F};

    WireBytes wire = WireBytes.of(bytes, 1, 2);

    Assert.assertArrayEquals(new byte[]{(byte) 0xFF, 0x00}, wire.toByteArray());
  }

  @Test
  public void testBytesExposesUnderlyingArrayWithoutCopy() {
    byte[] bytes = {1, 2, 3};

    WireBytes wire = WireBytes.of(bytes, 1, 1);

    Assert.assertSame(bytes, wire.bytes());

    bytes[1] = 42;
    Assert.assertEquals(42, wire.bytes()[1]);
    Assert.assertArrayEquals(new byte[]{42}, wire.toByteArray());
  }

  @Test
  public void testNullArray() {
    Assert.assertNull(WireBytes.of(null));
  }

  @Test
  public void testNullArrayWithRangeReturnsNull() {
    Assert.assertNull(WireBytes.of(null, 0, 0));
    Assert.assertNull(WireBytes.of(null, 1, 2));
  }

  @Test
  public void testInvalidRangeRejected() {
    byte[] bytes = {1, 2, 3};

    assertInvalid(bytes, -1, 0);
    assertInvalid(bytes, 0, -1);
    assertInvalid(bytes, 4, 0);
    assertInvalid(bytes, 3, 1);
    assertInvalid(bytes, 2, 2);
    assertInvalid(bytes, 0, 4);
  }

  @Test
  public void testEmptyArrayRangeRejected() {
    byte[] bytes = new byte[0];

    assertInvalid(bytes, 1, 0);
    assertInvalid(bytes, 0, 1);
  }

  @Test
  public void testExtremeRangeValuesRejected() {
    byte[] bytes = {1, 2, 3};

    assertInvalid(bytes, Integer.MIN_VALUE, 0);
    assertInvalid(bytes, -1, Integer.MAX_VALUE);
    assertInvalid(bytes, 0, Integer.MAX_VALUE);
    assertInvalid(bytes, Integer.MAX_VALUE, 0);
    assertInvalid(bytes, Integer.MAX_VALUE, Integer.MAX_VALUE);
    assertInvalid(bytes, 3, Integer.MIN_VALUE);
  }

  @Test
  public void testInvalidRangeMessageContainsArguments() {
    byte[] bytes = {1, 2, 3};

    try {
      WireBytes.of(bytes, 2, 2);
      Assert.fail("非法区间应抛 IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      String message = expected.getMessage();
      Assert.assertTrue(message.contains("offset [2]"));
      Assert.assertTrue(message.contains("length [2]"));
      Assert.assertTrue(message.contains("bytes length [3]"));
    }
  }

  private static void assertInvalid(byte[] bytes, int offset, int length) {
    try {
      WireBytes.of(bytes, offset, length);
      Assert.fail("非法 offset/length 应抛 IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // ok
    }
  }
}
