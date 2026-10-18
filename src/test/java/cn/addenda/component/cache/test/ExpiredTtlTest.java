package cn.addenda.component.cache.test;

import cn.addenda.component.base.util.SleepUtils;
import cn.addenda.component.cache.ExpiredConcurrentHashMapKVCache;
import cn.addenda.component.cache.ExpiredHashMapKVCache;
import cn.addenda.component.cache.ExpiredKVCache;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * {@link ExpiredKVCache} 的 TTL 换算规则：{@link ExpiredKVCache#ttlMillis} 与
 * 建立在它之上的 {@link ExpiredKVCache#deadline}。
 * <p>
 * 这两个方法的意义不在于算术本身，而在于<b>它是所有实现共用的唯一一份规则</b>。
 * 此前不是：两个 lettuce 实现各自持有一份私有取整逻辑，redisson 与 spring-data-redis
 * 把非正 timeout 原样下发给服务端、换来一句 {@code ERR invalid expire time}，
 * 而两个内存实现走的是 {@code deadline} —— 同一份 {@code timeout = 0} 的配置，
 * 在六个都实现本接口的实现上要么"立即过期"、要么抛异常。
 * <p>
 * 所以下面分两层验证：
 * <ol>
 *   <li>规则本身（纯函数，不依赖任何实现）；</li>
 *   <li><b>内存实现确实经由这条规则</b> —— 端到端地写一个非正 TTL，看它是否立即过期。
 *       redis 实现同样调用 {@code ttlMillis}（见各实现的方法体），只是端到端验证需要真实
 *       服务端，那部分由 {@code ExpiredKVCacheContractTest} 覆盖。</li>
 * </ol>
 */
public class ExpiredTtlTest {

  /**
   * 非正 TTL 一律取整为 1ms —— 不是抛异常，也不是原样下发。
   */
  @Test
  public void testNonPositiveTtlIsRoundedUpToOneMilli() {
    Assert.assertEquals("下限常量", 1L, ExpiredKVCache.MIN_TTL_MILLIS);

    Assert.assertEquals("0 毫秒", 1L, ExpiredKVCache.ttlMillis(0L, TimeUnit.MILLISECONDS));
    Assert.assertEquals("负数", 1L, ExpiredKVCache.ttlMillis(-1L, TimeUnit.MILLISECONDS));
    Assert.assertEquals("负得很大", 1L,
            ExpiredKVCache.ttlMillis(Long.MIN_VALUE, TimeUnit.MILLISECONDS));

    // 亚毫秒的"正数"同样取整为 1ms：toMillis 会先截断成 0
    Assert.assertEquals("纳秒级的小正数", 1L,
            ExpiredKVCache.ttlMillis(999, TimeUnit.MICROSECONDS));
    Assert.assertEquals("0 秒", 1L, ExpiredKVCache.ttlMillis(0L, TimeUnit.SECONDS));
    Assert.assertEquals("负数秒", 1L, ExpiredKVCache.ttlMillis(-5L, TimeUnit.SECONDS));
  }

  /**
   * 正 TTL 按单位换算，不被 1ms 下限改写。
   */
  @Test
  public void testPositiveTtlIsConvertedAsIs() {
    Assert.assertEquals(1L, ExpiredKVCache.ttlMillis(1L, TimeUnit.MILLISECONDS));
    Assert.assertEquals(1500L, ExpiredKVCache.ttlMillis(1500L, TimeUnit.MILLISECONDS));
    Assert.assertEquals(1000L, ExpiredKVCache.ttlMillis(1L, TimeUnit.SECONDS));
    Assert.assertEquals(60_000L, ExpiredKVCache.ttlMillis(1L, TimeUnit.MINUTES));
  }

  /**
   * {@code deadline} 就是"当下 + ttlMillis"，只在溢出时改写为 {@link Long#MAX_VALUE}。
   * <p>
   * 这里用区间断言而不是精确值：两次 {@code currentTimeMillis()} 之间可能跨毫秒。
   */
  @Test
  public void testDeadlineIsNowPlusTtl() {
    long before = System.currentTimeMillis();
    long deadline = ExpiredKVCache.deadline(1L, TimeUnit.SECONDS);
    long after = System.currentTimeMillis();

    Assert.assertTrue("到期时刻应落在 [now+1000, now+1000] 的毫秒窗口里",
            deadline >= before + 1000 && deadline <= after + 1000);
  }

  /**
   * 溢出仍然饱和到 {@link Long#MAX_VALUE}（"永不过期"），这条规则在把 TTL 换算
   * 抽出来之后必须保持不变：直接相加回绕成负数会让条目一写进去就被判过期。
   */
  @Test
  public void testDeadlineSaturatesOnOverflow() {
    Assert.assertEquals(Long.MAX_VALUE,
            ExpiredKVCache.deadline(Long.MAX_VALUE, TimeUnit.MILLISECONDS));
    Assert.assertEquals(Long.MAX_VALUE,
            ExpiredKVCache.deadline(Long.MAX_VALUE, TimeUnit.DAYS));
  }

  /**
   * <b>非正 TTL 在内存实现上表现为"立即过期"</b> —— 与 redis 实现取整 1ms 后的表现一致。
   * <p>
   * 断言 {@code get} 与 {@code containsKey} 都读不到，且 {@code setnx} 不会因为那条
   * "过期条目等同于不存在"的旧值而被挡住。
   */
  @Test
  public void testNonPositiveTtlExpiresImmediatelyOnInMemoryImpls() {
    List<ExpiredKVCache<String, String>> impls = new ArrayList<>();
    impls.add(new ExpiredHashMapKVCache<>());
    impls.add(new ExpiredConcurrentHashMapKVCache<>());

    for (ExpiredKVCache<String, String> cache : impls) {
      String name = cache.getClass().getSimpleName();

      cache.set("zero", "v", 0L, TimeUnit.MILLISECONDS);
      cache.set("negative", "v", -1L, TimeUnit.MILLISECONDS);
      cache.setnx("nx-zero", "v", 0L, TimeUnit.MILLISECONDS);
      // 1ms 的到期时刻，睡 5ms 足够跨过去（同毫秒内读到不算过期，见各实现的 isExpired）
      SleepUtils.sleep(TimeUnit.MILLISECONDS, 5);

      Assert.assertNull(name + ": timeout=0 应已过期", cache.get("zero"));
      Assert.assertFalse(name + ": timeout=0 应查不到", cache.containsKey("zero"));
      Assert.assertNull(name + ": timeout=-1 应已过期", cache.get("negative"));
      Assert.assertNull(name + ": setnx 的 timeout=0 同样立即过期", cache.get("nx-zero"));

      // 过期条目等同于不存在：不该把新写入挡在门外
      Assert.assertTrue(name + ": 过期的旧值不该挡住 setnx",
              cache.setnx("zero", "new", 1, TimeUnit.MINUTES));
      Assert.assertEquals(name + ": 新值要真的写进去", "new", cache.get("zero"));
      cache.delete("zero");
    }
  }

}
