package cn.addenda.component.cache.helper;

import cn.addenda.component.base.allocator.lock.LockAllocator;
import cn.addenda.component.ratelimiter.allocator.RateLimiterAllocator;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;

/**
 * @author addenda
 * @since 2023/6/3 16:57
 */
public class StringRedisTemplateRedisCacheHelper extends CacheHelper {

  private final StringRedisTemplate stringRedisTemplate;

  public StringRedisTemplateRedisCacheHelper(StringRedisTemplate stringRedisTemplate, long ppfExpirationDetectionInterval, LockAllocator<?> lockAllocator,
                                             ExecutorService cacheBuildEs, RateLimiterAllocator<?> realQueryRateLimiterAllocator, boolean useServiceException, String concurrencyGranularity) {
    super(new StringRedisTemplateRedisKVCache(stringRedisTemplate), ppfExpirationDetectionInterval, lockAllocator,
            cacheBuildEs, realQueryRateLimiterAllocator, useServiceException, concurrencyGranularity);
    this.stringRedisTemplate = stringRedisTemplate;
  }

  public StringRedisTemplateRedisCacheHelper(StringRedisTemplate stringRedisTemplate, long ppfExpirationDetectionInterval, LockAllocator<? extends Lock> lockAllocator) {
    super(new StringRedisTemplateRedisKVCache(stringRedisTemplate), ppfExpirationDetectionInterval, lockAllocator);
    this.stringRedisTemplate = stringRedisTemplate;
  }

  public StringRedisTemplateRedisCacheHelper(StringRedisTemplate stringRedisTemplate, long ppfExpirationDetectionInterval) {
    super(new StringRedisTemplateRedisKVCache(stringRedisTemplate), ppfExpirationDetectionInterval);
    this.stringRedisTemplate = stringRedisTemplate;
  }

  public StringRedisTemplateRedisCacheHelper(StringRedisTemplate stringRedisTemplate) {
    super(new StringRedisTemplateRedisKVCache(stringRedisTemplate));
    this.stringRedisTemplate = stringRedisTemplate;
  }

}
