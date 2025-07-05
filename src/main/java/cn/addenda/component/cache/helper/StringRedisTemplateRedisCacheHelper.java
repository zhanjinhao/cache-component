package cn.addenda.component.cache.helper;

import cn.addenda.component.ratelimiter.RateLimiter;
import cn.addenda.component.ratelimiter.allocator.RateLimiterAllocator;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.ExecutorService;

/**
 * @author addenda
 * @since 2023/6/3 16:57
 */
public class StringRedisTemplateRedisCacheHelper extends CacheHelper {

  private final StringRedisTemplate stringRedisTemplate;

  public StringRedisTemplateRedisCacheHelper(StringRedisTemplate stringRedisTemplate, ExecutorService cacheBuildEs,
                                             RateLimiterAllocator<?> realQueryRateLimiterAllocator,
                                             RateLimiterAllocator<? extends RateLimiter> ppfCacheExpirationLogRateLimiterAllocator) {
    super(new StringRedisTemplateRedisKVCache(stringRedisTemplate), cacheBuildEs, realQueryRateLimiterAllocator, ppfCacheExpirationLogRateLimiterAllocator);
    this.stringRedisTemplate = stringRedisTemplate;
  }

  public StringRedisTemplateRedisCacheHelper(StringRedisTemplate stringRedisTemplate) {
    super(new StringRedisTemplateRedisKVCache(stringRedisTemplate));
    this.stringRedisTemplate = stringRedisTemplate;
  }

}
