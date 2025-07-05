package cn.addenda.component.cache.helper;

import cn.addenda.component.ratelimiter.RateLimiter;
import cn.addenda.component.ratelimiter.allocator.RateLimiterAllocator;
import org.redisson.api.RedissonClient;

import java.util.concurrent.ExecutorService;

/**
 * @author addenda
 * @since 2023/6/3 16:57
 */
public class RedissonRedisCacheHelper extends CacheHelper {

  private final RedissonClient redissonClient;

  public RedissonRedisCacheHelper(RedissonClient redissonClient, ExecutorService cacheBuildEs,
                                  RateLimiterAllocator<?> realQueryRateLimiterAllocator,
                                  RateLimiterAllocator<? extends RateLimiter> ppfCacheExpirationLogRateLimiterAllocator) {
    super(new RedissonRedisKVCache(redissonClient), cacheBuildEs, realQueryRateLimiterAllocator, ppfCacheExpirationLogRateLimiterAllocator);
    this.redissonClient = redissonClient;
  }

  public RedissonRedisCacheHelper(RedissonClient redissonClient) {
    super(new RedissonRedisKVCache(redissonClient));
    this.redissonClient = redissonClient;
  }

}
