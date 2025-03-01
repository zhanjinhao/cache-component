package cn.addenda.component.cache.helper;

import cn.addenda.component.base.allocator.lock.LockAllocator;
import cn.addenda.component.ratelimiter.allocator.RateLimiterAllocator;
import org.redisson.api.RedissonClient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;

/**
 * @author addenda
 * @since 2023/6/3 16:57
 */
public class RedissonRedisCacheHelper extends CacheHelper {

  private final RedissonClient redissonClient;

  public RedissonRedisCacheHelper(RedissonClient redissonClient, long ppfExpirationDetectionInterval, LockAllocator<?> lockAllocator,
                                  ExecutorService cacheBuildEs, RateLimiterAllocator<?> realQueryRateLimiterAllocator,
                                  boolean useServiceException, String concurrencyGranularity) {
    super(new RedissonRedisKVCache(redissonClient), ppfExpirationDetectionInterval, lockAllocator,
            cacheBuildEs, realQueryRateLimiterAllocator, useServiceException, concurrencyGranularity);
    this.redissonClient = redissonClient;
  }

  public RedissonRedisCacheHelper(RedissonClient redissonClient, long ppfExpirationDetectionInterval, LockAllocator<? extends Lock> lockAllocator) {
    super(new RedissonRedisKVCache(redissonClient), ppfExpirationDetectionInterval, lockAllocator);
    this.redissonClient = redissonClient;
  }

  public RedissonRedisCacheHelper(RedissonClient redissonClient, long ppfExpirationDetectionInterval) {
    super(new RedissonRedisKVCache(redissonClient), ppfExpirationDetectionInterval);
    this.redissonClient = redissonClient;
  }

  public RedissonRedisCacheHelper(RedissonClient redissonClient) {
    super(new RedissonRedisKVCache(redissonClient));
    this.redissonClient = redissonClient;
  }

}
