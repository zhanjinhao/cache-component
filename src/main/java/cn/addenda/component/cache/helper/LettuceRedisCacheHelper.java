package cn.addenda.component.cache.helper;

import cn.addenda.component.ratelimiter.RateLimiter;
import cn.addenda.component.ratelimiter.allocator.RateLimiterAllocator;
import io.lettuce.core.RedisClient;

import java.util.concurrent.ExecutorService;

/**
 * @author addenda
 * @since 2023/6/3 16:57
 */
public class LettuceRedisCacheHelper extends CacheHelper {

  private final LettuceRedisKVCache lettuceRedisKVCache;

  public LettuceRedisCacheHelper(RedisClient redisClient, ExecutorService cacheBuildEs,
                                 RateLimiterAllocator<?> realQueryRateLimiterAllocator,
                                 RateLimiterAllocator<? extends RateLimiter> ppfCacheExpirationLogRateLimiterAllocator) {
    this(new LettuceRedisKVCache(redisClient), cacheBuildEs, realQueryRateLimiterAllocator, ppfCacheExpirationLogRateLimiterAllocator);
  }

  public LettuceRedisCacheHelper(RedisClient redisClient) {
    this(new LettuceRedisKVCache(redisClient));
  }

  private LettuceRedisCacheHelper(LettuceRedisKVCache lettuceRedisKVCache, ExecutorService cacheBuildEs,
                                  RateLimiterAllocator<?> realQueryRateLimiterAllocator,
                                  RateLimiterAllocator<? extends RateLimiter> ppfCacheExpirationLogRateLimiterAllocator) {
    super(lettuceRedisKVCache, cacheBuildEs, realQueryRateLimiterAllocator, ppfCacheExpirationLogRateLimiterAllocator);
    this.lettuceRedisKVCache = lettuceRedisKVCache;
  }

  private LettuceRedisCacheHelper(LettuceRedisKVCache lettuceRedisKVCache) {
    super(lettuceRedisKVCache);
    this.lettuceRedisKVCache = lettuceRedisKVCache;
  }

  @Override
  public void destroy() throws Exception {
    try {
      super.destroy();
    } finally {
      // 收尾的延迟删除任务依赖缓存连接，所以关闭连接必须放在super.destroy()之后
      lettuceRedisKVCache.close();
    }
  }

}
