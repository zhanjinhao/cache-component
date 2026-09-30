package cn.addenda.component.cache.helper;

import cn.addenda.component.ratelimiter.RateLimiter;
import cn.addenda.component.ratelimiter.allocator.RateLimiterAllocator;
import io.lettuce.core.cluster.RedisClusterClient;

import java.util.concurrent.ExecutorService;

/**
 * 用于 redis cluster 模式的 CacheHelper。
 * <p/>
 * {@link CacheHelper} 只用 get / set / delete 三种单 key 操作，
 * 不存在跨 slot 问题，所以逻辑本身对 cluster 是透明的，
 * 这里只需要换一个支持 cluster 的 {@link LettuceRedisClusterKVCache}。
 *
 * @author addenda
 */
public class LettuceRedisClusterCacheHelper extends CacheHelper {

  private final LettuceRedisClusterKVCache lettuceRedisClusterKVCache;

  public LettuceRedisClusterCacheHelper(RedisClusterClient redisClusterClient, ExecutorService cacheBuildEs,
                                        RateLimiterAllocator<?> realQueryRateLimiterAllocator,
                                        RateLimiterAllocator<? extends RateLimiter> ppfCacheExpirationLogRateLimiterAllocator) {
    this(new LettuceRedisClusterKVCache(redisClusterClient), cacheBuildEs, realQueryRateLimiterAllocator,
            ppfCacheExpirationLogRateLimiterAllocator);
  }

  public LettuceRedisClusterCacheHelper(RedisClusterClient redisClusterClient) {
    this(new LettuceRedisClusterKVCache(redisClusterClient));
  }

  private LettuceRedisClusterCacheHelper(LettuceRedisClusterKVCache lettuceRedisClusterKVCache,
                                         ExecutorService cacheBuildEs,
                                         RateLimiterAllocator<?> realQueryRateLimiterAllocator,
                                         RateLimiterAllocator<? extends RateLimiter> ppfCacheExpirationLogRateLimiterAllocator) {
    super(lettuceRedisClusterKVCache, cacheBuildEs, realQueryRateLimiterAllocator,
            ppfCacheExpirationLogRateLimiterAllocator);
    this.lettuceRedisClusterKVCache = lettuceRedisClusterKVCache;
  }

  private LettuceRedisClusterCacheHelper(LettuceRedisClusterKVCache lettuceRedisClusterKVCache) {
    super(lettuceRedisClusterKVCache);
    this.lettuceRedisClusterKVCache = lettuceRedisClusterKVCache;
  }

  @Override
  public void destroy() throws Exception {
    try {
      super.destroy();
    } finally {
      // 收尾的延迟删除任务依赖缓存连接，所以关闭连接必须放在super.destroy()之后
      lettuceRedisClusterKVCache.close();
    }
  }

}
