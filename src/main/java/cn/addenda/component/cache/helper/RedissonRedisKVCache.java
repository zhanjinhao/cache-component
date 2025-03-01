package cn.addenda.component.cache.helper;

import cn.addenda.component.cache.ExpiredKVCache;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;

import java.util.concurrent.TimeUnit;

/**
 * @author addenda
 * @since 2023/6/3 16:25
 */
public class RedissonRedisKVCache implements ExpiredKVCache<String, String> {

  private final RedissonClient redissonClient;

  public RedissonRedisKVCache(RedissonClient redissonClient) {
    this.redissonClient = redissonClient;
  }

  @Override
  public void set(String key, String value) {
    RBucket<Object> bucket = redissonClient.getBucket(key);
    bucket.set(value);
  }

  @Override
  public void set(String key, String value, long timeout, TimeUnit unit) {
    RBucket<Object> bucket = redissonClient.getBucket(key);
    bucket.set(value, timeout, unit);
  }

  @Override
  public boolean containsKey(String key) {
    RBucket<Object> bucket = redissonClient.getBucket(key);
    return bucket.get() != null;
  }

  @Override
  public String get(String key) {
    RBucket<String> bucket = redissonClient.getBucket(key);
    return bucket.get();
  }

  @Override
  public boolean delete(String key) {
    RBucket<String> bucket = redissonClient.getBucket(key);
    return bucket.delete();
  }

  @Override
  public long size() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long capacity() {
    return Long.MAX_VALUE;
  }

}
