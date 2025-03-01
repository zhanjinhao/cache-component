package cn.addenda.component.cache.test.helper;

import cn.addenda.component.cache.helper.RedissonRedisKVCache;
import cn.addenda.component.cache.test.RedissonClientBaseTest;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;

public class TestRedissonRedisKVCache {

  @Test
  @SneakyThrows
  public void test() {
    RedissonClient redissonClient = RedissonClientBaseTest.redissonClient();
    RedissonRedisKVCache redissonRedisKVCache = new RedissonRedisKVCache(redissonClient);

    RBucket<String> bucket = redissonClient.getBucket("asa");
    bucket.set("占");

    String asa = bucket.get();

    Assert.assertEquals("占", asa);
  }

}
