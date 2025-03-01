package cn.addenda.component.cache.helper;

import cn.addenda.component.cache.ExpiredKVCache;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

/**
 * @author addenda
 * @since 2023/6/3 16:25
 */
public class StringRedisTemplateRedisKVCache implements ExpiredKVCache<String, String> {

  private final StringRedisTemplate stringRedisTemplate;

  public StringRedisTemplateRedisKVCache(StringRedisTemplate stringRedisTemplate) {
    this.stringRedisTemplate = stringRedisTemplate;
  }

  @Override
  public void set(String key, String value) {
    stringRedisTemplate.opsForValue().set(key, value);
  }

  @Override
  public void set(String key, String value, long timeout, TimeUnit unit) {
    stringRedisTemplate.opsForValue().set(key, value, timeout, unit);
  }

  @Override
  public boolean containsKey(String s) {
    return Boolean.TRUE.equals(stringRedisTemplate.hasKey(s));
  }

  @Override
  public String get(String key) {
    return stringRedisTemplate.opsForValue().get(key);
  }

  @Override
  public boolean delete(String key) {
    return Boolean.TRUE.equals(stringRedisTemplate.delete(key));
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
