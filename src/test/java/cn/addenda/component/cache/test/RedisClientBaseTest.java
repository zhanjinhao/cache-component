package cn.addenda.component.cache.test;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

/**
 * @author addenda
 * @since 2023/9/21 22:34
 */
public class RedisClientBaseTest {

  public static RedisClient redisClient() throws Exception {
    return RedisClient.create(redisURI());
  }

  public static RedisURI redisURI() throws Exception {
    String path = RedisClientBaseTest.class.getClassLoader()
            .getResource("redis.properties").getPath();

    Properties properties = new Properties();
    try (InputStream in = new FileInputStream(path)) {
      properties.load(in);
    }

    // withPassword(String) 自 lettuce 6.0 起废弃：String 会长期留在字符串池里无法回收，
    // 所以用 char[] 重载（它内部会 Arrays.copyOf 做防御性拷贝）
    char[] password = properties.getProperty("password").toCharArray();

    return RedisURI.builder()
            .withHost(properties.getProperty("host"))
            .withPort(Integer.parseInt(properties.getProperty("port")))
            .withPassword(password)
            .withTimeout(Duration.ofSeconds(5))
            .build();
  }

}
