package cn.addenda.component.cache.test;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.Properties;

/**
 * @author addenda
 * @since 2023/9/21 22:34
 */
public class RedissonClientBaseTest {

  public static RedissonClient redissonClient() throws Exception {
    Properties properties = RedisTestProperties.loadStandaloneProperties();

    // 配置
    Config config = new Config();
    config.useSingleServer()
            .setConnectionPoolSize(2)
            .setConnectionMinimumIdleSize(2)
            .setSubscriptionConnectionPoolSize(2)
            .setSubscriptionConnectionMinimumIdleSize(2)
            .setAddress("redis://" + properties.get("host") + ":" + properties.get("port"))
            .setPassword((String) properties.get("password"));
    // 创建RedissonClient对象
    return Redisson.create(config);
  }

}
