package cn.addenda.component.cache.test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author addenda
 * @since 2023/9/21 22:34
 */
public class RedisTestProperties {

  static Properties loadStandaloneProperties() throws Exception {
    String path = RedisTestProperties.class.getClassLoader()
            .getResource("redis_standalone.properties").getPath();

    Properties properties = new Properties();
    try (InputStream in = new FileInputStream(path)) {
      properties.load(in);
    }
    return properties;
  }

  public static final String CLUSTER_PROPERTIES = "redis_cluster.properties";

  public static Properties loadClusterProperties() throws Exception {
    String path = RedisClusterClientBaseTest.class.getClassLoader()
            .getResource(CLUSTER_PROPERTIES).getPath();

    Properties properties = new Properties();
    try (InputStream in = new FileInputStream(path)) {
      properties.load(in);
    }
    return properties;
  }

}
