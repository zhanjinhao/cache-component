package cn.addenda.component.cache.test;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 对应 {@link RedisClientBaseTest}，但读的是 {@code redis_cluster.properties}。
 *
 * @author addenda
 */
public class RedisClusterClientBaseTest {

  public static final String CLUSTER_PROPERTIES = "redis_cluster.properties";

  public static RedisClusterClient redisClusterClient() throws Exception {
    Properties properties = RedisTestProperties.loadClusterProperties();

    RedisClusterClient clusterClient = RedisClusterClient.create(redisURIs(properties));
    clusterClient.setOptions(ClusterClientOptions.builder()
            .maxRedirects(Integer.parseInt(properties.getProperty("maxRedirects", "3").trim()))
            .topologyRefreshOptions(ClusterTopologyRefreshOptions.builder()
                    .enableAllAdaptiveRefreshTriggers()
                    .build())
            .build());
    return clusterClient;
  }

  private static List<RedisURI> redisURIs(Properties properties) {
    String password = properties.getProperty("password");

    List<RedisURI> uris = new ArrayList<>();
    for (String node : properties.getProperty("nodes").split(",")) {
      String trimmed = node.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      String[] parts = trimmed.split(":");
      RedisURI.Builder builder = RedisURI.builder()
              .withHost(parts[0])
              .withPort(Integer.parseInt(parts[1]))
              .withTimeout(Duration.ofSeconds(5));
      // 空密码时不能调 withPassword，否则会发一个空密码的 AUTH 出去
      if (password != null && !password.isEmpty()) {
        builder.withPassword(password.toCharArray());
      }
      uris.add(builder.build());
    }
    if (uris.isEmpty()) {
      throw new IllegalStateException(CLUSTER_PROPERTIES + " 里没有配置任何节点");
    }
    return uris;
  }

}
