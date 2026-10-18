package cn.addenda.component.cache.test;

import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Properties;

/**
 * 连着 standalone 的那套 {@link StringRedisTemplate}，与
 * {@link RedisClientBaseTest}（lettuce 原生客户端）、{@link RedisClusterClientBaseTest}（集群）、
 * {@link RedissonClientBaseTest}（redisson）并列 —— 都是"读完 properties、建好客户端、
 * 交给调用方释放"这同一件事。二者的区别只在客户端类型：那边是 lettuce / redisson 自己的
 * 客户端，这边是 spring-data-redis 的 {@link StringRedisTemplate}。
 * <p>
 * <b>为什么是两个方法而不是一个</b>：<b>持有连接、必须 {@code destroy()} 的是
 * {@link LettuceConnectionFactory}</b>，而调用方要的通常是可以直接用的
 * {@link StringRedisTemplate}。一个方法只能顾一头：返回 template 就把 factory 藏在里面，
 * 调用方拿不到它、也就释放不掉（连接泄漏）；返回 factory 就漏掉了
 * {@code new StringRedisTemplate(...)} + {@code afterPropertiesSet()} 这步固定装配。
 * 两样都给，<b>释放的责任仍然在调用方</b>——与其他几个 {@code *BaseTest} 一致，
 * 它们返回的客户端同样由调用方负责 shutdown。
 *
 * @author addenda
 */
public class StringRedisTemplateBaseTest {

  /**
   * 建一个连着 standalone 的 {@link LettuceConnectionFactory}。
   * <p>
   * <b>用完必须 {@code destroy()}</b>：它才是持有连接的那个对象，
   * 只丢掉 {@link StringRedisTemplate} 不会释放连接。
   *
   * @return 已 {@code afterPropertiesSet()} 的工厂
   */
  public static LettuceConnectionFactory lettuceConnectionFactory() throws Exception {
    Properties properties = RedisTestProperties.loadStandaloneProperties();

    LettuceConnectionFactory factory = new LettuceConnectionFactory(
            properties.getProperty("host"), Integer.parseInt(properties.getProperty("port")));
    factory.setPassword(properties.getProperty("password"));
    factory.afterPropertiesSet();
    return factory;
  }

  /**
   * 在给定的工厂之上装配一个可直接使用的 {@link StringRedisTemplate}。
   * <p>
   * {@code afterPropertiesSet()} 不能省：序列化器是在那一步配上的，漏掉之后模板用起来
   * 会在写数据时才报错，而不是在这里。
   *
   * @param factory 由 {@link #lettuceConnectionFactory()} 建出来的工厂；
   *                释放仍然要找它，见该方法说明
   */
  public static StringRedisTemplate stringRedisTemplate(LettuceConnectionFactory factory) {
    StringRedisTemplate template = new StringRedisTemplate(factory);
    template.afterPropertiesSet();
    return template;
  }

}
