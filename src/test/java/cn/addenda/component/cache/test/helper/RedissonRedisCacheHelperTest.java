package cn.addenda.component.cache.test.helper;

import cn.addenda.component.base.util.SleepUtils;
import cn.addenda.component.cache.helper.CacheHelper;
import cn.addenda.component.cache.helper.RedissonRedisCacheHelper;
import cn.addenda.component.cache.test.RedissonClientBaseTest;
import cn.addenda.component.cache.test.helper.biz.CacheHelperTestService;
import cn.addenda.component.cache.test.helper.biz.User;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author addenda
 * @since 2023/3/10 8:50
 */
@Slf4j
public class RedissonRedisCacheHelperTest {

  private AnnotationConfigApplicationContext context;

  private CacheHelper cacheHelper;

  private CacheHelperTestService service;

  @Before
  @SneakyThrows
  public void before() {
    context = new AnnotationConfigApplicationContext();
    RedissonClient redissonClient = RedissonClientBaseTest.redissonClient();
    context.registerBean(RedissonRedisCacheHelper.class, redissonClient);
    context.refresh();

    cacheHelper = context.getBean(CacheHelper.class);
    log.info(cacheHelper.toString());
    service = new CacheHelperTestService();
  }

  @After
  public void after() {
    context.close();
  }

  public static final String userCachePrefix = "user:";

  /**
   * 每次运行用唯一 id。
   * <p/>
   * 原来写死 "Q1"/"Q2"，而缓存在共享的 redis 里：删掉数据后 {@code setRdfCacheData}
   * 会写一个 {@code _NIL} 空值占位，TTL 是 {@code min(cacheNullTtl, ttl)}（默认 5 分钟）。
   * 下一次运行再查同一个 id 就命中这个占位符、直接返回 null。
   */
  private String uniqueUserId() {
    return "U" + UUID.randomUUID().toString().replace("-", "");
  }

  @Test
  public void test() {
    String userId = uniqueUserId();

    // insert 不走缓存
    service.insertUser(User.newUser(userId));

    User userFromDb1 = queryByPpf(userId);
    userFromDb1 = queryByPpf(userId);
    log.info(userFromDb1 != null ? userFromDb1.toString() : null);

    updateUserName(userId, "我被修改了！");
    User userFromDb2 = queryByPpf(userId);
    log.info(userFromDb2 != null ? userFromDb2.toString() : null);

    deleteUser(userId);
    User userFromDb3 = queryByPpf(userId);
    log.info(userFromDb3 != null ? userFromDb3.toString() : null);
  }

  private User queryByPpf(String userId) {
    return cacheHelper.queryWithPpf(userCachePrefix, userId, User.class, s -> {
//            SleepUtils.sleep(TimeUnit.SECONDS, 10);
      return service.queryBy(s);
    }, 5000L);
  }

  private void updateUserName(String userId, String userName) {
    cacheHelper.acceptWithPpf(userCachePrefix, userId, s -> service.updateUserName(s, userName));
  }

  private void deleteUser(String userId) {
    cacheHelper.acceptWithPpf(userCachePrefix, userId, s -> service.deleteUser(userId));
  }

  @Test
  public void test2() {
    String userId = uniqueUserId();

    // insert 不走缓存
    service.insertUser(User.newUser(userId));

    User userFromDb1 = queryByRdf(userId);
    log.info(userFromDb1.toString());

    updateUserName2(userId, "我被修改了！");
    User userFromDb2 = queryByRdf(userId);
    log.info(userFromDb2.toString());

    deleteUser2(userId);
    User userFromDb3 = queryByRdf(userId);
    log.info(userFromDb3 != null ? userFromDb3.toString() : null);
  }

  private User queryByRdf(String userId) {
    return cacheHelper.queryWithRdf(userCachePrefix, userId, User.class, s -> {
      SleepUtils.sleep(TimeUnit.SECONDS, 3);
      return service.queryBy(s);
    }, 500000L);
  }

  private void updateUserName2(String userId, String userName) {
    cacheHelper.acceptWithRdf(userCachePrefix, userId, s -> service.updateUserName(s, userName));
  }

  private void deleteUser2(String userId) {
    cacheHelper.acceptWithRdf(userCachePrefix, userId, s -> service.deleteUser(userId));
  }


  @Test
  public void test3() {
    String userId = uniqueUserId();
    service.insertUser(User.newUser(userId));

    Thread[] threads = new Thread[20];
    for (int i = 0; i < 20; i++) {
      threads[i] = new Thread(() -> {
        User user = queryByRdf(userId);
      });
    }

    Arrays.stream(threads).forEach(Thread::start);
    Arrays.stream(threads).forEach(thread -> {
      try {
        thread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

}
