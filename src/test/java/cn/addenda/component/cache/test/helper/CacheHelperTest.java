package cn.addenda.component.cache.test.helper;

import cn.addenda.component.base.util.SleepUtils;
import cn.addenda.component.cache.ExpiredHashMapKVCache;
import cn.addenda.component.cache.helper.CacheHelper;
import cn.addenda.component.cache.test.helper.biz.CacheHelperTestService;
import cn.addenda.component.cache.test.helper.biz.User;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author addenda
 * @since 2023/3/10 8:50
 */
@Slf4j
public class CacheHelperTest {

  private AnnotationConfigApplicationContext context;

  private CacheHelper cacheHelper;

  private CacheHelperTestService service;

  @Before
  public void before() {
    context = new AnnotationConfigApplicationContext();
    context.registerBean(CacheHelper.class, new ExpiredHashMapKVCache<String, String>() {
    });
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

  @Test
  public void test1() {
    // 此类初始化耗时
    UUID.randomUUID().toString();

    // insert 不走缓存
    service.insertUser(User.newUser("Q1"));
    printSeparator();

    User userFromDb1 = queryByPpf("Q1");
    userFromDb1 = queryByPpf("Q1");
    log.info(userFromDb1 != null ? userFromDb1.toString() : null);
    printSeparator();

    updateUserName("Q1", "我被修改了！");
    User userFromDb2 = queryByPpf("Q1");
    log.info(userFromDb2 != null ? userFromDb2.toString() : null);
    printSeparator();

    deleteUser("Q1");
    User userFromDb3 = queryByPpf("Q1");
    log.info(userFromDb3 != null ? userFromDb3.toString() : null);
    printSeparator();

    service.insertUser(User.newUser("Q1"));
    printSeparator();

    updateUserName("Q1", "我被修改了2！");
    User userFromDb5 = queryByPpf("Q1");
    log.info(userFromDb5 != null ? userFromDb5.toString() : null);
    printSeparator();

    SleepUtils.sleep(TimeUnit.SECONDS, 1);
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
    // 此类初始化耗时
    UUID.randomUUID().toString();

    // insert 不走缓存
    service.insertUser(User.newUser("Q1"));
    printSeparator();

    User userFromDb1 = queryByRdf("Q1");
    userFromDb1 = queryByRdf("Q1");
    log.info(userFromDb1 != null ? userFromDb1.toString() : null);
    printSeparator();

    updateUserName("Q1", "我被修改了！");
    User userFromDb2 = queryByRdf("Q1");
    log.info(userFromDb2 != null ? userFromDb2.toString() : null);
    printSeparator();

    deleteUser("Q1");
    User userFromDb3 = queryByRdf("Q1");
    log.info(userFromDb3 != null ? userFromDb3.toString() : null);
    printSeparator();

    service.insertUser(User.newUser("Q1"));
    printSeparator();

    updateUserName("Q1", "我被修改了2！");
    User userFromDb5 = queryByRdf("Q1");
    log.info(userFromDb5 != null ? userFromDb5.toString() : null);
    printSeparator();

    SleepUtils.sleep(TimeUnit.SECONDS, 1);
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
    // 此类初始化耗时
    UUID.randomUUID().toString();

    // insert 不走缓存
    service.insertUser(User.newUser("Q1"));
    printSeparator();

    User userFromDb1 = queryByRdf("Q1");
    userFromDb1 = queryByRdf("Q1");
    log.info(userFromDb1 != null ? userFromDb1.toString() : null);
    printSeparator();

    updateUserName2("Q1", "我被修改了！");
    User userFromDb2 = queryByRdf("Q1");
    log.info(userFromDb2 != null ? userFromDb2.toString() : null);
    printSeparator();

    deleteUser2("Q1");
    User userFromDb3 = queryByRdf("Q1");
    log.info(userFromDb3 != null ? userFromDb3.toString() : null);
    printSeparator();

    service.insertUser(User.newUser("Q1"));
    printSeparator();

    updateUserName2("Q1", "我被修改了2！");
    User userFromDb5 = queryByRdf("Q1");
    log.info(userFromDb5 != null ? userFromDb5.toString() : null);
    printSeparator();

    SleepUtils.sleep(TimeUnit.SECONDS, 1);
  }


  @Test
  public void test4() {
    service.insertUser(User.newUser("Q2"));

    Thread[] threads = new Thread[20];
    for (int i = 0; i < 20; i++) {
      threads[i] = new Thread(() -> {
        User user = queryByRdf("Q2");
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


  private void printSeparator() {
    log.info("---------------------------------------------------------------------------------------------------------------------------------");
  }

}
