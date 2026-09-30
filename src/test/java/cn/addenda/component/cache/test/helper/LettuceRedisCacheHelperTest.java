package cn.addenda.component.cache.test.helper;

import cn.addenda.component.cache.helper.CacheHelper;
import cn.addenda.component.cache.helper.LettuceRedisCacheHelper;
import cn.addenda.component.cache.helper.LettuceRedisKVCache;
import cn.addenda.component.cache.test.RedisClientBaseTest;
import cn.addenda.component.cache.test.helper.biz.CacheHelperTestService;
import cn.addenda.component.cache.test.helper.biz.User;
import io.lettuce.core.RedisClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author addenda
 */
@Slf4j
public class LettuceRedisCacheHelperTest {

  private static final String USER_CACHE_PREFIX = "user:";

  private AnnotationConfigApplicationContext context;

  private CacheHelper cacheHelper;

  private CacheHelperTestService service;

  private RedisClient redisClient;

  @Before
  @SneakyThrows
  public void before() {
    redisClient = RedisClientBaseTest.redisClient();
    context = new AnnotationConfigApplicationContext();
    context.registerBean(LettuceRedisCacheHelper.class, redisClient);
    context.refresh();

    cacheHelper = context.getBean(CacheHelper.class);
    log.info(cacheHelper.toString());
    service = new CacheHelperTestService();
  }

  @After
  public void after() {
    context.close();
    if (redisClient != null) {
      redisClient.shutdown();
    }
  }

  /**
   * 共用redis实例时，避免用例之间相互影响
   */
  private String uniqueUserId() {
    return "U" + UUID.randomUUID().toString().replace("-", "");
  }

  @Test
  public void testPpf() {
    String userId = uniqueUserId();
    service.insertUser(User.newUser(userId));
    AtomicInteger queryCounter = new AtomicInteger();

    // 缓存未命中，查询数据库
    User user = queryByPpf(userId, queryCounter);
    Assert.assertNotNull(user);
    Assert.assertEquals(userId + "姓名", user.getUsername());
    Assert.assertEquals(1, queryCounter.get());

    // 缓存命中，不再查询数据库
    user = queryByPpf(userId, queryCounter);
    Assert.assertNotNull(user);
    Assert.assertEquals(userId + "姓名", user.getUsername());
    Assert.assertEquals(1, queryCounter.get());

    // 更新数据库的同时删除缓存，再次查询能获取到新数据
    updateUserName(userId, "我被修改了！");
    waitDelayedDeletion();
    user = queryByPpf(userId, queryCounter);
    Assert.assertNotNull(user);
    Assert.assertEquals("我被修改了！", user.getUsername());
    Assert.assertEquals(2, queryCounter.get());

    // 删除数据库数据，缓存也被删除，返回null
    deleteUser(userId);
    waitDelayedDeletion();
    Assert.assertNull(queryByPpf(userId, queryCounter));
  }

  @Test
  public void testRdf() {
    String userId = uniqueUserId();
    service.insertUser(User.newUser(userId));

    User user = queryByRdf(userId);
    Assert.assertNotNull(user);
    Assert.assertEquals(userId + "姓名", user.getUsername());

    // 实时数据优先，每次更新缓存都会被同步删除
    updateUserNameByRdf(userId, "我被修改了！");
    waitDelayedDeletion();
    user = queryByRdf(userId);
    Assert.assertNotNull(user);
    Assert.assertEquals("我被修改了！", user.getUsername());

    // 删除数据库数据，返回null。空值也会被缓存
    deleteUserByRdf(userId);
    waitDelayedDeletion();
    Assert.assertNull(queryByRdf(userId));
    Assert.assertNull(queryByRdf(userId));
  }

  /**
   * 缓存命中时不应该查询数据库
   */
  @Test
  public void testRdfCacheHit() {
    String userId = uniqueUserId();
    service.insertUser(User.newUser(userId));
    AtomicInteger queryCounter = new AtomicInteger();

    Assert.assertNotNull(queryByRdf(userId, queryCounter));
    Assert.assertEquals(1, queryCounter.get());

    Assert.assertNotNull(queryByRdf(userId, queryCounter));
    Assert.assertEquals(1, queryCounter.get());
  }

  /**
   * 缓存被删除后，重新查询会再次访问数据库
   */
  @Test
  public void testDeleteCache() {
    String userId = uniqueUserId();
    service.insertUser(User.newUser(userId));
    AtomicInteger queryCounter = new AtomicInteger();

    Assert.assertNotNull(queryByRdf(userId, queryCounter));
    cacheHelper.deleteCache(USER_CACHE_PREFIX, userId, CacheHelper.REALTIME_DATA_FIRST_PREFIX);

    Assert.assertNotNull(queryByRdf(userId, queryCounter));
    Assert.assertEquals(2, queryCounter.get());
  }

  /**
   * 缓存已预热的情况下，并发查询全部命中缓存，不会击穿数据库
   */
  @Test
  public void testConcurrentQueryHitsCache() throws InterruptedException {
    String userId = uniqueUserId();
    service.insertUser(User.newUser(userId));
    AtomicInteger queryCounter = new AtomicInteger();

    Assert.assertNotNull(queryByPpf(userId, queryCounter));

    int threadCount = 20;
    CountDownLatch ready = new CountDownLatch(threadCount);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);
    List<Throwable> failures = new ArrayList<>();

    for (int i = 0; i < threadCount; i++) {
      Thread thread = new Thread(() -> {
        ready.countDown();
        try {
          start.await();
          User user = queryByPpf(userId);
          Assert.assertNotNull(user);
          Assert.assertEquals(userId + "姓名", user.getUsername());
        } catch (Throwable e) {
          synchronized (failures) {
            failures.add(e);
          }
        } finally {
          done.countDown();
        }
      });
      thread.setDaemon(true);
      thread.start();
    }

    Assert.assertTrue(ready.await(10, TimeUnit.SECONDS));
    start.countDown();
    Assert.assertTrue(done.await(30, TimeUnit.SECONDS));

    Assert.assertTrue(String.valueOf(failures), failures.isEmpty());
    Assert.assertEquals(1, queryCounter.get());
  }

  /**
   * CacheHelper使用逻辑过期时间控制缓存重建。这里验证ttl足够长时不会触发重建。
   */
  @Test
  public void testPpfDoesNotRebuildBeforeExpire() {
    String userId = uniqueUserId();
    service.insertUser(User.newUser(userId));
    AtomicInteger queryCounter = new AtomicInteger();

    Assert.assertNotNull(queryByPpf(userId, queryCounter));
    Assert.assertNotNull(queryByPpf(userId, queryCounter));
    Assert.assertNotNull(queryByPpf(userId, queryCounter));

    Assert.assertEquals(1, queryCounter.get());
  }

  // ------------------------------------------------------------------
  // 空值缓存（防缓存穿透）
  // ------------------------------------------------------------------

  /**
   * rdf：数据库里查不到的 id，空结果也会被缓存，后续查询不再打到数据库。
   * <p/>
   * 这是防缓存穿透的核心：没有这条，每次查一个不存在的 id 都会穿到库。
   */
  @Test
  public void testRdfNullIsCached() {
    // 注意：不 insert，service 里没有这个 id
    String userId = uniqueUserId();
    AtomicInteger queryCounter = new AtomicInteger();

    Assert.assertNull(queryByRdf(userId, queryCounter));
    Assert.assertEquals(1, queryCounter.get());

    // 第二次、第三次都应该命中空值占位，不再查库
    Assert.assertNull(queryByRdf(userId, queryCounter));
    Assert.assertNull(queryByRdf(userId, queryCounter));
    Assert.assertEquals("空值应该被缓存, 不应该重复查库", 1, queryCounter.get());
  }

  /**
   * rdf：空值在 redis 里的形态是 {@link CacheHelper#NULL_OBJECT} 占位符，而不是把 key 删掉。
   */
  @Test
  public void testRdfNullPlaceholderInRedis() {
    String userId = uniqueUserId();
    queryByRdf(userId, new AtomicInteger());

    String key = USER_CACHE_PREFIX + CacheHelper.REALTIME_DATA_FIRST_PREFIX + userId;
    // 另开一条连接直接看底层存了什么
    LettuceRedisKVCache probe = new LettuceRedisKVCache(redisClient);
    try {
      Assert.assertEquals("空值在 redis 里应该存成 " + CacheHelper.NULL_OBJECT + " 占位符",
              CacheHelper.NULL_OBJECT, probe.get(key));
    } finally {
      probe.close();
    }
  }

  /**
   * rdf：空值占位的存活时长受 {@code cacheNullTtl} 限制，到期后重新查库。
   * <p/>
   * 默认 cacheNullTtl 是 5 分钟，这里调到 500ms 验证它确实生效
   * （而不是按传入的 ttl=60s 缓存）。
   */
  @Test
  public void testRdfNullCacheRespectsCacheNullTtl() throws Exception {
    Long original = cacheHelper.getCacheNullTtl();
    cacheHelper.setCacheNullTtl(500L);
    try {
      String userId = uniqueUserId();
      AtomicInteger queryCounter = new AtomicInteger();

      Assert.assertNull(queryByRdf(userId, queryCounter));
      Assert.assertNull(queryByRdf(userId, queryCounter));
      Assert.assertEquals(1, queryCounter.get());

      // 等空值占位过期
      Thread.sleep(1000);

      Assert.assertNull(queryByRdf(userId, queryCounter));
      Assert.assertEquals("cacheNullTtl 到期后应该重新查库", 2, queryCounter.get());
    } finally {
      cacheHelper.setCacheNullTtl(original);
    }
  }

  /**
   * 空值缓存时长 = min(cacheNullTtl, ttl)，两者取其小。这里测反过来的方向：
   * <b>ttl 更小时按 ttl 走，cacheNullTtl 完全不参与</b>。
   * <p/>
   * 这才是默认情形 —— cacheNullTtl 默认 5 分钟，而业务传的 ttl 通常远小于它
   * （比如 60 秒），所以平时实际生效的是 ttl。
   */
  @Test
  public void testRdfNullCacheBoundedByCallerTtl() throws Exception {
    String userId = uniqueUserId();
    AtomicInteger queryCounter = new AtomicInteger();

    // ttl=500ms，cacheNullTtl 保持默认 5 分钟 -> min 的结果是 500ms
    Assert.assertNull(queryByRdf(userId, queryCounter, 500L));
    Assert.assertNull(queryByRdf(userId, queryCounter, 500L));
    Assert.assertEquals(1, queryCounter.get());

    Thread.sleep(1000);

    Assert.assertNull(queryByRdf(userId, queryCounter, 500L));
    Assert.assertEquals("ttl 小于 cacheNullTtl 时，空值应该按 ttl 过期", 2, queryCounter.get());
  }

  /**
   * ppf：空值同样被缓存（存在 CacheData 里，逻辑过期时间取 min(ttl, cacheNullTtl)）。
   */
  @Test
  public void testPpfNullIsCached() {
    String userId = uniqueUserId();
    AtomicInteger queryCounter = new AtomicInteger();

    Assert.assertNull(queryByPpf(userId, queryCounter));
    Assert.assertEquals(1, queryCounter.get());

    Assert.assertNull(queryByPpf(userId, queryCounter));
    Assert.assertNull(queryByPpf(userId, queryCounter));
    Assert.assertEquals("ppf 下空值也应该被缓存", 1, queryCounter.get());
  }

  /**
   * 空值占位必须能被写操作删除。
   * <p/>
   * 否则会出大事：查一个不存在的用户缓存了空值 → 然后真的创建了这个用户 →
   * 但缓存里还是"不存在"，新数据在 cacheNullTtl（默认 5 分钟）内永远读不到。
   */
  @Test
  public void testNullCacheInvalidatedByWrite() {
    String userId = uniqueUserId();
    AtomicInteger queryCounter = new AtomicInteger();

    // 1. 先缓存空值
    Assert.assertNull(queryByRdf(userId, queryCounter));
    Assert.assertEquals(1, queryCounter.get());

    // 2. 创建用户 + 删缓存
    cacheHelper.acceptWithRdf(USER_CACHE_PREFIX, userId, id -> service.insertUser(User.newUser(id)));
    waitDelayedDeletion();

    // 3. 空值占位被删除，重新查库拿到新数据
    User user = queryByRdf(userId, queryCounter);
    Assert.assertNotNull("空值占位应该被写操作删除, 否则新数据在 cacheNullTtl 内读不到", user);
    Assert.assertEquals(userId + "姓名", user.getUsername());
    Assert.assertEquals(2, queryCounter.get());
  }

  private User queryByPpf(String userId) {
    return cacheHelper.queryWithPpf(USER_CACHE_PREFIX, userId, User.class, service::queryBy, 60000L);
  }

  private User queryByPpf(String userId, AtomicInteger queryCounter) {
    return cacheHelper.queryWithPpf(USER_CACHE_PREFIX, userId, User.class, id -> {
      queryCounter.incrementAndGet();
      return service.queryBy(id);
    }, 60000L);
  }

  private User queryByRdf(String userId) {
    return cacheHelper.queryWithRdf(USER_CACHE_PREFIX, userId, User.class, service::queryBy, 60000L);
  }

  private User queryByRdf(String userId, AtomicInteger queryCounter) {
    return queryByRdf(userId, queryCounter, 60000L);
  }

  private User queryByRdf(String userId, AtomicInteger queryCounter, Long ttl) {
    return cacheHelper.queryWithRdf(USER_CACHE_PREFIX, userId, User.class, id -> {
      queryCounter.incrementAndGet();
      return service.queryBy(id);
    }, ttl);
  }

  private void updateUserName(String userId, String userName) {
    cacheHelper.acceptWithPpf(USER_CACHE_PREFIX, userId, id -> service.updateUserName(id, userName));
  }

  private void deleteUser(String userId) {
    cacheHelper.acceptWithPpf(USER_CACHE_PREFIX, userId, id -> service.deleteUser(id));
  }

  private void updateUserNameByRdf(String userId, String userName) {
    cacheHelper.acceptWithRdf(USER_CACHE_PREFIX, userId, id -> service.updateUserName(id, userName));
  }

  private void deleteUserByRdf(String userId) {
    cacheHelper.acceptWithRdf(USER_CACHE_PREFIX, userId, id -> service.deleteUser(id));
  }

  /**
   * accept/apply会注册一个延迟删除任务，等待任务执行完成后再断言
   */
  @SneakyThrows
  private void waitDelayedDeletion() {
    Thread.sleep(500);
  }

}
