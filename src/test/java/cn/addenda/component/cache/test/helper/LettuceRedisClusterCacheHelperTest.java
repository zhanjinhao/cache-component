package cn.addenda.component.cache.test.helper;

import cn.addenda.component.cache.helper.CacheHelper;
import cn.addenda.component.cache.helper.LettuceRedisClusterCacheHelper;
import cn.addenda.component.cache.helper.LettuceRedisClusterKVCache;
import cn.addenda.component.cache.test.RedisClusterClientBaseTest;
import cn.addenda.component.cache.test.helper.biz.CacheHelperTestService;
import cn.addenda.component.cache.test.helper.biz.User;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link LettuceRedisCacheHelperTest} 的集群版。
 * <p/>
 * 只保留和 {@link CacheHelper} 语义相关的用例 —— 底层 KVCache 的行为
 * 已由 {@link LettuceRedisClusterKVCacheTest} 逐条对齐验证过，不在这里重复。
 *
 * @author addenda
 */
@Slf4j
public class LettuceRedisClusterCacheHelperTest {

  private static final String USER_CACHE_PREFIX = "user:";

  private AnnotationConfigApplicationContext context;

  private CacheHelper cacheHelper;

  private CacheHelperTestService service;

  private RedisClusterClient redisClusterClient;

  @Before
  @SneakyThrows
  public void before() {
    redisClusterClient = RedisClusterClientBaseTest.redisClusterClient();
    context = new AnnotationConfigApplicationContext();
    context.registerBean(LettuceRedisClusterCacheHelper.class, redisClusterClient);
    context.refresh();

    cacheHelper = context.getBean(CacheHelper.class);
    log.info(cacheHelper.toString());
    service = new CacheHelperTestService();
  }

  @After
  public void after() {
    context.close();
    if (redisClusterClient != null) {
      redisClusterClient.shutdown();
    }
  }

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
    cacheHelper.deleteWithDelayedDeletion(USER_CACHE_PREFIX, userId, CacheHelper.REALTIME_DATA_FIRST_PREFIX);

    Assert.assertNotNull(queryByRdf(userId, queryCounter));
    Assert.assertEquals(2, queryCounter.get());
  }

  /**
   * rdf：数据库里查不到的 id，空结果也会被缓存，后续查询不再打到数据库
   */
  @Test
  public void testRdfNullIsCached() {
    String userId = uniqueUserId();
    AtomicInteger queryCounter = new AtomicInteger();

    Assert.assertNull(queryByRdf(userId, queryCounter));
    Assert.assertEquals(1, queryCounter.get());

    Assert.assertNull(queryByRdf(userId, queryCounter));
    Assert.assertNull(queryByRdf(userId, queryCounter));
    Assert.assertEquals("空值应该被缓存, 不应该重复查库", 1, queryCounter.get());
  }

  /**
   * rdf：空值在 redis 里的形态是 {@link CacheHelper#NULL_OBJECT} 占位符
   */
  @Test
  public void testRdfNullPlaceholderInRedis() {
    String userId = uniqueUserId();
    queryByRdf(userId, new AtomicInteger());

    String key = USER_CACHE_PREFIX + CacheHelper.REALTIME_DATA_FIRST_PREFIX + userId;
    LettuceRedisClusterKVCache probe = new LettuceRedisClusterKVCache(redisClusterClient);
    try {
      Assert.assertEquals("空值在 redis 里应该存成 " + CacheHelper.NULL_OBJECT + " 占位符",
              CacheHelper.NULL_OBJECT, probe.get(key));
    } finally {
      probe.close();
    }
  }

  /**
   * 空值缓存时长 = min(cacheNullTtl, ttl) —— cacheNullTtl 更小时按 cacheNullTtl 走
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

      Thread.sleep(1000);

      Assert.assertNull(queryByRdf(userId, queryCounter));
      Assert.assertEquals("cacheNullTtl 到期后应该重新查库", 2, queryCounter.get());
    } finally {
      cacheHelper.setCacheNullTtl(original);
    }
  }

  /**
   * 空值缓存时长 = min(cacheNullTtl, ttl) —— ttl 更小时按 ttl 走
   */
  @Test
  public void testRdfNullCacheBoundedByCallerTtl() throws Exception {
    String userId = uniqueUserId();
    AtomicInteger queryCounter = new AtomicInteger();

    Assert.assertNull(queryByRdf(userId, queryCounter, 500L));
    Assert.assertNull(queryByRdf(userId, queryCounter, 500L));
    Assert.assertEquals(1, queryCounter.get());

    Thread.sleep(1000);

    Assert.assertNull(queryByRdf(userId, queryCounter, 500L));
    Assert.assertEquals("ttl 小于 cacheNullTtl 时，空值应该按 ttl 过期", 2, queryCounter.get());
  }

  /**
   * ppf：空值同样被缓存
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
   * 空值占位必须能被写操作删除，否则新建的数据在 cacheNullTtl 内读不到
   */
  @Test
  public void testNullCacheInvalidatedByWrite() {
    String userId = uniqueUserId();
    AtomicInteger queryCounter = new AtomicInteger();

    Assert.assertNull(queryByRdf(userId, queryCounter));
    Assert.assertEquals(1, queryCounter.get());

    cacheHelper.acceptWithRdf(USER_CACHE_PREFIX, userId, id -> service.insertUser(User.newUser(id)));
    waitDelayedDeletion();

    User user = queryByRdf(userId, queryCounter);
    Assert.assertNotNull("空值占位应该被写操作删除, 否则新数据在 cacheNullTtl 内读不到", user);
    Assert.assertEquals(userId + "姓名", user.getUsername());
    Assert.assertEquals(2, queryCounter.get());
  }

  // ------------------------------------------------------------------

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

  @SneakyThrows
  private void waitDelayedDeletion() {
    Thread.sleep(500);
  }

}
