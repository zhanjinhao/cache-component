package cn.addenda.component.cache.helper;

import cn.addenda.component.base.datetime.DateUtils;
import cn.addenda.component.base.exception.ComponentServiceException;
import cn.addenda.component.base.exception.SystemException;
import cn.addenda.component.base.jackson.util.JacksonUtils;
import cn.addenda.component.base.jackson.util.TypeFactoryUtils;
import cn.addenda.component.base.string.Slf4jUtils;
import cn.addenda.component.base.string.StringUtils;
import cn.addenda.component.base.util.RetryUtils;
import cn.addenda.component.base.util.SleepUtils;
import cn.addenda.component.cache.CacheException;
import cn.addenda.component.cache.ExpiredKVCache;
import cn.addenda.component.concurrency.allocator.lock.LockAllocator;
import cn.addenda.component.concurrency.allocator.lock.ReentrantLockAllocator;
import cn.addenda.component.concurrency.thread.factory.NamedThreadFactory;
import cn.addenda.component.concurrency.util.CompletableFutureUtils;
import cn.addenda.component.ratelimiter.RateLimiter;
import cn.addenda.component.ratelimiter.allocator.RateLimiterAllocator;
import cn.addenda.component.ratelimiter.allocator.RequestIntervalRateLimiterDelayReleasedAllocator;
import cn.addenda.component.ratelimiter.allocator.TokenBucketRateLimiterDelayReleasedAllocator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author addenda
 * @since 2023/05/30
 */
@Slf4j
public class CacheHelper implements DisposableBean {

  public static final String NULL_OBJECT = "_NIL";

  /**
   * ppf: performance first
   */
  public static final String PERFORMANCE_FIRST_PREFIX = "pff:";
  /**
   * rdf: realtime data first
   */
  public static final String REALTIME_DATA_FIRST_PREFIX = "rdf:";

  private static final String CACHE_EXPIRED_OR_NOT_MSG = "从缓存获取到[{}]的数据[{}][{}]";

  private static final String BUILD_CACHE_SUCCESS_MSG = "构建缓存[{}][成功]. 获取到数据[{}], 缓存到期时间[{}]";
  private static final String PPF_BUILD_CACHE_MSG = "异步构建缓存[{}][{}]. 提交时间[{}], 最大执行耗时[{}ms], 开始执行时间[{}], 最大完成时间[{}], 完成时间[{}]";
  private static final String PPF_SUBMIT_BUILD_CACHE_TASK_SUCCESS_MSG = "获取锁[{}][成功]. 提交了缓存重建任务, 返回过期数据[{}]";
  private static final String PPF_SUBMIT_BUILD_CACHE_TASK_FAILED_MSG = "获取锁[{}][失败]. 未提交缓存重建任务, 返回过期数据[{}]";

  private static final String RDF_TRY_LOCK_FAIL_TERMINAL_MSG = "xId[{}]第[{}]次未获取到锁[{}], 终止获取锁";
  private static final String RDF_TRY_LOCK_FAIL_WAIT_MSG = "xId[{}]第[{}]次未获取到锁[{}], 休眠[{}]ms";

  private static final String CLEAR_CACHE_MSG = "清理缓存[{}][{}]. xId[{}], counter[{}], 任务开始时间[{}], 清理缓存开始时间[{}], 完成时间[{}], 任务耗时[{}], 预计[{}]执行[{}], 当前时间[{}]";
  private static final String DELAY_CLEAR_CACHE_MSG = "延迟清理缓存[{}][{}]. xId[{}], counter[{}], 出生时间[{}], 提交时间[{}], 延迟[{}ms], 预期开始执行时间[{}], 实际开始执行时间[{}], 最大完成时间[{}]";

  private static final String GET_NULL_MSG = "获取到[{}]的数据为空占位";

  private static final String CACHE_HELPER_ES_MSG = "CacheHelper-Es[{}] [{}]";

  /**
   * ms <p/>
   * 空 缓存多久
   */
  @Setter
  @Getter
  private Long cacheNullTtl = 5 * 60 * 1000L;

  /**
   * ms <p/>
   * ppf: 提交异步任务后等待多久 <p/>
   * rdf: 获取不到锁时休眠多久
   */
  @Setter
  @Getter
  private long lockWaitTime = 50L;

  @Setter
  @Getter
  private int rdfBusyLoop = 3;

  /**
   * key是lockKey
   */
  private final RateLimiterAllocator<? extends RateLimiter> ppfCacheExpirationLogRateLimiterAllocator;

  /**
   * 锁的管理器，防止查询相同数据的多个线程拿到不同的锁，导致加锁失败
   */
  @Setter
  private LockAllocator<?> lockAllocator = new ReentrantLockAllocator();

  /**
   * rdf模式下，查询数据库的限流器，解决缓存击穿问题
   */
  private final RateLimiterAllocator<? extends RateLimiter> realQueryRateLimiterAllocator;

  /**
   * 缓存异步构建使用的线程池
   */
  private final ExecutorService cacheHelperEs;

  /**
   * 真正存储数据的缓存
   */
  private final ExpiredKVCache<String, String> expiredKVCache;

  /**
   * ppf模式下默认过期检测间隔
   */
  private static final long DEFAULT_PPF_EXPIRATION_DETECTION_INTERVAL = 100L;

  /**
   * ppf模式下过期检测间隔（ms）
   */
  @Setter
  private long ppfExpirationDetectionInterval = DEFAULT_PPF_EXPIRATION_DETECTION_INTERVAL;

  /**
   * 在可以使用ServiceException的地方抛ServiceException
   */
  @Setter
  private boolean useServiceException = false;

  public static final String CONCURRENCY_GRANULARITY_KEY = "CGK";

  public static final String CONCURRENCY_GRANULARITY_PREFIX = "CGP";

  private String concurrencyGranularity = CONCURRENCY_GRANULARITY_PREFIX;

  private volatile DelayQueue<DeleteTask> deleteTaskQueue;

  private volatile Thread deleteTaskConsumer;

  public void setConcurrencyGranularity(String concurrencyGranularity) {
    switch (concurrencyGranularity) {
      case CONCURRENCY_GRANULARITY_KEY:
        this.concurrencyGranularity = CONCURRENCY_GRANULARITY_KEY;
        break;
      case CONCURRENCY_GRANULARITY_PREFIX:
        this.concurrencyGranularity = CONCURRENCY_GRANULARITY_PREFIX;
        break;
      default:
        this.concurrencyGranularity = null;
        throw new IllegalArgumentException(Slf4jUtils.format("concurrencyGranularity can only be {} or {}.", CONCURRENCY_GRANULARITY_KEY, CONCURRENCY_GRANULARITY_PREFIX));
    }
  }

  public CacheHelper(ExpiredKVCache<String, String> expiredKVCache, ExecutorService cacheHelperEs,
                     RateLimiterAllocator<? extends RateLimiter> realQueryRateLimiterAllocator,
                     RateLimiterAllocator<? extends RateLimiter> ppfCacheExpirationLogRateLimiterAllocator) {
    this.expiredKVCache = expiredKVCache;
    this.cacheHelperEs = cacheHelperEs;
    this.realQueryRateLimiterAllocator = realQueryRateLimiterAllocator;
    this.ppfCacheExpirationLogRateLimiterAllocator = ppfCacheExpirationLogRateLimiterAllocator;
    this.initDeleteTaskConsumer();
  }

  public CacheHelper(ExpiredKVCache<String, String> expiredKVCache) {
    this.expiredKVCache = expiredKVCache;
    this.cacheHelperEs = createCacheHelperEs();
    this.realQueryRateLimiterAllocator = new TokenBucketRateLimiterDelayReleasedAllocator(10, 10, 1000L);
    this.ppfCacheExpirationLogRateLimiterAllocator = new RequestIntervalRateLimiterDelayReleasedAllocator(ppfExpirationDetectionInterval, ppfExpirationDetectionInterval * 10);
    this.initDeleteTaskConsumer();
  }

  protected ExecutorService createCacheHelperEs() {
    return new ThreadPoolExecutor(
            2,
            2,
            30,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(100000),
            NamedThreadFactory.of("CacheHelper", Executors.defaultThreadFactory()));
  }

  protected void initDeleteTaskConsumer() {
    deleteTaskQueue = new DelayQueue<>();
    deleteTaskConsumer = new Thread(() -> {
      while (true) {
        DeleteTask take = null;
        try {
          take = deleteTaskQueue.take();
          final DeleteTask finalTake = take;
          CompletableFutureUtils
                  .orTimeout(CompletableFuture.runAsync(() -> {
                    finalTake.setRealExecutionTime(System.currentTimeMillis());
                    expiredKVCache.delete(finalTake.getKey());
                    log.info(getDelayDeleteMsg(finalTake, "成功"));
                  }, cacheHelperEs), finalTake.getTimeout(), TimeUnit.MILLISECONDS)
                  .whenComplete(
                          (s, throwable) -> {
                            if (throwable == null) {
                              return;
                            }
                            if (throwable instanceof CompletionException && throwable.getCause() instanceof TimeoutException) {
                              // 超时的时候不再二次加入延迟队列了，因为此时服务的压力已经很大了
                              // 当线程池里有多个活跃线程时，日志里打印超时的同时任务有可能是执行成功
                              log.error(getDelayDeleteMsg(finalTake, "超时"));
                            } else {
                              // 进入这里表示执行延迟删除缓存失败了。比如网络问题。
                              if (finalTake.getCounter() > 126) {
                                log.error(getDelayDeleteMsg(finalTake, "异常不重试"), throwable);
                              } else {
                                deleteTaskQueue.put(new DeleteTask(finalTake.getXId(), (byte) (finalTake.getCounter() + 1), finalTake.getKey(), finalTake.getSince(), finalTake.getDelay()));
                                log.error(getDelayDeleteMsg(finalTake, "异常且重试"), throwable);
                              }
                            }
                          });
        } catch (RejectedExecutionException e) {
          if (take != null) {
            log.error(getDelayDeleteMsg(take, "线程池触发拒绝策略"), e);
          }
        } catch (InterruptedException e) {
          log.debug("延迟删除线程关闭");
          Thread.currentThread().interrupt();
          break;
        } catch (Throwable e) {
          if (take != null) {
            log.error(getDelayDeleteMsg(take, "未知异常"), e);
          }
          // 不会进入的逻辑
          else {
            throw SystemException.unExpectedException();
          }
        }
      }
    });
    deleteTaskConsumer.setDaemon(true);
    deleteTaskConsumer.setName("CacheHelper-DelayedDeletionThread");
    deleteTaskConsumer.start();
  }

  private String getDelayDeleteMsg(DeleteTask deleteTask, String scenario) {
    return Slf4jUtils.format(DELAY_CLEAR_CACHE_MSG, deleteTask.getKey(), scenario, deleteTask.getXId(), deleteTask.getCounter(),
            toDateTimeStr(deleteTask.getSince()),
            toDateTimeStr(deleteTask.getStart()),
            deleteTask.getDelay(),
            toDateTimeStr(deleteTask.getExpectedExecutionTime()),
            Optional.ofNullable(deleteTask.getRealExecutionTime()).map(this::toDateTimeStr).orElse("未执行"),
            toDateTimeStr(deleteTask.getExpectedExecutionTime() + deleteTask.getTimeout()));
  }

  public <I> void acceptWithPpf(String keyPrefix, I id, Consumer<I> consumer) {
    accept(keyPrefix, id, consumer, PERFORMANCE_FIRST_PREFIX);
  }

  public <I> void acceptWithRdf(String keyPrefix, I id, Consumer<I> consumer) {
    accept(keyPrefix, id, consumer, REALTIME_DATA_FIRST_PREFIX);
  }

  private <I> void accept(String keyPrefix, I id, Consumer<I> consumer, String mode) {
    String key = formatKeyPrefix(keyPrefix) + mode + id;
    long now = System.currentTimeMillis();
    consumer.accept(id);
    doDelete(key, now, true);
  }

  public <I, R> R applyWithPpf(String keyPrefix, I id, Function<I, R> function) {
    return apply(keyPrefix, id, function, PERFORMANCE_FIRST_PREFIX);
  }

  public <I, R> R applyWithRdf(String keyPrefix, I id, Function<I, R> function) {
    return apply(keyPrefix, id, function, REALTIME_DATA_FIRST_PREFIX);
  }

  private <I, R> R apply(String keyPrefix, I id, Function<I, R> function, String mode) {
    String key = formatKeyPrefix(keyPrefix) + mode + id;
    long now = System.currentTimeMillis();
    R apply = function.apply(id);
    doDelete(key, now, true);
    return apply;
  }

  public void deleteCache(String key) {
    doDelete(key, System.currentTimeMillis(), false);
  }

  private void doDelete(String key, long taskStartMs, boolean ifDelayedDeletion) {
    String xId = UUID.randomUUID().toString().replace("-", "");
    byte initCounter = 1;
    long clearCacheStartMs = System.currentTimeMillis();
    try {
      RetryUtils.retryWhenException(() -> expiredKVCache.delete(key), key);
      long completeMs = System.currentTimeMillis();
      // 删除成功时，延迟删除的时间叠加系数
      long delay = 2 * (completeMs - taskStartMs);
      if (ifDelayedDeletion) {
        DeleteTask deleteTask = new DeleteTask(xId, (byte) (initCounter + 1), key, delay);
        log.info(CLEAR_CACHE_MSG, key, "成功", xId, initCounter,
                toDateTimeStr(taskStartMs),
                toDateTimeStr(clearCacheStartMs),
                toDateTimeStr(completeMs), delay / 2,
                toDateTimeStr(deleteTask.getExpectedExecutionTime()), "延迟删除", currentDateTimeStr());
        submitDeletionTask(deleteTask);
      } else {
        log.info(CLEAR_CACHE_MSG, key, "成功", xId, initCounter,
                toDateTimeStr(taskStartMs),
                toDateTimeStr(clearCacheStartMs),
                toDateTimeStr(completeMs), delay / 2,
                "不", "延迟删除", currentDateTimeStr());
      }
    } catch (Throwable e) {
      long completeMs = System.currentTimeMillis();
      // 删除失败时，延迟删除的时间不叠加系数
      long delay = (completeMs - taskStartMs);
      if (ifDelayedDeletion) {
        DeleteTask deleteTask = new DeleteTask(xId, initCounter, key, delay);
        log.error(CLEAR_CACHE_MSG, key, "异常", xId, initCounter,
                toDateTimeStr(taskStartMs),
                toDateTimeStr(clearCacheStartMs),
                toDateTimeStr(completeMs), delay,
                toDateTimeStr(deleteTask.getExpectedExecutionTime()), "重试", currentDateTimeStr(), e);
        submitDeletionTask(deleteTask);
      } else {
        log.error(CLEAR_CACHE_MSG, key, "成功", xId, initCounter,
                toDateTimeStr(taskStartMs),
                toDateTimeStr(clearCacheStartMs),
                toDateTimeStr(completeMs), delay,
                "不", "延迟删除", currentDateTimeStr());
      }
    }
  }

  private void submitDeletionTask(DeleteTask deleteTask) {
    if (cacheHelperEs != null && !cacheHelperEs.isShutdown()) {
      deleteTaskQueue.put(deleteTask);
    } else {
      throw SystemException.unExpectedException("服务未在运行, 无法执行缓存延迟删除任务");
    }
  }

  /**
   * 性能优先的缓存查询方法，基于逻辑过期实现。
   *
   * @param keyPrefix 与id一起构成完整的键
   * @param id        键值
   * @param rType     返回值类型
   * @param rtQuery   查询实时数据
   * @param ttl       生存时间
   * @param <R>       返回值类型
   * @param <I>       键值类型
   */
  public <R, I> R queryWithPpf(
          String keyPrefix, I id, Class<R> rType, Function<I, R> rtQuery, Long ttl) {
    assertRType(rType);
    return queryWithPpf(keyPrefix, id, TypeFactoryUtils.construct(rType), rtQuery, ttl);
  }

  /**
   * 性能优先的缓存查询方法，基于逻辑过期实现。
   *
   * @param keyPrefix 与id一起构成完整的键
   * @param id        键值
   * @param rType     返回值类型
   * @param rtQuery   查询实时数据
   * @param ttl       生存时间
   * @param <R>       返回值类型
   * @param <I>       键值类型
   */
  public <R, I> R queryWithPpf(
          String keyPrefix, I id, TypeReference<R> rType, Function<I, R> rtQuery, Long ttl) {
    return queryWithPpf(keyPrefix, id, TypeFactoryUtils.construct(rType), rtQuery, ttl);
  }

  /**
   * 性能优先的缓存查询方法，基于逻辑过期实现。
   *
   * @param keyPrefix 与id一起构成完整的键
   * @param id        键值
   * @param rType     返回值类型
   * @param rtQuery   查询实时数据
   * @param ttl       生存时间
   * @param <R>       返回值类型
   * @param <I>       键值类型
   */
  public <R, I> R queryWithPpf(
          String keyPrefix, I id, JavaType rType, Function<I, R> rtQuery, Long ttl) {
    String key = formatKeyPrefix(keyPrefix) + PERFORMANCE_FIRST_PREFIX + id;
    // 1 查询缓存
    String cachedJson = expiredKVCache.get(key);
    // 2.1 缓存不存在则基于互斥锁构建缓存
    if (cachedJson == null) {
      // 查询数据库
      R r = queryWithRdf(keyPrefix, id, rType, rtQuery, ttl, false);
      // 存在缓存里
      setCacheData(key, r, ttl);
      return r;
    }
    // 2.2 缓存存在则进入逻辑过期的判断
    else {
      String lockKey = getLockKey(keyPrefix, key);
      // 3.1 命中，需要先把json反序列化为对象
      CacheData<R> cacheData = JacksonUtils.toObj(cachedJson, TypeFactoryUtils.constructParametricType(CacheData.class, rType));
      LocalDateTime expireTime = cacheData.getExpireTime();
      R data = cacheData.getData();
      // 4.1 判断是否过期，未过期，直接返回
      if (expireTime.isAfter(LocalDateTime.now())) {
        log.debug(CACHE_EXPIRED_OR_NOT_MSG, key, data, "未过期");
      }
      // 4.2 判断是否过期，已过期，需要缓存重建
      else {
        // 5.1 获取互斥锁，成功，开启独立线程，进行缓存重建
        Lock lock = lockAllocator.allocate(lockKey);
        if (lock.tryLock()) {
          try {
            AtomicBoolean newDataReady = new AtomicBoolean(false);
            AtomicReference<R> newData = new AtomicReference<>(null);
            submitPpfRebuildTask(key, id, ttl, rtQuery, newDataReady, newData);
            // 提交完缓存构建任务后休息一段时间，防止其他线程提交缓存构建任务
            SleepUtils.sleep(TimeUnit.MILLISECONDS, lockWaitTime);
            if (newDataReady.get()) {
              return newData.get();
            } else {
              log.info(PPF_SUBMIT_BUILD_CACHE_TASK_SUCCESS_MSG, lockKey, data);
            }
          } finally {
            try {
              lock.unlock();
            } finally {
              lockAllocator.release(lockKey);
            }
          }
        }
        // 5.2 获取互斥锁，未成功不进行缓存重建
        else {
          lockAllocator.release(lockKey);
          log.info(PPF_SUBMIT_BUILD_CACHE_TASK_FAILED_MSG, lockKey, data);

          // -----------------------------------------------------------
          // 提交重建的线程如果没有在等待时间内获取到新的数据，不会走下面的告警。
          // 这是为了防止低并发下输出不必要的日志。
          // -----------------------------------------------------------

          // 如果过期了，输出告警信息。
          // 使用限流器防止高并发下大量打印日志。
          RateLimiter rateLimiter = ppfCacheExpirationLogRateLimiterAllocator.allocate(lockKey);
          try {
            if (rateLimiter.tryAcquire()) {
              log.error(CACHE_EXPIRED_OR_NOT_MSG, key, data, "已过期");
            }
          } finally {
            ppfCacheExpirationLogRateLimiterAllocator.delayRelease(lockKey);
          }
        }
      }
      return data;
    }
  }

  private <R, I> void submitPpfRebuildTask(String key, I id, Long ttl, Function<I, R> rtQuery,
                                           AtomicBoolean newDataReady, AtomicReference<R> newData) {
    long now = System.currentTimeMillis();
    cacheHelperEs.submit(() -> {
      long start = System.currentTimeMillis();
      long maxCost = 2 * lockWaitTime;
      try {
        // 查询数据库
        R r = rtQuery.apply(id);
        // 存在缓存里
        newData.set(r);
        newDataReady.set(true);
        setCacheData(key, r, ttl);
        if (System.currentTimeMillis() - now > maxCost) {
          log.error(PPF_BUILD_CACHE_MSG, key, "超时", toDateTimeStr(now), maxCost, toDateTimeStr(start),
                  toDateTimeStr(now + maxCost), toDateTimeStr(System.currentTimeMillis()));
        }
      } catch (Throwable e) {
        log.error(PPF_BUILD_CACHE_MSG, key, "异常", toDateTimeStr(now), maxCost, toDateTimeStr(start),
                toDateTimeStr(now + maxCost), toDateTimeStr(System.currentTimeMillis()), e);
      }
    });
  }

  private <R> void setCacheData(String key, R r, long ttl) {
    // 设置逻辑过期
    CacheData<R> newCacheData = new CacheData<>(r);
    if (r == null) {
      newCacheData.setExpireTime(LocalDateTime.now().plus(Math.min(ttl, cacheNullTtl), ChronoUnit.MILLIS));
    } else {
      newCacheData.setExpireTime(LocalDateTime.now().plus(ttl, ChronoUnit.MILLIS));
    }
    // 写缓存
    expiredKVCache.set(key, JacksonUtils.toStr(newCacheData), ttl * 2, TimeUnit.MILLISECONDS);
    log.info(BUILD_CACHE_SUCCESS_MSG, key, r, toDateTimeStr(DateUtils.localDateTimeToTimestamp(newCacheData.getExpireTime())));
  }


  /**
   * 实时数据优先的缓存查询方法，基于互斥锁实现。
   *
   * @param keyPrefix 与id一起构成完整的键
   * @param id        键值
   * @param rType     返回值类型
   * @param rtQuery   查询实时数据
   * @param ttl       生存时间
   * @param <R>       返回值类型
   * @param <I>       键值类型
   */
  public <R, I> R queryWithRdf(
          String keyPrefix, I id, JavaType rType, Function<I, R> rtQuery, Long ttl) {
    return queryWithRdf(keyPrefix, id, rType, rtQuery, ttl, true);
  }

  /**
   * 实时数据优先的缓存查询方法，基于互持锁实现。
   *
   * @param keyPrefix 与id一起构成完整的键
   * @param id        键值
   * @param rType     返回值类型
   * @param rtQuery   查询实时数据
   * @param ttl       生存时间
   * @param <R>       返回值类型
   * @param <I>       键值类型
   */
  public <R, I> R queryWithRdf(
          String keyPrefix, I id, Class<R> rType, Function<I, R> rtQuery, Long ttl) {
    assertRType(rType);
    return queryWithRdf(keyPrefix, id, TypeFactoryUtils.construct(rType), rtQuery, ttl, true);
  }

  /**
   * 实时数据优先的缓存查询方法，基于互持锁实现。
   *
   * @param keyPrefix 与id一起构成完整的键
   * @param id        键值
   * @param rType     返回值类型
   * @param rtQuery   查询实时数据
   * @param ttl       生存时间
   * @param <R>       返回值类型
   * @param <I>       键值类型
   */
  public <R, I> R queryWithRdf(
          String keyPrefix, I id, TypeReference<R> rType, Function<I, R> rtQuery, Long ttl) {
    return queryWithRdf(keyPrefix, id, TypeFactoryUtils.construct(rType), rtQuery, ttl, true);
  }

  /**
   * 实时数据优先的缓存查询方法，基于互持锁实现。
   *
   * @param keyPrefix 与id一起构成完整的键
   * @param id        键值
   * @param rType     返回值类型
   * @param rtQuery   查询实时数据
   * @param ttl       生存时间
   * @param cache     是否将实时查询的数据缓存
   * @param <R>       返回值类型
   * @param <I>       键值类型
   */
  private <R, I> R queryWithRdf(
          String keyPrefix, I id, JavaType rType, Function<I, R> rtQuery, Long ttl, boolean cache) {
    return doQueryWithRdf(keyPrefix, id, rType, rtQuery, ttl, 0, UUID.randomUUID().toString().replace("-", ""), cache);
  }

  /**
   * 实时数据优先的缓存查询方法，基于互持锁实现。
   *
   * @param keyPrefix 与id一起构成完整的键
   * @param id        键值
   * @param rType     返回值类型
   * @param rtQuery   查询实时数据
   * @param ttl       生存时间
   * @param xId       一次查询请求的唯一标识
   * @param itr       第几次尝试
   * @param cache     是否将实时查询的数据缓存
   * @param <R>       返回值类型
   * @param <I>       键值类型
   */
  private <R, I> R doQueryWithRdf(
          String keyPrefix, I id, JavaType rType, Function<I, R> rtQuery, Long ttl, int itr, String xId, boolean cache) {
    String key = formatKeyPrefix(keyPrefix) + REALTIME_DATA_FIRST_PREFIX + id;
    // 1.查询缓存
    String resultJson = expiredKVCache.get(key);
    // 2.如果返回的是占位的空值，返回null
    if (NULL_OBJECT.equals(resultJson)) {
      log.debug(GET_NULL_MSG, key);
      return null;
    }
    // 3.1如果字符串不为空，返回对象
    if (resultJson != null) {
      log.debug(CACHE_EXPIRED_OR_NOT_MSG, key, resultJson, "未过期");
      return JacksonUtils.toObj(resultJson, rType);
    }
    // 3.2如果字符串为空，进行缓存构建
    else {
      Supplier<R> supplier = () -> {
        R r = rtQuery.apply(id);
        if (cache) {
          LocalDateTime expireTime = LocalDateTime.now();
          if (r == null) {
            long realTtl = Math.min(cacheNullTtl, ttl);
            expireTime = expireTime.plus(realTtl, ChronoUnit.MILLIS);
            expiredKVCache.set(key, NULL_OBJECT, realTtl, TimeUnit.MILLISECONDS);
          } else {
            expireTime = expireTime.plus(ttl, ChronoUnit.MILLIS);
            expiredKVCache.set(key, JacksonUtils.toStr(r), ttl, TimeUnit.MILLISECONDS);
          }
          log.info(BUILD_CACHE_SUCCESS_MSG, key, r, toDateTimeStr(DateUtils.localDateTimeToTimestamp(expireTime)));
        }
        return r;
      };

      String lockKey = getLockKey(keyPrefix, key);
      // 单线程循环：qry, del, qry, del ...，会超过限流器的阈值
      // 但是系统的压力不大，因为是单线程：1、查询速度非常块；2、流量低。
      RateLimiter rateLimiter = realQueryRateLimiterAllocator.allocate(lockKey);
      if (rateLimiter.tryAcquire()) {
        try {
          return supplier.get();
        } finally {
          realQueryRateLimiterAllocator.release(lockKey);
        }
      } else {
        realQueryRateLimiterAllocator.release(lockKey);

        // 4.1获取互斥锁，获取到进行缓存构建
        Lock lock = lockAllocator.allocate(lockKey);
        if (lock.tryLock()) {
          try {
            return supplier.get();
          } finally {
            try {
              lock.unlock();
            } finally {
              lockAllocator.release(lockKey);
            }
          }
        }
        // 4.2获取互斥锁，获取不到就休眠直至抛出异常
        else {
          lockAllocator.release(lockKey);
          itr++;
          if (itr >= rdfBusyLoop) {
            log.error(RDF_TRY_LOCK_FAIL_TERMINAL_MSG, xId, itr, lockKey);
            if (useServiceException) {
              throw new ComponentServiceException("系统繁忙, 请稍后再试");
            } else {
              throw new CacheException("系统繁忙, 请稍后再试");
            }
          } else {
            log.info(RDF_TRY_LOCK_FAIL_WAIT_MSG, xId, itr, lockKey, lockWaitTime);
            SleepUtils.sleep(TimeUnit.MILLISECONDS, lockWaitTime);
            // 递归进入的时候，当前线程的tryLock是失败的，所以当前线程不持有锁，即递归进入的状态和初次进入的状态一致
            return doQueryWithRdf(keyPrefix, id, rType, rtQuery, ttl, itr, xId, cache);
          }
        }
      }
    }
  }

  private String getLockKey(String keyPrefix, String key) {
    if (CONCURRENCY_GRANULARITY_PREFIX.equals(concurrencyGranularity)) {
      return StringUtils.biTrimSpecifiedChar(keyPrefix, ':') + ":lock";
    } else if (CONCURRENCY_GRANULARITY_KEY.equals(concurrencyGranularity)) {
      return StringUtils.biTrimSpecifiedChar(keyPrefix, ':') + ":" + StringUtils.biTrimSpecifiedChar(key, ':') + ":lock";
    }
    throw SystemException.unExpectedException();
  }

  @Override
  public void destroy() throws Exception {
    if (deleteTaskConsumer != null) {
      deleteTaskConsumer.interrupt();
    }
    if (cacheHelperEs != null) {
      try {
        log.info(CACHE_HELPER_ES_MSG, cacheHelperEs, "开始关闭");
        // Disable new tasks from being submitted
        cacheHelperEs.shutdown();
        // Wait a while for existing tasks to terminate
        if (!cacheHelperEs.awaitTermination(30, TimeUnit.SECONDS)) {
          log.error(CACHE_HELPER_ES_MSG, cacheHelperEs, "关闭后等待超过30秒未终止");
          // Cancel currently executing tasks
          cacheHelperEs.shutdownNow();
          // Wait a while for tasks to respond to being cancelled
          cacheHelperEs.awaitTermination(30, TimeUnit.SECONDS);
        }
        log.info(CACHE_HELPER_ES_MSG, cacheHelperEs, "正常关闭");
      } catch (InterruptedException e) {
        // (Re-)Cancel if current thread also interrupted
        cacheHelperEs.shutdownNow();
        log.error(CACHE_HELPER_ES_MSG, cacheHelperEs, "异常关闭", e);
        // Preserve interrupt status
        Thread.currentThread().interrupt();
      }
    }
    executeRemainingDeleteTask();
  }

  private void executeRemainingDeleteTask() {
    if (deleteTaskQueue == null) {
      return;
    }
    DeleteTask[] array = deleteTaskQueue.toArray(new DeleteTask[]{});
    if (array.length > 0) {
      log.error(CACHE_HELPER_ES_MSG, cacheHelperEs, Slf4jUtils.format("已关闭, 还有[{}]个任务未被执行", array.length));
      for (DeleteTask deleteTask : array) {
        try {
          RetryUtils.retryWhenException(() -> expiredKVCache.delete(deleteTask.getKey()), deleteTask.getKey());
        } catch (Throwable e) {
          log.error(getDelayDeleteMsg(deleteTask, "服务关闭时延迟删除任务执行异常"), e);
        }
      }
    }
  }

  @Getter
  @ToString
  private static class DeleteTask implements Delayed {

    /**
     * 每一次写操作都会生成一个xId。一个xId表示一次更新数据库，多次缓存删除重试具备相同的xId。
     */
    private final String xId;

    /**
     * 删除次数计数器
     */
    private final byte counter;

    /**
     * 业务key
     */
    private final String key;

    /**
     * 出生时间
     */
    private final long since;

    /**
     * 提交时间。由于存在重试场景，所以提交时间和出生时间不一致。
     */
    private final long start;

    /**
     * 更新数据库 + 删除缓存
     */
    private final long delay;

    /**
     * 预计执行时间
     */
    private final long expectedExecutionTime;

    @Setter
    private Long realExecutionTime;

    private DeleteTask(String xId, byte counter, String key, long delay) {
      this.xId = xId;
      this.counter = counter;
      this.key = key;
      this.delay = delay;
      this.since = System.currentTimeMillis();
      this.start = since;
      this.expectedExecutionTime = calculateExpectedExecutionTime(delay);
    }

    private DeleteTask(String xId, byte counter, String key, long since, long delay) {
      this.xId = xId;
      this.counter = counter;
      this.key = key;
      this.delay = delay;
      this.since = since;
      this.start = System.currentTimeMillis();
      this.expectedExecutionTime = calculateExpectedExecutionTime(delay);
    }

    public static long calculateExpectedExecutionTime(long delay) {
      return System.currentTimeMillis() + delay;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(expectedExecutionTime - System.currentTimeMillis(), unit);
    }

    @Override
    public int compareTo(Delayed o) {
      if (o == null) {
        throw new NullPointerException("arg can not be null!");
      }
      DeleteTask t = (DeleteTask) o;
      if (this.expectedExecutionTime - t.expectedExecutionTime < 0) {
        return -1;
      } else if (this.expectedExecutionTime == t.expectedExecutionTime) {
        return 0;
      } else {
        return 1;
      }
    }

    public long getTimeout() {
      return delay * 2;
    }

  }

  private String currentDateTimeStr() {
    return toDateTimeStr(System.currentTimeMillis());
  }

  private String toDateTimeStr(long ts) {
    return DateUtils.format(DateUtils.timestampToLocalDateTime(ts), DateUtils.yMdHmsS_FORMATTER);
  }

  private String formatKeyPrefix(String a) {
    return StringUtils.biTrimSpecifiedChar(a, ':') + ":";
  }

  private void assertRType(Class<?> rType) {
    if (Collection.class.isAssignableFrom(rType)) {
      throw new CacheException(Slf4jUtils.format("不支持集合类型, 当前类型为[{}]", rType));
    }
    if (Map.class.isAssignableFrom(rType)) {
      throw new CacheException(Slf4jUtils.format("不支持Map类型, 当前类型为[{}]", rType));
    }
  }

}
