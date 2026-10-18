package cn.addenda.component.cache.test;

import cn.addenda.component.base.util.SleepUtils;
import cn.addenda.component.cache.SortedKVCache;
import cn.addenda.component.cache.TreeMapKVCache;
import cn.addenda.component.cache.SynchronizedSortedKVCache;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link SynchronizedSortedKVCache}：它就是 {@link SortedKVCache} 的线程安全包装。
 * <p>
 * 契约层面的东西（null 语义、key 校验、六个实现都跑的那组断言）在
 * {@link KVCacheContractTest} 里已覆盖，这里只钉它自己的两点：
 * <b>有序视图被完整保留</b>（这正是它相对 {@link cn.addenda.component.cache.SynchronizedKVCache}
 * 的意义），以及 <b>L1 + L2</b> —— 底层是普通 {@link TreeMapKVCache}（完全不线程安全）
 * 时，包一层也能拿到并发正确性。
 *
 * @author addenda
 */
@Slf4j
public class SynchronizedSortedKVCacheTest {

  private SortedKVCache<String, String> newCache() {
    return SynchronizedSortedKVCache.synchronize(new TreeMapKVCache<>());
  }

  /**
   * 有序视图原样保留：首尾按 key 的自然顺序取，<b>空表返回 null</b>
   * （包装层原样转发底层，见 {@link SortedKVCache#getFirst()}）。
   * <p>
   * 这里以前断的是"空表抛 {@code NoSuchElementException}，与底层一致"—— 那正是把
   * {@code TreeMap.firstKey()} 的原生行为照搬了出来。后来改了：空集是一条正常的业务路径
   * （缓存当然是空的），不该逼调用方用异常处理它；而 {@code null} 在这里无歧义，
   * 因为 {@code KVCache} 的 key 不允许为 null。仓库内 {@code LruDeque#getFirst()}
   * 一直是这个口径，两边统一了。
   * <p>
   * 对照：用 {@code SynchronizedKVCache.synchronize(new TreeMapKVCache<>())} 包出来的
   * 实例是 {@code KVCache}，这三个方法<b>拿不到</b>。
   */
  @Test
  public void testOrderedViewIsPreserved() {
    SortedKVCache<String, String> cache = newCache();

    Assert.assertNull("空表取首元素返回 null，不抛异常", cache.getFirst());
    Assert.assertNull("空表取尾元素返回 null，不抛异常", cache.getLast());

    cache.set("b", "2");
    cache.set("a", "1");
    cache.set("c", "3");

    Assert.assertEquals("首元素按 key 排序，与写入先后无关", "a", cache.getFirst());
    Assert.assertEquals("尾元素按 key 排序", "c", cache.getLast());

    cache.delete("c");
    Assert.assertEquals("删掉最大的那个，尾元素跟着变", "b", cache.getLast());
  }

  /**
   * {@code keySet()} 是<b>原样转发</b>底层的活视图，不是快照 —— 包装层只加锁，不改语义。
   * <p>
   * 这也是它保护不到的地方：视图交出去之后，遍历或修改都发生在锁之外。
   * 这里把这条性质钉出来，免得以后有人误以为"包了线程安全就什么都安全"。
   */
  @Test
  public void testKeySetIsForwardedAsTheDelegatesLiveView() {
    TreeMapKVCache<String, String> delegate = new TreeMapKVCache<>();
    SortedKVCache<String, String> cache = SynchronizedSortedKVCache.synchronize(delegate);

    cache.set("a", "1");
    cache.set("b", "2");

    Set<String> keys = cache.keySet();
    Assert.assertEquals(2, keys.size());

    // 活视图：改它就是在改底层存储
    Assert.assertTrue(keys.remove("a"));
    Assert.assertEquals("改动落到了缓存上", 1, cache.size());
    Assert.assertNull("缓存里 a 真的没了", cache.get("a"));
  }

  /**
   * <b>L2：并发 computeIfAbsent 只算一次。</b>
   * <p>
   * 底层是普通 {@link TreeMapKVCache}，自身连 L1 都不承诺；
   * 包装层把整段 check-then-act 锁住之后，既能安全并发、又只算一次。
   * 与 {@link L2ContractMatrixTest#testComputeIfAbsentIsAtomicOrRefused()} 里
   * {@code ConcurrentHashMapKVCache} 那一格同形 —— 那边靠 ConcurrentHashMap 的原生子操作，
   * 这边靠整段持锁。
   */
  @Test
  public void testComputeIfAbsentIsAtomicUnderConcurrency() throws Exception {
    int threadCount = 8;
    String key = "SynchronizedSortedKVCacheTest:computeIfAbsent";

    SortedKVCache<String, String> cache = newCache();
    AtomicInteger invoked = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);
    List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

    for (int t = 0; t < threadCount; t++) {
      final int threadId = t;
      Thread thread = new Thread(() -> {
        try {
          start.await();
          cache.computeIfAbsent(key, k -> {
            invoked.incrementAndGet();
            // 把 check 与 act 之间的窗口拉大，让并发暴露出来
            SleepUtils.sleep(TimeUnit.MILLISECONDS, 100);
            return "computed";
          });
        } catch (Throwable e) {
          failures.add(new IllegalStateException("线程 " + threadId + " 失败", e));
        } finally {
          done.countDown();
        }
      });
      thread.setDaemon(true);
      thread.start();
    }

    start.countDown();
    Assert.assertTrue("不应该死循环或死锁", done.await(30, TimeUnit.SECONDS));

    for (Throwable failure : failures) {
      log.error("", failure);
    }
    Assert.assertTrue("并发下不应该抛异常", failures.isEmpty());

    log.error("{} 个线程并发 computeIfAbsent，mappingFunction 被调用了 {} 次", threadCount, invoked.get());
    Assert.assertEquals("整段 check-then-act 在锁内，只应该算一次", 1, invoked.get());
    Assert.assertEquals("值要写进去", "computed", cache.get(key));
  }

}
