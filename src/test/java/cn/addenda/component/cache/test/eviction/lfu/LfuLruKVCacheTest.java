package cn.addenda.component.cache.test.eviction.lfu;

import cn.addenda.component.cache.TestDelegateAccess;
import cn.addenda.component.cache.eviction.lfu.LfuKVCache;
import cn.addenda.component.cache.eviction.lru.LruHashMapKVCache;
import cn.addenda.component.cache.eviction.lru.LruKVCache;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link LfuKVCache} 叠一层 LRU 时，<b>两层容量不一致</b>所引发的问题。
 * <p>
 * 两层容量一致的情况（推荐配置，底层 LRU 永远不会自行淘汰）在
 * {@code LfuHashMapKVCacheTest} 里与 {@code LfuHashMapKVCache} 跑同一组断言 ——
 * 那两个实现此时行为等价，不必在这里重复。
 * <p>
 * 容量一旦不一致，"LFU 先腾位置、底层不会自行淘汰"这个前提就不成立了：
 * <ul>
 *   <li>LFU 按外层容量判断，从不满，于是从不主动淘汰；</li>
 *   <li>写入一路透到底层，由底层按自己的容量淘汰队头；</li>
 *   <li>底层淘汰时<b>不会通知 LFU 层</b>，计数与档位因此残留，<b>账本</b>虚高（{@code size()} 取自数据层，不受影响）。</li>
 * </ul>
 * 本文件只钉这件事，以及它的各种后果：残留的清理（自愈）、清理带来的口径变化
 * （delete 的返回值、分数丢失）、以及多轮之后能否收敛。
 * <p>
 * 例外一条：两级自愈叠加（{@code testSelfHealPropagatesThroughBothLayers}）跟容量是否一致
 * 无关，它只要求"底层自己也有账本"，因此容量一致时同样成立 —— 放这里是因为只有本实现
 * 的底层是 LRU。
 *
 * @author addenda
 */
public class LfuLruKVCacheTest {

  /**
   * 外层（LFU 层）声称的容量
   */
  private static final int OUTER_CAPACITY = 5;
  /**
   * 内层（LRU 层）实际的容量，故意小于外层
   */
  private static final int INNER_CAPACITY = 2;

  /**
   * 外层声称 5 格、内层实际只有 2 格的 LFU 叠 LRU
   */
  private LfuKVCache<String, String> mismatched() {
    LruKVCache<String, String> delegate = new LruWithDequeVisible<>(INNER_CAPACITY);
    return new LfuOverLru<>(OUTER_CAPACITY, delegate);
  }

  /**
   * <b>外层声称的容量没有兑现</b>：{@code capacity()} 返回 5，而底层实际只装得下 2。
   * <p>
   * 这个落差就是本文件其余用例的根因：LFU 拿 5 去判断"满没满"，写 3 条时
   * {@code capacity() == size()} 不成立，它一个都不淘汰，淘汰全部由底层按 2 执行。
   */
  @Test
  public void testCapacityIsNotHonoredByUnderlyingLru() {
    LfuKVCache<String, String> layered = mismatched();

    Assert.assertEquals("外层声称 5 格", OUTER_CAPACITY, layered.capacity());
    Assert.assertEquals("底层实际只有 2 格", INNER_CAPACITY, TestDelegateAccess.delegateOf(layered).capacity());

    layered.set("a", "1");
    layered.set("b", "2");
    layered.set("c", "3");

    Assert.assertEquals("LFU 按 5 判断，写 3 条还远没满，所以它一个都没淘汰", 3, layered.getKeyToVisitorCount().size());
    Assert.assertEquals("真正在淘汰的是底层", INNER_CAPACITY, TestDelegateAccess.delegateOf(layered).size());
  }

  /**
   * <b>两层容量不一致时，底层 LRU 会自行淘汰，残留的计数靠下次访问自愈。</b>
   * <p>
   * 内层只有 2 格而外层声称 5 格时，写第 3 条就会让内层自行淘汰队头，
   * 而 LFU 层收不到任何通知 —— 计数留在那里，<b>账本</b>条数大于真实存活数。
   * <p>
   * 这里把"虚高的是账本、不是 {@code size()}"钉死：两者在这时必须分叉，
   * 而 {@code size()} 与底层存储始终一致。
   * <p>
   * 残留由 {@code EvictableKVCache#containsKey} 在下次访问到该 key 时清除（自愈）。
   */
  @Test
  public void testStatisticsSelfHealWhenUnderlyingLruEvicts() {
    LfuKVCache<String, String> layered = mismatched();

    layered.set("a", "1");
    layered.set("b", "2");
    layered.set("c", "3");      // 内层已满，自行淘汰队头 a

    Assert.assertEquals("底层只剩 2 条", INNER_CAPACITY, TestDelegateAccess.delegateOf(layered).size());
    Assert.assertEquals("size() 取自数据层，报的仍是真实条数", INNER_CAPACITY, layered.size());
    Assert.assertEquals("LFU 统计没收到通知，账本虚高成 3", 3, layered.getKeyToVisitorCount().size());
    Assert.assertTrue("虚高的是账本而不是 size()，两者在这时必须分叉",
            layered.size() != layered.getKeyToVisitorCount().size());

    // 访问 a 触发自愈
    Assert.assertFalse("a 已被底层淘汰", layered.containsKey("a"));
    Assert.assertEquals("统计追平底层存储", TestDelegateAccess.delegateOf(layered).size(), layered.getKeyToVisitorCount().size());
    Assert.assertEquals("自愈不会影响底层真实数据", INNER_CAPACITY, TestDelegateAccess.delegateOf(layered).size());

    Assert.assertEquals("b 仍在", "2", layered.get("b"));
    Assert.assertEquals("c 仍在", "3", layered.get("c"));
  }

  /**
   * 自愈之后，那个 key 的统计已经摘干净，可以重新写入并正常读回。
   * <p>
   * 注意重新写入会再次触发一轮"底层自行淘汰"（内层只有 2 格，写入必然挤出队头），
   * 这是容量不一致的固有代价，不是自愈失效 —— 再自愈一次两层即重新对齐。
   */
  @Test
  public void testReinsertAfterSelfHeal() {
    LfuKVCache<String, String> layered = mismatched();

    layered.set("a", "1");
    layered.set("b", "2");
    layered.set("c", "3");      // 底层淘汰 a
    layered.containsKey("a");   // 触发自愈

    layered.set("a", "1-again");

    Assert.assertEquals("自愈过之后可以重新写进来并读回", "1-again", layered.get("a"));

    // 这次轮到 b 被内层挤出，再触发一次自愈，两层重新对齐
    Assert.assertFalse(layered.containsKey("b"));
    Assert.assertEquals(TestDelegateAccess.delegateOf(layered).size(), layered.getKeyToVisitorCount().size());
  }

  /**
   * <b>底层已自行淘汰 k 时，{@code delete(k)} 返回 false —— 但账本已经被清了。</b>
   * <p>
   * {@code LfuKVCache.delete} 的顺序是"先清自己的统计，再委托底层删除"，返回值取自底层。
   * 底层早就没有这个 key 了，于是返回 false；调用方看到 false 会以为"什么都没删掉"，
   * 实际上 {@code size()} 已经减了。这是容量不一致才有的口径问题 ——
   * 容量一致时，账本里有值的 key 必然在底层也活着，两者不会分叉。
   */
  @Test
  public void testDeleteReturnsFalseWhenBackingStoreAlreadyEvicted() {
    LfuKVCache<String, String> layered = mismatched();

    layered.set("a", "1");
    layered.set("b", "2");
    layered.set("c", "3");      // 底层自行挤掉 a，账本里 a 还在

    Assert.assertFalse("底层已经没有 a，所以返回 false", layered.delete("a"));
    Assert.assertEquals("但账本确实被清了：3 → 2", 2, layered.getKeyToVisitorCount().size());
    Assert.assertEquals("两层由此对齐", TestDelegateAccess.delegateOf(layered).size(), layered.getKeyToVisitorCount().size());
  }

  /**
   * <b>底层已淘汰的 key 可以用 {@code setnx} 重新写进来，且写入前的自愈会清掉它残留的分数档位。</b>
   * <p>
   * {@code setnx} 先问 {@code containsKey}：底层没有 → 自愈清掉残留计数与档位 → 返回 false，
   * 于是走写入路径重新落地。
   * <p>
   * 自愈的价值不是"让 {@code containsKey} 返回 false"—— 它本来就按数据层判断；而是避免残留的
   * 旧档位与新写入打架：写入只把计数改成 0，不会把 key 从旧档位摘掉。不清理的话，账本里就会
   * 留下一个"计数 0 却挂在 1 档"的幽灵。
   */
  @Test
  public void testSetnxCanRewriteKeyEvictedByBackingStore() {
    LfuKVCache<String, String> layered = mismatched();   // 外层 5，内层 2

    layered.set("a", "1");
    layered.set("b", "2");
    layered.get("a");            // a 计数 1 → 落到 1 档；内层 LRU 把 a 挪到队尾
    layered.set("c", "3");       // 内层挤掉 b
    layered.set("d", "4");       // 内层挤掉 a —— a 的 1 分档位留在账本里

    Assert.assertEquals("前置：a 的残留分数还在", Long.valueOf(1L),
            layered.getKeyToVisitorCount().get("a"));
    Assert.assertNotNull("a 还挂在 1 档", layered.getVisitorCountToKeySet().get(1L));

    Assert.assertTrue("自愈之后能够重新写入", layered.setnx("a", "1-again"));

    Assert.assertNull("自愈清掉了 a 的旧档位，不该留下幽灵",
            layered.getVisitorCountToKeySet().get(1L));
    Assert.assertEquals("写得进去、读得出来", "1-again", layered.get("a"));
  }

  /**
   * <b>一次访问清掉两级残留。</b>
   * <p>
   * 底层 LRU 自己也有账本（队列），它的 delegate 才是真正存值的 HashMap。
   * 值被第三方丢掉时（过期、被别的写入挤掉），两级都会留下残留：
   * <ul>
   *   <li>底层 LRU 的队列里还留着 k；</li>
   *   <li>LFU 的计数表里也还留着 k。</li>
   * </ul>
   * {@code containsKey} 一层套一层地自愈，因此一次访问之后两个数字应当同时对齐。
   * 底层是普通 HashMap 的实现只有一级自愈，没有这个现象。
   */
  @Test
  public void testSelfHealPropagatesThroughBothLayers() {
    LfuKVCache<String, String> layered = mismatched();
    LruWithDequeVisible<String, String> innerLru =
            (LruWithDequeVisible<String, String>) TestDelegateAccess.delegateOf(layered);

    layered.set("a", "1");
    Assert.assertEquals("LFU 账本里有 a", 1, layered.getKeyToVisitorCount().size());
    Assert.assertEquals("底层 LRU 队列里也有 a", 1L, innerLru.dequeSize());

    // 模拟最底层的值被丢掉（过期 / 被外部删除）
    TestDelegateAccess.delegateOf(innerLru).delete("a");

    Assert.assertEquals("底层 LRU 的队列里还留着 a", 1L, innerLru.dequeSize());
    Assert.assertEquals("LFU 的计数表里也还留着 a", 1, layered.getKeyToVisitorCount().size());

    // 触发一次访问：自愈一层套一层
    Assert.assertFalse("值已经取不到了", layered.containsKey("a"));

    Assert.assertEquals("底层 LRU 的队列自愈", 0L, innerLru.dequeSize());
    Assert.assertEquals("LFU 的计数表自愈", 0, layered.getKeyToVisitorCount().size());
  }

  /**
   * <b>高分 key 被底层挤掉之后，分数也会一起丢。</b>
   * <p>
   * 一个被读了 20 次的 key，被底层 LRU 按自己的顺序挤掉后 LFU 侧毫不知情；
   * 再访问它时走自愈，20 分被<b>直接抹掉</b>，重新写入时从 0 分起步 ——
   * 下一次淘汰第一个走。
   * <p>
   * 对比：两层容量一致时不会发生 —— 淘汰一律由 LFU 决定，分数与存活状态同源；
   * 而这里的两者已经脱钩，"值没了"和"分数归零"之间隔了一次访问。
   */
  @Test
  public void testFrequentlyUsedKeyLosesItsScoreWhenEvictedByBackingStore() {
    LfuKVCache<String, String> layered = mismatched();

    layered.set("hot", "1");
    for (int i = 0; i < 20; i++) {
      layered.get("hot");
    }
    Assert.assertEquals("hot 攒到 20 分", Long.valueOf(20L), layered.getKeyToVisitorCount().get("hot"));

    // 内层只有 2 格，再写两条就把 hot 挤出底层
    layered.set("k0", "v");
    layered.set("k1", "v");

    Assert.assertNull("hot 已被底层挤掉", layered.get("hot"));
    Assert.assertNull("20 分连同它一起被自愈抹掉", layered.getKeyToVisitorCount().get("hot"));

    layered.set("hot", "2");
    Assert.assertEquals("重新写入时从 0 分起步", Long.valueOf(0L), layered.getKeyToVisitorCount().get("hot"));
  }

  /**
   * <b>多轮"底层自行淘汰 → 访问自愈"之后，两个数字始终不越界，且最终收敛。</b>
   * <p>
   * 每一轮写入都会在底层制造一次新的分叉（底层按自己的容量挤掉队头），
   * 而 LFU 层只有被访问到时才会知道。所以这里不能用"跑一遍看结果"来覆盖，
   * 而是每轮都断言不变量：
   * <ul>
   *   <li>LFU 账本不会小于底层真实条数 —— 分叉只会是"账本多算了"，不会反方向；</li>
   *   <li>底层不会超过自己的容量。</li>
   * </ul>
   * 把所有 key 都访问过一遍之后，账本里凡是底层已经没有的都会被自愈掉，两者必须相等。
   */
  @Test
  public void testStatisticsConvergeAfterRepeatedSelfHeal() {
    LfuKVCache<String, String> layered = mismatched();

    int rounds = 20;
    for (int i = 0; i < rounds; i++) {
      layered.set("k" + i, "v" + i);

      Assert.assertTrue("第 " + i + " 轮：账本不可能小于底层真实条数",
              layered.getKeyToVisitorCount().size() >= TestDelegateAccess.delegateOf(layered).size());
      Assert.assertTrue("第 " + i + " 轮：底层不会超过自己的容量",
              TestDelegateAccess.delegateOf(layered).size() <= INNER_CAPACITY);
    }

    // 逐个访问，把残留的账本项自愈掉
    for (int i = 0; i < rounds; i++) {
      layered.get("k" + i);
    }

    Assert.assertEquals("全部访问过一遍之后两层对齐",
            TestDelegateAccess.delegateOf(layered).size(), layered.getKeyToVisitorCount().size());
    Assert.assertTrue("底层仍然守着自己的容量",
            TestDelegateAccess.delegateOf(layered).size() <= INNER_CAPACITY);
  }

  /**
   * 仅供测试：LFU 层 + 指定的底层 LRU 实例。
   * <p>
   * {@link LfuKVCache} 是抽象基类，命名类 {@code LfuLruKVCache} 的底层存储由类名确定、
   * 不接受 delegate；而"内层 LRU 自行淘汰留下残留计数"这个场景需要刻意把两层的容量
   * 配成不一致，因此继承基类自己传 delegate —— 这正是那个受保护的扩展点。
   */
  private static class LfuOverLru<K, V> extends LfuKVCache<K, V> {
    private LfuOverLru(int capacity, LruKVCache<K, V> kvCacheDelegate) {
      super(capacity, kvCacheDelegate);
    }
  }

  /**
   * 仅供测试：暴露 LRU 的队列长度（本层账本）。
   * <p>
   * {@code size()} 已改为取数据层，要看"队列里还留着多少残留"就得直接读队列 ——
   * {@code getLruDeque()} 是 protected，只能从子类里取。
   */
  private static class LruWithDequeVisible<K, V> extends LruHashMapKVCache<K, V> {
    private LruWithDequeVisible(int capacity) {
      super(capacity);
    }

    private long dequeSize() {
      return getLruDeque().size();
    }
  }

}
