package cn.addenda.component.cache.test;

import lombok.ToString;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class TestDelayQueue {

  @ToString
  static class DelayedElement implements Delayed {
    private final long delayTime; // 延迟时间，单位为毫秒
    private final long expire; // 过期时间，即当前时间加上延迟时间
    private final String data; // 元素数据

    public DelayedElement(long delay, String data) {
      this.delayTime = delay;
      this.data = data;
      this.expire = System.currentTimeMillis() + delay;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(this.expire - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      if (this.getDelay(TimeUnit.MILLISECONDS) < o.getDelay(TimeUnit.MILLISECONDS)) {
        return -1;
      } else if (this.getDelay(TimeUnit.MILLISECONDS) > o.getDelay(TimeUnit.MILLISECONDS)) {
        return 1;
      } else {
        return 0;
      }
    }


  }

  public static void main(String[] args) throws InterruptedException {
    DelayQueue<DelayedElement> delayQueue = new DelayQueue<>();

    // 添加一些元素到DelayQueue
    delayQueue.put(new DelayedElement(1000, "Element1"));
    delayQueue.put(new DelayedElement(500, "Element2"));

    // 等待一段时间让元素到期（仅演示，实际应用中可能不需要这一步）
    Thread.sleep(600);

    // 使用toArray将队列转换为DelayedElement[]数组
    DelayedElement[] elementsArray = delayQueue.toArray(new DelayedElement[]{});

    Assert.assertEquals(500L, elementsArray[0].delayTime);
    Assert.assertEquals(1000L, elementsArray[1].delayTime);

    List<DelayedElement> result = new ArrayList<>();
    // 打印转换后的数组
    for (DelayedElement element : elementsArray) {
      // 每take的都能拿出来
      System.out.println(element);
      result.add(element);
    }
  }

}
