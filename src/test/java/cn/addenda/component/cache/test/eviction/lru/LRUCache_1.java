package cn.addenda.component.cache.test.eviction.lru;

import cn.addenda.component.cache.KVCache;
import cn.addenda.component.cache.eviction.lru.LruHashMapKVCache;

/**
 * 通过leetCode测试
 */
public class LRUCache_1 {

  private int capacity;

  KVCache<Integer, Integer> kvCache;

  public LRUCache_1(int capacity) {
    this.capacity = capacity;
    kvCache = new LruHashMapKVCache<>(capacity);
  }

  public int get(int key) {
    Integer a = kvCache.get(key);
    return a == null ? -1 : a;
  }

  public void put(int key, int value) {
    kvCache.set(key, value);
  }

}

