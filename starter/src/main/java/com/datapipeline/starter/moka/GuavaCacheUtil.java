package com.datapipeline.starter.moka;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class GuavaCacheUtil {

  private static CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
      .maximumSize(2000).initialCapacity(10);
  private static Cache<String, Object> cache;

  static {
    cache = cacheBuilder.build();
  }

  private GuavaCacheUtil() {
  }

  public static void set(String key, Object value) {
    cache.put(key, value);
  }

  public static void del(String key) {
    cache.invalidate(key);
  }

  public static Object get(String key) {
    return cache.getIfPresent(key);
  }

}
