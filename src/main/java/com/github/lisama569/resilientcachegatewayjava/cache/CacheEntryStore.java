package com.github.lisama569.resilientcachegatewayjava.cache;

import com.github.lisama569.resilientcachegatewayjava.model.CacheEntry;

import java.util.Optional;

/**
 * 按目标存储缓存条目。
 */
public interface CacheEntryStore {
    void put(String target, CacheEntry entry);
    Optional<CacheEntry> get(String target);
    void remove(String target);
    int size();
    void clear();
}
