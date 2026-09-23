package com.github.lisama569.resilientcachegatewayjava.cache;

import com.github.lisama569.resilientcachegatewayjava.model.CacheEntry;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于 ConcurrentHashMap 的内存缓存。过期条目的判定在服务层完成，
 * 这里只负责整体替换式写入，保证读到的永远是完整条目。
 */
public class InMemoryCacheEntryStore implements CacheEntryStore {

    private final ConcurrentHashMap<String, CacheEntry> entries = new ConcurrentHashMap<>();

    @Override public void put(String target, CacheEntry entry) {
        entries.put(target, entry);
    }

    @Override public Optional<CacheEntry> get(String target) {
        return Optional.ofNullable(entries.get(target));
    }

    @Override public void remove(String target) {
        entries.remove(target);
    }

    @Override public int size() {
        return entries.size();
    }

    @Override public void clear() {
        entries.clear();
    }
}
