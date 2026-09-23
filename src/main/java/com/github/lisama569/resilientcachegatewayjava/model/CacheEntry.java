package com.github.lisama569.resilientcachegatewayjava.model;

/**
 * 单个目标的缓存条目，不可变；刷新时整体替换。
 */
public final class CacheEntry {
    private final String value;
    private final long createdAt;

    public CacheEntry(String value, long createdAt) {
        this.value = value;
        this.createdAt = createdAt;
    }

    public String value() { return value; }
    public long createdAt() { return createdAt; }

    /** 是否仍在新鲜有效期内。 */
    public boolean isFresh(long now, long ttlMillis) {
        return now - createdAt < ttlMillis;
    }

    /** 是否已超过 staleTtl（过期兜底也不可用）。 */
    public boolean isExpiredBeyondStale(long now, long ttlMillis, long staleTtlMillis) {
        return now - createdAt >= ttlMillis + staleTtlMillis;
    }

    /** 是否已过期但仍在 staleTtl 兜底窗口内。 */
    public boolean isStaleUsable(long now, long ttlMillis, long staleTtlMillis) {
        long age = now - createdAt;
        return age >= ttlMillis && age < ttlMillis + staleTtlMillis;
    }
}
