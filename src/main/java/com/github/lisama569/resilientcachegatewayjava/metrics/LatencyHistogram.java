package com.github.lisama569.resilientcachegatewayjava.metrics;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * 固定毫秒桶的耗时直方图，支持 p50/p90/p99 与最大值。
 * 桶上界（毫秒）：1,2,5,10,20,50,100,200,500,1000,2000,5000，
 * 超过 5000ms 计入 +Inf 桶。分位数在导出时基于累计桶计数线性计算，
 * 无锁、无采样、开销恒定。
 */
public class LatencyHistogram {

    static final long[] BOUNDS_MILLIS = {1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000};

    private final AtomicLongArray buckets;
    private final AtomicLong count = new AtomicLong();
    private final AtomicLong maxNanos = new AtomicLong();

    public LatencyHistogram() {
        this.buckets = new AtomicLongArray(BOUNDS_MILLIS.length + 1);
    }

    public void record(long elapsedNanos) {
        long elapsedMillis = Math.max(1L, elapsedNanos / 1_000_000L);
        int idx = bucketIndexOf(elapsedMillis);
        buckets.incrementAndGet(idx);
        count.incrementAndGet();
        maxNanos.accumulateAndGet(elapsedNanos, Math::max);
    }

    private static int bucketIndexOf(long millis) {
        for (int i = 0; i < BOUNDS_MILLIS.length; i++) {
            if (millis <= BOUNDS_MILLIS[i]) {
                return i;
            }
        }
        return BOUNDS_MILLIS.length; // +Inf
    }

    public long count() {
        return count.get();
    }

    /**
     * 导出快照：count、maxMillis、p50/p90/p99 及每个桶的累计计数。
     */
    public Map<String, Object> snapshot() {
        long total = count.get();
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("count", total);
        snap.put("maxMillis", total == 0 ? 0 : maxNanos.get() / 1_000_000L);
        snap.put("p50Millis", quantile(0.50, total));
        snap.put("p90Millis", quantile(0.90, total));
        snap.put("p99Millis", quantile(0.99, total));

        List<Map<String, Object>> bucketList = new ArrayList<>();
        long cumulative = 0;
        for (int i = 0; i < BOUNDS_MILLIS.length; i++) {
            cumulative += buckets.get(i);
            long le = BOUNDS_MILLIS[i];
            bucketList.add(Map.of("leMillis", le, "cumulativeCount", cumulative));
        }
        cumulative += buckets.get(BOUNDS_MILLIS.length);
        bucketList.add(Map.of("leMillis", "+Inf", "cumulativeCount", cumulative));
        snap.put("buckets", bucketList);
        return snap;
    }

    private long quantile(double q, long total) {
        if (total == 0) {
            return 0L;
        }
        long rank = Math.min(total, Math.max(1L, (long) Math.ceil(q * total)));
        long cumulative = 0;
        for (int i = 0; i < buckets.length(); i++) {
            cumulative += buckets.get(i);
            if (cumulative >= rank) {
                return i < BOUNDS_MILLIS.length ? BOUNDS_MILLIS[i] : BOUNDS_MILLIS[BOUNDS_MILLIS.length - 1];
            }
        }
        return BOUNDS_MILLIS[BOUNDS_MILLIS.length - 1];
    }
}
