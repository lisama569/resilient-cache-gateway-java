package com.github.lisama569.resilientcachegatewayjava.gateway;

import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * 网关指标：请求量、各来源命中计数、拒绝计数、上游调用计数与响应耗时分位。
 * 通过 {@link #snapshot()} 导出为 Map，便于 REST 输出或测试断言。
 */
@Component
public class GatewayMetrics {

    /** 延迟样本环形缓冲容量，防止内存无限增长。 */
    private static final int LATENCY_WINDOW = 10_000;

    private final LongAdder totalRequests = new LongAdder();
    private final LongAdder freshHits = new LongAdder();
    private final LongAdder staleRefreshing = new LongAdder();
    private final LongAdder staleFallbacks = new LongAdder();
    private final LongAdder degraded = new LongAdder();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder upstreamCalls = new LongAdder();
    private final LongAdder upstreamFailures = new LongAdder();
    private final LongAdder coalesced = new LongAdder();
    private final LongAdder backgroundRefreshes = new LongAdder();
    private final LongAdder backgroundRefreshSuccesses = new LongAdder();
    private final LongAdder backgroundRefreshFailures = new LongAdder();
    private final LongAdder circuitRejected = new LongAdder();
    private final LongAdder queueTimeoutRejected = new LongAdder();
    private final LongAdder queueFullRejected = new LongAdder();

    private final Object latencyLock = new Object();
    private final Deque<Long> latencies = new ArrayDeque<>();

    public void recordTotal() {
        totalRequests.increment();
    }

    public void recordFresh() {
        freshHits.increment();
    }

    public void recordStaleFallback() {
        staleFallbacks.increment();
    }

    public void recordDegraded() {
        degraded.increment();
    }

    public void recordRejected() {
        rejected.increment();
    }

    public void recordUpstreamCall() {
        upstreamCalls.increment();
    }

    public void recordUpstreamFailure() {
        upstreamFailures.increment();
    }

    /** 记录一次因并发合并而共享结果的请求。 */
    public void recordCoalesced() {
        coalesced.increment();
    }

    /** 记录一次宽限窗口内返回旧值（后台更新中）。 */
    public void recordStaleRefreshing() {
        staleRefreshing.increment();
    }

    /** 记录一次后台更新被触发。 */
    public void recordBackgroundRefresh() {
        backgroundRefreshes.increment();
    }

    /** 记录一次后台更新成功。 */
    public void recordBackgroundRefreshSuccess() {
        backgroundRefreshSuccesses.increment();
    }

    /** 记录一次后台更新失败。 */
    public void recordBackgroundRefreshFailure() {
        backgroundRefreshFailures.increment();
    }

    /** 记录一次因熔断保护而未访问上游。 */
    public void recordCircuitRejected() {
        circuitRejected.increment();
    }

    /** 记录一次全局排队等待超时拒绝。 */
    public void recordQueueTimeoutRejected() {
        queueTimeoutRejected.increment();
    }

    /** 记录一次全局队列已满拒绝。 */
    public void recordQueueFullRejected() {
        queueFullRejected.increment();
    }

    public void recordLatency(long elapsedMillis) {
        synchronized (latencyLock) {
            if (latencies.size() >= LATENCY_WINDOW) {
                latencies.pollFirst();
            }
            latencies.addLast(elapsedMillis);
        }
    }

    /** 导出当前指标快照。 */
    public Map<String, Object> snapshot() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("totalRequests", totalRequests.sum());
        map.put("freshHits", freshHits.sum());
        map.put("staleRefreshing", staleRefreshing.sum());
        map.put("staleFallbacks", staleFallbacks.sum());
        map.put("degraded", degraded.sum());
        map.put("rejected", rejected.sum());
        map.put("coalesced", coalesced.sum());
        map.put("upstreamCalls", upstreamCalls.sum());
        map.put("upstreamFailures", upstreamFailures.sum());
        map.put("backgroundRefreshes", backgroundRefreshes.sum());
        map.put("backgroundRefreshSuccesses", backgroundRefreshSuccesses.sum());
        map.put("backgroundRefreshFailures", backgroundRefreshFailures.sum());
        map.put("circuitRejected", circuitRejected.sum());
        map.put("queueTimeoutRejected", queueTimeoutRejected.sum());
        map.put("queueFullRejected", queueFullRejected.sum());
        map.put("latency", latencyPercentiles());
        return map;
    }

    private Map<String, Object> latencyPercentiles() {
        List<Long> copy;
        synchronized (latencyLock) {
            copy = new ArrayList<>(latencies);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("count", copy.size());
        if (copy.isEmpty()) {
            result.put("p50", 0);
            result.put("p95", 0);
            result.put("p99", 0);
            result.put("max", 0);
            return result;
        }
        Collections.sort(copy);
        result.put("p50", percentile(copy, 0.50));
        result.put("p95", percentile(copy, 0.95));
        result.put("p99", percentile(copy, 0.99));
        result.put("max", copy.get(copy.size() - 1));
        return result;
    }

    private static long percentile(List<Long> sorted, double p) {
        int idx = (int) Math.ceil(p * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(idx, sorted.size() - 1)));
    }
}
