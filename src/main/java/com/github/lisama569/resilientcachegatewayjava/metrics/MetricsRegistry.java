package com.github.lisama569.resilientcachegatewayjava.metrics;

import com.github.lisama569.resilientcachegatewayjava.model.RejectReason;
import com.github.lisama569.resilientcachegatewayjava.model.ResultStatus;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 轻量级进程内指标（无第三方依赖，通过 /admin/metrics/export 导出）：
 *  - 请求总量、按 target 请求量；
 *  - 按结果类别（FRESH/STALE_FALLBACK/DEGRADED）计数，含每 target 维度；
 *  - 拒绝计数，按 RejectReason 与 target 区分；
 *  - 上游调用数、错误数、超时数；
 *  - 全局与每 target 的响应耗时分位（p50/p90/p99/max）。
 */
public class MetricsRegistry {

    private final AtomicLong totalRequests = new AtomicLong();
    private final ConcurrentHashMap<String, AtomicLong> requestsByTarget = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> statusCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> rejectCounts = new ConcurrentHashMap<>();
    private final AtomicLong upstreamCalls = new AtomicLong();
    private final AtomicLong upstreamErrors = new AtomicLong();
    private final AtomicLong upstreamTimeouts = new AtomicLong();
    private final LatencyHistogram globalLatency = new LatencyHistogram();
    private final ConcurrentHashMap<String, LatencyHistogram> latencyByTarget = new ConcurrentHashMap<>();

    public void recordRequest(String target, ResultStatus status, long elapsedNanos) {
        totalRequests.incrementAndGet();
        inc(requestsByTarget, target);
        inc(statusCounts, "status=" + status.name());
        inc(statusCounts, "target=" + target + "|status=" + status.name());
        globalLatency.record(elapsedNanos);
        latencyByTarget.computeIfAbsent(target, t -> new LatencyHistogram()).record(elapsedNanos);
    }

    public void recordRejected(String target, RejectReason reason) {
        inc(rejectCounts, "reason=" + reason.name());
        inc(rejectCounts, "target=" + target + "|reason=" + reason.name());
    }

    public void recordUpstreamCall() {
        upstreamCalls.incrementAndGet();
    }

    public void recordUpstreamError() {
        upstreamErrors.incrementAndGet();
    }

    public void recordUpstreamTimeout() {
        upstreamTimeouts.incrementAndGet();
    }

    public long totalRequests() {
        return totalRequests.get();
    }

    public long upstreamCalls() {
        return upstreamCalls.get();
    }

    /** 导出可 JSON 序列化的完整快照。 */
    public Map<String, Object> snapshot() {
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("totalRequests", totalRequests.get());
        snap.put("upstreamCalls", upstreamCalls.get());
        snap.put("upstreamErrors", upstreamErrors.get());
        snap.put("upstreamTimeouts", upstreamTimeouts.get());
        snap.put("requestsByTarget", snapshotOf(requestsByTarget));
        snap.put("statusCounts", snapshotOf(statusCounts));
        snap.put("rejectCounts", snapshotOf(rejectCounts));
        snap.put("latency", globalLatency.snapshot());
        Map<String, Object> byTarget = new LinkedHashMap<>();
        latencyByTarget.forEach((t, h) -> byTarget.put(t, h.snapshot()));
        snap.put("latencyByTarget", byTarget);
        return snap;
    }

    public void reset() {
        totalRequests.set(0L);
        upstreamCalls.set(0L);
        upstreamErrors.set(0L);
        upstreamTimeouts.set(0L);
        requestsByTarget.clear();
        statusCounts.clear();
        rejectCounts.clear();
        latencyByTarget.clear();
    }

    private static void inc(ConcurrentHashMap<String, AtomicLong> map, String key) {
        map.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
    }

    private static Map<String, Long> snapshotOf(ConcurrentHashMap<String, AtomicLong> map) {
        Map<String, Long> out = new LinkedHashMap<>();
        map.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }
}
