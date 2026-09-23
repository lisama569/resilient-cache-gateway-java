package com.github.lisama569.resilientcachegatewayjava.tests;

import com.github.lisama569.resilientcachegatewayjava.metrics.MetricsRegistry;
import com.github.lisama569.resilientcachegatewayjava.model.RejectReason;
import com.github.lisama569.resilientcachegatewayjava.model.ResultStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 指标导出测试：请求计数、状态/拒绝维度计数、耗时分位、上游计数、重置。
 */
class MetricsRegistryTest {

    private static final Logger log = LoggerFactory.getLogger(MetricsRegistryTest.class);

    @Test
    @SuppressWarnings("unchecked")
    void recordsCountersAndLatencyQuantilesAndResets() {
        MetricsRegistry metrics = new MetricsRegistry();

        // 100 次请求：90 次 FRESH，10 次 STALE_FALLBACK，耗时均约 5ms
        for (int i = 0; i < 90; i++) {
            metrics.recordRequest("a", ResultStatus.FRESH, 5_000_000L);
        }
        for (int i = 0; i < 10; i++) {
            metrics.recordRequest("a", ResultStatus.STALE_FALLBACK, 50_000_000L);
        }
        metrics.recordUpstreamCall();
        metrics.recordUpstreamCall();
        metrics.recordUpstreamError();
        metrics.recordUpstreamTimeout();
        metrics.recordRejected("a", RejectReason.PER_KEY_OVERLOADED);

        Map<String, Object> snap = metrics.snapshot();
        log.info("[metrics-test] 输入: 90 FRESH(5ms) + 10 STALE(50ms), 导出快照={}", snap);

        assertThat(snap.get("totalRequests")).isEqualTo(100L);
        assertThat(snap.get("upstreamCalls")).isEqualTo(2L);
        assertThat(snap.get("upstreamErrors")).isEqualTo(1L);
        assertThat(snap.get("upstreamTimeouts")).isEqualTo(1L);

        Map<String, Long> status = (Map<String, Long>) snap.get("statusCounts");
        assertThat(status.get("status=FRESH")).isEqualTo(90L);
        assertThat(status.get("status=STALE_FALLBACK")).isEqualTo(10L);
        assertThat(status.get("target=a|status=FRESH")).isEqualTo(90L);

        Map<String, Long> rejects = (Map<String, Long>) snap.get("rejectCounts");
        assertThat(rejects.get("reason=PER_KEY_OVERLOADED")).isEqualTo(1L);

        Map<String, Object> latency = (Map<String, Object>) snap.get("latency");
        log.info("[metrics-test] 判定依据: 100 个样本中 90 个 5ms, 期望 p50<=10ms 且 p90<=50ms, latency={}",
                latency);
        assertThat(latency.get("count")).isEqualTo(100L);
        assertThat((long) latency.get("p50Millis")).isLessThanOrEqualTo(10L);
        assertThat((long) latency.get("p90Millis")).isLessThanOrEqualTo(50L);
        assertThat((long) latency.get("p99Millis")).isLessThanOrEqualTo(50L);
        assertThat((long) latency.get("maxMillis")).isBetween(50L, 50L);

        metrics.reset();
        assertThat(metrics.totalRequests()).isZero();
        assertThat(metrics.upstreamCalls()).isZero();
        log.info("[metrics-test] reset 后 totalRequests={}, upstreamCalls={}",
                metrics.totalRequests(), metrics.upstreamCalls());
    }

    @Test
    void degradedAndRejectionReasonsAreSeparatelyCounted() {
        MetricsRegistry metrics = new MetricsRegistry();
        metrics.recordRequest("b", ResultStatus.DEGRADED, 1_000_000L);
        metrics.recordRejected("b", RejectReason.CIRCUIT_OPEN);
        metrics.recordRejected("b", RejectReason.QUEUE_TIMEOUT);
        metrics.recordRejected("c", RejectReason.GLOBAL_OVERLOADED);

        @SuppressWarnings("unchecked")
        Map<String, Long> rejects = (Map<String, Long>) metrics.snapshot().get("rejectCounts");
        log.info("[metrics-test] 拒绝原因维度计数={}", rejects);
        assertThat(rejects.get("reason=CIRCUIT_OPEN")).isEqualTo(1L);
        assertThat(rejects.get("reason=QUEUE_TIMEOUT")).isEqualTo(1L);
        assertThat(rejects.get("reason=GLOBAL_OVERLOADED")).isEqualTo(1L);
    }
}
