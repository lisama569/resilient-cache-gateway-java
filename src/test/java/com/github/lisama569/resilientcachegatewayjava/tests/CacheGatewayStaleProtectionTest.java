package com.github.lisama569.resilientcachegatewayjava.tests;

import com.github.lisama569.resilientcachegatewayjava.gateway.CacheGatewayService;
import com.github.lisama569.resilientcachegatewayjava.gateway.GatewayMetrics;
import com.github.lisama569.resilientcachegatewayjava.gateway.GatewayProperties;
import com.github.lisama569.resilientcachegatewayjava.gateway.GatewayResponse;
import com.github.lisama569.resilientcachegatewayjava.gateway.GlobalAdmissionLimiter;
import com.github.lisama569.resilientcachegatewayjava.gateway.ResultOrigin;
import com.github.lisama569.resilientcachegatewayjava.gateway.SimulatedUpstream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 旧结果与保护状态交互的回归测试（本轮 bugfix）：
 * <ul>
 *   <li>宽限窗口内的后台更新被容量占满/保护期拦回时，旧结果必须照常返回，且不计入连续失败。</li>
 *   <li>整体容量占满时，缓存命中与还有旧结果的请求照旧拿到旧值，只有完全没旧结果的请求
 *       才按“排队没等到”/“总量超限”两种可区分的拒绝处理。</li>
 *   <li>试探名额在“许可拿到但没真正打到上游”时必须归还，熔断器不能卡在试探期出不来。</li>
 * </ul>
 * 每个用例打印所用配置与判定依据。
 */
class CacheGatewayStaleProtectionTest {

    private static final Logger log = LoggerFactory.getLogger(CacheGatewayStaleProtectionTest.class);

    private GatewayProperties properties;
    private SimulatedUpstream upstream;
    private GatewayMetrics metrics;
    private CacheGatewayService service;

    @BeforeEach
    void setUp() {
        properties = new GatewayProperties();
        properties.setCacheTtlMillis(200);
        properties.setUpstreamTimeoutMillis(3000);
        properties.setMaxConcurrentPerKey(32);
        properties.setSimulatedLatencyMillis(20);
        properties.setSimulatedFailure(false);
        upstream = new SimulatedUpstream(properties);
        metrics = new GatewayMetrics();
        service = new CacheGatewayService(properties, upstream, metrics);
    }

    // ---------- 宽限窗口 × 容量占满 ----------

    @Test
    void graceWindowStaleSurvivesCapacitySaturation() throws Exception {
        properties.setCacheTtlMillis(200);
        properties.setSwrGraceMillis(10_000);
        properties.setSwrMaxBackgroundFailures(2);
        properties.setMaxConcurrentUpstream(1);
        properties.setMaxQueueSize(0); // 容量一满，任何真实回源立即“总量超限”
        properties.setQueueWaitTimeoutMillis(50);
        int threads = 6;
        log.info("[case] 宽限窗口×容量占满: ttlMs={} graceMs={} maxBgFailures={} maxConcurrentUpstream={} "
                        + "maxQueueSize={} 判定依据=容量占满期间窗口内请求始终STALE_REFRESHING(旧值), "
                        + "后台更新被拦回记skipped不计failure, 拦回次数超过上限{}也不落回同步回源",
                properties.getCacheTtlMillis(), properties.getSwrGraceMillis(),
                properties.getSwrMaxBackgroundFailures(), properties.getMaxConcurrentUpstream(),
                properties.getMaxQueueSize(), properties.getSwrMaxBackgroundFailures());

        GatewayResponse fresh = service.get("stale-key");
        assertEquals(ResultOrigin.FRESH, fresh.origin());
        Thread.sleep(300); // 进入宽限窗口

        // 慢请求占住唯一全局许可（上游健康但慢，1500ms 后自然释放）
        properties.setSimulatedLatencyMillis(1500);
        ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
        Future<GatewayResponse> holder = pool.submit(() -> service.get("holder-key"));
        waitFor(() -> admissionInFlight() == 1, 2000);

        // 容量占满期间：连发 4 轮并发请求（每轮 6 个），拦回次数 4 > 上限 2
        for (int round = 1; round <= 4; round++) {
            CountDownLatch gate = new CountDownLatch(1);
            List<Future<GatewayResponse>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    gate.await();
                    return service.get("stale-key");
                }));
            }
            gate.countDown();
            for (Future<GatewayResponse> f : futures) {
                GatewayResponse r = f.get(5, TimeUnit.SECONDS);
                assertEquals(ResultOrigin.STALE_REFRESHING, r.origin(),
                        "容量占满不得把宽限窗口内的旧结果变成报错");
                assertEquals(fresh.value(), r.value(), "必须返回上一份旧结果");
            }
            int expectedSkipped = round;
            waitFor(() -> skippedRefreshes() >= expectedSkipped, 2000);
            log.info("[result] round={} 全部{}个并发请求=STALE_REFRESHING(旧值), skipped={} failures={}",
                    round, threads, skippedRefreshes(), failedRefreshes());
        }

        Map<String, Object> snap = metrics.snapshot();
        log.info("[result] backgroundRefreshes={} skipped={} failures={} upstreamCalls={}",
                snap.get("backgroundRefreshes"), snap.get("backgroundRefreshSkipped"),
                snap.get("backgroundRefreshFailures"), upstream.getCallCount());
        assertEquals(0, failedRefreshes(), "没真正发到上游的后台更新不得计入连续失败");
        assertTrue(skippedRefreshes() > properties.getSwrMaxBackgroundFailures(),
                "拦回次数应超过失败上限，用于证明旧结果不会因此失效");

        // 容量释放后：后台更新真正发到上游并成功，立刻看到新值
        properties.setSimulatedLatencyMillis(20);
        holder.get(5, TimeUnit.SECONDS);
        waitFor(() -> admissionInFlight() == 0, 2000);
        waitFor(() -> service.get("stale-key").origin() == ResultOrigin.FRESH, 3000);
        GatewayResponse refreshed = service.get("stale-key");
        log.info("[result] 容量释放后: origin={} value={} (期望FRESH新值)", refreshed.origin(), refreshed.value());
        assertEquals(ResultOrigin.FRESH, refreshed.origin());
        assertNotEquals(fresh.value(), refreshed.value());
        pool.shutdown();
    }

    // ---------- 宽限窗口 × 熔断保护期 ----------

    @Test
    void graceWindowStaleSurvivesCircuitOpen() throws Exception {
        properties.setCacheTtlMillis(200);
        properties.setSwrGraceMillis(10_000);
        properties.setSwrMaxBackgroundFailures(2);
        properties.setCircuitFailureThreshold(2);
        properties.setCircuitOpenMillis(1500);
        properties.setCircuitHalfOpenProbes(1);
        log.info("[case] 宽限窗口×保护期: ttlMs={} graceMs={} maxBgFailures={} threshold={} openMs={} "
                        + "判定依据=保护期内窗口内请求始终STALE_REFRESHING(旧值)且不打上游, 后台更新被跳过记skipped, "
                        + "熔断恢复后后台更新成功",
                properties.getCacheTtlMillis(), properties.getSwrGraceMillis(),
                properties.getSwrMaxBackgroundFailures(), properties.getCircuitFailureThreshold(),
                properties.getCircuitOpenMillis());

        GatewayResponse fresh = service.get("grace-key");
        assertEquals(ResultOrigin.FRESH, fresh.origin());

        // 用别的 key 把熔断器打进保护期
        properties.setSimulatedFailure(true);
        service.get("cb-1");
        service.get("cb-2");
        assertEquals("OPEN", circuitState());
        long callsAtOpen = upstream.getCallCount();
        log.info("[result] circuit OPEN, upstreamCalls={}", callsAtOpen);

        Thread.sleep(300); // grace-key 进入宽限窗口
        // 保护期内连发 4 次（超过 maxBgFailures=2）：每次都应拿到旧结果
        for (int i = 1; i <= 4; i++) {
            GatewayResponse r = service.get("grace-key");
            assertEquals(ResultOrigin.STALE_REFRESHING, r.origin(),
                    "保护期不得把宽限窗口内的旧结果变成报错或同步回源");
            assertEquals(fresh.value(), r.value());
            int expectedSkipped = i;
            waitFor(() -> skippedRefreshes() >= expectedSkipped, 2000);
        }
        log.info("[result] 保护期内 4 次请求全部 STALE_REFRESHING(旧值), skipped={} failures={} "
                        + "upstreamCalls={}(未增加), circuit={}",
                skippedRefreshes(), failedRefreshes(), upstream.getCallCount(), circuitState());
        assertEquals(0, failedRefreshes(), "保护期跳过的后台更新不得计入连续失败");
        assertEquals(callsAtOpen, upstream.getCallCount(), "保护期内后台更新不得真正打到上游");

        // 上游恢复 + 保护期结束：试探成功 -> CLOSED，随后后台更新成功 -> FRESH 新值
        properties.setSimulatedFailure(false);
        Thread.sleep(1600);
        GatewayResponse probe = service.get("probe-key");
        assertEquals(ResultOrigin.FRESH, probe.origin());
        assertEquals("CLOSED", circuitState());
        waitFor(() -> service.get("grace-key").origin() == ResultOrigin.FRESH, 3000);
        GatewayResponse refreshed = service.get("grace-key");
        log.info("[result] 熔断恢复后: origin={} value={} (期望FRESH新值)", refreshed.origin(), refreshed.value());
        assertEquals(ResultOrigin.FRESH, refreshed.origin());
        assertNotEquals(fresh.value(), refreshed.value());
    }

    // ---------- 容量拒绝 × 旧结果可用 ----------

    @Test
    void capacityExceededFallsBackToStaleWhenCacheExists() throws Exception {
        properties.setCacheTtlMillis(200);
        properties.setSwrGraceMillis(0); // 关闭宽限窗口，只看同步路径
        properties.setMaxConcurrentUpstream(1);
        properties.setMaxQueueSize(0);
        properties.setQueueWaitTimeoutMillis(50);
        log.info("[case] 总量超限×旧结果: ttlMs={} grace=0 maxConcurrentUpstream={} maxQueueSize={} "
                        + "判定依据=有旧结果的请求拿到STALE_FALLBACK, 完全没旧结果的请求抛CapacityExceeded",
                properties.getCacheTtlMillis(), properties.getMaxConcurrentUpstream(),
                properties.getMaxQueueSize());

        GatewayResponse fresh = service.get("old-key");
        assertEquals(ResultOrigin.FRESH, fresh.origin());
        Thread.sleep(400); // 彻底过期（无宽限窗口）

        properties.setSimulatedLatencyMillis(1500);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<GatewayResponse> holder = pool.submit(() -> service.get("holder-key"));
        waitFor(() -> admissionInFlight() == 1, 2000);

        GatewayResponse stale = service.get("old-key");
        log.info("[result] 容量占满+有旧结果: origin={} value={} (期望STALE_FALLBACK旧值, 不抛异常)",
                stale.origin(), stale.value());
        assertEquals(ResultOrigin.STALE_FALLBACK, stale.origin());
        assertEquals(fresh.value(), stale.value());

        GlobalAdmissionLimiter.CapacityExceededException rejected = assertThrows(
                GlobalAdmissionLimiter.CapacityExceededException.class, () -> service.get("no-cache-key"));
        log.info("[result] 容量占满+无旧结果: {} (期望CAPACITY_EXCEEDED拒绝)", rejected.getMessage());

        properties.setSimulatedLatencyMillis(20);
        holder.get(5, TimeUnit.SECONDS);
        pool.shutdown();
    }

    @Test
    void queueWaitTimeoutFallsBackToStaleWhenCacheExists() throws Exception {
        properties.setCacheTtlMillis(200);
        properties.setSwrGraceMillis(0);
        properties.setMaxConcurrentUpstream(1);
        properties.setMaxQueueSize(1);
        properties.setQueueWaitTimeoutMillis(150);
        log.info("[case] 排队超时×旧结果: ttlMs={} grace=0 maxConcurrentUpstream={} maxQueueSize={} "
                        + "queueWaitTimeoutMs={} 判定依据=有旧结果的请求排队没等到后拿到STALE_FALLBACK, "
                        + "完全没旧结果的请求抛QueueWaitTimeout",
                properties.getCacheTtlMillis(), properties.getMaxConcurrentUpstream(),
                properties.getMaxQueueSize(), properties.getQueueWaitTimeoutMillis());

        GatewayResponse fresh = service.get("old-key");
        assertEquals(ResultOrigin.FRESH, fresh.origin());
        Thread.sleep(400); // 彻底过期

        properties.setSimulatedLatencyMillis(1500);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<GatewayResponse> holder = pool.submit(() -> service.get("holder-key"));
        waitFor(() -> admissionInFlight() == 1, 2000);

        GatewayResponse stale = service.get("old-key"); // 排队 150ms 没等到 -> 旧结果兜底
        log.info("[result] 排队没等到+有旧结果: origin={} value={} elapsedMs={} (期望STALE_FALLBACK旧值)",
                stale.origin(), stale.value(), stale.elapsedMillis());
        assertEquals(ResultOrigin.STALE_FALLBACK, stale.origin());
        assertEquals(fresh.value(), stale.value());

        GlobalAdmissionLimiter.QueueWaitTimeoutException rejected = assertThrows(
                GlobalAdmissionLimiter.QueueWaitTimeoutException.class, () -> service.get("no-cache-key"));
        log.info("[result] 排队没等到+无旧结果: {} (期望QUEUE_WAIT_TIMEOUT拒绝)", rejected.getMessage());

        properties.setSimulatedLatencyMillis(20);
        holder.get(5, TimeUnit.SECONDS);
        pool.shutdown();
        Map<String, Object> snap = metrics.snapshot();
        log.info("[result] metrics: queueWaitTimeouts={} capacityExceeded={}",
                snap.get("queueWaitTimeouts"), snap.get("capacityExceeded"));
        assertTrue(((Number) snap.get("queueWaitTimeouts")).longValue() >= 1);
    }

    // ---------- 试探名额不泄漏、状态可观察 ----------

    @Test
    void halfOpenProbeSlotIsReleasedWhenCapacityBlocksProbe() throws Exception {
        properties.setCircuitFailureThreshold(2);
        properties.setCircuitOpenMillis(400);
        properties.setCircuitHalfOpenProbes(1);
        properties.setMaxConcurrentUpstream(1);
        properties.setMaxQueueSize(1);
        properties.setQueueWaitTimeoutMillis(150);
        properties.setSimulatedFailure(true);
        log.info("[case] 试探名额泄漏: threshold={} openMs={} probes={} queueWaitTimeoutMs={} "
                        + "判定依据=试探请求被容量拦回后名额必须归还(halfOpenInFlight回到0), "
                        + "容量恢复后下一个试探能放行并恢复CLOSED, 不能卡在HALF_OPEN",
                properties.getCircuitFailureThreshold(), properties.getCircuitOpenMillis(),
                properties.getCircuitHalfOpenProbes(), properties.getQueueWaitTimeoutMillis());

        service.get("f1");
        service.get("f2");
        assertEquals("OPEN", circuitState());
        log.info("[result] circuit={} openCount={}", circuitState(), circuitOpenCount());

        Thread.sleep(500); // 保护期结束
        // 把全局容量调成 0：试探许可能拿到，但必然排队超时（没真正打到上游）
        properties.setMaxConcurrentUpstream(0);
        properties.setSimulatedFailure(false);

        GlobalAdmissionLimiter.QueueWaitTimeoutException blocked = assertThrows(
                GlobalAdmissionLimiter.QueueWaitTimeoutException.class, () -> service.get("probe-key"));
        log.info("[result] 试探被容量拦回: {} circuit={} halfOpenInFlight={} (期望仍HALF_OPEN且名额已归还)",
                blocked.getMessage(), circuitState(), halfOpenInFlight());
        assertEquals("HALF_OPEN", circuitState(), "没真正打到上游不算试探失败，不应重新OPEN");
        assertEquals(0, halfOpenInFlight(), "试探名额必须归还，否则熔断器卡在HALF_OPEN出不来");

        // 容量恢复：下一个试探必须能真正放行并成功 -> CLOSED
        properties.setMaxConcurrentUpstream(1);
        GatewayResponse recovered = service.get("recover-key");
        log.info("[result] 容量恢复后试探: origin={} circuit={} (期望FRESH+CLOSED)",
                recovered.origin(), circuitState());
        assertEquals(ResultOrigin.FRESH, recovered.origin());
        assertEquals("CLOSED", circuitState());
    }

    // ---------- 工具 ----------

    private String circuitState() {
        return String.valueOf(((Map<?, ?>) metrics.snapshot().get("circuit")).get("state"));
    }

    private long circuitOpenCount() {
        return ((Number) ((Map<?, ?>) metrics.snapshot().get("circuit")).get("openCount")).longValue();
    }

    private int halfOpenInFlight() {
        return ((Number) ((Map<?, ?>) metrics.snapshot().get("circuit")).get("halfOpenInFlight")).intValue();
    }

    private int admissionInFlight() {
        return ((Number) ((Map<?, ?>) metrics.snapshot().get("admission")).get("inFlight")).intValue();
    }

    private long skippedRefreshes() {
        return ((Number) metrics.snapshot().get("backgroundRefreshSkipped")).longValue();
    }

    private long failedRefreshes() {
        return ((Number) metrics.snapshot().get("backgroundRefreshFailures")).longValue();
    }

    private void waitFor(Check check, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (check.pass()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("condition not met within " + timeoutMillis + "ms");
    }

    @FunctionalInterface
    private interface Check {
        boolean pass() throws Exception;
    }
}
