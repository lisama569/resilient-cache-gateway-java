package com.github.lisama569.resilientcachegatewayjava.tests;

import com.github.lisama569.resilientcachegatewayjava.gateway.CacheGatewayService;
import com.github.lisama569.resilientcachegatewayjava.gateway.CircuitBreaker;
import com.github.lisama569.resilientcachegatewayjava.gateway.GatewayMetrics;
import com.github.lisama569.resilientcachegatewayjava.gateway.GatewayProperties;
import com.github.lisama569.resilientcachegatewayjava.gateway.GatewayResponse;
import com.github.lisama569.resilientcachegatewayjava.gateway.GlobalAdmissionGuard;
import com.github.lisama569.resilientcachegatewayjava.gateway.ResultOrigin;
import com.github.lisama569.resilientcachegatewayjava.gateway.SimulatedUpstream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 熔断保护测试：连续失败进入保护期、保护期内不访问上游、
 * 保护期后有限试探、试探成功恢复 / 失败再保护、并发下试探名额有限。
 */
class CircuitBreakerTest {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerTest.class);

    private GatewayProperties properties;
    private SimulatedUpstream upstream;
    private GatewayMetrics metrics;
    private CircuitBreaker circuitBreaker;
    private CacheGatewayService service;

    @BeforeEach
    void setUp() {
        properties = new GatewayProperties();
        properties.setCacheTtlMillis(200);
        properties.setStaleGraceMillis(0); // 聚焦熔断, 关闭宽限窗口
        properties.setUpstreamTimeoutMillis(500);
        properties.setMaxConcurrentPerKey(32);
        properties.setSimulatedLatencyMillis(20);
        properties.setSimulatedFailure(false);
        properties.setCircuitFailureThreshold(3);
        properties.setCircuitOpenMillis(500);
        properties.setCircuitProbeCount(1);
        upstream = new SimulatedUpstream(properties);
        metrics = new GatewayMetrics();
        circuitBreaker = new CircuitBreaker(properties);
        service = new CacheGatewayService(properties, upstream, metrics,
                circuitBreaker, new GlobalAdmissionGuard(properties));
        log.info("[setup] failureThreshold={} openMs={} probeCount={}",
                properties.getCircuitFailureThreshold(), properties.getCircuitOpenMillis(),
                properties.getCircuitProbeCount());
    }

    @Test
    void openThenHalfOpenProbeSuccessCloses() {
        log.info("[case] 保护期打开+试探恢复: 判定依据=3次失败后OPEN且不再打上游, 保护期后试探成功恢复CLOSED");
        properties.setSimulatedFailure(true);
        for (int i = 1; i <= 3; i++) {
            GatewayResponse r = service.get("cb-key");
            assertEquals(ResultOrigin.DEGRADED, r.origin());
            log.info("[result] failure #{} circuitState={}", i, circuitBreaker.state());
        }
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.state(), "连续失败达阈值必须进入保护期");

        long callsBefore = upstream.getCallCount();
        GatewayResponse suppressed = service.get("cb-key");
        assertEquals(ResultOrigin.DEGRADED, suppressed.origin(), "保护期内直接走兜底");
        assertEquals(callsBefore, upstream.getCallCount(), "保护期内不得再访问上游");
        assertTrue(((Number) metrics.snapshot().get("circuitRejected")).longValue() >= 1);
        log.info("[result] open period: upstreamCalls={} circuitRejected={}",
                upstream.getCallCount(), metrics.snapshot().get("circuitRejected"));

        // 上游恢复, 保护期结束后试探
        properties.setSimulatedFailure(false);
        sleep(properties.getCircuitOpenMillis() + 100);
        GatewayResponse probe = service.get("cb-key");
        log.info("[result] probe: origin={} circuitState={}", probe.origin(), circuitBreaker.state());
        assertEquals(ResultOrigin.FRESH, probe.origin(), "试探成功必须恢复正常读取");
        assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.state(), "试探成功后必须恢复CLOSED");
    }

    @Test
    void failedProbeReopensCircuit() {
        log.info("[case] 试探失败再保护: 判定依据=试探失败后回到OPEN, 继续不打上游");
        properties.setSimulatedFailure(true);
        for (int i = 0; i < 3; i++) {
            service.get("cb-key2");
        }
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.state());

        sleep(properties.getCircuitOpenMillis() + 100);
        GatewayResponse probe = service.get("cb-key2"); // 试探, 仍故障
        assertEquals(ResultOrigin.DEGRADED, probe.origin());
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.state(), "试探失败必须重新进入保护期");
        log.info("[result] probe failed, circuitState={}", circuitBreaker.state());

        long callsBefore = upstream.getCallCount();
        service.get("cb-key2");
        assertEquals(callsBefore, upstream.getCallCount(), "再保护期内不得访问上游");

        // 上游恢复后完整走出保护
        properties.setSimulatedFailure(false);
        sleep(properties.getCircuitOpenMillis() + 100);
        GatewayResponse recovered = service.get("cb-key2");
        assertEquals(ResultOrigin.FRESH, recovered.origin());
        assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.state());
        log.info("[result] recovered: origin={} circuitState={}", recovered.origin(), circuitBreaker.state());
    }

    @Test
    void halfOpenAdmitsOnlyLimitedProbesUnderConcurrency() throws Exception {
        int threads = 8;
        log.info("[case] 并发试探名额: threads={} probeCount={} 判定依据=保护期后并发请求只放行1个试探(上游调用只+1), 其余走兜底不丢",
                threads, properties.getCircuitProbeCount());
        properties.setSimulatedFailure(true);
        for (int i = 0; i < 3; i++) {
            service.get("probe-key");
        }
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.state());
        sleep(properties.getCircuitOpenMillis() + 100);

        long callsBefore = upstream.getCallCount();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger degraded = new AtomicInteger();
        List<Future<GatewayResponse>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                gate.await();
                GatewayResponse r = service.get("probe-key-" + idx); // 不同key, 避免合并
                if (r.origin() == ResultOrigin.DEGRADED) {
                    degraded.incrementAndGet();
                }
                return r;
            }));
        }
        gate.countDown();
        for (Future<GatewayResponse> f : futures) {
            f.get(10, TimeUnit.SECONDS); // 每个请求都必须有确定结果
        }
        pool.shutdown();

        long probeCalls = upstream.getCallCount() - callsBefore;
        log.info("[result] probeCalls={} degraded={} circuitState={}",
                probeCalls, degraded.get(), circuitBreaker.state());
        assertEquals(1, probeCalls, "并发下同一时刻只能放行probeCount个试探请求");
        assertEquals(threads, degraded.get(), "试探失败下全部请求走兜底, 但都拿到确定结果");
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.state(), "试探失败后继续保持保护");
    }

    @Test
    void circuitBreakerStateMachineUnit() {
        log.info("[case] 状态机单元: threshold=1 probeCount=2 判定依据=CLOSED→OPEN→HALF_OPEN名额受限→全成功才CLOSED");
        properties.setCircuitFailureThreshold(1);
        properties.setCircuitProbeCount(2);
        properties.setCircuitOpenMillis(50);
        CircuitBreaker cb = new CircuitBreaker(properties);

        assertTrue(cb.tryAcquire(), "CLOSED 正常放行");
        cb.onFailure();
        assertEquals(CircuitBreaker.State.OPEN, cb.state(), "失败达阈值进入OPEN");
        assertFalse(cb.tryAcquire(), "OPEN 保护期内不放行");

        sleep(80);
        assertTrue(cb.tryAcquire(), "保护期后进入HALF_OPEN并放行第1个试探");
        assertTrue(cb.tryAcquire(), "放行第2个试探");
        assertFalse(cb.tryAcquire(), "试探名额已满, 不得再放行");
        cb.onSuccess();
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.state(), "仍有在途试探, 保持HALF_OPEN");
        cb.onSuccess();
        assertEquals(CircuitBreaker.State.CLOSED, cb.state(), "全部试探成功才恢复CLOSED");
        log.info("[result] state machine transitions verified");
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
