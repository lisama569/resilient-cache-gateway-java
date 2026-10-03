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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 本轮新增能力的关键场景测试：宽限窗口（SWR）、全局准入与有界排队、熔断器。
 * 每个用例打印所用配置与判定依据，便于定位问题。
 */
class CacheGatewayNewFeaturesTest {

    private static final Logger log = LoggerFactory.getLogger(CacheGatewayNewFeaturesTest.class);

    private GatewayProperties properties;
    private SimulatedUpstream upstream;
    private GatewayMetrics metrics;
    private CacheGatewayService service;

    @BeforeEach
    void setUp() {
        properties = new GatewayProperties();
        properties.setCacheTtlMillis(300);
        properties.setUpstreamTimeoutMillis(1000);
        properties.setMaxConcurrentPerKey(32);
        properties.setSimulatedLatencyMillis(20);
        properties.setSimulatedFailure(false);
        upstream = new SimulatedUpstream(properties);
        metrics = new GatewayMetrics();
        service = new CacheGatewayService(properties, upstream, metrics);
    }

    // ---------- 宽限窗口（SWR） ----------

    @Test
    void graceWindowBoundariesAreDeterministic() throws Exception {
        properties.setCacheTtlMillis(300);
        properties.setSwrGraceMillis(600); // 窗口: [300ms, 900ms)
        log.info("[case] 宽限窗口边界: ttlMs={} graceMs={} 判定依据=窗口内STALE_REFRESHING, 窗口外落回同步回源/兜底",
                properties.getCacheTtlMillis(), properties.getSwrGraceMillis());

        GatewayResponse fresh = service.get("swr-key");
        assertEquals(ResultOrigin.FRESH, fresh.origin());

        // 窗口内（age≈400ms, 处于 [300,900)）：旧结果 + 后台更新
        Thread.sleep(400);
        GatewayResponse inGrace = service.get("swr-key");
        log.info("[result] in-grace: origin={} value={} (期望STALE_REFRESHING且为旧值)",
                inGrace.origin(), inGrace.value());
        assertEquals(ResultOrigin.STALE_REFRESHING, inGrace.origin());
        assertEquals(fresh.value(), inGrace.value(), "宽限窗口内必须返回上一份旧结果");

        // 后台更新成功后，后续读取立刻看到新结果
        waitFor(() -> service.get("swr-key").origin() == ResultOrigin.FRESH, 2000);
        GatewayResponse refreshed = service.get("swr-key");
        log.info("[result] after-bg-refresh: origin={} value={} (期望FRESH且为新值)",
                refreshed.origin(), refreshed.value());
        assertEquals(ResultOrigin.FRESH, refreshed.origin());
        assertNotEquals(fresh.value(), refreshed.value(), "后台更新成功后必须立刻可见新结果");

        // 窗口外（age > ttl+grace=900ms）且上游故障：落回现有兜底分类，不再拿旧结果顶
        Thread.sleep(1000); // 等当前条目彻底过期且超出宽限窗口
        properties.setSimulatedFailure(true);
        GatewayResponse beyondGrace = service.get("swr-key");
        log.info("[result] beyond-grace+failure: origin={} (期望STALE_FALLBACK, 不再是STALE_REFRESHING)",
                beyondGrace.origin());
        assertEquals(ResultOrigin.STALE_FALLBACK, beyondGrace.origin());
    }

    @Test
    void concurrentRequestsInGraceTriggerSingleBackgroundRefresh() throws Exception {
        properties.setCacheTtlMillis(200);
        properties.setSwrGraceMillis(5000);
        properties.setSimulatedLatencyMillis(500); // 后台更新较慢，保证并发请求都落在窗口内
        int threads = 8;
        log.info("[case] 并发只触发一次后台更新: threads={} ttlMs={} graceMs={} latencyMs={} "
                        + "判定依据=全部STALE_REFRESHING且上游总调用=2(首次+1次后台)",
                threads, properties.getCacheTtlMillis(), properties.getSwrGraceMillis(),
                properties.getSimulatedLatencyMillis());

        GatewayResponse first = service.get("hot-swr");
        assertEquals(ResultOrigin.FRESH, first.origin());
        Thread.sleep(300); // 进入宽限窗口

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<GatewayResponse>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                gate.await();
                return service.get("hot-swr");
            }));
        }
        gate.countDown();
        List<GatewayResponse> responses = new ArrayList<>();
        for (Future<GatewayResponse> f : futures) {
            responses.add(f.get(5, TimeUnit.SECONDS));
        }
        pool.shutdown();

        for (GatewayResponse r : responses) {
            assertEquals(ResultOrigin.STALE_REFRESHING, r.origin(), "窗口内并发请求都应拿到旧结果");
            assertEquals(first.value(), r.value());
        }
        // 等后台更新完成后再断言上游调用次数
        waitFor(() -> service.get("hot-swr").origin() == ResultOrigin.FRESH, 3000);
        long upstreamCalls = upstream.getCallCount();
        log.info("[result] upstreamCalls={} (期望=2: 首次回源+恰好1次后台更新), staleRefreshing={}",
                upstreamCalls, metrics.snapshot().get("staleRefreshing"));
        assertEquals(2, upstreamCalls, "并发进入窗口只能触发一次后台更新，不得重复回源");
    }

    @Test
    void backgroundRefreshFailuresEventuallyFallBackToStaleFallback() throws Exception {
        properties.setCacheTtlMillis(200);
        properties.setSwrGraceMillis(30000); // 窗口足够长，只看失败次数上限
        properties.setSwrMaxBackgroundFailures(2);
        log.info("[case] 后台更新连续失败降级: maxBgFailures={} 判定依据=失败次数未达上限仍STALE_REFRESHING, "
                + "达到上限后落回STALE_FALLBACK", properties.getSwrMaxBackgroundFailures());

        GatewayResponse fresh = service.get("fragile-key");
        assertEquals(ResultOrigin.FRESH, fresh.origin());
        Thread.sleep(300); // 进入宽限窗口
        properties.setSimulatedFailure(true);

        GatewayResponse attempt1 = service.get("fragile-key"); // 触发后台更新#1（失败）
        assertEquals(ResultOrigin.STALE_REFRESHING, attempt1.origin());
        waitFor(() -> ((Number) metrics.snapshot().get("backgroundRefreshFailures")).longValue() >= 1, 2000);

        GatewayResponse attempt2 = service.get("fragile-key"); // 失败1次 < 上限2, 触发后台更新#2（失败）
        log.info("[result] bgFailures=1: origin={} (期望仍为STALE_REFRESHING)", attempt2.origin());
        assertEquals(ResultOrigin.STALE_REFRESHING, attempt2.origin());
        waitFor(() -> ((Number) metrics.snapshot().get("backgroundRefreshFailures")).longValue() >= 2, 2000);

        GatewayResponse attempt3 = service.get("fragile-key"); // 失败2次 >= 上限2 -> 同步回源失败 -> 兜底
        log.info("[result] bgFailures=2: origin={} value={} (期望STALE_FALLBACK旧值)",
                attempt3.origin(), attempt3.value());
        assertEquals(ResultOrigin.STALE_FALLBACK, attempt3.origin(),
                "后台更新连续失败达到上限后不得再拿旧结果顶着，要落回兜底分类");
        assertEquals(fresh.value(), attempt3.value());
    }

    // ---------- 全局准入与有界排队 ----------

    @Test
    void queueWaitTimeoutAndCapacityExceededAreDistinct() throws Exception {
        properties.setMaxConcurrentUpstream(1);
        properties.setMaxQueueSize(1);
        properties.setQueueWaitTimeoutMillis(150);
        properties.setSimulatedLatencyMillis(800);
        properties.setUpstreamTimeoutMillis(3000);
        log.info("[case] 排队超时与总量拒绝区分: maxConcurrentUpstream={} maxQueueSize={} queueWaitTimeoutMs={} "
                        + "判定依据=排队者抛QueueWaitTimeout, 队列满时新请求抛CapacityExceeded",
                properties.getMaxConcurrentUpstream(), properties.getMaxQueueSize(),
                properties.getQueueWaitTimeoutMillis());

        // A 占住唯一许可（慢上游 800ms）
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<GatewayResponse> holder = pool.submit(() -> service.get("key-A"));
        waitFor(() -> ((Number) ((Map<?, ?>) metrics.snapshot().get("admission")).get("inFlight")).intValue() == 1,
                2000);

        // B 进入队列等待，150ms 后超时
        Future<GatewayResponse> waiter = pool.submit(() -> service.get("key-B"));
        waitFor(() -> ((Number) ((Map<?, ?>) metrics.snapshot().get("admission")).get("waiting")).intValue() == 1,
                2000);

        // C 到来时队列已满 -> 总量超限，立即拒绝
        GlobalAdmissionLimiter.CapacityExceededException capacity = assertThrows(
                GlobalAdmissionLimiter.CapacityExceededException.class, () -> service.get("key-C"));
        log.info("[result] key-C rejected immediately: {} (期望CAPACITY_EXCEEDED)", capacity.getMessage());

        // B 排队超时（Future.get 会包装成 ExecutionException，取 cause 判定类型）
        ExecutionException wrapped = assertThrows(ExecutionException.class, waiter::get);
        assertTrue(wrapped.getCause() instanceof GlobalAdmissionLimiter.QueueWaitTimeoutException,
                "排队没等到必须是 QueueWaitTimeoutException, 实际=" + wrapped.getCause());
        log.info("[result] key-B queue wait timeout: {} (期望QUEUE_WAIT_TIMEOUT)",
                wrapped.getCause().getMessage());

        holder.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        Map<String, Object> snap = metrics.snapshot();
        log.info("[result] metrics: queueWaitTimeouts={} capacityExceeded={}",
                snap.get("queueWaitTimeouts"), snap.get("capacityExceeded"));
        assertTrue(((Number) snap.get("queueWaitTimeouts")).longValue() >= 1);
        assertTrue(((Number) snap.get("capacityExceeded")).longValue() >= 1);
    }

    @Test
    void cacheHitsAndCoalescedRequestsBypassGlobalAdmission() throws Exception {
        properties.setMaxConcurrentUpstream(1);
        properties.setMaxQueueSize(0); // 队列长度0: 任何真实回源排队都会立即总量超限
        properties.setQueueWaitTimeoutMillis(50);
        properties.setSimulatedLatencyMillis(600);
        properties.setUpstreamTimeoutMillis(3000);
        log.info("[case] 缓存命中与合并不被全局准入误伤: maxConcurrentUpstream={} maxQueueSize={} "
                        + "判定依据=许可被占时缓存命中仍FRESH, 同key并发合并后共享结果不抛拒绝",
                properties.getMaxConcurrentUpstream(), properties.getMaxQueueSize());

        // 先建立 key-hot 的缓存
        GatewayResponse seeded = service.get("key-hot");
        assertEquals(ResultOrigin.FRESH, seeded.origin());

        // 慢请求占住唯一全局许可
        ExecutorService pool = Executors.newFixedThreadPool(4);
        Future<GatewayResponse> holder = pool.submit(() -> service.get("key-slow"));
        waitFor(() -> ((Number) ((Map<?, ?>) metrics.snapshot().get("admission")).get("inFlight")).intValue() == 1,
                2000);

        // 许可被占期间：缓存命中必须照常返回，不得被拒绝
        GatewayResponse hit = service.get("key-hot");
        log.info("[result] cache-hit under full capacity: origin={} (期望FRESH不抛异常)", hit.origin());
        assertEquals(ResultOrigin.FRESH, hit.origin());
        assertEquals(seeded.value(), hit.value());

        holder.get(5, TimeUnit.SECONDS);

        // 许可释放后：同一 key 的并发请求合并成一次回源，跟随者不占用全局名额
        int threads = 6;
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<GatewayResponse>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                gate.await();
                return service.get("key-coalesce");
            }));
        }
        gate.countDown();
        List<GatewayResponse> responses = new ArrayList<>();
        for (Future<GatewayResponse> f : futures) {
            responses.add(f.get(10, TimeUnit.SECONDS));
        }
        pool.shutdown();
        long upstreamCalls = upstream.getCallCount();
        log.info("[result] coalesced: upstreamCalls={} origins={} (期望总回源=3: hot+slow+coalesce各1次)",
                upstreamCalls, responses.stream().map(r -> r.origin().name()).distinct().toList());
        assertEquals(3, upstreamCalls, "合并跟随者不得触发额外回源也不得被全局准入拒绝");
        for (GatewayResponse r : responses) {
            assertEquals(ResultOrigin.FRESH, r.origin());
        }
    }

    // ---------- 熔断器 ----------

    @Test
    void circuitOpensAfterThresholdAndRecoversViaProbe() throws Exception {
        properties.setCircuitFailureThreshold(3);
        properties.setCircuitOpenMillis(400);
        properties.setCircuitHalfOpenProbes(1);
        properties.setSimulatedFailure(true);
        properties.setSimulatedLatencyMillis(20);
        log.info("[case] 熔断完整过程: threshold={} openMs={} probes={} 判定依据=连续失败{}次后OPEN不再打上游, "
                        + "保护期后试探失败继续OPEN, 试探成功恢复CLOSED",
                properties.getCircuitFailureThreshold(), properties.getCircuitOpenMillis(),
                properties.getCircuitHalfOpenProbes(), properties.getCircuitFailureThreshold());

        // 连续失败达到阈值 -> OPEN
        for (int i = 0; i < 3; i++) {
            GatewayResponse r = service.get("cb-key-" + i);
            assertEquals(ResultOrigin.DEGRADED, r.origin());
        }
        assertEquals("OPEN", circuitState());
        long callsAtOpen = upstream.getCallCount();
        log.info("[result] circuit OPEN after {} failures, upstreamCalls={}", 3, callsAtOpen);

        // 保护期内：直接兜底，不再打上游
        GatewayResponse duringOpen = service.get("cb-key-open");
        assertEquals(ResultOrigin.DEGRADED, duringOpen.origin());
        assertEquals(callsAtOpen, upstream.getCallCount(), "保护期内不得再访问上游");
        log.info("[result] during OPEN: origin={} upstreamCalls={} (未增加)", duringOpen.origin(),
                upstream.getCallCount());

        // 保护期结束 -> 试探；上游仍故障 -> 试探失败，重新OPEN
        Thread.sleep(500);
        GatewayResponse probeFail = service.get("cb-key-probe1");
        assertEquals(ResultOrigin.DEGRADED, probeFail.origin());
        assertEquals(callsAtOpen + 1, upstream.getCallCount(), "保护期后只放1个试探请求");
        assertEquals("OPEN", circuitState());
        log.info("[result] probe failed -> OPEN again, upstreamCalls={}", upstream.getCallCount());

        // 上游恢复 -> 下一个保护期结束后试探成功 -> CLOSED
        properties.setSimulatedFailure(false);
        Thread.sleep(500);
        GatewayResponse probeOk = service.get("cb-key-probe2");
        log.info("[result] probe success: origin={} state={} (期望FRESH+CLOSED)",
                probeOk.origin(), circuitState());
        assertEquals(ResultOrigin.FRESH, probeOk.origin());
        assertEquals("CLOSED", circuitState());
    }

    @Test
    void halfOpenAdmitsOnlyLimitedProbesUnderConcurrency() throws Exception {
        properties.setCircuitFailureThreshold(2);
        properties.setCircuitOpenMillis(300);
        properties.setCircuitHalfOpenProbes(1);
        properties.setSimulatedFailure(true);
        log.info("[case] 试探期并发限制: probes={} 判定依据=保护期后并发{}个请求只有{}个真正打到上游, 其余直接兜底",
                properties.getCircuitHalfOpenProbes(), 6, properties.getCircuitHalfOpenProbes());

        service.get("cb-c-1");
        service.get("cb-c-2");
        assertEquals("OPEN", circuitState());

        // 上游恢复但响应慢，让试探请求飞一会儿
        properties.setSimulatedFailure(false);
        properties.setSimulatedLatencyMillis(500);
        Thread.sleep(400); // 保护期结束

        int threads = 6;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger degraded = new AtomicInteger();
        AtomicInteger fresh = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        long callsBefore = upstream.getCallCount();
        for (int i = 0; i < threads; i++) {
            int idx = i;
            futures.add(pool.submit(() -> {
                gate.await();
                GatewayResponse r = service.get("cb-half-" + idx);
                if (r.origin() == ResultOrigin.FRESH) {
                    fresh.incrementAndGet();
                } else {
                    degraded.incrementAndGet();
                }
                return null;
            }));
        }
        gate.countDown();
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();

        long probeCalls = upstream.getCallCount() - callsBefore;
        log.info("[result] probeCalls={} fresh={} degraded={} state={} (期望probeCalls=1, 其余{}个兜底)",
                probeCalls, fresh.get(), degraded.get(), circuitState(), threads - 1);
        assertEquals(1, probeCalls, "试探期同一时间只能放出配置数量的试探请求，不得放一大批");
        assertEquals(threads - 1, degraded.get(), "未拿到试探名额的请求必须走兜底而不是被丢掉");
        waitFor(() -> "CLOSED".equals(circuitState()), 2000);
    }

    // ---------- 工具 ----------

    private String circuitState() {
        return String.valueOf(((Map<?, ?>) metrics.snapshot().get("circuit")).get("state"));
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
