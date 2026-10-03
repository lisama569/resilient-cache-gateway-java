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
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 宽限窗口（stale-while-revalidate）行为测试：
 * 窗口边界、并发只触发一次后台更新、更新后新值可见、更新失败超限后落回兜底。
 * 每个用例打印关键配置与判定依据。
 */
class StaleGraceWindowTest {

    private static final Logger log = LoggerFactory.getLogger(StaleGraceWindowTest.class);

    private GatewayProperties properties;
    private SimulatedUpstream upstream;
    private GatewayMetrics metrics;
    private CacheGatewayService service;

    @BeforeEach
    void setUp() {
        properties = new GatewayProperties();
        properties.setCacheTtlMillis(300);
        properties.setStaleGraceMillis(2000);
        properties.setStaleMaxRefreshFailures(2);
        properties.setUpstreamTimeoutMillis(1000);
        properties.setMaxConcurrentPerKey(32);
        properties.setSimulatedLatencyMillis(20);
        properties.setSimulatedFailure(false);
        upstream = new SimulatedUpstream(properties);
        metrics = new GatewayMetrics();
        service = new CacheGatewayService(properties, upstream, metrics,
                new CircuitBreaker(properties), new GlobalAdmissionGuard(properties));
        log.info("[setup] ttlMs={} staleGraceMs={} maxRefreshFailures={} timeoutMs={} latencyMs={}",
                properties.getCacheTtlMillis(), properties.getStaleGraceMillis(),
                properties.getStaleMaxRefreshFailures(), properties.getUpstreamTimeoutMillis(),
                properties.getSimulatedLatencyMillis());
    }

    @Test
    void graceWindowServesStaleRefreshingAndTriggersSingleBackgroundRefresh() throws Exception {
        int threads = 8;
        log.info("[case] 宽限窗口并发: threads={} 判定依据=全部返回STALE_REFRESHING(旧值), 上游总共只调用2次(同步1+后台1)",
                threads);
        GatewayResponse fresh = service.get("swr-key");
        assertEquals(ResultOrigin.FRESH, fresh.origin());
        Thread.sleep(properties.getCacheTtlMillis() + 100); // 过期但仍在宽限窗口内

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<GatewayResponse>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                gate.await();
                return service.get("swr-key");
            }));
        }
        gate.countDown();
        List<GatewayResponse> responses = new ArrayList<>();
        for (Future<GatewayResponse> f : futures) {
            responses.add(f.get(5, TimeUnit.SECONDS));
        }
        pool.shutdown();

        for (GatewayResponse r : responses) {
            assertEquals(ResultOrigin.STALE_REFRESHING, r.origin(),
                    "宽限窗口内必须明确标记为后台更新中的旧结果");
            assertEquals(fresh.value(), r.value(), "窗口内返回的必须是上一份旧结果");
        }
        // 等待后台更新完成
        waitUntil(() -> upstream.getCallCount() >= 2, 3000);
        Thread.sleep(100); // 留出并发窗口, 确认没有额外触发
        long calls = upstream.getCallCount();
        log.info("[result] upstreamCalls={} backgroundRefreshes={} origins=全部STALE_REFRESHING",
                calls, metrics.snapshot().get("backgroundRefreshes"));
        assertEquals(2, calls, "并发请求只能触发一次后台更新, 不得重复回源");
        assertEquals(1L, metrics.snapshot().get("backgroundRefreshes"));

        // 后台更新成功后, 后续读取立即看到新结果
        GatewayResponse after = service.get("swr-key");
        log.info("[result] after refresh: origin={} value={} 旧值={}", after.origin(), after.value(), fresh.value());
        assertEquals(ResultOrigin.FRESH, after.origin());
        assertNotEquals(fresh.value(), after.value(), "后台更新成功后必须立即看到新结果");
    }

    @Test
    void requestDuringInFlightRefreshDoesNotTriggerDuplicate() throws Exception {
        properties.setSimulatedLatencyMillis(400); // 让后台更新足够慢, 覆盖"更新进行中又来请求"
        log.info("[case] 更新进行中并发: latencyMs={} 判定依据=更新进行中来的请求仍返回STALE_REFRESHING且不重复触发",
                properties.getSimulatedLatencyMillis());
        GatewayResponse fresh = service.get("inflight-key");
        Thread.sleep(properties.getCacheTtlMillis() + 100);

        GatewayResponse first = service.get("inflight-key"); // 触发后台更新
        assertEquals(ResultOrigin.STALE_REFRESHING, first.origin());
        List<GatewayResponse> during = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            during.add(service.get("inflight-key")); // 更新进行中再来请求
        }
        for (GatewayResponse r : during) {
            assertEquals(ResultOrigin.STALE_REFRESHING, r.origin(), "更新进行中必须继续返回旧结果");
            assertEquals(fresh.value(), r.value());
        }
        waitUntil(() -> upstream.getCallCount() >= 2, 3000);
        Thread.sleep(100);
        log.info("[result] upstreamCalls={} backgroundRefreshes={}",
                upstream.getCallCount(), metrics.snapshot().get("backgroundRefreshes"));
        assertEquals(2, upstream.getCallCount(), "更新进行中的请求不得重复触发后台更新");
    }

    @Test
    void graceWindowBoundaryFallsBackToSyncPath() throws Exception {
        properties.setStaleGraceMillis(400);
        log.info("[case] 窗口边界: ttlMs={} graceMs={} 判定依据=窗口内STALE_REFRESHING, 窗口外落回同步路径(此处上游故障→STALE_FALLBACK)",
                properties.getCacheTtlMillis(), properties.getStaleGraceMillis());
        GatewayResponse fresh = service.get("boundary-key");
        assertEquals(ResultOrigin.FRESH, fresh.origin());

        Thread.sleep(properties.getCacheTtlMillis() + 100); // 窗口内
        GatewayResponse inWindow = service.get("boundary-key");
        log.info("[result] in-window: origin={}", inWindow.origin());
        assertEquals(ResultOrigin.STALE_REFRESHING, inWindow.origin(), "窗口内必须返回旧值+后台更新");
        waitUntil(() -> upstream.getCallCount() >= 2, 3000); // 等后台更新完成, 缓存被刷新

        // 等待新缓存也彻底越过 ttl+grace 边界
        Thread.sleep(properties.getCacheTtlMillis() + properties.getStaleGraceMillis() + 200);
        properties.setSimulatedFailure(true);
        GatewayResponse beyond = service.get("boundary-key");
        log.info("[result] beyond-window: origin={} value={} 判定依据=不再是STALE_REFRESHING",
                beyond.origin(), beyond.value());
        assertEquals(ResultOrigin.STALE_FALLBACK, beyond.origin(),
                "宽限时间用完后不得再拿旧结果顶着, 必须落回现有兜底分类");
    }

    @Test
    void consecutiveRefreshFailuresExhaustGraceAndFallBack() throws Exception {
        log.info("[case] 后台更新连续失败: maxRefreshFailures={} 判定依据=失败{}次后落回STALE_FALLBACK, 不再返回STALE_REFRESHING",
                properties.getStaleMaxRefreshFailures(), properties.getStaleMaxRefreshFailures());
        GatewayResponse fresh = service.get("failing-key");
        assertEquals(ResultOrigin.FRESH, fresh.origin());
        Thread.sleep(properties.getCacheTtlMillis() + 100);

        properties.setSimulatedFailure(true); // 上游开始持续故障
        for (int i = 1; i <= properties.getStaleMaxRefreshFailures(); i++) {
            GatewayResponse r = service.get("failing-key");
            assertEquals(ResultOrigin.STALE_REFRESHING, r.origin(),
                    "第" + i + "次失败前窗口内仍返回旧值");
            final int expected = i;
            waitUntil(() -> ((Number) metrics.snapshot().get("backgroundRefreshFailures")).intValue() >= expected,
                    3000);
            log.info("[result] refresh failure #{} recorded, backgroundRefreshFailures={}",
                    i, metrics.snapshot().get("backgroundRefreshFailures"));
        }

        GatewayResponse exhausted = service.get("failing-key");
        log.info("[result] exhausted: origin={} value={}", exhausted.origin(), exhausted.value());
        assertEquals(ResultOrigin.STALE_FALLBACK, exhausted.origin(),
                "后台更新连续失败达到上限后必须落回兜底分类");
        assertEquals(fresh.value(), exhausted.value(), "兜底仍返回已缓存的旧值");
    }

    /** 轮询等待条件成立, 避免用固定 sleep 赌时序。 */
    private static void waitUntil(BooleanSupplier condition, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!condition.getAsBoolean()) {
            assertTrue(System.currentTimeMillis() < deadline, "等待条件超时");
            Thread.sleep(20);
        }
    }
}
