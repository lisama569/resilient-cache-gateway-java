package com.github.lisama569.resilientcachegatewayjava.tests;

import com.github.lisama569.resilientcachegatewayjava.gateway.CacheGatewayService;
import com.github.lisama569.resilientcachegatewayjava.gateway.GatewayMetrics;
import com.github.lisama569.resilientcachegatewayjava.gateway.GatewayProperties;
import com.github.lisama569.resilientcachegatewayjava.gateway.GatewayResponse;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 缓存聚合网关核心行为测试：并发一致性、过期判定、超时降级、过载拒绝。
 * 每个用例打印关键输入与判定依据。
 */
class CacheGatewayServiceTest {

    private static final Logger log = LoggerFactory.getLogger(CacheGatewayServiceTest.class);

    private GatewayProperties properties;
    private SimulatedUpstream upstream;
    private GatewayMetrics metrics;
    private CacheGatewayService service;

    @BeforeEach
    void setUp() {
        properties = new GatewayProperties();
        properties.setCacheTtlMillis(300);
        properties.setUpstreamTimeoutMillis(1000);
        properties.setMaxConcurrentPerKey(8);
        properties.setSimulatedLatencyMillis(20);
        properties.setSimulatedFailure(false);
        upstream = new SimulatedUpstream(properties);
        metrics = new GatewayMetrics();
        service = new CacheGatewayService(properties, upstream, metrics);
        log.info("[setup] ttlMs={} timeoutMs={} maxConcurrentPerKey={} latencyMs={} failure={}",
                properties.getCacheTtlMillis(), properties.getUpstreamTimeoutMillis(),
                properties.getMaxConcurrentPerKey(), properties.getSimulatedLatencyMillis(),
                properties.isSimulatedFailure());
    }

    @Test
    void concurrentReadsAreCoalescedIntoSingleUpstreamCall() throws Exception {
        int threads = 16;
        properties.setMaxConcurrentPerKey(32); // 本用例聚焦合并, 不触发过载拒绝
        log.info("[case] 并发一致性: threads={} latencyMs={} 判定依据=所有结果值相同且上游只调用1次",
                threads, properties.getSimulatedLatencyMillis());
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<GatewayResponse>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                gate.await();
                return service.get("hot-key");
            }));
        }
        gate.countDown();
        List<GatewayResponse> responses = new ArrayList<>();
        for (Future<GatewayResponse> f : futures) {
            responses.add(f.get(5, TimeUnit.SECONDS));
        }
        pool.shutdown();

        long upstreamCalls = upstream.getCallCount();
        String firstValue = responses.get(0).value();
        log.info("[result] upstreamCalls={} firstValue={} origins={}",
                upstreamCalls, firstValue,
                responses.stream().map(r -> r.origin().name()).distinct().toList());
        assertEquals(1, upstreamCalls, "同一目标有效期内只应触发一次上游读取");
        for (GatewayResponse r : responses) {
            assertEquals(ResultOrigin.FRESH, r.origin());
            assertEquals(firstValue, r.value(), "并发请求必须拿到一致的结果");
        }
        assertTrue(metrics.snapshot().get("coalesced") instanceof Long c && c >= threads - 1,
                "应有请求被合并");
    }

    @Test
    void expiredEntryIsRefreshedAndNeverServedAsFresh() throws Exception {
        log.info("[case] 过期判定: ttlMs={} 判定依据=过期后重新回源且新值不同, 过期内容不得作为FRESH返回",
                properties.getCacheTtlMillis());
        GatewayResponse first = service.get("k1");
        GatewayResponse second = service.get("k1");
        log.info("[result] first={} second={} upstreamCalls={}",
                first.value(), second.value(), upstream.getCallCount());
        assertEquals(ResultOrigin.FRESH, first.origin());
        assertEquals(ResultOrigin.FRESH, second.origin(), "有效期内命中缓存仍为新鲜结果");
        assertEquals(first.value(), second.value(), "有效期内应命中缓存");
        assertEquals(1, upstream.getCallCount(), "有效期内不应重复回源");

        Thread.sleep(properties.getCacheTtlMillis() + 150);
        GatewayResponse third = service.get("k1");
        log.info("[result] after expiry: third={} fetchedAt={} upstreamCalls={}",
                third.value(), third.fetchedAt(), upstream.getCallCount());
        assertEquals(ResultOrigin.FRESH, third.origin(), "刷新成功的新结果才是FRESH");
        assertNotEquals(first.value(), third.value(), "过期后必须重新获取, 不得返回旧的过期内容");
        assertEquals(2, upstream.getCallCount(), "过期后必须重新回源一次");
    }

    @Test
    void upstreamTimeoutWithoutCacheYieldsDegraded() {
        properties.setSimulatedLatencyMillis(800);
        properties.setUpstreamTimeoutMillis(200);
        log.info("[case] 上游超时(无缓存): latencyMs={} timeoutMs={} 判定依据=origin=DEGRADED且value=null",
                properties.getSimulatedLatencyMillis(), properties.getUpstreamTimeoutMillis());
        GatewayResponse r = service.get("slow-key");
        log.info("[result] origin={} value={} elapsedMs={}", r.origin(), r.value(), r.elapsedMillis());
        assertEquals(ResultOrigin.DEGRADED, r.origin());
        assertNull(r.value());
        assertTrue(r.elapsedMillis() < 800, "超时必须在配置时间内返回, 不得等待慢上游");
    }

    @Test
    void upstreamTimeoutWithExpiredCacheYieldsStaleFallback() throws Exception {
        log.info("[case] 上游超时(有过期缓存): 先正常回源, 过期后上游变慢 判定依据=origin=STALE_FALLBACK且值为旧值");
        GatewayResponse fresh = service.get("stale-key");
        assertEquals(ResultOrigin.FRESH, fresh.origin());

        Thread.sleep(properties.getCacheTtlMillis() + 150);
        properties.setSimulatedLatencyMillis(800);
        properties.setUpstreamTimeoutMillis(200);
        log.info("[input] latencyMs={} timeoutMs={} cacheExpired=true",
                properties.getSimulatedLatencyMillis(), properties.getUpstreamTimeoutMillis());
        GatewayResponse r = service.get("stale-key");
        log.info("[result] origin={} value={} fetchedAt={}", r.origin(), r.value(), r.fetchedAt());
        assertEquals(ResultOrigin.STALE_FALLBACK, r.origin());
        assertEquals(fresh.value(), r.value(), "兜底必须返回已缓存的旧值");
    }

    @Test
    void upstreamFailureYieldsDegradedThenStaleFallback() throws Exception {
        log.info("[case] 上游故障: failure=true 判定依据=无缓存DEGRADED, 有过期缓存STALE_FALLBACK");
        properties.setSimulatedFailure(true);
        GatewayResponse degraded = service.get("fail-key");
        log.info("[result] no-cache: origin={} value={}", degraded.origin(), degraded.value());
        assertEquals(ResultOrigin.DEGRADED, degraded.origin());

        // 恢复正常建立缓存, 再故障验证过期兜底
        properties.setSimulatedFailure(false);
        GatewayResponse fresh = service.get("fail-key2");
        assertEquals(ResultOrigin.FRESH, fresh.origin());
        Thread.sleep(properties.getCacheTtlMillis() + 150);
        properties.setSimulatedFailure(true);
        GatewayResponse stale = service.get("fail-key2");
        log.info("[result] expired-cache: origin={} value={}", stale.origin(), stale.value());
        assertEquals(ResultOrigin.STALE_FALLBACK, stale.origin());
        assertEquals(fresh.value(), stale.value());
    }

    @Test
    void overloadRequestsAreRejectedDeterministically() throws Exception {
        int threads = 10;
        properties.setMaxConcurrentPerKey(2);
        properties.setSimulatedLatencyMillis(500);
        properties.setUpstreamTimeoutMillis(2000);
        log.info("[case] 过载拒绝: threads={} maxConcurrentPerKey={} 判定依据=2个成功+8个拒绝, 规则确定一致",
                threads, properties.getMaxConcurrentPerKey());

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                gate.await();
                try {
                    service.get("hotspot");
                    succeeded.incrementAndGet();
                } catch (CacheGatewayService.OverloadRejectedException e) {
                    rejected.incrementAndGet();
                }
                return null;
            }));
        }
        gate.countDown();
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();

        log.info("[result] succeeded={} rejected={} metrics.rejected={}",
                succeeded.get(), rejected.get(), metrics.snapshot().get("rejected"));
        assertEquals(threads, succeeded.get() + rejected.get(), "每个请求都有确定结果, 不得静默丢失");
        assertEquals(2, succeeded.get(), "最多放行maxConcurrentPerKey个并发请求");
        assertEquals(threads - 2, rejected.get(), "超出承受能力的请求必须被拒绝");
    }
}
