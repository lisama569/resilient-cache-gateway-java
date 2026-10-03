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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 全局容量保护与有界排队测试：
 * 排队等待超时与总量超限两种拒绝可区分；缓存命中与合并请求不受误伤。
 */
class GlobalAdmissionGuardTest {

    private static final Logger log = LoggerFactory.getLogger(GlobalAdmissionGuardTest.class);

    private GatewayProperties properties;
    private SimulatedUpstream upstream;
    private GatewayMetrics metrics;
    private CacheGatewayService service;

    @BeforeEach
    void setUp() {
        properties = new GatewayProperties();
        properties.setCacheTtlMillis(300);
        properties.setStaleGraceMillis(0); // 本类聚焦全局守卫, 关闭宽限窗口
        properties.setUpstreamTimeoutMillis(3000);
        properties.setMaxConcurrentPerKey(64); // 不触发单 key 拒绝
        properties.setGlobalMaxConcurrent(2);
        properties.setGlobalQueueCapacity(2);
        properties.setGlobalQueueTimeoutMillis(300);
        properties.setSimulatedLatencyMillis(800); // 慢上游, 占住全局许可
        properties.setSimulatedFailure(false);
        upstream = new SimulatedUpstream(properties);
        metrics = new GatewayMetrics();
        service = new CacheGatewayService(properties, upstream, metrics,
                new CircuitBreaker(properties), new GlobalAdmissionGuard(properties));
        log.info("[setup] globalMaxConcurrent={} queueCapacity={} queueTimeoutMs={} latencyMs={}",
                properties.getGlobalMaxConcurrent(), properties.getGlobalQueueCapacity(),
                properties.getGlobalQueueTimeoutMillis(), properties.getSimulatedLatencyMillis());
    }

    @Test
    void queueTimeoutAndQueueFullAreDistinguished() throws Exception {
        // 2 个占住许可 + 2 个排队(等不到→超时) + 1 个超出队列容量(立即拒绝)
        log.info("[case] 排队超时vs总量拒绝: 许可2 队列2 慢上游800ms 判定依据=2成功+2个QUEUE_TIMEOUT+1个QUEUE_FULL, 异常类型可区分");
        ExecutorService pool = Executors.newFixedThreadPool(5);
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger queueTimeout = new AtomicInteger();
        AtomicInteger queueFull = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                gate.await();
                try {
                    service.get("bulk-key-" + idx); // 不同 key, 避免合并与单key守卫干扰
                    succeeded.incrementAndGet();
                } catch (GlobalAdmissionGuard.QueueTimeoutRejectedException e) {
                    queueTimeout.incrementAndGet();
                } catch (GlobalAdmissionGuard.QueueFullRejectedException e) {
                    queueFull.incrementAndGet();
                } catch (RuntimeException e) {
                    other.incrementAndGet();
                    log.warn("[result] unexpected exception: {}", e.toString());
                }
                return null;
            }));
        }
        gate.countDown();
        for (Future<?> f : futures) {
            f.get(15, TimeUnit.SECONDS);
        }
        pool.shutdown();

        log.info("[result] succeeded={} queueTimeout={} queueFull={} other={}",
                succeeded.get(), queueTimeout.get(), queueFull.get(), other.get());
        assertEquals(2, succeeded.get(), "只有globalMaxConcurrent个请求能真正打到上游");
        assertEquals(2, queueTimeout.get(), "排队等不到许可的请求以QUEUE_TIMEOUT拒绝");
        assertEquals(1, queueFull.get(), "超出队列容量的请求以QUEUE_FULL立即拒绝");
        assertEquals(0, other.get(), "不得出现其他未分类结果");
        assertEquals(5, succeeded.get() + queueTimeout.get() + queueFull.get(),
                "每个请求都有确定结果, 不得无限等待或静默丢失");
    }

    @Test
    void cacheHitsBypassGlobalGuard() throws Exception {
        log.info("[case] 缓存命中不受全局守卫影响: 判定依据=许可被占满时命中缓存的读取仍立即返回FRESH");
        properties.setSimulatedLatencyMillis(20);
        GatewayResponse fresh = service.get("cached-key");
        assertEquals(ResultOrigin.FRESH, fresh.origin());

        // 用两个慢请求占满全局许可
        properties.setSimulatedLatencyMillis(800);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch started = new CountDownLatch(2);
        List<Future<?>> slow = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            final int idx = i;
            slow.add(pool.submit(() -> {
                started.countDown();
                service.get("slow-key-" + idx);
                return null;
            }));
        }
        started.await();
        Thread.sleep(100); // 确保慢请求已占住许可

        GatewayResponse hit = service.get("cached-key");
        log.info("[result] cache hit under full guard: origin={} elapsedMs={}", hit.origin(), hit.elapsedMillis());
        assertEquals(ResultOrigin.FRESH, hit.origin(), "缓存命中不得被全局排队误伤");
        assertTrue(hit.elapsedMillis() < properties.getGlobalQueueTimeoutMillis(),
                "缓存命中必须立即返回, 不得排队");

        for (Future<?> f : slow) {
            f.get(15, TimeUnit.SECONDS);
        }
        pool.shutdown();
    }

    @Test
    void coalescedRequestsBypassGlobalGuard() throws Exception {
        int threads = 6;
        properties.setSimulatedLatencyMillis(300);
        log.info("[case] 合并请求不受全局守卫影响: threads={} 同key 判定依据=全部成功且上游只调用1次, 无排队拒绝",
                threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<GatewayResponse>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                gate.await();
                return service.get("merge-key");
            }));
        }
        gate.countDown();
        List<GatewayResponse> responses = new ArrayList<>();
        for (Future<GatewayResponse> f : futures) {
            responses.add(f.get(10, TimeUnit.SECONDS)); // 合并等待者不应收到排队拒绝异常
        }
        pool.shutdown();

        for (GatewayResponse r : responses) {
            assertEquals(ResultOrigin.FRESH, r.origin(), "合并进同一次回源的请求不得被全局守卫误伤");
        }
        log.info("[result] upstreamCalls={} coalesced={}",
                upstream.getCallCount(), metrics.snapshot().get("coalesced"));
        assertEquals(1, upstream.getCallCount(), "同 key 并发只应有一次真实回源");
    }
}
