package com.github.lisama569.resilientcachegatewayjava.tests;

import com.github.lisama569.resilientcachegatewayjava.cache.CacheEntryStore;
import com.github.lisama569.resilientcachegatewayjava.cache.InMemoryCacheEntryStore;
import com.github.lisama569.resilientcachegatewayjava.config.GatewayProperties;
import com.github.lisama569.resilientcachegatewayjava.exception.RequestRejectedException;
import com.github.lisama569.resilientcachegatewayjava.metrics.MetricsRegistry;
import com.github.lisama569.resilientcachegatewayjava.model.RejectReason;
import com.github.lisama569.resilientcachegatewayjava.model.ResultStatus;
import com.github.lisama569.resilientcachegatewayjava.resilience.CircuitBreakerRegistry;
import com.github.lisama569.resilientcachegatewayjava.service.CacheGatewayService;
import com.github.lisama569.resilientcachegatewayjava.upstream.DefaultSimulationState;
import com.github.lisama569.resilientcachegatewayjava.upstream.SimulatedUpstreamClient;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 服务层并发保护测试（基于真实模拟上游 + 真实线程）：
 *  - 同热点并发只触发 1 次上游（端到端单飞）；
 *  - 超过每热点容量 -> PER_KEY_OVERLOADED 429 式拒绝；
 *  - 排队超时 -> QUEUE_TIMEOUT；
 *  - 全局上游并发耗尽 -> GLOBAL_OVERLOADED。
 */
class CacheGatewayOverloadTest {

    private static final Logger log = LoggerFactory.getLogger(CacheGatewayOverloadTest.class);

    private record Harness(CacheGatewayService service, DefaultSimulationState state,
                           MutableTimeSource clock, CacheEntryStore store,
                           CircuitBreakerRegistry circuits, MetricsRegistry metrics,
                           GatewayProperties props) {
    }

    private Harness harness(int perKey, int global, long acquireTimeoutMs,
                            long upstreamTimeoutMs, long defaultDelayMs) {
        GatewayProperties props = new GatewayProperties();
        props.setTtlMillis(5000);
        props.setStaleTtlMillis(5000);
        props.setUpstreamTimeoutMillis(upstreamTimeoutMs);
        props.setPerKeyMaxConcurrent(perKey);
        props.setGlobalMaxConcurrent(global);
        props.setAcquireTimeoutMillis(acquireTimeoutMs);
        props.setFailureThreshold(5);
        props.setOpenStateMillis(2000);
        props.setUpstreamPoolSize(Math.max(64, global * 2));
        props.getUpstream().setDefaultDelayMillis(defaultDelayMs);

        MutableTimeSource clock = new MutableTimeSource();
        CacheEntryStore store = new InMemoryCacheEntryStore();
        DefaultSimulationState state = new DefaultSimulationState(props);
        SimulatedUpstreamClient client = new SimulatedUpstreamClient(state, clock);
        CircuitBreakerRegistry circuits = new CircuitBreakerRegistry(props);
        MetricsRegistry metrics = new MetricsRegistry();
        CacheGatewayService service = new CacheGatewayService(
                store, client, props, clock, circuits, metrics);
        return new Harness(service, state, clock, store, circuits, metrics, props);
    }

    @Test
    void concurrentRequestsShareSingleUpstreamCallEndToEnd() throws Exception {
        Harness h = harness(50, 50, 5000, 5000, 300);
        int threads = 30;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    return h.service.fetch("hot");
                }));
            }
            start.countDown();
            int fresh = 0;
            for (Future<?> f : futures) {
                Object result = f.get(10, TimeUnit.SECONDS);
                assertThat(result).hasFieldOrPropertyWithValue("status", ResultStatus.FRESH);
                fresh++;
            }
            log.info("[overload-test] 30 并发同目标: 全部 FRESH={}, 上游实际调用={} 期望 1",
                    fresh, h.state.callCount("hot"));
            assertThat(fresh).isEqualTo(threads);
            assertThat(h.state.callCount("hot")).isEqualTo(1L);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void excessRequestsForHotKeyAreRejectedWithPerKeyOverloaded() throws Exception {
        // 容量 5（1 leader + 4 waiter），上游慢到足以让全部请求同时在途
        Harness h = harness(5, 50, 5000, 5000, 800);
        ExecutorService pool = Executors.newFixedThreadPool(20);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger rejected = new AtomicInteger();
        AtomicInteger fresh = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    try {
                        if (h.service.fetch("burst").status() == ResultStatus.FRESH) {
                            fresh.incrementAndGet();
                        } else {
                            other.incrementAndGet();
                        }
                    } catch (RequestRejectedException ex) {
                        if (ex.reason() == RejectReason.PER_KEY_OVERLOADED) {
                            rejected.incrementAndGet();
                        } else {
                            other.incrementAndGet();
                        }
                    }
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
            log.info("[overload-test] 输入 20 并发, perKeyMax=5: FRESH={}, PER_KEY_OVERLOADED={}, 其他={}",
                    fresh.get(), rejected.get(), other.get());
            assertThat(fresh.get()).isEqualTo(5);
            assertThat(rejected.get()).isEqualTo(15);
            assertThat(other.get()).isZero();
            assertThat(h.state.callCount("burst")).isEqualTo(1L);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void waitersBeyondAcquireTimeoutReceiveQueueTimeoutWithoutInfinitePileup() throws Exception {
        // 容量 10，排队上限 100ms，上游耗时 600ms
        Harness h = harness(10, 50, 100, 5000, 600);
        ExecutorService pool = Executors.newFixedThreadPool(10);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger queueTimeout = new AtomicInteger();
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    try {
                        h.service.fetch("slow");
                    } catch (RequestRejectedException ex) {
                        if (ex.reason() == RejectReason.QUEUE_TIMEOUT) {
                            queueTimeout.incrementAndGet();
                        }
                    }
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
            log.info("[overload-test] acquireTimeout=100ms, upstream=600ms: QUEUE_TIMEOUT 数={}（期望 9 个 waiter）",
                    queueTimeout.get());
            assertThat(queueTimeout.get()).isEqualTo(9);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void globalConcurrencyLimitProducesGlobalOverloadedForDistinctKeys() throws Exception {
        // 全局上游并发 = 2，10 个不同 key 同时加载
        Harness h = harness(10, 2, 5000, 5000, 600);
        ExecutorService pool = Executors.newFixedThreadPool(10);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger globalRejected = new AtomicInteger();
        AtomicInteger fresh = new AtomicInteger();
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                String target = "g" + i;
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    try {
                        if (h.service.fetch(target).status() == ResultStatus.FRESH) {
                            fresh.incrementAndGet();
                        }
                    } catch (RequestRejectedException ex) {
                        if (ex.reason() == RejectReason.GLOBAL_OVERLOADED) {
                            globalRejected.incrementAndGet();
                        }
                    }
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
            log.info("[overload-test] globalMax=2, 10 不同 key 并发: FRESH={}, GLOBAL_OVERLOADED={}",
                    fresh.get(), globalRejected.get());
            // 首批 2 个成功；其余 8 个在同一窗口内尝试获取全局许可时被拒（无缓存 -> 显式拒绝）
            assertThat(fresh.get()).isEqualTo(2);
            assertThat(globalRejected.get()).isEqualTo(8);
        } finally {
            pool.shutdownNow();
        }
    }
}
