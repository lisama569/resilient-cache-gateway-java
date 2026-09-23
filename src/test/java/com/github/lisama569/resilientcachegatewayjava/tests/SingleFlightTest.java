package com.github.lisama569.resilientcachegatewayjava.tests;

import com.github.lisama569.resilientcachegatewayjava.core.TimeSource;
import com.github.lisama569.resilientcachegatewayjava.exception.RequestRejectedException;
import com.github.lisama569.resilientcachegatewayjava.model.RejectReason;
import com.github.lisama569.resilientcachegatewayjava.resilience.SingleFlight;
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
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * 单飞合并与热点舱壁测试。
 */
class SingleFlightTest {

    private static final Logger log = LoggerFactory.getLogger(SingleFlightTest.class);

    private final TimeSource time = TimeSource.system();

    @Test
    void concurrentRequestsForSameKeyTriggerLoaderOnlyOnce() throws Exception {
        SingleFlight<String> sf = new SingleFlight<>(50, 5000, time);
        AtomicInteger loaderInvocations = new AtomicInteger();
        CountDownLatch insideLoader = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(20);
        try {
            List<Future<String>> futures = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                futures.add(pool.submit(() -> sf.call("hot", () -> {
                    loaderInvocations.incrementAndGet();
                    insideLoader.countDown();
                    try {
                        releaseLoader.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    return "shared-value";
                }, r -> { })));
            }

            assertThat(insideLoader.await(2, TimeUnit.SECONDS)).isTrue();
            // loader 尚未返回，20 个请求都已在组内，此刻上游只进入 1 次
            log.info("[sf-test] 20 并发同 key, loader 实际进入次数={} 期望 1",
                    loaderInvocations.get());
            assertThat(loaderInvocations.get()).isEqualTo(1);

            releaseLoader.countDown();
            for (Future<String> f : futures) {
                assertThat(f.get(2, TimeUnit.SECONDS)).isEqualTo("shared-value");
            }
            log.info("[sf-test] 全部 20 个请求共享同一结果，判定依据=单飞 CompletableFuture 合并");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void requestsBeyondPerKeyCapacityAreImmediatelyRejected() throws Exception {
        int capacity = 5;
        SingleFlight<String> sf = new SingleFlight<>(capacity, 5000, time);
        CountDownLatch insideLoader = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        AtomicInteger rejected = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(20);
        try {
            // 先占满 5 个名额（1 leader + 4 waiter）
            List<Future<?>> holders = new ArrayList<>();
            for (int i = 0; i < capacity; i++) {
                holders.add(pool.submit(() -> sf.call("k", () -> {
                    insideLoader.countDown();
                    try {
                        releaseLoader.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    return "v";
                }, r -> rejected.incrementAndGet())));
            }
            assertThat(insideLoader.await(2, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(100); // 确保 4 个 waiter 都已入组

            // 第 6 个请求必须立即被确定性拒绝
            assertThatCode(() -> sf.call("k", () -> "never", r -> rejected.incrementAndGet()))
                    .isInstanceOf(RequestRejectedException.class)
                    .satisfies(ex -> assertThat(((RequestRejectedException) ex).reason())
                            .isEqualTo(RejectReason.PER_KEY_OVERLOADED));
            log.info("[sf-test] 容量={} 已满, 第 6 个请求判定=PER_KEY_OVERLOADED, 拒绝计数={}",
                    capacity, rejected.get());
            assertThat(rejected.get()).isEqualTo(1);

            releaseLoader.countDown();
            for (Future<?> f : holders) {
                f.get(2, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void waiterExceedingAcquireTimeoutGetsQueueTimeout() throws Exception {
        // acquireTimeout=50ms；loader 持续 500ms
        SingleFlight<String> sf = new SingleFlight<>(50, 50, time);
        CountDownLatch leaderStarted = new CountDownLatch(1);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<?> leader = pool.submit(() -> sf.call("k", () -> {
                leaderStarted.countDown();
                Thread.sleep(500);
                return "late";
            }, r -> { }));

            assertThatCode(() -> {
                assertThat(leaderStarted.await(2, TimeUnit.SECONDS)).isTrue();
                Thread.sleep(80); // 保证 leader 已先入组
                sf.call("k", () -> "x", r -> { });
            }).isInstanceOfSatisfying(RequestRejectedException.class,
                    ex -> assertThat(ex.reason()).isEqualTo(RejectReason.QUEUE_TIMEOUT));
            log.info("[sf-test] 排队超过 50ms -> 判定 QUEUE_TIMEOUT（不无限堆积）");
            // leader 最终仍正常完成，其结果不受超时 waiter 影响
            assertThat(leader.get(2, TimeUnit.SECONDS)).isEqualTo("late");
        } finally {
            pool.shutdownNow();
        }
    }
}
