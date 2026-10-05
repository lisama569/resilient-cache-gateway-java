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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 本轮修复的关键场景：旧结果/保护状态与容量保护、熔断器之间的相互影响。
 *
 * <ul>
 *   <li>宽限窗口内的旧结果不因容量占满或熔断保护期而变成拒绝；</li>
 *   <li>未真正打到上游的后台更新（被容量挡回/保护期跳过）不计入连续失败；</li>
 *   <li>真正打到上游且失败的后台更新仍然按连续失败计入，达到上限落回兜底；</li>
 *   <li>试探请求被容量保护挡回时归还试探名额，熔断器不会卡在试探期。</li>
 * </ul>
 * 每个用例打印所用配置与判定依据。
 */
class CacheGatewayProtectionStateTest {

    private static final Logger log = LoggerFactory.getLogger(CacheGatewayProtectionStateTest.class);

    private GatewayProperties properties;
    private SimulatedUpstream upstream;
    private GatewayMetrics metrics;
    private CacheGatewayService service;

    @BeforeEach
    void setUp() {
        properties = new GatewayProperties();
        properties.setCacheTtlMillis(300);
        properties.setUpstreamTimeoutMillis(3000);
        properties.setMaxConcurrentPerKey(32);
        properties.setSimulatedLatencyMillis(20);
        properties.setSimulatedFailure(false);
        upstream = new SimulatedUpstream(properties);
        metrics = new GatewayMetrics();
        service = new CacheGatewayService(properties, upstream, metrics);
    }

    // ---------- 宽限窗口旧结果 vs 全局容量占满 ----------

    @Test
    void graceWindowStaleAndCacheHitSurviveCapacityFull() throws Exception {
        properties.setCacheTtlMillis(300);
        properties.setSwrGraceMillis(5000);
        properties.setSwrMaxBackgroundFailures(2); // 上限放低：若被跳过也算失败，2轮后就会恶化成拒绝
        properties.setMaxConcurrentUpstream(1);
        properties.setMaxQueueSize(0); // 容量一满，任何真实回源立即总量超限
        properties.setQueueWaitTimeoutMillis(50);
        log.info("[case] 容量占满时旧结果与缓存命中不受影响: ttlMs={} graceMs={} maxBgFailures={} "
                        + "maxConcurrentUpstream={} maxQueueSize={} 判定依据=许可被占期间缓存命中仍FRESH, "
                        + "窗口内反复请求仍STALE_REFRESHING不抛拒绝, 且后台更新被跳过不计失败",
                properties.getCacheTtlMillis(), properties.getSwrGraceMillis(),
                properties.getSwrMaxBackgroundFailures(), properties.getMaxConcurrentUpstream(),
                properties.getMaxQueueSize());

        GatewayResponse graceSeed = service.get("grace-key");
        assertEquals(ResultOrigin.FRESH, graceSeed.origin());
        Thread.sleep(400); // grace-key 进入宽限窗口 [300, 5300)

        GatewayResponse freshSeed = service.get("fresh-key");
        assertEquals(ResultOrigin.FRESH, freshSeed.origin());

        // 慢请求占住唯一全局许可，模拟服务整体容量被占满
        properties.setSimulatedLatencyMillis(800);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<GatewayResponse> holder = pool.submit(() -> service.get("slow-key"));
        waitFor(() -> admissionInFlight() == 1, 2000);
        log.info("[input] 全局许可已被 slow-key 占住: admission.inFlight={}", admissionInFlight());

        // 容量被占期间：缓存命中照常返回，不得被拒绝
        GatewayResponse hit = service.get("fresh-key");
        log.info("[result] cache-hit under full capacity: origin={} (期望FRESH)", hit.origin());
        assertEquals(ResultOrigin.FRESH, hit.origin());
        assertEquals(freshSeed.value(), hit.value());

        // 容量被占期间：窗口内反复请求都拿到旧结果，后台更新被挡回不得让旧结果失效
        for (int i = 1; i <= 5; i++) {
            GatewayResponse r = service.get("grace-key");
            log.info("[result] grace round {}: origin={} value={} (期望STALE_REFRESHING旧值, 不抛拒绝)",
                    i, r.origin(), r.value());
            assertEquals(ResultOrigin.STALE_REFRESHING, r.origin(),
                    "容量占满不得把宽限窗口内的旧结果变成拒绝");
            assertEquals(graceSeed.value(), r.value());
            Thread.sleep(60); // 留给后台更新（被容量挡回）完成的时间
        }
        log.info("[result] skipped={} bgFailures={} (期望skipped>=1且bgFailures=0: 没打出去不算失败)",
                skippedCount(), bgFailureCount());
        assertTrue(skippedCount() >= 1, "容量占满时后台更新应被跳过并有记录");
        assertEquals(0, bgFailureCount(), "未真正打到上游的更新不得计入连续失败");

        holder.get(5, TimeUnit.SECONDS);
        pool.shutdown();
    }

    // ---------- 宽限窗口旧结果 vs 熔断保护期 ----------

    @Test
    void graceWindowStaleSurvivesCircuitOpen() throws Exception {
        properties.setCacheTtlMillis(300);
        properties.setSwrGraceMillis(5000);
        properties.setSwrMaxBackgroundFailures(2);
        properties.setCircuitFailureThreshold(2);
        properties.setCircuitOpenMillis(1500);
        log.info("[case] 保护期内窗口旧结果不受影响: ttlMs={} graceMs={} maxBgFailures={} threshold={} "
                        + "openMs={} 判定依据=保护期内窗口内反复请求仍STALE_REFRESHING, 上游调用数不增加, "
                        + "被跳过的后台更新不计失败",
                properties.getCacheTtlMillis(), properties.getSwrGraceMillis(),
                properties.getSwrMaxBackgroundFailures(), properties.getCircuitFailureThreshold(),
                properties.getCircuitOpenMillis());

        GatewayResponse seed = service.get("grace-key");
        assertEquals(ResultOrigin.FRESH, seed.origin());
        Thread.sleep(400); // 进入宽限窗口

        // 用其他 key 的连续失败把熔断器打进保护期
        properties.setSimulatedFailure(true);
        assertEquals(ResultOrigin.DEGRADED, service.get("cb-1").origin());
        assertEquals(ResultOrigin.DEGRADED, service.get("cb-2").origin());
        assertEquals("OPEN", circuitState());
        properties.setSimulatedFailure(false); // 上游已恢复：后续打不出去纯粹因为保护期
        long callsAtOpen = upstream.getCallCount();
        log.info("[input] circuit OPEN, upstreamCalls={} (上游已恢复健康)", callsAtOpen);

        for (int i = 1; i <= 5; i++) {
            GatewayResponse r = service.get("grace-key");
            log.info("[result] grace round {}: origin={} (期望STALE_REFRESHING, 不抛拒绝)", i, r.origin());
            assertEquals(ResultOrigin.STALE_REFRESHING, r.origin(),
                    "保护期不得把宽限窗口内的旧结果变成拒绝");
            assertEquals(seed.value(), r.value());
            Thread.sleep(60);
        }
        log.info("[result] state={} upstreamCalls={} (未增加) skipped={} bgFailures={}",
                circuitState(), upstream.getCallCount(), skippedCount(), bgFailureCount());
        assertEquals("OPEN", circuitState(), "保护期未结束，状态应仍为OPEN");
        assertEquals(callsAtOpen, upstream.getCallCount(), "保护期内后台更新不得真正打到上游");
        assertTrue(skippedCount() >= 1, "保护期跳过的后台更新应有记录");
        assertEquals(0, bgFailureCount(), "保护期跳过的更新不得计入连续失败");
    }

    // ---------- 跳过的更新 vs 真正失败的上游更新 ----------

    @Test
    void skippedRefreshDoesNotCountButRealUpstreamFailureDoes() throws Exception {
        properties.setCacheTtlMillis(200);
        properties.setSwrGraceMillis(30000); // 窗口足够长，只看失败计数语义
        properties.setSwrMaxBackgroundFailures(2);
        properties.setMaxConcurrentUpstream(1);
        properties.setMaxQueueSize(0);
        properties.setQueueWaitTimeoutMillis(50);
        log.info("[case] 跳过不计失败/真失败才计: ttlMs={} graceMs={} maxBgFailures={} "
                        + "判定依据=容量占满时被跳过的更新不计数(旧结果一直可用), 容量恢复后上游真失败才计数, "
                        + "满{}次落回STALE_FALLBACK",
                properties.getCacheTtlMillis(), properties.getSwrGraceMillis(),
                properties.getSwrMaxBackgroundFailures(), properties.getSwrMaxBackgroundFailures());

        GatewayResponse seed = service.get("mix-key");
        assertEquals(ResultOrigin.FRESH, seed.origin());
        Thread.sleep(300); // 进入宽限窗口

        // 阶段一：容量被占满，后台更新打不出去 -> 跳过，不计失败
        properties.setSimulatedLatencyMillis(800);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<GatewayResponse> holder = pool.submit(() -> service.get("slow-key"));
        waitFor(() -> admissionInFlight() == 1, 2000);
        for (int i = 1; i <= 3; i++) {
            GatewayResponse r = service.get("mix-key");
            assertEquals(ResultOrigin.STALE_REFRESHING, r.origin(),
                    "容量占满时窗口内必须继续返回旧结果");
            Thread.sleep(60);
        }
        waitFor(() -> skippedCount() >= 1, 2000);
        log.info("[result] 阶段一(容量占满): skipped={} bgFailures={} (期望skipped>=1, bgFailures=0)",
                skippedCount(), bgFailureCount());
        assertEquals(0, bgFailureCount(), "没真正打出去的更新不得计入连续失败");
        holder.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        // 阶段二：容量恢复但上游真故障 -> 真正打到上游且失败，按连续失败计数
        properties.setSimulatedLatencyMillis(20);
        properties.setSimulatedFailure(true);
        GatewayResponse r1 = service.get("mix-key"); // 触发真失败#1
        assertEquals(ResultOrigin.STALE_REFRESHING, r1.origin());
        waitFor(() -> bgFailureCount() >= 1, 2000);
        GatewayResponse r2 = service.get("mix-key"); // 失败1次 < 上限2，仍给旧结果，触发真失败#2
        log.info("[result] 阶段二 bgFailures=1: origin={} (期望STALE_REFRESHING)", r2.origin());
        assertEquals(ResultOrigin.STALE_REFRESHING, r2.origin());
        waitFor(() -> bgFailureCount() >= 2, 2000);
        GatewayResponse r3 = service.get("mix-key"); // 真失败达上限 -> 落回兜底
        log.info("[result] 阶段二 bgFailures=2: origin={} value={} (期望STALE_FALLBACK旧值)",
                r3.origin(), r3.value());
        assertEquals(ResultOrigin.STALE_FALLBACK, r3.origin(),
                "真正打到上游且连续失败达上限后才落回兜底");
        assertEquals(seed.value(), r3.value());
    }

    // ---------- 试探名额泄漏：容量挡回不得卡死熔断器 ----------

    @Test
    void halfOpenProbeSlotReleasedWhenAdmissionRejects() throws Exception {
        properties.setCircuitFailureThreshold(1);
        properties.setCircuitOpenMillis(300);
        properties.setCircuitHalfOpenProbes(1);
        properties.setMaxConcurrentUpstream(1);
        properties.setMaxQueueSize(0);
        properties.setQueueWaitTimeoutMillis(50);
        properties.setSimulatedFailure(true);
        log.info("[case] 试探被容量挡回不卡死: threshold={} openMs={} probes={} 判定依据=试探请求被容量挡回后"
                        + "名额被归还(probesInFlight=0, 状态仍HALF_OPEN), 容量恢复后下一个试探能正常打出并恢复CLOSED",
                properties.getCircuitFailureThreshold(), properties.getCircuitOpenMillis(),
                properties.getCircuitHalfOpenProbes());

        GatewayResponse first = service.get("cb-key");
        assertEquals(ResultOrigin.DEGRADED, first.origin());
        assertEquals("OPEN", circuitState());

        // 模拟容量被持续占满（上限调为0：任何回源都立即总量超限）
        properties.setMaxConcurrentUpstream(0);
        properties.setSimulatedFailure(false); // 上游已恢复健康
        Thread.sleep(400); // 保护期结束 -> 试探期

        // 试探请求拿到熔断许可但被容量挡回：按总量超限拒绝，且必须归还试探名额
        GlobalAdmissionLimiter.CapacityExceededException rejected = assertThrows(
                GlobalAdmissionLimiter.CapacityExceededException.class, () -> service.get("probe-key"));
        log.info("[result] probe rejected by capacity: {} state={} probesInFlight={} "
                        + "(期望拒绝=CAPACITY_EXCEEDED, 状态仍HALF_OPEN, 名额已归还=0)",
                rejected.getMessage(), circuitState(), probesInFlight());
        assertEquals("HALF_OPEN", circuitState(), "试探未真正打到上游，不得当作试探失败重新开路");
        assertEquals(0, probesInFlight(), "被容量挡回的试探必须归还名额，不得泄漏卡死");

        // 容量恢复：下一个试探必须能正常放行并恢复（修复前这里会因名额泄漏永远卡在HALF_OPEN）
        properties.setMaxConcurrentUpstream(1);
        GatewayResponse probe = service.get("probe-key-2");
        log.info("[result] probe after capacity recovered: origin={} state={} (期望FRESH+CLOSED)",
                probe.origin(), circuitState());
        assertEquals(ResultOrigin.FRESH, probe.origin(), "试探名额未泄漏，新试探应能打到上游");
        assertEquals("CLOSED", circuitState(), "试探成功必须恢复CLOSED，不得卡在试探期");
    }

    // ---------- 工具 ----------

    private String circuitState() {
        return String.valueOf(((Map<?, ?>) metrics.snapshot().get("circuit")).get("state"));
    }

    private int probesInFlight() {
        return ((Number) ((Map<?, ?>) metrics.snapshot().get("circuit")).get("probesInFlight")).intValue();
    }

    private int admissionInFlight() {
        return ((Number) ((Map<?, ?>) metrics.snapshot().get("admission")).get("inFlight")).intValue();
    }

    private long skippedCount() {
        return ((Number) metrics.snapshot().get("backgroundRefreshSkipped")).longValue();
    }

    private long bgFailureCount() {
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
