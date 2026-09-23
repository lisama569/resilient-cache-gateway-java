package com.github.lisama569.resilientcachegatewayjava.service;

import com.github.lisama569.resilientcachegatewayjava.cache.CacheEntryStore;
import com.github.lisama569.resilientcachegatewayjava.config.GatewayProperties;
import com.github.lisama569.resilientcachegatewayjava.core.TimeSource;
import com.github.lisama569.resilientcachegatewayjava.exception.RequestRejectedException;
import com.github.lisama569.resilientcachegatewayjava.exception.UpstreamException;
import com.github.lisama569.resilientcachegatewayjava.metrics.MetricsRegistry;
import com.github.lisama569.resilientcachegatewayjava.model.CacheEntry;
import com.github.lisama569.resilientcachegatewayjava.model.RejectReason;
import com.github.lisama569.resilientcachegatewayjava.model.ResultStatus;
import com.github.lisama569.resilientcachegatewayjava.model.TargetResult;
import com.github.lisama569.resilientcachegatewayjava.model.UpstreamResponse;
import com.github.lisama569.resilientcachegatewayjava.resilience.CircuitBreaker;
import com.github.lisama569.resilientcachegatewayjava.resilience.CircuitBreakerRegistry;
import com.github.lisama569.resilientcachegatewayjava.resilience.SingleFlight;
import com.github.lisama569.resilientcachegatewayjava.upstream.UpstreamClient;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 网关核心编排，单次请求的判定顺序固定且可解释：
 *
 *  1. 查缓存：新鲜（age &lt; ttl）-> 直接 FRESH，绝不触发上游；
 *  2. 过期或缺失 -> 进入单飞合并（同一目标仅 1 次上游读取）；
 *  3. 加载前依次检查：熔断器（CIRCUIT_OPEN）、全局并发舱壁（GLOBAL_OVERLOADED）；
 *  4. 上游调用带 upstreamTimeoutMillis 硬超时（UPSTREAM_TIMEOUT）；
 *  5. 加载失败/超时/熔断时：若存在 staleTtl 窗口内的旧值 -> STALE_FALLBACK；
 *     否则 -> DEGRADED 或显式拒绝异常，绝不返回过期内容冒充新鲜数据。
 */
public class CacheGatewayService {

    private static final Logger log = LoggerFactory.getLogger(CacheGatewayService.class);

    private final CacheEntryStore store;
    private final UpstreamClient upstream;
    private final GatewayProperties props;
    private final TimeSource timeSource;
    private final CircuitBreakerRegistry circuits;
    private final MetricsRegistry metrics;
    private final SingleFlight<LoadOutcome> singleFlight;
    private final Semaphore globalInflight;
    private final ExecutorService upstreamPool;

    public CacheGatewayService(CacheEntryStore store, UpstreamClient upstream,
                               GatewayProperties props, TimeSource timeSource,
                               CircuitBreakerRegistry circuits, MetricsRegistry metrics) {
        this.store = store;
        this.upstream = upstream;
        this.props = props;
        this.timeSource = timeSource;
        this.circuits = circuits;
        this.metrics = metrics;
        this.singleFlight = new SingleFlight<>(props.getPerKeyMaxConcurrent(),
                props.getAcquireTimeoutMillis(), timeSource);
        this.globalInflight = new Semaphore(props.getGlobalMaxConcurrent());
        this.upstreamPool = Executors.newFixedThreadPool(
                Math.max(1, props.getUpstreamPoolSize()));
    }

    /**
     * 读取目标结果。成功路径与兜底路径返回带明确 {@link ResultStatus} 的结果；
     * 过载/排队超时/熔断且无兜底时抛 {@link RequestRejectedException} 显式拒绝。
     */
    public TargetResult fetch(String target) {
        long startNanos = timeSource.nowNanos();
        long now = timeSource.nowMillis();

        Optional<CacheEntry> cached = store.get(target);
        if (cached.isPresent()) {
            CacheEntry entry = cached.get();
            if (entry.isFresh(now, props.getTtlMillis())) {
                log.info("[gateway] target={} decision=FRESH fromCache ageMs={}",
                        target, now - entry.createdAt());
                return finish(target, ResultStatus.FRESH, entry.value(),
                        "cache hit within ttl", entry.createdAt(), startNanos);
            }
            log.info("[gateway] target={} cache STALE ageMs={} ttlMs={} -> refresh required",
                    target, now - entry.createdAt(), props.getTtlMillis());
        }

        LoadOutcome outcome;
        try {
            outcome = singleFlight.call(target, () -> load(target),
                    reason -> metrics.recordRejected(target, reason));
        } catch (RequestRejectedException ex) {
            // 过载/排队超时：有过期可用值则兜底，否则维持显式拒绝
            TargetResult fallback = staleOrDegraded(target, now, startNanos, ex.reason(),
                    "rejected before upstream: " + ex.getMessage());
            if (fallback != null) {
                return fallback;
            }
            throw ex;
        }

        if (outcome.success()) {
            return finish(target, ResultStatus.FRESH, outcome.value(),
                    "loaded from upstream", outcome.fetchedAt(), startNanos);
        }

        TargetResult fallback = staleOrDegraded(target, now, startNanos,
                rejectReasonOf(outcome.errorReason()), outcome.errorReason());
        if (fallback != null) {
            return fallback;
        }

        // 无缓存可兜底：熔断/超时/失败转为显式拒绝，由 Web 层翻译成确定的 HTTP 状态
        RejectReason reason = rejectReasonOf(outcome.errorReason());
        metrics.recordRejected(target, reason);
        throw new RequestRejectedException(target, reason, outcome.errorReason());
    }

    /**
     * 单飞 leader 执行的实际加载：二次检查缓存 -> 熔断 -> 全局舱壁 -> 超时控制 -> 上游。
     * 任何失败都封装为 LoadOutcome.fail 共享给同组等待者，不向上抛异常。
     */
    private LoadOutcome load(String target) {
        long now = timeSource.nowMillis();

        // 二次检查：可能在排队期间已被别的请求（上一轮 leader）刷新
        Optional<CacheEntry> recheck = store.get(target);
        if (recheck.isPresent() && recheck.get().isFresh(now, props.getTtlMillis())) {
            CacheEntry e = recheck.get();
            log.info("[gateway] target={} leader recheck FRESH, skip upstream", target);
            return LoadOutcome.ok(e.value(), e.createdAt());
        }

        CircuitBreaker breaker = circuits.forTarget(target);
        if (!breaker.allowRequest(now)) {
            String msg = "CIRCUIT_OPEN: circuit breaker open for target=" + target;
            log.info("[gateway] target={} decision=BLOCKED reason=CIRCUIT_OPEN", target);
            return LoadOutcome.fail(msg);
        }

        if (!globalInflight.tryAcquire()) {
            // 未真正发出调用，不消耗熔断器失败计数
            String msg = "GLOBAL_OVERLOADED: global upstream concurrency limit "
                    + props.getGlobalMaxConcurrent() + " reached for target=" + target;
            log.info("[gateway] target={} decision=BLOCKED reason=GLOBAL_OVERLOADED", target);
            return LoadOutcome.fail(msg);
        }

        metrics.recordUpstreamCall();
        Future<UpstreamResponse> future = upstreamPool.submit(() -> upstream.fetch(target));
        try {
            UpstreamResponse resp = future.get(props.getUpstreamTimeoutMillis(), TimeUnit.MILLISECONDS);
            breaker.recordSuccess();
            long fetchedAt = timeSource.nowMillis();
            store.put(target, new CacheEntry(resp.value(), fetchedAt));
            log.info("[gateway] target={} upstream OK fetchedAt={} -> cache refreshed",
                    target, fetchedAt);
            return LoadOutcome.ok(resp.value(), fetchedAt);
        } catch (TimeoutException ex) {
            future.cancel(true); // 中断仍在睡眠的模拟调用，尽快释放线程
            breaker.recordFailure(timeSource.nowMillis());
            metrics.recordUpstreamTimeout();
            metrics.recordUpstreamError();
            log.info("[gateway] target={} upstream TIMEOUT afterMs={}",
                    target, props.getUpstreamTimeoutMillis());
            return LoadOutcome.fail("UPSTREAM_TIMEOUT: upstream timeout after "
                    + props.getUpstreamTimeoutMillis() + "ms for target=" + target);
        } catch (Exception ex) {
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            breaker.recordFailure(timeSource.nowMillis());
            metrics.recordUpstreamError();
            boolean businessFailure = cause instanceof UpstreamException;
            log.info("[gateway] target={} upstream FAILURE type={} message={}",
                    target, cause.getClass().getSimpleName(), cause.getMessage());
            return LoadOutcome.fail((businessFailure ? "upstream failure: " : "upstream error: ")
                    + cause.getMessage());
        } finally {
            globalInflight.release();
        }
    }

    /**
     * 失败后的统一兜底：staleTtl 窗口内的旧值 -> STALE_FALLBACK；
     * 超过窗口或无旧值：若原因是熔断/超时/失败且没有过期数据 -> null（调用方决定拒绝），
     * 由调用方保证不会把任何过期内容当 FRESH 返回。
     */
    private TargetResult staleOrDegraded(String target, long now, long startNanos,
                                         RejectReason reason, String detail) {
        if (props.isStaleOnFailureEnabled()) {
            Optional<CacheEntry> cached = store.get(target);
            if (cached.isPresent()) {
                CacheEntry entry = cached.get();
                if (entry.isStaleUsable(now, props.getTtlMillis(), props.getStaleTtlMillis())) {
                    long age = now - entry.createdAt();
                    log.info("[gateway] target={} decision=STALE_FALLBACK reason={} staleAgeMs={}",
                            target, reason, age);
                    return finish(target, ResultStatus.STALE_FALLBACK, entry.value(),
                            "served stale while upstream unavailable: " + detail,
                            entry.createdAt(), startNanos);
                }
                if (entry.isExpiredBeyondStale(now, props.getTtlMillis(), props.getStaleTtlMillis())) {
                    log.info("[gateway] target={} stale EXPIRED beyond staleTtl, cannot fallback",
                            target);
                }
            }
        }

        // 过载类保护（每热点/全局/排队超时）不返回伪数据，直接拒绝
        if (reason == RejectReason.PER_KEY_OVERLOADED
                || reason == RejectReason.GLOBAL_OVERLOADED
                || reason == RejectReason.QUEUE_TIMEOUT) {
            metrics.recordRejected(target, reason);
            return null;
        }

        // 熔断打开属于保护式快速失败（本次并未真正尝试上游）：无旧值时显式拒绝，
        // 与"尝试后失败 -> DEGRADED"明确区分，且绝不静默丢失。
        if (reason == RejectReason.CIRCUIT_OPEN) {
            metrics.recordRejected(target, reason);
            return null;
        }

        // 真正尝试上游后失败/超时且完全无缓存：返回明确标记的降级结果（默认空值）
        log.info("[gateway] target={} decision=DEGRADED reason={}", target, reason);
        return finish(target, ResultStatus.DEGRADED, null,
                "no cached value available; " + detail, 0L, startNanos);
    }

    private TargetResult finish(String target, ResultStatus status, String value,
                                String reason, long upstreamAt, long startNanos) {
        long elapsed = timeSource.nowNanos() - startNanos;
        metrics.recordRequest(target, status, elapsed);
        return new TargetResult(target, status, value, reason, upstreamAt, elapsed);
    }

    private static RejectReason rejectReasonOf(String errorReason) {
        if (errorReason == null) {
            return RejectReason.UPSTREAM_FAILURE;
        }
        if (errorReason.startsWith("UPSTREAM_TIMEOUT")) {
            return RejectReason.UPSTREAM_TIMEOUT;
        }
        if (errorReason.startsWith("CIRCUIT_OPEN")) {
            return RejectReason.CIRCUIT_OPEN;
        }
        if (errorReason.startsWith("GLOBAL_OVERLOADED")) {
            return RejectReason.GLOBAL_OVERLOADED;
        }
        return RejectReason.UPSTREAM_FAILURE;
    }

    @PreDestroy
    public void shutdown() {
        upstreamPool.shutdownNow();
    }

    /**
     * 单飞加载动作的内部产出（成功值或失败原因），不向上抛异常，
     * 使同组所有等待者拿到完全一致的判定。
     */
    record LoadOutcome(boolean success, String value, long fetchedAt, String errorReason) {
        static LoadOutcome ok(String value, long fetchedAt) {
            return new LoadOutcome(true, value, fetchedAt, null);
        }

        static LoadOutcome fail(String reason) {
            return new LoadOutcome(false, null, 0L, reason);
        }
    }
}
