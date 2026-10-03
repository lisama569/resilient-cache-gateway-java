package com.github.lisama569.resilientcachegatewayjava.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 缓存聚合网关核心服务。
 *
 * <ul>
 *   <li>TTL 缓存：有效期内直接返回新鲜结果，过期后重新回源，绝不把过期内容当新鲜返回。</li>
 *   <li>宽限窗口（stale-while-revalidate）：过期后 {@code staleGraceMillis} 内返回旧值
 *       （{@code STALE_REFRESHING}）并只触发一次后台更新；窗口用完或后台更新连续失败
 *       达到 {@code staleMaxRefreshFailures} 后落回现有兜底分类。</li>
 *   <li>单飞合并：同一 key 需要同步回源时只放行一个请求，其余请求共享其结果。</li>
 *   <li>超时与降级：上游超时或失败时，有过期缓存则返回过期兜底，否则返回降级结果。</li>
 *   <li>并发保护：每个 key 一个信号量，超出上限的请求按确定规则直接拒绝。</li>
 *   <li>全局容量保护：真正访问上游前经过全局有界排队守卫，缓存命中与合并等待不经过。</li>
 *   <li>熔断：上游连续失败达到阈值后进入保护期，期间直接走兜底；保护期后少量试探。</li>
 * </ul>
 */
@Service
public class CacheGatewayService {

    private static final Logger log = LoggerFactory.getLogger(CacheGatewayService.class);

    /** 合并等待者比上游超时多等的宽限时间，避免赢家尚未写完结果就超时。 */
    private static final long JOIN_GRACE_MILLIS = 1000;

    private final GatewayProperties properties;
    private final SimulatedUpstream upstream;
    private final GatewayMetrics metrics;
    private final CircuitBreaker circuitBreaker;
    private final GlobalAdmissionGuard globalGuard;

    private final Map<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<GatewayResponse>> inflight = new ConcurrentHashMap<>();
    private final Map<String, Semaphore> keyGuards = new ConcurrentHashMap<>();
    /** 每个 key 正在进行中的后台更新（存在即表示已有一次，不重复触发）。 */
    private final Map<String, CompletableFuture<Void>> refreshing = new ConcurrentHashMap<>();
    /** 每个 key 的后台更新连续失败次数。 */
    private final Map<String, AtomicInteger> refreshFailures = new ConcurrentHashMap<>();
    private final ExecutorService upstreamExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public CacheGatewayService(GatewayProperties properties,
                               SimulatedUpstream upstream,
                               GatewayMetrics metrics,
                               CircuitBreaker circuitBreaker,
                               GlobalAdmissionGuard globalGuard) {
        this.properties = properties;
        this.upstream = upstream;
        this.metrics = metrics;
        this.circuitBreaker = circuitBreaker;
        this.globalGuard = globalGuard;
    }

    /** 缓存条目（不可变）。 */
    private record CacheEntry(String value, Instant fetchedAt) {
        boolean isFresh(long ttlMillis) {
            return fetchedAt.plusMillis(ttlMillis).isAfter(Instant.now());
        }

        /** 是否仍在宽限窗口内（过期但未满 ttl + grace）。边界：恰好等于 ttl+grace 时不在窗口内。 */
        boolean withinGrace(long ttlMillis, long graceMillis) {
            return fetchedAt.plusMillis(ttlMillis + graceMillis).isAfter(Instant.now());
        }
    }

    /** 并发超限拒绝异常。 */
    public static class OverloadRejectedException extends RuntimeException {
        public OverloadRejectedException(String key) {
            super("too many concurrent requests for key=" + key);
        }
    }

    /** 熔断保护期内访问上游被拒绝（内部兜底用）。 */
    private static class CircuitOpenException extends RuntimeException {
        CircuitOpenException(String message) {
            super(message);
        }
    }

    /**
     * 读取一个 key 的数据，带缓存、宽限窗口、合并、降级与多重并发保护。
     *
     * @throws OverloadRejectedException 该 key 并发请求超过上限时抛出（确定且一致的拒绝规则）
     * @throws GlobalAdmissionGuard.QueueTimeoutRejectedException 全局排队等待超时
     * @throws GlobalAdmissionGuard.QueueFullRejectedException    全局等待队列已满
     */
    public GatewayResponse get(String key) {
        long startNanos = System.nanoTime();
        metrics.recordTotal();
        Semaphore guard = keyGuards.computeIfAbsent(key,
                k -> new Semaphore(properties.getMaxConcurrentPerKey(), true));
        if (!guard.tryAcquire()) {
            metrics.recordRejected();
            log.info("[gateway] REJECTED key={} reason=overload maxConcurrentPerKey={}",
                    key, properties.getMaxConcurrentPerKey());
            throw new OverloadRejectedException(key);
        }
        try {
            GatewayResponse response = doGet(key, startNanos);
            long elapsed = elapsedMillis(startNanos);
            metrics.recordLatency(elapsed);
            log.info("[gateway] key={} origin={} elapsedMs={}", key, response.origin(), elapsed);
            return response;
        } finally {
            guard.release();
        }
    }

    private GatewayResponse doGet(String key, long startNanos) {
        CacheEntry cached = cache.get(key);
        if (cached != null && cached.isFresh(properties.getCacheTtlMillis())) {
            metrics.recordFresh();
            return new GatewayResponse(key, cached.value(), ResultOrigin.FRESH,
                    cached.fetchedAt(), elapsedMillis(startNanos));
        }

        // 宽限窗口：过期但未超 ttl+grace，且后台更新连续失败未达上限 →
        // 返回旧值（STALE_REFRESHING）并确保只有一次后台更新在进行。
        if (cached != null
                && cached.withinGrace(properties.getCacheTtlMillis(), properties.getStaleGraceMillis())
                && refreshFailureCount(key) < properties.getStaleMaxRefreshFailures()) {
            triggerBackgroundRefresh(key);
            metrics.recordStaleRefreshing();
            log.info("[gateway] key={} STALE_REFRESHING fetchedAt={} refreshFailures={}",
                    key, cached.fetchedAt(), refreshFailureCount(key));
            return new GatewayResponse(key, cached.value(), ResultOrigin.STALE_REFRESHING,
                    cached.fetchedAt(), elapsedMillis(startNanos));
        }

        // 未命中、窗口用完或后台更新失败超限：单飞合并，同一时刻只允许一个同步回源。
        CompletableFuture<GatewayResponse> future = new CompletableFuture<>();
        CompletableFuture<GatewayResponse> existing = inflight.putIfAbsent(key, future);
        if (existing != null) {
            metrics.recordCoalesced();
            log.info("[gateway] key={} coalesced into in-flight upstream call", key);
            return join(key, existing, cached, startNanos);
        }
        try {
            GatewayResponse response = fetchFromUpstream(key, cached, startNanos);
            future.complete(response);
            return response;
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
            throw e;
        } finally {
            inflight.remove(key, future);
        }
    }

    /** 合并等待者：共享赢家结果；赢家异常时按同样规则兜底。 */
    private GatewayResponse join(String key, CompletableFuture<GatewayResponse> future,
                                 CacheEntry cached, long startNanos) {
        try {
            return future.get(properties.getUpstreamTimeoutMillis() + JOIN_GRACE_MILLIS,
                    TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            return fallback(key, cached, startNanos, e.getCause());
        } catch (TimeoutException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return fallback(key, cached, startNanos, e);
        }
    }

    /**
     * 触发一次后台更新：同一 key 同一时刻只有一次；熔断保护期内或全局队列
     * 拒绝时跳过（不计失败）；上游失败计入该 key 的连续失败次数。
     */
    private void triggerBackgroundRefresh(String key) {
        CompletableFuture<Void> marker = new CompletableFuture<>();
        if (refreshing.putIfAbsent(key, marker) != null) {
            return; // 已有一次后台更新进行中，不重复触发
        }
        metrics.recordBackgroundRefresh();
        log.info("[gateway] key={} background refresh triggered", key);
        upstreamExecutor.submit(() -> {
            try {
                if (!circuitBreaker.tryAcquire()) {
                    log.info("[gateway] key={} background refresh skipped: circuit {}",
                            key, circuitBreaker.state());
                    return;
                }
                try {
                    globalGuard.acquire();
                } catch (RuntimeException e) {
                    log.info("[gateway] key={} background refresh skipped: {}", key, e.getMessage());
                    return;
                }
                try {
                    Future<String> task = upstreamExecutor.submit(() -> upstream.fetch(key));
                    String value;
                    try {
                        value = task.get(properties.getUpstreamTimeoutMillis(), TimeUnit.MILLISECONDS);
                    } catch (TimeoutException e) {
                        task.cancel(true);
                        throw e;
                    }
                    cache.put(key, new CacheEntry(value, Instant.now()));
                    refreshFailures.remove(key);
                    circuitBreaker.onSuccess();
                    metrics.recordBackgroundRefreshSuccess();
                    log.info("[gateway] key={} background refresh SUCCESS", key);
                } catch (TimeoutException e) {
                    onRefreshFailure(key, "timeout after " + properties.getUpstreamTimeoutMillis() + "ms");
                } catch (ExecutionException e) {
                    onRefreshFailure(key, String.valueOf(e.getCause().getMessage()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    onRefreshFailure(key, "interrupted");
                } finally {
                    globalGuard.release();
                }
            } finally {
                refreshing.remove(key, marker);
            }
        });
    }

    private void onRefreshFailure(String key, String cause) {
        int failures = refreshFailures.computeIfAbsent(key, k -> new AtomicInteger())
                .incrementAndGet();
        circuitBreaker.onFailure();
        metrics.recordBackgroundRefreshFailure();
        log.warn("[gateway] key={} background refresh FAILED ({}) consecutiveFailures={} max={}",
                key, cause, failures, properties.getStaleMaxRefreshFailures());
    }

    private int refreshFailureCount(String key) {
        AtomicInteger count = refreshFailures.get(key);
        return count == null ? 0 : count.get();
    }

    /** 回源：先过全局排队守卫与熔断器，带超时；超时/失败时走兜底逻辑。 */
    private GatewayResponse fetchFromUpstream(String key, CacheEntry cached, long startNanos) {
        // 全局容量保护：只对真正要访问上游的请求生效，可能抛出排队超时/队列满拒绝
        globalGuard.acquire();
        try {
            if (!circuitBreaker.tryAcquire()) {
                metrics.recordCircuitRejected();
                CircuitOpenException cause = new CircuitOpenException(
                        "circuit " + circuitBreaker.state() + ", upstream call suppressed");
                log.info("[gateway] key={} {}", key, cause.getMessage());
                return fallback(key, cached, startNanos, cause);
            }
            metrics.recordUpstreamCall();
            Future<String> task = upstreamExecutor.submit(() -> upstream.fetch(key));
            try {
                String value = task.get(properties.getUpstreamTimeoutMillis(), TimeUnit.MILLISECONDS);
                CacheEntry entry = new CacheEntry(value, Instant.now());
                cache.put(key, entry);
                refreshFailures.remove(key);
                circuitBreaker.onSuccess();
                metrics.recordFresh();
                return new GatewayResponse(key, value, ResultOrigin.FRESH,
                        entry.fetchedAt(), elapsedMillis(startNanos));
            } catch (TimeoutException e) {
                task.cancel(true); // 中断慢上游调用，避免请求堆积
                circuitBreaker.onFailure();
                metrics.recordUpstreamFailure();
                log.warn("[gateway] key={} upstream TIMEOUT after {}ms",
                        key, properties.getUpstreamTimeoutMillis());
                return fallback(key, cached, startNanos, e);
            } catch (ExecutionException e) {
                circuitBreaker.onFailure();
                metrics.recordUpstreamFailure();
                log.warn("[gateway] key={} upstream FAILED: {}", key, e.getCause().getMessage());
                return fallback(key, cached, startNanos, e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                circuitBreaker.onFailure();
                metrics.recordUpstreamFailure();
                return fallback(key, cached, startNanos, e);
            }
        } finally {
            globalGuard.release();
        }
    }

    /** 兜底：有过期缓存返回过期兜底，否则返回降级结果。 */
    private GatewayResponse fallback(String key, CacheEntry cached, long startNanos, Throwable cause) {
        if (cached != null) {
            metrics.recordStaleFallback();
            log.info("[gateway] key={} STALE_FALLBACK fetchedAt={} cause={}",
                    key, cached.fetchedAt(), cause.getMessage());
            return new GatewayResponse(key, cached.value(), ResultOrigin.STALE_FALLBACK,
                    cached.fetchedAt(), elapsedMillis(startNanos));
        }
        metrics.recordDegraded();
        log.info("[gateway] key={} DEGRADED cause={}", key, cause.getMessage());
        return new GatewayResponse(key, null, ResultOrigin.DEGRADED,
                null, elapsedMillis(startNanos));
    }

    /** 导出保护组件的运行状态（熔断状态、全局队列水位），供指标接口合并输出。 */
    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("circuitState", circuitBreaker.state().name());
        status.put("circuitConsecutiveFailures", circuitBreaker.consecutiveFailures());
        status.put("globalQueueWaiters", globalGuard.currentWaiters());
        status.put("globalAvailablePermits", globalGuard.availablePermits());
        return status;
    }

    private static long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
