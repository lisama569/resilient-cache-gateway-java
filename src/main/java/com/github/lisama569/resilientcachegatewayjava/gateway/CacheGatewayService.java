package com.github.lisama569.resilientcachegatewayjava.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
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

/**
 * 缓存聚合网关核心服务。
 *
 * <ul>
 *   <li>TTL 缓存：有效期内直接返回新鲜结果，过期后重新回源，绝不把过期内容当新鲜返回。</li>
 *   <li>宽限窗口（SWR）：过期后的一段可配置宽限时间内，请求立即拿到旧结果
 *       （{@link ResultOrigin#STALE_REFRESHING}），同时只触发一次后台更新；
 *       未真正打到上游的更新（容量保护挡回/熔断保护期跳过）不计入连续失败、不让旧结果失效；
 *       真正打到上游且失败/超时才计数，达到上限或宽限时间用完后，落回同步回源与兜底分类。</li>
 *   <li>单飞合并：同一 key 过期/未命中时只放行一个回源请求，其余请求共享其结果。</li>
 *   <li>全局准入：真实回源前经过全局并发上限与有界排队，缓存命中与合并跟随者不占用名额；
 *       排队超时与总量超限是两种明确区分的拒绝。</li>
 *   <li>熔断器：上游连续失败/超时达到阈值后进入保护期，期间直接走兜底；
 *       保护期结束后放少量请求试探，成功恢复、失败继续保护。</li>
 *   <li>超时与降级：上游超时或失败时，有过期缓存则返回过期兜底，否则返回降级结果。</li>
 *   <li>并发保护：每个 key 一个信号量，超出上限的请求按确定规则直接拒绝。</li>
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
    private final GlobalAdmissionLimiter admissionLimiter;

    private final Map<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<GatewayResponse>> inflight = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<Void>> backgroundRefreshes = new ConcurrentHashMap<>();
    private final Map<String, Semaphore> keyGuards = new ConcurrentHashMap<>();
    private final ExecutorService upstreamExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public CacheGatewayService(GatewayProperties properties,
                               SimulatedUpstream upstream,
                               GatewayMetrics metrics) {
        this.properties = properties;
        this.upstream = upstream;
        this.metrics = metrics;
        this.circuitBreaker = new CircuitBreaker(properties);
        this.admissionLimiter = new GlobalAdmissionLimiter(properties, metrics);
        this.metrics.attachRuntimeStatus(circuitBreaker, admissionLimiter);
    }

    /** 缓存条目（不可变）；backgroundFailures 记录宽限窗口内后台更新的连续失败次数。 */
    private record CacheEntry(String value, Instant fetchedAt, int backgroundFailures) {
        boolean isFresh(long ttlMillis) {
            return fetchedAt.plusMillis(ttlMillis).isAfter(Instant.now());
        }

        /** 是否处于宽限窗口内：已过期（age >= ttl）且未超出宽限边界（age < ttl + grace）。 */
        boolean withinGrace(long ttlMillis, long graceMillis) {
            Instant now = Instant.now();
            return !fetchedAt.plusMillis(ttlMillis).isAfter(now)
                    && fetchedAt.plusMillis(ttlMillis + graceMillis).isAfter(now);
        }
    }

    /** 并发超限拒绝异常（单 key 维度）。 */
    public static class OverloadRejectedException extends RuntimeException {
        public OverloadRejectedException(String key) {
            super("too many concurrent requests for key=" + key);
        }
    }

    /** 熔断器处于保护期/试探名额已满时，跳过上游直接兜底（内部信号）。 */
    private static class CircuitOpenException extends RuntimeException {
        CircuitOpenException(String message) {
            super(message);
        }
    }

    /** 上游调用失败（超时/异常/中断）的统一包装（内部信号）。 */
    private static class UpstreamFetchException extends RuntimeException {
        UpstreamFetchException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * 读取一个 key 的数据，带缓存、宽限窗口、合并、全局准入、熔断、降级与并发保护。
     *
     * @throws OverloadRejectedException 该 key 并发请求超过上限时抛出（确定且一致的拒绝规则）
     * @throws GlobalAdmissionLimiter.QueueWaitTimeoutException 全局容量满且排队等待超时
     * @throws GlobalAdmissionLimiter.CapacityExceededException 全局等待队列已满（总量超限）
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
        long ttlMillis = properties.getCacheTtlMillis();
        long graceMillis = properties.getSwrGraceMillis();
        if (cached != null && cached.isFresh(ttlMillis)) {
            metrics.recordFresh();
            return new GatewayResponse(key, cached.value(), ResultOrigin.FRESH,
                    cached.fetchedAt(), elapsedMillis(startNanos));
        }

        // 宽限窗口：已过期但在窗口内、且后台更新连续失败未达上限 -> 先给旧结果，只触发一次后台更新。
        if (cached != null && graceMillis > 0
                && cached.withinGrace(ttlMillis, graceMillis)
                && cached.backgroundFailures() < properties.getSwrMaxBackgroundFailures()) {
            triggerBackgroundRefresh(key);
            metrics.recordStaleRefreshing();
            log.info("[gateway] key={} STALE_REFRESHING fetchedAt={} bgFailures={}",
                    key, cached.fetchedAt(), cached.backgroundFailures());
            return new GatewayResponse(key, cached.value(), ResultOrigin.STALE_REFRESHING,
                    cached.fetchedAt(), elapsedMillis(startNanos));
        }

        // 未命中、已过期且不在宽限窗口（或后台更新失败次数已达上限）：单飞合并回源。
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

    /**
     * 触发一次后台更新；同一 key 同一时刻最多一个后台更新在进行，重复触发直接忽略。
     */
    private void triggerBackgroundRefresh(String key) {
        CompletableFuture<Void> marker = new CompletableFuture<>();
        if (backgroundRefreshes.putIfAbsent(key, marker) != null) {
            return; // 已有后台更新在进行，不重复触发
        }
        upstreamExecutor.submit(() -> {
            boolean failed = false;
            try {
                metrics.recordBackgroundRefresh();
                String value = guardedUpstreamFetch(key);
                cache.put(key, new CacheEntry(value, Instant.now(), 0));
                log.info("[gateway] key={} background refresh OK", key);
            } catch (CircuitOpenException
                     | GlobalAdmissionLimiter.QueueWaitTimeoutException
                     | GlobalAdmissionLimiter.CapacityExceededException e) {
                // 未真正打到上游（熔断保护期跳过 / 被自身容量保护挡回）：
                // 不算连续失败、不让窗口内的旧结果失效，下一个请求可再次触发更新。
                metrics.recordBackgroundRefreshSkipped();
                log.info("[gateway] key={} background refresh SKIPPED (not sent upstream): {}",
                        key, e.getMessage());
            } catch (RuntimeException e) {
                // 确实打到上游且失败/超时：才按连续失败累加，达到上限后落回兜底。
                failed = true;
                // 只给仍是旧内容的条目累加失败次数；期间若已被同步回源刷新则不动。
                cache.computeIfPresent(key, (k, entry) -> entry.isFresh(properties.getCacheTtlMillis())
                        ? entry
                        : new CacheEntry(entry.value(), entry.fetchedAt(), entry.backgroundFailures() + 1));
                log.warn("[gateway] key={} background refresh FAILED: {}", key, e.getMessage());
            } finally {
                // 先移除“更新进行中”标记再记失败指标：外部观察到失败计数时，
                // 本轮后台更新已彻底结束，下一次请求可以正常触发新一轮更新。
                backgroundRefreshes.remove(key, marker);
                if (failed) {
                    metrics.recordBackgroundRefreshFailure();
                }
                marker.complete(null);
            }
        });
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

    /** 回源：熔断开路直接兜底；排队超时/总量超限向上抛出明确拒绝；上游失败走兜底。 */
    private GatewayResponse fetchFromUpstream(String key, CacheEntry cached, long startNanos) {
        try {
            String value = guardedUpstreamFetch(key);
            CacheEntry entry = new CacheEntry(value, Instant.now(), 0);
            cache.put(key, entry);
            metrics.recordFresh();
            return new GatewayResponse(key, value, ResultOrigin.FRESH,
                    entry.fetchedAt(), elapsedMillis(startNanos));
        } catch (CircuitOpenException e) {
            log.info("[gateway] key={} circuit OPEN/HALF_OPEN-full -> fallback", key);
            return fallback(key, cached, startNanos, e);
        } catch (UpstreamFetchException e) {
            log.warn("[gateway] key={} upstream failed: {}", key, e.getMessage());
            return fallback(key, cached, startNanos, e.getCause());
        }
        // QueueWaitTimeoutException / CapacityExceededException 不捕获：明确拒绝语义向上传递
    }

    /**
     * 带熔断与全局准入的上游调用：成功返回 value，失败抛出内部信号异常。
     * 熔断许可未拿到、排队超时、总量超限都不计入熔断失败（未真正打到上游）。
     */
    private String guardedUpstreamFetch(String key) {
        if (!circuitBreaker.tryAcquirePermission()) {
            throw new CircuitOpenException("circuit breaker is " + circuitBreaker.state());
        }
        try {
            admissionLimiter.acquire();
        } catch (RuntimeException e) {
            // 容量保护挡回（排队超时/总量超限）：未真正打到上游，
            // 归还试探名额避免熔断器卡在试探期，且不计入熔断连续失败。
            circuitBreaker.onAdmissionRejected();
            throw e;
        }
        boolean success = false;
        try {
            metrics.recordUpstreamCall();
            Future<String> task = upstreamExecutor.submit(() -> upstream.fetch(key));
            try {
                String value = task.get(properties.getUpstreamTimeoutMillis(), TimeUnit.MILLISECONDS);
                success = true;
                return value;
            } catch (TimeoutException e) {
                task.cancel(true); // 中断慢上游调用，避免请求堆积
                metrics.recordUpstreamFailure();
                log.warn("[gateway] key={} upstream TIMEOUT after {}ms",
                        key, properties.getUpstreamTimeoutMillis());
                throw new UpstreamFetchException("upstream timeout", e);
            } catch (ExecutionException e) {
                metrics.recordUpstreamFailure();
                log.warn("[gateway] key={} upstream FAILED: {}", key, e.getCause().getMessage());
                throw new UpstreamFetchException("upstream failed", e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                metrics.recordUpstreamFailure();
                throw new UpstreamFetchException("interrupted", e);
            }
        } finally {
            admissionLimiter.release();
            if (success) {
                circuitBreaker.onSuccess();
            } else {
                circuitBreaker.onFailure();
            }
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

    private static long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
