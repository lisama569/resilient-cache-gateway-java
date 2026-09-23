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
 *   <li>单飞合并：同一 key 过期/未命中时只放行一个回源请求，其余请求共享其结果。</li>
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

    private final Map<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<GatewayResponse>> inflight = new ConcurrentHashMap<>();
    private final Map<String, Semaphore> keyGuards = new ConcurrentHashMap<>();
    private final ExecutorService upstreamExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public CacheGatewayService(GatewayProperties properties,
                               SimulatedUpstream upstream,
                               GatewayMetrics metrics) {
        this.properties = properties;
        this.upstream = upstream;
        this.metrics = metrics;
    }

    /** 缓存条目（不可变）。 */
    private record CacheEntry(String value, Instant fetchedAt) {
        boolean isFresh(long ttlMillis) {
            return fetchedAt.plusMillis(ttlMillis).isAfter(Instant.now());
        }
    }

    /** 并发超限拒绝异常。 */
    public static class OverloadRejectedException extends RuntimeException {
        public OverloadRejectedException(String key) {
            super("too many concurrent requests for key=" + key);
        }
    }

    /**
     * 读取一个 key 的数据，带缓存、合并、降级与并发保护。
     *
     * @throws OverloadRejectedException 该 key 并发请求超过上限时抛出（确定且一致的拒绝规则）
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

        // 未命中或已过期：单飞合并，同一时刻只允许一个回源。
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

    /** 回源：带超时；超时/失败时走兜底逻辑。 */
    private GatewayResponse fetchFromUpstream(String key, CacheEntry cached, long startNanos) {
        metrics.recordUpstreamCall();
        Future<String> task = upstreamExecutor.submit(() -> upstream.fetch(key));
        try {
            String value = task.get(properties.getUpstreamTimeoutMillis(), TimeUnit.MILLISECONDS);
            CacheEntry entry = new CacheEntry(value, Instant.now());
            cache.put(key, entry);
            metrics.recordFresh();
            return new GatewayResponse(key, value, ResultOrigin.FRESH,
                    entry.fetchedAt(), elapsedMillis(startNanos));
        } catch (TimeoutException e) {
            task.cancel(true); // 中断慢上游调用，避免请求堆积
            metrics.recordUpstreamFailure();
            log.warn("[gateway] key={} upstream TIMEOUT after {}ms",
                    key, properties.getUpstreamTimeoutMillis());
            return fallback(key, cached, startNanos, e);
        } catch (ExecutionException e) {
            metrics.recordUpstreamFailure();
            log.warn("[gateway] key={} upstream FAILED: {}", key, e.getCause().getMessage());
            return fallback(key, cached, startNanos, e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metrics.recordUpstreamFailure();
            return fallback(key, cached, startNanos, e);
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
