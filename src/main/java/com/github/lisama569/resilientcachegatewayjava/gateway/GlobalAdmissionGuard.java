package com.github.lisama569.resilientcachegatewayjava.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 服务整体容量保护：限制同时压向上游的全局并发数（{@code globalMaxConcurrent}）。
 * 瞬时超量时不直接打回，而是在有界队列（{@code globalQueueCapacity}）里等待
 * 一个可配置的时间（{@code globalQueueTimeoutMillis}）：
 * <ul>
 *   <li>等到许可 → 正常处理；</li>
 *   <li>等待超时 → {@link QueueTimeoutRejectedException}（排队没等到）；</li>
 *   <li>队列已满 → {@link QueueFullRejectedException}（总量超过上限，立即拒绝）。</li>
 * </ul>
 * 只作用于真正要访问上游的请求，缓存命中与合并等待不经过此守卫。
 */
@Component
public class GlobalAdmissionGuard {

    private static final Logger log = LoggerFactory.getLogger(GlobalAdmissionGuard.class);

    private final GatewayProperties properties;
    private final Semaphore permits;
    private final AtomicInteger waiters = new AtomicInteger();

    public GlobalAdmissionGuard(GatewayProperties properties) {
        this.properties = properties;
        this.permits = new Semaphore(properties.getGlobalMaxConcurrent(), true);
    }

    /**
     * 尝试进入全局处理通道：有许可立即返回；否则排队等待，超时或队列满则拒绝。
     *
     * @throws QueueTimeoutRejectedException 在队列中等待超过配置时间
     * @throws QueueFullRejectedException    等待队列已满（总量超过上限）
     */
    public void acquire() {
        if (permits.tryAcquire()) {
            return;
        }
        int waiting = waiters.incrementAndGet();
        if (waiting > properties.getGlobalQueueCapacity()) {
            waiters.decrementAndGet();
            log.warn("[guard] QUEUE_FULL waiters={} capacity={}",
                    waiting, properties.getGlobalQueueCapacity());
            throw new QueueFullRejectedException(
                    "global queue full: waiters=" + waiting
                            + " capacity=" + properties.getGlobalQueueCapacity());
        }
        try {
            boolean acquired = permits.tryAcquire(
                    properties.getGlobalQueueTimeoutMillis(), TimeUnit.MILLISECONDS);
            if (!acquired) {
                log.warn("[guard] QUEUE_TIMEOUT after {}ms waiters={}",
                        properties.getGlobalQueueTimeoutMillis(), waiting);
                throw new QueueTimeoutRejectedException(
                        "global queue wait timeout after "
                                + properties.getGlobalQueueTimeoutMillis() + "ms");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueueTimeoutRejectedException("interrupted while waiting in global queue");
        } finally {
            waiters.decrementAndGet();
        }
    }

    /** 释放一个全局许可。 */
    public void release() {
        permits.release();
    }

    /** 当前排队等待中的请求数（用于指标导出）。 */
    public int currentWaiters() {
        return waiters.get();
    }

    /** 当前可用许可数（用于指标导出）。 */
    public int availablePermits() {
        return permits.availablePermits();
    }

    /** 排队等待超时：调用方可与队列满区分。 */
    public static class QueueTimeoutRejectedException extends RuntimeException {
        public QueueTimeoutRejectedException(String message) {
            super(message);
        }
    }

    /** 等待队列已满（总量超过上限）。 */
    public static class QueueFullRejectedException extends RuntimeException {
        public QueueFullRejectedException(String message) {
            super(message);
        }
    }
}
