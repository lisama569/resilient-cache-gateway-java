package com.github.lisama569.resilientcachegatewayjava.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 全局准入限制器：限制同时访问上游的真实并发数。
 *
 * <p>容量满时请求不直接拒绝，而是进入有界队列等待一个可配置的时间：
 * 等到许可则正常处理；等待超时抛出 {@link QueueWaitTimeoutException}；
 * 等待队列本身已满则立即抛出 {@link CapacityExceededException}。
 * 两种拒绝语义明确区分。</p>
 *
 * <p>只作用于真正回源的请求；缓存命中与单飞合并的跟随者不经过这里。</p>
 */
public class GlobalAdmissionLimiter {

    private static final Logger log = LoggerFactory.getLogger(GlobalAdmissionLimiter.class);

    /** 排队等待超时：拿到了排队资格，但在配置时间内没等到许可。 */
    public static class QueueWaitTimeoutException extends RuntimeException {
        public QueueWaitTimeoutException(String message) {
            super(message);
        }
    }

    /** 总量超限：等待队列已满，立即拒绝。 */
    public static class CapacityExceededException extends RuntimeException {
        public CapacityExceededException(String message) {
            super(message);
        }
    }

    private final GatewayProperties properties;
    private final GatewayMetrics metrics;

    private final Object lock = new Object();
    private int inFlight;
    private int waiting;

    public GlobalAdmissionLimiter(GatewayProperties properties, GatewayMetrics metrics) {
        this.properties = properties;
        this.metrics = metrics;
    }

    /**
     * 获取一个全局上游访问许可，必要时在有界队列内等待。
     *
     * @throws QueueWaitTimeoutException 在 queueWaitTimeoutMillis 内没等到许可
     * @throws CapacityExceededException 等待队列已满（总量超限）
     */
    public void acquire() {
        long waitTimeout = properties.getQueueWaitTimeoutMillis();
        long deadlineNanos = System.nanoTime() + waitTimeout * 1_000_000L;
        synchronized (lock) {
            if (inFlight < properties.getMaxConcurrentUpstream()) {
                inFlight++;
                return;
            }
            if (waiting >= properties.getMaxQueueSize()) {
                metrics.recordCapacityExceeded();
                log.warn("[limiter] CAPACITY_EXCEEDED waiting={} maxQueueSize={}",
                        waiting, properties.getMaxQueueSize());
                throw new CapacityExceededException(
                        "global upstream capacity exceeded: queue full (maxQueueSize="
                                + properties.getMaxQueueSize() + ")");
            }
            waiting++;
            try {
                long remainingNanos = deadlineNanos - System.nanoTime();
                while (inFlight >= properties.getMaxConcurrentUpstream()) {
                    if (remainingNanos <= 0) {
                        metrics.recordQueueWaitTimeout();
                        log.warn("[limiter] QUEUE_WAIT_TIMEOUT waitTimeoutMs={} inFlight={}",
                                waitTimeout, inFlight);
                        throw new QueueWaitTimeoutException(
                                "timed out waiting for global upstream capacity after "
                                        + waitTimeout + "ms");
                    }
                    long waitMillis = remainingNanos / 1_000_000L;
                    int waitNanosExtra = (int) (remainingNanos % 1_000_000L);
                    try {
                        lock.wait(waitMillis, waitNanosExtra);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        metrics.recordQueueWaitTimeout();
                        throw new QueueWaitTimeoutException(
                                "interrupted while waiting for global upstream capacity");
                    }
                    remainingNanos = deadlineNanos - System.nanoTime();
                }
                inFlight++;
            } finally {
                waiting--;
            }
        }
    }

    /** 归还一个许可，并唤醒排队者。 */
    public void release() {
        synchronized (lock) {
            if (inFlight > 0) {
                inFlight--;
            }
            lock.notifyAll();
        }
    }

    /** 当前正在访问上游的并发数（指标导出用）。 */
    public int inFlight() {
        synchronized (lock) {
            return inFlight;
        }
    }

    /** 当前排队等待中的请求数（指标导出用）。 */
    public int waiting() {
        synchronized (lock) {
            return waiting;
        }
    }
}
