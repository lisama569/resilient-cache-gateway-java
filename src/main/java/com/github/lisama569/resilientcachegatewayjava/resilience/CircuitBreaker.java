package com.github.lisama569.resilientcachegatewayjava.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 单目标熔断器（时间由调用方注入，便于测试）：
 *
 *  CLOSED    ：正常放行；连续失败数达到 failureThreshold 后转 OPEN；
 *  OPEN      ：直接拒绝；持续 openStateMillis 后，下一次请求触发转 HALF_OPEN；
 *  HALF_OPEN ：仅放行 halfOpenPermits 个试探请求，其余拒绝；
 *              试探期间任一失败立即回 OPEN 并重置计时；全部在途试探成功后转 CLOSED。
 */
public class CircuitBreaker {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreaker.class);

    private final int failureThreshold;
    private final long openStateMillis;
    private final int halfOpenPermits;

    private CircuitState state = CircuitState.CLOSED;
    private int consecutiveFailures;
    private long openedAt;
    private int halfOpenInFlight;
    private int halfOpenSucceeded;

    public CircuitBreaker(int failureThreshold, long openStateMillis, int halfOpenPermits) {
        this.failureThreshold = Math.max(1, failureThreshold);
        this.openStateMillis = openStateMillis;
        this.halfOpenPermits = Math.max(1, halfOpenPermits);
    }

    /**
     * 是否允许本次调用通过。HALF_OPEN 时占用一个试探名额。
     */
    public synchronized boolean allowRequest(long now) {
        switch (state) {
            case CLOSED -> {
                return true;
            }
            case OPEN -> {
                if (now - openedAt >= openStateMillis) {
                    // 冷却结束，进入半开并占用第一个名额
                    state = CircuitState.HALF_OPEN;
                    halfOpenInFlight = 1;
                    halfOpenSucceeded = 0;
                    log.info("[circuit] OPEN -> HALF_OPEN (now={})", now);
                    return true;
                }
                return false;
            }
            case HALF_OPEN -> {
                if (halfOpenInFlight < halfOpenPermits) {
                    halfOpenInFlight++;
                    return true;
                }
                return false;
            }
            default -> {
                return false;
            }
        }
    }

    /** 试探或正常调用成功。 */
    public synchronized void recordSuccess() {
        if (state == CircuitState.HALF_OPEN) {
            halfOpenSucceeded++;
            if (halfOpenSucceeded >= halfOpenInFlight
                    && halfOpenSucceeded >= halfOpenPermits) {
                toClosed();
            }
        } else if (state == CircuitState.CLOSED) {
            consecutiveFailures = 0;
        }
    }

    /** 调用失败（含超时）。 */
    public synchronized void recordFailure(long now) {
        switch (state) {
            case HALF_OPEN -> toOpen(now);
            case CLOSED -> {
                consecutiveFailures++;
                if (consecutiveFailures >= failureThreshold) {
                    toOpen(now);
                }
            }
            default -> { }
        }
    }

    private void toOpen(long now) {
        state = CircuitState.OPEN;
        openedAt = now;
        halfOpenInFlight = 0;
        halfOpenSucceeded = 0;
        log.info("[circuit] -> OPEN (now={}, consecutiveFailures={})", now, consecutiveFailures);
    }

    private void toClosed() {
        state = CircuitState.CLOSED;
        consecutiveFailures = 0;
        halfOpenInFlight = 0;
        halfOpenSucceeded = 0;
        log.info("[circuit] HALF_OPEN -> CLOSED");
    }

    public synchronized CircuitState state() { return state; }
    public synchronized int consecutiveFailures() { return consecutiveFailures; }
    public synchronized long openedAt() { return openedAt; }
}
