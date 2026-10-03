package com.github.lisama569.resilientcachegatewayjava.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 上游熔断器：连续失败达到阈值后进入保护期（OPEN），期间不再访问上游；
 * 保护期结束后进入试探状态（HALF_OPEN），只放行有限数量的试探请求，
 * 任一试探失败立即重新 OPEN，全部在途试探都成功才恢复 CLOSED。
 *
 * <p>所有状态迁移都在内部锁上完成，并发下状态确定：
 * 同一时刻最多只有 {@code circuitProbeCount} 个试探请求被放行。</p>
 */
@Component
public class CircuitBreaker {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreaker.class);

    /** 熔断器状态。 */
    public enum State {
        CLOSED, OPEN, HALF_OPEN
    }

    private final GatewayProperties properties;
    private final Object lock = new Object();

    private State state = State.CLOSED;
    private int consecutiveFailures;
    private long openedAtMillis;
    private int probesInFlight;

    public CircuitBreaker(GatewayProperties properties) {
        this.properties = properties;
    }

    /**
     * 尝试获取一次访问上游的许可。
     *
     * @return true 表示允许访问上游；false 表示处于保护期或试探名额已满
     */
    public boolean tryAcquire() {
        synchronized (lock) {
            switch (state) {
                case CLOSED -> {
                    return true;
                }
                case OPEN -> {
                    long elapsed = System.currentTimeMillis() - openedAtMillis;
                    if (elapsed >= properties.getCircuitOpenMillis()) {
                        state = State.HALF_OPEN;
                        probesInFlight = 0;
                        log.info("[circuit] OPEN -> HALF_OPEN after {}ms, probeCount={}",
                                elapsed, properties.getCircuitProbeCount());
                        return acquireProbe();
                    }
                    return false;
                }
                case HALF_OPEN -> {
                    return acquireProbe();
                }
                default -> {
                    return false;
                }
            }
        }
    }

    private boolean acquireProbe() {
        if (probesInFlight < properties.getCircuitProbeCount()) {
            probesInFlight++;
            return true;
        }
        return false;
    }

    /** 上游调用成功后上报。 */
    public void onSuccess() {
        synchronized (lock) {
            if (state == State.HALF_OPEN) {
                probesInFlight--;
                if (probesInFlight <= 0) {
                    state = State.CLOSED;
                    consecutiveFailures = 0;
                    probesInFlight = 0;
                    log.info("[circuit] HALF_OPEN -> CLOSED, probe succeeded");
                }
            } else {
                consecutiveFailures = 0;
            }
        }
    }

    /** 上游调用失败/超时后上报。 */
    public void onFailure() {
        synchronized (lock) {
            consecutiveFailures++;
            if (state == State.HALF_OPEN) {
                probesInFlight--;
                state = State.OPEN;
                openedAtMillis = System.currentTimeMillis();
                log.warn("[circuit] HALF_OPEN -> OPEN, probe failed, reopenMs={}",
                        properties.getCircuitOpenMillis());
            } else if (state == State.CLOSED
                    && consecutiveFailures >= properties.getCircuitFailureThreshold()) {
                state = State.OPEN;
                openedAtMillis = System.currentTimeMillis();
                log.warn("[circuit] CLOSED -> OPEN after {} consecutive failures, openMs={}",
                        consecutiveFailures, properties.getCircuitOpenMillis());
            }
        }
    }

    /** 当前状态（用于指标导出）。 */
    public State state() {
        synchronized (lock) {
            // 仅用于展示：保护期已过时对外呈现为 HALF_OPEN，不改变内部许可计数
            if (state == State.OPEN
                    && System.currentTimeMillis() - openedAtMillis >= properties.getCircuitOpenMillis()) {
                return State.HALF_OPEN;
            }
            return state;
        }
    }

    /** 连续失败计数（用于指标导出）。 */
    public int consecutiveFailures() {
        synchronized (lock) {
            return consecutiveFailures;
        }
    }
}
