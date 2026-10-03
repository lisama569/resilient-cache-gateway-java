package com.github.lisama569.resilientcachegatewayjava.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 上游熔断器：连续失败/超时达到阈值后进入保护期（OPEN），期间请求直接走兜底；
 * 保护期结束后进入试探期（HALF_OPEN），只放行少量探测请求，
 * 探测成功恢复 CLOSED，失败则重新进入保护期。
 *
 * <p>状态机全部在内部锁内推进，保证并发下不会同时放出一批试探请求。
 * 配置项运行期可调（每次判断时读取最新值）。</p>
 */
public class CircuitBreaker {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreaker.class);

    /** 熔断器状态。 */
    public enum State {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    private final GatewayProperties properties;
    private final Object lock = new Object();

    private State state = State.CLOSED;
    private int consecutiveFailures;
    private long openedAtMillis;
    private int halfOpenInFlight;
    private long openCount;

    public CircuitBreaker(GatewayProperties properties) {
        this.properties = properties;
    }

    /**
     * 尝试获得一次访问上游的许可。
     *
     * @return true 表示允许访问上游；false 表示当前处于保护期或试探名额已满，请求应直接走兜底
     */
    public boolean tryAcquirePermission() {
        synchronized (lock) {
            transitionIfOpenExpired();
            switch (state) {
                case CLOSED:
                    return true;
                case HALF_OPEN:
                    if (halfOpenInFlight < properties.getCircuitHalfOpenProbes()) {
                        halfOpenInFlight++;
                        log.info("[circuit] HALF_OPEN probe admitted inFlight={}", halfOpenInFlight);
                        return true;
                    }
                    return false;
                case OPEN:
                default:
                    return false;
            }
        }
    }

    /** 上报一次上游成功。 */
    public void onSuccess() {
        synchronized (lock) {
            if (state == State.HALF_OPEN) {
                halfOpenInFlight = Math.max(0, halfOpenInFlight - 1);
                state = State.CLOSED;
                consecutiveFailures = 0;
                log.info("[circuit] probe succeeded -> CLOSED");
            } else if (state == State.CLOSED) {
                consecutiveFailures = 0;
            }
        }
    }

    /** 上报一次上游失败或超时。 */
    public void onFailure() {
        synchronized (lock) {
            if (state == State.HALF_OPEN) {
                halfOpenInFlight = Math.max(0, halfOpenInFlight - 1);
                openLocked("probe failed");
            } else if (state == State.CLOSED) {
                consecutiveFailures++;
                if (consecutiveFailures >= properties.getCircuitFailureThreshold()) {
                    openLocked("consecutiveFailures=" + consecutiveFailures);
                }
            }
        }
    }

    /** 当前状态（用于指标导出）；保护期到期的 OPEN 在此惰性转为 HALF_OPEN。 */
    public State state() {
        synchronized (lock) {
            transitionIfOpenExpired();
            return state;
        }
    }

    /** 累计进入保护期的次数（用于指标导出）。 */
    public long openCount() {
        synchronized (lock) {
            return openCount;
        }
    }

    private void transitionIfOpenExpired() {
        if (state == State.OPEN
                && System.currentTimeMillis() - openedAtMillis >= properties.getCircuitOpenMillis()) {
            state = State.HALF_OPEN;
            halfOpenInFlight = 0;
            log.info("[circuit] open period elapsed -> HALF_OPEN");
        }
    }

    private void openLocked(String reason) {
        state = State.OPEN;
        openedAtMillis = System.currentTimeMillis();
        openCount++;
        log.warn("[circuit] -> OPEN reason={} openMillis={}", reason, properties.getCircuitOpenMillis());
    }
}
