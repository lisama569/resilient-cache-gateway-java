package com.github.lisama569.resilientcachegatewayjava.tests;

import com.github.lisama569.resilientcachegatewayjava.resilience.CircuitBreaker;
import com.github.lisama569.resilientcachegatewayjava.resilience.CircuitState;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 熔断器状态机测试：阈值判定、OPEN 拒绝、冷却后半开试探、
 * 试探成功闭合 / 试探失败重新打开。关键输入与判定均打印日志。
 */
class CircuitBreakerTest {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerTest.class);

    @Test
    void opensAfterConsecutiveFailuresReachThresholdAndBlocksRequests() {
        int threshold = 3;
        CircuitBreaker cb = new CircuitBreaker(threshold, 1000, 1);
        log.info("[cb-test] threshold={}, 连续上报 3 次失败", threshold);

        cb.recordFailure(100);
        cb.recordFailure(200);
        assertThat(cb.state()).as("未达阈值仍为 CLOSED").isEqualTo(CircuitState.CLOSED);
        cb.recordFailure(300);

        log.info("[cb-test] 判定依据: consecutiveFailures={} >= threshold={} -> 期望 OPEN",
                cb.consecutiveFailures(), threshold);
        assertThat(cb.state()).isEqualTo(CircuitState.OPEN);
        assertThat(cb.allowRequest(400)).as("OPEN 期间请求必须被拒绝").isFalse();
    }

    @Test
    void successResetsConsecutiveFailures() {
        CircuitBreaker cb = new CircuitBreaker(3, 1000, 1);
        cb.recordFailure(100);
        cb.recordFailure(200);
        cb.recordSuccess();
        log.info("[cb-test] 两次失败后一次成功 -> consecutiveFailures={} 期望 0",
                cb.consecutiveFailures());
        assertThat(cb.consecutiveFailures()).isZero();
        assertThat(cb.state()).isEqualTo(CircuitState.CLOSED);
    }

    @Test
    void transitionsToHalfOpenAfterCooldownThenClosesOnSuccess() {
        CircuitBreaker cb = new CircuitBreaker(2, 1000, 1);
        cb.recordFailure(0);
        cb.recordFailure(0);
        assertThat(cb.state()).isEqualTo(CircuitState.OPEN);

        log.info("[cb-test] OPEN 后冷却期内 now=500 判定应仍拒绝");
        assertThat(cb.allowRequest(500)).isFalse();

        log.info("[cb-test] now=1001 满足冷却, allowRequest 触发 HALF_OPEN 并占 1 个试探名额");
        assertThat(cb.allowRequest(1001)).isTrue();
        assertThat(cb.state()).isEqualTo(CircuitState.HALF_OPEN);
        assertThat(cb.allowRequest(1002)).as("半开名额仅 1，第二个试探被拒绝").isFalse();

        cb.recordSuccess();
        log.info("[cb-test] 试探成功 -> 期望 CLOSED");
        assertThat(cb.state()).isEqualTo(CircuitState.CLOSED);
        assertThat(cb.allowRequest(1100)).isTrue();
    }

    @Test
    void halfOpenFailureReopensCircuit() {
        CircuitBreaker cb = new CircuitBreaker(2, 1000, 1);
        cb.recordFailure(0);
        cb.recordFailure(0);
        assertThat(cb.allowRequest(1001)).isTrue();
        assertThat(cb.state()).isEqualTo(CircuitState.HALF_OPEN);

        cb.recordFailure(1002);
        log.info("[cb-test] 半开试探失败 -> 期望重新 OPEN");
        assertThat(cb.state()).isEqualTo(CircuitState.OPEN);
        assertThat(cb.allowRequest(1003)).as("刚重新 OPEN，冷却未到，应拒绝").isFalse();
    }
}
