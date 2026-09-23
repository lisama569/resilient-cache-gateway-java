package com.github.lisama569.resilientcachegatewayjava.resilience;

/** 熔断器状态。 */
public enum CircuitState {
    CLOSED, OPEN, HALF_OPEN
}
