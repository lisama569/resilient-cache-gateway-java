package com.github.lisama569.resilientcachegatewayjava.gateway;

/**
 * 结果来源：新鲜数据 / 过期兜底 / 降级结果。
 */
public enum ResultOrigin {
    FRESH,
    STALE_FALLBACK,
    DEGRADED
}
