package com.github.lisama569.resilientcachegatewayjava.gateway;

/**
 * 结果来源：新鲜数据 / 宽限窗口内的旧结果（后台更新中）/ 过期兜底 / 降级结果。
 */
public enum ResultOrigin {
    FRESH,
    STALE_REFRESHING,
    STALE_FALLBACK,
    DEGRADED
}
