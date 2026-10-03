package com.github.lisama569.resilientcachegatewayjava.gateway;

/**
 * 结果来源：新鲜数据 / 宽限窗口内后台更新中的旧结果 / 过期兜底 / 降级结果。
 */
public enum ResultOrigin {
    FRESH,
    STALE_REFRESHING,
    STALE_FALLBACK,
    DEGRADED
}
