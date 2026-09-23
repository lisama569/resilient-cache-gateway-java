package com.github.lisama569.resilientcachegatewayjava.model;

/**
 * 每次返回结果的类别，调用方可明确区分。
 * FRESH        —— 新鲜数据（缓存未过期，或本次成功从上游获取）
 * STALE_FALLBACK —— 上游失败/超时/熔断时返回的过期兜底数据
 * DEGRADED     —— 无任何可用数据时的降级结果（默认值）
 */
public enum ResultStatus {
    FRESH,
    STALE_FALLBACK,
    DEGRADED
}
