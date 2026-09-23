package com.github.lisama569.resilientcachegatewayjava.model;

/**
 * 请求被拒绝（过载保护/排队超时/熔断）的确定性原因。
 */
public enum RejectReason {
    /** 该热点 key 并发数已达 perKeyMaxConcurrent 上限。 */
    PER_KEY_OVERLOADED,
    /** 全局上游并发已达 globalMaxConcurrent 上限。 */
    GLOBAL_OVERLOADED,
    /** 排队等待合并结果超过 acquireTimeoutMillis。 */
    QUEUE_TIMEOUT,
    /** 熔断器处于打开状态且无过期兜底。 */
    CIRCUIT_OPEN,
    /** 上游调用超时且无过期兜底。 */
    UPSTREAM_TIMEOUT,
    /** 上游返回失败且无过期兜底。 */
    UPSTREAM_FAILURE
}
