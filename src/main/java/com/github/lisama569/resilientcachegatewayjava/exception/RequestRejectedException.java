package com.github.lisama569.resilientcachegatewayjava.exception;

import com.github.lisama569.resilientcachegatewayjava.model.RejectReason;

/**
 * 请求因过载保护、排队超时或熔断被明确拒绝。
 * 调用链可据此返回确定性的 429/503，绝不静默丢结果。
 */
public class RequestRejectedException extends RuntimeException {

    private final RejectReason reason;
    private final String target;

    public RequestRejectedException(String target, RejectReason reason, String message) {
        super(message);
        this.target = target;
        this.reason = reason;
    }

    public RejectReason reason() { return reason; }
    public String target() { return target; }
}
