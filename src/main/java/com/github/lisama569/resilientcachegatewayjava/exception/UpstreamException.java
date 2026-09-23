package com.github.lisama569.resilientcachegatewayjava.exception;

/**
 * 模拟上游返回的业务失败（非超时）。
 */
public class UpstreamException extends RuntimeException {
    public UpstreamException(String message) { super(message); }
}
