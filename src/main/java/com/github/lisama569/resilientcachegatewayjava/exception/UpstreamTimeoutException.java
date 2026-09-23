package com.github.lisama569.resilientcachegatewayjava.exception;

/**
 * 模拟上游调用超过 upstreamTimeoutMillis。
 */
public class UpstreamTimeoutException extends RuntimeException {
    public UpstreamTimeoutException(String message) { super(message); }
}
