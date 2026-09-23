package com.github.lisama569.resilientcachegatewayjava.gateway;

import java.time.Instant;

/**
 * 网关统一返回模型。
 */
public record GatewayResponse(
        String key,
        String value,
        ResultOrigin origin,
        Instant fetchedAt,
        long elapsedMillis) {
}
