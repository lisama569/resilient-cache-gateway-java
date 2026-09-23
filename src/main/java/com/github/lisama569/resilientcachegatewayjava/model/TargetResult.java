package com.github.lisama569.resilientcachegatewayjava.model;

/**
 * 对上游目标读取的最终聚合结果。
 *
 * @param target       目标标识
 * @param status       结果类别
 * @param value        数据负载（DEGRADED 时可能为 null）
 * @param reason       判定依据/降级原因说明
 * @param upstreamAt   数据来源时间戳（毫秒），降级时为 0
 * @param elapsedNanos 本次请求处理耗时（纳秒）
 */
public record TargetResult(String target, ResultStatus status, String value,
                           String reason, long upstreamAt, long elapsedNanos) {
}
