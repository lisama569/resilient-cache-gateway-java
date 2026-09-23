package com.github.lisama569.resilientcachegatewayjava.model;

/**
 * 模拟上游成功返回的数据。不携带时间戳：缓存年龄必须由网关在
 * "收到并写入缓存"的同一时钟源上标记，避免传输/排队延迟造成误判。
 */
public record UpstreamResponse(String target, String value) {
}
