package com.github.lisama569.resilientcachegatewayjava.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 网关可配置项（前缀 gateway）。
 */
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    /** 缓存有效期（毫秒）。 */
    private long cacheTtlMillis = 2000;

    /** 上游调用超时（毫秒）。 */
    private long upstreamTimeoutMillis = 500;

    /** 单个热点 key 允许的最大并发上游回源数之外的排队/并发请求上限。 */
    private int maxConcurrentPerKey = 8;

    /** 模拟上游基础延迟（毫秒）。 */
    private long simulatedLatencyMillis = 50;

    /** 模拟上游是否强制失败。 */
    private boolean simulatedFailure = false;

    /** 过期后的宽限窗口（毫秒）：窗口内返回旧结果并触发一次后台更新。0 表示关闭。 */
    private long swrGraceMillis = 0;

    /** 宽限窗口内后台更新连续失败达到该次数后，不再用旧结果顶着，落回同步兜底。 */
    private int swrMaxBackgroundFailures = 3;

    /** 全局同时访问上游的最大并发数（真实回源压力上限）。 */
    private int maxConcurrentUpstream = 16;

    /** 全局容量满时，请求在有界队列中等待的最长时间（毫秒）。 */
    private long queueWaitTimeoutMillis = 200;

    /** 全局容量等待队列的最大长度，超出即按“总量超限”拒绝。 */
    private int maxQueueSize = 64;

    /** 熔断器：连续失败/超时达到该次数后进入保护期。 */
    private int circuitFailureThreshold = 5;

    /** 熔断器：保护期时长（毫秒），期间不打上游。 */
    private long circuitOpenMillis = 5000;

    /** 熔断器：保护期结束后允许同时试探的请求数。 */
    private int circuitHalfOpenProbes = 1;

    public long getCacheTtlMillis() {
        return cacheTtlMillis;
    }

    public void setCacheTtlMillis(long cacheTtlMillis) {
        this.cacheTtlMillis = cacheTtlMillis;
    }

    public long getUpstreamTimeoutMillis() {
        return upstreamTimeoutMillis;
    }

    public void setUpstreamTimeoutMillis(long upstreamTimeoutMillis) {
        this.upstreamTimeoutMillis = upstreamTimeoutMillis;
    }

    public int getMaxConcurrentPerKey() {
        return maxConcurrentPerKey;
    }

    public void setMaxConcurrentPerKey(int maxConcurrentPerKey) {
        this.maxConcurrentPerKey = maxConcurrentPerKey;
    }

    public long getSimulatedLatencyMillis() {
        return simulatedLatencyMillis;
    }

    public void setSimulatedLatencyMillis(long simulatedLatencyMillis) {
        this.simulatedLatencyMillis = simulatedLatencyMillis;
    }

    public boolean isSimulatedFailure() {
        return simulatedFailure;
    }

    public void setSimulatedFailure(boolean simulatedFailure) {
        this.simulatedFailure = simulatedFailure;
    }

    public long getSwrGraceMillis() {
        return swrGraceMillis;
    }

    public void setSwrGraceMillis(long swrGraceMillis) {
        this.swrGraceMillis = swrGraceMillis;
    }

    public int getSwrMaxBackgroundFailures() {
        return swrMaxBackgroundFailures;
    }

    public void setSwrMaxBackgroundFailures(int swrMaxBackgroundFailures) {
        this.swrMaxBackgroundFailures = swrMaxBackgroundFailures;
    }

    public int getMaxConcurrentUpstream() {
        return maxConcurrentUpstream;
    }

    public void setMaxConcurrentUpstream(int maxConcurrentUpstream) {
        this.maxConcurrentUpstream = maxConcurrentUpstream;
    }

    public long getQueueWaitTimeoutMillis() {
        return queueWaitTimeoutMillis;
    }

    public void setQueueWaitTimeoutMillis(long queueWaitTimeoutMillis) {
        this.queueWaitTimeoutMillis = queueWaitTimeoutMillis;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public int getCircuitFailureThreshold() {
        return circuitFailureThreshold;
    }

    public void setCircuitFailureThreshold(int circuitFailureThreshold) {
        this.circuitFailureThreshold = circuitFailureThreshold;
    }

    public long getCircuitOpenMillis() {
        return circuitOpenMillis;
    }

    public void setCircuitOpenMillis(long circuitOpenMillis) {
        this.circuitOpenMillis = circuitOpenMillis;
    }

    public int getCircuitHalfOpenProbes() {
        return circuitHalfOpenProbes;
    }

    public void setCircuitHalfOpenProbes(int circuitHalfOpenProbes) {
        this.circuitHalfOpenProbes = circuitHalfOpenProbes;
    }
}
