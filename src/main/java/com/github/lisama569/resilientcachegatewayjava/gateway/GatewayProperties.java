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

    /** 缓存过期后的宽限窗口（毫秒）：窗口内返回旧值并触发一次后台更新。 */
    private long staleGraceMillis = 3000;

    /** 宽限窗口内后台更新允许的最大连续失败次数，超过后落回兜底分类。 */
    private int staleMaxRefreshFailures = 3;

    /** 全局同时压向上游的最大并发数。 */
    private int globalMaxConcurrent = 32;

    /** 全局排队容量（等待中的请求数上限），超出即拒绝。 */
    private int globalQueueCapacity = 64;

    /** 全局排队最长等待时间（毫秒），超时拒绝。 */
    private long globalQueueTimeoutMillis = 200;

    /** 熔断阈值：连续失败/超时达到该次数后进入保护期。 */
    private int circuitFailureThreshold = 5;

    /** 熔断保护期（毫秒）：期间不访问上游。 */
    private long circuitOpenMillis = 5000;

    /** 试探期允许同时放行的试探请求数。 */
    private int circuitProbeCount = 2;

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

    public long getStaleGraceMillis() {
        return staleGraceMillis;
    }

    public void setStaleGraceMillis(long staleGraceMillis) {
        this.staleGraceMillis = staleGraceMillis;
    }

    public int getStaleMaxRefreshFailures() {
        return staleMaxRefreshFailures;
    }

    public void setStaleMaxRefreshFailures(int staleMaxRefreshFailures) {
        this.staleMaxRefreshFailures = staleMaxRefreshFailures;
    }

    public int getGlobalMaxConcurrent() {
        return globalMaxConcurrent;
    }

    public void setGlobalMaxConcurrent(int globalMaxConcurrent) {
        this.globalMaxConcurrent = globalMaxConcurrent;
    }

    public int getGlobalQueueCapacity() {
        return globalQueueCapacity;
    }

    public void setGlobalQueueCapacity(int globalQueueCapacity) {
        this.globalQueueCapacity = globalQueueCapacity;
    }

    public long getGlobalQueueTimeoutMillis() {
        return globalQueueTimeoutMillis;
    }

    public void setGlobalQueueTimeoutMillis(long globalQueueTimeoutMillis) {
        this.globalQueueTimeoutMillis = globalQueueTimeoutMillis;
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

    public int getCircuitProbeCount() {
        return circuitProbeCount;
    }

    public void setCircuitProbeCount(int circuitProbeCount) {
        this.circuitProbeCount = circuitProbeCount;
    }
}
