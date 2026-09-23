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
}
