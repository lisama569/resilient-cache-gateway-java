package com.github.lisama569.resilientcachegatewayjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 网关全部可配置项，前缀 gateway.*。
 */
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    /** 缓存新鲜有效期（毫秒），期内直接返回 FRESH。 */
    private long ttlMillis = 5000;

    /** 过期后多长时间内允许作为 STALE_FALLBACK 兜底（毫秒）。 */
    private long staleTtlMillis = 10000;

    /** 单次上游调用超时（毫秒）。 */
    private long upstreamTimeoutMillis = 800;

    /** 单个热点 key 允许参与合并的最大并发请求数（含 1 个实际加载者）。 */
    private int perKeyMaxConcurrent = 10;

    /** 全局同时进行的上游调用上限。 */
    private int globalMaxConcurrent = 200;

    /** 等待他人合并结果的最长排队时间（毫秒）。 */
    private long acquireTimeoutMillis = 100;

    /** 熔断器连续失败阈值。 */
    private int failureThreshold = 5;

    /** 熔断打开持续时间（毫秒），过后进入半开试探。 */
    private long openStateMillis = 2000;

    /** 半开状态允许的试探调用数。 */
    private int halfOpenPermits = 1;

    /** 上游失败时是否允许返回过期兜底。 */
    private boolean staleOnFailureEnabled = true;

    /** 执行上游调用的线程池大小。 */
    private int upstreamPoolSize = 200;

    private final Upstream upstream = new Upstream();

    public long getTtlMillis() { return ttlMillis; }
    public void setTtlMillis(long ttlMillis) { this.ttlMillis = ttlMillis; }
    public long getStaleTtlMillis() { return staleTtlMillis; }
    public void setStaleTtlMillis(long staleTtlMillis) { this.staleTtlMillis = staleTtlMillis; }
    public long getUpstreamTimeoutMillis() { return upstreamTimeoutMillis; }
    public void setUpstreamTimeoutMillis(long upstreamTimeoutMillis) { this.upstreamTimeoutMillis = upstreamTimeoutMillis; }
    public int getPerKeyMaxConcurrent() { return perKeyMaxConcurrent; }
    public void setPerKeyMaxConcurrent(int perKeyMaxConcurrent) { this.perKeyMaxConcurrent = perKeyMaxConcurrent; }
    public int getGlobalMaxConcurrent() { return globalMaxConcurrent; }
    public void setGlobalMaxConcurrent(int globalMaxConcurrent) { this.globalMaxConcurrent = globalMaxConcurrent; }
    public long getAcquireTimeoutMillis() { return acquireTimeoutMillis; }
    public void setAcquireTimeoutMillis(long acquireTimeoutMillis) { this.acquireTimeoutMillis = acquireTimeoutMillis; }
    public int getFailureThreshold() { return failureThreshold; }
    public void setFailureThreshold(int failureThreshold) { this.failureThreshold = failureThreshold; }
    public long getOpenStateMillis() { return openStateMillis; }
    public void setOpenStateMillis(long openStateMillis) { this.openStateMillis = openStateMillis; }
    public int getHalfOpenPermits() { return halfOpenPermits; }
    public void setHalfOpenPermits(int halfOpenPermits) { this.halfOpenPermits = halfOpenPermits; }
    public boolean isStaleOnFailureEnabled() { return staleOnFailureEnabled; }
    public void setStaleOnFailureEnabled(boolean staleOnFailureEnabled) { this.staleOnFailureEnabled = staleOnFailureEnabled; }
    public int getUpstreamPoolSize() { return upstreamPoolSize; }
    public void setUpstreamPoolSize(int upstreamPoolSize) { this.upstreamPoolSize = upstreamPoolSize; }
    public Upstream getUpstream() { return upstream; }

    /** 模拟上游的初始配置（运行期可通过 admin 接口调整）。 */
    public static class Upstream {
        private long defaultDelayMillis = 100;
        private long slowDelayMillis = 3000;
        private Map<String, Long> delays = new HashMap<>();
        private Set<String> failingKeys = new HashSet<>();
        private Set<String> slowKeys = new HashSet<>();

        public long getDefaultDelayMillis() { return defaultDelayMillis; }
        public void setDefaultDelayMillis(long defaultDelayMillis) { this.defaultDelayMillis = defaultDelayMillis; }
        public long getSlowDelayMillis() { return slowDelayMillis; }
        public void setSlowDelayMillis(long slowDelayMillis) { this.slowDelayMillis = slowDelayMillis; }
        public Map<String, Long> getDelays() { return delays; }
        public void setDelays(Map<String, Long> delays) { this.delays = delays; }
        public Set<String> getFailingKeys() { return failingKeys; }
        public void setFailingKeys(Set<String> failingKeys) { this.failingKeys = failingKeys; }
        public Set<String> getSlowKeys() { return slowKeys; }
        public void setSlowKeys(Set<String> slowKeys) { this.slowKeys = slowKeys; }
    }
}
