package com.github.lisama569.resilientcachegatewayjava.upstream;

import com.github.lisama569.resilientcachegatewayjava.config.GatewayProperties;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 模拟上游运行态的默认实现（线程安全）。
 *
 * 延迟优先级：目标单独配置 delays > slow 标记（slowDelayMillis）> defaultDelayMillis。
 * failing 标记使调用抛 UpstreamException；slow 标记使延迟放大到 slowDelayMillis，
 * 配合较短的 upstreamTimeoutMillis 即可在本地稳定复现超时。
 */
public class DefaultSimulationState implements SimulationState {

    private final long defaultDelayMillis;
    private final long slowDelayMillis;

    private final Map<String, Long> delays = new ConcurrentHashMap<>();
    private final Set<String> failingKeys = ConcurrentHashMap.newKeySet();
    private final Set<String> slowKeys = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicLong> callCounts = new ConcurrentHashMap<>();
    private final AtomicLong totalCalls = new AtomicLong();

    public DefaultSimulationState(GatewayProperties properties) {
        GatewayProperties.Upstream cfg = properties.getUpstream();
        this.defaultDelayMillis = cfg.getDefaultDelayMillis();
        this.slowDelayMillis = cfg.getSlowDelayMillis();
        this.delays.putAll(cfg.getDelays());
        this.failingKeys.addAll(cfg.getFailingKeys());
        this.slowKeys.addAll(cfg.getSlowKeys());
    }

    @Override
    public long delayMillis(String target) {
        Long explicit = delays.get(target);
        if (explicit != null) {
            return explicit;
        }
        return slowKeys.contains(target) ? slowDelayMillis : defaultDelayMillis;
    }

    @Override
    public boolean isFailing(String target) {
        return failingKeys.contains(target);
    }

    @Override
    public boolean isSlow(String target) {
        return slowKeys.contains(target);
    }

    /** 记录一次实际发生的上游调用（由 SimulatedUpstreamClient 调用）。 */
    public void recordCall(String target) {
        callCounts.computeIfAbsent(target, k -> new AtomicLong()).incrementAndGet();
        totalCalls.incrementAndGet();
    }

    @Override
    public long callCount(String target) {
        AtomicLong c = callCounts.get(target);
        return c == null ? 0L : c.get();
    }

    @Override
    public long totalCallCount() {
        return totalCalls.get();
    }

    @Override
    public void setDelay(String target, long delayMillis) {
        if (delayMillis < 0) {
            throw new IllegalArgumentException("delayMillis must be >= 0");
        }
        delays.put(target, delayMillis);
        slowKeys.remove(target);
    }

    @Override
    public void setFailing(String target, boolean failing) {
        if (failing) {
            failingKeys.add(target);
        } else {
            failingKeys.remove(target);
        }
    }

    @Override
    public void setSlow(String target, boolean slow) {
        if (slow) {
            slowKeys.add(target);
        } else {
            slowKeys.remove(target);
        }
    }

    @Override
    public void reset() {
        delays.clear();
        failingKeys.clear();
        slowKeys.clear();
        callCounts.clear();
        totalCalls.set(0L);
    }

    @Override
    public Snapshot snapshot() {
        Map<String, Long> counts = new ConcurrentHashMap<>();
        callCounts.forEach((k, v) -> counts.put(k, v.get()));
        return new Snapshot(Map.copyOf(delays), Set.copyOf(failingKeys),
                Set.copyOf(slowKeys), Map.copyOf(counts), totalCalls.get());
    }
}
