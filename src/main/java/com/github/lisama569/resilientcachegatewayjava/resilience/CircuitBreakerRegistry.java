package com.github.lisama569.resilientcachegatewayjava.resilience;

import com.github.lisama569.resilientcachegatewayjava.config.GatewayProperties;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按目标持有独立熔断器，并提供管理面查询/重置。
 */
public class CircuitBreakerRegistry {

    private final GatewayProperties properties;
    private final ConcurrentHashMap<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();

    public CircuitBreakerRegistry(GatewayProperties properties) {
        this.properties = properties;
    }

    public CircuitBreaker forTarget(String target) {
        return breakers.computeIfAbsent(target, t -> new CircuitBreaker(
                properties.getFailureThreshold(),
                properties.getOpenStateMillis(),
                properties.getHalfOpenPermits()));
    }

    public Map<String, CircuitBreaker> all() {
        return breakers;
    }

    public void reset(String target) {
        breakers.remove(target);
    }

    public void resetAll() {
        breakers.clear();
    }
}
