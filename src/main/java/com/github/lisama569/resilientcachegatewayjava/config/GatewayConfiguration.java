package com.github.lisama569.resilientcachegatewayjava.config;

import com.github.lisama569.resilientcachegatewayjava.cache.CacheEntryStore;
import com.github.lisama569.resilientcachegatewayjava.cache.InMemoryCacheEntryStore;
import com.github.lisama569.resilientcachegatewayjava.core.TimeSource;
import com.github.lisama569.resilientcachegatewayjava.metrics.MetricsRegistry;
import com.github.lisama569.resilientcachegatewayjava.resilience.CircuitBreakerRegistry;
import com.github.lisama569.resilientcachegatewayjava.service.CacheGatewayService;
import com.github.lisama569.resilientcachegatewayjava.upstream.DefaultSimulationState;
import com.github.lisama569.resilientcachegatewayjava.upstream.SimulatedUpstreamClient;
import com.github.lisama569.resilientcachegatewayjava.upstream.SimulationState;
import com.github.lisama569.resilientcachegatewayjava.upstream.UpstreamClient;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 网关组件装配。
 */
@Configuration
@EnableConfigurationProperties(GatewayProperties.class)
public class GatewayConfiguration {

    @Bean
    public TimeSource timeSource() {
        return TimeSource.system();
    }

    @Bean
    public SimulationState simulationState(GatewayProperties props) {
        return new DefaultSimulationState(props);
    }

    @Bean
    public UpstreamClient upstreamClient(SimulationState state, TimeSource timeSource) {
        return new SimulatedUpstreamClient(state, timeSource);
    }

    @Bean
    public CacheEntryStore cacheEntryStore() {
        return new InMemoryCacheEntryStore();
    }

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry(GatewayProperties props) {
        return new CircuitBreakerRegistry(props);
    }

    @Bean
    public MetricsRegistry metricsRegistry() {
        return new MetricsRegistry();
    }

    @Bean
    public CacheGatewayService cacheGatewayService(CacheEntryStore store, UpstreamClient upstream,
                                                   GatewayProperties props, TimeSource timeSource,
                                                   CircuitBreakerRegistry circuits,
                                                   MetricsRegistry metrics) {
        return new CacheGatewayService(store, upstream, props, timeSource, circuits, metrics);
    }
}
