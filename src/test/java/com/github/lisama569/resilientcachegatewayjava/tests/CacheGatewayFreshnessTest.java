package com.github.lisama569.resilientcachegatewayjava.tests;

import com.github.lisama569.resilientcachegatewayjava.cache.CacheEntryStore;
import com.github.lisama569.resilientcachegatewayjava.cache.InMemoryCacheEntryStore;
import com.github.lisama569.resilientcachegatewayjava.config.GatewayProperties;
import com.github.lisama569.resilientcachegatewayjava.metrics.MetricsRegistry;
import com.github.lisama569.resilientcachegatewayjava.model.ResultStatus;
import com.github.lisama569.resilientcachegatewayjava.model.TargetResult;
import com.github.lisama569.resilientcachegatewayjava.resilience.CircuitBreakerRegistry;
import com.github.lisama569.resilientcachegatewayjava.service.CacheGatewayService;
import com.github.lisama569.resilientcachegatewayjava.upstream.DefaultSimulationState;
import com.github.lisama569.resilientcachegatewayjava.upstream.SimulatedUpstreamClient;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 缓存有效期与过期判定（直接装配真实组件 + 可控时钟与模拟上游）：
 * 首次未缓存 -> 上游 FRESH；TTL 内 -> 命中 FRESH 且不再调上游；
 * TTL 后 -> 必须重新上游刷新；超过 staleTtl 的旧值绝不作为新鲜数据返回。
 */
class CacheGatewayFreshnessTest {

    private static final Logger log = LoggerFactory.getLogger(CacheGatewayFreshnessTest.class);

    private record Harness(CacheGatewayService service, DefaultSimulationState state,
                           MutableTimeSource clock, GatewayProperties props) {
    }

    private Harness harness(long ttl, long staleTtl, long delay) {
        GatewayProperties props = new GatewayProperties();
        props.setTtlMillis(ttl);
        props.setStaleTtlMillis(staleTtl);
        props.setUpstreamTimeoutMillis(5000);
        props.setPerKeyMaxConcurrent(10);
        props.setAcquireTimeoutMillis(2000);
        props.setFailureThreshold(5);
        props.setOpenStateMillis(1000);
        props.getUpstream().setDefaultDelayMillis(delay);

        MutableTimeSource clock = new MutableTimeSource();
        CacheEntryStore store = new InMemoryCacheEntryStore();
        DefaultSimulationState state = new DefaultSimulationState(props);
        SimulatedUpstreamClient client = new SimulatedUpstreamClient(state, clock);
        CircuitBreakerRegistry circuits = new CircuitBreakerRegistry(props);
        MetricsRegistry metrics = new MetricsRegistry();
        CacheGatewayService service = new CacheGatewayService(
                store, client, props, clock, circuits, metrics);
        return new Harness(service, state, clock, props);
    }

    @Test
    void firstFetchIsFreshFromUpstream() {
        Harness h = harness(1000, 1000, 20);
        TargetResult r1 = h.service.fetch("k1");
        log.info("[fresh-test] 首次读取 target=k1 status={} value={} reason={}",
                r1.status(), r1.value(), r1.reason());
        assertThat(r1.status()).isEqualTo(ResultStatus.FRESH);
        assertThat(r1.value()).startsWith("value-of-k1-at-");
        assertThat(h.state.callCount("k1")).isEqualTo(1L);
    }

    @Test
    void hitsWithinTtlDoNotCallUpstream() {
        Harness h = harness(1000, 1000, 20);
        TargetResult r1 = h.service.fetch("k2");
        h.clock.advanceMillis(500); // 仍在 ttl=1000 内
        TargetResult r2 = h.service.fetch("k2");
        TargetResult r3 = h.service.fetch("k2");

        log.info("[fresh-test] ttl=1000ms, age=500ms 内连续读取: r2.status={} r3.status={}, 上游调用次数={} 期望 1",
                r2.status(), r3.status(), h.state.callCount("k2"));
        assertThat(r2.status()).isEqualTo(ResultStatus.FRESH);
        assertThat(r3.status()).isEqualTo(ResultStatus.FRESH);
        assertThat(r2.value()).isEqualTo(r1.value());
        assertThat(h.state.callCount("k2")).isEqualTo(1L);
    }

    @Test
    void expiredEntryTriggersUpstreamRefreshWithNewValue() throws Exception {
        Harness h = harness(200, 500, 20);
        TargetResult r1 = h.service.fetch("k3");
        assertThat(r1.status()).isEqualTo(ResultStatus.FRESH);

        h.clock.advanceMillis(250); // 已过期但在 stale 窗口；上游正常 -> 必须刷新
        TargetResult r2 = h.service.fetch("k3");
        log.info("[fresh-test] ttl=200ms age=250ms 已过期, status={}（期望 FRESH，必须重新上游），上游次数={}",
                r2.status(), h.state.callCount("k3"));
        assertThat(r2.status()).isEqualTo(ResultStatus.FRESH);
        assertThat(r2.upstreamAt()).isGreaterThan(r1.upstreamAt());
        assertThat(h.state.callCount("k3")).isEqualTo(2L);
    }

    @Test
    void freshHitAndRefreshedHitHaveDistinctUpstreamAt() {
        Harness h = harness(100, 500, 10);
        TargetResult r1 = h.service.fetch("k4");
        TargetResult r1b = h.service.fetch("k4");
        assertThat(r1b.upstreamAt())
                .as("TTL 内读取的 upstreamAt 必须相同（同一份缓存）")
                .isEqualTo(r1.upstreamAt());

        h.clock.advanceMillis(150);
        TargetResult r2 = h.service.fetch("k4");
        log.info("[fresh-test] 过期刷新前 upstreamAt={}, 刷新后 upstreamAt={}",
                r1.upstreamAt(), r2.upstreamAt());
        assertThat(r2.upstreamAt()).isGreaterThan(r1.upstreamAt());
    }
}
