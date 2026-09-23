package com.github.lisama569.resilientcachegatewayjava.tests;

import com.github.lisama569.resilientcachegatewayjava.cache.CacheEntryStore;
import com.github.lisama569.resilientcachegatewayjava.cache.InMemoryCacheEntryStore;
import com.github.lisama569.resilientcachegatewayjava.config.GatewayProperties;
import com.github.lisama569.resilientcachegatewayjava.exception.RequestRejectedException;
import com.github.lisama569.resilientcachegatewayjava.metrics.MetricsRegistry;
import com.github.lisama569.resilientcachegatewayjava.model.RejectReason;
import com.github.lisama569.resilientcachegatewayjava.model.ResultStatus;
import com.github.lisama569.resilientcachegatewayjava.model.TargetResult;
import com.github.lisama569.resilientcachegatewayjava.resilience.CircuitBreakerRegistry;
import com.github.lisama569.resilientcachegatewayjava.resilience.CircuitState;
import com.github.lisama569.resilientcachegatewayjava.service.CacheGatewayService;
import com.github.lisama569.resilientcachegatewayjava.upstream.DefaultSimulationState;
import com.github.lisama569.resilientcachegatewayjava.upstream.SimulatedUpstreamClient;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 故障边界测试：
 *  - 上游业务失败 + 窗口内有旧值 -> STALE_FALLBACK；
 *  - 上游超时 + 窗口内有旧值 -> STALE_FALLBACK，reason 明确为超时；
 *  - 持续失败无缓存 -> DEGRADED；
 *  - 熔断打开后有旧值 -> STALE_FALLBACK（reason=CIRCUIT_OPEN），无旧值 -> 503 式拒绝；
 *  - 超过 staleTtl 的旧值不得用于兜底。
 */
class CacheGatewayFailureTest {

    private static final Logger log = LoggerFactory.getLogger(CacheGatewayFailureTest.class);

    private record Harness(CacheGatewayService service, DefaultSimulationState state,
                           MutableTimeSource clock, CacheEntryStore store,
                           CircuitBreakerRegistry circuits, GatewayProperties props) {
    }

    private Harness harness() {
        GatewayProperties props = new GatewayProperties();
        props.setTtlMillis(200);
        props.setStaleTtlMillis(1000);
        props.setUpstreamTimeoutMillis(100);
        props.setPerKeyMaxConcurrent(10);
        props.setAcquireTimeoutMillis(3000);
        props.setFailureThreshold(3);
        props.setOpenStateMillis(2000);
        props.setHalfOpenPermits(1);
        props.getUpstream().setDefaultDelayMillis(10);

        MutableTimeSource clock = new MutableTimeSource();
        CacheEntryStore store = new InMemoryCacheEntryStore();
        DefaultSimulationState state = new DefaultSimulationState(props);
        SimulatedUpstreamClient client = new SimulatedUpstreamClient(state, clock);
        CircuitBreakerRegistry circuits = new CircuitBreakerRegistry(props);
        MetricsRegistry metrics = new MetricsRegistry();
        CacheGatewayService service = new CacheGatewayService(
                store, client, props, clock, circuits, metrics);
        return new Harness(service, state, clock, store, circuits, props);
    }

    @Test
    void upstreamFailureReturnsStaleFallbackWithinStaleWindow() {
        Harness h = harness();
        TargetResult ok = h.service.fetch("fail-key");
        assertThat(ok.status()).isEqualTo(ResultStatus.FRESH);

        h.state.setFailing("fail-key", true);
        h.clock.advanceMillis(500); // ttl=200 已过，仍在 staleTtl=1000 窗口内
        TargetResult stale = h.service.fetch("fail-key");

        log.info("[fail-test] 输入: 旧缓存 age=500ms, ttl=200, staleTtl=1000, 上游 failing=true");
        log.info("[fail-test] 判定: status={}, reason={}, value 与旧值相等={}",
                stale.status(), stale.reason(), stale.value().equals(ok.value()));
        assertThat(stale.status()).isEqualTo(ResultStatus.STALE_FALLBACK);
        assertThat(stale.value()).isEqualTo(ok.value());
        assertThat(stale.upstreamAt()).isEqualTo(ok.upstreamAt());
    }

    @Test
    void upstreamTimeoutIsDistinguishableAndFallsBackToStale() {
        Harness h = harness();
        TargetResult ok = h.service.fetch("slow-key");

        // 延迟 500ms > 超时 100ms，且未标记 failing -> 纯超时
        h.state.setDelay("slow-key", 500);
        h.clock.advanceMillis(300);
        TargetResult stale = h.service.fetch("slow-key");

        log.info("[fail-test] 输入: delay=500ms > timeout=100ms, 旧缓存 age=300ms");
        log.info("[fail-test] 判定: status={}, reason='{}'（须含 timeout 以区分业务失败）",
                stale.status(), stale.reason());
        assertThat(stale.status()).isEqualTo(ResultStatus.STALE_FALLBACK);
        assertThat(stale.reason()).contains("timeout");
        assertThat(stale.value()).isEqualTo(ok.value());
    }

    @Test
    void failureWithoutAnyCacheReturnsDegraded() {
        Harness h = harness();
        h.state.setFailing("no-cache", true);
        TargetResult r = h.service.fetch("no-cache");

        log.info("[fail-test] 输入: 全新 key + failing=true, 判定 status={}, reason={}",
                r.status(), r.reason());
        assertThat(r.status()).isEqualTo(ResultStatus.DEGRADED);
        assertThat(r.value()).isNull();
    }

    @Test
    void staleBeyondStaleTtlIsNotServedAsFreshOrFallback() {
        Harness h = harness();
        TargetResult ok = h.service.fetch("old");
        h.state.setFailing("old", true);

        h.clock.advanceMillis(1300); // ttl(200)+staleTtl(1000)=1200 已过
        TargetResult r = h.service.fetch("old");
        log.info("[fail-test] 输入: age=1300ms > ttl+staleTtl=1200ms, 上游失败 -> 判定 status={}",
                r.status());
        assertThat(r.status())
                .as("超过兜底窗口的旧值既不能是 FRESH 也不能是 STALE_FALLBACK")
                .isEqualTo(ResultStatus.DEGRADED);
    }

    @Test
    void openCircuitWithStaleServesStaleWithoutStaleRejects503Style() {
        Harness h = harness();

        // 先对 fresh-key 建立缓存
        TargetResult ok = h.service.fetch("fresh-key");
        assertThat(ok.status()).isEqualTo(ResultStatus.FRESH);

        // 让一个无缓存 key 连续失败打熔断器
        h.state.setFailing("fresh-key", true);
        for (int i = 0; i < 3; i++) {
            h.clock.advanceMillis(250);
            TargetResult r = h.service.fetch("fresh-key");
            assertThat(r.status()).isEqualTo(ResultStatus.STALE_FALLBACK);
        }
        CircuitState state = h.circuits.forTarget("fresh-key").state();
        log.info("[fail-test] 连续 3 次失败后熔断器状态={} 期望 OPEN", state);
        assertThat(state).isEqualTo(CircuitState.OPEN);

        // 熔断打开 + 仍在 stale 窗口：直接 STALE_FALLBACK，reason 标记 CIRCUIT_OPEN，不再打上游
        long callsBefore = h.state.callCount("fresh-key");
        h.clock.advanceMillis(100);
        TargetResult staleDuringOpen = h.service.fetch("fresh-key");
        log.info("[fail-test] OPEN 期间读取: status={}, reason='{}', 上游调用增量={} 期望 0",
                staleDuringOpen.status(), staleDuringOpen.reason(),
                h.state.callCount("fresh-key") - callsBefore);
        assertThat(staleDuringOpen.status()).isEqualTo(ResultStatus.STALE_FALLBACK);
        assertThat(staleDuringOpen.reason()).contains("CIRCUIT_OPEN");
        assertThat(h.state.callCount("fresh-key")).isEqualTo(callsBefore);

        // 无缓存 key 熔断后必须显式拒绝（CIRCUIT_OPEN），不静默丢失
        h.state.setFailing("naked", true);
        for (int i = 0; i < 3; i++) {
            h.service.fetch("naked");
        }
        assertThat(h.circuits.forTarget("naked").state()).isEqualTo(CircuitState.OPEN);
        assertThatThrownBy(() -> h.service.fetch("naked"))
                .isInstanceOf(RequestRejectedException.class)
                .satisfies(ex -> assertThat(((RequestRejectedException) ex).reason())
                        .isEqualTo(RejectReason.CIRCUIT_OPEN));
        log.info("[fail-test] OPEN 且无任何缓存 -> RequestRejectedException(CIRCUIT_OPEN)，结果可区分、不丢失");
    }
}
