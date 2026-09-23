package com.github.lisama569.resilientcachegatewayjava.web;

import com.github.lisama569.resilientcachegatewayjava.cache.CacheEntryStore;
import com.github.lisama569.resilientcachegatewayjava.metrics.MetricsRegistry;
import com.github.lisama569.resilientcachegatewayjava.resilience.CircuitBreaker;
import com.github.lisama569.resilientcachegatewayjava.resilience.CircuitBreakerRegistry;
import com.github.lisama569.resilientcachegatewayjava.resilience.CircuitState;
import com.github.lisama569.resilientcachegatewayjava.upstream.SimulationState;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 本地验证用管理面：
 *  - 运行期调整模拟上游每个目标的延迟/故障/慢调用；
 *  - 导出指标（请求量、命中类别、拒绝原因、耗时分位）；
 *  - 查看/清理缓存与熔断器。
 */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private final SimulationState simulation;
    private final MetricsRegistry metrics;
    private final CacheEntryStore store;
    private final CircuitBreakerRegistry circuits;

    public AdminController(SimulationState simulation, MetricsRegistry metrics,
                           CacheEntryStore store, CircuitBreakerRegistry circuits) {
        this.simulation = simulation;
        this.metrics = metrics;
        this.store = store;
        this.circuits = circuits;
    }

    /** 查看模拟上游当前配置与调用计数。 */
    @GetMapping("/simulation")
    public SimulationState.Snapshot simulation() {
        return simulation.snapshot();
    }

    /**
     * 调整目标行为，所有字段可选：
     * {"delayMillis":100} 单独延迟（设置后 slow 标记自动清除）
     * {"failing":true}    上游业务失败
     * {"slow":true}       慢调用（延迟放大为 slowDelayMillis）
     */
    @PostMapping("/simulation/{target}")
    public Map<String, String> configure(@PathVariable String target,
                                         @RequestBody Map<String, Object> body) {
        if (body.containsKey("delayMillis")) {
            simulation.setDelay(target, ((Number) body.get("delayMillis")).longValue());
        }
        if (body.containsKey("failing")) {
            simulation.setFailing(target, Boolean.TRUE.equals(body.get("failing")));
        }
        if (body.containsKey("slow")) {
            simulation.setSlow(target, Boolean.TRUE.equals(body.get("slow")));
        }
        return Map.of("target", target, "result", "updated");
    }

    @PostMapping("/simulation/reset")
    public Map<String, String> resetSimulation() {
        simulation.reset();
        return Map.of("result", "simulation reset");
    }

    /** 导出全量指标快照。 */
    @GetMapping("/metrics/export")
    public Map<String, Object> metrics() {
        return metrics.snapshot();
    }

    @PostMapping("/metrics/reset")
    public Map<String, String> resetMetrics() {
        metrics.reset();
        return Map.of("result", "metrics reset");
    }

    /** 查看缓存条目数量。 */
    @GetMapping("/cache")
    public Map<String, Object> cache() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("size", store.size());
        return out;
    }

    @DeleteMapping("/cache/{target}")
    public Map<String, String> evict(@PathVariable String target) {
        store.remove(target);
        return Map.of("target", target, "result", "evicted");
    }

    @DeleteMapping("/cache")
    public Map<String, String> clearCache() {
        store.clear();
        return Map.of("result", "cache cleared");
    }

    /** 查看各目标熔断器状态。 */
    @GetMapping("/circuits")
    public Map<String, Object> circuits() {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, CircuitBreaker> e : circuits.all().entrySet()) {
            CircuitBreaker cb = e.getValue();
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("state", cb.state().name());
            info.put("consecutiveFailures", cb.consecutiveFailures());
            out.put(e.getKey(), info);
        }
        return out;
    }

    @PostMapping("/circuits/{target}/reset")
    public Map<String, String> resetCircuit(@PathVariable String target) {
        circuits.reset(target);
        return Map.of("target", target, "result", "circuit reset");
    }
}
