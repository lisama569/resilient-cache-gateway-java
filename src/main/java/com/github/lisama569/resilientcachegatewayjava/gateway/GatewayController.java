package com.github.lisama569.resilientcachegatewayjava.gateway;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 对外 REST 入口：数据读取、指标导出、模拟上游运行期调参。
 */
@RestController
@RequestMapping("/api")
public class GatewayController {

    private final CacheGatewayService gatewayService;
    private final GatewayMetrics metrics;
    private final GatewayProperties properties;

    public GatewayController(CacheGatewayService gatewayService,
                             GatewayMetrics metrics,
                             GatewayProperties properties) {
        this.gatewayService = gatewayService;
        this.metrics = metrics;
        this.properties = properties;
    }

    /** 读取某个 key 的数据，返回体中带 origin 区分新鲜/过期兜底/降级。 */
    @GetMapping("/data/{key}")
    public GatewayResponse getData(@PathVariable String key) {
        return gatewayService.get(key);
    }

    /** 导出指标快照。 */
    @GetMapping("/metrics")
    public Map<String, Object> getMetrics() {
        return metrics.snapshot();
    }

    /** 运行期调整模拟上游行为，便于本地复现超时与故障场景。 */
    @PostMapping("/upstream/config")
    public Map<String, Object> configureUpstream(
            @RequestParam(required = false) Long latencyMs,
            @RequestParam(required = false) Boolean failure) {
        if (latencyMs != null) {
            properties.setSimulatedLatencyMillis(latencyMs);
        }
        if (failure != null) {
            properties.setSimulatedFailure(failure);
        }
        Map<String, Object> current = new LinkedHashMap<>();
        current.put("simulatedLatencyMillis", properties.getSimulatedLatencyMillis());
        current.put("simulatedFailure", properties.isSimulatedFailure());
        return current;
    }

    /** 并发超限：确定的 429 拒绝语义。 */
    @ExceptionHandler(CacheGatewayService.OverloadRejectedException.class)
    public ResponseEntity<Map<String, Object>> handleOverload(
            CacheGatewayService.OverloadRejectedException e) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", "OVERLOAD_REJECTED");
        body.put("message", e.getMessage());
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(body);
    }
}
