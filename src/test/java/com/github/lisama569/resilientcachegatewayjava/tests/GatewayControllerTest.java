package com.github.lisama569.resilientcachegatewayjava.tests;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * REST 层集成测试：数据读取、指标导出、模拟上游调参。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "gateway.cache-ttl-millis=1000",
                "gateway.upstream-timeout-millis=300",
                "gateway.simulated-latency-millis=10",
                "gateway.simulated-failure=false"
        })
class GatewayControllerTest {

    private static final Logger log = LoggerFactory.getLogger(GatewayControllerTest.class);

    @Autowired
    private TestRestTemplate rest;

    @Test
    void dataEndpointReturnsFreshWithOrigin() {
        ResponseEntity<Map> r1 = rest.getForEntity("/api/data/demo", Map.class);
        ResponseEntity<Map> r2 = rest.getForEntity("/api/data/demo", Map.class);
        log.info("[case] REST读取: r1={} r2={} 判定依据=origin均为FRESH且值一致(缓存命中)",
                r1.getBody(), r2.getBody());
        assertEquals(HttpStatus.OK, r1.getStatusCode());
        assertEquals("FRESH", r1.getBody().get("origin"));
        assertEquals(r1.getBody().get("value"), r2.getBody().get("value"));
    }

    @Test
    void upstreamConfigEndpointTriggersDegraded() {
        rest.postForEntity("/api/upstream/config?latencyMs=800&failure=false", null, Map.class);
        ResponseEntity<Map> r = rest.getForEntity("/api/data/slow-rest-key", Map.class);
        log.info("[case] REST超时降级: latencyMs=800 timeoutMs=300 判定依据=origin=DEGRADED, body={}",
                r.getBody());
        assertEquals("DEGRADED", r.getBody().get("origin"));
        // 恢复，避免影响其他用例
        rest.postForEntity("/api/upstream/config?latencyMs=10&failure=false", null, Map.class);
    }

    @Test
    void metricsEndpointExportsCountersAndPercentiles() {
        rest.getForEntity("/api/data/metrics-key", Map.class);
        ResponseEntity<Map> r = rest.getForEntity("/api/metrics", Map.class);
        log.info("[case] 指标导出: body={} 判定依据=包含请求量/命中/耗时分位", r.getBody());
        assertEquals(HttpStatus.OK, r.getStatusCode());
        assertTrue(((Number) r.getBody().get("totalRequests")).longValue() >= 1);
        assertNotNull(r.getBody().get("freshHits"));
        Map<?, ?> latency = (Map<?, ?>) r.getBody().get("latency");
        assertNotNull(latency);
        assertNotNull(latency.get("p50"));
        assertNotNull(latency.get("p95"));
        assertNotNull(latency.get("p99"));
    }
}
