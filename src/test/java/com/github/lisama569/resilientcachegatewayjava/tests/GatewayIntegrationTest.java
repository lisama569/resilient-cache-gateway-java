package com.github.lisama569.resilientcachegatewayjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * HTTP 端到端验证：
 *  1. 首次读取 200 FRESH，TTL 内重复读取仍是 FRESH；
 *  2. 配置上游故障 + 等待过期后读取 -> 200 STALE_FALLBACK（与 FRESH 可区分）；
 *  3. 全新故障 key -> 200 DEGRADED；
 *  4. 熔断打开后无缓存 key -> 503 且 reason=CIRCUIT_OPEN；
 *  5. 热点突发 -> 429 且 reason=PER_KEY_OVERLOADED；
 *  6. /admin/metrics/export 可导出计数。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class GatewayIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(GatewayIntegrationTest.class);
    private final ObjectMapper json = new ObjectMapper();

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    @Test
    void endToEndFreshStaleDegradedRejectionAndMetrics() throws Exception {
        // 1) 首次 FRESH
        String target = "it-fresh";
        ResponseEntity<String> r1 = rest.getForEntity(url("/api/targets/" + target), String.class);
        JsonNode b1 = json.readTree(r1.getBody());
        log.info("[it] 首次读取 http={} status={} reason={}", r1.getStatusCode(),
                b1.get("status").asText(), b1.get("reason").asText());
        assertThat(r1.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(b1.get("status").asText()).isEqualTo("FRESH");

        // TTL 内再读：仍 FRESH（缓存生效）
        ResponseEntity<String> r1b = rest.getForEntity(url("/api/targets/" + target), String.class);
        assertThat(json.readTree(r1b.getBody()).get("status").asText()).isEqualTo("FRESH");

        // 2) 先正常取一次建立缓存，再配置故障，等待 ttl=300ms 过期 -> STALE_FALLBACK
        ResponseEntity<String> pre = rest.getForEntity(url("/api/targets/it-stale"), String.class);
        assertThat(json.readTree(pre.getBody()).get("status").asText()).isEqualTo("FRESH");
        rest.postForEntity(url("/admin/simulation/it-stale"),
                new HttpEntity<>(Map.of("failing", true), new HttpHeaders()), String.class);
        Thread.sleep(400);
        ResponseEntity<String> r2 = rest.getForEntity(url("/api/targets/it-stale"), String.class);
        JsonNode b2 = json.readTree(r2.getBody());
        log.info("[it] 过期+上游失败: http={} status={} reason='{}'",
                r2.getStatusCode(), b2.get("status").asText(), b2.get("reason").asText());
        assertThat(r2.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(b2.get("status").asText()).isEqualTo("STALE_FALLBACK");

        // 3) 全新 key 直接故障 -> DEGRADED
        rest.postForEntity(url("/admin/simulation/it-degraded"),
                new HttpEntity<>(Map.of("failing", true), new HttpHeaders()), String.class);
        ResponseEntity<String> r3 = rest.getForEntity(url("/api/targets/it-degraded"), String.class);
        JsonNode b3 = json.readTree(r3.getBody());
        log.info("[it] 全新 key 故障: http={} status={} reason='{}'",
                r3.getStatusCode(), b3.get("status").asText(), b3.get("reason").asText());
        assertThat(b3.get("status").asText()).isEqualTo("DEGRADED");

        // 4) 连续失败打熔断器（failure-threshold=3），之后无缓存 key -> 503 CIRCUIT_OPEN
        String naked = "it-circuit";
        rest.postForEntity(url("/admin/simulation/" + naked),
                new HttpEntity<>(Map.of("failing", true), new HttpHeaders()), String.class);
        for (int i = 0; i < 4; i++) {
            rest.getForEntity(url("/api/targets/" + naked), String.class);
        }
        ResponseEntity<String> r4 = rest.getForEntity(url("/api/targets/" + naked), String.class);
        JsonNode b4 = json.readTree(r4.getBody());
        log.info("[it] 熔断打开: http={} body.status={} reason={}",
                r4.getStatusCode(), b4.get("status"), b4.get("reason").asText());
        assertThat(r4.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(b4.get("reason").asText()).isEqualTo("CIRCUIT_OPEN");

        // 5) 指标导出
        ResponseEntity<String> m = rest.getForEntity(url("/admin/metrics/export"), String.class);
        JsonNode mb = json.readTree(m.getBody());
        log.info("[it] 指标导出 totalRequests={}, statusCounts={}",
                mb.get("totalRequests").asLong(), mb.get("statusCounts").toString());
        assertThat(mb.get("totalRequests").asLong()).isGreaterThan(0L);
        assertThat(mb.get("rejectCounts").has("reason=CIRCUIT_OPEN")).isTrue();
        assertThat(mb.get("latency").get("p99Millis").asLong()).isGreaterThanOrEqualTo(0L);
    }

    @Test
    void hotBurstProduces429WithDeterministicReason() throws Exception {
        // per-key=3，制造慢上游：8 并发，期望部分 429 PER_KEY_OVERLOADED
        rest.postForEntity(url("/admin/simulation/it-hot"),
                new HttpEntity<>(Map.of("slow", true), new HttpHeaders()), String.class);

        int threads = 8;
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
        java.util.List<java.util.concurrent.Future<ResponseEntity<String>>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return rest.getForEntity(url("/api/targets/it-hot"), String.class);
            }));
        }
        start.countDown();
        int ok200 = 0, rejected429 = 0;
        for (var f : futures) {
            ResponseEntity<String> resp = f.get();
            if (resp.getStatusCode() == HttpStatus.OK) {
                ok200++;
            } else if (resp.getStatusCode() == HttpStatus.TOO_MANY_REQUESTS) {
                JsonNode body = json.readTree(resp.getBody());
                assertThat(body.get("reason").asText()).isEqualTo("PER_KEY_OVERLOADED");
                rejected429++;
            }
        }
        pool.shutdownNow();
        log.info("[it] 热点突发 8 并发 perKey=3: 200={}, 429 PER_KEY_OVERLOADED={}", ok200, rejected429);
        assertThat(ok200).isEqualTo(3);
        assertThat(rejected429).isEqualTo(5);
    }
}
