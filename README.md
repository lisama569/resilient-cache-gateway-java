# Resilient Cache Gateway（弹性缓存网关）

在 Spring Boot 骨架之上实现的一套**可本地复现**的上游数据读取与缓存聚合能力。
用一个**延迟/故障完全可配置的模拟上游**代替真实外部依赖，使高并发读取、数据过期、
上游故障与过载场景下的返回结果**可预期、可区分、可解释**。

---

## 1. 能力总览

| 能力 | 实现方式 | 可观察结果 |
|---|---|---|
| 带有效期的结果缓存 | 内存缓存 + `ttlMillis`，整体替换式写入 | 期内返回 `FRESH`，不触发上游 |
| 并发请求合并（single-flight） | 每目标一个合并组 + `CompletableFuture` 共享 | 同目标在途并发只产生 **1 次**上游调用 |
| 过期刷新 | 过期后必须重新加载；成功才覆盖缓存 | 过期内容**绝不会**被当作 `FRESH` 返回 |
| 上游超时 | `Future.get(timeout)` 硬超时 + `cancel(true)` | 判定 `UPSTREAM_TIMEOUT`，与业务失败区分 |
| 部分失败降级 | 过期兜底窗口 + 默认降级值 | `STALE_FALLBACK` / `DEGRADED` 明确区分 |
| 熔断 | 每目标状态机 CLOSED/OPEN/HALF_OPEN | 打开期快速失败 `CIRCUIT_OPEN`，不堆积 |
| 热点并发保护 | 每目标舱壁 + 全局信号量 + 排队超时 | `PER_KEY_OVERLOADED` / `GLOBAL_OVERLOADED` / `QUEUE_TIMEOUT` |
| 可观测性 | 进程内计数 + 毫秒桶耗时直方图 | `/admin/metrics/export` 导出 p50/p90/p99 |

---

## 2. 缓存与刷新规则

- **新鲜判定**：`age = now - entry.createdAt`。
  - `age < ttlMillis`：命中，直接返回 `status=FRESH`，**不访问上游**。
  - `ttlMillis <= age < ttlMillis + staleTtlMillis`：已过期但在**兜底窗口**内；
    若上游可用则同步刷新并返回新的 `FRESH`；上游不可用时可作为过期兜底。
  - `age >= ttlMillis + staleTtlMillis`：连兜底资格也没有，过期条目不再对外服务。
- **刷新方式**：过期即同步重新加载（通过 single-flight 合并同一时刻的并发刷新），
  成功后用网关时钟在“写入瞬间”为条目打时间戳，**旧内容在任何路径上都不会被标记为 FRESH**。
- 单飞 leader 在真正调用上游前会做**二次缓存检查**：排队期间若已被上一轮刷新，直接复用新值。
- 管理面可主动驱逐：`DELETE /admin/cache/{target}`、`DELETE /admin/cache`。

---

## 3. 返回状态与降级语义

每次成功 HTTP 响应体都包含可机读的判定字段：

```json
{
  "target": "foo",
  "status": "STALE_FALLBACK",
  "value": "...",
  "reason": "served stale while upstream unavailable: UPSTREAM_TIMEOUT: upstream timeout after 100ms ...",
  "upstreamAt": 1790136573253,
  "elapsedNanos": 123456
}
```

| status / HTTP | 含义 | 触发条件 |
|---|---|---|
| `FRESH` (200) | 新鲜数据 | TTL 内缓存命中，或本次上游读取成功 |
| `STALE_FALLBACK` (200) | 过期兜底 | 上游失败/超时/熔断，但存在 staleTtl 窗口内的旧值；`value` 为旧值，`upstreamAt` 为旧数据时间，`reason` 注明真实原因 |
| `DEGRADED` (200) | 降级结果 | 确实尝试了上游但失败/超时，且**完全没有**任何缓存；`value=null` |
| `REJECTED` (**429**) | 保护性拒绝 | `PER_KEY_OVERLOADED`、`GLOBAL_OVERLOADED`、`QUEUE_TIMEOUT` |
| `REJECTED` (**503**) | 保护性拒绝 | `CIRCUIT_OPEN`、`UPSTREAM_TIMEOUT`/`UPSTREAM_FAILURE` 且无兜底（熔断为未尝试即快速失败，故与 DEGRADED 区分） |

拒绝响应体：`{"target": "...", "status": "REJECTED", "reason": "...", "message": "..."}`。

**不会发生的情况**：把过期内容当新鲜数据返回；上游持续异常时请求无限堆积；失败被静默吞掉
（任何分支都有明确的 `status`/`reason`，超时的模拟调用还会被 `cancel(true)` 中断释放线程）。

### 熔断器（每目标独立）

`CLOSED`：连续失败达到 `failure-threshold` → `OPEN`；
`OPEN`：所有请求快速失败，持续 `open-state-millis` → `HALF_OPEN`；
`HALF_OPEN`：只放行 `half-open-permits` 个试探，成功 → `CLOSED`，任一失败 → 重新 `OPEN`。
熔断期间有旧值可用时仍返回 `STALE_FALLBACK`（reason 含 `CIRCUIT_OPEN`）。

---

## 4. 并发保护策略（确定性规则）

1. **请求合并（single-flight）**：同一目标同一时刻只有 1 个 leader 真正读取上游，
   其余 waiter 共享同一个 `CompletableFuture`，得到完全一致的结果。
2. **每热点舱壁**：参与者（含 leader）达到 `per-key-max-concurrent` 后，
   新请求**立即**以 `PER_KEY_OVERLOADED` 拒绝（429），规则对所有请求一致，不随机、不抖动。
3. **排队超时**：waiter 最多等待 `acquire-timeout-millis`（按真实墙钟计时），
   超时以 `QUEUE_TIMEOUT` 拒绝（429），保证不堆积；leader 结果不受超时 waiter 影响。
4. **全局上游舱壁**：真正发往上游的调用数受 `global-max-concurrent` 信号量约束，
   取不到许可返回 `GLOBAL_OVERLOADED`（不消耗熔断失败计数）。

---

## 5. 可配置项（`application.properties`，前缀 `gateway`）

| 配置键 | 默认值 | 说明 |
|---|---|---|
| `gateway.ttl-millis` | 5000 | 缓存新鲜有效期 |
| `gateway.stale-ttl-millis` | 10000 | 过期兜底窗口 |
| `gateway.upstream-timeout-millis` | 800 | 单次上游硬超时 |
| `gateway.per-key-max-concurrent` | 10 | 单热点最大参与并发数（含加载者） |
| `gateway.global-max-concurrent` | 200 | 全局上游并发上限 |
| `gateway.acquire-timeout-millis` | 100 | 合并排队最长等待时间 |
| `gateway.failure-threshold` | 5 | 熔断连续失败阈值 |
| `gateway.open-state-millis` | 2000 | 熔断打开持续时间 |
| `gateway.half-open-permits` | 1 | 半开试探名额 |
| `gateway.stale-on-failure-enabled` | true | 失败时是否允许过期兜底 |
| `gateway.upstream-pool-size` | 200 | 上游调用线程池大小 |
| `gateway.upstream.default-delay-millis` | 100 | 模拟上游默认延迟 |
| `gateway.upstream.slow-delay-millis` | 3000 | slow 标记目标的延迟（大于超时即可复现超时） |
| `gateway.upstream.delays.<key>` | - | 为单个目标指定延迟 |
| `gateway.upstream.failing-keys[n]` | - | 启动即故障的目标 |
| `gateway.upstream.slow-keys[n]` | - | 启动即慢调用的目标 |

---

## 6. HTTP 接口

### 数据读取
- `GET /api/targets/{target}` —— 读取聚合结果（语义见第 3 节）。

### 管理面（本地验证用）
- `GET  /admin/simulation` —— 查看模拟上游配置与每目标调用计数。
- `POST /admin/simulation/{target}` —— 调整目标行为，字段均可选：
  `{"delayMillis": 50}`、`{"failing": true}`、`{"slow": true}`
  （设置 `delayMillis` 会自动清除 `slow` 标记）。
- `POST /admin/simulation/reset` —— 重置模拟状态与调用计数。
- `GET  /admin/metrics/export` —— 导出指标快照。
- `POST /admin/metrics/reset` —— 指标清零。
- `GET  /admin/cache` / `DELETE /admin/cache/{target}` / `DELETE /admin/cache`。
- `GET  /admin/circuits` / `POST /admin/circuits/{target}/reset`。

### 指标内容
`totalRequests`、`upstreamCalls/Errors/Timeouts`、`requestsByTarget`、
`statusCounts`（含 `target=..|status=..` 维度）、`rejectCounts`（含
`reason=PER_KEY_OVERLOADED` 等维度）、全局 `latency` 与每目标 `latencyByTarget`
的 `p50/p90/p99/max` 及毫秒桶累计计数（桶上界 1/2/5/10/20/50/100/200/500/1000/2000/5000ms）。

---

## 7. 本地验证方法

### 启动
```bash
mvn spring-boot:run
```

### 场景 1：TTL 内缓存命中（上游只被调用一次）
```bash
curl -s localhost:8080/api/targets/foo | jq .status     # "FRESH"
curl -s localhost:8080/api/targets/foo | jq .status     # 仍是 "FRESH"
curl -s localhost:8080/admin/simulation | jq .callCounts.foo   # 1
```

### 场景 2：过期后故障 → 过期兜底可识别
```bash
curl -s localhost:8080/api/targets/bar | jq .           # FRESH 建缓存
curl -s -X POST localhost:8080/admin/simulation/bar -H 'Content-Type: application/json' \
  -d '{"failing":true}'
sleep 6   # 超过 ttl=5s，仍在 staleTtl=10s 窗口内
curl -s localhost:8080/api/targets/bar | jq '{status,reason,upstreamAt}'
# status="STALE_FALLBACK"，reason 中说明 upstream failure
```

### 场景 3：上游超时（与业务失败区分）
```bash
curl -s localhost:8080/api/targets/baz | jq .status
curl -s -X POST localhost:8080/admin/simulation/baz -H 'Content-Type: application/json' \
  -d '{"slow":true}'      # 延迟变为 slow-delay-millis=3000 > 超时 800ms
sleep 6
curl -s localhost:8080/api/targets/baz | jq '{status,reason}'
# status="STALE_FALLBACK"，reason 含 "UPSTREAM_TIMEOUT"
```

### 场景 4：热点过载 → 确定性 429
```bash
curl -s -X POST localhost:8080/admin/simulation/hot -H 'Content-Type: application/json' -d '{"slow":true}'
for i in $(seq 1 20); do
  curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/api/targets/hot &
done; wait
# 默认 per-key-max-concurrent=10：恰好 10 个 200，其余 429
```

### 场景 5：熔断打开 → 503（无缓存目标）
```bash
curl -s -X POST localhost:8080/admin/simulation/x -H 'Content-Type: application/json' -d '{"failing":true}'
for i in $(seq 1 8); do
  curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/api/targets/x
done   # 前 5 次 200 DEGRADED（达到阈值），随后 503
curl -s localhost:8080/admin/circuits | jq .            # x=OPEN
```

### 场景 6：导出指标
```bash
curl -s localhost:8080/admin/metrics/export | jq .
```

---

## 8. 测试

```bash
mvn test
```

共 25 个用例（关键输入与判定依据均通过 SLF4J 打印在测试日志中）：

- `CircuitBreakerTest`（4）：阈值、OPEN 拒绝、冷却半开、试探成功/失败。
- `SingleFlightTest`（3）：20 并发仅 1 次加载、舱壁满立即 `PER_KEY_OVERLOADED`、排队 `QUEUE_TIMEOUT`。
- `CacheGatewayFreshnessTest`（4）：首次 FRESH、TTL 内不调上游、过期必刷新、时间戳可区分。
- `CacheGatewayFailureTest`（5）：失败/超时各自的 `STALE_FALLBACK`、无缓存 `DEGRADED`、
  超窗口旧值不兜底、熔断 OPEN 期 stale 与 503 拒绝。
- `CacheGatewayOverloadTest`（4）：端到端单飞、每热点 429 计数精确、排队超时、全局舱壁。
- `MetricsRegistryTest`（2）：计数维度、p50/p90/p99、重置。
- `GatewayIntegrationTest`（2）：HTTP 端到端 FRESH/STALE/DEGRADED/503/429 区分与指标导出。

单元层使用 `MutableTimeSource` 精确控制 TTL 与熔断时钟；并发用例使用真实线程池 +
闭锁制造真实竞态；HTTP 用例使用 `test` profile（`src/test/resources/application-test.properties`）。

---

## 9. 代码结构

```
config/      GatewayProperties（全部配置）、GatewayConfiguration（装配）
core/        TimeSource（可注入时钟）
model/       ResultStatus、RejectReason、TargetResult、UpstreamResponse、CacheEntry
cache/       CacheEntryStore / InMemoryCacheEntryStore
upstream/    UpstreamClient、SimulatedUpstreamClient、SimulationState（可配置延迟/故障）
resilience/  SingleFlight（合并+每热点舱壁+排队超时）、Circuit(Breaker|State|BreakerRegistry)
metrics/     MetricsRegistry、LatencyHistogram（分位数）
service/     CacheGatewayService（核心编排：缓存→合并→熔断→舱壁→超时→兜底/降级）
web/         GatewayController、AdminController、ApiExceptionHandler
```
