# Resilient Cache Gateway

在本地复现的"上游数据读取 + 缓存聚合"网关：用可配置延迟/故障的模拟上游代替真实外部依赖，
使高并发读取、数据过期与上游故障时的返回结果可预期且可解释。

## 架构

```
client → GatewayController → CacheGatewayService → SimulatedUpstream
                                    │
                                    ├─ TTL 缓存（ConcurrentHashMap）
                                    ├─ 单飞合并（inflight Future）
                                    ├─ 每 key 信号量（并发保护）
                                    └─ GatewayMetrics（计数 + 耗时分位）
```

## 缓存与刷新规则

- 每个 key 的缓存条目带 `fetchedAt` 时间戳，`gateway.cache-ttl-millis` 内为新鲜。
- 有效期内直接返回缓存，`origin=FRESH`，不回源。
- 过期后必须重新回源：同一 key 同一时刻只放行一个回源请求（单飞），
  其余并发请求合并等待并共享该结果；过期内容**绝不**作为 `FRESH` 返回。
- 回源成功后写入新缓存并返回 `FRESH`。

## 降级语义

每次返回的 `origin` 字段明确区分三种结果：

| origin | 含义 | 触发条件 |
|---|---|---|
| `FRESH` | 新鲜数据 | 缓存命中，或回源成功 |
| `STALE_FALLBACK` | 过期兜底 | 回源超时/失败，且存在已过期缓存，返回旧值并明确标记 |
| `DEGRADED` | 降级结果 | 回源超时/失败，且无任何缓存，`value=null` |

- 上游调用有超时（`gateway.upstream-timeout-millis`），超时后中断慢调用，避免请求堆积。
- 合并等待者共享赢家结果；赢家失败时按同样规则兜底，结果不静默丢失。
- 并发超限的请求被拒绝：REST 层返回 **429** `{"error":"OVERLOAD_REJECTED"}`，规则确定一致。

## 并发保护策略

- 每个 key 一个公平信号量，容量为 `gateway.max-concurrent-per-key`。
- 进入处理前 `tryAcquire`：拿不到许可立即拒绝（429），不排队、不阻塞，防止热点 key 拖垮服务。
- 回源在虚拟线程池执行，超时即 `cancel(true)` 中断。

## 可配置项（application.properties / 环境变量）

| 配置 | 默认 | 说明 |
|---|---|---|
| `gateway.cache-ttl-millis` | 2000 | 缓存有效期 |
| `gateway.upstream-timeout-millis` | 500 | 上游调用超时 |
| `gateway.max-concurrent-per-key` | 8 | 单 key 最大并发请求数，超出拒绝 |
| `gateway.simulated-latency-millis` | 50 | 模拟上游延迟 |
| `gateway.simulated-failure` | false | 模拟上游故障开关 |

## REST 接口

- `GET /api/data/{key}` — 读取数据，返回 `{key, value, origin, fetchedAt, elapsedMillis}`；过载返回 429。
- `GET /api/metrics` — 导出指标快照：请求总量、FRESH/STALE/DEGRADED/拒绝/合并/上游调用与失败计数、耗时 p50/p95/p99/max。
- `POST /api/upstream/config?latencyMs=&failure=` — 运行期调整模拟上游行为。

## 本地验证

```bash
# 运行测试（并发一致性、过期判定、超时降级、过载拒绝，日志含关键输入与判定依据）
mvn test

# 启动服务
mvn spring-boot:run

# 正常读取（第二次起命中缓存）
curl localhost:8080/api/data/demo

# 模拟慢上游（超过 500ms 超时 → 无缓存时 DEGRADED）
curl -X POST "localhost:8080/api/upstream/config?latencyMs=800"
curl localhost:8080/api/data/other-key

# 模拟上游故障（有 expired 缓存时 STALE_FALLBACK）
curl -X POST "localhost:8080/api/upstream/config?latencyMs=50&failure=true"

# 查看指标
curl localhost:8080/api/metrics

# 复现过载拒绝：调低上限后并发压测同一 key
# 设置 gateway.max-concurrent-per-key=2 后：
#   seq 20 | xargs -P20 -I{} curl -s -o /dev/null -w "%{http_code}\n" localhost:8080/api/data/hot
# 预期 2 个 200，其余 429
```

## 测试覆盖

- `CacheGatewayServiceTest`
  - 并发一致性：16 并发同 key，上游只回源 1 次，全部拿到一致的 `FRESH`。
  - 过期判定：过期后重新回源，旧值不作为 `FRESH` 返回。
  - 上游超时：无缓存 → `DEGRADED`；有过期缓存 → `STALE_FALLBACK`。
  - 上游故障：同上两种兜底路径。
  - 过载拒绝：上限 2、并发 10 → 恰好 2 成功、8 拒绝，无静默丢失。
- `GatewayControllerTest`：REST 读取、运行期调参触发降级、指标导出。
