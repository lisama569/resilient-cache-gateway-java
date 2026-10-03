# Resilient Cache Gateway

在本地复现的"上游数据读取 + 缓存聚合"网关：用可配置延迟/故障的模拟上游代替真实外部依赖，
使高并发读取、数据过期与上游故障时的返回结果可预期且可解释。

## 架构

```
client → GatewayController → CacheGatewayService → SimulatedUpstream
                                    │
                                    ├─ TTL 缓存（ConcurrentHashMap）
                                    ├─ 宽限窗口（stale-while-revalidate，后台异步刷新）
                                    ├─ 单飞合并（inflight Future）
                                    ├─ 每 key 信号量（单 key 并发保护）
                                    ├─ GlobalAdmissionGuard（全局有界排队）
                                    ├─ CircuitBreaker（熔断/试探状态机）
                                    └─ GatewayMetrics（计数 + 耗时分位 + 状态导出）
```

## 读取路径总览

一次 `GET /api/data/{key}` 按以下顺序决策：

1. 缓存新鲜（`age < ttl`）→ 直接返回 `FRESH`，不经过任何排队/熔断。
2. 已过期但在宽限窗口内（`ttl <= age < ttl + staleGrace`）且后台更新连续失败未达上限
   → 返回 `STALE_REFRESHING`（旧值），并确保该 key 只有一次后台更新在进行。
3. 其余情况（未命中 / 窗口用完 / 后台更新失败超限）→ 单飞合并后的同步回源：
   先过全局有界排队守卫，再过熔断器，最后带超时访问上游；
   失败时按现有规则兜底（`STALE_FALLBACK` / `DEGRADED`）。

## 返回分类（origin）

| origin | 含义 | 触发条件 |
|---|---|---|
| `FRESH` | 新鲜数据 | 缓存命中，或回源/后台更新成功后的读取 |
| `STALE_REFRESHING` | 后台更新中的旧结果 | 过期但在宽限窗口内，返回上一份结果并触发一次后台更新 |
| `STALE_FALLBACK` | 过期兜底 | 同步回源超时/失败/被熔断，且存在已过期缓存 |
| `DEGRADED` | 降级结果 | 同步回源超时/失败/被熔断，且无任何缓存，`value=null` |

`STALE_REFRESHING` 与 `STALE_FALLBACK` 都返回旧值，但前者表示"后台正在更新、
窗口仍有效"，后者表示"本次回源失败后的兜底"，调用方可明确区分。

## 宽限窗口（stale-while-revalidate）

- `gateway.stale-grace-millis`：过期后的宽限时间。窗口内请求立即拿到旧值
  （`STALE_REFRESHING`），同一 key 同一时刻只触发**一次**后台更新；
  更新进行中再来的请求继续拿旧值，不重复触发。
- 后台更新成功 → 写入新缓存，后续读取立即看到 `FRESH` 新值，该 key 的失败计数清零。
- 后台更新失败 → 该 key 连续失败计数 +1；达到 `gateway.stale-max-refresh-failures`
  或窗口时间用完（`age >= ttl + grace`，边界含等于）后，不再返回 `STALE_REFRESHING`，
  落回同步路径与现有兜底分类。
- 后台更新同样受熔断器与全局守卫约束：熔断保护期内或全局队列拒绝时跳过本次更新
  （不计入失败次数）。
- 设为 `0` 即关闭该能力，行为与上一轮完全一致（过期即同步回源）。

## 全局容量保护与有界排队

- 只作用于**真正要访问上游**的请求：缓存命中（`FRESH`）、宽限窗口命中
  （`STALE_REFRESHING`）、合并等待者都不经过全局守卫，不会被误伤。
- `gateway.global-max-concurrent`：同时压向上游的最大并发数。
- 许可占满时，请求进入有界队列等待，最长 `gateway.global-queue-timeout-millis`：
  - 等到许可 → 正常处理；
  - 等待超时 → 拒绝，REST 返回 **429** `{"error":"QUEUE_TIMEOUT"}`；
  - 等待中的请求数达到 `gateway.global-queue-capacity` → 立即拒绝，
    REST 返回 **429** `{"error":"QUEUE_FULL"}`。
- 两种拒绝是不同异常类型、不同 error code、不同指标计数
  （`queueTimeoutRejected` / `queueFullRejected`），调用方可明确区分
  "排队没等到"与"总量已超过上限"。排队有硬上限，调用方不会无限等待。

## 熔断保护

- 上游连续失败/超时达到 `gateway.circuit-failure-threshold` → 进入 OPEN 保护期
  （`gateway.circuit-open-millis`），期间请求直接走兜底，不再访问上游
  （指标 `circuitRejected` 计数）。
- 保护期结束 → HALF_OPEN 试探：同一时刻最多放行 `gateway.circuit-probe-count`
  个试探请求，并发下名额严格受限，不会放一大批试探出去；名额外的请求仍走兜底，
  不会被丢弃。
- 试探全部成功 → CLOSED 恢复正常；任一试探失败 → 立即重新 OPEN。
- 当前状态（`CLOSED` / `OPEN` / `HALF_OPEN`）通过 `GET /api/metrics` 的
  `circuitState` 字段导出。

## 单 key 并发保护（上一轮既有行为，未变）

- 每个 key 一个公平信号量，容量 `gateway.max-concurrent-per-key`，
  拿不到许可立即拒绝：REST 返回 **429** `{"error":"OVERLOAD_REJECTED"}`。

## 可配置项（application.properties / 环境变量）

| 配置 | 默认 | 说明 |
|---|---|---|
| `gateway.cache-ttl-millis` | 2000 | 缓存有效期 |
| `gateway.upstream-timeout-millis` | 500 | 上游调用超时 |
| `gateway.max-concurrent-per-key` | 8 | 单 key 最大并发请求数，超出拒绝 |
| `gateway.stale-grace-millis` | 3000 | 过期后的宽限窗口（0=关闭，退回旧行为） |
| `gateway.stale-max-refresh-failures` | 3 | 宽限窗口内后台更新最大连续失败次数 |
| `gateway.global-max-concurrent` | 32 | 全局同时压向上游的最大并发数 |
| `gateway.global-queue-capacity` | 64 | 全局排队容量，超出立即拒绝（QUEUE_FULL） |
| `gateway.global-queue-timeout-millis` | 200 | 全局排队最长等待时间（QUEUE_TIMEOUT） |
| `gateway.circuit-failure-threshold` | 5 | 熔断阈值（连续失败次数） |
| `gateway.circuit-open-millis` | 5000 | 熔断保护期 |
| `gateway.circuit-probe-count` | 2 | 试探期同时放行的试探请求数 |
| `gateway.simulated-latency-millis` | 50 | 模拟上游延迟 |
| `gateway.simulated-failure` | false | 模拟上游故障开关 |

## REST 接口

- `GET /api/data/{key}` — 读取数据，返回 `{key, value, origin, fetchedAt, elapsedMillis}`；
  拒绝时返回 429，`error` 为 `OVERLOAD_REJECTED` / `QUEUE_TIMEOUT` / `QUEUE_FULL` 之一。
- `GET /api/metrics` — 指标快照：各 origin 计数、拒绝分类计数、后台更新计数、
  上游调用/失败计数、耗时 p50/p95/p99/max，以及 `circuitState`、
  `circuitConsecutiveFailures`、`globalQueueWaiters`、`globalAvailablePermits`。
- `POST /api/upstream/config?latencyMs=&failure=` — 运行期调整模拟上游行为。

## 与旧行为的兼容范围

- 接口路径、返回体结构、`FRESH` / `STALE_FALLBACK` / `DEGRADED` 三种分类语义不变。
- 单 key 信号量拒绝（429 `OVERLOAD_REJECTED`）、单飞合并、上游超时中断逻辑不变。
- 新增能力全部由配置控制：`stale-grace-millis=0` 时过期处理与上一轮完全一致；
  全局守卫与熔断器默认参数宽松，不改变低负载下的行为。
- 新增 `STALE_REFRESHING` 分类只在宽限窗口开启（默认开启）且命中窗口时出现。

## 本地验证

```bash
# 运行全部测试（日志含每个场景的配置与判定依据）
mvn test

# 启动服务
mvn spring-boot:run
```

### 正常读取与宽限窗口

```bash
# 正常读取（第二次起命中缓存 FRESH）
curl localhost:8080/api/data/demo

# 等缓存过了 2s TTL 但在 3s 宽限窗口内再读：
#   origin=STALE_REFRESHING，返回旧值，同时后台触发一次更新；
#   紧接着再读即可看到 FRESH 新值
sleep 2.5 && curl localhost:8080/api/data/demo && curl localhost:8080/api/data/demo
```

### 熔断：正常 → 保护 → 试探恢复

```bash
# 打开故障开关，连续请求触发熔断（默认阈值 5）
curl -X POST "localhost:8080/api/upstream/config?failure=true"
for i in $(seq 6); do curl localhost:8080/api/data/cb-demo; echo; done

# 查看状态：circuitState=OPEN，circuitRejected 增长，上游不再被调用
curl localhost:8080/api/metrics

# 关闭故障，等保护期（默认 5s）过后再读：试探成功 → FRESH，circuitState=CLOSED
curl -X POST "localhost:8080/api/upstream/config?failure=false"
sleep 5.5 && curl localhost:8080/api/data/cb-demo
```

### 全局排队：等待超时与总量拒绝

```bash
# 用宽松队列启动便于观察：
#   gateway.global-max-concurrent=2 gateway.global-queue-capacity=2 \
#   gateway.global-queue-timeout-millis=300 gateway.simulated-latency-millis=2000
# 然后对不同 key 并发打 5 个请求：
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w "%{http_code}\n" localhost:8080/api/data/bulk-$i &
done; wait
# 预期：2 个 200（占住许可）、2 个 429 QUEUE_TIMEOUT（排队 300ms 等不到）、
#       1 个 429 QUEUE_FULL（队列已满立即拒绝）
# 期间读取已缓存的 key 仍立即 200（缓存命中不受全局守卫影响）
```

### 指标与状态

```bash
curl localhost:8080/api/metrics
# 关注：staleRefreshing / backgroundRefreshes / backgroundRefreshFailures /
#       queueTimeoutRejected / queueFullRejected / circuitRejected /
#       circuitState / globalQueueWaiters / globalAvailablePermits
```

## 测试覆盖

- `CacheGatewayServiceTest`（上一轮回归）：并发一致性、过期后刷新、上游超时/故障兜底、
  单 key 过载拒绝（显式关闭宽限窗口以保持旧语义）。
- `StaleGraceWindowTest`：窗口内并发只触发一次后台更新、更新成功后新值立即可见、
  更新进行中不重复触发、窗口边界落回同步路径、后台更新连续失败超限后落回兜底。
- `GlobalAdmissionGuardTest`：排队超时（QUEUE_TIMEOUT）与总量超限（QUEUE_FULL）可区分、
  缓存命中不受全局守卫影响、合并请求不受全局守卫影响。
- `CircuitBreakerTest`：连续失败进入保护期、保护期内不访问上游、试探成功恢复、
  试探失败再保护、并发下试探名额受限、状态机单元迁移。
- `GatewayControllerTest`：REST 读取、运行期调参触发降级、指标与状态导出。
