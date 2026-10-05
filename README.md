# Resilient Cache Gateway

在本地复现的"上游数据读取 + 缓存聚合"网关：用可配置延迟/故障的模拟上游代替真实外部依赖，
使高并发读取、数据过期与上游故障时的返回结果可预期且可解释。

## 架构

```
client → GatewayController → CacheGatewayService → SimulatedUpstream
                                    │
                                    ├─ TTL 缓存（ConcurrentHashMap）
                                    ├─ 宽限窗口 SWR（旧结果 + 单次后台更新）
                                    ├─ 单飞合并（inflight Future）
                                    ├─ 每 key 信号量（并发保护）
                                    ├─ 全局准入（GlobalAdmissionLimiter：并发上限 + 有界排队）
                                    ├─ 熔断器（CircuitBreaker：CLOSED/OPEN/HALF_OPEN）
                                    └─ GatewayMetrics（计数 + 耗时分位 + 熔断/排队状态）
```

## 缓存与刷新规则

- 每个 key 的缓存条目带 `fetchedAt` 时间戳，`gateway.cache-ttl-millis` 内为新鲜。
- 有效期内直接返回缓存，`origin=FRESH`，不回源。
- 过期后必须重新回源：同一 key 同一时刻只放行一个回源请求（单飞），
  其余并发请求合并等待并共享该结果；过期内容**绝不**作为 `FRESH` 返回。
- 回源成功后写入新缓存并返回 `FRESH`。

## 降级语义

每次返回的 `origin` 字段明确区分四种结果：

| origin | 含义 | 触发条件 |
|---|---|---|
| `FRESH` | 新鲜数据 | 缓存命中，或回源成功（含后台更新成功后的读取） |
| `STALE_REFRESHING` | 宽限窗口内的旧结果 | 缓存刚过期但在宽限窗口内、后台更新失败次数未达上限；同时后台只触发一次更新 |
| `STALE_FALLBACK` | 过期兜底 | 回源超时/失败、熔断保护期，或容量占满（排队没等到/总量超限）但手里还有旧结果时，返回旧值并明确标记 |
| `DEGRADED` | 降级结果 | 回源超时/失败（或熔断保护期），且无任何缓存，`value=null` |

- 上游调用有超时（`gateway.upstream-timeout-millis`），超时后中断慢调用，避免请求堆积。
- 合并等待者共享赢家结果；赢家失败时按同样规则兜底，结果不静默丢失。
- 并发超限的请求被拒绝：REST 层返回 **429** `{"error":"OVERLOAD_REJECTED"}`，规则确定一致。

## 宽限窗口（Stale-While-Revalidate）

- 缓存过期后进入 `gateway.swr-grace-millis` 的宽限窗口（`0` 表示关闭，保持旧行为）。
- 窗口内：请求**立即**拿到上一份旧结果（`origin=STALE_REFRESHING`，与最新数据明确区分），
  同时同一 key 只触发**一次**后台更新；更新进行中再来的请求复用这次更新，不重复触发。
- 后台更新成功后写缓存，后续读取立刻看到新结果（`FRESH`）。
- 后台更新**确实发到上游**且失败/超时，才计入连续失败；达到 `gateway.swr-max-background-failures` 次，
  或宽限窗口用完：不再拿旧结果顶着，落回同步回源与现有兜底分类（`STALE_FALLBACK` / `DEGRADED`）。
- 后台更新**没真正发到上游**（被全局容量拦回、或熔断保护期被跳过）不算失败：
  旧结果保持有效，窗口内请求照常返回 `STALE_REFRESHING`，下一次请求会重试后台更新。
  这类跳过单独计入指标 `backgroundRefreshSkipped`，与 `backgroundRefreshFailures` 明确区分。
- 边界确定：`age < ttl` 为新鲜；`ttl <= age < ttl + grace` 为宽限窗口；`age >= ttl + grace` 落回同步路径。

## 全局容量保护与有界排队

- 真实回源前经过全局准入：同时访问上游的并发数不超过 `gateway.max-concurrent-upstream`。
- 容量满时不直接拒绝，而是进入长度上限为 `gateway.max-queue-size` 的等待队列，
  最多等 `gateway.queue-wait-timeout-millis`：
  - 等到许可 → 正常处理；
  - 排队超时 → **429** `{"error":"QUEUE_WAIT_TIMEOUT"}`；
  - 队列已满（总量超限）→ 立即 **429** `{"error":"CAPACITY_EXCEEDED"}`。
  两种拒绝语义明确区分，指标分别计入 `queueWaitTimeouts` / `capacityExceeded`。
- **不误伤**：缓存命中（`FRESH`/`STALE_REFRESHING`）与单飞合并的跟随者不占用全局名额，
  保护只作用在真实回源压力上。
- **旧结果优先于拒绝**：容量占满（排队没等到或总量超限）时，只要手里还有旧结果
  （含宽限窗口内与已过窗口的过期缓存），请求照旧拿到旧值（`STALE_REFRESHING`/`STALE_FALLBACK`），
  不会被变成 429；只有完全没有旧结果可用、确实需要新数据的请求，
  才按 `QUEUE_WAIT_TIMEOUT` / `CAPACITY_EXCEEDED` 两种语义拒绝。
- 排队有界且等待有超时，上游持续不恢复时调用方不会无限等待。

## 熔断器

- 上游连续失败/超时达到 `gateway.circuit-failure-threshold` 次 → 进入保护期（OPEN），
  持续 `gateway.circuit-open-millis`，期间请求直接走兜底（`STALE_FALLBACK`/`DEGRADED`），不再打上游。
- 保护期结束 → 试探期（HALF_OPEN）：只放行 `gateway.circuit-half-open-probes` 个试探请求，
  其余请求仍然兜底（不丢请求，也不放一大批试探）。
- 试探成功 → 恢复 CLOSED；试探失败 → 重新进入保护期。
- 排队超时/总量超限/熔断拦截不计入熔断失败次数（未真正打到上游）。
- 试探许可拿到后、尚未打到上游就被容量拦回（排队没等到/总量超限）时，试探名额立即归还：
  不算成功也不算失败，状态保持 HALF_OPEN，容量恢复后下一个试探照常放行，
  熔断器不会卡在试探期出不来。
- 当前状态与累计开路次数从指标导出：`GET /api/metrics` 的 `circuit.state`（CLOSED/OPEN/HALF_OPEN）、
  `circuit.openCount`、`circuit.halfOpenInFlight`（试探期在飞的试探请求数）。

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
| `gateway.swr-grace-millis` | 0 | 过期后的宽限窗口；0 表示关闭（保持旧行为） |
| `gateway.swr-max-background-failures` | 3 | 宽限窗口内后台更新连续失败上限（只计真正打到上游的失败），达到后落回兜底 |
| `gateway.max-concurrent-upstream` | 16 | 全局同时回源的最大并发数 |
| `gateway.queue-wait-timeout-millis` | 200 | 全局容量满时排队等待的最长时间 |
| `gateway.max-queue-size` | 64 | 全局等待队列长度上限，超出即总量超限拒绝 |
| `gateway.circuit-failure-threshold` | 5 | 熔断器进入保护期所需的连续失败次数 |
| `gateway.circuit-open-millis` | 5000 | 熔断保护期时长 |
| `gateway.circuit-half-open-probes` | 1 | 试探期允许同时放行的试探请求数 |

## REST 接口

- `GET /api/data/{key}` — 读取数据，返回 `{key, value, origin, fetchedAt, elapsedMillis}`；
  拒绝时返回 429，`error` 区分 `OVERLOAD_REJECTED`（单 key 超限）/
  `QUEUE_WAIT_TIMEOUT`（排队没等到）/`CAPACITY_EXCEEDED`（总量超限）。
  注意：后两种拒绝只发生在完全没有任何旧结果可用的请求上；手里还有旧结果的请求
  在容量占满时返回 200 + `STALE_REFRESHING`/`STALE_FALLBACK`。
- `GET /api/metrics` — 导出指标快照：请求总量、各 origin 计数、拒绝/合并/上游调用与失败计数、
  后台更新计数（`backgroundRefreshes`/`backgroundRefreshFailures`/`backgroundRefreshSkipped`，
  分别表示后台更新尝试、真正打到上游的失败、未真正发出的跳过）、
  排队超时与总量超限计数、`circuit`（`state`/`openCount`/`halfOpenInFlight`）、
  `admission`（当前回源并发与排队数）、耗时 p50/p95/p99/max。
- `POST /api/upstream/config?latencyMs=&failure=` — 运行期调整模拟上游行为。

## 兼容性

- 接口与返回模型不变，仅 `origin` 枚举新增 `STALE_REFRESHING`；
  旧的三类（`FRESH`/`STALE_FALLBACK`/`DEGRADED`）语义不变。
- `gateway.swr-grace-millis` 默认为 0：不配置时宽限窗口关闭，过期行为与上一轮完全一致。
- 全局准入与熔断器默认参数宽松（16 并发 / 5 次失败），不影响既有单测与本地演示路径。
- 本轮行为修正（均为收紧误伤、不改变正常路径）：
  - 宽限窗口内的后台更新被容量拦回/保护期跳过时，不再计入 `swr-max-background-failures`
    （此前会让窗口内的旧结果提前失效）；只有真正打到上游的失败/超时才计数。
  - 容量占满时，手里还有旧结果的请求返回 200 + 旧值（此前可能返回 429）；
    完全没有旧结果的请求拒绝语义不变，仍为 `QUEUE_WAIT_TIMEOUT` / `CAPACITY_EXCEEDED`。
  - 试探许可被容量拦回时归还试探名额（此前名额泄漏会让熔断器卡在 HALF_OPEN）。
- 指标快照新增 `backgroundRefreshSkipped` 与 `circuit.halfOpenInFlight` 两个字段，纯增量。

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

### 宽限窗口（SWR）复现

```bash
# 启动时配置：gateway.cache-ttl-millis=2000 gateway.swr-grace-millis=10000
curl localhost:8080/api/data/swr-demo          # FRESH
sleep 3                                        # 进入宽限窗口（2s < age < 12s）
curl localhost:8080/api/data/swr-demo          # STALE_REFRESHING（旧值，后台更新已触发）
sleep 1                                        # 等后台更新完成
curl localhost:8080/api/data/swr-demo          # FRESH（新值）
# 上游故障时窗口内仍返回 STALE_REFRESHING；连续失败达到上限后落回 STALE_FALLBACK
curl -X POST "localhost:8080/api/upstream/config?failure=true"

# 熔断进入保护期后（连续失败达到阈值），窗口内的旧结果同样照常返回：
# 后台更新被保护期跳过，计入 backgroundRefreshSkipped 而不是失败，
# 窗口内请求始终是 200 STALE_REFRESHING；保护期结束、上游恢复后后台更新成功 → FRESH
```

### 排队等待超时 / 总量超限复现

```bash
# 启动时配置：gateway.max-concurrent-upstream=1 gateway.max-queue-size=1
#             gateway.queue-wait-timeout-millis=200 gateway.max-concurrent-per-key=32
curl -X POST "localhost:8080/api/upstream/config?latencyMs=2000&failure=false"
curl localhost:8080/api/data/slow-a &          # 占住唯一全局许可
sleep 0.2
curl localhost:8080/api/data/slow-b &          # 进入队列，200ms 后 429 QUEUE_WAIT_TIMEOUT
sleep 0.1
curl localhost:8080/api/data/slow-c            # 队列已满，立即 429 CAPACITY_EXCEEDED
# 期间读取已缓存的 key 仍正常返回 200 FRESH（缓存命中不占全局名额）
```

### 容量占满时“还有旧结果”的请求复现

```bash
# 启动时配置：gateway.cache-ttl-millis=2000 gateway.swr-grace-millis=30000
#             gateway.max-concurrent-upstream=1 gateway.max-queue-size=0
#             gateway.max-concurrent-per-key=32
curl localhost:8080/api/data/stale-demo        # FRESH，建立缓存
sleep 3                                        # 进入宽限窗口
curl -X POST "localhost:8080/api/upstream/config?latencyMs=5000&failure=false"
curl localhost:8080/api/data/holder &          # 慢请求占住唯一全局许可（maxQueueSize=0）
sleep 0.5
# 容量已占满，但窗口内的旧结果照常返回，且不会被变成 429：
for i in 1 2 3 4 5; do curl localhost:8080/api/data/stale-demo; done
#   全部 200 STALE_REFRESHING（旧值）；指标里 backgroundRefreshSkipped 增长、
#   backgroundRefreshFailures 不增长（后台更新被容量拦回，没真正打到上游，不算失败）
curl localhost:8080/api/metrics
# 没有缓存的 key 才会被拒：
curl localhost:8080/api/data/no-cache          # 429 CAPACITY_EXCEEDED
# holder 结束后（latencyMs 调回 50）再读 stale-demo：后台更新成功 → 200 FRESH（新值）
curl -X POST "localhost:8080/api/upstream/config?latencyMs=50"
```

### 熔断器复现（正常 → 保护 → 试探 → 恢复）

```bash
# 启动时配置：gateway.circuit-failure-threshold=3 gateway.circuit-open-millis=5000
curl -X POST "localhost:8080/api/upstream/config?failure=true"
for i in 1 2 3; do curl localhost:8080/api/data/cb-$i; done   # 3 次 DEGRADED
curl localhost:8080/api/metrics                # circuit.state=OPEN
curl localhost:8080/api/data/cb-open           # 保护期内直接 DEGRADED，upstreamCalls 不再增长
sleep 6
curl localhost:8080/api/data/cb-probe          # 试探失败 → 仍 OPEN
curl -X POST "localhost:8080/api/upstream/config?failure=false"
sleep 6
curl localhost:8080/api/data/cb-recover        # 试探成功 → FRESH，circuit.state=CLOSED
```

## 测试覆盖

- `CacheGatewayServiceTest`（上一轮回归）
  - 并发一致性：16 并发同 key，上游只回源 1 次，全部拿到一致的 `FRESH`。
  - 过期判定：过期后重新回源，旧值不作为 `FRESH` 返回。
  - 上游超时：无缓存 → `DEGRADED`；有过期缓存 → `STALE_FALLBACK`。
  - 上游故障：同上两种兜底路径。
  - 过载拒绝：上限 2、并发 10 → 恰好 2 成功、8 拒绝，无静默丢失。
- `CacheGatewayNewFeaturesTest`（本轮新增，日志含配置与判定依据）
  - 宽限窗口边界：窗口内 `STALE_REFRESHING`，窗口外落回同步回源/兜底。
  - 并发只触发一次后台更新：8 并发窗口内全部 `STALE_REFRESHING`，上游仅 1 次后台回源。
  - 后台更新连续失败达到上限后落回 `STALE_FALLBACK`。
  - 排队超时（`QueueWaitTimeoutException`）与总量超限（`CapacityExceededException`）区分。
  - 缓存命中与单飞合并在全局容量占满时不被误伤。
  - 熔断完整过程：连续失败 → OPEN 不打上游 → 试探失败续保护 → 试探成功恢复。
  - 试探期并发限制：6 并发只放 1 个试探，其余兜底不丢请求。
- `CacheGatewayStaleProtectionTest`（本轮 bugfix 新增，日志含配置与判定依据）
  - 宽限窗口 × 容量占满：4 轮并发（拦回次数超过失败上限）窗口内请求始终 `STALE_REFRESHING`，
    后台更新记 `backgroundRefreshSkipped` 不计失败，容量释放后后台更新成功。
  - 宽限窗口 × 保护期：保护期内窗口内请求始终 `STALE_REFRESHING` 且不打上游，
    熔断恢复后后台更新成功。
  - 总量超限/排队超时 × 旧结果：有旧结果 → `STALE_FALLBACK`；无旧结果 → 两种拒绝可区分。
  - 试探名额不泄漏：试探被容量拦回后 `halfOpenInFlight` 归零、保持 HALF_OPEN，
    容量恢复后下一个试探放行并恢复 CLOSED，状态全程可从指标观察。
- `GatewayControllerTest`：REST 读取、运行期调参触发降级、指标导出。
