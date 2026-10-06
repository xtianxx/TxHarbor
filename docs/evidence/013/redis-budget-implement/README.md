# 013 限流决策预算补充 — 实施批次验收证据（第三轮）

**批次**：`feat(013): 实施限流专用客户端+独立预算+TTL 兼容`（分支
`013-redis-latency-budget-design`，基线链 a59dcd1→db21da2→476e987→本批）。
设计与机制验证见 [../redis-latency-budget-design.md](../redis-latency-budget-design.md)
与 [../redis-budget-cte/](../redis-budget-cte/)。
第四轮（发布前审查 F1–F5 闭环：四形态整决策验收 + GateDelay 受控负例）
证据见 [../redis-budget-acceptance/](../redis-budget-acceptance/README.md)。

**环境**：Go 1.26.5；go-redis v9.22.0；容器 redis:8.2.10-alpine、
postgres:18.6-trixie（testcontainers）。所有命令在干净工作树上执行；
`TXHARBOR_FAULT_EVIDENCE_DIR` 指向本次运行的临时目录（无生产凭据、无敏感值）。

**实施内容（本轮）**：限流专用 Redis 客户端（`ContextTimeoutEnabled=true`、
`MaxRetries=-1`、显式 `PoolSize=16`；Dial/Read/Write=cfg.Redis.Timeout；
装配于 serve.go，健康/缓存共享客户端原样不动）；新键
`TXHARBOR_RATELIMIT_BUDGET`（缺省继承有效 `TXHARBOR_REDIS_TIMEOUT`；
显式值必须 ≤ 它）；桶 TTL 解耦为独立构造参数 `Config.BucketTTL`，装配处按
原算法 `2 × cfg.Redis.Timeout` 计算；错误四分类（父取消/父 deadline/
预算到期/传输故障——**错误身份优先，传输故障不查 ctx**，仅父取消豁免
`markUnavailable`/`setPaused`）；`.env.example` 013 键成组登记（全部注释化，
不作默认生效）。

**独立复核与修复**：本批经未参与修改者定向复核（5 项发现：.env.example
可选值默认生效、下游启动失败未关专用客户端、缺真实无重试取消验收、
GateDelay 定时窗口 pair 泄漏、摘要与归档数值不一致）——全部修复并重跑；
本目录为**修复后**运行的归档（`e2e_run.log`、`matrix_run1.log`、
`gate_delay_test.log` 为修复后权威日志）。

## 运行与判定（全部 PASS，-race 下无 DATA RACE）

| # | 命令 | 结果 | 日志 |
|---|---|---|---|
| 1 | `go test -race -tags integration_redis ./internal/ratelimit`（−v，127.7s） | 全 PASS；CTE 边界 9 记录（8 CTE-on 预算界 + 1 CTE-off 反向对照）、丢答 3 组、V-Ratelimit、A/B/C、RPC 预算全绿；`grep -c "DATA RACE"` = 0 | `integration_race_run.log` |
| 2 | `go test -race -tags e2e ./internal/app -run 'TestRatelimitAssembly\|TestRatelimitBudgetTTLDecoupling\|TestRatelimitFailureHTTPPolicy'` | 6/6 PASS（专用客户端选项、TTL 对照、丢答对照、池竞争、**取消身份**、真实 HTTP PD-1 策略回归） | `e2e_run.log` |
| 3 | `go test -race -tags e2e -run TestRatelimitBudgetMeasurementMatrix`（51.6s） | PASS；8 单元矩阵 | `matrix_run1.log` / `matrix_records.json` |
| 4 | `go test -race -tags integration_redis ./internal/testutil -run TestRedisGate` | 3/3 PASS（含 GateDelay 自测：延迟注入、客户端超时后 pair 回收、Hold 转换保持） | `gate_delay_test.log` |

辅助（同批工作树）：`go build ./...`、`go vet ./...`、`go vet -tags
integration_redis/e2e`、`gofmt -l` 全净；`make test`（全量单测）与
`go test -race ./internal/ratelimit ./internal/config ./internal/app` 全绿。

## 关键实测值（与归档 JSON 一致）

**1. 专用客户端（生产装配函数 `newLimiterRedisClient` 的 Options() 直读）**：
`ContextTimeoutEnabled=true`；`PoolSize=16`；Dial/Read/Write=`cfg.Redis.Timeout`；
`MaxRetries` 读回 0 = 构造值 -1 经库 `options.init()` 归一（0 语义=禁用；
未设会读回 3）——行为铁证见下条 1 次发送。

**2. 已执行丢答不重发（丢答夹具，预算 200ms、30ms 关连接）**：

| 装配 | sends | confirmed execs | 扣减估计 | 耗时 |
|---|---|---|---|---|
| 新（专用客户端 + MaxRetries=-1） | **1** | **1** | 0.991 | 36.1ms |
| 旧对照（共享式客户端 + 默认重试） | 3 | 3 | 2.993 | 201.3ms（失败反例保留；跨轮 3–4 次随退避时序波动） |

**3. TTL 解耦（PTTL 直读，桶键 `txharbor:rl:query`）**：

| Redis.Timeout | 预算 L | PTTL 实测 | 期望 2×Timeout |
|---|---|---|---|
| 1s | 未设→继承 1s | 1998ms | 2000ms |
| 1s | 200ms | 1999ms | 2000ms（≠2×L=400ms） |
| 3s | 未设→继承 3s | 5999ms | 6000ms |
| 3s | 200ms | 5999ms | 6000ms |

同时 `response_loss_records.json` 新增正向断言：settle 后
`tokens_settled == tokens_after`（旧归档中 `""`=400ms 过期的 TTL 耦合
观测已被本批修复，历史归档保持原样）。

**4. 并发池竞争（32 goroutines、PoolSize=16、GateHold、L=200ms，-race）**：
min/median/max = 201.3/203.2/205.2ms（全部 ≤ L+ε 容差 150ms）；
32/32 失败（预算截断）；返回后 `TotalConns=3`（≤ PoolSize 界内；个位数
归还残留，界断言为 ≤16 而非 =0）。

**5. 取消身份装配级验收（专用客户端；GateHold；60ms 处取消，L=200ms，-race）**：

| 场景 | 耗时 | Is(Canceled) | Is(Deadline) | 标记不可用 | observer 事件 | RPC send |
|---|---|---|---|---|---|---|
| 在途取消 | 200.5ms | **true** | false | **false** | 0 | ok |
| RPC 预算取用取消 | 200.8ms | true | false | false | 0 | ok（未 paused） |
| 对照：无取消超时 | 216.1ms | false | true | **true** | 1 | paused |

在途取消的终态错误是 socket `i/o timeout`（无重试路径不再经 Sleep 转换），
`context canceled` 身份来自 join——这正是四分类改造的目标场景；对照组
证明「仅父取消豁免」而非取消也被豁免。

**6. HTTP 误拒绝/耗时矩阵（N=32/单元，POST /withdrawals；拥塞=GateDelay
注入 400ms；-race）**：

| 单元 | 预算 | 场景 | 2xx | 503 | p50 | p95 | 拒绝键 0 行 |
|---|---|---|---|---|---|---|---|
| old_assembly（共享式客户端） | 1s | 正常 | 32 | 0 | 12.6ms | 14.0ms | — |
| old_assembly | 1s | 拥塞 400ms | 32 | 0 | 413.7ms | 415.4ms | — |
| dedicated_default_1s | 1s | 正常 | 32 | 0 | 12.3ms | 13.0ms | — |
| dedicated_default_1s | 1s | 拥塞 | 32 | 0 | 413.9ms | 415.6ms | — |
| candidate_200ms | 200ms | 正常 | 32 | 0 | 11.9ms | 13.0ms | — |
| candidate_200ms | 200ms | 拥塞 | 0 | **32（100%）** | 201.9ms | 202.5ms | **32/32** |
| candidate_300ms | 300ms | 正常 | 32 | 0 | 12.0ms | 12.8ms | — |
| candidate_300ms | 300ms | 拥塞 | 0 | **32（100%）** | 302.1ms | 302.7ms | **32/32** |

- 正常态误拒绝率（无故障）：0/32 全部 4 单元 = 0%。
- (200ms/300ms, 1s) 中间拥塞误拒绝率：400ms 注入下 100%（**这是 L 取值的
  核心代价；阈值待生产测量后批准，本证据不自行批准**）。
- 503 键 0 行 = 拒绝无新付款意图；2xx 键恰 1 行；503 体均含
  `temporarily_unavailable`。禁用重试的可用性取舍见设计文档与
  `specs/013-reliable-event-infrastructure/verification.md` §7。

## 边界与口径（如实声明）

1. `max_retries` 记录字段：-1=构造禁用（读回 0）；0=未设（库默认 3）。
2. GateDelay 逐 **backend→client 块**延迟；实测单块（congestion 单元
   `delayed_replies == eval_frames == 32`）；任一转发方向结束即回收 pair
   （复核 P2 修复：客户端超时不再滞留后端连接，gate 自测以
   `connected_clients` 回归基线证明）。
3. 矩阵 `old_assembly` 仅复刻旧客户端配置（无 CTE/默认重试），非旧二进制。
4. ε/容差（丢答/撤销 150ms、CTE 80ms）是测试容差，**不是生产 SLO**；
   200/300ms 是显式验证配置，不是新默认。
5. 扣减估计含 ≤ 重填量（rate=1/s 时每窗口 ≤1 token）；执行数一律来自
   INFO `cmdstat_eval` + 桶 token 直读，绝不从「无应答」推断。
6. A/B/C 延迟实验的 `samples.json/stats.json` 属其自身轮次证据
   （[../redis-latency-abc/](../redis-latency-abc/)），未纳入本目录。

## 复现入口

```sh
TXHARBOR_FAULT_EVIDENCE_DIR=$(mktemp -d) \
  go test -race -tags integration_redis -count=1 -timeout 20m ./internal/ratelimit
TXHARBOR_FAULT_EVIDENCE_DIR=$(mktemp -d) \
  go test -race -tags e2e -count=1 -timeout 20m ./internal/app \
  -run 'TestRatelimitAssembly|TestRatelimitBudgetTTLDecoupling|TestRatelimitFailureHTTPPolicy|TestRatelimitBudgetMeasurementMatrix'
TXHARBOR_FAULT_EVIDENCE_DIR=$(mktemp -d) \
  go test -race -tags integration_redis -count=1 ./internal/testutil -run TestRedisGate
```
