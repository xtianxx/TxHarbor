# 013 验证补充：较大积压排空验证（任务3）

- Feature: `013-reliable-event-infrastructure`；分支 `013-verify-supplement`（基线 `origin/main@d2bf559`）。
- 载体（本任务唯一新增代码）: `internal/events/backlog_drain_integration_test.go`
  （构建标签 `//go:build integration_backlog`；sha256 `4c542c19b12707fa47ab70ae3a82845f4e7da321234e5b828924f572e9dd3554`，1180 行）。
- 状态：负载/配置/口径已固定；**N=50 冒烟实测通过**；**N=5000 与 N=10000 已由 orchestrator 串行实测通过**
  （§5.2；2026-09-24；两轮各自 `--- PASS`，无断言跳过）。
- 原始日志（未入库）: `/tmp/opencode/013-supplement/`；本文件保留负载/配置/口径、证据索引与实测结果（§5）。
- 陈述纪律：投递为 **at-least-once**、处理为 **幂等**；PG 与 Kafka 无共享事务，本验证不声称跨系统
  「恰好一次」。所验证的「效果恰好一次」= 消费者 PG 效果侧计数（持久 inbox 吸收重投递），与项目
  既有证据边界一致（FR-13/FR-16/SC-04）。

## 1. 负载（固定、参数化、无随机）

| 项 | 值 |
|---|---|
| 事件类型 | `deposit.observation.status_changed`（`identity_kind=business_object`，无链身份） |
| 聚合 | `deposit_observation` / `t013-backlog-obs-%08d`：每事件唯一聚合，version 恒为 1 |
| payload | `from_state=pending`、`to_state=confirmed`、`reason=013 larger-backlog drain load` |
| 写入路径 | 真实 `events.Append`（身份派生/载荷规范化/版本派生/outbox 插入），**每 250 条一个事务批次**提交 |
| 数量 N | 默认 **5000**（5k–10k 验证区间下限）；`TXHARBOR_BACKLOG_DRAIN_N` 覆盖；区间外值可跑（冒烟）并在报告中标记 `in_verification_range=false`；`>50000` 拒绝 |
| 固定性 | 无随机种子/随机数：聚合 ID 与事件数均由序号和参数固定；两次冒烟 N=50 结果一致（同量级） |
| 防 OOM | 种子分批提交（250/事务）；发布器批量 250；消费者 poll 批量 500；监控仅存采样点（上限 4000，超出丢弃计数） |

## 2. 配置（测试有界值，非生产阈值）

| 组件 | 配置 |
|---|---|
| Publisher | `batch=250`、`lease_ttl=60s`、`backoff_base=1s`、`backoff_max=30s`、`jitter=0`（确定性） |
| KafkaSink | `delivery_timeout=10s`、`request_timeout=3s`、`max_inflight=5`、`max_buffered=2048` |
| Consumer | `gap_wait=1s`、`backoff_base=50ms`、`backoff_max=1s`、`retry_limit=5`、`chain_id=31337`（本负载无链字段）、`jitter=0` |
| KafkaConsumer | `poll_batch=500`、`commit_interval=200ms`、`group_prefix=txharbor` |
| Topic | `txharbor.events.v1`、6 分区（显式创建，未依赖 auto-create） |
| 容器镜像 | Kafka `confluentinc/confluent-local:7.9.10`；PostgreSQL `postgres:18.6-trixie`（testcontainers v0.44.0） |
| PG 连接上限 | pgxpool `max_conns=8`、`min_conns=1`（并发发布器 ×1 + 消费者 ×1 + 监控/断言） |
| 墙钟上限 | 测试 ctx 30m；发布窗口 12m；消费窗口（发布排空后）15m；均仅用于把卡死变为显式失败 |

## 3. 测量口径（报告 JSON 字段即口径）

报告形态：一行稳定前缀 JSON `TXHARBOR-BACKLOG-DRAIN {...}`（日志内），
`TXHARBOR_BACKLOG_REPORT=<path>` 时同内容写入该路径（原始证据不入库）。

| 字段 | 口径 |
|---|---|
| `seed_seconds` / `seed_events_per_second` | 种子开始 → 最后一批提交；吞吐 = N / 种子秒数 |
| `publish_seconds` / `publish_events_per_second` | t0（首个 `PublishOnce` 周期开始）→ DB `pending=0`；吞吐 = N / 秒数 |
| `total_drain_seconds` / `end_to_end_events_per_second` | t0 → 消费者 `applied=N`（效果事务已提交）；端到端吞吐 = N / 秒数 |
| `consumer_catchup_tail_seconds` | `total_drain_seconds − publish_seconds`（发布排空后的消费追赶尾巴） |
| `initial_oldest_age_seconds` | t0 时 `pending` 行 `now() − min(created_at)`（积压最老等待起点） |
| `oldest_wait_seconds` | 全部已发布行的 `max(published_at − created_at)`（单事件从创建到 broker ack 的最老等待，实测） |
| `max_observed_oldest_age_seconds` | 排空期间真实 `Publisher.RefreshGauges`（`CapacitySQL`）观测到的最老 age 最大值；仅观测、绝不门禁 |
| `publish_cycles/claimed/acked/released/blocked` | 真实 `PublishOnce` 逐周期累计；每周期断言 `Claimed ≤ batch` 且 `Acked+Released+Blocked == Claimed` |
| `publish_failures_by_class` | 真实 `PublishObserver.ObserveOutboxPublishFailure` 按 `transient/permanent/contract` 计数 |
| `consumer_applied/retries/quarantines` | 真实 `ConsumerObserver` 计数（quarantine 含失败类） |
| `consumer_effect_calls` | 效果函数被调用次数（含被回滚事务内的调用，仅记录；权威效果看行数） |
| 内存 | Go runtime：每次快照前 `runtime.GC()` 后读 `HeapAlloc/HeapSys/Sys/TotalAlloc/NumGC`；快照点 = 种子前 / 种子后 / 排空后 |
| PG 资源 | `pg_database_size(current_database())`、`pg_total_relation_size('outbox_events')`（表+索引），种子前 / 排空后 |
| 连接池 | `pgxpool.Stat()`：`max/total/idle/acquire/empty_acquire/canceled_acquire`，排空后 |
| broker 侧 | topic 各分区 end offset 合计（broker 已接受记录数；≥N，重复允许） |
| 曲线 | 监控每 200ms 采样 `pending/published/applied/ledger/progress_sum`；`pending_curve` 最多保留 200 点，丢弃数入 `curve_samples_dropped` |

**内存口径限制（显式）**：仅测量测试进程（含 pgx/Kafka 客户端，为上界）+ PG 服务端 relation/database 尺寸；
未测容器级 RSS（未接入 docker stats），未测多次重复的方差（每 N 建议至少单次，如需方差由 orchestrator 复跑）。

## 4. 断言（失败即非零退出；不延长超时、不跳过断言）

1. 种子后：`outbox 行数=N`、`pending=N`、`published=blocked=0`、不同聚合数=N、无 `Append` no-op。
2. 发布排空：窗口内 `pending=0`；每周期有界与账目守恒；结束后 `published=N`、`blocked=0`、
   `published_at IS NULL = 0`、行数仍为 N（不删行）、`acked=N`（0 丢失）；非 `transient` 发布失败类出现即失败。
3. 消费追赶：发布排空后窗口内 `applied=N`；`consumer_quarantines` 非空即失败。
4. 效果恰好一次边界：效果表 `count=N`、`count(DISTINCT event_id)=N`、每事件最大行数=1（表无唯一约束，
   重复效果会以第二行暴露）；`consumer_inbox=N`；`consumer_quarantine=0`。
5. 进度：`consumer_versions` 行数=N、`sum(max_version)=N`、`max(max_version)=1`；
   `consumer_progress` 分区数 1..6、`sum(next_offset) ∈ [N, broker 记录数]`；监控采样
   `pending` 单调不增，`published/applied/ledger/progress` 单调不减。
6. broker 侧：end offset 合计 ≥ N（0 丢失；重复允许且不破坏效果计数）。

## 5. 实测结果

### 5.1 冒烟 N=50（区间外冒烟值；2026-09-24T16:36Z；go1.26.5；HEAD d2bf559；最终代码）

原始证据：`/tmp/opencode/013-supplement/smoke_n50_rerun.log`、`smoke_n50_rerun.json`
（首次同负载冒烟 `smoke_n50.log` 亦通过，断言集为最终版前一修订）。

| 指标 | 实测 |
|---|---|
| 种子 | 0.067s（751 events/s）；行数=50、pending=50、唯一聚合=50 |
| 发布排空 | 0.356s（140 events/s）；cycles=1，claimed/acked/released/blocked = 50/50/0/0 |
| 端到端排空 | 0.658s（76 events/s）；消费追赶尾巴 0.302s |
| 最老等待 | 起点 0.074s；`published_at−created_at` 最大 0.429s；观测 gauge 最大 0.282s |
| 错误计数 | 发布失败 `{}`、released=0、consumer retries=0、quarantines=[] |
| 效果/进度 | 效果 50 行/50 事件/每事件最大 1；inbox=50；quarantine=0；versions=50（sum=50,max=1）；progress 6 分区 sum=50=broker 记录 50；采样曲线 [50,0,0]，单调断言通过 |
| 内存（强制 GC 后） | HeapAlloc `22,511,752 → 1,545,176 → 1,653,200` B（种子前/后/排空后）；种子后→排空后漂移 **+108 KB**；TotalAlloc `45.0M → 47.3M → 49.6M` B |
| PG 资源 | database `11,032,255 → 11,269,823` B；outbox relation `73,728 → 212,992` B（种子前→排空后） |
| 连接池 | max=8、total=2、idle=2、acquire=101、empty_acquire=1、canceled=0 |
| 容器启动+清理 | 测试总 wall 21.3s（含 PG+Kafka 容器启动/停止；排空本身 <1s） |

说明：`before_seed` 快照仍含容器启动期存活对象，不与 `after_seed` 直接比较为增长；泄漏/增长口径看
`after_seed → after_drain` 漂移与 TotalAlloc 增量（本次分别为 +108 KB / +2.3 MB）。

### 5.2 N=5000 / N=10000（orchestrator 串行实测；2026-09-24）

两轮均**通过**（各自 `--- PASS`，无断言跳过；`in_verification_range=true`）。执行即按下列命令（单机；两轮串行，未与另两路并发；`-timeout 40m` 未触发）：

```sh
mkdir -p /tmp/opencode/013-supplement
cd /home/dream/product_env/TxHarbor
export PATH=/usr/local/go/bin:$PATH

# N=5000（默认值；区间下限）
TXHARBOR_BACKLOG_DRAIN_N=5000 \
TXHARBOR_BACKLOG_REPORT=/tmp/opencode/013-supplement/backlog_n5000.json \
  go test -tags integration_backlog -count=1 -timeout 40m \
  -run 'TestBacklogDrain$' -v ./internal/events/ \
  2>&1 | tee /tmp/opencode/013-supplement/backlog_n5000.log

# N=10000（区间上限）
TXHARBOR_BACKLOG_DRAIN_N=10000 \
TXHARBOR_BACKLOG_REPORT=/tmp/opencode/013-supplement/backlog_n10000.json \
  go test -tags integration_backlog -count=1 -timeout 40m \
  -run 'TestBacklogDrain$' -v ./internal/events/ \
  2>&1 | tee /tmp/opencode/013-supplement/backlog_n10000.log
```

结果：两行 `TXHARBOR-BACKLOG-DRAIN` JSON 的实测值回填如下（原始件见 §7；未跑/失败项一律不预填，本次无失败项）：

| 指标 | N=5000 | N=10000 |
|---|---|---|
| 状态 | **通过**（`TestBacklogDrain` wall 46.95s） | **通过**（`TestBacklogDrain` wall 78.72s） |
| 起始时刻（UTC）/ 环境 | 2026-09-24T16:59:17Z；go1.26.5；GOMAXPROCS=16；host_cpus=16 | 2026-09-24T17:00:23Z；同左 |
| seed_seconds / 吞吐 | 7.56s / 662 events/s | 19.30s / 518 events/s |
| publish_seconds / 吞吐 | 0.86s / 5,796 events/s | 1.59s / 6,288 events/s |
| total_drain_seconds / 端到端吞吐 | 19.62s / 255 events/s | 39.60s / 253 events/s |
| consumer_catchup_tail_seconds | 18.75s | 38.01s |
| initial_oldest_age / oldest_wait / gauge 最大 | 7.57s / 7.88s / 2.11s | 18.87s / 19.19s / 6.24s |
| publish cycles / claimed / acked / released / blocked | 20 / 5000 / 5000 / 0 / 0 | 40 / 10000 / 10000 / 0 / 0 |
| errors（failures/released/blocked/retries/quarantines） | `{}` / 0 / 0 / 0 / `[]` | `{}` / 0 / 0 / 0 / `[]` |
| 效果/进度最终态 | 效果行 5000/5000 事件、每事件最大 1；inbox=5000；quarantine=0；versions=5000（sum=5000，max=1）；progress 6 分区 sum=5000；broker_records=5000；outbox published=5000、attempts_sum=5000、published_at_null=0 | 同口径全部 10000：效果行 10000/10000、每事件最大 1；inbox=10000；quarantine=0；versions=10000（sum=10000，max=1）；progress 6 分区 sum=10000；broker_records=10000；outbox published=10000、attempts_sum=10000、published_at_null=0 |
| 内存（强制 GC 后 HeapAlloc 种子前→种子后→排空后） | 18,316,336 → 1,538,824 → 1,858,840 B（种子后→排空后 **+320,016 B ≈ +313 KB**）；TotalAlloc 38.87M → 259.95M → 425.41M | 18,327,424 → 1,553,848 → 1,910,480 B（**+356,632 B ≈ +348 KB**）；TotalAlloc 43.50M → 485.66M → 813.82M |
| PG 资源（种子前→排空后） | database 11,032,255 → 22,976,191 B（≈+11.9 MB）；outbox relation 73,728 → 8,101,888 B（≈+8.0 MB） | database 11,032,255 → 34,914,831 B（≈+23.9 MB）；outbox relation 73,728 → 16,121,856 B（≈+16.0 MB） |
| 连接池（排空后） | max=8、total=3、idle=3、acquire=5458、empty_acquire=2、canceled=0 | max=8、total=3、idle=3、acquire=10876、empty_acquire=2、canceled=0 |
| 采样/曲线 | monitor_samples=98、observability_ticks=4、curve 丢弃=0；pending_curve `[5000,4500,2750,750,0,…]` 单调不增 | monitor_samples=197、observability_ticks=7、curve 丢弃=0；pending_curve `[10000,9500,8750,7250,6000,4250,2000,0,…]` 单调不增 |

实测观察（只陈述本轮数值，不设阈值、不构成容量结论）:

- 两个 N 的端到端吞吐几乎相同（255 vs 253 events/s），差异集中在 consumer 追赶尾巴（18.75s vs 38.01s）：
  ≈ 每 500 条 poll 批次 ~1.9s（10 批 vs 20 批），与固定配置 `poll_batch=500`、`gap_wait=1s` 一致；发布侧吞吐同量级（5,796→6,288 events/s）。
- 每轮断言全通过：每周期 `Claimed ≤ batch`、账目守恒；0 released/blocked/retries/quarantines；效果恰好一次边界（每事件效果行数=1、无重复 event_id、无 open quarantine）；broker records=N（0 丢失）。
- `consumer_effect_calls` = applied（5000/10000），无回滚重调。
- 种子吞吐随 N 下降（662→518 events/s），与固定种子批次（250 行/事务）及库表规模增长同现；单次测量，不作方差/容量结论。

## 6. 运行方式与 CI 纪律

- **构建标签 `integration_backlog` 是专用补充验证标签**：不被任何 Makefile target（`make test-*`）
  引用，也不在任何 `.github/workflows` 中触发 → 不进入普通 PR 必需 CI；`make lint` 的
  `go vet` 不覆盖该标签，运行前先做静态检查：
  `gofmt -l internal/events/ && go vet -tags integration_backlog ./internal/events/`。
- 无 Docker 的秒级冒烟（参数边界，纯 Go）：
  `go test -tags integration_backlog -count=1 -run TestBacklogDrainLoadProfile -v ./internal/events/`。
- 独立与可清理：每次运行自建独立 PG+Kafka 容器（testcontainers v0.44.0 + ryuk 会话回收）；
  `t.Cleanup` 在失败路径同样终止容器；表/消费者名 `t013-*`/`txharbor.backlog-drain.v1` 不与其它层共享。
- 不触碰：`specs/013-*/tasks.md`（T034 归任务1）、`.github/workflows`、`Makefile`、既有断言与业务语义。

## 7. 证据索引（原始件不入库）

| 证据 | 路径 | 状态 |
|---|---|---|
| 参数边界冒烟（无 Docker） | 命令见 §6；通过 | 通过（0.02s） |
| N=50 冒烟（首次） | `/tmp/opencode/013-supplement/smoke_n50.log` | 通过 |
| N=50 冒烟（最终代码） | `/tmp/opencode/013-supplement/smoke_n50_rerun.log`、`smoke_n50_rerun.json` | 通过 |
| N=5000 实测 | `/tmp/opencode/013-supplement/backlog_n5000.{log,json}` | **通过**（2026-09-24；wall 46.95s） |
| N=10000 实测 | `/tmp/opencode/013-supplement/backlog_n10000.{log,json}` | **通过**（2026-09-24；wall 78.72s） |

## 8. 待测 / 限制（不得读作已达标）

- N=5000 / N=10000 实测 **已完成**（§5.2，两轮均通过；2026-09-24 由 orchestrator 串行执行）。
- 未测：容器级 RSS；多轮重复的方差；故障并发场景（积压期间再故障）——本验证只覆盖「稳定环境下较大积压
  的恢复排空」，故障态由既有 T022/T023/T073/T074 层覆盖。
- 所有数值仅为本次单主机/容器化实测；**不构成生产容量、限流、告警或 lag 阈值**（阈值校准另行裁决/测量）。
