# 013 补强任务2：三轮依赖振荡验证（oscillation）

- Feature: `013-reliable-event-infrastructure`；任务: 013-supplement 任务2「三轮依赖振荡验证」。
- 分支/基线: `013-verify-supplement` @ `d2bf559`（origin/main）；写范围仅新增测试与本文档。
- 状态口径（三者分离，不得混同）:
  - **实现/静态检查（本分支）**：已完成——`gofmt` 干净、`go vet -tags fault ./internal/faultdrill/` 通过、`go test -tags fault -run '^$'` 编译通过。
  - **完整 Docker 实测**：**已由 orchestrator 顺序执行并通过（§6；2026-09-24；`84.67s`）**（三路代码齐后串行，未并发抢 Docker/PG/Kafka）；实测结果见 §6。
  - **生产就绪**：未宣称；T000-P 保持 OPEN。
- 层与门禁: `//go:build fault`（faultdrill 层既有约定，`make test-fault` / `fault-perf.yml`），**不进入普通 PR 必需 CI**；测试上下文超时 30m（对齐既有 drill 的 20–25m 预算并覆盖一次真实 broker 替换）。

## 1. 被测对象与口径

- 复用现有 faultdrill harness 原语：`StopKafka`/`StartKafka`、`SuspendKafka`/`ResumeKafka`、`SuspendDual`/`ResumeAll`（Redis 容器 Stop/Start），以及真实 `events.Publisher` + `events.KafkaConsumer` + 参考消费者运行时；不手调状态函数替代服务路径。
- 单节点容器的 Stop/Start 是**真实容器替换**，**不称为集群选举/主从切换验证**。
- harness 已声明：`Start` 会启动全新 broker（同固定地址），旧的 Kafka offset 坐标随日志替换而失效。因此容器替换轮（R1）被固定在**任何持久消费进度出现之前**执行；本演练**不宣称**跨 broker 替换的 offset 续传。EXPECTED：R2/R3 在 SIGSTOP 保留日志的原语上验证「持久进度续传」。

## 2. 固定时间线（代码内 `oscillationTimeline`，运行期不调整）

| 轮 | 故障（注入） | 恢复 | 真实容器 Stop/Start | 故障期新增行 | 身份对 | 预期累计 published |
|---|---|---|---|---|---|---|
| R1 | Kafka 容器 Stop（真实 terminate，原 topic 日志被替换） | Kafka 容器 Start（同固定地址的新 broker，显式重建 canonical topic） | 是（Kafka） | 3 + 1 身份对 = 4 | step 1 | 4 |
| R2 | Kafka broker SIGSTOP（日志与已提交 offset 保留） | SIGCONT（同一日志续跑） | 否 | 4 + 1 = 5 | step 2 | 9 |
| R3 | Kafka SIGSTOP + Redis 容器 Stop（双故障） | SIGCONT + Redis 容器 Start | 是（Redis） | 4 + 1 = 5 | step 3 | 14 |

每轮编排固定为：故障注入并确认不可达 → 真实 Append 路径继续提交（行保持 pending）→ 故障期快照 → 恢复并确认可达 → `ForceRetry` → 发布器有界批量排空（消费者尚未启动）→ 消费追赶（CatchupGap 递减至 0）→ **传输层重复发布**（本轮行重置 pending 后经真实发布器重发一次）→ 身份/效果断言。

## 3. 断言清单

每轮（`oscillation_round` 证据条目）：

1. **暂停与恢复**：注入后 `!KafkaReachable`（R3 另断言 `!RedisPing`）；恢复后两者可达（有界等待，超时即失败）。
2. **持久进度单调**：`consumer_progress` 按分区读取，故障前/故障中/追赶采样逐次 `next_offset` 不下降；分区行不得消失；追赶后至少存在进度行。
3. **积压追赶（CatchupGap 递减归零）**：消费者启动前 gap 精确等于本轮新增行数；追赶采样序列非递增且从 >0 到 0（发布器排空后消费者才启动，序列确定）。
4. **事件身份（同身份字节一致）**：同一 fact 经 `events.NewEvent` 两次产生相同 canonical payload 字节/hash；经 `events.Append` 二次提交必须 `Noop=true`、同 `event_id`/`outbox_id`/`aggregate_version`，且仅 1 行；存储 `payload_hash` 等于独立重算的 canonical hash。
5. **事件身份（重复发布可吸收）**：本轮行重置 pending 后经真实发布器重发（`attempt_count >= 2`），消费者提交位点前进 `>= 本轮行数`，ledger 与 gap 保持不变；另将 topic 中该事件的两份已投递字节与 outbox 行的确定性序列化（`OutboxRecord.EnvelopeJSON`）逐一比对（字节一致），并把完全相同字节再次送入真实参考消费者 → `OutcomeDuplicate`，效果数不变。
6. **参考消费者效果（效果=1）**：本轮每个事件 `LedgerApplications == 1`；全量 `assertLedgerExactlyOnce`（总行数 = 已发布数、无重复 event_id、无 open quarantine）；无 blocked 行。
7. **发布有界**：发布周期 `Claimed <= batch`（4）。

最终（`oscillation_final` 证据条目）：

- `pending=0`、`blocked=0`、`published=total=14`、`CatchupGap=0`；`CollectInvariants` + `AssertInvariants`（0 重复意图/nonce/发送槽、0 orphaned 入账、0 静默丢事件、0 blocked、0 open quarantine）。
- 发送侧权威指纹（`payment_intents`、`nonce_bindings`、`tx_attempts`、`tx_attempt_signings`、`tx_send_attempts`）全程不变：投递/消费/追赶**不产生任何付款权限或副作用**。
- 全程 0 静默丢失、0 重复模拟账本效果。

## 4. 证据索引（简短）

| 载体 | 内容 |
|---|---|
| `internal/faultdrill/three_round_oscillation_integration_test.go` | 测试本体（固定时间线 `oscillationTimeline`、每轮断言、失败非零） |
| `timeline.jsonl` `kind=oscillation_timeline` | 固定三轮时间线（故障/恢复标签、容器 Stop/Start 标记、事件量） |
| `timeline.jsonl` `kind=oscillation_round` | 每轮：故障期 pending、最老等待、gap 序列、排空/追赶耗时（仅实测值）、进度前后快照、事件 ID、身份/重复吸收标记 |
| `timeline.jsonl` `kind=oscillation_final` | 终态计数、不变量、权威指纹前后 |
| `env_spec.json` / `metrics.txt` | 环境规格（commit/镜像/主题）与 Prometheus 导出 |
| 原始大日志 | `/tmp/opencode/013-supplement/`（机器本地、**不入库**）；入库持久载体 = 测试文件 + 提交哈希 + 本文件 |

复现命令（orchestrator 实测；不与其他两路并发）：

```bash
mkdir -p /tmp/opencode/013-supplement
TXHARBOR_FAULT_EVIDENCE_DIR=/tmp/opencode/013-supplement/oscillation-evidence \
  go test -tags fault -count=1 -timeout 60m -run TestThreeRoundDependencyOscillation ./internal/faultdrill \
  2>&1 | tee /tmp/opencode/013-supplement/oscillation-run.log
```

## 5. 边界与未测项

- 未测/不宣称：集群选举或 broker 故障切换；跨 broker 替换的 Kafka offset/持久进度续传；追赶/排空耗时阈值（**不设阈值**；实测值见 §6，仅记录）。
- 参考消费者非生产账本、非权威（`events.ReferenceBoundaryStatement`），其效果计数仅证明本项目事件身份/版本/投递语义与消费者幂等契约。
- 完整三轮实测结果见 §6（已执行；§2/§3 保留运行前固定的时间线与断言清单，不随实测改写）。

## 6. 三轮实测结果（orchestrator 顺序执行；2026-09-24；单主机）

- 结果: **通过** —— `--- PASS: TestThreeRoundDependencyOscillation (84.67s)`，`ok github.com/xtianxx/txharbor/internal/faultdrill 84.688s`；无 SKIP、无 panic、无失败重试。
- 环境: go1.26.5 / linux/amd64；commit `d2bf559`；PG `postgres:18.6-trixie`、Kafka `confluentinc/confluent-local:7.9.10`、Redis `redis:8.2.10-alpine`（`env_spec.json`）。
- 原始日志（未入库）: `/tmp/opencode/013-supplement/oscillation-run.log`；结构化证据: `/tmp/opencode/013-supplement/oscillation-evidence/{timeline.jsonl,env_spec.json,metrics.txt}`。
- 执行命令: 见 §4 同式（`-timeout 60m`）；未与其他两路并发。

### 6.1 实际时间线（`timeline.jsonl` 的 `at`，UTC；耗时=实测）

| 轮 | 证据时刻 | 故障 → 恢复 | 容器 Stop/Start | 故障期新增 | 故障期 pending / 最老等待 | 排空 / 追赶 (s) | gap 序列 | 累计 published |
|---|---|---|---|---|---|---|---|---|
| 启动 | 16:57:19.95 | — | — | — | — | — | — | 0 |
| R1 | 16:57:36.91 | Kafka 容器 Stop → 新 broker Start | 是（Kafka） | 4 | 4 / 0.015s | 0.405 / 0.423 | `[4, 0]` | 4 |
| R2 | 16:57:57.67 | Kafka SIGSTOP → SIGCONT | 否 | 5 | 5 / 0.015s | 0.206 / 0.108 | `[5, 2, 0]` | 9 |
| R3 | 16:58:21.58 | Kafka SIGSTOP + Redis 容器 Stop → SIGCONT + Redis Start | 是（Redis） | 5 | 5 / 0.021s | 0.208 / 0.110 | `[5, 2, 0]` | 14 |
| 终态 | 16:58:21.61 | — | — | — | `pending=0`、`gap=0` | — | — | 14 |

测试进程 wall 84.67s = 容器启动 ≈9s + 三轮 ≈62s + 清理 ≈12s；单轮间隔 17–24s（含注入/快照/恢复等待）。

### 6.2 每轮断言 ↔ 实测（断言编号同 §3）

1. **暂停与恢复**：R1 Kafka 容器 Stop 后不可达、Start（显式重建 canonical topic）后可达；R2/R3 SIGSTOP 后不可达、SIGCONT 后可达；R3 故障期另有 Redis 不可达、容器 Start 后可达。全部有界等待，无超时失败。
2. **持久进度单调**：逐分区 `next_offset` 采样不下降、分区行不消失。R1 `progress_before={}` → `after={2:1,3:1,4:1,5:1}`；R2 `before={2:2,3:2,4:2,5:2}` → `after={1:3,2:3,3:2,4:3,5:2}`；R3 `before={1:6,2:4,3:2,4:4,5:2}` → `after={0:2,1:6,2:5,3:3,4:4,5:3}`（追赶后均至少 1 进度行；分区集合可因新发布增加，单调性按分区成立）。
3. **积压追赶归零**：消费者启动前 gap 精确等于本轮新增（R1=4、R2=5、R3=5）；序列 R1 `[4,0]`、R2 `[5,2,0]`、R3 `[5,2,0]` 非递增且归零。
4. **事件身份**：每轮 `noop_reappend=1`（同 fact 二次 `Append` 为 Noop、同身份仅 1 行）、`identity_event_id` 记录；`envelope_bytes_identical=true`（topic 两份投递字节 = outbox `EnvelopeJSON` = 独立重算 canonical hash）。
5. **重复发布可吸收**：每轮传输层重复发布（R1=4、R2=5、R3=5 条），`committed_offsets_before_dup`=4/8/18，`duplicate_absorbed=true` 且 `duplicate_effects=0`；ledger 与本轮 gap 不变（效果不重复）。
6. **参考消费者效果=1**：每轮 `effects_per_event=1`；三轮后全量 `assertLedgerExactlyOnce` 通过（见 6.3）。
7. **发布有界**：`publish_cycles`=1/2/2，每周期 `Claimed ≤ batch(4)` 且账目守恒由用例逐周期断言通过。

### 6.3 最终终态（`oscillation_final`）

- `pending=0`、`blocked=0`、`published=14=total_events`、`gap=0`、`progress_rows=6`、`loss=0`、`duplicate_effects=0`。
- 不变量全 0：`duplicate_intents/nonces/send_slots=0`、`orphaned_ledger_credits=0`、`missing_events_for_committed_units=0`、`blocked_rows=0`、`open_quarantine=0`、`outbox_total=14`。
- 发送侧权威指纹前后不变：`payment_intents`、`nonce_bindings`、`tx_attempts`、`tx_attempt_signings`、`tx_send_attempts` 前/后均为 0 —— 投递/消费/追赶未产生任何付款权限或副作用。
- 三轮各自累加 published 4→9→14，与本轮新增一致：**0 静默丢失**。

### 6.4 设计边界符合性（对本文件 §1 固定时间线的核对）

- R1（真实容器替换轮）实测 `progress_before={}`：替换发生在**任何持久消费进度出现之前**，与 §1 固定设计一致；跨 broker 替换的 offset 续传仍**不宣称**。
- R2/R3 保留日志（SIGSTOP/SIGCONT；R3 另替换 Redis 容器，Kafka 日志与 offset 保留）上观察到存量进度（R2 前 4 分区、R3 前 5 分区）并续传，符合「持久进度续传只在保留日志原语上验证」的口径。
- 单节点容器的 Stop/Start 仅为真实容器替换证据，**不构成**集群选举/故障切换验证。
