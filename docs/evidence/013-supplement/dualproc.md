# 013 补充证据：双独立 OS 进程发布器（dualproc）

- Feature: `013-reliable-event-infrastructure`（补充批次；分支 `013-verify-supplement`，基线 `origin/main@d2bf559`）
- 任务: T034「双实例」范围澄清 + V-PUBLISHER 双独立进程补充证据
- 状态: 用例实现完成；**10 分钟默认时长实测已由 orchestrator 顺序执行并通过（§4；2026-09-24，单主机；`654.62s`）**；缩短时长冒烟此前已通过（§4）
- 结论范围（硬限制，随引用携带）: **单本地主机**、两个真实独立 OS 进程、单 PostgreSQL 容器 + 单 Kafka broker、合成负载。**非多主机/分布式/生产结论**，不替代 Q8/Q9 故障演练与 Q10 基准。
- 投递语义: 至少一次 + 幂等处理；本文件与用例从不断言跨系统恰好一次。消费效应用参考消费者模拟账本度量（FR-16 边界，见 §5）。

## 1. 为什么补充

已合入的 T034/T037「双实例」证据是**同一测试进程内**两个 `*Publisher` 实例（goroutine 并发领取/发布：`TestPublisherKafkaRuntime/multi_instance_drain`），加上同进程 PG 层崩溃/租约矩阵（T036）。它们未启动两个独立 OS 进程，也未覆盖「真实进程被杀 → 租约到期 → 幸存进程接管」的进程级路径。

本补充用真实 `txharbor event-publisher` 二进制启动两个独立进程（独立 PID、独立 `owner` 身份、独立 PG/Kafka 连接），共享同一隔离 PG/Kafka，验证既定领取协议（`FOR UPDATE SKIP LOCKED` + `claim_owner`/`claim_expires_at` 租约 + `attempt_count` 递增；`internal/events/outbox.go`）在进程级成立。

## 2. 载体与复现

- 测试: `internal/events/publisher_dualproc_integration_test.go`（用例 `TestPublisherDualProcessSupplement`）
- 构建标签: `integration_dualproc`（独立层；**不**被 `make test-integration-kafka` 或 PR 必需 CI 收集——10 分钟双进程长测不进 PR 预算）
- 命令:

```sh
go test -tags integration_dualproc -count=1 -timeout 25m \
  -run TestPublisherDualProcessSupplement -v ./internal/events
```

- 时长: 默认约 10 分钟（`TXHARBOR_DUALPROC_DURATION` 仅用于实现冒烟，如 `20s`；验收运行用默认值）。
- 原始日志（机器本地、**未入库**）: `/tmp/opencode/013-supplement/dualproc_full.log`；结构化证据（可选）: `TXHARBOR_DUALPROC_EVIDENCE_DIR` 下 `dualproc-test-evidence.txt`。
- 跳过/清理约定: 无 Docker provider 时 `testcontainers.SkipIfProviderIsNotHealthy` 跳过（绝不算通过）；失败即 `t.Fatal`（非零退出）；PG/Kafka 容器与两个子进程均在 `t.Cleanup` 中清理（失败亦清理，已由冒烟中的一次失败验证）。

## 3. 断言 ↔ 证据

| 要求 | 断言 | 证据行（前缀 `DUALPROC-EVIDENCE`） |
|---|---|---|
| 双进程身份 | 两个 PID、两个互异 `owner=<32hex>`；子进程环境不含父进程 `TXHARBOR_*` | `identity A/B: owner=... pid=...` |
| 两者均实际领取并发布 | 两个进程各自 stderr 的 `publish cycle claimed/acked` 与 `outbox published count` 均 >0；日志计数与 DB `sum(attempt_count)`/`published` 在一个 batch 内闭合（被 kill 进程的在途 batch 除外） | `phase=both-publishing ...`、`attribution ...` |
| 互斥分区 | 采样中 owner 只能是二者之一（无未知 owner）；两 owner 在同一时刻各持不相交的 pending 分片（双双冻结快照） | `mutual-exclusion ...`、`phase=partition-snapshot ...` + 每行 `partition owner=... row=...` |
| 租约过期/一进程退出接管 | SIGSTOP 冻结 B 后在途领取稳定，SIGKILL 模拟崩溃退出；B 持有的 pending 行在 `claim_expires_at` 前保持 owner=B，到期后由 A 重领（`attempt_count +1`）并发布 | `phase=crash-kill ...`、每行 `takeover row=... pre_attempt/post_attempt/expires`、`lease-takeover verified` |
| 最终排空 | `pending=0`、`blocked=0`、无未清 claim；全部行 `published` 且行数=追加事件数 | `phase=drained ...`、`final-outbox ...` |
| 停机 | 幸存进程 SIGTERM 后退出码 0 且 stdout 含 `stopped` | `phase=graceful-stop ... exit=0 stdout=stopped` |
| 消费效果=1 / 无重复付款副作用 | 从 topic 起点全量消费；参考消费者持久幂等使每事件账本行数恰为 1（kill 点产生的重复投递被吸收）；007/008/009/010/011 表 0 行 | `consume records=... applied=... duplicates_absorbed=...`、`effect ledger_rows_per_event=1 ...`、`upstream_payment_rows=0` |

故障注入与判定细节: 对 B 先 SIGSTOP（冻结在途领取，消除「快照后已完成发布」竞态）再 SIGKILL；接管时间以下列断言为准——被遗留行在 `claim_expires_at` 之前不得离开 owner=B，且其 `attempt_count` 至少 +1。若同刻双 owner、活租约被抢或重复应用效果 >1，用例以 `t.Fatal` 失败。

## 4. 结果

| 项 | 状态 |
|---|---|
| 实现冒烟（`TXHARBOR_DUALPROC_DURATION=20s`，本机，四次独立运行） | **通过**。最终次（`dualproc_smoke4.log`）：双身份；两进程 claimed/acked 均 >0（A=300/B=300）；双双冻结快照同刻双 owner（A=64/B=99 行，互不相交）；B 崩溃遗留 99 行，全部在 `claim_expires_at` 后由 A 以 `attempt_count 1→2` 重领发布；全量排空（1423/1423 published，0 pending/blocked）；SIGTERM 退出码 0；消费效果每事件=1（99 条重复投递被吸收）；上游付款表 0 行。前三次分别遗留 100/13/85 行，同样闭合 |
| 10 分钟默认时长实测（验收口径） | **通过**（2026-09-24，go1.26.5，单主机；HEAD `d2bf559` + 未提交测试文件；`TestPublisherDualProcessSupplement` 654.62s，含容器启停）。双身份：A `owner=7717dcb573f19e33fbcf0a952f168bca pid=2286460`、B `owner=11d4ca08ef36eeb28d5503af6d263543 pid=2286474`（两个互异 32hex，独立 OS 进程）；稳态双发 A claimed/acked 400/400、B 300/300；冻结快照双 owner 同刻并持不相交分片（A=89/B=79 行，共 168 行）；B SIGSTOP→SIGKILL 后 79 行在 `claim_expires_at` 前保持 owner=B，到期后由 A 重领（`attempt_count 1→2`）并全部发布；终态 5803/5803 published、pending=0、blocked=0、unowned_claims=0、attempts=5882；幸存进程 A SIGTERM 退出码 0 且 stdout `stopped`；全量消费 records=5882→applied=5803、79 条重复投递被吸收、每事件账本行数=1（min=1 max=1）、上游付款表 0 行；互斥采样 24025 次 unknown_owners=0 |
| 多主机 / 生产负载 / 小时级长稳 | 未测、不宣称 |

冒烟原始日志（未入库）: `/tmp/opencode/013-supplement/dualproc_smoke.log`、`dualproc_smoke2.log`、`dualproc_smoke3.log`、`dualproc_smoke4.log`。

10 分钟验收运行原始日志与关键证据行（均未入库）:

- 原始日志: `/tmp/opencode/013-supplement/dualproc_full.log`（含最终 `--- PASS: TestPublisherDualProcessSupplement (654.62s)`）；结构化证据: `/tmp/opencode/013-supplement/dualproc-test-evidence.txt`（由 `TXHARBOR_DUALPROC_EVIDENCE_DIR` 写出）。
- 关键行（前缀 `DUALPROC-EVIDENCE`）: `scope=single-host; publisher_processes=2; duration=10m0s; lease=5s; batch=100; poll=250ms`；`phase=both-publishing A(claimed=400 acked=400 observer_published=400) B(claimed=300 acked=300 observer_published=300)`；`phase=partition-snapshot ownerA_claims=89 ownerB_claims=79 (disjoint rows, both owners pending simultaneously)`；`phase=crash-kill owner=11d4… pid=2286474 contested_rows=79 killed=true`；79 行 `takeover row=… pre_attempt=1 post_attempt=2 cleared_at=… (晚于 expires=…)`；`lease-takeover verified: 79 orphaned claims reclaimed only after claim_expires_at, all published`；`phase=drained pending=0 blocked=0 unowned_claims=0`；`phase=graceful-stop owner=7717… pid=2286460 exit=0 stdout=stopped`；`mutual-exclusion samples=24025 ownerA_seen=8 ownerB_seen=208 both_simultaneous=3 unknown_owners=0`（`both_simultaneous` 指采样瞬间两 owner 各持**不相交**分片，为分片并行的预期形态；`unknown_owners=0` 与同刻同行的互斥断言由用例另行强判）；`final-outbox events=5803 published=5803 pending=0 blocked=0 attempts=5882`；`attribution A(claimed=4661 acked=4661 published=4661) B(claimed=1142 acked=1142 published=1142) durable_attempts=5882 published_rows=5803`（A/B 差值来自崩溃点后 B 的不再参与；重复 publish 计 79 = attempts−published）；`consume records=5882 applied=5803 duplicates_absorbed=79 expected_events=5803`；`effect ledger_rows_per_event=1 for all 5803 events (min=1 max=1); upstream_payment_rows=0`。
- 耗时口径: 测试进程 wall 654.62s（≈10m54.6s）= 容器启动 ≈8s + 10m 稳态窗口 + 崩溃/接管/排空/全量消费/清理 ≈46s；`scope` 行记录窗口 `duration=10m0s`，未使用 `TXHARBOR_DUALPROC_DURATION` 缩短。

## 5. 边界（必须随引用携带）

- 单主机、单 broker、单 PG 容器、合成负载；**非**多主机/分布式/生产结论。
- 参考消费者是模拟账本（非生产、非权威），其效果断言只覆盖本项目事件身份/版本/投递语义与消费者幂等契约；逐字边界见 `internal/events/refconsumer.go` 的 `ReferenceBoundaryStatement`（由证据行 `boundary ...` 原样输出）。
- 至少一次投递 + 幂等处理；重复发布可发生（本例在 SIGKILL 点即产生与在途领取等量的重复记录）且被消费者吸收；不宣称跨系统恰好一次。

## 6. tasks.md T034 范围澄清（已应用）

T034 行已做最小修订，区分旧同进程证据与新双进程证据（未改其他行）：

> 完成条件：单实例冒烟；双实例（同进程并发实例，T037 multi_instance_drain；双独立 OS 进程补充证据见 `docs/evidence/013-supplement/dualproc.md`，单主机）；……证据：同进程双实例运行记录（T037）；双独立进程记录：`docs/evidence/013-supplement/dualproc.md`
