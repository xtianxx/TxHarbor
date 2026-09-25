# 013 补充证据：双独立 OS 进程发布器（dualproc）

- Feature: `013-reliable-event-infrastructure`（补充批次；分支 `013-verify-supplement`，基线 `origin/main@d2bf559`）
- 任务: T034「双实例」范围澄清 + V-PUBLISHER 双独立进程补充证据
- 状态: 用例实现完成；**三次 10 分钟默认时长运行通过**（§4 旧运行 2026-09-24 `654.62s`；§4.1 复运行 2026-09-25 `656.37s`；本轮收口后再次运行 `661.65s`）；缩短时长冒烟、失败复现与逐次结果详见 §4.1
- 结论范围（硬限制，随引用携带）: **单本地主机**、两个真实独立 OS 进程、单 PostgreSQL 容器 + 单 Kafka broker、合成负载。**非多主机/分布式/生产结论**，不替代 Q8/Q9 故障演练与 Q10 基准。用例的冻结/恢复判定依赖 **Linux `/proc/<pid>/status`**（§3、§5）。
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
| 双进程身份 | 两个 PID、两个互异 `owner=<32hex>`；子进程环境由 `dualprocChildEnv` 白名单构造（仅 PATH/HOME/TZ + 显式 013 配置），父进程 `TXHARBOR_*` 不在其中（测试进程的构造保证，不是对父环境的投毒断言） | `identity A/B: owner=... pid=...` |
| 两者均实际领取并发布 | 两个进程各自 stderr 的 `publish cycle claimed/acked` 与 `outbox published count` 均 >0；日志计数与 DB `sum(attempt_count)`/`published` 在一个 batch 内闭合（被 kill 进程的在途 batch 除外） | `phase=both-publishing ...`、`attribution ...` |
| 互斥分区 | 采样中 owner 只能是二者之一（无未知 owner）；两 owner 在同一时刻各持不相交的 pending 分片（双双冻结快照，用例显式断言两分片交集为空，失败即 `t.Fatal`） | `mutual-exclusion ...`、`phase=partition-snapshot ...` + 每行 `partition owner=... row=...` |
| 租约过期/一进程退出接管 | 冻结 B 的在途领取：用例以 `FOR UPDATE SKIP LOCKED` 行锁**原子**锁定 B 已提交的 pending 行（contested 集合只含真正锁到的行），再 SIGSTOP 并以 `/proc/<pid>/status` 的 `T` 状态确认（SIGCONT 后同样以 `/proc` 确认为非停止态）；SIGKILL 模拟崩溃退出后，**在行锁仍持有、SIGSTOP 仍生效时按各进程独立的 `application_name` 终止死进程的全部 PostgreSQL 后端**，强制其未提交事务中止（SIGKILL 不会杀死被行锁阻塞的服务端后端；若先释放锁，该后端仍可能完成并提交被阻塞的 settle，行会由死 owner 自行发布、根本不存在遗留租约），随后释放行锁并做数据库可观测的注入前置校验；B 持有的 pending 行在租约红线（`claim_expires_at − 1s`，见下）前保持 owner=B，其后由 A 重领（`attempt_count +1`）并发布 | `phase=freeze-start ...`、`phase=freeze-hold ...`、`phase=freeze-backends ...`、`phase=crash-backends-terminated ...`、`phase=crash-kill ...`、每行 `takeover row=... pre_attempt/post_attempt/expires`、`lease-takeover verified` |
| 最终排空 | `pending=0`、`blocked=0`、无未清 claim；全部行 `published` 且行数=追加事件数 | `phase=drained ...`、`final-outbox ...` |
| 停机 | 幸存进程 SIGTERM 后退出码 0 且 stdout 含 `stopped` | `phase=graceful-stop ... exit=0 stdout=stopped` |
| 消费效果=1 / 无重复付款副作用 | 从 topic 起点全量消费；参考消费者持久幂等使每事件账本行数恰为 1（kill 点产生的重复投递被吸收）；007/008/009/010/011 表 0 行 | `consume records=... applied=... duplicates_absorbed=...`、`effect ledger_rows_per_event=1 ...`、`upstream_payment_rows=0` |

故障注入与判定细节: 冻结 B 分三步——(1) 用例开启测试侧事务，以 `SELECT id ... WHERE publish_state='pending' AND claim_owner=$B FOR UPDATE SKIP LOCKED` 原子锁定 B 已提交的 pending 行，并把 contested 集合限定为**实际锁到的行**（普通读可能同时看到 owner 自身未提交 settle 正在更新的行；那些行未被本测试的行锁钉住，不进入 contested）：锁定成功即证明这些行在释放前既不能被 B 的 owner-guarded settle UPDATE 提交，也不能被 A 的 claim 扫描领取（行锁性质，非时序竞态；查询返回零行表示本轮领取窗口已结算，重试）；(2) 向 B 发送 SIGSTOP，并以 `/proc/<pid>/status` 的停止状态 `T` 确认（有界轮询，不再依赖固定 sleep）；(3) B 被 SIGKILL 之后、行锁释放之前，按各进程独立的 `application_name` 终止死进程的全部 PostgreSQL 后端，强制其未提交事务中止——SIGKILL 只杀死客户端进程，被行锁阻塞的服务端后端不会随之消失；若先释放锁，该后端仍可能完成并提交被阻塞的 settle，使行由死 owner 自行发布、根本不存在遗留租约。释放锁后立即做一次数据库可观测校验：contested 行不得出现「published 且 attempt_count ≤ 崩溃前 attempt」（命中即判崩溃注入失败）。行锁保持到完成上述终止之后才释放，A 因此不可能在崩溃注入前接管被遗留行、也不会提前结算自身被钉住的批次。接管时间以下列断言为准——被遗留行不得早于租约红线离开 owner=B，且其 `attempt_count` 至少 +1。判定方向与断言一致：采样中同刻观察到两 owner 各自持有互不相交的 pending 分片（`seenA/seenB>0`、`both>0`，且两分片交集为空由用例显式断言）是**通过证据**；失败条件（命中任一即 `t.Fatal`）是——采样从未观察到双 owner 分片（或任一身份从未被观察到）、出现两个身份以外的未知 claim owner、活租约被抢（行早于租约红线离开 owner）、或单事件重复应用效果 >1。接管判定先以数据库 `pending=0` 为准，再有界等待采样器记录每行的完整时间线（离开 owner=B → published；该等待在等待循环内主动采样，超时未观测时输出持久状态以区分采样器漏检与行从未被接管，不再依赖后台 ticker 恰好落在短窗口内；观测方法与失败诊断见 §4.1）；行的接管后状态是终态，该等待是可观测条件而非固定 sleep，失败仍由上述断言判定。

租约判定的 1 秒宽限: 租约红线是 `claim_expires_at − 1s`（代码常量 `dualprocLeaseGrace`），不是精确到期时刻。该宽限用于容忍采样间隔（25ms）以及两个子进程、PostgreSQL 与测试主机之间的时钟差异，其作用是判定「活租约未被抢」；它**不**构成精确到期验证——用例并不断言行恰好在 `claim_expires_at` 之后被接管，只断言不早于红线离开 owner 且最终全部发布。

## 4. 结果

| 项 | 状态 |
|---|---|
| 实现冒烟（`TXHARBOR_DUALPROC_DURATION=20s`，本机，四次独立运行） | **通过**。最终次（`dualproc_smoke4.log`）：双身份；两进程 claimed/acked 均 >0（A=300/B=300）；双双冻结快照同刻双 owner（A=64/B=99 行，互不相交）；B 崩溃遗留 99 行，全部在 `claim_expires_at` 后由 A 以 `attempt_count 1→2` 重领发布；全量排空（1423/1423 published，0 pending/blocked）；SIGTERM 退出码 0；消费效果每事件=1（99 条重复投递被吸收）；上游付款表 0 行。前三次分别遗留 100/13/85 行，同样闭合 |
| 10 分钟默认时长实测（旧运行，验收口径；2026-09-24） | **通过**（2026-09-24，go1.26.5，单主机；HEAD `d2bf559` + 未提交测试文件；`TestPublisherDualProcessSupplement` 654.62s，含容器启停）。双身份：A `owner=7717dcb573f19e33fbcf0a952f168bca pid=2286460`、B `owner=11d4ca08ef36eeb28d5503af6d263543 pid=2286474`（两个互异 32hex，独立 OS 进程）；稳态双发 A claimed/acked 400/400、B 300/300；冻结快照双 owner 同刻并持不相交分片（A=89/B=79 行，共 168 行）；B SIGSTOP→SIGKILL 后 79 行在 `claim_expires_at` 前保持 owner=B，到期后由 A 重领（`attempt_count 1→2`）并全部发布；终态 5803/5803 published、pending=0、blocked=0、unowned_claims=0、attempts=5882；幸存进程 A SIGTERM 退出码 0 且 stdout `stopped`；全量消费 records=5882→applied=5803、79 条重复投递被吸收、每事件账本行数=1（min=1 max=1）、上游付款表 0 行；互斥采样 24025 次 unknown_owners=0 |
| 多主机 / 生产负载 / 小时级长稳 | 未测、不宣称 |

冒烟原始日志（未入库）: `/tmp/opencode/013-supplement/dualproc_smoke.log`、`dualproc_smoke2.log`、`dualproc_smoke3.log`、`dualproc_smoke4.log`。

10 分钟验收运行原始日志与关键证据行（均未入库）:

- 原始日志: `/tmp/opencode/013-supplement/dualproc_full.log`（含最终 `--- PASS: TestPublisherDualProcessSupplement (654.62s)`）；结构化证据: `/tmp/opencode/013-supplement/dualproc-test-evidence.txt`（由 `TXHARBOR_DUALPROC_EVIDENCE_DIR` 写出）。
- 关键行（前缀 `DUALPROC-EVIDENCE`）: `scope=single-host; publisher_processes=2; duration=10m0s; lease=5s; batch=100; poll=250ms`；`phase=both-publishing A(claimed=400 acked=400 observer_published=400) B(claimed=300 acked=300 observer_published=300)`；`phase=partition-snapshot ownerA_claims=89 ownerB_claims=79 (disjoint rows, both owners pending simultaneously)`；`phase=crash-kill owner=11d4… pid=2286474 contested_rows=79 killed=true`；79 行 `takeover row=… pre_attempt=1 post_attempt=2 cleared_at=… (晚于 expires=…)`；`lease-takeover verified: 79 orphaned claims reclaimed only after claim_expires_at, all published`；`phase=drained pending=0 blocked=0 unowned_claims=0`；`phase=graceful-stop owner=7717… pid=2286460 exit=0 stdout=stopped`；`mutual-exclusion samples=24025 ownerA_seen=8 ownerB_seen=208 both_simultaneous=3 unknown_owners=0`（`both_simultaneous` 指采样瞬间两 owner 各持**不相交**分片，为分片并行的预期形态；`unknown_owners=0` 与同刻同行的互斥断言由用例另行强判）；`final-outbox events=5803 published=5803 pending=0 blocked=0 attempts=5882`；`attribution A(claimed=4661 acked=4661 published=4661) B(claimed=1142 acked=1142 published=1142) durable_attempts=5882 published_rows=5803`（A/B 差值来自崩溃点后 B 的不再参与；重复 publish 计 79 = attempts−published）；`consume records=5882 applied=5803 duplicates_absorbed=79 expected_events=5803`；`effect ledger_rows_per_event=1 for all 5803 events (min=1 max=1); upstream_payment_rows=0`。
- 耗时口径: 测试进程 wall 654.62s（≈10m54.6s）= 容器启动 ≈8s + 10m 稳态窗口 + 崩溃/接管/排空/全量消费/清理 ≈46s；`scope` 行记录窗口 `duration=10m0s`，未使用 `TXHARBOR_DUALPROC_DURATION` 缩短。

### 4.1 复运行、失败复现与本轮收口（2026-09-25）

本节与 §4 严格区分：§4 的 10 分钟运行是 2026-09-24 在 `d2bf559`+未提交测试文件上的旧运行（`654.62s`，分片 A=89/B=79，5803 事件）；本节第一行是 2026-09-25 在同一用例（`7402706` 文件内容）上的复运行（`full10m.log`，`656.37s`，分片 A=30/B=33，4903 事件）。两轮分片大小不同只反映冻结瞬间两个进程各自的在途领取窗口（瞬时状态），不构成可比性结论；两轮都以全量排空与消费效果=1 闭合。

| 运行（2026-09-25，单主机） | 参数 | 结果 | 冻结分片 A/B（contested=B） | 事件/attempt | 备注 |
|---|---|---|---|---|---|
| 复运行 `full10m.log`（HEAD `7402706` 文件内容） | 10m 默认 | 通过 `656.37s` | 30/33 | 4903/4936 | 33 行遗留全部 `attempt 1→2`；`takeover-observed 33/33`；samples=24025 |
| 负对照 `fix_target15.log`（本轮修正前） | 15s | **失败**（定向复现，用于定位） | 52/39 | 1471/1471 | 见下第 3 条：`attempts=published`，死 owner 自己发布了 contested 行 |
| 修正后 `fix_target15b.log` | 15s | 通过 | 46/45 | 1393/1438 | 45 行 `attempt 1→2`；`crash-backends-terminated`=0（该轮无悬挂后端） |
| 修正后 `fix_smoke1/2/3/4.log` | 20s ×4 | 4/4 通过 | 20/53、50/10、47/12、74/31 | 1423/1476、1433、1435、1454 | 每轮 contested 行全部 `attempt 1→2` 且不早于租约红线；`crash-backends-terminated`=1、1、0、1（四次中有三次确有死进程后端被终止）。`fix_smoke4.log` 为最终提交文件（仅证据行顺序调整后）的复跑 |
| 修正后完整 10 分钟 `fix_full10m.log` | 10m 默认 | 通过 `661.65s` | 53/22 | 5203/5225 | 22 行 `attempt 1→2`；消费 duplicates=22；samples=24028 unknown=0；`crash-backends-terminated`=1 |

先前两次短冒烟失败（如实记录与归因）:

1. `new_smoke1.log`（中间版本：行锁冻结、尚无有界观测等待）：接管等待 3 分钟超时，63 行 contested 的保留日志显示其始终为 pending（owner=B）。从该日志**无法**进一步区分是测试等待条件、环境调度还是产品行为——该次失败属**未完全定位**。本轮已为该路径补齐有界等待内的进度心跳、幸存进程存活/`/proc` 状态/计数、持久行状态与 `pg_stat_activity` 后端诊断，若复现可直接分类，不需重跑猜测。
2. `new_smoke2.log`（同一中间版本）：A 已接管并发布全部 44 行（`attempts=1467=1423+44`，数据库已排空），但用例在 `pending=0` 后立即读取采样器（该版本没有有界观测等待），得到 `post_attempt=0` 的**误红**。根因是观测方法缺界：`7402706` 已引入有界等待，本轮再改为等待循环内主动采样（不依赖后台 ticker 恰好落在短窗口内），并在超时未观测时输出该行持久状态以区分「采样器漏检」与「行从未被接管」。
3. 本轮负对照（`fix_target15.log`，修正前）稳定复现了**崩溃注入不确定**：被 SIGKILL 的客户端进程，其 PostgreSQL 后端不会随之消失——一个被本测试行锁阻塞的后端在锁释放后仍可完成并提交它被阻塞的 settle（该轮证据：contested 行 `durable(state=published owner="" attempt=1)`、`takeover-db-cleared wait=2ms`、`sampler_observed=0/39`），于是 contested 行由死 owner 自己发布、从未形成遗留租约；同一注入路径还可能让 contested 集合纳入未被行锁钉住的行。修正：每进程独立 `application_name`；SIGKILL 后在行锁仍持有的状态下终止死进程全部后端（强制其中止）；contested 改为「行锁实际锁到的行」；释放锁后新增数据库可观测的注入前置校验（`phase=freeze-backends`、`phase=crash-backends-terminated`，以及「published 且 `attempt_count ≤` 崩溃前值」即判注入失败）。**这是夹具缺陷，不是产品缺陷**：产品侧的租约/`claim_owner` 守卫语义未变，用例只是把「崩溃后确实存在遗留租约」这一前置条件做成确定性且可验证的。
4. 旧路径对照（`7a15c04`，SIGSTOP 停点轮询冻结）：本轮 4 次运行中 3 次通过、1 次失败（`old_smoke2.log`："process A held no claims while B was frozen"），失败类正是 `7402706` 行锁硬化所要消除的毫秒级时序竞态。

结论表述纪律：本轮修正后共 6 次通过（1×15s + 4×20s + 1×10m），逐次记录、非重试择优；它证明该夹具在本次环境/主机上可用，**不构成统计稳定性证明**，也不外推到多主机/生产负载。原始日志与结构化证据均在 `/tmp/opencode/013-fix/`（`fix_target15*.log`、`fix_smoke1..4.log`、`fix_full10m.log` 及各自 `dualproc-test-evidence.txt`，未入库）。

## 5. 边界（必须随引用携带）

- 单主机、单 broker、单 PG 容器、合成负载；**非**多主机/分布式/生产结论。结论只覆盖「同一本地主机上的两个真实独立 OS 进程」。
- **Linux `/proc` 依赖**: 冻结的 SIGSTOP 生效与 SIGCONT 恢复都以 `/proc/<pid>/status` 的进程状态判定（有界轮询）；在没有 `/proc` 的主机上这些探针按失败处理（不静默通过），该用例不应被视为可移植到非 Linux 主机。
- 通过次数只说明夹具在本次环境可用，**不是统计稳定性证明**；`new_smoke1` 的接管超时未完全定位（见 §4.1 第 1 条），不得据此宣称长稳或多主机结论。
- 参考消费者是模拟账本（非生产、非权威），其效果断言只覆盖本项目事件身份/版本/投递语义与消费者幂等契约；逐字边界见 `internal/events/refconsumer.go` 的 `ReferenceBoundaryStatement`（由证据行 `boundary ...` 原样输出）。
- 至少一次投递 + 幂等处理；重复发布可发生（本例在 SIGKILL 点即产生与在途领取等量的重复记录）且被消费者吸收；不宣称跨系统恰好一次。
- 夹具侧的崩溃注入使用 PostgreSQL `application_name` 与 `pg_terminate_backend`（测试库管理权限），以确定性实现「客户端进程已死、其未提交事务必须中止」；这是**测试夹具**行为，不是产品运行时依赖——产品运行时从不终止后端。

## 6. tasks.md T034 范围澄清（已应用）

T034 行已做最小修订，区分旧同进程证据与新双进程证据（未改其他行）：

> 完成条件：单实例冒烟；双实例（同进程并发实例，T037 multi_instance_drain；双独立 OS 进程补充证据见 `docs/evidence/013-supplement/dualproc.md`，单主机）；……证据：同进程双实例运行记录（T037）；双独立进程记录：`docs/evidence/013-supplement/dualproc.md`
