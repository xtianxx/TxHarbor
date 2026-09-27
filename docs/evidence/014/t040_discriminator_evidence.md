# T040 期望判别器实现证据（Expected-Event Discriminator）

- Feature: `014-reconciliation-exception-handling`
- 基线: HEAD `80b2b64`（`014-reconciliation-exception-handling`）+ 本工作树；**未提交、未推送**
- 关联: 设计 `specs/014-reconciliation-exception-handling/expected-event-discriminator.md`（Q-cutover 选项 B，2026-09-27 裁决）；任务 `tasks.md` T040；历史记录 `docs/evidence/014/quickstart_matrix_evidence.md` §6/§10（其 BLOCKED 记录为当时状态，本轮实现见本文）
- 结论口径: 本文只记录已实际运行的验证；远程 CI 仍待核验，不宣称已修复 T039 的 CI 分类缺口。

## 1. 迁移

- **编号 000017**：`migrations/000017_event_obligations.sql`（000001–000016 连续链上纯增量，不改 000015/000016 具名对象）。
- 新表 `event_obligation`（append-only；无触发器/函数/CDC/seed）：
  - 列：`obligation_id`（IDENTITY PK）、`aggregate_type`、`aggregate_id`、`aggregate_version`、`expected_event_type`、`obligated_at`、`source_kind`、`source_id`、`source_version`。
  - 具名约束：`event_obligation_pkey`、`event_obligation_identity_uniq`（aggregate 三元组＋expected_event_type）、`event_obligation_aggregate_version_check`、`event_obligation_source_version_check`、`event_obligation_aggregate_id_shape`、`event_obligation_source_kind_shape`、`event_obligation_source_id_shape`、`event_obligation_mapping_check`（封闭 `(aggregate_type, expected_event_type)` 映射，覆盖 8 类型）。
  - 索引：`event_obligation_aggregate_idx`、`event_obligation_obligated_at_idx`。
- **不伪造历史标记、不回填**：迁移不写任何行；Up 后行数=0 由测试断言。旧行无标记＝unknown。`event_ops_audit` 的 `retention_prune` 只删 outbox 已发布行，永不触碰本表（这正是“合法裁剪 vs 真丢失”的判据来源）。
- Down：仅 `DROP TABLE IF EXISTS event_obligation`；`TestEventInfrastructureMigrationUpDownUp` 覆盖 17→16→15→re-up 与 7 张 013 表在 down 后消失、再 up 恢复。
- 迁移编号/链断言同步更新：`internal/db/withdrawal_execution_migration_integration_test.go`（1..17）、`internal/db/scratch_009_overlay_integration_test.go`（joint head 17）、`internal/txlifecycle/migration_joint_integration_test.go`（tip 17）、`internal/signer/migration_integration_test.go`（head 17、applied=10/skipped=7 等重基线）、`internal/db/intent_fk_repair_integration_test.go`（era fixture 加 000017）。

## 2. 生产者接线与能力边界

**接线方式：唯一集中写点在 `internal/events/append.go` 的 `Append`。** 新增 `obligationInsertSQL`，在 outbox 行**实际插入成功**（`inserted=true`）后于**同一调用方事务**写入标记：`ON CONFLICT (aggregate_type, aggregate_id, aggregate_version, expected_event_type) DO NOTHING`。任一失败使 Append 失败，业务转换/事件行/标记整体回滚；幂等 no-op 重放（`inserted=false`）写 0 行、不回填。

| # | 目录类型 | 生产 Append 站点 | 业务转换调用点（同事务） |
|---|---|---|---|
| 1 | `deposit.observation.created` | `internal/indexer/outbox_events.go:104` | `internal/indexer/depositcommit.go:343` |
| 2 | `deposit.observation.status_changed` | `internal/indexer/outbox_events.go:146` | `internal/indexer/reorgcommit.go:588` |
| 3 | `deposit.observation.reinstated` | `internal/indexer/outbox_events.go:279` | `internal/indexer/reorgcommit.go:1310` |
| 4 | `deposit.confirmation.confirmed` | `internal/indexer/outbox_events.go:190` | `internal/indexer/confirmcommit.go:387` |
| 5 | `deposit.revision.applied` | `internal/indexer/outbox_events.go:350` | `internal/indexer/reorgcommit.go:599`（失效）/`:1315`（复活） |
| 6 | `withdrawal.request.received` | `internal/withdrawal/intake.go:226` | `internal/withdrawal/intake.go:498` |
| 7 | `withdrawal.execution.state_changed` | `internal/execution/intent.go:206` | `internal/execution/intent.go:248`（`TransitionIntentWithContext`） |
| 8 | `withdrawal.execution.revised` | `internal/execution/revision.go:370` | `internal/execution/revision.go:155` |

标记写在 `Append` 内的原因：8 类型/9 个调用点（含 revision 失效/复活两条路径）由构造覆盖，不存在漏接站点；写入与事件同一事务、同一提交/回滚单元。

**能力边界（必须与实现同时引用）**：

- 标记只能证明“Append 跑到并插入了事件时，该转换有事件义务”。**它不能检测整条 Append 路径被遗漏**（转换代码根本没调 Append 时既无事件也无标记），不得宣称独立完整性证明；生产者正确性仍由既有同事务原子性与 013 测试承担，属工程事项。
- 标记不参与“事件存在”判定：事件在场时判别器根本不读标记，既有 Present 路径与去重不受影响。
- `event_obligation_mapping_check` 封闭 pair；`internal/events/audit_integration_test.go` 的探针 fixture 原用 `audit_probe_object` 聚合，已按封闭映射改为 `deposit_observation`（探针只按 source_kind/source_id 断言）。

## 3. 判别落点与三路矩阵

- **唯一 party-status 出口**：`internal/reconciliation/scan.go` compare loop 的事件方构造处（原 L1798-1820）。仅当**决定性缺席**（chain Present ＋ PG Present ＋ event Absent，均在 `CoverageClosed` 证据下）时咨询期望证据；其他形态（链/PG 差异独自成票、事件在场、覆盖不完整）保持原状——不改事件方状态与 canonical bytes，**既有票 identity 不扰动**。
- **读取**：`internal/reconciliation/obligation.go` 新增 `EventObligationReader` 接口＋`NewEventObligationAdapter`（只读，两条有界 SELECT：`event_obligation` 按候选聚合批量读、`event_ops_audit` 最近 `retention_prune` 历史）。compare loop 每个含聚合的区间记账 2 次 PG；nil reader/读失败→R3。
- **构造**：`EventObligationAggregateOf` 把候选 EventKey（`aggregate/<type>/<id>`，013 冻结约定）映射到三个目录聚合；`tx_hash`/未知聚合类型→R2 证据。
- **裁决**（纯函数 `DiscriminateEventObligation`）：
  - **R1 `missing`**：存在标记，且**并非所有**期望都可被已审计裁剪解释（无裁剪历史也算不可解释）→ 事件缺席＝真丢失；沿用 classify.go 唯一造票点（`chain present && event absent → CategoryMissing`）。
  - **R2 `N/A`**：候选身份**可证明**无目录事件聚合（如 chain-first `tx_hash`）→ 事件方 `PartyNotApplicable`（新 emitter 落在 compare loop，不扩 `EventDeliveryStatus` 闭集、不动 canonical 版本域），classify 以新 reason `event_not_applicable` 收口；**不宣称三方一致**（`FullyConsistent()` 仍要求 upstream 阳性凭据）；链/PG 差异独立成立、不因事件 N/A 降级。
  - **R3 `pending`/gap**：无标记、读取失败/截断、或所有期望都可被合法裁剪解释 → 事件方 `PartyUnknown`，classify 新 reason `event_obligation_unproven`，常驻 pending、写 `query_failed` gap，不判一致、不闭合、不造票（标记缺席≠N/A；旧生产者未写标记保守 pending）。
- **合法裁剪/查询失败/真丢失三者区分**：读取审计裁剪窗口（`event_ops_audit.scope.retention` ＋ `created_at`）。`obligated_at < 任一 prune 的 cutoff` 且**全部**期望均如此→“可能被合法裁剪”→R3；存在任一期望晚于所有 cutoff→R1；审计历史读失败/畸形/截断→无法排除裁剪→R3。查询失败（表读错误）→ gap＋R3。证据不足一律保守。
- **祖父规则落地**：不在新标记缺失时无条件降级。已有充分证据的路径（事件在场、链/PG 差异独立成票、缺失 PG 行等）不经过判别器、分类不变；判别器只作用于“事件维度是唯一分歧”的形态。`event_obligation_mapping` 的封闭映射与候选聚合解析即“目录级义务证明”，其边界仍需业务确认（设计文档 §6-1）。

## 4. 旧任务兼容与失效

- 无标记旧任务一律 R3 pending：不自动关闭、不升级、不风险接受；存量票不被 scan 删除/关闭（集成测试用预置 `open_claimable` 票断言扫描后状态不变）。
- 旧实体新转换：义务按 (聚合, 事件类型) 标记独立判定，与实体年龄无关；不带标记的新转换同样 R3。
- 重组/版本轮换走原契约：判别器不改 `reverify`、`pending_verify`、失效规则；reverify 不调用 `Classify`，`EventStateReverifyEvaluator` 无改动。真实重验证入口测试 `TestIntegrationReverifyEntryUnaffectedByDiscriminatorPending`（同包集成）在 R3 pending 任务上跑真实 `RunReverifySweep`：rechecked/consistent/divergent=0，无新 verdict/票/关闭，R3 gap 保持 open、`VerifiedComplete()=false`；reconcileadmin `reverify` CLI 入口由既有 `TestIntegrationReconcileAdminUS3CallPathConvergence` 回归通过。
- 不新增风险接受/忽略/强制闭合/自动修复；资金门禁、011 gates、intake 容量/授权链未触碰（影响面回归见 §5）。

## 5. 验证矩阵（本地，真实 Docker）

| 层 | 命令 | 结果 |
|---|---|---|
| build/vet | `go build ./...`；`go vet`（含 `-tags integration`，events/reconciliation/reconcileadmin/db/withdrawal/signer/txlifecycle） | 通过 |
| 全量 unit | `go test ./...` | 通过 |
| 迁移 up/down/up | `go test -tags integration ./internal/db/`（含 17→16→15→re-up、additive-only、约束探针、编号 1..17、era fixtures） | 通过 |
| events（生产者原子性） | `go test -tags integration ./internal/events/`（8 类型标记、commit/rollback 三件套、no-op 零追加、标记失败回滚、裁剪后标记存活；含既有 append/conformance/publisher/consumer/replay 全套） | 通过（108.7s） |
| reconciliation（判别矩阵） | `go test -tags integration ./internal/reconciliation/`（R1 票+去重、R3 pending+gap、合法裁剪、旧票不关闭、R2 不造票且不掩链/PG 差异、真实 reverify 入口未被判别器改变；scan/chainfirst/window/lifecycle/reverify 全套） | 通过（61.5s） |
| reconcileadmin 真实入口 | `go test -tags integration ./internal/app/reconcileadmin/`（`start→resume→scan` 真 CLI：3d 有标记→missing；3e 无标记→pending+gap） | 通过 |
| 生产侧影响面 | `./internal/indexer/`、`./internal/withdrawal/`、`./internal/execution/`、`./internal/app/`、`./internal/txlifecycle/`、`./internal/signer/`（-tags integration） | 通过 |
| race 抽样 | `go test -race ./internal/events/ ./internal/reconciliation/ ./internal/app/reconcileadmin/`；`go test -race -tags integration -run 'TestAppendConcurrentVersionRace|TestIntegrationAppend…' ./internal/events/` | 通过 |
| NOT RUN | `make test-race` 全量、fault/perf 通道、Redis/Kafka e2e、远程 CI（未推送、未触发，T039 并行 lane） | 未运行 |
| T039 CI 缺口 | `internal/reconciliation/**` 是否已纳入 pg 分类集由 T039 lane 负责，本文不宣称 | 待核验 |

失败修正（首跑→复跑全绿）：

1. `internal/withdrawal`：`TestWithdrawalRecoveryPeriodNoExecutionArtefacts` 的 above-007 快照新增 `event_obligation` 例外，并把 delta 钉为恰好 1 条 receive fact 的标记（与 013 outbox 例外同形，注释记录 T040 调整）。
2. `internal/db`：intent-FK 修复序列的 era fixture 与计数（applied/skipped）按 17 链重基线；`scratch_009` overlay 的 joint head、round-trip 断言改为 17。
3. `internal/events`：audit 探针 fixture 聚合类型按封闭映射改为 `deposit_observation`。

## 6. 遗留与边界（不自行批准）

- 设计文档 §6 三项业务问题仍开放：祖父证据清单边界、旧生产者永久 pending 的运营口径、期望义务目录变更权。本实现只落地机制，未扩充/缩减任何“充分证据”认定。
- 判别粒度：候选事件身份按**聚合**匹配（013 matcher 语义），标记存在即证明该聚合至少一项转换有义务；无法区分聚合内“哪一个转换”缺失。这是保守方向（聚合有事件则 Present；全无事件且有标记则 missing）。
- 事件裁剪窗口来自 `event_ops_audit` 审计行（events-admin 操作记录）；若从未执行过 retention-prune，则不存在“合法裁剪”解释——该口径需运营确认。
- 不提交、不推送；本文不构成发布/生产就绪声明。
