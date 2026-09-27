# 014 复核写写反序缺陷修复证据（2026-09-27 复核有效性令牌轮）

- Feature: `014-reconciliation-exception-handling`
- 基线: `014-reconciliation-exception-handling` @ `034b488`（修复前工作区干净；本文件所述修复未提交）
- 关联任务: T026 / T027 / T028（修复前恢复未完成，达标后重新勾选；见 `specs/014-reconciliation-exception-handling/tasks.md`）
- 关联设计: `data-model.md` §3.1、`contracts/discrepancy-lifecycle.md` §Revalidation Token Protocol、`quickstart.md` §call-path
- 范围: 只修复核写入的时序保护；不放宽 close/处置/恢复/付款门禁，不改业务语义，不新增 daemon。

## 1. 根因（已证实）

证据读取在事务外进行；旧实现只在写入时校验票行状态（单票入口）或完全不校验（历史 sweep），没有捕获/校验取证前的有效性令牌。已证实的交错：

1. A 先取证（读取开始时票为 `pending_verify`/`closed`），在证据读取中被暂停；
2. B 后取证但先提交（`divergent`/`unknown`，或更新的 `consistent`）；
3. A 恢复提交：旧实现接受过期结果 → 覆盖 B 的有效结论（最新裁决变为 A 的旧 `consistent`）、删除 B 的 `query_failed` gap、推进 sweep 游标/计数；
4. `close` 在行锁内读到 A 的旧 `consistent` → 接受。

基线症状探针（`/tmp/opencode/tx-baseline` 工作树，修复前代码）：

```text
DEFECT SYMPTOMS: A.Discarded=false A.Consistent=true latest=consistent B.gapRemaining=0 closeErr=<nil>
```

即：A 未被丢弃、B 的结论被覆盖、B 的 gap 被删除、close 接受了旧 consistent。`created_at`/进程时钟/裁决类型都不构成保护（`consistent` 覆盖 `consistent` 同样有害）。

## 2. 协议（令牌捕获 / 失效 / 提交校验）

- **令牌捕获（取证前）**：物化待复核项的同一读取捕获 `(reverify_generation, state, sha256(evidence_version_domain))`：
  - 单票入口：`readPendingDiscrepancySQL`（`ticketverify.go`）；
  - 历史 sweep：`readClosedDiscrepancySQL` / `listReverify*CandidatesSQL`（`reverify.go`，经 `materializeClosedDiscrepancy` 投影）。
- **有界取证（事务外）**：不变；证据重读不持有事务/行锁，由 slice 预算 + 时长/尝试上限约束。
- **提交校验（共同锁内）**：`reverify.go persistReverifyOutcomeTx`（单票入口与 sweep 的唯一共享写入器）在 `SELECT … FOR UPDATE`（与 close/生命周期同一把 discrepancy 行锁）下重读 `(state, reverify_generation, evidence_version_domain)` 并逐项校验；不符 → 丢弃：
  - 只写 1 条 `recon_audit`（`action=reverify`，`result=discarded`，含 captured/observed 代次与状态、`evidence_changed`）；
  - 不写裁决行（不插替代性 `unknown`）、不增删 gap、不推进游标、不计 rechecked/裁决计数。
- **令牌失效（全部相关写入路径）**：
  - 被接受的裁决写入：`persistReverifyOutcomeTx` 在同一事务内 `reverify_generation = reverify_generation + 1`（单票入口 `ticketverify.go`、历史 sweep `reverify.go` 均经此）；
  - 失效/状态写入：`lifecycle.go updateDiscrepancySQL`（claim/dispose/失效/重开/close，含 `TransitionDiscrepancy`）与 `scan.go invalidateTxAggregateSQL`（扫描聚合证据替换）同事务推进代次；
  - `close readLatestReverifyTx`：仍在票行锁内读最新行（latest-row-wins）；过期写入已被拒绝，故最新行只会是被接受（已序列化）的结论；`created_at` 相同时以 `reverify_id`（提交序）裁决。
- **迁移**：`migrations/000018_reverify_generation.sql`（纯增列 `discrepancy.reverify_generation BIGINT NOT NULL DEFAULT 0`，真实编号顺延，未改写任何已应用迁移）。
- **gap 删除条件**：只有被接受的 `consistent` 才删除**该次复核精确覆盖位置**（task + position + reason=`query_failed`/`freshness_hold`）的 gap；拒绝路径不触任何 gap。跨 discrepancy 同位置 gap 属 000016 既有的“位置/区间”粒度语义（成功消除对应 gap 行），本修复不扩大。

## 3. 负对照（旧实现会失败）

- 工作树：`/tmp/opencode/tx-baseline`（`git worktree add --detach … 034b488`），仅拷入基线兼容测试文件 `internal/reconciliation/reverify_write_order_integration_test.go`（只使用基线 API + SQL）。
- 运行：`go test -tags integration -run TestIntegrationReverifyWriteOrder ./internal/reconciliation/ -count=1`
- 结果：**6 FAIL / 1 PASS**（唯一 PASS 为同时间戳回归守卫，两版都应通过）：

```text
--- FAIL: …TicketStaleConsistentCannotOverwriteNewerUnknown   (A result … Discarded:false, want discarded)
--- FAIL: …TicketConcurrentSameStateExactlyOneWins           (discarded 0 accepted 2, want 1/1)
--- FAIL: …TicketStaleConsistentVsNewerConsistent            (A result … Discarded:false)
--- FAIL: …SweepStaleConsistentCannotOverwriteNewerDivergent (latest verdict = "consistent", want divergent)
--- FAIL: …SweepConcurrentSameStateExactlyOneWins            (winners 2 discarded 0, want 1/1)
--- FAIL: …SweepDiscardBlocksCursorPastUnrevalidatedItem     (rechecked = 2, want 1)
--- PASS: …CloseLatestSameTimestamp
FAIL  github.com/xtianxx/txharbor/internal/reconciliation
```

- 基线症状探针（仅存在于基线工作树，断言四类症状并 PASS）：见 §1 输出。

## 4. 修后结果（真实 PG + channel 屏障，无 sleep）

`internal/reconciliation/reverify_write_order_integration_test.go`（基线兼容负对照文件，修后 7/7 PASS）+ `reverify_write_order_protocol_integration_test.go`（修后新字段断言）+ `reverify_token_test.go`（纯函数）：

| 测试 | 断言 |
|---|---|
| TicketStaleConsistentCannotOverwriteNewerUnknown | A 丢弃+审计；B 的 unknown 与 gap 保留；reverify 行仅 1 条；close 拒绝 |
| TicketConcurrentSameStateExactlyOneWins | 同初始状态并发取证恰 1 成功 1 丢弃；仅 1 条裁决行；幸存 consistent 仍可 close |
| TicketStaleConsistentVsNewerConsistent | 旧 consistent vs 新 consistent 同样被拒；重新取新令牌后合法 consistent 接受并可 close |
| SweepStaleConsistentCannotOverwriteNewerDivergent | B 的 divergent/失效保留；B 的 gap 保留；A rechecked=0、游标未动、`VerifiedComplete=false`、close 拒绝 |
| SweepConcurrentSameStateExactlyOneWins | 并发 slice 恰 1 成功 1 丢弃；仅 1 条裁决行；状态不变 |
| SweepDiscardBlocksCursorPastUnrevalidatedItem | 丢弃项之后的接受项不得推进游标；`history_sweep_through` 未持久化；丢弃项无替代裁决 |
| CloseLatestSameTimestamp | 同 `created_at` 下 close 取提交序最新（divergent）并拒绝 |
| ProtocolCounters | `sweep.Discarded=1`、warnings 可见、`VerifiedComplete=false`；ticket `DiscardReason` 指名 `generation advanced` |

真实 PG 集成（`TestIntegrationReverifyWriteOrder` 修后）：

```text
--- PASS: TestIntegrationReverifyWriteOrderTicketStaleConsistentCannotOverwriteNewerUnknown
--- PASS: TestIntegrationReverifyWriteOrderTicketConcurrentSameStateExactlyOneWins
--- PASS: TestIntegrationReverifyWriteOrderTicketStaleConsistentVsNewerConsistent
--- PASS: TestIntegrationReverifyWriteOrderSweepStaleConsistentCannotOverwriteNewerDivergent
--- PASS: TestIntegrationReverifyWriteOrderSweepConcurrentSameStateExactlyOneWins
--- PASS: TestIntegrationReverifyWriteOrderSweepDiscardBlocksCursorPastUnrevalidatedItem
--- PASS: TestIntegrationReverifyWriteOrderCloseLatestSameTimestamp
--- PASS: TestIntegrationReverifyWriteOrderProtocolCounters
ok  github.com/xtianxx/txharbor/internal/reconciliation
```

- 离态丢弃、close 锁内最新读、并发 close 恰一成功：既有 `TestIntegrationTicketVerifyDiscardsWhenStateChangesDuringRead`、`TestIntegrationCloseReadsLatestReverifyUnderRowLock`、`change_expiry_revocation_concurrency` 全量通过（未改动其语义；并发复核被取代改为可观察 no-op：stdout `discarded=true`，exit 0，审计 `discarded`）。
- 历史 sweep 与单票入口遵守同一协议（同一共享写入器 + 同一把行锁），正反例见上表。

## 5. 验证运行记录（2026-09-27，本机；Docker 可用）

| 检查 | 命令 | 结果 |
|---|---|---|
| gofmt | `gofmt -l .`（排除 `.evidence`/`.omo`） | 无输出 |
| build | `go build ./...` | PASS |
| vet | `go vet ./...` + `go vet -tags integration ./...` | PASS |
| unit | `go test ./... -count=1` | PASS |
| contract | `go test -tags contract ./... -count=1` | PASS |
| unit + race | `go test -race ./internal/reconciliation/ ./internal/app/reconcileadmin/ ./internal/db/ -count=1` | PASS |
| integration（reconciliation 全量） | `go test -tags integration -run TestIntegration ./internal/reconciliation/ -count=1` | PASS（95.7s） |
| integration（reconcileadmin 全量） | `go test -tags integration -run TestIntegration ./internal/app/reconcileadmin/ -count=1` | PASS（37.2s） |
| integration + race（写序协议全组） | `go test -race -tags integration -run TestIntegrationReverifyWriteOrder ./internal/reconciliation/ -count=1` | PASS（28.7s） |
| integration + race（ticket/close 守卫） | `go test -race -tags integration -run 'TestIntegrationReverifyWriteOrder\|TestIntegrationTicketVerify\|TestIntegrationClose' …` | PASS（50.8s） |
| integration（迁移层全量，含 000018 up/down） | `go test -tags integration ./internal/db/ -count=1` | PASS（132.8s） |
| integration（signer 迁移集/历史不动） | `go test -tags integration -run TestSignerMigration ./internal/signer/ -count=1` | PASS |
| integration（txlifecycle 迁移集） | `go test -tags integration -run TestT043MigrationSetMergeOrder ./internal/txlifecycle/ -count=1` | PASS |
| integration（withdrawal 快照门） | `go test -tags integration -run TestWithdrawalRecoveryPeriodNoExecutionArtefacts ./internal/withdrawal/ -count=1` | PASS |
| integration（events 期望载体） | `go test -tags integration -run TestIntegrationAppend ./internal/events/ -count=1` | PASS |
| 负对照（基线 034b488） | 见 §3 | 6 FAIL / 1 PASS（预期） |

迁移编号顺延同步更新（仅测试固定值，不改迁移内容）：`internal/db/withdrawal_execution_migration_integration_test.go`、`event_infrastructure_migration_integration_test.go`、`scratch_009_overlay_integration_test.go`、`intent_fk_repair_integration_test.go`、`internal/signer/migration_integration_test.go`、`internal/txlifecycle/migration_joint_integration_test.go`。

## 6. NOT RUN

- `fault` / `perf` 标签套件：本轮未改 fault/perf 文件，独立通道不跑，不记 pass。
- 其余未列出的整仓 integration 包（如 indexer/nonce 等）：未改其路径；本轮按“定向 PG 集成”执行已列包，不声称全仓 integration 通过。
- quickstart 全矩阵重跑（T031）：本轮未改扫描语义，沿用既有证据 `quickstart_matrix_evidence.md`。

## 7. 剩余限制（保守、显式）

1. 协议保护的是“同一 discrepancy 行上的复核/失效/状态写入”；`discrepancy_occurrence` 纯追加（重复检出、无结论变化）不推进代次（Q5：无关写入不失效），这是既有裁决，不属本缺陷面。
2. gap 行无 `discrepancy_id`，删除按（task + 位置 + reason）粒度（000016 设计）；同位置跨 discrepancy 的归属歧义保持原状，不因本修复扩大。
3. 被取代的复核在 CLI 上为可观察 no-op（exit 0 + `discarded=true` + stderr 说明 + 审计）；若运维需要“失败”语义需另行裁决，不在本轮放宽任何门禁。
4. 阈值/预算仍为部署参数，无默认生产阈值声明。
