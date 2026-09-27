# Implementation Plan: 014 Reconciliation and Exception Handling

**Branch**: `014-reconciliation-exception-handling` | **Date**: 2026-09-26 | **Spec**: [spec.md](spec.md) | **Research**: [research.md](research.md) | **Data model**: [data-model.md](data-model.md) | **Quickstart**: [quickstart.md](quickstart.md) | **Contracts**: [contracts/](contracts/) | **ADR**: [ADR-001](adr/ADR-001-carrier-and-isolation.md)

**Input**: Feature specification from `/specs/014-reconciliation-exception-handling/spec.md` (Clarifications Session 2026-09-26, Q1–Q5)

**Main Baseline**: `0a913e6` (ancestor of HEAD `6fd6410`); plan docs only, no product code/CI changes in this round.

**Gate status**: **T000-P stays OPEN** — this round is not a release and makes no production-readiness claim; evidence in this directory is local-scope only. **Risk-accept/ignore is explicitly absent**: not approved, not designed, no contract row, no migration table, no task; if ever required it is a business blocker (see Constraints and [research.md](research.md) §7).

## Summary

Build backend-only reconciliation for TxHarbor: scoped, checkpointed, resumable scan tasks comparing chain facts / PG business state / event delivery-consumer results; stable discrepancy identity with dedup, evidence, machine classification, alert-only default; operator claim/dispose/verify-close with least-privilege matrix and audit; reuse of verified read-only primitives (indexer observation, `txlifecycle.Reconcile/UnknownRecovery`, execution gates, outbox/publisher/consumer progress, `ReconciliationAudit`, quarantine replay/unblock, existing operator CLIs) without auto-triggering recovery, replay, or payments. New persistence is limited to 014 task/scan-attempt/checkpoint/gap/discrepancy/permission/audit records in a new migration `000016` (actual max today is `000015`; `000014` is occupied by intent-FK repair). Permission evaluation source is the dedicated `recon_permission` table (default-deny, no grants seeded; existing 011/009/012 permissions are not expanded — see [contracts/auth-matrix.md](contracts/auth-matrix.md) Evaluation Source). Risk-accept/ignore policy is NOT approved, explicitly absent from this design, and NOT built; if required it is a business blocker.

## Technical Context

**Language/Version**: Go 1.26.5 (`go.mod`)

**Primary Dependencies**: PostgreSQL (authoritative), franz-go Kafka client (existing `KafkaSink`/consumer), Redis (rate-limit/cache only, never financial truth), Anvil + local PG/Redis/Kafka/Docker Compose for deterministic tests

**Storage**: PostgreSQL唯一权威；新增 `000016` 承载 014 任务/检查点/差异/认领/处置/复核/审计记录；Redis/Kafka 不新增权威状态

**Testing**: `go test ./...` + build tags `integration` (PG), `integration_redis`, `integration_kafka`, `contract`, `e2e`, `fault`, `perf`; 普通 PR 必跑 lint/build/unit(+race)/contract/`ci-required`；PG/Redis/Kafka/e2e 按 `ci.yml` 路径分类触发（Docker 缺位记 NOT RUN + `ci:integration-pending`）；`fault`/`perf` 常驻 `fault-perf.yml`（schedule/dispatch），不进普通 PR 闸门

**Target Platform**: Linux server, single binary `txharbor` with subcommands (`cmd/txharbor/main.go`)

**Project Type**: modular monorepo backend service + operator CLI (no new service boundary in this phase)

**Performance Goals**: 扫描有界（并发/单次范围/时长配额）；暂停响应、资源上限、资金流程影响可测；本地测试值不得直宣生产阈值（Q3-6）

**Constraints**: Q1 有界只读复核；Q2 分层最小权限；FR-015 差异≠重付；FR-020 门禁继承；FR-021 无余额账本；FR-022 后端先行；T000-P 保持 OPEN（本轮不发布、不宣称生产就绪）；风险接受/忽略明确缺席——未批准、无设计、无权限行、无迁移表、无任务，需另行业务裁决（见 [contracts/auth-matrix.md](contracts/auth-matrix.md) 与 [research.md](research.md) §7）

**Scale/Scope**: 单机后端先行；多主机/灾备/完整控制台明确 out of scope

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design: PASS (no violations; no complexity-tracking entries).*

- I 资金正确性：只读复核默认只告警；改写事实/付款须操作员显式触发已批准能力；缺证据不判一致（FR-004/005/006/014/018）。
- II 幂等：差异身份去重（FR-007）、处置幂等（FR-016）、复用 `consumer_inbox`/`consumer_versions`/operation_id/`tx_reconciliations` append-only 语义；Q4 仅业务分歧建单。
- III PG 权威：新表仅承载 014 任务/工单/审计，不建余额账本（FR-021）；Redis/Kafka 不新增真相。
- IV Reorg 感知：孤块结论可撤销/转待复核（FR-017）；闭合仅对记录范围区块身份版本时点有效（Q5）。
- V 显式状态机：FR-010 生命周期非法跳转拒绝；处置完成≠复核一致≠验证闭合（Q5）。
- VI 事务边界：结果与检查点同库事务一致性（见 [data-model.md](data-model.md) §5）；dual-write 仍走 Outbox 既有机制，014 不新增跨系统原子假设。
- VII Nonce：不碰分配；仅复用 `nonce/reconcile.go` 观察语义。
- VIII 私钥隔离：014 无签名路径；复用 009 policy/gates 只读。
- IX 失败路径：重查退避/取消/预算耗尽/中断恢复均为一等行为（FR-003/019， Q3）。
- X/XI 确定性本地测试 + 测不变量：[quickstart.md](quickstart.md) 映射 FR/SC/Q1–Q5 到 fault/integration 场景；未知/重组/重复/中断/并发/越权/超预算全覆盖。
- XII 可观测：复用 `metrics/events.go` + reorg/outbox/consumer lag 口径，新增仅低基数枚举标签（FR-24/27 红线）。
- XIII 简单优先：不新建服务边界；核心库 + 薄 admin 命令（ADR-001）。
- XIV 小步规格驱动：本轮仅 plan 产物，不生成 tasks，不改产品代码/CI。

## Project Structure

### Documentation (this feature)

```text
specs/014-reconciliation-exception-handling/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   ├── task-lifecycle.md
│   ├── discrepancy-lifecycle.md
│   └── auth-matrix.md
├── adr/
│   └── ADR-001-carrier-and-isolation.md
└── checklists/
    └── requirements.md  # 16/16 (clarify 轮已裁决)
```

### Source Code (repository root)

```text
internal/
├── reconciliation/        # NEW (plan only, not implemented this round)
│   ├── scan.go            # ScanOnce core: scope → checkpointed read-only compare
│   ├── identity.go        # stable identity + evidence hash (plan §3)
│   ├── classify.go        # machine classification incl. business-divergence-only duplicates
│   ├── lifecycle.go       # claim/dispose/reverify/close transitions + invalidation
│   └── budget.go          # concurrency/range/time quotas, backoff, cancel
├── app/
│   └── reconcileadmin.go  # NEW thin operator command (plan only): start/pause/claim/dispose/close/show
├── indexer/               # REUSE read-only: scanner/logscanner/confirmscan/reorg/RecoverySnapshot
├── txlifecycle/           # REUSE: Reconcile/UnknownRecovery/gates snapshots (no auto-send)
├── execution/             # REUSE gates/claim/projection reads (no claim writes from 014)
├── events/                # REUSE: outbox/publisher progress/consumer ReadProgress/ReconciliationAudit/quarantine reads; replay/unblock only via existing CLIs
└── metrics/               # REUSE registry + bounded new low-cardinality series

migrations/
└── 000016_reconciliation_handling.sql  # NEW (plan only): 014 tasks/checkpoints/discrepancies/claims/dispositions/reverify/audit
```

**Structure Decision**: 单体 monorepo 内新增 `internal/reconciliation` 核心库（可测 ScanOnce）+ 薄 `reconcile-admin` 命令；serve/worker 本阶段不自动启动 014 扫描（Q3 只锁验收要求，形态取舍见 [ADR-001](adr/ADR-001-carrier-and-isolation.md)）；真实复用入口清单见 [research.md](research.md) §1。

## Complexity Tracking

> 无宪章违反，无需记录。
