# Implementation Plan: 015 Backup Recovery and Safe Service Resumption

**Branch**: `015-backup-recovery-safe-resumption` | **Date**: 2026-09-28 | **Spec**: [spec.md](spec.md) | **Research**: [research.md](research.md) | **Data model**: [data-model.md](data-model.md) | **Quickstart**: [quickstart.md](quickstart.md) | **Contracts**: [contracts/](contracts/) | **ADR**: [ADR-001](adr/ADR-001-recovery-control-store.md), [ADR-002](adr/ADR-002-backup-carrier-and-recovery-point.md)

**Input**: Feature specification from `/specs/015-backup-recovery-safe-resumption/spec.md` (Clarifications Session 2026-09-28, Q1–Q3 rulings on FR-036/FR-023/FR-019).

**Main Baseline**: `8d4b9af` (main merge; branch HEAD `03e5d1e`); plan docs only — no product code, no migration, no CI change, no push/PR/merge this round.

**Gate status**: **T000-P stays OPEN** — this round is not a release and makes no production-readiness claim; evidence in this directory is local-scope only. **Risk-accept forced resumption / loss write-off / manual compensation payment / automatic intent re-creation are explicitly absent**: not approved, not designed, no contract row, no table, no task; if ever required it is a business blocker (see Constraints and [research.md](research.md) §10).

## Summary

Build backend-only backup/recovery and safe resumption for TxHarbor. Three chosen mechanisms, all plan-only this round:

1. **Backup carrier** = logical `pg_dump --format=custom` from a repeatable-read exported snapshot + external **manifest** as the only backup identity/selection source; recovery point = snapshot tuple (`pg_current_snapshot`/WAL LSN upper bound/wall clock), never business-table max timestamp; "backup success ≠ restore success" — `verified` requires an actual isolated `pg_restore` + structural/constraint/compatibility/probe checks ([ADR-002](adr/ADR-002-backup-carrier-and-recovery-point.md), [contracts/backup-manifest.md](contracts/backup-manifest.md)).
2. **Recovery control information** = a separate PostgreSQL control store (`TXHARBOR_RECOVERY_CONTROL_DSN`, independent database, never in the data-DB backup/restore set) holding recovery instances, participants/identity map, evidence, verification items, gaps, isolation checks, approvals, releases and audit; it is not financial truth, and its loss is fail-closed. The data DB gets **zero schema changes** this phase ([ADR-001](adr/ADR-001-recovery-control-store.md), [data-model.md](data-model.md)).
3. **Resumption gate** = `internal/recovery.Gate` wired at every real entry point of the 7 capabilities (query/chain scan/deposit confirmation/existing withdrawal recovery/new withdrawal creation/event publishing/event consuming): normal mode passes through unchanged; while a recovery instance is open the wired entries **deny by default**, and a capability is released only by a derived evaluation (instance + evidence generation/hash + isolation checklist + no open gap + valid single/dual approvals + capability dependencies + unchanged existing fund gates), with bounded TTL and fail-closed on control-store unavailability ([contracts/resumption-gate.md](contracts/resumption-gate.md), [contracts/approval-matrix.md](contracts/approval-matrix.md)).

Out of scope this round: any implementation, tasks, migrations, CI edits, deployment; production RPO/RTO/frequency/retention numbers; K8s/auto failover/multi-region; backup vendor/PITR; risk-accept/kill-loss/compensation tooling.

## Technical Context

**Language/Version**: Go 1.26.5 (`go.mod`)

**Primary Dependencies**: PostgreSQL 18.6-trixie (`compose.yaml:19`; `pg_dump/pg_restore` from the same pinned image), `pgx/v5 v5.11.0`, `goose/v3 v3.28.0` (data DB migrations via `internal/db`; control store schema versioned separately), franz-go (event layer only), Redis (non-authoritative, event layer only), Anvil + testcontainers for drills

**Storage**: 数据 DB = 唯一权威，本阶段 **零 schema 变更**；新增**独立恢复控制库**（独立 DSN、独立保留域、不进数据备份集）；备份产物 + manifest 落部署配置目录；Redis/Kafka 永不成为金融真相

**Testing**: `go test ./...` + build tags `integration`（PG，含真实小规模 `pg_dump→pg_restore`）/`integration_redis`/`integration_kafka`/`contract`/`e2e`/`fault`/`perf` + 新增 `drill`（完整灾备演练，独立通道，不进普通 PR）；Docker 缺位记 NOT RUN 不记 pass

**Target Platform**: Linux server, single binary `txharbor` with subcommands (`cmd/txharbor/main.go`); 不新增常驻服务/监听/UI/调度器

**Project Type**: modular monorepo backend service + operator CLI

**Performance Goals**: 所有恢复/核验/放行为有界前台命令（可重复调用收敛，无 daemon）；门禁缓存 TTL 与核验批次有界；演练度量分开记录；本地测试值不得直宣生产阈值

**Constraints**: PG 唯一真源；私钥只在 Signer 边界（备份/证据/日志禁含）；不引入 K8s/自动主备/多地域/具体备份产品与厂商/PITR；继承 000018 禁混跑检查清单模式；FR-023 仅适用 015 灾备复服，不改变日常运行与 014 已批权限；T000-P 保持 OPEN；本地演练数值仅测试输入

**Scale/Scope**: 单机后端先行；控制库拓扑（同实例独立 database vs 独立实例）为部署决策，不宣称跨主机协调；多地域/主备切换 out of scope

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| 原则 | Phase 0 前 | Phase 1 后复核 | 设计落点 |
|---|---|---|---|
| I 资金正确性 | PASS | PASS — 恢复/核验/放行均 fail-closed；禁重复付款/错误事件效果；控制库不可达拒绝 | [contracts/verification-items.md](contracts/verification-items.md) §1、[data-model.md](data-model.md) §3 |
| II 幂等 | PASS | PASS — `operation_id` UNIQUE 读回、append-only 决策、代次令牌、重建目标幂等 | [data-model.md](data-model.md) §5/§6 |
| III PG 权威 | PASS | PASS — 数据 DB 唯一金融真源；控制库只承载治理事实（PG，非文件）；Redis/Kafka 非真相 | [ADR-001](adr/ADR-001-recovery-control-store.md) |
| IV Reorg 感知 | PASS | PASS — V1/V4 复用 002–006/014 链身份与版本捕获，孤块/不确定保持 unknown | [contracts/verification-items.md](contracts/verification-items.md) §1 |
| V 显式状态机 | PASS | PASS — 实例/能力/核验项/缺口/隔离项/批准/放行显式状态与非法跳转拒绝 | [data-model.md](data-model.md) §4 |
| VI 事务边界 | PASS | PASS — 控制库内结果+代次同事务；restore 前置检查显式；不新增跨系统原子假设 | [data-model.md](data-model.md) §5/§7 |
| VII Nonce 并发 | PASS | PASS — 不碰分配；V3 只读观察，绝不自动重分配 | [contracts/verification-items.md](contracts/verification-items.md) §1 |
| VIII 私钥隔离 | PASS | PASS — 备份/证据/日志禁密钥；恢复只验 Signer 边界可达性，不接触私钥 | [contracts/backup-manifest.md](contracts/backup-manifest.md) §1 |
| IX 失败路径 | PASS | PASS — 7 类失败注入 + 拒绝分类闭集 + 有界重试/丢弃审计 | [quickstart.md](quickstart.md) §2 |
| X 确定性本地测试 | PASS | PASS — 本地 PG/Anvil/中间件 + pinned 镜像真实恢复演练；不依赖公网 testnet | [quickstart.md](quickstart.md) §3 |
| XI 测不变量 | PASS | PASS — 关键不变量走真实 PG/控制库/恢复流程；替身只限纯逻辑单测 | [data-model.md](data-model.md) §9、[quickstart.md](quickstart.md) §4 |
| XII 可观测 | PASS | PASS — 放行/拒绝/缺口/度量低基数指标与诚实状态面 | [quickstart.md](quickstart.md) §1 S10/S12 |
| XIII 简单优先 | PASS | PASS — 无新服务/无 K8s；核心库 + 薄 CLI + 一个控制 schema；控制库经 ADR 论证 | [ADR-001](adr/ADR-001-recovery-control-store.md) |
| XIV 小步规格驱动 | PASS | PASS — 本轮仅 plan 产物；不生成 tasks、不改产品代码/迁移/CI | 本文件 |

- **Pre-Phase 0 结论：PASS**——无违例、无例外需记录。
- **Post-Phase 1 复核结论：PASS**——设计未引入新违例；"独立控制库"非宪章违例而是经 [ADR-001](adr/ADR-001-recovery-control-store.md) 论证的必要边界（回滚域独立），Complexity Tracking 无需记录。

## Project Structure

### Documentation (this feature)

```text
specs/015-backup-recovery-safe-resumption/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   ├── backup-manifest.md
│   ├── resumption-gate.md
│   ├── approval-matrix.md
│   └── verification-items.md
├── adr/
│   ├── ADR-001-recovery-control-store.md
│   └── ADR-002-backup-carrier-and-recovery-point.md
└── checklists/
    └── requirements.md  # 16/16（clarify 轮已裁决）
```

### Source Code (repository root, plan only — not implemented this round)

```text
internal/
├── recovery/                     # NEW core library (plan only)
│   ├── manifest.go               # backup manifest model/validate/select; recovery point
│   ├── gate.go                   # capability gate: derived evaluation + bounded TTL cache
│   ├── capabilities.go           # 7-capability closed set + dependency matrix
│   ├── verification.go           # V1–V9 read-only verification orchestration (reuse 006/013/014 read primitives)
│   ├── gaps.go                   # evidence bundle + independence/dependency proof
│   ├── approvals.go              # approval validity: dual/single, identity map, executor exclusion
│   ├── generation.go             # evidence generation token protocol (014 §3.1 同形)
│   └── controlstore/             # control DB store + schema (separate goose FS)
│       └── schema/               # control schema only; migrations/ (data DB) untouched
├── app/
│   └── recoveryadmin/            # NEW thin operator CLI: backup/verify-backup/restore/instance/
│                                 # checklist/verify/approve/release/status/drill (plan only)
├── app/serve.go                  # WIRE: query / chain_scan / deposit_confirmation / new_withdrawal_creation checkpoints
├── app/withdrawalworker.go       # WIRE: existing_withdrawal_recovery checkpoint
├── app/withdrawalexec.go         # WIRE: operator write path checkpoint
├── app/eventpublisher.go         # WIRE: event_publishing checkpoint
├── app/eventconsumer.go          # WIRE: event_consuming checkpoint
├── app/signer*.go, internal/signer/  # WIRE: signing delivery under existing_withdrawal_recovery dependency
├── indexer/ events/ execution/ txlifecycle/ nonce/ reconciliation/  # REUSE read-only or via their own authorized entries
├── metrics/                      # REUSE registry + bounded low-cardinality recovery series
└── faultdrill/ perf/             # REUSE patterns; drill additions behind new `drill` tag
```

**Structure Decision**: 单体 monorepo 内新增 `internal/recovery` 核心库（可测的 manifest/gate/verification/approval 纯逻辑与存储）+ 薄 `recovery-admin` 命令；**不新增服务边界、监听、daemon 或数据 DB 迁移**。7 类能力入口只做"动作前门禁接线 + 状态暴露"，其原有门禁与状态机一律保留（[contracts/resumption-gate.md](contracts/resumption-gate.md) §1）。

## 真实入口接线清单（与 7 类能力一一对应）

| # | 能力 | 真实入口（file:line） | 依赖 | 恢复环境默认 | 门禁检查点 |
|---|---|---|---|---|---|
| 1 | query | `internal/app/withdrawalhttp.go:227`（+`serve.go:473-474`）、执行读 `serve.go:479-480`、nonce 读 `serve.go:488` | — | 拒绝 | HTTP 准入前 |
| 2 | chain_scan | `serve.go:319/331`（`:615 runServiceStreams`；lease/fencing `internal/indexer/lease.go:56-67`） | — | 关闭 | 启动/每轮步进前 |
| 3 | deposit_confirmation | `serve.go:350-353`、`:380-398`、`:411`（006 recovery loop） | chain_scan | 关闭 | 同上 |
| 4 | existing_withdrawal_recovery | `internal/app/withdrawalworker.go:318/349/566`；`withdrawalexec.go:35`；下游 `signer-serve`（`internal/signer/gates.go:123/162`） | chain_scan | 关闭 | 认领/操作/交付前 |
| 5 | new_withdrawal_creation | `withdrawalhttp.go:148`（`serve.go:473` guardRoute+CapacityGate） | 4 | 拒绝 | 处理器准入前 |
| 6 | event_publishing | `eventpublisher.go:41` → `internal/events/publisher.go:177` | chain_scan | 关闭 | claim/settle 前 |
| 7 | event_consuming | `eventconsumer.go:31` → `internal/events/consumer.go:351`（Effect：`internal/cache/invalidator.go:120`；参考账本仅证据） | — | 关闭 | Effect/进度前 |

恢复/核验工具自身的写入边界与禁止项见 [contracts/resumption-gate.md](contracts/resumption-gate.md) §1 末两条。

## FR / SC / 澄清裁决 → 设计与验收映射

### FR-001–036

| FR | 设计落点 | 验收落点 |
|---|---|---|
| FR-001 备份身份/元数据/选择规则 | [contracts/backup-manifest.md](contracts/backup-manifest.md) §1–2、[data-model.md](data-model.md) §2 | [quickstart.md](quickstart.md) S1/S2 |
| FR-002 覆盖范围声明 | backup-manifest §1 `coverage` | S1 |
| FR-003 完整性可验证 | backup-manifest §2 | F1 |
| FR-004 schema/程序兼容 | backup-manifest §2、[data-model.md](data-model.md) §7 | F3 |
| FR-005 恢复依赖检查 | data-model §7、backup-manifest §2 | S4、F1/F3 |
| FR-006 实际恢复验证 | backup-manifest §3 | S2 |
| FR-007 私钥/凭据不入备份 | backup-manifest §1（排除声明）、data-model §7 | S1/S4 |
| FR-008 中断/部分完成 | data-model §7 | F2 |
| FR-009 默认隔离 | [contracts/resumption-gate.md](contracts/resumption-gate.md) §2、[ADR-001](adr/ADR-001-recovery-control-store.md) | S3/S8 负例 |
| FR-010 旧实例停止可验证 | resumption-gate §3 | S5、F5 |
| FR-011 禁混跑（000018 模式） | resumption-gate §3 | F5 |
| FR-012 覆盖全部外部效果路径 | resumption-gate §1/§4 | S8/S9 负例 |
| FR-013 既有约束/禁重放历史签名 | resumption-gate §4、[contracts/approval-matrix.md](contracts/approval-matrix.md) §5 | F7、S9 |
| FR-014 核验类别与记录 | [contracts/verification-items.md](contracts/verification-items.md) §1 | S6 |
| FR-015 DB 回退≠外部回退 | verification-items §1 | F4 |
| FR-016 缺失≠从未发生 | verification-items §1 | F6 |
| FR-017 不重建付款意图 | verification-items §1 | F6 |
| FR-018 unknown 不当通过 | verification-items §1 | S6/F6 |
| FR-019 缺口证据包/独立性/暂停 | verification-items §2、data-model §1.6/§3 | S7 |
| FR-020 与既有能力衔接不绕门禁 | verification-items §1、[research.md](research.md) §1 | S6 |
| FR-021 分级且无总开关 | resumption-gate §1–2 | S8/S9 |
| FR-022 restored/verified/approved | data-model §4.2、resumption-gate §2 | S10 |
| FR-023 审批规则（2026-09-28） | approval-matrix §1–3 | S8/S9/F7 |
| FR-024 幂等/可重入/撤销显式 | approval-matrix §4、data-model §6 | S12 幂等段 |
| FR-025 既有资金门禁继续有效 | resumption-gate §4、data-model §3 | S9 |
| FR-026 诚实报告状态 | resumption-gate §2 | S10 |
| FR-027 事件重复/offset 回退/幂等缺失 | verification-items §3 | F4、事件层集成 |
| FR-028 不承诺跨系统恰好一次 | verification-items §3 | [quickstart.md](quickstart.md) §5 |
| FR-029 未接回执不宣称外部一致 | verification-items §3 | S10/§5 |
| FR-030 隔离环境可复现演练 | quickstart §1/§3 | S12 |
| FR-031 时间口径分开/指标 | data-model §1.11、backup-manifest §4 | S12 |
| FR-032 失败路径 fail-closed | quickstart §2 | F1–F7 |
| FR-033 快速检查 vs 独立演练 | quickstart §3 | 分层表 |
| FR-034 PG 唯一真源 | data-model 存储分层、resumption-gate §4 | S9 |
| FR-035 不含 K8s/主备/厂商；T000-P | 本文件 Constraints/待裁决、ADR-001/002 | §5 非声明 |
| FR-036 可配置目标与测量口径 | backup-manifest §4、[research.md](research.md) §9、data-model §1.11 | S12 |

### SC-001–008

| SC | 设计落点 | 验收落点 |
|---|---|---|
| SC-001 备份验证通过率/0 误判 | backup-manifest §3 | S1/S2 + F1 |
| SC-002 外部领先场景：缺口识别/0 重复付款/独立放行留痕 | verification-items §1–2、resumption-gate §2 | F4、S7、S8 |
| SC-003 隔离未证明 100% 拒绝 | resumption-gate §3 | F5 |
| SC-004 7 能力独立+审批规则+失效 | resumption-gate §1、approval-matrix §1–4 | S8/S9/F7 |
| SC-005 端到端演练+分列口径+RTO 超时处理 | quickstart §1/§3、data-model §1.11 | S12 |
| SC-006 7 类失败注入 0 错误开放 | quickstart §2 | F1–F7 |
| SC-007 重复执行 10 次幂等 | data-model §6 | §2 幂等段 |
| SC-008 快速检查进 PR/演练独立 | quickstart §3 | 分层表 |

### 三项澄清裁决（2026-09-28）

| 裁决 | 设计落点 | 验收落点 |
|---|---|---|
| C1 FR-036：可配置目标+实测先行；RTO 超时不永久禁后续安全复服 | research §9、backup-manifest §4、data-model §1.11 | S12 |
| C2 FR-023：执行/核验/批准独立；高影响双人非执行者；批准绑定证据版本；硬门禁不可覆盖 | approval-matrix §1–3、data-model §3.1 | S8/S9/F7 |
| C3 FR-019：缺口无法补齐保留 unknown/pending+证据包+升级；仅可证明独立能力放行 | verification-items §2 | S7/F6 |

## 待测参数 · 部署前裁决 · 阻塞项

- **待测参数（实现后测量填入，不编造，不阻塞设计）**：门禁缓存 TTL、证据新鲜度容忍、核验批次上界、控制库语句超时、演练时长/备份耗时体积。
- **部署前裁决（单列，不批准、不阻塞无关设计）**：生产 RPO/RTO/备份频率/保留期（FR-036 明示留裁决）；控制库拓扑与保留；身份映射内容与维护者；真实下游 effect class 清单；单人/单机部署的非执行者批准人来源（FR-023 下无法自批）；备份产物异地/落盘策略。
- **阻塞项**：无设计阻塞。风险接受后强制复服、损失核销、人工补偿付款、自动补造意图明确缺席；若未来需要属业务阻塞，另行业务裁决；双人批准不得替代缺失证据。

## Complexity Tracking

> 无宪章违反；无例外需记录。独立控制库的取舍理由见 [ADR-001](adr/ADR-001-recovery-control-store.md)，属必要边界而非违例豁免。
