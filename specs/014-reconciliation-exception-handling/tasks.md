# Tasks: 014 Reconciliation and Exception Handling

**Input**: Design documents from `/specs/014-reconciliation-exception-handling/` (spec.md + Q1–Q5, plan.md, research.md, data-model.md, contracts/, ADR-001, quickstart.md, requirements.md 16/16)

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Included because spec acceptance scenarios + quickstart validation + constitution X/XI require integration/fault coverage for fund-safety paths. Ordinary PR CI stays layered; heavy runs stay on independent tags.

**Organization**: Tasks grouped by user story for independent implementation and testing. All boxes unchecked. No work started.

## Pre-check: Continuous Execution Mechanism (approved plan; not redesigned here)

- Executor: operator / external scheduler invoking the thin `reconcile-admin` command repeatedly (plan.md Structure Decision; ADR-001: core `ScanOnce` + thin admin command; daemon deferred; no auto-start in serve/worker).
- Trigger: explicit `start`/`resume` creating/activating a task row; each `scan` invocation claims the next budgeted interval (contracts/task-lifecycle.md Operations).
- Repeat: idempotent re-invocation advances `result_persisted_through` + checkpoint rows in the same DB transaction (data-model.md §5); crash resumes from pointer, covered ranges not re-reported, uncovered ranges continue.
- Pause/resume/restart: `pause`/`resume`/`cancel` task-state transitions scoped to 014 only (Q3; task-lifecycle Integrity Rules); restart reuses checkpoint pointer; in-flight work completes or cancels boundedly.
- “不自动启动” means no background auto-start in serve/worker; it does not mean every round is interactive — T028 wires the production entry (binary construction, config passing, lifecycle, documented call path) so repeated invocation is a first-class, budgeted, pausable operation. Continuous-reconcile requirement is not weakened.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Plan-only scaffolding references and CI/layering guardrails (no product code changes in this tasks round; tasks below are implementation work for later phases)

- [X] T001 Create `internal/reconciliation/` package skeleton per plan.md Source Code layout in `internal/reconciliation/doc.go`
- [X] T002 [P] Draft `migrations/000016_reconciliation_handling.sql` skeleton covering `recon_task` (incl. `upstream_receipt_source` per `data-model.md` §1.1)/`recon_checkpoint`/`recon_gap`/`discrepancy`/`discrepancy_occurrence`/`disposition`/`reverify`/`recon_audit`/`recon_scan_attempt`/`recon_permission` per `data-model.md` §1 (constraints: PK/FK/UNIQUE/CHECK/ENUMs, integer/NUMERIC money rule, append-only audit)
- [X] T003 [P] Add the tags×suites validation matrix table to the Layering section of `specs/014-reconciliation-exception-handling/quickstart.md` (rows: ordinary-PR tags vs independent `fault`/`perf` runs with Docker-absent NOT RUN discipline; no new ordinary-PR long tests)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Migration + shared state machines, identity, budget, auth/audit primitives that all stories depend on

**⚠️ CRITICAL**: No user story work can begin until this phase is complete. Merge responsibility: migration + state-machine + auth-matrix owner merges first; all stories rebase on it.

- [X] T004 Implement `migrations/000016_reconciliation_handling.sql` with all tables/constraints/indexes per `data-model.md` §1–2 — explicitly including `recon_scan_attempt` (§1.9, partial unique on `claimed`) and `recon_permission` (§1.10, default-deny, zero grants seeded) — (verify with `internal/db/migrate.go` compatibility; `goose_db_version` clean)
- [X] T005 [P] Implement stable identity + evidence-hash in `internal/reconciliation/identity.go` per `data-model.md` §2 and research §3 (key order fixed; unknown shapes conservative; Q4 business-divergence rule enforced at classifier boundary)
- [X] T006 [P] Implement task/checkpoint/gap state machine in `internal/reconciliation/scan.go` skeleton per `contracts/task-lifecycle.md` and `data-model.md` §1.9/§5.1 (claim in a short txn with `SELECT recon_task … FOR UPDATE`, no RPC inside; illegal transitions refused + audited; pointer never advances past unpersisted ranges)
- [X] T007 [P] Implement discrepancy lifecycle skeleton in `internal/reconciliation/lifecycle.go` per `contracts/discrepancy-lifecycle.md` (5 states + reopen; `disposed≠reverified≠closed`; Q5 invalidation/reopen rules as transition guards)
- [X] T008 [P] Implement budget/backoff/cancel primitives in `internal/reconciliation/budget.go` per Q3/FR-019 (bounded concurrency/range/time/PG-RPC quotas; no busy loop; observable wait/suspend)
- [X] T009 [P] Implement action×permission×scope enforcement helper in `internal/reconciliation/authz.go` per `contracts/auth-matrix.md` Evaluation Source + Management and Q2 (bind `principal` from the authenticated caller identity reusing the API-key auth middleware pattern — free-text operator fields stay audit-only; evaluate exact (principal, action, scope) matches against `recon_permission`; unconfigured rows, unknown actions, and out-of-scope access are denied + audited; claim≠execute right; fields≠authorization; `operation_id` audit idempotency only, never management authorization; existing 011/009/012 permissions are never expanded)
- [X] T010 Implement atomic result+checkpoint commit helper in `internal/reconciliation/store.go` per `data-model.md` §5/§5.1 (same-tx persist ordering; pointer advances only over the contiguous persisted prefix; late submitters with superseded/abandoned attempts are discarded + audited, never overwrite the pointer; crash-safe resume; bounded in-flight settle/cancel)

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - 持续对账发现漏处理与重复交付 (Priority: P1) 🎯 MVP

**Goal**: Scoped, checkpointed, resumable three-way compare (chain facts / PG state / event delivery-consumer results) with漏处理检出 and仅业务分歧建单; incomplete scans never report definitive anomalies (FR-001–009/014/015/019/021, Q1/Q3/Q4, SC-001/002/006)

**Independent Test**: Controlled dataset: 100%漏处理 + 业务分歧检出, absorbed duplicates zero tickets, incomplete ranges zero definitive false positives (quickstart §1–3,8)

### Tests for User Story 1

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [X] T011 [P] [US1] Contract test for task lifecycle transitions in `internal/reconciliation/scan_contract_test.go` (tags: `contract`)
- [X] T012 [P] [US1] Integration test for scoped scan + checkpoint resume + budget suspend plus the upstream-unconnected negative case in `internal/reconciliation/scan_integration_test.go` (tags: `integration`; PG required; Docker-absent → NOT RUN, not pass): unconnected scope yields `incomplete`/pending only, never `consistent` and never an external-credit claim

### Implementation for User Story 1

- [X] T013 [P] [US1] Implement read-only chain-facts adapter in `internal/reconciliation/chainfacts.go` reusing `internal/indexer/scanner.go`, `logscanner.go`, `confirm.go`, `reorgmetrics.go:60 LoadRecoverySnapshot` (no auto recovery triggers; orphaned evidence → pending reverify)
- [X] T014 [P] [US1] Implement PG-state adapter in `internal/reconciliation/pgstate.go` reusing `internal/txlifecycle/reconcile.go:62`, `:183 UnknownRecovery`, `execution/gates.go` reads, `consumer` progress reads (unknown stays unknown; FR-018)
- [X] T015 [P] [US1] Implement event-delivery adapter in `internal/reconciliation/eventstate.go` reusing `internal/events/outbox.go`, `consumer.go:887/914`, `audit.go:133/158`, `quarantine.go` reads (Q4: absorbed duplicates metrics-only)
- [X] T016 [US1] Implement `ScanOnce` compare loop in `internal/reconciliation/scan.go` (depends on T013–T015; scope→budgeted intervals→classify→persist results+checkpoint same-tx; missing evidence → incomplete/pending, never consistent) (covers FR-001–006, Q1/Q3)
- [X] T017 [US1] Implement machine classifier in `internal/reconciliation/classify.go` (depends on T005; reads per-scope `upstream_receipt_source.connected` as an input; categories `missing/duplicate_divergent/state_mismatch/unknown/incomplete`; absorbed-zero-effect → no ticket + metrics; insufficient evidence → pending/incomplete; unconnected/configured-but-unavailable → `incomplete`, never `consistent`; a `connected` flag alone never proves upstream success) (covers FR-006/009, Q4)
- [X] T018 [US1] Wire `reconcile-admin scan/start/pause/resume/cancel/show` thin commands in `internal/app/reconcileadmin.go`, register the command in `cmd/txharbor/main.go`, define the `TXHARBOR_RECON_*` key names and pass them through in `internal/config/config.go` (naming owner; pass-through only), and smoke-verify a single budgeted scan via the built binary (depends on T006,T008,T016; pause scoped to 014 tasks only; bounded in-flight settle/cancel) (covers FR-001/003/019, Q3; makes the MVP operable)

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently — operable via the built binary (`reconcile-admin scan/start/pause/resume/cancel/show`, T018), not library-only

**US1 受限验收状态（2026-09-26 收口核定，不撤销 T011–T018 勾选）**：T011–T018 文本条件达成；以下已批准但无任务归属的能力由 Phase 3b（T033–T038）承接，完成前 US1 按受限范围验收——chain-first missing 结论仅 withdrawal 且在 T033 前视为待归属（provisional，只告警）；time 范围链结论恒 pending（T036 前不验收链结论）；deposit chain-first 未交付（T034）。

**US1 限制解除（T033–T038 完成，累计 24/38）**：withdrawal/deposit chain-first 归属发现、tx 聚合身份、time window-resolver、策略文档、枚举预算记账均已交付并验收，上段受限状态解除。已知剩余限制（非 US2/US3 能力）：真实 CLI 高度扫描中事件侧无时间窗时 chain-first missing 保持 pending（保守正确；compare loop 高度→时间窗透传为后续优化，不属本批任务）。

**修正（真实链路修复轮，撤回上段解除结论）**：上段“解除”在真实 CLI 铸票证据到位前作出，不成立，撤回。真实高度→事件窗透传、CoverageClosed 门控、事件键 join 落地并经真实入口验收后，US1 状态以本段为准；剩余限制见下。历史测试通过记录保留。
- 已证实（真实 `Run(start→resume→scan)`＋真实三适配器＋真实 Anvil/PG）：deposit/withdrawal 链事实存在且 PG 无行→missing 票并稳定去重；两类匹配 PG 记录→零误报（含 receipted＋事件完整→consistent 零票）；事件不足→pending/gap；上游未接入不屏蔽已证实局部 missing（`ExternalCredit=unverified`）；重复扫描身份稳定；资金表行数＋全行摘要零变化；成员超限可追溯且 gapped 不可闭合。
- 剩余限制：① scope 末端落在本地可证覆盖前沿时带永久 `query_failed` gap（右缝回落），`done` 被阻——运维将 scope 末端设于前沿之下；② intent 键候选（receipt 无可解析 request_id）PG 侧 `linked_row_missing`→incomplete，只能 pending＋重扫（已有契约：000012 `payment_intents_request_fkey`、FR-004、data-model §2；不新增按 N 次失败升级建单能力）。FK 的存在不等同“唯一原因必然是读取竞态”——无快照/查询证据时原因保留未定，保守 pending 结论不变。③ pre-cutover 无事件行请求仍得 missing 票（fail-closed 告警，独立设计项）；④ chain-first `tx_hash` 候选不带 EventKey（不发明 join），其 missing 仅依据 PG 缺失。

---

## Phase 3b: US1 定向收口遗留 (Priority: P1) 🎯 MVP 补齐

**Goal**: 将已批准范围（FR-002/009、quickstart §1、FR-001 time kind）内但无任务归属的 US1 能力补入任务；不新增自动修复/付款/风险接受功能；不缩减 spec。

- [X] T033 [US1] Implement withdrawal chain-first 方向/资产归属过滤 in `internal/app/reconcileadmin/` enumeration + `internal/reconciliation/chainfacts.go` log read (depends on T016,T018; reuses T033 mechanism for T034): attributed (tx touches configured project address set AND contract in asset allowlist; from/to from topic1/topic2 extension read, amounts stay raw hex) → `missing`; unattributed → metrics-only, never a ticket; any chain log without a PG row MUST NOT equal project missing without attribution (covers FR-009, Q1; acceptance: quickstart §1 withdrawal rows)
- [X] T034 [US1] Implement deposit chain-first discovery reusing the T033 attribution mechanism (depends on T016,T018,T033): on-chain credit to a project address with no PG deposit row → `missing` (attributed only); business_types `deposit` closed-set unchanged (covers FR-002/009, Q1; acceptance: quickstart §1 deposit rows)
- [X] T035 [US1] Implement tx-aggregate identity + member-evidence rule in `internal/reconciliation/identity.go` + occurrence appends (depends on T005,T017; reuses the event occurrence contract): one ticket per (scope, `missing`, `tx_hash`, content-hash, version-domain); member logs (`log_index`/contract/topic0/block) enumerated in `evidence_ref` + `discrepancy_occurrence`; same-tx multi-log never splits tickets; reorg replacement (same `tx_hash`, new block) follows the Q5 invalidation path on the same identity (`pending_verify` + occurrence append), never a new ticket (covers FR-007, Q2/Q5)
- [X] T036 [US1] Design + implement the time window-resolver in `internal/reconciliation/` + admin wiring (depends on Foundational; explicitly NOT T028 — T028 is call-path docs + scheduling only): time→height mapping via the local block-time index; unmappable ranges → gap rows; reorg-invalidated mappings → pending; mapping queries charged to budget; `pending-by-design` is the current unfinished state, never an acceptance claim (covers FR-001 time kind, FR-003/017/019, Q3)
- [X] T037 [US1] Confirm-policy + task-policy docs closure with validation (depends on T006; docs in `contracts/task-lifecycle.md` + `quickstart.md`): `--confirm-threshold-n` is a reconcile observation parameter (source = explicit start flag, version snapshot = `task.policy_refs`), never writes `confirmation_policy_history` and never amends the 005 confirmation semantics; `policy_refs`/scope/budget are fixed at creation with no revision transaction (change = cancel + recreate); pre-fix tasks with `'{}'` stay fail-closed pending and are rebuilt, never silently defaulted (covers Q5; existing `TestScanTaskConfirmThresholdN` pins the parse/validation)
- [X] T038 [US1] Enumeration-layer budget accounting in `internal/reconciliation/scan.go` + `budget.go` seam (depends on T008,T016): every enumeration internal read is either charged via a budget callback or covered by a proven statement cap; overruns leave gap rows, never silent drops (covers FR-019, Q5)

**Checkpoint**: US1 full acceptance (quickstart §1–3,8 without restrictions) requires T033–T038; T011–T018 results stand as the restricted-scope evidence.

---

## Phase 4: User Story 2 - 差异去重分类告警与处置闭环 (Priority: P2)

**Goal**: Stable identity dedup, claim/dispose/audit loop, idempotent repeat disposal, least-privilege enforcement (FR-007/008/010–016/020, Q1/Q2/Q5, SC-002/004/005)

**Independent Test**: Fixed discrepancy set with reappearances: single ticket per root cause, claim mutual exclusion, 10x repeat disposal zero side effects, 100% unauthorized refusal + audit (quickstart §2,5,6,9)

### Tests for User Story 2

- [X] T019 [P] [US2] Contract test for discrepancy lifecycle + idempotency keys in `internal/reconciliation/lifecycle_contract_test.go` (tags: `contract`)
- [X] T020 [P] [US2] Integration test for claim/dispose/audit/unauthorized-refusal in `internal/reconciliation/lifecycle_integration_test.go` (tags: `integration`)

### Implementation for User Story 2

- [X] T021 [P] [US2] Implement occurrence append + dedup + linked-ticket logic in `internal/reconciliation/identity.go` (depends on T005; reappearances append `discrepancy_occurrence`, reopen original, no infinite tickets) (covers FR-007, SC-002, Q5-5)
- [X] T022 [US2] Implement claim/dispose paths in `internal/reconciliation/lifecycle.go` (depends on T007,T009,T021; CAS claim with owner; `idempotency_key` UNIQUE read-back; `new_fix_rule` dry-run only; reuse-recovery entries reference existing CLIs without auto-executing) (covers FR-010/012/013/016, Q1/Q2)
- [X] T023 [US2] Implement `reconcile-admin claim/dispose/show` wiring plus `permission-grant/permission-revoke/permission-show` management subcommands in `internal/app/reconcileadmin.go` (depends on T022; management ops reuse the 011 withdrawalexec local-privileged pattern — local execution, principal bound from authenticated caller, `--operator/--reason/--operation-id` required, audited, idempotent; ordinary 014 holders cannot self-grant; auth-matrix enforced; operator/reason/evidence recorded; refusals audited) (covers FR-011/012, Q2)

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - 重组未知中断并发下保守语义 (Priority: P3)

**Goal**: Reorg/unknown/interrupt/concurrent-change conservative semantics with auto-invalidation and reopen (FR-004/005/010/016–019, Q5, SC-003/006)

**Independent Test**: Four fault-injection groups (reorg, unknown receipt, kill/restart interrupt, concurrent updates): zero wrong payments, zero permanent false conclusions, all converge to defined conservative states (quickstart §4,5,7,10; independent `fault` tag runs); history revalidation acceptance per quickstart §11 (advanced waterline + changed old-range evidence discovered without per-item manual trigger)

### Tests for User Story 3

- [X] T024 [P] [US3] Fault test for reorg/unknown/interrupt/concurrent invalidation in `internal/reconciliation/conservative_fault_test.go` (tags: `fault`; independent channel, never ordinary-PR gate)
- [X] T025 [P] [US3] Integration test for checkpoint crash recovery (no-miss/no-dup) plus the overlap counterexample in `internal/reconciliation/recovery_integration_test.go` (tags: `integration`): two concurrent invocations claim the same task (only one `claimed` attempt lands), the late submitter is discarded + audited without moving the pointer, and no DB transaction is held across slow RPC

### Implementation for User Story 3

- [X] T026 [US3] Implement invalidation/reopen evaluator in `internal/reconciliation/lifecycle.go` (depends on T007; reads disposition/reverify/close_basis rows via Foundational schema, not US2 wiring; concurrent-write/reorg/new-evidence/**source-or-version rotation** → `pending_verify`; confirmed recurrence → `reopened` with history; unrelated writes ignored; stale results never close; rotation triggers reverify only, never auto-disposal) (covers Q5, FR-010/017/018)
- [X] T027 [US3] Implement bounded reverify executor in `internal/reconciliation/reverify.go` (depends on T015,T026; enumerates in-scope `closed` items oldest-evidence-first under a bounded budget slice per `data-model.md` §6 and `contracts/discrepancy-lifecycle.md` History section; consumes scan-loop cross-check candidates; failed items persist gap rows (`query_failed`) with bounded retry ordered by failure-count-asc + evidence-age-desc (no slice starvation); traversal cursor never implies verified completeness — gapped items cannot close; Q1 read-only scope + Q5-6 limits; timeout/incomplete/unavailable/insufficient → never `consistent`; bounded retry or stay pending) (covers Q1/Q5, FR-005/018; acceptance: quickstart §11)
- [X] T028 [US3] Prove repeat-invocation convergence and document the call path: reference the `TXHARBOR_RECON_*` key names defined in T018 (no renaming here), add the `§call-path` section to `specs/014-reconciliation-exception-handling/quickstart.md` (repeated budgeted invocations converge; pause/resume/restart entries), and verify scheduling behavior stays US3-scoped (recovery/reverify focus; no daemon added; no serve/worker auto-start) (depends on T018,T023) (covers user req 4, Q3/ADR-001)

**Checkpoint**: All user stories should now be independently functional

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Observability, evidence honesty, and layered validation (no risk-accept features; no prod-threshold claims)

- [ ] T029 [P] Add bounded low-cardinality 014 metrics in `internal/metrics/reconciliation.go` (reuse `internal/metrics/events.go` registry; ENUM labels only; FR-24/27 redaction; duplicates-rate alert separated from fund tickets per Q4; any `recon_audit` aggregation by action MUST exclude management rows via `target ? 'management_action'` — grant/revoke borrow `start`/`close` buckets with `target.management_action` as the discriminator; no new audit token or migration for this)
- [ ] T030 [P] Evidence-honesty pass in `internal/reconciliation/scan.go` and `internal/reconciliation/lifecycle.go` (paused/incomplete never renders “fully consistent”; uncovered ranges from new data visible; Q3-5; pre-cutover requests without event rows stay pending/gap and never mint event-missing tickets — missing-by-insufficient-evidence is alert-only and MUST NOT bypass dispose/close evidence gates; acceptance: quickstart §3 + §11 gapped-never-close)
- [ ] T031 Run `specs/014-reconciliation-exception-handling/quickstart.md` validation matrix and record results as test evidence (not production thresholds); keep ordinary PR CI layered, heavy runs on independent tags
- [ ] T032 [P] Docs touch-up in `specs/014-reconciliation-exception-handling/` (plan/research cross-links; risk-accept explicitly absent; T000-P OPEN restated)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories (merge responsibility: migration + state-machine + auth-matrix owner first)
- **User Stories (Phase 3+)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed), except the single T015→T027 edge (US3 reverify waits for the US1 event adapter)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Final Phase)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) - May integrate with US1 but independently testable
- **User Story 3 (P3)**: T024/T025/T026 start after Foundational (Phase 2) with no story dependencies; T027 (bounded reverify executor) additionally waits for US1 T015 (event-delivery adapter read path). Subtasks without cross-story edges stay parallel; only the T015→T027 edge is serialized.

### Within Each User Story

- Tests MUST be written and FAIL before implementation
- Adapters/identity before compare/classify before wiring
- Core implementation before integration
- Story complete before moving to next priority

### Shared-Artifact Confluence

- `migrations/000016_reconciliation_handling.sql` (T002/T004): single owner, one merge; stories rebase, never duplicate migration edits in parallel.
- `internal/reconciliation/lifecycle.go` state machine (T007/T022/T026): lifecycle owner serializes transitions; US2/US3 coordinate via that owner.
- `internal/app/reconcileadmin.go` + `cmd/txharbor/main.go` (T018/T023/T028): command owner serializes CLI surface; US1–US3 land subcommands through it.

### Parallel Opportunities

- T002, T003 parallel; T005–T009 parallel within Foundational (different files); T011‖T012, T013‖T014‖T015, T019‖T020, T024‖T025 parallel; T033‖T034‖T035‖T036‖T037‖T038 parallel after US1 batch (T034 reuses the T033 attribution mechanism; T033–T036 touch the enumeration/adapter seam via one owner); stories parallel after Foundation; T029‖T030‖T032 parallel in Polish.

---

## Parallel Example: User Story 1

```bash
# Launch all tests for User Story 1 together:
Task: "Contract test for task lifecycle transitions in internal/reconciliation/scan_contract_test.go"
Task: "Integration test for scoped scan + checkpoint resume + budget suspend in internal/reconciliation/scan_integration_test.go"

# Launch all adapters for User Story 1 together:
Task: "Read-only chain-facts adapter in internal/reconciliation/chainfacts.go"
Task: "PG-state adapter in internal/reconciliation/pgstate.go"
Task: "Event-delivery adapter in internal/reconciliation/eventstate.go"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: controlled dataset (SC-001/002/006; quickstart §1–3,8); ordinary-PR checks only
5. Local commit node; demo if ready

### Incremental Delivery

1. Setup + Foundational → Foundation ready (commit node)
2. US1 → independent test → commit node (MVP)
3. US2 → independent test → commit node
4. US3 (+T028 production entry) → independent test + fault channel → commit node
5. Polish → quickstart matrix → commit node

### Entry/Exit Criteria per Batch

- Entry: predecessor phases merged; story prerequisites (listed Depends) complete; fixtures available.
- Exit: story Independent Test green on required tags; uncovered/executed checks recorded (unexecuted checks never marked pass); story checkpoint validated; local commit created.
- Applicable checks: `gofmt`/`go vet`/unit(+race)/contract always; `integration*` per story tags with Docker-absent NOT RUN discipline; `fault`/`perf` only on independent channel.

### Parallel Team Strategy

1. Team completes Setup + Foundational together (migration/state-machine/auth owner merges first)
2. Once Foundational is done:
   - Developer A: User Story 1 (+ adapters)
   - Developer B: User Story 2 (+ lifecycle/audit)
   - Developer C: User Story 3 (+ reverify/production entry)
3. Stories complete and integrate independently through the confluence owners above

---

## Notes

- Coverage: FR-001–025, SC-001–006, Q1–Q5, quickstart §1–11 all mapped above (38 tasks: T001–T032 original + T033–T038 US1 closure);重点 six (atomicity, crash-no-miss, legal-duplicate silence, insufficient-evidence conservatism, invalidation on change, unauthorized refusal, unknown-disposal idempotency, pause/budget isolation) land in T010/T016/T017/T021/T022/T023/T026/T027/T028; chain-first attribution/deposit/identity/window-resolver/policy/budget-accounting land in T033/T034/T035/T036/T037/T038.
- Risk-accept/ignore: NOT approved — zero implementation tasks generated for it; any future need is a business blocker, not a tasks-time decision.
- Management authorization decided (014-only, 2026-09-26): first and subsequent grants/queries/revokes are single-executed by an authenticated local-ops principal explicitly granted management permission; ordinary holders cannot self-grant; first trust root comes from controlled deploy config, default-deny without valid config; no admin role created, no existing permission expanded; grants/revokes record before/after state, operator, and result.
- Prod thresholds pending do not block local validation; local numbers are never claimed as production thresholds.
- [P] tasks = different files, no dependencies; [Story] label maps traceability; commit after each task or logical group; stop at any checkpoint to validate independently.
