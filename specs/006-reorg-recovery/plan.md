# Implementation Plan: 006 Reorg Recovery

**Branch**: `006-reorg-recovery` | **Date**: 2026-09-14 | **Spec**: [spec.md](spec.md) (7ace1fe, Q1–Q4 integrated)

**Input**: Feature specification from `/specs/006-reorg-recovery/spec.md`

## Summary

When a proven fork appears, 006 persists a versioned recovery instance as the sole recovery
authority (writing no pause table — R9), which stops the chain's confirmations and all
withdrawal broadcasts through the recovery-state gate on every ordinary commit path while
pre-existing stream pause rows stay independently in force, finds the common
ancestor by read-only suffix walk within the bound depth, flips old-fork rows non-canonical while
retaining them, converts affected deposits to Orphaned with evidence, rolls back the three stream
checkpoints to guarded floors, replays from the ancestor under the recovery version, revives
re-canonicalized observations to Pending for 005 to reconfirm, and releases only after re-verified
completion (auto path) or authorized two-step reconciliation (manual path). Technical approach:
reuses the single `indexer_lease` coordination discipline for every transaction; carry fencing in a
new monotonic recovery-seq with the recovery row as the sole recovery authority (no pause-table
writes — stream diagnoses stay intact);
rework `chain_blocks` PK to hold both forks; model Orphaned as a third observation state with an
append-only transition log. See research.md R1–R14 for decisions, data-model.md for schema and the
per-transaction catalog.

## Technical Context

**Language/Version**: Go (repo toolchain, `go.mod`; `big.Int` for money, go-ethereum hash types —
constitution-mandated, no float path).

**Primary Dependencies**: pgx (PostgreSQL), go-ethereum ethclient RPC wrapper (existing timeout/
transport/chain-mismatch/invalid-response classification), goose migrations, Anvil (local chain).

**Storage**: PostgreSQL only. New: `reorg_recovery`, `reorg_recovery_events`, `reorg_policy_history`,
`deposit_observation_transitions`; altered: `chain_blocks` PK rework + canonical partial-unique,
`deposit_observations` 3-state CHECK + orphan-evidence columns. No Redis/Kafka/new infra.

**Testing**: `go test ./...` (unit) + `make test-integration` (real PostgreSQL) + Anvil E2E harness
(scenarios V1–V12 in quickstart.md; design only, nothing executed).

**Target Platform**: Linux server, single deployment single chain (constitution architecture evolution).

**Project Type**: Backend wallet/transaction infrastructure (monorepo: `internal/indexer` writers).

**Performance Goals**: Recovery poses no throughput target; lock discipline keeps every transaction to
3–5 point statements (inherited 5s writeGuard); ancestor walk is the only unbounded loop and is
read-only/off-lock. No benchmark claims.

**Constraints**: Bounded retries with backoff (reuse INDEX parameters for matching failure classes);
no tight loops; zero RPC inside any DB transaction; DB `now()` as the only clock for lease/version
validity; exact integer depth math (no saturation as audit basis).

**Scale/Scope**: One chain, one active recovery; policy/version tables tiny; transition log grows per
affected observation per cycle (bounded by fork size, indexed by identity).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-checked after Phase 1 design — see below.*

Pre-research gate (2026-09-14, before research.md): I Financial Correctness (recoverability over
speed; CHECK/UNIQUE/FK guards; no float) ✓ · II Idempotency (UNIQUE + exact guards + atomic
write+cursor, persistence-enforced) ✓ · III PostgreSQL source of truth (no new infra; Redis/Kafka
untouched) ✓ · IV Reorg-aware (explicit canonical/orphaned distinction; depth explicit per chain) ✓ ·
V Explicit state machines (recovery phases + observation Pending/Confirmed/Orphaned with allowed
transitions) ✓ · VI Atomic boundaries (per-transaction write sets; no dual-write assumption) ✓ ·
VII Nonce safety (untouched; 008 paused during recovery) ✓ · VIII Key isolation (no key material;
privileged ops are SQL-operator shaped like existing auth paths) ✓ · IX Failure paths first-class
(evidence classes, bounded retries, crash-resume, tamper backstop) ✓ · X Local deterministic testing
(Anvil + real PG tiers) ✓ · XI Invariant tests (per-scenario assertions, real concurrency) ✓ ·
XII Observability as correctness (metrics/state/diagnostics from day one) ✓ · XIII Simplicity
(reuse lease discipline, policy-history shape, pause gates; no new coordination/lock/metric systems;
new tables only where upstream cannot carry recovery state) ✓ · XIV Bounded spec-driven change
(006 only; 007–011 contracts, no tables/services) ✓. No violations, no complexity-tracking entries.

Post-design re-check (after research.md + data-model.md + contracts/ + quickstart.md): all gates
still pass. Two points verified, not waived: (1) `chain_blocks` PK rework preserves I1 via the
partial-unique (IV/V discipline kept at storage layer); (2) the single lock-holder residual is
documented with sweep-coverage proof (I/II honesty — claimed as bounded, not as zero). No new
violations introduced by the design.

## Requirement → design → validation mapping

| Spec | Design | Validation |
|------|--------|------------|
| FR-01/02 (detect, establish+pause) | `establish` txn; R9 recovery-row authority | V3, V11 |
| FR-03 (depth/config) | R3 math; R4 policy history; data-model Table 3 | V1, V2 |
| FR-04 (unrecoverable→reconcile) | R5 search terminals; reconcile phase | V9, V10 |
| FR-05/06/07 (invalidate+orphan+audit) | invalidate txns; Table 4 transitions | V3, V12 |
| FR-08 (re-mine vs revive) | new-identity vs revive rules; Q4 six rules in `revive_observation` | V4 |
| FR-09/10 (replay semantics) | `replay_range`; history-semantics inheritance; 005 re-gating | V3–V5 |
| FR-11 (skew/empty) | rollback floors; empty-range advance | V5 |
| FR-12/13 (resume/unknown) | phases + frontiers; re-read-to-triage | V7, resume drill |
| FR-14/15/20 (fencing/races) | R1 version gate (capture-first/commit-triple) + R7 proofs | V6, V7, V11 |
| FR-16 (re-fork) | ancestor re-validation, no false release | V8 |
| FR-17 (independent pauses) | release deletes recovery row only; survivors recorded | V11 |
| FR-18/21/22 (query/audit/observe) | R11; contracts/observability.md | V11, V12 |
| FR-19 (completion) | `complete_reverify`判据 re-read; ≠ live head | V8 |
| FR-23 (auto/manual auth) | auto vs two-step manual txns; Q2b evidence | V7, V11 |
| FR-24/25/26 (deferrals/matrix) | R1–R14; matrix in spec; downstream.md | V6, V11, V12 |
| SC-01–SC-12 | per-scenario assertions in quickstart V1–V12 | V1–V12 + drill |

OQ1 → R1+R5 · OQ2 → data-model Observability + contracts/observability.md · OQ3 → R1+R6 ·
FR-24 → R1–R14 (no remainder).

## Project Structure

### Documentation (this feature)

```text
specs/006-reorg-recovery/
├── plan.md                 # This file (/speckit.plan output)
├── research.md             # Phase 0 output (R1–R14 decisions)
├── data-model.md           # Phase 1 output (schema + txn catalog)
├── quickstart.md           # Phase 1 output (V1–V12 validation guide, design only)
├── contracts/              # Phase 1 output
│   ├── observability.md    # metrics/state/diagnostics/validity annotation
│   └── downstream.md       # 007–011 check obligations (no future tables/services)
├── checklists/
│   └── requirements.md     # specify/clarify artifact (re-validated, readability noted below)
└── spec.md                 # Q1–Q4 integrated (7ace1fe + clarify commit)
```

### Source Code (repository root)

```text
internal/indexer/
├── reorg.go                # recovery executor (phase machine driver, read-only search)
├── reorgcommit.go          # 006 transactions (establish/invalidate/rollback/replay/revive/release)
├── reorgpolicy.go          # depth-config bootstrap/change/verify (R4 shape)
├── reorgquery.go           # validity-annotated readers (R11)
├── scanner.go              # EXTEND: sibling-aware insert/compare; recovery-state gate recheck
├── logscanner.go           # EXTEND: recovery-state gate recheck
├── depositcommit.go        # EXTEND: recovery-state gate recheck (+006-owned re-read path)
├── confirmcommit.go        # EXTEND: recovery-state gate recheck; orphaned→stop routing note
├── lease.go                # reuse unchanged (single coordination row)
└── coordinator.go          # reuse unchanged (RunQuatro + recovery loop wiring)

migrations/
└── 000006_reorg_recovery.sql  # new tables + chain_blocks PK rework + CHECK rewrites (R13)

tests (per V-matrix; design only, not written here):
├── unit: depth math, guard matrices (go test)
├── integration: txn rechecks, tamper resistance, cycles (real PostgreSQL)
└── e2e: forks, crashes, races, pauses (Anvil)
```

**Structure Decision**: follow the existing `internal/indexer` writer layout (one concern per file,
shared lease/coordinator untouched); migration follows the numbered goose sequence. No new
packages, services, or infrastructure.

## Complexity Tracking

> Fill ONLY if Constitution Check has violations that must be justified

None. (New tables in one migration are required carriers — upstream tables cannot hold recovery
state without breaking their stream contracts; see R1/R2/R8 alternatives rejected.)

## Evidence separation (per task instruction — status, not proof)

- 005 merge point `a73e31b` (PR #7): present in this branch's ancestry (branch base) — verified.
- 005 regression / local acceptance / remote CI: NOT verified in this step (no implementation
  inspection run, no test executed here) — recorded as pre-implementation verification requirements
  (FR-25), carried as plan inputs, not as claims.
- T000-P: open per 005 spec assumptions — open status neither proves nor disproves local acceptance;
  production readiness stays out of scope.
- Missing live implementation evidence does not block this technical plan (instruction), but tasks/
  implementation MUST re-verify FR-25 items before touching 005-owned paths.

## Batch A evidence ledger (2026-09-14, branch `006-reorg-recovery`)

Recorded by T001–T005; nothing here claims 005 acceptance or production readiness.

- T001: `a73e31b` in ancestry (`merge-base --is-ancestor` OK); `specs/005-confirmation-tracking/`
  spec/plan/tasks present; `migrations/000005_confirmation_tracking.sql` present;
  002–004 spec dirs present. No 005 file modified.
- T002: evidence sources present as files (`confirmation_*_test.go`,
  `confirmcommit_test.go`, `confirmauth_integration_test.go`, `ci.yml`); their
  execution/pass evidence was NOT produced in this batch — recorded as
  missing-with-handling: T016/T034 stay blocked (plus story phases via unclosed
  Phase 2). 005 tasks show 29/30 checked with only T000-P open, but that box
  count is not scored as gate-passed here.
- T003: T000-P stays open per 005 spec Assumptions + 005 tasks `T000-P` unchecked;
  open neither proves nor disproves local acceptance; production readiness stays
  out of 006 scope. No task conflates T000-P with local preconditions.
- T004: pinned entries recorded available — U `make test`; I/E
  `make test-integration` (Docker daemon OK; testcontainers self-provision
  postgres:18 + Anvil foundry v1.8.1 chain-31337; compose stack is a manual-debug
  aid only, never consumed by automated tests). Execution itself is future work.
- T005: `migrations/000001_baseline.sql` through `000005_confirmation_tracking.sql`
  present in sequence; `migrations/embed.go` auto-includes `*.sql`, so the new
  `000006` file needs no embed change. Migrate role unchanged (same DB operator
  shape as 000002–000005).

## Readability follow-up (checklist remainder from clarify)

The requirements checklist still lists two partial items (non-technical-stakeholder wording).
Action taken in this step: added the 阅读对象 paragraph + operator-first phrasing in new artifacts
(research rationale lines, quickstart scenario table, this plan's Summary); domain terms
(canonical/Orphaned/checkpoint/ancestor) are defined once in spec Key Entities and reused exactly.
Assessment: items remain technically worded by necessity — chain-reorganization correctness cannot be
expressed without these terms, and inventing plain-language synonyms would create terminology drift
(a clarify anti-goal). Impact on plan: none — quickstart.md is the operator's runnable guide and is
written task-first.
