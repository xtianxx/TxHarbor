# Implementation Plan: 008 Nonce Manager

**Branch**: `008-nonce-manager` | **Date**: 2026-09-16 | **Spec**: [spec.md](spec.md) (OC-1–OC-7
closed 2026-09-16, zero markers)

**Input**: Feature specification from `/specs/008-nonce-manager/spec.md`

## Summary

008 provides concurrency-safe, durable nonce admission for outbound transactions: it binds one
payment intent to exactly one `(chain_id, sender, nonce)` and never lets a second intent claim
that nonce — across concurrency, retries, crash/restart, unknown outcomes, chain-view divergence,
and 006 recovery pauses. Technical approach (research R1–R13): every write rides the repo's
existing single coordination discipline (chain row `FOR UPDATE` + `writeGuard` + post-lock
rechecks, shared with 006's establish/release transactions so admission order is linearizable
against recovery), binding uniqueness is carried by two named DB UNIQUEs
(`nonce_bindings_intent_uniq`, `nonce_bindings_scope_nonce_uniq`), candidate selection never
adopts the chain view directly (explicit classification: `consistent` / bootstrap evidence /
`unattributed_consumption` / `unexplained_gap` / `divergence` / `unavailable`), ambiguous scopes
are held and released only by the existing operator carrier (`txharbor nonce-admin`) with a
re-verify-in-tx evidence standard, sender authority lives in a versioned controlled registry,
authorization reuses 007's approved carrier read-only, and 009 consumes a five-outcome read-only
HTTP contract (facts separated from permission). 008 never signs, broadcasts, holds keys, or
writes 006/007 state. Design artifacts: `research.md` (R1–R13), `data-model.md` (7 tables + txn
catalog + concurrency argument), `contracts/` (read API, reconcile observation/operator release,
downstream inputs), `quickstart.md` (V1–V13).

## Technical Context

**Language/Version**: Go 1.26.5 (`go.mod` toolchain); EVM quantities `math/big`; money/nonces as
integer SQL (`NUMERIC(78,0)`) — zero float path (原则 I).

**Primary Dependencies**: pgx v5.11.0 (pool + tx), go-ethereum v1.17.5 (JSON-RPC via the existing
`internal/eth` client classification), goose v3.28.0 (numbered migrations), existing
`internal/{config,db,eth,logx,metrics,app}`. **No new dependencies, no new infrastructure**
(no Redis/Kafka).

**Storage**: PostgreSQL 18.6 only, the durable source of truth. Planned new tables (design in
`data-model.md`; migration `migrations/000008_nonce_manager.sql`, not created in this step):
`nonce_wallet_registry`, `nonce_scope_state`, `nonce_bindings`, `nonce_binding_events`,
`nonce_observations`, `nonce_scope_holds`, `nonce_ops_audit`. Read-only inputs:
`indexer_pause`/`log_pause`/`deposit_pause`, `reorg_recovery`, `reorg_recovery_events` (006) and
`withdrawal_authorizations` (007) — never written by 008.

**Testing**: `go test ./...` (U: classification matrix, numeric bounds, outcome mapping, operator
input equality) + `make test-integration` (I/E: real PostgreSQL via testcontainers + real Anvil
foundry v1.8.1 as chain truth; concurrency, crash/restart, RPC fault injection, release races),
scenario matrix V1–V13 (`quickstart.md`, design only here). Real 009/010/011 integration
acceptance is deferred (`contracts/downstream.md` §4); test doubles are allowed for early
development only (原则 XI).

**Target Platform**: Linux server, single deployment, single chain (constitution Architecture
Evolution; upstream assumption).

**Project Type**: Backend wallet/transaction infrastructure (monorepo). New domain package
`internal/nonce` following the `internal/indexer`/`internal/withdrawal` one-concern-per-file
layout; extensions in `internal/app` (HTTP read mount + `nonce-admin` subcommand) and
`internal/config`.

**Performance Goals**: No throughput target. One admission = one bounded pre-tx chain observation
(existing `IndexRPCTimeout`/retry knobs, no new timing knobs) + one short locked transaction of
3–6 point statements under the shared 5s per-statement `writeGuard`. Reconcile observes each known
scope on the existing `IndexPollInterval` cadence. No benchmark claims.

**Constraints**: zero RPC inside any DB transaction; DB `now()`/`clock_timestamp()` as the only
validity clock; bounded retries reusing existing failure classes; endpoint authorization
fail-closed; secrets/tokens never logged (`logx.Redact`); test resources workdir-local or
testcontainer-provisioned (workflow R4); **no service started/executed in this planning step**.

**Scale/Scope**: one chain; registered senders in the tens; one binding per payment intent (grows
with payments, bounded by registry admission); observation rows append per allocation and per
reconcile tick (indexed by `(chain_id, sender, at)`); holds few and short-lived; operator
operations rare, append-only audit.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Pre-Phase-0 (2026-09-16, against Constitution 1.1.0):

- **I Financial correctness**: PASS — integer-only nonces/counts (`NUMERIC(78,0)` + `big.Int`,
  no float anywhere); invalid states made uncommittable via named UNIQUE/FK/CHECK; duplicate
  allocation is structurally impossible (intent-unique + scope-nonce-unique); ambiguous states
  are held, never guessed.
- **II Idempotency by design**: PASS — replay/conflict decided by DB UNIQUE carriers and in-tx
  intent re-read (never app memory); observation/event rows converge on named UNIQUEs; operator
  attempts dedup by `operation_id` at the persistence layer.
- **III PostgreSQL is the durable source of truth**: PASS — all 008 state in PostgreSQL; no cache
  or broker in any correctness path; restart re-reads durable rows only.
- **V State machines must be explicit**: PASS — binding lifecycle
  `allocated → in_flight → consumed | released` with CHECK-enforced terminal consistency and an
  append-only transition log; release only through one controlled operator path.
- **VI Transaction boundaries preserve invariants**: PASS — admission commits binding + event +
  evidence observation in one tx; release commits hold + floor + audit in one tx; no dual writes,
  no assumption of DB-plus-publish atomicity.
- **VII Nonce allocation concurrency-safe**: PASS — shared durable row lock (the chain
  coordination row, same discipline as 006 transactions) + UNIQUE backstops; in-memory mutexes are
  never relied on; recovery covers restart, pending, unknown outcomes, replacement linkage,
  RPC divergence, and external submission; real-concurrency integration tests required.
- **IX Failure paths are first-class**: PASS — RPC failure classes, divergence, gaps, external
  consumption, DB unavailability, crash/restart, release races are all design-visible with
  recorded causes and fail-closed behavior; retries bounded and intentional.
- **XI Test the invariant, not only the happy path**: PASS — the V-matrix is failure-first and
  exercises real concurrency; mocks are explicitly not a substitute; the deferred real
  009–011 acceptance is stated, not hidden.
- **XII Observability is part of correctness**: PASS — structured logs with chain/sender/nonce/
  binding/intent/hold/cause/classification fields (redacted) and metrics for allocations, holds,
  observations, read outcomes, operator actions from day one.
- **XIV Small, reviewable, spec-driven changes**: PASS — bounded to the 008 spec; no 010/011
  specs created; upstream contracts consumed read-only; scope fenced by Non-Goals.
- IV (reorg-aware): PASS — 006 state is read and annotated, never rewritten; admission order vs
  recovery establishment is linearized via the shared lock; no chain-atomicity claim is made.
- VIII (key isolation): PASS — no key material or signing anywhere in 008; the read API exposes
  facts only; privileged operations reuse the DSN-operator trust shape (no new role).
- X (deterministic local testing): PASS — testcontainer PostgreSQL + Anvil; workdir-local
  names/ports; no public testnet dependency.
- XIII (simplicity before distribution): PASS — no new service/infra; one existing lock object;
  one operator carrier reusing the `confirm-auth`/`withdrawal-authz` shape; tables are minimal
  carriers for state the upstream tables cannot hold.

No violations, no complexity-tracking entries.

Post-Phase-1 re-check (2026-09-16, after research/data-model/contracts/quickstart): all gates
still pass; three points verified rather than waived — (1) the five-outcome read API and the
operator release path share no writable state (reads are `REPEATABLE READ, READ ONLY`; OC-6
"查询 MUST NOT 新增、变更、释放或重分配" holds by construction); (2) the seven new tables are
required carriers (006/007 tables cannot hold bindings, holds, evidence, or registry state
without redefining their locked contracts), and every chain-dependent classification is
evidence-persisted so no hidden state exists; (3) observation concurrency is explicit and bounded
(`contracts/observation.md` §2.1/§3.1) — the pre-tx chain observation (two
`eth_getTransactionCount` reads per scope) is valid only for the tx it fronts; a concurrent
allocation, a 006 recovery establish/pause, or a registry change makes it stale, and the
coordination-row lock plus the in-tx re-read checklist (006 gate rows, registry `state`/`seq`,
bindings/`M`, floor/`last_*`, active holds) refuses on drift; the lock serializes 008/006 writers
only and makes no chain-atomicity claim, so an external consumption in the window surfaces as the
next observation's hold/reconcile — a stale observation never lowers the floor, releases a hold,
or resets/recycles an existing binding, and a pause release is valid only for the exact evidence
version re-verified under the lock. No new violations.

## Project Structure

### Documentation (this feature)

```text
specs/008-nonce-manager/
├── plan.md              # This file (/speckit.plan output)
├── research.md          # Phase 0 output (R1–R13 decisions)
├── data-model.md        # Phase 1 output (7 tables, state machine, txn catalog, concurrency argument)
├── quickstart.md        # Phase 1 output (V1–V13 validation guide, design only)
├── contracts/           # Phase 1 output
│   ├── read-api.md      # 008 → 009 provider-side five-outcome read contract
│   ├── observation.md   # pause/reconcile observation + operator release contract
│   └── downstream.md    # 010/011 input contract, fixture scheme, deferred acceptance
├── checklists/
│   └── requirements.md  # specify/clarify artifact (22/23; intentional tier item open)
├── spec.md              # OC-1–OC-7 integrated (zero markers)
└── tasks.md             # Phase 2 output (/speckit.tasks — NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
internal/nonce/                 # NEW domain package (one concern per file, repo layout)
├── allocate.go                 # admission tx: lock, 006/007 gates, classify, insert, replay/conflict
├── binding.go                  # binding model/readers, state machine guards, event writers
├── observe.go                  # chain observation: latest/pending/head, RPC error classification
├── classify.go                 # classification matrix (consistent/bootstrap/gap/consumption/divergence)
├── reconcile.go                # per-scope observer loop, transitions, hold establishment
├── hold.go                     # holds + scope floor queries/writes
├── registry.go                 # wallet registry reads; versioning
├── readapi.go                  # 009 read contract handlers (five outcomes, snapshot reads)
├── admin.go                    # operator library: hold-release/binding-release/registry txns
├── numeric.go                  # big.Int bounds/parsing, decimal/hex transport helpers
└── ..._test.go                 # unit; integration files with //go:build integration

internal/app/
├── serve.go                    # EXTEND: mount /nonce/bindings read endpoints; start reconcile loop;
│                               # startup rebuild verification gate; version echo
├── nonceadmin.go               # NEW subcommand carrier `txharbor nonce-admin`
│                               # (mirrors withdrawalauthz.go/confirmauth.go shape)
└── cmd/txharbor/main.go        # EXTEND: dispatch `nonce-admin` (one switch case)

internal/config/config.go       # EXTEND: read-token + reconcile/observation knobs
                                # (reuse Index* timing knobs; no new secret in errors/logs)

migrations/
└── 000008_nonce_manager.sql    # 7 tables (design: data-model.md); pure DDL;
                                # every classify-relevant constraint explicitly named

tests (per V-matrix; design only, not written here):
├── unit: go test (classification, bounds, equality, outcome mapping)
├── integration: real PostgreSQL (concurrency, crash/restart, release races, evidence retention)
└── e2e: Anvil (chain consumption, divergence, external consumption, recovery coexistence)
```

**Structure Decision**: follow the existing `internal/indexer`/`internal/withdrawal` writer
layout (one concern per file, shared coordination primitives reused not copied); migration follows
the numbered goose sequence; no new packages for cross-cutting concerns, no new services, no new
infrastructure. Upstream files are touched only at the existing extension points
(`serve.go` mount, `main.go` dispatch, `config.go` knobs) — 006/007 semantics and migration files
remain byte-identical.

## Requirement → design → validation mapping

| Spec | Design carrier | Verified by (future) |
|---|---|---|
| FR-01 (scope isolation, registry authority) | registry table + in-tx registry check + FK (R9) | V1, V10 |
| FR-02 (one active intent per nonce, persistence concurrency) | `nonce_bindings_scope_nonce_uniq` + coordination-row lock (R1/R3) | V1, V13 |
| FR-03 (persist before side effect, idempotent, OC-3 binding) | admission tx (binding+event+observation), intent replay/conflict (R3/R6) | V2, V4 |
| FR-04 (stable intent identity, never create) | `intent_id` opaque input + `nonce_bindings_intent_uniq`; deferred 011 linkage | V2, V11 |
| FR-05 (repeat reuse, persisted) | in-tx intent re-read + UNIQUE classify (R3) | V2 |
| FR-06 (crash/restart reuse) | durable-only decisions + rebuild gate (R5) | V3 |
| FR-07 (unknown retained) | `in_flight` persistent state + observation evidence (R6) | V4 |
| FR-08 (no auto recycle; operator reconcile) | hold model + operator release carrier + evidence standard (R7) | V7, V12 |
| FR-09 (never reassign; replacement stays) | nonce immutability + full UNIQUEs; replacement via binding identity (R6) | V1, V4 |
| FR-10 (divergence recorded, no silent reuse) | classification matrix + observations (R2/R4) | V5 |
| FR-11 (gaps not reusable, query/audit) | monotonic frontier + gap classification + read API (R2/R8) | V5, V9 |
| FR-12 (consumption recorded, no silent merge) | observation evidence + bootstrap path + holds (R2/R4) | V5, V6 |
| FR-13 (rebuild fail-closed) | startup rebuild verification gate (R5) | V3 |
| FR-14 (006 precedence, OC-6 use semantics) | shared lock ordering + in-tx 006 gate re-read + read annotations (R1/R8) | V7, V8, V9 |
| FR-15 (pre-commit re-verify; OC-7 ordering) | locked admission + post-lock rechecks + fail-closed reads (R1/R8) | V5, V8 |
| FR-16 (Accepted is not an intent/authorization) | no 007-triggered allocation path exists; input contract requires persisted intent + valid authorization (R10) | V11 + review |
| FR-17 (authorization validation, fail-closed) | read-only 007 carrier validation + identity/version storage (R10) | V11 |
| FR-18 (attempt/signing identities; no cross-intent claim) | binding identity contract; 010 linkage deferred (contracts/downstream.md) | review + deferred |
| FR-19 (no keys/signing/broadcast) | zero key paths in package; read-only API only (R8) | V9, V13 |
| FR-20 (explicit state machine) | binding states + CHECK + events (R6) | V4, V13 |
| FR-21 (observability, no secrets) | logs/metrics fields (R11) | V13 |
| FR-22 (real concurrency/restart testing) | V-matrix levels; testcontainers (R12) | V1–V13 |
| FR-23 (no upstream redefinition) | read-only consumption; ownership table (data-model) | snapshot assertions (V7, V11, V13) |
| SC-01–SC-09 | per-scenario assertions in `quickstart.md` | V1–V13 respectively |

006 inputs consumed (read-only, `8e1a440`): FR-26 allocation pause + in-flight-unknown semantics;
downstream preconditions (pause rows + active recovery row); Q2b operator-path shape for release;
validity vocabulary subset for read annotations. 007 inputs consumed (read-only, `19fa11e`):
receive-only boundary (no nonce field, no execution trigger); `withdrawal_authorizations` as the
OC-5 carrier; HTTP/auth/middleware patterns. Downstream contracts defined here: 009 read API,
010 attempt linkage identity, 011 allocation input (`contracts/`).

## Verification plan (this step writes the plan; execution belongs to tasks/implement)

- Migration verification (future task): `000008` up/down reproducibility on scratch DBs; negative
  probes — UNIQUE conflicts expect SQLSTATE 23505 with the exact `ConstraintName` values declared
  in the migration; CHECK violations expect 23514 (never in the 23505 classify list).
- 002–007 regression: `go test ./...` + `make test-integration` stay green; zero modifications to
  002–007 files outside the extension points listed in Structure (diff-gated in tasks).
- Interface tests (future): contract-shape tests for every row of `contracts/read-api.md` §2/
  §3 (status codes, body shape, `notice` presence, read-immutability snapshots) and every
  `contracts/observation.md` §3/§4 assertion (only named hold cleared, floor monotonicity,
  multi-cause survival, 006 byte-identity).
- CI: the existing `ci.yml` four jobs (lint / build / unit+race / integration-Docker) cover 008
  with no workflow change.
- Residual risks (stated, not hidden): chain-view freshness between observation and commit
  (mitigated by downstream gates + reconcile; no chain-atomicity claim); deferred 010/011
  integration (contracts frozen now, acceptance later); production provider selection remains
  T000-P open.

## Complexity Tracking

> Fill ONLY if Constitution Check has violations that must be justified

None. The seven new tables are required carriers — upstream tables cannot hold nonce bindings,
holds, chain-view evidence, registry state, or operator audit without breaking their locked
contracts (see research R2/R7/R9 alternatives rejected). One coordination lock object total
(the existing chain row); one new operator carrier reusing the existing privileged shape; one new
domain package; no new service, dependency, or infrastructure.

## Evidence separation (per instruction — status, not proof)

- Baselines: branch `008-nonce-manager` at `93975fb`; spec + checklist authored/closed in this
  workdir under the 008/009 parallel window (workflow R1/R2). 006 merge `8e1a440` and 007 merge
  `19fa11e` are consumed as read-only inputs; nothing here re-verifies them or claims their
  acceptance.
- T000-P remains open per spec Assumptions; it neither blocks nor substitutes for 008
  verification.
- No implementation code, migration, service, or test was written or executed in this step;
  V1–V13 and every verification line above are design-only.
- This plan is **uncommitted** on purpose (orchestrator owns the commit); the sibling 009 plan
  consumes the same shared basis independently, and interface cross-checks happen after both
  branches report.
