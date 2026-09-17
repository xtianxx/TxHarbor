# Research: 008 Nonce Manager

**Branch**: `008-nonce-manager` | **Date**: 2026-09-16 | **Spec**: [spec.md](spec.md) (clarify
OC-1–OC-7 closed 2026-09-16, zero markers)

Phase 0 output. All technical unknowns are resolved below; no NEEDS CLARIFICATION remains.
Business decisions (OC-1–OC-7) were closed by the two clarify sessions of 2026-09-16 and are
**inputs here, not unknowns** — this file does not re-open them. Evidence base is read-only
(specs 006/007, migrations 000001–000007, `internal/`); no code, service, or test is executed.

Evidence base (read-only):
- Upstream contracts: `specs/006-reorg-recovery/spec.md` (FR-26 matrix, Q2b, Downstream
  Handoff), `specs/006-reorg-recovery/contracts/downstream.md` (preconditions, in-flight
  unknowns), `specs/007-withdrawal-creation/spec.md` (receive-only, FR-03b, Downstream Handoff),
  `specs/007-withdrawal-creation/contracts/api.md` §4.
- Repo mechanics: `internal/indexer/lease.go` (coordination row, `acquireSQL`/`lockLeaseSQL`,
  fencing), `internal/indexer/scanner.go:1085` (`writeGuard`), `internal/indexer/reorgcommit.go`
  (5-step txn shape: writeGuard → ensure lease → FOR UPDATE → post-lock rechecks → COMMIT),
  `internal/indexer/reorgquery.go` (validity annotation), `internal/withdrawal/intake.go` /
  `grant.go` / `query.go` (insert-first 23505 classify, `FOR SHARE`/`FOR UPDATE` split,
  REPEATABLE READ read tx), `internal/app/withdrawalauthz.go` + `internal/app/serve.go`
  (operator subcommand carrier, HTTP mount), `internal/config/config.go` (env-only config,
  versioned config identity hash), `internal/db/migrate.go` (goose, advisory migrate lock only),
  `migrations/000006_reorg_recovery.sql`, `migrations/000007_withdrawal_creation.sql`.
- Constitution 1.1.0 (principles I–XIV; VII is the nonce core), `docs/workflow-008-009-parallel.md`
  (R1/R2/R3/R4/R6), `docs/project-context.md`, `agent.md`.
- External (knowledge base, no fetch): PostgreSQL row-lock semantics under READ COMMITTED
  (`FOR UPDATE` serialization, `clock_timestamp()` vs `now()`), `eth_getTransactionCount`
  semantics for `latest`/`pending`, EVM account-nonce ordering.

Repo pins that bind every decision: **postgres:18.6-trixie**; **pgx v5.11.0**; **Go 1.26.5**;
**no new infrastructure** (008 Non-Goals; constitution XIII — no Redis/Kafka as a correctness
dependency); **zero RPC inside any DB transaction** (006/007 precedent); **DB `now()`/`clock_timestamp()`
as the only clock**; money/EVM quantities integer-only (`big.Int` / `NUMERIC(78,0)`).

## R1 — Serialization: reuse the chain coordination row as the single lock object

- **Decision**: every 008 write transaction (allocation, observation, hold establish/release,
  binding disposition, registry change) uses the repo's existing chain coordination discipline:
  `BEGIN → SET LOCAL statement_timeout='5s'` (shared `writeGuard`) → ensure the `indexer_lease`
  row exists (`ensureLeaseSQL`) → `SELECT … FOR UPDATE` on it (shared `lockCoordSQL`) → in-tx
  rechecks → point writes → `COMMIT`. 008 does **not** acquire or renew the lease, does not read
  or bump the fencing token, and never writes `indexer_lease`. Fixed lock order: coordination row
  first, then domain rows; there is exactly one lock object on every 008 path.
- **Rationale**: 006 establish/pause/release transactions all run under the same row lock
  (`reorgcommit.go` 5-step shape), so taking it makes 008's **admission linearizable against
  recovery establishment**: a recovery that commits before 008's lock acquisition is always
  observed by the in-tx gate re-read; an admission that commits first is already durable and is
  handled by 006's downstream in-flight-unknown semantics. This is the "明确、可验证的先后顺序"
  OC-7 requires for 008's delivery point (预留准入). One lock object also means no lock-order ring
  is possible with 006/007 writers. Not being a lease owner is deliberate: 008 re-reads every
  decision under the lock, owns only its own tables, and never writes 006 state, so lease
  ownership would only couple allocation liveness to the indexer heartbeat with zero added
  guarantee.
- **Alternatives considered**:
  - Per-scope advisory lock (`pg_advisory_xact_lock`): rejected — a second coordination primitive
    with no ordering against 006 writers (establish would not serialize with allocation), and the
    repo keeps advisory locks to migration only.
  - Dedicated `nonce_lease` row: rejected — a second coordination object, and 006 cannot be changed
    to take it (006 is frozen upstream), so recovery-establish ordering would be lost.
  - `SERIALIZABLE` isolation: rejected — retry/serialization-failure handling for no additional
    guarantee over row lock + post-lock rechecks (same conclusion as 006/007).
  - Scope row `FOR UPDATE` as the primary lock: rejected as the *sole* mechanism — it serializes
    allocations per scope but gives no ordering against 006; the coordination row subsumes it.

## R2 — Allocation semantics: durable frontier + explicit classification (no max-value shortcut)

- **Decision**: candidate selection is durable-state-driven, computed inside the locked tx:
  - Let `M` = max bound nonce for the scope in `nonce_bindings` (NULL if none), `F` =
    `reconciled_floor` for the scope (NULL until a reconcile release sets it),
    `L`/`P` = the fresh chain observation's `latest`/`pending` transaction counts for the sender.
  - `candidate = max(M+1, F)` when `M`/`F` exist; for a first-time scope (no bindings, no floor)
    `candidate = P` **only** through the explicit bootstrap path below. `P` is never silently
    adopted as the next nonce (the forbidden max-value shortcut).
  - Bootstrap (no local records): requires a fresh consistent observation. `P > 0` records a
    durable `bootstrap_external_consumed` observation covering `[0, P)` (explicit, auditable,
    never silent) and allocates at `P`; `L > P` → divergence → refuse. No hold is created on the
    healthy bootstrap path — a sender's pre-existing chain history is normal onboarding, and a
    durable evidence row is what "不静默并入" requires.
  - Existing scope, `P > M+1` → classification: `L > M+1` → `unattributed_consumption`;
    otherwise `unexplained_gap` → **establish a 008 hold** (evidence-linked, Section contracts/
    observation.md) and refuse the allocation fail-closed. The consumed range is never adopted
    into the sequence without a controlled reconcile (R7).
  - Existing scope, `P <= M+1` → `consistent`; the candidate is taken from durable state
    (`max(M+1, F)`), never from the chain view.
  - Observation unavailable (transport/timeout/rate-limit/invalid/chain-mismatch) → refuse
    (`chain_view_unavailable`), persist the failed observation as evidence, change no domain
    state. A binding is never allocated without a fresh verified view (FR-15 fail-closed;
    US4-3 evidence discipline).
  - After the candidate decision, the binding insert itself is arbitrated by
    `CONSTRAINT nonce_bindings_scope_nonce_uniq` (last-resort backstop; classification in R3).
- **Rationale**: the spec forbids "静默复用、跳过或覆盖" (FR-10/FR-11/FR-12). Deriving the next
  nonce from `max(local, chain)` alone would silently absorb external consumption and unexplained
  gaps; deriving it from durable bindings alone plus classification keeps every chain-side fact
  recorded as evidence and routes ambiguous states to a hold. `P > M+1` is exactly the detectable
  "chain consumed what we cannot attribute" condition; `L > M+1` is the mined (stronger) form.
- **Alternatives considered**:
  - `candidate = max(M+1, P)` unconditionally: rejected — the max-value shortcut; silently merges
    external consumption (FR-12 violation).
  - `candidate = M+1` always, ignore the chain: rejected — external consumption of `M+1` would be
    detected only at broadcast time, after signing; FR-12 requires recording consumption, and
    FR-10 requires divergence signals.
  - Allocate from `L+1` (latest) only: rejected — pending external transactions would be
    double-allocated.
  - Hold the scope on bootstrap when `P > 0`: rejected — would require an operator release for
    every first use of any already-used sender; the spec's release path is for unexplained/
    unsafe states, and bootstrap external history is explainable by construction (the registry
    sender predates 008). Recorded explicitly so the choice is auditable.
- **Residual (stated, not hidden)**: the chain can change between the pre-tx observation and the
  commit; 008 does not claim atomicity with the chain. Downstream gates (009/010 broadcast
  re-verification, 006 pause precedence) and the reconcile observer re-check before any side
  effect; an external consumption landing in that window surfaces as a later observation
  (`unattributed_consumption`) and a hold.

## R3 — Identity and idempotency carriers: intent-unique and scope-nonce-unique, insert-first

- **Decision**: allocation is keyed by the durable `intent_id`. Inside the locked tx, 008 first
  reads `nonce_bindings WHERE intent_id = $1`:
  - hit + full allocation-input equality (`chain_id`, `sender`, `authorization_id`) → **replay**:
    return the original binding unchanged (no new row, no state change);
  - hit + any differ → **refuse** (fail-closed conflict; no second binding is ever created);
  - miss → compute candidate (R2), insert with the UNIQUE carriers.
  Constraints (exact names, declared in the migration): `nonce_bindings_pkey (binding_id)`,
  `CONSTRAINT nonce_bindings_intent_uniq UNIQUE (intent_id)`,
  `CONSTRAINT nonce_bindings_scope_nonce_uniq UNIQUE (chain_id, sender, nonce)`. A `23505` aborts
  the tx: ROLLBACK, then a fixed-order classify (by intent first, then scope-nonce) off the pool;
  a classify miss is retryable, never fabricated. Mechanism mirrors 007 R6/R8 (insert-first,
  `pgconn.PgError.ConstraintName` exact match, no untargeted `ON CONFLICT`).
- **Rationale**: FR-05's replay and OC-3's "同一 intent MUST NEVER 获得第二绑定 / 同一
  (chain_id, sender, nonce) MUST NEVER 绑定另一 intent" are exactly two UNIQUE carriers; the
  persistence layer decides, not application memory (原则 II). Concurrency between two callers
  for the same intent converges on the second carrier: the loser's in-tx re-read (serialized by
  R1) or its post-23505 classify returns the winner's binding.
- **Alternatives considered**: `ON CONFLICT DO NOTHING RETURNING` (returns nothing on conflict;
  classify still needed — 007 R6); `ON CONFLICT DO SELECT` (PG19 only, pinned PG18.6);
  app-level check-then-insert (not atomic; forbidden by 原则 II).

## R4 — Chain-view evidence: two counts + head identity, classified, persisted, outside tx

- **Decision**: each observation performs (outside any DB transaction, bounded by the existing
  `IndexRPCTimeout`/retry knobs, no new timing knobs): `eth_getTransactionCount(sender, "latest")`,
  `eth_getTransactionCount(sender, "pending")`, `eth_getBlockByNumber("latest")` for head
  number+hash. Error classes are the existing eth client classification (transport / timeout /
  rate-limit / invalid response / chain mismatch); each failed observation is still persisted with
  its error class as `unavailable` evidence. Persisted classifications: `consistent`,
  `bootstrap_external_consumed`, `unattributed_consumption`, `unexplained_gap`, `divergence`
  (`L > P`, or a `pending` regression versus the scope's previous observation), `unavailable`.
  Observation sequence: the allocation path observes immediately before its tx; a reconcile
  observer observes each known scope periodically (reusing `IndexPollInterval`) and drives
  `in_flight`/`consumed` transitions (R6). One observation row is one attempt; rows are never
  updated.
- **Rationale**: `latest` distinguishes mined from pending consumption; `pending` is the
  collision-avoidance floor; head identity makes every classification traceable to a specific
  chain point (FR-21 evidence, SC-06 100 % reconcile records). Keeping RPC outside tx matches
  every existing writer and keeps the 5s statement guard meaningful.
- **Alternatives considered**: single `pending` read (no mined/pending distinction; weakens
  external-consumption classification); reading `eth_getTransactionByHash` per binding (no tx
  identity is knowable to 008 without 010 linkage; deferred); trusting provider quantities without
  head identity (no evidence anchor).

## R5 — Restart recovery: durable reads are the rebuild; a verified readiness gate fronts serving

- **Decision**: 008 holds **no in-memory authoritative state**. Every allocation/release/observation
  decision re-reads its durable basis inside the locked tx, so a restart loses nothing. On startup
  the service runs a rebuild verification (aggregate integrity read per known scope: bindings
  present and constraint-consistent, holds readable, scope floors readable) and only then opens
  the allocation admission gate; before it completes, allocation requests are refused fail-closed
  with `rebuild_incomplete` recorded. Verification failure refuses to open the gate (no silent
  "repair", no memory guessing).
- **Rationale**: FR-13 requires rebuild from persisted records and fail-closed allocation until it
  is complete. Because the database is the authority, "rebuild" is a verification + readiness gate,
  not a data reload — which is the strongest form of the requirement (a reload could itself be
  wrong; a read-through cannot be stale in the same way). This mirrors 006's re-read-don't-trust
  discipline.
- **Alternatives considered**: eager in-memory reload of frontiers (creates a second truth that
  can drift; still needs the fail-closed gate); no gate (violates FR-13's "重建完成前 MUST NOT
  分配").

## R6 — Binding state machine and unknown-outcome retention

- **Decision**: states `allocated` → `in_flight` → `consumed` | `released`; direct
  `allocated → consumed` and `allocated|in_flight → released` are allowed; `consumed`/`released`
  are terminal and never leave. Transitions and triggers:
  - `allocated → in_flight`: observation shows `nonce ∈ [L, P)` (a pending transaction consumes
    it; outcome unknown) — persistent "结果未知", never auto-failed, never reassigned;
  - `allocated|in_flight → consumed`: observation shows `nonce < L` (mined, by us or externally);
  - `allocated|in_flight → released`: **operator-only** binding disposition with explicit evidence
    (R7) proving no external side effect is outstanding; timeouts or connection errors alone are
    never evidence (US3-3);
  - every transition appends `nonce_binding_events` (append-only) with the observation id or
    operator operation id as evidence.
  Chain observation never assigns to another intent: nonces are never reassigned to any other
  binding (full UNIQUEs; release does not free a nonce for reuse — OC-3's "MUST NEVER" is
  structural). Replacement attempts (OC-4, new `attempt_id`/`signing_request_id` under the same
  intent) reference the same binding identity; 008 never creates a second intent claim.
- **Rationale**: FR-07/FR-09 and the spec Key Entities lifecycle ("已预留 → 已发起外部副作用
  (结果已知/未知) → 终态 已消耗 | 经对账释放; 结果未知为持续态"). Mapping "已发起外部副作用"
  onto observable evidence (chain view) keeps 008 inside its boundary: 008 does not sign or
  broadcast, so it cannot know a side effect was emitted except through chain evidence (or, in a
  later integration, 010's attempt linkage — deferred, contracts/downstream.md).
- **Alternatives considered**: auto-release of `in_flight` after a timeout (forbidden: FR-08,
  US3-3); auto-recycle of `allocated` bindings (forbidden: FR-08); states reflecting signature
  outcomes owned by 009/010 (out of boundary — 008 keeps binding-level facts only).

## R7 — Reconciliation: auto-established holds, operator-only release, evidence standard, multi-cause

- **Decision**: a 008 hold is a durable row per cause (`nonce_scope_holds`), scoped to one
  `(chain_id, sender)`; a scope with any `active` hold refuses new allocations. Establishment is
  automatic from classification (`unattributed_consumption`, `unexplained_gap`, and `divergence`
  when it leaves the scope in an unsafe state), always with an observation-id evidence link and an
  append-only event. Release is **operator-only**, via the existing controlled operator carrier
  (new subcommand `txharbor nonce-admin hold-release`, mirroring `confirm-auth`/`withdrawal-authz`
  shape: flag parsing → `config.Load` → operator-connection tx, `--operation-id` required, minted
  first; exit codes 0/1/2):
  - scope authorization: `--chain-id` must equal the deployment chain, `--sender` must name the
    exact registry sender, `--hold-id` names exactly one hold; no role is added;
  - evidence standard: the operator supplies `--evidence` (free-text finding) and evidence
    references (`--observation-id` of the observation the remedy is based on); the service
    re-verifies **inside the locked tx** with a fresh pre-tx observation classified `consistent`
    (or the specific remediation the cause demands), confirms no unresolved conflicts in the
    scope, then clears **only the named hold** (`active → released` + operator/reason/evidence/
    operation id recorded);
  - refusal is a committed outcome (recorded, zero partial effect): evidence missing, re-read
    failure, re-verification failure, fresh observation still classifying the cause, or
    insufficient attribution → MUST NOT release; "修复完成 MUST NOT 自动等于解除" — no path
    releases without this command;
  - multi-cause: other holds (including other cause rows in the same scope and 006's recovery
    pause) survive untouched; the scope stays held while any other active hold remains;
  - 006 coexistence: 008 never reads-with-intent-to-write, never writes, never deletes 006 rows;
    release during active 006 recovery is allowed for the 008 cause only and the recorded audit
    includes the observed 006 state; allocation remains blocked by the 006 gate independently.
  - `nonce-admin binding-release` (binding disposition to terminal `released`) follows the same
    carrier/evidence discipline per binding.
- **Rationale**: OC-6/OC-7 (Round 2) fix exactly this: operator path reused, no new roles, scope
  limited to `(chain_id, sender)`, evidence conditions, only the specified 008 cause cleared,
  fixes do not auto-release. The re-verify-under-lock shape is the repo's "持锁后重读复核，失配
  回滚" discipline (006 FR-20), here applied to release-vs-observer/allocation concurrency.
- **Alternatives considered**: automatic release when a later observation looks consistent
  (forbidden — silent release; also "修复完成 MUST NOT 自动等于解除"); releasing all holds for a
  scope at once (forbidden — only the specified cause); a new dedicated role/approval (forbidden —
  MUST NOT 新增角色); editing 006 rows to lift the pause (forbidden — 006 owns them).

## R8 — 009 read contract carrier: authenticated HTTP JSON, read-only, single snapshot, 5 outcomes

- **Decision**: the provider side is an authenticated read-only HTTP JSON interface mounted on the
  existing HTTP server (007 precedent), authenticated by a service bearer credential held in
  deployment env (`TXHARBOR_NONCE_READ_TOKEN`, constant-time compare; not a business caller key;
  never logged; rotation = env change + restart; no key table in 008). Endpoints: lookup by
  `binding_id` and by `intent_id` (with optional expected `chain_id`/`sender` cross-check). Every
  request opens one `REPEATABLE READ` read-write transaction that acquires only `SELECT … FOR
  SHARE` locks and writes zero data (never `READ ONLY`, which rejects the lock; business semantics
  stay read-only — see contracts/read-api.md §4), reads the
  binding + scope holds + (for annotation only) the 006 recovery state in the same snapshot, and
  returns exactly one of **five outcomes** (contracts/read-api.md): `bound`, `terminal`,
  `not_bound`, `mismatch`, `unavailable`. Pauses are annotations on `bound` (OC-6: paused
  bindings MAY be returned read-only with explicit annotation), never a mutation; reads never
  create, change, release, or reassign anything. The response always distinguishes facts from
  permission: a fixed `notice` field states that a returned binding is not a signature/broadcast
  authorization, and `gate` reports only 008-owned/read-observed gate facts.
- **Rationale**: OC-3 requires an explicit read-only contract with a stable binding identity,
  `(intent_id, chain_id, sender, nonce, state)` facts; the spec's Downstream Handoff names it for
  009. HTTP JSON rides the existing server and auth middleware patterns without new
  infrastructure; a shared database access for 009 would couple schemas and contradict signer
  isolation (原则 VIII boundary discipline), and gRPC/IPC adds dependencies. The 5-outcome shape
  makes every consumer decision (proceed / hold / refuse / retry) expressible without reading 008
  internals.
- **Alternatives considered**: 009 reads 008 tables directly (rejected — implicit contract, schema
  coupling, no permission boundary); gRPC (no infra/dep budget); a database view with a separate
  read-only role (rejected — 008 exposes semantics, not storage; role separations do not exist in
  this repo today and would be claimed, not implemented); event/queue notification (new infra,
  forbidden).

## R9 — Controlled wallet registry: durable table + operator supply, versioned and auditable

- **Decision**: senders are registered in `nonce_wallet_registry` (per deployment chain: `state ∈
  {active, disabled}`, monotonic `registry_seq`, timestamps), supplied/changed only through
  `txharbor nonce-admin register|disable` (same operator carrier; `--operation-id` minted first;
  append-only `nonce_ops_audit`; attempt dedup by the named UNIQUE). Allocation consults the
  registry **inside the locked tx**: unknown sender, disabled, or unreadable row → refuse and
  preserve existing records. The registry is the authority (OC-2); the caller's sender claim is
  verified against it, never trusted. Each binding stores the `registry_seq` in force at
  admission; a later config change never alters an existing binding's sender/nonce (the binding
  is a fact; the registry gates admission only).
- **Rationale**: FR-01/OC-2 require controlled, auditable config changes with disable semantics
  and explicit non-retroactivity. The repo's controlled-change precedent is a history/version
  table + operator command (004 config history, 005/006 policy history, 007 grant supply); env
  config cannot express per-entry disable with durable audit. FK `(chain_id, sender)` from
  bindings keeps storage-level integrity.
- **Alternatives considered**: env-only sender list (`TXHARBOR_NONCE_SENDERS`): rejected — no
  per-entry disable state, no durable audit, changes require restarts and leave no evidence,
  failing "配置变更 MUST 受控且可审计"; a mutable boolean without a version: rejected — no
  re-verifiable version for audit ("版本" required).

## R10 — Authorization binding: reuse 007's approved carrier read-only; store identity + version

- **Decision**: the allocation input carries `authorization_id`; 008 validates it read-only against
  007's `withdrawal_authorizations` carrier (the approved OC-5 carrier, no new authorization
  model): row present, `state = 'active'`, not expired by DB clock (`expires_at IS NULL OR
  expires_at > clock_timestamp()`), `chain_id` equal to the binding scope. On success the binding
  stores `authorization_id` + `authorization_version` = deterministic SHA-256 hex over the
  authorization's binding-relevant canonical fields at admission (id, caller, chain, asset,
  recipient, amount, state, expires_at). 008 never writes 007 tables, never consumes or extends a
  grant (007 replay/one-grant semantics untouched), and the fail-closed rule applies: missing/
  inactive/expired/mismatched/unreadable → no new binding. The version is what 009/011 can compare
  against a current authorization read; 008 never re-derives it.
- **Rationale**: OC-5 requires explicit per-transaction authorization, fail-closed, and "008 的
  预留同样 MUST 绑定授权"; "授权载体与语义 MUST 优先复用 007 已批准方案，缺口 MUST 显式记录,
  MUST NOT 静默改写 007". Read-only reuse of Table 4 plus a recorded version satisfies both.
- **Alternatives considered**: a 008-owned authorization table (rejected — a second authorization
  model, forbidden by OC-5's reuse rule); storing only the id (rejected — no version evidence for
  OC-6's "记录授权版本"); trusting the caller's validity claim (rejected — 008 MUST 校验授权).
- **Recorded gap (not silently rewritten)**: intent↔request↔authorization linkage is 011-owned
  (OC-1) and its tables do not exist yet; 008 validates grant-level validity + chain and records
  the identity. The cross-check of the linkage is deferred to the real 011 integration
  (contracts/downstream.md), explicitly marked as deferred acceptance.

## R11 — Observability and redaction

- **Decision**: structured logs through the existing `logx` funnel with `chain_id`, `sender`,
  `nonce`, `binding_id`, `intent_id`, `hold_id`, `cause`, observation classification, RPC endpoint
  class, retry class, `registry_seq`; never key material, never the read token, never credentials
  (`logx.Redact`). Metrics ride the existing registry: allocation results (`allocated`, `replayed`,
  `refused_*`), active hold gauge and hold events by cause/action, observation classifications,
  read outcomes, release outcomes. Allocator health does not flip service readiness (mirrors 006's
  observer: zero readiness effect), but rebuild-gate state is logged and exposed as a metric.
  Every refusal path records its cause (FR-22, SC-05/SC-06).
- **Rationale**: 原则 XII ("not cosmetic final-stage additions"); evidence classes are already in
  the design, so exposing them costs nothing extra. No new metrics subsystem.
- **Alternatives considered**: logging raw RPC payloads (rejected — unbounded raw data, security
  rules); a new dashboard/endpoint (no new infra; observability contract is fields + metrics).

## R12 — Test resource isolation and the deferred integration boundary

- **Decision**: automated integration/E2E tests self-provision PostgreSQL and Anvil via
  testcontainers (repo precedent), never the shared compose stack/`pgdata`; any manual local run
  in this workdir uses workdir-local names and non-default ports (database/schema `txharbor_008`,
  PostgreSQL `127.0.0.1:55432`, Anvil `127.0.0.1:58545`, distinct compose project/volume) so the
  sibling 009 workdir cannot collide (workflow R4). Chain-facing unit work may use fake RPC/test
  doubles **for early development only**; final concurrency/restart/recovery acceptance MUST run
  against real PostgreSQL and real Anvil (原则 XI — mocks MUST NOT substitute). Real 009/010/011
  integration acceptance is explicitly **deferred to post-dependency**; this plan defines the
  interface contracts and the fixture scheme now (contracts/downstream.md) so those tests can be
  written without redesign.
- **Rationale**: 原则 X/XI + workflow R4/R5; the orchestrator instruction forbids starting any
  service in this step, and R5 says test doubles cannot replace final integration acceptance.
- **Alternatives considered**: sharing the compose stack (forbidden by R4 coordination);
  skipping interface contracts until 009–011 exist (would force a redesign; the parallel window
  exists precisely to close contracts now).

## R13 — Numeric representation and bounds

- **Decision**: nonce and all EVM quantities are integer-only: PostgreSQL `NUMERIC(78,0)` with
  `CHECK` bounds, Go `math/big` in code; nonce column bound `0 ≤ nonce ≤ 2^64−1` (EVM account
  nonce range), counts parsed from JSON-RPC hex with explicit overflow refusal. No `float64`
  anywhere; no saturation arithmetic as audit basis. `now()`/`clock_timestamp()` are the only
  clocks for validity decisions.
- **Rationale**: 原则 I (no floats), spec Edge Cases (nonce 0 / maximum boundary must not break
  uniqueness; representation left to plan), repo convention (`NUMERIC(78,0)` in 000004–000007).
- **Alternatives considered**: `BIGINT` (overflows the EVM uint64 nonce range at 2^63; NUMERIC is
  the repo's lossless convention); `bytea`/hex text storage (loses numeric ordering/comparison).
