# Data Model: 008-nonce-manager

**Branch**: `008-nonce-manager` | **Date**: 2026-09-16 | **Planned migration**:
`migrations/000008_nonce_manager.sql` (goose, new file; 000001–000007 untouched in meaning;
`migrations/embed.go` auto-includes `*.sql`). This document is **design** — the migration and any
implementation are Phase 2+ work; nothing here is created or executed by this step.

All decisions: research.md R1–R13. Business semantics (OC-1–OC-7) are spec inputs, not re-opened.
Conventions follow 004–007: lowercase `0x`-prefixed hex (`~ '^0x[0-9a-f]{40}$'` / 64-hex for
hashes), `NUMERIC(78,0)` integers with literal `CHECK` bounds, explicitly **named** constraints
for every carrier consumed by `ConstraintName` classification, append-only audit/evidence rows,
`TIMESTAMPTZ DEFAULT now()`, pure DDL (no stored functions; logic lives in the app's
parameterized SQL). No floats (原则 I, R13).

**Ownership**: 008 owns and may write only its seven tables below. 006/007 tables are read-only
inputs (`indexer_pause`/`log_pause`/`deposit_pause`, `reorg_recovery`, `reorg_recovery_events`,
`withdrawal_authorizations`); 008 never inserts, updates, or deletes an upstream row — resetting
or lifting an upstream pause is upstream's own operator path.

## Canonical allocation input (validated before any write)

| Field | Form | Validation (all fail-closed) |
|---|---|---|
| `intent_id` | opaque TEXT, 1–128 ASCII `0x21–0x7E` | required; stable identity created by 011 (OC-1); 008 never creates an intent; uniqueness via `nonce_bindings_intent_uniq` |
| `chain_id` | decimal BIGINT > 0 | required; MUST equal the deployment chain and the registry scope |
| `sender` | lowercase `0x` + 40 hex | required; verified against `nonce_wallet_registry` (authoritative, OC-2) inside the locked tx; caller claim is never trusted |
| `authorization_id` | opaque TEXT, 1–128 printable | required; validated read-only against `withdrawal_authorizations` (R10): row exists, `state='active'`, not expired by DB clock, chain matches |

The same four fields are the full input-equality set for replay/conflict classification (R3).
`registry_seq` for the binding is taken from the registry row read under the lock, never from the
caller.

## Table 1 — `nonce_wallet_registry` (sender authority per chain; OC-2)

| Column | Type | Constraints | Notes |
|---|---|---|---|
| chain_id | BIGINT | `NOT NULL CHECK (> 0)`, PK part | single deployment, chain-scoped |
| sender | TEXT | `NOT NULL CHECK ~ '^0x[0-9a-f]{40}$'`, PK part | lowercase canonical |
| state | TEXT | `NOT NULL CHECK IN ('active','disabled')` | admission gate only; never rewrites existing bindings |
| registry_seq | BIGINT | `NOT NULL CHECK (> 0)` | monotonic version, bumped per change (R9) |
| created_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |
| updated_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |

`CONSTRAINT nonce_wallet_registry_pkey PRIMARY KEY (chain_id, sender)`.
FK target for Tables 2/3/5/6. No delete path (rows never removed; `disabled` is the off state).

## Table 2 — `nonce_scope_state` (durable per-scope frontier / reconcile floor)

| Column | Type | Constraints | Notes |
|---|---|---|---|
| chain_id | BIGINT | `NOT NULL CHECK (> 0)`, PK part | |
| sender | TEXT | `NOT NULL CHECK ~ '^0x[0-9a-f]{40}$'`, PK part | |
| reconciled_floor | NUMERIC(78,0) | `CHECK (reconciled_floor >= 0 AND reconciled_floor <= 18446744073709551615)` (2⁶⁴−1), NULL before any release | only a `nonce-admin hold-release` may set it; monotonic (guarded: new ≥ old) |
| last_latest | NUMERIC(78,0) | `CHECK ≥ 0 AND ≤ 2⁶⁴−1`, NULL | last observed `latest` count (regression detection) |
| last_pending | NUMERIC(78,0) | `CHECK ≥ 0 AND ≤ 2⁶⁴−1`, NULL | last observed `pending` count (regression detection) |
| last_observation_id | TEXT | NULL | most recent observation for this scope |
| updated_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |

`CONSTRAINT nonce_scope_state_pkey PRIMARY KEY (chain_id, sender)`,
`CONSTRAINT nonce_scope_state_registry_fkey FOREIGN KEY (chain_id, sender) REFERENCES
nonce_wallet_registry (chain_id, sender)`. Row is created lazily by the first observation or
allocation; it is a durable cache of evidence-linked facts, never an authority over bindings
(bindings are the frontier of record — `M` is always recomputed from Table 4 under the lock).

## Table 3 — `nonce_bindings` (the durable binding; core invariant carrier)

| Column | Type | Constraints | Notes |
|---|---|---|---|
| binding_id | TEXT | PK; stable public identity | opaque (`nb-` + 32 hex), never reused |
| intent_id | TEXT | `NOT NULL UNIQUE`, 1–128 printable | durable intent identity (OC-1); one binding per intent **forever** |
| chain_id | BIGINT | `NOT NULL CHECK (> 0)` | scope |
| sender | TEXT | `NOT NULL CHECK ~ '^0x[0-9a-f]{40}$'` | scope; fixed at admission, never changed by later config |
| nonce | NUMERIC(78,0) | `NOT NULL CHECK (nonce >= 0 AND nonce <= 18446744073709551615)` | EVM uint64 account-nonce range (R13) |
| state | TEXT | `NOT NULL CHECK IN ('allocated','in_flight','consumed','released')` | explicit state machine (Table 6 below) |
| authorization_id | TEXT | `NOT NULL` | OC-5 binding; read-only reuse of the 007 carrier (R10) |
| authorization_version | CHAR(64) | `NOT NULL CHECK ~ '^[0-9a-f]{64}$'` | SHA-256 hex of the authorization's binding-relevant fields at admission (R10) |
| registry_seq | BIGINT | `NOT NULL CHECK (> 0)` | registry version in force at admission (R9) |
| allocation_observation_id | TEXT | `NOT NULL` | evidence anchor of the admission (Table 5) |
| created_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | admission commit time (008 `now()`) |
| updated_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |
| consumed_at | TIMESTAMPTZ | NULL | set with `state='consumed'` |
| released_at | TIMESTAMPTZ | NULL | set with `state='released'` |
| release_operation_id | TEXT | NULL | operator operation that disposed the binding |

Named carriers (exact `ConstraintName` values — never rely on PG auto-naming):
- `CONSTRAINT nonce_bindings_pkey PRIMARY KEY (binding_id)`
- `CONSTRAINT nonce_bindings_intent_uniq UNIQUE (intent_id)` — "同一 intent MUST NEVER 获得第二绑定"
- `CONSTRAINT nonce_bindings_scope_nonce_uniq UNIQUE (chain_id, sender, nonce)` — "同一
  (chain_id, sender, nonce) MUST NEVER 绑定另一 intent"
- `CONSTRAINT nonce_bindings_registry_fkey FOREIGN KEY (chain_id, sender) REFERENCES
  nonce_wallet_registry (chain_id, sender)` — only a registered sender can receive bindings;
  `disabled` does not delete the registry row, so existing bindings never dangle
- `CONSTRAINT nonce_bindings_state_check CHECK (state IN (...))`
- `CONSTRAINT nonce_bindings_terminal_consistency CHECK ((state = 'consumed') = (consumed_at IS
  NOT NULL) AND (state = 'released') = (released_at IS NOT NULL AND release_operation_id IS NOT
  NULL))` — pending states carry no terminal facts
- `CONSTRAINT nonce_bindings_nonce_range CHECK (nonce >= 0 AND nonce <= 18446744073709551615)`
- `CONSTRAINT nonce_bindings_intent_shape CHECK (length(intent_id) BETWEEN 1 AND 128 AND
  intent_id ~ '^[\x21-\x7e]+$')`

There is **no** `sender` or `nonce` update path in any transaction: the binding is immutable
except for `state` (+ terminal columns). Nonces are never reassigned after release (full UNIQUE
carriers stay in force; OC-3 "MUST NEVER" is structural).

## Table 4 — `nonce_binding_events` (append-only state-transition log)

| Column | Type | Constraints | Notes |
|---|---|---|---|
| event_id | BIGINT | PK `GENERATED ALWAYS AS IDENTITY` | |
| binding_id | TEXT | `NOT NULL` (no FK — audit outlives nothing but constrains nothing) | mirrors 006 event-log shape |
| from_state | TEXT | `CHECK IN ('allocated','in_flight','consumed','released')`, NULL for the creation event | |
| to_state | TEXT | `NOT NULL`, same domain | |
| observation_id | TEXT | NULL | chain-evidence transition anchor |
| operation_id | TEXT | NULL | operator disposition anchor |
| detail | TEXT | `NOT NULL DEFAULT ''` | redacted, no secrets |
| at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |

`CONSTRAINT nonce_binding_events_binding_to_uniq UNIQUE (binding_id, to_state)` — a binding
enters each destination state at most once; a duplicate transition (repeat observation) converges
on the constraint instead of duplicating history (006 transition-log idiom).

## Table 5 — `nonce_observations` (chain-view evidence; one row per attempt, never updated)

| Column | Type | Constraints | Notes |
|---|---|---|---|
| observation_id | TEXT | PK | opaque (`no-` + 32 hex) |
| chain_id | BIGINT | `NOT NULL CHECK (> 0)` | |
| sender | TEXT | `NOT NULL CHECK ~ '^0x[0-9a-f]{40}$'` | scope |
| kind | TEXT | `NOT NULL CHECK IN ('allocation','reconcile')` | why the observation was made |
| classification | TEXT | `NOT NULL CHECK IN ('consistent','bootstrap_external_consumed','unattributed_consumption','unexplained_gap','divergence','unavailable')` | R2/R4 matrix |
| latest_count | NUMERIC(78,0) | NULL, `CHECK ≥ 0 AND ≤ 2⁶⁴−1` | NULL when the read failed |
| pending_count | NUMERIC(78,0) | NULL, same check | NULL when the read failed |
| head_number | NUMERIC(78,0) | NULL | head identity of the read point |
| head_hash | TEXT | NULL `CHECK ~ '^0x[0-9a-f]{64}$'` | |
| error_class | TEXT | `NOT NULL DEFAULT ''` | transport / timeout / rate_limited / invalid_response / chain_mismatch / rpc_unavailable |
| rpc_ref | TEXT | `NOT NULL DEFAULT ''` | redacted endpoint alias, never a credential |
| observed_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |

Index (access pattern): `(chain_id, sender, observed_at)`. Rows are evidence and are never
deleted or rewritten.

## Table 6 — `nonce_scope_holds` (008-owned reconciliation pauses; one row per cause instance)

| Column | Type | Constraints | Notes |
|---|---|---|---|
| hold_id | TEXT | PK | opaque (`nh-` + 32 hex) |
| chain_id | BIGINT | `NOT NULL CHECK (> 0)` | scope |
| sender | TEXT | `NOT NULL CHECK ~ '^0x[0-9a-f]{40}$'` | scope (OC-6: 限定具体 `(chain_id, sender)`) |
| cause | TEXT | `NOT NULL CHECK IN ('unattributed_consumption','unexplained_gap','chain_view_divergence')` | 008 原因 only; 006 causes are never represented here |
| status | TEXT | `NOT NULL CHECK IN ('active','released')` | |
| established_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |
| evidence_observation_id | TEXT | `NOT NULL` | establishment evidence (Table 5) |
| evidence_detail | TEXT | `NOT NULL DEFAULT ''` | classification summary, redacted |
| released_at | TIMESTAMPTZ | NULL | |
| released_by | TEXT | NULL | declared operator identity |
| release_operation_id | TEXT | NULL | operator operation |
| release_evidence | TEXT | NULL | operator finding + re-verification summary |
| release_observation_id | TEXT | NULL | fresh observation used by the re-verify-to-release |
| CONSTRAINT nonce_scope_holds_status_consistency CHECK ((status = 'active') = (released_at IS NULL)) | | | |
| CONSTRAINT nonce_scope_holds_registry_fkey FOREIGN KEY (chain_id, sender) REFERENCES nonce_wallet_registry (chain_id, sender) | | | |

Multi-cause coexistence is by construction: several `active` rows may exist for one scope; a
release flips exactly the named `hold_id` to `released`. A scope is admission-held iff at least
one `active` row exists. There is no scope-level boolean to get out of sync.

## Table 7 — `nonce_ops_audit` (operator-operation audit + attempt dedup)

| Column | Type | Constraints | Notes |
|---|---|---|---|
| audit_id | BIGINT | PK `GENERATED ALWAYS AS IDENTITY` | one row per attempt |
| operation_id | TEXT | `NOT NULL`; uniqueness ONLY via the named constraint below | caller-minted before the command (R7); the only dedup key |
| action | TEXT | `NOT NULL CHECK IN ('registry_register','registry_disable','hold_release','binding_release')` | operator vocabulary |
| chain_id | BIGINT | `NOT NULL CHECK (> 0)` | |
| sender | TEXT | `NOT NULL CHECK ~ '^0x[0-9a-f]{40}$'` | scope authorization (OC-6) |
| subject_id | TEXT | `NOT NULL DEFAULT ''` | hold_id / binding_id / sender per action |
| outcome | TEXT | `NOT NULL CHECK IN ('applied','nop','refused')` | refusals are recorded outcomes, never re-executed |
| operator | TEXT | `NOT NULL DEFAULT ''` | declared identity (audit claim; trust root is DSN possession, R7) |
| reason | TEXT | `NOT NULL DEFAULT ''` | |
| evidence | TEXT | `NOT NULL DEFAULT ''` | operator finding + evidence refs (redacted) |
| detail | TEXT | `NOT NULL DEFAULT ''` | outcome detail / re-verification summary |
| recorded_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |

`CONSTRAINT nonce_ops_audit_pkey PRIMARY KEY (audit_id)`;
`CONSTRAINT nonce_ops_audit_operation_id_uniq UNIQUE (operation_id)` (23505 → rollback → re-read
by operation id → compare op-input → equal: report recorded outcome; differ: `operation_conflict`,
zero writes — 007 grant-audit protocol, R7).

## Binding state machine (Table 3 `state`)

| From | To | Trigger | Evidence written | Notes |
|---|---|---|---|---|
| — | `allocated` | allocation tx commits (T-allocate) | binding row + event (`from_state NULL`) + allocation observation | admission = 008's delivery point (OC-7 mapping) |
| `allocated` | `in_flight` | reconcile observation: `L <= nonce < P` | event + observation | persistent "结果未知"; never auto-failed, never reassigned |
| `allocated` | `consumed` | reconcile observation: `nonce < L` | event + observation | mined consumption (ours or external) |
| `in_flight` | `consumed` | reconcile observation: `nonce < L` | event + observation | resolves the unknown outcome |
| `allocated` | `released` | `nonce-admin binding-release` with evidence | event + ops audit | operator disposition; proof no external side effect |
| `in_flight` | `released` | `nonce-admin binding-release` with stronger evidence | event + ops audit | allowed only with explicit no-side-effect evidence; otherwise the item MAY remain in-flight (OC-6) |
| `consumed` / `released` | — | terminal | — | CHECK-enforced; no path out (invalid transitions rejected) |

No automatic transition ever produces `released`; no timeout/connection error alone ever counts
as "no side effect" (US3-3). "已签名/已广播/结果未知的 nonce 绝不再分配" holds structurally
(nonce immutability + full UNIQUEs).

## Observation classification matrix (per scope; computed under the lock, R2/R4)

`M` = max binding nonce (NULL if none), `F` = `reconciled_floor` (NULL if unset), `L`/`P` =
observed latest/pending counts, `P_prev` = `last_pending`.

| Condition | Classification | Admission effect |
|---|---|---|
| read failed (any RPC error class) | `unavailable` | refuse (`chain_view_unavailable`); observation persisted; no state change |
| `L > P` or `P < P_prev` | `divergence` | establish `chain_view_divergence` hold; refuse |
| no bindings, no floor, `P = 0` | `consistent` | candidate `0` |
| no bindings, no floor, `0 < P`, `L <= P` | `bootstrap_external_consumed` | record `[0, P)` as externally consumed evidence; candidate `P`; no hold |
| bindings exist, `P <= M+1` | `consistent` | candidate `max(M+1, F)` |
| bindings exist, `P > M+1`, `L > M+1` | `unattributed_consumption` | establish hold; refuse |
| bindings exist, `P > M+1`, `L <= M+1` | `unexplained_gap` | establish hold; refuse |

`candidate` is never `max(M+1, P)`-derived on the `P > M+1` branch — the scope is held and only a
reconcile release (setting `F = observed P`) can resume allocation above the consumed range.
`F` monotonically advances: guarded `UPDATE … SET reconciled_floor = GREATEST(reconciled_floor,
$new) WHERE …`.

## Transaction catalog (behavioral; SQL belongs to implementation)

All write transactions: `BEGIN → writeGuard ('SET LOCAL statement_timeout = ''5s''', per-statement
bound only) → ensureLeaseSQL → lockCoordSQL (FOR UPDATE, R1) → post-lock rechecks → point writes
with `RowsAffected` checks → COMMIT`. Zero RPC inside any transaction. RPC observations happen
immediately before the tx and are passed in as data.

- **T-allocate** (first admission): pre-tx validate input → RPC observation → BEGIN → lock →
  recheck 006 gates (three stream pause rows + active `reorg_recovery` row; any hit → refuse,
  rollback, zero writes) → recheck registry (`active`) → recheck active holds → re-read binding by
  `intent_id` (hit + equality → T-replay; hit + differ → T-conflict) → compute `M`/`F` from
  bindings under lock → apply classification matrix → insert observation (always, on any outcome
  that reached classification) → consistent: insert binding + creation event → COMMIT → return
  binding; classification-refusal: COMMIT observation (+ hold if the matrix says so), no binding.
- **T-replay**: in-tx hit + full input equality → return the original binding; no row touched
  (log/metric only).
- **T-conflict**: in-tx hit + any input differ → fail-closed refusal (`allocation_conflict`); no
  second binding, original untouched.
- **T-converge (23505)**: insert raced → ROLLBACK → fixed-order classify: (1) by `intent_id`
  (hit + equality → replay; hit + differ → conflict); (2) by `(chain_id, sender, nonce)` (hit →
  internal serialization failure, retryable — never another intent); (3) miss → retryable. Exact
  `ConstraintName` match only.
- **T-observe** (reconcile tick per scope): pre-tx observation → BEGIN → lock → re-read
  non-terminal bindings + scope state → apply `in_flight`/`consumed` transitions (event per
  transition, `UNIQUE (binding_id, to_state)` convergence) → establish a hold when the matrix
  requires → update `last_*` + `last_observation_id` → COMMIT.
- **T-hold-release**: pre-tx fresh observation → BEGIN → lock → `SELECT … FOR UPDATE` the named
  hold (missing/already released → `nop` audit, zero change) → re-verify: fresh observation
  consistent (or the specific remedy the cause demands), no unresolved scope conflicts, and the
  recorded evidence version re-read (`hold_id` still `active`, supplied `--observation-id` in
  scope, current `registry_seq`/`state`, current active-hold set, 006 state) → mark this hold
  `released` (only this row) → `UPDATE nonce_scope_state SET reconciled_floor =
  GREATEST(COALESCE(reconciled_floor, 0), $observed_pending)` → audit row (`applied`) → COMMIT.
  Any verification failure or version drift versus the operator's evidence → audit row (`refused`)
  + zero hold/floor change (contracts/observation.md §3.1).
- **T-binding-release**: same carrier; `FOR UPDATE` the binding; require non-terminal; require
  the no-side-effect evidence re-check; set `state='released'` + terminal columns + event + audit;
  refusal path records `refused` audit with zero binding change.
- **T-registry** (`registry_register` / `registry_disable`): BEGIN → lock → ensure registry row;
  `INSERT` first registration or `UPDATE … SET state, registry_seq = registry_seq + 1` guarded by
  `RowsAffected`; audit row; COMMIT. Attempt dedup per `nonce_ops_audit_operation_id_uniq`;
  same-`operation_id` replay returns the recorded outcome; differ → `operation_conflict`.
- **T-rebuild-verify** (startup): read-only; per known scope verify bindings/floor/holds are
  readable and constraint-consistent; success opens the admission gate (R5); failure keeps it
  closed with a structured error.

## Concurrency argument

1. **Allocation uniqueness**: the coordination-row lock (R1) serializes every 008 writer across
   processes and restarts; `M` is recomputed from `nonce_bindings` under that lock, so two
   concurrent same-scope allocations cannot choose the same candidate (the second reads the
   first's committed row only after the lock is released; if it somehow inserts anyway, the
   `nonce_bindings_scope_nonce_uniq` index insert aborts it and T-converge classifies).
2. **Intent idempotency**: `nonce_bindings_intent_uniq` + the in-tx intent re-read give at-most-one
   binding per intent under concurrency, response-loss, and restart (all state durable).
3. **Recovery-pause precedence**: 006 establish/pause/release transactions hold the same
   coordination row (`reorgcommit.go` shape), so admission and pause establishment are linearized:
   pause committed first ⇒ 008's in-tx recheck observes it and refuses; admission committed first
   ⇒ the binding stands and 006's downstream in-flight-unknown semantics apply. 008 never writes,
   resets, or deletes upstream rows.
4. **Release vs observer/allocation**: release runs under the same lock and re-verifies with a
   fresh observation plus the in-tx re-read of scope conflicts; an observer establishing a new
   cause row after the release is an independent, evidence-linked hold (correct re-establishment),
   while the released row is neither rewritten nor silently re-opened.
5. **Rebuild**: no decision trusts memory; the readiness gate only prevents serving before the
   verification pass, it is not a state source.
6. **Read serialization (bilateral, read-api §4)**: provider reads take the scope
   `nonce_scope_state` row `FOR SHARE` before snapshotting; all four writer transactions above
   take it `FOR UPDATE`, so 008-owned pause/registry/floor/binding writes are linearized against
   reads. 006 ordering inside the read snapshot stays best-effort; consumers re-read 006 gates
   in their own transactions. Consumers MAY additionally hold the same scope row `FOR SHARE`
   inside their own admission transaction (ordering-only, never writes) under the fixed order
   scope row → 006 gate tables → 007 grant row → own rows; 008 writers take no locks in reverse
   order, so no cycle is introduced.

## Numeric and evidence conventions

- Nonce/counts: `NUMERIC(78,0)`, `CHECK` upper bound `18446744073709551615` (2⁶⁴−1), Go
  `math/big`; JSON-RPC hex quantities parsed with explicit overflow refusal (R13). No floats.
- Evidence: every classification, transition, hold, and operator operation carries either an
  `observation_id` or an `operation_id`; rows are append-only and never rewritten.
- Clock: `now()` for commit-time facts; `clock_timestamp()` for validity checks taken after a
  lock wait (the 007 R8 correction applies identically to authorization expiry checks).
