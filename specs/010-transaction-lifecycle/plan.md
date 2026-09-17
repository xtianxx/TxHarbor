# Implementation Plan: 010 Transaction Lifecycle Management（交易生命周期管理）

**Branch**: `010-transaction-lifecycle` | **Date**: 2026-09-17 | **Spec**: [spec.md](spec.md) (clarify `bd59754`, Q1–Q3 resolved; zero markers)

**Input**: Feature specification from `/specs/010-transaction-lifecycle/spec.md`; approved upstream rulings 006 FR-26, 007 FR-08/FR-19, 008 OC-1–OC-7 (`specs/009-signer-service/contracts/gates.md` for the consumer-side form), 009 FR-05/13/17/23 + `contracts/{api,gates,persistence}.md`, PB-FR-01–PB-FR-08 + PB-C1/C2 (`migrations/000010_withdrawal_authorization_scopes.sql`); joint contract v1 (`docs/workflow-010-011-parallel.md` §联合设计合同 v1, J1–J6, C1–C12); 011 spec read-only at `.slim/worktrees/prep-011-worker/specs/011-withdrawal-executor/spec.md` (M1n/M2/M3).

## Summary

010 owns "after the signature": construct and persist a transaction attempt before the first 009 call,
obtain the signature through the 009 signing boundary, reconstruct and durably land the signed bytes +
locally computed hash **before any external send**, then broadcast inside a single fixed-order locked
PostgreSQL transaction (the **send region**) that re-verifies every current gate — 011 execution
qualification (claim row), 006 pause/recovery version, 007/PB authorization + scope, 008 binding scope
row — and holds those locks until the dispatch result is committed. Send results are classified
fail-safe (`accepted` / `rejected` / `unknown`); timeouts, response loss, partial writes and crashes
become durable `unknown` and are resolved only by chain reconciliation (`not_found_yet` is never a
failure verdict). Same-bytes replay re-uses the same attempt and the persisted bytes; fee replacement
creates a new attempt + identity under the same intent/binding with PB conditional grant reuse. Receipt
verification requires status success **and** the expected ERC-20 Transfer (emitter/from/to/integer
amount) against the indexer's canonical `chain_blocks` view; confirmation depth comes read-only from
005's `confirmation_policy_history`; reorg invalidates receipts through an append-only revision chain
that never rebuilds an intent, binding, or payment. Technical approach from research R-010-01–R-010-14;
new package `internal/txlifecycle`, one provisional additive migration `000011_tx_lifecycle.sql`, six
new tables, no new dependency, no new infrastructure, no new process. 010 keeps no balance ledger: its
bookkeeping is limited to chain facts, attempt/request state and projection revision, and revision never
exempts a later send from the normal authorization gates (M2, inherited).

## Technical Context

**Language/Version**: Go 1.26.5 (`go.mod`); integer-only quantities (`NUMERIC(78,0)` ↔ `math/big`; zero floats — constitution I); addresses lowercase `0x` hex.

**Primary Dependencies**: go-ethereum v1.17.5 (`core/types`, `rlp`, `crypto`, `common`, `accounts/abi`, `ethclient` — already required; transaction reconstruction, RLP encoding, ERC-20 calldata, log topics), pgx v5.11.0 (pool/tx/`pgconn.PgError` classification), goose v3.28.0; existing `internal/{config,db,eth,health,logx,metrics}`. **No new dependency, no new infrastructure** (constitution XIII).

**Storage**: PostgreSQL only (postgres:18.6-trixie pin). New 010 tables (provisional migration `000011`, additive): `tx_attempts`, `tx_attempt_signings`, `tx_send_attempts`, `tx_reconciliations`, `tx_receipts`, `tx_attempt_events` (data-model Tables 1–6). Read-only consumers: 006 `indexer_pause`/`log_pause`/`deposit_pause`/`reorg_recovery`/`reorg_recovery_events`, `chain_blocks`; 007 `withdrawal_authorizations` + PB `withdrawal_authorization_scopes` (`FOR SHARE`); 008 `nonce_bindings`/`nonce_scope_state`/`nonce_scope_holds`/`nonce_wallet_registry`; 011 `execution_claims` (adapter, absent pre-011 → fail closed); 005 `confirmation_policy_history`. 010 writes **none** of those tables. No Redis/Kafka.

**Testing**: `go test ./...` (unit: construction/envelope/content hash, fee arithmetic, classification, refusal taxonomy, receipt/Transfer verification, byte reconstruction determinism) + `make test-integration` with real PostgreSQL + real Anvil + real `signer-serve` (testcontainers pattern, `internal/nonce/unknown_outcome_integration_test.go:60-98`) + race detector; failure injection via a test HTTP proxy between 010 and 009. Quickstart V1–V11 is the 010-independent matrix; J1–J4 are joint requirements (design only in this step).

**Target Platform**: Linux server, single deployment single chain (v1; multi-chain out of scope).

**Project Type**: Backend wallet/transaction infrastructure (monorepo). New `internal/txlifecycle` domain package + `internal/eth` send/read extension + `internal/config`/`internal/metrics` extension + one migration. No new binary/subcommand.

**Performance Goals**: No throughput target. Each send is one bounded region (5s statement/lock guards + one bounded dispatch); reconciliation is a periodic point-query scan over non-terminal attempts; no benchmarks claimed.

**Constraints**: DB `now()` is the only gate clock; no external call inside any other transaction; dispatch bounded by `TXHARBOR_TX_SEND_TIMEOUT`; reconcile cadence/RPC timeout reuse the INDEX knobs (R-010-12); signed bytes/signature/credentials never logged; bounded retries only; fail-closed on every unreadable gate; test resources workdir-isolated (dedicated DB name + non-default ports, 009 R10 precedent).

**Scale/Scope**: Single chain; operator/worker cardinality (tens); six new tables; one new package; no UI, no admin HTTP surface; status projection is read-only data for 011.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Pre-Phase-0 (2026-09-17, against Constitution 1.1.0):

- **I (Financial correctness)**: PASS — integer-only amounts/fees/nonces/fees (`NUMERIC(78,0)`, BIGINT, `big.Int` arithmetic; fee-cap product computed exactly); replay/replacement identity chain makes same-nonce double-intent structurally impossible; unknown is never a failure verdict; no float anywhere.
- **II (Idempotency)**: PASS — persistence-layer PK/UNIQUE carriers + named-constraint 23505 classification + insert-first + row-lock serialization on the attempt; retries converge; never in-memory checks (R-010-02/04, data-model §Idempotency carriers).
- **III (PostgreSQL truth)**: PASS — all attempt/send/reconcile/receipt/revision facts in PostgreSQL; no cache; projection is explicitly non-authoritative.
- **IV (Reorg-aware)**: PASS — canonicality decided against `chain_blocks.canonical`; receipts carry block hash/number; revision chain per current recovery version; confirmation depth explicit/configurable via 005's policy (R-010-08/09).
- **V (Explicit state machines)**: PASS — attempt state CHECK + explicit transition table; send outcomes; receipt canonicality; transitions guarded by expected-state UPDATEs (data-model §State machines).
- **VI (Atomic boundaries)**: PASS — T1/T2/T3/T4 each commit one durable transition; bytes land before any send; dispatch + evidence + state in one region commit; no "broadcasted without durable reference".
- **VII (Nonce concurrency)**: PASS (boundary) — 010 never allocates/modifies nonces; it consumes 008's binding read-only under the coordination lock; 008 remains the allocation authority (R-010-04 step 7).
- **VIII (Key isolation)**: PASS — 010 holds no key material and has no signing code; signatures only via the 009 HTTP boundary (FR-10).
- **IX (Failure paths)**: PASS — dispatch/reconcile classification, unknown recovery, crash matrix, bounded timeouts/retries; RPC classes distinguished (R-010-05/06/12).
- **X (Deterministic local testing)**: PASS — real PG + Anvil + signer-serve locally/CI; no testnet dependency (R-010-11).
- **XI (Invariant tests)**: PASS — concurrency/crash/gate-race coverage uses real PostgreSQL + real chain; the only fixture is a labeled contract-shaped claim table for independent acceptance; never cited as joint acceptance.
- **XII (Observability)**: PASS — existing metrics registry + structured logs; attempt/tx/gate basis fields; no secrets/signature bytes (R-010-13).
- **XIII (Simplicity)**: PASS — one package, one migration, no new infra/service/dependency; reuses lock protocol, audit shape, transfer semantics, confirmation policy (R-010-01..09).
- **XIV (Spec-driven)**: PASS — this plan; scope fenced by Non-Goals; approved rulings consumed, not reopened.

Post-Phase-1 re-check (2026-09-17): no new violations introduced. The only deliberately non-minimal carriers are justified in Complexity Tracking below: the separate `tx_attempt_signings` table (FR-02 bytes-before-send as a discrete durable fact), the `tx_attempt_events` append-only log (FR-09 revision chain + refusal evidence), and the lock-held-across-dispatch region (R-010-04; required by Q3 and impossible to satisfy with a check-then-send shape). G-010-1 and G-010-2 are recorded residuals, not waived guarantees: no principle is weakened by them, and no grace/TTL/wider in-flight scope is introduced.

Verification-round addendum (2026-09-17): gate-expiry evaluation pinned to `clock_timestamp()` (send-gate matrix; 009 `submitClockSQL` precedent); `TXHARBOR_TX_SEND_TIMEOUT` default fixed at 15s (technical). Adjudication addendum (2026-09-17): G-010-1 closed as an explicit limited Q3 exception (natural-expiry residual only; recorded + reconciled, not called in-flight; no grace/TTL; prior checks never reused). Status split: plan documents delivered; design closed except G-010-2..G-010-6 residuals (reported, unverified), G-010-3 FK closure specified pending merge-time execution; joint acceptance not executed; no auto-entry into tasks.

## Project Structure

### Documentation (this feature)

```text
specs/010-transaction-lifecycle/
├── plan.md              # This file (/speckit.plan output)
├── research.md          # Phase 0 output (R-010-01..14 + rejected alternatives + G-010-1..6)
├── data-model.md        # Phase 1 output (Tables 1-6, state machines, txn catalog, idempotency carriers)
├── quickstart.md        # Phase 1 output (V1-V11 independent + J1-J4 joint requirements, design-only)
├── contracts/
│   ├── send-api.md      # 011-facing operations, refusal taxonomy, claim adapter mapping, projection read
│   ├── send-gate.md     # gate participants, effective points, lock order, race timelines, residuals
│   ├── signer-call.md   # 010→009 wire request/response handling incl. delivery-unknown + byte reconstruction
│   └── persistence.md   # T1-T4 catalog, crash-point matrix, unknown/reconcile protocol, idempotency carriers
├── checklists/
│   └── requirements.md  # specify/clarify artifact (consumed, not rewritten)
└── tasks.md             # Phase 2 output (/speckit.tasks — NOT created here)
```

### Source Code (repository root)

```text
internal/txlifecycle/
├── attempt.go        # request schema, canonical economic envelope + content hash, construction/validation
├── calldata.go       # ERC-20 transfer(address,uint256) construction + expected-Transfer extraction (abi)
├── store.go          # T1/T2 SQL: insert-first, 23505 classify, replay-or-conflict, signing persist, guarded state updates
├── signing.go        # 010→009 HTTP client: submit packing, response/refusal mapping, signed-bytes reconstruction + hash cross-check
├── gates.go          # send-gate read sequence (claim → coord → gate tables → 006 read → scope row → binding → grant/scope → attempt)
├── claim.go          # execution-qualification adapter over the frozen J2 shape (fail-closed when absent)
├── send.go           # T3 send region: refusal classes, dispatch, evidence + state in one commit
├── reconcile.go      # T4 chain probes, tx-by-hash/receipt classification, unknown recovery interface
├── verify.go         # receipt status + expected Transfer verification, canonicality against chain_blocks
├── confirm.go        # confirmation progress/threshold (005 policy read-only), reorg revision, replaced marking
├── projection.go     # authoritative status read (revision_seq/updated_at) for 011; never a permission
├── errors.go         # refusal taxonomy classes + retryability
└── *_test.go         # unit + integration (real PG/Anvil/signer-serve; contract-shaped claim fixture)

internal/eth/client.go       # EXTEND: SendSignedTransaction (raw eth_sendRawTransaction + hash check),
                             # TransactionByHash, TransactionReceipt, BlockNumber + send error classification
internal/config/config.go    # EXTEND: TXHARBOR_TX_SEND_TIMEOUT (+ signer URL/credential knobs); fail-closed validation
internal/metrics/            # EXTEND: send/reconcile/receipt/revision counters/gauges on the existing registry
internal/app/serve.go        # EXTEND: wire 010 deps + 010-owned reconcile loop (observe-only)
internal/logx/               # reuse Redact (no change expected)
internal/health/             # reuse (no change expected)

migrations/
└── 000011_tx_lifecycle.sql  # Tables 1-6 (pure DDL, named constraints, provisional number re-verified at merge)

tests (per V/J matrix; design only, not written here):
├── unit: go test (schema/envelope/calldata/fee arithmetic/classification/taxonomy/transfer match)
├── integration: real PostgreSQL + real Anvil + real signer-serve (races/crash/unknown/receipt/reorg)
└── joint: real 007 HTTP + 011 + 010 + 009 + PG + Anvil (defined here, executed later)
```

**Structure Decision**: one domain package per feature, one concern per file, repository-owned parameterized SQL — the existing `internal/indexer`/`internal/signer` layout, with the 009 insert-first/replay-or-conflict discipline on 010's own identity carriers. The send region is the single place in the codebase that performs an external send; `internal/eth` stays a thin classified transport and does not gain business state. No new service, no new listener, no new infra.

## Broadcast-gate design (Q3 / FR-15) — summary

Full protocol, timelines and evidence fields: `contracts/send-gate.md`. Participants and write entries:

| Participant | Write entry | Change | Effective point | 010 region lock |
|---|---|---|---|---|
| 011 claim re-lease / revocation | 011-owned writer (frozen J2) | `lease_version` / state / revocation marker | writer COMMIT | claim row `FOR SHARE` |
| 006 recovery establish / verify / release | `internal/indexer/reorgcommit.go` writers | `reorg_recovery` row + events | writer COMMIT | coordination row `FOR UPDATE` + gate-table `SHARE` |
| 006 pause on/off | stream writers / manual DBA `INSERT`/`DELETE` on `indexer_pause`/`log_pause`/`deposit_pause` | pause on/off | writer COMMIT | gate-table `SHARE` (conflicts with `ROW EXCLUSIVE`) |
| 007 revoke / re-supply | `RevokeGrant` / re-supply + PB scope bump | grant `state` / scope version | writer COMMIT | grant row `FOR SHARE` + scope row `FOR SHARE` |
| 008 pause / registry disable / release | 008-owned writers (`internal/nonce`) | 008 gate / binding state | writer COMMIT | coordination row `FOR UPDATE` + scope-row `FOR SHARE` |
| 010 dispatch | this region | `tx_send_attempts` + attempt state + event | region COMMIT | — |

Two required timelines: **(a) invalidation before the region** → observed under the held locks; zero dispatch; refusal class + observed basis committed. **(b) region first, invalidation during dispatch** → the invalidation writer blocks on the conflicting lock until the region commits; the dispatch is legally in-flight (it entered the uncancellable stage while every gate was valid), and the invalidation governs later replays/replacements only. Time-based expiry is re-evaluated on the DB clock immediately before dispatch and the evaluated basis is recorded; its irreducible residual is G-010-1. DB/network independent failures are G-010-2 and always end in `unknown` + reconcile, never in a fabricated success/failure.

## FR/SC/Scenario → design/verification mapping

| FR | Design carrier | Verified by (future) |
|---|---|---|
| FR-01 (persist attempt before 009; identity↔content binding) | T1 insert-first + canonical envelope + replay-or-conflict (R-010-02; data-model Table 1) | V1 |
| FR-02 (bytes + local hash durable before send) | Local reconstruction + hash cross-check + T2 commit before any dispatch (R-010-03; Table 2) | V2 |
| FR-03 (unknown ≠ success/failure/unpaid; reconcile path; no auto new payment) | Send-outcome taxonomy + unknown recovery interface + claim-free reconcile (R-010-05/06) | V4, V5 |
| FR-04 (same-bytes replay; same attempt/identity; current gates) | Replay path re-uses persisted bytes + full gate sequence (R-010-04; Table 3) | V3, V6 |
| FR-05 (replacement: new attempt + new identity, same intent/binding/semantics) | `replacement_of` + immutable content + `tx_hash` UNIQUE (R-010-07; Table 1) | V7 |
| FR-06 (PB conditional reuse + explicit authorization identity/version) | Gate step 8 reuse/fresh branch + attempt stores `authorization_id`+version (R-010-07) | V7 |
| FR-07 (006 pause/recovery inheritance; multi-pause; read-only) | Gate steps 3–5 + `SHARE` locks; zero writes to 006 tables (R-010-04; send-gate.md) | V6 |
| FR-08 (receipt + expected Transfer verification) | Receipt verify over canonical `chain_blocks` + pinned Transfer semantics (R-010-08; Table 5) | V8 |
| FR-09 (confirmation + reorg revision chain; no compensating rebuild) | Confirmation basis + append-only revision events + `replaced` marking (R-010-09) | V8, V9 |
| FR-10 (no keys; signatures only via 009) | Import/structural boundary; `internal/txlifecycle` has no key/provider code (R-010-01) | V11 (static) |
| FR-11 (re-verify every gate per send; fail closed) | Send region re-reads all gates inside the locked region; no cached basis (R-010-04) | V6 |
| FR-12 (identity chain traceability) | PK/FK/UNIQUE carriers + events (Table 1; `request_id → intent_id → binding → attempt → signing_request → authorization+version`) | V1, V7 |
| FR-13 (state authority + projection versioning) | Projection read with `revision_seq`/`updated_at`; anti-permission contract (R-010-13) | V10 |
| FR-14 (expired-worker fence; takeover; no second intent) | Claim row `FOR SHARE` + version/expiry/revocation equality; claim-free reconcile; takeover re-verification (R-010-04; G-010-4) | V6, J2 |
| FR-15 (check-to-send ordering, residuals, no grace/TTL) | Lock-held region + last-moment time re-evaluation + outcomes; G-010-1/G-010-2 reported (R-010-04/05/14) | V6, V9 |
| FR-16 (independent vs joint acceptance) | V-matrix vs J-matrix separation; labeled fixture; no mock-as-joint (R-010-11) | V/J boundary review |
| SC-01 | First-broadcast completeness + zero-send refusals | V1, V6 exit criteria |
| SC-02 | Injected timeout/response-loss → 100% unknown, zero facts lost | V4 exit criteria |
| SC-03 | Replay byte identity + zero new attempts/requests | V3 exit criteria |
| SC-04 | Replacement traceability + semantic equality | V7 exit criteria |
| SC-05 | Receipt/Transfer mismatch never marked successful | V8 exit criteria |
| SC-06 | Zero sends during recovery pause / stale view | V6 exit criteria |
| SC-07 | Reorg revision tracks original intent; zero rebuilt intents | V9, J4 exit criteria |

Upstream consumed read-only: 006 FR-26 matrix + pause/recovery tables; 007 receive-only boundary + grant table; 008 binding/registry/holds + read classes; 009 submit/status API + delivery refusals; PB scope carrier; joint contract J1–J6. Downstream (statement only, no 011 artifacts): 011 creates the intent/claim, calls Send/Replay/Replacement and Reconcile, stores a display projection keyed by `attempt_id` + source revision, and must never treat 010-independent results as joint acceptance.

## Task-input: parallelizable vs dependent (for the later tasks step)

- **Parallel after design approval**: migration DDL vs `internal/eth` send/read extension vs `internal/txlifecycle` unit-level pieces (attempt/calldata/envelope/classification) — disjoint files. Data-model carrier names must freeze before SQL and tests start.
- **Strictly dependent (do not start before their inputs land)**: send region (needs gate read sequence + migration), reconcile (needs chain read extension), confirmation/reorg (needs canonical view reads + V8/V9 chain scenarios), joint J-matrix (needs 011 implementation and 010→011 merge order; upstream sync precedes dependent acceptance).
- **Contract-freeze dependencies**: the claim adapter mapping (011 plan column names) and the intent FK closure (G-010-3) are joint items; neither blocks 010-independent work because both are fail-closed/absorbed in one adapter.
- **Real dependencies vs parallel cosmetics**: V1–V11 are 010-independent; J1–J4 require real 011 + real HTTP 007 path; mocks or the contract-shaped fixture must never be presented as J evidence (FR-16, R-010-11).

## Complexity Tracking

No Constitution Check violations; no principle is waived. Carriers beyond a bare minimum:

| Non-standard choice | Why needed | Simpler alternative rejected because |
|---|---|---|
| Locks held across the network dispatch (send region) | Q3 requires write-based invalidations to be ordered before/after the whole send; only a held conflicting lock gives that guarantee with existing primitives | Check-then-commit-then-send leaves an unbounded revoke-vs-send race (the OPEN-2 gap); a second check is not an ordering proof |
| Separate `tx_attempt_signings` table | FR-02's "bytes durable before send" is a discrete, independently auditable fact with its own UNIQUE hash guard | Folding nullable bytes into `tx_attempts` mixes a pre-send fact with identity/content and weakens the "landed before send" evidence |
| `tx_attempt_events` append-only log | FR-09 revision chain + refusal/unknown evidence must survive every later state | Mutating a status column loses the revision chain and gate basis history that FR-09/audit require |
| `content_hash` without UNIQUE | Replay equality + audit digest while allowing legitimate recovery-version rebuilds | `UNIQUE(content_hash)` blocks approved recovery rebuilds; no hash loses cross-envelope comparability |
| Claim adapter + contract-shaped fixture | 011 does not exist yet; the fence must be designed and independently testable without pretending the real table exists | Mocking the fence would violate constitution XI; creating 011's table in a 010 migration writes 011 artifacts |

## Verification plan (this step writes the plan; execution belongs to tasks/implement)

- **Migration** (`000011`, scratch/dedicated DB only): `up`/`down` reproducibility; negative probes — PK/UNIQUE/FK/CHECK conflicts expect 23505/23503/23514 with the exact named constraint; read-only assertion diff on 006/007/008 tables; adapter fail-closed when `execution_claims` is absent.
- **Unit**: schema strictness, canonical envelope determinism, content-hash stability, ERC-20 calldata + expected-Transfer extraction, fee arithmetic (`gas_limit × max_fee_per_gas` with `big.Int`), signed-bytes reconstruction determinism vs a fixed vector, dispatch/reconcile classification tables, refusal taxonomy retryability.
- **Integration (real PostgreSQL + Anvil + signer-serve)**: insert-first races (same identity/different content); bytes-before-send ordering assertion; the two send-region timelines (invalidation committed before → zero dispatch; invalidation racing dispatch → writer blocks until region commit); all four gate families individually; claim version/expiry/revocation; crash matrix (pre-T1, between T1/T2, between T2/T3, mid-region pre-dispatch, post-dispatch pre-commit, post-commit pre-response); unknown → reconcile → replay/replacement; receipt success/failure/missing/mismatch; confirmation threshold; reorg invalidate/reconfirm revision chain; replaced marking; read-only assertion on every upstream table.
- **Race detector**: `make test-race` on the 010 paths (attempt row lock, send-seq allocation, reconcile vs send).
- **Boundary/secrecy**: import test (no key provider/RPC signing imports; single send call site); log/metric scan for signed bytes, signatures, credentials.
- **CI**: existing `ci.yml` four jobs (unit+race, integration-Docker, build, lint) cover 010 with no workflow change.
- **Residual risks / ownership**: G-010-1 (time-based gate change interval), G-010-2 (DB/network independent failure), G-010-3/G-010-4 (pre-011 intent FK / claim table), G-010-5 (mempool absence), G-010-6 (revocation-path lock coverage) — each reported in research R-010-14 with boundaries; none is closed by widening in-flight scope.

## Evidence separation (status, not proof)

- This step produced **documentation only** (plan/research/data-model/contracts/quickstart); no code, migration, test, service, or container was written or executed; V/J artifacts are design-only.
- Prior-step evidence (006/007/008/009/PB merges, joint contract v1) is the consumed baseline only; nothing here re-verifies it.
- 010-independent acceptance ≠ joint acceptance (FR-16). The contract-shaped claim fixture is test scaffolding, never joint evidence.
- T000-P and A-13 remain OPEN.

## Deferred items (explicit, with owner)

| ID | Item | Owner |
|---|---|---|
| G-010-1 | Time-based gate expiry interval between last evaluation and dispatch entry (reported, not closed; no grace/TTL added) | 010 plan / adjudication |
| G-010-2 | DB-region failure vs network send → `unknown` + probe (unremovable dual-write residue, stated) | 010 plan |
| G-010-3 | No FK to `payment_intents` until 011 lands; joint-integration closure item | 010→011 merge |
| G-010-4 | `execution_claims` absent pre-011 → adapter fails closed; fixture labeled test-only | 010 independent / joint later |
| G-010-5 | `not_found_yet` never terminal; resolution requires chain facts or confirmed replacement | 010 design |
| G-010-6 | Revocation paths must take conflicting locks; unlocked side-table revocation would need re-analysis | future amendment guard |
| J2 mapping | 011 claim column names absorbed by one adapter mapping table (semantics frozen by J2) | 010↔011 joint |
| PLAN-1 | Migration number `000011` re-verified against the actual set at merge; no renumbering of applied migrations | tasks/implement |
| T000-P / A-13 | Production provider selection / full-chain E2E remain OPEN | later production track |
