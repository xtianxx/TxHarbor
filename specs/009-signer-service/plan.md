# Implementation Plan: 009 Signer Service（签名隔离服务）

**Branch**: `009-signer-service` | **Date**: 2026-09-16 | **Spec**: [spec.md](spec.md) (clarify 两轮 2026-09-16, zero markers; OC-1–OC-7 resolved)

**Input**: Feature specification from `/specs/009-signer-service/spec.md`; upstream contracts 006 `contracts/downstream.md`, 007 `contracts/api.md` §3/§4, `docs/workflow-008-009-parallel.md` R1–R7.

## Summary

Independent signing boundary as a separate process (`txharbor signer-serve`) that is the only
component with key access: authenticated, structured-transaction-only signing requests; full
content validation (chain/sender/asset/calldata/recipient/amount/fees); identity↔content binding
in PostgreSQL with same-identity replay and conflict refusal; sign+persist in one row-locked
transaction before any response; read-only consumption of 006 pause/recovery and 007
authorization gates with 008 scope-row `FOR SHARE` first, then a shared gate-table `SHARE` lock on the 006 gate tables plus `FOR SHARE`
on the 007 grant (one ring-free lock order — research R6); recorded delivery admission that
re-verifies 006/007/008 + `can_sign` gates on every response (first and retry alike) with a
**recorded delivery admission sharing one protected region with the bytes write** (gates
re-verified on every response, first and retry alike; write inside the held locks, bounded send
region — research R6); persisted results never re-delivered after expiry/revocation/pause (status-only);
conditional OC-5 fee replacement (reuse the grant only if it explicitly permits replacement and
the fee is in scope, else fresh authorization + new identity — research R7); no broadcast and no
RPC code path. Technical approach from research R1–R11: go-ethereum `types`+`crypto` (no custom
cryptography), narrow sender-bound `KeyProvider`, canonical envelope + `keccak256` content hash,
insert-first/23505 classification, single `000009` migration with six tables, and the R11
authorization-carrier closure (upstream-owned extension; 009 fails closed on unverifiable grants).

## Technical Context

**Language/Version**: Go 1.26.5 (`go.mod`); integer-only quantities (`NUMERIC(78,0)` ↔
`math/big`/`uint256`, BIGINT for chains/counters; zero floats per constitution I); addresses as
lowercase 0x hex with EIP-55 acceptance.

**Primary Dependencies**: go-ethereum v1.17.5 (`types`, `crypto`, `common` — already required;
signing/EIP-155/RLP authority); pgx v5.11.0 (pool, tx, `pgconn.PgError` constraint classification);
goose v3.28.0 migrations; existing `internal/{config,db,health,logx,metrics}`. **No new
dependencies** (no new module, no new infrastructure — constitution XIII).

**Storage**: PostgreSQL only (postgres:18.6-trixie pin; no `ON CONFLICT DO SELECT`). New:
`signer_caller`, `signer_credential`, `signing_requests`, `signature_results`,
`signing_request_audit`, `delivery_admissions` (data-model Tables 1–6). Read-only consumers:
006 `indexer_pause`/`log_pause`/`deposit_pause`, 006 `reorg_recovery` + events, 007
`withdrawal_authorizations` — read with a gate-table `LOCK … IN SHARE MODE` (lock only, no data
mutation; R6) plus `FOR SHARE` on the 007 grant. No Redis/Kafka/new infra.

**Testing**: `go test ./...` (unit: strict request schema, canonical envelope, validation matrix,
conflict/replay classification, refusal taxonomy, independent signature verification) +
`make test-integration` with real PostgreSQL (concurrency, crash/retry determinism, gate races,
delivery admission, read-only assertions) + race detector; **no Anvil/RPC** — absence of a chain
code path is itself asserted. Quickstart V1–V8 is the scenario matrix (design only in this step).

**Target Platform**: Linux server, single deployment single chain (constitution architecture
evolution; v1 scope).

**Project Type**: Backend wallet/transaction infrastructure (monorepo). New `internal/signer`
domain package + two subcommands (`signer-serve`, `signer-auth`) on the existing binary; one new
goose migration. Runtime process isolation = signing boundary; the business `serve` path wires no
`KeyProvider`.

**Performance Goals**: No throughput target; each request is a bounded sequence of point
statements under the 5s statement guard plus one bounded `KeyProvider` call; no benchmarks
claimed. Operator-issued caller cardinality, permanent append-only evidence.

**Constraints**: Fail-closed configuration (production mode without a real provider refuses
startup; missing test key refuses startup); DB `now()` as the only clock for expiry; no RPC, no
broadcast, no external calls inside transactions; secrets/signatures/raw signed bytes never
logged; bounded retries only; test resources workdir-isolated (dedicated DB name + non-default
ports, research R10).

**Scale/Scope**: Single chain; caller count operator-issued (tens); six new tables; no UI; no
admin HTTP surface (operator subcommand for credential lifecycle).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Pre-Phase-0 (2026-09-16, against Constitution 1.1.0):
- **I (Financial correctness)**: PASS — integer-only amounts/fees/nonces (`NUMERIC(78,0)`,
  uint256-bounded CHECKs); identity↔content binding makes "sign for the wrong object" structurally
  hard; no duplicate-signable path (research R3/R4).
- **II (Idempotency)**: PASS — persistence-layer UNIQUE carriers + insert-first/23505 classify +
  row lock; retries converge on one persisted result; never in-memory checks (R3/R4).
- **III (PostgreSQL truth)**: PASS — all durable bindings/results/admissions in PostgreSQL; no
  cache (not even for gates).
- **IV (Reorg-aware)**: PASS — 006 recovery version/pause semantics consumed read-only; stale-view
  content refused at sign and delivery (gates.md §1); 009 writes no chain state.
- **V (Explicit state machine)**: PASS — `received → validated → signed / rejected / failed` with
  storage CHECKs and guarded transitions (data-model Table 3).
- **VI (Atomic boundaries)**: PASS — gates + binding + authorization + sign + result + state +
  audit in one transaction; delivery admission committed before response bytes (persistence.md).
- **VII (Nonce concurrency)**: PASS (boundary) — 009 never allocates/modifies nonces; it consumes
  008's binding read-only with five classes (gates.md §3).
- **VIII (Key isolation)**: PASS — `KeyProvider` structured-tx-only surface, sender-bound, mode
  isolated; no digest-signing method; business paths do not import it (R2).
- **IX (Failure paths)**: PASS — refusal taxonomy + bounded timeouts/no internal retry loops;
  unknown outcomes are first-class (persistence.md §4–§6).
- **X (Deterministic local testing)**: PASS — real-PostgreSQL integration tests, no testnet,
  no chain dependency at all; isolation scheme for parallel worktrees (R10).
- **XI (Invariant tests)**: PASS — concurrency/crash/gate races use real PostgreSQL; mocks only
  for the not-yet-available 008 adapter, and never as final integration acceptance.
- **XII (Observability)**: PASS — structured logs + metrics on the existing registry; secrets/
  signatures/raw bytes excluded; request identity/content hash/version fields included (R9,
  persistence.md §7).
- **XIII (Simplicity)**: PASS — no new infra/services/protocol; one new package + one migration +
  two subcommands; reuse of existing config/log/metrics/persistence patterns.
- **XIV (Spec-driven)**: PASS — this plan; scope fenced by Non-Goals; upstream rulings consumed,
  not reopened.

Post-Phase-1 re-check (2026-09-16): no new violations introduced. Lock discipline is a single
ring-free order: gate-table `SHARE` locks on the 006 gate tables → 007 grant `FOR SHARE` →
`signer_caller` `FOR SHARE` → own request row (R6); no 006/007 writer takes a 009 table, so no
cycle can form. The `signer_caller`/`signer_credential` tables are required FR-04/05 carriers
(upstream tables cannot hold 009's caller namespace without breaking 007's contract); the canonical
envelope column is required to make same-identity equality byte-exact (R3); `replacement_of` +
the partial anchor index are required to make OC-5's conditional grant reuse physically possible
without rebinding an old request (R7). Former delivery-boundary gap **G-1 is closed in-plan** by
the gate-table `SHARE` lock (R6), not waived or deferred.

## Authorization carrier closure (OC-5 ↔ real 007 carrier)

Per-attribute closure matrix, the concrete upstream-owned extension carrier, its controlled write
entry, consistency protocol, migration compatibility, and the two genuine business questions are
in **research R11**. Summary of real vs extended capability:

- **Reused (real 007 capability)**: `authorization_id` identity, `caller_id` bind, `chain_id`/
  `asset`/`recipient`/`amount` equality, `state`/`expires_at` validity on the DB clock,
  `state`-flip revocation ordered by the grant-row lock, and the controlled `withdrawal-authz
  supply` entry (declared operator identity + append-only grant audit — an audit claim, not a
  cryptographic proof).
- **Extended (upstream-owned; 009 does not build it)**: a 007/011 additive carrier
  `withdrawal_authorization_scopes`, written in the **same supply transaction**, carrying
  `intent_id`, `request_id`, `sender`, `fee_scope`, `allows_fee_replacement`,
  `authorization_version`, `attested_by` (+ authorizer signature only if Q-A rules so). 009 reads
  it read-only in the same `FOR SHARE` sequence, checks equality, persists the version on the
  request, and re-checks version/fingerprint at delivery; a grant with no scope row/version is
  **refused fail-closed** (`authorization_unverifiable`), never silently accepted.
- **Not substitutes**: the `authz:v1` fingerprint, the `caller_id` FK, and ordinary caller/API
  authentication are not authorization capabilities.
- **Open business questions (R11)**: Q-A (cryptographic authenticity required in v1?) and Q-B
  (pre-extension grants: fail closed vs mandatory backfill).

## Pause/revoke ↔ delivery ordering (closes G-1; resolved in-plan)

Participants, write entries, and effective points (full protocol in research R6):

| Participant | Write entry | Change | Effective point | 009 ordering lock |
|---|---|---|---|---|
| 006 pause | `INSERT`/`DELETE` on `indexer_pause`/`log_pause`/`deposit_pause` (stream writers or manual DBA) | pause on/off | writer `COMMIT` | gate-table `SHARE` (conflicts with `ROW EXCLUSIVE`) |
| 006 recovery establish | `EstablishRecovery` (`internal/indexer/reorgcommit.go`) | `reorg_recovery`+events insert | `COMMIT` | gate-table `SHARE` |
| 006 recovery verify/complete | `CompleteRecoveryVerify` | phase move / row `DELETE` + event | `COMMIT` | gate-table `SHARE` |
| 006 recovery release | `AuthorizeRecoveryRelease` | row `DELETE` + `released` event | `COMMIT` | gate-table `SHARE` |
| 008 own pause / registry disable | 008-owned writer | 008 pause / binding state | 008 `COMMIT` | `BindingReader` serialization obligation (008 contract, gates.md §3) |
| 007 revoke / change | `RevokeGrant` / re-supply | grant `state`/fields | `COMMIT` | grant-row `FOR SHARE` |
| wallet/permission disable | `signer_caller.can_sign` via `signer-auth` | permission off | `COMMIT` | `signer_caller` row `FOR SHARE` |
| 009 first delivery / old-result replay | T-deliver admission `INSERT` | admission row | admission `COMMIT` (ordered by gate-table `SHARE`) | — |

Two required concurrency timelines (R6): **(a) pause/revoke before the region** → 009 observes
it under the held locks and returns status-only with zero signature bytes; **(b) region first,
pause/revoke after** → the pause/revoke write waits for the region `COMMIT` (ordered after);
bytes written inside the region are in-flight approved with a committed `delivered` marker, and
an admitted-but-unwritten outcome is impossible by construction (write failure → `ROLLBACK`).
Post-write pre-`COMMIT` crash → `unknown_reconcile` (honest unknown). Crash, commit-unknown,
and response-loss all resolve by same-identity retry re-reading durable rows and re-gating; the
result is never re-signed.

## Project Structure

### Documentation (this feature)

```text
specs/009-signer-service/
├── plan.md              # This file (/speckit.plan output)
├── research.md          # Phase 0 output (R1–R10 decisions + rejected alternatives)
├── data-model.md        # Phase 1 output (Tables 1–6, state machine, txn catalog, concurrency)
├── quickstart.md        # Phase 1 output (V1–V8 design-only validation + isolation scheme)
├── contracts/           # Phase 1 output
│   ├── api.md           # submit/status interface, response hygiene, processing order
│   ├── gates.md         # consumer-side reads: 006 pause/recovery, 007 grant FOR SHARE, 008 binding
│   └── persistence.md   # sign+persist ordering, crash/retry determinism, refusal taxonomy
├── checklists/
│   └── requirements.md  # specify/clarify artifact (consumed, not rewritten)
└── tasks.md             # Phase 2 output (/speckit.tasks — NOT created here)
```

### Source Code (repository root)

```text
internal/signer/
├── content.go      # strict request schema, canonical envelope + keccak256 content hash,
│                   # types.Transaction reconstruction (+ independent-verify helper)
├── validate.go     # field matrix: chain/sender/asset/calldata/recipient/amount/fees; EIP-55
├── policy.go       # versioned policy (allowlists/caps/registry) + policy_version identity hash
├── provider.go     # KeyProvider (structured-tx only) + local dev test-key provider + mode guard
├── auth.go         # credential verify (sha256 + constant-time + revoked_at), caller load
├── gates.go        # 006 one-statement pause/recovery/version read; 008 BindingReader interface;
│                   # 007 grant read FOR SHARE + fingerprint
├── submit.go       # T-submit-first / T-submit-replay (insert-first → 23505 classify → row lock)
├── delivery.go     # T-deliver: gate re-read, delivery_admissions record, response shaping
├── status.go       # desensitized own-request status read
├── errors.go       # refusal taxonomy classes + retryability
└── *_test.go       # unit: schema/envelope/policy/classification (no DB where possible)

internal/app/
├── signerserve.go  # `signer-serve` subcommand: config + pool + signer HTTP wiring (key deps
│                   # constructed ONLY here)
├── signerauth.go   # `signer-auth` operator subcommand (issue/rotate/revoke; apikey-auth shape)
└── *_test.go

cmd/txharbor/main.go       # EXTEND: dispatch `signer-serve` / `signer-auth`
internal/config/config.go  # EXTEND: TXHARBOR_SIGNER_* knobs (mode, addr, policy lists, key file,
                           # timeouts) with fail-closed validation and policy identity
internal/metrics/          # EXTEND: signer counters/gauges on the existing registry
internal/logx/             # reuse Redact (no change expected)
internal/health/           # reuse (no change expected)

migrations/
└── 000009_signer_service.sql   # Tables 1–6 (pure DDL; named constraints per 007 convention)

tests (per V-matrix; design only, not written here):
├── unit: go test (schema/envelope/validation/taxonomy/independent verification)
├── integration: real PostgreSQL (concurrency/crash/gate races/admission/read-only)
└── no chain tests: 009 has no RPC path (structural)
```

**Structure Decision**: follow the existing `internal/indexer` writer layout (one concern per
file; repository-owned parameterized SQL) and the 007 operator-subcommand pattern. The signing
boundary is a **process** boundary (`signer-serve`), not a new module/binary/new infra: the
single-binary + subcommand shape matches `confirm-auth`/`apikey-auth`/`withdrawal-authz`, and the
business `serve` path constructs no provider (import-boundary test). `internal/signer` MUST NOT
import `internal/withdrawal`/`internal/indexer` writer packages — it re-implements the small
read-side shapes (EIP-55 canonicalization, gate reads) so the signer boundary stays free of
business-writer dependencies (research R2/R5). No new services, no new listeners beyond the
signer's own address, no new infrastructure.

## FR/SC/Scenario → design/verification mapping

| FR | Design carrier | Verified by (future) |
|---|---|---|
| FR-01 (independent boundary, no business keys) | `signer-serve` process; `internal/signer` key deps only via `provider.go`; import-boundary test | V8 |
| FR-02/FR-03 (structured full content, no arbitrary digest) | strict schema (`DisallowUnknownFields`), no digest endpoint/field, content envelope | V1, V2 |
| FR-04 (authentication) | auth.go + Tables 1–2 + `signer-auth` lifecycle; generic 401 | V3 |
| FR-05 (permission + per-tx authorization, OC-5) | `can_sign` + 007 grant read/equality/expiry + OC-5 conditional replacement rule + R11 carrier closure | V3, V4, V6 |
| FR-06 (chain) | policy chain bind vs `config.ChainID` | V5 |
| FR-07 (sender) | sender registry/config + provider sender-key binding (OC-2) | V5, V8 |
| FR-08 (asset/calldata consistency) | `validate.go` calldata decode + allowlist + declared-triple equality | V5 |
| FR-09 (recipient) | EIP-55 shape + recipient policy | V5 |
| FR-10 (amount) | integer validators + uint256 CHECKs + policy cap | V5 |
| FR-11 (fees) | fee-shape CHECK + policy caps + OC-5 conditional replacement gate (R7) | V5, V6 |
| FR-12 (no silent bypass, auditable) | single validation entry + `policy_version` + audit rows | V5, V7 |
| FR-13 (identity↔content binding, OC-4) | Table 3 constraints + envelope equality + persist-before-return | V4 |
| FR-14 (retry/response-loss/restart determinism) | insert-first + row lock + persisted result + delivery path | V4 |
| FR-15 (explicit state machine) | state CHECK + fixed transaction order | V4 |
| FR-16 (result only, never broadcast) | response contract; no RPC import; no chain tests | V1, V8 |
| FR-17 (006 gate + delivery withholding + ordering, OC-6/OC-7) | gates.md §1 + gate-table `SHARE` lock ordering (R6) + delivery admission + bounded admission validity + status-only | V6, V7 |
| FR-18 (nonce given, read-only binding, OC-3) | never allocates nonce; `BindingReader` five classes | V6 |
| FR-19 (007 Accepted guard, OC-1) | no trigger from receive status; intent refs persisted, not inferred | V6 |
| FR-20 (KeyProvider, established crypto) | provider.go + go-ethereum `types.SignTx` | V8 |
| FR-21 (test/deploy key isolation) | mode config + fail-closed startup | V8 |
| FR-22 (secrecy) | log allowlist + Redact + status desensitization | V3, V7, V8 |
| FR-23 (bounded failures + OC-7 races) | taxonomy + admission ordering + unknown→reconcile | V7, V8 |
| FR-24 (observability) | existing metrics registry + structured logs | V1–V8 (assertions) |
| FR-25 (no new infra, PG truth, no floats) | design (no Redis/Kafka; NUMERIC/integer types) | V8 (static) |
| FR-26 (plan owns structures) | research/data-model/contracts (this step) | review |
| SC-01..SC-08 | see quickstart exit criteria | V1–V8 respectively |

Upstream consumed read-only: 006 FR-26 matrix + `contracts/downstream.md` preconditions and
version semantics; 007 receive-only boundary + authorization carrier + one-lock discipline;
`docs/workflow-008-009-parallel.md` R1–R7. Downstream (statement only, no specs written here):
010 persists attempt+full content before first signing call and must construct/broadcast under its
own 006 gate; 011 creates/persists the payment intent with a stable `intent_id` and consumes the
per-transaction authorization; 008 owns nonce allocation/binding/release authority.

## Complexity Tracking

No Constitution Check violations; no principle is waived. Carriers that go beyond a bare minimum
are justified explicitly:

| Non-standard choice | Why needed | Simpler alternative rejected because |
|---|---|---|
| Six new tables in one migration (incl. `signer_caller`/`signer_credential`) | FR-04/05 callers are 009's own authenticated namespace; upstream tables cannot hold them without coupling two independently deployable boundaries (research R9) | Reusing 007's `caller`/`api_key` tables would give withdrawal clients signing semantics and break 007's contract; env-only static tokens cannot be revoked per caller |
| Gate-table `SHARE` lock on the 006 gate tables (+ `FOR SHARE` on the 007 grant) | Closes the pause/revoke-vs-delivery race in-plan with one ring-free lock order, using only primitives already present (no 006/008 change); the relation-level lock also orders against future `INSERT`s and the manual-DBA release path a row lock cannot cover (R6) | Read-only snapshot without a shared lock: a pause/recovery commit between the last read and the response write is unobservable (the old G-1 gap); a 009-owned advisory lock 006 does not take: zero cross-writer guarantee |
| `replacement_of` + partial unique anchor index (`UNIQUE (authorization_id) WHERE replacement_of IS NULL`) | Makes OC-5's conditional same-grant reuse physically possible while keeping one anchor request per grant and forbidding any rebind of an old request row (R7) | Keeping `UNIQUE (authorization_id)`: reuse impossible, forcing a silent fresh-only reinterpretation; no constraint: two competing anchors for one grant |
| Stored canonical envelope (+ keccak256 hash) alongside parsed columns | Same-identity equality must be byte-exact and re-derivable after restart; audit needs one comparable artifact (research R3) | Column re-comparison is ambiguous across encodings (case, leading zeros); hash-only loses operator-readable evidence |
| Delivery admission rows before response bytes | OC-6/OC-7 require a verifiable ordering and recorded basis; also distinguishes in-flight-approved from never-delivered (persistence.md §2) | Delivering without a recorded admission cannot satisfy the ruling's evidence requirement; a log line is not durable state |

## Verification plan (this step writes the plan; execution belongs to tasks/implement)

- **Migration** (`000009`, scratch/dedicated database only): `up`/`down` reproducibility; negative
  probes — UNIQUE/PK/CHECK conflicts expect 23505/23514 with the exact named constraint
  (`signing_requests_caller_request_uniq`, `signing_requests_attempt_uniq`,
  `signing_requests_authorization_anchor_uniq`, `signature_results_tx_hash_uniq`, …); classification by
  `ConstraintName` only.
- **Unit**: strict schema rejection (unknown fields/arbitrary digest), canonical envelope
  determinism, validation matrix, taxonomy mapping, independent signature verification
  (recover sender + rebuild hash), no float types.
- **Integration (real PostgreSQL, workdir-local DB)**: concurrency (N identical submits →
  one result; different anchors on one grant → one winner; conditional replacement reuse vs fresh
  grant); crash points (pre-commit / post-commit / admission / response); the two R6 timelines
  (pause/revoke committed before delivery admission → status-only; admission committed before a
   pause/revoke → write inside the held locks or not at all; write failure → ROLLBACK); `can_sign` disabled between sign
  and delivery; read-only diff of 006/007 tables with the gate-table `SHARE` lock asserted; status
  privacy (identical 404).
- **Race detector**: `make test-race` on signer paths (row lock/insert classification).
- **Boundary/secrecy**: import test (business `serve` wires no `KeyProvider`; `internal/signer`
  imports no RPC/dial package); log/error/metric scan for keys, credentials, signature bytes on
  refusal paths, raw signed bytes.
- **CI**: existing `ci.yml` four jobs (unit+race, integration-Docker, build, lint) cover 009 with
  no workflow change; no new CI infrastructure.
- **Residual risks / ownership**: G-1 **closed in-plan** by the gate-table `SHARE` lock (R6) — no
  longer a residual item; 008 adapter integration (D3) now includes the R6 serialization
  obligation on `ReadBinding`; R11 authorization-carrier extension and its Q-A/Q-B business
  questions belong to 007/011 (009 fails closed until available); T000-P production provider
  (open). Test doubles for the 008 binding are contract-shape only and MUST NOT be cited as final
  integration acceptance (constitution XI; workflow R5).

## Evidence separation (status, not proof)

- Prior-step evidence reused as baseline only: 006 merge `8e1a440`, 007 merge `19fa11e`, workflow
  docs `d9096db`/baseline `5b0bf4a`. Nothing in this step re-verifies 006/007 or claims their
  verification.
- This step produced **documentation only** (plan/research/data-model/contracts/quickstart); no
  code, migrations, tests, services, or containers were written or executed; V1–V8 are design-only
  and MUST be executed in tasks/implement.
- Bilateral conformance with 008 is limited to the stated lock/read contracts (008 read-api §4
  scope-row `FOR SHARE`; 009 persistence §§1–2 joint participation; fixed lock order); full
  integration acceptance still waits for 008. No conformance is claimed with 010 or 011; the
  downstream statements there are one-directional consumption contracts.
- T000-P remains open; local success (future) does not imply production readiness.

## Deferred items (explicit, with owner)

| ID | Item | Owner |
|---|---|---|
| T000-P | Production KMS/HSM provider selection/integration (interface ready; no provider ships) | later production track |
| D3 | 008 binding read adapter + five-class integration acceptance (interface defined in gates.md §3); the R6 `ReadBinding` serialization obligation is now bilateral (008 read-api §4 scope-row `FOR SHARE`; 006 ordering stays 009-side R6) — final integration acceptance still waits for 008 | 008 contract availability |
| G-1 | **CLOSED in-plan** (2026-09-16): the pause/revoke-vs-delivery window is closed by the gate-table `SHARE` lock (research R6); no 006 change and no deferred residue | 009 plan (R6) |
| D-1 | 007 carrier gap: no `intent_id`/`request_id` linkage — concrete closure carrier/owner/entry/protocol in research R11 (`withdrawal_authorization_scopes`); 009 fails closed until available | 007/011 extension (R11) |
| D-2 | 007 carrier gap: no authorization version — closure via R11 `authorization_version`; the fingerprint remains a labelled surrogate, never a version | 007/011 extension (R11) |
| D-3 | 007 carrier gap: no fee-scope/purpose — OC-5 conditional rule restored (R7); reuse branch unreachable until the R11 carrier lands; fresh authorization is the operative branch, not the rule | 007/011 extension (R11) |
| D-4 | 007 carrier gap: no `revoked_at` (revocation observed as `state='revoked'`); closure column in R11 | 007/011 extension (R11) |
| Q-A/Q-B | **Decided 2026-09-16** (research R11 resolutions): Q-A → trusted-issuance control without mandatory per-grant cryptography (`--operator` audit-only; authenticated+authorized issuer; explicit trust boundary; evidence-or-fix in tasks); Q-B → per-grant `authorization_unverifiable` refusal, no bulk backfill, re-issuance with traceability and no second intent/nonce or silent rebinding; neither self-certifies merge/deploy readiness | business ruling recorded (007/011) |
| Merge order | Development of 008 (`000008`) and 009 (`000009`) may proceed in parallel with fail-closed behavior, but NEITHER merges early on the back of incompleteness: 008 merges on its own complete acceptance; 009 merges only with the scopes-carrier batch (a 007-extension batch owns the additive migration, the extended `withdrawal-authz supply` entry, and the Q-A trusted-issuance permission control — recorded assignment, overridable; 011 is a consumer) PLUS full legal-path acceptance (new-grant path, controlled entry, refusal paths, recovery scenarios per Q-A/Q-B rulings). 011 capabilities MUST NOT be treated as available now. | 007-extension batch + 009 acceptance |
| F-1 | 010/011 consumption fixtures: a contract-conforming test caller (OC-4 input shape) keeps 009 developable; no 010/011 specs, tables, or fixtures beyond the test caller are created here | 010/011 (later specs) |


