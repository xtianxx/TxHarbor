# Implementation Plan: 007 Withdrawal Creation & Query (Receive-Only)

**Branch**: `007-withdrawal-creation` | **Date**: 2026-09-15 | **Spec**: [spec.md](spec.md) (clarify 5/5, zero markers)

**Input**: Feature specification from `/specs/007-withdrawal-creation/spec.md`

## Summary

Receive-only, authenticated, authorized idempotent withdrawal intake: `POST /withdrawals`
persists an Accepted request (never executes), `GET /withdrawals/{id}` serves owner-restricted
facts. Technical approach (research R1–R8): deterministic SHA-256 API-key credentials against
`caller` + `api_key` tables with same-query revocation; intake as plain-INSERT → catch 23505 →
classify transactions over `UNIQUE (caller_id, idempotency_key)` + `UNIQUE (authorization_id)`
carriers, reusing the repo's shared `writeGuard`/lease/lock primitives and audit-in-tx discipline.
Recovery-period behavior is structural (no execution code exists in 007) with live 006 state reads.

## Technical Context

**Language/Version**: Go (repo toolchain 1.26.5, `go.mod`; money as integer `NUMERIC(78,0)` ↔
`math/big`, addresses as lowercase hex; zero float path per constitution).

**Primary Dependencies**: pgx v5 (pool + tx), go-ethereum (address checksum validation only),
goose migrations, existing `internal/{config,db,health,logx,metrics}` (no new packages for
cross-cutting concerns).

**Storage**: PostgreSQL only (postgres:18.6 pin — PG19 `ON CONFLICT DO SELECT` unavailable by
design). New: `caller`, `api_key`, `withdrawal_requests`, `withdrawal_authorizations`,
`withdrawal_request_audit` (data-model.md Tables 1–5). No Redis/Kafka/new infra.

**Testing**: `go test ./...` (unit: validation, equivalence, error mapping) +
`make test-integration` (real PostgreSQL: concurrency/crash/revocation races; Anvil E2E for
recovery-period zero-side-effect) + quickstart V1–V9 as the scenario matrix (design only here).

**Target Platform**: Linux server, single deployment single chain (constitution architecture evolution).

**Project Type**: Backend wallet/transaction infrastructure (monorepo; new `internal/withdrawal`
writer + `internal/app` HTTP wiring, following the `internal/indexer` one-concern-per-file layout).

**Performance Goals**: Intake poses no throughput target; every transaction is 3–6 point
statements under the shared 5s `writeGuard`; auth lookup is one indexed row; no benchmark claims.

**Constraints**: Bounded retries only (reuse INDEX timing triple for matching failure classes);
zero RPC inside any DB transaction; DB `now()` as the only clock for revocation/expiry validity;
exact integer amount math; secrets never in logs/repo/errors (`logx.Redact` + middleware allowlist).

**Scale/Scope**: Operator-issued key cardinality (tens, not millions); permanent retention
(FR-11) with append-only audit; single chain.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Pre-Phase-0 (2026-09-15, against Constitution 1.1.0):
- I (Financial correctness): PASS — integer-only amounts, DB CHECK/UNIQUE carriers, no duplicate
  intent possible by construction (R6/R7).
- II (Idempotency): PASS — persistence-layer UNIQUE + transactional bind (R6–R8), never app-memory.
- III (PostgreSQL truth): PASS — all durable state in PostgreSQL; no cache/Redis (R4).
- IV (Reorg-aware): PASS — 006 contracts consumed read-only; queries never present mixed views (contracts §4).
- V (Explicit state machine): PASS — single `accepted` state; no other 007 status exists by CHECK.
- VI (Atomic boundaries): PASS — auth-check + bind + insert + audit in one tx (R8).
- VII (Nonce): N/A — 007 allocates no nonces by design (FR-19); recorded as downstream boundary.
- VIII (Key isolation): PASS — API keys are auth credentials, not signing secrets; signer boundary
  untouched; no private key material anywhere in 007.
- IX/X/XI (Failure paths, local testing, invariant tests): PASS — V1–V9 matrix, failure-first.
- XII (Observability): PASS — structured logs with caller/request/chain/asset/retry fields, no
  secrets; metrics ride the existing registry (new counters named per repo shape).
- XIII (Simplicity): PASS — no new infra/services; two small tables + three business tables.
- XIV (Spec-driven): PASS — this plan; scope fenced by Non-Goals.

Post-Phase-1 re-check (2026-09-15): no new violations introduced. Data-model adds tables because
upstream tables cannot hold caller credentials or withdrawal intents without breaking their stream
contracts (same justification shape as 006 Complexity Tracking). Error-code vocabulary and field
naming stay inside plan scope (contracts/api.md), not implementation.

## Project Structure

### Documentation (this feature)

```text
specs/007-withdrawal-creation/
├── plan.md                 # This file (/speckit.plan output)
├── research.md             # Phase 0 output (R1–R8 decisions)
├── data-model.md           # Phase 1 output (Tables 1–5 + txn catalog + concurrency argument)
├── quickstart.md           # Phase 1 output (V1–V9 validation guide, design only)
├── contracts/              # Phase 1 output
│   └── api.md              # HTTP/validation-order/query-shape/recovery/downstream contract
├── checklists/
│   └── requirements.md     # specify/clarify artifact (21/22; intentional tier item open)
└── tasks.md                # Phase 2 output (/speckit.tasks — NOT created here)
```

### Source Code (repository root)

```text
internal/withdrawal/
├── auth.go                 # key verify (sha256 + constant-time + revocation predicate), caller load
├── validate.go             # FR-04–FR-07 input validation + EIP-55 + canonicalization
├── intake.go               # T-accept/T-replay/T-conflict/T-auth-bound/T-reject/T-unavailable txns
├── query.go                # ownership-enforced read + recovery-state annotation
├── grant.go                # upstream grant supply entry (controlled, audited; NOT caller-writable)
└── ..._test.go             # unit: vectors, equivalence, error mapping (no DB where possible)

internal/app/              # EXTEND: mount POST/GET on existing http.Server; config passthrough
internal/config/config.go  # EXTEND: chain bind + HTTP addr reuse (no new secret knobs in 007)
internal/metrics/          # EXTEND: intake counters/gauges on existing registry
internal/logx/             # reuse Redact (no change expected)

migrations/
└── 000007_withdrawal_creation.sql  # Tables 1–5 (R1–R8 carriers; pure DDL)

tests (per V-matrix; design only, not written here):
├── unit: go test (validation/equivalence/mapping)
├── integration: real PostgreSQL (concurrency/crash/revocation/restart races)
└── e2e: Anvil (recovery-period receive + zero-side-effect + privacy shape)
```

**Structure Decision**: follow the existing `internal/indexer` writer layout (one concern per file,
shared lease/coordinator consts untouched); migration follows the numbered goose sequence. No new
packages for config/logging/metrics; one new `internal/withdrawal` domain package. No new services.

## Complexity Tracking

None. (New tables in one migration are required carriers — upstream tables cannot hold caller
credentials or withdrawal intents without breaking their contracts; see R1/R3/R7 alternatives
rejected. Constraints-only intake adds no lock object.)

## FR/SC/Scenario → design/verification mapping

| FR | Design carrier | Verified by (future) |
|---|---|---|
| FR-01 (endpoints+auth) | app wiring + auth.go | V1, V2 |
| FR-02 (identity vs permission) | caller row + `can_create` + key binding | V2 |
| FR-03 (API key) | R1/R2/R4/R5; Table 2 | V2, V7 |
| FR-03b (grant model) | Table 4 + supply entry + atomic bind (R7/R8) | V3, V6, V7 |
| FR-04/FR-05 (chain/whitelist) | validate.go + deployment bind | V4 |
| FR-06/FR-07 (amount/address) | validate.go; NUMERIC(78,0) + hex CHECKs | V4, V5 |
| FR-08 (Accepted semantics) | status CHECK + contracts §3 | V1, V8 |
| FR-09/FR-10 (key scope/compare) | `caller_key_uniq` + FR-10 set (R6) | V5, V6 |
| FR-11 (permanent) | no cleanup path; full UNIQUEs (R1/R4) | V6, V7 |
| FR-12/FR-13 (atomic/concurrent) | txn catalog T-* + concurrency argument | V6 |
| FR-14/FR-15 (responses/privacy) | contracts/api.md §§1–3 | V1–V3, V9 |
| FR-16/FR-17 (recovery) | contracts §4; read-only 006 consumption | V8 |
| FR-18/FR-19 (upstream/downstream) | grant supply + downstream handoff | V3 + review |
| FR-20/FR-21 (no-float/secrets, logging) | types + Redact + allowlist | V1–V9 (log assertions) |
| SC-01–SC-08 | counters above | V1–V9 respectively |

006 inputs consumed (read-only, `8e1a440`): FR-26 receive-only + zero-side-effect boundary;
FR-18 validity vocabulary (subset); downstream.md preconditions subset; observability series
naming shape for new metrics. Downstream待承接 (008–011, no specs): `request_id` + grant
identity + immutable params + one-request↔one-intent association + re-auth/cancel surface.

## Verification plan (this step writes the plan; execution belongs to tasks/implement)

- Migration verification: goose up/down reproducibility on scratch DB; CHECK/UNIQUE negative tests.
- 002–006 regression: `go test ./...` + `make test-integration` must stay green; zero 002–006 file
  modifications outside the app-wiring extension points listed above (diff-gated in tasks).
- Interface tests: contract-shape tests for every row of contracts/api.md §1 table (status codes,
  404 byte-equality, retry-with-same-key instruction text).
- CI: existing `ci.yml` four jobs cover 007 with no workflow change (unit+race, integration-Docker,
  build, lint). Historical CI (34918673432) is 006 evidence, NOT 007 verification — stated here so
  no later step miscites it.
- Residual risks: EIP-55 dependency surface (go-ethereum already vendored); operator-tooling UX
  (no admin UI by design); `FOR SHARE` strictness default-off (plan default: constraints-only).

## Evidence separation (per instruction — status, not proof)

- 006 merge `8e1a440` + main CI 34918673432 (4/4 green): prior-step evidence, reused as baseline
  only; nothing here re-verifies 006 or claims 007 verification.
- T000-P: open per spec Assumptions; neither blocks nor substitutes for 007 verification.
- No implementation code written or executed in this step; V1–V9 are design-only.
