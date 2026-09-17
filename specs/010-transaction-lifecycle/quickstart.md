# Quickstart: 010 Transaction Lifecycle Management — validation guide

**Branch**: `010-transaction-lifecycle` | **Date**: 2026-09-17 | **Spec**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md)

This guide defines the runnable validation matrix for 010. **This step designs the scenarios only**; they
are executed in tasks/implement. Two layers, per FR-16:

- **V1–V11 — 010-independent**: real PostgreSQL + real Anvil + real `signer-serve` (disposable local dev
  key) + the test HTTP proxy for 009-boundary fault injection. The 011-owned `execution_claims` table does
  not exist on this branch, so these tests provision a **contract-shaped fixture** in the scratch database
  (columns exactly per the frozen J2 shape) — labeled test-only, never joint evidence, never a migration.
- **J1–J4 — joint requirements**: real 007 HTTP + 011 executor + 010 + 009 + PG + Anvil. Defined here;
  executed only after 011's real implementation + migrations are integrated into the integration workspace,
  with applicable joint gates completed **BEFORE 011 merges to main** (the contract-shaped fixture, any mock,
  or a 010-independent pass never substitutes). Mocks, fixtures and 010-independent results never count as
  joint acceptance.

## Prerequisites

- Docker daemon (testcontainers) + `make test-integration` (`Makefile:13-14`); unit layer: `make test`,
  race layer: `make test-race`.
- Environment for manual runs: `TXHARBOR_PG_DSN`, `TXHARBOR_RPC_URL` (Anvil),
  `TXHARBOR_CHAIN_ID=31337`, `TXHARBOR_TX_SIGNER_URL`, `TXHARBOR_TX_SIGNER_CREDENTIAL`,
  `TXHARBOR_TX_SEND_TIMEOUT`; reconcile cadence/RPC timeout reuse `TXHARBOR_INDEX_*` (R-010-12).
- Upstream state for a send: a 007 request → 011 intent (joint only; for V-tests, the intent id is a
  fixture value), an 008 binding row for `(intent_id, chain_id, sender, nonce)`, an active 007 grant with a
  PB scope row, and (fixture) an active claim row. The 009 caller must be a provisioned
  `signer_caller`/`signer_credential` with `can_sign = TRUE`.
- Isolation: workdir-local database name and non-default ports (009 R10 precedent); Anvil image pinned as
  in `compose.yaml` / `internal/nonce/unknown_outcome_integration_test.go:60-98`.

## V1 — Construction and pre-persistence (FR-01, FR-12, SC-01)

Drive `PrepareAttempt` with a valid request; assert: the row exists **before** any 009 request is observed;
identity↔content binding holds; `content_hash` is stable across runs. Then re-drive with the same identity
and identical content → converges on the original row (no second attempt, no content change); re-drive with
the same identity and a different amount/fee → `attempt_conflict`, zero writes. Race two concurrent inserts
of the same identity → one row, one conflict-or-replay. **Exit**: exactly one attempt row per identity; no
partial identity; conflict leaves the original untouched.

## V2 — Bytes land before send; hash cross-check (FR-02, SC-01/SC-03)

With a real `signer-serve`: assert T2 commits `signature` + `signed_tx_bytes` + `tx_hash` **before** the
first dispatch; assert `keccak256(bytes) == tx_hash == 009.tx_hash` and the recovered sender matches.
Tamper the signature (test hook) → `signature_mismatch`, zero dispatch, event recorded. Fixed-vector test
pins reconstruction determinism. **Exit**: no dispatch is ever observable before the signing row is
committed.

## V3 — Replay identity (FR-04, SC-03)

With an accepted dispatch recorded, drive `SendReplay`: assert the dispatched bytes equal the persisted
bytes byte-for-byte; assert no new attempt row, no new `signing_request_id`, no new 009 request, and
`send_seq` increments. Attempt a replay with a revoked grant / changed recovery version → refusal with the
observed basis, zero dispatch.

## V4 — Timeout and response loss → unknown (FR-03, SC-02)

Inject (proxy) a dispatch timeout, a response drop after the node accepted, a 429, an invalid response, and
a returned-hash mismatch. Assert each yields a durable `unknown`/`rejected` send row per the classification
matrix, the attempt moves to `unknown` (business effect undetermined), **no** row is marked success or
failure, and the bytes/hash remain intact. Assert a loss after COMMIT but before the caller sees the
response is resolved by the caller's retry (`already_accepted` for `SendInitial`, replay allowed) with no
duplicate attempt.

## V5 — Reconcile protocol (FR-03, SC-02)

For an `unknown` attempt: assert `Reconcile` records observations for `not_found_yet` (tx absent from
mempool — never a failure), `found_pending`, `included`, and `unavailable` (RPC down). Assert `not_found_yet`
alone never marks failure; that a dispatch from `unknown` requires the observation first; and that repeated
reconcile converges (no duplicate receipt rows). **Exit**: zero "unknown → failed/unpaid" conversions.

## V6 — Gate matrix and race ordering (FR-07, FR-11, FR-14, FR-15, SC-01/SC-06)

Refusals, each with zero `eth_sendRawTransaction` calls and a committed evidence row: claim absent /
version mismatch / expired / revoked; pause present (each of the three tables, plus multi-pause where
releasing one does not permit send); active recovery; recovery-version change; grant missing/inactive/
revoked/expired/mismatched; scopeless grant (`authorization_unverifiable`); binding absent/conflict/paused/
terminal; registry disabled; active 008 hold; non-sendable attempt state.

Races (both directions):
- **(a)** commit a revocation/pause/claim update, then start the region → refusal observed.
- **(b)** start the region and, while it holds locks, attempt the same invalidation → the writer must block
  until region commit; the send is recorded as in-flight with its gate snapshot; the invalidation governs
  later replays (drive a replay after it and assert refusal).
- **(c)** claim fencing: an old `lease_version` refuses; a takeover version + re-verification sends on the
  same intent/binding without a second intent; concurrent old/new workers serialize on the claim row.
- **(d)** simultaneous `Send`/`SendReplay` on one attempt serialize on the attempt row; `send_seq` is
  unique; a stale `ExpectedRevision` refuses with `send_stale`.

**Exit**: zero sends under any blocked gate (SC-06); zero dispatch for refusals; observed lock-block order
recorded in the test evidence.

## V7 — Replacement (FR-05, FR-06, SC-04)

Reuse branch: scope permits replacement and all three fee dimensions are in range → new attempt (new
`attempt_id`/`signing_request_id`) with `replacement_of` set, same intent/binding/sender/nonce and
unchanged semantics; 009 signs it; history of the original attempt is untouched. Forbidden branch: scope
does not permit → `scope_reuse_forbidden`; any fee dimension over cap → `fee_scope_exceeded` with the
per-dimension evidence; both zero dispatch. Fresh branch: a new grant passes and anchors the new attempt.
Non-fee-changing replacement → `replacement_no_fee_change`; mismatched intent/binding/semantics →
`replacement_mismatch`. **Exit**: 100% traceability to one intent/binding; no old bytes reused.

## V8 — Receipt and expected Transfer (FR-08, SC-05)

Anvil scenarios: successful transfer (effective, enters confirmation); receipt `status = 0`; missing
Transfer; wrong recipient/amount/emitter; transfer with extra logs. Assert exact verdicts and never
"successful" on any mismatch. Assert canonicality is decided against `chain_blocks` and a receipt at an
unindexed height stays `unverified`.

## V9 — Confirmation progress, reorg revision, replaced marking (FR-09, SC-07)

Confirm to the 005 policy threshold → `confirmed` with basis. Then reorg the block away (Anvil reorg):
assert the receipt becomes `orphaned`, a revision event carries the observed recovery version, the attempt
is revised without a new intent/binding/payment, and re-inclusion in the new canonical chain yields a new
receipt row + `reconfirmed`. When a confirmed attempt shares a binding with siblings, assert siblings are
revised to `replaced`. Crash matrix (V9b): kill the process at each T-boundary of data-model's crash table
and assert the recovery action there. **Exit**: zero rebuilt intents; 100% revision tracking.

## V10 — Read-only boundary, projection version, migration (FR-12, FR-13)

Assert the read-only diff over all upstream tables after the full V-matrix (persistence.md §6); assert
`revision_seq` monotonicity and that `Status` never triggers a send; assert `000011` up/down on a scratch
DB with named-constraint probes (`tx_attempts_pkey`, `tx_attempts_signing_request_uniq`,
`tx_attempt_signings_tx_hash_uniq`, `tx_attempts_binding_fkey`, `tx_attempts_authorization_fkey`,
`tx_send_attempts_attempt_seq_uniq`, `tx_attempt_events_attempt_seq_uniq`, `tx_receipts_tx_block_uniq`);
assert the claim adapter fails
closed when the fixture table is absent. **Exit**: no 010 write to any upstream table; reproducible
migration; clean fail-closed behavior pre-011.

## V11 — Secrecy, observability, boundary (FR-10, SC-01..07 assertions)

Unit/import tests: `internal/txlifecycle` imports no key/provider package and exposes exactly one dispatch
call site; logs/metrics contain no signature, signed bytes or credentials; counters for send outcomes, gate
refusals, unknown gauge, reconcile classes, receipt effects and revisions exist and are recorded on the
V-scenarios. **Exit**: static boundary holds; observability present at introduction (constitution XII).

## J1–J4 — joint acceptance requirements (defined here; executed later)

- **J1 intent/authorization wiring**: 011 creates the intent + claim from a real 007 request; 010 consumes
  the real claim and the real grant/scope; the full path sends on Anvil and reaches a verified receipt.
- **J2 expired-worker isolation**: an expired/fenced worker's sends are refused by 010 while a legitimate
  taker, after re-verifying current authorization/pause/recovery version/claim, resumes on the same intent,
  nonce and attempt history — no second intent.
- **J3 joint unknown reconciliation**: a real dispatch timeout/response loss in the joint stack ends in
  `unknown`; the joint reconcile flow (011 loop → `Reconcile`) resolves or honestly persists it; no
  repaying.
- **J4 reorg revision**: a joint reorg after confirmation revises 011's projection in revision order, keeps
  the original intent, and never rebuilds payment.
- **J5 lock-loss residual, detectable path only** (G-010-2 class (c), 2026-09-17 exception): fault injection kills
  the region's DB session after the gate reads without the sender's knowledge, commits a pause, then lets the
  network send proceed. Assert: the send row's recorded versions vs the pause evidence prove a stale basis →
  the intent's further sends freeze pending manual review (chain observation/query/reconcile stay available);
  unresolvable ordering stays indeterminate (never defaulted to safe); manual review lifts only this freeze
  cause and any resend re-verifies all current gates. Assert what is NOT claimed: no proof the window is
  tiny/rare; no detection guarantee for unobservable faults.

Joint exit requires real HTTP (007 + 009), real PG, real Anvil; the contract-shaped fixture, any mock, or a
010-independent pass **never** substitutes (FR-16, workflow P5).

## Not in this step

No test, migration, code, service or container is written or run by this planning step; every scenario
above is design-only. T000-P (production provider) and A-13 (full-chain E2E) remain OPEN.
