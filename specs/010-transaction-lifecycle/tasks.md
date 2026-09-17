# Tasks: 010 Transaction Lifecycle Management（交易生命周期管理）

**Input**: Design documents from `specs/010-transaction-lifecycle/` — spec.md (clarify `bd59754`, Q1–Q3 closed), plan.md, research.md (R-010-01..14 + G-010-1..6), data-model.md (Tables 1–6), contracts/ (send-api.md, send-gate.md, signer-call.md, persistence.md), quickstart.md (V1–V11 + J1–J5), checklists/requirements.md; `.specify/memory/constitution.md` v1.1.0; `docs/project-context.md`; `docs/workflow-010-011-parallel.md` (joint contract v1: P1–P5, C1–C12, J1–J6, provisional migrations 000011=010 / 000012=011 / PB occupies 000010, merge follow-through records).

**Prerequisites**: plan.md (required) + spec.md (required for user stories) confirmed present; research.md, data-model.md, contracts/, quickstart.md present (verified via the two read-only scripts below).

**Tests**: TDD was not requested by the spec or plan; no tests-first unit tasks are created. Spec FR-16 and the plan Verification plan require per-story validation tasks executing the quickstart V-matrix on real PostgreSQL + real Anvil + real `signer-serve` (the contract-shaped `execution_claims` fixture is test-only, never joint evidence). J1–J5 are executed only in the joint integration wave (Phase 9).

**Organization**: tasks are grouped by user story (US1–US5) in spec priority order; a residual-adjudication phase (Phase 8), the joint integration wave (Phase 9) and Polish (Phase 10) follow.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel (different files, no dependency on an incomplete task)
- **[Story]**: US1–US5 for user-story phase tasks only; Setup / Foundational / residual / joint / Polish tasks carry no story label
- Every task names its exact file path
- All tasks are unchecked; nothing in this file was executed

## Inputs & constraints (these are INPUTS — do not re-open, do not weaken)

- Adjudicated business rulings are inputs: Q1 (OPEN-1a), Q2 (OPEN-1b), Q3 (OPEN-2) — all 2026-09-17; the G-010-1 limited natural-expiry exception (2026-09-17); the G-010-2 second limited lock-loss exception (class (c), 2026-09-17); OC-1–OC-7, PB-C1/C2, 006 FR-26; lease/heartbeat/stall values are 011-owned frozen-contract inputs. Tasks MUST NOT re-ask or weaken them.
- Migration numbers are provisional: `000011` = 010, `000012` = 011, `000010` = PB (occupied). Re-verify the actual set at merge; never renumber or rewrite applied migrations (PLAN-1).
- COMMIT-failure semantics (mandatory): a failed COMMIT MUST NOT mechanically rewrite everything to `unknown`. Three facts stay distinct: (i) confirmed-not-sent this attempt → `region_aborted_no_dispatch` (not a send result; business effect stays `unknown` pending reconcile); (ii) a previously-`unknown` business effect stays persisted pending reconcile; (iii) known RPC/on-chain results (`accepted`/`rejected` send rows, verified receipts) are preserved and never rewritten. Recovery bases are persistable evidence (committed send rows' gate snapshots, `tx_hash`, receipts) — never inference.
- Joint wording (mandatory): joint acceptance is executed only after integrating 011's real implementation + migrations into the integration workspace; applicable joint gates complete BEFORE 011 merges to main. Any phrasing that places joint acceptance after 011's mainline merge MUST NOT be used.
- T000-P (production provider selection) and A-13 (full-chain E2E) remain OPEN: constraints, not tasks owned here.

## Applicability check (read-only, run before generation)

- `SPECIFY_FEATURE_DIRECTORY="specs/010-transaction-lifecycle" bash .specify/scripts/bash/setup-tasks.sh --json` → `FEATURE_DIR=/home/dream/product_env/TxHarbor/.slim/worktrees/prep-010-tx/specs/010-transaction-lifecycle`, `AVAILABLE_DOCS=[research.md, data-model.md, contracts/, quickstart.md]`, `TASKS_TEMPLATE=…/.specify/templates/tasks-template.md`.
- `bash .specify/scripts/bash/check-prerequisites.sh --json` → same FEATURE_DIR/docs; plan.md + spec.md confirmed present.
- All tasks below match `- [ ] Tnnn [P?] [USn?] <description with exact file path>`; none are checked.

## Wave overview

| Wave | Phases | Entry gate | Exit evidence |
|---|---|---|---|
| W0 | Phase 1–2 (T001–T014) | design docs approved (no re-plan) | migration up/down + named-constraint probes |
| W1 | Phase 3 / US1 (T015–T022) | W0 checkpoint | V1, V2, V6 |
| W2 | Phase 4 / US2 (T023–T027) | W1 (send region exists) | V4, V5 |
| W3 | Phase 5–7 / US3–US5 (T028–T038) | W1; single-writer on `send.go` | V3, V7, V8, V9, V9b |
| W4 | Phase 8 (T039–T041) | W1–W3 | residual evidence tests incl. freeze/release |
| W5 | Phase 9 joint wave (T042–T051) | 011's real implementation + migrations available in the integration workspace | J1–J5 executed; applicable joint gates complete BEFORE 011 merges to main |
| W6 | Phase 10 polish (T052–T058) | W1–W4 | V10, V11, doc-sync |

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: shared scaffolding with no story-specific behavior; disjoint files, parallelizable.

- [x] T001 [P] Add the 010 caller knobs to `internal/config/config.go`: `TXHARBOR_TX_SIGNER_URL` (009 base URL), `TXHARBOR_TX_SIGNER_CREDENTIAL` (secret, never logged), `TXHARBOR_TX_SEND_TIMEOUT` (default 15s, MUST be > 0, fail-closed startup validation); no other timing knobs — reconcile cadence/RPC timeout reuse `TXHARBOR_INDEX_POLL_INTERVAL`/`TXHARBOR_INDEX_RPC_TIMEOUT` (R-010-12).
- [x] T002 [P] Extend `internal/eth/client.go`: `SendSignedTransaction` (raw `eth_sendRawTransaction` + returned-hash equality with the persisted `tx_hash`), `TransactionByHash`, `TransactionReceipt`, `BlockNumber`, and send-error classification into the existing `eth.Kind` vocabulary with send-specific classes added alongside (never replacing; constitution §RPC; R-010-12).
- [x] T003 [P] Extend `internal/metrics/metrics.go` (+ `internal/metrics/signer.go` only if the registry is feature-scoped): dispatch-outcome counters, gate-refusal counter by class, unknown gauge, reconcile-class counters, receipt-effect counters, reorg-revision counter (R-010-13; constitution XII; asserted by V11).
- [x] T004 [P] Create `internal/txlifecycle/errors.go`: refusal classes + retryability exactly per `specs/010-transaction-lifecycle/contracts/send-api.md` §3 (`claim_absent`/`claim_version_mismatch`/`claim_expired`/`claim_revoked`; `pause_present`/`recovery_active`/`recovery_version_changed`; `gate_read_failed`; `binding_absent`/`binding_conflict`/`binding_paused`/`binding_terminal`/`binding_read_failed`; `authorization_missing`/`authorization_inactive`/`authorization_revoked`/`authorization_expired`/`authorization_mismatch`/`authorization_unverifiable`; `scope_reuse_forbidden`/`fee_scope_exceeded`; `attempt_conflict`/`hash_conflict`/`replacement_mismatch`/`replacement_no_fee_change`; `already_accepted`/`send_mode_mismatch`; `attempt_not_sendable`/`attempt_not_found`/`send_stale`; `signature_refused`/`signature_mismatch`; `coordination_unavailable`) + `tx_attempt_events.event` constants mirroring the data-model Table 6 CHECK list.
- [x] T005 [P] Create `internal/txlifecycle/attempt.go`: `PrepareRequest` schema (attempt_id + signing_request_id 1:1; replacement_of; intent_id; binding_ref; authorization_id + authorization_version >= 1; recovery_version >= 0; chain_id > 0; sender; nonce; tx_type 0|2 with exact one-of fee shapes; asset/recipient/amount >= 1) + shape validation + canonical 009 request envelope serialized once and stored verbatim + `content_hash = keccak256(domain-tagged economic projection)` over chain/sender/nonce/to/value/data/gas/fees without attempt/signing ids (`content_hash` deliberately NOT UNIQUE; regex `^0x[0-9a-f]{64}$`) (R-010-02; data-model Table 1; send-api §2.1).
- [x] T006 [P] Create `internal/txlifecycle/calldata.go`: ERC-20 `transfer(address,uint256)` calldata builder + expected-Transfer extraction (emitter = asset, topic1 = sender, topic2 = recipient, 32-byte data = amount; exactly 3 topics, 20-byte zero-padded slots) reusing `eth.TransferSig` (R-010-08; consumed by V8).

**Checkpoint**: shared scaffolding builds (`go build ./...`); no story work started.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: core infrastructure every user story depends on.

**CRITICAL**: no user story work can begin until this phase completes.

- [x] T007 Create `migrations/000011_tx_lifecycle.sql` with goose headers and Table 1 `tx_attempts`, constraints verbatim: `tx_attempts_pkey PRIMARY KEY (attempt_id)`; `tx_attempts_signing_request_uniq UNIQUE (signing_request_id)`; `tx_attempts_replacement_fkey FOREIGN KEY (replacement_of) REFERENCES tx_attempts (attempt_id)`; `tx_attempts_binding_fkey FOREIGN KEY (binding_ref) REFERENCES nonce_bindings (binding_id)`; `tx_attempts_authorization_fkey FOREIGN KEY (authorization_id) REFERENCES withdrawal_authorizations (authorization_id)`; `tx_attempts_state_check CHECK (state IN ('prepared','signed','sent','unknown','effective','ineffective','confirmed','orphaned','replaced'))`; `tx_attempts_attempt_shape CHECK (length(attempt_id) BETWEEN 1 AND 128 AND attempt_id ~ '^[\x21-\x7e]+$')`; same shape checks for `signing_request_id` and `intent_id`; `tx_attempts_replacement_not_self CHECK (replacement_of IS NULL OR replacement_of <> attempt_id)`; `chain_id > 0`; `sender ~ '^0x[0-9a-f]{40}$'`; `to_addr ~ '^0x[0-9a-f]{40}$'`; `asset = to_addr`; `recipient ~ '^0x[0-9a-f]{40}$'`; `value` 0..115792089237316195423570985008687907853269984665640564039457584007913129639935; `amount` 1..same max; `nonce` 0..18446744073709551615; `recovery_version >= 0`; `authorization_version >= 1`; `gas_limit > 0`; `tx_attempts_fee_shape_check CHECK ((tx_type = 0 AND gas_price IS NOT NULL AND max_fee_per_gas IS NULL AND max_priority_fee_per_gas IS NULL) OR (tx_type = 2 AND gas_price IS NULL AND max_fee_per_gas IS NOT NULL AND max_priority_fee_per_gas IS NOT NULL AND max_priority_fee_per_gas <= max_fee_per_gas))`; `content_hash ~ '^0x[0-9a-f]{64}$'`; `revision_seq > 0`; `tx_attempts_state_facts_check` implication set (confirmed ⇒ confirmed_at; orphaned ⇒ orphaned_at; replaced ⇒ replaced_at; effective/confirmed ⇒ effective_at); indexes `tx_attempts_scan_idx (state, updated_at)` and `tx_attempts_intent_idx (intent_id)`; identity/content columns immutable after insert (no UPDATE path) (data-model Table 1; migration provisional per PLAN-1).
- [x] T008 Extend `migrations/000011_tx_lifecycle.sql` with Table 2 `tx_attempt_signings` (`tx_attempt_signings_pkey PRIMARY KEY (attempt_id)`; `tx_attempt_signings_attempt_fkey FOREIGN KEY (attempt_id) REFERENCES tx_attempts (attempt_id)`; `tx_attempt_signings_tx_hash_uniq UNIQUE (tx_hash)`; `tx_attempt_signings_signature_check CHECK (signature ~ '^0x[0-9a-f]{130}$')`; `tx_attempt_signings_tx_hash_check CHECK (tx_hash ~ '^0x[0-9a-f]{64}$')`; `tx_attempt_signings_bytes_check CHECK (octet_length(signed_tx_bytes) > 0)`) and Table 3 `tx_send_attempts` (`tx_send_attempts_pkey PRIMARY KEY (send_id)`; FK attempt_id; `tx_send_attempts_attempt_seq_uniq UNIQUE (attempt_id, send_seq)`; `send_seq > 0`; `kind IN ('initial','replay')`; `outcome IN ('accepted','rejected','unknown')`; `tx_send_attempts_dispatched_check CHECK (outcome = 'unknown' OR dispatched_at IS NOT NULL)`; gate-snapshot columns `observed_recovery_version`/`observed_pause`/`observed_claim_version`/`observed_claim_expiry`/`observed_authorization_id`/`observed_authorization_version`/`observed_authorization_state`/`observed_expires_at`/`observed_now`/`observed_binding_state`; `rpc_class` vocabulary; index `tx_send_attempts_attempt_idx (attempt_id, send_seq)`), all constraints named (data-model Table 3).
- [x] T009 Extend `migrations/000011_tx_lifecycle.sql` with Table 4 `tx_reconciliations` (PK `reconcile_id`; FK attempt_id; `classification IN ('found_pending','included','not_found_yet','unavailable')`; `tx_hash ~ '^0x[0-9a-f]{64}$'`; `block_hash IS NULL OR block_hash ~ '^0x[0-9a-f]{64}$'`; `(block_number IS NULL) = (block_hash IS NULL)`; `classification <> 'included' OR block_number IS NOT NULL`; index `tx_reconciliations_attempt_idx (attempt_id, observed_at)`), Table 5 `tx_receipts` (PK `receipt_id`; FK attempt_id; `tx_receipts_tx_block_uniq UNIQUE (tx_hash, block_hash)`; `status IN (0,1)`; `effect IN ('effective','ineffective_status','ineffective_transfer_missing','ineffective_transfer_mismatch')`; `(effect = 'ineffective_status') = (status = 0) AND (effect <> 'effective' OR status = 1)`; `canonicality IN ('unverified','canonical','orphaned')`; orphan-facts implication `(canonicality <> 'orphaned' OR orphaned_at IS NOT NULL) AND (canonicality <> 'canonical' OR orphaned_at IS NULL)`; `block_number >= 0`; block_hash regex; `confirmations >= 0 AND confirmations = floor(confirmations)`; `confirm_threshold > 0`; `confirm_policy_seq > 0`; `confirmed_at IS NULL OR canonicality <> 'unverified'`; indexes `tx_receipts_attempt_idx (attempt_id, observed_at)` + partial `tx_receipts_open_idx (canonicality, block_number) WHERE canonicality <> 'orphaned'`) and Table 6 `tx_attempt_events` (PK `event_id`; `tx_attempt_events_attempt_seq_uniq UNIQUE (attempt_id, event_seq)`; `event_seq > 0`; `event IN ('created','replayed','attempt_conflict','signature_persisted','signature_refused','signature_mismatch','gate_refused','send_rejected','send_unknown','reconcile_observed','receipt_verified','receipt_ineffective','confirmed','orphaned','reconfirmed','replaced','unknown_cleared')`; no FK on attempt_id; index `tx_attempt_events_attempt_idx (attempt_id, event_seq)`), then `+goose Down` drops all six tables in exact reverse order (data-model Tables 4–6).
- [x] T010 [P] Create `internal/txlifecycle/store.go` (T1): exported `PrepareAttempt` → `SET LOCAL statement_timeout='5s'` → INSERT `tx_attempts` → classify 23505 by `ConstraintName` (`tx_attempts_pkey`, `tx_attempts_signing_request_uniq`) → equal `canonical_envelope` converges on the existing row, different envelope → `attempt_conflict` with zero writes and the original untouched; attempt fetch by id; no UPDATE path for identity/content columns (FR-01/FR-12; R-010-02; persistence.md T1).
- [x] T011 `internal/txlifecycle/signing.go`: submit the byte-identical `canonical_envelope` to `{TXHARBOR_TX_SIGNER_URL}/signer/v1/signing-requests` with the bearer credential (never logged); map 009 responses exactly per `specs/010-transaction-lifecycle/contracts/signer-call.md` §3 (200 signed; 409 refusals → `signature_refused` + basis, attempt stays `prepared`; 409 `request_conflict` → `attempt_conflict`, never overwritten; 503 `key_provider_*`/`storage_*`/`outcome_unknown`/`gate_read_failed` → delivery-unknown: retry the SAME identity + SAME envelope with bounded backoff, no new attempt/identity, no chain effect, never marked failed, never re-signed under a new identity; 401/403 and 400 malformed fail closed); reconstruct signed bytes locally (persisted content + 65-byte signature + `types.LatestSignerForChainID(chain_id)` → `MarshalBinary()` → `keccak256`; require `local_hash == 009.tx_hash` and recovered sender == attempt sender) and persist T2 (`tx_attempt_signings` insert + guarded `prepared→signed` + `signature_persisted` event; mismatch → `signature_mismatch`/`hash_conflict` fail closed, zero send) (FR-02/FR-10; R-010-03; signer-call.md §4).
- [x] T012 [P] Create `internal/txlifecycle/claim.go`: execution-qualification adapter over the frozen J2 shape — one `FOR SHARE` read keyed by `intent_id` requiring intent-unique claim row, worker identity equality, monotonic `lease_version` equality, `expires_at > clock_timestamp()`, active/not-revoked; the mapping table absorbs 011's concrete column names in one place; absent table/row → fail-closed `claim_absent`; natural expiry vs explicit revocation recorded with distinct basis and identical refusal effect (FR-14; J2/C6; G-010-4; send-api §4).
- [x] T013 [P] Create `internal/txlifecycle/projection.go`: authoritative `Status(ctx, attemptID)` read returning identity, state, `revision_seq`, `updated_at` (DB clock), `tx_hash` when persisted, latest dispatch outcome, latest reconcile class, latest receipt effect/confirmation basis, current recovery/authorization references; read-only, never triggers a send or gate bypass, never a permission (FR-13; R-010-13; Q1/C5; send-api §2.4).
- [x] T014 Verify `migrations/000011_tx_lifecycle.sql` reproducibility on a scratch DB: `up`/`down`/`up` clean, then negative probes expecting the exact named constraint (`tx_attempts_pkey`, `tx_attempts_signing_request_uniq`, `tx_attempt_signings_tx_hash_uniq`, `tx_attempts_binding_fkey`, `tx_attempts_authorization_fkey`, `tx_send_attempts_attempt_seq_uniq`, `tx_attempt_events_attempt_seq_uniq`, `tx_receipts_tx_block_uniq`) → 23505/23503/23514; read-only assertion diff on 006/007/008 tables (V10 migration portion; PLAN-1).

**Checkpoint**: schema + identity primitives + signing T2 + claim adapter + projection ready — user story work can begin.

---

## Phase 3: User Story 1 — 首次广播：落盘先行、门禁通过后发送 (Priority: P1) — MVP

**Goal**: first broadcast for a ready payment intent: attempt identity + immutable content persist before the first 009 call; signed bytes + local hash land before any external send; the send happens only inside the gate-verified send region; gate refusals keep attempt history and send nothing.

**Independent Test**: run the full first-broadcast flow on the local stack; assert the attempt row with intent/binding links exists before any 009 request; assert signed bytes + hash are persisted before the first dispatch; assert a gate failure records evidence with zero dispatch and keeps attempt history (quickstart V1/V2/V6).

### Implementation for User Story 1

- [x] T015 [US1] Extend `internal/txlifecycle/store.go`: revision/event machinery shared by send/reconcile/receipt paths — `event_seq` allocated under the attempt row lock in the same transaction, guarded state updates `WHERE attempt_id = $1 AND revision_seq = $2` (RowsAffected <> 1 → stale rollback, zero dispatch), `revision_seq` +1 per mutation, append-only `tx_attempt_events` (FR-09/FR-12; persistence.md §5).
- [x] T016 [US1] Create `internal/txlifecycle/gates.go`: the fixed-order gate read sequence (`specs/010-transaction-lifecycle/contracts/send-gate.md` §2): `SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='5s'` → claim `FOR SHARE` → coordination row `indexer_lease` `FOR UPDATE` → `LOCK TABLE indexer_pause, log_pause, deposit_pause, reorg_recovery, reorg_recovery_events IN SHARE MODE` → one-statement 006 gate read (zero pause rows, no active recovery row, `current_version == attempt.recovery_version`) → `nonce_scope_state(chain_id, sender)` `FOR SHARE` → 008 binding read (intent/chain/sender/nonce equality, state in (allocated,in_flight), registry active, zero active holds) → `withdrawal_authorizations` `FOR SHARE` + `withdrawal_authorization_scopes` `FOR SHARE` (state active, not expired on `clock_timestamp()`, field equality, intent equality) → `tx_attempts` `FOR UPDATE` + sendability + `send_seq`; every failure = fail-closed refusal class + recorded basis + zero dispatch (FR-07/FR-11/FR-14/FR-15; R-010-04; J3).
- [x] T017 [US1] Create `internal/txlifecycle/send.go`: exported `Send` T3 send region — refusal path commits zero-dispatch evidence (event + gate snapshot); dispatch path runs exactly one bounded `eth_sendRawTransaction` with the persisted bytes (`TXHARBOR_TX_SEND_TIMEOUT`), requires returned hash == persisted `tx_hash`, classifies fail-safe (`accepted`/`rejected`/`unknown` per R-010-05), inserts `tx_send_attempts` (gate snapshot + `rpc_class` + `dispatched_at`) + guarded attempt state transition + event in the same COMMIT; kind validation (`send_mode_mismatch`, `already_accepted`, `attempt_not_sendable`); `send_seq` unique per attempt; known send rows immutable (FR-03/FR-11/FR-15; send-api §2.2/§3).
- [x] T018 [US1] Extend `internal/txlifecycle/send.go`: residual handling — G-010-1: re-evaluate authorization and claim expiry with `clock_timestamp()` as the last read before dispatch entry and record `observed_expires_at`/`observed_now`; a natural expiry inside the residual is recorded as a post-final-check natural-expiry send residual and reconciled, never described as legally in-flight, no grace/TTL (queue/backoff/reconnect/retry re-evaluate gates); G-010-2(a): dispatch entered with protection verifiably held, then region-commit failure → `storage_region_failure` + send-`unknown`, probe `tx_hash`, never infer not-sent; G-010-2(b): reliably-preventable pre-dispatch abort (lock wait/deadlock/connection loss before dispatch is invoked) → record `region_aborted_no_dispatch` (NOT a send result; business effect stays `unknown` pending reconcile; committed refusals and definitive verdicts never rewritten); G-010-2(c) mitigation: same-session `SELECT 1` probe on the transaction holding the locks (never a reconnect), exactly one dispatch attempt, no transparent retry bypassing re-evaluation; definitive outcomes kept — only indeterminate outcomes become `unknown` (Q3; G-010-1; G-010-2(a)(b)(c); send-gate.md §4(c)(d)(e)).
- [x] T019 [US1] Create `internal/txlifecycle/testfixture_test.go` (build tag `integration`): provision the contract-shaped `execution_claims` fixture in the scratch DB only (columns exactly per the frozen J2 shape; created by test setup, never by a migration; labeled test-only; never joint evidence) (R-010-11; G-010-4; FR-16).

### Validation for User Story 1 (quickstart V-scenarios)

- [x] T020 [P] [US1] Execute V1 in `internal/txlifecycle/attempt_integration_test.go`: attempt row exists before any 009 request; identity↔content binding; `content_hash` stable; same identity + identical content converges; same identity + different content → `attempt_conflict` with zero writes; concurrent same-identity inserts → one row + one conflict-or-replay; unit assertions: schema strictness, canonical-envelope determinism, content-hash stability (FR-01/FR-12; SC-01).
- [x] T021 [P] [US1] Execute V2 in `internal/txlifecycle/signing_integration_test.go`: T2 commits signature + `signed_tx_bytes` + `tx_hash` before the first dispatch; `keccak256(bytes) == tx_hash == 009.tx_hash` and recovered sender matches; tampered signature → `signature_mismatch`, zero dispatch, event recorded; fixed-vector reconstruction determinism (FR-02; SC-01/SC-03).
- [x] T022 [P] [US1] Execute V6 in `internal/txlifecycle/send_integration_test.go`: gate refusals with zero `eth_sendRawTransaction` calls and committed evidence (claim absent/version mismatch/expired/revoked; each of the three pause tables + multi-pause; active recovery; recovery-version change; grant missing/inactive/revoked/expired/mismatched; scopeless `authorization_unverifiable`; binding absent/conflict/paused/terminal; registry disabled; active 008 hold; non-sendable state); race timelines (a) invalidation committed before the region → refusal observed; (b) invalidation during dispatch → writer blocks until region commit, in-flight recorded with gate snapshot, later replay refused; (c) claim fencing: old `lease_version` refuses, takeover version + re-verification sends on the same intent/binding without a second intent, old/new workers serialize on the claim row; (d) concurrent `Send`/`SendReplay` serialize on the attempt row, unique `send_seq`, stale `ExpectedRevision` → `send_stale`; unit assertions: dispatch classification table, fee arithmetic (`gas_limit × max_fee_per_gas` with `big.Int`), refusal-taxonomy retryability (FR-07/FR-11/FR-14/FR-15; SC-01/SC-06).

**Checkpoint**: US1 fully functional and testable independently (MVP).

---

## Phase 4: User Story 2 — 结果未知：超时与响应丢失进入对账 (Priority: P1)

**Goal**: timeouts, response loss and missing receipts produce durable `unknown` + reconciliation — never success, failure or unpaid; persisted facts survive; nothing rebuilds a payment.

**Independent Test**: inject send timeout and response loss; assert the attempt enters `unknown` with bytes/hash intact; assert the reconcile query returns unknown state + persisted facts + recovery conditions without creating a new intent or binding (quickstart V4/V5).

### Implementation for User Story 2

- [x] T023 [US2] Create `internal/txlifecycle/reconcile.go`: T4 reconcile engine — probe `eth_getTransactionByHash(tx_hash)` classified `found_pending`/`included`/`not_found_yet`/`unavailable`; append one `tx_reconciliations` row per observation (never edited); attempt transitions per the data-model state machine (`sent→unknown` on missing receipt, `unknown→sent` on found_pending, `signed→unknown` on evidence; never `unknown→failed`); `not_found_yet` is never a failure verdict; claim-free, observe-only, writes no upstream rows, idempotent under repetition (FR-03; R-010-06; G-010-5).
- [x] T024 [US2] Extend `internal/txlifecycle/reconcile.go`: enforce reconcile-before-dispatch — any `Send`/replay/replacement from `signed` (crash-ambiguous) or `unknown` MUST have a reconcile observation recorded in the current operation before dispatch; crash-point recovery (after T2 before region; mid-region before dispatch; after dispatch began before COMMIT) resolves probe-first, never assume-not-sent (FR-03; persistence.md §2/§3; R-010-06).
- [x] T025 [US2] Extend `internal/txlifecycle/reconcile.go`: unknown-recovery interface (J5) — given `(attempt_id, tx_hash)` return the unknown state + all persisted facts (bytes present, every dispatch row, every reconcile observation, last gate basis) + recovery conditions; append `unknown_cleared` when the effect becomes known; never auto-rebuilds intent/binding/payment; never sends (FR-03; J5/C9; send-api §2.3).

### Validation for User Story 2 (quickstart V-scenarios)

- [x] T026 [P] [US2] Execute V4 in `internal/txlifecycle/unknown_integration_test.go` (+ `internal/txlifecycle/faultproxy_test.go` for the test HTTP proxy between 010 and 009): inject dispatch timeout, response drop after node acceptance, 429, invalid response, returned-hash mismatch → durable `unknown`/`rejected` send rows per the classification matrix, attempt `unknown`, no row marked success/failure, bytes/hash intact; a loss after COMMIT before the caller sees the response is resolved by caller retry (`already_accepted` for `SendInitial`, replay allowed) with no duplicate attempt; assert `rejected`/`unknown` rows are never updated by later passes (FR-03; SC-02; send-gate §6).
- [x] T027 [P] [US2] Execute V5 in `internal/txlifecycle/reconcile_integration_test.go`: observations for `not_found_yet` (never failure), `found_pending`, `included`, `unavailable` (RPC down, fail-closed); dispatch from `unknown` requires the observation first; repeated reconcile converges (no duplicate receipt rows); zero unknown-to-failed/unpaid conversions (FR-03; SC-02).

**Checkpoint**: US1 + US2 both work independently.

---

## Phase 5: User Story 3 — 同字节重播：相同签名字节的再次提交 (Priority: P2)

**Goal**: re-submit the persisted signed bytes byte-identically; no new attempt, no new signing request, no identity/binding/authorization change; current gates still required.

**Independent Test**: replay the persisted signed bytes; assert byte-for-byte equality; assert the attempt + signing_request identity are unchanged; assert replay still obeys current gates (quickstart V3).

### Implementation for User Story 3

- [x] T028 [US3] Extend `internal/txlifecycle/send.go`: replay path — `SendReplay` dispatches the exact persisted `signed_tx_bytes` (never re-derived/re-signed), reuses the same attempt + `signing_request_id`, increments `send_seq`, runs the full current gate sequence, refuses `already_accepted`/`send_mode_mismatch`/gate classes with recorded basis, emits a `replayed` event; no new attempt or 009 request (FR-04; SC-03; send-api §2.2).
- [x] T029 [US3] Execute V3 in `internal/txlifecycle/replay_integration_test.go`: dispatched bytes byte-identical to persisted bytes; zero new attempt rows / `signing_request_id` / 009 requests; `send_seq` increments; replay with revoked grant or changed recovery version → refusal with observed basis, zero dispatch (FR-04; SC-03; V6 race-(b) overlap).

---

## Phase 6: User Story 4 — 费用替换：同意图同 nonce 的新尝试 (Priority: P2)

**Goal**: create a new attempt + new signing identity under the same intent/binding with unchanged payment semantics and PB-conditional grant reuse; keep the old attempt history.

**Independent Test**: create a fee replacement on the same binding; assert new attempt ids vs same intent/binding; assert the original history is intact; assert reuse only when the scope permits and all fee dimensions are in range (quickstart V7).

### Implementation for User Story 4

- [x] T030 [US4] Extend `internal/txlifecycle/attempt.go` + `internal/txlifecycle/store.go`: replacement anchor — `ReplacementOf` must name an existing attempt with equal `intent_id`/`binding_ref`/`chain_id`/`sender`/`nonce`/asset/recipient/amount (else `replacement_mismatch`), fee dimensions must differ from the anchor's (else `replacement_no_fee_change`); create new `attempt_id` + `signing_request_id` + `replacement_of`; anchor history immutable (FR-05/FR-12; R-010-07; send-api §2.1).
- [x] T031 [US4] Extend `internal/txlifecycle/gates.go` + `internal/txlifecycle/send.go`: PB conditional grant reuse — reuse branch records the same `authorization_id` + `authorization_version` and requires `allows_fee_replacement`, `gas_limit × max_fee_per_gas <= fee_max_total` (exact `big.Int`), `max_fee_per_gas <= fee_max_per_gas`, `max_priority_fee_per_gas <= fee_max_priority`, plus version/state/intent equality, else `scope_reuse_forbidden`/`fee_scope_exceeded` with per-dimension observed-vs-cap evidence; fresh branch records a fresh grant id+version that itself passes the full gate; never silently rebind the old attempt (FR-06; PB-C1/C2; Q3; R-010-07).
- [x] T032 [US4] Execute V7 in `internal/txlifecycle/replacement_integration_test.go`: reuse branch → new attempt (new ids, `replacement_of` set) with same intent/binding/sender/nonce and unchanged semantics, 009 signs it, anchor history untouched; forbidden branches zero dispatch; fresh branch anchors a new grant; `replacement_no_fee_change`/`replacement_mismatch`; 100% traceability to one intent/binding, no old bytes reused (FR-05/FR-06; SC-04).

---

## Phase 7: User Story 5 — 回执与确认：预期 Transfer 验证与重组后修订追踪 (Priority: P2)

**Goal**: verify receipt status + expected Transfer (recipient/asset/integer amount) against the canonical chain view; track confirmation; on reorg revise through the append-only revision chain and keep tracking the original intent without rebuilding a payment.

**Independent Test**: build success/failure/amount-mismatch/missing-log receipts and assert the verdicts; simulate a reorg invalidating a receipt and assert the revision keeps history and the original intent link (quickstart V8/V9/V9b).

### Implementation for User Story 5

- [x] T033 [US5] Create `internal/txlifecycle/verify.go`: receipt verification — `eth_getTransactionReceipt` absent → `not_found_yet` observation, stay unknown; effect verdict `effective` iff `status = 1` AND the expected Transfer (emitter = asset, topic1 = sender, topic2 = recipient, 32-byte data = amount) else `ineffective_status`/`ineffective_transfer_missing`/`ineffective_transfer_mismatch` with the observed difference in `transfer_detail`; canonicality against `chain_blocks WHERE canonical`; an unindexed height stays `unverified` and is re-checked, never assumed canonical (FR-08; R-010-08; SC-05).
- [x] T034 [US5] Create `internal/txlifecycle/confirm.go`: confirmation basis — `confirmations = canonical_head − receipt_block + 1`; threshold + `policy_seq` read-only from `MAX(confirmation_policy_history.policy_seq)` (005); guarded upsert by `tx_receipts_tx_block_uniq` (repeat observation converges: update progress, not duplicate); threshold reached → `confirmed` + event carrying `confirm_tip_*`, threshold, policy seq (FR-09; R-010-09; 005 read-only).
- [x] T035 [US5] Extend `internal/txlifecycle/confirm.go`: reorg revision + replaced marking — every pass re-verifies canonicality of non-orphaned receipts; mismatch/missing canonical row → receipt `orphaned` + revision event carrying the observed `recovery_version` + attempt `orphaned`/`unknown`; re-inclusion → new receipt row + `reconfirmed` + restore `effective`/`confirmed`; a sibling attempt on the binding reaching `effective`/`confirmed` marks the others `replaced`; never rebuilds intent/binding/payment; revision never exempts a later send from gates (FR-09; SC-07; 011 M2 inherited).

### Validation for User Story 5 (quickstart V-scenarios)

- [x] T036 [P] [US5] Execute V8 in `internal/txlifecycle/receipt_integration_test.go`: Anvil scenarios — successful transfer effective + enters confirmation; `status = 0`; missing Transfer; wrong recipient/amount/emitter; transfer with extra logs; exact verdicts, never successful on a mismatch; canonicality decided against `chain_blocks` and an unindexed-height receipt stays `unverified` (FR-08; SC-05).
- [x] T037 [P] [US5] Execute V9 in `internal/txlifecycle/confirm_integration_test.go`: confirm to the 005 policy threshold → `confirmed` with basis; Anvil reorg → receipt `orphaned`, revision event carries the observed recovery version, attempt revised without a new intent/binding/payment, re-inclusion → new receipt row + `reconfirmed`; a confirmed attempt sharing a binding → siblings `replaced`; zero rebuilt intents (FR-09; SC-07).
- [x] T038 [P] [US5] Execute V9b crash matrix in `internal/txlifecycle/crash_integration_test.go`: kill the process at each T-boundary of the data-model crash table (before T1; after T1 before 009; after 009 result before T2; after T2 before region; mid-region before dispatch; after dispatch began before COMMIT; after COMMIT before response; after T4) and assert the documented recovery action (FR-03/FR-09; persistence.md §2).

**Checkpoint**: all five stories independently functional.

---

## Phase 8: Residual Adjudications — Freeze & Controlled Manual Release (cross-cutting; no story label)

**Purpose**: implement the two 2026-09-17 limited Q3 exceptions' non-story mechanism. G-010-1's last-moment evaluation lives in US1 (`send.go`); G-010-2 classes (a)/(b) live in US1/US2; this phase implements class (c) detection, the freeze, and the controlled manual release.

- [x] T039 Implement G-010-2 class (c) detection + freeze: in `internal/txlifecycle/reconcile.go` compare the recorded authorization/recovery/execution versions + change evidence (post-hoc version equality proves nothing; unresolvable ordering stays indeterminate, never defaulted safe; normal takeover/version updates are not violation evidence); on confirmed protection loss with evidence of (or inability to exclude) a post-invalidation send, freeze that intent's further sends (chain observation/query/reconcile remain allowed); persist the freeze cause + evidence via an additive carrier in `migrations/000011_tx_lifecycle.sql` (not yet applied; single writer) and a refusal class in `internal/txlifecycle/errors.go` + enforcement in `internal/txlifecycle/send.go`; 011's freeze consumer is `011:T039` (qualified dependency).
- [x] T040 Implement the controlled manual release in `internal/txlifecycle/release.go` (new file for the adjudicated release entry point; no new process, no new HTTP admin surface — R-010-01): release lifts ONLY this residual's independent freeze cause under controlled permission + evidence + audit (`tx_attempt_events`), never overrides revoke/expiry/pause/eligibility gates; full-gate re-verification precedes any resend; reconcile never directly permits resend; audit records who/what/why/basis.
- [x] T041 Execute the residual evidence tests in `internal/txlifecycle/residual_integration_test.go`: (1) G-010-1 — an expiry landing between the last evaluation and dispatch entry is recorded as a natural-expiry residual with basis fields preserved, not described as legal in-flight, definitive results kept; (2) G-010-2(b) — a reliably-preventable pre-dispatch abort records `region_aborted_no_dispatch`, no send result, business effect `unknown` pending reconcile, committed refusals/verdicts unrewritten; (3) G-010-2(c) — session killed after the gate reads + pause committed + network send proceeds → recorded versions vs pause evidence prove a stale basis → the intent's sends freeze, reconcile/observation still allowed; unresolvable ordering stays indeterminate; manual release lifts only the freeze cause and any resend re-verifies all gates; assert the non-claims (no window-is-tiny/rare claim; no detection guarantee for unobservable faults) (Q3; G-010-1; G-010-2(c); quickstart J5 assertions, 010 side).

---

## Phase 9: Joint Integration Wave (owner 010; gated on 011's real implementation in the integration workspace)

**Purpose**: execute the real joint acceptance (J1–J5) required by FR-16 in an integration workspace that contains 011's real implementation + migrations. This wave is NOT executed before that integration; it is executed BEFORE 011 merges to main.

**Qualified-dependency legend**: cross-lane references use concrete `011:Txxx` IDs from `specs/011-withdrawal-executor/tasks.md` (generated in parallel 2026-09-17; substitution applied same day: `TBD-freeze-consumer`→`011:T039`, `TBD-handover`→`011:T041`, `TBD-migrations`→`011:T042`, `TBD-payment-intents`→`011:T043`, `TBD-execution-claims`→`011:T044`, `TBD-intent-claim-supply`/`TBD-worker-fencing`/`TBD-reconcile-loop`/`TBD-projection-updater`→`011:T045`, `TBD-joint-evidence`/`TBD-joint-register`→`011:T046`). No 011-internal task is authored in this file; each task below names exactly one owner (010) and the qualified counterpart ID.

- [ ] T042 Create the 010→011 integration workspace at `.slim/worktrees/joint-010-011` (dedicated worktree; single-writer discipline for shared files) and integrate 011's real implementation + migrations into it (owner 010; counterpart `011:T041`; no push/PR/merge/deploy; do not execute joint acceptance until both sides' implementations + migrations are present).
- [x] T043 Apply `migrations/000011_tx_lifecycle.sql` + 011's `000012_*` (and successors) in the integration-workspace scratch DB in merge order; re-verify the actual migration set and the provisional numbers (PLAN-1); confirm no applied migration was renumbered or rewritten (owner 010; counterpart `011:T042`).
- [x] T044 Add the intent-FK follow-up migration as the next number after `000012` (confirm from `migrations/` at execution time; expected `migrations/000013_tx_lifecycle_intent_fk.sql`) adding the named `tx_attempts_intent_fkey FOREIGN KEY (intent_id) REFERENCES payment_intents (intent_id)` with a 23503 probe — owner 010; batch = 010→011 joint integration batch; gate = MUST close before joint-acceptance completion; additive; never renumber/rewrite applied migrations (G-010-3; PLAN-1; J6; counterpart `011:T043`).
- [x] T045 Swap the claim adapter from the contract-shaped fixture to the real `execution_claims` in `internal/txlifecycle/claim.go` (real column names absorbed by the single mapping table; J2 semantics unchanged: intent-unique, worker identity, monotonic `lease_version`, expiry, revocation marker); keep the fixture path test-only for the 010-independent suite (owner 010; counterpart `011:T044`).
- [x] T046 Execute J1 (intent/authorization wiring) in `internal/txlifecycle/joint_j1_integration_test.go`: real 007 HTTP → 011 intent + claim → 010 consumes the real claim + real grant/scope → sends on Anvil → verified receipt (owner 010; counterpart `011:T045`).
- [x] T047 Execute J2 (expired-worker isolation + takeover) in `internal/txlifecycle/joint_j2_integration_test.go`: an expired/fenced worker's three send kinds are refused by 010; a legitimate taker re-verifies authorization/pause/recovery version/claim and resumes on the same intent, nonce and attempt history — no second intent (owner 010; counterpart `011:T045`).
- [x] T048 Execute J3 (joint unknown reconciliation) in `internal/txlifecycle/joint_j3_integration_test.go`: a real dispatch timeout/response loss in the joint stack ends `unknown`; the joint reconcile flow (011 loop → `Reconcile`) resolves or honestly persists it; no repaying (owner 010; counterpart `011:T045`).
- [x] T049 Execute J4 (reorg revision) in `internal/txlifecycle/joint_j4_integration_test.go`: a joint reorg after confirmation revises 011's projection in revision order (`revision_seq`/`updated_at`; stale never overwrites newer; freshness unconfirmable → marked possibly stale), keeps the original intent, never rebuilds payment (owner 010; counterpart `011:T045`).
- [x] T050 Execute J5 (lock-loss residual, detectable path only) in `internal/txlifecycle/joint_j5_integration_test.go`: fault injection kills the region's DB session after the gate reads without the sender's knowledge, commits a pause, then lets the network send proceed; assert the recorded versions vs pause evidence prove a stale basis → the intent's further sends freeze pending manual review (chain observation/query/reconcile stay available); unresolvable ordering stays indeterminate; manual review lifts only this freeze cause and a resend re-verifies all gates; assert what is NOT claimed (no proof the window is tiny/rare; no detection guarantee for unobservable faults) (owner 010; counterpart `011:T039`).
- [x] T051 Record the joint acceptance evidence and the gating statement: real 007 HTTP + 011 executor + 010 + 009 + PG + Anvil only — the contract-shaped fixture, any mock, or a 010-independent pass NEVER substitutes (FR-16; P5); record that 011's real implementation + migrations were integrated into the integration workspace and that real joint acceptance was executed there; applicable joint gates complete BEFORE 011 merges to main; write the record to `docs/workflow-010-011-parallel.md` (owner 010; counterpart `011:T046`).

**Checkpoint**: J1–J5 executed with real wiring; joint gate statement recorded; 011 merge to main remains gated on this completion.

> **Joint batch status (2026-09-17, `joint-010-011-integration`)** — complete.
> Executed and checked off: T043/T044/T045 (migration set + intent-FK + real
> `execution_claims` adapter), T021 (V2 against the real 009 signer-serve),
> T026 (010↔009 faultproxy), T038 (per-T-boundary process-kill matrix), and
> T046–T050 (J1–J5 on the real 007 HTTP + 011 + 010 + 009 + PG + Anvil stack);
> T051's evidence and gating record are in `docs/workflow-010-011-parallel.md`
> (联合批次执行记录). Full-suite evidence: `go test -tags integration
> ./internal/txlifecycle` ok (153.8s). Joint gates are complete, so 011 may
> proceed toward its merge after review. A-13 (full-chain E2E) and T000-P remain
> OPEN — this record does not claim A-13 closure. The batch surfaced real
> cross-lane findings (010 `TransferCalldata`/type-2 envelope; 009 replacement
> grant-scope preemption) recorded for backflow.
>
> **Final joint acceptance (2026-09-18, clean tree at `ad1cd84`)** — the five J
> scenarios were re-executed through the production worker Driver
> (`worker.Driver.IssueAndAdvance` over `txlifecycle.NewLifecycleLive` +
> `app.NewJointWithdrawalWorker`, real 008 Allocator, real 009 and real Anvil +
> test ERC-20), not through direct 010 Store calls; the direct-Store J tests
> remain as supplementary shared-state coverage. T038/T043–T045/T046–T051 all
> reproduced on this round's clean-tree logs (T042 stays `[ ]`). Per-item
> evidence and the regression/pre-existing-failure record are in
> `docs/workflow-010-011-parallel.md` (最终联合验收记录, 2026-09-18) with raw
> logs under `/tmp/opencode/joint-final/`. `Advance(ActionReplace)` remains a
> deliberate `refused_basis` gap (no 010 fee policy); the J2 replacement leg
> is recorded blocked-with-reason, never faked. A-13 and T000-P remain OPEN.

---

## Phase 10: Polish & Cross-Cutting Concerns

**Purpose**: matrix-closing verification and documentation sync.

- [x] T052 [P] Execute V10 in `internal/txlifecycle/readonly_integration_test.go`: read-only diff over `indexer_pause`/`log_pause`/`deposit_pause`/`reorg_recovery`/`reorg_recovery_events`/`chain_blocks`/`withdrawal_authorizations`/`withdrawal_authorization_scopes`/`nonce_bindings`/`nonce_scope_state`/`nonce_scope_holds`/`nonce_wallet_registry`/`confirmation_policy_history`/`execution_claims` after the full V-matrix; `revision_seq` monotonicity (+1 per mutation); `Status` never triggers a send (FR-12/FR-13; persistence.md §6).
- [x] T053 [P] Execute the V10 claim_absent fail-closed independent acceptance in `internal/txlifecycle/claim_failclosed_integration_test.go`: with the fixture table/row absent, every send entry point refuses `claim_absent` with zero dispatch and committed evidence — executable pre-011 (FR-14; G-010-4; R-010-11).
- [x] T054 [P] Execute V11 in `internal/txlifecycle/boundary_test.go`: `internal/txlifecycle` imports no key/provider package; exactly one dispatch call site; logs/metrics contain no signature, signed bytes or credentials (static/import + log/metric scan) (FR-10; constitution VIII/XII).
- [x] T055 [P] Execute V11 observability in `internal/txlifecycle/metrics_integration_test.go`: send-outcome counters, gate-refusal-by-class counter, unknown gauge, reconcile-class counters, receipt-effect counters and revision counter exist and are recorded on the V-scenarios (constitution XII; R-010-13).
- [x] T056 Doc-sync (Q3 reference consistency; no new rulings): add the second limited Q3 exception (unperceived lock-loss, G-010-2 class (c), adjudicated 2026-09-17) reference to `specs/010-transaction-lifecycle/spec.md` (Clarifications/FR-15) and `specs/010-transaction-lifecycle/contracts/send-api.md` §5 by copying the already-recorded text from `specs/010-transaction-lifecycle/plan.md` (Adjudication addendum 2), `specs/010-transaction-lifecycle/research.md` R-010-14 G-010-2, `specs/010-transaction-lifecycle/contracts/send-gate.md` §4(d)(c), `specs/010-transaction-lifecycle/quickstart.md` J5 and `docs/workflow-010-011-parallel.md` J4 — reference sync only, never invent business text.
- [x] T057 Doc-sync (mandatory wording; no new rulings): align the joint-acceptance wording in `specs/010-transaction-lifecycle/plan.md`, `specs/010-transaction-lifecycle/research.md` and `specs/010-transaction-lifecycle/quickstart.md` to: integrate 011's real implementation + migrations into the integration workspace, then execute real joint acceptance; applicable joint gates complete BEFORE 011 merges to main. Remove/replace any phrasing that implies acceptance after merging to main.
- [x] T058 Update the joint register `docs/workflow-010-011-parallel.md` (migration-number verification record; intent-FK follow-through record; joint-acceptance gating record) and the 010 status row in `docs/project-context.md`; single-writer (010 lane); counterpart `011:T046`.

> **Polish status (W6, 2026-09-17, 010 lane)** — T052–T058 executed and checked off.
> Backflow resolution: the joint-proven 010 commits landed on
> `010-transaction-lifecycle`; the T044 migration got a lane guard (000013 adds
> the FK only when `payment_intents` exists) so the 010 lane stays independently
> migratable, and T043/T044 skip on this lane (joint evidence stays on
> `joint-010-011-integration`). Evidence: `go build ./...` (exit 0); `go vet` +
> `go vet -tags integration` (exit 0); `go test ./...` (exit 0); `go test -tags
> integration ./internal/txlifecycle` (exit 0, 95.1s) including T052/T053/T055 and
> the static T054. T053 surfaced and fixed a real defect (an undefined-table claim
> read poisoned the region transaction; the refusal is now recorded in a fresh
> transaction, preserving `claim_absent`). T046–T051 stay unchecked: J1–J5 were
> not executed (missing 010↔011 production `LifecycleAdvancer/Reader` adapter,
> real 008 binding allocation and an on-chain transfer contract); **joint gates
> incomplete ⇒ 011 MUST NOT merge to main.** A-13 and T000-P remain OPEN.
> `internal/db` PB overlay tests carry a pre-existing out-of-lane failure (000011
> in their exact-count overlay set), reported as-is and not fixed.

---

## Traceability

### Requirements (spec.md FR-01..FR-16) → design → ruling → tasks → acceptance

| Requirement | Design carrier (plan/research/data-model/contracts) | Ruling | Tasks | Acceptance (quickstart) |
|---|---|---|---|---|
| FR-01 (attempt persist before 009; identity↔content) | T1 insert-first + canonical envelope + `content_hash` (plan §Broadcast-gate; R-010-02; data-model Table 1; `store.go`; send-api §2.1) | OC-4/D6; Q1 (010 facts are authority) | T005, T007, T010, T020 | V1 |
| FR-02 (bytes + local hash durable before send) | local reconstruction + hash/sender cross-check + T2 (R-010-03; data-model Table 2; `signing.go`; signer-call §4) | OC-4/D9 | T008, T011, T021 | V2 |
| FR-03 (unknown ≠ success/failure/unpaid; reconcile path; no auto new payment) | fail-safe dispatch classification + claim-free reconcile + unknown recovery interface (R-010-05/06; `reconcile.go`; send-api §2.3/§5) | Q1/C9; OC-6/OC-7 | T017, T023, T024, T025, T026 | V4, V5 |
| FR-04 (same-bytes replay; same attempt/identity; current gates) | replay re-uses persisted bytes + full gate sequence (R-010-04; `send.go`) | Q3 (no replay exception/TTL) | T028, T029 | V3, V6 |
| FR-05 (replacement: new attempt + identity, same intent/binding/semantics) | `replacement_of` + immutable anchor + `tx_hash` UNIQUE (R-010-07; data-model Table 1) | OC-4/D6 | T030, T032 | V7 |
| FR-06 (PB conditional reuse + explicit authorization identity/version) | gate step 8 reuse/fresh branch + attempt stores `authorization_id`+version (R-010-07; send-gate §3) | PB-C1/C2; OC-5 | T031, T032 | V7 |
| FR-07 (006 pause/recovery inheritance; multi-pause; read-only) | gate steps 2–5 + `SHARE` locks; zero writes to 006 tables (R-010-04; send-gate §2/§3) | OC-6; Q3 (G-010-1 natural expiry) | T016, T022 | V6 |
| FR-08 (receipt + expected Transfer verification) | receipt verify over canonical `chain_blocks` + pinned Transfer semantics (R-010-08; `verify.go`) | OC-7/D1 | T033, T036 | V8 |
| FR-09 (confirmation + reorg revision chain; no compensating rebuild) | confirmation basis + append-only revision events + `replaced` marking (R-010-09; `confirm.go`) | 011 M2 inherited; Q1 revision bookkeeping | T034, T035, T037, T038 | V8, V9, J4 |
| FR-10 (no keys; signatures only via 009) | import/structural boundary; no key/provider code; single 009 boundary (R-010-01; signer-call §5) | OC-5/D9; 009 FR-05 | T011, T054, T055 | V11 |
| FR-11 (re-verify every gate per send; fail closed) | send region re-reads all gates inside the locked region; no cached basis (R-010-04; send-gate §3) | Q3 | T016, T017, T022 | V6 |
| FR-12 (identity chain traceability) | PK/FK/UNIQUE carriers + append-only events; `request_id → intent_id → binding → attempt → signing_request → authorization+version` (data-model §Idempotency; send-api §1) | OC-1–OC-4; J1/C3 | T005, T007, T030, T052 | V1, V7 |
| FR-13 (state authority + projection versioning) | projection read with `revision_seq`/`updated_at`; anti-permission contract (R-010-13; `projection.go`) | Q1/OPEN-1a; C5 | T013, T049, T052 | V10, J4 |
| FR-14 (expired-worker fence; takeover; no second intent) | claim row `FOR SHARE` + version/expiry/revocation equality; claim-free reconcile (R-010-04; `claim.go`; send-api §4) | Q2/OPEN-1b; J2/C6 | T012, T016, T022, T045, T047 | V6, J2 |
| FR-15 (check-to-send ordering; residuals; no grace/TTL) | lock-held region + last-moment `clock_timestamp()` + outcome taxonomy; G-010-1/G-010-2 handling (R-010-04/05/14; send-gate §4) | Q3/OPEN-2; G-010-1 (2026-09-17); G-010-2 second limited exception (2026-09-17) | T016, T017, T018, T039, T040, T041, T050 | V6, V9, J5 |
| FR-16 (independent vs joint acceptance) | V-matrix vs J-matrix separation; labeled fixture; no mock-as-joint (R-010-11; quickstart) | FR-16; C10/P5 | T019, T042–T051 | V/J boundary review; J1–J5 |

### Rulings register (adjudicated inputs → tasks → acceptance)

| Ruling | Source | Tasks | Acceptance |
|---|---|---|---|
| Q1 (OPEN-1a): 011 may keep a display projection; 010 durable facts are authoritative; projection is never a permission; keep source version + updated_at; stale-if-unfresh; no display-latency SLA | spec Clarifications 2026-09-17; FR-13; J5/C5 | T013, T025, T049, T052 | V10, J4 |
| Q2 (OPEN-1b): rejecting expired-worker DB writes is not enough; 010 fences all three send kinds at initiation; no second intent; legitimate takeover re-verifies all gates | spec Clarifications 2026-09-17; FR-14; C6 | T012, T016, T022, T045, T047 | V6, J2 |
| Q3 (OPEN-2): every send requires all four gates current; no replay exception/TTL; decision-first is not in-flight; prevent when reliably preventable, else keep fact/unknown and reconcile; known results never rewritten | spec Clarifications 2026-09-17; FR-15; C7 | T016, T017, T018, T041 | V6, V9 |
| G-010-1 limited exception (natural expiry): final evaluation uses `clock_timestamp()`; residual recorded + reconciled, never called in-flight; no grace/TTL; prior checks never reused; definitive results kept | spec Clarifications 2026-09-17; send-gate §4(c); R-010-14 | T018, T041 | V6 (residual recording) |
| G-010-2 second limited exception (class (c), unperceived lock-loss): same-session probe mitigation only; single dispatch; no transparent retries; freeze + manual review on proof; reconcile never permits resend; full re-verification before any resend; not actual verification | plan Adjudication addendum 2; send-gate §4(d)(c); R-010-14; quickstart J5; register J4 (2026-09-17) | T039, T040, T041, T050 | J5 (+ T041 010-side evidence) |

### Success criteria → acceptance

| SC | Acceptance evidence | Tasks |
|---|---|---|
| SC-01 | first-broadcast completeness + zero-send refusals | T020, T021, T022 |
| SC-02 | injected timeout/response loss → 100% unknown, zero facts lost | T026, T027 |
| SC-03 | replay byte identity + zero new attempts/requests | T021, T029 |
| SC-04 | replacement traceability + semantic equality | T032 |
| SC-05 | receipt/Transfer mismatch never marked successful | T036 |
| SC-06 | zero sends during recovery pause / stale view | T022 |
| SC-07 | reorg revision tracks original intent; zero rebuilt intents | T037, T049 |

### Acceptance index (quickstart scenarios → tasks)

| Scenario | Focus | Tasks |
|---|---|---|
| V1 | FR-01/FR-12 | T020 |
| V2 | FR-02 | T021 |
| V3 | FR-04 | T029 |
| V4 | FR-03 | T026 |
| V5 | FR-03 | T027 |
| V6 | FR-07/FR-11/FR-14/FR-15 | T022 |
| V7 | FR-05/FR-06 | T032 |
| V8 | FR-08 | T036 |
| V9 / V9b | FR-09 | T037, T038 |
| V10 | FR-12/FR-13/FR-14 | T014, T052, T053 |
| V11 | FR-10/observability | T054, T055 |
| J1 | intent/authorization wiring | T046 |
| J2 | expired-worker isolation | T047 |
| J3 | joint unknown reconciliation | T048 |
| J4 | reorg revision / projection order | T049 |
| J5 | lock-loss residual, detectable path | T050 |

---

## Dependencies & Execution Order

### Phase dependencies

- **Setup (Phase 1)**: no dependencies.
- **Foundational (Phase 2)**: depends on Setup; BLOCKS all user stories.
- **US1 (Phase 3)**: after Foundational; no other story dependency.
- **US2 (Phase 4)**: after US1 (needs the send region + durable send evidence); independently testable with injected faults.
- **US3 (Phase 5)**: after US1 (persisted bytes + send region). Shares `send.go` with US4/US1 → single-writer.
- **US4 (Phase 6)**: after US1; shares `send.go`/`gates.go`/`attempt.go`/`store.go` with US1/US3 → single-writer.
- **US5 (Phase 7)**: after US1 (a sent attempt) and integrates with US2's reconcile loop (single writer for `reconcile.go`).
- **Phase 8 (residual)**: after US1 + US2 (extends `send.go`/`reconcile.go`; additive to `000011`).
- **Phase 9 (joint wave)**: after all local phases; gated on 011's real implementation + migrations being integrated into the integration workspace. Joint acceptance is executed in that workspace; applicable joint gates complete BEFORE 011 merges to main. Do not start this wave before that integration exists.
- **Phase 10 (Polish)**: after US1–US5 (V10 runs over the full V-matrix).

### Single-writer map (shared files; serialize, never parallel-edit)

| File | Ordered task chain |
|---|---|
| `migrations/000011_tx_lifecycle.sql` | T007 → T008 → T009 → T039 |
| `internal/txlifecycle/send.go` | T017 → T018 → T028 → T031 → T039 |
| `internal/txlifecycle/gates.go` | T016 → T031 |
| `internal/txlifecycle/reconcile.go` | T023 → T024 → T025 → T039 |
| `internal/txlifecycle/confirm.go` | T034 → T035 |
| `internal/txlifecycle/attempt.go` | T005 → T030 |
| `internal/txlifecycle/store.go` | T010 → T015 → T030 |
| `internal/config/config.go`, `internal/eth/client.go`, `internal/metrics/*` | T001/T002/T003 complete before story work consumes them |
| each test file | exactly one task per file |

### 011-side qualified dependencies (never duplicated here)

T039/T050 → `011:T039`; T042 → `011:T041`; T043 → `011:T042`; T044 → `011:T043`; T045 → `011:T044`; T046/T047/T048/T049 → `011:T045`; T051/T058 → `011:T046`. Substituted 2026-09-17 against the generated 011 tasks.md; no `TBD` placeholders remain.

### Within each story

- identity/content carriers before service code; gates before dispatch; core before integration; validation last.
- No task is checked until its evidence exists; a failed validation leaves the task unchecked and the story incomplete.

---

## Parallel Execution Examples

```bash
# Wave 0 — Setup (all different files):
Task: "T001 config knobs in internal/config/config.go"
Task: "T002 eth send/read extension in internal/eth/client.go"
Task: "T003 metrics extension in internal/metrics/"
Task: "T004 refusal taxonomy in internal/txlifecycle/errors.go"
Task: "T005 attempt/envelope/content-hash in internal/txlifecycle/attempt.go"
Task: "T006 calldata/Transfer extraction in internal/txlifecycle/calldata.go"

# Foundational — after T007–T009 (same migration file, sequential):
Task: "T010 T1 store in internal/txlifecycle/store.go"
Task: "T012 claim adapter in internal/txlifecycle/claim.go"
Task: "T013 Status projection in internal/txlifecycle/projection.go"

# US1 validation wave (different test files):
Task: "T020 execute V1 in internal/txlifecycle/attempt_integration_test.go"
Task: "T021 execute V2 in internal/txlifecycle/signing_integration_test.go"
Task: "T022 execute V6 in internal/txlifecycle/send_integration_test.go"

# US5 validation wave (different test files):
Task: "T036 execute V8 in internal/txlifecycle/receipt_integration_test.go"
Task: "T037 execute V9 in internal/txlifecycle/confirm_integration_test.go"
Task: "T038 execute V9b crash matrix in internal/txlifecycle/crash_integration_test.go"

# Polish wave (different files):
Task: "T052 read-only/revision in internal/txlifecycle/readonly_integration_test.go"
Task: "T053 claim_absent fail-closed in internal/txlifecycle/claim_failclosed_integration_test.go"
Task: "T054 boundary/secrecy in internal/txlifecycle/boundary_test.go"
Task: "T055 observability in internal/txlifecycle/metrics_integration_test.go"
```

Do NOT parallel-edit any file listed in the single-writer map; do NOT parallel-run tasks that share one test file; joint-wave tasks share one integration environment and run sequentially.

---

## Implementation Strategy

### MVP first

1. Phase 1 Setup → Phase 2 Foundational (CRITICAL — blocks all stories).
2. Phase 3 US1 → stop and validate (V1/V2/V6): the smallest demoable increment (persist → sign → land bytes → gated send).
3. Phase 4 US2 immediately after (second P1): unknown + reconcile is required for safe operations.
4. Deploy/demo only after both P1 stories pass their validations.

### Incremental delivery

Setup + Foundational → US1 (MVP) → US2 (unknown safety) → US3 (replay) → US4 (replacement) → US5 (receipt/confirmation/reorg) → Phase 8 residuals → Phase 9 joint wave → Phase 10 polish. Each story adds value without breaking previous stories; validation gates each step.

### Joint wave strategy

Integrate 011's real implementation + migrations into the integration workspace (T042–T045), then execute real joint acceptance J1–J5 (T046–T050); applicable joint gates complete BEFORE 011 merges to main. Mocks, fixtures and 010-independent passes never count as joint evidence (FR-16). The intent-FK follow-up migration (T044) closes G-010-3 inside this batch, before joint-acceptance completion.

### Gates and stop lines (this round)

- The formal analyze gate is retained: run `/speckit.analyze` over spec/plan/tasks after this generation and before any implementation; findings MUST be resolved first.
- This tasks round authorizes NO implementation batch: no code, migration, test, service or container is written or executed by this step; no push, PR, merge or deploy (P5 stop line).
- T000-P (production provider) and A-13 (full-chain E2E) remain OPEN constraints, not deliverables here.

---

## Notes

- `[P]` = different files, no dependency on an incomplete task; all other task pairs sharing a file are sequential (single-writer map above).
- Every task carries its exact file path; every story phase has Goal + Independent Test + validation tasks mapped to quickstart V-scenarios.
- COMMIT failure never mechanically rewrites everything to `unknown` (see Inputs & constraints): `region_aborted_no_dispatch` ≠ a send result; a persisted `unknown` business effect stays pending reconcile; known RPC/on-chain results are preserved.
- All tasks are unchecked; this file is a planning artifact and claims no execution, no analyze run, and no testing.
- 006/007/008/009/PB artifacts are consumed read-only; 010 writes none of their tables; the claim adapter fails closed pre-011 and the fixture is test-only.

