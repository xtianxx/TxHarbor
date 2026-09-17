# Research: 010 Transaction Lifecycle Management — Phase 0 decisions

**Branch**: `010-transaction-lifecycle` | **Date**: 2026-09-17 | **Spec**: [spec.md](spec.md) (clarify `bd59754`, Q1–Q3 resolved; zero `[NEEDS CLARIFICATION]`)

**Inputs consumed read-only**: [spec.md](spec.md) incl. Clarifications Q1/Q2/Q3; `docs/workflow-010-011-parallel.md` §联合设计合同 v1 (J1–J6) and C1–C12; 011 spec at `.slim/worktrees/prep-011-worker/specs/011-withdrawal-executor/spec.md` (M1n/M2/M3; consumed only via the frozen contract); `.specify/memory/constitution.md` v1.1.0; prior art `specs/009-signer-service/{plan,research,data-model,contracts/*}.md`.

**Purpose**: resolve the technical unknowns of the 010 Technical Context (Phase 0), record each design decision with its rejected alternatives, and state — not hide — the irreducible residuals. All business semantics are inherited from approved rulings (OC-1–OC-7, PB-C1/C2, 010 Q1–Q3, 006 FR-26, 011 M2); nothing here re-opens them. This step writes documentation only: no code, no migration, no tasks, no tests executed.

**Method**: reuse an existing repo primitive (lock protocol, guard, classifier, audit shape) before inventing one; add only the carrier that makes an invariant structural; no new dependency, no new infrastructure, no new service boundary (constitution XIII). Every claim below is anchored to a file:line in the current worktree or to an approved ruling.

---

## R-010-01 — Boundary and placement: in-process domain package, no new process

**Decision**: 010 is a new Go domain package `internal/txlifecycle` inside the existing binary, wired by the existing `serve` process (and callable by 011's executor when it lands). It owns construction, pre-persistence, the 009 call, signed-bytes landing, broadcast/reconcile, replacement, receipt/Transfer verification, confirmation tracking, and reorg revision. It owns no key material, no HTTP admin surface, no new listener, no new process. Its only network surfaces are the 009 signer HTTP API and the existing chain RPC client.

**Rationale**: FR-10 forbids holding/reading key material, not process colocation. The 009 process split exists because 009 owns signing secrets (`specs/009-signer-service/plan.md` Summary; `internal/app/signerserve.go:1-10`); 010 owns none, so a second process would add a wire protocol, a second config/metrics/health wiring and a second send-capable entry point for zero correctness gain (constitution XIII). In-process matches 011's FR-08 "call 010" shape.

**Alternatives considered**: separate `tx-serve` process + HTTP API (rejected: no secret boundary to protect, extra failure domain, extra audit surface for the FR-15 fence); folding into 008 or 009 (rejected: violates OC-3/FR-10 ownership split).

**Evidence**: `internal/eth/client.go:73-95`; `internal/config/config.go:29-58`; `specs/009-signer-service/plan.md:52-57`.

---

## R-010-02 — Attempt identity and content: insert-first, canonical envelope + economic content hash

**Decision**: The caller (011) pre-allocates `attempt_id` and `signing_request_id` (OC-4/D6). 010 persists identity + complete immutable content + `intent_id`/`binding_ref`/`authorization_id`+version references in transaction **T1 before the first 009 call** (FR-01). Same-identity resubmission is classified by 23505 on named carriers: equal envelope → replay of the existing attempt; different envelope → `attempt_conflict` refusal, zero writes, original untouched. 010 stores two deterministic artifacts:

- `canonical_envelope` — the exact 009 request body (identity + content + authorization + recovery version), used for byte-exact same-identity equality and for re-sending the identical 009 request after a crash;
- `content_hash` — `keccak256(domain-tagged economic projection)` over chain/sender/nonce/to/value/data/gas/fees **without** attempt/signing ids, used as the replay comparison key and audit digest.

**Rationale**: 009 already implements insert-first / 23505 classify / replay-or-conflict with a canonical envelope (`internal/signer/submit.go:39-57,180-200`; `internal/signer/content.go:216-327`). 010 mirrors the proven discipline instead of inventing a second identity protocol. The envelope includes ids because 009's equality is over the full request; the id-free economic hash lets replays, replacement linkage and duplicate detection be compared without weakening identity equality.

**Alternatives considered**: 010-allocated attempt ids (rejected: OC-4 pre-allocation is approved and 011's identity chain depends on it); column-by-column replay comparison (rejected: encoding ambiguity — the reason 009's envelope exists); `UNIQUE(content_hash)` as a duplicate guard (rejected: a recovery-version rebuild of the same economics with a new identity is a legitimate 009 outcome — `specs/009-signer-service/contracts/gates.md:38` — and duplicate bytes are already impossible to sign twice via `signature_results_tx_hash_uniq`, `migrations/000009_signer_service.sql:189`).

**Evidence**: `internal/signer/content.go:44-69,216-327,397-423`; `internal/signer/submit.go:39-57`; `migrations/000009_signer_service.sql:96-192`; spec FR-01.

---

## R-010-03 — Signed bytes are reconstructed locally; 009 never returns raw bytes

**Decision**: 009 returns only `signature` (65-byte `[R||S||V]` hex) and `tx_hash` (`specs/009-signer-service/contracts/api.md:86-88`; `migrations/000009_signer_service.sql:181-192`). 010 therefore reconstructs locally: rebuild the `types.Transaction` from its own persisted content, attach the signature with `types.LatestSignerForChainID(chain_id)`, produce `signed_tx_bytes = tx.MarshalBinary()`, compute `local_hash = keccak256(signed_tx_bytes)`, and require `local_hash == 009.tx_hash`. A mismatch fails closed (`signature_mismatch`): zero send, evidence recorded. Bytes + hash land durably in **T2 before any external send** (FR-02); `tx_hash` is UNIQUE across all attempts.

**Rationale**: FR-02 requires the local hash and durable signed bytes; the 009 contract explicitly forbids returning raw bytes, so local reconstruction is the only reading that satisfies both without re-opening 009. The reconstruction already exists as prior art (`Request.VerifySignature`, `internal/signer/content.go:397-423`) including the chain-signer choice; 010 reuses it. Persisting `BYTEA` makes replay byte-exact without depending on a live signer at replay time.

**Alternatives considered**: have 009 return raw bytes (rejected: approved 009 contract, out of scope to reopen); persist only the signature and re-derive at send (rejected: FR-02 demands the bytes themselves be the durable pre-send fact); trust 009's hash without local recomputation (rejected: it is the only cross-check that 010's envelope is what 009 signed).

**Evidence**: `specs/009-signer-service/contracts/api.md:86-88`; `internal/signer/content.go:356-423`; spec FR-02.

---

## R-010-04 — The send region: gates, dispatch and evidence in one locked database transaction

**Decision (core of Q3)**: Every send — first broadcast, replay, replacement first-send — runs inside one PostgreSQL transaction (the **send region**) with this fixed sequence:

1. guards: `SET LOCAL statement_timeout='5s'` then `SET LOCAL lock_timeout='5s'` (repo literals);
2. `FOR SHARE` on the 011-owned `execution_claims` row for the attempt's `intent_id` (the execution-qualification fence; J2/C6);
3. `FOR UPDATE` on the chain coordination row `indexer_lease` (the repo's single write-protocol lock, ordered against 006 recovery establishment and all 008 writers);
4. `LOCK TABLE indexer_pause, log_pause, deposit_pause, reorg_recovery, reorg_recovery_events IN SHARE MODE` (the 009 gate-table form);
5. one-statement 006 gate read (shape of `captureRecoveryVersion` + pause flags): require no pause row, no active recovery row, `current_version == attempt.recovery_version`;
6. `FOR SHARE` on the 008 scope row `nonce_scope_state(chain_id, sender)` (bilateral 008 serialization; taken before the gate-table lock is released);
7. 008 binding read by `binding_ref`: intent/chain/sender/nonce equality, state ∈ (`allocated`,`in_flight`), registry `active`, zero active `nonce_scope_holds`;
8. 007 grant `FOR SHARE` + PB scope `FOR SHARE`: `state='active'`, not expired on the DB clock, field equality (chain/asset/recipient/amount/sender), `intent_id` equality; replacement reuse additionally requires `allows_fee_replacement` + all three fee dimensions in scope + `authorization_version` equality; a fresh-grant replacement must itself pass step 8;
9. `FOR UPDATE` on the `tx_attempts` row (serializes concurrent sends of one attempt), sendability re-check, `send_seq = MAX+1`;
10. dispatch: `eth_sendRawTransaction` with the persisted bytes, bounded by `TXHARBOR_TX_SEND_TIMEOUT`; the returned hash must equal the persisted `tx_hash` (else `unknown` + `hash_mismatch`);
11. record + commit: one `tx_send_attempts` row (gate snapshot + outcome class), the attempt state transition and the event row, all in the same COMMIT.

Gate refusals (steps 2–9) commit zero-dispatch evidence and return a refusal class; the dispatch is never entered. After dispatch begins, the locks remain held until COMMIT, so every **write-based** invalidation path (grant revoke `FOR UPDATE`, pause/recovery `INSERT`/`DELETE`, claim re-lease `UPDATE`, 008 pause/registry/release via the coordination row) is ordered: it either committed before steps 2–8 and is observed, or it blocks until the region commits and is therefore effective only after the send entered the uncancellable external stage.

**Rationale**: Q3 demands an explicit verifiable ordering, rejects "one pre-send query", and counts as legal in-flight only operations that actually entered the uncancellable external send stage before invalidation takes effect. Holding conflicting locks across the dispatch converts "checked then sent" into "the invalidation writer is serialized before or after the whole region". All primitives are proven in-repo: 5-step coordination framing (`internal/nonce/coord.go:6-24,145-161`), scope-row share (`internal/nonce/coord.go:90-92`; `internal/nonce/readapi.go:350-412`), gate-table `SHARE` (`specs/009-signer-service/contracts/gates.md:10-16,53-66`). The frozen contract prescribes this shape (J3/J4, `docs/workflow-010-011-parallel.md:115-123`).

**Alternatives considered**: check-then-commit-then-dispatch outside the transaction (rejected: re-creates the OPEN-2 gap; revoke-vs-send undecidable); advisory lock or 010-owned lock table (rejected: no upstream writer takes it); claim-row `FOR UPDATE` instead of `FOR SHARE` (rejected: blocks legitimate 011 renewal and does not cover 006/008 writers); a second pre-send query (rejected: a check is not an ordering proof — Q3 explicitly forbids it).

**Evidence**: `internal/nonce/coord.go:45-48,56-96,145-190`; `internal/nonce/readapi.go:9-26,86,276-331,350-412`; `specs/009-signer-service/contracts/gates.md:10-16,24-66`; `internal/indexer/reorgcommit.go:104-129`; `docs/workflow-010-011-parallel.md:111-123`; spec FR-11/FR-14/FR-15.

## R-010-05 — Dispatch classification: known outcomes stay known, everything indistinct is `unknown`

**Decision**: Dispatch results are classified into three durable outcomes plus zero-dispatch refusals:

| Outcome | Trigger | Attempt state after | Re-send rule |
|---|---|---|---|
| `accepted` | RPC returned the same hash, or `already known` for identical bytes | `sent` | replay allowed directly (still under all gates) |
| `rejected` | an allowlisted deterministic RPC rejection that is not an acceptance signal | `unknown` (business effect undetermined) with the rejection preserved | probe-first, then replay/replacement under gates |
| `unknown` | timeout, transport failure, rate-limit exhaustion, invalid/unparseable response, returned-hash mismatch, any unrecognized RPC error, or any failure of the region's own writes after dispatch began | `unknown` | probe-first, then replay/replacement under gates |

Classification is fail-safe: only a returned, allowlisted, non-acceptance verdict yields `rejected`; every unrecognized shape yields `unknown`. Known send results are never rewritten (a `rejected` row keeps its class; reconciliation adds new evidence rows rather than editing old ones), and `unknown` is never rewritten to "failed" or "not broadcast". The attempt-level state vocabulary separates the *send result* (evidence) from the *business effect* (chain-derived).

**Rationale**: FR-03 makes unknown first-class; Q3 says "已知发送结果 MUST NOT 一律改写为未知" while unknown must never become "not sent". Splitting send-level evidence from attempt-level business state satisfies both: a `rejected` dispatch is a known send fact, but the payment effect stays undetermined until chain observation (e.g. `nonce_too_low` may mean an earlier identical send was mined). Fail-safe classification prevents provider message drift from producing false failure verdicts; the constitution's RPC rules require transport failure, timeout, rate limiting, invalid response and rejection to stay distinguishable.

**Alternatives considered**: any RPC error = `rejected` (rejected: can mark an on-chain payment failed); never classify, always `unknown` (rejected: Q3 requires known results to stay known; destroys operator evidence); message-substring classification as the primary mechanism (rejected: brittle; allowed only inside an explicit allowlist whose fallthrough is `unknown`).

**Evidence**: spec FR-03/FR-11/FR-15; `.specify/memory/constitution.md` §RPC and External Dependencies; `internal/eth/client.go:28-44,163-205`.

---

## R-010-06 — Unknown recovery protocol: reconcile first, decide after

**Decision**: A durable `unknown` attempt is resolved only by reconciliation, never by inference. The **unknown recovery interface** (J5): given `(attempt_id, tx_hash)`, 010 returns the unknown state plus all persisted facts (bytes present, every dispatch row, every reconcile observation, last gate basis) plus the recovery conditions. The reconcile operation:

1. probes `eth_getTransactionByHash(tx_hash)`, classified `found_pending` / `included` / `not_found_yet` / `unavailable`, and records one `tx_reconciliations` row;
2. on `included`: fetches the receipt, verifies canonicality against the indexer's canonical `chain_blocks` view, verifies status + expected Transfer, records `tx_receipts` + event rows;
3. on `not_found_yet`: **stays unknown** — absence of RPC evidence is never a failure verdict; the honest class name permits a later replay/replacement only after this observation is durably recorded and only under full current gates;
4. on `unavailable`: fail-closed observation; no send decision may be taken from it.

Every send/replay/replacement entry point requires that an attempt in `unknown` — or in `signed` without a durable dispatch record (a possibly-crashed region) — has a reconcile observation recorded **in the current operation** before dispatching. Reconciliation requires no claim and never sends: chain-fact observation, revision and reconciliation bookkeeping are not asset execution (011 M2, inherited); it writes no 006/007/008/011 rows and is idempotent under repetition (guarded/unique writes). The reconciler loop is 010-owned and only observes; the fence applies to sends, not to truthful bookkeeping.

**Rationale**: FR-03 demands a reconcile path and forbids automatic new intent/binding/payment; US2-3 requires a completed reconciliation judgement before retry/replacement; Q1 makes 010's durable facts the authority for that judgement. Making reconcile claim-free is required exactly when it matters most: a fenced-out worker must still have unknown reconciled, and a legitimate taker must reconcile before it may send. If reconcile required a claim, unknown tracking would freeze during the takeover window and chain-fact bookkeeping would become an execution privilege (contradicting M2).

**Alternatives considered**: replay immediately on unknown (rejected: violates "先对账后决策"); `not_found` = failed, rebuild (rejected: FR-03/US2/US5 forbid it); claim-gated reconcile (rejected: fences are for sends); separate reconcile service (rejected: unnecessary boundary, 010 owns the facts).

**Evidence**: spec FR-03/FR-09/FR-14; `docs/workflow-010-011-parallel.md:124-128`; 011 spec `.slim/worktrees/prep-011-worker/specs/011-withdrawal-executor/spec.md:26-27` (M2).

---

## R-010-07 — Replacement: new attempt + new signing identity, conditional grant reuse, same intent/binding

**Decision**: A fee replacement is a **new** `tx_attempts` row whose `replacement_of` points at the replaced attempt, with the same `intent_id`, `binding_ref`, `chain_id`, `sender`, `nonce` and unchanged payment semantics (`to`/asset/recipient/amount), new fee dimensions and a new `signing_request_id`. It runs the full T1 → 009 → T2 → send-region path and never reuses old signed bytes (structurally: own bytes/hash, `tx_hash` UNIQUE). Authorization follows PB-C1/C2 conditionally:

- **reuse branch**: the attempt records the same `authorization_id` + `authorization_version`; the send gate requires `allows_fee_replacement` and `gas_limit × max_fee_per_gas ≤ fee_max_total`, `max_fee_per_gas ≤ fee_max_per_gas`, `max_priority_fee_per_gas ≤ fee_max_priority`, plus version/state/intent equality;
- **fresh branch**: the attempt records a fresh `authorization_id` + version that itself passes the full gate; the old attempt's authorization reference is immutable (no silent rebinding).

009 threads `replacement_of` internally by anchoring on `(caller_id, intent_id, binding_ref, authorization_id)` (`internal/signer/submit.go:39-43,180-190`) with the partial anchor index (`migrations/000009_signer_service.sql:174-175`), so 010 adds no wire field: it passes the same or a different grant and 009 derives the anchor relationship.

When any attempt on a binding reaches `effective`/`confirmed`, sibling attempts on the same binding become `replaced` (event revision), because a confirmed transaction consumes the nonce permanently. Competing replacements before that moment are permitted — no new business rule is invented to forbid them; chain nonce semantics decide and reconciliation records it.

**Rationale**: FR-05/FR-06 + PB-C1/C2 define exactly this. The 009 anchor mechanism makes the fresh branch the automatic consequence of naming a different grant, keeping 010's validation honest without a parallel protocol. `gas_limit × max_fee_per_gas` is the conservative reading of PB-C2's total-fee cap, computed with `big.Int` (constitution I).

**Alternatives considered**: overwrite fees on the same attempt row (rejected: violates content immutability FR-01 and destroys evidence); add a `replacement_of` wire field to 009 (rejected: 009 derives it; reopening 009 is out of scope); forbid concurrent replacements (rejected: unapproved business constraint; the identity chain already prevents same-nonce double intent).

**Evidence**: `internal/signer/submit.go:34-43,180-190`; `migrations/000009_signer_service.sql:169-175`; `migrations/000010_withdrawal_authorization_scopes.sql:25-45`; spec FR-05/FR-06/US4.

---

## R-010-08 — Receipt and expected-Transfer verification reuses the repo's Transfer semantics

**Decision**: Receipt verification requires `status = 1` **and** a `Transfer(address,address,uint256)` log with emitter = expected asset (`to_addr`), `topic1 = sender`, `topic2 = recipient`, `data = amount` (exact integer). Missing log, mismatched emitter/from/to/amount, or `status = 0` yields an `ineffective_*` class with the observed difference recorded; nothing is marked paid on partial evidence. Topic matching reuses `eth.TransferSig` and the structural checks the deposit parser already pins (`internal/eth/client.go:46-48`; `internal/indexer/depositparse.go:137-163`): exactly three topics, zero-padded 20-byte slots, 32-byte data. Canonicality is decided against the indexer's canonical view (`chain_blocks WHERE canonical`, `migrations/000002_chain_indexer.sql:16-26`); a receipt at a height the indexer has not yet indexed stays `unverified` and is re-checked — never assumed canonical.

**Rationale**: FR-08 requires recipient/asset/integer-amount equality; reusing the deposit-side semantics guarantees 004 and 010 cannot disagree about a valid ERC-20 Transfer. `chain_blocks` is the reorg-aware durable view 006 owns (constitution IV); an RPC-only header can race a reorg with no durable record. Amounts are compared as big-endian integers; no floats anywhere (constitution I).

**Alternatives considered**: ad-hoc log parsing in 010 (rejected: second definition of "valid Transfer"); accept any successful receipt (rejected: FR-08 requires the match); RPC-only canonicality (rejected: indexer is the canonicality authority).

**Evidence**: `internal/eth/client.go:46-48`; `internal/indexer/depositparse.go:137-163`; `migrations/000002_chain_indexer.sql:16-26`; spec FR-08/US5.

## R-010-09 — Confirmation tracking reuses the 005 chain policy read-only; reorg revision is fact bookkeeping (M2)

**Decision**: Confirmation progress = `canonical_head_number − receipt_block_number + 1`, computed against the indexer's canonical view. Threshold and version are read read-only from `MAX(confirmation_policy_history.policy_seq)` for the chain (`migrations/000005_confirmation_tracking.sql:26-49`) — one chain-level configurable depth, no new policy table, no new knob, no invented threshold. Each receipt row records `confirmations`, `confirm_threshold`, `confirm_policy_seq`, `confirm_tip_number`, `confirm_tip_hash`; reaching the threshold moves the attempt to `confirmed` with an event. Reorg handling re-verifies each receipt's `(block_number, block_hash)` against the canonical view on every reconciler pass; mismatch/missing canonical row marks the receipt `orphaned`, appends a revision event carrying the observed `recovery_version`, and moves the attempt to `orphaned` (or to `unknown` when the tx may still be re-included) — never creating a new intent, binding, or payment. A re-included tx produces a new receipt row for the new block hash; if it verifies, a `reconfirmed` event is appended and the attempt returns to `effective`/`confirmed`.

**Rationale**: Constitution IV requires an explicit configurable confirmation depth; 005 already provides exactly one per chain, so reusing it prevents two competing policies. FR-09 requires revision per current recovery version and forbids compensating rebuilds. 011's M2 ruling (inherited) separates chain-fact revision (no new authorization needed) from any later send (current gates apply); this design implements that separation precisely: revision writes only receipts/events and touches nothing that authorizes a send. The bookkeeping is limited to chain facts (receipts, canonicality, confirmations), attempt/request state and projection revision; **010 maintains no balance ledger** and revision grants no exemption from any normal authorization check. Unknown reconciliation continues during recovery (D1: 暂停前已发出的外部请求保留结果未知并继续查询取证); sends do not.

**Alternatives considered**: a 010-owned confirmation policy table (rejected: duplicate configurable depth for the same chain, and 005's is already explicit/configurable); reusing 006 reorg thresholds as confirmation depth (rejected: different concept; 006's max_depth is a detection bound, not a finality policy); treating a confirmed receipt as immutable (rejected: FR-09 requires revision on reorg).

**Evidence**: `migrations/000005_confirmation_tracking.sql:26-49,104-108`; `migrations/000002_chain_indexer.sql:16-36`; `migrations/000006_reorg_recovery.sql:144-181`; spec FR-09/US5; 011 M2.

---

## R-010-10 — Migration `000011`: provisional number, additive-only, no FK to not-yet-existing tables

**Decision**: 010 adds one goose migration, provisionally `migrations/000011_tx_lifecycle.sql` (pure DDL, no stored functions, named constraints, `goose Up`/`Down` hygiene per repo convention). Numbering is re-verified at merge against the actual migration set; `000010` is confirmed occupied by PB (`migrations/000010_withdrawal_authorization_scopes.sql`); applied migrations are never renumbered or rewritten (PLAN-1). The migration is purely additive; it references existing upstream tables with FKs where they exist now — `nonce_bindings(binding_id)` (008, `migrations/000008_nonce_manager.sql:82-100`) and `withdrawal_authorizations(authorization_id)` (007, `migrations/000007_withdrawal_creation.sql:118-129`) — and does **not** declare FKs to 011-owned tables (`payment_intents`, `execution_claims`): they do not exist when `000011` applies on the 010 branch (merge order 010 → 011, J6), so an FK there would make the migration un-appliable standalone. `intent_id`/claim references are shape-checked columns; the intent FK is a recorded joint-integration item (G-010-3), not silently claimed as done.

**Rationale**: The repo precedent for cross-table integrity is real FKs with named constraints, and 008's `binding_id` / 007's `authorization_id` are stable PKs already merged. A cross-lane FK to a table created later would break the 010-branch migration run (integration tests apply migrations to a scratch DB) and would violate the single-writer rule for shared files (`docs/workflow-010-011-parallel.md:46-51`). The claim read is an adapter whose SQL targets the frozen J2 shape; absent table → fail-closed refusal, which is the correct pre-011 behavior.

**Alternatives considered**: FK to `payment_intents` now (rejected: table absent at apply time); no FKs at all (rejected: loses DB-level identity-chain integrity that constitution II/VI asks for and that the referenced tables can support today); creating placeholder 011 tables in 010's migration (rejected: writes 011 artifacts and crosses the ownership boundary).

**Evidence**: `migrations/000007_withdrawal_creation.sql:86-139`; `migrations/000008_nonce_manager.sql:76-120`; `migrations/000009_signer_service.sql:1-57` (pure-DDL + naming-rule precedent); `docs/workflow-010-011-parallel.md:98-110,129-132`; spec PLAN-1.

---

## R-010-11 — 010-independent vs joint acceptance; what substitutes may and may not claim

**Decision**: Two acceptance layers with different evidence standards:

- **010-independent (V-matrix)**: real PostgreSQL + real Anvil + real `signer-serve` (disposable local dev key) + real chain RPC, orchestrated the way existing integration tests do (testcontainers; `internal/nonce/unknown_outcome_integration_test.go:60-98`; `compose.yaml` anvil pin). The 011-owned `execution_claims` table does not exist on this branch, so 010-independent tests provision a **contract-shaped fixture** (columns exactly per frozen J2) in the scratch DB only, created by test setup, never by a migration and never cited as joint verification. Failure injection between 010 and 009 uses a test HTTP proxy that drops/duplicates responses — real HTTP with injected faults, not a mocked signer.
- **Joint (J-matrix, design requirements only)**: real 007 HTTP + 011 executor + 010 + 009 + PG + Anvil; covers intent creation, authorization supply, expired-worker fencing, joint unknown reconciliation and reorg revision (FR-16). Joint acceptance is defined here and executed later (010→011 merge order; upstream sync before dependent acceptance). Mocks/old-mechanism tests never count as joint evidence; 010-independent success is not claimed as joint success.

**Rationale**: FR-16 splits the obligations explicitly; the workflow rules forbid mock substitutes for real joint acceptance and require 010→011 merge order (`docs/workflow-010-011-parallel.md:59,76,129-132`). Labeling the claim fixture precisely prevents the classic failure mode where a test-only table gets mistaken for the real 011 contract.

**Alternatives considered**: mocking PG/RPC for speed (rejected: constitution XI forbids mocks substituting for PostgreSQL/Anvil behavior); waiting for 011 to exist before writing any 010 acceptance (rejected: violates the parallel exception and leaves 010 unverifiable); making the claim fixture a migration (rejected: would collide with 011's real migration and would be a 010-created 011 artifact).

**Evidence**: `internal/nonce/unknown_outcome_integration_test.go:60-98`; `compose.yaml`; `Makefile:13-14`; spec FR-16; `docs/workflow-010-011-parallel.md:56-59,128-132`.

---

## R-010-12 — Bounds, clocks and classification hygiene (no unfounded business thresholds)

**Decision**: All new bounds are technical, documented and configurable, or deliberately fixed at repo-common values:

- per-statement DB bound: the repo's shared `SET LOCAL statement_timeout='5s'` literal, plus `lock_timeout='5s'` on the send region (mirrors `internal/nonce/coord.go:45-48`, `internal/nonce/readapi.go:86`);
- dispatch bound: new `TXHARBOR_TX_SEND_TIMEOUT` (no default invented in the spec; plan fixes a conservative default and validates > 0 at startup, fail-closed like existing knobs);
- reconcile cadence / RPC timeout: reuse the existing `TXHARBOR_INDEX_POLL_INTERVAL` / `TXHARBOR_INDEX_RPC_TIMEOUT` knobs (006/008 precedent: no new timing knob names, `internal/config/config.go:39-41,54-58`);
- clocks: every validity comparison uses PostgreSQL `now()`/`clock_timestamp()` (`internal/signer/submit.go:30-32` precedent); the application clock is never used for gate decisions;
- classification: RPC failure classes follow the existing `eth.Kind` vocabulary (`internal/eth/client.go:28-44`), with send-specific error classes added alongside, never replacing.

No business threshold (confirmation depth, fee caps, lease TTL, retry counts as policy) is introduced by 010: caps come from the PB scope row, confirmation depth from 005's policy, lease/claim semantics from 011's frozen contract. Bounded retries are the caller's/loop's concern; 010 never uses unbounded retry loops (constitution IX).

**Rationale**: The spec forbids unfounded business thresholds and requires DB-clock validity. Reusing existing knobs keeps configuration surface minimal (XIII) and matches how 006/008 reused INDEX timing.

**Alternatives considered**: new confirmation/send policy tables (rejected: duplicates 005/PB); application-clock comparisons (rejected: skew makes gate ordering untestable and unsound); percent-based or seconds-based business SLAs for projection freshness (rejected: C11/Q1 explicitly left display-latency unspecified).

**Evidence**: `internal/config/config.go:29-58`; `internal/signer/submit.go:30-32`; `internal/nonce/readapi.go:79-86`; `docs/workflow-010-011-parallel.md:77` (C11).

## R-010-13 — Projection support (Q1) and observability: 010 supplies versioned authority, never a permission

**Decision**: 010 exposes an authoritative, read-only status view per attempt carrying `attempt_id`, `intent_id`, `binding_ref`, `signing_request_id`, `state`, `revision_seq` (monotonic per attempt, bumped on every mutation), `updated_at` (DB clock), `tx_hash` when bytes exist, the latest dispatch outcome, the latest reconcile classification, the latest receipt effect/confirmation basis, and the current recovery/authorization references. 011 may store a projection keyed by `attempt_id` recording `(source revision, updated_at)` and applying only strictly newer revisions; freshness that cannot be confirmed must be marked stale. The read is explicitly **not** a permission: every execution/replay/replacement/end-of-recovery decision must read and verify the current authoritative facts (FR-13; Q1). Observability follows the existing registry: counters for dispatch outcomes, gate refusals by class, unknown gauge, reconcile outcomes, receipt effects, confirmation transitions, reorg revisions; structured logs with `chain_id`, `attempt_id`, `intent_id`, `binding_ref`, `tx_hash`, `nonce`, gate basis fields — never signature bytes, signed bytes, or credentials (secrecy precedent `specs/009-signer-service/plan.md`).

**Rationale**: J5 fixes the authority/version split; FR-13 fixes the anti-permission rule; the revision counter is the smallest mechanism that makes "old version never overwrites new" checkable without a timestamp race (mirrors 008's monotonic `lease_version`/`registry_seq` idea). Metrics/log fields follow the constitution's observability principle and the existing code shape (`internal/metrics`, `internal/logx`).

**Alternatives considered**: let 011 poll `tx_attempts` directly (rejected: 010 owns the fact read shape and must keep the authority boundary explicit; also keeps refusal/evidence assembly in one place); make the projection the decision input (rejected: Q1 forbids it); expose undated state (rejected: cannot order revisions).

**Evidence**: `docs/workflow-010-011-parallel.md:124-128` (J5); spec FR-13; `internal/nonce/readapi.go:42-50` (fixed notice pattern); constitution XII.

---

## R-010-14 — Known limitations and residuals (reported, not papered over)

These are stated for adjudication; none is presented as closed, and none is addressed by adding grace periods, TTLs, or a wider "in-flight" definition (spec FR-15 forbids those).

- **G-010-1 (time-based gate changes between the last evaluation and dispatch entry).** Locks order *write-based* invalidations (revocation, pause, claim re-lease, binding change) against the send region, but **expiry is a time boundary with no writer to lock**. The design re-evaluates authorization and claim expiry on the DB clock as the last step before dispatch and records the observed basis in the send row. The irreducible residual is the interval between that evaluation returning and the dispatch call being entered: an expiry that takes effect exactly there can no longer be prevented. The design does not add grace/TTL and does not widen in-flight; a send that entered dispatch after the last passing evaluation is recorded as legally in-flight, with the evaluated timestamps as evidence. **Limitation reported for adjudication.**
- **G-010-2 (database-region failure vs network send).** If the region's transaction/connection is lost after dispatch begins (crash, pool failure, commit failure), the send may have happened with no committed record. Per J4 the attempt is treated as `unknown` and recovered by probing `tx_hash`; the binary will never infer "not sent" from a failed transaction. This dual-write residue is not removable by a single-database protocol and is reported as such.
- **G-010-3 (no FK to `payment_intents` on the 010 branch).** As in R-010-10, the intent reference is shape-checked and validated at send time by the claim read; the FK closure is a joint-integration item owned by the 010→011 merge, not a 010-alone deliverable.
- **G-010-4 (`execution_claims` absent until 011 lands).** The live adapter fails closed (`claim_absent`/unavailable) when the table/row is absent; 010-independent acceptance uses a contract-shaped fixture that must never be cited as joint verification (R-010-11).
- **G-010-5 (mempool absence is not evidence).** `not_found_yet` can never become a terminal verdict; an attempt whose bytes never landed and whose nonce was never consumed can remain `unknown` indefinitely by design. Resolution requires chain facts (receipt, nonce consumption recorded by 008, or a confirmed replacement). This is the honest cost of FR-03 and is not "fixed" by declaring failure.
- **G-010-6 (revocation paths must conflict with the region's locks).** The ordering guarantee holds only for invalidation mechanisms that take a conflicting lock (grant/scope row `FOR UPDATE`, claim row update, pause/recovery table writes, binding writers via the coordination row). A future revocation mechanism that writes only to an unlocked side table would not be covered and must not be introduced without re-opening this analysis.
- **J2 column-name mapping.** The frozen contract fixes the *semantics* of 011's claim row (intent-unique, worker identity, monotonic version, expiry, revocation marker); the concrete column names are 011's plan choice. 010's adapter carries an explicit mapping table (`contracts/send-api.md` §claim) so a naming difference is absorbed in one place and cannot silently weaken the fence.

**Rationale for reporting rather than closing**: FR-15 explicitly says that if the guarantee cannot be met, the plan must report the concrete gap for adjudication and must not self-widen in-flight scope; Q1 says mechanisms are left to plan while the business semantics stay fixed. G-010-1 and G-010-2 are the only two places where a guarantee is not fully mechanical; both are stated with their precise boundaries and observable evidence.

---

## Rejected alternatives (cross-cutting)

| Alternative | Why rejected |
|---|---|
| Gate check, commit, then dispatch outside the DB transaction | Re-creates OPEN-2: "decision first" is not in-flight; revoke-vs-send becomes undecidable (G-010-1 would widen to an unbounded window) |
| 010-owned lock table / advisory lock | No upstream writer takes it; zero cross-writer guarantee |
| Claim `FOR UPDATE` for the region | Blocks legitimate 011 renewal and still does not cover 006/008 writers |
| Have 009 return raw signed bytes | Approved 009 contract forbids it; reopening 009 is out of scope |
| `UNIQUE(content_hash)` to block duplicate attempts | Blocks legitimate recovery-version rebuilds; duplicate bytes are already impossible to sign twice |
| Treat unknown/not-found as failed | Forbidden by FR-03/US2/US5; causes re-payment risk |
| Claim-gated reconciliation | Freezes unknown tracking during takeover; makes bookkeeping an execution privilege (M2) |
| Separate process / new service for 010 | No secret boundary; adds failure domain and a second send entry point (XIII) |
| 010-owned confirmation policy table | Duplicates 005's explicit configurable chain depth |
| New timing/policy knobs for reconcile | 006/008 precedent reuses INDEX knobs; keeps config surface minimal |
| Mocked signer/PG/RPC for acceptance | Constitution XI and FR-16 forbid substitutes for real joint verification |

---

## Technical Context unknowns — resolved

| Unknown | Resolution |
|---|---|
| Language/deps for 010 | Go 1.26.5, pgx v5.11.0, go-ethereum v1.17.5 (`types`, `rlp`, `crypto`, `common`, `abi`, `ethclient`); no new dependency (R-010-01/03/08) |
| How signed bytes become durable when 009 returns no bytes | Local reconstruction + hash cross-check + `BYTEA` persist before send (R-010-03) |
| How the broadcast race is ordered | Fixed-order lock region holding locks across dispatch (R-010-04) |
| How send results are classified | Three outcomes, fail-safe allowlist, known results immutable (R-010-05) |
| How unknown is resolved | Probe-first reconcile; claim-free; `not_found_yet` never terminal (R-010-06) |
| How replacement authorization reuse is enforced | PB conditional reuse + fee-dimension arithmetic + 009 anchor derivation (R-010-07) |
| Receipt/Transfer semantics | Reuse `eth.TransferSig` + deposit parser structure (R-010-08) |
| Confirmation depth source | 005 `confirmation_policy_history` read-only (R-010-09) |
| Migration number | Provisional `000011`, re-verified at merge; additive; no cross-lane FK (R-010-10) |
| 010-independent vs joint acceptance | Two-layer matrix with labeled contract-shaped fixture (R-010-11) |
| Clocks/bounds/config | DB clock; 5s guards; one new send timeout; INDEX knobs reused (R-010-12) |
| Projection versioning | `revision_seq` + `updated_at`, never a permission (R-010-13) |
| Open guarantees | G-010-1..G-010-6 reported with boundaries (R-010-14) |

## Evidence separation (status, not proof)

- Prior-step evidence reused as baseline only: 006 merge `8e1a440`, 007 `19fa11e`, 008 `02641fb`/`da316d5`, PB `da316d5`, 009 `295c49d` merge point, joint contract v1 in `docs/workflow-010-011-parallel.md` (worktree HEAD `97a4062`). Nothing here re-verifies upstream work.
- This step produced **documentation only** (plan/research/data-model/contracts/quickstart); no code, migration, test, service, or container was written or executed. Every V/J scenario is design-only and belongs to tasks/implement.
- T000-P (production provider) and A-13 (full-chain E2E) remain OPEN; nothing in this plan closes them.
- No conformance with 011 is claimed: the 011 side is consumed only through the frozen joint contract v1; joint acceptance remains a requirement (J6).
