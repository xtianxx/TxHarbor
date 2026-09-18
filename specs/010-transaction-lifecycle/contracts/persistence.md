# Contracts: Persistence & Recovery Protocol — 010 Transaction Lifecycle Management

**Branch**: `010-transaction-lifecycle` | **Date**: 2026-09-17 | **Spec**: [spec.md](../spec.md) | **Data model**: [data-model.md](../data-model.md) | **Research**: [research.md](../research.md)

This contract fixes the transaction boundaries, the ordering guarantees, the crash/retry determinism, the
unknown/reconcile protocol and the receipt/revision protocol. Constitution VI is the governing principle:
one durable state transition per committed boundary, no step that can leave "broadcasted without a durable
transaction reference".

## 1. Transaction catalog (fixed order)

| Tx | Boundary | Commits exactly | Never contains |
|---|---|---|---|
| T1 `attempt persist` | before any 009 call | identity + content + intent/binding/authorization references | external calls |
| T2 `signing persist` | before any dispatch | signature + signed bytes + `tx_hash` + `prepared→signed` | external calls other than the bounded 009 submit |
| T3 `send region` | before any dispatch result is observable to the caller | gate snapshot + dispatch outcome + state transition + event | any second external call; any write to upstream tables |
| T4 `reconcile/receipt/revision` | after chain probes | one `tx_reconciliations` row always; receipts/confirmation/revision when observed | any dispatch |

Guards: every transaction opens with `SET LOCAL statement_timeout='5s'`; T3 adds
`SET LOCAL lock_timeout='5s'`. State mutations are version-guarded
(`WHERE attempt_id = $1 AND revision_seq = $2` or expected-state) and treat `RowsAffected() <> 1` as a
stale conflict → rollback, zero dispatch. Every write that can be repeated is carried by a named
UNIQUE/PK constraint (data-model §Idempotency carriers) and classified by `ConstraintName` only.

## 2. Crash/retry determinism

| Crash / failure point | Durable state | Next operation behavior | Never happens |
|---|---|---|---|
| before T1 COMMIT | none | a retry may create the attempt | partial identity |
| after T1, before 009 response | `prepared` | retry re-sends the same envelope to 009 (same identity) | new attempt/identity |
| after 009 `signed`, before T2 COMMIT | `prepared` (+ 009 persisted result) | retry 009 → identical signature/hash → T2 | re-signing under a new identity |
| after T2, before T3 | `signed` + bytes | reconcile probe first; `not_found_yet` → dispatch under full gates | assume "not sent" |
| inside T3 before dispatch | rollback | indistinguishable from the previous row; probe-first on the next operation | committed refusal for a dispatch that never happened |
| after dispatch begins, before T3 COMMIT | rollback (G-010-2) | probe-first; attempt stays `signed` or becomes `unknown` by evidence | infer failure/absence |
| after T3 COMMIT, response lost to caller | full durable state | caller retry: `SendInitial` → `already_accepted`; `SendReplay` → allowed under gates | duplicate attempt row |
| after T4 COMMIT | reconcile/receipt/revision durable | repeat reconcile converges (UNIQUE receipt identity), appends observation | duplicate receipt row for the same block |

Response loss between 010 and its caller is therefore never a correctness problem: everything durable was
committed before the response was shaped, and the retry paths are explicit refusals or idempotent replays.

## 3. Unknown classification and recovery

**Classes** (`tx_reconciliations.classification`, append-only):

| Class | Meaning | Permitted follow-up |
|---|---|---|
| `found_pending` | `eth_getTransactionByHash` returned the tx with no block | stays unknown/`sent`; replay is allowed after the observation, under gates |
| `included` | tx is in a block; receipt/canonicality/verification proceed | receipt path (no dispatch needed) |
| `not_found_yet` | the node has no record | never a failure verdict; replay/replacement allowed **after** this observation, under gates (G-010-5) |
| `unavailable` | probe failed / indeterminate | fail closed: no dispatch may be decided from it; retry later |

**Protocol**: a dispatch from `signed` (crash-ambiguous) or `unknown` requires a reconcile observation
committed first in the current operation; the observation is durable evidence that "先对账" happened, not a
rubber stamp. Reconciliation is claim-free (facts, not execution), idempotent, read-only against upstream
tables, and never sends.

**Known results are immutable**: `tx_send_attempts` rows are never updated. Reconcile appends rows/events;
it never converts `rejected`/`accepted`/`unknown` into one another. Attempt-level `unknown` describes the
business effect, not the send fact.

## 4. Receipt, Transfer verification and confirmation

1. `eth_getTransactionReceipt(tx_hash)`; absence → `not_found_yet` reconcile observation, remain unknown.
2. Canonicality: `(block_number, block_hash)` must match `chain_blocks WHERE canonical`; otherwise the
   receipt is stored `unverified` and re-checked (never assumed canonical; indexer lag is not reorg).
3. Effect verdict (FR-08): `status = 1` **and** an expected `Transfer` log (emitter = asset; topic1 =
   sender; topic2 = recipient; 32-byte data = amount). Any deviation → `ineffective_*` with the observed
   difference; never "paid".
4. Confirmation: `confirmations = canonical_head − receipt_block + 1`; threshold/policy sequence from
   `MAX(confirmation_policy_history.policy_seq)` (005, read-only). Threshold reached → `confirmed` with an
   event carrying the basis (`confirm_tip_*`, threshold, policy seq).
5. Reorg: every reconciler pass re-verifies canonicality for every non-orphaned receipt. A mismatch/missing
   canonical block marks the receipt `orphaned`, appends a revision event carrying the observed
   `recovery_version`, and moves the attempt to `orphaned`/`unknown` — never creating a new intent,
   binding or payment. Re-inclusion creates a new receipt row; verification success appends `reconfirmed`
   and restores `effective`/`confirmed`.
6. Nonce-consumption by a sibling: when an attempt on a binding reaches `effective`/`confirmed`, all sibling
   attempts on that binding are revised to `replaced` (they can never be included).
7. Revision is bookkeeping (011 M2, inherited): it needs no new authorization and never sends; any
   subsequent replay/replacement still runs the full current-gate send region.

## 5. Refusal and evidence recording

- Zero-dispatch refusals append one `tx_attempt_events` row (`event='gate_refused'`, `reason_class`, observed
  basis in `detail`) with the same `revision_seq` guard semantics; the attempt state does not change.
- Every committed send row carries the gate snapshot (`observed_*`) and evidence class; `dispatched_at` is
  set when the region entered the external stage.
- Every state mutation bumps `revision_seq` by exactly 1 and appends an event with the next `event_seq`.
- Logs carry identity/tx/gate-basis fields only; signed bytes, signatures and credentials are never logged.

## 6. Read-only boundary (asserted by tests)

Diff-based integration assertions require that no 010 path mutates `indexer_pause`, `log_pause`,
`deposit_pause`, `reorg_recovery`, `reorg_recovery_events`, `chain_blocks`, `withdrawal_authorizations`,
`withdrawal_authorization_scopes`, `nonce_bindings`, `nonce_scope_state`, `nonce_scope_holds`,
`nonce_wallet_registry`, `confirmation_policy_history`, or `execution_claims`. The only lock statements
against upstream tables are `LOCK … IN SHARE MODE` and `SELECT … FOR SHARE` (lock acquisition, no data
modification) — the 009 precedent (`specs/009-signer-service/contracts/gates.md:48-51`).

## 7. Failure-path coverage (design; executed in tasks/implement)

Timeout, response loss, rate limiting, invalid response, node/proxy failure, PostgreSQL interruption
mid-region, duplicate delivery of the same request, process crash at each T-boundary, reorg during
confirmation, nonce contention (008 scope), replacement race, and signer unavailability are all enumerated
as scenarios (quickstart V3–V9); each maps to a durable outcome and a recovery path rather than an
in-memory assumption (constitution IX).
