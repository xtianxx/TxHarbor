# Contracts: Broadcast Gate (send region) — 010 Transaction Lifecycle Management

**Branch**: `010-transaction-lifecycle` | **Date**: 2026-09-17 | **Spec**: [spec.md](../spec.md) | **Research**: [research.md](../research.md) R-010-04/05/06/14

This contract defines how 010 satisfies FR-11/FR-14/FR-15 (Q2/Q3): what is read, in what order, under which
locks, what each observation means, what evidence is recorded, and — explicitly — what the mechanism does
**not** claim.

## 1. Participants, write entries, effective points

| # | Participant | Write entry | Change | Effective point | 010 region lock |
|---|---|---|---|---|---|
| 1 | 011 claim/lease | 011-owned writer | `lease_version` bump / status / revocation marker | writer COMMIT | claim row `FOR SHARE` |
| 2 | 006 recovery establish | `internal/indexer/reorgcommit.go` | `reorg_recovery` insert + events | writer COMMIT | coordination row `FOR UPDATE` + gate-table `SHARE` |
| 3 | 006 recovery verify/release | `CompleteRecoveryVerify` / `AuthorizeRecoveryRelease` | phase update / row delete + event | writer COMMIT | coordination row + gate-table `SHARE` |
| 4 | 006 pause on/off | stream writers or manual DBA `INSERT`/`DELETE` | pause row | writer COMMIT | gate-table `SHARE` (relation-level, covers existing rows and future inserts) |
| 5 | 007 revoke / re-supply | `RevokeGrant` / re-supply + PB scope bump | grant `state` / scope fields/version | writer COMMIT | grant row `FOR SHARE` + scope row `FOR SHARE` |
| 6 | 008 pause / registry disable / release / binding change | 008-owned writers | 008 gate, binding state | writer COMMIT | coordination row `FOR UPDATE` + scope row `FOR SHARE` |
| 7 | 010 dispatch | this region | `tx_send_attempts` + attempt state + event | region COMMIT | — |

## 2. Fixed lock order (deadlock-free by construction)

```
SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='5s';
 1. execution_claims            FOR SHARE   (keyed by intent_id)
 2. indexer_lease               FOR UPDATE  (the chain coordination row)
 3. LOCK TABLE indexer_pause, log_pause, deposit_pause,
        reorg_recovery, reorg_recovery_events IN SHARE MODE
 4. 006 gate read (one SELECT) + scope-row FOR SHARE (nonce_scope_state)
 5. withdrawal_authorizations FOR SHARE + withdrawal_authorization_scopes FOR SHARE
 6. tx_attempts FOR UPDATE
 7. dispatch (bounded) + record + COMMIT
```

Why this order is acyclic against the existing writers:

- 006 recovery writers take the coordination row then their own tables (008's `coord.go` documents the same
  framing), so coordination-before-gate-tables matches them.
- 008 writers take coordination row → scope row; 010 takes coordination row → scope row, same direction.
- 009's submit/delivery takes scope row `FOR SHARE` → gate tables → grant; it never takes the coordination
  row or 010 tables, so no cycle exists between 009 and 010.
- 010 takes the claim row first; 011's claim writers take only their own row, so 010→claim never forms a
  cycle with 010→coordination.
- 010 never writes any participant table, so it cannot appear as a conflicting writer in someone else's
  lock sequence.

Any lock wait/deadlock is fail-closed: the region refuses (`gate_read_failed`/`coordination_unavailable`)
with zero dispatch and is retryable with the same identity.

## 3. Gate decision matrix

| Gate | Source of truth | Condition to send | Refusal class | Recorded basis |
|---|---|---|---|---|
| Execution qualification | 011 claim row | claim exists, active, not revoked, `expires_at > clock_timestamp()` (DB wall-clock per statement; `now()` is region-start and MUST NOT be used here — 009 `submitClockSQL` precedent), `lease_version == presented` | `claim_absent` / `claim_revoked` / `claim_expired` / `claim_version_mismatch` | claim id/worker/version, expiry, evaluated wall-clock |
| 006 pause | 3 pause tables | zero rows for the deployment chain | `pause_present` | which table(s) hit (multi-cause visible) |
| 006 recovery | `reorg_recovery` + events | no active row, `current_version == attempt.recovery_version` (active seq else events MAX else 0) | `recovery_active` / `recovery_version_changed` | phase/recovery id, observed vs built version |
| Authorization | `withdrawal_authorizations` + PB scope | row exists; `state='active'`; not expired on `clock_timestamp()`; chain/asset/recipient/amount/sender equal; scope `intent_id` equal | `authorization_*` | grant id/state/expiry, scope version |
| Replacement reuse | PB scope row | `allows_fee_replacement`; `gas_limit×max_fee_per_gas ≤ fee_max_total`; `max_fee_per_gas ≤ fee_max_per_gas`; `max_priority_fee_per_gas ≤ fee_max_priority`; `authorization_version` equal | `scope_reuse_forbidden` / `fee_scope_exceeded` | per-dimension observed vs cap, version equality |
| 008 binding | `nonce_bindings` + registry + holds + scope row | intent/chain/sender/nonce equal; state ∈ (`allocated`,`in_flight`); registry `active`; zero active holds | `binding_absent` / `binding_conflict` / `binding_paused` / `binding_terminal` / `binding_read_failed` | binding state, hold causes, registry state |
| Attempt | `tx_attempts` | row exists; sendability per state matrix; declared kind consistent; optional `revision_seq` guard | `attempt_not_found` / `attempt_not_sendable` / `already_accepted` / `send_mode_mismatch` / `send_stale` | state, revision, prior dispatch outcome |

No gate observation is cached across sends; every send re-reads all gates inside the region (FR-11: a prior
successful check is never a persistent permit).

## 4. Timelines

**(a) invalidation committed before the region.** The invalidation writer takes its conflicting lock and
commits; the region then acquires locks and reads the new state → refusal class + observed basis committed;
zero dispatch (SC-06).

**(b) region first, invalidation during dispatch.** The region holds claim `FOR SHARE` (blocks claim
re-lease/revocation), coordination `FOR UPDATE` (blocks 008 writers and 006 recovery establishment),
gate-table `SHARE` (blocks pause/recovery inserts and DBA pause release), grant/scope `FOR SHARE` (blocks
revoke/re-supply). The invalidation writer blocks until the region commits; the dispatch is recorded as
legally in-flight (it entered the uncancellable external stage while every gate was valid); the invalidation
governs later replays/replacements only. The evidence is the committed send row's gate snapshot, its
`dispatched_at`, and the invalidation writer's own commit order.

**(c) time-based expiry (G-010-1; 2026-09-17 approved limited exception to Q3).** Expiry has no writer and therefore no lock. The region re-evaluates
authorization and claim expiry with `clock_timestamp()` (statement wall-clock, never transaction-start
`now()`) as the last read before dispatch and records
`observed_expires_at`/`observed_now`. A natural expiry taking effect in the residual interval between that read and entering dispatch is recorded as a "post-final-check natural-expiry send residual" and reconciled; it MUST NOT be described as legally in-flight before invalidation. This exception covers natural expiry ONLY — not explicit revocation, pause, execution-version replacement, DB lock/connection failure, or any other residual — and approves no configurable grace period. Queueing, backoff, reconnects, auto-retries, or resumed execution MUST NOT reuse a prior check as permission and MUST re-evaluate gates; the plan constrains those paths and the thread-stall limits instead of assuming a fixed or tiny window. Preserve the final-evaluation time, authorization expiry, lease expiry, execution version, and observable send evidence; unobservable instants MUST NOT be fabricated as precise facts. Definitive send results are kept; only indeterminate outcomes are recorded `unknown` and reconciled. G-010-1 closure does not pass any other residual or the overall send-protection design.

**(d) database/network independent failure (G-010-2).** If the region's transaction/connection dies after
dispatch began, the dispatch may have happened with no committed record. The attempt is treated as
`unknown` and resolved by probing `tx_hash`; the system never infers "not sent". A crashed region is
indistinguishable from "never dispatched" at the storage layer, so the next operation performs a reconcile
probe before dispatching (R-010-06).

**(e) known results are not rewritten.** `accepted`/`rejected` rows are immutable. Reconciliation appends
observations and revisions; it never edits a send row and never converts a known send result into `unknown`
or vice versa. The attempt-level `unknown` state means the *business effect* is undetermined, which is a
different claim from the send-level result.

**(f) natural expiry vs explicit revocation.** Both refuse with identical effect and zero dispatch, but the
recorded basis distinguishes them (expiry = clock comparison at `observed_now`; revocation =
`state='revoked'`/revocation marker read under the share lock). Neither is presented as an execution permit
at any later time.

## 5. What this mechanism does not claim

- Holding a lock is not itself a gate decision: the region still reads and checks every condition inside the
  locks; the lock only guarantees the check and the dispatch are ordered against the writers.
- Entering the send function is not evidence of a legal in-flight operation: legality is defined by "gates
  passed and dispatch entered inside the locked region" and is recorded as such; queueing, preparing,
  checking or writing a send-intent row alone is never presented as in-flight.
- One passing query is never presented as eliminating the race (Q3 explicitly forbids it); the ordering
  proof is the lock conflict structure, and the time-based residue is reported (c).
- `accepted` does not mean paid; the payment verdict is the receipt + Transfer check (FR-08).
- 010 does not clear, release or queue any 006/007/008/011 state; it only reads.

## 6. Verification hooks

- Assert the lock sequence via concurrent injected writers: a revoke/pause/claim update started during the
  region must block until region commit (test observes it via a blocked writer + commit order), and a
  writer committed before the region must be observed by the refusal.
- Assert zero-dispatch refusals: no `eth_sendRawTransaction` call occurs (proxy/interceptor count),
  refusal event + snapshot committed.
- Assert evidence fields on committed send rows (`observed_*`, `dispatched_at`, `rpc_class`).
- Assert read-only diff: no 006/007/008/011 row is mutated by any 010 operation.
- Assert `rejected`/`unknown` rows are never updated by later reconcile passes.
