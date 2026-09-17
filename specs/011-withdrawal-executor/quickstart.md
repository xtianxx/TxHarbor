# Quickstart Validation — 011 Withdrawal Execution Worker

**Branch**: `011-withdrawal-executor` | **Date**: 2026-09-17 | **Spec**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md) | **Contracts**: [api.md](contracts/api.md), [gates.md](contracts/gates.md), [lifecycle.md](contracts/lifecycle.md), [persistence.md](contracts/persistence.md)

**Status**: design only. This step defines the scenarios and their expected outcomes; executing them belongs
to tasks/implement. No scenario is claimed as run. Substitutes used in 011-independent runs MUST NOT be
presented as joint verification (C10; FR-14).

## Isolation scheme (mandatory for every V-scenario)

- Worktree-local PostgreSQL database name (e.g. `txharbor_011_<worktree>`) and non-default ports; the shared
  `compose.yaml` PostgreSQL/Anvil instance is coordinated by the main orchestrator (workflow P4/R4); tests
  MUST NOT write the default database.
- 011-independent runs use real HTTP (`serve` admission/view routes) + real PostgreSQL + real migration
  `000012`; the 010 boundary uses contract-shape doubles only (never cited as joint).
- Race-sensitive scenarios (V2–V5) run with the Go race detector and real concurrent connections.
- No chain dependency in 011-independent runs: `internal/execution` must not import RPC/dial packages
  (structural assertion, V12).

## V1 — Admission and stable intent creation (US1; FR-01/FR-02/FR-03/FR-07; SC-01)

**Preconditions**: migration applied; caller with `can_execute=TRUE`; 007 request `accepted`; grant `active`
with matching fields; PB scope present with declared `intent_id`/`request_id`/`sender`; registry `active`;
no pause/recovery.

**Steps/assertions**:
1. `POST /withdrawals/{id}/execution` ⇒ 201, exactly one `payment_intents` row; `intent_id` equals the scope's
   declared value; `UNIQUE(request_id)`/`UNIQUE(authorization_id)` hold; an `admitted` event exists; the
   projection row exists with `state_version=1`.
2. Repeat (sequential and concurrent ×N) ⇒ exactly one intent; 200 recorded response; zero second intents.
3. Missing/expired/revoked grant; missing scope; scope `request_id`/`sender` mismatch; unregistered sender;
   pause row present; active recovery row present; `can_execute=FALSE`; foreign caller; non-`accepted`
   request ⇒ correct status (401/403/404/409/422), **zero** intent rows, refusal evidence recorded.
4. Ordering evidence: in a joint run, the intent row exists before the 008 binding row for the same
   `intent_id` (FR-02 "先于 008 预留"; joint only).

## V2 — Claim exclusivity, lease, renewal, expiry (US2; FR-04; SC-02)

1. Two workers race `Acquire` on one intent (real PG, N iterations) ⇒ exactly one winner; the loser returns
   no-row with zero side effects; `lease_version=1` for the winner.
2. Winner renews before expiry ⇒ execution right continues (`expires_at` advances, same version).
3. Freeze the winner (stop renewals) past TTL ⇒ any fenced write affects 0 rows; a claim-row read shows
   `expires_at <= now()`; a new `Acquire` takes over with `lease_version=2`.
4. Old-version write/step-issue attempts ⇒ `claim_lost`, zero writes, zero sends.

## V3 — Disqualification fencing: DB writes and 010-side sends (US3; FR-05; SC-03)

1. With worker A holding a valid claim, force disqualification (expiry, operator `claim-revoke`, or stall
   takeover); A's `T-step-converge` / state transitions affect 0 rows.
2. A calls `Advance` with its old `(owner_id, lease_version)` ⇒ the 010-side boundary refuses
   (`claim_not_current`/`claim_lost`), no send, refusal evidence recorded.
3. Revoke-before-gate-read vs gate-read-before-revoke: with real PG, the outcome follows the row-lock order
   (refuse / decision-stands); a send that entered the non-cancellable stage on response loss is recorded as
   unknown, never "nothing happened" (joint run exercises this against real 010; the independent run asserts
   011's side: single-row exact-version updates only, no lock-free claim, no cached permit).
4. Already-sent facts are never retracted: revocation does not rewrite history and does not mark failure.

## V4 — Takeover and equal re-competition (M1n; FR-04/FR-06; SC-04)

1. After disqualification (expiry or revocation), the **same** worker may re-acquire and must receive a new
   `lease_version`; no identity ban exists (attempt by the same `owner_id` succeeds).
2. Re-acquisition re-verifies all current gates (authorization/pause/recovery/registry/binding); a gate
   failure refuses with zero sends.
3. The new holder continues the **same** `intent_id`, same nonce binding reference, same attempt history;
   zero second intents; old steps are visible but cannot be converged by the old version.
4. Old in-process tasks that were never given the new version cannot act: any write with the old version is
   fenced (SC-04: intent continuity 100%, second intents 0, history complete).

## V5 — Stall determination and automatic takeover (M3; FR-15)

1. **No false takeover (progress-first)**: a claim with recent progress (step or `reconcile_observed` event)
   is not taken over even though `last_progress_at` is older than a naive timer; the takeover CAS predicate
   is false under the row lock.
2. **Takeover (stall-first)**: a claim whose `last_progress_at` exceeds `stall_window` with no progress is
   atomically invalidated and re-issued to a new owner (`lease_version+1`), with a `taken_over` event carrying
   prior owner/version/watermark; the old holder's next write affects 0 rows.
3. Renewal alone does not prevent takeover (heartbeat ≠ progress); repeated re-claiming is not progress.
4. Stall sweep marks (`stall_flagged_at` + event + metric) without taking over when no candidate exists;
   FR-15 evidence is retained, business state (intent/intent state/nonce binding/history/pauses) is untouched.
5. Operator `claim-revoke` requires evidence and the exact `expected_lease_version`; insufficient evidence or
   version mismatch refuses with zero writes.

## V6 — Authorization lifecycle mid-execution (FR-07; SC-03)

1. Admission with a valid grant; then revoke the grant (or expire it on the DB clock) ⇒ subsequent
   send-enabling step issue refuses (`authorization_revoked`/`authorization_expired`), zero calls to 010.
2. Scope re-supply after admission bumps `authorization_version` ⇒ next step refuses
   (`authorization_changed`), requiring re-established basis; no silent rebinding.
3. Authorized again (new grant + scope with the same intent linkage where permitted) ⇒ under current gates a
   new step may be issued; no second intent is ever created.
4. Scheduling never issues authorization: no code path in the worker/CLI can supply or revoke a 007 grant
   (import/statement assertion).

## V7 — 010 advance: retry, crash, unknown reuse (US4; FR-08; SC-05)

1. 010 double returns `refused_gate`/`refused_basis` ⇒ step `converged` with evidence; no state corruption.
2. 010 double returns `pending_unknown` ⇒ step `unknown`, intent `reconciling`, zero failure claims; a retry
   with the same `step_id` never creates a second attempt (double asserts keyed convergence).
3. Crash after step issue, before/after the 010 call ⇒ restart reconciles the open step via
   `LifecycleReader`; re-issue of the same `step_id` happens only with positive "no attempt persisted"
   evidence; unknown stays unknown.
4. Bounded retries: retryable vs non-retryable classes distinguished; exhausted retries leave the step
   `issued` for the next cycle (no unbounded loop).

## V8 — Recovery pause, version, multi-pause (US6; FR-11)

1. Pause row inserted after claim but before step issue ⇒ step issue refuses (`recovery_paused`), zero sends,
   zero "queued for later".
2. Active `reorg_recovery` row ⇒ refuse; unknown outcomes stay in reconcile.
3. Multiple independent pause rows: clearing one does not bypass the others; 011 only reads (no deletes).
4. Version move between 011's observation and 010's build/send ⇒ 010 refuses (`refused_basis`), step recorded
   converged/refused; re-observe and retry under current gates — no grace period.

## V9 — Projection freshness and decision separation (US5; FR-09; SC-06)

1. Feed a newer `lifecycle_version` then an older one ⇒ stored version only moves forward; the older input
   affects zero rows; the payload shows the newer attempt reference.
2. Simulate authority unavailability ⇒ `freshness='possibly_stale'`, `stale_since` set, old terminal state
   never presented as verified current; recovery read clears it to `confirmed`.
3. Decision-path assertion (static + runtime): claim, step issue, reconcile, revision application never read
   `request_status_projection`; `withdrawal-exec projection-refresh` updates only the projection.
4. Forced refresh is audited (`projection_refreshed`) with `operation_id` dedup; no display SLA is asserted
   anywhere (C11 not self-approved).

## V10 — Reorg revision and history (US6; FR-10; SC-07)

1. Apply a revision (`completed → revised`) ⇒ same intent, same history, `revision_applied` event recorded;
   zero new intents; zero compensation payments.
2. Revision needs no authorization and no claim (M2): the transition succeeds with no active claim.
3. A subsequent send-class step after revision requires current gates and a valid claim; without them it
   refuses (no reuse of invalidated basis, no auto-repay of a failed payment).

## V11 — Explicit state machine and illegal transitions (FR-12)

1. Illegal transitions (`admitted → completed` without authority facts; unclaimed `claimed → executing`;
   `failed → completed` without an authority revision; `→ revised` without a source version) are refused with
   zero writes and recorded as errors.
2. `refusal` is not a transition: repeated gate refusals leave the state unchanged and are observable.
3. Every allowed transition is version-guarded (`state_version` CAS); concurrent transitions from two actors
   converge to one winner (loser re-reads, no lost update).

## V12 — Migration, boundary, observability, secrecy (FR-13/FR-14; constitution)

1. `000012` up/down reproducible on a scratch DB; negative probes hit named constraints (`payment_intents_request_uniq`,
   `execution_claims_pkey`, `execution_events_kind_check`, `execution_ops_audit_operation_id_uniq`,
   `execution_steps_open_uniq`, projection freshness CHECKs) with 23505/23514 and classification by name only.
2. Additive-only: `git diff`/schema diff show no change to 000001–000010 objects; 011 tables are the only new
   objects; no stored functions/triggers.
3. Boundary: `internal/execution` imports no RPC/dial package, no 006/007/008 writer package; contains no
   non-`SELECT` statement against upstream tables.
4. Observability/secrecy: every scenario asserts the structured fields exist on its rows/events/logs; log/metric
   scan finds no credentials, keys, raw signed/transaction bytes; no float types in the feature's code or schema.
5. Integer discipline: amounts/fees/nonces rendered as integer decimal strings; no `float64` in the 011 paths.

## V13 — Joint acceptance requirements (FR-14/C10; design only, A-13 stays OPEN)

Real HTTP + real PostgreSQL + real Anvil, with real 010 (and 008/009 as its dependencies), executed after
010 merges and the necessary upstream sync, in the 010→011 merge order:

1. End-to-end first broadcast: admission (011) → intent → claim → 010 attempt → 009 signature → Anvil send →
   receipt/confirmation → `completed`; identity chain `request_id → intent_id → binding → attempt_id →
   signing_request_id → authorization/scope version` intact and traceable.
2. Concurrency: two workers on one intent; revoke/expiry racing a send at 010's gate (both lock orders);
   no double send, no second attempt for one step.
3. Unknown: RPC timeout / response loss / restart during send ⇒ unknown + reconcile, never failure/not-paid;
   no second intent/binding.
4. Replay and replacement: same-bytes replay identity; replacement under the PB conditional reuse rule,
   with the fee-triple test on the 010 side; history preserved.
5. Recovery: pause set/clear, reorg with receipt invalidation/confirmation rollback ⇒ revision tracking,
   projection freshness, no compensation intent.
6. Explicit non-substitution: any contract-shape double or old-mechanism test used earlier is NOT cited as
   joint evidence; 010's own acceptance is not joint acceptance.

## Exit criteria (design mapping)

| Scenario | Requirements | Success criteria |
|---|---|---|
| V1 | FR-01/FR-02/FR-03/FR-07 | SC-01 |
| V2 | FR-04 | SC-02 |
| V3 | FR-05 | SC-03 |
| V4 | FR-04/FR-06 (M1n) | SC-04 |
| V5 | FR-15 (M3) | SC-04 (history/continuity) |
| V6 | FR-07 | SC-03 |
| V7 | FR-08 | SC-05 |
| V8 | FR-11 | SC-05 (unknown stays unknown, never re-pay) |
| V9 | FR-09 | SC-06 |
| V10 | FR-10 (M2) | SC-07 |
| V11 | FR-12 | SC-01..SC-07 (structure) |
| V12 | FR-13/FR-14 + constitution | quality gates |
| V13 | FR-14/C10 | joint acceptance (execution deferred) |

## Operator runbook (design; commands land with implementation)

```text
txharbor withdrawal-worker                       # long-running: claim/renew/advance/reconcile/projection
txharbor withdrawal-exec permission-set   --caller-id N --can-execute=true --operation-id op-1 --operator alice --reason "…"
txharbor withdrawal-exec permission-revoke --caller-id N --operation-id op-2 --operator alice --reason "…"
txharbor withdrawal-exec claim-revoke     --intent-id I --expected-lease-version V --operation-id op-3 \
                                          --operator alice --reason "worker wedged" --evidence "…"
txharbor withdrawal-exec projection-refresh --request-id R --operation-id op-4 --operator alice
txharbor withdrawal-exec claim-show --intent-id I     # read-only
txharbor withdrawal-exec step-list  --intent-id I     # read-only
txharbor withdrawal-exec event-list --intent-id I     # read-only
```

## Environment knobs (proposals, pending user ruling — research R14; fail-closed validation at startup)

```text
TXHARBOR_WORKER_TTL_SECONDS=30            # lease TTL (proposal)
TXHARBOR_WORKER_HEARTBEAT_SECONDS=10      # TTL/3 with ±10% jitter (proposal)
TXHARBOR_WORKER_STALL_SECONDS=300         # 10×TTL, stall window (proposal)
TXHARBOR_WORKER_BACKOFF_BASE_MS=1000      # claim/step retry base (proposal)
TXHARBOR_WORKER_BACKOFF_MAX_MS=30000      # retry cap (proposal)
TXHARBOR_WORKER_SCAN_INTERVAL_MS=1000     # worker loop cadence (internal, not a business SLA)
TXHARBOR_WORKER_LABEL=<free text>         # evidence-only worker label
```

Validation: positive values; heartbeat < TTL; stall > TTL; no app-clock expiry anywhere. These defaults are
**not approved business thresholds**; tasks MUST record them as pending ruling until confirmed.

## Execution record

None. This step produced documentation only; no migration, code, container, or test was run. V1–V13 are
design-only and MUST be executed in tasks/implement. The 010/011 joint acceptance (A-13) and the production
provider selection (T000-P) remain OPEN.
