# Contracts: Persistence, Ordering, Retry & Failure Semantics — 011 Withdrawal Execution Worker

**Branch**: `011-withdrawal-executor` | **Date**: 2026-09-17 | **Spec**: [spec.md](../spec.md) | **Research**: [research.md](../research.md) R3–R10 | **Data model**: [data-model.md](../data-model.md)

011's durable truth lives entirely in PostgreSQL (constitution III). This contract fixes the ordering that
makes "no second intent", "no double execution", "unknown never becomes failure", and "old versions never
overwrite new" true, and defines the refusal/retry vocabulary. Gate reads: [gates.md](gates.md). 010 boundary:
[lifecycle.md](lifecycle.md).

## 1. Admission ordering (T-admit; FR-01/FR-02/FR-03)

1. Authenticate + fixed permission **before** any DB read (401/403 without touching the domain).
2. One transaction: gate-table `SHARE` → request row read → registry `FOR SHARE` → grant `FOR SHARE` →
   scope `FOR SHARE` → `INSERT payment_intents` → `INSERT execution_events('admitted')` →
   `INSERT request_status_projection`.
3. The intent row is durable **before** any 008 reservation can occur: reservation is initiated by 010 after
   the intent exists (OC-1/C1; 008 binds only existing intents). Ordering evidence is recorded in the joint
   acceptance (intent row exists before the binding row for the same `intent_id`).
4. Concurrent/repeated admission: insert-first; `23505` on `payment_intents_request_uniq` or
   `payment_intents_authorization_uniq` ⇒ rollback ⇒ re-read ⇒ classify by identity basis (same ⇒ recorded
   200; different ⇒ 409 conflict, zero writes).
5. Any gate failure ⇒ **zero intent rows** and a best-effort `admission_refused` event (007 audit precedent);
   Accepted is never treated as authorization (FR-01).

## 2. Step issue ordering (T-step-issue; FR-08/FR-11)

1. **Persist before call**: `execution_steps(issued)` with `step_id`, `action`, `lease_version`, `owner_id`,
   `recovery_version` is committed before `LifecycleAdvancer.Advance` is invoked. The partial unique index
   `execution_steps_open_uniq (intent_id) WHERE state='issued'` makes "one open step per intent" structural.
2. **No external call while holding locks**: the 010 call runs after the issue transaction commits; 011 holds
   no gate/claim lock across it (constitution VI/IX; 009 R6 rule).
3. **Converge after**: T-step-converge re-verifies the claim (`owner_id`, `lease_version`, `active`,
   unexpired) with `FOR UPDATE`; if the qualification changed, the old holder **cannot** converge — the new
   holder reconciles the open step (the send facts live in 010, not in 011's memory).
4. **Progress**: the issue and the convergence each advance `last_progress_at` (claim-fenced exact update);
   a renewal never does.

## 3. Claim / lease / takeover ordering (T-claim, T-renew; FR-04/FR-05/FR-15)

1. `Acquire` = one atomic CAS: `INSERT … ON CONFLICT (intent_id) DO UPDATE … WHERE <claimable> RETURNING
   lease_version`. Claimable = `state <> 'active'` OR `expires_at <= now()` OR `now() - last_progress_at >=
   stall_window`. Losers serialize on the row and re-evaluate the predicate against the winner's committed
   row (indexer `acquireSQL` proof, `internal/indexer/lease.go:49-64,146-164`).
2. `Renew` = lock-first short transaction: `SELECT … FOR UPDATE` → verify `owner_id`, `lease_version`,
   `state='active'`, `expires_at > now()` → extend `expires_at`, `last_heartbeat_at` → COMMIT. Zero rows or
   any mismatch ⇒ `ErrClaimLost`; the caller stops writing immediately and re-observes.
3. `Takeover after stall` = one transaction under the claim row `FOR UPDATE`: re-evaluate the stall predicate
   **under the lock** (no progress since `stall_window`), then `lease_version+1`, new owner, refreshed
   `expires_at`/`last_progress_at`, cleared `stall_flagged_at`, and a `taken_over` event carrying the prior
   owner/version/watermark. Two timelines are exhaustive: progress-write-first ⇒ predicate false, no
   takeover; takeover-first ⇒ progress write affects 0 rows and is fenced (R6).
4. `Operator revoke` = evidence-required, `expected_lease_version`-guarded single-row update (+ event + audit);
   version mismatch refuses with zero writes; it never "chases" a newer version.
5. **No grace periods, no TTL extensions, no TTL on the fence**: expiry/revocation/version mismatch have the
   same refusal effect at 010's send gate (J4).

## 4. Retry determinism

- Retrying a call with the same `step_id` converges on the same 010 attempt/replacement identity (010 owns
  the convergence; [lifecycle.md](lifecycle.md) §4). 011 never generates a new `step_id` for the same
  logical step.
- Restart determinism: 011 rebuilds execution position from `execution_steps` + `execution_events` + 010's
  `LifecycleFacts`; no in-memory state is authoritative. On restart: reconcile every open (`issued`) step
  first, then resume.
- Bounded retries only (proposal: ≤3 attempts per cycle with exponential backoff + jitter; retryable and
  non-retryable classes are distinguished, never collapsed). Exhausted retries leave the step `issued` and
  defer to the next reconcile cycle — never a failure or "not paid" claim.

## 5. Refusal taxonomy (machine classes)

| Class | Trigger | Retryable | Effect |
|---|---|---|---|
| `execution_permission_denied` | missing/false `can_execute` | no | 403, zero writes |
| `authorization_invalid` | grant absent/mismatched/not active | yes (after re-observation) | no intent (admission) / step refused |
| `authorization_expired` | DB clock past `expires_at` | no (until re-supplied) | same |
| `authorization_revoked` | grant `state='revoked'` | no | same |
| `authorization_unverifiable` | scope row absent/unreadable | no | fail closed (PB Q-B) |
| `authorization_changed` | scope version/sender changed after admission | yes (re-establish basis) | step refused |
| `scope_mismatch` | scope identity/request/sender mismatch | no | 422/refused |
| `sender_unregistered` | registry row missing/disabled | no | 422/refused |
| `recovery_paused` / `recovery_active` | 006 pause rows / active recovery | yes (later) | no send; unknown stays reconcile |
| `recovery_version_changed` | version moved between observation and 010 build/send | yes (re-observe) | 010 `refused_basis`, step converged |
| `gate_read_failed` | lock wait/timeout/statement error | yes | fail closed, zero send |
| `binding_paused` / `binding_absent` / `binding_conflict` / `binding_terminal` / `binding_read_failed` | 008 observation classes | per class | no send; paused/read-failed retryable via reconcile |
| `claim_lost` / `claim_not_current` | claim row missing/stale/owner or version mismatch | yes (re-claim) | write/send refused; the actor stops |
| `step_open_unreconciled` | a new step requested while an `issued` step exists | yes (after reconcile) | zero writes |
| `lifecycle_unavailable` | 010 reader/advancer unavailable | yes (bounded) | no failure claim; step stays issued/unknown |
| `operation_conflict` | operator `operation_id` reused with different input | no | zero writes |

Classes are recorded on the step/event/audit row with identity + versions as evidence; refusals never claim
"definitely not sent" when the outcome is unknown.

## 6. Unknown-outcome reconciliation (FR-08/FR-10, C9)

1. `pending_unknown`/`reconcile_required` (or a lost response where 010 cannot confirm) ⇒ step `unknown`,
   intent `reconciling`, `reconcile_observed` event; **no** `failed`, **no** new intent, **no** new binding.
2. Reconcile loop: read `LifecycleFacts`; then (a) converged/refused with positive evidence ⇒ converge the
   step and continue under current gates; (b) still unknown ⇒ record observation, stay `reconciling`;
   (c) reader unavailable ⇒ mark evidence, retry with backoff.
3. A new send step is only possible after the open step is converged to a terminal state; unknown is never
   silently converted to "not sent" or "failed" (Q3).
4. Reorg revision (fact-only): applied by version-monotonic consumption; no authorization consumed; no claim
   needed; the original intent and history are preserved (M2).
5. Three facts stay distinct (mandatory): (i) confirmed-not-sent this attempt (no send result; business
   effect `unknown` pending reconcile); (ii) a previously-`unknown` business effect persisted pending
   reconcile; (iii) known RPC/on-chain results preserved and never rewritten. A failed COMMIT MUST NOT
   mechanically rewrite everything to `unknown`; recovery bases are persistable evidence only.

## 7. Projection & revision ordering (FR-09/FR-10, J5)

1. Two input streams, each version-monotonic: `state_version` (011) and `lifecycle_version` (010). Apply only
   if the incoming version is greater; older arrivals affect zero rows and are ignored (never overwrite newer).
2. Projection rows reference attempt identity; they never copy transaction facts (hashes, receipts, fees).
3. On a failed/absent authority read: `freshness='possibly_stale'` with `stale_since`; display must surface
   it; a successful read at a version ≥ stored clears it (`confirmed`). Stale never blocks or permits any
   execution decision — decision paths do not read the projection at all.
4. Forced refresh is an audited operator operation (api.md §3); periodic consumption and startup catch-up are
   the automatic path. No numeric display SLA is promised (C11); the mechanism ensures staleness is always
   visible and always recoverable.

## 8. Crash points (restart determinism)

| Crash point | Durable state | Recovery |
|---|---|---|
| before T-admit commit | no intent | safe retry (converges) |
| after T-admit commit | intent + evidence | replay returns recorded 200 |
| before T-step-issue commit | no step | no external side effect; safe retry |
| after step issue, before/during 010 call | open `issued` step | reconcile first; converge via `(intent_id, step_id)`; re-issue only with positive "no attempt" evidence |
| after 010 returns, before T-step-converge | open `issued` step | reconcile; converge with 010's facts |
| T-step-converge mid-transaction | atomic rollback/commit | reconcile or converge |
| after converge | terminal step | continue next step under current claim |
| worker death (no crash handling) | lease expires (DB clock) | new claimant takes over; open steps reconciled |
| DB session loss with network progress | unknown | reconcile; never success/failure fabricated |

Three facts stay distinct across every crash point (mandatory): a no-send-result abort is recorded as
no-send with the business effect `unknown` pending reconcile; a prior `unknown` stays persisted pending
reconcile; known RPC/on-chain results are preserved and never rewritten. Recovery bases are persistable
evidence only — never inference.

## 9. Logging, metrics, secrecy (FR-13; constitution XII)

- Structured fields per event: `request_id, intent_id, caller_id, sender, nonce (008 绑定引用，观测时), chain_id,
  owner_id, lease_version, step_id, action, attempt_id, tx_hash, authorization_id, scope_version,
  recovery_version, revision_version, outcome_class, operator` (audit paths only where applicable).
- Metrics on the existing registry: claim acquisitions/takeovers/revocations, stall flags, open steps, unknown
  pending, reconcile outcomes, gate refusals by class, projection staleness, advance latency/error classes.
- Never logged/persisted: credentials, keys, raw signed bytes, raw transaction bytes, authorization secrets.
  Amounts/fees are integer decimal strings only; no floats anywhere (constitution I/XII).
- Every mutation carries evidence (identity + versions + class); audit rows are append-only.
