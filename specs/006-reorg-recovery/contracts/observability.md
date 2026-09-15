# Observability Contract: 006 Reorg Recovery

**Branch**: `006-reorg-recovery` | **Date**: 2026-09-14 | **Spec**: FR-22, SC-12, FR-18

Extends the 002/003/004 metric and diagnostic families (same naming shape, same redaction
boundary: heights/hashes/ranges/versions retained; secrets and unbounded raw responses never
emitted). No new metric system, no new log pipeline.

## State surface (durable, read at query time)

- Active `reorg_recovery` row: `phase`, `recovery_id`, `policy_seq`, `max_depth`,
  bound tip `(bound_old_number, bound_old_hash)`, ancestor `(ancestor_number, ancestor_hash)`,
  `new_tip_*` tracking, per-stream frontiers, `detected_at`/`updated_at`.
- History: `reorg_recovery_events` per-(recovery_id) lifecycle; `deposit_observation_transitions`
  per-observation status lineage; `deposit_pause_audit` unchanged.

## Metrics (counter/gauge names; labels include chain_id)

- `reorg_active{chain_id}` — 1 while a recovery row exists, else 0.
- `reorg_depth_vs_bound{chain_id}` — computed depth and bound (two gauges; alert on depth > bound only while the same chain's `reorg_active == 1`; with no active row the series keep last-known values and carry no live claim).
- `reorg_frontier_lag{chain_id,stream}` — per stream: swept_end − frontier (block/log/deposit).
- `reorg_orphaned_total{chain_id}` / `reorg_revived_total{chain_id}` — conversion counters.
- `reorg_reconcile_required{chain_id}` — 1 while phase = reconcile_required.
- `reorg_evidence_wait_total{chain_id,class}` — evidence-insufficient waits by class
  (transport/timeout/rate-limit/parse vs contradictory/insufficient).
- Existing 002/003/004/005 metrics keep their meaning; during recovery their
  "stopped" state is expected (pause-gated). Whether stopped work pages
  depends on the cause class in the Runbook below — never on `reorg_active`
  alone.

## Query validity annotation (FR-18; enforced by readers, not by writers)

Every affected-range answer carries `(recovery_state, validity)`:
- `recovery_state` ∈ {none, recovering, paused_reconcile, released}.
- `validity` ∈ {valid_unaffected, provisional_replaying, unknown_paused}.
- Rules: mixed views never labeled complete; no trusted data → explicit unavailable/unknown
  (never "always available"); Orphaned history never hidden; distinct source identities never
  merged by tx_hash in default views. Current status vs historical confirmation records always
  distinguishable (a revived Pending is never presented as still-Confirmed).

## Diagnostic evidence format

`detail`/evidence columns: `recovery=<id> phase=<phase> range=<a-b> old_tip=<h:hash>
ancestor=<h:hash> policy=<seq> version=<seq>` plus kind-specific refs. Key-value fragments,
same shape as 003 `detail.class` / 004 pause detail conventions.

## Runbook (T032)

Zero-advance on 002/003/004/005 while recovery is active is pause-gated
(expected), but whether it pages depends on the cause class below — never
on `reorg_active` alone.

- Caused wait or backoff (expected, non-paging): `txharbor_reorg_active == 1`,
  `txharbor_reorg_reconcile_required == 0`, `txharbor_reorg_evidence_wait_total`
  increasing under its cause class (`transport`/`timeout`/`rate-limited`/
  `invalid-response` retryable pass-through; `chain-mismatch`/
  `contradictory`/`insufficient` holds). Status: phase stays
  `detected`/`replaying`, frontier lags stable or shrinking. Reason: chain
  evidence not yet available or transient RPC failure. Steps: confirm the
  wait class in the counter labels, check serve logs for the matching
  `ancestor search chain read failed; retrying` / `suffix discontinuous;
  holding` line (redacted), verify RPC health; no operator action — the next
  tick re-walks the same range.
- Reconcile hold (needs a human): `txharbor_reorg_reconcile_required == 1`.
  Status: phase is `reconcile_required`, ordinary work stays paused
  independently. Reason: unrecoverable evidence (over-depth, ancestor
  unobtainable, exhausted history, contradictory RPC — see
  `reconcile_signaled` event detail for `cause=`/`searched=`/`evidence=`).
  Steps: read the `reorg_recovery_events` row for the recovery id
  (`reconcile_signaled` detail), follow the authorized repair/release path
  (operator + evidence + disposition, full re-verify before release); paging
  follows operator policy, it is NOT automatic.
- Execution error or unexplained sustained no-progress (investigate):
  `txharbor_reorg_active == 1`, `txharbor_reorg_reconcile_required == 0`,
  `txharbor_reorg_evidence_wait_total` flat, frontier lags frozen across
  ticks, depth/bound known. Status: phase and `updated_at` stop moving with
  no wait-class explanation. Reason: loop-side failure (tick errors, DB
  write refusal, stalled RPC) rather than evidence hold. Steps: check serve
  logs for `recovery tick failed; retrying` (phase + redacted error), read
  the `reorg_recovery` row (`phase`, `updated_at`, frontiers) to confirm the
  stall, verify DB/lease/RPC health, then escalate per operator policy.
- `txharbor_reorg_depth > txharbor_reorg_bound` fires only with an
  active-recovery guard on the same chain, e.g.
  `txharbor_reorg_depth > txharbor_reorg_bound and on(chain_id) txharbor_reorg_active == 1`
  (`and`, not `unless`: label matching is per same-`chain_id` series, and a
  missing active series or `active == 0` must suppress the alert, never pass
  it). The two gauges carry depth and bound separately; with no active row
  they keep last-known values (stale, not live) — same for `active` itself on
  a read failure (gauges untouched), so a flat alert input is diagnosed via
  serve logs (`recovery metrics read failed; keeping last observed`) plus the
  durable `reorg_recovery` row (`phase`, `updated_at`, frontiers), not via
  the alert condition alone. The active guard scopes the alert; it does not
  solve all staleness.
- Series mapping (contract → exposition, `txharbor_` prefix per repo shape):

  | Contract identity | Exposition name |
  |---|---|
  | `reorg_active` | `txharbor_reorg_active` |
  | `reorg_depth_vs_bound` | `txharbor_reorg_depth` + `txharbor_reorg_bound` |
  | `reorg_frontier_lag` | `txharbor_reorg_frontier_lag` |
  | `reorg_orphaned_total` | `txharbor_reorg_orphaned_total` |
  | `reorg_revived_total` | `txharbor_reorg_revived_total` |
  | `reorg_reconcile_required` | `txharbor_reorg_reconcile_required` |
  | `reorg_evidence_wait_total` | `txharbor_reorg_evidence_wait_total` |

- Redaction boundary (SC-12): heights/hashes/ranges/versions are retained in
  metric Help strings and diagnostic detail; secrets/credentials/raw unbounded
  responses are never emitted (funnel: `logx.Redact`).
- Wiring: counters ride the executor funnel (`RecoveryMetrics`, wired in
  `internal/app/serve.go` via `NewRecoveryLoopWithMetrics`); gauges ride the
  serve-side `recoveryObserver.observeMetrics` over `LoadRecoverySnapshot`
  (same registry served on `/metrics`).
