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
- `reorg_depth_vs_bound{chain_id}` — computed depth and bound (two gauges; alert on depth > bound).
- `reorg_frontier_lag{chain_id,stream}` — per stream: swept_end − frontier (block/log/deposit).
- `reorg_orphaned_total{chain_id}` / `reorg_revived_total{chain_id}` — conversion counters.
- `reorg_reconcile_required{chain_id}` — 1 while phase = reconcile_required.
- `reorg_evidence_wait_total{chain_id,class}` — evidence-insufficient waits by class
  (transport/timeout/rate-limit/parse vs contradictory/insufficient).
- Existing 002/003/004/005 metrics keep their meaning; during recovery their "stopped" state is
  expected (pause-gated), not an alert — runbooks must not page on zero-advance while
  `reorg_active == 1`.

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

- `txharbor_reorg_active == 1` → zero-advance on 002/003/004/005 is expected
  (pause-gated recovery), **non-paging**. Do not page on stopped
  002/003/004/005 scanners while active.
- `txharbor_reorg_reconcile_required == 1` → human attention. Paging follows
  operator policy; it is NOT automatic.
- `txharbor_reorg_depth > txharbor_reorg_bound` → alert (the alert rule lives
  here in the runbook; the two gauges carry depth and bound separately).
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
- Wiring note: the metrics surface + contract ship first. The recovery loop
  (`RecoveryExecutor` in `internal/indexer/reorg.go`) receives no
  `*metrics.Metrics` today, so loop wiring is unwired by design here — a
  007-era concern, and only with zero constructor churn.
