# Quickstart: 006 Reorg Recovery validation guide

**Branch**: `006-reorg-recovery` | **Date**: 2026-09-14

Design-only validation plan. Nothing below is recorded as executed or passed. Each scenario maps
to spec acceptance (A#), FRs, and SCs. Levels: U = unit (no DB/chain), I = integration (real
PostgreSQL, controlled fake RPC), E = end-to-end (Anvil + real DB).

## Environment

- `make test` (U), `make test-integration` (I, real PostgreSQL), Anvil E2E harness (E, deterministic
  chains + scripted forks). Fault injection: kill -9 at named points, proxy RPC failures/delays/
  contradictions, clock control via DB `now()` (never app clock), dual-executor launch, delayed
  worker release.

## Scenario matrix (run order: U → I → E)

| # | Scenario | Level | Setup | Expected assertion |
|---|----------|-------|-------|--------------------|
| V1 | Depth math: boundaries, MaxInt64 tip, invalid configs | U | pure cases | exact `bound-ancestor`; closed bound; missing/0/neg/non-int/out-of-range refuse |
| V2 | Txn guard matrix per 006 txn | I | fault-injected snapshots (stale seq/phase/position) | each recheck independently refuses; rowcounts enforced |
| V3 | Shallow fork, Pending+Confirmed affected | E | Anvil fork; ancestor-side Confirmed exists | pause; Orphaned conversions; ancestor data intact; replay + reconfirm; full audit (A1/A2 → SC-01/02) |
| V4 | Fork shapes: empty / new deposits / re-mined (index+content variants) / re-canonicalized same hash | E | four scripted forks | no漏 no-merge; new identities; revive path Orphaned→Pending→reconfirm; zero new rows on revive (A2 → SC-02) |
| V5 | Skewed progress + empty ranges + scan starts | E | lagged checkpoints, late starts | min-point rollback; floors; empty ranges advance (A3 → SC-03) |
| V6 | Repeat trigger + dual executors + delayed old workers | I+E | concurrent launch; delayed commit release | single active instance; ≤1 effective advance; stale submits refused (A4 → SC-04/05) |
| V7 | Confirm/policy-switch races; crash per phase; lost commit response | I+E | kill at each phase boundary; drop responses | old results rejected; resume from persisted phase; re-read-to-triage (A5/A6 → SC-06/07) |
| V8 | Re-fork mid-recovery; tip growth during recovery | E | second fork while replaying; advancing live tip | no false release; ancestor re-validation; completion ≠ live head (A7 → SC-08) |
| V9 | Boundary depth / over-deep / no ancestor | E | fork at bound, bound+1, history-pruned | bound recovers; others reconcile-paused, never chase head (A8 → SC-09) |
| V10 | RPC outage / lagging / contradictory | E | proxy faults | zero history revocations; zero releases until evidence suffices (A9 → SC-10) |
| V11 | Unauthorized release attempts; restart vs pause; independent pauses; stream-pause coexistence + complete-vs-pause race | I | forged operator calls; manual pause-row delete; pre-existing drift pause; stream pause landed mid-recovery; stream pause racing release | all refused; tamper backstop holds; independent pauses survive release; concurrent stream pause coexists (separate tables, no overwrite); release with live stream pause frees recovery cause only, ordinary work stays stopped (A10/A11 → SC-10/11) |
| V12 | Upstream write-path regression + migration + audit completeness | I | full 002–005 suites on migrated DB; transition-log cross-check | zero regressions; 005 data/meaning intact; every conversion traceable (A12 → SC-12) |

## Resume drill (E)

Kill -9 the recovery executor between each pair of phases; restart; assert: no redone committed
phase, no skipped phase, frontiers monotonic, exactly-once terminal release. Repeat for commit-
response loss: assert re-read-to-triage outcome matches persisted state in all three cases
(committed / uncommitted / unknown-at-crash).

## Pass definition (per scenario)

All assertions green at its level; the 100%/zero-count criteria from SC-01–SC-12 apply
as mapped above — any deviation is a failure, not a note.
