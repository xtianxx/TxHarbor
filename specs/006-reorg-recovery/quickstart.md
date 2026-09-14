# Quickstart: 006 Reorg Recovery validation guide

**Branch**: `006-reorg-recovery` | **Date**: 2026-09-14

Design-only validation plan. Nothing below is recorded as executed or passed. Each scenario maps
to spec acceptance (A#), FRs, and SCs. Levels: U = unit (no DB/chain), I = integration (real
PostgreSQL, controlled fake RPC), E = end-to-end (Anvil + real DB).

## Environment (pinned executable entries — no new make target, Makefile untouched)

- U: `make test` (= `go test -count=1 -timeout 5m ./...`, no Docker required).
- I/E: `make test-integration` (= `go test -tags integration -count=1 -timeout 20m ./...`, requires a
  Docker daemon). Tests self-provision their chain and DB via testcontainers — postgres:18
  (`startIndexerPostgres` precedent: lease/confirm-commit/auth integration files) and Anvil
  (foundry `v1.8.1`, `--host 0.0.0.0 --port 8545 --chain-id 31337` precedent:
  `logscan_integration_test.go:692-713`); manual equivalent is `docker compose up -d postgres anvil`
  per `compose.yaml` (postgres 18.6 + anvil, both on 127.0.0.1 only). New 006 files
  (`reorgcommit_test.go` for I, `reorg_recovery_integration_test.go` for E) ride this same entry with
  `//go:build integration` + the same helpers — the 005 precedent (`confirmation_integration_test.go`
  header: "Anvil is the real chain truth") is the pattern to copy, not a new harness to invent.
- Fault injection around the same entry: kill -9 at named phase points, proxy RPC failures/delays/
  contradictions, clock control via DB `now()` (never app clock), dual-executor launch, delayed
  worker release.
- Fail-exit: any non-zero `go test` exit fails the gate; `-count=1` defeats the test cache so CI
  always executes instead of reporting a cached pass.

## Scenario matrix (run order: U → I → E)

| # | Scenario | Level | Setup | Expected assertion |
|---|----------|-------|-------|--------------------|
| V1 | Depth math: boundaries, MaxInt64 tip, invalid configs | U | pure cases | exact `bound-ancestor`; closed bound; missing/0/neg/non-int/out-of-range refuse |
| V2 | Txn guard matrix per 006 txn | I | fault-injected snapshots (stale seq/phase/position) | each recheck independently refuses; rowcounts enforced |
| V3 | Shallow fork, Pending+Confirmed affected | E | Anvil fork; ancestor-side Confirmed exists | pause; Orphaned conversions; ancestor data intact; replay + reconfirm; full audit (A1/A2 → SC-01/02) |
| V4 | Fork shapes: empty / new deposits / re-mined (index+content variants) / re-canonicalized same hash; post-release bulk-orphan history vs live confirmation | E+I | four scripted forks; then release with large Orphaned history retained while new Pendings arrive | no omissions, no history merges; new identities; revive path Orphaned→Pending→reconfirm; zero new rows on revive; candidate selection never picks historical Orphaned (status filter); new Pendings confirm with zero per-tick stall (A2 → SC-02) |
| V5 | Skewed progress + empty ranges + scan starts | E | lagged checkpoints, late starts | min-point rollback; floors; empty ranges advance (A3 → SC-03) |
| V6 | Repeat trigger + dual executors + delayed old workers + demanded version interleaving | I+E | concurrent launch; delayed commit release; scripted capture-v → establish-v+1 → release → old batch commits with matching content; ordinary-first-then-establish order | single active instance; ≤1 effective advance; stale submits refused; demanded interleaving MUST refuse on version alone (content never consulted); legal pre-pause order still allowed (A4 → SC-04/05) |
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
