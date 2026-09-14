# Tasks: 006 Reorg Recovery

**Input**: Design documents from `/specs/006-reorg-recovery/` (baseline `e06222f`, branch `006-reorg-recovery`, base `origin/main` `a73e31b` PR #7)

**Prerequisites**: plan.md, spec.md (Q1–Q4 integrated, zero NEEDS CLARIFICATION), research.md (R1–R14), data-model.md (Tables 1–4 + txn catalog), contracts/ (observability.md, downstream.md), quickstart.md (V1–V12 + resume drill, design only)

**Tests**: Verification tasks are explicitly required (spec SC-01–SC-12 use 100%/zero-count assertions; quickstart V1–V12 maps each scenario to a level). Every verification task below states its level (U/I/E), setup, and pass assertion. Nothing in this file is recorded as executed or passed — all boxes stay unchecked until future implementation runs them.

**Organization**: Phase 1 = pre-implementation evidence (FR-25, T000-P separation). Phase 2 = foundational (migration + fencing + ordinary-path gates; blocks all stories). Phases 3–8 = US1–US6 in spec priority order. Final phase = cross-cutting (observability, downstream contract check, full regression, CI/runbook).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks). Deliberately withheld from same-write-path tasks (T013–T016) and shared-harness E2E tasks even when files differ — see Dependencies.
- **[Story]**: US1–US6 per spec.md. Phase 1/2/Final carry no story label.
- Each task cites its FR/SC/V mapping, dependency, and done-condition. A task is done only when its stated acceptance holds; any deviation is a failure, not a note (quickstart pass definition).

## Extension Hooks

**Optional Pre-Hook**: git
Command: `/speckit.git.commit`
Description: Auto-commit before task generation
Prompt: Commit outstanding changes before task generation?
To execute: `/speckit.git.commit`
Disposition: displayed only, NOT executed. Worktree at generation time holds only the pre-existing `M .gitignore` (worktree ignores) and `?? .omo/` (session state) — both excluded from this step's scope; there is no committable 006 content predating tasks.md.

---

## Phase 1: Setup & Pre-implementation Evidence (FR-25, T000-P)

**Purpose**: FR-25 mandates 005 evidence verification BEFORE 006 touches 005-owned paths. T000-P stays a separate open gate. No code changes in this phase; outputs are evidence records (present/missing + source). Missing evidence blocks dependent tasks but does not rewrite the plan.

- [x] T001 Verify 005 merge baseline and artifact presence in `git log` (ancestry must contain `a73e31b` PR #7), `specs/005-confirmation-tracking/` (spec/plan/tasks), and `migrations/000005_confirmation_tracking.sql` (FR-25; gates Phase 2 start — no Phase 2 task starts until this ledger records 005 + 002–004 spec artifacts present, missing items named as blockers; feeds T002; done when: present/missing recorded with exact refs, missing items named as blockers, no 005 file modified)
- [x] T002 Verify 005 regression / local-acceptance / remote-CI evidence from `internal/indexer/confirmation_*_test.go`, `internal/indexer/confirmcommit_test.go`, `internal/indexer/confirmauth_integration_test.go`, and CI records (FR-25; depends on T001; done when: each evidence source listed as verified-present or missing-with-handling — missing handling = the 005-behavior tasks T016/T034 stay blocked (plus story phases via unclosed Phase 2 — see Dependencies), NOT silently unblocked; nothing executed or passed here)
- [x] T003 Record T000-P production-gate separation against `specs/005-confirmation-tracking/spec.md` assumptions (FR-25, plan.md Evidence separation; depends on T001; done when: documented that T000-P open neither proves nor disproves local acceptance, production readiness stays out of 006 scope, and no task conflates T000-P with local preconditions)
- [x] T004 [P] Verify validation-environment availability per `quickstart.md` Environment + `Makefile` (`make test`, `make test-integration` on real PostgreSQL, Anvil E2E harness, fault-injection means: kill -9 points, proxy RPC faults, DB-`now()` clock control, dual-executor launch, delayed worker release) (supports V1–V12; independent of T001–T003, different surface; done when: pinned entries recorded available — U `make test`, I/E `make test-integration` (Docker daemon; testcontainers self-provision postgres:18 + Anvil foundry v1.8.1 chain-31337 and never consume the fixed endpoints of a manual compose stack); `docker compose up -d postgres anvil` is a manual-debug aid only, NOT an automated-test equivalent entry); execution itself is future work)
- [x] T005 [P] Verify migration baseline `migrations/000001_baseline.sql` through `migrations/000005_confirmation_tracking.sql` plus auto-include in `migrations/embed.go` (`*.sql`) for the future `000006` file (R13 privilege/order inputs; independent of T001–T004; done when: sequence, migrate role, and embed pattern confirmed in writing)

**Checkpoint**: Evidence ledger complete (T001–T005). If T002 reports missing 005 evidence, 005-behavior tasks remain blocked — proceed with the startable set (T006–T015, T017–T019) but NOT T016/T034; story phases wait while Phase 2 stays unclosed (see Dependencies).

---

## Phase 2: Foundational — Migration, Fencing, Ordinary-Path Gates (BLOCKING)

**Purpose**: Storage + version discipline + old-path gates. MUST be complete before ANY user-story phase.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete. T006 (migration) blocks every schema-dependent task. T011 (006 txn catalog) blocks every gate/verification task. T013–T016 share one fencing discipline and are intentionally NOT marked [P] — they must land as one reviewable unit even though files differ.

- [x] T006 Write `migrations/000006_reorg_recovery.sql` (data-model.md Tables 1–4, R2, R13; depends on T005; done when: pre-asserts first in-txn — single canonical per height, zero observations outside `{pending,confirmed}`; CREATE unique index on `(chain_id, number, hash)` → DROP old PK → ADD PK `(chain_id, number, hash)` USING index → partial `UNIQUE (chain_id, number) WHERE canonical` → validated (not `NOT VALID`) CHECK rewrites → 4 new tables + orphan-evidence columns + indexes;   `goose up` clean on fresh DB, statement failure rolls back whole txn, re-run safe; `migrations/000002`–`000005` untouched in meaning; Down section follows repo convention (reverse dependency order, cf. 000005 Down) as authoring hygiene only — its bounds are T007's, not a production rollback path)
- [x] T007 Verify migration behavior against `migrations/000006_reorg_recovery.sql` (R13 order/locks/outage, V12-migration; depends on T006; done when: dirty-data pre-assert refusal shown; 005 rows/columns intact in meaning — seeded 005 pending/confirmed rows survive the migration on a scratch DB with status/basis untouched; second-canonical-row insert refused by the partial unique on a scratch DB; bidirectional fork-identity probe on a scratch DB — `(h1, canonical=true)` + `(h2, canonical=false)` at the same chain+height co-insert OK, then `UPDATE h2 SET canonical=true` MUST be refused by the partial unique with both rows' pre-probe state intact (PK-rework self-verification, not deferred to Batch C); `ACCESS EXCLUSIVE` window + deploy-freeze + old-binaries-unrunnable (stale `ON CONFLICT (chain_id, number)` target) + no-mixed-fleet documented as the outage boundary; Down exercised ONLY on scratch DBs without fork history during authoring — downgrade after fork-history rows exist is unsupported (it would destroy audit evidence) and MUST NOT be described as lossless; production rollback = code + DB restore point per R13)
- [x] T008 Retarget 002 insert path in `internal/indexer/scanner.go` (R2 full linkage; FR-01 fork-evidence trigger; depends on T006; done when: upsert target is `(chain_id, number, hash)` — same number+hash idempotent `DO NOTHING`, same number + different hash inserts sibling row and routes fork-evidence pause on the ordinary path vs 006-owned rows under active recovery; existing `hash_mismatch` pause path preserved; Batch A verification = 002 insert-path smoke subset from `scan_integration_test.go` (first-scan order, restart, duplicate-delivery/rescan idempotency, parent-break pause) on the migrated DB — zero behavior change outside fork evidence; sibling-row minting itself is new behavior verified in Batch C (T020/T022), stated here not deferred silently)
- [x] T009 [P] Implement depth-config versions in `internal/indexer/reorgpolicy.go` (new file; R4, Table 3, Q1-change; depends on T006; done when: bootstrap/first-row, `expected_old_seq` gate, new≠old, single INSERT, `request_id` idempotency, failure atomic; startup env-vs-effective drift loud-refuses (005 precedent); in-flight recoveries bind captured `policy_seq`; limit raise never releases a reconcile pause; Q1 invalid configs — missing/zero/negative/non-integer/out-of-representation — refuse startup)
- [ ] T010 Implement recovery executor + read-only ancestor search in `internal/indexer/reorg.go` (new file; R5, OQ1-drive, Q1 depth anchor; depends on T006, T011 — driver calls T011's txn functions, so T011 executes first inside Batch B; NOT [P]; done when: phase machine drives `detected → ancestor_confirmed → invalidated → replaying → complete_pending → (DELETE + terminal event)`, any phase → `reconcile_required`; search is read-only/off-lock from `bound_old_tip` with `depth = bound_old_tip − ancestor` (`ancestor ≤ bound_old_tip` asserted), closed bound `depth ≤ max_depth`, dual local+chain height+hash equality + both-side parent continuity + new-chain suffix continuity; 003 failure taxonomy reused — retryable waits bounded, contradictory/insufficient evidence waits and never invalidates; local-exhausted / below-scan-start / beyond-bound → reconcile signal, stay paused; bound tip fixed at establish, live tip tracked separately, completion never means catching the live head; timing reuses `newBackoff` + the deposit default triple (poll 1s / initial 200ms / max 30s, loopTimings clamp, startup-only) per the research R5 timing table — no new knob names; executes AFTER T011 in Batch B (driver calls T011's txn functions) and compiles against it in-batch, so no forward reference escapes the batch)
- [ ] T011 Implement 006 transaction catalog in `internal/indexer/reorgcommit.go` (new file; R1 atomicity, R6 5-step shape, R7 sweep rule, R9 no-pause-writes, data-model §Transaction catalog; depends on T006, T009 (policy bind needs the policy code); executes BEFORE T010 in Batch B (T010 calls these functions); done when: all 11 txns follow BEGIN → ensure lease → `lockCoordSQL FOR UPDATE` → independent post-lock rechecks → writes + `RowsAffected` checks → COMMIT with `SET LOCAL statement_timeout='5s'` and zero RPC inside; `establish` atomically writes recovery row (seq = events-MAX+1 asserted `< MaxInt64`, else refuse, never wrap/reuse) + `established` event and writes NO pause table; `confirm_ancestor` recomputes depth ≤ bound; invalidations sweep live range `[ancestor+1, max(persisted_tip_at_sweep)]` (never establish-cached); rollbacks use exact-guard `$to = max(ancestor+1, start)`; `replay_range` is idempotent per-stream with frontier advance in-txn; `revive_observation` enforces the Q4 six rules; `complete_reverify`/`auth_release` re-read ALL FR-19判据, DELETE only the recovery row, and carry mandatory terminal detail — bound tip, policy_seq, ancestor, swept ranges, disposition list, surviving stream pauses; programmatic reconcile writes stay version+permission gated and can never trigger a paused external action or skip the gate; replay batching follows the research R5 timing table (one frontier-advance txn per stream range, 5s writeGuard bound, chosen row cap recorded in code))
- [ ] T012 [P] Implement validity-annotated readers in `internal/indexer/reorgquery.go` (new file; R11, FR-18, contracts/observability.md; depends on T006; done when: no active row → current answers; active row → every affected-range answer carries `(recovery_state ∈ {none, recovering, paused_reconcile, released}, validity ∈ {valid_unaffected, provisional_replaying, unknown_paused})`; mixed views never labeled complete; no trusted data → explicit unavailable/unknown; Orphaned history never hidden; default views never merge distinct source identities by `tx_hash`; revived Pending never presented as still-Confirmed)
- [ ] T013 Add triple recheck + loop gating to 002 commit path in `internal/indexer/scanner.go` — `commitBlock` plus `loadProgress`/loop gating (R1 authoritative gate, R7 post-release isolation; depends on T008 — same file — and T011; NOT [P]; done when: loop never starts a batch under an active recovery row; version captured BEFORE batch inputs; commit rechecks post-lock (a) no active non-terminal row, (b) captured == current (single-statement read: active seq else events MAX else 0), (c) existing verdicts/guards; (a)/(b) mismatch refuses the whole batch with zero writes and zero progress even on full content coincidence; recomputation starts a new batch, never re-labels old results; 006-owned txns gate on captured seq+phase via enumerated paths only — no bypass flag exists for ordinary callers)
- [ ] T014 Add triple recheck + loop gating to 003 commit path in `internal/indexer/logscanner.go` — `commitLogRange` plus `loadLogState`/loop gating (same gate discipline as T013; depends on T011; NOT [P] — one reviewable fencing unit with T013/T015/T016; done when: same capture/commit-triple/refuse-whole-batch semantics as T013 against `log_checkpoint` + canonical re-adjudication)
- [ ] T015 Add triple recheck + 006-owned re-read to 004 commit path in `internal/indexer/depositcommit.go` — `commitDepositUnit` (same gate discipline; R8 predicate update; depends on T011; NOT [P]; done when: same capture/commit-triple/refuse semantics; ordinary `storedStatus != 'pending'` conflict rule UNCHANGED; only 006's own re-reads exempt rows under the captured recovery version — ordinary 004 path stays stopped during recovery)
- [ ] T016 Add triple recheck + orphaned routing to 005 commit path in `internal/indexer/confirmcommit.go` (+ candidate scan in `internal/indexer/confirmscan.go`) — `ConfirmDepositUnit` (same gate discipline; R8 field semantics; depends on T002-blocked-on-missing-evidence, T011; NOT [P]; done when: same capture/commit-triple/refuse semantics; orphaned rows route to ChainViewError-stop (behavior unchanged, now reachable); `status` — never column non-NULLness — keys effectiveness; reconfirmation overwrites confirm_* with the NEW basis while the old survives only in transition rows; 005 policy/data meaning untouched)
- [ ] T017 Wire recovery loop + gating readers in `internal/indexer/coordinator.go` (reuse `lease.go` unchanged; R1, R6; depends on T010, T011; done when: recovery loop rides the existing RunQuatro discipline with no new lock order; startup/loop/health readers observe the recovery row the same way they read pause rows today; `lease.go` needs zero changes — asserted, not assumed)
- [ ] T018 [P] Unit tests for depth math and config refusal in `internal/indexer/reorgpolicy_test.go` (new file; V1; depends on T009; done when: exact `bound − ancestor`, closed-boundary allow/refuse, MaxInt64-tip arithmetic, missing/zero/negative/non-integer/out-of-representation refusal, no saturation/clamping value ever written to audit — all green via `make test`)
- [ ] T019 Guard-matrix integration tests in `internal/indexer/reorgcommit_test.go` (new file with legal `//go:build integration` header, same shape as the 005 integration-test precedent; V2; depends on T011; done when: each 006 txn's rechecks (L/P/V/G, stale seq/phase/position fault snapshots) independently refuse with rowcounts enforced on real PostgreSQL via `make test-integration`; seq-exhaustion snapshot: events MAX preset to 9223372036854775806 (MaxInt64−1, data-model Table 1 boundary) → next `establish` refuses with zero recovery row, zero event row, zero frontier movement — no wrap, no reuse; bounded-wait behavior under injected faults uses the capped backoff from the research R5 timing table, never tight-loops)

**Checkpoint**: Foundation ready (T006–T019 green). Migration + fencing + gates proven before any story phase starts.

---

## Phase 3: User Story 1 — Shallow Reorg Pause, Invalidate, Replay (Priority: P1) 🎯 MVP

**Goal**: Proven shallow fork affecting Pending + Confirmed closes the full pause → orphan → replay → reconfirm loop with ancestor-side data intact.

**Independent Test**: V3 (E) — Anvil fork with affected Pending P + affected Confirmed C + ancestor-side Confirmed K: pause lands, P/C → Orphaned, K untouched with basis intact, post-ancestor replay regenerates + reconfirms, full audit traceable (→ SC-01/SC-02).

- [ ] T020 [US1] Anvil E2E shallow-fork close-loop in `internal/indexer/reorg_recovery_integration_test.go` (new file with legal `//go:build integration` header, same shape as the 005 integration-test precedent; FR-01/02/05/06/07/09/10/19, V3; depends on Phase 2; done when: establish+pause atomic and restart-persistent; affected Pending+Confirmed 100% → Orphaned with evidence audit, ancestor-side rows 100% untouched; replay along historical semantics regenerates observations; reconfirmation meets ALL 005 conditions with zero old-basis reuse; auto-release only after re-verified FR-19判据; SC-01 assertions green)
- [ ] T021 [US1] Crash-resume spot check inside the US1 loop in `internal/indexer/reorg_recovery_integration_test.go` (FR-12/13, V7-part, resume drill; depends on T020; NOT [P] — same harness/file; done when: kill -9 between US1 phases restarts from persisted phase + frontiers with no redone committed phase and no skipped phase; one lost-commit-response case triages via re-read to the persisted outcome)

**Checkpoint**: US1 independently functional (MVP = Phase 1 + Phase 2 + US1). Stories US2+ remain unstarted.

---

## Phase 4: User Story 2 — Fork Shapes, Re-mine Identity, Same-Hash Revival (Priority: P1)

**Goal**: Every fork shape handled without omission or history merge; same-hash re-canonicalization revives in place through Pending.

**Independent Test**: V4 (E+I) — four scripted forks (empty / new deposits / re-mined with index+content variants / re-canonicalized same hash) then release with bulk Orphaned history retained while new Pendings arrive: no omissions, no merges, new identities, revive path Orphaned→Pending→reconfirm, zero new rows on revive, candidate selection never picks historical Orphaned, new Pendings confirm with zero per-tick stall (→ SC-02).

- [ ] T022 [US2] Fork-shape E2E in `internal/indexer/reorg_recovery_integration_test.go` (FR-08/09/10/11, V4-shape; depends on T020; NOT [P] — shared harness; done when: empty range advances legitimately with zero observations; new-deposit fork mints new-identity observations; re-mined tx (index change, content change) mints new-identity rows linked to the same tx history line while old rows stay Orphaned unmerged; same-hash re-canonicalization reuses the original row (zero new observations), Orphaned→Pending only via `revive_observation` with re-verified block/log-binding/history semantics, ex-Confirmed passes through Pending for 005 reconfirmation, every transition audited with version/basis/time, repeat execution converts exactly once, old workers cannot overwrite new state)
- [ ] T023 [US2] Post-release bulk-orphan non-blocking proof in `internal/indexer/reorgcommit_test.go` (R8 proof, V4-tail; depends on T016, T022; done when: `status='pending'` filter ⇒ historical Orphaned never selected; selected-then-orphaned halts exactly one tick via commit-time re-read then clears; genuine anomalies still stop-not-skip; new Pendings confirm alongside arbitrary Orphaned history with zero structural stall)

**Checkpoint**: US1 + US2 both independently functional (SC-02 green end to end).

---

## Phase 5: User Story 3 — Skewed Progress, Scan Starts, Empty Ranges (Priority: P1)

**Goal**: Mixed stream progress rolls back to the true minimum affected point; late starts and empty ranges replay correctly.

**Independent Test**: V5 (E) — lagged block/log/deposit checkpoints, scan start later than ancestor, legitimately-empty log interval: min-point rollback, start floors, empty advance (→ SC-03).

- [ ] T024 [US3] Skew/start/empty E2E in `internal/indexer/reorg_recovery_integration_test.go` (FR-04/11, V5; depends on T020; NOT [P] — shared harness; done when: log-behind-block and deposit-behind-log each roll back to the minimum affected point with full failed-interval coverage and no forward jump of unprocessed positions; ancestor-before-start floors each stream at its own start with no backfill demanded; empty ranges advance progress with zero observations and no gap markers; block/log/deposit `CHECK (next_block ≥ start_block)` preserved without DDL change)

**Checkpoint**: US1–US3 independently functional (SC-03 green).

---

## Phase 6: User Story 4 — Repeat Triggers, Concurrency, Stale Workers (Priority: P1)

**Goal**: One persistent instance, at most one effective advance, stale results refused — including after release when content coincides.

**Independent Test**: V6 (I+E) — repeat triggers, dual executors, delayed old workers, scripted capture-v → establish-v+1 → release → stale commit with matching content, ordinary-first-then-establish order (→ SC-04/SC-05).

- [ ] T025 [US4] Concurrency convergence tests in `internal/indexer/reorgcommit_test.go` (FR-12/14/15, V6-converge; depends on T019; done when: 2+ repeat triggers converge to exactly 1 persistent instance with no phase rollback/fork; dual executors on one position yield ≤ 1 effective advance with the loser failing safe and re-reading; SC-04 green)
- [ ] T026 [US4] Stale-worker + demanded-interleaving tests in `internal/indexer/reorgcommit_test.go` + Anvil delay run (FR-14/20, R1 I-A/I-B, R7 Class 1–3, V6-isolation; depends on T013–T016, T025; NOT [P] — same gate discipline under test; done when: delayed old scan/identify/confirm workers refused 100% post-version-change with zero expired results; demanded interleaving — capture v=5 → establish seq 6 → release → stale commit with fully matching content — REFUSED on version alone, content never consulted; preserved legal order — pre-pause commit then establish — still allowed; Class-1 pre-pause stragglers covered only by audited sweep cleanup; post-pause submits proven impossible-by-construction, never merely swept)

**Checkpoint**: US1–US4 independently functional (SC-04/SC-05 green). Fencing proven, not asserted.

---

## Phase 7: User Story 5 — Policy/Submit Races, Re-fork, Crash Resume (Priority: P2)

**Goal**: Version races refuse stale results; mid-recovery re-fork never falsely releases; every phase survives kill -9.

**Independent Test**: V7 (I+E) + V8 (E) — confirm-submit vs establish race, policy-switch vs recovery race, kill at each phase boundary, dropped responses, second fork mid-replay, advancing live tip (→ SC-06/SC-07/SC-08).

- [ ] T027 [US5] Version-race tests in `internal/indexer/reorgcommit_test.go` (FR-20, V7-race; depends on T016, T026; done when: post-establish old confirm commits succeed 0%; post-switch old-policy results succeed 0% with the policy version chain unmodified by recovery; submit txns hold the lock while re-reading and roll back on any mismatch)
- [ ] T028 [US5] Crash drill + re-fork E2E in `internal/indexer/reorg_recovery_integration_test.go` (FR-12/13/16/19, V7-crash + V8; depends on T020, T027; NOT [P] — shared harness; done when: kill -9 between EVERY phase pair resumes with exactly-once terminal release and monotonic frontiers; commit-response loss triages correctly in all three persisted outcomes — committed / uncommitted / unknown-at-crash; mid-replay second fork never falsely releases, re-validates the ancestor, and completes against the bound tip + swept range, never the live head)

**Checkpoint**: US1–US5 independently functional (SC-06/SC-07/SC-08 green).

---

## Phase 8: User Story 6 — Deep/Unprovable Forks Hold for Reconciliation (Priority: P2)

**Goal**: Beyond evidence, the system stops and waits for authorized humans — never chases the head, never clears чужой pauses.

**Independent Test**: V9 (E) + V10 (E) + V11 (I) — bound / bound+1 / pruned-history forks; dead/lagging/contradictory RPC; forged operators, manual pause-row delete, pre-existing drift pause, mid-recovery stream pause, pause-racing-release (→ SC-09/SC-10/SC-11).

- [ ] T029 [US6] Depth-boundary + evidence-insufficiency E2E in `internal/indexer/reorg_recovery_integration_test.go` (FR-03/04, Q1-boundary, V9 + V10; depends on T020; NOT [P] — shared harness; done when: bound-depth fork recovers 100%, bound+1 goes reconcile-paused 100%, ancestor-unobtainable holds with no head chase; dead/lagging/contradictory RPC yields zero history revocations and zero releases until evidence suffices; reconcile signal carries searched range + both tip evidences + cause class)
- [ ] T030 [US6] Manual-path two-step authorization tests in `internal/indexer/reorgcommit_test.go` (FR-23 Q2b, V11-auth; depends on T011; done when: `auth_enter_repair` (privileged, Q2b minimum evidence + full disposition list — 定性 per-record traceable, NOT re-Confirmed-everything) never releases confirm/sign/broadcast pauses; `auth_release` passes only with re-verified completion, records operator/time/evidence/cause, stays atomic-or-nothing, re-verifies after reboot, never invents payment intents for unknown withdrawal outcomes; forged/unauthorized attempts refused 100%)
- [ ] T031 [US6] Pause-coexistence + tamper-backstop tests in `internal/indexer/reorgcommit_test.go` (FR-17/23, R9, V11-coexist; depends on T013–T016, T030; NOT [P] — same gate discipline; done when: release deletes ONLY the recovery row — drift/data-anomaly pauses survive and keep ordinary work stopped (survivors in terminal event); concurrent stream pause coexists via separate tables with no overwrite; release racing a fresh stream pause frees the recovery cause only; manual pause-row delete cannot smuggle ordinary commits past the recovery-row gate — tamper backstop holds)

**Checkpoint**: All six stories independently functional (SC-09/SC-10/SC-11 green).

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Observability, downstream contract check (006-side only — 007–011 create NOTHING here), full upstream regression, CI + runbook. No future withdrawal tables, services, or state machines.

- [ ] T032 Extend existing metric/diagnostic families + runbook note in `internal/indexer/` metrics surface and `specs/006-reorg-recovery/contracts/observability.md` consumers (`reorg_active`, depth-vs-bound, per-stream frontier lag, orphaned/revived counters, reconcile flag, evidence-wait counters; same naming shape + redaction boundary; depends on T012, T031; done when: V11-runbook — zero-advance while `reorg_active == 1` is expected and non-paging; diagnostics carry heights/hashes/ranges/versions with zero secret/plaintext-credential occurrences — SC-12 redaction assertion green)
- [ ] T033 Verify downstream handoff check-obligations in `specs/006-reorg-recovery/contracts/downstream.md` (FR-26 matrix, Downstream Handoff; depends on T031; done when: per-action preconditions — no stream pause rows + no active recovery row (or released-terminal only) + own stage gates — recorded as 007–011 plan inputs; 007 receive-only, 010 unknown-retain-no-repay, 011 revise-not-recreate all carried WITHOUT creating any future business table/service/state machine in this feature — zero 007–011 implementation tasks exist in this file)
- [ ] T034 Run full 002–005 regression suites on the migrated DB in `internal/indexer/*_test.go`, `internal/indexer/*_integration_test.go` (`make test`, `make test-integration`) (V12-regression; depends on T002-evidence, T006–T008, T013–T016; BLOCKED while T002 reports missing 005 evidence; done when: zero regressions, 005 data/meaning intact, ordinary-path gates introduce no behavior change outside active recovery)
- [ ] T035 Audit-completeness cross-check from `reorg_recovery_events` + `deposit_observation_transitions` against every conversion in `internal/indexer/reorg_recovery_integration_test.go` runs (FR-07/21/22, V12-audit, SC-12; depends on T020, T022, T028; done when: every Orphaned conversion traceable to old source + old basis + recovery audit; every new confirmation traceable to new source + new basis; repeated Confirmed→Orphaned→Pending cycles fully preserved in the transition log)
- [ ] T036 Wire CI + record the recovery resume drill in CI config and `specs/006-reorg-recovery/quickstart.md` execution log (V1–V12 run order U → I → E, resume drill; depends on T018–T019, T020–T031; done when: CI runs unit → integration (real PG) → Anvil E2E with fault injection via the pinned quickstart Environment entries (no new make target, Makefile untouched); each scenario's 100%/zero-count assertion logged pass/fail — fails are failures, not notes; this task creates the execution record, it is NOT itself a pass claim)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (T001–T005)**: No code dependencies — starts immediately. T001 → T002 → (gates T016/T034). T003/T004/T005 independent ([P] on T004/T005).
- **Phase 2 (T006–T019)**: Depends on Phase 1 evidence ledger recorded (T001–T005 done). Single dependency rule (FR-25, no exceptions, no second reading): (i) **ledger recorded** = T001–T005 done — required before ANY Phase 2 task starts; (ii) **gate passed** = T002-verified, i.e. 005 regression/acceptance/CI evidence present — required to START only T016 (modifies 005-owned `confirmcommit.go`/`confirmscan.go`) and T034 (executes 005 suites); T015 is 004-owned (spec D3 verified) and does NOT require T002; (iii) **startable** = ledger recorded + listed deps done + gate satisfied where applicable. T006 blocks T007–T012, T015–T016 (schema). Inside Batch B, execution order is T011 → T010 (T010 calls T011's functions; IDs preserved, deps govern order) → T012–T019; T013–T016 sequential, no [P] (one fencing unit). Consequence of (ii): while T002 is missing, T016 stays blocked → Phase 2 cannot close → story phases wait as a whole (no per-story exceptions); the startable set is exactly {T006–T015, T017–T019}. Recording a missing item (i) is never scored as passing its gate (ii). **Checkpoint**: foundation green before stories.

> T002-GATE UPDATE 2026-09-14: gate PASSED — PR #7 checks green @`c71123d`, main run 34805751040 success @`a73e31b`, 005 T031 + review §(j), zero 005-file drift (see plan.md ledger). T016/T034 unblocked at gate level; story phases still wait on T010–T019 (Batch B).
- **Story phases (T020–T031)**: All depend on Phase 2. Run in priority order P1 (US1→US2→US3→US4) then P2 (US5→US6); parallel staffing allowed across stories ONLY with separate Anvil/DB instances — tasks sharing `reorg_recovery_integration_test.go` / `reorgcommit_test.go` harnesses are NOT marked [P].
- **Polish (T032–T036)**: Depends on stories as noted. T034 additionally blocked on T002-verified.

### Within-story order

- Migration/schema → transactions → gates → story E2E → cross-checks. Verification tasks run AFTER the code they verify; unit (V1) → integration (V2/V6/V7/V11) → E2E (V3/V4/V5/V8/V9/V10) per quickstart run order.

### Suggested implementation batches (design only — NOT executed in this step)

Each batch is independently verifiable and locally committable. Per-batch closeout: complete tasks → run applicable checks (`make test` / `make test-integration` / targeted Anvil) → inspect full diff → local commit → report and stop. Migration self-checks, write-path regression, and batch-correctness checks live INSIDE their batch — never deferred to a final综合 test.

- **Batch A — Evidence + migration + policy** (T001–T009): gate = evidence ledger + `goose up` clean on fresh AND scratch DBs + T007 scratch-DB probes (seeded 005 rows survive; second-canonical refused; bidirectional fork-identity probe: canonical+non-canonical co-insert OK, flip-to-second-canonical refused with state intact) + `go build ./...` + `make lint` + 002 insert-path smoke subset (`scan_integration_test.go`: order/restart/duplicate-delivery/parent-break) on the migrated DB + diff review. Explicitly NOT in this gate: `goose down` on any history-bearing DB (T007 bounds); V1 unit (lives in Batch B with T018). Tree compiles at close (no forward references escape). Commit, report, stop.
- **Batch B — Executor + fencing core** (T011 → T010 → T012–T019, deps govern order): gate = `go build ./...` + V1 (T018) + V2 guard matrix incl. exhaustion snapshot and bounded-wait behavior (T019) + 002–005 ordinary-path spot regression + diff review. Commit, report, stop. (Batch B must NOT claim the demanded interleaving — that proof lives in T026/Batch D.)
- **Batch C — Recovery flows** (T020–T024, US1–US3): gate = V3 + V4 + V5 Anvil runs + audit spot check. Commit, report, stop.
- **Batch D — Races + hold** (T025–T031, US4–US6): gate = V6 (incl. demanded interleaving REFUSED + legal order allowed) + V7 + V8 + V9 + V10 + V11. Commit, report, stop.
- **Batch E — Cross-cutting + full validation** (T032–T036): gate = V12 full regression + audit cross-check + CI wired + V1–V12 execution log opened. Commit, report, stop. (Execution results belong to future runs — Batch E opens the log, it does not pre-pass anything.)

### FR → task mapping (self-check)

| FR | Tasks |
|----|-------|
| FR-01 detect | T008, T020, T029 |
| FR-02 establish+pause atomic | T011, T020 |
| FR-03 depth/config | T009, T018, T029 |
| FR-04 unrecoverable→reconcile | T010, T024, T029 |
| FR-05/06/07 invalidate+orphan+audit | T011, T020, T035 |
| FR-08 re-mine vs revive (Q4) | T011, T022, T023 |
| FR-09/10 replay semantics | T011, T020, T022 |
| FR-11 skew/empty | T011, T024 |
| FR-12/13 resume/unknown | T010, T011, T021, T028 |
| FR-14/15/20 fencing/races | T011, T013–T017, T025–T027 |
| FR-16 re-fork | T010, T011, T028 |
| FR-17 independent pauses | T011, T031 |
| FR-18/21/22 query/audit/observe | T012, T032, T035 |
| FR-19 completion | T011, T020, T028 |
| FR-23 auto/manual auth | T011, T020, T030, T031 |
| FR-24 deferrals (R1–R14, no remainder) | T006–T017 |
| FR-25 preconditions | T001–T003 (T002 blocks T016/T034 while missing; T015 needs no T002 — 004-owned) |
| FR-26 operation matrix | T011 (recovery-side), T013–T016 (ordinary-side), T031 (coexistence), T033 (downstream checks) |

### SC / Story / V → task mapping (self-check)

| SC / Story / V | Tasks |
|---|---|
| SC-01 / US1 / V3 | T020 (+T021) |
| SC-02 / US2 / V4 | T022, T023 |
| SC-03 / US3 / V5 | T024 |
| SC-04 / US4 / V6-converge | T025 (+T026) |
| SC-05 / US4 / V6-isolation | T026 |
| SC-06 / US5 / V7-race | T027 |
| SC-07 / US5 / V7-crash+drill | T021, T028 |
| SC-08 / US5 / V8 | T028 |
| SC-09 / US6 / V9 | T029 |
| SC-10 / US6 / V10 + V11-tamper | T029, T031 |
| SC-11 / US6 / V11 | T030, T031 |
| SC-12 / cross / V12 | T007, T032, T034, T035 |
| Downstream 007–011 (contract only) | T033 (zero implementation tasks) |
| CI + execution log | T036 |

Coverage gaps / explicit non-claims: (1) T002 evidence may be missing — then T016/T034 stay blocked and story phases wait via unclosed Phase 2 (Dependencies), and that block is itself the recorded outcome. (2) Retry/backoff/batch-size/poll-interval values are bounded-but-unfixed per plan — implementers choose within stated bounds; no task fixes them here. (3) 007–011 behavior is downstream-plan input only. Formal `/speckit.analyze` readiness: YES — provided T002's verdict is recorded (pass or blocked-with-handling); analyze must not re-derive Q1–Q4 or R1–R14.

## Parallel Example

```bash
# Foundation new-file implementations (Batch A: T009 alone; Batch B: T012 parallel-safe with T011 — different files, T012 depends only on T006):
Task: "Implement depth-config versions in internal/indexer/reorgpolicy.go"        # T009
Task: "Implement validity-annotated readers in internal/indexer/reorgquery.go"    # T012
# T010 is NOT parallel: it calls T011's functions, so T011 executes first inside Batch B.
# Fencing gates T013–T016: NEVER parallel — one reviewable unit, shared discipline.
# E2E tasks T020–T022/T024/T028–T029: NEVER parallel on one harness — separate Anvil/DB per worker or run sequentially.
```

## Implementation Strategy

### MVP First (US1 Only)

1. Complete Phase 1 (T001–T005) — evidence ledger, environment known.
2. Complete Phase 2 (T006–T019) — migration + fencing + gates green.
3. Complete US1 (T020–T021) — shallow close-loop green.
4. **STOP and VALIDATE**: V3 + crash spot check independently green.
5. Local commit; report; stop. (No push/PR/merge — branch-local workflow per prior steps.)

### Incremental Delivery

Phase 1 → Phase 2 → US1 (MVP) → US2 → US3 → US4 → US5 → US6 → Polish. Each phase independently verifiable via its mapped V-scenarios; each suggested batch (A–E) ends with checks + diff + local commit + report-and-stop.

---

## Notes

- All 36 tasks (T001–T036) are UNCHECKED future work. Generating this list completes nothing.
- [P] appears ONLY on: T004, T005 (independent setup surfaces), T009, T012, T018 (new files with no same-batch upstream code). T010 lost its [P] (calls T011's functions — T011 first). Everything sharing the write-path discipline (T008+T013 same file; T013–T016 one fencing unit; T026/T031 gate tests) or the Anvil/DB harness is sequential by rule.
- Commit discipline: after each task or logical group (per-batch closeout above); local commits only.
- Design conflicts found during this step: NONE. e06222f rules (persistent version gate, capture-before-inputs, commit triple, mismatch-refuses-even-on-content-match, no exemptions) are carried verbatim into T011/T013–T016/T026; no approved rule altered, no plan rewritten.
