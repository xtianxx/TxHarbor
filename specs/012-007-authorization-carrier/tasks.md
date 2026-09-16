# Tasks: 007 Authorization Carrier Supplement (PB)

**Input**: Design documents from `/specs/012-007-authorization-carrier/` (spec.md, plan.md, research.md R-PB1–R-PB9, data-model.md, contracts/supply-scope.md, quickstart.md V-PB1–V-PB10)

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Branch**: `012-007-authorization-carrier` | **Base**: `origin/main 02641fb`

**Ownership**: all tasks below are owned by the 007-extension (PB) lane.
009's PB-01–PB-05 map 1:1 onto Phase 5 of this list (single ownership, no
duplicate归属). 009 consumption adaptation is a handoff requirement (§Handoff),
not a task here — 009 files MUST NOT be modified in this lane.

**Status rule**: every task starts `[ ]`. A task list is not implementation;
green requires the stated verification, never the plan's existence.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: different files, no dependencies — runnable in parallel
- **[Story]**: US1/US2/US3 per spec.md priorities
- Shared-file rule: `internal/withdrawal/grant.go` and
  `internal/app/withdrawalauthz.go` are touched by several tasks — those tasks
  are NOT marked [P] against each other; the orchestrator sequences them.
- Test-first: unit/integration tests are written to FAIL before the behavior
  they cover (red-green per task).

## Phase 1: Setup (lane infrastructure)

**Purpose**: verify the lane can build, migrate, and test before any behavior work.

- [ ] T001 Verify lane baseline in `.slim/worktrees/prep-pb-auth`: `git rev-parse HEAD` recorded, `go build ./...` + `go vet ./...` clean, `go test ./internal/withdrawal/ ./internal/app/ ./internal/db/` green on the unmodified tree. Done: recorded SHAs + clean outputs.
- [ ] T002 [P] Pin the 009 reference: record 009 worktree HEAD SHA and copy `migrations/000009_signer_service.sql` @pinned-SHA into a scratch-only dir (e.g. `/tmp/pb-009ref/`); NEVER commit it into this lane. Done: SHA + scratch path recorded; `git status` shows no 009 file in the lane.

## Phase 2: Foundational (blocks US1–US3)

**Purpose**: carrier DDL + op-input types everything else builds on.

- [ ] T003 Additive migration `migrations/000010_withdrawal_authorization_scopes.sql` (planning number; renumber-at-merge applies): `+goose Up` creates `withdrawal_authorization_scopes` per data-model.md (PK/FK, fee triple + `priority ≤ max_fee` CHECKs, version ≥ 1, purpose boolean, attested_by); `+goose Down` drops it. Pure DDL, zero 007 change. Done: `goose validate`-equivalent (`MigrationFiles` parse) + up/down on scratch DB green.
- [ ] T004 Extend supply op-input types in `internal/withdrawal/grant.go` (`OpInput`: scope fields + `attested_by`), validation (`validateSupplyOpInput`: sender shape, fee values ≥ 0, purpose boolean, attested_by presence rule), and `opInputDetail` snapshot extension. Done: unit tests green (valid/invalid matrices, nil-pool-safe like existing).
- [ ] T005 [P] Issuance allowlist config: new deployment config source (exact env name fixed here, e.g. `TXHARBOR_AUTHZ_ISSUER_CALLERS`), loader + pure `PermitIssue(callerID)` in `internal/withdrawal/` (nil-pool-safe). Done: unit tests (mapped → allow; unmapped/empty → deny).

**Checkpoint**: foundation ready — migration applies, types validate, permission predicate exists.

## Phase 3: US1 — 合法签发 (P1) 🎯 MVP

**Goal**: authorized supply writes grant + scope + audit atomically; 009-style read-back verifies.

**Independent test**: a newly supplied grant passes the full independent verification (all fields, version 1) with zero `authorization_unverifiable`.

- [ ] T010 [US1] Integration test: legal supply round-trip in `internal/withdrawal/grant_integration_test.go` (new scope assertions: row equality, version 1, audit agreement grant↔scope↔principal). Must FAIL before T012.
- [ ] T011 [US1] Integration test: permission gate in `internal/app/withdrawalauthz_integration_test.go` (unmapped principal → refusal + zero rows; mapped principal → green). Must FAIL before T013.
- [ ] T012 [US1] T-supply+scope in `internal/withdrawal/grant.go`: same-tx grant + scope + audit writes; scope fields in guarded resupply comparison; scope row in commit-unknown re-read. (Depends on T004; sequenced with T013 on the shared file.)
- [ ] T013 [US1] CLI carrier in `internal/app/withdrawalauthz.go`: new flags, resource order **validate-flags → open pool → Authenticate → PermitIssue → SupplyGrant** (Authenticate needs the pool for well-formed keys; only shape failures are pool-free — wire the order explicitly, not just "pre-pool"); `--operator` stays audit-only. (Sequenced with T012.)
- [ ] T014 [US1] Credential secrecy on the supply path: presented key never logged/stored (redaction assertion over CLI stderr + slog capture); `attested_by` mismatch semantics — same operation id from a different principal → `operation_conflict` (test), same principal → converge (existing semantics kept).
- [ ] T015 ⚠️ [US1] Auth-to-write window: key revocation / permission change between Authenticate and the supply-tx commit. Implement ONE of: (a) in-tx re-verification (key_hash re-read `FOR SHARE` inside T-supply+scope), or (b) bounded-window rationale (short-lived CLI process + principal/timestamp in audit) with a test pinning the chosen behavior. **BLOCKS US1 completion until decided in implement — flagged here, not silently defaulted.**
- [ ] T016 [US1] Fee triple validation (PB-C2): total/per-gas/priority caps, `priority ≤ max_fee` cross-check, missing/illegal/over-limit refuse, legacy `gas_price` path, native最小单位 integers. Unit + integration (each boundary both sides).

**Checkpoint**: US1 independently functional — legal supply verifiable end-to-end.

## Phase 4: US2 — 无权拒绝与撤销一致 (P2)

**Goal**: refusals hold; revoke keeps scope consistent; versions monotonic.

- [ ] T020 [P] [US2] Negative tests: unmapped/unauthenticated supply (zero rows), malformed scope input (zero rows), over-limit fee (zero rows). New cases in existing test files.
- [ ] T021 [US2] T-revoke-sync in `internal/withdrawal/grant.go`: revoke tx syncs scope state/version; version monotonicity (re-supply cycles bump, never rewrite applied rows). Integration test: supply → revoke → re-supply round-trip with version assertions.
- [ ] T022 [US2] Revoke-vs-supply race test: concurrent revoke + supply on one grant; loser observes winner state; scope consistent in both outcomes. (Real PG, no sleeps-as-proof — use barrier/lock-step like existing race tests.)

**Checkpoint**: US1 + US2 both green; refusals never write.

## Phase 5: US3 — 存量与重签发 (P3) + 009 PB-01–PB-05 single-ownership mapping

**Goal**: stock stays queryable-but-refused; explicit re-issuance with traceability; PB-01–PB-05 done exactly once, here.

- [ ] T030 [US3] T-reissue procedure in `internal/withdrawal/grant.go` + CLI: NEW grant id + scope in one supply tx, audit detail links old ids; asserts no UPDATE to old rows and no intent/nonce writes (table-set assertion). Integration test with old/new row census.
- [ ] T031 [US3] OPEN-3 dry-run execution (quickstart.md procedure): pick stock grant → re-verify → re-issue → assert trace + zero new intent/nonce; record the dry-run log pointer in quickstart.md. (Execution task; procedure already designed.)
- [ ] T032 PB-01 migration ownership: this lane's T003 IS PB-01 (renumber-at-merge rule applied at merge; applied numbers immutable). No second migration task elsewhere.
- [ ] T033 PB-02 entry ownership: this lane's T012/T013 IS PB-02. No 009-side entry work.
- [ ] T034 PB-03 evidence ownership: permission-control evidence = T011/T014/T020 tests + control-point doc; minimum fix (if any) lands via T013/T015. No separate 009 task.
- [ ] T035 PB-04 procedure ownership: this lane's T030/T031 IS PB-04 (per-grant refusal stays 009-side by design, not implemented here).
- [ ] T036 PB-05 upgrade verification: sequences (a) empty→full, (b) 007-era→carrier, (c) down→re-up, **(d) gap-fill** ({1..8,10} applied + files {1..10} → `migrate up` applies exactly 9; gate green after). Scratch DBs only. Code-reading conclusions MUST NOT be marked as test evidence.

**Checkpoint**: all stories independently functional; PB-01–PB-05 owned exactly once.

## Phase 6: Migration chain isolation (real runner proof)

**Purpose**: prove the R-PB8 order claim with the actual runner (reading ≠ passing).

- [ ] T040 Scratch-harness for 009-file integration: source = `/tmp/pb-009ref/` copy @pinned-SHA (T002); isolation = scratch PostgreSQL per sequence, files copied into a temp migrations FS overlay — the PB lane tree never contains 009's file. Done: harness documented + green on empty sequence.
- [ ] T041 Gap-fill sequence (d): applied {1..8,10} + embedded {1..10} → `migrate up` output `applied=1`, then `CheckCompatibility` green and a serve-gate dry check green; pre-fill gate red recorded (serve must refuse while 9 pending). Done: log pointers recorded.
- [ ] T042 Rollback order + limits: `down` from full chain reverts 10 before 9 (applied-descending); scopes data loss on 10-down stated as designed-for-scratch limit; `down` of an applied-then-merged number is forbidden (renumber rule). Done: order asserted, limits written into quickstart.md.

## Phase 7: Acceptance mapping & handoff (no new behavior)

### V-PB ↔ task map (each row executed, none by assertion)

| Case | Implementing tasks | Executing task |
|---|---|---|
| V-PB1 legal supply + read-back | T004, T012, T013 | T050 |
| V-PB2 unauthorized supply refused | T005, T013, T020 | T050 |
| V-PB3 scopeless per-grant refuse | T030 (procedure) + 009-side (handoff H4) | T050 (PB part) |
| V-PB4 fee boundaries (total/per-gas/priority/cross/missing/illegal/over) | T016 | T050 |
| V-PB5 fee replacement conditional | T016 + data-model rule | T050 |
| V-PB6 revoke vs supply race | T021, T022 | T050 |
| V-PB7 atomicity (kill -9 phase points) | T012 (convergence) | T050 |
| V-PB8 commit-unknown same-op retry | T012 | T050 |
| V-PB9 re-issuance dry-run (OPEN-3) | T030, T031 | T031, T050 |
| V-PB10 migration chain incl. gap-fill | T003, T036, T040, T041, T042 | T050 |

### PB-FR ↔ task map

PB-FR-01 → T003, T004, T012; PB-FR-02 → T012, T021, T030; PB-FR-03 → T005, T011, T013, T014, T015; PB-FR-04 → T030, T031; PB-FR-05 → T016; PB-FR-06 → T003 (additive-only gate), T052 (regression); PB-FR-07 → T051 (boundary); PB-FR-08 → T003, T036, T041, T042.

### PB-SC ↔ case map

PB-SC-01 → V-PB1, V-PB5; PB-SC-02 → V-PB2, V-PB6; PB-SC-03 → V-PB3, V-PB9; PB-SC-04 → V-PB10.

- [ ] T050 Execute quickstart.md V-PB1–V-PB10 matrix; record per-case log pointers + code SHA; PB-FR-01–PB-FR-08 ↔ tasks and PB-SC-01–PB-SC-04 ↔ cases mapping table completed. Refusal paths MUST NOT substitute for legal-supply acceptance (V-PB1 green is mandatory).
- [ ] T051 009 handoff requirements doc (lane-local note, 009 untouched): consumption fields, version persist/re-check, `replacement_of` threading, `GrantScope{Present:false}` replacement point (009 `submit.go:211`), delivery re-check point — all as *requirements on the 009 lane*, not implemented here.
- [ ] T052 Regression: `go test ./...` + affected integration packages + `gofmt`/`vet`/`build` green on final tree; evidence index appended (commands + SHAs + logs, MISSING where not persisted).

## Dependencies & Execution Order

- T001 → everything (baseline). T002 → T040/T041 (009-file source).
- Phase 2 (T003/T004/T005) BLOCKS US phases. T005 → T013/T011 (predicate before carrier/tests).
- US1: T010/T011 (fail-first) → T012/T013 (sequenced, shared files) → T014/T016 (parallel [P] after) → T015 decision BLOCKS US1-done.
- US2 (T020/T021/T022): needs Phase 2 only; T021 shares `grant.go` → sequenced after T012. Otherwise parallel with US1's tail.
- US3: T030 needs T012/T021 (same tx patterns); T031 needs T030; T032–T036 are ownership records completed by their mapped tasks; T036 needs T003 + T040.
- Phase 6: T040 → T041/T042. Phase 7: needs all behavior phases.
- Wave plan: W1 = T001/T002; W2 = T003/T004/T005; W3 = T010/T011/T020 (fail-first tests); W4 = T012→T013 (sequenced) + T021; W5 = T014/T016/T022/T030; W6 = T015 decision + T031 + T036 + Phase 6; W7 = Phase 7. No fixed model/task-count caps; shared files coordinated by the orchestrator.
- Dependency cycle check (tasks-internal, not formal analyze): T015→US1-done; US1→nothing (no back-edge); US3→US1/US2 files (forward only); Phase 6→Phase 2/009-ref (forward); Phase 7→all (terminal). No cycles; no dangling refs (every dep named above exists in this list).

## 009 handoff points (requirements, not tasks here)

H1: carrier read shape + scope checks (009 `gates.go`); H2: `authorization_version` persist + delivery re-check; H3: `replacement_of` threading; H4: replace hardcoded scopeless refusal; H5: 009 full legal-path acceptance post-PB-merge. 009 lane owns H1–H5 after PB merges.

## Notes

- All tasks `[ ]` (unchecked); counts: 28 tasks (T001–T052 numbered by phase).
- Genuine blocker flagged: T015 (auth-to-write window decision in implement).
- A-13 / T000-P stay OPEN; 009 files untouched; no push/PR/merge/deploy.
