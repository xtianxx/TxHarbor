# Tasks: 007 Authorization Carrier Supplement (PB)

**Input**: Design documents from `/specs/012-007-authorization-carrier/` (spec.md, plan.md, research.md R-PB1–R-PB10, data-model.md, contracts/supply-scope.md, quickstart.md V-PB1–V-PB11)

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

- [x] T001 Verify lane baseline in `.slim/worktrees/prep-pb-auth`: `git rev-parse HEAD` recorded, `go build ./...` + `go vet ./...` clean, `go test ./internal/withdrawal/ ./internal/app/ ./internal/db/` green on the unmodified tree. Done: recorded SHAs + clean outputs.
- [x] T002 [P] Pin the 009 reference: record 009 worktree HEAD SHA and copy `migrations/000009_signer_service.sql` @pinned-SHA into a scratch-only dir (e.g. `/tmp/pb-009ref/`); NEVER commit it into this lane. If 009 HEAD advances before Phase 6 runs, re-pin and judge the re-verify scope (new 009 migration content → re-run T040/T041). Done: SHA + scratch path recorded; `git status` shows no 009 file in the lane. SUPERSEDED (fixture fix): the pin now lives as a read-only test-only copy at `internal/db/testdata/000009_signer_service.sql` (provenance in `internal/db/testdata/README.md`; limited exception to the never-commit rule, production migrations untouched). Remote CI on PR #11 proved the scratch dir was a hidden dependency (run 35169843720: 6 failures on missing `/tmp/pb-009ref`); fixture fix re-verified 6/6 + full `internal/db` green.

## Phase 2: Foundational (blocks US1–US3)

**Purpose**: carrier DDL + op-input types everything else builds on.

- [x] T003 Additive migration `migrations/000010_withdrawal_authorization_scopes.sql` (planning number; renumber-at-merge applies): `+goose Up` creates `withdrawal_authorization_scopes` per data-model.md (PK/FK, fee triple + `priority ≤ max_fee` CHECKs, version ≥ 1, purpose boolean, attested_by); `+goose Down` drops it. Pure DDL, zero 007 change. Done: `goose validate`-equivalent (`MigrationFiles` parse) + up/down on scratch DB green.
- [x] T004 Extend supply op-input types in `internal/withdrawal/grant.go` (`OpInput`: scope fields + `attested_by`), validation (`validateSupplyOpInput`: sender shape, fee values ≥ 0, purpose boolean, attested_by presence rule), and `opInputDetail` snapshot extension. Done: unit tests green (valid/invalid matrices, nil-pool-safe like existing).
- [x] T005 [P] Issuance allowlist config: `TXHARBOR_AUTHZ_ISSUER_CALLERS` (comma-separated decimal caller_ids; whitespace trimmed, empty entries ignored; missing/empty → deny-all; illegal → startup config error exit 2), loader + pure `PermitIssue(callerID)` in `internal/withdrawal/` (nil-pool-safe). Done: unit tests (mapped → allow; unmapped/empty → deny; illegal → startup error).

**Checkpoint**: foundation ready — migration applies, types validate, permission predicate exists.

## Phase 3: US1 — 合法签发 (P1) 🎯 MVP

**Goal**: authorized supply writes grant + scope + audit atomically; 009-style read-back verifies.

**Independent test**: a newly supplied grant passes the full independent verification (all fields, version 1) with zero `authorization_unverifiable`.

- [x] T010 [US1] Integration test: legal supply round-trip in `internal/withdrawal/grant_integration_test.go` (new scope assertions: row equality, version 1, audit agreement grant↔scope↔principal). Must FAIL before T012.
- [x] T011 [US1] Integration test: permission gate in `internal/app/withdrawalauthz_integration_test.go` (unmapped principal → refusal + zero rows; mapped principal → green). Must FAIL before T013.
- [x] T012 [US1] T-supply+scope in `internal/withdrawal/grant.go`: same-tx grant + scope + audit writes; scope fields in guarded resupply comparison; scope row in commit-unknown re-read. (Depends on T004; sequenced with T013 on the shared file.)
- [x] T013 [US1] CLI carrier in `internal/app/withdrawalauthz.go`: new flags, resource order **validate-flags → open pool → Authenticate → PermitIssue → SupplyGrant** (Authenticate needs the pool for well-formed keys; only shape failures are pool-free — wire the order explicitly, not just "pre-pool"); `--operator` stays audit-only. (Sequenced with T012.)
- [x] T014 [P] [US1] Credential secrecy on the supply path: presented key never logged/stored (redaction assertion over CLI stderr + slog capture); `attested_by` mismatch semantics — same operation id from a different principal → `operation_conflict` (test), same principal → converge (existing semantics kept). (Test-only files, parallel-safe.)
- [x] T015 [US1] In-tx authority re-verification in `internal/withdrawal/grant.go` (design closed this round, R-PB10 — no bounded-window option): inside T-supply+scope before any write, re-read api_key row `FOR SHARE` + caller row `FOR SHARE` + re-evaluate `PermitIssue`; failure → `supply_refused` naming the check, zero writes. Test: revoke-then-supply refused; race where revocation commits first wins (loser observes revoked). Sequenced after T012 (same file, extends its tx).
- [x] T016 [US1] Fee triple validation (PB-C2): total/per-gas/priority caps, `priority ≤ max_fee` cross-check, missing/illegal/over-limit refuse, legacy `gas_price` path, native最小单位 integers. Unit + integration (each boundary both sides). (Touches `grant.go` — sequenced after T012/T015, not parallel with them.)

**Checkpoint**: US1 independently functional — legal supply verifiable end-to-end.

## Phase 4: US2 — 无权拒绝与撤销一致 (P2)

**Goal**: refusals hold; revoke keeps scope consistent; versions monotonic.

- [x] T020 [US2] Negative tests: unmapped/unauthenticated supply (zero rows), malformed scope input (zero rows), over-limit fee (zero rows). New cases in existing test files (shares files with T010/T011 — sequenced after them, not parallel).
- [x] T021 [US2] T-revoke-sync in `internal/withdrawal/grant.go`: revoke tx syncs scope state/version; version monotonicity (re-supply cycles bump, never rewrite applied rows). Integration test: supply → revoke → re-supply round-trip with version assertions.
- [x] T022 [US2] Revoke-vs-supply race test in `internal/withdrawal/grant_integration_test.go` (existing race-test file): concurrent revoke + supply on one grant; loser observes winner state; scope consistent in both outcomes. (Real PG, no sleeps-as-proof — barrier/lock-step like the file's existing race tests.)

**Checkpoint**: US1 + US2 both green; refusals never write.

## Phase 5: US3 — 存量与重签发 (P3) + 009 PB-01–PB-05 single-ownership mapping

**Goal**: stock stays queryable-but-refused; explicit re-issuance with traceability; PB-01–PB-05 done exactly once, here.

- [x] T030 [US3] T-reissue procedure in `internal/withdrawal/grant.go` + CLI: NEW grant id + scope in one supply tx, audit detail links old ids; asserts no UPDATE to old rows. Side-effect proof on an isolated small fixture: full-content snapshot compare (all business columns, ordered) of the seven `nonce_*` tables before/after (catches in-place UPDATE and equal-size replacement, not just counts); 007/PB tables per-operation allowlist (re-issue allows exactly +1 grant/+1 scope/+1 audit with expected values, everything else byte-identical). Intent scope: distinct-`intent_id`-unchanged applies to RE-ISSUE ONLY (same business intent reused); first legal supply MAY introduce new authorization linkage and MUST NOT be failed by it. No independent intent table exists in this tree — record the 009/011 re-check obligation (H5), invent no future tables. Integration test with old/new row census.
- [x] T031 [US3] OPEN-3 dry-run execution (quickstart.md procedure): pick stock grant → re-verify → re-issue → assert trace + zero new intent/nonce; record the dry-run log pointer in quickstart.md. (Execution task; procedure already designed.)
- [x] T032 PB-01 migration ownership: this lane's T003 IS PB-01 (renumber-at-merge rule applied at merge; applied numbers immutable). No second migration task elsewhere.
- [x] T033 PB-02 entry ownership: this lane's T012/T013 IS PB-02. No 009-side entry work.
- [x] T034 PB-03 evidence ownership: permission-control evidence = T011/T014/T020 tests + control-point doc; minimum fix (if any) lands via T013/T015. No separate 009 task.
- [x] T035 PB-04 procedure ownership: this lane's T030/T031 IS PB-04 (per-grant refusal stays 009-side by design, not implemented here).
- [x] T036 PB-05 upgrade verification: sequences (a) empty→full, (b) 007-era→carrier, (c) down→re-up, **(d) gap-fill** ({1..8,10} applied + files {1..10} → `migrate up` applies exactly 9; gate green after). Scratch DBs only. Code-reading conclusions MUST NOT be marked as test evidence.

**Checkpoint**: all stories independently functional; PB-01–PB-05 owned exactly once.

## Phase 6: Migration chain isolation (real runner proof)

**Purpose**: prove the R-PB8 order claim with the actual runner (reading ≠ passing).

- [x] T040 Scratch-harness for 009-file integration: source was `/tmp/pb-009ref/` copy @pinned-SHA (T002) — SUPERSEDED by the `internal/db/testdata` embedded fixture (same bytes, same sha256 assert; see T002 note). Isolation = scratch PostgreSQL per sequence, files copied into a temp migrations FS overlay — production migrations never contain 009's file (guarded by `TestLaneMigrationsExclude009`). Done: harness documented + green on empty sequence; fixture fix re-verified after PR #11 CI failure.
- [x] T041 Gap-fill sequence (d): applied {1..8,10} + embedded {1..10} → `migrate up` output `applied=1`, then `CheckCompatibility` green and a serve-gate dry check green; pre-fill gate red recorded (serve must refuse while 9 pending). Done: log pointers recorded.
- [x] T042 Rollback order + limits: `down` from full chain reverts 10 before 9 (applied-descending); scopes data loss on 10-down stated as designed-for-scratch limit; `down` of an applied-then-merged number is forbidden (renumber rule). Done: order asserted, limits written into quickstart.md.
- [x] T043 Allowlist switchover rehearsal (R-PB10 procedure, scratch only; needs T005, T013): halt entry → drain + confirm via process table and `pg_stat_activity` (no supply tx open) → atomic config swap with recorded checksum/timestamp → re-enable → old principal refused, new principal allowed; failure path (unaccounted old executor) keeps entry closed. Done: rehearsal log pointer recorded; per-host replica precondition checked or reported as gap.

## Phase 7: Acceptance mapping & handoff (no new behavior)

### V-PB ↔ task map (each row executed, none by assertion)

| Case | Implementing tasks | Executing task |
|---|---|---|
| V-PB1 legal supply + read-back | T004, T012, T013 | T050 |
| V-PB2 unauthorized supply refused | T005, T013, T020 | T050 |
| V-PB3-PB stock queryable + scope absent | T030 (procedure) | T050 (closes PB part only) |
| V-PB3-009 per-grant refuse on 009 lane | handoff H4 (009 lane, post-merge) | joint — NOT closed by T050 |
| V-PB4 fee boundaries (total/per-gas/priority/cross/missing/illegal/over) | T016 | T050 |
| V-PB5 fee replacement conditional | T016 + data-model rule | T050 |
| V-PB6 revoke vs supply race | T021, T022 | T050 |
| V-PB7 atomicity (kill -9 phase points) | T012 (convergence) | T050 |
| V-PB8 commit-unknown same-op retry | T012 | T050 |
| V-PB9 re-issuance dry-run (OPEN-3) | T030, T031 | T031, T050 |
| V-PB10 migration chain incl. gap-fill | T003, T036, T040, T041, T042 | T050 |
| V-PB11 allowlist switchover rehearsal | T043 | T050 |

### PB-FR ↔ task map

PB-FR-01 → T003, T004, T012; PB-FR-02 → T012, T021, T030; PB-FR-03 → T005, T011, T013, T014, T015, T043; PB-FR-04 → T030, T031; PB-FR-05 → T016; PB-FR-06 → T003 (additive-only gate), T052 (regression); PB-FR-07 → T051 (boundary); PB-FR-08 → T003, T036, T041, T042.

### PB-SC ↔ case map

PB-SC-01 → V-PB1, V-PB5; PB-SC-02 → V-PB2, V-PB6, V-PB11; PB-SC-03 → V-PB3-PB, V-PB9 (V-PB3-009 joint); PB-SC-04 → V-PB10.

- [x] T050 Execute quickstart.md V-PB1–V-PB11 matrix PB-executable part (V-PB3-009 stays joint-open for the 009 lane); record per-case log pointers + code SHA; PB-FR-01–PB-FR-08 ↔ tasks and PB-SC-01–PB-SC-04 ↔ cases mapping table completed. Refusal paths MUST NOT substitute for legal-supply acceptance (V-PB1 green is mandatory).
- [x] T051 009 handoff requirements doc (lane-local note, 009 untouched): consumption fields, version persist/re-check, `replacement_of` threading, `GrantScope{Present:false}` replacement point (009 `submit.go:211`), delivery re-check point — all as *requirements on the 009 lane*, not implemented here.
- [x] T052 Regression: `go test ./...` + affected integration packages + `gofmt`/`vet`/`build` green on final tree; evidence index appended (commands + SHAs + logs, MISSING where not persisted).

## Dependencies & Execution Order

- T001 → everything (baseline). T002 → T040/T041 (009-file source).
- Phase 2 (T003/T004/T005) BLOCKS US phases. T005 → T013/T011 (predicate before carrier/tests).
- US1: T010/T011 (fail-first) → T012/T013 (sequenced, shared files) → T014[P]/T015/T016/T021-tail in file order: T014 parallel-safe (test-only files); T015 then T016 sequenced after T012 (all touch `grant.go`).
- US2 (T020/T021/T022): needs Phase 2 only; T020 sequenced after T010/T011 (shared test files); T021 shares `grant.go` → sequenced after T012/T015/T016. Otherwise parallel with US1's tail.
- US3: T030 needs T012/T015/T016/T021 (same tx patterns + `grant.go` order); T031 needs T030; T032–T036 are ownership records completed by their mapped tasks; T036 needs T003 + T040.
- Phase 6: T040 → T041/T042. Phase 7: needs all behavior phases; T050 closes everything except joint V-PB3-009.
- Wave plan: W1 = T001/T002; W2 = T003/T004/T005; W3 = T010/T011 fail-first (T020 follows, not parallel); W4 = T012→T013→T015→T016→T021 (single `grant.go`/`withdrawalauthz.go` lane, strictly sequenced) + T014[P] alongside; W5 = T022/T030; W6 = T031 + T036 + Phase 6 (T040→T041/T042, T043 rehearsal); W7 = Phase 7. No fixed model/task-count caps; shared files coordinated by the orchestrator.
- Dependency cycle check (tasks-internal, not formal analyze): Phase 2 → US phases forward; T015→T016→T021→T030 chain forward; US3→US1/US2 files forward; Phase 6→Phase 2/009-ref forward; Phase 7 terminal. No cycles; no dangling refs (every dep named above exists in this list).

## 009 handoff points (requirements, not tasks here)

H1: carrier read shape + scope checks (009 `gates.go`); H2: `authorization_version` persist + delivery re-check; H3: `replacement_of` threading; H4: replace hardcoded scopeless refusal; H5: 009 full legal-path acceptance post-PB-merge. 009 lane owns H1–H5 after PB merges.

## Notes

- All tasks `[ ]` (unchecked); counts: 29 tasks (T001–T052 numbered by phase, incl. T043 rehearsal).
- Design closed this round (R-PB10): T015 implements in-tx re-verification, no bounded-window option, no decision left for implement.
- A-13 / T000-P stay OPEN; 009 files untouched; no push/PR/merge/deploy.
