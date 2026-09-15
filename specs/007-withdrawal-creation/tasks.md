# Tasks: 007 Withdrawal Creation & Query (Receive-Only)

**Input**: Design documents from `/specs/007-withdrawal-creation/`

**Prerequisites**: plan.md, spec.md (US1–US6), research.md (R1–R9), data-model.md (Tables 1–6 + tx catalog + supply pseudocode), contracts/api.md, quickstart.md (V1–V9)

**Tests**: Included per story — spec mandates failure-path coverage (constitution IX/XI) and quickstart V-matrix is the acceptance surface. Unit tests run under `go test ./...`; integration/E2E under `go test -tags integration` (see Makefile `test-integration`; `db.OpenPool` in `internal/db/pool.go`; test-file precedent `internal/app/confirmauth_test.go`, `internal/indexer/depositauth_test.go`).

**Organization**: Grouped by user story (US1–US6). FR/SC/V trace per phase. T000-P stays open (referenced, never closed here). 008–011 excluded. 006 merge/main-CI cited as baseline only, never as 007 verification.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1–US6)
- Exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Package skeleton, error vocabulary, shared validators — no business behavior

- [ ] T001 Create `internal/withdrawal/` package skeleton per plan.md source tree (auth.go, validate.go, intake.go, query.go, grant.go + _test.go stubs)
- [ ] T002 [P] Define typed domain errors + stable machine codes per contracts/api.md §1 table in `internal/withdrawal/errors.go` (FR-14: malformed_request/validation_failed/unauthenticated/unauthorized/authorization_invalid/idempotency_conflict/not_found/temporarily_unavailable/operation_conflict)
- [ ] T003 [P] Implement shared validators in `internal/withdrawal/validate.go` (FR-04 chain bind; FR-05 whitelist hook; FR-06 `[1-9][0-9]*` shape + `math/big` ≤ 2²⁵⁶−1; FR-07 EIP-55 + lowercase canonicalization; FR-09 key shape 1–128 ASCII 0x21–0x7E) with smoke vectors only (full matrix lives in T004)
- [ ] T004 [P] Unit tests for validators in `internal/withdrawal/validate_test.go` (V4 FULL matrix, owns all boundary assertions: wrong chain, non-whitelist, bad shape, mixed-case fail/pass, "0"/"00123"/"-5"/"1.5"/"1e3"/non-digits/>uint256, max-uint256 accept BOTH directions — persist-accept at max, layer-2 reject at max+1 with zero rows)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Migration + constraint carriers + auth/grant primitives every story needs. MUST complete before ANY user story.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T005 Write `migrations/000007_withdrawal_creation.sql` (Tables 1–6 per data-model.md: `caller`, `api_key` with `UNIQUE(key_hash)`, `withdrawal_requests` with `CONSTRAINT withdrawal_requests_caller_key_uniq UNIQUE (caller_id, idempotency_key)` + `CONSTRAINT withdrawal_requests_authorization_uniq UNIQUE (authorization_id)` + amount `CHECK(amount >= 1 AND amount <= 2²⁵⁶−1 literal)`, `withdrawal_authorizations` (PK `authorization_id`), `withdrawal_request_audit`, `withdrawal_grant_audit` with `CONSTRAINT withdrawal_grant_audit_operation_id_uniq UNIQUE (operation_id)`; ALL classifiable constraints explicitly named — short forms (`caller_key_uniq`/`authorization_uniq`/`operation_id_uniq`) are prose shorthands only, never match values; pure DDL, goose sequence)
- [ ] T006 Migration verification in `internal/withdrawal/migration_integration_test.go` (NEW file, tags: integration; precedent shape `internal/indexer/migrate_integration_test.go`; raw-SQL probes only — MUST NOT import `internal/withdrawal` intake/grant classifiers): (a) `git diff --name-only -- migrations/000001_baseline.sql migrations/000002_chain_indexer.sql migrations/000003_event_indexing.sql migrations/000004_deposit_detection.sql migrations/000005_confirmation_tracking.sql migrations/000006_reorg_recovery.sql` empty — history files untouched (normal test execution of historical migrations — fresh-DB build, seeded-upgrade replay — remains allowed; the ban is on modifying history files, never on replaying them); (b) upgrade-compat on an ISOLATED scratch instance only (scratch-only; NOT a production rollback strategy; historical migrations are never withdrawn by default): baseline→000006→000007 `goose up` green + `goose status` clean, then 000007 `down`/`up` reproducible; (c) behavior assertions: negative inserts for 000007 — UNIQUE/PK conflicts (dup key, dup auth, dup `operation_id`, concurrent first-supply PK race) MUST fail with SQLSTATE 23505 and the EXPECTED exact `ConstraintName` (`withdrawal_requests_caller_key_uniq`, `withdrawal_requests_authorization_uniq`, `withdrawal_grant_audit_operation_id_uniq`, `withdrawal_authorizations_pkey`); amount CHECK violations (zero/over-uint256 amount) and bad-hex CHECK violations MUST fail with SQLSTATE 23514 (NOT 23505 — amount CHECK is never in the 23505 classify list) — checksum-only comparison is NOT accepted as compat proof; (d) constraint-name probe: each provoked violation's `ConstraintName` equals the name declared in `migrations/000007_withdrawal_creation.sql` — a probe of names, NOT of app branches; app-level semantics (dual-race fixed-order, commit-unknown re-classify, same-O/`operation_conflict`, N1 PK bounded branch) belong to T023/T024/T025/T026 and are NOT claimed here; (e) upstream smoke: 002–006 tables still accept one representative write each post-upgrade (FR-11/FR-12 carriers; T000-P unaffected)
- [ ] T007 [P] API-key credential core in `internal/withdrawal/auth.go` (R1–R2: `txh_` + 32B CSPRNG generation, sha256-hex storage, `UNIQUE(key_hash)`, constant-time compare, single-statement revocation predicate, `caller_id` derivation, key_id+prefix-only logging; exposes `IssueKey`/`RotateKey`/`RevokeKey` library functions owned by T034's subcommand; `[P]` ONLY with T008 — T009/T010/T011/T034 build on its interfaces)
- [ ] T008 [P] Grant-supply core in `internal/withdrawal/grant.go` (R9: `mint` + `supply`/`revoke` statement scripts, Table 6 attempt semantics with caller-supplied `operation_id`, op-input 8-field binding + compare, PK-race bounded re-execution, uncertain-COMMIT same-O recovery; `operator`/`reason` as retry metadata; `[P]` ONLY with T007 — T009/T010 build on its interfaces)
- [ ] T009 `withdrawal-authz` operator subcommand in `internal/app/withdrawalauthz.go` + dispatch case in `cmd/txharbor/main.go` mirroring `confirm-auth` carrier (`internal/app/confirmauth.go:16-35`: flag → config.Load → operator-connection tx; REQUIRED `--operation-id`; exit codes 0/1/2; in-process testable; depends on T008)
- [ ] T010 Intake transaction core in `internal/withdrawal/intake.go` (R7 FINAL + R8: pre-tx classify, BEGIN→writeGuard→`FOR SHARE` grant row→`clock_timestamp()`→validity strict-`>`→plain INSERT→audit→COMMIT; 23505 fixed-order classify on EXACT names `withdrawal_requests_caller_key_uniq` → 200/409 then `withdrawal_requests_authorization_uniq` → 403 incl. T-dual-race; commit-unknown re-classify; N1 PK-branch O-first re-read; pre-tx reject audit writer per data-model.md Table 5 C3/N1 rule (single attempt, detached 2s timeout, never blocks/alters response); 401 path writes no audit row — asserted in T026; depends on T007, T008)
- [ ] T011 Query core in `internal/withdrawal/query.go` (ownership-enforced read, identical-404 shape, single-snapshot recovery annotation per contracts/api.md §3: REPEATABLE READ row+event, precedence rule, unknown on either-failure, `execution:not_started`, NO height-indexed validity; depends on T007)
- [ ] T034 `apikey-auth` operator subcommand in `internal/app/apikeyauth.go` + dispatch case in `cmd/txharbor/main.go` mirroring `confirm-auth` carrier (R5: `issue --caller-id C --label L --operator OP --reason R` / `rotate --caller-id C [--grace-seconds S] …` / `revoke --key-id K …`; flag → config.Load → operator-connection tx over the DB operator's connection; plaintext shown once, digest-only persisted; rotation inserts successor + stamps predecessor `revoked_at`, never mutates `caller_id`; exit codes 0/1/2; in-process tests in `internal/app/apikeyauth_test.go` (usage/exit codes) + `internal/app/apikeyauth_integration_test.go` (tags: integration; issue→auth→rotate→revoke lifecycle against real PostgreSQL); depends on T007; blocks T018)

**Checkpoint**: Foundation ready — migration applies cleanly (`go build ./...` green), all carriers exist with interfaces frozen (`auth.go` Issue/Verify, `grant.go` supply/revoke scripts), T006 verification green, user stories can begin. Foundation completion = build green + T006 green + carrier unit/in-process tests green (T034's integration tests, T010/T011 compile + unit-level classify tests); story-level V-scenarios remain with their stories, not claimed here.

---

## Phase 3: User Story 1 — 正常创建与本人查询 (Priority: P1) 🎯 MVP

**Goal**: Authenticated authorized caller creates a compliant request (201) and reads it back (200, identical content)

**Independent Test**: V1 — valid body → 201 + `request_id` + `status:accepted`; same-key GET → identical body with `recovery:{state:none,execution:not_started}` (FR-01, FR-08, SC-01)

- [ ] T012 [P] [US1] Integration test for create+self-query in `internal/withdrawal/intake_integration_test.go` (tags: integration; V1 happy path incl. amount-string round-trip, SC-01/SC-08)
- [ ] T013 [P] [US1] Contract test for POST/GET shapes + error codes in `internal/withdrawal/contract_test.go` (201/200 bodies per contracts/api.md §§1/3)
- [ ] T014 [US1] Wire POST /withdrawals + GET /withdrawals/{id} on existing `http.Server` (`health.NewServer(...).Handler()` mux in `internal/app/serve.go:288-293`; no new listener; caller_id from key row only; route registration + handler wiring only) (depends on T010, T011, T035)
- [ ] T035 [US1] Config passthrough in `internal/config/config.go` (plan wiring: `ChainID` from `TXHARBOR_CHAIN_ID` as deployment chain bind for FR-04 + `HTTPAddr` reuse; NO new secret knobs in 007; validation via existing `config.Load` + `internal/app/serve_config_test.go`-style unit test; depends on T001; blocks T014)
- [ ] T015 [US1] Structured logging + metrics in `internal/withdrawal/intake.go` (log fields) + `internal/metrics/` registry extension (plan wiring; caller/request/chain/asset/retry fields, zero secrets via `logx.Redact`; FR-20/FR-21; depends on T014)

**Checkpoint**: US1 fully functional and testable independently (create → persist → self-query round-trip green)

---

## Phase 4: User Story 2 — 认证/授权/越权 (Priority: P1)

**Goal**: Unauthenticated/unauthorized/cross-caller requests rejected without leakage or persistence

**Independent Test**: V2 + V9 — no/bad/revoked key → 401; `can_create=FALSE` → 403; forged caller_id ignored; B-reads-A → byte-identical 404 (FR-02, FR-03, FR-15, SC-02)

- [ ] T016 [P] [US2] Integration test for auth matrix in `internal/withdrawal/auth_integration_test.go` (V2: 401/403 paths, zero rows asserted, rotation preserves caller_id + scope)
- [ ] T017 [P] [US2] Integration test for cross-caller privacy in `internal/withdrawal/privacy_integration_test.go` (V9: 404 byte-equality random-id vs foreign-id)
- [ ] T018 [US2] Key lifecycle operator flow in `internal/app/apikeyauth_integration_test.go` (tags: integration; depends on T034) + startpoint-semantics tests in `internal/withdrawal/auth_integration_test.go` (R4/R5: grace-window dual-accept, post-startpoint revoke affects next attempt only; rotation preserves caller_id + scope; V7 key half; blocks no story — US2 checkpoint requires it green)

**Checkpoint**: US1 AND US2 both work independently (auth boundary holds, privacy shape exact)

---

## Phase 5: User Story 3 — 参数校验 (Priority: P1)

**Goal**: All illegal params rejected pre-persist with typed errors

**Independent Test**: V4 — full illegal matrix → 400/422 per contract, zero rows; EIP-55-mixed-case accepted + lowercased (FR-04–FR-07, SC-03)

- [ ] T019 [P] [US3] Integration test for param matrix in `internal/withdrawal/validation_integration_test.go` (V4 incl. `1.5`/`1e3` decimal-barrier, max/max+1 boundary both directions per data-model.md layered amount enforcement §四: shape → big.Int range → DB CHECK)
- [ ] T020 [US3] Whitelist enforcement wiring against 003/004 policy source in `internal/withdrawal/validate.go` (FR-05; no redefinition of asset list)

**Checkpoint**: Validation airtight at API + Go + DB layers (three-layer amount story green)

---

## Phase 6: User Story 4 — 幂等语义 (Priority: P1)

**Goal**: Same-key replay/conflict + cross-caller isolation exact

**Independent Test**: V5 — same-key-equal → 200 same `request_id` row-count-unchanged; same-key-differ → 409 original-untouched; cross-caller same key → independent 201s (FR-09/FR-10, SC-04)

- [ ] T021 [US4] Concurrency test same-key N-way in `internal/withdrawal/idempotency_integration_test.go` (exactly-1-row, single `request_id` to all; V5 + V6 first half; owns the file — T022 appends its cases after T021 lands)
- [ ] T022 [US4] Cross-key same-grant + cross-caller isolation test in `internal/withdrawal/idempotency_integration_test.go` (403-path, original untouched; caller scoping; appends to T021's file, no `[P]` with T021)
- [ ] T023 [US4] Same-O op-conflict + grant-PK three-outcome tests in `internal/withdrawal/grant_integration_test.go` (data-model.md supply-pseudocode first-supply concurrency (i/ii/iii): one-grant-one-supplied + per-O audits;异参 loser `supply_refused`; never `operation_conflict`/503 for PK race)

**Checkpoint**: Idempotency exact under concurrency (no duplicate intents constructible)

---

## Phase 7: User Story 5 — 并发/丢失/重启/存储失败 (Priority: P1)

**Goal**: Real-fault correctness: loss, unknown-commit, crash, outage

**Independent Test**: V6 remainder — kill-9 mid-response → same-key 200; restart → 200; storage-down → 503 zero-Accepted; uncertain-COMMIT → re-classify (FR-12/FR-13, SC-05)

- [ ] T024 [P] [US5] Crash/restart/retry test in `internal/withdrawal/recovery_integration_test.go` (kill-9, restart, same-O grant recovery per Table 6; O-capture crash semantics)
- [ ] T025 [P] [US5] Revocation-interleave + expiry tests in `internal/withdrawal/revocation_integration_test.go` (R7 three cases; lock-wait expiry → 403; `expires_at == t_check` expired; post-check window accepted-residual)
- [ ] T026 [US5] Storage-failure + unknown-commit tests in `internal/withdrawal/failure_integration_test.go` (503 shape + same-key-retry instruction text; never "definitely not created"; deadlock/timeout → 503 retryable, never mis-mapped to 23505; 401-no-audit-row assertion per data-model.md Table 5 C3/N1 rule: unauthenticated rejects write zero Table 5 rows; authenticated pre-tx rejects write exactly one best-effort row on the success path, zero-or-one (never duplicated) on write-failure/cancel paths)

**Checkpoint**: Fault behavior proven (loss/crash/outage all converge without duplicates)

---

## Phase 8: User Story 6 — 恢复期行为 (Priority: P2)

**Goal**: Receive-during-recovery + zero execution side effects + honest query signal

**Independent Test**: V8 — active-recovery POST persists with zero nonce/sign/broadcast artefacts; GET servable with snapshot `state`; read-kill → `unknown`; POST-loss-retry → same 200 (FR-16/FR-17, SC-06/SC-07)

- [ ] T027 [US6] Recovery-period intake test in `internal/withdrawal/recovery_period_integration_test.go` (Anvil E2E tags; V8 incl. absence-assertions: no rows outside 007 scope, no RPC broadcast; owns the file — T028 appends its cases after T027 lands)
- [ ] T028 [US6] Recovery-snapshot query tests in `internal/withdrawal/recovery_period_integration_test.go` (single-snapshot precedence incl. release-then-re-establish concurrency; unknown on either-failure; `execution:not_started` constant; NO height-indexed validity call; appends to T027's file, no `[P]` with T027)
- [ ] T029 [P] [US6] 006-governance read-only assertion in `internal/withdrawal/recovery_governance_integration_test.go` (tags: integration; depends on Foundational: no pause/de-auth/isolation rows written or deleted by any 007 path — `indexer_pause`/`log_pause`/`deposit_pause`/`reorg_recovery` untouched; downstream preconditions subset observed per 006 `contracts/downstream.md`; V8 governance half)

**Checkpoint**: 006 contract honored structurally (receive-only holds under recovery)

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Regression, CI, docs, runbook — no new behavior

- [ ] T030 [P] 002–006 regression gate (tags: unit + integration): `go test ./...` + `go test -tags integration` green. Diff gate (explicit allowlist, reviewed with the test evidence — a file being listed does NOT bless arbitrary changes inside it): ALLOWED-NEW `internal/withdrawal/**`, `internal/app/withdrawalauthz.go`, `internal/app/apikeyauth.go` (+ their `*_test.go` / `*_integration_test.go`), `migrations/000007_withdrawal_creation.sql`, `specs/007-withdrawal-creation/**`; ALLOWED-MODIFY `cmd/txharbor/main.go` (dispatch cases only), `internal/app/serve.go` (route mount only), `internal/app/serve_config_test.go` (T035 config unit test only), `internal/config/config.go` (chain-bind passthrough only, no new secret knobs), `internal/metrics/**` (new counters only). Any diff to `internal/indexer/**`, `internal/db/**`, `migrations/000001–000006`, or the shared `writeGuard`/lease/coordinator constants → STOP, explicit review required, full regression re-run. Read-only consumption of 003/004 whitelist sources (T020) and 006 recovery rows/events (T011/T028/T029) needs no gate exception — no files change. This gate checks for unintended drift; it does NOT substitute for the V-matrix behavior regression (T033). Historical CI 34918673432 cited as 006 baseline only.
- [ ] T031 [P] Lint/vet/build gate: `gofmt` + `go vet -tags integration ./...` + `go build ./...` per Makefile (existing `ci.yml` four jobs, no workflow change)
- [ ] T032 Operator runbook in `specs/007-withdrawal-creation/quickstart.md` (append §操作 runbook, 005 T029 precedent — no new contracts file): `withdrawal-authz` mint→supply→revoke flows + `apikey-auth` issue→rotate→revoke flows, O capture rule, exit codes 0/1/2, DSN trust root, uncertain-COMMIT same-O retry rule + V1–V9 checklist execution record (depends on T009, T034; no new behavior)
- [ ] T033 Full V-matrix validation run + FR/SC mapping sign-off (SC-01–SC-08 each tied to a green test; T000-P recorded still-open)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — T001 first, then T002–T004 in parallel [P]; T035 (config passthrough) after T001, blocks T014
- **Foundational (Phase 2)**: Depends on Phase 1 — T005→T006 migration chain first (table shapes + exact `ConstraintName` declarations needed by all carriers); then T007+T008 in parallel [P] (different files: `auth.go` / `grant.go`, neither imports the other); then T009 (depends on T008), T010 (depends on T007 + T008), T011 (depends on T007), T034 (depends on T007, blocks T018). T009/T010/T011/T034 carry NO `[P]` — each has a same-phase dependency. T010/T011 compile + unit-level classify tests green is part of the Foundation checkpoint, not deferred to stories.
- **User Stories (Phase 3–8)**: All depend on Foundational COMPLETE (checkpoint green); US1 (MVP) → US2 → US3 → US4 → US5 → US6 recommended order, US2/US3 test files can start in parallel once T007/T010 interfaces are frozen
- **Polish (Phase 9)**: Depends on all stories; T030–T031 [P] in parallel (different concerns: regression vs lint; both read-only gates), then T032 (depends on T009 + T034), then T033

### Within-story order

- Tests first (FAIL before implementation), then implementation, then checkpoint validate
- Migration/constraints before tx logic; tx logic before HTTP wiring; wiring before runbook

### Parallel opportunities

- T002, T003, T004 [P] after T001
- T007 + T008 [P] after T005+T006 (different files: auth.go / grant.go, neither imports the other — the ONLY same-phase parallel pair in Foundation); T009/T010/T011/T034 follow their stated dependencies, never in parallel with their prerequisites
- T030 + T031 [P] in Polish (different concerns: regression gate vs lint gate)

---

## FR/SC/V Trace Matrix

| FR | Tasks | SC | V |
|---|---|---|---|
| FR-01 (endpoints+auth) | T007, T014, T035 | SC-01 | V1 |
| FR-02 (identity/permission) | T007, T018, T034 | SC-02 | V2 |
| FR-03 (API key) | T007, T034, T015, T018 | SC-02/08 | V2, V7 |
| FR-03b (grant model) | T008, T009, T023, T025 | SC-04/05 | V3, V6, V7 |
| FR-04/FR-05 | T003, T020, T035 (chain bind) | SC-03 | V4 |
| FR-06/FR-07 | T003, T004, T019 | SC-03/08 | V4 |
| FR-08 (Accepted) | T005 (status CHECK), T006 (CHECK negative), T014 | SC-01/06 | V1, V8 |
| FR-09/FR-10 | T010, T021, T022 | SC-04 | V5, V6 |
| FR-11 (permanent) | T005, T006, T021 | SC-04/05 | V5–V7 |
| FR-12/FR-13 | T010, T021, T024, T026 | SC-05 | V6 |
| FR-14/FR-15 | T002, T013, T016, T017 | SC-02 | V1–V3, V9 |
| FR-16/FR-17 | T011, T027, T028, T029 | SC-06/07 | V8 |
| FR-18/FR-19 | T008, T009, T029 | — | V3 + review |
| FR-20/FR-21 | T002, T015 | SC-08 | V1–V9 |
| 006 inputs (read-only) | T011, T028, T029 | SC-06/07 | V8 |
| Downstream 008–011 | — (explicitly excluded) | — | — |

---

## Implementation Strategy

### MVP First (US1 only)

1. Phase 1 + Phase 2 → foundation (migration applies, carriers compile)
2. Phase 3 (US1) → create→persist→self-query round-trip green
3. **STOP and VALIDATE** before US2

### Incremental delivery

- Foundation → US1 (MVP) → US2 (security) → US3 (validation) → US4 (idempotency) → US5 (faults) → US6 (recovery) → Polish
- Each phase independently buildable (`go build ./...`) and verifiable (its V-scenarios)

---

## Notes

- [P] = different files, no dependencies; story label = traceability
- Commit after each task or logical group (small, reviewable batches per constitution XIV)
- T000-P stays open; 006 evidence (merge 8e1a440, CI 34918673432) is baseline context only
- No 008–011 tasks; no future execution modules
