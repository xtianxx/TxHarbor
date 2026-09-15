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
- [ ] T003 [P] Implement shared validators in `internal/withdrawal/validate.go` (FR-04 chain bind; FR-05 whitelist hook; FR-06 `[1-9][0-9]*` shape + `math/big` ≤ 2²⁵⁶−1; FR-07 EIP-55 + lowercase canonicalization; FR-09 key shape 1–128 ASCII 0x21–0x7E) with unit vectors
- [ ] T004 [P] Unit tests for validators in `internal/withdrawal/validate_test.go` (V4 matrix: wrong chain, non-whitelist, bad shape, mixed-case fail/pass, "0"/"00123"/"-5"/"1.5"/non-digits/>uint256, max-uint256 accept, max+1 reject)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Migration + constraint carriers + auth/grant primitives every story needs. MUST complete before ANY user story.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T005 Write `migrations/000007_withdrawal_creation.sql` (Tables 1–6 per data-model.md: `caller`, `api_key`, `withdrawal_requests` with `caller_key_uniq` + `authorization_uniq` + amount `CHECK(amount >= 1 AND amount <= 2²⁵⁶−1 literal)`, `withdrawal_authorizations`, `withdrawal_request_audit`, `withdrawal_grant_audit` with `UNIQUE(operation_id)`; pure DDL, goose sequence)
- [ ] T006 Migration verification: goose up/down reproducibility on scratch DB + CHECK/UNIQUE negative tests + 000001–000006 untouched-meaning check (FR-11/FR-12 carriers; T000-P unaffected)
- [ ] T007 [P] API-key credential core in `internal/withdrawal/auth.go` (R1–R2: `txh_` + 32B CSPRNG generation, sha256-hex storage, `UNIQUE(key_hash)`, constant-time compare, single-statement revocation predicate, `caller_id` derivation, key_id+prefix-only logging)
- [ ] T008 [P] Grant-supply core in `internal/withdrawal/grant.go` (R9: `mint` + `supply`/`revoke` statement scripts, Table 6 attempt semantics with caller-supplied `operation_id`, op-input 8-field binding + compare, PK-race bounded re-execution, uncertain-COMMIT same-O recovery; `operator`/`reason` as retry metadata)
- [ ] T009 [P] `withdrawal-authz` operator subcommand in `internal/app/withdrawalauthz.go` mirroring `confirm-auth` carrier (`internal/app/confirmauth.go:16-35`: flag → config.Load → operator-connection tx; REQUIRED `--operation-id`; exit codes 0/1/2; in-process testable)
- [ ] T010 Intake transaction core in `internal/withdrawal/intake.go` (R7 FINAL + R8: pre-tx classify, BEGIN→writeGuard→`FOR SHARE` grant row→`clock_timestamp()`→validity strict-`>`→plain INSERT→audit→COMMIT; 23505 fixed-order classify incl. T-dual-race; commit-unknown re-classify; N1 PK-branch O-first re-read)
- [ ] T011 Query core in `internal/withdrawal/query.go` (ownership-enforced read, identical-404 shape, single-snapshot recovery annotation per contracts/api.md §3: REPEATABLE READ row+event, precedence rule, unknown on either-failure, `execution:not_started`, NO height-indexed validity)

**Checkpoint**: Foundation ready — migration applies cleanly, all carriers exist, user stories can begin

---

## Phase 3: User Story 1 — 正常创建与本人查询 (Priority: P1) 🎯 MVP

**Goal**: Authenticated authorized caller creates a compliant request (201) and reads it back (200, identical content)

**Independent Test**: V1 — valid body → 201 + `request_id` + `status:accepted`; same-key GET → identical body with `recovery:{state:none,execution:not_started}` (FR-01, FR-08, SC-01)

- [ ] T012 [P] [US1] Integration test for create+self-query in `internal/withdrawal/intake_integration_test.go` (tags: integration; V1 happy path incl. amount-string round-trip, SC-01/SC-08)
- [ ] T013 [P] [US1] Contract test for POST/GET shapes + error codes in `internal/withdrawal/contract_test.go` (201/200 bodies per contracts/api.md §§1/3)
- [ ] T014 [US1] Wire POST /withdrawals + GET /withdrawals/{id} on existing `http.Server` in `internal/app/` (plan wiring; no new listener; caller_id from key row only) (depends on T010, T011)
- [ ] T015 [US1] Structured logging + metrics on existing registries (caller/request/chain/asset/retry fields, zero secrets via `logx.Redact`; FR-20/FR-21)

**Checkpoint**: US1 fully functional and testable independently (create → persist → self-query round-trip green)

---

## Phase 4: User Story 2 — 认证/授权/越权 (Priority: P1)

**Goal**: Unauthenticated/unauthorized/cross-caller requests rejected without leakage or persistence

**Independent Test**: V2 + V9 — no/bad/revoked key → 401; `can_create=FALSE` → 403; forged caller_id ignored; B-reads-A → byte-identical 404 (FR-02, FR-03, FR-15, SC-02)

- [ ] T016 [P] [US2] Integration test for auth matrix in `internal/withdrawal/auth_integration_test.go` (V2: 401/403 paths, zero rows asserted, rotation preserves caller_id + scope)
- [ ] T017 [P] [US2] Integration test for cross-caller privacy in `internal/withdrawal/privacy_integration_test.go` (V9: 404 byte-equality random-id vs foreign-id)
- [ ] T018 [US2] Key issuance/rotation/revocation operator flow + startpoint-semantics tests (R4/R5: grace-window dual-accept, post-startpoint revoke affects next attempt only; V7 key half)

**Checkpoint**: US1 AND US2 both work independently (auth boundary holds, privacy shape exact)

---

## Phase 5: User Story 3 — 参数校验 (Priority: P1)

**Goal**: All illegal params rejected pre-persist with typed errors

**Independent Test**: V4 — full illegal matrix → 400/422 per contract, zero rows; EIP-55-mixed-case accepted + lowercased (FR-04–FR-07, SC-03)

- [ ] T019 [P] [US3] Integration test for param matrix in `internal/withdrawal/validation_integration_test.go` (V4 incl. `1.5`/`1e3` decimal-barrier, max/max+1 boundary both directions per §四)
- [ ] T020 [US3] Whitelist enforcement wiring against 003/004 policy source in `internal/withdrawal/validate.go` (FR-05; no redefinition of asset list)

**Checkpoint**: Validation airtight at API + Go + DB layers (three-layer amount story green)

---

## Phase 6: User Story 4 — 幂等语义 (Priority: P1)

**Goal**: Same-key replay/conflict + cross-caller isolation exact

**Independent Test**: V5 — same-key-equal → 200 same `request_id` row-count-unchanged; same-key-differ → 409 original-untouched; cross-caller same key → independent 201s (FR-09/FR-10, SC-04)

- [ ] T021 [P] [US4] Concurrency test same-key N-way in `internal/withdrawal/idempotency_integration_test.go` (exactly-1-row, single `request_id` to all; V5 + V6 first half)
- [ ] T022 [P] [US4] Cross-key same-grant + cross-caller isolation test in `internal/withdrawal/idempotency_integration_test.go` (403-path, original untouched; caller scoping)
- [ ] T023 [US4] Same-O op-conflict + grant-PK three-outcome tests in `internal/withdrawal/grant_integration_test.go` (§二 i/ii/iii: one-grant-one-supplied + per-O audits;异参 loser `supply_refused`; never `operation_conflict`/503 for PK race)

**Checkpoint**: Idempotency exact under concurrency (no duplicate intents constructible)

---

## Phase 7: User Story 5 — 并发/丢失/重启/存储失败 (Priority: P1)

**Goal**: Real-fault correctness: loss, unknown-commit, crash, outage

**Independent Test**: V6 remainder — kill-9 mid-response → same-key 200; restart → 200; storage-down → 503 zero-Accepted; uncertain-COMMIT → re-classify (FR-12/FR-13, SC-05)

- [ ] T024 [P] [US5] Crash/restart/retry test in `internal/withdrawal/recovery_integration_test.go` (kill-9, restart, same-O grant recovery per Table 6; O-capture crash semantics)
- [ ] T025 [P] [US5] Revocation-interleave + expiry tests in `internal/withdrawal/revocation_integration_test.go` (R7 three cases; lock-wait expiry → 403; `expires_at == t_check` expired; post-check window accepted-residual)
- [ ] T026 [US5] Storage-failure + unknown-commit tests in `internal/withdrawal/failure_integration_test.go` (503 shape + same-key-retry instruction text; never "definitely not created"; deadlock/timeout → 503 retryable, never mis-mapped to 23505)

**Checkpoint**: Fault behavior proven (loss/crash/outage all converge without duplicates)

---

## Phase 8: User Story 6 — 恢复期行为 (Priority: P2)

**Goal**: Receive-during-recovery + zero execution side effects + honest query signal

**Independent Test**: V8 — active-recovery POST persists with zero nonce/sign/broadcast artefacts; GET servable with snapshot `state`; read-kill → `unknown`; POST-loss-retry → same 200 (FR-16/FR-17, SC-06/SC-07)

- [ ] T027 [P] [US6] Recovery-period intake test in `internal/withdrawal/recovery_period_integration_test.go` (Anvil E2E tags; V8 incl. absence-assertions: no rows outside 007 scope, no RPC broadcast)
- [ ] T028 [US6] Recovery-snapshot query tests in `internal/withdrawal/recovery_period_integration_test.go` (single-snapshot precedence incl. release-then-re-establish concurrency; unknown on either-failure; `execution:not_started` constant; NO height-indexed validity call)
- [ ] T029 [US6] 006-governance read-only assertion (no pause/de-auth/isolation rows written or deleted by any 007 path; downstream preconditions subset observed)

**Checkpoint**: 006 contract honored structurally (receive-only holds under recovery)

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Regression, CI, docs, runbook — no new behavior

- [ ] T030 [P] 002–006 regression gate: `go test ./...` + `go test -tags integration` green with zero 002–006 file modifications outside plan wiring points (diff-gated; historical CI 34918673432 cited as 006 baseline only)
- [ ] T031 [P] Lint/vet/build gate: `gofmt` + `go vet -tags integration ./...` + `go build ./...` per Makefile (existing `ci.yml` four jobs, no workflow change)
- [ ] T032 Runbook + operator docs for `withdrawal-authz` (mint→supply→revoke flows, O capture rule, exit codes) + quickstart V1–V9 checklist execution record
- [ ] T033 Full V-matrix validation run + FR/SC mapping sign-off (SC-01–SC-08 each tied to a green test; T000-P recorded still-open)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — T001 first, then T002–T004 in parallel [P]
- **Foundational (Phase 2)**: Depends on Phase 1 — T005→T006 migration chain first; T007–T011 in parallel [P] after T005 (table shapes needed); T011 needs T007
- **User Stories (Phase 3–8)**: All depend on Foundational; US1 (MVP) → US2 → US3 → US4 → US5 → US6 recommended; US2/US3 test files can start in parallel once T007/T010 land
- **Polish (Phase 9)**: Depends on all stories; T030–T031 [P] in parallel, then T032–T033

### Within-story order

- Tests first (FAIL before implementation), then implementation, then checkpoint validate
- Migration/constraints before tx logic; tx logic before HTTP wiring; wiring before runbook

### Parallel opportunities

- T002, T003, T004 [P] after T001
- T007, T008, T009 [P] after T005 (different files: auth.go / grant.go / withdrawalauthz.go)
- Story test files [P] within each story phase
- US2/US3/US4 test authoring can overlap once foundation interfaces are fixed

---

## FR/SC/V Trace Matrix

| FR | Tasks | SC | V |
|---|---|---|---|
| FR-01 (endpoints+auth) | T007, T014 | SC-01 | V1 |
| FR-02 (identity/permission) | T007, T018 | SC-02 | V2 |
| FR-03 (API key) | T007, T009, T015, T018 | SC-02/08 | V2, V7 |
| FR-03b (grant model) | T008, T009, T023, T025 | SC-04/05 | V3, V6, V7 |
| FR-04/FR-05 | T003, T020 | SC-03 | V4 |
| FR-06/FR-07 | T003, T004, T019 | SC-03/08 | V4 |
| FR-08 (Accepted) | T014 | SC-01/06 | V1, V8 |
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
- Commit after each task or logical group per batch-closeout rule (§四)
- T000-P stays open; 006 evidence (merge 8e1a440, CI 34918673432) is baseline context only
- No 008–011 tasks; no future execution modules
