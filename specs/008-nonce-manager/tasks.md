# Tasks: 008 Nonce Manager

**Input**: Design documents from `/specs/008-nonce-manager/`

**Prerequisites**: plan.md (Go 1.26.5, pgx v5.11.0, go-ethereum v1.17.5, goose v3.28.0; no new dependency, no new infrastructure), spec.md (FR-01–FR-23, US1–US5, SC-01–SC-09), research.md (R1–R13), data-model.md (7 tables + transaction catalog + concurrency argument + classification matrix), contracts/read-api.md, contracts/observation.md, contracts/downstream.md, quickstart.md (V1–V13), `.specify/memory/constitution.md` 1.1.0

**Tests**: Included per story — spec mandates failure-path/concurrency coverage (constitution IX/XI) and quickstart §Scenario matrix is the acceptance surface. Unit tests run under `make test`; integration/E2E under `make test-integration` (`go test -tags integration`; testcontainers PostgreSQL `18.6-trixie` + real Anvil `ghcr.io/foundry-rs/foundry:v1.8.1`, chain 31337; precedents `internal/db/migrate_integration_test.go`, `internal/indexer/logscan_integration_test.go:689` (`logscanStartAnvilNode`; also reused by reorg-recovery/confirmation/deposit integration tests), `internal/app/serve_integration_test.go` (`startAnvilContainer`)).

**Organization**: Grouped by user story (US1–US5, spec priority order). FR/SC/V trace per phase. 008 owns and executes only its **Provider** side; the 009 consumer client, 010 attempt linkage and 011 intent existence are deferred (see §Final Executor & Deferred Acceptance).

## Preconditions & evidence discipline (recorded before authoring this file)

- `git status` on branch `008-nonce-manager` at HEAD baseline `4e7f7d3` is clean (recorded when tasks were first authored at `46d2c63`; corrected to the HEAD timepoint). `.specify/scripts/bash/setup-tasks.sh --json` was run from this worktree root (the repo root for this stage) and returned `FEATURE_DIR=specs/008-nonce-manager` with `AVAILABLE_DOCS=research.md, data-model.md, contracts/, quickstart.md`; the tasks template was resolved from `.specify/templates/tasks-template.md`. This repo config defines **no `before_tasks` hook**, and the optional hook is skipped because the worktree is clean — recorded here, not silently omitted.
- Every task below is **unstarted** (`- [ ]`). plan.md / research.md / data-model.md / contracts/ / quickstart.md and any earlier probe output are **design artifacts, not completion evidence**. Cited historical CI runs / prior probes are baseline context only.
- Test doubles (fake RPC, scripted counts) are allowed for **early unit development only** and are marked `EARLY-VALIDATION` in-task; the real concurrency/restart/recovery acceptance MUST run against real PostgreSQL + real Anvil (constitution XI; workflow R5), and every such task is listed separately in its own story.
- Upstream 006 (`8e1a440`) and 007 (`19fa11e`) are **read-only inputs**: no task modifies `migrations/000001–000007`, `internal/indexer/**`, `internal/withdrawal/**`, or any 006/007 table row. T000-P stays open.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel — different files, no dependency on another task in the same phase
- **[Story]**: which user story (US1–US5); Setup/Foundational/Polish carry none
- Every task line ends with: prerequisites/external dependency, mapped FR / contract § / V-scenario, and an executable completion condition (`done:`)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Package skeleton, outcome vocabulary, shared numeric helpers — no business behavior

- [ ] T001 Create `internal/nonce/` package skeleton per plan.md §Project Structure: `allocate.go`, `binding.go`, `classify.go`, `observe.go`, `reconcile.go`, `hold.go`, `registry.go`, `readapi.go`, `admin.go`, `numeric.go`, plus `errors.go` and `coord.go` (one added file beyond plan's illustrative list: shared coordination/gate primitives, one-concern-per-file per the `internal/indexer`/`internal/withdrawal` layout) — doc comments only, no behavior. deps: none | plan.md | done: `go build ./...` and `go vet ./internal/nonce/...` green
- [ ] T002 [P] Domain outcome/error vocabulary in `internal/nonce/errors.go` with the exact machine strings from contracts/downstream.md §1 (`allocated`, `replayed`, `allocation_conflict`, `chain_view_unavailable`, `scope_held`, `sender_not_registered`, `sender_disabled`, `authorization_invalid`, `rebuild_incomplete`, `recovery_active`, `temporarily_unavailable`) and contracts/observation.md §3.4/§4 (`operation_conflict`) plus contracts/read-api.md §2/§3 read outcomes (`bound`, `terminal`, `not_bound`, `mismatch`, `unavailable`); + `internal/nonce/errors_test.go`. deps: T001 | FR-14/FR-17; downstream.md §1; observation.md §3.4/§4; read-api.md §2/§3 | done: unit test pins each string exactly (`go test ./internal/nonce -run TestOutcome`)
- [ ] T003 [P] Numeric helpers in `internal/nonce/numeric.go` (R13): `math/big` parse/bounds, JSON-RPC hex → integer with explicit overflow refusal, `NUMERIC(78,0)` range `0 ≤ n ≤ 2⁶⁴−1`, decimal/hex transport helpers; zero `float64` anywhere; + `internal/nonce/numeric_test.go` (0 and 2⁶⁴−1 accepted; 2⁶⁴, negative, non-decimal refused). deps: T001 | FR-01/FR-02; R13; V13 | done: `go test ./internal/nonce -run TestNumeric` green

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Migration, coordination discipline, binding/registry/hold/classify/observe carriers, admission core, read provider, operator library, subcommand, config — every story needs these. MUST complete before ANY user story.

**⚠️ CRITICAL**: No user story work can begin until this phase reaches its checkpoint.

- [ ] T004 Write `migrations/000008_nonce_manager.sql` (data-model.md Tables 1–7: `nonce_wallet_registry`, `nonce_scope_state`, `nonce_bindings`, `nonce_binding_events`, `nonce_observations`, `nonce_scope_holds`, `nonce_ops_audit`; pure DDL, goose numbered, no stored functions; every `ConstraintName` consumed by classification explicitly declared — `nonce_bindings_pkey`, `nonce_bindings_intent_uniq`, `nonce_bindings_scope_nonce_uniq`, `nonce_bindings_registry_fkey`, `nonce_bindings_state_check`, `nonce_bindings_terminal_consistency`, `nonce_bindings_nonce_range`, `nonce_bindings_intent_shape`, `nonce_wallet_registry_pkey`, `nonce_scope_state_pkey`, `nonce_scope_state_registry_fkey`, `nonce_binding_events_binding_to_uniq`, `nonce_scope_holds_status_consistency`, `nonce_scope_holds_registry_fkey`, `nonce_ops_audit_pkey`, `nonce_ops_audit_operation_id_uniq`; `NUMERIC(78,0)` bounds `0…2⁶⁴−1`). deps: none (Phase 1 may proceed in parallel) | FR-01/02/03/20/23; data-model.md Tables 1–7 | done: `go build ./...` green; `migrations/embed.go` auto-includes the file
- [ ] T005 Migration verification in NEW `internal/nonce/migration_integration_test.go` (tags: integration; raw-SQL probes only — MUST NOT import the `internal/nonce` classifiers): (a) `git diff --name-only -- migrations/000001_baseline.sql migrations/000002_chain_indexer.sql migrations/000003_event_indexing.sql migrations/000004_deposit_detection.sql migrations/000005_confirmation_tracking.sql migrations/000006_reorg_recovery.sql migrations/000007_withdrawal_creation.sql` empty; (b) isolated scratch DB baseline→000008 `goose up` green + `goose status` clean, then 000008 `down`/`up` reproducible; (c) negative inserts: dup `intent_id` → 23505 `nonce_bindings_intent_uniq`; dup `(chain_id,sender,nonce)` → 23505 `nonce_bindings_scope_nonce_uniq`; dup `operation_id` → 23505 `nonce_ops_audit_operation_id_uniq`; dup `(binding_id,to_state)` → 23505 `nonce_binding_events_binding_to_uniq`; unregistered sender → 23503 `nonce_bindings_registry_fkey`; CHECK violations (nonce > 2⁶⁴−1, terminal-consistency) → 23514 (never 23505); (d) each elicited `ConstraintName` equals the migration's declaration (name probe only — app-level classify belongs to T012/T022/T038). deps: T004 | FR-02/03/20/23; data-model.md named carriers | done: `go test -tags integration -run TestNonceMigration ./internal/nonce` green
- [ ] T006 [P] Coordination + 006 gate primitives in `internal/nonce/coord.go`: open every 008 write tx with the repo's single coordination discipline — `BEGIN → SET LOCAL statement_timeout='5s' → ensure coordination row → SELECT … FOR UPDATE → post-lock rechecks → COMMIT` (shape per `internal/indexer/reorgcommit.go` `lockRecoveryChain`); 008 never acquires/renews the lease and never writes `indexer_lease`; read-only 006 gate reads (`indexer_pause`/`log_pause`/`deposit_pause` existence + active `reorg_recovery` row); scope-row `FOR UPDATE` helper for `nonce_scope_state`. NOTE: `internal/indexer` constants are package-private and touching `internal/indexer/**` is out of scope, so the identical 5-step statements are declared locally in `coord.go` — the invariant is the same coordination row + `FOR UPDATE` lock, not identifier reuse. The local copy is accepted (no cross-package coupling), and `coord_test.go` carries a drift-guard assertion pinning the local guard literal to the repo's shared 5 s statement guard value so the copy and the indexer cannot silently diverge. + `internal/nonce/coord_test.go` (unit: lock-order framing, gate-hit refusal with zero writes, local-guard equivalence assertion). deps: T001 | R1/R4; FR-14/FR-15; observation.md §2.1 | done: unit green (incl. the equivalence assertion); zero writes to 006 tables
- [ ] T007 [P] Binding model + explicit state machine + append-only event writers in `internal/nonce/binding.go` (states `allocated → in_flight → consumed | released`; direct `allocated→consumed` and `allocated|in_flight→released` allowed; `consumed`/`released` terminal with no path out; terminal-consistency + nonce-range guards mirrored from `nonce_bindings_*`; event writer converging on `nonce_binding_events_binding_to_uniq`; readers by `binding_id` / `intent_id`; read-only rebuild verification reader `VerifyRebuild` per known scope). + `internal/nonce/binding_test.go` (unit: legal/illegal transitions, terminal immutability, no automatic path to `released`). deps: T001 | R6; FR-03/05/09/13/20; V4/V13 | done: `go test ./internal/nonce -run TestBinding` green
- [ ] T008 [P] Wallet registry reads/versioning in `internal/nonce/registry.go` (read `nonce_wallet_registry` by `(chain_id,sender)`; `active`/`disabled` gates **admission only**; return `registry_seq`; a later config change never alters an existing binding). + `internal/nonce/registry_test.go`. deps: T001 | R9; FR-01; V10 | done: unit green
- [ ] T009 Holds + scope frontier in `internal/nonce/hold.go` (establish a `nonce_scope_holds` row per cause from a persisted observation with `evidence_observation_id`; a scope is admission-held iff ≥1 `active` row; monotonic floor `UPDATE … SET reconciled_floor = GREATEST(COALESCE(reconciled_floor,0), $new)` guarded; `last_latest`/`last_pending`/`last_observation_id` updates; readers for active holds + scope state). + `internal/nonce/hold_test.go` (unit: multi-cause coexistence, no scope-level boolean, released cause re-detected ⇒ NEW hold instance). deps: T001,T007 | R7; FR-08/FR-11; V5/V7 | done: unit green
- [ ] T010 [P] Classification matrix in `internal/nonce/classify.go` (data-model §Observation classification matrix as a pure function of `M`, `F`, `L`, `P`, `P_prev`: `unavailable` on read failure; `divergence` on `L > P` or pending regression; `consistent` no-bindings/no-floor/`P=0` → candidate `0`; `bootstrap_external_consumed` first scope `0 < P`, `L <= P` → candidate `P`; `consistent` bindings `P <= M+1` → candidate `max(M+1, F)`; `unattributed_consumption` `P > M+1 && L > M+1`; `unexplained_gap` `P > M+1 && L <= M+1` — the last two hold+refuse and NEVER derive `max(M+1, P)`). + `internal/nonce/classify_test.go` FULL boundary matrix. deps: T003 | R2/R4; FR-10/11/12; V5/V6 | done: matrix unit green incl. every refusal/hold branch
- [ ] T011 Chain observation in `internal/nonce/observe.go` (R4: outside any DB transaction, `eth_getTransactionCount(sender,"latest")` + `("pending")` + `eth_getBlockByNumber("latest")` head number/hash; reuse the `internal/eth` error classification `KindTransport`/`KindTimeout`/`KindRateLimited`/`KindInvalidResponse`/`KindChainMismatch`/`KindNotFound` and the existing `IndexRPCTimeout`/`IndexRetryInitial`/`IndexRetryMax` knobs — no new timing knob; one `nonce_observations` row per attempt, `unavailable` + error class persisted on any failure; **zero RPC inside any DB tx**). + `internal/nonce/observe_test.go` (EARLY-VALIDATION fake RPC only — real RPC is covered by T027/T030). deps: T003,T010 | R4; FR-10/12/15; V5/V13 | done: unit green
- [ ] T012 Admission transaction core in `internal/nonce/allocate.go` (data-model §Transaction catalog: **T-allocate** pre-tx input validation → pre-tx observation → BEGIN → T006 lock → post-lock rechecks (006 gate rows, registry `state`/`registry_seq`, active holds, binding-by-`intent_id`, recompute `M`/`F` from `nonce_bindings` under the lock) → classify (T010) → insert observation always → consistent: insert binding + creation event, else commit observation (+ hold when the matrix says so) with no binding; **T-replay** input equality returns the original binding untouched; **T-conflict** any differ → fail-closed `allocation_conflict`, zero second binding; **T-converge** on 23505: ROLLBACK then off-pool fixed-order classify by `intent_id` first then `(chain_id,sender,nonce)`, exact `ConstraintName` match only, miss retryable; authorization validation takes `SELECT … FOR SHARE` on the 007 `withdrawal_authorizations` row under the fixed order scope row → 006 gate reads → 007 authorization row → own rows (a `RevokeGrant`/`SupplyGrant` committing before the share lock is observed and refused; one racing the admission blocks until commit; 008's own admission owns this approved-admission/revocation serialization — 009/011 re-checks are defense-in-depth, never a substitute), checks `state='active'`, not expired by `clock_timestamp()`, chain match, storing `authorization_id` + `authorization_version` SHA-256; 008 writes zero 007 rows). + `internal/nonce/allocate_test.go` (unit classify/replay/conflict ordering). deps: T006,T007,T008,T009,T010,T011 | R1/R2/R3/R5/R10; FR-01/02/03/04/05/15/17; V1/V2/V11 | done: unit green + `go build ./...` green
- [ ] T013 Read provider in `internal/nonce/readapi.go` (contracts/read-api.md §2/§3/§4: `GET /nonce/bindings/{binding_id}` and `GET /nonce/bindings/by-intent/{intent_id}?chain_id=&sender=`; exactly one of five outcomes `bound`/`terminal`/`not_bound` 404/`mismatch` 409/`unavailable` 503; `bound`/`terminal` carry the contract body + `annotations` (`gate` with every active hold id/cause/established_at, `recovery` state, `registry_state`) and the fixed `notice`; one `REPEATABLE READ` read-write tx taking `SELECT … FOR SHARE` on the scope `nonce_scope_state` row first (when it exists) then the snapshot reads, zero modification; 008 read failure → all-or-nothing `unavailable`, 006 read failure → `recovery.state="unknown"` never `none`/`released`; bearer `TXHARBOR_NONCE_READ_TOKEN` constant-time compare → 401 `unauthenticated`; snake_case JSON, nonce/counts decimal strings, addresses lowercase hex). + `internal/nonce/readapi_test.go` (unit outcome mapping + body shape). deps: T006,T007,T008 | R8; FR-01/11/14/19; read-api.md §1–§4; V9 | done: `go test ./internal/nonce -run TestReadAPI` green
- [ ] T014 Operator transaction library in `internal/nonce/admin.go` (contracts/observation.md §3, all under the T006 lock: **T-hold-release** `SELECT … FOR UPDATE` the named hold (missing/already released ⇒ `nop` audit, zero change) → re-verify fresh pre-tx observation `consistent`, no unresolved scope conflicts, and the exact evidence-version set (named hold still `active`, operator `--observation-id` belongs to the scope, current `registry_seq`/`state`, current active-hold set, 006 state read-only) → clear ONLY the named hold + `UPDATE nonce_scope_state SET reconciled_floor = GREATEST(COALESCE(...), $observed_pending)` + `applied` audit in one tx; any drift/insufficiency → `refused` audit + zero hold/floor change; **T-binding-release** same carrier per binding, non-terminal only, no-side-effect evidence mandatory (timeout/connection error alone is never evidence); **T-registry** `register`/`disable` with `registry_seq + 1` guarded `RowsAffected`; operation-id dedup via `nonce_ops_audit_operation_id_uniq` (23505 → rollback → re-read by op-id → equal op-input: report recorded outcome; differ: `operation_conflict`, zero writes)); + `internal/nonce/admin_test.go` (unit: refusal/version-drift/op-input equality). deps: T006,T007,T008,T009 | R7; FR-08/14; observation.md §3/§4; V7/V8/V10/V12 | done: `go test ./internal/nonce -run TestAdmin` green
- [ ] T015 `txharbor nonce-admin` operator subcommand in `internal/app/nonceadmin.go` + one dispatch case in `cmd/txharbor/main.go` (mirrors `internal/app/withdrawalauthz.go`/`confirmauth.go`: flag parse → `config.Load` → operator-connection tx; actions `mint` (pure entropy, no config/DB) / `hold-release` / `binding-release` / `register` / `disable` / read-only `status`; `--operation-id` REQUIRED and minted first; `--chain-id` deployment bind and `--sender` scope bind before connecting; exit codes 0/1/2; no new role, no admin UI, no HTTP write) + `internal/app/nonceadmin_test.go` (in-process usage/exit codes). deps: T014 | FR-08; observation.md §3 | done: unit green + `go build ./...` green
- [ ] T016 Config passthrough in `internal/config/config.go` (add `TXHARBOR_NONCE_READ_TOKEN`; reconcile/observation timing reuses the existing `IndexRPCTimeout`/`IndexPollInterval`/`IndexRetryInitial`/`IndexRetryMax` — NO new timing knobs; the token is never echoed raw in errors/`Summary()` and stays redacted) + NEW `internal/config/config_008_test.go` (`serve_config_test.go` unmodified). deps: T001 | FR-19/FR-21; read-api.md §1 | done: config unit green
- [ ] T017 `internal/app/serve.go` extension: mount the read endpoints on the existing mux next to `/withdrawals` (no new listener), and run the startup rebuild verification gate (R5: read-only per-known-scope integrity verification via `VerifyRebuild`; until success every allocation refuses `rebuild_incomplete`; verification failure keeps the gate closed with a structured error — no silent repair, no memory guess). deps: T013,T016 | R5; FR-13/FR-19; V3/V9 | done: `go build ./...` green; gate wired through `Serve`

**Checkpoint**: Foundation ready — 000008 applies cleanly, T005 verification green, all carriers compile with frozen interfaces, T012/T013/T014 unit-level classify/outcome behavior green. Story V-scenarios remain with their stories, never claimed here.

---

## Phase 3: User Story 1 — 并发下的唯一预留与重复调用复用 (Priority: P1) 🎯 MVP

**Goal**: Multiple executors for the same `(chain_id, sender)` get at most one valid binding per intent; repeat calls (retry, concurrent duplicate, restart) reuse the original binding.

**Independent Test**: V1 + V2 — N≥2 concurrent different intents plus a different sender in parallel; same-intent replay/conflict; assert no duplicate nonce, ≤1 binding per intent, replay changes nothing.

- [ ] T018 [P] [US1] V1 concurrency integration test in NEW `internal/nonce/allocate_concurrency_integration_test.go` (tags: integration; real PostgreSQL via testcontainers; Anvil not required): N≥2 parallel different intents on one `(chain_id,sender)` + a parallel different sender; assert each intent ≤1 binding, no duplicate nonce among active bindings, the other sender is unaffected, and the named DB UNIQUE carriers exist. deps: T012 (external: real PostgreSQL) | FR-01/02/09; V1; SC-01 | done: `go test -tags integration -run TestAllocateConcurrent ./internal/nonce` green
- [ ] T019 [P] [US1] V2 replay/conflict integration test in NEW `internal/nonce/allocate_replay_integration_test.go`: sequential retry, concurrent duplicate, retry after restart, and same intent with differing `chain_id`/`sender`/`authorization_id`; equal input → original `binding_id`/nonce with zero new rows; differ → `allocation_conflict`, original untouched, zero second binding. deps: T012 | FR-04/05; V2; SC-02 | done: integration green
- [ ] T020 [US1] Allocation observability wiring (R11/FR-21): `logx`-redacted structured fields (`chain_id`, `sender`, `nonce`, `binding_id`, `intent_id`, `hold_id`, `cause`, classification, `registry_seq`) emitted from `internal/nonce/allocate.go`, plus the new metrics series in `internal/metrics/metrics.go` (`txharbor_nonce_allocations_total{result}`, `txharbor_nonce_replays_total`, `txharbor_nonce_observations_total{classification}`, and every refusal's machine reason) with counters pinned in `internal/metrics/metrics_test.go` and a redaction unit `internal/nonce/observe_redact_test.go`. deps: T012 (no `[P]` with T012 — same `allocate.go`) | FR-21; R11; V13; SC-09 | done: unit green; zero token/key material in log output

**Checkpoint**: US1 fully functional and independently testable (V1/V2 green) — the MVP increment.

---

## Phase 4: User Story 2 — 预留后崩溃与重启恢复 (Priority: P1)

**Goal**: After the durable binding and before/after any external side effect, a crash+restart rebuilds from durable rows only: no double allocation, no lost binding, no allocation before rebuild completes.

**Independent Test**: V3 — kill -9 after admission commit, restart, retry same intent; and kill after an external effect started with unknown result.

- [ ] T021 [P] [US2] V3 crash/restart integration test in NEW `internal/nonce/restart_integration_test.go` (tags: integration; real PostgreSQL): kill -9 after admission commit before any downstream effect → restart → retry the same intent returns the original binding, no double allocation; allocation refused `rebuild_incomplete` until verification completes; durable rows are the only basis (no memory). deps: T012,T017 | FR-06/13; V3; SC-03 | done: green
- [ ] T022 [P] [US2] T-converge commit-unknown + 23505 fixed-order classify integration test in NEW `internal/nonce/converge_integration_test.go`: race two concurrent same-intent inserts and two same-scope different-intent inserts; on 23505 assert the fixed-order classify returns the winner (replay) or a retryable outcome, and never lets another intent claim the nonce; an uncertain COMMIT retried with the same identity converges. deps: T012 | FR-02/05/06; V2; SC-02 | done: green
- [ ] T023 [US2] Rebuild-gate integration test in NEW `internal/nonce/rebuild_integration_test.go`: with the gate closed every allocation refuses `rebuild_incomplete` and records the cause; verification success opens it; verification failure keeps it closed; DB unavailability fails closed (no memory-based allocation, no silent repair). deps: T017 | FR-13; R5; V3; SC-03 | done: green

**Checkpoint**: US1 AND US2 work independently (unique binding survives crash/restart).

---

## Phase 5: User Story 3 — 结果未知、禁止回收与同 nonce 替换 (Priority: P1)

**Goal**: Unknown-outcome bindings are retained and keep being observed; replacement keeps the original intent; a signed/broadcast/unknown nonce is never handed to another intent.

**Independent Test**: V4 — construct an unknown-outcome binding, issue new allocations and a same-nonce replacement; assert 0 recycle, 0 reassign, replacement stays on the original intent.

- [ ] T024 [P] [US3] V4 unknown-outcome retention E2E in NEW `internal/nonce/unknown_outcome_integration_test.go` (tags: integration; real PostgreSQL + real Anvil via the `logscanStartAnvilNode` precedent): chain shows the bound nonce pending (`nonce ∈ [L,P)`) → binding transitions to `in_flight` with observation evidence; it stays on the original intent and is never auto-failed/recycled/reassigned; evidence 100% retained; replacement attempts reference the same binding. deps: T012,T011 | FR-07/09; V4; SC-04/05 | done: green
- [ ] T025 [P] [US3] Binding disposition integration test in NEW `internal/nonce/binding_release_integration_test.go`: `nonce-admin binding-release` only for a non-terminal binding; explicit no-side-effect evidence required — a timeout/connection error alone is refused (US3-3); success is terminal `released` + event + `applied` audit and the nonce is never reused; an in-flight item MAY remain in-flight (release is not forced). deps: T014,T015 | FR-08/09; observation.md §3.2; V4; SC-05 | done: green
- [ ] T026 [US3] Reconcile transition (T-observe) integration test in NEW `internal/nonce/reconcile_integration_test.go`: transitions `allocated→in_flight` (`nonce ∈ [L,P)`) and `allocated|in_flight→consumed` (`nonce < L`) each append exactly one event; a repeat observation converges on `nonce_binding_events_binding_to_uniq`; no automatic transition ever produces `released`. deps: T007,T011,T012 | R6; FR-07/09/20; V4; SC-04 | done: green

**Checkpoint**: Unknown-outcome and replacement semantics proven (no recycle/reassign).

---

## Phase 6: User Story 4 — 链视图分歧、缺口与外部消耗的对账 (Priority: P1)

**Goal**: latest/pending divergence, gaps and external consumption are recorded as facts and routed to reconcile; never silently reused, skipped or merged.

**Independent Test**: V5 + V6 — external mined consumption, pending-only gap, RPC outage, divergent view, and a first-use bootstrap scope.

- [ ] T027 [P] [US4] V5 classification/holds E2E in NEW `internal/nonce/classify_e2e_integration_test.go` (real PostgreSQL + real Anvil): external mined consumption above frontier (`L>M+1` → `unattributed_consumption` hold + refusal); pending-only above frontier (`P>M+1`, `L<=M+1` → `unexplained_gap` hold + refusal); RPC outage (`unavailable`, no state change); divergent/regressing view (`L>P` / pending regression → `chain_view_divergence` hold + refusal); zero silent reuse/adoption; each classification + hold + evidence queryable. deps: T009,T010,T011,T012 | FR-10/11/12; V5; SC-06 | done: green
- [ ] T028 [P] [US4] V6 bootstrap evidence E2E in NEW `internal/nonce/bootstrap_integration_test.go`: first-ever scope with pre-existing chain history (`P>0`) → `bootstrap_external_consumed` observation persists `[0,P)`; admission at `P`; no hold; evidence queryable (never a silent merge). deps: T011,T012 | FR-12; R2; V6; SC-06 | done: green
- [ ] T029 [US4] Reconcile observer loop in `internal/nonce/reconcile.go` + `internal/app/serve.go` (per-known-scope loop on the existing `IndexPollInterval` cadence submitting T-observe under the shared lock; reuses the existing lifecycle/coordinator; allocator health never flips service readiness, mirroring 006's observer) + `internal/nonce/reconcile_test.go` (unit scheduling/stop). deps: T012,T017 (no `[P]` with T017 — same `serve.go`) | R4; FR-10/11/12; V5 | done: build + unit green; clean start/stop under `Serve`
- [ ] T030 [US4] RPC fault-injection integration test in NEW `internal/nonce/rpc_fault_integration_test.go`: inject transport / timeout / rate-limit / conflicting-view failures during admission and reconcile (reuse the 006 RPC-proxy harness pattern); assert each failure persists an `unavailable` observation with the eth error class, changes zero domain state, and never lowers the floor, releases a hold, or re-derives a candidate (contracts/observation.md §2.1 stale-observation rule). deps: T011,T029 | FR-10/15; observation.md §2.1; V5; SC-06 | done: green

**Checkpoint**: Divergence/gap/consumption is recorded, never silently absorbed.

---

## Phase 7: User Story 5 — 承接 006 恢复暂停与 007 仅接收边界 (Priority: P2)

**Goal**: Recovery pauses allocation and cannot be cleared by 008; 008 holds coexist independently; release is operator-only with an evidence standard; read exposes facts, not permission.

**Independent Test**: V7–V12 — release refusals/valid release, evidence drift, 006 precedence, read five outcomes, registry lifecycle, authorization fail-closed, operator attempt semantics.

- [ ] T031 [P] [US5] V7 hold-release integration test in NEW `internal/nonce/hold_release_integration_test.go`: insufficient evidence or cause still present → `refused` audit with zero hold/floor change; a valid release clears ONLY the named hold and advances `reconciled_floor` to the observed pending; other holds survive; `nonce-admin status` lists active holds, causes, evidence and floors. deps: T014,T015 | FR-08; observation.md §3.1/§4; V7; SC-07 | done: green
- [ ] T032 [P] [US5] Evidence-version drift refusal test in NEW `internal/nonce/release_version_integration_test.go`: registry `seq`/`state` change, hold already released, a foreign `--observation-id`, or a changed active-hold set between the operator's evidence and the in-tx re-verify → `refused`, zero hold/floor change; an applied release is never reinterpreted against later evidence; a re-detected cause creates a NEW hold instance. deps: T014 | FR-08; observation.md §2.1/§3.1; V7 | done: green
- [ ] T033 [P] [US5] V8 006-pause precedence + independent-cause coexistence in NEW `internal/nonce/recovery_coexistence_integration_test.go`: 006 recovery established before admission → refused with the recorded reason; admission committed first → the binding stands; 006 completion does NOT clear a 008 hold; a 008 release does not clear the 006 pause; `reorg_recovery`/`indexer_pause`/`log_pause`/`deposit_pause` rows byte-identical across every 008 path (snapshot). deps: T006,T009,T012 | FR-14/15/23; V7/V8; SC-07 | done: green
- [ ] T034 [P] [US5] V9 read-provider contract test in NEW `internal/nonce/readapi_contract_integration_test.go` (this is the 008 Provider-side acceptance; the 009 client is out of scope): each of `bound` (gate `open`/`held` with causes), `terminal`, `not_bound` (404), `mismatch` (409, durable-scope echo), `unavailable` (503, retryable) matches contracts/read-api.md §2/§3 exactly — the `bound` body field set is exactly §3.1 (`outcome`; `binding.binding_id`/`intent_id`/`chain_id`/`sender`/`nonce`/`state`/`created_at`; `binding.authorization.id`/`authorization.version`; `binding.registry_seq`; `annotations.gate.state` and `annotations.gate.causes[].hold_id`/`cause`/`established_at`; `annotations.recovery.state`; `annotations.registry_state`; `notice`) and the `terminal` body is exactly §3.2 (same fields with `state`∈`consumed`/`released`, plus `terminal_at` and, for `released`, `release_operation_id`), and `not_bound`/`mismatch`/`unavailable` match §3.3 — no extra/missing keys, fixed `notice`, decimal strings; 401 without/with a wrong token; read-immutability snapshots prove every 008 + 006/007 table unchanged; a release/establish racing a held-open read is never straddled (single snapshot); a 006 read failure yields `recovery.state="unknown"`, never `none`/`released`. deps: T013,T017 | FR-14/19; read-api.md §2/§3/§4; V9; SC-06/09 | done: green
- [ ] T035 [P] [US5] Consumer scope-row SHARE participation provider-side test in NEW `internal/nonce/readapi_lockorder_integration_test.go`: reads take `SELECT … FOR SHARE` on the scope `nonce_scope_state` row before snapshotting while every 008 writer takes the same row `FOR UPDATE` first (no reverse order ⇒ no lock cycle); a consumer-simulated `FOR SHARE` held into its own commit never deadlocks 008 writers and blocks them only until commit; 008 writers never write 006/007 rows. deps: T013,T014 | FR-14/15; read-api.md §4 (bilateral); V9 | done: green; the 009 side of this bilateral rule is executed by 009 (see §Final Executor & Deferred Acceptance)
- [ ] T036 [P] [US5] V10 registry lifecycle integration test in NEW `internal/nonce/registry_lifecycle_integration_test.go`: `register → disable → re-register` through the operator carrier; a disabled sender's new admission is refused `sender_disabled` while existing binding facts stay unchanged; `registry_seq` history + audit rows complete; an existing intent's sender/nonce never changes; operation-id replay/conflict semantics hold. deps: T008,T012,T014,T015 | FR-01; R9; V10; SC-05 | done: green
- [ ] T037 [P] [US5] V11 authorization binding + fail-closed + Accepted-guard in NEW `internal/nonce/authz_integration_test.go`: missing/inactive/expired/mismatched/unreadable authorization → no binding (zero rows); a valid authorization → binding stores `authorization_id` + version digest; 008 writes zero 007 rows (snapshot); retry does not consume/extend the authorization; presence of only 007 Accepted rows triggers no allocation and no inferred intent (FR-16 guard); authorization validity is serialized with 007 supply/revoke inside 008's own admission (asserted by T044). deps: T012 | FR-04/16/17; R10; V11; SC-05/08 (revoke-vs-admission race: T044) | done: green
- [ ] T038 [P] [US5] V12 operator attempt semantics in NEW `internal/nonce/admin_attempts_integration_test.go`: the same operation id + same op-input yields exactly one audit row and the recorded `applied`/`refused`/`nop` outcome unchanged (never upgraded); a differing op-input yields `operation_conflict` with zero writes; refusals are committed `refused` outcomes; an uncertain COMMIT retries with the SAME operation id. deps: T014,T015 | FR-08; R7; V12 | done: green
- [ ] T044 [P] [US5] V11 revoke-vs-admission race test in NEW `internal/nonce/authz_revoke_race_integration_test.go`: 007 `RevokeGrant` (and `SupplyGrant`) race a 008 admission on the same `withdrawal_authorizations` row — the admission takes `SELECT … FOR SHARE` on that row after the scope row and before its own-row writes; a revoke/supply committing before the share lock is observed and the admission refuses `authorization_invalid` with zero binding; an admission taking the share lock first commits and the 007 writer waits until it does; the fixed order scope row → 006 gate reads → 007 authorization row → own rows neither deadlocks nor lets 008 write a 007 row (snapshot byte-identity); the race is resolved by 008's own admission-time lock, never by downstream 009/011 re-validation (defense-in-depth only). deps: T012,T037 | FR-17/23; observation.md §2.1/§3.4; V11; SC-05 | done: green
- [ ] T045 [P] [US5] V11/V12 commit-unknown convergence across the 007 boundary in NEW `internal/nonce/authz_commit_unknown_integration_test.go`: an uncertain 008 admission COMMIT retried with the SAME `intent_id` converges on the original binding (replay) or a retryable outcome, never a second binding; an uncertain 007 `RevokeGrant`/`SupplyGrant` COMMIT retried with the SAME `operation_id` converges through the recorded attempt; neither side infers the other's outcome. deps: T012,T014 | FR-15/17; observation.md §3.4/§4; V11/V12; SC-02/05 | done: green

**Checkpoint**: All user stories independently functional; 006/007 boundaries honored structurally.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Regression, CI, runbook, acceptance sign-off — no new behavior

- [ ] T039 [P] 002–007 regression gate (unit + integration): `go test ./...` + `go test -tags integration ./...` green. Diff allowlist (reviewed with test evidence; being listed does NOT bless arbitrary changes inside a file): ALLOWED-NEW `internal/nonce/**`, `internal/app/nonceadmin.go` + `nonceadmin_test.go`, `internal/config/config_008_test.go`, `internal/nonce/*_integration_test.go`, `migrations/000008_nonce_manager.sql`, `specs/008-nonce-manager/**`; ALLOWED-MODIFY `cmd/txharbor/main.go` (dispatch case only), `internal/app/serve.go` (read mount + rebuild gate + reconcile loop start only), `internal/config/config.go` (read-token + knob passthrough only), `internal/metrics/metrics.go` + `metrics_test.go` (new series only). Any diff to `internal/indexer/**`, `internal/withdrawal/**`, `internal/db/**`, `migrations/000001–000007`, or the shared `writeGuard`/lease/coordinator constants → STOP, explicit review + full regression re-run. Historical CI runs are baseline context, never 008 evidence. done: gate green
- [ ] T040 [P] Lint/vet/build gate: `gofmt -l .` empty + `go vet ./...` + `go vet -tags integration ./...` + `go build ./...` per Makefile (existing `ci.yml` four jobs; no workflow change). done: green
- [ ] T041 Operator runbook + environment-isolation record in `specs/008-nonce-manager/quickstart.md` (append §操作 runbook + §资源隔离记录, 007 T032 / 005 T029 precedent; no new contracts file): `nonce-admin mint → hold-release / binding-release / register / disable / status` flows, operation-id capture rule, exit codes 0/1/2, DSN trust root, uncertain-COMMIT same-op-id retry rule, and the V1–V13 checklist execution record; isolation is workdir-local only — database/schema `txharbor_008`, PostgreSQL `127.0.0.1:55432`, Anvil `127.0.0.1:58545`, distinct compose project/volume (`txharbor008`); NEVER the shared `compose.yaml` `pgdata` / 5432 / 8545, so the sibling 009 workdir cannot collide. deps: T015 | FR-08; quickstart.md §Environment | done: runbook present; no behavior change
- [ ] T042 Full V-matrix validation run + FR/SC mapping sign-off: execute V1–V13 with each SC-01–SC-09 tied to a green test; record T000-P still open; mark test-double-only unit work as NOT acceptance evidence. deps: T018–T038 | FR-22; SC-01–SC-09; V1–V13 | done: matrix record green
- [ ] T043 Deferred-acceptance record (no code): state that real 009 client integration, 010 attempt linkage and 011 intent-existence/linkage cross-checks are deferred (contracts/downstream.md §4), that the FR-18 deferred half (attempt traceability, contracts/downstream.md §2) is a recorded gap and not a simulated behavior, that 008's own acceptance is scoped to admission/reconcile/read-provider behavior (quickstart.md), and that T000-P remains open. deps: T042 | FR-18 (deferred half) /FR-23; downstream.md §2/§4 | done: record present

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: no dependencies — T001 first, then T002/T003 in parallel [P]
- **Foundational (Phase 2)**: T004 first, then T005 (depends on T004); T006/T007/T008/T010 carry [P] (distinct files, no same-phase prerequisite) after T001/T003; T009 (after T007) and T011 (after T010) may run in parallel with the rest of the band once their stated prerequisite completes, but carry no [P] because each depends on a same-phase task; T012 depends on T006+T007+T008+T009+T010+T011; T013 on T006+T007+T008; T014 on T006+T007+T008+T009; T015 on T014; T016 on T001; T017 on T013+T016. No task carries [P] across a stated prerequisite.
- **User Stories (Phase 3–7)**: each depends on Foundational's checkpoint; US1 (MVP) → US2 → US3 → US4 → US5 is the recommended order, while US1/US2/US3 test files may start in parallel once T012 is frozen.
- **Polish (Phase 8)**: T039/T040 [P] after all stories; T041 after T015; T042 after T018–T038 and T044/T045; T043 after T042.

### Within-story order

- Tests first (they must FAIL before the story's implementation claim), then wiring, then checkpoint validate.
- Migration/constraints before tx logic; tx logic before wiring; wiring before runbook.

### Parallel opportunities (all [P] tasks touch distinct files)

- Setup: T002 + T003
- Foundational: T006 + T007 + T008 + T010 (four distinct files, nothing-prerequisite); T009 (after T007) and T011 (after T010) join in parallel once satisfied (not [P])
- US1: T018 + T019
- US2: T021 + T022
- US3: T024 + T025
- US4: T027 + T028
- US5: T031 + T032 + T033 + T034 + T035 + T036 + T037 + T038 + T044 + T045 (distinct test files; each still gated by its stated prerequisite completing)
- Polish: T039 + T040

### No dangling / no circular dependencies

Every `deps:` reference points to a lower-numbered task (or none). No story task depends on another story's task. There is no task whose dependency set is empty while claiming a prerequisite.

---

## FR/SC/V Trace Matrix

| FR | Tasks | SC | V |
|---|---|---|---|
| FR-01 (scope isolation, registry authority) | T004, T008, T011, T012, T018, T036 | SC-01 | V1, V10 |
| FR-02 (one active intent per nonce, persistence concurrency) | T004, T005, T012, T018, T022 | SC-01 | V1, V13 |
| FR-03 (persist before side effect, idempotent, OC-3) | T004, T012, T019 | SC-02 | V2, V4 |
| FR-04 (stable intent identity, never create) | T012, T019, T037 | SC-02 | V2, V11 |
| FR-05 (repeat reuse, persisted) | T007, T012, T019, T022 | SC-02 | V2 |
| FR-06 (crash/restart reuse) | T021, T022 | SC-03 | V3 |
| FR-07 (unknown retained) | T024, T026 | SC-04 | V4 |
| FR-08 (no auto recycle; operator reconcile) | T009, T014, T015, T031, T032, T038 | SC-06/07 | V7, V12 |
| FR-09 (never reassign; replacement stays) | T007, T024, T025, T026 | SC-05 | V1, V4 |
| FR-10 (divergence recorded; no silent reuse) | T010, T011, T027, T030 | SC-06 | V5 |
| FR-11 (gaps not reusable; query/audit) | T010, T013, T027, T029 | SC-06 | V5, V9 |
| FR-12 (consumption recorded; no silent merge) | T010, T011, T027, T028, T029 | SC-06 | V5, V6 |
| FR-13 (rebuild fail-closed) | T007, T017, T021, T023 | SC-03 | V3 |
| FR-14 (006 precedence; OC-6 use semantics) | T006, T009, T013, T031, T033, T034, T035 | SC-07 | V7, V8, V9 |
| FR-15 (pre-commit re-verify; OC-7 ordering) | T011, T012, T030, T033, T035, T045 | SC-07 | V5, V8 |
| FR-16 (Accepted is not an intent/authorization) | T037 | SC-08 | V11 |
| FR-17 (authorization validation, fail-closed) | T002, T012, T037, T044 | SC-05 | V11 |
| FR-18 (attempt/signing identities) | T004, T012, T024 (scope half: binding identity + no second intent claim); T043 (deferred half: attempt traceability) | SC-05 | V1, V4 + review + deferred |
| FR-19 (no keys/signing/broadcast) | T013, T016, T017, T034 | SC-09 | V9, V13 |
| FR-20 (explicit state machine) | T004, T007, T026 | — | V4, V13 |
| FR-21 (observability, no secrets) | T016, T020 | SC-09 | V13 |
| FR-22 (real concurrency/restart testing) | T018–T038, T042, T044, T045 | SC-01–SC-09 | V1–V13 |
| FR-23 (no upstream redefinition) | T005, T033, T039 | — | V7, V11, V13 |

| SC | Owning tasks | V |
|---|---|---|
| SC-01 no duplicate nonce under concurrency | T018 | V1 |
| SC-02 replay adds zero allocations | T019, T022 | V2 |
| SC-03 crash/restart: zero double allocation, zero lost binding | T021, T023 | V3 |
| SC-04 zero recycle/reassign; 100% evidence retained | T024, T026 | V4 |
| SC-05 replacement keeps original intent; terminal never reused | T024, T025, T036, T037 | V4, V10, V11 |
| SC-06 zero silent reuse; 100% reconcile records; zero release on insufficient evidence | T027, T028, T030, T031 | V5, V6, V7 |
| SC-07 zero allocation during recovery; completion clears no 008 hold | T031, T033 | V7, V8 |
| SC-08 zero implicit allocation from Accepted-only requests | T037 | V11 |
| SC-09 zero secrets/keys in logs and responses | T020, T034 | V13 |

---

## Final Executor & Deferred Acceptance (cross-module)

- **008 executes its own acceptance**: the migration probes (T005), the concurrency/crash/reconcile/release/006-coexistence suites (T018–T038), and the **ReadBinding provider contract test** (T034) plus the consumer–provider lock-order test (T035) and the §3.1/§3.2 enumerated field-set assertions against contracts/read-api.md (no extra/missing keys). `contracts/observation.md` §3/§4 assertions (only the named hold cleared, floor monotonicity, multi-cause survival, 006 byte-identity) are also 008's.
- **009 owns its side**: the read client, retry/backoff, authorizaton re-validation and 009's own `SELECT … FOR SHARE` scope-row participation (read-api.md §4) are 009's integration acceptance and are **NOT** implemented or re-verified in this 008 task list. The `attempt_id`/`signing_request_id` keys are the 009-side adapter's own local keys (persisted downstream by 010); 008 stores neither and links the two sides only through `binding_id`/`intent_id` (FR-18 deferred half).
- **Deferred (no bilateral claim)**: 011 intent existence / intent↔request↔authorization linkage; 010 attempt-level refinement; end-to-end `API → queue → nonce → signing → broadcast → confirmation`. Recorded by T043 (contracts/downstream.md §4).
- **T000-P** (production provider selection) remains open and neither blocks nor substitutes for 008 verification.

---

## Implementation Strategy

### MVP First (US1 only)

1. Phase 1 + Phase 2 → foundation (migration applies, carriers compile, 000008 probes + classification/outcome units green)
2. Phase 3 (US1) → V1/V2 green
3. **STOP and VALIDATE** before US2

### Incremental delivery

- Foundation → US1 (MVP) → US2 (crash/restart) → US3 (unknown/replacement) → US4 (divergence/gap/consumption) → US5 (006/007 boundary + operator/read) → Polish
- Each phase is independently buildable (`go build ./...`) and verifiable by its V-scenarios.

---

## Notes

- `[P]` = different files, no dependency on another same-phase task; story label = traceability
- Commit after each task or logical group (small, reviewable batches per constitution XIV) — not performed by the tasks-authoring step
- Test doubles are EARLY-VALIDATION only; every acceptance claim routes through real PostgreSQL + real Anvil (T018–T038)
- plan/research/data-model/contracts/quickstart are design artifacts, not evidence; T000-P stays open; historical CI runs are baseline context only
- No 009/010/011 implementation tasks exist here; upstream 006/007 files, migrations and rows stay byte-identical
