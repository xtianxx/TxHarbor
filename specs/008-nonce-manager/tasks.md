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

- [X] T001 Create `internal/nonce/` package skeleton per plan.md §Project Structure: `allocate.go`, `binding.go`, `classify.go`, `observe.go`, `reconcile.go`, `hold.go`, `registry.go`, `readapi.go`, `admin.go`, `numeric.go`, plus `errors.go` and `coord.go` (one added file beyond plan's illustrative list: shared coordination/gate primitives, one-concern-per-file per the `internal/indexer`/`internal/withdrawal` layout) — doc comments only, no behavior. deps: none | plan.md | done: `go build ./...` and `go vet ./internal/nonce/...` green
- [X] T002 [P] Domain outcome/error vocabulary in `internal/nonce/errors.go` with the exact machine strings from contracts/downstream.md §1 (`allocated`, `replayed`, `allocation_conflict`, `chain_view_unavailable`, `scope_held`, `sender_not_registered`, `sender_disabled`, `authorization_invalid`, `rebuild_incomplete`, `recovery_active`, `temporarily_unavailable`) and contracts/observation.md §3.4/§4 (`operation_conflict`) plus contracts/read-api.md §2/§3 read outcomes (`bound`, `terminal`, `not_bound`, `mismatch`, `unavailable`); + `internal/nonce/errors_test.go`. deps: T001 | FR-14/FR-17; downstream.md §1; observation.md §3.4/§4; read-api.md §2/§3 | done: unit test pins each string exactly (`go test ./internal/nonce -run TestOutcome`)
- [X] T003 [P] Numeric helpers in `internal/nonce/numeric.go` (R13): `math/big` parse/bounds, JSON-RPC hex → integer with explicit overflow refusal, `NUMERIC(78,0)` range `0 ≤ n ≤ 2⁶⁴−1`, decimal/hex transport helpers; zero `float64` anywhere; + `internal/nonce/numeric_test.go` (0 and 2⁶⁴−1 accepted; 2⁶⁴, negative, non-decimal refused). deps: T001 | FR-01/FR-02; R13; V13 | done: `go test ./internal/nonce -run TestNumeric` green

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Migration, coordination discipline, binding/registry/hold/classify/observe carriers, admission core, read provider, operator library, subcommand, config — every story needs these. MUST complete before ANY user story.

**⚠️ CRITICAL**: No user story work can begin until this phase reaches its checkpoint.

- [x] T004 Write `migrations/000008_nonce_manager.sql` (data-model.md Tables 1–7: `nonce_wallet_registry`, `nonce_scope_state`, `nonce_bindings`, `nonce_binding_events`, `nonce_observations`, `nonce_scope_holds`, `nonce_ops_audit`; pure DDL, goose numbered, no stored functions; every `ConstraintName` consumed by classification explicitly declared — `nonce_bindings_pkey`, `nonce_bindings_intent_uniq`, `nonce_bindings_scope_nonce_uniq`, `nonce_bindings_registry_fkey`, `nonce_bindings_state_check`, `nonce_bindings_terminal_consistency`, `nonce_bindings_nonce_range`, `nonce_bindings_intent_shape`, `nonce_wallet_registry_pkey`, `nonce_scope_state_pkey`, `nonce_scope_state_registry_fkey`, `nonce_binding_events_binding_to_uniq`, `nonce_scope_holds_status_consistency`, `nonce_scope_holds_registry_fkey`, `nonce_ops_audit_pkey`, `nonce_ops_audit_operation_id_uniq`; `NUMERIC(78,0)` bounds `0…2⁶⁴−1`). deps: none (Phase 1 may proceed in parallel) | FR-01/02/03/20/23; data-model.md Tables 1–7 | done: `go build ./...` green; `migrations/embed.go` auto-includes the file
- [x] T005 Migration verification in NEW `internal/nonce/migration_integration_test.go` (tags: integration; raw-SQL probes only — MUST NOT import the `internal/nonce` classifiers): (a) `git diff --name-only -- migrations/000001_baseline.sql migrations/000002_chain_indexer.sql migrations/000003_event_indexing.sql migrations/000004_deposit_detection.sql migrations/000005_confirmation_tracking.sql migrations/000006_reorg_recovery.sql migrations/000007_withdrawal_creation.sql` empty; (b) isolated scratch DB baseline→000008 `goose up` green + `goose status` clean, then 000008 `down`/`up` reproducible; (c) negative inserts: dup `intent_id` → 23505 `nonce_bindings_intent_uniq`; dup `(chain_id,sender,nonce)` → 23505 `nonce_bindings_scope_nonce_uniq`; dup `operation_id` → 23505 `nonce_ops_audit_operation_id_uniq`; dup `(binding_id,to_state)` → 23505 `nonce_binding_events_binding_to_uniq`; unregistered sender → 23503 `nonce_bindings_registry_fkey`; CHECK violations (nonce > 2⁶⁴−1, terminal-consistency) → 23514 (never 23505); (d) each elicited `ConstraintName` equals the migration's declaration (name probe only — app-level classify belongs to T012/T022/T038). deps: T004 | FR-02/03/20/23; data-model.md named carriers | done: `go test -tags integration -run TestNonceMigration ./internal/nonce` green
- [X] T006 [P] Coordination + 006 gate primitives in `internal/nonce/coord.go`: open every 008 write tx with the repo's single coordination discipline — `BEGIN → SET LOCAL statement_timeout='5s' → ensure coordination row → SELECT … FOR UPDATE → post-lock rechecks → COMMIT` (shape per `internal/indexer/reorgcommit.go` `lockRecoveryChain`); 008 never acquires/renews the lease and never writes `indexer_lease`; read-only 006 gate reads (`indexer_pause`/`log_pause`/`deposit_pause` existence + active `reorg_recovery` row); scope-row `FOR UPDATE` helper for `nonce_scope_state`. NOTE: `internal/indexer` constants are package-private and touching `internal/indexer/**` is out of scope, so the identical 5-step statements are declared locally in `coord.go` — the invariant is the same coordination row + `FOR UPDATE` lock, not identifier reuse. The local copy is accepted (no cross-package coupling), and `coord_test.go` carries a drift-guard assertion pinning the local guard literal to the repo's shared 5 s statement guard value so the copy and the indexer cannot silently diverge. + `internal/nonce/coord_test.go` (unit: lock-order framing, gate-hit refusal with zero writes, local-guard equivalence assertion). deps: T001 | R1/R4; FR-14/FR-15; observation.md §2.1 | done: unit green (incl. the equivalence assertion); zero writes to 006 tables
- [X] T007 [P] Binding model + explicit state machine + append-only event writers in `internal/nonce/binding.go` (states `allocated → in_flight → consumed | released`; direct `allocated→consumed` and `allocated|in_flight→released` allowed; `consumed`/`released` terminal with no path out; terminal-consistency + nonce-range guards mirrored from `nonce_bindings_*`; event writer converging on `nonce_binding_events_binding_to_uniq`; readers by `binding_id` / `intent_id`; read-only rebuild verification reader `VerifyRebuild` per known scope). + `internal/nonce/binding_test.go` (unit: legal/illegal transitions, terminal immutability, no automatic path to `released`). deps: T001 | R6; FR-03/05/09/13/20; V4/V13 | done: `go test ./internal/nonce -run TestBinding` green
- [X] T008 [P] Wallet registry reads/versioning in `internal/nonce/registry.go` (read `nonce_wallet_registry` by `(chain_id,sender)`; `active`/`disabled` gates **admission only**; return `registry_seq`; a later config change never alters an existing binding). + `internal/nonce/registry_test.go`. deps: T001 | R9; FR-01; V10 | done: unit green
- [X] T009 Holds + scope frontier in `internal/nonce/hold.go` (establish a `nonce_scope_holds` row per cause from a persisted observation with `evidence_observation_id`; a scope is admission-held iff ≥1 `active` row; monotonic floor `UPDATE … SET reconciled_floor = GREATEST(COALESCE(reconciled_floor,0), $new)` guarded; `last_latest`/`last_pending`/`last_observation_id` updates; readers for active holds + scope state). + `internal/nonce/hold_test.go` (unit: multi-cause coexistence, no scope-level boolean, released cause re-detected ⇒ NEW hold instance). deps: T001,T007 | R7; FR-08/FR-11; V5/V7 | done: unit green
- [X] T010 [P] Classification matrix in `internal/nonce/classify.go` (data-model §Observation classification matrix as a pure function of `M`, `F`, `L`, `P`, `P_prev`: `unavailable` on read failure; `divergence` on `L > P` or pending regression; `consistent` no-bindings/no-floor/`P=0` → candidate `0`; `bootstrap_external_consumed` first scope `0 < P`, `L <= P` → candidate `P`; `consistent` bindings `P <= M+1` → candidate `max(M+1, F)`; `unattributed_consumption` `P > M+1 && L > M+1`; `unexplained_gap` `P > M+1 && L <= M+1` — the last two hold+refuse and NEVER derive `max(M+1, P)`). + `internal/nonce/classify_test.go` FULL boundary matrix. deps: T003 | R2/R4; FR-10/11/12; V5/V6 | done: matrix unit green incl. every refusal/hold branch
- [X] T011 Chain observation in `internal/nonce/observe.go` (R4: outside any DB transaction, `eth_getTransactionCount(sender,"latest")` + `("pending")` + `eth_getBlockByNumber("latest")` head number/hash; reuse the `internal/eth` error classification `KindTransport`/`KindTimeout`/`KindRateLimited`/`KindInvalidResponse`/`KindChainMismatch`/`KindNotFound` and the existing `IndexRPCTimeout`/`IndexRetryInitial`/`IndexRetryMax` knobs — no new timing knob; one `nonce_observations` row per attempt, `unavailable` + error class persisted on any failure; **zero RPC inside any DB tx**). + `internal/nonce/observe_test.go` (EARLY-VALIDATION fake RPC only — real RPC is covered by T027/T030). deps: T003,T010 | R4; FR-10/12/15; V5/V13 | done: unit green
- [X] T012 Admission transaction core in `internal/nonce/allocate.go` (data-model §Transaction catalog: **T-allocate** pre-tx input validation → pre-tx observation → BEGIN → T006 lock → post-lock rechecks (006 gate rows, registry `state`/`registry_seq`, active holds, binding-by-`intent_id`, recompute `M`/`F` from `nonce_bindings` under the lock) → classify (T010) → insert observation always → consistent: insert binding + creation event, else commit observation (+ hold when the matrix says so) with no binding; **T-replay** input equality returns the original binding untouched; **T-conflict** any differ → fail-closed `allocation_conflict`, zero second binding; **T-converge** on 23505: ROLLBACK then off-pool fixed-order classify by `intent_id` first then `(chain_id,sender,nonce)`, exact `ConstraintName` match only, miss retryable; authorization validation takes `SELECT … FOR SHARE` on the 007 `withdrawal_authorizations` row under the fixed order scope row → 006 gate reads → 007 authorization row → own rows (a `RevokeGrant`/`SupplyGrant` committing before the share lock is observed and refused; one racing the admission blocks until commit; 008's own admission owns this approved-admission/revocation serialization — 009/011 re-checks are defense-in-depth, never a substitute), checks `state='active'`, not expired by `clock_timestamp()`, chain match, storing `authorization_id` + `authorization_version` SHA-256; 008 writes zero 007 rows). + `internal/nonce/allocate_test.go` (unit classify/replay/conflict ordering). deps: T006,T007,T008,T009,T010,T011 | R1/R2/R3/R5/R10; FR-01/02/03/04/05/15/17; V1/V2/V11 | done: unit green + `go build ./...` green
- [X] T013 Read provider in `internal/nonce/readapi.go` (contracts/read-api.md §2/§3/§4: `GET /nonce/bindings/{binding_id}` and `GET /nonce/bindings/by-intent/{intent_id}?chain_id=&sender=`; exactly one of five outcomes `bound`/`terminal`/`not_bound` 404/`mismatch` 409/`unavailable` 503; `bound`/`terminal` carry the contract body + `annotations` (`gate` with every active hold id/cause/established_at, `recovery` state, `registry_state`) and the fixed `notice`; one `REPEATABLE READ` read-write tx taking `SELECT … FOR SHARE` on the scope `nonce_scope_state` row first (when it exists) then the snapshot reads, zero modification; 008 read failure → all-or-nothing `unavailable`, 006 read failure → `recovery.state="unknown"` never `none`/`released`; bearer `TXHARBOR_NONCE_READ_TOKEN` constant-time compare → 401 `unauthenticated`; snake_case JSON, nonce/counts decimal strings, addresses lowercase hex). + `internal/nonce/readapi_test.go` (unit outcome mapping + body shape). deps: T006,T007,T008 | R8; FR-01/11/14/19; read-api.md §1–§4; V9 | done: `go test ./internal/nonce -run TestReadAPI` green
- [X] T014 Operator transaction library in `internal/nonce/admin.go` (contracts/observation.md §3, all under the T006 lock: **T-hold-release** `SELECT … FOR UPDATE` the named hold (missing/already released ⇒ `nop` audit, zero change) → re-verify fresh pre-tx observation `consistent`, no unresolved scope conflicts, and the exact evidence-version set (named hold still `active`, operator `--observation-id` belongs to the scope, current `registry_seq`/`state`, current active-hold set, 006 state read-only) → clear ONLY the named hold + `UPDATE nonce_scope_state SET reconciled_floor = GREATEST(COALESCE(...), $observed_pending)` + `applied` audit in one tx; any drift/insufficiency → `refused` audit + zero hold/floor change; **T-binding-release** same carrier per binding, non-terminal only, no-side-effect evidence mandatory (timeout/connection error alone is never evidence); **T-registry** `register`/`disable` with `registry_seq + 1` guarded `RowsAffected`; operation-id dedup via `nonce_ops_audit_operation_id_uniq` (23505 → rollback → re-read by op-id → equal op-input: report recorded outcome; differ: `operation_conflict`, zero writes)); + `internal/nonce/admin_test.go` (unit: refusal/version-drift/op-input equality). deps: T006,T007,T008,T009 | R7; FR-08/14; observation.md §3/§4; V7/V8/V10/V12 | done: `go test ./internal/nonce -run TestAdmin` green
- [X] T015 `txharbor nonce-admin` operator subcommand in `internal/app/nonceadmin.go` + one dispatch case in `cmd/txharbor/main.go` (mirrors `internal/app/withdrawalauthz.go`/`confirmauth.go`: flag parse → `config.Load` → operator-connection tx; actions `mint` (pure entropy, no config/DB) / `hold-release` / `binding-release` / `register` / `disable` / read-only `status`; `--operation-id` REQUIRED and minted first; `--chain-id` deployment bind and `--sender` scope bind before connecting; exit codes 0/1/2; no new role, no admin UI, no HTTP write) + `internal/app/nonceadmin_test.go` (in-process usage/exit codes). deps: T014 | FR-08; observation.md §3 | done: unit green + `go build ./...` green
- [X] T016 Config passthrough in `internal/config/config.go` (add `TXHARBOR_NONCE_READ_TOKEN`; reconcile/observation timing reuses the existing `IndexRPCTimeout`/`IndexPollInterval`/`IndexRetryInitial`/`IndexRetryMax` — NO new timing knobs; the token is never echoed raw in errors/`Summary()` and stays redacted) + NEW `internal/config/config_008_test.go` (`serve_config_test.go` unmodified). deps: T001 | FR-19/FR-21; read-api.md §1 | done: config unit green
- [X] T017 `internal/app/serve.go` extension: mount the read endpoints on the existing mux next to `/withdrawals` (no new listener), and run the startup rebuild verification gate (R5: read-only per-known-scope integrity verification via `VerifyRebuild`; until success every allocation refuses `rebuild_incomplete`; verification failure keeps the gate closed with a structured error — no silent repair, no memory guess). deps: T013,T016 | R5; FR-13/FR-19; V3/V9 | done: `go build ./...` green; gate wired through `Serve`

**Checkpoint**: Foundation ready — 000008 applies cleanly, T005 verification green, all carriers compile with frozen interfaces, T012/T013/T014 unit-level classify/outcome behavior green. Story V-scenarios remain with their stories, never claimed here.

---

## Phase 3: User Story 1 — 并发下的唯一预留与重复调用复用 (Priority: P1) 🎯 MVP

**Goal**: Multiple executors for the same `(chain_id, sender)` get at most one valid binding per intent; repeat calls (retry, concurrent duplicate, restart) reuse the original binding.

**Independent Test**: V1 + V2 — N≥2 concurrent different intents plus a different sender in parallel; same-intent replay/conflict; assert no duplicate nonce, ≤1 binding per intent, replay changes nothing.

- [X] T018 [P] [US1] V1 concurrency integration test in NEW `internal/nonce/allocate_concurrency_integration_test.go` (tags: integration; real PostgreSQL via testcontainers; Anvil not required): N≥2 parallel different intents on one `(chain_id,sender)` + a parallel different sender; assert each intent ≤1 binding, no duplicate nonce among active bindings, the other sender is unaffected, and the named DB UNIQUE carriers exist. deps: T012 (external: real PostgreSQL) | FR-01/02/09; V1; SC-01 | done: `go test -tags integration -run TestAllocateConcurrent ./internal/nonce` green
- [X] T019 [P] [US1] V2 replay/conflict integration test in NEW `internal/nonce/allocate_replay_integration_test.go`: sequential retry, concurrent duplicate, retry after restart, and same intent with differing `chain_id`/`sender`/`authorization_id`; equal input → original `binding_id`/nonce with zero new rows; differ → `allocation_conflict`, original untouched, zero second binding. deps: T012 | FR-04/05; V2; SC-02 | done: integration green
- [X] T020 [US1] Allocation observability wiring (R11/FR-21): `logx`-redacted structured fields (`chain_id`, `sender`, `nonce`, `binding_id`, `intent_id`, `hold_id`, `cause`, classification, `registry_seq`) emitted from `internal/nonce/allocate.go`, plus the new metrics series in `internal/metrics/metrics.go` (`txharbor_nonce_allocations_total{result}`, `txharbor_nonce_replays_total`, `txharbor_nonce_observations_total{classification}`, and every refusal's machine reason) with counters pinned in `internal/metrics/metrics_test.go` and a redaction unit `internal/nonce/observe_redact_test.go`. deps: T012 (no `[P]` with T012 — same `allocate.go`) | FR-21; R11; V13; SC-09 | done: unit green; zero token/key material in log output

**Checkpoint**: US1 fully functional and independently testable (V1/V2 green) — the MVP increment.

---

## Phase 4: User Story 2 — 预留后崩溃与重启恢复 (Priority: P1)

**Goal**: After the durable binding and before/after any external side effect, a crash+restart rebuilds from durable rows only: no double allocation, no lost binding, no allocation before rebuild completes.

**Independent Test**: V3 — kill -9 after admission commit, restart, retry same intent; and kill after an external effect started with unknown result.

- [X] T021 [P] [US2] V3 crash/restart integration test in NEW `internal/nonce/restart_integration_test.go` (tags: integration; real PostgreSQL): kill -9 after admission commit before any downstream effect → restart → retry the same intent returns the original binding, no double allocation; allocation refused `rebuild_incomplete` until verification completes; durable rows are the only basis (no memory). deps: T012,T017 | FR-06/13; V3; SC-03 | done: green
- [X] T022 [P] [US2] T-converge commit-unknown + 23505 fixed-order classify integration test in NEW `internal/nonce/converge_integration_test.go`: race two concurrent same-intent inserts and two same-scope different-intent inserts; on 23505 assert the fixed-order classify returns the winner (replay) or a retryable outcome, and never lets another intent claim the nonce; an uncertain COMMIT retried with the same identity converges. deps: T012 | FR-02/05/06; V2; SC-02 | done: green
- [X] T023 [US2] Rebuild-gate integration test in NEW `internal/nonce/rebuild_integration_test.go`: with the gate closed every allocation refuses `rebuild_incomplete` and records the cause; verification success opens it; verification failure keeps it closed; DB unavailability fails closed (no memory-based allocation, no silent repair). deps: T017 | FR-13; R5; V3; SC-03 | done: green

**Checkpoint**: US1 AND US2 work independently (unique binding survives crash/restart).

---

## Phase 5: User Story 3 — 结果未知、禁止回收与同 nonce 替换 (Priority: P1)

**Goal**: Unknown-outcome bindings are retained and keep being observed; replacement keeps the original intent; a signed/broadcast/unknown nonce is never handed to another intent.

**Independent Test**: V4 — construct an unknown-outcome binding, issue new allocations and a same-nonce replacement; assert 0 recycle, 0 reassign, replacement stays on the original intent.

- [X] T024 [P] [US3] V4 unknown-outcome retention E2E in NEW `internal/nonce/unknown_outcome_integration_test.go` (tags: integration; real PostgreSQL + real Anvil via the `logscanStartAnvilNode` precedent): chain shows the bound nonce pending (`nonce ∈ [L,P)`) → binding transitions to `in_flight` with observation evidence; it stays on the original intent and is never auto-failed/recycled/reassigned; evidence 100% retained; replacement attempts reference the same binding. deps: T012,T011 | FR-07/09; V4; SC-04/05 | done: green
- [X] T025 [P] [US3] Binding disposition integration test in NEW `internal/nonce/binding_release_integration_test.go`: `nonce-admin binding-release` only for a non-terminal binding; explicit no-side-effect evidence required — a timeout/connection error alone is refused (US3-3); success is terminal `released` + event + `applied` audit and the nonce is never reused; an in-flight item MAY remain in-flight (release is not forced). deps: T014,T015 | FR-08/09; observation.md §3.2; V4; SC-05 | done: green
- [X] T026 [US3] Reconcile transition (T-observe) integration test in NEW `internal/nonce/reconcile_integration_test.go`: transitions `allocated→in_flight` (`nonce ∈ [L,P)`) and `allocated|in_flight→consumed` (`nonce < L`) each append exactly one event; a repeat observation converges on `nonce_binding_events_binding_to_uniq`; no automatic transition ever produces `released`. deps: T007,T011,T012 | R6; FR-07/09/20; V4; SC-04 | done: green

**Checkpoint**: Unknown-outcome and replacement semantics proven (no recycle/reassign).

---

## Phase 6: User Story 4 — 链视图分歧、缺口与外部消耗的对账 (Priority: P1)

**Goal**: latest/pending divergence, gaps and external consumption are recorded as facts and routed to reconcile; never silently reused, skipped or merged.

**Independent Test**: V5 + V6 — external mined consumption, pending-only gap, RPC outage, divergent view, and a first-use bootstrap scope.

- [X] T027 [P] [US4] V5 classification/holds E2E in NEW `internal/nonce/classify_e2e_integration_test.go` (real PostgreSQL + real Anvil): external mined consumption above frontier (`L>M+1` → `unattributed_consumption` hold + refusal); pending-only above frontier (`P>M+1`, `L<=M+1` → `unexplained_gap` hold + refusal); RPC outage (`unavailable`, no state change); divergent/regressing view (`L>P` / pending regression → `chain_view_divergence` hold + refusal); zero silent reuse/adoption; each classification + hold + evidence queryable. deps: T009,T010,T011,T012 | FR-10/11/12; V5; SC-06 | done: green
- [X] T028 [P] [US4] V6 bootstrap evidence E2E in NEW `internal/nonce/bootstrap_integration_test.go`: first-ever scope with pre-existing chain history (`P>0`) → `bootstrap_external_consumed` observation persists `[0,P)`; admission at `P`; no hold; evidence queryable (never a silent merge). deps: T011,T012 | FR-12; R2; V6; SC-06 | done: green
- [X] T029 [US4] Reconcile observer loop in `internal/nonce/reconcile.go` + `internal/app/serve.go` (per-known-scope loop on the existing `IndexPollInterval` cadence submitting T-observe under the shared lock; reuses the existing lifecycle/coordinator; allocator health never flips service readiness, mirroring 006's observer) + `internal/nonce/reconcile_test.go` (unit scheduling/stop). deps: T012,T017 (no `[P]` with T017 — same `serve.go`) | R4; FR-10/11/12; V5 | done: build + unit green; clean start/stop under `Serve`
- [X] T030 [US4] RPC fault-injection integration test in NEW `internal/nonce/rpc_fault_integration_test.go`: inject transport / timeout / rate-limit / conflicting-view failures during admission and reconcile (reuse the 006 RPC-proxy harness pattern); assert each failure persists an `unavailable` observation with the eth error class, changes zero domain state, and never lowers the floor, releases a hold, or re-derives a candidate (contracts/observation.md §2.1 stale-observation rule). deps: T011,T029 | FR-10/15; observation.md §2.1; V5; SC-06 | done: green

**Checkpoint**: Divergence/gap/consumption is recorded, never silently absorbed.

---

## Phase 7: User Story 5 — 承接 006 恢复暂停与 007 仅接收边界 (Priority: P2)

**Goal**: Recovery pauses allocation and cannot be cleared by 008; 008 holds coexist independently; release is operator-only with an evidence standard; read exposes facts, not permission.

**Independent Test**: V7–V12 — release refusals/valid release, evidence drift, 006 precedence, read five outcomes, registry lifecycle, authorization fail-closed, operator attempt semantics.

- [X] T031 [P] [US5] V7 hold-release integration test in NEW `internal/nonce/hold_release_integration_test.go`: insufficient evidence or cause still present → `refused` audit with zero hold/floor change; a valid release clears ONLY the named hold and advances `reconciled_floor` to the observed pending; other holds survive; `nonce-admin status` lists active holds, causes, evidence and floors. deps: T014,T015 | FR-08; observation.md §3.1/§4; V7; SC-07 | done: green
- [X] T032 [P] [US5] Evidence-version drift refusal test in NEW `internal/nonce/release_version_integration_test.go`: registry `seq`/`state` change, hold already released, a foreign `--observation-id`, or a changed active-hold set between the operator's evidence and the in-tx re-verify → `refused`, zero hold/floor change; an applied release is never reinterpreted against later evidence; a re-detected cause creates a NEW hold instance. deps: T014 | FR-08; observation.md §2.1/§3.1; V7 | done: green
- [X] T033 [P] [US5] V8 006-pause precedence + independent-cause coexistence in NEW `internal/nonce/recovery_coexistence_integration_test.go`: 006 recovery established before admission → refused with the recorded reason; admission committed first → the binding stands; 006 completion does NOT clear a 008 hold; a 008 release does not clear the 006 pause; `reorg_recovery`/`indexer_pause`/`log_pause`/`deposit_pause` rows byte-identical across every 008 path (snapshot). deps: T006,T009,T012 | FR-14/15/23; V7/V8; SC-07 | done: green
- [X] T034 [P] [US5] V9 read-provider contract test in NEW `internal/nonce/readapi_contract_integration_test.go` (this is the 008 Provider-side acceptance; the 009 client is out of scope): each of `bound` (gate `open`/`held` with causes), `terminal`, `not_bound` (404), `mismatch` (409, durable-scope echo), `unavailable` (503, retryable) matches contracts/read-api.md §2/§3 exactly — the `bound` body field set is exactly §3.1 (`outcome`; `binding.binding_id`/`intent_id`/`chain_id`/`sender`/`nonce`/`state`/`created_at`; `binding.authorization.id`/`authorization.version`; `binding.registry_seq`; `annotations.gate.state` and `annotations.gate.causes[].hold_id`/`cause`/`established_at`; `annotations.recovery.state`; `annotations.registry_state`; `notice`) and the `terminal` body is exactly §3.2 (same fields with `state`∈`consumed`/`released`, plus `terminal_at` and, for `released`, `release_operation_id`), and `not_bound`/`mismatch`/`unavailable` match §3.3 — no extra/missing keys, fixed `notice`, decimal strings; 401 without/with a wrong token; read-immutability snapshots prove every 008 + 006/007 table unchanged; a release/establish racing a held-open read is never straddled (single snapshot); a 006 read failure yields `recovery.state="unknown"`, never `none`/`released`. deps: T013,T017 | FR-14/19; read-api.md §2/§3/§4; V9; SC-06/09 | done: green
- [X] T035 [P] [US5] Consumer scope-row SHARE participation provider-side test in NEW `internal/nonce/readapi_lockorder_integration_test.go`: reads take `SELECT … FOR SHARE` on the scope `nonce_scope_state` row before snapshotting while every 008 writer takes the same row `FOR UPDATE` first (no reverse order ⇒ no lock cycle); a consumer-simulated `FOR SHARE` held into its own commit never deadlocks 008 writers and blocks them only until commit; 008 writers never write 006/007 rows. deps: T013,T014 | FR-14/15; read-api.md §4 (bilateral); V9 | done: green; the 009 side of this bilateral rule is executed by 009 (see §Final Executor & Deferred Acceptance)
- [X] T036 [P] [US5] V10 registry lifecycle integration test in NEW `internal/nonce/registry_lifecycle_integration_test.go`: `register → disable → re-register` through the operator carrier; a disabled sender's new admission is refused `sender_disabled` while existing binding facts stay unchanged; `registry_seq` history + audit rows complete; an existing intent's sender/nonce never changes; operation-id replay/conflict semantics hold. deps: T008,T012,T014,T015 | FR-01; R9; V10; SC-05 | done: green
- [X] T037 [P] [US5] V11 authorization binding + fail-closed + Accepted-guard in NEW `internal/nonce/authz_integration_test.go`: missing/inactive/expired/mismatched/unreadable authorization → no binding (zero rows); a valid authorization → binding stores `authorization_id` + version digest; 008 writes zero 007 rows (snapshot); retry does not consume/extend the authorization; presence of only 007 Accepted rows triggers no allocation and no inferred intent (FR-16 guard); authorization validity is serialized with 007 supply/revoke inside 008's own admission (asserted by T044). deps: T012 | FR-04/16/17; R10; V11; SC-05/08 (revoke-vs-admission race: T044) | done: green
- [X] T038 [P] [US5] V12 operator attempt semantics in NEW `internal/nonce/admin_attempts_integration_test.go`: the same operation id + same op-input yields exactly one audit row and the recorded `applied`/`refused`/`nop` outcome unchanged (never upgraded); a differing op-input yields `operation_conflict` with zero writes; refusals are committed `refused` outcomes; an uncertain COMMIT retries with the SAME operation id. deps: T014,T015 | FR-08; R7; V12 | done: green
- [X] T044 [P] [US5] V11 revoke-vs-admission race test in NEW `internal/nonce/authz_revoke_race_integration_test.go`: 007 `RevokeGrant` (and `SupplyGrant`) race a 008 admission on the same `withdrawal_authorizations` row — the admission takes `SELECT … FOR SHARE` on that row after the scope row and before its own-row writes; a revoke/supply committing before the share lock is observed and the admission refuses `authorization_invalid` with zero binding; an admission taking the share lock first commits and the 007 writer waits until it does; the fixed order scope row → 006 gate reads → 007 authorization row → own rows neither deadlocks nor lets 008 write a 007 row (snapshot byte-identity); the race is resolved by 008's own admission-time lock, never by downstream 009/011 re-validation (defense-in-depth only). deps: T012,T037 | FR-17/23; observation.md §2.1/§3.4; V11; SC-05 | done: green
- [X] T045 [P] [US5] V11/V12 commit-unknown convergence across the 007 boundary in NEW `internal/nonce/authz_commit_unknown_integration_test.go`: an uncertain 008 admission COMMIT retried with the SAME `intent_id` converges on the original binding (replay) or a retryable outcome, never a second binding; an uncertain 007 `RevokeGrant`/`SupplyGrant` COMMIT retried with the SAME `operation_id` converges through the recorded attempt; neither side infers the other's outcome. deps: T012,T014 | FR-15/17; observation.md §3.4/§4; V11/V12; SC-02/05 | done: green

**Checkpoint**: All user stories independently functional; 006/007 boundaries honored structurally.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Regression, CI, runbook, acceptance sign-off — no new behavior

- [X] T039 [P] 002–007 regression gate (unit + integration): `go test ./...` + `go test -tags integration ./...` green. Diff allowlist (reviewed with test evidence; being listed does NOT bless arbitrary changes inside a file): ALLOWED-NEW `internal/nonce/**`, `internal/app/nonceadmin.go` + `nonceadmin_test.go`, `internal/config/config_008_test.go`, `internal/nonce/*_integration_test.go`, `migrations/000008_nonce_manager.sql`, `specs/008-nonce-manager/**`; ALLOWED-MODIFY `cmd/txharbor/main.go` (dispatch case only), `internal/app/serve.go` (read mount + rebuild gate + reconcile loop start only), `internal/config/config.go` (read-token + knob passthrough only), `internal/metrics/metrics.go` + `metrics_test.go` (new series only). Any diff to `internal/indexer/**`, `internal/withdrawal/**`, `internal/db/**`, `migrations/000001–000007`, or the shared `writeGuard`/lease/coordinator constants → STOP, explicit review + full regression re-run. Historical CI runs are baseline context, never 008 evidence. done: gate green. NOTE (2026-09-16 directed verification): unchecked — full-gate red on 3 migration-discovery failures (internal/db TestConfirmationMigrationDowngradeTo4RemovesAbove4 / internal/withdrawal TestWithdrawalMigrationUpgradeDowngradeFrom006 / internal/withdrawal TestWithdrawalRecoveryPeriodNoExecutionArtefacts), each proven byte-identical at base 6a3ce1d; cause = lane migration 000008 in the embedded FS vs hardcoded 000001–000007 expectations; gate condition NOT narrowed; fix in progress. STOP triggered (test-only diff to internal/db + internal/withdrawal): reviewed — no 000001–000007 SQL, prod code, or shared-constant change; expectations derived from embedded file list, 007 upgrade assertions + 007-range absence proof retained. RECHECK (2026-09-16): gate green after fix — gofmt empty, vet + vet-integration 0, build 0, unit 10pkgs ok, integration per-package ok (app/config/db/eth/health/logx/metrics/nonce/withdrawal/indexer), directed 3/3 pass. Evidence index: see §Evidence Index & Appendix (T039).
- [X] T040 [P] Lint/vet/build gate: `gofmt -l .` empty + `go vet ./...` + `go vet -tags integration ./...` + `go build ./...` per Makefile (existing `ci.yml` four jobs; no workflow change). done: green. Evidence index: see §Evidence Index & Appendix (T040).
- [X] T041 Operator runbook + environment-isolation record in `specs/008-nonce-manager/quickstart.md` (append §操作 runbook + §资源隔离记录, 007 T032 / 005 T029 precedent; no new contracts file): `nonce-admin mint → hold-release / binding-release / register / disable / status` flows, operation-id capture rule, exit codes 0/1/2, DSN trust root, uncertain-COMMIT same-op-id retry rule, and the V1–V13 checklist execution record; isolation is workdir-local only — database/schema `txharbor_008`, PostgreSQL `127.0.0.1:55432`, Anvil `127.0.0.1:58545`, distinct compose project/volume (`txharbor008`); NEVER the shared `compose.yaml` `pgdata` / 5432 / 8545, so the sibling 009 workdir cannot collide. deps: T015 | FR-08; quickstart.md §Environment | done: runbook present; no behavior change
- [X] T042 Full V-matrix validation run + FR/SC mapping sign-off: execute V1–V13 with each SC-01–SC-09 tied to a green test; record T000-P still open; mark test-double-only unit work as NOT acceptance evidence. deps: T018–T038 | FR-22; SC-01–SC-09; V1–V13 | done: matrix record green (see per-condition check below; checked only now that every cell is satisfied — not on the SC-09 change alone). CHECK (2026-09-16, option-B run): (a) V1–V13 × SC-01–SC-09 every cell `[x]` in `quickstart.md` §7 — SC-01–SC-08 by prior integration runs (unchanged code, evidence index batch-1/2), SC-09 by NEW `TestServeSC09LiveSecretScan` + `TestSC09ScannerPositiveControl` (`internal/app/serve_sc09_integration_test.go`, real Serve + PG + Anvil, `/tmp/txharbor-008-sc09-new6.log` then full-package `/tmp/txharbor-008-sc09-app-full.log`); (b) T000-P recorded OPEN (unchanged); (c) test-double-only unit work (T020 `observe_redact_test.go`) remains NOT acceptance evidence — SC-09 no longer relies on it (the live run supersedes it; the unit test stays as mechanism coverage only). T000-P and plan.md gate A-13 both remain OPEN (independent gates, untouched by this sign-off). History: this task was `[X]` → reverted to `[ ]` (dishonest green) → now `[X]` on a real full matrix.
- [X] T043 Deferred-acceptance record (no code): state that real 009 client integration, 010 attempt linkage and 011 intent-existence/linkage cross-checks are deferred (contracts/downstream.md §4), that the FR-18 deferred half (attempt traceability, contracts/downstream.md §2) is a recorded gap and not a simulated behavior, that 008's own acceptance is scoped to admission/reconcile/read-provider behavior (quickstart.md), and that T000-P remains open. deps: T042 | FR-18 (deferred half) /FR-23; downstream.md §2/§4 | done: record present

---

## Evidence Index & Appendix (added 2026-09-16; doc-only)

**Discipline (unchanged)**: design artifacts are not evidence; test doubles (`EARLY-VALIDATION`) are not
acceptance evidence; acceptance routes through real PostgreSQL (+ real Anvil where a chain is needed).
Logs below live **outside the repo** under `/tmp` (ephemeral) and **do not embed a SHA**, so the code
version is associated from the commit timeline, never back-filled. Runs whose raw logs were not persisted
are stated **MISSING**, never reconstructed and never given an invented hash.

### T039 evidence — 002–007 regression gate

| # | command | result | code version | log availability |
|---|---|---|---|---|
| 1 | `go test -tags integration ./...` (pre-fix) | FAIL: exactly the 3 migration-discovery tests named in T039's NOTE (`internal/db/TestConfirmationMigrationDowngradeTo4RemovesAbove4`; `internal/withdrawal/TestWithdrawalMigrationUpgradeDowngradeFrom006`, `TestWithdrawalRecoveryPeriodNoExecutionArtefacts`); all other pkgs ok, incl. `internal/nonce` 226.4s | working tree ≈ `db6e764` (log has no SHA) | `/tmp/txharbor_008_integration.log` — present (13:15) |
| 2 | `go test -tags integration ./internal/db -run TestConfirmationMigrationDowngradeTo4RemovesAbove4` | PASS 4.46s | post-fix tree; committed as `8dd084c` (test-only) | `/tmp/txharbor-008-db-directed.log` — present (14:08) |
| 3 | `go test -tags integration ./internal/withdrawal -run TestWithdrawalMigrationUpgradeDowngradeFrom006` | PASS 4.20s | as #2 | `/tmp/txharbor-008-wd-directed.log` — present (14:08); that first attempt's `…NoExecutionArtefacts` was still red |
| 4 | same, `-run TestWithdrawalRecoveryPeriodNoExecutionArtefacts` | PASS 6.09s | as #2 | `/tmp/txharbor-008-wd-directed2.log` — present (14:09) |
| 5 | `go test ./...` (unit) | ok, 10 pkgs / 0 FAIL | as #2 | `/tmp/txharbor-008-unit.log` — present (14:11) |
| 6 | `go test -tags integration ./internal/app ./internal/config ./internal/db ./internal/eth ./internal/health` | ok, 5/5 | as #2 | `/tmp/txharbor-008-int.log` — present (14:13) |
| 7 | `go test -tags integration ./internal/logx ./internal/metrics` | ok, 2/2 | as #2 | `/tmp/txharbor-008-int2.log` — present (14:21) |
| 8 | `go test -tags integration ./internal/nonce` | ok, 225 PASS / 0 FAIL, 226.4s | `db6e764` (`internal/nonce/**` byte-identical at `8dd084c`) | `/tmp/txharbor-008-int-nonce.log` — present (14:33) |
| 9 | `go test -tags integration ./internal/withdrawal` | ok, 319 PASS / 0 FAIL, 485.2s | as #2 | `/tmp/txharbor-008-int-withdrawal.log` — present (14:39) |
| 10 | `go test -tags integration ./internal/indexer` (first re-run) | FAIL 2 (006-owned `TestReorgRecoveryUS1CrashResume`, `TestT028CrashDrillAndRefork`, triage-phase asserts); package killed at 600.050s | as #2 | `/tmp/txharbor-008-int-indexer2.log` — present (14:44) |
| 11 | `go test -tags integration ./internal/indexer` (full re-run) | ok 643.492s | as #2 | `/tmp/txharbor-008-int-indexer-full.log` — present (15:31) |

- Fix scope asserted by `git show --stat 8dd084c`: `internal/db/confirmation_migration_integration_test.go`, `internal/withdrawal/migration_integration_test.go`, `internal/withdrawal/recovery_period_integration_test.go`, this `tasks.md` — **test/doc only**; no `migrations/000001–000007` or production change. Gate condition was not narrowed.
- Honest discrepancy: #10 red (2 tests) and #11 green are the same package; both observations are recorded as-is. This index does **not** declare the pair flaky nor stably-failing (does not re-verify 006's own tests).
- T039-time per-story raw logs (T001–T038/T044/T045) were not persisted → **MISSING**.

### T040 evidence — lint/vet/build gate

| command | result | code version | log availability |
|---|---|---|---|
| `go vet ./...`; `go vet -tags integration ./...`; `go build ./...` | clean, 0 output | post-fix tree (committed as `8dd084c`) | `/tmp/txharbor-008-vet.log`, `-vet-int.log`, `-build.log` — present, 0 bytes (a clean run emits nothing; 0 bytes is consistent with, not proof of, a clean run) |
| `gofmt -l .` | empty (per the T040 line record) | same | **MISSING** — no dedicated log persisted |

### PENDING-GATE — orchestrator final regression gate (do not invent)

This change is doc/comment-only (`tasks.md`, `quickstart.md`, one comment block in
`internal/withdrawal/recovery_period_integration_test.go`); production code is unchanged. The final gate
must run on the HEAD containing this change and fill these placeholders:

| command | result | code version | log |
|---|---|---|---|
| `go test ./...` | PENDING-GATE | `<final HEAD; production code == 8dd084c>` | PENDING-GATE |
| `go test -tags integration ./...` | PENDING-GATE | same | PENDING-GATE |
| `gofmt -l .`; `go vet ./...`; `go vet -tags integration ./...`; `go build ./...` | PENDING-GATE | same | PENDING-GATE |

### Evidence appendix — T001–T038, T044/T045 (one appendix, not 40 edits)

- Acceptance-critical story tasks (T018–T038, T044/T045) each resolve to a named passing integration test
  in `quickstart.md` §7 (V×SC×test×code-version table). Every such test is PASS in
  `/tmp/txharbor-008-int-nonce.log` (`go test -tags integration ./internal/nonce`, 225 PASS / 0 FAIL, 14:33).
- Foundation/unit tasks (T001–T017) are covered by `go test ./...` (10 pkgs ok,
  `/tmp/txharbor-008-unit.log`) plus that nonce run; fake-RPC/scripted unit files remain
  `EARLY-VALIDATION` and are **not** acceptance evidence.
- No per-task raw logs survive → **MISSING**; the aggregate runs above are the only retained evidence.

### Open items (updated batch-3)

- **T000-P** (production provider selection) remains **OPEN**.
- **plan.md gate A-13** (constitution XI chain-level `API → queue → nonce → signing → broadcast → confirmation` E2E) remains **OPEN**; 008 does not claim complete acceptance.
- **T042** is now **DONE** (option-B run; per-condition CHECK on the task line). Unit-only work stays excluded; T000-P/A-13 unaffected.

### Batch-2 review disposition (2026-09-16; static review 007→8dd084c → fixes in this tree)

Rule: no finding is closed by narrowing a safety contract or by written risk
acceptance. Each row ends in one of: fixed / not-a-defect (with basis) / open.
Code version for all green rows below: this batch's tree (unit 909 pass / 12 pkgs,
`/tmp/txharbor-008-batch2-unit.log`; nonce integration 236 pass,
`/tmp/txharbor-008-batch2-nonce.log`; withdrawal directed 2 pass,
`/tmp/txharbor-008-batch2-wd.log`; vet + vet-integration + build clean, gofmt empty).

P1:
1. Divergent view could drive irreversible consume — FIXED: `applyObservationTransitionTx`
   now excludes `divergence` exactly like `unavailable` (reconcile.go); the anomaly
   observation + `chain_view_divergence` hold are still recorded by the caller.
   Proof: NEW `TestNonceReconcileDivergenceNeverTransitions` (contradictory L>P never
   consumes; regressing P<P_prev never flips to in_flight; hold still established).
2. Read path ignored the rebuild gate — FIXED per existing contract (read-api.md §2
   already promised it): `ReadProvider` takes the gate and answers `unavailable`
   before any DB access; serve wires the startup gate into the provider (serve.go).
   Proof: NEW `TestReadAPIGateClosedIsUnavailable` (unit) + `TestNonceReadHandlerGateClosedIsUnavailable` (app).
3. Read tx without lock/statement bound — FIXED: `applyReadGuards` runs the shared
   statement guard + `SET LOCAL lock_timeout = '5s'` first in every read tx; guard or
   lock failure maps to retryable `unavailable` with tx rollback. No new knob: reads
   take no lock beyond one snapshot, so they reuse the repo's shared write bound.
   Proof: NEW `read_lock_timeout_is_unavailable_then_releases` subtest (real PG: blocked
   read fails closed ≈5s, pooled tx released, next read bound).
4. Admission sampled the waterline but never advanced it — FIXED: `allocateInTx`
   persists last_latest/last_pending via `updateScopeFrontierTx` on a successful read
   (unavailable/divergence views leave the waterline intact for the PendingPrev
   regression check). Reconcile ticks already persisted it (reconcile.go:421), so both
   writers now agree. Proof: release_version test re-verifies against the persisted
   waterline (explicit tick-simulation reset documented in-test); classify unit +
   full nonce integration green.
5. Rebuild probes missed domains/constraints — FIXED: `requiredConstraintNames` now
   covers all seven 008 tables' named carriers (pkey/UNIQUE/FK/CHECK) and
   `VerifyRebuild` adds row-level probes for registry/events/observations/ops-audit
   plus sender/chain/registry_seq/version domains on bindings/scopes/holds. Scope:
   008 tables only — not a whole-DB audit (unchanged contract). Proof: rebuild
   integration tests green on real PG.
6. Reconcile loop had no backoff/failure metric — FIXED: per-sender bounded backoff
   (`interval<<(streak-1)`, cap `interval*8`, map pruned to live scopes, single
   goroutine so no lock; cancellable between scopes) + label-free
   `txharbor_nonce_reconcile_failures_total` (no sender/cause/error labels, FR-21).
   Holds gained the label-free `txharbor_nonce_holds_total` with the same discipline.
   Proof: NEW backoff integration test (failing scope retried at bounded cadence,
   healthy scope unaffected) + extended `TestNonceMetricsContract`.
7. T042 claimed green while quickstart contradicted it — FIXED by honesty, not code:
   T042 reverted to `[ ]`; the real V×SC×test×code-version table lives in
   quickstart §7 with the SC-09 cell STOPped pending a business ruling (options A/B/C
   recorded there; default C: stays open). No contract language was rewritten to pass.
8. T039/completion records lacked traceable evidence — FIXED: §Evidence Index &
   Appendix above (per-command result, code version, log pointer; MISSING stated
   where logs were not persisted; 0-byte clean-run logs annotated as consistent-with,
   not proof-of). This batch's runs are logged at `/tmp/txharbor-008-batch2-*.log`.

P2 (decided on real call paths, not blind adoption):
- `BigFromNumeric` range — FIXED: now rejects >2⁶⁴−1 via `ValidateNonceRange`
   (+ unit case). It is on the real read path (scope/bind parsing), so the check is load-bearing.
- `NewAllocator` optional gate — FIXED by strengthening: gate is now a required
   parameter (no caller can silently lose R5/FR-13); all call sites updated (build green).
- Unauthenticated non-GET returned 405 — FIXED: auth precedes the method check
   (401, no `Allow` leak); authenticated non-GET still 405. Proof: extended
   `TestNonceReadHandlerAuthAndMethod`.
- RPC dial after `net.Listen` — FIXED: dial moved before listen; a dial failure
   aborts startup before the port accepts anything (all closes preserved).
- mismatch/by-intent lock order — NOT A DEFECT, documented: not_bound returns before
   any durable scope is known; mismatch echoes the immutable binding row under the same
   snapshot with no annotation reads — neither takes the durable-scope lock, so there
   is nothing to order (readapi.go + contracts/read-api.md §4 bilateral note).
- quickstart metrics wording ("reads/admin series") — FIXED by correction to the real
   series (allocations/replays/observations/holds/reconcile-failures); FR-21 requires
   structured fields, not those series names, so this is prose accuracy, not a contract change.
- RPC retry upper bound — FIXED + proven: fault double now counts per-method calls;
   admission asserts exact `fltFaultCalls` per fault, `<= fltMaxAttempts` on the first
   method, and zero head-read after a failed earlier read (observation stops at first failure).
- Converge/replay vs gate/auth/registry re-verify — NOT A DEFECT: the gate is checked
   at `Allocate` entry before any replay/converge path, and the gate is monotonic
   (closed→open once at startup, never closes), so no mid-call re-verify can change the
   verdict. Replay returns the binding created under its original authorization (input
   equality required); post-commit revoke races resolve to "admission first → binding
   stands" per the approved T033 coexistence semantic, with 009/011 re-validation as
   defense-in-depth per T044 — re-verifying inside replay would rewrite that approved semantic.
- Pending-only spike for new scopes — pinned, no new rule: NEW
   `TestNonceBootstrapPendingOnlySpikeAdmitsAtP` (real Anvil, automine off) proves
   0=latest<pending=P admits at P with zero holds and zero merge (existing policy).
- 006-active hold release — pinned: NEW `TestNonceHoldReleaseUnderActive006Recovery`
   proves release clears ONLY the named hold; the 006 row, coexisting holds, and the
   006 pause are byte-untouched (006 pause still blocks allocation per T033).
- Admin×allocation interleave — NOT A DEFECT (no new test): admin (admin.go:214) and
   allocation (allocate.go:380) take the scope row `FOR UPDATE` first in the same
   documented order (coordination → scope → 006 gates → 007 share → own rows), so no
   reverse order exists to cycle; concurrency is covered by T018 + attempt semantics (T038).

VERIFY (unconfirmed suspicions — checked, none became defects):
- caller_id absent from `authorizationVersionDigest` — NOT A DEFECT: 008 takes no caller
   identity input (`AllocationRequest` has intent/chain/sender/authorization only); the
   authorization is referenced purely by (id, chain) and 007's row is authoritative.
   The digest covers every 008-read field of that row; a 007-side reassignment is a
   007 state transition caught by the admission-time `FOR SHARE` + active predicate, not
   by the historical digest (evidence, not a live guard).
- Authorization expiry equality boundary — NOT A DEFECT: `expires_at > clock_timestamp()`
   fails closed at exact equality (treated expired). Safe side, no change.
- 006-probe degradation masking other errors — NOT A DEFECT: any non-006 in-tx statement
   error returns `unavailable` before commit; `degradedBy006Failure` can only be true when
   every fact read succeeded and the sole swallowed error is the best-effort 006 probe —
   and that serve-instead-of-collapse behavior is the approved T034 contract (real-PG test).
- Recovery-period row-count stability as proof — ADDRESSED as documentation scope, not
   test expansion: the withdrawal comment now states Phase B is path-scoped (no-new-rows/
   no-new-tables in the above-007 set), with the 002–006 snapshot + phase/seq + event-count
   assertions living in `TestWithdrawalRecoveryPeriodZeroSideEffectsOutsideScope`
   (comment-only change to a 007-owned file; no SQL/prod change; withdrawal directed 2/2 green).

Remaining gaps (not defects, recorded open): T000-P, A-13 — see Open items above,
unchanged. T042 no longer gaps (option-B run, see T042 CHECK).

### Batch-3 option-B run (2026-09-16; SC-09 live evidence + one in-spec fix)

Scope: NEW `internal/app/serve_sc09_integration_test.go`
(`TestServeSC09LiveSecretScan` + `TestSC09ScannerPositiveControl`) and a 3-line
nil guard in `internal/nonce/allocate.go`. No standard/exception added, no T042
rule rewritten.

- Finding (real, triggered by the new test, fixed in-spec): the first-ever
  end-to-end `sender_not_registered` refusal faulted with a nil dereference —
  `o.registrySeq = registry.RegistrySeq` on an absent row (introduced db6e764;
  prior tests only refused disabled senders, never the row-absent case). Fix:
  record the version only when the row is present, zero value otherwise; refusal
  outcome/reason unchanged. Regression: the SC-09 run itself (refusal path green)
  + full nonce integration below.
- SC-09 evidence: credential decoy (read token, real config + Bearer paths) 0 hits
  across serve stdout/stderr, slog stream, all HTTP bodies, admin outputs, startup/
  shutdown; `token=`-shaped decoy 0 hits with its `sc09-kv-` prefix present in logs
  (emitted, only the shape masked); scanner positive control independent; non-empty
  assertions on every surface (no vacuous pass). Conclusion scoped to the covered run.
  Scope statement (reviewable): SC-09 covers key/credential material; contract-pinned
  verbatim echo of operator-chosen IDs (§3.1/T034) and FR-21 log fields are specified
  behavior, not leaks.
- Code version: nonce rows re-verified on the final tree (allocate.go guard):
  `go test -tags integration ./internal/nonce` ok 122s
  (`/tmp/txharbor-008-sc09-nonce-full.log`); `.../internal/app` ok 97s
  (`/tmp/txharbor-008-sc09-app-full.log`); `go test ./...` 10 pkgs ok
  (`/tmp/txharbor-008-sc09-unit.log`); `gofmt -l` empty, `go vet ./...` +
  `go vet -tags integration ./internal/app/ ./internal/nonce/` + `go build ./...`
  clean. quickstart §7's `db6e764`＝`8dd084c` code-version line predates the guard;
  the batch-3 runs above supersede it for `internal/nonce/**` and `internal/app/**`.
- Old-evidence validity (dependency-checked, not package-diff-only): indexer/db/eth/
  health/logx/config neither import `internal/nonce` nor traverse the changed
  `Serve()` startup order, and the metrics change is additive — their 8dd084c
  evidence stands; every package touching the change (app, nonce, metrics) was re-run.


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
