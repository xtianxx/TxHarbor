# Tasks: 011 Withdrawal Execution Worker（提款执行）

**Input**: Design documents from `specs/011-withdrawal-executor/` — spec.md (clarify 2026-09-17: M1n/M2/M3 closed), plan.md, research.md (R1–R14 + threshold proposals), data-model.md (Tables 1–7), contracts/ (api.md, gates.md, lifecycle.md, persistence.md), quickstart.md (V1–V13 + operator runbook + environment knobs), checklists/requirements.md; `.specify/memory/constitution.md` v1.1.0; `docs/project-context.md`; `docs/workflow-010-011-parallel.md` (joint contract v1: P1–P5, C1–C12, J1–J6, merge follow-through records, both limited Q3 exceptions in J4; provisional migrations `000011` = 010 / `000012` = 011 / `000010` = PB).

**Prerequisites**: plan.md (required) + spec.md (required for user stories) confirmed present; research.md, data-model.md, contracts/, quickstart.md present (verified via the two read-only scripts below).

**Tests**: TDD was not requested by the spec or plan; no tests-first unit tasks are created. Each story gets an Independent Test criterion plus validation tasks that execute the quickstart V-matrix on real HTTP + real PostgreSQL + real migration `000012`. 011 has **no chain code path** (`internal/execution` imports no RPC/dial package), so Anvil belongs to the joint wave only. The 010 boundary uses contract-shape doubles labeled test-only (T030); they are NEVER cited as joint verification (C10; FR-14). J1–J5 are owned by 010 (`010:T046`–`010:T050`) and are not duplicated here.

**Organization**: one phase per user story (US1–US6 in spec priority order), a residual-freeze/recovery phase inside US6, the 011-side integration-readiness wave feeding 010's joint wave, then Polish. All tasks are unchecked; nothing in this file was executed.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel (different files, no dependency on an incomplete task)
- **[Story]**: US1–US6 for user-story phase tasks only; Setup / Foundational / integration-readiness / Polish tasks carry no story label
- Every task names its exact file path
- All tasks are unchecked

## Inputs & constraints (these are INPUTS — do not re-open, do not weaken)

- **Adjudicated business rulings**: M1n (FR-04 equal re-competition; new version + all-gates re-verification; old leases/versions never revived; new qualification never auto-permits surviving old tasks), M2 (FR-10 fact revision needs no authorization and no claim; subsequent sends follow Q3 + PB conditional reuse), M3 (FR-15 automatic takeover after explicit disqualification; "no progress" never proves lease failure; automatic re-open is scheduling qualification only, never a business-state reset) — all 2026-09-17. Inherited OC-1–OC-7, PB-C1/C2, 010 Q1–Q3; 006 FR-26. Tasks MUST NOT re-ask or weaken them.
- **Approved threshold semantics (2026-09-17 initial values)**: `lease_ttl` = 30s, `heartbeat` = 10s (±10% jitter), `stall_window` = 300s (independent value, not TTL-linked). Lease validity = DB authoritative time + a **successfully committed** renewal; a heartbeat initiated / process alive / renewal-result-unknown does NOT extend qualification; an expired qualification MUST NOT be revived by a late renewal and MUST be re-acquired as a new version. Config change/restart alone MUST NOT extend an existing qualification. Backoff/cadence remain technical values (not business limits).
- **010 inherited limited exceptions (inputs; NO re-adjudication)**: G-010-1 (natural-expiry residual after the final `clock_timestamp()` evaluation: record + reconcile, never described as legal in-flight, no grace/TTL, prior checks never reused across queueing/backoff/reconnect/retry, definitive results kept) and G-010-2 class (c) (unperceived lock-loss window: same-session probe mitigation only, single dispatch, no transparent retries, freeze + controlled manual review on proof, reconcile never permits resend, full gate re-verification before any resend, not actual verification). Recorded in register J4.
- **COMMIT-failure semantics (mandatory)**: a failed COMMIT MUST NOT mechanically rewrite everything to `unknown`. Three facts stay distinct wherever 011 records 010 outcomes (steps / unknown-recovery / projection): (i) confirmed-not-sent this attempt (no send result; 010-side `region_aborted_no_dispatch`-style abort) → recorded as no-send, business effect stays `unknown` pending reconcile; (ii) a previously-`unknown` business effect stays persisted pending reconcile; (iii) known RPC/on-chain results (`sent`/`refused_gate`/`refused_basis`, verified receipts) are preserved and never rewritten. Recovery bases are persistable evidence only — never inference.
- **Joint wording (mandatory)**: integrate 011's real implementation + migrations into the integration workspace, then execute real joint acceptance; applicable joint gates complete BEFORE 011 merges to main. Any phrasing that places joint acceptance after 011's mainline merge MUST NOT be used.
- **Migration numbering**: provisional (`000012` = 011); `000010` is occupied by PB; re-verify the actual set at merge; never renumber or rewrite applied migrations (C12/J6). The intent-FK follow-up migration is 010-owned (`010:T044`); before it closes the FK MUST NOT be claimed to exist.
- **Boundary rules**: 011 never allocates/consumes/releases nonces or bindings, never signs, never constructs/sends transactions, never writes 007/008/009/010 tables; the claim row is the only 011 object 010 reads, and 010-read-only. No new infrastructure (no Redis/Kafka/broker), no new module, no new listener.
- **C11**: display-latency business limit stays unspecified — mechanism only; no numeric SLA may be written into any task, test, or acceptance. **C12**: migration-number + FK-ordering residual recorded; gap reported for ruling, never self-widened.
- **Open items**: A-13 (full-chain joint E2E) and T000-P (production KMS/HSM provider selection) remain OPEN — constraints, not deliverables here.

## Applicability check (read-only, run before generation)

- `SPECIFY_FEATURE_DIRECTORY="specs/011-withdrawal-executor" bash .specify/scripts/bash/setup-tasks.sh --json` → `FEATURE_DIR=/home/dream/product_env/TxHarbor/.slim/worktrees/prep-011-worker/specs/011-withdrawal-executor`, `AVAILABLE_DOCS=[research.md, data-model.md, contracts/, quickstart.md]`, `TASKS_TEMPLATE=…/.specify/templates/tasks-template.md`.
- `bash .specify/scripts/bash/check-prerequisites.sh --json` → same `FEATURE_DIR`/docs; plan.md + spec.md confirmed present.
- Worktree clean before generation (`git status --porcelain` empty); branch `011-withdrawal-executor`; baseline `c011e9f2ed8ac4e7129586c462d07fd68a573570`.
- All tasks below match `- [ ] Tnnn [P?] [USn?] <description with exact file path>`; none are checked.

## Wave overview

| Wave | Phases | Entry gate | Exit evidence |
|---|---|---|---|
| W0 | Phase 1–2 (T001–T013) | design docs approved (no re-plan) | migration up/down + named-constraint probes |
| W1 | Phase 3 / US1 (T014–T018) | W0 checkpoint | V1 (PG + HTTP) |
| W2 | Phase 4 / US2 (T019–T021) | W0; claim core exists | V2 (+ race variants) |
| W3 | Phase 5 / US3 (T022–T026) | W2 (claim core + worker loop) | V3, V4, V5 |
| W4 | Phase 6 / US4 (T027–T033) | W1 (intent/gates) + W2 | V6, V7, V8 |
| W5 | Phase 7 / US5 (T034–T037) | W4 (advance + reconcile consume facts) | V9 |
| W6 | Phase 8 / US6 (T038–T040) | W4 | V10 |
| W7 | Phase 9 integration readiness (T041–T046) | W1–W6 complete; 011 implementation committed | 011 handover ready; joint wave (010:T042–T051) unblocked |
| W8 | Phase 10 Polish (T047–T053) | W3–W6 | V11, V12, doc-sync |

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: shared scaffolding with no story-specific behavior; disjoint files, parallelizable.

- [x] T001 [P] Extend `internal/config/config.go`: add the 011 worker knobs `TXHARBOR_WORKER_TTL_SECONDS` (default 30), `TXHARBOR_WORKER_HEARTBEAT_SECONDS` (default 10; TTL/3, ±10% jitter applied at runtime), `TXHARBOR_WORKER_STALL_SECONDS` (default 300; independent, MUST be > TTL), `TXHARBOR_WORKER_BACKOFF_BASE_MS` (1000), `TXHARBOR_WORKER_BACKOFF_MAX_MS` (30000), `TXHARBOR_WORKER_SCAN_INTERVAL_MS` (1000), `TXHARBOR_WORKER_LABEL`; fail-closed startup validation (all positive; heartbeat < TTL; stall > TTL; no app-clock expiry anywhere). TTL/heartbeat/stall are the values approved 2026-09-17 as **initial config** (R14 approval), not blanket business limits; a config change/restart alone MUST NOT extend an existing qualification (R14 approval condition).
- [x] T002 [P] Extend `internal/metrics/metrics.go` (+ one feature-scoped file only if the registry is feature-scoped): claim acquisitions/takeovers/revocations, stall flags, open steps, unknown-pending, reconcile outcomes, gate refusals by class, projection staleness, advance latency/error classes (R13; constitution XII; asserted by V12/FR-13).
- [x] T003 [P] Create `internal/execution/errors.go`: the closed refusal taxonomy with retryability exactly per `contracts/persistence.md` §5 (`execution_permission_denied`; `authorization_invalid`/`authorization_expired`/`authorization_revoked`/`authorization_unverifiable`/`authorization_changed`; `scope_mismatch`; `sender_unregistered`; `recovery_paused`/`recovery_active`/`recovery_version_changed`; `gate_read_failed`; `binding_paused`/`binding_absent`/`binding_conflict`/`binding_terminal`/`binding_read_failed`; `claim_lost`/`claim_not_current`; `step_open_unreconciled`; `lifecycle_unavailable`; `operation_conflict`) + the `execution_events.kind` constants mirroring the data-model Table 4 CHECK list verbatim (`admitted, admission_refused, claimed, released, taken_over, revoked, stall_flagged, state_changed, step_issued, step_converged, step_refused, step_unknown, reconcile_observed, revision_applied, projection_stale, projection_refreshed`).
- [x] T004 [P] Create `internal/execution/lifecycle.go`: the 011→010 consumer boundary — `LifecycleAdvancer`/`LifecycleReader` interfaces and `AdvanceRequest`/`AdvanceOutcome`/`LifecycleFacts`/`AdvanceAction` shapes verbatim per `contracts/lifecycle.md` §2 (011 passes only identity/fencing/action/`step_id`/anchors — never nonce/fees/calldata/signatures), the closed outcome-class set (`sent|refused_gate|refused_basis|pending_unknown|reconcile_required|unavailable`), and the class→step-state mapping per §7 (`pending_unknown`/`reconcile_required` → step `unknown` + intent `reconciling`, NEVER failure/not-paid).

**Checkpoint**: shared scaffolding builds (`go build ./...`); no story work started.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: durable carriers + upstream reads every user story depends on.

**CRITICAL**: no user story work can begin until this phase completes.

- [x] T005 Create `migrations/000012_withdrawal_execution.sql` (goose headers; **provisional** number per C12/J6) with Table 1 `payment_intents` — columns verbatim: `intent_id TEXT NOT NULL` (PB-scope-declared identity; shape 1..128 printable), `request_id TEXT NOT NULL` (FK → `withdrawal_requests.request_id`), `chain_id BIGINT NOT NULL`, `sender TEXT NOT NULL` (lowercase `0x`+40 hex; cross-checked against the registry at admission), `authorization_id TEXT NOT NULL` (007 grant identity; 1:1), `authorization_version BIGINT NOT NULL` (PB scope version observed at admission; ≥1; re-verified equal on every advance), `state TEXT NOT NULL`, `state_version BIGINT NOT NULL DEFAULT 1` (monotonic CAS version; ≥1), `admitted_recovery_version BIGINT NOT NULL`, `admitted_at`/`updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`; named constraints verbatim: `payment_intents_pkey PRIMARY KEY (intent_id)`, `payment_intents_request_uniq UNIQUE (request_id)`, `payment_intents_authorization_uniq UNIQUE (authorization_id)`, `payment_intents_request_fkey FOREIGN KEY (request_id) REFERENCES withdrawal_requests (request_id)`, `payment_intents_intent_shape CHECK (length(intent_id) BETWEEN 1 AND 128 AND intent_id ~ '^[\x21-\x7e]+$')`, `payment_intents_sender_check CHECK (sender ~ '^0x[0-9a-f]{40}$')`, `payment_intents_chain_id_check CHECK (chain_id > 0)`, `payment_intents_authorization_version_check CHECK (authorization_version >= 1)`, `payment_intents_state_check CHECK (state IN ('admitted','claimed','executing','completed','failed','reconciling','revised'))`, `payment_intents_state_version_check CHECK (state_version >= 1)`; and Table 2 `execution_claims` — columns verbatim: `intent_id TEXT NOT NULL` (PK; FK → `payment_intents`), `owner_id TEXT NOT NULL` (16B hex worker instance identity, per indexer `NewOwnerID`), `lease_version BIGINT NOT NULL` (≥1; monotonic per intent; takeover = +1; old versions never revive), `state TEXT NOT NULL` (`active|released|revoked`), `acquired_at TIMESTAMPTZ NOT NULL DEFAULT now()`, `expires_at TIMESTAMPTZ NOT NULL`, `last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now()` (renewal evidence; does NOT advance progress), `last_progress_at TIMESTAMPTZ NOT NULL DEFAULT now()` (the only stall-predicate input), `stall_flagged_at TIMESTAMPTZ`, `ended_at TIMESTAMPTZ`, `end_kind TEXT` (`released|revoked`), `updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`; named constraints verbatim: `execution_claims_pkey PRIMARY KEY (intent_id)`, `execution_claims_intent_fkey FOREIGN KEY (intent_id) REFERENCES payment_intents (intent_id)`, `execution_claims_owner_check CHECK (length(owner_id) BETWEEN 1 AND 128)`, `execution_claims_lease_version_check CHECK (lease_version >= 1)`, `execution_claims_state_check CHECK (state IN ('active','released','revoked'))`, `execution_claims_state_consistency CHECK ((state = 'active') = (ended_at IS NULL AND end_kind IS NULL))`, `execution_claims_end_kind_check CHECK (end_kind IS NULL OR end_kind IN ('released','revoked'))`, `execution_claims_expiry_check CHECK (expires_at > acquired_at)`. There is **no** `caller_id` column on intents (single source = `withdrawal_requests`); intents have no delete/invalidate path (identity root); claim rows only update, never delete (data-model Tables 1–2; J1/J2).
- [x] T006 Extend `migrations/000012_withdrawal_execution.sql` with Table 3 `execution_steps` — columns verbatim: `step_id TEXT NOT NULL` (PK; 011-preallocated 16B hex), `intent_id TEXT NOT NULL` (FK → `payment_intents`), `action TEXT NOT NULL`, `state TEXT NOT NULL`, `owner_id TEXT NOT NULL` (issuing instance identity; evidence), `lease_version BIGINT NOT NULL` (issuing qualification version; the fencing basis), `attempt_id TEXT` (010-owned identity reference; no fact copy), `anchor_attempt_id TEXT`, `tx_hash TEXT`, `outcome_class TEXT`, `recovery_version BIGINT`, `revision_version BIGINT`, `evidence TEXT NOT NULL DEFAULT ''` (no keys/raw signed bytes), `issued_at`/`updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`; named constraints verbatim: `execution_steps_pkey PRIMARY KEY (step_id)`, `execution_steps_intent_fkey FOREIGN KEY (intent_id) REFERENCES payment_intents (intent_id)`, `execution_steps_action_check CHECK (action IN ('first_broadcast','replay','replace'))`, `execution_steps_state_check CHECK (state IN ('issued','converged','refused','unknown'))`, `execution_steps_outcome_class_check CHECK (outcome_class IS NULL OR outcome_class IN ('sent','refused_gate','refused_basis','pending_unknown','reconcile_required','unavailable'))`, `execution_steps_lease_version_check CHECK (lease_version >= 1)`, `execution_steps_tx_hash_check CHECK (tx_hash IS NULL OR tx_hash ~ '^0x[0-9a-f]{64}$')`, `execution_steps_attempt_shape CHECK (attempt_id IS NULL OR (length(attempt_id) BETWEEN 1 AND 128 AND attempt_id ~ '^[\x21-\x7e]+$'))`, and partial unique index `CREATE UNIQUE INDEX execution_steps_open_uniq ON execution_steps (intent_id) WHERE state = 'issued'` (one open send step per intent; makes "reconcile before decision" structural); and Table 4 `execution_events` (append-only; `event_id BIGINT GENERATED ALWAYS AS IDENTITY`, `intent_id TEXT NOT NULL` with no FK — refusals before admission must be recordable; `kind/from_state/to_state/lease_version/step_id/attempt_id/revision_version/detail/at`) with `execution_events_kind_check` carrying the same 16-kind closed list as T003 and `CREATE INDEX execution_events_intent_time_idx ON execution_events (intent_id, at)` (data-model Tables 3–4).
- [x] T007 Extend `migrations/000012_withdrawal_execution.sql` with Table 5 `request_status_projection` (display-only; references identity, never copies facts): `request_id TEXT NOT NULL` (PK), `intent_id TEXT NOT NULL` (UNIQUE; FK → `payment_intents`), `execution_state TEXT NOT NULL`, `state_version BIGINT NOT NULL` (011-side source version), `lifecycle_attempt_id TEXT` (010 attempt identity reference only), `lifecycle_version BIGINT NOT NULL DEFAULT 0` (consumed authority revision version), `lifecycle_observed_at TIMESTAMPTZ`, `freshness TEXT NOT NULL` (`confirmed|possibly_stale`), `stale_since TIMESTAMPTZ`, `updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`; named constraints verbatim: `request_status_projection_pkey PRIMARY KEY (request_id)`, `request_status_projection_intent_uniq UNIQUE (intent_id)`, `request_status_projection_intent_fkey FOREIGN KEY (intent_id) REFERENCES payment_intents (intent_id)`, `request_status_projection_state_check CHECK (execution_state IN ('admitted','claimed','executing','completed','failed','reconciling','revised'))`, `request_status_projection_freshness_check CHECK (freshness IN ('confirmed','possibly_stale'))`, `request_status_projection_stale_consistency CHECK ((freshness = 'possibly_stale') = (stale_since IS NOT NULL))`; plus Table 6 `execution_caller_permission` (`caller_id BIGINT NOT NULL` PK, `can_execute BOOLEAN NOT NULL DEFAULT FALSE`, `updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`, `updated_by TEXT NOT NULL DEFAULT ''`; constraints `execution_caller_permission_pkey PRIMARY KEY (caller_id)`, `execution_caller_permission_caller_fkey FOREIGN KEY (caller_id) REFERENCES caller (caller_id)`); plus Table 7 `execution_ops_audit` (`audit_id BIGINT GENERATED ALWAYS AS IDENTITY`, `operation_id TEXT NOT NULL`, `action TEXT NOT NULL`, `intent_id TEXT NOT NULL DEFAULT ''`, `caller_id BIGINT`, `subject_version BIGINT`, `outcome TEXT NOT NULL`, `operator TEXT NOT NULL`, `reason TEXT NOT NULL DEFAULT ''`, `evidence TEXT NOT NULL DEFAULT ''`, `detail TEXT NOT NULL DEFAULT ''`, `recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()`; constraints `execution_ops_audit_pkey PRIMARY KEY (audit_id)`, `execution_ops_audit_operation_id_uniq UNIQUE (operation_id)`, `execution_ops_audit_action_check CHECK (action IN ('permission_set','permission_revoke','claim_revoke','projection_refresh'))`, `execution_ops_audit_outcome_check CHECK (outcome IN ('applied','nop','refused'))`); then `+goose Down` drops all seven tables in exact reverse order (pure DDL; additive only; no triggers/stored functions).
- [x] T008 [P] Verify `migrations/000012_withdrawal_execution.sql` on a scratch DB: `up`/`down`/`up` clean; negative probes hit the exact named constraint (`payment_intents_request_uniq`, `payment_intents_authorization_uniq`, `execution_claims_pkey`, `execution_claims_state_consistency`, `execution_steps_open_uniq`, `execution_ops_audit_operation_id_uniq`, `request_status_projection_stale_consistency`, `execution_events_kind_check`) → 23505/23514 classified by `ConstraintName` only; additive-only schema diff against `000001`–`000010` (no upstream object changes; 011 tables are the only new objects); provisional number re-verified against the actual set, never renumbered/rewritten once applied (C12; V12.1/V12.2).
- [x] T009 [P] Create `internal/execution/gates.go`: upstream gate reads per `contracts/gates.md` §0–§2 — `LOCK TABLE indexer_pause, log_pause, deposit_pause, reorg_recovery, reorg_recovery_events IN SHARE MODE`; one-statement 006 snapshot with version semantics `current_version = active recovery_seq, else events MAX, else 0`; 007 request row read for admission (`caller_id, chain_id, asset, recipient, amount::text, status` requiring `status='accepted'`); grant row `FOR SHARE` (exists, `state='active'`, `expires_at IS NULL OR expires_at > now()`, field equality on `caller_id/chain_id/asset/recipient/amount` compared as integer decimal); scope row `FOR SHARE` in the same sequence (`intent_id`, `request_id`, `sender`, `authorization_version`; fee caps and `allows_fee_replacement` read but not adjudicated by 011); `SET LOCAL statement_timeout` guard on every such transaction; DB `now()` only for validity; any lock wait/timeout/deadlock ⇒ `gate_read_failed`, fail closed. Only `SELECT` + gate-table `SHARE` locks against 006 objects; zero upstream writes.
- [x] T010 [P] Create `internal/execution/binding.go`: the 008 observation adapter over 008's read surface (reuse the exported `ReadProvider` exactly as 009's `binding_live.go` does) returning the five classes verbatim (`BindingMatches|BindingAbsent|BindingConflict|BindingPaused|BindingTerminal|BindingReadFailed`); only `BindingMatches` permits issuing a send-class step; `BindingPaused` ⇒ wait/reconcile (no send); `BindingTerminal` ⇒ no send (audit-only); `BindingAbsent`/`BindingConflict`/`BindingReadFailed` ⇒ fail closed; 011 never allocates/consumes/modifies/releases a binding (OC-3); the observed class is recorded as evidence; the business import graph stays free of 008's RPC-bearing read package (gates.md §3).
- [x] T011 [P] Create `internal/execution/intent.go`: intent read + the explicit `payment_intents.state` machine (data-model state machine A) with the two transition classes physically separated — **send-enabling** transitions (`admitted→claimed`, `claimed→executing`, `reconciling→executing`, `revised→executing`) guarded by `(state, state_version)` CAS **and** an existing valid claim (`owner_id` + `lease_version` + `active` + unexpired) **and** full current gates; **fact** transitions (`→reconciling`, `→completed`, `→failed`, `→revised`) guarded only by `(state, state_version)` CAS + version monotonicity, consuming no authorization and requiring no claim (M2); illegal transitions refused with zero writes and recorded as errors, never as transitions; `refusal ≠ transition`; append-only `execution_events` writes carrying identity/version evidence; finality: `completed` may be revised by authority, `failed` is terminal for automatic execution (no auto-repay).
- [x] T012 Create `internal/execution/claim.go`: the claim/lease core — `Acquire` as one atomic CAS (`INSERT … ON CONFLICT (intent_id) DO UPDATE … WHERE <claimable> RETURNING lease_version`; claimable = `state <> 'active'` OR `expires_at <= now()` OR `now() - last_progress_at >= stall_window`; losers serialize on the row and re-evaluate the predicate against the winner's committed row); `Renew` as a lock-first short transaction (`SELECT … FOR UPDATE` → verify `owner_id` + `lease_version` + `state='active'` + `expires_at > now()` → extend `expires_at`, `last_heartbeat_at`; any mismatch ⇒ `ErrClaimLost`, the holder stops writing immediately and re-observes); **progress advance** as a claim-fenced exact-version update whose sources are durable facts only — step issue/converge, authority-revision consumption, reconcile observations, state transitions — while renewals, re-claims, backoff and legitimate waiting are NOT progress (R6); `Release` (graceful exit); every qualification change is a single-row exact-version update (`WHERE intent_id=$1 AND owner_id=$2 AND lease_version=$3 …`), never a batch or lock-free claim (the 011 side of the J4 ordering obligation); expiry comparisons on the DB clock only.
- [x] T013 Create `internal/execution/steps.go`: the execution-position carrier — insert-first `execution_steps(issued)` issue with the 011-preallocated `step_id` (16B hex) plus `action`, `owner_id`, `lease_version`, observed `recovery_version`; the partial unique index `execution_steps_open_uniq` makes "one open step per intent" structural (a new step is impossible until the open one reaches a terminal state); fenced converge (`issued→converged|refused|unknown`) as an exact-version guarded update carrying `attempt_id`, `tx_hash`, `outcome_class`, `revision_version`; an `issued` row stays durable evidence until reconciled; step state machine per data-model C.

**Checkpoint**: schema + gate reads + intent/claim/step carriers ready — user story work can begin.

---

## Phase 3: User Story 1 — 执行准入与意图创建（Priority: P1）🎯 MVP

**Goal**: admit an `Accepted` 007 request (authenticated caller + fixed interface permission + valid per-request authorization + current gates) and persist exactly one stable payment intent **before** any 008 reservation.

**Independent Test**: drive admission with a compliant request and with missing/expired/revoked-authorization and paused-period requests; assert the former creates exactly one intent and the latter create zero; assert the intent exists before any 008 reservation (joint half) (quickstart V1).

### Implementation for User Story 1

- [ ] T014 [US1] Create `internal/execution/admit.go`: admission core (T-admit) — in one transaction: gate-table `SHARE` → request row read (exists, belongs to the authenticated caller, `status='accepted'`) → registry row `FOR SHARE` (`active` for `(chain_id, sender)`, else `sender_unregistered`) → grant `FOR SHARE` (`authorization_invalid`/`authorization_expired`/`authorization_revoked`) → scope `FOR SHARE` (`authorization_unverifiable` when absent — never a silent fresh-grant reinterpretation; `scope_mismatch` on intent/request/sender mismatch; `authorization_version ≥ 1` persisted) → 006 gates (observed recovery version recorded) → `INSERT payment_intents` (`intent_id` = the scope's declared identity; sender fixed and bound to `intent_id + chain_id`) → `INSERT execution_events('admitted')` → `INSERT request_status_projection` (`state_version=1`); `23505` on `payment_intents_request_uniq`/`payment_intents_authorization_uniq` ⇒ rollback → re-read → same identity basis = recorded outcome, different basis = 409 conflict with zero writes; any gate failure ⇒ **zero intent rows** + best-effort `admission_refused` event; Accepted is never admission or authorization (FR-01/FR-02/FR-03/FR-07; OC-1/C1; T-admit).
- [ ] T015 [US1] Create `internal/app/withdrawalexecution.go` and register the route on the existing `serve` listener (`internal/app/serve.go`): `POST /withdrawals/{request_id}/execution` — Bearer `txh_…` via 007's read-only `Authenticate`, fixed interface permission `execution_caller_permission.can_execute = TRUE` (absent/FALSE ⇒ 403 `forbidden`, fail-closed, checked before any domain read); empty body (the intent identity is the PB scope's declared value — nothing client-forgeable); mapping 201 (created) / 200 (recorded replay, same basis) / 400 / 401 / 403 / 404 (missing and foreign render identically — no existence leak) / 409 (different identity basis, zero writes) / 422 (recorded refusal class) / 503 (storage unavailable, never a false refusal); responses carry no credentials/keys/raw bytes; the endpoint never reserves nonces, claims, signs, broadcasts, creates compensation intents, or mutates 007 rows (`contracts/api.md` §1).
- [ ] T016 [US1] Create `internal/app/withdrawalexec.go` and add dispatch in `cmd/txharbor/main.go`: the `withdrawal-exec` operator subcommand skeleton with `permission-set` / `permission-revoke` (upsert / set `can_execute=FALSE` on `execution_caller_permission`; already-false ⇒ `nop` recorded); every mutation carries `--operation-id`/`--operator`/`--reason` and is deduplicated by `execution_ops_audit.operation_id` (23505 ⇒ rollback ⇒ read back ⇒ same input reports the recorded outcome, different input `operation_conflict` with zero writes — the 008 R7 protocol) (FR-01; api.md §3).
- [ ] T017 [P] [US1] Execute V1 (PostgreSQL side) in `internal/execution/admission_integration_test.go`: compliant request ⇒ exactly one `payment_intents` row, `intent_id` = the scope's declared value, `UNIQUE(request_id)`/`UNIQUE(authorization_id)` hold, `admitted` event + projection row with `state_version=1`; sequential/concurrent ×N repeats ⇒ exactly one intent, zero second intents; refusal matrix (missing/expired/revoked grant; missing scope; request/sender mismatch; unregistered sender; pause row; active recovery; `can_execute=FALSE`; foreign caller; non-`accepted` request) ⇒ zero intent rows + refusal evidence; unit assertions: admission gate matrix, intent shape/sender/state checks. The "intent exists before the 008 binding" ordering assertion is the **joint half** and MUST NOT be claimed here (010:T046; J1) (FR-01/FR-02/FR-03/FR-07; SC-01).
- [ ] T018 [P] [US1] Execute V1 (HTTP side) in `internal/app/execution_http_integration_test.go`: real `serve` routes — 201/200/401/403/404/409/422/503 mapping, ownership rendering identical 404, recorded-replay body shape, permission fail-closed with 401 before any DB read, zero rows on refusals, no secrets in bodies/logs (FR-01/FR-02; V12 hygiene overlap; api.md §1).

**Checkpoint**: US1 fully functional and testable independently (MVP).

---

## Phase 4: User Story 2 — 领取、续租与执行资格版本（Priority: P1）

**Goal**: one worker holds a valid lease on an intent; the lease is renewable and expiry disqualifies; qualification advances by version.

**Independent Test**: two workers race the same claim (exactly one wins); the holder's actions are refused after expiry; old-version bases are refused after version advance (quickstart V2).

### Implementation for User Story 2

- [ ] T019 [US2] Create `internal/app/withdrawalworker.go` and add dispatch in `cmd/txharbor/main.go`: the `withdrawal-worker` long-running loop skeleton — scan claimable/admitted intents → `Acquire` → heartbeat renewal at the `heartbeat` cadence (10s ±10% jitter) while active → graceful `Release`/shutdown; a renewal result is never treated as progress, and a renewal whose COMMIT result is unknown is **not** treated as an extension (the holder re-observes the claim row before any further write); every write is exact-version fenced so a disqualified holder stops immediately on `ErrClaimLost`; no in-memory authority (restart rebuilds from PG) (FR-04; R4/R5; T-claim/T-renew).
- [ ] T020 [P] [US2] Execute V2 (exclusivity/lease/renewal/expiry) in `internal/execution/claim_integration_test.go` (real PG, real connections, race detector): N-way concurrent `Acquire` on one intent ⇒ exactly one winner, losers return no-row with zero side effects, winner `lease_version=1`; a renewal before expiry extends `expires_at` on the same version; freezing the winner past TTL ⇒ any fenced write affects 0 rows, a claim read shows `expires_at <= now()`, and a new `Acquire` takes over with `lease_version+1`; old-version write/step-issue attempts ⇒ `claim_lost`, zero writes, zero sends (FR-04; SC-02; quickstart V2).
- [ ] T021 [P] [US2] Execute V2 race-variants in `internal/execution/claim_race_integration_test.go` (race detector): (a) **late renewal** — a renewal attempted after expiry with the old version fails and the qualification is never revived; re-acquire receives a new version; (b) **renewal commit-unknown** — a renewal transaction whose COMMIT result cannot be confirmed is treated as "not extended" until the holder re-observes; DB time is the only arbiter; (c) **contention** — multiple workers over multiple intents with bounded backoff + jitter produce no unbounded loops, no application-clock expiry, and no qualification errors (R14 approval conditions; M1n; J4 "no grace/TTL").

**Checkpoint**: US1 + US2 both work independently.

---

## Phase 5: User Story 3 — 失格与合法接管竞争（Priority: P1）

**Goal**: a disqualified worker's writes are refused and its three send kinds are fenced at 010; a legitimate taker re-verifies all gates and continues the same intent/history without a second intent.

**Independent Test**: make a worker disqualified and have it keep writing and requesting sends (writes refused; 010-side refusal is joint); have a legitimate taker re-verify and resume; assert intent continuity and history integrity (quickstart V3/V4/V5).

### Implementation for User Story 3

- [ ] T022 [US3] Extend `internal/execution/claim.go` + `internal/app/withdrawalworker.go`: stall/disqualification/takeover (M3/R6) — the stall predicate `state='active' AND now() - last_progress_at >= stall_window` (300s approved initial) is re-evaluated **under the claim row `FOR UPDATE`**; takeover happens in one transaction: `lease_version+1`, new owner, refreshed `expires_at`/`last_progress_at`, cleared `stall_flagged_at`, and a `taken_over` event carrying prior owner/version/watermark; the sweep idempotently marks stalled-but-not-taken claims (`stall_flagged_at` + `stall_flagged` event + metric) without changing business state (FR-15 evidence); two exhaustive timelines — progress-first ⇒ predicate false, no takeover; takeover-first ⇒ the old holder's progress write affects 0 rows and is fenced; old in-process tasks NEVER inherit the new qualification (they must re-claim through the same CAS and re-verify all gates — M1n equal re-competition).
- [ ] T023 [US3] Extend `internal/app/withdrawalexec.go` + `cmd/txharbor/main.go`: operator `claim-revoke` (evidence-required; `expected_lease_version`-guarded single-row update → `state='revoked'`, `ended_at`/`end_kind='revoked'`, `revoked` event, audit; insufficient evidence or version mismatch ⇒ refused with zero writes, and never "chase" a newer version) and the read-only inspection paths `claim-show`/`step-list`/`event-list` (no audit rows) — an escape hatch, never a required gate for automatic takeover (M3; FR-15; api.md §3).
- [ ] T024 [P] [US3] Execute V3 in `internal/execution/fencing_integration_test.go`: with worker A holding a valid claim, force disqualification (expiry / operator `claim-revoke` / stall takeover) ⇒ A's converge and state transitions affect 0 rows; every execution write remains a single-row exact-version update (structural assertion: no lock-free claim, no cached permit); a "no send result" for the current attempt is recorded as such — never as failure and never as "nothing happened" — and a previously-unknown business effect stays persisted pending reconcile while known results are preserved; the 010-side refusal of A's three send kinds is the **joint half** (010:T047; J2) and MUST NOT be claimed here — the independent run asserts 011's side with the contract-shape double (T030) labeled test-only (FR-05; SC-03; quickstart V3).
- [ ] T025 [P] [US3] Execute V4 in `internal/execution/takeover_integration_test.go`: after disqualification (expiry or revocation) the **same** worker may re-acquire and receives a new `lease_version` (no identity ban); re-acquisition re-verifies all current gates (authorization/pause/recovery/registry/binding) and refuses with zero sends on any gate failure; the new holder continues the same `intent_id`, the same nonce binding reference and the same attempt history; zero second intents; old-version steps are visible but cannot be converged by the old version; any write with the old version is fenced (M1n; FR-04/FR-06; SC-04; quickstart V4).
- [ ] T026 [P] [US3] Execute V5 in `internal/execution/stall_integration_test.go`: no false takeover (progress-first — a claim with recent progress is not taken over; the CAS predicate is false under the row lock); takeover (stall-first) atomically invalidates and re-issues with `lease_version+1` and a `taken_over` event carrying prior owner/version/watermark, and the old holder's next write affects 0 rows; renewal alone does not prevent takeover and repeated re-claiming is not progress; the sweep marks without taking over when no candidate exists, retaining FR-15 evidence and resetting nothing (intent state, nonce binding, history, pauses untouched); `claim-revoke` requires evidence and the exact `expected_lease_version` (M3; FR-15; SC-04; quickstart V5).

**Checkpoint**: US3 fully functional; disqualified workers are fenced at 011's write paths.

---

## Phase 6: User Story 4 — 调用 010：重试、崩溃与未知复用（Priority: P2）

**Goal**: drive 010 to advance execution; retry/crash/unknown reuse the same intent, nonce and attempt history; unknown first reconciles and never becomes a new payment.

**Independent Test**: inject call timeout, crash-restart and unknown outcomes; assert the same intent/history is reused and unknown enters reconcile without rebuilding a payment (quickstart V6/V7/V8).

### Implementation for User Story 4

- [ ] T027 [US4] Create `internal/execution/advance.go`: the send-class step driver — T-step-issue (gate-verified: claim `FOR UPDATE` verification, 006 gates, authorization/scope re-verification, binding observation; `replace` refused unless the scope's `allows_fee_replacement=TRUE`) persists `execution_steps(issued)` **before** the call; `LifecycleAdvancer.Advance` runs **outside** every transaction carrying only identity/fencing/action/`step_id`/anchors; T-step-converge is claim-fenced and maps the closed outcome classes to terminal step states with the mandatory three-case separation for recorded 010 outcomes: (i) **confirmed-not-sent this attempt** (010 abort with no send result) is recorded as no-send with the business effect remaining `unknown` pending reconcile — never a failure claim and never mechanically rewritten to `unknown`; (ii) a **previously-unknown business effect** stays persisted pending reconcile (`unknown` + `reconciling`, `reconcile_observed`); (iii) **known RPC/on-chain results** (`sent`, `refused_gate`, `refused_basis`) are preserved verbatim; every recorded class is backed by persistable evidence (step row + identity references + observed basis), never inference; retries reuse the same `step_id` (never a second attempt/identity), are bounded, and never bypass gate re-evaluation (FR-08/FR-11; Q3; G-010-1/G-010-2 as inputs; lifecycle.md §3/§4/§7; persistence.md §2/§4).
- [ ] T028 [US4] Create `internal/execution/reconcile.go`: T-reconcile — read `LifecycleReader.Read(intentID)` facts (identity references only; never copy facts), converge the open step by `(intent_id, step_id)`, record `reconcile_observed` with the authority revision version, advance progress, and transition `reconciling→executing` only on positive "no attempt/no side effect" evidence or an authority-required retry — never by inference; `unknown` is never converted to failure/"not paid"; a new send step is impossible while an open step exists (structural index); a failed read marks the projection possibly-stale (no business-state rewrite) and retries boundedly; consume the unknown-recovery interface `(attempt_id, tx_hash) + persisted facts + recovery conditions` per lifecycle.md §5; the recovery basis is persistable evidence only (FR-08; C9; J5; persistence.md §6).
- [ ] T029 [US4] Extend `internal/app/withdrawalworker.go`: worker cycle integration — scan → claim → renew → `advance`/`reconcile` → projection consumption; startup catch-up reconciles every open (`issued`) step first, rebuilds execution position from `execution_steps` + `execution_events` + 010 facts, and runs a version-ordered projection catch-up; no external call while holding DB locks; crash points per `persistence.md` §8 resolve probe-first (never assume-not-sent); bounded backoff classes are distinguished and never collapsed (FR-08; constitution VI/IX; persistence.md §4/§8).
- [ ] T030 [P] [US4] Create `internal/execution/lifecycle_double_test.go` (integration build tag): contract-shape double for `LifecycleAdvancer`/`LifecycleReader` keyed by `(intent_id, step_id)` with the closed class set — labeled test-only, NEVER cited as joint verification (C10; FR-14; R12) — plus deterministic fault switches (refused classes, `pending_unknown`, response loss, `unavailable`, abort-with-no-send-result) used by the 011-independent validations.
- [ ] T031 [P] [US4] Execute V7 in `internal/execution/advance_integration_test.go` (against the labeled double): `refused_gate`/`refused_basis` ⇒ converged with evidence; `pending_unknown` ⇒ step `unknown` + intent `reconciling`, zero failure claims; a retry with the same `step_id` never creates a second attempt (the double asserts keyed convergence); crash after issue, before/after the 010 call ⇒ restart reconciles the open step; re-issue of the same `step_id` only with positive "no attempt persisted" evidence; the no-send-result abort stays distinct from unknown and from known results; bounded retries leave the step `issued` for the next cycle. The joint reconcile run is 010:T048 and MUST NOT be claimed here (FR-08; SC-05; quickstart V7; persistence.md §8 crash matrix).
- [ ] T032 [P] [US4] Execute V6 in `internal/execution/authorization_integration_test.go`: admission with a valid grant, then revoke/expire it ⇒ subsequent send-enabling step issue refuses (`authorization_revoked`/`authorization_expired`) with zero calls to the 010 boundary; scope re-supply bumps `authorization_version` ⇒ `authorization_changed`, no silent rebinding; re-authorized under current gates ⇒ a new step may be issued, never a second intent; import/statement assertion: no code path in the worker or CLI supplies/revokes a 007 grant (scheduling ≠ issuance) (FR-07; SC-03; quickstart V6).
- [ ] T033 [P] [US4] Execute V8 in `internal/execution/recovery_gate_integration_test.go`: a pause row inserted after claim but before step issue ⇒ refusal `recovery_paused`, zero sends, zero "queued for later"; an active `reorg_recovery` row ⇒ refuse and unknown stays in reconcile; multiple independent pause rows — clearing one does not bypass the others (011 reads only, never deletes/releases); a version move between 011's observation and 010's send ⇒ 010 `refused_basis` recorded as converged/refused, then re-observe and retry under current gates — never a grace period (FR-11; SC-05; quickstart V8; gates.md §1).

**Checkpoint**: US4 fully functional; the 010 boundary is driven exclusively through the narrow consumer interfaces.

---

## Phase 7: User Story 5 — 请求状态投影与新鲜度（Priority: P2）

**Goal**: display-only request status with source version + freshness; the projection never overwrites newer with older, is marked possibly-stale when authority is unconfirmed, and is never read by a decision path.

**Independent Test**: construct lagging-revision and unknown-freshness scenarios; assert no old-over-new overwrite, stale marking, and zero decision reads of the projection (quickstart V9).

### Implementation for User Story 5

- [ ] T034 [US5] Create `internal/execution/projection.go`: T-projection — two version-monotonic input streams (`state_version` from 011, `lifecycle_version` from 010's revision version); apply only when the incoming version is greater (`SET … WHERE request_id=$r AND <column> < $incoming`; older arrivals affect 0 rows and never overwrite newer); rows reference attempt identity and never copy transaction facts; a failed/unconfirmed authority read sets `freshness='possibly_stale'` + `stale_since=coalesce(stale_since, now())` without rewriting stored versions or converting known results; a successful read at version ≥ stored clears to `confirmed`; the table is never read by any decision path (claim, step issue, reconcile, revision) — enforced statically and at runtime (FR-09; Q1; J5; R9; data-model Table 5 apply rules).
- [ ] T035 [US5] Extend `internal/app/withdrawalexecution.go` + `internal/app/serve.go`: `GET /withdrawals/{request_id}/execution` — ownership-enforced (identical 404 for missing/foreign); the `execution` block is read from 011's authoritative rows (`payment_intents`, `execution_claims`, `execution_steps`); the `lifecycle` block is read from `request_status_projection` and MUST include `{attempt_id, lifecycle_version, observed_at, freshness}` with `freshness ∈ {confirmed, possibly_stale}`; when possibly-stale the payload states the reference may be out of date and never presents a stale terminal state as a verified current result; the endpoint is never an admission/qualification/send/reconcile permit; no secrets (FR-09; Q1; api.md §2).
- [ ] T036 [US5] Extend `internal/app/withdrawalexec.go` + `cmd/txharbor/main.go`: `projection-refresh` (read 010 authority, apply the version-guarded projection update, `projection_refreshed` event; authority unavailable ⇒ `refused` recorded and the projection marked possibly-stale, zero business-state writes) with `operation_id` dedup and audit; **no display SLA is promised or implied** (C11 stays unspecified; mechanism only) (FR-09; api.md §3; R9/R14).
- [ ] T037 [P] [US5] Execute V9 in `internal/execution/projection_integration_test.go`: feed a newer `lifecycle_version` then an older one ⇒ the stored version only moves forward, the older input affects zero rows, the payload shows the newer attempt reference; authority unavailability ⇒ `possibly_stale` + `stale_since`, never a stale terminal presented as verified, and a recovery read clears it to `confirmed`; decision-path assertion (static scan + runtime): claim, step issue, reconcile and revision never read `request_status_projection`; forced refresh is audited with `operation_id` dedup; no SLA asserted anywhere. The joint projection/revision propagation is 010:T049 and MUST NOT be claimed here (FR-09; SC-06; quickstart V9).

**Checkpoint**: US5 fully functional; display and decision planes are separated.

---

## Phase 8: User Story 6 — 重组修订、恢复追踪与全链验收需求（Priority: P2）

**Goal**: revise results after a reorg while tracking the original intent (no compensation), observe recovery pauses, consume recovery/freeze signals, and define (not execute) the real full-chain acceptance requirements.

**Independent Test**: the step only defines requirements; testability is the FR-14 scenario list + SCs (quickstart V10/V13).

### Implementation for User Story 6

- [ ] T038 [US6] Create `internal/execution/revision.go`: T-revision — consume 010 revision rows in version order and apply only when `revision_version > stored`; authority-driven fact transitions (`completed→revised` etc.) via `(state, state_version)` CAS **without** consuming authorization and **without** requiring a claim (M2: chain-fact observation, state/projection revision and reconcile bookkeeping are not new asset execution); `revision_applied` event carrying the source version; revision never rebuilds an intent (no compensation); a subsequent send after revision re-runs the full current gate set under Q3 + PB conditional reuse (fact transitions grant no send authority); the M2 fence stays explicit: no balance ledger and no relief from normal authentication/operator permissions (FR-10; M2; SC-07; lifecycle.md §6).
- [ ] T039 [US6] Extend `internal/execution/reconcile.go` + `internal/app/withdrawalexec.go`: recovery tracking + freeze participation — counterpart **010:T039** (G-010-2 class (c) detection + freeze) and **010:T050** (J5 detectable-path joint run), resolving the 010-side `011:TBD-freeze-consumer` placeholder. Consume the 010 freeze cause/evidence through the unknown-recovery interface: while an intent is frozen, 011 refuses further sends and records the freeze class as evidence; reconcile remains allowed (chain observation/query) but MUST NOT reset the freeze, MUST NOT resend, and MUST NOT convert the condition to a known result; resending is possible only after the 010-side controlled manual release (**010:T040**) and a full gate re-verification; the manual-review handoff surfaces the frozen intent + evidence on the operator read-only paths; 011 claims no detection guarantee and no "window is tiny/rare" property (FR-10/FR-15; G-010-2 class (c) as input; register J4).
- [ ] T040 [P] [US6] Execute V10 in `internal/execution/revision_integration_test.go`: apply a revision (`completed → revised`) ⇒ same intent, same history, `revision_applied` recorded, zero new intents, zero compensation payments; the transition succeeds with no active claim and consumes no authorization (M2); a subsequent send-class step after revision requires current gates + a valid claim and refuses without them; a failed payment is never auto-repaid; an older revision input affects zero rows. The joint reorg-revision run is 010:T049 and MUST NOT be claimed here (FR-10; SC-07; quickstart V10).

**Checkpoint**: US6 implementation complete; FR-14's acceptance requirements are defined (execution deferred to the joint wave).

---

## Phase 9: Integration Readiness Wave (owner 011; feeds 010's joint wave; no story label)

**Purpose**: deliver 011's real implementation + migration `000012` to the integration workspace and verify 011-side readiness so 010's joint wave (J1–J5) can execute. The workspace creation, real-claim adapter swap, migration application and joint acceptance execution (J1–J5) are OWNER-010 (`010:T042`–`010:T051`) and MUST NOT be duplicated here. Each task below names exactly one owner (011) and the qualified counterpart 010 task.

- [ ] T041 [P] Deliver 011's real implementation + migration `000012` for the integration workspace — counterpart **010:T042** (workspace creation + integration; owner 010), resolving the 010-side `011:TBD-handover` placeholder. Record the 011 handover SHA and the exact integration inputs (migration file, config knob names, subcommand names, route patterns); the orchestrator commits (no push/PR/merge/deploy by this step); do NOT execute or claim joint acceptance until both sides' implementations + migrations are present in the workspace (P4; J6).
- [ ] T042 [P] Migration-number and application counterpart — counterpart **010:T043** (apply `000011` + `000012` in the integration scratch DB; owner 010), resolving `011:TBD-migrations`. Re-verify the provisional `000012` against the actual migration set at integration time; confirm merge-order application and that no applied migration is renumbered/rewritten and 011's migration stays additive-only; feed the record to T046 (C12; J6; PLAN-1).
- [ ] T043 [P] Validate the claim-row/FK closure post-integration in the integrated schema — counterpart **010:T044** (intent-FK follow-up migration; owner 010; must close before joint-acceptance completion), resolving `011:TBD-payment-intents`. After `010:T044` applies, verify `execution_claims_intent_fkey` (created by `000012`) and the follow-up `tx_attempts_intent_fkey` (010-owned; expected next number after `000012`, confirmed from `migrations/` at execution time) exist as named constraints and enforce 23503 on a missing `payment_intents` row; before `010:T044` closes, the FK MUST NOT be claimed to exist (G-010-3; J1; C12).
- [ ] T044 [P] Verify `execution_claims` column parity with the frozen J2 shape — counterpart **010:T045** (real-claim adapter swap; owner 010), resolving `011:TBD-execution-claims` together with T012. Assert the real column set/names/semantics consumed by 010's single mapping table match `data-model.md` Table 2 (intent-unique, worker identity, monotonic `lease_version`, expiry, revocation marker); the fixture path remains test-only for 010-independent runs; no cross-owner writes (J2; G-010-4).
- [ ] T045 Verify 011's joint-readiness for J1–J5 — counterparts **010:T046**, **010:T047**, **010:T048**, **010:T049**, **010:T050**, resolving `011:TBD-intent-claim-supply`, `011:TBD-worker-fencing`, `011:TBD-reconcile-loop`, `011:TBD-projection-updater`. Confirm the 011-side participant paths exist and run against real wiring in the integrated workspace — real admission/intent+claim supply (T014/T015), fencing + takeover (T022/T025), reconcile loop (T028), projection updater (T034), freeze consumer (T039); this task verifies 011-side readiness only — it does NOT execute joint acceptance (010 owns J1–J5) and never cites doubles/fixtures as joint evidence (FR-14/C10; J6).
- [ ] T046 [P] Record 011-side joint evidence + register inputs — counterparts **010:T051** (joint acceptance evidence + gating statement; owner 010) and **010:T058** (joint register update, single-writer 010 lane), resolving `011:TBD-joint-evidence` and `011:TBD-joint-register`. Assemble the 011-side record (handover SHA, migration verification from T042, FK validation from T043, claim parity from T044, readiness from T045, V-matrix results) and hand it to the 010-owned record tasks; state the gating form verbatim: integrate 011's real implementation + migrations into the integration workspace, then execute real joint acceptance; applicable joint gates complete BEFORE 011 merges to main. Mocks, fixtures and 010-independent passes NEVER count as joint evidence; 011 supplies content only — it does not write the single-writer register file.

**Checkpoint**: 011 handover ready; 010's joint wave (`010:T042`–`010:T051`) unblocked. J1–J5 remain NOT executed until 010's wave completes.

---

## Phase 10: Polish & Cross-Cutting Concerns

**Purpose**: matrix-closing verification and documentation sync.

- [ ] T047 [P] Execute V11 in `internal/execution/state_machine_integration_test.go`: illegal transitions (`admitted→completed` without authority facts; unclaimed `claimed→executing`; `failed→completed` without an authority revision; `→revised` without a source version) are refused with zero writes and recorded as errors; `refusal ≠ transition` (repeated gate refusals leave state unchanged and observable); every allowed transition is version-guarded (`state_version` CAS) and two concurrent actors converge to one winner with no lost update (FR-12; constitution V/VI; quickstart V11).
- [ ] T048 [P] Execute the boundary assertions in `internal/execution/boundary_test.go`: `internal/execution` imports no RPC/dial package and no 006/007/008 writer package; it contains no non-`SELECT` statement against upstream tables; the claim row is 011's only object read by 010; no transaction construction/nonce arithmetic/signing/broadcast path exists; exactly one consumer boundary to 010 (C10/FR-14; constitution VIII; gates.md §6; V12.3).
- [ ] T049 [P] Execute the secrecy/integer/observability assertions in `internal/execution/secrecy_test.go`: structured fields per event (`request_id, intent_id, caller_id, sender, owner_id, lease_version, step_id, action, attempt_id, tx_hash, authorization_id, scope_version, recovery_version, revision_version, outcome_class`) present on the V-scenario rows/events/logs; log/error/metric scan finds no credentials, keys, raw signed bytes or raw transaction bytes; amounts/fees/nonces render as integer decimal strings with no `float64` in 011 paths; the metrics from T002 are recorded (FR-13; constitution I/XII; persistence.md §9; V12.4/V12.5).
- [ ] T050 [P] Doc-sync (Q3 reference consistency; no new rulings): add references to 010's **two** limited exceptions to 011's own docs — G-010-1 (natural-expiry residual) and G-010-2 class (c) (unperceived lock-loss) — into `specs/011-withdrawal-executor/spec.md` (Clarifications / FR-11 / FR-15 vicinity), `plan.md`, `research.md`, `contracts/gates.md` §4, `contracts/lifecycle.md` §5/§7 and `quickstart.md`, by copying the already-recorded text from `docs/workflow-010-011-parallel.md` J4 (both exception texts verbatim, present in this branch) and, where 010-side detail is needed, the sibling 010 worktree documents (`.slim/worktrees/prep-010-tx/specs/010-transaction-lifecycle/{plan.md,research.md,contracts/send-gate.md}`; not present in this branch). Reference sync only — NEVER invent business text; the 011-side docs currently carry no reference, so the task exists to complete the sync.
- [ ] T051 [P] Doc-sync (mandatory wording; no new rulings): align the joint-acceptance wording in `specs/011-withdrawal-executor/plan.md` (Acceptance split / Reported limitations), `research.md` (R12), `contracts/lifecycle.md` §9 and `quickstart.md` (V13 / execution record) to: integrate 011's real implementation + migrations into the integration workspace, then execute real joint acceptance; applicable joint gates complete BEFORE 011 merges to main. Remove/replace any phrasing that implies acceptance after merging to main.
- [ ] T052 [P] Doc-sync (mandatory semantics; no new rulings): align 011's recording semantics for 010 outcomes in `specs/011-withdrawal-executor/contracts/lifecycle.md` §5/§7, `contracts/persistence.md` §6/§8 and `plan.md` D3/D4 so the three facts are expressed separately — (i) confirmed-not-sent this attempt (no send result; business effect `unknown` pending reconcile), (ii) previously-`unknown` business effect persisted pending reconcile, (iii) known RPC/on-chain results preserved and never rewritten; recovery basis tied to persistable evidence. This mirrors the 010-side correction (sibling worktree `.slim/worktrees/prep-010-tx/specs/010-transaction-lifecycle/tasks.md` Inputs & constraints, `contracts/send-gate.md` §4; not present in this branch) and MUST NOT change any business ruling.
- [ ] T053 Update the 011 status row in `docs/project-context.md` (single-writer, 011 lane) after implementation completes: record the 011 implementation/handover state, the migration number verification, and that A-13 and T000-P remain OPEN; the joint register file `docs/workflow-010-011-parallel.md` is updated by the 010 lane (010:T058) and is NOT edited here.

---

## Traceability

### Requirements (spec.md FR-01..FR-15, M1n/M2/M3) → design → ruling → tasks → acceptance

| Requirement | Design carrier (plan/research/data-model/contracts) | Ruling | Tasks | Acceptance (quickstart / joint) |
|---|---|---|---|---|
| FR-01 (admission gates: auth + fixed permission + valid grant + current gates; Accepted ≠ authorization) | plan D1; `admit.go`; api.md §1; T-admit; `execution_caller_permission` (Table 6) | OC-5/PB-C1/C2; FR-01 | T007, T014, T015, T016, T017, T018 | V1 (SC-01) |
| FR-02 (stable intent; one per request; before 008 reservation; reuse on retry/crash/unknown; never a second/compensation intent) | plan D1; `payment_intents` + `UNIQUE(request_id)`; insert-first; T-admit; R3 | OC-1/C1 | T005, T014, T017 | V1, V7, V10 (joint ordering half 010:T046) |
| FR-03 (sender from registry, fixed at admission, bound `intent_id+chain_id`) | plan D1; `nonce_wallet_registry FOR SHARE`; gates.md §3; R10 | OC-2/D4 | T009, T014, T017 | V1 |
| FR-04 (exclusive claim; lease expiry = disqualification; versioned qualification; M1n equal re-competition; new version + all-gates re-verification; old leases/versions never revived) | plan D2; `execution_claims` Table 2; `claim.go`; R4/R5/R7 | M1n (2026-09-17) | T005, T012, T019, T020, T021, T022, T025 | V2, V4 (SC-02/SC-04) |
| FR-05 (disqualified writes refused; verifiable qualification + takeover semantics to 010; three send classes fenced; sent facts kept) | plan D2; exact-version self-fencing; claim-row contract (gates.md §4); R5 | Q2; J2/J4 | T012, T013, T022, T024 | V3 (SC-03; joint half 010:T047) |
| FR-06 (legitimate takeover: re-verify all gates, same intent/nonce/history, no second intent, full history) | plan D2; T-claim/T-reconcile; intent continuity | M1n | T022, T025 | V4, V5 (SC-04) |
| FR-07 (per-request authorization consumed; scheduling ≠ issuance; bound to one intent; fail-closed; PB conditional reuse for replacement) | plan D1/D3; gates.md §2; R10 | OC-5/PB-C1/C2; M2 | T009, T014, T032 | V1, V6 (SC-03) |
| FR-08 (call 010 to advance; retry/crash/unknown reuse identity; never new payment; no conflicting identity) | plan D3; `LifecycleAdvancer` + `step_id`; lifecycle.md §2–§5 | OC-4/D6; Q3 | T013, T027, T028, T029, T031 | V7 (SC-05; joint half 010:T048) |
| FR-09 (display-only projection; authority = 010; source version + time; no old-over-new; stale marked; decisions read authority) | plan D4; `request_status_projection` Table 5; R9 | Q1/J5 | T034, T035, T036, T037 | V9 (SC-06; joint half 010:T049) |
| FR-10 (reorg revision without new authorization; keep revision chain; track original intent; no compensation) | plan D4/D5; authority-driven fact transitions; `revision.go`; lifecycle.md §6 | M2 (2026-09-17) | T038, T040 | V10 (SC-07; joint half 010:T049) |
| FR-11 (same broadcast matrix; per-send current gates; no replay exception/TTL; multi-pause; read-only observation) | plan D3; gates.md §0–§2; T-step-issue | Q3; G-010-1/G-010-2 (inputs) | T009, T027, T033 | V8 (SC-05) |
| FR-12 (explicit state machine; illegal transitions refused; durable transitions in transactions) | data-model state machines A/B/C; `intent.go` guards | constitution V/VI | T011, T047 | V11 (structure) |
| FR-13 (structured logs/observability; integer amounts; no secrets) | persistence.md §9; metrics/logx reuse; R13 | OC-7 | T002, T049 | V12 |
| FR-14 (real joint acceptance requirements; real HTTP/PG/Anvil; no mock substitution; 010→011 order; gates before 011 merges to main) | quickstart V13; plan acceptance split; J6 | C10/P5 | T045, T046 | V13 + J1–J5 (010:T046–T050) |
| FR-15 (long-stall marking with full evidence; no silent discard; automatic takeover after valid disqualification; no artificial human gates; thresholds evidenced) | plan D2/D6; stall predicate + takeover CAS + sweep marking; R6/R14 | M3 (2026-09-17) | T001, T012, T022, T026, T039 | V5 (SC-04) |

### Rulings register (adjudicated inputs → tasks → acceptance)

| Ruling | Source (2026-09-17) | Tasks | Acceptance |
|---|---|---|---|
| M1n (FR-04): equal re-competition, no identity ban; new version + all current gates; old leases/versions never revived; new qualification never auto-permits surviving old tasks | spec Clarifications; FR-04 | T012, T022, T025 | V4 |
| M2 (FR-10): fact revision needs no authorization and no claim; later sends follow Q3 + PB conditional reuse | spec Clarifications; FR-10 | T038, T040 | V10 |
| M3 (FR-15): automatic takeover; "no progress" never proves lease failure; automatic re-open = scheduling qualification only; thresholds evidenced | spec Clarifications; FR-15 | T022, T023, T026 | V5 |
| Lease/heartbeat/stall = 30s / 10s (±10%) / 300s approved as initial values | plan adjudication addendum; R14 | T001, T019, T022 | V2, V5 (+ T021 race variants) |
| Q1 (010): projection is display-only; 010 facts authority; version + time; stale marked; decisions read authority | 010 spec Clarifications; J5/C5 | T034, T035, T036, T037 | V9 |
| Q2 (010): disqualified workers' three send kinds fenced; no second intent; takeover re-verifies all gates | 010 spec Clarifications; C6 | T022, T024, T025 | V3, V4 (joint half 010:T047) |
| Q3 (010): every send needs all four gates current; no replay exception/TTL; decision-first is not in-flight; known results never rewritten | 010 spec Clarifications; C7 | T027, T032, T033 | V6, V7, V8 |
| G-010-1 limited exception (natural-expiry residual): record + reconcile; never called in-flight; no grace/TTL; prior checks never reused; definitive results kept | register J4; 010 plan/research | T027, T033, T050 | V8 (+ doc-sync T050) |
| G-010-2 class (c) limited exception (unperceived lock-loss): same-session probe only; single dispatch; freeze + manual review on proof; reconcile never permits resend; full re-verification before resend | register J4; 010 plan/research | T039, T050 | V10/US6 (+ joint detectable path 010:T050) |

### Success criteria → acceptance

| SC | Acceptance evidence | Tasks |
|---|---|---|
| SC-01 (admission intent-exactly-one 100%; zero on missing authorization/pause) | V1 | T014, T017, T018 |
| SC-02 (one winner under concurrency; zero loser side effects) | V2 (+ race variants) | T020, T021 |
| SC-03 (zero disqualified-write successes; 100% of its sends refused at 010) | V3, V6 | T024, T032 |
| SC-04 (takeover: intent continuity 100%, zero second intents, history complete) | V4, V5 | T025, T026 |
| SC-05 (retry/crash/unknown reuse 100%; zero unknown-as-failure/not-paid) | V7, V8 | T027, T028, T031, T033 |
| SC-06 (zero old-over-new; zero unmarked staleness; zero decision reads of projection) | V9 | T034, T037 |
| SC-07 (revision tracks original intent 100%; zero compensation intents) | V10 | T038, T040 |

### Acceptance index (quickstart scenarios → tasks)

| Scenario | Focus | Story | Tasks |
|---|---|---|---|
| V1 | admission / stable intent | US1 | T014–T018 |
| V2 | claim exclusivity / lease / renewal / expiry | US2 | T019–T021 |
| V3 | disqualification fencing | US3 | T024 |
| V4 | takeover / equal re-competition | US3 | T025 |
| V5 | stall / automatic takeover | US3 | T026 |
| V6 | authorization lifecycle mid-execution | US4 | T032 |
| V7 | 010 advance / retry / crash / unknown reuse | US4 | T027, T028, T031 |
| V8 | recovery pause / version / multi-pause | US4 | T033 |
| V9 | projection freshness / decision separation | US5 | T034, T037 |
| V10 | reorg revision / history | US6 | T038, T040 |
| V11 | explicit state machine / illegal transitions | Polish | T047 |
| V12 | migration / boundary / observability / secrecy | Foundational + Polish | T008, T048, T049 |
| V13 | joint acceptance requirements (design; A-13 OPEN) | Integration readiness | T045, T046 (execution: 010:T046–T050) |
| J1–J5 | joint intent/authorization, worker fencing, unknown reconcile, reorg/projection, lock-loss residual | Integration readiness | counterpart tasks T043–T046; **execution owned by 010:T046–T050** |

---

## Dependencies & Execution Order

### Phase dependencies

- **Setup (Phase 1)**: no dependencies.
- **Foundational (Phase 2)**: depends on Setup; BLOCKS all user stories.
- **US1 (Phase 3)**: after Foundational; no other story dependency (MVP).
- **US2 (Phase 4)**: after Foundational (claim/steps carriers exist); no dependency on US1 behavior.
- **US3 (Phase 5)**: after US2 (claim core + worker loop).
- **US4 (Phase 6)**: after US1 (intent/gates) and after US2 (worker loop); consumes the `advance`/`reconcile` boundary.
- **US5 (Phase 7)**: after US4 (authority reads exist to consume); projection logic is otherwise independent.
- **US6 (Phase 8)**: after US4 (reconcile/unknown-recovery path) and US5's `reconcile.go` (single-writer).
- **Phase 9 (integration readiness)**: after US1–US6 implementation is complete; gated on the 011 implementation being committed; feeds 010's joint wave.
- **Phase 10 (Polish)**: after US3–US6 (V11/V12 run over the implementation).

### Single-writer map (shared files; serialize, never parallel-edit)

| File | Ordered task chain |
|---|---|
| `migrations/000012_withdrawal_execution.sql` | T005 → T006 → T007 |
| `internal/execution/claim.go` | T012 → T022 |
| `internal/execution/reconcile.go` | T028 → T039 |
| `internal/execution/intent.go` | T011 (send-enabling + fact transitions; no later story edits) |
| `internal/app/withdrawalworker.go` | T019 → T022 → T029 |
| `internal/app/withdrawalexecution.go` | T015 → T035 |
| `internal/app/withdrawalexec.go` | T016 → T023 → T036 |
| `cmd/txharbor/main.go` | T016 → T019 → T023 → T036 |
| `internal/execution/gates.go` | T009 (shared read path; story tasks consume it, never edit in parallel) |
| `internal/execution/steps.go` | T013 (fenced converge reused by T027/T028) |
| each test file | exactly one task per file |

### Cross-lane boundary (mirror with the 010 track)

Integration workspace creation + real-claim adapter swap + migration application + joint acceptance execution (J1–J5) are **OWNER-010** and live in the sibling 010 worktree `.slim/worktrees/prep-010-tx/specs/010-transaction-lifecycle/tasks.md` (not present in this branch); they are referenced here, never duplicated. The 010-side tasks were generated in parallel and the 010 file uses `011:TBD-<slug>` placeholders (its Phase 9 legend) that MUST be replaced by these concrete 011 IDs:

| 010 counterpart (concrete) | 010 task (title) | 011 counterpart (concrete) | Resolution of 010 placeholder |
|---|---|---|---|
| `010:T039` | G-010-2 class (c) detection + freeze | T039 | `011:TBD-freeze-consumer` |
| `010:T040` | Controlled manual release | T039 (participates) | `011:TBD-freeze-consumer` |
| `010:T042` | Integration workspace creation + 011 integration | T041 | `011:TBD-handover` |
| `010:T043` | Apply migrations `000011` + `000012` | T042 | `011:TBD-migrations` |
| `010:T044` | Intent-FK follow-up migration (must close before joint-acceptance completion) | T043 | `011:TBD-payment-intents` |
| `010:T045` | Real-claim adapter swap | T012, T044 | `011:TBD-execution-claims` |
| `010:T046` | Execute J1 (intent/authorization wiring) | T045 | `011:TBD-intent-claim-supply` (via T014/T015) |
| `010:T047` | Execute J2 (expired-worker isolation + takeover) | T045 | `011:TBD-worker-fencing` (via T022/T025) |
| `010:T048` | Execute J3 (joint unknown reconciliation) | T045 | `011:TBD-reconcile-loop` (via T028) |
| `010:T049` | Execute J4 (reorg revision / projection order) | T045 | `011:TBD-projection-updater` (via T034) |
| `010:T050` | Execute J5 (lock-loss residual, detectable path) | T045 | `011:TBD-freeze-consumer` (via T039) |
| `010:T051` | Joint acceptance evidence + gating statement | T046 | `011:TBD-joint-evidence` |
| `010:T058` | Joint register update (single-writer 010 lane) | T046 | `011:TBD-joint-register` |

Each cross-phase task names exactly one owner and the qualified counterpart ID; no task is written as "handled by the other side". The 011-side joint-readiness tasks (T041–T046) are preparation/verification only; J1–J5 remain NOT executed until 010's wave runs.

### Within each story

- Carriers (schema/intent/claim/steps/gates) before behavior; gates before dispatch; core before integration; validation last.
- No task is checked until its evidence exists; a failed validation leaves the task unchecked and the story incomplete.
- 011-independent validations use the labeled contract-shape double (T030) for the 010 boundary; they are never joint evidence.

---

## Parallel Execution Examples

```bash
# Wave 0 — Setup (all different files):
Task: "T001 worker knobs in internal/config/config.go"
Task: "T002 execution metrics in internal/metrics/"
Task: "T003 refusal taxonomy in internal/execution/errors.go"
Task: "T004 consumer interfaces in internal/execution/lifecycle.go"

# Foundational — after T005–T007 (same migration file, sequential):
Task: "T008 migration reproducibility + named-constraint probes"
Task: "T009 upstream gate reads in internal/execution/gates.go"
Task: "T010 008 binding adapter in internal/execution/binding.go"
Task: "T011 intent state machine in internal/execution/intent.go"

# US1 validation wave (different files):
Task: "T017 execute V1 (PG) in internal/execution/admission_integration_test.go"
Task: "T018 execute V1 (HTTP) in internal/app/execution_http_integration_test.go"

# US2 validation wave (different files):
Task: "T020 execute V2 in internal/execution/claim_integration_test.go"
Task: "T021 execute V2 race-variants in internal/execution/claim_race_integration_test.go"

# US3 validation wave (different files):
Task: "T024 execute V3 in internal/execution/fencing_integration_test.go"
Task: "T025 execute V4 in internal/execution/takeover_integration_test.go"
Task: "T026 execute V5 in internal/execution/stall_integration_test.go"

# US4/US5 validation wave (different files):
Task: "T030 lifecycle double in internal/execution/lifecycle_double_test.go"
Task: "T031 execute V7 in internal/execution/advance_integration_test.go"
Task: "T032 execute V6 in internal/execution/authorization_integration_test.go"
Task: "T033 execute V8 in internal/execution/recovery_gate_integration_test.go"
Task: "T037 execute V9 in internal/execution/projection_integration_test.go"

# Integration-readiness wave (different files/records):
Task: "T041 011 handover package (counterpart 010:T042)"
Task: "T042 migration application record (counterpart 010:T043)"
Task: "T043 claim-row/FK validation (counterpart 010:T044)"
Task: "T044 execution_claims column parity (counterpart 010:T045)"
Task: "T046 joint evidence + register inputs (counterpart 010:T051 / 010:T058)"

# Polish wave (different files):
Task: "T047 V11 state machine in internal/execution/state_machine_integration_test.go"
Task: "T048 boundary assertions in internal/execution/boundary_test.go"
Task: "T049 secrecy/integer/observability in internal/execution/secrecy_test.go"
Task: "T050 doc-sync Q3 references"
Task: "T051 doc-sync joint wording"
Task: "T052 doc-sync recorded-outcome semantics"
```

Do NOT parallel-edit any file in the single-writer map; do NOT parallel-run tasks that share one test file; the integration-readiness and joint-wave tasks share one integration environment and run sequentially.

---

## Implementation Strategy

### MVP first

1. Phase 1 Setup → Phase 2 Foundational (CRITICAL — blocks all stories).
2. Phase 3 US1 → stop and validate (V1): the smallest demoable increment (admission → stable intent, zero-intent on refusals).
3. Phase 4 US2 (second P1) immediately after: claim/lease is the first line against duplicate payment.
4. Phase 5 US3 (third P1) after US2: disqualification fencing + takeover.
5. Deploy/demo only after all three P1 stories pass their validations.

### Incremental delivery

Setup + Foundational → US1 → US2 → US3 → US4 (010 boundary) → US5 (projection) → US6 (revision/recovery) → integration-readiness wave → Polish. Each story adds value without breaking previous stories; validation gates each step. The freeze-consumer behavior (T039) is developed with US6 but only fully exercised jointly (010:T050).

### Integration-readiness strategy

The 011 implementation lands on its branch; the orchestrator commits. Phase 9 hands the implementation + migration `000012` to the integration workspace (T041, counterpart 010:T042), verifies migration application (T042 / 010:T043), validates the claim-row/FK closure after 010:T044, checks `execution_claims` column parity (T044 / 010:T045), confirms 011-side readiness for J1–J5 (T045 / 010:T046–T050), and hands the evidence + register inputs to the 010-owned record tasks (T046 / 010:T051 + 010:T058). Real joint acceptance is executed in the integration workspace by 010 after both sides' implementations + migrations are present; applicable joint gates complete BEFORE 011 merges to main. Doubles, fixtures and 010-independent passes never count as joint evidence (C10; FR-14).

### Gates and stop lines (this round)

- The formal analyze gate is retained: run `/speckit.analyze` over spec/plan/tasks after this generation and before any implementation; findings MUST be resolved first.
- This tasks round authorizes NO implementation batch: no code, migration, test, service or container is written or executed by this step; no push, PR, merge or deploy (P5 stop line). The tasks above are unchecked planning items.
- A-13 (full-chain E2E) and T000-P (production provider) remain OPEN constraints, not deliverables here.
- C11 (display-latency business limit) stays unspecified; no numeric SLA may be introduced. C12 (migration number + FK ordering residual) is recorded; the gap is reported for ruling, never self-widened.

---

## Notes

- `[P]` = different files, no dependency on an incomplete task; all other task pairs sharing a file are sequential (single-writer map above).
- Every task carries its exact file path; every story phase has Goal + Independent Test + validation tasks mapped to quickstart V-scenarios.
- COMMIT failure never mechanically rewrites everything to `unknown` (see Inputs & constraints): a no-send-result abort ≠ a send result; a persisted `unknown` business effect stays pending reconcile; known RPC/on-chain results are preserved.
- Joint wording everywhere: integrate 011's real implementation + migrations into the integration workspace, then execute real joint acceptance; applicable joint gates complete BEFORE 011 merges to main. No phrasing places joint acceptance after 011's mainline merge.
- 006/007/008/009/PB artifacts are consumed read-only; 011 writes none of their tables; the 010 boundary is a narrow consumer interface with a test-only label on its double.
- All tasks are unchecked; this file is a planning artifact and claims no execution, no analyze run, and no testing.
