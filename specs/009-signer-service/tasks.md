# Tasks: 009 Signer Service

**Input**: Design documents from `specs/009-signer-service/`

**Prerequisites**: plan.md (Go 1.26.5, pgx v5.11.0, go-ethereum v1.17.5, goose v3.28.0; single binary + subcommands, no new infra), spec.md (FR-01–FR-26, US1–US6, SC-01–SC-08), research.md (R1–R11, Q-A/Q-B user rulings 2026-09-16, feasibility block, guarantee-scope revision), data-model.md (Tables 1–6, transaction catalog, lock order, TTL-withdrawn), contracts/ (api.md, gates.md, persistence.md), quickstart.md (V1–V8 + isolation scheme)

**Tests**: Included — spec mandates failure-path/concurrency coverage (constitution IX/XI); quickstart V-matrix is the acceptance surface. Unit `go test ./internal/signer/...`; integration `go test -tags integration` on real PostgreSQL (testcontainers, dedicated DB name/ports/volumes per quickstart isolation, disjoint from 008's `txharbor_008`/PG 55432/Anvil 58545). No chain tests (009 has no RPC path). Test doubles allowed for early unit work only; gate/concurrency/recovery acceptance MUST run against real PG (constitution XI).

**Organization**: Phase 0 = external prerequisite batch (007-extension, single ownership, blocks 009 completion/merge). Phases 1–2 shared; Phases 3–8 per user story (US1–US5 P1, US6 P2). 009 owns consumer-side acceptance; 008 provider tests belong to 008 (no duplication).

## Preconditions & evidence discipline

- Worktree `/home/dream/product_env/TxHarbor/.slim/worktrees/prep-009-signer`, branch `009-signer-service`, baseline `4ebfe86`, clean. `SPECIFY_FEATURE_DIRECTORY=specs/009-signer-service`. `setup-tasks.sh --json` run from this worktree root. `before_tasks` hook is optional and the tree is clean → skipped (recorded).
- Every task below is **not started** (`- [ ]`). plan/research/data-model/contracts/quickstart/spec are design artifacts, not completion evidence. Prior throwaway probes (Go buffering, PG 18 timeout behavior, in `/tmp`, not repo artifacts) are context only.
- Upstream 006 (`8e1a440`) and 007 (`19fa11e`) are read-only inputs: no 009 task modifies `migrations/000001–000007`, `internal/indexer/**` writers, or `internal/withdrawal/**`. T000-P stays open.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: different files, no dependencies on incomplete same-phase tasks
- **[Story]**: US1–US6 for story phases; Phase 0/1/2/Polish carry none (PB for prerequisite batch)
- Every task states exact file paths, prerequisites, mapped FR / contract § / V-scenario, and an executable completion condition.

---

## Phase 0: Prerequisite batch PB (007-extension, external delivery)

**Purpose**: 007-extension batch deliverables that 009's complete delivery depends on. Single ownership (007-extension batch, NOT 008, NOT duplicated in 009 implementation phases). 009 independent module work may proceed in parallel with fail-closed behavior; 009 completion/merge waits for this batch plus full legal-path acceptance (plan Merge order). **PB working baseline**: PB is its own lane branched off `main` (not off 009) and converges after 009 merges; the carrier migration is then numbered deterministically at merge time (PB-01 `renumber-at-merge`).

- [ ] PB-01 Additive scopes-carrier migration (owner: 007-extension batch) in `migrations/` (deterministic number rule **renumber-at-merge**: at merge time the carrier takes `max(merged migration number)+1` — planning value `000010`; numbering is decided at merge, no fixed assumption about which branch lands first; if 011 merges first the same `max+1` rule applies; **only *unapplied* PB migration numbers may be renumbered — applied migration numbers MUST NOT be rewritten at merge**; pure DDL, zero change to 007 columns/read shapes/intake semantics; existing grants valid at storage level, scope row optional). Deps: none (external; PB lane off `main`, converges after 009 merges). Maps: R11 carrier, Q-B. Done: `goose validate` + up/down on scratch DB green, 007 regression suite green.
- [ ] PB-02 Extended `withdrawal-authz supply` entry with scope op-input (owner: 007-extension batch) in `internal/app/withdrawalauthz.go` (same supply transaction writes grant + scope row: `intent_id`, `request_id`, `sender`, `fee_scope`, `allows_fee_replacement`, `authorization_version`, `attested_by`; `RevokeGrant` keeps scope state/version consistent; never writable by ordinary callers or 009). Deps: PB-01. Maps: R11 write entry/consistency protocol. Done: supply+revoke round-trip integration test green, audit shows matching grant+scope.
- [ ] PB-03 Q-A trusted-issuance permission-control evidence or minimum fix (owner: 007-extension batch) against `internal/app/withdrawalauthz.go` + deployment/OS/DB permission chain (`--operator` audit-only; authenticated + authorized issuer; explicit control points and trust boundary; grant content ↔ audit traceability). Cite real evidence or add the minimum control; no per-grant cryptography required by ruling. Deps: PB-02. Maps: Q-A ruling. Done: written evidence reviewed + any fix covered by test.
- [ ] PB-04 Scope/audit consistency + Q-B controlled re-issuance procedure (owner: 007-extension batch): per-grant `authorization_unverifiable` refusal stays 009-side; historic business continuation = authorized-issuer re-verification + explicit re-issuance with traceability to old grant/request, no second intent/nonce, no silent rebinding, no history backfill; scopeless old grants stay queryable/auditable only. Deps: PB-01, PB-02. Maps: Q-B ruling. Done: procedure documented + one re-issuance dry-run recorded.

- [ ] PB-05 Full-chain upgrade verification (owner: 007-extension batch) on scratch DB, three sequences: (a) empty DB → full chain `000001…000009` + carrier; (b) 007-era DB (`000001–000007`, pre-PB) → carrier upgrade; (c) carrier `down` then re-`up` (rollback / re-upgrade order asserted). Deps: PB-01. Maps: R11 migration compatibility, plan Merge order. Done: all three sequences green; no applied migration number rewritten.

**Checkpoint (PB delivery gate)**: PB-01–PB-05 delivered by 007-extension batch — carrier migration renumbered only while unapplied; scope↔grant round-trip (PB-02) and Q-A/Q-B evidence (PB-03/PB-04) recorded; full-chain upgrade sequences (empty / 007-era / rollback-re-upgrade, PB-05) green. 009 merge gate re-checks this phase.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Package skeleton, refusal vocabulary, isolated test resources.

- [ ] T001 Create `internal/signer/` package skeleton per plan.md Project Structure (`content.go`, `validate.go`, `policy.go`, `provider.go`, `auth.go`, `gates.go`, `submit.go`, `delivery.go`, `status.go`, `errors.go` + `*_test.go` stubs) with doc comments only, no behavior; `internal/app/signerserve.go`, `internal/app/signerauth.go` stubs. Deps: none. Done: `go build ./...` green.
- [ ] T002 [P] Refusal taxonomy + retryability in `internal/signer/errors.go` with the exact machine classes from contracts/persistence.md §4 (`unauthenticated`, `signing_not_permitted`, `validation_failed`, `authorization_invalid/expired/revoked`, `authorization_unverifiable`, `recovery_paused/active`, `recovery_version_changed`, `gate_read_failed`, `signature_withheld`, `outcome_unknown`, `outcome_not_yet_visible`, `storage_unavailable`, …) + `internal/signer/errors_test.go` pinning each string. Deps: T001. Maps: FR-12/FR-23, persistence §4. Done: unit test green.
- [ ] T003 [P] Isolated validation resources per quickstart.md isolation scheme: dedicated PG database name, ports and data volumes disjoint from 008 (`txharbor_008`/55432/58545) and from default 5432/8545; documented in `specs/009-signer-service/quickstart.md` isolation section (append) + compose/test-helper wiring. Deps: none. Done: parallel 008/009 test runs do not share a database or port.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Migration, verification primitives, policy/key/auth core, gate reads. No story work begins until complete.

- [ ] T004 Write `migrations/000009_signer_service.sql` (data-model.md Tables 1–6: `signer_caller` incl. `can_sign`, `signer_credential`, `signing_requests` incl. `replacement_of` + partial anchor index `signing_requests_authorization_anchor_uniq`, `signature_results` incl. `signature_results_pkey` + `signature_results_tx_hash_uniq`, `signing_request_audit`, `delivery_admissions`; pure DDL, named constraints per 007 convention). Deps: T001. Maps: FR-04/FR-05/FR-12–FR-15/FR-17/FR-23. Done: `goose validate` green.
- [ ] T005 Migration verification in `internal/signer/migration_integration_test.go` (tags: integration; raw-SQL probes, no business imports; 000001–000009 diff allowlist; negative probes expect exact SQLSTATE/ConstraintName for UNIQUE/PK/CHECK). Deps: T004. Done: green on scratch DB.
- [ ] T006 [P] Strict content schema + canonical envelope in `internal/signer/content.go` (complete tx fields, keccak256 content hash, `types.Transaction` reconstruction, arbitrary-digest rejection, independent-verify helper) + `internal/signer/content_test.go`. Deps: T001. Maps: FR-01/FR-02/FR-03/FR-13/FR-16, R1. Done: unit green incl. digest-only refusal vectors.
- [ ] T007 [P] Field validation matrix in `internal/signer/validate.go` (chain/sender/asset/calldata/recipient/amount/fees, EIP-55, integer-only money, no floats) + `internal/signer/validate_test.go`. Deps: T001. Maps: FR-06–FR-11, contracts/api.md §2. Done: unit green incl. boundary vectors.
- [ ] T008 [P] Versioned policy in `internal/signer/policy.go` (allowlists/caps/registry, `policy_version` identity hash) + `internal/signer/policy_test.go`. Deps: T001. Maps: FR-06–FR-12. Done: unit green.
- [ ] T009 [P] `KeyProvider` boundary in `internal/signer/provider.go` (structured-tx only, local dev test-key provider, mode guard, `TXHARBOR_SIGNER_KEY_TIMEOUT` default 5s) + `internal/signer/provider_test.go`. Deps: T001. Maps: FR-20/FR-21, R2. Done: unit green; `internal/signer` imports no key material.
- [ ] T010 [P] Credential verification + caller load in `internal/signer/auth.go` (sha256 + constant-time compare, `revoked_at`, `signer_caller` incl. `can_sign`) + `internal/signer/auth_test.go`. Deps: T001. Maps: FR-04/FR-05. Done: unit green.
- [ ] T011 Gate reads in `internal/signer/gates.go` (006 one-statement pause/recovery/version read; 008 `BindingReader` five-class adapter incl. `BindingTerminal` + hold/recovery mapping; 007 grant `FOR SHARE` + equality + `authz:v1` fingerprint surrogate labeled as surrogate; joint lock order scope-row → 006 tables → 007 row → own rows) + `internal/signer/gates_test.go` (pure mapping/unit parts). Deps: T006, T007. Maps: FR-17/FR-05, contracts/gates.md §§1–3, R6/R7/R11. Done: unit green; DB-backed races deferred to US6.
- [ ] T012 Metrics on the existing registry in `internal/metrics/` (signer counters/gauges: requests, refusals by class, gate refusals, admissions by verdict, commit results; no high-cardinality labels, no secret material) + wiring test. Deps: T002. Maps: FR-24. Done: unit green.
- [ ] T013 Config + subcommand wiring: `TXHARBOR_SIGNER_*` knobs with fail-closed validation in `internal/config/config.go`; `signer-serve` wiring in `internal/app/signerserve.go` (pool + signer HTTP, key deps constructed ONLY here); `signer-auth` operator subcommand in `internal/app/signerauth.go` (issue/rotate/revoke, `apikey-auth` shape); dispatch in `cmd/txharbor/main.go`; `WriteTimeout` on the shared `serve` listener + impact review on co-hosted 007 routes (shared-file change note). Deps: T009, T010. Maps: FR-04/FR-21, R6 feasibility (transport bound), R9. Done: `go build ./...` + config validation tests green; 007 route regression green.

**Checkpoint**: Foundation ready — story work may proceed in parallel.

---

## Phase 3: User Story 1 — Structured signing & digest refusal (Priority: P1) 🎯 MVP

**Goal**: Compliant structured requests sign; arbitrary digests/messages never sign.

**Independent Test**: V1 happy path + V2 refusal vectors on real PG (SC-01).

- [ ] T014 [P] [US1] V1 happy-path integration test in `internal/signer/submit_integration_test.go` (compliant request → persist-first result → signature+hash; no broadcast/RPC imports asserted). Deps: Phase 2. Maps: FR-01/FR-03/FR-13/FR-16, contracts/api.md, persistence §1. **PB-gate**: without the scopes carrier (PB-01) no grant is verifiable, so the legal happy path is unreachable — until PB lands this task accepts the fail-closed branch only (`authorization_unverifiable`), and no legal-path sign-off is claimed here. Done: green on isolated DB (fail-closed branch until PB).
- [ ] T015 [P] [US1] V2 refusal integration test in `internal/signer/refusal_integration_test.go` (digest-only/hash-only/message/incomplete → 4xx, zero signatures; unknown-field strict JSON). Deps: Phase 2. Maps: FR-02, SC-01. Done: green, signature count 0.
- [ ] T016 [US1] Submit path `internal/signer/submit.go` (T-submit-first: insert-first → 23505 classify → row lock → gates → policy → `KeyProvider.SignTx` → persist result → `signed`; T-submit-replay incl. `outcome_not_yet_visible`). Deps: T014, T015 (tests first). Maps: FR-13/FR-14/FR-15, persistence §§1/3. Done: tests pass, `go vet` clean.

**Checkpoint**: US1 independently functional (MVP).

---

## Phase 4: User Story 2 — Authentication & least privilege (Priority: P1)

**Goal**: No anonymous or over-privileged signing; identity never self-asserted.

**Independent Test**: V3 matrix on real PG (SC-02).

- [ ] T017 [P] [US2] V3 auth/permission/ownership integration test in `internal/signer/auth_integration_test.go` (missing/invalid/revoked credential → identical 401; `can_sign=false` → 403 zero signatures; sender validated against policy/registry, never trusted as identity). Deps: Phase 2. Maps: FR-04/FR-05, contracts/api.md §4, SC-02. Done: green.
- [ ] T018 [US2] Credential lifecycle via `signer-auth` (`internal/app/signerauth.go`): issue/rotate/revoke round-trip incl. rotation grace. Deps: T013, T017. Maps: FR-04. Done: integration green.

---

## Phase 5: User Story 3 — Identity binding, conflict, determinism (Priority: P1)

**Goal**: One identity ↔ one content; retries converge; conflicts refuse; replacements follow OC-5.

**Independent Test**: V4 on real PG, N≥2 concurrent + restart (SC-03).

- [ ] T019 [P] [US3] V4 binding/conflict/determinism integration test in `internal/signer/binding_integration_test.go` (same-identity same-content incl. concurrent/response-loss → one persisted result, exactly one observable signature; same-identity different-content → 409, second signature count 0). Deps: Phase 2. Maps: FR-13/FR-14/FR-15, persistence §3. Done: green with `-race`.
- [ ] T020 [US3] Restart recovery test in `internal/signer/restart_integration_test.go` (crash pre/post result-COMMIT → same-identity retry converges, never re-signs via `signature_results_pkey`). Deps: T019. Maps: FR-14/FR-15, persistence §5. Done: green.
- [ ] T021 [US3] OC-5 conditional fee replacement in `internal/signer/replacement_integration_test.go` (`replacement_of` + partial anchor index; explicit-permit + in-scope fee reuses grant, else fresh grant + new identity; persisted rows never rebound). Deps: T019. Maps: FR-05, contracts/gates.md §2, R7/R11. Done: green (full reuse branch covered after PB batch; until then, fresh-branch + fail-closed paths). Must include one explicit **scopeless-grant → `authorization_unverifiable`** assertion as behavior (refusal recorded, zero signature, audit class), not merely the taxonomy string; the full legal-path reuse acceptance is retained for after the PB carrier lands.

---

## Phase 6: User Story 4 — Validation matrix (Priority: P1)

**Goal**: Every out-of-policy field refused before signing.

**Independent Test**: V5 matrix (SC-04).

- [ ] T022 [P] [US4] V5 validation-matrix integration test in `internal/signer/policy_integration_test.go` (chain/sender/asset/calldata/recipient/amount/fee-shape violations → 422/4xx pre-sign, zero signatures; policy version pinned in audit). Deps: Phase 2. Maps: FR-06–FR-12. Done: green.

---

## Phase 7: User Story 5 — Key isolation & secrecy (Priority: P1)

**Goal**: Keys isolated; test/deploy separation enforced; zero secret leakage.

**Independent Test**: V8 secrecy/isolation slice (SC-05/SC-07).

- [ ] T023 [P] [US5] Import-boundary test in `internal/signer/boundary_test.go` (business `serve` constructs no provider; `internal/signer` imports no `internal/withdrawal`/`internal/indexer` writers, no RPC/dial packages). Deps: Phase 2. Maps: FR-01/FR-20, plan Structure Decision. Done: green.
- [ ] T024 [US5] Key-separation enforcement test (`internal/signer/keymode_test.go` + config): test/deploy mix → refuse startup or refuse signing; deploy never silently falls back to test keys. Deps: T009, T013. Maps: FR-20/FR-21, SC-05. Done: green.
- [ ] T025 [US5] Secrecy scan test (logs/errors/metrics/responses/repo contain zero key material and zero raw signed-tx bytes on refusal paths; `logx.Redact` coverage). Deps: Phase 2. Maps: FR-22, SC-05. Done: green.

---

## Phase 8: User Story 6 — Recovery gates & delivery protocol (Priority: P2)

**Goal**: 006/008/007-gated signing and delivery; unknown handled honestly; never broadcast.

**Independent Test**: V6 + V7 on real PG (SC-06/SC-08); 008 real-integration when available.

- [ ] T026 [P] [US6] V6 gate-consumption read-only test in `internal/signer/gates_integration_test.go` (006 pause rows + active recovery + version change → refuse/version-mismatch; 008 five classes incl. `BindingTerminal`; 007 `FOR SHARE` revoke ordering; read-only diff on 006/007 tables asserted; `can_sign` off) **plus one explicit scopeless-grant → `authorization_unverifiable` behavior assertion** (refusal recorded, zero signature, audit class — not just the class string). Deps: Phase 2. Maps: FR-17/FR-18/FR-19, contracts/gates.md, SC-06. Done: green.
- [ ] T027 [US6] V7 delivery/unknown test in `internal/signer/delivery_integration_test.go` (lock order; admission+write+marker atomic region; write-failure/timeout → `unknown` never "nothing delivered"; post-write pre-COMMIT crash → `unknown_reconcile`; overlap accounting; redelivery re-passes current gates — revoked/expired/paused blocks even byte-identical bytes; V7-step-6: session-loss, partial-write+RST, missing-marker restart, audit honesty). This is the test that drives T034 (`delivery.go`); there is **no pre-existing Phase-2 delivery implementation** to wire. Deps: T026. Maps: FR-17/FR-23, contracts/persistence.md §§2/5/6, SC-06/SC-08. Done: green with `-race` against T034.
- [ ] T028 [US6] 008 real-integration acceptance in `internal/signer/binding_live_integration_test.go` (real `ReadBinding` against live 008 when 008 merges; five-class mapping; consumer scope-row `FOR SHARE` participation; contract-shape doubles retired for this path; wires T034's delivery path against the live adapter). Deps: T026 + T034 + external 008 merge. Maps: FR-18, contracts/gates.md §3, D3. Done: green; doubles for this path deleted.
- [ ] T029 [US6] Desensitized status path in `internal/signer/status.go` (own-request status only; refusal carries no signature material; `tx_hash` only on delivered/admitted) + `internal/signer/status_test.go`. Deps: Phase 2. Maps: contracts/api.md §3, FR-17. Done: unit + integration green.
- [ ] T034 [US6] T-deliver implementation in `internal/signer/delivery.go` — the final delivery protocol (there is no Phase-2 delivery implementation to inherit): fixed lock order (008 scope-row `FOR SHARE` → 006 gate tables `LOCK … IN SHARE MODE` → 007 grant `FOR SHARE` → own request/admission rows `FOR UPDATE`); gate re-read inside the lock window (fresh `ReadBinding`, never the submit-time read); one protected region = admission `INSERT` + bytes write + `delivered` marker `UPDATE` + `COMMIT`; delivery-unknown basis persisted (write failure/timeout → `ROLLBACK` → `unknown_reconcile`, never "nothing delivered"); write-failure and restart recovery by same-identity re-read + re-gate; byte-identical re-delivery re-passes current gates (revoked/expired/paused blocks even identical bytes) and never re-signs. Fault-guarantee scope only — no absolute zero-delivery claim, no TTL/`valid_until` permission window. Deps: T011, T016 (Phase-2 gates/submit), T026, T027 (tests). Maps: FR-14/FR-17/FR-23, contracts/persistence.md §§2/5/6, contracts/gates.md §3, R6. Done: T027 green with `-race`, `go vet ./internal/signer/...` clean.

---

## Phase 9: Polish & Cross-Cutting Concerns

- [ ] T030 [P] Full V1–V8 validation run + FR/SC trace sign-off (matrix recorded; SC-01–SC-08 evidenced). Deps: US1–US6. **PB-gate**: the full legal path requires the PB scopes carrier (PB-01); until it lands the matrix runs the fail-closed subset only and MUST NOT claim delivery completeness — full legal-path acceptance is retained for after PB. Done: all green, trace complete (fail-closed subset until PB).
- [ ] T031 [P] Lint/vet/build gate (`go vet ./...`, repo lint, `go vet -tags integration ./internal/signer/...`) + 002–007 regression + migration diff allowlist. Deps: US1–US6. Done: green.
- [ ] T032 Operator runbook + isolation record in `specs/009-signer-service/quickstart.md` (append: `signer-serve`/`signer-auth` ops, env knobs, dedicated DB/ports/volumes, V1–V8 execution record with SHAs). Deps: T030. Done: quickstart executes end-to-end on isolated resources.
- [ ] T033 Deferred/evidence record: 010/011 fixtures stay contract-shape test caller only (F-1); T000-P open; no conformance claimed with 010/011; probes remain throwaway context. Deps: T030. Done: recorded in plan Evidence separation (append).
- [ ] T035 Mainline sync & integration responsibility (ownership + gate only; **no sync is executed in this planning round**): when 008 merges, 009 rebases/merges onto the then-current `main` and reconciles every shared file it touches — `cmd/txharbor/main.go`, `internal/config/config.go`, `internal/app/serve.go` (`WriteTimeout`) — resolving conflicts and re-verifying affected config and migration: rerun T005 (migration integration), T026 (gate reads), T028 (008 live binding) on the merged tree. Merge vs rebase is chosen at that time from the real branch state and repo rules; history is not pre-rewritten here. **Shared-file ownership registered in this task**: `cmd/txharbor/main.go`, `internal/config/config.go`, `internal/app/serve.go` are shared with 008; `internal/signer/**` and `migrations/000009_signer_service.sql` are 009-owned. Deps: T028 + external 008 merge. Maps: plan Merge order, R6/R9. Done: post-sync build + T005/T026/T028 green on the merged tree; reconciled shared-file diff recorded.

---

## Dependencies & Execution Order

- **Phase 0 (PB)**: external 007-extension batch; no 009 code dependency. Blocks 009 completion/merge, NOT independent module work.
- **Phase 1 → Phase 2**: strict order; Phase 2 blocks all stories.
- **Stories**: US1 → US2 → US3 → US4 → US5 → US6 preferred (priority + file layering: submit → auth → binding → policy → boundary → gates/delivery); US1/US2/US4/US5 validation tasks may parallelize after Phase 2 if staffed, but US3 needs T019 semantics before US6 redelivery tests, and US6/T028 needs external 008 merge. T034 implements the delivery protocol after its tests T026/T027; T035 runs only after 008 merges.
- **Within a story**: tests first (fail before implementation where new code is written), then implementation; T016/T018/T021 wire Phase-2 code to tests; T027 is the test that drives the new T034 delivery implementation (no pre-existing delivery code is wired); T028 wires T034 against the live 008 adapter.
- **No cycles**: PB ← nothing in 009 (PB lane off `main`, converges post-009); 009 implementation → PB (waits); stories → Phase 2 only; T034 ← T011/T016/T026/T027 → T028; T035 ← T028 + 008 merge; Polish → all.

### Parallel opportunities
- [P] tasks within a phase (different files) run together: T002+T003; T006–T010; T014+T015; T023+T024; T026 V6/V7 splits by file.
-Stories parallelize after Phase 2 subject to the ordering notes above; T028 always last (external 008).
- Shared files single ownership: `migrations/000009_*` (T004 only), `internal/signer/delivery.go` (T034 only), `internal/app/signerserve.go` + `cmd/txharbor/main.go` (T013 only), shared `serve.go` `WriteTimeout` (T013 only, with 007-route review inside the same task); post-008-merge reconciliation of the shared `main.go`/`config.go`/`serve.go` is owned by T035.

## Implementation Strategy

- **MVP**: Phase 0 tracked externally + Phase 1 + Phase 2 + US1 (structured signing, persist-first, digest refusal). STOP and VALIDATE.
- **Incremental**: US2 → US3 → US4 → US5 → US6; each story independently testable on the isolated DB.
- **fail-closed ≠ done**: stories passing with refusals do not certify delivery completeness; merge waits for the PB batch (incl. PB-05 full-chain upgrade verification) + full legal-path acceptance (plan Merge order).

## Notes

- Every task carries its FR / contract § / V-scenario mapping; completion conditions are executable (`go test`, `-tags integration`, `-race` where noted).
- Real PG + real (test-container) resources for all integration tasks; doubles retire at T028 for the binding path.
- Commit after each task or logical group; stop at checkpoints to validate.

## Plan-review disposition (2026-09-16)

First-round `analyze` gap fixes; planning-only, no implement/sync/test/service run, no commit.

| Item | Disposition | File § |
|---|---|---|
| B-01 deliver-task gap | Added **T034** T-deliver implementation (`internal/signer/delivery.go`): fixed lock order, gate re-read, protected send region, persisted delivery-unknown basis, write-failure/restart recovery, same-byte re-delivery re-gate; wired to T011/T016 (prereqs), T026/T027 (tests), T028 (live 008). Removed the dangling "existing Phase-2 code" claim from T027 and the execution-order note. Fault-guarantee scope kept (no absolute zero-delivery, no TTL). | tasks.md §Phase 8 (T034), §Dependencies; T027/T028 |
| B-02 PB-gate dependency | **T014** and **T030** now state the PB carrier dependency explicitly: without a scope carrier the legal path is unreachable, only the fail-closed branch (`authorization_unverifiable`) is runnable, no legal-path sign-off. | tasks.md §Phase 3 (T014), §Phase 9 (T030) |
| B-05 scopeless assertion | **T021** and **T026** now require one explicit scopeless-grant → `authorization_unverifiable` **behavior** assertion (refusal + zero signature + audit class), not just the taxonomy string; full legal-path acceptance retained. | tasks.md §Phase 5 (T021), §Phase 8 (T026) |
| B-03 / A-04 sync ownership | Added **T035** mainline sync & integration responsibility (post-008-merge reconcile of shared `main.go`/`config.go`/`serve.go`, rerun T005/T026/T028); merge-vs-rebase decided at that time; no history rewrite and no sync executed now; shared-file ownership registered in-task. | tasks.md §Phase 9 (T035), §Parallel opportunities |
| B-04 PB numbering/baseline/gate | **PB-01** rewritten to deterministic `renumber-at-merge` (`max(merged)+1`, planning `000010`; only unapplied numbers renumbered; 011-first case uses the same `max+1` rule; the unfounded "011 merges first" assumption removed). PB baseline declared as its own lane off `main` converging post-009. Added **PB-05** full-chain upgrade verification (empty / 007-era / rollback-re-upgrade) and an explicit PB delivery-gate checkpoint. | tasks.md §Phase 0 (Purpose, PB-01, PB-05, Checkpoint) |
| Immediate-write / TTL / `valid_until` residues | grep found no "immediate write" permit phrasing; the only `TTL`/`valid_until` occurrences are explicitly marked retracted/withdrawn and "MUST NOT reappear" (research.md §§R6/R11, data-model.md §Admission) — no permissive residue to remove. No new test duplicating 008 (PB-05 is upgrade-sequence only; T028 covers the real 008 path). | research.md, data-model.md (no change) |

Task count: PB 4 → 5 (**+1**), implementation T001–T033 → T001–T035 (**+2**), total 37 → 40.
