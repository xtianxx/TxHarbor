# 011 Integration Readiness Record (T041–T046)

**Owner**: 011 lane (`011-withdrawal-executor`). **Single-writer**: this file is 011-owned content;
the joint register `docs/workflow-010-011-parallel.md` is 010-owned (`010:T058`) and is **not** written
here.

**Scope**: the 011-side record that feeds 010's joint record tasks (`010:T051`, `010:T058`). It states
what is verified and what is **not**. J1–J5 are **NOT executed**; A-13 and T000-P remain **OPEN**.

**Grounding**: read-only integration workspace `.slim/worktrees/joint-010-011`
(branch `joint-010-011-integration`, baseline `f03e825` = 010 `d2c14b9` + 011 `beba5e9`; joint records
through HEAD `3d8556e`). No write to that worktree.

---

## T041 — Handover SHA and integration inputs

**Handover implementation SHA**: `beba5e9` (`feat(011): US5 projection + US6 revision/freeze consumer
(T034-T040)`, branch `011-withdrawal-executor`). The integration workspace already contains 011's real
implementation + migration at baseline `f03e825` (010's record: `docs/workflow-010-011-parallel.md`,
011 input section). This batch adds the T041–T053 record/polish commits on top of `beba5e9`.

**Exact integration inputs**:

| Input | Value |
|---|---|
| Migration file | `migrations/000012_withdrawal_execution.sql` (provisional; 011-owned) |
| Config knobs | `TXHARBOR_WORKER_TTL_SECONDS`, `TXHARBOR_WORKER_HEARTBEAT_SECONDS`, `TXHARBOR_WORKER_STALL_SECONDS`, `TXHARBOR_WORKER_BACKOFF_BASE_MS`, `TXHARBOR_WORKER_BACKOFF_MAX_MS`, `TXHARBOR_WORKER_SCAN_INTERVAL_MS`, `TXHARBOR_WORKER_LABEL` |
| Subcommands | `txharbor withdrawal-worker`; `txharbor withdrawal-exec {permission-set, permission-revoke, claim-revoke, projection-refresh, claim-show, step-list, event-list}` |
| HTTP routes | `POST /withdrawals/{request_id}/execution`, `GET /withdrawals/{request_id}/execution` |
| Boundary interfaces | `execution.LifecycleAdvancer`, `execution.LifecycleReader` (consumer side; production adapter NOT built) |

No push/PR/merge/deploy was performed by this step. Joint acceptance is not claimed: both sides'
implementations + migrations are present in the workspace, but the prerequisites in T045 block execution.

---

## T042 — Migration number and application record

**Byte-identity** (011 lane vs integration workspace):

```text
sha256sum migrations/000012_withdrawal_execution.sql
29613714cf1d2c8d2b2bf7724efb2a2c63848cff60fed9071626c084bc3798e9  (011 lane)
29613714cf1d2c8d2b2bf7724efb2a2c63848cff60fed9071626c084bc3798e9  (joint-010-011)
```

**Merge-order set** (`migrations/` on `joint-010-011`, applied `000011 → 000012 → 000013`):

```text
000011_tx_lifecycle.sql              ff17ebbaa3d457dd42a952a038aa7f1957db7a0082a30d03e035553a31059532  (010)
000012_withdrawal_execution.sql      29613714cf1d2c8d2b2bf7724efb2a2c63848cff60fed9071626c084bc3798e9  (011)
000013_tx_lifecycle_intent_fk.sql    fcd58484b564da4d11962ddcf0b547333d8255b1081a073d47898cbc1d511439  (010 follow-up)
```

- 011's migration stays additive-only: it creates only 011 tables/indexes/constraints; no 000001–000010
  object is altered, renamed, dropped, renumbered or rewritten.
- Application in merge order is verified by 010's integration test
  `TestT043MigrationSetMergeOrder` (`internal/txlifecycle`, joint branch): goose applied `{..11,12,13}`,
  no pending, and both lane migrations byte-identical to their lane commits (010 record, `a8e2333`).
- **Known out-of-lane failure (reported, not fixed here)**: 009/PB-era scratch-overlay and migration
  sequence tests in `internal/db` hard-code the pre-011 applied set (ending at `000010`) and therefore
  fail once 011's `000012` is present — e.g. `TestT040OverlayGreenOnEmptySequence`,
  `TestT041GapFillSequenceD`, `TestT042*`, `TestT036*`, `TestAuthzScopeMigrationDownRemovesOnlyItself`.
  Confirmed **pre-existing at `beba5e9`** (stash-verified representative
  `TestT040OverlayGreenOnEmptySequence`; the others share the same hard-coded-set class), not caused by
  this batch; their owner updates them after the merge order is confirmed (same class as the 010-flagged
  `internal/db` version-11 assertion). Evidence: `/tmp/txh-011-logs/integration.txt`,
  `/tmp/txh-011-logs/precheck.txt`.

---

## T043 — intent-FK closure validation

After `010:T044` (owner 010, migration `000013`), the integrated schema names both constraints:

| Constraint | Owner | Source |
|---|---|---|
| `execution_claims_intent_fkey` FK `execution_claims(intent_id) → payment_intents(intent_id)` | 011 | `migrations/000012_withdrawal_execution.sql` |
| `tx_attempts_intent_fkey` FK `tx_attempts(intent_id) → payment_intents(intent_id)` | 010 (follow-up) | `migrations/000013_tx_lifecycle_intent_fk.sql` |

- Both are **named** constraints; neither is `NOT VALID` (`convalidated=true`).
- `23503` is enforced on a missing `payment_intents` row, classified by `ConstraintName` only. Verified
  by 010's `TestT044IntentFK` (joint branch): `tx_attempts_intent_fkey`, `23503`, existing valid rows
  unaffected (010 record, `a8e2333`).
- **011 claims ownership only of `execution_claims_intent_fkey`**; `tx_attempts_intent_fkey` is 010-owned
  and is not claimed by 011. Before `010:T044` closes, neither FK is claimed to exist.
- Not re-executed in this lane (the FK follow-up is 010's migration; 011 only records the 010 evidence).

---

## T044 — `execution_claims` column parity (frozen J2 shape)

Column-by-column parity between `data-model.md` Table 2 and
`migrations/000012_withdrawal_execution.sql` is exact:

```text
intent_id, owner_id, lease_version, state, acquired_at, expires_at,
last_heartbeat_at, last_progress_at, stall_flagged_at, ended_at, end_kind, updated_at
```

J2 semantics asserted: intent-unique (`execution_claims_pkey PK(intent_id)`), worker identity
(`owner_id` 1..128), monotonic `lease_version` (`>=1`, `lease_version+1` on takeover, old versions never
revived), expiry (`expires_at > acquired_at`, DB-clock validity), revocation marker (`state ∈
{active,released,revoked}` with `ended_at/end_kind` consistency). 010's single mapping table reads
`owner_id, lease_version, expires_at, state` (010 record, `a8e2333`) — no column-name drift. The
contract-shaped fixture remains test-only for the 010-independent suite and is never joint evidence. No
cross-owner writes.

---

## T045 — 011-side joint readiness (J1–J5 **NOT executed**)

**011-side participant paths exist and run against real PostgreSQL wiring in this lane**:

| Participant path | Carrier |
|---|---|
| Real admission / intent + claim supply (T014/T015) | `internal/execution/admit.go`, `internal/execution/claim.go`, `internal/app/withdrawalexecution.go` |
| Fencing + takeover (T022/T025) | `internal/execution/advance.go` (`verifyClaimTx`), `claim.go` (`Claim`/`Takeover`/`RevokeClaimTx`/`SweepStalls`) |
| Reconcile loop (T028) | `internal/execution/reconcile.go`, `internal/app/withdrawalworker.go` |
| Projection updater (T034) | `internal/execution/projection.go`, `internal/app/withdrawalexecution.go` |
| Freeze consumer (T039) | `internal/execution/advance.go` (`IsFrozen`), `reconcile.go` |

**J1–J5 are NOT executed.** The joint wave could not run because three prerequisites are unbuilt:

1. **Production 010↔011 `LifecycleAdvancer`/`LifecycleReader` adapter** — absent in both lanes; the only
   implementation is the test-only double (`internal/execution/lifecycle_double_test.go`), which is never
   joint evidence (C10/FR-14).
2. **Real 008 binding allocation** — the joint stack has no real `nonce_bindings` allocation/binding for
   an intent (008's real allocation path is not exercised in the joint fixture).
3. **On-chain transfer contract** — no deployed ERC-20/transfer contract with funded Anvil account for
   the real send path.

These three are the **explicit next scope** for the joint wave; this batch does not build them (no silent
expansion). J1–J5 execution remains owned by `010:T046`–`010:T050`; 011 verifies only its own readiness.

---

## T046 — 011-side evidence + gating statement

**Assembled 011-side record**: handover SHA (T041), migration verification (T042), FK validation (T043),
claim parity (T044), readiness + blockers (T045), and this batch's V-matrix results (below).

**Gating statement (verbatim, mandatory)**: integrate 011's real implementation + migrations into the
integration workspace, then execute real joint acceptance; applicable joint gates complete BEFORE 011
merges to main.

- A contract-shape double, a mock, a fixture, or a 010-independent pass is **never** joint evidence.
- 011 supplies content only; the single-writer register `docs/workflow-010-011-parallel.md` is written by
  the 010 lane (`010:T058`).
- A-13 (full-chain joint E2E) and T000-P (production provider) remain **OPEN**.

### Verification commands (this batch; log paths under `/tmp/txh-011-logs/`)

| Command | Result | Log |
|---|---|---|
| `gofmt -l .` | empty (exit 0) | `gofmt.txt` |
| `go build ./...` | exit 0 | `build.txt` |
| `go vet ./...` | exit 0 | `vet.txt` |
| `go vet -tags integration ./...` | exit 0 | `vet-integration.txt` |
| `go test -count=1 ./...` | exit 0 | `unit.txt` |
| `go test -race -count=1 ./internal/execution/ ./internal/app/ ./internal/config/` | exit 0 | `race.txt` |
| `go test -tags integration -count=1 -timeout 20m ./internal/execution/` | ok 159.4s (V1–V12 011-side) | `integration.txt` |
| `go test -tags integration -count=1 -timeout 20m ./internal/db/` | exit 1 — pre-existing out-of-lane 009/PB overlay/sequence assertions (T042 note); re-confirmed at `beba5e9` | `integration.txt`, `precheck.txt` |

V11–V12 additions this batch: `internal/execution/state_machine_integration_test.go` (V11),
`boundary_test.go` (V12.3), `secrecy_test.go` (V12.4/V12.5).

---

## Joint final acceptance re-verification (2026-09-18, clean tree)

This section supersedes the "J1–J5 NOT executed" state above for the final
acceptance only; the lane record above stays as the point-in-time handover
record. Re-verified on the joint worktree at test commit `ad1cd84` (base
`0946638`), raw logs under `/tmp/opencode/joint-final/`.

**T043 (FK closure)** — re-run and PASS: `TestT043MigrationSetMergeOrder` +
`TestT044IntentFK` (010-owned probes) and the new
`TestT043JointClaimsIntentFK`, which closes the 011-owned evidence gap the lane
record left open: `execution_claims_intent_fkey` is a named, validated
constraint on `payment_intents(intent_id)` and a missing intent row is refused
with 23503 on the exact constraint name. Log: `011-t043-t044.log`,
`010-t043-t044.log`.

**T044 (column parity)** — re-run and PASS: the new
`TestT044JointClaimsColumnParity` asserts the live `execution_claims` column set
equals the frozen J2 shape in ordinal order, every column 010's single mapping
table reads is present, and the intent-unique / `lease_version>=1` / expiry /
revocation-marker semantics are enforced by the named constraints. Log:
`011-t043-t044.log`.

**T045 (joint readiness, J participation)** — re-run and PASS: the three
blocking prerequisites recorded above are resolved in this round — the
production adapter is `txlifecycle.LifecycleLive` constructed by
`app.NewJointWithdrawalWorker` (joint wiring unit), real 008 allocation runs
through `nonce.Allocator`, and the on-chain leg uses a real Anvil node plus a
test ERC-20 Transfer emitter. All five J scenarios run through
`worker.Driver.IssueAndAdvance` over that wiring
(`TestJointDriverJ1`–`TestJointDriverJ5`) plus `TestJointWorkerProductionWiring`,
proving the 011 participant paths (admission/intent+claim supply, fencing and
takeover, reconcile loop, projection updater, freeze consumer) operate in real
joint wiring. Log: `011-t045-readiness.log`, `j1-j5-driver.log`.

**Box state**: T043/T044/T045 were set to `[ ]` before this re-verification and
restored to `[x]` only after the runs passed on the clean tree. A-13 and T000-P
remain OPEN; the production process entry point (`WithdrawalWorkerCommand`)
still constructs the standalone worker, so production adapter assembly stays
A-13 scope. The deliberate `Advance(ActionReplace)` -> `refused_basis` gap (no
010 fee policy) is recorded, not faked.

*(Point-in-time record: the "standalone constructor" clause above was closed
on current main by the A-13 consolidation below — Lane-W wired
`jointwire.Worker` into `cmd/txharbor` and the process E2E exercises it. The
historical text is kept as-is.)*

---

## A-13 acceptance consolidation (2026-09-19, current main `df5a280`)

**Purpose**: fold the Lane-W / Lane-F2 / Lane-F3 evidence (PR #16, merge commit
`df5a2808dfae976c1d0fa0d99bff9b5dc30bcf70`, main CI push run `35401917939`)
into the acceptance matrix, state per-item satisfied/gap honestly, and give the
orchestrator a closure-basis conclusion. **A-13 stays OPEN pending user
review** (this section is input, not a status change). No new adjudication; the
clause texts are the constitution XI withdrawal-flow sentence, 011 quickstart
V13-1–7, and 010 FR-16 as already recorded, read-only.

**Current-main anchors** (all read-only):

- `df5a280` = PR #16 merge (parents `3ea6eb1` + `b8a0fb0`); tree byte-identical
  to candidate `b8a0fb0`.
- Lane commits on main: `fe57888` (Lane-W process entry E2E), `28e8cc3`
  (Lane-F2 continued tracking), `b8a0fb0` (Lane-F3 B1/B2).
- Main CI push run `35401917939` (4/4 jobs): integration job ran
  `make test-integration` over the whole tree — `internal/txlifecycle` ok
  160.99s, `app` ok 134.19s, `execution` ok 101.66s, `jointwire` ok 12.30s,
  `db` ok 103.82s, `nonce` ok 100.07s, `signer` ok 98.44s, `indexer` ok
  457.51s; zero SKIP/FAIL. Sanitized log:
  `.evidence/lane-g4/logs/ci-run-35401917939.log` (run-level; the CI log is
  non-verbose, so per-test claims below cite the lane/round-2 logs, not "main
  is green" alone).
- Evidence trees (repo-external, never committed): `.evidence/lane-w/`,
  `.evidence/lane-f2/`, `.evidence/lane-f3/`, `.evidence/lane-g4/`,
  `.evidence/joint-round2/` (INDEX.md carries command/exit/sha256 per log).

### Per-item matrix

| # | Clause / completion condition | Evidence (test → assertion → log) | On current main | Verdict |
|---|---|---|---|---|
| V13-1 | Full first-broadcast chain: admission (011) → intent → claim → 010 attempt → 009 signature → Anvil send → receipt/confirmation → `completed`; identity chain traceable | `TestJointProcessEntryEndToEnd` (`internal/txlifecycle/joint_process_entry_integration_test.go:206`; binary build `:193`, child `withdrawal-worker` `:218`; identity chain `:281`; on-chain receipt `:348`; effect `:364`). Full chain through confirmation depth in one run: `TestJointProcessTrackingAfterCompleted` (`joint_process_tracking_integration_test.go:545`; completed-on-`sent` `:558`, depth reached `:580`). Logs: `.evidence/lane-w/LANE-W-NOTE.md`, `.evidence/lane-f3/logs/process-tests.log` (entry + 4 tracking E2Es), `.evidence/lane-f2/logs/lane-f2-tests.log` | Real process, real HTTP/PG/Anvil/009 through the production `jointwire.Worker` assembly; ran in main CI (txlifecycle package) | **Satisfied** |
| V13-2a | Two workers on one intent | 011 claim exclusivity under 8-way concurrent `Acquire` (one winner): `TestClaimExclusivityRenewalExpiry` (`internal/execution/claim_integration_test.go:61`); expired-worker fencing + monotonic takeover through the production worker: `TestJointDriverJ2ExpiredWorkerIsolationAndTakeover` (`joint_driver_integration_test.go:204`) | Claim-layer exclusivity + takeover proven; **no literal dual-process concurrent run** | **Partial** (evidence limitation: the race is exercised at the claim layer, not as two live worker processes) |
| V13-2b | Revoke/expiry racing a send at 010's gate (**both lock orders**); no double send / no second attempt for one step | Deterministic interleave test added (this batch); previously refusals were only sequential (`TestV6GateRefusals` `send_integration_test.go:40`; `TestDisqualificationFencesHolderWrites` `fencing_integration_test.go:87`; J2 expiry-then-send `joint_driver_integration_test.go:220`). The 008-layer revoke-vs-admission race (`internal/nonce/authz_revoke_race_integration_test.go:459`) is a different boundary. No-double-send leg **is** covered: `TestV6RaceOrdering/concurrent_send_replay` (`send_integration_test.go:184`), `already_accepted` (`:172`), crash `after_commit_before_response` (`crash_integration_test.go:175`) | Deterministic interleave now executed: `TestV13_2bRevokeGateInterleave` `internal/txlifecycle/revoke_gate_race_integration_test.go` (4 scenes, both lock orders, real PG/gates; revoke-first refusal zero-dispatch; send-lock-first fenced replay; natural expiry; queued-auth expiry; bounded barriers, sanitized timing) — 4/4 PASS + `-count=3` + race + V6 regression; logs `.evidence/lane-a13-v13-2b/` | New in this batch (committed here) | **Satisfied** |
| V13-3 | Unknown: RPC timeout / response loss / restart during send ⇒ unknown + reconcile, never failure/not-paid; no second intent/binding | `TestJointDriverJ3UnknownReconciliation` (`joint_driver_integration_test.go:363`) + direct `TestJointJ3` (`joint_j3_integration_test.go:49`); crash matrix `after_dispatch_before_commit` → `unknown` probe-first (`crash_integration_test.go:151-172`); F2/F3 no-second-intent assertions | Byte-identical test files on main; verbose evidence `joint-round2/final/txlifecycle-joint-production-assembly-20260918.log` (J1–J5 driver 5/5 PASS), crash log 8/8; main CI ran the same files | **Satisfied** |
| V13-4a | Same-bytes replay identity | `TestV3ReplayIdentity` (`replay_integration_test.go:14`, byte-identical dispatch); J2 taker replay through the production worker (`joint_driver_integration_test.go:264-290`); crash `after_commit` retry → `already_accepted` no duplicate | On main; joint-round2 verbose + crash matrix logs | **Satisfied** |
| V13-4b | Replacement under the **PB conditional reuse rule**, fee-triple test on the 010 side, history preserved | Fresh-grant replacement end-to-end: `TestJointReplaceFeeBump` (`joint_replace_integration_test.go:112`) — new attempt/signing, same intent/binding/nonce, anchor history kept, sibling `replaced` only after chain fact; refusals: `TestJointReplaceRefusals` (`:205`, no-fee-change / over-cap / reuse_forbidden). **Same-grant reuse leg blocked**: `internal/signer/submit.go:254-267` re-applies `EvaluateGrantScope` after `EvaluateGrantReuse`, and `internal/signer/gates.go:283-297` requires `scope.RequestID == req.SigningRequestID`, so a same-grant replacement with a new signing identity is refused by 009 (recorded in `docs/workflow-010-011-parallel.md` joint-batch record as a 009 delivery-branch defect; no fix commit on main — latest `gates.go` change `51fa19f`) | Same-grant leg fixed + proven end-to-end: 009 anchored reuse (`EvaluateGrantScopeBoundIdentity` + anchor binding pin; no scope deletion/identity rewrite/allow-all) + `TestJointReplaceFeeBumpSameGrant` (real worker Driver + 009 HTTP + Anvil; same grant, new attempt/identity, broadcast + sibling revise from chain fact; retry idempotent) + signer matrices/legalpath; pre-fix failures kept; `refused_basis` no-fee remains expected behavior | New in this batch (committed here) | **Satisfied** |
| V13-5a | Pause set/clear | 011: `TestPauseRefusesStepIssue` (`recovery_gate_integration_test.go:34`), `TestMultiplePauseRowsNotBypassed` (`:59`, clearing one keeps the other); 010 gate: `TestV6GateRefusals/pause_present` (`send_integration_test.go:63`) + accepted send after clear (`readonly_integration_test.go:111-121`); J5 freeze + controlled release, pause remains → still refused (`joint_driver_integration_test.go:726-753`) | On main; joint-round2 verbose | **Satisfied** |
| V13-5b | Reorg with receipt invalidation / confirmation rollback ⇒ revision tracking, projection freshness, no compensation intent | `TestJointDriverJ4` (`joint_driver_integration_test.go:456`) + direct `TestJointJ4` (`joint_j4_integration_test.go:82`); real Anvil revert + identical-bytes re-inclusion through the production process: `TestJointProcessReorgAfterCompleted` (`joint_process_tracking_integration_test.go:635`) — `orphaned`→`reconfirmed`, `completed→revised`, `revision_applied` exactly 1 (`:705`), projection = authority revision, external HTTP view `revised`/`confirmed` (`:731-741`), 1 binding/attempt/send (`:723`); restart variant `TestJointProcessRestartConsumesRevisionAfterCompleted` (`:749`); B1 keeps orphan history and confirmation basis (`receipt_canonicality_integration_test.go:174-233`) | On main; Lane-F3 process logs + joint-round2 | **Satisfied** |
| V13-6 | Non-substitution (no double/fixture/010-independent cited as joint) | Process tests drive the real binary and `jointwire.Worker`; the J tests use the production `txlifecycle.LifecycleLive` adapter + real 008 `nonce.Allocator` + real 009 signer-serve + real Anvil ERC-20 emitter. Contract-shape doubles appear only in 010-independent suites, labeled test-only | On main | **Satisfied** |
| V13-7 | Inherited limited exceptions (G-010-1 / G-010-2(c)) as recorded inputs | `TestT041ResidualEvidence` (`residual_integration_test.go:12`: last-moment expiry refusal; region-abort; freeze + controlled release; indeterminate ordering does not freeze); J5 detectable path. Not re-adjudicated | On main | **Satisfied (as inputs)** |
| — | Register's stated A-13 residual scope: "生产进程入口对适配器的装配（`WithdrawalWorkerCommand` 仍用 standalone 构造器）" | Closed by Lane-W: `cmd/txharbor` injects `jointwire.Worker` (`cmd/txharbor/main.go:36`), the command refuses unlinked wiring (`internal/app/withdrawalworker.go:657`), and `TestJointProcessEntryEndToEnd` exercises the assembly through the real binary/Run loop; `TestWorkerAssemblyWiresRealParticipants` + `TestWithdrawalWorkerCommandLiveEntry` pin the assembly | On main; main CI | **Satisfied** |
| — | 008 allocation + replay on the real assembly path | `TestWorkerAssemblyAllocationReplayOutcome` (`internal/jointwire/allocation_replay_integration_test.go:27`): real assembly closure → `allocated` then `replayed`, same intent/binding/nonce, 1/1 rows; F2/F3 process tests assert 1 binding/attempt/send durably | On main; Lane-F3 log | **Satisfied** |
| — | 009 signing + broadcast (real) | Process E2E: in-process real `app.SignerServe` + real Anvil send (`joint_process_entry_integration_test.go`); `TestT021V2RealSigner` (`signing_integration_test.go:141`) byte-identity; J tests | On main | **Satisfied** |
| — | Post-`completed` confirmation depth | `TestJointProcessTrackingAfterCompleted` (`:545-584`): completed on `sent` alone (receipts=0), then receipt canonical + depth 3 in the same process; threshold from the real `confirmation_policy_history` table (test-controlled value) | On main; Lane-F2 log | **Satisfied** |
| — | Restart recovery | `TestJointProcessRestartTrackingAfterCompleted` (`:592`) — completed intent is unclaimable; only the restarted process's tracking reaches confirmed; `TestJointProcessRestartConsumesRevisionAfterCompleted` (`:749`) | On main; Lane-F2/F3 logs | **Satisfied** |
| — | B1 post-hoc canonicality (lagging view) | `TestV8UnverifiedPromotesWithChainTruth` (`receipt_canonicality_integration_test.go:87`): unverified stays until chain truth catches up (`:114`), then canonical + depth; repeat scans idempotent (no revision bump, one `confirmed` event) | On main; Lane-F3 log | **Satisfied** |
| — | B2 revision consumption into 011 + external HTTP projection | `TestRevisionReconfirmAndProjectionOnlyVersions` (`revision_integration_test.go:287`), `TestRevisionConcurrentConsumption` (`:364`); process scenes above; `t28ExecutionView` (`joint_process_tracking_integration_test.go:518`) queries the existing `GET /withdrawals/{request_id}/execution` route and cross-checks the DB version | On main; Lane-F3 log | **Satisfied** |

### Historical out-of-lane failures (now closed on main)

The T042 note's `internal/db` hard-coded-sequence failures and the V12
secrets-token finding were both closed before this consolidation: the db
suite is green in main CI (`35401917939`, `db` ok 103.82s; the migration-set
adaptations landed via `3fe4b73`/`4a0f207`, merged with the 011 candidate at
`3ea6eb1`), and the `internal/config/config.go` dispatch-timeout comment was
reworded (`c21f3da`).
The 010 defects the joint round exposed (`TransferCalldata`
selector/recipient clobbering, `CanonicalEnvelope` fee-shape `omitempty`) are
fixed on main (`299d39e`, `82b5c9a`).

### Evidence limitations (kept, not papered over)

1. Chain truth (`chain_blocks`) is harness-controlled in the process tests
   (`t28SyncChainTruth`), exactly like the joint acceptance's `seedChainTruth`;
   the production indexer process is not inside the withdrawal-chain scene
   (its own E2E lives in 002/003/006 lanes). Constitution XI's deposit flow is
   deliberately out of scope here (no deposit-in-same-scene condition added).
2. The process tests write a test-controlled confirmation threshold into the
   real `confirmation_policy_history` source table; the depth is test-controlled
   by design.
3. J2–J5 / T060-replacement / V9b-crash verbose evidence is the joint-round2
   final logs at content-identical files (byte-identity checked for
   `joint_j2/j3/j5/replace/crash/migration_joint`; J1/J4/wiring/process-entry
   were additionally re-run verbose in Lane-F3). Main CI ran the same package
   but its log is non-verbose, so it is not cited as per-test proof.
4. The B1 promotion/progress rules were verified against the stub chain probe
   (`stubRPC`) for the canonicality matrix, plus the real-Anvil reorg scene for
   the revision path.

### Closure conclusion (for the orchestrator / user review)

**A-13 closure basis (this batch)**: the two recorded gaps are now closed with executed evidence (V13-2b interleave, V13-4b same-grant reuse); remaining items are the ordered supplements below. A-13 stays OPEN pending user review.

- **Minimum supplements (ordered)**:
  1. **V13-4b**: fix the 009 same-grant reuse path (do not re-apply the
     identity-equality `EvaluateGrantScope` check to a replacement that already
     passed `EvaluateGrantReuse`, or adjudicate the intended carrier shape),
     then run a same-grant fee replacement end-to-end through the production
     worker (same intent/binding/nonce, new signing identity, real 009 + Anvil)
     — or record an explicit adjudication that the fresh-grant path is the
     intended PB conditional reuse shape.
  2. **V13-2b**: one deterministic interleave test racing claim revoke/expiry
     against a 010 send gate in both lock orders (real PostgreSQL; a
     lock-observation barrier in the shape of the 008
     `authz_revoke_race_integration_test.go` precedent), asserting zero double
     dispatch and no second attempt.
  3. **V13-2a (optional, evidence limitation)**: a literal two-worker-process
     race on one intent; the claim layer already proves exclusivity, so this is
     strengthening, not a correctness hole.
- **Non-blocking legacy items** (do not gate A-13): harness-controlled chain
  truth / indexer-outside-scene (limitation 1), test-controlled confirmation
  threshold (limitation 2), the deliberate `Advance(ActionReplace)` ->
  `refused_basis` no-fee-policy gap (recorded, not faked), and the sticky
  freeze marker design (only the 010-side release lifts it).
- Everything else in V13-1/3/5/6/7 plus the register's production-process-entry
  scope, the 008 allocation/replay path, the 009 signing/broadcast leg, the
  post-`completed` confirmation depth, restart recovery, reorg re-inclusion,
  B1 canonicality and B2 revision/projection is satisfied with the evidence
  above on current main `df5a280`.
- **A-13 and T000-P stay OPEN**; this consolidation does not change either
  status and does not claim production readiness or main-verified equivalence
  beyond the cited runs.
