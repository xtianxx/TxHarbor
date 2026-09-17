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
