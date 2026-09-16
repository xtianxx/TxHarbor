# PB Closeout Evidence Index

**Feature**: `012-007-authorization-carrier` | **Docs baseline**: HEAD `b680cb4`
(branch `012-007-authorization-carrier`).
**CODE_HEAD**: `bcc01bece1a69924925084aebd0e5b505f67bf67` (review-closeout batch).

**GATE RESULTS** (closeout regression, real runs — per-case tee logs not
persisted, commands + outcomes recorded instead):
- `go build ./...`, `go vet ./...`, `gofmt -l` clean.
- `go test ./... -count=1` → 964 passed, 12 packages.
- `go test -tags integration -count=1 ./internal/withdrawal/ ./internal/app/ ./internal/db/` → 697 passed, 3 packages (rerun after one flake, see below); `-race` same set → 697 passed.
- `go test -tags integration -count=1 -timeout 30m ./...` → 1689 passed, 1 skipped; 2 flakes, both isolated-green on rerun, both in files PB never touched: `TestWithdrawalRevocationLockWaitExpiry` (007 lock-wait timing) and `TestDepositObservabilityEndToEnd` (006 state-poll timing).
- New/changed suites green: D1 version-sync, T2 resolve-retryable, race, reissue, carrier-kill pre/post-commit, exit-2, open3 content snapshot, T036a/b/c, T040-T042, T043 both paths.
- `TestNonceMigrationUpStatusDownUp` fixed (FS pinned through 000008; PB 000010 no longer leaks into the 008-era baseline) and green.
**LOG POINTERS**: every per-case log path below is `MISSING` — no old run logs
were persisted in this lane; do NOT treat any absence as a pass. Final T052
regression fills each `LOG` line from a real run.

## How to run (integration)

All cases are `//go:build integration` (real PostgreSQL scratch DBs; some spawn
Anvil). Makefile target: `make test-integration` =
`go test -tags integration -count=1 -timeout 20m ./...`.

Single case:
`go test -tags integration -count=1 -timeout 20m -run '^TestName$' ./internal/<pkg>/`

## V-PB1–V-PB11 → tests + commands

| Case | Test(s) | File:line | Run command | Log |
|---|---|---|---|---|
| V-PB1 legal supply + 009-style read-back | `TestWithdrawalGrantScopedSupplyWritesScopeRow`; CLI `TestWithdrawalAuthzCmdMintSupplyRevoke` | `internal/withdrawal/grant_integration_test.go:1045`; `internal/app/withdrawalauthz_integration_test.go:127` | `go test -tags integration -count=1 -run 'TestWithdrawalGrantScopedSupplyWritesScopeRow|TestWithdrawalAuthzCmdMintSupplyRevoke' ./internal/withdrawal/ ./internal/app/` | MISSING |
| V-PB2 unauthorized/unmapped supply refused | `TestWithdrawalGrantAuthorityUnmappedRefused`; `TestWithdrawalGrantAuthorityInTxCheck`; CLI `TestWithdrawalAuthzCmdPermissionGate`, `TestWithdrawalAuthzCmdNegativeMatrix` | `internal/withdrawal/grant_authority_integration_test.go:127,68`; `internal/app/withdrawalauthz_integration_test.go:254,347` | `go test -tags integration -count=1 -run 'TestWithdrawalGrantAuthority' ./internal/withdrawal/ && go test -tags integration -count=1 -run 'TestWithdrawalAuthzCmd(PermissionGate|NegativeMatrix)' ./internal/app/` | MISSING |
| V-PB3-PB scopeless stock queryable + scope absent | `TestGrantScopelessDetailUnchanged` (unit); stock path exercised in `TestWithdrawalAuthzOpen3DryRunReissue` step 1; content proof `TestWithdrawalReissueMintsNewIdentityWithSideEffectProof` | `internal/withdrawal/grant_test.go:334`; `internal/app/open3_dryrun_integration_test.go:275-296`; `internal/withdrawal/grant_integration_test.go:1573` | `go test -count=1 -run 'TestGrantScopelessDetailUnchanged' ./internal/withdrawal/ && go test -tags integration -count=1 -run 'TestWithdrawalAuthzOpen3DryRunReissue' ./internal/app/` | MISSING |
| V-PB4 fee boundaries | `TestWithdrawalGrantScopedSupplyFeeTripleBoundaries` | `internal/withdrawal/grant_integration_test.go:1120` | `go test -tags integration -count=1 -run '^TestWithdrawalGrantScopedSupplyFeeTripleBoundaries$' ./internal/withdrawal/` | MISSING |
| V-PB5 fee-replacement conditional | `TestWithdrawalGrantScopedSupplyFeeTripleBoundaries` (carrier only *expresses* `allows_fee_replacement`; the conditional-reuse decision is 009-owned per PB-FR-05) | `internal/withdrawal/grant_integration_test.go:1120` | same as V-PB4 | MISSING |
| V-PB6 revoke vs supply race | `TestWithdrawalGrantRevokeSupplyRace`; authority race `TestWithdrawalGrantAuthorityRevokeRace` | `internal/withdrawal/grant_integration_test.go:1297`; `internal/withdrawal/grant_authority_integration_test.go:159` | `go test -tags integration -count=1 -run 'TestWithdrawalGrant(RevokeSupplyRace|AuthorityRevokeRace)' ./internal/withdrawal/` | MISSING |
| V-PB7 atomicity (kill -9 phase points) | `TestCarrierKillScopedSupplyPreCommit`; `TestCarrierKillScopedSupplyPostCommit`; child helper `TestCarrierKillChildScopedSupply` | `internal/withdrawal/carrier_kill_integration_test.go:362,429,274` | `go test -tags integration -count=1 -run '^TestCarrierKillScopedSupply(PreCommit|PostCommit)$' ./internal/withdrawal/` | MISSING |
| V-PB8 commit-unknown same-op retry | `TestWithdrawalGrantUncertainCommitSameOperationRetry` | `internal/withdrawal/grant_integration_test.go:643` | `go test -tags integration -count=1 -run '^TestWithdrawalGrantUncertainCommitSameOperationRetry$' ./internal/withdrawal/` | MISSING |
| V-PB9 re-issuance dry-run (OPEN-3) | `TestWithdrawalAuthzOpen3DryRunReissue` (owns the 7-table `row_to_json` content snapshot over a seeded non-empty fixture); library-level snapshot + `ctid`/`xmin` identity owned by T030 `TestWithdrawalReissueMintsNewIdentityWithSideEffectProof` | `internal/app/open3_dryrun_integration_test.go:214`; `internal/withdrawal/grant_integration_test.go:1573` | `go test -tags integration -count=1 -run 'TestWithdrawalAuthzOpen3DryRunReissue|TestWithdrawalReissueMintsNewIdentityWithSideEffectProof' ./internal/app/ ./internal/withdrawal/` | MISSING |
| V-PB10 migration chain incl. gap-fill | `TestT041GapFillSequenceD`; `TestT040OverlayGreenOnEmptySequence`; `TestT042RollbackRevertsTenBeforeNine`; `TestT042DownOfAppliedThenRenumberedNumberForbidden`; lane guard `TestLaneMigrationsExclude009` | `internal/db/scratch_009_overlay_integration_test.go:155,210,292,340`; `internal/db/lane_migrations_test.go:19` | `go test -tags integration -count=1 -run 'TestT04[012]|TestLaneMigrationsExclude009' ./internal/db/` | MISSING |
| V-PB11 allowlist switchover rehearsal | `TestAllowlistSwitchoverRehearsal`; `TestAllowlistSwitchoverRehearsalUnaccountedExecutorKeepsEntryClosed` | `internal/app/allowlist_switchover_integration_test.go:331,446` | `go test -tags integration -count=1 -run '^TestAllowlistSwitchoverRehearsal' ./internal/app/` | MISSING |

## T040–T043 / T031 / T036 → tests + commands

| Task | Test(s) | File:line | Run command | Log |
|---|---|---|---|---|
| T031 | `TestWithdrawalAuthzOpen3DryRunReissue` | `internal/app/open3_dryrun_integration_test.go:214` | `go test -tags integration -count=1 -run '^TestWithdrawalAuthzOpen3DryRunReissue$' ./internal/app/` | MISSING |
| T036 | `TestT036SequenceAEmptyToFull`; `TestT036SequenceB007EraToCarrier`; `TestT036SequenceCDownThenReUp` | `internal/db/scratch_pb05_upgrade_integration_test.go:67,129,215` | `go test -tags integration -count=1 -run '^TestT036Sequence' ./internal/db/` | MISSING |
| T040 | `TestT040OverlayGreenOnEmptySequence` | `internal/db/scratch_009_overlay_integration_test.go:155` | `go test -tags integration -count=1 -run '^TestT040OverlayGreenOnEmptySequence$' ./internal/db/` | MISSING |
| T041 | `TestT041GapFillSequenceD` | `internal/db/scratch_009_overlay_integration_test.go:210` | `go test -tags integration -count=1 -run '^TestT041GapFillSequenceD$' ./internal/db/` | MISSING |
| T042 | `TestT042RollbackRevertsTenBeforeNine`; `TestT042DownOfAppliedThenRenumberedNumberForbidden` | `internal/db/scratch_009_overlay_integration_test.go:292,340` | `go test -tags integration -count=1 -run '^TestT042' ./internal/db/` | MISSING |
| T043 | `TestAllowlistSwitchoverRehearsal`; `TestAllowlistSwitchoverRehearsalUnaccountedExecutorKeepsEntryClosed` | `internal/app/allowlist_switchover_integration_test.go:331,446` | `go test -tags integration -count=1 -run '^TestAllowlistSwitchoverRehearsal' ./internal/app/` | MISSING |

Note: T021 (T-revoke-sync, `tasks.md:65`) and T022 (revoke-vs-supply race,
`tasks.md:66`) are both checked in `tasks.md` and covered by
`TestWithdrawalGrantScopedRevokeSyncsVersion`
(`internal/withdrawal/grant_integration_test.go:373`) and
`TestWithdrawalGrantRevokeSupplyRace`
(`internal/withdrawal/grant_integration_test.go:1297`). `runRevokeTx` bumps the
scope version on the active→revoked branch (`grant.go:817-834`); the D1
disposition below records the code/contract match.

---

## Dispositions

### D1 — bare revoke and `authorization_version` (contract vs code)

**Contract** (data-model.md:36-37 T-revoke-sync; grant.go:148-157
`scopeBumpVersionSQL` comment): a revoke bumps the version, and a
revoke-then-re-supply cycle bumps it again.

**Code observed** (`internal/withdrawal/grant.go`, D1 landed):
- resupply path: non-active re-supply calls `bumpScopeVersionTx` (grant.go:774-778) ✓.
- `runRevokeTx`: the active-revoke branch now calls `bumpScopeVersionTx` in the
  same tx (grant.go:825-831); the already-revoked nop branch does not re-bump ✓.

**Disposition**: contract and code agree; the `handoff-009.md:94-98` assumption
("sibling D1 revoke-bump change has landed; `runRevokeTx` carries it") is
discharged. No PB doc change needed beyond the sentence already written. The
change is committed in the frozen code tree (`bcc01be`); final regression
re-confirms on the frozen SHA.

### T1 — `can_create` read but ignored (`grant.go:1044-1064`)

`callerAuthoritySelectSQL` reads `SELECT can_create FROM caller … FOR SHARE`
into `canCreate`, which is **never used**: the issuance predicate is
`auth.Issuers.PermitIssue(callerID)` only (grant.go:1061; the read at
1053-1054, const at 171-172). The approved
predicate in `contracts/supply-scope.md` §Preconditions (lines 20-23) is
`PermitIssue(caller_id)` against `TXHARBOR_AUTHZ_ISSUER_CALLERS`; it **does not
include `can_create`**. Ignoring `can_create` therefore matches the contract;
the read is defense-in-depth row presence (api_key→caller FK resolution).

**Disposition**: accepted as-is. Contract citation: `contracts/supply-scope.md:20-23`.

### T3 — scopeless-supply and `RevokeGrant` remain DSN-trust

The extended authority gate applies only to a **scoped** supply: CLI requires
`--api-key` when any scope flag is present (`withdrawalauthz.go:179-185`), and
only then runs `Authenticate` + `PermitIssue` + `SupplyGrantAuthorized`
(`withdrawalauthz.go:224-257`). A scopeless supply and every `revoke` stay the
legacy DSN-trust path — operator holds the deployment DSN, same trust as
`migrate`/`confirm-auth`; `--operator` is an audit claim only
(`withdrawalauthz.go:26-39`; `withdrawalauthz.go:302-328` `RevokeGrant`).

**Authorizing sources**: spec.md PB-FR-02 (scoped carrier same-tx write; the
controlled `withdrawal-authz supply` entry) and PB-FR-06 (007 receive-only
boundary unchanged; existing grants stay valid, scope row optional —
spec.md:98-112); `contracts/supply-scope.md:3-4` scopes the contract to the
extended entry only. PB-C1 (spec.md:151) governs the **new** issuer gate; it
does not re-open the pre-existing DSN-trust boundary.

**Disposition**: accepted. The DSN boundary is an explicit, documented trust
model, not an unguarded hole.

### V1 — credential argv / `Redact`

Presented API keys never land in logs/storage: CLI redacts every stderr path
through `logx.Redact` (`withdrawalauthz.go:105,273,346,355,361,393,396`), and
`attested_by` records `key:<id>/caller:<id>`, never the raw key
(`withdrawalauthz.go:294-300`).

**Existing secrecy tests (cite, do not re-add)**:
`TestWithdrawalAuthzSupplyKeySecrecyWithoutDB`
(`internal/app/withdrawalauthz_secrecy_test.go:26`),
`TestWithdrawalAuthzSupplyCredentialSecrecy`
(`internal/app/withdrawalauthz_secrecy_integration_test.go:29`), plus the
generic `logx.TestRedact` (`internal/logx/redact_test.go:9`) and
`TestSummaryRedactsCredentials` (`internal/config/config_test.go:279`).

**Disposition**: satisfied; no new test required.

### Security confirmation — `supply_refused` audit row vs "zero business writes"

**Code truth**: an **in-tx** refusal writes one `supply_refused` audit row and
commits, with **zero grant and zero scope rows** (`runSupplyTx` branch
`grant.go:714-723`, `insertAuditTx` at 668-678; the reissue path mirrors it at
`grant.go:868-877`). Pre-tx failures never reach a write: a pre-tx
`Authenticate`/`PermitIssue` denial returns through `withdrawalAuthzRefuse`
(CLI stderr only, `withdrawalauthz.go:226-233,387-397`), and usage/config/
unreachable-DB failures exit before any write. Exact consistent phrasing:

> in-tx: **zero grant/scope rows, one `supply_refused` audit row**; pre-tx: **zero rows, no audit row**

`contracts/supply-scope.md:26` and `data-model.md:33` previously said "zero
writes"/"writes nothing" (misreadable as "no row at all"); both are now
normalized to the split above, matching the code-comment phrasing at
`grant.go:1121`.

**Disposition**: confirmed consistent — an in-tx refusal is zero *business*
rows plus one audit row; a pre-tx refusal writes nothing.

### T031 — scoped to the actual proof, cites T030

`TestWithdrawalAuthzOpen3DryRunReissue` (`open3_dryrun_integration_test.go:214`)
executes the OPEN-3 procedure through the CLI carrier and owns its own
**7-table `row_to_json` full-content snapshot** over a seeded non-empty fixture
(`open3Snapshot`/`open3SeedNonceFixture`, lines 54-132, seeded at 300): every
008 `nonce_*` table is compared row-by-row before/after, so an in-place UPDATE
or an equal-size replacement fails exactly like an insert. It also asserts the
content-free old-row identity (`ctid`/`xmin`, lines 313-314/362-369), the trace
(audit links both old ids), the still-scopeless old grant, and an unchanged
distinct intent set. T030's `TestWithdrawalReissueMintsNewIdentityWithSideEffectProof`
(`grant_integration_test.go:1573`) owns the **library-level** snapshot across all
007/PB tables plus the seven nonce tables, with the `ctid`/`xmin` identity of the
old grant *and* old scope, and the per-operation census allowlist (+1 grant/
+1 scope/+1 audit). `quickstart.md` V-PB9 states the same split.

**Disposition**: T031 owns the CLI-level 7-table content snapshot; T030 owns the
library-level census + grant/scope tuple identity. No "counts only" claim.

---

## Remaining gaps to close at final regression

1. `CODE_HEAD` — filled (`bcc01be`); re-confirm the frozen tree still matches.
2. Every `LOG` cell — attach the real run log path; do not fabricate.
3. V-PB7 — kill-9 tests exist (`carrier_kill_integration_test.go`); attach
   their real logs at final regression.
4. D1 — bare-revoke bump observed landed in `runRevokeTx`; re-confirm on the
   frozen SHA.
