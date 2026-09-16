# PB Closeout Evidence Index

**Feature**: `012-007-authorization-carrier` | **Docs baseline**: HEAD `b680cb4`
(branch `012-007-authorization-carrier`).
**CODE_HEAD**: `<MISSING — fill at final regression>`.
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
| V-PB1 legal supply + 009-style read-back | `TestWithdrawalGrantScopedSupplyWritesScopeRow`; CLI `TestWithdrawalAuthzCmdMintSupplyRevoke` | `internal/withdrawal/grant_integration_test.go:949`; `internal/app/withdrawalauthz_integration_test.go:127` | `go test -tags integration -count=1 -run 'TestWithdrawalGrantScopedSupplyWritesScopeRow|TestWithdrawalAuthzCmdMintSupplyRevoke' ./internal/withdrawal/ ./internal/app/` | MISSING |
| V-PB2 unauthorized/unmapped supply refused | `TestWithdrawalGrantAuthorityUnmappedRefused`; `TestWithdrawalGrantAuthorityInTxCheck`; CLI `TestWithdrawalAuthzCmdPermissionGate`, `TestWithdrawalAuthzCmdNegativeMatrix` | `internal/withdrawal/grant_authority_integration_test.go:127,68`; `internal/app/withdrawalauthz_integration_test.go:254,347` | `go test -tags integration -count=1 -run 'TestWithdrawalGrantAuthority' ./internal/withdrawal/ && go test -tags integration -count=1 -run 'TestWithdrawalAuthzCmd(PermissionGate|NegativeMatrix)' ./internal/app/` | MISSING |
| V-PB3-PB scopeless stock queryable + scope absent | `TestGrantScopelessDetailUnchanged` (unit); stock path exercised in `TestWithdrawalAuthzOpen3DryRunReissue` step 1; content proof `TestWithdrawalReissueMintsNewIdentityWithSideEffectProof` | `internal/withdrawal/grant_test.go:334`; `internal/app/open3_dryrun_integration_test.go:210-231`; `internal/withdrawal/grant_integration_test.go:1477` | `go test -count=1 -run 'TestGrantScopelessDetailUnchanged' ./internal/withdrawal/ && go test -tags integration -count=1 -run 'TestWithdrawalAuthzOpen3DryRunReissue' ./internal/app/` | MISSING |
| V-PB4 fee boundaries | `TestWithdrawalGrantScopedSupplyFeeTripleBoundaries` | `internal/withdrawal/grant_integration_test.go:1024` | `go test -tags integration -count=1 -run '^TestWithdrawalGrantScopedSupplyFeeTripleBoundaries$' ./internal/withdrawal/` | MISSING |
| V-PB5 fee-replacement conditional | `TestWithdrawalGrantScopedSupplyFeeTripleBoundaries` (carrier only *expresses* `allows_fee_replacement`; the conditional-reuse decision is 009-owned per PB-FR-05) | `internal/withdrawal/grant_integration_test.go:1024` | same as V-PB4 | MISSING |
| V-PB6 revoke vs supply race | `TestWithdrawalGrantRevokeSupplyRace`; authority race `TestWithdrawalGrantAuthorityRevokeRace` | `internal/withdrawal/grant_integration_test.go:1201`; `internal/withdrawal/grant_authority_integration_test.go:159` | `go test -tags integration -count=1 -run 'TestWithdrawalGrant(RevokeSupplyRace|AuthorityRevokeRace)' ./internal/withdrawal/` | MISSING |
| V-PB7 atomicity (kill -9 phase points) | `TestCarrierKillScopedSupplyPreCommit`; `TestCarrierKillScopedSupplyPostCommit`; child helper `TestCarrierKillChildScopedSupply` | `internal/withdrawal/carrier_kill_integration_test.go:346,412,258` | `go test -tags integration -count=1 -run '^TestCarrierKillScopedSupply(PreCommit|PostCommit)$' ./internal/withdrawal/` | MISSING |
| V-PB8 commit-unknown same-op retry | `TestWithdrawalGrantUncertainCommitSameOperationRetry` | `internal/withdrawal/grant_integration_test.go:547` | `go test -tags integration -count=1 -run '^TestWithdrawalGrantUncertainCommitSameOperationRetry$' ./internal/withdrawal/` | MISSING |
| V-PB9 re-issuance dry-run (OPEN-3) | `TestWithdrawalAuthzOpen3DryRunReissue` (counts + tuple identity); content-snapshot proof owned by T030 `TestWithdrawalReissueMintsNewIdentityWithSideEffectProof` | `internal/app/open3_dryrun_integration_test.go:149`; `internal/withdrawal/grant_integration_test.go:1477` | `go test -tags integration -count=1 -run 'TestWithdrawalAuthzOpen3DryRunReissue|TestWithdrawalReissueMintsNewIdentityWithSideEffectProof' ./internal/app/ ./internal/withdrawal/` | MISSING |
| V-PB10 migration chain incl. gap-fill | `TestT041GapFillSequenceD`; `TestT040OverlayGreenOnEmptySequence`; `TestT042RollbackRevertsTenBeforeNine`; `TestT042DownOfAppliedThenRenumberedNumberForbidden`; lane guard `TestLaneMigrationsExclude009` | `internal/db/scratch_009_overlay_integration_test.go:155,210,292,340`; `internal/db/lane_migrations_test.go:19` | `go test -tags integration -count=1 -run 'TestT04[012]|TestLaneMigrationsExclude009' ./internal/db/` | MISSING |
| V-PB11 allowlist switchover rehearsal | `TestAllowlistSwitchoverRehearsal`; `TestAllowlistSwitchoverRehearsalUnaccountedExecutorKeepsEntryClosed` | `internal/app/allowlist_switchover_integration_test.go:331,446` | `go test -tags integration -count=1 -run '^TestAllowlistSwitchoverRehearsal' ./internal/app/` | MISSING |

## T040–T043 / T031 / T036 → tests + commands

| Task | Test(s) | File:line | Run command | Log |
|---|---|---|---|---|
| T031 | `TestWithdrawalAuthzOpen3DryRunReissue` | `internal/app/open3_dryrun_integration_test.go:149` | `go test -tags integration -count=1 -run '^TestWithdrawalAuthzOpen3DryRunReissue$' ./internal/app/` | MISSING |
| T036 | `TestT036SequenceAEmptyToFull`; `TestT036SequenceB007EraToCarrier`; `TestT036SequenceCDownThenReUp` | `internal/db/scratch_pb05_upgrade_integration_test.go:67,129,215` | `go test -tags integration -count=1 -run '^TestT036Sequence' ./internal/db/` | MISSING |
| T040 | `TestT040OverlayGreenOnEmptySequence` | `internal/db/scratch_009_overlay_integration_test.go:155` | `go test -tags integration -count=1 -run '^TestT040OverlayGreenOnEmptySequence$' ./internal/db/` | MISSING |
| T041 | `TestT041GapFillSequenceD` | `internal/db/scratch_009_overlay_integration_test.go:210` | `go test -tags integration -count=1 -run '^TestT041GapFillSequenceD$' ./internal/db/` | MISSING |
| T042 | `TestT042RollbackRevertsTenBeforeNine`; `TestT042DownOfAppliedThenRenumberedNumberForbidden` | `internal/db/scratch_009_overlay_integration_test.go:292,340` | `go test -tags integration -count=1 -run '^TestT042' ./internal/db/` | MISSING |
| T043 | `TestAllowlistSwitchoverRehearsal`; `TestAllowlistSwitchoverRehearsalUnaccountedExecutorKeepsEntryClosed` | `internal/app/allowlist_switchover_integration_test.go:331,446` | `go test -tags integration -count=1 -run '^TestAllowlistSwitchoverRehearsal' ./internal/app/` | MISSING |

Note: T021 (T-revoke-sync, `tasks.md:65`) and T022 (revoke-vs-supply race,
`tasks.md:66`) are still unchecked in `tasks.md`; the race test exists
(`TestWithdrawalGrantRevokeSupplyRace`) but the bare-revoke version bump does
**not** exist in `runRevokeTx` yet — see the D1 disposition below.

---

## Dispositions

### D1 — bare revoke and `authorization_version` (contract vs code)

**Contract** (data-model.md:34-35 T-revoke-sync; grant.go:148-157
`scopeBumpVersionSQL` comment): a revoke bumps the version, and a
revoke-then-re-supply cycle bumps it again.

**Code observed** (`internal/withdrawal/grant.go`, D1 landed):
- resupply path: non-active re-supply calls `bumpScopeVersionTx` (grant.go:774-778) ✓.
- `runRevokeTx`: the active-revoke branch now calls `bumpScopeVersionTx` in the
  same tx (grant.go:825-831); the already-revoked nop branch does not re-bump ✓.

**Disposition**: contract and code agree; the `handoff-009.md:94-98` assumption
("sibling D1 revoke-bump change has landed; `runRevokeTx` carries it") is
discharged. No PB doc change needed beyond the sentence already written. This
change is an uncommitted sibling artifact at docs-baseline `b680cb4`; final
regression re-confirms on the frozen SHA.

### T1 — `can_create` read but ignored (`grant.go:1031-1041`)

`callerAuthoritySelectSQL` reads `SELECT can_create FROM caller … FOR SHARE`
into `canCreate`, which is **never used**: the issuance predicate is
`auth.Issuers.PermitIssue(callerID)` only (grant.go:1039). The approved
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
through `logx.Redact` (`withdrawalauthz.go:105,273,346,356,361,393,396`), and
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

**Code truth**: on an authority-check failure the supply tx writes exactly one
`supply_refused` audit row and commits, with **zero grant and zero scope rows**
(`grant.go:709-724`, `insertAuditTx` at 668-678). Exact consistent phrasing:

> **zero grant/scope rows, one `supply_refused` audit row**

Two docs use looser wording that could be misread as "no row at all":
`contracts/supply-scope.md:26` ("zero writes") and `data-model.md:33`
("writes nothing"). The precise code-comment phrasing already used at
`grant.go:1107` is "records `supply_refused` naming the check and writes zero
grant/scope rows". **Reported, not edited** (outside this task's file list);
if normalized later, adopt the phrasing above.

**Disposition**: confirmed consistent — refusal is zero *business* rows plus
one audit row.

### T031 — scoped to the actual proof, cites T030

`TestWithdrawalAuthzOpen3DryRunReissue` proves: trace complete (audit links both
old ids), old grant/request tuple identity unchanged (no UPDATE), still
scopeless, and **zero new rows by count** across the seven 008 nonce tables +
distinct intent set (open3_dryrun_integration_test.go:257-311). It does **not**
do the full-content seven-table snapshot — that is T030's
`TestWithdrawalReissueMintsNewIdentityWithSideEffectProof`
(`grant_integration_test.go:1477`). `quickstart.md` V-PB9 row now reads "zero
new rows (counts); zero mutation proven by T030 content snapshot".

**Disposition**: T031 scoped to count + trace + identity proof; full-content
mutation proof cited to T030. No overclaim.

---

## Remaining gaps to close at final regression

1. `CODE_HEAD` — fill in the exact commit SHA of the frozen tree.
2. Every `LOG` cell — attach the real run log path; do not fabricate.
3. V-PB7 — kill-9 tests exist (`carrier_kill_integration_test.go`); attach
   their real logs at final regression.
4. D1 — bare-revoke bump observed landed in `runRevokeTx`; re-confirm on the
   frozen SHA.
