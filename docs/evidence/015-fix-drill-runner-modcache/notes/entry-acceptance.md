# Entry acceptance — drill coverage entry chain (local analysis, 2026-10-06)

Repository: `TxHarbor` @ `013-redis-latency-budget-design` (read-only analysis; no repo files modified).
Scope: Makefile `test-drill` + `require_tagged_tests` (Makefile:3-13, 74-90), `scripts/drillcoverage/check.go` (+ `check_test.go`),
`.github/workflows/drill.yml` step/exit propagation, `scripts/drillcoverage/run-in-container.sh`.
The full recovery drill was **not** run. This notes file is the only file written, and it lives under `.evidence/`,
which is excluded from git via `.git/info/exclude:10` (`.evidence/`) — the worktree stays clean.

---

## 1. Entry chain and non-zero-exit propagation

### 1.1 Chain (workflow step → checker)

| # | Stage | Location | What happens |
|---|-------|----------|--------------|
| 0 | Trigger | `.github/workflows/drill.yml:44-48` | `on:` = `schedule` (`cron: "41 3 * * 1"`, line 47) + `workflow_dispatch` (line 48). No `pull_request` trigger. |
| 1 | Workflow step | `.github/workflows/drill.yml:81-86` | `full recovery drill layer (pinned native-tool runner)` runs `scripts/drillcoverage/run-in-container.sh` (line 85), step timeout 200m (86); evidence dir + commit passed as env (82-84). |
| 2 | Wrapper pre-flight | `run-in-container.sh:29-41` | Prerequisites check (29-34); tree fingerprint computed on host (37) and regex-validated (`^sha256:[0-9a-f]{64}$`), otherwise `exit 1` (38-41). |
| 3 | `docker run` | `run-in-container.sh:54-64, 89-133` | Repo/GOROOT/module-cache mounted **read-only**, scratch+evidence read-write, Docker socket mounted (54-59); container env `GOMODCACHE=/host-gomodcache` (64), `CGO_ENABLED=0` (67), `GOCACHE`/`TMPDIR`/`HOME` on scratch (68-70), `TXHARBOR_TESTED_TREE_FINGERPRINT` passed in (77). `set +e` before the run (89); container entry `/bin/sh -ec` (90) does pin/tool verification and then `exec make test-drill` (131). |
| 4 | Makefile guard | `Makefile:7-13`, called at `Makefile:75` | `require_tagged_tests,drill,test-drill`: if `grep -rl --include='*_test.go' ... '^//go:build drill' .` finds no file → `echo "$(2): NOT RUN — no $(1)-tagged tests found"; exit 1` (Makefile:10-11). |
| 5 | Makefile recipe | `Makefile:76-90` | `run_dir` = `mktemp -d "$TXHARBOR_DRILL_EVIDENCE_DIR/run.XXXXXX"` (79; temp-dir fallback 82; cleanup trap keeps it only when `keep=1`, 84). `go test -json -tags drill -count=1 -timeout 120m ./... > "$events"; go_status=$?` (87). If `go_status != 0`, events are `cat`-ed to the log (88). Checker: `go run scripts/drillcoverage/check.go "$events" "$report" "$go_status" ./... "$run_id" "$started"; check_status=$?` (89). Final selector: `if [ $$go_status -ne 0 ]; then exit $$go_status; fi; exit $$check_status` (90). |
| 6 | Checker | `check.go:160-208` (7-arg mode) | Parses JSONL; enforces per-case `pass` + pinned package; diagnostics → `exit 1` (199-203); `!passed` → `exit 1` (205-206); success print + implicit exit 0 (208). Usage/IO errors → `exit 2` (165, 172, 177, 189, 195). |
| 7 | Wrapper post-check | `run-in-container.sh:133-144` | `docker_status=$?` (133) captures docker/container exit; fingerprint re-computed (135); `tree-validation.json` written (138-139); mismatch → "evidence is not claimable" `exit 1` (140-143); else `exit "$docker_status"` (144). |
| 8 | Workflow result | `.github/workflows/drill.yml:85-92` | Step result = wrapper exit code; evidence upload uses `if: always()` (88) so failed runs still archive. |

### 1.2 Exit propagation per case

`check_status` is the checker process exit; `go_status` is `go test`'s exit. The Makefile recipe's final exit is
`go_status` when non-zero, otherwise `check_status` (Makefile:90). GNU make then reports the recipe line and exits 2
(observed: `make: *** [Makefile:76: test-drill] Error 1`, log:1680), `docker run` propagates that code to the wrapper
(run-in-container.sh:133), and the wrapper forwards it (144).

| Case | `go test` result | Checker behavior (check.go) | Recipe exit (Makefile:90) | Downstream (make → docker → wrapper → step) |
|------|------------------|-----------------------------|----------------------------|---------------------------------------------|
| (a) go build failure inside `go test` | exits non-zero, `go_status=1`; JSON contains only package-level `fail`/build-output events, no required `Test` events | all 22 required names hit `statusError(...) = "NOT RUN: … (missing from go test -json output)"` (145-146) and additionally the package pin `pkg != "…/internal/recovery"` appends `FAIL: … reported from unexpected package ""` (105-111, 127-133); diagnostics → `exit 1` (203) | `exit $$go_status` = 1 | make `Error 1` → make exits **2** → wrapper exits **2** → step red with exit code 2 |
| (b) required test missing from JSON but `go_status=0` | exits 0 | same NOT RUN + wrong-package diagnostics; diagnostics → `exit 1` (203) | `exit $$check_status` = 1 | make exits 2 → wrapper 2 → step red |
| (c) required test skipped | exits 0 (test "ran" and skipped) | `statusError` skip branch: `"NOT RUN: %s (SKIP)"` (139-140); nested children under a required parent also fatal: `"NOT RUN: %s (SKIP beneath required %s)"` (121-122); `exit 1` (203) | `exit $$check_status` = 1 | make 2 → wrapper 2 → step red |
| (d) required test failing | exits non-zero, `go_status=1` | `statusError` fail branch: `"FAIL: %s"` (142-143); `exit 1` (203) | `exit $$go_status` = 1 | make 2 → wrapper 2 → step red |
| (e) all required passing (fingerprint set) | exits 0 | no diagnostics, `passed=true` → prints `All required drill scenarios and restore-dependent subtests passed.` (208) → exit 0 | `exit $$check_status` = 0 | make 0 → wrapper `exit "$docker_status"` = 0 → step green |

Extra paths:
- No `drill`-tagged tests in tree: guard fails first (`Makefile:11`) → recipe `exit 1` → make Error 1 → make exit 2 → step red; `go test` never runs.
- `TXHARBOR_TESTED_TREE_FINGERPRINT` missing/empty: checker appends `"NOT RUN: runtime/test source fingerprint unavailable"` and sets `passed=false` (check.go:182-184) → exit 1. The wrapper always sets it (run-in-container.sh:77).
- Checker misuse (unreadable JSON, bad args): `exit 2` (check.go:165/172/177/189) → recipe `exit $$check_status` = 2 → make Error 2 → make exit 2.

---

## 2. MUST-NOT-weaken behaviors (with enforcing code)

1. **Required scenario lists (`requiredTests` / `requiredSubtests`)** — 19 + 3 names, pinned verbatim:
   - `check.go:15-35` `var requiredTests = []string{ ... }` (19 names, first `TestDrillArmRefusesReplacedELFAndPreStartTamper`, last `TestT060DrillInstanceCloseOnlyWhenAllReleasedAndIdempotent`).
   - `check.go:37-40` `var requiredSubtests = []string{ ... }` (3 names: `TestT059F1…/corrupt-verification`, `…/edited-verified-manifest`, `TestT059F3…/incompatible-restore`).
   - Enforcement per name (`check.go:105-111`; same loop for subtests `127-133`):
     ```go
     for _, name := range requiredTests {
         if err := statusError(name, statuses[name]); err != "" {
             errors = append(errors, err)
         }
         if pkg := packages[name]; pkg != "github.com/xtianxx/txharbor/internal/recovery" {
             errors = append(errors, fmt.Sprintf("FAIL: %s reported from unexpected package %q", name, pkg))
         }
     }
     ```
   - `check.go:135` `return statuses, packages, len(errors) == 0, errors` — any diagnostic fails the run.
2. **NOT RUN discipline** (an unrun check must never read as a pass):
   - `Makefile:3-6` comment: "when no test file carries the build tag yet, the layer reports NOT RUN and exits non-zero instead of passing vacuously"; guard body `Makefile:8-11`: `if [ -z "$$files" ]; then echo "$(2): NOT RUN — no $(1)-tagged tests found"; exit 1; fi`.
   - `check.go:138-148` `statusError`: `skip → "NOT RUN: %s (SKIP)"` (139-140), `fail → "FAIL: %s"` (142-143), `no pass → "NOT RUN: %s (missing from go test -json output)"` (145-146).
   - `check.go:113` comment `// A parent can report PASS even if a nested case was skipped.` with the nested-skip scan at `check.go:114-125` (`"NOT RUN: %s (SKIP beneath required %s)"`, 121-122).
   - `check.go:199-203`: diagnostics printed and `os.Exit(1)`; `205-206`: `if !passed { os.Exit(1) }`.
   - `drill.yml:12-19` (workflow comment): missing tagged tests → NOT RUN non-zero; "an interrupted/partial run (job canceled, timeout) leaves the job red and produces no claimable evidence."
3. **Tree fingerprint stability**:
   - Before: `run-in-container.sh:37-41` computes and format-validates `TREE_FINGERPRINT_BEFORE`; injected into the container (`:77 -e "TXHARBOR_TESTED_TREE_FINGERPRINT=$TREE_FINGERPRINT_BEFORE"`).
   - After: `run-in-container.sh:135-143` recomputes, writes `tree-validation.json`, and on mismatch prints `runtime/test source changed during drill; evidence is not claimable` and `exit 1`.
   - `check.go:182-184` empty fingerprint ⇒ `"NOT RUN: runtime/test source fingerprint unavailable"` + `passed=false`; `check.go:235-240` sets `CheckerResult="FAIL"`, `CheckerExit=1`, and `OverallResult="FAIL"` when the fingerprint is absent.
   - Definition: `check.go:291-346` `runtimeFingerprint` (sha256 over sorted runtime inputs: contents + mode; excludes `.git`, `docs`, `specs`, `.evidence`, `artifacts`, `dist`, `build`); `check.go:349-367` `isRuntimeInput` covers `.go`, `.sql`, `go.mod`, `go.sum`, `Makefile`, `testdata/`, `scripts/*.sh`, `docker-compose*.yml`, `.github/workflows/drill.yml`.
4. **Evidence-per-run uniqueness**:
   - `Makefile:79` `run_dir="$$(mktemp -d "$$TXHARBOR_DRILL_EVIDENCE_DIR/run.XXXXXX")"` and `Makefile:73` comment "Each invocation gets a unique subdirectory; prior runs are untouched."; temp fallback `82`; `trap '[ "$$keep" -eq 1 ] || rm -rf -- "$$run_dir"' EXIT` (84) keeps evidence only when the evidence dir is configured.
   - `run-in-container.sh:27` `WRAPPER_RUN="$(mktemp -d "$EVIDENCE_HOST/wrapper.XXXXXX")"` holds `metadata.json` + `tree-validation.json` (observed artifacts `wrapper.IzHGk2/`, `run.pBV0i8/`).
   - `check.go:288` `return os.WriteFile(path, append(data, '\n'), 0600)` — report written 0600.
5. **Failure propagation** (no silent green):
   - `Makefile:87-90`:
     ```
     go test -json -tags drill -count=1 -timeout 120m ./... > "$$events"; go_status=$$?;
     if [ $$go_status -ne 0 ]; then cat "$$events"; fi;
     go run scripts/drillcoverage/check.go "$$events" "$$report" "$$go_status" ./... "$$run_id" "$$started"; check_status=$$?;
     if [ $$go_status -ne 0 ]; then exit $$go_status; fi; exit $$check_status
     ```
   - Wrapper: `run-in-container.sh:89` `set +e`, `:133` `docker_status=$?`, `:144` `exit "$docker_status"`.
   - Workflow keeps evidence even on failure: `drill.yml:87-92` (`if: always()`, upload `drill-evidence-${{ github.run_id }}`).

---

## 3. Host-only local validations

### 3.0 Preparation (reproducible)

Synthetic event files were generated by `/tmp/drill-entry-acceptance/gen.py`, which parses the
`requiredTests`/`requiredSubtests` lists **directly out of `check.go`** (no manual transcription) and writes
`i-empty.jsonl`, `ii-unrelated.jsonl`, `iii-skip.jsonl`, `iv-allpass.jsonl` plus `lists.json`.

```
$ python3 /tmp/drill-entry-acceptance/gen.py /tmp/drill-entry-acceptance
wrote cases to /tmp/drill-entry-acceptance: 19 requiredTests, 3 requiredSubtests
```

Equality re-verification: `PASS (lists.json == check.go lists exactly: 19 requiredTests + 3 requiredSubtests = 22)`

Case runner `/tmp/drill-entry-acceptance/run-cases.sh` sets
`TXHARBOR_TESTED_TREE_FINGERPRINT=sha256:<64 zeros>` (the checker only requires non-empty; real runs get the live
fingerprint via run-in-container.sh:77) and invokes, for each case:

```
go run scripts/drillcoverage/check.go <events.jsonl> <case-report.json> <go-exit> ./... run.<tag> 2026-10-06T00:00:00Z
```

Result matrix: **no discrepancy with expectation.**

### 3.a `go test ./scripts/drillcoverage` (expect pass)

```
$ time go test ./scripts/drillcoverage
ok  	github.com/xtianxx/txharbor/scripts/drillcoverage	(cached)

real	0m0.527s

$ time go test -count=1 ./scripts/drillcoverage
ok  	github.com/xtianxx/txharbor/scripts/drillcoverage	0.038s

real	0m0.720s
```
First (uncached) invocation in this session: `ok … 0.015s` in ~1.2s wall. So: **pass**; actual test runtime ~15-40ms,
wall time dominated by `go` tool startup (<1.3s).

### 3.b Synthetic checker cases

#### (i) Empty events file + `go_status=1` → expect FAIL naming required tests NOT RUN, non-zero exit

Command (as run by run-cases.sh): `go run scripts/drillcoverage/check.go /tmp/drill-entry-acceptance/i-empty.jsonl /tmp/drill-entry-acceptance/i-report.json 1 ./... run.i 2026-10-06T00:00:00Z`

Observed exit: **1** (stdout empty). Raw stderr (46 lines: header + 44 diagnostics + `go run` trailer):

```
Drill scenario coverage incomplete:
- NOT RUN: TestDrillArmRefusesReplacedELFAndPreStartTamper (missing from go test -json output)
- FAIL: TestDrillArmRefusesReplacedELFAndPreStartTamper reported from unexpected package ""
- NOT RUN: TestDrillTargetWitnessOldReconnectRejected (missing from go test -json output)
- FAIL: TestDrillTargetWitnessOldReconnectRejected reported from unexpected package ""
- NOT RUN: TestDrillTargetWitnessOldRoleLoginDuringRestoreAcceptanceFailsClosed (missing from go test -json output)
- FAIL: TestDrillTargetWitnessOldRoleLoginDuringRestoreAcceptanceFailsClosed reported from unexpected package ""
- NOT RUN: TestDrillTargetWitnessObserverTerminationDuringRestoreAcceptanceFailsClosed (missing from go test -json output)
- FAIL: TestDrillTargetWitnessObserverTerminationDuringRestoreAcceptanceFailsClosed reported from unexpected package ""
- NOT RUN: TestT058DrillE2EBackupAdvanceRestoreRefuseThenRelease (missing from go test -json output)
- FAIL: TestT058DrillE2EBackupAdvanceRestoreRefuseThenRelease reported from unexpected package ""
- NOT RUN: TestT058DrillGapCannotBeClosedStaysPaused (missing from go test -json output)
- FAIL: TestT058DrillGapCannotBeClosedStaysPaused reported from unexpected package ""
- NOT RUN: TestT058DrillEventLayerKafkaBrokerOffsetDivergence (missing from go test -json output)
- FAIL: TestT058DrillEventLayerKafkaBrokerOffsetDivergence reported from unexpected package ""
- NOT RUN: TestT058DrillEventEffectRecovery (missing from go test -json output)
- FAIL: TestT058DrillEventEffectRecovery reported from unexpected package ""
- NOT RUN: TestT058WithdrawalEffectRestoreRetry (missing from go test -json output)
- FAIL: TestT058WithdrawalEffectRestoreRetry reported from unexpected package ""
- NOT RUN: TestT059F1BackupUnusableCorruptTruncatedUnverified (missing from go test -json output)
- FAIL: TestT059F1BackupUnusableCorruptTruncatedUnverified reported from unexpected package ""
- NOT RUN: TestT059F2RestoreInterruptionNotRestoredRebuildRerunIdempotent (missing from go test -json output)
- FAIL: TestT059F2RestoreInterruptionNotRestoredRebuildRerunIdempotent reported from unexpected package ""
- NOT RUN: TestT059F3IncompatibleProgramRefusedNoSilentDowngrade (missing from go test -json output)
- FAIL: TestT059F3IncompatibleProgramRefusedNoSilentDowngrade reported from unexpected package ""
- NOT RUN: TestT059F4ExternalLeadZeroReplayZeroAuthorityWrites (missing from go test -json output)
- FAIL: TestT059F4ExternalLeadZeroReplayZeroAuthorityWrites reported from unexpected package ""
- NOT RUN: TestT059F5IsolationUnprovenRefusedAndNotInferred (missing from go test -json output)
- FAIL: TestT059F5IsolationUnprovenRefusedAndNotInferred reported from unexpected package ""
- NOT RUN: TestT059F6UnprovableGapStaysUnknownWithPackageAndEscalation (missing from go test -json output)
- FAIL: TestT059F6UnprovableGapStaysUnknownWithPackageAndEscalation reported from unexpected package ""
- NOT RUN: TestT059F7UnauthorizedInsufficientStaleApprovalsRefused (missing from go test -json output)
- FAIL: TestT059F7UnauthorizedInsufficientStaleApprovalsRefused reported from unexpected package ""
- NOT RUN: TestT060DrillRepeatedVerifyApproveReleaseCloseNoFlips (missing from go test -json output)
- FAIL: TestT060DrillRepeatedVerifyApproveReleaseCloseNoFlips reported from unexpected package ""
- NOT RUN: TestT060DrillRestoreReentryConvergesWithSingleState (missing from go test -json output)
- FAIL: TestT060DrillRestoreReentryConvergesWithSingleState reported from unexpected package ""
- NOT RUN: TestT060DrillInstanceCloseOnlyWhenAllReleasedAndIdempotent (missing from go test -json output)
- FAIL: TestT060DrillInstanceCloseOnlyWhenAllReleasedAndIdempotent reported from unexpected package ""
- NOT RUN: TestT059F1BackupUnusableCorruptTruncatedUnverified/corrupt-verification (missing from go test -json output)
- FAIL: TestT059F1BackupUnusableCorruptTruncatedUnverified/corrupt-verification reported from unexpected package ""
- NOT RUN: TestT059F1BackupUnusableCorruptTruncatedUnverified/edited-verified-manifest (missing from go test -json output)
- FAIL: TestT059F1BackupUnusableCorruptTruncatedUnverified/edited-verified-manifest reported from unexpected package ""
- NOT RUN: TestT059F3IncompatibleProgramRefusedNoSilentDowngrade/incompatible-restore (missing from go test -json output)
- FAIL: TestT059F3IncompatibleProgramRefusedNoSilentDowngrade/incompatible-restore reported from unexpected package ""
exit status 1
```

Report `i-report.json` (key fields): `{"run_id": "run.i", "go_test_exit_status": 1, "checker_result": "FAIL", "checker_exit_status": 1, "overall_result": "FAIL"}
required_cases statuses: {"NOTRUN": 22} (total 22)`

Note (not a discrepancy): every missing required name yields **two** diagnostics — the `NOT RUN … (missing from
go test -json output)` line (check.go:146) *and* `FAIL: … reported from unexpected package ""` (check.go:109-111),
because an absent test also has no package. 22 cases × 2 = 44 diagnostic lines; this exact pattern also appears in
the production log (see §4).

#### (ii) Non-empty file with `go_status=0` but required tests missing → expect FAIL

Command: `… /tmp/drill-entry-acceptance/ii-unrelated.jsonl /tmp/drill-entry-acceptance/ii-report.json 0 ./... run.ii …`
(file contains one unrelated `TestUnrelatedPresent` pass event).

Observed exit: **1** (stdout empty). Raw stderr (identical diagnostic set to case (i) — all 22 required cases absent):

```
Drill scenario coverage incomplete:
- NOT RUN: TestDrillArmRefusesReplacedELFAndPreStartTamper (missing from go test -json output)
- FAIL: TestDrillArmRefusesReplacedELFAndPreStartTamper reported from unexpected package ""
- NOT RUN: TestDrillTargetWitnessOldReconnectRejected (missing from go test -json output)
- FAIL: TestDrillTargetWitnessOldReconnectRejected reported from unexpected package ""
- NOT RUN: TestDrillTargetWitnessOldRoleLoginDuringRestoreAcceptanceFailsClosed (missing from go test -json output)
- FAIL: TestDrillTargetWitnessOldRoleLoginDuringRestoreAcceptanceFailsClosed reported from unexpected package ""
- NOT RUN: TestDrillTargetWitnessObserverTerminationDuringRestoreAcceptanceFailsClosed (missing from go test -json output)
- FAIL: TestDrillTargetWitnessObserverTerminationDuringRestoreAcceptanceFailsClosed reported from unexpected package ""
- NOT RUN: TestT058DrillE2EBackupAdvanceRestoreRefuseThenRelease (missing from go test -json output)
- FAIL: TestT058DrillE2EBackupAdvanceRestoreRefuseThenRelease reported from unexpected package ""
- NOT RUN: TestT058DrillGapCannotBeClosedStaysPaused (missing from go test -json output)
- FAIL: TestT058DrillGapCannotBeClosedStaysPaused reported from unexpected package ""
- NOT RUN: TestT058DrillEventLayerKafkaBrokerOffsetDivergence (missing from go test -json output)
- FAIL: TestT058DrillEventLayerKafkaBrokerOffsetDivergence reported from unexpected package ""
- NOT RUN: TestT058DrillEventEffectRecovery (missing from go test -json output)
- FAIL: TestT058DrillEventEffectRecovery reported from unexpected package ""
- NOT RUN: TestT058WithdrawalEffectRestoreRetry (missing from go test -json output)
- FAIL: TestT058WithdrawalEffectRestoreRetry reported from unexpected package ""
- NOT RUN: TestT059F1BackupUnusableCorruptTruncatedUnverified (missing from go test -json output)
- FAIL: TestT059F1BackupUnusableCorruptTruncatedUnverified reported from unexpected package ""
- NOT RUN: TestT059F2RestoreInterruptionNotRestoredRebuildRerunIdempotent (missing from go test -json output)
- FAIL: TestT059F2RestoreInterruptionNotRestoredRebuildRerunIdempotent reported from unexpected package ""
- NOT RUN: TestT059F3IncompatibleProgramRefusedNoSilentDowngrade (missing from go test -json output)
- FAIL: TestT059F3IncompatibleProgramRefusedNoSilentDowngrade reported from unexpected package ""
- NOT RUN: TestT059F4ExternalLeadZeroReplayZeroAuthorityWrites (missing from go test -json output)
- FAIL: TestT059F4ExternalLeadZeroReplayZeroAuthorityWrites reported from unexpected package ""
- NOT RUN: TestT059F5IsolationUnprovenRefusedAndNotInferred (missing from go test -json output)
- FAIL: TestT059F5IsolationUnprovenRefusedAndNotInferred reported from unexpected package ""
- NOT RUN: TestT059F6UnprovableGapStaysUnknownWithPackageAndEscalation (missing from go test -json output)
- FAIL: TestT059F6UnprovableGapStaysUnknownWithPackageAndEscalation reported from unexpected package ""
- NOT RUN: TestT059F7UnauthorizedInsufficientStaleApprovalsRefused (missing from go test -json output)
- FAIL: TestT059F7UnauthorizedInsufficientStaleApprovalsRefused reported from unexpected package ""
- NOT RUN: TestT060DrillRepeatedVerifyApproveReleaseCloseNoFlips (missing from go test -json output)
- FAIL: TestT060DrillRepeatedVerifyApproveReleaseCloseNoFlips reported from unexpected package ""
- NOT RUN: TestT060DrillRestoreReentryConvergesWithSingleState (missing from go test -json output)
- FAIL: TestT060DrillRestoreReentryConvergesWithSingleState reported from unexpected package ""
- NOT RUN: TestT060DrillInstanceCloseOnlyWhenAllReleasedAndIdempotent (missing from go test -json output)
- FAIL: TestT060DrillInstanceCloseOnlyWhenAllReleasedAndIdempotent reported from unexpected package ""
- NOT RUN: TestT059F1BackupUnusableCorruptTruncatedUnverified/corrupt-verification (missing from go test -json output)
- FAIL: TestT059F1BackupUnusableCorruptTruncatedUnverified/corrupt-verification reported from unexpected package ""
- NOT RUN: TestT059F1BackupUnusableCorruptTruncatedUnverified/edited-verified-manifest (missing from go test -json output)
- FAIL: TestT059F1BackupUnusableCorruptTruncatedUnverified/edited-verified-manifest reported from unexpected package ""
- NOT RUN: TestT059F3IncompatibleProgramRefusedNoSilentDowngrade/incompatible-restore (missing from go test -json output)
- FAIL: TestT059F3IncompatibleProgramRefusedNoSilentDowngrade/incompatible-restore reported from unexpected package ""
exit status 1
```

Report `ii-report.json` (key fields): `{"run_id": "run.ii", "go_test_exit_status": 0, "checker_result": "FAIL", "checker_exit_status": 1, "overall_result": "FAIL"}
required_cases statuses: {"NOTRUN": 22} (total 22)`

#### (iii) One required test with `Action=skip` → expect FAIL

Command: `… /tmp/drill-entry-acceptance/iii-skip.jsonl /tmp/drill-entry-acceptance/iii-report.json 0 ./... run.iii …`
(every required case emits `pass` except `TestDrillArmRefusesReplacedELFAndPreStartTamper`, which emits `skip`).

Observed exit: **1** (stdout empty). Raw stderr (complete):

```
Drill scenario coverage incomplete:
- NOT RUN: TestDrillArmRefusesReplacedELFAndPreStartTamper (SKIP)
exit status 1
```

Report `iii-report.json` (key fields): `{"run_id": "run.iii", "go_test_exit_status": 0, "checker_result": "FAIL", "checker_exit_status": 1, "overall_result": "FAIL"}
required_cases statuses: {"SKIP": 1, "PASS": 21} (total 22)`

Single diagnostic — because the skip is reported from the correct pinned package, the package-pin check passes and
only the skip/status rule fires (check.go:139-140).

#### (iv) All `requiredTests` + `requiredSubtests` pass, `go_status=0` → expect PASS

Command: `… /tmp/drill-entry-acceptance/iv-allpass.jsonl /tmp/drill-entry-acceptance/iv-report.json 0 ./... run.iv …`
(all 22 names emit exactly one `pass` event from `github.com/xtianxx/txharbor/internal/recovery`).

Observed exit: **0**. Raw stdout (complete):

```
All required drill scenarios and restore-dependent subtests passed.
```

Raw stderr: empty. Report `iv-report.json` (key fields): `{"run_id": "run.iv", "go_test_exit_status": 0, "checker_result": "PASS", "checker_exit_status": 0, "overall_result": "PASS"}
required_cases statuses: {"PASS": 22} (total 22)`

---

## 4. Production artifact confirmation — run 37298994567

Artifact `run.pBV0i8/coverage.json` (downloaded from `drill-evidence-37298994567`):

- `coverage.json` line 7: `"go_test_exit_status": 1` — matches prediction (a): `go test` failed (build failures).
- `coverage.json` lines 8-9: `"checker_result": "FAIL"`, `"checker_exit_status": 1` — checker ran after the failed
  `go test` and reported FAIL/exit 1.
- `coverage.json` line 10: `"overall_result": "FAIL"`; all 22 `required_cases` are `NOTRUN` /
  `"missing from go test -json output"` (coverage.json lines 11-83).
- `coverage.json` line 5 `tree_fingerprint` `sha256:cce2675f…dda9` equals `tree-validation.json` line 1
  `fingerprint_before`/`fingerprint_after` (`"source_tree_stable":true`) — wrapper post-check passed, so the
  downstream exit was the docker/make status, not a fingerprint failure.
- `metadata.json` line 1: pinned image digest `sha256:4ef4dbc9…2280`, `CGO_ENABLED=0`, `module_cache: read-only`,
  Go 1.26.5, PG tools 18.6 direct-ELF verified.

Full run log `logs/run-37298994567-full.log`:

- Log 369-370: `go: downloading github.com/testcontainers/testcontainers-go/modules/postgres v0.44.0` and
  `go: downloading github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1` — both absent from the read-only cache.
- Log 371-376, 470-475: build failures `mkdir /host-gomodcache/cache/download/…: read-only file system`
  (go-ethereum `crypto/signature_nocgo.go:28-29` for decred; `internal/recovery/borrowed-transport-phase1_linux_test.go:31`
  for testcontainers postgres); `internal/recovery [setup failed]` (log 474-475).
- Log 1633-1678: the Makefile `cat "$$events"` dump ends, then `Drill scenario coverage incomplete:` with exactly
  the 44 diagnostics (22 × NOT RUN missing + 22 × `FAIL: … unexpected package ""`) — byte-pattern identical to my
  synthetic cases (i)/(ii).
- Log 1679: `exit status 1` (from `go run` around the checker). Log 1680: `make: *** [Makefile:76: test-drill] Error 1`.
  Log 1681: `##[error]Process completed with exit code 2.` — i.e. recipe exited `go_status`=1 (Makefile:90) → make
  process 2 → docker run 2 → wrapper `exit "$docker_status"` 2 → step red, exactly as the §1.2 table predicts.

Conclusion: the (a) build-failure path behaved precisely as predicted: `go_test_exit_status=1`, checker FAIL/exit 1,
evidence retained (upload `if: always()`), step non-zero (2). No contradiction found.

---

## 5. Can the proposed env fix (prime host Go module cache before `docker run`) weaken anything?

Proposed fix: pre-populate the host module cache (`$(go env GOMODCACHE)`, resolved at run-in-container.sh:16 and
bind-mounted read-only at `:56`, container `GOMODCACHE=/host-gomodcache` `:64`) before the drill, with **no changes
to Makefile / check.go / check_test.go / workflow**.

- **No weakening of §2 items is possible from this fix.** It adds module-cache entries on the host; it does not touch
  the `requiredTests`/`requiredSubtests` lists or package pin (check.go:15-40, 105-133), the NOT RUN rules
  (Makefile:3-13; check.go:138-148, 199-206), the tree-fingerprint definition/validation (run-in-container.sh:37-41,
  77, 135-143; check.go:182-184, 235-240, 291-367 — the cache lives outside the repo, so it is not an input to the
  fingerprint), the per-run `mktemp` evidence naming (Makefile:73-84; run-in-container.sh:27), or the exit chain
  (Makefile:87-90; run-in-container.sh:89/133/144).
- **It is fail-closed and additive**: the observed root cause is that two modules pinned in `go.mod`
  (`go.mod:29` testcontainers postgres v0.44.0; `go.mod:55` decred secp256k1 v4.0.1) were missing from the
  read-only cache, so Go tried to `mkdir …/cache/download/…` and got `read-only file system` (log 372 etc.).
  Priming materializes exactly those pinned versions; if priming is incomplete, the same red failure remains —
  the fix cannot turn (a)-(d) into a pass.
- **Caveats the fix must respect (none proposed violate them):**
  1. `[INFERENCE]` Prime complete entries (download metadata + extracted modules) for the exact versions in `go.sum`;
     a partial/interrupted `go mod download` would reproduce the same failure. (Cache-hit verification is an
     environment-audit concern; not exercised here — this ticket only validates the entry/checker logic and that
     the fix cannot weaken it.)
  2. Do not enable `-mod=mod` / `GOFLAGS` writes inside the container: `/workspace` is read-only (run-in-container.sh:54)
     and `go.mod`/`go.sum` must not change; `go mod download` on the host only writes the module cache.
  3. Do not add retries/skips/`|| true` around `go test` or the checker; none are proposed.
- **Triggering stays schedule/manual-only (no PR gate):** `drill.yml:44-48` declares only `schedule` and
  `workflow_dispatch`; `ci.yml:216-224` already enforces this with an isolation guard that fails if a
  `pull_request` trigger ever appears on `drill.yml` (error text at ci.yml:218: "the drill channel stays
  schedule/dispatch only and must never block an ordinary PR"; ci.yml:221 logs "ci.yml carries no drill
  invocation"). The proposed fix touches neither file, so drill evidence remains an independent channel and
  ordinary PRs are unaffected.

---

## Artifacts / reproduction paths

- Notes: this file. Synthetic inputs + raw outputs: `/tmp/drill-entry-acceptance/` (`gen.py`, `run-cases.sh`,
  `*.jsonl`, `*-stdout.txt`, `*-stderr.txt`, `*-report.json`, `lists.json`).
- Source evidence: `Makefile:3-13,73-90`; `scripts/drillcoverage/check.go:15-40,104-148,160-208,211-240,245-289,291-367`;
  `scripts/drillcoverage/check_test.go` (tests for exactly these negatives);
  `scripts/drillcoverage/run-in-container.sh:16,27,29-41,54-77,89-144`; `.github/workflows/drill.yml:12-19,44-48,81-92`;
  `.github/workflows/ci.yml:216-224`.
- Production run: `.evidence/drill-runner-env/artifact-37298994567/drill-evidence-37298994567/` (`run.pBV0i8/coverage.json`,
  `run.pBV0i8/go-test.json`, `wrapper.IzHGk2/tree-validation.json`, `wrapper.IzHGk2/metadata.json`) and
  `.evidence/drill-runner-env/logs/run-37298994567-full.log` (lines cited inline).
