// drill.go implements T057 [US6]: the `recovery-admin drill` wiring — the
// isolated-environment recovery drill that executes the real restore flow (a
// real pg_restore, never a re-initialized empty database), records the S12
// measurements separately (recovery point, database restore duration,
// verification duration, per-capability safe resumption, backup lag,
// uncovered interval, gap counts and dispositions) and archives the run with
// a reference under docs/evidence/015/ (FR-030/031/036).
//
// Trust boundary and operating discipline:
//
//  1. Required flags: --manifest, --target-dsn, --instance, --chain-id. The
//     deployment binding TXHARBOR_RECOVERY_INSTANCE, when present, must equal
//     --instance. The authenticated subject is TXHARBOR_RECOVERY_PRINCIPAL,
//     which must hold the executor participant role on the instance; free
//     text never authorizes.
//  2. Isolation: --target-dsn must address the environment's own data
//     database (TXHARBOR_PG_DSN) and must never address the control store.
//     The drill restores into the recovery environment it verifies; a drill
//     that restored one database and verified another would prove nothing.
//  3. Real recovery flow: unless --record-only is given, the command runs
//     recovery.ExecuteRestore (real pg_restore + the four probes) and measures
//     the database restore scope from the real execution. --record-only never
//     claims a restore: it requires an accepted restore_probe evidence row
//     (a real restore must already have happened) and otherwise refuses.
//  4. Separate timing scopes: recovery point, db_restore_seconds,
//     verification_seconds, per-capability release seconds, backup lag and
//     uncovered interval are recorded as separate fields. No code path
//     derives an RTO verdict from a single scope; a database connection or a
//     completed restore is never an RTO measurement. If a target is
//     configured and an end-to-end measurement exists (all seven capabilities
//     observed released), metrics.EvaluateRTOTarget judges it; an exceeded
//     target is recorded not-met, alerted and escalated, and never
//     permanently blocks later safe resumption.
//  5. Configured vs unconfigured: the required recovery constraints
//     (TXHARBOR_RECOVERY_RPO_TARGET / _RTO_TARGET / _BACKUP_FREQUENCY /
//     _RETENTION) are read by exact key name. An unconfigured constraint is
//     recorded as an explicit unconfigured state (with its purpose) and
//     constraints_configured=false; a configured-but-invalid value refuses by
//     key name. Local drill values are test inputs only, never production
//     thresholds (T000-P stays OPEN).
//  6. Non-mutating beyond the drill itself: no approval and no release is
//     created. Per-capability release state is observed through the derived
//     gate evaluation (the single authority) at the canonical scope template
//     chain=<chain>;capability=<capability>; refusals are recorded as such.
//     Evidence gaps, isolation and approvals remain the only blockers.
//  7. Result classification is observed, never operator-declared: a declared
//     failure injection whose refusal was not observed is refused outright
//     (no false injection claim is recorded).
//  8. Idempotent: --operation-id replays the recorded outcome with zero side
//     effects (same input) or conflicts with zero writes (different input);
//     omitting it is a real rerun. Exit codes: 0 recorded, 1 refused/config
//     error, 2 usage. Plaintext DSNs never reach stdout/stderr; messages pass
//     through logx.Redact and targets are reported as credential-free
//     fingerprints.
//
// Dependencies: T055 (internal/metrics/recovery.go: the RTO evaluation), T056
// (internal/recovery/drill.go: the recovery_drill_run model).
package recoveryadmin

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/metrics"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// recoveryActionDrill is the recovery_audit action token of
// `recovery-admin drill`.
const recoveryActionDrill = "drill"

// recoveryDrillDefaultOutDir is the normative archive directory of drill
// artifacts (the T057 reference under docs/evidence/015/). It is overridden
// by --out; no repository write happens without a real drill invocation.
const recoveryDrillDefaultOutDir = "docs/evidence/015/drill"

// recoveryDrillPurpose is the mandatory purpose annotation of every drill
// record: local inputs, never production thresholds.
const recoveryDrillPurpose = "local drill inputs only; not production thresholds; T000-P stays OPEN"

// recoveryDrillConstraints are the four required recovery constraints of
// FR-036/C1, in canonical order.
var recoveryDrillConstraints = []struct {
	Key     string
	EnvKey  string
	Purpose string
}{
	{Key: "rpo_target", EnvKey: config.EnvRecoveryRPOTarget, Purpose: "recovery-point objective; local test input only (production value is a pending deployment ruling)"},
	{Key: "rto_target", EnvKey: config.EnvRecoveryRTOTarget, Purpose: "recovery-time objective; local test input only (production value is a pending deployment ruling)"},
	{Key: "backup_frequency", EnvKey: config.EnvRecoveryBackupFrequency, Purpose: "backup cadence; local test input only (a cadence is never a proven RPO)"},
	{Key: "retention", EnvKey: config.EnvRecoveryRetention, Purpose: "retention period; local test input only (production value is a pending deployment ruling)"},
}

// recoveryDrillConstraintStatus is the explicit configured / not-configured
// state of one required constraint (recorded in test_inputs).
type recoveryDrillConstraintStatus struct {
	Key        string `json:"key"`
	Configured bool   `json:"configured"`
	Value      string `json:"value,omitempty"`
	Purpose    string `json:"purpose"`
	State      string `json:"state"`
}

// recoveryAdminDrill implements `recovery-admin drill`.
func recoveryAdminDrill(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			recoveryAdminActionUsage(stdout, recoveryAdminActionByNameOrZero("drill"))
			return 0
		}
	}
	fs := flag.NewFlagSet("txharbor recovery-admin drill", flag.ContinueOnError)
	fs.SetOutput(stderr)
	manifestPath := fs.String("manifest", "", "manifest path (required; the control store must hold verified evidence bound to the instance and backup_id)")
	targetDSN := fs.String("target-dsn", "", "isolated recovery-environment DSN (or "+recoveryTargetDSNEnv+"; must address TXHARBOR_PG_DSN, never the control store)")
	instanceFlag := fs.String("instance", "", "recovery instance id (required)")
	chainIDFlag := fs.String("chain-id", "", "scope chain id (required; per-capability observation scope dimension)")
	scenarioFlag := fs.String("scenario", string(recovery.DrillScenarioFullRecovery), "drill scenario: full_recovery or one of the seven failure injections (f1_..f7_)")
	outFlag := fs.String("out", "", "archive directory (default "+recoveryDrillDefaultOutDir+")")
	operationID := fs.String("operation-id", "", "idempotent operation identity (optional)")
	recordOnly := fs.Bool("record-only", false, "record from the current control-store facts without running a restore (requires accepted restore evidence)")
	backupLagSeconds := fs.Float64("backup-lag-seconds", -1, "operator-provided backup-lag local input (optional; recorded as an explicit test input)")
	uncoveredIntervalSeconds := fs.Float64("uncovered-interval-seconds", -1, "operator-provided uncovered-interval local input (optional; recorded as an explicit test input)")
	signerEndpoint := fs.String("signer-endpoint", "", "signer boundary endpoint for reachability probing (optional)")
	rpcURL := fs.String("rpc-url", "", "RPC fact-source URL for the verification phase (optional; missing = verification not_configured)")
	brokerDSN := fs.String("broker-dsn", "", "broker DSN for reachability probing (optional; or "+recoveryBrokerDSNEnv+")")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	targetFlagSet, brokerFlagSet := false, false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "target-dsn" {
			targetFlagSet = true
		}
		if f.Name == "broker-dsn" {
			brokerFlagSet = true
		}
	})
	resolvedTarget, inputErr := recoveryProtectedDSNInput(*targetDSN, targetFlagSet, d.getenv(), recoveryTargetDSNEnv, "--target-dsn")
	if inputErr == nil {
		*targetDSN = resolvedTarget
	}
	resolvedBroker, brokerErr := recoveryProtectedDSNInput(*brokerDSN, brokerFlagSet, d.getenv(), recoveryBrokerDSNEnv, "--broker-dsn")
	if brokerErr == nil {
		*brokerDSN = resolvedBroker
	}
	if inputErr != nil || brokerErr != nil {
		fmt.Fprintln(stderr, "txharbor recovery-admin drill: invalid DSN source selection; refusing")
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*manifestPath) == "" || strings.TrimSpace(*targetDSN) == "" ||
		strings.TrimSpace(*instanceFlag) == "" || strings.TrimSpace(*chainIDFlag) == "" {
		recoveryAdminActionUsage(stderr, recoveryAdminActionByNameOrZero("drill"))
		return 2
	}
	scenario, err := recovery.ParseDrillScenario(*scenarioFlag)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: %s\n", logx.Redact(err.Error()))
		return 2
	}
	chainID, err := recoveryDrillChainID(*chainIDFlag)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: %s\n", logx.Redact(err.Error()))
		return 2
	}
	if err := recoveryDrillOptionalSeconds("--backup-lag-seconds", *backupLagSeconds); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: %s\n", logx.Redact(err.Error()))
		return 2
	}
	if err := recoveryDrillOptionalSeconds("--uncovered-interval-seconds", *uncoveredIntervalSeconds); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: %s\n", logx.Redact(err.Error()))
		return 2
	}
	operation, err := recoveryOpOperationID(*operationID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: %s\n", logx.Redact(err.Error()))
		return 2
	}

	instanceID, bound, code := recoveryOpInstanceContext(d, *instanceFlag, "drill")
	if code != 0 {
		return code
	}
	if !bound {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: %s is required (instance bound execution)\n", config.EnvRecoveryInstance)
		return 1
	}
	instanceID, err = recoveryDrillInstanceID(instanceID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: %s\n", logx.Redact(err.Error()))
		return 2
	}

	getenv := d.getenv()
	if getenv == nil {
		fmt.Fprintln(stderr, "txharbor recovery-admin drill: environment lookup is not wired; refusing")
		return 1
	}
	// A real restore needs an explicit deployment-configured observer with
	// visibility into tagged target sessions. Never infer it from --target-dsn.
	// Record-only does not execute a restore and retains its existing contract.
	observerDSN := ""
	if !*recordOnly {
		var ok bool
		observerDSN, ok = d.getenvValue(config.EnvRecoveryObserverDSN)
		if !ok || strings.TrimSpace(observerDSN) == "" {
			fmt.Fprintf(stderr, "txharbor recovery-admin drill: %s is required (not configured); refusing\n", config.EnvRecoveryObserverDSN)
			return 1
		}
	}

	env, code := recoveryOpOpen(ctx, d, "drill")
	if env == nil {
		return code
	}
	defer env.pool.Close()

	if err := recoveryOpRequireParticipant(ctx, env, instanceID, "executor", "drill"); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if err := recoveryDrillInstanceOpen(ctx, env, instanceID); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: refused: %s\n", logx.Redact(err.Error()))
		return 1
	}

	// Isolation guard: restore into the environment's own data database, never
	// the control store. The comparison is credential-free.
	if err := recoveryDrillTargetGuard(env, *targetDSN); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	gateTTL, err := recoveryDrillGateTTL(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: %s; refusing\n", logx.Redact(err.Error()))
		return 1
	}
	constraints, constraintsConfigured, err := recoveryDrillConstraintsOf(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: %s; refusing\n", logx.Redact(err.Error()))
		return 1
	}
	rtoTarget, rtoTargetConfigured, err := recoveryDrillRTOTargetOf(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: %s; refusing\n", logx.Redact(err.Error()))
		return 1
	}
	dataFingerprint := recoveryTargetFingerprint(env.dataDSN)

	// The recovery point comes from the real manifest (never a summary
	// string); --record-only uses the same manifest identity.
	manifest, err := recoveryDrillReadManifest(strings.TrimSpace(*manifestPath))
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: %s\n", logx.Redact(err.Error()))
		return 1
	}
	recoveryPoint, err := json.Marshal(manifest.RecoveryPoint)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: encode recovery point: %s\n", logx.Redact(err.Error()))
		return 1
	}

	// Idempotency identity (never includes generated ids or wall-clock data).
	inputDigest := recoveryOpInputDigest(recoveryActionDrill, map[string]string{
		"manifest":                   strings.TrimSpace(*manifestPath),
		"target":                     recoveryTargetFingerprint(*targetDSN),
		"instance_id":                instanceID,
		"scenario":                   string(scenario),
		"chain_id":                   strconv.FormatUint(chainID, 10),
		"record_only":                strconv.FormatBool(*recordOnly),
		"backup_lag_seconds":         recoveryDrillOptionalSecondsValue(*backupLagSeconds),
		"uncovered_interval_seconds": recoveryDrillOptionalSecondsValue(*uncoveredIntervalSeconds),
	})
	target := map[string]any{
		"instance_id": instanceID,
		"scenario":    string(scenario),
		"record_only": *recordOnly,
	}
	refuse := func(refusalClass, reason string, extra map[string]any) int {
		detail := map[string]any{"input_digest": inputDigest, "reason": logx.Redact(reason)}
		for key, value := range extra {
			detail[key] = value
		}
		if err := recoveryOpRecord(ctx, env.pool, recoveryActionDrill, operation, env.principal,
			controlstore.AuditRefused, detail, target); err != nil {
			reason += "; audit refusal could not be persisted: " + logx.Redact(err.Error())
		}
		if refusalClass != "" {
			fmt.Fprintf(stderr, "txharbor recovery-admin drill: refused: refusal_class=%s %s\n",
				refusalClass, logx.Redact(reason))
		} else {
			fmt.Fprintf(stderr, "txharbor recovery-admin drill: refused: %s\n", logx.Redact(reason))
		}
		return 1
	}

	release, recorded, err := recoveryOpBegin(ctx, env.pool, recoveryActionDrill, operation)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if release != nil {
		defer release()
	}
	if replayed, code := recoveryDrillReplay(stderr, stdout, recorded, inputDigest, operation); replayed {
		return code
	}
	boundEntryChains, inventoryErr := recoveryDrillBoundEntryChains(ctx, env.pool, instanceID)
	if inventoryErr != nil {
		// Missing/legacy/malformed inventory is never inferred from --chain-id.
		boundEntryChains = nil
	}

	// --record-only never claims a restore: a real restore must already have
	// been accepted for this instance, otherwise this refuses.
	var recordGeneration *int64
	if *recordOnly {
		manifestDigest, digestErr := manifest.Digest()
		if digestErr != nil {
			return refuse(string(recovery.RefusalControlStoreUnavailable), "compute manifest digest: "+digestErr.Error(), nil)
		}
		accepted, generation, err := recoveryDrillHasRestoreEvidence(ctx, env.pool, instanceID, manifest.BackupID,
			manifestDigest, recoveryTargetFingerprint(*targetDSN))
		if err != nil {
			return refuse(string(recovery.RefusalControlStoreUnavailable), err.Error(), nil)
		}
		if !accepted {
			return refuse("",
				"record-only requires an accepted restore_probe evidence row for this instance (a real restore must have happened; a re-initialized empty database is never a drill)", nil)
		}
		recordGeneration = &generation
	}

	programVersion, err := recovery.CurrentProgramVersion()
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: %s\n", logx.Redact(err.Error()))
		return 1
	}
	drillID := uuid.NewString()
	// Retain the monotonic clock component for the elapsed restore scope.
	restoreStarted := time.Now()

	// ------------------------------------------------------------------
	// Phase 1: real restore (or an explicit record-only skip).
	// ------------------------------------------------------------------
	restoreState := "skipped_record_only"
	var (
		dbRestoreSeconds *float64
		restoreBlocked   []string
		restoreRefusal   string
	)
	if !*recordOnly {
		// The started marker is durable before the first destructive side effect.
		// An interrupted operation is intentionally not retried under the same id.
		if err := recoveryOpRecord(ctx, env.pool, recoveryActionDrill, operation, env.principal,
			controlstore.AuditRefused, map[string]any{"stage": "started", "input_digest": inputDigest}, target); err != nil {
			fmt.Fprintf(stderr, "txharbor recovery-admin drill: cannot persist pre-restore operation marker: %s\n", logx.Redact(err.Error()))
			return 1
		}
		result, err := recovery.ExecuteRestore(ctx, recovery.RestoreOptions{
			ManifestPath:      strings.TrimSpace(*manifestPath),
			InstanceID:        instanceID,
			ControlStore:      env.store,
			ControlDSN:        env.controlDSN,
			ObserverDSN:       observerDSN,
			TargetDSN:         *targetDSN,
			TargetDeclaration: recovery.TargetIsolated,
			TargetReason:      "recovery drill (isolated environment)",
			Actor:             env.principal,
			ProgramVersion:    programVersion,
			OperationID:       operation,
			PG:                recovery.LocalPGCommand{},
			Convergence:       recoveryOpConvergenceStep(d, env, strings.TrimSpace(*targetDSN)),
			SignerEndpoint:    strings.TrimSpace(*signerEndpoint),
			RPCURL:            strings.TrimSpace(*rpcURL),
			BrokerDSN:         strings.TrimSpace(*brokerDSN),
		})
		if err != nil {
			restoreState = "refused"
			restoreBlocked = result.Blocked
			restoreRefusal = logx.Redact(err.Error())
		} else {
			restoreState = "executed"
			seconds := time.Since(restoreStarted).Seconds()
			dbRestoreSeconds = &seconds
			// The restored run's real marker is the timing origin of the
			// per-capability release observation.
			if observed, markerErr := recoveryDrillRestoreStartedAt(ctx, env.pool, instanceID, manifest.BackupID,
				recoveryTargetFingerprint(*targetDSN)); markerErr == nil && !observed.IsZero() {
				restoreStarted = observed
			} else if markerErr != nil || observed.IsZero() {
				restoreState = "observation_error"
				restoreRefusal = "restore-start marker could not be observed for this restore"
				if markerErr != nil {
					restoreRefusal += ": " + logx.Redact(markerErr.Error())
				}
			}
		}
	} else {
		// Record-only measures release times against the real restore marker
		// (never against this invocation's wall clock).
		if observed, err := recoveryDrillRestoreStartedAt(ctx, env.pool, instanceID, manifest.BackupID,
			recoveryTargetFingerprint(*targetDSN)); err == nil && !observed.IsZero() {
			restoreStarted = observed
		} else {
			restoreStarted = time.Time{}
			restoreState = "observation_error"
			restoreRefusal = "record-only could not observe the restore-start marker for this evidence generation"
			if err != nil {
				restoreRefusal += ": " + logx.Redact(err.Error())
			}
		}
	}

	// ------------------------------------------------------------------
	// Phase 2: verification (one bounded real step when configured).
	// ------------------------------------------------------------------
	verificationState := "skipped_record_only"
	var verificationSeconds *float64
	var verificationRefusal, verificationRef string
	if restoreState == "executed" {
		verificationState, verificationSeconds, verificationRefusal, verificationRef =
			recoveryDrillVerification(ctx, env, getenv, strings.TrimSpace(*rpcURL), chainID, dataFingerprint, instanceID, operation)
	} else if !*recordOnly {
		verificationState = "not_run_restore_refused"
	}

	// ------------------------------------------------------------------
	// Phase 3: observed facts (gaps, derived per-capability release state).
	// ------------------------------------------------------------------
	gapCounts, openGapCount, gapErr := recoveryDrillGapCounts(ctx, env, instanceID)
	if gapErr != nil {
		return refuse(string(recovery.RefusalControlStoreUnavailable),
			"the evidence-gap facts could not be read: "+gapErr.Error(), nil)
	}
	releaseSeconds, releaseObservations, releaseRefusals, releaseErr := recoveryDrillObserveReleases(
		ctx, env, instanceID, boundEntryChains, gateTTL, restoreStarted)
	if len(boundEntryChains) == 0 && releaseErr == nil {
		releaseErr = errors.New("bound entry-chain inventory is missing or invalid")
	}

	// ------------------------------------------------------------------
	// Phase 4: constraint states + RTO evaluation.
	// ------------------------------------------------------------------
	unconfigured := make([]string, 0, len(constraints))
	for _, constraint := range constraints {
		if !constraint.Configured {
			unconfigured = append(unconfigured, constraint.Key)
		}
	}
	fullComplete := recoveryDrillFullRecoveryComplete(*recordOnly, restoreState, verificationState, strings.TrimSpace(*rpcURL) != "",
		releaseSeconds, releaseObservations, releaseErr, openGapCount, boundEntryChains)
	measured, measuredKnown := recoveryDrillSafeResumption(releaseSeconds)
	measuredKnown = measuredKnown && fullComplete
	rtoEval := metrics.EvaluateRTOTarget(
		rtoTarget, time.Duration(measured*float64(time.Second)), metrics.RTOScopeSafeResumption, measuredKnown)

	// Result classification (observed, never operator-declared).
	refusalObserved := restoreState == "refused" || restoreState == "observation_error" || verificationState == "refused" ||
		verificationState == "discarded" || openGapCount > 0 || releaseErr != nil
	injection, isInjection := scenario.FailureInjection()
	var drillResult recovery.DrillResult
	switch {
	case isInjection && recoveryDrillInjectionObserved(injection, restoreState, restoreBlocked, verificationState, openGapCount, releaseRefusals) && releaseErr == nil:
		drillResult = recovery.DrillResultFailedInjected
	case isInjection && (releaseErr != nil || restoreState == "observation_error" || refusalObserved):
		drillResult = recovery.DrillResultRefusedSafe
	case isInjection:
		return refuse("", fmt.Sprintf(
			"declared failure injection %s (%s) was not observed in this flow (restore=%s verification=%s open_gaps=%d); refusing to record a false injection claim",
			injection, scenario, restoreState, verificationState, openGapCount), nil)
	case refusalObserved:
		drillResult = recovery.DrillResultRefusedSafe
	case !fullComplete:
		drillResult = recovery.DrillResultRefusedSafe
	default:
		drillResult = recovery.DrillResultOK
	}

	// ------------------------------------------------------------------
	// Phase 5: archive + record (the archive is written first; a failed
	// record removes the orphan file so no artifact references a missing
	// drill row).
	// ------------------------------------------------------------------
	outDir := strings.TrimSpace(*outFlag)
	if outDir == "" {
		outDir = recoveryDrillDefaultOutDir
	}
	archivePath := filepath.Join(outDir, "drill-"+drillID+".json")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return refuse("", "the drill archive directory cannot be created: "+err.Error(), nil)
	}
	archive := recoveryDrillArchive{
		Purpose:                  recoveryDrillPurpose,
		DrillID:                  drillID,
		InstanceID:               instanceID,
		Scenario:                 string(scenario),
		FailureInjection:         injection,
		Result:                   string(drillResult),
		RecordOnly:               *recordOnly,
		RestoreState:             restoreState,
		RestoreBlocked:           restoreBlocked,
		RestoreRefusal:           restoreRefusal,
		ReleaseObservationError:  recoveryDrillOptionalError(releaseErr),
		VerificationState:        verificationState,
		VerificationRef:          verificationRef,
		VerificationRefusal:      logx.Redact(verificationRefusal),
		BackupID:                 manifest.BackupID,
		ManifestVersion:          manifest.ManifestVersion,
		RecoveryPoint:            json.RawMessage(recoveryPoint),
		DBRestoreSeconds:         dbRestoreSeconds,
		VerificationSeconds:      verificationSeconds,
		CapabilityReleaseSeconds: recoveryDrillReleaseDoc(releaseSeconds),
		ReleaseObservations:      releaseObservations,
		ReleaseRefusals:          releaseRefusals,
		BackupLag:                recoveryDrillLagDoc(*backupLagSeconds, "backup_lag_seconds"),
		UncoveredInterval:        recoveryDrillLagDoc(*uncoveredIntervalSeconds, "uncovered_interval_seconds"),
		ConstraintsConfigured:    constraintsConfigured,
		Constraints:              constraints,
		UnconfiguredConstraints:  unconfigured,
		RTO:                      rtoEval,
		RTOTargetConfigured:      rtoTargetConfigured,
		GateTTL:                  gateTTL.String(),
		GapCounts:                gapCounts,
		LocalValuesOnly:          true,
		NonClaims: []string{
			recoveryDrillPurpose,
			"a reachable database, a completed restore or any single timing scope is never an RTO measurement",
			"no cross-system exactly-once claim",
			"no external ledger consistency claim; only project-verifiable facts",
			"evidence gaps, isolation and approvals remain the only resumption blockers; a time target never permanently blocks later safe resumption",
		},
	}
	if err := recoveryDrillWriteArchive(archivePath, &archive); err != nil {
		return refuse("", "the drill archive cannot be written: "+err.Error(), nil)
	}
	testInputs, err := archive.TestInputs()
	if err != nil {
		_ = os.Remove(archivePath)
		return refuse("", "the drill test-input annotation cannot be encoded: "+err.Error(), nil)
	}
	run, err := recovery.RecordDrillRun(ctx, env.store, recovery.DrillRunInput{
		InstanceID:                 instanceID,
		DrillID:                    drillID,
		Scenario:                   scenario,
		RecoveryPoint:              recoveryPoint,
		DBRestoreSeconds:           dbRestoreSeconds,
		VerificationSeconds:        verificationSeconds,
		CapabilityReleaseSeconds:   releaseSeconds,
		BackupLag:                  archive.BackupLagJSON(),
		UncoveredInterval:          archive.UncoveredIntervalJSON(),
		ConstraintsConfigured:      constraintsConfigured,
		TestInputs:                 testInputs,
		GapCounts:                  archive.GapCountsJSON(),
		Result:                     drillResult,
		LogRef:                     archivePath,
		Actor:                      env.principal,
		OperationID:                operation,
		ExpectedEvidenceGeneration: recordGeneration,
	})
	if err != nil {
		_ = os.Remove(archivePath)
		if errors.Is(err, controlstore.ErrOperationConflict) {
			return refuse("", "operation_conflict: "+err.Error(), nil)
		}
		if errors.Is(err, recovery.ErrDrillStaleGeneration) {
			return refuse("", "record-only restore probe became stale before the drill row was inserted", nil)
		}
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: the drill run could not be recorded: %s\n", logx.Redact(err.Error()))
		return 1
	}

	detail := map[string]any{
		"input_digest":               inputDigest,
		"drill_id":                   run.DrillID,
		"scenario":                   string(run.Scenario),
		"failure_injection":          injection,
		"result":                     string(run.Result),
		"record_only":                *recordOnly,
		"restore_state":              restoreState,
		"verification_state":         verificationState,
		"db_restore_seconds":         dbRestoreSeconds,
		"verification_seconds":       verificationSeconds,
		"capability_release_seconds": releaseSeconds,
		"release_refusals":           releaseRefusals,
		"release_observation_error":  recoveryDrillOptionalError(releaseErr),
		"gaps":                       gapCounts,
		"constraints_configured":     constraintsConfigured,
		"unconfigured_constraints":   unconfigured,
		"rto":                        rtoEval,
		"archive":                    archivePath,
	}
	if err := recoveryOpRecord(ctx, env.pool, recoveryActionDrill, operation, env.principal,
		controlstore.AuditOK, detail, target); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: run recorded but terminal audit could not be persisted: %s\n", logx.Redact(err.Error()))
		return 1
	}

	recoveryDrillPrint(stdout, stderr, run, archive, releaseSeconds, rtoEval, archivePath, releaseErr)

	if rtoEval.Status == metrics.RTOStatusExceeded {
		fmt.Fprintf(stderr,
			"txharbor recovery-admin drill: ALERT+ESCALATION rto_target_exceeded measured=%.3fs target=%.3fs (recorded not-met; the timeout never permanently blocks later safe resumption)\n",
			rtoEval.Measured.Seconds(), rtoEval.Target.Seconds())
	}
	if run.Result == recovery.DrillResultRefusedSafe {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: required recovery stage was incomplete or refused; recorded result=%s\n", run.Result)
		return 1
	}
	return 0
}

// ---------------------------------------------------------------------------
// Configuration resolution
// ---------------------------------------------------------------------------

// recoveryDrillGateTTL resolves the required gate TTL by exact key name; the
// gate has no default.
func recoveryDrillGateTTL(getenv func(string) (string, bool)) (time.Duration, error) {
	raw, ok := getenv(config.EnvRecoveryGateTTL)
	if !ok || strings.TrimSpace(raw) == "" {
		return 0, fmt.Errorf("%s is required (not configured); the gate has no default TTL", config.EnvRecoveryGateTTL)
	}
	ttl, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || ttl <= 0 {
		return 0, fmt.Errorf("%s=%q is not a positive duration", config.EnvRecoveryGateTTL, raw)
	}
	return ttl, nil
}

// recoveryDrillConstraintsOf resolves the four required constraints and their
// explicit configured / not-configured state. A missing key is a state; a
// configured-but-invalid value refuses by key name.
func recoveryDrillConstraintsOf(getenv func(string) (string, bool)) ([]recoveryDrillConstraintStatus, bool, error) {
	statuses := make([]recoveryDrillConstraintStatus, 0, len(recoveryDrillConstraints))
	allConfigured := true
	for _, constraint := range recoveryDrillConstraints {
		status := recoveryDrillConstraintStatus{
			Key:     constraint.Key,
			Purpose: constraint.Purpose,
		}
		raw, ok := getenv(constraint.EnvKey)
		if !ok || strings.TrimSpace(raw) == "" {
			status.State = "not_configured"
			allConfigured = false
			statuses = append(statuses, status)
			continue
		}
		value, err := time.ParseDuration(strings.TrimSpace(raw))
		if err != nil || value <= 0 {
			return nil, false, fmt.Errorf("%s=%q is not a positive duration", constraint.EnvKey, raw)
		}
		status.Configured = true
		status.State = "configured"
		status.Value = value.String()
		statuses = append(statuses, status)
	}
	return statuses, allConfigured, nil
}

// recoveryDrillRTOTargetOf resolves the optional RTO target. Absent means not
// configured (nothing is claimed); a configured-but-invalid value refuses by
// key name.
func recoveryDrillRTOTargetOf(getenv func(string) (string, bool)) (time.Duration, bool, error) {
	raw, ok := getenv(config.EnvRecoveryRTOTarget)
	if !ok || strings.TrimSpace(raw) == "" {
		return 0, false, nil
	}
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0, false, fmt.Errorf("%s=%q is not a positive duration", config.EnvRecoveryRTOTarget, raw)
	}
	return value, true, nil
}

// ---------------------------------------------------------------------------
// Guards and observations
// ---------------------------------------------------------------------------

// recoveryDrillInstanceID canonicalizes --instance.
func recoveryDrillInstanceID(raw string) (string, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("--instance %q is not a UUID", raw)
	}
	return parsed.String(), nil
}

// recoveryDrillChainID resolves the required chain id (the per-capability
// observation scope dimension).
func recoveryDrillChainID(raw string) (uint64, error) {
	value, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil || value == 0 || value > 1<<62 {
		return 0, fmt.Errorf("--chain-id %q is not a supported chain id", raw)
	}
	return value, nil
}

// recoveryDrillOptionalSeconds validates an optional local-input duration;
// -1 means "not provided".
func recoveryDrillOptionalSeconds(flagName string, value float64) error {
	if value == -1 {
		return nil
	}
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return fmt.Errorf("%s=%v is not a non-negative finite duration", flagName, value)
	}
	return nil
}

// recoveryDrillOptionalSecondsValue renders an optional local input for the
// operation digest.
func recoveryDrillOptionalSecondsValue(value float64) string {
	if value == -1 {
		return ""
	}
	return strconv.FormatFloat(value, 'f', 3, 64)
}

// recoveryDrillReadManifest reads and parses the manifest identity (the
// recovery point is the manifest's own snapshot tuple, never a summary).
func recoveryDrillReadManifest(path string) (*recovery.Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	manifest, err := recovery.ParseManifest(data)
	if err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	return manifest, nil
}

// recoveryDrillInstanceOpen requires the bound instance to be the open
// recovery instance.
func recoveryDrillInstanceOpen(ctx context.Context, env *recoveryOpEnv, instanceID string) error {
	var kind, state string
	err := env.pool.QueryRow(ctx,
		`SELECT kind, state FROM recovery_instance WHERE instance_id = $1`, instanceID).Scan(&kind, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("instance %s does not exist", instanceID)
	}
	if err != nil {
		return fmt.Errorf("read recovery instance %s: %w", instanceID, err)
	}
	if kind != "recovery" || state != "open" {
		return fmt.Errorf("instance %s is not the open recovery instance (kind=%s state=%s)", instanceID, kind, state)
	}
	return nil
}

// recoveryDrillTargetGuard proves the drill restores into the environment's
// own data database and never the control store. All comparisons are on
// credential-free DSN targets.
func recoveryDrillTargetGuard(env *recoveryOpEnv, targetDSN string) error {
	target, err := controlstore.ParseDSNTarget(targetDSN)
	if err != nil {
		return fmt.Errorf("--target-dsn is invalid: %s", logx.Redact(err.Error()))
	}
	dataTarget, err := controlstore.ParseDSNTarget(env.dataDSN)
	if err != nil {
		return fmt.Errorf("%s is invalid: %s", config.EnvPGDSN, logx.Redact(err.Error()))
	}
	if !target.SameDatabase(dataTarget) {
		return fmt.Errorf("--target-dsn does not address %s (the recovery environment's own data database); the drill must restore into the environment it verifies",
			config.EnvPGDSN)
	}
	controlTarget, err := controlstore.ParseDSNTarget(env.controlDSN)
	if err != nil {
		return fmt.Errorf("%s is invalid: %s", config.EnvRecoveryControlDSN, logx.Redact(err.Error()))
	}
	if target.SameDatabase(controlTarget) {
		return fmt.Errorf("--target-dsn addresses the control-store database; the control store is outside the data restore set")
	}
	return nil
}

// recoveryDrillHasRestoreEvidence reports whether the instance has an
// accepted restore_probe evidence row (the machine-checkable proof that a
// real restore happened — a re-initialized empty database has none).
func recoveryDrillHasRestoreEvidence(ctx context.Context, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, instanceID, backupID, manifestDigest, targetFingerprint string) (bool, int64, error) {
	var generation int64
	if err := pool.QueryRow(ctx,
		`SELECT evidence_generation FROM recovery_instance WHERE instance_id = $1`, instanceID).Scan(&generation); err != nil {
		return false, 0, fmt.Errorf("read current evidence generation: %w", err)
	}
	var found bool
	err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM recovery_evidence
    WHERE instance_id = $1 AND kind = 'restore_probe' AND generation = $2
      AND artifact_hash = $3 AND scope->>'backup_id' = $4
      AND scope->>'manifest_digest' = $3
      AND scope->>'target_fingerprint' = $5
)`, instanceID, generation, manifestDigest, backupID, targetFingerprint).Scan(&found)
	if err != nil {
		return false, 0, fmt.Errorf("read accepted restore evidence: %w", err)
	}
	return found, generation, nil
}

// recoveryDrillBoundEntryChains reads only the instance's deployment-bound
// inventory. The required --chain-id is an observation input, not inventory.
func recoveryDrillBoundEntryChains(ctx context.Context, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, instanceID string) ([]uint64, error) {
	var chains []uint64
	var version *int32
	err := pool.QueryRow(ctx, `SELECT entry_chain_inventory, entry_chain_inventory_version FROM recovery_instance WHERE instance_id = $1`, instanceID).
		Scan(&chains, &version)
	if err != nil {
		return nil, fmt.Errorf("read bound entry-chain inventory: %w", err)
	}
	if version == nil || *version != 1 || controlstore.ValidateEntryChainInventory(chains) != nil {
		return nil, errors.New("bound entry-chain inventory is missing or invalid")
	}
	return chains, nil
}

// recoveryDrillFullRecoveryComplete is the sole positive classification gate:
// an executed restore, an executed verification phase, no gaps, and all seven
// capabilities observed released and measured in this generation. An
// unconfigured verification phase is unknown, not complete.
func recoveryDrillFullRecoveryComplete(recordOnly bool, restoreState, verificationState string,
	verificationConfigured bool, releaseSeconds map[recovery.Capability]float64,
	releaseObservations map[string]string, releaseErr error, openGapCount int, chains []uint64) bool {
	if recordOnly || restoreState != "executed" || openGapCount != 0 || releaseErr != nil {
		return false
	}
	if len(chains) == 0 || controlstore.ValidateEntryChainInventory(chains) != nil {
		return false
	}
	if !verificationConfigured || verificationState != "executed" {
		return false
	}
	if _, complete := recoveryDrillSafeResumption(releaseSeconds); !complete {
		return false
	}
	for _, chain := range chains {
		for _, capability := range recovery.KnownCapabilities() {
			scope := "chain=" + strconv.FormatUint(chain, 10) + ";capability=" + string(capability)
			if releaseObservations[scope] != "released" {
				return false
			}
		}
	}
	return true
}

func recoveryDrillOptionalError(err error) string {
	if err == nil {
		return ""
	}
	return logx.Redact(err.Error())
}

// recoveryDrillRestoreStartedAt reads the latest real restore-start marker
// (the timing origin of the per-capability release observation).
func recoveryDrillRestoreStartedAt(ctx context.Context, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, instanceID, backupID, targetFingerprint string) (time.Time, error) {
	var at time.Time
	err := pool.QueryRow(ctx,
		`SELECT created_at FROM recovery_audit WHERE instance_id = $1 AND action = $2
         AND target->>'backup_id' = $3 AND target->>'target_fingerprint' = $4
         ORDER BY audit_id DESC LIMIT 1`,
		instanceID, recovery.ActionRestoreStarted, backupID, targetFingerprint).Scan(&at)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("read restore-start marker: %w", err)
	}
	return at, nil
}

// recoveryDrillVerification runs one bounded real V1-V9 verification step
// when the fact-source configuration exists. Missing dependencies are an
// explicit not_configured state (never a pass); a refusal/discard is
// recorded as such.
func recoveryDrillVerification(ctx context.Context, env *recoveryOpEnv, getenv func(string) (string, bool),
	rpcURL string, chainID uint64, dataFingerprint, instanceID, operation string) (
	state string, seconds *float64, refusal, batchRef string) {
	if strings.TrimSpace(rpcURL) == "" {
		return "not_configured", nil, "no " + config.EnvRPCURL + " is configured; the verification phase did not run (never a pass)", ""
	}
	stepOperation := "drill-verify:" + uuid.NewString()
	if operation != "" {
		stepOperation = operation + ":verify"
	}
	batchLimit, err := recovery.ResolveVerificationBatchLimit(getenv)
	if err != nil {
		return "not_configured", nil, err.Error(), ""
	}
	for _, category := range recovery.KnownVerificationCategories() {
		if _, err := recovery.ResolveVerificationFreshness(getenv, category); err != nil {
			return "refused", nil, err.Error(), ""
		}
	}
	deps, err := recoveryVerifyOpenDependencies(ctx, env, getenv, rpcURL, chainID, dataFingerprint)
	if err != nil {
		return "refused", nil, err.Error(), ""
	}
	defer deps.close()
	verification, err := recovery.NewVerification(env.store, recovery.VerificationOptions{
		BatchLimit: batchLimit,
		Freshness: func(category recovery.VerificationCategory) (time.Duration, error) {
			return recovery.ResolveVerificationFreshness(getenv, category)
		},
	})
	if err != nil {
		return "refused", nil, err.Error(), ""
	}
	start := time.Now()
	batch, err := verification.Verify(ctx, recovery.VerificationRequest{
		InstanceID:  instanceID,
		Actor:       env.principal,
		Scope:       []byte(`{"scope":"all"}`),
		OperationID: stepOperation,
		Sources:     deps.sources,
	})
	if err != nil {
		return "refused", nil, err.Error(), ""
	}
	if batch.Discarded {
		return "discarded", nil, "a concurrent evidence write committed first; nothing from this verification step was persisted", ""
	}
	elapsed := time.Since(start).Seconds()
	return "executed", &elapsed, "", batch.BatchID
}

// recoveryDrillGapCounts reads the instance's gap facts (counts by state and
// the affected-capability distribution). Gap reads change nothing.
func recoveryDrillGapCounts(ctx context.Context, env *recoveryOpEnv, instanceID string) (map[string]any, int, error) {
	gaps, err := recovery.NewGaps(env.store)
	if err != nil {
		return nil, 0, err
	}
	records, err := gaps.List(ctx, instanceID)
	if err != nil {
		return nil, 0, err
	}
	byState := map[string]int{"open": 0, "closed": 0, "escalated": 0}
	byCapability := map[string]int{}
	for _, gap := range records {
		byState[string(gap.State)]++
		if gap.State == recovery.GapStateOpen || gap.State == recovery.GapStateEscalated {
			for _, capability := range gap.AffectedCapabilities {
				byCapability[string(capability)]++
			}
		}
	}
	counts := map[string]any{
		"open":                  byState["open"],
		"closed":                byState["closed"],
		"escalated":             byState["escalated"],
		"affected_capabilities": byCapability,
	}
	return counts, byState["open"] + byState["escalated"], nil
}

// recoveryDrillObserveReleases evaluates the derived gate per capability at
// the drill's canonical scope template chain=<chain>;capability=<capability>
// and returns the measured safe-resumption times (for capabilities with a
// current release, relative to the real restore start) plus the observation
// and refusal breakdown. It creates no approval and no release.
func recoveryDrillObserveReleases(ctx context.Context, env *recoveryOpEnv, instanceID string,
	chains []uint64, gateTTL time.Duration, restoreStarted time.Time) (
	map[recovery.Capability]float64, map[string]string, map[string]string, error) {
	gate, err := recovery.NewGate(env.store, recovery.GateOptions{
		TTL: gateTTL,
		TrustedTarget: recovery.GateTargetBinding{
			TargetGuardKey:        env.targetGuardKey,
			TargetRoleFingerprint: env.targetRoleFingerprint,
		},
	})
	if err != nil {
		return nil, nil, nil, err
	}
	seconds := map[recovery.Capability]float64{}
	observations := map[string]string{}
	refusals := map[string]string{}
	var firstErr error
	for _, chainID := range chains {
		for _, capability := range recovery.KnownCapabilities() {
			scope := "chain=" + strconv.FormatUint(chainID, 10) + ";capability=" + string(capability)
			scopeHash, err := recovery.CanonicalScopeHash(scope)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			decision, err := gate.Admit(ctx, recovery.GateRequest{
				InstanceID: instanceID,
				Capability: capability,
				ScopeHash:  scopeHash,
				Action:     "drill_observe",
			})
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if !decision.Allowed {
				class := string(decision.RefusalClass)
				if class == "" {
					class = "no_release"
				}
				refusals[scope] = class
				observations[scope] = "not_released"
				continue
			}
			observations[scope] = "not_measured"
			if decision.DecisionRef == "" {
				continue
			}
			var releasedAt time.Time
			if err := env.pool.QueryRow(ctx,
				`SELECT created_at FROM recovery_release WHERE release_id = $1`, decision.DecisionRef).Scan(&releasedAt); err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("read release decision %s: %w", decision.DecisionRef, err)
				}
				continue
			}
			if restoreStarted.IsZero() || releasedAt.Before(restoreStarted) {
				// A release that predates the restore start is not bound to the
				// current evidence generation; it is never a measured safe
				// resumption of this run.
				continue
			}
			observations[scope] = "released"
			elapsed := releasedAt.Sub(restoreStarted).Seconds()
			if prior, ok := seconds[capability]; !ok || elapsed > prior {
				seconds[capability] = elapsed
			}
		}
	}
	return seconds, observations, refusals, firstErr
}

// recoveryDrillInjectionObserved accepts only a scenario-specific causal
// signal. Generic restore/verification refusals and any unrelated release
// blocker are deliberately insufficient.
func recoveryDrillInjectionObserved(injection, restoreState string, restoreBlocked []string,
	verificationState string, openGaps int, releaseRefusals map[string]string) bool {
	contains := func(needles ...string) bool {
		for _, text := range restoreBlocked {
			lower := strings.ToLower(text)
			for _, needle := range needles {
				if strings.Contains(lower, needle) {
					return true
				}
			}
		}
		return false
	}
	refusalHas := func(needles ...string) bool {
		for _, class := range releaseRefusals {
			lower := strings.ToLower(class)
			for _, needle := range needles {
				if strings.Contains(lower, needle) {
					return true
				}
			}
		}
		return false
	}
	switch injection {
	case "F1":
		return restoreState == "refused" && contains("manifest is not usable", "backup is not verified", "backup artifact is unavailable", "backup artifact is corrupt")
	case "F2":
		return restoreState == "refused" && contains("restore interrupted", "restore was interrupted", "partial restore")
	case "F3":
		return restoreState == "refused" && contains("version incompatible", "schema incompatible", "unsupported server version")
	case "F4":
		return verificationState == "refused" && contains("external fact ahead", "fact is ahead", "divergent external")
	case "F5":
		return refusalHas("isolation_unproven", "old_instance_not_isolated")
	case "F6":
		return openGaps > 0 && refusalHas("gap")
	case "F7":
		return refusalHas("approval", "authorization", "stale_approval", "insufficient_authority")
	default:
		return false
	}
}

// recoveryDrillSafeResumption derives the end-to-end safe-resumption duration
// from the measured per-capability observations: only a complete seven
// capability set is an RTO-grade measurement.
func recoveryDrillSafeResumption(releaseSeconds map[recovery.Capability]float64) (float64, bool) {
	if len(releaseSeconds) == 0 {
		return 0, false
	}
	var max float64
	for _, capability := range recovery.KnownCapabilities() {
		value, ok := releaseSeconds[capability]
		if !ok {
			return 0, false
		}
		if value > max {
			max = value
		}
	}
	return max, true
}

// recoveryDrillLagDoc renders one local-input lag payload: an explicit
// unknown state by default, an annotated operator-provided test input when a
// value was passed. It is never a production RPO measurement.
func recoveryDrillLagDoc(value float64, name string) map[string]any {
	if value == -1 {
		return map[string]any{
			"state":  "unknown",
			"reason": "no " + name + " measurement source is connected in this drill invocation; explicit unknown, never a fabricated zero",
		}
	}
	return map[string]any{
		"state":   "provided",
		"seconds": value,
		"purpose": "operator-provided local test input only; not a production RPO measurement",
	}
}

// recoveryDrillReleaseDoc renders the per-capability release payload for the
// archive with every known capability explicit.
func recoveryDrillReleaseDoc(releaseSeconds map[recovery.Capability]float64) map[string]any {
	payload := make(map[string]any, len(recovery.KnownCapabilities()))
	for _, capability := range recovery.KnownCapabilities() {
		if seconds, ok := releaseSeconds[capability]; ok {
			payload[string(capability)] = seconds
		} else {
			payload[string(capability)] = nil
		}
	}
	return payload
}

// ---------------------------------------------------------------------------
// Archive and output
// ---------------------------------------------------------------------------

// recoveryDrillArchive is the archived drill artifact shape (JSON). It carries
// the S12 sections separately and the non-claims verbatim.
type recoveryDrillArchive struct {
	Purpose                  string                          `json:"purpose"`
	DrillID                  string                          `json:"drill_id"`
	InstanceID               string                          `json:"instance_id"`
	Scenario                 string                          `json:"scenario"`
	FailureInjection         string                          `json:"failure_injection,omitempty"`
	Result                   string                          `json:"result"`
	RecordOnly               bool                            `json:"record_only"`
	RestoreState             string                          `json:"restore_state"`
	RestoreBlocked           []string                        `json:"restore_blocked,omitempty"`
	RestoreRefusal           string                          `json:"restore_refusal,omitempty"`
	ReleaseObservationError  string                          `json:"release_observation_error,omitempty"`
	VerificationState        string                          `json:"verification_state"`
	VerificationRef          string                          `json:"verification_ref,omitempty"`
	VerificationRefusal      string                          `json:"verification_refusal,omitempty"`
	BackupID                 string                          `json:"backup_id"`
	ManifestVersion          string                          `json:"manifest_version"`
	RecoveryPoint            json.RawMessage                 `json:"recovery_point"`
	DBRestoreSeconds         *float64                        `json:"db_restore_seconds"`
	VerificationSeconds      *float64                        `json:"verification_seconds"`
	CapabilityReleaseSeconds map[string]any                  `json:"capability_release_seconds"`
	ReleaseObservations      map[string]string               `json:"release_observations"`
	ReleaseRefusals          map[string]string               `json:"release_refusals"`
	BackupLag                map[string]any                  `json:"backup_lag"`
	UncoveredInterval        map[string]any                  `json:"uncovered_interval"`
	ConstraintsConfigured    bool                            `json:"constraints_configured"`
	Constraints              []recoveryDrillConstraintStatus `json:"constraints"`
	UnconfiguredConstraints  []string                        `json:"unconfigured_constraints"`
	RTO                      metrics.RTOEvaluation           `json:"rto"`
	RTOTargetConfigured      bool                            `json:"rto_target_configured"`
	GateTTL                  string                          `json:"gate_ttl"`
	GapCounts                map[string]any                  `json:"gap_counts"`
	LocalValuesOnly          bool                            `json:"local_values_only"`
	NonClaims                []string                        `json:"non_claims"`
}

// TestInputs renders the mandatory T056 test_inputs annotation: purpose,
// per-constraint state with purpose, the explicit unconfigured list and the
// RTO evaluation. A run with unconfigured constraints always names them.
func (a recoveryDrillArchive) TestInputs() ([]byte, error) {
	payload := map[string]any{
		"purpose":                   a.Purpose,
		"local_values_only":         a.LocalValuesOnly,
		"constraints":               a.Constraints,
		"unconfigured_constraints":  a.UnconfiguredConstraints,
		"rto":                       a.RTO,
		"rto_target_configured":     a.RTOTargetConfigured,
		"restore_state":             a.RestoreState,
		"verification_state":        a.VerificationState,
		"release_observations":      a.ReleaseObservations,
		"release_refusals":          a.ReleaseRefusals,
		"release_observation_error": a.ReleaseObservationError,
		"gate_ttl":                  a.GateTTL,
		"record_only":               a.RecordOnly,
	}
	return json.Marshal(payload)
}

// BackupLagJSON renders the backup-lag payload for the recorded row.
func (a recoveryDrillArchive) BackupLagJSON() []byte { return mustJSONBytes(a.BackupLag) }

// UncoveredIntervalJSON renders the uncovered-interval payload.
func (a recoveryDrillArchive) UncoveredIntervalJSON() []byte {
	return mustJSONBytes(a.UncoveredInterval)
}

// GapCountsJSON renders the gap payload.
func (a recoveryDrillArchive) GapCountsJSON() []byte { return mustJSONBytes(a.GapCounts) }

// mustJSONBytes renders a payload (the archive sections are always
// JSON-encodable maps; a marshal failure would be a programming error, and
// the empty result then fails T056 validation instead of writing junk).
func mustJSONBytes(payload map[string]any) []byte {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	return encoded
}

// recoveryDrillWriteArchive writes the archive with the S12 sections.
func recoveryDrillWriteArchive(path string, archive *recoveryDrillArchive) error {
	encoded, err := json.MarshalIndent(archive, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(encoded, '\n'), 0o644)
}

// recoveryDrillReplay reports a recorded drill outcome for an already-seen
// operation; the same input replays with zero side effects, a different
// input conflicts with zero writes.
func recoveryDrillReplay(stderr, stdout io.Writer, recorded *recoveryOpRecorded, inputDigest, operation string) (bool, int) {
	if recorded == nil {
		return false, 0
	}
	var detail map[string]any
	if err := json.Unmarshal(recorded.Detail, &detail); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: operation_id=%s has an unreadable audit record; refusing ambiguous replay\n", operation)
		return true, 1
	}
	if digest, _ := detail["input_digest"].(string); digest != inputDigest {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: operation_conflict operation_id=%s (the operation id was recorded with a different input; zero writes)\n", operation)
		return true, 1
	}
	if stage, _ := detail["stage"].(string); stage == "started" {
		fmt.Fprintf(stderr, "txharbor recovery-admin drill: operation_id=%s has a started but non-terminal restore marker; refusing ambiguous retry\n", operation)
		return true, 1
	}
	if recorded.Result == controlstore.AuditOK {
		result, _ := detail["result"].(string)
		fmt.Fprintf(stdout,
			"txharbor recovery-admin drill: replayed=true operation_id=%s result=%s drill_id=%v result_class=%v archive=%v (read back from the recorded outcome; zero side effects)\n",
			operation, recorded.Result, detail["drill_id"], result, detail["archive"])
		if result == string(recovery.DrillResultRefusedSafe) {
			return true, 1
		}
		return true, 0
	}
	fmt.Fprintf(stderr,
		"txharbor recovery-admin drill: replayed=true operation_id=%s result=%s (the recorded refusal replays; zero side effects)\n",
		operation, recorded.Result)
	return true, 1
}

// recoveryDrillPrint renders the recorded run: the S12 sections separately,
// the explicit constraint states and the non-claims.
func recoveryDrillPrint(stdout, stderr io.Writer, run recovery.DrillRun, archive recoveryDrillArchive,
	releaseSeconds map[recovery.Capability]float64, rtoEval metrics.RTOEvaluation, archivePath string, releaseErr error) {
	fmt.Fprintf(stdout,
		"txharbor recovery-admin drill: drill_id=%s instance=%s scenario=%s failure_injection=%s result=%s record_only=%t archive=%s\n",
		run.DrillID, run.InstanceID, run.Scenario, recoveryDrillOr(archive.FailureInjection, "none"),
		run.Result, archive.RecordOnly, archivePath)
	fmt.Fprintf(stdout, "  recovery_point=%s backup_id=%s manifest_version=%s\n",
		recoveryDrillRecoveryPoint(archive.RecoveryPoint), archive.BackupID, archive.ManifestVersion)
	fmt.Fprintf(stdout, "  db_restore_seconds=%s restore_state=%s\n",
		recoveryDrillSecondsDisplay(archive.DBRestoreSeconds), archive.RestoreState)
	for _, blocked := range archive.RestoreBlocked {
		fmt.Fprintf(stdout, "    restore_missing_precondition=%s\n", logx.Redact(blocked))
	}
	if archive.RestoreRefusal != "" {
		fmt.Fprintf(stderr, "  restore_refusal=%s\n", archive.RestoreRefusal)
	}
	fmt.Fprintf(stdout, "  verification_seconds=%s verification_state=%s%s\n",
		recoveryDrillSecondsDisplay(archive.VerificationSeconds), archive.VerificationState,
		recoveryDrillRefusalSuffix(archive.VerificationRefusal))
	for _, capability := range recovery.KnownCapabilities() {
		value := "not_measured"
		if seconds, ok := releaseSeconds[capability]; ok {
			value = strconv.FormatFloat(seconds, 'f', 3, 64)
		}
		fmt.Fprintf(stdout, "  capability_release_seconds capability=%s seconds=%s state=%s refusal_class=%s\n",
			capability, value,
			recoveryDrillOr(archive.ReleaseObservations[string(capability)], "not_observed"),
			recoveryDrillOr(archive.ReleaseRefusals[string(capability)], "none"))
	}
	fmt.Fprintf(stdout, "  backup_lag state=%v seconds=%v uncovered_interval state=%v seconds=%v\n",
		archive.BackupLag["state"], recoveryDrillOr(fmt.Sprint(archive.BackupLag["seconds"]), "not_measured"),
		archive.UncoveredInterval["state"], recoveryDrillOr(fmt.Sprint(archive.UncoveredInterval["seconds"]), "not_measured"))
	fmt.Fprintf(stdout, "  gaps open=%v closed=%v escalated=%v affected_capabilities=%v\n",
		archive.GapCounts["open"], archive.GapCounts["closed"], archive.GapCounts["escalated"],
		archive.GapCounts["affected_capabilities"])
	for _, constraint := range archive.Constraints {
		fmt.Fprintf(stdout, "  constraint=%s state=%s value=%s purpose=%s\n",
			constraint.Key, constraint.State, recoveryDrillOr(constraint.Value, "none"), constraint.Purpose)
	}
	fmt.Fprintf(stdout, "  constraints_configured=%t unconfigured_constraints=%s\n",
		archive.ConstraintsConfigured, recoveryDrillOr(strings.Join(archive.UnconfiguredConstraints, ","), "none"))
	fmt.Fprintf(stdout, "  rto status=%s target_configured=%t target_seconds=%s measured_seconds=%s not_met=%t alert=%t escalation_required=%t permanent_resumption_block=%t\n",
		rtoEval.Status, archive.RTOTargetConfigured, recoveryDrillDurationDisplay(rtoEval.Target),
		recoveryDrillSecondsDisplay(optionalSeconds(rtoEval.MeasuredKnown, rtoEval.Measured.Seconds())),
		rtoEval.NotMet, rtoEval.Alert, rtoEval.EscalationRequired, rtoEval.PermanentResumptionBlock)
	fmt.Fprintln(stdout, "  non_claims: local drill inputs only (not production thresholds; T000-P OPEN); no cross-system exactly-once claim; no external ledger consistency claim; RTO is never claimed from a reachable database or a single timing scope.")
	if releaseErr != nil {
		fmt.Fprintf(stderr, "  release_observation_warning=%s\n", logx.Redact(releaseErr.Error()))
	}
}

// recoveryDrillSecondsDisplay renders an optional measurement ("not_measured"
// is a state, never a zero).
func recoveryDrillSecondsDisplay(seconds *float64) string {
	if seconds == nil {
		return "not_measured"
	}
	return strconv.FormatFloat(*seconds, 'f', 3, 64)
}

// recoveryDrillDurationDisplay renders a duration ("not_configured" when 0).
func recoveryDrillDurationDisplay(value time.Duration) string {
	if value <= 0 {
		return "not_configured"
	}
	return value.String()
}

// recoveryDrillRefusalSuffix renders the verification refusal suffix.
func recoveryDrillRefusalSuffix(reason string) string {
	if strings.TrimSpace(reason) == "" {
		return ""
	}
	return " (reason=" + reason + ")"
}

// recoveryDrillRecoveryPoint renders the recovery point JSON compactly.
func recoveryDrillRecoveryPoint(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return "unknown"
	}
	return string(raw)
}

// recoveryDrillOr returns value or fallback when blank.
func recoveryDrillOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

// optionalSeconds returns a pointer to value when ok, else nil (never a
// fabricated zero measurement).
func optionalSeconds(ok bool, value float64) *float64 {
	if !ok {
		return nil
	}
	return &value
}
