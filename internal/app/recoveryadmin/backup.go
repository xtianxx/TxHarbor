// backup.go implements T021: the `recovery-admin backup` and
// `recovery-admin verify-backup` operator wiring.
//
// Trust boundary and operating discipline:
//
//  1. The authenticated subject is bound from TXHARBOR_RECOVERY_PRINCIPAL in
//     the canonical <kind>:<id> form and never from a flag; free text can
//     never authorize. The principal must be provable: an active identity
//     mapping is required, and when a recovery instance context exists
//     (TXHARBOR_RECOVERY_INSTANCE, --instance or the single open instance)
//     the principal must hold the role binding the command needs
//     (executor for backup/restore, verifier for verify-backup).
//  2. TXHARBOR_RECOVERY_ARTIFACT_DIR is required by name unless --out
//     overrides it; a missing value is refused, never defaulted to the
//     current directory.
//  3. TXHARBOR_RECOVERY_CONTROL_DSN and TXHARBOR_PG_DSN are both required and
//     must address different databases (the control store is outside the data
//     backup/restore set); only the control DSN is opened here for
//     governance checks, the data DSN belongs to the backup executor.
//  4. --operation-id is the persistent idempotency key: with it, an
//     already-recorded operation replays with zero side effects (a recorded
//     refusal replays as a refusal, a different input conflicts); without it
//     the invocation is still audited under a derived id but is not
//     replayable.
//  5. Exit codes: 0 success, 1 refusal/config/verified=false, 2 usage.
//     Plaintext DSNs never reach stdout/stderr: messages pass through
//     logx.Redact and target identity is reported as fingerprints.
package recoveryadmin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// recoveryOpConnectTimeout bounds the control-store connect of the operation
// commands (a local command bound, not a production threshold).
const recoveryOpConnectTimeout = 5 * time.Second

// The audit action tokens of the wired operator commands.
const (
	recoveryActionBackup       = "backup"
	recoveryActionVerifyBackup = "verify_backup"
	recoveryActionRestore      = "restore"
)

// recoveryAdminActionByNameOrZero returns the action or the zero value (usage
// rendering only).
func recoveryAdminActionByNameOrZero(name string) recoveryAdminAction {
	action, _ := recoveryAdminActionByName(name)
	return action
}

// getenvValue reads one deployment-controlled environment key.
func (d Deps) getenvValue(key string) (string, bool) {
	if d.Getenv == nil {
		return "", false
	}
	return d.Getenv(key)
}

// recoveryOpEnv is the opened operation context: the authenticated principal,
// the control-store pool/store and the parsed target identities. The data DSN
// is carried for the executors but never echoed.
type recoveryOpEnv struct {
	principal             string
	dataDSN               string
	controlDSN            string
	controlFingerprint    string
	targetGuardKey        string
	targetRoleFingerprint string
	dataTargetFingerprint []byte
	pool                  *pgxpool.Pool
	store                 *controlstore.Store
}

// recoveryOpConfig validates the by-name configuration common to the wired
// commands before any connection is made.
func recoveryOpConfig(d Deps, command string) (getenv func(string) (string, bool), principal, controlDSN, dataDSN string, code int) {
	stderr := d.stderr()
	getenv = d.getenv()
	if getenv == nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: environment lookup is not wired; refusing\n", command)
		return nil, "", "", "", 1
	}
	controlDSN, ok := getenv(config.EnvRecoveryControlDSN)
	if !ok || strings.TrimSpace(controlDSN) == "" {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: %s is required (not configured); refusing\n",
			command, config.EnvRecoveryControlDSN)
		return nil, "", "", "", 1
	}
	dataDSN, ok = getenv(config.EnvPGDSN)
	if !ok || strings.TrimSpace(dataDSN) == "" {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: %s is required to prove the control store is an independent database (not configured); refusing\n",
			command, config.EnvPGDSN)
		return nil, "", "", "", 1
	}
	principalRaw, ok := getenv(config.EnvRecoveryPrincipal)
	if !ok || strings.TrimSpace(principalRaw) == "" {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: %s is required (authenticated subject; default deny)\n",
			command, config.EnvRecoveryPrincipal)
		return nil, "", "", "", 1
	}
	if principalRaw != strings.TrimSpace(principalRaw) {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: %s must not carry surrounding whitespace\n",
			command, config.EnvRecoveryPrincipal)
		return nil, "", "", "", 1
	}
	principal, err := controlstore.NormalizePrincipal(principalRaw)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: %s is invalid: %s\n",
			command, config.EnvRecoveryPrincipal, logx.Redact(err.Error()))
		return nil, "", "", "", 1
	}
	controlTarget, err := controlstore.ParseDSNTarget(controlDSN)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: %s is invalid: %s\n",
			command, config.EnvRecoveryControlDSN, logx.Redact(err.Error()))
		return nil, "", "", "", 1
	}
	dataTarget, err := controlstore.ParseDSNTarget(dataDSN)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: %s is invalid: %s\n",
			command, config.EnvPGDSN, logx.Redact(err.Error()))
		return nil, "", "", "", 1
	}
	if controlTarget.SameDatabase(dataTarget) {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: %s and %s address the same database target; the control store must be independent (refusing)\n",
			command, config.EnvRecoveryControlDSN, config.EnvPGDSN)
		return nil, "", "", "", 1
	}
	return getenv, principal, controlDSN, dataDSN, 0
}

// recoveryOpOpen opens the version-guarded control store for the wired
// commands. Every failure is fail-closed and reported by exact configuration
// key name, never by DSN value.
func recoveryOpOpen(ctx context.Context, d Deps, command string) (*recoveryOpEnv, int) {
	getenv, principal, controlDSN, dataDSN, code := recoveryOpConfig(d, command)
	if getenv == nil {
		return nil, code
	}
	controlTarget, err := controlstore.ParseDSNTarget(controlDSN)
	if err != nil {
		fmt.Fprintf(d.stderr(), "txharbor recovery-admin %s: %s is invalid: %s\n",
			command, config.EnvRecoveryControlDSN, logx.Redact(err.Error()))
		return nil, 1
	}
	dataTarget, err := controlstore.ParseDSNTarget(dataDSN)
	needsTargetBinding := command == "instance-open" || command == "instance-close"
	if err != nil || (needsTargetBinding && (strings.TrimSpace(dataTarget.Host) == "" || dataTarget.Host != strings.TrimSpace(dataTarget.Host) || strings.Contains(dataTarget.Host, ",") || dataTarget.Port == 0 || strings.TrimSpace(dataTarget.Database) == "" || strings.TrimSpace(dataTarget.Role) == "")) {
		fmt.Fprintf(d.stderr(), "txharbor recovery-admin %s: %s has an unknown or ambiguous host/port/database/role identity; refusing\n",
			command, config.EnvPGDSN)
		return nil, 1
	}
	targetGuardKey, err := controlstore.TargetGuardKey(dataTarget)
	if err != nil {
		fmt.Fprintf(d.stderr(), "txharbor recovery-admin %s: %s target identity is invalid: %s\n",
			command, config.EnvPGDSN, logx.Redact(err.Error()))
		return nil, 1
	}
	dataTargetFingerprint, err := json.Marshal(dataTarget.DataTargetFingerprint())
	if err != nil {
		fmt.Fprintf(d.stderr(), "txharbor recovery-admin %s: could not encode configured target fingerprint; refusing\n", command)
		return nil, 1
	}
	pool, err := db.OpenPool(ctx, controlDSN, recoveryOpConnectTimeout)
	if err != nil {
		fmt.Fprintf(d.stderr(), "txharbor recovery-admin %s: control store unavailable: %s\n",
			command, logx.Redact(err.Error()))
		return nil, 1
	}
	store, err := controlstore.NewStore(ctx, pool)
	if err != nil {
		pool.Close()
		fmt.Fprintf(d.stderr(), "txharbor recovery-admin %s: control store unavailable: %s\n",
			command, logx.Redact(err.Error()))
		return nil, 1
	}
	return &recoveryOpEnv{
		principal:             principal,
		dataDSN:               dataDSN,
		controlDSN:            controlDSN,
		controlFingerprint:    controlTarget.DataTargetFingerprint().TargetFingerprint,
		targetGuardKey:        targetGuardKey,
		targetRoleFingerprint: dataTarget.DataTargetFingerprint().RoleFingerprint,
		dataTargetFingerprint: dataTargetFingerprint,
		pool:                  pool,
		store:                 store,
	}, 0
}

// recoveryOpConvergenceStep builds the R2 deployment-lane convergence step
// from TXHARBOR_RECOVERY_DEPLOYMENT_ADMIN_DSN. Missing configuration returns
// nil: a bound restore then refuses at the coordinator's acceptance gate
// (fail-closed) instead of silently skipping convergence. The original writer
// role is the trusted authoritative data target's role (env.dataDSN), never a
// caller-supplied value.
func recoveryOpConvergenceStep(d Deps, env *recoveryOpEnv, targetDSN string) recovery.ConvergenceStep {
	admin, ok := d.getenvValue(config.EnvRecoveryDeploymentAdminDSN)
	if !ok || strings.TrimSpace(admin) == "" {
		return nil
	}
	authoritative, err := controlstore.ParseDSNTarget(env.dataDSN)
	if err != nil || authoritative.Role == "" {
		return nil
	}
	step := recovery.DeploymentConvergence{
		AdminDSN:     strings.TrimSpace(admin),
		TargetDSN:    targetDSN,
		OriginalRole: authoritative.Role,
		Actor:        env.principal,
		Store:        env.store,
	}
	return step.Converge
}

// recoveryOpArtifactDir resolves the artifact directory: --out wins, then
// TXHARBOR_RECOVERY_ARTIFACT_DIR; a missing value is refused by key name.
func recoveryOpArtifactDir(d Deps, out, command string) (string, int) {
	if value := strings.TrimSpace(out); value != "" {
		return value, 0
	}
	getenv := d.getenv()
	if getenv == nil {
		fmt.Fprintf(d.stderr(), "txharbor recovery-admin %s: environment lookup is not wired; refusing\n", command)
		return "", 1
	}
	value, ok := getenv(config.EnvRecoveryArtifactDir)
	if !ok || strings.TrimSpace(value) == "" {
		fmt.Fprintf(d.stderr(), "txharbor recovery-admin %s: %s is required (not configured); refusing\n",
			command, config.EnvRecoveryArtifactDir)
		return "", 1
	}
	return strings.TrimSpace(value), 0
}

// recoveryOpSafeText keeps credential-shaped material out of operator output
// and persisted operation audit fields. It is intentionally redaction, not a
// general-purpose secret detector; semantic inputs continue to use the
// original value.
func recoveryOpSafeText(value string) string {
	return logx.Redact(value)
}

// recoveryOpInstanceContext resolves the instance the command is bound to:
// flag first (when given), then the deployment-controlled
// TXHARBOR_RECOVERY_INSTANCE. Missing context returns ("", false, nil) for the
// commands that support a manifest-level run.
func recoveryOpInstanceContext(d Deps, flagValue, command string) (string, bool, int) {
	if value := strings.TrimSpace(flagValue); value != "" {
		if env, ok := d.getenvValue(config.EnvRecoveryInstance); ok && strings.TrimSpace(env) != "" &&
			strings.TrimSpace(env) != value {
			fmt.Fprintf(d.stderr(), "txharbor recovery-admin %s: --instance %s does not match %s=%s (instance bound execution; refusing)\n",
				command, value, config.EnvRecoveryInstance, strings.TrimSpace(env))
			return "", false, 1
		}
		return value, true, 0
	}
	if env, ok := d.getenvValue(config.EnvRecoveryInstance); ok && strings.TrimSpace(env) != "" {
		return strings.TrimSpace(env), true, 0
	}
	return "", false, 0
}

// recoveryOpOpenInstance finds the single open recovery instance, if any.
func recoveryOpOpenInstance(ctx context.Context, env *recoveryOpEnv) (string, bool, error) {
	var instanceID string
	err := env.pool.QueryRow(ctx,
		`SELECT instance_id::text FROM recovery_instance WHERE state = 'open' ORDER BY opened_at DESC, instance_id DESC LIMIT 1`).
		Scan(&instanceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("look up the open recovery instance: %w", err)
	}
	return instanceID, true, nil
}

// recoveryOpOpenTargetBindings reads the complete target-binding inventory
// for every open instance. Null legacy bindings are retained as empty values
// so the opaque isolated-target constructor can fail closed.
func recoveryOpOpenTargetBindings(ctx context.Context, pool *pgxpool.Pool) ([]recovery.IsolatedInstanceBinding, error) {
	rows, err := pool.Query(ctx, `SELECT target_guard_key, target_role_fingerprint
FROM recovery_instance WHERE state = 'open'`)
	if err != nil {
		return nil, fmt.Errorf("list open recovery instance target bindings: %w", err)
	}
	defer rows.Close()
	var bindings []recovery.IsolatedInstanceBinding
	for rows.Next() {
		var key, role pgtype.Text
		if err := rows.Scan(&key, &role); err != nil {
			return nil, fmt.Errorf("scan open recovery instance target binding: %w", err)
		}
		binding := recovery.IsolatedInstanceBinding{}
		if key.Valid {
			binding.TargetGuardKey = key.String
		}
		if role.Valid {
			binding.TargetRoleFingerprint = role.String
		}
		bindings = append(bindings, binding)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list open recovery instance target bindings: %w", err)
	}
	return bindings, nil
}

// recoveryOpRequireParticipant enforces the T021 subject rule: the
// authenticated principal must have an active identity mapping (a person can
// be proven) and, when an instance context exists, must hold the required
// participant role on that open instance. Free-text annotations can never
// authorize.
func recoveryOpRequireParticipant(ctx context.Context, env *recoveryOpEnv, instanceID, role, command string) error {
	_, ok, err := env.store.ActivePersonID(ctx, env.principal)
	if err != nil {
		return fmt.Errorf("resolve the principal identity mapping: %w", err)
	}
	if !ok {
		return fmt.Errorf("%s %s has no active identity mapping; register the mapping first (cannot prove a person)",
			command, env.principal)
	}
	if strings.TrimSpace(instanceID) == "" {
		return nil
	}
	bindings, err := env.store.ParticipantBindings(ctx, instanceID)
	if err != nil {
		return fmt.Errorf("read participant bindings: %w", err)
	}
	for _, binding := range bindings {
		if binding.Principal == env.principal && binding.Role == role {
			return nil
		}
	}
	return fmt.Errorf("%s %s is not registered as %s on instance %s; register the participant first",
		command, env.principal, role, instanceID)
}

// recoveryAdminBackup implements `recovery-admin backup`.
func recoveryAdminBackup(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("txharbor recovery-admin backup", flag.ContinueOnError)
	fs.SetOutput(stderr)
	chainID := fs.String("chain-id", "", "scope chain identity (required)")
	out := fs.String("out", "", "artifact output directory (defaults to "+config.EnvRecoveryArtifactDir+")")
	operationID := fs.String("operation-id", "", "idempotent operation identity (optional)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*chainID) == "" {
		recoveryAdminActionUsage(stderr, recoveryAdminActionByNameOrZero("backup"))
		return 2
	}
	operation, err := recoveryOpOperationID(*operationID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin backup: %s\n", logx.Redact(err.Error()))
		return 2
	}

	artifactDir, code := recoveryOpArtifactDir(d, *out, "backup")
	if code != 0 {
		return code
	}
	instanceID, bound, code := recoveryOpInstanceContext(d, "", "backup")
	if code != 0 {
		return code
	}

	env, code := recoveryOpOpen(ctx, d, "backup")
	if env == nil {
		return code
	}
	defer env.pool.Close()

	if !bound {
		openID, ok, err := recoveryOpOpenInstance(ctx, env)
		if err != nil {
			fmt.Fprintf(stderr, "txharbor recovery-admin backup: %s\n", logx.Redact(err.Error()))
			return 1
		}
		if ok {
			instanceID, bound = openID, true
		}
	}
	if err := recoveryOpRequireParticipant(ctx, env, instanceID, "executor", "backup"); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin backup: refused: %s\n", logx.Redact(err.Error()))
		return 1
	}

	inputDigest := recoveryOpInputDigest(recoveryActionBackup, map[string]string{
		"chain_id":     strings.TrimSpace(*chainID),
		"artifact_dir": artifactDir,
		"data_target":  recoveryTargetFingerprint(env.dataDSN),
		"instance_id":  instanceID,
	})
	release, recorded, err := recoveryOpBegin(ctx, env.pool, recoveryActionBackup, operation)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin backup: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if release != nil {
		defer release()
	}
	if replayed, code := recoveryOpReplay(stderr, stdout, recorded, inputDigest, operation); replayed {
		return code
	}

	programVersion, err := recovery.CurrentProgramVersion()
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin backup: %s\n", logx.Redact(err.Error()))
		return 1
	}
	result, err := recovery.ExecuteBackup(ctx, recovery.BackupOptions{
		DSN:                  env.dataDSN,
		ArtifactDir:          artifactDir,
		ChainID:              strings.TrimSpace(*chainID),
		CreatedBy:            env.principal,
		ProgramVersion:       programVersion,
		ProgramMinCompatible: programVersion,
		PG:                   recovery.LocalPGCommand{},
	})
	if err != nil {
		_ = recoveryOpRecord(ctx, env.pool, recoveryActionBackup, operation, env.principal, controlstore.AuditRefused,
			map[string]any{"input_digest": inputDigest, "reason": recoveryOpSafeText(err.Error())},
			map[string]any{"chain_id": strings.TrimSpace(*chainID), "artifact_dir": recoveryOpSafeText(artifactDir)})
		fmt.Fprintf(stderr, "txharbor recovery-admin backup: refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	_ = recoveryOpRecord(ctx, env.pool, recoveryActionBackup, operation, env.principal, controlstore.AuditOK,
		map[string]any{
			"input_digest": inputDigest,
			"verification": string(result.Manifest.Verification.State),
			"bytes":        result.Manifest.Artifacts[0].Bytes,
			"sha256":       result.Manifest.Artifacts[0].SHA256,
		},
		map[string]any{
			"backup_id":     result.Manifest.BackupID,
			"manifest_path": recoveryOpSafeText(result.ManifestPath),
			"chain_id":      strings.TrimSpace(*chainID),
			"instance_id":   instanceID,
		})
	fmt.Fprintf(stdout,
		"txharbor recovery-admin backup: backup_id=%s manifest=%s artifact=%s verification=%s operation_id=%s principal=%s instance=%s\n",
		result.Manifest.BackupID, recoveryOpSafeText(result.ManifestPath), recoveryOpSafeText(result.ArtifactPath),
		result.Manifest.Verification.State, recoveryOpDisplayID(operation), env.principal, instanceID)
	return 0
}

// recoveryAdminVerifyBackup implements `recovery-admin verify-backup`.
func recoveryAdminVerifyBackup(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("txharbor recovery-admin verify-backup", flag.ContinueOnError)
	fs.SetOutput(stderr)
	manifestPath := fs.String("manifest", "", "manifest path (required)")
	targetDSN := fs.String("target-dsn", "", "optional assertion of the deployment-configured isolated target (or "+recoveryTargetDSNEnv+"; never used as connection target)")
	instanceFlag := fs.String("instance", "", "recovery instance id (optional; binds the conclusion)")
	operationID := fs.String("operation-id", "", "idempotent operation identity (optional)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	targetFlagSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "target-dsn" {
			targetFlagSet = true
		}
	})
	resolvedTarget, inputErr := recoveryProtectedDSNInput(*targetDSN, targetFlagSet, d.getenv(), recoveryTargetDSNEnv, "--target-dsn")
	if inputErr != nil {
		fmt.Fprintln(stderr, "txharbor recovery-admin verify-backup: invalid DSN source selection; refusing")
		return 2
	}
	*targetDSN = resolvedTarget
	if fs.NArg() > 0 || strings.TrimSpace(*manifestPath) == "" {
		recoveryAdminActionUsage(stderr, recoveryAdminActionByNameOrZero("verify-backup"))
		return 2
	}
	observerDSN, ok := d.getenvValue(config.EnvRecoveryObserverDSN)
	if !ok || strings.TrimSpace(observerDSN) == "" {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify-backup: %s is required (not configured); refusing\n", config.EnvRecoveryObserverDSN)
		return 1
	}
	operation, err := recoveryOpOperationID(*operationID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify-backup: %s\n", logx.Redact(err.Error()))
		return 2
	}
	instanceID, bound, code := recoveryOpInstanceContext(d, *instanceFlag, "verify-backup")
	if code != 0 {
		return code
	}

	env, code := recoveryOpOpen(ctx, d, "verify-backup")
	if env == nil {
		return code
	}
	defer env.pool.Close()

	if !bound {
		openID, ok, err := recoveryOpOpenInstance(ctx, env)
		if err != nil {
			fmt.Fprintf(stderr, "txharbor recovery-admin verify-backup: %s\n", logx.Redact(err.Error()))
			return 1
		}
		if ok {
			instanceID, bound = openID, true
		}
	}
	if err := recoveryOpRequireParticipant(ctx, env, instanceID, "verifier", "verify-backup"); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify-backup: refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	configuredIsolated, ok := d.getenvValue(config.EnvRecoveryIsolatedTargetDSN)
	if !ok || strings.TrimSpace(configuredIsolated) == "" {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify-backup: %s is required (not configured); refusing\n", config.EnvRecoveryIsolatedTargetDSN)
		return 1
	}
	openBindings, err := recoveryOpOpenTargetBindings(ctx, env.pool)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify-backup: open instance target bindings unavailable: %s\n", logx.Redact(err.Error()))
		return 1
	}
	isolated, err := recovery.BindIsolatedTarget(env.dataDSN, env.controlDSN, configuredIsolated, openBindings)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify-backup: isolated target refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if strings.TrimSpace(*targetDSN) != "" {
		if err := recovery.AssertIsolatedTarget(isolated, *targetDSN); err != nil {
			fmt.Fprintf(stderr, "txharbor recovery-admin verify-backup: isolated target assertion refused: %s\n", logx.Redact(err.Error()))
			return 1
		}
	}

	inputDigest := recoveryOpInputDigest(recoveryActionVerifyBackup, map[string]string{
		"manifest":    strings.TrimSpace(*manifestPath),
		"target":      isolated.TargetFingerprint(),
		"instance_id": instanceID,
		"control_dsn": env.controlFingerprint,
	})
	release, recorded, err := recoveryOpBegin(ctx, env.pool, recoveryActionVerifyBackup, operation)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify-backup: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if release != nil {
		defer release()
	}
	if replayed, code := recoveryOpReplayVerifyBackup(ctx, stderr, stdout, env.store, recorded,
		strings.TrimSpace(*manifestPath), inputDigest, operation, env.principal); replayed {
		return code
	}

	programVersion, err := recovery.CurrentProgramVersion()
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify-backup: %s\n", logx.Redact(err.Error()))
		return 1
	}
	result, err := recovery.ExecuteVerifyBackup(ctx, recovery.VerifyBackupOptions{
		ManifestPath:     strings.TrimSpace(*manifestPath),
		Binding:          isolated,
		TargetDSN:        strings.TrimSpace(*targetDSN),
		Verifier:         env.principal,
		InstanceID:       instanceID,
		ControlStore:     env.store,
		ControlDSN:       env.controlDSN,
		AuthoritativeDSN: env.dataDSN,
		ObserverDSN:      observerDSN,
		ProgramVersion:   programVersion,
		OperationID:      operation,
		PG:               recovery.LocalPGCommand{},
	})
	if err != nil {
		reason := strings.TrimSpace(result.Reason)
		if reason == "" {
			reason = err.Error()
		}
		reason = recoveryOpSafeText(reason)
		_ = recoveryOpRecord(ctx, env.pool, recoveryActionVerifyBackup, operation, env.principal, controlstore.AuditRefused,
			map[string]any{"input_digest": inputDigest, "state": string(result.State), "reason": reason},
			map[string]any{"manifest_path": recoveryOpSafeText(strings.TrimSpace(*manifestPath))})
		fmt.Fprintf(stderr, "txharbor recovery-admin verify-backup: %s: %s\n", result.State, reason)
		return 1
	}
	checks := result.Checks
	_ = recoveryOpRecord(ctx, env.pool, recoveryActionVerifyBackup, operation, env.principal,
		map[bool]string{true: controlstore.AuditOK, false: controlstore.AuditRefused}[result.State == recovery.VerificationVerified],
		map[string]any{
			"input_digest":       inputDigest,
			"state":              string(result.State),
			"reason":             recoveryOpSafeText(result.Reason),
			"backup_id":          result.BackupID,
			"manifest_version":   result.ManifestVersion,
			"evidence_ref":       result.EvidenceRef,
			"verification_epoch": result.VerificationEpoch,
			"accepted_audit_id":  result.AcceptedAuditID,
			"checks": map[string]bool{
				"readable":                checks.Readable,
				"structure_constraints":   checks.StructureConstraints,
				"business_state_probes":   checks.BusinessStateProbes,
				"verification_executable": checks.VerificationExecutable,
			},
			"manifest_digest": result.ManifestDigest,
		},
		map[string]any{"manifest_path": recoveryOpSafeText(result.ManifestPath), "instance_id": instanceID})
	fmt.Fprintf(stdout,
		"txharbor recovery-admin verify-backup: manifest=%s verification=%s readable=%t structure_constraints=%t business_state_probes=%t verification_executable=%t evidence_ref=%s operation_id=%s verifier=%s instance=%s\n",
		recoveryOpSafeText(result.ManifestPath), result.State, checks.Readable, checks.StructureConstraints,
		checks.BusinessStateProbes, checks.VerificationExecutable, result.EvidenceRef,
		recoveryOpDisplayID(operation), env.principal, instanceID)
	if result.State != recovery.VerificationVerified {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify-backup: verification %s: %s\n", result.State, recoveryOpSafeText(result.Reason))
		return 1
	}
	return 0
}

// recoveryOpReplayVerifyBackup refuses a cached success once a newer START
// epoch has superseded the exact accepted receipt. It performs only a control
// store lookup and never runs pg_restore or other verification tools.
func recoveryOpReplayVerifyBackup(ctx context.Context, stderr, stdout io.Writer, store *controlstore.Store,
	recorded *recoveryOpRecorded, manifestPath, inputDigest, operation, actor string) (bool, int) {
	if recorded == nil || recorded.Result != controlstore.AuditOK {
		return recoveryOpReplay(stderr, stdout, recorded, inputDigest, operation)
	}
	var detail struct {
		InputDigest     string `json:"input_digest"`
		State           string `json:"state"`
		BackupID        string `json:"backup_id"`
		ManifestVersion string `json:"manifest_version"`
		ManifestDigest  string `json:"manifest_digest"`
		EvidenceRef     string `json:"evidence_ref"`
		AcceptedAuditID int64  `json:"accepted_audit_id"`
	}
	if err := json.Unmarshal(recorded.Detail, &detail); err != nil || detail.InputDigest != inputDigest {
		return recoveryOpReplay(stderr, stdout, recorded, inputDigest, operation)
	}
	if detail.State != string(recovery.VerificationVerified) || detail.BackupID == "" ||
		detail.ManifestVersion == "" || detail.ManifestDigest == "" || detail.EvidenceRef == "" || detail.AcceptedAuditID <= 0 {
		fmt.Fprintf(stderr, "txharbor recovery-admin: replay refused operation_id=%s cached verification has no accepted receipt\n", operation)
		return true, 1
	}
	refuse := func(reason string) (bool, int) {
		_ = recoveryOpRecord(ctx, store.Pool(), recoveryActionVerifyBackup, operation, actor, controlstore.AuditRefused,
			map[string]any{"input_digest": inputDigest, "state": string(recovery.VerificationVerified), "reason": reason},
			map[string]any{"manifest_path": recoveryOpSafeText(manifestPath)})
		fmt.Fprintf(stderr, "txharbor recovery-admin: replay refused operation_id=%s %s\n", operation, reason)
		return true, 1
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return refuse("cached verification manifest is unavailable or invalid")
	}
	manifest, err := recovery.ParseManifest(manifestBytes)
	if err != nil {
		return refuse("cached verification manifest is unavailable or invalid")
	}
	currentDigest, err := manifest.Digest()
	if err != nil || manifest.BackupID != detail.BackupID || manifest.ManifestVersion != detail.ManifestVersion ||
		manifest.Verification.State != recovery.VerificationVerified ||
		manifest.Verification.EvidenceRef != detail.EvidenceRef || currentDigest != detail.ManifestDigest {
		return refuse("cached verification manifest no longer matches its accepted receipt")
	}
	var target map[string]any
	_ = json.Unmarshal(recorded.Target, &target)
	instanceID, _ := target["instance_id"].(string)
	current, err := recovery.BackupVerificationReceiptCurrent(ctx, store, detail.BackupID,
		instanceID, detail.ManifestVersion, detail.ManifestDigest, detail.EvidenceRef, detail.AcceptedAuditID)
	if err != nil || !current {
		return refuse("verification receipt is no longer current")
	}
	return recoveryOpReplay(stderr, stdout, recorded, inputDigest, operation)
}

// ---------------------------------------------------------------------------
// Operation id (persistent idempotency)
// ---------------------------------------------------------------------------

// recoveryOpOperationID validates the optional operation_id carriage. It is
// an idempotency key, never an authorization input.
func recoveryOpOperationID(raw string) (string, error) {
	operation := strings.TrimSpace(raw)
	if operation == "" {
		return "", nil
	}
	if len(operation) > 128 || strings.ContainsAny(operation, "\x00\n\r\t") {
		return "", errors.New("--operation-id must be 1..128 characters without control characters")
	}
	return operation, nil
}

// recoveryOpRecorded is one recorded command outcome read back for replay.
type recoveryOpRecorded struct {
	Result string
	Target []byte
	Detail []byte
}

// recoveryOpBegin serializes equal operation ids across processes (session
// advisory lock, released by the returned function) and reads back the
// recorded outcome. Without an operation id it does nothing: the invocation
// is still audited under a derived id but is not replayable.
func recoveryOpBegin(ctx context.Context, pool *pgxpool.Pool, action, operation string) (func(), *recoveryOpRecorded, error) {
	if operation == "" {
		return nil, nil, nil
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("acquire operation lock connection: %w", err)
	}
	lockKey := "recovery_admin_op:" + action + ":" + operation
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtext($1))`, lockKey); err != nil {
		conn.Release()
		return nil, nil, fmt.Errorf("lock operation id: %w", err)
	}
	release := func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock(hashtext($1))`, lockKey)
		conn.Release()
	}
	var rec recoveryOpRecorded
	err = conn.QueryRow(ctx, `
SELECT result, COALESCE(target, '{}'::jsonb)::text, COALESCE(detail, '{}'::jsonb)::text
FROM recovery_audit
WHERE action = $1 AND operation_id = $2
ORDER BY audit_id DESC
LIMIT 1`, action, operation).Scan(&rec.Result, &rec.Target, &rec.Detail)
	if errors.Is(err, pgx.ErrNoRows) {
		return release, nil, nil
	}
	if err != nil {
		release()
		return nil, nil, fmt.Errorf("read recorded operation: %w", err)
	}
	return release, &rec, nil
}

// recoveryOpReplay reports a recorded outcome for an already-seen operation.
// Same input replays exactly (zero side effects); different input conflicts
// with zero writes.
func recoveryOpReplay(stderr, stdout io.Writer, recorded *recoveryOpRecorded, inputDigest, operation string) (bool, int) {
	if recorded == nil {
		return false, 0
	}
	var detail map[string]any
	_ = json.Unmarshal(recorded.Detail, &detail)
	if digest, _ := detail["input_digest"].(string); digest != inputDigest {
		fmt.Fprintf(stderr, "txharbor recovery-admin: operation_conflict operation_id=%s (the operation id was recorded with a different input; zero writes)\n", operation)
		return true, 1
	}
	var target map[string]any
	_ = json.Unmarshal(recorded.Target, &target)
	switch recorded.Result {
	case controlstore.AuditOK:
		verification := detail["verification"]
		if verification == nil {
			verification = detail["state"]
		}
		fmt.Fprintf(stdout, "txharbor recovery-admin: replayed=true operation_id=%s result=ok manifest=%v verification=%v\n",
			operation, recoveryOpSafeText(fmt.Sprint(target["manifest_path"])), verification)
		return true, 0
	default:
		fmt.Fprintf(stderr, "txharbor recovery-admin: replayed=true operation_id=%s result=%s (the recorded refusal replays; zero side effects)\n",
			operation, recorded.Result)
		return true, 1
	}
}

// recoveryOpRecord appends the command outcome audit row (best effort: the
// command result is already decided; an audit failure never changes it).
func recoveryOpRecord(ctx context.Context, pool *pgxpool.Pool, action, operation, actor, result string,
	detail, target map[string]any) error {
	detailJSON, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	targetJSON, err := json.Marshal(target)
	if err != nil {
		return err
	}
	return controlstore.WriteAudit(ctx, pool, controlstore.AuditRecord{
		Actor:       actor,
		Action:      action,
		Target:      targetJSON,
		Detail:      detailJSON,
		Result:      result,
		OperationID: operation,
	})
}

// recoveryOpInputDigest binds a recorded outcome to its exact input: a reused
// operation id with a different input is a conflict, never a silent replay.
func recoveryOpInputDigest(action string, fields map[string]string) string {
	payload, _ := json.Marshal(map[string]any{"action": action, "fields": fields})
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// recoveryOpDisplayID renders the operation id (or the untracked marker).
func recoveryOpDisplayID(operation string) string {
	if operation == "" {
		return "(untracked)"
	}
	return operation
}

// recoveryTargetFingerprint derives the credential-free fingerprint of a DSN
// for audit detail; an unparsable DSN is reported as such.
func recoveryTargetFingerprint(dsn string) string {
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		return "<unparsable>"
	}
	return target.DataTargetFingerprint().TargetFingerprint
}
