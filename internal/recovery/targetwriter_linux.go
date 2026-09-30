//go:build linux

package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// TargetWriterProof is the locally observed basis passed to the caller's
// acceptance transaction. It deliberately excludes DSNs and credentials.
type TargetWriterProof struct {
	TargetKey   TargetKey
	Application string
	Command     PGCommandResult
	MarkerToken EvidenceToken
	Probe       TargetWriterProbeResult
}

type TargetWriterProbeOutcome string

const TargetWriterProbePassed TargetWriterProbeOutcome = "passed"

// TargetWriterProbeResult is a typed post-restore check. ApplicationName must
// echo the exact attempt tag observed by the probe, and Evidence must be valid
// JSON describing its accepted probe basis.
type TargetWriterProbeResult struct {
	Outcome         TargetWriterProbeOutcome
	ApplicationName string
	Evidence        json.RawMessage
}

// TargetWriterAcceptance is the caller's receipt for accepted result rows
// inserted by Acceptance inside the supplied transaction. At least one
// nonempty row reference and the PostgreSQL transaction ID observed in that
// same transaction are mandatory; returning nil error alone is not proof.
type TargetWriterAcceptance struct {
	AcceptedRowRefs       []string
	TransactionID         int64
	AcceptedEvidenceToken EvidenceToken
}

// TargetWriterOperationKind is a closed description of the evidence protocol
// used by a supervised target writer. It is intentionally required: a caller
// must not silently receive restore semantics for a verification operation.
type TargetWriterOperationKind string

const (
	TargetWriterOperationRestore      TargetWriterOperationKind = "restore"
	TargetWriterOperationVerifyBackup TargetWriterOperationKind = "verify_backup"
)

// TargetWriterOptions describes one supervised pg_restore. TrustedTarget is
// supplied by the caller's trusted binding, not derived from TargetDSN.
// ObserverDSN must connect to the same PostgreSQL cluster with pg_read_all_stats
// (or superuser) visibility. Endpoint identity cannot resolve DNS aliases or
// detect proxy routing; activity visibility proves only exact tagged sessions
// on the observer's cluster, not arbitrary external writers. A missing durable
// clean guard is never bootstrapped.
type TargetWriterOptions struct {
	OperationKind TargetWriterOperationKind
	Store         *controlstore.Store
	ControlDSN    string
	TargetDSN     string
	ObserverDSN   string
	TrustedTarget controlstore.DSNTarget
	// IsolatedBinding is mandatory for verify operations. Its unexported
	// representation can only be populated by BindIsolatedTarget.
	IsolatedBinding  IsolatedTarget
	AuthoritativeDSN string
	InstanceID       string // empty only for an isolated/unbound verify target
	OperationID      string
	Archive          io.Reader
	// executable is a package-private test seam. Production callers leave it
	// empty and use a PATH-resolved, validated pg_restore ELF executable.
	// Tests may set this to controlled scripts; it is not a production path.
	executable            string
	prelaunchCommit       func(context.Context, pgx.Tx) error
	afterAcceptanceCommit func() error
	Runner                TargetProcessRunner
	// Prelaunch runs after this coordinator holds the target session lock and
	// recovery instance row lock, but before target guard preparation/launch
	// intent. Bound restore callers must write their restore_started marker in
	// this tx (normally via CommitEvidenceWriteTx) and return that accepted token.
	// It must not call CommitEvidenceWrite, which would nest and commit a second tx.
	Prelaunch          func(context.Context, pgx.Tx, controlstore.InstanceToken) (EvidenceToken, error)
	Probe              func(context.Context, TargetWriterProof) (TargetWriterProbeResult, error)
	LockTimeout        time.Duration
	AcceptanceTimeout  time.Duration
	QuiescenceTimeout  time.Duration
	QuiescenceInterval time.Duration
	// Acceptance writes caller evidence/result rows in this tx (normally via
	// CommitEvidenceWriteTx, never the committing CommitEvidenceWrite wrapper)
	// and returns references plus this transaction's txid_current() value. Bound
	// restores must also return the accepted restore_probe_accepted token; a
	// discarded/stale generation token is never eligible for guard acceptance.
	Acceptance         func(context.Context, pgx.Tx, TargetWriterProof) (TargetWriterAcceptance, error)
	LockHealthInterval time.Duration
}

type TargetWriterResult struct {
	TargetKey   TargetKey
	Application string
	Command     PGCommandResult
	MarkerToken EvidenceToken
	Probe       TargetWriterProbeResult
}

// runTargetWriter coordinates only this direct pg_restore child. It does not
// make arbitrary external target writers safe or prove they obey this lock.
// Any failure after durable launch intent leaves the guard unknown or
// rebuild_required; ordinary execution never clears either state.
func runTargetWriter(ctx context.Context, opts TargetWriterOptions) (result TargetWriterResult, retErr error) {
	executable, err := resolveTargetWriterExecutable(opts.executable)
	if err != nil {
		return result, errors.New("target writer requires a direct executable pg_restore ELF")
	}
	if opts.OperationKind != TargetWriterOperationRestore && opts.OperationKind != TargetWriterOperationVerifyBackup {
		return result, errors.New("target writer operation kind is missing or unsupported")
	}
	if ctx == nil || opts.Store == nil || opts.Store.Pool() == nil || opts.ControlDSN == "" || opts.TargetDSN == "" || opts.ObserverDSN == "" || opts.OperationID == "" || opts.Archive == nil || opts.Probe == nil || opts.Acceptance == nil {
		return result, errors.New("target writer requires context, control store, DSNs, operation, archive, probe, and acceptance callback")
	}
	if opts.InstanceID != "" && opts.OperationKind == TargetWriterOperationRestore && opts.Prelaunch == nil {
		return result, errors.New("bound target restore requires a transactional prelaunch marker hook")
	}
	if opts.OperationKind == TargetWriterOperationVerifyBackup && opts.Prelaunch != nil {
		return result, errors.New("backup verification must not use a restore-start marker hook")
	}
	if opts.OperationKind == TargetWriterOperationVerifyBackup {
		if opts.IsolatedBinding.BoundDSN() == "" || opts.AuthoritativeDSN == "" {
			return result, errors.New("backup verification requires an opaque isolated target binding and authoritative target")
		}
		if opts.IsolatedBinding.BoundDSN() != opts.TargetDSN {
			return result, errors.New("backup verification target does not match its isolated binding")
		}
	}
	if err := validatePGChildEnvironment(os.Environ()); err != nil {
		return result, errors.New("ambient PostgreSQL environment is unsupported")
	}
	controlTarget, err := controlstore.ParseDSNTarget(opts.ControlDSN)
	if err != nil {
		return result, errors.New("control-store DSN identity is invalid")
	}
	parsed, err := controlstore.ParseDSNTarget(opts.TargetDSN)
	if err != nil {
		return result, errors.New("target DSN identity is invalid")
	}
	trustedKey, err := CanonicalTargetKey(opts.TrustedTarget)
	if err != nil {
		return result, errors.New("trusted target identity is invalid")
	}
	parsedKey, err := CanonicalTargetKey(parsed)
	if err != nil || trustedKey != parsedKey {
		return result, errors.New("target DSN does not match trusted target identity")
	}
	if opts.OperationKind == TargetWriterOperationVerifyBackup && (opts.IsolatedBinding.TargetGuardKey() != trustedKey.String() || opts.IsolatedBinding.RoleFingerprint() != opts.TrustedTarget.DataTargetFingerprint().RoleFingerprint) {
		return result, errors.New("backup verification isolated binding identity is inconsistent")
	}
	if parsed.DataTargetFingerprint().RoleFingerprint != opts.TrustedTarget.DataTargetFingerprint().RoleFingerprint {
		return result, errors.New("target DSN role does not match trusted target identity")
	}
	controlKey, err := CanonicalTargetKey(controlTarget)
	if err != nil || controlKey == trustedKey {
		return result, errors.New("control-store database cannot be the target writer database")
	}
	observer, err := controlstore.ParseDSNTarget(opts.ObserverDSN)
	if err != nil {
		return result, errors.New("target observer identity is invalid")
	}
	observerKey, err := CanonicalTargetKey(observer)
	if err != nil || observerKey != trustedKey {
		return result, errors.New("target observer does not match trusted target identity")
	}
	key := trustedKey
	result.TargetKey = key
	lockTimeout := opts.LockTimeout
	if lockTimeout <= 0 {
		lockTimeout = 10 * time.Second
	}
	lock, err := AcquireTargetLock(ctx, opts.ControlDSN, key, lockTimeout, 50*time.Millisecond)
	if err != nil {
		return result, errors.New("could not acquire bounded target writer lock")
	}
	defer func() {
		if err := lock.Release(context.Background()); err != nil && retErr == nil {
			retErr = errors.New("target evidence and guard were committed, but target writer lock release could not be confirmed")
		}
	}()
	appName, err := NewAttemptApplicationName()
	if err != nil {
		return result, errors.New("could not create target attempt identity")
	}
	result.Application = appName
	conninfo, err := ConninfoWithAttemptApplicationName(opts.TargetDSN, appName)
	if err != nil {
		return result, errors.New("could not tag target writer connection")
	}
	prepareTx, err := opts.Store.Pool().Begin(ctx)
	if err != nil {
		return result, errors.New("could not begin target prelaunch transaction")
	}
	var markerToken EvidenceToken
	var locked controlstore.InstanceToken
	if opts.InstanceID != "" {
		if opts.OperationKind == TargetWriterOperationVerifyBackup {
			authoritative, parseErr := controlstore.ParseDSNTarget(opts.AuthoritativeDSN)
			if parseErr == nil {
				locked, err = lockOpenInstanceAuthoritative(ctx, prepareTx, opts.InstanceID, authoritative)
			} else {
				err = parseErr
			}
		} else {
			locked, err = lockOpenInstanceTarget(ctx, prepareTx, opts.InstanceID, key, opts.TrustedTarget)
		}
		if err != nil {
			_ = prepareTx.Rollback(context.Background())
			return result, errors.New("recovery instance target binding is not usable")
		}
		if opts.OperationKind == TargetWriterOperationRestore {
			markerToken, err = opts.Prelaunch(ctx, prepareTx, locked)
			if err != nil {
				_ = prepareTx.Rollback(context.Background())
				return result, errors.New("transactional restore-start marker hook failed")
			}
			updated, lockErr := controlstore.LockInstance(ctx, prepareTx, opts.InstanceID)
			if lockErr != nil || !markerToken.Matches(LockedEvidenceToken(updated)) || markerToken.Generation <= locked.EvidenceGeneration || markerToken.State != "open" {
				_ = prepareTx.Rollback(context.Background())
				return result, errors.New("prelaunch hook did not return the committed marker evidence token")
			}
			if err := requireRestoreStartedMarker(ctx, prepareTx, updated.InstanceID, opts.OperationID, markerToken); err != nil {
				_ = prepareTx.Rollback(context.Background())
				return result, errors.New("prelaunch hook did not commit restore_started evidence for this operation")
			}
			locked = updated
		} else {
			// Verification has no T019 restore_started marker. The row lock and
			// this captured token make subsequent acceptance conditional on the
			// same open instance generation observed before launching the probe.
			markerToken = LockedEvidenceToken(locked)
		}
		result.MarkerToken = markerToken
	}
	if opts.OperationKind == TargetWriterOperationVerifyBackup {
		bindings, bindErr := readOpenIsolatedBindingsTx(ctx, prepareTx)
		if bindErr != nil {
			_ = prepareTx.Rollback(context.Background())
			return result, errors.New("could not re-read isolated target bindings under target lock")
		}
		binding, bindErr := BindIsolatedTarget(opts.AuthoritativeDSN, opts.ControlDSN, opts.TargetDSN, bindings)
		if bindErr != nil || binding != opts.IsolatedBinding {
			_ = prepareTx.Rollback(context.Background())
			return result, errors.New("isolated target binding is stale or conflicts with an open recovery instance")
		}
	}
	guardInstanceID := opts.InstanceID
	if opts.OperationKind == TargetWriterOperationVerifyBackup {
		// Verification writes only to its separately-bound isolated target; the
		// instance's authoritative target guard is unrelated to this operation.
		guardInstanceID = ""
	}
	if err = requireAndPrepareTargetGuard(ctx, prepareTx, guardInstanceID, key, opts.TrustedTarget, opts.OperationID); err != nil {
		_ = prepareTx.Rollback(context.Background())
		return result, err
	}
	if opts.OperationKind == TargetWriterOperationVerifyBackup {
		// PrepareTargetGuard holds the target-guard transaction lock through the
		// launch-intent commit. Re-read after acquiring it: OpenInstance inserts
		// its inventory row before taking this same guard lock, so a pre-lock
		// snapshot alone cannot rule out an uncommitted open.
		if err := verifyIsolatedBindingTx(ctx, prepareTx, opts); err != nil {
			_ = prepareTx.Rollback(context.Background())
			return result, errors.New("isolated target binding changed during target preparation")
		}
	}
	if err = controlstore.MarkTargetGuardLaunchIntent(ctx, prepareTx, key.String(), appName, opts.OperationID); err != nil {
		_ = prepareTx.Rollback(context.Background())
		return result, errors.New("could not record target launch intent")
	}
	intentDurable := true // commit outcome may be ambiguous from this point on
	failureCleanupEligible := false
	var lockHealth func(context.Context) error
	defer func() {
		if retErr != nil && intentDurable && failureCleanupEligible {
			// Only the original healthy lock owner may finalize a failed attempt.
			// The independent context lets observations run after caller cancellation.
			if err := finalizeFailedTargetAttempt(lock, lockHealth, opts.ObserverDSN, key.String(), opts.OperationID, appName, opts.QuiescenceTimeout, opts.QuiescenceInterval); err != nil {
				retErr = errors.Join(retErr, errors.New("failed target attempt guard finalization is uncertain"))
			}
		}
	}()
	if opts.prelaunchCommit != nil {
		err = opts.prelaunchCommit(ctx, prepareTx)
	} else {
		err = prepareTx.Commit(ctx)
	}
	if err != nil {
		_ = prepareTx.Rollback(context.Background())
		return result, errors.New("prelaunch marker and target launch-intent commit is uncertain")
	}

	// Keep the original tagged DSN only in parent memory. The supervised
	// process path must call protectPGChildArgsWithEnvironment before Start;
	// argv receives only the helper-rewritten, credential-safe value.
	args := []string{"--clean", "--if-exists", "--dbname=" + conninfo}
	var healthMu sync.Mutex
	rawLockHealth := func(checkCtx context.Context) error {
		healthMu.Lock()
		defer healthMu.Unlock()
		return lock.Health(checkCtx)
	}
	healthTimeout := opts.Runner.HealthCheckTimeout
	if healthTimeout <= 0 {
		healthTimeout = time.Second
	}
	lockHealth = func(checkCtx context.Context) error {
		return runBoundedLockHealth(checkCtx, rawLockHealth, healthTimeout)
	}
	command, runErr := opts.Runner.RunPGCommandWithEnv(ctx, executable, args, opts.Archive, io.Discard, io.Discard, os.Environ(), lockHealth)
	result.Command = command
	failureCleanupEligible = targetAttemptFailureCleanupEligible(command)
	if command.Started {
		if err := markTargetLaunched(opts.Store, key.String(), opts.OperationID, appName); err != nil {
			return result, errors.New("target launch observation could not be committed")
		}
	}
	if runErr != nil || command.Outcome != PGCommandSucceeded || !command.Started || !command.ProcessGroupDrained {
		return result, errors.New("target writer did not complete with a proven process-group drain")
	}
	if err := waitTargetQuiescentWithLock(ctx, opts.ObserverDSN, appName, lockHealth, opts.QuiescenceTimeout, opts.QuiescenceInterval); err != nil {
		return result, errors.New("target writer sessions did not become provably quiescent")
	}
	if err := lockHealth(ctx); err != nil {
		return result, errors.New("target writer lock health is uncertain after drain")
	}
	proof := TargetWriterProof{TargetKey: key, Application: appName, Command: command, MarkerToken: markerToken}
	var probeResult TargetWriterProbeResult
	if err := runWithTargetLockMonitor(ctx, lockHealth, opts.LockHealthInterval, func(probeCtx context.Context) error {
		var probeErr error
		probeResult, probeErr = opts.Probe(probeCtx, proof)
		return probeErr
	}); err != nil {
		return result, errors.New("target post-restore probe failed or lock health was lost")
	}
	if probeResult.Outcome != TargetWriterProbePassed || probeResult.ApplicationName != appName || !json.Valid(probeResult.Evidence) {
		return result, errors.New("target post-restore probe did not prove success for the exact attempt tag")
	}
	proof.Probe = probeResult
	result.Probe = probeResult
	if err := lockHealth(ctx); err != nil {
		return result, errors.New("target writer lock health is uncertain after probe")
	}

	acceptance := TargetWriterAcceptance{}
	acceptTimeout := opts.AcceptanceTimeout
	if acceptTimeout <= 0 {
		acceptTimeout = 30 * time.Second
	}
	acceptCtx, cancelAcceptance := context.WithTimeout(ctx, acceptTimeout)
	defer cancelAcceptance()
	if err := lock.WithTransaction(acceptCtx, func(txCtx context.Context, acceptTx pgx.Tx) error {
		if opts.InstanceID != "" {
			if opts.OperationKind == TargetWriterOperationVerifyBackup {
				authoritative, parseErr := controlstore.ParseDSNTarget(opts.AuthoritativeDSN)
				if parseErr == nil {
					locked, err = lockOpenInstanceAuthoritative(txCtx, acceptTx, opts.InstanceID, authoritative)
				} else {
					err = parseErr
				}
			} else {
				locked, err = lockOpenInstanceTarget(txCtx, acceptTx, opts.InstanceID, key, opts.TrustedTarget)
			}
			if err != nil {
				return errors.New("bound recovery instance changed before target acceptance")
			}
			if !markerToken.Matches(LockedEvidenceToken(locked)) {
				return errors.New("pre-probe evidence token is stale before target acceptance")
			}
		}
		if opts.OperationKind == TargetWriterOperationVerifyBackup {
			if err := verifyIsolatedBindingTx(txCtx, acceptTx, opts); err != nil {
				return errors.New("isolated target binding changed before target acceptance")
			}
		}
		var acceptErr error
		acceptance, acceptErr = opts.Acceptance(txCtx, acceptTx, proof)
		if acceptErr != nil {
			return fmt.Errorf("target acceptance callback failed: %w", acceptErr)
		}
		if err := validateTargetWriterAcceptance(acceptance); err != nil {
			return errors.New("target acceptance callback returned no accepted row proof")
		}
		var acceptanceTxID int64
		if err := acceptTx.QueryRow(txCtx, `SELECT txid_current()`).Scan(&acceptanceTxID); err != nil || acceptance.TransactionID != acceptanceTxID {
			return errors.New("target acceptance receipt does not identify this transaction")
		}
		if opts.InstanceID != "" {
			currentToken, err := CaptureEvidenceToken(txCtx, acceptTx, opts.InstanceID)
			if err != nil || !acceptance.AcceptedEvidenceToken.Matches(currentToken) || currentToken.Generation <= markerToken.Generation || currentToken.State != "open" {
				return errors.New("bound acceptance did not return a new accepted evidence token")
			}
			if opts.OperationKind == TargetWriterOperationRestore {
				if err := requireRestoreProbeAccepted(txCtx, acceptTx, opts.InstanceID, opts.OperationID, currentToken, acceptance.AcceptedRowRefs); err != nil {
					return errors.New("bound acceptance has no accepted restore_probe evidence row")
				}
			} else if err := requireBackupManifestAcceptance(txCtx, acceptTx, opts.InstanceID, currentToken, acceptance.AcceptedRowRefs); err != nil {
				return errors.New("bound verification has no new backup_manifest evidence for its accepted generation")
			}
		} else if opts.OperationKind == TargetWriterOperationVerifyBackup {
			if err := requireUnboundBackupManifestAcceptance(txCtx, acceptTx, acceptance.AcceptedRowRefs); err != nil {
				return errors.New("unbound verification has no new backup_manifest evidence and audit")
			}
		}
		if err := markTargetWriterDrainedInTx(txCtx, acceptTx, key.String(), opts.OperationID, appName); err != nil {
			return errors.New("target writer drain could not be committed")
		}
		evidence, err := json.Marshal(map[string]any{
			"application_name": appName, "process_outcome": command.Outcome,
			"process_group_drained": true, "target_quiescent": true,
			"probe_evidence": probeResult.Evidence, "accepted_row_refs": acceptance.AcceptedRowRefs,
			"marker_token": markerToken,
		})
		if err != nil {
			return errors.New("target acceptance evidence could not be encoded")
		}
		if err := controlstore.AcceptTargetGuardCleanForAttempt(txCtx, acceptTx, key.String(), opts.OperationID, appName, evidence); err != nil {
			return errors.New("target guard acceptance refused")
		}
		return nil
	}); err != nil {
		return result, fmt.Errorf("target evidence and guard acceptance transaction failed: %w", err)
	}
	// WithTransaction returned only after the owner-session commit succeeded.
	// Later acknowledgement/reporting or Release errors must not alter clean.
	intentDurable = false
	if opts.afterAcceptanceCommit != nil {
		if err := opts.afterAcceptanceCommit(); err != nil {
			return result, fmt.Errorf("target acceptance committed but its acknowledgement was uncertain: %w", err)
		}
	}
	return result, nil
}

func targetAttemptFailureCleanupEligible(command PGCommandResult) bool {
	return command.Started && command.ProcessGroupDrained &&
		(command.Outcome == PGCommandSucceeded || command.Outcome == PGCommandFailed || command.Outcome == PGCommandCanceled)
}

func verifyIsolatedBindingTx(ctx context.Context, tx pgx.Tx, opts TargetWriterOptions) error {
	bindings, err := readOpenIsolatedBindingsTx(ctx, tx)
	if err != nil {
		return err
	}
	binding, err := BindIsolatedTarget(opts.AuthoritativeDSN, opts.ControlDSN, opts.TargetDSN, bindings)
	if err != nil || binding != opts.IsolatedBinding {
		return errors.New("isolated target binding is stale or conflicts with an open recovery instance")
	}
	return nil
}

// resolveTargetWriterExecutable enforces the supported production topology:
// invoke a resolved local pg_restore ELF directly, not a shell script or
// wrapper. The package-private configured path is retained for controlled
// tests only. Resolving and vetting the path before any guard work prevents a
// PATH shim from receiving durable launch intent. The filesystem can still be
// changed between validation and exec (TOCTOU); this is a topology assertion,
// not cryptographic verification against arbitrary ELF wrappers or aliases.
func resolveTargetWriterExecutable(configured string) (string, error) {
	if configured != "" {
		return configured, nil
	}
	path, err := exec.LookPath("pg_restore")
	if err != nil {
		return "", errors.New("pg_restore unavailable")
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", errors.New("pg_restore path invalid")
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", errors.New("pg_restore path invalid")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return "", errors.New("pg_restore is not an executable regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", errors.New("pg_restore cannot be inspected")
	}
	defer file.Close()
	var magic [4]byte
	if _, err := io.ReadFull(file, magic[:]); err != nil || magic != [4]byte{0x7f, 'E', 'L', 'F'} {
		return "", errors.New("pg_restore is not an ELF executable")
	}
	return path, nil
}

func requireAndPrepareTargetGuard(ctx context.Context, tx pgx.Tx, instanceID string, key TargetKey, target controlstore.DSNTarget, operationID string) error {
	if instanceID != "" {
		if _, err := lockOpenInstanceTarget(ctx, tx, instanceID, key, target); err != nil {
			return errors.New("recovery instance target binding is not usable")
		}
		fp := target.DataTargetFingerprint()
		if err := controlstore.RequireCleanTargetGuard(ctx, tx, instanceID, key.String(), fp.RoleFingerprint); err != nil {
			return errors.New("bound target guard is not clean")
		}
	}
	guard, found, err := controlstore.ReadTargetGuard(ctx, tx, key.String())
	if err != nil || !found || guard.State != controlstore.TargetGuardClean || guard.ActiveWriter {
		return errors.New("target guard is missing or not clean")
	}
	if err := controlstore.PrepareTargetGuardForAttempt(ctx, tx, key.String(), guard.OperationID, guard.AttemptAppName, operationID); err != nil {
		return errors.New("target guard preparation was refused")
	}
	return nil
}

func lockOpenInstanceTarget(ctx context.Context, tx pgx.Tx, instanceID string, key TargetKey, target controlstore.DSNTarget) (controlstore.InstanceToken, error) {
	token, err := controlstore.LockInstance(ctx, tx, instanceID)
	if err != nil {
		return controlstore.InstanceToken{}, err
	}
	fp := target.DataTargetFingerprint()
	if token.State != "open" || token.TargetGuardKey != key.String() || token.TargetRoleFingerprint != fp.RoleFingerprint {
		return controlstore.InstanceToken{}, errors.New("instance is closed or has a different target binding")
	}
	return token, nil
}

// lockOpenInstanceAuthoritative validates the immutable instance binding
// independently from a verification operation's isolated writer target.
func lockOpenInstanceAuthoritative(ctx context.Context, tx pgx.Tx, instanceID string, authoritative controlstore.DSNTarget) (controlstore.InstanceToken, error) {
	token, err := controlstore.LockInstance(ctx, tx, instanceID)
	if err != nil {
		return controlstore.InstanceToken{}, err
	}
	key, err := CanonicalTargetKey(authoritative)
	if err != nil {
		return controlstore.InstanceToken{}, err
	}
	fingerprint := authoritative.DataTargetFingerprint()
	if token.State != "open" || token.TargetGuardKey == "" || token.TargetRoleFingerprint == "" ||
		token.TargetGuardKey != key.String() || token.TargetRoleFingerprint != fingerprint.RoleFingerprint {
		return controlstore.InstanceToken{}, errors.New("instance is closed or has an unusable authoritative target binding")
	}
	return token, nil
}

func requireRestoreStartedMarker(ctx context.Context, tx pgx.Tx, instanceID, operationID string, token EvidenceToken) error {
	var found bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
SELECT 1 FROM recovery_audit
WHERE instance_id = $1 AND action = $2 AND operation_id = $3 AND result = 'ok'
  AND target->>'kind' = $4 AND target->>'accepted_generation' = $5
)`, instanceID, ActionEvidenceWrite, operationID, string(MutationRestoreStarted), strconv.FormatInt(token.Generation, 10)).Scan(&found); err != nil {
		return err
	}
	if !found {
		return errors.New("accepted restore_started evidence audit row is missing")
	}
	return nil
}

func requireRestoreProbeAccepted(ctx context.Context, tx pgx.Tx, instanceID, operationID string, token EvidenceToken, refs []string) error {
	var found bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
SELECT 1 FROM recovery_audit
WHERE instance_id = $1 AND action = $2 AND operation_id = $3 AND result = 'ok'
  AND target->>'kind' = $4 AND target->>'accepted_generation' = $5
  AND xmin::text::bigint = txid_current() % 4294967296
) AND EXISTS (
SELECT 1 FROM recovery_evidence
WHERE instance_id = $1 AND generation = $6 AND kind = 'restore_probe'
  AND artifact_ref = ANY($7::text[])
  AND xmin::text::bigint = txid_current() % 4294967296
)`, instanceID, ActionEvidenceWrite, operationID, string(MutationRestoreProbeAccepted), strconv.FormatInt(token.Generation, 10), token.Generation, refs).Scan(&found); err != nil {
		return err
	}
	if !found {
		return errors.New("accepted restore_probe evidence audit row is missing")
	}
	return nil
}

func readOpenIsolatedBindingsTx(ctx context.Context, tx pgx.Tx) ([]IsolatedInstanceBinding, error) {
	rows, err := tx.Query(ctx, `SELECT target_guard_key, target_role_fingerprint FROM recovery_instance WHERE state='open'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var bindings []IsolatedInstanceBinding
	for rows.Next() {
		var binding IsolatedInstanceBinding
		if err := rows.Scan(&binding.TargetGuardKey, &binding.TargetRoleFingerprint); err != nil {
			return nil, err
		}
		bindings = append(bindings, binding)
	}
	return bindings, rows.Err()
}

// requireBackupManifestAcceptance proves that a real instance-bound
// backup_manifest row was inserted in this acceptance transaction at the
// generation returned by the evidence writer. Callback references alone are
// not evidence of persistence.
func requireBackupManifestAcceptance(ctx context.Context, tx pgx.Tx, instanceID string, token EvidenceToken, refs []string) error {
	var found bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
SELECT 1 FROM recovery_evidence
WHERE instance_id = $1 AND generation = $2 AND kind = 'backup_manifest'
  AND artifact_ref = ANY($3::text[])
  AND xmin::text::bigint = txid_current() % 4294967296
)`, instanceID, token.Generation, refs).Scan(&found); err != nil {
		return err
	}
	if !found {
		return errors.New("new backup_manifest evidence row for accepted generation is missing")
	}
	return nil
}

// requireUnboundBackupManifestAcceptance applies the same same-transaction
// check to an unbound conclusion and additionally requires its explicit
// verify_backup audit record. An arbitrary callback receipt cannot clean the
// target guard.
func requireUnboundBackupManifestAcceptance(ctx context.Context, tx pgx.Tx, refs []string) error {
	var found bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
SELECT 1 FROM recovery_evidence
WHERE instance_id IS NULL AND generation = 0 AND kind = 'backup_manifest'
  AND artifact_ref = ANY($1::text[])
  AND xmin::text::bigint = txid_current() % 4294967296
) AND EXISTS (
SELECT 1 FROM recovery_audit
WHERE action = 'verify_backup' AND result = 'ok'
  AND detail->>'evidence_ref' = ANY($1::text[])
  AND xmin::text::bigint = txid_current() % 4294967296
)`, refs).Scan(&found); err != nil {
		return err
	}
	if !found {
		return errors.New("same-transaction backup_manifest evidence and audit are missing")
	}
	return nil
}

func markTargetLaunched(store *controlstore.Store, key, operationID, appName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	tx, err := store.Pool().Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := controlstore.MarkTargetGuardLaunchedForAttempt(ctx, tx, key, operationID, appName); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func markTargetWriterDrainedInTx(ctx context.Context, tx pgx.Tx, key, operationID, appName string) error {
	return controlstore.MarkTargetGuardWriterDrainedForAttempt(ctx, tx, key, operationID, appName)
}

func finalizeFailedTargetAttempt(lock *TargetLock, lockHealth func(context.Context) error, observerDSN string, key, operationID, appName string, timeout, interval time.Duration) error {
	if lock == nil || lockHealth == nil {
		return errors.New("target lock owner is unknown")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := lockHealth(ctx); err != nil {
		return fmt.Errorf("target lock health check failed: %w", err)
	}
	if err := waitTargetQuiescentWithLock(ctx, observerDSN, appName, lockHealth, timeout, interval); err != nil {
		return fmt.Errorf("tagged target-session quiescence was not proven: %w", err)
	}
	if err := lockHealth(ctx); err != nil {
		return fmt.Errorf("target lock health check after quiescence failed: %w", err)
	}
	err := lock.WithTransaction(ctx, func(txCtx context.Context, tx pgx.Tx) error {
		if err := controlstore.MarkTargetGuardWriterDrainedForAttempt(txCtx, tx, key, operationID, appName); err != nil {
			return err
		}
		return controlstore.MarkTargetGuardRebuildRequiredForAttempt(txCtx, tx, key, operationID, appName)
	})
	return err
}

func waitTargetQuiescentWithLock(ctx context.Context, observerDSN, appName string, lockHealth func(context.Context) error, timeout, interval time.Duration) error {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	bounded, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := lockHealth(bounded); err != nil {
			return err
		}
		quiet, err := CheckTargetQuiescent(bounded, observerDSN, appName)
		if err != nil {
			return err
		}
		if quiet {
			return nil
		}
		select {
		case <-bounded.Done():
			return bounded.Err()
		case <-ticker.C:
		}
	}
}

func validateTargetWriterAcceptance(acceptance TargetWriterAcceptance) error {
	if len(acceptance.AcceptedRowRefs) == 0 || acceptance.TransactionID <= 0 {
		return errors.New("accepted row references and transaction id are required")
	}
	seen := make(map[string]struct{}, len(acceptance.AcceptedRowRefs))
	for _, ref := range acceptance.AcceptedRowRefs {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			return errors.New("accepted row reference is empty")
		}
		if _, duplicate := seen[ref]; duplicate {
			return errors.New("accepted row references must be unique")
		}
		seen[ref] = struct{}{}
	}
	return nil
}

// runWithTargetLockMonitor keeps checking the dedicated advisory-lock session
// while caller work is in progress. Cancellation is cooperative: callers must
// honor ctx; even after return the lock is checked synchronously before the
// coordinator advances acceptance.
func runWithTargetLockMonitor(ctx context.Context, health func(context.Context) error, interval time.Duration, work func(context.Context) error) error {
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	if err := health(ctx); err != nil {
		return err
	}
	workCtx, cancel := context.WithCancel(ctx)
	monitorDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-workCtx.Done():
				monitorDone <- nil
				return
			case <-ticker.C:
				healthCtx, stop := context.WithTimeout(workCtx, time.Second)
				err := health(healthCtx)
				stop()
				if err != nil {
					cancel()
					monitorDone <- err
					return
				}
			}
		}
	}()
	workErr := work(workCtx)
	cancel()
	monitorErr := <-monitorDone
	if monitorErr != nil {
		return errors.New("target lock health was lost during guarded callback")
	}
	if workErr != nil {
		return workErr
	}
	if err := health(ctx); err != nil {
		return errors.New("target lock health could not be proven after guarded callback")
	}
	return nil
}
