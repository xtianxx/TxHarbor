package recovery

// attemptproof.go implements the retained original-attempt proof of ADR-004
// missing-condition #4 in its bounded local form: when a bound restore attempt
// has actually launched a supervised child and then failed or was interrupted,
// the coordinator appends an append-only attempt_proof audit row carrying the
// authentic facts of that attempt — child start identity (pid + Linux start
// identity), sole-Wait terminal completion, process-group drain, the trusted
// role fingerprint, and a credential binding hash of the target role's SCRAM
// verifier captured at attempt time.
//
// A later controlled rebuild (RebuildTargetWithRetainedProof) refuses unless
// that proof exists, is complete, and still matches the live environment
// (same role credentials, complete session census). No proof, partial proof,
// or a manufactured witness after the fact can authorize DROP/CREATE or a
// clean guard transition. The proof is a bounded local mechanism: it does not
// replace the ADR-004 protected admission route, and production deployments
// without a privileged observer produce incomplete proofs, which stay
// refusing (fail-closed).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// ActionAttemptProof is the recovery_audit action of the retained
// original-attempt proof rows.
const ActionAttemptProof = "attempt_proof"

type attemptProofFacts struct {
	TargetGuardKey  string `json:"target_guard_key"`
	Application     string `json:"application_name"`
	ChildPID        int    `json:"child_pid"`
	ChildStartID    uint64 `json:"child_start_id"`
	WaitTerminal    bool   `json:"wait_terminal"`
	WaitExitCode    int    `json:"wait_exit_code"`
	ProcessDrained  bool   `json:"process_group_drained"`
	RoleFingerprint string `json:"role_fingerprint"`
	VerifierSHA256  string `json:"verifier_sha256"`
	Complete        bool   `json:"complete"`
}

// retainAttemptProof appends the attempt proof for a launched-and-failed
// bound restore. It is best-effort with a bounded observer query; an
// incomplete proof is recorded as such (result=refused) and can never
// authorize a rebuild. It never masks or replaces the caller's error.
func retainAttemptProof(ctx context.Context, opts TargetWriterOptions, key TargetKey, appName string, command PGCommandResult) {
	if opts.Store == nil || opts.Store.Pool() == nil || opts.InstanceID == "" || opts.OperationID == "" {
		return
	}
	snapshot := opts.Runner.observation.snapshot()
	facts := attemptProofFacts{
		TargetGuardKey:  key.String(),
		Application:     appName,
		ChildPID:        snapshot.pid,
		ChildStartID:    snapshot.startID,
		WaitTerminal:    snapshot.terminal && snapshot.childWaitCompleted,
		WaitExitCode:    snapshot.waitExitCode,
		ProcessDrained:  command.ProcessGroupDrained,
		RoleFingerprint: opts.TrustedTarget.DataTargetFingerprint().RoleFingerprint,
	}
	if verifier, err := captureRoleVerifierSHA256(ctx, opts.ObserverDSN, opts.TrustedTarget.Role); err == nil {
		facts.VerifierSHA256 = verifier
	}
	facts.Complete = facts.ChildPID > 0 && facts.ChildStartID > 0 && facts.WaitTerminal &&
		facts.ProcessDrained && facts.VerifierSHA256 != "" && facts.RoleFingerprint != ""
	result := controlstore.AuditRefused
	if facts.Complete {
		result = controlstore.AuditOK
	}
	targetJSON, _ := json.Marshal(map[string]any{"target_guard_key": facts.TargetGuardKey})
	detailJSON, _ := json.Marshal(facts)
	writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = controlstore.WriteAudit(writeCtx, opts.Store.Pool(), controlstore.AuditRecord{
		InstanceID:  opts.InstanceID,
		Actor:       "system:recovery-coordinator",
		Action:      ActionAttemptProof,
		Target:      targetJSON,
		Detail:      detailJSON,
		Result:      result,
		OperationID: opts.OperationID,
	})
}

// captureRoleVerifierSHA256 binds the proof to the role's credentials as they
// existed at attempt time: the observer reads the SCRAM verifier and only its
// SHA-256 is retained. Unreadable/unprivileged observers yield an incomplete
// proof, never a fabricated binding.
func captureRoleVerifierSHA256(ctx context.Context, observerDSN, role string) (string, error) {
	if strings.TrimSpace(observerDSN) == "" || strings.TrimSpace(role) == "" {
		return "", errors.New("observer and role are required for the credential binding")
	}
	readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := pgx.Connect(readCtx, observerDSN)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var verifier string
	if err := conn.QueryRow(readCtx, `SELECT COALESCE(rolpassword,'') FROM pg_authid WHERE rolname=$1`, role).Scan(&verifier); err != nil {
		return "", err
	}
	if verifier == "" {
		return "", errors.New("role has no verifier")
	}
	sum := sha256.Sum256([]byte(verifier))
	return hex.EncodeToString(sum[:]), nil
}

// RebuildOptions is one controlled rebuild driven by a retained
// original-attempt proof.
type RebuildOptions struct {
	Store          *controlstore.Store
	ControlDSN     string
	TargetDSN      string
	ObserverDSN    string
	AdminDSN       string
	TrustedTarget  controlstore.DSNTarget
	OldOperationID string
	NewOperationID string
	Actor          string
	LockTimeout    time.Duration
	CensusTimeout  time.Duration
}

// RebuildResult reports the controlled rebuild outcome.
type RebuildResult struct {
	Rebuilt      bool   `json:"rebuilt"`
	DatabaseOID  uint32 `json:"database_oid"`
	ProofAuditID int64  `json:"proof_audit_id"`
	GuardClean   bool   `json:"guard_clean"`
}

// RebuildTargetWithRetainedProof performs the controlled rebuild only when the
// retained original-attempt proof is present, complete and still matching the
// live environment. Every check is fail-closed; the guard transitions to clean
// only inside the control-store transaction that records the rebuild evidence.
func RebuildTargetWithRetainedProof(ctx context.Context, opts RebuildOptions) (RebuildResult, error) {
	var result RebuildResult
	if opts.Store == nil || opts.Store.Pool() == nil || opts.ControlDSN == "" || opts.TargetDSN == "" ||
		opts.ObserverDSN == "" || opts.AdminDSN == "" || opts.OldOperationID == "" || opts.NewOperationID == "" || opts.Actor == "" {
		return result, errors.New("witnessed rebuild requires store, DSNs, operations and actor")
	}
	key, err := CanonicalTargetKey(opts.TrustedTarget)
	if err != nil {
		return result, errors.New("witnessed rebuild target identity is invalid")
	}
	target, err := controlstore.ParseDSNTarget(opts.TargetDSN)
	if err != nil || target.Database == "" || target.Role == "" {
		return result, errors.New("witnessed rebuild target DSN is unusable")
	}
	if target.Database != opts.TrustedTarget.Database {
		return result, errors.New("witnessed rebuild target database does not match the trusted target")
	}
	lockTimeout := opts.LockTimeout
	if lockTimeout <= 0 {
		lockTimeout = 10 * time.Second
	}
	lock, err := AcquireTargetLock(ctx, opts.ControlDSN, key, lockTimeout, 50*time.Millisecond)
	if err != nil {
		return result, errors.New("witnessed rebuild could not acquire the target lock")
	}
	defer func() { _ = lock.Release(context.Background()) }()

	// 1) The guard must be the interrupted attempt's row: same operation,
	// attempt application recorded, writer inactive, disposition unresolved.
	guard, found, err := controlstore.ReadTargetGuard(ctx, opts.Store.Pool(), key.String())
	if err != nil || !found {
		return result, errors.New("witnessed rebuild requires the interrupted attempt guard row")
	}
	if guard.OperationID != opts.OldOperationID || guard.AttemptAppName == "" || guard.ActiveWriter {
		return result, errors.New("witnessed rebuild guard does not match the interrupted attempt")
	}
	if guard.State == controlstore.TargetGuardClean {
		return result, errors.New("witnessed rebuild refused: the target is already clean")
	}

	// 2) The retained proof must exist, be complete and match the attempt.
	proof, auditID, err := readRetainedAttemptProof(ctx, opts.Store.Pool(), key.String(), opts.OldOperationID, guard.AttemptAppName)
	if err != nil {
		return result, err
	}
	if !proof.Complete || proof.RoleFingerprint != opts.TrustedTarget.DataTargetFingerprint().RoleFingerprint {
		return result, errors.New("witnessed rebuild refused: retained attempt proof is incomplete or bound to another role")
	}
	result.ProofAuditID = auditID

	// 3) The live environment must still match the proof: role credentials
	// unchanged since the attempt, and a complete target session census.
	verifier, err := captureRoleVerifierSHA256(ctx, opts.ObserverDSN, opts.TrustedTarget.Role)
	if err != nil || verifier != proof.VerifierSHA256 {
		return result, errors.New("witnessed rebuild refused: role credentials changed since the interrupted attempt")
	}
	censusTimeout := opts.CensusTimeout
	if censusTimeout <= 0 {
		censusTimeout = 10 * time.Second
	}
	if err := requireEmptyTargetCensus(ctx, opts.ObserverDSN, target.Database, censusTimeout); err != nil {
		return result, err
	}

	// 4) Controlled replacement through the deployment-admin identity:
	// autocommit DROP (never FORCE) then CREATE with the original owner.
	adminURL, err := parseAdminURL(opts.AdminDSN)
	if err != nil {
		return result, err
	}
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return result, errors.New("witnessed rebuild could not open the deployment-admin connection")
	}
	defer func() { _ = admin.Close(context.Background()) }()
	var oldOID uint32
	if err := admin.QueryRow(ctx, `SELECT oid FROM pg_database WHERE datname=$1`, target.Database).Scan(&oldOID); err != nil {
		return result, errors.New("witnessed rebuild could not read the target database identity")
	}
	db := pgx.Identifier{target.Database}.Sanitize()
	if _, err := admin.Exec(ctx, "DROP DATABASE "+db); err != nil {
		return result, errors.New("witnessed rebuild DROP DATABASE was refused")
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+db+" OWNER "+pgx.Identifier{target.Role}.Sanitize()); err != nil {
		return result, errors.New("witnessed rebuild CREATE DATABASE was refused")
	}
	var newOID uint32
	if err := admin.QueryRow(ctx, `SELECT oid FROM pg_database WHERE datname=$1`, target.Database).Scan(&newOID); err != nil || newOID == oldOID {
		return result, errors.New("witnessed rebuild replacement database identity was not renewed")
	}
	result.DatabaseOID = newOID

	// 5) Record the rebuild evidence and the clean transition atomically.
	evidence, _ := json.Marshal(map[string]any{
		"rebuild":         "controlled",
		"old_operation":   opts.OldOperationID,
		"new_operation":   opts.NewOperationID,
		"proof_audit_id":  auditID,
		"child_pid":       proof.ChildPID,
		"child_start_id":  proof.ChildStartID,
		"wait_terminal":   proof.WaitTerminal,
		"process_drained": proof.ProcessDrained,
		"database_oid":    newOID,
	})
	tx, err := opts.Store.Pool().Begin(ctx)
	if err != nil {
		return result, errors.New("witnessed rebuild could not begin the guard transaction")
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := controlstore.RecordTargetGuardRebuild(ctx, tx, instanceIDForGuard(opts), key.String(), opts.Actor, opts.NewOperationID, evidence); err != nil {
		return result, errors.New("witnessed rebuild could not record the rebuild evidence")
	}
	if err := controlstore.ResolveTargetGuardClean(ctx, tx, key.String(), opts.NewOperationID, evidence); err != nil {
		return result, errors.New("witnessed rebuild could not record the clean guard transition")
	}
	if err := tx.Commit(ctx); err != nil {
		return result, errors.New("witnessed rebuild guard transaction was not committed")
	}
	result.Rebuilt = true
	result.GuardClean = true
	return result, nil
}

// instanceIDForGuard reads the open instance bound to this target, when any.
func instanceIDForGuard(opts RebuildOptions) string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var id string
	_ = opts.Store.Pool().QueryRow(ctx,
		`SELECT instance_id::text FROM recovery_instance WHERE state='open' ORDER BY opened_at DESC, instance_id DESC LIMIT 1`).Scan(&id)
	return id
}

func parseAdminURL(dsn string) (string, error) {
	if !strings.HasPrefix(dsn, "postgres://") && !strings.HasPrefix(dsn, "postgresql://") {
		return "", errors.New("witnessed rebuild admin DSN must be a postgres URI")
	}
	if strings.Contains(dsn, "dbname=") {
		return "", errors.New("witnessed rebuild admin DSN must be a postgres URI")
	}
	return dsn, nil
}

// requireEmptyTargetCensus refuses unless the observer proves the target
// database has no remaining sessions (complete census; no FORCE semantics).
func requireEmptyTargetCensus(ctx context.Context, observerDSN, database string, timeout time.Duration) error {
	readCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := pgx.Connect(readCtx, observerDSN)
	if err != nil {
		return errors.New("witnessed rebuild census observer is unavailable")
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var n int
	if err := conn.QueryRow(readCtx,
		`SELECT count(*) FROM pg_stat_activity WHERE datname=$1`, database).Scan(&n); err != nil {
		return errors.New("witnessed rebuild census could not be proven")
	}
	if n != 0 {
		return fmt.Errorf("witnessed rebuild refused: %d session(s) remain on the target database", n)
	}
	return nil
}

// readRetainedAttemptProof loads the complete proof row for the attempt.
func readRetainedAttemptProof(ctx context.Context, q controlstore.Queryer, key, operationID, appName string) (attemptProofFacts, int64, error) {
	var (
		facts   attemptProofFacts
		auditID int64
		detail  []byte
		result  string
	)
	err := q.QueryRow(ctx, `
SELECT audit_id, COALESCE(detail,'{}'::jsonb), result
FROM recovery_audit
WHERE action=$1 AND operation_id=$2 AND target->>'target_guard_key'=$3
ORDER BY audit_id DESC LIMIT 1`, ActionAttemptProof, operationID, key).Scan(&auditID, &detail, &result)
	if err != nil {
		return facts, 0, errors.New("witnessed rebuild refused: no retained attempt proof exists for the interrupted attempt")
	}
	if result != controlstore.AuditOK {
		return facts, auditID, errors.New("witnessed rebuild refused: the retained attempt proof is incomplete")
	}
	if err := json.Unmarshal(detail, &facts); err != nil {
		return facts, auditID, errors.New("witnessed rebuild refused: the retained attempt proof is unreadable")
	}
	if facts.Application != appName {
		return facts, auditID, errors.New("witnessed rebuild refused: the retained attempt proof is bound to another application identity")
	}
	return facts, auditID, nil
}
