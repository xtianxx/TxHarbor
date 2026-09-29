package controlstore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Target guard states. A rebuild is never cleared implicitly.
const (
	TargetGuardUnknown         = "unknown"
	TargetGuardRebuildRequired = "rebuild_required"
	TargetGuardClean           = "clean"
	ActionTargetGuardRebuild   = "target_guard_rebuild"
)

var sha256FingerprintPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// TargetGuardKey returns the canonical, role-independent endpoint key shared
// with recovery.CanonicalTargetKey. Credentials, role, and DSN options never
// participate in this durable or advisory-lock identity.
func TargetGuardKey(target DSNTarget) (string, error) {
	if strings.TrimSpace(target.Host) == "" || target.Port == 0 || target.Database == "" || strings.ContainsAny(target.Host+target.Database, "\x00\r\n") {
		return "", errors.New("target host, numeric port, and database are required")
	}
	h := sha256.New()
	h.Write([]byte("txharbor-target-lock-v1\x00"))
	writePart := func(p []byte) {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(p)))
		h.Write(n[:])
		h.Write(p)
	}
	writePart([]byte(strings.ToLower(target.Host)))
	var port [2]byte
	binary.BigEndian.PutUint16(port[:], target.Port)
	writePart(port[:])
	writePart([]byte(target.Database))
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// TargetGuard is the current durable attempt state for one target identity.
type TargetGuard struct {
	Key               string
	State             string
	ActiveWriter      bool
	LaunchIntent      bool
	AttemptAppName    string
	OperationID       string
	PreparedAt        time.Time
	LaunchIntentAt    *time.Time
	LaunchedAt        *time.Time
	RebuildRequiredAt *time.Time
	CleanAt           *time.Time
	RebuildEvidence   []byte
}

// InitializeTargetGuard creates explicit unknown inventory. A missing row is
// never interpreted as clean.
func InitializeTargetGuard(ctx context.Context, tx pgx.Tx, key, operationID string) error {
	if tx == nil {
		return errors.New("target guard requires a transaction")
	}
	if !sha256FingerprintPattern.MatchString(key) {
		return errors.New("target guard key must be a full sha256 fingerprint")
	}
	operationID, err := normalizeOperationID(operationID)
	if err != nil {
		return err
	}
	if err := lockTargetGuard(ctx, tx, key); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO recovery_target_guard (target_guard_key, disposition, operation_id)
VALUES ($1, 'unknown', $2)`, key, operationID)
	if err != nil {
		return fmt.Errorf("initialize target guard: %w", err)
	}
	return nil
}

// PrepareTargetGuard is a legacy clean-row transition that does not fence the
// prior attempt identity. Coordinators should use PrepareTargetGuardForAttempt
// so a delayed caller cannot invalidate a newer clean result.
func PrepareTargetGuard(ctx context.Context, tx pgx.Tx, key, operationID string) error {
	if tx == nil {
		return errors.New("target guard requires a transaction")
	}
	if !sha256FingerprintPattern.MatchString(key) {
		return errors.New("target guard key must be a full sha256 fingerprint")
	}
	operationID, err := normalizeOperationID(operationID)
	if err != nil {
		return err
	}
	if err := lockTargetGuard(ctx, tx, key); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE recovery_target_guard
SET disposition = 'unknown', active_writer = FALSE, launch_intent = FALSE,
    attempt_app_name = NULL, launch_intent_at = NULL, launched_at = NULL,
    rebuild_required_at = NULL, clean_at = NULL, rebuild_evidence = NULL,
    operation_id = $2, prepared_at = now()
WHERE target_guard_key = $1 AND disposition = 'clean' AND NOT active_writer`, key, operationID)
	if err != nil {
		return fmt.Errorf("prepare target guard: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("target guard missing or not clean; refusing attempt")
	}
	return nil
}

// PrepareTargetGuardForAttempt starts an ordinary attempt from a positively
// clean row only if the previously accepted operation and app name still match.
// expectedAppName may be empty for a clean baseline that has no attempt app.
// This identity fence prevents delayed cleanup from invalidating a newer
// attempt's clean result.
func PrepareTargetGuardForAttempt(ctx context.Context, tx pgx.Tx, key, expectedOperationID, expectedAppName, operationID string) error {
	var err error
	expectedOperationID, err = normalizeOperationID(expectedOperationID)
	if err != nil {
		return err
	}
	if expectedAppName != "" {
		expectedAppName, err = normalizeIdentityText(expectedAppName, "expected attempt app name", 128)
		if err != nil {
			return err
		}
	}
	operationID, err = normalizeOperationID(operationID)
	if err != nil {
		return err
	}
	if err := lockTargetGuard(ctx, tx, key); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE recovery_target_guard
SET disposition = 'unknown', active_writer = FALSE, launch_intent = FALSE,
    attempt_app_name = NULL, launch_intent_at = NULL, launched_at = NULL,
    rebuild_required_at = NULL, clean_at = NULL, rebuild_evidence = NULL,
    operation_id = $4, prepared_at = now()
WHERE target_guard_key = $1 AND operation_id = $2
  AND COALESCE(attempt_app_name, '') = $3 AND disposition = 'clean' AND NOT active_writer`,
		key, expectedOperationID, expectedAppName, operationID)
	if err != nil {
		return fmt.Errorf("prepare target guard for attempt: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("target guard missing, previous attempt identity does not match, or target is not clean")
	}
	return nil
}

// MarkTargetGuardLaunchIntent records the unique app name before external
// launch is attempted. The operation must match the operation that prepared
// this guard; this prevents a stale or cross-attempt caller from taking over an
// unlaunched row. This is the durable boundary between a provably prelaunch
// failure and an attempt that may have mutated the target.
func MarkTargetGuardLaunchIntent(ctx context.Context, tx pgx.Tx, key, appName, operationID string) error {
	appName, err := normalizeIdentityText(appName, "attempt app name", 128)
	if err != nil {
		return err
	}
	operationID, err = normalizeOperationID(operationID)
	if err != nil {
		return err
	}
	if err := lockTargetGuard(ctx, tx, key); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE recovery_target_guard
SET launch_intent = TRUE, active_writer = TRUE, attempt_app_name = $2, launch_intent_at = now()
WHERE target_guard_key = $1 AND operation_id = $3 AND disposition = 'unknown'
  AND NOT active_writer AND NOT launch_intent AND attempt_app_name IS NULL
  AND launch_intent_at IS NULL AND launched_at IS NULL
  AND rebuild_required_at IS NULL AND clean_at IS NULL`, key, appName, operationID)
	if err != nil {
		return fmt.Errorf("mark target launch intent: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("target guard is missing or not in an unlaunched prepared state")
	}
	return nil
}

// MarkTargetGuardLaunched records a launch observation only; it is not a child
// success signal. This key-only compatibility method is unsafe for a
// coordinator: delayed calls can affect a later attempt. Use
// MarkTargetGuardLaunchedForAttempt instead.
func MarkTargetGuardLaunched(ctx context.Context, tx pgx.Tx, key string) error {
	if err := lockTargetGuard(ctx, tx, key); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE recovery_target_guard
SET launched_at = now()
WHERE target_guard_key = $1 AND disposition = 'unknown' AND launch_intent AND launched_at IS NULL`, key)
	if err != nil {
		return fmt.Errorf("mark target launched: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("target guard has no launch intent or is not prepared")
	}
	return nil
}

// MarkTargetGuardLaunchedForAttempt records launch only if both attempt
// identity fields still match the guard. Durable intent already blocks
// successors if this crashes.
func MarkTargetGuardLaunchedForAttempt(ctx context.Context, tx pgx.Tx, key, operationID, appName string) error {
	return updateTargetGuardForAttempt(ctx, tx, key, operationID, appName,
		`UPDATE recovery_target_guard SET launched_at = now()
WHERE target_guard_key = $1 AND operation_id = $2 AND attempt_app_name = $3
  AND disposition = 'unknown' AND launch_intent AND active_writer AND launched_at IS NULL`,
		"mark target launched")
}

// MarkTargetGuardWriterDrained updates liveness separately from disposition.
// This key-only compatibility method is unsafe for a coordinator; use
// MarkTargetGuardWriterDrainedForAttempt instead.
func MarkTargetGuardWriterDrained(ctx context.Context, tx pgx.Tx, key string) error {
	if err := lockTargetGuard(ctx, tx, key); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE recovery_target_guard SET active_writer = FALSE
WHERE target_guard_key = $1 AND launch_intent`, key)
	if err != nil {
		return fmt.Errorf("mark target writer drained: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("target guard launch intent is missing")
	}
	return nil
}

// MarkTargetGuardWriterDrainedForAttempt updates liveness only for the matching
// attempt. Disposition and launch intent are checked to reject stale cleanup.
func MarkTargetGuardWriterDrainedForAttempt(ctx context.Context, tx pgx.Tx, key, operationID, appName string) error {
	return updateTargetGuardForAttempt(ctx, tx, key, operationID, appName,
		`UPDATE recovery_target_guard SET active_writer = FALSE
WHERE target_guard_key = $1 AND operation_id = $2 AND attempt_app_name = $3
  AND disposition = 'unknown' AND launch_intent AND active_writer`,
		"mark target writer drained")
}

// MarkTargetGuardRebuildRequired records a post-launch failure. It can be
// called from launch-intent onward, including when launch outcome is unknown.
// This key-only compatibility method is unsafe for a coordinator; use
// MarkTargetGuardRebuildRequiredForAttempt instead.
func MarkTargetGuardRebuildRequired(ctx context.Context, tx pgx.Tx, key string) error {
	if err := lockTargetGuard(ctx, tx, key); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE recovery_target_guard
SET disposition = 'rebuild_required', rebuild_required_at = now(), rebuild_evidence = NULL
WHERE target_guard_key = $1 AND disposition = 'unknown' AND launch_intent`, key)
	if err != nil {
		return fmt.Errorf("mark target rebuild required: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("target guard is missing or no launch intent was recorded")
	}
	return nil
}

// MarkTargetGuardRebuildRequiredForAttempt records failure only for the
// matching attempt. Launch intent must still be present, while launched_at is
// intentionally not required because launch outcome may be unknown.
func MarkTargetGuardRebuildRequiredForAttempt(ctx context.Context, tx pgx.Tx, key, operationID, appName string) error {
	return updateTargetGuardForAttempt(ctx, tx, key, operationID, appName,
		`UPDATE recovery_target_guard
SET disposition = 'rebuild_required', rebuild_required_at = now(), rebuild_evidence = NULL
WHERE target_guard_key = $1 AND operation_id = $2 AND attempt_app_name = $3
  AND disposition = 'unknown' AND launch_intent`,
		"mark target rebuild required")
}

// AcceptTargetGuardClean is a legacy key-only compatibility method and is
// unsafe for a coordinator because it does not fence against a later attempt.
// Use AcceptTargetGuardCleanForAttempt. The execution coordinator must prove
// success and drain first; this storage transition does not assert that the
// child succeeded.
func AcceptTargetGuardClean(ctx context.Context, tx pgx.Tx, key, operationID string, evidence []byte) error {
	return resolveTargetGuardClean(ctx, tx, key, operationID, evidence, TargetGuardUnknown, true)
}

// AcceptTargetGuardCleanForAttempt atomically accepts successful-operation
// evidence only while the same operation and app name remain the active guard
// identity. The caller must prove success and drain first. Unlike controlled
// baseline/rebuild resolution, this is strictly an ordinary-attempt transition.
func AcceptTargetGuardCleanForAttempt(ctx context.Context, tx pgx.Tx, key, operationID, appName string, evidence []byte) error {
	if len(evidence) == 0 {
		return errors.New("rebuild evidence is required")
	}
	if !json.Valid(evidence) {
		return errors.New("invalid rebuild evidence: must be valid JSON")
	}
	operationID, err := normalizeOperationID(operationID)
	if err != nil {
		return err
	}
	appName, err = normalizeIdentityText(appName, "attempt app name", 128)
	if err != nil {
		return err
	}
	if err := lockTargetGuard(ctx, tx, key); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE recovery_target_guard
SET disposition = 'clean', clean_at = now(), rebuild_evidence = $4::jsonb
WHERE target_guard_key = $1 AND operation_id = $2 AND attempt_app_name = $3
  AND disposition = 'unknown' AND launch_intent AND launched_at IS NOT NULL AND NOT active_writer`,
		key, operationID, appName, evidence)
	if err != nil {
		return fmt.Errorf("accept target guard clean for attempt: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("target guard is missing, attempt identity does not match, writer-active, or not eligible for clean acceptance")
	}
	return nil
}

func updateTargetGuardForAttempt(ctx context.Context, tx pgx.Tx, key, operationID, appName, query, label string) error {
	operationID, err := normalizeOperationID(operationID)
	if err != nil {
		return err
	}
	appName, err = normalizeIdentityText(appName, "attempt app name", 128)
	if err != nil {
		return err
	}
	if err := lockTargetGuard(ctx, tx, key); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, query, key, operationID, appName)
	if err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("target guard is missing, attempt identity does not match, or transition is not eligible")
	}
	return nil
}

// ResolveTargetGuardClean is reserved for controlled, audited baseline/rebuild
// re-entry. Audit, positive completion evidence, and this transition belong in
// the caller's same transaction.
func ResolveTargetGuardClean(ctx context.Context, tx pgx.Tx, key, operationID string, evidence []byte) error {
	return resolveTargetGuardClean(ctx, tx, key, operationID, evidence, "controlled", false)
}

func resolveTargetGuardClean(ctx context.Context, tx pgx.Tx, key, operationID string, evidence []byte, from string, attempt bool) error {
	if len(evidence) == 0 {
		return errors.New("rebuild evidence is required")
	}
	if !json.Valid(evidence) {
		return errors.New("invalid rebuild evidence: must be valid JSON")
	}
	operationID, err := normalizeOperationID(operationID)
	if err != nil {
		return err
	}
	if err := lockTargetGuard(ctx, tx, key); err != nil {
		return err
	}
	query := `UPDATE recovery_target_guard
SET disposition = 'clean', clean_at = now(), rebuild_evidence = $3::jsonb, operation_id = $2
WHERE target_guard_key = $1 AND NOT active_writer AND
      (($4 = 'controlled' AND disposition IN ('unknown', 'rebuild_required')) OR disposition = $4)`
	if attempt {
		query += ` AND launch_intent AND launched_at IS NOT NULL`
	}
	result, err := tx.Exec(ctx, query, key, operationID, evidence, from)
	if err != nil {
		return fmt.Errorf("resolve target guard clean: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("target guard is missing, writer-active, or not eligible for controlled resolution")
	}
	return nil
}

// RequireCleanTargetGuard verifies a key is present, matches the immutable
// instance binding and has no unresolved launch/rebuild state. Callers should
// invoke it in their locked transaction before any target mutation.
func RequireCleanTargetGuard(ctx context.Context, q Queryer, instanceID, key, roleFingerprint string) error {
	if !sha256FingerprintPattern.MatchString(key) || !sha256FingerprintPattern.MatchString(roleFingerprint) {
		return errors.New("target guard key and role fingerprint must be full sha256 fingerprints")
	}
	var state string
	var activeWriter bool
	err := q.QueryRow(ctx, `SELECT g.disposition, g.active_writer
FROM recovery_instance i
JOIN recovery_target_guard g ON g.target_guard_key = i.target_guard_key
WHERE i.instance_id = $1 AND i.target_guard_key = $2
  AND i.target_role_fingerprint = $3`, instanceID, key, roleFingerprint).Scan(&state, &activeWriter)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("target guard binding is missing; refusing target mutation")
	}
	if err != nil {
		return fmt.Errorf("read target guard: %w", err)
	}
	if state != TargetGuardClean || activeWriter {
		return fmt.Errorf("target guard state %q is not clean; refusing target mutation", state)
	}
	return nil
}

// RecordTargetGuardRebuild is the controlled-rebuild audit hook. It is
// deliberately separate from ResolveTargetGuardClean and never changes guard
// state; callers must record the actual evidence and then resolve in that same
// transaction.
func RecordTargetGuardRebuild(ctx context.Context, tx pgx.Tx, instanceID, key, actor, operationID string, evidence []byte) error {
	if !json.Valid(evidence) {
		return errors.New("invalid rebuild evidence: must be valid JSON")
	}
	if !sha256FingerprintPattern.MatchString(key) {
		return errors.New("target guard key must be a full sha256 fingerprint")
	}
	actor, err := normalizeIdentityText(actor, "audit actor", 256)
	if err != nil {
		return err
	}
	operationID, err = normalizeOperationID(operationID)
	if err != nil {
		return err
	}
	return WriteAudit(ctx, tx, AuditRecord{
		InstanceID: instanceID, Actor: actor, Action: ActionTargetGuardRebuild,
		Target: mustJSON(map[string]string{"target_guard_key": key}), Detail: evidence,
		Result: AuditOK, OperationID: operationID,
	})
}

// ReadTargetGuard fetches one guard row; it is safe to call through a locked tx.
func ReadTargetGuard(ctx context.Context, q Queryer, key string) (TargetGuard, bool, error) {
	var g TargetGuard
	err := q.QueryRow(ctx, `SELECT target_guard_key, disposition, active_writer, launch_intent,
COALESCE(attempt_app_name, ''), operation_id, prepared_at, launch_intent_at,
launched_at, rebuild_required_at, clean_at, rebuild_evidence
FROM recovery_target_guard WHERE target_guard_key = $1`, key).Scan(
		&g.Key, &g.State, &g.ActiveWriter, &g.LaunchIntent, &g.AttemptAppName, &g.OperationID,
		&g.PreparedAt, &g.LaunchIntentAt, &g.LaunchedAt, &g.RebuildRequiredAt,
		&g.CleanAt, &g.RebuildEvidence)
	if errors.Is(err, pgx.ErrNoRows) {
		return TargetGuard{}, false, nil
	}
	if err != nil {
		return TargetGuard{}, false, fmt.Errorf("read target guard: %w", err)
	}
	return g, true, nil
}

func lockTargetGuard(ctx context.Context, tx pgx.Tx, key string) error {
	if tx == nil {
		return errors.New("target guard requires a transaction")
	}
	if !sha256FingerprintPattern.MatchString(key) {
		return errors.New("target guard key must be a full sha256 fingerprint")
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "target_guard:"+key); err != nil {
		return fmt.Errorf("lock target guard: %w", err)
	}
	return nil
}
