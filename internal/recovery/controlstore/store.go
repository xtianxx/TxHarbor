// Package controlstore is the 015 recovery control store: the embedded,
// independently versioned control-store schema (schema/) plus the single
// append-only decision/audit read/write entry point used by every recovery
// command (T007) and the schema version guard that refuses unknown or
// incompatible control stores (T069).
//
// Trust boundary (T008, ADR-001, data-model.md §1):
//
//   - The control store is an independent PostgreSQL database reached through
//     TXHARBOR_RECOVERY_CONTROL_DSN. It is never part of the data-DB backup or
//     restore set, and 015 makes zero data-DB schema changes.
//   - It is not a financial source of truth: it holds recovery governance
//     facts only (instances, people, evidence, verification items, gaps,
//     isolation checks, approvals, releases, audit, drill metrics). No
//     amount/money column exists in this model.
//   - Plaintext DSNs never reach persisted rows: data_target carries
//     credential-free fingerprints (database/role/target hashes) and
//     OpenInstance refuses a data_target that embeds a DSN or credential.
//   - This store is the only decision read/write path. Decision writes are
//     serialized per instance by the instance row lock (SELECT ... FOR
//     UPDATE), and append-only decision reads derive the current decision by
//     commit order — the audit BIGSERIAL allocation written in the same
//     transaction — never by created_at. A decision row without its paired
//     audit row cannot be ordered and makes reads refuse (fail-closed), so a
//     direct row write can never be read back as an active release.
//   - No writable "released" boolean exists anywhere (INV-2): this package
//     stores append-only release/revoke rows and the gate (T012) re-derives
//     validity on every evaluation.
//   - Schema version unknown/incompatible refuses (INV-11, T069): NewStore
//     refuses to hand out a store over an unknown or incompatible control
//     store, expressed as refusal class control_store_unavailable with the
//     observed version audited when the audit sink exists. There is no
//     best-effort read path and no downgrade path.
package controlstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
)

// ControlVersionTable is the goose bookkeeping table of the control store.
// The control store owns its own version sequence; the data DB's
// goose_db_version table lives in a different database and is never touched.
const ControlVersionTable = "goose_db_version"

// EmptyEvidenceHash is the evidence-snapshot aggregate hash of an instance
// that has no accepted evidence yet (SHA-256 over the empty string). It is
// not a proof of anything: any accepted evidence write refreshes it together
// with evidence_generation (data-model §5).
const EmptyEvidenceHash = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// RefusalControlStoreUnavailable is the closed-set refusal class used when
// the control store cannot be read, is at an unknown/incompatible schema
// version, or is otherwise unusable. It is expressed (and audited) without
// adding any allow path (data-model §3.3, INV-11).
const RefusalControlStoreUnavailable = "control_store_unavailable"

// Audit action names written by this package. Decision writes pair each
// append-only decision row with an audit row carrying the same operation_id
// and one of these actions; that pairing is the commit-order anchor used by
// the decision reads (see appendOnlyOrder).
const (
	ActionInstanceOpen     = "instance_open"
	ActionReleaseDecision  = "release_decision"
	ActionApprovalDecision = "approval_decision"
	ActionStoreConnect     = "store_connect"
)

// controlTableNames are the recovery_* entities of data-model.md §1 plus the
// durable target-guard inventory introduced by control-store schema 0003. They
// are used by the version guard to distinguish "pristine empty database"
// (bootstrap allowed) from "recovery objects without a version table"
// (unknown state, refused).
var controlTableNames = []string{
	"recovery_instance",
	"recovery_identity",
	"recovery_participant",
	"recovery_evidence",
	"recovery_verification_item",
	"recovery_gap",
	"recovery_isolation_check",
	"recovery_approval",
	"recovery_release",
	"recovery_audit",
	"recovery_drill_run",
	"recovery_target_guard",
}

// ControlTableNames returns all recovery-owned table names, including the
// target-guard inventory introduced by schema 0003.
// The list is informational (version guard, status and tests); it is never a
// substitute for the version check.
func ControlTableNames() []string {
	return slices.Clone(controlTableNames)
}

// Queryer is the PostgreSQL surface shared by *pgxpool.Pool and pgx.Tx, so
// every store operation can run either standalone or inside the instance
// row-lock transaction.
type Queryer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// ---------------------------------------------------------------------------
// Schema version guard (T069 / INV-11)
// ---------------------------------------------------------------------------

// SchemaState is a read-only snapshot of the control-store schema version and
// object landscape. It never changes anything.
type SchemaState struct {
	// VersionTable reports whether the control store's goose version table
	// exists (and is visible on the current search_path).
	VersionTable bool
	// Current is the highest applied migration version (0 when none).
	Current int64
	// Applied lists the applied migration versions in ascending order.
	Applied []int64
	// Unknown lists applied versions this binary does not know.
	Unknown []int64
	// Pending lists embedded versions not yet applied.
	Pending []int64
	// Target is the highest version this binary embeds.
	Target int64
	// Known lists every embedded version.
	Known []int64
	// RecoveryObjects lists visible recovery_* tables that exist.
	RecoveryObjects []string
}

// InspectSchema reads the control-store version table and object landscape.
// It is strictly read-only and never creates the version table.
func InspectSchema(ctx context.Context, q Queryer) (SchemaState, error) {
	if q == nil {
		return SchemaState{}, errors.New("control-store schema inspection requires a database handle")
	}
	files, err := db.MigrationFiles(schema.FS)
	if err != nil {
		return SchemaState{}, fmt.Errorf("read embedded control-store migrations: %w", err)
	}
	state := SchemaState{Known: make([]int64, 0, len(files))}
	for _, f := range files {
		state.Known = append(state.Known, f.Version)
	}
	state.Target = files[len(files)-1].Version
	known := make(map[int64]bool, len(state.Known))
	for _, v := range state.Known {
		known[v] = true
	}

	if err := q.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", ControlVersionTable).Scan(&state.VersionTable); err != nil {
		return SchemaState{}, fmt.Errorf("check control-store version table: %w", err)
	}
	applied := make(map[int64]bool)
	if state.VersionTable {
		rows, err := q.Query(ctx, "SELECT version_id FROM "+ControlVersionTable+
			" WHERE is_applied AND version_id > 0 ORDER BY version_id")
		if err != nil {
			return SchemaState{}, fmt.Errorf("read applied control-store migrations: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var v int64
			if err := rows.Scan(&v); err != nil {
				return SchemaState{}, fmt.Errorf("scan applied control-store migration: %w", err)
			}
			applied[v] = true
			state.Applied = append(state.Applied, v)
		}
		if err := rows.Err(); err != nil {
			return SchemaState{}, fmt.Errorf("read applied control-store migrations: %w", err)
		}
	}
	if len(state.Applied) > 0 {
		state.Current = state.Applied[len(state.Applied)-1]
	}
	for _, v := range state.Applied {
		if !known[v] {
			state.Unknown = append(state.Unknown, v)
		}
	}
	for _, f := range files {
		if !applied[f.Version] {
			state.Pending = append(state.Pending, f.Version)
		}
	}

	rows, err := q.Query(ctx,
		`SELECT name FROM unnest($1::text[]) AS name WHERE to_regclass(name) IS NOT NULL ORDER BY name`,
		controlTableNames)
	if err != nil {
		return SchemaState{}, fmt.Errorf("inspect control-store objects: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return SchemaState{}, fmt.Errorf("scan control-store object: %w", err)
		}
		state.RecoveryObjects = append(state.RecoveryObjects, name)
	}
	if err := rows.Err(); err != nil {
		return SchemaState{}, fmt.Errorf("inspect control-store objects: %w", err)
	}
	return state, nil
}

// SchemaError is the typed fail-closed refusal of an unknown or incompatible
// control-store schema version. Its RefusalClass is always
// RefusalControlStoreUnavailable; callers map it to the closed refusal set and
// audit the observed version (T069).
type SchemaError struct {
	RefusalClass    string
	Reason          string
	ObservedVersion int64
	TargetVersion   int64
	VersionTable    bool
	UnknownVersions []int64
	RecoveryObjects []string
}

// Error renders the refusal with the observed/target version. It carries no
// credential material and no DSN.
func (e *SchemaError) Error() string {
	if e == nil {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s [observed_version=%d target_version=%d version_table=%t",
		e.RefusalClass, e.Reason, e.ObservedVersion, e.TargetVersion, e.VersionTable)
	if len(e.UnknownVersions) > 0 {
		fmt.Fprintf(&b, " unknown_versions=%v", e.UnknownVersions)
	}
	if len(e.RecoveryObjects) > 0 {
		fmt.Fprintf(&b, " recovery_objects=%v", e.RecoveryObjects)
	}
	b.WriteString("]")
	return b.String()
}

// IsControlStoreUnavailable reports whether err is a schema/version refusal.
func IsControlStoreUnavailable(err error) bool {
	var se *SchemaError
	return errors.As(err, &se) && se.RefusalClass == RefusalControlStoreUnavailable
}

func newSchemaError(state SchemaState, reason string) *SchemaError {
	return &SchemaError{
		RefusalClass:    RefusalControlStoreUnavailable,
		Reason:          reason,
		ObservedVersion: state.Current,
		TargetVersion:   state.Target,
		VersionTable:    state.VersionTable,
		UnknownVersions: slices.Clone(state.Unknown),
		RecoveryObjects: slices.Clone(state.RecoveryObjects),
	}
}

// CheckCompatible is the store-read guard: the control store must be exactly
// at this binary's embedded schema version. Anything else — missing version
// table, unknown/never-seen versions, a newer-than-known version, or pending
// migrations — refuses; there is no best-effort read and no downgrade.
func (s SchemaState) CheckCompatible() error {
	if s.Target == 0 {
		return newSchemaError(s, "this binary embeds no control-store migrations")
	}
	if !s.VersionTable {
		return newSchemaError(s, "control-store schema version table is missing")
	}
	if len(s.Unknown) > 0 {
		return newSchemaError(s, fmt.Sprintf(
			"control-store schema version %d is unknown to this binary", s.Unknown[0]))
	}
	if s.Current > s.Target {
		return newSchemaError(s, fmt.Sprintf(
			"control-store schema version %d is newer than this binary expects (%d)", s.Current, s.Target))
	}
	if s.Current != s.Target || len(s.Pending) > 0 {
		return newSchemaError(s, fmt.Sprintf(
			"control-store schema is not at this binary's version (current=%d target=%d pending=%d); run \"recovery-admin migrate up\" first",
			s.Current, s.Target, len(s.Pending)))
	}
	return nil
}

// CheckMigratable is the migrate guard: applying migrations is allowed from
// a pristine empty database and from a known applied prefix; anything unknown
// — unknown/incompatible versions, a database newer than this binary, or
// recovery objects without a version table — refuses before any migration
// runs (never a silent downgrade, never a best-effort read).
func (s SchemaState) CheckMigratable() error {
	if s.Target == 0 {
		return newSchemaError(s, "this binary embeds no control-store migrations")
	}
	if len(s.Unknown) > 0 {
		return newSchemaError(s, fmt.Sprintf(
			"control-store schema version %d is unknown to this binary", s.Unknown[0]))
	}
	if s.Current > s.Target {
		return newSchemaError(s, fmt.Sprintf(
			"control-store schema version %d is newer than this binary expects (%d); refusing to continue",
			s.Current, s.Target))
	}
	if !s.VersionTable && len(s.RecoveryObjects) > 0 {
		return newSchemaError(s, fmt.Sprintf(
			"recovery objects %v exist without a %s version table; refusing to continue on an unknown state",
			s.RecoveryObjects, ControlVersionTable))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

// Store is the control-store decision and audit entry point over a pool.
// The pool remains owned by the caller.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore opens the decision entry point only over a compatible control
// store: the schema version guard runs first, and an unknown/incompatible
// version refuses with a *SchemaError (control_store_unavailable) instead of
// handing out a store. The refusal is annotated with the observed version in
// the audit sink when that sink exists; the refusal itself never depends on
// the annotation write.
func NewStore(ctx context.Context, pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, errors.New("control-store pool is required")
	}
	state, err := InspectSchema(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("inspect control-store schema: %w", err)
	}
	if err := state.CheckCompatible(); err != nil {
		recordStoreConnectRefusal(ctx, pool, state, err)
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Pool returns the underlying pool (read-only helpers and tests).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// recordStoreConnectRefusal is the best-effort version annotation of a store
// refusal: it writes one recovery_audit refusal row (control_store_unavailable)
// with the observed version when the audit table exists under the observed
// (known) shape. A write failure is ignored — the refusal is already
// fail-closed without it.
func recordStoreConnectRefusal(ctx context.Context, pool *pgxpool.Pool, state SchemaState, cause error) {
	if !slices.Contains(state.RecoveryObjects, "recovery_audit") {
		return
	}
	detail, err := json.Marshal(map[string]any{
		"observed_version": state.Current,
		"target_version":   state.Target,
		"unknown_versions": state.Unknown,
		"pending_versions": state.Pending,
		"version_table":    state.VersionTable,
		"reason":           cause.Error(),
	})
	if err != nil {
		return
	}
	_ = WriteAudit(ctx, pool, AuditRecord{
		Actor:        "system:controlstore",
		Action:       ActionStoreConnect,
		Detail:       detail,
		Result:       AuditRefused,
		RefusalClass: RefusalControlStoreUnavailable,
	})
}

// ---------------------------------------------------------------------------
// Instance row lock
// ---------------------------------------------------------------------------

// InstanceToken is the authoritative instance snapshot read under the row
// lock. It is the generation token of data-model §5: callers re-read it under
// SELECT ... FOR UPDATE and validate (state, evidence_generation,
// evidence_hash) before accepting any write.
type InstanceToken struct {
	InstanceID            string
	Kind                  string
	State                 string
	EvidenceGeneration    int64
	EvidenceHash          string
	SupersedesInstanceID  string
	OpenedBy              string
	OpenedAt              time.Time
	RestorePoint          []byte
	DataTarget            []byte
	EntryChainInventory   []uint64
	EntryChainVersion     int32
	EntryChainBound       bool
	TargetGuardKey        string
	TargetRoleFingerprint string
}

// LockInstance locks the recovery instance row FOR UPDATE inside tx and
// returns the authoritative token. The same lock serializes every decision
// write for the instance, so commit order equals lock order (data-model §5);
// decision reads taken inside this lock see a stable, complete history.
func LockInstance(ctx context.Context, tx pgx.Tx, instanceID string) (InstanceToken, error) {
	if tx == nil {
		return InstanceToken{}, errors.New("instance row lock requires a transaction")
	}
	id, err := normalizeUUID(instanceID, "instance_id")
	if err != nil {
		return InstanceToken{}, err
	}
	const sql = `
SELECT instance_id::text, kind, state, evidence_generation, evidence_hash,
       COALESCE(supersedes_instance_id::text, ''), opened_by, opened_at,
       restore_point, data_target, entry_chain_inventory, entry_chain_inventory_version,
       target_guard_key, target_role_fingerprint
FROM recovery_instance
WHERE instance_id = $1
FOR UPDATE`
	var token InstanceToken
	var entryChainVersion *int32
	var targetGuardKey, targetRoleFingerprint *string
	err = tx.QueryRow(ctx, sql, id).Scan(
		&token.InstanceID, &token.Kind, &token.State, &token.EvidenceGeneration,
		&token.EvidenceHash, &token.SupersedesInstanceID, &token.OpenedBy,
		&token.OpenedAt, &token.RestorePoint, &token.DataTarget,
		&token.EntryChainInventory, &entryChainVersion, &targetGuardKey, &targetRoleFingerprint)
	if errors.Is(err, pgx.ErrNoRows) {
		return InstanceToken{}, fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
	}
	if err != nil {
		return InstanceToken{}, fmt.Errorf("lock recovery instance %s: %w", id, err)
	}
	if entryChainVersion != nil {
		token.EntryChainVersion = *entryChainVersion
	}
	if targetGuardKey != nil {
		token.TargetGuardKey = *targetGuardKey
	}
	if targetRoleFingerprint != nil {
		token.TargetRoleFingerprint = *targetRoleFingerprint
	}
	token.EntryChainBound = token.EntryChainInventory != nil && token.EntryChainVersion == 1
	return token, nil
}

// ---------------------------------------------------------------------------
// Instance creation
// ---------------------------------------------------------------------------

// OpenInstanceRequest creates one recovery/baseline instance. RestorePoint and
// DataTarget are optional JSONB payloads; DataTarget must be credential-free
// (fingerprints only) and is validated as such.
type OpenInstanceRequest struct {
	Kind                  string
	OpenedBy              string
	Reason                string
	RestorePoint          []byte
	DataTarget            []byte
	EntryChainInventory   []uint64
	TargetGuardKey        string
	TargetRoleFingerprint string
}

// OpenInstanceResult carries the control-store-generated instance id.
type OpenInstanceResult struct {
	InstanceID string
}

const insertOpenInstanceSQL = `
INSERT INTO recovery_instance
    (instance_id, kind, state, restore_point, data_target, evidence_generation,
     evidence_hash, opened_by, reason, entry_chain_inventory, entry_chain_inventory_version,
     target_guard_key, target_role_fingerprint)
VALUES (gen_random_uuid(), $1, 'open', $2, $3, 0, $4, $5, $6, $7, CASE WHEN $7::jsonb IS NULL THEN NULL ELSE 1 END, $8, $9)
RETURNING instance_id::text`

// validateOpenTargetGuard runs after the new instance row has been inserted,
// preserving instance→guard transaction-lock order. The FK requires a guard
// inventory row first; provisionOpenTargetGuard inserts only-if-absent while
// the caller's dedicated target session lock serializes open/bootstrap.
func validateOpenTargetGuard(ctx context.Context, tx pgx.Tx, key string) error {
	if err := lockTargetGuard(ctx, tx, key); err != nil {
		return err
	}
	guard, found, err := ReadTargetGuard(ctx, tx, key)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("target guard inventory disappeared during instance open")
	}
	if !targetGuardConsistent(guard, true) {
		return errors.New("target guard is dirty, active, or inconsistent; refusing instance open")
	}
	return nil
}

func provisionOpenTargetGuard(ctx context.Context, tx pgx.Tx, key, operationID string) error {
	if !sha256FingerprintPattern.MatchString(key) {
		return errors.New("target guard key must be a full sha256 fingerprint")
	}
	operationID, err := normalizeOperationID(operationID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO recovery_target_guard (target_guard_key, disposition, operation_id)
VALUES ($1, 'unknown', $2) ON CONFLICT (target_guard_key) DO NOTHING`, key, operationID)
	if err != nil {
		return fmt.Errorf("provision target guard inventory: %w", err)
	}
	return nil
}

func targetGuardConsistent(guard TargetGuard, allowClean bool) bool {
	intentPair := guard.LaunchIntent == (guard.AttemptAppName != "") && guard.LaunchIntent == (guard.LaunchIntentAt != nil)
	if !intentPair || guard.ActiveWriter && (guard.State != TargetGuardUnknown || !guard.LaunchIntent) {
		return false
	}
	switch guard.State {
	case TargetGuardUnknown:
		return !guard.ActiveWriter && !guard.LaunchIntent && guard.LaunchedAt == nil && guard.RebuildRequiredAt == nil && guard.CleanAt == nil && guard.RebuildEvidence == nil
	case TargetGuardClean:
		return allowClean && !guard.ActiveWriter && guard.CleanAt != nil && guard.RebuildRequiredAt == nil && len(guard.RebuildEvidence) > 0 && json.Valid(guard.RebuildEvidence)
	default:
		return false
	}
}

// RequireInstanceTargetBinding verifies the immutable configured-target
// binding while preserving the lifecycle lock order: instance row first,
// target guard transaction lock second.
func RequireInstanceTargetBinding(ctx context.Context, tx pgx.Tx, token InstanceToken, key, roleFingerprint string) error {
	if !sha256FingerprintPattern.MatchString(key) || !sha256FingerprintPattern.MatchString(roleFingerprint) ||
		token.TargetGuardKey == "" || token.TargetRoleFingerprint == "" {
		return errors.New("instance target guard binding is missing (legacy-null); refusing lifecycle action")
	}
	if token.TargetGuardKey != key || token.TargetRoleFingerprint != roleFingerprint {
		return errors.New("configured data target does not match the immutable instance target guard binding")
	}
	if err := lockTargetGuard(ctx, tx, key); err != nil {
		return err
	}
	guard, found, err := ReadTargetGuard(ctx, tx, key)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("bound target guard inventory is missing; refusing lifecycle action")
	}
	if !targetGuardConsistent(guard, true) {
		return errors.New("bound target guard is dirty, active, or inconsistent; refusing lifecycle action")
	}
	return nil
}

// ValidateEntryChainInventory accepts only a complete-shape candidate: a
// nonempty, strictly ascending list of positive IDs. Completeness is supplied
// by deployment assembly, not inferable by this structural validator.
func ValidateEntryChainInventory(chains []uint64) error {
	if len(chains) == 0 {
		return errors.New("trusted entry chain inventory must be nonempty")
	}
	for i, chain := range chains {
		if chain == 0 || (i > 0 && chains[i-1] >= chain) {
			return errors.New("trusted entry chain inventory must contain positive, strictly ascending unique IDs")
		}
	}
	return nil
}

// OpenInstance creates an open recovery instance. The partial unique index
// recovery_instance_one_open_uniq allows at most one open instance globally
// (INV-1); a conflicting insert is rolled back and refused with
// ErrInstanceAlreadyOpen (zero instance writes, one best-effort refusal audit
// naming the already-open instance).
func (s *Store) OpenInstance(ctx context.Context, req OpenInstanceRequest) (OpenInstanceResult, error) {
	if err := validateCredentialText("reason", req.Reason); err != nil {
		return OpenInstanceResult{}, err
	}
	kind := strings.TrimSpace(req.Kind)
	if kind != "recovery" && kind != "baseline" {
		return OpenInstanceResult{}, fmt.Errorf("instance kind must be recovery or baseline, got %q", req.Kind)
	}
	openedBy := strings.TrimSpace(req.OpenedBy)
	if openedBy == "" {
		return OpenInstanceResult{}, errors.New("instance opened_by is required")
	}
	restorePoint, err := normalizeJSON("restore_point", req.RestorePoint)
	if err != nil {
		return OpenInstanceResult{}, err
	}
	dataTarget, err := normalizeJSON("data_target", req.DataTarget)
	if err != nil {
		return OpenInstanceResult{}, err
	}
	if len(dataTarget) > 0 {
		if err := ValidateDataTarget(dataTarget); err != nil {
			return OpenInstanceResult{}, err
		}
	}
	if !sha256FingerprintPattern.MatchString(req.TargetGuardKey) || !sha256FingerprintPattern.MatchString(req.TargetRoleFingerprint) {
		return OpenInstanceResult{}, errors.New("instance open requires a full target guard key and role fingerprint")
	}
	var inventoryJSON []byte
	if req.EntryChainInventory != nil {
		if err := ValidateEntryChainInventory(req.EntryChainInventory); err != nil {
			return OpenInstanceResult{}, err
		}
		inventoryJSON, err = json.Marshal(req.EntryChainInventory)
		if err != nil {
			return OpenInstanceResult{}, fmt.Errorf("encode entry chain inventory: %w", err)
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return OpenInstanceResult{}, fmt.Errorf("begin open instance: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// The target FK requires inventory before the instance row. Only an absent
	// row is inserted, and the app holds the dedicated session target lock; the
	// guarded read/validation itself takes the transaction lock after the new
	// instance row exists below.
	if err := provisionOpenTargetGuard(ctx, tx, req.TargetGuardKey, uuid.NewString()); err != nil {
		return OpenInstanceResult{}, err
	}

	var instanceID string
	err = tx.QueryRow(ctx, insertOpenInstanceSQL, kind, jsonOrNil(restorePoint), jsonOrNil(dataTarget),
		EmptyEvidenceHash, openedBy, req.Reason, jsonOrNil(inventoryJSON), req.TargetGuardKey, req.TargetRoleFingerprint).Scan(&instanceID)
	if err != nil {
		_ = tx.Rollback(ctx)
		if isUniqueViolation(err, "recovery_instance_one_open_uniq") {
			existing, _ := openInstanceID(ctx, s.pool)
			detail, _ := json.Marshal(map[string]any{
				"kind":               kind,
				"already_open":       existing,
				"already_open_found": existing != "",
				"reason":             req.Reason,
			})
			_ = WriteAudit(ctx, s.pool, AuditRecord{
				InstanceID: existing,
				Actor:      openedBy,
				Action:     ActionInstanceOpen,
				Detail:     detail,
				Result:     AuditRefused,
			})
			if existing != "" {
				return OpenInstanceResult{}, fmt.Errorf("%w: %s", ErrInstanceAlreadyOpen, existing)
			}
			return OpenInstanceResult{}, ErrInstanceAlreadyOpen
		}
		return OpenInstanceResult{}, fmt.Errorf("open recovery instance: %w", err)
	}
	// Match lifecycle writers' global lock order: instance row first, then the
	// target guard transaction lock. The insert and absent-row bootstrap commit
	// together, so a failed inventory check leaves neither behind.
	if err := validateOpenTargetGuard(ctx, tx, req.TargetGuardKey); err != nil {
		return OpenInstanceResult{}, err
	}

	target, err := json.Marshal(map[string]any{"instance_id": instanceID, "kind": kind})
	if err != nil {
		return OpenInstanceResult{}, fmt.Errorf("encode instance audit target: %w", err)
	}
	if err := WriteAudit(ctx, tx, AuditRecord{
		InstanceID: instanceID,
		Actor:      openedBy,
		Action:     ActionInstanceOpen,
		Target:     target,
		Detail:     mustJSON(map[string]any{"reason": req.Reason}),
		Result:     AuditOK,
	}); err != nil {
		return OpenInstanceResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return OpenInstanceResult{}, fmt.Errorf("commit open instance: %w", err)
	}
	return OpenInstanceResult{InstanceID: instanceID}, nil
}

func openInstanceID(ctx context.Context, q Queryer) (string, error) {
	var id string
	err := q.QueryRow(ctx,
		"SELECT instance_id::text FROM recovery_instance WHERE state = 'open' ORDER BY opened_at DESC, instance_id DESC LIMIT 1").Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return id, nil
}

// ---------------------------------------------------------------------------
// Append-only decisions
// ---------------------------------------------------------------------------

// DecisionResult is one recorded append-only decision row.
type DecisionResult struct {
	DecisionID         string
	InstanceID         string
	Capability         string
	ScopeHash          string
	Decision           string
	EvidenceGeneration int64
	EvidenceHash       string
	ApprovalRefs       []string
	ApprovalClass      string
	Principal          string
	PersonID           string
	SupersedesID       string
	OperationID        string
	Reason             string
	CreatedAt          time.Time
	// Recorded is true when this call read back an already-recorded decision
	// (same operation_id, same input); false when the call inserted it.
	Recorded bool
}

// ReleaseDecisionRequest appends one release/revoke decision for
// (instance, capability, scope_hash). All decision writes for an instance
// serialize on the instance row lock.
type ReleaseDecisionRequest struct {
	InstanceID          string
	Capability          string
	ScopeHash           string
	Decision            string
	EvidenceGeneration  int64
	EvidenceHash        string
	ApprovalRefs        []string
	SupersedesReleaseID string
	Actor               string
	Reason              string
	OperationID         string
}

// ApprovalDecisionRequest appends one approve/revoke decision for
// (instance, capability, scope_hash, principal).
type ApprovalDecisionRequest struct {
	InstanceID            string
	Capability            string
	ScopeHash             string
	Decision              string
	ApprovalClassSnapshot string
	Principal             string
	PersonID              string
	EvidenceGeneration    int64
	EvidenceHash          string
	SupersedesApprovalID  string
	Reason                string
	OperationID           string
}

// AppendReleaseDecision records one release/revoke decision idempotently:
// the operation_id UNIQUE constraint is the idempotency key. The same input
// reads the recorded row back (Recorded=true, zero duplicate writes); a
// different input with the same operation_id refuses with
// ErrOperationConflict and zero writes of any kind (011 execOperatorOp
// precedent, data-model §6).
func (s *Store) AppendReleaseDecision(ctx context.Context, req ReleaseDecisionRequest) (DecisionResult, error) {
	canonical, err := req.canonical()
	if err != nil {
		return DecisionResult{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return DecisionResult{}, fmt.Errorf("begin release decision: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	token, err := LockInstance(ctx, tx, canonical.InstanceID)
	if err != nil {
		return DecisionResult{}, err
	}
	if token.State != "open" {
		return DecisionResult{}, fmt.Errorf("%w: %s", ErrInstanceNotOpen, token.InstanceID)
	}
	result, err := InsertReleaseDecision(ctx, tx, canonical)
	if err != nil {
		if errors.Is(err, ErrOperationIDTaken) {
			_ = tx.Rollback(ctx)
			return s.releaseDecisionReadBack(ctx, canonical)
		}
		return DecisionResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return DecisionResult{}, fmt.Errorf("commit release decision: %w", err)
	}
	return result, nil
}

// AppendApprovalDecision records one approve/revoke decision idempotently
// (same operation_id semantics as AppendReleaseDecision).
func (s *Store) AppendApprovalDecision(ctx context.Context, req ApprovalDecisionRequest) (DecisionResult, error) {
	canonical, err := req.canonical()
	if err != nil {
		return DecisionResult{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return DecisionResult{}, fmt.Errorf("begin approval decision: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	token, err := LockInstance(ctx, tx, canonical.InstanceID)
	if err != nil {
		return DecisionResult{}, err
	}
	if token.State != "open" {
		return DecisionResult{}, fmt.Errorf("%w: %s", ErrInstanceNotOpen, token.InstanceID)
	}
	result, err := InsertApprovalDecision(ctx, tx, canonical)
	if err != nil {
		if errors.Is(err, ErrOperationIDTaken) {
			_ = tx.Rollback(ctx)
			return s.approvalDecisionReadBack(ctx, canonical)
		}
		return DecisionResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return DecisionResult{}, fmt.Errorf("commit approval decision: %w", err)
	}
	return result, nil
}

// InsertReleaseDecision appends one release/revoke row and its paired audit
// row inside tx. Callers MUST hold the instance row lock in the same
// transaction (LockInstance) so the audit BIGSERIAL allocation — the
// commit-order anchor — is serialized per instance. A duplicate operation_id
// is reported as ErrOperationIDTaken (transaction left for the caller to
// roll back); the caller-owned idempotent path is AppendReleaseDecision.
func InsertReleaseDecision(ctx context.Context, tx pgx.Tx, req ReleaseDecisionRequest) (DecisionResult, error) {
	if tx == nil {
		return DecisionResult{}, errors.New("release decision insert requires a transaction")
	}
	canonical, err := req.canonical()
	if err != nil {
		return DecisionResult{}, err
	}
	if err := requireOpenInstance(ctx, tx, canonical.InstanceID); err != nil {
		return DecisionResult{}, err
	}

	refs := make([]pgtype.UUID, 0, len(canonical.ApprovalRefs))
	for _, raw := range canonical.ApprovalRefs {
		u, err := uuid.Parse(raw)
		if err != nil {
			return DecisionResult{}, fmt.Errorf("approval_ref %q is not a UUID: %w", raw, err)
		}
		refs = append(refs, pgtype.UUID{Bytes: u, Valid: true})
	}
	id := uuid.NewString()
	const sql = `
INSERT INTO recovery_release
    (release_id, instance_id, capability, scope_hash, decision, evidence_generation,
     evidence_hash, approval_refs, operation_id, supersedes_release_id, reason)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
	_, err = tx.Exec(ctx, sql, id, canonical.InstanceID, canonical.Capability, canonical.ScopeHash,
		canonical.Decision, canonical.EvidenceGeneration, canonical.EvidenceHash, refs,
		canonical.OperationID, nullableText(canonical.SupersedesReleaseID), canonical.Reason)
	if err != nil {
		if isUniqueViolation(err, "recovery_release_operation_id_uniq") {
			return DecisionResult{}, fmt.Errorf("%w: %s", ErrOperationIDTaken, canonical.OperationID)
		}
		return DecisionResult{}, fmt.Errorf("insert release decision: %w", err)
	}

	target, err := json.Marshal(map[string]any{
		"release_id": id,
		"decision":   canonical.Decision,
		"scope_hash": canonical.ScopeHash,
	})
	if err != nil {
		return DecisionResult{}, fmt.Errorf("encode release audit target: %w", err)
	}
	if err := WriteAudit(ctx, tx, AuditRecord{
		InstanceID:         canonical.InstanceID,
		Actor:              canonical.Actor,
		Action:             ActionReleaseDecision,
		Target:             target,
		Result:             AuditOK,
		EvidenceGeneration: int64Ptr(canonical.EvidenceGeneration),
		OperationID:        canonical.OperationID,
	}); err != nil {
		return DecisionResult{}, err
	}
	return DecisionResult{
		DecisionID:         id,
		InstanceID:         canonical.InstanceID,
		Capability:         canonical.Capability,
		ScopeHash:          canonical.ScopeHash,
		Decision:           canonical.Decision,
		EvidenceGeneration: canonical.EvidenceGeneration,
		EvidenceHash:       canonical.EvidenceHash,
		ApprovalRefs:       canonical.ApprovalRefs,
		SupersedesID:       canonical.SupersedesReleaseID,
		OperationID:        canonical.OperationID,
		Reason:             canonical.Reason,
	}, nil
}

// InsertApprovalDecision appends one approve/revoke row and its paired audit
// row inside tx (same lock contract as InsertReleaseDecision).
func InsertApprovalDecision(ctx context.Context, tx pgx.Tx, req ApprovalDecisionRequest) (DecisionResult, error) {
	if tx == nil {
		return DecisionResult{}, errors.New("approval decision insert requires a transaction")
	}
	canonical, err := req.canonical()
	if err != nil {
		return DecisionResult{}, err
	}
	if err := requireOpenInstance(ctx, tx, canonical.InstanceID); err != nil {
		return DecisionResult{}, err
	}

	id := uuid.NewString()
	const sql = `
INSERT INTO recovery_approval
    (approval_id, instance_id, capability, scope_hash, decision, approval_class_snapshot,
     principal, person_id, reason, evidence_generation, evidence_hash, operation_id,
     supersedes_approval_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
	_, err = tx.Exec(ctx, sql, id, canonical.InstanceID, canonical.Capability, canonical.ScopeHash,
		canonical.Decision, canonical.ApprovalClassSnapshot, canonical.Principal, canonical.PersonID,
		canonical.Reason, canonical.EvidenceGeneration, canonical.EvidenceHash, canonical.OperationID,
		nullableText(canonical.SupersedesApprovalID))
	if err != nil {
		if isUniqueViolation(err, "recovery_approval_operation_id_uniq") {
			return DecisionResult{}, fmt.Errorf("%w: %s", ErrOperationIDTaken, canonical.OperationID)
		}
		return DecisionResult{}, fmt.Errorf("insert approval decision: %w", err)
	}

	target, err := json.Marshal(map[string]any{
		"approval_id": id,
		"decision":    canonical.Decision,
		"scope_hash":  canonical.ScopeHash,
	})
	if err != nil {
		return DecisionResult{}, fmt.Errorf("encode approval audit target: %w", err)
	}
	if err := WriteAudit(ctx, tx, AuditRecord{
		InstanceID:         canonical.InstanceID,
		Actor:              canonical.Principal,
		Action:             ActionApprovalDecision,
		Target:             target,
		Result:             AuditOK,
		EvidenceGeneration: int64Ptr(canonical.EvidenceGeneration),
		OperationID:        canonical.OperationID,
	}); err != nil {
		return DecisionResult{}, err
	}
	return DecisionResult{
		DecisionID:         id,
		InstanceID:         canonical.InstanceID,
		Capability:         canonical.Capability,
		ScopeHash:          canonical.ScopeHash,
		Decision:           canonical.Decision,
		EvidenceGeneration: canonical.EvidenceGeneration,
		EvidenceHash:       canonical.EvidenceHash,
		ApprovalClass:      canonical.ApprovalClassSnapshot,
		Principal:          canonical.Principal,
		PersonID:           canonical.PersonID,
		SupersedesID:       canonical.SupersedesApprovalID,
		OperationID:        canonical.OperationID,
		Reason:             canonical.Reason,
	}, nil
}

func requireOpenInstance(ctx context.Context, tx pgx.Tx, instanceID string) error {
	var state string
	err := tx.QueryRow(ctx, "SELECT state FROM recovery_instance WHERE instance_id = $1", instanceID).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}
	if err != nil {
		return fmt.Errorf("check recovery instance %s: %w", instanceID, err)
	}
	if state != "open" {
		return fmt.Errorf("%w: %s", ErrInstanceNotOpen, instanceID)
	}
	return nil
}

// releaseDecisionReadBack resolves the idempotent replay of a release
// decision: the recorded row is returned when it is the same input, otherwise
// the reuse refuses with ErrOperationConflict and zero writes.
func (s *Store) releaseDecisionReadBack(ctx context.Context, req ReleaseDecisionRequest) (DecisionResult, error) {
	existing, found, err := s.ReleaseDecisionByOperationID(ctx, s.pool, req.OperationID)
	if err != nil {
		return DecisionResult{}, err
	}
	if !found {
		return DecisionResult{}, fmt.Errorf("operation_id %q conflicted but no release decision row exists", req.OperationID)
	}
	if !existing.matchesReleaseInput(req) {
		return DecisionResult{}, fmt.Errorf("%w: operation_id %q", ErrOperationConflict, req.OperationID)
	}
	existing.Recorded = true
	return existing, nil
}

func (s *Store) approvalDecisionReadBack(ctx context.Context, req ApprovalDecisionRequest) (DecisionResult, error) {
	existing, found, err := s.ApprovalDecisionByOperationID(ctx, s.pool, req.OperationID)
	if err != nil {
		return DecisionResult{}, err
	}
	if !found {
		return DecisionResult{}, fmt.Errorf("operation_id %q conflicted but no approval decision row exists", req.OperationID)
	}
	if !existing.matchesApprovalInput(req) {
		return DecisionResult{}, fmt.Errorf("%w: operation_id %q", ErrOperationConflict, req.OperationID)
	}
	existing.Recorded = true
	return existing, nil
}

// ReleaseDecisionByOperationID reads the release decision recorded under an
// operation_id (idempotency read-back; strictly read-only).
func (s *Store) ReleaseDecisionByOperationID(ctx context.Context, q Queryer, operationID string) (DecisionResult, bool, error) {
	q = s.queryer(q)
	const sql = `
SELECT release_id::text, instance_id::text, capability, scope_hash, decision,
       evidence_generation, evidence_hash, approval_refs::text[], operation_id,
       COALESCE(supersedes_release_id::text, ''), reason, created_at
FROM recovery_release WHERE operation_id = $1`
	result := DecisionResult{OperationID: operationID}
	err := q.QueryRow(ctx, sql, operationID).Scan(
		&result.DecisionID, &result.InstanceID, &result.Capability, &result.ScopeHash,
		&result.Decision, &result.EvidenceGeneration, &result.EvidenceHash, &result.ApprovalRefs,
		&result.OperationID, &result.SupersedesID, &result.Reason, &result.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return DecisionResult{}, false, nil
	}
	if err != nil {
		return DecisionResult{}, false, fmt.Errorf("read release decision by operation_id: %w", err)
	}
	return result, true, nil
}

// ApprovalDecisionByOperationID reads the approval decision recorded under an
// operation_id (idempotency read-back; strictly read-only).
func (s *Store) ApprovalDecisionByOperationID(ctx context.Context, q Queryer, operationID string) (DecisionResult, bool, error) {
	q = s.queryer(q)
	const sql = `
SELECT approval_id::text, instance_id::text, capability, scope_hash, decision,
       approval_class_snapshot, principal, person_id, reason, evidence_generation,
       evidence_hash, operation_id, COALESCE(supersedes_approval_id::text, ''), created_at
FROM recovery_approval WHERE operation_id = $1`
	result := DecisionResult{OperationID: operationID}
	err := q.QueryRow(ctx, sql, operationID).Scan(
		&result.DecisionID, &result.InstanceID, &result.Capability, &result.ScopeHash,
		&result.Decision, &result.ApprovalClass, &result.Principal, &result.PersonID,
		&result.Reason, &result.EvidenceGeneration, &result.EvidenceHash,
		&result.OperationID, &result.SupersedesID, &result.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return DecisionResult{}, false, nil
	}
	if err != nil {
		return DecisionResult{}, false, fmt.Errorf("read approval decision by operation_id: %w", err)
	}
	return result, true, nil
}

// ---------------------------------------------------------------------------
// Append-only decision reads (commit order, never created_at)
// ---------------------------------------------------------------------------

// DecisionKey identifies one decision stream: an instance, a capability and
// the canonical scope hash. Approvals additionally bind to a principal.
type DecisionKey struct {
	InstanceID string
	Capability string
	ScopeHash  string
}

// DecisionState is the current decision of one stream. Found=false means no
// decision has been recorded. CommitSeq is the audit BIGSERIAL of the paired
// audit row — the commit-order anchor; it is never derived from created_at.
type DecisionState struct {
	Found              bool
	DecisionID         string
	Decision           string
	EvidenceGeneration int64
	EvidenceHash       string
	ApprovalRefs       []string
	ApprovalClass      string
	Principal          string
	PersonID           string
	SupersedesID       string
	OperationID        string
	Reason             string
	CreatedAt          time.Time
	CommitSeq          int64
}

// CurrentReleaseDecision returns the latest release/revoke decision of the
// stream by commit order (the paired audit row's BIGSERIAL allocation under
// the serialized writer lock), never by created_at. A revoke row recorded
// after a release row is therefore the current state. If the stream contains
// a decision row that cannot be ordered (no paired ok audit row), the read
// refuses with ErrDecisionUnordered (fail-closed) instead of guessing.
//
// For an authoritative read, call this inside the transaction that holds the
// instance row lock (LockInstance).
func (s *Store) CurrentReleaseDecision(ctx context.Context, q Queryer, key DecisionKey) (DecisionState, error) {
	q = s.queryer(q)
	instanceID, err := normalizeUUID(key.InstanceID, "instance_id")
	if err != nil {
		return DecisionState{}, err
	}
	if err := validateScopeHash(key.ScopeHash); err != nil {
		return DecisionState{}, err
	}
	const sql = `
SELECT d.release_id::text, d.decision, d.evidence_generation, d.evidence_hash,
       d.approval_refs::text[], COALESCE(d.supersedes_release_id::text, ''),
       d.operation_id, d.reason, d.created_at,
       (SELECT max(a.audit_id) FROM recovery_audit a
         WHERE a.operation_id = d.operation_id
           AND a.instance_id = d.instance_id
           AND a.action = '` + ActionReleaseDecision + `'
           AND a.result = 'ok'
           AND a.target->>'release_id' = d.release_id::text) AS commit_seq
FROM recovery_release d
WHERE d.instance_id = $1 AND d.capability = $2 AND d.scope_hash = $3
ORDER BY commit_seq DESC NULLS LAST`
	rows, err := q.Query(ctx, sql, instanceID, key.Capability, key.ScopeHash)
	if err != nil {
		return DecisionState{}, fmt.Errorf("read release decisions: %w", err)
	}
	defer rows.Close()

	var states []DecisionState
	for rows.Next() {
		var (
			st  DecisionState
			seq *int64
		)
		if err := rows.Scan(&st.DecisionID, &st.Decision, &st.EvidenceGeneration, &st.EvidenceHash,
			&st.ApprovalRefs, &st.SupersedesID, &st.OperationID, &st.Reason, &st.CreatedAt, &seq); err != nil {
			return DecisionState{}, fmt.Errorf("scan release decision: %w", err)
		}
		if seq == nil {
			return DecisionState{}, fmt.Errorf("%w: release operation_id %q has no paired ok audit row",
				ErrDecisionUnordered, st.OperationID)
		}
		st.CommitSeq = *seq
		st.Found = true
		states = append(states, st)
	}
	if err := rows.Err(); err != nil {
		return DecisionState{}, fmt.Errorf("read release decisions: %w", err)
	}
	if len(states) == 0 {
		return DecisionState{}, nil
	}
	return states[0], nil
}

// CurrentApprovalDecision returns the latest approve/revoke decision of one
// principal's stream by commit order (never created_at); a revoke covers only
// this principal's earlier approve rows (data-model §1.8). Ordering failures
// refuse with ErrDecisionUnordered.
func (s *Store) CurrentApprovalDecision(ctx context.Context, q Queryer, key DecisionKey, principal string) (DecisionState, error) {
	q = s.queryer(q)
	instanceID, err := normalizeUUID(key.InstanceID, "instance_id")
	if err != nil {
		return DecisionState{}, err
	}
	if err := validateScopeHash(key.ScopeHash); err != nil {
		return DecisionState{}, err
	}
	principal = strings.TrimSpace(principal)
	if !principalPattern.MatchString(principal) {
		return DecisionState{}, fmt.Errorf("approval principal %q is not in <kind>:<id> form", principal)
	}
	const sql = `
SELECT d.approval_id::text, d.decision, d.approval_class_snapshot, d.person_id,
       d.evidence_generation, d.evidence_hash, COALESCE(d.supersedes_approval_id::text, ''),
       d.operation_id, d.reason, d.created_at,
       (SELECT max(a.audit_id) FROM recovery_audit a
         WHERE a.operation_id = d.operation_id
           AND a.instance_id = d.instance_id
           AND a.action = '` + ActionApprovalDecision + `'
           AND a.result = 'ok'
           AND a.target->>'approval_id' = d.approval_id::text) AS commit_seq
FROM recovery_approval d
WHERE d.instance_id = $1 AND d.capability = $2 AND d.scope_hash = $3 AND d.principal = $4
ORDER BY commit_seq DESC NULLS LAST`
	rows, err := q.Query(ctx, sql, instanceID, key.Capability, key.ScopeHash, principal)
	if err != nil {
		return DecisionState{}, fmt.Errorf("read approval decisions: %w", err)
	}
	defer rows.Close()

	var states []DecisionState
	for rows.Next() {
		var (
			st  DecisionState
			seq *int64
		)
		if err := rows.Scan(&st.DecisionID, &st.Decision, &st.ApprovalClass, &st.PersonID,
			&st.EvidenceGeneration, &st.EvidenceHash, &st.SupersedesID, &st.OperationID,
			&st.Reason, &st.CreatedAt, &seq); err != nil {
			return DecisionState{}, fmt.Errorf("scan approval decision: %w", err)
		}
		if seq == nil {
			return DecisionState{}, fmt.Errorf("%w: approval operation_id %q has no paired ok audit row",
				ErrDecisionUnordered, st.OperationID)
		}
		st.CommitSeq = *seq
		st.Principal = principal
		st.Found = true
		states = append(states, st)
	}
	if err := rows.Err(); err != nil {
		return DecisionState{}, fmt.Errorf("read approval decisions: %w", err)
	}
	if len(states) == 0 {
		return DecisionState{}, nil
	}
	return states[0], nil
}

func (s *Store) queryer(q Queryer) Queryer {
	if q == nil {
		return s.pool
	}
	return q
}

// ---------------------------------------------------------------------------
// Audit
// ---------------------------------------------------------------------------

// Audit result values of recovery_audit.result.
const (
	AuditOK        = "ok"
	AuditRefused   = "refused"
	AuditDiscarded = "discarded"
	AuditFailed    = "failed"
)

// AuditRecord is one append-only recovery_audit row. Refusals and discards are
// rows too. RefusalClass, when set, must come from the closed set of
// data-model §3.3; EvidenceGeneration carries the generation token the
// operation was evaluated against.
type AuditRecord struct {
	InstanceID         string
	Actor              string
	Action             string
	Target             []byte
	Detail             []byte
	Result             string
	RefusalClass       string
	EvidenceGeneration *int64
	OperationID        string
}

const insertAuditSQL = `
INSERT INTO recovery_audit
    (instance_id, actor, action, target, detail, result, refusal_class,
     evidence_generation, operation_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

// WriteAudit appends one audit row. It validates the closed sets before the
// database does, so a caller cannot silently misspell a result or refusal
// class. Callable on a pool or inside a transaction (same-transaction audit
// is the pairing anchor of decision writes).
func WriteAudit(ctx context.Context, q Queryer, rec AuditRecord) error {
	args, err := auditInsertArgs(q, rec)
	if err != nil {
		return err
	}
	if _, err := q.Exec(ctx, insertAuditSQL, args...); err != nil {
		return fmt.Errorf("write recovery audit: %w", err)
	}
	return nil
}

// WriteAuditReturningID appends a fully validated audit row and returns the
// database-generated audit_id from that exact INSERT.
func WriteAuditReturningID(ctx context.Context, q Queryer, rec AuditRecord) (int64, error) {
	args, err := auditInsertArgs(q, rec)
	if err != nil {
		return 0, err
	}
	var auditID int64
	if err := q.QueryRow(ctx, insertAuditSQL+` RETURNING audit_id`, args...).Scan(&auditID); err != nil {
		return 0, fmt.Errorf("write recovery audit: %w", err)
	}
	return auditID, nil
}

func auditInsertArgs(q Queryer, rec AuditRecord) ([]any, error) {
	if q == nil {
		return nil, errors.New("audit write requires a database handle")
	}
	// Validate the original strings before trimming or passing them to the
	// driver: these fields are durable audit data, not log-only annotations.
	for field, value := range map[string]string{"actor": rec.Actor, "action": rec.Action, "operation_id": rec.OperationID} {
		if err := validateCredentialText(field, value); err != nil {
			return nil, err
		}
	}
	actor := strings.TrimSpace(rec.Actor)
	if actor == "" {
		return nil, errors.New("audit actor is required")
	}
	action := strings.TrimSpace(rec.Action)
	if action == "" {
		return nil, errors.New("audit action is required")
	}
	if _, ok := auditResults[rec.Result]; !ok {
		return nil, fmt.Errorf("audit result %q is not in the closed set ok|refused|discarded|failed", rec.Result)
	}
	if rec.RefusalClass != "" {
		if _, ok := refusalClasses[rec.RefusalClass]; !ok {
			return nil, fmt.Errorf("audit refusal_class %q is not in the closed set", rec.RefusalClass)
		}
	}
	if rec.EvidenceGeneration != nil && *rec.EvidenceGeneration < 0 {
		return nil, fmt.Errorf("audit evidence_generation must be >= 0, got %d", *rec.EvidenceGeneration)
	}
	var instanceID any
	if strings.TrimSpace(rec.InstanceID) != "" {
		id, err := normalizeUUID(rec.InstanceID, "instance_id")
		if err != nil {
			return nil, err
		}
		instanceID = id
	}
	target, err := normalizeJSON("target", rec.Target)
	if err != nil {
		return nil, err
	}
	if err := validateCredentialJSON("audit target", target); err != nil {
		return nil, err
	}
	detail, err := normalizeJSON("detail", rec.Detail)
	if err != nil {
		return nil, err
	}
	if err := validateCredentialJSON("audit detail", detail); err != nil {
		return nil, err
	}
	var refusalClass any
	if rec.RefusalClass != "" {
		refusalClass = rec.RefusalClass
	}
	var generation any
	if rec.EvidenceGeneration != nil {
		generation = *rec.EvidenceGeneration
	}
	var operationID any
	if strings.TrimSpace(rec.OperationID) != "" {
		operationID = rec.OperationID
	}
	return []any{instanceID, actor, action, jsonOrNil(target), jsonOrNil(detail), rec.Result, refusalClass, generation, operationID}, nil
}

// ---------------------------------------------------------------------------
// DSN target identity and credential-free fingerprints
// ---------------------------------------------------------------------------

// DSNTarget is the credential-free identity of one PostgreSQL target. It is
// used to refuse a control DSN that points at the data DB and to derive the
// fingerprint stored in recovery_instance.data_target.
type DSNTarget struct {
	Host     string
	Port     uint16
	Database string
	Role     string
}

// ParseDSNTarget parses a DSN without connecting to it.
func ParseDSNTarget(dsn string) (DSNTarget, error) {
	if strings.TrimSpace(dsn) == "" {
		return DSNTarget{}, errors.New("database dsn is empty")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return DSNTarget{}, fmt.Errorf("parse database dsn: %w", err)
	}
	return DSNTarget{
		Host:     cfg.ConnConfig.Host,
		Port:     cfg.ConnConfig.Port,
		Database: cfg.ConnConfig.Database,
		Role:     cfg.ConnConfig.User,
	}, nil
}

// SameDatabase reports whether both targets address the same database on the
// same host and port (credentials and parameters aside). Host comparison is
// case-insensitive; the check cannot see through DNS aliases.
func (t DSNTarget) SameDatabase(other DSNTarget) bool {
	return strings.EqualFold(t.Host, other.Host) && t.Port == other.Port && t.Database == other.Database
}

const fingerprintDomain = "txharbor-recovery-target-v1"

func fingerprint(kind string, parts ...string) string {
	h := sha256.New()
	h.Write([]byte(fingerprintDomain))
	h.Write([]byte{0})
	h.Write([]byte(kind))
	for _, p := range parts {
		h.Write([]byte{0})
		h.Write([]byte(p))
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// DataTargetFingerprint is the credential-free JSONB payload allowed in
// recovery_instance.data_target: database/role hashes plus a combined target
// hash. No plaintext DSN, hostname, credential or role name is stored.
type DataTargetFingerprint struct {
	Kind                string `json:"kind"`
	DatabaseFingerprint string `json:"database_fingerprint"`
	RoleFingerprint     string `json:"role_fingerprint"`
	TargetFingerprint   string `json:"target_fingerprint"`
}

// DataTargetFingerprint derives the credential-free fingerprint payload of
// this target.
func (t DSNTarget) DataTargetFingerprint() DataTargetFingerprint {
	return DataTargetFingerprint{
		Kind:                "pg",
		DatabaseFingerprint: fingerprint("database", t.Database),
		RoleFingerprint:     fingerprint("role", t.Role),
		TargetFingerprint: fingerprint("target",
			strings.ToLower(t.Host), strconv.Itoa(int(t.Port)), t.Database, t.Role),
	}
}

var (
	credentialKeys = map[string]bool{
		"dsn": true, "database_url": true, "url": true, "uri": true,
		"password": true, "passwd": true, "pwd": true, "pgpassword": true,
		"secret": true, "token": true, "api_key": true, "apikey": true,
		"access_key": true, "private_key": true, "credential": true, "credentials": true,
	}
	dsnLikeValue = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)|(\b(?:password|passwd|pwd)\s*=)`)
)

// ValidateDataTarget refuses a data_target payload that embeds a DSN or
// credential material at any depth (data_target stores fingerprints only;
// plaintext DSNs never reach the control store).
func ValidateDataTarget(raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("data_target must be valid JSON: %w", err)
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return errors.New("data_target must be a JSON object")
	}
	return validateDataTargetValue(obj)
}

func validateDataTargetValue(value any) error {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if credentialKeys[strings.ToLower(strings.ReplaceAll(key, "-", "_"))] {
				return fmt.Errorf("data_target field %q would store credential material; only database/role fingerprints are allowed", key)
			}
			if err := validateDataTargetValue(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range v {
			if err := validateDataTargetValue(child); err != nil {
				return err
			}
		}
	case string:
		if dsnLikeValue.MatchString(v) {
			return errors.New("data_target value looks like a DSN or credential; only database/role fingerprints are allowed")
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

var (
	capabilitySet = map[string]bool{
		"query": true, "chain_scan": true, "deposit_confirmation": true,
		"existing_withdrawal_recovery": true, "new_withdrawal_creation": true,
		"event_publishing": true, "event_consuming": true,
	}
	refusalClasses = map[string]bool{
		"no_instance": true, "instance_mismatch": true, "no_release": true,
		"release_invalidated_generation": true, "release_revoked": true,
		"capability_dependency_closed": true, "isolation_unproven": true, "gap_open": true,
		"approval_missing": true, "approval_identity_unverified": true,
		"approval_executor_excluded": true, "approval_stale": true,
		"hard_gate_active": true, "control_store_unavailable": true, "scope_mismatch": true,
	}
	auditResults = map[string]bool{
		AuditOK: true, AuditRefused: true, AuditDiscarded: true, AuditFailed: true,
	}
	principalPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*:[^[:space:]]+$`)
	uuidPattern      = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

func normalizeUUID(raw, field string) (string, error) {
	value := strings.TrimSpace(raw)
	if !uuidPattern.MatchString(value) {
		return "", fmt.Errorf("%s %q is not a UUID", field, raw)
	}
	return strings.ToLower(value), nil
}

func validateScopeHash(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return errors.New("scope_hash is required")
	}
	return nil
}

func validateDecision(decision string) error {
	if decision != "release" && decision != "revoke" {
		return fmt.Errorf("decision must be release or revoke, got %q", decision)
	}
	return nil
}

func validateApprovalDecision(decision string) error {
	if decision != "approve" && decision != "revoke" {
		return fmt.Errorf("approval decision must be approve or revoke, got %q", decision)
	}
	return nil
}

func canonicalUUIDList(raw []string) ([]string, error) {
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		id, err := normalizeUUID(item, "approval_refs")
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}

func (r ReleaseDecisionRequest) canonical() (ReleaseDecisionRequest, error) {
	if err := validateCredentialText("reason", r.Reason); err != nil {
		return ReleaseDecisionRequest{}, err
	}
	id, err := normalizeUUID(r.InstanceID, "instance_id")
	if err != nil {
		return ReleaseDecisionRequest{}, err
	}
	r.InstanceID = id
	if !capabilitySet[r.Capability] {
		return ReleaseDecisionRequest{}, fmt.Errorf("capability %q is not in the closed 7-capability set", r.Capability)
	}
	if err := validateScopeHash(r.ScopeHash); err != nil {
		return ReleaseDecisionRequest{}, err
	}
	if err := validateDecision(r.Decision); err != nil {
		return ReleaseDecisionRequest{}, err
	}
	if r.EvidenceGeneration < 0 {
		return ReleaseDecisionRequest{}, fmt.Errorf("evidence_generation must be >= 0, got %d", r.EvidenceGeneration)
	}
	if strings.TrimSpace(r.EvidenceHash) == "" {
		return ReleaseDecisionRequest{}, errors.New("evidence_hash is required")
	}
	if r.ApprovalRefs, err = canonicalUUIDList(r.ApprovalRefs); err != nil {
		return ReleaseDecisionRequest{}, err
	}
	if strings.TrimSpace(r.SupersedesReleaseID) != "" {
		if r.SupersedesReleaseID, err = normalizeUUID(r.SupersedesReleaseID, "supersedes_release_id"); err != nil {
			return ReleaseDecisionRequest{}, err
		}
	}
	r.Actor = strings.TrimSpace(r.Actor)
	if r.Actor == "" {
		return ReleaseDecisionRequest{}, errors.New("release decision actor is required")
	}
	r.OperationID = strings.TrimSpace(r.OperationID)
	if r.OperationID == "" {
		return ReleaseDecisionRequest{}, errors.New("operation_id is required")
	}
	return r, nil
}

func (r ApprovalDecisionRequest) canonical() (ApprovalDecisionRequest, error) {
	if err := validateCredentialText("reason", r.Reason); err != nil {
		return ApprovalDecisionRequest{}, err
	}
	id, err := normalizeUUID(r.InstanceID, "instance_id")
	if err != nil {
		return ApprovalDecisionRequest{}, err
	}
	r.InstanceID = id
	if !capabilitySet[r.Capability] {
		return ApprovalDecisionRequest{}, fmt.Errorf("capability %q is not in the closed 7-capability set", r.Capability)
	}
	if err := validateScopeHash(r.ScopeHash); err != nil {
		return ApprovalDecisionRequest{}, err
	}
	if err := validateApprovalDecision(r.Decision); err != nil {
		return ApprovalDecisionRequest{}, err
	}
	if r.ApprovalClassSnapshot != "single_non_executor" && r.ApprovalClassSnapshot != "dual_non_executor" {
		return ApprovalDecisionRequest{}, fmt.Errorf("approval_class_snapshot must be single_non_executor or dual_non_executor, got %q", r.ApprovalClassSnapshot)
	}
	r.Principal = strings.TrimSpace(r.Principal)
	if !principalPattern.MatchString(r.Principal) {
		return ApprovalDecisionRequest{}, fmt.Errorf("principal %q is not in <kind>:<id> form", r.Principal)
	}
	r.PersonID = strings.TrimSpace(r.PersonID)
	if r.PersonID == "" {
		return ApprovalDecisionRequest{}, errors.New("person_id is required (a principal without a resolved mapping must not register an approval)")
	}
	if r.EvidenceGeneration < 0 {
		return ApprovalDecisionRequest{}, fmt.Errorf("evidence_generation must be >= 0, got %d", r.EvidenceGeneration)
	}
	if strings.TrimSpace(r.EvidenceHash) == "" {
		return ApprovalDecisionRequest{}, errors.New("evidence_hash is required")
	}
	if strings.TrimSpace(r.SupersedesApprovalID) != "" {
		if r.SupersedesApprovalID, err = normalizeUUID(r.SupersedesApprovalID, "supersedes_approval_id"); err != nil {
			return ApprovalDecisionRequest{}, err
		}
	}
	r.OperationID = strings.TrimSpace(r.OperationID)
	if r.OperationID == "" {
		return ApprovalDecisionRequest{}, errors.New("operation_id is required")
	}
	return r, nil
}

func (r DecisionResult) matchesReleaseInput(in ReleaseDecisionRequest) bool {
	return r.InstanceID == in.InstanceID &&
		r.Capability == in.Capability &&
		r.ScopeHash == in.ScopeHash &&
		r.Decision == in.Decision &&
		r.EvidenceGeneration == in.EvidenceGeneration &&
		r.EvidenceHash == in.EvidenceHash &&
		slices.Equal(r.ApprovalRefs, in.ApprovalRefs) &&
		r.SupersedesID == in.SupersedesReleaseID &&
		r.OperationID == in.OperationID &&
		r.Reason == in.Reason
}

func (r DecisionResult) matchesApprovalInput(in ApprovalDecisionRequest) bool {
	return r.InstanceID == in.InstanceID &&
		r.Capability == in.Capability &&
		r.ScopeHash == in.ScopeHash &&
		r.Decision == in.Decision &&
		r.ApprovalClass == in.ApprovalClassSnapshot &&
		r.Principal == in.Principal &&
		r.PersonID == in.PersonID &&
		r.EvidenceGeneration == in.EvidenceGeneration &&
		r.EvidenceHash == in.EvidenceHash &&
		r.SupersedesID == in.SupersedesApprovalID &&
		r.OperationID == in.OperationID &&
		r.Reason == in.Reason
}

func normalizeJSON(field string, raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("%s must be valid JSON", field)
	}
	return raw, nil
}

func jsonOrNil(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	return string(raw)
}

func nullableText(raw string) any {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return raw
}

func int64Ptr(v int64) *int64 { return &v }

func mustJSON(value any) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		return []byte(`{}`)
	}
	return encoded
}

func isUniqueViolation(err error, name string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		return false
	}
	if name == "" {
		return true
	}
	return pgErr.ConstraintName == name || strings.Contains(pgErr.Message, name)
}

// ---------------------------------------------------------------------------
// Typed refusals
// ---------------------------------------------------------------------------

var (
	// ErrInstanceNotFound: the instance id does not exist in the control store.
	ErrInstanceNotFound = errors.New("recovery instance not found")
	// ErrInstanceNotOpen: the instance exists but is not open; decisions bind
	// to open instances only.
	ErrInstanceNotOpen = errors.New("recovery instance is not open")
	// ErrInstanceAlreadyOpen: the partial unique index refused a second open
	// instance (INV-1).
	ErrInstanceAlreadyOpen = errors.New("a recovery instance is already open")
	// ErrOperationIDTaken: the operation_id is already recorded (idempotency
	// key collision inside the insert); callers read the recorded decision back.
	ErrOperationIDTaken = errors.New("operation_id is already recorded")
	// ErrOperationConflict: the operation_id was reused with a different
	// input; the append refuses with zero writes.
	ErrOperationConflict = errors.New("operation_id reused with a different input; zero writes")
	// ErrDecisionUnordered: an append-only decision row has no paired ok audit
	// row, so commit order cannot be established; reads refuse (fail-closed)
	// rather than guessing.
	ErrDecisionUnordered = errors.New("decision history cannot be ordered by commit sequence; refusing")
)
