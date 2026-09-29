// drill.go implements T056 [US6]: the recovery_drill_run record model of
// FR-030/FR-031/FR-036 — the scenario vocabulary (including the seven failure
// injection identifiers), the strict input validation and the append-only
// control-store writer/reader used by the `recovery-admin drill` wiring (T057)
// and the independent drill channel (B17/T058–T060).
//
// The stored model is exactly the control-store table (schema §11):
//
//	drill_id, instance_id, scenario, recovery_point JSONB,
//	db_restore_seconds NUMERIC, verification_seconds NUMERIC,
//	capability_release_seconds JSONB, backup_lag JSONB,
//	uncovered_interval JSONB, constraints_configured BOOL,
//	test_inputs JSONB, gap_counts JSONB, result, log_ref, created_at
//
// Discipline (MUST NOT be weakened):
//
//   - Timing scopes stay separate. db_restore_seconds and
//     verification_seconds are independent columns, capability release times
//     are per capability, and this model has no method that derives an RTO
//     verdict from one scope: SafeResumptionSeconds returns ok=false unless
//     every one of the seven capabilities carries its own release time. A
//     completed restore, a database connection or a verification duration is
//     never an RTO claim (FR-031/FR-036; C1).
//   - No production threshold promise is written. Every run must carry
//     test_inputs with an explicit purpose (local drill inputs only), and a
//     run with constraints_configured=false must additionally name the
//     unconfigured required constraints verbatim — "not configured" is a
//     state, never a silent default (FR-036, C1).
//   - backup_lag and uncovered_interval are NOT NULL and encode "unknown"
//     explicitly; a missing measurement is never a fabricated zero.
//   - result is the closed set ok|refused_safe|failed_injected. The writer
//     refuses an unknown scenario/result instead of mapping it onto a
//     default.
//   - The writer appends the row and its paired recovery_audit row
//     (ActionDrillRun) in one transaction and never touches verification
//     items, gaps, approvals, releases or the evidence generation: recording
//     a drill grants nothing.
//
// The control store is reached only through a *controlstore.Store built by
// controlstore.NewStore, so the T069 schema-version guard (unknown or
// incompatible version => control_store_unavailable) is inherited and there
// is no unguarded drill write path.
package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// ActionDrillRun is the recovery_audit action of one recorded drill run; the
// audit target carries the drill_id and the input digest (idempotent read
// back under an operation_id).
const ActionDrillRun = "drill_run"

var (
	// ErrDrillInput marks a malformed or incomplete drill run input. The
	// request is never turned into a default: it is refused and nothing is
	// written.
	ErrDrillInput = errors.New("drill run input is invalid")
	// ErrUnknownDrillScenario marks a scenario outside the closed set.
	ErrUnknownDrillScenario = errors.New("unknown drill scenario")
	// ErrUnknownDrillResult marks a result outside the closed set.
	ErrUnknownDrillResult = errors.New("unknown drill result")
	// ErrDrillStaleGeneration marks a record-only run whose accepted evidence
	// generation changed before the drill row was inserted.
	ErrDrillStaleGeneration = errors.New("record-only drill evidence generation is stale")
)

// DrillScenario is one member of the closed drill scenario set: the positive
// full-recovery flow plus the seven failure injections of FR-032/SC-006.
// Values appear verbatim in recovery_drill_run.scenario, so renaming one is a
// contract change, not a refactor.
type DrillScenario string

// The closed scenario set. The seven injections mirror FR-032's failure
// classes one-to-one (quickstart §2 F1–F7).
const (
	// DrillScenarioFullRecovery is the positive flow: real restore,
	// verification, refusal of unsafe resumption where conditions demand it,
	// and graded resumption of what is provably safe.
	DrillScenarioFullRecovery DrillScenario = "full_recovery"
	// DrillScenarioF1BackupUnusable: backup unavailable/corrupt/truncated/
	// unverified must be refused, never "best effort" restored.
	DrillScenarioF1BackupUnusable DrillScenario = "f1_backup_unusable_or_unverified"
	// DrillScenarioF2RestoreInterrupted: an interrupted/partial restore is
	// never marked restored and retries are idempotent after a rebuild.
	DrillScenarioF2RestoreInterrupted DrillScenario = "f2_restore_interrupted_or_partial"
	// DrillScenarioF3VersionIncompatible: version/schema incompatibility is
	// refused explicitly with no silent downgrade or rewrite.
	DrillScenarioF3VersionIncompatible DrillScenario = "f3_version_or_schema_incompatible"
	// DrillScenarioF4ExternalFactAhead: external facts (chain/signing/
	// broadcast/downstream) lead the recovery point; divergence/unknown is
	// recorded and the affected capabilities stay closed.
	DrillScenarioF4ExternalFactAhead DrillScenario = "f4_external_fact_ahead"
	// DrillScenarioF5OldInstanceNotIsolated: the old instance cannot be
	// proven stopped; writes/sends/deliveries stay refused.
	DrillScenarioF5OldInstanceNotIsolated DrillScenario = "f5_old_instance_not_isolated"
	// DrillScenarioF6UnprovableGap: verification finds a gap that cannot be
	// proven; it stays unknown/pending, is escalated and blocks its affected
	// capabilities.
	DrillScenarioF6UnprovableGap DrillScenario = "f6_unprovable_evidence_gap"
	// DrillScenarioF7UnauthorizedResumption: an out-of-authority, insufficient
	// or stale-evidence resumption request is refused and audited.
	DrillScenarioF7UnauthorizedResumption DrillScenario = "f7_unauthorized_or_stale_approval"
)

// knownDrillScenarios is the canonical order (positive first, then F1–F7).
var knownDrillScenarios = []DrillScenario{
	DrillScenarioFullRecovery,
	DrillScenarioF1BackupUnusable,
	DrillScenarioF2RestoreInterrupted,
	DrillScenarioF3VersionIncompatible,
	DrillScenarioF4ExternalFactAhead,
	DrillScenarioF5OldInstanceNotIsolated,
	DrillScenarioF6UnprovableGap,
	DrillScenarioF7UnauthorizedResumption,
}

// failureInjections maps every injection scenario onto its F identifier. The
// positive scenario has none.
var failureInjections = map[DrillScenario]string{
	DrillScenarioF1BackupUnusable:         "F1",
	DrillScenarioF2RestoreInterrupted:     "F2",
	DrillScenarioF3VersionIncompatible:    "F3",
	DrillScenarioF4ExternalFactAhead:      "F4",
	DrillScenarioF5OldInstanceNotIsolated: "F5",
	DrillScenarioF6UnprovableGap:          "F6",
	DrillScenarioF7UnauthorizedResumption: "F7",
}

// Known reports whether s is one of the closed drill scenarios.
func (s DrillScenario) Known() bool { return slices.Contains(knownDrillScenarios, s) }

// KnownDrillScenarios returns the closed scenario set in canonical order. The
// caller receives a fresh slice and may mutate it freely.
func KnownDrillScenarios() []DrillScenario { return slices.Clone(knownDrillScenarios) }

// KnownFailureInjections returns the seven F identifiers in canonical order.
func KnownFailureInjections() []string { return []string{"F1", "F2", "F3", "F4", "F5", "F6", "F7"} }

// FailureInjection returns the F identifier of an injection scenario and
// false for DrillScenarioFullRecovery (or any unknown scenario).
func (s DrillScenario) FailureInjection() (string, bool) {
	id, ok := failureInjections[s]
	return id, ok
}

// ParseDrillScenario maps raw onto the closed scenario set. Matching is exact
// (no case folding, no trimming beyond surrounding whitespace): an unknown
// scenario is refused with ErrUnknownDrillScenario instead of being mapped to
// a default.
func ParseDrillScenario(raw string) (DrillScenario, error) {
	s := DrillScenario(strings.TrimSpace(raw))
	if !s.Known() {
		return "", fmt.Errorf("%w: %q (known: %v)", ErrUnknownDrillScenario, raw, knownDrillScenarios)
	}
	return s, nil
}

// DrillResult is one member of the closed drill result set.
type DrillResult string

// The three drill results (schema CHECK).
const (
	// DrillResultOK: the requested flow executed with no unhandled refusal.
	DrillResultOK DrillResult = "ok"
	// DrillResultRefusedSafe: the flow stopped at a fail-closed refusal and
	// nothing unsafe was opened.
	DrillResultRefusedSafe DrillResult = "refused_safe"
	// DrillResultFailedInjected: a declared failure injection was observed and
	// converged fail-closed.
	DrillResultFailedInjected DrillResult = "failed_injected"
)

// knownDrillResults is the canonical result order.
var knownDrillResults = []DrillResult{DrillResultOK, DrillResultRefusedSafe, DrillResultFailedInjected}

// Known reports whether r is one of the three drill results.
func (r DrillResult) Known() bool { return slices.Contains(knownDrillResults, r) }

// KnownDrillResults returns the closed result set in canonical order.
func KnownDrillResults() []DrillResult { return slices.Clone(knownDrillResults) }

// ParseDrillResult maps raw onto the closed result set; unknown input is
// refused with ErrUnknownDrillResult.
func ParseDrillResult(raw string) (DrillResult, error) {
	r := DrillResult(strings.TrimSpace(raw))
	if !r.Known() {
		return "", fmt.Errorf("%w: %q (known: %v)", ErrUnknownDrillResult, raw, knownDrillResults)
	}
	return r, nil
}

// DrillRunInput is one validated drill run to record.
//
// Timing fields are separate on purpose: DBRestoreSeconds and
// VerificationSeconds are independent scopes and CapabilityReleaseSeconds
// carries one measurement per capability. There is no combined field and no
// RTO field: an RTO verdict is derived elsewhere from the end-to-end
// safe-resumption scope only.
type DrillRunInput struct {
	// InstanceID binds the run to the open recovery instance (T056: drills
	// record against the active recovery instance).
	InstanceID string
	// Scenario is the closed scenario set (positive or one of the seven
	// injections).
	Scenario DrillScenario
	// RecoveryPoint is the manifest-derived recovery point JSONB (required
	// object; the caller persists the manifest facts, never a summary string).
	RecoveryPoint []byte
	// DBRestoreSeconds is the measured real-restore duration; nil = the phase
	// did not run (explicit "not measured", never 0).
	DBRestoreSeconds *float64
	// VerificationSeconds is the measured verification duration; nil = not
	// measured.
	VerificationSeconds *float64
	// CapabilityReleaseSeconds carries the measured per-capability safe
	// resumption duration. Unknown capabilities are refused. Nil = no release
	// was observed.
	CapabilityReleaseSeconds map[Capability]float64
	// BackupLag is the required JSONB lag payload; when the lag is not
	// measurable it carries an explicit {"state":"unknown", ...} object.
	BackupLag []byte
	// UncoveredInterval is the required JSONB uncovered-range payload (same
	// explicit-unknown rule).
	UncoveredInterval []byte
	// ConstraintsConfigured reports whether every required recovery constraint
	// (RPO/RTO targets, backup frequency, retention) was configured for this
	// drill. False requires TestInputs to name the unconfigured constraints.
	ConstraintsConfigured bool
	// TestInputs is the required JSONB annotation of the local drill inputs:
	// it must name the purpose ("local test inputs only; not production
	// thresholds") and, when ConstraintsConfigured is false, must carry a
	// non-empty unconfigured_constraints list.
	TestInputs []byte
	// GapCounts is the optional JSONB gap/disposition count payload.
	GapCounts []byte
	// Result is the closed result set.
	Result DrillResult
	// LogRef references the archived drill artifact (for example under
	// docs/evidence/015/). Optional; never a credential or a DSN.
	LogRef string
	// Actor is the authenticated principal recorded on the paired audit row.
	Actor string
	// OperationID is the optional idempotency key: the same id with the same
	// input reads the recorded run back (Recorded=true, zero writes); a
	// different input under the same operation_id refuses with
	// controlstore.ErrOperationConflict and zero writes.
	OperationID string
	// DrillID is an optional caller-supplied run id (a naming hint so an
	// archive can reference the row before it exists). It is NOT part of the
	// replay identity: an operation_id replay returns the originally recorded
	// row. Invalid UUIDs are refused; empty means the writer generates one.
	DrillID string
	// ExpectedEvidenceGeneration, when non-nil, requires the current instance
	// generation to match while RecordDrillRun holds its instance-row lock.
	// Record-only drills use this to bind the run to their accepted restore
	// probe generation. Nil leaves this guard disabled.
	ExpectedEvidenceGeneration *int64
}

// DrillRun is one persisted recovery_drill_run row.
type DrillRun struct {
	DrillID             string
	InstanceID          string
	Scenario            DrillScenario
	RecoveryPoint       []byte
	DBRestoreSeconds    *float64
	VerificationSeconds *float64
	// CapabilityReleaseSeconds holds only measured capabilities; an absent
	// capability was not observed as released.
	CapabilityReleaseSeconds map[Capability]float64
	BackupLag                []byte
	UncoveredInterval        []byte
	ConstraintsConfigured    bool
	TestInputs               []byte
	GapCounts                []byte
	Result                   DrillResult
	LogRef                   string
	CreatedAt                time.Time
	// Recorded is true when this call read an already-recorded run back
	// (same operation_id and input); false when it inserted the row.
	Recorded bool
}

// SafeResumptionSeconds returns the end-to-end safe-resumption duration —
// the only scope an RTO target may ever be judged against — and only when
// every one of the seven capabilities carries its own measured release time.
//
// A restored database, a reachable target and a single db_restore_seconds
// value never satisfy this method: with no complete per-capability set it
// returns ok=false (FR-031/FR-036; C1).
func (r DrillRun) SafeResumptionSeconds() (float64, bool) {
	if len(r.CapabilityReleaseSeconds) == 0 {
		return 0, false
	}
	var max float64
	for _, capability := range knownCapabilities {
		seconds, ok := r.CapabilityReleaseSeconds[capability]
		if !ok {
			return 0, false
		}
		if seconds > max {
			max = seconds
		}
	}
	return max, true
}

const drillInputDigestDomain = "txharbor-recovery-drill-input-v1"

// drillInputDigest is the canonical digest payload of one drill run input
// (deterministic: struct field order plus sorted map keys).
type drillInputDigest struct {
	InstanceID               string              `json:"instance_id"`
	Scenario                 string              `json:"scenario"`
	RecoveryPoint            json.RawMessage     `json:"recovery_point"`
	DBRestoreSeconds         *float64            `json:"db_restore_seconds"`
	VerificationSeconds      *float64            `json:"verification_seconds"`
	CapabilityReleaseSeconds map[string]*float64 `json:"capability_release_seconds"`
	BackupLag                json.RawMessage     `json:"backup_lag"`
	UncoveredInterval        json.RawMessage     `json:"uncovered_interval"`
	ConstraintsConfigured    bool                `json:"constraints_configured"`
	TestInputs               json.RawMessage     `json:"test_inputs"`
	GapCounts                json.RawMessage     `json:"gap_counts,omitempty"`
	Result                   string              `json:"result"`
	LogRef                   string              `json:"log_ref,omitempty"`
}

// RecordDrillRun validates and appends one recovery_drill_run row plus its
// paired drift audit row in one transaction. It changes no verification item,
// gap, approval, release or evidence generation: a drill record is
// observability, never a permission.
func RecordDrillRun(ctx context.Context, store *controlstore.Store, input DrillRunInput) (DrillRun, error) {
	if store == nil {
		return DrillRun{}, errors.New("drill run requires a controlstore.Store built by controlstore.NewStore")
	}
	prepared, err := prepareDrillRun(input)
	if err != nil {
		return DrillRun{}, err
	}

	tx, err := store.Pool().Begin(ctx)
	if err != nil {
		return DrillRun{}, fmt.Errorf("begin drill run: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Serialize on the instance row (the same lock every decision write uses)
	// so the idempotency read-back and the insert cannot interleave.
	var kind, state string
	var currentGeneration int64
	err = tx.QueryRow(ctx,
		`SELECT kind, state, evidence_generation FROM recovery_instance WHERE instance_id = $1 FOR UPDATE`, prepared.instanceID).
		Scan(&kind, &state, &currentGeneration)
	if errors.Is(err, pgx.ErrNoRows) {
		return DrillRun{}, fmt.Errorf("drill run instance %s does not exist", prepared.instanceID)
	}
	if err != nil {
		return DrillRun{}, fmt.Errorf("lock drill run instance: %w", err)
	}
	if kind != "recovery" {
		return DrillRun{}, fmt.Errorf("drill run instance %s has kind %q; drill runs bind to the open recovery instance", prepared.instanceID, kind)
	}
	if state != "open" {
		return DrillRun{}, fmt.Errorf("drill run instance %s is not open (state=%s); close the drill before closing the instance", prepared.instanceID, state)
	}

	// Optional idempotent read-back: the recorded audit row anchors the
	// drill_id and the input digest.
	if prepared.operationID != "" {
		recorded, found, err := drillRunByOperationID(ctx, tx, prepared.operationID)
		if err != nil {
			return DrillRun{}, err
		}
		if found {
			if recorded.inputDigest != prepared.inputDigest {
				return DrillRun{}, fmt.Errorf("%w: operation_id %q", controlstore.ErrOperationConflict, prepared.operationID)
			}
			run := recorded.run
			run.Recorded = true
			return run, nil
		}
	}
	if input.ExpectedEvidenceGeneration != nil && currentGeneration != *input.ExpectedEvidenceGeneration {
		return DrillRun{}, fmt.Errorf("%w: expected %d, current %d", ErrDrillStaleGeneration,
			*input.ExpectedEvidenceGeneration, currentGeneration)
	}

	drillID := prepared.drillID
	if drillID == "" {
		drillID = uuid.NewString()
	}
	const insertSQL = `
INSERT INTO recovery_drill_run
    (drill_id, instance_id, scenario, recovery_point, db_restore_seconds,
     verification_seconds, capability_release_seconds, backup_lag,
     uncovered_interval, constraints_configured, test_inputs, gap_counts,
     result, log_ref)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`
	var gapCounts any
	if len(prepared.gapCounts) > 0 {
		gapCounts = prepared.gapCounts
	}
	var logRef any
	if prepared.logRef != "" {
		logRef = prepared.logRef
	}
	if _, err := tx.Exec(ctx, insertSQL,
		drillID, prepared.instanceID, string(prepared.scenario), prepared.recoveryPoint,
		prepared.dbRestoreSeconds, prepared.verificationSeconds,
		prepared.capabilityReleaseSeconds, prepared.backupLag, prepared.uncoveredInterval,
		prepared.constraintsConfigured, prepared.testInputs, gapCounts,
		string(prepared.result), logRef); err != nil {
		return DrillRun{}, fmt.Errorf("insert drill run: %w", err)
	}

	target, err := json.Marshal(map[string]any{
		"drill_id":     drillID,
		"input_digest": prepared.inputDigest,
	})
	if err != nil {
		return DrillRun{}, fmt.Errorf("encode drill run audit target: %w", err)
	}
	if err := controlstore.WriteAudit(ctx, tx, controlstore.AuditRecord{
		InstanceID:  prepared.instanceID,
		Actor:       prepared.actor,
		Action:      ActionDrillRun,
		Target:      target,
		Result:      controlstore.AuditOK,
		OperationID: prepared.operationID,
	}); err != nil {
		return DrillRun{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return DrillRun{}, fmt.Errorf("commit drill run: %w", err)
	}

	return DrillRun{
		DrillID:                  drillID,
		InstanceID:               prepared.instanceID,
		Scenario:                 prepared.scenario,
		RecoveryPoint:            prepared.recoveryPoint,
		DBRestoreSeconds:         prepared.dbRestoreSeconds,
		VerificationSeconds:      prepared.verificationSeconds,
		CapabilityReleaseSeconds: prepared.capabilityReleases,
		BackupLag:                prepared.backupLag,
		UncoveredInterval:        prepared.uncoveredInterval,
		ConstraintsConfigured:    prepared.constraintsConfigured,
		TestInputs:               prepared.testInputs,
		GapCounts:                prepared.gapCounts,
		Result:                   prepared.result,
		LogRef:                   prepared.logRef,
		CreatedAt:                time.Now().UTC(),
	}, nil
}

// DrillRuns reads every recorded drill run of one instance in creation order
// (the archival/evidence read surface).
func DrillRuns(ctx context.Context, store *controlstore.Store, instanceID string) ([]DrillRun, error) {
	if store == nil {
		return nil, errors.New("drill runs require a controlstore.Store built by controlstore.NewStore")
	}
	instance, err := drillInstanceID(instanceID)
	if err != nil {
		return nil, err
	}
	const sql = `
SELECT drill_id::text, instance_id::text, scenario, recovery_point,
       db_restore_seconds, verification_seconds, capability_release_seconds,
       backup_lag, uncovered_interval, constraints_configured, test_inputs,
       gap_counts, result, COALESCE(log_ref, ''), created_at
FROM recovery_drill_run
WHERE instance_id = $1
ORDER BY created_at, drill_id`
	rows, err := store.Pool().Query(ctx, sql, instance)
	if err != nil {
		return nil, fmt.Errorf("read drill runs of instance %s: %w", instance, err)
	}
	defer rows.Close()
	var out []DrillRun
	for rows.Next() {
		run, err := scanDrillRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scan drill run of instance %s: %w", instance, err)
		}
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read drill runs of instance %s: %w", instance, err)
	}
	return out, nil
}

// drillRunRecord is one recorded run plus its input digest.
type drillRunRecord struct {
	run         DrillRun
	inputDigest string
}

// drillRunByOperationID resolves the idempotent read-back of one operation id
// from the paired audit row (read inside the caller's transaction).
func drillRunByOperationID(ctx context.Context, tx pgx.Tx, operationID string) (drillRunRecord, bool, error) {
	var target []byte
	err := tx.QueryRow(ctx, `
SELECT target
FROM recovery_audit
WHERE action = $1 AND operation_id = $2 AND result = 'ok'
ORDER BY audit_id DESC
LIMIT 1`, ActionDrillRun, operationID).Scan(&target)
	if errors.Is(err, pgx.ErrNoRows) {
		return drillRunRecord{}, false, nil
	}
	if err != nil {
		return drillRunRecord{}, false, fmt.Errorf("read drill run by operation_id: %w", err)
	}
	var anchor struct {
		DrillID     string `json:"drill_id"`
		InputDigest string `json:"input_digest"`
	}
	if err := json.Unmarshal(target, &anchor); err != nil {
		return drillRunRecord{}, false, fmt.Errorf("decode drill run audit target: %w", err)
	}
	if anchor.DrillID == "" {
		return drillRunRecord{}, false, fmt.Errorf("drill run audit row for operation_id %q carries no drill_id", operationID)
	}
	run, found, err := drillRunByID(ctx, tx, anchor.DrillID)
	if err != nil {
		return drillRunRecord{}, false, err
	}
	if !found {
		return drillRunRecord{}, false, fmt.Errorf("drill run %s recorded under operation_id %q no longer exists", anchor.DrillID, operationID)
	}
	return drillRunRecord{run: run, inputDigest: anchor.InputDigest}, true, nil
}

func drillRunByID(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, drillID string) (DrillRun, bool, error) {
	const sql = `
SELECT drill_id::text, instance_id::text, scenario, recovery_point,
       db_restore_seconds, verification_seconds, capability_release_seconds,
       backup_lag, uncovered_interval, constraints_configured, test_inputs,
       gap_counts, result, COALESCE(log_ref, ''), created_at
FROM recovery_drill_run
WHERE drill_id = $1`
	run, err := scanDrillRun(q.QueryRow(ctx, sql, drillID))
	if errors.Is(err, pgx.ErrNoRows) {
		return DrillRun{}, false, nil
	}
	if err != nil {
		return DrillRun{}, false, fmt.Errorf("read drill run %s: %w", drillID, err)
	}
	return run, true, nil
}

// rowScanner is the shared Scan surface of pgx.Row and pgx.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanDrillRun(row rowScanner) (DrillRun, error) {
	var (
		run              DrillRun
		scenario, result string
		capabilityJSON   []byte
	)
	err := row.Scan(
		&run.DrillID, &run.InstanceID, &scenario, &run.RecoveryPoint,
		&run.DBRestoreSeconds, &run.VerificationSeconds, &capabilityJSON,
		&run.BackupLag, &run.UncoveredInterval, &run.ConstraintsConfigured,
		&run.TestInputs, &run.GapCounts, &result, &run.LogRef, &run.CreatedAt)
	if err != nil {
		return DrillRun{}, err
	}
	run.Scenario = DrillScenario(scenario)
	run.Result = DrillResult(result)
	releases, err := decodeCapabilityReleases(capabilityJSON)
	if err != nil {
		return DrillRun{}, err
	}
	run.CapabilityReleaseSeconds = releases
	return run, nil
}

// decodeCapabilityReleases decodes the per-capability JSONB payload, keeping
// only measured (non-null, known-capability) entries.
func decodeCapabilityReleases(raw []byte) (map[Capability]float64, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var decoded map[string]*float64
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("decode capability_release_seconds: %w", err)
	}
	out := make(map[Capability]float64, len(decoded))
	for name, seconds := range decoded {
		if seconds == nil {
			continue
		}
		capability := Capability(name)
		if !capability.Known() {
			return nil, fmt.Errorf("%w: capability_release_seconds names unknown capability %q", ErrDrillInput, name)
		}
		out[capability] = *seconds
	}
	return out, nil
}

// preparedDrillRun is one validated drill run input.
type preparedDrillRun struct {
	instanceID               string
	scenario                 DrillScenario
	recoveryPoint            []byte
	dbRestoreSeconds         *float64
	verificationSeconds      *float64
	capabilityReleaseSeconds []byte
	capabilityReleases       map[Capability]float64
	backupLag                []byte
	uncoveredInterval        []byte
	constraintsConfigured    bool
	testInputs               []byte
	gapCounts                []byte
	result                   DrillResult
	logRef                   string
	actor                    string
	operationID              string
	drillID                  string
	inputDigest              string
}

// prepareDrillRun validates one input and renders its storage form. Every
// failure is ErrDrillInput (or a wrapped closed-set error) and writes
// nothing.
func prepareDrillRun(input DrillRunInput) (preparedDrillRun, error) {
	var prepared preparedDrillRun
	instance, err := drillInstanceID(input.InstanceID)
	if err != nil {
		return prepared, err
	}
	prepared.instanceID = instance

	if !input.Scenario.Known() {
		return prepared, fmt.Errorf("%w: %q", ErrUnknownDrillScenario, input.Scenario)
	}
	prepared.scenario = input.Scenario
	if !input.Result.Known() {
		return prepared, fmt.Errorf("%w: %q", ErrUnknownDrillResult, input.Result)
	}
	prepared.result = input.Result

	actor := strings.TrimSpace(input.Actor)
	if actor == "" {
		return prepared, fmt.Errorf("%w: actor is required; the authenticated principal is recorded on the paired audit row", ErrDrillInput)
	}
	prepared.actor = actor

	prepared.recoveryPoint, err = drillJSONObject("recovery_point", input.RecoveryPoint, true)
	if err != nil {
		return prepared, err
	}
	prepared.backupLag, err = drillJSONObject("backup_lag", input.BackupLag, true)
	if err != nil {
		return prepared, err
	}
	prepared.uncoveredInterval, err = drillJSONObject("uncovered_interval", input.UncoveredInterval, true)
	if err != nil {
		return prepared, err
	}
	prepared.testInputs, err = drillJSONObject("test_inputs", input.TestInputs, true)
	if err != nil {
		return prepared, err
	}
	if err := validateDrillTestInputs(prepared.testInputs, input.ConstraintsConfigured); err != nil {
		return prepared, err
	}
	prepared.constraintsConfigured = input.ConstraintsConfigured
	if len(input.GapCounts) > 0 {
		prepared.gapCounts, err = drillJSONObject("gap_counts", input.GapCounts, false)
		if err != nil {
			return prepared, err
		}
	}

	if input.DBRestoreSeconds != nil {
		if err := drillSeconds("db_restore_seconds", *input.DBRestoreSeconds); err != nil {
			return prepared, err
		}
		value := *input.DBRestoreSeconds
		prepared.dbRestoreSeconds = &value
	}
	if input.VerificationSeconds != nil {
		if err := drillSeconds("verification_seconds", *input.VerificationSeconds); err != nil {
			return prepared, err
		}
		value := *input.VerificationSeconds
		prepared.verificationSeconds = &value
	}

	prepared.capabilityReleases = make(map[Capability]float64, len(input.CapabilityReleaseSeconds))
	for capability, seconds := range input.CapabilityReleaseSeconds {
		if !capability.Known() {
			return prepared, fmt.Errorf("%w: capability_release_seconds names unknown capability %q", ErrDrillInput, capability)
		}
		if err := drillSeconds("capability_release_seconds["+string(capability)+"]", seconds); err != nil {
			return prepared, err
		}
		prepared.capabilityReleases[capability] = seconds
	}
	prepared.capabilityReleaseSeconds, err = encodeCapabilityReleases(prepared.capabilityReleases)
	if err != nil {
		return prepared, err
	}

	logRef := strings.TrimSpace(input.LogRef)
	if len(logRef) > 1024 {
		return prepared, fmt.Errorf("%w: log_ref is above the 1024-byte bound", ErrDrillInput)
	}
	if strings.IndexFunc(logRef, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
		return prepared, fmt.Errorf("%w: log_ref contains a control character", ErrDrillInput)
	}
	prepared.logRef = logRef
	prepared.operationID = strings.TrimSpace(input.OperationID)
	if raw := strings.TrimSpace(input.DrillID); raw != "" {
		parsed, err := uuid.Parse(raw)
		if err != nil {
			return prepared, fmt.Errorf("%w: drill_id %q is not a UUID", ErrDrillInput, input.DrillID)
		}
		prepared.drillID = parsed.String()
	}

	digest, err := drillInputDigestOf(prepared)
	if err != nil {
		return prepared, err
	}
	prepared.inputDigest = digest
	return prepared, nil
}

// drillInstanceID canonicalizes an instance id (the fix for a UUID string).
func drillInstanceID(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("%w: instance_id is required", ErrDrillInput)
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", fmt.Errorf("%w: instance_id %q is not a UUID", ErrDrillInput, raw)
	}
	return parsed.String(), nil
}

// drillJSONObject validates one required/optional JSONB payload: it must
// decode as a JSON object (the schema stores objects; an unknown measurement
// is an explicit {"state":"unknown", ...} object, never a scalar or null).
func drillJSONObject(name string, raw []byte, required bool) ([]byte, error) {
	if len(raw) == 0 {
		if required {
			return nil, fmt.Errorf("%w: %s is required and is never defaulted", ErrDrillInput, name)
		}
		return nil, nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, fmt.Errorf("%w: %s must be a JSON object", ErrDrillInput, name)
	}
	return raw, nil
}

// validateDrillTestInputs enforces the no-production-threshold-promise rule:
// every run names its purpose, and an unconfigured constraint set is named
// explicitly (FR-036/C1).
func validateDrillTestInputs(raw []byte, constraintsConfigured bool) error {
	var doc struct {
		Purpose                 string   `json:"purpose"`
		UnconfiguredConstraints []string `json:"unconfigured_constraints"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("%w: test_inputs must be a JSON object: %v", ErrDrillInput, err)
	}
	if strings.TrimSpace(doc.Purpose) == "" {
		return fmt.Errorf("%w: test_inputs.purpose is required (local drill inputs only; not production thresholds)", ErrDrillInput)
	}
	if !constraintsConfigured {
		var names []string
		for _, name := range doc.UnconfiguredConstraints {
			if strings.TrimSpace(name) != "" {
				names = append(names, strings.TrimSpace(name))
			}
		}
		if len(names) == 0 {
			return fmt.Errorf("%w: constraints_configured=false requires test_inputs.unconfigured_constraints to name the unconfigured required constraints (never a silent default)", ErrDrillInput)
		}
	}
	return nil
}

// drillSeconds rejects a negative or non-finite duration measurement.
func drillSeconds(name string, seconds float64) error {
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < 0 {
		return fmt.Errorf("%w: %s=%v is not a non-negative finite duration", ErrDrillInput, name, seconds)
	}
	return nil
}

// encodeCapabilityReleases renders the JSONB payload with every known
// capability present; a capability without a measurement is an explicit
// null. The per-capability shape is therefore stable and a missing
// measurement can never be misread as a zero.
func encodeCapabilityReleases(measured map[Capability]float64) ([]byte, error) {
	payload := make(map[string]*float64, len(knownCapabilities))
	for _, capability := range knownCapabilities {
		payload[string(capability)] = nil
	}
	for capability, seconds := range measured {
		value := seconds
		payload[string(capability)] = &value
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: encode capability_release_seconds: %v", ErrDrillInput, err)
	}
	return encoded, nil
}

// drillInputDigestOf renders the canonical input digest of one prepared run.
func drillInputDigestOf(prepared preparedDrillRun) (string, error) {
	releases := make(map[string]*float64, len(prepared.capabilityReleases))
	for _, capability := range knownCapabilities {
		releases[string(capability)] = nil
	}
	for capability, seconds := range prepared.capabilityReleases {
		value := seconds
		releases[string(capability)] = &value
	}
	payload := drillInputDigest{
		InstanceID:               prepared.instanceID,
		Scenario:                 string(prepared.scenario),
		RecoveryPoint:            prepared.recoveryPoint,
		DBRestoreSeconds:         prepared.dbRestoreSeconds,
		VerificationSeconds:      prepared.verificationSeconds,
		CapabilityReleaseSeconds: releases,
		BackupLag:                prepared.backupLag,
		UncoveredInterval:        prepared.uncoveredInterval,
		ConstraintsConfigured:    prepared.constraintsConfigured,
		TestInputs:               prepared.testInputs,
		GapCounts:                prepared.gapCounts,
		Result:                   string(prepared.result),
		LogRef:                   prepared.logRef,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("%w: encode drill input digest: %v", ErrDrillInput, err)
	}
	hash := sha256.New()
	hash.Write([]byte(drillInputDigestDomain))
	hash.Write([]byte{0})
	hash.Write(encoded)
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}
