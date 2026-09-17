package execution

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AdmissionStatus values mirror contracts/api.md §1. Only the domain outcomes
// are decided here; 401/403/503 stay on the transport/permission side.
const (
	AdmissionCreated  = 201
	AdmissionRecorded = 200
	AdmissionNotFound = 404
	AdmissionConflict = 409
	AdmissionRefused  = 422
)

// ErrAdmissionUnavailable wraps every indeterminate storage/lock failure on the
// admission path. It is never rendered as a business refusal: the caller must
// answer 503 and let the client retry, because a false refusal would be a lie
// about the durable state.
var ErrAdmissionUnavailable = errors.New("admission storage unavailable")

// AdmissionOutcome is the T-admit result. Exactly one of the status branches is
// taken: created (201), recorded replay (200), missing/foreign (404), conflict
// (409, zero writes) or refusal (422, zero intent rows + refusal evidence).
type AdmissionOutcome struct {
	Intent  Intent
	Status  int
	Refusal RefusalClass
	Basis   string
}

var (
	intentIDShape = regexp.MustCompile(`^[\x21-\x7e]{1,128}$`)
	senderShape   = regexp.MustCompile(`^0x[0-9a-f]{40}$`)
)

// Admit runs the 011 admission transaction (persistence.md §1, gates.md §0):
// gate-table SHARE → 006 snapshot → 007 request row → grant FOR SHARE → scope
// FOR SHARE → 008 registry FOR SHARE → INSERT payment_intents → admitted event
// → projection row. Any gate failure writes zero intent rows and a best-effort
// admission_refused event. The caller has already authenticated and enforced
// the fixed can_execute permission before this call (fail-closed, before any
// domain read).
//
// A 23505 on payment_intents_request_uniq / payment_intents_authorization_uniq
// rolls back and re-reads the existing intent: the same identity basis is a
// recorded replay (200) and a different one is a conflict (409) with zero
// writes (insert-first protocol, 008 R3 precedent).
func Admit(ctx context.Context, pool *pgxpool.Pool, requestID string, callerID int64) (AdmissionOutcome, error) {
	if pool == nil {
		return AdmissionOutcome{}, fmt.Errorf("%w: nil pool", ErrAdmissionUnavailable)
	}
	if requestID == "" {
		return AdmissionOutcome{Status: AdmissionRefused, Basis: "empty request_id"}, nil
	}

	tx, err := BeginGateTx(ctx, pool)
	if err != nil {
		return AdmissionOutcome{}, fmt.Errorf("%w: %v", ErrAdmissionUnavailable, err)
	}
	committed := false
	defer func() {
		if !committed && tx != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if err := LockGateTables(ctx, tx); err != nil {
		return AdmissionOutcome{}, fmt.Errorf("%w: %v", ErrAdmissionUnavailable, err)
	}

	// 007 request row (ownership is compared here; missing and foreign render
	// identically as 404 so no existence leaks).
	req, found, err := ReadRequest(ctx, tx, requestID)
	if err != nil {
		return AdmissionOutcome{}, fmt.Errorf("%w: %v", ErrAdmissionUnavailable, err)
	}
	if !found || req.CallerID != callerID {
		return AdmissionOutcome{Status: AdmissionNotFound}, nil
	}
	if !req.IsAccepted() {
		return refuseAdmission(ctx, tx, "", requestID, "request_status="+req.Status, 0)
	}

	// The 007 grant identity is read alongside the row; RequestReadSQL stays
	// untouched (gates.go is the shared read path, T009).
	authorizationID, err := readRequestAuthorization(ctx, tx, requestID)
	if err != nil {
		return AdmissionOutcome{}, fmt.Errorf("%w: %v", ErrAdmissionUnavailable, err)
	}

	// 006 gate snapshot (one statement under the gate-table SHARE lock).
	recovery, err := ReadRecoveryGate(ctx, tx, req.ChainID)
	if err != nil {
		return AdmissionOutcome{}, fmt.Errorf("%w: %v", ErrAdmissionUnavailable, err)
	}

	// 007 grant row FOR SHARE.
	grant, grantFound, err := ReadGrant(ctx, tx, authorizationID)
	if err != nil {
		return AdmissionOutcome{}, fmt.Errorf("%w: %v", ErrAdmissionUnavailable, err)
	}
	if class := grant.Evaluate(grantFound, req.CallerID, req.ChainID, req.Asset, req.Recipient, req.Amount); class != "" {
		return refuseAdmission(ctx, tx, "", requestID, string(class), recovery.CurrentVersion())
	}

	// PB scope row FOR SHARE in the same sequence. Absence is
	// authorization_unverifiable (PB Q-B), never a silent fresh-grant
	// reinterpretation.
	scope, err := ReadScope(ctx, tx, authorizationID)
	if err != nil {
		return AdmissionOutcome{}, fmt.Errorf("%w: %v", ErrAdmissionUnavailable, err)
	}
	if !scope.Present {
		return refuseAdmission(ctx, tx, "", requestID, string(ClassAuthorizationUnverifiable), recovery.CurrentVersion())
	}
	sender := strings.ToLower(scope.Sender)
	if scope.RequestID != requestID {
		return refuseAdmission(ctx, tx, scope.IntentID, requestID, string(ClassScopeMismatch), recovery.CurrentVersion())
	}
	if !intentIDShape.MatchString(scope.IntentID) {
		return refuseAdmission(ctx, tx, "", requestID, "scope_intent_shape", recovery.CurrentVersion())
	}
	if !senderShape.MatchString(sender) {
		return refuseAdmission(ctx, tx, scope.IntentID, requestID, string(ClassScopeMismatch), recovery.CurrentVersion())
	}
	if scope.AuthorizationVersion < 1 {
		return refuseAdmission(ctx, tx, scope.IntentID, requestID, "scope_version_lt_1", recovery.CurrentVersion())
	}

	// 008 registry row FOR SHARE (registration proves usability, not authority).
	state, registryFound, err := ReadRegistryState(ctx, tx, req.ChainID, sender)
	if err != nil {
		return AdmissionOutcome{}, fmt.Errorf("%w: %v", ErrAdmissionUnavailable, err)
	}
	if class := RegistryRefusal(state, registryFound); class != "" {
		return refuseAdmission(ctx, tx, scope.IntentID, requestID, string(class), recovery.CurrentVersion())
	}

	// 006 gates: any pause row / active recovery refuses before the insert.
	if class := recovery.Evaluate(recovery.CurrentVersion()); class != "" {
		return refuseAdmission(ctx, tx, scope.IntentID, requestID, string(class), recovery.CurrentVersion())
	}

	recoveryVersion := recovery.CurrentVersion()
	if _, err := tx.Exec(ctx, insertIntentSQL,
		scope.IntentID, requestID, req.ChainID, sender, authorizationID,
		scope.AuthorizationVersion, recoveryVersion); err != nil {
		if isConstraint(err, "payment_intents_pkey") ||
			isConstraint(err, "payment_intents_request_uniq") ||
			isConstraint(err, "payment_intents_authorization_uniq") {
			// Roll back the failed insert, then classify by identity basis in a
			// fresh read (the winner has committed by the time 23505 surfaces).
			_ = tx.Rollback(ctx)
			tx = nil
			return classifyAdmissionConflict(ctx, pool, requestID, authorizationID,
				scope.IntentID, sender, req.ChainID, scope.AuthorizationVersion)
		}
		return AdmissionOutcome{}, fmt.Errorf("%w: %v", ErrAdmissionUnavailable, err)
	}

	if err := AppendEvent(ctx, tx, Event{
		IntentID: scope.IntentID,
		Kind:     EventAdmitted,
		Detail:   "request_id=" + requestID + " authorization_id=" + authorizationID,
	}); err != nil {
		return AdmissionOutcome{}, fmt.Errorf("%w: %v", ErrAdmissionUnavailable, err)
	}
	if _, err := tx.Exec(ctx, insertProjectionSQL, requestID, scope.IntentID, IntentAdmitted, 1); err != nil {
		return AdmissionOutcome{}, fmt.Errorf("%w: %v", ErrAdmissionUnavailable, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return AdmissionOutcome{}, fmt.Errorf("%w: %v", ErrAdmissionUnavailable, err)
	}
	committed = true

	return AdmissionOutcome{
		Status: AdmissionCreated,
		Intent: Intent{
			IntentID:                scope.IntentID,
			RequestID:               requestID,
			ChainID:                 req.ChainID,
			Sender:                  sender,
			AuthorizationID:         authorizationID,
			AuthorizationVersion:    scope.AuthorizationVersion,
			State:                   IntentAdmitted,
			StateVersion:            1,
			AdmittedRecoveryVersion: recoveryVersion,
		},
	}, nil
}

const insertIntentSQL = `INSERT INTO payment_intents
  (intent_id, request_id, chain_id, sender, authorization_id, authorization_version,
   state, state_version, admitted_recovery_version)
  VALUES ($1, $2, $3, $4, $5, $6, 'admitted', 1, $7)`

const insertProjectionSQL = `INSERT INTO request_status_projection
  (request_id, intent_id, execution_state, state_version, freshness)
  VALUES ($1, $2, $3, $4, 'confirmed')`

// refuseAdmission writes the best-effort admission_refused event and commits
// (there is no intent row to roll back; the event is the only durable write).
// A failed event write still returns the refusal: evidence is best-effort, the
// zero-intent guarantee is not.
func refuseAdmission(ctx context.Context, tx pgx.Tx, intentID, requestID string, basis string, version int64) (AdmissionOutcome, error) {
	if tx != nil {
		_ = AppendEvent(ctx, tx, Event{
			IntentID: intentID,
			Kind:     EventAdmissionRefused,
			Detail:   "request_id=" + requestID + " basis=" + sanitizeDetail(basis),
		})
		if err := tx.Commit(ctx); err != nil {
			_ = tx.Rollback(ctx)
		}
	}
	class := RefusalClass(basis)
	if !knownRefusal(class) {
		class = ""
	}
	return AdmissionOutcome{Status: AdmissionRefused, Refusal: class, Basis: basis}, nil
}

// knownRefusal reports whether a basis string is one of the closed machine
// refusal classes (the admission event detail carries the raw basis either
// way).
func knownRefusal(c RefusalClass) bool {
	switch c {
	case ClassAuthorizationInvalid, ClassAuthorizationExpired, ClassAuthorizationRevoked,
		ClassAuthorizationUnverifiable, ClassAuthorizationChanged, ClassScopeMismatch,
		ClassSenderUnregistered, ClassRecoveryPaused, ClassRecoveryActive,
		ClassRecoveryVersionChanged, ClassGateReadFailed:
		return true
	default:
		return false
	}
}

// sanitizeDetail keeps event detail to a bounded, newline-free evidence string
// (no secrets: only classes and identities ever reach this path).
func sanitizeDetail(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 256 {
		return s[:256]
	}
	return s
}

// classifyAdmissionConflict re-reads the winning intent by request_id then by
// authorization_id and decides recorded (same basis) vs conflict (different
// basis, zero writes).
func classifyAdmissionConflict(ctx context.Context, pool *pgxpool.Pool, requestID, authorizationID, intentID, sender string, chainID, authVersion int64) (AdmissionOutcome, error) {
	existing, found, err := readIntentByRequestID(ctx, pool, requestID)
	if err != nil {
		return AdmissionOutcome{}, fmt.Errorf("%w: %v", ErrAdmissionUnavailable, err)
	}
	if !found {
		existing, found, err = readIntentByAuthorizationID(ctx, pool, authorizationID)
		if err != nil {
			return AdmissionOutcome{}, fmt.Errorf("%w: %v", ErrAdmissionUnavailable, err)
		}
	}
	if !found {
		// The unique violation always has a winner; a miss here means the
		// database is inconsistent, never a refusal.
		return AdmissionOutcome{}, fmt.Errorf("%w: unique conflict but no winning intent", ErrAdmissionUnavailable)
	}
	same := existing.RequestID == requestID &&
		existing.AuthorizationID == authorizationID &&
		existing.IntentID == intentID &&
		existing.ChainID == chainID &&
		strings.EqualFold(existing.Sender, sender) &&
		existing.AuthorizationVersion == authVersion
	if !same {
		return AdmissionOutcome{
			Status: AdmissionConflict,
			Intent: existing,
			Basis:  "request_id=" + requestID + " authorization_id=" + authorizationID,
		}, nil
	}
	return AdmissionOutcome{Status: AdmissionRecorded, Intent: existing}, nil
}

const readIntentByRequestSQL = `SELECT intent_id, request_id, chain_id, sender, authorization_id,
  authorization_version, state, state_version, admitted_recovery_version, admitted_at, updated_at
  FROM payment_intents WHERE request_id = $1`
const readIntentByAuthorizationSQL = `SELECT intent_id, request_id, chain_id, sender, authorization_id,
  authorization_version, state, state_version, admitted_recovery_version, admitted_at, updated_at
  FROM payment_intents WHERE authorization_id = $1`

const canExecuteSQL = `SELECT can_execute FROM execution_caller_permission WHERE caller_id = $1`

// CanExecute reports the fixed interface permission for one caller. An absent
// row is FALSE (fail-closed); an indeterminate read is the only error.
func CanExecute(ctx context.Context, q Queryer, callerID int64) (bool, error) {
	var allowed bool
	err := q.QueryRow(ctx, canExecuteSQL, callerID).Scan(&allowed)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read execution_caller_permission: %w", err)
	}
	return allowed, nil
}

const requestAuthorizationSQL = `SELECT authorization_id FROM withdrawal_requests WHERE request_id = $1`

func readRequestAuthorization(ctx context.Context, q Queryer, requestID string) (string, error) {
	var id string
	if err := q.QueryRow(ctx, requestAuthorizationSQL, requestID).Scan(&id); err != nil {
		return "", fmt.Errorf("read 007 request authorization_id: %w", err)
	}
	return id, nil
}

func readIntentByRequestID(ctx context.Context, q Queryer, requestID string) (Intent, bool, error) {
	return scanIntent(q.QueryRow(ctx, readIntentByRequestSQL, requestID))
}

func readIntentByAuthorizationID(ctx context.Context, q Queryer, authorizationID string) (Intent, bool, error) {
	return scanIntent(q.QueryRow(ctx, readIntentByAuthorizationSQL, authorizationID))
}

func scanIntent(row pgx.Row) (Intent, bool, error) {
	var in Intent
	err := row.Scan(&in.IntentID, &in.RequestID, &in.ChainID, &in.Sender, &in.AuthorizationID,
		&in.AuthorizationVersion, &in.State, &in.StateVersion,
		&in.AdmittedRecoveryVersion, &in.AdmittedAt, &in.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return in, false, nil
	}
	if err != nil {
		return in, false, fmt.Errorf("read payment intent: %w", err)
	}
	return in, true, nil
}
