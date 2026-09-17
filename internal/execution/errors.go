// Package execution owns the 011 withdrawal execution worker's durable
// carriers (payment intents, execution claims, steps, events, status
// projection) and its read-only consumer boundary to 010. It imports no
// RPC/dial package and no upstream writer package; every durable write goes to
// the 011 tables created by migration 000012.
package execution

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Queryer is the read/write subset shared by pgx.Tx and *pgxpool.Pool, so a
// carrier can run inside a caller's transaction or directly on a pool.
type Queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// RefusalClass is the closed machine refusal vocabulary of
// contracts/persistence.md §5. A refusal is recorded evidence: it is never a
// state transition, and it never claims "definitely not sent" while the
// outcome is unknown.
type RefusalClass string

const (
	ClassExecutionPermissionDenied RefusalClass = "execution_permission_denied"
	ClassAuthorizationInvalid      RefusalClass = "authorization_invalid"
	ClassAuthorizationExpired      RefusalClass = "authorization_expired"
	ClassAuthorizationRevoked      RefusalClass = "authorization_revoked"
	ClassAuthorizationUnverifiable RefusalClass = "authorization_unverifiable"
	ClassAuthorizationChanged      RefusalClass = "authorization_changed"
	ClassScopeMismatch             RefusalClass = "scope_mismatch"
	ClassSenderUnregistered        RefusalClass = "sender_unregistered"
	ClassRecoveryPaused            RefusalClass = "recovery_paused"
	ClassRecoveryActive            RefusalClass = "recovery_active"
	ClassRecoveryVersionChanged    RefusalClass = "recovery_version_changed"
	ClassGateReadFailed            RefusalClass = "gate_read_failed"
	ClassBindingPaused             RefusalClass = "binding_paused"
	ClassBindingAbsent             RefusalClass = "binding_absent"
	ClassBindingConflict           RefusalClass = "binding_conflict"
	ClassBindingTerminal           RefusalClass = "binding_terminal"
	ClassBindingReadFailed         RefusalClass = "binding_read_failed"
	ClassClaimLost                 RefusalClass = "claim_lost"
	ClassClaimNotCurrent           RefusalClass = "claim_not_current"
	ClassStepOpenUnreconciled      RefusalClass = "step_open_unreconciled"
	ClassLifecycleUnavailable      RefusalClass = "lifecycle_unavailable"
	ClassOperationConflict         RefusalClass = "operation_conflict"
)

// retryable is the §5 Retryable column. An unknown class never retries.
var retryable = map[RefusalClass]bool{
	ClassExecutionPermissionDenied: false,
	ClassAuthorizationInvalid:      true,
	ClassAuthorizationExpired:      false,
	ClassAuthorizationRevoked:      false,
	ClassAuthorizationUnverifiable: false,
	ClassAuthorizationChanged:      true,
	ClassScopeMismatch:             false,
	ClassSenderUnregistered:        false,
	ClassRecoveryPaused:            true,
	ClassRecoveryActive:            true,
	ClassRecoveryVersionChanged:    true,
	ClassGateReadFailed:            true,
	ClassBindingPaused:             true,
	ClassBindingAbsent:             false,
	ClassBindingConflict:           false,
	ClassBindingTerminal:           false,
	ClassBindingReadFailed:         true,
	ClassClaimLost:                 true,
	ClassClaimNotCurrent:           true,
	ClassStepOpenUnreconciled:      true,
	ClassLifecycleUnavailable:      true,
	ClassOperationConflict:         false,
}

// Retryable reports the §5 retryability column.
func (c RefusalClass) Retryable() bool { return retryable[c] }

// execution_events.kind closed list (data-model Table 4, mirroring the
// migration's execution_events_kind_check verbatim).
const (
	EventAdmitted            = "admitted"
	EventAdmissionRefused    = "admission_refused"
	EventClaimed             = "claimed"
	EventReleased            = "released"
	EventTakenOver           = "taken_over"
	EventRevoked             = "revoked"
	EventStallFlagged        = "stall_flagged"
	EventStateChanged        = "state_changed"
	EventStepIssued          = "step_issued"
	EventStepConverged       = "step_converged"
	EventStepRefused         = "step_refused"
	EventStepUnknown         = "step_unknown"
	EventReconcileObserved   = "reconcile_observed"
	EventRevisionApplied     = "revision_applied"
	EventProjectionStale     = "projection_stale"
	EventProjectionRefreshed = "projection_refreshed"
)

// EventKinds is the closed 16-kind list in the migration's CHECK order.
var EventKinds = []string{
	EventAdmitted, EventAdmissionRefused, EventClaimed, EventReleased,
	EventTakenOver, EventRevoked, EventStallFlagged, EventStateChanged,
	EventStepIssued, EventStepConverged, EventStepRefused, EventStepUnknown,
	EventReconcileObserved, EventRevisionApplied, EventProjectionStale,
	EventProjectionRefreshed,
}

// ValidEventKind reports whether kind is in the closed list.
func ValidEventKind(kind string) bool {
	for _, k := range EventKinds {
		if k == kind {
			return true
		}
	}
	return false
}
