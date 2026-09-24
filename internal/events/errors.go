package events

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// Closed error taxonomy (T013; data-model §10; contracts/outbox-publisher.md
// §4; contracts/consumer.md §4/§7). PostgreSQL 23505/23514 errors are
// classified by ConstraintName only; unknown errors are non-retryable and
// observable (fail-closed, never a silent retry loop).

// Sentinel classification errors.
var (
	// ErrIdentityConflict is the same-identity/different-content refusal
	// (FR-09/SC-03): the transaction MUST be rejected and the conflict MUST
	// alert; it is never retried as a transient failure and never overwrites.
	ErrIdentityConflict = errors.New("events: identity conflict: same identity with different payload")

	// ErrContract is a catalog/contract or CHECK violation (data-model §10):
	// non-retryable, must alert.
	ErrContract = errors.New("events: contract violation")

	// ErrSchemaUnsupported marks an event whose schema_version is outside the
	// compatibility window. Consumers MUST quarantine it
	// (failure_class=schema_unsupported) and MUST NOT guess-parse (FR-12;
	// contracts/events.md §4.5).
	ErrSchemaUnsupported = errors.New("events: unsupported schema version")
)

// contractErrorf wraps a contract violation with context while keeping
// errors.Is(err, ErrContract) true.
func contractErrorf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrContract, fmt.Sprintf(format, args...))
}

// FailureClass is the closed transient/permanent/contract vocabulary used by
// the publish path (outbox_publish_failures_total{error_class}; verification.md
// §1) and by the consumer bounded-retry decision.
type FailureClass string

const (
	// ClassTransient is retryable: broker unreachable, timeout, throttling,
	// leader switch (contracts/outbox-publisher.md §4).
	ClassTransient FailureClass = "transient"
	// ClassPermanent is non-retryable: serialization/contract validation
	// failure, invalid topic; the publisher blocks the row and alerts, and
	// never drops it.
	ClassPermanent FailureClass = "permanent"
	// ClassContract is a catalog/identity contract violation detected on the
	// publish path; non-retryable and alerting.
	ClassContract FailureClass = "contract"
)

// ClassifiedError carries an explicit failure class.
type ClassifiedError struct {
	Class FailureClass
	Err   error
}

func (e *ClassifiedError) Error() string {
	if e.Err == nil {
		return string(e.Class)
	}
	return fmt.Sprintf("%s: %s", e.Class, e.Err)
}

// Unwrap exposes the wrapped cause to errors.Is/As.
func (e *ClassifiedError) Unwrap() error { return e.Err }

// Transient wraps err as retryable (broker unavailable/timeout/throttled/
// leader switch).
func Transient(err error) error {
	if err == nil {
		return nil
	}
	return &ClassifiedError{Class: ClassTransient, Err: err}
}

// Permanent wraps err as non-retryable (serialization/contract validation/
// invalid topic); the publisher blocks the row and alerts.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &ClassifiedError{Class: ClassPermanent, Err: err}
}

// ClassifyPublishError returns the closed publish class of err:
//   - an explicit Transient wrapper -> transient (retry with backoff);
//   - an explicit Permanent wrapper -> permanent (block + alert);
//   - ErrIdentityConflict / ErrContract -> contract (block + alert);
//   - anything unknown -> permanent. Unknown errors default to non-retryable
//     and stay observable through outbox_publish_failures_total{class}; the
//     publisher blocks the row instead of retrying forever.
func ClassifyPublishError(err error) FailureClass {
	if err == nil {
		return ClassPermanent
	}
	var classified *ClassifiedError
	if errors.As(err, &classified) {
		return classified.Class
	}
	if errors.Is(err, ErrIdentityConflict) || errors.Is(err, ErrContract) {
		return ClassContract
	}
	return ClassPermanent
}

// IsRetryable reports whether the publish class allows a bounded retry with
// backoff. Only the transient class is retryable.
func IsRetryable(err error) bool {
	return ClassifyPublishError(err) == ClassTransient
}

// PostgreSQL SQLSTATE codes classified by this package (data-model §10).
const (
	sqlStateUniqueViolation      = "23505"
	sqlStateCheckViolation       = "23514"
	sqlStateSerializationFailure = "40001"
	sqlStateDeadlockDetected     = "40P01"
)

// ClassifyPGError maps PostgreSQL 23505/23514 to the closed events taxonomy by
// ConstraintName only (data-model §10). Identity-index violations become
// ErrIdentityConflict; every other unique violation and every CHECK violation
// becomes ErrContract. Non-PostgreSQL errors pass through unchanged.
func ClassifyPGError(err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	switch pgErr.Code {
	case sqlStateUniqueViolation:
		switch pgErr.ConstraintName {
		case "outbox_events_object_identity_uniq",
			"outbox_events_log_identity_uniq",
			"outbox_events_event_id_uniq":
			return fmt.Errorf("%w: %s", ErrIdentityConflict, pgErr.ConstraintName)
		default:
			return fmt.Errorf("%w: unexpected unique violation on %s", ErrContract, pgErr.ConstraintName)
		}
	case sqlStateCheckViolation:
		return fmt.Errorf("%w: check violation on %s", ErrContract, pgErr.ConstraintName)
	}
	return err
}

// QuarantineReason is the closed consumer-quarantine vocabulary (data-model
// Table 5; contracts/consumer.md §4/§7).
type QuarantineReason string

const (
	// QuarantineRetryExhausted: the bounded retry budget was exhausted.
	QuarantineRetryExhausted QuarantineReason = "retry_exhausted"
	// QuarantineNonRetryable: a permanent, non-retryable failure.
	QuarantineNonRetryable QuarantineReason = "non_retryable"
	// QuarantineVersionGap: a version gap survived the bounded wait window.
	QuarantineVersionGap QuarantineReason = "version_gap"
	// QuarantineSchemaUnsupported: unknown/unsupported schema_version
	// (fail-closed, never guess-parsed).
	QuarantineSchemaUnsupported QuarantineReason = "schema_unsupported"
	// QuarantineIdentityMismatch: chain identity missing or inconsistent with
	// the local chain.
	QuarantineIdentityMismatch QuarantineReason = "identity_mismatch"
)

// quarantineReasons is the frozen vocabulary in declaration order; the
// consumer_quarantine_failure_class_check CHECK is 1:1 with it.
var quarantineReasons = []QuarantineReason{
	QuarantineRetryExhausted,
	QuarantineNonRetryable,
	QuarantineVersionGap,
	QuarantineSchemaUnsupported,
	QuarantineIdentityMismatch,
}

// QuarantineReasons returns a copy of the frozen vocabulary.
func QuarantineReasons() []QuarantineReason {
	return append([]QuarantineReason(nil), quarantineReasons...)
}

// Valid reports whether r is inside the closed vocabulary.
func (r QuarantineReason) Valid() bool {
	for _, known := range quarantineReasons {
		if r == known {
			return true
		}
	}
	return false
}

// ConsumerRetryable reports whether a consumer-side failure may be retried
// inside the bounded backoff window (FR-14). Explicit transient wrappers and
// the two PostgreSQL concurrency SQLSTATEs are retryable; contract/identity
// failures and anything unclassified are not (fail-closed: unknown failures
// quarantine instead of retrying forever).
func ConsumerRetryable(err error) bool {
	if err == nil {
		return false
	}
	var classified *ClassifiedError
	if errors.As(err, &classified) {
		return classified.Class == ClassTransient
	}
	if errors.Is(err, ErrContract) || errors.Is(err, ErrIdentityConflict) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == sqlStateSerializationFailure || pgErr.Code == sqlStateDeadlockDetected
	}
	return false
}
