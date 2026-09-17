package signer

// errors.go owns the refusal taxonomy: the exact machine classes of
// contracts/persistence.md §4 (one vocabulary shared by audit rows, logs and
// the HTTP contract) and their retryability. HTTP status mapping of a class is
// fixed by contracts/api.md §2 and applied by the serving layer (T002;
// FR-12/FR-23).

// RefusalClass is a machine refusal class from contracts/persistence.md §4.
type RefusalClass string

// The §4 vocabulary. Strings are pinned verbatim by errors_test.go; never
// rename a class without updating the contract.
const (
	ClassUnauthenticated           RefusalClass = "unauthenticated"            // credential missing/invalid/revoked
	ClassSigningNotPermitted       RefusalClass = "signing_not_permitted"      // authenticated, can_sign=false
	ClassMalformedRequest          RefusalClass = "malformed_request"          // body unparseable / unknown fields
	ClassArbitraryDigestRejected   RefusalClass = "arbitrary_digest_rejected"  // digest/hash/message-only content
	ClassValidationFailed          RefusalClass = "validation_failed"          // field-shape/semantics
	ClassPolicyRefused             RefusalClass = "policy_refused"             // chain/sender/asset/recipient/caps
	ClassRequestConflict           RefusalClass = "request_conflict"           // same identity, different envelope
	ClassBindingAbsent             RefusalClass = "binding_absent"             // 008: no binding
	ClassBindingConflict           RefusalClass = "binding_conflict"           // 008: binding mismatch
	ClassBindingPaused             RefusalClass = "binding_paused"             // 008: paused/reconciling
	ClassBindingTerminal           RefusalClass = "binding_terminal"           // 008: terminal, never signable
	ClassBindingReadFailed         RefusalClass = "binding_read_failed"        // 008: read failed/indeterminate
	ClassAuthorizationInvalid      RefusalClass = "authorization_invalid"      // 007: absent/inactive/mismatch
	ClassAuthorizationExpired      RefusalClass = "authorization_expired"      // 007: expired
	ClassAuthorizationRevoked      RefusalClass = "authorization_revoked"      // 007: revoked
	ClassAuthorizationUnverifiable RefusalClass = "authorization_unverifiable" // 007: no verifiable carrier
	ClassRecoveryPaused            RefusalClass = "recovery_paused"            // 006 pause row
	ClassRecoveryActive            RefusalClass = "recovery_active"            // 006 active recovery
	ClassRecoveryVersionChanged    RefusalClass = "recovery_version_changed"   // content built on stale recovery version
	ClassGateReadFailed            RefusalClass = "gate_read_failed"           // gate statement error/indeterminate
	ClassKeyProviderUnavailable    RefusalClass = "key_provider_unavailable"   // signing backend bounded failure
	ClassKeyProviderTimeout        RefusalClass = "key_provider_timeout"       // signing backend deadline
	ClassStorageUnavailable        RefusalClass = "storage_unavailable"        // pool/tx/commit unavailable
	ClassOutcomeNotYetVisible      RefusalClass = "outcome_not_yet_visible"    // row exists, result not yet durable
	ClassSignatureWithheld         RefusalClass = "signature_withheld"         // delivery gate failed after result exists
	ClassOutcomeUnknown            RefusalClass = "outcome_unknown"            // delivery outcome indeterminate
)

// Retryability says how a caller should retry after a class is returned; it is
// a property of the class, not of the caller's patience (persistence.md §4).
type Retryability int

const (
	// RetryNever: same-identity retry cannot succeed; the basis must change
	// upstream, or the content needs a new identity.
	RetryNever Retryability = iota
	// RetryAfterRelease: same-identity retry is valid once the blocking state
	// is released (006 pause/recovery, 008 pause, delivery withholding).
	RetryAfterRelease
	// RetryNow: bounded same-identity retry is expected (transient read,
	// storage or key-provider failure; result not yet durably visible).
	RetryNow
	// RetryReconcile: no re-sign and no new identity; reconcile the persisted
	// outcome, then same-identity retry re-gates (persistence.md §6).
	RetryReconcile
)

// retryabilityByClass is exhaustive over the §4 vocabulary; errors_test.go
// fails if a class is missing from it.
var retryabilityByClass = map[RefusalClass]Retryability{
	ClassUnauthenticated:           RetryNever,
	ClassSigningNotPermitted:       RetryNever,
	ClassMalformedRequest:          RetryNever,
	ClassArbitraryDigestRejected:   RetryNever,
	ClassValidationFailed:          RetryNever,
	ClassPolicyRefused:             RetryNever,
	ClassRequestConflict:           RetryNever,
	ClassBindingAbsent:             RetryNever,
	ClassBindingConflict:           RetryNever,
	ClassBindingPaused:             RetryAfterRelease,
	ClassBindingTerminal:           RetryNever,
	ClassBindingReadFailed:         RetryNow,
	ClassAuthorizationInvalid:      RetryNever,
	ClassAuthorizationExpired:      RetryNever,
	ClassAuthorizationRevoked:      RetryNever,
	ClassAuthorizationUnverifiable: RetryNever,
	ClassRecoveryPaused:            RetryAfterRelease,
	ClassRecoveryActive:            RetryAfterRelease,
	ClassRecoveryVersionChanged:    RetryNever,
	ClassGateReadFailed:            RetryNow,
	ClassKeyProviderUnavailable:    RetryNow,
	ClassKeyProviderTimeout:        RetryNow,
	ClassStorageUnavailable:        RetryNow,
	ClassOutcomeNotYetVisible:      RetryNow,
	ClassSignatureWithheld:         RetryAfterRelease,
	ClassOutcomeUnknown:            RetryReconcile,
}

// RetryabilityOf returns the retryability of c. An unknown class fails closed
// to RetryNever.
func RetryabilityOf(c RefusalClass) Retryability {
	if r, ok := retryabilityByClass[c]; ok {
		return r
	}
	return RetryNever
}
