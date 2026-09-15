// errors.go owns the stable machine codes and the single classified error
// type carried across the withdrawal package (contracts/api.md §1, FR-14).
// Codes are wire vocabulary: HTTP handlers map them to status codes, and
// errors never carry key material or other secrets (constitution §I/§XII).
package withdrawal

// Code is a stable machine-readable failure code returned to API callers
// (contracts/api.md §1). Its value is the wire string and MUST NOT be reworded
// once published.
type Code string

// The withdrawal machine codes (contracts/api.md §1). Each maps to exactly one
// HTTP status in the 007 contract.
const (
	// CodeMalformedRequest is a request body that is not valid JSON (400).
	CodeMalformedRequest Code = "malformed_request"
	// CodeValidationFailed is a schema or parameter violation (422); the
	// offending field, when known, is carried in Error.Field.
	CodeValidationFailed Code = "validation_failed"
	// CodeUnauthenticated is a missing, invalid, or revoked API key (401).
	CodeUnauthenticated Code = "unauthenticated"
	// CodeUnauthorized is a caller without interface permission (403).
	CodeUnauthorized Code = "unauthorized"
	// CodeAuthorizationInvalid is a missing, inactive, or mismatched grant
	// (403).
	CodeAuthorizationInvalid Code = "authorization_invalid"
	// CodeIdempotencyConflict is a replay whose business parameters or
	// authorization id differ from the original request (409).
	CodeIdempotencyConflict Code = "idempotency_conflict"
	// CodeNotFound is a nonexistent or unowned withdrawal; both render
	// identically to reduce existence leakage (404).
	CodeNotFound Code = "not_found"
	// CodeTemporarilyUnavailable is storage unavailability or an uncertain
	// submit outcome; the caller MUST retry with the same key and parameters,
	// never rotating keys (503).
	CodeTemporarilyUnavailable Code = "temporarily_unavailable"
	// CodeOperationConflict is a conflict between two legal operations against
	// the same resource (409).
	CodeOperationConflict Code = "operation_conflict"
)

// Error is the single classified failure type for the withdrawal package. It
// carries a stable Code and an optional Field naming the offending request
// parameter; it never carries key material or other secrets.
type Error struct {
	Code  Code   // stable machine code (contracts/api.md §1)
	Field string // offending request field, empty when not applicable
	msg   string // human-readable message
	cause error  // wrapped internal error, if any
}

// New builds an Error with the given machine code and human-readable message.
func New(code Code, msg string) *Error {
	return &Error{Code: code, msg: msg}
}

// WithField returns a copy of e with Field set to the offending request
// parameter. It does not mutate e, so a base error may be extended along
// several branches without aliasing.
func (e *Error) WithField(field string) *Error {
	clone := *e
	clone.Field = field
	return &clone
}

// Error renders "[code] msg", appending " field=..." when a field is set.
func (e *Error) Error() string {
	s := "[" + string(e.Code) + "] " + e.msg
	if e.Field != "" {
		s += " field=" + e.Field
	}
	return s
}

// Unwrap returns the wrapped internal cause, letting errors.Is and errors.As
// traverse to it. It is nil when the error has no cause.
func (e *Error) Unwrap() error { return e.cause }
