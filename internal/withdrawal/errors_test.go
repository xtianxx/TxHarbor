// errors_test.go pins the stable machine codes and Error rendering
// (contracts/api.md §1, FR-14).
package withdrawal

import (
	"errors"
	"testing"
)

// TestErrorCodeWireStrings pins every Code constant to its published wire
// string; a rename here is a contract break, not a refactor.
func TestErrorCodeWireStrings(t *testing.T) {
	cases := []struct {
		name string
		code Code
		want string
	}{
		{"malformed request", CodeMalformedRequest, "malformed_request"},
		{"validation failed", CodeValidationFailed, "validation_failed"},
		{"unauthenticated", CodeUnauthenticated, "unauthenticated"},
		{"unauthorized", CodeUnauthorized, "unauthorized"},
		{"authorization invalid", CodeAuthorizationInvalid, "authorization_invalid"},
		{"idempotency conflict", CodeIdempotencyConflict, "idempotency_conflict"},
		{"not found", CodeNotFound, "not_found"},
		{"temporarily unavailable", CodeTemporarilyUnavailable, "temporarily_unavailable"},
		{"operation conflict", CodeOperationConflict, "operation_conflict"},
	}
	if len(cases) != 9 {
		t.Fatalf("expected 9 machine codes, table has %d", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := string(tc.code); got != tc.want {
				t.Errorf("Code wire string = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestErrorRender checks Error() with and without Field.
func TestErrorRender(t *testing.T) {
	base := New(CodeValidationFailed, "amount is not a positive integer")
	if got, want := base.Error(), "[validation_failed] amount is not a positive integer"; got != want {
		t.Errorf("Error() without field = %q, want %q", got, want)
	}

	withField := New(CodeValidationFailed, "amount is not a positive integer").WithField("amount")
	if got, want := withField.Error(), "[validation_failed] amount is not a positive integer field=amount"; got != want {
		t.Errorf("Error() with field = %q, want %q", got, want)
	}
}

// TestErrorWithFieldIsCopy locks WithField as non-mutating: a shared base error
// stays unchanged when a branch attaches a field.
func TestErrorWithFieldIsCopy(t *testing.T) {
	base := New(CodeValidationFailed, "bad value")
	derived := base.WithField("asset")
	if base.Field != "" {
		t.Errorf("WithField mutated the receiver: Field = %q, want empty", base.Field)
	}
	if derived.Field != "asset" {
		t.Errorf("derived Field = %q, want %q", derived.Field, "asset")
	}
	if base == derived {
		t.Error("WithField returned the same pointer, want a distinct copy")
	}
	if base.Error() == derived.Error() {
		t.Error("derived error rendered identically to base, want field appended")
	}
}

// TestErrorAsAndUnwrap checks that errors.As recovers the typed error, that
// Unwrap exposes the cause, and that errors.Is traverses to it.
func TestErrorAsAndUnwrap(t *testing.T) {
	cause := errors.New("storage unavailable")
	err := New(CodeTemporarilyUnavailable, "withdrawal storage unavailable")
	err.cause = cause

	var target *Error
	if !errors.As(err, &target) {
		t.Fatal("errors.As failed to match *Error")
	}
	if target.Code != CodeTemporarilyUnavailable {
		t.Errorf("recovered Code = %q, want %q", target.Code, CodeTemporarilyUnavailable)
	}
	if got := errors.Unwrap(err); got != cause {
		t.Errorf("Unwrap() = %v, want %v", got, cause)
	}
	if !errors.Is(err, cause) {
		t.Error("errors.Is(cause) = false, want true")
	}

	bare := New(CodeNotFound, "not found")
	if got := errors.Unwrap(bare); got != nil {
		t.Errorf("Unwrap() with no cause = %v, want nil", got)
	}
}
