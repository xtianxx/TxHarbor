// grant_attestedby_test.go is the T014 (credential secrecy) attested_by
// equality half at the library boundary: the server-resolved issuance principal
// is bound op-input (contracts/supply-scope.md), so two op-inputs that differ
// ONLY in attested_by must not compare equal, and a scoped supply without an
// attested_by must be a validation failure. No database is needed for either
// rule.
package withdrawal

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// t014ScopedOp builds a minimal valid scoped supply op-input; the attested_by
// semantics under test ride on top of this shape.
func t014ScopedOp(attestedBy string) OpInput {
	return OpInput{
		OperationID:     "01400000000000000000000000000001",
		Action:          grantSupplyAction,
		AuthorizationID: "auth-t014-scope",
		CallerID:        7201,
		ChainID:         31337,
		Asset:           "0x1111111111111111111111111111111111111111",
		Recipient:       "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Amount:          "100",
		IntentID:        "intent-t014",
		RequestID:       "request-t014",
		Sender:          "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		AttestedBy:      attestedBy,
	}
}

// TestWithdrawalGrantAttestedByJoinsOpInputEquality pins the contract line that
// attested_by joins op-input equality: same principal ⇒ identical detail (and
// therefore same-attempt convergence), different principal ⇒ different detail
// (and therefore operation_conflict at the DB boundary).
func TestWithdrawalGrantAttestedByJoinsOpInputEquality(t *testing.T) {
	principalA := t014ScopedOp("principal-a")
	sameA := t014ScopedOp("principal-a")
	principalB := t014ScopedOp("principal-b")

	if opInputDetail(principalA) != opInputDetail(sameA) {
		t.Fatalf("same principal produced different op-input detail:\n a=%s\n b=%s",
			opInputDetail(principalA), opInputDetail(sameA))
	}
	if opInputDetail(principalA) == opInputDetail(principalB) {
		t.Fatalf("op-input detail ignores attested_by: %s", opInputDetail(principalA))
	}
	if !strings.Contains(opInputDetail(principalA), `attested_by="principal-a"`) {
		t.Fatalf("op-input detail does not bind attested_by: %s", opInputDetail(principalA))
	}
}

// TestWithdrawalGrantScopedSupplyRequiresAttestedBy pins the presence rule: a
// scoped supply without a server-resolved attested_by is refused before any
// database access, while the scopeless stock/OPEN path keeps its pre-extension
// shape (attested_by optional).
func TestWithdrawalGrantScopedSupplyRequiresAttestedBy(t *testing.T) {
	scoped := t014ScopedOp("")
	if _, err := validateSupplyOpInput(scoped, time.Now()); err == nil {
		t.Fatal("scoped supply without attested_by validated, want CodeValidationFailed")
	} else {
		var e *Error
		if !errors.As(err, &e) || e.Code != CodeValidationFailed {
			t.Fatalf("scoped-without-attested_by error = %v, want CodeValidationFailed", err)
		}
	}

	stock := t014ScopedOp("")
	stock.IntentID, stock.RequestID, stock.Sender = "", "", ""
	if _, err := validateSupplyOpInput(stock, time.Now()); err != nil {
		t.Fatalf("scopeless stock supply with no attested_by = %v, want valid (pre-extension shape kept)", err)
	}
}
