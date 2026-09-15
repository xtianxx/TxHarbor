// intake_test.go owns the T010 receipt core's unit surface (no database): the
// FR-10 equality rule, the code→status table of contracts §1, and the
// WriteRejectAudit defect contract (nil pool fails loudly instead of panicking).
package withdrawal

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// intakeTestRecipient is a valid, all-lowercase FR-07 recipient.
const intakeTestRecipient = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// intakeTestParams is the stored-side canonical comparison set.
func intakeTestParams() submitParams {
	return submitParams{
		chainID:         31337,
		asset:           lowerAddr,
		recipient:       intakeTestRecipient,
		amount:          "100",
		authorizationID: "auth-1",
	}
}

// TestWithdrawalIntakeRequestEquality covers FR-10: equality holds for the
// identical set and fails for every single-field difference.
func TestWithdrawalIntakeRequestEquality(t *testing.T) {
	base := requestRow{
		requestID:       "wr-0011",
		authorizationID: "auth-1",
		chainID:         31337,
		asset:           lowerAddr,
		recipient:       intakeTestRecipient,
		amount:          "100",
	}
	p := intakeTestParams()
	if !base.matches(p) {
		t.Fatal("identical request row must satisfy FR-10 equality")
	}

	differ := []struct {
		name   string
		mutate func(*requestRow)
	}{
		{"chain_id", func(r *requestRow) { r.chainID = 1 }},
		{"asset", func(r *requestRow) { r.asset = "0x1111111111111111111111111111111111111111" }},
		{"recipient", func(r *requestRow) { r.recipient = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" }},
		{"amount", func(r *requestRow) { r.amount = "101" }},
		{"authorization_id", func(r *requestRow) { r.authorizationID = "auth-2" }},
	}
	for _, tc := range differ {
		t.Run(tc.name, func(t *testing.T) {
			// Given a stored row that differs from the attempt in one business param
			// When FR-10 equality is evaluated
			// Then it fails (the difference is a 409 conflict signal)
			row := base
			tc.mutate(&row)
			if row.matches(p) {
				t.Fatalf("row differing in %s must not satisfy FR-10 equality", tc.name)
			}
		})
	}
}

// TestWithdrawalIntakeCanonicalAddressEquality proves FR-07/FR-10 address
// equivalence: a valid EIP-55 mixed-case asset/recipient canonicalizes to the
// same lowercase form the row stores, so it satisfies equality.
func TestWithdrawalIntakeCanonicalAddressEquality(t *testing.T) {
	// Given a stored canonical row and the same addresses presented in mixed case
	row := requestRow{
		requestID:       "wr-0011",
		authorizationID: "auth-1",
		chainID:         31337,
		asset:           lowerAddr,
		recipient:       strings.ToLower(otherValidAddr),
		amount:          "100",
	}
	req := SubmitRequest{
		PresentedKey:    "txh_ignored",
		IdempotencyKey:  "idem-1",
		ChainID:         31337,
		ExpectedChainID: 31337,
		Asset:           checksummedAddr,
		Recipient:       otherValidAddr,
		Amount:          "100",
		AuthorizationID: "auth-1",
		Allowlist:       []string{lowerAddr},
	}

	// When the attempt is normalized
	p, err := normalizeSubmit(req)
	if err != nil {
		t.Fatalf("normalizeSubmit() error = %v, want nil", err)
	}

	// Then the canonical forms match (case is not a parameter difference)
	if !row.matches(p) {
		t.Fatalf("case-variant addresses must compare canonically equal: row=%+v params=%+v", row, p)
	}
	if p.asset != lowerAddr {
		t.Fatalf("canonical asset = %q, want %q", p.asset, lowerAddr)
	}
}

// TestWithdrawalIntakeStatusForCode pins the complete machine-code → HTTP status
// table of contracts §1. Every declared code appears exactly once.
func TestWithdrawalIntakeStatusForCode(t *testing.T) {
	cases := []struct {
		code Code
		want int
	}{
		{CodeMalformedRequest, 400},
		{CodeValidationFailed, 422},
		{CodeUnauthenticated, 401},
		{CodeUnauthorized, 403},
		{CodeAuthorizationInvalid, 403},
		{CodeIdempotencyConflict, 409},
		{CodeNotFound, 404},
		{CodeTemporarilyUnavailable, 503},
		{CodeOperationConflict, 409},
	}
	if len(cases) != 9 {
		t.Fatalf("status table covers %d codes, want all 9 declared codes", len(cases))
	}
	for _, tc := range cases {
		if got := statusForCode(tc.code); got != tc.want {
			t.Errorf("statusForCode(%q) = %d, want %d", tc.code, got, tc.want)
		}
	}
}

// TestWithdrawalIntakeUnavailableResultShape pins the 503 contract text: it must
// instruct a same-key retry and never claim "Accepted" or "definitely not
// created".
func TestWithdrawalIntakeUnavailableResultShape(t *testing.T) {
	got := intakeResultFromError(storageUnavailable("test", errors.New("boom")))
	if got.Status != 503 || got.Code != CodeTemporarilyUnavailable {
		t.Fatalf("unavailable result = %+v, want 503/%s", got, CodeTemporarilyUnavailable)
	}
	if !strings.Contains(got.Message, "same idempotency key") {
		t.Fatalf("503 message %q must instruct a same-key retry", got.Message)
	}
	for _, forbidden := range []string{"Accepted", "definitely not created"} {
		if strings.Contains(got.Message, forbidden) {
			t.Fatalf("503 message %q must not contain %q", got.Message, forbidden)
		}
	}
}

// TestWithdrawalIntakeWriteRejectAuditNilPool proves the writer fails loudly on
// a nil pool instead of panicking. WriteRejectAudit cannot validate anything
// without a pool, so nil is rejected first.
func TestWithdrawalIntakeWriteRejectAuditNilPool(t *testing.T) {
	if err := WriteRejectAudit(context.Background(), nil, 1, "wr-x", auditActionRejected, "x"); err == nil {
		t.Fatal("WriteRejectAudit(nil pool) = nil, want an error")
	}
}
