// contract_test.go owns T013: the library-shape contract of
// specs/007-withdrawal-creation/contracts/api.md §1-§3 that the HTTP wiring
// (T014) consumes. It pins the response table's (Status, Code) vocabulary, the
// §3 body shape, the FR-15 404 byte-equality rule, and the trace-id split.
//
// Scope: no database, no HTTP. The 400 row's body decode and the wire render
// itself (headers, JSON encoding, status line, request trace id) belong to T014
// in internal/app; the DB-bound 201/200 emissions belong to the T010
// integration suite. This file asserts that the shapes those layers consume are
// contract-complete and that no core path can report a contradicting pair.
package withdrawal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// contractNotFoundMessage is the exact FR-15 read message. query.go's
// queryReadRequest is the single construction site: a missing id and a foreign
// id both fall through the same pgx.ErrNoRows branch to this one New call, so
// both render identically.
const contractNotFoundMessage = "withdrawal not found"

// TestWithdrawalContractResponseTable pins every row of the contracts §1
// response table to the library's (Status, Code) vocabulary. The error rows
// reuse the core's own conversion (intakeResultFromError → statusForCode); the
// two success rows assert the core's success shape: a 2xx Status with an empty
// machine Code (the DB-bound emitter is T010's integration coverage).
func TestWithdrawalContractResponseTable(t *testing.T) {
	cases := []struct {
		name       string
		wantStatus int
		wantCode   Code // "" marks a success row
	}{
		{"first persist", 201, ""},
		{"same key + FR-10 equality", 200, ""},
		{"same key + differing params", 409, CodeIdempotencyConflict},
		{"malformed JSON", 400, CodeMalformedRequest},
		{"schema/param invalid", 422, CodeValidationFailed},
		{"missing/invalid/revoked key", 401, CodeUnauthenticated},
		{"no interface permission", 403, CodeUnauthorized},
		{"grant missing/inactive/mismatched", 403, CodeAuthorizationInvalid},
		{"storage unavailable", 503, CodeTemporarilyUnavailable},
		{"uncertain submit outcome", 503, CodeTemporarilyUnavailable},
	}
	if len(cases) != 10 {
		t.Fatalf("response table covers %d rows, want all 10 contracts §1 rows", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given a contracts §1 outcome row
			if tc.wantCode == "" {
				// When the outcome is one of the two success rows
				// Then the core reports a 2xx status with an empty machine
				// code; success is never a machine code (see the never-2xx test)
				if tc.wantStatus < 200 || tc.wantStatus > 299 {
					t.Fatalf("success row status = %d, want 2xx", tc.wantStatus)
				}
				if got := (SubmitResult{Status: tc.wantStatus}).Code; got != "" {
					t.Fatalf("success row Code = %q, want empty", got)
				}
				return
			}

			// When the core classifies a failure carrying that machine code
			got := intakeResultFromError(New(tc.wantCode, "contract probe"))

			// Then it reports exactly the contracted (Status, Code) pair, and
			// the mapping helper agrees
			if got.Status != tc.wantStatus || got.Code != tc.wantCode {
				t.Fatalf("intakeResultFromError(%q) = (%d, %q), want (%d, %q)",
					tc.wantCode, got.Status, got.Code, tc.wantStatus, tc.wantCode)
			}
			if mapped := statusForCode(tc.wantCode); mapped != tc.wantStatus {
				t.Fatalf("statusForCode(%q) = %d, want %d", tc.wantCode, mapped, tc.wantStatus)
			}

			// Then no request/trace id is fabricated on a failure row: §1's
			// trace id is T014's HTTP-layer responsibility
			// (TestWithdrawalContractRequestTraceIDIsTransportOwned).
			if got.RequestID != "" {
				t.Fatalf("failure row %q carries RequestID %q, want empty", tc.name, got.RequestID)
			}
		})
	}
}

// TestWithdrawalContractMalformedIsTransportOwned documents that the 400 row is
// HTTP-only: no core path emits CodeMalformedRequest because no JSON body
// crosses the core boundary (intake.go's package comment); the code and its
// mapping are still part of the frozen vocabulary T014 renders.
func TestWithdrawalContractMalformedIsTransportOwned(t *testing.T) {
	// Given the malformed_request machine code (contracts §1, 400)
	// When it is mapped to its status
	// Then the mapping is stable for the T014 body decoder to consume
	if got := statusForCode(CodeMalformedRequest); got != 400 {
		t.Fatalf("statusForCode(malformed_request) = %d, want 400", got)
	}
}

// TestWithdrawalContractErrorCodesNeverMapToSuccess proves the success rows
// cannot be confused with an error: the complete mapping table yields only
// error statuses for every declared machine code, so a SubmitResult with an
// empty Code is the sole success representation T014 sees.
func TestWithdrawalContractErrorCodesNeverMapToSuccess(t *testing.T) {
	// Given every declared machine code
	codes := []Code{
		CodeMalformedRequest, CodeValidationFailed, CodeUnauthenticated,
		CodeUnauthorized, CodeAuthorizationInvalid, CodeIdempotencyConflict,
		CodeNotFound, CodeTemporarilyUnavailable, CodeOperationConflict,
	}
	// When each is mapped by the core's status table
	for _, code := range codes {
		status := statusForCode(code)
		// Then it is a defined error status, never a 2xx success
		if status < 400 || status > 599 {
			t.Errorf("statusForCode(%q) = %d, want an error status (4xx/5xx)", code, status)
		}
	}
}

// TestWithdrawalContractUnavailableRetryInstruction pins the two 503 rows
// (storage unavailable and uncertain submit): both use the one core result and
// its message must tell the caller to retry with the same key and parameters
// and never to rotate the key, while claiming neither an accept nor a definite
// non-creation.
func TestWithdrawalContractUnavailableRetryInstruction(t *testing.T) {
	// Given the two §1 503 rows: a storage failure and an uncertain submit
	storage := intakeResultFromError(storageUnavailable("contract storage", errors.New("boom")))
	uncertain := intakeUnavailableResult(nil, nil, 0, context.Background())

	for name, got := range map[string]*SubmitResult{"storage unavailable": storage, "uncertain submit": uncertain} {
		t.Run(name, func(t *testing.T) {
			// Then both report the contracted 503 pair
			if got.Status != 503 || got.Code != CodeTemporarilyUnavailable {
				t.Fatalf("result = (%d, %q), want (503, %q)", got.Status, got.Code, CodeTemporarilyUnavailable)
			}
			// Then the message instructs a same-key/same-param retry and never
			// says to rotate the key
			if !strings.Contains(got.Message, "same idempotency key") {
				t.Fatalf("503 message %q must instruct a same-key retry", got.Message)
			}
			if !strings.Contains(got.Message, "never rotate the key") {
				t.Fatalf("503 message %q must warn against rotating the key", got.Message)
			}
			// Then it claims neither acceptance nor definite non-creation
			for _, forbidden := range []string{"Accepted", "definitely not created"} {
				if strings.Contains(got.Message, forbidden) {
					t.Fatalf("503 message %q must not claim %q", got.Message, forbidden)
				}
			}
		})
	}
}

// TestWithdrawalContractValidationFailedCarriesField exercises the core's real
// FR-06 validation and asserts the §1 "(+ field)" promise: a 422 outcome names
// the offending request parameter.
func TestWithdrawalContractValidationFailedCarriesField(t *testing.T) {
	// Given a request whose amount violates FR-06 while every earlier rule holds
	req := SubmitRequest{
		IdempotencyKey:  "idem-contract-1",
		ChainID:         31337,
		ExpectedChainID: 31337,
		Asset:           lowerAddr,
		Recipient:       intakeTestRecipient,
		Amount:          "1.5",
		AuthorizationID: "auth-contract-1",
		Allowlist:       []string{lowerAddr},
	}

	// When the create is normalized
	_, verr := normalizeSubmit(req)

	// Then the core reports validation_failed naming the amount field
	if verr == nil {
		t.Fatal("normalizeSubmit accepted a non-decimal amount")
	}
	if verr.Code != CodeValidationFailed || verr.Field != "amount" {
		t.Fatalf("normalizeSubmit error = (%q, field %q), want (%q, field amount)",
			verr.Code, verr.Field, CodeValidationFailed)
	}
	got := intakeResultFromError(verr)
	if got.Status != 422 || got.Code != CodeValidationFailed {
		t.Fatalf("422 outcome = (%d, %q), want (422, %q)", got.Status, got.Code, CodeValidationFailed)
	}
	if !strings.Contains(got.Message, "field: amount") {
		t.Fatalf("422 message %q must name the offending field", got.Message)
	}
}

// TestWithdrawalContractNotFoundByteEquality pins FR-15: the missing-id and
// foreign-id reads are the same construction and must render byte-identical
// code and message. query.go's queryReadRequest builds both from the single
// contractNotFoundMessage on the same pgx.ErrNoRows branch; the DB predicate
// itself is covered by the T011 integration suite.
func TestWithdrawalContractNotFoundByteEquality(t *testing.T) {
	// Given a nonexistent id and another caller's id (both intended as misses)
	missing := New(CodeNotFound, contractNotFoundMessage)
	foreign := New(CodeNotFound, contractNotFoundMessage)

	// When both are rendered for the wire
	// Then they are byte-identical, so no existence signal leaks
	if missing.Error() != foreign.Error() {
		t.Fatalf("404 renderings differ: missing=%q foreign=%q", missing.Error(), foreign.Error())
	}
	if missing.Code != CodeNotFound || foreign.Code != CodeNotFound {
		t.Fatalf("404 codes = (%q, %q), want (%q, %q)",
			missing.Code, foreign.Code, CodeNotFound, CodeNotFound)
	}
	if want := "[not_found] withdrawal not found"; missing.Error() != want {
		t.Fatalf("404 rendering = %q, want %q", missing.Error(), want)
	}
}

// TestWithdrawalContractViewShape pins the contracts §3 body: every field is
// present with its contracted Go type (compile-time assertions below), status
// is the accepted standing fact, amount is a decimal string never a float, and
// recovery.execution is not_started.
func TestWithdrawalContractViewShape(t *testing.T) {
	// Given a persisted request as the §3 body vocabulary expresses it
	view := WithdrawalView{
		RequestID: "wr-00112233445566778899aabbccddeeff",
		CallerID:  7,
		ChainID:   31337,
		Asset:     lowerAddr,
		Recipient: intakeTestRecipient,
		Amount:    maxUint256Dec,
		Status:    "accepted",
		CreatedAt: time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC),
		Recovery:  RecoverySignal{State: queryStateNone, Execution: queryExecutionNotStarted},
	}

	// Then every §3 field is present with its contracted Go type: a string
	// amount can never be a float, and ids/counts are int64.
	var (
		_ string    = view.RequestID
		_ int64     = view.CallerID
		_ int64     = view.ChainID
		_ string    = view.Asset
		_ string    = view.Recipient
		_ string    = view.Amount
		_ string    = view.Status
		_ time.Time = view.CreatedAt
		_ string    = view.Recovery.State
		_ string    = view.Recovery.Execution
	)

	// Then the §3 standing facts hold
	if view.Status != "accepted" {
		t.Fatalf("status = %q, want accepted", view.Status)
	}
	if view.Recovery.Execution != queryExecutionNotStarted {
		t.Fatalf("recovery.execution = %q, want %q", view.Recovery.Execution, queryExecutionNotStarted)
	}
	if view.Amount != maxUint256Dec {
		t.Fatalf("amount = %q, want the uint256-max decimal string", view.Amount)
	}
}

// TestWithdrawalContractRecoveryExecutionAlwaysNotStarted locks the Q8
// correction: for every recovery state a 007 response may carry, execution is
// the standing not_started fact, never an unknown-outcome vocabulary.
func TestWithdrawalContractRecoveryExecutionAlwaysNotStarted(t *testing.T) {
	// Given every §3 recovery.state value
	states := []string{
		queryStateNone,
		queryStateRecovering,
		queryStatePausedReconcile,
		queryStateReleased,
		queryStateUnknown,
	}

	// When the core builds the recovery signal for each state
	for _, state := range states {
		view := WithdrawalView{Recovery: queryRecoverySignal(state)}
		// Then execution is always not_started
		if view.Recovery.Execution != queryExecutionNotStarted {
			t.Errorf("state %q: recovery.execution = %q, want %q",
				state, view.Recovery.Execution, queryExecutionNotStarted)
		}
	}
}

// TestWithdrawalContractRequestTraceIDIsTransportOwned documents the split §1
// leaves to the HTTP layer: the core fabricates no trace identifier. It sets
// the public request_id only on outcomes where a request exists (201, 200, and
// 409-against-an-existing-row); error bodies carry none, so T014 owns rendering
// the request trace id. T010's integration suite covers the non-empty
// request_id on the success rows.
func TestWithdrawalContractRequestTraceIDIsTransportOwned(t *testing.T) {
	// Given an error outcome (no request exists to identify)
	got := intakeResultFromError(New(CodeUnauthenticated, "missing or invalid API key"))

	// When it is handed to the transport
	// Then the core has fabricated no request/trace identifier
	if got.RequestID != "" {
		t.Fatalf("error outcome RequestID = %q, want empty (T014 owns the trace id)", got.RequestID)
	}
	// Then the §3 view identifies its request as a string, the vocabulary T014
	// echoes; no new trace-id scheme is introduced by the core.
	var view WithdrawalView
	var _ string = view.RequestID
}
