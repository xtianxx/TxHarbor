// withdrawalhttp_test.go owns the T014 transport-only unit surface: no
// database, no network. It proves the pre-core gates (malformed JSON 400,
// missing/invalidBearer 401, method mismatch 405), the trace-id rendering, and
// that a caller_id smuggled into the body can never authenticate.
package app

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// withdrawalHTTPTraceShape is the mints trace-id format: "tr-" + 8 lowercase hex.
var withdrawalHTTPTraceShape = regexp.MustCompile(`^tr-[0-9a-f]{8}$`)

// withdrawalHTTPPost fires one POST /withdrawals through the handler (nil pool:
// only the pre-core gates are exercised).
func withdrawalHTTPPost(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	h := &WithdrawalHandler{}
	req := httptest.NewRequest(http.MethodPost, "/withdrawals", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestWithdrawalHTTPMalformedJSONIs400 covers the handler-only 400 row: a body
// that is not valid JSON is rejected before any database access.
func TestWithdrawalHTTPMalformedJSONIs400(t *testing.T) {
	// Given a POST whose body is not valid JSON and no credential
	rec := withdrawalHTTPPost(t, "{not json")

	// When the handler processes it
	// Then it answers 400 malformed_request and does not reach the core
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var body withdrawalErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Code != string(withdrawal.CodeMalformedRequest) {
		t.Fatalf("code = %q, want %q", body.Code, withdrawal.CodeMalformedRequest)
	}
}

// TestWithdrawalHTTPMissingBearerIs401 covers the unauthenticated row: a
// body-valid request with no Authorization header is rejected before the core
// (so no audit intent and no database access).
func TestWithdrawalHTTPMissingBearerIs401(t *testing.T) {
	// Given a structurally plausible body but no Bearer credential
	rec := withdrawalHTTPPost(t, `{"idempotency_key":"idem","chain_id":31337,"amount":"100"}`)

	// Then the handler answers 401 unauthenticated
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	var body withdrawalErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Code != string(withdrawal.CodeUnauthenticated) {
		t.Fatalf("code = %q, want %q", body.Code, withdrawal.CodeUnauthenticated)
	}
}

// TestWithdrawalHTTPCallerIDFromBodyIsIgnored proves identity never comes from
// the body: a caller_id member without a credential still yields 401, and a
// bogus non-Bearer header yields 401 as well (the body field is never read).
func TestWithdrawalHTTPCallerIDFromBodyIsIgnored(t *testing.T) {
	// Given a body that names a caller but presents no credential
	rec := withdrawalHTTPPost(t, `{"caller_id":999,"idempotency_key":"idem","chain_id":31337,"amount":"100"}`)

	// Then the smuggled caller is ignored (401, never 200)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status with body caller_id = %d, want 401", rec.Code)
	}

	// And a non-Bearer Authorization header is also rejected
	h := &WithdrawalHandler{}
	req := httptest.NewRequest(http.MethodPost, "/withdrawals", strings.NewReader(`{"caller_id":999}`))
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("status with Basic credential = %d, want 401", rec2.Code)
	}
}

// TestWithdrawalHTTPMethodMismatchIs405 covers both routes' method gate without
// touching the core or the database.
func TestWithdrawalHTTPMethodMismatchIs405(t *testing.T) {
	h := &WithdrawalHandler{}
	cases := []struct {
		name, method, target string
	}{
		{"GET on the collection", http.MethodGet, "/withdrawals"},
		{"PUT on an item", http.MethodPut, "/withdrawals/wr-00112233445566778899aabbccddeeff"},
		{"DELETE on the collection", http.MethodDelete, "/withdrawals"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.target, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, want 405", rec.Code)
			}
		})
	}
}

// TestWithdrawalHTTPErrorBodiesCarryTraceID pins the §1 request trace id: every
// error body exposes a "tr-…" id in the JSON body and the response header.
func TestWithdrawalHTTPErrorBodiesCarryTraceID(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"malformed", "{", http.StatusBadRequest},
		{"unauthenticated", `{"chain_id":31337}`, http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := withdrawalHTTPPost(t, tc.body)
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d", rec.Code, tc.status)
			}
			var body withdrawalErrorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode error body: %v", err)
			}
			if !withdrawalHTTPTraceShape.MatchString(body.TraceID) {
				t.Fatalf("body trace_id = %q, want tr- + 8 hex", body.TraceID)
			}
			if header := rec.Header().Get("X-Request-Trace-Id"); header != body.TraceID {
				t.Fatalf("header trace = %q, body trace = %q; want equal", header, body.TraceID)
			}
		})
	}
}

// TestWithdrawalHTTPNilPoolIs503 covers the miswired-pool defect on both routes:
// with a well-formed Bearer credential and a nil Pool, GET and POST both answer
// 503 temporarily_unavailable carrying the retry instruction, and neither
// panics. No audit row can be written because no database call is reached.
func TestWithdrawalHTTPNilPoolIs503(t *testing.T) {
	wellFormedKey := "txh_" + base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	cases := []struct {
		name, method, target, body string
	}{
		{
			name:   "GET with valid Bearer",
			method: http.MethodGet,
			target: "/withdrawals/wr-00112233445566778899aabbccddeeff",
		},
		{
			name:   "POST with valid JSON and Bearer",
			method: http.MethodPost,
			target: "/withdrawals",
			body:   `{"idempotency_key":"idem-1","chain_id":31337,"asset":"0x0000000000000000000000000000000000000000","recipient":"0x0000000000000000000000000000000000000000","amount":"100"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &WithdrawalHandler{Pool: nil}
			req := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer "+wellFormedKey)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503", rec.Code)
			}
			var body withdrawalErrorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode error body: %v", err)
			}
			if body.Code != string(withdrawal.CodeTemporarilyUnavailable) {
				t.Fatalf("code = %q, want %q", body.Code, withdrawal.CodeTemporarilyUnavailable)
			}
			if body.Message != withdrawalUnavailableMessage {
				t.Fatalf("message = %q, want %q", body.Message, withdrawalUnavailableMessage)
			}
			if !strings.Contains(body.Message, "retry with the same idempotency key") {
				t.Fatalf("message %q lacks the retry instruction", body.Message)
			}
		})
	}
}
