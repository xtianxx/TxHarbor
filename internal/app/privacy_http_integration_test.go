//go:build integration

// privacy_http_integration_test.go closes review gap #4: the V9/FR-15 404
// normalization was proven only at the GetWithdrawal library layer
// (internal/withdrawal/privacy_integration_test.go), never on the wire. This
// test drives a real httptest.Server over a real PostgreSQL 18 container
// (testcontainers) and compares the two hostile GETs at the HTTP boundary.
//
// A second caller/key (caller B) probes (a) caller A's real request_id and
// (b) a well-formed id that never existed. The contract normalizes
// code/message/shape, so the two responses MUST agree on status, body bytes,
// decoded error shape, Content-Length, and every remaining header. Only the
// per-request trace id (header-only by design, contracts/api.md §1) and the
// server Date may differ.
//
// All seeding reuses the reviewed app-package helpers (withdrawalHTTPSetup/
// Key/Supply/SeedPolicy/Handler/CreateBody); no raw INSERT and no non-test
// file is touched. Helpers here are privacy-prefixed so no name collides with
// the other 007 test files.
package app

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// privacyHTTPGet fires one authenticated GET and returns the status, the full
// body bytes, and a clone of the response headers so the test can compare the
// wire shape, not just the body.
func privacyHTTPGet(t *testing.T, url, token string) (int, []byte, http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new GET %s: %v", url, err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, raw, resp.Header.Clone()
}

// privacyWireHeaders returns the response headers minus the per-request
// trace/date members the FR-15 contract deliberately leaves unnormalized.
func privacyWireHeaders(h http.Header) http.Header {
	clone := h.Clone()
	clone.Del("X-Request-Trace-Id")
	clone.Del("Date")
	return clone
}

// TestWithdrawalHTTPPrivacyNotFoundWireEqual is review gap #4: caller B's
// hostile GET of caller A's real request id and its GET of a random
// nonexistent id MUST be indistinguishable at the HTTP boundary — same 404
// status, same body bytes, same decoded code/message/shape, same
// Content-Length, and same headers once the contract-excluded per-request
// trace id (header-only) and Date are removed.
func TestWithdrawalHTTPPrivacyNotFoundWireEqual(t *testing.T) {
	ctx, pool, _ := withdrawalHTTPSetup(t)
	const (
		callerA = int64(8401)
		callerB = int64(8402)
	)
	keyA := withdrawalHTTPKey(t, ctx, pool, callerA)
	keyB := withdrawalHTTPKey(t, ctx, pool, callerB)
	if keyA == keyB {
		t.Fatal("keys A and B are identical, want distinct credentials")
	}
	withdrawalHTTPSupply(t, ctx, pool, callerA, "auth-privacy-wire-a")
	withdrawalHTTPSeedPolicy(t, ctx, pool)

	srv := httptest.NewServer(withdrawalHTTPHandler(pool))
	defer srv.Close()

	status, raw := withdrawalHTTPDo(t, http.MethodPost, srv.URL+"/withdrawals", keyA,
		withdrawalHTTPCreateBody(t, "idem-privacy-wire-a", withdrawalHTTPAmount, "auth-privacy-wire-a"))
	if status != http.StatusCreated {
		t.Fatalf("create status = %d (%s), want 201", status, raw)
	}
	var created withdrawalPostResponse
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("decode create body %s: %v", raw, err)
	}
	if created.RequestID == "" {
		t.Fatal("create body carries no request_id")
	}

	// The random id is well-formed (wr- + 32 hex) yet was never created.
	randomID := "wr-" + strings.Repeat("0", 32)

	// Key B (a second caller/key) probes A's real id and the random absent id.
	foreignStatus, foreignBody, foreignHeaders := privacyHTTPGet(t, srv.URL+"/withdrawals/"+created.RequestID, keyB)
	randomStatus, randomBody, randomHeaders := privacyHTTPGet(t, srv.URL+"/withdrawals/"+randomID, keyB)

	// Equal 404 status.
	if foreignStatus != http.StatusNotFound || randomStatus != http.StatusNotFound {
		t.Fatalf("404 statuses = (%d, %d), want (404, 404)", foreignStatus, randomStatus)
	}

	// Equal normalized error code/message/shape bytes.
	if !bytes.Equal(foreignBody, randomBody) {
		t.Fatalf("404 bodies differ:\nforeign=%s\nrandom=%s", foreignBody, randomBody)
	}
	var foreignErr, randomErr withdrawalErrorResponse
	if err := json.Unmarshal(foreignBody, &foreignErr); err != nil {
		t.Fatalf("decode foreign 404 body %s: %v", foreignBody, err)
	}
	if err := json.Unmarshal(randomBody, &randomErr); err != nil {
		t.Fatalf("decode random 404 body %s: %v", randomBody, err)
	}
	if foreignErr.Code != string(withdrawal.CodeNotFound) || foreignErr.Message != "withdrawal not found" {
		t.Fatalf("foreign 404 = %+v, want code %q message %q", foreignErr, withdrawal.CodeNotFound, "withdrawal not found")
	}
	if foreignErr != randomErr {
		t.Fatalf("404 shapes differ: foreign=%+v random=%+v", foreignErr, randomErr)
	}
	if foreignErr.RequestID != "" || foreignErr.TraceID != "" {
		t.Fatalf("404 body leaks request-scoped members: %+v (GET body must omit request_id and trace_id)", foreignErr)
	}

	// Equal Content-Length, matching the actual bytes on both responses.
	foreignLen := foreignHeaders.Get("Content-Length")
	if randomLen := randomHeaders.Get("Content-Length"); foreignLen != randomLen {
		t.Fatalf("Content-Length differs: foreign=%q random=%q", foreignLen, randomLen)
	}
	if foreignLen != strconv.Itoa(len(foreignBody)) {
		t.Fatalf("Content-Length = %q, want %d (actual body length)", foreignLen, len(foreignBody))
	}

	// Every header except the contract-excluded trace/date members MUST match
	// byte for byte. The trace id rides the header only by design; Date is
	// server-generated.
	foreignWire := privacyWireHeaders(foreignHeaders)
	randomWire := privacyWireHeaders(randomHeaders)
	if !reflect.DeepEqual(foreignWire, randomWire) {
		t.Fatalf("wire headers differ beyond trace/date:\nforeign=%v\nrandom=%v", foreignWire, randomWire)
	}

	// The trace id is header-only: each 404 carries one, and it never leaks
	// into the body the contract requires to stay byte-equal.
	traceForeign := foreignHeaders.Get("X-Request-Trace-Id")
	traceRandom := randomHeaders.Get("X-Request-Trace-Id")
	if traceForeign == "" || traceRandom == "" {
		t.Fatalf("trace headers = (%q, %q), want a non-empty header trace id on each 404", traceForeign, traceRandom)
	}
	if strings.Contains(string(foreignBody), traceForeign) {
		t.Fatalf("trace id %q leaked into the body %s (must be header-only)", traceForeign, foreignBody)
	}
}
