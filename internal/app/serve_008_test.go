package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/nonce"
)

const testNonceReadToken = "test-nonce-read-token"

type fakeNonceReader struct {
	token string
	resp  nonce.ReadResponse
	err   error
	calls int
	got   nonce.ReadRequest
}

func (f *fakeNonceReader) Authenticate(presented string) bool {
	return presented == f.token
}

func (f *fakeNonceReader) Read(_ context.Context, req nonce.ReadRequest) (nonce.ReadResponse, error) {
	f.calls++
	f.got = req
	return f.resp, f.err
}

func TestNonceReadHandlerAuthAndMethod(t *testing.T) {
	h := &nonceReadHandler{provider: &fakeNonceReader{token: testNonceReadToken}}
	for _, tc := range []struct {
		name          string
		authorization string
	}{
		{"missing", ""},
		{"wrong-bearer", "Bearer wrong"},
		{"no-scheme", testNonceReadToken},
	} {
		req := httptest.NewRequest(http.MethodGet, "/nonce/bindings/b-1", nil)
		if tc.authorization != "" {
			req.Header.Set("Authorization", tc.authorization)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s: status = %d, want 401", tc.name, rec.Code)
		}
		if body := rec.Body.String(); strings.Contains(body, testNonceReadToken) {
			t.Fatalf("%s: 401 body leaked the configured token: %s", tc.name, body)
		}
	}

	// Auth precedes the method check: an unauthenticated non-GET is 401, not
	// 405, so the endpoint's method surface is never disclosed pre-auth.
	reqPost := httptest.NewRequest(http.MethodPost, "/nonce/bindings/b-1", nil)
	recPost := httptest.NewRecorder()
	h.ServeHTTP(recPost, reqPost)
	if recPost.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated POST status = %d, want 401 (auth precedes method)", recPost.Code)
	}
	if recPost.Header().Get("Allow") != "" {
		t.Fatalf("unauthenticated POST leaked Allow: %q", recPost.Header().Get("Allow"))
	}

	req := httptest.NewRequest(http.MethodPost, "/nonce/bindings/b-1", nil)
	req.Header.Set("Authorization", "Bearer "+testNonceReadToken)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("POST status/allow = %d/%q, want 405/GET", rec.Code, rec.Header().Get("Allow"))
	}
}

// TestNonceReadHandlerGateClosedIsUnavailable wires the real provider over a
// closed rebuild gate: an authenticated read is the retryable 503, never a
// served fact (read-api.md §2). A nil pool is deliberate — a closed gate must
// answer before any transaction opens.
func TestNonceReadHandlerGateClosedIsUnavailable(t *testing.T) {
	gate := nonce.NewRebuildGate()
	gate.KeepClosed("rebuild_incomplete: named carrier missing")
	provider := nonce.NewReadProvider(nil, testNonceReadToken, gate)
	h := &nonceReadHandler{provider: provider}

	req := httptest.NewRequest(http.MethodGet, "/nonce/bindings/b-1", nil)
	req.Header.Set("Authorization", "Bearer "+testNonceReadToken)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("gate-closed read status = %d, want 503", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, `"outcome":"unavailable"`) ||
		!strings.Contains(body, `"code":"unavailable"`) {
		t.Fatalf("gate-closed body = %s, want unavailable", body)
	}
}

func TestNonceReadHandlerRoutes(t *testing.T) {
	fake := &fakeNonceReader{
		token: testNonceReadToken,
		resp: nonce.ReadResponse{
			Outcome: nonce.ReadNotBound,
			Error:   &nonce.ReadErrorBody{Code: string(nonce.ReadNotBound)},
			Notice:  nonce.ReadNoticeNotBound,
		},
	}
	h := &nonceReadHandler{provider: fake}
	get := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer "+testNonceReadToken)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	if rec := get("/nonce/bindings/b-1"); rec.Code != http.StatusNotFound || fake.got.BindingID != "b-1" {
		t.Fatalf("binding route: status %d, binding_id %q; want 404/b-1", rec.Code, fake.got.BindingID)
	}

	if rec := get("/nonce/bindings/by-intent/i-1?chain_id=999&sender=0xsender"); rec.Code != http.StatusNotFound ||
		fake.got.IntentID != "i-1" || fake.got.ExpectedChainID == nil ||
		*fake.got.ExpectedChainID != 999 || fake.got.ExpectedSender != "0xsender" {
		t.Fatalf("by-intent route: status %d, req %+v", rec.Code, fake.got)
	}

	if rec := get("/nonce/bindings/by-intent/i-2?chain_id=not-a-number"); rec.Code != http.StatusNotFound ||
		fake.got.ExpectedChainID == nil || *fake.got.ExpectedChainID != 0 {
		t.Fatalf("malformed chain_id: status %d, expected chain %v; want an impossible scope (0)", rec.Code, fake.got.ExpectedChainID)
	}

	before := fake.calls
	if rec := get("/nonce/bindings/"); rec.Code != http.StatusNotFound || fake.calls != before {
		t.Fatalf("identity-less path: status %d, calls %d->%d; want 404 with no provider call", rec.Code, before, fake.calls)
	}
}

func TestRunRebuildGate(t *testing.T) {
	gate := nonce.NewRebuildGate()
	if gate.IsOpen() {
		t.Fatal("zero gate must start closed")
	}
	runRebuildGate(context.Background(), 31337, gate, func(context.Context, int64) (nonce.RebuildReport, error) {
		return nonce.RebuildReport{ChainID: 31337, Scopes: 2, Bindings: 3, Holds: 1}, nil
	})
	if !gate.IsOpen() {
		t.Fatal("successful verification must open the gate")
	}

	failed := nonce.NewRebuildGate()
	wantErr := errors.New("0 of 16 named carriers present")
	runRebuildGate(context.Background(), 31337, failed, func(context.Context, int64) (nonce.RebuildReport, error) {
		return nonce.RebuildReport{}, wantErr
	})
	open, reason := failed.State()
	if open || !strings.Contains(reason, "rebuild_incomplete") || !strings.Contains(reason, wantErr.Error()) {
		t.Fatalf("failed verification: open=%v reason=%q; want closed with structured rebuild_incomplete reason", open, reason)
	}
}
