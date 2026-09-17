//go:build integration

package txlifecycle

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// faultProxy is the T026 test HTTP proxy between 010 and 009: it sits on the
// 010 SignerClient's wire and scripts one 009-boundary fault per mode, counting
// requests so the delivery-unknown retry is observable.
type faultProxy struct {
	mode     string
	requests atomic.Int64
	srv      *httptest.Server
}

func newFaultProxy(t *testing.T, mode string) *faultProxy {
	t.Helper()
	p := &faultProxy{mode: mode}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.requests.Add(1)
		switch p.mode {
		case "timeout":
			time.Sleep(600 * time.Millisecond)
		case "drop":
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("response writer does not support hijack")
				return
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_ = conn.Close()
			return
		case "rate_limited":
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"code":"rate_limited","message":"slow down"}`))
			return
		case "invalid":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`not-json`))
			return
		case "conflict":
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"code":"request_conflict","message":"same identity, different envelope"}`))
			return
		case "unavailable":
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"code":"outcome_unknown","message":"delivery unknown"}`))
			return
		case "unauthorized":
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":"unauthenticated","message":"bad credential"}`))
			return
		case "signed":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"state":"signed","signature":"0x00","tx_hash":"0x00"}`))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *faultProxy) client(t *testing.T) *SignerClient {
	t.Helper()
	c, err := NewSignerClient(SignerConfig{BaseURL: p.srv.URL, Credential: "test-credential", Timeout: 300 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewSignerClient: %v", err)
	}
	return c
}

// signerClass extracts the refusal class from either boundary error shape:
// SignerError (transport/503/refusal survivors) or RefusalError (409 conflict).
func signerClass(err error) RefusalClass {
	var se *SignerError
	if errors.As(err, &se) {
		return se.Class
	}
	return refusalClass(err)
}

// TestT026SignerFaultProxy is the classification matrix over the proxy: a
// timeout/drop/503 is delivery-unknown and retried on the SAME identity; 429,
// invalid body, 401 and 409 request_conflict are fail-closed with exactly one
// request (never re-signed under a new identity).
func TestT026SignerFaultProxy(t *testing.T) {
	attempt := &Attempt{AttemptID: "att-fault", SigningRequestID: "sr-fault", CanonicalEnvelope: `{"signing_request_id":"sr-fault"}`}

	cases := []struct {
		mode        string
		wantClass   RefusalClass
		wantReqs    int64
		wantUnknown bool
	}{
		{"timeout", ClassCoordinationUnavailable, signerRetryAttempts, true},
		{"drop", ClassCoordinationUnavailable, signerRetryAttempts, true},
		{"unavailable", ClassCoordinationUnavailable, signerRetryAttempts, true},
		{"rate_limited", ClassSignatureRefused, 1, false},
		{"invalid", ClassSignatureRefused, 1, false},
		{"conflict", ClassAttemptConflict, 1, false},
		{"unauthorized", ClassSignatureRefused, 1, false},
	}
	for _, c := range cases {
		t.Run(c.mode, func(t *testing.T) {
			p := newFaultProxy(t, c.mode)
			client := p.client(t)
			_, err := client.Submit(context.Background(), attempt)
			if err == nil {
				t.Fatal("expected a signer-boundary error")
			}
			if got := signerClass(err); got != c.wantClass {
				t.Fatalf("class = %s, want %s (%v)", got, c.wantClass, err)
			}
			var se *SignerError
			if errors.As(err, &se) {
				if se.DeliveryUnknown != c.wantUnknown {
					t.Fatalf("deliveryUnknown = %v, want %v", se.DeliveryUnknown, c.wantUnknown)
				}
			} else if c.wantUnknown {
				t.Fatalf("error %v carries no DeliveryUnknown marker", err)
			}
			if got := p.requests.Load(); got != c.wantReqs {
				t.Fatalf("requests = %d, want %d", got, c.wantReqs)
			}
		})
	}
}

// TestT026SignedBodyAccepted proves the pass-through path still yields the 009
// signed result (signature + tx_hash), so the matrix above is a fault layer,
// not a blanket refusal.
func TestT026SignedBodyAccepted(t *testing.T) {
	p := newFaultProxy(t, "signed")
	client := p.client(t)
	res, err := client.Submit(context.Background(), &Attempt{AttemptID: "att-ok", CanonicalEnvelope: `{"signing_request_id":"sr-ok"}`})
	if err != nil {
		t.Fatalf("signed submit: %v", err)
	}
	if res.Signature != "0x00" || res.TxHash != "0x00" {
		t.Fatalf("signed result = %+v, want signature/tx_hash passthrough", res)
	}
	if got := p.requests.Load(); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
}

// TestT026ProxyPostCommitLossRetry documents the post-COMMIT response-loss
// retry: the proxy accepts (the durable 010 row already exists) and answers
// request_conflict for the same identity; 010 classifies attempt_conflict with
// one request and never mints a second identity.
func TestT026ProxyPostCommitLossRetry(t *testing.T) {
	p := newFaultProxy(t, "conflict")
	client := p.client(t)
	_, err := client.Submit(context.Background(), &Attempt{AttemptID: "att-commit", CanonicalEnvelope: `{"signing_request_id":"sr-commit"}`})
	if got := signerClass(err); got != ClassAttemptConflict {
		t.Fatalf("post-commit retry = %v (%s), want attempt_conflict", err, got)
	}
	if got := p.requests.Load(); got != 1 {
		t.Fatalf("requests = %d, want 1 (no duplicate signing request)", got)
	}
}

// TestT026ProxyNeverLeaksCredential pins the transport shape: the bearer
// credential reaches 009 but never appears in the error text.
func TestT026ProxyNeverLeaksCredential(t *testing.T) {
	p := newFaultProxy(t, "unauthorized")
	client := p.client(t)
	_, err := client.Submit(context.Background(), &Attempt{AttemptID: "att-secret", CanonicalEnvelope: `{}`})
	if err == nil || strings.Contains(err.Error(), "test-credential") {
		t.Fatalf("credential leaked or no error: %v", err)
	}
}

// TestT026ProxyUpstreamReachable is a guard against a dead test server: the
// proxy must actually be listening.
func TestT026ProxyUpstreamReachable(t *testing.T) {
	p := newFaultProxy(t, "signed")
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(p.srv.URL, "http://"), time.Second)
	if err != nil {
		t.Fatalf("proxy not reachable: %v", err)
	}
	_ = conn.Close()
}
