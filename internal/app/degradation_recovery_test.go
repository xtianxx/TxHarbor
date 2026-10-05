// degradation_recovery_test.go pins the T063 recovery-period honesty surface
// (FR-022/026) without a database or control store:
//
//   - the status surface reports the in-process recovery posture (normal /
//     configured / bound) and never a per-capability derived state, so
//     restored/verified/released can never be conflated or displayed from a
//     stale cache;
//   - the probe annotation marks recovery mode without changing readiness,
//     a status code or any capability verdict.
//
// The authoritative bounded review of the derived states is
// `recovery-admin status` (T051/F13) and is exercised by its own tests.
package app

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRecoveryStatusPostureModes(t *testing.T) {
	if got := newRecoveryStatusPosture(nil); got != nil {
		t.Fatalf("newRecoveryStatusPosture(nil) = %+v, want nil (normal mode)", got)
	}
	configured := newRecoveryStatusPosture(&serveRecoveryWiring{})
	if configured == nil || configured.mode != recoveryStatusModeConfigured || configured.instance != "" {
		t.Fatalf("configured posture = %+v, want mode=%s with no instance", configured, recoveryStatusModeConfigured)
	}
	bound := newRecoveryStatusPosture(&serveRecoveryWiring{instance: "  11111111-1111-1111-1111-111111111111 "})
	if bound == nil || bound.mode != recoveryStatusModeBound {
		t.Fatalf("bound posture = %+v, want mode=%s", bound, recoveryStatusModeBound)
	}
	if bound.instance != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("bound instance = %q, want the trimmed binding", bound.instance)
	}
}

// decodeRecoveryStatus drives the real status handler and returns the decoded
// body plus the raw JSON.
func decodeRecoveryStatus(t *testing.T, state *degradationState) (degradationStatusBody, string) {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("GET /status/degradation", &degradationStatusHandler{state: state})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + "/status/degradation")
	if err != nil {
		t.Fatalf("GET /status/degradation: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status endpoint = %d, want 200", resp.StatusCode)
	}
	wire, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("read status body: %v", err)
	}
	var body degradationStatusBody
	dec := json.NewDecoder(bytes.NewReader(wire))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		t.Fatalf("decode status body %q: %v", wire, err)
	}
	return body, string(wire)
}

func TestDegradationStatusRecoveryNormalMode(t *testing.T) {
	body, raw := decodeRecoveryStatus(t, newDegradationState(false, nil))
	if body.Recovery == nil {
		t.Fatal("recovery block absent in normal mode; the mode must be explicit")
	}
	if body.Recovery.Mode != recoveryStatusModeNormal || body.Recovery.InstanceID != "" {
		t.Fatalf("recovery block = %+v, want normal mode without an instance", body.Recovery)
	}
	if strings.Contains(raw, "instance_id") {
		t.Fatalf("normal-mode body carries an instance_id: %s", raw)
	}
}

func TestDegradationStatusRecoveryBoundModeNeverClaimsDerivedStates(t *testing.T) {
	state := newDegradationState(false, nil)
	state.recovery = newRecoveryStatusPosture(&serveRecoveryWiring{instance: "11111111-1111-1111-1111-111111111111"})
	body, raw := decodeRecoveryStatus(t, state)

	if body.Recovery == nil || body.Recovery.Mode != recoveryStatusModeBound {
		t.Fatalf("recovery block = %+v, want bound mode", body.Recovery)
	}
	if body.Recovery.InstanceID != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("recovery instance = %q, want the binding", body.Recovery.InstanceID)
	}
	if body.Recovery.Attestation == "" || body.Recovery.States == "" || body.Recovery.FundingGate == "" {
		t.Fatalf("recovery block carries empty honesty fields: %+v", body.Recovery)
	}

	// The surface reports none of the three derived states and never presents
	// unverified/rolled-back data as normally consistent: a positive state
	// claim (`restored/verified/released=true` or `"released":true`) must not
	// appear anywhere in the body.
	for _, claim := range []string{`"released":true`, `"verified":true`, `"restored":true`, "released=true", "verified=true", "restored=true"} {
		if strings.Contains(raw, claim) {
			t.Fatalf("status body claims derived state %q: %s", claim, raw)
		}
	}
	if !strings.Contains(body.Recovery.States, "never impersonate one another") {
		t.Fatalf("states honesty text missing: %q", body.Recovery.States)
	}
	if !strings.Contains(body.Note, "never attest restored/verified/released state") {
		t.Fatalf("note does not disclaim derived-state attestation: %q", body.Note)
	}
}

func TestRecoveryHealthAnnotationMarksModeWithoutClaimingState(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	// Normal mode: the handler is untouched.
	rec := httptest.NewRecorder()
	recoveryHealthAnnotation(nil, next).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if got := rec.Header().Get("X-TXHarbor-Recovery-Mode"); got != "" {
		t.Fatalf("normal-mode probe carried recovery header %q", got)
	}

	for _, tt := range []struct {
		name     string
		wiring   *serveRecoveryWiring
		wantMode string
	}{
		{"configured", &serveRecoveryWiring{}, recoveryStatusModeConfigured},
		{"bound", &serveRecoveryWiring{instance: "11111111-1111-1111-1111-111111111111"}, recoveryStatusModeBound},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			recoveryHealthAnnotation(tt.wiring, next).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want the unchanged 200", rec.Code)
			}
			if got := rec.Header().Get("X-TXHarbor-Recovery-Mode"); got != tt.wantMode {
				t.Fatalf("mode header = %q, want %q", got, tt.wantMode)
			}
			if note := rec.Header().Get("X-TXHarbor-Recovery-Attestation"); !strings.Contains(note, "never attests") {
				t.Fatalf("attestation header = %q, want the non-attestation note", note)
			}
		})
	}
}
