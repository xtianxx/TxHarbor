// withdrawalhttp_recovery_test.go pins the T031 write/read entry-point units
// of the 015 resumption gate without a database: the /withdrawals route
// wrapper must refuse POST /withdrawals as new_withdrawal_creation and GET
// /withdrawals/{id} as query BEFORE the next handler (which stands in for
// guardRoute plus the 007 handler), surface the closed refusal_class, admit
// exactly one action per request, and leave every non-action request (other
// methods, the collection GET) on the original 007 path. The PG-backed
// acceptance lives in recovery_isolation_integration_test.go (T026); the gate
// derivation itself is recovery's contract.
package app

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/recovery"
)

// withdrawalRecoveryTestWiring builds the T031 wiring over a scripted
// admission surface. The scope/actor match the serve assembly vocabulary; the
// gate itself is replaced, so no control store is needed.
func withdrawalRecoveryTestWiring(t *testing.T, admitter *fakeRecoveryAdmitter) *serveRecoveryWiring {
	t.Helper()
	return &serveRecoveryWiring{gate: admitter, scopes: recovServeTestScopes(t, 31337), actor: "deploy:tester"}
}

// withdrawalRecoveryAdmissions drains the recorded admissions.
func withdrawalRecoveryAdmissions(admitter *fakeRecoveryAdmitter) []recovery.GateRequest {
	var out []recovery.GateRequest
	for {
		select {
		case req := <-admitter.calls:
			out = append(out, req)
		default:
			return out
		}
	}
}

// TestWithdrawalRecoveryGateRefusesCreateBeforeNextHandler covers the T031
// contract for the write entry point: while a recovery instance is open, POST
// /withdrawals is the single action new_withdrawal_creation and is refused
// before anything else runs.
func TestWithdrawalRecoveryGateRefusesCreateBeforeNextHandler(t *testing.T) {
	admitter := newFakeRecoveryAdmitter(recovery.GateDecision{
		Allowed:      false,
		RefusalClass: recovery.RefusalInstanceMismatch,
		Reason:       "a recovery instance is open and this admission is not bound to it",
		InstanceID:   "11111111-1111-1111-1111-111111111111",
	})
	wiring := withdrawalRecoveryTestWiring(t, admitter)
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusBadRequest)
	})

	rec := httptest.NewRecorder()
	withdrawalRecoveryGate(wiring, next).ServeHTTP(rec,
		httptest.NewRequest(http.MethodPost, "/withdrawals", strings.NewReader("{")))

	if called {
		t.Fatal("the original chain ran on a refused create admission; the refusal belongs before HTTP admission")
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 for a refused create admission", rec.Code)
	}
	var body recoveryRefusalBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode refusal body %q: %v", rec.Body.String(), err)
	}
	if body.RefusalClass != string(recovery.RefusalInstanceMismatch) {
		t.Fatalf("refusal_class = %q, want %q", body.RefusalClass, recovery.RefusalInstanceMismatch)
	}
	if body.Capability != string(recovery.CapabilityNewWithdrawalCreation) {
		t.Fatalf("capability = %q, want %q", body.Capability, recovery.CapabilityNewWithdrawalCreation)
	}

	admissions := withdrawalRecoveryAdmissions(admitter)
	if len(admissions) != 1 {
		t.Fatalf("create admissions = %d, want exactly 1 (one admission per action)", len(admissions))
	}
	if admissions[0].Capability != recovery.CapabilityNewWithdrawalCreation {
		t.Fatalf("admission capability = %q, want new_withdrawal_creation", admissions[0].Capability)
	}
	if strings.TrimSpace(admissions[0].ScopeHash) == "" {
		t.Fatal("admission scope_hash is empty; the gate never substitutes a default scope")
	}
	if admissions[0].Action == "" {
		t.Fatal("admission action label is empty; the audit row must name the action")
	}
}

// TestWithdrawalRecoveryGateCreatePassThroughNormalMode pins FR-023 for the
// write entry point: an admitted (normal mode) create reaches the original
// 007 chain unchanged, with exactly one admission consumed.
func TestWithdrawalRecoveryGateCreatePassThroughNormalMode(t *testing.T) {
	admitter := newFakeRecoveryAdmitter(recovery.GateDecision{
		Allowed:      true,
		Normal:       true,
		RefusalClass: recovery.RefusalNoInstance,
	})
	wiring := withdrawalRecoveryTestWiring(t, admitter)
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusUnauthorized)
	})

	rec := httptest.NewRecorder()
	withdrawalRecoveryGate(wiring, next).ServeHTTP(rec,
		httptest.NewRequest(http.MethodPost, "/withdrawals", strings.NewReader("{}")))

	if !called || rec.Code != http.StatusUnauthorized {
		t.Fatalf("normal-mode pass-through: called=%t status=%d, want the original chain's 401", called, rec.Code)
	}
	admissions := withdrawalRecoveryAdmissions(admitter)
	if len(admissions) != 1 || admissions[0].Capability != recovery.CapabilityNewWithdrawalCreation {
		t.Fatalf("normal-mode create admissions = %+v, want exactly one new_withdrawal_creation", admissions)
	}
}

// TestWithdrawalRecoveryGateReadIsQueryAndRefusedBeforeNext covers the read
// half of the combined gate: GET /withdrawals/{id} is admitted as query by
// T030's read gate, which the T031 wrapper composes rather than duplicates.
func TestWithdrawalRecoveryGateReadIsQueryAndRefusedBeforeNext(t *testing.T) {
	admitter := newFakeRecoveryAdmitter(recovery.GateDecision{
		Allowed:      false,
		RefusalClass: recovery.RefusalIsolationUnproven,
		Reason:       "isolation item old_writers_stopped is pending",
	})
	wiring := withdrawalRecoveryTestWiring(t, admitter)
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusUnauthorized)
	})

	rec := httptest.NewRecorder()
	withdrawalRecoveryGate(wiring, next).ServeHTTP(rec,
		httptest.NewRequest(http.MethodGet, "/withdrawals/recov-1", nil))

	if called {
		t.Fatal("the original chain ran on a refused read admission")
	}
	var body recoveryRefusalBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode refusal body %q: %v", rec.Body.String(), err)
	}
	if rec.Code != http.StatusServiceUnavailable || body.RefusalClass != string(recovery.RefusalIsolationUnproven) {
		t.Fatalf("read refusal: status=%d class=%q, want 503 isolation_unproven", rec.Code, body.RefusalClass)
	}
	admissions := withdrawalRecoveryAdmissions(admitter)
	if len(admissions) != 1 || admissions[0].Capability != recovery.CapabilityQuery {
		t.Fatalf("read admissions = %+v, want exactly one query", admissions)
	}
}

// TestWithdrawalRecoveryGateLeavesNonActionsUntouched proves the wrapper only
// claims the two wired single actions: a method mismatch, the collection GET
// and a POST on an item path reach the original handlers with zero admissions.
func TestWithdrawalRecoveryGateLeavesNonActionsUntouched(t *testing.T) {
	cases := []struct {
		name, method, target string
	}{
		{"PUT on the collection", http.MethodPut, "/withdrawals"},
		{"GET on the collection", http.MethodGet, "/withdrawals"},
		{"DELETE on an item", http.MethodDelete, "/withdrawals/recov-1"},
		{"POST on an item", http.MethodPost, "/withdrawals/recov-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			admitter := newFakeRecoveryAdmitter(recovery.GateDecision{
				Allowed:      false,
				RefusalClass: recovery.RefusalInstanceMismatch,
			})
			wiring := withdrawalRecoveryTestWiring(t, admitter)
			called := false
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				called = true
				w.WriteHeader(http.StatusMethodNotAllowed)
			})

			rec := httptest.NewRecorder()
			withdrawalRecoveryGate(wiring, next).ServeHTTP(rec, httptest.NewRequest(tc.method, tc.target, nil))

			if !called || rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("non-action request: called=%t status=%d, want the original handler's 405", called, rec.Code)
			}
			if admissions := withdrawalRecoveryAdmissions(admitter); len(admissions) != 0 {
				t.Fatalf("non-action request consumed %d admission(s): %+v", len(admissions), admissions)
			}
		})
	}
}

// TestWithdrawalRecoveryGateNilWiringIsNoOp pins the unconfigured process
// (FR-023): without a wiring the wrapper installs nothing at all.
func TestWithdrawalRecoveryGateNilWiringIsNoOp(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	rec := httptest.NewRecorder()
	withdrawalRecoveryGate(nil, next).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/withdrawals", strings.NewReader("{}")))
	if !called || rec.Code != http.StatusOK {
		t.Fatalf("unconfigured recovery mode must leave the route unchanged: called=%t status=%d", called, rec.Code)
	}
}

// TestWithdrawalRecoveryGateUnavailableStoreSurfacesClosedClass covers the
// fail-closed decision: an admission that could not be evaluated refuses with
// the closed control_store_unavailable class and never a 2xx.
func TestWithdrawalRecoveryGateUnavailableStoreSurfacesClosedClass(t *testing.T) {
	admitter := newFakeRecoveryAdmitter(recovery.GateDecision{Allowed: false})
	admitter.errs = []error{errors.New("dial tcp: connection refused")}
	wiring := withdrawalRecoveryTestWiring(t, admitter)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusCreated) })

	rec := httptest.NewRecorder()
	withdrawalRecoveryGate(wiring, next).ServeHTTP(rec,
		httptest.NewRequest(http.MethodPost, "/withdrawals", strings.NewReader("{}")))

	var body recoveryRefusalBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode refusal body %q: %v", rec.Body.String(), err)
	}
	if rec.Code != http.StatusServiceUnavailable || body.RefusalClass != string(recovery.RefusalControlStoreUnavailable) {
		t.Fatalf("unavailable store: status=%d class=%q, want 503 control_store_unavailable", rec.Code, body.RefusalClass)
	}
}

// TestWithdrawalRecoveryGateScopeIsCanonical pins the canonical business scope
// the T031 admission carries through the shared serve wiring (T050). The scope
// is derived per capability from the production constructor; the wrapper must
// never invent its own scope and the write path's capability is
// new_withdrawal_creation (not the read path's query).
func TestWithdrawalRecoveryGateScopeIsCanonical(t *testing.T) {
	admitter := newFakeRecoveryAdmitter(recovery.GateDecision{Allowed: true})
	wiring := &serveRecoveryWiring{gate: admitter, scopes: recovServeTestScopes(t, 31337)}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	rec := httptest.NewRecorder()
	withdrawalRecoveryGate(wiring, next).ServeHTTP(rec,
		httptest.NewRequest(http.MethodPost, "/withdrawals", strings.NewReader("{}")))
	admissions := withdrawalRecoveryAdmissions(admitter)
	want := "capability=new_withdrawal_creation;chain=31337"
	if len(admissions) != 1 || admissions[0].ScopeHash != want {
		t.Fatalf("scope = %+v, want the canonical write-path scope %q", admissions, want)
	}
	if admissions[0].Capability != recovery.CapabilityNewWithdrawalCreation {
		t.Fatalf("capability = %q, want %q", admissions[0].Capability, recovery.CapabilityNewWithdrawalCreation)
	}
}
