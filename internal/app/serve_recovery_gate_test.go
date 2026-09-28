// serve_recovery_gate_test.go pins the T030 serve-side wiring units without a
// database: the read-path admission (refusal body, closed refusal_class,
// normal-mode pass-through, write paths untouched), the stream wrapper
// (resident while refused, loop-start check, per-step callback, lease-gate
// priority) and the fail-closed assembly pre-checks. The PG-backed acceptance
// lives in recovery_isolation_integration_test.go (T026).
package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/recovery"
)

// fakeRecoveryAdmitter scripts the gate admission surface without a control
// store. Decisions are consumed in order; the last decision repeats once the
// script is exhausted (so a re-admit converges). Requested admissions are
// exposed on calls.
type fakeRecoveryAdmitter struct {
	calls     chan recovery.GateRequest
	decisions []recovery.GateDecision
	errs      []error
	index     int
}

func newFakeRecoveryAdmitter(decisions ...recovery.GateDecision) *fakeRecoveryAdmitter {
	return &fakeRecoveryAdmitter{calls: make(chan recovery.GateRequest, 64), decisions: decisions}
}

func (f *fakeRecoveryAdmitter) Admit(_ context.Context, req recovery.GateRequest) (recovery.GateDecision, error) {
	select {
	case f.calls <- req:
	default:
	}
	i := f.index
	if f.index < len(f.decisions) {
		f.index++
	}
	var dec recovery.GateDecision
	switch {
	case len(f.decisions) == 0:
	case i < len(f.decisions):
		dec = f.decisions[i]
	default:
		dec = f.decisions[len(f.decisions)-1]
	}
	var err error
	if i < len(f.errs) {
		err = f.errs[i]
	}
	return dec, err
}

func TestRecoveryQueryGateRefusesBeforeNextHandler(t *testing.T) {
	admitter := newFakeRecoveryAdmitter(recovery.GateDecision{
		Allowed:      false,
		RefusalClass: recovery.RefusalInstanceMismatch,
		Reason:       "a recovery instance is open and this admission is not bound to it",
		InstanceID:   "11111111-1111-1111-1111-111111111111",
	})
	wiring := &serveRecoveryWiring{gate: admitter, scope: "chain=1;surface=serve", actor: "deploy:tester"}
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	recoveryQueryGate(wiring, next).ServeHTTP(rec,
		httptest.NewRequest(http.MethodGet, "/withdrawals/recov-1", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 for a refused query admission", rec.Code)
	}
	if called {
		t.Fatal("the route handler ran on a refused admission; the refusal belongs before HTTP admission")
	}
	var body recoveryRefusalBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode refusal body %q: %v", rec.Body.String(), err)
	}
	if body.RefusalClass != string(recovery.RefusalInstanceMismatch) {
		t.Fatalf("refusal_class = %q, want %q", body.RefusalClass, recovery.RefusalInstanceMismatch)
	}
	if body.Capability != string(recovery.CapabilityQuery) {
		t.Fatalf("capability = %q, want %q", body.Capability, recovery.CapabilityQuery)
	}
	select {
	case req := <-admitter.calls:
		if req.Capability != recovery.CapabilityQuery {
			t.Fatalf("admission capability = %q, want query", req.Capability)
		}
		if strings.TrimSpace(req.ScopeHash) == "" {
			t.Fatal("admission scope_hash is empty; the gate never substitutes a default scope")
		}
		if req.InstanceID != "" {
			t.Fatalf("unbound process admission carried instance %q", req.InstanceID)
		}
	default:
		t.Fatal("no admission was requested for the read path")
	}
}

func TestRecoveryQueryGateLeavesWritePathsUntouched(t *testing.T) {
	admitter := newFakeRecoveryAdmitter(recovery.GateDecision{
		Allowed:      false,
		RefusalClass: recovery.RefusalInstanceMismatch,
	})
	wiring := &serveRecoveryWiring{gate: admitter, scope: "chain=1;surface=serve"}
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusBadRequest)
	})

	rec := httptest.NewRecorder()
	recoveryQueryGate(wiring, next).ServeHTTP(rec,
		httptest.NewRequest(http.MethodPost, "/withdrawals", strings.NewReader("{")))

	if !called {
		t.Fatal("a write request was gated by the query read admission; write paths belong to their own entries (T031/T032)")
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want the untouched handler's 400", rec.Code)
	}
	select {
	case req := <-admitter.calls:
		t.Fatalf("write request consumed a query admission: %+v", req)
	default:
	}
}

func TestRecoveryQueryGateNormalModePassThrough(t *testing.T) {
	admitter := newFakeRecoveryAdmitter(recovery.GateDecision{
		Allowed:      true,
		Normal:       true,
		RefusalClass: recovery.RefusalNoInstance,
	})
	wiring := &serveRecoveryWiring{gate: admitter, scope: "chain=1;surface=serve"}
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusUnauthorized)
	})

	rec := httptest.NewRecorder()
	recoveryQueryGate(wiring, next).ServeHTTP(rec,
		httptest.NewRequest(http.MethodGet, "/withdrawals/recov-1", nil))
	if !called || rec.Code != http.StatusUnauthorized {
		t.Fatalf("normal-mode pass-through: called=%t status=%d, want the original handler's 401", called, rec.Code)
	}

	// A nil wiring (recovery mode not configured) installs nothing at all.
	nilCalled := false
	nilNext := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		nilCalled = true
		w.WriteHeader(http.StatusOK)
	})
	rec = httptest.NewRecorder()
	recoveryQueryGate(nil, nilNext).ServeHTTP(rec,
		httptest.NewRequest(http.MethodGet, "/withdrawals/recov-1", nil))
	if !nilCalled || rec.Code != http.StatusOK {
		t.Fatalf("unconfigured recovery mode must leave the route unchanged: called=%t status=%d", nilCalled, rec.Code)
	}
}

func TestRecoveryQueryGateUnavailableDecisionSurfacesClosedClass(t *testing.T) {
	admitter := newFakeRecoveryAdmitter(recovery.GateDecision{Allowed: false})
	admitter.errs = []error{errors.New("dial tcp: connection refused")}
	wiring := &serveRecoveryWiring{gate: admitter, scope: "chain=1;surface=serve"}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	rec := httptest.NewRecorder()
	recoveryQueryGate(wiring, next).ServeHTTP(rec,
		httptest.NewRequest(http.MethodGet, "/withdrawals/recov-1", nil))
	var body recoveryRefusalBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode refusal body %q: %v", rec.Body.String(), err)
	}
	if rec.Code != http.StatusServiceUnavailable || body.RefusalClass != string(recovery.RefusalControlStoreUnavailable) {
		t.Fatalf("unavailable store decision: status=%d class=%q, want 503 control_store_unavailable",
			rec.Code, body.RefusalClass)
	}
}

func TestRecoveryGateServeLoopStaysResidentWhileRefused(t *testing.T) {
	admitter := newFakeRecoveryAdmitter(recovery.GateDecision{
		Allowed:      false,
		RefusalClass: recovery.RefusalInstanceMismatch,
	})
	wiring := &serveRecoveryWiring{gate: admitter, scope: "chain=1;surface=serve", retry: time.Millisecond}
	innerCalled := make(chan struct{}, 1)
	inner := func(context.Context, func() error) error {
		innerCalled <- struct{}{}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- wiring.gateServeLoop(recovery.CapabilityChainScan, "chain_scan", inner)(ctx, nil)
	}()

	select {
	case req := <-admitter.calls:
		if req.Capability != recovery.CapabilityChainScan {
			t.Fatalf("admission capability = %q, want chain_scan", req.Capability)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no loop-start admission was requested")
	}
	select {
	case <-innerCalled:
		t.Fatal("the scan loop started while the gate refused; the loop must not start before admission")
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wrapper returned %v, want nil on shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wrapper did not stop on shutdown while refused")
	}
}

func TestRecoveryGateServeLoopReentersAfterRefusedStep(t *testing.T) {
	admitter := newFakeRecoveryAdmitter(
		recovery.GateDecision{Allowed: true},                                           // loop start #1
		recovery.GateDecision{Allowed: false, RefusalClass: recovery.RefusalNoRelease}, // step #1
		recovery.GateDecision{Allowed: true},                                           // loop start #2 (then repeated)
	)
	wiring := &serveRecoveryWiring{gate: admitter, scope: "chain=1;surface=serve", retry: time.Millisecond}
	invocations := 0
	inner := func(_ context.Context, checkLost func() error) error {
		invocations++
		if invocations == 1 {
			if err := checkLost(); !errors.Is(err, errRecoveryStepRefused) {
				t.Fatalf("refused step: checkLost = %v, want errRecoveryStepRefused", err)
			}
			return errRecoveryStepRefused
		}
		if err := checkLost(); err != nil {
			t.Fatalf("admitted re-entry: checkLost = %v, want nil", err)
		}
		return nil
	}

	if err := wiring.gateServeLoop(recovery.CapabilityChainScan, "chain_scan", inner)(context.Background(), nil); err != nil {
		t.Fatalf("wrapper returned %v, want nil after the admitted re-entry", err)
	}
	if invocations != 2 {
		t.Fatalf("inner invocations = %d, want 2 (refused step then re-admitted run)", invocations)
	}
}

func TestRecoveryStepGateKeepsLeaseGateFirst(t *testing.T) {
	admitter := newFakeRecoveryAdmitter(recovery.GateDecision{Allowed: true})
	wiring := &serveRecoveryWiring{gate: admitter, scope: "chain=1;surface=serve"}
	leaseErr := errors.New("lease lost")
	step := wiring.stepGate(context.Background(), recovery.CapabilityChainScan, "chain_scan",
		func() error { return leaseErr })
	if err := step(); !errors.Is(err, leaseErr) {
		t.Fatalf("step = %v, want the original lease error untouched", err)
	}
	select {
	case req := <-admitter.calls:
		t.Fatalf("gate was consulted after the lease was lost: %+v", req)
	default:
	}
}

func TestAssembleServeRecoveryNormalModeIsNil(t *testing.T) {
	wiring, err := assembleServeRecovery(context.Background(), &config.Config{}, fakeEnv(map[string]string{}))
	if err != nil {
		t.Fatalf("normal-mode assembly error = %v, want nil", err)
	}
	if wiring != nil {
		t.Fatal("normal-mode assembly returned a wiring; no control store means no recovery instance can exist")
	}
}

func TestAssembleServeRecoveryRefusesBoundInstanceWithoutControlStore(t *testing.T) {
	cfg := &config.Config{}
	wiring, err := assembleServeRecovery(context.Background(), cfg, fakeEnv(map[string]string{
		config.EnvRecoveryInstance: "11111111-1111-1111-1111-111111111111",
	}))
	if err == nil || wiring != nil {
		t.Fatalf("bound instance without control store: wiring=%v err=%v, want a refusal", wiring, err)
	}
	for _, name := range []string{config.EnvRecoveryInstance, config.EnvRecoveryControlDSN} {
		if !strings.Contains(err.Error(), name) {
			t.Fatalf("refusal %q does not name %s", err, name)
		}
	}
}

func TestAssembleServeRecoveryRefusesMissingGateTTL(t *testing.T) {
	cfg := &config.Config{Recovery: config.RecoveryConfig{
		ControlDSN: "postgres://user:secret@127.0.0.1:1/control?sslmode=disable",
	}}
	wiring, err := assembleServeRecovery(context.Background(), cfg, fakeEnv(map[string]string{}))
	if err == nil || wiring != nil {
		t.Fatalf("configured control store without TTL: wiring=%v err=%v, want a refusal", wiring, err)
	}
	if !strings.Contains(err.Error(), config.EnvRecoveryGateTTL) {
		t.Fatalf("refusal %q does not name %s", err, config.EnvRecoveryGateTTL)
	}
}

func TestServeRecoveryScopeIsStableAndNonEmpty(t *testing.T) {
	if got, want := serveRecoveryScope(31337), "chain=31337;surface=serve"; got != want {
		t.Fatalf("serveRecoveryScope(31337) = %q, want %q", got, want)
	}
}
