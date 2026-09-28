// existingwithdrawalrecovery_gate_test.go pins the T032/T070 wiring units
// without a database: the fail-closed assembly pre-checks (normal mode nil,
// bound-without-store refusal, missing TTL refusal), the refusal class mapping,
// the worker/CLI admission call points and the signer delivery checkpoint
// adapter. The PG-backed acceptance lives in recovery_isolation_integration_test.go
// (T026) and internal/signer's delivery integration tests.
package app

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/signer"
)

// existingWithdrawalFakeGate scripts the T012 admission surface without a
// control store and records every request.
type existingWithdrawalFakeGate struct {
	mu       sync.Mutex
	dec      recovery.GateDecision
	err      error
	requests []recovery.GateRequest
}

func (f *existingWithdrawalFakeGate) Admit(_ context.Context, req recovery.GateRequest) (recovery.GateDecision, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, req)
	return f.dec, f.err
}

func TestAssembleExistingWithdrawalRecoveryNormalModeIsNil(t *testing.T) {
	wiring, err := assembleExistingWithdrawalRecovery(context.Background(), &config.Config{}, fakeEnv(map[string]string{}))
	if err != nil {
		t.Fatalf("normal-mode assembly error = %v, want nil", err)
	}
	if wiring != nil {
		t.Fatal("normal-mode assembly returned a wiring; no control store means no recovery instance can exist")
	}
	// The nil receiver is a documented normal-mode pass-through: the worker
	// and CLI call sites never need a nil check, and the pass-through is
	// never an admission inside recovery mode.
	dec, err := wiring.admit(context.Background(), recovery.CapabilityExistingWithdrawalRecovery, "worker_claim", "")
	if err != nil || !dec.Allowed || !dec.Normal || dec.RefusalClass != recovery.RefusalNoInstance {
		t.Fatalf("nil wiring admit = (%+v, %v), want the normal-mode marker", dec, err)
	}
}

func TestAssembleExistingWithdrawalRecoveryRefusesBoundInstanceWithoutControlStore(t *testing.T) {
	wiring, err := assembleExistingWithdrawalRecovery(context.Background(), &config.Config{}, fakeEnv(map[string]string{
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

func TestAssembleExistingWithdrawalRecoveryRefusesMissingGateTTL(t *testing.T) {
	cfg := &config.Config{Recovery: config.RecoveryConfig{
		ControlDSN: "postgres://user:secret@127.0.0.1:1/control?sslmode=disable",
	}}
	wiring, err := assembleExistingWithdrawalRecovery(context.Background(), cfg, fakeEnv(map[string]string{}))
	if err == nil || wiring != nil {
		t.Fatalf("configured control store without TTL: wiring=%v err=%v, want a refusal", wiring, err)
	}
	if !strings.Contains(err.Error(), config.EnvRecoveryGateTTL) {
		t.Fatalf("refusal %q does not name %s", err, config.EnvRecoveryGateTTL)
	}
}

func TestExistingWithdrawalRecoveryScopeIsStableAndNonEmpty(t *testing.T) {
	got := existingWithdrawalRecoveryScope(31337)
	if got == "" {
		t.Fatal("scope_hash must never be empty (the gate refuses an empty scope)")
	}
	if got != "chain=31337;capability=existing_withdrawal_recovery" {
		t.Fatalf("existingWithdrawalRecoveryScope(31337) = %q", got)
	}
}

func TestExistingWithdrawalRecoveryRefusalClassMapsUnavailable(t *testing.T) {
	for _, dec := range []recovery.GateDecision{
		{},
		{RefusalClass: recovery.RefusalNoInstance},
		{RefusalClass: "not_a_class"},
	} {
		if got := existingWithdrawalRecoveryRefusalClass(dec); got != recovery.RefusalControlStoreUnavailable {
			t.Fatalf("refusal class of %+v = %q, want %q", dec, got, recovery.RefusalControlStoreUnavailable)
		}
	}
	if got := existingWithdrawalRecoveryRefusalClass(recovery.GateDecision{RefusalClass: recovery.RefusalGapOpen}); got != recovery.RefusalGapOpen {
		t.Fatalf("known class = %q, want gap_open", got)
	}
}

func TestWithdrawalWorkerAdmitRecoveryPassesCapabilityAndRefusal(t *testing.T) {
	fake := &existingWithdrawalFakeGate{dec: recovery.GateDecision{
		Allowed: false, RefusalClass: recovery.RefusalIsolationUnproven, Reason: "unverified",
	}}
	w := &WithdrawalWorker{Recovery: &existingWithdrawalRecoveryWiring{gate: fake, scope: "chain=31337;capability=existing_withdrawal_recovery"}}
	dec, err := w.admitRecovery(context.Background(), "worker_claim", "intent-1")
	if err != nil {
		t.Fatalf("admitRecovery error = %v", err)
	}
	if dec.Allowed || dec.RefusalClass != recovery.RefusalIsolationUnproven {
		t.Fatalf("decision = %+v, want the gate refusal", dec)
	}
	if len(fake.requests) != 1 {
		t.Fatalf("gate calls = %d, want 1", len(fake.requests))
	}
	req := fake.requests[0]
	if req.Capability != recovery.CapabilityExistingWithdrawalRecovery {
		t.Fatalf("capability = %q", req.Capability)
	}
	if req.OperationID != "intent-1" || req.Action != "worker_claim" {
		t.Fatalf("request = %+v, want action/operation carried", req)
	}
}

func TestWithdrawalExecRecoveryRefuseSurfacesClosedClass(t *testing.T) {
	var stderr bytes.Buffer
	if code := withdrawalExecRecoveryRefuse(context.Background(), &stderr, nil, "permission_set", "op-1"); code != 0 {
		t.Fatalf("nil wiring exit = %d, want 0 (normal mode)", code)
	}
	if stderr.Len() != 0 {
		t.Fatalf("normal mode wrote %q", stderr.String())
	}

	fake := &existingWithdrawalFakeGate{dec: recovery.GateDecision{
		Allowed: false, RefusalClass: recovery.RefusalGapOpen, Reason: "gap names the capability",
	}}
	stderr.Reset()
	code := withdrawalExecRecoveryRefuse(context.Background(), &stderr, &existingWithdrawalRecoveryWiring{gate: fake, scope: "chain=31337;capability=existing_withdrawal_recovery"},
		"claim_revoke", "op-2")
	if code == 0 {
		t.Fatal("refused admission exit = 0, want non-zero")
	}
	if !strings.Contains(stderr.String(), "refusal_class=gap_open") {
		t.Fatalf("stderr = %q, want the closed refusal_class surfaced", stderr.String())
	}
	if got := fake.requests[0].OperationID; got != "op-2" {
		t.Fatalf("audit operation_id = %q, want op-2", got)
	}
}

// TestWithdrawalWorkerRefusesClaimAndCycleBeforeExecution pins the two worker
// action call points: with the gate refusing, the claim and the execution
// cycle return without touching the pool, the claim store or any 011 state
// (the stub pool is nil and any reach-through would panic).
func TestWithdrawalWorkerRefusesClaimAndCycleBeforeExecution(t *testing.T) {
	fake := &existingWithdrawalFakeGate{dec: recovery.GateDecision{
		Allowed: false, RefusalClass: recovery.RefusalCapabilityDependencyClosed, Reason: "chain_scan not released",
	}}
	w := &WithdrawalWorker{Recovery: &existingWithdrawalRecoveryWiring{gate: fake, scope: "chain=31337;capability=existing_withdrawal_recovery"}}

	w.serveIntent(context.Background(), "intent-refused-claim")
	if len(fake.requests) != 1 || fake.requests[0].Action != "worker_claim" {
		t.Fatalf("claim admission requests = %+v, want exactly the claim gate call", fake.requests)
	}
	if err := w.execIntentCycle(context.Background(), "intent-refused-cycle", 1); err != nil {
		t.Fatalf("refused execIntentCycle error = %v, want nil (no progress, retry kept)", err)
	}
	if len(fake.requests) != 2 || fake.requests[1].Action != "worker_exec_cycle" {
		t.Fatalf("cycle admission requests = %+v, want the cycle gate call after the claim", fake.requests)
	}
}

func TestSignerDeliveryRecoveryGateAdapter(t *testing.T) {
	// Normal mode: the supported assembly always injects a function; a nil
	// wiring is the documented normal-mode checkpoint and never a missing one.
	if err := signerDeliveryRecoveryGate(nil)(context.Background(), signer.DeliveryGateRequest{SigningRequestID: "sr-1"}); err != nil {
		t.Fatalf("normal-mode checkpoint = %v, want nil", err)
	}

	fake := &existingWithdrawalFakeGate{dec: recovery.GateDecision{
		Allowed: false, RefusalClass: recovery.RefusalInstanceMismatch, Reason: "unbound",
	}}
	refuse := signerDeliveryRecoveryGate(&existingWithdrawalRecoveryWiring{gate: fake, scope: "chain=31337;capability=existing_withdrawal_recovery"})
	err := refuse(context.Background(), signer.DeliveryGateRequest{SigningRequestID: "sr-2", ChainID: 31337})
	if err == nil || !strings.Contains(err.Error(), "refusal_class=instance_mismatch") {
		t.Fatalf("refusal = %v, want the closed refusal_class", err)
	}
	if req := fake.requests[0]; req.Capability != recovery.CapabilityExistingWithdrawalRecovery || req.OperationID != "sr-2" {
		t.Fatalf("checkpoint request = %+v", req)
	}

	fake.dec = recovery.GateDecision{Allowed: true}
	if err := refuse(context.Background(), signer.DeliveryGateRequest{SigningRequestID: "sr-3"}); err != nil {
		t.Fatalf("allowed checkpoint = %v, want nil", err)
	}
}

// executionRecoveryTestHandler builds the handler with a seam-injected wiring
// and no database: the admission runs before every 009 read, so the refusal
// paths below never touch Pool.
func executionRecoveryTestHandler(t *testing.T, load func(context.Context, uint64, func(string) (string, bool)) (*existingWithdrawalRecoveryWiring, error)) *WithdrawalExecutionHandler {
	t.Helper()
	previous := withdrawalExecutionRecoveryLoad
	withdrawalExecutionRecoveryLoad = load
	t.Cleanup(func() { withdrawalExecutionRecoveryLoad = previous })
	return &WithdrawalExecutionHandler{ChainID: 31337}
}

func TestExecutionHTTPAdmissionRefusesBeforeAuthAndDomain(t *testing.T) {
	fake := &existingWithdrawalFakeGate{dec: recovery.GateDecision{
		Allowed: false, RefusalClass: recovery.RefusalIsolationUnproven, Reason: "isolation unproven",
	}}
	h := executionRecoveryTestHandler(t, func(context.Context, uint64, func(string) (string, bool)) (*existingWithdrawalRecoveryWiring, error) {
		return &existingWithdrawalRecoveryWiring{gate: fake, scope: "chain=31337;capability=existing_withdrawal_recovery"}, nil
	})

	rec := httptest.NewRecorder()
	// No bearer credential: a correctly ordered admission refuses before the
	// 401, exactly like the serve read paths, and no request row is created.
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/withdrawals/req-1/execution", strings.NewReader("{")))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 for a refused execution admission", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "isolation_unproven") {
		t.Fatalf("body = %q, want the closed refusal_class", rec.Body.String())
	}
	if len(fake.requests) != 1 || fake.requests[0].Capability != recovery.CapabilityExistingWithdrawalRecovery {
		t.Fatalf("gate requests = %+v", fake.requests)
	}
	if req := fake.requests[0]; req.OperationID != "req-1" || strings.TrimSpace(req.ScopeHash) == "" {
		t.Fatalf("admission request = %+v, want request id and non-empty scope", req)
	}
}

func TestExecutionHTTPAdmissionNormalModeKeepsOriginalOrder(t *testing.T) {
	h := executionRecoveryTestHandler(t, func(context.Context, uint64, func(string) (string, bool)) (*existingWithdrawalRecoveryWiring, error) {
		return nil, nil // normal mode: no control store configured
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/withdrawals/req-2/execution", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want the original 401 (normal mode must not alter the 011 order)", rec.Code)
	}
}

func TestExecutionHTTPAdmissionAssemblyFailureRefusesClosed(t *testing.T) {
	h := executionRecoveryTestHandler(t, func(context.Context, uint64, func(string) (string, bool)) (*existingWithdrawalRecoveryWiring, error) {
		return nil, errors.New("control store unavailable: dial tcp: connection refused")
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/withdrawals/req-3/execution", nil))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), string(recovery.RefusalControlStoreUnavailable)) {
		t.Fatalf("assembly failure: status=%d body=%q, want 503 control_store_unavailable", rec.Code, rec.Body.String())
	}
}
