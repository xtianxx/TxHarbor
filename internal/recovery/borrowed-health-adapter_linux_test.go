//go:build linux && drill

// borrowed-health-adapter_linux_test.go proves the one small drill-only health
// adapter: a real factory-created borrowed run reports healthy through the
// ACTUAL lock, and a terminated owner, a released lock, a canceled/nil-ish
// context, and inert/zero/JSON values are all refused. No native or gate
// process is launched and no OS identity claim is made.
package recovery

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func borrowedHealthRun(t *testing.T, e *borrowedTransportPG, operation string) (*DrillBorrowedWriterRun, *TargetLock) {
	t.Helper()
	tools, err := DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed tools: %v", err)
	}
	endpoint, err := DrillOpenOriginEndpoint()
	if err != nil {
		t.Fatalf("open factory endpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Listener().Close() })
	originalKey, err := CanonicalTargetKey(e.target)
	if err != nil {
		t.Fatalf("original target key: %v", err)
	}
	lock, err := AcquireTargetLock(e.ctx, e.ctrlDSN, originalKey, 3*time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire real borrowed lock: %v", err)
	}
	opts := TargetWriterOptions{
		OperationKind: TargetWriterOperationRestore,
		Store:         e.store, ControlDSN: e.ctrlDSN, TargetDSN: e.targetDSN,
		ObserverDSN: borrowedTransportObserverDSN(t, e.adminDSN, e.targetDSN), TrustedTarget: e.target,
		OperationID: operation,
		Archive:     strings.NewReader("archive"),
		Probe: func(context.Context, TargetWriterProof) (TargetWriterProbeResult, error) {
			return TargetWriterProbeResult{}, nil
		},
		Acceptance: func(context.Context, pgx.Tx, TargetWriterProof) (TargetWriterAcceptance, error) {
			return TargetWriterAcceptance{}, nil
		},
	}
	run, err := DrillNewBorrowedWriterRun(e.ctx, opts, lock, endpoint, tools)
	if err != nil {
		t.Fatalf("borrowed writer run factory: %v", err)
	}
	return run, lock
}

func TestBorrowedHealthValidLockDelegatesToActualLock(t *testing.T) {
	e := newBorrowedTransportPG(t)
	run, _ := borrowedHealthRun(t, e, "borrow-health-valid")
	if err := run.Health(e.ctx); err != nil {
		t.Fatalf("healthy factory-created run was refused: %v", err)
	}
	canceled, cancel := context.WithCancel(e.ctx)
	cancel()
	if err := run.Health(canceled); err == nil {
		t.Fatal("canceled context was not refused by the health adapter")
	}
	expired, cancelTimeout := context.WithTimeout(e.ctx, time.Nanosecond)
	defer cancelTimeout()
	if err := run.Health(expired); err == nil {
		t.Fatal("expired bounded context was not refused by the health adapter")
	}
	if err := run.Health(nil); err == nil {
		t.Fatal("nil context was not refused by the health adapter")
	}
	// The real lock is still healthy after canceled probes: the adapter never
	// closes or hides the caller context.
	if err := run.Health(e.ctx); err != nil {
		t.Fatalf("healthy lock after canceled probes: %v", err)
	}
}

func TestBorrowedHealthRefusesTerminatedOwnerAndReleasedLock(t *testing.T) {
	e := newBorrowedTransportPG(t)
	run, _ := borrowedHealthRun(t, e, "borrow-health-terminated")
	binding := run.Binding()
	if binding.ControlBackendPID() <= 0 {
		t.Fatalf("captured control owner pid is missing: %+v", binding)
	}
	if _, err := e.ctrl.Exec(e.ctx, `SELECT pg_terminate_backend($1)`, binding.ControlBackendPID()); err != nil {
		t.Fatalf("terminate borrowed control owner: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := run.Health(e.ctx)
		if err != nil {
			t.Logf("causal: terminated owner health error=%q", err)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("terminated borrowed control owner still reported healthy")
		}
		time.Sleep(20 * time.Millisecond)
	}

	second, secondLock := borrowedHealthRun(t, e, "borrow-health-released")
	if err := second.Health(e.ctx); err != nil {
		t.Fatalf("second factory run not healthy before release: %v", err)
	}
	if err := secondLock.Release(context.Background()); err != nil {
		t.Fatalf("release second borrowed lock: %v", err)
	}
	if err := second.Health(e.ctx); err == nil {
		t.Fatal("released borrowed lock was not refused by the health adapter")
	} else {
		t.Logf("causal: released lock health error=%q", err)
	}
}

func TestBorrowedHealthRefusesNilZeroAndJSONInert(t *testing.T) {
	e := newBorrowedTransportPG(t)
	run, _ := borrowedHealthRun(t, e, "borrow-health-inert")

	serialized, err := json.Marshal(run)
	if err != nil || string(serialized) != "{}" {
		t.Fatalf("borrowed run serialized authority material: err=%v doc=%q", err, serialized)
	}
	var zero DrillBorrowedWriterRun
	if err := zero.Health(e.ctx); err == nil {
		t.Fatal("zero borrowed run accepted a health call")
	}
	var zeroPointer *DrillBorrowedWriterRun
	if err := zeroPointer.Health(e.ctx); err == nil {
		t.Fatal("nil borrowed run accepted a health call")
	}
	var decoded DrillBorrowedWriterRun
	if err := json.Unmarshal([]byte(`{"lock":{},"binding":{"sql":true}}`), &decoded); err != nil {
		t.Fatalf("forged borrowed run document: %v", err)
	}
	if err := decoded.Health(e.ctx); err == nil {
		t.Fatal("JSON-forged borrowed run accepted a health call")
	}
	// The real run remains the only healthy value.
	if err := run.Health(e.ctx); err != nil {
		t.Fatalf("real run health after inert refusals: %v", err)
	}
}
