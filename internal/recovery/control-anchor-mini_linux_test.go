//go:build linux && drill

// control-anchor-mini_linux_test.go is the focused real-PG validation of the
// mini-phase-1 actual control anchor: a captured anchor matches the actual
// control database/namespace/backend/connection tuple; owner termination,
// release and changed identity permanently invalidate it (shared by copies);
// zero/JSON values and context/mutex interruption refuse without ever
// constructing facts from caller inputs. Every anchor is produced by the real
// DrillNewBorrowedWriterRun factory over the existing fixture helpers; no
// hand-built SQL snapshot is accepted and no child is launched.
package recovery

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// controlAnchorFixtureRun builds one real factory-created borrowed run over
// the existing real-PG fixture helpers (read-only reuse).
func controlAnchorFixtureRun(t *testing.T) (*borrowedTransportPG, *DrillBorrowedWriterRun, *TargetLock, TargetKey) {
	t.Helper()
	e := newBorrowedTransportPG(t)
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
	t.Cleanup(func() { _ = lock.Release(context.Background()) })
	opts := TargetWriterOptions{
		OperationKind: TargetWriterOperationRestore,
		Store:         e.store, ControlDSN: e.ctrlDSN, TargetDSN: e.targetDSN,
		ObserverDSN: borrowedTransportObserverDSN(t, e.adminDSN, e.targetDSN), TrustedTarget: e.target,
		OperationID: "control-anchor-mini",
		Archive:     strings.NewReader("archive"),
		Probe: func(context.Context, TargetWriterProof) (TargetWriterProbeResult, error) {
			return TargetWriterProbeResult{}, errControlAnchorProbeRefused
		},
		Acceptance: func(context.Context, pgx.Tx, TargetWriterProof) (TargetWriterAcceptance, error) {
			return TargetWriterAcceptance{}, errControlAnchorAcceptanceRefused
		},
	}
	run, err := DrillNewBorrowedWriterRun(e.ctx, opts, lock, endpoint, tools)
	if err != nil {
		t.Fatalf("borrowed writer run factory: %v", err)
	}
	return e, run, lock, originalKey
}

var (
	errControlAnchorProbeRefused      = errControlAnchorSentinel("control anchor mini probe intentionally refuses")
	errControlAnchorAcceptanceRefused = errControlAnchorSentinel("control anchor mini acceptance intentionally refuses")
)

type errControlAnchorSentinel string

func (e errControlAnchorSentinel) Error() string { return string(e) }

func TestControlAnchorCaptureMatchesActualControlOwner(t *testing.T) {
	e, run, lock, originalKey := controlAnchorFixtureRun(t)
	anchor, err := run.CaptureControlAnchor(e.ctx)
	if err != nil {
		t.Fatalf("control anchor capture: %v", err)
	}
	facts := anchor.Diagnostics()
	if !facts.Present || facts.Invalidated {
		t.Fatalf("fresh anchor diagnostics are not present/valid: %+v", facts)
	}
	binding := run.Binding()

	// Actual control database OID, read independently from the same live
	// control database.
	var actualDatabaseOID uint32
	if err := e.ctrl.QueryRow(e.ctx, `SELECT oid FROM pg_database WHERE datname = current_database()`).Scan(&actualDatabaseOID); err != nil {
		t.Fatalf("independent control database OID read: %v", err)
	}
	if actualDatabaseOID == 0 || facts.ControlDatabaseOID != actualDatabaseOID || facts.ControlDatabaseOID != binding.ControlDatabaseOID() {
		t.Fatal("anchor control database OID is not the actual control database")
	}

	// Actual backend pid/start of the lock owner session.
	var (
		ownerPID   int
		ownerStart time.Time
	)
	if err := e.ctrl.QueryRow(e.ctx, `SELECT pid::int, backend_start FROM pg_stat_activity WHERE pid = $1`, facts.BackendPID).Scan(&ownerPID, &ownerStart); err != nil {
		t.Fatalf("independent control backend read: %v", err)
	}
	if ownerPID != facts.BackendPID || !ownerStart.Equal(facts.BackendStart) ||
		facts.BackendPID != binding.ControlBackendPID() || !facts.BackendStart.Equal(binding.ControlBackendStart()) {
		t.Fatal("anchor backend identity is not the actual control lock owner")
	}

	// Actual granted namespace: the logical original DataTargetKey advisory
	// pair in the actual control database, never the transport key.
	namespaceKey1, namespaceKey2 := originalKey.AdvisoryLockKey()
	if facts.NamespaceClassID != uint32(namespaceKey1) || facts.NamespaceObjectID != uint32(namespaceKey2) {
		t.Fatal("anchor namespace is not the logical original DataTargetKey advisory pair")
	}
	if facts.NamespaceDatabaseID != actualDatabaseOID {
		t.Fatal("anchor namespace database is not the actual control database")
	}
	var namespaceHeld bool
	if err := e.ctrl.QueryRow(e.ctx, `
SELECT EXISTS (
  SELECT 1 FROM pg_locks
  WHERE locktype = 'advisory' AND granted AND pid = $1
    AND objsubid = 2 AND classid = $2::oid AND objid = $3::oid AND database = $4::oid
)`, facts.BackendPID, facts.NamespaceClassID, facts.NamespaceObjectID, facts.NamespaceDatabaseID).Scan(&namespaceHeld); err != nil || !namespaceHeld {
		t.Fatal("anchor namespace advisory lock is not the actually granted lock")
	}

	// Actual SQL incarnation facts.
	var (
		actualSystemID   string
		actualPostmaster time.Time
	)
	if err := e.admin.QueryRow(e.ctx, `SELECT system_identifier::text, pg_postmaster_start_time() FROM pg_control_system()`).Scan(&actualSystemID, &actualPostmaster); err != nil {
		t.Fatalf("independent cluster identity read: %v", err)
	}
	if facts.SystemIdentifier != actualSystemID || !facts.PostmasterStart.Equal(actualPostmaster) ||
		facts.SystemIdentifier != binding.controlClusterIdentifier || !facts.PostmasterStart.Equal(binding.ControlPostmasterStart()) {
		t.Fatal("anchor SQL incarnation is not the actual control cluster/incarnation")
	}

	// Actual live TCP tuple of the same dedicated connection.
	controlTarget, err := controlstore.ParseDSNTarget(e.ctrlDSN)
	if err != nil {
		t.Fatalf("parse control DSN: %v", err)
	}
	localHost, localPort, err := net.SplitHostPort(facts.LocalAddress)
	if err != nil {
		t.Fatalf("split anchor local tuple: %v", err)
	}
	remoteHost, remotePort, err := net.SplitHostPort(facts.RemoteAddress)
	if err != nil {
		t.Fatalf("split anchor remote tuple: %v", err)
	}
	localIP := net.ParseIP(localHost)
	remoteIP := net.ParseIP(remoteHost)
	if localIP == nil || remoteIP == nil || !localIP.IsLoopback() {
		t.Fatal("anchor local tuple is not a loopback TCP endpoint")
	}
	if localPort != strconv.Itoa(facts.LocalPort) || remotePort != strconv.Itoa(facts.RemotePort) {
		t.Fatal("anchor tuple ports disagree with the diagnostic projection")
	}
	if facts.LocalPort <= 0 || facts.RemotePort != int(controlTarget.Port) {
		t.Fatal("anchor tuple does not name the actual control connection endpoint")
	}

	// The same live anchor rechecks successfully and no child was started.
	if err := anchor.Recheck(e.ctx); err != nil {
		t.Fatalf("fresh anchor recheck: %v", err)
	}
	if err := run.Health(e.ctx); err != nil {
		t.Fatalf("factory run health after anchor capture: %v", err)
	}
	if lock.key1 != namespaceKey1 || lock.key2 != namespaceKey2 {
		t.Fatal("anchor capture changed the borrowed lock namespace")
	}
	if identity := run.Observation().StartedIdentity(); identity.Started {
		t.Fatal("control anchor capture started a child")
	}
}

// installControlAnchorStageHook installs the narrow test-only pause seam for
// one test and removes it at cleanup.
func installControlAnchorStageHook(t *testing.T, hook func(drillControlAnchorTestStage)) {
	t.Helper()
	drillControlAnchorStageHook.Store(&hook)
	t.Cleanup(func() { drillControlAnchorStageHook.Store(nil) })
}

func controlAnchorWaitStage(t *testing.T, entered <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(15 * time.Second):
		t.Fatalf("control anchor never reached %s", name)
	}
}

// TestControlAnchorConcurrentInvalidationLinearizesSuccess proves CA01: a
// genuine healthy Recheck paused AFTER its fact inspection and BEFORE success
// publication must fail when a copied handle invalidates the shared state
// concurrently. Success publication and invalidation linearize under the same
// invalidMu; without that check the paused healthy call would return nil and
// grant an invalid capability.
func TestControlAnchorConcurrentInvalidationLinearizesSuccess(t *testing.T) {
	e, run, _, _ := controlAnchorFixtureRun(t)
	anchor, err := run.CaptureControlAnchor(e.ctx)
	if err != nil {
		t.Fatalf("control anchor capture: %v", err)
	}
	copied := anchor
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	installControlAnchorStageHook(t, func(stage drillControlAnchorTestStage) {
		if stage != drillControlAnchorStageRecheckPublish {
			return
		}
		once.Do(func() { close(entered) })
		<-release
	})
	result := make(chan error, 1)
	go func() { result <- anchor.Recheck(e.ctx) }()
	controlAnchorWaitStage(t, entered, "the pre-publication stage of a healthy recheck")

	canceled, cancel := context.WithCancel(e.ctx)
	cancel()
	if err := copied.Recheck(canceled); err == nil {
		t.Fatal("canceled copied recheck unexpectedly succeeded")
	}
	if diagnostics := copied.Diagnostics(); !diagnostics.Invalidated {
		t.Fatal("copied cancellation did not invalidate the shared anchor state")
	}
	close(release)
	select {
	case recheckErr := <-result:
		if recheckErr == nil {
			t.Fatal("paused healthy recheck published success after the concurrent invalidation")
		}
		if !strings.Contains(recheckErr.Error(), "invalidated") {
			t.Fatalf("paused healthy recheck failed for a different cause: %v", recheckErr)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("paused healthy recheck never completed after the concurrent invalidation")
	}
	if diagnostics := anchor.Diagnostics(); !diagnostics.Invalidated {
		t.Fatal("concurrent invalidation did not persist on the original handle")
	}
	if err := anchor.Recheck(e.ctx); err == nil {
		t.Fatal("concurrently invalidated anchor rechecked successfully")
	}
}

// TestControlAnchorRecheckParentCancellationBeforePublicationFailsClosed
// proves the final CA01 publication contract: a healthy Recheck paused after
// its genuine Health call must fail closed when ITS OWN parent context is
// canceled before the publication decision, record a shared permanent
// invalidation with the fixed safe reason, and refuse every later recheck. No
// second Recheck on a copy is involved.
func TestControlAnchorRecheckParentCancellationBeforePublicationFailsClosed(t *testing.T) {
	e, run, _, _ := controlAnchorFixtureRun(t)
	anchor, err := run.CaptureControlAnchor(e.ctx)
	if err != nil {
		t.Fatalf("control anchor capture: %v", err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	installControlAnchorStageHook(t, func(stage drillControlAnchorTestStage) {
		if stage != drillControlAnchorStageRecheckPublish {
			return
		}
		once.Do(func() { close(entered) })
		<-release
	})
	parent, cancelParent := context.WithCancel(e.ctx)
	result := make(chan error, 1)
	go func() { result <- anchor.Recheck(parent) }()
	controlAnchorWaitStage(t, entered, "the pre-publication stage of a healthy recheck")
	cancelParent()
	close(release)
	select {
	case recheckErr := <-result:
		if recheckErr == nil {
			t.Fatal("recheck published success after its own parent context was canceled before publication")
		}
		if !strings.Contains(recheckErr.Error(), drillControlAnchorContextEndedReason) {
			t.Fatalf("cancellation refusal is not the fixed publication cause: %v", recheckErr)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("recheck did not complete after its own parent cancellation")
	}
	if diagnostics := anchor.Diagnostics(); !diagnostics.Invalidated || diagnostics.InvalidReason != drillControlAnchorContextEndedReason {
		t.Fatalf("own-parent cancellation did not record the shared permanent invalidation: %+v", diagnostics)
	}
	if err := anchor.Recheck(e.ctx); err == nil {
		t.Fatal("permanently invalidated anchor rechecked successfully")
	}
}

// TestControlAnchorCaptureParentCancellationBeforePublicationFailsClosed
// proves the same fail-closed publication rule for a first capture: after the
// genuine Health call succeeded, canceling the capture's own parent before
// publication refuses with the fixed cause and never returns a usable
// capability. No shared invalidation is leaked by the failed publication.
func TestControlAnchorCaptureParentCancellationBeforePublicationFailsClosed(t *testing.T) {
	e, run, lock, _ := controlAnchorFixtureRun(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	installControlAnchorStageHook(t, func(stage drillControlAnchorTestStage) {
		if stage != drillControlAnchorStageCapturePublish {
			return
		}
		once.Do(func() { close(entered) })
		<-release
	})
	type captureOutcome struct {
		anchor DrillControlAnchor
		err    error
	}
	parent, cancelParent := context.WithCancel(e.ctx)
	outcome := make(chan captureOutcome, 1)
	go func() {
		captured, captureErr := run.CaptureControlAnchor(parent)
		outcome <- captureOutcome{anchor: captured, err: captureErr}
	}()
	controlAnchorWaitStage(t, entered, "the pre-publication stage of a healthy capture")
	cancelParent()
	close(release)
	select {
	case got := <-outcome:
		if got.err == nil {
			t.Fatal("capture published a capability after its own parent context was canceled before publication")
		}
		if !strings.Contains(got.err.Error(), drillControlAnchorContextEndedReason) {
			t.Fatalf("capture cancellation refusal is not the fixed publication cause: %v", got.err)
		}
		if diagnostics := got.anchor.Diagnostics(); diagnostics.Present {
			t.Fatal("canceled capture returned a usable capability")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("capture did not complete after its own parent cancellation")
	}
	// The refused publication left no shared state behind: a fresh capture
	// against the still-live control lock succeeds and the lock stays healthy.
	fresh, err := run.CaptureControlAnchor(e.ctx)
	if err != nil {
		t.Fatalf("fresh capture after a canceled publication: %v", err)
	}
	if !fresh.Diagnostics().Present {
		t.Fatal("fresh capture after a canceled publication is inert")
	}
	if err := lock.Health(e.ctx); err != nil {
		t.Fatalf("control lock disturbed by the canceled publication: %v", err)
	}
}

// TestControlAnchorBoundedContextsCapLongCallerDeadline proves CA02: an
// arbitrarily long caller deadline can never extend the component read window.
// Capture and Recheck must refuse at the 3s fact bound while the actual mutex
// is held, and a sooner caller deadline must be respected as-is.
func TestControlAnchorBoundedContextsCapLongCallerDeadline(t *testing.T) {
	e, run, lock, _ := controlAnchorFixtureRun(t)
	longCtx, cancelLong := context.WithTimeout(e.ctx, time.Hour)
	defer cancelLong()

	lock.mu.Lock()
	factStart := time.Now()
	_, captureErr := run.CaptureControlAnchor(longCtx)
	factElapsed := time.Since(factStart)
	lock.mu.Unlock()
	if captureErr == nil {
		t.Fatal("capture succeeded while the actual control mutex was held")
	}
	if factElapsed > 6*time.Second || factElapsed < 2*time.Second {
		t.Fatalf("capture fact stage was not bounded by the component bound: %s", factElapsed)
	}

	anchor, err := run.CaptureControlAnchor(e.ctx)
	if err != nil {
		t.Fatalf("control anchor capture: %v", err)
	}
	shortCtx, cancelShort := context.WithTimeout(e.ctx, 250*time.Millisecond)
	defer cancelShort()
	lock.mu.Lock()
	shortStart := time.Now()
	shortErr := anchor.Recheck(shortCtx)
	shortElapsed := time.Since(shortStart)
	lock.mu.Unlock()
	if shortErr == nil {
		t.Fatal("recheck succeeded while the actual control mutex was held")
	}
	if shortElapsed > 2*time.Second || shortElapsed < 100*time.Millisecond {
		t.Fatalf("sooner caller deadline was not respected: %s", shortElapsed)
	}
	if diagnostics := anchor.Diagnostics(); !diagnostics.Invalidated {
		t.Fatal("fact-stage refusal did not permanently invalidate the anchor")
	}
}

// TestControlAnchorHealthStageRespectsBoundedContext proves CA03: after fact
// inspection the genuine Health stage must respect the bounded context. The
// test pauses the healthy call at the Health stage, then holds the ACTUAL
// control mutex; Recheck/Capture must deadline-refuse and permanently
// invalidate without waiting for the mutex to be released. This integrates
// with fix99's context-aware TargetLock.Health; at the prepatch targetlock
// source (blocking mutex acquisition) this negative fails and must not be
// reported as merged.
func TestControlAnchorHealthStageRespectsBoundedContext(t *testing.T) {
	e, run, lock, _ := controlAnchorFixtureRun(t)
	anchor, err := run.CaptureControlAnchor(e.ctx)
	if err != nil {
		t.Fatalf("control anchor capture: %v", err)
	}
	longCtx, cancelLong := context.WithTimeout(e.ctx, time.Hour)
	defer cancelLong()

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	installControlAnchorStageHook(t, func(stage drillControlAnchorTestStage) {
		if stage != drillControlAnchorStageRecheckHealth {
			return
		}
		once.Do(func() { close(entered) })
		<-release
	})
	result := make(chan error, 1)
	go func() { result <- anchor.Recheck(longCtx) }()
	controlAnchorWaitStage(t, entered, "the genuine Health stage of a healthy recheck")
	lock.mu.Lock()
	stageStart := time.Now()
	close(release)
	var (
		recheckErr error
		stageTimed bool
	)
	select {
	case recheckErr = <-result:
	case <-time.After(5 * time.Second):
		stageTimed = true
	}
	stageElapsed := time.Since(stageStart)
	lock.mu.Unlock()
	if stageTimed {
		t.Fatal("anchor Recheck waited for the actual control mutex at the genuine Health stage: targetlock.Health is not context-aware at this source hash")
	}
	if recheckErr == nil {
		t.Fatal("health-stage bounded recheck unexpectedly succeeded")
	}
	if stageElapsed > 3*time.Second {
		t.Fatalf("health stage exceeded its one-second component bound: %s", stageElapsed)
	}
	if diagnostics := anchor.Diagnostics(); !diagnostics.Invalidated {
		t.Fatal("health-stage refusal did not permanently invalidate the anchor")
	}

	// The Capture publication path must be bounded at its healthy stage too.
	_, run2, lock2, _ := controlAnchorFixtureRun(t)
	entered2 := make(chan struct{})
	release2 := make(chan struct{})
	var once2 sync.Once
	installControlAnchorStageHook(t, func(stage drillControlAnchorTestStage) {
		if stage != drillControlAnchorStageCaptureHealth {
			return
		}
		once2.Do(func() { close(entered2) })
		<-release2
	})
	result2 := make(chan error, 1)
	go func() {
		_, captureErr := run2.CaptureControlAnchor(longCtx)
		result2 <- captureErr
	}()
	controlAnchorWaitStage(t, entered2, "the genuine Health stage of a capture")
	lock2.mu.Lock()
	stageStart2 := time.Now()
	close(release2)
	var (
		captureErr  error
		stageTimed2 bool
	)
	select {
	case captureErr = <-result2:
	case <-time.After(5 * time.Second):
		stageTimed2 = true
	}
	stageElapsed2 := time.Since(stageStart2)
	lock2.mu.Unlock()
	if stageTimed2 {
		t.Fatal("CaptureControlAnchor waited for the actual control mutex at the genuine Health stage")
	}
	if captureErr == nil {
		t.Fatal("health-stage bounded capture unexpectedly succeeded")
	}
	if stageElapsed2 > 3*time.Second {
		t.Fatalf("capture health stage exceeded its one-second component bound: %s", stageElapsed2)
	}
	if err := lock2.Health(e.ctx); err != nil {
		t.Fatalf("capture health-stage refusal disturbed the control lock: %v", err)
	}
}

func TestControlAnchorOwnerTerminationPermanentlyInvalidates(t *testing.T) {
	e, run, _, _ := controlAnchorFixtureRun(t)
	anchor, err := run.CaptureControlAnchor(e.ctx)
	if err != nil {
		t.Fatalf("control anchor capture: %v", err)
	}
	facts := anchor.Diagnostics()
	if _, err := e.ctrl.Exec(e.ctx, `SELECT pg_terminate_backend($1)`, facts.BackendPID); err != nil {
		t.Fatalf("terminate captured control owner: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		if err := anchor.Recheck(e.ctx); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("terminated control owner still rechecked as a live anchor")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := anchor.Recheck(e.ctx); err == nil {
		t.Fatal("permanent invalidation after owner termination did not persist")
	}
	if diagnostics := anchor.Diagnostics(); !diagnostics.Invalidated || diagnostics.InvalidReason == "" {
		t.Fatal("terminated owner anchor did not record a permanent invalidation")
	}
	if err := run.Health(e.ctx); err == nil {
		t.Fatal("terminated control owner still reported healthy")
	}
}

func TestControlAnchorReleasePermanentlyInvalidates(t *testing.T) {
	e, run, lock, _ := controlAnchorFixtureRun(t)
	anchor, err := run.CaptureControlAnchor(e.ctx)
	if err != nil {
		t.Fatalf("control anchor capture: %v", err)
	}
	if err := anchor.Recheck(e.ctx); err != nil {
		t.Fatalf("fresh anchor recheck: %v", err)
	}
	if err := lock.Release(context.Background()); err != nil {
		t.Fatalf("release borrowed lock: %v", err)
	}
	if err := anchor.Recheck(e.ctx); err == nil {
		t.Fatal("released control owner still rechecked as a live anchor")
	}
	if err := anchor.Recheck(e.ctx); err == nil {
		t.Fatal("release invalidation did not persist")
	}
	if diagnostics := anchor.Diagnostics(); !diagnostics.Invalidated {
		t.Fatal("released control owner anchor did not record a permanent invalidation")
	}
}

func TestControlAnchorCopiesShareInvalidationAndInertValues(t *testing.T) {
	e, run, lock, _ := controlAnchorFixtureRun(t)
	anchor, err := run.CaptureControlAnchor(e.ctx)
	if err != nil {
		t.Fatalf("control anchor capture: %v", err)
	}
	copied := anchor // value copy: shares the private pointer state

	// No JSON representation carries authority.
	encoded, err := json.Marshal(anchor)
	if err != nil || string(encoded) != "{}" {
		t.Fatalf("anchor serialized authority material: err=%v doc=%q", err, encoded)
	}
	var decoded DrillControlAnchor
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal zero anchor: %v", err)
	}
	if err := decoded.Recheck(e.ctx); err == nil {
		t.Fatal("JSON-decoded anchor rechecked")
	}
	var zero DrillControlAnchor
	if err := zero.Recheck(e.ctx); err == nil {
		t.Fatal("zero anchor rechecked")
	}
	if facts := zero.Diagnostics(); facts.Present {
		t.Fatal("zero anchor reported diagnostic facts")
	}
	// No caller-fact constructor: an inert run cannot capture.
	var zeroRun DrillBorrowedWriterRun
	if _, err := zeroRun.CaptureControlAnchor(e.ctx); err == nil {
		t.Fatal("inert run captured a control anchor")
	}
	// Nil and already-canceled contexts are refused.
	if _, err := run.CaptureControlAnchor(nil); err == nil {
		t.Fatal("nil context captured a control anchor")
	}
	canceled, cancel := context.WithCancel(e.ctx)
	cancel()
	if _, err := run.CaptureControlAnchor(canceled); err == nil {
		t.Fatal("canceled context captured a control anchor")
	}

	// A busy actual control mutex is bounded by the caller context without
	// abandoning a locker goroutine.
	lock.mu.Lock()
	busyCtx, cancelBusy := context.WithTimeout(e.ctx, 250*time.Millisecond)
	started := time.Now()
	_, busyErr := run.CaptureControlAnchor(busyCtx)
	elapsed := time.Since(started)
	lock.mu.Unlock()
	cancelBusy()
	if busyErr == nil || elapsed > 3*time.Second {
		t.Fatalf("busy control mutex was not refused within the bounded context (elapsed=%s err=%v)", elapsed, busyErr)
	}

	// Copies share the permanent invalidation.
	if err := lock.Release(context.Background()); err != nil {
		t.Fatalf("release borrowed lock: %v", err)
	}
	if err := copied.Recheck(e.ctx); err == nil {
		t.Fatal("copied anchor rechecked after control owner release")
	}
	if err := anchor.Recheck(e.ctx); err == nil {
		t.Fatal("original anchor rechecked after control owner release")
	}
	if diagnostics := copied.Diagnostics(); !diagnostics.Invalidated {
		t.Fatal("copied anchor did not observe the shared invalidation")
	}
	if diagnostics := anchor.Diagnostics(); !diagnostics.Invalidated {
		t.Fatal("original anchor did not record the shared invalidation")
	}
}
