//go:build linux && drill

// borrowed-run-invariants_linux_test.go proves the factory-lifetime invariants
// of the borrowed-writer run: every struct copy shares exactly one reservation
// behind the private lifetime pointer, zero/JSON values are permanently inert,
// a canceled invocation still consumes the single attempt before entry, and a
// conflicting private executable selection is refused while the authentic
// factory always selects (and revalidates) the sealed private restore path.
// It also proves that visibility facts missing from the control-owner session
// stay INCOMPLETE instead of being published as all-true.
package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func borrowedTransportContains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

// TestBorrowedWriterRunCopyReservationAndExecutableSelection covers the
// one-lifetime reservation of factory-created runs and the factory's private
// sealed executable selection.
func TestBorrowedWriterRunCopyReservationAndExecutableSelection(t *testing.T) {
	unsetBorrowedTransportPGEnvironment(t)
	e := newBorrowedTransportPG(t)
	tools, err := DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed tools: %v", err)
	}
	endpoint, err := DrillOpenOriginEndpoint()
	if err != nil {
		t.Fatalf("open factory endpoint: %v", err)
	}
	defer endpoint.Listener().Close()
	originalKey, err := CanonicalTargetKey(e.target)
	if err != nil {
		t.Fatalf("original target key: %v", err)
	}
	lock, err := AcquireTargetLock(e.ctx, e.ctrlDSN, originalKey, 3*time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire real borrowed lock: %v", err)
	}
	defer lock.Release(context.Background())
	opts := TargetWriterOptions{
		OperationKind: TargetWriterOperationRestore,
		Store:         e.store, ControlDSN: e.ctrlDSN, TargetDSN: e.targetDSN,
		ObserverDSN: borrowedTransportObserverDSN(t, e.adminDSN, e.targetDSN), TrustedTarget: e.target,
		OperationID: "borrow-invariants-1",
		Archive:     strings.NewReader("archive"),
		Probe: func(context.Context, TargetWriterProof) (TargetWriterProbeResult, error) {
			return TargetWriterProbeResult{}, errors.New("invariants probe intentionally refuses")
		},
		Acceptance: func(context.Context, pgx.Tx, TargetWriterProof) (TargetWriterAcceptance, error) {
			return TargetWriterAcceptance{}, errors.New("invariants acceptance intentionally refuses")
		},
	}

	// A conflicting private executable selection is refused; the factory alone
	// chooses the sealed private path.
	conflicting := opts
	conflicting.executable = "/bin/true"
	if run, err := DrillNewBorrowedWriterRun(e.ctx, conflicting, lock, endpoint, tools); err == nil || run != nil {
		t.Fatal("factory accepted a conflicting private restore executable")
	} else if !strings.Contains(err.Error(), "conflicting private restore executable") {
		t.Fatalf("conflicting executable refusal is not the factory cause: %v", err)
	}

	run, err := DrillNewBorrowedWriterRun(e.ctx, opts, lock, endpoint, tools)
	if err != nil {
		t.Fatalf("borrowed writer run factory: %v", err)
	}
	if run.opts.executable != tools.sealed.restore.path {
		t.Fatal("factory did not select the private sealed restore executable")
	}

	// Zero value and JSON round trip are permanently inert.
	var zero DrillBorrowedWriterRun
	if _, _, err := zero.Run(e.ctx); err == nil || !strings.Contains(err.Error(), "factory") {
		t.Fatalf("zero run was not inert: %v", err)
	}
	encoded, err := json.Marshal(run)
	if err != nil {
		t.Fatalf("marshal run: %v", err)
	}
	var decoded DrillBorrowedWriterRun
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal run: %v", err)
	}
	if _, _, err := decoded.Run(e.ctx); err == nil || !strings.Contains(err.Error(), "factory") {
		t.Fatalf("JSON round-trip run was not inert: %v", err)
	}

	// Actual struct copies (not pointer aliases) taken BEFORE the first
	// invocation share exactly one reservation: concurrent canceled-context
	// invocations yield exactly one reserved entry and one refusal, with no
	// child ever started and no observation fact touched.
	copiedA, copiedB := *run, *run
	canceledCtx, cancelCanceled := context.WithCancel(e.ctx)
	cancelCanceled()
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, copied := range []*DrillBorrowedWriterRun{&copiedA, &copiedB} {
		wg.Add(1)
		go func(candidate *DrillBorrowedWriterRun) {
			defer wg.Done()
			_, _, err := candidate.Run(canceledCtx)
			results <- err
		}(copied)
	}
	wg.Wait()
	close(results)
	entries, refusals := 0, 0
	for err := range results {
		if err == nil {
			t.Fatal("canceled-context invocation unexpectedly succeeded")
		}
		if strings.Contains(err.Error(), "already reserved") {
			refusals++
		} else {
			entries++
		}
	}
	if entries != 1 || refusals != 1 {
		t.Fatalf("shared lifetime did not yield exactly one entry and one refusal: entries=%d refusals=%d", entries, refusals)
	}
	handle := run.Observation()
	if identity := handle.StartedIdentity(); identity.Started {
		t.Fatal("a canceled-before-entry invocation started a child")
	}
	if terminal := handle.WaitTerminal(); terminal.Terminal {
		t.Fatal("a canceled-before-entry invocation fabricated terminal wait facts")
	}

	// Terminal/canceled refusal: any later invocation through any copy is
	// refused without reacquiring a lock or starting a new target namespace.
	if _, _, err := copiedA.Run(e.ctx); err == nil || !strings.Contains(err.Error(), "already reserved") {
		t.Fatalf("post-cancel invocation through a copy was not refused: %v", err)
	}
	if _, _, err := copiedB.Run(e.ctx); err == nil || !strings.Contains(err.Error(), "already reserved") {
		t.Fatalf("post-cancel invocation through the second copy was not refused: %v", err)
	}
	if _, _, err := run.Run(e.ctx); err == nil || !strings.Contains(err.Error(), "already reserved") {
		t.Fatalf("post-cancel invocation through the original was not refused: %v", err)
	}
}

// TestBorrowedTransportPhase1IncompleteControlFactsStayIncomplete proves a
// control-owner session that cannot expose the cluster system_identifier is
// published as an INCOMPLETE capture (not all-true, never a mismatched
// cluster), while the readable postmaster/namespace facts are still captured.
func TestBorrowedTransportPhase1IncompleteControlFactsStayIncomplete(t *testing.T) {
	unsetBorrowedTransportPGEnvironment(t)
	e := newBorrowedTransportPG(t)
	tools, err := DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed tools: %v", err)
	}
	endpoint, err := DrillOpenOriginEndpoint()
	if err != nil {
		t.Fatalf("open factory endpoint: %v", err)
	}
	defer endpoint.Listener().Close()
	originalKey, err := CanonicalTargetKey(e.target)
	if err != nil {
		t.Fatalf("original target key: %v", err)
	}
	// Remove PUBLIC's execute on the cluster-identity function IN THE CONTROL
	// DATABASE so the limited control-owner role cannot read it; function ACLs
	// are per-database and the superuser observer still can.
	if _, err := e.ctrl.Exec(e.ctx, `REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM PUBLIC`); err != nil {
		t.Fatalf("revoke control identity from PUBLIC: %v", err)
	}
	limitedRole := "borrowed_limited_control_owner"
	if _, err := e.admin.Exec(e.ctx, `CREATE ROLE `+pgx.Identifier{limitedRole}.Sanitize()+` LOGIN PASSWORD 'borrowed-limited-password'`); err != nil {
		t.Fatalf("create limited control role: %v", err)
	}
	limitedDSN := borrowedTransportDSNWithRole(t, e.ctrlDSN, limitedRole, "borrowed-limited-password")
	limitedConn, err := pgx.Connect(e.ctx, limitedDSN)
	if err != nil {
		t.Fatalf("connect limited control role: %v", err)
	}
	var limitedIdentity string
	if err := limitedConn.QueryRow(e.ctx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&limitedIdentity); err == nil {
		_ = limitedConn.Close(context.Background())
		t.Fatal("fixture premise failed: limited control role can still read the cluster identity")
	}
	if err := limitedConn.Close(context.Background()); err != nil {
		t.Fatalf("close limited control connection: %v", err)
	}
	lock, err := AcquireTargetLock(e.ctx, limitedDSN, originalKey, 3*time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire limited-role borrowed lock: %v", err)
	}
	defer lock.Release(context.Background())
	opts := TargetWriterOptions{
		OperationKind: TargetWriterOperationRestore,
		Store:         e.store, ControlDSN: limitedDSN, TargetDSN: e.targetDSN,
		ObserverDSN: borrowedTransportObserverDSN(t, e.adminDSN, e.targetDSN), TrustedTarget: e.target,
		OperationID: "borrow-invariants-incomplete-1",
		Archive:     strings.NewReader("archive"),
		Probe: func(context.Context, TargetWriterProof) (TargetWriterProbeResult, error) {
			return TargetWriterProbeResult{}, errors.New("invariants probe intentionally refuses")
		},
		Acceptance: func(context.Context, pgx.Tx, TargetWriterProof) (TargetWriterAcceptance, error) {
			return TargetWriterAcceptance{}, errors.New("invariants acceptance intentionally refuses")
		},
	}
	run, err := DrillNewBorrowedWriterRun(e.ctx, opts, lock, endpoint, tools)
	if err != nil {
		t.Fatalf("factory refused an incomplete-but-coherent control-owner capture: %v", err)
	}
	binding := run.Binding()
	if binding.SQLFactsComplete() {
		t.Fatal("incomplete control-owner facts were published as complete")
	}
	if !borrowedTransportContains(binding.MissingSQLFacts(), "control cluster system_identifier") {
		t.Fatalf("missing facts do not name the unreadable control cluster identity: %v", binding.MissingSQLFacts())
	}
	if binding.ControlPostmasterStart().IsZero() {
		t.Fatal("readable control postmaster incarnation must still be captured")
	}
	if binding.ClusterSystemIdentifier() == "" || binding.PostmasterStartTime().IsZero() {
		t.Fatal("observer-side original cluster facts must still be captured")
	}
	if binding.OriginalTargetKey() != originalKey || binding.ControlTargetKey() == (TargetKey{}) {
		t.Fatal("incomplete capture changed the original target/control binding")
	}
	if state := binding.Phase2OSBindingState(); state != "OPEN" {
		t.Fatalf("phase-2 OS binding must stay explicitly OPEN, got %q", state)
	}
}
