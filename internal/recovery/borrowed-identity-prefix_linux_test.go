//go:build linux && drill

// borrowed-identity-prefix_linux_test.go runs the four focused identity-prefix
// scenarios against the ACTUAL protected fixture. Tests 2 (second-fixture
// binding mismatch) and 4 (postmaster restart incarnation change) remain
// explicitly OPEN in this window; the first two implemented cases are the
// positive socket chain and the real owner-loss permanent invalidation.
package recovery_test

import (
	"context"
	"testing"
	"time"
)

func TestBorrowedIdentityPrefixMatchesActualSocketChain(t *testing.T) {
	f := newBorrowedIdentityFixture(t)
	prefix := f.Capture(t)
	copyPrefix := *prefix
	if invalid, reason := copyPrefix.Invalid(); invalid {
		t.Fatalf("fresh prefix copy is invalid: %s", reason)
	}
	if err := prefix.Recheck(t, context.Background()); err != nil {
		t.Fatalf("fresh anchor recheck: %v", err)
	}
	if invalid, reason := prefix.Invalid(); invalid {
		t.Fatalf("fresh recheck invalidated the prefix: %s", reason)
	}
	if identity := f.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("identity prefix started a writer child: %+v", identity)
	}
	binding := f.run.Binding()
	if binding.OriginalTargetKey() != f.targetKey || binding.ControlTargetKey() != f.controlKey {
		t.Fatalf("factory binding no longer names the original/control keys: %+v", binding)
	}
}

func TestBorrowedIdentityPrefixOwnerLossPermanentlyInvalidates(t *testing.T) {
	f := newBorrowedIdentityFixture(t)
	prefix := f.Capture(t)
	facts := prefix.anchor.Diagnostics()
	if facts.BackendPID <= 0 {
		t.Fatalf("captured anchor has no control backend pid: %+v", facts)
	}
	if _, err := f.fixture.admin.Exec(context.Background(), `SELECT pg_terminate_backend($1)`, facts.BackendPID); err != nil {
		t.Fatalf("terminate captured control owner: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var recheckErr error
	for time.Now().Before(deadlineOf(ctx)) {
		recheckErr = prefix.Recheck(t, ctx)
		if recheckErr != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if recheckErr == nil {
		t.Fatal("owner loss did not invalidate the anchor")
	}
	copyPrefix := *prefix
	if invalid, reason := copyPrefix.Invalid(); !invalid || reason == "" {
		t.Fatalf("permanent invalidation is not shared with copies: invalid=%t reason=%q", invalid, reason)
	}
	if err := f.run.Health(ctx); err == nil {
		t.Fatal("dead borrowed lock still reported healthy")
	}
	if err := prefix.Recheck(t, ctx); err == nil {
		t.Fatal("permanently invalid prefix rechecked successfully")
	}
	if identity := f.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("owner-loss scenario started a writer child: %+v", identity)
	}
}

func deadlineOf(ctx context.Context) time.Time {
	if deadline, ok := ctx.Deadline(); ok {
		return deadline
	}
	return time.Now()
}
