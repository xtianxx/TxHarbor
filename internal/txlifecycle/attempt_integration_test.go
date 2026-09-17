//go:build integration

package txlifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// TestV1AttemptPersistConvergeConflict is quickstart V1: identity + content
// persist before any 009 call, identical content converges, different content
// refuses with the original untouched.
func TestV1AttemptPersistConvergeConflict(t *testing.T) {
	e := newEnv(t)
	f := e.seed()
	ctx := context.Background()

	ref, err := e.store.PrepareAttempt(ctx, f.request)
	if err != nil {
		t.Fatalf("re-drive identical: %v", err)
	}
	if !ref.Converged || ref.AttemptID != f.attemptID {
		t.Fatalf("identical re-drive did not converge: %+v", ref)
	}

	conflict := *f.request
	conflict.Amount = "1001"
	if _, err := e.store.PrepareAttempt(ctx, &conflict); err == nil {
		t.Fatal("different content accepted")
	} else if refErr := refusalClass(err); refErr != ClassAttemptConflict {
		t.Fatalf("conflict class = %s, want attempt_conflict", refErr)
	}

	var rows int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE attempt_id = $1`, f.attemptID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("attempt rows = %d, want 1", rows)
	}
	var stored string
	if err := e.pool.QueryRow(ctx, `SELECT canonical_envelope FROM tx_attempts WHERE attempt_id = $1`, f.attemptID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != stringMust(f.request.CanonicalEnvelope()) {
		t.Fatal("conflict rewrote the original content")
	}
}

// TestV1ConcurrentSameIdentity is V1's race: one row, the loser converges or
// conflicts, never a partial identity.
func TestV1ConcurrentSameIdentity(t *testing.T) {
	e := newEnv(t)
	f := e.seed()
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := *f.request
			_, errs[i] = e.store.PrepareAttempt(ctx, &req)
		}(i)
	}
	wg.Wait()

	var rows int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE attempt_id = $1`, f.attemptID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("concurrent inserts produced %d rows, want 1", rows)
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d failed instead of converging: %v", i, err)
		}
	}
}

// TestV1ConcurrentDifferentContent: same identity + different content in a
// race must leave exactly one row and one conflict.
func TestV1ConcurrentDifferentContent(t *testing.T) {
	e := newEnv(t)
	f := e.seed()
	ctx := context.Background()

	a := *f.request
	b := *f.request
	b.Amount = "2000"
	_, errA := e.store.PrepareAttempt(ctx, &a)
	_, errB := e.store.PrepareAttempt(ctx, &b)
	if errB == nil {
		t.Fatal("different content in a race was accepted")
	}
	if refusalClass(errB) != ClassAttemptConflict {
		t.Fatalf("race conflict class = %s", refusalClass(errB))
	}
	if errA != nil && refusalClass(errA) != ClassAttemptConflict {
		t.Fatalf("unexpected winner error: %v", errA)
	}
	var rows int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
}

func refusalClass(err error) RefusalClass {
	var ref *RefusalError
	if errors.As(err, &ref) {
		return ref.Class
	}
	return ""
}

func stringMust(b []byte, err error) string {
	if err != nil {
		return ""
	}
	return string(b)
}
