//go:build integration

// claim_race_integration_test.go executes the V2 race variants (T021) under the
// Go race detector: a late renewal never revives an expired qualification, a
// renewal whose outcome is unknown is not an extension until the holder
// re-observes (DB time is the only arbiter), and bounded-backoff contention
// over many intents terminates without qualification errors or app-clock
// expiry.
package execution

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestClaimLateRenewalNeverRevives(t *testing.T) {
	ctx, pool := executionPool(t)
	seedExecIntentRow(t, ctx, pool, "intent-late")
	store := execClaimStore(t, pool)

	v1, ok, err := store.Acquire(ctx, "intent-late", "owner-a")
	if err != nil || !ok || v1 != 1 {
		t.Fatalf("first Acquire = %d/%v/%v, want v1 won", v1, ok, err)
	}
	expireExecClaim(t, ctx, pool, "intent-late")
	if err := store.Renew(ctx, "intent-late", "owner-a", 1); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("late Renew = %v, want ErrClaimLost", err)
	}
	if current, _ := ClaimIsCurrent(ctx, pool, "intent-late", "owner-a", 1); current {
		t.Fatal("late renewal revived an expired qualification")
	}
	v2, ok, err := store.Acquire(ctx, "intent-late", "owner-b")
	if err != nil || !ok || v2 != 2 {
		t.Fatalf("re-acquire = %d/%v/%v, want v2 won", v2, ok, err)
	}
}

func TestClaimRenewalUnknownIsNotExtension(t *testing.T) {
	ctx, pool := executionPool(t)
	seedExecIntentRow(t, ctx, pool, "intent-unknown")
	store := execClaimStore(t, pool)

	v1, ok, err := store.Acquire(ctx, "intent-unknown", "owner-a")
	if err != nil || !ok || v1 != 1 {
		t.Fatalf("Acquire = %d/%v/%v, want v1 won", v1, ok, err)
	}

	// A renewal whose outcome cannot be confirmed: the cancellation makes the
	// transaction fail before COMMIT, so the result is unknown, not success.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := store.Renew(canceled, "intent-unknown", "owner-a", 1); err == nil {
		t.Fatal("canceled Renew returned nil; outcome cannot be confirmed")
	}
	// The holder must re-observe rather than assume the extension. While the
	// row is still current the re-observation lets it continue; DB time is the
	// only arbiter, never the application clock.
	if current, err := ClaimIsCurrent(ctx, pool, "intent-unknown", "owner-a", 1); err != nil || !current {
		t.Fatalf("re-observe current = %v (%v), want true", current, err)
	}
	expireExecClaim(t, ctx, pool, "intent-unknown")
	if current, _ := ClaimIsCurrent(ctx, pool, "intent-unknown", "owner-a", 1); current {
		t.Fatal("re-observation treated an expired claim as current after an unknown renewal")
	}
}

func TestClaimContentionTerminates(t *testing.T) {
	ctx, pool := executionPool(t)
	const intents = 4
	for i := 0; i < intents; i++ {
		seedExecIntentRow(t, ctx, pool, "intent-contend-"+string(rune('a'+i)))
	}
	store := execClaimStore(t, pool)

	const workers = 6
	const rounds = 40
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + 1))
			for r := 0; r < rounds; r++ {
				intentID := "intent-contend-" + string(rune('a'+r%intents))
				owner := "owner-" + string(rune('a'+w))
				version, won, err := store.Acquire(ctx, intentID, owner)
				if err != nil {
					errCh <- err
					return
				}
				if won {
					_ = store.Release(ctx, intentID, owner, version)
				}
				// Bounded backoff with jitter; never an app-clock expiry.
				time.Sleep(time.Duration(rng.Intn(3)) * time.Millisecond)
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("contention error: %v", err)
	}
	for i := 0; i < intents; i++ {
		intentID := "intent-contend-" + string(rune('a'+i))
		if v, found := claimVersion(t, ctx, pool, intentID); !found || v < 1 {
			t.Fatalf("intent %s version = %d found=%v, want >=1", intentID, v, found)
		}
	}
}
