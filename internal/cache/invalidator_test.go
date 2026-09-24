// invalidator_test.go is the T060 unit evidence: the closed event-type →
// family mapping, exact-key and family-range deletion, idempotence, and the
// failure discipline (a Redis outage during invalidation never surfaces as an
// effect failure, never blocks the partition).
package cache

import (
	"context"
	"errors"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/xtianxx/txharbor/internal/events"
)

func TestAffectedRefsMapsCatalogV1(t *testing.T) {
	cases := []struct {
		eventType string
		want      Ref
	}{
		{events.EventTypeDepositObservationCreated, Ref{FamilyDeposit, "obs-1"}},
		{events.EventTypeDepositObservationStatusChanged, Ref{FamilyDeposit, "obs-1"}},
		{events.EventTypeDepositObservationReinstated, Ref{FamilyDeposit, "obs-1"}},
		{events.EventTypeDepositConfirmationConfirmed, Ref{FamilyDeposit, "obs-1"}},
		{events.EventTypeDepositRevisionApplied, Ref{FamilyDeposit, "obs-1"}},
		{events.EventTypeWithdrawalRequestReceived, Ref{FamilyWithdrawal, "w-1"}},
		{events.EventTypeWithdrawalExecutionStateChanged, Ref{FamilyExecution, "intent-1"}},
		{events.EventTypeWithdrawalExecutionRevised, Ref{FamilyExecution, "intent-1"}},
	}
	for _, tc := range cases {
		refs := AffectedRefs(events.Envelope{EventType: tc.eventType, AggregateID: tc.want.ID})
		if len(refs) != 1 || refs[0] != tc.want {
			t.Errorf("AffectedRefs(%s) = %+v, want [%+v]", tc.eventType, refs, tc.want)
		}
	}
	if refs := AffectedRefs(events.Envelope{EventType: "unknown.fact", AggregateID: "x"}); len(refs) != 0 {
		t.Fatalf("AffectedRefs(unknown) = %+v, want none", refs)
	}
}

func TestInvalidatorDeletesExactKeysAndFamilyRanges(t *testing.T) {
	store := newFakeStore()
	client := newTestClient(t, store, Config{
		Epoch: "1", TTL: time.Minute, Timeout: time.Second, MaxFallbackConcurrency: 1,
	})
	inv, err := NewInvalidator(client)
	if err != nil {
		t.Fatalf("NewInvalidator: %v", err)
	}
	ctx := context.Background()
	for _, key := range []string{
		client.Key(FamilyDeposit, "obs-1"),
		client.Key(FamilyDeposit, "obs-2"),
		client.Key(FamilyWithdrawal, "w-1"),
	} {
		if err := store.Set(ctx, key, []byte("{}"), time.Minute); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}

	inv.Handle(ctx, events.Envelope{EventType: events.EventTypeDepositObservationCreated, AggregateID: "obs-1"})
	if _, ok := client.Get(ctx, FamilyDeposit, "obs-1"); ok {
		t.Fatal("created event did not invalidate its exact key")
	}
	if _, ok := client.Get(ctx, FamilyDeposit, "obs-2"); !ok {
		t.Fatal("created event invalidated an unrelated key")
	}

	// A family-wide ref (empty ID) clears the whole family.
	inv.Invalidate(ctx, []Ref{{Family: FamilyDeposit}})
	if _, ok := client.Get(ctx, FamilyDeposit, "obs-2"); ok {
		t.Fatal("family-wide invalidation left a deposit key behind")
	}
	if _, ok := client.Get(ctx, FamilyWithdrawal, "w-1"); !ok {
		t.Fatal("family-wide invalidation crossed into another family")
	}

	// Idempotent: repeating the invalidation is harmless.
	inv.Invalidate(ctx, []Ref{{Family: FamilyDeposit}})
}

func TestInvalidatorRedisFailureIsLoggedNotReturned(t *testing.T) {
	store := newFakeStore()
	store.failGet = errors.New("redis: down")
	client := newTestClient(t, store, Config{
		Epoch: "1", TTL: time.Minute, Timeout: time.Second, MaxFallbackConcurrency: 1,
	})
	inv, err := NewInvalidator(client)
	if err != nil {
		t.Fatalf("NewInvalidator: %v", err)
	}
	// Invalidate swallows store errors; Apply must never turn a cache outage
	// into a consumer failure (the event would otherwise be quarantined for a
	// non-authoritative reason).
	if err := inv.Apply(context.Background(), nil, events.Envelope{
		EventType:   events.EventTypeWithdrawalRequestReceived,
		AggregateID: "w-1",
		EventID:     uuid.New(),
	}); err != nil {
		t.Fatalf("Apply during a Redis outage = %v, want nil", err)
	}
}

func TestNewInvalidatorRequiresClient(t *testing.T) {
	if _, err := NewInvalidator(nil); err == nil {
		t.Fatal("NewInvalidator(nil) succeeded, want fail-closed error")
	}
}

// TestFundingDecisionPathsNeverImportCache is the T059 structural half of
// "decision paths never read the cache" (contracts/redis.md §2.1): funding
// packages may not import this package at all.
func TestFundingDecisionPathsNeverImportCache(t *testing.T) {
	root := filepath.Join("..", "..", "internal")
	for _, pkg := range []string{"withdrawal", "execution", "nonce", "txlifecycle", "indexer", "events", "eth"} {
		dir := filepath.Join(root, pkg)
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir %s: %v", dir, err)
		}
		found := 0
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			found++
			file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, name), nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", name, err)
			}
			for _, imp := range file.Imports {
				if strings.Trim(imp.Path.Value, `"`) == "github.com/xtianxx/txharbor/internal/cache" {
					t.Errorf("%s/%s imports internal/cache: funding decision paths never read the cache", pkg, name)
				}
			}
		}
		if found == 0 {
			t.Errorf("package %s has no production sources; scan is stale", pkg)
		}
	}
}

// TestInvalidatorImplementsConsumerEffect pins the reuse seam: the invalidator
// is an events.Effect, so the consumer runtime can drive it unchanged.
func TestInvalidatorImplementsConsumerEffect(t *testing.T) {
	var _ events.Effect = (*Invalidator)(nil)
}
