package events

import (
	"strings"
	"testing"
	"time"
)

func TestPublishStateValid(t *testing.T) {
	for _, state := range []PublishState{PublishStatePending, PublishStatePublished, PublishStateBlocked} {
		if !state.Valid() {
			t.Fatalf("PublishState(%q).Valid() = false", state)
		}
	}
	for _, state := range []PublishState{"", "dropped", "PENDING"} {
		if state.Valid() {
			t.Fatalf("PublishState(%q).Valid() = true", state)
		}
	}
}

func TestPublishStateTransitionMatrix(t *testing.T) {
	legal := map[[2]PublishState]bool{
		{PublishStatePending, PublishStatePublished}: true,
		{PublishStatePending, PublishStateBlocked}:   true,
		{PublishStateBlocked, PublishStatePending}:   true,
	}
	states := []PublishState{PublishStatePending, PublishStatePublished, PublishStateBlocked}
	for _, from := range states {
		for _, to := range states {
			want := legal[[2]PublishState{from, to}]
			if got := CanTransition(from, to); got != want {
				t.Fatalf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
	if CanTransition("unknown", PublishStatePending) || CanTransition(PublishStatePending, "unknown") {
		t.Fatal("unknown states must not transition")
	}
}

func TestOutboxQueryGuards(t *testing.T) {
	requireContains := func(name, sql string, needles ...string) {
		t.Helper()
		for _, needle := range needles {
			if !strings.Contains(sql, needle) {
				t.Fatalf("%s missing %q:\n%s", name, needle, sql)
			}
		}
	}
	requireAbsent := func(name, sql string, needles ...string) {
		t.Helper()
		for _, needle := range needles {
			if strings.Contains(strings.ToLower(sql), strings.ToLower(needle)) {
				t.Fatalf("%s must not contain %q:\n%s", name, needle, sql)
			}
		}
	}

	requireContains("ClaimPendingSQL", ClaimPendingSQL,
		"publish_state = 'pending'", "next_attempt_at <= now()", "ORDER BY id",
		"LIMIT $1", "FOR UPDATE SKIP LOCKED")
	requireContains("ClaimMarkSQL", ClaimMarkSQL,
		"claim_owner = $2", "claim_expires_at", "publish_state = 'pending'")
	requireContains("AckPublishedSQL", AckPublishedSQL,
		"publish_state = 'published'", "published_at = now()",
		"claim_owner = $2", "publish_state = 'pending'")
	requireContains("ReleaseClaimSQL", ReleaseClaimSQL,
		"next_attempt_at = now()", "last_error_class = $4", "claim_owner = $2",
		"publish_state = 'pending'")
	requireContains("BlockClaimSQL", BlockClaimSQL,
		"publish_state = 'blocked'", "last_error_class = $3",
		"claim_owner = $2", "publish_state = 'pending'")
	requireContains("UnblockSQL", UnblockSQL,
		"SET publish_state = 'pending'", "publish_state = 'blocked'", "last_error_class = NULL")
	requireContains("CapacitySQL", CapacitySQL,
		"publish_state = 'pending'", "GROUP BY 1", "min(created_at)")
	requireContains("PruneWatermarkSQL", PruneWatermarkSQL,
		"publish_state = 'published'", "min(published_at)")
	requireContains("PruneSQL", PruneSQL,
		"DELETE FROM outbox_events", "publish_state = 'published'",
		"published_at < now()")

	// Retention must never touch pending or blocked rows.
	requireAbsent("PruneSQL", PruneSQL, "'pending'", "'blocked'")
	requireAbsent("PruneWatermarkSQL", PruneWatermarkSQL, "'pending'", "'blocked'")

	// No query may carry a network call: SQL stays in PG (T012/T016).
	for name, sql := range map[string]string{
		"ClaimPendingSQL": ClaimPendingSQL, "ClaimMarkSQL": ClaimMarkSQL,
		"AckPublishedSQL": AckPublishedSQL, "ReleaseClaimSQL": ReleaseClaimSQL,
		"BlockClaimSQL": BlockClaimSQL, "UnblockSQL": UnblockSQL,
		"CapacitySQL": CapacitySQL, "PruneWatermarkSQL": PruneWatermarkSQL,
		"PruneSQL": PruneSQL,
	} {
		requireAbsent(name, sql, "http", "kafka", "produce", "redis")
	}
}

func TestRetryDelay(t *testing.T) {
	base := time.Second
	maxDelay := 60 * time.Second

	if got := RetryDelay(1, base, maxDelay, nil); got != base {
		t.Fatalf("RetryDelay(1) = %s, want %s", got, base)
	}
	if got := RetryDelay(0, base, maxDelay, nil); got != base {
		t.Fatalf("RetryDelay(0) = %s, want %s (attempt clamped to 1)", got, base)
	}
	if got := RetryDelay(2, base, maxDelay, nil); got != 2*time.Second {
		t.Fatalf("RetryDelay(2) = %s, want 2s", got)
	}
	if got := RetryDelay(3, base, maxDelay, nil); got != 4*time.Second {
		t.Fatalf("RetryDelay(3) = %s, want 4s", got)
	}
	if got := RetryDelay(7, base, maxDelay, nil); got != maxDelay {
		t.Fatalf("RetryDelay(7) = %s, want the 60s cap", got)
	}
	if got := RetryDelay(100, base, maxDelay, nil); got != maxDelay {
		t.Fatalf("RetryDelay(100) = %s, want the 60s cap", got)
	}

	if got := RetryDelay(1, base, maxDelay, func() float64 { return 1 }); got != 1200*time.Millisecond {
		t.Fatalf("RetryDelay(jitter=+1) = %s, want 1.2s", got)
	}
	if got := RetryDelay(1, base, maxDelay, func() float64 { return -1 }); got != 800*time.Millisecond {
		t.Fatalf("RetryDelay(jitter=-1) = %s, want 0.8s", got)
	}
	if got := RetryDelay(1, base, maxDelay, func() float64 { return 0 }); got != base {
		t.Fatalf("RetryDelay(jitter=0) = %s, want %s", got, base)
	}
	// The cap applies before jitter, so the jittered value stays within ±20%
	// of the cap, never unbounded.
	if got := RetryDelay(100, base, maxDelay, func() float64 { return 1 }); got != 72*time.Second {
		t.Fatalf("RetryDelay(capped, jitter=+1) = %s, want 72s", got)
	}
}
