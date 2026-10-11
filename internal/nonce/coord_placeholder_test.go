package nonce

import (
	"strings"
	"testing"
)

// TestCoordPlaceholderIsBornExpired pins the coordination placeholder contract
// (008 R1/R4 over 002's indexer_lease): 008 may only ever materialize the row
// as an already-expired lock anchor, never as a live lease. The regression
// this guards: a non-negative first-materialization offset leaves the row
// valid for that long, and the indexer's expiry-based CAS Acquire (which takes
// over only expires_at < now()) is then refused — silently stalling indexing
// until the offset lapses.
func TestCoordPlaceholderIsBornExpired(t *testing.T) {
	if coordPlaceholderExpirySeconds >= 0 {
		t.Fatalf("coordPlaceholderExpirySeconds = %v, want strictly negative: a non-negative offset materializes a live lease placeholder and blocks the indexer's Acquire", coordPlaceholderExpirySeconds)
	}
	if !strings.Contains(ensureCoordRowSQL, "now() + make_interval(secs => $4)") {
		t.Fatalf("ensureCoordRowSQL must derive the placeholder expiry from the database clock and the $4 offset, got:\n%s", ensureCoordRowSQL)
	}
	if !strings.Contains(ensureCoordRowSQL, "ON CONFLICT (chain_id) DO NOTHING") {
		t.Fatalf("ensureCoordRowSQL must never touch an existing lease row, got:\n%s", ensureCoordRowSQL)
	}
}
