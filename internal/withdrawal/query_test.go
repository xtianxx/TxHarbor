// query_test.go owns the T011 no-database unit tests: the pure recovery-state
// precedence combiner and the standing not_started execution fact. The
// database-backed half of T011 lives in query_integration_test.go.
package withdrawal

import (
	"errors"
	"testing"
)

// TestQueryCombineRecoveryState table-tests every branch of the contracts
// api.md §3 precedence: row-present phase mapping, released/none, the row over
// terminal-event ordering, and unknown on either read failure.
func TestQueryCombineRecoveryState(t *testing.T) {
	cases := []struct {
		name       string
		rowPresent bool
		phase      string
		released   bool
		readErr    error
		want       string
	}{
		{"no row, no event is none", false, "", false, nil, "none"},
		{"no row, terminal event is released", false, "", true, nil, "released"},
		{"detected maps to recovering", true, "detected", false, nil, "recovering"},
		{"ancestor_confirmed maps to recovering", true, "ancestor_confirmed", false, nil, "recovering"},
		{"invalidated maps to recovering", true, "invalidated", false, nil, "recovering"},
		{"replaying maps to recovering", true, "replaying", false, nil, "recovering"},
		{"complete_pending maps to recovering", true, "complete_pending", false, nil, "recovering"},
		{"reconcile_required maps to paused_reconcile", true, "reconcile_required", false, nil, "paused_reconcile"},
		{"active row wins over terminal event", true, "detected", true, nil, "recovering"},
		{"row read failure is unknown", false, "", false, errors.New("row read failed"), "unknown"},
		{"event read failure is unknown", false, "", false, errors.New("event read failed"), "unknown"},
		{"failure with an active row is still unknown", true, "reconcile_required", false, errors.New("boom"), "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := combineRecoveryState(tc.rowPresent, tc.phase, tc.released, tc.readErr)
			if got != tc.want {
				t.Fatalf("combineRecoveryState(row=%v, phase=%q, released=%v, err=%v) = %q, want %q",
					tc.rowPresent, tc.phase, tc.released, tc.readErr, got, tc.want)
			}
		})
	}
}

// TestQueryRecoverySignalExecutionNotStarted locks the standing fact: every
// recovery state carries execution=not_started (Q8), and the state passes
// through unchanged.
func TestQueryRecoverySignalExecutionNotStarted(t *testing.T) {
	states := []string{
		queryStateNone,
		queryStateRecovering,
		queryStatePausedReconcile,
		queryStateReleased,
		queryStateUnknown,
	}
	for _, state := range states {
		sig := queryRecoverySignal(state)
		if sig.Execution != "not_started" {
			t.Errorf("state %q: Execution = %q, want not_started", state, sig.Execution)
		}
		if sig.State != state {
			t.Errorf("state %q: State = %q, want passthrough", state, sig.State)
		}
	}
}
