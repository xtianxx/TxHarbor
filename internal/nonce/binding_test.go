package nonce

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestBindingTransitions pins the explicit state machine (R6/FR-20):
// allocated → in_flight → consumed|released, plus the direct
// allocated→consumed and allocated|in_flight→released edges; every other pair
// is illegal.
func TestBindingTransitions(t *testing.T) {
	states := []string{StateAllocated, StateInFlight, StateConsumed, StateReleased}
	legal := map[string]map[string]bool{
		StateAllocated: {StateInFlight: true, StateConsumed: true, StateReleased: true},
		StateInFlight:  {StateConsumed: true, StateReleased: true},
	}
	for _, from := range states {
		for _, to := range states {
			if got, want := CanTransition(from, to), legal[from][to]; got != want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
	// Unknown/empty states are refused in both directions.
	for _, s := range []string{"", "bogus", "ALLOCATED"} {
		if CanTransition(s, StateInFlight) || CanTransition(StateAllocated, s) {
			t.Errorf("unknown state %q accepted as transition endpoint", s)
		}
	}
}

// TestBindingTerminalImmutability pins that consumed/released are terminal:
// no outgoing edge exists, and the guarded writer refuses a terminal source
// with zero writes.
func TestBindingTerminalImmutability(t *testing.T) {
	states := []string{StateAllocated, StateInFlight, StateConsumed, StateReleased}
	for _, terminal := range []string{StateConsumed, StateReleased} {
		if !IsTerminal(terminal) {
			t.Fatalf("IsTerminal(%s) = false", terminal)
		}
		for _, to := range states {
			if CanTransition(terminal, to) {
				t.Errorf("terminal %s accepts edge to %s", terminal, to)
			}
		}
	}
	if IsTerminal(StateAllocated) || IsTerminal(StateInFlight) {
		t.Fatal("pending state reported terminal")
	}

	f := &fakeQuerier{}
	for _, to := range states {
		ok, err := advanceBindingStateTx(context.Background(), f, "b-1", to, "op-1", StateConsumed)
		if err == nil || ok {
			t.Errorf("advance consumed -> %s = (%v, %v), want refusal", to, ok, err)
		}
	}
	if got := f.execCount(); got != 0 {
		t.Fatalf("terminal advance issued %d writes, want 0", got)
	}
}

// TestBindingTerminalConsistency pins the local mirror of the
// nonce_bindings_terminal_consistency CHECK: pending states carry no terminal
// facts; each terminal state carries exactly its own.
func TestBindingTerminalConsistency(t *testing.T) {
	now := time.Now().UTC()
	op := "op-1"
	cases := []struct {
		name string
		b    Binding
		want bool
	}{
		{"allocated clean", Binding{State: StateAllocated}, true},
		{"allocated with consumed_at", Binding{State: StateAllocated, ConsumedAt: &now}, false},
		{"in_flight clean", Binding{State: StateInFlight}, true},
		{"consumed with consumed_at", Binding{State: StateConsumed, ConsumedAt: &now}, true},
		{"consumed missing consumed_at", Binding{State: StateConsumed}, false},
		{"consumed with full released facts", Binding{State: StateConsumed, ConsumedAt: &now, ReleasedAt: &now, ReleaseOperationID: &op}, false},
		{"released with released_at + op", Binding{State: StateReleased, ReleasedAt: &now, ReleaseOperationID: &op}, true},
		{"released missing release_operation_id", Binding{State: StateReleased, ReleasedAt: &now}, false},
		{"released missing released_at", Binding{State: StateReleased, ReleaseOperationID: &op}, false},
	}
	for _, tc := range cases {
		if got := tc.b.TerminalConsistent(); got != tc.want {
			t.Errorf("%s: TerminalConsistent() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestBindingNoAutomaticRelease pins R6: no automatic (observer-driven)
// transition ever produces released — release is operator-only. Every
// non-release automatic edge is exactly a legal edge.
func TestBindingNoAutomaticRelease(t *testing.T) {
	states := []string{StateAllocated, StateInFlight, StateConsumed, StateReleased, "bogus"}
	for _, from := range states {
		for _, to := range []string{StateAllocated, StateInFlight, StateConsumed, StateReleased} {
			if CanAutoTransition(from, StateReleased) {
				t.Errorf("automatic %s -> released allowed", from)
			}
			if to == StateReleased {
				continue
			}
			if got, want := CanAutoTransition(from, to), CanTransition(from, to); got != want {
				t.Errorf("auto edge %s -> %s = %v, want %v", from, to, got, want)
			}
		}
	}
}

// TestBindingEventWriterAppendOnly pins the append-only event writer: one
// INSERT into nonce_binding_events converging on the named
// (binding_id, to_state) UNIQUE, so a repeat transition never duplicates
// history.
func TestBindingEventWriterAppendOnly(t *testing.T) {
	f := &fakeQuerier{}
	from := StateAllocated
	ok, err := insertBindingEventTx(context.Background(), f, BindingEvent{
		BindingID: "b-1", FromState: &from, ToState: StateConsumed,
		ObservationID: "obs-1", Detail: "reconcile observed consumption",
	})
	if err != nil || !ok {
		t.Fatalf("insertBindingEventTx = (%v, %v), want (true, nil)", ok, err)
	}
	execs := f.recordedExecs()
	if len(execs) != 1 ||
		!strings.Contains(execs[0], "INSERT INTO nonce_binding_events") ||
		!strings.Contains(execs[0], "ON CONFLICT (binding_id, to_state) DO NOTHING") {
		t.Fatalf("event writer execs = %q, want one converging INSERT", execs)
	}

	// The creation event has no from state (NULL); it is still one append.
	f2 := &fakeQuerier{}
	ok, err = insertBindingEventTx(context.Background(), f2, BindingEvent{
		BindingID: "b-1", ToState: StateAllocated, Detail: "created",
	})
	if err != nil || !ok {
		t.Fatalf("creation insertBindingEventTx = (%v, %v), want (true, nil)", ok, err)
	}
}

// TestBindingReaders pins the nonce as integer text (float-free) readers by
// stable identity and by intent, including an absent row.
func TestBindingReaders(t *testing.T) {
	sender := "0x" + strings.Repeat("ab", 20)
	now := time.Now().UTC()
	rowVals := []any{
		"b-1", "intent-1", int64(31337), sender, "7", StateAllocated,
		"auth-1", strings.Repeat("0", 64), int64(9), "obs-1",
		now, now, nil, nil, nil,
	}

	f := &fakeQuerier{handler: func(string, []any) pgx.Row { return rows(rowVals...) }}
	b, err := readBindingByIDTx(context.Background(), f, "b-1")
	if err != nil || b == nil {
		t.Fatalf("readBindingByIDTx = (%v, %v)", b, err)
	}
	if b.Nonce == nil || b.Nonce.String() != "7" || b.RegistrySeq != 9 || b.State != StateAllocated {
		t.Fatalf("binding = %+v", b)
	}
	if qs := f.recordedQueries(); len(qs) != 1 || !strings.Contains(qs[0], "WHERE binding_id = $1") {
		t.Fatalf("reader queries = %q", qs)
	}

	f2 := &fakeQuerier{handler: func(string, []any) pgx.Row { return rows(rowVals...) }}
	if _, err := readBindingByIntentTx(context.Background(), f2, "intent-1"); err != nil {
		t.Fatalf("readBindingByIntentTx error = %v", err)
	}
	if qs := f2.recordedQueries(); len(qs) != 1 || !strings.Contains(qs[0], "WHERE intent_id = $1") {
		t.Fatalf("intent reader queries = %q", qs)
	}

	missing, err := readBindingByIntentTx(context.Background(), &fakeQuerier{}, "intent-x")
	if err != nil || missing != nil {
		t.Fatalf("absent binding = (%v, %v), want (nil, nil)", missing, err)
	}
}

// TestBindingVerifyRebuild pins the read-only rebuild verification (R5): all
// named carriers must be present and every scope/binding/hold row consistent.
// It issues zero writes; a short carrier set fails closed.
func TestBindingVerifyRebuild(t *testing.T) {
	t.Run("consistent", func(t *testing.T) {
		f := &fakeQuerier{handler: func(sql string, _ []any) pgx.Row {
			switch {
			case strings.Contains(sql, "pg_constraint"):
				return rows(int64(len(requiredConstraintNames)))
			case strings.Contains(sql, "nonce_scope_state WHERE"):
				return rows(int64(2))
			case strings.Contains(sql, "nonce_bindings WHERE"):
				return rows(int64(3))
			case strings.Contains(sql, "nonce_scope_holds WHERE"):
				return rows(int64(0))
			default: // integrity probes: zero inconsistent rows
				return rows(int64(0))
			}
		}}
		rep, err := VerifyRebuild(context.Background(), f, 31337)
		if err != nil {
			t.Fatalf("VerifyRebuild error = %v", err)
		}
		if rep.ConstraintsFound != int64(len(requiredConstraintNames)) || rep.Scopes != 2 || rep.Bindings != 3 {
			t.Fatalf("report = %+v", rep)
		}
		if got := f.execCount(); got != 0 {
			t.Fatalf("VerifyRebuild issued %d writes, want 0", got)
		}
	})

	t.Run("missing carriers fails closed", func(t *testing.T) {
		f := &fakeQuerier{handler: func(sql string, _ []any) pgx.Row {
			if strings.Contains(sql, "pg_constraint") {
				return rows(int64(len(requiredConstraintNames) - 1))
			}
			return rows(int64(0))
		}}
		if _, err := VerifyRebuild(context.Background(), f, 31337); err == nil ||
			!strings.Contains(err.Error(), "named carriers present") {
			t.Fatalf("VerifyRebuild error = %v, want missing-carrier refusal", err)
		}
	})

	t.Run("inconsistent rows fails closed", func(t *testing.T) {
		f := &fakeQuerier{handler: func(sql string, _ []any) pgx.Row {
			switch {
			case strings.Contains(sql, "pg_constraint"):
				return rows(int64(len(requiredConstraintNames)))
			case strings.Contains(sql, "state NOT IN"):
				return rows(int64(1))
			default:
				return rows(int64(0))
			}
		}}
		if _, err := VerifyRebuild(context.Background(), f, 31337); err == nil ||
			!strings.Contains(err.Error(), "bindings are constraint-inconsistent") {
			t.Fatalf("VerifyRebuild error = %v, want integrity refusal", err)
		}
	})
}
