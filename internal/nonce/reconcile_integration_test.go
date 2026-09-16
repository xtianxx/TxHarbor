//go:build integration

// reconcile_integration_test.go owns spec task T026 for 008-nonce-manager: the
// T-observe binding transition applier (R6, FR-07/09/20) over a real
// PostgreSQL. It drives the applier from persisted nonce_observations rows
// (seeded through observe.go's own writer, exactly the shape the observer
// persists) and proves:
//
//   - allocated -> in_flight for nonce in [L,P): exactly one event;
//   - allocated|in_flight -> consumed for nonce < L: exactly one event;
//   - a repeat observation appends zero further events, converging on
//     nonce_binding_events_binding_to_uniq (including the crash window where
//     the event committed but the state advance did not);
//   - no automatic transition ever produces released, even for stale/odd
//     (unavailable-with-counts, no-op window, terminal) observations.
//
// No Anvil is needed: the chain view is seeded evidence, not an RPC call.
//
// The file is package nonce (not nonce_test) because the applier and the
// observation writer are package-private, and it reuses convergeSetup — the
// isolated testcontainers PostgreSQL + MigrateUp harness the migration/rebuild
// integration tests established (skips, never passes, without a Docker
// provider).
package nonce

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	recChainID = int64(8080)
	recSender  = "0x8888888888888888888888888888888888888888"
	recAuth    = "wa-reconcile"
	// recHash is a 64-lowercase-hex authorization_version digest.
	recHash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// recSeedScope registers one active sender (the nonce_bindings registry FK).
func recSeedScope(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		VALUES ($1, $2, 'active', 1)`, recChainID, recSender); err != nil {
		t.Fatalf("seed registry: %v", err)
	}
}

// recSeedBinding writes one durable binding (consumed_at set for a terminal
// consumed row, satisfying nonce_bindings_terminal_consistency).
func recSeedBinding(t *testing.T, ctx context.Context, pool *pgxpool.Pool, bindingID string, nonce int64, state string) {
	t.Helper()
	var consumedAt any
	if state == StateConsumed {
		consumedAt = time.Now().UTC()
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nonce_bindings
		(binding_id, intent_id, chain_id, sender, nonce, state, authorization_id,
		 authorization_version, registry_seq, allocation_observation_id, consumed_at)
		VALUES ($1, $2, $3, $4, $5::numeric, $6, $7, $8, 1, 'no-rec-seed', $9)`,
		bindingID, "intent-"+bindingID, recChainID, recSender, nonce, state, recAuth, recHash, consumedAt); err != nil {
		t.Fatalf("seed binding %s/%s: %v", bindingID, state, err)
	}
}

// recPersistObservation persists one reconcile observation through observe.go's
// writer and returns it with its durable id, so the applier is driven from a
// real nonce_observations row.
func recPersistObservation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, latest, pending int64, classification string) Observation {
	t.Helper()
	obs := Observation{
		ChainID:        recChainID,
		Sender:         recSender,
		Kind:           ObservationKindReconcile,
		Classification: classification,
		LatestCount:    big.NewInt(latest),
		PendingCount:   big.NewInt(pending),
		HeadNumber:     big.NewInt(1),
		HeadHash:       "0x" + strings.Repeat("cd", 32),
	}
	id, err := insertObservationTx(ctx, pool, obs)
	if err != nil {
		t.Fatalf("persist observation L=%d P=%d: %v", latest, pending, err)
	}
	obs.ObservationID = id
	return obs
}

// recApplyAll runs one T-observe tick shape in a transaction: read the scope's
// nontrivial bindings, apply the applier to each, commit.
func recApplyAll(t *testing.T, ctx context.Context, pool *pgxpool.Pool, obs Observation) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin reconcile tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	bindings, err := readBindingsByScopeTx(ctx, tx, recChainID, recSender)
	if err != nil {
		t.Fatalf("read scope bindings: %v", err)
	}
	for i := range bindings {
		if _, err := applyObservationTransitionTx(ctx, tx, bindings[i], obs); err != nil {
			t.Fatalf("apply transition for %s: %v", bindings[i].BindingID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit reconcile tx: %v", err)
	}
}

// recBindingState reads the durable state of one binding.
func recBindingState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, bindingID string) string {
	t.Helper()
	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM nonce_bindings WHERE binding_id = $1`, bindingID).Scan(&state); err != nil {
		t.Fatalf("read state of %s: %v", bindingID, err)
	}
	return state
}

// recEventTargets lists one binding's transition events in append order.
func recEventTargets(t *testing.T, ctx context.Context, pool *pgxpool.Pool, bindingID string) []string {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT to_state FROM nonce_binding_events WHERE binding_id = $1 ORDER BY event_id`, bindingID)
	if err != nil {
		t.Fatalf("query events for %s: %v", bindingID, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var state string
		if err := rows.Scan(&state); err != nil {
			t.Fatalf("scan event for %s: %v", bindingID, err)
		}
		out = append(out, state)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate events for %s: %v", bindingID, err)
	}
	return out
}

// TestNonceReconcileTransitionsAppendOneEvent proves the two automatic edges
// (allocated->in_flight for nonce in [L,P); allocated|in_flight->consumed for
// nonce < L) each append exactly one event, from one persisted observation.
func TestNonceReconcileTransitionsAppendOneEvent(t *testing.T) {
	ctx, pool := convergeSetup(t)
	recSeedScope(t, ctx, pool)
	recSeedBinding(t, ctx, pool, "rec-b1", 1, StateAllocated) // nonce < L -> consumed
	recSeedBinding(t, ctx, pool, "rec-b2", 3, StateAllocated) // L <= nonce < P -> in_flight
	recSeedBinding(t, ctx, pool, "rec-b3", 5, StateInFlight)  // nonce >= P -> untouched
	recSeedBinding(t, ctx, pool, "rec-b4", 0, StateInFlight)  // nonce < L -> consumed

	obs := recPersistObservation(t, ctx, pool, 2, 4, ClassificationConsistent) // L=2, P=4
	recApplyAll(t, ctx, pool, obs)

	wantStates := map[string]string{
		"rec-b1": StateConsumed,
		"rec-b2": StateInFlight,
		"rec-b3": StateInFlight,
		"rec-b4": StateConsumed,
	}
	for id, want := range wantStates {
		if got := recBindingState(t, ctx, pool, id); got != want {
			t.Errorf("binding %s state = %q, want %q", id, got, want)
		}
	}

	wantEvents := map[string][]string{
		"rec-b1": {StateConsumed},
		"rec-b2": {StateInFlight},
		"rec-b3": nil,
		"rec-b4": {StateConsumed},
	}
	for id, want := range wantEvents {
		got := recEventTargets(t, ctx, pool, id)
		if len(got) != len(want) {
			t.Errorf("binding %s events = %v, want exactly %v", id, got, want)
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("binding %s event[%d] = %q, want %q", id, i, got[i], want[i])
			}
		}
	}

	// The evidence link is the persisted observation id.
	var linked string
	if err := pool.QueryRow(ctx,
		`SELECT observation_id FROM nonce_binding_events WHERE binding_id = 'rec-b2'`).Scan(&linked); err != nil {
		t.Fatalf("read event evidence link: %v", err)
	}
	if linked != obs.ObservationID {
		t.Fatalf("event observation_id = %q, want the persisted %q", linked, obs.ObservationID)
	}

	// No automatic path produced the terminal operator state.
	if n := recCount(t, ctx, pool, `SELECT count(*) FROM nonce_bindings WHERE state = 'released'`); n != 0 {
		t.Fatalf("reconcile produced %d released bindings, want 0", n)
	}
}

// TestNonceReconcileRepeatObservationConverges proves a repeated identical
// observation appends zero further events and that a crash window (event
// committed, state advance not) heals without duplicating history — the
// convergence is on nonce_binding_events_binding_to_uniq.
func TestNonceReconcileRepeatObservationConverges(t *testing.T) {
	ctx, pool := convergeSetup(t)
	recSeedScope(t, ctx, pool)
	recSeedBinding(t, ctx, pool, "rec-rep", 7, StateAllocated)
	obs := recPersistObservation(t, ctx, pool, 0, 10, ClassificationConsistent) // 7 in [0,10) -> in_flight

	recApplyAll(t, ctx, pool, obs)
	if got := recBindingState(t, ctx, pool, "rec-rep"); got != StateInFlight {
		t.Fatalf("first apply state = %q, want %q", got, StateInFlight)
	}
	if got := recEventTargets(t, ctx, pool, "rec-rep"); len(got) != 1 {
		t.Fatalf("first apply events = %v, want exactly one", got)
	}

	// Repeat identical observation: already in_flight, so zero new events.
	recApplyAll(t, ctx, pool, obs)
	if got := recBindingState(t, ctx, pool, "rec-rep"); got != StateInFlight {
		t.Fatalf("repeat apply state = %q, want %q", got, StateInFlight)
	}
	if got := recEventTargets(t, ctx, pool, "rec-rep"); len(got) != 1 {
		t.Fatalf("repeat apply events = %v, want still exactly one", got)
	}

	// Crash window: the transition event committed but the state advance did
	// not. The applier re-runs, the event insert converges on the named UNIQUE,
	// and the state advance heals — still exactly one history row.
	recSeedBinding(t, ctx, pool, "rec-crash", 8, StateAllocated)
	recMustExec(t, ctx, pool, `INSERT INTO nonce_binding_events
		(binding_id, from_state, to_state, observation_id, detail)
		VALUES ('rec-crash', 'allocated', 'in_flight', $1, 'reconcile')`, obs.ObservationID)
	recApplyAll(t, ctx, pool, obs)
	if got := recBindingState(t, ctx, pool, "rec-crash"); got != StateInFlight {
		t.Fatalf("crash-window state = %q, want %q", got, StateInFlight)
	}
	if got := recEventTargets(t, ctx, pool, "rec-crash"); len(got) != 1 {
		t.Fatalf("crash-window events = %v, want exactly one (unique-carrier convergence)", got)
	}

	// The convergence carrier is the named UNIQUE: a duplicate transition
	// insert raises 23505 on exactly that constraint.
	_, err := pool.Exec(ctx,
		`INSERT INTO nonce_binding_events (binding_id, to_state) VALUES ('rec-crash', 'in_flight')`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" ||
		pgErr.ConstraintName != "nonce_binding_events_binding_to_uniq" {
		t.Fatalf("duplicate event error = %v, want 23505 on nonce_binding_events_binding_to_uniq", err)
	}
}

// TestNonceReconcileNeverProducesReleased proves no automatic transition ever
// yields released, even for stale/odd observations (unavailable-with-counts,
// no-op windows, a terminal binding).
func TestNonceReconcileNeverProducesReleased(t *testing.T) {
	// Structural: the automatic edge refuses released from every pending state
	// and offers no path out of the terminal states.
	if CanAutoTransition(StateAllocated, StateReleased) || CanAutoTransition(StateInFlight, StateReleased) {
		t.Fatal("automatic transition allows released")
	}
	if CanAutoTransition(StateConsumed, StateInFlight) || CanAutoTransition(StateReleased, StateConsumed) {
		t.Fatal("an automatic transition leaves a terminal state")
	}

	ctx, pool := convergeSetup(t)
	recSeedScope(t, ctx, pool)
	recSeedBinding(t, ctx, pool, "rec-odd", 4, StateAllocated)
	recSeedBinding(t, ctx, pool, "rec-term", 1, StateConsumed)

	// Unavailable-with-counts must not move the binding (an unavailable read is
	// never a value), and neither no-op window touches it.
	recApplyAll(t, ctx, pool, recPersistObservation(t, ctx, pool, 9, 9, ClassificationUnavailable))
	if got := recBindingState(t, ctx, pool, "rec-odd"); got != StateAllocated {
		t.Fatalf("unavailable observation moved the binding to %q", got)
	}
	recApplyAll(t, ctx, pool, recPersistObservation(t, ctx, pool, 0, 0, ClassificationConsistent))
	recApplyAll(t, ctx, pool, recPersistObservation(t, ctx, pool, 4, 4, ClassificationConsistent))
	if got := recBindingState(t, ctx, pool, "rec-odd"); got != StateAllocated {
		t.Fatalf("no-op observations moved the binding to %q", got)
	}
	if got := recEventTargets(t, ctx, pool, "rec-odd"); len(got) != 0 {
		t.Fatalf("stale observations wrote events: %v", got)
	}

	// A live window then a mined window drive the real edges; still no released.
	recApplyAll(t, ctx, pool, recPersistObservation(t, ctx, pool, 0, 5, ClassificationConsistent))
	if got := recBindingState(t, ctx, pool, "rec-odd"); got != StateInFlight {
		t.Fatalf("pending window state = %q, want %q", got, StateInFlight)
	}
	recApplyAll(t, ctx, pool, recPersistObservation(t, ctx, pool, 9, 9, ClassificationConsistent))
	if got := recBindingState(t, ctx, pool, "rec-odd"); got != StateConsumed {
		t.Fatalf("mined window state = %q, want %q", got, StateConsumed)
	}

	// The terminal binding was never touched by any of it.
	if got := recBindingState(t, ctx, pool, "rec-term"); got != StateConsumed {
		t.Fatalf("terminal binding state = %q, want untouched %q", got, StateConsumed)
	}
	if got := recEventTargets(t, ctx, pool, "rec-term"); len(got) != 0 {
		t.Fatalf("terminal binding gained events: %v", got)
	}

	if n := recCount(t, ctx, pool, `SELECT count(*) FROM nonce_bindings WHERE state = 'released'`); n != 0 {
		t.Fatalf("reconcile produced %d released bindings, want 0", n)
	}
	if n := recCount(t, ctx, pool, `SELECT count(*) FROM nonce_binding_events WHERE to_state = 'released'`); n != 0 {
		t.Fatalf("reconcile produced %d released events, want 0", n)
	}
}

// recMustExec fails the test on any raw exec error (crash-window seeding).
func recMustExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// recCount runs one scalar count query.
func recCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}
