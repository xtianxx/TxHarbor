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
	"sync"
	"sync/atomic"
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

// TestNonceReconcileDivergenceNeverTransitions proves a divergent observation
// (contradictory L > P, and regressing P < P_prev) is treated exactly like an
// unavailable read by the transition applier: a binding whose nonce is below
// the contradictory latest is NEVER consumed, and the regressing pending view
// never moves an allocated binding to in_flight. The classification still
// records its anomaly observation, and the full reconcile path still
// establishes the chain_view_divergence hold (the ambiguity is held, not
// silently resolved) — observation.md §2/§2.1, classify.go.
func TestNonceReconcileDivergenceNeverTransitions(t *testing.T) {
	ctx, pool := convergeSetup(t)
	recSeedScope(t, ctx, pool)
	// nonce 4 is below the contradictory L=10, so without the guard the
	// applier consumes it; under the regressing view (L=3,P=5) it would flip
	// to in_flight.
	recSeedBinding(t, ctx, pool, "rec-div-consumed", 4, StateAllocated)
	// nonce 3 exercises the regressing [L,P) window (L=3,P=5) that would flip
	// an allocated binding to in_flight without the guard.
	recSeedBinding(t, ctx, pool, "rec-div-regress", 3, StateAllocated)
	// A durable last_pending of 9 makes a fresh pending of 5 a regression.
	recMustExec(t, ctx, pool, `INSERT INTO nonce_scope_state
		(chain_id, sender, reconciled_floor, last_latest, last_pending)
		VALUES ($1, $2, NULL, 5::numeric, 9::numeric)`, recChainID, recSender)

	// (a) Contradictory view L > P: observe 10/4 (P < durable 9 too).
	contradictory := recPersistObservation(t, ctx, pool, 10, 4, ClassificationDivergence)
	recApplyAll(t, ctx, pool, contradictory)
	// (b) Regressing view L <= P: observe 3/5 with durable last_pending 9.
	regressing := recPersistObservation(t, ctx, pool, 3, 5, ClassificationDivergence)
	recApplyAll(t, ctx, pool, regressing)

	for _, id := range []string{"rec-div-consumed", "rec-div-regress"} {
		if got := recBindingState(t, ctx, pool, id); got != StateAllocated {
			t.Errorf("divergent observation moved %s to %q, want untouched %q", id, got, StateAllocated)
		}
		if got := recEventTargets(t, ctx, pool, id); len(got) != 0 {
			t.Errorf("divergent observation wrote events for %s: %v", id, got)
		}
	}

	// The anomaly evidence is still persisted (never dropped), and the full
	// reconcile path still establishes the divergence hold while admitting
	// nothing.
	if n := recCount(t, ctx, pool, `SELECT count(*) FROM nonce_observations
		WHERE chain_id = $1 AND sender = $2 AND classification = 'divergence'`,
		recChainID, recSender); n != 2 {
		t.Fatalf("divergence observations = %d, want the 2 persisted anomalies", n)
	}
	fullReconcile(t, ctx, pool, 10, 4)
	if got := recBindingState(t, ctx, pool, "rec-div-consumed"); got != StateAllocated {
		t.Fatalf("full reconcile consumed from a divergent view: state = %q", got)
	}
	if n := recCount(t, ctx, pool, `SELECT count(*) FROM nonce_scope_holds
		WHERE chain_id = $1 AND sender = $2 AND cause = 'chain_view_divergence' AND status = 'active'`,
		recChainID, recSender); n != 1 {
		t.Fatalf("divergence holds = %d, want exactly 1 established by the full path", n)
	}
	if n := recCount(t, ctx, pool, `SELECT count(*) FROM nonce_bindings WHERE state = 'consumed'`); n != 0 {
		t.Fatalf("divergent observations produced %d consumed bindings, want 0", n)
	}
}

// recWaterline reads the durable last_latest/last_pending waterline.
func recWaterline(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (string, string) {
	t.Helper()
	var latest, pending string
	if err := pool.QueryRow(ctx, `SELECT last_latest::text, last_pending::text
		FROM nonce_scope_state WHERE chain_id = $1 AND sender = $2`,
		recChainID, recSender).Scan(&latest, &pending); err != nil {
		t.Fatalf("read scope waterline: %v", err)
	}
	return latest, pending
}

// fullReconcileUnavailable drives reconcileScopeInTx with an unavailable read
// (nil counts), the shape a failed RPC observation takes on the tick path.
func fullReconcileUnavailable(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin unavailable reconcile tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	obs := Observation{
		ChainID:        recChainID,
		Sender:         recSender,
		Kind:           ObservationKindReconcile,
		Classification: ClassificationUnavailable,
		HeadNumber:     big.NewInt(1),
		HeadHash:       "0x" + strings.Repeat("cd", 32),
	}
	if err := reconcileScopeInTx(ctx, tx, recChainID, recSender, obs); err != nil {
		t.Fatalf("reconcileScopeInTx unavailable: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit unavailable reconcile tx: %v", err)
	}
}

// TestNonceReconcileAnomalyNeverPoisonsWaterline proves the tick path treats
// the waterline as trusted-only, exactly like the admission path: a
// contradictory view (L > P), a regressing view (P < trusted P_prev), and an
// unavailable read (nil counts) never overwrite last_latest/last_pending, so
// one anomaly cannot move the PendingPrev baseline and mask a later real
// regression. Anomaly observations and the divergence hold still persist —
// skipping the waterline drops no evidence.
func TestNonceReconcileAnomalyNeverPoisonsWaterline(t *testing.T) {
	ctx, pool := convergeSetup(t)
	recSeedScope(t, ctx, pool)
	// nonce 12 sits above every view below: no transition may ever touch it.
	recSeedBinding(t, ctx, pool, "rec-wl-keep", 12, StateAllocated)
	wantWater := func(step, latest, pending string) {
		t.Helper()
		if l, p := recWaterline(t, ctx, pool); l != latest || p != pending {
			t.Fatalf("waterline after %s = (%s, %s), want trusted (%s, %s)", step, l, p, latest, pending)
		}
	}

	// (a) Trusted view 9/9: consistent, advances the waterline to 9/9.
	fullReconcile(t, ctx, pool, 9, 9)
	wantWater("trusted 9/9", "9", "9")

	// (b) Contradictory view 10/4: divergence — anomaly + hold persist, the
	// waterline stays at the trusted 9/9.
	fullReconcile(t, ctx, pool, 10, 4)
	wantWater("divergent 10/4", "9", "9")

	// (c) The masked-alarm scenario: a consistent-shaped view 3/5 against the
	// trusted P=9 must still read as a regression (divergence), which is only
	// possible because (b) did not move the baseline to 4. The waterline
	// stays 9/9 and the active divergence hold is reused, not duplicated.
	fullReconcile(t, ctx, pool, 3, 5)
	wantWater("regressing 3/5", "9", "9")
	if n := recCount(t, ctx, pool, `SELECT count(*) FROM nonce_observations
		WHERE chain_id = $1 AND sender = $2 AND classification = 'divergence'`,
		recChainID, recSender); n != 2 {
		t.Fatalf("divergence observations = %d, want exactly the 2 anomalies", n)
	}
	if n := recCount(t, ctx, pool, `SELECT count(*) FROM nonce_scope_holds
		WHERE chain_id = $1 AND sender = $2 AND cause = 'chain_view_divergence' AND status = 'active'`,
		recChainID, recSender); n != 1 {
		t.Fatalf("active divergence holds = %d, want exactly 1 (re-detected cause converges)", n)
	}

	// (d) Unavailable read: observation persists, waterline intact, no hold change.
	fullReconcileUnavailable(t, ctx, pool)
	wantWater("unavailable", "9", "9")
	if n := recCount(t, ctx, pool, `SELECT count(*) FROM nonce_observations
		WHERE chain_id = $1 AND sender = $2 AND classification = 'unavailable'`,
		recChainID, recSender); n != 1 {
		t.Fatalf("unavailable observations = %d, want 1", n)
	}
	if n := recCount(t, ctx, pool, `SELECT count(*) FROM nonce_scope_holds
		WHERE chain_id = $1 AND sender = $2 AND status = 'active'`,
		recChainID, recSender); n != 1 {
		t.Fatalf("active holds after unavailable = %d, want still exactly 1", n)
	}

	// No anomaly ever terminally touched the binding.
	if got := recBindingState(t, ctx, pool, "rec-wl-keep"); got != StateAllocated {
		t.Fatalf("anomaly path moved rec-wl-keep to %q, want untouched allocated", got)
	}
	if got := recEventTargets(t, ctx, pool, "rec-wl-keep"); len(got) != 0 {
		t.Fatalf("anomaly path wrote events for rec-wl-keep: %v", got)
	}
	if n := recCount(t, ctx, pool, `SELECT count(*) FROM nonce_bindings WHERE state = 'consumed'`); n != 0 {
		t.Fatalf("anomaly path produced %d consumed bindings, want 0", n)
	}
}

// fullReconcile drives reconcileScopeInTx end to end for the seeded scope with
// the given chain view, so the hold/anomaly behavior of the real T-observe path
// is exercised (not just the applier).
func fullReconcile(t *testing.T, ctx context.Context, pool *pgxpool.Pool, latest, pending int64) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin full reconcile tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	obs := Observation{
		ChainID:      recChainID,
		Sender:       recSender,
		Kind:         ObservationKindReconcile,
		LatestCount:  big.NewInt(latest),
		PendingCount: big.NewInt(pending),
		HeadNumber:   big.NewInt(1),
		HeadHash:     "0x" + strings.Repeat("cd", 32),
	}
	if err := reconcileScopeInTx(ctx, tx, recChainID, recSender, obs); err != nil {
		t.Fatalf("reconcileScopeInTx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit full reconcile tx: %v", err)
	}
}

// recSink is a fake ReconcileSink: it counts failure ticks with no labels at
// all, so the test proves a failing scope can never become a metric label.
type recSink struct {
	mu sync.Mutex
	n  int
}

func (s *recSink) ObserveNonceReconcileFailure() {
	s.mu.Lock()
	s.n++
	s.mu.Unlock()
}

func (s *recSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

// TestNonceReconcileLoopBacksOffFailingScope proves P1-6's bound: a scope that
// keeps failing is retried under a bounded per-sender backoff instead of every
// tick, a healthy scope in the same pass keeps converging at the cadence (no
// starvation), each failure is counted through the low-cardinality sink with a
// fixed classification (never a sender label), and cancellation joins cleanly.
func TestNonceReconcileLoopBacksOffFailingScope(t *testing.T) {
	ctx, pool := convergeSetup(t)
	recSeedScope(t, ctx, pool) // the healthy sender, recSender
	const failSender = "0x9999999999999999999999999999999999999999"
	recMustExec(t, ctx, pool, `INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		VALUES ($1, $2, 'active', 1)`, recChainID, failSender)

	var healthy, failing atomic.Int64
	sink := &recSink{}
	loop := NewReconcileLoop(pool, nil, recChainID, 20*time.Millisecond, discardLog(), sink)
	// The scope list and the sink are real; only the per-scope work is scripted
	// so exactly one sender fails every time it is actually attempted.
	loop.reconcile = func(_ context.Context, sender string) error {
		if sender == failSender {
			failing.Add(1)
			return errors.New("persistent scope failure")
		}
		healthy.Add(1)
		return nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); loop.Run(runCtx) }()

	reconcileWaitFor(t, 3*time.Second, func() bool { return healthy.Load() >= 8 })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation (ctx.Done not respected)")
	}
	stoppedHealthy, stoppedFailing := healthy.Load(), failing.Load()

	// Backoff, not a per-tick tight retry: the failing scope was attempted
	// strictly fewer times than the healthy one over the same passes. Without
	// the bound both counters advance once per pass.
	if stoppedFailing >= stoppedHealthy {
		t.Fatalf("failing scope attempted %d times vs healthy %d: no backoff, tight per-tick retry",
			stoppedFailing, stoppedHealthy)
	}

	if n := sink.count(); n != int(stoppedFailing) {
		t.Fatalf("failure counter = %d, want exactly the %d failed attempts", n, stoppedFailing)
	}

	// No further work after cancel.
	time.Sleep(4 * 20 * time.Millisecond)
	if healthy.Load() != stoppedHealthy || failing.Load() != stoppedFailing {
		t.Fatalf("loop kept working after cancel: healthy %d->%d failing %d->%d",
			stoppedHealthy, healthy.Load(), stoppedFailing, failing.Load())
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
