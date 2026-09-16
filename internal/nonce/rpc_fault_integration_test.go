//go:build integration

// rpc_fault_integration_test.go owns spec task T030 for 008-nonce-manager:
// RPC fault injection during admission AND reconcile over a real PostgreSQL
// (testcontainers). It mirrors the 006 transport-level JSON-RPC proxy pattern
// (internal/indexer/logscan_integration_test.go): an httptest endpoint scripts
// the failure while the durable assertions are raw-SQL probes.
//
// Four faults are injected on the observation reads for one (chain_id, sender)
// scope; each must leave the observation `unavailable` carrying the eth error
// class observe.go derives, with zero domain-state change:
//
//   - transport        HTTP 500                -> error_class transport
//   - timeout          reply stalls past the   -> error_class timeout
//     RPCTimeout deadline
//   - rate-limit       HTTP 429                -> error_class rate_limited
//   - conflicting-view JSON-RPC server error   -> error_class invalid_response
//
// The §2.1 stale-observation rule is asserted structurally for both paths: the
// reconciled floor is never lowered, an active hold is never released (and no
// hold is created for an unavailable read), and no candidate is re-derived
// (the scope keeps exactly its seeded binding).
//
// The observer's block type is package-private, so the JSON double answers
// eth_getBlockByNumber with a JSON object the exported Observer unmarshals
// itself; only exported Observer / Allocator / ReconcileLoop surfaces are
// driven (package nonce_test).
package nonce_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

// fltChainBase offsets each subtest onto its own chain so a reconcile loop
// observes exactly the scope under test.
const fltChainBase = int64(9500)

// fltFaultKind selects the injected failure.
type fltFaultKind string

const (
	fltTransport       fltFaultKind = "transport"
	fltTimeout         fltFaultKind = "timeout"
	fltRateLimit       fltFaultKind = "rate-limit"
	fltConflictingView fltFaultKind = "conflicting-view"
)

// fltFaults is the injected set, in order.
var fltFaults = []fltFaultKind{fltTransport, fltTimeout, fltRateLimit, fltConflictingView}

// fltFaultClasses is the eth error class observe.go's errorClassOf must persist
// for each fault (a JSON-RPC server error is the invalid-response class: the
// endpoint returns a view that conflicts with a usable result).
var fltFaultClasses = map[fltFaultKind]string{
	fltTransport:       "transport",
	fltTimeout:         "timeout",
	fltRateLimit:       "rate_limited",
	fltConflictingView: "invalid_response",
}

// fltSender mints a distinct lowercase 0x + 40 hex address per index.
func fltSender(n int) string { return fmt.Sprintf("0x%040x", n) }

// fltSetup boots one isolated PostgreSQL container, applies the embedded
// migrations, and returns the context, a raw-SQL handle, and the pgx pool.
func fltSetup(t *testing.T) (context.Context, *sql.DB, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	dsn := nonceStartPostgres(t)
	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)
	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("OpenPool: %v", err)
	}
	t.Cleanup(pool.Close)
	return ctx, sqlDB, pool
}

// fltSeedCaller registers the single 007 caller the authorizations hang off.
func fltSeedCaller(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES (1, 'flt')`)
}

// fltSeedScope registers an active sender and one active 007 authorization so
// admission reaches the observation/classification step.
func fltSeedScope(t *testing.T, sqlDB *sql.DB, chainID int64, sender, authID string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		VALUES ($1, $2, 'active', 1)`, chainID, sender)
	nonceMustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1, 1, $2, $3, $3, 1, 'active')`, authID, chainID, sender)
}

// fltSeedBinding writes one durable binding at nonce 5 (the seed the fault must
// never move, release, or duplicate).
func fltSeedBinding(t *testing.T, sqlDB *sql.DB, chainID int64, sender, bindingID, intentID string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_bindings
		(binding_id, intent_id, chain_id, sender, nonce, state, authorization_id,
		 authorization_version, registry_seq, allocation_observation_id)
		VALUES ($1, $2, $3, $4, 5::numeric, 'allocated', 'flt-seed-auth', $5, 1, 'no-flt-seed')`,
		bindingID, intentID, chainID, sender, strings.Repeat("a", 64))
}

// fltSeedFrontier writes the durable scope frontier: floor 5, last_pending 9.
func fltSeedFrontier(t *testing.T, sqlDB *sql.DB, chainID int64, sender string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_scope_state
		(chain_id, sender, reconciled_floor, last_latest, last_pending)
		VALUES ($1, $2, 5::numeric, 5::numeric, 9::numeric)`, chainID, sender)
}

// fltSeedHold plants an active hold the reconcile fault must never release.
func fltSeedHold(t *testing.T, sqlDB *sql.DB, chainID int64, sender, holdID string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_scope_holds
		(hold_id, chain_id, sender, cause, status, evidence_observation_id, evidence_detail)
		VALUES ($1, $2, $3, 'chain_view_divergence', 'active', 'no-flt-seed-hold', 'seed')`,
		holdID, chainID, sender)
}

// fltFaultClient serves the three R4 observation methods as a transport-level
// JSON-RPC double that injects the configured fault instead of a usable view.
func fltFaultClient(t *testing.T, fault fltFaultKind) *gethrpc.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "malformed JSON-RPC request", http.StatusBadRequest)
			return
		}
		switch fault {
		case fltTimeout:
			// Stall past the observer's RPCTimeout; the client cancellation
			// ends the handler if it fires first.
			select {
			case <-time.After(300 * time.Millisecond):
			case <-r.Context().Done():
				return
			}
		case fltTransport:
			http.Error(w, "injected transport fault", http.StatusInternalServerError)
			return
		case fltRateLimit:
			http.Error(w, "injected rate limit", http.StatusTooManyRequests)
			return
		case fltConflictingView:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      json.RawMessage(req.ID),
				"error":   map[string]any{"code": -32000, "message": "conflicting view"},
			})
			return
		}
		http.Error(w, "unscripted fault "+string(fault), http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	client, err := gethrpc.Dial(srv.URL)
	if err != nil {
		t.Fatalf("dial fault JSON-RPC: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

// fltObserver wires the exported Observer with a short RPCTimeout so the
// timeout fault stays fast.
func fltObserver(client *gethrpc.Client) *nonce.Observer {
	return nonce.NewObserver(client, nonce.ObserverConfig{
		RPCTimeout:   40 * time.Millisecond,
		RetryInitial: time.Millisecond,
		RetryMax:     3 * time.Millisecond,
	})
}

// fltObs is one persisted observation's evidence fields.
type fltObs struct{ classification, errorClass string }

// fltObservations reads the scope's observation evidence for one kind.
func fltObservations(t *testing.T, sqlDB *sql.DB, chainID int64, sender, kind string) []fltObs {
	t.Helper()
	rows, err := sqlDB.QueryContext(context.Background(),
		`SELECT classification, error_class FROM nonce_observations
		 WHERE chain_id = $1 AND sender = $2 AND kind = $3 ORDER BY observed_at`,
		chainID, sender, kind)
	if err != nil {
		t.Fatalf("read observations: %v", err)
	}
	defer rows.Close()
	var out []fltObs
	for rows.Next() {
		var o fltObs
		if err := rows.Scan(&o.classification, &o.errorClass); err != nil {
			t.Fatalf("scan observation: %v", err)
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate observations: %v", err)
	}
	return out
}

// fltCount runs one scalar count query.
func fltCount(t *testing.T, sqlDB *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := sqlDB.QueryRowContext(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

// fltAssertUnchanged probes every durable surface a fault must not touch:
// exactly the one seeded binding (state + nonce, so no candidate was
// re-derived), the seeded floor/last_pending (never lowered or moved), the
// expected active-hold count (never released, never created), and zero
// transition events.
func fltAssertUnchanged(t *testing.T, sqlDB *sql.DB, chainID int64, sender, bindingID, wantState string, wantActiveHolds int) {
	t.Helper()
	if n := fltCount(t, sqlDB,
		`SELECT count(*) FROM nonce_bindings WHERE chain_id = $1 AND sender = $2`, chainID, sender); n != 1 {
		t.Fatalf("scope bindings = %d, want exactly the seeded 1 (no candidate re-derived)", n)
	}
	var state string
	var nonceVal int64
	if err := sqlDB.QueryRowContext(context.Background(),
		`SELECT state, nonce::bigint FROM nonce_bindings WHERE binding_id = $1`, bindingID).
		Scan(&state, &nonceVal); err != nil {
		t.Fatalf("read seeded binding: %v", err)
	}
	if state != wantState || nonceVal != 5 {
		t.Fatalf("seeded binding = (%s,%d), want (%s,5) untouched", state, nonceVal, wantState)
	}
	var floor, lastPending int64
	if err := sqlDB.QueryRowContext(context.Background(),
		`SELECT reconciled_floor::bigint, last_pending::bigint FROM nonce_scope_state
		 WHERE chain_id = $1 AND sender = $2`, chainID, sender).Scan(&floor, &lastPending); err != nil {
		t.Fatalf("read scope frontier: %v", err)
	}
	if floor != 5 || lastPending != 9 {
		t.Fatalf("scope frontier = (floor %d, last_pending %d), want (5,9) unchanged", floor, lastPending)
	}
	if n := fltCount(t, sqlDB,
		`SELECT count(*) FROM nonce_scope_holds WHERE chain_id = $1 AND sender = $2 AND status = 'active'`,
		chainID, sender); n != wantActiveHolds {
		t.Fatalf("active holds = %d, want %d (never released/created)", n, wantActiveHolds)
	}
	if n := fltCount(t, sqlDB,
		`SELECT count(*) FROM nonce_binding_events WHERE binding_id = $1`, bindingID); n != 0 {
		t.Fatalf("binding events = %d, want 0 (zero domain transitions)", n)
	}
}

// TestRPCFaultAdmissionPersistsUnavailableWithoutDomainChange injects each
// fault into a fresh admission and asserts the observation is `unavailable`
// with the eth error class, the admission refuses chain_view_unavailable, and
// no domain state moved.
func TestRPCFaultAdmissionPersistsUnavailableWithoutDomainChange(t *testing.T) {
	ctx, sqlDB, pool := fltSetup(t)
	fltSeedCaller(t, sqlDB)

	for i, fault := range fltFaults {
		t.Run(string(fault), func(t *testing.T) {
			chainID := fltChainBase + int64(i)
			sender := fltSender(100 + i)
			authID := fmt.Sprintf("flt-auth-%d", i)
			seededBinding := fmt.Sprintf("flt-seed-b-%d", i)

			fltSeedScope(t, sqlDB, chainID, sender, authID)
			fltSeedBinding(t, sqlDB, chainID, sender, seededBinding, fmt.Sprintf("flt-seed-intent-%d", i))
			fltSeedFrontier(t, sqlDB, chainID, sender)

			allocator := nonce.NewAllocator(pool, fltObserver(fltFaultClient(t, fault)))

			_, outcome, err := allocator.Allocate(ctx, nonce.AllocationRequest{
				IntentID:        fmt.Sprintf("flt-intent-%d", i),
				ChainID:         chainID,
				Sender:          sender,
				AuthorizationID: authID,
			})
			if err == nil || outcome != nonce.OutcomeChainViewUnavailable {
				t.Fatalf("outcome=%q err=%v, want %q", outcome, err, nonce.OutcomeChainViewUnavailable)
			}

			obs := fltObservations(t, sqlDB, chainID, sender, "allocation")
			if len(obs) != 1 {
				t.Fatalf("allocation observations = %d, want exactly 1", len(obs))
			}
			if obs[0].classification != nonce.ClassificationUnavailable || obs[0].errorClass != fltFaultClasses[fault] {
				t.Fatalf("observation = %+v, want unavailable/%s", obs[0], fltFaultClasses[fault])
			}
			fltAssertUnchanged(t, sqlDB, chainID, sender, seededBinding, "allocated", 0)
		})
	}
}

// TestRPCFaultReconcilePersistsUnavailableWithoutDomainChange injects each
// fault into the periodic reconcile observer and asserts the loop persists
// `unavailable` observations with the eth error class and touches nothing:
// the seeded hold stays active, the floor stays put, and the binding is
// neither moved nor released.
func TestRPCFaultReconcilePersistsUnavailableWithoutDomainChange(t *testing.T) {
	ctx, sqlDB, pool := fltSetup(t)
	fltSeedCaller(t, sqlDB)

	for i, fault := range fltFaults {
		t.Run(string(fault), func(t *testing.T) {
			chainID := fltChainBase + int64(10+i)
			sender := fltSender(200 + i)
			seededBinding := fmt.Sprintf("flt-rec-seed-b-%d", i)

			fltSeedScope(t, sqlDB, chainID, sender, fmt.Sprintf("flt-rec-auth-%d", i))
			fltSeedBinding(t, sqlDB, chainID, sender, seededBinding, fmt.Sprintf("flt-rec-seed-intent-%d", i))
			fltSeedFrontier(t, sqlDB, chainID, sender)
			fltSeedHold(t, sqlDB, chainID, sender, fmt.Sprintf("flt-rec-hold-%d", i))

			loop := nonce.NewReconcileLoop(pool, fltObserver(fltFaultClient(t, fault)), chainID, 25*time.Millisecond, nil)

			runCtx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			go func() {
				defer close(done)
				loop.Run(runCtx)
			}()
			fltWaitForObservation(t, sqlDB, chainID, sender)
			cancel()
			<-done

			obs := fltObservations(t, sqlDB, chainID, sender, "reconcile")
			if len(obs) == 0 {
				t.Fatal("reconcile persisted no observation")
			}
			for _, o := range obs {
				if o.classification != nonce.ClassificationUnavailable || o.errorClass != fltFaultClasses[fault] {
					t.Fatalf("reconcile observation = %+v, want unavailable/%s", o, fltFaultClasses[fault])
				}
			}
			fltAssertUnchanged(t, sqlDB, chainID, sender, seededBinding, "allocated", 1)
		})
	}
}

// fltWaitForObservation polls until the scope has at least one reconcile
// observation, failing after 3s.
func fltWaitForObservation(t *testing.T, sqlDB *sql.DB, chainID int64, sender string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fltCount(t, sqlDB,
			`SELECT count(*) FROM nonce_observations WHERE chain_id = $1 AND sender = $2 AND kind = 'reconcile'`,
			chainID, sender) > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("reconcile persisted no observation for %s within 3s", sender)
}
