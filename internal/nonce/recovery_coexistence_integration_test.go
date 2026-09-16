//go:build integration

// recovery_coexistence_integration_test.go owns spec task T033 for
// 008-nonce-manager (US5; FR-14/15/23; V7/V8; SC-07): the V8 006-pause
// precedence and independent-cause coexistence over a real PostgreSQL (no
// Anvil: the chain view is a scripted JSON-RPC double).
//
// What it proves:
//   - 006 precedence: a 006 recovery established before an admission refuses
//     that admission with the recorded 006 reason and zero domain writes;
//   - admission committed first: a 006 recovery established afterwards leaves
//     the committed binding byte-identical, and later admissions refuse;
//   - independent causes: 006 completion does NOT clear a 008 hold (the hold
//     row survives, still active, byte-identical);
//   - a 008 hold release does NOT clear a 006 pause (the pause rows survive
//     byte-identical);
//   - `reorg_recovery`/`indexer_pause`/`log_pause`/`deposit_pause` rows are
//     byte-identical across every 008 path above (snapshot before/after).
//
// All 006 rows are seeded and completed by raw SQL fixtures in the isolated
// DB; internal/indexer writers are never imported (008 only reads 006 state).
// Every helper this file owns is `co`-prefixed so it cannot collide with a
// sibling integration test.
package nonce_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

const (
	coChain = int64(80071)
	coAuth  = "wa-co-1"
)

// coHeadHash is the 0x + 64-hex head identity every scripted view reports.
var coHeadHash = "0x" + strings.Repeat("cd", 32)

// One dedicated sender scope per scenario keeps each case's bindings, scope
// row, holds and floor independent of the others.
var (
	coSenderPre   = nonceAddr("c1")
	coSenderPost  = nonceAddr("c2")
	coSenderHold  = nonceAddr("c3")
	coSenderPause = nonceAddr("c4")
)

// coRPCCaller is the raw JSON-RPC surface the Observer needs; the exported
// signature matches the unexported rpcCaller interface NewObserver accepts.
type coRPCCaller interface {
	CallContext(ctx context.Context, result any, method string, args ...any) error
}

// coRPC is a scripted chain view: distinct latest/pending counts (a divergence
// when latest > pending) and a fixed head identity. The values are JSON
// literals because the observer's block-result type is package-private.
type coRPC struct {
	latest  string
	pending string
}

func coHealthy() coRPC { return coRPC{latest: `"0x0"`, pending: `"0x0"`} }

// coDivergent is latest=2, pending=1: a contradictory view (L > P) that the
// matrix classifies as divergence and holds (CauseChainViewDivergence).
func coDivergent() coRPC { return coRPC{latest: `"0x2"`, pending: `"0x1"`} }

func (r coRPC) CallContext(_ context.Context, result any, method string, args ...any) error {
	switch method {
	case "eth_getTransactionCount":
		if block, _ := args[1].(string); block == "pending" {
			return json.Unmarshal([]byte(r.pending), result)
		}
		return json.Unmarshal([]byte(r.latest), result)
	case "eth_getBlockByNumber":
		payload := fmt.Sprintf(`{"number":"0x0","hash":%q}`, coHeadHash)
		return json.Unmarshal([]byte(payload), result)
	default:
		return fmt.Errorf("coRPC: unexpected method %q", method)
	}
}

func coObserver(caller coRPCCaller) *nonce.Observer {
	return nonce.NewObserver(caller, nonce.ObserverConfig{
		RPCTimeout:   2 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	})
}

func coAllocator(pool *pgxpool.Pool, caller coRPCCaller) *nonce.Allocator {
	return nonce.NewAllocator(pool, coObserver(caller))
}

func coRunner(pool *pgxpool.Pool, caller coRPCCaller) *nonce.AdminRunner {
	return nonce.NewAdminRunner(pool, coObserver(caller))
}

// coSeed creates the 007 caller/authorization rows plus one registry + scope
// row per scenario sender (lockScopeRowTx requires the scope row to exist).
func coSeed(t *testing.T, sqlDB *sql.DB, senders ...string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES (1, 'co-it')`)
	nonceMustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1, 1, $2, $3, $4, 1, 'active')`, coAuth, coChain, nonceAddr("cc"), nonceAddr("dd"))
	for _, sender := range senders {
		nonceMustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry
			(chain_id, sender, state, registry_seq) VALUES ($1, $2, 'active', 1)`, coChain, sender)
		nonceMustExec(t, sqlDB, `INSERT INTO nonce_scope_state (chain_id, sender) VALUES ($1, $2)`, coChain, sender)
	}
}

// coSeedPolicy writes the 006 bootstrap policy row the recovery FK needs
// (idempotent: repeated scenarios share one chain).
func coSeedPolicy(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO reorg_policy_history
		(chain_id, policy_seq, max_depth, prev_seq, operator, request_id)
		VALUES ($1, 1, 64, NULL, 'bootstrap', NULL)
		ON CONFLICT (chain_id, policy_seq) DO NOTHING`, coChain)
}

// coSeedRecovery establishes one active 006 recovery via raw SQL (its
// `established` terminal-event history included).
func coSeedRecovery(t *testing.T, sqlDB *sql.DB, recoveryID, phase string) {
	t.Helper()
	coSeedPolicy(t, sqlDB)
	nonceMustExec(t, sqlDB, `INSERT INTO reorg_recovery
		(chain_id, recovery_id, phase, policy_seq, max_depth,
		 bound_old_number, bound_old_hash, recovery_seq)
		VALUES ($1, $2, $3, 1, 64, 10, $4, 1)`, coChain, recoveryID, phase, coHeadHash)
	nonceMustExec(t, sqlDB, `INSERT INTO reorg_recovery_events
		(chain_id, recovery_id, recovery_seq, event_seq, event)
		VALUES ($1, $2, 1, 1, 'established')`, coChain, recoveryID)
}

// coCompleteRecovery performs 006 completion exactly as 006 does: append the
// terminal event, then delete the active recovery row (history survives).
func coCompleteRecovery(t *testing.T, sqlDB *sql.DB, recoveryID string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO reorg_recovery_events
		(chain_id, recovery_id, recovery_seq, event_seq, event)
		VALUES ($1, $2, 1, 2, 'auto_completed')`, coChain, recoveryID)
	nonceMustExec(t, sqlDB, `DELETE FROM reorg_recovery WHERE chain_id = $1`, coChain)
}

// coSeedPauses writes one row in each 006 pause table via raw SQL.
func coSeedPauses(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO indexer_pause
		(chain_id, height, expected_hash, actual_hash, kind, detail)
		VALUES ($1, 10, $2, $2, 'hash_mismatch', 'co')`, coChain, coHeadHash)
	nonceMustExec(t, sqlDB, `INSERT INTO log_pause (chain_id, height, kind, detail)
		VALUES ($1, 10, 'chain_view_changed', 'co')`, coChain)
	nonceMustExec(t, sqlDB, `INSERT INTO deposit_pause (chain_id, height, kind, detail)
		VALUES ($1, 10, 'upstream_gap', 'co')`, coChain)
}

// coGateTables are the four 006 tables whose rows must be byte-identical
// across every 008 path.
var coGateTables = []string{"reorg_recovery", "indexer_pause", "log_pause", "deposit_pause"}

// coGateSnapshot renders each 006 table's full row set as one canonical
// string (ordered composite text). Equality of the returned map is the
// byte-identity assertion.
func coGateSnapshot(t *testing.T, sqlDB *sql.DB) map[string]string {
	t.Helper()
	out := make(map[string]string, len(coGateTables))
	for _, table := range coGateTables {
		var blob string
		query := fmt.Sprintf(
			`SELECT COALESCE(string_agg(t::text, E'\n' ORDER BY t::text), '<empty>') FROM %s t`, table)
		if err := sqlDB.QueryRow(query).Scan(&blob); err != nil {
			t.Fatalf("snapshot %s: %v", table, err)
		}
		out[table] = blob
	}
	return out
}

func coWantGateUnchanged(t *testing.T, before, after map[string]string, when string) {
	t.Helper()
	for _, table := range coGateTables {
		if before[table] != after[table] {
			t.Fatalf("006 table %s changed across %s:\nbefore=%q\nafter =%q",
				table, when, before[table], after[table])
		}
	}
}

// coHoldRow is one nonce_scope_holds row read by the scenario assertions.
type coHoldRow struct {
	holdID     string
	status     string
	evidenceID string
}

func coReadHold(t *testing.T, sqlDB *sql.DB, sender string) coHoldRow {
	t.Helper()
	var h coHoldRow
	if err := sqlDB.QueryRow(`SELECT hold_id, status, evidence_observation_id
		FROM nonce_scope_holds WHERE chain_id = $1 AND sender = $2`, coChain, sender).
		Scan(&h.holdID, &h.status, &h.evidenceID); err != nil {
		t.Fatalf("read hold for %s: %v", sender, err)
	}
	return h
}

// coHoldBlob snapshots the scope's whole hold row set as one string.
func coHoldBlob(t *testing.T, sqlDB *sql.DB, sender string) string {
	t.Helper()
	var blob string
	if err := sqlDB.QueryRow(`SELECT COALESCE(string_agg(t::text, E'\n' ORDER BY t::text), '<empty>')
		FROM nonce_scope_holds t WHERE chain_id = $1 AND sender = $2`, coChain, sender).Scan(&blob); err != nil {
		t.Fatalf("snapshot holds for %s: %v", sender, err)
	}
	return blob
}

// coBindingBlob snapshots the scope's whole binding row set as one string.
func coBindingBlob(t *testing.T, sqlDB *sql.DB, sender string) string {
	t.Helper()
	var blob string
	if err := sqlDB.QueryRow(`SELECT COALESCE(string_agg(t::text, E'\n' ORDER BY t::text), '<empty>')
		FROM nonce_bindings t WHERE chain_id = $1 AND sender = $2`, coChain, sender).Scan(&blob); err != nil {
		t.Fatalf("snapshot bindings for %s: %v", sender, err)
	}
	return blob
}

// TestNonceRecoveryCoexistenceIntegration is the T033 acceptance over one
// migrated scratch database with one scope per scenario.
func TestNonceRecoveryCoexistenceIntegration(t *testing.T) {
	dsn := nonceStartPostgres(t)
	ctx := context.Background()

	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)
	coSeed(t, sqlDB, coSenderPre, coSenderPost, coSenderHold, coSenderPause)

	pool := replayOpenPool(t, dsn)
	healthy := coAllocator(pool, coHealthy())
	healthyRunner := coRunner(pool, coHealthy())
	divergent := coAllocator(pool, coDivergent())

	// --- 006 established before admission: refused, zero writes -----------
	t.Run("006_recovery_before_admission_refused", func(t *testing.T) {
		coSeedRecovery(t, sqlDB, "co-rec-pre", "detected")
		before := coGateSnapshot(t, sqlDB)

		binding, outcome, err := healthy.Allocate(ctx, nonce.AllocationRequest{
			IntentID: "co-intent-pre", ChainID: coChain, Sender: coSenderPre, AuthorizationID: coAuth,
		})
		if binding != nil || outcome != nonce.OutcomeRecoveryActive || !nonce.IsOutcome(err, nonce.OutcomeRecoveryActive) {
			t.Fatalf("admission under active 006 recovery = (%v, %q, %v), want recovery_active refusal with no binding", binding, outcome, err)
		}
		if err == nil || !strings.Contains(err.Error(), "reorg_recovery:detected") {
			t.Fatalf("refusal reason = %v, want the recorded 006 cause reorg_recovery:detected", err)
		}
		if blob := coBindingBlob(t, sqlDB, coSenderPre); blob != "<empty>" {
			t.Fatalf("refused admission wrote bindings: %q", blob)
		}
		var observations int
		if err := sqlDB.QueryRow(`SELECT count(*) FROM nonce_observations
			WHERE chain_id = $1 AND sender = $2`, coChain, coSenderPre).Scan(&observations); err != nil {
			t.Fatalf("count observations: %v", err)
		}
		if observations != 0 {
			t.Fatalf("refused admission persisted %d observation(s), want 0", observations)
		}
		coWantGateUnchanged(t, before, coGateSnapshot(t, sqlDB), "006 recovery before admission")

		coCompleteRecovery(t, sqlDB, "co-rec-pre") // cleanup for later scenarios
	})

	// --- admission committed first: the binding stands ---------------------
	t.Run("admission_first_binding_stands", func(t *testing.T) {
		before := coGateSnapshot(t, sqlDB)
		binding, outcome, err := healthy.Allocate(ctx, nonce.AllocationRequest{
			IntentID: "co-intent-post", ChainID: coChain, Sender: coSenderPost, AuthorizationID: coAuth,
		})
		if err != nil || outcome != nonce.OutcomeAllocated || binding == nil || binding.Nonce.String() != "0" {
			t.Fatalf("admission before 006 establishment = (%v, %q, %v), want allocated nonce 0", binding, outcome, err)
		}
		coWantGateUnchanged(t, before, coGateSnapshot(t, sqlDB), "admission commit")
		committed := coBindingBlob(t, sqlDB, coSenderPost)

		// 006 recovery establishes AFTER the binding committed.
		coSeedRecovery(t, sqlDB, "co-rec-post", "replaying")
		withRecovery := coGateSnapshot(t, sqlDB)
		if now := coBindingBlob(t, sqlDB, coSenderPost); now != committed {
			t.Fatalf("006 establishment changed the committed binding:\nbefore=%q\nafter =%q", committed, now)
		}

		// A later admission refuses 006_active and leaves the binding intact.
		late, lateOutcome, lateErr := healthy.Allocate(ctx, nonce.AllocationRequest{
			IntentID: "co-intent-post-late", ChainID: coChain, Sender: coSenderPost, AuthorizationID: coAuth,
		})
		if late != nil || lateOutcome != nonce.OutcomeRecoveryActive || !nonce.IsOutcome(lateErr, nonce.OutcomeRecoveryActive) {
			t.Fatalf("admission after 006 establishment = (%v, %q, %v), want recovery_active refusal", late, lateOutcome, lateErr)
		}
		if now := coBindingBlob(t, sqlDB, coSenderPost); now != committed {
			t.Fatalf("refused admission changed the committed binding:\nbefore=%q\nafter =%q", committed, now)
		}
		coWantGateUnchanged(t, withRecovery, coGateSnapshot(t, sqlDB), "refused admission after 006 establishment")

		coCompleteRecovery(t, sqlDB, "co-rec-post") // cleanup for later scenarios
	})

	// --- 006 completion does NOT clear a 008 hold --------------------------
	t.Run("006_completion_does_not_clear_008_hold", func(t *testing.T) {
		before := coGateSnapshot(t, sqlDB)
		binding, outcome, err := divergent.Allocate(ctx, nonce.AllocationRequest{
			IntentID: "co-intent-hold", ChainID: coChain, Sender: coSenderHold, AuthorizationID: coAuth,
		})
		if binding != nil || outcome != nonce.OutcomeScopeHeld || !nonce.IsOutcome(err, nonce.OutcomeScopeHeld) {
			t.Fatalf("divergence admission = (%v, %q, %v), want scope_held refusal", binding, outcome, err)
		}
		coWantGateUnchanged(t, before, coGateSnapshot(t, sqlDB), "hold establishment")

		holdBefore := coReadHold(t, sqlDB, coSenderHold)
		if holdBefore.status != "active" {
			t.Fatalf("established hold status = %q, want active", holdBefore.status)
		}
		blobBefore := coHoldBlob(t, sqlDB, coSenderHold)

		// 006 recovery established and then completed entirely via raw 006
		// fixtures (terminal event appended, active row deleted).
		coSeedRecovery(t, sqlDB, "co-rec-hold", "complete_pending")
		coCompleteRecovery(t, sqlDB, "co-rec-hold")

		if after := coReadHold(t, sqlDB, coSenderHold); after != holdBefore {
			t.Fatalf("006 completion changed the 008 hold row: %+v -> %+v", holdBefore, after)
		}
		if blob := coHoldBlob(t, sqlDB, coSenderHold); blob != blobBefore {
			t.Fatalf("006 completion changed the 008 hold row set:\nbefore=%q\nafter =%q", blobBefore, blob)
		}
		if after := coReadHold(t, sqlDB, coSenderHold); after.status != "active" {
			t.Fatalf("006 completion cleared the 008 hold (status %q), want still active", after.status)
		}
	})

	// --- 008 hold release does NOT clear a 006 pause -----------------------
	t.Run("008_release_does_not_clear_006_pause", func(t *testing.T) {
		before := coGateSnapshot(t, sqlDB)
		_, outcome, err := divergent.Allocate(ctx, nonce.AllocationRequest{
			IntentID: "co-intent-pause", ChainID: coChain, Sender: coSenderPause, AuthorizationID: coAuth,
		})
		if outcome != nonce.OutcomeScopeHeld || !nonce.IsOutcome(err, nonce.OutcomeScopeHeld) {
			t.Fatalf("divergence admission = (%q, %v), want scope_held refusal", outcome, err)
		}
		coWantGateUnchanged(t, before, coGateSnapshot(t, sqlDB), "hold establishment (pause scenario)")

		hold := coReadHold(t, sqlDB, coSenderPause)
		if hold.status != "active" {
			t.Fatalf("established hold status = %q, want active", hold.status)
		}

		// All three 006 pauses established AFTER the hold exists.
		coSeedPauses(t, sqlDB)
		paused := coGateSnapshot(t, sqlDB)
		for _, table := range []string{"indexer_pause", "log_pause", "deposit_pause"} {
			if paused[table] == "<empty>" {
				t.Fatalf("%s fixture row missing before the 008 release path", table)
			}
		}

		res, err := healthyRunner.Run(ctx, nonce.AdminRequest{
			Action:        nonce.AdminActionHoldRelease,
			OperationID:   "op-co-hold-release-1",
			ChainID:       coChain,
			Sender:        coSenderPause,
			HoldID:        hold.holdID,
			ObservationID: hold.evidenceID,
			Evidence:      "co: fresh upstream re-observation is consistent; the divergence cause is resolved",
			Operator:      "co-operator",
			Reason:        "coexistence release",
		})
		if err != nil {
			t.Fatalf("hold release: %v", err)
		}
		if res.Outcome != nonce.AdminApplied {
			t.Fatalf("hold release outcome = %q, want applied (detail %q)", res.Outcome, res.Detail)
		}
		if after := coReadHold(t, sqlDB, coSenderPause); after.status != "released" {
			t.Fatalf("hold status after release = %q, want released", after.status)
		}
		coWantGateUnchanged(t, paused, coGateSnapshot(t, sqlDB), "008 hold release")
	})
}
