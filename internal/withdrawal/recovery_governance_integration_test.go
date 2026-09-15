//go:build integration

// recovery_governance_integration_test.go owns the T029 006-governance
// read-only assertion (US6, V8 governance half). Against a real PostgreSQL 18
// container (testcontainers) it seeds representative 006 governance rows — the
// three stream pauses, the recovery authority with its policy version and event
// log, and the observation-isolation transition log — snapshots every row's
// FULL content, drives EVERY 007 write path against the same database, then
// proves the 006 state is byte-identical: 007 never pauses, de-authorizes, or
// isolates, and the downstream preconditions subset still reads the same.
//
// It reuses the T006 container/migration helper (grantSetup) and the T007/T008
// intake helpers (intakeKey, intakeSupplyGrant, intakeReq); it adds only
// governance-prefixed helpers so no name collides with the sibling test files.
package withdrawal

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// governanceChainID is the chain shared by the seeded 006 rows and the 007
// withdrawal requests, so the downstream precondition re-read and
// GetWithdrawal's recovery signal cover the same chain.
const governanceChainID int64 = 31337

// governanceTables is every 006-governance table an 007 write must never touch:
// the three stream pauses, the recovery authority plus its policy and event
// log, and the observation-isolation transition log.
var governanceTables = []string{
	"reorg_policy_history",
	"reorg_recovery",
	"reorg_recovery_events",
	"indexer_pause",
	"log_pause",
	"deposit_pause",
	"deposit_observation_transitions",
}

// governanceHex32 builds a valid 0x + 64-lowercase-hex hash from a 2-char seed
// (the CHECK shape every hash column in 000006 requires).
func governanceHex32(pair string) string { return "0x" + strings.Repeat(pair, 32) }

// governanceSeed writes one representative row into every 006 governance table
// per migrations/000006_reorg_recovery.sql (plus the 002/003/004 pause DDLs): a
// bootstrap policy version, one ACTIVE recovery row, its `established` event, a
// terminal `released` event for a prior instance, one row in each stream pause,
// and one isolation transition.
//
// These are direct seed writes by the test setup ONLY; no 007 path is used to
// create or mutate any of them.
func governanceSeed(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	exec := func(label, sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed %s: %v", label, err)
		}
	}

	exec("reorg_policy_history",
		`INSERT INTO reorg_policy_history (chain_id, policy_seq, max_depth, operator)
		 VALUES ($1, 1, 64, 'bootstrap')`,
		governanceChainID)

	exec("reorg_recovery",
		`INSERT INTO reorg_recovery
		     (chain_id, recovery_id, phase, policy_seq, max_depth, bound_old_number, bound_old_hash, recovery_seq)
		 VALUES ($1, 'rec-gov', 'detected', 1, 64, 100, $2, 1)`,
		governanceChainID, governanceHex32("aa"))

	exec("reorg_recovery_events active",
		`INSERT INTO reorg_recovery_events (chain_id, recovery_id, recovery_seq, event_seq, event, detail)
		 VALUES ($1, 'rec-gov', 1, 1, 'established', 'active governance recovery')`,
		governanceChainID)

	exec("reorg_recovery_events terminal",
		`INSERT INTO reorg_recovery_events (chain_id, recovery_id, recovery_seq, event_seq, event, detail)
		 VALUES ($1, 'rec-gov-prev', 1, 1, 'released', 'terminal release')`,
		governanceChainID)

	exec("indexer_pause",
		`INSERT INTO indexer_pause (chain_id, height, expected_hash, actual_hash, kind, detail)
		 VALUES ($1, 90, $2, $3, 'hash_mismatch', 'governance seed')`,
		governanceChainID, governanceHex32("bb"), governanceHex32("cc"))

	exec("log_pause",
		`INSERT INTO log_pause (chain_id, height, kind, detail)
		 VALUES ($1, 90, 'chain_view_changed', 'governance seed')`,
		governanceChainID)

	exec("deposit_pause",
		`INSERT INTO deposit_pause (chain_id, height, kind, detail)
		 VALUES ($1, 90, 'chain_view_changed', 'governance seed')`,
		governanceChainID)

	exec("deposit_observation_transitions",
		`INSERT INTO deposit_observation_transitions
		     (chain_id, block_hash, tx_hash, log_index, from_status, to_status, recovery_id, basis_snapshot)
		 VALUES ($1, $2, $3, 0, 'confirmed', 'orphaned', 'rec-gov', 'isolated by 006')`,
		governanceChainID, governanceHex32("dd"), governanceHex32("ee"))
}

// governanceSnapshot reads the FULL content of every 006 governance table as a
// sorted list of row JSON. It is a content compare, not a count: an in-place
// UPDATE is caught as drift exactly like an insert or a delete.
func governanceSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string][]string {
	t.Helper()
	snap := make(map[string][]string, len(governanceTables))
	for _, table := range governanceTables {
		rows, err := pool.Query(ctx, `SELECT row_to_json(t)::text FROM `+table+` t ORDER BY 1`)
		if err != nil {
			t.Fatalf("snapshot %s: %v", table, err)
		}
		var out []string
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				rows.Close()
				t.Fatalf("scan %s row: %v", table, err)
			}
			out = append(out, line)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatalf("iterate %s: %v", table, err)
		}
		rows.Close()
		if len(out) == 0 {
			t.Fatalf("snapshot %s is empty: governance seed did not land", table)
		}
		snap[table] = out
	}
	return snap
}

// governanceWantUnchanged asserts every snapshot row is byte-identical: no new
// row, no deletion, and no in-place mutation in any 006 governance table.
func governanceWantUnchanged(t *testing.T, label string, before, after map[string][]string) {
	t.Helper()
	for _, table := range governanceTables {
		if !reflect.DeepEqual(before[table], after[table]) {
			t.Fatalf("%s: %s drifted\nbefore=%v\nafter=%v", label, table, before[table], after[table])
		}
	}
}

// governanceWantPreconditions re-reads the 006 contracts/downstream.md
// precondition subset (item 1 stream pauses, item 2 active recovery) after the
// 007 writes: the same gated-action refusal a downstream stage would observe is
// still in force, and the active row is not in a released-terminal phase.
func governanceWantPreconditions(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, table := range []string{"indexer_pause", "log_pause", "deposit_pause"} {
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM `+table+` WHERE chain_id = $1`, governanceChainID).Scan(&n); err != nil {
			t.Fatalf("read pause %s: %v", table, err)
		}
		if n != 1 {
			t.Fatalf("%s rows for chain %d = %d, want the seeded 1 (still paused)", table, governanceChainID, n)
		}
	}
	var phase string
	if err := pool.QueryRow(ctx,
		`SELECT phase FROM reorg_recovery WHERE chain_id = $1`, governanceChainID).Scan(&phase); err != nil {
		t.Fatalf("read active recovery: %v", err)
	}
	if phase != "detected" {
		t.Fatalf("active recovery phase = %q, want detected (not a released-terminal phase)", phase)
	}
}

// TestWithdrawalGovernanceReadOnly is T029: drive every 007 write path against
// one scratch database that carries representative 006 governance rows, then
// prove the governance state is byte-identical (V8 governance half — 007 never
// pauses, de-authorizes, or isolates) and the downstream precondition subset is
// still observed.
func TestWithdrawalGovernanceReadOnly(t *testing.T) {
	// Given a migrated scratch database seeded with representative 006
	// governance rows and snapshotted in full.
	ctx, pool := grantSetup(t)
	governanceSeed(t, ctx, pool)
	before := governanceSnapshot(t, ctx, pool)

	// When every 007 write path runs against that same database.
	const (
		callerSubmit = int64(7601)
		callerRotate = int64(7602)
	)

	// IssueKey + SupplyGrant: arm one submit-ready caller.
	keySubmit := intakeKey(t, ctx, pool, callerSubmit)
	intakeSupplyGrant(t, ctx, pool, callerSubmit, "auth-gov-submit", intakeAmount)

	// SubmitWithdrawal: first persist is a 201.
	first, err := SubmitWithdrawal(ctx, pool, intakeReq(keySubmit, "idem-gov-1", "auth-gov-submit"))
	if err != nil {
		t.Fatalf("first SubmitWithdrawal: %v", err)
	}
	if first.Status != 201 {
		t.Fatalf("first status = %d (%+v), want 201", first.Status, first)
	}

	// SubmitWithdrawal: a same-key equal replay is a 200 with the same id.
	replay, err := SubmitWithdrawal(ctx, pool, intakeReq(keySubmit, "idem-gov-1", "auth-gov-submit"))
	if err != nil {
		t.Fatalf("replay SubmitWithdrawal: %v", err)
	}
	if replay.Status != 200 || replay.RequestID != first.RequestID {
		t.Fatalf("replay = %+v, want 200 with request_id %q", replay, first.RequestID)
	}

	// SubmitWithdrawal: a same-key differing parameter is a 409.
	conflictReq := intakeReq(keySubmit, "idem-gov-1", "auth-gov-submit")
	conflictReq.Amount = "101"
	conflict, err := SubmitWithdrawal(ctx, pool, conflictReq)
	if err != nil {
		t.Fatalf("conflicting SubmitWithdrawal: %v", err)
	}
	if conflict.Status != 409 || conflict.Code != CodeIdempotencyConflict {
		t.Fatalf("conflict = %+v, want 409/%s", conflict, CodeIdempotencyConflict)
	}

	// SubmitWithdrawal: a bad parameter is a 422.
	badReq := intakeReq(keySubmit, "idem-gov-bad", "auth-gov-submit")
	badReq.Amount = "1.5"
	rejected, err := SubmitWithdrawal(ctx, pool, badReq)
	if err != nil {
		t.Fatalf("bad-param SubmitWithdrawal: %v", err)
	}
	if rejected.Status != 422 || rejected.Code != CodeValidationFailed {
		t.Fatalf("bad-param = %+v, want 422/%s", rejected, CodeValidationFailed)
	}

	// WriteRejectAudit: the transport's best-effort pre-tx intent write, landing
	// in the 007 audit table only (exactly one new row).
	var rejectsBefore int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM withdrawal_request_audit WHERE action = 'rejected'`).Scan(&rejectsBefore); err != nil {
		t.Fatalf("count rejected audit rows before: %v", err)
	}
	if err := WriteRejectAudit(ctx, pool, callerSubmit, first.RequestID, auditActionRejected, "governance reject intent"); err != nil {
		t.Fatalf("WriteRejectAudit: %v", err)
	}
	var rejectsAfter int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM withdrawal_request_audit WHERE action = 'rejected'`).Scan(&rejectsAfter); err != nil {
		t.Fatalf("count rejected audit rows after: %v", err)
	}
	if rejectsAfter != rejectsBefore+1 {
		t.Fatalf("rejected audit rows = %d, want %d (one intent write)", rejectsAfter, rejectsBefore+1)
	}

	// GetWithdrawal: the owner read returns the persisted facts and the seeded
	// 006 recovery signal (recovering, not_started) read-only.
	view, err := GetWithdrawal(ctx, pool, callerSubmit, first.RequestID)
	if err != nil {
		t.Fatalf("GetWithdrawal: %v", err)
	}
	if view.RequestID != first.RequestID || view.CallerID != callerSubmit {
		t.Fatalf("view identity = %q/%d, want %q/%d", view.RequestID, view.CallerID, first.RequestID, callerSubmit)
	}
	if view.Status != "accepted" || view.Amount != intakeAmount {
		t.Fatalf("view status/amount = %q/%q, want accepted/%q", view.Status, view.Amount, intakeAmount)
	}
	if view.Recovery.State != queryStateRecovering || view.Recovery.Execution != queryExecutionNotStarted {
		t.Fatalf("view recovery = %+v, want {%s %s}", view.Recovery, queryStateRecovering, queryExecutionNotStarted)
	}

	// IssueKey + RotateKey + RevokeKey: rotate immediately (grace 0 terminates
	// the predecessor), revoke the successor, then prove both credentials 401.
	oldKey := intakeKey(t, ctx, pool, callerRotate)
	rotated, ref, err := RotateKey(ctx, pool, callerRotate, 0)
	if err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	if ref.CallerID != callerRotate {
		t.Fatalf("rotated ref caller = %d, want %d", ref.CallerID, callerRotate)
	}
	if err := RevokeKey(ctx, pool, ref.KeyID); err != nil {
		t.Fatalf("RevokeKey: %v", err)
	}
	for _, cred := range []struct {
		name, key, idem string
	}{
		{"predecessor after grace-0 rotation", oldKey, "idem-gov-401-old"},
		{"successor after explicit revoke", rotated, "idem-gov-401-new"},
	} {
		res, err := SubmitWithdrawal(ctx, pool, intakeReq(cred.key, cred.idem, "auth-gov-x"))
		if err != nil {
			t.Fatalf("%s SubmitWithdrawal: %v", cred.name, err)
		}
		if res.Status != 401 || res.Code != CodeUnauthenticated {
			t.Fatalf("%s submit = %+v, want 401/%s", cred.name, res, CodeUnauthenticated)
		}
	}

	// SupplyGrant + RevokeGrant: supply then revoke one grant through 007.
	intakeSupplyGrant(t, ctx, pool, callerRotate, "auth-gov-revoke", intakeAmount)
	revoked, err := RevokeGrant(ctx, pool, "revoke-op-gov", "auth-gov-revoke", "governance-test", "governance revoke")
	if err != nil {
		t.Fatalf("RevokeGrant: %v", err)
	}
	if revoked.Action != grantOutcomeRevoked {
		t.Fatalf("RevokeGrant action = %s, want %s", revoked.Action, grantOutcomeRevoked)
	}

	// Then the 006 governance state is byte-identical: 007 never paused,
	// de-authorized, or isolated anything, and the downstream precondition
	// subset still reads the same refusal.
	after := governanceSnapshot(t, ctx, pool)
	governanceWantUnchanged(t, "after every 007 write path", before, after)
	governanceWantPreconditions(t, ctx, pool)
}
