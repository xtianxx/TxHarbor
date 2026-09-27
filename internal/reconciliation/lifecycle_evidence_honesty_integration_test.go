//go:build integration

// lifecycle_evidence_honesty_integration_test.go is the T030 acceptance layer
// for the close evidence gate over the real 000016 schema (quickstart §3/§11;
// tasks T030):
//
//   - Store.CloseDiscrepancy always reads the latest reverify row from the
//     database: an older fresh consistent row can never beat a newer
//     unknown/gap-limited row, and a stale or future-dated row is refused
//     regardless of the configured tolerance (the tolerance is the window, it
//     never substitutes for freshness);
//   - a refused close changes nothing: the ticket stays pending_verify, no
//     close_basis is written and the refusal is audited;
//   - only a fresh consistent row on complete evidence closes, and the
//     close_basis snapshot is recorded.
//
// The pending/incomplete scan classifications never reach these gates at all
// (they carry no ticket); these cases pin the lifecycle half of the T030
// requirement that evidence gates cannot be bypassed. T030's scan-side
// companion (event-only absence as alert-only pending) is BLOCKED on a missing
// cutover/expected-event discriminator and stays on the fail-closed missing
// path for now — see the blocker note in scan_evidence_honesty_test.go. No
// pre-cutover or post-cutover shape reaches these gates without the same
// fresh, DB-recorded consistent reverify evidence.
//
// PostgreSQL comes from testcontainers via the T012 helpers; without a Docker
// provider the package reports NOT RUN (t.Skip), never a pass.
package reconciliation

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// lifecycleHonestyCloseBasis renders the required JSON object snapshot.
func lifecycleHonestyCloseBasis() []byte {
	return []byte(`{"range":"100..105","block":105,"version":"v1","observed_at":"2026-09-27T00:00:00Z"}`)
}

func TestIntegrationCloseReadsLatestReverifyRowFromDB(t *testing.T) {
	ctx, pool, store := reconIT(t)
	const tolerance = time.Hour
	const actor = "deploy:it-closer"

	t.Run("latest_unknown_verdict_beats_an_older_fresh_consistent_row", func(t *testing.T) {
		id := uuid.NewString()
		lifecycleITSeedDiscrepancy(t, ctx, pool, id, DiscrepancyStatePendingVerify, "")
		fresh := time.Now().UTC().Add(-time.Minute)
		if _, err := pool.Exec(ctx, `
			INSERT INTO reverify (discrepancy_id, verdict, evidence_ref, freshness_at)
			VALUES ($1::uuid, 'consistent', 'evidence-older-fresh', $2)`, id, fresh); err != nil {
			t.Fatalf("insert older consistent reverify: %v", err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO reverify (discrepancy_id, verdict, evidence_ref)
			VALUES ($1::uuid, 'unknown', 'scan-gap:query_failed')`, id); err != nil {
			t.Fatalf("insert newer unknown reverify: %v", err)
		}

		_, err := store.CloseDiscrepancy(ctx, DiscrepancyCloseRequest{
			DiscrepancyID: id, Actor: actor, Reason: "close over the latest row",
			CloseBasis: lifecycleHonestyCloseBasis(), ReverifyTolerance: tolerance,
		})
		if !errors.Is(err, ErrReverifyRequired) {
			t.Fatalf("close err = %v, want ErrReverifyRequired (the latest row is unknown)", err)
		}
		if state, _, _, _ := lifecycleITDiscrepancyState(t, ctx, pool, id); state != DiscrepancyStatePendingVerify {
			t.Fatalf("state after the refused close = %s, want pending_verify", state)
		}
		if n := lifecycleITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM discrepancy
			WHERE discrepancy_id = $1::uuid AND close_basis IS NULL`, id); n != 1 {
			t.Fatalf("the refused close wrote a close_basis")
		}
		if n := lifecycleITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM recon_audit
			WHERE action = 'refuse' AND target->>'discrepancy_id' = $1`, id); n != 1 {
			t.Fatalf("refuse audit rows = %d, want 1", n)
		}
	})

	t.Run("stale_consistent_row_is_refused_even_though_the_verdict_is_consistent", func(t *testing.T) {
		id := uuid.NewString()
		lifecycleITSeedDiscrepancy(t, ctx, pool, id, DiscrepancyStatePendingVerify, "")
		stale := time.Now().UTC().Add(-2 * time.Hour)
		if _, err := pool.Exec(ctx, `
			INSERT INTO reverify (discrepancy_id, verdict, evidence_ref, freshness_at)
			VALUES ($1::uuid, 'consistent', 'evidence-stale', $2)`, id, stale); err != nil {
			t.Fatalf("insert stale consistent reverify: %v", err)
		}

		if _, err := store.CloseDiscrepancy(ctx, DiscrepancyCloseRequest{
			DiscrepancyID: id, Actor: actor, Reason: "close on stale evidence",
			CloseBasis: lifecycleHonestyCloseBasis(), ReverifyTolerance: tolerance,
		}); !errors.Is(err, ErrReverifyRequired) {
			t.Fatalf("close on stale evidence err = %v, want ErrReverifyRequired", err)
		}
		if state, _, _, _ := lifecycleITDiscrepancyState(t, ctx, pool, id); state != DiscrepancyStatePendingVerify {
			t.Fatalf("state after the stale close = %s, want pending_verify", state)
		}
	})

	t.Run("future_dated_evidence_is_refused_regardless_of_tolerance", func(t *testing.T) {
		id := uuid.NewString()
		lifecycleITSeedDiscrepancy(t, ctx, pool, id, DiscrepancyStatePendingVerify, "")
		future := time.Now().UTC().Add(time.Hour)
		if _, err := pool.Exec(ctx, `
			INSERT INTO reverify (discrepancy_id, verdict, evidence_ref, freshness_at)
			VALUES ($1::uuid, 'consistent', 'evidence-future', $2)`, id, future); err != nil {
			t.Fatalf("insert future consistent reverify: %v", err)
		}

		if _, err := store.CloseDiscrepancy(ctx, DiscrepancyCloseRequest{
			DiscrepancyID: id, Actor: actor, Reason: "close on future evidence",
			CloseBasis: lifecycleHonestyCloseBasis(), ReverifyTolerance: 30 * 24 * time.Hour,
		}); !errors.Is(err, ErrReverifyRequired) {
			t.Fatalf("close on future evidence err = %v, want ErrReverifyRequired", err)
		}
		if state, _, _, _ := lifecycleITDiscrepancyState(t, ctx, pool, id); state != DiscrepancyStatePendingVerify {
			t.Fatalf("state after the future close = %s, want pending_verify", state)
		}
	})

	t.Run("missing_tolerance_never_closes", func(t *testing.T) {
		id := uuid.NewString()
		lifecycleITSeedDiscrepancy(t, ctx, pool, id, DiscrepancyStatePendingVerify, "")
		if _, err := pool.Exec(ctx, `
			INSERT INTO reverify (discrepancy_id, verdict, evidence_ref, freshness_at)
			VALUES ($1::uuid, 'consistent', 'evidence-fresh', now())`, id); err != nil {
			t.Fatalf("insert fresh consistent reverify: %v", err)
		}

		if _, err := store.CloseDiscrepancy(ctx, DiscrepancyCloseRequest{
			DiscrepancyID: id, Actor: actor, Reason: "close without a tolerance",
			CloseBasis: lifecycleHonestyCloseBasis(), ReverifyTolerance: 0,
		}); !errors.Is(err, ErrReverifyRequired) {
			t.Fatalf("close without a tolerance err = %v, want ErrReverifyRequired", err)
		}
	})

	t.Run("fresh_consistent_row_closes_and_records_the_basis", func(t *testing.T) {
		id := uuid.NewString()
		lifecycleITSeedDiscrepancy(t, ctx, pool, id, DiscrepancyStatePendingVerify, "")
		if _, err := pool.Exec(ctx, `
			INSERT INTO reverify (discrepancy_id, verdict, evidence_ref, freshness_at)
			VALUES ($1::uuid, 'consistent', 'evidence-fresh', now())`, id); err != nil {
			t.Fatalf("insert fresh consistent reverify: %v", err)
		}

		result, err := store.CloseDiscrepancy(ctx, DiscrepancyCloseRequest{
			DiscrepancyID: id, Actor: actor, Reason: "close on fresh consistent evidence",
			CloseBasis: lifecycleHonestyCloseBasis(), ReverifyTolerance: tolerance,
		})
		if err != nil || result.From != DiscrepancyStatePendingVerify || result.To != DiscrepancyStateClosed {
			t.Fatalf("close = %+v (err %v), want pending_verify -> closed", result, err)
		}
		if state, _, _, _ := lifecycleITDiscrepancyState(t, ctx, pool, id); state != DiscrepancyStateClosed {
			t.Fatalf("state after the close = %s, want closed", state)
		}
		if n := lifecycleITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM discrepancy
			WHERE discrepancy_id = $1::uuid AND close_basis->>'range' = '100..105'`, id); n != 1 {
			t.Fatalf("close_basis was not recorded")
		}
	})
}
