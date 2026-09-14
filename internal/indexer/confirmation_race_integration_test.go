//go:build integration

// T026 [US5] mid-flight race injection: pause / chain-view / policy-switch
// (specs/005-confirmation-tracking/tasks.md T026; FR-06/07/08, SC-03/07;
// US5-1, quickstart D3; data-model.md §并发时序论证 all three timing cases).
//
// Each race is injected from an INDEPENDENT SECOND database connection with
// lock-wait as the sync point (pg_stat_activity wait_event_type='Lock',
// mirroring the deposit-auth stale-revision precedent): a holder transaction
// pins the chain-wide indexer_lease coordination row FOR UPDATE while the
// racer and the confirmer queue behind it in a FORCED order; the holder then
// rolls back and the queue drains FIFO. Queue order selects the legal
// linearization under test, never probability, never fixed sleeps:
//
//   - Order A (racer first): racer commits (pause row / new tip / new policy
//     row lands) -> confirmer acquires the lock later -> post-lock reread
//     mismatch -> ROLLBACK, zero state change.
//   - Order B (confirmer first): confirmer holds the lock first, racer stays
//     blocked -> confirmer commits legally on the old (still current)
//     snapshot -> racer proceeds.
//
// Contract buckets are exact (T020 rule, quoted read-only): policy_drift /
// pause_present -> transition_total{rejected}; every other commit-time halt
// reason (tip_untrusted, reference_unverifiable) -> transition_total{stale}.
// Timing case 2 (dual confirm converges) and case 3 (old-config worker
// drifts and loud-stops per R7, never commits old results) have their own
// assertions. Helpers are race26-prefixed; production code is untouched.
//
// Append discipline: T033 appends the divergent-first-start scenario below
// (-- T033 banner --); nothing else lands in this file without a T0xx banner.
package indexer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/metrics"
)

// --- T026 helpers -----------------------------------------------------------

// race26SecondPool opens an independent connection pool against the same
// database with a distinct application_name so its lock-wait is observable
// via pg_stat_activity (the dual-connection precedent).
func race26SecondPool(t *testing.T, ctx context.Context, dsn, appName string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse race pool dsn: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = appName
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("create race pool %s: %v", appName, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// race26WaitBlocked polls pg_stat_activity until the named connection parks
// on a lock (wait_event_type='Lock'). Polling a lock-wait is the sync point;
// there is no fixed-sleep timing anywhere in this file.
func race26WaitBlocked(t *testing.T, ctx context.Context, pool *pgxpool.Pool, appName, describe string) {
	t.Helper()
	waitUntil(t, time.Now().Add(10*time.Second), describe, func() bool {
		var waiting bool
		if err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM pg_stat_activity
    WHERE application_name = $1 AND wait_event_type = 'Lock')`, appName).Scan(&waiting); err != nil {
			return false
		}
		return waiting
	})
}

// race26BeginHolder opens a transaction that pins the chain-wide coordination
// row FOR UPDATE (the shared lockCoordSQL, never copied) and holds it until
// the caller rolls back. The lease row must already exist (acquire a lease
// first). It changes nothing, so ROLLBACK is the release.
func race26BeginHolder(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) pgx.Tx {
	t.Helper()
	hold, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin holder transaction: %v", err)
	}
	var (
		owner string
		token int64
		valid bool
	)
	if err := hold.QueryRow(ctx, lockCoordSQL, chainID).Scan(&owner, &token, &valid); err != nil {
		_ = hold.Rollback(ctx)
		t.Fatalf("holder lock coordination row: %v", err)
	}
	return hold
}

// race26SeedBase plants one convertible world: canonical blocks h..tip, one
// pending observation at h, policy (1, N). It returns the observation
// identity plus the pre-race captured basis and a real acquired lease.
func race26SeedBase(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, h, tip, n uint64) (bh, txHash string, basis ConfirmBasis, lease *Lease) {
	t.Helper()
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	bh, txHash = confirmSeedPending(t, ctx, pool, chainID, h)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)
	return bh, txHash, ConfirmBasis{
		BlockHash: bh, TxHash: txHash, LogIndex: 0, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: n,
	}, depositITLease(t, pool, chainID)
}

// race26TransitionBucket quotes the T020 counter rule read-only (production
// owns it inline in the ServeLoop halt branch): drift/pause refusals are
// "rejected", every other commit-time halt reason is "stale".
func race26TransitionBucket(reason string) string {
	if reason == "policy_drift" || reason == "pause_present" {
		return "rejected"
	}
	return "stale"
}

// race26ObserveRefusal classifies a commit refusal with the real production
// classifier, samples the contract bucket on a real metrics registry, and
// asserts exactly one sample. It returns the stop reason.
func race26ObserveRefusal(t *testing.T, m *metrics.Metrics, chainID int64, err error) string {
	t.Helper()
	if err == nil {
		t.Fatalf("commit refusal = nil, want a typed refusal")
	}
	_, reason := classifyConfirmationOutcome(err)
	if reason == "" {
		t.Fatalf("classify(%v) reason is empty, want a halt reason", err)
	}
	bucket := race26TransitionBucket(reason)
	m.ObserveConfirmationTransition(chainID, bucket)
	want := confirm13ChainLabel(chainID)
	want["result"] = bucket
	if got := confirm13Counter(t, m, metrics.ConfirmationTransitionMetricName, want); got != 1 {
		t.Fatalf("%s{%s} = %v, want 1 (reason %s for %v)",
			metrics.ConfirmationTransitionMetricName, bucket, got, reason, err)
	}
	return reason
}

// --- T026 race 1: pause-row insert mid-flight (timing case 1) ----------------

// TestConfirmationRacePauseMidFlight injects a deposit_pause row between the
// candidate read and the confirm commit, in both legal orders. Order A:
// the pause lands first, the confirmer's post-lock reread sees it and rolls
// back (pause_present -> rejected, zero state change); the caller rereads
// and stays stopped. Order B: the confirmer commits legally on the old
// snapshot first, the pause lands after; an old-basis replay is refused by
// the now-present pause with the winner's basis intact.
func TestConfirmationRacePauseMidFlight(t *testing.T) {
	t.Run("OrderA_RacerFirst", func(t *testing.T) {
		dsn := startIndexerPostgres(t)
		pool := openIndexerPool(t, dsn)
		defer pool.Close()
		ctx := context.Background()
		m := metrics.New(func() bool { return true })

		const chainID, h, tip, n = int64(301), uint64(100), uint64(109), uint64(10)
		rcap := testRecoveryCap(t, ctx, pool, chainID)
		bh, txHash, basis, lease := race26SeedBase(t, ctx, pool, chainID, h, tip, n)
		hold := race26BeginHolder(t, ctx, pool, chainID)

		racerPool := race26SecondPool(t, ctx, dsn, "race26-pause-a-racer")
		confirmerPool := race26SecondPool(t, ctx, dsn, "race26-pause-a-conf")
		confirmer, err := NewConfirmationCommitter(confirmerPool, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
		if err != nil {
			t.Fatalf("NewConfirmationCommitter(): %v", err)
		}

		type outcome struct{ err error }
		racerDone := make(chan outcome, 1)
		go func() {
			tx, err := racerPool.Begin(ctx)
			if err != nil {
				racerDone <- outcome{err: err}
				return
			}
			defer func() { _ = tx.Rollback(ctx) }()
			var (
				owner string
				token int64
				valid bool
			)
			if err := tx.QueryRow(ctx, lockCoordSQL, chainID).Scan(&owner, &token, &valid); err != nil {
				racerDone <- outcome{err: err}
				return
			}
			if _, err := tx.Exec(ctx, `
INSERT INTO deposit_pause (chain_id, height, kind, detail) VALUES ($1, 10, 'upstream_gap', 'race26')`,
				chainID); err != nil {
				racerDone <- outcome{err: err}
				return
			}
			racerDone <- outcome{err: tx.Commit(ctx)}
		}()
		race26WaitBlocked(t, ctx, pool, "race26-pause-a-racer", "pause racer to park on the coordination lock")

		confDone := make(chan outcome, 1)
		go func() { confDone <- outcome{err: confirmer.ConfirmDepositUnit(ctx, lease, basis, rcap)} }()
		race26WaitBlocked(t, ctx, pool, "race26-pause-a-conf", "confirmer to park on the coordination lock")

		if err := hold.Rollback(ctx); err != nil {
			t.Fatalf("release holder: %v", err)
		}
		if got := <-racerDone; got.err != nil {
			t.Fatalf("pause racer commit: %v", got.err)
		}
		got := <-confDone
		var paused *streamPauseError
		if !errors.As(got.err, &paused) || paused.stream != "deposit_pause" {
			t.Fatalf("ConfirmDepositUnit() = %v (%T), want *streamPauseError for deposit_pause", got.err, got.err)
		}
		confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 1)
		if n := depositCountRows(t, ctx, pool, "deposit_pause", chainID); n != 1 {
			t.Fatalf("deposit_pause rows = %d, want 1 (the racer's row, and only it)", n)
		}
		if reason := race26ObserveRefusal(t, m, chainID, got.err); reason != "pause_present" {
			t.Fatalf("stop reason = %q, want pause_present", reason)
		}
		// Caller rereads and re-decides: a fresh submit still sees the
		// pause and stays stopped with zero further change.
		if err := confirmer.ConfirmDepositUnit(ctx, lease, basis, rcap); !errors.As(err, &paused) {
			t.Fatalf("reread ConfirmDepositUnit() = %v, want *streamPauseError (still stopped)", err)
		}
		confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 1)
	})

	t.Run("OrderB_ConfirmerFirst", func(t *testing.T) {
		dsn := startIndexerPostgres(t)
		pool := openIndexerPool(t, dsn)
		defer pool.Close()
		ctx := context.Background()
		_ = metrics.New(func() bool { return true })

		const chainID, h, tip, n = int64(302), uint64(100), uint64(109), uint64(10)
		rcap := testRecoveryCap(t, ctx, pool, chainID)
		bh, txHash, basis, lease := race26SeedBase(t, ctx, pool, chainID, h, tip, n)
		hold := race26BeginHolder(t, ctx, pool, chainID)

		racerPool := race26SecondPool(t, ctx, dsn, "race26-pause-b-racer")
		confirmerPool := race26SecondPool(t, ctx, dsn, "race26-pause-b-conf")
		confirmer, err := NewConfirmationCommitter(confirmerPool, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
		if err != nil {
			t.Fatalf("NewConfirmationCommitter(): %v", err)
		}

		type outcome struct{ err error }
		confDone := make(chan outcome, 1)
		go func() { confDone <- outcome{err: confirmer.ConfirmDepositUnit(ctx, lease, basis, rcap)} }()
		race26WaitBlocked(t, ctx, pool, "race26-pause-b-conf", "confirmer to park on the coordination lock")

		racerDone := make(chan outcome, 1)
		go func() {
			tx, err := racerPool.Begin(ctx)
			if err != nil {
				racerDone <- outcome{err: err}
				return
			}
			defer func() { _ = tx.Rollback(ctx) }()
			var (
				owner string
				token int64
				valid bool
			)
			if err := tx.QueryRow(ctx, lockCoordSQL, chainID).Scan(&owner, &token, &valid); err != nil {
				racerDone <- outcome{err: err}
				return
			}
			if _, err := tx.Exec(ctx, `
INSERT INTO deposit_pause (chain_id, height, kind, detail) VALUES ($1, 10, 'upstream_gap', 'race26')`,
				chainID); err != nil {
				racerDone <- outcome{err: err}
				return
			}
			racerDone <- outcome{err: tx.Commit(ctx)}
		}()
		race26WaitBlocked(t, ctx, pool, "race26-pause-b-racer", "pause racer to park on the coordination lock")

		if err := hold.Rollback(ctx); err != nil {
			t.Fatalf("release holder: %v", err)
		}
		if got := <-confDone; got.err != nil {
			t.Fatalf("legal-first ConfirmDepositUnit(): %v", got.err)
		}
		if got := <-racerDone; got.err != nil {
			t.Fatalf("pause racer commit: %v", got.err)
		}
		// The conversion landed on the then-current basis before the pause.
		status, _, tipN, thr, seq, _, conf := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
		if status != "confirmed" || tipN != int64(tip) || thr != int64(n) || seq != 1 || conf != "10" {
			t.Fatalf("winner basis = status %s tip %d N %d conf %s seq %d, want confirmed/109/10/10/1",
				status, tipN, thr, conf, seq)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_pause", chainID); n != 1 {
			t.Fatalf("deposit_pause rows = %d, want 1 (landed after the commit)", n)
		}
		// Old-basis replay after the pause: refused, winner basis intact.
		var paused *streamPauseError
		if err := confirmer.ConfirmDepositUnit(ctx, lease, basis, rcap); !errors.As(err, &paused) {
			t.Fatalf("replay ConfirmDepositUnit() = %v, want *streamPauseError", err)
		}
		after, _, tipN2, _, _, _, _ := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
		if after != "confirmed" || tipN2 != int64(tip) {
			t.Fatalf("replay rewrote the winner basis: status %s tip %d", after, tipN2)
		}
	})
}

// --- T026 race 2: canonical tip advance mid-flight (timing case 1) ----------

// TestConfirmationRaceTipAdvanceMidFlight advances the canonical tip between
// the candidate read and the confirm commit, in both legal orders. Order A:
// the tip moves first, the confirmer's post-lock tip reread mismatches and
// rolls back (tip_untrusted -> stale, zero state change); the caller rereads
// the new tip and re-decides on the fresh basis. Order B: the confirmer
// commits legally on the old tip first; an old-basis replay is then refused
// by the moved tip with the winner's basis intact.
func TestConfirmationRaceTipAdvanceMidFlight(t *testing.T) {
	t.Run("OrderA_RacerFirst", func(t *testing.T) {
		dsn := startIndexerPostgres(t)
		pool := openIndexerPool(t, dsn)
		defer pool.Close()
		ctx := context.Background()
		m := metrics.New(func() bool { return true })

		const chainID, h, tip, n = int64(303), uint64(100), uint64(109), uint64(10)
		const newTip = tip + 1
		rcap := testRecoveryCap(t, ctx, pool, chainID)
		bh, txHash, basis, lease := race26SeedBase(t, ctx, pool, chainID, h, tip, n)
		hold := race26BeginHolder(t, ctx, pool, chainID)

		racerPool := race26SecondPool(t, ctx, dsn, "race26-tip-a-racer")
		confirmerPool := race26SecondPool(t, ctx, dsn, "race26-tip-a-conf")
		confirmer, err := NewConfirmationCommitter(confirmerPool, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
		if err != nil {
			t.Fatalf("NewConfirmationCommitter(): %v", err)
		}

		type outcome struct{ err error }
		racerDone := make(chan outcome, 1)
		go func() {
			tx, err := racerPool.Begin(ctx)
			if err != nil {
				racerDone <- outcome{err: err}
				return
			}
			defer func() { _ = tx.Rollback(ctx) }()
			var (
				owner string
				token int64
				valid bool
			)
			if err := tx.QueryRow(ctx, lockCoordSQL, chainID).Scan(&owner, &token, &valid); err != nil {
				racerDone <- outcome{err: err}
				return
			}
			if _, err := tx.Exec(ctx, `UPDATE chain_blocks SET canonical = false WHERE chain_id = $1 AND number = $2`,
				chainID, int64(tip)); err != nil {
				racerDone <- outcome{err: err}
				return
			}
			if _, err := tx.Exec(ctx, `
INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
VALUES ($1, $2, $3, $4, true)`,
				chainID, int64(newTip), depositBlockHash(newTip), depositBlockHash(tip)); err != nil {
				racerDone <- outcome{err: err}
				return
			}
			racerDone <- outcome{err: tx.Commit(ctx)}
		}()
		race26WaitBlocked(t, ctx, pool, "race26-tip-a-racer", "tip racer to park on the coordination lock")

		confDone := make(chan outcome, 1)
		go func() { confDone <- outcome{err: confirmer.ConfirmDepositUnit(ctx, lease, basis, rcap)} }()
		race26WaitBlocked(t, ctx, pool, "race26-tip-a-conf", "confirmer to park on the coordination lock")

		if err := hold.Rollback(ctx); err != nil {
			t.Fatalf("release holder: %v", err)
		}
		if got := <-racerDone; got.err != nil {
			t.Fatalf("tip racer commit: %v", got.err)
		}
		got := <-confDone
		var chainView *ConfirmationChainViewError
		if !errors.As(got.err, &chainView) {
			t.Fatalf("ConfirmDepositUnit() = %v (%T), want *ConfirmationChainViewError", got.err, got.err)
		}
		confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 1)
		if reason := race26ObserveRefusal(t, m, chainID, got.err); reason != "tip_untrusted" {
			t.Fatalf("stop reason = %q, want tip_untrusted", reason)
		}
		// Caller rereads and re-decides: the fresh tip is visible, and a
		// commit on the fresh basis lands exactly once.
		var tipNumber int64
		var tipHash string
		if err := pool.QueryRow(ctx, readConfirmationTipSQL, chainID).Scan(&tipNumber, &tipHash); err != nil {
			t.Fatalf("reread canonical tip: %v", err)
		}
		if uint64(tipNumber) != newTip || tipHash != depositBlockHash(newTip) {
			t.Fatalf("reread tip = (%d %s), want (%d %s)", tipNumber, tipHash, newTip, depositBlockHash(newTip))
		}
		fresh := basis
		fresh.TipNumber, fresh.TipHash = newTip, depositBlockHash(newTip)
		if err := confirmer.ConfirmDepositUnit(ctx, lease, fresh, rcap); err != nil {
			t.Fatalf("fresh-basis ConfirmDepositUnit(): %v", err)
		}
		status, _, tipN, _, _, _, conf := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
		if status != "confirmed" || tipN != int64(newTip) || conf != "11" {
			t.Fatalf("fresh commit = status %s tip %d conf %s, want confirmed/%d/11", status, tipN, conf, newTip)
		}
	})

	t.Run("OrderB_ConfirmerFirst", func(t *testing.T) {
		dsn := startIndexerPostgres(t)
		pool := openIndexerPool(t, dsn)
		defer pool.Close()
		ctx := context.Background()

		const chainID, h, tip, n = int64(304), uint64(100), uint64(109), uint64(10)
		const newTip = tip + 1
		rcap := testRecoveryCap(t, ctx, pool, chainID)
		bh, txHash, basis, lease := race26SeedBase(t, ctx, pool, chainID, h, tip, n)
		hold := race26BeginHolder(t, ctx, pool, chainID)

		racerPool := race26SecondPool(t, ctx, dsn, "race26-tip-b-racer")
		confirmerPool := race26SecondPool(t, ctx, dsn, "race26-tip-b-conf")
		confirmer, err := NewConfirmationCommitter(confirmerPool, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
		if err != nil {
			t.Fatalf("NewConfirmationCommitter(): %v", err)
		}

		type outcome struct{ err error }
		confDone := make(chan outcome, 1)
		go func() { confDone <- outcome{err: confirmer.ConfirmDepositUnit(ctx, lease, basis, rcap)} }()
		race26WaitBlocked(t, ctx, pool, "race26-tip-b-conf", "confirmer to park on the coordination lock")

		racerDone := make(chan outcome, 1)
		go func() {
			tx, err := racerPool.Begin(ctx)
			if err != nil {
				racerDone <- outcome{err: err}
				return
			}
			defer func() { _ = tx.Rollback(ctx) }()
			var (
				owner string
				token int64
				valid bool
			)
			if err := tx.QueryRow(ctx, lockCoordSQL, chainID).Scan(&owner, &token, &valid); err != nil {
				racerDone <- outcome{err: err}
				return
			}
			if _, err := tx.Exec(ctx, `UPDATE chain_blocks SET canonical = false WHERE chain_id = $1 AND number = $2`,
				chainID, int64(tip)); err != nil {
				racerDone <- outcome{err: err}
				return
			}
			if _, err := tx.Exec(ctx, `
INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
VALUES ($1, $2, $3, $4, true)`,
				chainID, int64(newTip), depositBlockHash(newTip), depositBlockHash(tip)); err != nil {
				racerDone <- outcome{err: err}
				return
			}
			racerDone <- outcome{err: tx.Commit(ctx)}
		}()
		race26WaitBlocked(t, ctx, pool, "race26-tip-b-racer", "tip racer to park on the coordination lock")

		if err := hold.Rollback(ctx); err != nil {
			t.Fatalf("release holder: %v", err)
		}
		if got := <-confDone; got.err != nil {
			t.Fatalf("legal-first ConfirmDepositUnit(): %v", got.err)
		}
		if got := <-racerDone; got.err != nil {
			t.Fatalf("tip racer commit: %v", got.err)
		}
		status, _, tipN, thr, seq, _, conf := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
		if status != "confirmed" || tipN != int64(tip) || thr != int64(n) || seq != 1 || conf != "10" {
			t.Fatalf("winner basis = status %s tip %d N %d conf %s seq %d, want confirmed/109/10/10/1",
				status, tipN, thr, conf, seq)
		}
		// Old-basis replay after the tip move: refused, winner basis intact.
		var chainView *ConfirmationChainViewError
		if err := confirmer.ConfirmDepositUnit(ctx, lease, basis, rcap); !errors.As(err, &chainView) {
			t.Fatalf("replay ConfirmDepositUnit() = %v, want *ConfirmationChainViewError", err)
		}
		after, _, tipN2, _, seq2, _, conf2 := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
		if after != "confirmed" || tipN2 != int64(tip) || seq2 != 1 || conf2 != "10" {
			t.Fatalf("replay rewrote the winner basis: status %s tip %d conf %s seq %d", after, tipN2, conf2, seq2)
		}
	})
}

// --- T026 race 3: policy-switch INSERT mid-flight (timing cases 1 + 3) ------

// TestConfirmationRacePolicySwitchMidFlight switches the policy (new seq row
// via the privileged AuthorizeConfirmationPolicy on the second connection)
// between the candidate read and the confirm commit, in both legal orders.
// Order A: the switch lands first, the confirmer's post-lock policy guard
// mismatches and rolls back (policy_drift -> rejected, zero state change;
// timing case 1). Order B: the confirmer commits legally first, the switch
// lands after; an old-config resubmission is drift-refused with zero state
// change (timing case 3: never commits old results).
func TestConfirmationRacePolicySwitchMidFlight(t *testing.T) {
	t.Run("OrderA_RacerFirst", func(t *testing.T) {
		dsn := startIndexerPostgres(t)
		pool := openIndexerPool(t, dsn)
		defer pool.Close()
		ctx := context.Background()
		m := metrics.New(func() bool { return true })

		const chainID, h, tip, n = int64(305), uint64(100), uint64(109), uint64(10)
		rcap := testRecoveryCap(t, ctx, pool, chainID)
		bh, txHash, basis, lease := race26SeedBase(t, ctx, pool, chainID, h, tip, n)
		hold := race26BeginHolder(t, ctx, pool, chainID)

		racerPool := race26SecondPool(t, ctx, dsn, "race26-pol-a-racer")
		confirmerPool := race26SecondPool(t, ctx, dsn, "race26-pol-a-conf")
		confirmer, err := NewConfirmationCommitter(confirmerPool, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
		if err != nil {
			t.Fatalf("NewConfirmationCommitter(): %v", err)
		}

		type switchOutcome struct {
			res ConfirmAuthResult
			err error
		}
		racerDone := make(chan switchOutcome, 1)
		go func() {
			res, err := AuthorizeConfirmationPolicy(ctx, racerPool, ConfirmAuthRequest{
				ChainID: chainID, RequestID: "race26-pol-a", ExpectedOldSeq: 1,
				NewThresholdRaw: "20", Operator: "op-race", Reason: "race switch",
			})
			racerDone <- switchOutcome{res: res, err: err}
		}()
		race26WaitBlocked(t, ctx, pool, "race26-pol-a-racer", "policy racer to park on the coordination lock")

		type commitOutcome struct{ err error }
		confDone := make(chan commitOutcome, 1)
		go func() { confDone <- commitOutcome{err: confirmer.ConfirmDepositUnit(ctx, lease, basis, rcap)} }()
		race26WaitBlocked(t, ctx, pool, "race26-pol-a-conf", "confirmer to park on the coordination lock")

		if err := hold.Rollback(ctx); err != nil {
			t.Fatalf("release holder: %v", err)
		}
		racer := <-racerDone
		if racer.err != nil {
			t.Fatalf("policy racer switch: %v", racer.err)
		}
		if racer.res.PolicySeq != 2 || racer.res.Threshold != 20 || racer.res.Recorded {
			t.Fatalf("racer switch = %+v, want {PolicySeq:2 Threshold:20 Recorded:false}", racer.res)
		}
		got := <-confDone
		var drift *ConfirmationDriftError
		if !errors.As(got.err, &drift) {
			t.Fatalf("ConfirmDepositUnit() = %v (%T), want *ConfirmationDriftError", got.err, got.err)
		}
		confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 2)
		confirmAuthAssertZeroPause(t, ctx, pool, chainID)
		if reason := race26ObserveRefusal(t, m, chainID, got.err); reason != "policy_drift" {
			t.Fatalf("stop reason = %q, want policy_drift", reason)
		}
		// Caller rereads and re-decides: the old basis stays refused, the
		// effective policy is the racer's row.
		if err := confirmer.ConfirmDepositUnit(ctx, lease, basis, rcap); !errors.As(err, &drift) {
			t.Fatalf("reread ConfirmDepositUnit() = %v, want *ConfirmationDriftError", err)
		}
		confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 2)
	})

	t.Run("OrderB_ConfirmerFirst", func(t *testing.T) {
		dsn := startIndexerPostgres(t)
		pool := openIndexerPool(t, dsn)
		defer pool.Close()
		ctx := context.Background()

		const chainID, h, tip, n = int64(306), uint64(100), uint64(109), uint64(10)
		rcap := testRecoveryCap(t, ctx, pool, chainID)
		bh, txHash, basis, lease := race26SeedBase(t, ctx, pool, chainID, h, tip, n)
		hold := race26BeginHolder(t, ctx, pool, chainID)

		racerPool := race26SecondPool(t, ctx, dsn, "race26-pol-b-racer")
		confirmerPool := race26SecondPool(t, ctx, dsn, "race26-pol-b-conf")
		confirmer, err := NewConfirmationCommitter(confirmerPool, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
		if err != nil {
			t.Fatalf("NewConfirmationCommitter(): %v", err)
		}

		type commitOutcome struct{ err error }
		confDone := make(chan commitOutcome, 1)
		go func() { confDone <- commitOutcome{err: confirmer.ConfirmDepositUnit(ctx, lease, basis, rcap)} }()
		race26WaitBlocked(t, ctx, pool, "race26-pol-b-conf", "confirmer to park on the coordination lock")

		type switchOutcome struct {
			res ConfirmAuthResult
			err error
		}
		racerDone := make(chan switchOutcome, 1)
		go func() {
			res, err := AuthorizeConfirmationPolicy(ctx, racerPool, ConfirmAuthRequest{
				ChainID: chainID, RequestID: "race26-pol-b", ExpectedOldSeq: 1,
				NewThresholdRaw: "20", Operator: "op-race", Reason: "race switch",
			})
			racerDone <- switchOutcome{res: res, err: err}
		}()
		race26WaitBlocked(t, ctx, pool, "race26-pol-b-racer", "policy racer to park on the coordination lock")

		if err := hold.Rollback(ctx); err != nil {
			t.Fatalf("release holder: %v", err)
		}
		if got := <-confDone; got.err != nil {
			t.Fatalf("legal-first ConfirmDepositUnit(): %v", got.err)
		}
		racer := <-racerDone
		if racer.err != nil {
			t.Fatalf("policy racer switch: %v", racer.err)
		}
		if racer.res.PolicySeq != 2 || racer.res.Threshold != 20 || racer.res.Recorded {
			t.Fatalf("racer switch = %+v, want {PolicySeq:2 Threshold:20 Recorded:false}", racer.res)
		}
		status, _, tipN, thr, seq, _, conf := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
		if status != "confirmed" || tipN != int64(tip) || thr != int64(n) || seq != 1 || conf != "10" {
			t.Fatalf("winner basis = status %s tip %d N %d conf %s seq %d, want confirmed/109/10/10/1",
				status, tipN, thr, conf, seq)
		}
		confirmAuthAssertZeroPause(t, ctx, pool, chainID)
		// Old-config resubmission after the switch: drift-refused, zero
		// state change, effective policy untouched (timing case 3).
		var drift *ConfirmationDriftError
		if err := confirmer.ConfirmDepositUnit(ctx, lease, basis, rcap); !errors.As(err, &drift) {
			t.Fatalf("old-config ConfirmDepositUnit() = %v, want *ConfirmationDriftError", err)
		}
		after, _, tipN2, _, seq2, _, conf2 := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
		if after != "confirmed" || tipN2 != int64(tip) || seq2 != 1 || conf2 != "10" {
			t.Fatalf("old-config replay rewrote the winner basis: status %s tip %d conf %s seq %d",
				after, tipN2, conf2, seq2)
		}
		if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 2 {
			t.Fatalf("policy rows = %d, want 2 (refusal writes nothing)", n)
		}
	})
}

// --- T026 timing case 2: dual confirm converges ------------------------------

// TestConfirmationRaceDoubleConfirmConverges runs two concurrent commits of
// the same pending candidate through one shared lease (duplicate delivery)
// behind a start barrier: both serialize on the coordination lock, exactly
// one conditional UPDATE lands, the loser converges to nil, and the winner's
// basis is never rewritten (first-seen facts stay the winner's, I2).
func TestConfirmationRaceDoubleConfirmConverges(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID, h, tip, n = int64(307), uint64(100), uint64(109), uint64(10)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	bh, txHash, basis, lease := race26SeedBase(t, ctx, pool, chainID, h, tip, n)
	c, err := NewConfirmationCommitter(pool, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(): %v", err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			results <- c.ConfirmDepositUnit(ctx, lease, basis, rcap)
		}()
	}
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("concurrent ConfirmDepositUnit() = %v, want nil (win or converge)", err)
		}
	}
	status, nullAt, tipN, thr, seq, tipH, conf := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if status != "confirmed" || nullAt {
		t.Fatalf("status=%s nullAt=%v, want confirmed/non-null confirmed_at", status, nullAt)
	}
	if tipN != int64(tip) || tipH != depositBlockHash(tip) || thr != int64(n) || seq != 1 || conf != "10" {
		t.Fatalf("basis = tip(%d %s) N=%d conf=%s seq=%d, want tip(109 %s) N=10 conf=10 seq=1",
			tipN, tipH, thr, conf, seq, depositBlockHash(tip))
	}
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 1 {
		t.Fatalf("policy rows = %d, want 1 (the race bootstraps nothing extra)", n)
	}
}

// --- T026 timing case 3: old-config worker after a switch --------------------

// TestConfirmationRaceOldConfigWorkerDriftStops commits a policy switch and
// then submits with the old configuration: the policy guard mismatches and
// refuses (never commits old results, zero state change), and the old-N
// loop loud-stops with the R7 drift error (policy_drift -> rejected). A
// new-config worker on an advanced tip still converts on the new policy,
// proving the switch bricks nothing but old results.
func TestConfirmationRaceOldConfigWorkerDriftStops(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()
	m := metrics.New(func() bool { return true })

	const chainID, h, tip, n = int64(308), uint64(100), uint64(109), uint64(10)
	const newN = uint64(20)
	const newTip = uint64(119)
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	bh, txHash, basis, lease := race26SeedBase(t, ctx, pool, chainID, h, tip, n)

	res, err := AuthorizeConfirmationPolicy(ctx, pool, ConfirmAuthRequest{
		ChainID: chainID, RequestID: "race26-oldcfg", ExpectedOldSeq: 1,
		NewThresholdRaw: "20", Operator: "op-race", Reason: "raise depth",
	})
	if err != nil {
		t.Fatalf("policy switch: %v", err)
	}
	if res.PolicySeq != 2 || res.Threshold != 20 || res.Recorded {
		t.Fatalf("switch = %+v, want {PolicySeq:2 Threshold:20 Recorded:false}", res)
	}

	// Old-config worker submits the old basis: guard mismatch, zero writes.
	oldWorker, err := NewConfirmationCommitter(pool, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(old N): %v", err)
	}
	oldLease := lease
	var drift *ConfirmationDriftError
	if err := oldWorker.ConfirmDepositUnit(ctx, oldLease, basis, rcap); !errors.As(err, &drift) {
		t.Fatalf("old-config ConfirmDepositUnit() = %v (%T), want *ConfirmationDriftError", err, err)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 2)

	// R7 loud stop: the old-N loop exits non-nil on the drift, sampling
	// transition_total{rejected} exactly once with zero state change. The
	// scanner reuses the live lease: a second Acquire on this chain cannot
	// win while the first is valid (acquireSQL takes over only on expiry).
	oldCommitter, err := NewConfirmationCommitter(pool, ConfirmationConfig{ChainID: chainID, ThresholdN: n})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(old N): %v", err)
	}
	sc, err := NewConfirmationScanner(pool, ConfirmationConfig{ChainID: chainID, ThresholdN: n}, oldCommitter, m)
	if err != nil {
		t.Fatalf("NewConfirmationScanner(old N): %v", err)
	}
	err = sc.ServeLoop(ctx, lease, nil)
	var mismatch *confirmationConfigMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("old-N ServeLoop() = %v (%T), want *confirmationConfigMismatchError", err, err)
	}
	want := confirm13ChainLabel(chainID)
	want["result"] = "rejected"
	if got := confirm13Counter(t, m, metrics.ConfirmationTransitionMetricName, want); got != 1 {
		t.Fatalf("%s{rejected} = %v, want 1 (R7 drift stop)", metrics.ConfirmationTransitionMetricName, got)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 2)
	confirmAuthAssertZeroPause(t, ctx, pool, chainID)

	// New-config worker converts on the advanced tip under the new policy.
	depositSeedCanonical(t, ctx, pool, chainID, tip+1, newTip, true)
	newWorker, err := NewConfirmationCommitter(pool, ConfirmationConfig{ChainID: chainID, ThresholdN: newN})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(new N): %v", err)
	}
	fresh := ConfirmBasis{BlockHash: bh, TxHash: txHash, LogIndex: 0, Height: h,
		TipNumber: newTip, TipHash: depositBlockHash(newTip), PolicySeq: 2, ThresholdN: newN}
	if err := newWorker.ConfirmDepositUnit(ctx, lease, fresh, rcap); err != nil {
		t.Fatalf("new-config ConfirmDepositUnit(): %v", err)
	}
	status, _, tipN, thr, seq, _, conf := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if status != "confirmed" || tipN != int64(newTip) || thr != int64(newN) || seq != 2 || conf != "20" {
		t.Fatalf("new-policy commit = status %s tip %d N %d conf %s seq %d, want confirmed/119/20/20/2",
			status, tipN, thr, conf, seq)
	}
}

// --- T033: divergent-first-start (FR-03/FR-08 Q2, US4-concurrency-extension + US5-5 drift side) ---

// TestConfirmationRaceDivergentFirstStart_N10Wins and
// TestConfirmationRaceDivergentFirstStart_N20Wins cover data-model.md
// §首确认协议 分歧双首启: two instances with DIFFERENT env N and ZERO policy
// rows start simultaneously -> exactly ONE bootstrap row lands (the winner's N
// becomes the initial effective policy); the loser hits the PK (chain_id, 1)
// conflict, rolls back with zero writes, and resolveBootstrapConflict returns
// the exact production type *ConfirmationDriftError (never a silent follow).
// The winner's subsequent conversions bind the effective policy (seq
// consistency); the loser's loop loud-stops with
// *confirmationConfigMismatchError (R7, policy_drift -> rejected).
//
// Both loser-never-wins orders are forced deterministically (not by
// probability): the holder + lock-wait FIFO rendezvous reused from T026 pins
// the queue order, so each test names its winner by queueing it first. Race
// outcome only decides who-writes-first, never who-is-correct: a wrong-N
// winner is corrected via the authorized switch (§授权切换协议, T029 runbook
// path — cited, not implemented here).
//
// Lease sharing: the confirmer lease is acquired ONCE via depositITLease and
// shared by both committers — a second Acquire cannot win on a live chain
// (acquireSQL takes over only on expiry; see T026 old-config test), and the
// lease verdict only checks owner/token identity, so sharing is the
// production-faithful shape for two instances on one chain.
func race33DivergentFirstStart(t *testing.T, tag string, chainID int64, winnerN, loserN uint64) {
	t.Helper()
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()
	m := metrics.New(func() bool { return true })

	// tip-h = 20 keeps BOTH N=10 and N=20 eligible on the raced candidate
	// (conf = 21) and on the follow-up candidate h2 = h+1 (conf = 20), so
	// the loser loses only on the policy conflict, never on the gate.
	const h, tip, h2 = uint64(100), uint64(120), uint64(101)
	depositSeedCanonical(t, ctx, pool, chainID, h, tip, true)
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
	// Second pending needs its own observation row only (shared seq-1
	// history row already exists); the winner converts it post-race to
	// prove follow-up conversions bind the effective policy.
	bh2, txHash2 := depositBlockHash(h2), depositTxHash(h2, 0)
	depositSeedObservation(t, ctx, pool, chainID, h2, bh2, txHash2, 0, "1", 1)
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 0 {
		t.Fatalf("policy rows = %d, want 0 pre-race (first-confirm protocol)", n)
	}
	lease := depositITLease(t, pool, chainID)
	hold := race26BeginHolder(t, ctx, pool, chainID)

	winnerPool := race26SecondPool(t, ctx, dsn, "race33-"+tag+"-winner")
	loserPool := race26SecondPool(t, ctx, dsn, "race33-"+tag+"-loser")
	winner, err := NewConfirmationCommitter(winnerPool, ConfirmationConfig{ChainID: chainID, ThresholdN: winnerN})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(winner N=%d): %v", winnerN, err)
	}
	loser, err := NewConfirmationCommitter(loserPool, ConfirmationConfig{ChainID: chainID, ThresholdN: loserN})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(loser N=%d): %v", loserN, err)
	}
	rcap := testRecoveryCap(t, ctx, pool, chainID)
	winnerBasis := ConfirmBasis{BlockHash: bh, TxHash: txHash, LogIndex: 0, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: winnerN}
	loserBasis := ConfirmBasis{BlockHash: bh, TxHash: txHash, LogIndex: 0, Height: h,
		TipNumber: tip, TipHash: depositBlockHash(tip), PolicySeq: 1, ThresholdN: loserN}

	type outcome struct{ err error }
	winnerDone := make(chan outcome, 1)
	go func() { winnerDone <- outcome{err: winner.ConfirmDepositUnit(ctx, lease, winnerBasis, rcap)} }()
	race26WaitBlocked(t, ctx, pool, "race33-"+tag+"-winner", "winner to park on the coordination lock")

	loserDone := make(chan outcome, 1)
	go func() { loserDone <- outcome{err: loser.ConfirmDepositUnit(ctx, lease, loserBasis, rcap)} }()
	race26WaitBlocked(t, ctx, pool, "race33-"+tag+"-loser", "loser to park on the coordination lock")

	if err := hold.Rollback(ctx); err != nil {
		t.Fatalf("release holder: %v", err)
	}
	if got := <-winnerDone; got.err != nil {
		t.Fatalf("winner(N=%d) ConfirmDepositUnit(): %v", winnerN, got.err)
	}
	got := <-loserDone
	var drift *ConfirmationDriftError
	if !errors.As(got.err, &drift) {
		t.Fatalf("loser(N=%d) ConfirmDepositUnit() = %v (%T), want *ConfirmationDriftError (exact production type)",
			loserN, got.err, got.err)
	}
	if reason := race26ObserveRefusal(t, m, chainID, got.err); reason != "policy_drift" {
		t.Fatalf("loser stop reason = %q, want policy_drift", reason)
	}

	// Exactly one bootstrap row: the winner's N is the initial effective
	// policy, operator bootstrap, request_id NULL.
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 1 {
		t.Fatalf("policy rows = %d, want exactly 1 (winner N=%d bootstrap)", n, winnerN)
	}
	var seq, thr int64
	var op string
	var reqID *string
	if err := pool.QueryRow(ctx, `
SELECT policy_seq, threshold, operator, request_id FROM confirmation_policy_history
WHERE chain_id = $1`, chainID).Scan(&seq, &thr, &op, &reqID); err != nil {
		t.Fatalf("read bootstrap policy row: %v", err)
	}
	if seq != 1 || thr != int64(winnerN) || op != "bootstrap" || reqID != nil {
		t.Fatalf("bootstrap row = (%d %d %q %v), want (1 %d bootstrap NULL)", seq, thr, op, reqID, winnerN)
	}

	// Loser performed ZERO confirmation conversions: the raced candidate
	// carries the winner's basis, and no confirmed row binds the loser N.
	status, _, tipN, convThr, convSeq, _, conf := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if status != "confirmed" || tipN != int64(tip) || convThr != int64(winnerN) || convSeq != 1 || conf != "21" {
		t.Fatalf("raced basis = status %s tip %d N %d conf %s seq %d, want confirmed/120/%d/21/1",
			status, tipN, convThr, conf, convSeq, winnerN)
	}
	var loserConfirmed int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed' AND confirm_threshold = $2`,
		chainID, int64(loserN)).Scan(&loserConfirmed); err != nil {
		t.Fatalf("count loser-N conversions: %v", err)
	}
	if loserConfirmed != 0 {
		t.Fatalf("loser-N confirmed rows = %d, want 0 (loser converts nothing)", loserConfirmed)
	}

	// Loser resubmission on the still-pending second candidate: drift-refused
	// with zero state change (never silently follows the new policy).
	loserBasis2 := loserBasis
	loserBasis2.BlockHash, loserBasis2.TxHash, loserBasis2.Height = bh2, txHash2, h2
	if err := loser.ConfirmDepositUnit(ctx, lease, loserBasis2, rcap); !errors.As(err, &drift) {
		t.Fatalf("loser resubmit ConfirmDepositUnit() = %v, want *ConfirmationDriftError", err)
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bh2, txHash2, 1)

	// Winner's subsequent conversion binds the effective policy (seq
	// consistency): converts with (1, winnerN), row count stays 1.
	winnerBasis2 := winnerBasis
	winnerBasis2.BlockHash, winnerBasis2.TxHash, winnerBasis2.Height = bh2, txHash2, h2
	if err := winner.ConfirmDepositUnit(ctx, lease, winnerBasis2, rcap); err != nil {
		t.Fatalf("winner follow-up ConfirmDepositUnit(): %v", err)
	}
	status2, _, tipN2, convThr2, convSeq2, _, conf2 := confirmReadBasis(t, ctx, pool, chainID, bh2, txHash2)
	if status2 != "confirmed" || tipN2 != int64(tip) || convThr2 != int64(winnerN) || convSeq2 != 1 || conf2 != "20" {
		t.Fatalf("follow-up basis = status %s tip %d N %d conf %s seq %d, want confirmed/120/%d/20/1",
			status2, tipN2, convThr2, conf2, convSeq2, winnerN)
	}
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 1 {
		t.Fatalf("policy rows = %d, want 1 (follow-up binds, never appends)", n)
	}

	// Loser loud-stops: its loop exits non-nil with the R7 startup mismatch
	// (policy_drift -> rejected, exactly one sample), zero further writes.
	// The scanner reuses the live shared lease (second acquire can't win).
	loserCommitter, err := NewConfirmationCommitter(pool, ConfirmationConfig{ChainID: chainID, ThresholdN: loserN})
	if err != nil {
		t.Fatalf("NewConfirmationCommitter(loser N): %v", err)
	}
	loserMetrics := metrics.New(func() bool { return true })
	sc, err := NewConfirmationScanner(pool, ConfirmationConfig{ChainID: chainID, ThresholdN: loserN}, loserCommitter, loserMetrics)
	if err != nil {
		t.Fatalf("NewConfirmationScanner(loser N): %v", err)
	}
	err = sc.ServeLoop(ctx, lease, nil)
	var mismatch *confirmationConfigMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("loser ServeLoop() = %v (%T), want *confirmationConfigMismatchError", err, err)
	}
	want := confirm13ChainLabel(chainID)
	want["result"] = "rejected"
	if gotN := confirm13Counter(t, loserMetrics, metrics.ConfirmationTransitionMetricName, want); gotN != 1 {
		t.Fatalf("%s{rejected} = %v, want 1 (R7 loser drift stop)", metrics.ConfirmationTransitionMetricName, gotN)
	}
	if n := depositCountRows(t, ctx, pool, "confirmation_policy_history", chainID); n != 1 {
		t.Fatalf("policy rows after loser stop = %d, want 1 (stop writes nothing)", n)
	}
	confirmAuthAssertZeroPause(t, ctx, pool, chainID)
}

// TestConfirmationRaceDivergentFirstStart_N10Wins forces the N=10 instance to
// win the first-confirm race (queued first under the holder): bootstrap (1,
// 10) lands, the N=20 loser drift-refuses with zero conversions and
// loud-stops.
func TestConfirmationRaceDivergentFirstStart_N10Wins(t *testing.T) {
	race33DivergentFirstStart(t, "n10wins", 331, 10, 20)
}

// TestConfirmationRaceDivergentFirstStart_N20Wins forces the N=20 instance to
// win the first-confirm race (queued first under the holder): bootstrap (1,
// 20) lands, the N=10 loser drift-refuses with zero conversions and
// loud-stops. A wrong-N winner decides nothing about correctness — ops
// corrects via the authorized switch (§授权切换协议, T029 runbook path).
func TestConfirmationRaceDivergentFirstStart_N20Wins(t *testing.T) {
	race33DivergentFirstStart(t, "n20wins", 332, 20, 10)
}

// --- T033 ends here; nothing else lands in this file without a T0xx banner. ---
