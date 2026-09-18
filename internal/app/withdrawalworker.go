// withdrawalworker.go owns the long-running 011 execution worker process
// (`txharbor withdrawal-worker`, contracts/api.md §5). The worker holds at most
// one valid claim per intent, renews it on the heartbeat cadence, and never
// keeps execution authority in memory: a restart rebuilds every position from
// PostgreSQL (constitution III). Renewals are never progress; a renewal whose
// COMMIT is unknown is not an extension and forces a re-observation of the
// claim row before any further write.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/indexer"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/metrics"
)

// claimScanLimit bounds one scan batch; the loop repeats on its cadence.
const claimScanLimit = 32

// WithdrawalWorker is the 011 execution worker. All durable state lives in
// PostgreSQL; the struct holds only configuration and the live goroutine set.
type WithdrawalWorker struct {
	Pool         *pgxpool.Pool
	Claims       *execution.ClaimStore
	Metrics      *metrics.Metrics
	OwnerID      string
	Label        string
	Log          *slog.Logger
	ScanInterval time.Duration
	BackoffBase  time.Duration
	BackoffMax   time.Duration
	Heartbeat    time.Duration
	Stall        time.Duration
	// Reconciler and Driver are the 010 boundary participants. They are nil in
	// standalone 011 runs (the real 010 adapters are joint wiring); joint
	// deployment builds them through NewJointWithdrawalWorker.
	Reconciler *execution.Reconciler
	Driver     *execution.StepDriver

	mu     sync.Mutex
	active map[string]context.CancelFunc
	wg     sync.WaitGroup
}

// NewWithdrawalWorker validates the worker timings and mints a fresh instance
// identity.
func NewWithdrawalWorker(pool *pgxpool.Pool, cfg *config.Config, m *metrics.Metrics, log *slog.Logger) (*WithdrawalWorker, error) {
	claims, err := execution.NewClaimStore(pool, cfg.WorkerTTL, cfg.WorkerStall)
	if err != nil {
		return nil, err
	}
	owner, err := indexer.NewOwnerID()
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	return &WithdrawalWorker{
		Pool:         pool,
		Claims:       claims,
		Metrics:      m,
		OwnerID:      owner,
		Label:        cfg.WorkerLabel,
		Log:          log,
		ScanInterval: cfg.WorkerScanInterval,
		BackoffBase:  cfg.WorkerBackoffBase,
		BackoffMax:   cfg.WorkerBackoffMax,
		Heartbeat:    cfg.WorkerHeartbeat,
		Stall:        cfg.WorkerStall,
		active:       make(map[string]context.CancelFunc),
	}, nil
}

// Run reconciles every open step and catches the projection up before entering
// the scan loop, so a restart rebuilds execution position from PostgreSQL and
// 010's facts rather than from memory (persistence.md §4/§8).
func (w *WithdrawalWorker) Run(ctx context.Context) {
	w.startupCatchUp(ctx)
	ticker := time.NewTicker(w.ScanInterval)
	defer ticker.Stop()
	for {
		w.cycle(ctx)
		select {
		case <-ctx.Done():
			w.wg.Wait()
			return
		case <-ticker.C:
		}
	}
}

// cycle starts one goroutine per claimable intent that is not already served.
// It first marks stalled-but-not-taken claims (evidence only, no business
// state change).
func (w *WithdrawalWorker) cycle(ctx context.Context) {
	if marked, err := w.Claims.SweepStalls(ctx); err != nil {
		w.log().Warn("stall sweep failed", "error", logx.Redact(err.Error()))
	} else if marked > 0 && w.Metrics != nil {
		for i := 0; i < marked; i++ {
			w.Metrics.ObserveWorkerStallFlag()
		}
	}
	intents, err := w.scanClaimable(ctx)
	if err != nil {
		w.log().Warn("claim scan failed", "error", logx.Redact(err.Error()))
		return
	}
	for _, intentID := range intents {
		w.mu.Lock()
		if _, busy := w.active[intentID]; busy {
			w.mu.Unlock()
			continue
		}
		runCtx, cancel := context.WithCancel(ctx)
		w.active[intentID] = cancel
		w.mu.Unlock()

		w.wg.Add(1)
		go func(intentID string) {
			defer w.wg.Done()
			defer func() {
				w.mu.Lock()
				delete(w.active, intentID)
				w.mu.Unlock()
			}()
			w.serveIntent(runCtx, intentID)
		}(intentID)
	}
}

// serveIntent acquires the claim, marks it claimed, and renews it while active.
// A lost or expired qualification stops the holder immediately.
func (w *WithdrawalWorker) serveIntent(ctx context.Context, intentID string) {
	res, err := w.Claims.Claim(ctx, intentID, w.OwnerID)
	if err != nil {
		w.observeAcquisition("error")
		w.log().Warn("claim acquire failed", "intent_id", intentID, "error", logx.Redact(err.Error()))
		return
	}
	if !res.Acquired {
		w.observeAcquisition("not_claimable")
		return
	}
	w.observeAcquisition("acquired")
	if res.TakenOver && w.Metrics != nil {
		w.Metrics.ObserveWorkerClaimTakeover()
	}
	version := res.Version
	w.advanceIntentToClaimed(ctx, intentID, version)

	hbCtx, hbCancel := context.WithCancel(ctx)
	var hb sync.WaitGroup
	hb.Add(1)
	go func() {
		defer hb.Done()
		w.heartbeat(hbCtx, intentID, version)
	}()
	defer func() {
		hbCancel()
		hb.Wait()
		if err := w.Claims.Release(context.WithoutCancel(ctx), intentID, w.OwnerID, version); err != nil && !errors.Is(err, execution.ErrClaimLost) {
			w.log().Warn("claim release failed", "intent_id", intentID, "error", logx.Redact(err.Error()))
		}
	}()

	w.workLoop(ctx, intentID, version)
}

// workLoop reconciles then advances while the qualification stays current,
// stopping the moment the claim is lost. No external call runs while holding a
// DB lock (the driver commits the issue before calling 010).
func (w *WithdrawalWorker) workLoop(ctx context.Context, intentID string, version int64) {
	for {
		if err := w.execIntentCycle(ctx, intentID, version); err != nil {
			if errors.Is(err, execution.ErrClaimLost) {
				return
			}
			w.log().Warn("execution cycle failed", "intent_id", intentID, "error", logx.Redact(err.Error()))
		}
		current, err := execution.ClaimIsCurrent(ctx, w.Pool, intentID, w.OwnerID, version)
		if err != nil || !current {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(w.ScanInterval):
		}
	}
}

// execIntentCycle consumes 010 facts (reconcile), refreshes the display
// projection, and issues a send-class step only for a first/retry state with no
// open step. A converged "sent" step leaves the intent executing (receipt
// tracking is 010-owned) and is never re-sent automatically.
func (w *WithdrawalWorker) execIntentCycle(ctx context.Context, intentID string, version int64) error {
	intent, found, err := execution.ReadIntent(ctx, w.Pool, intentID)
	if err != nil || !found {
		return err
	}
	if intent.State == execution.IntentCompleted || intent.State == execution.IntentFailed {
		return nil
	}
	if w.Reconciler != nil {
		if _, err := w.Reconciler.ReconcileIntent(ctx, intentID, w.OwnerID, version); err != nil {
			return err
		}
		if intent, found, err = execution.ReadIntent(ctx, w.Pool, intentID); err != nil || !found {
			return err
		}
		if err := w.Reconciler.ApplyStateProjection(ctx, intent.RequestID, intent.State, intent.StateVersion); err != nil {
			return err
		}
	}
	if w.Driver == nil {
		return nil
	}
	if _, open, err := execution.ReadOpenStep(ctx, w.Pool, intentID); err != nil {
		return err
	} else if open {
		return nil
	}
	switch intent.State {
	case execution.IntentClaimed, execution.IntentReconciling, execution.IntentRevised:
		out, err := w.Driver.IssueAndAdvance(ctx, execution.StepRequest{
			IntentID: intentID, RequestID: intent.RequestID,
			OwnerID: w.OwnerID, LeaseVersion: version, Action: execution.ActionFirstBroadcast,
		})
		if err != nil {
			return err
		}
		if out.Refusal != "" && w.Metrics != nil {
			w.Metrics.ObserveWorkerGateRefusal(string(out.Refusal))
		}
	}
	return nil
}

// startupCatchUp reconciles every open (issued) step first, then runs a
// version-ordered projection catch-up. Both are no-ops without the 010 wiring.
func (w *WithdrawalWorker) startupCatchUp(ctx context.Context) {
	if w.Reconciler == nil {
		return
	}
	if err := w.Reconciler.ReconcileAllOpenSteps(ctx); err != nil {
		w.log().Warn("startup reconcile failed", "error", logx.Redact(err.Error()))
	}
	if err := w.Reconciler.ProjectionCatchUp011(ctx); err != nil {
		w.log().Warn("startup projection catch-up failed", "error", logx.Redact(err.Error()))
	}
}

// heartbeat renews the claim on the heartbeat cadence with ±10% jitter. It
// never advances progress. A renewal error is not an extension: the holder
// re-observes the claim row and stops unless the row is still current.
func (w *WithdrawalWorker) heartbeat(ctx context.Context, intentID string, version int64) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(jittered(w.Heartbeat, 10)):
		}
		if err := w.Claims.Renew(ctx, intentID, w.OwnerID, version); err != nil {
			if errors.Is(err, execution.ErrClaimLost) {
				return
			}
			current, reErr := execution.ClaimIsCurrent(ctx, w.Pool, intentID, w.OwnerID, version)
			if reErr != nil || !current {
				return
			}
		}
	}
}

// advanceIntentToClaimed applies admitted->claimed when the intent is still
// admitted. The claim event is written by ClaimStore.Claim in the same
// generation.
func (w *WithdrawalWorker) advanceIntentToClaimed(ctx context.Context, intentID string, version int64) {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if intent, found, err := execution.ReadIntent(ctx, tx, intentID); err == nil && found && intent.State == execution.IntentAdmitted {
		_ = execution.TransitionIntent(ctx, tx, intentID, execution.IntentAdmitted, intent.StateVersion, execution.IntentClaimed, version)
	}
	_ = tx.Commit(ctx)
}

func (w *WithdrawalWorker) observeAcquisition(result string) {
	if w.Metrics != nil {
		w.Metrics.ObserveWorkerClaimAcquisition(result)
	}
}

func (w *WithdrawalWorker) log() *slog.Logger {
	if w.Log != nil {
		return w.Log
	}
	return slog.Default()
}

// jittered returns base +/- percent% using integer arithmetic only, always
// positive. No float is used anywhere in a 011 value path (constitution I/XII);
// the approved heartbeat jitter (10%) is expressed as an integer fraction.
func jittered(base time.Duration, percent int64) time.Duration {
	if base <= 0 || percent <= 0 {
		return base
	}
	delta := int64(base) * percent / 100
	if delta <= 0 {
		return base
	}
	return base + time.Duration(rand.Int63n(2*delta+1)) - time.Duration(delta)
}

// WithdrawalWorkerCommand runs the long-running joint worker until the process
// context is cancelled. The 010/008 participants are assembled through
// Deps.JointWiring; a missing assembly or a failed assembly refuses startup so
// the worker never silently degrades to the claim-scan-only standalone mode.
func WithdrawalWorkerCommand(ctx context.Context, args []string, d Deps) int {
	stderr := d.stderr()
	cfg, err := config.Load(d.getenv())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor withdrawal-worker: configuration error: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if d.JointWiring == nil {
		fmt.Fprintf(stderr, "txharbor withdrawal-worker: 010/008 joint wiring is not linked; refusing a claim-scan-only worker\n")
		return 1
	}
	pool, err := db.OpenPool(ctx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor withdrawal-worker: %s\n", logx.Redact(err.Error()))
		return 1
	}
	defer pool.Close()

	deps, err := d.JointWiring(ctx, cfg, pool)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor withdrawal-worker: joint wiring failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	worker, err := NewJointWithdrawalWorker(pool, cfg, nil, slog.Default(), deps)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor withdrawal-worker: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(d.stdout(), "txharbor withdrawal-worker: joint wiring ready (driver=%T reconciler=%T)\n", worker.Driver, worker.Reconciler)
	worker.Run(ctx)
	return 0
}

const scanClaimableSQL = `SELECT i.intent_id
  FROM payment_intents i
  LEFT JOIN execution_claims c ON c.intent_id = i.intent_id
  WHERE i.state NOT IN ('completed', 'failed')
    AND (c.intent_id IS NULL
         OR c.state <> 'active'
         OR c.expires_at <= now()
         OR now() - c.last_progress_at >= make_interval(secs => $1))
  ORDER BY i.admitted_at
  LIMIT $2`

// scanClaimable lists intents whose claim is absent, inactive, expired on the
// DB clock, or stalled. Terminal intents are excluded (failed never
// auto-executes; completed waits on authority revision).
func (w *WithdrawalWorker) scanClaimable(ctx context.Context) ([]string, error) {
	rows, err := w.Pool.Query(ctx, scanClaimableSQL, w.Stall.Seconds(), claimScanLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
