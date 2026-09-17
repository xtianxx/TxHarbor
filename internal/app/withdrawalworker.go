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

// Run scans claimable intents on the configured cadence until ctx is done.
func (w *WithdrawalWorker) Run(ctx context.Context) {
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
func (w *WithdrawalWorker) cycle(ctx context.Context) {
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
	version, ok, err := w.Claims.Acquire(ctx, intentID, w.OwnerID)
	if err != nil {
		w.observeAcquisition("error")
		w.log().Warn("claim acquire failed", "intent_id", intentID, "error", logx.Redact(err.Error()))
		return
	}
	if !ok {
		w.observeAcquisition("not_claimable")
		return
	}
	w.observeAcquisition("acquired")
	w.markClaimed(ctx, intentID, version)

	defer func() {
		if err := w.Claims.Release(context.WithoutCancel(ctx), intentID, w.OwnerID, version); err != nil && !errors.Is(err, execution.ErrClaimLost) {
			w.log().Warn("claim release failed", "intent_id", intentID, "error", logx.Redact(err.Error()))
		}
	}()

	w.heartbeat(ctx, intentID, version)
}

// heartbeat renews the claim on the heartbeat cadence with ±10% jitter. It
// never advances progress. A renewal error is not an extension: the holder
// re-observes the claim row and stops unless the row is still current.
func (w *WithdrawalWorker) heartbeat(ctx context.Context, intentID string, version int64) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(jittered(w.Heartbeat, 0.10)):
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

// markClaimed appends the claimed event and applies admitted->claimed when the
// intent is still admitted. It is best-effort: a refusal just leaves the
// intent for the next cycle.
func (w *WithdrawalWorker) markClaimed(ctx context.Context, intentID string, version int64) {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := execution.AppendEvent(ctx, tx, execution.Event{
		IntentID: intentID, Kind: execution.EventClaimed, LeaseVersion: version,
		Detail: "owner_id=" + w.OwnerID,
	}); err != nil {
		return
	}
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

// jittered returns base +/- frac (fraction of base), always positive.
func jittered(base time.Duration, frac float64) time.Duration {
	if base <= 0 {
		return base
	}
	delta := float64(base) * frac
	return base + time.Duration((rand.Float64()*2-1)*delta)
}

// WithdrawalWorkerCommand runs the long-running worker until the process
// context is cancelled.
func WithdrawalWorkerCommand(ctx context.Context, args []string, d Deps) int {
	stderr := d.stderr()
	cfg, err := config.Load(d.getenv())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor withdrawal-worker: configuration error: %s\n", logx.Redact(err.Error()))
		return 1
	}
	pool, err := db.OpenPool(ctx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor withdrawal-worker: %s\n", logx.Redact(err.Error()))
		return 1
	}
	defer pool.Close()

	worker, err := NewWithdrawalWorker(pool, cfg, nil, slog.Default())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor withdrawal-worker: %s\n", logx.Redact(err.Error()))
		return 1
	}
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
