//go:build integration

// flip_barrier_integration_test.go owns the silent first-write barrier used by
// TestReadyzFlipsAndRecoversWithRealDependencies before the PostgreSQL fault
// injection (Phase 1, plan A in docs/ci-health-phase0-review.md).
//
// Root cause it addresses: the container Stop used to race the indexers' first
// commits. When the first write lost to the stop (57P01, scanner.go's
// database-failure branch) its backoff retry landed between the before/after
// snapshots, and the exact-equality assertion reported a photo-finish false
// negative.
//
// The barrier is a pure test-side observation loop: no goroutine, no fixed
// sleep beyond the shared 100ms polling idiom, no production change, bounded
// deadline. It waits until BOTH the serve-side gauges (indexerObserver and
// logObserver), read from /metrics, AND the persisted first-write rows
// (chain_blocks + log_checkpoint, read directly from PostgreSQL) are present
// and have been unchanged for N consecutive polls. The observation time is
// returned so the caller can stamp the Stop and log the ordering edge.
//
// Happens-before: scanner.Checkpoint only reports ok after setProgress, which
// runs only after commitBlock returned nil (scanner.go), so the gauge proves
// the first header commit committed; the chain_blocks row proves the same on
// disk; the log_checkpoint row is the log scanner's first interval-atomic
// commit. With both rows present and unchanged across four 100ms polls the
// first writes are settled (both scanners have moved on to their head-wait
// state) before Stop starts.
package health_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// flipBarrierPollInterval is the polling idiom shared with waitForStatus
	// and waitFlipProbeAdvance.
	flipBarrierPollInterval = 100 * time.Millisecond
	// flipBarrierStablePolls is how many consecutive identical settled
	// observations count as quiet: four 100ms polls, i.e. at least 300ms with
	// no persisted first-write value moving.
	flipBarrierStablePolls = 4
	// flipBarrierTimeout bounds the wait. The first writes land well within
	// the first second on a healthy scene; expiry means something is wrong,
	// so the caller fails with the last observation as diagnostics.
	flipBarrierTimeout = 30 * time.Second
)

// flipBarrierIndexerSeries matches the header checkpoint gauge mirrored by
// indexerObserver for the scene chain (TXHARBOR_CHAIN_ID=31337 in
// flip_integration_test.go).
var flipBarrierIndexerSeries = regexp.MustCompile(`txharbor_indexer_checkpoint_height\{chain="31337"\}\s+([0-9.eE+]+)`)

// flipBarrierLogSeries matches the log checkpoint gauge mirrored by
// logObserver for the scene chain; it appears once log_checkpoint has a row.
var flipBarrierLogSeries = regexp.MustCompile(`txharbor_log_checkpoint_next\{chain="31337"\}\s+([0-9.eE+]+)`)

// flipFirstWriteSnapshot is one barrier observation: both serve-side gauge
// samples plus the persisted first-write facts.
type flipFirstWriteSnapshot struct {
	indexerGauge string // txharbor_indexer_checkpoint_height sample, "" while absent
	logGauge     string // txharbor_log_checkpoint_next sample, "" while absent
	blockRows    int64  // chain_blocks rows for the scene chain
	blockHeight  int64  // highest chain_blocks number
	logNext      int64  // log_checkpoint.next_block
	logPresent   bool   // log_checkpoint row exists
}

// settled reports whether the observation proves both first writes landed.
func (s flipFirstWriteSnapshot) settled() bool {
	return s.indexerGauge != "" && s.logGauge != "" && s.blockRows > 0 && s.logPresent
}

func (s flipFirstWriteSnapshot) String() string {
	return fmt.Sprintf("gauge[indexer=%s log=%s] chain_blocks{rows=%d max_height=%d} log_checkpoint{present=%v next=%d}",
		orAbsentFlip(s.indexerGauge), orAbsentFlip(s.logGauge), s.blockRows, s.blockHeight, s.logPresent, s.logNext)
}

func orAbsentFlip(v string) string {
	if v == "" {
		return "absent"
	}
	return v
}

// waitFlipFirstWritesSettled blocks until both indexers' first writes are
// observed committed and quiet for flipBarrierStablePolls consecutive polls,
// then returns the observation time and snapshot. Timeout fails the test with
// the last observation and read error as diagnostics.
func waitFlipFirstWritesSettled(t *testing.T, base, dsn string) (time.Time, flipFirstWriteSnapshot) {
	t.Helper()
	deadline := time.Now().Add(flipBarrierTimeout)
	stable := 0
	havePrev := false
	var prev, last flipFirstWriteSnapshot
	var lastErr error
	for time.Now().Before(deadline) {
		snap, err := readFlipFirstWriteSnapshot(base, dsn)
		observedAt := time.Now()
		if err != nil {
			lastErr = err
			stable, havePrev = 0, false
		} else {
			last = snap
			switch {
			case snap.settled() && havePrev && snap == prev:
				stable++
				if stable >= flipBarrierStablePolls {
					return observedAt, snap
				}
			case snap.settled():
				stable = 1 // first settled observation; start counting here
			default:
				stable = 0
			}
			havePrev = true
		}
		prev = snap
		time.Sleep(flipBarrierPollInterval)
	}
	t.Fatalf("first-write barrier did not settle within %s (need %d consecutive identical settled %s polls): last=%s err=%v",
		flipBarrierTimeout, flipBarrierStablePolls, flipBarrierPollInterval, last, lastErr)
	return time.Time{}, last
}

// readFlipFirstWriteSnapshot performs one bounded, read-only observation:
// one /metrics GET, then two short queries against the scene database. No
// assertions here — the polling loop in waitFlipFirstWritesSettled decides.
func readFlipFirstWriteSnapshot(base, dsn string) (flipFirstWriteSnapshot, error) {
	var snap flipFirstWriteSnapshot
	body, err := flipBarrierMetricsGet(base + "/metrics")
	if err != nil {
		return snap, fmt.Errorf("metrics: %w", err)
	}
	if m := flipBarrierIndexerSeries.FindStringSubmatch(body); m != nil {
		snap.indexerGauge = m[1]
	}
	if m := flipBarrierLogSeries.FindStringSubmatch(body); m != nil {
		snap.logGauge = m[1]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return snap, fmt.Errorf("pg connect: %w", err)
	}
	defer conn.Close(ctx)

	if err := conn.QueryRow(ctx,
		`SELECT count(*), COALESCE(max(number), 0) FROM chain_blocks WHERE chain_id = $1`,
		flipChainID).Scan(&snap.blockRows, &snap.blockHeight); err != nil {
		return snap, fmt.Errorf("chain_blocks: %w", err)
	}
	err = conn.QueryRow(ctx,
		`SELECT next_block FROM log_checkpoint WHERE chain_id = $1`, flipChainID).Scan(&snap.logNext)
	switch {
	case err == nil:
		snap.logPresent = true
	case errors.Is(err, pgx.ErrNoRows):
		// Empty progress: the log scanner's first interval has not committed.
	default:
		return snap, fmt.Errorf("log_checkpoint: %w", err)
	}
	return snap, nil
}

// flipBarrierMetricsGet is one bounded GET of the serve metrics endpoint; it
// returns errors instead of failing so the barrier loop owns the deadline.
func flipBarrierMetricsGet(url string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	return string(body), nil
}
