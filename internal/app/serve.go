// Package app wires the lifecycle: configuration gate, startup chain, probe
// loop and signal-driven shutdown. Commands stay thin wrappers around it so
// behavior is unit-testable without spawning processes.
package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/health"
	"github.com/xtianxx/txharbor/internal/indexer"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/metrics"
)

// Deps carries process dependencies so commands are testable in-process.
type Deps struct {
	Getenv func(string) (string, bool)
	Stdout io.Writer
	Stderr io.Writer
	// Signals, when non-nil, replaces the OS signal notifier (tests).
	Signals <-chan os.Signal
}

func (d Deps) stdout() io.Writer {
	if d.Stdout == nil {
		return io.Discard
	}
	return d.Stdout
}

func (d Deps) stderr() io.Writer {
	if d.Stderr == nil {
		return io.Discard
	}
	return d.Stderr
}

func (d Deps) getenv() func(string) (string, bool) {
	if d.Getenv == nil {
		return os.LookupEnv
	}
	return d.Getenv
}

// Serve runs the full startup chain and blocks until a termination signal
// (FR-004/008/009/010/013/014, contracts/cli.md). It returns the process exit
// code: 0 clean shutdown, non-zero on any startup/shutdown failure.
func Serve(ctx context.Context, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()

	// 1. Configuration gate: fail before opening any listener or connection.
	cfg, err := config.Load(d.getenv())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor serve: configuration error: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout, "txharbor serve: config %s\n", cfg.Summary())

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := d.Signals
	if sigCh == nil {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(ch)
		sigCh = ch
	}
	go func() {
		select {
		case sig := <-sigCh:
			fmt.Fprintf(stdout, "txharbor serve: received %s, stopping new work\n", sig)
			cancel()
		case <-runCtx.Done():
		}
	}()

	// 2. Startup budget: all steps below share one 30s parent deadline.
	startupCtx, startupCancel := context.WithTimeout(runCtx, cfg.StartupTimeout)
	defer startupCancel()

	fail := func(format string, args ...any) int {
		fmt.Fprintf(stderr, "txharbor serve: "+format+"\n", args...)
		return 1
	}

	pool, err := db.OpenPool(startupCtx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		return fail("startup failed (database): %s", logx.Redact(err.Error()))
	}

	// 3. Version compatibility: pending/unknown/newer versions refuse startup;
	// migration is never automatic (FR-005/008).
	opts := db.MigrateOptions{
		DSN:            cfg.PGDSN,
		LockTimeout:    cfg.MigrateLockTimeout,
		ConnectTimeout: cfg.ProbeTimeout,
	}
	compatCtx, compatCancel := context.WithTimeout(startupCtx, cfg.ProbeTimeout)
	_, err = db.CheckCompatibility(compatCtx, opts)
	compatCancel()
	if err != nil {
		pool.Close()
		return fail("startup failed (schema): %s", logx.Redact(err.Error()))
	}

	// 4. RPC connectivity and chain-id check (expected/actual reported).
	ethClient, err := eth.Dial(startupCtx, cfg.RPCURL, cfg.ProbeTimeout)
	if err != nil {
		pool.Close()
		return fail("startup failed (rpc): %s", logx.Redact(err.Error()))
	}
	expectedChain := new(big.Int).SetUint64(cfg.ChainID)
	if err := ethClient.CheckChainID(startupCtx, expectedChain, cfg.ProbeTimeout); err != nil {
		ethClient.Close()
		pool.Close()
		return fail("startup failed (chain): %s", logx.Redact(err.Error()))
	}

	// 5. Probes, metrics and listeners. Startup checks already passed, so the
	// aggregate starts ready; the runner re-verifies on its first tick.
	agg := health.New("db", "rpc", "version", "chain")
	agg.Set("db", nil)
	agg.Set("rpc", nil)
	agg.Set("version", nil)
	agg.Set("chain", nil)

	m := metrics.New(agg.Ready)
	runner := &health.Runner{
		Interval: cfg.ProbeInterval,
		Timeout:  cfg.ProbeTimeout,
		Agg:      agg,
		Checks: []health.Check{
			{Name: "db", Probe: health.DBProber(pool)},
			{Name: "rpc", Probe: health.RPCProber(ethClient, expectedChain)},
		},
		Observe: m.ObserveProbe,
	}

	// 5b. Indexer: startup-unique lease owner plus the header and log scanners,
	// both behind one coordinator (a single acquisition loop and a single
	// heartbeat, research R1). RPC outcomes feed only the indexer metrics;
	// readiness stays dependency-probe driven.
	if cfg.ChainID > math.MaxInt64 {
		ethClient.Close()
		pool.Close()
		return fail("startup failed (indexer): chain id %d exceeds the bigint column range", cfg.ChainID)
	}
	chainID := int64(cfg.ChainID)
	ownerID, err := indexer.NewOwnerID()
	if err != nil {
		ethClient.Close()
		pool.Close()
		return fail("startup failed (indexer): %s", logx.Redact(err.Error()))
	}
	lease, err := indexer.NewLease(pool, chainID, indexer.Params{OwnerID: ownerID})
	if err != nil {
		ethClient.Close()
		pool.Close()
		return fail("startup failed (indexer): %s", logx.Redact(err.Error()))
	}
	headerRPC := indexerRPC{client: ethClient, m: m}
	scanner, err := indexer.NewScanner(pool, headerRPC, lease, indexer.Config{
		StartHeight:  cfg.StartHeight,
		RPCTimeout:   cfg.IndexRPCTimeout,
		PollInterval: cfg.IndexPollInterval,
		RetryInitial: cfg.IndexRetryInitial,
		RetryMax:     cfg.IndexRetryMax,
	}, slog.Default())
	if err != nil {
		ethClient.Close()
		pool.Close()
		return fail("startup failed (indexer): %s", logx.Redact(err.Error()))
	}
	// The log scanner's frozen configuration identity (start block and
	// whitelist hash) is compared against the durable log_checkpoint row when
	// the loop starts: a mismatch refuses to scan and exits non-zero (FR-05).
	logScanner, err := indexer.NewLogScanner(pool, headerRPC, logRPC{client: ethClient, m: m}, lease, indexer.LogConfig{
		StartBlock:   cfg.LogStartHeight,
		Contracts:    cfg.LogContracts,
		ConfigHash:   cfg.LogConfigHash,
		BatchBlocks:  cfg.LogBatchBlocks,
		RPCTimeout:   cfg.IndexRPCTimeout,
		PollInterval: cfg.IndexPollInterval,
		RetryInitial: cfg.IndexRetryInitial,
		RetryMax:     cfg.IndexRetryMax,
		ResultLimit:  0, // count-based completeness verdict disabled until the provider annex (research R5)
	}, slog.Default())
	if err != nil {
		ethClient.Close()
		pool.Close()
		return fail("startup failed (log indexer): %s", logx.Redact(err.Error()))
	}

	srv := &http.Server{
		Handler:           health.NewServer(agg, m.Handler()).Handler(),
		ReadHeaderTimeout: cfg.ProbeTimeout,
	}
	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		ethClient.Close()
		pool.Close()
		return fail("startup failed (http listen): %s", logx.Redact(err.Error()))
	}
	startupCancel()

	observer := &indexerObserver{scanner: scanner, m: m, chainID: chainID}
	observer.observe()
	logObserver := &logObserver{scanner: logScanner, header: scanner, m: m, chainID: chainID}
	logObserver.observe()

	go runner.Run(runCtx)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(listener) }()
	indexerErr := make(chan error, 1)
	indexerDone := make(chan struct{})
	go func() {
		// The coordinator owns the only acquisition loop and heartbeat and
		// joins both serve loops before it returns (research R1).
		indexerErr <- indexer.RunPair(runCtx, lease, scanner.ServeLoop, logScanner.ServeLoop)
		close(indexerDone)
	}()
	fmt.Fprintf(stdout, "txharbor serve: listening on %s\n", listener.Addr())

	// Mirror both scanners' snapshots into metrics for as long as serve runs.
	// A scanner failure is terminal exactly like an HTTP server failure:
	// record both terminal states, then shut down with a non-zero exit code.
	metricTicker := time.NewTicker(indexerMetricInterval)
	defer metricTicker.Stop()

	exitCode := 0
serveLoop:
	for {
		select {
		case <-runCtx.Done():
			break serveLoop
		case err := <-serveErr:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				fmt.Fprintf(stderr, "txharbor serve: http server error: %s\n", logx.Redact(err.Error()))
				exitCode = 1
			}
			break serveLoop
		case err := <-indexerErr:
			observer.observe()    // capture the terminal state before teardown
			logObserver.observe() // both loops are joined by the coordinator
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				fmt.Fprintf(stderr, "txharbor serve: indexer stopped: %s\n", logx.Redact(err.Error()))
				exitCode = 1
			}
			break serveLoop
		case <-metricTicker.C:
			observer.observe()
			logObserver.observe()
		}
	}
	cancel() // stop probe loop and indexer before releasing resources

	// 6. Shutdown: stop accepting work, let the indexer exit, then release
	// resources, all sharing one 15s budget. Budget exhaustion is recorded and
	// exits non-zero.
	if err := runShutdown(context.Background(), cfg.ShutdownTimeout,
		func(shCtx context.Context) error { return srv.Shutdown(shCtx) },
		func(shCtx context.Context) error {
			select {
			case <-indexerDone:
				return nil
			case <-shCtx.Done():
				return shCtx.Err()
			}
		},
		func(context.Context) error { ethClient.Close(); return nil },
		func(context.Context) error { pool.Close(); return nil },
	); err != nil {
		fmt.Fprintf(stderr, "txharbor serve: shutdown error: %s\n", logx.Redact(err.Error()))
		exitCode = 1
	}
	return exitCode
}

// indexerMetricInterval is how often scanner state and checkpoint snapshots
// are mirrored into metrics; short enough to observe the default 1s poll and
// 200ms initial backoff windows.
const indexerMetricInterval = 100 * time.Millisecond

// indexerRPC decorates the chain client with txharbor_indexer_rpc_total
// bookkeeping. Successful reads carry no failure class and are not counted;
// not-found is the wait polarity and is counted as ok, every other classified
// failure as error (contracts/observability.md).
type indexerRPC struct {
	client indexer.HeaderClient
	m      *metrics.Metrics
}

func (r indexerRPC) ChainID(ctx context.Context) (*big.Int, error) {
	id, err := r.client.ChainID(ctx)
	r.observe(err)
	return id, err
}

func (r indexerRPC) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	header, err := r.client.HeaderByNumber(ctx, number)
	r.observe(err)
	return header, err
}

func (r indexerRPC) observe(err error) {
	if err == nil {
		return
	}
	kind := eth.KindOf(err)
	if kind == "" {
		kind = eth.KindInvalidResponse
	}
	r.m.ObserveIndexerRPC(string(kind), kind == eth.KindNotFound)
}

// logRPC decorates eth.Client.FilterLogs with txharbor_log_rpc_total
// bookkeeping (contracts/observability.md). Successful calls carry no failure
// class and are not counted; not-found is the wait polarity and is counted as
// ok, every other classified failure as error.
type logRPC struct {
	client *eth.Client
	m      *metrics.Metrics
}

func (r logRPC) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	logs, err := r.client.FilterLogs(ctx, q)
	if err != nil {
		kind := eth.KindOf(err)
		if kind == "" {
			kind = eth.KindInvalidResponse
		}
		r.m.ObserveLogRPC(string(kind), kind == eth.KindNotFound)
	}
	return logs, err
}

// indexerObserver mirrors the scanner's snapshot-only state and checkpoint
// into the metrics registry: it is sampled on a ticker and once more when the
// scanner stops. Pauses increment txharbor_indexer_pause_total on the
// transition into the paused state.
type indexerObserver struct {
	scanner *indexer.Scanner
	m       *metrics.Metrics
	chainID int64
	last    indexer.State
	sampled bool
}

func (o *indexerObserver) observe() {
	state := o.scanner.State()
	if state == indexer.StatePaused && (!o.sampled || o.last != indexer.StatePaused) {
		o.m.ObserveIndexerPause(o.chainID)
	}
	o.last, o.sampled = state, true
	o.m.ObserveIndexerState(o.chainID, int(state))
	height, _, ok := o.scanner.Checkpoint()
	o.m.ObserveIndexerCheckpoint(o.chainID, height, ok)
}

// logObserver mirrors the log scanner's snapshot-only state, checkpoint and
// lag into the metrics registry next to the header observer; both are sampled
// on the same ticker and once more when the coordinator stops. Lag is 002's
// checkpoint height minus (log next block - 1); the series is absent while
// either checkpoint is empty (contracts/observability.md). Pauses increment
// txharbor_log_pause_total on the transition into the paused state.
type logObserver struct {
	scanner *indexer.LogScanner
	header  *indexer.Scanner
	m       *metrics.Metrics
	chainID int64
	last    indexer.State
	sampled bool
}

func (o *logObserver) observe() {
	state := o.scanner.State()
	if state == indexer.StatePaused && (!o.sampled || o.last != indexer.StatePaused) {
		o.m.ObserveLogPause(o.chainID)
	}
	o.last, o.sampled = state, true
	o.m.ObserveLogState(o.chainID, int(state))

	next, logOK := o.scanner.Checkpoint()
	o.m.ObserveLogCheckpointNext(o.chainID, next, logOK)
	height, _, headerOK := o.header.Checkpoint()
	if !logOK || !headerOK || next == 0 {
		o.m.ObserveLogLag(o.chainID, 0, false)
		return
	}
	done := next - 1
	lag := uint64(0)
	if height > done {
		lag = height - done
	}
	o.m.ObserveLogLag(o.chainID, lag, true)
}

// runShutdown executes steps in order under one shared budget. Remaining
// steps are still attempted after the budget expires so already-allocated
// resources are released before the process exits (FR-014).
func runShutdown(parent context.Context, budget time.Duration, steps ...func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	var firstErr error
	for i, step := range steps {
		if err := step(ctx); err != nil && firstErr == nil {
			if ctx.Err() != nil {
				firstErr = fmt.Errorf("shutdown budget %s exceeded at step %d: %w", budget, i+1, ctx.Err())
			} else {
				firstErr = fmt.Errorf("shutdown step %d failed: %w", i+1, err)
			}
		}
	}
	return firstErr
}
