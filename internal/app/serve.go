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

	// 006 recovery depth (FR-03/Q1): refuse before opening any listener or
	// connection. The raw string rides cfg unvalidated so a missing/zero/
	// negative/non-integer/out-of-representation depth refuses here, on the
	// same fail() path as every other startup failure.
	if _, err := indexer.ParseReorgMaxDepth(cfg.ReorgMaxDepthRaw); err != nil {
		return fail("startup failed (recovery): invalid %s %q: %s", config.EnvReorgMaxDepth, cfg.ReorgMaxDepthRaw, logx.Redact(err.Error()))
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
	// The deposit scanner consumes the indexed log stream (004): its frozen
	// configuration identity is compared against the durable deposit rows
	// when the loop starts, and a mismatch refuses to scan and exits
	// non-zero, exactly like the 002/003 scanners above. Deposit
	// authorization itself stays a privileged out-of-loop SQL operation.
	depositScanner, err := indexer.NewDepositScanner(pool, indexer.DepositConfig{
		ChainID:        chainID,
		StartBlock:     cfg.DepositStartHeight,
		Assets:         cfg.DepositContracts,
		Watches:        cfg.DepositWatchAddresses,
		ConfigHash:     cfg.DepositConfigHash,
		BatchBlocks:    cfg.DepositBatchBlocks,
		PollInterval:   cfg.IndexPollInterval,
		RetryInitial:   cfg.IndexRetryInitial,
		RetryMax:       cfg.IndexRetryMax,
		LogContracts:   cfg.LogContracts,
		LogConfigHash:  cfg.LogConfigHash,
		LogStartHeight: cfg.LogStartHeight,
	})
	if err != nil {
		ethClient.Close()
		pool.Close()
		return fail("startup failed (deposit indexer): %s", logx.Redact(err.Error()))
	}
	depositScanner.SetResultObserver(func(result string) {
		m.ObserveDepositObservation(chainID, result)
	})
	depositScanner.SetPauseObserver(func() {
		m.ObserveDepositPause(chainID)
	})
	// The confirmation scanner converts eligible Pending rows (005): its
	// frozen threshold N is compared against the effective policy row when
	// the loop starts, and a mismatch refuses to scan and exits non-zero,
	// exactly like the deposit scanner above. Confirmation authorization
	// itself stays a privileged out-of-loop SQL operation.
	confirmCfg := indexer.ConfirmationConfig{
		ChainID:      chainID,
		ThresholdN:   cfg.ConfirmationDepth,
		PollInterval: cfg.IndexPollInterval,
		RetryInitial: cfg.IndexRetryInitial,
		RetryMax:     cfg.IndexRetryMax,
	}
	confirmCommitter, err := indexer.NewConfirmationCommitter(pool, confirmCfg)
	if err != nil {
		ethClient.Close()
		pool.Close()
		return fail("startup failed (confirmation indexer): %s", logx.Redact(err.Error()))
	}
	confirmationScanner, err := indexer.NewConfirmationScanner(pool, confirmCfg, confirmCommitter, m)
	if err != nil {
		ethClient.Close()
		pool.Close()
		return fail("startup failed (confirmation indexer): %s", logx.Redact(err.Error()))
	}
	// The recovery executor (006) joins the same coordinator as the fifth
	// stream: same lease acquisition loop, same heartbeat, same ServeFunc
	// shape. Timing reuses the INDEX triple and the replay cap comes from
	// config (research R5 — no new knob names); the depth already refused
	// above, and NewRecoveryLoop re-validates without I/O. Conversions and
	// evidence waits count into the scraped registry through the adapter;
	// gauges ride the recoveryObserver below, derived from the durable row.
	recoveryServe, err := indexer.NewRecoveryLoopWithMetrics(pool, lease, headerRPC, logRPC{client: ethClient, m: m}, buildRecoveryConfig(cfg, chainID), recoveryMetricsAdapter{m: m})
	if err != nil {
		ethClient.Close()
		pool.Close()
		return fail("startup failed (recovery): %s", logx.Redact(err.Error()))
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
	depositObserver := &depositObserver{deposit: depositScanner, log: logScanner, m: m, chainID: chainID}
	depositObserver.observe()
	confirmationObserver := &confirmationObserver{confirm: confirmationScanner, m: m, chainID: chainID}
	confirmationObserver.observe()
	// Recovery state rides the same sample points through the approved
	// readers, never through readiness: see recoveryObserver.
	recoveryRead := func(ctx context.Context) (indexer.RecoveryState, indexer.Validity, bool) {
		row, err := indexer.LoadRecoveryState(ctx, pool, chainID)
		if err != nil {
			slog.Warn("recovery state read failed; keeping last observed",
				"error", logx.Redact(err.Error()))
			return "", "", false
		}
		var released bool
		if row == nil {
			released, err = indexer.RecoveryReleased(ctx, pool, chainID)
			if err != nil {
				slog.Warn("recovery release read failed; keeping last observed",
					"error", logx.Redact(err.Error()))
				return "", "", false
			}
		}
		var height int64
		if h, _, ok := scanner.Checkpoint(); ok {
			height = int64(h)
		}
		state, validity := indexer.AnnotateRecoveryHeight(row, released, height)
		return state, validity, true
	}
	recoveryObserver := &recoveryObserver{read: recoveryRead}
	recoveryObserver.observe(runCtx)
	observeRecoveryMetrics := func(ctx context.Context) {
		recoveryObserver.observeMetrics(ctx, m, chainID, func(ctx context.Context) (indexer.RecoverySnapshot, error) {
			return indexer.LoadRecoverySnapshot(ctx, pool, chainID)
		})
	}
	observeRecoveryMetrics(runCtx)

	go runner.Run(runCtx)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(listener) }()
	indexerErr := make(chan error, 1)
	indexerDone := make(chan struct{})
	go func() {
		// The coordinator owns the only acquisition loop and heartbeat and
		// joins all five serve loops before it returns (research R1). The
		// deposit, confirmation and recovery loops adapt to the shared
		// ServeFunc shape with the coordinator-held lease; authorization
		// stays out of loop.
		depositServe := func(loopCtx context.Context, checkLost func() error) error {
			return depositScanner.ServeLoop(loopCtx, lease, checkLost)
		}
		confirmServe := func(loopCtx context.Context, checkLost func() error) error {
			return confirmationScanner.ServeLoop(loopCtx, lease, checkLost)
		}
		indexerErr <- runServiceStreams(runCtx, lease, scanner.ServeLoop, logScanner.ServeLoop, depositServe, confirmServe, recoveryServe)
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
			logObserver.observe() // all loops are joined by the coordinator
			depositObserver.observe()
			confirmationObserver.observe()
			recoveryObserver.observe(runCtx)
			observeRecoveryMetrics(runCtx)
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				fmt.Fprintf(stderr, "txharbor serve: indexer stopped: %s\n", logx.Redact(err.Error()))
				exitCode = 1
			}
			break serveLoop
		case <-metricTicker.C:
			observer.observe()
			logObserver.observe()
			depositObserver.observe()
			confirmationObserver.observe()
			recoveryObserver.observe(runCtx)
			observeRecoveryMetrics(runCtx)
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

// depositObserver mirrors the deposit scanner's progress and loop condition
// plus the log-vs-deposit lag into the metrics registry next to the header
// and log observers; all three are sampled on the same ticker and once more
// when the coordinator stops. The lag series is absent while either progress
// is empty (contracts/observability.md). Pause events arrive through the
// pause hook, not sampling.
type depositObserver struct {
	deposit *indexer.DepositScanner
	log     *indexer.LogScanner
	m       *metrics.Metrics
	chainID int64
}

func (o *depositObserver) observe() {
	next, ok := o.deposit.DepositProgress()
	o.m.ObserveDepositNext(o.chainID, next, ok)
	o.m.ObserveDepositState(o.chainID, o.deposit.DepositState())

	logNext, logOK := o.log.Checkpoint()
	if !ok || !logOK {
		o.m.ObserveDepositLag(o.chainID, 0, false)
		return
	}
	var lag uint64
	if logNext > next {
		lag = logNext - next
	}
	o.m.ObserveDepositLag(o.chainID, lag, true)
}

// confirmationObserver mirrors the confirmation scanner's loop condition
// into the metrics registry next to the header, log and deposit observers;
// all four are sampled on the same ticker and once more when the
// coordinator stops. Per-tick pending/lag/policy gauges are pushed by the
// loop itself; the observer only re-samples the loop-condition state so the
// terminal verdict is captured before teardown.
type confirmationObserver struct {
	confirm *indexer.ConfirmationScanner
	m       *metrics.Metrics
	chainID int64
}

func (o *confirmationObserver) observe() {
	o.m.ObserveConfirmationState(o.chainID, o.confirm.ConfirmationState())
}

// buildRecoveryConfig maps the serve configuration onto the recovery
// executor: the raw Q1 depth plus the shared INDEX timing triple and the
// replay cap (research R5). Pure so lifecycle tests pin the mapping.
func buildRecoveryConfig(cfg *config.Config, chainID int64) indexer.RecoveryConfig {
	return indexer.RecoveryConfig{
		ChainID:          chainID,
		MaxDepthRaw:      cfg.ReorgMaxDepthRaw,
		PollInterval:     cfg.IndexPollInterval,
		RetryInitial:     cfg.IndexRetryInitial,
		RetryMax:         cfg.IndexRetryMax,
		ReplayHeights:    int(cfg.ReorgReplayBatch),
		BlockStartHeight: cfg.StartHeight,
	}
}

// coordinatorLease is the acquisition/heartbeat surface the five-stream
// service needs; *indexer.Lease implements it, lifecycle tests substitute
// a scripted fake.
type coordinatorLease interface {
	Acquire(ctx context.Context) (bool, int64, error)
	Heartbeat(ctx context.Context) error
}

// runServiceStreams starts the four ordinary loops plus the recovery loop
// under the single shared lease acquisition loop and heartbeat (006
// T017a): it is indexer.RunQuatroPlusRecovery with the serve-path argument
// order fixed, so lifecycle tests drive the startup-path wiring with fakes
// instead of re-testing the coordinator itself.
func runServiceStreams(ctx context.Context, lease coordinatorLease, header, log, deposit, confirm, recovery indexer.ServeFunc) error {
	return indexer.RunQuatroPlusRecovery(ctx, lease, header, log, deposit, confirm, recovery)
}

// recoveryMetricsAdapter funnels the executor's exact conversion and
// evidence-wait counts into the scraped registry (T032). Orphaned arrives
// with the idempotent transaction's own rowcount, so a bulk sweep counts
// once no matter how large; revived arrives per real conversion only.
type recoveryMetricsAdapter struct {
	m *metrics.Metrics
}

func (a recoveryMetricsAdapter) AddReorgOrphaned(chain int64, n int64) {
	for i := int64(0); i < n; i++ {
		a.m.ObserveReorgOrphaned(chain)
	}
}

func (a recoveryMetricsAdapter) AddReorgRevived(chain int64, n int64) {
	for i := int64(0); i < n; i++ {
		a.m.ObserveReorgRevived(chain)
	}
}

func (a recoveryMetricsAdapter) AddReorgEvidenceWait(chain int64, class string) {
	a.m.ObserveReorgEvidenceWait(chain, class)
}

// recoveryObserver samples the durable recovery row through the approved
// readers (LoadRecoveryState/AnnotateRecoveryHeight): recovery activity,
// chain pauses, and result validity stay three separate signals — this
// observer only logs recovery-state transitions and never touches the
// readiness aggregate, so an active recovery cannot flip service readiness
// by itself. Read failures keep the last observed state for the next tick.
//
// observeMetrics mirrors the same row into the T032 gauges (active,
// depth/bound, per-stream frontier lag, reconcile flag) through one
// LoadRecoverySnapshot read per call: Set/Delete only, so repeat ticks
// with no state change rewrite identical values and never inflate a
// counter. A read failure keeps every gauge at its last value (never
// zeroed into fake idle); a clean read with no row clears active,
// reconcile and frontier lags (release leaves no active=1 behind) while
// depth/bound keep their last known values. Counters are executor-owned
// and never touched here, so polling cannot double-count a conversion.
type recoveryObserver struct {
	read         func(ctx context.Context) (indexer.RecoveryState, indexer.Validity, bool)
	last         indexer.RecoveryState
	lastValidity indexer.Validity
	sampled      bool
}

func (o *recoveryObserver) observe(ctx context.Context) {
	state, validity, ok := o.read(ctx)
	if !ok {
		return
	}
	if !o.sampled || state != o.last || validity != o.lastValidity {
		slog.Info("recovery state",
			"recovery_state", string(state),
			"validity", string(validity))
	}
	o.last, o.lastValidity, o.sampled = state, validity, true
}

// observeMetrics snapshots the durable recovery state into m. load is the
// point read (production: LoadRecoverySnapshot over the serve pool;
// tests: scripted). A load error leaves every gauge untouched.
func (o *recoveryObserver) observeMetrics(ctx context.Context, m *metrics.Metrics, chainID int64, load func(ctx context.Context) (indexer.RecoverySnapshot, error)) {
	if m == nil || load == nil {
		return
	}
	snap, err := load(ctx)
	if err != nil {
		slog.Warn("recovery metrics read failed; keeping last observed",
			"error", logx.Redact(err.Error()))
		return
	}
	if snap.Row == nil {
		m.ObserveReorgActive(chainID, false)
		m.ObserveReorgReconcile(chainID, false)
		for _, stream := range []string{"block", "log", "deposit"} {
			m.ObserveReorgFrontierLag(chainID, stream, 0, false)
		}
		return
	}
	m.ObserveReorgActive(chainID, true)
	m.ObserveReorgReconcile(chainID, snap.Reconcile)
	if snap.Depth == nil {
		m.ObserveReorgDepthPending(chainID, snap.Bound)
	} else {
		m.ObserveReorgDepthBound(chainID, *snap.Depth, snap.Bound)
	}
	frontiers := map[string]*int64{
		"block":   snap.Row.BlockFrontier,
		"deposit": snap.Row.DepositFrontier,
		"log":     snap.Row.LogFrontier,
	}
	for _, stream := range []string{"block", "log", "deposit"} {
		f := frontiers[stream]
		if f == nil || snap.SweepEnd == nil {
			m.ObserveReorgFrontierLag(chainID, stream, 0, false)
			continue
		}
		var lag uint64
		if *snap.SweepEnd > *f {
			lag = uint64(*snap.SweepEnd - *f)
		}
		m.ObserveReorgFrontierLag(chainID, stream, lag, true)
	}
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
