// Package app wires the lifecycle: configuration gate, startup chain, probe
// loop and signal-driven shutdown. Commands stay thin wrappers around it so
// behavior is unit-testable without spawning processes.
package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/health"
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

	go runner.Run(runCtx)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(listener) }()
	fmt.Fprintf(stdout, "txharbor serve: listening on %s\n", listener.Addr())

	exitCode := 0
	select {
	case <-runCtx.Done():
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(stderr, "txharbor serve: http server error: %s\n", logx.Redact(err.Error()))
			exitCode = 1
		}
	}
	cancel() // stop probe loop before releasing resources

	// 6. Shutdown: stop accepting work first, then release resources, all
	// sharing one 15s budget. Budget exhaustion is recorded and exits non-zero.
	if err := runShutdown(context.Background(), cfg.ShutdownTimeout,
		func(shCtx context.Context) error { return srv.Shutdown(shCtx) },
		func(context.Context) error { ethClient.Close(); return nil },
		func(context.Context) error { pool.Close(); return nil },
	); err != nil {
		fmt.Fprintf(stderr, "txharbor serve: shutdown error: %s\n", logx.Redact(err.Error()))
		exitCode = 1
	}
	return exitCode
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
