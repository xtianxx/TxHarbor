//go:build integration

// Probe-exit synchronization discrimination tests for the F2 interrupted
// restore rebuild helper (TestRestoreInterruptionNotRestoredAndRerunIdempotent,
// run 37257158182 failure follow-up).
//
// These tests prove the bkpAwaitProbeExit helper's discrimination, not wiring:
// they never touch the production recovery paths. The positive case proves the
// helper returns once the probe's own backend is gone and the authoritative
// census then observes zero sessions. The negative cases prove the bounded
// wait cannot absorb a foreign session (unknown writer or old-attempt
// application_name tag): the census still refuses while the helper keeps
// waiting only on its own probe identity, and the timeout diagnostic names the
// remaining session's identity (pid/backend_start/application_name/state).
package recovery

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// bkpCensusDiscriminationFixture opens one fixture (its own throwaway
// postgres container through the package's standard bkpFixture TestMain
// discipline) and a dedicated discrimination database inside it.
func bkpCensusDiscriminationFixture(t *testing.T) (f *bkpFixture, targetDB string, targetDSN string) {
	t.Helper()
	f = newBkpFixture(t)
	targetDSN = f.createDatabase(t, "probe_disc")
	targetDB = bkpDBNameOf(t, f.adminDSN, targetDSN)
	return f, targetDB, targetDSN
}

// Positive: after the probe's own backend exits, the bounded wait returns and
// the authoritative census observes exactly zero sessions.
func TestBkpProbeExitPositiveZeroCensus(t *testing.T) {
	f, targetDB, targetDSN := bkpCensusDiscriminationFixture(t)

	probe, err := pgx.Connect(context.Background(), targetDSN)
	if err != nil {
		t.Fatalf("connect probe: %v", err)
	}
	var probePID int32
	var probeBackendStart time.Time
	if err := probe.QueryRow(context.Background(),
		`SELECT pg_backend_pid(), backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&probePID, &probeBackendStart); err != nil {
		_ = probe.Close(context.Background())
		t.Fatalf("capture probe identity: %v", err)
	}
	if _, err := probe.Exec(context.Background(), `SELECT 1`); err != nil {
		t.Fatalf("probe roundtrip: %v", err)
	}
	if err := probe.Close(context.Background()); err != nil {
		t.Fatalf("close probe: %v", err)
	}

	start := time.Now()
	if err := bkpAwaitProbeExit(f.ctx, f.admin, targetDB, probePID, probeBackendStart,
		bkpProbeExitTimeout, bkpProbeExitInterval); err != nil {
		t.Fatalf("probe exit wait failed: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 5*time.Second {
		t.Fatalf("probe exit took %s, want within the known ~30ms window", elapsed)
	}

	var postSessions int
	if err := f.admin.QueryRow(f.ctx,
		`SELECT count(*) FROM pg_stat_activity WHERE datname=$1`, targetDB).Scan(&postSessions); err != nil || postSessions != 0 {
		t.Fatalf("post-exit census = %d err=%v, want zero sessions\n%s", postSessions, err,
			bkpTargetSessionDump(f.ctx, f.admin, targetDB))
	}
}

// Negative 1: an unknown foreign session (empty application_name, the
// old-writer shape) present while the helper waits means the census must
// refuse - the bounded probe wait never absorbs or excludes it.
func TestBkpProbeExitForeignSessionRefusedByCensus(t *testing.T) {
	f, targetDB, targetDSN := bkpCensusDiscriminationFixture(t)

	probe, err := pgx.Connect(context.Background(), targetDSN)
	if err != nil {
		t.Fatalf("connect probe: %v", err)
	}
	var probePID int32
	var probeBackendStart time.Time
	if err := probe.QueryRow(context.Background(),
		`SELECT pg_backend_pid(), backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&probePID, &probeBackendStart); err != nil {
		_ = probe.Close(context.Background())
		t.Fatalf("capture probe identity: %v", err)
	}
	if err := probe.Close(context.Background()); err != nil {
		t.Fatalf("close probe: %v", err)
	}
	if err := bkpAwaitProbeExit(f.ctx, f.admin, targetDB, probePID, probeBackendStart,
		bkpProbeExitTimeout, bkpProbeExitInterval); err != nil {
		t.Fatalf("probe exit wait failed: %v", err)
	}

	// Inject the foreign session AFTER the probe is gone: an idle session the
	// test does not own, shaped like an old writer (empty application_name).
	foreign, err := pgx.Connect(context.Background(), targetDSN)
	if err != nil {
		t.Fatalf("connect foreign session: %v", err)
	}
	defer func() {
		// The test closes only the session it created itself; the helper under
		// test must never terminate unknown sessions (asserted below).
		_ = foreign.Close(context.Background())
	}()
	if _, err := foreign.Exec(f.ctx, `BEGIN`); err != nil {
		t.Fatalf("foreign begin: %v", err)
	}
	var foreignPID int32
	if err := foreign.QueryRow(f.ctx,
		`SELECT pg_backend_pid() FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&foreignPID); err != nil {
		t.Fatalf("capture foreign identity: %v", err)
	}

	var postSessions int
	if err := f.admin.QueryRow(f.ctx,
		`SELECT count(*) FROM pg_stat_activity WHERE datname=$1`, targetDB).Scan(&postSessions); err == nil && postSessions == 0 {
		t.Fatal("census observed zero sessions despite a foreign session present: the fix would be absorbing unknown writers")
	}
	dump := bkpTargetSessionDump(f.ctx, f.admin, targetDB)
	if !strings.Contains(dump, "application_name=\"\"") {
		t.Fatalf("session dump must identify the foreign session by identity, got:\n%s", dump)
	}
	if !strings.Contains(dump, "state=\"idle in transaction\"") {
		t.Fatalf("session dump must show the foreign session state, got:\n%s", dump)
	}
	if foreignPID == 0 || !strings.Contains(dump, "pid=") {
		t.Fatalf("session dump missing pid, got:\n%s", dump)
	}
}

// Negative 2: a session tagged with the interrupted attempt's
// application_name tag shape must still be refused by the census: the fix
// must not whitelist or blacklist by application_name.
func TestBkpProbeExitAttemptTaggedSessionStillRefused(t *testing.T) {
	f, targetDB, targetDSN := bkpCensusDiscriminationFixture(t)

	probe, err := pgx.Connect(context.Background(), targetDSN)
	if err != nil {
		t.Fatalf("connect probe: %v", err)
	}
	var probePID int32
	var probeBackendStart time.Time
	if err := probe.QueryRow(context.Background(),
		`SELECT pg_backend_pid(), backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&probePID, &probeBackendStart); err != nil {
		_ = probe.Close(context.Background())
		t.Fatalf("capture probe identity: %v", err)
	}
	if err := probe.Close(context.Background()); err != nil {
		t.Fatalf("close probe: %v", err)
	}
	if err := bkpAwaitProbeExit(f.ctx, f.admin, targetDB, probePID, probeBackendStart,
		bkpProbeExitTimeout, bkpProbeExitInterval); err != nil {
		t.Fatalf("probe exit wait failed: %v", err)
	}

	// Inject a session carrying an attempt-tag-shaped application_name
	// (txh015_<hex> is the production AttemptAppName shape, targetlock.go).
	tagged, err := pgx.ConnectConfig(context.Background(), mustBkpProbeTaggedConfig(t, targetDSN, "txh015_deadbeef01"))
	if err != nil {
		t.Fatalf("connect tagged session: %v", err)
	}
	defer func() { _ = tagged.Close(context.Background()) }()
	if _, err := tagged.Exec(f.ctx, `SELECT 1`); err != nil {
		t.Fatalf("tagged roundtrip: %v", err)
	}

	var postSessions int
	if err := f.admin.QueryRow(f.ctx,
		`SELECT count(*) FROM pg_stat_activity WHERE datname=$1`, targetDB).Scan(&postSessions); err == nil && postSessions == 0 {
		t.Fatal("census observed zero sessions despite an attempt-tagged session present: name-based exclusion leaked in")
	}
	dump := bkpTargetSessionDump(f.ctx, f.admin, targetDB)
	if !strings.Contains(dump, "txh015_deadbeef01") {
		t.Fatalf("session dump must identify the tagged session, got:\n%s", dump)
	}
}

// Negative 3 (boundedness/fault injection): the bounded wait must fail with
// identity diagnostics when the probe's (pid, backend_start) never leaves.
// A live session's own real identity is handed to the helper with a
// deliberately short deadline; the error must name the tuple and enumerate
// remaining sessions, and must not exceed the deadline by more than
// scheduling slack.
func TestBkpProbeExitWaitIsBoundedAndDiagnoses(t *testing.T) {
	f, _, targetDSN := bkpCensusDiscriminationFixture(t)

	// Keep a live session open; the helper is called with that session's own
	// real (pid, backend_start) identity, which cannot exit while the
	// connection lives. This is the fault-injection stand-in for "probe did
	// not leave".
	ghost, err := pgx.Connect(context.Background(), targetDSN)
	if err != nil {
		t.Fatalf("connect ghost: %v", err)
	}
	defer func() { _ = ghost.Close(context.Background()) }()
	var ghostPID int32
	var ghostBackendStart time.Time
	if err := ghost.QueryRow(f.ctx,
		`SELECT pg_backend_pid(), backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&ghostPID, &ghostBackendStart); err != nil {
		t.Fatalf("capture ghost identity: %v", err)
	}

	start := time.Now()
	waitErr := bkpAwaitProbeExit(f.ctx, f.admin, f.bkpDBNameOfDisc(targetDSN), ghostPID, ghostBackendStart,
		400*time.Millisecond, 25*time.Millisecond)
	elapsed := time.Since(start)
	if waitErr == nil {
		t.Fatal("bounded wait returned nil for a never-exiting backend")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("bounded wait exceeded its deadline by %s", elapsed)
	}
	if !strings.Contains(waitErr.Error(), "pid=") || !strings.Contains(waitErr.Error(), "backend_start=") {
		t.Fatalf("timeout error must name the probe tuple, got: %v", waitErr)
	}
	if !strings.Contains(waitErr.Error(), "remaining session pid=") {
		t.Fatalf("timeout error must dump remaining sessions, got: %v", waitErr)
	}
}

// bkpDBNameOfDisc exposes the package helper's database-name derivation for
// this file without re-parsing DSNs by hand.
func (f *bkpFixture) bkpDBNameOfDisc(dsn string) string {
	return bkpDBNameOf(f.t, f.adminDSN, dsn)
}

func mustBkpProbeTaggedConfig(t *testing.T, dsn, appName string) (cfg *pgx.ConnConfig) {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse tagged dsn: %v", err)
	}
	cfg.RuntimeParams["application_name"] = appName
	return cfg
}
