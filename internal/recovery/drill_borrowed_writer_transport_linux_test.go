//go:build linux && drill

// drill_borrowed_writer_transport_linux_test.go is the non-authorizing
// borrowed-writer transport lane through the protected origin gate. The
// concrete coordinator run (DrillNewBorrowedWriterRun) drives the real native
// pg_restore child into the gate's EXACT factory endpoint; the gate admits it
// through the borrowed adapter (identity prefix + immutable original binding +
// native backend catalog/incarnation evidence) and holds the first executable
// frame until registration, the first watcher pass and the borrowed
// pre-release check all succeed. Nothing here creates acceptance/restored
// authority: the Probe deliberately rejects and no acceptance ever runs.
package recovery_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
)

// borrowedGateFixture is the private borrowed-writer gate fixture: one
// protected origin-gate fixture cluster carrying a fresh pristine target
// database, an owned source database with a real custom archive, a dedicated
// control database/store/instance identity, a real borrowed *TargetLock on the
// ORIGINAL target key, and the factory run plus fresh identity prefix.
type borrowedGateFixture struct {
	fx        *originGateFixture
	gate      *originGate
	run       *recovery.DrillBorrowedWriterRun
	prefix    *borrowedIdentityPrefix
	lock      *recovery.TargetLock
	tools     recovery.DrillNativePGTools
	targetDSN string
	targetDB  string
	table     string
	operation string
	guardKey  string

	controlPool     *pgxpool.Pool
	targetAdmin     *pgx.Conn
	archive         *os.File
	probeCalls      int32
	acceptanceCalls int32
}

type borrowedGateRunOutcome struct {
	result  recovery.TargetWriterResult
	receipt recovery.DrillTargetProcessReceipt
	err     error
}

func newBorrowedGateFixture(t *testing.T) *borrowedGateFixture {
	t.Helper()
	ctx := context.Background()
	fx := newOriginGateFixture(t)
	gate := newOriginGate(t, ctx, fx)

	strictHelper := buildBorrowedIdentityStrictHelper(t)
	if err := fx.container.CopyFileToContainer(ctx, strictHelper, borrowedIdentityStrictHelperPath, 0o700); err != nil {
		t.Fatalf("copy strict namespace helper into the gate fixture: %v", err)
	}

	nano := time.Now().UnixNano()
	sourceDB := fmt.Sprintf("borrowed_gate_source_%d", nano)
	targetDB := fmt.Sprintf("borrowed_gate_target_%d", nano)
	controlDB := fmt.Sprintf("borrowed_gate_ctrl_%d", nano)
	for _, name := range []string{sourceDB, targetDB, controlDB} {
		if err := fx.createOwnedDatabase(ctx, name); err != nil {
			t.Fatalf("create owned database %s: %v", name, err)
		}
	}
	sourceDSN := borrowedIdentityRoute(t, ctx, fx, sourceDB)
	targetDSN := borrowedIdentityRoute(t, ctx, fx, targetDB)
	controlDSN := borrowedIdentityRoute(t, ctx, fx, controlDB)
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: controlDSN, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS,
	}, io.Discard); err != nil {
		t.Fatalf("migrate borrowed gate control database: %v", err)
	}
	controlPool, err := pgxpool.New(ctx, controlDSN)
	if err != nil {
		t.Fatalf("open borrowed gate control pool: %v", err)
	}
	t.Cleanup(controlPool.Close)
	store, err := controlstore.NewStore(ctx, controlPool)
	if err != nil {
		t.Fatalf("borrowed gate control store: %v", err)
	}
	target, err := controlstore.ParseDSNTarget(targetDSN)
	if err != nil {
		t.Fatalf("parse borrowed gate target identity: %v", err)
	}
	targetKey, err := recovery.CanonicalTargetKey(target)
	if err != nil {
		t.Fatalf("borrowed gate target key: %v", err)
	}
	lock, err := recovery.AcquireTargetLock(ctx, controlDSN, targetKey, 3*time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire borrowed gate lock on the original target key: %v", err)
	}
	t.Cleanup(func() { _ = lock.Release(context.Background()) })

	// The target database must be provably pristine as a WHOLE fresh catalog
	// (not just one requested relation absent) BEFORE any guard INIT, factory
	// run or probe. The check is read-only and takes no caller "pristine bool".
	table := fmt.Sprintf("borrowed_gate_rows_%d", nano)
	targetAdmin, err := pgx.Connect(ctx, targetDSN)
	if err != nil {
		t.Fatalf("connect pristine borrowed gate target: %v", err)
	}
	t.Cleanup(func() { _ = targetAdmin.Close(context.Background()) })
	pristineDigest, err := verifyBorrowedGatePristineCatalog(ctx, targetAdmin, targetDB, fx.role)
	if err != nil {
		t.Fatalf("borrowed gate target is not a pristine fresh owned catalog: %v", err)
	}

	// Real custom archive from an owned source database with exactly three rows.
	sourceAdmin, err := pgx.Connect(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("connect owned source database: %v", err)
	}
	t.Cleanup(func() { _ = sourceAdmin.Close(context.Background()) })
	if _, err := sourceAdmin.Exec(ctx, `CREATE TABLE public.`+pgx.Identifier{table}.Sanitize()+` (ref text NOT NULL)`); err != nil {
		t.Fatalf("create owned source table: %v", err)
	}
	for row := 1; row <= 3; row++ {
		if _, err := sourceAdmin.Exec(ctx, `INSERT INTO public.`+pgx.Identifier{table}.Sanitize()+` (ref) VALUES ($1)`, fmt.Sprintf("row-%d", row)); err != nil {
			t.Fatalf("seed owned source row: %v", err)
		}
	}
	tools, err := recovery.DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed native PG18 tools: %v", err)
	}
	dumpPath, ok := tools.DumpPath()
	if !ok {
		t.Fatal("sealed dump tool is unavailable")
	}
	archivePath := filepath.Join(t.TempDir(), "borrowed-gate-source.dump")
	dump := exec.CommandContext(ctx, dumpPath, "--format=custom", "--no-owner", "--no-privileges", "--file="+archivePath, sourceDSN)
	if err := dump.Run(); err != nil {
		t.Fatalf("real custom source dump failed: %v", err)
	}
	archive, err := os.Open(archivePath)
	if err != nil {
		t.Fatalf("open real custom archive: %v", err)
	}
	t.Cleanup(func() { _ = archive.Close() })

	operation := fmt.Sprintf("borrowed-gate-%d", nano)
	if err := initBorrowedGateGuard(ctx, controlPool, targetKey.String(),
		target.DataTargetFingerprint().RoleFingerprint, "fixture-admin:"+fx.role, operation, pristineDigest); err != nil {
		t.Fatalf("borrowed gate guard inline-preserved init refused (an existing guard row must refuse, never be reset): %v", err)
	}

	observerDSN := targetDSN
	if parsed, err := url.Parse(observerDSN); err == nil {
		query := parsed.Query()
		query.Set("application_name", "borrowed_gate_observer")
		parsed.RawQuery = query.Encode()
		observerDSN = parsed.String()
	}
	fixture := &borrowedGateFixture{
		fx: fx, gate: gate, lock: lock, tools: tools,
		targetDSN: targetDSN, targetDB: targetDB, table: table, operation: operation,
		guardKey:    targetKey.String(),
		controlPool: controlPool, targetAdmin: targetAdmin, archive: archive,
	}
	run, err := recovery.DrillNewBorrowedWriterRun(ctx, recovery.TargetWriterOptions{
		OperationKind: recovery.TargetWriterOperationRestore,
		Store:         store, ControlDSN: controlDSN, TargetDSN: targetDSN,
		ObserverDSN: observerDSN, TrustedTarget: target,
		OperationID: operation, Archive: archive,
		Probe: func(context.Context, recovery.TargetWriterProof) (recovery.TargetWriterProbeResult, error) {
			atomic.AddInt32(&fixture.probeCalls, 1)
			return recovery.TargetWriterProbeResult{}, fmt.Errorf("borrowed gate probe intentionally rejects")
		},
		Acceptance: func(context.Context, pgx.Tx, recovery.TargetWriterProof) (recovery.TargetWriterAcceptance, error) {
			atomic.AddInt32(&fixture.acceptanceCalls, 1)
			return recovery.TargetWriterAcceptance{}, fmt.Errorf("borrowed gate acceptance must never run")
		},
		QuiescenceTimeout: 5 * time.Second, QuiescenceInterval: 50 * time.Millisecond,
	}, lock, gate.endpoint, tools)
	if err != nil {
		t.Fatalf("borrowed gate run factory: %v", err)
	}
	fixture.run = run
	setup := newBorrowedIdentityCandidateSetup(t, fx, targetDSN)
	prefix, err := setup.capture(ctx, run)
	if err != nil {
		t.Fatalf("fresh identity prefix capture for the borrowed gate run: %v", err)
	}
	fixture.prefix = prefix
	if err := gate.UseBorrowedOrigin(run, prefix); err != nil {
		t.Fatalf("register borrowed origin on the gate: %v", err)
	}
	return fixture
}

func (f *borrowedGateFixture) waitFor(t *testing.T, timeout time.Duration, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (f *borrowedGateFixture) relationAbsent(t *testing.T) bool {
	t.Helper()
	var absent bool
	if err := f.targetAdmin.QueryRow(context.Background(), `SELECT to_regclass($1) IS NULL`, "public."+f.table).Scan(&absent); err != nil {
		t.Fatalf("target relation probe: %v", err)
	}
	return absent
}

func (f *borrowedGateFixture) rowCount(t *testing.T) int {
	t.Helper()
	var count int
	if err := f.targetAdmin.QueryRow(context.Background(), `SELECT count(*) FROM public.`+pgx.Identifier{f.table}.Sanitize()).Scan(&count); err != nil {
		t.Fatalf("target row count: %v", err)
	}
	return count
}

func (f *borrowedGateFixture) guardState(t *testing.T) string {
	t.Helper()
	var disposition string
	if err := f.controlPool.QueryRow(context.Background(), `SELECT disposition FROM recovery_target_guard WHERE target_guard_key=$1`, f.guardKey).Scan(&disposition); err != nil {
		t.Fatalf("target guard state read: %v", err)
	}
	return disposition
}

// TestDrillBorrowedWriterGateRestoresAndProbeRejects runs the genuine
// coordinator over the real native child through the gate. The first
// executable frame is held (target relation absent, zero effects) until
// registration, the first watcher pass and the borrowed pre-release check
// (fresh prefix + control health + native backend catalog/incarnation) all
// pass. Release restores the real archive (three rows); the Probe rejects
// exactly once and Acceptance never runs, so no restored/verified/evidence
// authority is created. The borrowed lock remains caller-held.
func TestDrillBorrowedWriterGateRestoresAndProbeRejects(t *testing.T) {
	f := newBorrowedGateFixture(t)
	ctx := context.Background()
	f.gate.HoldRegistration()
	outcome := make(chan borrowedGateRunOutcome, 1)
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go func() {
		result, receipt, err := f.run.Run(runCtx)
		outcome <- borrowedGateRunOutcome{result: result, receipt: receipt, err: err}
	}()
	admitCtx, cancelAdmit := context.WithTimeout(ctx, 90*time.Second)
	defer cancelAdmit()
	session, err := f.gate.AdmitBorrowed(admitCtx, f.run, f.prefix)
	if err != nil {
		cancelRun()
		t.Fatalf("borrowed admission: %v", err)
	}
	// Hold the actual first executable frame: the real server backend behind
	// BackendKeyData exists and the client's first executable frame is buffered.
	f.waitFor(t, 60*time.Second, "the held first executable frame", func() bool {
		return session.BackendPID() > 0 && session.BufferedFrames() > 0
	})
	if session.WatcherReady() {
		t.Fatal("gate released executable frames before registration release")
	}
	if !f.relationAbsent(t) {
		t.Fatal("target relation exists before the first executable frame is released")
	}
	f.gate.ReleaseRegistration()
	f.waitFor(t, 90*time.Second, "registration, watcher and borrowed pre-release checks", func() bool {
		return session.WatcherReady()
	})
	select {
	case got := <-outcome:
		if got.err == nil {
			t.Fatal("probe rejection did not fail the coordinator run")
		}
		facts, err := got.receipt.ConsumeFacts()
		if err != nil {
			t.Fatalf("frozen process receipt: %v", err)
		}
		if !facts.Started || !facts.Terminal {
			t.Fatalf("process receipt does not retain real started/terminal facts: %+v", facts)
		}
		if facts.Command.Outcome != recovery.PGCommandSucceeded || facts.Command.ExitCode != 0 || !facts.Command.ProcessGroupDrained {
			t.Fatalf("native child did not complete with a proven drain: %+v", facts.Command)
		}
		if facts.OperationID != f.operation || facts.RoleFingerprint != f.run.Binding().OriginalRoleFingerprint() || facts.TargetKey != f.run.Binding().OriginalTargetKey() {
			t.Fatal("process receipt lost the original target/role/operation binding")
		}
	case <-time.After(150 * time.Second):
		cancelRun()
		t.Fatal("coordinator run did not return after the probe rejection")
	}
	if atomic.LoadInt32(&f.probeCalls) != 1 || atomic.LoadInt32(&f.acceptanceCalls) != 0 {
		t.Fatalf("probe/acceptance counts are not the deliberate rejection: probe=%d acceptance=%d", f.probeCalls, f.acceptanceCalls)
	}
	if count := f.rowCount(t); count != 3 {
		t.Fatalf("real archive restore did not produce exactly three rows: %d", count)
	}
	if state := f.guardState(t); state == "clean" {
		t.Fatal("failed probe attempt left a clean target guard")
	}
	if err := f.lock.Health(ctx); err != nil {
		t.Fatalf("borrowed lock is no longer the caller-held original session: %v", err)
	}
}

// TestDrillBorrowedWriterGateCancelBeforeReleaseDrains cancels the admission
// and run contexts while the first executable frame is still held and
// registration is blocked: no executable frame is forwarded, the target
// relation never appears, the gate latches and authoritatively drains the real
// backend, the sole owner stops and reaps the child, and the one-use run and
// receipt are refused on replay.
func TestDrillBorrowedWriterGateCancelBeforeReleaseDrains(t *testing.T) {
	f := newBorrowedGateFixture(t)
	ctx := context.Background()
	f.gate.HoldRegistration()
	outcome := make(chan borrowedGateRunOutcome, 1)
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go func() {
		result, receipt, err := f.run.Run(runCtx)
		outcome <- borrowedGateRunOutcome{result: result, receipt: receipt, err: err}
	}()
	admitCtx, cancelAdmit := context.WithCancel(ctx)
	defer cancelAdmit()
	session, err := f.gate.AdmitBorrowed(admitCtx, f.run, f.prefix)
	if err != nil {
		cancelRun()
		t.Fatalf("borrowed admission: %v", err)
	}
	f.waitFor(t, 60*time.Second, "the held first executable frame", func() bool {
		return session.BackendPID() > 0 && session.BufferedFrames() > 0
	})
	if session.WatcherReady() {
		t.Fatal("gate released executable frames before registration release")
	}
	if !f.relationAbsent(t) {
		t.Fatal("target relation exists before the first executable frame is released")
	}
	cancelAdmit()
	cancelRun()
	f.waitFor(t, 30*time.Second, "the cancellation latch and authoritative backend drain", func() bool {
		latched, _ := f.gate.Latched()
		return latched && f.gate.DrainState().Verified
	})
	drain := f.gate.DrainState()
	if !drain.Verified || drain.Unknown {
		t.Fatalf("gate backend drain is not authoritatively verified: %+v", drain)
	}
	select {
	case got := <-outcome:
		if got.err == nil {
			t.Fatal("canceled coordinator run was reported as success")
		}
		facts, err := got.receipt.ConsumeFacts()
		if err != nil {
			t.Fatalf("frozen canceled receipt: %v", err)
		}
		if !facts.Started || !facts.Terminal || !facts.Command.ProcessGroupDrained {
			t.Fatalf("canceled native child has no stopped/reaped sole-wait facts: %+v", facts)
		}
		if _, err := got.receipt.ConsumeFacts(); err == nil {
			t.Fatal("process receipt replay was accepted")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("canceled coordinator run did not return")
	}
	if atomic.LoadInt32(&f.probeCalls) != 0 || atomic.LoadInt32(&f.acceptanceCalls) != 0 {
		t.Fatalf("canceled attempt ran probe/acceptance: probe=%d acceptance=%d", f.probeCalls, f.acceptanceCalls)
	}
	if !f.relationAbsent(t) {
		t.Fatal("canceled attempt created the target relation")
	}
	if _, _, err := f.run.Run(ctx); err == nil {
		t.Fatal("one-use borrowed run replay was accepted")
	}
	if state := f.guardState(t); state == "clean" {
		t.Fatal("canceled attempt left a clean target guard")
	}
}

// ---------------------------------------------------------------------------
// Fixture safety helpers: whole-catalog pristine proof and INSERT-only guard
// init with bound original key/role/admin provenance. Both return errors so
// negative tests can assert refusals; the fixture constructor turns an
// unexpected refusal into a fatal. Neither returns any clean/authority value.
// ---------------------------------------------------------------------------

// borrowedGateFixtureRefusal is the fixed safe refusal of the fixture safety
// helpers. reason is a constant phrase; no DSN, address or SQL text leaks.
type borrowedGateFixtureRefusal struct{ reason string }

func (e *borrowedGateFixtureRefusal) Error() string {
	return "borrowed gate fixture safety refused: " + e.reason
}

func borrowedGateFixtureRefuse(reason string) error {
	return &borrowedGateFixtureRefusal{reason: reason}
}

// verifyBorrowedGatePristineCatalog proves the whole fresh owned catalog is
// pristine: actual database name/OID/owner, no user relations of ANY relkind
// in ANY non-builtin namespace, and no non-default schemas. It refuses on any
// foreign object even when a requested application relation would be absent,
// and it accepts no caller "pristine" argument. The returned digest binds the
// verified initial facts into the guard provenance.
func verifyBorrowedGatePristineCatalog(ctx context.Context, conn *pgx.Conn, expectedDatabase, expectedOwner string) (string, error) {
	if ctx == nil || conn == nil || expectedDatabase == "" || expectedOwner == "" {
		return "", borrowedGateFixtureRefuse("pristine check inputs")
	}
	var (
		database    string
		databaseOID uint32
		owner       string
	)
	if err := conn.QueryRow(ctx, `
SELECT current_database(),
       (SELECT oid FROM pg_database WHERE datname = current_database()),
       pg_get_userbyid((SELECT datdba FROM pg_database WHERE datname = current_database()))`).
		Scan(&database, &databaseOID, &owner); err != nil {
		return "", borrowedGateFixtureRefuse("catalog identity unreadable")
	}
	if database != expectedDatabase || databaseOID == 0 {
		return "", borrowedGateFixtureRefuse("database identity mismatch")
	}
	if owner != expectedOwner {
		return "", borrowedGateFixtureRefuse("database owner mismatch")
	}
	// Any user relation of any relkind (table, partitioned table, view,
	// materialized view, sequence, index) in any non-builtin namespace refuses
	// pristine. pg_catalog/information_schema and toast/temp schemas are the
	// only built-ins allowed.
	rows, err := conn.Query(ctx, `
SELECT n.nspname, c.relkind, c.relname
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%'
LIMIT 1`)
	if err != nil {
		return "", borrowedGateFixtureRefuse("catalog relation scan unreadable")
	}
	defer rows.Close()
	if rows.Next() {
		return "", borrowedGateFixtureRefuse("foreign relation present in target catalog")
	}
	if err := rows.Err(); err != nil {
		return "", borrowedGateFixtureRefuse("catalog relation scan incomplete")
	}
	var extraSchemas int
	if err := conn.QueryRow(ctx, `
SELECT count(*) FROM pg_namespace
WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'public')
  AND nspname NOT LIKE 'pg_toast%' AND nspname NOT LIKE 'pg_temp%'`).Scan(&extraSchemas); err != nil {
		return "", borrowedGateFixtureRefuse("catalog schema scan unreadable")
	}
	if extraSchemas != 0 {
		return "", borrowedGateFixtureRefuse("non-default schema present in target catalog")
	}
	facts, err := json.Marshal(map[string]any{
		"database": database, "database_oid": databaseOID, "owner": owner,
		"user_relations": 0, "non_default_schemas": 0,
	})
	if err != nil {
		return "", borrowedGateFixtureRefuse("pristine facts unencodable")
	}
	sum := sha256.Sum256(facts)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// initBorrowedGateGuard inserts the single positively clean guard row the real
// coordinator requires for a verified pristine fresh target. It is INSERT-only:
// any pre-existing row (clean, unknown, rebuild_required, active writer or
// launch intent) refuses as a duplicate. There is no update, no SetGuard, no
// clear-active and no cleanup overwrite, and no clean/authority value is
// returned to the caller.
func initBorrowedGateGuard(ctx context.Context, pool *pgxpool.Pool, boundTargetKey, roleFingerprint, adminProvenance, operationID, pristineDigest string) error {
	if ctx == nil || pool == nil || boundTargetKey == "" || roleFingerprint == "" || adminProvenance == "" || operationID == "" || pristineDigest == "" {
		return borrowedGateFixtureRefuse("guard init inputs")
	}
	evidence, err := json.Marshal(map[string]any{
		"fixture":          "borrowed-gate",
		"bound_target_key": boundTargetKey,
		"role_fingerprint": roleFingerprint,
		"admin_provenance": adminProvenance,
		"pristine_digest":  pristineDigest,
		"initial_facts": map[string]any{
			"user_relations": 0, "non_default_schemas": 0, "disposition": "clean",
		},
	})
	if err != nil {
		return borrowedGateFixtureRefuse("guard evidence unencodable")
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO recovery_target_guard
    (target_guard_key, disposition, operation_id, clean_at, rebuild_evidence)
VALUES ($1, 'clean', $2, now(), $3::jsonb)`, boundTargetKey, operationID, string(evidence)); err != nil {
		return err
	}
	return nil
}
