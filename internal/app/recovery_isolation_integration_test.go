//go:build integration

// recovery_isolation_integration_test.go is T026 of batch B7 (US2): the
// real-entry-point default-denial acceptance of the 015 resumption gate.
//
// State at HEAD d00fabc (B9 wiring not landed): internal/app/serve.go,
// withdrawalhttp.go, withdrawalworker.go, eventpublisher.go and
// eventconsumer.go do not consult the recovery gate at all. This file is
// test-first: it pins the behavior T030–T034 must land and FAILS until that
// wiring exists. Every failing subtest is the advance failure evidence for B9;
// where the gate logic itself can already be exercised through the existing
// recovery.Gate layer over a real control store, the same expectation is
// verified separately and passes today.
//
// Wiring points asserted here (contracts/resumption-gate.md §1, tasks
// T030–T034, data-model.md §3):
//
//   - serve read paths: GET /withdrawals/{id} (query), GET
//     /withdrawals/{id}/execution (execution read) and GET /nonce/bindings/*
//     are refused at HTTP admission (before the handler) with the closed
//     refusal_class exposed and audited;
//   - serve POST /withdrawals: refused in front of guardRoute/CapacityGate and
//     before the 007 decode/auth order, with zero withdrawal rows created;
//   - serve chain_scan: the scan loops must not start or progress while the
//     recovery instance is open (the durable chain_blocks/header/log/deposit
//     frontier stays frozen even though fresh blocks were mined first);
//   - a process restart re-reads the same control store and still refuses
//     (isolation is never held in process memory);
//   - withdrawal-worker (existing_withdrawal_recovery): refuses before any
//     claim/advance (zero execution_claims) and exposes the refusal class;
//   - event-publisher (event_publishing): refuses before any broker contact,
//     claim or settle (a seeded outbox row stays untouched) and exposes the
//     refusal class;
//   - event-consumer (event_consuming): refuses before any effect or offset
//     advance (zero consumer_progress/consumer_inbox rows) and exposes the
//     refusal class;
//   - a TXHARBOR_RECOVERY_INSTANCE binding that does not match the open
//     instance is refused as instance_mismatch.
//
// TestRecoveryIsolationNormalModeEntryPointsUnchanged is the FR-023 companion:
// with the control store configured but no open instance, the original 007
// gates decide exactly as before (passes today, must stay green).
//
// Two layers are distinguished per subtest: (1) the control-store-derived
// refusal class the gate produces for the capability/binding of the entry
// (already implemented, exercised here directly), and (2) the real entry point
// surfacing that refusal to the caller (B9 wiring, currently failing).
//
// Docker missing: the package TestMain (app_shared_pg_test.go) reports NOT RUN
// locally (exit 0) and fails the package under CI=true /
// TXHARBOR_REQUIRE_DOCKER=1 — an unrun PG layer is never a pass.
package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
)

const (
	// recovControlDatabase is the independent control-store database inside the
	// scene's PostgreSQL instance. A separate database is the logical
	// rollback-domain separation of data-model §0; its DSN is never the data
	// DSN, so the T025 trust boundary stays intact.
	recovControlDatabase = "txharbor_recovery_control"

	// recovGateTTLValue is the required gate cache TTL (the gate has no
	// default; a missing TTL refuses by name).
	recovGateTTLValue = "30s"

	// recovPrincipal is the deployment principal the process commands bind. It
	// never authorizes anything by itself.
	recovPrincipal = "deploy:recov-test-executor"
)

// recovScopeHash returns the canonical capability scope of one capability on
// the scene chain (T050): the production constructor the real entries use, so
// the gate-layer defaults are asserted against the same stream keys.
func recovScopeHash(t *testing.T, capability recovery.Capability) string {
	t.Helper()
	scope, err := recovery.CapabilityScope(31337, capability)
	if err != nil {
		t.Fatalf("CapabilityScope(%s): %v", capability, err)
	}
	return scope
}

// recovScene is one data database plus one independent control-store database
// inside a single test PostgreSQL container, and (optionally) an Anvil RPC the
// serve process can dial.
type recovScene struct {
	ctx         context.Context
	dataDSN     string
	controlDSN  string
	rpcURL      string
	dataPool    *pgxpool.Pool
	controlPool *pgxpool.Pool
	store       *controlstore.Store
	instanceID  string
}

// recovNewScene boots the container, migrates the data schema and the
// control-store schema into two separate databases, and opens the pools.
// needRPC additionally boots Anvil for the serve entry point.
func recovNewScene(t *testing.T, needRPC bool) *recovScene {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	pgCtr := startPostgresContainer(t)
	dataDSN := postgresDSN(t, pgCtr)

	// Control store: independent database, its own embedded schema and its own
	// version sequence (never the data DB's migrations).
	admin, err := pgx.Connect(ctx, dataDSN)
	if err != nil {
		t.Fatalf("connect to create the control database: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+recovControlDatabase); err != nil {
		_ = admin.Close(ctx)
		t.Fatalf("create control database %s: %v", recovControlDatabase, err)
	}
	if err := admin.Close(ctx); err != nil {
		t.Fatalf("close control-database bootstrap connection: %v", err)
	}
	controlDSN, err := deriveAppDSN(dataDSN, recovControlDatabase)
	if err != nil {
		t.Fatalf("derive control dsn: %v", err)
	}
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: controlDSN, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS,
	}, io.Discard); err != nil {
		t.Fatalf("migrate control store: %v", err)
	}
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: dataDSN, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second,
	}, io.Discard); err != nil {
		t.Fatalf("migrate data database: %v", err)
	}

	dataPool, err := pgxpool.New(ctx, dataDSN)
	if err != nil {
		t.Fatalf("open data pool: %v", err)
	}
	t.Cleanup(dataPool.Close)
	controlPool, err := pgxpool.New(ctx, controlDSN)
	if err != nil {
		t.Fatalf("open control pool: %v", err)
	}
	t.Cleanup(controlPool.Close)
	store, err := controlstore.NewStore(ctx, controlPool)
	if err != nil {
		t.Fatalf("version-guarded control store: %v", err)
	}

	scene := &recovScene{
		ctx:         ctx,
		dataDSN:     dataDSN,
		controlDSN:  controlDSN,
		dataPool:    dataPool,
		controlPool: controlPool,
		store:       store,
	}
	if needRPC {
		scene.rpcURL = anvilURL(t, startAnvilContainer(t))
	}
	return scene
}

// openRecoveryInstance opens the one global open recovery instance every
// default-deny expectation of this file is anchored to.
func (s *recovScene) openRecoveryInstance(t *testing.T) {
	t.Helper()
	res, err := s.store.OpenInstance(s.ctx, controlstore.OpenInstanceRequest{
		Kind: "recovery", OpenedBy: recovPrincipal,
	})
	if err != nil {
		t.Fatalf("open recovery instance: %v", err)
	}
	if res.InstanceID == "" {
		t.Fatal("control store returned an empty instance id")
	}
	s.instanceID = res.InstanceID
}

// serveEnv is the full serve environment plus the 015 recovery configuration
// (independent control DSN, required TTL, bound principal). The binding
// variable is added per subtest where a bound/mismatched caller is asserted.
func (s *recovScene) serveEnv(addr string) map[string]string {
	const asset = "0x2222222222222222222222222222222222222222"
	const watch = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	env := fullServeEnv(addr)
	env[config.EnvPGDSN] = s.dataDSN
	env[config.EnvRPCURL] = s.rpcURL
	env[config.EnvLogContracts] = asset
	env[config.EnvDepositContracts] = asset + ":0"
	env[config.EnvDepositWatchAddresses] = watch + ":0"
	env[config.EnvRecoveryControlDSN] = s.controlDSN
	env[config.EnvRecoveryGateTTL] = recovGateTTLValue
	env[config.EnvRecoveryPrincipal] = recovPrincipal
	return env
}

// commandEnv is the full serve environment for the process commands (no RPC,
// no HTTP listener) plus the 015 recovery configuration.
func (s *recovScene) commandEnv() map[string]string {
	env := fullServeEnv("127.0.0.1:0")
	env[config.EnvPGDSN] = s.dataDSN
	env[config.EnvRecoveryControlDSN] = s.controlDSN
	env[config.EnvRecoveryGateTTL] = recovGateTTLValue
	env[config.EnvRecoveryPrincipal] = recovPrincipal
	return env
}

// eventsCommandEnv is the full 013 events configuration for the
// event-publisher/event-consumer commands plus the 015 recovery
// configuration. The broker address stays unreachable on purpose: a correctly
// wired entry refuses before any broker contact while the instance is open.
func (s *recovScene) eventsCommandEnv() map[string]string {
	env := fullEventsEnv()
	env[config.EnvPGDSN] = s.dataDSN
	env[config.EnvRecoveryControlDSN] = s.controlDSN
	env[config.EnvRecoveryGateTTL] = recovGateTTLValue
	env[config.EnvRecoveryPrincipal] = recovPrincipal
	return env
}

// recovSyncBuffer is a race-free io.Writer for a command/serve process whose
// output is read while it may still be running.
type recovSyncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *recovSyncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *recovSyncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// recovServeHandle is one in-process `txharbor serve` run.
type recovServeHandle struct {
	t       *testing.T
	addr    string
	signals chan os.Signal
	done    chan int
	stdout  recovSyncBuffer
	stderr  recovSyncBuffer
	exited  bool
	code    int
}

func recovStartServe(t *testing.T, env map[string]string, addr string) *recovServeHandle {
	t.Helper()
	h := &recovServeHandle{
		t:       t,
		addr:    addr,
		signals: make(chan os.Signal, 1),
		done:    make(chan int, 1),
	}
	go func() {
		h.done <- Serve(context.Background(), Deps{
			Getenv:  envGetter(env),
			Stdout:  &h.stdout,
			Stderr:  &h.stderr,
			Signals: h.signals,
		})
	}()
	return h
}

func (h *recovServeHandle) base() string { return "http://" + h.addr }

func (h *recovServeHandle) checkExited() (int, bool) {
	if h.exited {
		return h.code, true
	}
	select {
	case h.code = <-h.done:
		h.exited = true
		return h.code, true
	default:
		return 0, false
	}
}

// waitReady waits for the shared probe listener to answer /readyz. A process
// that exits instead is a failure: the 015 contract places the refusal at
// HTTP admission, so serve must stay resident while isolated.
func (h *recovServeHandle) waitReady(timeout time.Duration) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		if code, ok := h.checkExited(); ok {
			h.t.Fatalf("serve exited before becoming ready (code=%d); the recovery gate belongs at HTTP admission, the process must keep serving; stderr=%s",
				code, recovTruncate(h.stderr.String()))
		}
		resp, err := client.Get(h.base() + "/readyz")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	h.t.Fatalf("serve never became ready at %s within %s; stderr=%s", h.base(), timeout, recovTruncate(h.stderr.String()))
}

// stop terminates the process and requires a clean (zero) exit. It is
// idempotent so a subtest can stop a process explicitly and still register the
// deferred safety stop that covers every earlier failure path.
func (h *recovServeHandle) stop() {
	h.t.Helper()
	if h.exited {
		return
	}
	if code, ok := h.checkExited(); ok {
		h.t.Fatalf("serve exited before the test stopped it (code=%d); stderr=%s", code, recovTruncate(h.stderr.String()))
	}
	select {
	case h.signals <- os.Interrupt:
	default:
	}
	select {
	case h.code = <-h.done:
		h.exited = true
		if h.code != 0 {
			h.t.Fatalf("serve exit code = %d, want 0 on clean shutdown; stderr=%s", h.code, recovTruncate(h.stderr.String()))
		}
	case <-time.After(30 * time.Second):
		h.t.Fatalf("serve did not shut down within 30s after the termination signal")
	}
}

// recovHTTP performs one request and returns status plus a bounded body.
func recovHTTP(t *testing.T, method, url string, body io.Reader, contentType string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("build %s %s: %v", method, url, err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("read %s %s body: %v", method, url, err)
	}
	return resp.StatusCode, string(raw)
}

// recovRefusalCheck reports one response-level refusal expectation without
// aborting the subtest, so a table of paths/tests records every item
// independently (the T026 evidence is per entry point).
func recovRefusalCheck(t *testing.T, status int, body string, want recovery.RefusalClass, what string) bool {
	t.Helper()
	if status >= 200 && status < 300 {
		t.Errorf("%s: request was not refused (status=%d body=%s); an open recovery instance must deny by default",
			what, status, recovTruncate(body))
		return false
	}
	class, ok := recovRefusalClassFromBody(body)
	if !ok {
		t.Errorf("%s: refusal does not expose a closed-set refusal_class (status=%d body=%s); wire the gate decision into the refusal surface",
			what, status, recovTruncate(body))
		return false
	}
	if want != "" && class != want {
		t.Errorf("%s: refusal_class = %q, want %q (gate derivation of data-model §3)", what, class, want)
		return false
	}
	return true
}

// recovRequireRefusal asserts one response is a refusal exposing a closed-set
// refusal_class (contracts/resumption-gate.md §1.1: "拒绝必须暴露闭集
// refusal_class 并审计"). want=="" accepts any closed-set class.
func recovRequireRefusal(t *testing.T, status int, body string, want recovery.RefusalClass, what string) recovery.RefusalClass {
	t.Helper()
	if !recovRefusalCheck(t, status, body, want, what) {
		t.FailNow()
	}
	class, _ := recovRefusalClassFromBody(body)
	return class
}

// recovRefusalClassFromBody extracts the closed-set refusal_class from a JSON
// refusal body (top level or nested).
func recovRefusalClassFromBody(body string) (recovery.RefusalClass, bool) {
	var decoded any
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		return "", false
	}
	return recovFindRefusalClass(decoded)
}

func recovFindRefusalClass(v any) (recovery.RefusalClass, bool) {
	switch node := v.(type) {
	case map[string]any:
		if raw, ok := node["refusal_class"].(string); ok {
			class := recovery.RefusalClass(raw)
			if class.Known() && class != recovery.RefusalNoInstance {
				return class, true
			}
		}
		for _, child := range node {
			if class, ok := recovFindRefusalClass(child); ok {
				return class, true
			}
		}
	case []any:
		for _, child := range node {
			if class, ok := recovFindRefusalClass(child); ok {
				return class, true
			}
		}
	}
	return "", false
}

// recovClassInText extracts a closed-set refusal class from a command's
// stdout/stderr. A `refusal_class` marker is preferred; a bare closed-set
// token is accepted so a compact one-line refusal still counts as exposure.
func recovClassInText(text string) (recovery.RefusalClass, bool) {
	if !strings.Contains(text, "refusal_class") {
		return "", false
	}
	for _, class := range recovery.KnownRefusalClasses() {
		if class == recovery.RefusalNoInstance {
			continue // normal-mode marker, never a refusal
		}
		if strings.Contains(text, string(class)) {
			return class, true
		}
	}
	return "", false
}

// recovRefusedAuditCount counts audited gate refusals on the open instance.
func recovRefusedAuditCount(t *testing.T, s *recovScene) int {
	t.Helper()
	return recovCount(t, s.ctx, s.controlPool,
		`SELECT count(*) FROM recovery_audit
		 WHERE instance_id = $1 AND result = 'refused' AND refusal_class IS NOT NULL`,
		s.instanceID)
}

// recovCount runs one count query against the given pool.
func recovCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %s: %v", recovTruncate(query), err)
	}
	return n
}

// recovScanFrontier is the durable chain-scan progress of one data database.
type recovScanFrontier struct {
	blocks      int
	header      int64
	logNext     int64
	depositNext int64
}

// recovReadScanFrontier reads the durable scan progress; -1 marks an absent
// checkpoint row (nothing scanned yet).
func recovReadScanFrontier(t *testing.T, s *recovScene) recovScanFrontier {
	t.Helper()
	var f recovScanFrontier
	f.blocks = recovCount(t, s.ctx, s.dataPool, "SELECT count(*) FROM chain_blocks")
	if err := s.dataPool.QueryRow(s.ctx,
		"SELECT COALESCE(max(height), -1) FROM indexer_checkpoint WHERE chain_id = 31337").Scan(&f.header); err != nil {
		t.Fatalf("read header checkpoint: %v", err)
	}
	if err := s.dataPool.QueryRow(s.ctx,
		"SELECT COALESCE(max(next_block), -1) FROM log_checkpoint WHERE chain_id = 31337").Scan(&f.logNext); err != nil {
		t.Fatalf("read log checkpoint: %v", err)
	}
	if err := s.dataPool.QueryRow(s.ctx,
		"SELECT COALESCE(max(next_block), -1) FROM deposit_checkpoint WHERE chain_id = 31337").Scan(&f.depositNext); err != nil {
		t.Fatalf("read deposit checkpoint: %v", err)
	}
	return f
}

// moved reports whether any durable scan frontier advanced past before.
func (f recovScanFrontier) moved(before recovScanFrontier) bool {
	return f.blocks > before.blocks || f.header > before.header ||
		f.logNext > before.logNext || f.depositNext > before.depositNext
}

// recovRequireCommandRefusal asserts one command entry exposed and audited a
// gate refusal while the recovery instance is open. code is the process exit
// code; the output is stdout+stderr.
func recovRequireCommandRefusal(t *testing.T, s *recovScene, entry string, code int, output string, auditBefore int) {
	t.Helper()
	class, exposed := recovClassInText(output)
	if !exposed {
		t.Errorf("%s: output does not expose a closed-set refusal_class (exit=%d); the entry must refuse the action and name the class; output=%s",
			entry, code, recovTruncate(output))
	} else if class != recovery.RefusalInstanceMismatch {
		t.Errorf("%s: exposed refusal_class = %q, want %q for an unbound process while a recovery instance is open",
			entry, class, recovery.RefusalInstanceMismatch)
	}
	if after := recovRefusedAuditCount(t, s); after <= auditBefore {
		t.Errorf("%s: no gate refusal was audited (before=%d after=%d); every refusal must expose and audit the class",
			entry, auditBefore, after)
	}
}

// recovRunEntry runs one real command entry with a bounded observation window.
// It returns the exit code and reports whether the window elapsed first (the
// process was then cancelled and joined).
func recovRunEntry(t *testing.T, timeout time.Duration, run func(ctx context.Context) int) (code int, timedOut bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	go func() { done <- run(ctx) }()
	select {
	case code = <-done:
		return code, false
	case <-time.After(timeout):
		cancel()
		select {
		case code = <-done:
			return code, true
		case <-time.After(20 * time.Second):
			t.Fatalf("entry did not exit within 20s after cancellation")
		}
	}
	return 0, false
}

// recovTruncate bounds failure messages.
func recovTruncate(s string) string {
	const max = 800
	s = strings.TrimSpace(s)
	if len(s) <= max {
		return s
	}
	return s[:max] + "...(truncated)"
}

// ---------------------------------------------------------------------------
// Inert 010/008 assembly for the withdrawal-worker entry
// ---------------------------------------------------------------------------

// recovInertLifecycle supplies the four required JointDeps participants without
// performing any I/O. The empty data database offers no claimable intent, so
// these seams are never reached when the entry is (correctly) denied; when a
// broken implementation runs the loop anyway, the loop still performs zero
// execution work.
type recovInertLifecycle struct{}

func (recovInertLifecycle) Advance(context.Context, execution.AdvanceRequest) (execution.AdvanceOutcome, error) {
	return execution.AdvanceOutcome{Class: execution.OutcomeUnavailable}, nil
}

func (recovInertLifecycle) Read(context.Context, string) (execution.LifecycleFacts, error) {
	return execution.LifecycleFacts{}, nil
}

func (recovInertLifecycle) ReadBinding(context.Context, string) (execution.BindingResult, error) {
	return execution.BindingAbsent, nil
}

func recovInertJointWiring(context.Context, *config.Config, *pgxpool.Pool) (JointDeps, error) {
	return JointDeps{
		Advancer:       recovInertLifecycle{},
		Reader:         recovInertLifecycle{},
		Binding:        recovInertLifecycle{},
		AllocBinding:   func(context.Context, string) (string, error) { return "allocated", nil },
		ConfirmAttempt: func(context.Context, string) error { return nil },
	}, nil
}

// recovSeedPendingOutbox appends one committed event through the real Append
// path, leaving exactly one pending outbox row the denied publisher must not
// touch.
func recovSeedPendingOutbox(t *testing.T, s *recovScene) {
	t.Helper()
	obsID := "recov-isolation-observation"
	ev, err := events.NewEvent(events.Event{
		EventType:     events.EventTypeDepositObservationCreated,
		SchemaVersion: events.SchemaVersionV1,
		IdentityKind:  events.IdentityKindEVMLog,
		AggregateType: "deposit_observation",
		AggregateID:   obsID,
		Payload: map[string]any{
			"observation_id": obsID,
			"state":          "pending",
		},
		OccurredAt:  time.Now().UTC(),
		ChainID:     31337,
		BlockNumber: 100,
		BlockHash:   fmt.Sprintf("0x%064x", 0x5150),
		TxHash:      fmt.Sprintf("0x%064x", 0x6161),
		LogIndex:    1,
	})
	if err != nil {
		t.Fatalf("build outbox seed event: %v", err)
	}
	tx, err := s.dataPool.Begin(s.ctx)
	if err != nil {
		t.Fatalf("begin outbox seed: %v", err)
	}
	if _, err := events.Append(s.ctx, tx, ev); err != nil {
		_ = tx.Rollback(s.ctx)
		t.Fatalf("append outbox seed event: %v", err)
	}
	if err := tx.Commit(s.ctx); err != nil {
		t.Fatalf("commit outbox seed: %v", err)
	}
	if n := recovCount(t, s.ctx, s.dataPool, `SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`); n != 1 {
		t.Fatalf("seeded pending outbox rows = %d, want 1", n)
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestRecoveryIsolationNormalModeEntryPointsUnchanged pins FR-023
// pass-through: with the control store configured but no open recovery
// instance, the daily runtime is unchanged and the original 007 gate order
// stands. It passes today and must stay green after B9.
func TestRecoveryIsolationNormalModeEntryPointsUnchanged(t *testing.T) {
	scene := recovNewScene(t, true)

	addr := freeAddr(t)
	h := recovStartServe(t, scene.serveEnv(addr), addr)
	defer h.stop()
	h.waitReady(90 * time.Second)
	base := h.base()

	if status, body := recovHTTP(t, http.MethodGet, base+"/withdrawals/recov-1", nil, ""); status != http.StatusUnauthorized {
		t.Fatalf("normal mode GET /withdrawals/{id} = %d %s, want 401 (007 auth unchanged; no open recovery instance must not alter the daily runtime)",
			status, recovTruncate(body))
	}
	// 007 POST order is decode (400) before auth (401); a pass-through gate
	// must not move that order.
	if status, body := recovHTTP(t, http.MethodPost, base+"/withdrawals", strings.NewReader("{"), "application/json"); status != http.StatusBadRequest {
		t.Fatalf("normal mode POST /withdrawals malformed = %d %s, want 400 (original decode-before-auth order)",
			status, recovTruncate(body))
	}
	if status, body := recovHTTP(t, http.MethodPost, base+"/withdrawals", strings.NewReader("{}"), "application/json"); status != http.StatusUnauthorized {
		t.Fatalf("normal mode POST /withdrawals no key = %d %s, want 401 (007 auth unchanged)",
			status, recovTruncate(body))
	}
}

// TestRecoveryIsolationRealServeEntryPointsDefaultDeny covers the serve entry
// points of T026: the query read paths, POST /withdrawals, chain_scan
// non-progress, restart persistence, the matched/mismatched binding and the
// existing gate-layer derivation the entry points must carry.
//
// This scene runs no normal-mode serve first, so its first serve run is the
// one under test: it acquires a free lease and a scanner that (incorrectly)
// keeps running would really have fresh blocks to index (the chain is mined
// before the run), making the frozen-frontier assertion conclusive.
func TestRecoveryIsolationRealServeEntryPointsDefaultDeny(t *testing.T) {
	scene := recovNewScene(t, true)
	scene.openRecoveryInstance(t)

	// (1) Open recovery instance, unbound process: every real serve path
	// refuses with instance_mismatch (the gate's unbound denial), the
	// refusal_class is exposed and audited, no withdrawal row exists, and
	// chain_scan does not start or progress.
	t.Run("open_instance_real_serve_paths_default_deny", func(t *testing.T) {
		auditBefore := recovRefusedAuditCount(t, scene)

		// chain_scan baseline: mine two new blocks so a scanner that
		// (incorrectly) keeps running would have fresh work to index; the
		// frozen durable frontier below then proves nothing stepped.
		pauseSceneAnvilRPC(t, scene.rpcURL, "anvil_mine", "0x2")
		frontierBefore := recovReadScanFrontier(t, scene)

		addr := freeAddr(t)
		h := recovStartServe(t, scene.serveEnv(addr), addr)
		defer h.stop()
		h.waitReady(90 * time.Second)
		base := h.base()

		// Read paths: the checkpoint is HTTP admission, so the refusal must
		// arrive even without a bearer credential (and must not be the 401).
		for _, path := range []string{
			"/withdrawals/recov-1",           // query: 007 GET read path
			"/withdrawals/recov-1/execution", // query: 011 execution read path
			"/nonce/bindings/recov-1",        // query: 008 nonce read path
		} {
			status, body := recovHTTP(t, http.MethodGet, base+path, nil, "")
			recovRefusalCheck(t, status, body, recovery.RefusalInstanceMismatch, "GET "+path)
		}

		// Write path: a malformed body proves the recovery admission precedes
		// the 007 decode/auth and every guardRoute gate (T031: refusal before
		// guardRoute/CapacityGate).
		status, body := recovHTTP(t, http.MethodPost, base+"/withdrawals", strings.NewReader("{"), "application/json")
		recovRefusalCheck(t, status, body, recovery.RefusalInstanceMismatch, "POST /withdrawals (malformed body)")

		if n := recovCount(t, scene.ctx, scene.dataPool, "SELECT count(*) FROM withdrawal_requests"); n != 0 {
			t.Errorf("withdrawal_requests = %d rows after the refusals, want 0", n)
		}
		if after := recovRefusedAuditCount(t, scene); after <= auditBefore {
			t.Errorf("serve refusals were not audited (before=%d after=%d); the closed refusal_class must be exposed and audited",
				auditBefore, after)
		}

		// chain_scan (T030 loop-start check + wrapped step callback): no block
		// or checkpoint may advance while the instance is open.
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			if recovReadScanFrontier(t, scene).moved(frontierBefore) {
				t.Errorf("chain_scan advanced (chain_blocks or a header/log/deposit checkpoint frontier moved) while the recovery instance is open; the serve scan loop must not start or step before chain_scan is released")
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
	})

	// (2) A process restart re-reads the same control store: isolation is not
	// held in process memory and is not lifted by a restart.
	t.Run("process_restart_does_not_lift_isolation", func(t *testing.T) {
		addr := freeAddr(t)
		first := recovStartServe(t, scene.serveEnv(addr), addr)
		defer first.stop()
		first.waitReady(90 * time.Second)
		status, body := recovHTTP(t, http.MethodGet, first.base()+"/withdrawals/recov-2", nil, "")
		recovRefusalCheck(t, status, body, recovery.RefusalInstanceMismatch, "first run GET /withdrawals/{id}")
		first.stop()

		auditBefore := recovRefusedAuditCount(t, scene)
		second := recovStartServe(t, scene.serveEnv(addr), addr)
		defer second.stop()
		second.waitReady(90 * time.Second)
		status, body = recovHTTP(t, http.MethodGet, second.base()+"/withdrawals/recov-2", nil, "")
		recovRefusalCheck(t, status, body, recovery.RefusalInstanceMismatch, "restarted run GET /withdrawals/{id}")
		if after := recovRefusedAuditCount(t, scene); after <= auditBefore {
			t.Errorf("no gate refusal was audited after the restart (before=%d after=%d); a restart must re-read the control store, never resume from memory",
				auditBefore, after)
		}
	})

	// (3) A process bound to the open instance reaches the full derivation and
	// is still denied by the unverified isolation checklist (not a binding
	// error) — the two refusal classes are not interchangeable.
	t.Run("bound_instance_default_deny_isolation_unproven", func(t *testing.T) {
		addr := freeAddr(t)
		env := scene.serveEnv(addr)
		env[config.EnvRecoveryInstance] = scene.instanceID
		h := recovStartServe(t, env, addr)
		defer h.stop()
		h.waitReady(90 * time.Second)
		status, body := recovHTTP(t, http.MethodGet, h.base()+"/withdrawals/recov-3", nil, "")
		recovRequireRefusal(t, status, body, recovery.RefusalIsolationUnproven, "bound instance GET /withdrawals/{id}")
	})

	// (4) TXHARBOR_RECOVERY_INSTANCE binding mismatch: the process that is not
	// bound to the open instance is refused as instance_mismatch.
	t.Run("txharbor_recovery_instance_binding_mismatch_refused", func(t *testing.T) {
		addr := freeAddr(t)
		env := scene.serveEnv(addr)
		env[config.EnvRecoveryInstance] = uuid.NewString()
		h := recovStartServe(t, env, addr)
		defer h.stop()
		h.waitReady(90 * time.Second)
		status, body := recovHTTP(t, http.MethodGet, h.base()+"/withdrawals/recov-4", nil, "")
		recovRequireRefusal(t, status, body, recovery.RefusalInstanceMismatch, "mismatched binding GET /withdrawals/{id}")
	})

	// (5) The gate-layer derivation the real entries must carry. This layer
	// exists today and is the part of T026 that already passes: the refusal
	// class each entry must surface is the deterministic gate outcome of its
	// capability and binding.
	t.Run("gate_layer_defaults_for_real_entry_capabilities", func(t *testing.T) {
		gate, err := recovery.NewGate(scene.store, recovery.GateOptions{TTL: 30 * time.Second})
		if err != nil {
			t.Fatalf("NewGate: %v", err)
		}
		otherInstance := uuid.NewString()
		cases := []struct {
			name       string
			capability recovery.Capability
			binding    string
			want       recovery.RefusalClass
			// audited: the gate records an audit row for this refusal against a
			// known instance. A binding that names no existing instance is
			// refused but the not-found branch writes its best-effort audit
			// without an instance row, so no audit growth is asserted there
			// (the refusal_class exposure is still asserted).
			audited bool
		}{
			{"serve read path unbound", recovery.CapabilityQuery, "", recovery.RefusalInstanceMismatch, true},
			{"serve read path mismatched binding", recovery.CapabilityQuery, otherInstance, recovery.RefusalInstanceMismatch, false},
			{"serve read path bound", recovery.CapabilityQuery, scene.instanceID, recovery.RefusalIsolationUnproven, true},
			{"POST /withdrawals bound (via existing_withdrawal_recovery)", recovery.CapabilityNewWithdrawalCreation, scene.instanceID, recovery.RefusalCapabilityDependencyClosed, true},
			{"event-publisher bound (via chain_scan)", recovery.CapabilityEventPublishing, scene.instanceID, recovery.RefusalCapabilityDependencyClosed, true},
			{"event-consumer bound", recovery.CapabilityEventConsuming, scene.instanceID, recovery.RefusalIsolationUnproven, true},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				before := recovRefusedAuditCount(t, scene)
				dec, err := gate.Admit(scene.ctx, recovery.GateRequest{
					InstanceID: tc.binding,
					Capability: tc.capability,
					ScopeHash:  recovScopeHash(t, tc.capability),
					Actor:      recovPrincipal,
				})
				if err != nil {
					t.Fatalf("Admit(%s): %v", tc.capability, err)
				}
				if dec.Allowed {
					t.Fatalf("Admit(%s) allowed while the instance is open and nothing is released", tc.capability)
				}
				if dec.RefusalClass != tc.want {
					t.Fatalf("Admit(%s) refusal_class = %q, want %q", tc.capability, dec.RefusalClass, tc.want)
				}
				if tc.audited {
					if after := recovRefusedAuditCount(t, scene); after <= before {
						t.Fatalf("Admit(%s) refusal was not audited", tc.capability)
					}
				}
			})
		}
	})
}

// TestRecoveryIsolationRealCommandEntryPointsDefaultDeny covers the process
// command entries of T026: withdrawal-worker, event-publisher and
// event-consumer must refuse and expose the refusal class while the recovery
// instance is open, and must produce zero claims/effects/progress.
func TestRecoveryIsolationRealCommandEntryPointsDefaultDeny(t *testing.T) {
	scene := recovNewScene(t, false)
	scene.openRecoveryInstance(t)

	t.Run("withdrawal_worker_default_denied", func(t *testing.T) {
		auditBefore := recovRefusedAuditCount(t, scene)
		claimsBefore := recovCount(t, scene.ctx, scene.dataPool, "SELECT count(*) FROM execution_claims")

		var stdout, stderr recovSyncBuffer
		code, _ := recovRunEntry(t, 8*time.Second, func(ctx context.Context) int {
			return WithdrawalWorkerCommand(ctx, nil, Deps{
				Getenv:      envGetter(scene.commandEnv()),
				Stdout:      &stdout,
				Stderr:      &stderr,
				JointWiring: recovInertJointWiring,
			})
		})
		recovRequireCommandRefusal(t, scene, "withdrawal-worker", code, stdout.String()+stderr.String(), auditBefore)

		if n := recovCount(t, scene.ctx, scene.dataPool, "SELECT count(*) FROM execution_claims"); n > claimsBefore {
			t.Fatalf("execution_claims grew from %d to %d while isolated; the worker must not claim or advance before existing_withdrawal_recovery is released",
				claimsBefore, n)
		}
	})

	t.Run("event_publisher_default_denied", func(t *testing.T) {
		recovSeedPendingOutbox(t, scene)
		auditBefore := recovRefusedAuditCount(t, scene)

		var stdout, stderr recovSyncBuffer
		code, _ := recovRunEntry(t, 10*time.Second, func(ctx context.Context) int {
			return EventPublisher(ctx, nil, Deps{
				Getenv: envGetter(scene.eventsCommandEnv()),
				Stdout: &stdout,
				Stderr: &stderr,
			})
		})
		recovRequireCommandRefusal(t, scene, "event-publisher", code, stdout.String()+stderr.String(), auditBefore)

		if n := recovCount(t, scene.ctx, scene.dataPool,
			`SELECT count(*) FROM outbox_events
			 WHERE publish_state <> 'pending' OR claim_owner IS NOT NULL OR attempt_count <> 0`); n != 0 {
			t.Fatalf("%d outbox row(s) were claimed/settled/progressed while isolated; event_publishing must not claim or settle before release", n)
		}
	})

	t.Run("event_consumer_default_denied", func(t *testing.T) {
		auditBefore := recovRefusedAuditCount(t, scene)

		var stdout, stderr recovSyncBuffer
		code, _ := recovRunEntry(t, 10*time.Second, func(ctx context.Context) int {
			return EventConsumer(ctx, nil, Deps{
				Getenv: envGetter(scene.eventsCommandEnv()),
				Stdout: &stdout,
				Stderr: &stderr,
			})
		})
		recovRequireCommandRefusal(t, scene, "event-consumer", code, stdout.String()+stderr.String(), auditBefore)

		for _, table := range []string{"consumer_progress", "consumer_inbox"} {
			if n := recovCount(t, scene.ctx, scene.dataPool, "SELECT count(*) FROM "+table); n != 0 {
				t.Fatalf("%s = %d rows while isolated; event_consuming must not apply effects or advance progress before release", table, n)
			}
		}
	})
}
