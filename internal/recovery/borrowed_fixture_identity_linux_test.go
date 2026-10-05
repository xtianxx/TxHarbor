//go:build linux && drill

// borrowed_fixture_identity_linux_test.go is the private identity-prefix helper
// for the borrowed-writer run and the ACTUAL protected originGateFixture. It
// chains the opaque control anchor to the fixture-owned server identity: actual
// container, postmaster PID/start, direct-route original DSNs, control SQL
// (pg_stat_activity / pg_locks / catalog / incarnation) and a PREFIX-SPECIFIC
// STRICT namespace inspection of the exact required control backend (OS start
// bracketing, all fd readlinks classified, TCP inode metadata, retained socket
// token) — never the weak shared auth/gate census.
//
// It is deliberately NON-AUTHORIZING: diagnostic facts are not authority, no
// writer/probe/DDL/restore/acceptance is launched, validity is never cached,
// and any loss, mismatch, uncertain metadata or context end permanently
// invalidates the shared private state as UNKNOWN. All verification is
// expressed as error-returning routines with typed safe reason enums;
// testing.T.Fatal exists only in outer setup/assert wrappers, never inside the
// verification path.
//
// IP04a: the verification window is bracketed at BOTH ends. After the initial
// SQL/OS/anchor checks, the retained immutable tokens (anchor SQL facts,
// target catalog OIDs, backend OS start, server socket inode, postmaster
// incarnation) are re-read and compared against the ORIGINAL retained values
// on a fresh independent SQL connection plus a fresh strict OS census, and only
// then is success published under the shared state mutex.
package recovery_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
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

const (
	// borrowedIdentityStrictHelperPath is the private per-container path of the
	// strict namespace inspector; it never overwrites the shared auth/gate
	// census helper at /tmp/drill-auth-boundary-client.
	borrowedIdentityStrictHelperPath = "/tmp/borrowed-identity-strict-client"
	// borrowedIdentityVerificationBound caps every prefix verification when the
	// caller supplied no nearer deadline.
	borrowedIdentityVerificationBound = 5 * time.Second
	// borrowedIdentitySQLConnectBound caps the independent owned-fixture admin
	// connection used by each SQL phase.
	borrowedIdentitySQLConnectBound = 3 * time.Second
	// borrowedIdentityContextEndedReason is the fixed safe reason recorded when
	// a caller context ends between the inspected facts and the publication
	// decision. It never includes raw context text or DSN material.
	borrowedIdentityContextEndedReason = "identity context ended before success publication"
)

// borrowedIdentityReason is the fixed safe predicate enum reported by every
// verification refusal. It never includes DSN, address or SQL text.
type borrowedIdentityReason string

const (
	reasonPrefixAbsent        borrowedIdentityReason = "prefix-absent"
	reasonPrefixInvalid       borrowedIdentityReason = "prefix-invalid"
	reasonStartedChild        borrowedIdentityReason = "started-child"
	reasonAnchorUnavailable   borrowedIdentityReason = "anchor-unavailable"
	reasonDeclaredTarget      borrowedIdentityReason = "declared-target-invalid"
	reasonKeyBinding          borrowedIdentityReason = "key-binding-mismatch"
	reasonNamespaceBinding    borrowedIdentityReason = "namespace-binding-mismatch"
	reasonSQLConnect          borrowedIdentityReason = "sql-connect"
	reasonSQLBackend          borrowedIdentityReason = "sql-backend-row"
	reasonSQLNamespace        borrowedIdentityReason = "sql-namespace"
	reasonSQLIncarnation      borrowedIdentityReason = "sql-incarnation"
	reasonSQLTargetCatalog    borrowedIdentityReason = "sql-target-catalog"
	reasonOSPostmaster        borrowedIdentityReason = "os-postmaster"
	reasonOSBackend           borrowedIdentityReason = "os-backend"
	reasonOSServerInode       borrowedIdentityReason = "os-server-inode"
	reasonAnchorRecheck       borrowedIdentityReason = "anchor-recheck"
	reasonEndSQLBackend       borrowedIdentityReason = "end-sql-backend-row"
	reasonEndSQLNamespace     borrowedIdentityReason = "end-sql-namespace"
	reasonEndSQLIncarnation   borrowedIdentityReason = "end-sql-incarnation"
	reasonEndSQLTargetCatalog borrowedIdentityReason = "end-sql-target-catalog"
	reasonEndOSPostmaster     borrowedIdentityReason = "end-os-postmaster"
	reasonEndOSBackendStart   borrowedIdentityReason = "end-os-backend-start-token"
	reasonEndOSServerInode    borrowedIdentityReason = "end-os-server-inode-token"
	reasonStrictExec          borrowedIdentityReason = "strict-exec"
	reasonStrictMalformed     borrowedIdentityReason = "strict-report-malformed"
	reasonStrictIncomplete    borrowedIdentityReason = "strict-report-incomplete"
	reasonStrictReportPID     borrowedIdentityReason = "strict-report-pid"
	reasonStrictReportPost    borrowedIdentityReason = "strict-report-postmaster"
	reasonStrictReportBackend borrowedIdentityReason = "strict-report-backend"
	reasonStrictReportState   borrowedIdentityReason = "strict-report-state"
	reasonStrictReportPPID    borrowedIdentityReason = "strict-report-ppid"
	reasonStrictReportFD      borrowedIdentityReason = "strict-report-fd-accounting"
	reasonStrictReportFDUnk   borrowedIdentityReason = "strict-report-fd-unknown"
	reasonStrictReportSocket  borrowedIdentityReason = "strict-report-socket"
	reasonStrictRequiredInode borrowedIdentityReason = "strict-report-required-inode"
	reasonContextEnded        borrowedIdentityReason = "context-ended"
	reasonConcurrentInvalid   borrowedIdentityReason = "concurrent-invalidation"
)

// borrowedIdentityRefusal is the typed verification refusal. The reason is a
// fixed safe enum constant only.
type borrowedIdentityRefusal struct {
	reason borrowedIdentityReason
}

func (e *borrowedIdentityRefusal) Error() string {
	return "borrowed identity UNKNOWN (" + string(e.reason) + ")"
}

func borrowedIdentityRefuse(reason borrowedIdentityReason) error {
	return &borrowedIdentityRefusal{reason: reason}
}

// borrowedIdentityRefusalStage extracts the fixed predicate enum of a
// verification refusal (false for non-verification errors).
func borrowedIdentityRefusalStage(err error) (borrowedIdentityReason, bool) {
	var refusal *borrowedIdentityRefusal
	if !errors.As(err, &refusal) {
		return "", false
	}
	return refusal.reason, true
}

// borrowedIdentityStage names the narrow test-only prefix pause points. They
// are not authorization callbacks and are never consulted by production code.
type borrowedIdentityStage string

const (
	// borrowedIdentityStageInspectPublish pauses the prefix publication after
	// the final anchor recheck and before the shared state decision.
	borrowedIdentityStageInspectPublish borrowedIdentityStage = "inspect-publish"
	// borrowedIdentityStageEndWindow pauses between the initial checks and the
	// end-of-window rechecks, after the final anchor recheck.
	borrowedIdentityStageEndWindow borrowedIdentityStage = "end-window"
)

// borrowedIdentityStageHook is the per-process test pause seam, installed and
// cleared per test and read atomically.
var borrowedIdentityStageHook atomic.Pointer[func(borrowedIdentityStage)]

func borrowedIdentityPauseAt(stage borrowedIdentityStage) {
	if hook := borrowedIdentityStageHook.Load(); hook != nil {
		(*hook)(stage)
	}
}

// borrowedIdentityStrictPhase distinguishes the initial census from the
// end-of-window census so a deterministic negative report seam can target only
// the end recheck.
type borrowedIdentityStrictPhase string

const (
	borrowedIdentityStrictPhaseInitial borrowedIdentityStrictPhase = "initial"
	borrowedIdentityStrictPhaseEnd     borrowedIdentityStrictPhase = "end"
)

// borrowedIdentityStrictReportHook is the deterministic post-anchor negative
// report seam: it may only mutate a structurally parsed census before its
// invariants are validated, and the retained-token end comparison still binds
// the result. Tests install it for one test and clear it at cleanup.
var borrowedIdentityStrictReportHook atomic.Pointer[func(*borrowedIdentityStrictCensus, borrowedIdentityStrictPhase)]

// borrowedIdentityState is the shared permanent invalidation state of a prefix.
// Copies share this pointer; there is no Valid/Clean/Accepted cache.
type borrowedIdentityState struct {
	mu      sync.Mutex
	invalid bool
	reason  string
}

func (s *borrowedIdentityState) invalidate(reason string) {
	s.mu.Lock()
	if !s.invalid {
		s.invalid = true
		s.reason = reason
	}
	s.mu.Unlock()
}

func (s *borrowedIdentityState) invalidReason() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.invalid {
		return ""
	}
	return s.reason
}

// borrowedIdentityPrefix is the private prefix object. All fields are private;
// there is no JSON and no authority grant. Copies share the state pointer, the
// per-prefix admin serialization and the immutable token fields.
type borrowedIdentityPrefix struct {
	fixture *originGateFixture
	run     *recovery.DrillBorrowedWriterRun
	anchor  recovery.DrillControlAnchor
	state   *borrowedIdentityState

	// adminMu serializes the prefix SQL phase across copies of one prefix.
	// It is not used for capture claims or anchor rechecks.
	adminMu *sync.Mutex
	// strictHelperPath is the actual in-container private path of the strict
	// namespace inspector.
	strictHelperPath string

	// Immutable expected tokens established by the first successful
	// inspection and never overwritten with fresh values.
	capturedFacts       recovery.DrillControlAnchorFacts
	capturedTargetDBOID uint32
	capturedRoleOID     uint32
	capturedTargetDB    string
	capturedTargetRole  string
	backendOSStart      string
	serverSocketInode   string

	directTargetDSN   string
	directControlDSN  string
	directObserverDSN string
	targetKey         recovery.TargetKey
	controlKey        recovery.TargetKey
}

// borrowedIdentitySetup is the fixture-owned declared identity anchored before
// any capture: the actual container plus the declared target/control/observer
// DSNs and the shared per-prefix serialization. It carries no public authority
// and no getter constructor from projections.
type borrowedIdentitySetup struct {
	fixture          *originGateFixture
	state            *borrowedIdentityState
	adminMu          *sync.Mutex
	strictHelperPath string

	directTargetDSN   string
	directControlDSN  string
	directObserverDSN string
}

// borrowedIdentityRoute derives the directly reachable container route for a
// NEW original DSN set: the actual container IP plus the actual server listen
// port read from trusted fixture SQL (never assumed).
func borrowedIdentityRoute(t *testing.T, ctx context.Context, fx *originGateFixture, database string) string {
	t.Helper()
	containerIP, err := fx.container.ContainerIP(ctx)
	if err != nil || containerIP == "" {
		t.Fatalf("actual fixture container IP unavailable: %v", err)
	}
	var listenPort int
	if err := fx.admin.QueryRow(ctx, `SELECT inet_server_port()::int`).Scan(&listenPort); err != nil || listenPort <= 0 {
		t.Fatalf("actual server listen port is not proven: err=%v port=%d", err, listenPort)
	}
	parsed, err := url.Parse(fx.dsn)
	if err != nil {
		t.Fatalf("parse fixture base DSN: %v", err)
	}
	parsed.Host = net.JoinHostPort(containerIP, strconv.Itoa(listenPort))
	parsed.Path = "/" + database
	return parsed.String()
}

// buildBorrowedIdentityStrictHelper compiles the standalone strict namespace
// inspector as a static Go binary from the ignore-tagged helper source.
func buildBorrowedIdentityStrictHelper(t *testing.T) string {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate borrowed identity test source for the strict helper")
	}
	source := filepath.Join(filepath.Dir(testFile), "borrowed-identity-strict-client_linux_testhelper.go")
	output := filepath.Join(t.TempDir(), "borrowed-identity-strict-client")
	cmd := exec.Command("go", "build", "-trimpath", "-o", output, source)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if result, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build strict namespace helper: %v: %s", err, strings.TrimSpace(string(result)))
	}
	return output
}

// newBorrowedIdentityFixture creates the direct-route original DSNs BEFORE any
// TargetKey/fingerprint/lock/factory run, then realizes the real factory run
// and captures the opaque control anchor. Existing fixture fields and proxy
// modes are not mutated.
func newBorrowedIdentityFixture(t *testing.T) *borrowedIdentityFixture {
	t.Helper()
	ctx := context.Background()
	fx := newOriginGateFixture(t)
	// The fixture must have the census helper staged (protected startup path).
	if fx.containerID == "" || fx.postmasterPID <= 0 || fx.postmasterStr == "" {
		t.Fatal("fixture-owned container/postmaster identity is unavailable")
	}
	strictHelper := buildBorrowedIdentityStrictHelper(t)
	if err := fx.container.CopyFileToContainer(ctx, strictHelper, borrowedIdentityStrictHelperPath, 0o700); err != nil {
		t.Fatalf("copy strict namespace helper into the owned fixture: %v", err)
	}
	nano := time.Now().UnixNano()
	controlDB := fmt.Sprintf("borrowed_identity_ctrl_%d", nano)
	targetDB := fmt.Sprintf("borrowed_identity_target_%d", nano)
	if err := fx.createOwnedDatabase(ctx, controlDB); err != nil {
		t.Fatalf("create identity control database: %v", err)
	}
	if err := fx.createOwnedDatabase(ctx, targetDB); err != nil {
		t.Fatalf("create identity target database: %v", err)
	}
	directControlDSN := borrowedIdentityRoute(t, ctx, fx, controlDB)
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: directControlDSN, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS,
	}, io.Discard); err != nil {
		t.Fatalf("migrate direct-route control database: %v", err)
	}
	ctrlPool, err := pgxpool.New(ctx, directControlDSN)
	if err != nil {
		t.Fatalf("open direct-route control pool: %v", err)
	}
	t.Cleanup(ctrlPool.Close)
	store, err := controlstore.NewStore(ctx, ctrlPool)
	if err != nil {
		t.Fatalf("controlstore.NewStore on the direct route: %v", err)
	}
	directTargetDSN := borrowedIdentityRoute(t, ctx, fx, targetDB)
	directObserver := borrowedIdentityRoute(t, ctx, fx, targetDB)
	if parsed, err := url.Parse(directObserver); err == nil {
		query := parsed.Query()
		query.Set("application_name", "borrowed_identity_observer")
		parsed.RawQuery = query.Encode()
		directObserver = parsed.String()
	}
	target, err := controlstore.ParseDSNTarget(directTargetDSN)
	if err != nil {
		t.Fatalf("parse direct-route target identity: %v", err)
	}
	targetKey, err := recovery.CanonicalTargetKey(target)
	if err != nil {
		t.Fatalf("direct-route target key: %v", err)
	}
	controlTarget, err := controlstore.ParseDSNTarget(directControlDSN)
	if err != nil {
		t.Fatalf("parse direct-route control identity: %v", err)
	}
	controlKey, err := recovery.CanonicalTargetKey(controlTarget)
	if err != nil {
		t.Fatalf("direct-route control key: %v", err)
	}
	lock, err := recovery.AcquireTargetLock(ctx, directControlDSN, targetKey, 3*time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire real borrowed lock on the direct route: %v", err)
	}
	t.Cleanup(func() { _ = lock.Release(context.Background()) })
	tools, err := recovery.DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed tools: %v", err)
	}
	endpoint, err := recovery.DrillOpenOriginEndpoint()
	if err != nil {
		t.Fatalf("open factory endpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Listener().Close() })
	refuseProbe := func(context.Context, recovery.TargetWriterProof) (recovery.TargetWriterProbeResult, error) {
		return recovery.TargetWriterProbeResult{}, fmt.Errorf("borrowed identity prefix probe intentionally refuses")
	}
	refuseAcceptance := func(context.Context, pgx.Tx, recovery.TargetWriterProof) (recovery.TargetWriterAcceptance, error) {
		return recovery.TargetWriterAcceptance{}, fmt.Errorf("borrowed identity prefix acceptance intentionally refuses")
	}
	run, err := recovery.DrillNewBorrowedWriterRun(ctx, recovery.TargetWriterOptions{
		OperationKind: recovery.TargetWriterOperationRestore,
		Store:         store, ControlDSN: directControlDSN, TargetDSN: directTargetDSN,
		ObserverDSN: directObserver, TrustedTarget: target,
		OperationID: fmt.Sprintf("borrowed-identity-%d", nano),
		Archive:     strings.NewReader("archive"),
		Probe:       refuseProbe, Acceptance: refuseAcceptance,
	}, lock, endpoint, tools)
	if err != nil {
		t.Fatalf("direct-route borrowed writer run factory: %v", err)
	}
	setup := &borrowedIdentitySetup{
		fixture: fx, state: &borrowedIdentityState{}, adminMu: &sync.Mutex{},
		strictHelperPath: borrowedIdentityStrictHelperPath,
		directTargetDSN:  directTargetDSN, directControlDSN: directControlDSN, directObserverDSN: directObserver,
	}
	return &borrowedIdentityFixture{
		fixture: fx, run: run, setup: setup, state: setup.state,
		adminMu: setup.adminMu, strictHelperPath: setup.strictHelperPath,
		directTargetDSN: directTargetDSN, directControlDSN: directControlDSN, directObserverDSN: directObserver,
		targetKey: targetKey, controlKey: controlKey,
	}
}

// newBorrowedIdentityCandidateSetup builds a fresh candidate pairing against an
// actual fixture with a fixture-owned declared target DSN. It carries a fresh
// shared state so a refused candidate never invalidates an original prefix.
func newBorrowedIdentityCandidateSetup(t *testing.T, fx *originGateFixture, declaredTargetDSN string) *borrowedIdentitySetup {
	t.Helper()
	if fx == nil || fx.containerID == "" || declaredTargetDSN == "" {
		t.Fatal("candidate identity setup requires an actual fixture and declared target")
	}
	return &borrowedIdentitySetup{
		fixture: fx, state: &borrowedIdentityState{}, adminMu: &sync.Mutex{},
		strictHelperPath:  borrowedIdentityStrictHelperPath,
		directTargetDSN:   declaredTargetDSN,
		directControlDSN:  borrowedIdentityRoute(t, context.Background(), fx, databaseOf(t, fx.dsn)),
		directObserverDSN: declaredTargetDSN,
	}
}

// borrowedIdentityFixture is the realized fixture plus its captured prefix.
type borrowedIdentityFixture struct {
	fixture           *originGateFixture
	run               *recovery.DrillBorrowedWriterRun
	setup             *borrowedIdentitySetup
	prefix            *borrowedIdentityPrefix
	adminMu           *sync.Mutex
	strictHelperPath  string
	directTargetDSN   string
	directControlDSN  string
	directObserverDSN string
	targetKey         recovery.TargetKey
	controlKey        recovery.TargetKey
	state             *borrowedIdentityState
}

// capture is the actual error-returning capture path used by negatives: any
// failed fact or verification invalidates the shared state before the error is
// returned. There is no authority projection and no caller-supplied fact.
func (f *borrowedIdentityFixture) capture(ctx context.Context) (*borrowedIdentityPrefix, error) {
	prefix, err := f.setup.capture(ctx, f.run)
	if err != nil {
		return nil, err
	}
	f.prefix = prefix
	return prefix, nil
}

// Capture is the outer positive/assert wrapper. Negative proofs must use the
// error-returning capture method instead.
func (f *borrowedIdentityFixture) Capture(t *testing.T) *borrowedIdentityPrefix {
	t.Helper()
	prefix, err := f.capture(context.Background())
	if err != nil {
		t.Fatalf("borrowed identity capture: %v", err)
	}
	return prefix
}

// capture binds a concrete factory-created run to this fixture-owned declared
// identity and validates the full window. The candidate keys are the run's own
// immutable binding keys; the declared fixture target/control identities come
// from this setup. Any failure permanently invalidates the setup's shared
// state.
func (s *borrowedIdentitySetup) capture(ctx context.Context, run *recovery.DrillBorrowedWriterRun) (*borrowedIdentityPrefix, error) {
	if ctx == nil || s == nil || s.fixture == nil || s.state == nil {
		return nil, borrowedIdentityRefuse(reasonPrefixAbsent)
	}
	if run == nil {
		return nil, borrowedIdentityRefuse(reasonPrefixAbsent)
	}
	if identity := run.Observation().StartedIdentity(); identity.Started {
		s.state.invalidate("started writer child")
		return nil, borrowedIdentityRefuse(reasonStartedChild)
	}
	anchor, err := run.CaptureControlAnchor(ctx)
	if err != nil {
		s.state.invalidate("control anchor capture refused")
		return nil, err
	}
	binding := run.Binding()
	prefix := &borrowedIdentityPrefix{
		fixture: s.fixture, run: run, anchor: anchor, state: s.state,
		adminMu: s.adminMu, strictHelperPath: s.strictHelperPath,
		directTargetDSN: s.directTargetDSN, directControlDSN: s.directControlDSN,
		directObserverDSN: s.directObserverDSN,
		targetKey:         binding.OriginalTargetKey(), controlKey: binding.ControlTargetKey(),
	}
	if err := prefix.inspect(ctx); err != nil {
		s.state.invalidate("identity verification refused")
		return nil, err
	}
	if identity := run.Observation().StartedIdentity(); identity.Started {
		s.state.invalidate("started writer child")
		return nil, borrowedIdentityRefuse(reasonStartedChild)
	}
	return prefix, nil
}

// Verify is the outer assertion wrapper around the bounded error routine. It
// is not used by Recheck or by negative capture proofs.
func (p *borrowedIdentityPrefix) Verify(t *testing.T) {
	t.Helper()
	if err := p.verification(context.Background()); err != nil {
		t.Fatalf("identity verification: %v", err)
	}
}

// inspect is the actual error-returning verification used by capture and
// Recheck. Any failed fact invalidates the shared state before returning.
func (p *borrowedIdentityPrefix) inspect(ctx context.Context) error {
	if p == nil || p.state == nil {
		return borrowedIdentityRefuse(reasonPrefixAbsent)
	}
	if err := p.verification(ctx); err != nil {
		p.state.invalidate("identity verification refused")
		return err
	}
	return nil
}

// verification performs the full private identity window and returns a typed
// error result instead of calling testing.T.Fatal anywhere. The window is:
//
//	initial independent SQL check -> initial strict OS census -> final genuine
//	anchor recheck -> end independent SQL check -> end strict OS census ->
//	publication under the shared state mutex
//
// The expected values are the retained immutable tokens captured by the first
// inspection and are never overwritten with fresh values.
func (p *borrowedIdentityPrefix) verification(ctx context.Context) error {
	if p == nil || p.state == nil {
		return borrowedIdentityRefuse(reasonPrefixAbsent)
	}
	if ctx == nil {
		return borrowedIdentityRefuse(reasonContextEnded)
	}
	if reason := p.state.invalidReason(); reason != "" {
		return borrowedIdentityRefuse(reasonPrefixInvalid)
	}
	bounded, cancel := context.WithTimeout(ctx, borrowedIdentityVerificationBound)
	defer cancel()

	facts := p.anchor.Diagnostics()
	if !facts.Present || facts.Invalidated {
		return borrowedIdentityRefuse(reasonAnchorUnavailable)
	}
	binding := p.run.Binding()
	if !p.capturedFacts.Present {
		declaredDatabase, err := borrowedIdentityDatabase(p.directTargetDSN)
		if err != nil {
			return borrowedIdentityRefuse(reasonDeclaredTarget)
		}
		declaredRole, err := borrowedIdentityRole(p.directTargetDSN)
		if err != nil {
			return borrowedIdentityRefuse(reasonDeclaredTarget)
		}
		if binding.TargetDatabaseOID() == 0 || binding.WriterRoleOID() == 0 {
			return borrowedIdentityRefuse(reasonDeclaredTarget)
		}
		p.capturedFacts = facts
		p.capturedTargetDBOID = binding.TargetDatabaseOID()
		p.capturedRoleOID = binding.WriterRoleOID()
		p.capturedTargetDB = declaredDatabase
		p.capturedTargetRole = declaredRole
	}
	expected := p.capturedFacts
	if expected.OriginalTargetKey != p.targetKey || expected.ControlTargetKey != p.controlKey {
		return borrowedIdentityRefuse(reasonKeyBinding)
	}
	if expected.TransportTargetKey == p.targetKey {
		return borrowedIdentityRefuse(reasonNamespaceBinding)
	}
	namespaceKey1, namespaceKey2 := p.targetKey.AdvisoryLockKey()
	if int32(expected.NamespaceClassID) != namespaceKey1 || int32(expected.NamespaceObjectID) != namespaceKey2 {
		return borrowedIdentityRefuse(reasonNamespaceBinding)
	}

	// Initial window: independent SQL check, strict OS census, genuine anchor
	// recheck.
	if err := p.inspectSQLPhase(bounded, expected, false); err != nil {
		return err
	}
	if err := p.inspectOS(bounded, expected, borrowedIdentityStrictPhaseInitial); err != nil {
		return err
	}
	if err := p.anchor.Recheck(bounded); err != nil {
		return borrowedIdentityRefuse(reasonAnchorRecheck)
	}

	// End-of-window rechecks against the ORIGINAL retained tokens.
	borrowedIdentityPauseAt(borrowedIdentityStageEndWindow)
	if err := p.recheckPostmasterIncarnation(bounded); err != nil {
		return err
	}
	if err := p.inspectSQLPhase(bounded, expected, true); err != nil {
		return err
	}
	if err := p.inspectOS(bounded, expected, borrowedIdentityStrictPhaseEnd); err != nil {
		return err
	}

	// Publication under the shared state mutex: copied invalidation first,
	// then an ended own context recorded directly as permanent loss (no
	// recursive invalidate, no I/O under the mutex). The decision is the
	// linearization point; a later cancellation cannot revoke published
	// success.
	borrowedIdentityPauseAt(borrowedIdentityStageInspectPublish)
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	if p.state.invalid {
		return borrowedIdentityRefuse(reasonConcurrentInvalid)
	}
	if err := bounded.Err(); err != nil {
		p.state.invalid = true
		p.state.reason = borrowedIdentityContextEndedReason
		return borrowedIdentityRefuse(reasonContextEnded)
	}
	return nil
}

// inspectSQLPhase runs the read-only SQL identity checks on a fresh independent
// connection to the actual owned fixture, serialized per prefix lifetime by
// the shared admin mutex with context-aware acquisition. The expected catalog
// OIDs and anchor facts are the retained immutable values; an end phase reports
// end-specific safe predicates.
func (p *borrowedIdentityPrefix) inspectSQLPhase(ctx context.Context, expected recovery.DrillControlAnchorFacts, end bool) error {
	if p.adminMu == nil {
		return borrowedIdentityRefuse(reasonPrefixAbsent)
	}
	stage := func(initial, endReason borrowedIdentityReason) borrowedIdentityReason {
		if end {
			return endReason
		}
		return initial
	}
	if err := borrowedIdentityAcquireMu(ctx, p.adminMu); err != nil {
		return borrowedIdentityRefuse(reasonSQLConnect)
	}
	defer p.adminMu.Unlock()
	connectCtx, cancelConnect := context.WithTimeout(ctx, borrowedIdentitySQLConnectBound)
	conn, err := pgx.Connect(connectCtx, p.fixture.dsn)
	cancelConnect()
	if err != nil {
		return borrowedIdentityRefuse(reasonSQLConnect)
	}
	defer func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), time.Second)
		_ = conn.Close(closeCtx)
		cancelClose()
	}()

	var (
		backendStart time.Time
		backID       uint32
		postmaster   time.Time
		systemID     string
		granted      int
		targetDBOID  uint32
		roleOID      uint32
	)
	if err := conn.QueryRow(ctx, `SELECT backend_start, datid::oid FROM pg_stat_activity WHERE pid=$1`, expected.BackendPID).
		Scan(&backendStart, &backID); err != nil {
		return borrowedIdentityRefuse(stage(reasonSQLBackend, reasonEndSQLBackend))
	}
	if !backendStart.Equal(expected.BackendStart) || backID != expected.ControlDatabaseOID {
		return borrowedIdentityRefuse(stage(reasonSQLBackend, reasonEndSQLBackend))
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND granted AND objsubid=2 AND classid=$2::oid AND objid=$3::oid AND database=$4::oid`,
		expected.BackendPID, expected.NamespaceClassID, expected.NamespaceObjectID, expected.NamespaceDatabaseID).Scan(&granted); err != nil || granted != 1 {
		return borrowedIdentityRefuse(stage(reasonSQLNamespace, reasonEndSQLNamespace))
	}
	if err := conn.QueryRow(ctx, `SELECT pg_postmaster_start_time(), system_identifier::text FROM pg_control_system()`).Scan(&postmaster, &systemID); err != nil {
		return borrowedIdentityRefuse(stage(reasonSQLIncarnation, reasonEndSQLIncarnation))
	}
	if !postmaster.Equal(expected.PostmasterStart) || systemID == "" || systemID != expected.SystemIdentifier {
		return borrowedIdentityRefuse(stage(reasonSQLIncarnation, reasonEndSQLIncarnation))
	}
	if err := conn.QueryRow(ctx, `SELECT (SELECT oid FROM pg_database WHERE datname=$1), (SELECT oid FROM pg_roles WHERE rolname=$2)`, p.capturedTargetDB, p.capturedTargetRole).
		Scan(&targetDBOID, &roleOID); err != nil {
		return borrowedIdentityRefuse(stage(reasonSQLTargetCatalog, reasonEndSQLTargetCatalog))
	}
	if p.capturedTargetDBOID == 0 || p.capturedRoleOID == 0 || targetDBOID != p.capturedTargetDBOID || roleOID != p.capturedRoleOID {
		return borrowedIdentityRefuse(stage(reasonSQLTargetCatalog, reasonEndSQLTargetCatalog))
	}
	return nil
}

// inspectOS strictly inspects the exact required control backend in the owned
// fixture container. The initial phase establishes or verifies the immutable
// OS start and server socket tokens; the end phase re-reads and compares them
// against the retained tokens.
func (p *borrowedIdentityPrefix) inspectOS(ctx context.Context, expected recovery.DrillControlAnchorFacts, phase borrowedIdentityStrictPhase) error {
	pmPID, pmStart, err := inspectPostmaster(ctx, p.fixture.containerID)
	if err != nil {
		if phase == borrowedIdentityStrictPhaseEnd {
			return borrowedIdentityRefuse(reasonEndOSPostmaster)
		}
		return borrowedIdentityRefuse(reasonOSPostmaster)
	}
	if pmPID != p.fixture.postmasterPID || pmStart != p.fixture.postmasterStr {
		if phase == borrowedIdentityStrictPhaseEnd {
			return borrowedIdentityRefuse(reasonEndOSPostmaster)
		}
		return borrowedIdentityRefuse(reasonOSPostmaster)
	}
	requiredInode := p.serverSocketInode
	if phase == borrowedIdentityStrictPhaseInitial && requiredInode == "" {
		requiredInode = ""
	}
	census, err := inspectBorrowedIdentityStrict(ctx, p.fixture.containerID, p.strictHelperPath, pmPID, pmStart, expected.BackendPID, requiredInode, phase)
	if err != nil {
		return err
	}
	if phase == borrowedIdentityStrictPhaseEnd {
		return p.compareEndCensus(expected, census)
	}
	return p.establishInitialTokens(expected, census)
}

// establishInitialTokens sets the immutable tokens on the first inspection and
// compares against them on subsequent inspections; fresh values never
// overwrite the retained tokens.
func (p *borrowedIdentityPrefix) establishInitialTokens(expected recovery.DrillControlAnchorFacts, census borrowedIdentityStrictCensus) error {
	if census.Backend.PID != expected.BackendPID || census.Backend.State == "Z" {
		return borrowedIdentityRefuse(reasonOSBackend)
	}
	matchedInode, err := p.matchServerSocket(expected, census)
	if err != nil {
		return err
	}
	if p.backendOSStart == "" {
		p.backendOSStart = census.Backend.StartBefore
		p.serverSocketInode = matchedInode
		return nil
	}
	if census.Backend.StartBefore != p.backendOSStart {
		return borrowedIdentityRefuse(reasonOSBackend)
	}
	if matchedInode != p.serverSocketInode {
		return borrowedIdentityRefuse(reasonOSServerInode)
	}
	return nil
}

// compareEndCensus rechecks the end-of-window census against the retained
// tokens.
func (p *borrowedIdentityPrefix) compareEndCensus(expected recovery.DrillControlAnchorFacts, census borrowedIdentityStrictCensus) error {
	if census.Backend.PID != expected.BackendPID || census.Backend.State == "Z" {
		return borrowedIdentityRefuse(reasonEndOSBackendStart)
	}
	if p.backendOSStart == "" || census.Backend.StartBefore != p.backendOSStart {
		return borrowedIdentityRefuse(reasonEndOSBackendStart)
	}
	matchedInode, err := p.matchServerSocket(expected, census)
	if err != nil {
		return err
	}
	if p.serverSocketInode == "" || matchedInode != p.serverSocketInode {
		return borrowedIdentityRefuse(reasonEndOSServerInode)
	}
	return nil
}

// matchServerSocket requires exactly one established TCP socket of the
// required backend reversing the anchor client/server tuples.
func (p *borrowedIdentityPrefix) matchServerSocket(expected recovery.DrillControlAnchorFacts, census borrowedIdentityStrictCensus) (string, error) {
	anchorClientIP, anchorClientPort, err := borrowedIdentityTuple(expected.LocalAddress, expected.LocalPort)
	if err != nil {
		return "", borrowedIdentityRefuse(reasonOSServerInode)
	}
	anchorServerIP, anchorServerPort, err := borrowedIdentityTuple(expected.RemoteAddress, expected.RemotePort)
	if err != nil {
		return "", borrowedIdentityRefuse(reasonOSServerInode)
	}
	matched := 0
	matchedInode := ""
	for _, candidate := range census.Sockets {
		if !candidate.TCP || candidate.State != "01" {
			continue
		}
		localIP, localPort, err := borrowedIdentityEndpoint(candidate.Local)
		if err != nil {
			return "", borrowedIdentityRefuse(reasonOSServerInode)
		}
		remoteIP, remotePort, err := borrowedIdentityEndpoint(candidate.Remote)
		if err != nil {
			return "", borrowedIdentityRefuse(reasonOSServerInode)
		}
		if localIP == anchorServerIP && localPort == anchorServerPort &&
			remoteIP == anchorClientIP && remotePort == anchorClientPort {
			matched++
			matchedInode = candidate.Inode
		}
	}
	if matched != 1 {
		return "", borrowedIdentityRefuse(reasonOSServerInode)
	}
	return matchedInode, nil
}

// recheckPostmasterIncarnation re-reads the exact fixture postmaster after the
// anchor recheck so the SQL anchor facts stay inside the OS incarnation
// bracket.
func (p *borrowedIdentityPrefix) recheckPostmasterIncarnation(ctx context.Context) error {
	pmPID, pmStart, err := inspectPostmaster(ctx, p.fixture.containerID)
	if err != nil {
		return borrowedIdentityRefuse(reasonEndOSPostmaster)
	}
	if pmPID != p.fixture.postmasterPID || pmStart != p.fixture.postmasterStr {
		return borrowedIdentityRefuse(reasonEndOSPostmaster)
	}
	return nil
}

// borrowedIdentityAcquireMu acquires the prefix admin mutex with context-aware
// TryLock polling; no locker goroutine is ever abandoned.
func borrowedIdentityAcquireMu(ctx context.Context, mu *sync.Mutex) error {
	if ctx == nil || mu == nil {
		return borrowedIdentityRefuse(reasonPrefixAbsent)
	}
	if mu.TryLock() {
		return nil
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if mu.TryLock() {
				return nil
			}
		}
	}
}

// Recheck captures a fresh proof and permanently invalidates the shared state
// on any loss, mismatch, uncertain metadata or context end. The signature is
// retained for the existing tests; it delegates to the error-returning
// inspect routine and keeps no testing.T in the verification path.
func (p *borrowedIdentityPrefix) Recheck(t *testing.T, ctx context.Context) error {
	t.Helper()
	return p.inspect(ctx)
}

// Invalid reports the shared permanent invalidation state (copy-shared).
func (p *borrowedIdentityPrefix) Invalid() (bool, string) {
	reason := p.state.invalidReason()
	return reason != "", reason
}

// borrowedIdentityStrictProcess is the private parsed process record of the
// strict census.
type borrowedIdentityStrictProcess struct {
	PID         int
	StartBefore string
	StartAfter  string
	State       string
	PPID        int
}

// borrowedIdentityStrictEndpoint is the private parsed socket record.
type borrowedIdentityStrictEndpoint struct {
	Inode  string
	TCP    bool
	Local  string
	Remote string
	State  string
}

// borrowedIdentityStrictCensus is the parsed strict namespace census.
type borrowedIdentityStrictCensus struct {
	OK                   bool
	Complete             bool
	Errors               []string
	Postmaster           borrowedIdentityStrictProcess
	Backend              borrowedIdentityStrictProcess
	FDTotal              int
	FDReadable           int
	FDENOENT             int
	FDUnknown            int
	Sockets              []borrowedIdentityStrictEndpoint
	RequiredInode        string
	RequiredInodePresent bool
}

// borrowedIdentityParseStrictReport parses one strict census JSON document. It
// performs structural decoding only; invariant validation is separate so the
// deterministic negative report seam can inject structurally valid changes
// before they are validated and bound by the retained tokens.
func borrowedIdentityParseStrictReport(raw []byte) (borrowedIdentityStrictCensus, error) {
	var document struct {
		OK         bool     `json:"ok"`
		Complete   bool     `json:"complete"`
		Errors     []string `json:"errors"`
		Postmaster struct {
			PID         int    `json:"pid"`
			StartBefore string `json:"start_before"`
			StartAfter  string `json:"start_after"`
			State       string `json:"state"`
			PPID        int    `json:"ppid"`
		} `json:"postmaster"`
		Backend struct {
			PID         int    `json:"pid"`
			StartBefore string `json:"start_before"`
			StartAfter  string `json:"start_after"`
			State       string `json:"state"`
			PPID        int    `json:"ppid"`
		} `json:"backend"`
		FDTotal    int `json:"fd_total"`
		FDReadable int `json:"fd_readable"`
		FDENOENT   int `json:"fd_enoent"`
		FDUnknown  int `json:"fd_unknown"`
		Sockets    []struct {
			Inode  string `json:"inode"`
			TCP    bool   `json:"tcp"`
			Local  string `json:"local"`
			Remote string `json:"remote"`
			State  string `json:"state"`
		} `json:"sockets"`
		RequiredInode        string `json:"required_inode"`
		RequiredInodePresent bool   `json:"required_inode_present"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return borrowedIdentityStrictCensus{}, borrowedIdentityRefuse(reasonStrictMalformed)
	}
	census := borrowedIdentityStrictCensus{
		OK: document.OK, Complete: document.Complete, Errors: append([]string(nil), document.Errors...),
		Postmaster: borrowedIdentityStrictProcess{PID: document.Postmaster.PID, StartBefore: document.Postmaster.StartBefore, StartAfter: document.Postmaster.StartAfter, State: document.Postmaster.State, PPID: document.Postmaster.PPID},
		Backend:    borrowedIdentityStrictProcess{PID: document.Backend.PID, StartBefore: document.Backend.StartBefore, StartAfter: document.Backend.StartAfter, State: document.Backend.State, PPID: document.Backend.PPID},
		FDTotal:    document.FDTotal, FDReadable: document.FDReadable, FDENOENT: document.FDENOENT, FDUnknown: document.FDUnknown,
		RequiredInode: document.RequiredInode, RequiredInodePresent: document.RequiredInodePresent,
	}
	for _, socket := range document.Sockets {
		census.Sockets = append(census.Sockets, borrowedIdentityStrictEndpoint{Inode: socket.Inode, TCP: socket.TCP, Local: socket.Local, Remote: socket.Remote, State: socket.State})
	}
	return census, nil
}

// borrowedIdentityValidateStrictReport validates the report invariants before
// any positive use: explicit completeness, expected and nonzero identities,
// same before/after starts and PIDs, direct postmaster parenthood, non-zombie
// states, complete FD accounting with zero unknown entries, consistent socket
// metadata and required-inode presence.
func borrowedIdentityValidateStrictReport(census borrowedIdentityStrictCensus, postmasterPID int, postmasterStart string, backendPID int, requiredInode string) error {
	if !census.OK || !census.Complete || len(census.Errors) != 0 {
		return borrowedIdentityRefuse(reasonStrictIncomplete)
	}
	if postmasterPID < 2 || backendPID < 2 || census.Postmaster.PID != postmasterPID || census.Backend.PID != backendPID {
		return borrowedIdentityRefuse(reasonStrictReportPID)
	}
	if census.Postmaster.StartBefore == "" || census.Postmaster.StartBefore != census.Postmaster.StartAfter ||
		census.Postmaster.StartBefore != postmasterStart || census.Postmaster.State == "" || census.Postmaster.State == "Z" {
		return borrowedIdentityRefuse(reasonStrictReportPost)
	}
	if census.Backend.StartBefore == "" || census.Backend.StartBefore != census.Backend.StartAfter {
		return borrowedIdentityRefuse(reasonStrictReportBackend)
	}
	if census.Backend.State == "" || census.Backend.State == "Z" {
		return borrowedIdentityRefuse(reasonStrictReportState)
	}
	if census.Backend.PPID != postmasterPID {
		return borrowedIdentityRefuse(reasonStrictReportPPID)
	}
	if census.FDTotal < 0 || census.FDReadable < 0 || census.FDENOENT < 0 || census.FDUnknown < 0 {
		return borrowedIdentityRefuse(reasonStrictReportFD)
	}
	if census.FDTotal != census.FDReadable+census.FDENOENT+census.FDUnknown {
		return borrowedIdentityRefuse(reasonStrictReportFD)
	}
	if census.FDUnknown != 0 {
		return borrowedIdentityRefuse(reasonStrictReportFDUnk)
	}
	for _, socket := range census.Sockets {
		if socket.Inode == "" {
			return borrowedIdentityRefuse(reasonStrictReportSocket)
		}
		if socket.TCP && (socket.Local == "" || socket.Remote == "" || socket.State == "") {
			return borrowedIdentityRefuse(reasonStrictReportSocket)
		}
	}
	if requiredInode == "" {
		if census.RequiredInode != "" || census.RequiredInodePresent {
			return borrowedIdentityRefuse(reasonStrictRequiredInode)
		}
	} else {
		if census.RequiredInode != requiredInode || !census.RequiredInodePresent {
			return borrowedIdentityRefuse(reasonStrictRequiredInode)
		}
		present := false
		for _, socket := range census.Sockets {
			if socket.Inode == requiredInode {
				present = true
				break
			}
		}
		if !present {
			return borrowedIdentityRefuse(reasonStrictRequiredInode)
		}
	}
	return nil
}

// inspectBorrowedIdentityStrict runs the private strict helper inside the
// owned fixture container and validates the report invariants. Any exec
// failure, malformed output, incomplete census or inconsistent report is an
// UNKNOWN error (never a weak skip); a missing required backend/inode is
// reported as a refusal.
func inspectBorrowedIdentityStrict(ctx context.Context, containerID, helperPath string, postmasterPID int, postmasterStart string, backendPID int, requiredInode string, phase borrowedIdentityStrictPhase) (borrowedIdentityStrictCensus, error) {
	args := []string{helperPath, "inspect", strconv.Itoa(postmasterPID), postmasterStart, strconv.Itoa(backendPID)}
	if requiredInode != "" {
		args = append(args, requiredInode)
	}
	out, err := dockerExec(ctx, containerID, args...)
	if err != nil {
		return borrowedIdentityStrictCensus{}, borrowedIdentityRefuse(reasonStrictExec)
	}
	census, err := borrowedIdentityParseStrictReport(out)
	if err != nil {
		return borrowedIdentityStrictCensus{}, err
	}
	if hook := borrowedIdentityStrictReportHook.Load(); hook != nil {
		(*hook)(&census, phase)
	}
	if err := borrowedIdentityValidateStrictReport(census, postmasterPID, postmasterStart, backendPID, requiredInode); err != nil {
		return census, err
	}
	return census, nil
}

// borrowedIdentityDatabase extracts the database name from a fixture DSN
// without any test-fatal side effect.
func borrowedIdentityDatabase(dsn string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", borrowedIdentityRefuse(reasonDeclaredTarget)
	}
	name := strings.TrimPrefix(parsed.Path, "/")
	if name == "" {
		return "", borrowedIdentityRefuse(reasonDeclaredTarget)
	}
	return name, nil
}

// borrowedIdentityRole extracts the role name from a fixture DSN without any
// test-fatal side effect.
func borrowedIdentityRole(dsn string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", borrowedIdentityRefuse(reasonDeclaredTarget)
	}
	if parsed.User == nil {
		return "", borrowedIdentityRefuse(reasonDeclaredTarget)
	}
	role := parsed.User.Username()
	if role == "" {
		return "", borrowedIdentityRefuse(reasonDeclaredTarget)
	}
	return role, nil
}

// borrowedIdentityTuple splits an anchor address string and checks it against
// the separately projected port, normalizing IPv4-mapped addresses.
func borrowedIdentityTuple(address string, port int) (string, int, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, fmt.Errorf("malformed address string")
	}
	parsedPort, err := strconv.Atoi(portText)
	if err != nil || parsedPort <= 0 || parsedPort != port {
		return "", 0, fmt.Errorf("address and projected port disagree")
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return "", 0, fmt.Errorf("malformed address")
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	return ip.String(), parsedPort, nil
}

// borrowedIdentityEndpoint normalizes a strict census ip:port string.
func borrowedIdentityEndpoint(raw string) (string, int, error) {
	host, portText, err := net.SplitHostPort(raw)
	if err != nil {
		return "", 0, fmt.Errorf("strict census endpoint is malformed")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 {
		return "", 0, fmt.Errorf("strict census endpoint port is malformed")
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return "", 0, fmt.Errorf("strict census endpoint address is malformed")
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	return ip.String(), port, nil
}

func databaseOf(t *testing.T, dsn string) string {
	t.Helper()
	name, err := borrowedIdentityDatabase(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return name
}

func roleOf(t *testing.T, dsn string) string {
	t.Helper()
	role, err := borrowedIdentityRole(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return role
}

// decodeProcNetTCP decodes the kernel /proc/net/tcp hex endpoint form with
// IPv4-mapped IPv6 normalization and a decimal port. Wildcards are decoded as
// such and can never equal a concrete anchor tuple.
func decodeProcNetTCP(raw string) (string, int, error) {
	parts := strings.Split(raw, ":")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("malformed endpoint")
	}
	port64, err := strconv.ParseInt(parts[1], 16, 32)
	if err != nil {
		return "", 0, fmt.Errorf("malformed port")
	}
	addressBytes, err := hex.DecodeString(parts[0])
	if err != nil {
		return "", 0, fmt.Errorf("malformed address")
	}
	var ip net.IP
	switch len(addressBytes) {
	case 4:
		// /proc/net/tcp stores IPv4 in little-endian byte order.
		ip = net.IPv4(addressBytes[3], addressBytes[2], addressBytes[1], addressBytes[0])
	case 16:
		// /proc/net/tcp6 stores four little-endian 32-bit words; normalize the
		// IPv4-mapped form to its 4-byte address, otherwise keep the raw IPv6.
		mapped := true
		for _, b := range addressBytes[:10] {
			if b != 0 {
				mapped = false
				break
			}
		}
		if mapped && addressBytes[10] == 0xff && addressBytes[11] == 0xff {
			ip = net.IPv4(addressBytes[15], addressBytes[14], addressBytes[13], addressBytes[12])
		} else {
			ip = net.IP(addressBytes)
		}
	default:
		return "", 0, fmt.Errorf("unsupported address width")
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	return ip.String(), int(port64), nil
}
