//go:build linux && drill

// borrowed-identity-prefix-negatives_linux_test.go is the negative group for
// the identity prefix. Every negative now runs the ACTUAL error-returning
// prefix capture/verification on a REAL concrete factory run paired with an
// actual wrong fixture or an actual wrong target catalog: the foreign pairing
// is refused by the real trusted-SQL/strict-window predicates, the wrong
// catalog pairing is refused by the declared fixture expectation against the
// original immutable binding, and the owned restart records the actual
// predicate that fails (truthfully, whether SQL or OS first). No old census
// stage-walker is used as the proof predicate. No writer child is launched and
// no claim/first-frame authority is created anywhere.
package recovery_test

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// TestBorrowedIdentityPrefixRejectsForeignFixtureIdentity pairs the REAL
// factory run of the first fixture with the second fixture's declared identity:
// same-looking database and role names, but genuinely different clusters and
// incarnations. The actual prefix verification must refuse through its typed
// predicates while both original prefixes stay valid and no child is started.
func TestBorrowedIdentityPrefixRejectsForeignFixtureIdentity(t *testing.T) {
	ctx := context.Background()
	first := newBorrowedIdentityFixture(t)
	prefixFirst := first.Capture(t)
	second := newBorrowedIdentityFixture(t)
	prefixSecond := second.Capture(t)

	// Same-looking declared target database name in the second fixture; both
	// fixtures already share the same-looking role name.
	sharedName := databaseOf(t, first.directTargetDSN)
	if err := second.fixture.createOwnedDatabase(ctx, sharedName); err != nil {
		t.Fatalf("create same-named database in the second fixture: %v", err)
	}
	if roleOf(t, first.directTargetDSN) != roleOf(t, second.directTargetDSN) {
		t.Fatal("fixtures do not share the same-looking role name")
	}
	sharedDeclared := borrowedIdentityRoute(t, ctx, second.fixture, sharedName)

	factsFirst := prefixFirst.anchor.Diagnostics()
	factsSecond := prefixSecond.anchor.Diagnostics()
	if factsFirst.SystemIdentifier == "" || factsSecond.SystemIdentifier == "" || factsFirst.SystemIdentifier == factsSecond.SystemIdentifier {
		t.Fatalf("fixtures are not genuinely different clusters: %q vs %q", factsFirst.SystemIdentifier, factsSecond.SystemIdentifier)
	}
	if factsFirst.ControlDatabaseOID == factsSecond.ControlDatabaseOID && factsFirst.PostmasterStart.Equal(factsSecond.PostmasterStart) {
		t.Fatal("fixtures are not genuinely distinct incarnations")
	}

	// Actual caller proof: the real first run paired with the second
	// fixture-owned declared identity must be refused by the actual prefix
	// verification, not by a DSN-string or census-walker predicate.
	candidateSetup := newBorrowedIdentityCandidateSetup(t, second.fixture, sharedDeclared)
	candidate, err := candidateSetup.capture(ctx, first.run)
	if err == nil || candidate != nil {
		t.Fatal("foreign fixture pairing was accepted by the actual prefix verification")
	}
	stage := borrowedIdentityRequireStage(t, err,
		reasonSQLConnect, reasonSQLBackend, reasonSQLNamespace, reasonSQLIncarnation, reasonSQLTargetCatalog, reasonOSBackend, reasonOSServerInode)
	if reason := candidateSetup.state.invalidReason(); reason == "" {
		t.Fatal("refused foreign candidate did not permanently invalidate its shared state")
	}
	for _, fixture := range []*borrowedIdentityFixture{first, second} {
		if invalid, reason := fixture.prefix.Invalid(); invalid {
			t.Fatalf("own-fixture prefix invalidated by the foreign candidate: %s", reason)
		}
		if identity := fixture.run.Observation().StartedIdentity(); identity.Started {
			t.Fatalf("negative identity group started a writer child: %+v", identity)
		}
	}
	if first.run.Binding().OriginalTargetKey() != first.targetKey || first.run.Binding().ControlTargetKey() != first.controlKey {
		t.Fatal("original/control binding keys changed during the foreign comparison")
	}
	t.Logf("causal negative: actual foreign prefix refusal stage=%s", stage)
}

// TestBorrowedIdentityPrefixRejectsWrongFactoryTargetCatalog pairs an
// authentic factory run naming a different REAL target database on the same
// server with the original fixture-owned declared target. The verification
// contract compares the declared fixture expectation against the original
// immutable binding and must refuse at the target-catalog predicate; the
// original prefix and binding stay valid and untouched.
func TestBorrowedIdentityPrefixRejectsWrongFactoryTargetCatalog(t *testing.T) {
	ctx := context.Background()
	base := newBorrowedIdentityFixture(t)
	prefix := base.Capture(t)
	originalBinding := base.run.Binding()

	// Authentic factory run with a different REAL target database on the same
	// fixture cluster; no forged snapshot, no manual claim construction.
	wrongDB := fmt.Sprintf("borrowed_identity_wrong_%d", time.Now().UnixNano())
	if err := base.fixture.createOwnedDatabase(ctx, wrongDB); err != nil {
		t.Fatalf("create wrong target database: %v", err)
	}
	wrongTargetDSN := borrowedIdentityRoute(t, ctx, base.fixture, wrongDB)
	wrongObserver := wrongTargetDSN
	if parsed, err := url.Parse(wrongObserver); err == nil {
		query := parsed.Query()
		query.Set("application_name", "borrowed_identity_wrong_observer")
		parsed.RawQuery = query.Encode()
		wrongObserver = parsed.String()
	}
	wrongTarget, err := controlstore.ParseDSNTarget(wrongTargetDSN)
	if err != nil {
		t.Fatalf("parse wrong target identity: %v", err)
	}
	wrongKey, err := recovery.CanonicalTargetKey(wrongTarget)
	if err != nil {
		t.Fatalf("wrong target key: %v", err)
	}
	controlTarget, err := controlstore.ParseDSNTarget(base.directControlDSN)
	if err != nil {
		t.Fatalf("parse control identity: %v", err)
	}
	controlKey, err := recovery.CanonicalTargetKey(controlTarget)
	if err != nil {
		t.Fatalf("control key: %v", err)
	}
	wrongPool, err := pgxpool.New(ctx, base.directControlDSN)
	if err != nil {
		t.Fatalf("open wrong-run control pool: %v", err)
	}
	t.Cleanup(wrongPool.Close)
	wrongStore, err := controlstore.NewStore(ctx, wrongPool)
	if err != nil {
		t.Fatalf("wrong-run control store: %v", err)
	}
	wrongLock, err := recovery.AcquireTargetLock(ctx, base.directControlDSN, wrongKey, 3*time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire wrong-target lock: %v", err)
	}
	t.Cleanup(func() { _ = wrongLock.Release(context.Background()) })
	wrongTools, err := recovery.DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision wrong-run tools: %v", err)
	}
	wrongEndpoint, err := recovery.DrillOpenOriginEndpoint()
	if err != nil {
		t.Fatalf("open wrong-run endpoint: %v", err)
	}
	t.Cleanup(func() { _ = wrongEndpoint.Listener().Close() })
	wrongRun, err := recovery.DrillNewBorrowedWriterRun(ctx, recovery.TargetWriterOptions{
		OperationKind: recovery.TargetWriterOperationRestore,
		Store:         wrongStore, ControlDSN: base.directControlDSN, TargetDSN: wrongTargetDSN,
		ObserverDSN: wrongObserver, TrustedTarget: wrongTarget,
		OperationID: fmt.Sprintf("borrowed-identity-wrong-%d", time.Now().UnixNano()),
		Archive:     strings.NewReader("archive"),
		Probe: func(context.Context, recovery.TargetWriterProof) (recovery.TargetWriterProbeResult, error) {
			return recovery.TargetWriterProbeResult{}, fmt.Errorf("wrong-catalog probe intentionally refuses")
		},
		Acceptance: func(context.Context, pgx.Tx, recovery.TargetWriterProof) (recovery.TargetWriterAcceptance, error) {
			return recovery.TargetWriterAcceptance{}, fmt.Errorf("wrong-catalog acceptance intentionally refuses")
		},
	}, wrongLock, wrongEndpoint, wrongTools)
	if err != nil {
		t.Fatalf("authentic factory refused the wrong target before capture (stage=factory): %v", err)
	}

	// Genuine SQL catalog evidence: the wrong database is a real distinct
	// catalog object.
	var wrongDBOID, originalDBOID uint32
	if err := base.fixture.admin.QueryRow(ctx,
		`SELECT (SELECT oid FROM pg_database WHERE datname=$1), (SELECT oid FROM pg_database WHERE datname=$2)`,
		wrongDB, databaseOf(t, base.directTargetDSN)).Scan(&wrongDBOID, &originalDBOID); err != nil {
		t.Fatalf("real catalog identity read: %v", err)
	}
	if wrongDBOID == originalDBOID {
		t.Fatal("wrong target database is not a distinct real catalog object")
	}
	if wrongRun.Binding().TargetDatabaseOID() != wrongDBOID {
		t.Fatalf("wrong-run binding is not the real wrong database: captured=%d actual=%d", wrongRun.Binding().TargetDatabaseOID(), wrongDBOID)
	}
	if wrongRun.Binding().TargetDatabaseOID() == originalBinding.TargetDatabaseOID() {
		t.Fatal("wrong-run binding equals the original target catalog")
	}
	if wrongRun.Binding().ControlTargetKey() != controlKey {
		t.Fatalf("wrong-run control key changed: %v", wrongRun.Binding().ControlTargetKey())
	}

	// Actual caller proof: pair the concrete wrong run with the ORIGINAL
	// fixture-owned declared target; verification must refuse at the
	// target-catalog predicate against the immutable binding, and the refused
	// candidate's state is permanently invalid.
	candidateSetup := newBorrowedIdentityCandidateSetup(t, base.fixture, base.directTargetDSN)
	candidate, err := candidateSetup.capture(ctx, wrongRun)
	if err == nil || candidate != nil {
		t.Fatal("wrong target catalog pairing was accepted by the actual prefix verification")
	}
	stage := borrowedIdentityRequireStage(t, err, reasonSQLTargetCatalog)
	if reason := candidateSetup.state.invalidReason(); reason == "" {
		t.Fatal("refused wrong-catalog candidate did not permanently invalidate its shared state")
	}

	// The original binding and prefix remain untouched.
	if invalid, reason := prefix.Invalid(); invalid {
		t.Fatalf("original prefix invalidated by the wrong-target run: %s", reason)
	}
	if base.run.Binding().OriginalTargetKey() != base.targetKey || base.run.Binding().TargetDatabaseOID() != originalBinding.TargetDatabaseOID() {
		t.Fatal("original binding target key/catalog changed after the wrong-target candidate")
	}
	if identity := wrongRun.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("wrong-target factory started a writer child: %+v", identity)
	}
	if identity := base.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("base factory started a writer child: %+v", identity)
	}
	t.Logf("causal negative: actual wrong catalog DB OID %d != original %d refused at stage=%s", wrongDBOID, originalDBOID, stage)
}

// TestBorrowedIdentityPrefixRestartIncarnationRefused restarts the owned
// disposable container and records the ACTUAL predicate that refuses the
// prefix after the genuine control owner loss (truthful, whether the
// independent SQL check or the anchor/OS stage fails first). The original
// prefix permanently invalidates for copies.
func TestBorrowedIdentityPrefixRestartIncarnationRefused(t *testing.T) {
	ctx := context.Background()
	base := newBorrowedIdentityFixture(t)
	prefix := base.Capture(t)
	capturedFacts := prefix.anchor.Diagnostics()
	originalStart := base.fixture.postmasterStr
	originalPID := base.fixture.postmasterPID
	originalSystem := capturedFacts.SystemIdentifier

	// Capture the direct-route bootstrap DSN BEFORE the restart: the host port
	// mapping is not guaranteed usable afterwards, while the container-IP
	// direct route is.
	directBootstrap := borrowedIdentityRoute(t, ctx, base.fixture, databaseOf(t, base.fixture.dsn))

	// Restart the OWNED disposable container: the original control owner is
	// lost and the postmaster incarnation changes.
	stopTimeout := 30 * time.Second
	if err := base.fixture.container.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop owned fixture container: %v", err)
	}
	if err := base.fixture.container.Start(ctx); err != nil {
		t.Fatalf("start owned fixture container: %v", err)
	}
	var newPID int
	var newStart string
	deadline := time.Now().Add(60 * time.Second)
	for {
		pid, start, err := inspectPostmaster(ctx, base.fixture.containerID)
		if err == nil && pid > 0 && start != "" {
			newPID, newStart = pid, start
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted postmaster never became inspectable: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if newStart == originalStart && newPID == originalPID {
		t.Fatal("owned restart did not change the postmaster incarnation")
	}
	// Independent SQL incarnation proof: same data directory (same cluster
	// system_identifier) but a later pg_postmaster_start_time.
	readyDSN := directBootstrap
	if ip, ipErr := base.fixture.container.ContainerIP(ctx); ipErr == nil {
		if parsed, parseErr := url.Parse(directBootstrap); parseErr == nil {
			_, port, splitErr := net.SplitHostPort(parsed.Host)
			if splitErr == nil {
				parsed.Host = net.JoinHostPort(ip, port)
				readyDSN = parsed.String()
			}
		}
	}
	if err := awaitNativePGReady(ctx, readyDSN); err != nil {
		t.Fatalf("restarted fixture never became ready on the direct route: %v", err)
	}
	conn, err := pgx.Connect(ctx, readyDSN)
	if err != nil {
		t.Fatalf("connect restarted fixture for SQL identity: %v", err)
	}
	defer conn.Close(ctx)
	var sqlStart time.Time
	var sqlSystem string
	if err := conn.QueryRow(ctx, `SELECT pg_postmaster_start_time(), system_identifier::text FROM pg_control_system()`).Scan(&sqlStart, &sqlSystem); err != nil {
		t.Fatalf("restarted SQL incarnation read: %v", err)
	}
	if !sqlStart.After(capturedFacts.PostmasterStart) {
		t.Fatalf("SQL postmaster start did not advance: captured=%s restarted=%s", capturedFacts.PostmasterStart, sqlStart)
	}
	if originalSystem != "" && sqlSystem != originalSystem {
		t.Fatalf("restarted cluster system_identifier changed unexpectedly: %q vs %q", originalSystem, sqlSystem)
	}

	// Record the ACTUAL prefix predicate that refuses after the owner loss; no
	// assumption that the anchor health stage is first.
	recheckErr := prefix.Recheck(t, ctx)
	stage := borrowedIdentityRequireStage(t, recheckErr,
		reasonSQLConnect, reasonSQLBackend, reasonSQLNamespace, reasonSQLIncarnation, reasonSQLTargetCatalog,
		reasonOSPostmaster, reasonOSBackend, reasonOSServerInode, reasonAnchorRecheck,
		reasonEndSQLBackend, reasonEndSQLNamespace, reasonEndSQLIncarnation, reasonEndSQLTargetCatalog,
		reasonEndOSPostmaster, reasonEndOSBackendStart, reasonEndOSServerInode)
	copied := *prefix
	if invalid, reason := copied.Invalid(); !invalid || reason == "" {
		t.Fatalf("restart invalidation is not permanently shared: invalid=%t reason=%q", invalid, reason)
	}
	if err := prefix.Recheck(t, ctx); err == nil {
		t.Fatal("restarted fixture rechecked the original prefix successfully")
	}
	// Supporting genuine control health loss (no longer the assumed first
	// predicate).
	if err := base.run.Health(ctx); err == nil {
		t.Fatal("restarted fixture still reports the original control owner healthy")
	}
	if identity := base.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("restart group started a writer child: %+v", identity)
	}
	t.Logf("causal incarnation: postmaster OS start %s -> %s (pid %d -> %d); SQL postmaster start %s -> %s; system_identifier unchanged=%t; actual prefix refusal stage=%s; shared permanent invalid",
		originalStart, newStart, originalPID, newPID, capturedFacts.PostmasterStart, sqlStart, originalSystem == sqlSystem, stage)
}
