//go:build linux && drill

// borrowed-identity-prefix-role_linux_test.go is the single wrong-ROLE
// negative. It drives the ACTUAL helper capture and Recheck paths:
//
//  1. the helper's error-returning candidate capture binds an authentic
//     factory run bound to a different genuine writer role to the ORIGINAL
//     declared fixture identity and must refuse with the typed
//     sql-target-catalog predicate (real catalog role OID mismatch); and
//  2. the actual prefix Recheck after the writer role is genuinely recreated
//     under the same name (real catalog role OID change) must refuse with the
//     same typed predicate and permanently invalidate.
//
// No stage walker, no caller-supplied fact and no self-cert authority is used.
package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

type borrowedWrongRoleRun struct {
	run       *recovery.DrillBorrowedWriterRun
	binding   recovery.DrillBorrowedControlBinding
	pool      *pgxpool.Pool
	role      string
	password  string
	roleOID   uint32
	targetDSN string
	observer  string
	operation string
}

// newBorrowedWrongRoleRun realizes an authentic factory run bound to a
// different genuine writer role in the SAME real fixture database, reached
// through the fixture mapped route so the original direct-route borrowed lock
// is not contended.
func newBorrowedWrongRoleRun(t *testing.T, ctx context.Context, base *borrowedIdentityFixture) *borrowedWrongRoleRun {
	t.Helper()
	nano := time.Now().UnixNano()
	role := fmt.Sprintf("borrowed_wrong_role_%d", nano)
	password := fmt.Sprintf("borrowed_wrong_pw_%d", nano)
	if _, err := base.fixture.admin.Exec(ctx,
		`CREATE ROLE `+pgx.Identifier{role}.Sanitize()+` LOGIN SUPERUSER PASSWORD `+sqlLiteral(password)); err != nil {
		t.Fatalf("create genuine wrong writer role: %v", err)
	}
	t.Cleanup(func() {
		_, _ = base.fixture.admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+pgx.Identifier{role}.Sanitize())
	})
	targetName := databaseOf(t, base.directTargetDSN)
	mapped, err := url.Parse(base.fixture.dsn)
	if err != nil {
		t.Fatalf("parse fixture mapped DSN: %v", err)
	}
	mapped.Path = "/" + targetName
	mapped.User = url.UserPassword(role, password)
	targetDSN := mapped.String()
	observer := targetDSN
	if parsed, err := url.Parse(targetDSN); err == nil {
		query := parsed.Query()
		query.Set("application_name", "borrowed_identity_wrong_role_observer")
		parsed.RawQuery = query.Encode()
		observer = parsed.String()
	}
	target, err := controlstore.ParseDSNTarget(targetDSN)
	if err != nil {
		t.Fatalf("parse wrong-role target identity: %v", err)
	}
	targetKey, err := recovery.CanonicalTargetKey(target)
	if err != nil {
		t.Fatalf("wrong-role target key: %v", err)
	}
	pool, err := pgxpool.New(ctx, base.directControlDSN)
	if err != nil {
		t.Fatalf("open wrong-role control pool: %v", err)
	}
	t.Cleanup(pool.Close)
	store, err := controlstore.NewStore(ctx, pool)
	if err != nil {
		t.Fatalf("wrong-role control store: %v", err)
	}
	lock, err := recovery.AcquireTargetLock(ctx, base.directControlDSN, targetKey, 3*time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire wrong-role lock on the mapped route: %v", err)
	}
	t.Cleanup(func() { _ = lock.Release(context.Background()) })
	tools, err := recovery.DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision wrong-role tools: %v", err)
	}
	endpoint, err := recovery.DrillOpenOriginEndpoint()
	if err != nil {
		t.Fatalf("open wrong-role endpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Listener().Close() })
	operation := fmt.Sprintf("borrowed-identity-wrong-role-%d", nano)
	run, err := recovery.DrillNewBorrowedWriterRun(ctx, recovery.TargetWriterOptions{
		OperationKind: recovery.TargetWriterOperationRestore,
		Store:         store, ControlDSN: base.directControlDSN, TargetDSN: targetDSN,
		ObserverDSN: observer, TrustedTarget: target,
		OperationID: operation,
		Archive:     strings.NewReader("archive"),
		Probe: func(context.Context, recovery.TargetWriterProof) (recovery.TargetWriterProbeResult, error) {
			return recovery.TargetWriterProbeResult{}, fmt.Errorf("wrong-role probe intentionally refuses")
		},
		Acceptance: func(context.Context, pgx.Tx, recovery.TargetWriterProof) (recovery.TargetWriterAcceptance, error) {
			return recovery.TargetWriterAcceptance{}, fmt.Errorf("wrong-role acceptance intentionally refuses")
		},
	}, lock, endpoint, tools)
	if err != nil {
		t.Fatalf("authentic factory refused the different-role run before prefix capture (stage=factory): %v", err)
	}
	var roleOID uint32
	if err := base.fixture.admin.QueryRow(ctx, `SELECT oid FROM pg_roles WHERE rolname=$1`, role).Scan(&roleOID); err != nil {
		t.Fatalf("read wrong-role catalog OID: %v", err)
	}
	return &borrowedWrongRoleRun{
		run: run, binding: run.Binding(), pool: pool, role: role, password: password, roleOID: roleOID,
		targetDSN: targetDSN, observer: observer, operation: operation,
	}
}

func wrongRoleRefusalReason(t *testing.T, err error) borrowedIdentityReason {
	t.Helper()
	var refusal *borrowedIdentityRefusal
	if !errors.As(err, &refusal) {
		t.Fatalf("refusal is not the typed prefix refusal: %v", err)
	}
	return refusal.reason
}

// TestBorrowedIdentityPrefixRejectsWrongWriterRoleOnCapture drives the actual
// candidate capture: the ORIGINAL declared fixture identity paired with the
// different-role run must refuse with the sql-target-catalog predicate.
func TestBorrowedIdentityPrefixRejectsWrongWriterRoleOnCapture(t *testing.T) {
	ctx := context.Background()
	base := newBorrowedIdentityFixture(t)
	basePrefix := base.Capture(t)
	wrong := newBorrowedWrongRoleRun(t, ctx, base)

	var originalRoleOID, originalDBOID uint32
	if err := base.fixture.admin.QueryRow(ctx,
		`SELECT (SELECT oid FROM pg_roles WHERE rolname=$1), (SELECT oid FROM pg_database WHERE datname=$2)`,
		roleOf(t, base.directTargetDSN), databaseOf(t, base.directTargetDSN)).Scan(&originalRoleOID, &originalDBOID); err != nil {
		t.Fatalf("read original catalog OIDs: %v", err)
	}
	if wrong.roleOID == originalRoleOID {
		t.Fatal("wrong writer role is not a genuinely different catalog role")
	}
	if wrong.binding.WriterRoleOID() != wrong.roleOID {
		t.Fatalf("factory binding does not name the real wrong role: binding=%d actual=%d", wrong.binding.WriterRoleOID(), wrong.roleOID)
	}
	if wrong.binding.TargetDatabaseOID() != originalDBOID {
		t.Fatalf("wrong-role run is not the same real fixture database: binding=%d actual=%d", wrong.binding.TargetDatabaseOID(), originalDBOID)
	}

	setup := newBorrowedIdentityCandidateSetup(t, base.fixture, base.directTargetDSN)
	prefix, err := setup.capture(ctx, wrong.run)
	if err == nil || prefix != nil {
		t.Fatalf("actual candidate capture accepted the wrong writer role: prefix=%v err=%v", prefix, err)
	}
	if reason := wrongRoleRefusalReason(t, err); reason != reasonSQLTargetCatalog {
		t.Fatalf("actual capture refusal reason = %q, want %q", reason, reasonSQLTargetCatalog)
	}
	if reason := setup.state.invalidReason(); reason == "" {
		t.Fatal("actual capture refusal did not permanently invalidate the candidate shared state")
	}

	if invalid, reason := basePrefix.Invalid(); invalid {
		t.Fatalf("original good prefix invalidated by the wrong-role capture case: %s", reason)
	}
	if base.run.Binding().OriginalTargetKey() != base.targetKey || base.run.Binding().ControlTargetKey() != base.controlKey {
		t.Fatal("original binding keys changed during the wrong-role case")
	}
	if identity := base.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("original good run started a writer child: %+v", identity)
	}
	if identity := wrong.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("wrong-role run started a writer child: %+v", identity)
	}
	var audits int
	if err := wrong.pool.QueryRow(ctx, `SELECT count(*) FROM recovery_audit WHERE operation_id=$1`, wrong.operation).Scan(&audits); err != nil || audits != 0 {
		t.Fatalf("wrong-role capture case wrote audit rows: count=%d err=%v", audits, err)
	}
	t.Logf("causal negative: actual candidate capture refused wrong writer role OID %d (original %d) in the same fixture database OID %d with typed reason %q; candidate state permanently invalid; original prefix valid; no child started",
		wrong.roleOID, originalRoleOID, originalDBOID, reasonSQLTargetCatalog)
}

// TestBorrowedIdentityPrefixRejectsWrongWriterRoleOnRecheck drives the actual
// Recheck: a consistent wrong-role candidate is captured, the writer role is
// genuinely recreated under the same name (new catalog OID), and the actual
// Recheck must refuse with the typed sql-target-catalog predicate.
func TestBorrowedIdentityPrefixRejectsWrongWriterRoleOnRecheck(t *testing.T) {
	ctx := context.Background()
	base := newBorrowedIdentityFixture(t)
	basePrefix := base.Capture(t)
	wrong := newBorrowedWrongRoleRun(t, ctx, base)

	setup := newBorrowedIdentityCandidateSetup(t, base.fixture, wrong.targetDSN)
	wrongPrefix, err := setup.capture(ctx, wrong.run)
	if err != nil || wrongPrefix == nil {
		t.Fatalf("consistent wrong-role candidate capture refused before the catalog change (stage=capture): %v", err)
	}
	if identity := wrong.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("wrong-role run started a writer child: %+v", identity)
	}

	// Genuine catalog role OID change: drop and recreate the same role name.
	if _, err := base.fixture.admin.Exec(ctx, `DROP ROLE `+pgx.Identifier{wrong.role}.Sanitize()); err != nil {
		t.Fatalf("drop wrong writer role for the catalog change: %v", err)
	}
	if _, err := base.fixture.admin.Exec(ctx,
		`CREATE ROLE `+pgx.Identifier{wrong.role}.Sanitize()+` LOGIN SUPERUSER PASSWORD `+sqlLiteral(wrong.password)); err != nil {
		t.Fatalf("recreate wrong writer role with a new catalog OID: %v", err)
	}
	var recreatedOID uint32
	if err := base.fixture.admin.QueryRow(ctx, `SELECT oid FROM pg_roles WHERE rolname=$1`, wrong.role).Scan(&recreatedOID); err != nil {
		t.Fatalf("read recreated wrong-role OID: %v", err)
	}
	if recreatedOID == wrong.roleOID {
		t.Fatal("recreated writer role did not get a genuinely different catalog OID")
	}

	err = wrongPrefix.Recheck(t, ctx)
	if err == nil {
		t.Fatal("actual prefix Recheck accepted the changed writer role catalog")
	}
	if reason := wrongRoleRefusalReason(t, err); reason != reasonSQLTargetCatalog {
		t.Fatalf("actual Recheck refusal reason = %q, want %q", reason, reasonSQLTargetCatalog)
	}
	if reason := setup.state.invalidReason(); reason == "" {
		t.Fatal("actual Recheck refusal did not permanently invalidate the shared state")
	}
	copied := *wrongPrefix
	if invalid, reason := copied.Invalid(); !invalid || reason == "" {
		t.Fatalf("Recheck invalidation is not shared with prefix copies: invalid=%t reason=%q", invalid, reason)
	}
	if err := wrongPrefix.Recheck(t, ctx); err == nil || wrongRoleRefusalReason(t, err) != reasonPrefixInvalid {
		t.Fatalf("invalidated wrong-role prefix rechecked without the prefix-invalid reason: %v", err)
	}

	if invalid, reason := basePrefix.Invalid(); invalid {
		t.Fatalf("original good prefix invalidated by the wrong-role recheck case: %s", reason)
	}
	if base.run.Binding().OriginalTargetKey() != base.targetKey || base.run.Binding().ControlTargetKey() != base.controlKey {
		t.Fatal("original binding keys changed during the wrong-role recheck case")
	}
	if identity := base.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("original good run started a writer child: %+v", identity)
	}
	var audits int
	if err := wrong.pool.QueryRow(ctx, `SELECT count(*) FROM recovery_audit WHERE operation_id=$1`, wrong.operation).Scan(&audits); err != nil || audits != 0 {
		t.Fatalf("wrong-role recheck case wrote audit rows: count=%d err=%v", audits, err)
	}
	t.Logf("causal negative: actual Recheck refused writer role catalog OID %d -> %d under the same role name with typed reason %q; shared state permanently invalid; original prefix valid; no child started",
		wrong.roleOID, recreatedOID, reasonSQLTargetCatalog)
}
