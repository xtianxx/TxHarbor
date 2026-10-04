//go:build linux && drill

// borrowed-gate-fixture-safety_linux_test.go proves the BG02 fixture safety
// properties on real PostgreSQL: the whole-catalog pristine check refuses any
// foreign relation or non-default schema (regardless of a requested relation
// being absent), and the INSERT-only guard init refuses every pre-existing
// guard row without changing a single field or writing audit/marker rows.
// No coordinator/native run happens in this file.
package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
)

func newBorrowedGateSafetyFixture(t *testing.T) (*originGateFixture, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	fx := newOriginGateFixture(t)
	controlDB := fmt.Sprintf("borrowed_gate_safety_ctrl_%d", time.Now().UnixNano())
	if err := fx.createOwnedDatabase(ctx, controlDB); err != nil {
		t.Fatalf("create safety control database: %v", err)
	}
	controlDSN := borrowedIdentityRoute(t, ctx, fx, controlDB)
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: controlDSN, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS,
	}, io.Discard); err != nil {
		t.Fatalf("migrate safety control database: %v", err)
	}
	pool, err := pgxpool.New(ctx, controlDSN)
	if err != nil {
		t.Fatalf("open safety control pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return fx, pool
}

func borrowedGateSafetyKey(seed byte) string {
	return "sha256:" + strings.Repeat(string([]byte{seed}), 64)
}

func borrowedGateRowDigest(t *testing.T, pool *pgxpool.Pool, key string) string {
	t.Helper()
	var digest string
	if err := pool.QueryRow(context.Background(),
		`SELECT md5(row_to_json(t)::text) FROM recovery_target_guard t WHERE target_guard_key=$1`, key).Scan(&digest); err != nil {
		t.Fatalf("guard row digest: %v", err)
	}
	return digest
}

func borrowedGateAuditCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM recovery_audit`).Scan(&count); err != nil {
		t.Fatalf("audit row count: %v", err)
	}
	return count
}

func TestBorrowedGateGuardInitRefusesExistingRows(t *testing.T) {
	_, pool := newBorrowedGateSafetyFixture(t)
	ctx := context.Background()

	cases := []struct {
		name       string
		seed       string
		initDigest string
	}{
		{
			name: "unknown active writer launch intent",
			seed: `INSERT INTO recovery_target_guard
				(target_guard_key, disposition, active_writer, launch_intent, attempt_app_name, operation_id, launch_intent_at)
				VALUES ($1, 'unknown', TRUE, TRUE, 'borrowed-gate-existing-a', 'borrowed-gate-existing-op-a', now())`,
		},
		{
			name: "rebuild required",
			seed: `INSERT INTO recovery_target_guard
				(target_guard_key, disposition, operation_id, rebuild_required_at)
				VALUES ($1, 'rebuild_required', 'borrowed-gate-existing-op-b', now())`,
		},
		{
			name: "prior clean",
			seed: `INSERT INTO recovery_target_guard
				(target_guard_key, disposition, operation_id, clean_at, rebuild_evidence)
				VALUES ($1, 'clean', 'borrowed-gate-existing-op-c', now(), '{"prior":"clean"}')`,
		},
	}
	for index, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := borrowedGateSafetyKey(byte('a' + index))
			if _, err := pool.Exec(ctx, tc.seed, key); err != nil {
				t.Fatalf("seed pre-existing guard row: %v", err)
			}
			before := borrowedGateRowDigest(t, pool, key)
			auditsBefore := borrowedGateAuditCount(t, pool)

			err := initBorrowedGateGuard(ctx, pool, key,
				"sha256:"+strings.Repeat("b", 64), "fixture-admin:safety",
				fmt.Sprintf("borrowed-gate-init-%d", index), "sha256:"+strings.Repeat("c", 64))
			if err == nil {
				t.Fatal("INSERT-only guard init accepted a pre-existing guard row")
			}
			if !strings.Contains(err.Error(), "duplicate key") && !strings.Contains(err.Error(), "23505") {
				t.Fatalf("guard init refusal is not the duplicate-key refusal: %v", err)
			}
			if after := borrowedGateRowDigest(t, pool, key); after != before {
				t.Fatalf("guard init changed the pre-existing row: before=%s after=%s", before, after)
			}
			if auditsAfter := borrowedGateAuditCount(t, pool); auditsAfter != auditsBefore {
				t.Fatalf("guard init wrote audit/marker rows: before=%d after=%d", auditsBefore, auditsAfter)
			}
		})
	}

	// A positive insertion on a fresh key still works and records the bound
	// provenance digest without returning any clean/authority value.
	freshKey := borrowedGateSafetyKey('f')
	if err := initBorrowedGateGuard(ctx, pool, freshKey,
		"sha256:"+strings.Repeat("d", 64), "fixture-admin:safety",
		"borrowed-gate-init-fresh", "sha256:"+strings.Repeat("e", 64)); err != nil {
		t.Fatalf("fresh guard init refused: %v", err)
	}
	var evidence string
	if err := pool.QueryRow(ctx, `SELECT rebuild_evidence::text FROM recovery_target_guard WHERE target_guard_key=$1`, freshKey).Scan(&evidence); err != nil {
		t.Fatalf("read fresh guard evidence: %v", err)
	}
	for _, needle := range []string{"bound_target_key", "role_fingerprint", "admin_provenance", "pristine_digest", freshKey} {
		if !strings.Contains(evidence, needle) {
			t.Fatalf("fresh guard evidence lacks %q: %s", needle, evidence)
		}
	}
}

func TestBorrowedGatePristineCatalogWholeTargetRefusals(t *testing.T) {
	fx, _ := newBorrowedGateSafetyFixture(t)
	ctx := context.Background()

	connect := func(name string) *pgx.Conn {
		t.Helper()
		dsn := borrowedIdentityRoute(t, ctx, fx, name)
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatalf("connect safety target %s: %v", name, err)
		}
		t.Cleanup(func() { _ = conn.Close(context.Background()) })
		return conn
	}
	create := func(label string) string {
		t.Helper()
		name := fmt.Sprintf("borrowed_gate_%s_%d", label, time.Now().UnixNano())
		if err := fx.createOwnedDatabase(ctx, name); err != nil {
			t.Fatalf("create safety target %s: %v", name, err)
		}
		return name
	}

	t.Run("positive exact empty catalog passes", func(t *testing.T) {
		name := create("pristine_ok")
		digest, err := verifyBorrowedGatePristineCatalog(ctx, connect(name), name, fx.role)
		if err != nil {
			t.Fatalf("fresh empty catalog refused: %v", err)
		}
		if !strings.HasPrefix(digest, "sha256:") || len(digest) != len("sha256:")+64 {
			t.Fatalf("pristine digest is not bound: %q", digest)
		}
	})
	t.Run("foreign table under a different name refuses", func(t *testing.T) {
		name := create("foreign_table")
		conn := connect(name)
		if _, err := conn.Exec(ctx, `CREATE TABLE public.borrowed_gate_unrelated (id integer)`); err != nil {
			t.Fatalf("seed foreign table: %v", err)
		}
		digest, err := verifyBorrowedGatePristineCatalog(ctx, conn, name, fx.role)
		if err == nil {
			t.Fatalf("foreign table was accepted as pristine: digest=%q", digest)
		}
		var refusal *borrowedGateFixtureRefusal
		if !errors.As(err, &refusal) || !strings.Contains(refusal.reason, "foreign relation") {
			t.Fatalf("refusal is not the safe foreign-relation reason: %v", err)
		}
		if strings.Contains(err.Error(), "postgres://") {
			t.Fatalf("refusal exposed raw DSN material: %v", err)
		}
		// The seeded object is left in place: a refusal is never repaired by a
		// destructive cleanup before passing.
		var exists bool
		if err := conn.QueryRow(ctx, `SELECT to_regclass('public.borrowed_gate_unrelated') IS NOT NULL`).Scan(&exists); err != nil || !exists {
			t.Fatalf("foreign object was modified by the refusal: exists=%t err=%v", exists, err)
		}
	})
	t.Run("foreign view refuses", func(t *testing.T) {
		name := create("foreign_view")
		conn := connect(name)
		if _, err := conn.Exec(ctx, `CREATE VIEW public.borrowed_gate_view AS SELECT 1 AS v`); err != nil {
			t.Fatalf("seed foreign view: %v", err)
		}
		if _, err := verifyBorrowedGatePristineCatalog(ctx, conn, name, fx.role); err == nil {
			t.Fatal("foreign view was accepted as pristine")
		}
	})
	t.Run("foreign sequence in another schema refuses", func(t *testing.T) {
		name := create("foreign_sequence")
		conn := connect(name)
		if _, err := conn.Exec(ctx, `CREATE SCHEMA borrowed_gate_other`); err != nil {
			t.Fatalf("seed foreign schema: %v", err)
		}
		if _, err := conn.Exec(ctx, `CREATE SEQUENCE borrowed_gate_other.seq`); err != nil {
			t.Fatalf("seed foreign sequence: %v", err)
		}
		if _, err := verifyBorrowedGatePristineCatalog(ctx, conn, name, fx.role); err == nil {
			t.Fatal("foreign sequence in another schema was accepted as pristine")
		}
	})
	t.Run("wrong expected owner refuses", func(t *testing.T) {
		name := create("wrong_owner")
		if _, err := verifyBorrowedGatePristineCatalog(ctx, connect(name), name, "not_the_owner"); err == nil {
			t.Fatal("wrong expected owner was accepted")
		}
	})
}
