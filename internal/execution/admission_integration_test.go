//go:build integration

// admission_integration_test.go executes quickstart V1 on the PostgreSQL side
// (T017): exactly one stable intent for a compliant request, zero intent rows
// on every gate failure with refusal evidence, and insert-first convergence
// under sequential/concurrent repeats. The "intent exists before the 008
// binding" ordering assertion is the joint half (010:T046/J1) and is NOT
// claimed here.
package execution

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
)

const (
	execChainID   int64 = 31337
	execAsset           = "0x1111111111111111111111111111111111111111"
	execRecipient       = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	execAmount          = "100"
	execSender          = "0xcccccccccccccccccccccccccccccccccccccccc"
)

func startExecutionPostgres(t *testing.T) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
		postgres.WithDatabase("txharbor"),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	return dsn
}

func executionPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	dsn := startExecutionPostgres(t)
	ctx := context.Background()
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second}, io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return ctx, pool
}

func seedExecCaller(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO caller (caller_id, label) VALUES ($1, 'ops')
		ON CONFLICT (caller_id) DO NOTHING`, callerID); err != nil {
		t.Fatalf("seed caller %d: %v", callerID, err)
	}
}

func seedExecAuthorization(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID string, callerID int64, state string, expires *time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		authorizationID, callerID, execChainID, execAsset, execRecipient, execAmount, state, expires); err != nil {
		t.Fatalf("seed authorization %s: %v", authorizationID, err)
	}
}

func seedExecRequest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID, authorizationID string, callerID int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO withdrawal_requests
		(request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		requestID, callerID, "idem-"+strings.ReplaceAll(requestID, " ", "-"), authorizationID, execChainID, execAsset, execRecipient, execAmount); err != nil {
		t.Fatalf("seed request %s: %v", requestID, err)
	}
}

func seedExecScope(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID, intentID, requestID, sender string, version int64, allowsReplacement bool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO withdrawal_authorization_scopes
		(authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas,
		 fee_max_priority, allows_fee_replacement, authorization_version, attested_by)
		VALUES ($1, $2, $3, $4, 0, 0, 0, $5, $6, 'test')`,
		authorizationID, intentID, requestID, sender, allowsReplacement, version); err != nil {
		t.Fatalf("seed scope %s: %v", authorizationID, err)
	}
}

func seedExecRegistry(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sender, state string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO nonce_wallet_registry
		(chain_id, sender, state, registry_seq) VALUES ($1, $2, $3, 1)
		ON CONFLICT (chain_id, sender) DO UPDATE SET state = EXCLUDED.state`, execChainID, sender, state); err != nil {
		t.Fatalf("seed registry %s: %v", sender, err)
	}
}

func seedExecPermission(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64, canExecute bool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO execution_caller_permission (caller_id, can_execute, updated_by)
		VALUES ($1, $2, 'test') ON CONFLICT (caller_id) DO UPDATE SET can_execute = EXCLUDED.can_execute`,
		callerID, canExecute); err != nil {
		t.Fatalf("seed permission %d: %v", callerID, err)
	}
}

func countIntents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_intents WHERE request_id = $1`, requestID).Scan(&n); err != nil {
		t.Fatalf("count intents: %v", err)
	}
	return n
}

func countRefusalEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM execution_events
		WHERE kind = 'admission_refused' AND detail LIKE '%request_id=' || $1 || '%'`, requestID).Scan(&n); err != nil {
		t.Fatalf("count refusal events: %v", err)
	}
	return n
}

// seedFullAdmission seeds the compliant baseline for one request.
func seedFullAdmission(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID, authorizationID, intentID string) {
	t.Helper()
	seedExecCaller(t, ctx, pool, 1)
	seedExecAuthorization(t, ctx, pool, authorizationID, 1, "active", nil)
	seedExecRequest(t, ctx, pool, requestID, authorizationID, 1)
	seedExecScope(t, ctx, pool, authorizationID, intentID, requestID, execSender, 1, false)
	seedExecRegistry(t, ctx, pool, execSender, "active")
	seedExecPermission(t, ctx, pool, 1, true)
}

func TestAdmissionCreatesExactlyOneIntent(t *testing.T) {
	ctx, pool := executionPool(t)
	seedFullAdmission(t, ctx, pool, "req-v1-a", "authz-v1-a", "intent-v1-a")

	out, err := Admit(ctx, pool, "req-v1-a", 1)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if out.Status != AdmissionCreated {
		t.Fatalf("Admit status = %d, want 201 (%+v)", out.Status, out)
	}
	if out.Intent.IntentID != "intent-v1-a" {
		t.Fatalf("intent_id = %q, want the scope-declared value", out.Intent.IntentID)
	}
	if got := countIntents(t, ctx, pool, "req-v1-a"); got != 1 {
		t.Fatalf("intent rows = %d, want 1", got)
	}

	var state string
	var stateVersion int64
	if err := pool.QueryRow(ctx, `SELECT execution_state, state_version FROM request_status_projection WHERE request_id = 'req-v1-a'`).Scan(&state, &stateVersion); err != nil {
		t.Fatalf("read projection: %v", err)
	}
	if state != IntentAdmitted || stateVersion != 1 {
		t.Fatalf("projection = %s v%d, want admitted v1", state, stateVersion)
	}
	var admitted int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM execution_events WHERE intent_id = 'intent-v1-a' AND kind = 'admitted'`).Scan(&admitted); err != nil || admitted != 1 {
		t.Fatalf("admitted events = %d (err %v), want 1", admitted, err)
	}

	out, err = Admit(ctx, pool, "req-v1-a", 1)
	if err != nil || out.Status != AdmissionRecorded {
		t.Fatalf("sequential replay = %d (err %v), want 200", out.Status, err)
	}
	if got := countIntents(t, ctx, pool, "req-v1-a"); got != 1 {
		t.Fatalf("intent rows after replay = %d, want 1", got)
	}

	seedFullAdmission(t, ctx, pool, "req-v1-b", "authz-v1-b", "intent-v1-b")
	const n = 8
	var wg sync.WaitGroup
	statuses := make([]int, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			o, err := Admit(ctx, pool, "req-v1-b", 1)
			statuses[i], errs[i] = o.Status, err
		}(i)
	}
	wg.Wait()
	created := 0
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("concurrent Admit[%d]: %v", i, errs[i])
		}
		switch statuses[i] {
		case AdmissionCreated:
			created++
		case AdmissionRecorded:
		default:
			t.Fatalf("concurrent Admit[%d] status = %d", i, statuses[i])
		}
	}
	if created != 1 {
		t.Fatalf("concurrent creators = %d, want exactly 1", created)
	}
	if got := countIntents(t, ctx, pool, "req-v1-b"); got != 1 {
		t.Fatalf("concurrent intent rows = %d, want 1", got)
	}
}

func TestAdmissionRefusalMatrix(t *testing.T) {
	ctx, pool := executionPool(t)

	cases := []struct {
		name    string
		prepare func(t *testing.T, requestID, authorizationID, intentID string)
		want    int
	}{
		{
			name: "missing grant",
			prepare: func(t *testing.T, requestID, authorizationID, intentID string) {
				seedExecCaller(t, ctx, pool, 1)
				seedExecRequest(t, ctx, pool, requestID, authorizationID, 1)
				seedExecRegistry(t, ctx, pool, execSender, "active")
			},
			want: AdmissionRefused,
		},
		{
			name: "expired grant",
			prepare: func(t *testing.T, requestID, authorizationID, intentID string) {
				seedExecCaller(t, ctx, pool, 1)
				past := time.Now().Add(-time.Hour)
				seedExecAuthorization(t, ctx, pool, authorizationID, 1, "active", &past)
				seedExecRequest(t, ctx, pool, requestID, authorizationID, 1)
				seedExecScope(t, ctx, pool, authorizationID, intentID, requestID, execSender, 1, false)
				seedExecRegistry(t, ctx, pool, execSender, "active")
			},
			want: AdmissionRefused,
		},
		{
			name: "revoked grant",
			prepare: func(t *testing.T, requestID, authorizationID, intentID string) {
				seedExecCaller(t, ctx, pool, 1)
				seedExecAuthorization(t, ctx, pool, authorizationID, 1, "revoked", nil)
				seedExecRequest(t, ctx, pool, requestID, authorizationID, 1)
				seedExecScope(t, ctx, pool, authorizationID, intentID, requestID, execSender, 1, false)
				seedExecRegistry(t, ctx, pool, execSender, "active")
			},
			want: AdmissionRefused,
		},
		{
			name: "missing scope",
			prepare: func(t *testing.T, requestID, authorizationID, intentID string) {
				seedExecCaller(t, ctx, pool, 1)
				seedExecAuthorization(t, ctx, pool, authorizationID, 1, "active", nil)
				seedExecRequest(t, ctx, pool, requestID, authorizationID, 1)
				seedExecRegistry(t, ctx, pool, execSender, "active")
			},
			want: AdmissionRefused,
		},
		{
			name: "scope request mismatch",
			prepare: func(t *testing.T, requestID, authorizationID, intentID string) {
				seedExecCaller(t, ctx, pool, 1)
				seedExecAuthorization(t, ctx, pool, authorizationID, 1, "active", nil)
				seedExecRequest(t, ctx, pool, requestID, authorizationID, 1)
				seedExecScope(t, ctx, pool, authorizationID, intentID, "other-request", execSender, 1, false)
				seedExecRegistry(t, ctx, pool, execSender, "active")
			},
			want: AdmissionRefused,
		},
		{
			name: "unregistered sender",
			prepare: func(t *testing.T, requestID, authorizationID, intentID string) {
				seedExecCaller(t, ctx, pool, 1)
				seedExecAuthorization(t, ctx, pool, authorizationID, 1, "active", nil)
				seedExecRequest(t, ctx, pool, requestID, authorizationID, 1)
				seedExecScope(t, ctx, pool, authorizationID, intentID, requestID, execSender, 1, false)
				seedExecRegistry(t, ctx, pool, execSender, "disabled")
			},
			want: AdmissionRefused,
		},
		{
			name: "pause row",
			prepare: func(t *testing.T, requestID, authorizationID, intentID string) {
				seedFullAdmission(t, ctx, pool, requestID, authorizationID, intentID)
				if _, err := pool.Exec(ctx, `INSERT INTO indexer_pause (chain_id, height, expected_hash, actual_hash, kind)
					VALUES ($1, 1, repeat('a', 64), repeat('b', 64), 'hash_mismatch')`, execChainID); err != nil {
					t.Fatalf("seed pause: %v", err)
				}
				t.Cleanup(func() {
					_, _ = pool.Exec(context.Background(), `DELETE FROM indexer_pause WHERE chain_id = $1`, execChainID)
				})
			},
			want: AdmissionRefused,
		},
		{
			name: "active recovery",
			prepare: func(t *testing.T, requestID, authorizationID, intentID string) {
				seedFullAdmission(t, ctx, pool, requestID, authorizationID, intentID)
				if _, err := pool.Exec(ctx, `INSERT INTO reorg_policy_history (chain_id, policy_seq, max_depth)
					VALUES ($1, 1, 100)`, execChainID); err != nil {
					t.Fatalf("seed policy: %v", err)
				}
				if _, err := pool.Exec(ctx, `INSERT INTO reorg_recovery
					(chain_id, recovery_id, phase, policy_seq, max_depth, bound_old_number, bound_old_hash, recovery_seq)
					VALUES ($1, 'rec-1', 'detected', 1, 100, 10, '0x' || repeat('c', 64), 7)`, execChainID); err != nil {
					t.Fatalf("seed recovery: %v", err)
				}
				t.Cleanup(func() {
					_, _ = pool.Exec(context.Background(), `DELETE FROM reorg_recovery WHERE chain_id = $1`, execChainID)
					_, _ = pool.Exec(context.Background(), `DELETE FROM reorg_policy_history WHERE chain_id = $1`, execChainID)
				})
			},
			want: AdmissionRefused,
		},
	}

	for i, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			requestID := "req-refuse-" + tc.name
			authorizationID := "authz-refuse-" + tc.name
			intentID := "intent-refuse-0" + string(rune('0'+i))
			tc.prepare(t, requestID, authorizationID, intentID)

			out, err := Admit(ctx, pool, requestID, 1)
			if err != nil {
				t.Fatalf("Admit: %v", err)
			}
			if out.Status != tc.want {
				t.Fatalf("Admit status = %d, want %d (refusal %q, basis %q)", out.Status, tc.want, out.Refusal, out.Basis)
			}
			if got := countIntents(t, ctx, pool, requestID); got != 0 {
				t.Fatalf("intent rows = %d, want 0 on refusal", got)
			}
			if got := countRefusalEvents(t, ctx, pool, requestID); got != 1 {
				t.Fatalf("admission_refused events = %d, want 1", got)
			}
		})
	}
}

func TestAdmissionForeignCallerAndPermission(t *testing.T) {
	ctx, pool := executionPool(t)
	seedFullAdmission(t, ctx, pool, "req-foreign", "authz-foreign", "intent-foreign")

	out, err := Admit(ctx, pool, "req-foreign", 2)
	if err != nil {
		t.Fatalf("Admit foreign: %v", err)
	}
	if out.Status != AdmissionNotFound {
		t.Fatalf("foreign caller status = %d, want 404", out.Status)
	}
	if got := countIntents(t, ctx, pool, "req-foreign"); got != 0 {
		t.Fatalf("intent rows after foreign attempt = %d, want 0", got)
	}

	allowed, err := CanExecute(ctx, pool, 1)
	if err != nil || !allowed {
		t.Fatalf("CanExecute(caller 1) = %v, %v; want true", allowed, err)
	}
	seedExecPermission(t, ctx, pool, 1, false)
	allowed, err = CanExecute(ctx, pool, 1)
	if err != nil || allowed {
		t.Fatalf("CanExecute(caller 1) after revoke = %v, %v; want false", allowed, err)
	}
	allowed, err = CanExecute(ctx, pool, 99)
	if err != nil || allowed {
		t.Fatalf("CanExecute(absent caller) = %v, %v; want false (fail-closed)", allowed, err)
	}
}

func TestAdmissionConflictDifferentBasis(t *testing.T) {
	ctx, pool := executionPool(t)
	seedFullAdmission(t, ctx, pool, "req-conflict", "authz-conflict", "intent-conflict-1")

	out, err := Admit(ctx, pool, "req-conflict", 1)
	if err != nil || out.Status != AdmissionCreated {
		t.Fatalf("first Admit = %d (err %v), want 201", out.Status, err)
	}

	// Re-supply the scope with a different declared intent identity: the
	// request unique now conflicts with a different basis -> 409, zero writes.
	if _, err := pool.Exec(ctx, `UPDATE withdrawal_authorization_scopes
		SET intent_id = 'intent-conflict-2' WHERE authorization_id = 'authz-conflict'`); err != nil {
		t.Fatalf("update scope: %v", err)
	}
	out, err = Admit(ctx, pool, "req-conflict", 1)
	if err != nil {
		t.Fatalf("conflict Admit: %v", err)
	}
	if out.Status != AdmissionConflict {
		t.Fatalf("different-basis status = %d, want 409", out.Status)
	}
	if got := countIntents(t, ctx, pool, "req-conflict"); got != 1 {
		t.Fatalf("intent rows after conflict = %d, want 1 (zero writes)", got)
	}
}

// TestNonAcceptedRequestIsUnrepresentable documents that the 007 schema fixes
// status='accepted' (the "non-accepted request" admission case cannot exist);
// the CHECK is the structural evidence.
func TestNonAcceptedRequestIsUnrepresentable(t *testing.T) {
	ctx, pool := executionPool(t)
	seedExecCaller(t, ctx, pool, 1)
	seedExecAuthorization(t, ctx, pool, "authz-status", 1, "active", nil)
	_, err := pool.Exec(ctx, `INSERT INTO withdrawal_requests
		(request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount, status)
		VALUES ('req-status', 1, 'idem-status', 'authz-status', $1, $2, $3, 100, 'pending')`,
		execChainID, execAsset, execRecipient)
	if err == nil {
		t.Fatal("insert of a non-accepted request succeeded; the accepted-only CHECK is missing")
	}
	if !isConstraint(err, "withdrawal_requests_status_check") {
		t.Fatalf("non-accepted insert error = %v, want withdrawal_requests_status_check", err)
	}
}
