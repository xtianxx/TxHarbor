//go:build integration

package jointwire

import (
	"context"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/app"
)

// TestWorkerAssemblyWiresRealParticipants proves the production assembly over
// real PostgreSQL and a real Anvil node: every boundary half is non-nil and
// the joint constructor builds the real Driver/Reconciler around them.
func TestWorkerAssemblyWiresRealParticipants(t *testing.T) {
	dsn := startMigratedPG(t)
	anvilURL := startAnvil(t)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	cfg := entryConfig(dsn, anvilURL)
	deps, err := Worker(ctx, cfg, pool)
	if err != nil {
		t.Fatalf("Worker: %v", err)
	}
	if deps.Advancer == nil || deps.Reader == nil || deps.Binding == nil {
		t.Fatalf("assembly left a nil participant: %+v", deps)
	}
	worker, err := app.NewJointWithdrawalWorker(pool, cfg, nil, slog.Default(), deps)
	if err != nil {
		t.Fatalf("NewJointWithdrawalWorker: %v", err)
	}
	if worker.Driver == nil || worker.Reconciler == nil {
		t.Fatalf("joint worker not wired: driver=%v reconciler=%v", worker.Driver, worker.Reconciler)
	}
	if worker.Driver.Advancer == nil || worker.Driver.Binding == nil || worker.Reconciler.Reader == nil {
		t.Fatalf("boundary halves missing: driver=%+v reconciler=%+v", worker.Driver, worker.Reconciler)
	}
}
