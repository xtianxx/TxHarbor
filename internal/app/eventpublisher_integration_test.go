//go:build integration_kafka

// eventpublisher_integration_test.go is the T034 command-level smoke under the
// real Kafka layer: the actual `event-publisher` command starts against a real
// broker and a migrated PostgreSQL, ensures the topic explicitly, drains the
// outbox and stops cleanly on cancellation. The runtime protocol itself is
// covered by the events package layers (T036/T037).
package app

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/testutil"
)

// appendCommandEvent inserts one committed deposit observation event through
// the real Append path.
func appendCommandEvent(t *testing.T, pool *pgxpool.Pool, n int) {
	t.Helper()
	obsID := fmt.Sprintf("t034-obs-%d", n)
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
		BlockHash:   fmt.Sprintf("0x%064x", 0x3000+n),
		TxHash:      fmt.Sprintf("0x%064x", 0x4000+n),
		LogIndex:    n,
	})
	if err != nil {
		t.Fatalf("build event: %v", err)
	}
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := events.Append(ctx, tx, ev); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

// TestEventPublisherCommandSmoke runs the real command: config -> pool ->
// producer -> explicit topic creation -> drain -> clean stop. All rows
// committed before startup reach the broker and the command exits 0.
func TestEventPublisherCommandSmoke(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	k, err := testutil.StartKafka(ctx)
	if err != nil {
		t.Fatalf("StartKafka: %v", err)
	}
	t.Cleanup(func() { _ = k.Close(context.Background()) })

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
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 10 * time.Second}, io.Discard); err != nil {
		t.Fatalf("MigrateUp: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	appendCommandEvent(t, pool, 1)
	appendCommandEvent(t, pool, 2)

	env := fullEventsEnv()
	env[config.EnvPGDSN] = dsn
	env[config.EnvKafkaBrokers] = strings.Join(k.Brokers(), ",")

	var stdout, stderr bytes.Buffer
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan int, 1)
	go func() {
		done <- EventPublisher(runCtx, nil, Deps{Getenv: fakeEnv(env), Stdout: &stdout, Stderr: &stderr})
	}()

	deadline := time.Now().Add(60 * time.Second)
	for {
		var unpublished int64
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM outbox_events WHERE publish_state <> 'published'`).Scan(&unpublished); err != nil {
			t.Fatalf("count unpublished: %v", err)
		}
		if unpublished == 0 {
			break
		}
		if time.Now().After(deadline) {
			stop()
			t.Fatalf("command did not drain; stdout=%s stderr=%s", stdout.String(), stderr.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
	stop()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit = %d, want 0; stderr=%s", code, stderr.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("command did not stop on cancellation")
	}
	if !strings.Contains(stdout.String(), "stopped") {
		t.Fatalf("stdout = %q, want the clean-stop line", stdout.String())
	}
	// The topic was created explicitly at startup (auto-create stays off).
	detail, err := k.TopicDetail(ctx, testutil.KafkaTopic)
	if err != nil || detail.Err != nil || len(detail.Partitions) != int(testutil.KafkaPartitions) {
		t.Fatalf("topic detail = %+v, err = %v", detail, err)
	}
}
