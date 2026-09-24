//go:build integration_redis

// invalidator_integration_test.go is the T068 acceptance (Integration-Redis;
// V-CACHE): the event-driven invalidator running through the real consumer
// runtime deletes exactly the affected cache ranges against a real Redis,
// redelivery is idempotent, TTL remains the fallback bound, and a cleared
// namespace rotates the epoch so pre-rotation values are unreachable (0 stale
// reads). A real PostgreSQL provides the consumer's durable state; the Redis
// container stop/start provides the fault cycle.
package cache

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/testutil"
)

// t068Postgres boots one migrated PostgreSQL container (the consumer's
// durable state: inbox, versions, progress).
func t068Postgres(t *testing.T) string {
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
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 10 * time.Second}, io.Discard); err != nil {
		t.Fatalf("MigrateUp: %v", err)
	}
	return dsn
}

// t068Envelope builds one valid transport envelope for the given event.
func t068Envelope(t *testing.T, eventType string, identity events.IdentityKind, aggregateType, aggregateID string, version int64, payload map[string]any) []byte {
	t.Helper()
	blockHash := "0x" + strings.Repeat("a", 64)
	txHash := "0x" + strings.Repeat("b", 64)
	ev, err := events.NewEvent(events.Event{
		EventType:        eventType,
		SchemaVersion:    events.SchemaVersionV1,
		IdentityKind:     identity,
		AggregateType:    aggregateType,
		AggregateID:      aggregateID,
		AggregateVersion: version,
		Payload:          payload,
		OccurredAt:       time.Now().UTC(),
		ChainID:          31337,
		BlockNumber:      1,
		BlockHash:        blockHash,
		TxHash:           txHash,
		LogIndex:         0,
	})
	if err != nil {
		t.Fatalf("NewEvent(%s): %v", eventType, err)
	}
	chainID, blockNumber, logIndex := int64(31337), int64(1), 0
	eventID := events.NewEventID(events.EVMLogNaturalKey(chainID, blockHash, txHash, logIndex))
	if identity == events.IdentityKindBusinessObject {
		eventID = events.NewEventID(events.BusinessObjectNaturalKey(aggregateType, aggregateID, version))
	}
	rec := events.OutboxRecord{
		OutboxID:         1,
		EventID:          eventID,
		EventType:        ev.EventType,
		SchemaVersion:    ev.SchemaVersion,
		IdentityKind:     ev.IdentityKind,
		AggregateType:    ev.AggregateType,
		AggregateID:      ev.AggregateID,
		AggregateVersion: ev.AggregateVersion,
		Payload:          ev.PayloadBytes(),
		OccurredAt:       ev.OccurredAt,
		ChainID:          &chainID,
		BlockNumber:      &blockNumber,
		BlockHash:        &blockHash,
		TxHash:           &txHash,
		LogIndex:         &logIndex,
	}
	raw, err := rec.EnvelopeJSON()
	if err != nil {
		t.Fatalf("EnvelopeJSON: %v", err)
	}
	return raw
}

func TestInvalidatorVCacheEventDrivenAgainstRealRedis(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	dsn := t068Postgres(t)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	redisCtr, err := testutil.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() { _ = redisCtr.Close(context.Background()) })
	opt, err := redis.ParseURL(redisCtr.Addr())
	if err != nil {
		t.Fatalf("parse redis addr: %v", err)
	}
	raw := redis.NewClient(opt)
	t.Cleanup(func() { _ = raw.Close() })
	store, err := NewRedisStore(raw)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	observer := &recordingObserver{}
	client, err := NewClient(store, Config{
		Epoch:                  "1",
		TTL:                    30 * time.Second,
		Timeout:                2 * time.Second,
		MaxFallbackConcurrency: 4,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client.SetObserver(observer)
	invalidator, err := NewInvalidator(client)
	if err != nil {
		t.Fatalf("NewInvalidator: %v", err)
	}

	// The invalidator rides the real consumer runtime (inbox dedup, version
	// guard, durable progress) as its effect.
	consumer, err := events.NewConsumer(pool, "cache-invalidator", invalidator, events.ConsumerOptions{
		GapWait:     200 * time.Millisecond,
		BackoffBase: 10 * time.Millisecond,
		BackoffMax:  50 * time.Millisecond,
		RetryLimit:  3,
		ChainID:     31337,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	// Prime the cache: one value per family.
	for _, ref := range []struct {
		family Family
		id     string
	}{
		{FamilyDeposit, "obs-1"},
		{FamilyWithdrawal, "w-1"},
		{FamilyExecution, "intent-1"},
	} {
		if _, err := client.GetOrLoad(ctx, ref.family, ref.id, 1, func(context.Context) ([]byte, int64, error) {
			return []byte("primed"), 1, nil
		}); err != nil {
			t.Fatalf("prime %s/%s: %v", ref.family, ref.id, err)
		}
	}

	// 1. A deposit authority event invalidates exactly its key range.
	depositEnvelope := t068Envelope(t, events.EventTypeDepositObservationCreated, events.IdentityKindEVMLog,
		"deposit_observation", "obs-1", 1, map[string]any{"observation_id": "obs-1", "state": "pending"})
	result, err := consumer.Process(ctx, events.Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 0, Value: depositEnvelope})
	if err != nil || result.Outcome != events.OutcomeApplied {
		t.Fatalf("Process(deposit.created) = (%+v, %v), want applied", result, err)
	}
	if _, ok := client.Get(ctx, FamilyDeposit, "obs-1"); ok {
		t.Fatal("deposit.created did not invalidate its key")
	}
	if _, ok := client.Get(ctx, FamilyWithdrawal, "w-1"); !ok {
		t.Fatal("deposit.created invalidated an unrelated family")
	}
	if _, ok := client.Get(ctx, FamilyExecution, "intent-1"); !ok {
		t.Fatal("deposit.created invalidated an unrelated family")
	}

	// 2. Redelivery is absorbed by the inbox and the invalidation stays
	// idempotent.
	result, err = consumer.Process(ctx, events.Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 0, Value: depositEnvelope})
	if err != nil || result.Outcome != events.OutcomeDuplicate {
		t.Fatalf("redelivery = (%+v, %v), want duplicate", result, err)
	}

	// 3. A withdrawal event invalidates the withdrawal family key.
	withdrawalEnvelope := t068Envelope(t, events.EventTypeWithdrawalRequestReceived, events.IdentityKindBusinessObject,
		"withdrawal_request", "w-1", 1, map[string]any{"request_id": "w-1", "caller": "caller-1", "state": "accepted"})
	result, err = consumer.Process(ctx, events.Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 1, Value: withdrawalEnvelope})
	if err != nil || result.Outcome != events.OutcomeApplied {
		t.Fatalf("Process(withdrawal.request.received) = (%+v, %v), want applied", result, err)
	}
	if _, ok := client.Get(ctx, FamilyWithdrawal, "w-1"); ok {
		t.Fatal("withdrawal event did not invalidate its key")
	}

	// 4. TTL is the fallback bound: a cached value carries the configured TTL.
	if pttl, err := raw.PTTL(ctx, client.Key(FamilyExecution, "intent-1")).Result(); err != nil || pttl <= 0 || pttl > 30*time.Second {
		t.Fatalf("PTTL = %v err=%v, want a positive TTL bound <= 30s", pttl, err)
	}

	// 5. Redis restart/clear: the sentinel disappears, EnsureEpoch rotates,
	// and the pre-rotation key is unreachable (0 stale reads) even though it
	// physically survived in Redis.
	if err := redisCtr.Stop(ctx); err != nil {
		t.Fatalf("stop redis: %v", err)
	}
	// While Redis is down, invalidation keeps the consumer moving (the cache
	// is non-authoritative; an outage must never quarantine a valid event).
	executionEnvelope := t068Envelope(t, events.EventTypeWithdrawalExecutionStateChanged, events.IdentityKindBusinessObject,
		"execution_intent", "intent-1", 2, map[string]any{"from_state": "admitted", "to_state": "claimed", "intent_id": "intent-1"})
	result, err = consumer.Process(ctx, events.Message{Topic: "txharbor.events.v1", Partition: 0, Offset: 2, Value: executionEnvelope})
	if err != nil || result.Outcome != events.OutcomeApplied {
		t.Fatalf("Process during the Redis outage = (%+v, %v), want applied (no quarantine for a cache outage)", result, err)
	}

	if err := redisCtr.Start(ctx); err != nil {
		t.Fatalf("start redis: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := raw.Ping(ctx).Err(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("redis never recovered")
		}
		time.Sleep(200 * time.Millisecond)
	}
	oldKey := client.Key(FamilyExecution, "intent-1")
	oldEpoch := client.CurrentEpoch()
	if err := raw.Del(ctx, "txharbor:"+oldEpoch+":epoch").Err(); err != nil {
		t.Fatalf("delete sentinel: %v", err)
	}
	if !client.EnsureEpoch(ctx) {
		t.Fatal("EnsureEpoch = false after the sentinel disappeared")
	}
	if client.CurrentEpoch() == oldEpoch {
		t.Fatal("epoch did not rotate after the namespace was cleared")
	}
	if n, err := raw.Exists(ctx, oldKey).Result(); err != nil || n != 1 {
		t.Fatalf("old key present=%d err=%v, want the physical key to remain (unreachable, not deleted)", n, err)
	}
	if _, ok := client.Get(ctx, FamilyExecution, "intent-1"); ok {
		t.Fatal("pre-rotation value reachable after the epoch rotation")
	}
	if _, _, _, rotations := observer.snapshot(); rotations == 0 {
		t.Fatal("epoch rotation was never observed")
	}
}
