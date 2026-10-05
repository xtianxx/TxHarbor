//go:build drill

package recovery_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/recovery"
)

// TestT058DrillEventEffectRecovery exercises the event-side recovery boundary
// independently of the broad drill: a real outbox publisher sends to Kafka,
// the reference consumer applies the delivered envelope, and the restored
// recovery point sees the broker lead before any replay is attempted. The
// reference ledger is an observable consumer effect, but is intentionally not
// represented as a real downstream business system.
func TestT058DrillEventEffectRecovery(t *testing.T) {
	requireDrillLocalPGRestore(t)
	testcontainers.SkipIfProviderIsNotHealthy(t)
	kafka := drillStartKafkaOrSkip(t)
	drillKafkaEnsureTopic(t, kafka, drillKafkaTopic, 1)

	env := newDrillEnv(t, true)
	env.seedLiveBusinessState()
	backup := env.backup()
	recoveryPoint := drillManifestWallClock(t, backup)

	// Advance the live data DB with a valid event, then use the production
	// publisher against the real broker.
	event, err := events.NewEvent(events.Event{
		EventType:     events.EventTypeDepositObservationStatusChanged,
		SchemaVersion: events.SchemaVersionV1,
		IdentityKind:  events.IdentityKindBusinessObject,
		AggregateType: "deposit_observation",
		AggregateID:   "drill-event-effect",
		Payload:       map[string]any{"from_state": "pending", "to_state": "confirmed", "reason": "recovery drill"},
		OccurredAt:    time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	tx, err := env.data.Begin(env.ctx)
	if err != nil {
		t.Fatalf("begin event append: %v", err)
	}
	appended, err := events.Append(env.ctx, tx, event)
	if err != nil {
		_ = tx.Rollback(env.ctx)
		t.Fatalf("Append: %v", err)
	}
	if err := tx.Commit(env.ctx); err != nil {
		t.Fatalf("commit event append: %v", err)
	}
	// Match the restored fixture's next_offset=5 with five real prior records;
	// this target event then advances the broker/group frontier to six.
	seedProducer, err := kgo.NewClient(kgo.SeedBrokers(kafka.Brokers()...), kgo.DefaultProduceTopic(drillKafkaTopic))
	if err != nil {
		t.Fatalf("Kafka seed producer: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := seedProducer.ProduceSync(env.ctx, &kgo.Record{Key: []byte(fmt.Sprintf("t058-seed-%d", i)), Value: []byte(`{"seed":true}`)}).FirstErr(); err != nil {
			seedProducer.Close()
			t.Fatalf("produce prior broker record %d: %v", i, err)
		}
	}
	seedProducer.Close()

	sink, err := events.NewKafkaSink(events.KafkaSinkConfig{
		Brokers: kafka.Brokers(), Topic: drillKafkaTopic,
		DeliveryTimeout: 5 * time.Second, RequestTimeout: 2 * time.Second,
		MaxInflight: 5, MaxBuffered: 16,
	})
	if err != nil {
		t.Fatalf("NewKafkaSink: %v", err)
	}
	t.Cleanup(sink.Close)
	publisher, err := events.NewPublisher(env.data, sink, "t058-event-effect", events.PublisherOptions{
		Batch: 10, LeaseTTL: 30 * time.Second, BackoffBase: time.Second,
		BackoffMax: time.Minute, Jitter: func() float64 { return 0 },
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	if outcome, err := publisher.PublishOnce(env.ctx); err != nil || outcome.Acked < 1 {
		t.Fatalf("PublishOnce = %+v, err=%v; want the event acknowledged by Kafka", outcome, err)
	}

	// Run the production Kafka consumer runtime in a real group. It consumes
	// records from the broker, applies the reference-ledger effect together
	// with the inbox, then commits the group offset itself.
	reference, err := events.NewReferenceConsumer(env.data, events.ConsumerOptions{
		GapWait: time.Second, BackoffBase: 10 * time.Millisecond, BackoffMax: 100 * time.Millisecond,
		RetryLimit: 3, ChainID: 1, Jitter: func() float64 { return 0 },
	})
	if err != nil {
		t.Fatalf("NewReferenceConsumer: %v", err)
	}
	if err := reference.EnsureLedgerSchema(env.ctx); err != nil {
		t.Fatalf("EnsureLedgerSchema: %v", err)
	}
	runtime, err := events.NewKafkaConsumer(events.KafkaConsumerConfig{
		Brokers: kafka.Brokers(), Topic: drillKafkaTopic, GroupPrefix: "drill-t058-event-effect",
		ConsumerName: events.RefConsumerName, PollBatch: 16, CommitInterval: 200 * time.Millisecond,
	}, reference.Consumer)
	if err != nil {
		t.Fatalf("NewKafkaConsumer: %v", err)
	}
	consumerCtx, stopConsumer := context.WithCancel(env.ctx)
	consumerDone := make(chan error, 1)
	go func() { consumerDone <- runtime.Run(consumerCtx) }()
	groupID := runtime.GroupID
	brokerOffsets := drillKafkaOffsets{brokers: kafka.Brokers(), groupID: groupID}
	deadline := time.Now().Add(60 * time.Second)
	var committed int64 = -1
	for time.Now().Before(deadline) {
		var effects int64
		if err := env.data.QueryRow(env.ctx,
			`SELECT count(*) FROM `+events.RefLedgerTable+` WHERE consumer_name = $1 AND event_id = $2`,
			events.RefConsumerName, appended.EventID).Scan(&effects); err != nil {
			t.Fatalf("wait for consumer effect: %v", err)
		}
		if effects == 1 {
			var offsetErr error
			committed, offsetErr = brokerOffsets.CommittedOffset(env.ctx, drillKafkaTopic, 0)
			if offsetErr == nil && committed >= 6 {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if committed < 6 {
		stopConsumer()
		<-consumerDone
		t.Fatalf("actual consumer group committed offset = %d, want >= 6 after target event", committed)
	}
	stopConsumer()
	select {
	case err := <-consumerDone:
		if err != nil {
			t.Fatalf("Kafka consumer runtime stopped: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Kafka consumer runtime did not stop")
	}
	assertDrillEventEffect(t, env, env.data, appended.EventID, 1)

	// Publish the exact Kafka envelope again at a new broker offset. A second
	// real runtime/group delivery must use event_id/inbox identity—not Kafka
	// offset—as the dedup key and leave the observable effect at one.
	reader, err := kgo.NewClient(kgo.SeedBrokers(kafka.Brokers()...), kgo.ConsumeTopics(drillKafkaTopic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatalf("duplicate envelope reader: %v", err)
	}
	var duplicateWire []byte
	readDeadline := time.Now().Add(30 * time.Second)
	for len(duplicateWire) == 0 && time.Now().Before(readDeadline) {
		pollCtx, cancel := context.WithTimeout(env.ctx, time.Second)
		fetches := reader.PollFetches(pollCtx)
		cancel()
		fetches.EachRecord(func(record *kgo.Record) {
			var candidate struct {
				EventID string `json:"event_id"`
			}
			if json.Unmarshal(record.Value, &candidate) == nil && candidate.EventID == appended.EventID.String() {
				duplicateWire = append([]byte(nil), record.Value...)
			}
		})
	}
	reader.Close()
	if len(duplicateWire) == 0 {
		t.Fatal("did not find the publisher's envelope to redeliver")
	}
	duplicateProducer, err := kgo.NewClient(kgo.SeedBrokers(kafka.Brokers()...), kgo.DefaultProduceTopic(drillKafkaTopic))
	if err != nil {
		t.Fatalf("duplicate producer: %v", err)
	}
	if err := duplicateProducer.ProduceSync(env.ctx, &kgo.Record{
		Key: []byte("deposit_observation:drill-event-effect"), Value: duplicateWire,
	}).FirstErr(); err != nil {
		duplicateProducer.Close()
		t.Fatalf("republish identical event envelope: %v", err)
	}
	duplicateProducer.Close()
	duplicateRuntime, err := events.NewKafkaConsumer(events.KafkaConsumerConfig{
		Brokers: kafka.Brokers(), Topic: drillKafkaTopic, GroupPrefix: "drill-t058-event-effect",
		ConsumerName: events.RefConsumerName, PollBatch: 16, CommitInterval: 200 * time.Millisecond,
	}, reference.Consumer)
	if err != nil {
		t.Fatalf("NewKafkaConsumer for duplicate: %v", err)
	}
	duplicateCtx, stopDuplicate := context.WithCancel(env.ctx)
	duplicateDone := make(chan error, 1)
	go func() { duplicateDone <- duplicateRuntime.Run(duplicateCtx) }()
	duplicateTargetOffset := drillKafkaEndOffset(t, kafka)
	duplicateDeadline := time.Now().Add(45 * time.Second)
	var duplicateCommitted int64 = -1
	for time.Now().Before(duplicateDeadline) {
		duplicateCommitted, err = brokerOffsets.CommittedOffset(env.ctx, drillKafkaTopic, 0)
		if err == nil && duplicateCommitted >= duplicateTargetOffset {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	stopDuplicate()
	select {
	case err := <-duplicateDone:
		if err != nil {
			t.Fatalf("duplicate consumer runtime stopped: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("duplicate consumer runtime did not stop")
	}
	if duplicateCommitted < duplicateTargetOffset {
		t.Fatalf("duplicate broker position=%d, want >=%d", duplicateCommitted, duplicateTargetOffset)
	}
	assertDrillEventEffect(t, env, env.data, appended.EventID, 1)

	// Restore a true pre-event recovery point. V6 must identify the broker as
	// ahead of restored consumer state; the test deliberately performs no
	// replay/effect in this divergent state.
	verifyDSN := env.createDatabase("event_effect_verify_backup")
	env.verifyBackup(backup.ManifestPath, verifyDSN, env.operation("event-effect-verify-backup"))
	env.seedAuthoritativeTargetCleanBaseline()
	recoveredDSN := env.recoveryTarget()
	recovered := env.openPool(recoveredDSN)
	env.restore(backup.ManifestPath, recoveredDSN, env.operation("event-effect-restore"))
	endBefore := drillKafkaEndOffset(t, kafka)
	env.broker = brokerOffsets
	batch := env.verify(recovered, env.operation("event-effect-verify"))
	var v6Divergent bool
	for _, item := range batch.Items {
		if item.Category == recovery.VerificationV6 && item.Conclusion == recovery.ConclusionDivergent {
			v6Divergent = true
		}
	}
	if !v6Divergent {
		t.Fatalf("V6 did not detect Kafka/restored-progress divergence: %+v", batch.Items)
	}
	assertDrillEventEffect(t, env, recovered, appended.EventID, 0)
	decision := env.admit(env.gate(), recovery.CapabilityEventConsuming)
	if decision.Allowed || decision.RefusalClass != recovery.RefusalIsolationUnproven {
		t.Fatalf("event_consuming decision = %+v; want isolation_unproven (the pending old_writers_stopped evidence preempts gap gating)", decision)
	}
	if end := drillKafkaEndOffset(t, kafka); end != endBefore {
		t.Fatalf("verification changed Kafka end offset %d -> %d", endBefore, end)
	}

	drillWriteEvidence(t, "t058-event-effect", map[string]any{
		"recovery_point": recoveryPoint.Format(time.RFC3339Nano),
		"event_id":       appended.EventID.String(), "broker_end_offset": endBefore,
		"v6_divergent": v6Divergent, "restored_effect_count": 0,
		"gate_refusal_class": string(decision.RefusalClass), "t058_status": "OPEN",
		"v6_gap_gating_proven": false,
		"live_effect_count":    1, "actual_consumer_group": groupID,
		"actual_group_committed_offset": duplicateCommitted, "duplicate_delivery_offset": duplicateTargetOffset - 1,
		"limitation": "V6 detected divergence, but admission was refused for isolation_unproven because old_writers_stopped remains pending; V6 gap gating is NOT proven and T058 remains OPEN. The reference ledger proves consumer inbox/effect idempotency only; evidence-backed capability release/resume and a production downstream business effect were not exercised",
	})
}

func assertDrillEventEffect(t *testing.T, env *drillEnv, pool *pgxpool.Pool, eventID uuid.UUID, want int64) {
	t.Helper()
	reference, err := events.NewReferenceConsumer(pool, events.ConsumerOptions{
		GapWait: time.Second, BackoffBase: 10 * time.Millisecond, BackoffMax: 100 * time.Millisecond,
		RetryLimit: 3, ChainID: 1, Jitter: func() float64 { return 0 },
	})
	if err != nil {
		t.Fatalf("NewReferenceConsumer for effect observation: %v", err)
	}
	if err := reference.EnsureLedgerSchema(env.ctx); err != nil {
		t.Fatalf("EnsureLedgerSchema for effect observation: %v", err)
	}
	var got int64
	if err := pool.QueryRow(env.ctx,
		`SELECT count(*) FROM `+events.RefLedgerTable+` WHERE consumer_name = $1 AND event_id = $2`,
		events.RefConsumerName, eventID).Scan(&got); err != nil {
		t.Fatalf("count observable reference-consumer effects: %v", err)
	}
	if got != want {
		t.Fatalf("observable effects for %s = %d, want %d", eventID, got, want)
	}
	var inbox int64
	if err := pool.QueryRow(env.ctx,
		`SELECT count(*) FROM consumer_inbox WHERE consumer_name = $1 AND event_id = $2`,
		events.RefConsumerName, eventID).Scan(&inbox); err != nil {
		t.Fatalf("count consumer inbox identity: %v", err)
	}
	wantInbox := want
	if inbox != wantInbox {
		t.Fatalf("inbox rows for %s = %d, want %d", eventID, inbox, wantInbox)
	}
}
