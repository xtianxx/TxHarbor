//go:build integration_kafka

package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestKafkaSkeletonStartStopCreateTopic is the T006 skeleton: start a KRaft
// broker, create the canonical topic explicitly (idempotently), produce and
// consume one record, stop the broker (fault injection) and recover on the
// same address with a fresh broker (the module cannot restart the same
// container; the helper documents the replacement semantics). Skips (never
// passes) without Docker.
func TestKafkaSkeletonStartStopCreateTopic(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	k, err := StartKafka(ctx)
	if err != nil {
		t.Fatalf("StartKafka() error = %v", err)
	}
	t.Cleanup(func() { _ = k.Close(context.Background()) })
	addrBefore := k.Brokers()

	if err := k.EnsureTopic(ctx, KafkaTopic, KafkaPartitions); err != nil {
		t.Fatalf("EnsureTopic() error = %v", err)
	}
	// Idempotent: a second explicit create must not fail the run.
	if err := k.EnsureTopic(ctx, KafkaTopic, KafkaPartitions); err != nil {
		t.Fatalf("EnsureTopic() second call error = %v", err)
	}
	detail, err := k.TopicDetail(ctx, KafkaTopic)
	if err != nil {
		t.Fatalf("TopicDetail() error = %v", err)
	}
	if detail.Err != nil {
		t.Fatalf("topic %q detail error = %v", KafkaTopic, detail.Err)
	}
	if got := len(detail.Partitions); got != int(KafkaPartitions) {
		t.Fatalf("topic %q partitions = %d, want %d", KafkaTopic, got, KafkaPartitions)
	}

	// Auto-creation is off: an unknown topic must surface as an error.
	unknown := "txharbor.unknown." + randomSuffix()
	missing, err := k.TopicDetail(ctx, unknown)
	if err != nil {
		t.Fatalf("TopicDetail(unknown) error = %v", err)
	}
	if missing.Err == nil {
		t.Fatalf("unknown topic %q reported healthy; auto-create must be disabled", unknown)
	}

	// Produce and consume one record end to end.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(k.Brokers()...),
		kgo.ConsumeTopics(KafkaTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient() error = %v", err)
	}
	defer cl.Close()
	if res := cl.ProduceSync(ctx, &kgo.Record{Topic: KafkaTopic, Key: []byte("skeleton"), Value: []byte("1")}); res.FirstErr() != nil {
		t.Fatalf("ProduceSync() error = %v", res.FirstErr())
	}
	fetchCtx, fetchCancel := context.WithTimeout(ctx, 30*time.Second)
	defer fetchCancel()
	fetches := cl.PollFetches(fetchCtx)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatalf("PollFetches() errors = %v", errs)
	}
	if n := len(fetches.Records()); n != 1 {
		t.Fatalf("consumed %d records, want 1", n)
	}
	cl.Close()

	// Fault injection: stop the broker; the address must be refused while it
	// is down.
	if err := k.Stop(ctx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	downCtx, downCancel := context.WithTimeout(ctx, 5*time.Second)
	defer downCancel()
	if _, err := k.TopicDetail(downCtx, KafkaTopic); err == nil {
		t.Fatal("TopicDetail() succeeded while the broker was stopped, want failure")
	}

	// Recovery: a fresh broker on the same fixed address; topics are
	// re-ensured explicitly (documented replacement semantics).
	if err := k.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if got := k.Brokers(); len(got) != 1 || got[0] != addrBefore[0] {
		t.Fatalf("Brokers after restart = %v, want %v (stable address)", got, addrBefore)
	}
	if err := k.EnsureTopic(ctx, KafkaTopic, KafkaPartitions); err != nil {
		t.Fatalf("EnsureTopic() after restart error = %v", err)
	}
	detail, err = k.TopicDetail(ctx, KafkaTopic)
	if err != nil || detail.Err != nil || len(detail.Partitions) != int(KafkaPartitions) {
		t.Fatalf("topic after restart: detail=%v err=%v", detail.Err, err)
	}
}
