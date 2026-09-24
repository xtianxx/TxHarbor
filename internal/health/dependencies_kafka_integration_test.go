//go:build integration_kafka

// dependencies_kafka_integration_test.go is the T017 Kafka probe smoke: a
// real broker is reported available, a stopped broker unavailable, and a
// restarted broker available again on the same address. The probe is
// non-authoritative by construction (it only feeds DependencySignals and the
// kafka_available gauge).
package health

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xtianxx/txharbor/internal/testutil"
)

func TestKafkaDependencyProbeAgainstRealBroker(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	kafkaCtr, err := testutil.StartKafka(ctx)
	if err != nil {
		t.Fatalf("StartKafka: %v", err)
	}
	t.Cleanup(func() { _ = kafkaCtr.Close(context.Background()) })

	client, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaCtr.Brokers()...),
		kgo.DialTimeout(3*time.Second),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	t.Cleanup(client.Close)

	signals := NewDependencySignals()
	runner := &DependencyRunner{
		Interval: time.Hour,
		Timeout:  5 * time.Second,
		Signals:  signals,
		Probes: []DependencyProbe{{
			Name:  "kafka",
			Probe: func(ctx context.Context) error { return client.Ping(ctx) },
		}},
	}

	// Up.
	runner.ProbeOnce(ctx)
	if available, known := signals.Availability("kafka"); !known || !available {
		t.Fatalf("kafka signal after start = (%v, %v), want up", available, known)
	}

	// Down: the broker is terminated; Ping must fail within the probe timeout.
	if err := kafkaCtr.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		runner.ProbeOnce(ctx)
		if available, _ := signals.Availability("kafka"); !available {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("kafka probe never reported the stopped broker")
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Up again on the same address (a fresh broker).
	if err := kafkaCtr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	deadline = time.Now().Add(30 * time.Second)
	for {
		runner.ProbeOnce(ctx)
		if available, _ := signals.Availability("kafka"); available {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("kafka probe never recovered after the broker restart")
		}
		time.Sleep(250 * time.Millisecond)
	}
}
