package testutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaImage is the pinned Kafka image: the testcontainers Kafka module's
// KRaft starter script targets the Confluent local image; 7.9.10 is the
// latest 7.x patch (module requires >= 7.4.0). Verified available and
// start/stop/topic-create tested on 2026-09-24 (T002/T004/T006 evidence).
const KafkaImage = "confluentinc/confluent-local:7.9.10"

// KafkaTopic is the canonical 013 topic (research R5).
const KafkaTopic = "txharbor.events.v1"

// KafkaPartitions is the documented initial partition count (initial value,
// to be calibrated after measurement; research R5).
const KafkaPartitions = int32(6)

// Kafka wraps a testcontainers Kafka (KRaft, single node) instance with
// explicit topic creation and stop/start fault-injection hooks.
//
// Recovery semantics: the testcontainers Kafka module cannot restart the same
// container — its KRaft starter script re-runs `kafka-storage format` with a
// fresh random cluster id on every start, which fails against the existing
// data directory ("Invalid cluster.id ... Expected X, but read Y"; observed
// 2026-09-24, T006 evidence). Kafka is a non-authoritative carrier (plan III:
// loss must not make authoritative state unrecoverable), so Start boots a
// fresh broker on the same fixed host address. Callers MUST re-ensure topics
// after Start; the application ensures the canonical topic at startup
// (research R17).
type Kafka struct {
	container *tckafka.KafkaContainer
	brokers   []string
	port      int
}

// StartKafka boots one KRaft Kafka broker with automatic topic creation
// disabled (tests must create topics explicitly) and returns the helper.
// The container port is bound to a fixed loopback host port so the address is
// stable across the Stop/Start fault cycle. Callers own the lifecycle and
// MUST call Close.
func StartKafka(ctx context.Context) (*Kafka, error) {
	port, err := freeLoopbackPort()
	if err != nil {
		return nil, err
	}
	k := &Kafka{port: port}
	if err := k.startContainer(ctx); err != nil {
		return nil, err
	}
	return k, nil
}

// startContainer boots a fresh broker on the helper's fixed host port.
func (k *Kafka) startContainer(ctx context.Context) error {
	ctr, err := tckafka.Run(ctx, KafkaImage,
		withTestName("kafka"),
		withFixedLoopbackPort("9093/tcp", k.port),
		// Explicit creation only: a typo in a topic name must surface as an
		// error instead of silently materializing a topic.
		testcontainers.WithEnv(map[string]string{"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false"}),
	)
	if err != nil {
		return fmt.Errorf("start kafka container: %w", err)
	}
	brokers, err := ctr.Brokers(ctx)
	if err != nil {
		_ = ctr.Terminate(context.Background())
		return fmt.Errorf("kafka brokers: %w", err)
	}
	k.container = ctr
	k.brokers = brokers
	return nil
}

// Brokers returns a copy of the bootstrap broker list.
func (k *Kafka) Brokers() []string {
	return append([]string(nil), k.brokers...)
}

// EnsureTopic creates topic with partitions and replication factor 1. The
// call is idempotent: an already-existing topic is not an error. Nothing here
// relies on broker-side auto-creation. Transient broker responses that a
// fresh KRaft broker can return before its controller/leadership settles
// (NotController / LeaderNotAvailable / request timeout) are retried for a
// bounded window so a slow CI machine cannot turn a healthy broker into a
// failure.
func (k *Kafka) EnsureTopic(ctx context.Context, topic string, partitions int32) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(k.brokers...))
	if err != nil {
		return fmt.Errorf("kafka admin client: %w", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	deadline := time.Now().Add(ensureTopicWindow)
	for {
		res, err := adm.CreateTopics(ctx, partitions, 1, nil, topic)
		if err == nil {
			resp, ok := res[topic]
			if !ok {
				return fmt.Errorf("create topic %q: broker returned no response", topic)
			}
			if resp.Err == nil || errors.Is(resp.Err, kerr.TopicAlreadyExists) {
				return nil
			}
			if !transientCreateError(resp.Err) {
				return fmt.Errorf("create topic %q: %w", topic, resp.Err)
			}
			err = resp.Err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("create topic %q: %w", topic, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("create topic %q: %w", topic, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// ensureTopicWindow bounds the transient-response retry window of EnsureTopic.
const ensureTopicWindow = 60 * time.Second

// transientCreateError reports broker responses a fresh KRaft broker can
// return before its controller/leadership settles.
func transientCreateError(err error) bool {
	return errors.Is(err, kerr.NotController) ||
		errors.Is(err, kerr.LeaderNotAvailable) ||
		errors.Is(err, kerr.RequestTimedOut) ||
		errors.Is(err, kerr.NetworkException)
}

// TopicDetail returns the metadata for one topic. A missing topic surfaces
// as a per-topic error (the broker's auto-creation is off).
func (k *Kafka) TopicDetail(ctx context.Context, topic string) (kadm.TopicDetail, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(k.brokers...))
	if err != nil {
		return kadm.TopicDetail{}, fmt.Errorf("kafka client: %w", err)
	}
	defer cl.Close()
	details, err := kadm.NewClient(cl).ListTopics(ctx, topic)
	if err != nil {
		return kadm.TopicDetail{}, fmt.Errorf("list topic %q: %w", topic, err)
	}
	return details[topic], nil
}

// Stop removes the broker: fault injection for "Kafka unavailable". The host
// port is released for the next Start.
func (k *Kafka) Stop(ctx context.Context) error {
	if k.container == nil {
		return nil
	}
	ctr := k.container
	k.container = nil
	if err := ctr.Terminate(ctx); err != nil {
		return fmt.Errorf("stop kafka container: %w", err)
	}
	return nil
}

// Start boots a fresh broker on the same fixed host address (see the Kafka
// type comment for why the same container cannot be restarted). Callers MUST
// re-ensure topics after Start.
func (k *Kafka) Start(ctx context.Context) error {
	if k.container != nil {
		return errors.New("kafka container already running")
	}
	return k.startContainer(ctx)
}

// Close terminates the container (no-op after Stop).
func (k *Kafka) Close(ctx context.Context) error {
	if k.container == nil {
		return nil
	}
	return k.Stop(ctx)
}

// Suspend freezes every Java process inside the running broker container
// (SIGSTOP): the broker stops answering clients while its log segments,
// committed offsets and in-memory state are preserved. This is the fault
// injection for an outage that must not lose the broker log — a container
// Stop/Start boots a fresh broker on the same address (see the Kafka type
// comment), which is fine for publisher-only drills but would invalidate a
// consumer's durable resume offset. Resume undoes it (SIGCONT); the broker
// continues exactly where it stopped.
func (k *Kafka) Suspend(ctx context.Context) error { return k.signalBroker(ctx, "STOP") }

// Resume unfreezes the broker processes (SIGCONT) after Suspend.
func (k *Kafka) Resume(ctx context.Context) error { return k.signalBroker(ctx, "CONT") }

// signalBroker sends one signal to every java PID of the container. The Kafka
// image carries no ps/pgrep, so /proc is the portable process source.
func (k *Kafka) signalBroker(ctx context.Context, signal string) error {
	if k.container == nil {
		return errors.New("kafka container not running")
	}
	script := fmt.Sprintf(
		`for p in /proc/[0-9]*; do pid=${p#/proc/}; if [ "$(cat "$p/comm" 2>/dev/null)" = "java" ]; then kill -%s "$pid" || exit 1; fi; done`,
		signal)
	code, reader, err := k.container.Exec(ctx, []string{"bash", "-c", script})
	if err != nil {
		return fmt.Errorf("kafka %s exec: %w", signal, err)
	}
	out, _ := io.ReadAll(reader)
	if code != 0 {
		return fmt.Errorf("kafka %s exited %d: %s", signal, code, strings.TrimSpace(string(out)))
	}
	return nil
}

// withTestName randomizes the container name so concurrent runs never
// collide.
func withTestName(prefix string) testcontainers.CustomizeRequestOption {
	return testcontainers.WithName(fmt.Sprintf("txharbor-test-%s-%s", prefix, randomSuffix()))
}
