// publisher_test.go is the T033 unit layer: the transport envelope, the
// partition key, the closed Kafka error classification, the publisher
// constructor guards, the claim/settle SQL guards and the statement
// discipline (at-least-once delivery plus idempotent processing; no
// cross-system delivery claim). The claim/lease/ack state machine itself is
// exercised against real PostgreSQL in publisher_integration_test.go (T036).
package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// stubSink is a PublishSink with no behavior; constructor/validation tests
// only need a non-nil sink.
type stubSink struct{}

func (stubSink) Publish(context.Context, []OutboxRecord) PublishResult {
	return PublishResult{Failures: map[int64]error{}}
}

func TestOutboxRecordPartitionKey(t *testing.T) {
	rec := OutboxRecord{AggregateType: "withdrawal_intent", AggregateID: "intent-7"}
	if got := rec.PartitionKey(); got != "withdrawal_intent:intent-7" {
		t.Fatalf("PartitionKey() = %q", got)
	}
}

// TestOutboxRecordEnvelopeJSON pins the transport envelope (contracts/events.md
// §1): required fields, optional chain/revision fields, the verbatim stored
// payload and byte determinism across retries.
func TestOutboxRecordEnvelopeJSON(t *testing.T) {
	eventID := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	payload := json.RawMessage(`{"request_id":"req-1","state":"accepted"}`)
	rec := OutboxRecord{
		OutboxID:         7,
		EventID:          eventID,
		EventType:        EventTypeWithdrawalRequestReceived,
		SchemaVersion:    SchemaVersionV1,
		IdentityKind:     IdentityKindBusinessObject,
		AggregateType:    "withdrawal_request",
		AggregateID:      "req-1",
		AggregateVersion: 3,
		Payload:          payload,
		OccurredAt:       time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
		ChainID:          ptrInt64(31337),
	}

	body, err := rec.EnvelopeJSON()
	if err != nil {
		t.Fatalf("EnvelopeJSON() error = %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("envelope is not valid JSON: %v", err)
	}
	if decoded["event_id"] != eventID.String() {
		t.Fatalf("event_id = %v, want %s", decoded["event_id"], eventID)
	}
	if decoded["event_type"] != EventTypeWithdrawalRequestReceived ||
		decoded["schema_version"] != float64(SchemaVersionV1) ||
		decoded["identity_kind"] != string(IdentityKindBusinessObject) ||
		decoded["aggregate_type"] != "withdrawal_request" ||
		decoded["aggregate_id"] != "req-1" ||
		decoded["aggregate_version"] != float64(3) ||
		decoded["chain_id"] != float64(31337) {
		t.Fatalf("envelope fields = %v", decoded)
	}
	if _, present := decoded["tx_hash"]; present {
		t.Fatal("envelope carries tx_hash for a business_object event")
	}
	if _, present := decoded["revises_event_id"]; present {
		t.Fatal("envelope carries revises_event_id for a non-revision event")
	}
	if !strings.Contains(string(body), string(payload)) {
		t.Fatalf("envelope does not embed the stored payload verbatim:\n%s", body)
	}

	// Byte determinism: retries of the same row publish identical bytes.
	again, err := rec.EnvelopeJSON()
	if err != nil {
		t.Fatalf("EnvelopeJSON() second call error = %v", err)
	}
	if string(again) != string(body) {
		t.Fatalf("envelope bytes are not deterministic:\n%s\n%s", body, again)
	}
}

// TestOutboxRecordEnvelopeJSONRejectsInvalidPayload pins the permanent class
// for unreadable stored payload bytes: block + alert, never retry forever.
func TestOutboxRecordEnvelopeJSONRejectsInvalidPayload(t *testing.T) {
	rec := OutboxRecord{
		OutboxID:      9,
		EventID:       uuid.New(),
		EventType:     EventTypeWithdrawalRequestReceived,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindBusinessObject,
		AggregateType: "withdrawal_request",
		AggregateID:   "req-bad",
		Payload:       json.RawMessage(`{not json`),
		OccurredAt:    time.Now().UTC(),
	}
	if _, err := rec.EnvelopeJSON(); !errors.Is(err, ErrContract) {
		t.Fatalf("EnvelopeJSON() error = %v, want ErrContract", err)
	} else if class := ClassifyPublishError(err); class != ClassContract {
		t.Fatalf("invalid payload class = %s, want contract (non-retryable, blocks)", class)
	}
}

// TestClassifyKafkaProduceError pins the closed publish taxonomy mapping of
// final franz-go errors (contracts/outbox-publisher.md §4): Kafka's own
// retriable flag decides, context cancellation/deadline is transient, and
// anything unknown is permanent (fail-closed).
func TestClassifyKafkaProduceError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want FailureClass
	}{
		{"network exception", kerr.NetworkException, ClassTransient},
		{"unknown topic", kerr.UnknownTopicOrPartition, ClassTransient},
		{"leader election", kerr.LeaderNotAvailable, ClassTransient},
		{"not leader", kerr.NotLeaderForPartition, ClassTransient},
		{"request timeout", kerr.RequestTimedOut, ClassTransient},
		{"broker unavailable", kerr.BrokerNotAvailable, ClassTransient},
		{"storage error", kerr.KafkaStorageError, ClassTransient},
		{"context deadline", context.DeadlineExceeded, ClassTransient},
		{"context canceled", context.Canceled, ClassTransient},
		{"record timeout", kgo.ErrRecordTimeout, ClassTransient},
		{"record retries exhausted", kgo.ErrRecordRetries, ClassTransient},
		{"max buffered", kgo.ErrMaxBuffered, ClassTransient},
		{"client closed", kgo.ErrClientClosed, ClassTransient},
		{"record timeout with last error", fmt.Errorf("%w, last err: %w", kgo.ErrRecordTimeout, kerr.NetworkException), ClassTransient},
		{"wrapped network", fmt.Errorf("delivery: %w", kerr.NetworkException), ClassTransient},
		{"invalid topic", kerr.InvalidTopicException, ClassPermanent},
		{"message too large", kerr.MessageTooLarge, ClassPermanent},
		{"topic authorization", kerr.TopicAuthorizationFailed, ClassPermanent},
		{"unknown error", errors.New("boom"), ClassPermanent},
	}
	for _, tc := range cases {
		got := ClassifyPublishError(classifyKafkaProduceError(tc.err))
		if got != tc.want {
			t.Fatalf("%s: class = %s, want %s", tc.name, got, tc.want)
		}
	}
}

// TestPublisherSQLGuards pins the claim/settle query protocol (contracts/
// outbox-publisher.md §1/§4): the lease-expiry takeover condition, the attempt
// counter increment, the owner guards and the never-drop rules.
func TestPublisherSQLGuards(t *testing.T) {
	requireContains := func(name, sql string, needles ...string) {
		t.Helper()
		for _, needle := range needles {
			if !strings.Contains(sql, needle) {
				t.Fatalf("%s missing %q:\n%s", name, needle, sql)
			}
		}
	}
	requireContains("ClaimPendingSQL", ClaimPendingSQL,
		"publish_state = 'pending'", "next_attempt_at <= now()",
		"claim_expires_at <= now()", "FOR UPDATE SKIP LOCKED", "ORDER BY id", "LIMIT $1",
		"payload", "aggregate_type", "aggregate_id", "aggregate_version")
	requireContains("ClaimMarkSQL", ClaimMarkSQL,
		"claim_owner = $2", "claim_expires_at", "attempt_count = attempt_count + 1",
		"publish_state = 'pending'", "RETURNING id, attempt_count")
	requireContains("AckPublishedSQL", AckPublishedSQL,
		"publish_state = 'published'", "claim_owner = $2", "publish_state = 'pending'")
	requireContains("ReleaseClaimSQL", ReleaseClaimSQL,
		"claim_owner = $2", "next_attempt_at = now()", "last_error_class = $4")
	requireContains("ReleaseClaimImmediateSQL", ReleaseClaimImmediateSQL,
		"claim_owner = $2", "next_attempt_at = now()", "publish_state = 'pending'")
	requireContains("BlockClaimSQL", BlockClaimSQL,
		"publish_state = 'blocked'", "last_error_class = $3", "claim_owner = $2")
	requireContains("BlockedCountSQL", BlockedCountSQL, "publish_state = 'blocked'")

	// The settlement queries never touch anything but pending rows: a
	// published or blocked row can never be reopened by the publisher.
	requireContains("ReleaseClaimSQL", ReleaseClaimSQL, "publish_state = 'pending'")
	requireContains("ReleaseClaimImmediateSQL", ReleaseClaimImmediateSQL, "publish_state = 'pending'")
	requireContains("BlockClaimSQL", BlockClaimSQL, "publish_state = 'pending'")
}

// TestNewPublisherValidation pins the fail-closed constructor: a missing
// bound would mean unbounded runtime behavior.
func TestNewPublisherValidation(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	valid := PublisherOptions{Batch: 10, LeaseTTL: time.Second, BackoffBase: time.Millisecond, BackoffMax: time.Second}

	if _, err := NewPublisher(nil, stubSink{}, "owner", valid); err == nil {
		t.Fatal("NewPublisher(nil pool) succeeded")
	}
	if _, err := NewPublisher(pool, nil, "owner", valid); err == nil {
		t.Fatal("NewPublisher(nil sink) succeeded")
	}
	if _, err := NewPublisher(pool, stubSink{}, "", valid); err == nil {
		t.Fatal("NewPublisher(empty owner) succeeded")
	}
	bad := []struct {
		name string
		opts PublisherOptions
	}{
		{"zero batch", PublisherOptions{Batch: 0, LeaseTTL: time.Second, BackoffBase: time.Millisecond, BackoffMax: time.Second}},
		{"zero lease", PublisherOptions{Batch: 1, LeaseTTL: 0, BackoffBase: time.Millisecond, BackoffMax: time.Second}},
		{"zero backoff base", PublisherOptions{Batch: 1, LeaseTTL: time.Second, BackoffBase: 0, BackoffMax: time.Second}},
		{"backoff max below base", PublisherOptions{Batch: 1, LeaseTTL: time.Second, BackoffBase: time.Second, BackoffMax: time.Millisecond}},
	}
	for _, tc := range bad {
		if _, err := NewPublisher(pool, stubSink{}, "owner", tc.opts); err == nil {
			t.Fatalf("NewPublisher(%s) succeeded", tc.name)
		}
	}
	if _, err := NewPublisher(pool, stubSink{}, "owner", valid); err != nil {
		t.Fatalf("NewPublisher(valid) error = %v", err)
	}
}

// TestNewKafkaSinkValidation pins the producer bounds (contracts/
// outbox-publisher.md §3): the in-flight ceiling is 1..5 and timeouts/buffers
// must be positive.
func TestNewKafkaSinkValidation(t *testing.T) {
	valid := KafkaSinkConfig{Brokers: []string{"127.0.0.1:9092"}, Topic: "t", DeliveryTimeout: time.Second, RequestTimeout: time.Second, MaxInflight: 5, MaxBuffered: 16}
	if _, err := NewKafkaSink(valid); err != nil {
		t.Fatalf("NewKafkaSink(valid) error = %v", err)
	}
	bad := []KafkaSinkConfig{
		{Brokers: nil, Topic: "t", DeliveryTimeout: time.Second, RequestTimeout: time.Second, MaxInflight: 5, MaxBuffered: 16},
		{Brokers: []string{"127.0.0.1:9092"}, Topic: " ", DeliveryTimeout: time.Second, RequestTimeout: time.Second, MaxInflight: 5, MaxBuffered: 16},
		{Brokers: []string{"127.0.0.1:9092"}, Topic: "t", DeliveryTimeout: 0, RequestTimeout: time.Second, MaxInflight: 5, MaxBuffered: 16},
		{Brokers: []string{"127.0.0.1:9092"}, Topic: "t", DeliveryTimeout: time.Second, RequestTimeout: time.Second, MaxInflight: 6, MaxBuffered: 16},
		{Brokers: []string{"127.0.0.1:9092"}, Topic: "t", DeliveryTimeout: time.Second, RequestTimeout: time.Second, MaxInflight: 0, MaxBuffered: 16},
		{Brokers: []string{"127.0.0.1:9092"}, Topic: "t", DeliveryTimeout: time.Second, RequestTimeout: time.Second, MaxInflight: 5, MaxBuffered: 0},
	}
	for i, cfg := range bad {
		if _, err := NewKafkaSink(cfg); err == nil {
			t.Fatalf("NewKafkaSink(bad %d) succeeded", i)
		}
	}
}

// forbiddenCrossSystemClaimTokens are statement fragments that would claim a
// cross-system delivery guarantee the design explicitly does not provide
// (FR-08/FR-13; contracts/outbox-publisher.md §0; plan D2). The fragments are
// assembled from pieces so this scanner never carries the full phrase itself.
var forbiddenCrossSystemClaimTokens = []string{
	"跨系统" + "恰好一次",
	"端到端" + "恰好一次",
	"cross-system " + "exactly once",
	"end-to-end " + "exactly once",
	"exactly once " + "across",
}

// TestPublisherStatementDiscipline scans the package's production sources:
// none may claim a cross-system delivery guarantee, and the publisher runtime
// must positively state the accurate guarantee.
func TestPublisherStatementDiscipline(t *testing.T) {
	atLeastOnce := false
	for _, file := range productionSources(t) {
		for _, token := range forbiddenCrossSystemClaimTokens {
			if strings.Contains(file.src, token) {
				t.Errorf("%s claims %q; the guarantee is at-least-once delivery plus idempotent processing",
					file.name, token)
			}
		}
		if strings.Contains(file.src, "at-least-once") {
			atLeastOnce = true
		}
	}
	if !atLeastOnce {
		t.Fatal("no production source states the at-least-once delivery guarantee")
	}
}

func ptrInt64(v int64) *int64 { return &v }
