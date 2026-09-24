// publisher.go owns the outbox publisher runtime (T033): the claim/lease
// protocol, publishing strictly outside any PostgreSQL transaction, the
// ack / release / block settlement, the bounded retry backoff and the
// franz-go producer sink.
//
// Guarantee statement (global, MUST NOT be weakened): delivery is
// at-least-once and processing is idempotent. PostgreSQL and Kafka have no
// shared transaction, so a crash between the broker acknowledgement and the
// published mark simply re-publishes the same event identity, which the
// consumer's persistent idempotency absorbs. This runtime never claims a
// stronger delivery guarantee (FR-08/FR-13; contracts/outbox-publisher.md §0).
package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// OutboxRecord is one claimed outbox row materialized for publication. Payload
// carries the stored JSONB text verbatim: every retry of the same row
// publishes byte-identical payload bytes, so no re-serialization drift can
// occur (data-model §3.5). AttemptCount is the post-increment attempt number
// from the claim mark.
type OutboxRecord struct {
	OutboxID         int64
	EventID          uuid.UUID
	EventType        string
	SchemaVersion    int
	IdentityKind     IdentityKind
	AggregateType    string
	AggregateID      string
	AggregateVersion int64
	Payload          json.RawMessage
	OccurredAt       time.Time
	ChainID          *int64
	BlockNumber      *int64
	BlockHash        *string
	TxHash           *string
	LogIndex         *int
	RecoveryVersion  *int64
	RevisesEventID   *uuid.UUID
	AttemptCount     int
}

// PartitionKey returns the documented partition key
// `aggregate_type:aggregate_id` (contracts/outbox-publisher.md §2): events of
// one business object land on the same partition as a best-effort ordering
// optimization. Ordering correctness stays in the consumer version guard.
func (r OutboxRecord) PartitionKey() string {
	return r.AggregateType + ":" + r.AggregateID
}

// transportEnvelope is the JSON object published to Kafka (contracts/events.md
// §1). Optional chain/revision fields are omitted when absent.
type transportEnvelope struct {
	EventID          string          `json:"event_id"`
	EventType        string          `json:"event_type"`
	SchemaVersion    int             `json:"schema_version"`
	IdentityKind     string          `json:"identity_kind"`
	AggregateType    string          `json:"aggregate_type"`
	AggregateID      string          `json:"aggregate_id"`
	AggregateVersion int64           `json:"aggregate_version"`
	OccurredAt       time.Time       `json:"occurred_at"`
	ChainID          *int64          `json:"chain_id,omitempty"`
	BlockNumber      *int64          `json:"block_number,omitempty"`
	BlockHash        *string         `json:"block_hash,omitempty"`
	TxHash           *string         `json:"tx_hash,omitempty"`
	LogIndex         *int            `json:"log_index,omitempty"`
	RecoveryVersion  *int64          `json:"recovery_version,omitempty"`
	RevisesEventID   *string         `json:"revises_event_id,omitempty"`
	Payload          json.RawMessage `json:"payload"`
}

// EnvelopeJSON renders the transport envelope of one record. The payload is
// embedded verbatim from the stored JSONB text and the envelope field order is
// fixed, so retries and duplicate publishes carry identical bytes
// (contracts/events.md §1; data-model §3.5). Invalid stored payload bytes are
// a contract failure (permanent: block + alert, never retried forever).
func (r OutboxRecord) EnvelopeJSON() ([]byte, error) {
	if !json.Valid(r.Payload) {
		return nil, contractErrorf("outbox %d payload is not valid JSON", r.OutboxID)
	}
	env := transportEnvelope{
		EventID:          r.EventID.String(),
		EventType:        r.EventType,
		SchemaVersion:    r.SchemaVersion,
		IdentityKind:     string(r.IdentityKind),
		AggregateType:    r.AggregateType,
		AggregateID:      r.AggregateID,
		AggregateVersion: r.AggregateVersion,
		OccurredAt:       r.OccurredAt.UTC(),
		ChainID:          r.ChainID,
		BlockNumber:      r.BlockNumber,
		BlockHash:        r.BlockHash,
		TxHash:           r.TxHash,
		LogIndex:         r.LogIndex,
		RecoveryVersion:  r.RecoveryVersion,
		Payload:          r.Payload,
	}
	if r.RevisesEventID != nil {
		revises := r.RevisesEventID.String()
		env.RevisesEventID = &revises
	}
	return json.Marshal(env)
}

// PublishResult is the per-record outcome of one sink publish: Acked lists
// the broker-confirmed outbox ids; Failures maps an outbox id to its
// classified delivery error.
type PublishResult struct {
	Acked    []int64
	Failures map[int64]error
}

// PublishSink delivers one claimed batch to the broker and reports the
// per-record outcome. It MUST be called outside any PostgreSQL transaction
// (contracts/outbox-publisher.md §0). The production sink is *KafkaSink;
// tests inject deterministic sinks.
type PublishSink interface {
	Publish(ctx context.Context, records []OutboxRecord) PublishResult
}

// PublishObserver observes publisher outcomes (verification.md §1). A nil
// observer disables observation; *metrics.Metrics satisfies it.
type PublishObserver interface {
	ObserveOutboxPublishFailure(errorClass string)
	ObserveOutboxPublished(n int)
	ObserveOutboxAttempts(n int)
	SetOutboxBlocked(n int)
	SetOutboxPending(eventFamily string, count int)
	SetOutboxPendingOldestAge(eventFamily string, seconds float64)
}

// PublisherOptions carries the validated runtime bounds of a Publisher.
type PublisherOptions struct {
	Batch       int
	LeaseTTL    time.Duration
	BackoffBase time.Duration
	BackoffMax  time.Duration
	// Jitter returns a value in [-1, 1) for the ±20% retry jitter; nil uses
	// math/rand (tests inject deterministic jitter).
	Jitter   func() float64
	Observer PublishObserver
}

// Publisher drives the claim -> publish -> settle protocol. All durable state
// lives in PostgreSQL; the struct holds configuration and the sink only.
// Multiple instances shard naturally through FOR UPDATE SKIP LOCKED plus the
// bounded lease; no Redis lock participates (D2).
type Publisher struct {
	Pool        *pgxpool.Pool
	Sink        PublishSink
	Owner       string
	Batch       int
	LeaseTTL    time.Duration
	BackoffBase time.Duration
	BackoffMax  time.Duration
	Jitter      func() float64
	Observer    PublishObserver
}

// NewPublisher validates the runtime bounds fail-closed (missing bounds would
// mean unbounded behavior) and builds the publisher.
func NewPublisher(pool *pgxpool.Pool, sink PublishSink, owner string, opts PublisherOptions) (*Publisher, error) {
	if pool == nil {
		return nil, contractErrorf("publisher requires a database pool")
	}
	if sink == nil {
		return nil, contractErrorf("publisher requires a publish sink")
	}
	if strings.TrimSpace(owner) == "" {
		return nil, contractErrorf("publisher requires an instance owner id")
	}
	if opts.Batch <= 0 {
		return nil, contractErrorf("publisher batch must be positive")
	}
	if opts.LeaseTTL <= 0 {
		return nil, contractErrorf("publisher lease ttl must be positive")
	}
	if opts.BackoffBase <= 0 {
		return nil, contractErrorf("publisher backoff base must be positive")
	}
	if opts.BackoffMax < opts.BackoffBase {
		return nil, contractErrorf("publisher backoff max must be >= base")
	}
	return &Publisher{
		Pool:        pool,
		Sink:        sink,
		Owner:       owner,
		Batch:       opts.Batch,
		LeaseTTL:    opts.LeaseTTL,
		BackoffBase: opts.BackoffBase,
		BackoffMax:  opts.BackoffMax,
		Jitter:      opts.Jitter,
		Observer:    opts.Observer,
	}, nil
}

// PublishOutcome reports one publish cycle.
type PublishOutcome struct {
	Claimed  int
	Acked    int
	Released int
	Blocked  int
}

// ClaimBatch claims up to Batch pending rows in one transaction (T2): the
// locked SELECT, the owner/lease/attempt mark and the COMMIT happen together,
// and the network publish runs strictly after the commit — no network call is
// made while holding a row lock (contracts/outbox-publisher.md §1.1).
func (p *Publisher) ClaimBatch(ctx context.Context) ([]OutboxRecord, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin claim transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	rows, err := tx.Query(ctx, ClaimPendingSQL, p.Batch)
	if err != nil {
		return nil, fmt.Errorf("claim query: %w", err)
	}
	var records []OutboxRecord
	var ids []int64
	for rows.Next() {
		rec, err := scanOutboxRecord(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		records = append(records, rec)
		ids = append(ids, rec.OutboxID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim rows: %w", err)
	}
	if len(records) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit empty claim: %w", err)
		}
		return nil, nil
	}

	marked, err := tx.Query(ctx, ClaimMarkSQL, ids, p.Owner, p.LeaseTTL.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim mark: %w", err)
	}
	attempts := make(map[int64]int, len(records))
	for marked.Next() {
		var id int64
		var attempt int
		if err := marked.Scan(&id, &attempt); err != nil {
			marked.Close()
			return nil, fmt.Errorf("scan claim mark: %w", err)
		}
		attempts[id] = attempt
	}
	marked.Close()
	if err := marked.Err(); err != nil {
		return nil, fmt.Errorf("claim mark rows: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}

	out := make([]OutboxRecord, 0, len(records))
	for _, rec := range records {
		attempt, ok := attempts[rec.OutboxID]
		if !ok {
			// The row left pending between the lock and the mark: impossible
			// while this transaction holds the row lock. Treat as unclaimed
			// rather than publishing an unstamped row.
			continue
		}
		rec.AttemptCount = attempt
		out = append(out, rec)
	}
	return out, nil
}

// scanOutboxRecord materializes one claimed row. Nullable chain/revision
// columns scan into nil pointers (pgx pointer-to-pointer plan).
func scanOutboxRecord(rows pgx.Rows) (OutboxRecord, error) {
	var rec OutboxRecord
	var identityKind string
	if err := rows.Scan(
		&rec.OutboxID, &rec.EventID, &rec.EventType, &rec.SchemaVersion, &identityKind,
		&rec.AggregateType, &rec.AggregateID, &rec.AggregateVersion,
		&rec.Payload, &rec.OccurredAt,
		&rec.ChainID, &rec.BlockNumber, &rec.BlockHash, &rec.TxHash, &rec.LogIndex,
		&rec.RecoveryVersion, &rec.RevisesEventID,
	); err != nil {
		return OutboxRecord{}, fmt.Errorf("scan claimed row: %w", err)
	}
	rec.IdentityKind = IdentityKind(identityKind)
	return rec, nil
}

// PublishOnce runs one claim -> publish -> settle cycle:
//
//  1. claim a bounded batch (single transaction, committed before any network
//     call);
//  2. publish strictly outside the transaction through the sink;
//  3. settle every record in separate transactions: broker-acknowledged rows
//     become published under the owner guard; transient failures are released
//     with the bounded exponential backoff; permanent/contract failures are
//     blocked and alerted, never dropped.
//
// A cancelled ctx still settles what was claimed (stop claiming, finish the
// in-flight work, release what was not confirmed): the settle transactions run
// on a non-cancelled context.
func (p *Publisher) PublishOnce(ctx context.Context) (PublishOutcome, error) {
	records, err := p.ClaimBatch(ctx)
	if err != nil {
		return PublishOutcome{}, err
	}
	if len(records) == 0 {
		return PublishOutcome{}, nil
	}
	if p.Observer != nil {
		p.Observer.ObserveOutboxAttempts(len(records))
	}
	return p.PublishClaimed(ctx, records)
}

// PublishClaimed delivers an already-claimed batch and settles the results.
// PublishOnce is ClaimBatch + PublishClaimed; tests drive the two phases
// separately to reproduce the T3 crash points (claim committed / published /
// acknowledged).
func (p *Publisher) PublishClaimed(ctx context.Context, records []OutboxRecord) (PublishOutcome, error) {
	outcome := PublishOutcome{Claimed: len(records)}
	if len(records) == 0 {
		return outcome, nil
	}
	settleCtx := context.WithoutCancel(ctx)

	if ctx.Err() != nil {
		// Shutdown between claim and publish: nothing reached the broker;
		// release immediately so a restart can re-publish without waiting out
		// the lease.
		released, err := p.releaseImmediate(settleCtx, records)
		outcome.Released = released
		return outcome, err
	}

	result := p.Sink.Publish(ctx, records)
	settled := make(map[int64]bool, len(records))
	if len(result.Acked) > 0 {
		acked, err := p.markPublished(settleCtx, result.Acked)
		if err != nil {
			return outcome, err
		}
		outcome.Acked = acked
		for _, id := range result.Acked {
			settled[id] = true
		}
	}

	byClass := make(map[FailureClass][]int64)
	for id, publishErr := range result.Failures {
		if settled[id] {
			continue
		}
		settled[id] = true
		class := ClassifyPublishError(publishErr)
		if p.Observer != nil {
			p.Observer.ObserveOutboxPublishFailure(string(class))
		}
		byClass[class] = append(byClass[class], id)
	}
	for class, classIDs := range byClass {
		if class == ClassTransient {
			released, err := p.releaseTransient(settleCtx, records, classIDs)
			outcome.Released += released
			if err != nil {
				return outcome, err
			}
			continue
		}
		blocked, err := p.block(settleCtx, class, classIDs)
		outcome.Blocked += blocked
		if err != nil {
			return outcome, err
		}
	}

	// A sink that never reported a claimed record is a sink contract
	// violation; release those claims immediately instead of leaving them
	// unaccounted until the lease expires.
	var unaccounted []OutboxRecord
	for _, rec := range records {
		if !settled[rec.OutboxID] {
			unaccounted = append(unaccounted, rec)
		}
	}
	if len(unaccounted) > 0 {
		released, err := p.releaseImmediate(settleCtx, unaccounted)
		outcome.Released += released
		if err != nil {
			return outcome, err
		}
	}
	return outcome, nil
}

// markPublished flips broker-acknowledged rows to published in their own
// transaction, guarded by the owner (T3). A row whose claim was taken over by
// another instance updates zero rows: it is re-published by the new owner, so
// nothing is lost and duplicates stay possible.
func (p *Publisher) markPublished(ctx context.Context, ids []int64) (int, error) {
	n, err := p.countGuardedUpdates(ctx, AckPublishedSQL, ids, p.Owner)
	if err != nil {
		return n, fmt.Errorf("mark published: %w", err)
	}
	if p.Observer != nil && n > 0 {
		p.Observer.ObserveOutboxPublished(n)
	}
	return n, nil
}

// releaseTransient returns transient-failure claims to pending with the
// bounded exponential backoff of the attempt that just failed
// (contracts/outbox-publisher.md §4). Records are grouped by their computed
// delay so one UPDATE settles a whole group.
func (p *Publisher) releaseTransient(ctx context.Context, records []OutboxRecord, ids []int64) (int, error) {
	byID := make(map[int64]OutboxRecord, len(records))
	for _, rec := range records {
		byID[rec.OutboxID] = rec
	}
	groups := make(map[time.Duration][]int64)
	for _, id := range ids {
		rec, ok := byID[id]
		if !ok {
			continue
		}
		delay := RetryDelay(rec.AttemptCount, p.BackoffBase, p.BackoffMax, p.jitter())
		groups[delay] = append(groups[delay], id)
	}
	total := 0
	for delay, groupIDs := range groups {
		n, err := p.countGuardedUpdates(ctx, ReleaseClaimSQL, groupIDs, p.Owner, delay.Seconds(), string(ClassTransient))
		if err != nil {
			return total, fmt.Errorf("release claim: %w", err)
		}
		total += n
	}
	return total, nil
}

// releaseImmediate returns claims to pending without a backoff (graceful
// shutdown and unaccounted records).
func (p *Publisher) releaseImmediate(ctx context.Context, records []OutboxRecord) (int, error) {
	ids := make([]int64, 0, len(records))
	for _, rec := range records {
		ids = append(ids, rec.OutboxID)
	}
	n, err := p.countGuardedUpdates(ctx, ReleaseClaimImmediateSQL, ids, p.Owner)
	if err != nil {
		return n, fmt.Errorf("release claim immediate: %w", err)
	}
	return n, nil
}

// block moves permanent/contract-failure claims to blocked with the failure
// class recorded: the row stays visible and auditable and is never dropped.
// Only an audited events-admin unblock (T045) returns it to pending.
func (p *Publisher) block(ctx context.Context, class FailureClass, ids []int64) (int, error) {
	n, err := p.countGuardedUpdates(ctx, BlockClaimSQL, ids, p.Owner, string(class))
	if err != nil {
		return n, fmt.Errorf("block claim: %w", err)
	}
	if n > 0 && p.Observer != nil {
		var blocked int64
		if err := p.Pool.QueryRow(ctx, BlockedCountSQL).Scan(&blocked); err != nil {
			return n, fmt.Errorf("blocked count: %w", err)
		}
		p.Observer.SetOutboxBlocked(int(blocked))
	}
	return n, nil
}

// countGuardedUpdates runs one owner-guarded UPDATE ... RETURNING id and
// counts the rows actually updated: a stale owner or an illegal transition
// updates zero rows.
func (p *Publisher) countGuardedUpdates(ctx context.Context, sql string, args ...any) (int, error) {
	rows, err := p.Pool.Query(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return n, err
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return n, err
	}
	return n, nil
}

// RefreshGauges republishes the pending and blocked backlog observations
// (outbox_pending_count / outbox_pending_oldest_age_seconds /
// outbox_blocked_count; verification.md §1). PostgreSQL-only: Redis never
// participates in capacity observation (data-model §6).
func (p *Publisher) RefreshGauges(ctx context.Context) error {
	if p.Observer == nil {
		return nil
	}
	rows, err := p.Pool.Query(ctx, CapacitySQL)
	if err != nil {
		return fmt.Errorf("capacity query: %w", err)
	}
	for rows.Next() {
		var family string
		var count int64
		var oldestAge float64
		if err := rows.Scan(&family, &count, &oldestAge); err != nil {
			rows.Close()
			return fmt.Errorf("scan capacity row: %w", err)
		}
		p.Observer.SetOutboxPending(family, int(count))
		p.Observer.SetOutboxPendingOldestAge(family, oldestAge)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("capacity rows: %w", err)
	}
	var blocked int64
	if err := p.Pool.QueryRow(ctx, BlockedCountSQL).Scan(&blocked); err != nil {
		return fmt.Errorf("blocked count: %w", err)
	}
	p.Observer.SetOutboxBlocked(int(blocked))
	return nil
}

// jitter returns the configured jitter source (math/rand by default).
func (p *Publisher) jitter() func() float64 {
	if p.Jitter != nil {
		return p.Jitter
	}
	return func() float64 { return rand.Float64()*2 - 1 }
}

// Kafka producer initial values (contracts/outbox-publisher.md §3): bounded,
// to be calibrated after measurement.
const (
	// DefaultKafkaDeliveryTimeout bounds one batch delivery including client
	// retries.
	DefaultKafkaDeliveryTimeout = 30 * time.Second
	// DefaultKafkaRequestTimeout bounds a single produce request.
	DefaultKafkaRequestTimeout = 10 * time.Second
	// maxProducerInflight is the contract ceiling
	// (max.in.flight.requests.per.connection ≤ 5) that keeps a partition's
	// order within one producer session.
	maxProducerInflight = 5
	// DefaultTopicPartitions is the documented initial partition count
	// (initial value, to be calibrated after measurement; research R5).
	DefaultTopicPartitions = int32(6)
	// defaultMaxBufferedRecords bounds the client-side record buffer.
	defaultMaxBufferedRecords = 4096
	// ensureTopicWindow bounds the explicit topic-creation retry window.
	ensureTopicWindow = 60 * time.Second
)

// KafkaSinkConfig bounds the franz-go producer.
type KafkaSinkConfig struct {
	Brokers         []string
	Topic           string
	DeliveryTimeout time.Duration
	RequestTimeout  time.Duration
	// MaxInflight must stay within 1..5 (the contract's
	// max.in.flight.requests.per.connection ceiling). Under idempotency
	// franz-go pins the effective in-flight bound itself (≤ 5), so this value
	// is a validated contract assertion, not a client option.
	MaxInflight int
	MaxBuffered int
}

// DefaultKafkaSinkConfig returns the documented initial producer bounds.
func DefaultKafkaSinkConfig(brokers []string, topic string) KafkaSinkConfig {
	return KafkaSinkConfig{
		Brokers:         brokers,
		Topic:           topic,
		DeliveryTimeout: DefaultKafkaDeliveryTimeout,
		RequestTimeout:  DefaultKafkaRequestTimeout,
		MaxInflight:     maxProducerInflight,
		MaxBuffered:     defaultMaxBufferedRecords,
	}
}

// KafkaSink is the production PublishSink: a franz-go producer with acks=all,
// an idempotent producer session and bounded in-flight/delivery/request
// limits (contracts/outbox-publisher.md §3). Acknowledgement means the broker
// accepted the record under the topic policy; it never means a consumer
// processed it.
type KafkaSink struct {
	client *kgo.Client
	topic  string
}

// NewKafkaSink validates the bounds fail-closed and builds the producer.
func NewKafkaSink(cfg KafkaSinkConfig) (*KafkaSink, error) {
	if len(cfg.Brokers) == 0 {
		return nil, contractErrorf("kafka sink requires at least one broker")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, contractErrorf("kafka sink requires a topic")
	}
	if cfg.DeliveryTimeout <= 0 || cfg.RequestTimeout <= 0 {
		return nil, contractErrorf("kafka sink timeouts must be positive")
	}
	if cfg.MaxInflight < 1 || cfg.MaxInflight > maxProducerInflight {
		return nil, contractErrorf("kafka max in-flight %d must be within 1..%d", cfg.MaxInflight, maxProducerInflight)
	}
	if cfg.MaxBuffered < 1 {
		return nil, contractErrorf("kafka max buffered records must be positive")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Idempotent production is on by default in franz-go and is never
		// disabled here: under idempotency franz-go itself pins the per-broker
		// produce in-flight ceiling (1 on Kafka v0.11, 5 from v1 on), so the
		// contract's max.in.flight ≤ 5 bound holds by construction.
		// MaxProduceRequestsInflightPerBroker is deliberately not passed: with
		// idempotency enabled the client rejects any explicit value.
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),
		kgo.ProduceRequestTimeout(cfg.RequestTimeout),
		kgo.MaxBufferedRecords(cfg.MaxBuffered),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &KafkaSink{client: client, topic: cfg.Topic}, nil
}

// Close releases the producer client.
func (s *KafkaSink) Close() {
	if s == nil || s.client == nil {
		return
	}
	s.client.Close()
}

// EnsureTopic creates the configured topic explicitly and idempotently:
// auto-creation stays disabled (research R17), so a typo or an uncreated topic
// must surface instead of silently materializing. Fresh KRaft brokers can
// answer NotController/LeaderNotAvailable before leadership settles; those
// retriable responses are retried inside a bounded window.
func (s *KafkaSink) EnsureTopic(ctx context.Context, partitions int32) error {
	if s == nil || s.client == nil {
		return contractErrorf("kafka sink is not connected")
	}
	if partitions <= 0 {
		return contractErrorf("topic partitions must be positive")
	}
	adm := kadm.NewClient(s.client)
	deadline := time.Now().Add(ensureTopicWindow)
	var lastErr error
	for {
		res, err := adm.CreateTopics(ctx, partitions, 1, nil, s.topic)
		if err == nil {
			resp, ok := res[s.topic]
			switch {
			case !ok:
				return contractErrorf("topic %s: broker returned no response", s.topic)
			case resp.Err == nil || errors.Is(resp.Err, kerr.TopicAlreadyExists):
				return nil
			case isRetriableKafkaError(resp.Err):
				lastErr = resp.Err
			default:
				return fmt.Errorf("ensure topic %s: %w", s.topic, resp.Err)
			}
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("ensure topic %s: %w", s.topic, lastErr)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("ensure topic %s: %w", s.topic, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Publish delivers one claimed batch and returns the per-record outcome. The
// value is the envelope JSON, the key is the documented partition key and the
// headers carry event_id/event_type/schema_version for routing and tracing
// (contracts/outbox-publisher.md §2/§3).
func (s *KafkaSink) Publish(ctx context.Context, records []OutboxRecord) PublishResult {
	result := PublishResult{Failures: make(map[int64]error)}
	if len(records) == 0 {
		return result
	}
	krecords := make([]*kgo.Record, 0, len(records))
	ids := make([]int64, 0, len(records))
	for _, rec := range records {
		value, err := rec.EnvelopeJSON()
		if err != nil {
			result.Failures[rec.OutboxID] = Permanent(err)
			continue
		}
		krecords = append(krecords, &kgo.Record{
			Topic: s.topic,
			Key:   []byte(rec.PartitionKey()),
			Value: value,
			Headers: []kgo.RecordHeader{
				{Key: "event_id", Value: []byte(rec.EventID.String())},
				{Key: "event_type", Value: []byte(rec.EventType)},
				{Key: "schema_version", Value: []byte(strconv.Itoa(rec.SchemaVersion))},
			},
		})
		ids = append(ids, rec.OutboxID)
	}
	if len(krecords) == 0 {
		return result
	}
	produced := s.client.ProduceSync(ctx, krecords...)
	for i, res := range produced {
		id := ids[i]
		if res.Err != nil {
			result.Failures[id] = classifyKafkaProduceError(res.Err)
			continue
		}
		result.Acked = append(result.Acked, id)
	}
	return result
}

// classifyKafkaProduceError maps a final franz-go produce error to the closed
// publish taxonomy (T013; contracts/outbox-publisher.md §4). Kafka's own
// retriable flag decides transient vs permanent, so unknown non-retriable
// errors block instead of retrying forever (fail-closed). Context
// cancellation/deadline and every client-level produce failure (record
// timeout, retries exhausted, buffer backpressure, abort, client closed) are
// transient: the record was not confirmed, so it stays pending and is
// retried.
func classifyKafkaProduceError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, kgo.ErrRecordTimeout) || errors.Is(err, kgo.ErrRecordRetries) ||
		errors.Is(err, kgo.ErrMaxBuffered) || errors.Is(err, kgo.ErrAborting) ||
		errors.Is(err, kgo.ErrClientClosed) {
		return Transient(err)
	}
	var kerrErr *kerr.Error
	if errors.As(err, &kerrErr) && kerrErr.Retriable {
		return Transient(err)
	}
	return Permanent(err)
}

// isRetriableKafkaError reports whether Kafka marks the error retriable
// (leadership/controller settling responses of a fresh broker).
func isRetriableKafkaError(err error) bool {
	var kerrErr *kerr.Error
	if errors.As(err, &kerrErr) {
		return kerrErr.Retriable
	}
	return false
}
