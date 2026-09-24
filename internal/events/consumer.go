// consumer.go owns the idempotent consumer runtime (T041/T044): the T4
// application transaction (persistent inbox dedup, version guard, effect,
// durable progress), the bounded retry/backoff policy, the bounded
// version-gap wait and the quarantine decision, plus the franz-go consumer
// group that feeds the processor.
//
// Guarantee statement (global, MUST NOT be weakened): delivery is
// at-least-once and processing is idempotent; the same event is effectively
// applied exactly once inside this consumer's PostgreSQL effects. PostgreSQL
// and Kafka have no shared transaction: the Kafka offset is committed after
// the effect transaction commits, and a crash in between only causes a
// redelivery that the inbox absorbs. This runtime never claims cross-system
// exactly-once (FR-13; contracts/consumer.md §1).
package events

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ErrIdentityMismatch is the consumer-side chain-identity refusal: the
// envelope carries a chain that does not match the local configuration, or an
// evm_log envelope misses its chain identity shape. Consumers quarantine it
// (failure_class=identity_mismatch) and never apply it (contracts/events.md
// §6).
var ErrIdentityMismatch = errors.New("events: chain identity mismatch")

// ErrVersionGap marks an event whose aggregate_version jumped beyond
// max_version+1: the consumer waits inside the configured window and
// quarantines version_gap when the gap survives it. It is never applied out
// of order and never silently skipped (FR-10; contracts/consumer.md §3).
var ErrVersionGap = errors.New("events: consumer version gap")

// Envelope is the consumer-side materialization of the transport envelope
// (contracts/events.md §1). Unknown JSON fields are ignored: backward
// compatible additions keep the same schema_version (FR-12).
type Envelope struct {
	EventID          uuid.UUID
	EventType        string
	SchemaVersion    int
	IdentityKind     IdentityKind
	AggregateType    string
	AggregateID      string
	AggregateVersion int64
	OccurredAt       time.Time

	ChainID     *int64
	BlockNumber *int64
	BlockHash   *string
	TxHash      *string
	LogIndex    *int

	RecoveryVersion *int64
	RevisesEventID  *uuid.UUID

	Payload map[string]any

	// raw is the exact delivered envelope JSON: the quarantine snapshot and
	// every replay reuse these bytes, so no re-serialization drift can occur
	// (data-model §3.5).
	raw []byte
}

// Raw returns the delivered envelope bytes (nil for hand-built envelopes).
func (e Envelope) Raw() []byte { return append([]byte(nil), e.raw...) }

// envelopeJSON mirrors the publisher transport envelope (publisher.go
// transportEnvelope) and contracts/events.md §1.
type envelopeJSON struct {
	EventID          string          `json:"event_id"`
	EventType        string          `json:"event_type"`
	SchemaVersion    int             `json:"schema_version"`
	IdentityKind     string          `json:"identity_kind"`
	AggregateType    string          `json:"aggregate_type"`
	AggregateID      string          `json:"aggregate_id"`
	AggregateVersion int64           `json:"aggregate_version"`
	OccurredAt       time.Time       `json:"occurred_at"`
	ChainID          *int64          `json:"chain_id"`
	BlockNumber      *int64          `json:"block_number"`
	BlockHash        *string         `json:"block_hash"`
	TxHash           *string         `json:"tx_hash"`
	LogIndex         *int            `json:"log_index"`
	RecoveryVersion  *int64          `json:"recovery_version"`
	RevisesEventID   *string         `json:"revises_event_id"`
	Payload          json.RawMessage `json:"payload"`
}

// ParseEnvelope decodes and validates one delivered envelope against the
// frozen catalog v1: required fields, identity kind, chain identity shape,
// required payload keys and the payload minimization scan. Unknown event
// types and unknown schema versions fail closed with ErrContract /
// ErrSchemaUnsupported so callers quarantine with the right reason (FR-12;
// contracts/events.md §4.5).
func ParseEnvelope(data []byte) (Envelope, error) {
	var wire envelopeJSON
	if err := json.Unmarshal(data, &wire); err != nil {
		return Envelope{}, contractErrorf("envelope is not valid JSON: %v", err)
	}
	spec, err := RouteEvent(wire.EventType, wire.SchemaVersion)
	if err != nil {
		return Envelope{}, err
	}
	eventID, err := uuid.Parse(wire.EventID)
	if err != nil {
		return Envelope{}, contractErrorf("envelope event_id %q is not a UUID: %v", wire.EventID, err)
	}
	kind := IdentityKind(wire.IdentityKind)
	if kind != spec.IdentityKind {
		return Envelope{}, contractErrorf("event_type %s requires identity_kind %s, got %s",
			wire.EventType, spec.IdentityKind, wire.IdentityKind)
	}
	if wire.AggregateType == "" || wire.AggregateID == "" {
		return Envelope{}, contractErrorf("envelope aggregate_type and aggregate_id are required")
	}
	if wire.AggregateVersion <= 0 {
		return Envelope{}, contractErrorf("envelope aggregate_version must be > 0")
	}
	if wire.OccurredAt.IsZero() {
		return Envelope{}, contractErrorf("envelope occurred_at is required")
	}
	env := Envelope{
		EventID:          eventID,
		EventType:        wire.EventType,
		SchemaVersion:    wire.SchemaVersion,
		IdentityKind:     kind,
		AggregateType:    wire.AggregateType,
		AggregateID:      wire.AggregateID,
		AggregateVersion: wire.AggregateVersion,
		OccurredAt:       wire.OccurredAt.UTC(),
		ChainID:          wire.ChainID,
		BlockNumber:      wire.BlockNumber,
		BlockHash:        wire.BlockHash,
		TxHash:           wire.TxHash,
		LogIndex:         wire.LogIndex,
		RecoveryVersion:  wire.RecoveryVersion,
		raw:              append([]byte(nil), data...),
	}
	if kind == IdentityKindEVMLog {
		if wire.ChainID == nil || wire.BlockNumber == nil || wire.BlockHash == nil ||
			wire.TxHash == nil || wire.LogIndex == nil {
			return Envelope{}, contractErrorf("evm_log event %s misses chain identity", wire.EventType)
		}
	}
	if wire.RevisesEventID != nil && *wire.RevisesEventID != "" {
		revises, err := uuid.Parse(*wire.RevisesEventID)
		if err != nil {
			return Envelope{}, contractErrorf("envelope revises_event_id %q is not a UUID: %v", *wire.RevisesEventID, err)
		}
		env.RevisesEventID = &revises
	}
	if len(wire.Payload) == 0 {
		return Envelope{}, contractErrorf("envelope payload is required")
	}
	if err := json.Unmarshal(wire.Payload, &env.Payload); err != nil {
		return Envelope{}, contractErrorf("envelope payload is not a JSON object: %v", err)
	}
	if env.Payload == nil {
		return Envelope{}, contractErrorf("envelope payload is required")
	}
	for _, key := range spec.RequiredPayloadKeys {
		value, present := env.Payload[key]
		if !present || value == nil {
			return Envelope{}, contractErrorf("payload of %s is missing required key %q", wire.EventType, key)
		}
	}
	if key, found := ForbiddenPayload(env.Payload); found {
		return Envelope{}, contractErrorf("payload of %s carries forbidden material %q", wire.EventType, key)
	}
	return env, nil
}

// CheckChainIdentity verifies the envelope's chain identity against the local
// chain. expectedChainID <= 0 refuses every chain-carrying envelope (the
// local chain is configuration, never guessed). A missing chain identity on an
// evm_log event or a mismatch is ErrIdentityMismatch (contracts/events.md §6).
func (e Envelope) CheckChainIdentity(expectedChainID int64) error {
	if e.IdentityKind == IdentityKindEVMLog {
		if e.ChainID == nil || e.BlockNumber == nil || e.BlockHash == nil || *e.BlockHash == "" {
			return fmt.Errorf("%w: %s misses chain identity", ErrIdentityMismatch, e.EventType)
		}
	}
	if e.ChainID == nil {
		return nil
	}
	if expectedChainID <= 0 {
		return fmt.Errorf("%w: local chain is not configured", ErrIdentityMismatch)
	}
	if *e.ChainID != expectedChainID {
		return fmt.Errorf("%w: envelope chain %d != local chain %d", ErrIdentityMismatch, *e.ChainID, expectedChainID)
	}
	return nil
}

// Message is one delivered event: the Kafka coordinates plus the envelope
// bytes. A negative Offset means "no partition delivery" (a replay from a
// snapshot): the consumer then leaves consumer_progress untouched.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Value     []byte
}

// Effect applies the business effect of one event inside the T4 transaction.
// The effect MUST be idempotent at the transaction level: it commits together
// with the inbox row, so a redelivery never re-applies it (FR-13).
type Effect interface {
	Apply(ctx context.Context, tx pgx.Tx, env Envelope) error
}

// EffectFunc adapts a function to Effect.
type EffectFunc func(ctx context.Context, tx pgx.Tx, env Envelope) error

// Apply implements Effect.
func (f EffectFunc) Apply(ctx context.Context, tx pgx.Tx, env Envelope) error {
	if f == nil {
		return contractErrorf("consumer effect is required")
	}
	return f(ctx, tx, env)
}

// ConsumerObserver observes consumer outcomes (verification.md §1). A nil
// observer disables observation; *metrics.Metrics satisfies it.
type ConsumerObserver interface {
	ObserveConsumerApplied()
	ObserveConsumerRetry(failureClass string)
	ObserveConsumerQuarantine(failureClass string)
	SetConsumerLag(consumerName string, partition int, lagSeconds float64, lagMessages int64)
	ObserveEventReplay(opKind, consumerName string)
	SetConsumerReplayClock(consumerName string, unixSeconds float64)
}

// Sleeper waits for d or until ctx is done. Tests inject deterministic or
// immediate sleepers; nil means real time.
type Sleeper func(ctx context.Context, d time.Duration) error

func realSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// ConsumerOptions carries the validated bounds of one Consumer.
type ConsumerOptions struct {
	// GapWait bounds the wait for a missing earlier version before the event
	// is quarantined as version_gap (initial 10s, calibrated after
	// measurement; contracts/consumer.md §3).
	GapWait time.Duration
	// BackoffBase/BackoffMax/RetryLimit bound the retryable-failure backoff
	// (initial base 500ms / cap 30s / 8 attempts; contracts/consumer.md §4).
	BackoffBase time.Duration
	BackoffMax  time.Duration
	RetryLimit  int
	// ChainID is the local chain every chain-carrying envelope must match.
	ChainID int64
	// Sleep waits for the backoff/gap intervals; nil means real time.
	Sleep Sleeper
	// ProbeGap re-reads the aggregate's applied version while waiting for a
	// version gap to close. nil uses the PostgreSQL read. Tests inject it to
	// exercise the gap branches without a database.
	ProbeGap func(ctx context.Context, env Envelope) (maxVersion int64, found bool, err error)
	// Jitter returns a value in [-1, 1) for the ±20% retry jitter; nil uses
	// math/rand (tests inject deterministic jitter).
	Jitter   func() float64
	Observer ConsumerObserver
}

// Consumer processes delivered events through the T4 transaction. All durable
// state (inbox, versions, progress, quarantine) lives in PostgreSQL; Redis
// never participates in progress or dedup (FR-02/FR-15; contracts/consumer.md
// §9).
type Consumer struct {
	Pool        *pgxpool.Pool
	Name        string
	Effect      Effect
	GapWait     time.Duration
	BackoffBase time.Duration
	BackoffMax  time.Duration
	RetryLimit  int
	ChainID     int64
	Sleep       Sleeper
	ProbeGap    func(ctx context.Context, env Envelope) (int64, bool, error)
	Jitter      func() float64
	Observer    ConsumerObserver
	Quarantine  *QuarantineStore
}

// NewConsumer validates the runtime bounds fail-closed (a missing bound would
// mean unbounded behavior) and builds the consumer.
func NewConsumer(pool *pgxpool.Pool, name string, effect Effect, opts ConsumerOptions) (*Consumer, error) {
	if pool == nil {
		return nil, contractErrorf("consumer requires a database pool")
	}
	if strings.TrimSpace(name) == "" {
		return nil, contractErrorf("consumer requires a consumer name")
	}
	if effect == nil {
		return nil, contractErrorf("consumer requires an effect")
	}
	if opts.GapWait <= 0 {
		return nil, contractErrorf("consumer gap wait must be positive")
	}
	if opts.BackoffBase <= 0 {
		return nil, contractErrorf("consumer backoff base must be positive")
	}
	if opts.BackoffMax < opts.BackoffBase {
		return nil, contractErrorf("consumer backoff max must be >= base")
	}
	if opts.RetryLimit <= 0 {
		return nil, contractErrorf("consumer retry limit must be positive")
	}
	if opts.ChainID <= 0 {
		return nil, contractErrorf("consumer requires the local chain id")
	}
	sleep := opts.Sleep
	if sleep == nil {
		sleep = realSleep
	}
	jitter := opts.Jitter
	if jitter == nil {
		jitter = func() float64 { return rand.Float64()*2 - 1 }
	}
	return &Consumer{
		Pool:        pool,
		Name:        name,
		Effect:      effect,
		GapWait:     opts.GapWait,
		BackoffBase: opts.BackoffBase,
		BackoffMax:  opts.BackoffMax,
		RetryLimit:  opts.RetryLimit,
		ChainID:     opts.ChainID,
		Sleep:       sleep,
		ProbeGap:    opts.ProbeGap,
		Jitter:      jitter,
		Observer:    opts.Observer,
		Quarantine:  &QuarantineStore{Pool: pool},
	}, nil
}

// ProcessOutcome is the closed per-message result vocabulary.
type ProcessOutcome string

const (
	// OutcomeApplied: the event was applied exactly once (inbox insert,
	// version guard and effect committed together).
	OutcomeApplied ProcessOutcome = "applied"
	// OutcomeDuplicate: the inbox already carried the event (redelivery,
	// restart or rebalance); no second effect.
	OutcomeDuplicate ProcessOutcome = "duplicate"
	// OutcomeVersionSkip: the aggregate version was equal to or older than
	// the applied high-water mark; the newer state is never overwritten.
	OutcomeVersionSkip ProcessOutcome = "version_skip"
	// OutcomeQuarantined: the event was persisted in the quarantine with its
	// full snapshot; the partition keeps moving.
	OutcomeQuarantined ProcessOutcome = "quarantined"
)

// ProcessResult reports one Process outcome.
type ProcessResult struct {
	Outcome      ProcessOutcome
	Reason       QuarantineReason
	QuarantineID int64
	Attempts     int
}

// Process applies one delivered event through the T4 transaction with the
// bounded retry/gap policy:
//
//  1. parse/route/identity checks fail closed into a persistent quarantine
//     (non_retryable / schema_unsupported / identity_mismatch);
//  2. the T4 transaction: inbox dedup -> version guard -> effect ->
//     progress; a duplicate advances progress and skips the effect, an
//     equal/older version is skipped without overwriting, a gap rolls back
//     and waits;
//  3. retryable failures retry with the bounded backoff; exhaustion,
//     permanent failures and a surviving gap quarantine the event with its
//     snapshot and keep the partition moving (FR-10/13/14;
//     contracts/consumer.md §2–§4).
//
// Process never returns a retryable error to the caller: every delivered
// message reaches a terminal durable state (applied/duplicate/skipped/
// quarantined) or Process returns a transport/context error.
func (c *Consumer) Process(ctx context.Context, msg Message) (ProcessResult, error) {
	env, parseErr := ParseEnvelope(msg.Value)
	if parseErr != nil {
		reason, text := quarantineReasonFor(parseErr)
		env.EventID = quarantineIdentity(msg.Value)
		id, qerr := c.quarantineMessage(ctx, msg, env, reason, text, 0)
		if qerr != nil {
			return ProcessResult{}, qerr
		}
		return ProcessResult{Outcome: OutcomeQuarantined, Reason: reason, QuarantineID: id}, nil
	}
	if err := env.CheckChainIdentity(c.ChainID); err != nil {
		reason, text := quarantineReasonFor(err)
		id, qerr := c.quarantineMessage(ctx, msg, env, reason, text, 0)
		if qerr != nil {
			return ProcessResult{}, qerr
		}
		return ProcessResult{Outcome: OutcomeQuarantined, Reason: reason, QuarantineID: id}, nil
	}

	attempts := 0
	gapWaited := false
	for {
		attempts++
		outcome, err := c.applyOnce(ctx, msg, env)
		switch {
		case err == nil:
			if outcome == OutcomeApplied && c.Observer != nil {
				c.Observer.ObserveConsumerApplied()
			}
			return ProcessResult{Outcome: outcome, Attempts: attempts}, nil

		case errors.Is(err, ErrVersionGap):
			if !gapWaited {
				gapWaited = true
				if werr := c.waitForGap(ctx, env); werr == nil {
					continue
				}
			}
			if ctx.Err() != nil {
				return ProcessResult{}, ctx.Err()
			}
			id, qerr := c.quarantineMessage(ctx, msg, env, QuarantineVersionGap,
				fmt.Sprintf("version gap survived the %s wait window", c.GapWait), attempts)
			if qerr != nil {
				return ProcessResult{}, qerr
			}
			return ProcessResult{Outcome: OutcomeQuarantined, Reason: QuarantineVersionGap,
				QuarantineID: id, Attempts: attempts}, nil

		case ConsumerRetryable(err) && attempts < c.RetryLimit:
			if c.Observer != nil {
				c.Observer.ObserveConsumerRetry(string(ClassTransient))
			}
			delay := RetryDelay(attempts, c.BackoffBase, c.BackoffMax, c.Jitter)
			if serr := c.Sleep(ctx, delay); serr != nil {
				return ProcessResult{}, serr
			}
			continue

		default:
			reason := QuarantineNonRetryable
			if ConsumerRetryable(err) {
				reason = QuarantineRetryExhausted
			}
			id, qerr := c.quarantineMessage(ctx, msg, env, reason, err.Error(), attempts)
			if qerr != nil {
				return ProcessResult{}, qerr
			}
			return ProcessResult{Outcome: OutcomeQuarantined, Reason: reason,
				QuarantineID: id, Attempts: attempts}, nil
		}
	}
}

// quarantineReasonFor maps a parse/validation failure to its closed
// quarantine reason.
func quarantineReasonFor(err error) (QuarantineReason, string) {
	switch {
	case errors.Is(err, ErrSchemaUnsupported):
		return QuarantineSchemaUnsupported, err.Error()
	case errors.Is(err, ErrIdentityMismatch):
		return QuarantineIdentityMismatch, err.Error()
	default:
		return QuarantineNonRetryable, err.Error()
	}
}

// quarantineIdentity derives a deterministic quarantine identity for a
// message that could not be parsed: distinct delivered bytes get distinct
// entries (two poison messages are never conflated) and identical bytes
// converge on one open entry on redelivery. The snapshot keeps the exact
// bytes, so an operator can still inspect and replay the original message.
func quarantineIdentity(raw []byte) uuid.UUID {
	sum := sha256.Sum256(raw)
	return uuid.NewSHA1(eventIDNamespace, append([]byte("quarantine|"), sum[:]...))
}

// guardDecision is the pure version-guard verdict (contracts/consumer.md §3):
// apply when the version is newer (or the object has no baseline yet), skip
// when it is equal/older, gap when it jumps beyond max+1.
type guardDecision int

const (
	guardApply guardDecision = iota
	guardSkip
	guardGap
)

func decideVersionGuard(found bool, maxVersion, eventVersion int64) guardDecision {
	if !found {
		// First-seen baseline: the cutover stream starts at version 1 and an
		// absent record is never treated as a gap (contracts/consumer.md §3).
		return guardApply
	}
	switch {
	case eventVersion > maxVersion+1:
		return guardGap
	case eventVersion > maxVersion:
		return guardApply
	default:
		return guardSkip
	}
}

// applyOnce runs one T4 application transaction:
//
//	BEGIN
//	  INSERT consumer_inbox ... ON CONFLICT DO NOTHING   -- 0 rows = applied before
//	  SELECT consumer_versions ... FOR UPDATE            -- version guard
//	  effect                                             -- in the same transaction
//	  UPSERT consumer_versions                           -- monotonic max_version
//	  UPSERT consumer_progress                           -- durable high-water mark
//	COMMIT
//
// A duplicate advances progress and returns without touching the effect. An
// equal/older version rolls back (no durable inbox marker, so a later replay
// is not swallowed) and returns a skip. A gap rolls back and returns
// ErrVersionGap. A concurrent writer that advanced the version past this event
// makes the upsert update zero rows: the transaction rolls back and the
// failure is classified retryable so the caller re-runs the guard instead of
// overwriting (data-model §5 T4).
func (c *Consumer) applyOnce(ctx context.Context, msg Message, env Envelope) (ProcessOutcome, error) {
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return "", Transient(fmt.Errorf("begin apply transaction: %w", err))
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	tag, err := tx.Exec(ctx, insertInboxSQL,
		c.Name, env.EventID, env.AggregateType, env.AggregateID, env.AggregateVersion,
		msg.Topic, msg.Partition, msg.Offset)
	if err != nil {
		return "", ClassifyPGError(err)
	}
	if tag.RowsAffected() == 0 {
		// Already applied: the redelivery advances progress (monotonically)
		// and skips the effect (SC-04; contracts/consumer.md §2).
		if err := c.advanceProgress(ctx, tx, msg); err != nil {
			return "", err
		}
		if err := tx.Commit(ctx); err != nil {
			return "", Transient(fmt.Errorf("commit duplicate skip: %w", err))
		}
		return OutcomeDuplicate, nil
	}

	var maxVersion int64
	found := true
	if err := tx.QueryRow(ctx, readVersionSQL, c.Name, env.AggregateType, env.AggregateID).Scan(&maxVersion); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			found = false
			maxVersion = 0
		} else {
			return "", ClassifyPGError(err)
		}
	}
	switch decideVersionGuard(found, maxVersion, env.AggregateVersion) {
	case guardSkip:
		// Old/equal version: no effect and no durable inbox marker, so a
		// later replay is not swallowed by the dedup. The T4 transaction is
		// rolled back and progress advances on its own so the partition is
		// not blocked by a stale redelivery.
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			return "", Transient(fmt.Errorf("rollback version skip: %w", err))
		}
		if err := c.advanceProgressStandalone(ctx, msg); err != nil {
			return "", err
		}
		return OutcomeVersionSkip, nil
	case guardGap:
		return "", fmt.Errorf("%w: aggregate %s/%s applied=%d event=%d",
			ErrVersionGap, env.AggregateType, env.AggregateID, maxVersion, env.AggregateVersion)
	}

	if err := c.Effect.Apply(ctx, tx, env); err != nil {
		return "", err
	}

	var versionTag pgconn.CommandTag
	if found {
		versionTag, err = tx.Exec(ctx, updateVersionSQL,
			c.Name, env.AggregateType, env.AggregateID, env.AggregateVersion)
	} else {
		versionTag, err = tx.Exec(ctx, insertVersionSQL,
			c.Name, env.AggregateType, env.AggregateID, env.AggregateVersion)
	}
	if err != nil {
		return "", ClassifyPGError(err)
	}
	if versionTag.RowsAffected() == 0 {
		// A concurrent writer applied a newer version first: roll the whole
		// transaction back and re-run the guard (never overwrite, never
		// double-apply).
		return "", Transient(fmt.Errorf("consumer version advanced concurrently for %s/%s",
			env.AggregateType, env.AggregateID))
	}

	if err := c.advanceProgress(ctx, tx, msg); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", Transient(fmt.Errorf("commit apply: %w", err))
	}
	return OutcomeApplied, nil
}

// advanceProgress moves the durable high-water mark to offset+1 when the
// message carries a real partition delivery. GREATEST keeps it monotonic: a
// replay or a stale redelivery never rewinds progress (FR-15).
func (c *Consumer) advanceProgress(ctx context.Context, tx pgx.Tx, msg Message) error {
	if msg.Offset < 0 {
		return nil
	}
	if msg.Topic == "" {
		return contractErrorf("consumer progress requires a topic")
	}
	if _, err := tx.Exec(ctx, advanceProgressSQL, c.Name, msg.Topic, msg.Partition, msg.Offset+1); err != nil {
		return ClassifyPGError(err)
	}
	return nil
}

// advanceProgressStandalone moves the durable high-water mark outside a
// transaction (the version-skip path, where the T4 transaction is rolled back
// to keep the inbox free of a marker for an event that was never applied).
func (c *Consumer) advanceProgressStandalone(ctx context.Context, msg Message) error {
	if msg.Offset < 0 {
		return nil
	}
	if msg.Topic == "" {
		return contractErrorf("consumer progress requires a topic")
	}
	if _, err := c.Pool.Exec(ctx, advanceProgressSQL, c.Name, msg.Topic, msg.Partition, msg.Offset+1); err != nil {
		return ClassifyPGError(err)
	}
	return nil
}

// waitForGap waits inside the configured window for the missing earlier
// version to arrive: another worker (or a replay) may apply it meanwhile. It
// returns nil when the gap closed (or the object gained a baseline covering
// the event) and an error when the window expired. The wait is bounded and
// never holds a database transaction or a lock (FR-10/FR-14).
func (c *Consumer) waitForGap(ctx context.Context, env Envelope) error {
	deadline := time.Now().Add(c.GapWait)
	step := c.GapWait / 10
	if step > 500*time.Millisecond {
		step = 500 * time.Millisecond
	}
	if step <= 0 {
		step = c.GapWait
	}
	for {
		maxVersion, found, err := c.probeGap(ctx, env)
		if err != nil {
			return err
		}
		if !found || env.AggregateVersion <= maxVersion+1 {
			return nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("%w: aggregate %s/%s still below %d", ErrVersionGap,
				env.AggregateType, env.AggregateID, env.AggregateVersion)
		}
		wait := step
		if wait > remaining {
			wait = remaining
		}
		if err := c.Sleep(ctx, wait); err != nil {
			return err
		}
	}
}

// probeGap reads the aggregate's applied version without a lock; the injected
// probe wins when configured.
func (c *Consumer) probeGap(ctx context.Context, env Envelope) (int64, bool, error) {
	if c.ProbeGap != nil {
		return c.ProbeGap(ctx, env)
	}
	var maxVersion int64
	err := c.Pool.QueryRow(ctx, readVersionNoLockSQL, c.Name, env.AggregateType, env.AggregateID).Scan(&maxVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, ClassifyPGError(err)
	}
	return maxVersion, true, nil
}

// quarantineMessage persists one event (or unparseable bytes) with its full
// snapshot and advances the partition progress in the same transaction, so a
// quarantined event never blocks the partition and is never silently dropped
// (T5; FR-14; contracts/consumer.md §4). A duplicate open quarantine item is
// an idempotent no-op.
func (c *Consumer) quarantineMessage(ctx context.Context, msg Message, env Envelope,
	reason QuarantineReason, text string, attempts int) (int64, error) {
	if c.Pool == nil {
		return 0, contractErrorf("consumer quarantine requires a database pool")
	}
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return 0, Transient(fmt.Errorf("begin quarantine transaction: %w", err))
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	entry := QuarantineEntry{
		ConsumerName:    c.Name,
		EventID:         env.EventID,
		EventSnapshot:   quarantineSnapshot(msg.Value, env, text),
		FailureClass:    reason,
		Reason:          text,
		AttemptCount:    attempts,
		SourceTopic:     msg.Topic,
		SourcePartition: msg.Partition,
		SourceOffset:    msg.Offset,
	}
	id, _, err := c.Quarantine.Enqueue(ctx, tx, entry)
	if err != nil {
		return 0, err
	}
	if err := c.advanceProgress(ctx, tx, msg); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, Transient(fmt.Errorf("commit quarantine: %w", err))
	}
	if c.Observer != nil {
		c.Observer.ObserveConsumerQuarantine(string(reason))
	}
	return id, nil
}

// quarantineSnapshot builds the JSONB snapshot stored with a quarantine
// entry: the exact delivered bytes when they are a JSON object, otherwise a
// minimal forensic object carrying the raw bytes and the parse error. The
// snapshot is never a re-serialization of the parsed envelope, so a replay
// feeds the identical event identity and payload (data-model §3.5; Table 5).
func quarantineSnapshot(raw []byte, env Envelope, parseError string) []byte {
	if len(raw) > 0 && json.Valid(raw) && jsonObject(raw) {
		return append([]byte(nil), raw...)
	}
	fallback := map[string]any{
		"raw_hex":     hex.EncodeToString(raw),
		"parse_error": parseError,
	}
	if env.EventID != uuid.Nil {
		fallback["event_id"] = env.EventID.String()
		fallback["event_type"] = env.EventType
		fallback["aggregate_type"] = env.AggregateType
		fallback["aggregate_id"] = env.AggregateID
		fallback["aggregate_version"] = env.AggregateVersion
	}
	body, err := json.Marshal(fallback)
	if err != nil {
		return []byte(`{"parse_error":"snapshot marshal failed"}`)
	}
	return body
}

// jsonObject reports whether data starts a JSON object (a JSONB column
// requires an object/array/scalar; the transport envelope is always an
// object).
func jsonObject(data []byte) bool {
	trimmed := strings.TrimSpace(string(data))
	return strings.HasPrefix(trimmed, "{")
}

// Consumer progress/version/dedup SQL. Every statement runs inside the T4
// transaction; no statement reaches Redis and none performs a network call.
const (
	insertInboxSQL = `
INSERT INTO consumer_inbox (
	consumer_name, event_id, aggregate_type, aggregate_id, aggregate_version,
	topic, partition, "offset"
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (consumer_name, event_id) DO NOTHING`

	readVersionSQL = `
SELECT max_version FROM consumer_versions
WHERE consumer_name = $1 AND aggregate_type = $2 AND aggregate_id = $3
FOR UPDATE`

	readVersionNoLockSQL = `
SELECT max_version FROM consumer_versions
WHERE consumer_name = $1 AND aggregate_type = $2 AND aggregate_id = $3`

	updateVersionSQL = `
UPDATE consumer_versions
SET max_version = $4, updated_at = now()
WHERE consumer_name = $1 AND aggregate_type = $2 AND aggregate_id = $3
  AND max_version < $4`

	insertVersionSQL = `
INSERT INTO consumer_versions (consumer_name, aggregate_type, aggregate_id, max_version)
VALUES ($1, $2, $3, $4)
ON CONFLICT (consumer_name, aggregate_type, aggregate_id)
DO UPDATE SET max_version = EXCLUDED.max_version, updated_at = now()
WHERE consumer_versions.max_version < EXCLUDED.max_version`

	advanceProgressSQL = `
INSERT INTO consumer_progress (consumer_name, topic, partition, next_offset)
VALUES ($1, $2, $3, $4)
ON CONFLICT (consumer_name, topic, partition)
DO UPDATE SET next_offset = GREATEST(consumer_progress.next_offset, EXCLUDED.next_offset),
              updated_at = now()`
)

// Progress is one durable consumer high-water mark (contracts/consumer.md §5).
type Progress struct {
	ConsumerName string
	Topic        string
	Partition    int
	NextOffset   int64
	UpdatedAt    time.Time
}

// ReadProgress returns the durable high-water marks of one consumer, ordered
// by topic/partition. This is the authoritative resume point: restart and
// rebalance positions derive from it, never from memory (FR-15).
func (c *Consumer) ReadProgress(ctx context.Context) ([]Progress, error) {
	rows, err := c.Pool.Query(ctx, `
SELECT consumer_name, topic, partition, next_offset, updated_at
FROM consumer_progress
WHERE consumer_name = $1
ORDER BY topic, partition`, c.Name)
	if err != nil {
		return nil, ClassifyPGError(err)
	}
	defer rows.Close()
	var out []Progress
	for rows.Next() {
		var p Progress
		if err := rows.Scan(&p.ConsumerName, &p.Topic, &p.Partition, &p.NextOffset, &p.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan consumer progress: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate consumer progress: %w", err)
	}
	return out, nil
}

// ResumeOffset returns the durable next offset of one partition, found=false
// when the consumer has no progress row yet (the Kafka committed offset is
// then the fallback; contracts/consumer.md §5).
func (c *Consumer) ResumeOffset(ctx context.Context, topic string, partition int) (int64, bool, error) {
	var next int64
	err := c.Pool.QueryRow(ctx, `
SELECT next_offset FROM consumer_progress
WHERE consumer_name = $1 AND topic = $2 AND partition = $3`,
		c.Name, topic, partition).Scan(&next)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, ClassifyPGError(err)
	}
	return next, true, nil
}

// ProgressUpdatedAt returns the timestamp of the last durable progress
// advance of one partition, found=false when the consumer has no progress row
// yet. It feeds the PG-side catch-up lag observation.
func (c *Consumer) ProgressUpdatedAt(ctx context.Context, topic string, partition int) (time.Time, bool, error) {
	var updatedAt time.Time
	err := c.Pool.QueryRow(ctx, `
SELECT updated_at FROM consumer_progress
WHERE consumer_name = $1 AND topic = $2 AND partition = $3`,
		c.Name, topic, partition).Scan(&updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, ClassifyPGError(err)
	}
	return updatedAt, true, nil
}

// KafkaConsumerConfig bounds the franz-go group consumer (T044). All values
// are validated fail-closed; the group id is the configured prefix plus the
// consumer name so the reference consumer never shares a group with another
// consumer (contracts/consumer.md §8).
type KafkaConsumerConfig struct {
	Brokers        []string
	Topic          string
	GroupPrefix    string
	ConsumerName   string
	PollBatch      int
	CommitInterval time.Duration
}

// KafkaConsumer is the production delivery loop: one consumer group, per
// partition sequential processing, the durable PostgreSQL progress as the
// authoritative resume point, Kafka offset commits strictly after the effect
// transaction, and bounded lag observation. Acknowledged delivery never means
// a consumer processed the event and never grants any send permission
// (contracts/consumer.md §1/§5/§9).
type KafkaConsumer struct {
	Client   *kgo.Client
	Consumer *Consumer
	Topic    string
	GroupID  string

	pollBatch      int
	commitInterval time.Duration
	observer       ConsumerObserver
}

// NewKafkaConsumer validates the bounds and builds the group consumer. The
// caller owns Close.
func NewKafkaConsumer(cfg KafkaConsumerConfig, consumer *Consumer) (*KafkaConsumer, error) {
	if consumer == nil {
		return nil, contractErrorf("kafka consumer requires a consumer processor")
	}
	if len(cfg.Brokers) == 0 {
		return nil, contractErrorf("kafka consumer requires at least one broker")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, contractErrorf("kafka consumer requires a topic")
	}
	if strings.TrimSpace(cfg.ConsumerName) == "" {
		return nil, contractErrorf("kafka consumer requires a consumer name")
	}
	if cfg.PollBatch <= 0 {
		return nil, contractErrorf("kafka consumer poll batch must be positive")
	}
	if cfg.CommitInterval <= 0 {
		return nil, contractErrorf("kafka consumer commit interval must be positive")
	}
	groupID := cfg.ConsumerName
	if strings.TrimSpace(cfg.GroupPrefix) != "" {
		groupID = cfg.GroupPrefix + "." + cfg.ConsumerName
	}
	k := &KafkaConsumer{
		Consumer:       consumer,
		Topic:          cfg.Topic,
		GroupID:        groupID,
		pollBatch:      cfg.PollBatch,
		commitInterval: cfg.CommitInterval,
		observer:       consumer.Observer,
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(cfg.Topic),
		// An unknown partition starts at the beginning of the topic: the
		// durable progress (when present) overrides this in the assignment
		// callback, and at-least-once delivery never skips an unprocessed
		// event (contracts/consumer.md §5).
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Commits are effect-driven: only offsets marked after the effect
		// transaction commits are autocommitted (async, bounded interval).
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(cfg.CommitInterval),
		// A rebalance must not revoke a partition while its records are
		// being processed: the poll loop calls AllowRebalance when the batch
		// is settled.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(k.onPartitionsAssigned),
		kgo.OnPartitionsRevoked(k.onPartitionsRevoked),
		kgo.OnPartitionsLost(k.onPartitionsLost),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer client: %w", err)
	}
	k.Client = client
	return k, nil
}

// onPartitionsAssigned resolves the start position of every newly assigned
// partition: max(durable PostgreSQL progress, Kafka committed offset), with
// the committed offset as the fallback when PostgreSQL has no record yet
// (contracts/consumer.md §5). The client has loaded the committed offsets
// before this callback runs.
func (k *KafkaConsumer) onPartitionsAssigned(ctx context.Context, cl *kgo.Client, assigned map[string][]int32) {
	committed := cl.CommittedOffsets()
	set := make(map[string]map[int32]kgo.EpochOffset, len(assigned))
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			start, ok := k.resolveStart(ctx, topic, partition, committed)
			if !ok {
				continue
			}
			if set[topic] == nil {
				set[topic] = make(map[int32]kgo.EpochOffset)
			}
			set[topic][partition] = kgo.EpochOffset{Epoch: -1, Offset: start}
		}
	}
	cl.SetOffsets(set)
}

// resolveStart computes the resume position of one partition. It returns
// ok=false when neither source has a position (the client's reset offset
// applies).
func (k *KafkaConsumer) resolveStart(ctx context.Context, topic string, partition int32,
	committed map[string]map[int32]kgo.EpochOffset) (int64, bool) {
	pgNext, pgFound, err := k.Consumer.ResumeOffset(ctx, topic, int(partition))
	if err != nil {
		// A progress read failure must not silently skip work: fall back to
		// the committed offset (or the reset offset) so the inbox absorbs
		// any redelivery.
		pgFound = false
	}
	var kafkaCommitted int64
	kafkaFound := false
	if topicOffsets, ok := committed[topic]; ok {
		if epochOffset, ok := topicOffsets[partition]; ok && epochOffset.Offset >= 0 {
			kafkaCommitted = epochOffset.Offset
			kafkaFound = true
		}
	}
	switch {
	case pgFound && kafkaFound:
		if kafkaCommitted > pgNext {
			return kafkaCommitted, true
		}
		return pgNext, true
	case pgFound:
		return pgNext, true
	case kafkaFound:
		return kafkaCommitted, true
	default:
		return 0, false
	}
}

// onPartitionsRevoked commits the marked offsets before the partition moves
// (the durable effect is already committed, so the commit is an
// optimization; a failed commit only causes a redelivery the inbox absorbs).
func (k *KafkaConsumer) onPartitionsRevoked(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
	_ = cl.CommitMarkedOffsets(ctx)
}

// onPartitionsLost re-seeks nothing: lost partitions are not committed (the
// group no longer owns them) and their records are redelivered to the new
// owner.
func (k *KafkaConsumer) onPartitionsLost(_ context.Context, _ *kgo.Client, _ map[string][]int32) {
}

// Run drives the bounded poll/process/commit loop until ctx is cancelled:
// per-partition sequential processing, the effect transaction before the
// Kafka offset mark, a rebalance gate around each batch, periodic lag
// observation and a graceful stop (finish the in-flight batch, commit marked
// offsets, close the client).
func (k *KafkaConsumer) Run(ctx context.Context) error {
	defer k.Client.Close()
	for {
		if ctx.Err() != nil {
			return nil
		}
		fetches := k.Client.PollRecords(ctx, k.pollBatch)
		if fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if ctx.Err() == nil {
				// Fetch errors are transport-level: the durable state is
				// unaffected and the partition is retried by the client.
				fmt.Printf("txharbor event-consumer: fetch error topic=%s partition=%d: %v\n", topic, partition, err)
			}
		})
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			result, err := k.Consumer.Process(ctx, Message{
				Topic:     record.Topic,
				Partition: int(record.Partition),
				Offset:    record.Offset,
				Value:     record.Value,
			})
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				// A transport-level failure (database unavailable) leaves
				// the record unprocessed: do not mark it, so it is
				// redelivered instead of being lost.
				return fmt.Errorf("consume %s[%d]@%d: %w", record.Topic, record.Partition, record.Offset, err)
			}
			_ = result
			// The effect (or the durable quarantine) is committed; only now
			// may the Kafka offset be marked for commit.
			k.Client.MarkCommitRecords(record)
		}
		k.Client.AllowRebalance()
		k.refreshLag(ctx)
	}
}

// refreshLag observes the Kafka partition lag (messages) and the durable
// progress age (the PG catch-up watermark) through the configured observer.
// Values are observations only: lag never gates a decision.
func (k *KafkaConsumer) refreshLag(ctx context.Context) {
	if k.observer == nil {
		return
	}
	lags, err := k.Lag(ctx)
	if err != nil {
		return
	}
	for partition, lag := range lags {
		// The second lag dimension is the PG catch-up watermark: how long
		// the durable progress of this partition has been standing still.
		// With no Kafka backlog the watermark age is 0; a standing watermark
		// while a backlog exists is the "consumer is not catching up" signal
		// (verification.md §1; contracts/consumer.md §5).
		lagSeconds := 0.0
		if lag > 0 {
			if updatedAt, found, rerr := k.Consumer.ProgressUpdatedAt(ctx, k.Topic, int(partition)); found && rerr == nil {
				if age := time.Since(updatedAt).Seconds(); age > 0 {
					lagSeconds = age
				}
			}
		}
		k.observer.SetConsumerLag(k.Consumer.Name, int(partition), lagSeconds, lag)
	}
}

// Lag returns the consumer group's per-partition message lag from the broker
// (Kafka high watermark minus the group position). It is read-only and
// bounded by the caller's context. A partition lag of -1 means the broker
// could not compute it; it is reported as 0 so a transient broker error never
// fabricates a backlog.
func (k *KafkaConsumer) Lag(ctx context.Context) (map[int32]int64, error) {
	adm := kadm.NewClient(k.Client)
	described, err := adm.Lag(ctx, k.GroupID)
	if err != nil {
		return nil, fmt.Errorf("kafka group lag: %w", err)
	}
	out := map[int32]int64{}
	group, ok := described[k.GroupID]
	if !ok {
		return out, nil
	}
	for topic, partitions := range group.Lag {
		if topic != k.Topic {
			continue
		}
		for partition, memberLag := range partitions {
			lag := memberLag.Lag
			if lag < 0 {
				lag = 0
			}
			out[partition] = lag
		}
	}
	return out, nil
}
