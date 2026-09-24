// quarantine.go owns the persistent poison-event quarantine (T042) and the
// audited operator primitives the events-admin command drives (T045):
// replay, unblock and retention prune.
//
// The quarantine is durable and never deletes an event: an entry carries the
// full event snapshot, the closed failure class, the reason, the attempt
// count and the delivery coordinates. A quarantined event never blocks the
// partition (the consumer advances progress and continues), and a replay
// after the cause is fixed goes through the same T4 transaction, so the
// persistent idempotency and version guard still decide (FR-14; PD-4;
// contracts/consumer.md §4/§6).
//
// Replay is strictly a re-delivery/re-processing of historical events. It
// MUST NOT re-execute a chain payment, create a withdrawal intent, allocate a
// nonce, sign or broadcast: the only effect a replay can reach is the wired
// consumer's effect, inside its T4 transaction.
package events

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// QuarantineStatus is the closed quarantine state machine (data-model §8).
type QuarantineStatus string

const (
	// QuarantineStatusOpen: awaiting an operator decision or a fix.
	QuarantineStatusOpen QuarantineStatus = "open"
	// QuarantineStatusReplayed: an audited replay handled the event.
	QuarantineStatusReplayed QuarantineStatus = "replayed"
	// QuarantineStatusSuperseded: a newer decision replaced this entry.
	QuarantineStatusSuperseded QuarantineStatus = "superseded"
)

// Valid reports whether s is a quarantine status.
func (s QuarantineStatus) Valid() bool {
	switch s {
	case QuarantineStatusOpen, QuarantineStatusReplayed, QuarantineStatusSuperseded:
		return true
	}
	return false
}

// CanTransition reports whether the quarantine state machine allows from->to:
// only open -> replayed (audited replay) and open -> superseded (a newer
// decision). Every other transition updates zero rows and is refused.
func (s QuarantineStatus) CanTransition(to QuarantineStatus) bool {
	return s == QuarantineStatusOpen && (to == QuarantineStatusReplayed || to == QuarantineStatusSuperseded)
}

// QuarantineEntry is one persisted quarantine row (data-model Table 5).
type QuarantineEntry struct {
	ID                int64
	ConsumerName      string
	EventID           uuid.UUID
	EventSnapshot     []byte
	FailureClass      QuarantineReason
	Reason            string
	AttemptCount      int
	SourceTopic       string
	SourcePartition   int
	SourceOffset      int64
	FirstSeenAt       time.Time
	LastSeenAt        time.Time
	Status            QuarantineStatus
	ReplayedAt        *time.Time
	ReplayOperationID *string
}

// QuarantineStore is the PostgreSQL quarantine surface. Every write takes the
// caller's transaction so a quarantine entry and its progress advance commit
// together (T5; contracts/consumer.md §4).
type QuarantineStore struct {
	Pool *pgxpool.Pool
}

// Enqueue persists one quarantine entry. The partial unique index keeps at
// most one open entry per (consumer, event): a repeated quarantine is an
// idempotent no-op that returns the existing open row (created=false).
func (s *QuarantineStore) Enqueue(ctx context.Context, tx pgx.Tx, entry QuarantineEntry) (int64, bool, error) {
	if s == nil || s.Pool == nil {
		return 0, false, contractErrorf("quarantine store requires a database pool")
	}
	if tx == nil {
		return 0, false, contractErrorf("quarantine enqueue requires an in-progress transaction")
	}
	if strings.TrimSpace(entry.ConsumerName) == "" {
		return 0, false, contractErrorf("quarantine entry requires a consumer name")
	}
	if !entry.FailureClass.Valid() {
		return 0, false, contractErrorf("quarantine entry requires a closed failure class, got %q", entry.FailureClass)
	}
	if len(entry.EventSnapshot) == 0 || !json.Valid(entry.EventSnapshot) {
		return 0, false, contractErrorf("quarantine entry requires a valid JSON snapshot")
	}
	var id int64
	err := tx.QueryRow(ctx, insertQuarantineSQL,
		entry.ConsumerName, entry.EventID, string(entry.EventSnapshot), string(entry.FailureClass),
		entry.Reason, entry.AttemptCount,
		nullableString(entry.SourceTopic), nullablePartition(entry.SourcePartition),
		nullableOffset(entry.SourceOffset)).Scan(&id)
	if err == nil {
		return id, true, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		// The partial unique index refused a second open entry: return the
		// existing one (idempotent quarantine).
		var existing int64
		if qerr := tx.QueryRow(ctx, selectOpenQuarantineSQL, entry.ConsumerName, entry.EventID).Scan(&existing); qerr != nil {
			return 0, false, ClassifyPGError(qerr)
		}
		return existing, false, nil
	}
	return 0, false, ClassifyPGError(err)
}

// MarkReplayed flips one open entry to replayed under the audited operation
// id. A non-open entry updates zero rows and is refused (illegal transition).
func (s *QuarantineStore) MarkReplayed(ctx context.Context, tx pgx.Tx, id int64, operationID string) (bool, error) {
	tag, err := tx.Exec(ctx, markReplayedSQL, id, operationID)
	if err != nil {
		return false, ClassifyPGError(err)
	}
	return tag.RowsAffected() == 1, nil
}

// MarkSuperseded flips one open entry to superseded with a reason (a newer
// decision replaced it). A non-open entry updates zero rows.
func (s *QuarantineStore) MarkSuperseded(ctx context.Context, tx pgx.Tx, id int64, reason string) (bool, error) {
	tag, err := tx.Exec(ctx, markSupersededSQL, id, reason)
	if err != nil {
		return false, ClassifyPGError(err)
	}
	return tag.RowsAffected() == 1, nil
}

// Get reads one quarantine entry by id.
func (s *QuarantineStore) Get(ctx context.Context, id int64) (QuarantineEntry, bool, error) {
	rows, err := s.Pool.Query(ctx, selectQuarantineSQL+` WHERE id = $1`, id)
	if err != nil {
		return QuarantineEntry{}, false, ClassifyPGError(err)
	}
	defer rows.Close()
	entries, err := scanQuarantineRows(rows)
	if err != nil {
		return QuarantineEntry{}, false, err
	}
	if len(entries) == 0 {
		return QuarantineEntry{}, false, nil
	}
	return entries[0], true, nil
}

// ListOpen returns the open quarantine entries of one consumer, oldest first.
func (s *QuarantineStore) ListOpen(ctx context.Context, consumerName string) ([]QuarantineEntry, error) {
	rows, err := s.Pool.Query(ctx, selectQuarantineSQL+
		` WHERE consumer_name = $1 AND status = 'open' ORDER BY id`, consumerName)
	if err != nil {
		return nil, ClassifyPGError(err)
	}
	defer rows.Close()
	return scanQuarantineRows(rows)
}

// OpenCount returns the open quarantine size of one consumer.
func (s *QuarantineStore) OpenCount(ctx context.Context, consumerName string) (int64, error) {
	var n int64
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM consumer_quarantine WHERE consumer_name = $1 AND status = 'open'`,
		consumerName).Scan(&n); err != nil {
		return 0, ClassifyPGError(err)
	}
	return n, nil
}

// listByEventIDs returns the quarantine entries (any status) of one consumer
// for the given event ids, oldest first.
func (s *QuarantineStore) listByEventIDs(ctx context.Context, consumerName string, ids []uuid.UUID) ([]QuarantineEntry, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.Pool.Query(ctx, selectQuarantineSQL+
		` WHERE consumer_name = $1 AND event_id = ANY($2) ORDER BY id`, consumerName, ids)
	if err != nil {
		return nil, ClassifyPGError(err)
	}
	defer rows.Close()
	return scanQuarantineRows(rows)
}

// listForAggregate returns the quarantine entries of one consumer for one
// aggregate, oldest first.
func (s *QuarantineStore) listForAggregate(ctx context.Context, consumerName, aggregateType, aggregateID string) ([]QuarantineEntry, error) {
	rows, err := s.Pool.Query(ctx, selectQuarantineSQL+`
 WHERE consumer_name = $1
   AND event_snapshot->>'aggregate_type' = $2
   AND event_snapshot->>'aggregate_id' = $3
 ORDER BY id`, consumerName, aggregateType, aggregateID)
	if err != nil {
		return nil, ClassifyPGError(err)
	}
	defer rows.Close()
	return scanQuarantineRows(rows)
}

// listByOffsetRange returns the quarantine entries of one consumer whose
// source coordinates fall inside the given range, oldest first.
func (s *QuarantineStore) listByOffsetRange(ctx context.Context, consumerName, topic string, partition int, from, to int64) ([]QuarantineEntry, error) {
	rows, err := s.Pool.Query(ctx, selectQuarantineSQL+`
 WHERE consumer_name = $1 AND source_topic = $2 AND source_partition = $3
   AND source_offset >= $4 AND source_offset <= $5
 ORDER BY source_offset, id`, consumerName, topic, partition, from, to)
	if err != nil {
		return nil, ClassifyPGError(err)
	}
	defer rows.Close()
	return scanQuarantineRows(rows)
}

func scanQuarantineRows(rows pgx.Rows) ([]QuarantineEntry, error) {
	var out []QuarantineEntry
	for rows.Next() {
		var (
			entry        QuarantineEntry
			eventID      uuid.UUID
			snapshot     []byte
			failureClass string
			sourceTopic  *string
			sourcePart   *int
			sourceOffset *int64
			status       string
			replayedAt   *time.Time
			replayOpID   *string
		)
		if err := rows.Scan(&entry.ID, &entry.ConsumerName, &eventID, &snapshot, &failureClass,
			&entry.Reason, &entry.AttemptCount, &sourceTopic, &sourcePart, &sourceOffset,
			&entry.FirstSeenAt, &entry.LastSeenAt, &status, &replayedAt, &replayOpID); err != nil {
			return nil, fmt.Errorf("scan quarantine row: %w", err)
		}
		entry.EventID = eventID
		entry.EventSnapshot = append([]byte(nil), snapshot...)
		entry.FailureClass = QuarantineReason(failureClass)
		entry.Status = QuarantineStatus(status)
		entry.ReplayedAt = replayedAt
		entry.ReplayOperationID = replayOpID
		if sourceTopic != nil {
			entry.SourceTopic = *sourceTopic
		}
		if sourcePart != nil {
			entry.SourcePartition = *sourcePart
		} else {
			entry.SourcePartition = -1
		}
		if sourceOffset != nil {
			entry.SourceOffset = *sourceOffset
		} else {
			entry.SourceOffset = -1
		}
		out = append(out, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate quarantine rows: %w", err)
	}
	return out, nil
}

// nullablePartition stores a partition, NULL when the entry has no delivery
// coordinates (a replay from an outbox row).
func nullablePartition(v int) any {
	if v < 0 {
		return nil
	}
	return v
}

// nullableOffset stores an offset, NULL when the entry has no delivery
// coordinates.
func nullableOffset(v int64) any {
	if v < 0 {
		return nil
	}
	return v
}

// Quarantine SQL. The partial unique index consumer_quarantine_open_uniq is
// the conflict target so a repeated quarantine of the same event is an
// idempotent no-op (T5; data-model §10).
const (
	insertQuarantineSQL = `
INSERT INTO consumer_quarantine (
	consumer_name, event_id, event_snapshot, failure_class, reason, attempt_count,
	source_topic, source_partition, source_offset, first_seen_at, last_seen_at
) VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, $8, $9, now(), now())
ON CONFLICT (consumer_name, event_id) WHERE status = 'open' DO NOTHING
RETURNING id`

	selectOpenQuarantineSQL = `
SELECT id FROM consumer_quarantine
WHERE consumer_name = $1 AND event_id = $2 AND status = 'open'`

	selectQuarantineSQL = `
SELECT id, consumer_name, event_id, event_snapshot, failure_class, reason, attempt_count,
       source_topic, source_partition, source_offset, first_seen_at, last_seen_at,
       status, replayed_at, replay_operation_id
FROM consumer_quarantine`

	markReplayedSQL = `
UPDATE consumer_quarantine
SET status = 'replayed', replayed_at = now(), replay_operation_id = $2
WHERE id = $1 AND status = 'open'`

	markSupersededSQL = `
UPDATE consumer_quarantine
SET status = 'superseded', replayed_at = now(), reason = reason || ' | superseded: ' || $2
WHERE id = $1 AND status = 'open'`
)

// ReplayScopeKind is the closed replay scope vocabulary (contracts/consumer.md
// §6).
type ReplayScopeKind string

const (
	// ReplayScopeEventIDs replays the named events (quarantine snapshot first,
	// outbox row as the fallback source).
	ReplayScopeEventIDs ReplayScopeKind = "event-ids"
	// ReplayScopeAggregate replays every event of one business object.
	ReplayScopeAggregate ReplayScopeKind = "aggregate"
	// ReplayScopeTimeRange replays outbox events created inside a time range.
	ReplayScopeTimeRange ReplayScopeKind = "time-range"
	// ReplayScopeOffsetRange replays quarantined events by their delivery
	// coordinates.
	ReplayScopeOffsetRange ReplayScopeKind = "offset-range"
)

// ReplayScope is one operator-selected event range (audited verbatim).
type ReplayScope struct {
	Kind     ReplayScopeKind
	EventIDs []uuid.UUID

	AggregateType string
	AggregateID   string

	From time.Time
	To   time.Time

	Topic      string
	Partition  int
	FromOffset int64
	ToOffset   int64
}

// Validate checks the scope shape fail-closed: an ambiguous or empty scope
// never reaches the replay.
func (s ReplayScope) Validate() error {
	switch s.Kind {
	case ReplayScopeEventIDs:
		if len(s.EventIDs) == 0 {
			return contractErrorf("event-ids scope requires at least one event id")
		}
	case ReplayScopeAggregate:
		if s.AggregateType == "" || s.AggregateID == "" {
			return contractErrorf("aggregate scope requires aggregate type and id")
		}
	case ReplayScopeTimeRange:
		if s.From.IsZero() || s.To.IsZero() || s.To.Before(s.From) {
			return contractErrorf("time-range scope requires a valid from/to range")
		}
	case ReplayScopeOffsetRange:
		if s.Topic == "" || s.Partition < 0 || s.ToOffset < s.FromOffset {
			return contractErrorf("offset-range scope requires topic, partition and a valid offset range")
		}
	default:
		return contractErrorf("unknown replay scope %q", s.Kind)
	}
	return nil
}

// auditJSON renders the scope for the event_ops_audit row.
func (s ReplayScope) auditJSON() ([]byte, error) {
	scope := map[string]any{"kind": string(s.Kind)}
	switch s.Kind {
	case ReplayScopeEventIDs:
		ids := make([]string, 0, len(s.EventIDs))
		for _, id := range s.EventIDs {
			ids = append(ids, id.String())
		}
		scope["event_ids"] = ids
	case ReplayScopeAggregate:
		scope["aggregate_type"] = s.AggregateType
		scope["aggregate_id"] = s.AggregateID
	case ReplayScopeTimeRange:
		scope["from"] = s.From.UTC().Format(time.RFC3339Nano)
		scope["to"] = s.To.UTC().Format(time.RFC3339Nano)
	case ReplayScopeOffsetRange:
		scope["topic"] = s.Topic
		scope["partition"] = s.Partition
		scope["from_offset"] = s.FromOffset
		scope["to_offset"] = s.ToOffset
	}
	return json.Marshal(scope)
}

// ReplayOptions bounds one audited replay.
type ReplayOptions struct {
	ConsumerName string
	Scope        ReplayScope
	Reason       string
	Operator     string
	// OperationID is the CLI idempotency key; empty derives a deterministic
	// id from the operation parameters, so a retried command returns the
	// stored result instead of replaying twice (data-model §10).
	OperationID string
	// Topic is the configured event topic, used for outbox-sourced replays.
	Topic string
	// Limit bounds the number of events one operation may replay.
	Limit int
}

// ReplayResult reports one audited replay.
type ReplayResult struct {
	OperationID string
	// Deduplicated reports that the operation id already existed: the stored
	// result is returned and nothing was replayed (CLI retry convergence).
	Deduplicated     bool
	StoredResult     string
	Total            int
	Replayed         int
	SkippedDuplicate int
	VersionSkipped   int
	Quarantined      int
}

// Result renders the audit result summary.
func (r ReplayResult) Result() string {
	return fmt.Sprintf("replayed=%d, skipped_duplicate=%d, version_skipped=%d, quarantined=%d, total=%d",
		r.Replayed, r.SkippedDuplicate, r.VersionSkipped, r.Quarantined, r.Total)
}

// Replay re-delivers historical events through the wired consumer's T4
// transaction (inbox dedup, version guard, effect) and records one
// event_ops_audit row. It never bypasses the persistent idempotency or the
// version guard and never reaches an upstream writer: the only effect is the
// consumer's own effect inside the T4 transaction (FR-14/FR-15; PD-4;
// contracts/consumer.md §6).
func Replay(ctx context.Context, pool *pgxpool.Pool, consumer *Consumer, opts ReplayOptions) (ReplayResult, error) {
	if pool == nil {
		return ReplayResult{}, contractErrorf("replay requires a database pool")
	}
	if consumer == nil {
		return ReplayResult{}, contractErrorf("replay requires the wired consumer")
	}
	if opts.ConsumerName != consumer.Name {
		return ReplayResult{}, contractErrorf("replay consumer %q does not match the wired consumer %q",
			opts.ConsumerName, consumer.Name)
	}
	if strings.TrimSpace(opts.Reason) == "" {
		return ReplayResult{}, contractErrorf("replay requires a reason")
	}
	if strings.TrimSpace(opts.Operator) == "" {
		return ReplayResult{}, contractErrorf("replay requires an operator")
	}
	if err := opts.Scope.Validate(); err != nil {
		return ReplayResult{}, err
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}
	scopeJSON, err := opts.Scope.auditJSON()
	if err != nil {
		return ReplayResult{}, fmt.Errorf("render replay scope: %w", err)
	}
	operationID := opts.OperationID
	if operationID == "" {
		operationID = deriveOperationID("replay", consumer.Name, string(scopeJSON), opts.Reason, opts.Operator)
	}

	// CLI retry convergence: an existing operation id returns the stored
	// result and replays nothing (operation_id UNIQUE; data-model §10).
	if stored, found, err := readAuditResult(ctx, pool, operationID); err != nil {
		return ReplayResult{}, err
	} else if found {
		return ReplayResult{OperationID: operationID, Deduplicated: true, StoredResult: stored}, nil
	}

	candidates, err := resolveReplayCandidates(ctx, pool, consumer.Name, opts, limit)
	if err != nil {
		return ReplayResult{}, err
	}
	result := ReplayResult{OperationID: operationID, Total: len(candidates)}
	for _, candidate := range candidates {
		processResult, perr := consumer.Process(ctx, candidate.Message)
		if perr != nil {
			return result, fmt.Errorf("replay event %s: %w", candidate.Value, perr)
		}
		switch processResult.Outcome {
		case OutcomeApplied:
			result.Replayed++
		case OutcomeDuplicate:
			result.SkippedDuplicate++
		case OutcomeVersionSkip:
			result.VersionSkipped++
		case OutcomeQuarantined:
			result.Quarantined++
		}
		if candidate.QuarantineID > 0 && processResult.Outcome != OutcomeQuarantined {
			// The replayed event left the quarantine: mark the open entry
			// replayed under this operation id (open -> replayed only).
			if err := markQuarantineReplayed(ctx, pool, candidate.QuarantineID, operationID); err != nil {
				return result, err
			}
		}
	}
	if err := writeAuditRow(ctx, pool, auditRow{
		OperationID: operationID,
		OpKind:      "replay",
		Operator:    opts.Operator,
		Scope:       scopeJSON,
		Reason:      opts.Reason,
		Result:      result.Result(),
	}); err != nil {
		if dedup, found, rerr := readAuditResult(ctx, pool, operationID); rerr == nil && found {
			result.Deduplicated = true
			result.StoredResult = dedup
			return result, nil
		}
		return result, err
	}
	if consumer.Observer != nil {
		consumer.Observer.ObserveEventReplay("replay", consumer.Name)
		consumer.Observer.SetConsumerReplayClock(consumer.Name, float64(time.Now().Unix()))
	}
	return result, nil
}

// replayCandidate is one resolved event to replay: the message fed to the
// consumer plus the quarantine entry to close when the replay succeeds.
type replayCandidate struct {
	Message
	QuarantineID int64
}

// resolveReplayCandidates resolves the operator scope into bounded messages.
// Quarantine snapshots are the preferred source (exact bytes); outbox rows
// are the fallback for events that were never quarantined. Outbox-sourced
// replays carry no delivery coordinates, so they never move the partition
// progress.
func resolveReplayCandidates(ctx context.Context, pool *pgxpool.Pool, consumerName string,
	opts ReplayOptions, limit int) ([]replayCandidate, error) {
	store := &QuarantineStore{Pool: pool}
	var candidates []replayCandidate
	seen := map[uuid.UUID]bool{}
	appendQuarantine := func(entries []QuarantineEntry) {
		for _, entry := range entries {
			if len(candidates) >= limit || seen[entry.EventID] {
				continue
			}
			seen[entry.EventID] = true
			candidates = append(candidates, replayCandidate{
				Message: Message{
					Topic:     entry.SourceTopic,
					Partition: entry.SourcePartition,
					Offset:    entry.SourceOffset,
					Value:     entry.EventSnapshot,
				},
				QuarantineID: entry.ID,
			})
		}
	}

	switch opts.Scope.Kind {
	case ReplayScopeEventIDs:
		entries, err := store.listByEventIDs(ctx, consumerName, opts.Scope.EventIDs)
		if err != nil {
			return nil, err
		}
		appendQuarantine(entries)
		for _, id := range opts.Scope.EventIDs {
			if len(candidates) >= limit || seen[id] {
				continue
			}
			msg, err := outboxReplayMessage(ctx, pool, id, opts.Topic)
			if err != nil {
				return nil, err
			}
			if msg == nil {
				continue
			}
			seen[id] = true
			candidates = append(candidates, replayCandidate{Message: *msg})
		}
	case ReplayScopeAggregate:
		entries, err := store.listForAggregate(ctx, consumerName, opts.Scope.AggregateType, opts.Scope.AggregateID)
		if err != nil {
			return nil, err
		}
		appendQuarantine(entries)
		msgs, err := outboxReplayMessagesForAggregate(ctx, pool, opts.Scope.AggregateType, opts.Scope.AggregateID, opts.Topic, limit)
		if err != nil {
			return nil, err
		}
		for _, msg := range msgs {
			if len(candidates) >= limit {
				break
			}
			var id uuid.UUID
			if env, perr := ParseEnvelope(msg.Value); perr == nil {
				id = env.EventID
			}
			if id != uuid.Nil && seen[id] {
				continue
			}
			if id != uuid.Nil {
				seen[id] = true
			}
			candidates = append(candidates, replayCandidate{Message: msg})
		}
	case ReplayScopeTimeRange:
		msgs, err := outboxReplayMessagesForTimeRange(ctx, pool, opts.Scope.From, opts.Scope.To, opts.Topic, limit)
		if err != nil {
			return nil, err
		}
		for _, msg := range msgs {
			candidates = append(candidates, replayCandidate{Message: msg})
		}
	case ReplayScopeOffsetRange:
		entries, err := store.listByOffsetRange(ctx, consumerName, opts.Scope.Topic, opts.Scope.Partition,
			opts.Scope.FromOffset, opts.Scope.ToOffset)
		if err != nil {
			return nil, err
		}
		appendQuarantine(entries)
	default:
		return nil, contractErrorf("unknown replay scope %q", opts.Scope.Kind)
	}
	return candidates, nil
}

// outboxReplayMessage materializes one outbox row as a replay message. It
// returns nil when the row does not exist.
func outboxReplayMessage(ctx context.Context, pool *pgxpool.Pool, eventID uuid.UUID, topic string) (*Message, error) {
	rows, err := pool.Query(ctx, replayOutboxSQL+` WHERE event_id = $1`, eventID)
	if err != nil {
		return nil, ClassifyPGError(err)
	}
	defer rows.Close()
	records, err := scanReplayOutboxRows(rows)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	return replayMessageFromRecord(records[0], topic)
}

// outboxReplayMessagesForAggregate materializes the outbox rows of one
// aggregate, oldest version first.
func outboxReplayMessagesForAggregate(ctx context.Context, pool *pgxpool.Pool, aggregateType, aggregateID, topic string, limit int) ([]Message, error) {
	rows, err := pool.Query(ctx, replayOutboxSQL+`
 WHERE aggregate_type = $1 AND aggregate_id = $2
 ORDER BY aggregate_version, id
 LIMIT $3`, aggregateType, aggregateID, limit)
	if err != nil {
		return nil, ClassifyPGError(err)
	}
	defer rows.Close()
	return scanReplayOutboxMessages(rows, topic)
}

// outboxReplayMessagesForTimeRange materializes the outbox rows created
// inside a time range, oldest first.
func outboxReplayMessagesForTimeRange(ctx context.Context, pool *pgxpool.Pool, from, to time.Time, topic string, limit int) ([]Message, error) {
	rows, err := pool.Query(ctx, replayOutboxSQL+`
 WHERE created_at >= $1 AND created_at <= $2
 ORDER BY created_at, id
 LIMIT $3`, from, to, limit)
	if err != nil {
		return nil, ClassifyPGError(err)
	}
	defer rows.Close()
	return scanReplayOutboxMessages(rows, topic)
}

func scanReplayOutboxMessages(rows pgx.Rows, topic string) ([]Message, error) {
	records, err := scanReplayOutboxRows(rows)
	if err != nil {
		return nil, err
	}
	out := make([]Message, 0, len(records))
	for _, record := range records {
		msg, err := replayMessageFromRecord(record, topic)
		if err != nil {
			return nil, err
		}
		out = append(out, *msg)
	}
	return out, nil
}

// replayMessageFromRecord renders the transport envelope of one outbox row.
// The payload bytes come from the stored JSONB verbatim, so the replay feeds
// the identical event identity and payload bytes (data-model §3.5). The
// message carries no partition coordinates: an outbox-sourced replay never
// moves consumer_progress.
func replayMessageFromRecord(record OutboxRecord, topic string) (*Message, error) {
	body, err := record.EnvelopeJSON()
	if err != nil {
		return nil, err
	}
	return &Message{Topic: topic, Partition: -1, Offset: -1, Value: body}, nil
}

// scanReplayOutboxRows reads outbox rows with the claim column shape (the
// publisher materialization), so the replay envelope reuses EnvelopeJSON.
func scanReplayOutboxRows(rows pgx.Rows) ([]OutboxRecord, error) {
	var out []OutboxRecord
	for rows.Next() {
		rec, err := scanOutboxRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate replay outbox rows: %w", err)
	}
	return out, nil
}

// replayOutboxSQL selects the transport columns of replayable outbox rows.
const replayOutboxSQL = `
SELECT id, event_id, event_type, schema_version, identity_kind,
       aggregate_type, aggregate_id, aggregate_version,
       payload, occurred_at,
       chain_id, block_number, block_hash, tx_hash, log_index,
       recovery_version, revises_event_id
FROM outbox_events`

// markQuarantineReplayed closes one open quarantine entry after a successful
// replay.
func markQuarantineReplayed(ctx context.Context, pool *pgxpool.Pool, id int64, operationID string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return Transient(fmt.Errorf("begin quarantine replay mark: %w", err))
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	store := &QuarantineStore{Pool: pool}
	if _, err := store.MarkReplayed(ctx, tx, id, operationID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return Transient(fmt.Errorf("commit quarantine replay mark: %w", err))
	}
	return nil
}

// UnblockOptions bounds one audited publisher unblock.
type UnblockOptions struct {
	OutboxID    int64
	Reason      string
	Operator    string
	OperationID string
}

// UnblockResult reports one audited unblock.
type UnblockResult struct {
	OperationID  string
	Deduplicated bool
	StoredResult string
	Unblocked    bool
}

// Result renders the audit result summary.
func (r UnblockResult) Result() string {
	return fmt.Sprintf("unblocked=%t", r.Unblocked)
}

// Unblock returns one permanently blocked outbox row to pending under an
// audited operator decision. The blocked -> pending guard is the only allowed
// direction; a non-blocked row updates zero rows (data-model §8; PD-4).
func Unblock(ctx context.Context, pool *pgxpool.Pool, opts UnblockOptions) (UnblockResult, error) {
	if pool == nil {
		return UnblockResult{}, contractErrorf("unblock requires a database pool")
	}
	if opts.OutboxID <= 0 {
		return UnblockResult{}, contractErrorf("unblock requires an outbox id")
	}
	if strings.TrimSpace(opts.Reason) == "" {
		return UnblockResult{}, contractErrorf("unblock requires a reason")
	}
	if strings.TrimSpace(opts.Operator) == "" {
		return UnblockResult{}, contractErrorf("unblock requires an operator")
	}
	operationID := opts.OperationID
	if operationID == "" {
		operationID = deriveOperationID("unblock", fmt.Sprintf("%d", opts.OutboxID), opts.Reason, opts.Operator)
	}
	if stored, found, err := readAuditResult(ctx, pool, operationID); err != nil {
		return UnblockResult{}, err
	} else if found {
		return UnblockResult{OperationID: operationID, Deduplicated: true, StoredResult: stored}, nil
	}

	scope, err := json.Marshal(map[string]any{"outbox_id": opts.OutboxID})
	if err != nil {
		return UnblockResult{}, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return UnblockResult{}, Transient(fmt.Errorf("begin unblock: %w", err))
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	tag, err := tx.Exec(ctx, UnblockSQL, opts.OutboxID)
	if err != nil {
		return UnblockResult{}, ClassifyPGError(err)
	}
	result := UnblockResult{OperationID: operationID, Unblocked: tag.RowsAffected() == 1}
	if err := insertAuditRow(ctx, tx, auditRow{
		OperationID: operationID,
		OpKind:      "unblock",
		Operator:    opts.Operator,
		Scope:       scope,
		Reason:      opts.Reason,
		Result:      result.Result(),
	}); err != nil {
		if errors.Is(err, ErrContract) {
			// Concurrent duplicate operation id: converge on the stored row.
			_ = tx.Rollback(ctx)
			if stored, found, rerr := readAuditResult(ctx, pool, operationID); rerr == nil && found {
				return UnblockResult{OperationID: operationID, Deduplicated: true, StoredResult: stored}, nil
			}
		}
		return UnblockResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return UnblockResult{}, Transient(fmt.Errorf("commit unblock: %w", err))
	}
	return result, nil
}

// PruneOptions bounds one audited retention prune.
type PruneOptions struct {
	Retention   time.Duration
	Reason      string
	Operator    string
	OperationID string
}

// PruneResult reports one audited retention prune.
type PruneResult struct {
	OperationID  string
	Deduplicated bool
	StoredResult string
	Deleted      int64
	Watermark    time.Time
}

// Result renders the audit result summary.
func (r PruneResult) Result() string {
	return fmt.Sprintf("deleted=%d, oldest_published_at=%s", r.Deleted, r.Watermark.UTC().Format(time.RFC3339Nano))
}

// RetentionPrune deletes published rows older than the retention window and
// records one audit row. pending and blocked rows are never deleted or
// overwritten (data-model §5 T7; contracts/outbox-publisher.md §6).
func RetentionPrune(ctx context.Context, pool *pgxpool.Pool, opts PruneOptions) (PruneResult, error) {
	if pool == nil {
		return PruneResult{}, contractErrorf("retention prune requires a database pool")
	}
	if opts.Retention <= 0 {
		return PruneResult{}, contractErrorf("retention prune requires a positive retention window")
	}
	if strings.TrimSpace(opts.Reason) == "" {
		return PruneResult{}, contractErrorf("retention prune requires a reason")
	}
	if strings.TrimSpace(opts.Operator) == "" {
		return PruneResult{}, contractErrorf("retention prune requires an operator")
	}
	operationID := opts.OperationID
	if operationID == "" {
		operationID = deriveOperationID("retention_prune", opts.Retention.String(), opts.Reason, opts.Operator)
	}
	if stored, found, err := readAuditResult(ctx, pool, operationID); err != nil {
		return PruneResult{}, err
	} else if found {
		return PruneResult{OperationID: operationID, Deduplicated: true, StoredResult: stored}, nil
	}

	scope, err := json.Marshal(map[string]any{"retention": opts.Retention.String()})
	if err != nil {
		return PruneResult{}, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return PruneResult{}, Transient(fmt.Errorf("begin retention prune: %w", err))
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var (
		prunable int64
		oldest   time.Time
	)
	if err := tx.QueryRow(ctx, PruneWatermarkSQL, opts.Retention.Seconds()).Scan(&prunable, &oldest); err != nil {
		return PruneResult{}, ClassifyPGError(err)
	}
	tag, err := tx.Exec(ctx, PruneSQL, opts.Retention.Seconds())
	if err != nil {
		return PruneResult{}, ClassifyPGError(err)
	}
	result := PruneResult{OperationID: operationID, Deleted: tag.RowsAffected(), Watermark: oldest}
	if err := insertAuditRow(ctx, tx, auditRow{
		OperationID: operationID,
		OpKind:      "retention_prune",
		Operator:    opts.Operator,
		Scope:       scope,
		Reason:      opts.Reason,
		Result:      result.Result(),
	}); err != nil {
		if errors.Is(err, ErrContract) {
			_ = tx.Rollback(ctx)
			if stored, found, rerr := readAuditResult(ctx, pool, operationID); rerr == nil && found {
				return PruneResult{OperationID: operationID, Deduplicated: true, StoredResult: stored}, nil
			}
		}
		return PruneResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return PruneResult{}, Transient(fmt.Errorf("commit retention prune: %w", err))
	}
	return result, nil
}

// auditRow is one event_ops_audit row (data-model Table 6).
type auditRow struct {
	OperationID string
	OpKind      string
	Operator    string
	Scope       []byte
	Reason      string
	Result      string
}

// insertAuditRow writes one audit row; the operation_id UNIQUE violation is
// classified as ErrContract so callers converge on the stored result.
func insertAuditRow(ctx context.Context, tx pgx.Tx, row auditRow) error {
	_, err := tx.Exec(ctx, `
INSERT INTO event_ops_audit (operation_id, op_kind, operator, scope, reason, result)
VALUES ($1, $2, $3, $4::jsonb, $5, $6)`,
		row.OperationID, row.OpKind, row.Operator, string(row.Scope), row.Reason, row.Result)
	if err != nil {
		return ClassifyPGError(err)
	}
	return nil
}

// writeAuditRow writes one audit row in its own transaction.
func writeAuditRow(ctx context.Context, pool *pgxpool.Pool, row auditRow) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return Transient(fmt.Errorf("begin audit write: %w", err))
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := insertAuditRow(ctx, tx, row); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return Transient(fmt.Errorf("commit audit write: %w", err))
	}
	return nil
}

// readAuditResult reads the stored result of one operation id (CLI retry
// convergence).
func readAuditResult(ctx context.Context, pool *pgxpool.Pool, operationID string) (string, bool, error) {
	var result string
	err := pool.QueryRow(ctx,
		`SELECT result FROM event_ops_audit WHERE operation_id = $1`, operationID).Scan(&result)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, ClassifyPGError(err)
	}
	return result, true, nil
}

// deriveOperationID builds the deterministic CLI idempotency key of an
// operation from its parameters: a retried command converges on the stored
// audit row instead of executing twice.
func deriveOperationID(kind string, parts ...string) string {
	h := sha256.New()
	_, _ = io.WriteString(h, kind)
	for _, part := range parts {
		_, _ = io.WriteString(h, "|")
		_, _ = io.WriteString(h, part)
	}
	return kind + "-" + hex.EncodeToString(h.Sum(nil))[:32]
}
