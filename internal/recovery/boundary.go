// boundary.go implements T052 [US5]: the FR-027–FR-029 event/downstream
// boundary report. It is the report layer of the event boundary, complementary
// to the V5/V6 verification adapters of internal/recovery/sources (T040): the
// adapters observe the verification objects for the T038 batch, and this file
// assembles the boundary report an operator (or an app-level test) can read
// without persisting a verification item.
//
// What the report does:
//
//   - Rollback detection (FR-027): the durable consumer_progress next_offset is
//     compared with the broker committed offset when a read-only broker reader
//     is wired. A broker position ahead of the restored PG progress is a
//     rollback: the local consumption state was regressed while the already
//     consumed external effects remain external facts (FR-015). An unreadable
//     broker keeps the comparison unprovable and the conclusion unknown; it is
//     never defaulted to zero and never treated as equality.
//   - Duplicate absorption (FR-027): repeated deliveries are absorbed by the
//     existing consumer_inbox/consumer_versions idempotency inside the T4
//     transaction (internal/events). This file reads the idempotency history
//     only: it never inserts an inbox row, never advances an offset, never
//     replays and never quarantines.
//   - Missing historical idempotency records (FR-028): a positive PG progress
//     with no readable consumer_inbox/consumer_versions history is unknown, is
//     never silently backfilled as "processed", and never makes the range
//     re-executable. Such a range is reported as a possible source of external
//     duplicate effects because a redelivery or an authorized replay of an
//     already-effect-ed event can no longer be absorbed by the inbox.
//   - Downstream dedup identity (FR-028): the stable identity is `event_id`
//     plus the delivery source triple (topic, partition, offset); the durable
//     inbox key stays (consumer_name, event_id) and the source triple is
//     recorded on the inbox row for detection/reporting. This project supplies
//     the identity and the detection/reporting boundary; it never guarantees
//     any external consumer's dedup.
//   - External scope (FR-028/029): the report never claims cross-system
//     exactly-once (delivery is at-least-once and only the PG transaction is
//     idempotent). Without real upstream/downstream receipts connected, the
//     declared verifiable scope is limited to this project's surface (status,
//     cursors, inbox/version records, the offset relation and chain facts);
//     "the external ledger is consistent" or "the external effects were
//     restored" is never declared.
//
// Read-only discipline: every database statement in this file is a SELECT, and
// no method calls an effect path. Re-processing exists only through the
// existing authorized quarantine-replay/events-admin entry points
// (internal/events/quarantine.go), which this file never invokes. The T038
// orchestrator remains the only writer of verification items, gaps and audit
// rows.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xtianxx/txharbor/internal/logx"
)

// ErrEventBoundary marks a malformed event-boundary request (a missing
// read-only data surface). It is a contract error: the report is never
// assembled from defaults or from an absent source.
var ErrEventBoundary = errors.New("event boundary request is invalid")

// ---------------------------------------------------------------------------
// Dedup identity (FR-028)
// ---------------------------------------------------------------------------

// The durable event-identity/source columns the boundary report is anchored to.
// The names match migration 000015: consumer_inbox(event_id) plus the delivery
// source triple consumer_inbox(topic, partition, "offset"). Values are frozen
// vocabulary of contracts/verification-items.md §3.
const (
	// EventBoundaryEventIDColumn is the stable event identity column.
	EventBoundaryEventIDColumn = "event_id"
	// EventBoundarySourceTopicColumn is the delivery source topic.
	EventBoundarySourceTopicColumn = "topic"
	// EventBoundarySourcePartitionColumn is the delivery source partition.
	EventBoundarySourcePartitionColumn = "partition"
	// EventBoundarySourceOffsetColumn is the delivery source offset (a reserved
	// word in PostgreSQL, always quoted).
	EventBoundarySourceOffsetColumn = "offset"
)

// EventDedupIdentity is the downstream dedup identity contract of FR-028: the
// stable identity an external downstream needs to dedup this project's
// deliveries. It is a description, not a guarantee: the project never claims
// that any external consumer implements it.
type EventDedupIdentity struct {
	EventIDColumn         string
	SourceTopicColumn     string
	SourcePartitionColumn string
	SourceOffsetColumn    string
	// Statement is the human-readable identity statement.
	Statement string
}

// DownstreamDedupIdentity returns the frozen FR-028 identity: event_id plus
// the source triple (topic, partition, offset). The caller receives a value
// copy and may mutate it freely.
func DownstreamDedupIdentity() EventDedupIdentity {
	return EventDedupIdentity{
		EventIDColumn:         EventBoundaryEventIDColumn,
		SourceTopicColumn:     EventBoundarySourceTopicColumn,
		SourcePartitionColumn: EventBoundarySourcePartitionColumn,
		SourceOffsetColumn:    EventBoundarySourceOffsetColumn,
		Statement: "downstream dedup identity = event_id + source triple (topic, partition, offset). " +
			"The durable inbox key remains (consumer_name, event_id) and the delivery source triple is recorded on " +
			"the inbox row for detection/reporting. This project supplies the identity and the detection/reporting " +
			"boundary; it does not guarantee any external consumer's dedup (FR-028).",
	}
}

// ---------------------------------------------------------------------------
// Closed offset-relation vocabulary
// ---------------------------------------------------------------------------

// EventOffsetRelation is the closed relation between the restored PG
// consumer_progress next_offset and the broker committed offset.
type EventOffsetRelation string

const (
	// EventOffsetEqual: the restored PG progress equals the broker committed
	// offset. That alone is not a pass: the idempotency history must exist too.
	EventOffsetEqual EventOffsetRelation = "equal"
	// EventOffsetBrokerAhead: the broker position leads the restored PG
	// progress — the rollback regressed the local consumption state. The
	// consumed external effects remain external facts (FR-015).
	EventOffsetBrokerAhead EventOffsetRelation = "broker_ahead"
	// EventOffsetPGAhead: the restored PG progress leads the broker committed
	// offset — a broker commit may be pending and the external consumption
	// state cannot be proven.
	EventOffsetPGAhead EventOffsetRelation = "pg_ahead"
	// EventOffsetUnprovable: no readable broker surface; the comparison cannot
	// be performed and is never defaulted to equality or zero.
	EventOffsetUnprovable EventOffsetRelation = "unprovable"
)

// knownEventOffsetRelations is the canonical display order.
var knownEventOffsetRelations = []EventOffsetRelation{
	EventOffsetEqual, EventOffsetBrokerAhead, EventOffsetPGAhead, EventOffsetUnprovable,
}

// Known reports whether r is one of the four closed relations.
func (r EventOffsetRelation) Known() bool {
	for _, known := range knownEventOffsetRelations {
		if r == known {
			return true
		}
	}
	return false
}

// KnownEventOffsetRelations returns a fresh copy of the closed set.
func KnownEventOffsetRelations() []EventOffsetRelation {
	return append([]EventOffsetRelation(nil), knownEventOffsetRelations...)
}

// ---------------------------------------------------------------------------
// Boundary statements (FR-028/029)
// ---------------------------------------------------------------------------

// EventBoundaryExactlyOnceStatement is the guarantee statement every report
// carries. Cross-system exactly-once is never claimed: Kafka and PostgreSQL
// share no transaction and delivery is at-least-once.
const EventBoundaryExactlyOnceStatement = "No cross-system exactly-once is claimed: delivery is at-least-once and only the " +
	"consumer's PostgreSQL transaction (inbox, version guard, effect, progress) is idempotent; Kafka and PostgreSQL " +
	"share no transaction (FR-028)."

// EventBoundaryScopeWithoutReceipts is the declared verifiable scope when no
// real upstream/downstream receipts are connected: this project's own surface
// only, never an external ledger conclusion (FR-029).
const EventBoundaryScopeWithoutReceipts = "Only this project's verifiable range is declared: PostgreSQL status and " +
	"cursors, the consumer_inbox/consumer_versions idempotency records, the consumer_progress/broker committed-offset " +
	"relation and chain facts. No external ledger consistency and no \"external effects restored\" conclusion is " +
	"declared (FR-029)."

// EventBoundaryScopeWithReceipts is the declared scope when the deployment
// declares real upstream/downstream receipts connected: external receipt
// evidence is evaluated outside this report, and the report still declares no
// cross-system exactly-once and no external-ledger consistency (FR-028/029).
const EventBoundaryScopeWithReceipts = "Real upstream/downstream receipts are declared connected by the deployment; " +
	"external receipt evidence is evaluated outside this report. This report still declares no cross-system " +
	"exactly-once and no external-ledger consistency (FR-028/029)."

// EventBoundaryBoundaryStatement describes the detection/disposition boundary
// of this report verbatim. It is carried on every report.
const EventBoundaryBoundaryStatement = "The event boundary report is read-only: it detects the PG-progress/broker-offset " +
	"relation, reports missing idempotency history and possible external duplicate effects, and never applies, " +
	"backfills, re-delivers or quarantines anything. Re-processing exists only through the existing authorized " +
	"quarantine-replay/events-admin entry points (FR-027/028)."

// ---------------------------------------------------------------------------
// Read-only inputs and pure assessment
// ---------------------------------------------------------------------------

// EventBoundaryBrokerOffsets is the read-only broker surface of one consumer
// group: the committed offset (the next offset to consume) of one
// topic/partition. A nil reader keeps the offset relation unprovable (unknown),
// never a default zero.
type EventBoundaryBrokerOffsets interface {
	CommittedOffset(ctx context.Context, topic string, partition int) (int64, error)
}

// EventBoundaryQuerier is the narrow read-only database surface the inspector
// uses. *pgxpool.Pool satisfies it. It exposes no transaction, no Exec and no
// write path: the report cannot write even by mistake.
type EventBoundaryQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// EventBoundaryFacts is one consumer/partition's read-only fact set. Known
// flags distinguish "read and empty" from "not readable at the restore point":
// a missing relation is unknown, never an empty history (FR-016/028).
type EventBoundaryFacts struct {
	ConsumerName string
	Topic        string
	Partition    int32
	NextOffset   int64
	UpdatedAt    time.Time

	InboxRows      int64
	InboxMaxOffset int64
	InboxKnown     bool

	VersionRows  int64
	VersionKnown bool

	OpenQuarantineRows int64
	QuarantineKnown    bool
}

// EventBoundaryBrokerFact is the broker side of one comparison. Readable=false
// keeps the relation unprovable; ReadErr is the (redacted) reason recorded on
// the check.
type EventBoundaryBrokerFact struct {
	Readable        bool
	CommittedOffset int64
	ReadErr         error
}

// EventBoundaryCheck is one consumer/partition's boundary verdict.
type EventBoundaryCheck struct {
	ConsumerName string
	Topic        string
	Partition    int32

	Conclusion VerificationConclusion
	Reason     string

	OffsetRelation EventOffsetRelation
	// RollbackDetected: the broker position leads the restored PG progress
	// (EventOffsetBrokerAhead). The consumed external effects remain facts.
	RollbackDetected bool

	PGNextOffset          int64
	PGProgressUpdatedAt   time.Time
	BrokerCommittedOffset int64
	BrokerReadable        bool

	InboxRows           int64
	InboxHistoryKnown   bool
	VersionRows         int64
	VersionHistoryKnown bool
	OpenQuarantineRows  int64

	// MissingIdempotencyHistory: the restored progress is positive but no
	// readable inbox/version history exists for the consumer. The absence is
	// never backfilled as "processed" and never makes the range re-executable
	// (FR-028).
	MissingIdempotencyHistory bool
	// PossibleExternalDuplicateEffects: a redelivery or an authorized replay of
	// an already-effect-ed event can no longer be absorbed by the missing
	// idempotency record, or the broker position proves consumption beyond the
	// restored local state. Reported, never resolved here (FR-027).
	PossibleExternalDuplicateEffects bool
}

// ObjectKey returns the stable object identity of one check.
func (c EventBoundaryCheck) ObjectKey() string {
	return fmt.Sprintf("consumer=%s&topic=%s&partition=%d", c.ConsumerName, c.Topic, c.Partition)
}

// AssessEventBoundaryCheck is the pure FR-027/028 decision over one
// consumer/partition fact set and one broker fact. It reads no clock, store or
// network. The rules are fail-closed: only equality plus a readable, non-empty
// idempotency history (and no open quarantine) is consistent; every other
// combination is divergent or unknown.
func AssessEventBoundaryCheck(facts EventBoundaryFacts, broker EventBoundaryBrokerFact) EventBoundaryCheck {
	check := EventBoundaryCheck{
		ConsumerName:          facts.ConsumerName,
		Topic:                 facts.Topic,
		Partition:             facts.Partition,
		PGNextOffset:          facts.NextOffset,
		PGProgressUpdatedAt:   facts.UpdatedAt.UTC(),
		InboxRows:             facts.InboxRows,
		InboxHistoryKnown:     facts.InboxKnown,
		VersionRows:           facts.VersionRows,
		VersionHistoryKnown:   facts.VersionKnown,
		OpenQuarantineRows:    facts.OpenQuarantineRows,
		BrokerReadable:        broker.Readable,
		BrokerCommittedOffset: broker.CommittedOffset,
	}
	if facts.ConsumerName == "" || facts.Topic == "" || facts.Partition < 0 {
		check.Conclusion = ConclusionUnknown
		check.OffsetRelation = EventOffsetUnprovable
		check.Reason = "the boundary facts are incomplete: a consumer name, a topic and a non-negative partition are " +
			"required; nothing is inferred from a missing identity"
		return check
	}

	// The idempotency-history verdict is independent of the broker surface: a
	// positive local progress with no readable inbox/version history is a
	// missing history fact even when the offset relation is unprovable.
	historyMissing := facts.NextOffset > 0 &&
		(!facts.InboxKnown || !facts.VersionKnown || (facts.InboxRows == 0 && facts.VersionRows == 0))
	check.MissingIdempotencyHistory = historyMissing
	if historyMissing {
		check.PossibleExternalDuplicateEffects = true
	}

	switch {
	case !broker.Readable:
		check.OffsetRelation = EventOffsetUnprovable
		check.Conclusion = ConclusionUnknown
		reason := "no broker committed-offset reader is configured"
		if broker.ReadErr != nil {
			reason = logx.Redact(broker.ReadErr.Error())
		}
		check.Reason = fmt.Sprintf("the broker committed offset of %s/%s/%d is unreadable (%s); the PG progress cannot "+
			"be compared with the broker, a rolled-back offset is not detectable and the external consumption state "+
			"stays unknown (FR-027/028)", facts.ConsumerName, facts.Topic, facts.Partition, reason)
		if historyMissing {
			check.Reason += "; the missing consumer_inbox/consumer_versions history is never backfilled as processed " +
				"and a redelivery could duplicate an external effect (FR-028)"
		}
	case broker.CommittedOffset > facts.NextOffset:
		check.OffsetRelation = EventOffsetBrokerAhead
		check.RollbackDetected = true
		check.PossibleExternalDuplicateEffects = true
		check.Conclusion = ConclusionDivergent
		check.Reason = fmt.Sprintf("the broker committed offset %d is ahead of the restored consumer_progress "+
			"next_offset %d by %d; the database rollback regressed the local consumption progress while the already "+
			"consumed external effects remain external facts. The advanced range is not re-consumed and a redelivery "+
			"or authorized replay could duplicate an external effect (FR-015/027/028)",
			broker.CommittedOffset, facts.NextOffset, broker.CommittedOffset-facts.NextOffset)
		if historyMissing {
			check.Reason += "; the consumer_inbox/consumer_versions history of the consumer is missing and is never " +
				"backfilled as processed (FR-028)"
		}
	case broker.CommittedOffset < facts.NextOffset:
		check.OffsetRelation = EventOffsetPGAhead
		check.Conclusion = ConclusionUnknown
		check.Reason = fmt.Sprintf("the restored PG progress %d leads the broker committed offset %d by %d; the "+
			"external consumption state cannot be proven (a broker commit may be pending) and re-delivery must be "+
			"absorbed by the inbox, never by re-running effects (FR-027)", facts.NextOffset, broker.CommittedOffset,
			facts.NextOffset-broker.CommittedOffset)
		if historyMissing {
			check.Reason += "; the consumer_inbox/consumer_versions history of the consumer is missing and is never " +
				"backfilled as processed (FR-028)"
		}
	default:
		check.OffsetRelation = EventOffsetEqual
		switch {
		case !facts.InboxKnown || !facts.VersionKnown:
			check.Conclusion = ConclusionUnknown
			check.Reason = fmt.Sprintf("the broker committed offset equals the restored next_offset %d, but the "+
				"consumer_inbox/consumer_versions relations are not migrated at the restore point; the idempotency "+
				"history is unprovable and is never backfilled as processed (FR-028)", facts.NextOffset)
		case historyMissing:
			check.Conclusion = ConclusionUnknown
			check.Reason = fmt.Sprintf("the broker committed offset equals the restored next_offset %d, but the "+
				"restore point carries no consumer_inbox/consumer_versions history for the consumer; missing "+
				"historical idempotency records are never silently written back as processed and never make the range "+
				"re-executable (FR-028); a redelivery could duplicate an external effect (FR-027)", facts.NextOffset)
		case !facts.QuarantineKnown:
			check.Conclusion = ConclusionUnknown
			check.Reason = fmt.Sprintf("the consumer_quarantine relation is not migrated at the restore point; the "+
				"quarantine state of consumer %s is unprovable and a missing relation is never an empty quarantine "+
				"(FR-016)", facts.ConsumerName)
		case facts.OpenQuarantineRows > 0:
			check.Conclusion = ConclusionUnknown
			check.Reason = fmt.Sprintf("consumer %s carries %d open quarantine entr(ies); their effects were not "+
				"applied and remain unresolved. Re-processing exists only through the authorized quarantine-replay "+
				"path and is never triggered by this report (FR-027)", facts.ConsumerName, facts.OpenQuarantineRows)
		default:
			check.Conclusion = ConclusionConsistent
			check.Reason = fmt.Sprintf("the broker committed offset equals the restored next_offset %d and the "+
				"consumer_inbox/consumer_versions history exists; repeated deliveries are absorbed by the existing "+
				"idempotency and never re-run an effect (FR-027)", facts.NextOffset)
		}
	}
	return check
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

// EventBoundaryReport is the assembled FR-027–FR-029 boundary report. It is a
// read-only observation: it carries no "safe to reprocess" verdict and no
// replay/reprocess entry point.
type EventBoundaryReport struct {
	CheckedAt  time.Time
	Conclusion VerificationConclusion
	Checks     []EventBoundaryCheck

	// RollbackDetected: at least one broker position leads the restored PG
	// progress (the rollback regression is visible).
	RollbackDetected bool
	// MissingIdempotencyHistory lists the object keys whose restored progress
	// has no readable inbox/version history.
	MissingIdempotencyHistory []string
	// PossibleExternalDuplicateEffects lists the object keys (with reason) where
	// a redelivery or authorized replay could duplicate an external effect.
	PossibleExternalDuplicateEffects []string

	// DedupIdentity is the FR-028 downstream dedup identity.
	DedupIdentity EventDedupIdentity
	// CrossSystemExactlyOnce is always false: the report never claims it.
	CrossSystemExactlyOnce bool
	// ExactlyOnceStatement is the guarantee statement carried verbatim.
	ExactlyOnceStatement string
	// ExternalReceiptsConnected records the deployment declaration the scope
	// statement was chosen from.
	ExternalReceiptsConnected bool
	// VerifiableScope is the declared scope of this report.
	VerifiableScope string
	// BoundaryStatement is the detection/disposition boundary carried verbatim.
	BoundaryStatement string
}

// Passes reports whether every check is consistent. Only a fully consistent
// report passes; divergent/unknown/stale is never displayed as normal
// (FR-018/028).
func (r EventBoundaryReport) Passes() bool { return r.Conclusion.Passes() }

// BuildEventBoundaryReport assembles the final report from the assessed
// checks. An empty check set is unknown: nothing was proven. Any divergent
// check makes the report divergent; otherwise any non-consistent check makes it
// unknown.
func BuildEventBoundaryReport(checkedAt time.Time, checks []EventBoundaryCheck, externalReceiptsConnected bool) EventBoundaryReport {
	report := EventBoundaryReport{
		CheckedAt:                 checkedAt.UTC(),
		Checks:                    append([]EventBoundaryCheck(nil), checks...),
		DedupIdentity:             DownstreamDedupIdentity(),
		CrossSystemExactlyOnce:    false,
		ExactlyOnceStatement:      EventBoundaryExactlyOnceStatement,
		ExternalReceiptsConnected: externalReceiptsConnected,
		BoundaryStatement:         EventBoundaryBoundaryStatement,
	}
	if externalReceiptsConnected {
		report.VerifiableScope = EventBoundaryScopeWithReceipts
	} else {
		report.VerifiableScope = EventBoundaryScopeWithoutReceipts
	}
	report.Conclusion = ConclusionUnknown
	if len(checks) > 0 {
		report.Conclusion = ConclusionConsistent
		for _, check := range checks {
			if check.Conclusion == ConclusionDivergent {
				report.Conclusion = ConclusionDivergent
				break
			}
			if check.Conclusion != ConclusionConsistent {
				report.Conclusion = ConclusionUnknown
			}
		}
	}
	for _, check := range checks {
		if check.RollbackDetected {
			report.RollbackDetected = true
		}
		if check.MissingIdempotencyHistory {
			report.MissingIdempotencyHistory = append(report.MissingIdempotencyHistory, check.ObjectKey())
		}
		if check.PossibleExternalDuplicateEffects {
			report.PossibleExternalDuplicateEffects = append(report.PossibleExternalDuplicateEffects,
				check.ObjectKey()+": "+check.Reason)
		}
	}
	return report
}

// ---------------------------------------------------------------------------
// Inspector
// ---------------------------------------------------------------------------

// EventBoundaryOptions assembles an EventBoundaryInspector. Data is required;
// Broker is optional (nil keeps every offset relation unprovable -> unknown).
type EventBoundaryOptions struct {
	Data   EventBoundaryQuerier
	Broker EventBoundaryBrokerOffsets
	// ExternalReceiptsConnected declares whether real upstream/downstream
	// receipts are actually connected. It only selects the declared scope
	// statement; the report never claims cross-system exactly-once either way.
	ExternalReceiptsConnected bool
	// Now is a test seam for the report timestamp (nil means time.Now).
	Now func() time.Time
}

// EventBoundaryInspector reads the durable consumer state and assembles the
// boundary report. Every read is a SELECT.
type EventBoundaryInspector struct {
	data             EventBoundaryQuerier
	broker           EventBoundaryBrokerOffsets
	externalReceipts bool
	now              func() time.Time
}

// NewEventBoundaryInspector builds the inspector. A missing data surface
// refuses: there is no best-effort assembly in which an absent consumer state
// silently produces a pass.
func NewEventBoundaryInspector(opts EventBoundaryOptions) (*EventBoundaryInspector, error) {
	if opts.Data == nil {
		return nil, fmt.Errorf("%w: the event boundary inspector requires the read-only data surface", ErrEventBoundary)
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &EventBoundaryInspector{
		data:             opts.Data,
		broker:           opts.Broker,
		externalReceipts: opts.ExternalReceiptsConnected,
		now:              now,
	}, nil
}

// Boundary read-only SQL. Only SELECT statements exist in this file.
const (
	eventBoundaryProgressSQL = `
SELECT consumer_name, topic, partition, next_offset, updated_at
FROM consumer_progress
ORDER BY consumer_name, topic, partition`

	eventBoundaryInboxSQL = `
SELECT count(*), COALESCE(max("offset"), -1)
FROM consumer_inbox
WHERE consumer_name = $1 AND topic = $2 AND partition = $3`

	eventBoundaryVersionsSQL = `
SELECT count(*) FROM consumer_versions WHERE consumer_name = $1`

	eventBoundaryQuarantineSQL = `
SELECT count(*) FROM consumer_quarantine WHERE consumer_name = $1 AND status = 'open'`
)

// eventBoundaryRelationMissing reports PostgreSQL's undefined_table (42P01): a
// relation that does not exist at the restore point is "not migrated" and is
// recorded as unknown, never as an empty result.
func eventBoundaryRelationMissing(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

// Report reads every durable consumer progress row with its idempotency state
// and assembles the report. A missing consumer_progress relation (or no row at
// all) is one scope-level unknown check: absence is never evidence that nothing
// was ever consumed and an offset regression cannot be detected without a
// durable progress row (FR-027/028).
func (i *EventBoundaryInspector) Report(ctx context.Context) (EventBoundaryReport, error) {
	checkedAt := i.now()
	rows, err := i.data.Query(ctx, eventBoundaryProgressSQL)
	if err != nil {
		if eventBoundaryRelationMissing(err) {
			return BuildEventBoundaryReport(checkedAt, []EventBoundaryCheck{missingProgressCheck(
				"the consumer_progress relation does not exist at the restore point (000015 not migrated); the " +
					"consumer progress and any offset regression are unprovable")}, i.externalReceipts), nil
		}
		return EventBoundaryReport{}, fmt.Errorf("read consumer progress: %w", err)
	}
	defer rows.Close()

	var checks []EventBoundaryCheck
	for rows.Next() {
		var (
			consumerName string
			topic        string
			partition    int32
			nextOffset   int64
			updatedAt    time.Time
		)
		if err := rows.Scan(&consumerName, &topic, &partition, &nextOffset, &updatedAt); err != nil {
			return EventBoundaryReport{}, fmt.Errorf("scan consumer progress: %w", err)
		}
		facts, err := i.progressFacts(ctx, consumerName, topic, partition, nextOffset, updatedAt)
		if err != nil {
			return EventBoundaryReport{}, err
		}
		brokerFact := i.brokerFact(ctx, topic, partition)
		checks = append(checks, AssessEventBoundaryCheck(facts, brokerFact))
	}
	if err := rows.Err(); err != nil {
		return EventBoundaryReport{}, fmt.Errorf("read consumer progress: %w", err)
	}
	if len(checks) == 0 {
		checks = append(checks, missingProgressCheck("no consumer_progress row exists at the restore point; absence "+
			"is not evidence that nothing was ever consumed and an offset regression cannot be detected without a "+
			"durable progress row (FR-027/028)"))
	}
	return BuildEventBoundaryReport(checkedAt, checks, i.externalReceipts), nil
}

// missingProgressCheck is the conservative scope-level unknown used when no
// per-partition progress row can be read at all.
func missingProgressCheck(reason string) EventBoundaryCheck {
	return EventBoundaryCheck{
		Conclusion:     ConclusionUnknown,
		OffsetRelation: EventOffsetUnprovable,
		Reason:         reason,
	}
}

// progressFacts reads the idempotency state of one consumer/partition. A
// missing relation is recorded as unknown (Known=false), never as an empty
// history.
func (i *EventBoundaryInspector) progressFacts(ctx context.Context, consumerName, topic string,
	partition int32, nextOffset int64, updatedAt time.Time) (EventBoundaryFacts, error) {
	facts := EventBoundaryFacts{
		ConsumerName: consumerName,
		Topic:        topic,
		Partition:    partition,
		NextOffset:   nextOffset,
		UpdatedAt:    updatedAt,
	}
	if err := i.data.QueryRow(ctx, eventBoundaryInboxSQL, consumerName, topic, partition).
		Scan(&facts.InboxRows, &facts.InboxMaxOffset); err != nil {
		if !eventBoundaryRelationMissing(err) {
			return EventBoundaryFacts{}, fmt.Errorf("read consumer inbox %s/%s/%d: %w", consumerName, topic, partition, err)
		}
	} else {
		facts.InboxKnown = true
	}
	if err := i.data.QueryRow(ctx, eventBoundaryVersionsSQL, consumerName).Scan(&facts.VersionRows); err != nil {
		if !eventBoundaryRelationMissing(err) {
			return EventBoundaryFacts{}, fmt.Errorf("read consumer versions %s: %w", consumerName, err)
		}
	} else {
		facts.VersionKnown = true
	}
	if err := i.data.QueryRow(ctx, eventBoundaryQuarantineSQL, consumerName).Scan(&facts.OpenQuarantineRows); err != nil {
		if !eventBoundaryRelationMissing(err) {
			return EventBoundaryFacts{}, fmt.Errorf("read consumer quarantine %s: %w", consumerName, err)
		}
	} else {
		facts.QuarantineKnown = true
	}
	return facts, nil
}

// brokerFact reads the broker committed offset when a reader is wired; a
// missing/unreachable reader keeps the relation unprovable.
func (i *EventBoundaryInspector) brokerFact(ctx context.Context, topic string, partition int32) EventBoundaryBrokerFact {
	if i.broker == nil {
		return EventBoundaryBrokerFact{Readable: false, ReadErr: errors.New("no broker committed-offset reader is configured")}
	}
	committed, err := i.broker.CommittedOffset(ctx, topic, int(partition))
	if err != nil {
		return EventBoundaryBrokerFact{Readable: false, ReadErr: err}
	}
	return EventBoundaryBrokerFact{Readable: true, CommittedOffset: committed}
}
