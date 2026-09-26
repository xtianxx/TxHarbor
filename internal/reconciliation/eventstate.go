// eventstate.go implements T015: the read-only event-delivery adapter of the
// 014 three-way comparison (spec.md FR-002/004/005/006/009, Q4; data-model.md
// §1.1 upstream_receipt_source, §2 stable identity/evidence hash, §4 evidence
// bundle; research §1 "Outbox/消费者", §2 "事件侧").
//
// The adapter answers one question honestly: "what did the event pipeline
// record for this scan interval, and is that evidence sufficient to draw a
// conclusion?" It reads, and only reads:
//
//   - outbox_events rows in scope: publish state, business/chain identity,
//     payload hash, source watermark and emission time. No mutation SQL from
//     outbox.go is ever executed;
//   - consumer results for those events: the durable progress high-water
//     marks (consumer.go:887 ReadProgress and consumer.go:914 ResumeOffset)
//     plus consumer_inbox / consumer_versions rows shaped after the T4
//     transaction (consumer.go:569-637);
//   - open quarantine entries through the existing reads
//     (quarantine.go ListOpen/OpenCount); replay/unblock stay operator paths
//     and are never invoked here;
//   - the optional ReconciliationAudit (audit.go:133 NewReconciliationAudit /
//     audit.go:158 Run) so the source-watermark missing/behind comparison can
//     be included without reimplementing it.
//
// Hard boundaries encoded here:
//
//   - Read-only by construction: this file contains SELECTs only and opens no
//     database transaction at all. Slow reads therefore never hold a
//     transaction or a lock (data-model.md §5.1; FR-014/023).
//   - No DB read failure is silently turned into "no signal": every failed
//     read is recorded in a bounded reason and downgrades the bundle to
//     incomplete/unknown (FR-004); failures are returned as an error alongside
//     the partial bundle, exactly like T013's Observe.
//   - Q4: a legally absorbed duplicate (the version guard ignoring an
//     equal/older version, or an inbox-idempotent redelivery) is metrics-only
//     evidence. It never becomes a ticket and never feeds a fund-anomaly
//     claim; evidence that cannot prove absorption stays unproven/pending.
//   - FR-006: an unconnected or unconfigured upstream receipt source keeps
//     the bundle incomplete. A `connected` declaration never proves upstream
//     success (ExternalLedgerProvable always reports false).
//   - One physical source is one evidence item: outbox rows are keyed by
//     event_id, and applications/watermarks/quarantine signals are folded
//     into that single observation (never wrapped as several evidence items);
//     EventEvidenceSet additionally dedups provenance refs.
//   - Authorization is NOT decided here. Callers must have passed the authz.go
//     evaluation before calling Observe; this file self-grants nothing.
//
// Integration point for T016/T017: EventStateEvidence exposes per-observation
// PartyObservation/DuplicateEvidence conversions and an absence marker
// (EventAbsenceSnapshot), so the compare loop can assemble classify.Observation
// without re-deriving event facts.
package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/events"
)

// eventStateCanonicalVersion freezes the scope-level event-party snapshot
// canonicalization domain. Changing it changes every content hash and is a
// dedup-breaking change.
const eventStateCanonicalVersion = "txharbor.reconciliation.eventstate.v1"

// eventObservationCanonicalVersion freezes the per-event canonicalization
// domain used for PartyObservation.Content.
const eventObservationCanonicalVersion = "txharbor.reconciliation.eventobservation.v1"

// eventAbsenceCanonicalVersion freezes the definitive-absence marker domain.
// An absence is canonicalized as its own non-empty marker so a truly absent
// event party can still contribute to a snapshot hash (classify.go requires
// all three parties' content); an empty byte slice would only downgrade the
// observation to incomplete.
const eventAbsenceCanonicalVersion = "txharbor.reconciliation.eventabsence.v1"

// eventStateWatermarkChunk bounds how many (aggregate_type, aggregate_id)
// tuples one consumer_versions read may carry, so a bounded scan never builds
// an unbounded parameter list.
const eventStateWatermarkChunk = 200

// EventDeliverySource is the closed provenance vocabulary of event evidence.
// A source identifies one physical read path; the same path re-read (or one
// read wrapped twice) is the same source, never two independent evidence
// items.
type EventDeliverySource string

const (
	// EventSourceOutbox is the outbox_events row read (publish state, event
	// identity, source watermark).
	EventSourceOutbox EventDeliverySource = "outbox_events"
	// EventSourceInbox is the consumer_inbox idempotency/effect record read.
	EventSourceInbox EventDeliverySource = "consumer_inbox"
	// EventSourceVersions is the consumer_versions version-guard watermark read.
	EventSourceVersions EventDeliverySource = "consumer_versions"
	// EventSourceProgress is the consumer_progress durable offset read
	// (consumer.go:887/914).
	EventSourceProgress EventDeliverySource = "consumer_progress"
	// EventSourceQuarantine is the open consumer_quarantine read.
	EventSourceQuarantine EventDeliverySource = "consumer_quarantine"
	// EventSourceAudit is the optional ReconciliationAudit read
	// (audit.go:133/158).
	EventSourceAudit EventDeliverySource = "outbox_audit"
	// EventSourceBacklog is the global capacity/blocked backlog read
	// (outbox.go CapacitySQL/BlockedCountSQL).
	EventSourceBacklog EventDeliverySource = "outbox_capacity"
)

// Valid reports whether s is part of the closed source vocabulary.
func (s EventDeliverySource) Valid() bool {
	switch s {
	case EventSourceOutbox, EventSourceInbox, EventSourceVersions, EventSourceProgress,
		EventSourceQuarantine, EventSourceAudit, EventSourceBacklog:
		return true
	}
	return false
}

// EventEvidenceRef is one provenance handle: the physical source plus the
// stable identity of the underlying fact. A ref is self-delimiting and carries
// no secret material.
type EventEvidenceRef struct {
	Source EventDeliverySource `json:"source"`
	Ref    string              `json:"ref"`
}

// Validate checks the ref shape conservatively.
func (r EventEvidenceRef) Validate() error {
	if !r.Source.Valid() {
		return contractErrorf("unknown event evidence source %q", r.Source)
	}
	if strings.TrimSpace(r.Ref) == "" || len(r.Ref) > 512 {
		return contractErrorf("event evidence ref must be 1..512 bytes")
	}
	if strings.ContainsRune(r.Ref, 0) {
		return contractErrorf("event evidence ref contains NUL")
	}
	return nil
}

// String renders the ref for audit details.
func (r EventEvidenceRef) String() string { return string(r.Source) + ":" + r.Ref }

// EventEvidenceSet is a deduplicating provenance set. Adding the same physical
// source ref twice is idempotent: the second add reports false and does not
// inflate the independent-evidence count ("同一来源不得包装成多独立证据").
// The zero value is usable.
type EventEvidenceSet struct {
	refs []EventEvidenceRef
	seen map[string]struct{}
}

// Add records one provenance ref. It returns (true, nil) for a new ref,
// (false, nil) when the same source+ref was already recorded, and an error for
// a malformed ref.
func (s *EventEvidenceSet) Add(ref EventEvidenceRef) (bool, error) {
	if err := ref.Validate(); err != nil {
		return false, err
	}
	key := ref.String()
	if s.seen == nil {
		s.seen = make(map[string]struct{})
	}
	if _, ok := s.seen[key]; ok {
		return false, nil
	}
	s.seen[key] = struct{}{}
	s.refs = append(s.refs, ref)
	return true, nil
}

// Len returns the number of distinct evidence refs.
func (s *EventEvidenceSet) Len() int { return len(s.refs) }

// Refs returns a defensive copy of the recorded refs in insertion order.
func (s *EventEvidenceSet) Refs() []EventEvidenceRef {
	out := make([]EventEvidenceRef, len(s.refs))
	copy(out, s.refs)
	return out
}

// Sources returns the distinct physical sources in stable sorted order.
func (s *EventEvidenceSet) Sources() []EventDeliverySource {
	seen := make(map[EventDeliverySource]struct{}, len(s.refs))
	out := make([]EventDeliverySource, 0, len(s.refs))
	for _, ref := range s.refs {
		if _, ok := seen[ref.Source]; ok {
			continue
		}
		seen[ref.Source] = struct{}{}
		out = append(out, ref.Source)
	}
	slices.Sort(out)
	return out
}

// IndependentSourceCount returns how many distinct physical sources back this
// evidence. Duplicate refs never raise it.
func (s *EventEvidenceSet) IndependentSourceCount() int { return len(s.Sources()) }

// EventDeliveryStatus is the conservative usability status of one event-state
// bundle. Only EventDeliveryComplete can support a consistent conclusion;
// every other status maps to pending (stale/unknown/incomplete) and never to
// consistent (FR-004/005/006).
type EventDeliveryStatus string

const (
	// EventDeliveryComplete: every scoped read succeeded, every in-scope
	// delivery reached a terminal consumer outcome, receipts are connected
	// and the evidence is inside its freshness tolerance. Zero observations
	// with this status means "covered and empty".
	EventDeliveryComplete EventDeliveryStatus = "complete"
	// EventDeliveryIncomplete: coverage or consumer-result evidence is not
	// closed (truncated scope, unclosed delivery, open quarantine, blocked
	// publish, unproven duplicate, unreadable auxiliary evidence, or an
	// unconnected/unconfigured upstream).
	EventDeliveryIncomplete EventDeliveryStatus = "incomplete"
	// EventDeliveryStale: the bundle is closed but evidence exceeds its
	// freshness tolerance; conclusions degrade to pending (FR-005) and never
	// trigger automatic repair.
	EventDeliveryStale EventDeliveryStatus = "stale"
	// EventDeliveryUnknown: a required read (outbox or consumer inbox) failed,
	// or the scope identity cannot be read at all; nothing may be concluded.
	EventDeliveryUnknown EventDeliveryStatus = "unknown"
)

// Valid reports whether s is one of the closed statuses.
func (s EventDeliveryStatus) Valid() bool {
	switch s {
	case EventDeliveryComplete, EventDeliveryIncomplete, EventDeliveryStale, EventDeliveryUnknown:
		return true
	}
	return false
}

// CanSupportConsistent reports whether this status may support a consistent
// verdict. Only complete does; stale/incomplete/unknown never can (FR-004/005).
func (s EventDeliveryStatus) CanSupportConsistent() bool { return s == EventDeliveryComplete }

// eventDeliverySeverity orders statuses so that combining observations keeps
// the most conservative outcome.
func eventDeliverySeverity(s EventDeliveryStatus) int {
	switch s {
	case EventDeliveryUnknown:
		return 3
	case EventDeliveryIncomplete:
		return 2
	case EventDeliveryStale:
		return 1
	case EventDeliveryComplete:
		return 0
	}
	return 0
}

// Stable machine reasons recorded on a bundle. They are audit/evidence
// vocabulary (bounded, low cardinality) and MUST NOT be reworded once
// published.
const (
	EventReasonSourceReadFailed         = "source_read_failed"
	EventReasonScopeTruncated           = "scope_rows_exceed_budget"
	EventReasonUnknownRowShape          = "unknown_row_shape"
	EventReasonDeliveryNotClosed        = "delivery_not_closed"
	EventReasonNoConsumerRegistered     = "no_consumer_registered"
	EventReasonPublishPending           = "publish_pending"
	EventReasonPublishBlocked           = "publish_blocked"
	EventReasonQuarantineOpen           = "quarantine_open"
	EventReasonVersionGapUnconfirmed    = "version_gap_unconfirmed"
	EventReasonDuplicateAbsorbed        = "duplicate_absorbed_metrics_only"
	EventReasonDuplicateDivergent       = "duplicate_divergent_evidenced"
	EventReasonDuplicateUnverifiable    = "duplicate_absorption_unproven"
	EventReasonReceiptUnconnected       = "upstream_receipt_source_unconnected"
	EventReasonReceiptUnconfigured      = "upstream_receipt_source_unconfigured"
	EventReasonStale                    = "evidence_age_exceeds_tolerance"
	EventReasonProgressUnknown          = "consumer_progress_unavailable"
	EventReasonHeightScopeWithoutTime   = "height_scope_without_time_window"
	EventReasonAuditProbeFailed         = "outbox_audit_probe_failed"
	EventReasonAuditGap                 = "outbox_audit_gap_observed"
	EventReasonScopeCoveredEmpty        = "scope_covered_and_empty"
	EventReasonExternalCreditUnprovable = "external_credit_never_provable_from_events"
)

// EventVersionRelation is the per-consumer position of one emitted event
// version against the consumer's durable version watermark, or the consumer
// effect that was actually observed.
type EventVersionRelation string

const (
	// EventVersionApplied: the consumer's inbox carries this event (the
	// durable idempotency/effect record).
	EventVersionApplied EventVersionRelation = "applied"
	// EventVersionSuperseded: no effect for this event and the durable
	// watermark is already at or past its version: the version guard ignored
	// an equal/older version (Q4 legal absorption, metrics-only).
	EventVersionSuperseded EventVersionRelation = "superseded"
	// EventVersionNext: the event is exactly the next expected version; it is
	// in flight, never a divergence.
	EventVersionNext EventVersionRelation = "next"
	// EventVersionGap: the event jumps beyond watermark+1; the gap is
	// unconfirmed (may be in flight or retention-trimmed), never a claim.
	EventVersionGap EventVersionRelation = "gap"
	// EventVersionUnapplied: the consumer has a readable, empty state for this
	// aggregate; the event has simply not been consumed yet.
	EventVersionUnapplied EventVersionRelation = "unapplied"
	// EventVersionUnknown: the evidence needed to place the version (or the
	// consumer's liveness) is unavailable; nothing may be inferred.
	EventVersionUnknown EventVersionRelation = "unknown"
)

// Valid reports whether r is one of the closed version relations.
func (r EventVersionRelation) Valid() bool {
	switch r {
	case EventVersionApplied, EventVersionSuperseded, EventVersionNext,
		EventVersionGap, EventVersionUnapplied, EventVersionUnknown:
		return true
	}
	return false
}

// EventContradiction is a hard, adapter-observable contradiction between the
// emitted event and the consumer's durable records. Unlike a missing/aged
// signal, a contradiction is definite evidence and never resolved by waiting.
type EventContradiction string

const (
	// EventContradictionNone: no contradiction observed.
	EventContradictionNone EventContradiction = ""
	// EventContradictionContentMismatch: the inbox record for this event_id
	// carries a different aggregate identity/version than the emitted row:
	// the delivered content diverged from the emission.
	EventContradictionContentMismatch EventContradiction = "content_mismatch"
	// EventContradictionVersionRegression: the applied version is ahead of
	// the durable watermark it must have advanced (the T4 transaction writes
	// both atomically).
	EventContradictionVersionRegression EventContradiction = "version_regression"
	// EventContradictionWatermarkMissing: the inbox carries the event but no
	// durable watermark exists for the aggregate (the T4 transaction writes
	// both atomically).
	EventContradictionWatermarkMissing EventContradiction = "watermark_missing"
)

// Valid reports whether c is part of the closed contradiction vocabulary.
func (c EventContradiction) Valid() bool {
	switch c {
	case EventContradictionNone, EventContradictionContentMismatch,
		EventContradictionVersionRegression, EventContradictionWatermarkMissing:
		return true
	}
	return false
}

// EventDuplicateVerdict is the Q4 duplicate judgment at the observation level.
// It is derived only from evidenced facts (content comparison, version rules,
// idempotency/effect records); a delivery count alone never decides it.
type EventDuplicateVerdict string

const (
	// EventDuplicateNone: no duplicate signal, or a consumer effect exists.
	EventDuplicateNone EventDuplicateVerdict = "none"
	// EventDuplicateAbsorbed: every observed consumer relation is a legal
	// version-guard supersession with no effect — Q4 absorbed, metrics-only.
	EventDuplicateAbsorbed EventDuplicateVerdict = "absorbed"
	// EventDuplicateDivergent: a hard contradiction was observed (delivered
	// content diverged, or a version rule was violated).
	EventDuplicateDivergent EventDuplicateVerdict = "divergent"
	// EventDuplicateUnverifiable: the evidence cannot prove absorption or
	// divergence (a read failed or the consumer's liveness is unknown);
	// pending, never a ticket.
	EventDuplicateUnverifiable EventDuplicateVerdict = "unverifiable"
)

// Valid reports whether v is part of the closed verdict vocabulary.
func (v EventDuplicateVerdict) Valid() bool {
	switch v {
	case EventDuplicateNone, EventDuplicateAbsorbed, EventDuplicateDivergent, EventDuplicateUnverifiable:
		return true
	}
	return false
}

// EventApplication is one consumer_inbox row: the durable idempotency and
// in-project effect record of one consumer for one event (the T4 transaction
// commits inbox + version + effect + progress together).
type EventApplication struct {
	ConsumerName     string    `json:"consumer_name"`
	EventID          uuid.UUID `json:"event_id"`
	AggregateType    string    `json:"aggregate_type"`
	AggregateID      string    `json:"aggregate_id"`
	AggregateVersion int64     `json:"aggregate_version"`
	Topic            string    `json:"topic"`
	Partition        int       `json:"partition"`
	Offset           int64     `json:"offset"`
	AppliedAt        time.Time `json:"applied_at"`
}

// EventVersionWatermark is one consumer_versions row: the durable
// version-guard high-water mark of one consumer for one aggregate.
type EventVersionWatermark struct {
	ConsumerName  string    `json:"consumer_name"`
	AggregateType string    `json:"aggregate_type"`
	AggregateID   string    `json:"aggregate_id"`
	MaxVersion    int64     `json:"max_version"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// EventQuarantineSignal is one open consumer_quarantine entry of an in-scope
// event. Quarantine is observation only: replay/unblock stay operator paths.
type EventQuarantineSignal struct {
	ConsumerName    string                  `json:"consumer_name"`
	EventID         uuid.UUID               `json:"event_id"`
	FailureClass    events.QuarantineReason `json:"failure_class"`
	AttemptCount    int                     `json:"attempt_count"`
	FirstSeenAt     time.Time               `json:"first_seen_at"`
	LastSeenAt      time.Time               `json:"last_seen_at"`
	SourceTopic     string                  `json:"source_topic,omitempty"`
	SourcePartition *int                    `json:"source_partition,omitempty"`
	SourceOffset    *int64                  `json:"source_offset,omitempty"`
}

// EventConsumerRelation is one consumer's observed relationship to one emitted
// event.
type EventConsumerRelation struct {
	ConsumerName           string               `json:"consumer_name"`
	Relation               EventVersionRelation `json:"relation"`
	HasWatermark           bool                 `json:"has_watermark"`
	MaxVersion             int64                `json:"max_version,omitempty"`
	AppliedVersion         int64                `json:"applied_version,omitempty"`
	AppliedAt              *time.Time           `json:"applied_at,omitempty"`
	Quarantined            bool                 `json:"quarantined,omitempty"`
	QuarantineFailureClass string               `json:"quarantine_failure_class,omitempty"`
	Contradiction          EventContradiction   `json:"contradiction,omitempty"`
	ContradictionDetail    string               `json:"contradiction_detail,omitempty"`
}

// EventResumeCheck records one consumer.go:914 ResumeOffset check performed
// when the durable progress set was unexpectedly empty.
type EventResumeCheck struct {
	Topic      string `json:"topic"`
	Partition  int    `json:"partition"`
	Found      bool   `json:"found"`
	NextOffset int64  `json:"next_offset,omitempty"`
}

// EventConsumerState is one registered consumer's observed liveness state.
type EventConsumerState struct {
	Name              string                  `json:"name"`
	Progress          []events.Progress       `json:"progress,omitempty"`
	ProgressKnown     bool                    `json:"progress_known"`
	ProgressAge       time.Duration           `json:"progress_age"`
	OpenQuarantine    int64                   `json:"open_quarantine"`
	ScopedQuarantine  int                     `json:"scoped_quarantine"`
	QuarantineSignals []EventQuarantineSignal `json:"quarantine_signals,omitempty"`
	ResumeChecks      []EventResumeCheck      `json:"resume_checks,omitempty"`
}

// EventFamilyBacklog is one global outbox capacity row (outbox.go CapacitySQL):
// the pending backlog of one event family, scope-independent context only.
type EventFamilyBacklog struct {
	Family    string        `json:"family"`
	Pending   int64         `json:"pending"`
	OldestAge time.Duration `json:"oldest_age"`
}

// EventBacklogState records the scoped and global outbox backlog observed with
// the bundle. Pending rows are in flight; blocked rows are definite delivery
// failures until an audited unblock.
type EventBacklogState struct {
	ScopedPending   int64                `json:"scoped_pending"`
	ScopedBlocked   int64                `json:"scoped_blocked"`
	OldestPendingAt *time.Time           `json:"oldest_pending_at,omitempty"`
	GlobalFamilies  []EventFamilyBacklog `json:"global_families,omitempty"`
	GlobalBlocked   int64                `json:"global_blocked"`
}

// EventFreshnessState records the freshness inputs of one observation.
// Staleness only ever downgrades; it never triggers automatic repair (FR-005).
type EventFreshnessState struct {
	Tolerance         time.Duration `json:"tolerance"`
	CapturedAt        time.Time     `json:"captured_at"`
	HasPending        bool          `json:"has_pending"`
	OldestPendingAge  time.Duration `json:"oldest_pending_age"`
	HasUnclosed       bool          `json:"has_unclosed"`
	OldestUnclosedAge time.Duration `json:"oldest_unclosed_age"`
	Stale             bool          `json:"stale"`
	Unknown           bool          `json:"unknown"`
	Reasons           []string      `json:"reasons,omitempty"`
}

// EventDeliverySummary is the bounded observation summary of one bundle.
// Absorbed duplicates are deliberately counted separately from divergent
// duplicates and from unproven ones (Q4); T029 can export the absorbed rate as
// an operational signal separate from fund tickets.
type EventDeliverySummary struct {
	Observations           int `json:"observations"`
	Terminal               int `json:"terminal"`
	Unclosed               int `json:"unclosed"`
	AbsorbedDuplicates     int `json:"absorbed_duplicates"`
	DivergentDuplicates    int `json:"divergent_duplicates"`
	UnverifiableDuplicates int `json:"unverifiable_duplicates"`
	SupersededRelations    int `json:"superseded_relations"`
	OpenQuarantines        int `json:"open_quarantines"`
	BlockedPublishes       int `json:"blocked_publishes"`
}

// EventDeliveryObservation is one outbox event with its folded consumer
// results: the single evidence item of one physical source identity. Outbox
// rows are keyed by event_id (the outbox UNIQUE constraint), so one event is
// never wrapped as several observations.
type EventDeliveryObservation struct {
	EventID          uuid.UUID           `json:"event_id"`
	IdentityKind     events.IdentityKind `json:"identity_kind"`
	EventType        string              `json:"event_type"`
	AggregateType    string              `json:"aggregate_type"`
	AggregateID      string              `json:"aggregate_id"`
	AggregateVersion int64               `json:"aggregate_version"`
	PayloadHash      string              `json:"payload_hash"`
	OccurredAt       time.Time           `json:"occurred_at"`
	EmittedAt        time.Time           `json:"emitted_at"`
	ChainID          *int64              `json:"chain_id,omitempty"`
	BlockNumber      *int64              `json:"block_number,omitempty"`
	BlockHash        *string             `json:"block_hash,omitempty"`
	TxHash           *string             `json:"tx_hash,omitempty"`
	LogIndex         *int                `json:"log_index,omitempty"`
	RecoveryVersion  *int64              `json:"recovery_version,omitempty"`
	RevisesEventID   *uuid.UUID          `json:"revises_event_id,omitempty"`
	SourceKind       string              `json:"source_kind,omitempty"`
	SourceID         string              `json:"source_id,omitempty"`
	SourceVersion    *int64              `json:"source_version,omitempty"`
	PublishState     events.PublishState `json:"publish_state"`
	PublishedAt      *time.Time          `json:"published_at,omitempty"`
	LastErrorClass   string              `json:"last_error_class,omitempty"`

	// SharedSourceDeliveries counts how many in-scope outbox rows carry the
	// same (source_kind, source_id, source_version) watermark, including this
	// row. 0 means the row declares no source watermark. This is a delivery
	// count hint only; T017 never treats a count as a fund anomaly.
	SharedSourceDeliveries int `json:"shared_source_deliveries,omitempty"`

	Applications []EventApplication      `json:"applications,omitempty"`
	Watermarks   []EventVersionWatermark `json:"watermarks,omitempty"`
	Quarantines  []EventQuarantineSignal `json:"quarantines,omitempty"`
	Consumers    []EventConsumerRelation `json:"consumers,omitempty"`

	// Duplicate is the Q4 duplicate verdict for this single event identity.
	Duplicate EventDuplicateVerdict `json:"duplicate"`
	// Terminal is true when the event reached a definite recorded outcome
	// (effect present, legally absorbed, quarantined, blocked publish, or a
	// contradiction). A false value keeps the scope incomplete/pending.
	Terminal bool `json:"terminal"`
	// PendingReason names why an unclosed delivery is pending (bounded tokens).
	PendingReason string `json:"pending_reason,omitempty"`
	// MissingEvidence lists the evidence gaps observed for this event.
	MissingEvidence []string `json:"missing_evidence,omitempty"`
	// ShapeProblems lists schema-shape surprises (unknown catalog type,
	// unknown publish state). Any entry keeps the bundle incomplete.
	ShapeProblems []string `json:"shape_problems,omitempty"`
}

// EventStateConfig carries the adapter's bounded read tolerances. Every value
// is a deployment/test parameter (research §7): the adapter never invents a
// production threshold and unbounded is not representable.
type EventStateConfig struct {
	// FreshnessTolerance is the maximum age of pending/unclosed event
	// evidence before it degrades to stale (FR-005).
	FreshnessTolerance time.Duration
	// MaxEntries bounds how many outbox rows one observation may read. It is
	// the task budget's per-claim item quota (T008); a scope with more rows
	// stays incomplete instead of scanning unbounded (FR-019).
	MaxEntries int
}

// Validate fails closed on missing or non-positive bounds.
func (c EventStateConfig) Validate() error {
	if c.FreshnessTolerance <= 0 {
		return contractErrorf("event state config requires a positive freshness tolerance")
	}
	if c.MaxEntries <= 0 {
		return contractErrorf("event state config requires a positive max entries bound")
	}
	return nil
}

// EventProgressReader is the durable consumer-progress surface
// (consumer.go:887 ReadProgress and consumer.go:914 ResumeOffset).
// *events.Consumer satisfies it; tests may substitute a reader.
type EventProgressReader interface {
	ReadProgress(ctx context.Context) ([]events.Progress, error)
	ResumeOffset(ctx context.Context, topic string, partition int) (int64, bool, error)
}

// EventQuarantineReader is the read-only quarantine surface
// (quarantine.go ListOpen/OpenCount). *events.QuarantineStore satisfies it;
// replay/unblock are deliberately NOT part of this interface.
type EventQuarantineReader interface {
	ListOpen(ctx context.Context, consumerName string) ([]events.QuarantineEntry, error)
	OpenCount(ctx context.Context, consumerName string) (int64, error)
}

// EventConsumerRegistration names one registered consumer whose delivery
// results the adapter observes. Name is the consumer name (contracts/consumer.md
// §8: the reference consumer never shares a name); Progress is a durable
// progress reader, typically the live *events.Consumer.
type EventConsumerRegistration struct {
	Name     string              `json:"name"`
	Progress EventProgressReader `json:"-"`
}

// Validate checks the registration shape conservatively.
func (c EventConsumerRegistration) Validate() error {
	if strings.TrimSpace(c.Name) == "" || strings.TrimSpace(c.Name) != c.Name {
		return contractErrorf("event consumer registration requires a non-empty consumer name")
	}
	if len(c.Name) > 128 || strings.ContainsRune(c.Name, 0) {
		return contractErrorf("event consumer name %q is malformed", c.Name)
	}
	if c.Progress == nil {
		return contractErrorf("event consumer %q has no progress reader", c.Name)
	}
	return nil
}

// EventStateInterval is one inclusive scan interval of the task's scope kind.
// From/To are RangeBounds of the same kind; a height interval filters
// outbox_events.block_number, a time interval filters outbox_events.occurred_at.
type EventStateInterval struct {
	From RangeBound
	To   RangeBound
}

// Validate checks the interval shape conservatively.
func (i EventStateInterval) Validate() error {
	if !i.From.Valid() || !i.To.Valid() {
		return contractErrorf("event state interval has an invalid range bound")
	}
	if !i.From.Kind.Known() {
		return contractErrorf("event state interval has unknown scope kind %q", i.From.Kind)
	}
	if i.From.Kind != i.To.Kind {
		return contractErrorf("event state interval kinds differ: %q vs %q", i.From.Kind, i.To.Kind)
	}
	cmp, err := i.From.Compare(i.To)
	if err != nil {
		return err
	}
	if cmp > 0 {
		return contractErrorf("event state interval is not ascending")
	}
	return nil
}

// Kind returns the interval's scope kind.
func (i EventStateInterval) Kind() ScopeKind { return i.From.Kind }

// EventStateQuery is one read-only observation request over a bounded scan
// interval. Scope supplies the chain id and the closed business-type set; the
// interval supplies the height/time window of the claimed attempt.
//
// A height-scoped interval attributes outbox rows through two alternative
// paths: evm_log rows by their exact block identity, and blockless
// business-object rows by the interval's resolved OccurredFrom/To chain-time
// window (endpoint heights exact and inclusive; the chain times are the
// header's own second-precision timestamps, closed at the next height's chain
// time so adjacent intervals tile without a seam; resolved by the T036 height
// window resolver, with an unclosable right seam declared as a caller gap).
// Without the resolved window only block-identified rows are attributable and
// the bundle stays incomplete instead of silently under-covering; the window
// is never guessed from a local clock or an index insert time, and no
// sub-second precision is claimed.
type EventStateQuery struct {
	Scope    IdentityScope      `json:"scope"`
	Interval EventStateInterval `json:"interval"`
	// OccurredFrom/OccurredTo carry the interval's resolved occurrence-time
	// window (both or neither). For a time interval they narrow the
	// occurred_at filter; for a height interval they are the alternative
	// attribution path for blockless business-object rows (OR-ed with the
	// exact block-identity window), resolved from chain block times — never
	// guessed. Times carry chain-header second precision.
	OccurredFrom *time.Time `json:"occurred_from,omitempty"`
	OccurredTo   *time.Time `json:"occurred_to,omitempty"`
	// Consumers are the registered consumers that own the scope's event
	// types. An event untouched by every registered consumer is unclosed and
	// keeps the bundle incomplete; an empty list can never be closed over.
	Consumers []EventConsumerRegistration `json:"consumers,omitempty"`
	// Quarantine is the read-only quarantine surface; required when consumers
	// are registered so a poisoned event is never mistaken for "not yet
	// consumed".
	Quarantine EventQuarantineReader `json:"-"`
	// UpstreamReceipts is the task's declared per-business-type upstream
	// receipt source (recon_task.upstream_receipt_source, parsed by
	// ParseChainUpstreamReceiptSources). Nil/empty means "not connected",
	// which downgrades the bundle (FR-006).
	UpstreamReceipts []ChainUpstreamReceiptSource `json:"upstream_receipts,omitempty"`
	// AuditProbes optionally run the existing ReconciliationAudit
	// (audit.go:133/158) as part of this observation. The report is attached
	// to the bundle; probe errors and gaps downgrade it conservatively.
	AuditProbes []events.SourceProbe `json:"-"`
}

// Validate checks the query shape conservatively.
func (q EventStateQuery) Validate() error {
	if err := q.Scope.Validate(); err != nil {
		return err
	}
	if err := q.Interval.Validate(); err != nil {
		return err
	}
	if q.Interval.Kind() != q.Scope.Kind {
		return contractErrorf("event state interval kind %q does not match scope kind %q",
			q.Interval.Kind(), q.Scope.Kind)
	}
	if (q.OccurredFrom == nil) != (q.OccurredTo == nil) {
		return contractErrorf("event state query requires both occurred bounds or neither")
	}
	if q.OccurredFrom != nil {
		if q.OccurredFrom.IsZero() || q.OccurredTo.IsZero() {
			return contractErrorf("event state query occurred bounds must be non-zero")
		}
		if q.OccurredTo.Before(*q.OccurredFrom) {
			return contractErrorf("event state query occurred window is not ascending")
		}
	}
	seenConsumers := make(map[string]struct{}, len(q.Consumers))
	for _, consumer := range q.Consumers {
		if err := consumer.Validate(); err != nil {
			return err
		}
		if _, dup := seenConsumers[consumer.Name]; dup {
			return contractErrorf("duplicate event consumer registration %q", consumer.Name)
		}
		seenConsumers[consumer.Name] = struct{}{}
	}
	if len(q.Consumers) > 0 && q.Quarantine == nil {
		return contractErrorf("event state query requires a quarantine reader when consumers are registered")
	}
	seenReceipts := make(map[BusinessType]struct{}, len(q.UpstreamReceipts))
	for _, receipt := range q.UpstreamReceipts {
		if err := receipt.Validate(); err != nil {
			return err
		}
		if _, dup := seenReceipts[receipt.BusinessType]; dup {
			return contractErrorf("duplicate upstream receipt declaration for business type %q", receipt.BusinessType)
		}
		seenReceipts[receipt.BusinessType] = struct{}{}
	}
	return nil
}

// EventStateAdapter reads durable event-delivery facts. It is read-only and
// stateless apart from its pool/config; concurrent use is safe.
type EventStateAdapter struct {
	pool *pgxpool.Pool
	cfg  EventStateConfig
}

// NewEventStateAdapter validates the dependencies and bounds. A nil pool or
// invalid config is refused fail-closed. It performs no I/O.
func NewEventStateAdapter(pool *pgxpool.Pool, cfg EventStateConfig) (*EventStateAdapter, error) {
	if pool == nil {
		return nil, contractErrorf("event state adapter requires a database pool")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &EventStateAdapter{pool: pool, cfg: cfg}, nil
}

// EventStateEvidence is one immutable event-party evidence bundle. Status and
// Reasons are conservative metadata; CanonicalBytes covers only the observed
// facts so re-observing the same facts stays identity-stable.
type EventStateEvidence struct {
	Scope            IdentityScope                `json:"scope"`
	Interval         EventStateInterval           `json:"interval"`
	OccurredFrom     *time.Time                   `json:"occurred_from,omitempty"`
	OccurredTo       *time.Time                   `json:"occurred_to,omitempty"`
	CapturedAt       time.Time                    `json:"captured_at"`
	Config           EventStateConfig             `json:"config"`
	Observations     []EventDeliveryObservation   `json:"observations,omitempty"`
	Consumers        []EventConsumerState         `json:"consumers,omitempty"`
	Backlog          EventBacklogState            `json:"backlog"`
	Freshness        EventFreshnessState          `json:"freshness"`
	UpstreamReceipts []ChainUpstreamReceiptSource `json:"upstream_receipts,omitempty"`
	Audit            *events.AuditReport          `json:"-"`

	Summary EventDeliverySummary `json:"summary"`
	Status  EventDeliveryStatus  `json:"status"`
	Reasons []string             `json:"reasons,omitempty"`

	evidence EventEvidenceSet
}

// mark raises the bundle status monotonically and records a reason. Unknown
// always wins over incomplete, which wins over stale, which wins over complete:
// the most conservative observation survives combination.
func (e *EventStateEvidence) mark(status EventDeliveryStatus, reason string) {
	if eventDeliverySeverity(status) > eventDeliverySeverity(e.Status) {
		e.Status = status
	}
	if reason != "" {
		e.addReason(reason)
	}
}

// addReason appends one reason without duplicates.
func (e *EventStateEvidence) addReason(reason string) {
	if slices.Contains(e.Reasons, reason) {
		return
	}
	e.Reasons = append(e.Reasons, reason)
}

// addEvidence records one provenance ref, deduplicating by ref.
func (e *EventStateEvidence) addEvidence(source EventDeliverySource, ref string) error {
	_, err := e.evidence.Add(EventEvidenceRef{Source: source, Ref: ref})
	return err
}

// CanSupportConsistent reports whether this bundle may support a consistent
// verdict. Only a complete bundle (fresh, closed, connected) can.
func (e *EventStateEvidence) CanSupportConsistent() bool {
	return e != nil && e.Status == EventDeliveryComplete
}

// CoverageClosed reports whether the bundle's own observation window is
// closed, independently of the upstream receipt declaration. It differs from
// CanSupportConsistent in exactly one dimension: an unconnected or
// unconfigured upstream receipt source downgrades Status (FR-006), but it is
// an input of the classifier's upstream-credit verdict
// (Observation.Upstream / ExternalCredit), not a defect of the event-delivery
// coverage. The compare loop therefore uses this predicate for ScanComplete
// while the classifier keeps ExternalCredit at its honest (usually
// unverified) state and still refuses a consistent conclusion without a
// connected, available source.
//
// The predicate is deliberately exact, never a superset of "complete": a
// complete bundle is closed; an incomplete bundle is closed only when every
// recorded reason is an upstream-connectivity mark or a purely informational
// note (the always-recorded external-credit reason and the absorbed-duplicate
// metrics-only marker). Any other reason (truncated scope, unknown row shape,
// unclosed delivery, no registered consumer, open quarantine, blocked publish,
// unproven duplicate, unreadable progress, failed audit probe, height scope
// without a time window, stale or unknown reads) means the delivery coverage
// itself is unproven and the observation must stay pending/gap.
func (e *EventStateEvidence) CoverageClosed() bool {
	if e == nil {
		return false
	}
	switch e.Status {
	case EventDeliveryComplete:
		return true
	case EventDeliveryIncomplete:
		if len(e.Reasons) == 0 {
			// An unexplained incomplete status cannot prove its own reason;
			// fail closed.
			return false
		}
		for _, reason := range e.Reasons {
			switch reason {
			case EventReasonReceiptUnconnected, EventReasonReceiptUnconfigured,
				EventReasonExternalCreditUnprovable,
				// EventReasonDuplicateAbsorbed is informational only: it is
				// recorded alongside an already terminal, legally absorbed
				// duplicate (metrics-only, no downgrade). Excluding it would
				// let one absorbed duplicate fail the whole interval's coverage
				// and re-mask fully proven local facts through the Q4 path,
				// while the classifier's absorbed-duplicate verdict is
				// unchanged (classify.go).
				EventReasonDuplicateAbsorbed:
				// Upstream-declaration dimension (owned by the classifier) and
				// informational notes: not delivery-coverage marks.
			default:
				return false
			}
		}
		return true
	}
	return false
}

// EmptyCovered reports whether the scope was covered and contained no event
// delivery facts at all (Edge: "覆盖为空且完整", never "no anomalies").
func (e *EventStateEvidence) EmptyCovered() bool {
	return e != nil && e.Status == EventDeliveryComplete && len(e.Observations) == 0
}

// PendingReverify maps the bundle onto the reverify vocabulary. ok=false means
// the evidence is complete and the caller may proceed to compare; when ok=true
// the verdict is never consistent (FR-004/005, Q1/Q5).
func (e *EventStateEvidence) PendingReverify() (ReverifyVerdict, bool) {
	if e == nil {
		return ReverifyUnknown, true
	}
	switch e.Status {
	case EventDeliveryComplete:
		return "", false
	case EventDeliveryStale:
		return ReverifyStale, true
	default:
		return ReverifyUnknown, true
	}
}

// EvidenceRefs returns the deduplicated provenance set of the bundle.
func (e *EventStateEvidence) EvidenceRefs() []EventEvidenceRef { return e.evidence.Refs() }

// Sources returns the distinct physical sources backing the evidence.
func (e *EventStateEvidence) Sources() []EventDeliverySource { return e.evidence.Sources() }

// IndependentSourceCount returns the number of distinct physical sources. A
// duplicated read never raises it.
func (e *EventStateEvidence) IndependentSourceCount() int { return e.evidence.IndependentSourceCount() }

// UnconnectedUpstreams lists the scope business types whose declared upstream
// receipt source is not connected. A missing declaration is reported by
// UpstreamDeclaredConnected, not here.
func (e *EventStateEvidence) UnconnectedUpstreams() []BusinessType {
	var out []BusinessType
	for _, item := range e.UpstreamReceipts {
		if !item.Connected {
			out = append(out, item.BusinessType)
		}
	}
	return out
}

// UpstreamDeclaredConnected reports whether every scope business type has a
// connected upstream receipt declaration. This is a declaration about a read
// path, never proof of upstream success (FR-006): ExternalLedgerProvable
// always reports false.
func (e *EventStateEvidence) UpstreamDeclaredConnected() bool {
	if e == nil || len(e.Scope.BusinessTypes) == 0 {
		return false
	}
	declared := make(map[BusinessType]ChainUpstreamReceiptSource, len(e.UpstreamReceipts))
	for _, item := range e.UpstreamReceipts {
		declared[item.BusinessType] = item
	}
	for _, businessType := range e.Scope.BusinessTypes {
		item, ok := declared[businessType]
		if !ok || !item.Connected {
			return false
		}
	}
	return true
}

// ExternalLedgerProvable reports whether this evidence proves the upstream
// ledger processed the scope correctly. Always false: the local event pipeline
// is not the upstream ledger, and a connected flag alone never proves upstream
// success (FR-006, Q4-5, data-model.md §4).
func (e *EventStateEvidence) ExternalLedgerProvable() bool { return false }

// VersionDomain derives the event-side evidence version domain (identity.go
// VersionDomain): the latest block identity observed in scope (evm_log
// events), the highest recovery version observed, and the evidence time.
// PG-side versions are supplied by the other adapters; the caller merges.
func (e *EventStateEvidence) VersionDomain() VersionDomain {
	domain := VersionDomain{EvidenceAt: e.CapturedAt}
	if e == nil {
		return domain
	}
	var (
		latestBlock int64 = -1
		latestHash  string
		maxRecovery int64
	)
	for i := range e.Observations {
		observation := &e.Observations[i]
		if observation.BlockNumber != nil && *observation.BlockNumber > latestBlock {
			latestBlock = *observation.BlockNumber
			if observation.BlockHash != nil {
				latestHash = *observation.BlockHash
			}
		}
		if observation.RecoveryVersion != nil && *observation.RecoveryVersion > maxRecovery {
			maxRecovery = *observation.RecoveryVersion
		}
	}
	if latestBlock >= 0 && latestHash != "" {
		domain.BlockNumber = uint64(latestBlock)
		domain.BlockHash = latestHash
	}
	if maxRecovery > 0 {
		domain.RecoveryVersion = strconv.FormatInt(maxRecovery, 10)
	}
	return domain
}

// CanonicalBytes returns the deterministic scope-level event-party snapshot
// bytes. Only observed facts are encoded (chain/scope/interval and the sorted
// per-event canonical snapshots): status, reasons, freshness, receipts and
// capture time are evidence metadata and are deliberately excluded so
// re-observing the same facts yields the same content hash.
func (e *EventStateEvidence) CanonicalBytes() []byte {
	w := &identityCanonWriter{}
	w.bytesField("eventstate.version", []byte(eventStateCanonicalVersion))
	w.stringField("eventstate.chain_id", e.Scope.ChainID)
	w.stringField("eventstate.scope_kind", string(e.Scope.Kind))
	w.int64Field("eventstate.scope_from", e.Scope.From)
	w.int64Field("eventstate.scope_to", e.Scope.To)
	if e.Interval.Kind() == ScopeTime {
		w.int64Field("eventstate.interval_from_unix_micro", e.Interval.From.Time.UnixMicro())
		w.int64Field("eventstate.interval_to_unix_micro", e.Interval.To.Time.UnixMicro())
	} else {
		w.int64Field("eventstate.interval_from", e.Interval.From.Height)
		w.int64Field("eventstate.interval_to", e.Interval.To.Height)
	}

	ordered := make([]EventDeliveryObservation, len(e.Observations))
	copy(ordered, e.Observations)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].EventID.String() < ordered[j].EventID.String()
	})
	w.uint64Field("eventstate.observations.count", uint64(len(ordered)))
	for i := range ordered {
		w.bytesField(fmt.Sprintf("eventstate.observation.%d", i), ordered[i].CanonicalBytes())
	}
	return w.buf.Bytes()
}

// CanonicalBytes returns the deterministic per-event snapshot bytes used as
// the event party's content (PartyObservation.Content). Delivery coordinates
// (topic/partition/offset) are excluded: they are operational metadata, not
// business facts.
func (o EventDeliveryObservation) CanonicalBytes() []byte {
	w := &identityCanonWriter{}
	w.bytesField("eventobservation.version", []byte(eventObservationCanonicalVersion))
	w.stringField("eventobservation.event_id", o.EventID.String())
	w.stringField("eventobservation.identity_kind", string(o.IdentityKind))
	w.stringField("eventobservation.event_type", o.EventType)
	w.stringField("eventobservation.aggregate_type", o.AggregateType)
	w.stringField("eventobservation.aggregate_id", o.AggregateID)
	w.int64Field("eventobservation.aggregate_version", o.AggregateVersion)
	w.stringField("eventobservation.payload_hash", o.PayloadHash)
	w.int64Field("eventobservation.occurred_at_unix_nano", o.OccurredAt.UnixNano())
	writeOptionalInt64(w, "eventobservation.chain_id", o.ChainID)
	writeOptionalInt64(w, "eventobservation.block_number", o.BlockNumber)
	writeOptionalString(w, "eventobservation.block_hash", o.BlockHash)
	writeOptionalString(w, "eventobservation.tx_hash", o.TxHash)
	writeOptionalInt(w, "eventobservation.log_index", o.LogIndex)
	writeOptionalInt64(w, "eventobservation.recovery_version", o.RecoveryVersion)
	if o.RevisesEventID != nil {
		w.uint64Field("eventobservation.revises_event_id.present", 1)
		w.stringField("eventobservation.revises_event_id.value", o.RevisesEventID.String())
	} else {
		w.uint64Field("eventobservation.revises_event_id.present", 0)
	}
	w.stringField("eventobservation.source_kind", o.SourceKind)
	w.stringField("eventobservation.source_id", o.SourceID)
	writeOptionalInt64(w, "eventobservation.source_version", o.SourceVersion)
	w.stringField("eventobservation.publish_state", string(o.PublishState))
	w.stringField("eventobservation.last_error_class", o.LastErrorClass)

	applications := append([]EventApplication(nil), o.Applications...)
	sort.Slice(applications, func(i, j int) bool { return applications[i].ConsumerName < applications[j].ConsumerName })
	w.uint64Field("eventobservation.applications.count", uint64(len(applications)))
	for i, application := range applications {
		prefix := fmt.Sprintf("eventobservation.applications.%d", i)
		w.stringField(prefix+".consumer_name", application.ConsumerName)
		w.stringField(prefix+".aggregate_type", application.AggregateType)
		w.stringField(prefix+".aggregate_id", application.AggregateID)
		w.int64Field(prefix+".aggregate_version", application.AggregateVersion)
		w.int64Field(prefix+".applied_at_unix_nano", application.AppliedAt.UnixNano())
	}

	watermarks := append([]EventVersionWatermark(nil), o.Watermarks...)
	sort.Slice(watermarks, func(i, j int) bool { return watermarks[i].ConsumerName < watermarks[j].ConsumerName })
	w.uint64Field("eventobservation.watermarks.count", uint64(len(watermarks)))
	for i, watermark := range watermarks {
		prefix := fmt.Sprintf("eventobservation.watermarks.%d", i)
		w.stringField(prefix+".consumer_name", watermark.ConsumerName)
		w.int64Field(prefix+".max_version", watermark.MaxVersion)
	}

	quarantines := append([]EventQuarantineSignal(nil), o.Quarantines...)
	sort.Slice(quarantines, func(i, j int) bool { return quarantines[i].ConsumerName < quarantines[j].ConsumerName })
	w.uint64Field("eventobservation.quarantines.count", uint64(len(quarantines)))
	for i, quarantine := range quarantines {
		prefix := fmt.Sprintf("eventobservation.quarantines.%d", i)
		w.stringField(prefix+".consumer_name", quarantine.ConsumerName)
		w.stringField(prefix+".failure_class", string(quarantine.FailureClass))
		w.int64Field(prefix+".first_seen_at_unix_nano", quarantine.FirstSeenAt.UnixNano())
	}
	return w.buf.Bytes()
}

// PartyObservation builds the event-party observation of this single event
// fact. The event is present; absence is canonicalized by EventAbsenceSnapshot.
func (o EventDeliveryObservation) PartyObservation() PartyObservation {
	return PartyObservation{Status: PartyPresent, Content: o.CanonicalBytes()}
}

// EventBusinessKey returns the stable business key of the event identity.
func (o EventDeliveryObservation) EventBusinessKey() BusinessKey {
	return BusinessKey{Kind: BusinessKeyEventID, Value: o.EventID.String()}
}

// EventBusinessKeyAggregate is the business key kind of one aggregate identity
// (aggregate_type/aggregate_id) used by the event adapter. It is a stable
// token: renaming it is a dedup-breaking change.
const EventBusinessKeyAggregate BusinessKeyKind = "aggregate"

// AggregateBusinessKey returns the stable business key of the event's
// aggregate identity; the zero value means the row carries no aggregate shape.
func (o EventDeliveryObservation) AggregateBusinessKey() BusinessKey {
	if o.AggregateType == "" || o.AggregateID == "" {
		return BusinessKey{}
	}
	return BusinessKey{Kind: EventBusinessKeyAggregate, Value: o.AggregateType + "/" + o.AggregateID}
}

// EvidenceRef returns the bounded evidence reference of this event for
// occurrence/reverify/audit rows (1..512 bytes).
func (o EventDeliveryObservation) EvidenceRef() string {
	return "events:v1 event_id=" + o.EventID.String() +
		" publish=" + string(o.PublishState) +
		" duplicate=" + string(o.Duplicate)
}

// DuplicateEvidence maps the observed delivery facts onto the T017 duplicate
// vocabulary (classify.go). Only evidenced facts are set: the version-guard
// legal-absorption flag, the content comparison, the idempotency record and
// the in-project consumer-effect count. Business-effect facts owned by the PG
// adapter (RepeatedBusinessEffect/RepeatedWithdrawalIntent) are left false and
// merged by the compare loop (T016).
func (o EventDeliveryObservation) DuplicateEvidence() DuplicateEvidence {
	d := DuplicateEvidence{
		Deliveries:            1,
		EffectEvidencePresent: len(o.Applications) > 0,
		EffectCount:           len(o.Applications),
	}
	if o.SharedSourceDeliveries > 1 {
		d.Deliveries = o.SharedSourceDeliveries
	}
	for _, relation := range o.Consumers {
		if relation.HasWatermark || relation.Quarantined {
			d.IdempotencyRecorded = true
		}
		switch relation.Contradiction {
		case EventContradictionContentMismatch:
			d.ContentChecked = true
			d.ContentDivergent = true
		case EventContradictionVersionRegression, EventContradictionWatermarkMissing:
			d.ContentChecked = true
			d.VersionRuleViolated = true
		}
	}
	switch o.Duplicate {
	case EventDuplicateAbsorbed:
		d.ContentChecked = true
		d.VersionGuardIgnoredLegalOld = true
	case EventDuplicateDivergent:
		d.ContentChecked = true
	case EventDuplicateUnverifiable:
		// Unproven: deliberately leave absorption flags unset so T017 keeps
		// the observation pending instead of fabricating a claim.
	}
	return d
}

// EventAbsenceSnapshot returns the canonical bytes of a definitively absent
// event delivery fact for one business key. It is a non-empty marker so a
// genuinely absent party can still contribute to SnapshotHash; an empty byte
// slice would only downgrade the observation to incomplete. The marker is
// only meaningful inside a closed, gap-free scan (the caller owns coverage).
func EventAbsenceSnapshot(key BusinessKey) []byte {
	w := &identityCanonWriter{}
	w.bytesField("eventabsence.version", []byte(eventAbsenceCanonicalVersion))
	w.stringField("eventabsence.business_key.kind", string(key.Kind))
	w.stringField("eventabsence.business_key.value", key.Value)
	return w.buf.Bytes()
}

// writeOptionalInt64 encodes a nullable int64 with an explicit presence flag.
func writeOptionalInt64(w *identityCanonWriter, name string, value *int64) {
	if value == nil {
		w.uint64Field(name+".present", 0)
		return
	}
	w.uint64Field(name+".present", 1)
	w.int64Field(name+".value", *value)
}

// writeOptionalInt encodes a nullable int with an explicit presence flag.
func writeOptionalInt(w *identityCanonWriter, name string, value *int) {
	if value == nil {
		w.uint64Field(name+".present", 0)
		return
	}
	w.uint64Field(name+".present", 1)
	w.int64Field(name+".value", int64(*value))
}

// writeOptionalString encodes a nullable string with an explicit presence flag.
func writeOptionalString(w *identityCanonWriter, name string, value *string) {
	if value == nil {
		w.uint64Field(name+".present", 0)
		return
	}
	w.uint64Field(name+".present", 1)
	w.stringField(name+".value", *value)
}

// eventVersionKey identifies one consumer_versions row.
type eventVersionKey struct {
	ConsumerName  string
	AggregateType string
	AggregateID   string
}

// eventQuarantineKey identifies one open quarantine row of one event.
type eventQuarantineKey struct {
	ConsumerName string
	EventID      uuid.UUID
}

// eventReadFacts captures which consumer-result reads succeeded. When a read
// failed, the corresponding relations stay unknown instead of guessing.
type eventReadFacts struct {
	applicationsReadable bool
	watermarksReadable   bool
}

// Observe reads the event-delivery facts of one scan interval. It is strictly
// read-only: no transaction is opened, no lock is taken, no replay/unblock is
// invoked and no 001-015 table is written (FR-014/020/023).
//
// Failure handling is conservative: a required read failure (outbox rows or
// consumer inbox) yields EventDeliveryUnknown; a failed auxiliary read or an
// unclosed delivery yields EventDeliveryIncomplete; the error is returned
// alongside the partial bundle so callers can log/audit the cause but can
// never conclude consistency from an ignored error. A scope larger than the
// configured MaxEntries bound stays incomplete instead of scanning unbounded.
func (a *EventStateAdapter) Observe(ctx context.Context, q EventStateQuery) (EventStateEvidence, error) {
	if a == nil || a.pool == nil {
		return EventStateEvidence{}, contractErrorf("event state adapter has no database")
	}
	if err := q.Validate(); err != nil {
		return EventStateEvidence{}, err
	}
	if err := a.cfg.Validate(); err != nil {
		return EventStateEvidence{}, err
	}

	now := time.Now().UTC()
	evidence := EventStateEvidence{
		Scope:            q.Scope,
		Interval:         q.Interval,
		OccurredFrom:     q.OccurredFrom,
		OccurredTo:       q.OccurredTo,
		CapturedAt:       now,
		Config:           a.cfg,
		UpstreamReceipts: append([]ChainUpstreamReceiptSource(nil), q.UpstreamReceipts...),
		Status:           EventDeliveryComplete,
	}
	if err := evidence.addEvidence(EventSourceOutbox,
		fmt.Sprintf("chain=%s/interval=%s", q.Scope.ChainID, eventStateIntervalRef(q.Interval))); err != nil {
		return evidence, err
	}
	evidence.Freshness = EventFreshnessState{Tolerance: a.cfg.FreshnessTolerance, CapturedAt: now}

	var errs []error
	fail := func(status EventDeliveryStatus, reason string, err error) {
		evidence.mark(status, reason)
		errs = append(errs, err)
	}

	chainID, chainIDOK := eventStateChainID(q.Scope.ChainID)
	if !chainIDOK {
		fail(EventDeliveryUnknown, EventReasonSourceReadFailed,
			fmt.Errorf("event state scope chain_id %q is not a positive numeric evm chain id", q.Scope.ChainID))
	}
	if q.Interval.Kind() == ScopeHeight && q.OccurredFrom == nil {
		// Business-object rows carry no block identity; a height interval
		// without the resolved time window would silently under-cover them.
		evidence.mark(EventDeliveryIncomplete, EventReasonHeightScopeWithoutTime)
	}

	var observations []EventDeliveryObservation
	if chainIDOK {
		rows, truncated, err := a.readOutboxRows(ctx, q, chainID)
		switch {
		case err != nil:
			fail(EventDeliveryUnknown, EventReasonSourceReadFailed, fmt.Errorf("read outbox scope: %w", err))
		case truncated:
			fail(EventDeliveryIncomplete, EventReasonScopeTruncated,
				fmt.Errorf("outbox scope exceeds the %d-entry budget", a.cfg.MaxEntries))
			observations, errs = materializeEventObservations(rows, &evidence, errs)
		default:
			observations, errs = materializeEventObservations(rows, &evidence, errs)
		}
	}
	evidence.Observations = observations

	if len(observations) > 0 {
		updated, err := a.applyConsumerEvidence(ctx, q, observations, &evidence, fail)
		if err != nil {
			fail(EventDeliveryIncomplete, EventReasonSourceReadFailed, err)
		}
		evidence.Observations = updated
		markEventObservationReasons(&evidence)
	}

	// Global backlog observation (outbox.go CapacitySQL/BlockedCountSQL). It
	// is scope-independent context; a failure still downgrades the bundle
	// because the pipeline state could not be observed.
	backlog, err := a.readGlobalBacklog(ctx)
	if err != nil {
		fail(EventDeliveryIncomplete, EventReasonSourceReadFailed, fmt.Errorf("read outbox backlog: %w", err))
	} else {
		evidence.Backlog = backlog
		if err := evidence.addEvidence(EventSourceBacklog, "outbox=pending_by_family/blocked"); err != nil {
			return evidence, err
		}
	}
	applyScopedBacklog(&evidence, evidence.Observations)

	// Optional audit cross-check through the existing ReconciliationAudit.
	if len(q.AuditProbes) > 0 {
		audit, err := events.NewReconciliationAudit(q.AuditProbes, a.cfg.MaxEntries, nil)
		if err != nil {
			return evidence, fmt.Errorf("event state audit probes: %w", err)
		}
		report, err := audit.Run(ctx, a.pool)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return evidence, ctxErr
			}
			fail(EventDeliveryIncomplete, EventReasonSourceReadFailed, fmt.Errorf("run outbox audit: %w", err))
		} else {
			evidence.Audit = &report
			if err := evidence.addEvidence(EventSourceAudit,
				fmt.Sprintf("checked=%d/gaps=%d", report.Checked, len(report.Gaps))); err != nil {
				return evidence, err
			}
			if len(report.ProbeErrors) > 0 {
				evidence.mark(EventDeliveryIncomplete, EventReasonAuditProbeFailed)
			}
			for _, gap := range report.Gaps {
				if err := evidence.addEvidence(EventSourceAudit,
					fmt.Sprintf("gap=%s/%s/%d", gap.SourceKind, gap.SourceID, gap.SourceVersion)); err != nil {
					return evidence, err
				}
			}
			if len(report.Gaps) > 0 {
				evidence.mark(EventDeliveryIncomplete, EventReasonAuditGap)
			}
		}
	}

	applyEventObservationSummary(&evidence)
	a.applyFreshness(&evidence, q, evidence.Observations, now)
	a.applyUpstream(&evidence)
	if len(evidence.Observations) > 0 && len(q.Consumers) == 0 {
		evidence.mark(EventDeliveryIncomplete, EventReasonNoConsumerRegistered)
	}
	if len(evidence.Observations) == 0 && evidence.Status == EventDeliveryComplete {
		evidence.addReason(EventReasonScopeCoveredEmpty)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return evidence, ctxErr
	}
	if len(errs) > 0 {
		return evidence, errors.Join(errs...)
	}
	return evidence, nil
}

// readOutboxRows reads at most MaxEntries outbox rows of the scope window,
// ordered by emission time. truncated reports that the scope holds more rows
// than the budget allows (the caller stays incomplete).
func (a *EventStateAdapter) readOutboxRows(ctx context.Context, q EventStateQuery, chainID int64) ([]eventStateOutboxRow, bool, error) {
	where, args := eventStateOutboxWhere(q, chainID)
	args = append(args, a.cfg.MaxEntries+1)
	sql := fmt.Sprintf(eventStateOutboxSelectSQL, where, len(args))

	rows, err := a.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	var out []eventStateOutboxRow
	for rows.Next() {
		var row eventStateOutboxRow
		if err := rows.Scan(&row.eventID, &row.identityKind, &row.eventType,
			&row.aggregateType, &row.aggregateID, &row.aggregateVersion, &row.payloadHash,
			&row.occurredAt, &row.chainID, &row.blockNumber, &row.blockHash, &row.txHash,
			&row.logIndex, &row.recoveryVersion, &row.revisesEventID,
			&row.sourceKind, &row.sourceID, &row.sourceVersion,
			&row.publishState, &row.lastErrorClass, &row.publishedAt, &row.createdAt); err != nil {
			return nil, false, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if len(out) > a.cfg.MaxEntries {
		return out[:a.cfg.MaxEntries], true, nil
	}
	return out, false, nil
}

// eventStateOutboxWhere builds the scope predicate of the outbox read.
//
// A height interval covers two row shapes through two alternative (OR-ed)
// attribution paths, never a conjunction: evm_log rows by their exact block
// identity (the chain-anchored path), and blockless business_object rows by
// the interval's resolved chain-time window (the only time axis such rows
// carry). AND-ing the two would silently drop an evm_log row whose occurred_at
// (emission time) is later than its block's chain time even though its block is
// inside the interval. Without the resolved window only the block path is
// attributable, so Observe marks the bundle incomplete
// (EventReasonHeightScopeWithoutTime) and the caller can never claim absence
// over blockless rows.
func eventStateOutboxWhere(q EventStateQuery, chainID int64) (string, []any) {
	args := &eventStateArgs{}
	predicates := []string{"chain_id = " + args.add(chainID)}
	switch q.Interval.Kind() {
	case ScopeHeight:
		blockWindow := "block_number >= " + args.add(q.Interval.From.Height) +
			" AND block_number <= " + args.add(q.Interval.To.Height)
		if q.OccurredFrom != nil {
			observedWindow := "occurred_at >= " + args.add(*q.OccurredFrom) +
				" AND occurred_at <= " + args.add(*q.OccurredTo)
			predicates = append(predicates, "("+blockWindow+" OR "+observedWindow+")")
		} else {
			predicates = append(predicates, blockWindow)
		}
	case ScopeTime:
		predicates = append(predicates,
			"occurred_at >= "+args.add(q.Interval.From.Time),
			"occurred_at <= "+args.add(q.Interval.To.Time))
		// A time interval already filters occurred_at exactly; a caller-supplied
		// window only narrows it further, so it stays a conjunction.
		if q.OccurredFrom != nil {
			predicates = append(predicates,
				"occurred_at >= "+args.add(*q.OccurredFrom),
				"occurred_at <= "+args.add(*q.OccurredTo))
		}
	}
	if predicate, ok := eventStateBusinessPredicate(q.Scope.BusinessTypes, args); ok {
		predicates = append(predicates, predicate)
	}
	return strings.Join(predicates, " AND "), args.args
}

// eventStateBusinessPredicate maps the closed business-type set onto the event
// type namespace. BusinessEventDelivery covers the whole pipeline (every event
// type); withdrawal/deposit map onto their catalog prefixes.
func eventStateBusinessPredicate(businessTypes []BusinessType, args *eventStateArgs) (string, bool) {
	var prefixes []string
	for _, businessType := range businessTypes {
		switch businessType {
		case BusinessWithdrawal:
			prefixes = append(prefixes, "withdrawal.%")
		case BusinessDeposit:
			prefixes = append(prefixes, "deposit.%")
		case BusinessEventDelivery:
			return "", false
		}
	}
	if len(prefixes) == 0 {
		return "", false
	}
	parts := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		parts = append(parts, "event_type LIKE "+args.add(prefix))
	}
	return "(" + strings.Join(parts, " OR ") + ")", true
}

// eventStateArgs accumulates positional SQL arguments and returns their
// placeholders.
type eventStateArgs struct {
	args []any
}

func (a *eventStateArgs) add(value any) string {
	a.args = append(a.args, value)
	return "$" + strconv.Itoa(len(a.args))
}

// eventStateOutboxRow is one raw outbox_events row before materialization.
type eventStateOutboxRow struct {
	eventID          uuid.UUID
	identityKind     string
	eventType        string
	aggregateType    string
	aggregateID      string
	aggregateVersion int64
	payloadHash      string
	occurredAt       time.Time
	chainID          *int64
	blockNumber      *int64
	blockHash        *string
	txHash           *string
	logIndex         *int
	recoveryVersion  *int64
	revisesEventID   *uuid.UUID
	sourceKind       string
	sourceID         string
	sourceVersion    *int64
	publishState     string
	lastErrorClass   *string
	publishedAt      *time.Time
	createdAt        time.Time
}

// materializeEventObservations converts raw rows into deduplicated
// observations. Rows sharing an event_id cannot exist (outbox UNIQUE), but a
// defensive duplicate is refused as a shape problem instead of inflating the
// evidence. Shape surprises are recorded and keep the bundle incomplete; they
// never crash the scan and never fabricate a fact.
func materializeEventObservations(rows []eventStateOutboxRow, evidence *EventStateEvidence, errs []error) ([]EventDeliveryObservation, []error) {
	observations := make([]EventDeliveryObservation, 0, len(rows))
	seen := make(map[uuid.UUID]struct{}, len(rows))
	for i := range rows {
		row := rows[i]
		if row.eventID != uuid.Nil {
			if _, dup := seen[row.eventID]; dup {
				evidence.mark(EventDeliveryIncomplete, EventReasonUnknownRowShape)
				errs = append(errs, contractErrorf("event state read returned duplicate event_id %s", row.eventID))
				continue
			}
			seen[row.eventID] = struct{}{}
		}
		observation := EventDeliveryObservation{
			EventID:          row.eventID,
			IdentityKind:     events.IdentityKind(row.identityKind),
			EventType:        row.eventType,
			AggregateType:    row.aggregateType,
			AggregateID:      row.aggregateID,
			AggregateVersion: row.aggregateVersion,
			PayloadHash:      row.payloadHash,
			OccurredAt:       row.occurredAt,
			EmittedAt:        row.createdAt,
			ChainID:          row.chainID,
			BlockNumber:      row.blockNumber,
			BlockHash:        row.blockHash,
			TxHash:           row.txHash,
			LogIndex:         row.logIndex,
			RecoveryVersion:  row.recoveryVersion,
			RevisesEventID:   row.revisesEventID,
			SourceKind:       row.sourceKind,
			SourceID:         row.sourceID,
			SourceVersion:    row.sourceVersion,
			PublishState:     events.PublishState(row.publishState),
			PublishedAt:      row.publishedAt,
			Duplicate:        EventDuplicateNone,
			PendingReason:    "consumer_result_pending",
		}
		if row.lastErrorClass != nil {
			observation.LastErrorClass = *row.lastErrorClass
		}
		if row.eventID == uuid.Nil {
			observation.ShapeProblems = append(observation.ShapeProblems, "missing event_id")
		}
		if !observation.IdentityKind.Valid() {
			observation.ShapeProblems = append(observation.ShapeProblems, "unknown identity_kind")
		}
		if !observation.PublishState.Valid() {
			observation.ShapeProblems = append(observation.ShapeProblems, "unknown publish_state")
		}
		if _, found := events.LookupEventSpec(row.eventType); !found {
			observation.ShapeProblems = append(observation.ShapeProblems, "event_type not in the frozen catalog")
		}
		if observation.AggregateType == "" || observation.AggregateID == "" || observation.AggregateVersion <= 0 {
			observation.ShapeProblems = append(observation.ShapeProblems, "incomplete aggregate identity")
		}
		if observation.PayloadHash == "" {
			observation.ShapeProblems = append(observation.ShapeProblems, "missing payload_hash")
		}
		if len(observation.ShapeProblems) > 0 {
			observation.MissingEvidence = append(observation.MissingEvidence, observation.ShapeProblems...)
			evidence.mark(EventDeliveryIncomplete, EventReasonUnknownRowShape)
		}
		observations = append(observations, observation)
	}
	return observations, errs
}

// markEventObservationReasons surfaces per-event shape problems, duplicate
// verdicts, quarantines and unclosed deliveries as bounded bundle reasons (one
// reason per class, not per event).
func markEventObservationReasons(evidence *EventStateEvidence) {
	for i := range evidence.Observations {
		observation := &evidence.Observations[i]
		if len(observation.ShapeProblems) > 0 {
			evidence.mark(EventDeliveryIncomplete, EventReasonUnknownRowShape)
		}
		switch observation.Duplicate {
		case EventDuplicateDivergent:
			evidence.mark(EventDeliveryIncomplete, EventReasonDuplicateDivergent)
		case EventDuplicateUnverifiable:
			evidence.mark(EventDeliveryIncomplete, EventReasonDuplicateUnverifiable)
		case EventDuplicateAbsorbed:
			evidence.addReason(EventReasonDuplicateAbsorbed)
		}
		if observation.PublishState == events.PublishStateBlocked {
			evidence.mark(EventDeliveryIncomplete, EventReasonPublishBlocked)
		}
		if len(observation.Quarantines) > 0 {
			evidence.mark(EventDeliveryIncomplete, EventReasonQuarantineOpen)
		}
		if !observation.Terminal {
			evidence.mark(EventDeliveryIncomplete, EventReasonDeliveryNotClosed)
			switch observation.PendingReason {
			case "publish_pending":
				evidence.addReason(EventReasonPublishPending)
			case "version_gap_unconfirmed":
				evidence.addReason(EventReasonVersionGapUnconfirmed)
			}
		}
	}
}

// applyConsumerEvidence reads consumer results (inbox, versions, quarantine,
// progress) for the scoped events and folds them into the observations. It
// applies the claim that one physical source is one evidence item: all rows of
// one event are folded into its single observation.
func (a *EventStateAdapter) applyConsumerEvidence(ctx context.Context, q EventStateQuery,
	observations []EventDeliveryObservation, evidence *EventStateEvidence,
	fail func(EventDeliveryStatus, string, error)) ([]EventDeliveryObservation, error) {

	ids := make([]uuid.UUID, 0, len(observations))
	for i := range observations {
		if observations[i].EventID != uuid.Nil {
			ids = append(ids, observations[i].EventID)
		}
	}

	// consumer_inbox: the durable idempotency/effect records. A failed read
	// leaves every application relation unknown: the event party cannot be
	// compared without it.
	facts := eventReadFacts{applicationsReadable: true, watermarksReadable: true}
	applications, truncated, err := a.readEventApplications(ctx, ids, len(q.Consumers))
	if err != nil {
		facts.applicationsReadable = false
		fail(EventDeliveryUnknown, EventReasonSourceReadFailed, fmt.Errorf("read consumer_inbox: %w", err))
	} else if truncated {
		facts.applicationsReadable = false
		evidence.mark(EventDeliveryIncomplete, EventReasonScopeTruncated)
		return observations, contractErrorf("consumer_inbox rows exceed the bounded read for %d events", len(ids))
	} else if len(applications) > 0 {
		if err := evidence.addEvidence(EventSourceInbox,
			fmt.Sprintf("events=%d/applications=%d", len(ids), len(applications))); err != nil {
			return observations, err
		}
	}
	applicationsByEvent := make(map[uuid.UUID][]EventApplication, len(applications))
	for _, application := range applications {
		applicationsByEvent[application.EventID] = append(applicationsByEvent[application.EventID], application)
	}

	// consumer_versions: the version-guard watermarks for the scoped
	// aggregates and the registered consumers.
	consumerNames := make([]string, 0, len(q.Consumers))
	for _, consumer := range q.Consumers {
		consumerNames = append(consumerNames, consumer.Name)
	}
	for _, application := range applications {
		if !containsString(consumerNames, application.ConsumerName) {
			consumerNames = append(consumerNames, application.ConsumerName)
		}
	}
	sort.Strings(consumerNames)
	watermarks, truncated, err := a.readEventWatermarks(ctx, observations, consumerNames)
	if err != nil {
		facts.watermarksReadable = false
		fail(EventDeliveryIncomplete, EventReasonSourceReadFailed, fmt.Errorf("read consumer_versions: %w", err))
	} else if truncated {
		facts.watermarksReadable = false
		evidence.mark(EventDeliveryIncomplete, EventReasonScopeTruncated)
	} else if len(watermarks) > 0 {
		if err := evidence.addEvidence(EventSourceVersions,
			fmt.Sprintf("consumers=%d/watermarks=%d", len(consumerNames), len(watermarks))); err != nil {
			return observations, err
		}
	}

	// consumer_quarantine + consumer_progress per registered consumer.
	consumerStates, consumerReadability, readFailures := a.readConsumerStates(ctx, q, applications)
	for _, readErr := range readFailures {
		fail(EventDeliveryIncomplete, EventReasonProgressUnknown, readErr)
	}
	evidence.Consumers = consumerStates

	quarantines := make(map[eventQuarantineKey]EventQuarantineSignal)
	for i := range consumerStates {
		for j := range consumerStates[i].QuarantineSignals {
			signal := consumerStates[i].QuarantineSignals[j]
			quarantines[eventQuarantineKey{signal.ConsumerName, signal.EventID}] = signal
		}
	}
	if len(quarantines) > 0 {
		if err := evidence.addEvidence(EventSourceQuarantine,
			fmt.Sprintf("open_signals=%d", len(quarantines))); err != nil {
			return observations, err
		}
	}

	// Fold evidence into the observations and evaluate the delivery outcome.
	evaluateEventDeliveries(observations, consumerNames, applicationsByEvent, watermarks,
		quarantines, consumerReadability, facts, evidence.CapturedAt, evidence.Config.FreshnessTolerance)
	applySharedSourceDeliveries(observations)
	return observations, nil
}

// readEventApplications reads the consumer_inbox rows of the scoped events,
// bounded by events × (registered consumers + 1) + 1 so unexpected consumer
// rows are still visible and a truncation is detectable.
func (a *EventStateAdapter) readEventApplications(ctx context.Context, ids []uuid.UUID, consumerCount int) ([]EventApplication, bool, error) {
	if len(ids) == 0 {
		return nil, false, nil
	}
	limit := len(ids)*(consumerCount+1) + 1
	rows, err := a.pool.Query(ctx, eventStateInboxSelectSQL, ids, limit)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var out []EventApplication
	for rows.Next() {
		var application EventApplication
		if err := rows.Scan(&application.ConsumerName, &application.EventID,
			&application.AggregateType, &application.AggregateID, &application.AggregateVersion,
			&application.Topic, &application.Partition, &application.Offset, &application.AppliedAt); err != nil {
			return nil, false, err
		}
		out = append(out, application)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if len(out) > limit-1 {
		return out[:limit-1], true, nil
	}
	return out, false, nil
}

// readEventWatermarks reads the consumer_versions rows of the scoped
// aggregates and the registered consumers in bounded chunks. A failed or
// truncated read returns readable=false (via the caller) so every version
// relation stays unknown instead of guessing.
func (a *EventStateAdapter) readEventWatermarks(ctx context.Context, observations []EventDeliveryObservation,
	consumerNames []string) ([]EventVersionWatermark, bool, error) {

	if len(consumerNames) == 0 {
		return nil, false, nil
	}
	tuples := make([]eventVersionKey, 0, len(observations))
	seen := make(map[eventVersionKey]struct{}, len(observations))
	for i := range observations {
		observation := &observations[i]
		if observation.AggregateType == "" || observation.AggregateID == "" {
			continue
		}
		key := eventVersionKey{AggregateType: observation.AggregateType, AggregateID: observation.AggregateID}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		tuples = append(tuples, key)
	}
	if len(tuples) == 0 {
		return nil, false, nil
	}

	var out []EventVersionWatermark
	for start := 0; start < len(tuples); start += eventStateWatermarkChunk {
		end := min(start+eventStateWatermarkChunk, len(tuples))
		chunk := tuples[start:end]
		expected := len(chunk) * len(consumerNames)
		sql := fmt.Sprintf(eventStateWatermarkSelectSQL, eventStateWatermarkPairs(len(chunk)),
			len(chunk)*2+1, len(chunk)*2+2)
		args := make([]any, 0, len(chunk)*2+2)
		for _, tuple := range chunk {
			args = append(args, tuple.AggregateType, tuple.AggregateID)
		}
		args = append(args, consumerNames, expected+1)

		rows, err := a.pool.Query(ctx, sql, args...)
		if err != nil {
			return nil, false, err
		}
		chunkCount := 0
		for rows.Next() {
			var watermark EventVersionWatermark
			if err := rows.Scan(&watermark.ConsumerName, &watermark.AggregateType,
				&watermark.AggregateID, &watermark.MaxVersion, &watermark.UpdatedAt); err != nil {
				rows.Close()
				return nil, false, err
			}
			out = append(out, watermark)
			chunkCount++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, false, err
		}
		if chunkCount > expected {
			// A truncated set cannot be trusted for absorption analysis.
			return nil, true, nil
		}
	}
	return out, false, nil
}

// readConsumerStates reads quarantine and durable progress of every registered
// consumer, bounded by the consumer list. Progress absent while applications
// exist is double-checked with ResumeOffset (consumer.go:914): a missing
// durable resume point is a read failure, never "no activity".
func (a *EventStateAdapter) readConsumerStates(ctx context.Context, q EventStateQuery,
	applications []EventApplication) ([]EventConsumerState, map[string]bool, []error) {

	states := make([]EventConsumerState, 0, len(q.Consumers))
	readability := make(map[string]bool, len(q.Consumers))
	applicationsByConsumer := make(map[string][]EventApplication)
	for _, application := range applications {
		applicationsByConsumer[application.ConsumerName] = append(applicationsByConsumer[application.ConsumerName], application)
	}

	var failures []error
	for i := range q.Consumers {
		registration := q.Consumers[i]
		state := EventConsumerState{Name: registration.Name}
		progressOK := true
		quarantineOK := true

		openCount, err := q.Quarantine.OpenCount(ctx, registration.Name)
		if err != nil {
			quarantineOK = false
			failures = append(failures, fmt.Errorf("read consumer_quarantine count of %q: %w", registration.Name, err))
		} else {
			state.OpenQuarantine = openCount
			if openCount > 0 {
				entries, err := q.Quarantine.ListOpen(ctx, registration.Name)
				if err != nil {
					quarantineOK = false
					failures = append(failures, fmt.Errorf("read consumer_quarantine entries of %q: %w", registration.Name, err))
				} else {
					state.foldQuarantine(entries)
				}
			}
		}

		progress, err := registration.Progress.ReadProgress(ctx)
		if err != nil {
			progressOK = false
			failures = append(failures, fmt.Errorf("read consumer_progress of %q: %w", registration.Name, err))
		} else {
			state.Progress = progress
			state.ProgressKnown = len(progress) > 0
			if state.ProgressKnown {
				latest := progress[0].UpdatedAt
				for j := range progress {
					if progress[j].UpdatedAt.After(latest) {
						latest = progress[j].UpdatedAt
					}
				}
				state.ProgressAge = ageAt(latest, time.Now().UTC())
			}
		}
		if !state.ProgressKnown && len(applicationsByConsumer[registration.Name]) > 0 {
			checks, checkFailures := a.verifyResumePoints(ctx, registration, applicationsByConsumer[registration.Name])
			state.ResumeChecks = checks
			if len(checkFailures) > 0 {
				progressOK = false
				failures = append(failures, checkFailures...)
			}
		}

		readability[registration.Name] = progressOK && quarantineOK
		states = append(states, state)
	}
	return states, readability, failures
}

// verifyResumePoints double-checks consumer.go:914 ResumeOffset for the
// distinct (topic, partition) coordinates that already show an application for
// this consumer when ReadProgress returned nothing. A missing resume point is
// a read failure (incomplete), not "no activity".
func (a *EventStateAdapter) verifyResumePoints(ctx context.Context, registration EventConsumerRegistration,
	applications []EventApplication) ([]EventResumeCheck, []error) {

	type partitionKey struct {
		topic     string
		partition int
	}
	seen := make(map[partitionKey]struct{})
	var checks []EventResumeCheck
	var failures []error
	for _, application := range applications {
		key := partitionKey{topic: application.Topic, partition: application.Partition}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		nextOffset, found, err := registration.Progress.ResumeOffset(ctx, application.Topic, application.Partition)
		if err != nil {
			failures = append(failures, fmt.Errorf("read resume offset of %q %s/%d: %w",
				registration.Name, application.Topic, application.Partition, err))
			continue
		}
		checks = append(checks, EventResumeCheck{
			Topic:      application.Topic,
			Partition:  application.Partition,
			Found:      found,
			NextOffset: nextOffset,
		})
		if !found {
			failures = append(failures, fmt.Errorf("consumer %q has applications on %s/%d but no durable progress row",
				registration.Name, application.Topic, application.Partition))
		}
	}
	return checks, failures
}

// foldQuarantine keeps the open quarantine entries of the scoped events and
// records their provenance. The operation is a pure filter: the entries were
// already read through the existing quarantine.go reads.
func (s *EventConsumerState) foldQuarantine(entries []events.QuarantineEntry) {
	signals := make([]EventQuarantineSignal, 0, len(entries))
	for i := range entries {
		entry := entries[i]
		if entry.Status != events.QuarantineStatusOpen {
			continue
		}
		signal := EventQuarantineSignal{
			ConsumerName: entry.ConsumerName,
			EventID:      entry.EventID,
			FailureClass: entry.FailureClass,
			AttemptCount: entry.AttemptCount,
			FirstSeenAt:  entry.FirstSeenAt,
			LastSeenAt:   entry.LastSeenAt,
			SourceTopic:  entry.SourceTopic,
		}
		if entry.SourcePartition >= 0 {
			partition := entry.SourcePartition
			signal.SourcePartition = &partition
		}
		if entry.SourceOffset >= 0 {
			offset := entry.SourceOffset
			signal.SourceOffset = &offset
		}
		signals = append(signals, signal)
	}
	s.QuarantineSignals = signals
	s.ScopedQuarantine = len(signals)
}

// evaluateEventDeliveries folds the consumer evidence into the observations
// and derives the Q4 verdict, terminality and pending reason of each event.
//
// Absorption is only asserted when the event was actually published, the
// delivery has settled past the freshness tolerance (a still-in-flight
// delivery must not be rendered absorbed) and every observed consumer relation
// is a legal version-guard supersession.
func evaluateEventDeliveries(observations []EventDeliveryObservation, consumerNames []string,
	applicationsByEvent map[uuid.UUID][]EventApplication,
	watermarks []EventVersionWatermark,
	quarantines map[eventQuarantineKey]EventQuarantineSignal,
	consumerReadability map[string]bool,
	facts eventReadFacts,
	capturedAt time.Time, tolerance time.Duration) {

	watermarkIndex := make(map[eventVersionKey]EventVersionWatermark, len(watermarks))
	for _, watermark := range watermarks {
		watermarkIndex[eventVersionKey{watermark.ConsumerName, watermark.AggregateType, watermark.AggregateID}] = watermark
	}

	names := append([]string(nil), consumerNames...)
	sort.Strings(names)

	for i := range observations {
		observation := &observations[i]
		observation.Applications = sortedApplications(applicationsByEvent[observation.EventID])
		observation.Watermarks = watermarksForAggregate(watermarks, observation.AggregateType, observation.AggregateID)
		observation.Quarantines = quarantinesForEvent(quarantines, observation.EventID)

		settled := false
		if observation.PublishState == events.PublishStatePublished {
			base := observation.EmittedAt
			if observation.PublishedAt != nil {
				base = *observation.PublishedAt
			}
			settled = tolerance > 0 && capturedAt.Sub(base) > tolerance
		}

		anyApplied := false
		anyQuarantine := false
		anyUnknown := false
		anyDivergent := false
		allSuperseded := len(names) > 0
		pendingReason := ""

		observation.PendingReason = ""
		observation.Consumers = observation.Consumers[:0]
		for _, name := range names {
			relation := eventRelation(observation, name, applicationsByEvent[observation.EventID],
				watermarkIndex, quarantines, consumerReadability, facts)
			if relation.Contradiction != EventContradictionNone {
				anyDivergent = true
			}
			switch relation.Relation {
			case EventVersionApplied:
				anyApplied = true
				allSuperseded = false
			case EventVersionSuperseded:
				// contributes to allSuperseded
			case EventVersionUnknown:
				anyUnknown = true
				allSuperseded = false
			default:
				allSuperseded = false
				if pendingReason == "" {
					pendingReason = eventPendingReason(relation.Relation)
				}
			}
			if relation.Quarantined {
				anyQuarantine = true
			}
			if relation.Relation == EventVersionUnknown {
				observation.MissingEvidence = append(observation.MissingEvidence,
					"consumer "+name+": version or liveness evidence unavailable")
			}
			observation.Consumers = append(observation.Consumers, relation)
		}
		absorbed := allSuperseded && settled

		switch {
		case anyDivergent:
			observation.Duplicate = EventDuplicateDivergent
		case anyUnknown:
			observation.Duplicate = EventDuplicateUnverifiable
		case anyApplied || anyQuarantine:
			observation.Duplicate = EventDuplicateNone
		case absorbed:
			observation.Duplicate = EventDuplicateAbsorbed
		default:
			observation.Duplicate = EventDuplicateNone
		}

		switch {
		case anyDivergent, anyQuarantine, anyApplied, absorbed:
			observation.Terminal = true
		case observation.PublishState == events.PublishStateBlocked:
			observation.Terminal = true
		case observation.PublishState == events.PublishStatePending:
			observation.Terminal = false
			observation.PendingReason = "publish_pending"
		case anyUnknown:
			observation.Terminal = false
			observation.PendingReason = "consumer_evidence_unknown"
		case len(names) == 0:
			observation.Terminal = false
			observation.PendingReason = "no_consumer_registered"
		default:
			observation.Terminal = false
			switch {
			case pendingReason != "":
				observation.PendingReason = pendingReason
			case allSuperseded:
				// The watermark is ahead but the delivery has not settled
				// past the freshness tolerance yet: still in flight.
				observation.PendingReason = "delivery_in_flight"
			}
		}
		if !observation.Terminal && observation.PendingReason == "" {
			observation.PendingReason = "consumer_result_pending"
		}
	}
}

// eventRelation derives one consumer's relation to one emitted event.
func eventRelation(observation *EventDeliveryObservation, consumerName string,
	applications []EventApplication, watermarks map[eventVersionKey]EventVersionWatermark,
	quarantines map[eventQuarantineKey]EventQuarantineSignal,
	consumerReadability map[string]bool,
	facts eventReadFacts) EventConsumerRelation {

	relation := EventConsumerRelation{ConsumerName: consumerName}
	application, hasApplication := findEventApplication(applications, consumerName)
	if !facts.applicationsReadable {
		hasApplication = false
	}
	watermark, hasWatermark := watermarks[eventVersionKey{consumerName, observation.AggregateType, observation.AggregateID}]
	if !facts.watermarksReadable {
		hasWatermark = false
	}
	if hasWatermark {
		relation.HasWatermark = true
		relation.MaxVersion = watermark.MaxVersion
	}
	if quarantine, hasQuarantine := quarantines[eventQuarantineKey{consumerName, observation.EventID}]; hasQuarantine {
		relation.Quarantined = true
		relation.QuarantineFailureClass = string(quarantine.FailureClass)
	}

	switch {
	case hasApplication:
		appliedAt := application.AppliedAt
		relation.AppliedAt = &appliedAt
		relation.AppliedVersion = application.AggregateVersion
		relation.Relation = EventVersionApplied
		switch {
		case application.AggregateType != observation.AggregateType ||
			application.AggregateID != observation.AggregateID:
			relation.Contradiction = EventContradictionContentMismatch
			relation.ContradictionDetail = "applied aggregate identity differs from the emitted event"
		case application.AggregateVersion != observation.AggregateVersion:
			relation.Contradiction = EventContradictionContentMismatch
			relation.ContradictionDetail = "applied aggregate version differs from the emitted event"
		case relation.HasWatermark && application.AggregateVersion > watermark.MaxVersion:
			relation.Contradiction = EventContradictionVersionRegression
			relation.ContradictionDetail = "applied version is ahead of the durable version watermark"
		case !relation.HasWatermark && facts.watermarksReadable:
			relation.Contradiction = EventContradictionWatermarkMissing
			relation.ContradictionDetail = "applied event has no durable version watermark"
		}
	case !facts.applicationsReadable:
		relation.Relation = EventVersionUnknown
	case !facts.watermarksReadable:
		relation.Relation = EventVersionUnknown
	case hasWatermark:
		switch {
		case observation.AggregateVersion <= watermark.MaxVersion:
			relation.Relation = EventVersionSuperseded
		case observation.AggregateVersion == watermark.MaxVersion+1:
			relation.Relation = EventVersionNext
		default:
			relation.Relation = EventVersionGap
		}
	case consumerReadability[consumerName]:
		relation.Relation = EventVersionUnapplied
	default:
		relation.Relation = EventVersionUnknown
	}
	return relation
}

// eventPendingReason maps a non-terminal version relation onto a bounded
// pending token.
func eventPendingReason(relation EventVersionRelation) string {
	switch relation {
	case EventVersionNext:
		return "delivery_in_flight"
	case EventVersionGap:
		return "version_gap_unconfirmed"
	case EventVersionUnapplied:
		return "not_consumed"
	case EventVersionUnknown:
		return "consumer_evidence_unknown"
	default:
		return ""
	}
}

// applySharedSourceDeliveries records how many in-scope rows share one source
// watermark. It is a delivery-count hint only; T017 never treats a count as a
// fund anomaly (Q4).
func applySharedSourceDeliveries(observations []EventDeliveryObservation) {
	counts := make(map[string]int, len(observations))
	keys := make([]string, len(observations))
	for i := range observations {
		observation := &observations[i]
		if observation.SourceKind == "" || observation.SourceID == "" || observation.SourceVersion == nil {
			continue
		}
		key := observation.SourceKind + "\x00" + observation.SourceID + "\x00" + strconv.FormatInt(*observation.SourceVersion, 10)
		keys[i] = key
		counts[key]++
	}
	for i := range observations {
		if keys[i] != "" {
			observations[i].SharedSourceDeliveries = counts[keys[i]]
		}
	}
}

// applyScopedBacklog records the scoped pending/blocked counts and the oldest
// pending age.
func applyScopedBacklog(evidence *EventStateEvidence, observations []EventDeliveryObservation) {
	for i := range observations {
		observation := &observations[i]
		switch observation.PublishState {
		case events.PublishStatePending:
			evidence.Backlog.ScopedPending++
			if evidence.Backlog.OldestPendingAt == nil || observation.EmittedAt.Before(*evidence.Backlog.OldestPendingAt) {
				emittedAt := observation.EmittedAt
				evidence.Backlog.OldestPendingAt = &emittedAt
			}
		case events.PublishStateBlocked:
			evidence.Backlog.ScopedBlocked++
		}
	}
}

// applyEventObservationSummary fills the bounded summary counters.
func applyEventObservationSummary(evidence *EventStateEvidence) {
	for i := range evidence.Observations {
		observation := &evidence.Observations[i]
		evidence.Summary.Observations++
		if observation.Terminal {
			evidence.Summary.Terminal++
		} else {
			evidence.Summary.Unclosed++
		}
		switch observation.Duplicate {
		case EventDuplicateAbsorbed:
			evidence.Summary.AbsorbedDuplicates++
		case EventDuplicateDivergent:
			evidence.Summary.DivergentDuplicates++
		case EventDuplicateUnverifiable:
			evidence.Summary.UnverifiableDuplicates++
		}
		for _, relation := range observation.Consumers {
			if relation.Relation == EventVersionSuperseded {
				evidence.Summary.SupersededRelations++
			}
		}
		evidence.Summary.OpenQuarantines += len(observation.Quarantines)
		if observation.PublishState == events.PublishStateBlocked {
			evidence.Summary.BlockedPublishes++
		}
	}
}

// applyFreshness enforces FR-005: pending backlog or unclosed delivery
// evidence older than the tolerance marks the bundle stale, and an unclosed
// delivery whose consumer progress cannot be read marks freshness unknown.
// Neither ever triggers automatic repair.
func (a *EventStateAdapter) applyFreshness(evidence *EventStateEvidence, q EventStateQuery,
	observations []EventDeliveryObservation, now time.Time) {

	freshness := &evidence.Freshness
	for i := range observations {
		observation := &observations[i]
		if observation.PublishState == events.PublishStatePending {
			freshness.HasPending = true
			if age := ageAt(observation.EmittedAt, now); age > freshness.OldestPendingAge {
				freshness.OldestPendingAge = age
			}
		}
		if !observation.Terminal {
			freshness.HasUnclosed = true
			if age := ageAt(observation.EmittedAt, now); age > freshness.OldestUnclosedAge {
				freshness.OldestUnclosedAge = age
			}
		}
	}
	switch {
	case freshness.HasPending && freshness.OldestPendingAge > freshness.Tolerance:
		freshness.Stale = true
		appendUniqueString(&freshness.Reasons, "pending_backlog_age_exceeds_tolerance")
	case freshness.HasUnclosed && freshness.OldestUnclosedAge > freshness.Tolerance:
		freshness.Stale = true
		appendUniqueString(&freshness.Reasons, "unclosed_delivery_age_exceeds_tolerance")
	}
	if freshness.Stale {
		evidence.mark(EventDeliveryStale, EventReasonStale)
	}

	// A consumer whose progress cannot be read while deliveries are still
	// unclosed leaves freshness unproven; it never silently counts as "idle".
	if !freshness.HasUnclosed {
		return
	}
	progressKnown := make(map[string]bool, len(evidence.Consumers))
	for i := range evidence.Consumers {
		progressKnown[evidence.Consumers[i].Name] = evidence.Consumers[i].ProgressKnown
	}
	for _, consumer := range q.Consumers {
		if !progressKnown[consumer.Name] {
			freshness.Unknown = true
			appendUniqueString(&freshness.Reasons, "consumer_progress_unknown:"+consumer.Name)
		}
	}
	if freshness.Unknown {
		evidence.mark(EventDeliveryIncomplete, EventReasonProgressUnknown)
	}
}

// applyUpstream enforces FR-006 declaration semantics, identical in substance
// to the chain-facts adapter's applyUpstream: a missing declaration for a
// scope business type counts as unconfigured, a declaration with
// Connected=false counts as unconnected, and either downgrades the bundle to
// incomplete. A connected declaration still never proves upstream success;
// the ExternalCredit verdict itself is derived by the classifier from
// Observation.Upstream, not from this status (see CoverageClosed).
func (a *EventStateAdapter) applyUpstream(evidence *EventStateEvidence) {
	declared := make(map[BusinessType]ChainUpstreamReceiptSource, len(evidence.UpstreamReceipts))
	for _, item := range evidence.UpstreamReceipts {
		declared[item.BusinessType] = item
	}
	for _, businessType := range evidence.Scope.BusinessTypes {
		item, found := declared[businessType]
		if !found {
			evidence.mark(EventDeliveryIncomplete, EventReasonReceiptUnconfigured)
			continue
		}
		if !item.Connected {
			evidence.mark(EventDeliveryIncomplete, EventReasonReceiptUnconnected)
		}
	}
	evidence.addReason(EventReasonExternalCreditUnprovable)
}

// readGlobalBacklog reads the global pending-by-family backlog (outbox.go
// CapacitySQL) and the blocked backlog (outbox.go BlockedCountSQL).
func (a *EventStateAdapter) readGlobalBacklog(ctx context.Context) (EventBacklogState, error) {
	var backlog EventBacklogState
	rows, err := a.pool.Query(ctx, events.CapacitySQL)
	if err != nil {
		return backlog, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			family      string
			pending     int64
			oldestEpoch float64
		)
		if err := rows.Scan(&family, &pending, &oldestEpoch); err != nil {
			return backlog, err
		}
		backlog.GlobalFamilies = append(backlog.GlobalFamilies, EventFamilyBacklog{
			Family:    family,
			Pending:   pending,
			OldestAge: secondsToDuration(oldestEpoch),
		})
	}
	if err := rows.Err(); err != nil {
		return backlog, err
	}
	if err := a.pool.QueryRow(ctx, events.BlockedCountSQL).Scan(&backlog.GlobalBlocked); err != nil {
		return backlog, err
	}
	return backlog, nil
}

// secondsToDuration converts an observation-only epoch-seconds value into a
// duration. It is never used for money or quantities.
func secondsToDuration(seconds float64) time.Duration {
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}

// ageAt returns now - at, clamped at zero.
func ageAt(at, now time.Time) time.Duration {
	if at.IsZero() || now.Before(at) {
		return 0
	}
	return now.Sub(at)
}

// eventStateChainID parses the scope chain id into the numeric EVM chain id
// used by outbox_events.chain_id.
func eventStateChainID(chainID string) (int64, bool) {
	parsed, err := strconv.ParseInt(strings.TrimSpace(chainID), 10, 64)
	if err != nil || parsed <= 0 {
		return 0, false
	}
	return parsed, true
}

// eventStateIntervalRef renders one interval for provenance refs/audit.
func eventStateIntervalRef(interval EventStateInterval) string {
	if interval.Kind() == ScopeTime {
		return fmt.Sprintf("time=%d..%d", interval.From.Time.UnixMicro(), interval.To.Time.UnixMicro())
	}
	return fmt.Sprintf("height=%d..%d", interval.From.Height, interval.To.Height)
}

// sortedApplications returns a consumer-name-ordered copy.
func sortedApplications(applications []EventApplication) []EventApplication {
	out := append([]EventApplication(nil), applications...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].ConsumerName != out[j].ConsumerName {
			return out[i].ConsumerName < out[j].ConsumerName
		}
		return out[i].AppliedAt.Before(out[j].AppliedAt)
	})
	return out
}

// findEventApplication returns the application of one consumer.
func findEventApplication(applications []EventApplication, consumerName string) (EventApplication, bool) {
	for _, application := range applications {
		if application.ConsumerName == consumerName {
			return application, true
		}
	}
	return EventApplication{}, false
}

// watermarksForAggregate returns the consumer-ordered watermarks of one
// aggregate.
func watermarksForAggregate(watermarks []EventVersionWatermark, aggregateType, aggregateID string) []EventVersionWatermark {
	var out []EventVersionWatermark
	for _, watermark := range watermarks {
		if watermark.AggregateType == aggregateType && watermark.AggregateID == aggregateID {
			out = append(out, watermark)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ConsumerName < out[j].ConsumerName })
	return out
}

// quarantinesForEvent returns the event's open quarantine signals in consumer
// order.
func quarantinesForEvent(quarantines map[eventQuarantineKey]EventQuarantineSignal, eventID uuid.UUID) []EventQuarantineSignal {
	var out []EventQuarantineSignal
	for key, signal := range quarantines {
		if key.EventID == eventID {
			out = append(out, signal)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ConsumerName < out[j].ConsumerName })
	return out
}

// containsString reports membership in a plain string slice.
func containsString(values []string, target string) bool {
	return slices.Contains(values, target)
}

// appendUniqueString appends value when absent.
func appendUniqueString(values *[]string, value string) {
	if slices.Contains(*values, value) {
		return
	}
	*values = append(*values, value)
}

// eventStateWatermarkPairs renders the row-value pairs of one watermark chunk.
func eventStateWatermarkPairs(count int) string {
	pairs := make([]string, 0, count)
	for i := range count {
		pairs = append(pairs, fmt.Sprintf("($%d,$%d)", 2*i+1, 2*i+2))
	}
	return strings.Join(pairs, ",")
}

// SQL statement constants. Table and column names follow migration 000015
// (outbox_events, consumer_*) and keep the T4 transaction's ordering semantics
// (consumer.go:569-637). Every statement is a bounded SELECT.

const eventStateOutboxSelectSQL = `
SELECT event_id, identity_kind, event_type, aggregate_type, aggregate_id,
       aggregate_version, payload_hash, occurred_at, chain_id, block_number,
       block_hash, tx_hash, log_index, recovery_version, revises_event_id,
       COALESCE(source_kind, ''), COALESCE(source_id, ''), source_version,
       publish_state, last_error_class, published_at, created_at
FROM outbox_events
WHERE %s
ORDER BY occurred_at, event_id
LIMIT $%d`

const eventStateInboxSelectSQL = `
SELECT consumer_name, event_id, aggregate_type, aggregate_id, aggregate_version,
       topic, partition, "offset", applied_at
FROM consumer_inbox
WHERE event_id = ANY($1)
ORDER BY consumer_name, event_id
LIMIT $2`

// eventStateWatermarkSelectSQL takes the row-value pairs, the consumer-name
// array and the expected+1 bound; it is built per bounded chunk.
const eventStateWatermarkSelectSQL = `
SELECT consumer_name, aggregate_type, aggregate_id, max_version, updated_at
FROM consumer_versions
WHERE (aggregate_type, aggregate_id) IN (%s)
  AND consumer_name = ANY($%d)
ORDER BY consumer_name, aggregate_type, aggregate_id
LIMIT $%d`
