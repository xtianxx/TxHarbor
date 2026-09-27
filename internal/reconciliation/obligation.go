// obligation.go implements the T040 expected-event discriminator evidence
// surface: the read-only 014 side of the producer-written expectation carrier
// (event_obligation, migration 000017) plus the R1/R2/R3 verdict of
// specs/014-reconciliation-exception-handling/expected-event-discriminator.md
// §2.
//
// The discriminator answers exactly one question for one candidate whose event
// delivery is absent while its chain fact and PG business record are present:
// was an event due? It answers with one of three verdicts and never with a
// fourth "accepted approximation":
//
//   - R1 ObligationProven: a durable expectation marker exists for the
//     candidate's event aggregate and the absence cannot be explained by any
//     audited retention prune → the event delivery is a genuine loss; the
//     caller keeps the existing fail-closed `missing` ticket.
//   - R2 ObligationNotApplicable: the candidate identity provably carries no
//     catalog event aggregate (frozen 013 catalog) → the event dimension is
//     N/A; chain/PG differences stay independently classified and are never
//     downgraded because of the event dimension.
//   - R3 ObligationUnproven: no marker, unreadable/truncated evidence, or an
//     audited legal trim could explain the absence → the observation stays
//     pending/gap, observable but never a `missing` claim. 标记缺席 ≠ N/A.
//
// The verdict function is pure (no I/O, no clock) so the matrix is directly
// testable; the adapter is the only database surface and performs bounded
// read-only SELECTs. It never writes: the carrier is written by the producer
// transaction (internal/events.Append), 014 only reads it.
package reconciliation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// EventObligationAggregate is one catalog event aggregate identity
// (aggregate_type, aggregate_id) as the discriminator reads it. The aggregate
// types are the frozen 013 catalog values (specs/013 data-model §2):
// deposit_observation, withdrawal_request, withdrawal_intent. They are spelled
// literally here so this package keeps no import on the business packages; a
// rename is a catalog change, never an implicit one.
type EventObligationAggregate struct {
	AggregateType string
	AggregateID   string
}

// Valid reports whether the aggregate carries a usable shape.
func (a EventObligationAggregate) Valid() bool {
	switch a.AggregateType {
	case "deposit_observation", "withdrawal_request", "withdrawal_intent":
	default:
		return false
	}
	return strings.TrimSpace(a.AggregateID) != "" && !strings.ContainsRune(a.AggregateID, 0)
}

// EventObligationQuery bounds one interval-level expectation read. Aggregates
// are the candidates' catalog event identities; a query without aggregates
// performs no read.
type EventObligationQuery struct {
	Aggregates []EventObligationAggregate
}

// ObligatedExpectation is one durable expectation marker row of the carrier.
type ObligatedExpectation struct {
	EventType        string
	AggregateVersion int64
	ObligatedAt      time.Time
	SourceKind       string
	SourceID         string
	SourceVersion    *int64
}

// RetentionPruneAudit is one audited events-admin retention prune observed in
// event_ops_audit: rows with publish_state='published' and
// published_at < PrunedAt-Window were deleted by that operator run. It is the
// only evidence that lets the discriminator separate "possibly legally
// trimmed" from a real loss.
type RetentionPruneAudit struct {
	PrunedAt time.Time
	Window   time.Duration
}

// cutoff returns the publish-time watermark of this prune: a published row
// older than the cutoff may have been deleted by it.
func (r RetentionPruneAudit) cutoff() (time.Time, bool) {
	if r.PrunedAt.IsZero() || r.Window <= 0 {
		return time.Time{}, false
	}
	return r.PrunedAt.Add(-r.Window), true
}

// EventObligationEvidence is the bounded read result of one interval. ReadOK
// false (and Truncated true) mean the evidence cannot be used: every decisive
// absence stays R3 pending, never missing. Missing evidence is never read as
// "no obligation".
type EventObligationEvidence struct {
	CapturedAt      time.Time
	ReadOK          bool
	Truncated       bool
	Expectations    map[EventObligationAggregate][]ObligatedExpectation
	RetentionAudits []RetentionPruneAudit
	// RetentionReadOK false means the audited prune history could not be
	// proven (read failure, malformed scope or truncation): a possible legal
	// trim cannot be ruled out, so the verdict stays conservative.
	RetentionReadOK bool
}

// EventObligationState is the three-way discriminator verdict vocabulary.
type EventObligationState string

const (
	// EventObligationProven: R1 — a durable expectation proves an event was
	// due and no audited trim explains its absence.
	EventObligationProven EventObligationState = "proven"
	// EventObligationNotApplicable: R2 — the candidate provably carries no
	// catalog event obligation; the event dimension is N/A.
	EventObligationNotApplicable EventObligationState = "not_applicable"
	// EventObligationUnproven: R3 — the expectation cannot be proven;
	// pending/gap, alert-only, never a missing claim.
	EventObligationUnproven EventObligationState = "unproven"
)

// Valid reports whether s is one of the closed verdict states.
func (s EventObligationState) Valid() bool {
	switch s {
	case EventObligationProven, EventObligationNotApplicable, EventObligationUnproven:
		return true
	}
	return false
}

// The bounded machine reason tokens of a verdict. They are audit/observability
// material (low cardinality, no secrets) and MUST NOT be reworded once
// published.
const (
	ObligationReasonMarker              = "expectation_marker"
	ObligationReasonNoCatalogAggregate  = "no_catalog_event_aggregate"
	ObligationReasonNoMarker            = "no_expectation_marker"
	ObligationReasonEvidenceUnavailable = "obligation_evidence_unavailable"
	ObligationReasonRetentionUnknown    = "retention_history_unprovable"
	ObligationReasonPossiblyTrimmed     = "possibly_retention_trimmed"
)

// EventObligationVerdict is one R1/R2/R3 verdict with its machine reason.
type EventObligationVerdict struct {
	State  EventObligationState
	Reason string
}

// ProvesMissing reports whether the verdict supports the existing fail-closed
// missing ticket (R1).
func (v EventObligationVerdict) ProvesMissing() bool {
	return v.State == EventObligationProven
}

// DiscriminateEventObligation applies the R1/R2/R3 matrix to one decisive
// event-only absence. aggregate is nil when the candidate's event identity
// provably carries no catalog event aggregate (R2); evidence is the interval's
// bounded read.
func DiscriminateEventObligation(aggregate *EventObligationAggregate, evidence EventObligationEvidence) EventObligationVerdict {
	if aggregate == nil {
		// The candidate identity itself proves there is no catalog event
		// obligation to attach (for example the chain-first tx_hash key:
		// no event aggregate exists for a bare transaction hash).
		return EventObligationVerdict{State: EventObligationNotApplicable, Reason: ObligationReasonNoCatalogAggregate}
	}
	if !evidence.ReadOK || evidence.Truncated {
		// The carrier could not be read/proven bounded: missing evidence is
		// never "no obligation".
		return EventObligationVerdict{State: EventObligationUnproven, Reason: ObligationReasonEvidenceUnavailable}
	}
	expectations := evidence.Expectations[*aggregate]
	if len(expectations) == 0 {
		// Old producers / pre-carrier history: 标记缺席 ≠ N/A.
		return EventObligationVerdict{State: EventObligationUnproven, Reason: ObligationReasonNoMarker}
	}
	if !evidence.RetentionReadOK {
		return EventObligationVerdict{State: EventObligationUnproven, Reason: ObligationReasonRetentionUnknown}
	}
	// The event stream of the aggregate is entirely absent. A legal trim can
	// only explain the absence when EVERY expected emission could have been
	// pruned; a single expectation newer than every prune cutoff proves the
	// absence is a real loss.
	for _, expectation := range expectations {
		if !retentionCouldExplain(expectation.ObligatedAt, evidence.RetentionAudits) {
			return EventObligationVerdict{State: EventObligationProven, Reason: ObligationReasonMarker}
		}
	}
	return EventObligationVerdict{State: EventObligationUnproven, Reason: ObligationReasonPossiblyTrimmed}
}

// retentionCouldExplain reports whether any audited prune could have deleted a
// published row committed at obligatedAt. published_at >= committed time, and
// a prune deletes published rows with published_at < PrunedAt-Window; the
// absence is explainable when the obligation instant itself lies before a
// prune cutoff. Anything unprovable (zero time, non-positive window) never
// explains a loss.
func retentionCouldExplain(obligatedAt time.Time, audits []RetentionPruneAudit) bool {
	if obligatedAt.IsZero() {
		// An unreadable obligation instant cannot support a loss claim.
		return true
	}
	for _, audit := range audits {
		cutoff, ok := audit.cutoff()
		if !ok {
			continue
		}
		if obligatedAt.Before(cutoff) {
			return true
		}
	}
	return false
}

// EventObligationAggregateOf maps one candidate event identity onto the
// catalog event aggregate it names. ok=false means the identity provably
// carries no catalog event aggregate (R2 evidence): the kind is not the frozen
// aggregate convention, the value has no aggregate_type/aggregate_id split, or
// the aggregate type is unknown to the frozen catalog.
func EventObligationAggregateOf(key BusinessKey) (EventObligationAggregate, bool) {
	if key.Kind != EventBusinessKeyAggregate {
		return EventObligationAggregate{}, false
	}
	aggregateType, aggregateID, found := strings.Cut(key.Value, "/")
	if !found {
		return EventObligationAggregate{}, false
	}
	aggregate := EventObligationAggregate{AggregateType: aggregateType, AggregateID: aggregateID}
	if !aggregate.Valid() {
		return EventObligationAggregate{}, false
	}
	return aggregate, true
}

// eventNotApplicableCanonicalVersion freezes the N/A event-party marker
// domain. It is a new, additive snapshot (never mixed into the absence or
// unavailable markers), so no existing identity content hash changes.
const eventNotApplicableCanonicalVersion = "eventnotapplicable/v1"

// EventNotApplicableSnapshot returns the canonical bytes of an event party
// that provably has no obligation for this candidate: the event dimension is
// excluded from the comparison instead of being guessed absent (R2). The
// marker is only meaningful inside a closed, gap-free scan; the caller owns
// coverage.
func EventNotApplicableSnapshot(key BusinessKey) []byte {
	w := &identityCanonWriter{}
	w.bytesField("eventnotapplicable.version", []byte(eventNotApplicableCanonicalVersion))
	w.stringField("eventnotapplicable.business_key.kind", string(key.Kind))
	w.stringField("eventnotapplicable.business_key.value", key.Value)
	return w.buf.Bytes()
}

// EventObligationConfig bounds the adapter's two read-only queries. Missing or
// non-positive bounds are refused (no unbounded read, no invented default).
type EventObligationConfig struct {
	// MaxExpectations bounds the marker rows one interval read may return; a
	// read that hits the bound is truncated and stays conservative (R3).
	MaxExpectations int
	// MaxRetentionAudits bounds the audited prune history examined; a read
	// that hits the bound cannot rule out a legal trim, so it stays
	// conservative (R3).
	MaxRetentionAudits int
}

// Validate fails closed on missing or non-positive bounds.
func (c EventObligationConfig) Validate() error {
	if c.MaxExpectations <= 0 {
		return contractErrorf("event obligation config requires a positive expectation bound")
	}
	if c.MaxRetentionAudits <= 0 {
		return contractErrorf("event obligation config requires a positive retention-audit bound")
	}
	return nil
}

// EventObligationReader is the read-only expectation-carrier surface the scan
// compare loop consumes. A nil reader leaves every decisive absence R3
// pending: the discriminator never guesses.
type EventObligationReader interface {
	ReadObligations(ctx context.Context, q EventObligationQuery) (EventObligationEvidence, error)
}

// EventObligationAdapter is the bounded read-only implementation:
// event_obligation rows for the requested aggregates plus the audited
// retention-prune history of event_ops_audit. It never writes.
type EventObligationAdapter struct {
	pool *pgxpool.Pool
	cfg  EventObligationConfig
}

// NewEventObligationAdapter builds the adapter; a nil pool or an invalid
// bound is refused.
func NewEventObligationAdapter(pool *pgxpool.Pool, cfg EventObligationConfig) (*EventObligationAdapter, error) {
	if pool == nil {
		return nil, contractErrorf("event obligation adapter requires a database pool")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &EventObligationAdapter{pool: pool, cfg: cfg}, nil
}

// eventObligationSelectSQL reads the expectation markers of the requested
// aggregate pairs. One extra row is requested so a truncated bound is detected
// instead of silently dropping expectations (a dropped marker could otherwise
// downgrade R1 to R3, which is conservative; the truncation flag keeps the
// whole read honest either way). Read-only.
const eventObligationSelectSQL = `
SELECT aggregate_type, aggregate_id, aggregate_version, expected_event_type,
       obligated_at, source_kind, source_id, source_version
FROM event_obligation
WHERE (aggregate_type, aggregate_id) IN (%s)
ORDER BY aggregate_type, aggregate_id, aggregate_version, expected_event_type
LIMIT $%d`

// eventRetentionAuditSelectSQL reads the most recent audited retention prunes,
// newest first. op_kind is the frozen event_ops_audit vocabulary value.
const eventRetentionAuditSelectSQL = `
SELECT scope, created_at
FROM event_ops_audit
WHERE op_kind = 'retention_prune'
ORDER BY created_at DESC, id DESC
LIMIT $1`

// ReadObligations performs the bounded read. An error means the evidence is
// unusable; the returned evidence carries ReadOK=false so a caller that keeps
// going conservatively never mistakes the failure for "no obligation".
func (a *EventObligationAdapter) ReadObligations(ctx context.Context, q EventObligationQuery) (EventObligationEvidence, error) {
	evidence := EventObligationEvidence{
		CapturedAt:   time.Now().UTC(),
		Expectations: map[EventObligationAggregate][]ObligatedExpectation{},
	}
	if a == nil || a.pool == nil {
		return evidence, contractErrorf("event obligation adapter has no database")
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return evidence, ctxErr
	}

	if len(q.Aggregates) > 0 {
		pairs := make([]string, 0, len(q.Aggregates))
		args := make([]any, 0, len(q.Aggregates)*2+1)
		for _, aggregate := range q.Aggregates {
			if !aggregate.Valid() {
				return evidence, contractErrorf("event obligation query carries an invalid aggregate shape")
			}
			args = append(args, aggregate.AggregateType, aggregate.AggregateID)
			pairs = append(pairs, fmt.Sprintf("($%d,$%d)", len(args)-1, len(args)))
		}
		limit := a.cfg.MaxExpectations + 1
		args = append(args, limit)
		rows, err := a.pool.Query(ctx, fmt.Sprintf(eventObligationSelectSQL,
			strings.Join(pairs, ","), len(args)), args...)
		if err != nil {
			return evidence, fmt.Errorf("read event obligations: %w", err)
		}
		count := 0
		for rows.Next() {
			var (
				aggregate   EventObligationAggregate
				expectation ObligatedExpectation
			)
			if err := rows.Scan(&aggregate.AggregateType, &aggregate.AggregateID,
				&expectation.AggregateVersion, &expectation.EventType,
				&expectation.ObligatedAt, &expectation.SourceKind,
				&expectation.SourceID, &expectation.SourceVersion); err != nil {
				rows.Close()
				return evidence, fmt.Errorf("scan event obligation: %w", err)
			}
			count++
			if count > a.cfg.MaxExpectations {
				evidence.Truncated = true
				break
			}
			evidence.Expectations[aggregate] = append(evidence.Expectations[aggregate], expectation)
		}
		if err := rows.Err(); err != nil {
			return evidence, fmt.Errorf("read event obligations: %w", err)
		}
		rows.Close()
	}

	// The retention history read is required to separate a possible legal
	// trim from a real loss. A failure keeps RetentionReadOK=false so the
	// verdict stays R3, never missing.
	audits, readOK, err := a.readRetentionAudits(ctx)
	if err != nil {
		return evidence, err
	}
	evidence.RetentionAudits = audits
	evidence.RetentionReadOK = readOK
	evidence.ReadOK = true
	return evidence, nil
}

// readRetentionAudits reads the bounded prune history and parses each audited
// scope. An unparseable scope, a missing retention window or a truncated read
// makes the history unprovable (readOK=false), never "no trim happened".
func (a *EventObligationAdapter) readRetentionAudits(ctx context.Context) ([]RetentionPruneAudit, bool, error) {
	limit := a.cfg.MaxRetentionAudits + 1
	rows, err := a.pool.Query(ctx, eventRetentionAuditSelectSQL, limit)
	if err != nil {
		return nil, false, fmt.Errorf("read retention prune history: %w", err)
	}
	defer rows.Close()
	var (
		audits []RetentionPruneAudit
		readOK = true
		count  int
	)
	for rows.Next() {
		var (
			scopeJSON []byte
			createdAt time.Time
		)
		if err := rows.Scan(&scopeJSON, &createdAt); err != nil {
			return nil, false, fmt.Errorf("scan retention prune audit: %w", err)
		}
		count++
		if count > a.cfg.MaxRetentionAudits {
			readOK = false
			continue
		}
		window, ok := parseRetentionScope(scopeJSON)
		if !ok || createdAt.IsZero() {
			readOK = false
			continue
		}
		audits = append(audits, RetentionPruneAudit{PrunedAt: createdAt.UTC(), Window: window})
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("read retention prune history: %w", err)
	}
	return audits, readOK, nil
}

// parseRetentionScope decodes the events-admin prune audit scope
// ({"retention":"168h0m0s"}, quarantine.go RetentionPrune). A missing or
// malformed value returns ok=false so the caller stays conservative.
func parseRetentionScope(raw []byte) (time.Duration, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	var scope map[string]any
	if err := json.Unmarshal(raw, &scope); err != nil {
		return 0, false
	}
	value, ok := scope["retention"].(string)
	if !ok {
		return 0, false
	}
	window, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil || window <= 0 {
		return 0, false
	}
	return window, true
}
