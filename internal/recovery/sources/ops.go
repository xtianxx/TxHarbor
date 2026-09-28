// ops.go implements T040 [US3]: the V5-V9 read-only verification adapters of
// internal/recovery/sources (tasks.md T040; contracts/verification-items.md
// §1; data-model.md §8; FR-014/015/016/020/027/028/034).
//
// The five adapters observe one stable-object family each:
//
//	V5 outboxFactsSource     outbox_events + event_obligation (append-only) +
//	                         publisher progress + event_system_state +
//	                         event_ops_audit retention-prune history
//	V6 consumerFactsSource   consumer_inbox / consumer_versions /
//	                         consumer_progress / consumer_quarantine + the
//	                         broker committed offset when a reader is wired
//	V7 reconFactsSource      the 000016 reconciliation reference: task /
//	                         checkpoint / gap / discrepancy / disposition /
//	                         reverify / audit / scan-attempt / permission
//	V8 authSurfaceSource     the data-DB authorization surface (caller /
//	                         api_key) plus the control-store
//	                         authorization_recheck external-truth evidence
//	V9 readinessSource       tool/dependency readiness: data DSN, control
//	                         store, canonical chain client and the assembled
//	                         DependencyProbe set (image/registry, signer
//	                         boundary, additional DSNs)
//
// Read-only discipline (contracts/verification-items.md §1, F4/INV-12): every
// read is a SELECT against the data DB or the version-guarded control store
// (controlstore.Store, T069), and the broker surface is a caller-supplied
// read-only offset reader. No method in this file writes anywhere: V5 never
// publishes, acks, unblocks, prunes or advances an outbox row; V6 never
// replays, quarantines, inserts an inbox row or advances an offset; V7 never
// claims, disposes, reverifies or closes a 014 ticket; V8 never applies a
// revoke/grant and never touches a credential; V9 only probes reachability and
// never acquires a key, session or signer handle. The T038 orchestrator owns
// every append-only control-store write (verification items, evidence gaps,
// audit).
//
// Conclusion discipline (all fail-closed):
//
//   - a record missing at the restore point is unknown plus an FR-019 evidence
//     gap; absence is never "never emitted", "never consumed", "never
//     rechecked" or safe to replay (FR-016/FR-017);
//   - an explicit local/external contradiction is divergent and names the
//     difference (FR-015): an obligation marker whose event row carries another
//     type, and a broker committed offset ahead of the restored PG progress;
//   - an unreadable source is unknown plus a gap: no broker reader (V6), an
//     unreadable retention-prune history (V5), missing/not-migrated 000016
//     relations (V7), no external-truth authorization_recheck record (V8),
//     an unreachable boundary (V9);
//   - a missing historical idempotency record is never silently written back
//     as "processed" (FR-028), and verification never triggers an effect:
//     re-processing exists only through the authorized quarantine-replay and
//     events-admin paths, which this file does not call.
package sources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// ---------------------------------------------------------------------------
// V5-V9 assembly
// ---------------------------------------------------------------------------

// DependencyProbe is one caller-assembled V9 readiness probe. Name is the
// stable boundary name recorded in the object identity and the audit trail (a
// blank name is recorded as "unnamed"); Check performs one bounded
// reachability read and MUST NOT obtain a credential, key or signer session.
// The assembly layer supplies the probes for the boundaries this package
// cannot dial itself (image/registry client, signer boundary, additional
// DSNs); an empty probe set leaves those boundaries unproven (unknown).
type DependencyProbe interface {
	Name() string
	Check(ctx context.Context) error
}

// BrokerOffsetReader is the read-only broker surface for one consumer group's
// committed offset (the next offset to consume). The T053 event layer supplies
// the adapter over the real broker; this package never dials the broker
// itself. A nil reader keeps V6's offset-regression detection unprovable
// (unknown), never a default zero.
type BrokerOffsetReader interface {
	CommittedOffset(ctx context.Context, topic string, partition int) (int64, error)
}

// OpsOptions constructs the V5-V9 read-only adapters. Data, Control and RPC
// are required: V5-V7 read the data DB, V8 reads the version-guarded control
// store as its external-truth source, and V9 probes the canonical chain
// boundary. BrokerOffsets and Dependencies are optional; a missing surface
// keeps the affected conclusion conservative (unknown), never a pass.
type OpsOptions struct {
	Data          *pgxpool.Pool       // data DB; the pool may be a SELECT-only role
	Control       *controlstore.Store // version-guarded control store (V8)
	RPC           *eth.Client         // canonical chain reads (V9 readiness)
	BrokerOffsets BrokerOffsetReader  // nil = broker offsets unreadable -> unknown
	Dependencies  []DependencyProbe   // V9 reachability probes; empty = unproven
	DataTarget    string
	// Now is a test seam for observation timestamps (nil means time.Now).
	Now func() time.Time
}

// NewOpsSources builds the five read-only V5-V9 adapters. A missing required
// dependency refuses construction: there is no best-effort assembly in which a
// missing control store silently disables the V8 external-truth check or a
// missing chain client silently disables the V9 readiness boundary.
func NewOpsSources(opts OpsOptions) ([]recovery.VerificationSource, error) {
	if opts.Data == nil {
		return nil, errors.New("ops sources require the data-DB pool")
	}
	if opts.Control == nil {
		return nil, errors.New("ops sources require the version-guarded control store (V8 external truth)")
	}
	if opts.RPC == nil {
		return nil, errors.New("ops sources require the canonical chain client (V9 readiness)")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	base := &opsSourceBase{
		data:       opts.Data,
		control:    opts.Control,
		rpc:        opts.RPC,
		broker:     opts.BrokerOffsets,
		deps:       slices.Clone(opts.Dependencies),
		dataTarget: opts.DataTarget,
		now:        now,
	}
	return []recovery.VerificationSource{
		&outboxFactsSource{base: base},
		&consumerFactsSource{base: base},
		&reconFactsSource{base: base},
		&authSurfaceSource{base: base},
		&readinessSource{base: base},
	}, nil
}

// The capability sets a missing/unprovable V5-V9 object blocks. The sets are
// the directly related sets of contracts/verification-items.md §1; the T038
// gap writer conservatively expands them along the dependency matrix.
var (
	// V5 blocks the event-publishing capability.
	eventPublishingCaps = []recovery.Capability{recovery.CapabilityEventPublishing}
	// V6 blocks the event-consuming capability.
	eventConsumingCaps = []recovery.Capability{recovery.CapabilityEventConsuming}
	// V7 overlaps the V2/V5/V6 capabilities and V8 guards the fund and
	// delivery capabilities; in the closed seven-capability set both reduce to
	// the same directly related set.
	fundsDeliveryCaps = []recovery.Capability{
		recovery.CapabilityNewWithdrawalCreation,
		recovery.CapabilityExistingWithdrawalRecovery,
		recovery.CapabilityEventPublishing,
		recovery.CapabilityEventConsuming,
	}
	// V9 is the readiness prerequisite of every capability (全部（前置）).
	readinessCaps = recovery.KnownCapabilities()
)

// opsSourceBase carries the read-only dependencies shared by V5-V9. It holds
// no mutable state.
type opsSourceBase struct {
	data       *pgxpool.Pool
	control    *controlstore.Store
	rpc        *eth.Client
	broker     BrokerOffsetReader
	deps       []DependencyProbe
	dataTarget string
	now        func() time.Time
}

// observationInstant is the timestamp recorded on every V5-V9 observation.
// Whole-second precision is deliberate (same rule as the V1-V4 base): the
// orchestrator captures its evaluation instant immediately before reading the
// sources, so sub-second precision could postdate it and trip the evaluator's
// strict "future timestamp" rule.
func (b *opsSourceBase) observationInstant() time.Time {
	return b.now().UTC().Truncate(time.Second)
}

// opsObservationInput is one assembled V5-V9 adapter observation.
type opsObservationInput struct {
	category     recovery.VerificationCategory
	objectKey    string
	conclusion   recovery.VerificationConclusion
	reason       string
	facts        map[string]any
	evidenceRefs []string
	// directCaps names the directly affected capabilities when the conclusion
	// is unknown: a missing/unprovable record establishes an FR-019 gap.
	directCaps []recovery.Capability
	// required describes the external evidence needed to close the gap.
	required map[string]any
}

// opsObservationCollector accumulates observations deterministically.
type opsObservationCollector struct {
	base *opsSourceBase
	out  []recovery.SourceObservation
}

func (c *opsObservationCollector) add(in opsObservationInput) error {
	obs, err := c.base.observe(in)
	if err != nil {
		return err
	}
	c.out = append(c.out, obs)
	return nil
}

// observe renders one SourceObservation. A gap bundle is attached only to an
// unknown conclusion (missing/unprovable evidence): T038 caps a gapped item at
// unknown by construction, so a gapped observation must not be divergent (an
// explicit contradiction is reported as divergent, without a gap).
func (b *opsSourceBase) observe(in opsObservationInput) (recovery.SourceObservation, error) {
	facts := in.facts
	if facts == nil {
		facts = map[string]any{}
	}
	sourcesPayload, err := json.Marshal(facts)
	if err != nil {
		return recovery.SourceObservation{}, fmt.Errorf("encode %s/%s sources payload: %w", in.category, in.objectKey, err)
	}
	scope := map[string]any{
		"category": string(in.category),
		"object":   in.objectKey,
	}
	if b.dataTarget != "" {
		scope["data_target"] = b.dataTarget
	}
	scopePayload, err := json.Marshal(scope)
	if err != nil {
		return recovery.SourceObservation{}, fmt.Errorf("encode %s/%s scope payload: %w", in.category, in.objectKey, err)
	}
	observation := recovery.SourceObservation{
		ObjectKey:    in.objectKey,
		Scope:        scopePayload,
		Sources:      sourcesPayload,
		Conclusion:   in.conclusion,
		Reason:       in.reason,
		EvidenceRefs: in.evidenceRefs,
		ObservedAt:   b.observationInstant(),
	}
	if in.conclusion == recovery.ConclusionUnknown && len(in.directCaps) > 0 {
		gap, err := b.opsGapEvidence(in.objectKey, in.directCaps, facts, in.required)
		if err != nil {
			return recovery.SourceObservation{}, err
		}
		observation.GapEvidence = gap
	}
	return observation, nil
}

// opsGapEvidence renders the FR-019 evidence bundle of one unprovable object.
func (b *opsSourceBase) opsGapEvidence(objectKey string, direct []recovery.Capability, existing, required map[string]any) (*recovery.GapEvidence, error) {
	timeline, err := json.Marshal(map[string]any{
		"object":      objectKey,
		"observed_at": b.observationInstant().Format(time.RFC3339),
	})
	if err != nil {
		return nil, fmt.Errorf("encode gap timeline for %s: %w", objectKey, err)
	}
	existingPayload, err := json.Marshal(existing)
	if err != nil {
		return nil, fmt.Errorf("encode gap existing evidence for %s: %w", objectKey, err)
	}
	if required == nil {
		required = externalRequirement("manual reconciliation with trusted external evidence")
	}
	requiredPayload, err := json.Marshal(required)
	if err != nil {
		return nil, fmt.Errorf("encode gap required evidence for %s: %w", objectKey, err)
	}
	risk, err := json.Marshal(map[string]any{"unproven_external_effect": true, "object": objectKey})
	if err != nil {
		return nil, fmt.Errorf("encode gap risk for %s: %w", objectKey, err)
	}
	return &recovery.GapEvidence{
		Timeline:             timeline,
		ExistingEvidence:     existingPayload,
		RequiredEvidence:     requiredPayload,
		Risk:                 risk,
		AffectedCapabilities: direct,
	}, nil
}

// ---------------------------------------------------------------------------
// Shared read helpers
// ---------------------------------------------------------------------------

// relationMissing reports whether err is PostgreSQL's undefined_table (42P01):
// a relation that does not exist at the restore point is "not migrated" and is
// recorded as unknown, never as an empty result.
func relationMissing(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

// stateCounts reads a `SELECT state, count(*) ... GROUP BY state` result into a
// map. missing=true means the relation does not exist (the caller records the
// conservative unknown instead of an empty count).
func (b *opsSourceBase) stateCounts(ctx context.Context, query string) (map[string]int64, bool, error) {
	rows, err := b.data.Query(ctx, query)
	if err != nil {
		if relationMissing(err) {
			return nil, true, nil
		}
		return nil, false, err
	}
	defer rows.Close()
	counts := make(map[string]int64)
	for rows.Next() {
		var (
			state string
			count int64
		)
		if err := rows.Scan(&state, &count); err != nil {
			return nil, false, err
		}
		counts[state] = count
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	return counts, false, nil
}

// singleCount reads one count(*) scalar. missing=true means the relation does
// not exist.
func (b *opsSourceBase) singleCount(ctx context.Context, query string, args ...any) (int64, bool, error) {
	var count int64
	if err := b.data.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		if relationMissing(err) {
			return 0, true, nil
		}
		return 0, false, err
	}
	return count, false, nil
}

// ---------------------------------------------------------------------------
// V5: outbox events, obligations and publisher progress
// ---------------------------------------------------------------------------

type outboxFactsSource struct{ base *opsSourceBase }

// Category implements recovery.VerificationSource.
func (s *outboxFactsSource) Category() recovery.VerificationCategory { return recovery.VerificationV5 }

// Observe reads the event-system cutover state, the outbox publisher progress
// and every obligation marker, and compares each marker with its outbox row.
// An obligation whose event row is absent is unknown (possibly retention-
// trimmed, never "never emitted"); a pending/blocked row is unknown (its
// broker publication cannot be confirmed and is never assumed delivered or
// undelivered); a marker whose row carries another event type is divergent.
// Nothing is published, acked, unblocked or pruned (F10 history).
func (s *outboxFactsSource) Observe(ctx context.Context) ([]recovery.SourceObservation, error) {
	c := &opsObservationCollector{base: s.base}
	if err := s.observeEventSystemState(ctx, c); err != nil {
		return nil, err
	}
	if err := s.observePublisherProgress(ctx, c); err != nil {
		return nil, err
	}
	if err := s.observeObligations(ctx, c); err != nil {
		return nil, err
	}
	return c.out, nil
}

func (s *outboxFactsSource) observeEventSystemState(ctx context.Context, c *opsObservationCollector) error {
	b := s.base
	objectKey := "v5/event_system_state?id=1"
	refs := []string{
		"data:event_system_state?id=1",
		"data:outbox_events?scope=all",
	}
	var (
		cutoverAt      time.Time
		catalogVersion int32
		updatedAt      time.Time
	)
	err := b.data.QueryRow(ctx,
		`SELECT cutover_at, catalog_version, updated_at FROM event_system_state WHERE id = 1`).
		Scan(&cutoverAt, &catalogVersion, &updatedAt)
	switch {
	case relationMissing(err):
		return c.add(opsObservationInput{
			category:   recovery.VerificationV5,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: "the event_system_state table does not exist at the restore point (000015 not migrated); the " +
				"event-catalog cutover state cannot be established and a missing relation is not evidence that no " +
				"event was ever emitted (FR-016)",
			facts:        map[string]any{"table": "event_system_state", "found": false},
			evidenceRefs: refs,
			directCaps:   eventPublishingCaps,
			required: externalRequirement(
				"the event_system_state row from a trusted copy of the data DB",
			),
		})
	case errors.Is(err, pgx.ErrNoRows):
		return c.add(opsObservationInput{
			category:   recovery.VerificationV5,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: "no event_system_state row exists at the restore point; the event-catalog cutover marker and " +
				"catalog version are unprovable and a missing record is never filled from a default (FR-016/FR-018)",
			facts:        map[string]any{"table": "event_system_state", "found": false},
			evidenceRefs: refs,
			directCaps:   eventPublishingCaps,
			required: externalRequirement(
				"the event_system_state row from a trusted copy of the data DB",
			),
		})
	case err != nil:
		return fmt.Errorf("read event system state: %w", err)
	}
	return c.add(opsObservationInput{
		category:   recovery.VerificationV5,
		objectKey:  objectKey,
		conclusion: recovery.ConclusionConsistent,
		facts: map[string]any{
			"table":           "event_system_state",
			"cutover_at":      cutoverAt.UTC().Format(time.RFC3339Nano),
			"catalog_version": catalogVersion,
			"updated_at":      updatedAt.UTC().Format(time.RFC3339Nano),
		},
		evidenceRefs: refs,
	})
}

// outboxStateFact is one publish_state group of the outbox queue.
type outboxStateFact struct {
	state       string
	rows        int64
	oldest      *time.Time
	latestPub   *time.Time
	claimedRows int64
}

func (s *outboxFactsSource) observePublisherProgress(ctx context.Context, c *opsObservationCollector) error {
	b := s.base
	objectKey := "v5/outbox_events?scope=all"
	refs := []string{
		"data:outbox_events?scope=all",
		"data:event_obligation?scope=all",
	}
	rows, err := b.data.Query(ctx, `
		SELECT publish_state, count(*),
		       min(occurred_at) FILTER (WHERE publish_state <> 'published'),
		       max(published_at),
		       count(*) FILTER (WHERE claim_owner IS NOT NULL)
		  FROM outbox_events
		 GROUP BY publish_state
		 ORDER BY publish_state`)
	if err != nil {
		if relationMissing(err) {
			return c.add(opsObservationInput{
				category:   recovery.VerificationV5,
				objectKey:  objectKey,
				conclusion: recovery.ConclusionUnknown,
				reason: "the outbox_events table does not exist at the restore point (000015 not migrated); the " +
					"publisher progress cannot be read and a missing relation is not evidence that no event was due",
				facts:        map[string]any{"table": "outbox_events", "found": false},
				evidenceRefs: refs,
				directCaps:   eventPublishingCaps,
				required: externalRequirement(
					"the outbox_events rows from a trusted copy of the data DB",
				),
			})
		}
		return fmt.Errorf("read outbox publish states: %w", err)
	}
	var states []outboxStateFact
	for rows.Next() {
		var row outboxStateFact
		if err := rows.Scan(&row.state, &row.rows, &row.oldest, &row.latestPub, &row.claimedRows); err != nil {
			rows.Close()
			return fmt.Errorf("scan outbox publish state: %w", err)
		}
		states = append(states, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read outbox publish states: %w", err)
	}

	facts := map[string]any{"table": "outbox_events"}
	byState := make(map[string]int64, len(states))
	var (
		total           int64
		unsettled       int64
		inFlightClaims  int64
		oldestUnsettled *time.Time
		latestPublished *time.Time
	)
	for _, row := range states {
		byState[row.state] = row.rows
		total += row.rows
		inFlightClaims += row.claimedRows
		switch row.state {
		case "pending", "blocked":
			unsettled += row.rows
			if row.oldest != nil && (oldestUnsettled == nil || row.oldest.Before(*oldestUnsettled)) {
				oldestUnsettled = row.oldest
			}
		case "published":
			if row.latestPub != nil && (latestPublished == nil || row.latestPub.After(*latestPublished)) {
				latestPublished = row.latestPub
			}
		}
	}
	facts["publish_states"] = byState
	facts["total"] = total
	facts["unsettled"] = unsettled
	facts["in_flight_claims"] = inFlightClaims
	if oldestUnsettled != nil {
		facts["oldest_unsettled_at"] = oldestUnsettled.UTC().Format(time.RFC3339Nano)
	}
	if latestPublished != nil {
		facts["latest_published_at"] = latestPublished.UTC().Format(time.RFC3339Nano)
	}

	switch {
	case total == 0:
		return c.add(opsObservationInput{
			category:   recovery.VerificationV5,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: "no outbox_events row exists at the restore point; absence is not evidence that no event was " +
				"ever due, and the publisher progress cannot be established from an empty relation (FR-016)",
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   eventPublishingCaps,
			required: externalRequirement(
				"the outbox_events rows from a trusted copy of the data DB",
				"external evidence that the event stream of the restored window was fully accounted for",
			),
		})
	case unsettled > 0:
		return c.add(opsObservationInput{
			category:   recovery.VerificationV5,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("%d pending/blocked outbox event(s) remain at the restore point; a pending row may "+
				"already have reached the broker, its publication cannot be confirmed from here, and a blocked row "+
				"is never auto-unblocked or re-published (FR-016, F10 history)", unsettled),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   eventPublishingCaps,
			required: externalRequirement(
				"the broker-side delivery/retention evidence for the unsettled events",
				"the audited unblock decision for every blocked event",
			),
		})
	}
	return c.add(opsObservationInput{
		category:     recovery.VerificationV5,
		objectKey:    objectKey,
		conclusion:   recovery.ConclusionConsistent,
		facts:        facts,
		evidenceRefs: refs,
	})
}

// pruneAuditFact is one audited events-admin retention prune: published rows
// with published_at before PrunedAt-window were deleted by that run.
type pruneAuditFact struct {
	prunedAt time.Time
	window   time.Duration
}

// readPruneAudits reads a bounded retention-prune history. readOK=false means
// the history is unreadable or truncated: a legal trim of a published row can
// never be ruled out, so a missing event stays conservatively unknown.
func (b *opsSourceBase) readPruneAudits(ctx context.Context) ([]pruneAuditFact, bool, error) {
	const bound = 20
	rows, err := b.data.Query(ctx, `
		SELECT scope::text, created_at
		  FROM event_ops_audit
		 WHERE op_kind = 'retention_prune'
		 ORDER BY created_at DESC, id DESC
		 LIMIT $1`, bound+1)
	if err != nil {
		if relationMissing(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("read retention prune history: %w", err)
	}
	defer rows.Close()
	readOK := true
	var audits []pruneAuditFact
	for rows.Next() {
		var (
			scope     string
			createdAt time.Time
		)
		if err := rows.Scan(&scope, &createdAt); err != nil {
			return nil, false, fmt.Errorf("scan retention prune audit: %w", err)
		}
		if len(audits) >= bound {
			readOK = false
			continue
		}
		window, ok := pruneWindowOf(scope)
		if !ok || createdAt.IsZero() {
			readOK = false
			continue
		}
		audits = append(audits, pruneAuditFact{prunedAt: createdAt.UTC(), window: window})
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("read retention prune history: %w", err)
	}
	return audits, readOK, nil
}

// pruneWindowOf decodes the events-admin prune audit scope
// ({"retention":"168h0m0s"}); a missing/malformed window returns ok=false so
// the caller stays conservative.
func pruneWindowOf(scope string) (time.Duration, bool) {
	var decoded map[string]any
	if err := json.Unmarshal([]byte(scope), &decoded); err != nil {
		return 0, false
	}
	raw, ok := decoded["retention"].(string)
	if !ok {
		return 0, false
	}
	window, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || window <= 0 {
		return 0, false
	}
	return window, true
}

// pruneExplains reports whether any audited prune could have deleted the
// published row of an obligation. An unreadable obligation instant never
// supports a loss claim (it returns false here and the caller keeps unknown).
func pruneExplains(obligatedAt time.Time, audits []pruneAuditFact) bool {
	if obligatedAt.IsZero() {
		return false
	}
	for _, audit := range audits {
		if audit.window <= 0 {
			continue
		}
		if obligatedAt.Before(audit.prunedAt.Add(-audit.window)) {
			return true
		}
	}
	return false
}

func (s *outboxFactsSource) observeObligations(ctx context.Context, c *opsObservationCollector) error {
	b := s.base
	aggregateKey := "v5/event_obligation?scope=all"
	aggregateRefs := []string{
		"data:event_obligation?scope=all",
		"data:outbox_events?scope=all",
		"data:event_ops_audit?op_kind=retention_prune",
	}
	prunes, prunesOK, err := b.readPruneAudits(ctx)
	if err != nil {
		return err
	}
	rows, err := b.data.Query(ctx, `
		SELECT o.aggregate_type, o.aggregate_id, o.aggregate_version, o.expected_event_type,
		       o.obligated_at, o.source_kind, o.source_id,
		       e.event_id::text, e.event_type, e.publish_state, e.published_at
		  FROM event_obligation o
		  LEFT JOIN outbox_events e
		    ON e.identity_kind = 'business_object'
		   AND e.aggregate_type = o.aggregate_type
		   AND e.aggregate_id = o.aggregate_id
		   AND e.aggregate_version = o.aggregate_version
		 ORDER BY o.obligated_at, o.aggregate_type, o.aggregate_id, o.aggregate_version, o.expected_event_type`)
	if err != nil {
		if relationMissing(err) {
			return c.add(opsObservationInput{
				category:   recovery.VerificationV5,
				objectKey:  aggregateKey,
				conclusion: recovery.ConclusionUnknown,
				reason: "the event_obligation table does not exist at the restore point (000017 not migrated); the " +
					"expectation carrier cannot be read and a missing marker is never evidence that no obligation " +
					"existed (FR-016)",
				facts:        map[string]any{"table": "event_obligation", "found": false},
				evidenceRefs: aggregateRefs,
				directCaps:   eventPublishingCaps,
				required: externalRequirement(
					"the event_obligation markers from a trusted copy of the data DB",
				),
			})
		}
		return fmt.Errorf("read event obligations: %w", err)
	}
	type obligationRow struct {
		aggregateType    string
		aggregateID      string
		aggregateVersion int64
		expectedType     string
		obligatedAt      time.Time
		sourceKind       string
		sourceID         string
		eventID          *string
		eventType        *string
		publishState     *string
		publishedAt      *time.Time
	}
	var obligations []obligationRow
	for rows.Next() {
		var row obligationRow
		if err := rows.Scan(&row.aggregateType, &row.aggregateID, &row.aggregateVersion, &row.expectedType,
			&row.obligatedAt, &row.sourceKind, &row.sourceID, &row.eventID, &row.eventType,
			&row.publishState, &row.publishedAt); err != nil {
			rows.Close()
			return fmt.Errorf("scan event obligation: %w", err)
		}
		obligations = append(obligations, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read event obligations: %w", err)
	}

	for _, row := range obligations {
		objectKey := fmt.Sprintf("v5/event_obligation?aggregate=%s/%s&version=%d&event_type=%s",
			row.aggregateType, row.aggregateID, row.aggregateVersion, row.expectedType)
		refs := []string{
			fmt.Sprintf("data:event_obligation?aggregate_type=%s&aggregate_id=%s&aggregate_version=%d",
				row.aggregateType, row.aggregateID, row.aggregateVersion),
			"data:event_ops_audit?op_kind=retention_prune",
		}
		facts := map[string]any{
			"table":               "event_obligation",
			"aggregate_type":      row.aggregateType,
			"aggregate_id":        row.aggregateID,
			"aggregate_version":   row.aggregateVersion,
			"expected_event_type": row.expectedType,
			"obligated_at":        row.obligatedAt.UTC().Format(time.RFC3339Nano),
		}
		if row.sourceKind != "" || row.sourceID != "" {
			facts["source"] = map[string]any{"kind": row.sourceKind, "id": row.sourceID}
		}

		if row.eventID == nil {
			facts["outbox_event"] = "missing"
			explanation := "no audited retention prune explains the absence"
			if !prunesOK {
				explanation = "the retention-prune history is unreadable, so a legal trim cannot be ruled out"
			} else if pruneExplains(row.obligatedAt, prunes) {
				explanation = "an audited retention prune could have removed the published row"
			}
			if err := c.add(opsObservationInput{
				category:   recovery.VerificationV5,
				objectKey:  objectKey,
				conclusion: recovery.ConclusionUnknown,
				reason: fmt.Sprintf("the obligation marker proves the transition had an event obligation, but its "+
					"outbox_events row is absent at the restore point (%s); a missing row is never evidence that "+
					"the event was never emitted or delivered (FR-016)", explanation),
				facts:        facts,
				evidenceRefs: refs,
				directCaps:   eventPublishingCaps,
				required: externalRequirement(
					"the outbox_events row (or its broker delivery evidence) from a trusted source",
					"the audited retention-prune decision covering the obligation instant",
				),
			}); err != nil {
				return err
			}
			continue
		}

		eventFact := map[string]any{
			"event_id": *row.eventID,
		}
		if row.eventType != nil {
			eventFact["event_type"] = *row.eventType
		}
		if row.publishState != nil {
			eventFact["publish_state"] = *row.publishState
		}
		if row.publishedAt != nil {
			eventFact["published_at"] = row.publishedAt.UTC().Format(time.RFC3339Nano)
		}
		facts["outbox_event"] = eventFact

		if row.eventType != nil && *row.eventType != row.expectedType {
			if err := c.add(opsObservationInput{
				category:   recovery.VerificationV5,
				objectKey:  objectKey,
				conclusion: recovery.ConclusionDivergent,
				reason: fmt.Sprintf("the obligation marker expects event type %s for aggregate %s/%s version %d "+
					"while the outbox row carries %s; the restored records contradict each other",
					row.expectedType, row.aggregateType, row.aggregateID, row.aggregateVersion, *row.eventType),
				facts:        facts,
				evidenceRefs: refs,
			}); err != nil {
				return err
			}
			continue
		}

		state := ""
		if row.publishState != nil {
			state = *row.publishState
		}
		if state != "published" {
			if err := c.add(opsObservationInput{
				category:   recovery.VerificationV5,
				objectKey:  objectKey,
				conclusion: recovery.ConclusionUnknown,
				reason: fmt.Sprintf("the obligation's outbox event is publish_state=%s; it has not advanced to a "+
					"settled publication at the restore point and its broker delivery is unknown, never assumed "+
					"(FR-016)", state),
				facts:        facts,
				evidenceRefs: refs,
				directCaps:   eventPublishingCaps,
				required: externalRequirement(
					"the broker-side delivery evidence (or audited unblock decision) for the event",
				),
			}); err != nil {
				return err
			}
			continue
		}
		if err := c.add(opsObservationInput{
			category:     recovery.VerificationV5,
			objectKey:    objectKey,
			conclusion:   recovery.ConclusionConsistent,
			facts:        facts,
			evidenceRefs: refs,
		}); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// V6: consumer idempotency, progress, quarantine and broker offsets
// ---------------------------------------------------------------------------

type consumerFactsSource struct{ base *opsSourceBase }

// Category implements recovery.VerificationSource.
func (s *consumerFactsSource) Category() recovery.VerificationCategory {
	return recovery.VerificationV6
}

// Observe reads every consumer's durable progress and idempotency state and,
// when a broker reader is configured, compares the PG next_offset with the
// broker committed offset. A regression (broker ahead of the restored PG
// progress) is divergent: the consumed external effects remain facts (FR-015).
// An unreadable broker is unknown plus a gap; a positive PG progress with no
// idempotency history is unknown (never silently "processed"); an open
// quarantine row is unknown (its effect was not applied, replay only through
// the authorized path). Nothing is replayed, re-applied or advanced.
func (s *consumerFactsSource) Observe(ctx context.Context) ([]recovery.SourceObservation, error) {
	c := &opsObservationCollector{base: s.base}
	progress, missing, err := s.progressFacts(ctx)
	if err != nil {
		return nil, err
	}
	if missing {
		if err := c.add(s.consumerProgressAggregate("the consumer_progress table does not exist at the restore " +
			"point (000015 not migrated); the consumer progress and any offset regression are unprovable")); err != nil {
			return nil, err
		}
	} else {
		for _, row := range progress {
			if err := s.observeProgress(ctx, c, row); err != nil {
				return nil, err
			}
		}
		if len(progress) == 0 {
			if err := c.add(s.consumerProgressAggregate("no consumer_progress row exists at the restore point; " +
				"absence is not evidence that nothing was ever consumed, and an offset regression cannot be " +
				"detected without a durable progress row (FR-027/028)")); err != nil {
				return nil, err
			}
		}
	}
	if err := s.observeQuarantine(ctx, c); err != nil {
		return nil, err
	}
	return c.out, nil
}

// consumerProgressFact is one durable consumer progress row.
type consumerProgressFact struct {
	consumerName string
	topic        string
	partition    int32
	nextOffset   int64
	updatedAt    time.Time
}

func (s *consumerFactsSource) progressFacts(ctx context.Context) ([]consumerProgressFact, bool, error) {
	rows, err := s.base.data.Query(ctx, `
		SELECT consumer_name, topic, partition, next_offset, updated_at
		  FROM consumer_progress
		 ORDER BY consumer_name, topic, partition`)
	if err != nil {
		if relationMissing(err) {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("read consumer progress: %w", err)
	}
	defer rows.Close()
	var facts []consumerProgressFact
	for rows.Next() {
		var row consumerProgressFact
		if err := rows.Scan(&row.consumerName, &row.topic, &row.partition, &row.nextOffset, &row.updatedAt); err != nil {
			return nil, false, fmt.Errorf("scan consumer progress: %w", err)
		}
		facts = append(facts, row)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("read consumer progress: %w", err)
	}
	return facts, false, nil
}

// consumerProgressAggregate is the conservative scope-level observation used
// when no per-partition progress row can be read.
func (s *consumerFactsSource) consumerProgressAggregate(reason string) opsObservationInput {
	return opsObservationInput{
		category:   recovery.VerificationV6,
		objectKey:  "v6/consumer_progress?scope=all",
		conclusion: recovery.ConclusionUnknown,
		reason:     reason,
		facts:      map[string]any{"table": "consumer_progress", "found": false},
		evidenceRefs: []string{
			"data:consumer_progress?scope=all",
			"data:consumer_inbox?scope=all",
			"broker:committed_offset?scope=all",
		},
		directCaps: eventConsumingCaps,
		required: externalRequirement(
			"the consumer progress rows from a trusted copy of the data DB",
			"the broker committed offsets of the consumer groups",
		),
	}
}

func (s *consumerFactsSource) observeProgress(ctx context.Context, c *opsObservationCollector, row consumerProgressFact) error {
	b := s.base
	objectKey := fmt.Sprintf("v6/consumer_progress?consumer=%s&topic=%s&partition=%d",
		row.consumerName, row.topic, row.partition)
	refs := []string{
		fmt.Sprintf("data:consumer_progress?consumer=%s&topic=%s&partition=%d", row.consumerName, row.topic, row.partition),
		fmt.Sprintf("data:consumer_inbox?consumer=%s&topic=%s&partition=%d", row.consumerName, row.topic, row.partition),
		fmt.Sprintf("data:consumer_versions?consumer=%s", row.consumerName),
		fmt.Sprintf("data:consumer_quarantine?consumer=%s", row.consumerName),
		fmt.Sprintf("broker:committed_offset?topic=%s&partition=%d", row.topic, row.partition),
	}
	facts := map[string]any{
		"table":         "consumer_progress",
		"consumer_name": row.consumerName,
		"topic":         row.topic,
		"partition":     row.partition,
		"next_offset":   row.nextOffset,
		"updated_at":    row.updatedAt.UTC().Format(time.RFC3339Nano),
	}

	var (
		inboxRows      int64
		inboxMaxOffset *int64
	)
	if err := b.data.QueryRow(ctx,
		`SELECT count(*), max("offset") FROM consumer_inbox
		  WHERE consumer_name = $1 AND topic = $2 AND partition = $3`,
		row.consumerName, row.topic, row.partition).Scan(&inboxRows, &inboxMaxOffset); err != nil {
		if relationMissing(err) {
			facts["consumer_inbox"] = "missing"
		} else {
			return fmt.Errorf("read consumer inbox %s/%s/%d: %w", row.consumerName, row.topic, row.partition, err)
		}
	} else {
		facts["consumer_inbox_rows"] = inboxRows
		if inboxMaxOffset != nil {
			facts["consumer_inbox_max_offset"] = *inboxMaxOffset
		}
	}
	versionRows, versionMissing, err := b.singleCount(ctx,
		`SELECT count(*) FROM consumer_versions WHERE consumer_name = $1`, row.consumerName)
	if err != nil {
		return fmt.Errorf("read consumer versions %s: %w", row.consumerName, err)
	}
	if versionMissing {
		facts["consumer_versions"] = "missing"
	} else {
		facts["consumer_versions_rows"] = versionRows
	}
	openQuarantine, quarantineMissing, err := b.singleCount(ctx,
		`SELECT count(*) FROM consumer_quarantine WHERE consumer_name = $1 AND status = 'open'`, row.consumerName)
	if err != nil {
		return fmt.Errorf("read consumer quarantine %s: %w", row.consumerName, err)
	}
	if quarantineMissing {
		facts["consumer_quarantine"] = "missing"
	} else {
		facts["open_quarantine_rows"] = openQuarantine
	}

	if b.broker == nil {
		return c.add(opsObservationInput{
			category:   recovery.VerificationV6,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: "no broker committed-offset reader is configured; the PG progress cannot be compared with the " +
				"broker, so a rolled-back/regressed offset is not detectable and the external consumption state " +
				"stays unknown (FR-027/028)",
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   eventConsumingCaps,
			required: externalRequirement(
				"the broker committed offset for the consumer's topic/partition",
			),
		})
	}
	committed, err := b.broker.CommittedOffset(ctx, row.topic, int(row.partition))
	if err != nil {
		facts["broker_committed_offset"] = "unreadable"
		facts["broker_committed_offset_error"] = logx.Redact(err.Error())
		return c.add(opsObservationInput{
			category:   recovery.VerificationV6,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the broker committed offset of %s/%s/%d is unreadable (%s); the PG progress cannot "+
				"be compared and a regression cannot be excluded", row.consumerName, row.topic, row.partition,
				logx.Redact(err.Error())),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   eventConsumingCaps,
			required: externalRequirement(
				"a reachable broker committed-offset read",
			),
		})
	}
	facts["broker_committed_offset"] = committed

	switch {
	case committed > row.nextOffset:
		return c.add(opsObservationInput{
			category:   recovery.VerificationV6,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionDivergent,
			reason: fmt.Sprintf("the broker committed offset %d is ahead of the restored consumer_progress "+
				"next_offset %d by %d; the database rollback regressed the consumption progress while the consumed "+
				"external effects remain external facts (FR-015/028)", committed, row.nextOffset,
				committed-row.nextOffset),
			facts:        facts,
			evidenceRefs: refs,
		})
	case committed < row.nextOffset:
		return c.add(opsObservationInput{
			category:   recovery.VerificationV6,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the restored PG progress %d leads the broker committed offset %d by %d; the "+
				"external consumption state cannot be proven (a broker commit may be pending) and re-delivery must "+
				"be absorbed by the inbox, never by re-running effects", row.nextOffset, committed,
				row.nextOffset-committed),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   eventConsumingCaps,
			required: externalRequirement(
				"the broker-side committed progress for the consumer's topic/partition",
			),
		})
	case row.nextOffset > 0 && inboxRows == 0 && versionRows == 0:
		return c.add(opsObservationInput{
			category:   recovery.VerificationV6,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the broker committed offset equals the restored next_offset %d, but the restore "+
				"point carries no consumer_inbox/consumer_versions history for the consumer; missing historical "+
				"idempotency records are never silently written back as processed (FR-028)", row.nextOffset),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   eventConsumingCaps,
			required: externalRequirement(
				"the historical consumer_inbox/consumer_versions records from a trusted copy of the data DB",
			),
		})
	}
	return c.add(opsObservationInput{
		category:     recovery.VerificationV6,
		objectKey:    objectKey,
		conclusion:   recovery.ConclusionConsistent,
		facts:        facts,
		evidenceRefs: refs,
	})
}

// observeQuarantine reads every open quarantine entry. An open entry means the
// event's effect was not applied and its outcome is unresolved: re-processing
// exists only through the authorized quarantine-replay path, which
// verification never invokes.
func (s *consumerFactsSource) observeQuarantine(ctx context.Context, c *opsObservationCollector) error {
	b := s.base
	rows, err := b.data.Query(ctx, `
		SELECT consumer_name, event_id::text, failure_class, first_seen_at, last_seen_at,
		       COALESCE(source_topic, ''), COALESCE(source_partition, -1), COALESCE(source_offset, -1)
		  FROM consumer_quarantine
		 WHERE status = 'open'
		 ORDER BY first_seen_at, id`)
	if err != nil {
		if relationMissing(err) {
			return c.add(opsObservationInput{
				category:   recovery.VerificationV6,
				objectKey:  "v6/consumer_quarantine?scope=all",
				conclusion: recovery.ConclusionUnknown,
				reason: "the consumer_quarantine table does not exist at the restore point (000015 not migrated); " +
					"poison/unknown-version isolation cannot be read and a missing relation is never an empty " +
					"quarantine (FR-016)",
				facts:        map[string]any{"table": "consumer_quarantine", "found": false},
				evidenceRefs: []string{"data:consumer_quarantine?scope=all"},
				directCaps:   eventConsumingCaps,
				required: externalRequirement(
					"the consumer_quarantine rows from a trusted copy of the data DB",
				),
			})
		}
		return fmt.Errorf("read consumer quarantine: %w", err)
	}
	type quarantineRow struct {
		consumerName    string
		eventID         string
		failureClass    string
		firstSeenAt     time.Time
		lastSeenAt      time.Time
		sourceTopic     string
		sourcePartition int32
		sourceOffset    int64
	}
	var quarantined []quarantineRow
	for rows.Next() {
		var row quarantineRow
		if err := rows.Scan(&row.consumerName, &row.eventID, &row.failureClass, &row.firstSeenAt,
			&row.lastSeenAt, &row.sourceTopic, &row.sourcePartition, &row.sourceOffset); err != nil {
			rows.Close()
			return fmt.Errorf("scan consumer quarantine: %w", err)
		}
		quarantined = append(quarantined, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read consumer quarantine: %w", err)
	}

	for _, row := range quarantined {
		objectKey := fmt.Sprintf("v6/consumer_quarantine?consumer=%s&event_id=%s", row.consumerName, row.eventID)
		facts := map[string]any{
			"table":            "consumer_quarantine",
			"consumer_name":    row.consumerName,
			"event_id":         row.eventID,
			"failure_class":    row.failureClass,
			"first_seen_at":    row.firstSeenAt.UTC().Format(time.RFC3339Nano),
			"last_seen_at":     row.lastSeenAt.UTC().Format(time.RFC3339Nano),
			"source_topic":     row.sourceTopic,
			"source_partition": row.sourcePartition,
			"source_offset":    row.sourceOffset,
		}
		if err := c.add(opsObservationInput{
			category:   recovery.VerificationV6,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("an open quarantine row (failure_class=%s) isolates event %s of consumer %s; its "+
				"effect was not applied and its outcome is unresolved — re-processing exists only through the "+
				"authorized quarantine-replay path and verification never triggers it (FR-027/028)",
				row.failureClass, row.eventID, row.consumerName),
			facts:        facts,
			evidenceRefs: []string{"data:consumer_quarantine?event_id=" + row.eventID},
			directCaps:   eventConsumingCaps,
			required: externalRequirement(
				"the audited quarantine replay/unblock decision for the event",
				"the downstream duplicate-effect assessment for the event",
			),
		}); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// V7: 014 reconciliation reference (000016, read-only)
// ---------------------------------------------------------------------------

type reconFactsSource struct{ base *opsSourceBase }

// Category implements recovery.VerificationSource.
func (s *reconFactsSource) Category() recovery.VerificationCategory { return recovery.VerificationV7 }

// Observe reads the 000016 reconciliation reference: task/checkpoint progress,
// uncovered recon_gap ranges, discrepancy tickets and their disposition/
// reverify history, the append-only audit trail and the default-deny
// permission registry. A missing/not-migrated relation is unknown; an
// unresolved task, an uncovered gap, a claimed scan attempt or a non-closed
// discrepancy ticket is unknown plus a gap (the restored difference history is
// not traceable to a terminal outcome). Nothing is claimed, disposed,
// reverified or closed; 014 remains its own authority (FR-014/020).
func (s *reconFactsSource) Observe(ctx context.Context) ([]recovery.SourceObservation, error) {
	c := &opsObservationCollector{base: s.base}
	if err := s.observeScanState(ctx, c); err != nil {
		return nil, err
	}
	if err := s.observeDiscrepancyState(ctx, c); err != nil {
		return nil, err
	}
	if err := s.observePermissionAudit(ctx, c); err != nil {
		return nil, err
	}
	return c.out, nil
}

// reconTablesMissing is the conservative unknown of one unreadable 000016
// reference object.
func (s *reconFactsSource) reconTablesMissing(c *opsObservationCollector, objectKey string, tables []string) error {
	return c.add(opsObservationInput{
		category:   recovery.VerificationV7,
		objectKey:  objectKey,
		conclusion: recovery.ConclusionUnknown,
		reason: "the 000016 reconciliation relations are missing/not migrated at the restore point; the 014 " +
			"difference/disposition reference cannot be read and a missing relation is not evidence that no " +
			"difference ever existed (FR-016)",
		facts:        map[string]any{"tables": tables, "found": false},
		evidenceRefs: tableRefs(tables),
		directCaps:   fundsDeliveryCaps,
		required: externalRequirement(
			"the 000016 reconciliation tables from a trusted copy of the data DB",
			"the 014 difference/disposition history for the restored window",
		),
	})
}

func tableRefs(tables []string) []string {
	refs := make([]string, 0, len(tables))
	for _, table := range tables {
		refs = append(refs, "data:"+table+"?scope=all")
	}
	return refs
}

func (s *reconFactsSource) observeScanState(ctx context.Context, c *opsObservationCollector) error {
	b := s.base
	objectKey := "v7/recon_scan_state?scope=all"
	tables := []string{"recon_task", "recon_checkpoint", "recon_gap", "recon_scan_attempt"}

	taskStates, missing, err := b.stateCounts(ctx,
		`SELECT state, count(*) FROM recon_task GROUP BY state ORDER BY state`)
	if err != nil {
		return fmt.Errorf("read recon task states: %w", err)
	}
	if missing {
		return s.reconTablesMissing(c, objectKey, tables)
	}
	var (
		checkpointRows int64
		checkpointSeq  int64
	)
	if err := b.data.QueryRow(ctx,
		`SELECT count(*), COALESCE(max(seq), 0) FROM recon_checkpoint`).
		Scan(&checkpointRows, &checkpointSeq); err != nil {
		if relationMissing(err) {
			return s.reconTablesMissing(c, objectKey, tables)
		}
		return fmt.Errorf("read recon checkpoints: %w", err)
	}
	gapRows, missing, err := b.singleCount(ctx, `SELECT count(*) FROM recon_gap`)
	if err != nil {
		return fmt.Errorf("read recon gaps: %w", err)
	}
	if missing {
		return s.reconTablesMissing(c, objectKey, tables)
	}
	attemptStates, missing, err := b.stateCounts(ctx,
		`SELECT state, count(*) FROM recon_scan_attempt GROUP BY state ORDER BY state`)
	if err != nil {
		return fmt.Errorf("read recon scan attempt states: %w", err)
	}
	if missing {
		return s.reconTablesMissing(c, objectKey, tables)
	}

	unfinishedTasks := taskStates["created"] + taskStates["running"] + taskStates["paused"] +
		taskStates["suspended_budget"]
	claimedAttempts := attemptStates["claimed"]
	facts := map[string]any{
		"tables":                    tables,
		"recon_task_states":         taskStates,
		"recon_checkpoint_rows":     checkpointRows,
		"recon_checkpoint_max_seq":  checkpointSeq,
		"recon_gap_rows":            gapRows,
		"recon_scan_attempt_states": attemptStates,
		"unfinished_tasks":          unfinishedTasks,
		"claimed_attempts":          claimedAttempts,
	}
	if gapRows > 0 || unfinishedTasks > 0 || claimedAttempts > 0 {
		return c.add(opsObservationInput{
			category:   recovery.VerificationV7,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the restored 014 reference state carries %d uncovered recon_gap row(s), %d "+
				"unfinished task(s) and %d claimed scan attempt(s); the reconciliation coverage is not closed and "+
				"the differences are not traceable to a terminal outcome", gapRows, unfinishedTasks, claimedAttempts),
			facts:        facts,
			evidenceRefs: tableRefs(tables),
			directCaps:   fundsDeliveryCaps,
			required: externalRequirement(
				"the terminal outcome (or explicit closure) of every task, gap and scan attempt",
				"the audited 014 disposition/reverify history covering the restored window",
			),
		})
	}
	return c.add(opsObservationInput{
		category:     recovery.VerificationV7,
		objectKey:    objectKey,
		conclusion:   recovery.ConclusionConsistent,
		facts:        facts,
		evidenceRefs: tableRefs(tables),
	})
}

func (s *reconFactsSource) observeDiscrepancyState(ctx context.Context, c *opsObservationCollector) error {
	b := s.base
	objectKey := "v7/discrepancy_state?scope=all"
	tables := []string{"discrepancy", "discrepancy_occurrence", "disposition", "reverify"}

	discrepancyStates, missing, err := b.stateCounts(ctx,
		`SELECT state, count(*) FROM discrepancy GROUP BY state ORDER BY state`)
	if err != nil {
		return fmt.Errorf("read discrepancy states: %w", err)
	}
	if missing {
		return s.reconTablesMissing(c, objectKey, tables)
	}
	occurrences, missing, err := b.singleCount(ctx, `SELECT count(*) FROM discrepancy_occurrence`)
	if err != nil {
		return fmt.Errorf("read discrepancy occurrences: %w", err)
	}
	if missing {
		return s.reconTablesMissing(c, objectKey, tables)
	}
	dispositions, missing, err := b.singleCount(ctx, `SELECT count(*) FROM disposition`)
	if err != nil {
		return fmt.Errorf("read dispositions: %w", err)
	}
	if missing {
		return s.reconTablesMissing(c, objectKey, tables)
	}
	reverifications, missing, err := b.singleCount(ctx, `SELECT count(*) FROM reverify`)
	if err != nil {
		return fmt.Errorf("read reverifications: %w", err)
	}
	if missing {
		return s.reconTablesMissing(c, objectKey, tables)
	}

	unresolved := discrepancyStates["open_claimable"] + discrepancyStates["claimed"] +
		discrepancyStates["disposing"] + discrepancyStates["pending_verify"] + discrepancyStates["reopened"]
	facts := map[string]any{
		"tables":             tables,
		"discrepancy_states": discrepancyStates,
		"occurrence_rows":    occurrences,
		"disposition_rows":   dispositions,
		"reverify_rows":      reverifications,
		"unresolved_tickets": unresolved,
	}
	if unresolved > 0 {
		return c.add(opsObservationInput{
			category:   recovery.VerificationV7,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("%d discrepancy ticket(s) are not closed at the restore point; the restored "+
				"difference history is not traceable to a terminal disposition and is never silently closed by "+
				"verification (FR-016/020)", unresolved),
			facts:        facts,
			evidenceRefs: tableRefs(tables),
			directCaps:   fundsDeliveryCaps,
			required: externalRequirement(
				"the terminal disposition/reverify verdict for every open ticket",
			),
		})
	}
	return c.add(opsObservationInput{
		category:     recovery.VerificationV7,
		objectKey:    objectKey,
		conclusion:   recovery.ConclusionConsistent,
		facts:        facts,
		evidenceRefs: tableRefs(tables),
	})
}

func (s *reconFactsSource) observePermissionAudit(ctx context.Context, c *opsObservationCollector) error {
	b := s.base
	objectKey := "v7/recon_permission?scope=all"
	tables := []string{"recon_permission", "recon_audit"}

	var (
		grants  int64
		actions int64
	)
	if err := b.data.QueryRow(ctx,
		`SELECT count(*), count(DISTINCT action) FROM recon_permission`).Scan(&grants, &actions); err != nil {
		if relationMissing(err) {
			return s.reconTablesMissing(c, objectKey, tables)
		}
		return fmt.Errorf("read recon permissions: %w", err)
	}
	var (
		auditRows   int64
		latestAudit *time.Time
	)
	if err := b.data.QueryRow(ctx,
		`SELECT count(*), max(created_at) FROM recon_audit`).Scan(&auditRows, &latestAudit); err != nil {
		if relationMissing(err) {
			return s.reconTablesMissing(c, objectKey, tables)
		}
		return fmt.Errorf("read recon audit: %w", err)
	}
	facts := map[string]any{
		"tables":             tables,
		"permission_grants":  grants,
		"permission_actions": actions,
		"audit_rows":         auditRows,
		"default_deny_scope": grants == 0,
	}
	if latestAudit != nil {
		facts["latest_audit_at"] = latestAudit.UTC().Format(time.RFC3339Nano)
	}
	return c.add(opsObservationInput{
		category:     recovery.VerificationV7,
		objectKey:    objectKey,
		conclusion:   recovery.ConclusionConsistent,
		facts:        facts,
		evidenceRefs: tableRefs(tables),
	})
}

// ---------------------------------------------------------------------------
// V8: authorization-surface drift (control-store external truth)
// ---------------------------------------------------------------------------

type authSurfaceSource struct{ base *opsSourceBase }

// Category implements recovery.VerificationSource.
func (s *authSurfaceSource) Category() recovery.VerificationCategory { return recovery.VerificationV8 }

// Observe reads the restored data-DB authorization surface (caller, api_key)
// and the control store's authorization_recheck record: the external-truth
// evidence that the post-restore-point revokes/grants were re-verified (and
// re-applied) by a non-executor. Without that record the authorization-surface
// drift is unprovable and the conclusion is unknown: the fund and delivery
// capabilities stay closed (contracts/resumption-gate.md §3). Verification
// never applies a revoke/grant and never touches key material.
func (s *authSurfaceSource) Observe(ctx context.Context) ([]recovery.SourceObservation, error) {
	b := s.base
	c := &opsObservationCollector{base: b}
	objectKey := "v8/authorization_surface?scope=all"
	refs := []string{
		"data:caller?scope=all",
		"data:api_key?scope=all",
		"control:recovery_isolation_check?item_key=authorization_recheck",
	}
	facts := map[string]any{"table": "caller"}

	var (
		callers        int64
		createDenied   int64
		callersUpdated *time.Time
	)
	if err := b.data.QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE NOT can_create), max(updated_at) FROM caller`).
		Scan(&callers, &createDenied, &callersUpdated); err != nil {
		if relationMissing(err) {
			return c.out, c.add(opsObservationInput{
				category:   recovery.VerificationV8,
				objectKey:  objectKey,
				conclusion: recovery.ConclusionUnknown,
				reason: "the caller table does not exist at the restore point; the authorization surface cannot be " +
					"read and a missing relation is not an empty surface (FR-016)",
				facts:        map[string]any{"table": "caller", "found": false},
				evidenceRefs: refs,
				directCaps:   fundsDeliveryCaps,
				required: externalRequirement(
					"the caller rows from a trusted copy of the data DB",
					"the external-truth evidence of the post-restore-point authorization-surface changes",
				),
			})
		}
		return nil, fmt.Errorf("read caller surface: %w", err)
	}
	facts["callers"] = callers
	facts["callers_create_denied"] = createDenied
	if callersUpdated != nil {
		facts["callers_updated_at"] = callersUpdated.UTC().Format(time.RFC3339Nano)
	}

	var (
		apiKeys       int64
		apiKeysActive int64
		latestRevoked *time.Time
		latestCreated *time.Time
	)
	if err := b.data.QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE revoked_at IS NULL), max(revoked_at), max(created_at) FROM api_key`).
		Scan(&apiKeys, &apiKeysActive, &latestRevoked, &latestCreated); err != nil {
		if relationMissing(err) {
			return c.out, c.add(opsObservationInput{
				category:   recovery.VerificationV8,
				objectKey:  objectKey,
				conclusion: recovery.ConclusionUnknown,
				reason: "the api_key table does not exist at the restore point; the credential surface cannot be " +
					"read and a missing relation is not an empty surface (FR-016)",
				facts:        map[string]any{"table": "api_key", "found": false},
				evidenceRefs: refs,
				directCaps:   fundsDeliveryCaps,
				required: externalRequirement(
					"the api_key rows from a trusted copy of the data DB",
					"the external-truth evidence of the post-restore-point authorization-surface changes",
				),
			})
		}
		return nil, fmt.Errorf("read api key surface: %w", err)
	}
	facts["api_keys"] = apiKeys
	facts["api_keys_active"] = apiKeysActive
	facts["api_keys_revoked"] = apiKeys - apiKeysActive
	if latestRevoked != nil {
		facts["latest_key_revoked_at"] = latestRevoked.UTC().Format(time.RFC3339Nano)
	}
	if latestCreated != nil {
		facts["latest_key_created_at"] = latestCreated.UTC().Format(time.RFC3339Nano)
	}

	// The external truth lives in the version-guarded control store: the
	// authorization_recheck isolation record of the open instance.
	var (
		instanceID  string
		state       string
		evidenceRef string
		summaryText string
		checkedBy   string
		checkedAt   *time.Time
		verifiedBy  string
		verifiedAt  *time.Time
	)
	err := b.control.Pool().QueryRow(ctx, `
SELECT i.instance_id::text, COALESCE(c.state, ''), COALESCE(c.evidence_ref, ''),
       COALESCE(c.checkpoint_summary::text, ''), COALESCE(c.checked_by, ''), c.checked_at,
       COALESCE(c.verified_by, ''), c.verified_at
  FROM recovery_instance i
  LEFT JOIN recovery_isolation_check c
    ON c.instance_id = i.instance_id AND c.item_key = 'authorization_recheck'
 WHERE i.state = 'open'`).Scan(&instanceID, &state, &evidenceRef, &summaryText, &checkedBy, &checkedAt,
		&verifiedBy, &verifiedAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		facts["control_evidence"] = "no_open_instance"
		return c.out, c.add(opsObservationInput{
			category:   recovery.VerificationV8,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: "no open recovery instance exists in the control store; the external-truth record of the " +
				"authorization surface cannot be located and the drift stays unprovable",
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   fundsDeliveryCaps,
			required: externalRequirement(
				"the open instance and its authorization_recheck external-truth record",
			),
		})
	case err != nil:
		return nil, fmt.Errorf("read authorization recheck evidence: %w", err)
	}
	facts["instance_id"] = instanceID
	controlEvidence := map[string]any{
		"state":           state,
		"evidence_ref":    evidenceRef,
		"summary_present": strings.TrimSpace(summaryText) != "",
	}
	if checkedBy != "" {
		controlEvidence["checked_by"] = checkedBy
	}
	if checkedAt != nil {
		controlEvidence["checked_at"] = checkedAt.UTC().Format(time.RFC3339Nano)
	}
	if verifiedBy != "" {
		controlEvidence["verified_by"] = verifiedBy
	}
	if verifiedAt != nil {
		controlEvidence["verified_at"] = verifiedAt.UTC().Format(time.RFC3339Nano)
	}
	facts["control_evidence"] = controlEvidence

	if state != string(recovery.ChecklistStateVerified) {
		reason := "the control store records no authorization_recheck evidence for the open instance; the " +
			"post-restore-point revokes/grants cannot be proven to have been re-checked/re-applied and the fund " +
			"and delivery capabilities stay closed (V8, resumption-gate §3)"
		switch state {
		case string(recovery.ChecklistStateEvidenced):
			reason = "the authorization_recheck evidence is collected but not verified by a non-executor " +
				"participant; the post-restore-point revokes/grants are not confirmed to have been " +
				"re-checked/re-applied and the fund and delivery capabilities stay closed"
		case string(recovery.ChecklistStateRejected):
			reason = "the authorization_recheck evidence was rejected; the post-restore-point revokes/grants must " +
				"be re-collected and re-verified before the fund and delivery capabilities can be considered"
		case string(recovery.ChecklistStatePending):
			reason = "the authorization_recheck item is pending; the post-restore-point revokes/grants are not yet " +
				"re-checked/re-applied and the fund and delivery capabilities stay closed"
		case "":
			// Absent row: the reason above already names it.
		default:
			reason = fmt.Sprintf("the authorization_recheck record carries state=%q outside the closed checklist "+
				"state set; the external-truth evidence is unprovable", state)
		}
		return c.out, c.add(opsObservationInput{
			category:     recovery.VerificationV8,
			objectKey:    objectKey,
			conclusion:   recovery.ConclusionUnknown,
			reason:       reason,
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   fundsDeliveryCaps,
			required: externalRequirement(
				"the external-truth evidence of the post-restore-point authorization-surface revokes/grants",
				"the non-executor verification of the re-check/re-apply result (resumption-gate §3)",
			),
		})
	}
	return c.out, c.add(opsObservationInput{
		category:     recovery.VerificationV8,
		objectKey:    objectKey,
		conclusion:   recovery.ConclusionConsistent,
		facts:        facts,
		evidenceRefs: refs,
	})
}

// ---------------------------------------------------------------------------
// V9: tool and dependency readiness
// ---------------------------------------------------------------------------

type readinessSource struct{ base *opsSourceBase }

// Category implements recovery.VerificationSource.
func (s *readinessSource) Category() recovery.VerificationCategory { return recovery.VerificationV9 }

// Observe probes the readiness boundaries: the data DSN, the control store,
// the canonical chain client and every assembled DependencyProbe (image/
// registry, signer boundary, additional DSNs). Any unreachable boundary or an
// empty probe set is unknown plus a gap naming every capability (V9 is the
// readiness prerequisite of all seven). A probe only checks reachability; no
// credential, key or signer session is ever acquired.
func (s *readinessSource) Observe(ctx context.Context) ([]recovery.SourceObservation, error) {
	b := s.base
	c := &opsObservationCollector{base: b}
	if err := s.observeBoundary(ctx, c, "data-db", "data:dsn", func(ctx context.Context) error {
		return b.data.Ping(ctx)
	}); err != nil {
		return nil, err
	}
	if err := s.observeBoundary(ctx, c, "control-store", "control:dsn", func(ctx context.Context) error {
		return b.control.Pool().Ping(ctx)
	}); err != nil {
		return nil, err
	}
	if err := s.observeBoundary(ctx, c, "chain-rpc", "chain:rpc", func(ctx context.Context) error {
		_, err := b.rpc.BlockNumber(ctx)
		return err
	}); err != nil {
		return nil, err
	}
	for _, probe := range b.deps {
		if probe == nil {
			if err := c.add(opsObservationInput{
				category:   recovery.VerificationV9,
				objectKey:  "v9/dependency?name=unnamed",
				conclusion: recovery.ConclusionUnknown,
				reason:     "a nil V9 dependency probe was configured; its boundary cannot be probed",
				facts:      map[string]any{"dependency": "unnamed", "reachable": false},
				evidenceRefs: []string{
					"dependency:unnamed",
				},
				directCaps: readinessCaps,
				required: externalRequirement(
					"a configured readiness probe for the unnamed boundary",
				),
			}); err != nil {
				return nil, err
			}
			continue
		}
		name := strings.TrimSpace(probe.Name())
		if name == "" {
			name = "unnamed"
		}
		if err := s.observeBoundary(ctx, c, name, "dependency:"+name, probe.Check); err != nil {
			return nil, err
		}
	}
	if len(b.deps) == 0 {
		if err := c.add(opsObservationInput{
			category:   recovery.VerificationV9,
			objectKey:  "v9/dependency_set?scope=all",
			conclusion: recovery.ConclusionUnknown,
			reason: "no V9 readiness probe is configured for the image/registry client, the signer boundary or " +
				"additional DSNs; those dependencies cannot be proven reachable and every capability's readiness " +
				"prerequisite stays closed (FR-014/020)",
			facts: map[string]any{
				"configured_probes": 0,
				"boundaries":        []string{"image/registry", "additional DSNs", "signer"},
			},
			evidenceRefs: []string{"dependency-set:configured"},
			directCaps:   readinessCaps,
			required: externalRequirement(
				"the assembled readiness probes for the image/client/DSN/signer boundaries",
			),
		}); err != nil {
			return nil, err
		}
	}
	return c.out, nil
}

// observeBoundary probes one readiness boundary and records consistent
// (reachable) or unknown plus a gap (unreachable). The redacted error text is
// an error class only; no credential or DSN material is ever recorded.
func (s *readinessSource) observeBoundary(ctx context.Context, c *opsObservationCollector, name, ref string, check func(context.Context) error) error {
	objectKey := "v9/dependency?name=" + name
	facts := map[string]any{"dependency": name, "boundary": ref}
	if err := check(ctx); err != nil {
		redacted := logx.Redact(err.Error())
		facts["reachable"] = false
		facts["error"] = redacted
		if kind := eth.KindOf(err); kind != "" {
			facts["error_class"] = string(kind)
		}
		return c.add(opsObservationInput{
			category:   recovery.VerificationV9,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the %s boundary is unreachable (%s); recovery and verification readiness cannot be "+
				"proven and every capability's readiness prerequisite stays closed (FR-014/020)", name, redacted),
			facts:        facts,
			evidenceRefs: []string{ref},
			directCaps:   readinessCaps,
			required: externalRequirement(
				"a reachable " + name + " boundary",
			),
		})
	}
	facts["reachable"] = true
	return c.add(opsObservationInput{
		category:     recovery.VerificationV9,
		objectKey:    objectKey,
		conclusion:   recovery.ConclusionConsistent,
		facts:        facts,
		evidenceRefs: []string{ref},
	})
}

// The five adapters implement the single read-only verification surface.
var (
	_ recovery.VerificationSource = (*outboxFactsSource)(nil)
	_ recovery.VerificationSource = (*consumerFactsSource)(nil)
	_ recovery.VerificationSource = (*reconFactsSource)(nil)
	_ recovery.VerificationSource = (*authSurfaceSource)(nil)
	_ recovery.VerificationSource = (*readinessSource)(nil)
)
