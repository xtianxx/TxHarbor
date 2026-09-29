//go:build integration

// recovery_eventboundary_integration_test.go is the T054 [US5] app-level
// layer: after a recovery instance is opened, the event-publisher and
// event-consumer paths admit through the real T012 gate over the real control
// store (both refusals and released passes), and the FR-027–FR-029 boundary
// report (T052) is observable over the real data database:
//
//   - before any release every events path refuses with a closed refusal_class
//     exposed and audited, and nothing is claimed, applied or advanced;
//   - after the release the same production wrappers run (claim/settle
//     admissions of the publisher, the Effect admission of the consumer);
//   - duplicate consumption of one event is absorbed by the persistent
//     inbox/version guard with zero extra real effects;
//   - the boundary report detects a rollback (broker position ahead of the
//     restored PG progress), reports missing idempotency history and possible
//     external duplicate effects, keeps an unreadable broker unknown, and
//     never displays an unverified state as normal;
//   - the reference consumer ledger is evidence material only, explicitly not
//     a production ledger, and the report never claims cross-system
//     exactly-once or external-ledger consistency.
package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/recovery"
)

// t054BoundaryTopic mirrors the configured canonical topic of fullEventsEnv.
const t054BoundaryTopic = "txharbor.events.v1"

// t054BrokerOffsets is a controllable read-only broker offset surface: the
// tests set the committed offset (or an unreadable error) and observe that the
// boundary report compares instead of defaulting.
type t054BrokerOffsets struct {
	mu      sync.Mutex
	offsets map[string]int64
	err     error
}

// CommittedOffset implements recovery.EventBoundaryBrokerOffsets.
func (b *t054BrokerOffsets) CommittedOffset(_ context.Context, topic string, partition int) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return 0, b.err
	}
	value, ok := b.offsets[fmt.Sprintf("%s/%d", topic, partition)]
	if !ok {
		return 0, fmt.Errorf("no committed offset for %s/%d", topic, partition)
	}
	return value, nil
}

// set replaces the committed offset of one topic/partition.
func (b *t054BrokerOffsets) set(topic string, partition int, offset int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.offsets[fmt.Sprintf("%s/%d", topic, partition)] = offset
}

// setError makes the broker surface unreadable.
func (b *t054BrokerOffsets) setError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.err = err
}

// t054PhasePublisher is a deterministic outboxPhasePublisher double: it counts
// the claim/settle calls the gated wrapper allows and never touches the outbox.
type t054PhasePublisher struct {
	claimCalls   int
	publishCalls int
	records      []events.OutboxRecord
}

func (p *t054PhasePublisher) ClaimBatch(context.Context) ([]events.OutboxRecord, error) {
	p.claimCalls++
	return p.records, nil
}

func (p *t054PhasePublisher) PublishClaimed(_ context.Context, records []events.OutboxRecord) (events.PublishOutcome, error) {
	p.publishCalls++
	return events.PublishOutcome{Claimed: len(records), Acked: len(records)}, nil
}

func (p *t054PhasePublisher) RefreshGauges(context.Context) error { return nil }

// t054BoundaryRecord renders one real transport envelope through the
// production renderer (OutboxRecord.EnvelopeJSON), so the consumer path parses
// the exact wire shape.
func t054BoundaryRecord(t *testing.T, eventID uuid.UUID, aggregateID string) []byte {
	t.Helper()
	record := events.OutboxRecord{
		OutboxID:         1,
		EventID:          eventID,
		EventType:        events.EventTypeDepositObservationStatusChanged,
		SchemaVersion:    events.SchemaVersionV1,
		IdentityKind:     events.IdentityKindBusinessObject,
		AggregateType:    "deposit_observation",
		AggregateID:      aggregateID,
		AggregateVersion: 1,
		Payload:          []byte(`{"from_state":"pending","to_state":"confirmed","reason":"t054"}`),
		OccurredAt:       time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC),
	}
	value, err := record.EnvelopeJSON()
	if err != nil {
		t.Fatalf("render transport envelope: %v", err)
	}
	return value
}

// t054Refusal asserts one events-path error is the production refusal type
// carrying a known closed class.
func t054Refusal(t *testing.T, err error, capability recovery.Capability, what string) *eventsGateRefusedError {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected a recovery refusal", what)
	}
	var refused *eventsGateRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("%s: error = %v, want *eventsGateRefusedError", what, err)
	}
	if refused.Capability != capability {
		t.Fatalf("%s: refusal capability = %s, want %s", what, refused.Capability, capability)
	}
	if !refused.RefusalClass.Known() {
		t.Fatalf("%s: refusal class %q is outside the closed set", what, refused.RefusalClass)
	}
	return refused
}

// TestRecoveryEventBoundaryGateAndRollbackObservable is the T054 acceptance.
func TestRecoveryEventBoundaryGateAndRollbackObservable(t *testing.T) {
	scene := t045NewScene(t)
	ctx := scene.ctx

	env := scene.eventsCommandEnv()
	env[config.EnvRecoveryInstance] = scene.instanceID
	cfg, err := config.Load(envGetter(env))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	// T050 canonical scopes: chain + capability, no entry identity. The
	// event_publishing dependency (chain_scan) is evaluated by the gate at the
	// mapped dependency scope, so one chain_scan release on this chain covers
	// both event capabilities.
	pubScope, err := recovery.CapabilityScope(cfg.ChainID, recovery.CapabilityEventPublishing)
	if err != nil {
		t.Fatalf("CapabilityScope(event_publishing): %v", err)
	}
	consScope, err := recovery.CapabilityScope(cfg.ChainID, recovery.CapabilityEventConsuming)
	if err != nil {
		t.Fatalf("CapabilityScope(event_consuming): %v", err)
	}
	chainScanScope, err := recovery.CapabilityScope(cfg.ChainID, recovery.CapabilityChainScan)
	if err != nil {
		t.Fatalf("CapabilityScope(chain_scan): %v", err)
	}

	pubWiring, err := assembleEventsRecovery(ctx, cfg, envGetter(env), recovery.CapabilityEventPublishing)
	if err != nil {
		t.Fatalf("assemble event-publisher wiring: %v", err)
	}
	defer pubWiring.close()
	consWiring, err := assembleEventsRecovery(ctx, cfg, envGetter(env), recovery.CapabilityEventConsuming)
	if err != nil {
		t.Fatalf("assemble event-consumer wiring: %v", err)
	}
	defer consWiring.close()
	if pubWiring == nil || consWiring == nil {
		t.Fatal("a bound recovery process with the control store configured must assemble the gate wiring")
	}

	// Seed every isolation dependency set before any approval/release.
	scene.seedIsolation(t, recovery.CapabilityChainScan)
	scene.seedIsolation(t, recovery.CapabilityEventPublishing)
	scene.seedIsolation(t, recovery.CapabilityEventConsuming)

	reference, err := events.NewReferenceConsumer(scene.dataPool, events.ConsumerOptions{
		GapWait:     200 * time.Millisecond,
		BackoffBase: 10 * time.Millisecond,
		BackoffMax:  100 * time.Millisecond,
		RetryLimit:  4,
		ChainID:     int64(cfg.ChainID),
		Jitter:      func() float64 { return 0 },
	})
	if err != nil {
		t.Fatalf("NewReferenceConsumer: %v", err)
	}
	if err := reference.EnsureLedgerSchema(ctx); err != nil {
		t.Fatalf("EnsureLedgerSchema: %v", err)
	}
	refusals := &consumerGateRefusals{}
	gatedEffect := &gatedConsumerEffect{
		inner: reference.Consumer.Effect, gate: consWiring, onRefusal: refusals.record,
	}

	// -----------------------------------------------------------------------
	// Default deny: both paths refuse before any claim/effect and audit it.
	// -----------------------------------------------------------------------
	t.Run("default_deny_before_release", func(t *testing.T) {
		auditBefore := recovRefusedAuditCount(t, scene.recovScene)

		t054Refusal(t, pubWiring.require(ctx, recovery.CapabilityEventPublishing, "startup"),
			recovery.CapabilityEventPublishing, "event-publisher startup admission")
		phase := &t054PhasePublisher{records: []events.OutboxRecord{{OutboxID: 1}}}
		gated := &gatedOutboxPublisher{inner: phase, gate: pubWiring}
		_, err := gated.PublishOnce(ctx)
		t054Refusal(t, err, recovery.CapabilityEventPublishing, "event-publisher claim/settle admission")
		if phase.claimCalls != 0 || phase.publishCalls != 0 {
			t.Fatalf("refused publisher claimed=%d published=%d, want 0/0", phase.claimCalls, phase.publishCalls)
		}

		t054Refusal(t, consWiring.require(ctx, recovery.CapabilityEventConsuming, "startup"),
			recovery.CapabilityEventConsuming, "event-consumer startup admission")
		tx, err := scene.dataPool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin effect transaction: %v", err)
		}
		err = gatedEffect.Apply(ctx, tx, events.Envelope{EventID: uuid.New()})
		_ = tx.Rollback(ctx)
		t054Refusal(t, err, recovery.CapabilityEventConsuming, "event-consumer Effect admission")
		if refusals.first() == nil {
			t.Fatal("the entry-visible refusal record was not populated")
		}

		if n := recovCount(t, ctx, scene.dataPool, `SELECT count(*) FROM consumer_inbox`); n != 0 {
			t.Fatalf("consumer_inbox = %d while isolated, want 0", n)
		}
		if n := recovCount(t, ctx, scene.dataPool, `SELECT count(*) FROM consumer_progress`); n != 0 {
			t.Fatalf("consumer_progress = %d while isolated, want 0", n)
		}
		if after := recovRefusedAuditCount(t, scene.recovScene); after <= auditBefore {
			t.Fatalf("events refusals were not audited (before=%d after=%d)", auditBefore, after)
		}
	})

	// -----------------------------------------------------------------------
	// Release in dependency order (chain_scan -> event_publishing /
	// event_consuming) and run the same wrappers.
	// -----------------------------------------------------------------------
	scene.releaseValidAt(t, recovery.CapabilityChainScan, chainScanScope)
	scene.releaseValidAt(t, recovery.CapabilityEventPublishing, pubScope)
	scene.releaseValidAt(t, recovery.CapabilityEventConsuming, consScope)

	if err := pubWiring.require(ctx, recovery.CapabilityEventPublishing, "startup"); err != nil {
		t.Fatalf("released event-publishing startup admission: %v", err)
	}
	if err := consWiring.require(ctx, recovery.CapabilityEventConsuming, "startup"); err != nil {
		t.Fatalf("released event-consuming startup admission: %v", err)
	}
	phase := &t054PhasePublisher{records: []events.OutboxRecord{{OutboxID: 1}}}
	gated := &gatedOutboxPublisher{inner: phase, gate: pubWiring}
	outcome, err := gated.PublishOnce(ctx)
	if err != nil {
		t.Fatalf("released publisher cycle: %v", err)
	}
	if phase.claimCalls != 1 || phase.publishCalls != 1 || outcome.Claimed != 1 {
		t.Fatalf("released publisher claims=%d publishes=%d outcome=%+v, want 1/1/1",
			phase.claimCalls, phase.publishCalls, outcome)
	}

	// The consumer path: two deliveries of one event through the real T4
	// transaction with the production Effect admission.
	reference.Consumer.Effect = gatedEffect
	eventID := uuid.New()
	msg := events.Message{Topic: t054BoundaryTopic, Partition: 0, Offset: 0,
		Value: t054BoundaryRecord(t, eventID, "t054-boundary")}

	upstreamBefore := t054UpstreamCounts(t, scene)
	first, err := reference.Consumer.Process(ctx, msg)
	if err != nil {
		t.Fatalf("first Process: %v", err)
	}
	if first.Outcome != events.OutcomeApplied {
		t.Fatalf("first Process = %+v, want applied", first)
	}
	duplicate, err := reference.Consumer.Process(ctx, msg)
	if err != nil {
		t.Fatalf("duplicate Process: %v", err)
	}
	if duplicate.Outcome != events.OutcomeDuplicate {
		t.Fatalf("duplicate Process = %+v, want duplicate", duplicate)
	}
	if n, err := reference.LedgerApplications(ctx, eventID); err != nil || n != 1 {
		t.Fatalf("reference ledger rows = %d (err %v), want 1 (the duplicate must not re-apply)", n, err)
	}
	if n := recovCount(t, ctx, scene.dataPool, `SELECT count(*) FROM consumer_inbox
		WHERE consumer_name = $1 AND event_id = $2`, events.RefConsumerName, eventID); n != 1 {
		t.Fatalf("consumer_inbox rows = %d, want 1", n)
	}
	if n := recovCount(t, ctx, scene.dataPool, `SELECT next_offset FROM consumer_progress
		WHERE consumer_name = $1 AND topic = $2 AND partition = 0`,
		events.RefConsumerName, t054BoundaryTopic); n != 1 {
		t.Fatalf("consumer_progress next_offset = %d, want 1", n)
	}
	t054RequireUpstreamUnchanged(t, scene, upstreamBefore)

	// The reference ledger is evidence material only, explicitly not a ledger.
	if got := reference.BoundaryStatement(); got != events.ReferenceBoundaryStatement {
		t.Fatalf("reference boundary statement drifted:\n%s", got)
	}
	if !strings.Contains(events.ReferenceBoundaryStatement, "not a production ledger") ||
		!strings.Contains(events.ReferenceBoundaryStatement, "not authoritative") {
		t.Fatalf("reference boundary statement must state its non-ledger status: %s", events.ReferenceBoundaryStatement)
	}

	// -----------------------------------------------------------------------
	// Boundary report observability (T052).
	// -----------------------------------------------------------------------
	broker := &t054BrokerOffsets{offsets: map[string]int64{t054BoundaryTopic + "/0": 1}}
	inspector, err := recovery.NewEventBoundaryInspector(recovery.EventBoundaryOptions{
		Data: scene.dataPool, Broker: broker,
	})
	if err != nil {
		t.Fatalf("NewEventBoundaryInspector: %v", err)
	}
	dataBefore := t054DataFingerprint(t, scene)
	auditBefore := recovRefusedAuditCount(t, scene.recovScene)

	report, err := inspector.Report(ctx)
	if err != nil {
		t.Fatalf("boundary Report: %v", err)
	}
	if report.Conclusion != recovery.ConclusionConsistent || !report.Passes() {
		t.Fatalf("boundary report = %+v, want consistent after the absorbed duplicate", report)
	}
	if report.CrossSystemExactlyOnce {
		t.Fatal("the boundary report claimed cross-system exactly-once")
	}
	if report.VerifiableScope != recovery.EventBoundaryScopeWithoutReceipts {
		t.Fatalf("verifiable scope = %q, want the project-only scope (no real receipts connected)", report.VerifiableScope)
	}
	if report.DedupIdentity.EventIDColumn != "event_id" || report.DedupIdentity.SourceTopicColumn != "topic" {
		t.Fatalf("dedup identity = %+v, want event_id + source triple", report.DedupIdentity)
	}

	// Unverified/rolled-back states are never displayed as normal.
	broker.set(t054BoundaryTopic, 0, 2)
	report, err = inspector.Report(ctx)
	if err != nil {
		t.Fatalf("rollback Report: %v", err)
	}
	if report.Conclusion != recovery.ConclusionDivergent || report.Passes() {
		t.Fatalf("rollback report = %+v, want divergent (never normal)", report)
	}
	if !report.RollbackDetected || len(report.PossibleExternalDuplicateEffects) == 0 {
		t.Fatalf("rollback report = %+v, want the rollback and possible-duplicate report", report)
	}
	if after := t054DataFingerprint(t, scene); after != dataBefore {
		t.Fatalf("the boundary report changed the data state:\nbefore=%s\nafter=%s", dataBefore, after)
	}
	broker.set(t054BoundaryTopic, 0, 1)

	if _, err := scene.dataPool.Exec(ctx, `DELETE FROM consumer_inbox WHERE consumer_name = $1`,
		events.RefConsumerName); err != nil {
		t.Fatalf("delete inbox history: %v", err)
	}
	if _, err := scene.dataPool.Exec(ctx, `DELETE FROM consumer_versions WHERE consumer_name = $1`,
		events.RefConsumerName); err != nil {
		t.Fatalf("delete version history: %v", err)
	}
	dataAfterDeletes := t054DataFingerprint(t, scene)
	report, err = inspector.Report(ctx)
	if err != nil {
		t.Fatalf("missing-history Report: %v", err)
	}
	if report.Conclusion != recovery.ConclusionUnknown || report.Passes() {
		t.Fatalf("missing-history report = %+v, want unknown (never normal)", report)
	}
	if len(report.MissingIdempotencyHistory) != 1 || len(report.PossibleExternalDuplicateEffects) != 1 {
		t.Fatalf("missing-history report = %+v, want one missing-history and one possible-duplicate entry", report)
	}
	if !strings.Contains(report.Checks[0].Reason, "never") {
		t.Fatalf("missing-history reason %q does not refuse the backfill/re-execution", report.Checks[0].Reason)
	}

	broker.setError(errors.New("broker unreachable after the restore"))
	report, err = inspector.Report(ctx)
	if err != nil {
		t.Fatalf("unreadable-broker Report: %v", err)
	}
	if report.Conclusion != recovery.ConclusionUnknown || report.Passes() {
		t.Fatalf("unreadable-broker report = %+v, want unknown (never normal)", report)
	}
	if len(report.Checks) != 1 || report.Checks[0].OffsetRelation != recovery.EventOffsetUnprovable {
		t.Fatalf("unreadable-broker checks = %+v, want one unprovable relation", report.Checks)
	}

	// The whole report sequence is read-only: the data state and the control
	// audit are untouched (the report neither backfills nor advances).
	if after := t054DataFingerprint(t, scene); after != dataAfterDeletes {
		t.Fatalf("the missing-history reports changed the data state:\nbefore=%s\nafter=%s", dataAfterDeletes, after)
	}
	if after := recovRefusedAuditCount(t, scene.recovScene); after != auditBefore {
		t.Fatalf("the boundary report wrote control-store audit rows (%d -> %d)", auditBefore, after)
	}
	if n, err := reference.LedgerApplications(ctx, eventID); err != nil || n != 1 {
		t.Fatalf("reference ledger rows after the reports = %d (err %v), want 1 (no reprocessing)", n, err)
	}
}

// t054UpstreamCounts counts the authority tables a consumed/replayed event must
// never add a row to (a processed event is not a send permission).
func t054UpstreamCounts(t *testing.T, scene *t045Scene) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for _, table := range []string{
		"withdrawal_requests", "payment_intents", "nonce_bindings",
		"tx_attempts", "tx_send_attempts", "signing_requests",
	} {
		out[table] = int64(recovCount(t, scene.ctx, scene.dataPool, "SELECT count(*) FROM "+table))
	}
	return out
}

// t054RequireUpstreamUnchanged asserts no authority row appeared.
func t054RequireUpstreamUnchanged(t *testing.T, scene *t045Scene, before map[string]int64) {
	t.Helper()
	for table, want := range before {
		if got := int64(recovCount(t, scene.ctx, scene.dataPool, "SELECT count(*) FROM "+table)); got != want {
			t.Fatalf("%s rows = %d, want %d (an event never reaches an upstream writer)", table, got, want)
		}
	}
}

// t054DataFingerprint renders a deterministic content fingerprint of the event
// boundary tables plus the reference ledger, so "the report wrote nothing" is
// asserted on content, not only on row counts.
func t054DataFingerprint(t *testing.T, scene *t045Scene) string {
	t.Helper()
	ctx := scene.ctx
	var parts []string
	for _, query := range []string{
		`SELECT COALESCE(string_agg(to_jsonb(t)::text, '|' ORDER BY to_jsonb(t)::text), '') FROM consumer_inbox t`,
		`SELECT COALESCE(string_agg(to_jsonb(t)::text, '|' ORDER BY to_jsonb(t)::text), '') FROM consumer_versions t`,
		`SELECT COALESCE(string_agg(to_jsonb(t)::text, '|' ORDER BY to_jsonb(t)::text), '') FROM consumer_progress t`,
		`SELECT COALESCE(string_agg(to_jsonb(t)::text, '|' ORDER BY to_jsonb(t)::text), '') FROM consumer_quarantine t`,
		`SELECT COALESCE(string_agg(to_jsonb(t)::text, '|' ORDER BY to_jsonb(t)::text), '') FROM ` + events.RefLedgerTable + ` t`,
	} {
		var value string
		if err := scene.dataPool.QueryRow(ctx, query).Scan(&value); err != nil {
			t.Fatalf("data fingerprint (%s): %v", query, err)
		}
		parts = append(parts, value)
	}
	return strings.Join(parts, "||")
}
