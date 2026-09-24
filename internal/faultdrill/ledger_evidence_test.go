//go:build fault

// ledger_evidence_test.go is the T081 FR-16 ledger evidence (SC-12;
// verification.md §5): under duplicate delivery the reference consumer's
// simulated upstream ledger records exactly one effective application per
// event, and the evidence carries the explicit boundary declaration — it
// covers only this project's event identity/version/delivery semantics plus
// the consumer idempotency contract and the reference-consumer demonstration;
// it is NOT a guarantee about any external real ledger.
//
// Duplicate delivery is exercised on two levels:
//
//  1. transport level: the durable outbox rows are reset to pending (a
//     drill-local fault injection, never a production path) and republished
//     through the real publisher into the real Kafka broker; the real group
//     consumer consumes the duplicate records and the persistent inbox
//     absorbs them (committed offsets advance, effects do not);
//  2. per-event level: the byte-identical transport envelope of every event
//     is fed to the real reference consumer twice more; both redeliveries
//     must return the duplicate outcome and must not touch the ledger.
//
// Evidence: ledger_counts.json (deliveries, attempts, inbox/ledger rows,
// duplicate outcomes, per-event applications) and boundary.md plus
// boundary_scan.json (the FR-16 boundary statement verbatim and a scan that
// finds 0 affirmative external-ledger guarantee claims).
package faultdrill

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/testutil"
)

// groupCommittedSum sums the committed offsets of one consumer group: it
// advances only after the consumer committed progress past a processed record
// (Kafka commits strictly after the effect transaction).
func groupCommittedSum(ctx context.Context, env *Env, group string) (int64, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(env.Kafka.Brokers()...))
	if err != nil {
		return 0, fmt.Errorf("kafka client: %w", err)
	}
	defer cl.Close()
	committed, err := kadm.NewClient(cl).FetchOffsets(ctx, group)
	if err != nil {
		return 0, fmt.Errorf("fetch offsets: %w", err)
	}
	var total int64
	for _, partitions := range committed {
		for _, offset := range partitions {
			if offset.Err == nil && offset.At > 0 {
				total += offset.At
			}
		}
	}
	return total, nil
}

// loadOutboxRecord materializes one committed outbox row into the exported
// record shape so the exact published bytes can be rendered for redelivery.
func loadOutboxRecord(ctx context.Context, env *Env, eventID uuid.UUID) (events.OutboxRecord, error) {
	var rec events.OutboxRecord
	var eventIDText string
	var identityKind string
	var payload []byte
	var chainID, blockNumber, logIndex, recoveryVersion *int64
	var blockHash, txHash *string
	var revises *string
	err := env.Pool.QueryRow(ctx, `
SELECT id, event_id::text, event_type, schema_version, identity_kind,
       aggregate_type, aggregate_id, aggregate_version, payload, occurred_at,
       chain_id, block_number, block_hash, tx_hash, log_index,
       recovery_version, revises_event_id::text, attempt_count
FROM outbox_events WHERE event_id = $1`, eventID).Scan(
		&rec.OutboxID, &eventIDText, &rec.EventType, &rec.SchemaVersion, &identityKind,
		&rec.AggregateType, &rec.AggregateID, &rec.AggregateVersion, &payload, &rec.OccurredAt,
		&chainID, &blockNumber, &blockHash, &txHash, &logIndex,
		&recoveryVersion, &revises, &rec.AttemptCount)
	if err != nil {
		return events.OutboxRecord{}, fmt.Errorf("load outbox record: %w", err)
	}
	parsed, err := uuid.Parse(eventIDText)
	if err != nil {
		return events.OutboxRecord{}, fmt.Errorf("parse event id: %w", err)
	}
	rec.EventID = parsed
	rec.IdentityKind = events.IdentityKind(identityKind)
	rec.Payload = payload
	rec.ChainID = chainID
	rec.BlockNumber = blockNumber
	rec.BlockHash = blockHash
	rec.TxHash = txHash
	if logIndex != nil {
		li := int(*logIndex)
		rec.LogIndex = &li
	}
	rec.RecoveryVersion = recoveryVersion
	if revises != nil {
		revised, err := uuid.Parse(*revises)
		if err != nil {
			return events.OutboxRecord{}, fmt.Errorf("parse revises event id: %w", err)
		}
		rec.RevisesEventID = &revised
	}
	return rec, nil
}

// guaranteeClaimRe finds every guarantee phrase; a claim is affirmative when
// its own clause carries no negation.
var guaranteeClaimRe = regexp.MustCompile(`(?i)guarantee`)

// negationRe recognizes the negation vocabulary inside one clause ("not",
// "never", "no", "non-"), covering "is not a guarantee" and "does NOT
// guarantee" alike.
var negationRe = regexp.MustCompile(`(?i)\b(not|never|no|non-)\b`)

// externalGuaranteeClaims returns the affirmative guarantee phrases in text:
// every "guarantee" occurrence whose own clause (the text after the last
// sentence delimiter before it) carries no negation. The T081 scan requires
// this list to be empty (0 external guarantee claims). It is a heuristic over
// authored evidence text, deliberately conservative in the other direction: a
// clause that mixes a negation with an affirmative claim would be missed,
// which is acceptable for a self-scan of evidence we author.
func externalGuaranteeClaims(text string) []string {
	var claims []string
	for _, match := range guaranteeClaimRe.FindAllStringIndex(text, -1) {
		start := match[0] - 80
		if start < 0 {
			start = 0
		}
		before := strings.TrimSpace(text[start:match[0]])
		// Look at the last clause only, so an unrelated earlier negation does
		// not launder a later affirmative claim.
		if idx := strings.LastIndexAny(before, ".;:!?"); idx >= 0 {
			before = strings.TrimSpace(before[idx+1:])
		}
		if negationRe.MatchString(before) || before == "" {
			continue
		}
		end := match[1] + 60
		if end > len(text) {
			end = len(text)
		}
		claims = append(claims, strings.TrimSpace(text[start:end]))
	}
	return claims
}

// TestLedgerEvidenceExactlyOnceUnderRedelivery is the T081 acceptance.
func TestLedgerEvidenceExactlyOnceUnderRedelivery(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	env, err := StartEnv(ctx)
	if err != nil {
		t.Fatalf("StartEnv: %v", err)
	}
	t.Cleanup(func() { _ = env.Close(context.Background()) })

	if err := env.EnsureReferenceLedger(ctx); err != nil {
		t.Fatalf("reference ledger schema: %v", err)
	}
	record := func(kind string, data any) {
		t.Helper()
		if err := env.Evidence.Record(kind, data); err != nil {
			t.Fatalf("record evidence %s: %v", kind, err)
		}
	}

	// --- phase 1: real delivery through the real broker ----------------------
	const total = 10
	eventIDs := make([]uuid.UUID, 0, total)
	for i := 0; i < total; i++ {
		id, err := env.AppendDepositEvent(ctx, 2000+i)
		if err != nil {
			t.Fatalf("append event %d: %v", i, err)
		}
		parsed, err := uuid.Parse(id)
		if err != nil {
			t.Fatalf("parse appended event id %q: %v", id, err)
		}
		eventIDs = append(eventIDs, parsed)
	}
	pubLoop := startPublisherLoop(t, ctx, env, "ledger-evidence", 10, 100*time.Millisecond, nil)
	defer pubLoop.stop(t)
	consLoop, reference := startConsumerLoop(t, ctx, env)
	defer consLoop.stop(t)

	if err := WaitFor(ctx, 4*time.Minute, func() (bool, error) {
		pubLoop.check(t)
		consLoop.check(t)
		rows, err := reference.LedgerCount(ctx)
		if err != nil {
			return false, err
		}
		return rows == total, nil
	}); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	assertLedgerExactlyOnce(t, ctx, env, total)
	committedAfterFirst, err := groupCommittedSum(ctx, env, "faultdrill."+events.RefConsumerName)
	if err != nil {
		t.Fatalf("committed offsets after first delivery: %v", err)
	}
	record("delivery:first", map[string]any{"ledger_rows": drillLedgerRows(t, ctx, env), "committed_offsets": committedAfterFirst})

	// --- phase 2: transport-level duplicate delivery -------------------------
	// Drill-local fault injection: reset the committed rows to pending so the
	// real publisher publishes byte-identical envelopes a second time. The
	// attempt counter proves the republish; the ledger must not grow.
	if _, err := env.Pool.Exec(ctx, `
UPDATE outbox_events
SET publish_state = 'pending', published_at = NULL, claim_owner = NULL,
    claim_expires_at = NULL, next_attempt_at = now()
WHERE event_id = ANY($1::uuid[])`, eventIDs); err != nil {
		t.Fatalf("reset rows for republish: %v", err)
	}
	if err := WaitFor(ctx, 2*time.Minute, func() (bool, error) {
		pubLoop.check(t)
		consLoop.check(t)
		var republished int64
		if err := env.Pool.QueryRow(ctx, `
SELECT count(*) FROM outbox_events
WHERE event_id = ANY($1::uuid[]) AND publish_state = 'published' AND attempt_count >= 2`, eventIDs).Scan(&republished); err != nil {
			return false, err
		}
		return republished == total, nil
	}); err != nil {
		t.Fatalf("transport republish: %v", err)
	}
	// The group committed offsets must advance past the duplicate records:
	// commits happen strictly after the effect transaction, so this proves the
	// real consumer consumed the duplicates.
	if err := WaitFor(ctx, 2*time.Minute, func() (bool, error) {
		pubLoop.check(t)
		consLoop.check(t)
		sum, err := groupCommittedSum(ctx, env, "faultdrill."+events.RefConsumerName)
		if err != nil {
			return false, err
		}
		return sum >= committedAfterFirst+total, nil
	}); err != nil {
		t.Fatalf("duplicate records were not consumed: %v", err)
	}
	assertLedgerExactlyOnce(t, ctx, env, total)

	var attempts, inboxRows int64
	if err := env.Pool.QueryRow(ctx, `SELECT coalesce(sum(attempt_count), 0) FROM outbox_events WHERE event_id = ANY($1::uuid[])`, eventIDs).Scan(&attempts); err != nil {
		t.Fatalf("attempt sum: %v", err)
	}
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM consumer_inbox WHERE consumer_name = $1`, events.RefConsumerName).Scan(&inboxRows); err != nil {
		t.Fatalf("inbox rows: %v", err)
	}
	record("delivery:transport_duplicate", map[string]any{
		"republished": total, "attempt_sum": attempts, "inbox_rows": inboxRows,
		"ledger_rows": drillLedgerRows(t, ctx, env),
	})

	// --- phase 3: deterministic per-event redelivery -------------------------
	// Feed the byte-identical envelope to the real reference consumer twice
	// per event. Both redeliveries must report the duplicate outcome and leave
	// the ledger at one application per event. The synthetic partition keeps
	// the deterministic redeliveries away from the real consumer's partitions.
	duplicateOutcomes := 0
	for i, id := range eventIDs {
		rec, err := loadOutboxRecord(ctx, env, id)
		if err != nil {
			t.Fatalf("load event %s: %v", id, err)
		}
		envelope, err := rec.EnvelopeJSON()
		if err != nil {
			t.Fatalf("render envelope %s: %v", id, err)
		}
		for attempt := 0; attempt < 2; attempt++ {
			result, err := reference.Process(ctx, events.Message{
				Topic:     testutil.KafkaTopic,
				Partition: 999,
				Offset:    int64(i*2 + attempt),
				Value:     envelope,
			})
			if err != nil {
				t.Fatalf("redeliver event %s attempt %d: %v", id, attempt, err)
			}
			if result.Outcome != events.OutcomeDuplicate {
				t.Fatalf("redelivery outcome for %s = %q, want duplicate", id, result.Outcome)
			}
			duplicateOutcomes++
		}
		applications, err := reference.LedgerApplications(ctx, id)
		if err != nil {
			t.Fatalf("ledger applications %s: %v", id, err)
		}
		if applications != 1 {
			t.Fatalf("ledger applications for %s = %d, want exactly 1", id, applications)
		}
	}
	// The deterministic redeliveries must not have moved any other effect.
	assertLedgerExactlyOnce(t, ctx, env, total)

	counts := map[string]any{
		"events":                        total,
		"transport_republished":         total,
		"publisher_attempt_sum":         attempts,
		"consumer_inbox_rows":           inboxRows,
		"ledger_rows":                   drillLedgerRows(t, ctx, env),
		"ledger_events_with_duplicates": 0,
		"deterministic_redeliveries":    duplicateOutcomes,
		"redelivery_outcome":            string(events.OutcomeDuplicate),
		"applications_per_event":        1,
		"open_quarantine":               0,
	}
	if err := env.Evidence.WriteJSON("ledger_counts.json", counts); err != nil {
		t.Fatalf("write ledger counts: %v", err)
	}
	record("ledger:counts", counts)

	// --- phase 4: boundary declaration and the 0-claims scan -----------------
	boundary := "# FR-16 ledger evidence boundary\n\n" +
		"Scope of this evidence: this project's event identity/version/delivery semantics, " +
		"the consumer idempotency contract (persistent inbox, version guard, quarantine, progress) " +
		"and the reference-consumer demonstration. The duplicate deliveries above were absorbed " +
		"by the persistent inbox: the simulated ledger records exactly one effective application " +
		"per event.\n\n" +
		"This evidence does NOT guarantee any external real ledger, and the reference consumer " +
		"is not a production ledger: it holds no user balances and is not authoritative.\n\n" +
		events.ReferenceBoundaryStatement + "\n"
	if err := env.Evidence.WriteFile("boundary.md", []byte(boundary)); err != nil {
		t.Fatalf("write boundary statement: %v", err)
	}
	claims := externalGuaranteeClaims(boundary)
	if len(claims) != 0 {
		t.Fatalf("affirmative external-ledger guarantee claims = %d, want 0: %v", len(claims), claims)
	}
	if !strings.Contains(boundary, events.ReferenceBoundaryStatement) {
		t.Fatal("boundary.md does not carry the FR-16 boundary statement verbatim")
	}
	scan := map[string]any{
		"external_guarantee_claims":       len(claims),
		"boundary_statement_verbatim":     true,
		"scope":                           "event identity/version/delivery semantics + consumer idempotency contract + reference consumer",
		"external_real_ledger_guaranteed": false,
	}
	if err := env.Evidence.WriteJSON("boundary_scan.json", scan); err != nil {
		t.Fatalf("write boundary scan: %v", err)
	}
	record("ledger:boundary", scan)
}
