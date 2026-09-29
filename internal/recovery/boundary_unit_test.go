// boundary_unit_test.go is the T052 [US5] unit layer: the pure FR-027–FR-029
// derivation (offset relation, duplicate absorption facts, missing idempotency
// history, possible external duplicate effects) and the read-only / no-effect
// boundary of internal/recovery/boundary.go. No Docker, no database.
package recovery

import (
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestBoundaryOffsetRelationClosedSet(t *testing.T) {
	relations := KnownEventOffsetRelations()
	if len(relations) != 4 {
		t.Fatalf("closed offset relation set has %d members, want 4", len(relations))
	}
	for _, want := range []EventOffsetRelation{
		EventOffsetEqual, EventOffsetBrokerAhead, EventOffsetPGAhead, EventOffsetUnprovable,
	} {
		if !want.Known() {
			t.Fatalf("%q is not a known offset relation", want)
		}
	}
	if EventOffsetRelation("unknown").Known() {
		t.Fatal("an unknown relation must not be a member of the closed set")
	}
}

func TestBoundaryDedupIdentity(t *testing.T) {
	identity := DownstreamDedupIdentity()
	if identity.EventIDColumn != "event_id" {
		t.Fatalf("event identity column = %q, want event_id", identity.EventIDColumn)
	}
	if identity.SourceTopicColumn != "topic" || identity.SourcePartitionColumn != "partition" ||
		identity.SourceOffsetColumn != "offset" {
		t.Fatalf("source triple = (%q, %q, %q), want (topic, partition, offset)",
			identity.SourceTopicColumn, identity.SourcePartitionColumn, identity.SourceOffsetColumn)
	}
	for _, want := range []string{"event_id", "topic", "partition", "offset"} {
		if !strings.Contains(identity.Statement, want) {
			t.Fatalf("dedup identity statement does not name %q: %s", want, identity.Statement)
		}
	}
}

// TestBoundaryAssessFailClosed pins every branch of the pure decision: only
// equality plus a readable, non-empty idempotency history (no open quarantine)
// is consistent; a broker position ahead of the restored progress is divergent
// and never a pass; missing history is unknown and never re-executable.
func TestBoundaryAssessFailClosed(t *testing.T) {
	fullHistory := EventBoundaryFacts{
		ConsumerName: "txharbor.ref-consumer.v1",
		Topic:        "txharbor.events.v1",
		Partition:    0,
		NextOffset:   1,
		InboxRows:    1, InboxKnown: true,
		VersionRows: 1, VersionKnown: true,
		QuarantineKnown: true,
	}

	t.Run("equal_with_history_is_consistent", func(t *testing.T) {
		check := AssessEventBoundaryCheck(fullHistory, EventBoundaryBrokerFact{Readable: true, CommittedOffset: 1})
		if check.Conclusion != ConclusionConsistent || !check.Conclusion.Passes() {
			t.Fatalf("check = %+v, want consistent", check)
		}
		if check.OffsetRelation != EventOffsetEqual {
			t.Fatalf("relation = %q, want equal", check.OffsetRelation)
		}
		if check.MissingIdempotencyHistory || check.PossibleExternalDuplicateEffects {
			t.Fatalf("check = %+v, want no missing-history flag", check)
		}
	})

	t.Run("broker_ahead_is_divergent_rollback", func(t *testing.T) {
		check := AssessEventBoundaryCheck(fullHistory, EventBoundaryBrokerFact{Readable: true, CommittedOffset: 3})
		if check.Conclusion != ConclusionDivergent || check.Conclusion.Passes() {
			t.Fatalf("check = %+v, want divergent", check)
		}
		if !check.RollbackDetected || check.OffsetRelation != EventOffsetBrokerAhead {
			t.Fatalf("check = %+v, want a detected broker_ahead rollback", check)
		}
		if !check.PossibleExternalDuplicateEffects {
			t.Fatal("a broker-ahead rollback must report possible external duplicate effects")
		}
	})

	t.Run("pg_ahead_is_unknown", func(t *testing.T) {
		check := AssessEventBoundaryCheck(fullHistory, EventBoundaryBrokerFact{Readable: true, CommittedOffset: 0})
		if check.Conclusion != ConclusionUnknown || check.OffsetRelation != EventOffsetPGAhead {
			t.Fatalf("check = %+v, want unknown pg_ahead", check)
		}
	})

	t.Run("unreadable_broker_is_unknown", func(t *testing.T) {
		check := AssessEventBoundaryCheck(fullHistory, EventBoundaryBrokerFact{
			Readable: false, ReadErr: errors.New("dial tcp timeout"),
		})
		if check.Conclusion != ConclusionUnknown || check.OffsetRelation != EventOffsetUnprovable {
			t.Fatalf("check = %+v, want unknown unprovable", check)
		}
		if check.BrokerReadable {
			t.Fatal("an unreadable broker must stay unreadable")
		}
	})

	t.Run("missing_history_is_unknown_and_never_re_executable", func(t *testing.T) {
		facts := fullHistory
		facts.InboxRows, facts.VersionRows = 0, 0
		check := AssessEventBoundaryCheck(facts, EventBoundaryBrokerFact{Readable: true, CommittedOffset: 1})
		if check.Conclusion != ConclusionUnknown || check.Conclusion.Passes() {
			t.Fatalf("check = %+v, want unknown", check)
		}
		if !check.MissingIdempotencyHistory {
			t.Fatal("a positive progress with no inbox/version history must be a missing-history fact")
		}
		if !check.PossibleExternalDuplicateEffects {
			t.Fatal("missing history must report possible external duplicate effects")
		}
		if !strings.Contains(check.Reason, "never") {
			t.Fatalf("reason %q must state that the record is never backfilled/re-executed", check.Reason)
		}
	})

	t.Run("missing_relation_is_unknown", func(t *testing.T) {
		facts := fullHistory
		facts.InboxKnown, facts.VersionKnown = false, false
		check := AssessEventBoundaryCheck(facts, EventBoundaryBrokerFact{Readable: true, CommittedOffset: 1})
		if check.Conclusion != ConclusionUnknown || !check.MissingIdempotencyHistory {
			t.Fatalf("check = %+v, want unknown with missing history", check)
		}
	})

	t.Run("open_quarantine_is_unknown", func(t *testing.T) {
		facts := fullHistory
		facts.OpenQuarantineRows = 1
		check := AssessEventBoundaryCheck(facts, EventBoundaryBrokerFact{Readable: true, CommittedOffset: 1})
		if check.Conclusion != ConclusionUnknown {
			t.Fatalf("check = %+v, want unknown while an effect is unresolved", check)
		}
	})

	t.Run("missing_quarantine_relation_is_unknown", func(t *testing.T) {
		facts := fullHistory
		facts.QuarantineKnown = false
		check := AssessEventBoundaryCheck(facts, EventBoundaryBrokerFact{Readable: true, CommittedOffset: 1})
		if check.Conclusion != ConclusionUnknown {
			t.Fatalf("check = %+v, want unknown when the quarantine relation is not migrated", check)
		}
	})

	t.Run("incomplete_identity_is_unknown", func(t *testing.T) {
		check := AssessEventBoundaryCheck(EventBoundaryFacts{}, EventBoundaryBrokerFact{Readable: true, CommittedOffset: 0})
		if check.Conclusion != ConclusionUnknown || check.OffsetRelation != EventOffsetUnprovable {
			t.Fatalf("check = %+v, want unknown unprovable", check)
		}
	})
}

func TestBoundaryReportAggregation(t *testing.T) {
	checkedAt := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)

	empty := BuildEventBoundaryReport(checkedAt, nil, false)
	if empty.Conclusion != ConclusionUnknown || empty.Passes() {
		t.Fatalf("an empty report = %+v, want unknown (nothing proven)", empty)
	}

	consistent := AssessEventBoundaryCheck(EventBoundaryFacts{
		ConsumerName: "c", Topic: "t", Partition: 0, NextOffset: 2,
		InboxRows: 2, InboxKnown: true, VersionRows: 2, VersionKnown: true, QuarantineKnown: true,
	}, EventBoundaryBrokerFact{Readable: true, CommittedOffset: 2})
	divergent := AssessEventBoundaryCheck(EventBoundaryFacts{
		ConsumerName: "c2", Topic: "t", Partition: 0, NextOffset: 1,
		InboxRows: 1, InboxKnown: true, VersionRows: 1, VersionKnown: true, QuarantineKnown: true,
	}, EventBoundaryBrokerFact{Readable: true, CommittedOffset: 4})

	report := BuildEventBoundaryReport(checkedAt, []EventBoundaryCheck{consistent, divergent}, false)
	if report.Conclusion != ConclusionDivergent || report.Passes() {
		t.Fatalf("report = %+v, want divergent", report)
	}
	if !report.RollbackDetected {
		t.Fatal("the divergent broker_ahead check must surface as a rollback")
	}
	if len(report.PossibleExternalDuplicateEffects) == 0 {
		t.Fatal("the divergent check must surface a possible external duplicate effect")
	}
	if report.CrossSystemExactlyOnce {
		t.Fatal("a report must never claim cross-system exactly-once")
	}
	if report.VerifiableScope != EventBoundaryScopeWithoutReceipts {
		t.Fatalf("scope = %q, want the no-receipts scope", report.VerifiableScope)
	}
	if withReceipts := BuildEventBoundaryReport(checkedAt, []EventBoundaryCheck{consistent}, true); withReceipts.VerifiableScope != EventBoundaryScopeWithReceipts {
		t.Fatalf("scope = %q, want the receipts scope", withReceipts.VerifiableScope)
	}
	if report.CheckedAt != checkedAt {
		t.Fatalf("checkedAt = %s, want %s", report.CheckedAt, checkedAt)
	}
	if report.DedupIdentity.EventIDColumn != "event_id" {
		t.Fatal("the report must carry the FR-028 dedup identity")
	}
}

// TestBoundaryExactlyOnceNeverClaimed pins the FR-028/029 statements.
func TestBoundaryExactlyOnceNeverClaimed(t *testing.T) {
	for name, statement := range map[string]string{
		"exactly_once": EventBoundaryExactlyOnceStatement,
		"no_receipts":  EventBoundaryScopeWithoutReceipts,
		"receipts":     EventBoundaryScopeWithReceipts,
	} {
		if statement == "" {
			t.Fatalf("%s statement is empty", name)
		}
	}
	if !strings.Contains(EventBoundaryExactlyOnceStatement, "at-least-once") {
		t.Fatalf("exactly-once statement must name the delivery semantics: %s", EventBoundaryExactlyOnceStatement)
	}
	if !strings.Contains(EventBoundaryScopeWithoutReceipts, "No external ledger consistency") {
		t.Fatalf("the no-receipts scope must refuse an external ledger conclusion: %s", EventBoundaryScopeWithoutReceipts)
	}
	if !strings.Contains(EventBoundaryBoundaryStatement, "read-only") {
		t.Fatalf("boundary statement must state the read-only discipline: %s", EventBoundaryBoundaryStatement)
	}
}

// boundaryForbiddenPatterns are the write / effect / reprocess shapes that must
// never appear in boundary.go: the report detects, it never applies, backfills,
// replays or re-delivers. The scan is code-shaped so ordinary prose ("never
// replays") does not trip it.
var boundaryForbiddenPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b(insert\s+into|update\s+\w+\s+set|delete\s+from|truncate\s+table)\b`),
	regexp.MustCompile(`\.Exec\(`),
	regexp.MustCompile(`\b(pgx\.Tx|Process|Replay|Unblock|RetentionPrune|Append|ProduceSync|SendMessage)\s*\(`),
	regexp.MustCompile(`\bBegin\(`),
}

// TestBoundarySourceIsReadOnly scans the production file: every database
// statement is a SELECT and no effect/replay entry point is reachable.
func TestBoundarySourceIsReadOnly(t *testing.T) {
	src, err := os.ReadFile("boundary.go")
	if err != nil {
		t.Fatalf("read boundary.go: %v", err)
	}
	body := string(src)
	for _, pattern := range boundaryForbiddenPatterns {
		if match := pattern.FindString(body); match != "" {
			t.Fatalf("boundary.go carries a write/effect/reprocess shape %q", match)
		}
	}
	// The only SQL keywords used are SELECT statements (count them so an
	// accidental second statement kind is caught).
	for _, token := range []string{"INSERT", "UPDATE ", "DELETE", "TRUNCATE", "UPSERT"} {
		if strings.Contains(body, token) {
			t.Fatalf("boundary.go carries the SQL keyword %q", token)
		}
	}
}
