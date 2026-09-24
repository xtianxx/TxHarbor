//go:build integration

// audit_integration_test.go is the T035 Integration-PG layer: the
// reconciliation audit detects injected gaps (missing emitted rows, source
// versions ahead of emitted versions), reports unverifiable watermarks without
// fabricating gaps, never writes back, never touches pending|blocked publish
// state and survives a failing probe. It runs against the real 000015
// migration through `make test-integration`.
package events

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const auditProbeTable = "t035_source_probe"

const auditProbeSQL = `
SELECT source_id, source_version
FROM t035_source_probe
WHERE source_kind = 't035'
ORDER BY source_version DESC, source_id DESC
LIMIT $1`

// countingAuditObserver records observed gap source kinds.
type countingAuditObserver struct {
	mu    sync.Mutex
	kinds []string
}

func (o *countingAuditObserver) ObserveOutboxAuditGap(sourceKind string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.kinds = append(o.kinds, sourceKind)
}

func (o *countingAuditObserver) observed() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.kinds...)
}

// createAuditProbeTable creates the scratch source-domain probe table.
func createAuditProbeTable(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (source_kind TEXT NOT NULL, source_id TEXT NOT NULL, source_version BIGINT NOT NULL)`,
		auditProbeTable)); err != nil {
		t.Fatalf("create audit probe table: %v", err)
	}
}

// insertAuditProbeRow records one committed source-side transition.
func insertAuditProbeRow(t *testing.T, pool *pgxpool.Pool, sourceID string, sourceVersion int64) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), fmt.Sprintf(
		`INSERT INTO %s (source_kind, source_id, source_version) VALUES ('t035', $1, $2)`,
		auditProbeTable), sourceID, sourceVersion); err != nil {
		t.Fatalf("insert audit probe row: %v", err)
	}
}

// appendAuditEvent emits one committed outbox row for the audit probe domain.
// sourceVersion <= 0 leaves the source watermark unrecorded.
func appendAuditEvent(t *testing.T, pool *pgxpool.Pool, sourceID string, sourceVersion int64, aggregateID string) int64 {
	t.Helper()
	ev, err := NewEvent(Event{
		EventType:     EventTypeDepositObservationStatusChanged,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  IdentityKindBusinessObject,
		AggregateType: "audit_probe_object",
		AggregateID:   aggregateID,
		Payload: map[string]any{
			"from_state": "pending",
			"to_state":   "confirmed",
			"reason":     "audit-probe",
		},
		OccurredAt:    time.Now().UTC(),
		SourceKind:    "t035",
		SourceID:      sourceID,
		SourceVersion: sourceVersion,
	})
	if err != nil {
		t.Fatalf("build audit event: %v", err)
	}
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	res, err := Append(ctx, tx, ev)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return res.OutboxID
}

// snapshotOutboxRows renders the publish-relevant state of every outbox row so
// the audit's read-only property is asserted byte-for-byte.
func snapshotOutboxRows(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT id, publish_state, coalesce(claim_owner, ''), attempt_count,
		       coalesce(last_error_class, ''), coalesce(published_at::text, '')
		FROM outbox_events ORDER BY id`)
	if err != nil {
		t.Fatalf("snapshot outbox rows: %v", err)
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var id, attempt int64
		var state, owner, class, published string
		if err := rows.Scan(&id, &state, &owner, &attempt, &class, &published); err != nil {
			t.Fatalf("scan snapshot: %v", err)
		}
		fmt.Fprintf(&b, "%d|%s|%s|%d|%s|%s\n", id, state, owner, attempt, class, published)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("snapshot rows: %v", err)
	}
	return b.String()
}

// TestReconciliationAuditDetectsGapsWithoutWriteback covers the gap decision
// against real rows: matching, missing, behind and unverifiable watermarks,
// the observation of every gap, the read-only property and the no-writeback
// rule.
func TestReconciliationAuditDetectsGapsWithoutWriteback(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()
	createAuditProbeTable(t, pool)

	// Matching: emitted row with the same source version.
	appendAuditEvent(t, pool, "match-1", 2, "obj-match-1")
	insertAuditProbeRow(t, pool, "match-1", 2)
	// Missing: committed source transition with no emitted event.
	insertAuditProbeRow(t, pool, "missing-1", 1)
	// Behind: emitted event lags the source version.
	appendAuditEvent(t, pool, "behind-1", 1, "obj-behind-1")
	insertAuditProbeRow(t, pool, "behind-1", 5)
	// Unverifiable: emitted row without a recorded source version.
	appendAuditEvent(t, pool, "unverifiable-1", 0, "obj-unverifiable-1")
	insertAuditProbeRow(t, pool, "unverifiable-1", 3)

	observer := &countingAuditObserver{}
	probe, err := NewSQLProbe("t035", auditProbeSQL)
	if err != nil {
		t.Fatalf("NewSQLProbe: %v", err)
	}
	audit, err := NewReconciliationAudit([]SourceProbe{probe}, 100, observer)
	if err != nil {
		t.Fatalf("NewReconciliationAudit: %v", err)
	}

	before := snapshotOutboxRows(t, pool)
	report, err := audit.Run(ctx, pool)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Checked != 4 {
		t.Fatalf("Checked = %d, want 4", report.Checked)
	}
	if report.Unverifiable != 1 {
		t.Fatalf("Unverifiable = %d, want 1", report.Unverifiable)
	}
	if len(report.ProbeErrors) != 0 {
		t.Fatalf("ProbeErrors = %v", report.ProbeErrors)
	}
	if len(report.Gaps) != 2 {
		t.Fatalf("Gaps = %+v, want 2", report.Gaps)
	}
	gaps := map[string]string{}
	for _, gap := range report.Gaps {
		gaps[gap.SourceID] = gap.Reason
	}
	if gaps["missing-1"] != "missing" {
		t.Fatalf("missing-1 gap = %q, want missing", gaps["missing-1"])
	}
	if gaps["behind-1"] != "behind" {
		t.Fatalf("behind-1 gap = %q, want behind", gaps["behind-1"])
	}
	if len(observer.observed()) != 2 {
		t.Fatalf("observed gaps = %v, want 2", observer.observed())
	}

	// Read-only: the publish-relevant state is byte-identical after the audit.
	if after := snapshotOutboxRows(t, pool); after != before {
		t.Fatalf("audit changed outbox state:\nbefore:\n%s\nafter:\n%s", before, after)
	}
	// No writeback: the missing source transition still has no emitted row.
	var emitted int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE source_kind = 't035' AND source_id = 'missing-1'`).Scan(&emitted); err != nil {
		t.Fatalf("count missing rows: %v", err)
	}
	if emitted != 0 {
		t.Fatalf("audit silently backfilled the missing transition (%d rows)", emitted)
	}
}

// TestReconciliationAuditProbeErrorsAndPublishStateUntouched covers a failing
// probe: the error is reported, the other probe still runs, and neither
// pending nor blocked publish state is touched.
func TestReconciliationAuditProbeErrorsAndPublishStateUntouched(t *testing.T) {
	dsn := startMigratedPostgres(t)
	pool := openEventsPool(t, dsn)
	ctx := context.Background()
	createAuditProbeTable(t, pool)

	// One pending and one blocked row: the audit must not touch either.
	pendingID := appendAuditEvent(t, pool, "pending-1", 1, "obj-pending-1")
	blockedID := appendAuditEvent(t, pool, "blocked-1", 1, "obj-blocked-1")
	if _, err := pool.Exec(ctx, `
		UPDATE outbox_events SET publish_state = 'blocked', last_error_class = 'permanent' WHERE id = $1`,
		blockedID); err != nil {
		t.Fatalf("seed blocked row: %v", err)
	}

	broken, err := NewSQLProbe("broken", "SELECT source_id, source_version FROM t035_missing_table LIMIT $1")
	if err != nil {
		t.Fatalf("NewSQLProbe(broken): %v", err)
	}
	valid, err := NewSQLProbe("t035", auditProbeSQL)
	if err != nil {
		t.Fatalf("NewSQLProbe(valid): %v", err)
	}
	insertAuditProbeRow(t, pool, "match-2", 1)
	appendAuditEvent(t, pool, "match-2", 1, "obj-match-2")

	observer := &countingAuditObserver{}
	audit, err := NewReconciliationAudit([]SourceProbe{broken, valid}, 100, observer)
	if err != nil {
		t.Fatalf("NewReconciliationAudit: %v", err)
	}

	before := snapshotOutboxRows(t, pool)
	report, err := audit.Run(ctx, pool)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(report.ProbeErrors) != 1 {
		t.Fatalf("ProbeErrors = %v, want 1", report.ProbeErrors)
	}
	if len(report.Gaps) != 0 {
		t.Fatalf("Gaps = %+v, want 0 (a probe error is not a gap)", report.Gaps)
	}
	if report.Checked != 1 {
		t.Fatalf("Checked = %d, want 1 (the valid probe still ran)", report.Checked)
	}
	if after := snapshotOutboxRows(t, pool); after != before {
		t.Fatalf("audit changed publish state:\nbefore:\n%s\nafter:\n%s", before, after)
	}
	for id, want := range map[int64]string{pendingID: string(PublishStatePending), blockedID: string(PublishStateBlocked)} {
		if got := readOutboxRow(t, pool, id); got.publishState != want {
			t.Fatalf("row %d state = %s, want %s", id, got.publishState, want)
		}
	}
}
