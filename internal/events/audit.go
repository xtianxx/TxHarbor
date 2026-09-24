// audit.go owns the periodic reconciliation audit (T035; data-model §4): it
// compares the latest committed source watermarks of each registered upstream
// domain against the outbox rows that should have been emitted for them. A
// gap means a committed source transition has no corresponding outbox row
// ("missing") or a newer source version has no emitted event ("behind"): the
// audit raises an observation (metric + alert) as a detection fallback.
//
// The audit is read-only: it never writes back, never silently backfills and
// never touches pending|blocked rows. Repair of a detected gap goes through
// the audited events-admin path (PD-4; contracts/outbox-publisher.md §6).
// internal/events never imports an upstream writer package (T016 import
// boundary), so the source-domain probes are registered by the app layer.
package events

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// SourceWatermark is one committed source-side transition: the stable source
// identity plus its version. SourceVersion 0 means the source side records no
// version for this transition; the audit then checks presence only.
type SourceWatermark struct {
	SourceID      string
	SourceVersion int64
}

// SourceProbe supplies the latest committed source watermarks for one
// source_kind. Latest MUST be read-only, bounded by limit and must not lock
// across external calls.
type SourceProbe interface {
	SourceKind() string
	Latest(ctx context.Context, q Querier, limit int) ([]SourceWatermark, error)
}

// Querier is the read-only database surface the audit uses; *pgxpool.Pool
// satisfies it.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// SQLProbe is a read-only probe backed by a caller-supplied SELECT. The query
// returns exactly (source_id TEXT, source_version BIGINT) and takes one
// parameter: the row limit. Build probes with NewSQLProbe so the read-only
// shape is enforced.
type SQLProbe struct {
	Kind  string
	Query string
}

// NewSQLProbe validates and builds a read-only SQL probe. The query MUST start
// with SELECT: an audit probe that writes would violate the detection-only
// rule (T035).
func NewSQLProbe(kind, query string) (SQLProbe, error) {
	if strings.TrimSpace(kind) == "" {
		return SQLProbe{}, contractErrorf("audit probe kind is empty")
	}
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "SELECT") {
		return SQLProbe{}, contractErrorf("audit probe %s query must be a read-only SELECT", kind)
	}
	return SQLProbe{Kind: kind, Query: query}, nil
}

// SourceKind returns the registered source_kind.
func (p SQLProbe) SourceKind() string { return p.Kind }

// Latest runs the probe query bounded by limit.
func (p SQLProbe) Latest(ctx context.Context, q Querier, limit int) ([]SourceWatermark, error) {
	rows, err := q.Query(ctx, p.Query, limit)
	if err != nil {
		return nil, fmt.Errorf("audit probe %s: %w", p.Kind, err)
	}
	defer rows.Close()
	var out []SourceWatermark
	for rows.Next() {
		var mark SourceWatermark
		if err := rows.Scan(&mark.SourceID, &mark.SourceVersion); err != nil {
			return nil, fmt.Errorf("audit probe %s scan: %w", p.Kind, err)
		}
		out = append(out, mark)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("audit probe %s rows: %w", p.Kind, err)
	}
	return out, nil
}

// AuditObserver observes reconciliation gaps (verification.md §1). A nil
// observer disables observation; *metrics.Metrics satisfies it.
type AuditObserver interface {
	ObserveOutboxAuditGap(sourceKind string)
}

// AuditGap is one detected gap. Reason is "missing" (no outbox row exists for
// the source transition) or "behind" (the emitted events carry an older
// source version).
type AuditGap struct {
	SourceKind    string
	SourceID      string
	SourceVersion int64
	Reason        string
}

// AuditReport is one audit pass over all probes.
type AuditReport struct {
	// Checked counts the source watermarks examined.
	Checked int
	// Unverifiable counts watermarks whose outbox rows carry no
	// source_version: the version comparison is impossible, so no gap is
	// fabricated for them.
	Unverifiable int
	// Gaps lists the detected gaps (each also observed through the observer).
	Gaps []AuditGap
	// ProbeErrors lists per-probe read failures; they never stop the other
	// probes and are never reported as gaps.
	ProbeErrors []error
}

// ReconciliationAudit compares registered source probes against the outbox.
type ReconciliationAudit struct {
	probes   []SourceProbe
	limit    int
	observer AuditObserver
}

// NewReconciliationAudit validates the probes and builds the audit. limit
// bounds the recent source window examined per probe; a non-positive limit is
// refused fail-closed.
func NewReconciliationAudit(probes []SourceProbe, limit int, observer AuditObserver) (*ReconciliationAudit, error) {
	if limit <= 0 {
		return nil, contractErrorf("audit limit must be positive")
	}
	for _, probe := range probes {
		if probe == nil {
			return nil, contractErrorf("audit probe is nil")
		}
		if strings.TrimSpace(probe.SourceKind()) == "" {
			return nil, contractErrorf("audit probe kind is empty")
		}
	}
	return &ReconciliationAudit{probes: probes, limit: limit, observer: observer}, nil
}

// auditWatermarkSQL reads the outbox watermark of one source identity:
// (emitted row count, max recorded source_version).
const auditWatermarkSQL = `
SELECT count(*)::bigint, max(source_version)
FROM outbox_events
WHERE source_kind = $1 AND source_id = $2`

// Run executes one audit pass. Every probe failure is recorded in the report
// and does not stop the other probes. The audit issues read-only queries only
// and never changes publish state: pending|blocked rows are untouched.
func (a *ReconciliationAudit) Run(ctx context.Context, q Querier) (AuditReport, error) {
	report := AuditReport{}
	for _, probe := range a.probes {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		marks, err := probe.Latest(ctx, q, a.limit)
		if err != nil {
			report.ProbeErrors = append(report.ProbeErrors, err)
			continue
		}
		for _, mark := range marks {
			report.Checked++
			var emitted int64
			var maxVersion *int64
			if err := q.QueryRow(ctx, auditWatermarkSQL, probe.SourceKind(), mark.SourceID).
				Scan(&emitted, &maxVersion); err != nil {
				report.ProbeErrors = append(report.ProbeErrors,
					fmt.Errorf("audit watermark %s/%s: %w", probe.SourceKind(), mark.SourceID, err))
				continue
			}
			gap, isGap, unverifiable := classifyWatermark(probe.SourceKind(), mark, emitted, maxVersion)
			if unverifiable {
				// The outbox rows exist but carry no source watermark; the
				// comparison is impossible, never a fabricated gap.
				report.Unverifiable++
				continue
			}
			if isGap {
				report.observeGap(a, gap)
			}
		}
	}
	return report, nil
}

// classifyWatermark decides the audit outcome of one source watermark against
// its outbox watermark (emitted row count, max recorded source_version):
//   - no emitted row: gap "missing";
//   - emitted rows without a recorded source_version: unverifiable (the
//     version comparison is impossible; no fabricated gap);
//   - a recorded source version ahead of the emitted one: gap "behind".
func classifyWatermark(kind string, mark SourceWatermark, emitted int64, maxVersion *int64) (AuditGap, bool, bool) {
	if emitted == 0 {
		return AuditGap{
			SourceKind: kind, SourceID: mark.SourceID,
			SourceVersion: mark.SourceVersion, Reason: "missing",
		}, true, false
	}
	if mark.SourceVersion > 0 && maxVersion == nil {
		return AuditGap{}, false, true
	}
	if mark.SourceVersion > 0 && maxVersion != nil && mark.SourceVersion > *maxVersion {
		return AuditGap{
			SourceKind: kind, SourceID: mark.SourceID,
			SourceVersion: mark.SourceVersion, Reason: "behind",
		}, true, false
	}
	return AuditGap{}, false, false
}

// observeGap appends one gap and raises the observation.
func (r *AuditReport) observeGap(a *ReconciliationAudit, gap AuditGap) {
	r.Gaps = append(r.Gaps, gap)
	if a.observer != nil {
		a.observer.ObserveOutboxAuditGap(gap.SourceKind)
	}
}
