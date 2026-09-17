package execution

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// T-projection (data-model Table 5): the display-only request status. This file
// is the single writer of request_status_projection. Two version-monotonic
// input streams feed it — state_version from 011's payment_intents and
// lifecycle_version from 010's authority revision version — and each applies
// only when the incoming version is greater; an older arrival affects zero rows
// and never overwrites a newer reference (Q1).
//
// The table is NEVER read by a decision path (claim, step issue, reconcile,
// revision): those read payment_intents/execution_claims/execution_steps and
// 010's authority facts, never this display row. A failed authority read marks
// the row possibly_stale without rewriting any stored version or converting a
// known result; a successful read at version >= stored clears it to confirmed.
// readiness/display reads go through ReadProjection only.

// Display freshness values (data-model Table 5). stale_since is non-null iff
// freshness is possibly_stale (the migration CHECK enforces the biconditional).
const (
	FreshnessConfirmed     = "confirmed"
	FreshnessPossiblyStale = "possibly_stale"
)

// ProjectionRow is one request_status_projection row (display model).
type ProjectionRow struct {
	RequestID           string
	IntentID            string
	ExecutionState      string
	StateVersion        int64
	LifecycleAttemptID  *string
	LifecycleVersion    int64
	LifecycleObservedAt *time.Time
	Freshness           string
	StaleSince          *time.Time
}

const readProjectionSQL = `SELECT request_id, intent_id, execution_state, state_version,
  lifecycle_attempt_id, lifecycle_version, lifecycle_observed_at, freshness, stale_since
  FROM request_status_projection WHERE request_id = $1`

// ReadProjection reads the display row. Only display paths (the HTTP execution
// view, the operator projection-refresh) may call it; decision paths must not.
func ReadProjection(ctx context.Context, q Queryer, requestID string) (ProjectionRow, bool, error) {
	var p ProjectionRow
	err := q.QueryRow(ctx, readProjectionSQL, requestID).Scan(
		&p.RequestID, &p.IntentID, &p.ExecutionState, &p.StateVersion,
		&p.LifecycleAttemptID, &p.LifecycleVersion, &p.LifecycleObservedAt, &p.Freshness, &p.StaleSince)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, false, nil
	}
	if err != nil {
		return p, false, fmt.Errorf("read request status projection: %w", err)
	}
	return p, true, nil
}

// applyStateProjection applies the 011-side state stream only when the incoming
// version is greater; an older arrival affects zero rows and never overwrites a
// newer one (data-model Table 5).
func applyStateProjection(ctx context.Context, q Queryer, requestID, state string, version int64) (bool, error) {
	tag, err := q.Exec(ctx, `UPDATE request_status_projection
		SET execution_state = $2, state_version = $3, updated_at = now()
		WHERE request_id = $1 AND state_version < $3`, requestID, state, version)
	if err != nil {
		return false, fmt.Errorf("apply state projection: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// applyLifecycleProjection applies the 010-side lifecycle stream. A strictly
// greater incoming version moves the referenced attempt identity and the stored
// version; an equal version only refreshes the observation time and clears
// staleness (a successful authority read at version >= stored). An older
// arrival affects zero rows and never rewrites a newer reference (Q1). The row
// references attempt identity only — it never copies 010's transaction facts.
func applyLifecycleProjection(ctx context.Context, q Queryer, requestID, attemptID string, version int64) (bool, error) {
	tag, err := q.Exec(ctx, `UPDATE request_status_projection
		SET lifecycle_attempt_id = CASE WHEN lifecycle_version < $3 THEN NULLIF($2, '') ELSE lifecycle_attempt_id END,
		    lifecycle_version = CASE WHEN lifecycle_version < $3 THEN $3 ELSE lifecycle_version END,
		    lifecycle_observed_at = now(), freshness = 'confirmed', stale_since = NULL, updated_at = now()
		WHERE request_id = $1 AND lifecycle_version <= $3`, requestID, attemptID, version)
	if err != nil {
		return false, fmt.Errorf("apply lifecycle projection: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// markProjectionStale records a possibly-stale freshness without rewriting any
// stored version or converting a known result. The coalesce keeps the first
// stale_since: repeated failures do not move the watermark.
func markProjectionStale(ctx context.Context, q Queryer, requestID string) error {
	if _, err := q.Exec(ctx, `UPDATE request_status_projection
		SET freshness = 'possibly_stale', stale_since = coalesce(stale_since, now()), updated_at = now()
		WHERE request_id = $1 AND freshness = 'confirmed'`, requestID); err != nil {
		return fmt.Errorf("mark projection stale: %w", err)
	}
	return nil
}

// ProjectionCatchUp011 applies the 011 state stream to every projection row in
// version order (the display plane; never read by decisions).
func (r *Reconciler) ProjectionCatchUp011(ctx context.Context) error {
	rows, err := r.Pool.Query(ctx, `SELECT request_id, state, state_version FROM payment_intents ORDER BY state_version ASC`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type row struct {
		requestID string
		state     string
		version   int64
	}
	var all []row
	for rows.Next() {
		var rr row
		if err := rows.Scan(&rr.requestID, &rr.state, &rr.version); err != nil {
			return err
		}
		all = append(all, rr)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, rr := range all {
		if _, err := applyStateProjection(ctx, r.Pool, rr.requestID, rr.state, rr.version); err != nil {
			return err
		}
	}
	return nil
}

// ApplyStateProjection exposes the version-guarded 011 state apply to the
// worker cycle.
func (r *Reconciler) ApplyStateProjection(ctx context.Context, requestID, state string, version int64) error {
	_, err := applyStateProjection(ctx, r.Pool, requestID, state, version)
	return err
}

// Forced projection-refresh outcomes (contracts/api.md §3). refused is a
// recorded outcome, not an error: the row is still marked possibly_stale, and
// no business state is written.
const (
	ProjectionRefreshApplied = "applied"
	ProjectionRefreshRefused = "refused"
)

// ProjectionRefresh is one forced-refresh result.
type ProjectionRefresh struct {
	Found           bool
	IntentID        string
	Outcome         string
	Basis           string
	AttemptID       string
	RevisionVersion int64
}

const intentForRequestSQL = `SELECT intent_id FROM payment_intents WHERE request_id = $1`

// RefreshProjection reads 010 authority (LifecycleReader) and applies the
// version-guarded projection update, appending a projection_refreshed event. An
// unavailable authority marks the row possibly_stale (never rewriting a stored
// version or converting a known result) and records refused, with zero
// business-state writes. No operation of this mechanism promises or implies a
// numeric display SLA (C11); it only ensures staleness is always visible and
// always recoverable. The real 010 reader is joint wiring; a nil reader is the
// honest independent-run "authority unavailable" case.
func RefreshProjection(ctx context.Context, tx pgx.Tx, reader LifecycleReader, requestID string) (ProjectionRefresh, error) {
	var intentID string
	err := tx.QueryRow(ctx, intentForRequestSQL, requestID).Scan(&intentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProjectionRefresh{Found: false, Outcome: ProjectionRefreshRefused, Basis: "no intent for request"}, nil
	}
	if err != nil {
		return ProjectionRefresh{}, fmt.Errorf("resolve intent for projection refresh: %w", err)
	}

	out := ProjectionRefresh{Found: true, IntentID: intentID}
	if reader == nil {
		return refuseProjectionRefresh(ctx, tx, requestID, intentID, out)
	}
	facts, readErr := reader.Read(ctx, intentID)
	if readErr != nil {
		return refuseProjectionRefresh(ctx, tx, requestID, intentID, out)
	}

	if _, err := applyLifecycleProjection(ctx, tx, requestID, facts.CurrentAttemptID, facts.RevisionVersion); err != nil {
		return ProjectionRefresh{}, err
	}
	if err := AppendEvent(ctx, tx, Event{
		IntentID: intentID, Kind: EventProjectionRefreshed,
		AttemptID: facts.CurrentAttemptID, RevisionVersion: facts.RevisionVersion,
		Detail: "forced refresh",
	}); err != nil {
		return ProjectionRefresh{}, err
	}
	out.Outcome = ProjectionRefreshApplied
	out.AttemptID = facts.CurrentAttemptID
	out.RevisionVersion = facts.RevisionVersion
	out.Basis = "authority read"
	return out, nil
}

// refuseProjectionRefresh marks the display row possibly_stale and records the
// refusal as evidence; it writes no business state and moves no version.
func refuseProjectionRefresh(ctx context.Context, tx pgx.Tx, requestID, intentID string, out ProjectionRefresh) (ProjectionRefresh, error) {
	if err := markProjectionStale(ctx, tx, requestID); err != nil {
		return ProjectionRefresh{}, err
	}
	if err := AppendEvent(ctx, tx, Event{
		IntentID: intentID, Kind: EventProjectionStale,
		Detail: "forced refresh authority unavailable",
	}); err != nil {
		return ProjectionRefresh{}, err
	}
	out.Outcome = ProjectionRefreshRefused
	out.Basis = string(ClassLifecycleUnavailable)
	return out, nil
}
