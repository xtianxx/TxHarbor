package txlifecycle

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ReleaseRequest is the adjudicated controlled manual release: it lifts only
// the intent's independent freeze cause and records the audit basis (T040).
type ReleaseRequest struct {
	IntentID   string
	Permission string
	Operator   string
	Reason     string
	Basis      string
}

// Release lifts exactly one residual's freeze cause under a controlled
// permission, with evidence and audit. It never overrides revoke/expiry/pause/
// eligibility gates; any subsequent resend re-verifies every gate (T040).
func (s *Store) Release(ctx context.Context, req *ReleaseRequest) (bool, error) {
	if s == nil || s.db == nil {
		return false, Refuse(ClassCoordinationUnavailable, "", "store has no database")
	}
	if req == nil || req.IntentID == "" || req.Permission == "" || s.releaseToken == "" || req.Permission != s.releaseToken {
		return false, Refuse(ClassReleaseNotPermitted, "permission", "controlled release permission required")
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return false, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}

	var cause string
	err = tx.QueryRow(ctx,
		`SELECT cause FROM tx_intent_freezes WHERE intent_id = $1 AND released_at IS NULL FOR UPDATE`, req.IntentID).Scan(&cause)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, Refuse(ClassReleaseNotPermitted, "intent_id", "no active freeze for this intent")
	}
	if err != nil {
		return false, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	if _, err := tx.Exec(ctx,
		`UPDATE tx_intent_freezes SET released_at = now(), released_by = $2, release_basis = $3
		  WHERE intent_id = $1 AND released_at IS NULL`,
		req.IntentID, req.Operator, req.Basis); err != nil {
		return false, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	detail := "cause=" + cause + " operator=" + req.Operator + " reason=" + req.Reason + " basis=" + req.Basis
	if err := s.auditRelease(ctx, tx, req.IntentID, detail); err != nil {
		return false, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	if err := tx.Commit(ctx); err != nil {
		return false, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	return true, nil
}

// auditRelease appends the release audit to tx_attempt_events. A freeze may
// exist before any attempt row, so it falls back to a synthetic audit id
// (Table 6 carries no FK on attempt_id).
func (s *Store) auditRelease(ctx context.Context, tx pgx.Tx, intentID, detail string) error {
	rows, err := tx.Query(ctx, `SELECT attempt_id FROM tx_attempts WHERE intent_id = $1`, intentID)
	if err != nil {
		return err
	}
	var attempts []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		attempts = append(attempts, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(attempts) == 0 {
		return appendEventTx(ctx, tx, "intent:"+intentID, EventReleased, "", nil, detail)
	}
	for _, id := range attempts {
		revision, _, err := lockAttemptRow(ctx, tx, id)
		if err != nil {
			return err
		}
		_ = revision
		if err := appendEventTx(ctx, tx, id, EventReleased, "", nil, detail); err != nil {
			return err
		}
	}
	return nil
}
