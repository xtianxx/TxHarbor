package txlifecycle

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Status is the authoritative read of one attempt (FR-13; send-api.md §2.4):
// identity, state, revision_seq, DB-clock updated_at, tx_hash when persisted,
// the latest dispatch outcome, reconcile class and receipt effect, plus the
// recovery/authorization references. It is data, never a permission.
type Status struct {
	AttemptID             string
	SigningRequestID      string
	IntentID              string
	State                 string
	RevisionSeq           int64
	UpdatedAt             time.Time
	TxHash                string
	LatestDispatchOutcome string
	LatestReconcileClass  string
	LatestReceiptEffect   string
	RecoveryVersion       int64
	AuthorizationID       string
	AuthorizationVersion  int64
}

const statusSQL = `SELECT a.attempt_id, a.signing_request_id, a.intent_id, a.state, a.revision_seq,
	a.updated_at, a.recovery_version, a.authorization_id, a.authorization_version,
	COALESCE(s.tx_hash, ''),
	COALESCE((SELECT sa.outcome FROM tx_send_attempts sa
	          WHERE sa.attempt_id = a.attempt_id ORDER BY sa.send_seq DESC LIMIT 1), ''),
	COALESCE((SELECT r.classification FROM tx_reconciliations r
	          WHERE r.attempt_id = a.attempt_id ORDER BY r.reconcile_id DESC LIMIT 1), ''),
	COALESCE((SELECT rc.effect FROM tx_receipts rc
	          WHERE rc.attempt_id = a.attempt_id ORDER BY rc.receipt_id DESC LIMIT 1), '')
FROM tx_attempts a
LEFT JOIN tx_attempt_signings s ON s.attempt_id = a.attempt_id
WHERE a.attempt_id = $1`

// Status reads one attempt; it never triggers a send, gate bypass or delivery
// (FR-13/Q1). A missing attempt is attempt_not_found.
func (s *Store) Status(ctx context.Context, attemptID string) (*Status, error) {
	if s == nil || s.db == nil {
		return nil, Refuse(ClassCoordinationUnavailable, "", "store has no database")
	}
	st := &Status{}
	err := s.db.QueryRow(ctx, statusSQL, attemptID).Scan(
		&st.AttemptID, &st.SigningRequestID, &st.IntentID, &st.State, &st.RevisionSeq,
		&st.UpdatedAt, &st.RecoveryVersion, &st.AuthorizationID, &st.AuthorizationVersion,
		&st.TxHash, &st.LatestDispatchOutcome, &st.LatestReconcileClass, &st.LatestReceiptEffect)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, Refuse(ClassAttemptNotFound, "attempt_id", "no such attempt")
	}
	if err != nil {
		return nil, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	return st, nil
}
