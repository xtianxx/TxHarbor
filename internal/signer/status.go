// status.go owns the desensitized status path (T029; contracts/api.md §3,
// FR-17): an ownership-scoped read of one own signing request plus its latest
// delivery admission. It is a pure read of recorded facts — status never
// triggers signing, a gate bypass, or a delivery. The view never carries
// signature bytes, and tx_hash surfaces only when the latest admission was
// already cleared for delivery (admitted/delivered); after revocation, expiry
// or pause the response is status-only, the explicit inversion of the 007
// still-200 replay (OC-6).
package signer

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// State is the closed signing-request state vocabulary (data-model.md Table 3
// CHECK): the only values a signing_requests.state may hold.
type State string

const (
	StateReceived  State = "received"
	StateValidated State = "validated"
	StateSigned    State = "signed"
	StateRejected  State = "rejected"
	StateFailed    State = "failed"
)

// Verdict is the closed delivery-admission verdict vocabulary (data-model.md
// Table 6 CHECK).
type Verdict string

const (
	VerdictAdmitted         Verdict = "admitted"
	VerdictDelivered        Verdict = "delivered"
	VerdictBlocked          Verdict = "blocked"
	VerdictUnknownReconcile Verdict = "unknown_reconcile"
)

// AllowsTxHash reports whether an admission with this verdict has cleared
// tx_hash for the status view: only admitted/delivered were cleared for
// delivery (api.md §3); blocked/unknown_reconcile are status-only.
func (v Verdict) AllowsTxHash() bool {
	return v == VerdictAdmitted || v == VerdictDelivered
}

// ErrStatusNotFound is the single identical outcome of the status path for a
// nonexistent signing_request_id and for another caller's id. The caller maps
// it to 404 not_found; neither existence nor ownership leaks (api.md §3).
var ErrStatusNotFound = errors.New("not_found")

// Delivery is the latest recorded delivery admission (Table 6) for a request:
// the last admission verdict and timestamp, plus the observed basis when the
// admission was blocked.
type Delivery struct {
	Verdict            Verdict
	AttemptSeq         int
	Reason             string
	PauseBasis         string
	RecoveryBasis      string
	AuthorizationState string
	BindingClass       string
	CanSign            bool
	DecidedAt          time.Time
	DeliveredAt        *time.Time
}

// Status is the desensitized status view of one own request (api.md §3). It
// carries recorded facts only: never signature bytes, never raw
// signed-transaction bytes. TxHash is non-empty only when the latest admission
// was admitted/delivered. RefusalClass is the last recorded refusal class — the
// request row's class when rejected, else the latest admission's reason when
// blocked — and is empty when nothing was refused.
type Status struct {
	SigningRequestID string
	State            State
	ContentHash      string
	AttemptID        string
	IntentID         string
	PolicyVersion    string
	RefusalClass     RefusalClass
	CreatedAt        time.Time
	UpdatedAt        time.Time
	Delivery         *Delivery
	TxHash           string
}

// statusSelectSQL is the single T-status read (data-model.md): the request row
// scoped by BOTH the verified caller and the public id — so a foreign id is
// indistinguishable from a nonexistent one — its latest admission row, and the
// persisted result's tx_hash (never its signature). One statement, so the facts
// and the admission come from one snapshot.
const statusSelectSQL = `SELECT
  r.signing_request_id, r.state, r.content_hash, r.attempt_id, r.intent_id,
  r.policy_version, r.refusal_class, r.created_at, r.updated_at,
  a.attempt_seq, a.verdict, a.reason, a.pause_basis, a.recovery_basis,
  a.authorization_state, a.binding_class, a.can_sign, a.decided_at, a.delivered_at,
  s.tx_hash
FROM signing_requests r
LEFT JOIN LATERAL (
  SELECT attempt_seq, verdict, reason, pause_basis, recovery_basis,
         authorization_state, binding_class, can_sign, decided_at, delivered_at
  FROM delivery_admissions
  WHERE signing_request_row = r.id
  ORDER BY attempt_seq DESC
  LIMIT 1
) a ON TRUE
LEFT JOIN signature_results s ON s.signing_request_row = r.id
WHERE r.caller_id = $1 AND r.signing_request_id = $2`

// LookupStatus reads one own request: the request row by
// (caller_id, signing_request_id) plus its latest admission row. A missing id
// and another caller's id both return the identical ErrStatusNotFound. A
// genuine storage failure returns ClassStorageUnavailable, never a false
// not_found. callerID MUST already be authenticated by the transport layer.
//
// The returned view is desensitized: signature bytes are never read, and
// tx_hash is surfaced only when the latest admission was admitted/delivered.
func LookupStatus(ctx context.Context, db DB, callerID int64, signingRequestID string) (*Status, error) {
	var (
		st                                                             Status
		requestID, state, contentHash, attemptID, intentID, policyVer  string
		refusal                                                        string
		attemptSeq                                                     *int
		verdict, reason, pauseBasis, recoveryBasis, authState, binding *string
		canSign                                                        *bool
		decidedAt, deliveredAt                                         *time.Time
		txHash                                                         *string
	)
	err := db.QueryRow(ctx, statusSelectSQL, callerID, signingRequestID).Scan(
		&requestID, &state, &contentHash, &attemptID, &intentID,
		&policyVer, &refusal, &st.CreatedAt, &st.UpdatedAt,
		&attemptSeq, &verdict, &reason, &pauseBasis, &recoveryBasis,
		&authState, &binding, &canSign, &decidedAt, &deliveredAt,
		&txHash,
	)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, ErrStatusNotFound
	case err != nil:
		return nil, refuse(ClassStorageUnavailable, "", "status store unavailable")
	}

	st.SigningRequestID = requestID
	st.State = State(state)
	st.ContentHash = contentHash
	st.AttemptID = attemptID
	st.IntentID = intentID
	st.PolicyVersion = policyVer
	st.RefusalClass = RefusalClass(refusal)

	// The LATERAL columns are all NOT NULL when the admission row exists; the
	// pointer is nil only when no admission was ever recorded.
	if verdict != nil {
		d := &Delivery{
			Verdict:            Verdict(*verdict),
			AttemptSeq:         *attemptSeq,
			Reason:             *reason,
			PauseBasis:         *pauseBasis,
			RecoveryBasis:      *recoveryBasis,
			AuthorizationState: *authState,
			BindingClass:       *binding,
			CanSign:            *canSign,
			DecidedAt:          *decidedAt,
		}
		if deliveredAt != nil {
			d.DeliveredAt = deliveredAt
		}
		st.Delivery = d
		// A blocked admission's reason is the authoritative last refusal for the
		// status-only response.
		if d.Reason != "" {
			st.RefusalClass = RefusalClass(d.Reason)
		}
		if d.Verdict.AllowsTxHash() && txHash != nil {
			st.TxHash = *txHash
		}
	}
	return &st, nil
}
