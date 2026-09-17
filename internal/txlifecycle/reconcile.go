package txlifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/xtianxx/txharbor/internal/eth"
)

// ReconcileResult is one observe-only chain probe outcome (send-api.md §2.3).
type ReconcileResult struct {
	Classification string
	TxHash         string
	AttemptState   string
	ReceiptEffect  string
	Confirmations  int64
}

// ReconcileFact is one append-only tx_reconciliations row.
type ReconcileFact struct {
	Classification string
	BlockNumber    *int64
	BlockHash      string
	RPCClass       string
	ObservedAt     time.Time
}

// SendFact is one immutable tx_send_attempts row.
type SendFact struct {
	SendSeq      int
	Kind         string
	Outcome      string
	RPCClass     string
	DispatchedAt *time.Time
}

// UnknownRecovery is the J5 unknown-recovery interface: facts + recovery
// conditions, never a permission and never a rebuild (T025).
type UnknownRecovery struct {
	AttemptID          string
	IntentID           string
	TxHash             string
	State              string
	RevisionSeq        int64
	SignedBytesPresent bool
	Dispatches         []SendFact
	Observations       []ReconcileFact
	LastGateBasis      GateSnapshot
	LastReceiptEffect  string
	RecoveryConditions []string
	Frozen             bool
}

// Reconcile is T4: probe the chain by tx_hash, append one observation, and
// apply the data-model transitions. Claim-free, observe-only, idempotent;
// it never sends and never writes an upstream table (FR-03; R-010-06).
func (s *Store) Reconcile(ctx context.Context, attemptID, txHash string) (ReconcileResult, error) {
	if s == nil || s.db == nil {
		return ReconcileResult{}, Refuse(ClassCoordinationUnavailable, "", "store has no database")
	}
	if s.rpc == nil {
		return ReconcileResult{}, Refuse(ClassCoordinationUnavailable, "rpc", "no chain client configured")
	}
	a, err := s.AttemptByID(ctx, attemptID)
	if err != nil {
		return ReconcileResult{}, err
	}
	hash := strings.ToLower(txHash)
	if hash == "" {
		persisted, _, err := s.signingRow(ctx, attemptID)
		if err != nil {
			return ReconcileResult{}, Refuse(ClassAttemptNotSendable, "tx_hash", "attempt has no persisted tx hash")
		}
		hash = persisted
	}

	classification, blockNumber, blockHash, rpcClass := s.probe(ctx, hash)

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return ReconcileResult{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return ReconcileResult{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO tx_reconciliations (attempt_id, tx_hash, classification, block_number, block_hash, rpc_class)
		 VALUES ($1,$2,$3,$4,$5,$6)`,
		attemptID, hash, classification, blockNumber, nullIfEmpty(blockHash), rpcClass); err != nil {
		return ReconcileResult{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}

	revision, state, err := lockAttemptRow(ctx, tx, attemptID)
	if err != nil {
		return ReconcileResult{}, err
	}
	toState, fromStates := reconcileTransition(classification, state)
	if toState != "" {
		if _, err := applyStateTx(ctx, tx, attemptID, revision, fromStates, toState, ""); err != nil {
			if errors.Is(err, ErrRevisionMoved) {
				return ReconcileResult{}, Refuse(ClassSendStale, "", "attempt revision moved during reconcile")
			}
			return ReconcileResult{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
		}
	}
	if err := appendEventTx(ctx, tx, attemptID, EventReconcileObserved, "", recoveryVersionPtr(a.RecoveryVersion), "classification="+classification); err != nil {
		return ReconcileResult{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}

	result := ReconcileResult{Classification: classification, TxHash: hash}
	if classification == "included" {
		effect, confirmations, err := s.applyReceipt(ctx, tx, a, hash, blockNumber, blockHash)
		if err != nil {
			return ReconcileResult{}, err
		}
		result.ReceiptEffect = effect
		result.Confirmations = confirmations
	}

	// T039: compare recorded versions against change evidence at reconcile.
	if _, err := s.detectFreeze(ctx, tx, a); err != nil {
		return ReconcileResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ReconcileResult{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	if refreshed, err := s.AttemptByID(ctx, attemptID); err == nil {
		result.AttemptState = refreshed.State
	} else {
		result.AttemptState = state
	}
	return result, nil
}

// probe classifies one eth_getTransactionByHash observation.
func (s *Store) probe(ctx context.Context, hash string) (classification string, blockNumber *int64, blockHash, rpcClass string) {
	_, isPending, err := s.rpc.TransactionByHash(ctx, common.HexToHash(hash))
	if err != nil {
		if eth.KindOf(err) == eth.KindNotFound {
			return "not_found_yet", nil, "", ""
		}
		return "unavailable", nil, "", string(eth.KindOf(err))
	}
	if isPending {
		return "found_pending", nil, "", ""
	}
	receipt, err := s.rpc.TransactionReceipt(ctx, common.HexToHash(hash))
	if err != nil || receipt == nil {
		return "found_pending", nil, "", ""
	}
	number := receipt.BlockNumber.Int64()
	return "included", &number, receipt.BlockHash.Hex(), ""
}

// reconcileTransition maps one classification onto the attempt state machine.
// not_found_yet is never a failure verdict (G-010-5).
func reconcileTransition(classification, state string) (toState string, fromStates []string) {
	switch classification {
	case "found_pending":
		if state == "signed" || state == "unknown" {
			return "sent", []string{state}
		}
	case "not_found_yet":
		if state == "sent" {
			return "unknown", []string{"sent"}
		}
	}
	return "", nil
}

// UnknownRecovery returns the persisted facts for an unknown attempt plus the
// conditions under which recovery may proceed. It never rebuilds intent,
// binding or payment, and never sends (T025; J5/C9).
func (s *Store) UnknownRecovery(ctx context.Context, attemptID, txHash string) (*UnknownRecovery, error) {
	a, err := s.AttemptByID(ctx, attemptID)
	if err != nil {
		return nil, err
	}
	out := &UnknownRecovery{
		AttemptID: a.AttemptID, IntentID: a.IntentID, State: a.State,
		RevisionSeq: a.RevisionSeq, TxHash: strings.ToLower(txHash),
	}
	if out.TxHash == "" && a.TxHash != "" {
		out.TxHash = a.TxHash
	}
	if _, _, err := s.signingRow(ctx, attemptID); err == nil {
		out.SignedBytesPresent = true
	}

	sends, err := s.sendFacts(ctx, attemptID)
	if err != nil {
		return nil, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	out.Dispatches = sends
	obs, err := s.reconcileFacts(ctx, attemptID)
	if err != nil {
		return nil, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	out.Observations = obs
	if len(sends) > 0 {
		out.LastGateBasis = s.lastGateBasis(ctx, attemptID)
	}
	_ = s.db.QueryRow(ctx,
		`SELECT COALESCE((SELECT effect FROM tx_receipts WHERE attempt_id = $1 ORDER BY receipt_id DESC LIMIT 1), '')`,
		attemptID).Scan(&out.LastReceiptEffect)
	if ref, _ := s.checkIntentFreeze(ctx, a.IntentID); ref != nil {
		out.Frozen = true
	}
	out.RecoveryConditions = recoveryConditions(out)
	return out, nil
}

// sendFacts loads the immutable dispatch rows.
func (s *Store) sendFacts(ctx context.Context, attemptID string) ([]SendFact, error) {
	rs, err := s.queryRows(ctx,
		`SELECT send_seq, kind, outcome, rpc_class, dispatched_at FROM tx_send_attempts WHERE attempt_id = $1 ORDER BY send_seq`,
		attemptID)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []SendFact
	for rs.Next() {
		var f SendFact
		if err := rs.Scan(&f.SendSeq, &f.Kind, &f.Outcome, &f.RPCClass, &f.DispatchedAt); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rs.Err()
}

// reconcileFacts loads the append-only observations.
func (s *Store) reconcileFacts(ctx context.Context, attemptID string) ([]ReconcileFact, error) {
	rs, err := s.queryRows(ctx,
		`SELECT classification, block_number, COALESCE(block_hash, ''), rpc_class, observed_at
		   FROM tx_reconciliations WHERE attempt_id = $1 ORDER BY reconcile_id`, attemptID)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []ReconcileFact
	for rs.Next() {
		var f ReconcileFact
		if err := rs.Scan(&f.Classification, &f.BlockNumber, &f.BlockHash, &f.RPCClass, &f.ObservedAt); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rs.Err()
}

func (s *Store) lastGateBasis(ctx context.Context, attemptID string) GateSnapshot {
	var snap GateSnapshot
	var claimVersion, authVersion *int64
	var claimExpiry, expiresAt *time.Time
	_ = s.db.QueryRow(ctx,
		`SELECT observed_recovery_version, observed_pause, observed_claim_version, observed_claim_expiry,
		        COALESCE(observed_authorization_id, ''), observed_authorization_version,
		        observed_authorization_state, observed_expires_at, observed_now, observed_binding_state
		   FROM tx_send_attempts WHERE attempt_id = $1 ORDER BY send_id DESC LIMIT 1`,
		attemptID).
		Scan(&snap.ObservedRecoveryVersion, &snap.ObservedPause, &claimVersion, &claimExpiry,
			&snap.ObservedAuthorizationID, &authVersion, &snap.ObservedAuthorizationState,
			&expiresAt, &snap.ObservedNow, &snap.ObservedBindingState)
	snap.ObservedClaimVersion = claimVersion
	snap.ObservedClaimExpiry = claimExpiry
	snap.ObservedAuthorizationVersion = authVersion
	snap.ObservedExpiresAt = expiresAt
	return snap
}

// recoveryConditions states the honest options; it never authorizes a send.
func recoveryConditions(u *UnknownRecovery) []string {
	var out []string
	if u.Frozen {
		out = append(out, "intent frozen pending manual review; replay refused")
	}
	if !u.SignedBytesPresent {
		out = append(out, "no persisted signed bytes; recovery requires the original identity")
	}
	if u.TxHash == "" {
		out = append(out, "no persisted tx_hash; the chain cannot be probed")
	} else {
		out = append(out, "probe the same tx_hash; included -> receipt path, absent -> replay only under full gates")
	}
	last := ""
	if len(u.Observations) > 0 {
		last = u.Observations[len(u.Observations)-1].Classification
	}
	switch last {
	case "not_found_yet":
		out = append(out, "no chain evidence yet; not_found_yet is never a failure verdict")
	case "found_pending":
		out = append(out, "present in mempool; await inclusion before any re-dispatch")
	case "included":
		out = append(out, "already included; no dispatch is required")
	case "unavailable":
		out = append(out, "last probe was unavailable; retry before deciding anything")
	}
	return out
}

// queryRows runs a read query through the store DB.
func (s *Store) queryRows(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if q, ok := s.db.(interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	}); ok {
		return q.Query(ctx, sql, args...)
	}
	return nil, fmt.Errorf("store database does not support Query")
}

// detectFreeze implements T039: a proven-stale basis (a pause committed before
// the dispatch that recorded 'none') freezes the intent's further sends.
// Post-hoc version equality alone proves nothing; unresolvable ordering stays
// indeterminate and is never defaulted safe.
func (s *Store) detectFreeze(ctx context.Context, tx pgx.Tx, a *Attempt) (bool, error) {
	rows, err := tx.Query(ctx,
		`SELECT sa.send_id, sa.observed_pause, sa.dispatched_at,
		        (SELECT MIN(p.created_at) FROM (
		            SELECT created_at FROM indexer_pause WHERE chain_id = $2
		            UNION ALL SELECT created_at FROM log_pause WHERE chain_id = $2
		            UNION ALL SELECT created_at FROM deposit_pause WHERE chain_id = $2) p)
		   FROM tx_send_attempts sa
		  WHERE sa.attempt_id = $1 AND sa.dispatched_at IS NOT NULL`, a.AttemptID, a.ChainID)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var sendID int64
		var observedPause string
		var dispatchedAt *time.Time
		var earliestPause *time.Time
		if err := rows.Scan(&sendID, &observedPause, &dispatchedAt, &earliestPause); err != nil {
			return false, err
		}
		if observedPause != "none" || dispatchedAt == nil || earliestPause == nil {
			continue
		}
		if earliestPause.Before(*dispatchedAt) {
			evidence := fmt.Sprintf("send_id=%d recorded pause=none but a pause committed at %s before dispatched_at=%s",
				sendID, earliestPause.UTC().Format(time.RFC3339Nano), dispatchedAt.UTC().Format(time.RFC3339Nano))
			if _, err := tx.Exec(ctx,
				`INSERT INTO tx_intent_freezes (intent_id, cause, evidence) VALUES ($1, 'protection_loss_residual', $2)
				 ON CONFLICT (intent_id) DO NOTHING`, a.IntentID, evidence); err != nil {
				return false, err
			}
			if err := appendEventTx(ctx, tx, a.AttemptID, EventFrozen, "protection_loss_residual", recoveryVersionPtr(a.RecoveryVersion), evidence); err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return false, rows.Err()
}
