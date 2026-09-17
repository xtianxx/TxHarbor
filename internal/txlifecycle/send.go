package txlifecycle

import (
	"context"
	"errors"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
	"github.com/xtianxx/txharbor/internal/eth"
)

// SendKind is the caller's declared dispatch intent; it is validated against
// durable state rather than guessed (send-api.md §2.2).
type SendKind string

const (
	SendInitial SendKind = "initial"
	SendReplay  SendKind = "replay"
)

// SendRequest is the exact caller schema of send-api.md §2.2.
type SendRequest struct {
	AttemptID        string
	Kind             SendKind
	Claim            ClaimRef
	ExpectedRevision *int64
}

// SendResult is the dispatch outcome. Outcome is one of accepted/rejected/
// unknown/blocked; a blocked result carries RefusalClass and records zero
// dispatch. None of these values is a payment verdict (FR-08).
type SendResult struct {
	Outcome      string
	RPCClass     string
	TxHash       string
	SendSeq      int
	RefusalClass string
}

// ChainRPC is the minimal chain surface the send/reconcile paths need;
// *eth.Client satisfies it.
type ChainRPC interface {
	SendSignedTransaction(ctx context.Context, raw []byte, expected common.Hash) (common.Hash, error)
	TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error)
	TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error)
	BlockNumber(ctx context.Context) (uint64, error)
}

// WithChain wires the chain client and dispatch timeout used by T3/T4.
func (s *Store) WithChain(rpc ChainRPC, timeout time.Duration) *Store {
	s.rpc = rpc
	if timeout > 0 {
		s.sendTimeout = timeout
	}
	return s
}

// WithSigner wires the 009 client used by T2.
func (s *Store) WithSigner(c *SignerClient) *Store {
	s.signer = c
	return s
}

// WithReleaseToken configures the controlled manual-release permission (T040).
func (s *Store) WithReleaseToken(token string) *Store {
	s.releaseToken = token
	return s
}

// Send runs the send region: T2 first when the attempt is unsigned, then the
// gate-verified T3 dispatch. A zero-dispatch refusal is returned with
// Outcome=blocked and a committed gate_refused event (T017).
func (s *Store) Send(ctx context.Context, req *SendRequest) (SendResult, error) {
	if s == nil || s.db == nil {
		return SendResult{}, Refuse(ClassCoordinationUnavailable, "", "store has no database")
	}
	if req == nil || req.AttemptID == "" {
		return SendResult{}, Refuse(ClassAttemptNotFound, "attempt_id", "empty attempt id")
	}
	a, err := s.AttemptByID(ctx, req.AttemptID)
	if err != nil {
		return SendResult{}, err
	}

	if a.State == "prepared" {
		if req.Kind != SendInitial {
			return s.refuse(a, Refuse(ClassSendModeMismatch, "kind", "attempt is unsigned; only initial is valid"))
		}
		if s.signer == nil {
			return s.refuse(a, Refuse(ClassAttemptNotSendable, "signer", "attempt is unsigned and no signer is configured"))
		}
		result, err := s.signer.Submit(ctx, a)
		if err != nil {
			return SendResult{}, err
		}
		if _, err := s.SignAndPersist(ctx, a, result); err != nil {
			return SendResult{}, err
		}
		if a, err = s.AttemptByID(ctx, req.AttemptID); err != nil {
			return SendResult{}, err
		}
	}

	// G-010-2 class (c): a frozen intent refuses every further send. This is
	// an independent cause that only a controlled manual release lifts.
	if ref, err := s.checkIntentFreeze(ctx, a.IntentID); err != nil {
		return SendResult{}, Refuse(ClassCoordinationUnavailable, "", "freeze read failed")
	} else if ref != nil {
		s.recordStandaloneRefusal(ctx, a, ref)
		return s.refuse(a, ref)
	}

	// T024: a dispatch from signed/unknown is crash-ambiguous and MUST have a
	// reconcile observation recorded in this operation first.
	if a.State == "signed" || a.State == "unknown" {
		rec, err := s.Reconcile(ctx, req.AttemptID, "")
		if err != nil {
			return SendResult{}, err
		}
		switch rec.Classification {
		case "unavailable":
			return s.refuse(a, Refuse(ClassGateReadFailed, "reconcile", "chain probe unavailable; no dispatch decided"))
		case "included":
			return s.refuse(a, Refuse(ClassAlreadyAccepted, "reconcile", "transaction is already included; no re-dispatch"))
		}
		if a, err = s.AttemptByID(ctx, req.AttemptID); err != nil {
			return SendResult{}, err
		}
	}

	anchorAuth := ""
	if a.ReplacementOf != "" {
		anchor, err := s.AttemptByID(ctx, a.ReplacementOf)
		if err != nil {
			return SendResult{}, err
		}
		anchorAuth = anchor.AuthorizationID
	}
	return s.sendRegion(ctx, a, req, anchorAuth)
}

// sendRegion is T3: fixed-order gates, exactly one bounded dispatch, and one
// COMMIT carrying the gate snapshot + outcome + state transition + event.
func (s *Store) sendRegion(ctx context.Context, a *Attempt, req *SendRequest, anchorAuth string) (SendResult, error) {
	if s.rpc == nil {
		return s.refuse(a, Refuse(ClassCoordinationUnavailable, "rpc", "no chain client configured"))
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return SendResult{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return SendResult{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	if _, err := tx.Exec(ctx, lockGuard); err != nil {
		_ = tx.Rollback(ctx)
		return s.recordRegionAbort(ctx, a, err)
	}

	res, ref, err := runGates(ctx, tx, gateInputs{
		Attempt: a, Claim: req.Claim, Kind: req.Kind,
		ExpectedRevision: req.ExpectedRevision, AnchorAuthorizationID: anchorAuth,
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		return s.recordRegionAbort(ctx, a, err)
	}
	if ref != nil {
		return s.commitRefusal(ctx, tx, a, ref)
	}

	hash, raw, err := signingRowTx(ctx, tx, a.AttemptID)
	if err != nil {
		_ = tx.Rollback(ctx)
		return s.recordRegionAbort(ctx, a, err)
	}

	// G-010-1: re-evaluate expiry as the last read before dispatch entry.
	expiresAt, now, ref, err := lastMomentExpiry(ctx, tx, a)
	if err != nil {
		_ = tx.Rollback(ctx)
		return s.recordRegionAbort(ctx, a, err)
	}
	if ref != nil {
		return s.commitRefusal(ctx, tx, a, ref)
	}
	if expiresAt != nil {
		res.Snapshot.ObservedExpiresAt = expiresAt
	}
	res.Snapshot.ObservedNow = now

	// G-010-2(c): same-session liveness probe. A reconnect would not hold the
	// locks; a successful probe proves nothing past its own return.
	var one int
	if err := tx.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil {
		_ = tx.Rollback(ctx)
		return s.recordRegionAbort(ctx, a, err)
	}

	dispatchedAt, err := dbClock(ctx, tx)
	if err != nil {
		_ = tx.Rollback(ctx)
		return s.recordRegionAbort(ctx, a, err)
	}
	dispatchCtx, cancel := context.WithTimeout(ctx, s.sendTimeout)
	returned, dispatchErr := s.rpc.SendSignedTransaction(dispatchCtx, raw, common.HexToHash(hash))
	cancel()
	_ = returned
	outcome, rpcClass := classifyDispatch(dispatchErr)
	res.Snapshot.ObservedNow = dispatchedAt
	s.observeDispatch(outcome)

	if err := recordDispatchTx(ctx, tx, a, res, req.Kind, outcome, rpcClass, dispatchedAt); err != nil {
		_ = tx.Rollback(ctx)
		if errors.Is(err, ErrRevisionMoved) {
			return s.recordRegionAbort(ctx, a, err)
		}
		return s.recordRegionWriteFailure(ctx, a, res, req.Kind, hash, rpcClass)
	}
	if err := tx.Commit(ctx); err != nil {
		_ = tx.Rollback(ctx)
		return s.recordRegionWriteFailure(ctx, a, res, req.Kind, hash, rpcClass)
	}
	s.observeUnknown(ctx)
	return SendResult{Outcome: outcome, RPCClass: rpcClass, TxHash: hash, SendSeq: res.SendSeq}, nil
}

// commitRefusal commits zero-dispatch evidence in the region transaction.
func (s *Store) commitRefusal(ctx context.Context, tx pgx.Tx, a *Attempt, ref *RefusalError) (SendResult, error) {
	if err := appendEventTx(ctx, tx, a.AttemptID, EventGateRefused, string(ref.Class), recoveryVersionPtr(a.RecoveryVersion), ref.Basis); err != nil {
		_ = tx.Rollback(ctx)
		if isInFailedTransaction(err) {
			// G-010-4: an undefined-table/column claim read poisons the region
			// transaction. Record the decided refusal in a fresh transaction so
			// the class survives instead of degrading to coordination_unavailable.
			s.recordStandaloneRefusal(ctx, a, ref)
			return s.refuse(a, ref)
		}
		return s.recordRegionAbort(ctx, a, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return s.refuse(a, ref)
	}
	return s.refuse(a, ref)
}

// recordRegionAbort handles G-010-2(b): a failure detected before dispatch was
// invoked. Zero dispatch is certain in-process; the attempt's business effect
// stays unknown pending reconcile; committed refusals/verdicts are untouched.
func (s *Store) recordRegionAbort(ctx context.Context, a *Attempt, cause error) (SendResult, error) {
	rx, err := s.db.Begin(ctx)
	if err == nil {
		defer func() { _ = rx.Rollback(ctx) }()
		if _, execErr := rx.Exec(ctx, writeGuard); execErr == nil {
			if _, _, lockErr := lockAttemptRow(ctx, rx, a.AttemptID); lockErr == nil {
				_ = appendEventTx(ctx, rx, a.AttemptID, EventRegionAbortedNoDispatch, "", recoveryVersionPtr(a.RecoveryVersion), cause.Error())
				_ = rx.Commit(ctx)
			}
		}
	}
	ref := Refuse(ClassCoordinationUnavailable, "", "region aborted before dispatch: "+cause.Error())
	return s.refuse(a, ref)
}

// recordRegionWriteFailure handles G-010-2(a): dispatch entered with protection
// held, then the region COMMIT failed. A possibly-unrecorded send is
// send-unknown, never inferred as not-sent; the hash is probed for evidence.
func (s *Store) recordRegionWriteFailure(ctx context.Context, a *Attempt, res gateResult, kind SendKind, hash, rpcClass string) (SendResult, error) {
	rx, err := s.db.Begin(ctx)
	if err == nil {
		func() {
			defer func() { _ = rx.Rollback(ctx) }()
			if _, execErr := rx.Exec(ctx, writeGuard); execErr != nil {
				return
			}
			rev, state, lockErr := lockAttemptRow(ctx, rx, a.AttemptID)
			if lockErr != nil {
				return
			}
			seq, seqErr := nextSendSeq(ctx, rx, a.AttemptID)
			if seqErr != nil {
				return
			}
			res.SendSeq = seq
			res.Snapshot = snapshotFor(res.Snapshot, a, state)
			if insertErr := insertSendRow(ctx, rx, a, seq, kind, "unknown", "storage_region_failure", res.Snapshot, time.Now().UTC()); insertErr != nil {
				return
			}
			to := "unknown"
			if state == "sent" {
				to = "sent"
			}
			if _, err := applyStateTx(ctx, rx, a.AttemptID, rev, []string{state}, to, ""); err != nil {
				return
			}
			_ = appendEventTx(ctx, rx, a.AttemptID, EventSendUnknown, "storage_region_failure", recoveryVersionPtr(a.RecoveryVersion), "tx_hash="+hash)
			_ = rx.Commit(ctx)
		}()
	}
	_, _ = s.Reconcile(ctx, a.AttemptID, hash)
	s.observeDispatch("unknown")
	return SendResult{Outcome: "unknown", RPCClass: "storage_region_failure", TxHash: hash, SendSeq: res.SendSeq}, nil
}

// recordDispatchTx inserts the send row, applies the guarded attempt
// transition and appends the outcome event in the region transaction.
func recordDispatchTx(ctx context.Context, tx pgx.Tx, a *Attempt, res gateResult, kind SendKind, outcome, rpcClass string, dispatchedAt time.Time) error {
	rev, state, err := lockAttemptRow(ctx, tx, a.AttemptID)
	if err != nil {
		return err
	}
	if err := insertSendRow(ctx, tx, a, res.SendSeq, kind, outcome, rpcClass, res.Snapshot, dispatchedAt); err != nil {
		return err
	}
	to := "unknown"
	if outcome == "accepted" || state == "sent" {
		to = "sent"
	}
	if _, err := applyStateTx(ctx, tx, a.AttemptID, rev, []string{state}, to, ""); err != nil {
		return err
	}
	if event := dispatchEvent(kind, outcome); event != "" {
		if err := appendEventTx(ctx, tx, a.AttemptID, event, rpcClass, recoveryVersionPtr(a.RecoveryVersion), "tx_hash="+signingHashOrEmpty(ctx, tx, a.AttemptID)); err != nil {
			return err
		}
	}
	// G-010-1 residual: if the DB clock at dispatch entry is past the last
	// observed expiry, record the natural-expiry residual (never described as
	// legally in-flight).
	if res.Snapshot.ObservedExpiresAt != nil && dispatchedAt.After(*res.Snapshot.ObservedExpiresAt) {
		detail := "observed_expires_at=" + res.Snapshot.ObservedExpiresAt.UTC().Format(time.RFC3339Nano) +
			" dispatched_at=" + dispatchedAt.UTC().Format(time.RFC3339Nano)
		if err := appendEventTx(ctx, tx, a.AttemptID, EventPostFinalCheckExpiry, "", recoveryVersionPtr(a.RecoveryVersion), detail); err != nil {
			return err
		}
	}
	return nil
}

func dispatchEvent(kind SendKind, outcome string) string {
	switch outcome {
	case "rejected":
		return EventSendRejected
	case "unknown":
		return EventSendUnknown
	case "accepted":
		if kind == SendReplay {
			return EventReplayed
		}
	}
	return ""
}

func insertSendRow(ctx context.Context, tx pgx.Tx, a *Attempt, seq int, kind SendKind, outcome, rpcClass string, snap GateSnapshot, dispatchedAt time.Time) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO tx_send_attempts (
		   attempt_id, send_seq, kind, outcome, rpc_class,
		   observed_recovery_version, observed_pause, observed_claim_version, observed_claim_expiry,
		   observed_authorization_id, observed_authorization_version, observed_authorization_state,
		   observed_expires_at, observed_now, observed_binding_state, dispatched_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
		a.AttemptID, seq, string(kind), outcome, rpcClass,
		snap.ObservedRecoveryVersion, emptyToNone(snap.ObservedPause), snap.ObservedClaimVersion, snap.ObservedClaimExpiry,
		nullIfEmpty(snap.ObservedAuthorizationID), snap.ObservedAuthorizationVersion, snap.ObservedAuthorizationState,
		snap.ObservedExpiresAt, snap.ObservedNow, snap.ObservedBindingState, dispatchedAt.UTC())
	return err
}

// snapshotFor fills recovery/pause/binding defaults for the region-write-failure
// recovery row where the full snapshot was not committed.
func snapshotFor(snap GateSnapshot, a *Attempt, state string) GateSnapshot {
	snap.ObservedRecoveryVersion = a.RecoveryVersion
	if snap.ObservedPause == "" {
		snap.ObservedPause = "none"
	}
	snap.ObservedBindingState = state
	if snap.ObservedNow.IsZero() {
		snap.ObservedNow = time.Now().UTC()
	}
	return snap
}

func nextSendSeq(ctx context.Context, tx pgx.Tx, attemptID string) (int, error) {
	var seq int
	err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(send_seq), 0) + 1 FROM tx_send_attempts WHERE attempt_id = $1`, attemptID).Scan(&seq)
	return seq, err
}

func signingHashOrEmpty(ctx context.Context, tx pgx.Tx, attemptID string) string {
	var hash string
	if err := tx.QueryRow(ctx, `SELECT tx_hash FROM tx_attempt_signings WHERE attempt_id = $1`, attemptID).Scan(&hash); err != nil {
		return ""
	}
	return hash
}

// signingRowTx loads the persisted signed bytes for dispatch inside the region.
func signingRowTx(ctx context.Context, tx pgx.Tx, attemptID string) (string, []byte, error) {
	var hash string
	var raw []byte
	if err := tx.QueryRow(ctx, `SELECT tx_hash, signed_tx_bytes FROM tx_attempt_signings WHERE attempt_id = $1`, attemptID).Scan(&hash, &raw); err != nil {
		return "", nil, err
	}
	return hash, raw, nil
}

// signingRow loads the persisted signed bytes outside a region.
func (s *Store) signingRow(ctx context.Context, attemptID string) (string, []byte, error) {
	var hash string
	var raw []byte
	if err := s.db.QueryRow(ctx, `SELECT tx_hash, signed_tx_bytes FROM tx_attempt_signings WHERE attempt_id = $1`, attemptID).Scan(&hash, &raw); err != nil {
		return "", nil, err
	}
	return hash, raw, nil
}

// lastMomentExpiry re-evaluates grant expiry on the statement wall clock as the
// final read before dispatch entry (never transaction-start now()).
func lastMomentExpiry(ctx context.Context, tx pgx.Tx, a *Attempt) (*time.Time, time.Time, *RefusalError, error) {
	var now time.Time
	var expires *time.Time
	err := tx.QueryRow(ctx,
		`SELECT clock_timestamp(), (SELECT expires_at FROM withdrawal_authorizations WHERE authorization_id = $1)`,
		a.AuthorizationID).Scan(&now, &expires)
	if err != nil {
		return nil, time.Time{}, nil, err
	}
	now = now.UTC()
	if expires != nil && !expires.After(now) {
		return expires, now, &RefusalError{Class: ClassAuthorizationExpired, Field: "expires_at",
			Basis: "expiry observed at post-final-check read"}, nil
	}
	return expires, now, nil, nil
}

// dbClock reads the statement wall clock.
func dbClock(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, err
	}
	return now.UTC(), nil
}

// recordStandaloneRefusal commits gate-refusal evidence for a refusal decided
// before the region transaction is open (the intent freeze). Best-effort: the
// refusal itself is already fail-closed.
func (s *Store) recordStandaloneRefusal(ctx context.Context, a *Attempt, ref *RefusalError) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return
	}
	if _, _, err := lockAttemptRow(ctx, tx, a.AttemptID); err != nil {
		return
	}
	if err := appendEventTx(ctx, tx, a.AttemptID, EventGateRefused, string(ref.Class), recoveryVersionPtr(a.RecoveryVersion), ref.Basis); err != nil {
		return
	}
	_ = tx.Commit(ctx)
}

// checkIntentFreeze returns a refusal while an intent's freeze cause is
// unreleased (T039).
func (s *Store) checkIntentFreeze(ctx context.Context, intentID string) (*RefusalError, error) {
	var cause, evidence string
	var frozenAt time.Time
	err := s.db.QueryRow(ctx,
		`SELECT cause, evidence, frozen_at FROM tx_intent_freezes WHERE intent_id = $1 AND released_at IS NULL`, intentID).
		Scan(&cause, &evidence, &frozenAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &RefusalError{Class: ClassIntentFrozen, Field: "intent_id",
		Basis: "frozen cause=" + cause + " at=" + frozenAt.UTC().Format(time.RFC3339Nano) + " evidence=" + evidence}, nil
}

// classifyDispatch maps a dispatch error onto the fail-safe send classes
// (R-010-05): accepted only on the same hash or already-known identical bytes.
func classifyDispatch(err error) (outcome, rpcClass string) {
	if err == nil {
		return "accepted", ""
	}
	switch eth.KindOf(err) {
	case eth.KindAlreadyKnown:
		return "accepted", "already_known"
	case eth.KindNonceTooLow:
		return "rejected", "nonce_too_low"
	case eth.KindReplacementUnderpriced:
		return "rejected", "replacement_underpriced"
	case eth.KindInsufficientFunds:
		return "rejected", "insufficient_funds"
	case eth.KindIntrinsicGasTooLow:
		return "rejected", "intrinsic_gas_too_low"
	case eth.KindTimeout:
		return "unknown", "timeout"
	case eth.KindRateLimited:
		return "unknown", "rate_limited"
	case eth.KindHashMismatch:
		return "unknown", "hash_mismatch"
	case eth.KindInvalidResponse:
		return "unknown", "invalid_response"
	case eth.KindTransport:
		return "unknown", "transport"
	default:
		return "unknown", "unrecognized"
	}
}

// blocked builds a zero-dispatch result paired with its refusal.
func blocked(a *Attempt, ref *RefusalError) (SendResult, error) {
	hash := ""
	if a != nil {
		hash = a.TxHash
	}
	return SendResult{Outcome: "blocked", RefusalClass: string(ref.Class), TxHash: hash}, ref
}

func emptyToNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// feeTotal is gas_limit × effective fee-per-gas, exact (T031).
func feeTotal(a *Attempt) *big.Int {
	gas, _ := new(big.Int).SetString(a.GasLimit, 10)
	fee := a.GasPrice
	if a.TxType == int(TxTypeDynamicFee) {
		fee = a.MaxFeePerGas
	}
	f, _ := new(big.Int).SetString(fee, 10)
	if gas == nil || f == nil {
		return new(big.Int)
	}
	return new(big.Int).Mul(gas, f)
}
