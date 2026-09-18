package txlifecycle

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// gateInputs is the read-only input to the fixed-order gate sequence.
type gateInputs struct {
	Attempt          *Attempt
	Claim            ClaimRef
	Kind             SendKind
	ExpectedRevision *int64
	// AnchorAuthorizationID is the authorization_id of the replacement anchor
	// (empty for a fresh attempt). Equal to the attempt's own authorization_id
	// means the conditional reuse branch (T031).
	AnchorAuthorizationID string
}

// GateSnapshot is the recorded basis under which a send region decided. It is
// evidence only; it is never read back as an authorization (data-model Table 3).
type GateSnapshot struct {
	ObservedRecoveryVersion      int64
	ObservedPause                string
	ObservedClaimVersion         *int64
	ObservedClaimExpiry          *time.Time
	ObservedAuthorizationID      string
	ObservedAuthorizationVersion *int64
	ObservedAuthorizationState   string
	ObservedExpiresAt            *time.Time
	ObservedNow                  time.Time
	ObservedBindingState         string
}

// gateResult carries the snapshot plus the locked attempt facts the dispatch
// path needs.
type gateResult struct {
	Snapshot GateSnapshot
	Revision int64
	SendSeq  int
	State    string
}

// runGates executes the fixed lock/read sequence of send-gate.md §2 inside the
// caller's T3 transaction. Every condition is read inside the locks; a failed
// condition is a fail-closed refusal with a recorded basis and zero dispatch.
// A hard storage error (lock wait/deadlock/connection loss) is returned as the
// third value so the caller can record region_aborted_no_dispatch (G-010-2(b)).
func runGates(ctx context.Context, tx pgx.Tx, in gateInputs) (gateResult, *RefusalError, error) {
	var res gateResult
	snap := &res.Snapshot

	// (1) execution claim FOR SHARE, keyed by intent_id.
	basis, ref := NewClaimAdapter().Read(ctx, tx, in.Claim)
	if ref != nil {
		snap.ObservedClaimVersion = int64Ptr(basis.LeaseVersion)
		snap.ObservedClaimExpiry = timePtr(basis.ExpiresAt)
		snap.ObservedNow = basis.Now
		if ref.Class == ClassGateReadFailed {
			return res, nil, ref
		}
		return res, ref, nil
	}
	snap.ObservedClaimVersion = int64Ptr(basis.LeaseVersion)
	snap.ObservedClaimExpiry = timePtr(basis.ExpiresAt)
	snap.ObservedNow = basis.Now

	// (2) chain coordination row FOR UPDATE, then the gate tables in SHARE
	// MODE (relation-level, covers present and future rows).
	var coordChain int64
	if err := tx.QueryRow(ctx,
		`SELECT chain_id FROM indexer_lease WHERE chain_id = $1 FOR UPDATE`, in.Attempt.ChainID).Scan(&coordChain); err != nil {
		if err == pgx.ErrNoRows {
			return res, Refuse(ClassCoordinationUnavailable, "indexer_lease", "no coordination row for chain"), nil
		}
		return res, nil, err
	}
	if _, err := tx.Exec(ctx,
		`LOCK TABLE indexer_pause, log_pause, deposit_pause, reorg_recovery, reorg_recovery_events IN SHARE MODE`); err != nil {
		return res, nil, err
	}

	// (3) 006 gate read in one statement: any pause row, active recovery, and
	// the current recovery version (active seq else events MAX else 0).
	var pausedIndexer, pausedLog, pausedDeposit bool
	var activeVersion, eventVersion *int64
	if err := tx.QueryRow(ctx,
		`SELECT
		   EXISTS(SELECT 1 FROM indexer_pause WHERE chain_id = $1),
		   EXISTS(SELECT 1 FROM log_pause WHERE chain_id = $1),
		   EXISTS(SELECT 1 FROM deposit_pause WHERE chain_id = $1),
		   (SELECT recovery_seq FROM reorg_recovery WHERE chain_id = $1),
		   (SELECT MAX(recovery_seq) FROM reorg_recovery_events WHERE chain_id = $1)`,
		in.Attempt.ChainID).Scan(&pausedIndexer, &pausedLog, &pausedDeposit, &activeVersion, &eventVersion); err != nil {
		return res, nil, err
	}
	pauses := pauseCauses(pausedIndexer, pausedLog, pausedDeposit)
	snap.ObservedPause = joinCauses(pauses)
	if len(pauses) > 0 {
		return res, &RefusalError{Class: ClassPausePresent, Basis: "paused: " + snap.ObservedPause}, nil
	}
	if activeVersion != nil {
		snap.ObservedRecoveryVersion = *activeVersion
		return res, &RefusalError{Class: ClassRecoveryActive, Basis: fmt.Sprintf("active recovery_seq=%d", *activeVersion)}, nil
	}
	currentVersion := int64(0)
	if eventVersion != nil {
		currentVersion = *eventVersion
	}
	snap.ObservedRecoveryVersion = currentVersion
	if currentVersion != in.Attempt.RecoveryVersion {
		return res, &RefusalError{Class: ClassRecoveryVersionChanged,
			Basis: fmt.Sprintf("observed recovery_version=%d, attempt built under %d", currentVersion, in.Attempt.RecoveryVersion)}, nil
	}

	// (4) 008 scope row FOR SHARE (serialization only; the binding read is the
	// authority).
	sender := strings.ToLower(in.Attempt.Sender)
	var scopeChain int64
	if err := tx.QueryRow(ctx,
		`SELECT chain_id FROM nonce_scope_state WHERE chain_id = $1 AND sender = $2 FOR SHARE`,
		in.Attempt.ChainID, sender).Scan(&scopeChain); err != nil && err != pgx.ErrNoRows {
		return res, nil, err
	}

	// (5) 008 binding + registry.
	var bIntent, bSender, bNonce, bState, rState string
	var bChain int64
	err := tx.QueryRow(ctx,
		`SELECT b.intent_id, b.chain_id, b.sender, b.nonce, b.state, r.state
		   FROM nonce_bindings b
		   JOIN nonce_wallet_registry r ON r.chain_id = b.chain_id AND r.sender = b.sender
		  WHERE b.binding_id = $1`, in.Attempt.BindingRef).
		Scan(&bIntent, &bChain, &bSender, &bNonce, &bState, &rState)
	if err == pgx.ErrNoRows {
		return res, Refuse(ClassBindingAbsent, "binding_ref", "no binding row for binding_ref"), nil
	}
	if err != nil {
		return res, nil, err
	}
	snap.ObservedBindingState = bState
	if bIntent != in.Attempt.IntentID || bChain != in.Attempt.ChainID || bSender != sender || !decimalEqual(bNonce, in.Attempt.Nonce) {
		return res, &RefusalError{Class: ClassBindingConflict, Basis: "binding facts differ from the attempt"}, nil
	}
	switch bState {
	case "allocated", "in_flight":
	default:
		return res, &RefusalError{Class: ClassBindingTerminal, Basis: "binding state=" + bState}, nil
	}
	if rState != "active" {
		return res, &RefusalError{Class: ClassBindingPaused, Basis: "registry state=" + rState}, nil
	}
	var holdCount int64
	var holdCauses string
	if err := tx.QueryRow(ctx,
		`SELECT count(*), COALESCE(string_agg(DISTINCT cause, ','), '')
		   FROM nonce_scope_holds WHERE chain_id = $1 AND sender = $2 AND status = 'active'`,
		in.Attempt.ChainID, sender).Scan(&holdCount, &holdCauses); err != nil {
		return res, nil, err
	}
	if holdCount > 0 {
		return res, &RefusalError{Class: ClassBindingPaused, Basis: "active holds: " + holdCauses}, nil
	}

	// (6) 007 grant + PB scope FOR SHARE, expiry evaluated on the statement
	// wall clock (never transaction-start now()).
	var aState, aAsset, aRecipient, aAmount string
	var aChain int64
	var aExpires *time.Time
	var now time.Time
	err = tx.QueryRow(ctx,
		`SELECT state, chain_id, asset, recipient, amount, expires_at, clock_timestamp()
		   FROM withdrawal_authorizations WHERE authorization_id = $1 FOR SHARE`, in.Attempt.AuthorizationID).
		Scan(&aState, &aChain, &aAsset, &aRecipient, &aAmount, &aExpires, &now)
	if err == pgx.ErrNoRows {
		return res, Refuse(ClassAuthorizationMissing, "authorization_id", "no grant row"), nil
	}
	if err != nil {
		return res, nil, err
	}
	snap.ObservedAuthorizationID = in.Attempt.AuthorizationID
	snap.ObservedAuthorizationState = aState
	snap.ObservedExpiresAt = aExpires
	snap.ObservedNow = now.UTC()
	switch aState {
	case "active":
	case "revoked":
		return res, &RefusalError{Class: ClassAuthorizationRevoked, Basis: "grant state=revoked"}, nil
	default:
		return res, &RefusalError{Class: ClassAuthorizationInactive, Basis: "grant state=" + aState}, nil
	}
	if aExpires != nil && !aExpires.After(now) {
		return res, &RefusalError{Class: ClassAuthorizationExpired,
			Basis: fmt.Sprintf("expires_at=%s observed_now=%s", aExpires.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano))}, nil
	}
	if aChain != in.Attempt.ChainID || aAsset != strings.ToLower(in.Attempt.Asset) ||
		aRecipient != strings.ToLower(in.Attempt.Recipient) || !decimalEqual(aAmount, in.Attempt.Amount) {
		return res, &RefusalError{Class: ClassAuthorizationMismatch, Basis: "grant economic fields differ from the attempt"}, nil
	}

	var sIntent, sSender string
	var sVersion int64
	var allowsReplacement bool
	var feeMaxTotal, feeMaxPerGas, feeMaxPriority int64
	err = tx.QueryRow(ctx,
		`SELECT intent_id, sender, authorization_version, allows_fee_replacement,
		        fee_max_total, fee_max_per_gas, fee_max_priority
		   FROM withdrawal_authorization_scopes WHERE authorization_id = $1 FOR SHARE`,
		in.Attempt.AuthorizationID).
		Scan(&sIntent, &sSender, &sVersion, &allowsReplacement, &feeMaxTotal, &feeMaxPerGas, &feeMaxPriority)
	if err == pgx.ErrNoRows {
		return res, Refuse(ClassAuthorizationUnverifiable, "authorization_scope", "grant has no PB scope row"), nil
	}
	if err != nil {
		return res, nil, err
	}
	snap.ObservedAuthorizationVersion = int64Ptr(sVersion)
	if sIntent != in.Attempt.IntentID || sSender != sender || sVersion != in.Attempt.AuthorizationVersion {
		return res, &RefusalError{Class: ClassAuthorizationMismatch,
			Basis: fmt.Sprintf("scope intent/sender/version differ (version observed=%d attempt=%d)", sVersion, in.Attempt.AuthorizationVersion)}, nil
	}

	// (7) PB conditional reuse (FR-06): only when the replacement names the
	// anchor's own grant. A fresh grant skips this and passes the full gate.
	if in.Attempt.ReplacementOf != "" && in.Attempt.AuthorizationID == in.AnchorAuthorizationID {
		if !allowsReplacement {
			return res, Refuse(ClassScopeReuseForbidden, "allows_fee_replacement", "scope forbids fee replacement"), nil
		}
		if feeRef := checkFeeScope(in.Attempt, feeMaxTotal, feeMaxPerGas, feeMaxPriority); feeRef != nil {
			return res, feeRef, nil
		}
	}

	// (8) attempt row FOR UPDATE + sendability + send_seq + optional revision.
	revision, state, err := lockAttemptRow(ctx, tx, in.Attempt.AttemptID)
	if err != nil {
		if ref, ok := err.(*RefusalError); ok {
			return res, ref, nil
		}
		return res, nil, err
	}
	res.Revision = revision
	res.State = state
	if in.ExpectedRevision != nil && *in.ExpectedRevision != revision {
		return res, &RefusalError{Class: ClassSendStale, Basis: fmt.Sprintf("expected revision=%d observed=%d", *in.ExpectedRevision, revision)}, nil
	}
	var accepted bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM tx_send_attempts WHERE attempt_id = $1 AND outcome = 'accepted')`,
		in.Attempt.AttemptID).Scan(&accepted); err != nil {
		return res, nil, err
	}
	if sref := sendabilityRefusal(in.Kind, state, accepted); sref != nil {
		return res, sref, nil
	}
	var sendSeq int
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(send_seq), 0) + 1 FROM tx_send_attempts WHERE attempt_id = $1`,
		in.Attempt.AttemptID).Scan(&sendSeq); err != nil {
		return res, nil, err
	}
	res.SendSeq = sendSeq
	return res, nil, nil
}

// sendabilityRefusal enforces the data-model sendability matrix.
func sendabilityRefusal(kind SendKind, state string, accepted bool) *RefusalError {
	switch kind {
	case SendInitial:
		if state == "signed" {
			return nil
		}
		if state == "prepared" {
			return &RefusalError{Class: ClassAttemptNotSendable, Basis: "attempt is not signed yet"}
		}
		if accepted || state == "sent" || state == "unknown" {
			return &RefusalError{Class: ClassAlreadyAccepted, Basis: "state=" + state + " already has a dispatch"}
		}
		return &RefusalError{Class: ClassAttemptNotSendable, Basis: "state=" + state}
	case SendReplay:
		switch state {
		case "sent", "unknown", "signed":
			return nil
		case "prepared":
			return &RefusalError{Class: ClassSendModeMismatch, Basis: "attempt has no signed result to replay"}
		default:
			return &RefusalError{Class: ClassAttemptNotSendable, Basis: "state=" + state}
		}
	default:
		return &RefusalError{Class: ClassSendModeMismatch, Basis: "unknown send kind"}
	}
}

// checkFeeScope enforces the PB fee bounds with exact big.Int arithmetic and
// per-dimension evidence (PB-C2; T031).
func checkFeeScope(a *Attempt, feeMaxTotal, feeMaxPerGas, feeMaxPriority int64) *RefusalError {
	gasLimit, ok := new(big.Int).SetString(a.GasLimit, 10)
	if !ok {
		return Refuse(ClassFeeScopeExceeded, "gas_limit", "unparseable gas_limit")
	}
	feePerGas := a.GasPrice
	priority := a.MaxPriorityFeePerGas
	if a.TxType == int(TxTypeDynamicFee) {
		feePerGas = a.MaxFeePerGas
	}
	fee, ok := new(big.Int).SetString(feePerGas, 10)
	if !ok {
		return Refuse(ClassFeeScopeExceeded, "fee", "unparseable fee")
	}
	total := new(big.Int).Mul(gasLimit, fee)
	if total.Cmp(big.NewInt(feeMaxTotal)) > 0 {
		return &RefusalError{Class: ClassFeeScopeExceeded, Field: "fee_max_total",
			Basis: fmt.Sprintf("gas_limit*fee=%s > fee_max_total=%d", total.String(), feeMaxTotal)}
	}
	if fee.Cmp(big.NewInt(feeMaxPerGas)) > 0 {
		return &RefusalError{Class: ClassFeeScopeExceeded, Field: "fee_max_per_gas",
			Basis: fmt.Sprintf("fee_per_gas=%s > fee_max_per_gas=%d", fee.String(), feeMaxPerGas)}
	}
	if a.TxType == int(TxTypeDynamicFee) {
		p, ok := new(big.Int).SetString(priority, 10)
		if !ok {
			return Refuse(ClassFeeScopeExceeded, "max_priority_fee_per_gas", "unparseable priority fee")
		}
		if p.Cmp(big.NewInt(feeMaxPriority)) > 0 {
			return &RefusalError{Class: ClassFeeScopeExceeded, Field: "fee_max_priority",
				Basis: fmt.Sprintf("max_priority_fee_per_gas=%s > fee_max_priority=%d", p.String(), feeMaxPriority)}
		}
	}
	return nil
}

func pauseCauses(indexer, log, deposit bool) []string {
	var causes []string
	if indexer {
		causes = append(causes, "indexer_pause")
	}
	if log {
		causes = append(causes, "log_pause")
	}
	if deposit {
		causes = append(causes, "deposit_pause")
	}
	return causes
}

func joinCauses(causes []string) string {
	if len(causes) == 0 {
		return "none"
	}
	return strings.Join(causes, ",")
}

func int64Ptr(v int64) *int64 { return &v }

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// decimalEqual compares two NUMERIC decimal strings numerically.
func decimalEqual(a, b string) bool {
	ai, okA := new(big.Int).SetString(a, 10)
	bi, okB := new(big.Int).SetString(b, 10)
	return okA && okB && ai.Cmp(bi) == 0
}
