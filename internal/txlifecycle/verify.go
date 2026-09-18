package txlifecycle

import (
	"context"
	"errors"
	"math/big"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
)

// receiptEffect computes FR-08's verdict: effective requires status = 1 AND
// the expected Transfer; every deviation is an ineffective_* class.
func receiptEffect(status int, logs []*types.Log, expected ExpectedTransfer) (string, string) {
	if status == 0 {
		return "ineffective_status", ""
	}
	flat := make([]types.Log, 0, len(logs))
	for _, l := range logs {
		if l != nil {
			flat = append(flat, *l)
		}
	}
	ok, reason := FindExpectedTransfer(flat, expected)
	if ok {
		return "effective", ""
	}
	if reason == "transfer_missing" {
		return "ineffective_transfer_missing", reason
	}
	return "ineffective_transfer_mismatch", reason
}

// applyReceipt verifies the included receipt against the attempt's expected
// Transfer and canonical chain view, upserts the receipt, applies the attempt
// verdict + confirmation, and runs the reorg revision pass (T033/T034/T035).
func (s *Store) applyReceipt(ctx context.Context, tx pgx.Tx, a *Attempt, hash string, blockNumber *int64, blockHash string) (string, int64, error) {
	receipt, err := s.rpc.TransactionReceipt(ctx, common.HexToHash(hash))
	if err != nil || receipt == nil {
		return "", 0, nil
	}
	if blockNumber == nil {
		n := receipt.BlockNumber.Int64()
		blockNumber = &n
	}
	if blockHash == "" {
		blockHash = receipt.BlockHash.Hex()
	}
	asset := common.HexToAddress(strings.ToLower(a.Asset))
	sender := common.HexToAddress(strings.ToLower(a.Sender))
	recipient := common.HexToAddress(strings.ToLower(a.Recipient))
	amount, ok := new(big.Int).SetString(a.Amount, 10)
	if !ok {
		return "", 0, Refuse(ClassCoordinationUnavailable, "amount", "unparseable attempt amount")
	}
	expected := ExpectedTransfer{Asset: asset, Sender: sender, Recipient: recipient, Amount: amount}
	effect, detail := receiptEffect(int(receipt.Status), receipt.Logs, expected)

	var canonical bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM chain_blocks WHERE chain_id = $1 AND number = $2 AND hash = $3 AND canonical)`,
		a.ChainID, *blockNumber, strings.ToLower(blockHash)).Scan(&canonical); err != nil {
		return "", 0, err
	}
	canonicality := "unverified"
	if canonical {
		canonicality = "canonical"
	}

	threshold, policySeq, head, err := s.confirmationBasis(ctx, tx, a.ChainID)
	if err != nil {
		return "", 0, err
	}
	confirmations := int64(0)
	if head >= *blockNumber {
		confirmations = head - *blockNumber + 1
	}
	confirmed := canonical && effect == "effective" && confirmations >= threshold

	receiptID, err := upsertReceipt(ctx, tx, a.AttemptID, hash, int(receipt.Status), *blockNumber, strings.ToLower(blockHash),
		effect, detail, canonicality, confirmations, threshold, policySeq, head)
	if err != nil {
		return "", 0, err
	}

	revision, state, err := lockAttemptRow(ctx, tx, a.AttemptID)
	if err != nil {
		return "", 0, err
	}
	toState, extra := receiptTransition(state, effect, canonical, confirmed)
	if toState != "" {
		if _, err := applyStateTx(ctx, tx, a.AttemptID, revision, []string{state}, toState, extra); err != nil {
			if errors.Is(err, ErrRevisionMoved) {
				return "", 0, Refuse(ClassSendStale, "", "attempt revision moved during receipt apply")
			}
			return "", 0, err
		}
	}
	event := EventReceiptVerified
	reason := ""
	if effect != "effective" {
		event = EventReceiptIneffective
		reason = effect
	}
	if err := appendEventTx(ctx, tx, a.AttemptID, event, reason, recoveryVersionPtr(a.RecoveryVersion), "receipt_id="+itoa(receiptID)); err != nil {
		return "", 0, err
	}
	if confirmed {
		if err := appendEventTx(ctx, tx, a.AttemptID, EventConfirmed, "", recoveryVersionPtr(a.RecoveryVersion),
			"confirm_threshold="+itoa(threshold)+" policy_seq="+itoa(policySeq)+" confirmations="+itoa(confirmations)+" tip="+itoa(head)); err != nil {
			return "", 0, err
		}
	}
	if (effect == "effective" || effect == "ineffective") && state == "unknown" {
		if err := appendEventTx(ctx, tx, a.AttemptID, EventUnknownCleared, "", recoveryVersionPtr(a.RecoveryVersion),
			"effect="+effect+" receipt_id="+itoa(receiptID)); err != nil {
			return "", 0, err
		}
	}
	if state == "orphaned" && canonical && effect == "effective" {
		if err := appendEventTx(ctx, tx, a.AttemptID, EventReconfirmed, "", recoveryVersionPtr(a.RecoveryVersion),
			"receipt_id="+itoa(receiptID)); err != nil {
			return "", 0, err
		}
		if s.metrics != nil {
			s.metrics.ObserveTxRevision()
		}
	}

	if _, err := s.reviseOrphanedReceipts(ctx, tx, a, strings.ToLower(blockHash)); err != nil {
		return "", 0, err
	}
	if confirmed || effect == "effective" {
		if err := s.markSiblingsReplaced(ctx, tx, a); err != nil {
			return "", 0, err
		}
	}
	if s.metrics != nil {
		s.metrics.ObserveTxReceiptEffect(effect)
	}
	return effect, confirmations, nil
}

// receiptTransition maps the verdict onto the attempt state machine.
func receiptTransition(state, effect string, canonical, confirmed bool) (string, string) {
	if !canonical {
		return "", ""
	}
	if effect == "effective" {
		if confirmed {
			return "confirmed", "effective_at = COALESCE(effective_at, now()), confirmed_at = COALESCE(confirmed_at, now())"
		}
		if state == "effective" || state == "confirmed" {
			return "", ""
		}
		return "effective", "effective_at = COALESCE(effective_at, now())"
	}
	if state == "ineffective" {
		return "", ""
	}
	return "ineffective", ""
}

// upsertReceipt converges repeat observations on tx_receipts_tx_block_uniq:
// progress is updated, the row is never duplicated.
func upsertReceipt(ctx context.Context, tx pgx.Tx, attemptID, hash string, status int, blockNumber int64, blockHash,
	effect, detail, canonicality string, confirmations, threshold, policySeq, head int64) (int64, error) {
	var receiptID int64
	var tip *int64
	if head >= 0 {
		tip = &head
	}
	err := tx.QueryRow(ctx,
		`INSERT INTO tx_receipts (
		   attempt_id, tx_hash, status, block_number, block_hash, effect, transfer_detail,
		   canonicality, confirmations, confirm_threshold, confirm_policy_seq, confirm_tip_number,
		   confirmed_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
		         CASE WHEN $13 THEN now() ELSE NULL END, now())
		 ON CONFLICT ON CONSTRAINT tx_receipts_tx_block_uniq DO UPDATE SET
		   confirmations = EXCLUDED.confirmations,
		   confirm_tip_number = EXCLUDED.confirm_tip_number,
		   updated_at = now(),
		   confirmed_at = COALESCE(tx_receipts.confirmed_at, EXCLUDED.confirmed_at)
		 RETURNING receipt_id`,
		attemptID, hash, status, blockNumber, blockHash, effect, detail,
		canonicality, confirmations, threshold, policySeq, tip,
		effect == "effective" && canonicality == "canonical" && confirmations >= threshold).Scan(&receiptID)
	return receiptID, err
}

func itoa(v int64) string {
	return strconv.FormatInt(v, 10)
}
