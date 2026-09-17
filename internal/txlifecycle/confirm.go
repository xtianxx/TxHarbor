package txlifecycle

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// confirmationBasis reads the confirmation threshold and its policy sequence
// from 005 read-only (MAX(policy_seq)) plus the canonical head used to compute
// confirmations (T034).
func (s *Store) confirmationBasis(ctx context.Context, tx pgx.Tx, chainID int64) (threshold, policySeq, head int64, err error) {
	err = tx.QueryRow(ctx,
		`SELECT policy_seq, threshold FROM confirmation_policy_history
		  WHERE chain_id = $1 ORDER BY policy_seq DESC LIMIT 1`, chainID).Scan(&policySeq, &threshold)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, 0, Refuse(ClassCoordinationUnavailable, "confirmation_policy", "no confirmation policy row for chain")
	}
	if err != nil {
		return 0, 0, 0, err
	}
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(number), 0) FROM chain_blocks WHERE chain_id = $1 AND canonical`, chainID).Scan(&head); err != nil {
		return 0, 0, 0, err
	}
	return threshold, policySeq, head, nil
}

// reviseOrphanedReceipts re-verifies canonicality of every non-orphaned
// receipt and marks the ones whose canonical block is gone; the newly verified
// receipt at skipHash is exempt (T035).
func (s *Store) reviseOrphanedReceipts(ctx context.Context, tx pgx.Tx, a *Attempt, skipHash string) (bool, error) {
	rows, err := tx.Query(ctx,
		`SELECT receipt_id, block_number, block_hash FROM tx_receipts
		  WHERE attempt_id = $1 AND canonicality <> 'orphaned'`, a.AttemptID)
	if err != nil {
		return false, err
	}
	type candidate struct {
		id     int64
		number int64
		hash   string
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.id, &c.number, &c.hash); err != nil {
			rows.Close()
			return false, err
		}
		candidates = append(candidates, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, err
	}
	orphaned := false
	for _, c := range candidates {
		if c.hash == skipHash {
			continue
		}
		var canonical bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM chain_blocks WHERE chain_id = $1 AND number = $2 AND hash = $3 AND canonical)`,
			a.ChainID, c.number, c.hash).Scan(&canonical); err != nil {
			return false, err
		}
		if canonical {
			continue
		}
		if _, err := tx.Exec(ctx,
			`UPDATE tx_receipts SET canonicality = 'orphaned', orphaned_at = now(), updated_at = now()
			  WHERE receipt_id = $1 AND canonicality <> 'orphaned'`, c.id); err != nil {
			return false, err
		}
		if err := appendEventTx(ctx, tx, a.AttemptID, EventOrphaned, "", recoveryVersionPtr(a.RecoveryVersion),
			"receipt_id="+itoa(c.id)+" block="+itoa(c.number)+" hash="+c.hash); err != nil {
			return false, err
		}
		revision, state, err := lockAttemptRow(ctx, tx, a.AttemptID)
		if err != nil {
			return false, err
		}
		if state == "effective" || state == "confirmed" || state == "orphaned" {
			if _, err := applyStateTx(ctx, tx, a.AttemptID, revision, []string{state}, "unknown", "orphaned_at = now()"); err != nil {
				if errors.Is(err, ErrRevisionMoved) {
					return false, ErrRevisionMoved
				}
				return false, err
			}
		}
		orphaned = true
	}
	return orphaned, nil
}

// markSiblingsReplaced revises every other non-terminal attempt on the same
// binding to replaced once one attempt is effective/confirmed (T035).
func (s *Store) markSiblingsReplaced(ctx context.Context, tx pgx.Tx, a *Attempt) error {
	rows, err := tx.Query(ctx,
		`SELECT attempt_id FROM tx_attempts
		  WHERE binding_ref = $1 AND attempt_id <> $2
		    AND state NOT IN ('effective', 'confirmed', 'ineffective', 'replaced')`, a.BindingRef, a.AttemptID)
	if err != nil {
		return err
	}
	var siblings []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		siblings = append(siblings, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range siblings {
		revision, state, err := lockAttemptRow(ctx, tx, id)
		if err != nil {
			return err
		}
		if _, err := applyStateTx(ctx, tx, id, revision, []string{state}, "replaced", "replaced_at = now()"); err != nil {
			if errors.Is(err, ErrRevisionMoved) {
				continue
			}
			return err
		}
		if err := appendEventTx(ctx, tx, id, EventReplaced, "", nil, "binding="+a.BindingRef); err != nil {
			return err
		}
	}
	return nil
}
