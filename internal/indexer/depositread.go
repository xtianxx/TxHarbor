// depositread.go exposes the bounded, read-only deposit-observation read path
// of 004 for consumers outside this package (014 reconciliation, T034/T035):
// the deposit storage stays owned by internal/indexer (the write-path
// confinement keeps every raw table reference inside this package), so
// external code reads through this explicit, contract-shaped API and never
// embeds the table name or its columns.
//
// Read-only by construction: one bounded SELECT, no transaction, no lock, no
// write, no status transition. Amounts stay in their NUMERIC text form; no
// float ever touches them.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// DepositObservationQuerier is the minimal read-only pgx surface the read
// needs; *pgxpool.Pool and pgx.Tx both satisfy it structurally. Callers MUST
// NOT hand it a transaction held across slow work: the read runs as one
// standalone statement.
type DepositObservationQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// DepositStoredObservation is one stored 004 deposit observation: the
// authoritative receive-side credit fact of one source log. AmountText keeps
// the exact NUMERIC text (callers parse integers; never a float). Status is
// the stored 004 value ('pending' until 005 confirmation owns the transition)
// and is carried verbatim, never concluded here.
type DepositStoredObservation struct {
	BlockNumber int64
	BlockHash   string
	TxHash      string
	LogIndex    int64
	Contract    string
	Sender      string
	Recipient   string
	AmountText  string
	Status      string
	VersionSeq  int64
	ObservedAt  time.Time
}

// MaxDepositObservationTxHashes bounds one deposit-observation lookup so a
// caller can never issue an unbounded IN list. The bound matches the 014
// per-interval candidate budget scale; a larger request is refused, never
// silently truncated.
const MaxDepositObservationTxHashes = 4096

// ReadDepositStoredObservations returns the stored deposit rows of the given
// transaction hashes on one chain, ordered by (block number, block hash, log
// index). An empty hash list returns no rows without a query. Hashes are
// normalized to their stored lowercase form; a malformed hash is refused.
func ReadDepositStoredObservations(ctx context.Context, q DepositObservationQuerier, chainID int64, txHashes []string) ([]DepositStoredObservation, error) {
	if q == nil {
		return nil, errors.New("deposit observation reader requires a querier")
	}
	if chainID <= 0 {
		return nil, fmt.Errorf("deposit observation reader requires a positive chain id, got %d", chainID)
	}
	normalized, err := normalizeDepositTxHashes(txHashes)
	if err != nil {
		return nil, err
	}
	if len(normalized) == 0 {
		return nil, nil
	}
	rows, err := q.Query(ctx, depositObservationsByTxSQL, chainID, normalized)
	if err != nil {
		return nil, fmt.Errorf("read deposit observations: %w", err)
	}
	defer rows.Close()
	var out []DepositStoredObservation
	for rows.Next() {
		var row DepositStoredObservation
		if err := rows.Scan(&row.BlockNumber, &row.BlockHash, &row.TxHash, &row.LogIndex,
			&row.Contract, &row.Sender, &row.Recipient, &row.AmountText, &row.Status,
			&row.VersionSeq, &row.ObservedAt); err != nil {
			return nil, fmt.Errorf("scan deposit observation: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read deposit observations: %w", err)
	}
	return out, nil
}

// normalizeDepositTxHashes lowercases, trims, deduplicates and bounds the
// requested transaction hashes.
func normalizeDepositTxHashes(txHashes []string) ([]string, error) {
	if len(txHashes) > MaxDepositObservationTxHashes {
		return nil, fmt.Errorf("deposit observation lookup for %d transactions exceeds the bound of %d",
			len(txHashes), MaxDepositObservationTxHashes)
	}
	seen := make(map[string]struct{}, len(txHashes))
	out := make([]string, 0, len(txHashes))
	for _, hash := range txHashes {
		trimmed := strings.ToLower(strings.TrimSpace(hash))
		if trimmed == "" {
			return nil, errors.New("deposit observation lookup contains a blank transaction hash")
		}
		if _, dup := seen[trimmed]; dup {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out, nil
}

// depositObservationsByTxSQL is the single read-only statement behind
// ReadDepositStoredObservations. It never writes, locks or advances anything.
const depositObservationsByTxSQL = `
SELECT block_number, block_hash, tx_hash, log_index, contract, sender, recipient,
       amount::text, status, version_seq, observed_at
FROM deposit_observations
WHERE chain_id = $1 AND tx_hash = ANY($2::text[])
ORDER BY block_number, block_hash, log_index`
