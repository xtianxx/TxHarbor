// snapshot.go owns the read-only 013 bootstrap snapshot reader for deposit
// observations (T031; quickstart Q0). It exists so the operator export in
// internal/app never reads the 004-owned table directly: the SELECT stays in
// the owning package, inside the write-path confinement boundary
// (TestDepositWritePathConfinement). The reader is strictly read-only — it
// writes nothing, deletes nothing, emits no event and fabricates no history.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ObservationSnapshot is one deposit observation row projected for the
// bootstrap export. It is historical state only; consumers baseline on
// first-seen event versions, never on this snapshot (data-model §7).
type ObservationSnapshot struct {
	ChainID     int64
	BlockNumber uint64
	BlockHash   string
	TxHash      string
	LogIndex    uint64
	Status      string
	VersionSeq  int64
	Amount      string
	ObservedAt  time.Time
}

// StreamObservationSnapshots streams every deposit observation in a stable
// identity order through yield. A nil pool or a nil yield is a programmer
// error, never a silent empty export.
func StreamObservationSnapshots(ctx context.Context, pool *pgxpool.Pool, yield func(ObservationSnapshot) error) error {
	if pool == nil {
		return errors.New("observation snapshot: nil pool")
	}
	if yield == nil {
		return errors.New("observation snapshot: nil yield")
	}
	rows, err := pool.Query(ctx, `
SELECT chain_id, block_number, block_hash, tx_hash, log_index, status, version_seq, amount::text, observed_at
FROM deposit_observations
ORDER BY chain_id, block_number, block_hash, tx_hash, log_index`)
	if err != nil {
		return fmt.Errorf("query deposit observations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			snapshot    ObservationSnapshot
			blockNumber int64
			logIndex    int64
		)
		if err := rows.Scan(&snapshot.ChainID, &blockNumber, &snapshot.BlockHash, &snapshot.TxHash,
			&logIndex, &snapshot.Status, &snapshot.VersionSeq, &snapshot.Amount, &snapshot.ObservedAt); err != nil {
			return fmt.Errorf("scan deposit observation: %w", err)
		}
		if blockNumber < 0 || logIndex < 0 {
			return fmt.Errorf("deposit observation has negative chain position %d/%d", blockNumber, logIndex)
		}
		snapshot.BlockNumber = uint64(blockNumber)
		snapshot.LogIndex = uint64(logIndex)
		if err := yield(snapshot); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate deposit observations: %w", err)
	}
	return nil
}
