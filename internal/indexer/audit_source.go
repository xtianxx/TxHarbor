// audit_source.go owns the read-only 013 reconciliation-audit source reader for
// deposit observations (T035). It exists so the audit probes in internal/app
// never read the 004-owned table directly: the SELECT stays in the owning
// package, inside the write-path confinement boundary
// (TestDepositWritePathConfinement), mirroring the T031 snapshot reader.
//
// Strictly read-only: it writes nothing, deletes nothing, emits no event,
// locks nothing across an external call and touches no publish state.
package indexer

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ObservationWatermark is one committed deposit observation transition as the
// reconciliation audit sees it: the stable source identity plus its version.
// Version 0 means the observation insert carries no per-transition version
// watermark; the audit then checks presence only (the created event is
// emission-stream version 1, while status changes emit later object versions).
type ObservationWatermark struct {
	SourceID      string
	SourceVersion int64
}

// LatestObservationWatermarks returns the most recent committed deposit
// observations at or after the migration-seeded cutover, bounded by limit.
// Pre-cutover history legitimately has no emitted event (data-model §7), so it
// is excluded instead of being reported as an audit gap. A nil pool or a
// non-positive limit is a programmer error, never a silent empty result.
func LatestObservationWatermarks(ctx context.Context, pool *pgxpool.Pool, limit int) ([]ObservationWatermark, error) {
	if pool == nil {
		return nil, errors.New("observation watermark: nil pool")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("observation watermark: limit %d must be positive", limit)
	}
	rows, err := pool.Query(ctx, `
SELECT format('%s/%s/%s/%s', chain_id, block_hash, tx_hash, log_index) AS source_id,
       0::bigint AS source_version
FROM deposit_observations
WHERE observed_at >= (SELECT cutover_at FROM event_system_state WHERE id = 1)
ORDER BY observed_at DESC, chain_id DESC, block_number DESC, log_index DESC
LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("query deposit observation watermarks: %w", err)
	}
	defer rows.Close()
	var out []ObservationWatermark
	for rows.Next() {
		var mark ObservationWatermark
		if err := rows.Scan(&mark.SourceID, &mark.SourceVersion); err != nil {
			return nil, fmt.Errorf("scan deposit observation watermark: %w", err)
		}
		out = append(out, mark)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate deposit observation watermarks: %w", err)
	}
	return out, nil
}
