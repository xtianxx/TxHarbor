// capacitypause.go owns the capacity hard-boundary pause decision for the
// 003/004 chain-processing streams (T071; PD-2; contracts/capacity.md §3).
//
// Rules (MUST NOT be weakened):
//
//   - On-chain facts are never rejected: a hard boundary alone never stops a
//     stream whose writes still succeed. While persistence is safe the fact
//     continues into the Outbox and pending may exceed hard_limit.
//   - When persistence is not safe (the append/commit failed at the hard
//     boundary), the stream pauses from its durable 003/004 reliable progress
//     and, after capacity recovers, rescans from that point: no observation is
//     skipped and no on-chain fact is refused.
//   - In-flight withdrawals finish under their existing pause/reconciliation
//     protocol; this file never creates a payment intent, never signs, never
//     broadcasts and never reads or changes intent/attempt rows.
//   - The pause decision and the rescan window are observable values (reason,
//     level, reliable progress, resume height) so operations and the drill
//     evidence can record them.
//   - This file does not change any upstream gate: it only decides whether a
//     stream pauses. Downstream recovery is the existing 003/004 checkpoint
//     rescan, which already re-observes every height after the last committed
//     progress.
package indexer

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/events"
)

// Reliable progress sources: the durable 003/004 checkpoints a paused stream
// resumes from.
const (
	// ReliableProgressHeaderSource is the 002 header checkpoint
	// (indexer_checkpoint).
	ReliableProgressHeaderSource = "indexer_header"
	// ReliableProgressLogSource is the 003 log checkpoint (log_checkpoint).
	ReliableProgressLogSource = "log_stream"
	// ReliableProgressDepositSource is the 004 deposit checkpoint
	// (deposit_checkpoint).
	ReliableProgressDepositSource = "deposit_stream"
)

// ReliableProgress is the durable resume point of one 003/004 stream. It is
// read from the real checkpoint rows; a stream without a checkpoint reports
// HasProgress=false and resumes from its configured start height without
// skipping anything.
type ReliableProgress struct {
	ChainID int64
	Source  string
	// LastHeight is the last durably committed height (0 when none).
	LastHeight uint64
	// ResumeHeight is the first height a rescan must process. It is never
	// above the first unprocessed height, so no observation can be skipped.
	ResumeHeight uint64
	// Hash is the checkpoint hash the progress is bound to (the header block
	// hash for the header source, the configuration hash for the log/deposit
	// sources).
	Hash string
	// HasProgress is false when the stream has no durable checkpoint yet.
	HasProgress bool
}

// progressQuerier is the read-only subset satisfied by *pgxpool.Pool and
// pgx.Tx; the progress read never opens its own transaction.
type progressQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Read-only progress statements: one row per chain, no write shape.
const (
	readHeaderProgressSQL = `
SELECT height, block_hash, start_height
FROM indexer_checkpoint
WHERE chain_id = $1`

	readLogProgressSQL = `
SELECT next_block, start_block, config_hash
FROM log_checkpoint
WHERE chain_id = $1`

	readDepositProgressSQL = `
SELECT next_block, start_block, config_hash
FROM deposit_checkpoint
WHERE chain_id = $1`
)

// ReadReliableProgress reads the durable progress of one stream. A missing
// checkpoint is not an error: the stream has simply not committed anything
// yet (HasProgress=false, ResumeHeight=0) and the caller resumes from its
// configured start height.
func ReadReliableProgress(ctx context.Context, q progressQuerier, chainID int64, source string) (ReliableProgress, error) {
	p := ReliableProgress{ChainID: chainID, Source: source}
	switch source {
	case ReliableProgressHeaderSource:
		var height, start uint64
		err := q.QueryRow(ctx, readHeaderProgressSQL, chainID).Scan(&height, &p.Hash, &start)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return p, nil
		case err != nil:
			return p, fmt.Errorf("read %s progress: %w", source, err)
		}
		p.HasProgress = true
		p.LastHeight = height
		p.ResumeHeight = height + 1
	case ReliableProgressLogSource:
		var next, start uint64
		err := q.QueryRow(ctx, readLogProgressSQL, chainID).Scan(&next, &start, &p.Hash)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return p, nil
		case err != nil:
			return p, fmt.Errorf("read %s progress: %w", source, err)
		}
		p.HasProgress = true
		p.ResumeHeight = next
		if next > 0 {
			p.LastHeight = next - 1
		}
	case ReliableProgressDepositSource:
		var next, start uint64
		err := q.QueryRow(ctx, readDepositProgressSQL, chainID).Scan(&next, &start, &p.Hash)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return p, nil
		case err != nil:
			return p, fmt.Errorf("read %s progress: %w", source, err)
		}
		p.HasProgress = true
		p.ResumeHeight = next
		if next > 0 {
			p.LastHeight = next - 1
		}
	default:
		return p, fmt.Errorf("unknown reliable-progress source %q", source)
	}
	return p, nil
}

// CapacityPauseReasonNotPersistable is the only pause cause: the hard boundary
// was reached and the append/commit could not be persisted safely.
const CapacityPauseReasonNotPersistable = "capacity_not_persistable"

// CapacityPauseDecision is the recorded decision of one pause evaluation.
// Pause=false is the normal answer: the stream keeps processing (soft/normal
// backlog, or a hard backlog with safe persistence). Detail is a structured,
// secret-free explanation for logs and drill evidence.
type CapacityPauseDecision struct {
	Pause    bool
	Reason   string
	Level    events.CapacityLevel
	Progress ReliableProgress
	Detail   string
}

// EvaluateCapacityPause decides whether a 003/004 stream must pause. persistErr
// is the error of the append/commit the stream attempted at the observed
// level; nil means the write path is healthy.
//
//   - normal/soft: never pause chain processing; on-chain facts continue.
//   - hard + nil persistErr: never pause; the fact keeps entering the Outbox
//     (pending may exceed hard_limit) and no observation is rejected.
//   - hard + persistErr: pause from the reliable progress; the rescan after
//     recovery starts at ResumeHeight, so nothing is skipped.
func EvaluateCapacityPause(level events.CapacityLevel, persistErr error, progress ReliableProgress) CapacityPauseDecision {
	dec := CapacityPauseDecision{Level: level, Progress: progress}
	if level != events.CapacityHard || persistErr == nil {
		return dec
	}
	dec.Pause = true
	dec.Reason = CapacityPauseReasonNotPersistable
	dec.Detail = fmt.Sprintf(
		"capacity hard boundary reached and persistence failed: %v; pausing from %s reliable progress (last=%d resume=%d) and rescanning after recovery",
		persistErr, progress.Source, progress.LastHeight, progress.ResumeHeight)
	return dec
}

// RescanPlan is the post-recovery rescan window computed from a pause. The
// rescan starts at ResumeFrom and must re-observe every height from there on;
// it may start earlier (ReprocessFromStart is true when no checkpoint exists)
// but never later.
type RescanPlan struct {
	ChainID      int64
	Source       string
	ResumeFrom   uint64
	HasProgress  bool
	ReprocessAll bool
}

// RescanFrom computes the rescan plan of one pause decision.
func RescanFrom(dec CapacityPauseDecision) RescanPlan {
	return RescanPlan{
		ChainID:      dec.Progress.ChainID,
		Source:       dec.Progress.Source,
		ResumeFrom:   dec.Progress.ResumeHeight,
		HasProgress:  dec.Progress.HasProgress,
		ReprocessAll: !dec.Progress.HasProgress,
	}
}

// RescanContinuity compares a rescan plan with the heights a resumed stream
// actually processed: every height in [ResumeFrom, max(processed)] must appear
// exactly once. Missing heights (a skipped observation) and duplicate heights
// (a re-applied fact) are both reported, so drill evidence can assert
// "0 missing, 0 duplicate" instead of trusting the run. With no durable
// checkpoint the window is the caller's full range (ReprocessAll) and
// continuity is asserted against the configured start height by the caller.
func RescanContinuity(plan RescanPlan, processed []uint64) (missing, duplicates []uint64) {
	if !plan.HasProgress {
		return nil, nil
	}
	seen := make(map[uint64]int, len(processed))
	maxHeight := plan.ResumeFrom
	for _, h := range processed {
		seen[h]++
		if h > maxHeight {
			maxHeight = h
		}
	}
	for h := plan.ResumeFrom; h <= maxHeight; h++ {
		switch seen[h] {
		case 0:
			missing = append(missing, h)
		case 1:
		default:
			duplicates = append(duplicates, h)
		}
	}
	return missing, duplicates
}
