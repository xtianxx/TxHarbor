// reorgcommit.go implements the 006 transaction catalog (T011): the
// durable half of specs/006-reorg-recovery/data-model.md §Transaction catalog
// (R1 atomicity, R6 5-step shape, R7 sweep rule, R9 no-pause-writes), driven
// by the recovery executor in reorg.go (T010, later in this batch).
//
// Every 006 transaction follows the 5-step shape (BEGIN → ensure lease →
// FOR UPDATE → independent post-lock rechecks → writes with RowsAffected
// checks → COMMIT; SET LOCAL statement_timeout='5s' via shared writeGuard).
// Zero RPC inside any transaction: all chain evidence arrives as arguments
// captured off-lock by the executor; the transaction only re-verifies the
// durable side. Coordination (writeGuard/ensureLeaseSQL/lockCoordSQL)
// reuses the shared scanner.go constants — never copied.
//
// Recovery authority (research R9): the reorg_recovery row is the sole
// recovery authority. establish writes ONLY that row (+ event row); it never
// writes indexer_pause / log_pause / deposit_pause. complete_reverify and
// auth_release DELETE only the recovery row (+ terminal event); they never
// touch stream pause rows. Ordinary commit paths stop via the recovery-state
// recheck below (the R1 gate); 006's own transactions gate on the captured
// (recovery_id, recovery_seq) plus an enumerated phase allowlist — there is
// no pause to bypass and no bypass flag any caller can pass.
//
// Version accounting (research R1/R7): the current version is always readable
// in one statement — the active row's seq if a row exists, else the
// events-stream MAX for the chain (0 with no history). Ordinary batches
// capture it BEFORE reading batch inputs; the commit rechecks post-lock (a)
// no active row, (b) captured == current, (c) all existing guards. A mismatch
// refuses the whole batch with zero writes and zero progress, even on full
// content coincidence. Recomputation starts a NEW batch, never re-labels old
// results. Deletion removes the row, never the version: post-release stale
// batches still mismatch against the events MAX.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RecoveryGateError is the 006 recovery-state refusal: an ordinary batch
// whose captured version is stale, or any submit outside its allowed phase.
// The loop treats it like a stale basis (re-capture, new batch) — never a
// pause write, never a silent retry of the same inputs.
type RecoveryGateError struct{ detail string }

func (e *RecoveryGateError) Error() string { return "recovery gate refused: " + e.detail }

// RecoveryOwned marks a 006-owned transaction path: calls carrying it gate
// on the enumerated recovery identity + phase instead of on absence. Only
// the transaction functions in this file can obtain one (callers pass the
// values the executor captured; the database rows under the lock decide).
type RecoveryOwned struct {
	RecoveryID string
}

// RecoveryCapture is the version bound to one batch of inputs. Ordinary
// paths capture before reading inputs (loop level) and commit exactly once;
// 006-owned paths additionally carry the recovery identity the inputs were
// derived under.
type RecoveryCapture struct {
	Seq   int64
	Owned *RecoveryOwned // nil = ordinary path
}

// RecoveryRow is the durable active-recovery instance (data-model Table 1).
type RecoveryRow struct {
	RecoveryID      string
	Phase           string
	PolicySeq       int64
	MaxDepth        int64
	BoundOldNumber  int64
	BoundOldHash    string
	AncestorNumber  *int64
	AncestorHash    *string
	NewTipNumber    *int64
	NewTipHash      *string
	BlockFrontier   *int64
	LogFrontier     *int64
	DepositFrontier *int64
	Seq             int64
}

// Recovery phases (data-model §Phases). Terminal release = row DELETE;
// reconcile_required persists as the manual-hold authority until auth_release.
const (
	reorgPhaseDetected          = "detected"
	reorgPhaseAncestorConfirmed = "ancestor_confirmed"
	reorgPhaseInvalidated       = "invalidated"
	reorgPhaseReplaying         = "replaying"
	reorgPhaseCompletePending   = "complete_pending"
	reorgPhaseReconcileRequired = "reconcile_required"
)

// maxRecoverySeq is the last usable fencing version (MaxInt64-1 per Table 1;
// MaxInt64 itself never persists — the column CHECK and the establish assert
// both forbid it).
const maxRecoverySeq = int64(1<<63 - 1)

// captureRecoveryVersion reads the current version in one statement (R1
// authoritative): the active row's seq if present, else the events-stream MAX
// for the chain, else 0. active reports whether a recovery row exists (any
// phase — every persisted phase blocks ordinary batches).
func captureRecoveryVersion(ctx context.Context, q depositQuerier, chainID int64) (cap RecoveryCapture, active bool, err error) {
	var (
		activeSeq *int64
		eventsMax int64
	)
	if err := q.QueryRow(ctx, readRecoveryGateSQL, chainID).Scan(&activeSeq, &eventsMax); err != nil {
		// The gate query structurally yields exactly one row (scalar
		// subquery + COALESCE aggregate), so ErrNoRows is impossible from
		// PostgreSQL — only scripted unit fakes produce it, and for them
		// empty is the truthful answer. Every REAL error (including a
		// pre-000006 schema without these tables) stays an error: the gate
		// fails closed, never open.
		if errors.Is(err, pgx.ErrNoRows) {
			return RecoveryCapture{}, false, nil
		}
		return RecoveryCapture{}, false, fmt.Errorf("capture recovery version: %w", err)
	}
	if activeSeq != nil {
		return RecoveryCapture{Seq: *activeSeq}, true, nil
	}
	return RecoveryCapture{Seq: eventsMax}, false, nil
}

// readRecoveryRow loads the active instance for owned-path rechecks and
// validity-annotated readers (nil row = no active recovery).
func readRecoveryRow(ctx context.Context, q depositQuerier, chainID int64) (*RecoveryRow, error) {
	var r RecoveryRow
	var ancestorNumber, newTipNumber, blockFrontier, logFrontier, depositFrontier *int64
	var ancestorHash, newTipHash *string
	err := q.QueryRow(ctx, readRecoveryRowSQL, chainID).Scan(
		&r.RecoveryID, &r.Phase, &r.PolicySeq, &r.MaxDepth,
		&r.BoundOldNumber, &r.BoundOldHash,
		&ancestorNumber, &ancestorHash, &newTipNumber, &newTipHash,
		&blockFrontier, &logFrontier, &depositFrontier, &r.Seq)
	switch {
	case err == nil:
		r.AncestorNumber, r.AncestorHash = ancestorNumber, ancestorHash
		r.NewTipNumber, r.NewTipHash = newTipNumber, newTipHash
		r.BlockFrontier, r.LogFrontier, r.DepositFrontier = blockFrontier, logFrontier, depositFrontier
		return &r, nil
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	default:
		return nil, fmt.Errorf("read recovery row: %w", err)
	}
}

// recheckRecoveryGate enforces the version gate post-lock (R1/R7):
//   - ordinary (Owned == nil): refuse when any recovery row exists; refuse
//     when captured != current (active seq else events MAX else 0).
//   - 006-owned: the row must exist with the captured identity + seq and a
//     phase in the allowlist; anything else refuses.
//
// Refusal carries zero writes by construction (callers return before writing).
func recheckRecoveryGate(ctx context.Context, tx pgx.Tx, chainID int64, cap RecoveryCapture, allowedPhases ...string) (*RecoveryRow, error) {
	var (
		activeSeq *int64
		eventsMax int64
	)
	if err := tx.QueryRow(ctx, readRecoveryGateSQL, chainID).Scan(&activeSeq, &eventsMax); err != nil {
		return nil, fmt.Errorf("recovery gate read: %w", err)
	}
	if cap.Owned == nil {
		if activeSeq != nil {
			return nil, &RecoveryGateError{detail: fmt.Sprintf(
				"active recovery row present (seq=%d); ordinary batch refused", *activeSeq)}
		}
		if eventsMax != cap.Seq {
			return nil, &RecoveryGateError{detail: fmt.Sprintf(
				"captured version %d differs from current %d; ordinary batch refused without consulting content",
				cap.Seq, eventsMax)}
		}
		return nil, nil
	}
	if activeSeq == nil {
		return nil, &RecoveryGateError{detail: "006-owned path requires the active recovery row"}
	}
	row, err := readRecoveryRowTx(ctx, tx, chainID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, &RecoveryGateError{detail: "006-owned path requires the active recovery row"}
	}
	if row.RecoveryID != cap.Owned.RecoveryID || row.Seq != cap.Seq {
		return nil, &RecoveryGateError{detail: fmt.Sprintf(
			"captured recovery (%s seq=%d) differs from active (%s seq=%d)",
			cap.Owned.RecoveryID, cap.Seq, row.RecoveryID, row.Seq)}
	}
	for _, p := range allowedPhases {
		if row.Phase == p {
			return row, nil
		}
	}
	return nil, &RecoveryGateError{detail: fmt.Sprintf(
		"recovery %s phase %q not in [%s]", row.RecoveryID, row.Phase, strings.Join(allowedPhases, ","))}
}

// readRecoveryRowTx is readRecoveryRow over a pgx.Tx (same statement).
func readRecoveryRowTx(ctx context.Context, tx pgx.Tx, chainID int64) (*RecoveryRow, error) {
	return readRecoveryRow(ctx, txAdapter{tx: tx}, chainID)
}

// lockRecoveryChain runs the shared lease framing every 006 transaction
// opens with: writeGuard → ensure row → FOR UPDATE lock. With a nil lease the
// owner/token verdict is skipped (privileged DB-operator path, same shape as
// confirmauth.go); otherwise the verdict binds the executor's ownership.
func lockRecoveryChain(ctx context.Context, tx pgx.Tx, chainID int64, lease *Lease) error {
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return fmt.Errorf("recovery transaction statement guard: %w", err)
	}
	owner, token, ttl := "reorg-operator", int64(0), float64(3600)
	if lease != nil {
		owner, token, ttl = lease.ownerID, lease.Token(), lease.ttl.Seconds()
	}
	if _, err := tx.Exec(ctx, ensureLeaseSQL, chainID, owner, token, ttl); err != nil {
		return fmt.Errorf("ensure recovery coordination row: %w", err)
	}
	var (
		ownerOut string
		tokenOut int64
		valid    bool
	)
	if err := tx.QueryRow(ctx, lockCoordSQL, chainID).Scan(&ownerOut, &tokenOut, &valid); err != nil {
		return fmt.Errorf("lock recovery coordination row: %w", err)
	}
	if lease != nil {
		var one int
		err := tx.QueryRow(ctx, leaseVerdictSQL, chainID, lease.ownerID, lease.Token()).Scan(&one)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: owner/fencing/expiry verdict failed", ErrLeaseLost)
		}
		if err != nil {
			return fmt.Errorf("recovery lease verdict: %w", err)
		}
	}
	return nil
}

// nextEventSeq allocates the next per-instance event order (append-only; the
// UNIQUE on (chain_id, recovery_id, event_seq) makes repeat execution
// converge instead of duplicating lifecycle state).
func nextEventSeq(ctx context.Context, tx pgx.Tx, chainID int64, recoveryID string) (int64, error) {
	var n int64
	if err := tx.QueryRow(ctx, nextRecoveryEventSeqSQL, chainID, recoveryID).Scan(&n); err != nil {
		return 0, fmt.Errorf("allocate recovery event order: %w", err)
	}
	return n, nil
}

// appendRecoveryEvent writes one audit row (heights, hashes, ranges, versions;
// never secrets, never raw RPC dumps — same redaction boundary as 002/003).
func appendRecoveryEvent(ctx context.Context, tx pgx.Tx, chainID int64, recoveryID string, seq int64, event, detail string) error {
	n, err := nextEventSeq(ctx, tx, chainID, recoveryID)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, insertRecoveryEventSQL, chainID, recoveryID, seq, n, event, detail)
	if err != nil {
		return fmt.Errorf("insert recovery event %s: %w", event, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("insert recovery event %s affected %d rows, want 1", event, tag.RowsAffected())
	}
	return nil
}

// EstablishRequest is one fork-evidence intent. All chain evidence was read
// off-lock by the executor; the transaction re-verifies only durable state.
type EstablishRequest struct {
	ChainID        int64
	OldTipNumber   int64
	OldTipHash     string
	NewTipNumber   int64
	NewTipHash     string
	DetectedHeight int64
	EnvMaxDepthRaw string
}

// EstablishResult reports the bound instance. Converged means a repeat
// trigger joined the one persistent instance (no new row, no phase move).
type EstablishResult struct {
	RecoveryID string
	Seq        int64
	Converged  bool
}

// EstablishRecovery atomically writes the recovery row (phase=detected,
// bound tip, policy bind, seq=events-MAX+1 asserted < MaxInt64) plus the
// established event in ONE transaction — rollback burns no seq, binds no id.
// It writes NO pause table (R9); pre-existing stream pauses are read as the
// recorded precondition and left intact. A concurrent second establish
// converges on the existing row (INSERT conflict → re-read → join).
func EstablishRecovery(ctx context.Context, pool *pgxpool.Pool, lease *Lease, req EstablishRequest) (EstablishResult, error) {
	if pool == nil {
		return EstablishResult{}, errors.New("establish recovery: nil pool")
	}
	if req.ChainID <= 0 {
		return EstablishResult{}, fmt.Errorf("establish recovery: %w: chain id %d must be > 0", ErrReorgPolicyRejected, req.ChainID)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return EstablishResult{}, fmt.Errorf("begin establish transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after COMMIT
	if err := lockRecoveryChain(ctx, tx, req.ChainID, lease); err != nil {
		return EstablishResult{}, err
	}

	// Repeat triggers converge: one persistent instance per chain.
	if row, err := readRecoveryRowTx(ctx, tx, req.ChainID); err != nil {
		return EstablishResult{}, err
	} else if row != nil {
		return EstablishResult{RecoveryID: row.RecoveryID, Seq: row.Seq, Converged: true}, nil
	}

	// Policy bind (env max_depth == effective row, else refuse; first round
	// bootstraps seq 1). In-flight authority is this bound seq from here on.
	policySeq, err := bindReorgPolicyTx(ctx, tx, req.ChainID, req.EnvMaxDepthRaw)
	if err != nil {
		return EstablishResult{}, err
	}
	var maxDepth int64
	if err := tx.QueryRow(ctx, readReorgPolicyDepthSQL, req.ChainID, policySeq).Scan(&maxDepth); err != nil {
		return EstablishResult{}, fmt.Errorf("read bound reorg policy: %w", err)
	}

	// Fencing version: monotonic across rounds and releases (events survive
	// row DELETEs, so deletes can never resurrect old versions). Exhaustion
	// refuses with zero writes — never wrap, never reset, never reuse.
	var eventsMax int64
	if err := tx.QueryRow(ctx, maxRecoverySeqSQL, req.ChainID).Scan(&eventsMax); err != nil {
		return EstablishResult{}, fmt.Errorf("read recovery event versions: %w", err)
	}
	seq := eventsMax + 1
	if seq <= 0 || seq >= 1<<63-1 {
		return EstablishResult{}, fmt.Errorf("establish recovery refused: fencing versions exhausted (events MAX=%d)", eventsMax)
	}

	// Pre-existing stream pauses are the recorded precondition, never
	// modified: read each for the established-event detail.
	prePauses := readStreamPausesTx(ctx, tx, req.ChainID)

	recoveryID := fmt.Sprintf("reorg-%d-%d-%d", req.ChainID, req.OldTipNumber, nowUnixNano())
	tag, err := tx.Exec(ctx, insertRecoveryRowSQL,
		req.ChainID, recoveryID, reorgPhaseDetected, policySeq, maxDepth,
		req.OldTipNumber, req.OldTipHash, req.NewTipNumber, req.NewTipHash, seq)
	if err != nil {
		if isUniqueViolation(err) {
			// A concurrent establish won: converge on its row.
			_ = tx.Rollback(ctx)
			return convergeEstablish(ctx, pool, req.ChainID)
		}
		return EstablishResult{}, fmt.Errorf("insert recovery row: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return EstablishResult{}, fmt.Errorf("insert recovery row affected %d rows, want 1", tag.RowsAffected())
	}
	detail := fmt.Sprintf("recovery=%s phase=detected old_tip=%d:%s new_tip=%d:%s detected_at=%d policy=%d version=%d pre_pauses=%s",
		recoveryID, req.OldTipNumber, req.OldTipHash, req.NewTipNumber, req.NewTipHash,
		req.DetectedHeight, policySeq, seq, prePauses)
	if err := appendRecoveryEvent(ctx, tx, req.ChainID, recoveryID, seq, "established", detail); err != nil {
		return EstablishResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		// Unknown COMMIT: the row is the authority — a visible row with this
		// id committed; anything else converges or retries (never assume).
		if row, rerr := readRecoveryRowByID(ctx, pool, req.ChainID, recoveryID); rerr == nil && row != nil {
			return EstablishResult{RecoveryID: recoveryID, Seq: seq}, nil
		}
		_ = tx.Rollback(ctx)
		return convergeEstablish(ctx, pool, req.ChainID)
	}
	return EstablishResult{RecoveryID: recoveryID, Seq: seq}, nil
}

// convergeEstablish joins the one persistent instance after losing the
// establish race (or an uncertain COMMIT): exactly one active recovery.
func convergeEstablish(ctx context.Context, pool *pgxpool.Pool, chainID int64) (EstablishResult, error) {
	row, err := readRecoveryRow(ctx, pool, chainID)
	if err != nil {
		return EstablishResult{}, err
	}
	if row == nil {
		return EstablishResult{}, fmt.Errorf("establish conflict but no recovery row is visible")
	}
	return EstablishResult{RecoveryID: row.RecoveryID, Seq: row.Seq, Converged: true}, nil
}

// readStreamPausesTx snapshots the three stream-pause causes for evidence
// detail (read-only; 006 never writes or deletes these rows).
func readStreamPausesTx(ctx context.Context, tx pgx.Tx, chainID int64) string {
	causes := []string{}
	var one int
	if err := tx.QueryRow(ctx, pauseExistsSQL, chainID).Scan(&one); err == nil {
		causes = append(causes, "indexer_pause")
	}
	if err := tx.QueryRow(ctx, logPauseExistsSQL, chainID).Scan(&one); err == nil {
		causes = append(causes, "log_pause")
	}
	if err := tx.QueryRow(ctx, depositPauseExistsSQL, chainID).Scan(&one); err == nil {
		causes = append(causes, "deposit_pause")
	}
	if len(causes) == 0 {
		return "none"
	}
	return strings.Join(causes, ",")
}

// ConfirmRecoveryAncestor pins the read-only search result: the ancestor must
// still be the local canonical row at its height, and the recomputed depth
// (bound_old_tip − ancestor, exact) must satisfy the closed bound. Chain-side
// equality and suffix continuity were proven off-lock by the search (T010);
// the evidence refs travel in the event detail, never as new RPC here.
func ConfirmRecoveryAncestor(ctx context.Context, pool *pgxpool.Pool, lease *Lease, chainID int64, cap RecoveryCapture, ancestorNumber int64, ancestorHash, evidence string) error {
	if pool == nil {
		return errors.New("confirm ancestor: nil pool")
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin confirm-ancestor transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockRecoveryChain(ctx, tx, chainID, lease); err != nil {
		return err
	}
	row, err := recheckRecoveryGate(ctx, tx, chainID, cap, reorgPhaseDetected)
	if err != nil {
		return err
	}
	var stored string
	err = tx.QueryRow(ctx, canonicalHashAtHeightSQL, chainID, ancestorNumber).Scan(&stored)
	if errors.Is(err, pgx.ErrNoRows) {
		return &RecoveryGateError{detail: fmt.Sprintf("ancestor %d has no canonical row under the lock", ancestorNumber)}
	}
	if err != nil {
		return fmt.Errorf("re-read ancestor canonical: %w", err)
	}
	if stored != ancestorHash {
		return &RecoveryGateError{detail: fmt.Sprintf(
			"ancestor %d canonical %s differs from searched %s; evidence must be re-walked", ancestorNumber, stored, ancestorHash)}
	}
	depth, err := reorgDepth(row.BoundOldNumber, ancestorNumber)
	if err != nil {
		return &RecoveryGateError{detail: fmt.Sprintf("ancestor depth: %v", err)}
	}
	if depth > row.MaxDepth {
		return &RecoveryGateError{detail: fmt.Sprintf(
			"ancestor depth %d exceeds bound %d; reconcile path owns this fork", depth, row.MaxDepth)}
	}
	tag, err := tx.Exec(ctx, confirmRecoveryAncestorSQL, chainID, row.RecoveryID, row.Seq, ancestorNumber, ancestorHash)
	if err != nil {
		return fmt.Errorf("confirm recovery ancestor: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return &RecoveryGateError{detail: "ancestor confirm affected 0 rows (concurrent phase move)"}
	}
	detail := fmt.Sprintf("recovery=%s phase=ancestor_confirmed ancestor=%d:%s depth=%d bound=%d policy=%d version=%d evidence=%s",
		row.RecoveryID, ancestorNumber, ancestorHash, depth, row.MaxDepth, row.PolicySeq, row.Seq, evidence)
	if err := appendRecoveryEvent(ctx, tx, chainID, row.RecoveryID, row.Seq, "ancestor_confirmed", detail); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// InvalidateRecoveryBlocks flips the old fork non-canonical over the LIVE
// swept range [ancestor+1, max(persisted_tip_at_sweep)] (never the
// establish-cached tip — R7 coverage rule). Old rows are retained as history;
// the partial UNIQUE keeps at most one canonical per height throughout.
func InvalidateRecoveryBlocks(ctx context.Context, pool *pgxpool.Pool, lease *Lease, chainID int64, cap RecoveryCapture) (sweptFrom, sweptTo, flipped int64, err error) {
	if pool == nil {
		return 0, 0, 0, errors.New("invalidate blocks: nil pool")
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, 0, 0, fmt.Errorf("begin invalidate-blocks transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockRecoveryChain(ctx, tx, chainID, lease); err != nil {
		return 0, 0, 0, err
	}
	row, err := recheckRecoveryGate(ctx, tx, chainID, cap, reorgPhaseAncestorConfirmed, reorgPhaseInvalidated)
	if err != nil {
		return 0, 0, 0, err
	}
	var sweepTip int64
	if err := tx.QueryRow(ctx, maxPersistedTipSQL, chainID).Scan(&sweepTip); err != nil {
		return 0, 0, 0, fmt.Errorf("read live sweep tip: %w", err)
	}
	from := *row.AncestorNumber + 1
	if sweepTip < from {
		sweepTip = from - 1 // empty sweep above the ancestor: nothing to flip
	}
	var n int64
	if sweepTip >= from {
		tag, err := tx.Exec(ctx, invalidateBlocksSQL, chainID, from, sweepTip)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("invalidate blocks: %w", err)
		}
		n = tag.RowsAffected()
	}
	tag, err := tx.Exec(ctx, advanceRecoveryPhaseSQL, chainID, row.RecoveryID, row.Seq, reorgPhaseInvalidated)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("advance recovery phase: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return 0, 0, 0, &RecoveryGateError{detail: "phase advance affected 0 rows (concurrent move)"}
	}
	detail := fmt.Sprintf("recovery=%s range=%d-%d flipped=%d version=%d", row.RecoveryID, from, sweepTip, n, row.Seq)
	if err := appendRecoveryEvent(ctx, tx, chainID, row.RecoveryID, row.Seq, "blocks_invalidated", detail); err != nil {
		return 0, 0, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, 0, fmt.Errorf("commit invalidate blocks: %w", err)
	}
	return from, sweepTip, n, nil
}

// InvalidateRecoveryObservations converts every affected Pending/Confirmed
// observation in the swept number range to Orphaned (ancestor-side rows
// untouched), retaining confirm basis columns as history evidence and
// appending one transition-log row per conversion (repeat execution conflicts
// on the log UNIQUE → already recorded, zero new state).
func InvalidateRecoveryObservations(ctx context.Context, pool *pgxpool.Pool, lease *Lease, chainID int64, cap RecoveryCapture) (orphaned int64, err error) {
	if pool == nil {
		return 0, errors.New("invalidate observations: nil pool")
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin invalidate-observations transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockRecoveryChain(ctx, tx, chainID, lease); err != nil {
		return 0, err
	}
	row, err := recheckRecoveryGate(ctx, tx, chainID, cap, reorgPhaseInvalidated)
	if err != nil {
		return 0, err
	}
	from := *row.AncestorNumber + 1
	tag, err := tx.Exec(ctx, insertOrphanTransitionsSQL, chainID, from, row.RecoveryID)
	if err != nil {
		return 0, fmt.Errorf("insert orphan transitions: %w", err)
	}
	transitions := tag.RowsAffected()
	tag, err = tx.Exec(ctx, orphanObservationsSQL, chainID, from, row.RecoveryID)
	if err != nil {
		return 0, fmt.Errorf("invalidate observations: %w", err)
	}
	n := tag.RowsAffected()
	if n != transitions {
		return 0, fmt.Errorf("orphan conversion mismatch: %d transitions for %d conversions", transitions, n)
	}
	detail := fmt.Sprintf("recovery=%s range_from=%d orphaned=%d version=%d", row.RecoveryID, from, n, row.Seq)
	if err := appendRecoveryEvent(ctx, tx, chainID, row.RecoveryID, row.Seq, "observations_invalidated", detail); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit invalidate observations: %w", err)
	}
	return n, nil
}

// RollbackRecoveryCheckpoint moves one stream checkpoint to its guarded floor
// $to = max(ancestor+1, start) with an exact current-position match (a
// concurrent advance fails the guard → stale, never a jump). Floors preserve
// every existing CHECK without DDL; empty-log ranges stay legitimately empty.
//
// Block checkpoints carry (height, hash) under a composite FK onto
// chain_blocks, so the rollback must land on a persisted canonical row:
// normally (ancestor, ancestorHash). When the floor is the scan start ahead
// of the ancestor (pre-start history stays untouched, US3-2), no canonical
// row exists at floor-1 and the row is DELETED with the exact guard instead
// (first-unit semantics; replay re-inserts it with start_height=floor).
// A missing checkpoint row is a vacuous no-op success (a stream with no
// progress has nothing swept and nothing to replay; its ordinary bootstrap
// owns it post-release).
func RollbackRecoveryCheckpoint(ctx context.Context, pool *pgxpool.Pool, lease *Lease, chainID int64, cap RecoveryCapture, stream RecoveryStream) (from, to int64, err error) {
	if pool == nil {
		return 0, 0, errors.New("rollback checkpoint: nil pool")
	}
	var updateSQL string
	switch stream {
	case RecoveryStreamBlock:
		updateSQL = rollbackBlockCheckpointSQL
	case RecoveryStreamLog:
		updateSQL = rollbackLogCheckpointSQL
	case RecoveryStreamDeposit:
		updateSQL = rollbackDepositCheckpointSQL
	default:
		return 0, 0, fmt.Errorf("rollback checkpoint: unknown stream %q", stream)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, 0, fmt.Errorf("begin rollback transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockRecoveryChain(ctx, tx, chainID, lease); err != nil {
		return 0, 0, err
	}
	row, err := recheckRecoveryGate(ctx, tx, chainID, cap, reorgPhaseInvalidated)
	if err != nil {
		return 0, 0, err
	}
	if row.AncestorNumber == nil {
		return 0, 0, &RecoveryGateError{detail: "rollback requires a confirmed ancestor"}
	}
	ancestor := *row.AncestorNumber
	if stream == RecoveryStreamBlock {
		return rollbackBlockCheckpointTx(ctx, tx, chainID, row, ancestor)
	}
	var cur, start int64
	var configHash string
	if err := tx.QueryRow(ctx, readStreamCheckpointSQL(stream), chainID).Scan(&cur, &start, &configHash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ancestor + 1, ancestor + 1, nil // vacuous: no progress, nothing swept
		}
		return 0, 0, fmt.Errorf("read %s checkpoint: %w", stream, err)
	}
	floor := ancestor + 1
	if start > floor {
		floor = start
	}
	tag, err := tx.Exec(ctx, updateSQL, append([]any{chainID, floor}, streamGuardArgs(start, configHash, cur)...)...)
	if err != nil {
		return 0, 0, fmt.Errorf("rollback %s checkpoint: %w", stream, err)
	}
	if tag.RowsAffected() != 1 {
		return 0, 0, &RecoveryGateError{detail: fmt.Sprintf(
			"%s checkpoint moved under the lock (position fault)", stream)}
	}
	detail := fmt.Sprintf("recovery=%s stream=%s from=%d to=%d version=%d", row.RecoveryID, stream, cur, floor, row.Seq)
	if err := appendRecoveryEvent(ctx, tx, chainID, row.RecoveryID, row.Seq, "checkpoints_rolled_back", detail); err != nil {
		return 0, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("commit rollback %s checkpoint: %w", stream, err)
	}
	return cur, floor, nil
}

// streamGuardArgs orders the exact-guard arguments for the log/deposit
// rollback statements (start, config, current).
func streamGuardArgs(start int64, configHash string, cur int64) []any {
	return []any{start, configHash, cur}
}

// rollbackBlockCheckpointTx rolls the header checkpoint back under the FK:
// normally onto (ancestor, ancestorHash); when the floor is the scan start
// ahead of the ancestor, the row is deleted with the exact guard instead.
func rollbackBlockCheckpointTx(ctx context.Context, tx pgx.Tx, chainID int64, row *RecoveryRow, ancestor int64) (int64, int64, error) {
	var curHeight, startHeight int64
	var curHash string
	if err := tx.QueryRow(ctx, readBlockCheckpointTxSQL, chainID).Scan(&curHeight, &curHash, &startHeight); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ancestor + 1, ancestor + 1, nil // vacuous: no progress, nothing swept
		}
		return 0, 0, fmt.Errorf("read block checkpoint: %w", err)
	}
	floor := ancestor + 1
	if startHeight > floor {
		floor = startHeight
	}
	if floor == ancestor+1 {
		var canonHash string
		if err := tx.QueryRow(ctx, canonicalHashAtHeightSQL, chainID, ancestor).Scan(&canonHash); err != nil {
			return 0, 0, fmt.Errorf("read ancestor canonical for rollback: %w", err)
		}
		tag, err := tx.Exec(ctx, rollbackBlockCheckpointSQL, chainID, ancestor, canonHash, curHeight, curHash)
		if err != nil {
			return 0, 0, fmt.Errorf("rollback block checkpoint: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return 0, 0, &RecoveryGateError{detail: "block checkpoint moved under the lock (position fault)"}
		}
		detail := fmt.Sprintf("recovery=%s stream=block from=%d to=%d version=%d", row.RecoveryID, curHeight, ancestor, row.Seq)
		if err := appendRecoveryEvent(ctx, tx, chainID, row.RecoveryID, row.Seq, "checkpoints_rolled_back", detail); err != nil {
			return 0, 0, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, 0, fmt.Errorf("commit rollback block checkpoint: %w", err)
		}
		return curHeight, ancestor, nil
	}
	// Pre-start floor: nothing at floor-1 is canonical by definition, so the
	// row goes away with the exact guard (replay re-inserts it).
	tag, err := tx.Exec(ctx, deleteBlockCheckpointSQL, chainID, curHeight, curHash)
	if err != nil {
		return 0, 0, fmt.Errorf("delete block checkpoint for pre-start floor: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return 0, 0, &RecoveryGateError{detail: "block checkpoint moved under the lock (position fault)"}
	}
	detail := fmt.Sprintf("recovery=%s stream=block from=%d to=deleted floor=%d version=%d", row.RecoveryID, curHeight, floor, row.Seq)
	if err := appendRecoveryEvent(ctx, tx, chainID, row.RecoveryID, row.Seq, "checkpoints_rolled_back", detail); err != nil {
		return 0, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("commit rollback block checkpoint: %w", err)
	}
	return curHeight, floor, nil
}

// RecoveryStream names one replay stream (frontier column + checkpoint).
type RecoveryStream string

const (
	RecoveryStreamBlock   RecoveryStream = "block"
	RecoveryStreamLog     RecoveryStream = "log"
	RecoveryStreamDeposit RecoveryStream = "deposit"
)

// ReplayBlock is one new-chain header the executor read off-lock.
type ReplayBlock struct {
	Number     int64
	Hash       string
	ParentHash string
}

// ReplayLog is one new-chain log row the executor read off-lock.
type ReplayLog struct {
	BlockNumber int64
	BlockHash   string
	TxHash      string
	LogIndex    int64
	Contract    string
	Topic0      string
	Topic1      string
	Topic2      string
	Data        string
}

// ReplayObservation is one re-identified deposit under historical semantics
// (version_seq is the version in effect when the source was indexed —
// inherited, never current-config).
type ReplayObservation struct {
	BlockHash   string
	TxHash      string
	LogIndex    int64
	BlockNumber int64
	Contract    string
	Sender      string
	Recipient   string
	Amount      string
	VersionSeq  int64
}

// ReplayRecoveryRange replays one stream range [from,to] idempotently: new
// rows insert with ON CONFLICT DO NOTHING (new identities only — retained
// old-fork rows are never touched), the batch proves coverage (every height
// in range ends with exactly one canonical row), then the stream frontier
// AND the stream checkpoint advance in the same transaction. A covered range
// re-run is a no-op nil; a gap (frontier ahead of from-1... behind) refuses
// as a position fault. Empty data slices are legitimate empty intervals:
// coverage still proves, the frontier still advances.
func ReplayRecoveryRange(ctx context.Context, pool *pgxpool.Pool, lease *Lease, chainID int64, cap RecoveryCapture, stream RecoveryStream, from, to int64, blocks []ReplayBlock, logs []ReplayLog, observations []ReplayObservation) error {
	if pool == nil {
		return errors.New("replay range: nil pool")
	}
	if from > to {
		return fmt.Errorf("replay range: empty height range [%d,%d]", from, to)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin replay transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockRecoveryChain(ctx, tx, chainID, lease); err != nil {
		return err
	}
	row, err := recheckRecoveryGate(ctx, tx, chainID, cap, reorgPhaseInvalidated, reorgPhaseReplaying)
	if err != nil {
		return err
	}
	frontier := streamFrontier(row, stream)
	if frontier == nil {
		// First range of this stream: position is enforced independently by
		// the checkpoint guard below.
	} else {
		if *frontier >= to {
			return nil // already covered: idempotent no-op
		}
		if *frontier != from-1 {
			return &RecoveryGateError{detail: fmt.Sprintf(
				"%s replay frontier %d does not meet range start %d", stream, *frontier, from)}
		}
	}

	var inserted int64
	switch stream {
	case RecoveryStreamBlock:
		for _, b := range blocks {
			if b.Number < from || b.Number > to {
				return &RecoveryGateError{detail: fmt.Sprintf("replay block %d outside range [%d,%d]", b.Number, from, to)}
			}
			tag, err := tx.Exec(ctx, replayBlockSQL, chainID, b.Number, b.Hash, b.ParentHash)
			if err != nil {
				return fmt.Errorf("replay block %d: %w", b.Number, err)
			}
			inserted += tag.RowsAffected()
		}
	case RecoveryStreamLog:
		for _, l := range logs {
			if l.BlockNumber < from || l.BlockNumber > to {
				return &RecoveryGateError{detail: fmt.Sprintf("replay log %d outside range [%d,%d]", l.BlockNumber, from, to)}
			}
			tag, err := tx.Exec(ctx, insertLogSQL,
				chainID, l.BlockNumber, l.BlockHash, l.TxHash, l.LogIndex,
				l.Contract, l.Topic0, l.Topic1, l.Topic2, l.Data)
			if err != nil {
				return fmt.Errorf("replay log %s/%s/%d: %w", l.BlockHash, l.TxHash, l.LogIndex, err)
			}
			inserted += tag.RowsAffected()
		}
	case RecoveryStreamDeposit:
		for _, o := range observations {
			if o.BlockNumber < from || o.BlockNumber > to {
				return &RecoveryGateError{detail: fmt.Sprintf("replay observation %d outside range [%d,%d]", o.BlockNumber, from, to)}
			}
			amount, err := depositNumericAmount(o.Amount)
			if err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, insertDepositObservationSQL,
				chainID, o.BlockHash, o.TxHash, o.LogIndex, o.BlockNumber,
				o.Contract, o.Sender, o.Recipient, amount, o.VersionSeq)
			if err != nil {
				return fmt.Errorf("replay observation %s/%s/%d: %w", o.BlockHash, o.TxHash, o.LogIndex, err)
			}
			inserted += tag.RowsAffected()
		}
	default:
		return fmt.Errorf("replay range: unknown stream %q", stream)
	}

	// Coverage/canon proof over the batch: every replayed height ends with
	// exactly one canonical row (new identities effective, old fork retained
	// non-canonical). A same-hash-above-ancestor height belongs to the
	// recanonicalize path, never here — the proof fails fast on misrouting.
	var canon int64
	if err := tx.QueryRow(ctx, countCanonicalRangeSQL, chainID, from, to).Scan(&canon); err != nil {
		return fmt.Errorf("replay coverage proof: %w", err)
	}
	if canon != to-from+1 {
		return &RecoveryGateError{detail: fmt.Sprintf(
			"replay range [%d,%d] proves %d canonical heights, want %d", from, to, canon, to-from+1)}
	}
	// Parent-linkage proof over the batch: every canonical row in range must
	// link to the canonical row below (from >= 1 always: ancestor and start
	// are both >= 0, so the h = 0 genesis boundary never enters a range).
	var dangling int64
	if err := tx.QueryRow(ctx, checkParentLinkageSQL, chainID, from, to).Scan(&dangling); err != nil {
		return fmt.Errorf("replay linkage proof: %w", err)
	}
	if dangling != 0 {
		return &RecoveryGateError{detail: fmt.Sprintf(
			"replay range [%d,%d] has %d canonical rows with broken parent linkage", from, to, dangling)}
	}

	if err := advanceReplayProgressTx(ctx, tx, chainID, row, stream, from, to); err != nil {
		return err
	}
	if row.Phase == reorgPhaseInvalidated {
		tag, err := tx.Exec(ctx, advanceRecoveryPhaseSQL, chainID, row.RecoveryID, row.Seq, reorgPhaseReplaying)
		if err != nil {
			return fmt.Errorf("advance to replaying: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return &RecoveryGateError{detail: "replaying advance affected 0 rows (concurrent move)"}
		}
	}
	detail := fmt.Sprintf("recovery=%s stream=%s range=%d-%d inserted=%d version=%d",
		row.RecoveryID, stream, from, to, inserted, row.Seq)
	if err := appendRecoveryEvent(ctx, tx, chainID, row.RecoveryID, row.Seq, "replay_progress", detail); err != nil {
		return err
	}
	// Completion emerges from replay: when every stream frontier reaches the
	// invalidated sweep end, the phase moves to complete_pending (the release
	// gate re-verifies everything before deleting the row).
	if err := maybeCompletePendingTx(ctx, tx, chainID, row); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// streamFrontier reads the in-txn frontier for one stream.
func streamFrontier(row *RecoveryRow, stream RecoveryStream) *int64 {
	switch stream {
	case RecoveryStreamBlock:
		return row.BlockFrontier
	case RecoveryStreamLog:
		return row.LogFrontier
	default:
		return row.DepositFrontier
	}
}

// sweepEndTx recomputes the invalidated sweep end under the lock: the max
// non-canonical height above the ancestor (the old suffix, fixed once
// invalidation committed). Zero such rows means nothing was swept.
func sweepEndTx(ctx context.Context, tx pgx.Tx, chainID int64, ancestor int64) (int64, error) {
	var n *int64
	if err := tx.QueryRow(ctx, maxInvalidatedHeightSQL, chainID, ancestor).Scan(&n); err != nil {
		return 0, fmt.Errorf("read invalidated sweep end: %w", err)
	}
	if n == nil {
		return ancestor, nil
	}
	return *n, nil
}

// advanceReplayProgressTx advances the stream frontier plus the stream
// checkpoint with exact-position guards (a concurrent move fails the guard →
// stale, never a jump or a skip).
func advanceReplayProgressTx(ctx context.Context, tx pgx.Tx, chainID int64, row *RecoveryRow, stream RecoveryStream, from, to int64) error {
	tag, err := tx.Exec(ctx, advanceStreamFrontierSQL(stream), chainID, row.RecoveryID, row.Seq, to)
	if err != nil {
		return fmt.Errorf("advance %s frontier: %w", stream, err)
	}
	if tag.RowsAffected() != 1 {
		return &RecoveryGateError{detail: fmt.Sprintf("%s frontier moved under the lock", stream)}
	}
	var checkpointSQL string
	var args []any
	switch stream {
	case RecoveryStreamBlock:
		var curHeight int64
		var curHash string
		var startHeight int64
		err := tx.QueryRow(ctx, readBlockCheckpointTxSQL, chainID).Scan(&curHeight, &curHash, &startHeight)
		if errors.Is(err, pgx.ErrNoRows) {
			// Pre-start floor case (rollback deleted the row): re-insert
			// with start_height = the bound floor. A misrouted from fails
			// closed downstream (ordinary start check refuses).
			var canonHash string
			if err := tx.QueryRow(ctx, canonicalHashAtHeightSQL, chainID, to).Scan(&canonHash); err != nil {
				return fmt.Errorf("read replay end canonical: %w", err)
			}
			tag, err := tx.Exec(ctx, insertBlockCheckpointTxSQL, chainID, to, canonHash, from)
			if err != nil {
				return fmt.Errorf("insert block checkpoint: %w", err)
			}
			if tag.RowsAffected() != 1 {
				return &RecoveryGateError{detail: "block checkpoint insert affected 0 rows (concurrent bootstrap)"}
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("read block checkpoint for replay: %w", err)
		}
		if curHeight != from-1 {
			return &RecoveryGateError{detail: fmt.Sprintf(
				"block checkpoint %d does not meet replay start %d", curHeight, from)}
		}
		var canonPrev, canonHash string
		if err := tx.QueryRow(ctx, canonicalHashAtHeightSQL, chainID, from-1).Scan(&canonPrev); err != nil {
			return fmt.Errorf("read replay start canonical: %w", err)
		}
		if curHash != canonPrev {
			return &RecoveryGateError{detail: "block checkpoint hash is not the canonical boundary"}
		}
		if err := tx.QueryRow(ctx, canonicalHashAtHeightSQL, chainID, to).Scan(&canonHash); err != nil {
			return fmt.Errorf("read replay end canonical: %w", err)
		}
		checkpointSQL = advanceBlockCheckpointTxSQL
		args = []any{chainID, to, canonHash, curHeight, curHash}
	case RecoveryStreamLog:
		checkpointSQL = advanceLogCheckpointTxSQL
		args = []any{chainID, to + 1, from}
	case RecoveryStreamDeposit:
		checkpointSQL = advanceDepositCheckpointTxSQL
		args = []any{chainID, to + 1, from}
	default:
		return fmt.Errorf("replay range: unknown stream %q", stream)
	}
	tag, err = tx.Exec(ctx, checkpointSQL, args...)
	if err != nil {
		return fmt.Errorf("advance %s checkpoint: %w", stream, err)
	}
	if tag.RowsAffected() != 1 {
		return &RecoveryGateError{detail: fmt.Sprintf("%s checkpoint moved under the lock", stream)}
	}
	return nil
}

// maybeCompletePendingTx moves replaying → complete_pending once every
// stream frontier reaches the invalidated sweep end (re-read under the lock;
// completion never means catching the live head — only the swept range).
func maybeCompletePendingTx(ctx context.Context, tx pgx.Tx, chainID int64, row *RecoveryRow) error {
	updated, err := readRecoveryRowTx(ctx, tx, chainID)
	if err != nil {
		return err
	}
	if updated == nil || updated.Phase != reorgPhaseReplaying && updated.Phase != reorgPhaseInvalidated {
		return nil
	}
	end, err := sweepEndTx(ctx, tx, chainID, *row.AncestorNumber)
	if err != nil {
		return err
	}
	// Every stream must reach the sweep end — with one principled exception:
	// a stream with no checkpoint row never processed anything, so nothing
	// was swept for it and there is nothing to replay; its ordinary
	// bootstrap owns it post-release. Frontier NULL + row present, however,
	// is unfinished work, never completion.
	frontiers := [3]*int64{updated.BlockFrontier, updated.LogFrontier, updated.DepositFrontier}
	names := [3]string{"block", "log", "deposit"}
	checks := [3]string{readBlockCheckpointTxSQL, readLogCheckpointTxSQL, readDepositCheckpointTxSQL}
	for i, f := range frontiers {
		if f != nil {
			if *f < end {
				return nil
			}
			continue
		}
		var derr error
		if i == 0 {
			var h, st int64
			var hh string
			derr = tx.QueryRow(ctx, checks[i], chainID).Scan(&h, &hh, &st)
		} else {
			var n, st int64
			var cfg string
			derr = tx.QueryRow(ctx, checks[i], chainID).Scan(&n, &st, &cfg)
		}
		if derr == nil {
			return nil // progress exists but unreplayed: not complete
		}
		if !errors.Is(derr, pgx.ErrNoRows) {
			return fmt.Errorf("read %s checkpoint for completion: %w", names[i], derr)
		}
	}
	tag, err := tx.Exec(ctx, advanceRecoveryPhaseSQL, chainID, row.RecoveryID, row.Seq, reorgPhaseCompletePending)
	if err != nil {
		return fmt.Errorf("advance to complete_pending: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return &RecoveryGateError{detail: "complete_pending advance affected 0 rows"}
	}
	return nil
}

// RecanonicalizeRecoveryBlock flips one height back to its old block (Q4
// same-hash path): the new-fork row goes canonical=false FIRST, then the old
// row goes canonical=true — each statement individually satisfies the partial
// unique (no deferral); the transient zero-canonical state is invisible
// inside the transaction. Only afterwards may revive run for that height.
func RecanonicalizeRecoveryBlock(ctx context.Context, pool *pgxpool.Pool, lease *Lease, chainID int64, cap RecoveryCapture, height int64, oldHash string) error {
	if pool == nil {
		return errors.New("recanonicalize block: nil pool")
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin recanonicalize transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockRecoveryChain(ctx, tx, chainID, lease); err != nil {
		return err
	}
	row, err := recheckRecoveryGate(ctx, tx, chainID, cap, reorgPhaseInvalidated, reorgPhaseReplaying)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, clearNewForkCanonicalSQL, chainID, height, oldHash)
	if err != nil {
		return fmt.Errorf("clear new-fork canonical: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return &RecoveryGateError{detail: fmt.Sprintf(
			"height %d has no single canonical foreign row to clear", height)}
	}
	tag, err = tx.Exec(ctx, restoreOldForkCanonicalSQL, chainID, height, oldHash)
	if err != nil {
		return fmt.Errorf("restore old-fork canonical: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return &RecoveryGateError{detail: fmt.Sprintf("height %d old row %s not restored", height, oldHash)}
	}
	detail := fmt.Sprintf("recovery=%s height=%d hash=%s version=%d", row.RecoveryID, height, oldHash, row.Seq)
	if err := appendRecoveryEvent(ctx, tx, chainID, row.RecoveryID, row.Seq, "replay_progress", detail); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ReviveRecoveryObservation returns one Orphaned observation to Pending in
// place (Q4 six rules): the original row is reused (zero new observations),
// only the current effective recovery may convert, and only after re-verified
// block binding (canonical row), log binding (source log row), and history
// identity. Ex-Confirmed rows pass through Pending (basis retained as
// history); 005 reconfirms post-release under the live policy. Repeat
// execution converges on the transition-log UNIQUE.
func ReviveRecoveryObservation(ctx context.Context, pool *pgxpool.Pool, lease *Lease, chainID int64, cap RecoveryCapture, blockHash, txHash string, logIndex int64, evidence string) error {
	if pool == nil {
		return errors.New("revive observation: nil pool")
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin revive transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockRecoveryChain(ctx, tx, chainID, lease); err != nil {
		return err
	}
	row, err := recheckRecoveryGate(ctx, tx, chainID, cap, reorgPhaseInvalidated, reorgPhaseReplaying)
	if err != nil {
		return err
	}
	var status, orphanID string
	err = tx.QueryRow(ctx, readObservationStatusSQL, chainID, blockHash, txHash, logIndex).Scan(&status, &orphanID)
	if errors.Is(err, pgx.ErrNoRows) {
		return &RecoveryGateError{detail: "revive target observation is missing"}
	}
	if err != nil {
		return fmt.Errorf("re-read revive target: %w", err)
	}
	if status == "pending" {
		// Possible repeat: converge only when this recovery already recorded
		// the conversion; otherwise the row was revived by someone else's
		// authority and this call refuses.
		var one int
		if err := tx.QueryRow(ctx, readTransitionSQL, chainID, blockHash, txHash, logIndex, "orphaned", "pending", row.RecoveryID).Scan(&one); err == nil {
			return nil
		}
		return &RecoveryGateError{detail: "revive target is pending outside this recovery's conversion"}
	}
	if status != "orphaned" || orphanID != row.RecoveryID {
		return &RecoveryGateError{detail: fmt.Sprintf(
			"revive target status=%s recovery=%s is not this recovery's orphan", status, orphanID)}
	}
	var blockNumber int64
	var canonical bool
	err = tx.QueryRow(ctx, readBlockBindingSQL, chainID, blockHash).Scan(&blockNumber, &canonical)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && !canonical {
		return &RecoveryGateError{detail: "revive target block is not canonical under the lock"}
	}
	if err != nil {
		return fmt.Errorf("re-read revive block binding: %w", err)
	}
	var one int
	if err := tx.QueryRow(ctx, readLogBindingSQL, chainID, blockHash, txHash, logIndex).Scan(&one); errors.Is(err, pgx.ErrNoRows) {
		return &RecoveryGateError{detail: "revive target log binding is missing under the lock"}
	} else if err != nil {
		return fmt.Errorf("re-read revive log binding: %w", err)
	}
	// History identity: the observation's version row still exists (versions
	// are append-only, so this is a corruption tripwire, not a migration).
	if err := tx.QueryRow(ctx, readObservationVersionSQL, chainID, blockHash, txHash, logIndex).Scan(&one); err != nil {
		return fmt.Errorf("re-read revive version binding: %w", err)
	}
	tag, err := tx.Exec(ctx, reviveObservationSQL, chainID, blockHash, txHash, logIndex, row.RecoveryID)
	if err != nil {
		return fmt.Errorf("revive observation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return &RecoveryGateError{detail: "revive affected 0 rows (concurrent conversion)"}
	}
	snapshot := fmt.Sprintf("revive block=%s canonical=%d evidence=%s", blockHash, blockNumber, evidence)
	tag, err = tx.Exec(ctx, insertTransitionSQL, chainID, blockHash, txHash, logIndex, "orphaned", "pending", row.RecoveryID, snapshot)
	if err != nil {
		if isUniqueViolation(err) {
			// Lost the insert race after converting: the converter holds the
			// same (from,to,recovery) — converge only if the row now reads
			// pending for this recovery (re-read outside? no — same txn sees
			// its own write; a conflict means a CONCURRENT txn committed the
			// same transition, so our UPDATE also hit 0... unreachable: our
			// UPDATE affected 1, so no concurrent converter exists).
			return fmt.Errorf("revive transition conflict after converting: %w", err)
		}
		return fmt.Errorf("insert revive transition: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("insert revive transition affected %d rows, want 1", tag.RowsAffected())
	}
	detail := fmt.Sprintf("recovery=%s observation=%s/%s/%d version=%d evidence=%s",
		row.RecoveryID, blockHash, txHash, logIndex, row.Seq, evidence)
	if err := appendRecoveryEvent(ctx, tx, chainID, row.RecoveryID, row.Seq, "observation_revived", detail); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// completionReport is the re-verified FR-19判据 for release decisions.
type completionReport struct {
	Ancestor     int64
	SweepEnd     int64
	Orphaned     int64
	Revived      int64
	Frontiers    [3]int64
	Survivors    string
	Dispositions string
}

// verifyCompletionReady re-reads ALL FR-19判据 under the lock (shared by the
// auto and manual release paths — neither trusts expired checks):
// ancestor pinned; swept range single-canonical; zero effective observations
// on non-canonical blocks in the swept range; zero stranded orphans on
// canonical blocks in the swept range; all frontiers at the sweep end.
// "Reconfirmation re-computed" is satisfied by revive/new-identity rows
// standing pending-eligible for 005 — recovery itself never confirms
// (FR-26 ban), so 005 reconfirms post-release under the live policy.
func verifyCompletionReady(ctx context.Context, tx pgx.Tx, chainID int64, row *RecoveryRow) (*completionReport, error) {
	if row.AncestorNumber == nil || row.AncestorHash == nil {
		return nil, &RecoveryGateError{detail: "completion requires a confirmed ancestor"}
	}
	ancestor := *row.AncestorNumber
	end, err := sweepEndTx(ctx, tx, chainID, ancestor)
	if err != nil {
		return nil, err
	}
	var canon int64
	if err := tx.QueryRow(ctx, countCanonicalRangeSQL, chainID, ancestor+1, end).Scan(&canon); err != nil {
		return nil, fmt.Errorf("completion canon proof: %w", err)
	}
	if end > ancestor && canon != end-ancestor {
		return nil, &RecoveryGateError{detail: fmt.Sprintf(
			"swept range [%d,%d] holds %d canonical rows, want %d", ancestor+1, end, canon, end-ancestor)}
	}
	var staleEffective int64
	if err := tx.QueryRow(ctx, countStaleEffectiveSQL, chainID, ancestor).Scan(&staleEffective); err != nil {
		return nil, fmt.Errorf("completion stale-effective proof: %w", err)
	}
	if staleEffective != 0 {
		return nil, &RecoveryGateError{detail: fmt.Sprintf(
			"%d effective observations still bound to non-canonical blocks", staleEffective)}
	}
	var stranded int64
	if err := tx.QueryRow(ctx, countStrandedOrphanSQL, chainID, ancestor).Scan(&stranded); err != nil {
		return nil, fmt.Errorf("completion stranded-orphan proof: %w", err)
	}
	if stranded != 0 {
		return nil, &RecoveryGateError{detail: fmt.Sprintf(
			"%d orphaned observations still bound to canonical blocks", stranded)}
	}
	frontiers := [3]*int64{row.BlockFrontier, row.LogFrontier, row.DepositFrontier}
	names := [3]string{"block", "log", "deposit"}
	var fv [3]int64
	for i, f := range frontiers {
		if f == nil || *f < end {
			got := int64(-1)
			if f != nil {
				got = *f
			}
			return nil, &RecoveryGateError{detail: fmt.Sprintf(
				"%s frontier %d has not reached sweep end %d", names[i], got, end)}
		}
		fv[i] = *f
	}
	var orphans, revived int64
	_ = tx.QueryRow(ctx, countRecoveryOrphansSQL, chainID, row.RecoveryID).Scan(&orphans)
	_ = tx.QueryRow(ctx, countRecoveryRevivedSQL, chainID, row.RecoveryID).Scan(&revived)
	dispositions := fmt.Sprintf("orphaned=%d revived=%d replayed=block:%d,log:%d,deposit:%d",
		orphans, revived, fv[0], fv[1], fv[2])
	return &completionReport{
		Ancestor: ancestor, SweepEnd: end, Orphaned: orphans, Revived: revived,
		Frontiers: fv, Survivors: readStreamPausesTx(ctx, tx, chainID), Dispositions: dispositions,
	}, nil
}

// terminalDetail renders the mandatory release content: bound tip,
// policy_seq, ancestor, swept ranges, disposition list, surviving pauses.
func terminalDetail(row *RecoveryRow, rep *completionReport, event string) string {
	return fmt.Sprintf("recovery=%s event=%s bound_old=%d:%s policy=%d ancestor=%d:%s swept=%d-%d disposition=[%s] surviving_pauses=%s version=%d",
		row.RecoveryID, event, row.BoundOldNumber, row.BoundOldHash, row.PolicySeq,
		rep.Ancestor, *row.AncestorHash, rep.Ancestor+1, rep.SweepEnd, rep.Dispositions, rep.Survivors, row.Seq)
}

// CompleteRecoveryVerify is the auto path (Q2a): re-verify everything, then
// DELETE only the recovery row + terminal event. It releases ONLY this
// recovery's cause — independent pauses survive and keep ordinary work
// stopped (survivors ride the terminal event as evidence).
func CompleteRecoveryVerify(ctx context.Context, pool *pgxpool.Pool, lease *Lease, chainID int64, cap RecoveryCapture) error {
	if pool == nil {
		return errors.New("complete recovery: nil pool")
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin complete transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockRecoveryChain(ctx, tx, chainID, lease); err != nil {
		return err
	}
	row, err := recheckRecoveryGate(ctx, tx, chainID, cap, reorgPhaseCompletePending)
	if err != nil {
		return err
	}
	rep, err := verifyCompletionReady(ctx, tx, chainID, row)
	if err != nil {
		return err
	}
	detail := terminalDetail(row, rep, "auto_completed")
	if err := appendRecoveryEvent(ctx, tx, chainID, row.RecoveryID, row.Seq, "auto_completed", detail); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, deleteRecoveryRowSQL, chainID, row.RecoveryID, row.Seq)
	if err != nil {
		return fmt.Errorf("delete recovery row: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return &RecoveryGateError{detail: "release delete affected 0 rows (concurrent release)"}
	}
	return tx.Commit(ctx)
}

// SignalRecoveryReconcile moves any phase to reconcile_required with the
// searched range + both tip evidences + cause class (unrecoverable evidence:
// over-deep, ancestor-unobtainable, exhausted history, contradictory RPC).
// It never chases the head and never marks completion.
func SignalRecoveryReconcile(ctx context.Context, pool *pgxpool.Pool, lease *Lease, chainID int64, cap RecoveryCapture, cause, searchedRange, evidence string) error {
	if pool == nil {
		return errors.New("signal reconcile: nil pool")
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin reconcile-signal transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockRecoveryChain(ctx, tx, chainID, lease); err != nil {
		return err
	}
	phases := []string{reorgPhaseDetected, reorgPhaseAncestorConfirmed, reorgPhaseInvalidated, reorgPhaseReplaying, reorgPhaseCompletePending}
	row, err := recheckRecoveryGate(ctx, tx, chainID, cap, phases...)
	if err != nil {
		// No active row (or foreign identity): nothing to hold. A reconcile
		// signal without a row is a no-op nil — the pause rows (if any) keep
		// ordinary work stopped independently.
		if _, ok := err.(*RecoveryGateError); ok {
			if r, rerr := readRecoveryRowTx(ctx, tx, chainID); rerr == nil && r == nil {
				return nil
			}
		}
		return err
	}
	tag, err := tx.Exec(ctx, advanceRecoveryPhaseSQL, chainID, row.RecoveryID, row.Seq, reorgPhaseReconcileRequired)
	if err != nil {
		return fmt.Errorf("advance to reconcile_required: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return &RecoveryGateError{detail: "reconcile advance affected 0 rows"}
	}
	detail := fmt.Sprintf("recovery=%s cause=%s searched=%s evidence=%s version=%d",
		row.RecoveryID, cause, searchedRange, evidence, row.Seq)
	if err := appendRecoveryEvent(ctx, tx, chainID, row.RecoveryID, row.Seq, "reconcile_signaled", detail); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// NoteRecoveryEvidence appends programmatic reconcile writes (evidence,
// progress, disposition) as short audited transactions under the same lock
// with version + phase rechecks. Each is data-only: none can trigger a
// paused external action or skip the completion gate (by construction — this
// function issues no RPC and touches no pause table).
func NoteRecoveryEvidence(ctx context.Context, pool *pgxpool.Pool, lease *Lease, chainID int64, cap RecoveryCapture, kind, detail string) error {
	if pool == nil {
		return errors.New("note evidence: nil pool")
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin evidence transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockRecoveryChain(ctx, tx, chainID, lease); err != nil {
		return err
	}
	row, err := recheckRecoveryGate(ctx, tx, chainID, cap,
		reorgPhaseDetected, reorgPhaseAncestorConfirmed, reorgPhaseInvalidated,
		reorgPhaseReplaying, reorgPhaseCompletePending, reorgPhaseReconcileRequired)
	if err != nil {
		return err
	}
	event := "replay_progress"
	if row.Phase == reorgPhaseReconcileRequired {
		event = "reconcile_signaled"
	}
	if err := appendRecoveryEvent(ctx, tx, chainID, row.RecoveryID, row.Seq, event,
		fmt.Sprintf("recovery=%s kind=%s %s version=%d", row.RecoveryID, kind, detail, row.Seq)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// AuthorizeRecoveryRepair is manual step 1 (Q2b): the authorized operator
// records Q2b minimum evidence (both tip identities, searched range, cause
// class) plus the full per-record disposition list. It NEVER releases
// confirm/sign/broadcast pauses (the row stays; phases stay); it only marks
// the authorized continuation in the audit trail. Entry is the DB operator
// over a direct connection (same shape as confirmauth.go); no endpoint, no
// new role, no payment-intent invention.
func AuthorizeRecoveryRepair(ctx context.Context, pool *pgxpool.Pool, chainID int64, operator, evidence, disposition string) error {
	if pool == nil {
		return errors.New("authorize repair: nil pool")
	}
	if operator == "" || evidence == "" || disposition == "" {
		return fmt.Errorf("authorize repair: %w: operator, evidence and disposition are all required (Q2b minimum)",
			ErrReorgPolicyRejected)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin repair-auth transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockRecoveryChain(ctx, tx, chainID, nil); err != nil {
		return err
	}
	row, err := readRecoveryRowTx(ctx, tx, chainID)
	if err != nil {
		return err
	}
	if row == nil || row.Phase != reorgPhaseReconcileRequired {
		return &RecoveryGateError{detail: "repair authorization requires a reconcile-held recovery"}
	}
	detail := fmt.Sprintf("recovery=%s operator=%s evidence=%s disposition=[%s] version=%d",
		row.RecoveryID, operator, evidence, disposition, row.Seq)
	if err := appendRecoveryEvent(ctx, tx, chainID, row.RecoveryID, row.Seq, "repair_authorized", detail); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// AuthorizeRecoveryRelease is manual step 2 (Q2b): privilege + Q2b minimum
// evidence + a terminal disposition list + a full re-verify pass, then DELETE
// only the recovery row + terminal event with the same mandatory content as
// the auto path. Atomic-or-nothing; reboot re-verifies (stateless by
// construction); independent pauses are never deleted; unknown withdrawal
// outcomes keep their待对账 semantics (no payment intent is ever created
// here — 011 owns that design).
func AuthorizeRecoveryRelease(ctx context.Context, pool *pgxpool.Pool, chainID int64, operator, evidence, disposition string) error {
	if pool == nil {
		return errors.New("authorize release: nil pool")
	}
	if operator == "" || evidence == "" || disposition == "" {
		return fmt.Errorf("authorize release: %w: operator, evidence and disposition are all required (Q2b minimum)",
			ErrReorgPolicyRejected)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin release-auth transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockRecoveryChain(ctx, tx, chainID, nil); err != nil {
		return err
	}
	row, err := readRecoveryRowTx(ctx, tx, chainID)
	if err != nil {
		return err
	}
	if row == nil || row.Phase != reorgPhaseReconcileRequired {
		return &RecoveryGateError{detail: "release authorization requires a reconcile-held recovery"}
	}
	rep, err := verifyCompletionReady(ctx, tx, chainID, row)
	if err != nil {
		return err
	}
	detail := terminalDetail(row, rep, "released") +
		fmt.Sprintf(" operator=%s evidence=%s authorized_disposition=[%s]", operator, evidence, disposition)
	if err := appendRecoveryEvent(ctx, tx, chainID, row.RecoveryID, row.Seq, "released", detail); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, deleteRecoveryRowSQL, chainID, row.RecoveryID, row.Seq)
	if err != nil {
		return fmt.Errorf("delete recovery row: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return &RecoveryGateError{detail: "release delete affected 0 rows (concurrent release)"}
	}
	return tx.Commit(ctx)
}

// Recovery commit SQL: parameterized statements of the 006 unified write
// protocol. Coordination (writeGuard/ensureLeaseSQL/lockCoordSQL/
// leaseVerdictSQL), the three pause existence checks and the canonical point
// reads are the shared constants — never copied here.
const (
	// readRecoveryGateSQL reads the current version in one statement: the
	// active row's seq (NULL when none), plus the events-stream MAX (0 with
	// no history). Callers derive current = active else MAX else 0.
	readRecoveryGateSQL = `
SELECT (SELECT recovery_seq FROM reorg_recovery WHERE chain_id = $1),
       COALESCE((SELECT MAX(recovery_seq) FROM reorg_recovery_events WHERE chain_id = $1), 0)`

	// readRecoveryRowSQL loads the active instance (nil row = no recovery).
	readRecoveryRowSQL = `
SELECT recovery_id, phase, policy_seq, max_depth, bound_old_number, bound_old_hash,
       ancestor_number, ancestor_hash, new_tip_number, new_tip_hash,
       block_frontier, log_frontier, deposit_frontier, recovery_seq
FROM reorg_recovery WHERE chain_id = $1`

	// maxRecoverySeqSQL reads the fencing high-water for seq generation.
	maxRecoverySeqSQL = `
SELECT COALESCE(MAX(recovery_seq), 0) FROM reorg_recovery_events WHERE chain_id = $1`

	// readReorgPolicyDepthSQL reads the bound policy value for establish.
	readReorgPolicyDepthSQL = `
SELECT max_depth FROM reorg_policy_history WHERE chain_id = $1 AND policy_seq = $2`

	// nextRecoveryEventSeqSQL allocates per-instance event order.
	nextRecoveryEventSeqSQL = `
SELECT COALESCE(MAX(event_seq), 0) + 1 FROM reorg_recovery_events WHERE chain_id = $1 AND recovery_id = $2`

	// insertRecoveryEventSQL appends one audit row.
	insertRecoveryEventSQL = `
INSERT INTO reorg_recovery_events (chain_id, recovery_id, recovery_seq, event_seq, event, detail)
VALUES ($1, $2, $3, $4, $5, $6)`

	// insertRecoveryRowSQL opens the instance (frontiers NULL = not started).
	insertRecoveryRowSQL = `
INSERT INTO reorg_recovery (chain_id, recovery_id, phase, policy_seq, max_depth,
    bound_old_number, bound_old_hash, new_tip_number, new_tip_hash, recovery_seq)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

	// confirmRecoveryAncestorSQL pins the ancestor (exact identity guard: the
	// row must still carry this seq in the detected phase).
	confirmRecoveryAncestorSQL = `
UPDATE reorg_recovery
SET phase = 'ancestor_confirmed', ancestor_number = $4, ancestor_hash = $5, updated_at = now()
WHERE chain_id = $1 AND recovery_id = $2 AND recovery_seq = $3 AND phase = 'detected'`

	// maxPersistedTipSQL reads the LIVE sweep tip at sweep execution (R7
	// coverage rule — never the establish-cached tip).
	maxPersistedTipSQL = `SELECT COALESCE(MAX(number), -1) FROM chain_blocks WHERE chain_id = $1`

	// invalidateBlocksSQL flips the old fork non-canonical over the live
	// swept range (retained, never deleted).
	invalidateBlocksSQL = `
UPDATE chain_blocks SET canonical = false
WHERE chain_id = $1 AND number >= $2 AND number <= $3 AND canonical`

	// advanceRecoveryPhaseSQL moves the phase with the exact identity guard
	// (seq + recovery_id); zero rows means a concurrent move.
	advanceRecoveryPhaseSQL = `
UPDATE reorg_recovery SET phase = $4, updated_at = now()
WHERE chain_id = $1 AND recovery_id = $2 AND recovery_seq = $3`

	// invalidateObservationsSQL converts affected observations to Orphaned
	// (ancestor-side rows outside the number range are untouched; confirm
	// basis columns retained as history evidence). The transition INSERT
	// below runs FIRST so from_status carries the pre-conversion value
	// (UPDATE ... RETURNING only yields new values); both statements share
	// the transaction, and their rowcounts must agree.
	orphanObservationsSQL = `
UPDATE deposit_observations
SET status = 'orphaned', orphaned_at = now(), orphan_recovery_id = $3,
    orphan_reason = 'reorg_invalidated'
WHERE chain_id = $1 AND block_number >= $2 AND status IN ('pending', 'confirmed')`

	// insertOrphanTransitionsSQL appends one transition row per conversion
	// with the true from_status plus a confirm-basis snapshot (empty when the
	// row never confirmed). Repeat execution conflicts on the log UNIQUE →
	// already recorded, zero new state.
	insertOrphanTransitionsSQL = `
INSERT INTO deposit_observation_transitions
    (chain_id, block_hash, tx_hash, log_index, from_status, to_status, recovery_id, basis_snapshot)
SELECT chain_id, block_hash, tx_hash, log_index, status, 'orphaned', $3,
    COALESCE(confirmed_at::text, '') || '|' || COALESCE(confirm_tip_number::text, '') || '|' ||
    COALESCE(confirm_tip_hash, '') || '|' || COALESCE(confirm_threshold::text, '') || '|' ||
    COALESCE(confirmations::text, '') || '|' || COALESCE(confirm_policy_seq::text, '')
FROM deposit_observations
WHERE chain_id = $1 AND block_number >= $2 AND status IN ('pending', 'confirmed')
ON CONFLICT DO NOTHING`

	// readStreamCheckpointSQL compasses the rollback: position + floor base.
	// (Three shapes; the block checkpoint carries (height, start_height).)
	readBlockCheckpointTxSQL   = `SELECT height, block_hash, start_height FROM indexer_checkpoint WHERE chain_id = $1`
	readLogCheckpointTxSQL     = `SELECT next_block, start_block, config_hash FROM log_checkpoint WHERE chain_id = $1`
	readDepositCheckpointTxSQL = `SELECT next_block, start_block, config_hash FROM deposit_checkpoint WHERE chain_id = $1`

	// rollbackBlockCheckpointSQL lands the header checkpoint on the ancestor
	// with the exact (height, hash) guard (the FK forbids anything else).
	rollbackBlockCheckpointSQL = `
UPDATE indexer_checkpoint SET height = $2, block_hash = $3, updated_at = now()
WHERE chain_id = $1 AND height = $4 AND block_hash = $5`

	// deleteBlockCheckpointSQL removes the header checkpoint for the
	// pre-start floor with the exact guard (replay re-inserts it).
	deleteBlockCheckpointSQL = `
DELETE FROM indexer_checkpoint WHERE chain_id = $1 AND height = $2 AND block_hash = $3`

	// insertBlockCheckpointTxSQL re-inserts the header checkpoint across the
	// first replayed range (first-unit semantics; start_height = the floor
	// the executor bound, fail-closed downstream on mismatch).
	insertBlockCheckpointTxSQL = `
INSERT INTO indexer_checkpoint (chain_id, height, block_hash, start_height)
VALUES ($1, $2, $3, $4)`

	// rollbackBlockCheckpointSQL moves the header checkpoint to the floor
	// with the exact-position guard (height must still equal the observed
	// value; the new hash is re-read canonically below — see replay).
	// rollbackLogCheckpointSQL / rollbackDepositCheckpointSQL move the
	// stream watermark to the floor with the exact (start, config, next)
	// guard. Zero rows means a concurrent move → stale, never a jump.
	rollbackLogCheckpointSQL = `
UPDATE log_checkpoint SET next_block = $2, updated_at = now()
WHERE chain_id = $1 AND start_block = $3 AND config_hash = $4 AND next_block = $5`

	// rollbackDepositCheckpointSQL mirrors the log shape on its own table.
	rollbackDepositCheckpointSQL = `
UPDATE deposit_checkpoint SET next_block = $2, updated_at = now()
WHERE chain_id = $1 AND start_block = $3 AND config_hash = $4 AND next_block = $5`

	// replayBlockSQL inserts one new-chain header (new identities only).
	replayBlockSQL = `
INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
VALUES ($1, $2, $3, $4, true)
ON CONFLICT (chain_id, number, hash) DO NOTHING`

	// countCanonicalRangeSQL proves batch coverage: exactly one canonical
	// row per replayed height when the batch is complete.
	countCanonicalRangeSQL = `
SELECT COUNT(*) FROM chain_blocks
WHERE chain_id = $1 AND number >= $2 AND number <= $3 AND canonical`

	// checkParentLinkageSQL proves suffix continuity: no canonical row in
	// range may dangle (missing or foreign parent below).
	checkParentLinkageSQL = `
SELECT COUNT(*) FROM chain_blocks b
WHERE b.chain_id = $1 AND b.number >= $2 AND b.number <= $3 AND b.canonical AND b.number > 0
  AND NOT EXISTS (SELECT 1 FROM chain_blocks c
      WHERE c.chain_id = $1 AND c.number = b.number - 1 AND c.canonical AND c.hash = b.parent_hash)`

	// maxInvalidatedHeightSQL recomputes the swept end: the max
	// non-canonical height above the ancestor.
	maxInvalidatedHeightSQL = `
SELECT MAX(number) FROM chain_blocks WHERE chain_id = $1 AND number > $2 AND NOT canonical`

	// canonicalHashAtHeightSQL names one height's canonical hash (partial
	// UNIQUE: at most one row).
	canonicalHashAtHeightSQL = `
SELECT hash FROM chain_blocks WHERE chain_id = $1 AND number = $2 AND canonical`

	// advanceBlockCheckpointTxSQL moves the header checkpoint across a
	// replayed range with the exact (height, hash) guard.
	advanceBlockCheckpointTxSQL = `
UPDATE indexer_checkpoint SET height = $2, block_hash = $3, updated_at = now()
WHERE chain_id = $1 AND height = $4 AND block_hash = $5`

	// advanceLogCheckpointTxSQL moves the log watermark across a replayed
	// range with the exact next_block guard.
	advanceLogCheckpointTxSQL = `
UPDATE log_checkpoint SET next_block = $2, updated_at = now()
WHERE chain_id = $1 AND next_block = $3`

	// advanceDepositCheckpointTxSQL mirrors the log shape.
	advanceDepositCheckpointTxSQL = `
UPDATE deposit_checkpoint SET next_block = $2, updated_at = now()
WHERE chain_id = $1 AND next_block = $3`

	// clearNewForkCanonicalSQL clears the single canonical foreign row at a
	// re-canonicalized height (recanonicalize step 1 of 2).
	clearNewForkCanonicalSQL = `
UPDATE chain_blocks SET canonical = false
WHERE chain_id = $1 AND number = $2 AND hash <> $3 AND canonical`

	// restoreOldForkCanonicalSQL restores the old row (step 2 of 2).
	restoreOldForkCanonicalSQL = `
UPDATE chain_blocks SET canonical = true
WHERE chain_id = $1 AND number = $2 AND hash = $3 AND NOT canonical`

	// readObservationStatusSQL re-reads the revive target's convertible state.
	readObservationStatusSQL = `
SELECT status, COALESCE(orphan_recovery_id, '') FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4`

	// readTransitionSQL checks one recorded conversion (repeat convergence).
	readTransitionSQL = `
SELECT 1 FROM deposit_observation_transitions
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4
  AND from_status = $5 AND to_status = $6 AND recovery_id = $7`

	// readBlockBindingSQL re-verifies the revive block is present + canonical.
	readBlockBindingSQL = `
SELECT number, canonical FROM chain_blocks WHERE chain_id = $1 AND hash = $2`

	// readLogBindingSQL re-verifies the revive source log row exists.
	readLogBindingSQL = `
SELECT 1 FROM erc20_transfer_logs
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4`

	// readObservationVersionSQL re-verifies the history version binding
	// (append-only versions: a corruption tripwire, never a migration).
	readObservationVersionSQL = `
SELECT 1 FROM deposit_observations o
JOIN deposit_config_history h ON (o.chain_id = h.chain_id AND o.version_seq = h.version_seq)
WHERE o.chain_id = $1 AND o.block_hash = $2 AND o.tx_hash = $3 AND o.log_index = $4`

	// reviveObservationSQL flips one orphan back to pending in place (the
	// exact orphan guard makes repeat/concurrent conversion affect 0 rows).
	reviveObservationSQL = `
UPDATE deposit_observations SET status = 'pending'
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4
  AND status = 'orphaned' AND orphan_recovery_id = $5`

	// insertTransitionSQL appends one status conversion (repeat execution
	// conflicts on the log UNIQUE → already recorded).
	insertTransitionSQL = `
INSERT INTO deposit_observation_transitions
    (chain_id, block_hash, tx_hash, log_index, from_status, to_status, recovery_id, basis_snapshot)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT DO NOTHING`

	// countStaleEffectiveSQL proves no effective observation is bound to a
	// non-canonical block in the swept range (missing block rows also count:
	// a swept observation without a block row is unjudgeable, never passed).
	countStaleEffectiveSQL = `
SELECT COUNT(*) FROM deposit_observations o
WHERE o.chain_id = $1 AND o.block_number > $2 AND o.status IN ('pending', 'confirmed')
  AND NOT EXISTS (SELECT 1 FROM chain_blocks b
      WHERE b.chain_id = o.chain_id AND b.hash = o.block_hash AND b.canonical)`

	// countStrandedOrphanSQL proves no orphan is still bound to a canonical
	// block in the swept range (every re-canonicalized height revived).
	countStrandedOrphanSQL = `
SELECT COUNT(*) FROM deposit_observations o
WHERE o.chain_id = $1 AND o.block_number > $2 AND o.status = 'orphaned'
  AND EXISTS (SELECT 1 FROM chain_blocks b
      WHERE b.chain_id = o.chain_id AND b.hash = o.block_hash AND b.canonical)`

	// countRecoveryOrphansSQL / countRecoveryRevivedSQL summarize this
	// round's dispositions for the terminal detail.
	countRecoveryOrphansSQL = `
SELECT COUNT(*) FROM deposit_observations WHERE chain_id = $1 AND orphan_recovery_id = $2`
	countRecoveryRevivedSQL = `
SELECT COUNT(*) FROM deposit_observation_transitions
WHERE chain_id = $1 AND recovery_id = $2 AND from_status = 'orphaned' AND to_status = 'pending'`

	// deleteRecoveryRowSQL releases exactly this round's cause (identity
	// guard: never a foreign round's row).
	deleteRecoveryRowSQL = `
DELETE FROM reorg_recovery WHERE chain_id = $1 AND recovery_id = $2 AND recovery_seq = $3`

	// readDepositVersionAtSQL attributes one height to its historical
	// version (FR-09 replay inheritance): the latest version whose
	// start_block covers h, with its frozen asset/watch snapshots.
	readDepositVersionAtSQL = `
SELECT version_seq, start_block, assets, watches FROM deposit_config_history
WHERE chain_id = $1 AND start_block <= $2 ORDER BY version_seq DESC LIMIT 1`
)

// readStreamCheckpointSQL returns the position/floor-base read per stream.
func readStreamCheckpointSQL(stream RecoveryStream) string {
	switch stream {
	case RecoveryStreamBlock:
		return readBlockCheckpointTxSQL
	case RecoveryStreamLog:
		return readLogCheckpointTxSQL
	default:
		return readDepositCheckpointTxSQL
	}
}

// advanceStreamFrontierSQL advances one stream frontier with the exact
// identity guard (recovery_id + seq); zero rows means a concurrent move.
func advanceStreamFrontierSQL(stream RecoveryStream) string {
	switch stream {
	case RecoveryStreamBlock:
		return `UPDATE reorg_recovery SET block_frontier = $4, updated_at = now()
WHERE chain_id = $1 AND recovery_id = $2 AND recovery_seq = $3`
	case RecoveryStreamLog:
		return `UPDATE reorg_recovery SET log_frontier = $4, updated_at = now()
WHERE chain_id = $1 AND recovery_id = $2 AND recovery_seq = $3`
	default:
		return `UPDATE reorg_recovery SET deposit_frontier = $4, updated_at = now()
WHERE chain_id = $1 AND recovery_id = $2 AND recovery_seq = $3`
	}
}

// isRecoveryGate reports the 006 recovery-state refusal for loop
// classification (loops handle it exactly like a stale basis: re-read state,
// start a new batch — never re-label old results).
func isRecoveryGate(err error) bool {
	var gate *RecoveryGateError
	return errors.As(err, &gate)
}

// readRecoveryRowByID loads the row for uncertain-COMMIT triage (establish:
// a visible row with our id means the insert landed).
func readRecoveryRowByID(ctx context.Context, q depositQuerier, chainID int64, recoveryID string) (*RecoveryRow, error) {
	row, err := readRecoveryRow(ctx, q, chainID)
	if err != nil || row == nil || row.RecoveryID != recoveryID {
		return nil, err
	}
	return row, nil
}

// bindReorgPolicyTx is bindReorgPolicy over an explicit pgx.Tx (establish
// calls it inside its own transaction; see reorgpolicy.go).
func bindReorgPolicyTx(ctx context.Context, tx pgx.Tx, chainID int64, envMaxDepthRaw string) (int64, error) {
	return bindReorgPolicy(ctx, tx, chainID, envMaxDepthRaw)
}

// nowUnixNano stamps new recovery identities (uniqueness is by DB UNIQUE;
// the stamp is identity, never validity — validity is DB now()).
func nowUnixNano() int64 {
	return time.Now().UnixNano()
}
