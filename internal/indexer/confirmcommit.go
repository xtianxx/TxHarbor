// confirmcommit.go implements 005's confirmation commit transaction + policy
// bootstrap (T010): the transaction-internal half of
// specs/005-confirmation-tracking/data-model.md §提交协议 / §首确认协议
// (FR-05/FR-07/FR-08, I1/I2/I3/I4), driven by the scan loop in confirmscan.go
// (T011, later).
//
// The transaction follows the shared five-step lease protocol used by
// 002/003/004 (ensure the coordination row, lock it FOR UPDATE, re-adjudicate
// with independent statements, write, COMMIT). Every verdict read happens
// after the lock, so anything committed while waiting for it is observed
// (Read Committed statement snapshots). Step numbers in the comments below
// correspond 1:1 to the data-model §提交协议 steps.
//
// Three captured bases travel together and are re-read independently under
// the lock (data-model §版本三分法); any mismatch rolls back with zero
// writes:
//
//   - chain view = captured tip (T, TH) + all three pause tables empty;
//   - policy version = captured (S, N) against MAX(policy_seq);
//   - deposit state = candidate still status='pending' with its referenced
//     chain_blocks(h) row present, canonical and hash-equal.
//
// A failed verdict or write leaves nothing behind (single transaction); an
// unknown COMMIT outcome is resolved by re-reading the observation PK and
// 定性. The tx is pure: no metrics writes (counters wiring is T013/
// integration scope); every refusal is a typed error the caller can count.
// This file never creates pause rows and never touches policy switching
// (T024).
package indexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ConfirmationDriftError is the data-model §首确认协议 / §并发时序情形 3
// refusal: the under-lock effective policy (MAX policy_seq row) differs from
// the captured (S, N). The scanner loud-stops on it (T011); it is never
// retried silently and never commits an old-threshold result.
type ConfirmationDriftError struct{ detail string }

func (e *ConfirmationDriftError) Error() string { return "confirmation policy drift: " + e.detail }

// ConfirmationChainViewError is the §候选分类 loop-stop side expressed as a
// commit refusal: the tip moved or vanished, the candidate row is gone or no
// longer pending, its referenced block is missing / non-canonical /
// hash-diverged, or the re-computed gate no longer holds. Zero writes.
type ConfirmationChainViewError struct{ detail string }

func (e *ConfirmationChainViewError) Error() string {
	return "confirmation chain view changed: " + e.detail
}

// ConfirmBasis is the caller-captured basis for one candidate: the
// observation PK (chain_id is constant per committer) plus the three
// captured versions — tip (T, TH) and policy (S, N). The transaction
// re-reads and compares each; in-memory values are never the write basis.
type ConfirmBasis struct {
	BlockHash  string
	TxHash     string
	LogIndex   uint64
	Height     uint64 // h: candidate block_number
	TipNumber  uint64 // T: captured canonical tip height
	TipHash    string // TH: captured canonical tip hash
	PolicySeq  int64  // S: captured policy_seq (1 when bootstrapping)
	ThresholdN uint64 // N: captured threshold (must equal configured N)
}

// ConfirmationCommitter holds the frozen confirmation input for the commit
// transaction. The scan loop (T011) owns candidate selection; this type owns
// only the atomic commit.
type ConfirmationCommitter struct {
	pool *pgxpool.Pool
	cfg  ConfirmationConfig
}

// NewConfirmationCommitter validates without any I/O (threshold range via
// NewConfirmationConfig, so N == 0 can never reach the gate's N-1).
func NewConfirmationCommitter(pool *pgxpool.Pool, cfg ConfirmationConfig) (*ConfirmationCommitter, error) {
	if pool == nil {
		return nil, errors.New("confirmation committer: nil pool")
	}
	valid, err := NewConfirmationConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("confirmation committer: %w", err)
	}
	return &ConfirmationCommitter{pool: pool, cfg: valid}, nil
}

// confirmNumericConfirmations binds the exact confirmation count as a decimal
// NUMERIC with no int64/float64 transit (data-model §确认数计算, OI-1;
// shape mirrors depositNumericAmount): uint64 -> base-10 -> NUMERIC.
func confirmNumericConfirmations(v uint64) pgtype.Numeric {
	return pgtype.Numeric{Int: new(big.Int).SetUint64(v), Exp: 0, Valid: true}
}

// ConfirmDepositUnit commits one Pending -> Confirmed conversion atomically:
// the conditional UPDATE plus, on the first confirmation, the policy
// bootstrap row land in one short transaction. basis carries the captured
// (T, TH, S, N); the caller must hold the lease.
func (c *ConfirmationCommitter) ConfirmDepositUnit(ctx context.Context, lease *Lease, basis ConfirmBasis) error {
	if lease == nil {
		return errors.New("confirmation commit: nil lease")
	}
	if basis.ThresholdN == 0 {
		return errors.New("confirmation commit: captured threshold N must be >= 1")
	}
	if basis.ThresholdN != c.cfg.ThresholdN {
		return &ConfirmationDriftError{detail: fmt.Sprintf(
			"captured N=%d differs from configured N=%d; refusing to commit a foreign basis",
			basis.ThresholdN, c.cfg.ThresholdN)}
	}
	if basis.PolicySeq < 1 {
		return &ConfirmationDriftError{detail: fmt.Sprintf(
			"captured policy_seq=%d is not a version identity", basis.PolicySeq)}
	}

	// Step 1: BEGIN with the shared statement guard; zero external calls
	// inside the transaction from here on.
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin confirmation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after COMMIT
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return fmt.Errorf("confirmation transaction statement guard: %w", err)
	}

	// Step 1 (cont.): idempotently ensure the coordination row exists.
	if _, err := tx.Exec(ctx, ensureLeaseSQL, c.cfg.ChainID, lease.ownerID, lease.Token(), lease.ttl.Seconds()); err != nil {
		return fmt.Errorf("ensure coordination row: %w", err)
	}
	// Step 2: take the chain-wide coordination lock (held to COMMIT;
	// confirm, 004-consume, auth-switch and pause-write txs serialize here).
	var (
		owner string
		token int64
		valid bool
	)
	err = tx.QueryRow(ctx, lockCoordSQL, c.cfg.ChainID).Scan(&owner, &token, &valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: coordination row missing", ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("lock coordination row: %w", err)
	}

	// Step 3: independent statements re-read and adjudicate under the lock.
	// A lost lease is refused here (a dispossessed worker commits nothing).
	var one int
	if err := tx.QueryRow(ctx, leaseVerdictSQL, c.cfg.ChainID, lease.ownerID, lease.Token()).Scan(&one); errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: owner/fencing/expiry verdict failed", ErrLeaseLost)
	} else if err != nil {
		return fmt.Errorf("lease verdict: %w", err)
	}
	// All three pause streams must be absent (data-model §提交协议 step 3;
	// 005 builds no pause row itself — a present row stops confirmation).
	for _, stream := range []struct {
		name string
		sql  string
	}{
		{"deposit_pause", depositPauseExistsSQL},
		{"log_pause", logPauseExistsSQL},
		{"indexer_pause", pauseExistsSQL},
	} {
		err := tx.QueryRow(ctx, stream.sql, c.cfg.ChainID).Scan(&one)
		switch {
		case err == nil:
			return &streamPauseError{stream: stream.name, chainID: c.cfg.ChainID}
		case errors.Is(err, pgx.ErrNoRows):
		default:
			return fmt.Errorf("read %s: %w", stream.name, err)
		}
	}

	// Policy guard: the effective policy (MAX policy_seq row) must equal the
	// captured (S, N). Zero rows takes the §首确认协议 bootstrap path below.
	var (
		storedSeq, storedThreshold int64
		havePolicy                 bool
	)
	err = tx.QueryRow(ctx, readConfirmationPolicySQL, c.cfg.ChainID).Scan(&storedSeq, &storedThreshold)
	switch {
	case err == nil:
		havePolicy = true
	case errors.Is(err, pgx.ErrNoRows):
		havePolicy = false
	default:
		return fmt.Errorf("read effective confirmation policy: %w", err)
	}

	if !havePolicy {
		// §首确认协议: still no row under the lock -> create (chain_id, 1)
		// in this same transaction and continue step 4 with (1, N) as the
		// established guard. A bootstrap row may only open seq 1.
		if basis.PolicySeq != 1 {
			return &ConfirmationDriftError{detail: fmt.Sprintf(
				"no policy row exists but captured policy_seq=%d (want 1 for bootstrap)", basis.PolicySeq)}
		}
		if _, err := tx.Exec(ctx, insertConfirmationBootstrapSQL, c.cfg.ChainID, int64(basis.ThresholdN)); err != nil {
			// Concurrent double-first-write serializes on PK (chain_id, 1):
			// the loser hits the unique conflict, rolls back with zero
			// writes, re-reads the policy and either continues the normal
			// path (same N) or drift-refuses (divergent N).
			_ = tx.Rollback(ctx)
			return c.resolveBootstrapConflict(ctx, basis)
		}
		storedSeq, storedThreshold = 1, int64(basis.ThresholdN)
	}
	if storedSeq != basis.PolicySeq || storedThreshold != int64(basis.ThresholdN) {
		// §并发时序情形 3: an old-config worker commits nothing after a
		// switch; the caller loud-stops (research R7).
		return &ConfirmationDriftError{detail: fmt.Sprintf(
			"captured policy (seq=%d N=%d) differs from effective (seq=%d N=%d)",
			basis.PolicySeq, basis.ThresholdN, storedSeq, storedThreshold)}
	}

	// Tip re-adjudication: the canonical tip must still be exactly (T, TH).
	// Missing tip -> no row -> refuse; moved tip -> mismatch -> refuse.
	var (
		tipNumber int64
		tipHash   string
	)
	err = tx.QueryRow(ctx, readConfirmationTipSQL, c.cfg.ChainID).Scan(&tipNumber, &tipHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return &ConfirmationChainViewError{detail: "canonical tip is missing under the lock"}
	}
	if err != nil {
		return fmt.Errorf("read canonical tip: %w", err)
	}
	if tipNumber < 0 {
		return &ConfirmationChainViewError{detail: fmt.Sprintf("canonical tip number %d is negative", tipNumber)}
	}
	if uint64(tipNumber) != basis.TipNumber || tipHash != basis.TipHash {
		return &ConfirmationChainViewError{detail: fmt.Sprintf(
			"captured tip (%d %s) differs from canonical tip (%d %s)",
			basis.TipNumber, basis.TipHash, tipNumber, tipHash)}
	}

	// Candidate re-adjudication: the observation row must still be pending,
	// and chain_blocks(h) must exist, be canonical and hash-match bh
	// (missing reference / hash divergence = chain-view anomaly, US3-2 /
	// Edge-170: stop confirmation, build no pause row).
	var (
		storedStatus string
		storedNumber int64
		storedHash   string
	)
	err = tx.QueryRow(ctx, readConfirmationCandidateSQL,
		c.cfg.ChainID, basis.BlockHash, basis.TxHash, int64(basis.LogIndex)).Scan(&storedStatus, &storedNumber, &storedHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return &ConfirmationChainViewError{detail: fmt.Sprintf(
			"candidate %s/%s/%d is missing under the lock",
			basis.BlockHash, basis.TxHash, basis.LogIndex)}
	}
	if err != nil {
		return fmt.Errorf("re-read confirmation candidate: %w", err)
	}
	if storedStatus != "confirmed" && storedStatus != "pending" {
		return &ConfirmationChainViewError{detail: fmt.Sprintf("candidate status %q is neither pending nor confirmed", storedStatus)}
	}
	if storedStatus != "pending" || storedNumber < 0 || uint64(storedNumber) != basis.Height || storedHash != basis.BlockHash {
		if storedStatus == "confirmed" {
			// §并发时序情形 2: the winner already converted; converge
			// without writing (first-seen facts stay the winner's, I2).
			_ = tx.Rollback(ctx)
			return c.convergeCommitted(ctx, basis)
		}
		return &ConfirmationChainViewError{detail: fmt.Sprintf(
			"candidate re-read (status=%s block_number=%d block_hash=%s) mismatches captured (h=%d bh=%s)",
			storedStatus, storedNumber, storedHash, basis.Height, basis.BlockHash)}
	}
	var refHash string
	err = tx.QueryRow(ctx, canonicalBlockHashSQL, c.cfg.ChainID, int64(basis.Height)).Scan(&refHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return &ConfirmationChainViewError{detail: fmt.Sprintf(
			"reference block %d is missing or non-canonical under the lock", basis.Height)}
	}
	if err != nil {
		return fmt.Errorf("re-read reference block: %w", err)
	}
	if refHash != basis.BlockHash {
		return &ConfirmationChainViewError{detail: fmt.Sprintf(
			"reference block %d hash %s diverges from candidate bh %s",
			basis.Height, refHash, basis.BlockHash)}
	}

	// Gate re-computation on the re-read values (R1 equivalent form, never
	// the spec formula's +1: tip >= h && tip-h >= N-1, overflow-free).
	tip, h, n := uint64(tipNumber), basis.Height, basis.ThresholdN
	if !ConfirmationReached(tip, h, n) {
		return &ConfirmationChainViewError{detail: fmt.Sprintf(
			"re-computed gate fails: tip=%d h=%d N=%d", tip, h, n)}
	}
	exact, err := ExactConfirmations(tip, h)
	if err != nil {
		// Saturation guard: unreachable defense-in-depth; triggered = internal
		// error, refuse the commit, persist nothing.
		return fmt.Errorf("confirmation commit refused: %w", err)
	}

	// Step 4: conditional UPDATE with the status='pending' predicate (second
	// door) writing status + confirmed_at + all five basis columns.
	// RowsAffected != 1 -> concurrent conversion (converge) or drift ->
	// roll back and re-read.
	tag, err := tx.Exec(ctx, confirmDepositObservationSQL,
		c.cfg.ChainID, basis.BlockHash, basis.TxHash, int64(basis.LogIndex),
		int64(tip), tipHash, int64(n), confirmNumericConfirmations(exact), basis.PolicySeq)
	if err != nil {
		return fmt.Errorf("confirm deposit observation %s/%s/%d: %w",
			basis.BlockHash, basis.TxHash, basis.LogIndex, err)
	}
	if tag.RowsAffected() != 1 {
		_ = tx.Rollback(ctx)
		return c.convergeCommitted(ctx, basis)
	}
	// Step 5: COMMIT. On unknown outcome (disconnect) re-read the
	// observation PK to定性 (mirror 004 uncertain-commit recovery): a
	// visible confirmed row means the commit landed.
	if err := tx.Commit(ctx); err != nil {
		visible, rerr := c.commitVisible(ctx, basis)
		if rerr == nil && visible {
			return nil
		}
		if rerr != nil {
			return fmt.Errorf("commit confirmation %s/%s/%d (outcome unknown, observation re-read failed: %v): %w",
				basis.BlockHash, basis.TxHash, basis.LogIndex, rerr, err)
		}
		return fmt.Errorf("commit confirmation %s/%s/%d (outcome unknown, observation unchanged): %w",
			basis.BlockHash, basis.TxHash, basis.LogIndex, err)
	}
	return nil
}

// resolveBootstrapConflict re-reads the policy after a lost bootstrap race
// (PK conflict -> rollback with zero writes): the winner's N decides. Same
// N continues the normal path on retry (stale, not an error of record);
// divergent N is drift (the loser's env is foreign, §首确认协议分歧双首启).
func (c *ConfirmationCommitter) resolveBootstrapConflict(ctx context.Context, basis ConfirmBasis) error {
	var (
		seq, threshold int64
	)
	err := c.pool.QueryRow(ctx, readConfirmationPolicySQL, c.cfg.ChainID).Scan(&seq, &threshold)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("confirmation bootstrap conflict but no policy row is visible")
	}
	if err != nil {
		return fmt.Errorf("re-read confirmation policy after bootstrap conflict: %w", err)
	}
	if seq != basis.PolicySeq || threshold != int64(basis.ThresholdN) {
		return &ConfirmationDriftError{detail: fmt.Sprintf(
			"bootstrap race lost: winner policy (seq=%d N=%d) differs from captured (seq=%d N=%d)",
			seq, threshold, basis.PolicySeq, basis.ThresholdN)}
	}
	return errStaleState
}

// convergeCommitted re-reads the observation PK after a lost conditional
// write: a visible confirmed row is convergence (nil, §并发时序情形 2);
// anything else is a stale precondition change for the caller to re-read.
func (c *ConfirmationCommitter) convergeCommitted(ctx context.Context, basis ConfirmBasis) error {
	visible, err := c.commitVisible(ctx, basis)
	if err != nil {
		return fmt.Errorf("converge re-read %s/%s/%d: %w",
			basis.BlockHash, basis.TxHash, basis.LogIndex, err)
	}
	if visible {
		return nil
	}
	return errStaleState
}

// commitVisible reports whether the observation PK is durably confirmed.
func (c *ConfirmationCommitter) commitVisible(ctx context.Context, basis ConfirmBasis) (bool, error) {
	var status string
	err := c.pool.QueryRow(ctx, readConfirmationStatusSQL,
		c.cfg.ChainID, basis.BlockHash, basis.TxHash, int64(basis.LogIndex)).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return status == "confirmed", nil
}

// Confirmation commit SQL: parameterized statements of the 005 unified write
// protocol (data-model §提交协议 step 4 / §首确认协议). Coordination
// (writeGuard/ensureLeaseSQL/lockCoordSQL/leaseVerdictSQL), the three pause
// existence checks (depositPauseExistsSQL/logPauseExistsSQL/pauseExistsSQL)
// and the canonical point read (canonicalBlockHashSQL) are the shared
// constants — never copied here.
const (
	// readConfirmationPolicySQL reads the effective policy (MAX policy_seq
	// row): the single-row authority (data-model Table 2, research R3).
	readConfirmationPolicySQL = `
SELECT policy_seq, threshold FROM confirmation_policy_history
WHERE chain_id = $1
ORDER BY policy_seq DESC LIMIT 1`

	// readConfirmationTipSQL reads the current canonical tip (number, hash).
	readConfirmationTipSQL = `
SELECT number, hash FROM chain_blocks
WHERE chain_id = $1 AND canonical
ORDER BY number DESC LIMIT 1`

	// readConfirmationCandidateSQL re-reads the candidate's convertible
	// state by observation PK (chain_id is constant per committer).
	readConfirmationCandidateSQL = `
SELECT status, block_number, block_hash FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4`

	// readConfirmationStatusSQL定性 an unknown write outcome by PK.
	readConfirmationStatusSQL = `
SELECT status FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4`

	// insertConfirmationBootstrapSQL creates the first policy row in the
	// same transaction as the first conversion (§首确认协议): policy_seq=1,
	// prev_seq NULL, operator='bootstrap', request_id NULL (the single NULL
	// row per chain via the partial unique index). reason/expected_old_seq/
	// created_at take their defaults. A concurrent double-first-write
	// collides on PK (chain_id, 1) and the loser converges via
	// resolveBootstrapConflict.
	insertConfirmationBootstrapSQL = `
INSERT INTO confirmation_policy_history
    (chain_id, policy_seq, threshold, prev_seq, operator, request_id)
VALUES ($1, 1, $2, NULL, 'bootstrap', NULL)`

	// confirmDepositObservationSQL is the only 005 write path to
	// deposit_observations: the status='pending' predicate plus the
	// RowsAffected==1 check make confirmed rows unwritable here (I2).
	// confirmations binds as a decimal NUMERIC (see
	// confirmNumericConfirmations; no int64/float64 transit).
	confirmDepositObservationSQL = `
UPDATE deposit_observations
SET status = 'confirmed', confirmed_at = now(),
    confirm_tip_number = $5, confirm_tip_hash = $6, confirm_threshold = $7,
    confirmations = $8, confirm_policy_seq = $9
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4
  AND status = 'pending'`
)
