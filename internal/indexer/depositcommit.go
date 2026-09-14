// depositcommit.go implements 004's unit atomic commit (T006): the
// transaction-internal half of specs/004-deposit-detection/data-model.md
// §写事务协议 (FR-08/FR-09, I1/I2/I3), driven by the scan loop in
// depositscanner.go.
//
// The transaction follows the shared five-step lease protocol used by 002/003
// (ensure the coordination row, lock it FOR UPDATE, re-adjudicate with
// independent statements, write, COMMIT). Every verdict read happens after the
// lock, so anything committed while waiting is observed (Read Committed
// statement snapshots); the protocol description is data-model step 4.
//
// Three identities are enforced:
//   - version identity: the version_seq captured when the unit was read must
//     still be the latest one under the lock, otherwise the whole result is
//     abandoned and re-read. Version isolation is by seq, never by hash, so an
//     H1 loopback (same content hash on a newer seq) is rejected as well.
//   - progress identity: the checkpoint must still be exactly
//     (start_block, config_hash, next_block=a); a first unit requires both
//     deposit_checkpoint and deposit_config_history to be absent (one-sided
//     state is corruption, never bootstrapped). New observations carry the
//     captured version_seq, never "latest at commit".
//   - content identity: pre-existing observations are re-read and compared
//     field by field WITHOUT version_seq (a replay keeps the original
//     reference); any mismatch fails the whole batch, never overwrites.
//
// A failed verdict or write leaves nothing behind (single transaction);
// an unknown COMMIT outcome is resolved by re-reading the durable progress and
// continuing idempotently. Pause persistence, resume and authorization
// transitions are T014/T015/T025: this file never creates pause rows, never
// deletes or repairs anything.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
)

// depositObservationStatusPending is the only status 004 writes (data-model
// Table 1: single-value CHECK, extended only by 005's own migration).
const depositObservationStatusPending = "pending"

var (
	// errDepositVersionMismatch means the captured version_seq is no longer the
	// latest one under the coordination lock (a concurrent authorization
	// committed, or another worker bootstrapped the chain). The unit result is
	// abandoned and re-read; nothing is written and same-hash loopbacks are
	// rejected on seq identity alone (data-model 写事务协议 step 4).
	errDepositVersionMismatch = errors.New("deposit commit: captured config version is no longer current")

	// errDepositCoverageLost means the under-lock coverage re-proof failed: the
	// upstream log_checkpoint row is gone or no longer proves next_block > b.
	// The watermark only advances, so a lost row is a chain-view anomaly and a
	// refusal, never a wait (R2).
	errDepositCoverageLost = errors.New("deposit commit: upstream coverage re-proof failed")
)

// depositConfigMismatchError is the FR-06 refusal: the durable checkpoint's
// (start_block, config_hash) differs from this scanner's frozen configuration.
// The comparison object is the row's start_block, never next_block.
type depositConfigMismatchError struct{ detail string }

func (e *depositConfigMismatchError) Error() string { return "deposit config changed: " + e.detail }

// depositIdentityConflictError is the data-model step-5 identity_conflict
// verdict: an existing observation for the same source identity carries
// different content. The whole batch rolls back and the existing row is never
// overwritten (T014 persists validation_failed).
type depositIdentityConflictError struct {
	identity string
	detail   string
}

func (e *depositIdentityConflictError) Error() string {
	return fmt.Sprintf("deposit identity conflict: class=identity_conflict identity=%s %s", e.identity, e.detail)
}

// depositIdentity is the data-model Table 1 row identity (chain_id is constant
// per scanner).
type depositIdentity struct {
	blockHash string
	txHash    string
	logIndex  uint64
}

func depositSourceIdentity(row depositSourceLog) depositIdentity {
	return depositIdentity{blockHash: row.blockHash, txHash: row.txHash, logIndex: row.logIndex}
}

func (id depositIdentity) String() string {
	return fmt.Sprintf("%s/%s/%d", id.blockHash, id.txHash, id.logIndex)
}

// depositIdentityCount returns the number of distinct source identities in the
// unit, the left side of the step-5 row-count reconciliation.
func depositIdentityCount(rows []depositSourceLog) int {
	seen := make(map[depositIdentity]struct{}, len(rows))
	for _, row := range rows {
		seen[depositSourceIdentity(row)] = struct{}{}
	}
	return len(seen)
}

// reconcileDepositBatch enforces the data-model step-5 row-count invariant:
// every matched identity has exactly one consistent stored row, and the
// distinct source identity count equals inserted + existing consistent + legal
// zero generation (zero/nomatch). Any imbalance is a silent-loss failure and
// rolls the batch back.
func reconcileDepositBatch(identityCount, matched, inserted, existing, zero, nomatch int) error {
	if inserted+existing != matched {
		return fmt.Errorf("deposit batch reconciliation: %d matched identities, %d inserted + %d existing consistent rows",
			matched, inserted, existing)
	}
	if identityCount != inserted+existing+zero+nomatch {
		return fmt.Errorf("deposit batch reconciliation: %d distinct source identities, %d inserted + %d existing + %d zero + %d nomatch",
			identityCount, inserted, existing, zero, nomatch)
	}
	return nil
}

// depositUnitEnd returns the last height of the unit starting at a when the
// batch size is used as an upper bound: the unit is always a continuous closed
// interval and a+batch-1 is guarded against unsigned overflow.
func depositUnitEnd(a, batch uint64) uint64 {
	if batch == 0 {
		return a
	}
	if a > math.MaxUint64-(batch-1) {
		return math.MaxUint64
	}
	return a + batch - 1
}

// depositCoveredEnd sizes one unit: BatchBlocks is an upper bound (research
// R7), trimmed to the exclusive upstream watermark nextExclusive-1, and only
// when the watermark actually covers the unit start (nextExclusive > a, so the
// subtraction never underflows). nextExclusive <= a means nothing is
// processable yet; the returned b is then only the classification probe and
// the caller waits. The result is never below a (no empty or reversed unit),
// and on the covered path it is bounded by the upstream BIGINT watermark, so
// the commit's b+1 stays in int64 range.
func depositCoveredEnd(a, batch, nextExclusive uint64) uint64 {
	b := depositUnitEnd(a, batch)
	if nextExclusive > a {
		if end := nextExclusive - 1; b > end {
			b = end
		}
	}
	if b < a {
		b = a // invariant guard: a unit never ends before it starts
	}
	return b
}

// depositNumericAmount converts the exact base-10 amount string produced by
// the parser into the NUMERIC binding; no float64 is ever on this path
// (data-model Table 1 / R4).
func depositNumericAmount(amount string) (pgtype.Numeric, error) {
	v, ok := new(big.Int).SetString(amount, 10)
	if !ok || v.Sign() <= 0 {
		return pgtype.Numeric{}, fmt.Errorf("deposit amount %q is not a positive base-10 integer", amount)
	}
	return pgtype.Numeric{Int: v, Exp: 0, Valid: true}, nil
}

// commitDepositUnit commits one processed unit [a,b] atomically: the matched
// observations and the checkpoint advance to exactly b+1 in one short
// transaction, with the bootstrap history row created in the same transaction
// as the first checkpoint row (data-model Table 4, step 5).
//
// captured is the progress read together with the unit (nil = empty progress,
// first unit); it carries the version_seq the processing was based on. u.rows
// are the source rows the batch was parsed from, used for the row-count
// reconciliation. The caller must hold the lease; the transaction re-verifies
// ownership, the three pause rows, the version, the exact progress guard, the
// upstream coverage and the canonical view under the coordination lock before
// writing anything.
func (s *DepositScanner) commitDepositUnit(
	ctx context.Context,
	lease *Lease,
	u *depositUnit,
	batch depositBatch,
	captured *depositProgress,
	a, b uint64,
	rc ...RecoveryCapture,
) error {
	if lease == nil {
		return errors.New("deposit commit: nil lease")
	}
	if u == nil {
		return errors.New("deposit commit: nil unit")
	}
	rcap, err := resolveRecoveryCapture(ctx, s.pool, s.cfg.ChainID, rc)
	if err != nil {
		return err
	}
	first := captured == nil
	if first {
		if a != s.cfg.StartBlock {
			return errStaleState
		}
	} else {
		if captured.startBlock != s.cfg.StartBlock || captured.configHash != s.cfg.ConfigHash {
			return &depositConfigMismatchError{detail: fmt.Sprintf(
				"captured basis (start_block=%d config_hash=%s) differs from configured (start_block=%d config_hash=%s)",
				captured.startBlock, captured.configHash, s.cfg.StartBlock, s.cfg.ConfigHash)}
		}
		if captured.nextBlock != a {
			return errStaleState
		}
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin deposit transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after COMMIT
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return fmt.Errorf("deposit transaction statement guard: %w", err)
	}

	// Step 2: idempotently ensure the coordination row exists.
	if _, err := tx.Exec(ctx, ensureLeaseSQL, s.cfg.ChainID, lease.ownerID, lease.Token(), lease.ttl.Seconds()); err != nil {
		return fmt.Errorf("ensure coordination row: %w", err)
	}
	// Step 3: take the chain-wide coordination lock (held to COMMIT).
	var (
		owner string
		token int64
		valid bool
	)
	err = tx.QueryRow(ctx, lockCoordSQL, s.cfg.ChainID).Scan(&owner, &token, &valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: coordination row missing", ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("lock coordination row: %w", err)
	}

	// Step 4: independent statements re-read and adjudicate under the lock.
	var one int
	if err := tx.QueryRow(ctx, leaseVerdictSQL, s.cfg.ChainID, lease.ownerID, lease.Token()).Scan(&one); errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: owner/fencing/expiry verdict failed", ErrLeaseLost)
	} else if err != nil {
		return fmt.Errorf("lease verdict: %w", err)
	}
	// All three pause streams must be absent (FR-12); an upstream pause blocks
	// the deposit commit as well.
	if err := s.rejectStreamPauses(ctx, tx); err != nil {
		return err
	}
	// 006 recovery gate (T015): same capture/commit-triple/refuse semantics;
	// 006-owned re-reads (Owned set, identity + phase verified under the lock
	// inside recheckRecoveryGate) pass where ordinary batches refuse.
	if _, err := recheckRecoveryGate(ctx, tx, s.cfg.ChainID, rcap); err != nil {
		return err
	}

	// Progress integrity, exact guard and version isolation. readProgress
	// reports one-sided checkpoint/history state as corruption, never repairs.
	current, err := s.readProgress(ctx, tx)
	if err != nil {
		return err
	}
	switch {
	case first && current != nil:
		// Another writer bootstrapped between the capture and the lock: the
		// captured empty basis is gone.
		return errDepositVersionMismatch
	case !first && current == nil:
		// The captured basis vanished (manual deletion): never bootstrap over
		// it, abandon and re-read.
		return errDepositVersionMismatch
	case !first && current.versionSeq != captured.versionSeq:
		// The version changed (even with the same content hash): the in-flight
		// result belongs to an older version and must not be committed.
		return errDepositVersionMismatch
	}
	if current != nil {
		if current.startBlock != s.cfg.StartBlock || current.configHash != s.cfg.ConfigHash {
			return &depositConfigMismatchError{detail: fmt.Sprintf(
				"chain %d checkpoint (start_block=%d config_hash=%s) differs from configured (start_block=%d config_hash=%s)",
				s.cfg.ChainID, current.startBlock, current.configHash, s.cfg.StartBlock, s.cfg.ConfigHash)}
		}
		if current.nextBlock != a {
			// Exactly continuous + unchanged: any other position is stale
			// (a concurrent writer committed while we waited for the lock).
			return errStaleState
		}
	}

	// Coverage re-proof (R2): the upstream watermark must still exceed b. The
	// identity is re-checked so the proof names the same whitelist.
	if err := s.verifyUpstreamCoverageUnderLock(ctx, tx, b); err != nil {
		return err
	}
	// Chain-view re-adjudication: every height in [a,b] must still exist,
	// canonical and with the hash observed before the lock.
	if err := s.readjudicateCanonicalRange(ctx, tx, u, a, b); err != nil {
		return err
	}

	// Step 5: write. The first unit creates the version ledger row in the same
	// transaction as the first checkpoint row (data-model Table 4: the
	// bootstrap "创世授权" is not an authorization request: request_id NULL,
	// operator=bootstrap).
	versionSeq := int64(1)
	if captured != nil {
		versionSeq = captured.versionSeq
	}
	if first {
		if _, err := tx.Exec(ctx, insertDepositBootstrapHistorySQL,
			s.cfg.ChainID, s.cfg.ConfigHash, int64(a),
			config.DepositSnapshot(s.cfg.Assets), config.DepositSnapshot(s.cfg.Watches)); err != nil {
			return fmt.Errorf("insert bootstrap config history: %w", err)
		}
	}

	inserted := 0
	for _, obs := range batch.matched {
		amount, err := depositNumericAmount(obs.amount)
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, insertDepositObservationSQL,
			s.cfg.ChainID, obs.blockHash, obs.txHash, int64(obs.logIndex), int64(obs.blockNumber),
			obs.contract, obs.sender, obs.recipient, amount, versionSeq)
		if err != nil {
			return fmt.Errorf("insert deposit observation %s: %w",
				depositIdentity{blockHash: obs.blockHash, txHash: obs.txHash, logIndex: obs.logIndex}, err)
		}
		inserted += int(tag.RowsAffected())
	}

	// Conflict content comparison: re-read every matched identity and compare
	// field by field. version_seq is deliberately excluded (a replay keeps the
	// original reference); in-memory knowledge is never the correctness basis.
	for _, obs := range batch.matched {
		id := depositIdentity{blockHash: obs.blockHash, txHash: obs.txHash, logIndex: obs.logIndex}
		amount, err := depositNumericAmount(obs.amount)
		if err != nil {
			return err
		}
		var (
			storedContract  string
			storedNumber    int64
			storedSender    string
			storedRecipient string
			storedAmount    string
			storedStatus    string
			amountEqual     bool
		)
		err = tx.QueryRow(ctx, readDepositObservationSQL,
			s.cfg.ChainID, obs.blockHash, obs.txHash, int64(obs.logIndex), amount).Scan(
			&storedContract, &storedNumber, &storedSender, &storedRecipient,
			&storedAmount, &storedStatus, &amountEqual)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("deposit observation missing after insert: identity %s (silent loss)", id)
		}
		if err != nil {
			return fmt.Errorf("re-read deposit observation %s: %w", id, err)
		}
		if storedContract != obs.contract || uint64(storedNumber) != obs.blockNumber ||
			storedSender != obs.sender || storedRecipient != obs.recipient ||
			!amountEqual || storedStatus != depositObservationStatusPending {
			if storedStatus != depositObservationStatusPending {
				// Ordinary rule UNCHANGED: any non-pending row fails the
				// batch. Only 006's own re-reads exempt rows orphaned under
				// the captured recovery version (the ordinary 004 path stays
				// stopped during recovery anyway); exempt rows still face
				// the content comparison below, never an overwrite.
				exempt, xerr := s.exemptOwnedOrphan(ctx, tx, rcap, id)
				if xerr != nil {
					return xerr
				}
				if exempt {
					if storedContract != obs.contract || uint64(storedNumber) != obs.blockNumber ||
						storedSender != obs.sender || storedRecipient != obs.recipient ||
						!amountEqual {
						return &depositIdentityConflictError{
							identity: id.String(),
							detail: fmt.Sprintf(
								"exempt orphan content differs: stored contract=%s block_number=%d sender=%s recipient=%s amount=%s",
								storedContract, storedNumber, storedSender, storedRecipient, storedAmount),
						}
					}
					continue
				}
			}
			return &depositIdentityConflictError{
				identity: id.String(),
				detail: fmt.Sprintf(
					"stored contract=%s block_number=%d sender=%s recipient=%s amount=%s status=%s",
					storedContract, storedNumber, storedSender, storedRecipient, storedAmount, storedStatus),
			}
		}
	}

	existing := len(batch.matched) - inserted
	if err := reconcileDepositBatch(depositIdentityCount(u.rows), len(batch.matched),
		inserted, existing, batch.zero, batch.nomatch); err != nil {
		return err
	}

	if first {
		tag, err := tx.Exec(ctx, insertDepositCheckpointSQL,
			s.cfg.ChainID, int64(a), s.cfg.ConfigHash, int64(b+1))
		if err != nil {
			return fmt.Errorf("insert deposit checkpoint: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return errStaleState
		}
	} else {
		tag, err := tx.Exec(ctx, advanceDepositCheckpointSQL,
			s.cfg.ChainID, int64(b+1), int64(a), int64(s.cfg.StartBlock), s.cfg.ConfigHash)
		if err != nil {
			return fmt.Errorf("advance deposit checkpoint: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return errStaleState
		}
	}

	if err := tx.Commit(ctx); err != nil {
		// Unknown COMMIT outcome: never assume either way. Re-read the durable
		// progress; if this unit's advance (b+1) is visible the commit landed
		// and the loop continues idempotently, otherwise the error is
		// returned and the exact guard re-adjudicates on retry (R2 step 5).
		resolved, rerr := s.commitResultVisible(ctx, a, b)
		if rerr == nil && resolved {
			return nil
		}
		if rerr != nil {
			return fmt.Errorf("commit deposit unit [%d,%d] (outcome unknown, progress re-read failed: %v): %w", a, b, rerr, err)
		}
		return fmt.Errorf("commit deposit unit [%d,%d] (outcome unknown, progress unchanged): %w", a, b, err)
	}
	return nil
}

// exemptOwnedOrphan scopes the 006-owned re-read exemption (T015): only a
// row orphaned under the captured recovery identity passes, and only past
// the status check — content is still compared by the caller, and the row is
// never overwritten. Ordinary calls (Owned == nil) never reach here exempt.
func (s *DepositScanner) exemptOwnedOrphan(ctx context.Context, tx pgx.Tx, rcap RecoveryCapture, id depositIdentity) (bool, error) {
	if rcap.Owned == nil {
		return false, nil
	}
	var scope string
	err := tx.QueryRow(ctx, readDepositOrphanScopeSQL,
		s.cfg.ChainID, id.blockHash, id.txHash, int64(id.logIndex)).Scan(&scope)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("re-read orphan scope %s: %w", id, err)
	}
	return scope != "" && scope == rcap.Owned.RecoveryID, nil
}

// commitResultVisible re-reads deposit_checkpoint after an uncertain COMMIT
// and reports whether the unit's advance is durably visible.
func (s *DepositScanner) commitResultVisible(ctx context.Context, a, b uint64) (bool, error) {
	var (
		start, next int64
		hash        string
	)
	err := s.pool.QueryRow(ctx, readDepositCheckpointSQL, s.cfg.ChainID).Scan(&start, &hash, &next)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	}
	if uint64(start) != s.cfg.StartBlock || hash != s.cfg.ConfigHash {
		return false, nil
	}
	return uint64(next) == b+1, nil
}

// verifyUpstreamCoverageUnderLock re-proves R2 under the coordination lock:
// the 003 checkpoint exists, names the same chain and whitelist identity and
// still has next_block > b.
func (s *DepositScanner) verifyUpstreamCoverageUnderLock(ctx context.Context, q depositQuerier, b uint64) error {
	var (
		chainID, start, next int64
		hash                 string
	)
	err := q.QueryRow(ctx, readUpstreamCheckpointSQL, s.cfg.ChainID).Scan(&chainID, &start, &hash, &next)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: log_checkpoint row for chain %d is missing", errDepositCoverageLost, s.cfg.ChainID)
	}
	if err != nil {
		return fmt.Errorf("coverage re-proof: %w", err)
	}
	if chainID != s.cfg.ChainID {
		return fmt.Errorf("%w: row chain_id=%d, configured %d", errDepositCoverageLost, chainID, s.cfg.ChainID)
	}
	if hash != s.upstreamHash {
		return fmt.Errorf("%w: persisted config_hash=%s, recomputed %s", errDepositCoverageLost, hash, s.upstreamHash)
	}
	if uint64(next) <= b {
		return fmt.Errorf("%w: next_block=%d does not cover b=%d", errDepositCoverageLost, next, b)
	}
	return nil
}

// readjudicateCanonicalRange re-reads every height of [a,b] under the lock and
// requires it to still be canonical with the hash observed during the R2 read.
// A missing or changed height is a chain-view divergence; the loop stops and
// T014 persists the pause (I4).
func (s *DepositScanner) readjudicateCanonicalRange(ctx context.Context, q depositQuerier, u *depositUnit, a, b uint64) error {
	for n := a; ; n++ {
		var stored string
		err := q.QueryRow(ctx, canonicalBlockHashSQL, s.cfg.ChainID, int64(n)).Scan(&stored)
		switch {
		case err == nil:
			want, ok := u.canonical[n]
			if !ok || stored != want {
				return &chainViewError{height: n, expected: want, actual: stored}
			}
		case errors.Is(err, pgx.ErrNoRows):
			return &chainViewError{height: n, absent: true}
		default:
			return fmt.Errorf("canonical re-adjudication at height %d: %w", n, err)
		}
		if n == b {
			return nil
		}
	}
}

// Deposit commit SQL: parameterized statements of the unified write protocol
// (data-model §写事务协议 step 5).
const (
	// insertDepositObservationSQL dedupes on the Table 1 identity; content is
	// verified by the subsequent re-read and never overwritten.
	insertDepositObservationSQL = `
INSERT INTO deposit_observations
    (chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount, version_seq)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (chain_id, block_hash, tx_hash, log_index) DO NOTHING`

	// readDepositObservationSQL re-reads one identity's stored content for the
	// step-5 conflict comparison. version_seq is deliberately not selected:
	// the version reference is never part of the comparison (a replay keeps
	// the original value). $5 is the expected amount as NUMERIC, so equality
	// is by value, not by text rendering.
	readDepositObservationSQL = `
SELECT contract, block_number, sender, recipient, amount::text, status, (amount = $5)
FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4`

	// insertDepositBootstrapHistorySQL creates the first version row in the
	// same transaction as the first checkpoint row (data-model Table 4):
	// version_seq=1, prev_seq NULL, request_id NULL, operator=bootstrap,
	// replay_from=start_block.
	insertDepositBootstrapHistorySQL = `
INSERT INTO deposit_config_history
    (chain_id, version_seq, config_hash, prev_seq, start_block, assets, watches, replay_from, operator, request_id)
VALUES ($1, 1, $2, NULL, $3, $4, $5, $3, 'bootstrap', NULL)`

	// readDepositOrphanScopeSQL names the recovery an orphaned row belongs
	// to (T015 exemption scope). It is deliberately separate from
	// readDepositObservationSQL, whose pinned shape must not widen.
	readDepositOrphanScopeSQL = `
SELECT COALESCE(orphan_recovery_id, '') FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4`

	// insertDepositCheckpointSQL atomically creates the progress row on the
	// first unit; zero rows means stale.
	insertDepositCheckpointSQL = `
INSERT INTO deposit_checkpoint (chain_id, start_block, config_hash, next_block)
VALUES ($1, $2, $3, $4)
ON CONFLICT (chain_id) DO NOTHING`

	// advanceDepositCheckpointSQL advances only from exactly a with the frozen
	// configuration identity: next_block=$2 (b+1), guard next_block=$3 (a),
	// start_block=$4, config_hash=$5. Zero rows is stale.
	advanceDepositCheckpointSQL = `
UPDATE deposit_checkpoint
SET next_block = $2, updated_at = now()
WHERE chain_id = $1 AND start_block = $4 AND config_hash = $5 AND next_block = $3`
)

// pgxPoolQuerier documents that *pgxpool.Pool satisfies depositQuerier; the
// commit itself uses a pgx.Tx for every adjudication read.
var _ depositQuerier = (*pgxpool.Pool)(nil)
