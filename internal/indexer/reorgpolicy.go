// reorgpolicy.go implements the 006 depth-config version ledger (T009):
// the policy half of specs/006-reorg-recovery/data-model.md Table 3
// (FR-03/Q1, research R4), mirroring the reviewed 004/005 authorization
// precedent (depositauth.go, confirmauth.go) instead of inventing a third
// config mechanism.
//
// Carrier decision: a controlled SQL script — a repository-owned,
// version-controlled sequence of parameterized SQL statements executed in one
// explicit BEGIN..COMMIT over the DB operator's connection. Deliberately NOT
// an endpoint, service, role, or in-database function: no new entry path
// exists that bypasses the guards below (entry uniqueness by construction).
// Coordination (writeGuard/ensureLeaseSQL/lockCoordSQL) reuses the shared
// scanner.go constants — never copied.
//
// Protocol shape (data-model Table 3):
//
//  1. Pre-tx request_id classification by (chain_id, request_id): hit with the
//     same intent returns the recorded result; hit with a different intent is
//     refused; a miss is an independent request. An explicitly failed switch
//     writes nothing, so its request_id stays unbound and retryable.
//  2. Parse the new max_depth with the Q1 rules (required positive integer in
//     [1, MaxInt64] via ParseUint, no default) -> require expected_old_seq ==
//     current max seq (else stale/expired refusal) -> require the new depth to
//     differ from the current one (else empty-authorization refusal).
//  3. BEGIN -> ensure the indexer_lease row (shared ensureLeaseSQL) ->
//     SELECT ... FOR UPDATE on it (shared lockCoordSQL; owner/token checks are
//     SKIPPED on this privileged path — the executor is the DB operator, not
//     the lease holder; the operator lands in the audit columns instead).
//  4. Re-verify under the lock with independent statements: a source version
//     must exist (switch-with-empty-table is refused — the first row belongs
//     to the first establish transaction, never to a switch); max seq must
//     still equal expected_old_seq; then a single-row INSERT of the new
//     policy row (seq=max+1, prev_seq=old, operator/reason/request_id/
//     expected_old_seq) -> COMMIT. One statement is atomic: failure leaves no
//     partial row and no second version.
//  5. Unknown COMMIT outcome is resolved by re-reading (chain_id, request_id):
//     same-intent hit -> recorded result; miss -> retryable commit error;
//     different-intent hit -> refusal with zero state change.
//
// Bootstrap/bind for the first establish transaction (Batch B, T011) lives
// here as bindReorgPolicy: with no row it inserts the bootstrap row
// (policy_seq=1, operator='bootstrap', request_id NULL) carrying the env
// depth; with a row it binds the captured seq after the env-vs-effective
// drift check. In-flight recoveries bind the captured policy_seq; a later
// limit raise never widens their authority and never releases a reconcile
// pause (release is only via the Q2b path in T011).
//
// This transaction never writes pause rows of any stream and never touches
// the recovery row. Metric/counter wiring lands in T032; the outcome is
// carried by the DB history row alone here.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrReorgPolicyExpired marks a switch whose expected_old_seq no longer
	// equals the current max policy_seq. The refusal reports both versions
	// and changes no state (data-model Table 3, step 2/4).
	ErrReorgPolicyExpired = errors.New("reorg auth: authorization expired")

	// ErrReorgPolicyRejected marks the deterministic, state-preserving
	// refusals: unparseable/illegal max_depth, empty change (D' == D),
	// switch with no source version, or the same request_id recorded with a
	// different intent.
	ErrReorgPolicyRejected = errors.New("reorg auth: rejected")
)

// ReorgPolicyDriftError is the startup refusal: the effective policy
// (MAX policy_seq row) max_depth differs from the configured env depth.
// The process loud-refuses (mirrors the 005 verifyPolicyIdentity precedent);
// it never follows silently and never repairs the row itself.
type ReorgPolicyDriftError struct{ detail string }

func (e *ReorgPolicyDriftError) Error() string { return "reorg policy drift: " + e.detail }

// ReorgAuthRequest is one operator policy-switch intent. The intent fields
// (request_id, expected_old_seq, new max_depth) are compared verbatim when a
// duplicate request_id is classified; derived results (new policy_seq) are
// never part of that comparison (mirrors 005, narrowed: no threshold
// columns exist on this path).
type ReorgAuthRequest struct {
	ChainID int64
	// RequestID binds the request identity (chain_id, request_id) alone,
	// never the target depth.
	RequestID string

	// ExpectedOldSeq is the policy_seq the caller based this switch on; it
	// must still be the max seq under the lock, otherwise the switch is
	// expired (never "close enough").
	ExpectedOldSeq int64

	// NewMaxDepthRaw is the new max_depth in Q1 form: a required positive
	// decimal integer in [1, MaxInt64] with no default. It is parsed here,
	// never trusted as a number from the caller.
	NewMaxDepthRaw string

	Operator string
	Reason   string
}

// ReorgAuthResult is the outcome of a switch, whether executed now or read
// back for a duplicate request_id.
type ReorgAuthResult struct {
	PolicySeq int64 // established (chain_id, policy_seq)
	MaxDepth  int64 // max_depth of the established row
	Recorded  bool  // true: read back from history, not executed
}

// reorgPolicyRow is the current effective policy: the MAX(policy_seq) row,
// the single-row authority (data-model Table 3, research R4).
type reorgPolicyRow struct {
	policySeq int64
	maxDepth  int64
}

// maxReorgDepth is the system range cap (BIGINT, not a business cap): Q1
// sets no business upper bound; values above MaxInt64 refuse startup.
const maxReorgDepth = uint64(1<<63 - 1)

// ParseReorgMaxDepth applies the Q1 rules to a required max_depth string:
// non-empty decimal integer, > 0, <= MaxInt64. There is no default — blank,
// zero, negative, non-integer, and out-of-representation inputs are refused.
func ParseReorgMaxDepth(raw string) (uint64, error) {
	if raw == "" {
		return 0, errors.New("max_depth is required (no default)")
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a decimal integer", raw)
	}
	if n == 0 {
		return 0, errors.New("max_depth must be a positive decimal integer (> 0)")
	}
	if n > maxReorgDepth {
		return 0, fmt.Errorf("%q exceeds system-supported integer range (max %d)", raw, maxReorgDepth)
	}
	return n, nil
}

// readReorgPolicy reads the effective policy row (MAX policy_seq); nil with
// nil error means the table holds no row for this chain (pre-bootstrap).
func readReorgPolicy(ctx context.Context, q depositQuerier, chainID int64) (*reorgPolicyRow, error) {
	var (
		seq, depth int64
	)
	err := q.QueryRow(ctx, readReorgPolicySQL, chainID).Scan(&seq, &depth)
	switch {
	case err == nil:
		return &reorgPolicyRow{policySeq: seq, maxDepth: depth}, nil
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	default:
		return nil, fmt.Errorf("reorg auth read effective policy: %w", err)
	}
}

// VerifyReorgPolicyIdentity applies the startup comparison to the effective
// policy row: once the row exists its max_depth is frozen and must equal the
// configured env depth. A mismatch is a configuration change, reported and
// never continued silently. No row is the pre-bootstrap state (ok), never an
// error — the first establish transaction writes the bootstrap row.
func VerifyReorgPolicyIdentity(p *reorgPolicyRow, envMaxDepth int64) error {
	if p == nil {
		return nil
	}
	if p.maxDepth != envMaxDepth {
		return &ReorgPolicyDriftError{detail: fmt.Sprintf(
			"chain effective policy (seq=%d max_depth=%d) differs from configured max_depth=%d",
			p.policySeq, p.maxDepth, envMaxDepth)}
	}
	return nil
}

// bindReorgPolicy binds the establish-time policy inside the caller's
// transaction (Batch B, T011): the caller holds the lease lock, so the read
// below is ordered with every concurrent switch. With no row it inserts the
// bootstrap row (policy_seq=1, prev_seq NULL, operator='bootstrap',
// request_id NULL, expected_old_seq=0) carrying the env depth and returns 1;
// with a row it requires env-vs-effective equality (else drift refusal) and
// returns the bound seq. In-flight recoveries keep this bound seq; a later
// raise never widens it.
func bindReorgPolicy(ctx context.Context, tx pgx.Tx, chainID int64, envMaxDepthRaw string) (int64, error) {
	envDepth, err := ParseReorgMaxDepth(envMaxDepthRaw)
	if err != nil {
		return 0, fmt.Errorf("reorg policy bind: %w: %v", ErrReorgPolicyRejected, err)
	}
	current, err := readReorgPolicy(ctx, txAdapter{tx: tx}, chainID)
	if err != nil {
		return 0, err
	}
	if current == nil {
		tag, err := tx.Exec(ctx, insertReorgBootstrapSQL, chainID, int64(envDepth))
		if err != nil {
			return 0, fmt.Errorf("insert reorg policy bootstrap: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return 0, fmt.Errorf("insert reorg policy bootstrap affected %d rows, want 1", tag.RowsAffected())
		}
		return 1, nil
	}
	if err := VerifyReorgPolicyIdentity(current, int64(envDepth)); err != nil {
		return 0, err
	}
	return current.policySeq, nil
}

// txAdapter lets bindReorgPolicy reuse readReorgPolicy's depositQuerier
// shape over a pgx.Tx without opening a second connection.
type txAdapter struct {
	tx pgx.Tx
}

func (a txAdapter) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return a.tx.QueryRow(ctx, sql, args...)
}

func (a txAdapter) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return a.tx.Query(ctx, sql, args...)
}

// AuthorizeReorgPolicy runs the privileged policy-switch transaction for one
// operator request. It owns BEGIN..COMMIT; the caller passes no transaction
// (data-model Table 3, T009).
func AuthorizeReorgPolicy(ctx context.Context, pool *pgxpool.Pool, req ReorgAuthRequest) (ReorgAuthResult, error) {
	return authorizeReorgPolicy(ctx, pool, req)
}

func authorizeReorgPolicy(ctx context.Context, pool *pgxpool.Pool, req ReorgAuthRequest) (ReorgAuthResult, error) {
	if pool == nil {
		return ReorgAuthResult{}, errors.New("reorg auth: nil pool")
	}
	if req.ChainID <= 0 {
		return ReorgAuthResult{}, fmt.Errorf("reorg auth: %w: chain id %d must be > 0", ErrReorgPolicyRejected, req.ChainID)
	}
	if req.RequestID == "" {
		return ReorgAuthResult{}, fmt.Errorf("reorg auth: %w: empty request_id", ErrReorgPolicyRejected)
	}
	if req.Operator == "" || req.Reason == "" {
		return ReorgAuthResult{}, fmt.Errorf("reorg auth: %w: operator and reason are audit fields and must both be set", ErrReorgPolicyRejected)
	}

	// Step 2 (parse): Q1 rules — required positive integer, no default,
	// storage-capped at MaxInt64 (mirrors parseConfirmThreshold).
	newDepth, err := ParseReorgMaxDepth(req.NewMaxDepthRaw)
	if err != nil {
		return ReorgAuthResult{}, fmt.Errorf("reorg auth: %w: %v", ErrReorgPolicyRejected, err)
	}

	// Step 1 (classify) needs the current max row first: with no source
	// version the switch is refused outright — the first row belongs to the
	// first establish transaction, never to a switch. The refusal happens
	// before any transaction opens, so the request_id stays unbound and a
	// later bootstrap + retry with the same ID is unaffected.
	current, err := readReorgPolicy(ctx, pool, req.ChainID)
	if err != nil {
		return ReorgAuthResult{}, err
	}
	if current == nil {
		return ReorgAuthResult{}, fmt.Errorf("reorg auth: %w: chain %d has no source version; the first row is created only by the first establish transaction",
			ErrReorgPolicyRejected, req.ChainID)
	}
	hit, same, err := classifyReorgAuthRequest(ctx, pool, req, newDepth)
	if err != nil {
		return ReorgAuthResult{}, err
	}
	if hit != nil {
		if same {
			return ReorgAuthResult{PolicySeq: hit.policySeq, MaxDepth: hit.maxDepth, Recorded: true}, nil
		}
		return ReorgAuthResult{}, fmt.Errorf("reorg auth: %w: request_id %q was recorded with a different intent",
			ErrReorgPolicyRejected, req.RequestID)
	}

	// Step 2 (gates, pre-tx): expiry by seq identity, then empty change by
	// depth inequality. A failed switch writes nothing, so its request_id
	// stays unbound and retryable with corrected params.
	if req.ExpectedOldSeq != current.policySeq {
		return ReorgAuthResult{}, fmt.Errorf("reorg auth: %w: expected policy_seq %d but the latest is %d",
			ErrReorgPolicyExpired, req.ExpectedOldSeq, current.policySeq)
	}
	if int64(newDepth) == current.maxDepth {
		return ReorgAuthResult{}, fmt.Errorf("reorg auth: %w: empty authorization (new max_depth equals current %d); a switch must change the depth",
			ErrReorgPolicyRejected, current.maxDepth)
	}

	// Step 3: BEGIN a short transaction and take the chain-wide coordination
	// lock. Owner/token checks are skipped: the executor is the DB operator,
	// not the lease holder; mutual exclusion with every writer comes from
	// the lock itself. Shared writeGuard/ensureLeaseSQL/lockCoordSQL — never
	// copied SQL text.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ReorgAuthResult{}, fmt.Errorf("begin reorg auth transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after COMMIT
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return ReorgAuthResult{}, fmt.Errorf("reorg auth transaction statement guard: %w", err)
	}
	if _, err := tx.Exec(ctx, ensureLeaseSQL, req.ChainID, "reorg-auth", 0, float64(3600)); err != nil {
		return ReorgAuthResult{}, fmt.Errorf("ensure auth coordination row: %w", err)
	}
	var (
		owner string
		token int64
		valid bool
	)
	if err := tx.QueryRow(ctx, lockCoordSQL, req.ChainID).Scan(&owner, &token, &valid); err != nil {
		return ReorgAuthResult{}, fmt.Errorf("lock auth coordination row: %w", err)
	}

	// Step 4: independent re-verification under the lock. Any failure rolls
	// back with zero state change (no new row, no pause row, no request_id
	// binding).
	locked, err := readReorgPolicy(ctx, txAdapter{tx: tx}, req.ChainID)
	if err != nil {
		return ReorgAuthResult{}, err
	}
	if locked == nil {
		return ReorgAuthResult{}, fmt.Errorf("reorg auth: %w: no source version under the lock; refusing bootstrap-via-switch",
			ErrReorgPolicyRejected)
	}
	if locked.policySeq != req.ExpectedOldSeq {
		return ReorgAuthResult{}, fmt.Errorf("reorg auth: %w: expected policy_seq %d but the latest is %d",
			ErrReorgPolicyExpired, req.ExpectedOldSeq, locked.policySeq)
	}
	if locked.maxDepth == int64(newDepth) {
		return ReorgAuthResult{}, fmt.Errorf("reorg auth: %w: empty authorization under the lock (current max_depth %d)",
			ErrReorgPolicyRejected, locked.maxDepth)
	}
	newSeq := locked.policySeq + 1
	tag, err := tx.Exec(ctx, insertReorgPolicySQL,
		req.ChainID, newSeq, int64(newDepth), locked.policySeq, req.Operator, req.Reason, req.RequestID, req.ExpectedOldSeq)
	if err != nil {
		if isUniqueViolation(err) {
			// A concurrent duplicate won the race: roll back and
			// re-classify by request_id with the database as authority.
			_ = tx.Rollback(ctx)
			return resolveReorgAuthRace(ctx, pool, req, newDepth)
		}
		return ReorgAuthResult{}, fmt.Errorf("insert reorg policy history: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ReorgAuthResult{}, fmt.Errorf("insert reorg policy history affected %d rows, want 1", tag.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		// Step 5: uncertain COMMIT — the database is the authority. A
		// same-intent hit committed; a different-intent hit is refused with
		// zero state change; a miss returns the commit error for a
		// recalculated retry (the ID was never bound).
		if hit, same, qerr := classifyReorgAuthRequest(ctx, pool, req, newDepth); qerr == nil && hit != nil {
			if same {
				return ReorgAuthResult{PolicySeq: hit.policySeq, MaxDepth: hit.maxDepth, Recorded: true}, nil
			}
			return ReorgAuthResult{}, fmt.Errorf("reorg auth: %w: request_id %q was recorded with a different intent",
				ErrReorgPolicyRejected, req.RequestID)
		}
		return ReorgAuthResult{}, fmt.Errorf("commit reorg auth (outcome unknown, no recorded request): %w", err)
	}
	return ReorgAuthResult{PolicySeq: newSeq, MaxDepth: int64(newDepth)}, nil
}

// reorgAuthRecord is one recorded switch row for request_id classification.
type reorgAuthRecord struct {
	policySeq      int64
	maxDepth       int64
	expectedOldSeq int64
	operator       string
	reason         string
}

// classifyReorgAuthRequest reads the (chain_id, request_id) row, if any, and
// reports whether the recorded intent matches this request verbatim.
func classifyReorgAuthRequest(ctx context.Context, q depositQuerier, req ReorgAuthRequest, newDepth uint64) (*reorgAuthRecord, bool, error) {
	var rec reorgAuthRecord
	err := q.QueryRow(ctx, classifyReorgAuthSQL, req.ChainID, req.RequestID).
		Scan(&rec.policySeq, &rec.maxDepth, &rec.expectedOldSeq, &rec.operator, &rec.reason)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("reorg auth classify request: %w", err)
	}
	same := rec.expectedOldSeq == req.ExpectedOldSeq &&
		rec.maxDepth == int64(newDepth) &&
		rec.operator == req.Operator &&
		rec.reason == req.Reason
	return &rec, same, nil
}

// resolveReorgAuthRace re-classifies after a unique-violation rollback: the
// concurrent winner is the authority now.
func resolveReorgAuthRace(ctx context.Context, pool *pgxpool.Pool, req ReorgAuthRequest, newDepth uint64) (ReorgAuthResult, error) {
	hit, same, err := classifyReorgAuthRequest(ctx, pool, req, newDepth)
	if err != nil {
		return ReorgAuthResult{}, err
	}
	if hit == nil {
		return ReorgAuthResult{}, fmt.Errorf("reorg auth: concurrent insert conflict with no recorded request; retry with a recalculated expected_old_seq")
	}
	if !same {
		return ReorgAuthResult{}, fmt.Errorf("reorg auth: %w: request_id %q was recorded with a different intent",
			ErrReorgPolicyRejected, req.RequestID)
	}
	return ReorgAuthResult{PolicySeq: hit.policySeq, MaxDepth: hit.maxDepth, Recorded: true}, nil
}

const (
	// readReorgPolicySQL reads the effective policy (MAX policy_seq row).
	readReorgPolicySQL = `
SELECT policy_seq, max_depth FROM reorg_policy_history
WHERE chain_id = $1 AND policy_seq = (SELECT MAX(policy_seq) FROM reorg_policy_history WHERE chain_id = $1)`

	// classifyReorgAuthSQL reads one recorded switch by request identity.
	classifyReorgAuthSQL = `
SELECT policy_seq, max_depth, expected_old_seq, operator, reason
FROM reorg_policy_history WHERE chain_id = $1 AND request_id = $2`

	// insertReorgBootstrapSQL writes the first policy row. It carries no
	// request identity (request_id NULL, the single NULL row per chain) and
	// belongs to the first establish transaction only.
	insertReorgBootstrapSQL = `
INSERT INTO reorg_policy_history (chain_id, policy_seq, max_depth, prev_seq, operator, reason, request_id, expected_old_seq)
VALUES ($1, 1, $2, NULL, 'bootstrap', '', NULL, 0)`

	// insertReorgPolicySQL writes one switch row: exactly one INSERT per
	// transaction (single-row atomicity — failure leaves no partial row).
	insertReorgPolicySQL = `
INSERT INTO reorg_policy_history (chain_id, policy_seq, max_depth, prev_seq, operator, reason, request_id, expected_old_seq)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
)
