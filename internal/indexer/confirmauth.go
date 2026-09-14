// confirmauth.go implements the 005 privileged confirmation-policy switch
// transaction (T024): the transaction half of
// specs/005-confirmation-tracking/data-model.md §授权切换协议 (FR-03/Q2,
// research R3/R7), driven by a DB operator over a direct SQL connection.
//
// Carrier decision (mirrors 004 T024, see specs/004-deposit-detection/
// tasks.md:91 and depositauth.go): a controlled SQL script — a
// repository-owned, version-controlled sequence of parameterized SQL
// statements executed in one explicit BEGIN..COMMIT from this module over the
// DB operator's connection. Deliberately NOT an endpoint, service, role, or
// in-database (PL/pgSQL) function:
//
//   - No new endpoint/service/role exists: the operator entry point is this
//     module's AuthorizeConfirmationPolicy, executed with the DB operator
//     role outside every serve loop (data-model §授权切换协议 入口与角色).
//   - Every guard (old-seq expectation, new-value legality and difference,
//     under-lock re-verification, request_id classification) runs inside the
//     same transaction's SQL — there is no second entry path that bypasses
//     them (entry uniqueness by construction).
//   - Caller-supplied values never decide correctness, only request identity;
//     the database rows under the coordination lock are the authority.
//
// Protocol shape (data-model §授权切换协议 steps 1-5):
//
//  1. Pre-tx request_id classification by (chain_id, request_id): hit with the
//     same intent returns the recorded result; hit with a different intent is
//     refused; a miss is an independent request. An explicitly failed switch
//     writes nothing, so its request_id stays unbound and retryable.
//  2. Parse the new threshold with the Q1 rules (required positive integer in
//     [1, MaxInt64] via ParseUint, no default) -> require expected_old_seq ==
//     current max seq (else stale/expired refusal) -> require the new
//     threshold to differ from the current one (else empty-authorization
//     refusal; H' must differ, so every seq advance implies a threshold
//     change).
//  3. BEGIN -> ensure the indexer_lease row (shared ensureLeaseSQL) ->
//     SELECT ... FOR UPDATE on it (shared lockCoordSQL; owner/token checks are
//     SKIPPED on this privileged path — the executor is the DB operator, not
//     the lease holder; the operator lands in the audit columns instead).
//  4. Re-verify under the lock with independent statements: a source version
//     must exist (switch-with-empty-table is refused — the first row belongs
//     to the first-confirm transaction, never to a switch); max seq must
//     still equal expected_old_seq; then a single-row INSERT of the new
//     policy row (seq=max+1, prev_seq=old, operator/reason/request_id/
//     expected_old_seq) -> COMMIT. One statement is atomic: failure leaves no
//     partial row and no second version.
//  5. Unknown COMMIT outcome is resolved by re-reading (chain_id, request_id):
//     same-intent hit -> recorded result; miss -> retryable commit error;
//     different-intent hit -> refusal with zero state change.
//
// This transaction never writes pause rows of any stream (asserted in tests).
// Observability follows contracts/observability.md: the outcome hook feeds
// txharbor_confirmation_policy_transition_total{ok|rejected} (detail lives in
// the DB history row; the counter is only the index), and the switch log
// carries chain_id, request_id, operator, old_seq, old/new_threshold,
// result(ok|rejected) and reason.
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
	// ErrConfirmAuthExpired marks a switch whose expected_old_seq no longer
	// equals the current max policy_seq. The refusal reports both versions
	// and changes no state (data-model §授权切换协议 steps 2/4).
	ErrConfirmAuthExpired = errors.New("confirmation auth: authorization expired")

	// ErrConfirmAuthRejected marks the deterministic, state-preserving
	// refusals: unparseable/illegal threshold, empty change (H' == H),
	// switch with no source version, or the same request_id recorded with a
	// different intent.
	ErrConfirmAuthRejected = errors.New("confirmation auth: rejected")
)

// ConfirmAuthRequest is one operator policy-switch intent. The intent fields
// (request_id, expected_old_seq, new threshold, operator, reason) are compared
// verbatim when a duplicate request_id is classified; derived results (new
// policy_seq) are never part of that comparison (mirrors 004, narrowed: no
// replay/pause-disposition columns exist on this path).
type ConfirmAuthRequest struct {
	ChainID int64
	// RequestID binds the request identity (chain_id, request_id) alone,
	// never the target threshold.
	RequestID string

	// ExpectedOldSeq is the policy_seq the caller based this switch on; it
	// must still be the max seq under the lock, otherwise the switch is
	// expired (never "close enough").
	ExpectedOldSeq int64

	// NewThresholdRaw is the new threshold N in Q1 form: a required positive
	// decimal integer in [1, MaxInt64] with no default. It is parsed here,
	// never trusted as a number from the caller.
	NewThresholdRaw string

	Operator string
	Reason   string
}

// ConfirmAuthResult is the outcome of a switch, whether executed now or read
// back for a duplicate request_id.
type ConfirmAuthResult struct {
	PolicySeq int64 // established (chain_id, policy_seq)
	Threshold int64 // threshold of the established row
	Recorded  bool  // true: read back from history, not executed
}

// AuthorizeConfirmationPolicy runs the privileged policy-switch transaction
// for one operator request. It owns BEGIN..COMMIT; the caller passes no
// transaction (data-model.md §授权切换协议, T024).
func AuthorizeConfirmationPolicy(ctx context.Context, pool *pgxpool.Pool, req ConfirmAuthRequest) (ConfirmAuthResult, error) {
	res, err := authorizeConfirmationPolicy(ctx, pool, req)
	observeConfirmAuthResult(res, err)
	return res, err
}

func authorizeConfirmationPolicy(ctx context.Context, pool *pgxpool.Pool, req ConfirmAuthRequest) (ConfirmAuthResult, error) {
	if pool == nil {
		return ConfirmAuthResult{}, errors.New("confirmation auth: nil pool")
	}
	if req.ChainID <= 0 {
		return ConfirmAuthResult{}, fmt.Errorf("confirmation auth: %w: chain id %d must be > 0", ErrConfirmAuthRejected, req.ChainID)
	}
	if req.RequestID == "" {
		return ConfirmAuthResult{}, fmt.Errorf("confirmation auth: %w: empty request_id", ErrConfirmAuthRejected)
	}
	if req.Operator == "" || req.Reason == "" {
		return ConfirmAuthResult{}, fmt.Errorf("confirmation auth: %w: operator and reason are audit fields and must both be set", ErrConfirmAuthRejected)
	}

	// Step 2 (parse): Q1 rules — required positive integer, no default,
	// storage-capped at MaxInt64 (mirrors parseConfirmationDepth).
	newN, err := parseConfirmThreshold(req.NewThresholdRaw)
	if err != nil {
		return ConfirmAuthResult{}, fmt.Errorf("confirmation auth: %w: %v", ErrConfirmAuthRejected, err)
	}

	// Step 1 (classify) needs the current max row first: with no source
	// version the switch is refused outright — the first row belongs to the
	// first-confirm transaction (§首确认协议), never to a switch. The
	// refusal happens before any transaction opens, so the request_id stays
	// unbound and a later bootstrap + retry with the same ID is unaffected.
	current, err := readConfirmPolicy(ctx, pool, req.ChainID)
	if err != nil {
		return ConfirmAuthResult{}, err
	}
	if current == nil {
		return ConfirmAuthResult{}, fmt.Errorf("confirmation auth: %w: chain %d has no source version; the first row is created only by the first-confirm transaction",
			ErrConfirmAuthRejected, req.ChainID)
	}
	hit, same, err := classifyConfirmAuthRequest(ctx, pool, req, newN)
	if err != nil {
		return ConfirmAuthResult{}, err
	}
	if hit != nil {
		if same {
			return ConfirmAuthResult{PolicySeq: hit.policySeq, Threshold: hit.threshold, Recorded: true}, nil
		}
		return ConfirmAuthResult{}, fmt.Errorf("confirmation auth: %w: request_id %q was recorded with a different intent",
			ErrConfirmAuthRejected, req.RequestID)
	}

	// Step 2 (gates, pre-tx): expiry by seq identity, then empty change by
	// threshold inequality. A failed switch writes nothing, so its
	// request_id stays unbound and retryable with corrected params.
	if req.ExpectedOldSeq != current.policySeq {
		return ConfirmAuthResult{}, fmt.Errorf("confirmation auth: %w: expected policy_seq %d but the latest is %d",
			ErrConfirmAuthExpired, req.ExpectedOldSeq, current.policySeq)
	}
	if int64(newN) == current.threshold {
		return ConfirmAuthResult{}, fmt.Errorf("confirmation auth: %w: empty authorization (new threshold equals current %d); a switch must change the threshold",
			ErrConfirmAuthRejected, current.threshold)
	}

	// Step 3: BEGIN a short transaction and take the chain-wide coordination
	// lock. Owner/token checks are skipped: the executor is the DB operator,
	// not the lease holder; mutual exclusion with confirm commits comes from
	// the lock itself. Shared writeGuard/ensureLeaseSQL/lockCoordSQL — never
	// copied SQL text.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ConfirmAuthResult{}, fmt.Errorf("begin confirmation auth transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after COMMIT
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return ConfirmAuthResult{}, fmt.Errorf("confirmation auth transaction statement guard: %w", err)
	}
	if _, err := tx.Exec(ctx, ensureLeaseSQL, req.ChainID, "confirm-auth", 0, float64(3600)); err != nil {
		return ConfirmAuthResult{}, fmt.Errorf("ensure auth coordination row: %w", err)
	}
	var (
		owner string
		token int64
		valid bool
	)
	if err := tx.QueryRow(ctx, lockCoordSQL, req.ChainID).Scan(&owner, &token, &valid); err != nil {
		return ConfirmAuthResult{}, fmt.Errorf("lock auth coordination row: %w", err)
	}

	// Step 4: independent re-verification under the lock. Any failure rolls
	// back with zero state change (no new row, no pause row, no request_id
	// binding).
	locked, err := readConfirmPolicy(ctx, tx, req.ChainID)
	if err != nil {
		return ConfirmAuthResult{}, err
	}
	if locked == nil {
		return ConfirmAuthResult{}, fmt.Errorf("confirmation auth: %w: no source version under the lock; refusing bootstrap-via-switch",
			ErrConfirmAuthRejected)
	}
	if locked.policySeq != req.ExpectedOldSeq {
		return ConfirmAuthResult{}, fmt.Errorf("confirmation auth: %w: expected policy_seq %d but the latest is %d",
			ErrConfirmAuthExpired, req.ExpectedOldSeq, locked.policySeq)
	}
	if locked.threshold == int64(newN) {
		return ConfirmAuthResult{}, fmt.Errorf("confirmation auth: %w: empty authorization under the lock (current threshold %d)",
			ErrConfirmAuthRejected, locked.threshold)
	}
	newSeq := locked.policySeq + 1
	tag, err := tx.Exec(ctx, insertConfirmPolicySQL,
		req.ChainID, newSeq, int64(newN), locked.policySeq, req.Operator, req.Reason, req.RequestID, req.ExpectedOldSeq)
	if err != nil {
		if isUniqueViolation(err) {
			// A concurrent duplicate won the race: roll back and
			// re-classify by request_id with the database as authority.
			_ = tx.Rollback(ctx)
			return resolveConfirmAuthRace(ctx, pool, req, newN)
		}
		return ConfirmAuthResult{}, fmt.Errorf("insert confirmation policy history: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ConfirmAuthResult{}, fmt.Errorf("insert confirmation policy history affected %d rows, want 1", tag.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		// Step 5: uncertain COMMIT — the database is the authority. A
		// same-intent hit committed; a different-intent hit is refused with
		// zero state change; a miss returns the commit error for a
		// recalculated retry (the ID was never bound).
		if hit, same, qerr := classifyConfirmAuthRequest(ctx, pool, req, newN); qerr == nil && hit != nil {
			if same {
				return ConfirmAuthResult{PolicySeq: hit.policySeq, Threshold: hit.threshold, Recorded: true}, nil
			}
			return ConfirmAuthResult{}, fmt.Errorf("confirmation auth: %w: request_id %q was recorded with a different intent",
				ErrConfirmAuthRejected, req.RequestID)
		}
		return ConfirmAuthResult{}, fmt.Errorf("commit confirmation auth (outcome unknown, no recorded request): %w", err)
	}
	return ConfirmAuthResult{PolicySeq: newSeq, Threshold: int64(newN)}, nil
}

// parseConfirmThreshold applies the Q1 rules to a required threshold string:
// non-empty decimal integer, > 0, <= MaxInt64 (the BIGINT system range, not a
// business cap). There is no default — blank is refused.
func parseConfirmThreshold(raw string) (uint64, error) {
	if raw == "" {
		return 0, errors.New("threshold is required (no default)")
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a decimal integer", raw)
	}
	if n == 0 {
		return 0, errors.New("threshold must be a positive decimal integer (> 0)")
	}
	if n > maxConfirmationThreshold {
		return 0, fmt.Errorf("%q exceeds system-supported integer range (max %d)", raw, maxConfirmationThreshold)
	}
	return n, nil
}

// confirmPolicyRow is the current effective policy: the MAX(policy_seq) row,
// the single-row authority (data-model Table 2, research R3).
type confirmPolicyRow struct {
	policySeq int64
	threshold int64
}

// readConfirmPolicy reads the effective policy row (MAX policy_seq); nil with
// nil error means the table holds no row for this chain.
func readConfirmPolicy(ctx context.Context, q depositQuerier, chainID int64) (*confirmPolicyRow, error) {
	var (
		seq, threshold int64
	)
	err := q.QueryRow(ctx, readConfirmPolicySQL, chainID).Scan(&seq, &threshold)
	switch {
	case err == nil:
		return &confirmPolicyRow{policySeq: seq, threshold: threshold}, nil
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	default:
		return nil, fmt.Errorf("confirmation auth read effective policy: %w", err)
	}
}

// confirmAuthRecordHit is one history row addressed by (chain_id, request_id).
type confirmAuthRecordHit struct {
	policySeq      int64
	prevSeq        *int64
	threshold      int64
	operator       string
	reason         string
	expectedOldSeq int64
}

// classifyConfirmAuthRequest finds the request by identity alone (never by
// target threshold) and reports whether the recorded intent equals the
// caller's. Derived results (new policy_seq) are never compared.
func classifyConfirmAuthRequest(ctx context.Context, q depositQuerier, req ConfirmAuthRequest, newN uint64) (*confirmAuthRecordHit, bool, error) {
	hit := &confirmAuthRecordHit{}
	err := q.QueryRow(ctx, readConfirmAuthRequestSQL, req.ChainID, req.RequestID).Scan(
		&hit.policySeq, &hit.prevSeq, &hit.threshold, &hit.operator, &hit.reason, &hit.expectedOldSeq)
	switch {
	case err == nil:
	case errors.Is(err, pgx.ErrNoRows):
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("confirmation auth classify request: %w", err)
	}
	same := hit.prevSeq != nil && *hit.prevSeq == req.ExpectedOldSeq &&
		hit.threshold == int64(newN) &&
		hit.operator == req.Operator &&
		hit.reason == req.Reason
	return hit, same, nil
}

// resolveConfirmAuthRace re-classifies after a request_id UNIQUE conflict:
// the winner's row decides, with the database as the authority.
func resolveConfirmAuthRace(ctx context.Context, pool *pgxpool.Pool, req ConfirmAuthRequest, newN uint64) (ConfirmAuthResult, error) {
	hit, same, err := classifyConfirmAuthRequest(ctx, pool, req, newN)
	if err != nil {
		return ConfirmAuthResult{}, err
	}
	if hit != nil {
		if same {
			return ConfirmAuthResult{PolicySeq: hit.policySeq, Threshold: hit.threshold, Recorded: true}, nil
		}
		return ConfirmAuthResult{}, fmt.Errorf("confirmation auth: %w: request_id %q was recorded with a different intent",
			ErrConfirmAuthRejected, req.RequestID)
	}
	current, err := readConfirmPolicy(ctx, pool, req.ChainID)
	if err != nil {
		return ConfirmAuthResult{}, err
	}
	cur := int64(-1)
	if current != nil {
		cur = current.policySeq
	}
	if current == nil || cur != req.ExpectedOldSeq {
		return ConfirmAuthResult{}, fmt.Errorf("confirmation auth: %w: expected policy_seq %d but the latest is %d",
			ErrConfirmAuthExpired, req.ExpectedOldSeq, cur)
	}
	return ConfirmAuthResult{}, fmt.Errorf("confirmation auth request_id %q conflicted without a recorded row; retry", req.RequestID)
}

// confirmAuthObserver, when non-nil, receives one call per switch terminal
// outcome behind txharbor_confirmation_policy_transition_total
// (contracts/observability.md: ok|rejected; the detail stays in the DB
// history row, the counter is only the index). It keeps the transaction
// independent of the metrics registry; operator tooling wires it. A nil
// observer (the default) disables counting.
var confirmAuthObserver func(result string)

// SetConfirmAuthObserver wires the switch-outcome counter hook.
func SetConfirmAuthObserver(observe func(result string)) {
	confirmAuthObserver = observe
}

// observeConfirmAuthResult classifies one terminal outcome for the hook:
// committed ok, deterministic refusals (rejected, expired) rejected,
// everything else (DB failures, unknown outcomes) error.
func observeConfirmAuthResult(res ConfirmAuthResult, err error) {
	if confirmAuthObserver == nil {
		return
	}
	_ = res
	switch {
	case err == nil:
		confirmAuthObserver("ok")
	default:
		if errors.Is(err, ErrConfirmAuthRejected) || errors.Is(err, ErrConfirmAuthExpired) {
			confirmAuthObserver("rejected")
			return
		}
		confirmAuthObserver("error")
	}
}

const (
	// readConfirmPolicySQL reads the effective policy (MAX policy_seq row).
	readConfirmPolicySQL = `
SELECT policy_seq, threshold FROM confirmation_policy_history
WHERE chain_id = $1
ORDER BY policy_seq DESC LIMIT 1`

	// readConfirmAuthRequestSQL classifies one request by identity alone.
	readConfirmAuthRequestSQL = `
SELECT policy_seq, prev_seq, threshold, operator, reason, expected_old_seq
FROM confirmation_policy_history WHERE chain_id = $1 AND request_id = $2`

	// insertConfirmPolicySQL appends exactly one policy version row:
	// seq=max+1, prev_seq=old, full audit set (operator/reason/request_id/
	// expected_old_seq). Coordination (writeGuard/ensureLeaseSQL/
	// lockCoordSQL) reuses the shared scanner.go constants — never copied.
	insertConfirmPolicySQL = `
INSERT INTO confirmation_policy_history
    (chain_id, policy_seq, threshold, prev_seq, operator, reason, request_id, expected_old_seq)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
)
