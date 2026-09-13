// depositauth.go declares the 004 privileged configuration-transition
// transaction (FR-06/FR-12, research R11, data-model.md "授权转换事务协议").
// It is the only home of that transaction; no other file issues it inline.
//
// T024 carrier decision (2026-09-13): controlled SQL script. The transaction
// is a repository-owned, version-controlled sequence of parameterized SQL
// statements executed in one explicit BEGIN..COMMIT from this module over the
// DB operator's connection. It is deliberately NOT an in-database (PL/pgSQL)
// transaction function. Rationale:
//
//   - D11 failure injection needs statement-level control: the uncertain
//     COMMIT and mid-protocol failure cases reuse the existing dialer wrapper
//     pattern (logscanCommitDropConn, research R9). A single opaque function
//     call cannot deterministically place a connection drop at COMMIT.
//   - Protocol step 1 is non-transactional Go work (configuration parsing, the
//     H' encoding owned and test-locked by T002, replay_from computation,
//     request identity). A stored function would re-implement the
//     configuration identity in a second language with no equivalent test
//     lock.
//   - Repository convention: migrations/ are pure DDL (no stored-function
//     precedent); every write runs as parameterized Go SQL under the shared
//     indexer_lease lock and the writeGuard statement timeout.
//   - The reviewed, tested and shipped artifact is the artifact executed; a
//     production-side CREATE OR REPLACE FUNCTION could bypass review.
//   - Entry and permissions are unchanged: a privileged SQL operation run with
//     the DB operator role outside the serve loop, no new service, endpoint or
//     infrastructure (research R11).
//
// The carrier is fixed by this file: T025/T026/T027/T028 build on the
// controlled-script boundary above. Until T024 closes they must not assume
// either carrier. AuthorizeDepositConfig below is the only declared surface
// and is not implemented yet (T025).
package indexer

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrAuthNotImplemented marks the T024 skeleton: the boundary is declared,
	// T025 has not implemented the statement script yet.
	ErrAuthNotImplemented = errors.New("deposit auth: transaction not implemented (T024 skeleton)")

	// ErrAuthExpired marks an authorization whose expected_old_seq no longer
	// equals the current latest version_seq. The refusal reports both versions
	// and changes no state (research R11).
	ErrAuthExpired = errors.New("deposit auth: authorization expired")

	// ErrAuthRejected marks the deterministic, state-preserving refusals:
	// corrupted integrity, same request_id with a different intent, empty
	// change (H' == H), stale or mismatched pause target, structural upstream
	// gap, an active upstream pause, and evidence that requires 006.
	ErrAuthRejected = errors.New("deposit auth: rejected")
)

// DepositAuthRequest is one operator authorization intent. The intent fields
// (request_id, expected_old_seq, the new-configuration snapshot and the pause
// target) are compared verbatim when a duplicate request_id is classified;
// derived results (new version_seq, replay_from, pause disposition) are never
// part of that comparison (research R11, data-model.md Table 4).
type DepositAuthRequest struct {
	ChainID   int64
	RequestID string

	// ExpectedOldSeq is the version_seq the caller based this authorization
	// on; it must still be the latest one under the lock, otherwise the
	// authorization is expired (never "close enough").
	ExpectedOldSeq int64

	Operator string
	Reason   string

	// NewConfigHash is H', derived from the T002 canonical encoding. The
	// in-transaction gate re-derives the snapshot columns below; H' is never
	// accepted as the only representation of the change.
	NewConfigHash string
	// NewStartBlock and the two snapshots are exactly what is written to
	// deposit_config_history: the global start block, the `contract:effective`
	// asset lines and the `address:effective` watch lines, each lexicographic
	// with no trailing newline (T002 encoding).
	NewStartBlock int64
	NewAssets     string
	NewWatches    string

	// ExpectedPauseID/Revision bind one explicit release target read from the
	// live pause row. Both nil means "not authorized to dispose of any
	// existing pause" (a non-wildcard), which is not the same as rejection.
	ExpectedPauseID       *int64
	ExpectedPauseRevision *int64
}

// DepositPauseDisposition reports what the transaction did to the pause row;
// it is a derived result, never compared for request idempotency.
type DepositPauseDisposition string

const (
	// DepositPauseRetained: an existing pause row was left untouched (the
	// evidence is outside this authorization's scope; consumption stays
	// stopped). No audit row is written for a retained pause.
	DepositPauseRetained DepositPauseDisposition = "retained"
	// DepositPauseReleased: the explicitly targeted instance was removed by an
	// instance+revision conditional DELETE with its audit row in the same
	// transaction.
	DepositPauseReleased DepositPauseDisposition = "released"
)

// DepositAuthResult is the outcome of an authorization, whether executed now
// or read back for a duplicate request_id.
type DepositAuthResult struct {
	VersionSeq int64                   // established (chain_id, version_seq)
	ReplayFrom int64                   // position committed in the same transaction
	Pause      DepositPauseDisposition // derived pause verdict
	Recorded   bool                    // true: read back from history, not executed
}

// AuthorizeDepositConfig runs the privileged configuration-transition
// transaction for one operator authorization request. It owns BEGIN..COMMIT;
// the caller passes no transaction (data-model.md 授权转换事务协议,
// implemented by T025).
//
// Before BEGIN, read-only and state-preserving: integrity triage of
// deposit_checkpoint versus deposit_config_history; request_id classification
// (identical intent returns the recorded result without opening a transaction,
// a different intent is refused, a miss is an independent request); refusal
// when no version exists; the must-dispose/retain pause decision when no
// target is given; parsing of the new configuration and derivation of H' and
// replay_from; reading of the current (start_block, config_hash, next_block),
// latest version_seq, pause row and upstream (start, hash, next); gap
// classification; coverage and canonical pre-checks; expiry and empty-change
// refusals.
//
// Then BEGIN; SET LOCAL statement_timeout = '5s'; ensure the indexer_lease
// row; SELECT ... FOR UPDATE on it (owner/token checks are skipped: the
// executor is the DB operator, not the lease holder). In independent
// statements under the lock, any failure rolls back with no state change:
//
//   - integrity: checkpoint and history both exist and the checkpoint's
//     (start_block, config_hash) equals the latest history row (a corrupted
//     state is never repaired here);
//   - basis: (start_block, config_hash, next_block) is still the basis read
//     before BEGIN, and latest version_seq still equals expected_old_seq;
//   - H' snapshot consistency with the parsed configuration;
//   - coverage re-proof: upstream next >= current next (coverage was not lost)
//     and every height of [replay_from, a-1] is canonical and matches the
//     source rows (row count equals the interval length);
//   - pause verdict recomputed under the lock: the live (pause_id, revision)
//     must match the basis; a provided expected_pause must match the live row;
//     no target plus must-dispose is refused; no target plus retain leaves the
//     row untouched; an upstream (log_pause/indexer_pause) row or needs-006
//     evidence is refused; only untouched or an instance+revision conditional
//     DELETE with its audit row is allowed, never UPDATE/merge;
//   - empty change (H' == H) is refused.
//
// Atomic commit: new version_seq = latest + 1; INSERT the history row with the
// full audit set (request_id, expected_old_seq, operator, old/new hash,
// replay_from, expected_pause; a missing field rolls back); UPDATE the
// checkpoint under the exact guard (start_block, config_hash, next_block) to
// (H', replay_from), where a row count other than one means stale and rolls
// back; apply the pause disposition; COMMIT.
//
// Idempotency and expiry: request identity is (chain_id, request_id) alone,
// never the target hash. Same id + same intent returns the recorded result
// even after later versions; same id + different intent is refused; a
// different id is always independent. Expiry refusals report the expected and
// the current version. An uncertain COMMIT is resolved by re-reading history
// for the request_id, with the database as the authority; the UNIQUE
// (chain_id, request_id) constraint settles concurrent duplicates.
//
// T024 skeleton: this stub fixes the signature and the boundary above. T025
// replaces the body with the controlled statement script; until then no caller
// may treat the transaction as available.
func AuthorizeDepositConfig(ctx context.Context, pool *pgxpool.Pool, req DepositAuthRequest) (DepositAuthResult, error) {
	return DepositAuthResult{}, ErrAuthNotImplemented
}
