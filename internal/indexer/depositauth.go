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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
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

	// UpstreamHash and UpstreamContracts are the shared 003 env whitelist
	// truth (research R5), supplied by operator tooling from the environment:
	// the in-transaction re-proof recomputes the whitelist identity from
	// UpstreamContracts and binds it to the persisted log_checkpoint row, so
	// the authorization never consumes a log stream it cannot name. The
	// consumer loop enforces the same check at startup (T005); this binds it
	// inside the authorization transaction as well.
	UpstreamHash      string
	UpstreamContracts []string
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
// T025 implementation: the controlled statement script. The function owns
// BEGIN..COMMIT; every adjudication read is an independent parameterized
// statement, any failure rolls back with no state change.
func AuthorizeDepositConfig(ctx context.Context, pool *pgxpool.Pool, req DepositAuthRequest) (DepositAuthResult, error) {
	if pool == nil {
		return DepositAuthResult{}, errors.New("deposit auth: nil pool")
	}
	if req.ChainID <= 0 {
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: chain id %d must be > 0", ErrAuthRejected, req.ChainID)
	}
	if req.RequestID == "" {
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: empty request_id", ErrAuthRejected)
	}
	if req.Operator == "" || req.Reason == "" {
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: operator and reason are audit fields and must both be set", ErrAuthRejected)
	}
	if (req.ExpectedPauseID == nil) != (req.ExpectedPauseRevision == nil) {
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: expected_pause_id and expected_pause_revision must both be set or both be empty", ErrAuthRejected)
	}
	if !validConfigHash(req.NewConfigHash) {
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: new config hash %q is not 64 lowercase hex", ErrAuthRejected, req.NewConfigHash)
	}
	if req.UpstreamHash != "" && !validConfigHash(req.UpstreamHash) {
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: upstream hash %q is not 64 lowercase hex", ErrAuthRejected, req.UpstreamHash)
	}

	// Step 1, integrity triage: both sides exist or neither does, and the
	// checkpoint agrees with the latest history row. Corruption is reported
	// and never repaired; it still does not block the read-back below.
	progress, latest, corruptErr := readAuthProgress(ctx, pool, req.ChainID)
	if corruptErr != nil {
		// A recorded result stays readable under corruption (no transaction,
		// no re-execution); a new execution is refused.
		if hit, same, qerr := classifyAuthRequest(ctx, pool, req); qerr == nil && hit != nil {
			if same {
				return recordedAuthResult(ctx, pool, req, hit), nil
			}
			return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: request_id %q was recorded with a different intent",
				ErrAuthRejected, req.RequestID)
		}
		return DepositAuthResult{}, corruptErr
	}
	if progress == nil {
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: chain %d has no source version; the first version is created only by the first unit commit",
			ErrAuthRejected, req.ChainID)
	}

	// Step 1, request classification by (chain_id, request_id) alone.
	hit, same, err := classifyAuthRequest(ctx, pool, req)
	if err != nil {
		return DepositAuthResult{}, err
	}
	if hit != nil {
		if same {
			return recordedAuthResult(ctx, pool, req, hit), nil
		}
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: request_id %q was recorded with a different intent",
			ErrAuthRejected, req.RequestID)
	}

	// Independent request: parse the new configuration snapshots (canonical
	// "address:effective" lines, sorted, non-empty) and re-derive H'. H' is
	// never accepted as the only representation of the change.
	newAssets, err := parseAuthSnapshot(req.NewAssets, "assets")
	if err != nil {
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: %v", ErrAuthRejected, err)
	}
	newWatches, err := parseAuthSnapshot(req.NewWatches, "watches")
	if err != nil {
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: %v", ErrAuthRejected, err)
	}
	derived := depositAuthIdentity(uint64(req.NewStartBlock), newAssets, newWatches)
	if derived != req.NewConfigHash {
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: snapshot re-derives %s, not the claimed %s",
			ErrAuthRejected, derived, req.NewConfigHash)
	}

	// Stored snapshots must still parse: unparseable stored state is
	// corruption, never silently worked around.
	oldAssets, err := parseAuthSnapshot(latest.assets, "stored assets")
	if err != nil {
		return DepositAuthResult{}, &depositCorruptStateError{detail: fmt.Sprintf(
			"chain %d latest history version %d has an unparseable assets snapshot", req.ChainID, latest.versionSeq)}
	}
	oldWatches, err := parseAuthSnapshot(latest.watches, "stored watches")
	if err != nil {
		return DepositAuthResult{}, &depositCorruptStateError{detail: fmt.Sprintf(
			"chain %d latest history version %d has an unparseable watches snapshot", req.ChainID, latest.versionSeq)}
	}

	// Expiry and empty-change gates are evaluated before any proof work: the
	// version check is by seq identity, never "close enough", and the content
	// check is by hash equality.
	if req.ExpectedOldSeq != latest.versionSeq {
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: expected version_seq %d but the latest is %d",
			ErrAuthExpired, req.ExpectedOldSeq, latest.versionSeq)
	}
	if req.NewConfigHash == progress.configHash {
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: empty change (H' == H); an authorization must change the identity",
			ErrAuthRejected)
	}

	// Upstream bind: the persisted 003 row must exist for this chain, match
	// the env-derived identity, and still cover the consumed position. The
	// whitelist membership set comes from the same env list.
	upHash, err := authUpstreamIdentity(req.UpstreamContracts)
	if err != nil {
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: %v", ErrAuthRejected, err)
	}
	if req.UpstreamHash != "" && upHash != req.UpstreamHash {
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: upstream contracts recompute %s, not the supplied %s",
			ErrAuthRejected, upHash, req.UpstreamHash)
	}
	up, err := readAuthUpstream(ctx, pool, req.ChainID, upHash, progress.nextBlock)
	if err != nil {
		return DepositAuthResult{}, err
	}

	// Replay position (data-model §回放位置计算, T025 scope): min over the
	// current next and the affected-combination effective starts. Gap-start
	// candidates and the shrink-boundary rules are T026's extension point.
	replay, affected := depositAuthReplayFrom(uint64(req.NewStartBlock), latest.startBlock,
		oldAssets, oldWatches, newAssets, newWatches, progress.nextBlock, req.UpstreamContracts)
	// Feasibility only (never trim to fit): history below the upstream start
	// or an affected asset the upstream never indexed is structural.
	if replay < up.startBlock {
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: structural gap: replay_from %d is below the upstream start %d",
			ErrAuthRejected, replay, up.startBlock)
	}
	for _, asset := range affected.assets {
		if _, ok := affected.whitelist[asset]; !ok {
			return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: structural gap: affected asset %s is not indexed upstream",
				ErrAuthRejected, asset)
		}
	}

	// Pause basis (data-model 授权协议步骤 1): upstream pauses refuse flatly;
	// a deposit pause is must-dispose (needs an explicit matching target) when
	// its evidence is in scope and re-proven resolved, retainable otherwise.
	// needs_006 evidence may be retained under a double-empty target but never
	// dissolved by one.
	basis, err := decideAuthPause(ctx, pool, req, progress, up, replay)
	if err != nil {
		return DepositAuthResult{}, err
	}
	// T026 gap fold (data-model §回放位置计算 "未解决适用缺口起点"): a
	// disposed gap was never consumed, so the replay position must include
	// its start or the range would be skipped. basis.lo already covers
	// [min(replay,from), a-1], so the under-lock re-proof stays valid.
	if basis.gapFrom != nil && *basis.gapFrom < replay {
		replay = *basis.gapFrom
	}

	// Step 2: BEGIN a short transaction and take the chain-wide coordination
	// lock. Owner/token checks are skipped: the executor is the DB operator,
	// not the lease holder; mutual exclusion with consumer commits and pause
	// writes comes from the lock itself.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return DepositAuthResult{}, fmt.Errorf("begin deposit auth transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after COMMIT
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return DepositAuthResult{}, fmt.Errorf("deposit auth transaction statement guard: %w", err)
	}
	if _, err := tx.Exec(ctx, ensureAuthLeaseSQL, req.ChainID); err != nil {
		return DepositAuthResult{}, fmt.Errorf("ensure auth coordination row: %w", err)
	}
	var (
		owner string
		token int64
		valid bool
	)
	if err := tx.QueryRow(ctx, lockCoordSQL, req.ChainID).Scan(&owner, &token, &valid); err != nil {
		return DepositAuthResult{}, fmt.Errorf("lock auth coordination row: %w", err)
	}

	// Step 3: independent re-verification under the lock. Any failure rolls
	// back with the identity, position and pause untouched.
	if err := reverifyAuthUnderLock(ctx, tx, req, progress, latest, up, upHash, newAssets, newWatches, replay, basis); err != nil {
		return DepositAuthResult{}, err
	}

	// Step 4: atomic commit. The new seq is latest+1 re-read under the lock;
	// the basis equality above guarantees latest did not move, and the PK
	// settles any residual race.
	newSeq := latest.versionSeq + 1
	var expID, expRev *int64
	if basis.dispose {
		expID, expRev = req.ExpectedPauseID, req.ExpectedPauseRevision
	}
	tag, err := tx.Exec(ctx, insertAuthHistorySQL,
		req.ChainID, newSeq, req.NewConfigHash, latest.versionSeq, int64(req.NewStartBlock),
		req.NewAssets, req.NewWatches, int64(replay), req.Operator, req.Reason, req.RequestID, expID, expRev)
	if err != nil {
		if isUniqueViolation(err) {
			// A concurrent duplicate won the race: roll back and re-classify
			// by request_id with the database as the authority.
			_ = tx.Rollback(ctx)
			return resolveAuthRace(ctx, pool, req)
		}
		return DepositAuthResult{}, fmt.Errorf("insert auth config history: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return DepositAuthResult{}, fmt.Errorf("insert auth config history affected %d rows, want 1", tag.RowsAffected())
	}
	// Checkpoint exact guard on the old basis; the start moves to S_new with
	// the identity (the spec text lists config_hash/next_block; start_block
	// must move as well or the Table 2 both-sides invariant breaks — flagged
	// in the batch report as a spec-text question).
	tag, err = tx.Exec(ctx, advanceAuthCheckpointSQL,
		req.ChainID, req.NewConfigHash, int64(req.NewStartBlock), int64(replay),
		int64(progress.startBlock), progress.configHash, int64(progress.nextBlock))
	if err != nil {
		return DepositAuthResult{}, fmt.Errorf("advance auth checkpoint: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: checkpoint moved under the authorization (expected version_seq %d)",
			ErrAuthExpired, req.ExpectedOldSeq)
	}
	disp := DepositPauseRetained
	if basis.dispose {
		if err := deleteAuthPause(ctx, tx, req, basis.pause, newSeq); err != nil {
			return DepositAuthResult{}, err
		}
		disp = DepositPauseReleased
	}
	if err := tx.Commit(ctx); err != nil {
		// Uncertain COMMIT: the database is the authority. A hit with the
		// same intent committed; a hit with a different intent is refused; a
		// miss returns the commit error for a recalculated retry.
		if hit, same, qerr := classifyAuthRequest(ctx, pool, req); qerr == nil && hit != nil {
			if same {
				return recordedAuthResult(ctx, pool, req, hit), nil
			}
			return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: request_id %q was recorded with a different intent",
				ErrAuthRejected, req.RequestID)
		}
		return DepositAuthResult{}, fmt.Errorf("commit deposit auth (outcome unknown, no recorded request): %w", err)
	}
	return DepositAuthResult{VersionSeq: newSeq, ReplayFrom: int64(replay), Pause: disp}, nil
}

// Auth SQL: the controlled statement script (T024 carrier). Every statement
// is parameterized and independent; the transaction in AuthorizeDepositConfig
// owns BEGIN..COMMIT.
const (
	readAuthCheckpointSQL = `
SELECT start_block, config_hash, next_block FROM deposit_checkpoint WHERE chain_id = $1`

	readAuthLatestHistorySQL = `
SELECT version_seq, start_block, config_hash, assets, watches
FROM deposit_config_history WHERE chain_id = $1 ORDER BY version_seq DESC LIMIT 1`

	readAuthRequestSQL = `
SELECT version_seq, prev_seq, config_hash, start_block, assets, watches,
    replay_from, operator, reason, expected_pause_id, expected_pause_revision
FROM deposit_config_history WHERE chain_id = $1 AND request_id = $2`

	readAuthUpstreamSQL = `
SELECT chain_id, start_block, config_hash, next_block FROM log_checkpoint WHERE chain_id = $1`

	readAuthPauseSQL = `
SELECT pause_id, revision, height, kind, detail FROM deposit_pause WHERE chain_id = $1`

	readAuthPauseAuditReleaseSQL = `
SELECT 1 FROM deposit_pause_audit WHERE chain_id = $1 AND pause_id = $2 AND action = 'release'`

	// ensureAuthLeaseSQL makes sure the coordination row exists for the lock.
	// The placeholder is already expired (seconds in the past), so a later
	// consumer Acquire takes it over normally; the authorization itself never
	// performs owner/token verdicts.
	ensureAuthLeaseSQL = `
INSERT INTO indexer_lease (chain_id, owner_id, fencing_token, expires_at)
VALUES ($1, 'deposit-auth', 0, now() - make_interval(secs => 1))
ON CONFLICT (chain_id) DO NOTHING`

	insertAuthHistorySQL = `
INSERT INTO deposit_config_history
    (chain_id, version_seq, config_hash, prev_seq, start_block, assets, watches,
     replay_from, operator, reason, request_id, expected_pause_id, expected_pause_revision)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`

	// advanceAuthCheckpointSQL moves the checkpoint under the exact guard on
	// the old basis. start_block moves to S_new with the identity: keeping
	// the old S would break the Table 2 both-sides invariant against the new
	// history row (flagged as a spec-text question in the batch report).
	advanceAuthCheckpointSQL = `
UPDATE deposit_checkpoint
SET config_hash = $2, start_block = $3, next_block = $4, updated_at = now()
WHERE chain_id = $1 AND start_block = $5 AND config_hash = $6 AND next_block = $7`

	deleteAuthPauseSQL = `
DELETE FROM deposit_pause WHERE chain_id = $1 AND pause_id = $2 AND revision = $3`

	insertAuthPauseAuditSQL = `
INSERT INTO deposit_pause_audit
    (chain_id, pause_id, revision, action, operator, reason, version_seq, kind, height, detail)
VALUES ($1, $2, $3, 'release', $4, $5, $6, $7, $8, $9)`
)

// authSnapshotEntry is one parsed "address:effective" snapshot line.
type authSnapshotEntry struct {
	address   string
	effective uint64
	line      string
}

// parseAuthSnapshot parses a canonical snapshot text (DepositSnapshot form:
// "address:effective" lines, lexicographic, no trailing newline) into entries.
// The text must already be in canonical order with no duplicates and neither
// collection may be blank; the history write reuses the input text verbatim.
func parseAuthSnapshot(text, what string) ([]authSnapshotEntry, error) {
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("%s snapshot is blank; refusing to authorize", what)
	}
	raw := strings.Split(text, "\n")
	out := make([]authSnapshotEntry, 0, len(raw))
	for _, ln := range raw {
		addr, effRaw, ok := strings.Cut(ln, ":")
		if !ok || !common.IsHexAddress(addr) {
			return nil, fmt.Errorf("%s snapshot line %q is not address:effective", what, ln)
		}
		eff, err := strconv.ParseUint(effRaw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s snapshot line %q has a non-decimal effective height", what, ln)
		}
		addr = strings.ToLower(addr)
		out = append(out, authSnapshotEntry{address: addr, effective: eff,
			line: addr + ":" + strconv.FormatUint(eff, 10)})
	}
	for i := 1; i < len(out); i++ {
		if out[i].line <= out[i-1].line {
			return nil, fmt.Errorf("%s snapshot is not in canonical sorted order (line %d %q)", what, i, raw[i])
		}
	}
	return out, nil
}

// depositAuthIdentity re-derives the R3 configuration identity from canonical
// snapshot lines: SHA-256("deposit:v1\nstart:<S>\n" + "asset:<line>" +
// "watch:<line>"), hex lowercase. The layout is pinned by the R3 standard
// vector unit test; the prefix order matches config.depositIdentity because
// the constant per-collection prefix preserves the snapshot sort order.
func depositAuthIdentity(start uint64, assets, watches []authSnapshotEntry) string {
	lines := make([]string, 0, len(assets)+len(watches))
	for _, e := range assets {
		lines = append(lines, "asset:"+e.line)
	}
	for _, e := range watches {
		lines = append(lines, "watch:"+e.line)
	}
	sum := sha256.Sum256([]byte("deposit:v1\nstart:" + strconv.FormatUint(start, 10) + "\n" + strings.Join(lines, "\n")))
	return hex.EncodeToString(sum[:])
}

// authAffected is the replay scope: the affected asset addresses plus the
// upstream whitelist they are checked against.
type authAffected struct {
	assets    []string
	whitelist map[string]struct{}
}

// depositAuthReplayFrom computes the replay position (data-model §回放位置计算):
// min over the current next and the affected-combination effective starts. A
// combination (asset × watch) is affected when it is new (absent from the old
// snapshots) or when the global start was lowered, in which case every new
// combination is in scope. Pure shrink naturally yields the current next via
// min(). A resolved upstream-gap start folds in afterwards via
// authPauseBasis.gapFrom (T026); shrink-boundary no-retroactivity is a test
// lock (observations are never rewritten or deleted).
func depositAuthReplayFrom(newStart, oldStart uint64, oldAssets, oldWatches,
	newAssets, newWatches []authSnapshotEntry, a uint64, upstreamContracts []string) (uint64, authAffected) {
	old := make(map[string]struct{}, len(oldAssets)*len(oldWatches))
	for _, ac := range oldAssets {
		for _, wc := range oldWatches {
			old[ac.line+"\x00"+wc.line] = struct{}{}
		}
	}
	whitelist := make(map[string]struct{}, len(upstreamContracts))
	for _, c := range upstreamContracts {
		whitelist[strings.ToLower(c)] = struct{}{}
	}
	allNew := newStart < oldStart
	replay := a
	seen := make(map[string]struct{})
	var assets []string
	for _, ac := range newAssets {
		for _, wc := range newWatches {
			key := ac.line + "\x00" + wc.line
			if _, ok := old[key]; ok && !allNew {
				continue
			}
			eff := newStart
			if ac.effective > eff {
				eff = ac.effective
			}
			if wc.effective > eff {
				eff = wc.effective
			}
			if eff < replay {
				replay = eff
			}
			if _, ok := seen[ac.address]; !ok {
				seen[ac.address] = struct{}{}
				assets = append(assets, ac.address)
			}
		}
	}
	return replay, authAffected{assets: assets, whitelist: whitelist}
}

// authUpstreamIdentity recomputes the 003 whitelist identity from the shared
// env list (research R5): the same derivation the consumer loop uses.
func authUpstreamIdentity(contracts []string) (string, error) {
	if len(contracts) == 0 {
		return "", errors.New("empty upstream contract whitelist")
	}
	_, hash, err := config.NormalizeWhitelist(strings.Join(contracts, ","))
	if err != nil {
		return "", fmt.Errorf("recompute upstream identity: %w", err)
	}
	return hash, nil
}

// authLatestVersion is the newest deposit_config_history row needed for the
// basis and the expiry gate.
type authLatestVersion struct {
	versionSeq int64
	startBlock uint64
	configHash string
	assets     string
	watches    string
}

// readAuthProgress reads the checkpoint plus the latest history version and
// enforces the Table 2 integrity (both sides exist or neither does, and their
// (start_block, config_hash) agree). It mirrors DepositScanner.readProgress
// for the operator entry point, which owns no scanner.
func readAuthProgress(ctx context.Context, q depositQuerier, chainID int64) (*depositProgress, *authLatestVersion, error) {
	var (
		start, next int64
		hash        string
	)
	cp := (*depositProgress)(nil)
	err := q.QueryRow(ctx, readAuthCheckpointSQL, chainID).Scan(&start, &hash, &next)
	switch {
	case err == nil:
		cp = &depositProgress{startBlock: uint64(start), configHash: hash, nextBlock: uint64(next)}
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return nil, nil, fmt.Errorf("auth read deposit checkpoint: %w", err)
	}
	var (
		versionSeq, hStart int64
		hHash, assets, watches string
	)
	haveHistory := false
	latest := (*authLatestVersion)(nil)
	err = q.QueryRow(ctx, readAuthLatestHistorySQL, chainID).Scan(&versionSeq, &hStart, &hHash, &assets, &watches)
	switch {
	case err == nil:
		haveHistory = true
		latest = &authLatestVersion{versionSeq: versionSeq, startBlock: uint64(hStart),
			configHash: hHash, assets: assets, watches: watches}
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return nil, nil, fmt.Errorf("auth read deposit config history: %w", err)
	}
	switch {
	case cp == nil && !haveHistory:
		return nil, nil, nil
	case cp == nil:
		return nil, nil, &depositCorruptStateError{detail: fmt.Sprintf(
			"chain %d has deposit_config_history version %d but no deposit_checkpoint row", chainID, versionSeq)}
	case !haveHistory:
		return nil, nil, &depositCorruptStateError{detail: fmt.Sprintf(
			"chain %d has deposit_checkpoint (start_block=%d config_hash=%s next_block=%d) but no deposit_config_history row",
			chainID, cp.startBlock, cp.configHash, cp.nextBlock)}
	case cp.startBlock != uint64(hStart) || cp.configHash != hHash:
		return nil, nil, &depositCorruptStateError{detail: fmt.Sprintf(
			"chain %d checkpoint (start_block=%d config_hash=%s) disagrees with latest history version %d (start_block=%d config_hash=%s)",
			chainID, cp.startBlock, cp.configHash, versionSeq, hStart, hHash)}
	}
	cp.versionSeq = versionSeq
	return cp, latest, nil
}

// authRecordHit is one history row addressed by (chain_id, request_id).
type authRecordHit struct {
	versionSeq       int64
	prevSeq          *int64
	configHash       string
	startBlock       int64
	assets, watches  string
	replayFrom       int64
	operator, reason string
	expID, expRev    *int64
}

// classifyAuthRequest finds the request by identity alone (never by target
// hash) and reports whether the recorded intent equals the caller's. Derived
// results (new seq, replay_from, pause disposition) are never compared.
func classifyAuthRequest(ctx context.Context, q depositQuerier, req DepositAuthRequest) (*authRecordHit, bool, error) {
	hit := &authRecordHit{}
	err := q.QueryRow(ctx, readAuthRequestSQL, req.ChainID, req.RequestID).Scan(
		&hit.versionSeq, &hit.prevSeq, &hit.configHash, &hit.startBlock,
		&hit.assets, &hit.watches, &hit.replayFrom, &hit.operator, &hit.reason,
		&hit.expID, &hit.expRev)
	switch {
	case err == nil:
	case errors.Is(err, pgx.ErrNoRows):
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("auth classify request: %w", err)
	}
	same := hit.prevSeq != nil && *hit.prevSeq == req.ExpectedOldSeq &&
		hit.configHash == req.NewConfigHash &&
		hit.startBlock == int64(req.NewStartBlock) &&
		hit.assets == req.NewAssets &&
		hit.watches == req.NewWatches &&
		hit.operator == req.Operator &&
		hit.reason == req.Reason &&
		equalPauseTarget(hit.expID, hit.expRev, req.ExpectedPauseID, req.ExpectedPauseRevision)
	return hit, same, nil
}

// equalPauseTarget compares two optional (id, revision) pairs verbatim.
func equalPauseTarget(aID, aRev, bID, bRev *int64) bool {
	if (aID == nil) != (bID == nil) || (aRev == nil) != (bRev == nil) {
		return false
	}
	if aID == nil {
		return true
	}
	return *aID == *bID && *aRev == *bRev
}

// recordedAuthResult returns the stored outcome without executing anything.
// version_seq and replay_from are authoritative; the pause disposition is
// derived (a double-empty target retained by definition, otherwise the audit
// release record decides), because the schema stores the target, not the
// verdict.
func recordedAuthResult(ctx context.Context, q depositQuerier, req DepositAuthRequest, hit *authRecordHit) DepositAuthResult {
	res := DepositAuthResult{VersionSeq: hit.versionSeq, ReplayFrom: hit.replayFrom, Recorded: true}
	if req.ExpectedPauseID == nil {
		res.Pause = DepositPauseRetained
		return res
	}
	var one int
	if err := q.QueryRow(ctx, readAuthPauseAuditReleaseSQL, req.ChainID, *req.ExpectedPauseID).Scan(&one); err == nil {
		res.Pause = DepositPauseReleased
		return res
	}
	res.Pause = DepositPauseRetained
	return res
}

// authUpstreamState is the 003 log_checkpoint row bound to the env identity.
type authUpstreamState struct {
	startBlock uint64
	nextBlock  uint64
}

// readAuthUpstream reads and binds the upstream row: this chain's, matching
// the recomputed whitelist identity (drift refuses), still covering the
// consumed position (coverage loss refuses).
func readAuthUpstream(ctx context.Context, q depositQuerier, chainID int64, upHash string, a uint64) (*authUpstreamState, error) {
	var (
		rowChain, start, next int64
		hash                  string
	)
	err := q.QueryRow(ctx, readAuthUpstreamSQL, chainID).Scan(&rowChain, &start, &hash, &next)
	switch {
	case err == nil:
	case errors.Is(err, pgx.ErrNoRows):
		return nil, fmt.Errorf("deposit auth: %w: log_checkpoint row for chain %d is missing",
			ErrAuthRejected, chainID)
	default:
		return nil, fmt.Errorf("auth read upstream checkpoint: %w", err)
	}
	if rowChain != chainID {
		return nil, fmt.Errorf("deposit auth: %w: upstream row chain_id=%d, configured %d",
			ErrAuthRejected, rowChain, chainID)
	}
	if hash != upHash {
		return nil, &upstreamDriftError{persisted: hash, expected: upHash}
	}
	if uint64(next) < a {
		return nil, fmt.Errorf("%w: auth upstream next_block=%d no longer covers next=%d",
			errDepositCoverageLost, next, a)
	}
	return &authUpstreamState{startBlock: uint64(start), nextBlock: uint64(next)}, nil
}

// authPauseRow is the live deposit_pause row the disposition is decided on.
type authPauseRow struct {
	id, rev  int64
	height   int64
	kind     string
	detail   string
}

// authPauseBasis is the step-1 pause decision: dispose (needs the explicit
// matching target) or retain, plus the re-proof low bound. gapFrom carries a
// resolved upstream-gap range start that folds into replay_from (T026;
// data-model §回放位置计算 "未解决适用缺口起点": a disposed gap was never
// consumed, so replay must include it or the range would be skipped).
type authPauseBasis struct {
	pause   *authPauseRow
	dispose bool
	lo      uint64
	gapFrom *uint64
}

// parseGapRange extracts the "gap=<from>-<to>" range from a pause detail line
// (the depositGap.Error() format the seeder writes).
func parseGapRange(detail string) (uint64, uint64, bool) {
	idx := strings.Index(detail, "gap=")
	if idx < 0 {
		return 0, 0, false
	}
	var from, to int64
	if _, err := fmt.Sscanf(detail[idx:], "gap=%d-%d", &from, &to); err != nil || from < 0 || to < 0 || uint64(from) > uint64(to) {
		return 0, 0, false
	}
	return uint64(from), uint64(to), true
}

// decideAuthPause applies the step-1 pause rules on the pre-transaction reads.
// Upstream pauses refuse flatly. A deposit pause whose evidence is in scope
// and re-proven resolved must be disposed with an explicit matching target;
// out-of-scope evidence (including needs_006) may be retained targetless but
// never dissolved by a target.
func decideAuthPause(ctx context.Context, q depositQuerier, req DepositAuthRequest,
	progress *depositProgress, up *authUpstreamState, replay uint64) (*authPauseBasis, error) {
	for _, stream := range []struct {
		name string
		sql  string
	}{
		{"log_pause", logPauseExistsSQL},
		{"indexer_pause", pauseExistsSQL},
	} {
		var one int
		err := q.QueryRow(ctx, stream.sql, req.ChainID).Scan(&one)
		switch {
		case err == nil:
			return nil, fmt.Errorf("deposit auth: %w: %s row exists for chain %d; authorizations wait for the upstream pause to lift",
				ErrAuthRejected, stream.name, req.ChainID)
		case errors.Is(err, pgx.ErrNoRows):
		default:
			return nil, fmt.Errorf("auth read %s: %w", stream.name, err)
		}
	}
	var (
		id, rev, height int64
		kind, detail    string
	)
	err := q.QueryRow(ctx, readAuthPauseSQL, req.ChainID).Scan(&id, &rev, &height, &kind, &detail)
	switch {
	case err == nil:
	case errors.Is(err, pgx.ErrNoRows):
		return &authPauseBasis{lo: replay}, nil
	default:
		return nil, fmt.Errorf("auth read deposit pause: %w", err)
	}
	pause := &authPauseRow{id: id, rev: rev, height: height, kind: kind, detail: detail}
	if strings.Contains(detail, "needs_006") {
		// Evidence requires 006: retainable targetless, never dissolvable.
		if req.ExpectedPauseID != nil {
			return nil, fmt.Errorf("deposit auth: %w: pause %d needs 006 handling; refusing a release target",
				ErrAuthRejected, id)
		}
		return &authPauseBasis{pause: pause, lo: replay}, nil
	}
	lo := replay
	switch kind {
	case "upstream_gap":
		from, to, ok := parseGapRange(detail)
		if !ok {
			return nil, fmt.Errorf("deposit auth: %w: pause %d gap evidence %q cannot be assessed; refusing",
				ErrAuthRejected, id, detail)
		}
		if from < lo {
			lo = from
		}
		if up.nextBlock <= to {
			return nil, fmt.Errorf("deposit auth: %w: pause %d gap %d-%d is still uncovered (N_u=%d)",
				ErrAuthRejected, id, from, to, up.nextBlock)
		}
	case "chain_view_changed", "validation_failed":
	default:
		return nil, fmt.Errorf("deposit auth: %w: pause %d has unexpected kind %q; refusing",
			ErrAuthRejected, id, kind)
	}
	if err := authReproveRange(ctx, q, req.ChainID, lo, progress.nextBlock-1); err != nil {
		return nil, fmt.Errorf("deposit auth: %w: pause %d evidence not resolved: %v",
			ErrAuthRejected, id, err)
	}
	// In scope and resolved: disposal needs the explicit matching target.
	// The resolved gap start folds into the replay position via gapFrom
	// (applied by the caller after this decision).
	if req.ExpectedPauseID == nil {
		return nil, fmt.Errorf("deposit auth: %w: pause %d (%s) must be disposed; re-authorize with a new request_id and the explicit expected_pause target",
			ErrAuthRejected, id, kind)
	}
	if *req.ExpectedPauseID != id || *req.ExpectedPauseRevision != rev {
		return nil, fmt.Errorf("deposit auth: %w: pause target (%d,%d) replaced live (%d,%d); refusing",
			ErrAuthRejected, *req.ExpectedPauseID, *req.ExpectedPauseRevision, id, rev)
	}
	basis := &authPauseBasis{pause: pause, dispose: true, lo: lo}
	if kind == "upstream_gap" {
		if from, _, ok := parseGapRange(detail); ok {
			gf := from
			basis.gapFrom = &gf
		}
	}
	return basis, nil
}

// authReproveRange re-proves [lo, hi] (empty when lo > hi): every height
// canonical in chain_blocks and every source row bound to its canonical hash.
func authReproveRange(ctx context.Context, q depositQuerier, chainID int64, lo, hi uint64) error {
	if lo > hi {
		return nil
	}
	canonical := make(map[uint64]string, hi-lo+1)
	for n := lo; ; n++ {
		var hash string
		err := q.QueryRow(ctx, canonicalBlockHashSQL, chainID, int64(n)).Scan(&hash)
		switch {
		case err == nil:
			canonical[n] = hash
		case errors.Is(err, pgx.ErrNoRows):
			return &chainViewError{height: n, absent: true}
		default:
			return fmt.Errorf("auth canonical re-proof at height %d: %w", n, err)
		}
		if n == hi {
			break
		}
	}
	rows, err := q.Query(ctx, readDepositSourceRowsSQL, chainID, int64(lo), int64(hi))
	if err != nil {
		return fmt.Errorf("auth re-proof source rows: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			number, index int64
			row           depositSourceLog
		)
		if err := rows.Scan(&number, &row.blockHash, &row.txHash, &index, &row.contract,
			&row.topic0, &row.topic1, &row.topic2, &row.data); err != nil {
			return fmt.Errorf("auth re-proof scan source row: %w", err)
		}
		row.blockNumber = uint64(number)
		row.logIndex = uint64(index)
		if want := canonical[row.blockNumber]; row.blockHash != want {
			return &chainViewError{height: row.blockNumber, expected: want, actual: row.blockHash}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("auth re-proof source rows: %w", err)
	}
	return nil
}

// reverifyAuthUnderLock re-runs the step-3 gates on fresh under-lock reads:
// integrity, the captured basis (identity + position + version), the H'
// consistency, the upstream bind with coverage, the re-proof over the basis
// low bound, and the pause re-derivation against the basis snapshot.
func reverifyAuthUnderLock(ctx context.Context, tx pgx.Tx, req DepositAuthRequest,
	progress *depositProgress, latest *authLatestVersion, up *authUpstreamState, upHash string,
	newAssets, newWatches []authSnapshotEntry, replay uint64, basis *authPauseBasis) error {
	current, curLatest, err := readAuthProgress(ctx, tx, req.ChainID)
	if err != nil {
		return err
	}
	if current == nil || curLatest == nil {
		return &depositCorruptStateError{detail: fmt.Sprintf(
			"chain %d deposit state vanished under the authorization lock", req.ChainID)}
	}
	if current.startBlock != progress.startBlock || current.configHash != progress.configHash ||
		current.nextBlock != progress.nextBlock || curLatest.versionSeq != latest.versionSeq {
		return fmt.Errorf("deposit auth: %w: basis moved under the lock (expected version_seq %d)",
			ErrAuthExpired, req.ExpectedOldSeq)
	}
	if derived := depositAuthIdentity(uint64(req.NewStartBlock), newAssets, newWatches); derived != req.NewConfigHash {
		return fmt.Errorf("deposit auth: %w: H' snapshot inconsistent under the lock", ErrAuthRejected)
	}
	if _, err := readAuthUpstream(ctx, tx, req.ChainID, upHash, progress.nextBlock); err != nil {
		return err
	}
	if hi := progress.nextBlock - 1; basis.lo <= hi {
		if err := authReproveRange(ctx, tx, req.ChainID, basis.lo, hi); err != nil {
			return fmt.Errorf("deposit auth: %w: under-lock re-proof failed: %v", ErrAuthRejected, err)
		}
	}
	var live *authPauseRow
	{
		var (
			id, rev, height int64
			kind, detail     string
		)
		err := tx.QueryRow(ctx, readAuthPauseSQL, req.ChainID).Scan(&id, &rev, &height, &kind, &detail)
		switch {
		case err == nil:
			live = &authPauseRow{id: id, rev: rev, height: height, kind: kind, detail: detail}
		case errors.Is(err, pgx.ErrNoRows):
		default:
			return fmt.Errorf("auth re-read deposit pause: %w", err)
		}
	}
	switch {
	case basis.pause == nil && live == nil:
	case basis.pause != nil && live != nil && basis.pause.id == live.id && basis.pause.rev == live.rev:
	default:
		got := "absent"
		if live != nil {
			got = fmt.Sprintf("(%d,%d)", live.id, live.rev)
		}
		want := "absent"
		if basis.pause != nil {
			want = fmt.Sprintf("(%d,%d)", basis.pause.id, basis.pause.rev)
		}
		return fmt.Errorf("deposit auth: %w: pause state changed under the lock (basis %s, live %s)",
			ErrAuthRejected, want, got)
	}
	for _, stream := range []struct {
		name string
		sql  string
	}{
		{"log_pause", logPauseExistsSQL},
		{"indexer_pause", pauseExistsSQL},
	} {
		var one int
		err := tx.QueryRow(ctx, stream.sql, req.ChainID).Scan(&one)
		switch {
		case err == nil:
			return fmt.Errorf("deposit auth: %w: %s row appeared under the lock", ErrAuthRejected, stream.name)
		case errors.Is(err, pgx.ErrNoRows):
		default:
			return fmt.Errorf("auth re-read %s: %w", stream.name, err)
		}
	}
	if basis.dispose {
		if req.ExpectedPauseID == nil || live == nil ||
			*req.ExpectedPauseID != live.id || *req.ExpectedPauseRevision != live.rev {
			return fmt.Errorf("deposit auth: %w: pause target no longer matches under the lock", ErrAuthRejected)
		}
	} else if req.ExpectedPauseID != nil && (live == nil ||
		*req.ExpectedPauseID != live.id || *req.ExpectedPauseRevision != live.rev) {
		return fmt.Errorf("deposit auth: %w: pause target replaced under the lock", ErrAuthRejected)
	}
	if req.NewConfigHash == current.configHash {
		return fmt.Errorf("deposit auth: %w: empty change under the lock", ErrAuthRejected)
	}
	return nil
}

// resolveAuthRace re-classifies after a request_id UNIQUE conflict: the
// winner's row decides, with the database as the authority.
func resolveAuthRace(ctx context.Context, pool *pgxpool.Pool, req DepositAuthRequest) (DepositAuthResult, error) {
	hit, same, err := classifyAuthRequest(ctx, pool, req)
	if err != nil {
		return DepositAuthResult{}, err
	}
	if hit != nil {
		if same {
			return recordedAuthResult(ctx, pool, req, hit), nil
		}
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: request_id %q was recorded with a different intent",
			ErrAuthRejected, req.RequestID)
	}
	progress, latest, err := readAuthProgress(ctx, pool, req.ChainID)
	if err != nil {
		return DepositAuthResult{}, err
	}
	if progress == nil || latest.versionSeq != req.ExpectedOldSeq {
		cur := int64(-1)
		if latest != nil {
			cur = latest.versionSeq
		}
		return DepositAuthResult{}, fmt.Errorf("deposit auth: %w: expected version_seq %d but the latest is %d",
			ErrAuthExpired, req.ExpectedOldSeq, cur)
	}
	return DepositAuthResult{}, fmt.Errorf("deposit auth request_id %q conflicted without a recorded row; retry", req.RequestID)
}

// deleteAuthPause removes the resolved pause row by instance + revision and
// writes its release audit row in the same transaction. Only this conditional
// DELETE (or untouched retention) is allowed on the auth path: never UPDATE,
// never merge.
func deleteAuthPause(ctx context.Context, tx pgx.Tx, req DepositAuthRequest, pause *authPauseRow, newSeq int64) error {
	tag, err := tx.Exec(ctx, deleteAuthPauseSQL, req.ChainID, pause.id, pause.rev)
	if err != nil {
		return fmt.Errorf("delete auth pause row: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("deposit auth: %w: pause (%d,%d) changed under the delete",
			ErrAuthRejected, pause.id, pause.rev)
	}
	tag, err = tx.Exec(ctx, insertAuthPauseAuditSQL, req.ChainID, pause.id, pause.rev,
		req.Operator, req.Reason, newSeq, pause.kind, pause.height, pause.detail)
	if err != nil {
		return fmt.Errorf("insert auth pause audit: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("insert auth pause audit affected %d rows, want 1", tag.RowsAffected())
	}
	return nil
}

// isUniqueViolation reports a PostgreSQL unique-violation (23505): the
// concurrent-duplicate signal for the request_id race.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
