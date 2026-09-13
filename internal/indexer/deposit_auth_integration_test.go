//go:build integration

// T025 acceptance tests for the privileged configuration-transition
// transaction (AuthorizeDepositConfig, depositauth.go): the H1->H2 happy path
// with its version chain and audit columns, request-id idempotency (recorded
// read-back across later versions, different-intent refusal, independent ids),
// expiry and empty-change gates, the state-preserving refusals (no source
// version, one-sided corruption, must-dispose without a target, a replaced
// target) and the pause dispositions (explicit-target release with its audit
// row, needs_006 retention under a targetless request).
//
// Real PostgreSQL only: the DDL enforces the version chain, the request-id
// unique index and the pause unique instances, and every refusal is asserted
// to leave the checkpoint values, history count, observations, pause row and
// audit count untouched. Chain ids are drawn from 110-119. Upstream rows are
// seeded over the [testContractA, testContractB] whitelist because every
// authorized snapshot must be an asset the upstream actually indexed; H' is
// always re-derived through the production R3 identity (depositAuthIdentity),
// never invented.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
)

// --- helpers ----------------------------------------------------------------

// depositAuthLine renders one canonical "address:effective" snapshot line.
func depositAuthLine(addr string, effective uint64) string {
	return strings.ToLower(addr) + ":" + strconv.FormatUint(effective, 10)
}

// depositAuthSnapshot builds the canonical snapshot text: lexicographically
// sorted lines, no trailing newline (T002 encoding).
func depositAuthSnapshot(lines ...string) string {
	sorted := append([]string(nil), lines...)
	sort.Strings(sorted)
	return strings.Join(sorted, "\n")
}

// depositAuthHash re-derives H' from the snapshot text through the production
// R3 identity, so a test can never supply a hash the transaction would reject
// for an unrelated reason.
func depositAuthHash(t *testing.T, start uint64, assets, watches string) string {
	t.Helper()
	ae, err := parseAuthSnapshot(assets, "assets")
	if err != nil {
		t.Fatalf("parseAuthSnapshot(assets): %v", err)
	}
	we, err := parseAuthSnapshot(watches, "watches")
	if err != nil {
		t.Fatalf("parseAuthSnapshot(watches): %v", err)
	}
	return depositAuthIdentity(start, ae, we)
}

// depositAuthBaseReq is the valid request shape every scenario starts from:
// the shared upstream whitelist is [testContractA, testContractB] so an
// authorized snapshot adding testContractB stays indexed upstream.
func depositAuthBaseReq(t *testing.T, chainID int64, requestID string, oldSeq int64) DepositAuthRequest {
	t.Helper()
	return DepositAuthRequest{
		ChainID:           chainID,
		RequestID:         requestID,
		Operator:          "operator-1",
		Reason:            "test authorization",
		ExpectedOldSeq:    oldSeq,
		UpstreamHash:      depositUpstreamHash(t, testContractA, testContractB),
		UpstreamContracts: []string{testContractA, testContractB},
	}
}

// depositAuthSeedChain plants the base H1 state: canonical [10,next-1], the
// upstream row over [testContractA, testContractB], the checkpoint and the
// bootstrap history row (snapshots testContractA:10 / depositWatchAddr:10).
// It returns H1 and the two canonical snapshot texts.
func depositAuthSeedChain(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, next uint64) (h1, assets, watches string) {
	t.Helper()
	assets = depositAuthLine(testContractA, 10)
	watches = depositAuthLine(depositWatchAddr, 10)
	h1 = depositAuthHash(t, 10, assets, watches)
	contracts := []string{testContractA, testContractB}
	depositSeedCanonical(t, ctx, pool, chainID, 10, next-1, true)
	depositSeedUpstream(t, ctx, pool, chainID, 0, depositUpstreamHash(t, contracts...), next)
	depositSeedCheckpoint(t, ctx, pool, chainID, 10, h1, next)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, h1)
	return h1, assets, watches
}

// depositAuthSeedPause plants one deposit_pause instance and returns its
// (pause_id, revision).
func depositAuthSeedPause(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64,
	kind, detail string, height uint64) (int64, int64) {
	t.Helper()
	var id, rev int64
	if err := pool.QueryRow(ctx, `
INSERT INTO deposit_pause (chain_id, height, kind, detail)
VALUES ($1, $2, $3, $4)
RETURNING pause_id, revision`, chainID, int64(height), kind, detail).Scan(&id, &rev); err != nil {
		t.Fatalf("seed deposit_pause: %v", err)
	}
	return id, rev
}

// depositAuthPause is one live pause row snapshot.
type depositAuthPause struct {
	id, rev, height int64
	kind, detail    string
}

func depositAuthReadPause(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) (depositAuthPause, bool) {
	t.Helper()
	var row depositAuthPause
	err := pool.QueryRow(ctx, `
SELECT pause_id, revision, height, kind, detail FROM deposit_pause WHERE chain_id = $1`, chainID).
		Scan(&row.id, &row.rev, &row.height, &row.kind, &row.detail)
	if errors.Is(err, pgx.ErrNoRows) {
		return row, false
	}
	if err != nil {
		t.Fatalf("read deposit_pause: %v", err)
	}
	return row, true
}

// depositAuthState is the durable footprint a refusal must not change:
// checkpoint values and timestamp, row counts and the live pause row. The
// pause is stringified so the struct snapshot compares exactly.
type depositAuthState struct {
	cpOK      bool
	cpStart   int64
	cpHash    string
	cpNext    int64
	cpUpdated time.Time
	history   int
	obs       int
	audit     int
	pause     string // "" = absent, else id/rev/kind/height/detail
}

func depositAuthStateOf(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) depositAuthState {
	t.Helper()
	var st depositAuthState
	var start, next int64
	err := pool.QueryRow(ctx, `
SELECT start_block, config_hash, next_block, updated_at FROM deposit_checkpoint WHERE chain_id = $1`, chainID).
		Scan(&start, &st.cpHash, &next, &st.cpUpdated)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		t.Fatalf("read deposit_checkpoint: %v", err)
	default:
		st.cpOK, st.cpStart, st.cpNext = true, start, next
	}
	st.history = depositCountRows(t, ctx, pool, "deposit_config_history", chainID)
	st.obs = depositCountRows(t, ctx, pool, "deposit_observations", chainID)
	st.audit = depositCountRows(t, ctx, pool, "deposit_pause_audit", chainID)
	if pause, ok := depositAuthReadPause(t, ctx, pool, chainID); ok {
		st.pause = fmt.Sprintf("%d/%d/%s/%d/%s", pause.id, pause.rev, pause.kind, pause.height, pause.detail)
	}
	return st
}

// depositAuthAssertUnchanged asserts the refusal left every durable trace
// exactly as it was: checkpoint values and timestamp, history count,
// observations, pause row and pause audit count.
func depositAuthAssertUnchanged(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, before depositAuthState) {
	t.Helper()
	after := depositAuthStateOf(t, ctx, pool, chainID)
	if after.cpOK != before.cpOK || after.cpStart != before.cpStart || after.cpHash != before.cpHash ||
		after.cpNext != before.cpNext || !after.cpUpdated.Equal(before.cpUpdated) {
		t.Fatalf("checkpoint changed: %+v -> %+v", before, after)
	}
	if after.history != before.history || after.obs != before.obs || after.audit != before.audit {
		t.Fatalf("row counts changed: history %d->%d observations %d->%d pause_audit %d->%d",
			before.history, after.history, before.obs, after.obs, before.audit, after.audit)
	}
	if after.pause != before.pause {
		t.Fatalf("pause row changed: %q -> %q", before.pause, after.pause)
	}
}

// depositAuthHistory is one version row with optional columns stringified for
// direct struct comparison ("NULL" for SQL NULL).
type depositAuthHistory struct {
	seq                                    int64
	prevSeq, hash, assets, watches         string
	start, replay                          int64
	operator, reason, requestID            string
	expectedPauseID, expectedPauseRevision string
}

func depositAuthReadHistory(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID, seq int64) depositAuthHistory {
	t.Helper()
	var row depositAuthHistory
	err := pool.QueryRow(ctx, `
SELECT version_seq, COALESCE(prev_seq::text, 'NULL'), config_hash, start_block, assets, watches,
    replay_from, operator, reason, COALESCE(request_id, 'NULL'),
    COALESCE(expected_pause_id::text, 'NULL'), COALESCE(expected_pause_revision::text, 'NULL')
FROM deposit_config_history WHERE chain_id = $1 AND version_seq = $2`, chainID, seq).
		Scan(&row.seq, &row.prevSeq, &row.hash, &row.start, &row.assets, &row.watches,
			&row.replay, &row.operator, &row.reason, &row.requestID,
			&row.expectedPauseID, &row.expectedPauseRevision)
	if err != nil {
		t.Fatalf("read deposit_config_history v%d: %v", seq, err)
	}
	return row
}

// depositAuthAudit is one pause audit row snapshot.
type depositAuthAudit struct {
	action, operator, reason string
	versionSeq, height       int64
	kind, detail             string
}

func depositAuthReadAudit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID, pauseID, rev int64) depositAuthAudit {
	t.Helper()
	var row depositAuthAudit
	err := pool.QueryRow(ctx, `
SELECT action, operator, reason, version_seq, kind, height, detail
FROM deposit_pause_audit WHERE chain_id = $1 AND pause_id = $2 AND revision = $3`,
		chainID, pauseID, rev).
		Scan(&row.action, &row.operator, &row.reason, &row.versionSeq, &row.kind, &row.height, &row.detail)
	if err != nil {
		t.Fatalf("read deposit_pause_audit (%d,%d): %v", pauseID, rev, err)
	}
	return row
}

// --- happy path, idempotency and expiry gates -------------------------------

// TestDepositAuthHappyPathAndDuplicates: H1->H2 commits the new version, the
// replay position and the audit set atomically; an old-basis unit is then
// refused under the lock; a duplicate request_id reads back the recorded
// result even after later versions (cross-version idempotency by request_id
// alone), a different intent is refused and independent ids still observe the
// expiry and empty-change gates.
func TestDepositAuthHappyPathAndDuplicates(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID = 110
	h1, _, watches := depositAuthSeedChain(t, ctx, pool, chainID, 15)
	// Extend coverage one block past the position so the pre-authorization
	// unit [15,15] is readable before the authorization rewinds the position
	// to 12.
	depositSeedCanonical(t, ctx, pool, chainID, 15, 15, true)
	if _, err := pool.Exec(ctx, `UPDATE log_checkpoint SET next_block = 16 WHERE chain_id = $1`, chainID); err != nil {
		t.Fatalf("extend upstream watermark: %v", err)
	}

	// A scanner with the unit at the pre-authorization position [15,15]
	// captured under the H1 basis (version_seq=1, next_block=15): after the
	// authorization the same unit must be refused by version isolation.
	cfg := depositITConfig(t, chainID, testContractA, testContractB)
	cfg.ConfigHash = h1
	sc := depositITScanner(t, pool, cfg)
	lease := depositITLease(t, pool, chainID)
	unit, batch, captured := depositITPrepareUnit(t, ctx, sc, 15, 15)

	h2Assets := depositAuthSnapshot(depositAuthLine(testContractA, 10), depositAuthLine(testContractB, 12))
	h2 := depositAuthHash(t, 10, h2Assets, watches)
	req2 := depositAuthBaseReq(t, chainID, "auth-req-2", 1)
	req2.NewConfigHash, req2.NewStartBlock, req2.NewAssets, req2.NewWatches = h2, 10, h2Assets, watches

	res, err := AuthorizeDepositConfig(ctx, pool, req2)
	if err != nil {
		t.Fatalf("authorize H2: %v", err)
	}
	if want := (DepositAuthResult{VersionSeq: 2, ReplayFrom: 12, Pause: DepositPauseRetained}); res != want {
		t.Fatalf("result = %+v, want %+v", res, want)
	}
	start, hash, next, ok := depositCheckpointState(t, ctx, pool, chainID)
	if !ok || start != 10 || hash != h2 || next != 12 {
		t.Fatalf("checkpoint = (%d,%s,%d,%v), want (10,%s,12,true)", start, hash, next, ok, h2)
	}
	want2 := depositAuthHistory{
		seq: 2, prevSeq: "1", hash: h2, start: 10, assets: h2Assets, watches: watches,
		replay: 12, operator: "operator-1", reason: "test authorization",
		requestID: "auth-req-2", expectedPauseID: "NULL", expectedPauseRevision: "NULL",
	}
	if got := depositAuthReadHistory(t, ctx, pool, chainID, 2); got != want2 {
		t.Fatalf("history v2 = %+v, want %+v", got, want2)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_config_history", chainID); n != 2 {
		t.Fatalf("history rows = %d, want 2 (bootstrap + authorization)", n)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
		t.Fatalf("observations = %d, want 0", n)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_pause_audit", chainID); n != 0 {
		t.Fatalf("pause audit rows = %d, want 0", n)
	}

	// The old-basis unit is refused under the lock: the authorization
	// committed version_seq=2, so the captured version_seq=1 is stale (the
	// config identity moved with it) and nothing may be written.
	err = sc.commitDepositUnit(ctx, lease, unit, batch, captured, 15, 15)
	if !errors.Is(err, errDepositVersionMismatch) {
		t.Fatalf("old-basis commit = %v (%T), want errDepositVersionMismatch", err, err)
	}
	if _, hash, next, _ := depositCheckpointState(t, ctx, pool, chainID); hash != h2 || next != 12 {
		t.Fatalf("checkpoint changed by the refused old-basis commit: (%s,%d)", hash, next)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
		t.Fatalf("observations = %d after the refused old-basis commit, want 0", n)
	}

	// A second, independent authorization establishes v3 (a watch change), so
	// the duplicate read-back below is genuinely cross-version.
	w2 := "0x00000000000000000000000000000000000000d2"
	h3Watches := depositAuthSnapshot(depositAuthLine(w2, 14), watches)
	h3 := depositAuthHash(t, 10, h2Assets, h3Watches)
	req3 := depositAuthBaseReq(t, chainID, "auth-req-3", 2)
	req3.NewConfigHash, req3.NewStartBlock, req3.NewAssets, req3.NewWatches = h3, 10, h2Assets, h3Watches
	res3, err := AuthorizeDepositConfig(ctx, pool, req3)
	if err != nil {
		t.Fatalf("authorize H3: %v", err)
	}
	if want := (DepositAuthResult{VersionSeq: 3, ReplayFrom: 12, Pause: DepositPauseRetained}); res3 != want {
		t.Fatalf("H3 result = %+v, want %+v", res3, want)
	}
	if _, hash, _, _ := depositCheckpointState(t, ctx, pool, chainID); hash != h3 {
		t.Fatalf("checkpoint hash = %s, want H3 %s", hash, h3)
	}

	before := depositAuthStateOf(t, ctx, pool, chainID)

	// Duplicate of req2 after v3: recorded read-back, no execution, no state
	// change.
	res2b, err := AuthorizeDepositConfig(ctx, pool, req2)
	if err != nil {
		t.Fatalf("duplicate req2: %v", err)
	}
	if want := (DepositAuthResult{VersionSeq: 2, ReplayFrom: 12, Pause: DepositPauseRetained, Recorded: true}); res2b != want {
		t.Fatalf("duplicate result = %+v, want %+v", res2b, want)
	}
	depositAuthAssertUnchanged(t, ctx, pool, chainID, before)

	// Same request_id, different intent: refused with no state change.
	req2Diff := req2
	req2Diff.Reason = "a different intent"
	_, err = AuthorizeDepositConfig(ctx, pool, req2Diff)
	if !errors.Is(err, ErrAuthRejected) || !strings.Contains(err.Error(), "different intent") {
		t.Fatalf("different-intent error = %v, want ErrAuthRejected naming the different intent", err)
	}
	depositAuthAssertUnchanged(t, ctx, pool, chainID, before)

	// Different id, stale basis: independent requests still expire.
	reqStale := depositAuthBaseReq(t, chainID, "auth-req-stale", 1)
	reqStale.NewConfigHash, reqStale.NewStartBlock, reqStale.NewAssets, reqStale.NewWatches = h2, 10, h2Assets, watches
	_, err = AuthorizeDepositConfig(ctx, pool, reqStale)
	if !errors.Is(err, ErrAuthExpired) || !strings.Contains(err.Error(), "latest is 3") {
		t.Fatalf("stale independent request = %v, want ErrAuthExpired reporting the current 3", err)
	}
	depositAuthAssertUnchanged(t, ctx, pool, chainID, before)

	// Different id, current hash: the empty-change gate still applies.
	reqEmpty := depositAuthBaseReq(t, chainID, "auth-req-empty", 3)
	reqEmpty.NewConfigHash, reqEmpty.NewStartBlock, reqEmpty.NewAssets, reqEmpty.NewWatches = h3, 10, h2Assets, h3Watches
	_, err = AuthorizeDepositConfig(ctx, pool, reqEmpty)
	if !errors.Is(err, ErrAuthRejected) || !strings.Contains(err.Error(), "empty change") {
		t.Fatalf("empty-change independent request = %v, want ErrAuthRejected naming the empty change", err)
	}
	depositAuthAssertUnchanged(t, ctx, pool, chainID, before)
}

// TestDepositAuthRefusals covers the pre-transaction state-preserving
// refusals: a stale expected_old_seq, an empty change, a chain with no source
// version and the two one-sided corruption shapes. Every refusal must leave
// the checkpoint bytes, history count, observations, pause row and audit count
// untouched.
func TestDepositAuthRefusals(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	// The H2-style snapshot shared by the refusal requests below.
	h2Assets := depositAuthSnapshot(depositAuthLine(testContractA, 10), depositAuthLine(testContractB, 12))
	h2Watches := depositAuthLine(depositWatchAddr, 10)
	h2 := depositAuthHash(t, 10, h2Assets, h2Watches)

	t.Run("expiry_stale_expected_seq", func(t *testing.T) {
		const chainID = 111
		depositAuthSeedChain(t, ctx, pool, chainID, 15)
		req := depositAuthBaseReq(t, chainID, "auth-refusal-expiry", 0)
		req.NewConfigHash, req.NewStartBlock, req.NewAssets, req.NewWatches = h2, 10, h2Assets, h2Watches
		before := depositAuthStateOf(t, ctx, pool, chainID)

		_, err := AuthorizeDepositConfig(ctx, pool, req)
		if !errors.Is(err, ErrAuthExpired) || !strings.Contains(err.Error(), "expected version_seq 0") ||
			!strings.Contains(err.Error(), "latest is 1") {
			t.Fatalf("expiry error = %v, want ErrAuthExpired naming expected 0 and current 1", err)
		}
		depositAuthAssertUnchanged(t, ctx, pool, chainID, before)
	})

	t.Run("empty_change", func(t *testing.T) {
		const chainID = 112
		h1, assets, watches := depositAuthSeedChain(t, ctx, pool, chainID, 15)
		req := depositAuthBaseReq(t, chainID, "auth-refusal-empty", 1)
		req.NewConfigHash, req.NewStartBlock, req.NewAssets, req.NewWatches = h1, 10, assets, watches
		before := depositAuthStateOf(t, ctx, pool, chainID)

		_, err := AuthorizeDepositConfig(ctx, pool, req)
		if !errors.Is(err, ErrAuthRejected) || !strings.Contains(err.Error(), "empty change") {
			t.Fatalf("empty-change error = %v, want ErrAuthRejected naming the empty change", err)
		}
		depositAuthAssertUnchanged(t, ctx, pool, chainID, before)
	})

	t.Run("no_source_version", func(t *testing.T) {
		const chainID = 113 // deliberately unseeded
		req := depositAuthBaseReq(t, chainID, "auth-refusal-nosource", 1)
		req.NewConfigHash, req.NewStartBlock, req.NewAssets, req.NewWatches = h2, 10, h2Assets, h2Watches
		before := depositAuthStateOf(t, ctx, pool, chainID)

		_, err := AuthorizeDepositConfig(ctx, pool, req)
		if !errors.Is(err, ErrAuthRejected) || !strings.Contains(err.Error(), "no source version") {
			t.Fatalf("no-source error = %v, want ErrAuthRejected naming the missing source version", err)
		}
		depositAuthAssertUnchanged(t, ctx, pool, chainID, before)
	})

	t.Run("checkpoint_without_history", func(t *testing.T) {
		const chainID = 114
		h1 := depositAuthHash(t, 10, depositAuthLine(testContractA, 10), depositAuthLine(depositWatchAddr, 10))
		depositSeedCheckpoint(t, ctx, pool, chainID, 10, h1, 15)
		req := depositAuthBaseReq(t, chainID, "auth-refusal-corrupt-cp", 1)
		req.NewConfigHash, req.NewStartBlock, req.NewAssets, req.NewWatches = h2, 10, h2Assets, h2Watches
		before := depositAuthStateOf(t, ctx, pool, chainID)

		_, err := AuthorizeDepositConfig(ctx, pool, req)
		var corrupt *depositCorruptStateError
		if !errors.As(err, &corrupt) {
			t.Fatalf("corruption error = %v (%T), want *depositCorruptStateError", err, err)
		}
		depositAuthAssertUnchanged(t, ctx, pool, chainID, before)
	})

	t.Run("history_without_checkpoint", func(t *testing.T) {
		const chainID = 115
		h1 := depositAuthHash(t, 10, depositAuthLine(testContractA, 10), depositAuthLine(depositWatchAddr, 10))
		depositSeedHistory(t, ctx, pool, chainID, 1, 10, h1)
		req := depositAuthBaseReq(t, chainID, "auth-refusal-corrupt-h", 1)
		req.NewConfigHash, req.NewStartBlock, req.NewAssets, req.NewWatches = h2, 10, h2Assets, h2Watches
		before := depositAuthStateOf(t, ctx, pool, chainID)

		_, err := AuthorizeDepositConfig(ctx, pool, req)
		var corrupt *depositCorruptStateError
		if !errors.As(err, &corrupt) {
			t.Fatalf("corruption error = %v (%T), want *depositCorruptStateError", err, err)
		}
		depositAuthAssertUnchanged(t, ctx, pool, chainID, before)
	})
}

// TestDepositAuthPauseDispositions covers the pause rules of the authorization
// transaction: an explicit matching target releases the resolved instance with
// its audit row and records the target in the history row; a needs_006 pause
// is retained under a targetless request (targets are refused); a resolved
// in-scope pause without a target is refused as must-dispose; and a target
// that no longer matches the live instance is refused. Each refusal leaves the
// pause row and every other durable trace untouched.
func TestDepositAuthPauseDispositions(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	h2Assets := depositAuthSnapshot(depositAuthLine(testContractA, 10), depositAuthLine(testContractB, 12))

	t.Run("explicit_target_release", func(t *testing.T) {
		const chainID = 116
		h1, _, watches := depositAuthSeedChain(t, ctx, pool, chainID, 15)
		detail := "class=structural gap=10-11 cause=below_upstream_start config=" + h1
		pauseID, pauseRev := depositAuthSeedPause(t, ctx, pool, chainID, "upstream_gap", detail, 12)

		h2 := depositAuthHash(t, 10, h2Assets, watches)
		req := depositAuthBaseReq(t, chainID, "auth-release", 1)
		req.NewConfigHash, req.NewStartBlock, req.NewAssets, req.NewWatches = h2, 10, h2Assets, watches
		req.ExpectedPauseID, req.ExpectedPauseRevision = &pauseID, &pauseRev

		res, err := AuthorizeDepositConfig(ctx, pool, req)
		if err != nil {
			t.Fatalf("release authorization: %v", err)
		}
		// Replay folds to the resolved gap start (T026): combos alone give
		// 12, but gap=10-11 was never consumed.
		if want := (DepositAuthResult{VersionSeq: 2, ReplayFrom: 10, Pause: DepositPauseReleased}); res != want {
			t.Fatalf("result = %+v, want %+v", res, want)
		}
		if _, ok := depositAuthReadPause(t, ctx, pool, chainID); ok {
			t.Fatal("pause row still present after the explicit release")
		}
		audit := depositAuthReadAudit(t, ctx, pool, chainID, pauseID, pauseRev)
		wantAudit := depositAuthAudit{
			action: "release", operator: "operator-1", reason: "test authorization",
			versionSeq: 2, kind: "upstream_gap", height: 12, detail: detail,
		}
		if audit != wantAudit {
			t.Fatalf("release audit = %+v, want %+v", audit, wantAudit)
		}
		want2 := depositAuthHistory{
			seq: 2, prevSeq: "1", hash: h2, start: 10, assets: h2Assets, watches: watches,
			replay: 10, operator: "operator-1", reason: "test authorization",
			requestID:       "auth-release",
			expectedPauseID: strconv.FormatInt(pauseID, 10), expectedPauseRevision: strconv.FormatInt(pauseRev, 10),
		}
		if got := depositAuthReadHistory(t, ctx, pool, chainID, 2); got != want2 {
			t.Fatalf("history v2 = %+v, want %+v", got, want2)
		}
		if start, hash, next, ok := depositCheckpointState(t, ctx, pool, chainID); !ok || start != 10 || hash != h2 || next != 10 {
			t.Fatalf("checkpoint = (%d,%s,%d,%v), want (10,%s,10,true)", start, hash, next, ok, h2)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_pause_audit", chainID); n != 1 {
			t.Fatalf("pause audit rows = %d, want exactly 1", n)
		}
	})

	t.Run("retain_needs_006", func(t *testing.T) {
		const chainID = 117
		_, _, watches := depositAuthSeedChain(t, ctx, pool, chainID, 15)
		detail := "class=chain_view_changed needs_006=true version=1"
		pauseID, pauseRev := depositAuthSeedPause(t, ctx, pool, chainID, "chain_view_changed", detail, 12)
		live, ok := depositAuthReadPause(t, ctx, pool, chainID)
		if !ok {
			t.Fatal("precondition: pause row missing")
		}
		h2 := depositAuthHash(t, 10, h2Assets, watches)

		// A target must not dissolve needs_006 evidence: refused, unchanged.
		reqTarget := depositAuthBaseReq(t, chainID, "auth-needs006-target", 1)
		reqTarget.NewConfigHash, reqTarget.NewStartBlock, reqTarget.NewAssets, reqTarget.NewWatches = h2, 10, h2Assets, watches
		reqTarget.ExpectedPauseID, reqTarget.ExpectedPauseRevision = &pauseID, &pauseRev
		before := depositAuthStateOf(t, ctx, pool, chainID)
		_, err := AuthorizeDepositConfig(ctx, pool, reqTarget)
		if !errors.Is(err, ErrAuthRejected) || !strings.Contains(err.Error(), "needs 006") {
			t.Fatalf("needs_006 target error = %v, want ErrAuthRejected naming the 006 requirement", err)
		}
		depositAuthAssertUnchanged(t, ctx, pool, chainID, before)

		// Targetless: retained, the row keeps its identity and revision, the
		// history stores NULL targets, and the checkpoint still moves.
		reqRetain := depositAuthBaseReq(t, chainID, "auth-needs006-retain", 1)
		reqRetain.NewConfigHash, reqRetain.NewStartBlock, reqRetain.NewAssets, reqRetain.NewWatches = h2, 10, h2Assets, watches
		res, err := AuthorizeDepositConfig(ctx, pool, reqRetain)
		if err != nil {
			t.Fatalf("retain authorization: %v", err)
		}
		if want := (DepositAuthResult{VersionSeq: 2, ReplayFrom: 12, Pause: DepositPauseRetained}); res != want {
			t.Fatalf("result = %+v, want %+v", res, want)
		}
		after, ok := depositAuthReadPause(t, ctx, pool, chainID)
		if !ok || after != live {
			t.Fatalf("pause row = %+v (ok=%v), want untouched %+v", after, ok, live)
		}
		want2 := depositAuthHistory{
			seq: 2, prevSeq: "1", hash: h2, start: 10, assets: h2Assets, watches: watches,
			replay: 12, operator: "operator-1", reason: "test authorization",
			requestID: "auth-needs006-retain", expectedPauseID: "NULL", expectedPauseRevision: "NULL",
		}
		if got := depositAuthReadHistory(t, ctx, pool, chainID, 2); got != want2 {
			t.Fatalf("history v2 = %+v, want %+v", got, want2)
		}
		if start, hash, next, ok := depositCheckpointState(t, ctx, pool, chainID); !ok || start != 10 || hash != h2 || next != 12 {
			t.Fatalf("checkpoint = (%d,%s,%d,%v), want (10,%s,12,true)", start, hash, next, ok, h2)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_pause_audit", chainID); n != 0 {
			t.Fatalf("pause audit rows = %d, want 0 for a retained pause", n)
		}
	})

	t.Run("must_dispose_without_target", func(t *testing.T) {
		const chainID = 118
		_, _, watches := depositAuthSeedChain(t, ctx, pool, chainID, 15)
		depositAuthSeedPause(t, ctx, pool, chainID, "chain_view_changed", "class=chain_view_changed version=1", 12)
		h2 := depositAuthHash(t, 10, h2Assets, watches)
		req := depositAuthBaseReq(t, chainID, "auth-must-dispose", 1)
		req.NewConfigHash, req.NewStartBlock, req.NewAssets, req.NewWatches = h2, 10, h2Assets, watches
		before := depositAuthStateOf(t, ctx, pool, chainID)

		_, err := AuthorizeDepositConfig(ctx, pool, req)
		if !errors.Is(err, ErrAuthRejected) || !strings.Contains(err.Error(), "must be disposed") {
			t.Fatalf("must-dispose error = %v, want ErrAuthRejected naming the required disposal", err)
		}
		depositAuthAssertUnchanged(t, ctx, pool, chainID, before)
	})

	t.Run("replaced_target", func(t *testing.T) {
		const chainID = 119
		_, _, watches := depositAuthSeedChain(t, ctx, pool, chainID, 15)
		pauseID, pauseRev := depositAuthSeedPause(t, ctx, pool, chainID, "chain_view_changed", "class=chain_view_changed version=1", 12)
		replacedID, replacedRev := pauseID+1000, pauseRev
		h2 := depositAuthHash(t, 10, h2Assets, watches)
		req := depositAuthBaseReq(t, chainID, "auth-replaced-target", 1)
		req.NewConfigHash, req.NewStartBlock, req.NewAssets, req.NewWatches = h2, 10, h2Assets, watches
		req.ExpectedPauseID, req.ExpectedPauseRevision = &replacedID, &replacedRev
		before := depositAuthStateOf(t, ctx, pool, chainID)

		_, err := AuthorizeDepositConfig(ctx, pool, req)
		if !errors.Is(err, ErrAuthRejected) || !strings.Contains(err.Error(), "replaced live") {
			t.Fatalf("replaced-target error = %v, want ErrAuthRejected naming the replaced target", err)
		}
		depositAuthAssertUnchanged(t, ctx, pool, chainID, before)
	})
}

// --- T026 replay, shrink and the gap fold ------------------------------------

// TestDepositAuthReplayShrink: pure shrink replays from the current position
// (no backward move, history preserved byte-identical), a deleted asset keeps
// its observations with their original version, a resolved upstream gap folds
// its start into replay_from, and a replay below the upstream start refuses
// instead of trimming.
func TestDepositAuthReplayShrink(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	readObservation := func(t *testing.T, chainID int64, block uint64) (amount string, version int64) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT amount::text, version_seq FROM deposit_observations
WHERE chain_id = $1 AND block_number = $2`, chainID, int64(block)).Scan(&amount, &version); err != nil {
			t.Fatalf("read observation at %d: %v", block, err)
		}
		return amount, version
	}

	t.Run("postpone_height_replays_at_position", func(t *testing.T) {
		const chainID = 120
		_, _, watches := depositAuthSeedChain(t, ctx, pool, chainID, 15)
		depositSeedObservation(t, ctx, pool, chainID, 12, depositBlockHash(12), depositTxHash(12, 0), 0, "5", 1)
		// Postpone the asset effective height 10->15: pure shrink, replay = a.
		postponed := depositAuthSnapshot(depositAuthLine(testContractA, 15))
		h2 := depositAuthHash(t, 10, postponed, watches)
		req := depositAuthBaseReq(t, chainID, "auth-postpone", 1)
		req.NewConfigHash, req.NewStartBlock, req.NewAssets, req.NewWatches = h2, 10, postponed, watches
		res, err := AuthorizeDepositConfig(ctx, pool, req)
		if err != nil {
			t.Fatalf("authorize shrink: %v", err)
		}
		if want := (DepositAuthResult{VersionSeq: 2, ReplayFrom: 15, Pause: DepositPauseRetained}); res != want {
			t.Fatalf("result = %+v, want %+v", res, want)
		}
		if start, hash, next, ok := depositCheckpointState(t, ctx, pool, chainID); !ok || start != 10 || hash != h2 || next != 15 {
			t.Fatalf("checkpoint = (%d,%s,%d,%v), want (10,%s,15,true): shrink must not move the position", start, hash, next, ok, h2)
		}
		if got := depositAuthReadHistory(t, ctx, pool, chainID, 2); got.replay != 15 || got.assets != postponed || got.prevSeq != "1" {
			t.Fatalf("history v2 = %+v, want replay 15 with the postponed snapshot and prev 1", got)
		}
		if amount, version := readObservation(t, chainID, 12); amount != "5" || version != 1 {
			t.Fatalf("old observation = (%s,%d), want (5,1) retained with its version", amount, version)
		}
	})

	t.Run("raise_start_replays_at_position", func(t *testing.T) {
		const chainID = 121
		_, assets, watches := depositAuthSeedChain(t, ctx, pool, chainID, 15)
		h2 := depositAuthHash(t, 12, assets, watches)
		req := depositAuthBaseReq(t, chainID, "auth-raise-start", 1)
		req.NewConfigHash, req.NewStartBlock, req.NewAssets, req.NewWatches = h2, 12, assets, watches
		res, err := AuthorizeDepositConfig(ctx, pool, req)
		if err != nil {
			t.Fatalf("authorize raise-start: %v", err)
		}
		if want := (DepositAuthResult{VersionSeq: 2, ReplayFrom: 15, Pause: DepositPauseRetained}); res != want {
			t.Fatalf("result = %+v, want %+v", res, want)
		}
		if start, hash, next, ok := depositCheckpointState(t, ctx, pool, chainID); !ok || start != 12 || hash != h2 || next != 15 {
			t.Fatalf("checkpoint = (%d,%s,%d,%v), want (12,%s,15,true)", start, hash, next, ok, h2)
		}
	})

	t.Run("delete_after_add_keeps_history", func(t *testing.T) {
		const chainID = 122
		_, _, watches := depositAuthSeedChain(t, ctx, pool, chainID, 15)
		withB := depositAuthSnapshot(depositAuthLine(testContractA, 10), depositAuthLine(testContractB, 12))
		h1b := depositAuthHash(t, 10, withB, watches)
		add := depositAuthBaseReq(t, chainID, "auth-add-b", 1)
		add.NewConfigHash, add.NewStartBlock, add.NewAssets, add.NewWatches = h1b, 10, withB, watches
		if res, err := AuthorizeDepositConfig(ctx, pool, add); err != nil {
			t.Fatalf("authorize add: %v", err)
		} else if res.ReplayFrom != 12 || res.VersionSeq != 2 {
			t.Fatalf("add result = %+v, want v2/replay 12", res)
		}
		depositSeedObservation(t, ctx, pool, chainID, 12, depositBlockHash(12), depositTxHash(12, 0), 0, "7", 2)
		// Delete B again: every new combination already existed, replay = a.
		onlyA := depositAuthLine(testContractA, 10)
		h2 := depositAuthHash(t, 10, onlyA, watches)
		del := depositAuthBaseReq(t, chainID, "auth-del-b", 2)
		del.NewConfigHash, del.NewStartBlock, del.NewAssets, del.NewWatches = h2, 10, onlyA, watches
		res, err := AuthorizeDepositConfig(ctx, pool, del)
		if err != nil {
			t.Fatalf("authorize delete: %v", err)
		}
		if want := (DepositAuthResult{VersionSeq: 3, ReplayFrom: 12, Pause: DepositPauseRetained}); res != want {
			t.Fatalf("result = %+v, want %+v", res, want)
		}
		if _, hash, next, _ := depositCheckpointState(t, ctx, pool, chainID); hash != h2 || next != 12 {
			t.Fatalf("checkpoint = (%s,%d), want (%s,12)", hash, next, h2)
		}
		if n := depositCountRows(t, ctx, pool, "deposit_config_history", chainID); n != 3 {
			t.Fatalf("history rows = %d, want 3 (shrink preserves history)", n)
		}
		if amount, version := readObservation(t, chainID, 12); amount != "7" || version != 2 {
			t.Fatalf("deleted-asset observation = (%s,%d), want (7,2) preserved", amount, version)
		}
	})

	t.Run("resolved_gap_folds_into_replay", func(t *testing.T) {
		const chainID = 123
		h1, _, watches := depositAuthSeedChain(t, ctx, pool, chainID, 15)
		depositSeedCanonical(t, ctx, pool, chainID, 15, 20, true)
		if _, err := pool.Exec(ctx, `UPDATE log_checkpoint SET next_block = 21 WHERE chain_id = $1`, chainID); err != nil {
			t.Fatalf("extend upstream watermark: %v", err)
		}
		pauseID, pauseRev := depositAuthSeedPause(t, ctx, pool, chainID, "upstream_gap",
			"deposit upstream gap: class=structural gap=10-14 cause=asset_not_indexed config="+h1, 10)
		// New asset effective 18: combos alone give replay 15, but the
		// resolved gap [10,14] folds replay back to 10.
		withB := depositAuthSnapshot(depositAuthLine(testContractA, 10), depositAuthLine(testContractB, 18))
		h2 := depositAuthHash(t, 10, withB, watches)
		req := depositAuthBaseReq(t, chainID, "auth-gap-fold", 1)
		req.NewConfigHash, req.NewStartBlock, req.NewAssets, req.NewWatches = h2, 10, withB, watches
		req.ExpectedPauseID, req.ExpectedPauseRevision = &pauseID, &pauseRev
		res, err := AuthorizeDepositConfig(ctx, pool, req)
		if err != nil {
			t.Fatalf("authorize gap fold: %v", err)
		}
		if want := (DepositAuthResult{VersionSeq: 2, ReplayFrom: 10, Pause: DepositPauseReleased}); res != want {
			t.Fatalf("result = %+v, want %+v", res, want)
		}
		if _, hash, next, _ := depositCheckpointState(t, ctx, pool, chainID); hash != h2 || next != 10 {
			t.Fatalf("checkpoint = (%s,%d), want (%s,10): the gap start must fold into replay", hash, next, h2)
		}
		if got := depositAuthReadHistory(t, ctx, pool, chainID, 2); got.replay != 10 {
			t.Fatalf("history v2 replay = %d, want 10", got.replay)
		}
		if _, ok := depositAuthReadPause(t, ctx, pool, chainID); ok {
			t.Fatal("pause row still exists after the release")
		}
		audit := depositAuthReadAudit(t, ctx, pool, chainID, pauseID, pauseRev)
		if audit.action != "release" || audit.versionSeq != 2 {
			t.Fatalf("audit = %+v, want release at version 2", audit)
		}
	})

	t.Run("replay_below_upstream_start_refused", func(t *testing.T) {
		const chainID = 124
		_, assets, watches := depositAuthSeedChain(t, ctx, pool, chainID, 15)
		if _, err := pool.Exec(ctx, `UPDATE log_checkpoint SET start_block = 20, next_block = 25 WHERE chain_id = $1`, chainID); err != nil {
			t.Fatalf("raise upstream start: %v", err)
		}
		h2 := depositAuthHash(t, 5, assets, watches)
		req := depositAuthBaseReq(t, chainID, "auth-below-start", 1)
		req.NewConfigHash, req.NewStartBlock, req.NewAssets, req.NewWatches = h2, 5, assets, watches
		before := depositAuthStateOf(t, ctx, pool, chainID)
		_, err := AuthorizeDepositConfig(ctx, pool, req)
		if !errors.Is(err, ErrAuthRejected) || !strings.Contains(err.Error(), "structural gap") {
			t.Fatalf("below-start error = %v, want ErrAuthRejected naming the structural gap (never trim to fit)", err)
		}
		depositAuthAssertUnchanged(t, ctx, pool, chainID, before)
	})
}

// --- T027 D11 authorization subset (1): failure, idempotency, unknown -------

// depositAuthD11H2 derives the standard second identity of the D11 scenarios:
// the H1 snapshots plus testContractB at effective 12, so replay_from is 12
// against a position of 16.
func depositAuthD11H2(t *testing.T, watches string) (h2, assets string) {
	t.Helper()
	assets = depositAuthSnapshot(depositAuthLine(testContractA, 10), depositAuthLine(testContractB, 12))
	return depositAuthHash(t, 10, assets, watches), assets
}

// depositAuthD11Req builds a request for the standard H2 change.
func depositAuthD11Req(t *testing.T, chainID int64, requestID string, oldSeq int64, hash, assets, watches string) DepositAuthRequest {
	t.Helper()
	req := depositAuthBaseReq(t, chainID, requestID, oldSeq)
	req.NewConfigHash, req.NewStartBlock, req.NewAssets, req.NewWatches = hash, 10, assets, watches
	return req
}

// depositAuthD11Invariants asserts the global invariants that must hold after
// any authorization outcome: checkpoint and latest history agree; the version
// chain is contiguous with correct prev_seq links; every authorization row
// carries a complete audit set; every pause audit row resolves to an existing
// version; releases are unique per instance+revision; and any live pause row is
// a valid instance. Stress tests assert only these, never a specific outcome.
func depositAuthD11Invariants(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) {
	t.Helper()
	if _, _, err := readAuthProgress(ctx, pool, chainID); err != nil {
		t.Fatalf("checkpoint/latest-history integrity broken: %v", err)
	}
	var chainBreaks, auditGaps, dangling, duplicateReleases int
	if err := pool.QueryRow(ctx, `
SELECT
  (SELECT count(*) FROM deposit_config_history h
   WHERE h.chain_id = $1 AND ((h.version_seq = 1 AND h.prev_seq IS NOT NULL)
       OR (h.version_seq > 1 AND h.prev_seq IS DISTINCT FROM h.version_seq - 1))),
  (SELECT count(*) FROM deposit_config_history h
   WHERE h.chain_id = $1 AND h.request_id IS NOT NULL
     AND (h.operator = '' OR h.reason = '')),
  (SELECT count(*) FROM deposit_pause_audit a
   LEFT JOIN deposit_config_history h ON h.chain_id = a.chain_id AND h.version_seq = a.version_seq
   WHERE a.chain_id = $1 AND (h.version_seq IS NULL OR a.version_seq < 1)),
  (SELECT count(*) FROM (
     SELECT pause_id, revision FROM deposit_pause_audit
     WHERE chain_id = $1 AND action = 'release'
     GROUP BY pause_id, revision HAVING count(*) > 1) d)`, chainID).
		Scan(&chainBreaks, &auditGaps, &dangling, &duplicateReleases); err != nil {
		t.Fatalf("invariant query: %v", err)
	}
	if chainBreaks != 0 || auditGaps != 0 || dangling != 0 || duplicateReleases != 0 {
		t.Fatalf("invariants broken: chain breaks %d, auth audit gaps %d, dangling pause audits %d, duplicate releases %d",
			chainBreaks, auditGaps, dangling, duplicateReleases)
	}
	if pause, ok := depositAuthReadPause(t, ctx, pool, chainID); ok {
		switch pause.kind {
		case "upstream_gap", "chain_view_changed", "validation_failed":
		default:
			t.Fatalf("live pause kind %q is not a valid class", pause.kind)
		}
		if pause.id <= 0 || pause.rev < 1 || pause.height < 0 {
			t.Fatalf("live pause row invalid: %+v", pause)
		}
	}
	var rows, maxSeq int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*), COALESCE(max(version_seq), 0) FROM deposit_config_history WHERE chain_id = $1`,
		chainID).Scan(&rows, &maxSeq); err != nil {
		t.Fatalf("version count: %v", err)
	}
	if rows != maxSeq {
		t.Fatalf("version rows = %d, max seq = %d (the chain must be contiguous from 1)", rows, maxSeq)
	}
}

// TestDepositAuthD11UnknownCommitResolvesByDB: the COMMIT reply is dropped
// after PostgreSQL accepted it; the transaction resolves the unknown outcome
// from the database, reports the one committed version as recorded, and never
// double-advances. A same-request retry reads the same record.
func TestDepositAuthD11UnknownCommitResolvesByDB(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	const chainID = 140
	_, _, watches := depositAuthSeedChain(t, ctx, pool, chainID, 16)
	h2, assets := depositAuthD11H2(t, watches)
	// The request id must not contain the SQL-trigger marker: the fault
	// wrapper sees bound parameter bytes too, and a "commit" inside the value
	// would arm the drop on a read instead of the COMMIT statement.
	req := depositAuthD11Req(t, chainID, "d11-unknown-reply", 1, h2, assets, watches)

	fault := newDepositFault()
	faultPool := depositOpenFaultPool(t, dsn, fault)
	fault.armSQL(depositFaultCommit, true, false)

	res, err := AuthorizeDepositConfig(ctx, faultPool, req)
	if err != nil {
		t.Fatalf("authorize with the COMMIT reply dropped = %v, want the recorded result read from the DB", err)
	}
	if got := fault.fires.Load(); got != 1 {
		t.Fatalf("COMMIT drops = %d, want exactly 1 (a retry would double-commit)", got)
	}
	if want := (DepositAuthResult{VersionSeq: 2, ReplayFrom: 12, Pause: DepositPauseRetained, Recorded: true}); res != want {
		t.Fatalf("result = %+v, want %+v", res, want)
	}
	start, hash, next, ok := depositCheckpointState(t, ctx, pool, chainID)
	if !ok || start != 10 || hash != h2 || next != 12 {
		t.Fatalf("checkpoint = (%d,%s,%d,%v), want (10,%s,12,true): exactly one advance", start, hash, next, ok, h2)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_config_history", chainID); n != 2 {
		t.Fatalf("history rows = %d, want 2 (a single committed version)", n)
	}

	// The retry is a recorded read-back and changes nothing.
	before := depositAuthStateOf(t, ctx, pool, chainID)
	res2, err := AuthorizeDepositConfig(ctx, pool, req)
	if err != nil {
		t.Fatalf("same-request retry: %v", err)
	}
	if !res2.Recorded || res2.VersionSeq != 2 || res2.ReplayFrom != 12 {
		t.Fatalf("retry result = %+v, want recorded v2/replay 12", res2)
	}
	depositAuthAssertUnchanged(t, ctx, pool, chainID, before)
}

// TestDepositAuthD11ConcurrentDuplicateRequestID: two concurrent calls with
// the same request identity settle to exactly one executed version. The
// chain-wide lease lock serializes the transactions, so the loser either reads
// the recorded row back (classified after the winner committed) or reports the
// documented basis-moved expiry; both variants are accepted and must agree on
// the derived values with the winner.
func TestDepositAuthD11ConcurrentDuplicateRequestID(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	const chainID = 141
	_, _, watches := depositAuthSeedChain(t, ctx, pool, chainID, 16)
	h2, assets := depositAuthD11H2(t, watches)
	req := depositAuthD11Req(t, chainID, "d11-concurrent", 1, h2, assets, watches)

	type outcome struct {
		res DepositAuthResult
		err error
	}
	outcomes := make([]outcome, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range outcomes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res, err := AuthorizeDepositConfig(ctx, pool, req)
			outcomes[i] = outcome{res: res, err: err}
		}(i)
	}
	close(start)
	wg.Wait()

	wantExecuted := DepositAuthResult{VersionSeq: 2, ReplayFrom: 12, Pause: DepositPauseRetained}
	wantRecorded := wantExecuted
	wantRecorded.Recorded = true
	executed, recorded, expired := 0, 0, 0
	for i, o := range outcomes {
		switch {
		case o.err == nil && o.res == wantExecuted:
			executed++
		case o.err == nil && o.res == wantRecorded:
			recorded++
		case o.err != nil && errors.Is(o.err, ErrAuthExpired):
			expired++
			if !strings.Contains(o.err.Error(), "expected version_seq 1") {
				t.Fatalf("outcome %d expiry = %v, want it to name the expected version 1", i, o.err)
			}
		default:
			t.Fatalf("outcome %d = (%+v, %v), want the executed, recorded or expired verdict", i, o.res, o.err)
		}
	}
	if executed != 1 || recorded+expired != 1 {
		t.Fatalf("outcomes = executed %d recorded %d expired %d, want exactly one executed and one converging duplicate",
			executed, recorded, expired)
	}
	if start, hash, next, ok := depositCheckpointState(t, ctx, pool, chainID); !ok || start != 10 || hash != h2 || next != 12 {
		t.Fatalf("checkpoint = (%d,%s,%d,%v), want (10,%s,12,true)", start, hash, next, ok, h2)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_config_history", chainID); n != 2 {
		t.Fatalf("history rows = %d, want 2 (the duplicate never executes twice)", n)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
		t.Fatalf("observations = %d, want 0", n)
	}

	// A joined same-request retry always reads the recorded result.
	before := depositAuthStateOf(t, ctx, pool, chainID)
	res, err := AuthorizeDepositConfig(ctx, pool, req)
	if err != nil || res != wantRecorded {
		t.Fatalf("joined retry = (%+v, %v), want %+v", res, err, wantRecorded)
	}
	depositAuthAssertUnchanged(t, ctx, pool, chainID, before)
}

// TestDepositAuthD11LoopbackOldAuthExpired: H1->H2 by req-A, then a new
// authorization back to the H1 content under a new request id creates v3
// (same content hash as v1, distinct seq). A new-id request carrying the old
// expected_old_seq=1 expires reporting 1 vs 3, while req-A still reads its v2
// record back across versions.
func TestDepositAuthD11LoopbackOldAuthExpired(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	const chainID = 142
	h1, h1Assets, watches := depositAuthSeedChain(t, ctx, pool, chainID, 16)
	h2, h2Assets := depositAuthD11H2(t, watches)

	reqA := depositAuthD11Req(t, chainID, "d11-loopback-a", 1, h2, h2Assets, watches)
	resA, err := AuthorizeDepositConfig(ctx, pool, reqA)
	if err != nil {
		t.Fatalf("authorize H2: %v", err)
	}
	if want := (DepositAuthResult{VersionSeq: 2, ReplayFrom: 12, Pause: DepositPauseRetained}); resA != want {
		t.Fatalf("H2 result = %+v, want %+v", resA, want)
	}

	// The loopback: the H1 content returns under a brand-new request id and a
	// new seq (content hash repetition never reuses a version identity).
	reqB := depositAuthD11Req(t, chainID, "d11-loopback-b", 2, h1, h1Assets, watches)
	resB, err := AuthorizeDepositConfig(ctx, pool, reqB)
	if err != nil {
		t.Fatalf("authorize loopback H1: %v", err)
	}
	if want := (DepositAuthResult{VersionSeq: 3, ReplayFrom: 12, Pause: DepositPauseRetained}); resB != want {
		t.Fatalf("loopback result = %+v, want %+v", resB, want)
	}
	got3 := depositAuthReadHistory(t, ctx, pool, chainID, 3)
	if got3.hash != h1 || got3.prevSeq != "2" || got3.replay != 12 || got3.requestID != "d11-loopback-b" {
		t.Fatalf("history v3 = %+v, want the H1 hash on seq 3 with prev 2 and replay 12", got3)
	}
	if _, hash, next, _ := depositCheckpointState(t, ctx, pool, chainID); hash != h1 || next != 12 {
		t.Fatalf("checkpoint = (%s,%d), want (%s,12)", hash, next, h1)
	}

	// A different id based on the old seq 1 is expired, reporting both.
	reqStale := depositAuthD11Req(t, chainID, "d11-loopback-stale", 1, h2, h2Assets, watches)
	before := depositAuthStateOf(t, ctx, pool, chainID)
	_, err = AuthorizeDepositConfig(ctx, pool, reqStale)
	if !errors.Is(err, ErrAuthExpired) || !strings.Contains(err.Error(), "expected version_seq 1") ||
		!strings.Contains(err.Error(), "latest is 3") {
		t.Fatalf("old-seq request = %v, want ErrAuthExpired reporting expected 1 and current 3", err)
	}
	depositAuthAssertUnchanged(t, ctx, pool, chainID, before)

	// The original request still reads its v2 record back, not a new execution.
	resA2, err := AuthorizeDepositConfig(ctx, pool, reqA)
	if err != nil {
		t.Fatalf("req-A read-back: %v", err)
	}
	if want := (DepositAuthResult{VersionSeq: 2, ReplayFrom: 12, Pause: DepositPauseRetained, Recorded: true}); resA2 != want {
		t.Fatalf("req-A read-back = %+v, want %+v", resA2, want)
	}
	depositAuthAssertUnchanged(t, ctx, pool, chainID, before)
}

// TestDepositAuthD11MissingTargetThenRetarget: a resolvable pause without an
// explicit target is refused as must-dispose and binds nothing (no record, no
// side effects); reusing that request id with a target is an independent
// candidate that succeeds after full re-verification (2026-09-13 approved
// revision); once bound, the same id with a changed intent is refused; a new
// id with the now-stale target is refused per the current conditions.
func TestDepositAuthD11MissingTargetThenRetarget(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	const chainID = 143
	_, _, watches := depositAuthSeedChain(t, ctx, pool, chainID, 16)
	pauseID, pauseRev := depositAuthSeedPause(t, ctx, pool, chainID, "chain_view_changed", "class=chain_view_changed version=1", 12)
	h2, assets := depositAuthD11H2(t, watches)
	req := depositAuthD11Req(t, chainID, "d11-retarget", 1, h2, assets, watches)

	before := depositAuthStateOf(t, ctx, pool, chainID)
	_, err := AuthorizeDepositConfig(ctx, pool, req)
	if !errors.Is(err, ErrAuthRejected) || !strings.Contains(err.Error(), "must be disposed") {
		t.Fatalf("no-target request = %v, want ErrAuthRejected naming the required disposal", err)
	}
	depositAuthAssertUnchanged(t, ctx, pool, chainID, before)

	// The refusal bound nothing: the id classifies as a miss (rule: explicit
	// failure without a persisted record leaves the id unbound).
	if hit, _, err := classifyAuthRequest(ctx, pool, req); err != nil || hit != nil {
		t.Fatalf("classify refused id = (%v,%v), want a clean miss with no record", hit, err)
	}

	// Same id, now with a target: an independent candidate that succeeds
	// after full re-verification, with audit and target consistent.
	reqTarget := req
	reqTarget.ExpectedPauseID, reqTarget.ExpectedPauseRevision = &pauseID, &pauseRev
	res, err := AuthorizeDepositConfig(ctx, pool, reqTarget)
	if err != nil {
		t.Fatalf("same-id retarget: %v, want success as an independent candidate", err)
	}
	if want := (DepositAuthResult{VersionSeq: 2, ReplayFrom: 12, Pause: DepositPauseReleased}); res != want {
		t.Fatalf("retarget result = %+v, want %+v", res, want)
	}
	if _, ok := depositAuthReadPause(t, ctx, pool, chainID); ok {
		t.Fatal("pause row still present after the release")
	}
	if audit := depositAuthReadAudit(t, ctx, pool, chainID, pauseID, pauseRev); audit.action != "release" || audit.versionSeq != 2 {
		t.Fatalf("release audit = %+v, want release at version 2", audit)
	}
	if start, hash, next, ok := depositCheckpointState(t, ctx, pool, chainID); !ok || start != 10 || hash != h2 || next != 12 {
		t.Fatalf("checkpoint = (%d,%s,%d,%v), want (10,%s,12,true)", start, hash, next, ok, h2)
	}
	if got := depositAuthReadHistory(t, ctx, pool, chainID, 2); got.expectedPauseID != strconv.FormatInt(pauseID, 10) ||
		got.expectedPauseRevision != strconv.FormatInt(pauseRev, 10) {
		t.Fatalf("history v2 targets = (%s,%s), want (%d,%d)", got.expectedPauseID, got.expectedPauseRevision, pauseID, pauseRev)
	}
	released := depositAuthStateOf(t, ctx, pool, chainID)

	// Once bound, the same id with a changed intent (target dropped) is
	// refused against the record, with no state change.
	_, err = AuthorizeDepositConfig(ctx, pool, req)
	if !errors.Is(err, ErrAuthRejected) || !strings.Contains(err.Error(), "different intent") {
		t.Fatalf("bound-id changed intent = %v, want ErrAuthRejected for the different intent", err)
	}
	depositAuthAssertUnchanged(t, ctx, pool, chainID, released)

	// Conditions changed (pause gone): a new id with the stale target is
	// refused per the current conditions, not executed by default. The new
	// identity H3 keeps the request past the empty-change gate so the pause
	// adjudication is what refuses.
	w3 := "0x00000000000000000000000000000000000000d3"
	h3Watches := depositAuthSnapshot(watches, depositAuthLine(w3, 10))
	h3 := depositAuthHash(t, 10, assets, h3Watches)
	reqStale := depositAuthD11Req(t, chainID, "d11-retarget-stale", 2, h3, assets, h3Watches)
	reqStale.ExpectedPauseID, reqStale.ExpectedPauseRevision = &pauseID, &pauseRev
	_, err = AuthorizeDepositConfig(ctx, pool, reqStale)
	if !errors.Is(err, ErrAuthRejected) || !strings.Contains(err.Error(), "no live pause") {
		t.Fatalf("stale-target request = %v, want ErrAuthRejected naming the missing live pause", err)
	}
	depositAuthAssertUnchanged(t, ctx, pool, chainID, released)
}

// TestDepositAuthD11RetainedPauseStopsConsumer: after a needs_006 pause is
// retained under a targetless authorization (identity/position/history
// committed), the consumer running the new configuration with a live lease
// still stops on the pause row with *streamPauseError and zero advance, so the
// retention really keeps consumption stopped.
func TestDepositAuthD11RetainedPauseStopsConsumer(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	const chainID = 144
	_, _, watches := depositAuthSeedChain(t, ctx, pool, chainID, 16)
	pauseID, pauseRev := depositAuthSeedPause(t, ctx, pool, chainID, "chain_view_changed",
		"class=chain_view_changed needs_006=true version=1", 12)
	h2, assets := depositAuthD11H2(t, watches)
	req := depositAuthD11Req(t, chainID, "d11-retain-consumer", 1, h2, assets, watches)
	res, err := AuthorizeDepositConfig(ctx, pool, req)
	if err != nil {
		t.Fatalf("retain authorization: %v", err)
	}
	if want := (DepositAuthResult{VersionSeq: 2, ReplayFrom: 12, Pause: DepositPauseRetained}); res != want {
		t.Fatalf("result = %+v, want %+v", res, want)
	}
	if live, ok := depositAuthReadPause(t, ctx, pool, chainID); !ok || live.id != pauseID || live.rev != pauseRev {
		t.Fatalf("pause row = %+v (ok=%v), want the retained (%d,%d)", live, ok, pauseID, pauseRev)
	}

	// One lease object drives the consumer under the new configuration.
	cfg := depositITConfig(t, chainID, testContractA, testContractB)
	cfg.Assets = []config.DepositEntry{
		{Address: testContractA, Effective: 10},
		{Address: testContractB, Effective: 12},
	}
	cfg.ConfigHash = h2
	sc := depositITScanner(t, pool, cfg)
	lease := depositITLease(t, pool, chainID)
	err = depositRunLoopOnce(t, sc, lease)
	var paused *streamPauseError
	if !errors.As(err, &paused) || paused.stream != "deposit_pause" {
		t.Fatalf("ServeLoop() = %v (%T), want *streamPauseError for deposit_pause", err, err)
	}
	if start, hash, next, ok := depositCheckpointState(t, ctx, pool, chainID); !ok || start != 10 || hash != h2 || next != 12 {
		t.Fatalf("checkpoint moved by the stopped consumer: (%d,%s,%d,%v)", start, hash, next, ok)
	}
	if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
		t.Fatalf("observations = %d, want 0 while the pause is retained", n)
	}
}

// TestDepositAuthD11StaleInstanceDeleteZeroRows: after P1 is released by an
// authorization, P2 arrives with a fresh instance identity. A releaser still
// carrying P1's (id, revision) hits zero rows on the conditional DELETE, writes
// no audit row, fabricates no release for P2 and leaves P2 intact: staleness is
// reported by the audit lookup, never by deleting the wrong instance.
func TestDepositAuthD11StaleInstanceDeleteZeroRows(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	const chainID = 145
	_, _, watches := depositAuthSeedChain(t, ctx, pool, chainID, 16)
	p1, p1rev := depositAuthSeedPause(t, ctx, pool, chainID, "chain_view_changed", "class=chain_view_changed version=1", 12)
	h2, assets := depositAuthD11H2(t, watches)
	req := depositAuthD11Req(t, chainID, "d11-stale-p1", 1, h2, assets, watches)
	req.ExpectedPauseID, req.ExpectedPauseRevision = &p1, &p1rev
	res, err := AuthorizeDepositConfig(ctx, pool, req)
	if err != nil {
		t.Fatalf("release P1: %v", err)
	}
	if res.Pause != DepositPauseReleased {
		t.Fatalf("release result = %+v, want released", res)
	}
	if _, ok := depositAuthReadPause(t, ctx, pool, chainID); ok {
		t.Fatal("P1 still present after its release")
	}

	// A new pause instance P2 arrives after P1's release.
	p2, p2rev := depositAuthSeedPause(t, ctx, pool, chainID, "chain_view_changed", "class=chain_view_changed version=2", 12)
	live, ok := depositAuthReadPause(t, ctx, pool, chainID)
	if !ok || live.id != p2 || live.rev != p2rev {
		t.Fatalf("live pause = %+v (ok=%v), want P2 (%d,%d)", live, ok, p2, p2rev)
	}
	before := depositAuthStateOf(t, ctx, pool, chainID)

	// The stale releaser uses P1's identity: zero rows, no audit fabrication.
	tag, err := pool.Exec(ctx, deleteAuthPauseSQL, chainID, p1, p1rev)
	if err != nil {
		t.Fatalf("stale conditional DELETE: %v", err)
	}
	if n := tag.RowsAffected(); n != 0 {
		t.Fatalf("stale conditional DELETE affected %d rows, want 0", n)
	}
	after, ok := depositAuthReadPause(t, ctx, pool, chainID)
	if !ok || after != live {
		t.Fatalf("P2 = %+v (ok=%v), want it untouched (%+v)", after, ok, live)
	}
	var p2Releases int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_pause_audit WHERE chain_id = $1 AND pause_id = $2 AND action = 'release'`,
		chainID, p2).Scan(&p2Releases); err != nil {
		t.Fatalf("read P2 release audits: %v", err)
	}
	if p2Releases != 0 {
		t.Fatalf("release audit rows for P2 = %d, want 0 (staleness must be reported, not fabricated)", p2Releases)
	}
	depositAuthAssertUnchanged(t, ctx, pool, chainID, before)
}

// TestDepositAuthD11SameContentReplayAndTimestamp: two observations with the
// same content (contract/sender/recipient/amount) and the same fixed
// observed_at timestamp, but different source identities, exist under two
// different configuration versions; each row resolves through its own
// version_seq foreign key to the matching history identity, so the version
// reference — not the timestamp — distinguishes them.
func TestDepositAuthD11SameContentReplayAndTimestamp(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	const chainID = 146
	h1, _, watches := depositAuthSeedChain(t, ctx, pool, chainID, 16)
	h2, assets := depositAuthD11H2(t, watches)
	req := depositAuthD11Req(t, chainID, "d11-content-version", 1, h2, assets, watches)
	if _, err := AuthorizeDepositConfig(ctx, pool, req); err != nil {
		t.Fatalf("authorize H2: %v", err)
	}

	observedAt := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
	seed := func(block uint64, versionSeq int64) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
INSERT INTO deposit_observations
    (chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount, status, version_seq, observed_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending', $10, $11)`,
			chainID, depositBlockHash(block), depositTxHash(block, 0), 0, int64(block),
			testContractA, testContractB, depositWatchAddr, "5", versionSeq, observedAt); err != nil {
			t.Fatalf("seed observation at block %d: %v", block, err)
		}
	}
	seed(12, 1)
	seed(13, 2)

	// Identical content and identical timestamp across both rows.
	var sameContent int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM deposit_observations
WHERE chain_id = $1 AND contract = $2 AND sender = $3 AND recipient = $4 AND amount = 5 AND observed_at = $5`,
		chainID, testContractA, testContractB, depositWatchAddr, observedAt).Scan(&sameContent); err != nil {
		t.Fatalf("read same-content rows: %v", err)
	}
	if sameContent != 2 {
		t.Fatalf("same-content same-timestamp rows = %d, want 2", sameContent)
	}

	// Each row joins its own version_seq to the version that produced it.
	for _, tc := range []struct {
		block   uint64
		hash    string
		version int64
	}{
		{12, h1, 1},
		{13, h2, 2},
	} {
		var hash string
		var version int64
		if err := pool.QueryRow(ctx, `
SELECT h.config_hash, o.version_seq
FROM deposit_observations o
JOIN deposit_config_history h ON h.chain_id = o.chain_id AND h.version_seq = o.version_seq
WHERE o.chain_id = $1 AND o.block_number = $2`, chainID, int64(tc.block)).Scan(&hash, &version); err != nil {
			t.Fatalf("resolve observation at block %d: %v", tc.block, err)
		}
		if hash != tc.hash || version != tc.version {
			t.Fatalf("observation at block %d = (version %d, hash %s), want (%d, %s)", tc.block, version, hash, tc.version, tc.hash)
		}
	}
	var distinctVersions int
	if err := pool.QueryRow(ctx,
		`SELECT count(DISTINCT version_seq) FROM deposit_observations WHERE chain_id = $1`, chainID).Scan(&distinctVersions); err != nil {
		t.Fatalf("distinct version references: %v", err)
	}
	if distinctVersions != 2 {
		t.Fatalf("distinct observation version references = %d, want 2 (seq, never the timestamp, is the discriminator)", distinctVersions)
	}
}

// TestDepositAuthD11PauseFlipStressInvariants: ten sequential authorizations
// with a fresh explicit target run against a concurrent revision flipper.
// Individual outcomes are recorded, never asserted; after every attempt only
// the global invariants must hold: checkpoint <-> latest-history agreement,
// a contiguous version chain with correct prev_seq, complete audit fields on
// authorization rows, pause audit rows resolving to real versions, unique
// releases per instance, and a valid live pause row.
func TestDepositAuthD11PauseFlipStressInvariants(t *testing.T) {
	dsn := startIndexerPostgres(t)
	ctx := context.Background()
	pool := openIndexerPool(t, dsn)
	defer pool.Close()

	const chainID = 147
	_, _, watches := depositAuthSeedChain(t, ctx, pool, chainID, 16)
	w2 := depositAuthLine("0x00000000000000000000000000000000000000d2", 14)
	assets := depositAuthLine(testContractA, 10)
	watchesWithW2 := depositAuthSnapshot(w2, watches)
	hWithout := depositAuthHash(t, 10, assets, watches)
	hWith := depositAuthHash(t, 10, assets, watchesWithW2)

	// The flipper bumps the live instance revision continuously; it never
	// touches the version ledger, so any refusal it causes is a pause-race
	// refusal, never a basis expiry.
	flipCtx, cancelFlip := context.WithCancel(ctx)
	defer cancelFlip()
	var flips atomic.Int32
	flipDone := make(chan struct{})
	go func() {
		defer close(flipDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-flipCtx.Done():
				return
			case <-ticker.C:
				tag, err := pool.Exec(flipCtx,
					`UPDATE deposit_pause SET revision = revision + 1 WHERE chain_id = $1`, chainID)
				if err == nil {
					flips.Add(int32(tag.RowsAffected()))
				}
			}
		}
	}()

	outcomes := map[string]int{}
	for i := 0; i < 10; i++ {
		progress, latest, err := readAuthProgress(ctx, pool, chainID)
		if err != nil {
			t.Fatalf("attempt %d basis read: %v", i, err)
		}
		// Always pick the identity the chain is not currently on, so the
		// attempt can never be an empty change.
		hash, ws := hWith, watchesWithW2
		if progress.configHash == hWith {
			hash, ws = hWithout, watches
		}
		if _, ok := depositAuthReadPause(t, ctx, pool, chainID); !ok {
			depositAuthSeedPause(t, ctx, pool, chainID, "chain_view_changed", "class=chain_view_changed version=1", 14)
		}
		live, ok := depositAuthReadPause(t, ctx, pool, chainID)
		if !ok {
			t.Fatalf("attempt %d: no live pause to target", i)
		}
		req := depositAuthD11Req(t, chainID, fmt.Sprintf("d11-stress-%d", i), latest.versionSeq, hash, assets, ws)
		req.ExpectedPauseID, req.ExpectedPauseRevision = &live.id, &live.rev

		res, err := AuthorizeDepositConfig(ctx, pool, req)
		switch {
		case err == nil:
			outcomes["executed"]++
			if res.Pause == DepositPauseReleased {
				outcomes["released"]++
			}
			t.Logf("attempt %d executed v%d replay %d pause %s", i, res.VersionSeq, res.ReplayFrom, res.Pause)
		case errors.Is(err, ErrAuthRejected):
			outcomes["rejected"]++
			t.Logf("attempt %d rejected: %v", i, err)
		case errors.Is(err, ErrAuthExpired):
			outcomes["expired"]++
			t.Logf("attempt %d expired: %v", i, err)
		default:
			t.Fatalf("attempt %d: unexpected error %v", i, err)
		}
		depositAuthD11Invariants(t, ctx, pool, chainID)
	}
	cancelFlip()
	<-flipDone
	t.Logf("pause-flip stress outcomes: %v (pause revision flips observed: %d)", outcomes, flips.Load())
	if outcomes["executed"]+outcomes["rejected"]+outcomes["expired"] != 10 {
		t.Fatalf("outcomes = %v, want all 10 attempts classified", outcomes)
	}
	depositAuthD11Invariants(t, ctx, pool, chainID)
}
