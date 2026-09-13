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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
		if want := (DepositAuthResult{VersionSeq: 2, ReplayFrom: 12, Pause: DepositPauseReleased}); res != want {
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
			replay: 12, operator: "operator-1", reason: "test authorization",
			requestID:       "auth-release",
			expectedPauseID: strconv.FormatInt(pauseID, 10), expectedPauseRevision: strconv.FormatInt(pauseRev, 10),
		}
		if got := depositAuthReadHistory(t, ctx, pool, chainID, 2); got != want2 {
			t.Fatalf("history v2 = %+v, want %+v", got, want2)
		}
		if start, hash, next, ok := depositCheckpointState(t, ctx, pool, chainID); !ok || start != 10 || hash != h2 || next != 12 {
			t.Fatalf("checkpoint = (%d,%s,%d,%v), want (10,%s,12,true)", start, hash, next, ok, h2)
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
