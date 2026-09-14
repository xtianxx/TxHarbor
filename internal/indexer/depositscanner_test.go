// depositscanner_test.go locks the T005 proof logic that can be exercised
// without a database: the recomputed upstream identity, the R5 gap
// classification, the deposit-state integrity reads, constructor validation,
// the R2 statement order and the SQL shape. Real-PostgreSQL assertions live in
// deposit_integration_test.go.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/config"
)

// testDepositConfig is the minimal valid deposit configuration used by the
// unit tests (the upstream identity is recomputed by the helper).
func testDepositConfig(t *testing.T) DepositConfig {
	t.Helper()
	cfg := DepositConfig{
		ChainID:        7,
		StartBlock:     0,
		Assets:         []config.DepositEntry{{Address: testContractA, Effective: 0}},
		Watches:        []config.DepositEntry{{Address: depositWatchAddr, Effective: 0}},
		ConfigHash:     strings.Repeat("aa", 32),
		BatchBlocks:    10,
		LogContracts:   []string{testContractA},
		LogStartHeight: 0,
	}
	cfg.LogConfigHash = depositUpstreamHash(t, cfg.LogContracts...)
	return cfg
}

func depositUpstreamHash(t *testing.T, contracts ...string) string {
	t.Helper()
	_, hash, err := config.NormalizeWhitelist(strings.Join(contracts, ","))
	if err != nil {
		t.Fatalf("NormalizeWhitelist(%v): %v", contracts, err)
	}
	return hash
}

// depositFakeQuerier records statement order and serves canned scans; it lets
// the unit tests lock the R2 read order and the integrity branches without a
// database.
type depositFakeQuerier struct {
	calls []string
	rows  map[string]func(dest ...any) error
	qerr  error
}

func (f *depositFakeQuerier) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	f.calls = append(f.calls, sql)
	scan, ok := f.rows[sql]
	if !ok {
		return depositFakeRow(func(dest ...any) error { return pgx.ErrNoRows })
	}
	return depositFakeRow(scan)
}

func (f *depositFakeQuerier) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	f.calls = append(f.calls, sql)
	if f.qerr == nil {
		return nil, errors.New("unexpected Query in unit test")
	}
	return nil, f.qerr
}

type depositFakeRow func(dest ...any) error

func (r depositFakeRow) Scan(dest ...any) error { return r(dest...) }

func depositCallIndex(calls []string, sql string) int {
	for i, c := range calls {
		if c == sql {
			return i
		}
	}
	return -1
}

// TestRecomputeUpstreamIdentity pins the 003 identity recomputation from the
// shared env whitelist (R5: the env list is the whitelist truth) and its
// convergence under case/order/duplicate variants.
func TestRecomputeUpstreamIdentity(t *testing.T) {
	// Research R9 V2 vector, pinned by internal/config as well.
	const want = "b4eeddb97cb6ab1ba66eb8bb97e43b3f11b466de859f10bf30497ebd2e9cef7f"

	got, err := recomputeUpstreamIdentity(DepositConfig{LogContracts: []string{testContractA, testContractB}})
	if err != nil {
		t.Fatalf("recomputeUpstreamIdentity() error = %v", err)
	}
	if got != want {
		t.Fatalf("identity = %q, want %q", got, want)
	}

	variants := [][]string{
		{testContractB, testContractA},
		{testContractB, "0X" + strings.ToUpper(testContractA[2:]), testContractA},
	}
	for _, v := range variants {
		got, err := recomputeUpstreamIdentity(DepositConfig{LogContracts: v})
		if err != nil || got != want {
			t.Fatalf("recomputeUpstreamIdentity(%v) = %q, %v; want %q", v, got, err, want)
		}
	}

	// A configured hash that disagrees with the recomputation is refused.
	if _, err := recomputeUpstreamIdentity(DepositConfig{
		LogContracts:  []string{testContractA, testContractB},
		LogConfigHash: strings.Repeat("ab", 32),
	}); err == nil {
		t.Fatal("configured/recomputed identity mismatch must be an error")
	}
	if _, err := recomputeUpstreamIdentity(DepositConfig{}); err == nil {
		t.Fatal("empty whitelist must be an error")
	}
}

func TestVerifyUpstreamIdentity(t *testing.T) {
	up := &upstreamState{chainID: 7, startBlock: 0, configHash: strings.Repeat("ab", 32), nextBlock: 5}
	if err := verifyUpstreamIdentity(up, strings.Repeat("ab", 32)); err != nil {
		t.Fatalf("matching identity: %v", err)
	}
	err := verifyUpstreamIdentity(up, strings.Repeat("cd", 32))
	var drift *upstreamDriftError
	if !errors.As(err, &drift) {
		t.Fatalf("error = %v (%T), want *upstreamDriftError", err, err)
	}
	if drift.persisted != strings.Repeat("ab", 32) || drift.expected != strings.Repeat("cd", 32) {
		t.Fatalf("drift = %+v", drift)
	}
}

func TestClassifyUpstreamGap(t *testing.T) {
	cfg := DepositConfig{
		ChainID:    7,
		StartBlock: 10,
		Assets: []config.DepositEntry{
			{Address: testContractA, Effective: 10},
			{Address: testContractB, Effective: 200}, // not in the upstream whitelist
		},
		Watches:        []config.DepositEntry{{Address: depositWatchAddr, Effective: 10}},
		ConfigHash:     strings.Repeat("aa", 32),
		BatchBlocks:    500,
		LogContracts:   []string{testContractA},
		LogConfigHash:  strings.Repeat("bb", 32),
		LogStartHeight: 10,
	}
	up := func(start, next uint64) *upstreamState {
		return &upstreamState{chainID: 7, startBlock: start, configHash: cfg.LogConfigHash, nextBlock: next}
	}

	tests := []struct {
		name      string
		up        *upstreamState
		a, b      uint64
		wantClass string
		wantCause string
	}{
		{"covered_closed_boundary", up(10, 100), 10, 99, "", ""},
		{"start_equality_covered", up(10, 100), 10, 10, "", ""},
		{"watermark_reached_waits", up(10, 100), 10, 100, depositGapTransient, gapCauseBehindHead},
		{"watermark_straddles_unit", up(10, 50), 10, 100, depositGapTransient, gapCauseBehindHead},
		{"below_upstream_start", up(10, 100), 9, 20, depositGapStructural, gapCauseBelowUpstreamStart},
		{"asset_not_indexed_when_required", up(10, 500), 10, 200, depositGapStructural, gapCauseAssetNotIndexed},
		{"asset_not_required_yet", up(10, 500), 10, 199, "", ""},
		{"upstream_absent_waits", nil, 10, 20, depositGapTransient, gapCauseBehindHead},
		{"upstream_absent_below_declared_start", nil, 5, 20, depositGapStructural, gapCauseBelowUpstreamStart},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gap := classifyUpstreamGap(cfg, tc.up, tc.a, tc.b)
			if tc.wantClass == "" {
				if gap != nil {
					t.Fatalf("gap = %+v, want covered", gap)
				}
				return
			}
			if gap == nil {
				t.Fatalf("gap = nil, want class=%s cause=%s", tc.wantClass, tc.wantCause)
			}
			if gap.class != tc.wantClass || gap.cause != tc.wantCause {
				t.Fatalf("gap = %+v, want class=%s cause=%s", gap, tc.wantClass, tc.wantCause)
			}
			if gap.from != tc.a || gap.to != tc.b || gap.configHash != cfg.ConfigHash {
				t.Fatalf("gap range/config = %+v, want %d-%d config=%s", gap, tc.a, tc.b, cfg.ConfigHash)
			}
		})
	}
}

func TestValidateDepositConfig(t *testing.T) {
	base := testDepositConfig(t)
	checkErr := func(name string, mutate func(*DepositConfig)) {
		t.Helper()
		cfg := base
		mutate(&cfg)
		if hash, err := validateDepositConfig(cfg); err == nil {
			t.Fatalf("%s: validateDepositConfig() = %q, want error", name, hash)
		}
	}
	// The unmodified configuration is valid.
	if _, err := validateDepositConfig(base); err != nil {
		t.Fatalf("valid configuration rejected: %v", err)
	}
	checkErr("chain_id_zero", func(c *DepositConfig) { c.ChainID = 0 })
	checkErr("empty_assets", func(c *DepositConfig) { c.Assets = nil })
	checkErr("empty_watches", func(c *DepositConfig) { c.Watches = nil })
	checkErr("uppercase_config_hash", func(c *DepositConfig) { c.ConfigHash = strings.Repeat("AB", 32) })
	checkErr("zero_batch_blocks", func(c *DepositConfig) { c.BatchBlocks = 0 })
	checkErr("bad_address", func(c *DepositConfig) {
		c.Assets = []config.DepositEntry{{Address: "0x1234", Effective: 0}}
	})
	checkErr("empty_log_contracts", func(c *DepositConfig) { c.LogContracts = nil })
	checkErr("log_config_hash_mismatch", func(c *DepositConfig) { c.LogConfigHash = strings.Repeat("cd", 32) })
}

// TestDepositProgressIntegrity locks the Table 2 integrity branches: both
// sides exist or neither does, and their (start_block, config_hash) agree.
func TestDepositProgressIntegrity(t *testing.T) {
	cfg := testDepositConfig(t)
	sc := &DepositScanner{cfg: cfg, upstreamHash: depositUpstreamHash(t, cfg.LogContracts...)}
	cpRow := func(start uint64, hash string, next uint64) func(dest ...any) error {
		return func(dest ...any) error {
			*(dest[0].(*int64)) = int64(start)
			*(dest[1].(*string)) = hash
			*(dest[2].(*int64)) = int64(next)
			return nil
		}
	}
	histRow := func(seq int64, start uint64, hash string) func(dest ...any) error {
		return func(dest ...any) error {
			*(dest[0].(*int64)) = seq
			*(dest[1].(*int64)) = int64(start)
			*(dest[2].(*string)) = hash
			return nil
		}
	}
	hashA := strings.Repeat("aa", 32)

	tests := []struct {
		name        string
		rows        map[string]func(dest ...any) error
		wantNil     bool
		wantCorrupt bool
		want        depositProgress
	}{
		{"empty_progress", nil, true, false, depositProgress{}},
		{
			"checkpoint_without_history",
			map[string]func(dest ...any) error{readDepositCheckpointSQL: cpRow(0, hashA, 10)},
			false, true, depositProgress{},
		},
		{
			"history_without_checkpoint",
			map[string]func(dest ...any) error{readLatestDepositHistorySQL: histRow(1, 0, hashA)},
			false, true, depositProgress{},
		},
		{
			"start_block_disagrees",
			map[string]func(dest ...any) error{
				readDepositCheckpointSQL:    cpRow(5, hashA, 10),
				readLatestDepositHistorySQL: histRow(3, 6, hashA),
			},
			false, true, depositProgress{},
		},
		{
			"config_hash_disagrees",
			map[string]func(dest ...any) error{
				readDepositCheckpointSQL:    cpRow(5, hashA, 10),
				readLatestDepositHistorySQL: histRow(3, 5, strings.Repeat("bb", 32)),
			},
			false, true, depositProgress{},
		},
		{
			"consistent",
			map[string]func(dest ...any) error{
				readDepositCheckpointSQL:    cpRow(5, hashA, 10),
				readLatestDepositHistorySQL: histRow(3, 5, hashA),
			},
			false, false,
			depositProgress{startBlock: 5, configHash: hashA, nextBlock: 10, versionSeq: 3},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := &depositFakeQuerier{rows: tc.rows}
			got, err := sc.readProgress(context.Background(), q)
			if tc.wantCorrupt {
				var corrupt *depositCorruptStateError
				if !errors.As(err, &corrupt) {
					t.Fatalf("error = %v (%T), want *depositCorruptStateError", err, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("readProgress() error = %v", err)
			}
			if tc.wantNil {
				if got != nil {
					t.Fatalf("progress = %+v, want nil (empty progress)", got)
				}
				return
			}
			if got == nil || *got != tc.want {
				t.Fatalf("progress = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestDepositReadOrder pins R2's statement order: the 003 checkpoint is read
// before any source row, so an interleaved 003 commit can only add coverage.
func TestDepositReadOrder(t *testing.T) {
	cfg := testDepositConfig(t)
	sc := &DepositScanner{cfg: cfg, upstreamHash: depositUpstreamHash(t, cfg.LogContracts...)}
	blockHash := fmt.Sprintf("0x%064x", 0xd00d)
	q := &depositFakeQuerier{
		rows: map[string]func(dest ...any) error{
			readUpstreamCheckpointSQL: func(dest ...any) error {
				*(dest[0].(*int64)) = cfg.ChainID
				*(dest[1].(*int64)) = 0
				*(dest[2].(*string)) = sc.upstreamHash
				*(dest[3].(*int64)) = 100
				return nil
			},
			canonicalBlockHashSQL: func(dest ...any) error {
				*(dest[0].(*string)) = blockHash
				return nil
			},
		},
		qerr: errors.New("stop after the row read is recorded"),
	}
	_, err := sc.readCoveredUnit(context.Background(), q, 10, 10)
	if err == nil {
		t.Fatal("readCoveredUnit() error = nil, want the injected row-read failure")
	}
	checkpoint := depositCallIndex(q.calls, readUpstreamCheckpointSQL)
	rows := depositCallIndex(q.calls, readDepositSourceRowsSQL)
	canonical := depositCallIndex(q.calls, canonicalBlockHashSQL)
	if checkpoint != 0 {
		t.Fatalf("first statement = %q, want the upstream checkpoint read", q.calls[0])
	}
	if rows < 0 || canonical < 0 {
		t.Fatalf("read sequence %d statements, missing canonical or source-row read", len(q.calls))
	}
	if !(checkpoint < canonical && canonical < rows) {
		t.Fatalf("statement order checkpoint=%d canonical=%d rows=%d, want checkpoint < canonical < rows",
			checkpoint, canonical, rows)
	}
}

// TestDepositReadSQLShape pins the chain scope and consumption order of every
// new statement (chain identity is part of the coverage proof elements).
func TestDepositReadSQLShape(t *testing.T) {
	checks := []struct {
		name  string
		sql   string
		wants []string
	}{
		{"readUpstreamCheckpoint", readUpstreamCheckpointSQL, []string{
			"log_checkpoint", "chain_id = $1", "chain_id, start_block, config_hash, next_block",
		}},
		{"readDepositCheckpoint", readDepositCheckpointSQL, []string{
			"deposit_checkpoint", "chain_id = $1", "start_block, config_hash, next_block",
		}},
		{"readLatestDepositHistory", readLatestDepositHistorySQL, []string{
			"deposit_config_history", "chain_id = $1", "version_seq", "ORDER BY version_seq DESC", "LIMIT 1",
		}},
		{"depositPauseExists", depositPauseExistsSQL, []string{"deposit_pause", "chain_id = $1"}},
		{"readDepositSourceRows", readDepositSourceRowsSQL, []string{
			"erc20_transfer_logs", "chain_id = $1", "block_number >= $2", "block_number <= $3",
			"ORDER BY block_number, log_index",
		}},
		{"canonicalPointRead", canonicalBlockHashSQL, []string{"chain_blocks", "chain_id = $1", "canonical"}},
	}
	for _, c := range checks {
		for _, w := range c.wants {
			if !strings.Contains(c.sql, w) {
				t.Errorf("%s: SQL does not contain %q", c.name, w)
			}
		}
	}
}

// TestDepositCommitSQLShape pins the T006 step-5 statement semantics: dedupe
// on the source identity, the conflict comparison that must not read
// version_seq, the bootstrap history row and the exact checkpoint guards.
func TestDepositCommitSQLShape(t *testing.T) {
	checks := []struct {
		name    string
		sql     string
		wants   []string
		notWant []string
	}{
		{"insertObservation", insertDepositObservationSQL, []string{
			"deposit_observations", "ON CONFLICT (chain_id, block_hash, tx_hash, log_index) DO NOTHING",
			"block_number", "contract", "sender", "recipient", "amount", "version_seq",
		}, nil},
		{"readObservation", readDepositObservationSQL, []string{
			"deposit_observations", "contract", "block_number", "sender", "recipient",
			"amount::text", "status", "(amount = $5)",
		}, []string{"version_seq"}},
		{"bootstrapHistory", insertDepositBootstrapHistorySQL, []string{
			"deposit_config_history", "version_seq", "config_hash", "prev_seq", "start_block",
			"assets", "watches", "replay_from", "'bootstrap'", "request_id",
		}, nil},
		{"insertCheckpoint", insertDepositCheckpointSQL, []string{
			"deposit_checkpoint", "ON CONFLICT (chain_id) DO NOTHING",
		}, nil},
		{"advanceCheckpoint", advanceDepositCheckpointSQL, []string{
			"deposit_checkpoint", "next_block = $2", "start_block = $4", "config_hash = $5", "next_block = $3",
		}, nil},
	}
	for _, c := range checks {
		for _, w := range c.wants {
			if !strings.Contains(c.sql, w) {
				t.Errorf("%s: SQL does not contain %q", c.name, w)
			}
		}
		for _, nw := range c.notWant {
			if strings.Contains(c.sql, nw) {
				t.Errorf("%s: SQL must not contain %q (%s)", c.name, nw, c.sql)
			}
		}
	}
}

// TestDepositReconcileBatch locks the data-model step-5 row-count invariant:
// matched identities equal inserted + existing consistent rows, and the
// distinct source identity count equals inserted + existing + legal zero
// generation.
func TestDepositReconcileBatch(t *testing.T) {
	row := func(n uint64) depositSourceLog {
		return depositSourceLog{blockHash: fmt.Sprintf("0x%064x", n), txHash: fmt.Sprintf("0x%064x", n), logIndex: n}
	}
	tests := []struct {
		name                        string
		rows                        []depositSourceLog
		matched, inserted, existing int
		zero, nomatch               int
		wantErr                     bool
	}{
		{"mixed_legal_zero", []depositSourceLog{row(1), row(2), row(3)}, 1, 1, 0, 1, 1, false},
		{"replay_existing", []depositSourceLog{row(1)}, 1, 0, 1, 0, 0, false},
		{"matched_silent_loss", []depositSourceLog{row(1)}, 1, 0, 0, 0, 0, true},
		{"zero_generation_silent_loss", []depositSourceLog{row(1), row(2), row(3)}, 1, 1, 0, 0, 0, true},
		{"duplicate_identity", []depositSourceLog{row(1), row(1)}, 2, 1, 1, 0, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := reconcileDepositBatch(depositIdentityCount(tc.rows), tc.matched, tc.inserted, tc.existing, tc.zero, tc.nomatch)
			if tc.wantErr && err == nil {
				t.Fatal("reconcileDepositBatch() = nil, want error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("reconcileDepositBatch() = %v, want nil", err)
			}
		})
	}
}

func TestDepositUnitEnd(t *testing.T) {
	if got := depositUnitEnd(10, 11); got != 20 {
		t.Fatalf("depositUnitEnd(10,11) = %d, want 20", got)
	}
	if got := depositUnitEnd(0, 1); got != 0 {
		t.Fatalf("depositUnitEnd(0,1) = %d, want 0", got)
	}
	if got := depositUnitEnd(5, 0); got != 5 {
		t.Fatalf("depositUnitEnd(5,0) = %d, want 5", got)
	}
	if got := depositUnitEnd(^uint64(0), 2); got != ^uint64(0) {
		t.Fatalf("depositUnitEnd(max,2) = %d, want max", got)
	}
}

// TestDepositCoveredEnd pins the batch-as-upper-bound sizing: the unit end is
// trimmed to the exclusive watermark N_u-1 when it covers a, never underflows
// when the watermark is at or below a, never falls below a and never wraps on
// a+batch-1.
func TestDepositCoveredEnd(t *testing.T) {
	tests := []struct {
		name                    string
		a, batch, nextExclusive uint64
		want                    uint64
	}{
		{"single_covered_block_full_batch", 10, 500, 11, 10},
		{"partial_batch_tail", 10, 500, 14, 13},
		{"batch_cap_below_watermark", 10, 3, 14, 12},
		{"watermark_at_start_no_trim", 10, 500, 10, 509},
		{"watermark_below_start_no_trim", 10, 500, 9, 509},
		{"genesis_single_block", 0, 1, 1, 0},
		{"batch_one", 10, 1, 100, 10},
		{"overflow_guard", ^uint64(0), 2, 0, ^uint64(0)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := depositCoveredEnd(tc.a, tc.batch, tc.nextExclusive)
			if got != tc.want {
				t.Fatalf("depositCoveredEnd(%d,%d,%d) = %d, want %d", tc.a, tc.batch, tc.nextExclusive, got, tc.want)
			}
			if got < tc.a {
				t.Fatalf("depositCoveredEnd(%d,%d,%d) = %d violates b >= a", tc.a, tc.batch, tc.nextExclusive, got)
			}
		})
	}
}

func TestDepositNumericAmount(t *testing.T) {
	uint256Max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	n, err := depositNumericAmount(uint256Max.String())
	if err != nil {
		t.Fatalf("depositNumericAmount(uint256 max): %v", err)
	}
	if n.Int.Cmp(uint256Max) != 0 || n.Exp != 0 || !n.Valid {
		t.Fatalf("numeric = %+v, want %s exp 0 valid", n, uint256Max)
	}
	for _, bad := range []string{"", "0", "-1", "1.5", "0x10", "12a"} {
		if _, err := depositNumericAmount(bad); err == nil {
			t.Fatalf("depositNumericAmount(%q) = nil error, want rejection", bad)
		}
	}
}

func TestDepositLoopTimingsDefaults(t *testing.T) {
	sc := &DepositScanner{cfg: testDepositConfig(t)}
	poll, initial, max := sc.loopTimings()
	if poll != depositDefaultPollInterval || initial != depositDefaultRetryInitial || max != depositDefaultRetryMax {
		t.Fatalf("loopTimings() = %s/%s/%s, want defaults", poll, initial, max)
	}
	sc.cfg.PollInterval = 3 * time.Second
	sc.cfg.RetryInitial = 2 * time.Second
	sc.cfg.RetryMax = time.Second // below initial: raised
	poll, initial, max = sc.loopTimings()
	if poll != 3*time.Second || initial != 2*time.Second || max != 2*time.Second {
		t.Fatalf("loopTimings() = %s/%s/%s, want 3s/2s/2s", poll, initial, max)
	}
}

func TestDepositVerifyProgressIdentity(t *testing.T) {
	cfg := testDepositConfig(t)
	sc := &DepositScanner{cfg: cfg}
	if err := sc.verifyProgressIdentity(nil); err != nil {
		t.Fatalf("nil progress: %v", err)
	}
	ok := &depositProgress{startBlock: cfg.StartBlock, configHash: cfg.ConfigHash, nextBlock: 1}
	if err := sc.verifyProgressIdentity(ok); err != nil {
		t.Fatalf("matching progress: %v", err)
	}
	for name, p := range map[string]*depositProgress{
		"start_block": {startBlock: cfg.StartBlock + 1, configHash: cfg.ConfigHash, nextBlock: 1},
		"config_hash": {startBlock: cfg.StartBlock, configHash: strings.Repeat("bb", 32), nextBlock: 1},
	} {
		var mismatch *depositConfigMismatchError
		if err := sc.verifyProgressIdentity(p); !errors.As(err, &mismatch) {
			t.Fatalf("%s: error = %v (%T), want *depositConfigMismatchError", name, err, err)
		}
	}
}

func TestDepositMatchConfigNormalizes(t *testing.T) {
	cfg := testDepositConfig(t)
	cfg.StartBlock = 7
	cfg.Assets = []config.DepositEntry{{Address: strings.ToUpper(testContractA), Effective: 5}}
	cfg.Watches = []config.DepositEntry{{Address: strings.ToUpper(depositWatchAddr), Effective: 6}}
	sc := &DepositScanner{cfg: cfg}
	match := sc.matchConfig()
	if match.startBlock != 7 {
		t.Fatalf("startBlock = %d, want 7", match.startBlock)
	}
	if match.assets[testContractA] != 5 {
		t.Fatalf("assets = %v, want lowercase key %s:5", match.assets, testContractA)
	}
	if match.watches[depositWatchAddr] != 6 {
		t.Fatalf("watches = %v, want lowercase key %s:6", match.watches, depositWatchAddr)
	}
}

var (
	depositInsertIntoRe = regexp.MustCompile(`(?i)INSERT\s+INTO\s+([A-Za-z_]\w*)`)
	depositUpdateObsRe  = regexp.MustCompile(`(?i)\bUPDATE\s+deposit_observations\b`)
	depositDeleteObsRe  = regexp.MustCompile(`(?i)\bDELETE\s+FROM\s+deposit_observations\b`)
)

// TestDepositWritePathConfinement is the T020 FR-13/14 negative assertion
// (grep assertion), extended by 005 T023 (partial: grep gate only; the
// zero-row-rewrite behavior is proven by confirmcommit_integration_test.go
// and confirmation_integration_test.go, not by this static scan):
// balance (amount) writes land only in deposit_observations via exactly one
// INSERT; observation rows are never deleted; the ONLY permitted UPDATE of
// deposit_observations is the approved 005 confirmation transition in
// confirmcommit.go (conditional Pending→Confirmed with the status='pending'
// predicate and the seven approved SET assignments, data-model §提交协议
// step 4); confirmation_policy_history is append-only (INSERT, never UPDATE),
// with exactly two approved INSERT sites: the first-confirm bootstrap row in
// confirmcommit.go and the authorized switch row in confirmauth.go (F-R1).
// This gate is write-path regression protection, NOT database access
// control: anyone holding the write DSN can technically write past it
// (see runbook trust boundary); the gate only guarantees the approved
// binary paths stay the sole code paths.
// Pending literals are pinned per file so a new write path or predicate
// fails loudly instead of slipping past the shape-pinning tests above.
// Production SQL lives in raw string literals; the per-statement window ends
// at the closing backtick (capped at 1500 bytes), so a window can never bleed
// into neighboring Go code.
var (
	depositConfirmUpdateFile = "confirmcommit.go"
	depositConfirmUpdateRe   = regexp.MustCompile(`(?i)\bUPDATE\s+deposit_observations\b`)
	depositPolicyUpdateRe    = regexp.MustCompile(`(?i)\bUPDATE\s+confirmation_policy_history\b`)
	depositConfirmSetRes     = []string{
		`SET\s+status\s*=\s*'confirmed'`,
		`confirmed_at\s*=\s*now\(\)`,
		`confirm_tip_number\s*=`,
		`confirm_tip_hash\s*=`,
		`confirm_threshold\s*=`,
		`confirmations\s*=`,
		`confirm_policy_seq\s*=`,
	}
	depositConfirmPredicateRe = regexp.MustCompile(`(?i)\bAND\s+status\s*=\s*'pending'`)
	depositSetStatusRe        = regexp.MustCompile(`(?i)\bstatus\s*=`)

	// Approved INSERT sites into confirmation_policy_history (F-R1): the
	// bootstrap row (first-confirm transaction) and the authorized switch
	// row. A third INSERT site fails the gate; enforcement against
	// out-of-binary writes is the DSN trust boundary, not this scan.
	depositPolicyInsertAllow = map[string]int{
		"confirmcommit.go": 1, // insertConfirmationBootstrapSQL
		"confirmauth.go":   1, // authorized switch single-row INSERT
	}

	// Approved "pending" literal sites (comment-stripped bodies). A new
	// literal anywhere else fails the gate.
	depositDoublePendingAllow = map[string]int{
		"depositcommit.go": 1, // depositObservationStatusPending const
		"confirmcommit.go": 2, // candidate status comparisons (re-read guards)
	}
	depositSinglePendingAllow = map[string]int{
		"confirmcommit.go": 1, // the conditional UPDATE predicate
		"confirmscan.go":   2, // read-only candidate + count WHERE filters
	}
)

func TestDepositWritePathConfinement(t *testing.T) {
	pkgDir := depositSourceDir(t)
	entries, err := os.ReadDir(pkgDir)
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	amountInserts := 0
	updateFiles := map[string]int{}
	policyInserts := map[string]int{}
	doublePending := map[string]int{}
	singlePending := map[string]int{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(pkgDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		body := depositStripGoComments(string(raw))
		for _, loc := range depositInsertIntoRe.FindAllStringSubmatchIndex(body, -1) {
			table := body[loc[2]:loc[3]]
			end := len(body)
			if k := strings.IndexByte(body[loc[1]:], '`'); k >= 0 {
				end = loc[1] + k
			}
			if end-loc[0] > 1500 {
				end = loc[0] + 1500
			}
			if window := body[loc[0]:end]; strings.Contains(window, "amount") {
				if table != "deposit_observations" {
					t.Errorf("%s: amount write targets %s, want deposit_observations only", name, table)
				}
				amountInserts++
			}
			if table == "confirmation_policy_history" {
				policyInserts[name]++
			}
		}
		if n := len(depositConfirmUpdateRe.FindAllStringIndex(body, -1)); n > 0 {
			updateFiles[name] += n
		}
		if depositPolicyUpdateRe.MatchString(body) {
			t.Errorf("%s: UPDATE of confirmation_policy_history is forbidden (policy is append-only)", name)
		}
		if depositDeleteObsRe.MatchString(body) {
			t.Errorf("%s: DELETE FROM deposit_observations is forbidden", name)
		}
		doublePending[name] += strings.Count(body, `"pending"`)
		singlePending[name] += strings.Count(body, `'pending'`)
	}
	if amountInserts != 1 {
		t.Errorf("amount INSERT statements = %d, want exactly 1 (insertDepositObservationSQL)", amountInserts)
	}
	for name, want := range depositPolicyInsertAllow {
		if got := policyInserts[name]; got != want {
			t.Errorf("INSERT INTO confirmation_policy_history in %s = %d, want %d", name, got, want)
		}
	}
	for name, got := range policyInserts {
		if _, ok := depositPolicyInsertAllow[name]; !ok && got != 0 {
			t.Errorf("unexpected INSERT INTO confirmation_policy_history in %s = %d, want 0", name, got)
		}
	}
	// The single approved 005 write path: exactly one UPDATE of
	// deposit_observations, in confirmcommit.go, carrying the
	// status='pending' predicate and only the approved SET assignments.
	if len(updateFiles) != 1 || updateFiles[depositConfirmUpdateFile] != 1 {
		t.Errorf("UPDATE deposit_observations sites = %v, want exactly 1 in %s (confirmDepositObservationSQL)",
			updateFiles, depositConfirmUpdateFile)
	} else {
		raw, err := os.ReadFile(filepath.Join(pkgDir, depositConfirmUpdateFile))
		if err != nil {
			t.Fatalf("read %s: %v", depositConfirmUpdateFile, err)
		}
		body := depositStripGoComments(string(raw))
		loc := depositConfirmUpdateRe.FindStringIndex(body)
		window := body[loc[0]:]
		if k := strings.IndexByte(window, '`'); k >= 0 {
			window = window[:k]
		}
		if len(window) > 1500 {
			window = window[:1500]
		}
		if !depositConfirmPredicateRe.MatchString(window) {
			t.Errorf("%s: approved UPDATE lacks the status='pending' predicate", depositConfirmUpdateFile)
		}
		for _, re := range depositConfirmSetRes {
			if !regexp.MustCompile(re).MatchString(window) {
				t.Errorf("%s: approved UPDATE lacks required assignment %s", depositConfirmUpdateFile, re)
			}
		}
		setRegion := window
		if k := strings.Index(strings.ToUpper(window), "WHERE"); k >= 0 {
			setRegion = window[:k]
		}
		if n := len(depositSetStatusRe.FindAllStringIndex(setRegion, -1)); n != 1 {
			t.Errorf("%s: SET region assigns status %d times, want exactly 1 (to 'confirmed')",
				depositConfirmUpdateFile, n)
		}
		if strings.Contains(setRegion, "'pending'") {
			t.Errorf("%s: SET region must not assign 'pending'", depositConfirmUpdateFile)
		}
	}
	for name, want := range depositDoublePendingAllow {
		if got := doublePending[name]; got != want {
			t.Errorf(`%s: "pending" literals = %d, want %d`, name, got, want)
		}
	}
	for name, got := range doublePending {
		if _, ok := depositDoublePendingAllow[name]; !ok && got != 0 {
			t.Errorf(`%s: unexpected "pending" literals = %d, want 0`, name, got)
		}
	}
	for name, want := range depositSinglePendingAllow {
		if got := singlePending[name]; got != want {
			t.Errorf(`%s: 'pending' literals = %d, want %d`, name, got, want)
		}
	}
	for name, got := range singlePending {
		if _, ok := depositSinglePendingAllow[name]; !ok && got != 0 {
			t.Errorf(`%s: unexpected 'pending' literals = %d, want 0`, name, got)
		}
	}
	// Outside internal/indexer only the metrics name may mention the table.
	root := filepath.Join(pkgDir, "..", "..")
	prefix := "internal" + string(filepath.Separator) + "indexer" + string(filepath.Separator)
	if err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if base := filepath.Base(path); base == ".git" || base == ".slim" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if rel, err := filepath.Rel(root, path); err != nil || strings.HasPrefix(rel, prefix) {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.Contains(line, "deposit_observations") &&
				!strings.Contains(line, "txharbor_deposit_observations_total") {
				rel, _ := filepath.Rel(root, path)
				t.Errorf("%s: references deposit_observations outside internal/indexer", rel)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("walk repo: %v", err)
	}
}

func depositSourceDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(file)
}

// depositStripGoComments removes // line comments and /* block */ comments so
// keyword scans do not trip on prose. It is approximate (a // inside a string
// literal truncates the line) but statement windows are backtick-bounded, so a
// truncated line can only shrink a window, never widen one past its literal.
func depositStripGoComments(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		if strings.HasPrefix(s[i:], "//") {
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		}
		if strings.HasPrefix(s[i:], "/*") {
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				break
			}
			i += 2 + end + 2
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}
