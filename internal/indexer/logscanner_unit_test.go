package indexer

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xtianxx/txharbor/internal/eth"
)

const (
	testContractA = "0x1111111111111111111111111111111111111111"
	testContractB = "0x2222222222222222222222222222222222222222"
)

// testLogScanner builds the minimal receiver validateLog/validateLogs need;
// no database or RPC is involved.
func testLogScanner(contracts ...string) *LogScanner {
	s := &LogScanner{chainID: 7, whitelist: make(map[string]struct{}, len(contracts))}
	for _, c := range contracts {
		s.whitelist[strings.ToLower(c)] = struct{}{}
	}
	return s
}

func testLog(block uint64, contract string) types.Log {
	return types.Log{
		Address: common.HexToAddress(contract),
		Topics: []common.Hash{
			eth.TransferSig,
			common.HexToHash("0x000000000000000000000000" + strings.Repeat("11", 20)),
			common.HexToHash("0x000000000000000000000000" + strings.Repeat("22", 20)),
		},
		Data:        bytes.Repeat([]byte{0x00}, 32),
		BlockNumber: block,
		BlockHash:   common.HexToHash("0x" + strings.Repeat("ab", 32)),
		TxHash:      common.HexToHash("0x" + strings.Repeat("cd", 32)),
		Index:       3,
	}
}

func testCoverage(a, b uint64, l types.Log) map[uint64]string {
	m := make(map[uint64]string, b-a+1)
	for n := a; n <= b; n++ {
		m[n] = hashHex(l.BlockHash)
	}
	return m
}

func deepCopyLog(l types.Log) types.Log {
	l.Topics = append([]common.Hash(nil), l.Topics...)
	l.Data = append([]byte(nil), l.Data...)
	return l
}

func wantClass(t *testing.T, err error, class string) {
	t.Helper()
	var lv *logValidationError
	if !errors.As(err, &lv) {
		t.Fatalf("error = %v (%T), want *logValidationError", err, err)
	}
	if lv.class != class {
		t.Fatalf("class = %q, want %q (detail %q)", lv.class, class, lv.detail)
	}
}

func TestValidateLogAcceptsValidTransfer(t *testing.T) {
	s := testLogScanner(testContractA)
	a, b := uint64(10), uint64(20)
	l := testLog(a, testContractA)

	rows, err := s.validateLogs(a, b, testCoverage(a, b, l), []types.Log{l})
	if err != nil {
		t.Fatalf("validateLogs() error = %v, want nil", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1", len(rows))
	}
	row := rows[0]
	if row.blockNumber != a || row.logIndex != uint64(l.Index) {
		t.Fatalf("row = %+v, want block=%d index=%d", row, a, l.Index)
	}
	if row.blockHash != hashHex(l.BlockHash) || row.txHash != hashHex(l.TxHash) {
		t.Fatalf("row hashes = %s/%s, want %s/%s", row.blockHash, row.txHash, hashHex(l.BlockHash), hashHex(l.TxHash))
	}
	if row.contract != testContractA {
		t.Fatalf("contract = %q, want %q", row.contract, testContractA)
	}
	if row.topic0 != hashHex(eth.TransferSig) {
		t.Fatalf("topic0 = %q, want %q", row.topic0, hashHex(eth.TransferSig))
	}
	if want := "0x" + strings.Repeat("00", 32); row.data != want {
		t.Fatalf("data = %q, want %q (raw zero amount must be preserved)", row.data, want)
	}
}

func TestValidateLogFailureClasses(t *testing.T) {
	a, b := uint64(10), uint64(20)
	base := testLog(a, testContractA)
	coverage := testCoverage(a, b, base)

	tests := []struct {
		name   string
		class  string
		mutate func(*types.Log)
	}{
		{"bad_address", classBadAddress, func(l *types.Log) {
			l.Address = common.HexToAddress(testContractB)
		}},
		{"out_of_range", classOutOfRange, func(l *types.Log) { l.BlockNumber = b + 1 }},
		{"missing_tx_hash", classMissingField, func(l *types.Log) { l.TxHash = common.Hash{} }},
		{"missing_block_hash", classMissingField, func(l *types.Log) { l.BlockHash = common.Hash{} }},
		{"removed", classRemoved, func(l *types.Log) { l.Removed = true }},
		{"topics_count", classBadTopics, func(l *types.Log) { l.Topics = l.Topics[:2] }},
		{"topics_topic0", classBadTopics, func(l *types.Log) {
			l.Topics[0] = common.HexToHash("0x" + strings.Repeat("ff", 32))
		}},
		{"topics_padding", classBadTopics, func(l *types.Log) { l.Topics[1][0] = 0x01 }},
		{"bad_data", classBadData, func(l *types.Log) { l.Data = l.Data[:31] }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := testLogScanner(testContractA)
			l := deepCopyLog(base)
			tc.mutate(&l)
			_, err := s.validateLogs(a, b, coverage, []types.Log{l})
			wantClass(t, err, tc.class)
		})
	}
}

func TestValidateLogChainViewMismatch(t *testing.T) {
	s := testLogScanner(testContractA)
	a, b := uint64(10), uint64(20)
	l := testLog(a, testContractA)

	// The stored canonical hash at the log's height differs from the log's
	// block hash: a chain-view divergence, not a validation class.
	other := common.HexToHash("0x" + strings.Repeat("ef", 32))
	coverage := testCoverage(a, b, l)
	coverage[a] = hashHex(other)

	_, err := s.validateLogs(a, b, coverage, []types.Log{l})
	var cv *chainViewError
	if !errors.As(err, &cv) {
		t.Fatalf("error = %v (%T), want *chainViewError", err, err)
	}
	if cv.height != a || cv.actual != hashHex(l.BlockHash) || cv.expected != hashHex(other) {
		t.Fatalf("chainViewError = %+v, want height=%d expected=%s actual=%s",
			cv, a, hashHex(other), hashHex(l.BlockHash))
	}
}

func TestValidateLogsDedupAndConflict(t *testing.T) {
	s := testLogScanner(testContractA)
	a, b := uint64(10), uint64(20)
	base := testLog(a, testContractA)
	coverage := testCoverage(a, b, base)

	// Exact duplicate identity and content converges to one row (FR-09).
	rows, err := s.validateLogs(a, b, coverage, []types.Log{base, deepCopyLog(base)})
	if err != nil {
		t.Fatalf("exact duplicate must converge: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1 after dedup", len(rows))
	}

	// Same identity, one byte of data different: whole batch fails with
	// detail.class=identity_conflict (FR-10).
	conflict := deepCopyLog(base)
	conflict.Data = bytes.Repeat([]byte{0x01}, 32)
	_, err = s.validateLogs(a, b, coverage, []types.Log{base, conflict})
	wantClass(t, err, classIdentityConflict)

	// Same (block_hash, log_index) with a different tx_hash: the second
	// UNIQUE constraint, also identity_conflict regardless of content or
	// arrival order.
	otherTx := deepCopyLog(base)
	otherTx.TxHash = common.HexToHash("0x" + strings.Repeat("ee", 32))
	for _, batch := range [][]types.Log{{base, otherTx}, {otherTx, base}} {
		_, err = s.validateLogs(a, b, coverage, batch)
		wantClass(t, err, classIdentityConflict)
	}
}

// TestInsertLogFailureUniqueViolation pins the batch INSERT classification:
// the PK conflict is absorbed by ON CONFLICT DO NOTHING, so any 23505 is the
// block-scoped UNIQUE and must become an identity conflict (pause path), never
// a generic DB failure (infinite backoff).
func TestInsertLogFailureUniqueViolation(t *testing.T) {
	row := logRow{
		blockNumber: 10,
		blockHash:   "0x" + strings.Repeat("ab", 32),
		txHash:      "0x" + strings.Repeat("cd", 32),
		logIndex:    3,
		contract:    testContractA,
	}
	wrapped := fmt.Errorf("exec insert: %w", &pgconn.PgError{
		Code:           "23505",
		ConstraintName: "erc20_transfer_logs_chain_id_block_hash_log_index_key",
	})
	wantClass(t, insertLogFailure(row, wrapped), classIdentityConflict)

	for _, dbErr := range []error{
		errors.New("connection reset"),
		&pgconn.PgError{Code: "40001"}, // serialization_failure: retry, not a conflict
	} {
		var lv *logValidationError
		if got := insertLogFailure(row, dbErr); errors.As(got, &lv) {
			t.Fatalf("insertLogFailure(%v) = %v, want a plain DB failure", dbErr, got)
		}
	}
}

func TestShrinkSpanSequence(t *testing.T) {
	span := uint64(500)
	var got []uint64
	for {
		next, ok := shrinkSpan(span)
		if !ok {
			break
		}
		got = append(got, next)
		span = next
	}
	want := []uint64{250, 125, 62, 31, 15, 7, 3, 1}
	if len(got) != len(want) {
		t.Fatalf("shrink sequence = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("shrink sequence = %v, want %v", got, want)
		}
	}
	// A single block is the floor: the caller must stop with range_incomplete.
	if next, ok := shrinkSpan(1); ok || next != 0 {
		t.Fatalf("shrinkSpan(1) = (%d, %t), want (0, false)", next, ok)
	}
}

func TestCountIncomplete(t *testing.T) {
	if countIncomplete(0, 1_000_000) {
		t.Fatal("ResultLimit 0 must disable the count-based verdict")
	}
	if countIncomplete(5, 4) {
		t.Fatal("count below the limit is not incomplete")
	}
	if !countIncomplete(5, 5) {
		t.Fatal("count at the limit is incomplete")
	}
	if !countIncomplete(5, 6) {
		t.Fatal("count above the limit is incomplete")
	}
}

func TestCapSpan(t *testing.T) {
	if got := capSpan(10, 5, 100); got != 14 {
		t.Fatalf("capSpan(10,5,100) = %d, want 14", got)
	}
	if got := capSpan(10, 500, 12); got != 12 {
		t.Fatalf("capSpan(10,500,12) = %d, want 12 (coverage cap)", got)
	}
	if got := capSpan(math.MaxUint64-1, 500, math.MaxUint64); got != math.MaxUint64 {
		t.Fatalf("capSpan overflow guard = %d, want %d", got, uint64(math.MaxUint64))
	}
}

func TestValidConfigHash(t *testing.T) {
	if !validConfigHash(strings.Repeat("ab", 32)) {
		t.Fatal("64 lowercase hex must be valid")
	}
	for _, bad := range []string{
		"",
		strings.Repeat("ab", 31),
		strings.Repeat("AB", 32),
		strings.Repeat("zz", 32),
		"0x" + strings.Repeat("ab", 32),
	} {
		if validConfigHash(bad) {
			t.Fatalf("validConfigHash(%q) = true, want false", bad)
		}
	}
}

// TestLogWriteSQLShape pins the write-protocol statements the unit tests can
// reach without a database: guards, dedup conflicts and pause atomicity.
func TestLogWriteSQLShape(t *testing.T) {
	checks := []struct {
		name  string
		sql   string
		wants []string
	}{
		{"logCheckpointGuard", logCheckpointGuardSQL, []string{
			"log_checkpoint", "start_block = $2", "config_hash = $3", "next_block = $4",
		}},
		{"advanceLogCheckpoint", advanceLogCheckpointSQL, []string{
			"UPDATE log_checkpoint", "next_block = $2", "chain_id = $1",
			"start_block = $4", "config_hash = $5", "next_block = $3",
		}},
		{"insertLogCheckpoint", insertLogCheckpointSQL, []string{
			"INSERT INTO log_checkpoint", "ON CONFLICT (chain_id) DO NOTHING",
		}},
		{"insertLog", insertLogSQL, []string{
			"INSERT INTO erc20_transfer_logs",
			"ON CONFLICT (chain_id, block_hash, tx_hash, log_index) DO NOTHING",
		}},
		{"logPauseExists", logPauseExistsSQL, []string{"log_pause"}},
		{"insertLogPause", insertLogPauseSQL, []string{
			"INSERT INTO log_pause", "ON CONFLICT (chain_id) DO NOTHING",
		}},
		{"readLogRow", readLogRowSQL, []string{
			"contract", "block_number", "topic0", "topic1", "topic2", "data",
		}},
		{"canonicalBlockHash", canonicalBlockHashSQL, []string{"chain_blocks", "canonical"}},
	}
	for _, c := range checks {
		for _, w := range c.wants {
			if !strings.Contains(c.sql, w) {
				t.Errorf("%s: SQL does not contain %q", c.name, w)
			}
		}
	}
}
