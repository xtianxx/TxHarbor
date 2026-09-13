// depositparse_test.go locks the pure Transfer parse/match rules (T004):
// keccak-derived topic0, address padding/extraction, exact base-10 amounts
// with uint256 boundaries, the effective-height triple closed interval, exact
// whitelist/watch matching and the whole-batch failure semantics. No DB, no
// RPC, no float64.
package indexer

import (
	"bytes"
	"errors"
	"math/big"
	"regexp"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/xtianxx/txharbor/internal/eth"
)

const (
	depositWatchAddr = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	depositOtherAddr = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// depositDecimalOnly rejects any float or exponent artifact: the amount must
// be a plain base-10 integer string.
var depositDecimalOnly = regexp.MustCompile(`^[0-9]+$`)

// depositRow builds one stored source row: topic1/topic2 left-pad the given
// addresses (12 zero bytes + 20 address bytes), data is the 32-byte
// big-endian amount.
func depositRow(block uint64, contract string, sender, recipient common.Address, amount *big.Int) depositSourceLog {
	return depositSourceLog{
		blockNumber: block,
		blockHash:   "0x" + strings.Repeat("ab", 32),
		txHash:      "0x" + strings.Repeat("cd", 32),
		logIndex:    3,
		contract:    contract,
		topic0:      eth.TransferSig.Hex(),
		topic1:      common.BytesToHash(sender.Bytes()).Hex(),
		topic2:      common.BytesToHash(recipient.Bytes()).Hex(),
		data:        common.BigToHash(amount).Hex(),
	}
}

func depositCfg(start uint64, assets, watches map[string]uint64) depositMatchConfig {
	return depositMatchConfig{startBlock: start, assets: assets, watches: watches}
}

func wantDepositClass(t *testing.T, err error, class string) {
	t.Helper()
	var pe *depositParseError
	if !errors.As(err, &pe) {
		t.Fatalf("error = %v (%T), want *depositParseError", err, err)
	}
	if pe.class != class {
		t.Fatalf("class = %q, want %q (detail %q)", pe.class, class, pe.detail)
	}
}

// TestDepositTransferSigDerived pins that topic0 is asserted against
// keccak256("Transfer(address,address,uint256)") and not a magic hex literal
// (T004; 004 R4/FR-02).
func TestDepositTransferSigDerived(t *testing.T) {
	want := crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))
	if eth.TransferSig != want {
		t.Fatalf("eth.TransferSig = %s, want keccak256(...) = %s", eth.TransferSig.Hex(), want.Hex())
	}

	cfg := depositCfg(0, map[string]uint64{testContractA: 0}, map[string]uint64{depositWatchAddr: 0})
	row := depositRow(10, testContractA, common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))

	// The derived signature matches (mixed case is normalized).
	row.topic0 = "0x" + strings.ToUpper(want.Hex()[2:])
	batch, err := parseDepositLogs(cfg, []depositSourceLog{row})
	if err != nil || len(batch.matched) != 1 {
		t.Fatalf("topic0=keccak256(Transfer) must match: batch=%+v err=%v", batch, err)
	}

	// One byte off fails the whole batch.
	row.topic0 = "0x" + strings.Repeat("ff", 32)
	if _, err := parseDepositLogs(cfg, []depositSourceLog{row}); err == nil {
		t.Fatal("wrong topic0 must fail the batch")
	} else {
		wantDepositClass(t, err, depositClassBadTopics)
	}
}

func TestParseDepositLogsMatchesTransfer(t *testing.T) {
	cfg := depositCfg(0, map[string]uint64{testContractA: 0}, map[string]uint64{depositWatchAddr: 0})
	row := depositRow(10, testContractA, common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(1))

	batch, err := parseDepositLogs(cfg, []depositSourceLog{row})
	if err != nil {
		t.Fatalf("parseDepositLogs() error = %v, want nil", err)
	}
	if len(batch.matched) != 1 || batch.nomatch != 0 || batch.zero != 0 {
		t.Fatalf("batch = %+v, want exactly one matched observation", batch)
	}
	obs := batch.matched[0]
	if obs.blockNumber != 10 || obs.logIndex != 3 {
		t.Fatalf("obs block/index = %d/%d, want 10/3", obs.blockNumber, obs.logIndex)
	}
	if obs.blockHash != row.blockHash || obs.txHash != row.txHash {
		t.Fatalf("obs hashes = %s/%s, want %s/%s", obs.blockHash, obs.txHash, row.blockHash, row.txHash)
	}
	if obs.contract != testContractA {
		t.Fatalf("contract = %q, want %q", obs.contract, testContractA)
	}
	if obs.sender != testContractB {
		t.Fatalf("sender = %q, want topic1 low-20 %q", obs.sender, testContractB)
	}
	if obs.recipient != depositWatchAddr {
		t.Fatalf("recipient = %q, want topic2 low-20 %q", obs.recipient, depositWatchAddr)
	}
	if obs.amount != "1" {
		t.Fatalf("amount = %q, want %q", obs.amount, "1")
	}
}

// TestParseDepositLogsAmountBoundaries covers zero, 1 wei and uint256 max
// (2^256-1) with exact decimal strings; big.Int only, no float path.
func TestParseDepositLogsAmountBoundaries(t *testing.T) {
	const maxUint256Decimal = "115792089237316195423570985008687907853269984665640564039457584007913129639935"
	maxUint256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	if maxUint256.String() != maxUint256Decimal {
		t.Fatalf("test vector inconsistent: %s", maxUint256.String())
	}

	tests := []struct {
		name        string
		amount      *big.Int
		wantMatched bool
		wantAmount  string
	}{
		{"zero", big.NewInt(0), false, ""},
		{"one_wei", big.NewInt(1), true, "1"},
		{"uint256_max", maxUint256, true, maxUint256Decimal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := depositCfg(0, map[string]uint64{testContractA: 0}, map[string]uint64{depositWatchAddr: 0})
			row := depositRow(10, testContractA, common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), tc.amount)
			batch, err := parseDepositLogs(cfg, []depositSourceLog{row})
			if err != nil {
				t.Fatalf("parseDepositLogs() error = %v, want nil", err)
			}
			if !tc.wantMatched {
				if len(batch.matched) != 0 || batch.zero != 1 {
					t.Fatalf("batch = %+v, want zero-value legal log (no observation)", batch)
				}
				return
			}
			if len(batch.matched) != 1 || batch.zero != 0 {
				t.Fatalf("batch = %+v, want exactly one matched observation", batch)
			}
			if got := batch.matched[0].amount; got != tc.wantAmount {
				t.Fatalf("amount = %q, want %q", got, tc.wantAmount)
			}
		})
	}
}

// TestParseDepositLogsAmountExactRoundTrip proves the decimal string is a
// lossless encoding of the 32-byte big-endian amount (no truncation, no
// float64): parse the emitted string back and compare bytes.
func TestParseDepositLogsAmountExactRoundTrip(t *testing.T) {
	maxUint256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	values := []*big.Int{
		big.NewInt(1),
		new(big.Int).Lsh(big.NewInt(1), 64),
		new(big.Int).Lsh(big.NewInt(1), 255),
		maxUint256,
	}
	cfg := depositCfg(0, map[string]uint64{testContractA: 0}, map[string]uint64{depositWatchAddr: 0})
	for _, v := range values {
		row := depositRow(10, testContractA, common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), v)
		batch, err := parseDepositLogs(cfg, []depositSourceLog{row})
		if err != nil || len(batch.matched) != 1 {
			t.Fatalf("parseDepositLogs(%s) = %+v, %v", v, batch, err)
		}
		got := batch.matched[0].amount
		if !depositDecimalOnly.MatchString(got) {
			t.Fatalf("amount %q is not a plain base-10 integer (float artifact?)", got)
		}
		back, ok := new(big.Int).SetString(got, 10)
		if !ok || back.Cmp(v) != 0 {
			t.Fatalf("round trip amount = %q -> %v, want %s", got, back, v)
		}
		if !bytes.Equal(back.FillBytes(make([]byte, 32)), v.FillBytes(make([]byte, 32))) {
			t.Fatalf("round trip bytes differ for %s (amount %q)", v, got)
		}
	}
}

// TestParseDepositLogsEffectiveHeightTriple pins FR-05: eligible only when the
// block is >= global start, >= asset effective height and >= watch effective
// height, with equality included.
func TestParseDepositLogsEffectiveHeightTriple(t *testing.T) {
	sender := common.HexToAddress(testContractB)
	watch := common.HexToAddress(depositWatchAddr)

	tests := []struct {
		name    string
		start   uint64
		asset   uint64
		watch   uint64
		heights map[uint64]bool // block -> want matched
	}{
		{
			name: "triple_boundary", start: 50, asset: 60, watch: 70,
			heights: map[uint64]bool{49: false, 50: false, 59: false, 60: false, 69: false, 70: true, 71: true},
		},
		{
			name: "start_boundary", start: 100, asset: 0, watch: 0,
			heights: map[uint64]bool{99: false, 100: true},
		},
		{
			name: "asset_boundary", start: 0, asset: 5, watch: 0,
			heights: map[uint64]bool{4: false, 5: true},
		},
		{
			name: "watch_boundary", start: 0, asset: 0, watch: 7,
			heights: map[uint64]bool{6: false, 7: true},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := depositCfg(tc.start, map[string]uint64{testContractA: tc.asset}, map[string]uint64{depositWatchAddr: tc.watch})
			for block, wantMatched := range tc.heights {
				row := depositRow(block, testContractA, sender, watch, big.NewInt(1))
				batch, err := parseDepositLogs(cfg, []depositSourceLog{row})
				if err != nil {
					t.Fatalf("block %d: parseDepositLogs() error = %v", block, err)
				}
				if got := len(batch.matched) == 1; got != wantMatched {
					t.Fatalf("block %d: matched = %t, want %t (batch %+v)", block, got, wantMatched, batch)
				}
				if !wantMatched && batch.nomatch != 1 {
					t.Fatalf("block %d: nomatch = %d, want 1", block, batch.nomatch)
				}
			}
		})
	}
}

// TestParseDepositLogsExactMatching covers FR-04 normalization + exact set
// membership and the FR-01 non-match/no-observation outcomes.
func TestParseDepositLogsExactMatching(t *testing.T) {
	watch := common.HexToAddress(depositWatchAddr)
	other := common.HexToAddress(depositOtherAddr)
	cfg := depositCfg(0, map[string]uint64{testContractA: 0}, map[string]uint64{depositWatchAddr: 0})

	tests := []struct {
		name     string
		mutate   func(*depositSourceLog)
		want     depositVerdict
		wantSend string
	}{
		{"plain_match", func(*depositSourceLog) {}, depositMatched, testContractB},
		{"asset_not_whitelisted", func(r *depositSourceLog) { r.contract = testContractB }, depositNoMatch, ""},
		{"recipient_not_watched", func(r *depositSourceLog) {
			r.topic2 = common.BytesToHash(other.Bytes()).Hex()
		}, depositNoMatch, ""},
		{"uppercase_hex_normalizes", func(r *depositSourceLog) {
			r.contract = "0x" + strings.ToUpper(r.contract[2:])
			r.topic2 = "0x" + strings.ToUpper(r.topic2[2:])
		}, depositMatched, testContractB},
		{"self_transfer", func(r *depositSourceLog) {
			r.topic1 = common.BytesToHash(watch.Bytes()).Hex()
		}, depositMatched, depositWatchAddr},
		{"zero_to_unwatched_is_nomatch", func(r *depositSourceLog) {
			r.data = "0x" + strings.Repeat("00", 32)
			r.topic2 = common.BytesToHash(other.Bytes()).Hex()
		}, depositNoMatch, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			row := depositRow(10, testContractA, common.HexToAddress(testContractB), watch, big.NewInt(5))
			tc.mutate(&row)
			batch, err := parseDepositLogs(cfg, []depositSourceLog{row})
			if err != nil {
				t.Fatalf("parseDepositLogs() error = %v, want nil", err)
			}
			switch tc.want {
			case depositMatched:
				if len(batch.matched) != 1 || batch.nomatch+batch.zero != 0 {
					t.Fatalf("batch = %+v, want exactly one match", batch)
				}
				if batch.matched[0].sender != tc.wantSend {
					t.Fatalf("sender = %q, want %q", batch.matched[0].sender, tc.wantSend)
				}
			case depositNoMatch:
				if len(batch.matched) != 0 || batch.nomatch != 1 {
					t.Fatalf("batch = %+v, want one nomatch and no observation", batch)
				}
			}
		})
	}
}

// TestParseDepositLogsCounts pins the observability classification of a mixed
// valid batch: matched / nomatch / zero.
func TestParseDepositLogsCounts(t *testing.T) {
	cfg := depositCfg(0, map[string]uint64{testContractA: 0}, map[string]uint64{depositWatchAddr: 0})
	sender := common.HexToAddress(testContractB)
	watch := common.HexToAddress(depositWatchAddr)
	other := common.HexToAddress(depositOtherAddr)

	rows := []depositSourceLog{
		depositRow(10, testContractA, sender, watch, big.NewInt(5)), // matched
		depositRow(10, testContractA, sender, other, big.NewInt(5)), // recipient not watched
		depositRow(10, testContractA, sender, watch, big.NewInt(0)), // matched shape, zero
		depositRow(10, testContractB, sender, watch, big.NewInt(5)), // asset not whitelisted
	}
	batch, err := parseDepositLogs(cfg, rows)
	if err != nil {
		t.Fatalf("parseDepositLogs() error = %v, want nil", err)
	}
	if len(batch.matched) != 1 || batch.nomatch != 2 || batch.zero != 1 {
		t.Fatalf("batch = %+v, want matched=1 nomatch=2 zero=1", batch)
	}
}

// TestParseDepositLogsMalformedRows covers the FR-02 structural failures and
// their data-model Table 3 classes.
func TestParseDepositLogsMalformedRows(t *testing.T) {
	cfg := depositCfg(0, map[string]uint64{testContractA: 0}, map[string]uint64{depositWatchAddr: 0})
	base := depositRow(10, testContractA, common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(7))

	tests := []struct {
		name   string
		class  string
		mutate func(*depositSourceLog)
	}{
		{"bad_contract_empty", depositClassBadAddress, func(r *depositSourceLog) { r.contract = "" }},
		{"bad_contract_short", depositClassBadAddress, func(r *depositSourceLog) { r.contract = "0x1234" }},
		{"bad_contract_unbounded", depositClassBadAddress, func(r *depositSourceLog) {
			r.contract = strings.Repeat("f", 10_000)
		}},
		{"bad_topic0_empty", depositClassBadTopics, func(r *depositSourceLog) { r.topic0 = "" }},
		{"bad_topic0_mismatch", depositClassBadTopics, func(r *depositSourceLog) {
			r.topic0 = "0x" + strings.Repeat("ff", 32)
		}},
		{"bad_topic1_short", depositClassBadTopics, func(r *depositSourceLog) { r.topic1 = "0x00" }},
		{"bad_topic1_nonhex", depositClassBadTopics, func(r *depositSourceLog) {
			r.topic1 = "0x" + strings.Repeat("zz", 32)
		}},
		{"bad_topic1_padding", depositClassBadTopics, func(r *depositSourceLog) {
			r.topic1 = "0x01" + strings.Repeat("00", 31)
		}},
		{"bad_topic2_padding", depositClassBadTopics, func(r *depositSourceLog) {
			r.topic2 = "0x01" + strings.Repeat("00", 31)
		}},
		{"bad_data_truncated", depositClassBadAmount, func(r *depositSourceLog) { r.data = r.data[:64] }},
		{"bad_data_long", depositClassBadAmount, func(r *depositSourceLog) { r.data += "00" }},
		{"bad_data_nonhex", depositClassBadAmount, func(r *depositSourceLog) {
			r.data = "0x" + strings.Repeat("zz", 32)
		}},
		{"bad_data_empty", depositClassBadAmount, func(r *depositSourceLog) { r.data = "" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			row := base
			tc.mutate(&row)
			batch, err := parseDepositLogs(cfg, []depositSourceLog{row})
			if err == nil {
				t.Fatalf("parseDepositLogs() = %+v, want error", batch)
			}
			wantDepositClass(t, err, tc.class)
			if len(batch.matched)+batch.nomatch+batch.zero != 0 {
				t.Fatalf("failed batch leaked partial result: %+v", batch)
			}
			if tc.name == "bad_contract_unbounded" && len(err.Error()) > 200 {
				t.Fatalf("error detail is not bounded: %d chars", len(err.Error()))
			}
		})
	}
}

// TestParseDepositLogsAnyInvalidFailsWholeBatch pins FR-10: one invalid row
// fails the batch in either arrival order, with no partial observations.
func TestParseDepositLogsAnyInvalidFailsWholeBatch(t *testing.T) {
	cfg := depositCfg(0, map[string]uint64{testContractA: 0}, map[string]uint64{depositWatchAddr: 0})
	good := depositRow(10, testContractA, common.HexToAddress(testContractB), common.HexToAddress(depositWatchAddr), big.NewInt(7))
	bad := good
	bad.data = "0x00"

	for _, rows := range [][]depositSourceLog{{good, bad}, {bad, good}} {
		batch, err := parseDepositLogs(cfg, rows)
		if err == nil {
			t.Fatal("invalid row must fail the whole batch")
		}
		wantDepositClass(t, err, depositClassBadAmount)
		if len(batch.matched)+batch.nomatch+batch.zero != 0 {
			t.Fatalf("failed batch leaked partial result: %+v", batch)
		}
	}
}

func TestParseDepositLogsEmptyBatch(t *testing.T) {
	cfg := depositCfg(0, map[string]uint64{testContractA: 0}, map[string]uint64{depositWatchAddr: 0})
	batch, err := parseDepositLogs(cfg, nil)
	if err != nil {
		t.Fatalf("parseDepositLogs(nil) error = %v, want nil", err)
	}
	if len(batch.matched) != 0 || batch.nomatch != 0 || batch.zero != 0 {
		t.Fatalf("empty batch = %+v, want all zero", batch)
	}
}
