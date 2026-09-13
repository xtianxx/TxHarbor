// depositparse.go is 004's pure Transfer parse/match layer: stored
// erc20_transfer_logs rows in, at most one matched deposit observation per row
// out. It re-checks the stored source (research R4: DB corruption or a future
// upstream bug must not silently become a deposit), applies the FR-05
// start/asset/watch effective-height triple as a closed interval, skips valid
// logs that do not match (FR-01/FR-03), and fails the whole batch on any
// structurally invalid row (FR-02/FR-10). Zero DB, zero RPC: behavior is
// locked by specs/004-deposit-detection/ (FR-01..FR-05, data-model Tables 1/3,
// research R3/R4).
package indexer

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	"github.com/xtianxx/txharbor/internal/eth"
)

// depositClass* are the data-model Table 3 detail.class values this layer can
// produce; depositcommit persists them in deposit_pause.detail.
const (
	depositClassBadAddress = "bad_address"
	depositClassBadTopics  = "bad_topics"
	depositClassBadAmount  = "bad_amount"
)

// depositMatchConfig is the pure matching view of the FR-04/FR-05 config: the
// global scan start plus normalized lowercase asset/watch sets carrying each
// entry's effective height. The caller normalizes and freezes it at startup.
type depositMatchConfig struct {
	startBlock uint64
	assets     map[string]uint64 // lowercase 0x contract -> effective height
	watches    map[string]uint64 // lowercase 0x address -> effective height
}

// depositSourceLog is one stored erc20_transfer_logs row, the read-only input
// of the parse/match layer. The string fields are the stored 0x hex forms
// (003 data-model Table 1): 20-byte contract and 32-byte topics/data.
type depositSourceLog struct {
	blockNumber uint64
	blockHash   string
	txHash      string
	logIndex    uint64
	contract    string
	topic0      string
	topic1      string
	topic2      string
	data        string
}

// depositObservation is one matched source log ready for the Table 1 insert:
// normalized addresses, an exact base-10 amount (no float anywhere), and the
// source/block identity carried through from the stored row.
type depositObservation struct {
	blockNumber uint64
	blockHash   string
	txHash      string
	logIndex    uint64
	contract    string
	sender      string
	recipient   string
	amount      string // base-10 integer, always > 0
}

// depositBatch is the successful outcome of parseDepositLogs. matched holds
// the observations in source order; nomatch and zero classify the remaining
// valid rows (observability contract: result=matched|nomatch|zero; an
// invalid row is the returned error, never a partial batch).
type depositBatch struct {
	matched []depositObservation
	nomatch int
	zero    int
}

// depositParseError is one deterministic structural failure; class matches
// data-model Table 3's detail.class set. Any such failure fails the whole
// batch (FR-10), so callers never see partial results.
type depositParseError struct {
	height uint64
	class  string
	detail string
}

func (e *depositParseError) Error() string { return e.detail }

// depositVerdict is one row's classification after a successful parse.
type depositVerdict int

const (
	depositNoMatch depositVerdict = iota
	depositZero
	depositMatched
)

// parseDepositLogs parses and matches one source batch. On success every row
// is matched, zero-valued or non-matching; on any invalid row the whole batch
// fails and returns the zero depositBatch (FR-10).
func parseDepositLogs(cfg depositMatchConfig, rows []depositSourceLog) (depositBatch, error) {
	batch := depositBatch{matched: make([]depositObservation, 0, len(rows))}
	for _, row := range rows {
		obs, verdict, err := cfg.parseLog(row)
		if err != nil {
			return depositBatch{}, err
		}
		switch verdict {
		case depositMatched:
			batch.matched = append(batch.matched, obs)
		case depositZero:
			batch.zero++
		default:
			batch.nomatch++
		}
	}
	return batch, nil
}

// parseLog applies the FR-02 structural re-check, the R4 topic extraction and
// the FR-01/FR-03/FR-04/FR-05 matching rules to one stored row.
func (cfg depositMatchConfig) parseLog(row depositSourceLog) (depositObservation, depositVerdict, error) {
	fail := func(class, format string, args ...any) (depositObservation, depositVerdict, error) {
		return depositObservation{}, depositNoMatch, &depositParseError{
			height: row.blockNumber,
			class:  class,
			detail: fmt.Sprintf("class=%s block=%d log_index=%d %s",
				class, row.blockNumber, row.logIndex, fmt.Sprintf(format, args...)),
		}
	}

	contract, ok := parseDepositAddress(row.contract)
	if !ok {
		return fail(depositClassBadAddress, "expected=20_byte_contract actual=%s", bounded(row.contract))
	}
	// topic0 is asserted against keccak256("Transfer(address,address,uint256)")
	// via eth.TransferSig (pinned by TestDepositTransferSigDerived), never a
	// hardcoded hex literal.
	topic0, ok := parseDepositHash(row.topic0)
	if !ok {
		return fail(depositClassBadTopics, "expected=topic0_%s actual=%s", eth.TransferSig.Hex(), bounded(row.topic0))
	}
	if topic0 != eth.TransferSig {
		return fail(depositClassBadTopics, "expected=topic0_%s actual=%s", eth.TransferSig.Hex(), topic0.Hex())
	}
	topic1, ok := parseDepositHash(row.topic1)
	if !ok || !zeroHigh12(topic1) {
		return fail(depositClassBadTopics, "expected=topic1_address_padded actual=%s", bounded(row.topic1))
	}
	topic2, ok := parseDepositHash(row.topic2)
	if !ok || !zeroHigh12(topic2) {
		return fail(depositClassBadTopics, "expected=topic2_address_padded actual=%s", bounded(row.topic2))
	}
	data, ok := depositHexFixed(row.data, 32)
	if !ok {
		return fail(depositClassBadAmount, "expected=32_byte_data actual=%s", bounded(row.data))
	}
	// big.Int only: the amount reaches the DB as this exact decimal string,
	// never through float64 (uint256 exceeds 2^53; R4/FR-02).
	amount := new(big.Int).SetBytes(data)
	sender := strings.ToLower(common.BytesToAddress(topic1[12:]).Hex())
	recipient := strings.ToLower(common.BytesToAddress(topic2[12:]).Hex())

	assetHeight, assetOK := cfg.assets[contract]
	watchHeight, watchOK := cfg.watches[recipient]
	if !assetOK || !watchOK ||
		row.blockNumber < cfg.startBlock ||
		row.blockNumber < assetHeight ||
		row.blockNumber < watchHeight {
		return depositObservation{}, depositNoMatch, nil
	}
	if amount.Sign() == 0 {
		return depositObservation{}, depositZero, nil
	}
	return depositObservation{
		blockNumber: row.blockNumber,
		blockHash:   row.blockHash,
		txHash:      row.txHash,
		logIndex:    row.logIndex,
		contract:    contract,
		sender:      sender,
		recipient:   recipient,
		amount:      amount.String(),
	}, depositMatched, nil
}

// depositHexFixed strictly decodes a 0x-prefixed hex string of exactly n
// bytes. common.HexToHash/HexToAddress pad and truncate silently, which would
// hide exactly the corruption this layer's re-check exists to catch.
func depositHexFixed(s string, n int) ([]byte, bool) {
	if len(s) != 2+2*n || !strings.HasPrefix(s, "0x") {
		return nil, false
	}
	b, err := hex.DecodeString(s[2:])
	if err != nil {
		return nil, false
	}
	return b, true
}

// parseDepositAddress decodes one stored 20-byte address and returns its
// normalized lowercase 0x form (FR-04).
func parseDepositAddress(s string) (string, bool) {
	b, ok := depositHexFixed(s, 20)
	if !ok {
		return "", false
	}
	return "0x" + hex.EncodeToString(b), true
}

// parseDepositHash decodes one stored 32-byte topic/hash.
func parseDepositHash(s string) (common.Hash, bool) {
	b, ok := depositHexFixed(s, 32)
	if !ok {
		return common.Hash{}, false
	}
	return common.BytesToHash(b), true
}

// bounded renders a stored source string for diagnostics without echoing
// unbounded tampered data (FR-15: no unlimited raw dumps).
func bounded(s string) string {
	const max = 80
	if len(s) > max {
		return fmt.Sprintf("<%d bytes>", len(s))
	}
	return fmt.Sprintf("%q", s)
}
