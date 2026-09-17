package txlifecycle

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/xtianxx/txharbor/internal/eth"
)

// transferSelector is the 4-byte ERC-20 transfer(address,uint256) selector,
// derived so it can never drift from the ABI text.
var transferSelector = crypto.Keccak256([]byte("transfer(address,uint256)"))[:4]

// TransferCalldata builds v1's ERC-20 transfer(recipient, amount) calldata:
// selector + 32-byte zero-padded recipient + 32-byte amount (R-010-08).
func TransferCalldata(recipient common.Address, amount *big.Int) []byte {
	out := make([]byte, 4+32+32)
	copy(out[0:4], transferSelector)
	copy(out[4:36], common.LeftPadBytes(recipient.Bytes(), 32))
	amount.FillBytes(out[36:68])
	return out
}

// ExpectedTransfer is the pinned semantics of the expected ERC-20 Transfer a
// receipt must carry (FR-08): emitter = asset, topic1 = sender, topic2 =
// recipient, 32-byte data = amount.
type ExpectedTransfer struct {
	Asset     common.Address
	Sender    common.Address
	Recipient common.Address
	Amount    *big.Int
}

// Match reports whether log is exactly the expected Transfer, returning a
// stable mismatch reason (for transfer_detail) when it is not. Extra logs are
// the caller's concern: this checks one log at a time.
func (e ExpectedTransfer) Match(log types.Log) (bool, string) {
	if log.Address != e.Asset {
		return false, "emitter"
	}
	if len(log.Topics) != 3 {
		return false, "topic_count"
	}
	if log.Topics[0] != eth.TransferSig {
		return false, "topic0"
	}
	if log.Topics[1] != common.BytesToHash(e.Sender.Bytes()) {
		return false, "sender"
	}
	if log.Topics[2] != common.BytesToHash(e.Recipient.Bytes()) {
		return false, "recipient"
	}
	if len(log.Data) != 32 {
		return false, "data_length"
	}
	if new(big.Int).SetBytes(log.Data).Cmp(e.Amount) != 0 {
		return false, "amount"
	}
	return true, ""
}

// FindExpectedTransfer scans logs for the expected Transfer and returns
// (found, reason): when no log matches, reason is the first candidate's
// mismatch detail, or "transfer_missing" when there is no ERC-20 Transfer at
// all.
func FindExpectedTransfer(logs []types.Log, expected ExpectedTransfer) (bool, string) {
	reason := "transfer_missing"
	for _, log := range logs {
		if len(log.Topics) == 0 || log.Topics[0] != eth.TransferSig {
			continue
		}
		if ok, why := expected.Match(log); ok {
			return true, ""
		} else {
			reason = why
		}
	}
	return false, reason
}
