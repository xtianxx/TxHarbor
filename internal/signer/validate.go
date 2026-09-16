// validate.go owns the field validation matrix (T007; FR-06–FR-11,
// contracts/api.md §2): chain/sender/asset/calldata/recipient/amount/fee
// shapes, EIP-55 acceptance, integer-only money. Membership and caps belong
// to policy.go (T008); presence/types belong to DecodeRequest (T006).
package signer

import (
	"bytes"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// transferSelector is ERC-20 transfer(address,uint256).
var transferSelector = [4]byte{0xa9, 0x05, 0x9c, 0xbb}

// maxUint256 is 2^256-1, the largest value the NUMERIC(78,0) money columns
// accept (migration 000009 signing_requests_amount_check). Over-limit amounts
// die here so they never reach the database (FR-10; V5 "over-uint256 amount").
var maxUint256 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))

// checkAddress accepts lowercase hex or mixed-case that passes EIP-55 and
// returns the canonical lowercase form.
func checkAddress(field, s string) (string, error) {
	if !common.IsHexAddress(s) {
		return "", refuse(ClassValidationFailed, field, "not a 0x hex address")
	}
	if s != strings.ToLower(s) {
		ma, err := common.NewMixedcaseAddressFromString(s)
		if err != nil || !ma.ValidChecksum() {
			return "", refuse(ClassValidationFailed, field, "mixed-case address fails EIP-55 checksum")
		}
	}
	return strings.ToLower(common.HexToAddress(s).Hex()), nil
}

// Validate checks every field's shape and cross-field semantics, returning
// ClassValidationFailed naming the field.
func Validate(r *Request) error {
	if r.ChainID == 0 {
		return refuse(ClassValidationFailed, "chain_id", "chain_id must be non-zero")
	}
	if r.TxType != 0 && r.TxType != 2 {
		return refuse(ClassValidationFailed, "tx_type", "tx_type must be 0 or 2 in v1")
	}
	if _, err := checkAddress("sender", r.Sender); err != nil {
		return err
	}
	to, err := checkAddress("to", r.To)
	if err != nil {
		return err
	}
	asset, err := checkAddress("asset", r.Asset)
	if err != nil {
		return err
	}
	if asset != to {
		return refuse(ClassValidationFailed, "asset", "asset must equal to")
	}
	recipient, err := checkAddress("recipient", r.Recipient)
	if err != nil {
		return err
	}
	if _, err := decimalUint64("nonce", r.Nonce); err != nil {
		return err
	}
	value, err := decimalBig("value", r.Value)
	if err != nil {
		return err
	}
	if value.Sign() != 0 {
		return refuse(ClassValidationFailed, "value", `value must be "0": native currency is gas-only in v1`)
	}
	amount, err := decimalBig("amount", r.Amount)
	if err != nil {
		return err
	}
	if amount.Sign() <= 0 {
		return refuse(ClassValidationFailed, "amount", "amount must be positive")
	}
	if amount.Cmp(maxUint256) > 0 {
		return refuse(ClassValidationFailed, "amount", "amount exceeds the uint256 maximum (2^256-1)")
	}
	gasLimit, err := decimalUint64("gas_limit", r.GasLimit)
	if err != nil {
		return err
	}
	if gasLimit == 0 {
		return refuse(ClassValidationFailed, "gas_limit", "gas_limit must be positive")
	}
	if err := checkFees(r); err != nil {
		return err
	}
	if err := checkCalldata(r, recipient, amount); err != nil {
		return err
	}
	return nil
}

// checkFees enforces fee shape per tx_type and positive fees with
// max_priority_fee_per_gas <= max_fee_per_gas.
func checkFees(r *Request) error {
	positive := func(field, s string) (*big.Int, error) {
		v, err := decimalBig(field, s)
		if err != nil {
			return nil, err
		}
		if v.Sign() <= 0 {
			return nil, refuse(ClassValidationFailed, field, field+" must be positive")
		}
		return v, nil
	}
	switch r.TxType {
	case 0:
		if r.GasPrice == "" {
			return refuse(ClassValidationFailed, "gas_price", "tx_type 0 requires gas_price")
		}
		if r.MaxFeePerGas != "" || r.MaxPriorityFeePerGas != "" {
			return refuse(ClassValidationFailed, "gas_price", "tx_type 0 takes gas_price only")
		}
		_, err := positive("gas_price", r.GasPrice)
		return err
	case 2:
		if r.MaxFeePerGas == "" || r.MaxPriorityFeePerGas == "" {
			return refuse(ClassValidationFailed, "max_fee_per_gas", "tx_type 2 requires max_fee_per_gas + max_priority_fee_per_gas")
		}
		if r.GasPrice != "" {
			return refuse(ClassValidationFailed, "gas_price", "tx_type 2 takes max fees only")
		}
		maxFee, err := positive("max_fee_per_gas", r.MaxFeePerGas)
		if err != nil {
			return err
		}
		maxPriority, err := positive("max_priority_fee_per_gas", r.MaxPriorityFeePerGas)
		if err != nil {
			return err
		}
		if maxPriority.Cmp(maxFee) > 0 {
			return refuse(ClassValidationFailed, "max_priority_fee_per_gas", "max_priority_fee_per_gas must not exceed max_fee_per_gas")
		}
		return nil
	default:
		return refuse(ClassValidationFailed, "tx_type", "tx_type must be 0 or 2 in v1")
	}
}

// checkCalldata enforces the v1 transfer shape: 0x-hex, 4-byte selector +
// two 32-byte words, selector transfer(address,uint256), first word
// zero-padded to the recipient, second word equal to amount.
func checkCalldata(r *Request, recipient string, amount *big.Int) error {
	raw, err := hexutil.Decode(r.Data)
	if err != nil {
		return refuse(ClassValidationFailed, "data", "data must be 0x-hex")
	}
	if len(raw) != 4+32+32 || !bytes.Equal(raw[:4], transferSelector[:]) {
		return refuse(ClassValidationFailed, "data", "data must be transfer(address,uint256) calldata")
	}
	if !bytes.Equal(raw[4:4+12], make([]byte, 12)) {
		return refuse(ClassValidationFailed, "data", "recipient word must be zero-padded")
	}
	if got := common.BytesToAddress(raw[4+12 : 4+32]).Hex(); !strings.EqualFold(got, recipient) {
		return refuse(ClassValidationFailed, "data", "calldata recipient differs from recipient")
	}
	if got := new(big.Int).SetBytes(raw[4+32:]); got.Cmp(amount) != 0 {
		return refuse(ClassValidationFailed, "data", "calldata amount differs from amount")
	}
	return nil
}
