// content.go owns the strict request schema, the canonical envelope and its
// keccak256 content hash, `types.Transaction` reconstruction, arbitrary-digest
// rejection, and the independent-verification helper (T006; FR-01/02/03/13/16,
// contracts/api.md §2). Field semantics (EIP-55, calldata, fee matrix) belong
// to validate.go (T007); allowlists/caps belong to policy.go (T008).
package signer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// RefusalError carries a machine refusal class (errors.go) plus the offending
// request field when one exists. It is the error value returned by the
// content, validation, policy, auth and provider paths; the serving layer
// maps Class to HTTP status per contracts/api.md §2.
type RefusalError struct {
	Class RefusalClass
	Field string
	Msg   string
}

func (e *RefusalError) Error() string {
	if e.Field != "" {
		return string(e.Class) + ": " + e.Field + ": " + e.Msg
	}
	return string(e.Class) + ": " + e.Msg
}

// refuse builds a RefusalError.
func refuse(class RefusalClass, field, msg string) *RefusalError {
	return &RefusalError{Class: class, Field: field, Msg: msg}
}

// Request is the strict schema of one submit body (api.md §2). All fields are
// required; amounts/nonces/fees are decimal strings, data is 0x-hex, and
// chain_id/recovery_version/tx_type are JSON numbers. Fee presence per shape
// is enforced at reconstruction; fee semantics belong to T007.
type Request struct {
	SigningRequestID     string `json:"signing_request_id"`
	AttemptID            string `json:"attempt_id"`
	IntentID             string `json:"intent_id"`
	BindingRef           string `json:"binding_ref"`
	RecoveryVersion      uint64 `json:"recovery_version"`
	ChainID              uint64 `json:"chain_id"`
	Sender               string `json:"sender"`
	Nonce                string `json:"nonce"`
	TxType               uint8  `json:"tx_type"`
	To                   string `json:"to"`
	Value                string `json:"value"`
	Data                 string `json:"data"`
	GasLimit             string `json:"gas_limit"`
	GasPrice             string `json:"gas_price"`
	MaxFeePerGas         string `json:"max_fee_per_gas"`
	MaxPriorityFeePerGas string `json:"max_priority_fee_per_gas"`
	Asset                string `json:"asset"`
	Recipient            string `json:"recipient"`
	Amount               string `json:"amount"`
	AuthorizationID      string `json:"authorization_id"`
}

// requiredKeys are always-required JSON keys (fee keys are shape-conditional).
var requiredKeys = []string{
	"signing_request_id", "attempt_id", "intent_id", "binding_ref",
	"recovery_version", "chain_id", "sender", "nonce", "tx_type", "to",
	"value", "data", "gas_limit", "asset", "recipient", "amount",
	"authorization_id",
}

// feeKeys are the conditional fee-shape keys.
var feeKeys = []string{"gas_price", "max_fee_per_gas", "max_priority_fee_per_gas"}

// digestKeys are keys that carry a digest/hash/message instead of a complete
// transaction. A body built only of these is refused as
// arbitrary_digest_rejected and is never signed (api.md §2).
var digestKeys = map[string]bool{
	"digest": true, "hash": true, "message": true, "message_hash": true,
	"signing_hash": true, "sighash": true, "tx_hash": true, "payload": true,
	"payload_hash": true, "raw": true, "raw_tx": true, "raw_transaction": true,
}

// txMarkers are keys that indicate complete-transaction content.
var txMarkers = map[string]bool{
	"chain_id": true, "sender": true, "tx_type": true, "nonce": true,
	"to": true, "value": true, "data": true, "gas_limit": true,
	"gas_price": true, "max_fee_per_gas": true, "max_priority_fee_per_gas": true,
}

// DecodeRequest parses a submit body with strict schema: JSON object, no
// unknown fields, no nulls, no trailing data, all required keys present, and
// at least one fee-shape key. Digest-only bodies are refused before
// unknown-field checks so they report arbitrary_digest_rejected (api.md §4).
func DecodeRequest(body []byte) (Request, error) {
	var req Request
	dec := json.NewDecoder(bytes.NewReader(body))
	var fields map[string]json.RawMessage
	if err := dec.Decode(&fields); err != nil {
		return req, refuse(ClassMalformedRequest, "", "body is not a JSON object")
	}
	if dec.More() {
		return req, refuse(ClassMalformedRequest, "", "trailing data after JSON object")
	}

	hasDigest, hasTx := false, false
	for k := range fields {
		if digestKeys[k] {
			hasDigest = true
		}
		if txMarkers[k] {
			hasTx = true
		}
	}
	if hasDigest && !hasTx {
		return req, refuse(ClassArbitraryDigestRejected, "", "digest/hash/message without complete transaction content is never signed")
	}

	allowed := map[string]bool{}
	for _, k := range requiredKeys {
		allowed[k] = true
	}
	for _, k := range feeKeys {
		allowed[k] = true
	}
	for k, raw := range fields {
		if !allowed[k] {
			return req, refuse(ClassMalformedRequest, k, "unknown field")
		}
		if string(bytes.TrimSpace(raw)) == "null" {
			return req, refuse(ClassMalformedRequest, k, "null is not a value")
		}
	}
	for _, k := range requiredKeys {
		if _, ok := fields[k]; !ok {
			return req, refuse(ClassValidationFailed, k, "missing required field")
		}
	}
	feePresent := false
	for _, k := range feeKeys {
		if _, ok := fields[k]; ok {
			feePresent = true
		}
	}
	if !feePresent {
		return req, refuse(ClassValidationFailed, "gas_price", "missing fee shape: exactly one of gas_price or max_fee_per_gas + max_priority_fee_per_gas")
	}

	dec2 := json.NewDecoder(bytes.NewReader(body))
	dec2.DisallowUnknownFields()
	if err := dec2.Decode(&req); err != nil {
		return req, refuse(ClassMalformedRequest, "", "wrong JSON type for a field")
	}
	var extra json.RawMessage
	if err := dec2.Decode(&extra); err != io.EOF {
		return req, refuse(ClassMalformedRequest, "", "trailing data after JSON object")
	}
	return req, nil
}

// decimalBig parses a decimal-integer string with no lenient forms.
func decimalBig(field, s string) (*big.Int, error) {
	if s == "" {
		return nil, refuse(ClassValidationFailed, field, "empty decimal string")
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return nil, refuse(ClassValidationFailed, field, "not a decimal integer string")
		}
	}
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, refuse(ClassValidationFailed, field, "not a decimal integer string")
	}
	return v, nil
}

// decimalUint64 parses a decimal-integer string into a uint64.
func decimalUint64(field, s string) (uint64, error) {
	v, err := decimalBig(field, s)
	if err != nil {
		return 0, err
	}
	if !v.IsUint64() {
		return 0, refuse(ClassValidationFailed, field, "out of uint64 range")
	}
	return v.Uint64(), nil
}

// lowerAddress normalizes a hex address to lowercase 0x-hex after a shape
// check (EIP-55 checksum acceptance belongs to T007).
func lowerAddress(field, s string) (string, error) {
	if !common.IsHexAddress(s) {
		return "", refuse(ClassValidationFailed, field, "not a 0x hex address")
	}
	return strings.ToLower(common.HexToAddress(s).Hex()), nil
}

// lowerHex normalizes 0x-hex data to lowercase.
func lowerHex(field, s string) (string, error) {
	if _, err := hexutil.Decode(s); err != nil {
		return "", refuse(ClassValidationFailed, field, "not 0x-hex")
	}
	return strings.ToLower(s), nil
}

// contentDomain separates the content hash from every other keccak256 use.
const contentDomain = "txharbor:signer:content:v1"

// CanonicalEnvelope returns the deterministic bytes hashed to the content
// hash: the domain tag plus a fixed-order projection of every request field
// with normalized case and canonical decimals. Equal envelopes mean
// byte-identical requests; any difference is a different envelope
// (api.md: same identity, different envelope → request_conflict).
func (r *Request) CanonicalEnvelope() ([]byte, error) {
	sender, err := lowerAddress("sender", r.Sender)
	if err != nil {
		return nil, err
	}
	to, err := lowerAddress("to", r.To)
	if err != nil {
		return nil, err
	}
	asset, err := lowerAddress("asset", r.Asset)
	if err != nil {
		return nil, err
	}
	recipient, err := lowerAddress("recipient", r.Recipient)
	if err != nil {
		return nil, err
	}
	data, err := lowerHex("data", r.Data)
	if err != nil {
		return nil, err
	}
	dec := func(field, s string) (string, error) {
		v, err := decimalBig(field, s)
		if err != nil {
			return "", err
		}
		return v.String(), nil
	}
	nonce, err := dec("nonce", r.Nonce)
	if err != nil {
		return nil, err
	}
	value, err := dec("value", r.Value)
	if err != nil {
		return nil, err
	}
	gasLimit, err := dec("gas_limit", r.GasLimit)
	if err != nil {
		return nil, err
	}
	amount, err := dec("amount", r.Amount)
	if err != nil {
		return nil, err
	}
	gasPrice, err := dec("gas_price", orZero(r.GasPrice))
	if err != nil {
		return nil, err
	}
	maxFee, err := dec("max_fee_per_gas", orZero(r.MaxFeePerGas))
	if err != nil {
		return nil, err
	}
	maxPriority, err := dec("max_priority_fee_per_gas", orZero(r.MaxPriorityFeePerGas))
	if err != nil {
		return nil, err
	}
	proj := struct {
		SigningRequestID     string `json:"signing_request_id"`
		AttemptID            string `json:"attempt_id"`
		IntentID             string `json:"intent_id"`
		BindingRef           string `json:"binding_ref"`
		RecoveryVersion      uint64 `json:"recovery_version"`
		ChainID              uint64 `json:"chain_id"`
		Sender               string `json:"sender"`
		Nonce                string `json:"nonce"`
		TxType               uint8  `json:"tx_type"`
		To                   string `json:"to"`
		Value                string `json:"value"`
		Data                 string `json:"data"`
		GasLimit             string `json:"gas_limit"`
		GasPrice             string `json:"gas_price"`
		MaxFeePerGas         string `json:"max_fee_per_gas"`
		MaxPriorityFeePerGas string `json:"max_priority_fee_per_gas"`
		Asset                string `json:"asset"`
		Recipient            string `json:"recipient"`
		Amount               string `json:"amount"`
		AuthorizationID      string `json:"authorization_id"`
	}{
		r.SigningRequestID, r.AttemptID, r.IntentID, r.BindingRef,
		r.RecoveryVersion, r.ChainID, sender, nonce, r.TxType, to, value,
		data, gasLimit, gasPrice, maxFee, maxPriority, asset, recipient,
		amount, r.AuthorizationID,
	}
	raw, err := json.Marshal(proj)
	if err != nil {
		return nil, refuse(ClassValidationFailed, "", "envelope serialization failed")
	}
	out := append([]byte(contentDomain+"\x00"), raw...)
	return out, nil
}

func orZero(s string) string {
	if s == "" {
		return "0"
	}
	return s
}

// ContentHash is keccak256(domain || canonical envelope).
func (r *Request) ContentHash() (common.Hash, error) {
	env, err := r.CanonicalEnvelope()
	if err != nil {
		return common.Hash{}, err
	}
	return crypto.Keccak256Hash(env), nil
}

// Transaction rebuilds the exact types.Transaction the request describes:
// legacy (tx_type 0, gas_price) or dynamic-fee (tx_type 2, max fees).
// Fee-shape presence per tx_type is enforced here because construction is
// impossible without it; fee semantics belong to T007.
func (r *Request) Transaction() (*types.Transaction, error) {
	to, err := lowerAddress("to", r.To)
	if err != nil {
		return nil, err
	}
	toAddr := common.HexToAddress(to)
	nonce, err := decimalUint64("nonce", r.Nonce)
	if err != nil {
		return nil, err
	}
	value, err := decimalBig("value", r.Value)
	if err != nil {
		return nil, err
	}
	data, err := lowerHex("data", r.Data)
	if err != nil {
		return nil, err
	}
	gasLimit, err := decimalUint64("gas_limit", r.GasLimit)
	if err != nil {
		return nil, err
	}
	dataBytes := common.FromHex(data)
	switch r.TxType {
	case 0:
		if r.GasPrice == "" {
			return nil, refuse(ClassValidationFailed, "gas_price", "tx_type 0 requires gas_price")
		}
		if r.MaxFeePerGas != "" || r.MaxPriorityFeePerGas != "" {
			return nil, refuse(ClassValidationFailed, "gas_price", "tx_type 0 takes gas_price only")
		}
		gasPrice, err := decimalBig("gas_price", r.GasPrice)
		if err != nil {
			return nil, err
		}
		return types.NewTx(&types.LegacyTx{
			Nonce: nonce, GasPrice: gasPrice, Gas: gasLimit,
			To: &toAddr, Value: value, Data: dataBytes,
		}), nil
	case 2:
		if r.MaxFeePerGas == "" || r.MaxPriorityFeePerGas == "" {
			return nil, refuse(ClassValidationFailed, "max_fee_per_gas", "tx_type 2 requires max_fee_per_gas + max_priority_fee_per_gas")
		}
		if r.GasPrice != "" {
			return nil, refuse(ClassValidationFailed, "gas_price", "tx_type 2 takes max fees only")
		}
		maxFee, err := decimalBig("max_fee_per_gas", r.MaxFeePerGas)
		if err != nil {
			return nil, err
		}
		maxPriority, err := decimalBig("max_priority_fee_per_gas", r.MaxPriorityFeePerGas)
		if err != nil {
			return nil, err
		}
		return types.NewTx(&types.DynamicFeeTx{
			ChainID: new(big.Int).SetUint64(r.ChainID), Nonce: nonce,
			GasTipCap: maxPriority, GasFeeCap: maxFee, Gas: gasLimit,
			To: &toAddr, Value: value, Data: dataBytes,
		}), nil
	default:
		return nil, refuse(ClassValidationFailed, "tx_type", fmt.Sprintf("tx_type %d unsupported in v1", r.TxType))
	}
}

// VerifySignature independently re-derives the transaction from the request,
// attaches the 65-byte [R||S||V] signature, recovers the signer under the
// request chain, and proves it equals the declared sender. It returns the
// recovered sender and the transaction hash (R1).
func (r *Request) VerifySignature(signature []byte) (common.Address, common.Hash, error) {
	if len(signature) != 65 {
		return common.Address{}, common.Hash{}, refuse(ClassValidationFailed, "signature", "signature must be 65 bytes [R||S||V]")
	}
	tx, err := r.Transaction()
	if err != nil {
		return common.Address{}, common.Hash{}, err
	}
	signer := types.LatestSignerForChainID(new(big.Int).SetUint64(r.ChainID))
	signed, err := tx.WithSignature(signer, signature)
	if err != nil {
		return common.Address{}, common.Hash{}, refuse(ClassValidationFailed, "signature", "signature does not fit this transaction")
	}
	from, err := types.Sender(signer, signed)
	if err != nil {
		return common.Address{}, common.Hash{}, refuse(ClassValidationFailed, "signature", "sender is not recoverable from this signature")
	}
	want := common.HexToAddress(r.Sender)
	if from != want {
		return common.Address{}, common.Hash{}, refuse(ClassValidationFailed, "sender", "recovered sender does not match the declared sender")
	}
	return from, signed.Hash(), nil
}
