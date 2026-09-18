package txlifecycle

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// TxTypeLegacy / TxTypeDynamicFee are v1's only closed fee shapes.
const (
	TxTypeLegacy     uint8 = 0
	TxTypeDynamicFee uint8 = 2
)

// maxUint256 is the v1 value/amount/gas ceiling (NUMERIC(78,0) domain).
var maxUint256 = func() *big.Int {
	v, _ := new(big.Int).SetString("115792089237316195423570985008687907853269984665640564039457584007913129639935", 10)
	return v
}()

// maxUint64 is the nonce domain (NUMERIC(78,0), 0..2^64-1).
var maxUint64 = new(big.Int).SetUint64(^uint64(0))

// PrepareRequest is the exact caller schema of send-api.md §2.1. 010 derives
// to = asset, value = 0 and data = transfer(recipient, amount); a request that
// would violate the v1 shape is refused.
type PrepareRequest struct {
	AttemptID            string
	SigningRequestID     string
	ReplacementOf        string
	IntentID             string
	BindingRef           string
	AuthorizationID      string
	AuthorizationVersion int64
	RecoveryVersion      uint64
	ChainID              uint64
	Sender               string
	Nonce                string
	TxType               uint8
	GasLimit             string
	GasPrice             string
	MaxFeePerGas         string
	MaxPriorityFeePerGas string
	Asset                string
	Recipient            string
	Amount               string
}

// Validate enforces the v1 request shape in Go before any storage write
// (FR-01); the same shapes are enforced again at storage level.
func (r *PrepareRequest) Validate() error {
	for _, id := range []struct{ field, value string }{
		{"attempt_id", r.AttemptID},
		{"signing_request_id", r.SigningRequestID},
		{"intent_id", r.IntentID},
		{"binding_ref", r.BindingRef},
		{"authorization_id", r.AuthorizationID},
	} {
		if err := validatePrintableID(id.field, id.value); err != nil {
			return err
		}
	}
	if r.ReplacementOf != "" {
		if err := validatePrintableID("replacement_of", r.ReplacementOf); err != nil {
			return err
		}
		if r.ReplacementOf == r.AttemptID {
			return Refuse(ClassAttemptConflict, "replacement_of", "a replacement must name a different attempt")
		}
	}
	if r.AuthorizationVersion < 1 {
		return Refuse(ClassAuthorizationMissing, "authorization_version", "must be >= 1")
	}
	if r.ChainID == 0 {
		return Refuse(ClassAttemptConflict, "chain_id", "must be a positive chain id")
	}
	for _, a := range []struct{ field, value string }{
		{"sender", r.Sender},
		{"asset", r.Asset},
		{"recipient", r.Recipient},
	} {
		if _, err := lowerAddress(a.field, a.value); err != nil {
			return err
		}
	}
	if _, err := decimalUint64("nonce", r.Nonce); err != nil {
		return err
	}
	if _, err := positiveUint256("gas_limit", r.GasLimit); err != nil {
		return err
	}
	if _, err := positiveUint256("amount", r.Amount); err != nil {
		return err
	}
	return r.validateFeeShape()
}

// validateFeeShape enforces exactly one fee shape consistent with tx_type; the
// max_priority <= max_fee ordering is enforced here and at storage level.
func (r *PrepareRequest) validateFeeShape() error {
	switch r.TxType {
	case TxTypeLegacy:
		if r.GasPrice == "" {
			return Refuse(ClassAttemptConflict, "gas_price", "tx_type 0 requires gas_price")
		}
		if r.MaxFeePerGas != "" || r.MaxPriorityFeePerGas != "" {
			return Refuse(ClassAttemptConflict, "gas_price", "tx_type 0 takes gas_price only")
		}
		_, err := positiveUint256("gas_price", r.GasPrice)
		return err
	case TxTypeDynamicFee:
		if r.MaxFeePerGas == "" || r.MaxPriorityFeePerGas == "" {
			return Refuse(ClassAttemptConflict, "max_fee_per_gas", "tx_type 2 requires max_fee_per_gas + max_priority_fee_per_gas")
		}
		if r.GasPrice != "" {
			return Refuse(ClassAttemptConflict, "gas_price", "tx_type 2 takes max fees only")
		}
		maxFee, err := positiveUint256("max_fee_per_gas", r.MaxFeePerGas)
		if err != nil {
			return err
		}
		maxPriority, err := positiveUint256("max_priority_fee_per_gas", r.MaxPriorityFeePerGas)
		if err != nil {
			return err
		}
		if maxPriority.Cmp(maxFee) > 0 {
			return Refuse(ClassFeeScopeExceeded, "max_priority_fee_per_gas", "max_priority_fee_per_gas exceeds max_fee_per_gas")
		}
		return nil
	default:
		return Refuse(ClassAttemptConflict, "tx_type", fmt.Sprintf("tx_type %d unsupported in v1", r.TxType))
	}
}

// contentDomain is the 010 attempt content-hash domain tag.
const contentDomain = "txharbor:txlifecycle:content:v1"

// canonicalProjection is the non-identity economic projection hashed into
// content_hash: chain/sender/nonce/to/value/data/gas/fees (plus tx_type, which
// selects the fee shape) and deliberately NO attempt/signing id (R-010-02), so
// a recovery-version rebuild under a new identity may legitimately reuse the
// economics.
type canonicalProjection struct {
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
}

// signerBody is the exact 009 request body (signer-call.md §2): serialized
// once, stored verbatim as canonical_envelope, and re-sent byte-identically
// on same-identity retry.
type signerBody struct {
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
	GasPrice             string `json:"gas_price,omitempty"`
	MaxFeePerGas         string `json:"max_fee_per_gas,omitempty"`
	MaxPriorityFeePerGas string `json:"max_priority_fee_per_gas,omitempty"`
	Asset                string `json:"asset"`
	Recipient            string `json:"recipient"`
	Amount               string `json:"amount"`
	AuthorizationID      string `json:"authorization_id"`
}

// CanonicalEnvelope returns the deterministic 009 request body bytes. Equal
// envelopes mean byte-identical requests; any difference is a different
// envelope (same identity + different envelope → attempt_conflict).
func (r *PrepareRequest) CanonicalEnvelope() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	sender, _ := lowerAddress("sender", r.Sender)
	asset, _ := lowerAddress("asset", r.Asset)
	recipient, _ := lowerAddress("recipient", r.Recipient)
	nonce, _ := decimalUint64("nonce", r.Nonce)
	amount, _ := new(big.Int).SetString(r.Amount, 10)
	value := new(big.Int)
	// 009 enforces exactly one fee shape: the inapplicable fee dimensions must
	// be absent, not canonicalized to "0" (which would defeat omitempty).
	gasPrice, maxFee, maxPriority := "", "", ""
	if r.TxType == TxTypeLegacy {
		gasPrice = canonicalDecimal(r.GasPrice)
	} else {
		maxFee = canonicalDecimal(r.MaxFeePerGas)
		maxPriority = canonicalDecimal(r.MaxPriorityFeePerGas)
	}
	body := signerBody{
		SigningRequestID:     r.SigningRequestID,
		AttemptID:            r.AttemptID,
		IntentID:             r.IntentID,
		BindingRef:           r.BindingRef,
		RecoveryVersion:      r.RecoveryVersion,
		ChainID:              r.ChainID,
		Sender:               sender,
		Nonce:                fmt.Sprintf("%d", nonce),
		TxType:               r.TxType,
		To:                   asset,
		Value:                value.String(),
		Data:                 "0x" + hexutil.Encode(TransferCalldata(common.HexToAddress(recipient), amount))[2:],
		GasLimit:             canonicalDecimal(r.GasLimit),
		GasPrice:             gasPrice,
		MaxFeePerGas:         maxFee,
		MaxPriorityFeePerGas: maxPriority,
		Asset:                asset,
		Recipient:            recipient,
		Amount:               amount.String(),
		AuthorizationID:      r.AuthorizationID,
	}
	out, err := json.Marshal(body)
	if err != nil {
		return nil, Refuse(ClassCoordinationUnavailable, "", "envelope serialization failed")
	}
	return out, nil
}

// ContentHash is keccak256(domain || 0x00 || canonical economic projection),
// returned as 0x + 64 lowercase hex (the tx_attempts_content_hash_check
// domain). content_hash is deliberately NOT UNIQUE (R-010-02).
func (r *PrepareRequest) ContentHash() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	sender, _ := lowerAddress("sender", r.Sender)
	asset, _ := lowerAddress("asset", r.Asset)
	recipient, _ := lowerAddress("recipient", r.Recipient)
	nonce, _ := decimalUint64("nonce", r.Nonce)
	amount, _ := new(big.Int).SetString(r.Amount, 10)
	proj := canonicalProjection{
		ChainID:              r.ChainID,
		Sender:               sender,
		Nonce:                fmt.Sprintf("%d", nonce),
		TxType:               r.TxType,
		To:                   asset,
		Value:                "0",
		Data:                 "0x" + hexutil.Encode(TransferCalldata(common.HexToAddress(recipient), amount))[2:],
		GasLimit:             canonicalDecimal(r.GasLimit),
		GasPrice:             canonicalDecimalOptional(r.GasPrice),
		MaxFeePerGas:         canonicalDecimalOptional(r.MaxFeePerGas),
		MaxPriorityFeePerGas: canonicalDecimalOptional(r.MaxPriorityFeePerGas),
	}
	raw, err := json.Marshal(proj)
	if err != nil {
		return "", Refuse(ClassCoordinationUnavailable, "", "content hash serialization failed")
	}
	digest := crypto.Keccak256(append([]byte(contentDomain+"\x00"), raw...))
	return "0x" + hexutil.Encode(digest)[2:], nil
}

// Transaction rebuilds the exact types.Transaction the request describes
// (legacy tx_type 0 or dynamic-fee tx_type 2) for local byte reconstruction.
func (r *PrepareRequest) Transaction() (*types.Transaction, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	asset, _ := lowerAddress("asset", r.Asset)
	recipient, _ := lowerAddress("recipient", r.Recipient)
	nonce, _ := decimalUint64("nonce", r.Nonce)
	gasLimit, _ := decimalUint64("gas_limit", r.GasLimit)
	amount, _ := new(big.Int).SetString(r.Amount, 10)
	data := TransferCalldata(common.HexToAddress(recipient), amount)
	to := common.HexToAddress(asset)
	value := new(big.Int)
	switch r.TxType {
	case TxTypeLegacy:
		gasPrice, _ := new(big.Int).SetString(r.GasPrice, 10)
		return types.NewTx(&types.LegacyTx{
			Nonce: nonce, GasPrice: gasPrice, Gas: gasLimit,
			To: &to, Value: value, Data: data,
		}), nil
	default:
		maxFee, _ := new(big.Int).SetString(r.MaxFeePerGas, 10)
		maxPriority, _ := new(big.Int).SetString(r.MaxPriorityFeePerGas, 10)
		return types.NewTx(&types.DynamicFeeTx{
			ChainID: new(big.Int).SetUint64(r.ChainID), Nonce: nonce,
			GasTipCap: maxPriority, GasFeeCap: maxFee, Gas: gasLimit,
			To: &to, Value: value, Data: data,
		}), nil
	}
}

// validatePrintableID enforces the identity shape shared with the storage
// CHECKs: 1..128 printable ASCII bytes, no space.
func validatePrintableID(field, s string) error {
	if len(s) < 1 || len(s) > 128 {
		return Refuse(ClassAttemptConflict, field, "length must be 1..128")
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e {
			return Refuse(ClassAttemptConflict, field, "must be printable ASCII (0x21..0x7e)")
		}
	}
	return nil
}

func decimalBig(field, s string) (*big.Int, error) {
	if s == "" {
		return nil, Refuse(ClassAttemptConflict, field, "empty decimal string")
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return nil, Refuse(ClassAttemptConflict, field, "not a decimal integer string")
		}
	}
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, Refuse(ClassAttemptConflict, field, "not a decimal integer string")
	}
	return v, nil
}

func decimalUint64(field, s string) (uint64, error) {
	v, err := decimalBig(field, s)
	if err != nil {
		return 0, err
	}
	if v.Cmp(maxUint64) > 0 {
		return 0, Refuse(ClassAttemptConflict, field, "out of uint64 range")
	}
	return v.Uint64(), nil
}

func positiveUint256(field, s string) (*big.Int, error) {
	v, err := decimalBig(field, s)
	if err != nil {
		return nil, err
	}
	if v.Sign() <= 0 {
		return nil, Refuse(ClassAttemptConflict, field, "must be > 0")
	}
	if v.Cmp(maxUint256) > 0 {
		return nil, Refuse(ClassAttemptConflict, field, "out of uint256 range")
	}
	return v, nil
}

func lowerAddress(field, s string) (string, error) {
	if !common.IsHexAddress(s) {
		return "", Refuse(ClassAttemptConflict, field, "not a 0x hex address")
	}
	return strings.ToLower(common.HexToAddress(s).Hex()), nil
}

// canonicalDecimal normalizes a validated decimal string (no leading zeros).
func canonicalDecimal(s string) string {
	v, _ := new(big.Int).SetString(s, 10)
	return v.String()
}

func canonicalDecimalOptional(s string) string {
	if s == "" {
		return "0"
	}
	return canonicalDecimal(s)
}
