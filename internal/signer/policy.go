// policy.go owns the versioned signing policy (T008; FR-06–FR-12): chain,
// sender, asset and recipient allowlists plus amount/gas/fee caps, with a
// deterministic policy_version identity hash. Field shapes belong to
// validate.go (T007); gate reads belong to gates.go (T011).
package signer

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
)

// PolicyConfig carries the operator-configured allowlists and caps. The
// config layer (T013) builds it from TXHARBOR_SIGNER_* knobs; amounts are
// big integers so no float ever enters the policy.
type PolicyConfig struct {
	ChainIDs             []int64
	Senders              []string
	Assets               []string
	Recipients           []string
	MaxAmount            *big.Int
	MaxGasLimit          uint64
	MaxFeePerGas         *big.Int
	MaxPriorityFeePerGas *big.Int
	MaxGasPrice          *big.Int
}

// policyDomain separates the version hash from every other keccak256 use.
const policyDomain = "txharbor:signer:policy:v1"

// Policy is a frozen, canonicalized signing policy. Sets are deduplicated,
// lowercased and sorted at construction, so equivalent configs produce
// identical versions.
type Policy struct {
	chainIDs    []int64
	senders     []string
	assets      []string
	recipients  []string
	maxAmount   *big.Int
	maxGasLimit uint64
	maxFee      *big.Int
	maxPriority *big.Int
	maxGasPrice *big.Int
	version     string
}

// NewPolicy canonicalizes cfg and freezes it. Every allowlist must be
// non-empty and every cap positive: a policy that cannot sign anything, or
// one with an unbounded field, is a configuration error, not a policy.
func NewPolicy(cfg PolicyConfig) (*Policy, error) {
	need := func(name string, v *big.Int) (*big.Int, error) {
		if v == nil || v.Sign() <= 0 {
			return nil, refuse(ClassPolicyRefused, name, "policy cap must be positive")
		}
		return new(big.Int).Set(v), nil
	}
	maxAmount, err := need("max_amount", cfg.MaxAmount)
	if err != nil {
		return nil, err
	}
	maxFee, err := need("max_fee_per_gas", cfg.MaxFeePerGas)
	if err != nil {
		return nil, err
	}
	maxPriority, err := need("max_priority_fee_per_gas", cfg.MaxPriorityFeePerGas)
	if err != nil {
		return nil, err
	}
	maxGasPrice, err := need("max_gas_price", cfg.MaxGasPrice)
	if err != nil {
		return nil, err
	}
	if cfg.MaxGasLimit == 0 {
		return nil, refuse(ClassPolicyRefused, "max_gas_limit", "policy cap must be positive")
	}
	chains := append([]int64(nil), cfg.ChainIDs...)
	sort.Slice(chains, func(i, j int) bool { return chains[i] < chains[j] })
	if len(chains) == 0 {
		return nil, refuse(ClassPolicyRefused, "chain_ids", "policy needs at least one chain")
	}
	lowerSet := func(name string, in []string) ([]string, error) {
		set := map[string]bool{}
		for _, s := range in {
			l := strings.ToLower(s)
			if l == "" {
				return nil, refuse(ClassPolicyRefused, name, "policy set holds an empty entry")
			}
			set[l] = true
		}
		out := make([]string, 0, len(set))
		for s := range set {
			out = append(out, s)
		}
		sort.Strings(out)
		if len(out) == 0 {
			return nil, refuse(ClassPolicyRefused, name, "policy needs at least one "+name)
		}
		return out, nil
	}
	senders, err := lowerSet("senders", cfg.Senders)
	if err != nil {
		return nil, err
	}
	assets, err := lowerSet("assets", cfg.Assets)
	if err != nil {
		return nil, err
	}
	recipients, err := lowerSet("recipients", cfg.Recipients)
	if err != nil {
		return nil, err
	}
	p := &Policy{
		chainIDs: chains, senders: senders, assets: assets,
		recipients: recipients, maxAmount: maxAmount,
		maxGasLimit: cfg.MaxGasLimit, maxFee: maxFee,
		maxPriority: maxPriority, maxGasPrice: maxGasPrice,
	}
	raw, _ := json.Marshal(struct {
		Chains      []int64  `json:"chains"`
		Senders     []string `json:"senders"`
		Assets      []string `json:"assets"`
		Recipients  []string `json:"recipients"`
		MaxAmount   string   `json:"max_amount"`
		MaxGasLimit uint64   `json:"max_gas_limit"`
		MaxFee      string   `json:"max_fee_per_gas"`
		MaxPriority string   `json:"max_priority_fee_per_gas"`
		MaxGasPrice string   `json:"max_gas_price"`
	}{chains, senders, assets, recipients, maxAmount.String(), cfg.MaxGasLimit, maxFee.String(), maxPriority.String(), maxGasPrice.String()})
	p.version = "0x" + fmt.Sprintf("%x", crypto.Keccak256(append([]byte(policyDomain+"\x00"), raw...)))
	return p, nil
}

// Version is the policy_version identity hash pinned in audit rows: equal
// policies hash equal, any allowlist/cap change hashes different.
func (p *Policy) Version() string { return p.version }

// member reports allowlist membership (inputs are lowercased by callers).
func member(set []string, s string) bool {
	for _, v := range set {
		if v == s {
			return true
		}
	}
	return false
}

// Check enforces membership and caps on an already shape-valid request,
// returning ClassPolicyRefused naming the field with a policy_* detail. It
// fails closed on unparseable numbers (shape validation runs first, but a
// policy must never pass what it cannot read).
func (p *Policy) Check(r *Request) error {
	refused := func(field, detail string) error {
		return refuse(ClassPolicyRefused, field, detail)
	}
	chainOK := false
	for _, c := range p.chainIDs {
		if c == int64(r.ChainID) {
			chainOK = true
		}
	}
	if !chainOK {
		return refused("chain_id", "policy_chain: chain not permitted")
	}
	if !member(p.senders, strings.ToLower(r.Sender)) {
		return refused("sender", "policy_sender: sender not permitted")
	}
	if !member(p.assets, strings.ToLower(r.Asset)) {
		return refused("asset", "policy_asset: asset not permitted")
	}
	if !member(p.recipients, strings.ToLower(r.Recipient)) {
		return refused("recipient", "policy_recipient: recipient not permitted")
	}
	amount, err := decimalBig("amount", r.Amount)
	if err != nil {
		return refused("amount", "policy_amount: unreadable amount")
	}
	if amount.Cmp(p.maxAmount) > 0 {
		return refused("amount", "policy_amount: amount exceeds cap")
	}
	gasLimit, err := decimalUint64("gas_limit", r.GasLimit)
	if err != nil {
		return refused("gas_limit", "policy_gas: unreadable gas limit")
	}
	if gasLimit > p.maxGasLimit {
		return refused("gas_limit", "policy_gas: gas limit exceeds cap")
	}
	switch r.TxType {
	case 0:
		price, err := decimalBig("gas_price", r.GasPrice)
		if err != nil {
			return refused("gas_price", "policy_fee: unreadable gas price")
		}
		if price.Cmp(p.maxGasPrice) > 0 {
			return refused("gas_price", "policy_fee: gas price exceeds cap")
		}
	case 2:
		maxFee, err := decimalBig("max_fee_per_gas", r.MaxFeePerGas)
		if err != nil {
			return refused("max_fee_per_gas", "policy_fee: unreadable max fee")
		}
		maxPriority, err := decimalBig("max_priority_fee_per_gas", r.MaxPriorityFeePerGas)
		if err != nil {
			return refused("max_priority_fee_per_gas", "policy_fee: unreadable max priority fee")
		}
		if maxFee.Cmp(p.maxFee) > 0 {
			return refused("max_fee_per_gas", "policy_fee: max fee exceeds cap")
		}
		if maxPriority.Cmp(p.maxPriority) > 0 {
			return refused("max_priority_fee_per_gas", "policy_fee: max priority fee exceeds cap")
		}
	default:
		return refused("tx_type", "policy_tx: unsupported tx type")
	}
	return nil
}
