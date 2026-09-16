// observe.go owns the observation path: sampling
// eth_getTransactionCount("latest"/"pending") plus eth_getBlockByNumber
// ("latest") head identity for one (chain_id, sender) scope, persisting one
// nonce_observations row per attempt (FR-08..FR-10, R4).
//
// RPC discipline: every read happens OUTSIDE any DB transaction, bounded by
// the existing IndexRPCTimeout/IndexRetryInitial/IndexRetryMax knobs
// (ObserverConfig — no new timing knob). insertObservationTx writes through
// the caller's handle and opens no transaction of its own: the caller owns
// the tx/lock discipline (T006/T012).
package nonce

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"regexp"
	"time"

	"github.com/ethereum/go-ethereum"
	gethrpc "github.com/ethereum/go-ethereum/rpc"

	"github.com/xtianxx/txharbor/internal/eth"
)

// Observation kinds (nonce_observations.kind CHECK).
const (
	ObservationKindAllocation = "allocation"
	ObservationKindReconcile  = "reconcile"
)

// JSON-RPC methods the observation performs (R4).
const (
	methodTransactionCount = "eth_getTransactionCount"
	methodBlockByNumber    = "eth_getBlockByNumber"
)

// obsMaxAttempts bounds the retry loop; delays come from
// ObserverConfig.RetryInitial/RetryMax. An attempt count, not a timing knob.
const obsMaxAttempts = 3

var (
	obsSenderPattern = regexp.MustCompile(`^0x[0-9a-f]{40}$`)
	obsHashPattern   = regexp.MustCompile(`^0x[0-9a-f]{64}$`)
)

// ObserverConfig carries the existing index timing knobs (T016 wires
// cfg.IndexRPCTimeout/IndexRetryInitial/IndexRetryMax — no new knob).
type ObserverConfig struct {
	RPCTimeout   time.Duration
	RetryInitial time.Duration
	RetryMax     time.Duration
}

// rpcCaller is the raw JSON-RPC surface the observer needs; *gethrpc.Client
// satisfies it. Unit tests substitute a scripted fake (EARLY-VALIDATION; real
// RPC is covered by T027/T030).
type rpcCaller interface {
	CallContext(ctx context.Context, result any, method string, args ...any) error
}

var _ rpcCaller = (*gethrpc.Client)(nil)

// Observer samples the chain view for nonce scopes, outside any DB tx.
type Observer struct {
	rpc rpcCaller
	cfg ObserverConfig
}

// NewObserver wires the raw JSON-RPC client with the existing timing knobs.
func NewObserver(rpc rpcCaller, cfg ObserverConfig) *Observer {
	return &Observer{rpc: rpc, cfg: cfg}
}

// Observation is one nonce_observations row (data-model Table 5): evidence of
// a single observation attempt, never updated. On any read failure
// Classification is ClassificationUnavailable and ErrorClass carries the eth
// error class; on success the caller classifies (T010) and sets
// Classification before insertObservationTx.
type Observation struct {
	ObservationID  string   // minted by insertObservationTx when empty ("no-" + 32 hex)
	ChainID        int64    // scope
	Sender         string   // scope (lowercase 0x address)
	Kind           string   // ObservationKindAllocation | ObservationKindReconcile
	Classification string   // classify.go vocabulary; "" until classified
	LatestCount    *big.Int // nil when the read failed
	PendingCount   *big.Int // nil when the read failed
	HeadNumber     *big.Int // head identity of the read point
	HeadHash       string
	ErrorClass     string // data-model vocabulary: transport, timeout, rate_limited, …
	RPCRef         string // redacted endpoint alias, never a credential
}

// Observe performs the three R4 reads — eth_getTransactionCount(sender,
// "latest"), eth_getTransactionCount(sender, "pending") and
// eth_getBlockByNumber("latest", false) — outside any DB transaction, and
// returns the row the caller persists through its own tx handle. Any failure
// yields an `unavailable` row carrying the eth error class; a partial result
// is never treated as a value.
func (o *Observer) Observe(ctx context.Context, chainID int64, sender, kind string) Observation {
	obs := Observation{ChainID: chainID, Sender: sender, Kind: kind}
	latest, k := o.transactionCount(ctx, sender, "latest")
	if k != "" {
		return obs.failed(k)
	}
	pending, k := o.transactionCount(ctx, sender, "pending")
	if k != "" {
		return obs.failed(k)
	}
	head, hash, k := o.latestHead(ctx)
	if k != "" {
		return obs.failed(k)
	}
	obs.LatestCount, obs.PendingCount, obs.HeadNumber, obs.HeadHash = latest, pending, head, hash
	return obs
}

func (o Observation) failed(kind eth.Kind) Observation {
	o.Classification = ClassificationUnavailable
	o.ErrorClass = errorClassOf(kind)
	return o
}

// rpcHead is the eth_getBlockByNumber result shape (number/hash only).
type rpcHead struct {
	Number *string `json:"number"`
	Hash   string  `json:"hash"`
}

// transactionCount reads one eth_getTransactionCount quantity. Kind != "" on
// failure (including a malformed or null quantity).
func (o *Observer) transactionCount(ctx context.Context, sender, block string) (*big.Int, eth.Kind) {
	var raw string
	if k, err := o.call(ctx, &raw, methodTransactionCount, sender, block); err != nil {
		return nil, k
	}
	n, err := ParseHexQuantity(raw)
	if err != nil {
		return nil, eth.KindInvalidResponse
	}
	return n, ""
}

// latestHead reads the head block number and hash. A null block is
// KindNotFound (wait polarity); a malformed number/hash is KindInvalidResponse.
func (o *Observer) latestHead(ctx context.Context) (*big.Int, string, eth.Kind) {
	var head *rpcHead
	if k, err := o.call(ctx, &head, methodBlockByNumber, "latest", false); err != nil {
		return nil, "", k
	}
	if head == nil {
		return nil, "", eth.KindNotFound
	}
	if head.Number == nil {
		return nil, "", eth.KindInvalidResponse
	}
	n, err := ParseHexQuantity(*head.Number)
	if err != nil || !obsHashPattern.MatchString(head.Hash) {
		return nil, "", eth.KindInvalidResponse
	}
	return n, head.Hash, ""
}

// call performs one JSON-RPC request under RPCTimeout, retrying retryable
// kinds (transport/timeout/rate-limited) with RetryInitial→RetryMax backoff.
// It returns the eth classification of the last failure ("" on success).
func (o *Observer) call(ctx context.Context, result any, method string, args ...any) (eth.Kind, error) {
	delay := o.cfg.RetryInitial
	var lastErr error
	var kind eth.Kind
	for attempt := 1; attempt <= obsMaxAttempts; attempt++ {
		callCtx := ctx
		var cancel context.CancelFunc = func() {}
		if o.cfg.RPCTimeout > 0 {
			callCtx, cancel = context.WithTimeout(ctx, o.cfg.RPCTimeout)
		}
		err := o.rpc.CallContext(callCtx, result, method, args...)
		if err == nil {
			cancel()
			return "", nil
		}
		kind = obsKind(err, callCtx)
		cancel()
		lastErr = err
		if attempt == obsMaxAttempts || !obsRetryable(kind) {
			break
		}
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return obsKind(lastErr, ctx), lastErr
			case <-timer.C:
			}
		}
		delay *= 2
		if o.cfg.RetryMax > 0 && delay > o.cfg.RetryMax {
			delay = o.cfg.RetryMax
		}
	}
	return kind, lastErr
}

// obsRetryable mirrors the indexer's retry set: network-ish failures are
// retried, semantic refusals are not.
func obsRetryable(kind eth.Kind) bool {
	switch kind {
	case eth.KindTransport, eth.KindTimeout, eth.KindRateLimited:
		return true
	}
	return false
}

// obsKind classifies a raw JSON-RPC failure with the same rules as
// internal/eth's package-private classify (the Kind constants are shared; an
// error already carrying an eth.Error keeps its kind). The rule is repeated
// locally, like coord.go's write-guard copy, because internal/eth's helper
// cannot be exported from a frozen package.
func obsKind(err error, ctx context.Context) eth.Kind {
	if err == nil {
		return ""
	}
	if k := eth.KindOf(err); k != "" {
		return k
	}
	if errors.Is(err, ethereum.NotFound) {
		return eth.KindNotFound
	}
	var httpErr gethrpc.HTTPError
	if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusTooManyRequests {
		return eth.KindRateLimited
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return eth.KindTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return eth.KindTimeout
	}
	var rpcErr gethrpc.Error
	if errors.As(err, &rpcErr) {
		return eth.KindInvalidResponse
	}
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &syntaxErr) || errors.As(err, &typeErr) {
		return eth.KindInvalidResponse
	}
	return eth.KindTransport
}

// errorClassOf renders an eth kind in the nonce_observations.error_class
// vocabulary (data-model Table 5); not-found, incomplete and anything
// unclassified fall back to rpc_unavailable.
func errorClassOf(kind eth.Kind) string {
	switch kind {
	case eth.KindTransport:
		return "transport"
	case eth.KindTimeout:
		return "timeout"
	case eth.KindRateLimited:
		return "rate_limited"
	case eth.KindInvalidResponse:
		return "invalid_response"
	case eth.KindChainMismatch:
		return "chain_mismatch"
	default:
		return "rpc_unavailable"
	}
}

// insertObservationSQL is Table 5's insert; observed_at stays the column
// default (now()).
const insertObservationSQL = `
INSERT INTO nonce_observations (
	observation_id, chain_id, sender, kind, classification,
	latest_count, pending_count, head_number, head_hash, error_class, rpc_ref
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

// insertObservationTx persists exactly one evidence row through the caller's
// handle. It opens no transaction and performs no RPC: Observe runs before
// the caller's BEGIN, and this write lands inside the caller's tx (T012/T014).
// It returns the observation id (minted "no-" + 32 hex when empty).
func insertObservationTx(ctx context.Context, q txQuerier, obs Observation) (string, error) {
	if err := validateObservation(obs); err != nil {
		return "", err
	}
	id := obs.ObservationID
	if id == "" {
		minted, err := mintObservationID()
		if err != nil {
			return "", fmt.Errorf("mint observation id: %w", err)
		}
		id = minted
	}
	var headHash any
	if obs.HeadHash != "" {
		headHash = obs.HeadHash
	}
	_, err := q.Exec(ctx, insertObservationSQL,
		id, obs.ChainID, obs.Sender, obs.Kind, obs.Classification,
		NumericValue(obs.LatestCount), NumericValue(obs.PendingCount), NumericValue(obs.HeadNumber),
		headHash, obs.ErrorClass, obs.RPCRef)
	if err != nil {
		return "", fmt.Errorf("insert observation: %w", err)
	}
	return id, nil
}

// validateObservation refuses rows that could not satisfy Table 5's CHECKs,
// so a malformed value never aborts the caller's transaction.
func validateObservation(obs Observation) error {
	if obs.ChainID <= 0 {
		return fmt.Errorf("observation chain id %d is not positive", obs.ChainID)
	}
	if !obsSenderPattern.MatchString(obs.Sender) {
		return fmt.Errorf("observation sender %q is not a lowercase 0x address", obs.Sender)
	}
	switch obs.Kind {
	case ObservationKindAllocation, ObservationKindReconcile:
	default:
		return fmt.Errorf("observation kind %q is not allocation/reconcile", obs.Kind)
	}
	switch obs.Classification {
	case ClassificationConsistent, ClassificationBootstrapExternal,
		ClassificationUnattributedConsumption, ClassificationUnexplainedGap,
		ClassificationDivergence, ClassificationUnavailable:
	default:
		return fmt.Errorf("observation classification %q is not persistable", obs.Classification)
	}
	for _, v := range []struct {
		name string
		n    *big.Int
	}{
		{"latest_count", obs.LatestCount},
		{"pending_count", obs.PendingCount},
		{"head_number", obs.HeadNumber},
	} {
		if v.n == nil {
			continue
		}
		if err := ValidateNonceRange(v.n); err != nil {
			return fmt.Errorf("observation %s: %w", v.name, err)
		}
	}
	if obs.HeadHash != "" && !obsHashPattern.MatchString(obs.HeadHash) {
		return fmt.Errorf("observation head hash %q is not 0x + 64 hex", obs.HeadHash)
	}
	return nil
}

// mintObservationID mirrors hold.go's minting: crypto/rand only; a failure
// refuses the write rather than inventing a weak id.
func mintObservationID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return "no-" + hex.EncodeToString(buf), nil
}
