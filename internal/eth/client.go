// Package eth wraps go-ethereum's ethclient for the chain interactions the
// foundation needs: a bounded, classified chain-id check and header fetch.
package eth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
)

// Kind classifies external RPC failures (research/plan: transport, timeout,
// chain-mismatch, invalid-response, not-found, rate-limited) so callers can
// react and count them.
type Kind string

const (
	KindTransport       Kind = "transport"
	KindTimeout         Kind = "timeout"
	KindChainMismatch   Kind = "chain-mismatch"
	KindInvalidResponse Kind = "invalid-response"
	// KindNotFound means the requested height has no block yet: a wait
	// polarity, not a failure (R1/R4, FR-11).
	KindNotFound Kind = "not-found"
	// KindRateLimited means the endpoint throttled the request: retryable
	// with backoff (R4, FR-09).
	KindRateLimited Kind = "rate-limited"
	// KindIncomplete means the log query cannot be confirmed complete:
	// range/result/response-size errors, explicit truncated/partial markers,
	// or unfinished pagination. Callers must shrink the interval and retry
	// from the same height, never treat it as an empty result (003 FR-12).
	KindIncomplete Kind = "incomplete"

	// Send-specific classes (010 T002/R-010-05; added alongside, never
	// replacing, the read-side vocabulary above). These are dispatch-evidence
	// classes, never payment verdicts.
	KindAlreadyKnown           Kind = "already-known"
	KindNonceTooLow            Kind = "nonce-too-low"
	KindReplacementUnderpriced Kind = "replacement-underpriced"
	KindInsufficientFunds      Kind = "insufficient-funds"
	KindIntrinsicGasTooLow     Kind = "intrinsic-gas-too-low"
	// KindHashMismatch means eth_sendRawTransaction returned a hash different
	// from the persisted tx_hash: a fail-safe unknown, never accepted.
	KindHashMismatch Kind = "hash-mismatch"
)

// TransferSig is keccak256("Transfer(address,address,uint256)"), the
// ERC-20 Transfer event signature (003 FR-07). Asserted by unit test.
var TransferSig = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))

// Error is a classified chain client error.
type Error struct {
	Kind Kind
	Op   string
	Err  error
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s: %v", e.Op, e.Kind, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// KindOf returns the classification of err, or "" when it is not an eth.Error.
func KindOf(err error) Kind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return ""
}

// Client is a thin ethclient wrapper. It is safe to Close more than once.
type Client struct {
	client    *ethclient.Client
	closeOnce sync.Once
}

// Dial connects to the JSON-RPC endpoint. The timeout bounds the handshake.
func Dial(ctx context.Context, rawURL string, timeout time.Duration) (*Client, error) {
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	c, err := ethclient.DialContext(dialCtx, rawURL)
	if err != nil {
		return nil, &Error{Kind: classify(err, dialCtx), Op: "dial", Err: err}
	}
	return &Client{client: c}, nil
}

// Close releases the underlying RPC client.
func (c *Client) Close() {
	if c == nil || c.client == nil {
		return
	}
	c.closeOnce.Do(func() { c.client.Close() })
}

// ChainID calls eth_chainId; the caller's context carries the deadline.
func (c *Client) ChainID(ctx context.Context) (*big.Int, error) {
	id, err := c.client.ChainID(ctx)
	if err != nil {
		return nil, &Error{Kind: classify(err, ctx), Op: "eth_chainId", Err: err}
	}
	if id == nil || id.Sign() <= 0 {
		return nil, &Error{Kind: KindInvalidResponse, Op: "eth_chainId", Err: errors.New("chain id is not a positive integer")}
	}
	return id, nil
}

// CheckChainID verifies the endpoint's chain id against expected, applying its
// own timeout. A mismatch reports both values (FR-010, US4).
func (c *Client) CheckChainID(ctx context.Context, expected *big.Int, timeout time.Duration) error {
	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	id, err := c.ChainID(checkCtx)
	if err != nil {
		return err
	}
	if expected != nil && id.Cmp(expected) != 0 {
		return &Error{
			Kind: KindChainMismatch,
			Op:   "eth_chainId",
			Err:  fmt.Errorf("expected %s, actual %s", expected.String(), id.String()),
		}
	}
	return nil
}

// HeaderByNumber returns the header at number via eth_getBlockByNumber with
// fullTx=false (R1). A height beyond the current head comes back as
// ethereum.NotFound wrapped in a KindNotFound Error: callers wait/retry
// instead of treating it as a fault. errors.Is(err, ethereum.NotFound) holds.
func (c *Client) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	header, err := c.client.HeaderByNumber(ctx, number)
	if err != nil {
		return nil, &Error{Kind: classify(err, ctx), Op: "eth_getBlockByNumber", Err: err}
	}
	if header == nil {
		// Defensive: ethclient already turns a null result into ethereum.NotFound.
		return nil, &Error{Kind: KindNotFound, Op: "eth_getBlockByNumber", Err: ethereum.NotFound}
	}
	return header, nil
}

// FilterLogs returns the logs matching q via a single eth_getLogs call. An
// empty result is a legitimate success (a log-free interval advances
// normally); only transport/timeout/rate-limit/incomplete/invalid outcomes
// are errors. Callers must never treat any error as an empty result.
func (c *Client) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	logs, err := c.client.FilterLogs(ctx, q)
	if err != nil {
		return nil, &Error{Kind: classifyLogFilter(err, ctx), Op: "eth_getLogs", Err: err}
	}
	return logs, nil
}

// SendSignedTransaction submits raw signed bytes via eth_sendRawTransaction and
// requires the node to return the persisted hash. A different hash is
// KindHashMismatch (fail-safe unknown); transport/timeout/rate-limit and the
// allowlisted deterministic refusals are classified for the caller (010 T002).
func (c *Client) SendSignedTransaction(ctx context.Context, raw []byte, expected common.Hash) (common.Hash, error) {
	var returned common.Hash
	if err := c.client.Client().CallContext(ctx, &returned, "eth_sendRawTransaction", hexutil.Encode(raw)); err != nil {
		return common.Hash{}, &Error{Kind: classifySend(err, ctx), Op: "eth_sendRawTransaction", Err: err}
	}
	if returned != expected {
		return returned, &Error{
			Kind: KindHashMismatch,
			Op:   "eth_sendRawTransaction",
			Err:  fmt.Errorf("returned hash %s differs from persisted %s", returned.Hex(), expected.Hex()),
		}
	}
	return returned, nil
}

// TransactionByHash probes eth_getTransactionByHash. isPending is true when
// the node knows the tx but has not included it; ethereum.NotFound is
// KindNotFound (a wait polarity, never a failure verdict).
func (c *Client) TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	tx, isPending, err := c.client.TransactionByHash(ctx, hash)
	if err != nil {
		return nil, false, &Error{Kind: classify(err, ctx), Op: "eth_getTransactionByHash", Err: err}
	}
	return tx, isPending, nil
}

// TransactionReceipt fetches eth_getTransactionReceipt; a missing receipt is
// KindNotFound (the caller records not_found_yet and stays unknown).
func (c *Client) TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	receipt, err := c.client.TransactionReceipt(ctx, hash)
	if err != nil {
		return nil, &Error{Kind: classify(err, ctx), Op: "eth_getTransactionReceipt", Err: err}
	}
	if receipt == nil {
		return nil, &Error{Kind: KindNotFound, Op: "eth_getTransactionReceipt", Err: ethereum.NotFound}
	}
	return receipt, nil
}

// BlockNumber returns the current head number via eth_blockNumber.
func (c *Client) BlockNumber(ctx context.Context) (uint64, error) {
	n, err := c.client.BlockNumber(ctx)
	if err != nil {
		return 0, &Error{Kind: classify(err, ctx), Op: "eth_blockNumber", Err: err}
	}
	return n, nil
}

// classifySend maps eth_sendRawTransaction rejections onto the send-specific
// classes before falling back to the read-side classifier. The allowlist is
// message-based (go-ethereum wraps node errors as rpc.Error); an unrecognized
// error stays a generic transport/invalid-response class, never a verdict.
func classifySend(err error, ctx context.Context) Kind {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "already known"):
		return KindAlreadyKnown
	case strings.Contains(msg, "nonce too low"):
		return KindNonceTooLow
	case strings.Contains(msg, "replacement transaction underpriced"):
		return KindReplacementUnderpriced
	case strings.Contains(msg, "insufficient funds"):
		return KindInsufficientFunds
	case strings.Contains(msg, "intrinsic gas too low"):
		return KindIntrinsicGasTooLow
	}
	return classify(err, ctx)
}

// limitExceededCode is the de-facto JSON-RPC "limit exceeded" code returned by
// several hosted providers for over-wide log queries. It is the only
// code-based incomplete signal until the T000-P provider annex confirms or
// extends the mapping for the selected production provider; unknown errors
// are never classified as incomplete.
const limitExceededCode = -32005

func classifyLogFilter(err error, ctx context.Context) Kind {
	var httpErr gethrpc.HTTPError
	if errors.As(err, &httpErr) {
		if httpErr.StatusCode == http.StatusTooManyRequests {
			return KindRateLimited
		}
		if httpErr.StatusCode == http.StatusRequestEntityTooLarge {
			return KindIncomplete
		}
	}
	var rpcErr gethrpc.Error
	if errors.As(err, &rpcErr) && rpcErr.ErrorCode() == limitExceededCode {
		return KindIncomplete
	}
	return classify(err, ctx)
}

func classify(err error, ctx context.Context) Kind {
	if errors.Is(err, ethereum.NotFound) {
		return KindNotFound
	}
	var httpErr gethrpc.HTTPError
	if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusTooManyRequests {
		return KindRateLimited
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return KindTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return KindTimeout
	}
	var rpcErr gethrpc.Error
	if errors.As(err, &rpcErr) {
		return KindInvalidResponse
	}
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &syntaxErr) || errors.As(err, &typeErr) {
		return KindInvalidResponse
	}
	return KindTransport
}
