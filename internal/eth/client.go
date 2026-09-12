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
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
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
)

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
