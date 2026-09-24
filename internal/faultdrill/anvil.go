//go:build fault

// anvil.go is the T019 local-chain primitive of the drill harness: a pinned
// Anvil container plus the JSON-RPC calls and the Transfer-log emitter the
// five-state drills use to produce real on-chain deposits. It never touches
// any financial table; the chain is the drill's canonical source.
package faultdrill

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/eth"
)

// AnvilImage is the pinned local-chain image (the E2E layer pins the same tag).
const AnvilImage = "ghcr.io/foundry-rs/foundry:v1.8.1"

// Anvil is one real local chain.
type Anvil struct {
	ctr testcontainers.Container
	URL string
}

// startAnvil boots the pinned Anvil image and waits until it accepts RPC.
func startAnvil(ctx context.Context) (*Anvil, error) {
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        AnvilImage,
			ExposedPorts: []string{"8545/tcp"},
			Entrypoint:   []string{"anvil"},
			Cmd:          []string{"--host", "0.0.0.0", "--port", "8545", "--chain-id", "31337"},
			WaitingFor:   wait.ForLog("Listening on"),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("start anvil: %w", err)
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		_ = ctr.Terminate(context.Background())
		return nil, fmt.Errorf("anvil host: %w", err)
	}
	port, err := ctr.MappedPort(ctx, "8545/tcp")
	if err != nil {
		_ = ctr.Terminate(context.Background())
		return nil, fmt.Errorf("anvil port: %w", err)
	}
	return &Anvil{ctr: ctr, URL: fmt.Sprintf("http://%s:%s", host, port.Port())}, nil
}

// Terminate stops the chain.
func (a *Anvil) Terminate(ctx context.Context) error {
	if a == nil || a.ctr == nil {
		return nil
	}
	return a.ctr.Terminate(ctx)
}

// call issues one JSON-RPC request, retrying transient connection failures
// for a bounded window (a fresh container can refuse for a moment).
func (a *Anvil) call(method string, params ...any) (json.RawMessage, error) {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		return nil, fmt.Errorf("anvil %s marshal: %w", method, err)
	}
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for {
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Post(a.URL, "application/json", bytes.NewReader(body))
		if err != nil {
			lastErr = err
		} else {
			raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			var out struct {
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(raw, &out); err != nil {
				return nil, fmt.Errorf("anvil %s decode: %w (%s)", method, err, raw)
			}
			if out.Error != nil {
				return nil, fmt.Errorf("anvil %s error: %s", method, out.Error.Message)
			}
			return out.Result, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("anvil %s: %w", method, lastErr)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Accounts returns the unlocked accounts.
func (a *Anvil) Accounts() ([]common.Address, error) {
	raw, err := a.call("eth_accounts")
	if err != nil {
		return nil, err
	}
	var accounts []common.Address
	if err := json.Unmarshal(raw, &accounts); err != nil {
		return nil, fmt.Errorf("eth_accounts: %w", err)
	}
	if len(accounts) == 0 {
		return nil, fmt.Errorf("anvil returned no unlocked accounts")
	}
	return accounts, nil
}

// Mine mines n blocks and waits for the head to answer.
func (a *Anvil) Mine(n uint64) error {
	if _, err := a.call("anvil_mine", hexutil.EncodeUint64(n)); err != nil {
		return err
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := a.call("eth_blockNumber"); err == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("anvil head did not answer after mining %d blocks", n)
}

// BlockNumber returns the current head.
func (a *Anvil) BlockNumber() (uint64, error) {
	raw, err := a.call("eth_blockNumber")
	if err != nil {
		return 0, err
	}
	var head hexutil.Uint64
	if err := json.Unmarshal(raw, &head); err != nil {
		return 0, fmt.Errorf("eth_blockNumber: %w", err)
	}
	return uint64(head), nil
}

// emitTransferCode is the runtime bytecode of a contract whose fallback emits
// exactly one Transfer log (LOG3) — the 003/004 fixture shape.
func emitTransferCode(topics [3]common.Hash, amount *big.Int) []byte {
	var code []byte
	if amount != nil {
		code = append(code, 0x7f) // PUSH32 amount
		code = append(code, common.BigToHash(amount).Bytes()...)
		code = append(code, 0x60, 0x00, 0x52) // PUSH1 0; MSTORE
	}
	for _, topic := range [3]common.Hash{topics[2], topics[1], topics[0]} {
		code = append(code, 0x7f) // PUSH32 topic
		code = append(code, topic.Bytes()...)
	}
	code = append(code, 0x60, 0x20, 0x60, 0x00, 0xa3, 0x00) // PUSH1 32; PUSH1 0; LOG3; STOP
	return code
}

// EmitDeposit plants the emitting contract at the asset address and sends one
// transaction to it, returning the transaction hash and its mined height. It
// is a real on-chain fact; nothing off-chain fabricates an observation.
func (a *Anvil) EmitDeposit(from, asset, watch common.Address, amount *big.Int) (common.Hash, uint64, error) {
	topics := [3]common.Hash{
		eth.TransferSig,
		common.BytesToHash(from.Bytes()),
		common.BytesToHash(watch.Bytes()),
	}
	if _, err := a.call("anvil_setCode", asset, hexutil.Encode(emitTransferCode(topics, amount))); err != nil {
		return common.Hash{}, 0, fmt.Errorf("set emitter code: %w", err)
	}
	raw, err := a.call("eth_sendTransaction", map[string]any{
		"from": from, "to": asset, "data": "0x", "gas": "0x30d40",
	})
	if err != nil {
		return common.Hash{}, 0, fmt.Errorf("eth_sendTransaction: %w", err)
	}
	var txHash common.Hash
	if err := json.Unmarshal(raw, &txHash); err != nil {
		return common.Hash{}, 0, fmt.Errorf("eth_sendTransaction decode: %w", err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		raw, err := a.call("eth_getTransactionReceipt", txHash)
		var receipt *struct {
			BlockNumber hexutil.Uint64 `json:"blockNumber"`
		}
		if err == nil {
			_ = json.Unmarshal(raw, &receipt)
		}
		if receipt != nil {
			return txHash, uint64(receipt.BlockNumber), nil
		}
		if time.Now().After(deadline) {
			return common.Hash{}, 0, fmt.Errorf("transaction %s was not mined", txHash)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
