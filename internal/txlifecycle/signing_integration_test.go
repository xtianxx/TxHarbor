//go:build integration

package txlifecycle

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/signer"
)

// t021Env is the env the real 009 `SignerServe` needs: the full serve config
// plus an explicit development key file and policy allowlist covering the 010
// fixture sender/asset/recipient/fees.
func t021Env(t *testing.T, dsn, addr, keyFile, sender string, chainID int64) map[string]string {
	t.Helper()
	env := map[string]string{
		config.EnvPGDSN:                 dsn,
		config.EnvRPCURL:                "http://127.0.0.1:8545",
		config.EnvChainID:               strconv.FormatInt(chainID, 10),
		config.EnvStartHeight:           "0",
		config.EnvLogStartHeight:        "0",
		config.EnvLogContracts:          "0x1111111111111111111111111111111111111111",
		config.EnvDepositStartHeight:    "0",
		config.EnvDepositContracts:      "0x1111111111111111111111111111111111111111",
		config.EnvDepositWatchAddresses: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		config.EnvConfirmationDepth:     "10",
		config.EnvReorgMaxDepth:         "100",
		config.EnvHTTPAddr:              "127.0.0.1:0",
		config.EnvSignerHTTPAddr:        addr,
		config.EnvSignerMode:            "development",
		config.EnvSignerKeyFile:         keyFile,
		config.EnvSignerChains:          strconv.FormatInt(chainID, 10),
		config.EnvSignerSenders:         sender,
		config.EnvSignerAssets:          fxAsset,
		config.EnvSignerRecipients:      fxRecipient,
		config.EnvSignerMaxAmount:       "2000000",
		config.EnvSignerMaxGasLimit:     "100000",
		config.EnvSignerMaxFeePerGas:    "2000000000",
		config.EnvSignerMaxPriorityFee:  "1500000000",
		config.EnvSignerMaxGasPrice:     "2000000000",
		config.EnvNonceReadToken:        "t021-nonce-read-token",
	}
	return env
}

// startSignerServe boots the real in-process 009 server (`app.SignerServe`,
// the same function cmd/txharbor calls) and returns its base URL.
func startSignerServe(t *testing.T, ctx context.Context, env map[string]string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	env[config.EnvSignerHTTPAddr] = addr

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan int, 1)
	var stderr bytes.Buffer
	go func() {
		done <- app.SignerServe(runCtx, nil, app.Deps{
			Getenv: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
			Stdout: io.Discard,
			Stderr: &stderr,
		})
	}()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case code := <-done:
			t.Fatalf("SignerServe exited early with %d; stderr=%s", code, stderr.String())
		default:
		}
		if conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
			_ = conn.Close()
			t.Cleanup(func() {
				cancel()
				select {
				case code := <-done:
					if code != 0 {
						t.Errorf("SignerServe exit = %d; stderr=%s", code, stderr.String())
					}
				case <-time.After(15 * time.Second):
					t.Errorf("SignerServe did not shut down")
				}
			})
			return "http://" + addr
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("signer listener never opened; stderr=%s", stderr.String())
	return ""
}

// tamperSignature flips one hex nibble of a 65-byte signature so local
// reconstruction cannot recover the attempt sender.
func tamperSignature(sig string) string {
	raw := strings.TrimPrefix(strings.ToLower(sig), "0x")
	b := []byte(raw)
	for i := 4; i < len(b); i++ {
		if b[i] != '0' {
			b[i] = '0'
			break
		}
	}
	return "0x" + string(b)
}

func signatureBytes(t *testing.T, sig string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(strings.TrimPrefix(strings.ToLower(sig), "0x"))
	if err != nil || len(raw) != 65 {
		t.Fatalf("signature %q is not 65 bytes", sig)
	}
	return raw
}

// TestT021V2RealSigner is T021/V2 executed against the real in-process 009
// signer-serve: T2 commits signature + signed_tx_bytes + tx_hash before the
// first dispatch; keccak256(bytes) == tx_hash == 009.tx_hash with the recovered
// sender equal to the attempt sender; a tampered signature is refused with zero
// dispatch and a recorded event.
func TestT021V2RealSigner(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	sender := strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex())
	keyFile := filepath.Join(t.TempDir(), "t021.key")
	if err := os.WriteFile(keyFile, []byte(common.Bytes2Hex(crypto.FromECDSA(key))), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	e.devKey = key

	cred, err := signer.IssueCredential(ctx, e.pool, 1, "t021")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	if err := signer.SetCanSign(ctx, e.pool, 1, true); err != nil {
		t.Fatalf("SetCanSign: %v", err)
	}

	baseURL := startSignerServe(t, ctx, t021Env(t, e.dsn, "", keyFile, sender, e.chainID))
	sc, err := NewSignerClient(SignerConfig{BaseURL: baseURL, Credential: cred, Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("NewSignerClient: %v", err)
	}
	e.store.WithSigner(sc)

	f := e.seed()
	if f.sender != sender {
		t.Fatalf("fixture sender %s != dev key sender %s", f.sender, sender)
	}

	var sawSigningRow bool
	e.rpc.onDispatch = func() {
		var n int
		if qerr := e.pool.QueryRow(ctx,
			`SELECT count(*) FROM tx_attempt_signings WHERE attempt_id = $1`, f.attemptID).Scan(&n); qerr == nil {
			sawSigningRow = n == 1
		}
	}
	res, err := e.send(f, SendInitial, nil)
	if err != nil {
		t.Fatalf("real 009 send: %v", err)
	}
	if res.Outcome != "accepted" {
		t.Fatalf("send outcome = %s (%+v), want accepted", res.Outcome, res)
	}
	if !sawSigningRow {
		t.Fatal("dispatch ran before the T2 signing row was committed")
	}

	hash, raw, err := e.store.signingRow(ctx, f.attemptID)
	if err != nil {
		t.Fatalf("signingRow: %v", err)
	}
	if got := crypto.Keccak256Hash(raw).Hex(); got != hash {
		t.Fatalf("keccak256(bytes) = %s != tx_hash %s", got, hash)
	}
	var sig string
	if err := e.pool.QueryRow(ctx,
		`SELECT signature FROM tx_attempt_signings WHERE attempt_id = $1`, f.attemptID).Scan(&sig); err != nil {
		t.Fatalf("read signature: %v", err)
	}
	unsigned, err := f.request.Transaction()
	if err != nil {
		t.Fatalf("rebuild tx: %v", err)
	}
	chainSigner := types.LatestSignerForChainID(new(big.Int).SetUint64(f.request.ChainID))
	signed, err := unsigned.WithSignature(chainSigner, signatureBytes(t, sig))
	if err != nil {
		t.Fatalf("with signature: %v", err)
	}
	from, err := types.Sender(chainSigner, signed)
	if err != nil || !strings.EqualFold(from.Hex(), f.sender) {
		t.Fatalf("recovered sender = %s (%v), want %s", from.Hex(), err, f.sender)
	}

	attempt, err := e.store.AttemptByID(ctx, f.attemptID)
	if err != nil {
		t.Fatalf("AttemptByID: %v", err)
	}
	replay, err := sc.Submit(ctx, attempt)
	if err != nil {
		t.Fatalf("009 re-delivery: %v", err)
	}
	if !strings.EqualFold(replay.TxHash, hash) {
		t.Fatalf("009.tx_hash = %s != persisted tx_hash %s", replay.TxHash, hash)
	}

	var persisted int
	if err := e.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'signature_persisted'`, f.attemptID).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != 1 {
		t.Fatalf("signature_persisted events = %d, want 1", persisted)
	}

	t.Run("tampered_signature_refused", func(t *testing.T) {
		f2 := e.seed()
		att, err := e.store.AttemptByID(ctx, f2.attemptID)
		if err != nil {
			t.Fatal(err)
		}
		good, err := sc.Submit(ctx, att)
		if err != nil {
			t.Fatalf("009 sign: %v", err)
		}
		tampered := good
		tampered.Signature = tamperSignature(good.Signature)
		beforeDispatch := e.rpc.dispatchCount()
		_, err = e.store.SignAndPersist(ctx, att, tampered)
		if refusalClass(err) != ClassSignatureMismatch {
			t.Fatalf("tampered signature = %v (%s), want signature_mismatch", err, refusalClass(err))
		}
		if got := e.rpc.dispatchCount(); got != beforeDispatch {
			t.Fatalf("tampered signature dispatched: %d -> %d", beforeDispatch, got)
		}
		var mismatches, signingRows int
		if err := e.pool.QueryRow(ctx,
			`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = 'signature_mismatch'`, f2.attemptID).Scan(&mismatches); err != nil {
			t.Fatal(err)
		}
		if mismatches != 1 {
			t.Fatalf("signature_mismatch events = %d, want 1", mismatches)
		}
		if err := e.pool.QueryRow(ctx,
			`SELECT count(*) FROM tx_attempt_signings WHERE attempt_id = $1`, f2.attemptID).Scan(&signingRows); err != nil {
			t.Fatal(err)
		}
		if signingRows != 0 {
			t.Fatalf("tampered signature persisted %d signing row(s), want 0", signingRows)
		}
	})
}
