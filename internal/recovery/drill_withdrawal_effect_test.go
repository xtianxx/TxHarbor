//go:build drill

package recovery_test

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// TestT058WithdrawalEffectRestoreRetry is a deliberately bounded, real-chain
// subset of the worker/recovery acceptance. It proves a real signed Anvil
// transaction remains the same external fact across a real PostgreSQL restore,
// and that replaying its exact bytes does not pay twice. It does NOT run the
// app worker or production signer: the fixture request is accepted, the signed
// transaction below is locally constructed, the broadcast/receipt is real,
// and no unknown signer outcome is simulated. Those evidence classes must not
// be conflated with SQL fixture transitions or described as a worker retry.
func TestT058WithdrawalEffectRestoreRetry(t *testing.T) {
	requireDrillLocalPGRestore(t)
	env := newDrillEnv(t, true)
	env.seedLiveBusinessState()

	var requestStatus string
	if err := env.data.QueryRow(env.ctx,
		`SELECT status FROM withdrawal_requests WHERE request_id = 'drill-req-1'`).Scan(&requestStatus); err != nil {
		t.Fatalf("read accepted request fixture: %v", err)
	}
	if requestStatus != "accepted" {
		t.Fatalf("request status = %q, want accepted", requestStatus)
	}
	var prePointIntents int
	if err := env.data.QueryRow(env.ctx,
		`SELECT count(*) FROM payment_intents WHERE request_id = 'drill-req-1'`).Scan(&prePointIntents); err != nil {
		t.Fatalf("count pre-point intents: %v", err)
	}
	if prePointIntents != 0 {
		t.Fatalf("accepted request already has %d payment intents, want none", prePointIntents)
	}

	backup := env.backup()
	key, err := crypto.HexToECDSA(drillDevKeyHex)
	if err != nil {
		t.Fatalf("parse disposable Anvil key: %v", err)
	}
	from := crypto.PubkeyToAddress(key.PublicKey)
	to := crypto.PubkeyToAddress(drillSecondDevKey(t).PublicKey)
	chainID := new(big.Int).SetUint64(drillChainID)
	ctx, cancel := context.WithTimeout(env.ctx, 30*time.Second)
	defer cancel()

	var nonceHex hexutil.Uint64
	env.anvil.mustCall(t, &nonceHex, "eth_getTransactionCount", from, "latest")
	initialRecipientBalance := drillBalance(t, env, ctx, to, "latest")
	value := new(big.Int).SetUint64(1_000_000_000_000_000)
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID: chainID, Nonce: uint64(nonceHex), GasTipCap: big.NewInt(1_000_000_000),
		GasFeeCap: big.NewInt(2_000_000_000), Gas: 21_000, To: &to, Value: value,
	})
	signed, err := types.SignTx(tx, types.LatestSignerForChainID(chainID), key)
	if err != nil {
		t.Fatalf("sign local disposable transaction: %v", err)
	}
	raw, err := signed.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal signed transaction: %v", err)
	}
	var broadcastHash common.Hash
	env.anvil.mustCall(t, &broadcastHash, "eth_sendRawTransaction", hexutil.Encode(raw))
	if broadcastHash != signed.Hash() {
		t.Fatalf("broadcast hash = %s, signed identity = %s", broadcastHash, signed.Hash())
	}
	receipt := waitDrillReceipt(t, env, ctx, signed.Hash())
	if receipt.Status != types.ReceiptStatusSuccessful || receipt.GasUsed != 21_000 {
		t.Fatalf("receipt = status %d gas %d, want successful 21000-gas transfer", receipt.Status, receipt.GasUsed)
	}
	paidBalance := drillBalance(t, env, ctx, to, hexutil.EncodeBig(receipt.BlockNumber))
	if new(big.Int).Sub(paidBalance, initialRecipientBalance).Cmp(value) != 0 {
		t.Fatalf("recipient balance delta = %s, want exactly %s", new(big.Int).Sub(paidBalance, initialRecipientBalance), value)
	}
	var nonceAfterPayment hexutil.Uint64
	env.anvil.mustCall(t, &nonceAfterPayment, "eth_getTransactionCount", from, "latest")
	if uint64(nonceAfterPayment) != uint64(nonceHex)+1 {
		t.Fatalf("sender nonce after payment = %d, want %d", nonceAfterPayment, uint64(nonceHex)+1)
	}

	// Restore the actual pre-broadcast PostgreSQL recovery point. The chain
	// remains ahead; recovery does not rewind Anvil or infer signer/broadcast
	// state from the accepted request row.
	verifyDSN := env.createDatabase("withdrawal-effect-verify")
	env.verifyBackup(backup.ManifestPath, verifyDSN, env.operation("withdrawal-effect-verify"))
	env.seedAuthoritativeTargetCleanBaseline()
	recoveredDSN := env.recoveryTarget()
	recovered := env.openPool(recoveredDSN)
	env.restore(backup.ManifestPath, recoveredDSN, env.operation("withdrawal-effect-restore"))
	var restoredStatus string
	if err := recovered.QueryRow(env.ctx,
		`SELECT status FROM withdrawal_requests WHERE request_id = 'drill-req-1'`).Scan(&restoredStatus); err != nil {
		t.Fatalf("read restored request: %v", err)
	}
	if restoredStatus != "accepted" {
		t.Fatalf("restored request status = %q, want accepted", restoredStatus)
	}
	for _, check := range []struct {
		name  string
		query string
	}{
		{"payment intent", `SELECT count(*) FROM payment_intents WHERE request_id = 'drill-req-1'`},
		{"signing request", `SELECT count(*) FROM signing_requests WHERE intent_id = 'drill-intent-1'`},
		{"transaction attempt", `SELECT count(*) FROM tx_attempts WHERE intent_id = 'drill-intent-1'`},
	} {
		var count int
		if err := recovered.QueryRow(env.ctx, check.query).Scan(&count); err != nil {
			t.Fatalf("count restored %s: %v", check.name, err)
		}
		if count != 0 {
			t.Errorf("restored pre-broadcast point contains %d %s rows; it must not infer a signer or worker outcome", count, check.name)
		}
	}

	// Re-deliver the exact same signed bytes as a conservative retry probe. Anvil
	// may answer with the existing hash or a duplicate-known error; either way,
	// the receipt identity, recipient balance and sender nonce must stay fixed.
	var replayHash common.Hash
	replayErr := env.anvil.rpc.CallContext(ctx, &replayHash, "eth_sendRawTransaction", hexutil.Encode(raw))
	if replayErr != nil && !knownTransactionError(replayErr) {
		t.Fatalf("exact signed-byte retry failed for a reason other than duplicate delivery: %v", replayErr)
	}
	if replayErr == nil && replayHash != signed.Hash() {
		t.Fatalf("retry transaction identity = %s, want original %s", replayHash, signed.Hash())
	}
	replayReceipt := waitDrillReceipt(t, env, ctx, signed.Hash())
	if replayReceipt.TxHash != receipt.TxHash || replayReceipt.BlockNumber.Cmp(receipt.BlockNumber) != 0 {
		t.Fatalf("retry receipt = (%s, block %s), original = (%s, block %s)", replayReceipt.TxHash, replayReceipt.BlockNumber, receipt.TxHash, receipt.BlockNumber)
	}
	finalRecipientBalance := drillBalance(t, env, ctx, to, "latest")
	if finalRecipientBalance.Cmp(paidBalance) != 0 {
		t.Fatalf("recipient balance changed on retry: %s -> %s", paidBalance, finalRecipientBalance)
	}
	var finalNonce hexutil.Uint64
	env.anvil.mustCall(t, &finalNonce, "eth_getTransactionCount", from, "latest")
	if finalNonce != nonceAfterPayment {
		t.Fatalf("sender nonce changed on retry: %d -> %d", nonceAfterPayment, finalNonce)
	}

	drillWriteEvidence(t, "t058-withdrawal-effect", map[string]any{
		"scenario": "real Anvil transaction survives pre-broadcast PostgreSQL restore; identical raw-byte retry has no second payment",
		"evidence_classes": map[string]string{
			"accepted_intent": "accepted withdrawal request fixture only; no payment intent existed at the recovery point",
			"signed":          "locally signed disposable Anvil transaction; not the production signer",
			"broadcast":       "real Anvil transaction hash and successful receipt",
			"unknown":         "not simulated; no claim about signer timeout or unknown outcome",
			"worker":          "not run; this is not a real withdrawal-worker retry",
		},
		"transaction_hash": signed.Hash().Hex(), "nonce": uint64(nonceHex),
		"receipt_block": receipt.BlockNumber.Uint64(), "recipient_balance": finalRecipientBalance.String(),
		"retry_duplicate_error": replayErr != nil,
	})
}

func waitDrillReceipt(t *testing.T, env *drillEnv, ctx context.Context, hash common.Hash) *types.Receipt {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		receipt, err := env.anvil.client.TransactionReceipt(ctx, hash)
		if err == nil {
			if receipt.TxHash != hash {
				t.Fatalf("receipt tx hash = %s, want %s", receipt.TxHash, hash)
			}
			return receipt
		}
		if !errors.Is(err, ethereum.NotFound) && !strings.Contains(strings.ToLower(err.Error()), "not found") {
			t.Fatalf("read transaction receipt %s: %v", hash, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("transaction receipt %s unavailable after 15s", hash)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for receipt %s: %v", hash, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func drillBalance(t *testing.T, env *drillEnv, ctx context.Context, address common.Address, block string) *big.Int {
	t.Helper()
	var encoded hexutil.Big
	if err := env.anvil.rpc.CallContext(ctx, &encoded, "eth_getBalance", address, block); err != nil {
		t.Fatalf("read Anvil balance for %s at %s: %v", address, block, err)
	}
	return new(big.Int).Set((*big.Int)(&encoded))
}

func knownTransactionError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "already known") || strings.Contains(message, "known transaction") ||
		strings.Contains(message, "already imported") || strings.Contains(message, "nonce too low")
}
