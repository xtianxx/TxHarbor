// provider.go owns the KeyProvider boundary (T009; FR-20/FR-21, research
// R2): whole-transaction in/out, a local development test-key provider, a
// mode guard that refuses production without a non-local provider, and the
// TXHARBOR_SIGNER_KEY_TIMEOUT default. Key material enters only here (from an
// operator-supplied file) and never leaves as bytes; business code receives
// signed transactions, never keys.
package signer

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// Mode selects the key backend. Production has no local provider: wiring one
// would silently bind deployment to a test key (T000-P stays open).
type Mode string

const (
	// ModeDevelopment allows the local file-backed test-key provider.
	ModeDevelopment Mode = "development"
	// ModeProduction refuses any local key provider.
	ModeProduction Mode = "production"
)

// KeyTimeoutEnv is the signing-backend deadline knob; DefaultKeyTimeout is
// the fail-closed default when it is unset (R2).
const KeyTimeoutEnv = "TXHARBOR_SIGNER_KEY_TIMEOUT"

// DefaultKeyTimeout is the signing deadline when TXHARBOR_SIGNER_KEY_TIMEOUT
// is unset.
const DefaultKeyTimeout = 5 * time.Second

// KeyTimeoutFromEnv reads the signing deadline: unset means the default; a
// set value must parse as a Go duration and be positive, otherwise startup
// refuses instead of running with an unbounded or zero deadline.
func KeyTimeoutFromEnv(getenv func(string) string) (time.Duration, error) {
	raw := strings.TrimSpace(getenv(KeyTimeoutEnv))
	if raw == "" {
		return DefaultKeyTimeout, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, refuse(ClassKeyProviderUnavailable, KeyTimeoutEnv, "invalid signing timeout")
	}
	return d, nil
}

// KeyProvider signs whole structured transactions and returns the signed
// transaction. It never exposes key bytes and never accepts digests.
type KeyProvider interface {
	SignTx(ctx context.Context, sender common.Address, chainID *big.Int, tx *types.Transaction) (*types.Transaction, error)
}

// DevKeyProvider is the local test-key provider: exactly one development
// key, loaded from an operator-supplied file holding 32 bytes of hex
// (0x prefix optional). There is no default path and no implicit fallback.
type DevKeyProvider struct {
	key     *ecdsa.PrivateKey
	address common.Address
	timeout time.Duration
}

// NewDevKeyProvider loads the development key. Production mode refuses;
// development mode requires an explicit key file.
func NewDevKeyProvider(mode Mode, keyFile string, timeout time.Duration) (*DevKeyProvider, error) {
	if mode == ModeProduction {
		return nil, refuse(ClassKeyProviderUnavailable, "mode", "production requires a non-local key provider")
	}
	if mode != ModeDevelopment {
		return nil, refuse(ClassKeyProviderUnavailable, "mode", "unknown signer mode")
	}
	if strings.TrimSpace(keyFile) == "" {
		return nil, refuse(ClassKeyProviderUnavailable, "key_file", "development requires an explicit key file: no default, no fallback")
	}
	raw, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, refuse(ClassKeyProviderUnavailable, "key_file", "cannot read key file")
	}
	hexKey := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(string(raw)), "0x"))
	key, err := crypto.HexToECDSA(hexKey)
	if err != nil {
		return nil, refuse(ClassKeyProviderUnavailable, "key_file", "key file does not hold 32 bytes of hex")
	}
	if timeout <= 0 {
		timeout = DefaultKeyTimeout
	}
	return &DevKeyProvider{
		key:     key,
		address: crypto.PubkeyToAddress(key.PublicKey),
		timeout: timeout,
	}, nil
}

// Address is the single sender this provider can sign for.
func (p *DevKeyProvider) Address() common.Address { return p.address }

// SignTx signs tx for sender on chainID. It refuses when the sender is not
// the bound key (policy, never a signature for someone else), when the
// caller's deadline already expired (timeout class), and maps backend
// failures to the key-provider classes (R2).
func (p *DevKeyProvider) SignTx(ctx context.Context, sender common.Address, chainID *big.Int, tx *types.Transaction) (*types.Transaction, error) {
	if sender != p.address {
		return nil, refuse(ClassPolicyRefused, "sender", "sender is not bound to this signing key")
	}
	if chainID == nil || chainID.Sign() <= 0 {
		return nil, refuse(ClassValidationFailed, "chain_id", "chain id must be positive")
	}
	if tx == nil {
		return nil, refuse(ClassValidationFailed, "", "nothing to sign")
	}
	if err := ctx.Err(); err != nil {
		return nil, refuse(ClassKeyProviderTimeout, "", "signing deadline already expired")
	}
	signer := types.LatestSignerForChainID(chainID)
	type result struct {
		tx  *types.Transaction
		err error
	}
	done := make(chan result, 1)
	go func() {
		signed, err := types.SignTx(tx, signer, p.key)
		done <- result{signed, err}
	}()
	timer := time.NewTimer(p.timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, refuse(ClassKeyProviderTimeout, "", "signing deadline exceeded")
	case <-timer.C:
		return nil, refuse(ClassKeyProviderTimeout, "", "signing backend deadline exceeded")
	case r := <-done:
		if r.err != nil {
			return nil, refuse(ClassKeyProviderUnavailable, "", "signing backend failed")
		}
		return r.tx, nil
	}
}
