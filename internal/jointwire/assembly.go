// Package jointwire assembles the production 010/008 participants for the 011
// withdrawal worker from process configuration and the database pool. It is
// the composition root that knows both sides: internal/app must not import
// internal/txlifecycle (the txlifecycle joint tests import app, closing a
// test-build cycle), so cmd/txharbor links this package and injects Worker
// through app.Deps.JointWiring.
package jointwire

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/nonce"
	"github.com/xtianxx/txharbor/internal/txlifecycle"
)

// Worker builds the real joint participants for `txharbor withdrawal-worker`:
// the 010 Store (chain RPC and 009 signer client from configuration) behind
// LifecycleLive, plus the 008 read-only binding observation. Every required
// piece is refused by name, so a missing knob stops startup instead of
// producing a half-wired worker.
func Worker(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool) (app.JointDeps, error) {
	if cfg == nil {
		return app.JointDeps{}, fmt.Errorf("joint wiring: configuration is required")
	}
	if cfg.RPCURL == "" {
		return app.JointDeps{}, fmt.Errorf("joint wiring: %s is required for the 010 chain client", config.EnvRPCURL)
	}
	if cfg.TxSignerURL == "" {
		return app.JointDeps{}, fmt.Errorf("joint wiring: %s is required for 009 signing", config.EnvTxSignerURL)
	}
	if cfg.TxSignerCredential == "" {
		return app.JointDeps{}, fmt.Errorf("joint wiring: %s is required for 009 signing", config.EnvTxSignerCredential)
	}
	if pool == nil {
		return app.JointDeps{}, fmt.Errorf("joint wiring: database pool is required")
	}

	chain, err := eth.Dial(ctx, cfg.RPCURL, cfg.ProbeTimeout)
	if err != nil {
		return app.JointDeps{}, fmt.Errorf("joint wiring: dial %s: %w", config.EnvRPCURL, err)
	}
	signer, err := txlifecycle.NewSignerClient(txlifecycle.SignerConfig{
		BaseURL:    cfg.TxSignerURL,
		Credential: cfg.TxSignerCredential,
		Timeout:    cfg.TxSendTimeout,
	})
	if err != nil {
		return app.JointDeps{}, fmt.Errorf("joint wiring: 009 signer client: %w", err)
	}
	store := txlifecycle.NewStore(pool).WithChain(chain, cfg.TxSendTimeout).WithSigner(signer)
	adapter, err := txlifecycle.NewLifecycleLive(pool, store)
	if err != nil {
		return app.JointDeps{}, fmt.Errorf("joint wiring: 010 lifecycle adapter: %w", err)
	}
	return app.JointDeps{
		Advancer: adapter,
		Reader:   adapter,
		Binding:  &bindingReader{provider: nonce.NewReadProvider(pool, cfg.NonceReadToken)},
	}, nil
}
