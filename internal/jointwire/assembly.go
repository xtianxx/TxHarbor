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
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/nonce"
	"github.com/xtianxx/txharbor/internal/txlifecycle"
)

// Worker builds the real joint participants for `txharbor withdrawal-worker`:
// the 010 Store (chain RPC and 009 signer client from configuration) behind
// LifecycleLive, the 008 read-only binding observation, and the 008 binding
// allocator (provisioned before the driver's step-issue gate; idempotent per
// intent — allocated first, replayed on the identical re-request). Every
// required piece is refused by name, so a missing knob stops startup instead
// of producing a half-wired worker.
//
// Scope note (Lane-W): the 008 allocator here runs with the nil rebuild-gate
// configuration the allocator package supports; the nonce rebuild
// verification belongs to 008's own process-entry obligations and is not
// wired into this composition root.
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
	anvilRPC, err := rpc.DialContext(ctx, cfg.RPCURL)
	if err != nil {
		return app.JointDeps{}, fmt.Errorf("joint wiring: rpc dial %s: %w", config.EnvRPCURL, err)
	}
	// The 008 allocator with the nil rebuild-gate configuration (see the
	// scope note above): the gate stays closed only when a rebuild
	// verification is wired, which belongs to 008's own process entry.
	gate := nonce.NewRebuildGate()
	gate.Open()
	alloc := nonce.NewAllocator(pool, nonce.NewObserver(anvilRPC, nonce.ObserverConfig{
		RPCTimeout: cfg.ProbeTimeout, RetryInitial: 10 * time.Millisecond, RetryMax: 100 * time.Millisecond,
	}), gate)
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
		ConfirmAttempt: func(ctx context.Context, intentID string) error {
			// 010's receipt/confirmation scan for the current attempt: a
			// no-op while the attempt has not left the prepare/sign states
			// (the scan's contract is the already-sent or unknown attempt).
			var attemptID, state string
			if err := pool.QueryRow(ctx,
				`SELECT attempt_id, state FROM tx_attempts WHERE intent_id = $1
				 ORDER BY created_at DESC, attempt_id DESC LIMIT 1`,
				intentID).Scan(&attemptID, &state); err != nil {
				if err == pgx.ErrNoRows {
					return nil
				}
				return fmt.Errorf("010 confirmation scan: read current attempt: %w", err)
			}
			if state == "prepared" || state == "signed" {
				// Nothing sendable persisted yet: no chain probe exists for
				// this attempt (the scan's contract starts at sent).
				return nil
			}
			// Every other state is scannable, and the scan never grants a
			// send permission: sent/effective advance the confirmation
			// depth, confirmed keeps the receipt/canonicality/reorg-revision
			// facts fresh after `completed`, unknown/orphaned probe for
			// loss/reorg evidence, and ineffective/replaced re-observe a
			// receipt revision without resurrecting a dispatch.
			if _, err := store.Reconcile(ctx, attemptID, ""); err != nil {
				return fmt.Errorf("010 confirmation scan: %w", err)
			}
			return nil
		},
		AllocBinding: func(ctx context.Context, intentID string) error {
			// Provision-or-replay: the allocator is idempotent for one
			// intent (allocated first, replayed for the identical
			// re-request), so a repeat call is a no-op by contract. The
			// sender and authorization come from the durable intent row.
			var sender, authorizationID string
			if err := pool.QueryRow(ctx,
				`SELECT sender, authorization_id FROM payment_intents WHERE intent_id = $1`,
				intentID).Scan(&sender, &authorizationID); err != nil {
				return fmt.Errorf("008 allocator: read intent identity: %w", err)
			}
			binding, outcome, err := alloc.Allocate(ctx, nonce.AllocationRequest{
				IntentID: intentID, ChainID: int64(cfg.ChainID), Sender: sender,
				AuthorizationID: authorizationID,
			})
			if err != nil {
				return fmt.Errorf("008 allocator: %w", err)
			}
			if binding == nil || (outcome != nonce.OutcomeAllocated && outcome != nonce.OutcomeReplayed) {
				return fmt.Errorf("008 allocator: outcome %s, want allocated or replayed", outcome)
			}
			return nil
		},
	}, nil
}
