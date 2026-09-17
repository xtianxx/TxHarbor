// signerlive.go owns the production live 008 wiring for signer-serve, in the
// default (untagged) build: an app-local adapter over *nonce.ReadProvider that
// implements 009's BindingReader + ScopeLocker. internal/signer's own
// LiveBindingReader (binding_live.go) carries the `integration` build tag
// because its internal/nonce import transitively carries 008's RPC client and
// T023 pins internal/signer's default graph RPC-free; this file is the
// untagged port of the same two operations. internal/app importing
// internal/nonce already has 008 precedent (serve.go, nonceadmin.go) and T023
// constrains internal/signer only.
package app

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/nonce"
	"github.com/xtianxx/txharbor/internal/signer"
)

// newSignerLiveBinding builds the live 008 half of the signer deps for one
// process pool: the read-only *nonce.ReadProvider (no startup rebuild gate —
// signer-serve runs no allocation admission; the T-deliver scope lock plus the
// fresh in-transaction reads are the cross-process guarantee) behind an
// adapter that is both the fact read and the scope lock.
func newSignerLiveBinding(pool *pgxpool.Pool, token string) (signerLiveBinding, error) {
	if pool == nil {
		return signerLiveBinding{}, fmt.Errorf("008 live binding wiring needs a database pool")
	}
	adapter := &signerLiveAdapter{provider: nonce.NewReadProvider(pool, token)}
	return signerLiveBinding{Binding: adapter, Scope: adapter}, nil
}

// signerLiveAdapter adapts 008's *nonce.ReadProvider to 009's binding
// interfaces. It holds no cache: every call is one fresh provider read, so a
// decision is never based on a pre-transaction observation.
type signerLiveAdapter struct {
	provider *nonce.ReadProvider
}

var (
	_ signer.BindingReader = (*signerLiveAdapter)(nil)
	_ signer.ScopeLocker   = (*signerLiveAdapter)(nil)
)

// ReadBinding reads one binding fact set and derives the 009 class. 008 keys
// bindings by intent_id only — there is no attempt axis in its read contract —
// so attemptID stays part of 009's own identity and is never sent (the durable
// intent is the binding identity).
func (a *signerLiveAdapter) ReadBinding(ctx context.Context, intentID, attemptID string) (signer.BindingResult, error) {
	_ = attemptID
	resp, err := a.provider.Read(ctx, nonce.ReadRequest{IntentID: intentID})
	return signerBindingResult(resp, err)
}

// LockScope takes the canonical 008 scope-row `FOR SHARE` (the gates.md §3
// statement, signer.ScopeShareSQL) inside the caller's sign/delivery
// transaction, before any gate read, so an 008 pause/registry/release writer
// racing the transaction waits until it commits and one that committed first
// is visible to the reads that follow. A missing scope row locks nothing; the
// fact read fails closed on that inconsistency.
func (a *signerLiveAdapter) LockScope(ctx context.Context, tx pgx.Tx, chainID int64, sender string) error {
	if _, err := tx.Exec(ctx, signer.ScopeShareSQL, chainID, sender); err != nil {
		return fmt.Errorf("008 scope row FOR SHARE: %w", err)
	}
	return nil
}

// signerBindingResult is the only allowed (Outcome, Annotations) → BindingResult
// mapping (contracts/gates.md:149-158: five outcomes → six classes; `bound`
// splits Matches/Paused by annotations):
//
//	bound + no hold + recovery none + registry active → BindingMatches
//	bound + any hold / recovery != none / registry disabled → BindingPaused
//	terminal → BindingTerminal
//	not_bound → BindingAbsent
//	mismatch → BindingConflict
//	unavailable → BindingReadFailed
//
// A read error or an unknown outcome fails closed to BindingReadFailed; a
// `bound` answer without annotations is not trustworthy and also fails closed.
// The canonical copy is signer.ClassifyBindingResponse
// (internal/signer/binding_live.go), which compiles only under `-tags
// integration`; the app-adapter live parity test pins the two copies to the
// same class over the full outcome matrix so this port cannot drift.
func signerBindingResult(resp nonce.ReadResponse, err error) (signer.BindingResult, error) {
	if err != nil {
		return signer.BindingReadFailed, err
	}
	switch resp.Outcome {
	case nonce.ReadBound:
		ann := resp.Annotations
		if ann == nil {
			return signer.BindingReadFailed, fmt.Errorf("008 bound read carried no annotations")
		}
		if ann.Gate.State != nonce.ReadGateOpen || len(ann.Gate.Causes) > 0 ||
			ann.Recovery.State != nonce.RecoveryNone ||
			ann.RegistryState != nonce.RegistryActive {
			return signer.BindingPaused, nil
		}
		return signer.BindingMatches, nil
	case nonce.ReadTerminal:
		return signer.BindingTerminal, nil
	case nonce.ReadNotBound:
		return signer.BindingAbsent, nil
	case nonce.ReadMismatch:
		return signer.BindingConflict, nil
	case nonce.ReadUnavailable:
		return signer.BindingReadFailed, nil
	default:
		return signer.BindingReadFailed, fmt.Errorf("unexpected 008 read outcome %q", resp.Outcome)
	}
}
