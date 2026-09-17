//go:build integration

// binding_live.go owns the T028 live 008 adapter (contracts/gates.md §3, plan
// D3): 009's BindingReader + ScopeLocker over the merged 008
// *nonce.ReadProvider (internal/nonce/readapi.go NewReadProvider/Read/
// ReadByIntent/ReadByBindingID), including the only allowed
// (Outcome, Annotations) → BindingResult mapping (gates.md:149-158: five
// outcomes → six classes; `bound` splits Matches/Paused by annotations). It
// replaces the contract-shape doubles on the live path: the fact half is a
// read-only provider call (008 state is never consumed or written) and the
// lock half takes the same scope-row `FOR SHARE` inside the caller's
// sign/delivery transaction that gates.md §3 requires.
//
// Build tag: the adapter is compiled for the live acceptance only. Its import
// of internal/nonce transitively carries 008's gethrpc client (observe.go),
// and T023's import-boundary assertion (boundary_test.go: `internal/signer`
// must reach no RPC/dial package) pins the production graph. Tagging the
// adapter keeps the business and signer-serve builds RPC-free while the
// T028/T040 live paths (and any app-layer wiring, which already imports nonce)
// compile it with `-tags integration`.
package signer

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/nonce"
)

// LiveBindingReader adapts the merged 008 read provider to the 009 gate
// interfaces. It holds no cache: every call is one fresh provider read, so a
// decision is never based on a pre-transaction observation.
type LiveBindingReader struct {
	provider *nonce.ReadProvider
}

// NewLiveBindingReader wires the 008 read provider (T016 wires the read token;
// a startup rebuild gate may be supplied through NewReadProvider).
func NewLiveBindingReader(provider *nonce.ReadProvider) *LiveBindingReader {
	return &LiveBindingReader{provider: provider}
}

var (
	_ BindingReader = (*LiveBindingReader)(nil)
	_ ScopeLocker   = (*LiveBindingReader)(nil)
)

// ReadBinding reads one binding fact set and derives the 009 class through
// ClassifyBindingResponse. 008 keys bindings by intent_id only — there is no
// attempt axis in its read contract — so attemptID stays part of 009's own
// identity and is never sent (the durable intent is the binding identity).
func (l *LiveBindingReader) ReadBinding(ctx context.Context, intentID, attemptID string) (BindingResult, error) {
	_ = attemptID
	resp, err := l.provider.Read(ctx, nonce.ReadRequest{IntentID: intentID})
	return ClassifyBindingResponse(resp, err)
}

// LockScope is the T034 ScopeLocker half (gates.md §3 bilateral rule): it
// takes the 008 scope-row `FOR SHARE` inside the caller's delivery/admission
// transaction, before any gate read, so an 008 pause/registry/release writer
// racing this transaction waits until it commits and one that committed first
// is visible to the reads that follow in the same transaction. The returned
// `ReadBinding` facts alone are never the admission's isolation basis. A
// missing scope row locks nothing; the fact read fails closed on that
// inconsistency (008 always materializes the scope row before a binding).
func (l *LiveBindingReader) LockScope(ctx context.Context, tx pgx.Tx, chainID int64, sender string) error {
	if _, err := tx.Exec(ctx, ScopeShareSQL, chainID, sender); err != nil {
		return fmt.Errorf("008 scope row FOR SHARE: %w", err)
	}
	return nil
}

// ClassifyBindingResponse is the only allowed 008 outcome mapping
// (gates.md:149-158):
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
func ClassifyBindingResponse(resp nonce.ReadResponse, err error) (BindingResult, error) {
	if err != nil {
		return BindingReadFailed, err
	}
	switch resp.Outcome {
	case nonce.ReadBound:
		ann := resp.Annotations
		if ann == nil {
			return BindingReadFailed, fmt.Errorf("008 bound read carried no annotations")
		}
		if ann.Gate.State != nonce.ReadGateOpen || len(ann.Gate.Causes) > 0 ||
			ann.Recovery.State != nonce.RecoveryNone ||
			ann.RegistryState != nonce.RegistryActive {
			return BindingPaused, nil
		}
		return BindingMatches, nil
	case nonce.ReadTerminal:
		return BindingTerminal, nil
	case nonce.ReadNotBound:
		return BindingAbsent, nil
	case nonce.ReadMismatch:
		return BindingConflict, nil
	case nonce.ReadUnavailable:
		return BindingReadFailed, nil
	default:
		return BindingReadFailed, fmt.Errorf("unexpected 008 read outcome %q", resp.Outcome)
	}
}
