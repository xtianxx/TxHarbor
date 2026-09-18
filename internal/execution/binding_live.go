//go:build integration

package execution

import (
	"context"
	"fmt"

	"github.com/xtianxx/txharbor/internal/nonce"
)

// LiveBindingReader adapts 008's exported read provider to BindingReader. It
// holds no cache: every call is one fresh provider read, so a decision is never
// based on a pre-transaction observation. 011 never allocates, consumes,
// modifies or releases a binding (OC-3); it observes and records the class.
type LiveBindingReader struct {
	provider *nonce.ReadProvider
}

func NewLiveBindingReader(provider *nonce.ReadProvider) *LiveBindingReader {
	return &LiveBindingReader{provider: provider}
}

var _ BindingReader = (*LiveBindingReader)(nil)

func (l *LiveBindingReader) ReadBinding(ctx context.Context, intentID string) (BindingResult, error) {
	resp, err := l.provider.Read(ctx, nonce.ReadRequest{IntentID: intentID})
	return ClassifyBindingResponse(resp, err)
}

// ClassifyBindingResponse is the only allowed 008 outcome mapping (gates.md
// §3): bound with no hold, recovery none and registry active is BindingMatches;
// any other bound answer is BindingPaused; terminal -> BindingTerminal;
// not_bound -> BindingAbsent; mismatch -> BindingConflict; unavailable ->
// BindingReadFailed. A read error or an unknown outcome fails closed.
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
