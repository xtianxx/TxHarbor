// binding.go is the untagged production port of 011's sanctioned 008
// read-only seam. internal/execution's canonical adapter (binding_live.go) is
// integration-tagged because that package's default import graph must stay
// free of 008's RPC-bearing read package (boundary_test.go); this port has the
// same shape as internal/app/signerlive.go for 009, and the live parity test
// pins it to execution.ClassifyBindingResponse so the copy cannot drift.
package jointwire

import (
	"context"
	"fmt"

	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/nonce"
)

// bindingReader observes one 008 binding class per read. It holds no cache, so
// a decision is never based on a pre-transaction observation, and it never
// allocates, consumes, modifies or releases a binding (OC-3).
type bindingReader struct {
	provider *nonce.ReadProvider
}

var _ execution.BindingReader = (*bindingReader)(nil)

func (b *bindingReader) ReadBinding(ctx context.Context, intentID string) (execution.BindingResult, error) {
	resp, err := b.provider.Read(ctx, nonce.ReadRequest{IntentID: intentID})
	return classifyBindingResponse(resp, err)
}

// classifyBindingResponse mirrors execution.ClassifyBindingResponse over the
// closed 008 outcome set; a read error or an unknown outcome fails closed to
// BindingReadFailed.
func classifyBindingResponse(resp nonce.ReadResponse, err error) (execution.BindingResult, error) {
	if err != nil {
		return execution.BindingReadFailed, err
	}
	switch resp.Outcome {
	case nonce.ReadBound:
		ann := resp.Annotations
		if ann == nil {
			return execution.BindingReadFailed, fmt.Errorf("008 bound read carried no annotations")
		}
		if ann.Gate.State != nonce.ReadGateOpen || len(ann.Gate.Causes) > 0 ||
			ann.Recovery.State != nonce.RecoveryNone ||
			ann.RegistryState != nonce.RegistryActive {
			return execution.BindingPaused, nil
		}
		return execution.BindingMatches, nil
	case nonce.ReadTerminal:
		return execution.BindingTerminal, nil
	case nonce.ReadNotBound:
		return execution.BindingAbsent, nil
	case nonce.ReadMismatch:
		return execution.BindingConflict, nil
	case nonce.ReadUnavailable:
		return execution.BindingReadFailed, nil
	default:
		return execution.BindingReadFailed, fmt.Errorf("unexpected 008 read outcome %q", resp.Outcome)
	}
}
