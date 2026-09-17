//go:build integration

package jointwire

import (
	"errors"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/nonce"
)

// TestBindingPortParityWithCanonicalMapping pins the untagged jointwire port to
// the canonical integration-tagged mapping (execution.ClassifyBindingResponse)
// over the full 008 outcome matrix, so the copy cannot drift.
func TestBindingPortParityWithCanonicalMapping(t *testing.T) {
	open := nonce.ReadAnnotations{
		Gate:          nonce.ReadGate{State: nonce.ReadGateOpen},
		Recovery:      nonce.ReadRecovery{State: nonce.RecoveryNone},
		RegistryState: nonce.RegistryActive,
	}
	cases := []struct {
		name    string
		resp    nonce.ReadResponse
		err     error
		want    execution.BindingResult
		wantErr bool
	}{
		{name: "bound clean", resp: nonce.ReadResponse{Outcome: nonce.ReadBound, Annotations: &open},
			want: execution.BindingMatches},
		{name: "bound gate held", resp: nonce.ReadResponse{Outcome: nonce.ReadBound, Annotations: &nonce.ReadAnnotations{
			Gate:          nonce.ReadGate{State: nonce.ReadGateHeld, Causes: []nonce.ReadGateCause{{HoldID: "hold-1", Cause: "reconcile", EstablishedAt: time.Now()}}},
			Recovery:      nonce.ReadRecovery{State: nonce.RecoveryNone},
			RegistryState: nonce.RegistryActive,
		}}, want: execution.BindingPaused},
		{name: "bound open gate with causes", resp: nonce.ReadResponse{Outcome: nonce.ReadBound, Annotations: &nonce.ReadAnnotations{
			Gate:          nonce.ReadGate{State: nonce.ReadGateOpen, Causes: []nonce.ReadGateCause{{HoldID: "hold-2", Cause: "hold", EstablishedAt: time.Now()}}},
			Recovery:      nonce.ReadRecovery{State: nonce.RecoveryNone},
			RegistryState: nonce.RegistryActive,
		}}, want: execution.BindingPaused},
		{name: "bound recovery recovering", resp: nonce.ReadResponse{Outcome: nonce.ReadBound, Annotations: &nonce.ReadAnnotations{
			Gate:          nonce.ReadGate{State: nonce.ReadGateOpen},
			Recovery:      nonce.ReadRecovery{State: nonce.RecoveryRecovering},
			RegistryState: nonce.RegistryActive,
		}}, want: execution.BindingPaused},
		{name: "bound registry disabled", resp: nonce.ReadResponse{Outcome: nonce.ReadBound, Annotations: &nonce.ReadAnnotations{
			Gate:          nonce.ReadGate{State: nonce.ReadGateOpen},
			Recovery:      nonce.ReadRecovery{State: nonce.RecoveryNone},
			RegistryState: nonce.RegistryDisabled,
		}}, want: execution.BindingPaused},
		{name: "bound nil annotations", resp: nonce.ReadResponse{Outcome: nonce.ReadBound},
			want: execution.BindingReadFailed, wantErr: true},
		{name: "terminal", resp: nonce.ReadResponse{Outcome: nonce.ReadTerminal}, want: execution.BindingTerminal},
		{name: "not_bound", resp: nonce.ReadResponse{Outcome: nonce.ReadNotBound}, want: execution.BindingAbsent},
		{name: "mismatch", resp: nonce.ReadResponse{Outcome: nonce.ReadMismatch}, want: execution.BindingConflict},
		{name: "unavailable", resp: nonce.ReadResponse{Outcome: nonce.ReadUnavailable}, want: execution.BindingReadFailed},
		{name: "unauthenticated", resp: nonce.ReadResponse{Outcome: nonce.ReadUnauthenticated},
			want: execution.BindingReadFailed, wantErr: true},
		{name: "unknown outcome", resp: nonce.ReadResponse{Outcome: nonce.Outcome("future_outcome")},
			want: execution.BindingReadFailed, wantErr: true},
		{name: "provider read error", resp: nonce.ReadResponse{Outcome: nonce.ReadBound, Annotations: &open},
			err: errors.New("provider read failed"), want: execution.BindingReadFailed, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			portClass, portErr := classifyBindingResponse(tc.resp, tc.err)
			canonClass, canonErr := execution.ClassifyBindingResponse(tc.resp, tc.err)
			if portClass != canonClass || (portErr == nil) != (canonErr == nil) {
				t.Fatalf("port mapping = %v/%v, canonical = %v/%v", portClass, portErr, canonClass, canonErr)
			}
			if portClass != tc.want || (portErr != nil) != tc.wantErr {
				t.Fatalf("port mapping = %v/%v, want class %v err=%v", portClass, portErr, tc.want, tc.wantErr)
			}
		})
	}
}
