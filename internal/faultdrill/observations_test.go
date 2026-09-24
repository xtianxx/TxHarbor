//go:build fault

// observations_test.go holds the deposit-observation probes for the drill
// scenes. It is a test file (not a test-support source file) because the
// upstream 004 confinement rule allows only internal/indexer to reference the
// deposit_observations table outside tests.
package faultdrill

import (
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// WaitObservation waits until the 004 scanner observed the deposit.
func (s *Scene) WaitObservation(txHash common.Hash) {
	s.T.Helper()
	if err := WaitFor(s.Ctx, 120*time.Second, func() (bool, error) {
		if err := s.checkRuntimes(); err != nil {
			return false, err
		}
		var n int64
		if err := s.Pool.QueryRow(s.Ctx,
			`SELECT count(*) FROM deposit_observations WHERE tx_hash = $1`, strings.ToLower(txHash.Hex())).Scan(&n); err != nil {
			return false, err
		}
		return n > 0, nil
	}); err != nil {
		s.T.Fatalf("deposit observation for %s: %v%s", txHash, err, s.diagnostics())
	}
}

// WaitConfirmed waits until the 005 scanner confirmed the observed deposit.
func (s *Scene) WaitConfirmed(txHash common.Hash) {
	s.T.Helper()
	if err := WaitFor(s.Ctx, 120*time.Second, func() (bool, error) {
		if err := s.checkRuntimes(); err != nil {
			return false, err
		}
		var n int64
		if err := s.Pool.QueryRow(s.Ctx,
			`SELECT count(*) FROM deposit_observations WHERE tx_hash = $1 AND status = 'confirmed'`,
			strings.ToLower(txHash.Hex())).Scan(&n); err != nil {
			return false, err
		}
		return n > 0, nil
	}); err != nil {
		s.T.Fatalf("deposit confirmation for %s: %v%s", txHash, err, s.diagnostics())
	}
}
