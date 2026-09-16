// registry.go owns the controlled wallet registry reads (nonce_wallet_registry):
// the authoritative sender admission check with admission-only gates, and the
// registry_seq version a binding carries (FR-01, R9).
//
// The registry is operator-supplied (T-registry, admin.go) and read inside the
// locked tx. Unknown sender, disabled sender, or any state other than active
// refuses admission fail-closed and preserves existing records. The gate is
// admission-only: each binding stores the registry_seq in force at admission,
// and no binding update path writes registry_seq — a later config change never
// alters an existing binding (pinned by registry_test.go).
package nonce

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Registry states (the nonce_wallet_registry.state CHECK domain).
const (
	RegistryActive   = "active"
	RegistryDisabled = "disabled"
)

// WalletRegistry is one controlled-sender row (data-model Table 1). It is the
// authority for sender admission (OC-2); the caller's sender claim is verified
// against it, never trusted.
type WalletRegistry struct {
	ChainID     int64
	Sender      string
	State       string
	RegistrySeq int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// registryColumnsSQL reads the row in scan order.
const registryColumnsSQL = `
SELECT chain_id, sender, state, registry_seq, created_at, updated_at
FROM nonce_wallet_registry`

const readRegistryBySenderSQL = registryColumnsSQL + ` WHERE chain_id = $1 AND sender = $2`

// scanRegistry reads one registry row; pgx.ErrNoRows yields (nil, nil).
func scanRegistry(row pgx.Row) (*WalletRegistry, error) {
	var r WalletRegistry
	err := row.Scan(&r.ChainID, &r.Sender, &r.State, &r.RegistrySeq, &r.CreatedAt, &r.UpdatedAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return &r, nil
}

// readRegistryBySenderTx reads the authoritative registry row for one scope.
// An absent row yields (nil, nil) — the caller refuses sender_not_registered.
// The returned registry_seq is the version carried onto a binding created
// under it. Read-only: this function never writes.
func readRegistryBySenderTx(ctx context.Context, q txQuerier, chainID int64, sender string) (*WalletRegistry, error) {
	r, err := scanRegistry(q.QueryRow(ctx, readRegistryBySenderSQL, chainID, sender))
	if err != nil {
		return nil, fmt.Errorf("read wallet registry: %w", err)
	}
	return r, nil
}

// AdmissionRefusal maps the registry row to the fail-closed admission outcome:
// "" when admissible, sender_not_registered when the sender has no row,
// sender_disabled otherwise (disabled — or any state other than active —
// refuses). Admission-only gate: it never writes and never alters an existing
// binding.
func (r *WalletRegistry) AdmissionRefusal() Outcome {
	switch {
	case r == nil:
		return OutcomeSenderNotRegistered
	case r.State != RegistryActive:
		return OutcomeSenderDisabled
	default:
		return ""
	}
}
