// binding.go owns the durable binding model, its explicit state machine
// (R6, FR-20), the append-only transition-event writers, the binding readers,
// and the read-only rebuild verification used by the startup gate (R5).
//
// States: allocated → in_flight → consumed | released. Direct
// allocated → consumed and allocated|in_flight → released are allowed;
// consumed/released are terminal with no path out. No automatic transition
// ever produces released: release is operator-only (admin.go).
package nonce

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// Binding states (the nonce_bindings.state CHECK domain).
const (
	StateAllocated = "allocated"
	StateInFlight  = "in_flight"
	StateConsumed  = "consumed"
	StateReleased  = "released"
)

// Binding is one durable nonce binding (data-model Table 3). nonce is
// integer-only (big.Int); the row is immutable except for state and the
// terminal columns.
type Binding struct {
	BindingID               string
	IntentID                string
	ChainID                 int64
	Sender                  string
	Nonce                   *big.Int
	State                   string
	AuthorizationID         string
	AuthorizationVersion    string
	RegistrySeq             int64
	AllocationObservationID string
	CreatedAt               time.Time
	UpdatedAt               time.Time
	ConsumedAt              *time.Time
	ReleasedAt              *time.Time
	ReleaseOperationID      *string
}

// IsTerminal reports whether the state has no path out.
func IsTerminal(state string) bool {
	return state == StateConsumed || state == StateReleased
}

// CanTransition reports whether from → to is a legal state-machine edge
// (operator and observer transitions combined). Illegal transitions are
// refused, never applied.
func CanTransition(from, to string) bool {
	switch from {
	case StateAllocated:
		return to == StateInFlight || to == StateConsumed || to == StateReleased
	case StateInFlight:
		return to == StateConsumed || to == StateReleased
	default:
		return false // consumed / released: terminal
	}
}

// CanAutoTransition reports the automatic (observer-driven) edges: reconcile
// may move a binding toward in_flight/consumed, and NEVER produces released.
func CanAutoTransition(from, to string) bool {
	return CanTransition(from, to) && to != StateReleased
}

// TerminalConsistent mirrors the nonce_bindings_terminal_consistency CHECK:
// pending states carry no terminal facts; each terminal state carries exactly
// its own.
func (b *Binding) TerminalConsistent() bool {
	if b.State == StateConsumed != (b.ConsumedAt != nil) {
		return false
	}
	if (b.State == StateReleased) != (b.ReleasedAt != nil && b.ReleaseOperationID != nil) {
		return false
	}
	return true
}

// BindingEvent is one append-only state-transition row (data-model Table 4).
// from_state is NULL for the creation event.
type BindingEvent struct {
	BindingID     string
	FromState     *string
	ToState       string
	ObservationID string
	OperationID   string
	Detail        string
}

// Read SQL for one binding (nonce as ::text so the numeric roundtrip is
// integer-exact and float-free).
const bindingColumnsSQL = `
SELECT binding_id, intent_id, chain_id, sender, nonce::text, state,
       authorization_id, authorization_version, registry_seq,
       allocation_observation_id, created_at, updated_at,
       consumed_at, released_at, release_operation_id
FROM nonce_bindings`

const (
	readBindingByIDSQL     = bindingColumnsSQL + ` WHERE binding_id = $1`
	readBindingByIntentSQL = bindingColumnsSQL + ` WHERE intent_id = $1`
	readBindingByNonceSQL  = bindingColumnsSQL + ` WHERE chain_id = $1 AND sender = $2 AND nonce = $3::numeric`
	readBindingsByScopeSQL = bindingColumnsSQL + ` WHERE chain_id = $1 AND sender = $2 ORDER BY nonce`

	insertBindingSQL = `
INSERT INTO nonce_bindings (binding_id, intent_id, chain_id, sender, nonce, state,
       authorization_id, authorization_version, registry_seq, allocation_observation_id)
VALUES ($1, $2, $3, $4, $5::numeric, $6, $7, $8, $9, $10)`

	insertBindingEventSQL = `
INSERT INTO nonce_binding_events (binding_id, from_state, to_state, observation_id, operation_id, detail)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (binding_id, to_state) DO NOTHING`

	advanceBindingStateSQL = `
UPDATE nonce_bindings
SET state = $2,
    updated_at = now(),
    consumed_at = CASE WHEN $2 = 'consumed' THEN now() ELSE consumed_at END,
    released_at = CASE WHEN $2 = 'released' THEN now() ELSE released_at END,
    release_operation_id = CASE WHEN $2 = 'released' THEN $3 ELSE release_operation_id END
WHERE binding_id = $1 AND state = $4`
)

// scanBinding reads one binding row; pgx.ErrNoRows yields (nil, nil).
func scanBinding(row pgx.Row) (*Binding, error) {
	var (
		b          Binding
		nonceText  string
		consumedAt *time.Time
		releasedAt *time.Time
		releaseOp  *string
	)
	err := row.Scan(&b.BindingID, &b.IntentID, &b.ChainID, &b.Sender, &nonceText, &b.State,
		&b.AuthorizationID, &b.AuthorizationVersion, &b.RegistrySeq,
		&b.AllocationObservationID, &b.CreatedAt, &b.UpdatedAt,
		&consumedAt, &releasedAt, &releaseOp)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, err
	}
	nonce, err := ParseDecimal(nonceText)
	if err != nil {
		return nil, fmt.Errorf("binding %s nonce: %w", b.BindingID, err)
	}
	b.Nonce = nonce
	b.ConsumedAt, b.ReleasedAt, b.ReleaseOperationID = consumedAt, releasedAt, releaseOp
	return &b, nil
}

// readBindingByIDTx reads one binding by its stable public identity.
func readBindingByIDTx(ctx context.Context, q txQuerier, bindingID string) (*Binding, error) {
	b, err := scanBinding(q.QueryRow(ctx, readBindingByIDSQL, bindingID))
	if err != nil {
		return nil, fmt.Errorf("read binding by id: %w", err)
	}
	return b, nil
}

// readBindingByIntentTx reads one binding by intent identity — the in-tx
// replay/conflict re-read (R3).
func readBindingByIntentTx(ctx context.Context, q txQuerier, intentID string) (*Binding, error) {
	b, err := scanBinding(q.QueryRow(ctx, readBindingByIntentSQL, intentID))
	if err != nil {
		return nil, fmt.Errorf("read binding by intent: %w", err)
	}
	return b, nil
}

// readBindingByScopeNonceTx reads one binding by its scope+nonce carrier —
// the second step of the fixed-order T-converge classify.
func readBindingByScopeNonceTx(ctx context.Context, q txQuerier, chainID int64, sender string, nonce *big.Int) (*Binding, error) {
	b, err := scanBinding(q.QueryRow(ctx, readBindingByNonceSQL, chainID, sender, NumericValue(nonce)))
	if err != nil {
		return nil, fmt.Errorf("read binding by scope+nonce: %w", err)
	}
	return b, nil
}

// readBindingsByScopeTx lists a scope's bindings ordered by nonce; M is
// always recomputed from this set under the lock.
func readBindingsByScopeTx(ctx context.Context, q txQuerier, chainID int64, sender string) ([]Binding, error) {
	rowRows, err := q.Query(ctx, readBindingsByScopeSQL, chainID, sender)
	if err != nil {
		return nil, fmt.Errorf("list scope bindings: %w", err)
	}
	defer rowRows.Close()
	var out []Binding
	for rowRows.Next() {
		b, err := scanBinding(rowRows)
		if err != nil {
			return nil, err
		}
		out = append(out, *b)
	}
	if err := rowRows.Err(); err != nil {
		return nil, fmt.Errorf("list scope bindings: %w", err)
	}
	return out, nil
}

// insertBindingTx inserts one binding row (the UNIQUE carriers decide
// replay/conflict on 23505).
func insertBindingTx(ctx context.Context, q txQuerier, b Binding) error {
	_, err := q.Exec(ctx, insertBindingSQL,
		b.BindingID, b.IntentID, b.ChainID, b.Sender, NumericValue(b.Nonce), b.State,
		b.AuthorizationID, b.AuthorizationVersion, b.RegistrySeq, b.AllocationObservationID)
	if err != nil {
		return fmt.Errorf("insert binding: %w", err)
	}
	return nil
}

// insertBindingEventTx appends one transition row; a repeat transition
// converges on nonce_binding_events_binding_to_uniq instead of duplicating
// history (006 transition-log idiom).
func insertBindingEventTx(ctx context.Context, q txQuerier, ev BindingEvent) (bool, error) {
	from := ev.FromState
	if from != nil && *from == "" {
		from = nil
	}
	var fromArg any
	if from != nil {
		fromArg = *from
	}
	var obsArg, opArg any
	if ev.ObservationID != "" {
		obsArg = ev.ObservationID
	}
	if ev.OperationID != "" {
		opArg = ev.OperationID
	}
	tag, err := q.Exec(ctx, insertBindingEventSQL, ev.BindingID, fromArg, ev.ToState, obsArg, opArg, ev.Detail)
	if err != nil {
		return false, fmt.Errorf("insert binding event: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// advanceBindingStateTx applies one legal transition guarded by the expected
// current state; zero affected rows is a concurrent-move refusal.
func advanceBindingStateTx(ctx context.Context, q txQuerier, bindingID, to, operationID, from string) (bool, error) {
	if !CanTransition(from, to) {
		return false, fmt.Errorf("illegal binding transition %s -> %s", from, to)
	}
	var opArg any
	if operationID != "" {
		opArg = operationID
	}
	tag, err := q.Exec(ctx, advanceBindingStateSQL, bindingID, to, opArg, from)
	if err != nil {
		return false, fmt.Errorf("advance binding state: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// requiredConstraintNames are the named carriers the classification consumes
// plus the table keys the rebuild verification trusts; the migration declares
// every one explicitly (never PG auto-naming).
var requiredConstraintNames = []string{
	"nonce_wallet_registry_pkey",
	"nonce_scope_state_pkey",
	"nonce_scope_state_registry_fkey",
	"nonce_bindings_pkey",
	"nonce_bindings_intent_uniq",
	"nonce_bindings_scope_nonce_uniq",
	"nonce_bindings_registry_fkey",
	"nonce_bindings_state_check",
	"nonce_bindings_terminal_consistency",
	"nonce_bindings_nonce_range",
	"nonce_bindings_intent_shape",
	"nonce_binding_events_binding_to_uniq",
	"nonce_scope_holds_status_consistency",
	"nonce_scope_holds_registry_fkey",
	"nonce_ops_audit_pkey",
	"nonce_ops_audit_operation_id_uniq",
}

// RebuildReport is the read-only rebuild verification result (R5).
type RebuildReport struct {
	ChainID          int64
	Scopes           int64
	Bindings         int64
	Holds            int64
	ConstraintsFound int64
}

const (
	countScopesSQL      = `SELECT count(*) FROM nonce_scope_state WHERE chain_id = $1`
	countBindingsSQL    = `SELECT count(*) FROM nonce_bindings WHERE chain_id = $1`
	countHoldsSQL       = `SELECT count(*) FROM nonce_scope_holds WHERE chain_id = $1`
	countConstraintsSQL = `
SELECT count(*) FROM pg_constraint WHERE conname = ANY($1::text[])`
	countInvalidBindingsSQL = `
SELECT count(*) FROM nonce_bindings
WHERE chain_id = $1 AND (
    state NOT IN ('allocated', 'in_flight', 'consumed', 'released')
    OR (state = 'consumed') <> (consumed_at IS NOT NULL)
    OR (state = 'released') <> (released_at IS NOT NULL AND release_operation_id IS NOT NULL)
    OR nonce < 0 OR nonce > 18446744073709551615
    OR length(intent_id) NOT BETWEEN 1 AND 128
)`
	countInvalidScopesSQL = `
SELECT count(*) FROM nonce_scope_state
WHERE chain_id = $1 AND (
    (reconciled_floor IS NOT NULL AND (reconciled_floor < 0 OR reconciled_floor > 18446744073709551615))
    OR (last_latest IS NOT NULL AND (last_latest < 0 OR last_latest > 18446744073709551615))
    OR (last_pending IS NOT NULL AND (last_pending < 0 OR last_pending > 18446744073709551615))
)`
	countInvalidHoldsSQL = `
SELECT count(*) FROM nonce_scope_holds
WHERE chain_id = $1 AND (
    status NOT IN ('active', 'released')
    OR (status = 'active') <> (released_at IS NULL)
    OR length(evidence_observation_id) = 0
)`
)

// VerifyRebuild runs the read-only per-known-scope integrity verification
// (R5, T-rebuild-verify): every named carrier is declared, every scope's
// bindings/floors/holds are readable and constraint-consistent. It writes
// nothing; success is the only condition that opens the admission gate.
func VerifyRebuild(ctx context.Context, q txQuerier, chainID int64) (RebuildReport, error) {
	rep := RebuildReport{ChainID: chainID}

	var found int64
	names := append([]string(nil), requiredConstraintNames...)
	if err := q.QueryRow(ctx, countConstraintsSQL, names).Scan(&found); err != nil {
		return rep, fmt.Errorf("rebuild verify: constraint probe: %w", err)
	}
	rep.ConstraintsFound = found
	if int(found) != len(requiredConstraintNames) {
		return rep, fmt.Errorf("rebuild verify: %d of %d named carriers present",
			found, len(requiredConstraintNames))
	}

	if err := q.QueryRow(ctx, countScopesSQL, chainID).Scan(&rep.Scopes); err != nil {
		return rep, fmt.Errorf("rebuild verify: scope read: %w", err)
	}
	if err := q.QueryRow(ctx, countBindingsSQL, chainID).Scan(&rep.Bindings); err != nil {
		return rep, fmt.Errorf("rebuild verify: binding read: %w", err)
	}
	if err := q.QueryRow(ctx, countHoldsSQL, chainID).Scan(&rep.Holds); err != nil {
		return rep, fmt.Errorf("rebuild verify: hold read: %w", err)
	}

	var bad int64
	if err := q.QueryRow(ctx, countInvalidBindingsSQL, chainID).Scan(&bad); err != nil {
		return rep, fmt.Errorf("rebuild verify: binding integrity: %w", err)
	}
	if bad != 0 {
		return rep, fmt.Errorf("rebuild verify: %d bindings are constraint-inconsistent", bad)
	}
	if err := q.QueryRow(ctx, countInvalidScopesSQL, chainID).Scan(&bad); err != nil {
		return rep, fmt.Errorf("rebuild verify: scope integrity: %w", err)
	}
	if bad != 0 {
		return rep, fmt.Errorf("rebuild verify: %d scope rows are constraint-inconsistent", bad)
	}
	if err := q.QueryRow(ctx, countInvalidHoldsSQL, chainID).Scan(&bad); err != nil {
		return rep, fmt.Errorf("rebuild verify: hold integrity: %w", err)
	}
	if bad != 0 {
		return rep, fmt.Errorf("rebuild verify: %d hold rows are constraint-inconsistent", bad)
	}
	return rep, nil
}

// unusedNumeric keeps the pgtype import meaningful for future numeric
// readers while the binding readers use ::text transport.
var _ = pgtype.Numeric{}
