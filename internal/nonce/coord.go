// coord.go owns 008's shared coordination and gate primitives: the repo's
// single coordination discipline every 008 write transaction opens with, the
// read-only 006 gate reads, the per-scope row lock helpers, and the startup
// rebuild verification gate.
//
// Coordination (R1/R4): 008 reuses the repo's chain coordination row
// (indexer_lease) as its single lock object — BEGIN → SET LOCAL
// statement_timeout='5s' → ensure the row → SELECT … FOR UPDATE → post-lock
// rechecks → COMMIT, the same 5-step shape as internal/indexer/reorgcommit.go
// lockRecoveryChain. 008 never acquires or renews the lease and never writes
// indexer_lease; it only serializes on the row so admission is linearizable
// against 006 recovery establishment.
//
// The indexer's constants are package-private and internal/indexer is out of
// 008's change scope, so the identical statements are declared locally here
// (accepted: no cross-package coupling). coord_test.go pins the local guard
// literal against the repo's shared 5s value read from
// internal/indexer/scanner.go, so the copy and the indexer cannot silently
// diverge.
//
// Fixed lock order inside every 008 write tx (and the cross-module order
// shared with 006/007/009): coordination row → scope nonce_scope_state row →
// 006 gate reads → 007 authorization row → 008-owned rows. Reads of 006/007
// state are non-locking SELECTs; 008 writes zero 006/007 rows.
package nonce

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// txQuerier is the minimal statement surface shared by pgx.Tx and
// *pgxpool.Pool. Unit tests substitute scripted fakes.
type txQuerier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// localWriteGuard is the local copy of the indexer's shared per-statement
// bound; coord_test.go asserts equality with internal/indexer/scanner.go's
// writeGuard literal.
const localWriteGuard = "SET LOCAL statement_timeout = '5s'"

// Coordination + gate SQL. Every string is a local declaration (see the file
// doc); names/shapes mirror internal/indexer's write protocol.
const (
	// ensureCoordRowSQL is write-protocol step 2: idempotently make sure the
	// coordination row exists so step 3 always has a row to lock. 008 never
	// renews or fences the lease.
	ensureCoordRowSQL = `
INSERT INTO indexer_lease (chain_id, owner_id, fencing_token, expires_at)
VALUES ($1, $2, $3, now() + make_interval(secs => $4))
ON CONFLICT (chain_id) DO NOTHING`

	// lockCoordRowSQL is write-protocol step 3: take the chain-wide
	// coordination lock and re-read the current owner under it.
	lockCoordRowSQL = `
SELECT owner_id, fencing_token, expires_at > now()
FROM indexer_lease WHERE chain_id = $1 FOR UPDATE`

	// 006 gate reads (read-only; 008 never writes these rows). Any hit
	// refuses the admission with the cause recorded and zero writes.
	indexerPauseExistsSQL = `SELECT 1 FROM indexer_pause WHERE chain_id = $1`
	logPauseExistsSQL     = `SELECT 1 FROM log_pause WHERE chain_id = $1`
	depositPauseExistsSQL = `SELECT 1 FROM deposit_pause WHERE chain_id = $1`
	activeRecoverySQL     = `SELECT recovery_id, phase FROM reorg_recovery WHERE chain_id = $1`
	// recoveryReleasedSQL is the read-only 006 completion marker (mirrors
	// internal/indexer/reorgquery.go RecoveryReleased): the terminal events
	// survive the active-row DELETE.
	recoveryReleasedSQL = `
SELECT 1 FROM reorg_recovery_events
WHERE chain_id = $1 AND event IN ('auto_completed', 'released') LIMIT 1`

	// Scope-row SQL. The row is created lazily by the first observation or
	// allocation; it is a durable cache of evidence-linked facts, never an
	// authority over bindings.
	ensureScopeRowSQL = `
INSERT INTO nonce_scope_state (chain_id, sender)
VALUES ($1, $2)
ON CONFLICT (chain_id, sender) DO NOTHING`
	lockScopeRowSQL = `
SELECT chain_id FROM nonce_scope_state
WHERE chain_id = $1 AND sender = $2 FOR UPDATE`
	shareScopeRowSQL = `
SELECT chain_id FROM nonce_scope_state
WHERE chain_id = $1 AND sender = $2 FOR SHARE`
	readScopeStateSQL = `
SELECT reconciled_floor::text, last_latest::text, last_pending::text,
       last_observation_id, updated_at
FROM nonce_scope_state WHERE chain_id = $1 AND sender = $2`
)

// GateState is the read-only 006 gate snapshot taken inside a locked tx.
// Any active member refuses new admissions (006 precedence, FR-14).
type GateState struct {
	IndexerPause  bool
	LogPause      bool
	DepositPause  bool
	RecoveryID    string // active reorg_recovery row id, "" when none
	RecoveryPhase string
}

// Any reports whether any 006 gate is active.
func (g GateState) Any() bool {
	return g.IndexerPause || g.LogPause || g.DepositPause || g.RecoveryID != ""
}

// Causes lists the active gate causes in a stable order.
func (g GateState) Causes() []string {
	var out []string
	if g.IndexerPause {
		out = append(out, "indexer_pause")
	}
	if g.LogPause {
		out = append(out, "log_pause")
	}
	if g.DepositPause {
		out = append(out, "deposit_pause")
	}
	if g.RecoveryID != "" {
		out = append(out, "reorg_recovery:"+g.RecoveryPhase)
	}
	return out
}

// Refusal returns the fail-closed admission outcome for an active gate, or
// "" when the gates are clear.
func (g GateState) Refusal() Outcome {
	if g.Any() {
		return OutcomeRecoveryActive
	}
	return ""
}

// lockChain runs the shared 5-step coordination framing for one 008 write
// transaction: statement guard → ensure coordination row → FOR UPDATE lock.
// It takes no lease and writes nothing: the INSERT is ON CONFLICT DO NOTHING
// and only materialises the shared row when it does not exist yet.
func lockChain(ctx context.Context, tx txQuerier, chainID int64) error {
	if _, err := tx.Exec(ctx, localWriteGuard); err != nil {
		return fmt.Errorf("nonce transaction statement guard: %w", err)
	}
	if _, err := tx.Exec(ctx, ensureCoordRowSQL, chainID, "nonce-008", int64(0), float64(3600)); err != nil {
		return fmt.Errorf("ensure coordination row: %w", err)
	}
	var (
		owner string
		token int64
		valid bool
	)
	if err := tx.QueryRow(ctx, lockCoordRowSQL, chainID).Scan(&owner, &token, &valid); err != nil {
		return fmt.Errorf("lock coordination row: %w", err)
	}
	return nil
}

// readGateStateTx reads the three 006 pause rows and the active recovery row
// (non-locking SELECTs, after the coordination lock). A hit is a refusal
// cause; this function never writes.
func readGateStateTx(ctx context.Context, tx txQuerier, chainID int64) (GateState, error) {
	var g GateState
	var one int
	if err := tx.QueryRow(ctx, indexerPauseExistsSQL, chainID).Scan(&one); err == nil {
		g.IndexerPause = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return GateState{}, fmt.Errorf("read indexer_pause gate: %w", err)
	}
	if err := tx.QueryRow(ctx, logPauseExistsSQL, chainID).Scan(&one); err == nil {
		g.LogPause = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return GateState{}, fmt.Errorf("read log_pause gate: %w", err)
	}
	if err := tx.QueryRow(ctx, depositPauseExistsSQL, chainID).Scan(&one); err == nil {
		g.DepositPause = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return GateState{}, fmt.Errorf("read deposit_pause gate: %w", err)
	}
	if err := tx.QueryRow(ctx, activeRecoverySQL, chainID).Scan(&g.RecoveryID, &g.RecoveryPhase); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return GateState{}, fmt.Errorf("read reorg_recovery gate: %w", err)
		}
	}
	return g, nil
}

// ensureScopeRowTx materialises the scope row (lazy creation) so the FOR
// UPDATE helper always has a row to lock.
func ensureScopeRowTx(ctx context.Context, tx txQuerier, chainID int64, sender string) error {
	if _, err := tx.Exec(ctx, ensureScopeRowSQL, chainID, sender); err != nil {
		return fmt.Errorf("ensure scope row: %w", err)
	}
	return nil
}

// lockScopeRowTx takes the scope row FOR UPDATE after the coordination lock
// (fixed order). The row must exist (ensureScopeRowTx first).
func lockScopeRowTx(ctx context.Context, tx txQuerier, chainID int64, sender string) error {
	var cid int64
	if err := tx.QueryRow(ctx, lockScopeRowSQL, chainID, sender).Scan(&cid); err != nil {
		return fmt.Errorf("lock scope row: %w", err)
	}
	return nil
}

// shareScopeRowTx takes the scope row FOR SHARE (read-provider side of the
// bilateral lock protocol) and reports whether the row exists. A read that
// finds no scope row locks nothing and returns false.
func shareScopeRowTx(ctx context.Context, tx txQuerier, chainID int64, sender string) (bool, error) {
	var cid int64
	err := tx.QueryRow(ctx, shareScopeRowSQL, chainID, sender).Scan(&cid)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("share scope row: %w", err)
	}
	return true, nil
}

// RebuildGate is the fail-closed startup readiness gate (R5, FR-13). The
// zero value is closed: until a successful rebuild verification opens it,
// every allocation refuses rebuild_incomplete. Verification failure keeps the
// gate closed with the structured reason; there is no silent repair and no
// memory-based allocation.
type RebuildGate struct {
	mu     sync.RWMutex
	open   bool
	reason string
}

// NewRebuildGate returns a closed gate.
func NewRebuildGate() *RebuildGate { return &RebuildGate{} }

// Open marks the rebuild verification as complete. It is the only transition
// to open.
func (g *RebuildGate) Open() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.open = true
	g.reason = ""
}

// KeepClosed records why the gate stays closed (verification failure or a
// missing/incomplete pass). It never opens a closed gate.
func (g *RebuildGate) KeepClosed(reason string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.open = false
	g.reason = reason
}

// State reports the gate state and, when closed, the structured reason.
func (g *RebuildGate) State() (open bool, reason string) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.open, g.reason
}

// IsOpen reports whether allocation may proceed.
func (g *RebuildGate) IsOpen() bool {
	open, _ := g.State()
	return open
}
