package execution

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrGateReadFailed is the fail-closed outcome of any indeterminate upstream
// gate read (lock wait, statement timeout, deadlock, transport error): no send
// may follow, and the caller records gate_read_failed.
var ErrGateReadFailed = errors.New("gate read failed")

func gateReadErr(op string, err error) error {
	return fmt.Errorf("%w: %s: %v", ErrGateReadFailed, op, err)
}

// GateStatementTimeout bounds every point statement in a gate transaction (the
// 009 renewLockGuard shape); a lock wait past it becomes gate_read_failed.
const GateStatementTimeout = "SET LOCAL statement_timeout = '5s'"

// GateLockSQL takes the relation-level SHARE lock over the 006 gate tables
// before any gate read, so the observation is linearly ordered against
// pause/recovery writers. Lock acquisition only; no data modification.
const GateLockSQL = `LOCK TABLE indexer_pause, log_pause, deposit_pause, reorg_recovery, reorg_recovery_events IN SHARE MODE`

// RecoverySnapshotSQL is the one-statement 006 snapshot: three pause flags plus
// the active recovery identity/phase/seq and the events high-water ($1 =
// chain_id). One statement so no two reads can straddle a committing write.
const RecoverySnapshotSQL = `SELECT
  EXISTS (SELECT 1 FROM indexer_pause) AS indexer_paused,
  EXISTS (SELECT 1 FROM log_pause) AS log_paused,
  EXISTS (SELECT 1 FROM deposit_pause) AS deposit_paused,
  (SELECT recovery_id FROM reorg_recovery WHERE chain_id = $1) AS recovery_id,
  (SELECT phase FROM reorg_recovery WHERE chain_id = $1) AS recovery_phase,
  (SELECT recovery_seq FROM reorg_recovery WHERE chain_id = $1) AS recovery_seq,
  (SELECT MAX(recovery_seq) FROM reorg_recovery_events WHERE chain_id = $1) AS events_max`

// RegistryShareSQL locks the 008 registry row for (chain_id, sender) FOR SHARE
// inside the admission transaction; a missing row is sender_unregistered.
const RegistryShareSQL = `SELECT state FROM nonce_wallet_registry
  WHERE chain_id = $1 AND sender = $2 FOR SHARE`

// RequestReadSQL reads the 007 request row for admission; status must be
// 'accepted' (receipt, never authority). Ownership is enforced by the caller.
const RequestReadSQL = `SELECT caller_id, chain_id, asset, recipient, amount::text, status
  FROM withdrawal_requests WHERE request_id = $1`

// GrantReadSQL reads the 007 grant row FOR SHARE and, on the DB clock only,
// whether it is unexpired. A revoke committing before the share lock is
// observed; one racing it waits for this transaction.
const GrantReadSQL = `SELECT caller_id, chain_id, asset, recipient, amount::text, state, expires_at,
  (expires_at IS NULL OR expires_at > now()) AS unexpired
  FROM withdrawal_authorizations WHERE authorization_id = $1 FOR SHARE`

// ScopeReadSQL reads the PB carrier row FOR SHARE in the same grant sequence.
// Absence is authorization_unverifiable (PB Q-B), never a fresh-grant
// reinterpretation.
const ScopeReadSQL = `SELECT authorization_id, intent_id, request_id, sender,
  fee_max_total, fee_max_per_gas, fee_max_priority, allows_fee_replacement, authorization_version
  FROM withdrawal_authorization_scopes WHERE authorization_id = $1 FOR SHARE`

// BeginGateTx opens a gate transaction with the fixed statement-timeout guard.
// Every send-enabling gate read runs inside such a transaction.
func BeginGateTx(ctx context.Context, pool *pgxpool.Pool) (pgx.Tx, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, gateReadErr("begin gate transaction", err)
	}
	if _, err := tx.Exec(ctx, GateStatementTimeout); err != nil {
		_ = tx.Rollback(ctx)
		return nil, gateReadErr("statement_timeout guard", err)
	}
	return tx, nil
}

// LockGateTables takes the 006 gate-table SHARE lock; any lock error is
// gate_read_failed.
func LockGateTables(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, GateLockSQL); err != nil {
		return gateReadErr("gate table SHARE lock", err)
	}
	return nil
}

// RecoveryGate is the observed 006 gate basis (gates.md §1).
type RecoveryGate struct {
	IndexerPaused bool
	LogPaused     bool
	DepositPaused bool
	HasRecovery   bool
	RecoveryPhase string
	RecoverySeq   int64
	EventsMax     int64
	HasEventsMax  bool
}

// CurrentVersion is active recovery_seq, else events MAX, else 0.
func (g *RecoveryGate) CurrentVersion() int64 {
	if g.HasRecovery {
		return g.RecoverySeq
	}
	if g.HasEventsMax {
		return g.EventsMax
	}
	return 0
}

// PauseBases names the pause rows present, for the recorded basis.
func (g *RecoveryGate) PauseBases() []string {
	var out []string
	if g.IndexerPaused {
		out = append(out, "indexer_pause")
	}
	if g.LogPaused {
		out = append(out, "log_pause")
	}
	if g.DepositPaused {
		out = append(out, "deposit_pause")
	}
	return out
}

// Evaluate applies the §1 refusal table against the version observed at issue.
// "" means the gate passes.
func (g *RecoveryGate) Evaluate(requestVersion int64) RefusalClass {
	if g.IndexerPaused || g.LogPaused || g.DepositPaused {
		return ClassRecoveryPaused
	}
	if g.HasRecovery {
		return ClassRecoveryActive
	}
	if g.CurrentVersion() != requestVersion {
		return ClassRecoveryVersionChanged
	}
	return ""
}

// SendRefusal is the send-path 006 check: any pause row or an active recovery
// instance refuses. The observed version is recorded as evidence (the
// step-issue version) rather than compared to admission; a version move
// between 011's observation and 010's build/send is 010's refused_basis.
func (g *RecoveryGate) SendRefusal() RefusalClass {
	if g.IndexerPaused || g.LogPaused || g.DepositPaused {
		return ClassRecoveryPaused
	}
	if g.HasRecovery {
		return ClassRecoveryActive
	}
	return ""
}

// ReadRecoveryGate reads the one-statement 006 snapshot; the caller holds the
// gate-table SHARE lock.
func ReadRecoveryGate(ctx context.Context, tx pgx.Tx, chainID int64) (RecoveryGate, error) {
	var (
		g          RecoveryGate
		recoveryID *string
		phase      *string
		seq        *int64
		eventsMax  *int64
	)
	if err := tx.QueryRow(ctx, RecoverySnapshotSQL, chainID).Scan(
		&g.IndexerPaused, &g.LogPaused, &g.DepositPaused,
		&recoveryID, &phase, &seq, &eventsMax); err != nil {
		return g, gateReadErr("006 recovery snapshot", err)
	}
	if recoveryID != nil {
		g.HasRecovery = true
	}
	if phase != nil {
		g.RecoveryPhase = *phase
	}
	if seq != nil {
		g.RecoverySeq = *seq
	}
	if eventsMax != nil {
		g.HasEventsMax = true
		g.EventsMax = *eventsMax
	}
	return g, nil
}

// RequestRow is the observed 007 request row.
type RequestRow struct {
	CallerID  int64
	ChainID   int64
	Asset     string
	Recipient string
	Amount    *big.Int
	Status    string
}

// ReadRequest reads the 007 request row; found=false when it does not exist
// (the caller renders missing and foreign identically).
func ReadRequest(ctx context.Context, tx pgx.Tx, requestID string) (RequestRow, bool, error) {
	var (
		r      RequestRow
		amount string
	)
	err := tx.QueryRow(ctx, RequestReadSQL, requestID).Scan(
		&r.CallerID, &r.ChainID, &r.Asset, &r.Recipient, &amount, &r.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, false, nil
	}
	if err != nil {
		return r, false, gateReadErr("007 request row", err)
	}
	parsed, ok := new(big.Int).SetString(amount, 10)
	if !ok {
		return r, false, gateReadErr("007 request amount", fmt.Errorf("%q is not an integer decimal", amount))
	}
	r.Amount = parsed
	return r, true, nil
}

// IsAccepted reports the only admission-eligible status.
func (r RequestRow) IsAccepted() bool { return r.Status == "accepted" }

// GrantRow is the observed 007 grant row. Unexpired is decided by the DB clock
// in the read statement, never by the application clock.
type GrantRow struct {
	CallerID  int64
	ChainID   int64
	Asset     string
	Recipient string
	Amount    *big.Int
	State     string
	ExpiresAt *time.Time
	Unexpired bool
}

// ReadGrant reads the 007 grant row FOR SHARE; found=false when absent.
func ReadGrant(ctx context.Context, tx pgx.Tx, authorizationID string) (GrantRow, bool, error) {
	var (
		g      GrantRow
		amount string
	)
	err := tx.QueryRow(ctx, GrantReadSQL, authorizationID).Scan(
		&g.CallerID, &g.ChainID, &g.Asset, &g.Recipient, &amount, &g.State, &g.ExpiresAt, &g.Unexpired)
	if errors.Is(err, pgx.ErrNoRows) {
		return g, false, nil
	}
	if err != nil {
		return g, false, gateReadErr("007 grant row", err)
	}
	parsed, ok := new(big.Int).SetString(amount, 10)
	if !ok {
		return g, false, gateReadErr("007 grant amount", fmt.Errorf("%q is not an integer decimal", amount))
	}
	g.Amount = parsed
	return g, true, nil
}

// Evaluate checks a found grant against the request: active state, DB-clock
// expiry and field equality (amount as integer decimal). found=false is
// authorization_invalid; a read failure is reported by the caller as
// gate_read_failed. "" means the grant passes.
func (g GrantRow) Evaluate(found bool, callerID, chainID int64, asset, recipient string, amount *big.Int) RefusalClass {
	if !found {
		return ClassAuthorizationInvalid
	}
	if g.State == "revoked" {
		return ClassAuthorizationRevoked
	}
	if g.State != "active" {
		return ClassAuthorizationInvalid
	}
	if !g.Unexpired {
		return ClassAuthorizationExpired
	}
	if g.CallerID != callerID || g.ChainID != chainID {
		return ClassAuthorizationInvalid
	}
	if !strings.EqualFold(g.Asset, asset) || !strings.EqualFold(g.Recipient, recipient) {
		return ClassAuthorizationInvalid
	}
	if g.Amount == nil || amount == nil || g.Amount.Cmp(amount) != 0 {
		return ClassAuthorizationInvalid
	}
	return ""
}

// ScopeRow is the observed PB carrier row. Present=false means no scope row:
// authorization_unverifiable (PB Q-B). Fee caps and AllowsFeeReplacement are
// read but not adjudicated by 011 (010 constructs the replacement content).
type ScopeRow struct {
	Present              bool
	AuthorizationID      string
	IntentID             string
	RequestID            string
	Sender               string
	FeeMaxTotal          int64
	FeeMaxPerGas         int64
	FeeMaxPriority       int64
	AllowsFeeReplacement bool
	AuthorizationVersion int64
}

// ReadScope reads the PB carrier row FOR SHARE in the grant read sequence.
func ReadScope(ctx context.Context, tx pgx.Tx, authorizationID string) (ScopeRow, error) {
	var s ScopeRow
	err := tx.QueryRow(ctx, ScopeReadSQL, authorizationID).Scan(
		&s.AuthorizationID, &s.IntentID, &s.RequestID, &s.Sender,
		&s.FeeMaxTotal, &s.FeeMaxPerGas, &s.FeeMaxPriority,
		&s.AllowsFeeReplacement, &s.AuthorizationVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, nil
	}
	if err != nil {
		return s, gateReadErr("007 scope row", err)
	}
	s.Present = true
	return s, nil
}

// EvaluateScope applies the PB fail-closed rules: absent scope is
// authorization_unverifiable; identity/request/sender mismatch is
// scope_mismatch; a version change after admission is authorization_changed.
// "" means the scope covers the request.
func (s ScopeRow) Evaluate(authorizationID, intentID, requestID, sender string, authorizationVersion int64) RefusalClass {
	if !s.Present {
		return ClassAuthorizationUnverifiable
	}
	if s.AuthorizationID != authorizationID {
		return ClassScopeMismatch
	}
	if s.IntentID != intentID || s.RequestID != requestID || !strings.EqualFold(s.Sender, sender) {
		return ClassScopeMismatch
	}
	if s.AuthorizationVersion != authorizationVersion {
		return ClassAuthorizationChanged
	}
	return ""
}

// ReadRegistryState reads the 008 registry state for (chain_id, sender) FOR
// SHARE; found=false when no row exists. Only "active" admits.
func ReadRegistryState(ctx context.Context, tx pgx.Tx, chainID int64, sender string) (string, bool, error) {
	var state string
	err := tx.QueryRow(ctx, RegistryShareSQL, chainID, sender).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, gateReadErr("008 registry row", err)
	}
	return state, true, nil
}

// RegistryRefusal maps an observed registry state to its refusal class; ""
// admits. A missing/disabled row is sender_unregistered.
func RegistryRefusal(state string, found bool) RefusalClass {
	if !found || state != "active" {
		return ClassSenderUnregistered
	}
	return ""
}
