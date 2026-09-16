// gates.go owns the gate reads (T011; FR-17/FR-18/FR-19, contracts/gates.md
// §§1–3, research R6/R7/R11): the 006 pause/recovery/version one-statement
// read, the 008 BindingReader five-class adapter with hold/recovery mapping,
// the 007 grant FOR SHARE read with field equality and the authz:v1
// fingerprint surrogate, and the joint lock order (scope row → 006 tables →
// 007 row → own rows). This task covers mapping/unit parts; DB-backed races
// are deferred to US6. Reads are SELECT-only plus the gate-table SHARE lock;
// 009 never writes upstream state.
package signer

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

// GateLockSQL acquires the shared coordination lock over the 006 gate tables
// before any gate read, so the observation is linearly ordered against
// pause/recovery writers (gates.md common rule). Lock acquisition only; no
// data modification.
const GateLockSQL = `LOCK TABLE indexer_pause, log_pause, deposit_pause, reorg_recovery, reorg_recovery_events IN SHARE MODE`

// GateReadSQL is the one-statement 006 snapshot: three pause flags plus the
// active recovery identity/phase/seq and the events max for the deployment
// chain. One statement so no two reads can straddle a committing write; pure
// SELECT (gates.md §1).
const GateReadSQL = `SELECT
  EXISTS (SELECT 1 FROM indexer_pause) AS indexer_paused,
  EXISTS (SELECT 1 FROM log_pause) AS log_paused,
  EXISTS (SELECT 1 FROM deposit_pause) AS deposit_paused,
  (SELECT id FROM reorg_recovery LIMIT 1) AS recovery_id,
  (SELECT phase FROM reorg_recovery LIMIT 1) AS recovery_phase,
  (SELECT seq FROM reorg_recovery LIMIT 1) AS recovery_seq,
  (SELECT MAX(seq) FROM reorg_recovery_events) AS events_max`

// ScopeShareSQL takes the 008 scope row FOR SHARE before any gate read, so a
// racing 008 pause/registry/release write blocks until 009 commits
// (gates.md §3 bilateral rule). The scope row key is supplied by the caller;
// this is the lock statement shape, not a full query.
const ScopeShareSQL = `SELECT 1 FROM nonce_scope_state WHERE chain_id = $1 AND sender = $2 FOR SHARE`

// GrantReadSQL reads the 007 grant row FOR SHARE inside the sign/delivery
// transaction (gates.md §2). A revoke committing before the share lock is
// observed; one racing it waits until 009 commits.
const GrantReadSQL = `SELECT caller_id, chain_id, asset, recipient, amount::text, state, expires_at
  FROM withdrawal_authorizations WHERE authorization_id = $1 FOR SHARE`

// BindingResult is the 009-side binding classification (gates.md §3). Only
// BindingMatches admits signing; every other class refuses with its mapped
// refusal class and records the read basis.
type BindingResult int

const (
	// BindingMatches: exists, no holds, recovery none (registry enabled).
	BindingMatches BindingResult = iota
	// BindingAbsent: 008 has no binding.
	BindingAbsent
	// BindingConflict: 008 reports a binding mismatch.
	BindingConflict
	// BindingPaused: paused, reconciling, hold-annotated, or registry disabled.
	BindingPaused
	// BindingTerminal: consumed/released; audit-only, never signable.
	BindingTerminal
	// BindingReadFailed: read failed or indeterminate.
	BindingReadFailed
)

// BindingReader reads the 008 binding for one request identity. The concrete
// adapter lands against 008's provider contract (D3); until then callers use
// contract-shape doubles retired at T028.
type BindingReader interface {
	ReadBinding(ctx context.Context, intentID, attemptID string) (BindingResult, error)
}

// BindingRefusal maps a BindingResult to its admission outcome: "" admits,
// otherwise the refusal class. A reader error fails closed to
// binding_read_failed (gates.md §3 admission rule).
func BindingRefusal(res BindingResult, err error) RefusalClass {
	if err != nil {
		return ClassBindingReadFailed
	}
	switch res {
	case BindingMatches:
		return ""
	case BindingAbsent:
		return ClassBindingAbsent
	case BindingConflict:
		return ClassBindingConflict
	case BindingPaused:
		return ClassBindingPaused
	case BindingTerminal:
		return ClassBindingTerminal
	case BindingReadFailed:
		return ClassBindingReadFailed
	default:
		return ClassBindingReadFailed
	}
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

// Evaluate applies the §1 refusal table against the request's built-under
// version. "" means the gate passes; otherwise the refusal class.
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

// AuthzGrant is the observed 007 grant row (gates.md §2).
type AuthzGrant struct {
	CallerID  int64
	ChainID   int64
	Asset     string
	Recipient string
	Amount    *big.Int
	State     string
	ExpiresAt *time.Time
}

// AuthzFingerprintDomain tags the fingerprint from every other hash use.
const AuthzFingerprintDomain = "authz:v1"

// Fingerprint is the read-time authz:v1 surrogate over the observed fields.
// It is persisted for delivery equality and never presented as an
// authorization version (gates.md §2).
func (g *AuthzGrant) Fingerprint() string {
	expires := "NULL"
	if g.ExpiresAt != nil {
		expires = g.ExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	amount := ""
	if g.Amount != nil {
		amount = g.Amount.String()
	}
	raw := strings.Join([]string{
		fmt.Sprintf("%d", g.CallerID), fmt.Sprintf("%d", g.ChainID),
		strings.ToLower(g.Asset), strings.ToLower(g.Recipient),
		amount, g.State, expires,
	}, "\x00")
	return AuthzFingerprintDomain + ":" + fmt.Sprintf("%x", crypto.Keccak256([]byte(AuthzFingerprintDomain+"\x00"+raw)))
}

// EvaluateGrant checks the grant against the request: active state, database
// clock expiry, and field equality with the bound values. Absent is reported
// by passing found=false (→ authorization_invalid); a read failure is
// reported by the caller as gate_read_failed. "" means the grant passes.
func EvaluateGrant(found bool, grant *AuthzGrant, callerID int64, req *Request, now time.Time) RefusalClass {
	if !found {
		return ClassAuthorizationInvalid
	}
	if grant.State == "revoked" {
		return ClassAuthorizationRevoked
	}
	if grant.State != "active" {
		return ClassAuthorizationInvalid
	}
	if grant.ExpiresAt != nil && !grant.ExpiresAt.After(now) {
		return ClassAuthorizationExpired
	}
	if grant.CallerID != callerID {
		return ClassAuthorizationInvalid
	}
	if grant.ChainID != int64(req.ChainID) {
		return ClassAuthorizationInvalid
	}
	if !strings.EqualFold(grant.Asset, req.Asset) || !strings.EqualFold(grant.Recipient, req.Recipient) {
		return ClassAuthorizationInvalid
	}
	want, err := decimalBig("amount", req.Amount)
	if err != nil || grant.Amount == nil || grant.Amount.Cmp(want) != 0 {
		return ClassAuthorizationInvalid
	}
	return ""
}
