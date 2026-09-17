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
// chain ($1 = chain_id). One statement so no two reads can straddle a
// committing write; pure SELECT (gates.md §1). Column names mirror 006's
// captureRecoveryVersion: reorg_recovery.recovery_seq is the active version,
// reorg_recovery_events.recovery_seq its high-water when no active row exists.
const GateReadSQL = `SELECT
  EXISTS (SELECT 1 FROM indexer_pause) AS indexer_paused,
  EXISTS (SELECT 1 FROM log_pause) AS log_paused,
  EXISTS (SELECT 1 FROM deposit_pause) AS deposit_paused,
  (SELECT recovery_id FROM reorg_recovery WHERE chain_id = $1) AS recovery_id,
  (SELECT phase FROM reorg_recovery WHERE chain_id = $1) AS recovery_phase,
  (SELECT recovery_seq FROM reorg_recovery WHERE chain_id = $1) AS recovery_seq,
  (SELECT MAX(recovery_seq) FROM reorg_recovery_events WHERE chain_id = $1) AS events_max`

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

// GrantScopeReadSQL reads the 1:1 PB carrier row for the same authorization_id
// inside the grant read's FOR SHARE sequence (gates.md §2; PB data-model:
// "grant + scope FOR SHARE in one sequence"). It introduces no new lock
// object: the scope read rides the grant-row coordination (PB writers take the
// grant row FOR UPDATE before touching the scope), so a pause/revoke/re-supply
// committing before the share lock is observed and one racing it waits for
// this transaction. Row absence is pre-extension stock, not an error; pure
// SELECT, never a write.
const GrantScopeReadSQL = `SELECT authorization_id, intent_id, request_id, sender,
  fee_max_total, fee_max_per_gas, fee_max_priority, allows_fee_replacement, authorization_version
  FROM withdrawal_authorization_scopes WHERE authorization_id = $1 FOR SHARE`

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

// GrantScope is the observed PB carrier row
// (withdrawal_authorization_scopes, 1:1 with the 007 grant; research R11,
// PB-C2). Present=false means no row: a pre-extension stock grant, refused
// per request (Q-B ruling). The remaining fields are the consumption basis the
// 007 grant row cannot express: identity linkage, the sender anchor, the fee
// bounds, and the fee-replacement purpose token. `attested_by` is deliberately
// absent — it is issuance-side identity with no 009-comparable principal, so
// it is never a refusal basis by itself (H1; Q-A ruling).
type GrantScope struct {
	// Present reports whether a scope row was found for the grant.
	Present bool
	// AuthorizationID is the carrier's grant key; it must equal the
	// request's authorization_id.
	AuthorizationID string
	// IntentID links the carrier to the request's intent_id; RequestID links
	// it to the request identity (the request's signing_request_id), the only
	// request id 009 can compare against.
	IntentID  string
	RequestID string
	// Sender is the carrier's mandatory identity anchor (stored lowercase).
	Sender string
	// FeeMaxTotal, FeeMaxPerGas and FeeMaxPriority are native-coin
	// smallest-unit caps (PB-C2). An all-zero triple declares no fee
	// constraint; any capped dimension requires its applicable caps.
	FeeMaxTotal    int64
	FeeMaxPerGas   int64
	FeeMaxPriority int64
	// AllowsFeeReplacement is the explicit purpose token the fee-replacement
	// path is gated on (consumed by H3/T038, carried here).
	AllowsFeeReplacement bool
	// AuthorizationVersion is the PB scope version observed at read time
	// (PB carrier semantics: starts at 1, bumped on non-active re-supply).
	// T037 snapshots it at submit and re-checks equality at delivery; only
	// meaningful when Present.
	AuthorizationVersion int64
}

// EvaluateGrantScope applies the R11 fail-closed rule and the PB consumption
// checks once the 007 row checks pass (gates.md §2; H1; PB-C2). A missing
// carrier refuses authorization_unverifiable; a carrier that does not cover
// this request refuses authorization_invalid. Pure comparison: no write, no
// inference from history or caller claims. "" means the carrier is present and
// covers the request.
func EvaluateGrantScope(scope GrantScope, req *Request) RefusalClass {
	if !scope.Present {
		return ClassAuthorizationUnverifiable
	}
	if scope.AuthorizationID != req.AuthorizationID {
		return ClassAuthorizationInvalid
	}
	if !strings.EqualFold(scope.Sender, req.Sender) {
		return ClassAuthorizationInvalid
	}
	if scope.IntentID != req.IntentID || scope.RequestID != req.SigningRequestID {
		return ClassAuthorizationInvalid
	}
	return feeScopeRefusal(scope, req)
}

// EvaluateGrantReuse applies OC-5's conditional reuse permission to a
// fee-replacement request that names its anchor's grant (H3/T038; PB-FR-05,
// research R7/R11, gates.md §2 condition 4). "" admits the reuse under the
// anchor's grant; otherwise the refusal class, whose instruction is that a
// fresh authorization (PB re-issue) is required. Admission needs the carrier
// present, the explicit fee-replacement purpose token, and the replacement's
// fee triple within the carrier's caps (the same T036 fee rules); an
// unverifiable or out-of-range reuse never shares the anchor's grant — it must
// not become a second signable object on it.
func EvaluateGrantReuse(scope GrantScope, req *Request) RefusalClass {
	if !scope.Present || !scope.AllowsFeeReplacement {
		return ClassAuthorizationInvalid
	}
	return feeScopeRefusal(scope, req)
}

// feeScopeRefusal enforces the PB-C2 fee bounds on the request's fee triple:
// total = gas_limit × per-gas price, per-gas price (max_fee_per_gas or
// gas_price), and the EIP-1559 priority tip (legacy has none). An all-zero
// carrier triple declares no fee constraint; once the carrier caps any
// dimension, a cap the request's dimension needs must be present — a missing
// applicable cap is refused, never read as "unlimited" — and every present
// dimension must stay within its cap with priority <= per-gas.
func feeScopeRefusal(scope GrantScope, req *Request) RefusalClass {
	if scope.FeeMaxTotal == 0 && scope.FeeMaxPerGas == 0 && scope.FeeMaxPriority == 0 {
		return ""
	}
	total, perGas, priority, ok := requestFeeTriple(req)
	if !ok || priority.Cmp(perGas) > 0 || scope.FeeMaxPriority > scope.FeeMaxPerGas {
		return ClassAuthorizationInvalid
	}
	if scope.FeeMaxTotal == 0 || scope.FeeMaxPerGas == 0 {
		return ClassAuthorizationInvalid
	}
	if total.Cmp(big.NewInt(scope.FeeMaxTotal)) > 0 || perGas.Cmp(big.NewInt(scope.FeeMaxPerGas)) > 0 {
		return ClassAuthorizationInvalid
	}
	if priority.Sign() > 0 {
		if scope.FeeMaxPriority == 0 || priority.Cmp(big.NewInt(scope.FeeMaxPriority)) > 0 {
			return ClassAuthorizationInvalid
		}
	}
	return ""
}

// requestFeeTriple renders the PB-C2 request fee triple, failing closed on an
// unreadable or illegal shape (Validate runs before this on the submit path).
func requestFeeTriple(req *Request) (*big.Int, *big.Int, *big.Int, bool) {
	gasLimit, err := decimalBig("gas_limit", req.GasLimit)
	if err != nil || gasLimit.Sign() <= 0 {
		return nil, nil, nil, false
	}
	switch req.TxType {
	case 0:
		price, err := decimalBig("gas_price", req.GasPrice)
		if err != nil || price.Sign() <= 0 {
			return nil, nil, nil, false
		}
		return new(big.Int).Mul(gasLimit, price), price, big.NewInt(0), true
	case 2:
		maxFee, err := decimalBig("max_fee_per_gas", req.MaxFeePerGas)
		if err != nil || maxFee.Sign() <= 0 {
			return nil, nil, nil, false
		}
		tip, err := decimalBig("max_priority_fee_per_gas", req.MaxPriorityFeePerGas)
		if err != nil || tip.Sign() <= 0 {
			return nil, nil, nil, false
		}
		return new(big.Int).Mul(gasLimit, maxFee), maxFee, tip, true
	default:
		return nil, nil, nil, false
	}
}
