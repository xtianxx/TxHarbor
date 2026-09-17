// lifecycle_live.go owns 010's in-process implementation of 011's
// LifecycleAdvancer/LifecycleReader boundary (contracts/lifecycle.md §2):
// 011 passes identity, fencing, action and step_id only; this adapter composes
// 010's own T1/T2/T3 (PrepareAttempt -> Send, which re-verifies the claim,
// 006 gates, grant/scope and the 008 observation inside the send region and
// signs through the 009 path) and reads 010's attempt/signing/freeze facts for
// the reconcile side. It never allocates a nonce, never creates an intent and
// never bypasses a gate.
//
// Dependency direction: this package imports internal/execution for the
// 011-owned boundary interfaces only; the reverse import is forbidden and is
// pinned by internal/execution/boundary_test.go. The 008 binding read port for
// the untagged build lives in internal/jointwire because internal/execution's
// default import graph must stay free of 008's RPC-bearing read package.
//
// Idempotency (lifecycle.md §4, required of 010): 010 converges on
// (intent_id, step_id). The attempt identity is derived deterministically from
// the 011-preallocated step_id and the signing-request identity is the 007
// request id (the only request identity 009 can compare against the PB scope
// carrier), so a retry with the same step re-observes the recorded attempt and
// never creates a second one; PrepareAttempt converges by identity, the send
// gates lock the attempt row, and tx_attempt_signings.tx_hash is UNIQUE, so a
// duplicate dispatch is structurally impossible even under two concurrent
// Advance calls.
package txlifecycle

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/execution"
)

// advanceGasLimit is 010's v1 construction shape for the ERC-20 transfer
// (send-gate.md: the attempt carries the content the gates measure).
// ponytail: a fixed limit and fees signed at the grant caps — no fee oracle
// exists in 010; add one when fee policy demands a market-derived candidate.
const advanceGasLimit = "100000"

// attemptIDForStep derives 010's attempt identity from the 011-preallocated
// step id. It stays inside the 1..128 printable domain.
func attemptIDForStep(stepID string) string { return "att-" + stepID }

// LifecycleLive is the 010-owned adapter over one Store. Advance drives 010;
// Read returns 010's durable authority facts. It holds no cache and no
// decision state: every call reads current rows.
type LifecycleLive struct {
	pool  *pgxpool.Pool
	store *Store
}

func NewLifecycleLive(pool *pgxpool.Pool, store *Store) (*LifecycleLive, error) {
	if pool == nil || store == nil {
		return nil, fmt.Errorf("010 lifecycle adapter needs a pool and a store")
	}
	return &LifecycleLive{pool: pool, store: store}, nil
}

var (
	_ execution.LifecycleAdvancer = (*LifecycleLive)(nil)
	_ execution.LifecycleReader   = (*LifecycleLive)(nil)
)

// Advance drives one send-class action. first_broadcast builds and persists
// the attempt under the current gates; replay resends the anchor's persisted
// bytes under the same identity; replace is refused fail-closed (no fee policy
// exists to construct a differing replacement — see replace()).
func (l *LifecycleLive) Advance(ctx context.Context, req execution.AdvanceRequest) (execution.AdvanceOutcome, error) {
	switch req.Action {
	case execution.ActionFirstBroadcast:
		return l.firstBroadcast(ctx, req)
	case execution.ActionReplay:
		return l.replay(ctx, req)
	case execution.ActionReplace:
		return l.replace(req)
	default:
		return execution.AdvanceOutcome{}, fmt.Errorf("lifecycle advance: unknown action %q", req.Action)
	}
}

// firstBroadcast converges on (intent_id, step_id): a retry with the same step
// returns the recorded attempt/outcome; only a step whose attempt does not
// exist yet constructs and persists new content.
func (l *LifecycleLive) firstBroadcast(ctx context.Context, req execution.AdvanceRequest) (execution.AdvanceOutcome, error) {
	if req.StepID == "" {
		return execution.AdvanceOutcome{}, fmt.Errorf("lifecycle advance: first_broadcast needs a step id")
	}
	attemptID := attemptIDForStep(req.StepID)
	att, err := l.store.AttemptByID(ctx, attemptID)
	if err == nil {
		// A crash between T1/T2 and the dispatch is repaired by the same
		// identity: Send signs the prepared envelope or dispatches the
		// persisted bytes; any other state is already a recorded outcome.
		if att.State == "prepared" || att.State == "signed" {
			return l.dispatch(ctx, req, attemptID, SendInitial)
		}
		return outcomeForAttempt(att), nil
	}
	var ref *RefusalError
	if !errors.As(err, &ref) || ref.Class != ClassAttemptNotFound {
		return execution.AdvanceOutcome{}, err
	}

	content, err := l.constructFirst(ctx, req, attemptID)
	if err != nil {
		return l.refusalOutcome(ctx, attemptID, err, SendResult{})
	}
	if _, err := l.store.PrepareAttempt(ctx, content); err != nil {
		return l.refusalOutcome(ctx, attemptID, err, SendResult{})
	}
	return l.dispatch(ctx, req, attemptID, SendInitial)
}

// replay resends the anchor attempt's persisted bytes. 011 asserts the anchor
// and, when it has one, the expected hash; a wrong anchor is refused rather
// than silently replayed. No new attempt or signing identity is created
// (010 FR-04).
func (l *LifecycleLive) replay(ctx context.Context, req execution.AdvanceRequest) (execution.AdvanceOutcome, error) {
	if req.AnchorAttemptID == "" {
		return execution.AdvanceOutcome{Class: execution.OutcomeRefusedBasis, Basis: "replay requires an anchor attempt"}, nil
	}
	att, err := l.store.AttemptByID(ctx, req.AnchorAttemptID)
	if err != nil {
		return l.refusalOutcome(ctx, req.AnchorAttemptID, err, SendResult{})
	}
	if att.IntentID != req.IntentID {
		return execution.AdvanceOutcome{AttemptID: att.AttemptID, Class: execution.OutcomeRefusedBasis,
			Basis: "anchor attempt belongs to another intent"}, nil
	}
	if req.ExpectedTxHash != "" && att.TxHash != "" && !equalHex(att.TxHash, req.ExpectedTxHash) {
		return execution.AdvanceOutcome{AttemptID: att.AttemptID, Class: execution.OutcomeRefusedBasis,
			Basis: "anchor tx hash differs from the expected hash"}, nil
	}
	return l.dispatch(ctx, req, att.AttemptID, SendReplay)
}

// replace is refused fail-closed: a replacement must carry fee dimensions that
// differ from the anchor and stay inside the PB scope caps, and 010 has no
// fee-construction policy (or oracle) to derive them. The refusal is recorded
// as a basis-bearing converged step, never a silent no-op; wiring replace needs
// a construction-policy ruling first.
func (l *LifecycleLive) replace(req execution.AdvanceRequest) (execution.AdvanceOutcome, error) {
	return execution.AdvanceOutcome{Class: execution.OutcomeRefusedBasis,
		Basis: "replace construction has no 010 fee policy; refused before any write"}, nil
}

// dispatch runs T2/T3 through Send under the presented claim and maps the
// recorded result onto the closed 011 outcome classes. No external call runs
// here; Send owns the region and its gates.
func (l *LifecycleLive) dispatch(ctx context.Context, req execution.AdvanceRequest, attemptID string, kind SendKind) (execution.AdvanceOutcome, error) {
	res, err := l.store.Send(ctx, &SendRequest{
		AttemptID: attemptID, Kind: kind,
		Claim: ClaimRef{IntentID: req.IntentID, WorkerID: req.OwnerID, LeaseVersion: req.LeaseVersion},
	})
	if err != nil {
		return l.refusalOutcome(ctx, attemptID, err, res)
	}
	out := execution.AdvanceOutcome{AttemptID: attemptID, TxHash: res.TxHash}
	switch res.Outcome {
	case "accepted":
		out.Class = execution.OutcomeSent
		out.Basis = "dispatch accepted"
	case "unknown":
		out.Class = execution.OutcomePendingUnknown
		out.Basis = "dispatch unknown rpc_class=" + res.RPCClass
	case "rejected":
		// A node rejection of these bytes is a known dispatch result but never
		// a payment verdict: reconcile before any re-dispatch (R-010-05).
		out.Class = execution.OutcomeReconcileRequired
		out.Basis = "dispatch rejected rpc_class=" + res.RPCClass + "; reconcile before re-dispatch"
	default:
		out.Class = execution.OutcomeReconcileRequired
		out.Basis = "unclassified send outcome " + res.Outcome
	}
	if att, aerr := l.store.AttemptByID(ctx, attemptID); aerr == nil {
		out.RevisionVersion = att.RevisionSeq
		if out.TxHash == "" {
			out.TxHash = att.TxHash
		}
	}
	return out, nil
}

// refusalOutcome maps a 010/009 refusal onto a closed class without running an
// external call. A non-refusal error is returned as an error so the driver's
// bounded same-step retry owns it.
func (l *LifecycleLive) refusalOutcome(ctx context.Context, attemptID string, err error, res SendResult) (execution.AdvanceOutcome, error) {
	var ref *RefusalError
	if errors.As(err, &ref) {
		out := execution.AdvanceOutcome{AttemptID: attemptID, TxHash: res.TxHash, Basis: string(ref.Class) + ": " + ref.Basis}
		if class, ok := advanceClassForRefusal(ref.Class); ok {
			out.Class = class
			return out, nil
		}
		// already_accepted: the durable attempt state decides sent vs unknown.
		att, aerr := l.store.AttemptByID(ctx, attemptID)
		if aerr != nil {
			return execution.AdvanceOutcome{}, aerr
		}
		return outcomeForAttempt(att), nil
	}
	var se *SignerError
	if errors.As(err, &se) {
		out := execution.AdvanceOutcome{AttemptID: attemptID, Basis: string(se.Class) + ": " + se.Basis}
		if se.DeliveryUnknown {
			out.Class = execution.OutcomeUnavailable
		} else {
			out.Class = execution.OutcomeRefusedBasis
		}
		return out, nil
	}
	return execution.AdvanceOutcome{}, err
}

// advanceClassForRefusal maps a 010 refusal class onto the 011 outcome class.
// ok=false is reserved for already_accepted, whose class depends on the
// attempt's durable state. Classes absent from the gate/retry sets fall to
// refused_basis (the caller needs a fresh authorization/identity or must
// inspect state), never to a success class.
func advanceClassForRefusal(class RefusalClass) (string, bool) {
	switch class {
	case ClassCoordinationUnavailable, ClassGateReadFailed, ClassSendStale, ClassAttemptNotSendable:
		return execution.OutcomeUnavailable, true
	case ClassAlreadyAccepted:
		return "", false
	case ClassClaimAbsent, ClassClaimVersionMismatch, ClassClaimExpired, ClassClaimRevoked,
		ClassPausePresent, ClassRecoveryActive, ClassIntentFrozen,
		ClassBindingAbsent, ClassBindingConflict, ClassBindingPaused,
		ClassBindingTerminal, ClassBindingReadFailed:
		return execution.OutcomeRefusedGate, true
	default:
		return execution.OutcomeRefusedBasis, true
	}
}

// outcomeForAttempt is the recorded-outcome mapping for an existing attempt:
// sent/effective/confirmed are a sent fact; unknown needs reconcile first;
// orphaned/ineffective are unresolved chain facts (reconcile, never failure);
// replaced is a known no-send for this attempt.
func outcomeForAttempt(att *Attempt) execution.AdvanceOutcome {
	out := execution.AdvanceOutcome{
		AttemptID: att.AttemptID, TxHash: att.TxHash,
		RevisionVersion: att.RevisionSeq, Basis: "010 attempt state=" + att.State,
	}
	switch att.State {
	case "sent", "effective", "confirmed":
		out.Class = execution.OutcomeSent
	case "unknown":
		out.Class = execution.OutcomePendingUnknown
	case "replaced":
		out.Class = execution.OutcomeRefusedBasis
	default:
		out.Class = execution.OutcomeReconcileRequired
	}
	return out
}

// constructFirst builds the T1 content from 010's durable facts: the intent
// (011-owned identity/linkage), the 007 request economics, the PB scope caps
// and the 008 binding. The send region re-verifies every one of them under
// its locks; this read is construction input, never an authorization.
func (l *LifecycleLive) constructFirst(ctx context.Context, req execution.AdvanceRequest, attemptID string) (*PrepareRequest, error) {
	if req.RecoveryVersion < 0 {
		return nil, Refuse(ClassAttemptConflict, "recovery_version", "negative recovery version")
	}
	intent, found, err := execution.ReadIntent(ctx, l.pool, req.IntentID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("lifecycle advance: intent %s is absent", req.IntentID)
	}
	if req.RequestID != "" && intent.RequestID != req.RequestID {
		return nil, Refuse(ClassAttemptConflict, "request_id", "advance request_id differs from the persisted intent")
	}
	econ, err := l.requestEconomics(ctx, intent.RequestID)
	if err != nil {
		return nil, err
	}
	scope, err := l.scopeCaps(ctx, intent.AuthorizationID)
	if err != nil {
		return nil, err
	}
	binding, err := l.bindingForIntent(ctx, intent.IntentID)
	if err != nil {
		return nil, err
	}
	maxFee, priority, err := constructFees(advanceGasLimit, scope.feeMaxTotal, scope.feeMaxPerGas, scope.feeMaxPriority)
	if err != nil {
		return nil, err
	}
	return &PrepareRequest{
		AttemptID: attemptID, SigningRequestID: intent.RequestID,
		IntentID: intent.IntentID, BindingRef: binding.bindingID,
		AuthorizationID: intent.AuthorizationID, AuthorizationVersion: scope.version,
		RecoveryVersion: uint64(req.RecoveryVersion), ChainID: uint64(intent.ChainID),
		Sender: intent.Sender, Nonce: binding.nonce, TxType: TxTypeDynamicFee,
		GasLimit: advanceGasLimit, MaxFeePerGas: maxFee, MaxPriorityFeePerGas: priority,
		Asset: econ.asset, Recipient: econ.recipient, Amount: econ.amount,
	}, nil
}

// advanceEconomics is the 007 request's economic reference set.
type advanceEconomics struct {
	chainID   int64
	asset     string
	recipient string
	amount    string
}

// requestEconomics reads the 007 request's economic fields (the grant equality
// reference).
func (l *LifecycleLive) requestEconomics(ctx context.Context, requestID string) (advanceEconomics, error) {
	var e advanceEconomics
	err := l.pool.QueryRow(ctx,
		`SELECT chain_id, asset, recipient, amount::text FROM withdrawal_requests WHERE request_id = $1`,
		requestID).Scan(&e.chainID, &e.asset, &e.recipient, &e.amount)
	if errors.Is(err, pgx.ErrNoRows) {
		return e, Refuse(ClassGateReadFailed, "request_id", "007 request row is absent")
	}
	if err != nil {
		return e, err
	}
	return e, nil
}

// advanceScopeCaps is the PB scope's version and fee ceilings.
type advanceScopeCaps struct {
	version        int64
	feeMaxTotal    int64
	feeMaxPerGas   int64
	feeMaxPriority int64
}

// scopeCaps reads the PB fee caps and version; a grant without a scope cannot
// carry content (the send gate refuses it the same way).
func (l *LifecycleLive) scopeCaps(ctx context.Context, authorizationID string) (advanceScopeCaps, error) {
	var s advanceScopeCaps
	err := l.pool.QueryRow(ctx,
		`SELECT authorization_version, fee_max_total, fee_max_per_gas, fee_max_priority
		   FROM withdrawal_authorization_scopes WHERE authorization_id = $1`,
		authorizationID).Scan(&s.version, &s.feeMaxTotal, &s.feeMaxPerGas, &s.feeMaxPriority)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, Refuse(ClassAuthorizationUnverifiable, "authorization_scope", "grant has no PB scope row")
	}
	if err != nil {
		return s, err
	}
	return s, nil
}

// advanceBinding is the read-only 008 binding reference 010 consumes (OC-3).
type advanceBinding struct {
	bindingID string
	nonce     string
	state     string
}

// bindingForIntent reads the intent's 008 binding; 010 consumes it read-only
// (OC-3).
func (l *LifecycleLive) bindingForIntent(ctx context.Context, intentID string) (advanceBinding, error) {
	var b advanceBinding
	err := l.pool.QueryRow(ctx,
		`SELECT binding_id, nonce::text, state FROM nonce_bindings WHERE intent_id = $1`,
		intentID).Scan(&b.bindingID, &b.nonce, &b.state)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, Refuse(ClassBindingAbsent, "intent_id", "no 008 binding for intent")
	}
	if err != nil {
		return b, err
	}
	switch b.state {
	case "allocated", "in_flight":
	default:
		return b, Refuse(ClassBindingTerminal, "state", "binding state="+b.state)
	}
	return b, nil
}

// constructFees derives the dynamic-fee candidate from the scope caps with
// exact big.Int arithmetic: gas_limit x max_fee_per_gas <= fee_max_total,
// max_fee_per_gas <= fee_max_per_gas, priority <= fee_max_priority.
func constructFees(gasLimit string, capTotal, capPerGas, capPriority int64) (string, string, error) {
	gas, ok := new(big.Int).SetString(gasLimit, 10)
	if !ok || gas.Sign() <= 0 {
		return "", "", Refuse(ClassFeeScopeExceeded, "gas_limit", "unparseable gas limit")
	}
	perGas := big.NewInt(capPerGas)
	if capTotal > 0 {
		if byTotal := new(big.Int).Div(big.NewInt(capTotal), gas); byTotal.Cmp(perGas) < 0 {
			perGas = byTotal
		}
	}
	if perGas.Sign() <= 0 {
		return "", "", Refuse(ClassFeeScopeExceeded, "fee_max_total", "no fee headroom under the scope caps")
	}
	priority := big.NewInt(capPriority)
	if priority.Cmp(perGas) > 0 {
		priority = new(big.Int).Set(perGas)
	}
	return perGas.String(), priority.String(), nil
}

// equalHex compares two hashes case-insensitively; empty never equals a hash.
func equalHex(a, b string) bool {
	if a == "" || b == "" {
		return a == b
	}
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// lifecycleAttemptsSQL reads the intent's attempts with the persisted hash, in
// creation order (the reconcile facts of contracts/lifecycle.md §5).
const lifecycleAttemptsSQL = `SELECT a.attempt_id, a.state, a.revision_seq, COALESCE(s.tx_hash, '')
  FROM tx_attempts a LEFT JOIN tx_attempt_signings s ON s.attempt_id = a.attempt_id
 WHERE a.intent_id = $1 ORDER BY a.created_at, a.attempt_id`

const lifecycleFreezeSQL = `SELECT cause FROM tx_intent_freezes
 WHERE intent_id = $1 AND released_at IS NULL`

// Read returns the honest 010 authority facts for one intent: every attempt's
// identity/state/revision, the current (latest) attempt, and — when the intent
// is frozen — the unreleased freeze condition. A failed read is an error; the
// caller retries and marks its projection possibly-stale, never inventing
// facts from an absent read.
func (l *LifecycleLive) Read(ctx context.Context, intentID string) (execution.LifecycleFacts, error) {
	rows, err := l.pool.Query(ctx, lifecycleAttemptsSQL, intentID)
	if err != nil {
		return execution.LifecycleFacts{}, err
	}
	defer rows.Close()
	facts := execution.LifecycleFacts{Basis: "010 authority read"}
	for rows.Next() {
		var id, state, hash string
		var rev int64
		if err := rows.Scan(&id, &state, &rev, &hash); err != nil {
			return execution.LifecycleFacts{}, err
		}
		facts.Attempts = append(facts.Attempts, execution.AttemptRef{AttemptID: id, State: state})
		facts.CurrentAttemptID = id
		if rev > facts.RevisionVersion {
			facts.RevisionVersion = rev
		}
		if state == "unknown" {
			facts.Unknown = &execution.UnknownRef{
				AttemptID: id, TxHash: hash,
				RecoveryCondition: "probe the same tx_hash; reconcile before any re-dispatch",
			}
		}
	}
	if err := rows.Err(); err != nil {
		return execution.LifecycleFacts{}, err
	}

	// An unreleased protection-loss freeze is surfaced as a freeze condition
	// so the 011 reconciler records its own marker; the controlled 010-side
	// release removes the condition (released_at IS NULL).
	var cause string
	err = l.pool.QueryRow(ctx, lifecycleFreezeSQL, intentID).Scan(&cause)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return execution.LifecycleFacts{}, err
	default:
		if facts.Unknown == nil {
			facts.Unknown = &execution.UnknownRef{AttemptID: facts.CurrentAttemptID,
				RecoveryCondition: "freeze:" + execution.FreezeLockLoss}
		} else {
			facts.Unknown.RecoveryCondition = "freeze:" + execution.FreezeLockLoss
		}
	}
	return facts, nil
}
