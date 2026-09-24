package execution

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrAdvanceNoSendResult is the 010-side "confirmed no send happened this
// attempt (abort with no send result)". It is distinct from an unknown
// business effect and from a known RPC/on-chain result: the step is recorded
// refused (no external side effect) while the intent's business effect stays
// unknown pending reconcile. It is never a failure and never "nothing
// happened".
var ErrAdvanceNoSendResult = errors.New("010 advance aborted with no send result")

// ErrAdvanceUnavailable marks a transport/unavailable advance result: the step
// stays issued for a bounded retry. It never fabricates success, failure, or
// not-paid.
var ErrAdvanceUnavailable = errors.New("010 advance unavailable")

// StepRequest is one send-class step the driver may issue and advance.
type StepRequest struct {
	IntentID        string
	RequestID       string
	CallerID        int64
	OwnerID         string
	LeaseVersion    int64
	RecoveryVersion int64
	Action          AdvanceAction
	AnchorAttemptID string
	ExpectedTxHash  string

	// ReplacementFeeMaxPerGas / ReplacementFeeMaxPriorityFeePerGas are the
	// caller-supplied candidate fee dimensions for ActionReplace (exact
	// decimal strings); 010 validates them against the PB scope caps.
	ReplacementFeeMaxPerGas            string
	ReplacementFeeMaxPriorityFeePerGas string
	// ReplacementAuthorizationID optionally names the replacement's fresh PB
	// grant ("" = the anchor's grant); ReplacementSigningRequestID optionally
	// preallocates the replacement signing identity ("" = derived from StepID).
	ReplacementAuthorizationID  string
	ReplacementSigningRequestID string
}

// StepOutcome is the driver's recorded result.
type StepOutcome struct {
	StepID         string
	Refusal        RefusalClass
	Basis          string
	OutcomeClass   string
	FinalStepState string
	AttemptID      string
	TxHash         string
	Reconciling    bool
	Retryable      bool
}

// StepDriver issues a durable step before the 010 call and converges it after.
// No external call runs while holding a lock, and every converge is
// claim-fenced.
type StepDriver struct {
	Pool     *pgxpool.Pool
	Claims   *ClaimStore
	Binding  BindingReader
	Advancer LifecycleAdvancer
	// MaxRetries bounds same-step retries for transport/unavailable classes
	// (default 3). Exhaustion leaves the step issued for the next cycle.
	MaxRetries int
}

func (d *StepDriver) maxRetries() int {
	if d.MaxRetries > 0 {
		return d.MaxRetries
	}
	return 3
}

// IssueAndAdvance runs T-step-issue (persist before the call), the bounded
// external call, then T-step-converge (claim-fenced). A refusal before the
// call writes no step; the 010 call never runs while holding a lock.
func (d *StepDriver) IssueAndAdvance(ctx context.Context, req StepRequest) (StepOutcome, error) {
	ir, err := d.issue(ctx, req)
	if err != nil {
		return StepOutcome{}, err
	}
	if ir.stepID == "" {
		return StepOutcome{Refusal: ir.refusal, Basis: ir.basis}, nil
	}
	return d.advanceLoop(ctx, req, ir.stepID, ir.recoveryVersion)
}

// issueResult carries the T-step-issue outcome: a step id when issued, or the
// refusal class/basis with zero writes.
type issueResult struct {
	stepID          string
	recoveryVersion int64
	refusal         RefusalClass
	basis           string
}

// issue is the T-step-issue transaction.
func (d *StepDriver) issue(ctx context.Context, req StepRequest) (issueResult, error) {
	tx, err := BeginGateTx(ctx, d.Pool)
	if err != nil {
		return issueResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := LockGateTables(ctx, tx); err != nil {
		return issueResult{}, err
	}

	claimOK, err := verifyClaimTx(ctx, tx, req.IntentID, req.OwnerID, req.LeaseVersion)
	if err != nil {
		return issueResult{}, err
	}
	if !claimOK {
		return issueResult{refusal: ClassClaimNotCurrent, basis: "claim not current at step issue"}, nil
	}

	intent, found, err := ReadIntent(ctx, tx, req.IntentID)
	if err != nil {
		return issueResult{}, err
	}
	if !found {
		return issueResult{}, fmt.Errorf("%w: intent %s missing", ErrGateReadFailed, req.IntentID)
	}
	if intent.State == IntentCompleted || intent.State == IntentFailed {
		return issueResult{}, fmt.Errorf("%w: intent %s is terminal", ErrGateReadFailed, req.IntentID)
	}

	recovery, err := ReadRecoveryGate(ctx, tx, intent.ChainID)
	if err != nil {
		return issueResult{}, err
	}
	if class := recovery.SendRefusal(); class != "" {
		return issueResult{refusal: class, basis: "recovery gate"}, nil
	}

	// The 007 request row carries the business fields the grant must equal
	// (gates.md §2); the intent holds only identity.
	request, reqFound, err := ReadRequest(ctx, tx, intent.RequestID)
	if err != nil {
		return issueResult{}, err
	}
	if !reqFound {
		return issueResult{refusal: ClassGateReadFailed, basis: "007 request row missing"}, nil
	}
	if req.CallerID != 0 && request.CallerID != req.CallerID {
		return issueResult{refusal: ClassGateReadFailed, basis: "007 request caller mismatch"}, nil
	}
	grant, grantFound, err := ReadGrant(ctx, tx, intent.AuthorizationID)
	if err != nil {
		return issueResult{}, err
	}
	if class := grant.Evaluate(grantFound, request.CallerID, request.ChainID, request.Asset, request.Recipient, request.Amount); class != "" {
		return issueResult{refusal: class, basis: "007 grant gate"}, nil
	}

	scope, err := ReadScope(ctx, tx, intent.AuthorizationID)
	if err != nil {
		return issueResult{}, err
	}
	if class := scope.Evaluate(intent.AuthorizationID, intent.IntentID, req.RequestID, intent.Sender, intent.AuthorizationVersion); class != "" {
		return issueResult{refusal: class, basis: "007 scope gate"}, nil
	}
	if req.Action == ActionReplace && !scope.AllowsFeeReplacement {
		return issueResult{basis: "allows_fee_replacement=false"}, nil
	}

	frozen, freezeClass, err := IsFrozen(ctx, tx, req.IntentID)
	if err != nil {
		return issueResult{}, err
	}
	if frozen {
		return issueResult{basis: "frozen:" + freezeClass}, nil
	}

	if d.Binding == nil {
		return issueResult{refusal: ClassBindingReadFailed, basis: "no binding reader"}, nil
	}
	binding, err := d.Binding.ReadBinding(ctx, req.IntentID)
	if err != nil {
		return issueResult{refusal: ClassBindingReadFailed, basis: "binding read failed"}, nil
	}
	if !binding.PermitsSend() {
		return issueResult{refusal: binding.RefusalClass(), basis: "binding observation " + binding.String()}, nil
	}

	if _, open, err := ReadOpenStep(ctx, tx, req.IntentID); err != nil {
		return issueResult{}, err
	} else if open {
		return issueResult{refusal: ClassStepOpenUnreconciled, basis: "open step exists"}, nil
	}

	stepID, err := NewStepID()
	if err != nil {
		return issueResult{}, err
	}
	rv := recovery.CurrentVersion()
	if err := IssueStep(ctx, tx, StepIssue{
		StepID: stepID, IntentID: req.IntentID, Action: req.Action,
		OwnerID: req.OwnerID, LeaseVersion: req.LeaseVersion, RecoveryVersion: &rv,
	}); err != nil {
		if errors.Is(err, ErrOpenStepExists) {
			return issueResult{refusal: ClassStepOpenUnreconciled, basis: "open step exists"}, nil
		}
		return issueResult{}, err
	}
	if err := advanceProgressTx(ctx, tx, req.IntentID, req.OwnerID, req.LeaseVersion); err != nil {
		return issueResult{}, err
	}
	if err := AppendEvent(ctx, tx, Event{
		IntentID: req.IntentID, Kind: EventStepIssued, LeaseVersion: req.LeaseVersion,
		StepID: stepID, Detail: "action=" + string(req.Action) + " recovery_version=" + fmt.Sprint(rv),
	}); err != nil {
		return issueResult{}, err
	}
	// claimed->executing and revised->executing are both send-enabling edges:
	// the claim and the full gate set above were re-verified in this
	// transaction, so a revision grants no send authority by itself (M2/Q3).
	if intent.State == IntentClaimed || intent.State == IntentRevised {
		if err := TransitionIntent(ctx, tx, req.IntentID, intent.State, intent.StateVersion, IntentExecuting, req.LeaseVersion); err != nil && !errors.Is(err, ErrTransitionRefused) {
			return issueResult{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return issueResult{}, fmt.Errorf("commit step issue: %w", err)
	}
	return issueResult{stepID: stepID, recoveryVersion: rv}, nil
}

// advanceLoop calls the 010 boundary outside every transaction with a bounded
// same-step retry for transport/unavailable classes.
func (d *StepDriver) advanceLoop(ctx context.Context, req StepRequest, stepID string, recoveryVersion int64) (StepOutcome, error) {
	if d.Advancer == nil {
		return StepOutcome{StepID: stepID, FinalStepState: StepIssued, Retryable: true, Refusal: ClassLifecycleUnavailable, Basis: "no advancer wired"}, nil
	}
	max := d.maxRetries()
	for attempt := 0; attempt < max; attempt++ {
		out, err := d.Advancer.Advance(ctx, AdvanceRequest{
			IntentID: req.IntentID, RequestID: req.RequestID, CallerID: req.CallerID,
			OwnerID: req.OwnerID, LeaseVersion: req.LeaseVersion, RecoveryVersion: recoveryVersion,
			StepID: stepID, Action: req.Action,
			AnchorAttemptID: req.AnchorAttemptID, ExpectedTxHash: req.ExpectedTxHash,
			ReplacementFeeMaxPerGas:            req.ReplacementFeeMaxPerGas,
			ReplacementFeeMaxPriorityFeePerGas: req.ReplacementFeeMaxPriorityFeePerGas,
			ReplacementAuthorizationID:         req.ReplacementAuthorizationID,
			ReplacementSigningRequestID:        req.ReplacementSigningRequestID,
		})
		if err != nil {
			if errors.Is(err, ErrAdvanceNoSendResult) {
				return d.converge(ctx, req, stepID, advanceResult{noSendResult: true})
			}
			continue // transport/unavailable: same step_id, bounded
		}
		if out.Class == OutcomeUnavailable {
			continue
		}
		return d.converge(ctx, req, stepID, advanceResult{
			class: out.Class, attemptID: out.AttemptID, txHash: out.TxHash, revisionVersion: out.RevisionVersion,
		})
	}
	return StepOutcome{StepID: stepID, FinalStepState: StepIssued, Retryable: true, Refusal: ClassLifecycleUnavailable, Basis: "bounded retries exhausted"}, nil
}

type advanceResult struct {
	class           string
	attemptID       string
	txHash          string
	revisionVersion int64
	noSendResult    bool
}

// converge is the T-step-converge transaction. It re-verifies the claim under
// FOR UPDATE; a changed qualification means the old holder cannot converge and
// the step stays durable evidence for the new holder to reconcile.
func (d *StepDriver) converge(ctx context.Context, req StepRequest, stepID string, res advanceResult) (StepOutcome, error) {
	state, ok := StepStateForOutcome(res.class)
	if res.noSendResult {
		state, ok = StepRefused, true
	}
	if !ok {
		// An unknown class fails closed: leave the step issued for reconcile.
		return StepOutcome{StepID: stepID, FinalStepState: StepIssued, Retryable: true, Refusal: ClassLifecycleUnavailable, Basis: "unknown outcome class"}, nil
	}

	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return StepOutcome{}, fmt.Errorf("begin step converge: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, GateStatementTimeout); err != nil {
		return StepOutcome{}, fmt.Errorf("converge statement guard: %w", err)
	}
	claimOK, err := verifyClaimTx(ctx, tx, req.IntentID, req.OwnerID, req.LeaseVersion)
	if err != nil {
		return StepOutcome{}, err
	}
	if !claimOK {
		return StepOutcome{StepID: stepID, FinalStepState: StepIssued, Refusal: ClassClaimNotCurrent, Basis: "qualification advanced before converge"}, nil
	}

	conv := StepConverge{StepID: stepID, State: state, OwnerID: req.OwnerID, LeaseVersion: req.LeaseVersion}
	if res.class != "" {
		class := res.class
		conv.OutcomeClass = &class
	}
	if res.attemptID != "" {
		conv.AttemptID = &res.attemptID
	}
	if res.txHash != "" {
		conv.TxHash = &res.txHash
	}
	if res.revisionVersion > 0 {
		v := res.revisionVersion
		conv.RevisionVersion = &v
	}
	conv.Evidence = evidenceFor(res)
	if err := ConvergeStep(ctx, tx, conv); err != nil {
		if errors.Is(err, ErrStepConvergeRefused) {
			return StepOutcome{StepID: stepID, FinalStepState: StepIssued, Refusal: ClassClaimNotCurrent, Basis: "step converge fenced"}, nil
		}
		return StepOutcome{}, err
	}
	if err := advanceProgressTx(ctx, tx, req.IntentID, req.OwnerID, req.LeaseVersion); err != nil {
		return StepOutcome{}, err
	}

	kind := EventStepConverged
	switch {
	case res.noSendResult:
		kind = EventStepRefused
	case state == StepUnknown:
		kind = EventStepUnknown
	case state == StepRefused:
		kind = EventStepRefused
	}
	event := Event{
		IntentID: req.IntentID, Kind: kind, LeaseVersion: req.LeaseVersion,
		StepID: stepID, AttemptID: res.attemptID, RevisionVersion: res.revisionVersion,
		Detail: "outcome_class=" + res.class + " state=" + state,
	}
	if res.noSendResult {
		event.Detail = "no_send_result=true state=refused"
	}
	if err := AppendEvent(ctx, tx, event); err != nil {
		return StepOutcome{}, err
	}

	// Unknown business effect: converge the step and move the intent to
	// reconciling (fact transition, M2) and record the observation. Never
	// failure, never "not paid".
	if state == StepUnknown || res.noSendResult {
		intent, found, err := ReadIntent(ctx, tx, req.IntentID)
		if err != nil {
			return StepOutcome{}, err
		}
		if found && intent.State == IntentExecuting {
			// 013 T030: the unknown-result transition carries the 010 attempt
			// reference (and outcome context) into the outbox event so the
			// transition is traceable back to the attempt fact (T040
			// attempt-level referential-integrity evidence).
			if err := TransitionIntentWithContext(ctx, tx, req.IntentID, intent.State, intent.StateVersion, IntentReconciling, req.LeaseVersion, IntentEventContext{
				AttemptID:    res.attemptID,
				OutcomeClass: res.class,
				NoSendResult: res.noSendResult,
			}); err != nil && !errors.Is(err, ErrTransitionRefused) {
				return StepOutcome{}, err
			}
		}
		if err := AppendEvent(ctx, tx, Event{
			IntentID: req.IntentID, Kind: EventReconcileObserved, LeaseVersion: req.LeaseVersion,
			StepID: stepID, AttemptID: res.attemptID, RevisionVersion: res.revisionVersion,
			Detail: "reason=step_outcome class=" + res.class,
		}); err != nil {
			return StepOutcome{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return StepOutcome{}, fmt.Errorf("commit step converge: %w", err)
	}
	return StepOutcome{
		StepID: stepID, OutcomeClass: res.class, FinalStepState: state,
		AttemptID: res.attemptID, TxHash: res.txHash,
		Reconciling: state == StepUnknown || res.noSendResult,
	}, nil
}

func evidenceFor(res advanceResult) string {
	if res.noSendResult {
		return "no_send_result=true"
	}
	return "outcome_class=" + res.class
}

// verifyClaimTx locks the claim row FOR UPDATE and re-verifies the exact
// qualification (owner + version + active + unexpired on the DB clock).
func verifyClaimTx(ctx context.Context, tx pgx.Tx, intentID, ownerID string, leaseVersion int64) (bool, error) {
	var (
		owner     string
		version   int64
		state     string
		unexpired bool
	)
	err := tx.QueryRow(ctx, `SELECT owner_id, lease_version, state, expires_at > now()
		FROM execution_claims WHERE intent_id = $1 FOR UPDATE`, intentID).
		Scan(&owner, &version, &state, &unexpired)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock claim: %w", err)
	}
	return owner == ownerID && version == leaseVersion && state == "active" && unexpired, nil
}

// advanceProgressTx is the claim-fenced progress write inside the caller's
// transaction: zero rows means the qualification advanced.
func advanceProgressTx(ctx context.Context, tx pgx.Tx, intentID, ownerID string, leaseVersion int64) error {
	tag, err := tx.Exec(ctx, advanceProgressSQL, intentID, ownerID, leaseVersion)
	if err != nil {
		return fmt.Errorf("advance progress in tx: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: progress write fenced", ErrClaimLost)
	}
	return nil
}

const readFreezeSQL = `SELECT detail FROM execution_events
  WHERE intent_id = $1 AND kind = 'reconcile_observed' AND detail LIKE 'freeze:%'
  ORDER BY event_id DESC LIMIT 1`

// IsFrozen reports whether the intent has an unconsumed 010 freeze marker.
// While frozen, 011 refuses further sends and never clears the marker (only
// the 010-side controlled manual release plus a full re-verification may
// resume).
func IsFrozen(ctx context.Context, q Queryer, intentID string) (bool, string, error) {
	var detail string
	err := q.QueryRow(ctx, readFreezeSQL, intentID).Scan(&detail)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("read freeze marker: %w", err)
	}
	class := strings.TrimPrefix(detail, "freeze:")
	if i := strings.IndexAny(class, " \t"); i >= 0 {
		class = class[:i]
	}
	return true, class, nil
}
