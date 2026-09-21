//go:build integration

// Batch D replaced-attempt resend coverage. An attempt that already holds an
// accepted dispatch and has since been revised to `replaced` must refuse both
// send kinds with zero dispatch and zero new durable rows, in two lanes:
//
//   - scratch-DB lane (D1a): the real 010 Store over a migrated scratch
//     database and the lane-local stubRPC chain double (never joint evidence);
//   - joint lane (D1b): the production-wired 011 worker Driver
//     (`app.NewJointWithdrawalWorker` -> `StepDriver.IssueAndAdvance`) over the
//     real 010 adapter, the real 009 signer-serve and a real Anvil node.
//
// D1b additionally records, as implemented and without resolving, the
// `attempt_not_sendable` -> `unavailable` mapping (lifecycle_live.go
// advanceClassForRefusal) combined with the driver's retry-everything loop
// (execution/advance.go advanceLoop retries every OutcomeUnavailable to
// exhaustion). The replaced anchor is a terminal no-send state, so no new
// send/attempt/009-result row may appear regardless of the retry shape.
package txlifecycle

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/xtianxx/txharbor/internal/execution"
)

// envCount runs a count query through the scratch-lane pool.
func envCount(t *testing.T, e *env, query string, args ...any) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

// TestV7ReplacedAnchorResendRefused closes the resend gap at the terminal
// `replaced` state for an attempt that already took its accepted dispatch.
//
// Defect: none known — coverage gap.
// Invariant: the 010 data-model sendability matrix makes
// effective/ineffective/confirmed/orphaned/replaced non-sendable. `SendInitial`
// refuses `already_accepted` when an accepted dispatch already exists
// (gates.go:281-283); `SendReplay` refuses `attempt_not_sendable` for a state
// outside {sent, unknown, signed} (gates.go:291-293). Both are zero-dispatch
// refusals committing gate_refused evidence only, and the send_seq/revision
// history is not advanced.
// Gap: V6's attempt_not_sendable arm hand-sets `effective` with no accepted
// dispatch; no test drives a genuinely `replaced` attempt that dispatched
// accepted first and then re-sends either kind.
// Level: integration (real PostgreSQL scratch DB + stubRPC; never joint
// evidence).
// Criteria: after the anchor's accepted `initial` dispatch, a replacement
// sibling observed included/effective revises the anchor to `replaced`
// (markSiblingsReplaced); then SendReplay -> attempt_not_sendable and
// SendInitial -> already_accepted, each with zero dispatch and committed
// evidence; tx_send_attempts stays at 1 row with send_seq MAX unchanged, and
// revision_seq / tx_attempts / tx_attempt_signings counts stay unchanged.
func TestV7ReplacedAnchorResendRefused(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	// 1) The anchor takes its accepted dispatch first, so the replaced state
	// is reached with durable accepted evidence.
	f := e.seed()
	f.sign()
	first, err := e.send(f, SendInitial, nil)
	if err != nil || first.Outcome != "accepted" {
		t.Fatalf("anchor send = %+v %v, want accepted", first, err)
	}

	// 2) TestV9SiblingReplaced recipe (confirm_integration_test.go:127-134): a
	// replacement sibling over the same binding.
	replacement := *f.request
	replacement.AttemptID = f.attemptID + "-rep"
	replacement.SigningRequestID = f.signingRequestID + "-rep"
	replacement.ReplacementOf = f.attemptID
	replacement.MaxPriorityFeePerGas = "50000000"
	if _, err := e.store.PrepareAttempt(ctx, &replacement); err != nil {
		t.Fatalf("replacement prepare: %v", err)
	}
	f.signRequest(&replacement)

	// 3) The replacement is observed included/effective, so T4 revises the
	// anchor to replaced; the sibling revision is chain-fact driven, never
	// pre-declared.
	e.rpc.mu.Lock()
	e.rpc.txFound = true
	e.rpc.txPending = false
	e.rpc.receipt = receiptAt(100, blockHashHex(100), 1, []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)})
	e.rpc.mu.Unlock()
	if _, err := e.store.Reconcile(ctx, replacement.AttemptID, ""); err != nil {
		t.Fatalf("reconcile replacement: %v", err)
	}
	anchor, err := e.store.AttemptByID(ctx, f.attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if anchor.State != "replaced" {
		t.Fatalf("anchor state = %s, want replaced", anchor.State)
	}

	revBefore := anchor.RevisionSeq
	attemptsBefore := envCount(t, e, `SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, f.intentID)
	signingsBefore := envCount(t, e, `SELECT count(*) FROM tx_attempt_signings WHERE attempt_id = $1`, f.attemptID)
	sendsBefore := envCount(t, e, `SELECT count(*) FROM tx_send_attempts WHERE attempt_id = $1`, f.attemptID)
	seqBefore := envCount(t, e, `SELECT COALESCE(MAX(send_seq), 0) FROM tx_send_attempts WHERE attempt_id = $1`, f.attemptID)
	if sendsBefore != 1 || seqBefore != 1 {
		t.Fatalf("anchor history = %d sends, max send_seq %d; want 1/1", sendsBefore, seqBefore)
	}

	// (1) Replay of the replaced attempt: state=replaced is outside the
	// {sent, unknown, signed} replay set -> attempt_not_sendable.
	before := e.rpc.dispatchCount()
	replayRes, replayErr := e.send(f, SendReplay, nil)
	assertBlocked(t, e, f.attemptID, replayRes, replayErr, ClassAttemptNotSendable, before)

	// (2) Initial on the replaced attempt: an accepted dispatch exists, so the
	// accepted arm wins over the state fallback -> already_accepted.
	before = e.rpc.dispatchCount()
	initialRes, initialErr := e.send(f, SendInitial, nil)
	assertBlocked(t, e, f.attemptID, initialRes, initialErr, ClassAlreadyAccepted, before)

	// Zero new rows: the refusals are evidence-only.
	if got := envCount(t, e, `SELECT count(*) FROM tx_send_attempts WHERE attempt_id = $1`, f.attemptID); got != sendsBefore {
		t.Fatalf("send rows moved: %d -> %d", sendsBefore, got)
	}
	if got := envCount(t, e, `SELECT COALESCE(MAX(send_seq), 0) FROM tx_send_attempts WHERE attempt_id = $1`, f.attemptID); got != seqBefore {
		t.Fatalf("send_seq moved: %d -> %d", seqBefore, got)
	}
	if got := envCount(t, e, `SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, f.intentID); got != attemptsBefore {
		t.Fatalf("attempt rows moved: %d -> %d", attemptsBefore, got)
	}
	if got := envCount(t, e, `SELECT count(*) FROM tx_attempt_signings WHERE attempt_id = $1`, f.attemptID); got != signingsBefore {
		t.Fatalf("signing rows moved: %d -> %d", signingsBefore, got)
	}
	anchorAfter, err := e.store.AttemptByID(ctx, f.attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if anchorAfter.State != "replaced" || anchorAfter.RevisionSeq != revBefore {
		t.Fatalf("anchor history moved: state=%s revision=%d, want replaced/%d",
			anchorAfter.State, anchorAfter.RevisionSeq, revBefore)
	}
	if got := e.rpc.dispatchCount(); got != before {
		t.Fatalf("dispatch count moved during refusals: %d", got)
	}
}

// signatureResultRowsForIntent counts the 009 signature_results persisted
// under the intent's signing identities.
func (j *jointEnv) signatureResultRowsForIntent(intentID string) int {
	j.t.Helper()
	var n int
	if err := j.pool.QueryRow(j.ctx,
		`SELECT count(*) FROM signature_results r
		   JOIN signing_requests s ON s.id = r.signing_request_row
		  WHERE s.intent_id = $1`, intentID).Scan(&n); err != nil {
		j.t.Fatal(err)
	}
	return n
}

// signingRowsForAttempt counts 010's persisted signed bytes for one attempt.
func (j *jointEnv) signingRowsForAttempt(attemptID string) int {
	j.t.Helper()
	var n int
	if err := j.pool.QueryRow(j.ctx,
		`SELECT count(*) FROM tx_attempt_signings WHERE attempt_id = $1`, attemptID).Scan(&n); err != nil {
		j.t.Fatal(err)
	}
	return n
}

// gateRefusedRows counts the attempt's committed zero-dispatch refusal
// evidence for one refusal class.
func (j *jointEnv) gateRefusedRows(attemptID string, class RefusalClass) int {
	j.t.Helper()
	var n int
	if err := j.pool.QueryRow(j.ctx,
		`SELECT count(*) FROM tx_attempt_events
		  WHERE attempt_id = $1 AND event = 'gate_refused' AND reason_class = $2`,
		attemptID, string(class)).Scan(&n); err != nil {
		j.t.Fatal(err)
	}
	return n
}

// TestJointReplacedAnchorNoResend drives a replay against a replaced anchor
// through the production worker Driver on the real joint stack.
//
// Defect: the driver's advanceLoop retries every `unavailable` outcome
// (execution/advance.go), and 010's adapter maps `attempt_not_sendable` to
// `unavailable` (lifecycle_live.go advanceClassForRefusal), so a terminal
// no-send state is retried to exhaustion instead of converging on the
// recorded refusal. lifecycle.md §7 requires retryable vs non-retryable
// classes to be distinguished and never collapsed. Encoded as implemented;
// NOT resolved here.
// Invariant: 010 FR-04 replay resends the anchor's persisted bytes only under
// the full gate set; `replaced` is terminal/non-sendable (data-model attempt
// state machine), so no new attempt, signing, send, 009 result or bytes may
// appear, and the anchor's accepted history is immutable. The worker's step
// stays issued (never a fabricated success/failure) for reconcile.
// Gap: no joint test drove a replay against a replaced anchor through the
// production Driver; the retry-everything behavior on the unavailable mapping
// was unrecorded.
// Level: joint lane (real PostgreSQL, real Anvil, real 009 signer-serve,
// production worker Driver + real 010 adapter).
// Criteria: TestJointReplaceFeeBump's flow revises the anchor to `replaced`
// from the chain fact; the driver replay leaves the step `issued` with no
// attempt, adds zero tx_send_attempts / tx_attempts / tx_attempt_signings /
// signature_results rows, keeps the anchor's accepted send row and
// revision_seq byte-identical, and commits exactly one attempt_not_sendable
// gate_refused event per bounded retry (3).
func TestJointReplacedAnchorNoResend(t *testing.T) {
	ctx := context.Background()
	j := newJointEnv(t)
	j.setAutomine(false)
	j.installTransferEmit(1000)

	jj := j.admit()
	anchorRes, err := j.store.Send(ctx, &SendRequest{AttemptID: jj.attemptID, Kind: SendInitial, Claim: j.claim(jj)})
	if err != nil || anchorRes.Outcome != "accepted" {
		t.Fatalf("anchor send = %+v %v, want accepted", anchorRes, err)
	}
	anchorHash := anchorRes.TxHash

	fields := j.seedReplacementGrant(jj, "wa-batchd-rep", "sr-batchd-rep")
	j.markClaimed(jj)
	worker, _ := j.productionWorker()

	out, err := worker.Driver.IssueAndAdvance(ctx, fields.stepRequest(jj, jj.attemptID, execution.ActionReplace))
	if err != nil || out.OutcomeClass != execution.OutcomeSent || out.FinalStepState != execution.StepConverged || out.AttemptID == "" {
		t.Fatalf("driver replace = %+v %v, want sent/converged with a new attempt", out, err)
	}
	if out.AttemptID == jj.attemptID || strings.EqualFold(out.TxHash, anchorHash) {
		t.Fatalf("replacement reused the anchor identity/hash: %+v", out)
	}

	j.mine(1)
	j.waitMined(out.TxHash)
	if receipt, rerr := j.eth.TransactionReceipt(ctx, common.HexToHash(anchorHash)); rerr == nil && receipt != nil {
		t.Fatal("the superseded anchor was included on chain")
	}
	j.seedChainTruth()
	if _, err := j.store.Reconcile(ctx, out.AttemptID, ""); err != nil {
		t.Fatalf("reconcile replacement: %v", err)
	}
	anchorBefore, err := j.store.AttemptByID(ctx, jj.attemptID)
	if err != nil || anchorBefore.State != "replaced" {
		t.Fatalf("anchor = %+v %v, want replaced after the chain fact", anchorBefore, err)
	}

	var (
		seqBefore     int
		kindBefore    string
		outcomeBefore string
		atBefore      time.Time
	)
	if err := j.pool.QueryRow(ctx,
		`SELECT send_seq, kind, outcome, dispatched_at FROM tx_send_attempts WHERE attempt_id = $1`,
		jj.attemptID).Scan(&seqBefore, &kindBefore, &outcomeBefore, &atBefore); err != nil {
		t.Fatal(err)
	}
	revBefore := anchorBefore.RevisionSeq
	sendsBefore := j.sendRowCount(jj.attemptID)
	attemptsBefore := j.attemptCount(jj.intentID)
	signingsBefore := j.signingRowsForAttempt(jj.attemptID)
	resultsBefore := j.signatureResultRowsForIntent(jj.intentID)
	refusalsBefore := j.gateRefusedRows(jj.attemptID, ClassAttemptNotSendable)

	replay, err := worker.Driver.IssueAndAdvance(ctx, execution.StepRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: j.callerID,
		OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, Action: execution.ActionReplay,
		AnchorAttemptID: jj.attemptID, ExpectedTxHash: anchorHash,
	})
	if err != nil {
		t.Fatalf("driver replay on the replaced anchor: %v", err)
	}
	// Implemented behavior (defect encoded, not resolved): attempt_not_sendable
	// -> OutcomeUnavailable, and advanceLoop retries every unavailable class;
	// after the bounded retries the step stays issued. The 010 gate refuses
	// before any dispatch, so no bytes ever leave.
	if replay.FinalStepState != execution.StepIssued || !replay.Retryable || replay.Refusal != execution.ClassLifecycleUnavailable {
		t.Fatalf("replaced-anchor replay = %+v, want issued/retryable %s (attempt_not_sendable mapped to unavailable and retried)",
			replay, execution.ClassLifecycleUnavailable)
	}
	if replay.StepID == "" {
		t.Fatal("replay refusal lost the step identity")
	}
	var stepState string
	var stepAttempt *string
	if err := j.pool.QueryRow(ctx,
		`SELECT state, attempt_id FROM execution_steps WHERE step_id = $1`, replay.StepID).
		Scan(&stepState, &stepAttempt); err != nil {
		t.Fatal(err)
	}
	if stepState != execution.StepIssued || stepAttempt != nil {
		t.Fatalf("replay step = %s/%v, want issued with no attempt (reconcile-before-decision holds)", stepState, stepAttempt)
	}

	// Zero new send/attempt/009-result rows.
	if got := j.sendRowCount(jj.attemptID); got != sendsBefore {
		t.Fatalf("anchor send rows moved: %d -> %d", sendsBefore, got)
	}
	if got := j.attemptCount(jj.intentID); got != attemptsBefore {
		t.Fatalf("intent attempt rows moved: %d -> %d", attemptsBefore, got)
	}
	if got := j.signingRowsForAttempt(jj.attemptID); got != signingsBefore {
		t.Fatalf("anchor signing rows moved: %d -> %d", signingsBefore, got)
	}
	if got := j.signatureResultRowsForIntent(jj.intentID); got != resultsBefore {
		t.Fatalf("009 signature result rows moved: %d -> %d", resultsBefore, got)
	}

	// The anchor's accepted send row and revision are byte-identical.
	var (
		seqAfter     int
		kindAfter    string
		outcomeAfter string
		atAfter      time.Time
	)
	if err := j.pool.QueryRow(ctx,
		`SELECT send_seq, kind, outcome, dispatched_at FROM tx_send_attempts WHERE attempt_id = $1`,
		jj.attemptID).Scan(&seqAfter, &kindAfter, &outcomeAfter, &atAfter); err != nil {
		t.Fatal(err)
	}
	if seqAfter != seqBefore || kindAfter != kindBefore || outcomeAfter != outcomeBefore || !atAfter.Equal(atBefore) {
		t.Fatalf("anchor send row changed: %d/%s/%s/%s -> %d/%s/%s/%s",
			seqBefore, kindBefore, outcomeBefore, atBefore, seqAfter, kindAfter, outcomeAfter, atAfter)
	}
	anchorAfter, err := j.store.AttemptByID(ctx, jj.attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if anchorAfter.State != "replaced" || anchorAfter.RevisionSeq != revBefore {
		t.Fatalf("anchor history moved: state=%s revision=%d, want replaced/%d",
			anchorAfter.State, anchorAfter.RevisionSeq, revBefore)
	}

	// The retry-everything loop commits one gate_refused event per bounded
	// retry (3); that evidence is the only new anchor history.
	if got := j.gateRefusedRows(jj.attemptID, ClassAttemptNotSendable); got-refusalsBefore != 3 {
		t.Fatalf("attempt_not_sendable refusals = %d, want 3 (one per bounded retry; retry-everything encoded)", got-refusalsBefore)
	}
}
