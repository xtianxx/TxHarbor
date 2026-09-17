//go:build integration

// joint_replace_integration_test.go is the round-2 fee-replacement acceptance
// (010 FR-05; contracts/send-api.md §2.1; send-gate.md §3 replacement reuse row;
// persistence.md §3 G-010-5): the successful bump runs through the
// production-wired worker Driver (`app.NewJointWithdrawalWorker` ->
// `worker.Driver.IssueAndAdvance` -> `txlifecycle.NewLifecycleLive`) over the
// real 010 Store gates, the real 009 signer-serve and a real Anvil node with
// automine off, so the anchor stays pending and the higher-fee replacement
// supersedes it on the same nonce. Refusals stay zero-write/zero-dispatch, and
// the sibling attempt is revised only from the observed chain fact.
//
// The replacement runs under a fresh PB grant: the scope carrier fixes
// request_id == signing_request_id, so a new signing identity under the
// anchor's own scope can never pass 009's scope gate (the cross-lane finding
// recorded in joint_j2_integration_test.go). The same-grant conditional-reuse
// branch is exercised by its refusal proofs (purpose token, fee caps,
// no-fee-change).
package txlifecycle

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

func (j *jointEnv) setAutomine(on bool) {
	j.t.Helper()
	var ok bool
	jointRPC(j.t, &ok, j.anvilURL, "anvil_setAutomine", on)
}

func (j *jointEnv) mine(blocks int) {
	j.t.Helper()
	var res json.RawMessage
	jointRPC(j.t, &res, j.anvilURL, "anvil_mine", hexutil.EncodeUint64(uint64(blocks)))
}

// replacementStepFields is the caller-supplied replacement candidate read from
// the real PB scope caps; 010 never invents fee values.
type replacementStepFields struct {
	authID      string
	signingID   string
	maxPerGas   string
	maxPriority string
}

// seedReplacementGrant writes the fresh PB grant through the real 007/PB
// supply path with the round-2 scope payload and returns the candidate fee
// values read back from the persisted caps.
func (j *jointEnv) seedReplacementGrant(jj *jointIntent, authID, signingID string) replacementStepFields {
	j.t.Helper()
	if _, err := withdrawal.SupplyGrant(j.ctx, j.pool, withdrawal.OpInput{
		OperationID: "op-" + authID, Action: "supply",
		AuthorizationID: authID, CallerID: j.callerID, ChainID: jointChainID,
		Asset: j.asset, Recipient: j.recipient, Amount: j.amount,
		IntentID: jj.intentID, RequestID: signingID, Sender: j.sender,
		FeeMaxTotal: int64(1e15), FeeMaxPerGas: int64(2e9), FeeMaxPriority: int64(15e8),
		AllowsFeeReplacement: true, AttestedBy: "joint",
	}, "joint", "seed"); err != nil {
		j.t.Fatalf("fresh PB grant supply: %v", err)
	}
	var capPerGas, capPriority int64
	if err := j.pool.QueryRow(j.ctx,
		`SELECT fee_max_per_gas, fee_max_priority FROM withdrawal_authorization_scopes
		  WHERE authorization_id = $1`, authID).Scan(&capPerGas, &capPriority); err != nil {
		j.t.Fatalf("read fresh scope caps: %v", err)
	}
	return replacementStepFields{
		authID: authID, signingID: signingID,
		maxPerGas: strconv.FormatInt(capPerGas, 10), maxPriority: strconv.FormatInt(capPriority, 10),
	}
}

func (f replacementStepFields) stepRequest(jj *jointIntent, anchorID string, action execution.AdvanceAction) execution.StepRequest {
	return execution.StepRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: 1,
		OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, Action: action,
		AnchorAttemptID:                    anchorID,
		ReplacementFeeMaxPerGas:            f.maxPerGas,
		ReplacementFeeMaxPriorityFeePerGas: f.maxPriority,
		ReplacementAuthorizationID:         f.authID,
		ReplacementSigningRequestID:        f.signingID,
	}
}

func (f replacementStepFields) advanceRequest(jj *jointIntent, anchorID, stepID string) execution.AdvanceRequest {
	req := f.stepRequest(jj, anchorID, execution.ActionReplace)
	return execution.AdvanceRequest{
		IntentID: req.IntentID, RequestID: req.RequestID, CallerID: req.CallerID,
		OwnerID: req.OwnerID, LeaseVersion: req.LeaseVersion, StepID: stepID,
		Action: execution.ActionReplace, AnchorAttemptID: anchorID,
		ReplacementFeeMaxPerGas:            f.maxPerGas,
		ReplacementFeeMaxPriorityFeePerGas: f.maxPriority,
		ReplacementAuthorizationID:         f.authID,
		ReplacementSigningRequestID:        f.signingID,
	}
}

// TestJointReplaceFeeBump proves the successful replacement end to end: same
// intent/binding/nonce, new attempt + signing identity, anchor history kept,
// same-step retry idempotent, and the sibling revised to replaced only after
// the replacement is included on chain (never pre-declared).
func TestJointReplaceFeeBump(t *testing.T) {
	ctx := context.Background()
	j := newJointEnv(t)
	j.setAutomine(false)
	j.installTransferEmit(1000)

	jj := j.admit()
	anchorRes, err := j.store.Send(ctx, &SendRequest{AttemptID: jj.attemptID, Kind: SendInitial, Claim: j.claim(jj)})
	if err != nil || anchorRes.Outcome != "accepted" {
		t.Fatalf("anchor send = %+v %v", anchorRes, err)
	}
	anchorHash := anchorRes.TxHash

	fields := j.seedReplacementGrant(jj, "wa-replace-fresh", "sr-replace-1")
	j.markClaimed(jj)
	worker, adapter := j.productionWorker()

	out, err := worker.Driver.IssueAndAdvance(ctx, fields.stepRequest(jj, jj.attemptID, execution.ActionReplace))
	if err != nil {
		t.Fatalf("driver replace: %v", err)
	}
	if out.OutcomeClass != execution.OutcomeSent || out.FinalStepState != execution.StepConverged ||
		out.AttemptID == "" || out.TxHash == "" {
		t.Fatalf("driver replace = %+v, want sent/converged with a new attempt and hash", out)
	}
	if out.AttemptID == jj.attemptID || strings.EqualFold(out.TxHash, anchorHash) {
		t.Fatalf("replacement reused the anchor identity/hash: %+v", out)
	}

	repl, err := j.store.AttemptByID(ctx, out.AttemptID)
	if err != nil {
		t.Fatalf("read replacement: %v", err)
	}
	if repl.ReplacementOf != jj.attemptID || repl.IntentID != jj.intentID || repl.BindingRef != jj.bindingID ||
		repl.Nonce != jj.nonce || repl.Sender != j.sender || repl.State != "sent" {
		t.Fatalf("replacement identity/semantics = %+v", repl)
	}
	if repl.SigningRequestID != fields.signingID || repl.AuthorizationID != fields.authID {
		t.Fatalf("replacement identity = signing %s grant %s, want %s/%s",
			repl.SigningRequestID, repl.AuthorizationID, fields.signingID, fields.authID)
	}
	if repl.MaxFeePerGas != fields.maxPerGas || repl.MaxPriorityFeePerGas != fields.maxPriority {
		t.Fatalf("replacement fees = %s/%s, want caller candidate %s/%s",
			repl.MaxFeePerGas, repl.MaxPriorityFeePerGas, fields.maxPerGas, fields.maxPriority)
	}
	anchor, err := j.store.AttemptByID(ctx, jj.attemptID)
	if err != nil || anchor.State != "sent" {
		t.Fatalf("anchor state = %+v %v, want sent (sibling not pre-declared)", anchor, err)
	}
	if j.sendRowCount(jj.attemptID) != 1 || j.sendRowCount(out.AttemptID) != 1 {
		t.Fatalf("send rows anchor=%d replacement=%d, want 1/1 (history retained)",
			j.sendRowCount(jj.attemptID), j.sendRowCount(out.AttemptID))
	}

	retry, err := adapter.Advance(ctx, fields.advanceRequest(jj, jj.attemptID, out.StepID))
	if err != nil {
		t.Fatalf("same-step replace retry: %v", err)
	}
	if retry.Class != execution.OutcomeSent || retry.AttemptID != out.AttemptID || retry.TxHash != out.TxHash {
		t.Fatalf("retry = %+v, want the recorded sent outcome for %s", retry, out.AttemptID)
	}
	if j.attemptCount(jj.intentID) != 2 || j.sendRowCount(out.AttemptID) != 1 {
		t.Fatalf("retry wrote a second attempt/send: attempts=%d sends=%d",
			j.attemptCount(jj.intentID), j.sendRowCount(out.AttemptID))
	}

	j.mine(1)
	j.waitMined(out.TxHash)
	if receipt, err := j.eth.TransactionReceipt(ctx, common.HexToHash(anchorHash)); err == nil && receipt != nil {
		t.Fatal("the superseded anchor was included on chain")
	}
	j.seedChainTruth()
	rec, err := j.store.Reconcile(ctx, out.AttemptID, "")
	if err != nil || rec.Classification != "included" || rec.ReceiptEffect != "effective" {
		t.Fatalf("reconcile = %+v %v, want included/effective", rec, err)
	}
	anchorAfter, err := j.store.AttemptByID(ctx, jj.attemptID)
	if err != nil || anchorAfter.State != "replaced" {
		t.Fatalf("anchor after chain fact = %+v %v, want replaced", anchorAfter, err)
	}
	replAfter, err := j.store.AttemptByID(ctx, out.AttemptID)
	if err != nil || (replAfter.State != "effective" && replAfter.State != "confirmed") {
		t.Fatalf("replacement after chain fact = %+v %v, want effective/confirmed", replAfter, err)
	}
	if j.sendRowCount(jj.attemptID) != 1 {
		t.Fatal("anchor send history changed on sibling revision")
	}
}

// TestJointReplaceRefusals proves the zero-write/zero-dispatch refusals on the
// production path: identical fees (replacement_no_fee_change), out-of-cap fees
// (fee_scope_exceeded), and the scope purpose token (scope_reuse_forbidden),
// with the driver-level gate and the adapter-level branch both fail-closed.
func TestJointReplaceRefusals(t *testing.T) {
	ctx := context.Background()
	j := newJointEnv(t)
	jj := j.admit()
	j.markClaimed(jj)
	worker, adapter := j.productionWorker()
	attemptsBefore := j.attemptCount(jj.intentID)

	t.Run("no_fee_change", func(t *testing.T) {
		out, err := worker.Driver.IssueAndAdvance(ctx, execution.StepRequest{
			IntentID: jj.intentID, RequestID: jj.requestID, CallerID: 1,
			OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, Action: execution.ActionReplace,
			AnchorAttemptID:                    jj.attemptID,
			ReplacementFeeMaxPerGas:            "1000000000",
			ReplacementFeeMaxPriorityFeePerGas: "100000000",
		})
		if err != nil {
			t.Fatalf("driver replace: %v", err)
		}
		if out.OutcomeClass != execution.OutcomeRefusedBasis || out.FinalStepState != execution.StepConverged {
			t.Fatalf("no-fee-change = %+v, want refused_basis/converged", out)
		}
		adv, err := adapter.Advance(ctx, execution.AdvanceRequest{
			IntentID: jj.intentID, RequestID: jj.requestID, CallerID: 1,
			OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, StepID: "step-replace-nofee",
			Action: execution.ActionReplace, AnchorAttemptID: jj.attemptID,
			ReplacementFeeMaxPerGas:            "1000000000",
			ReplacementFeeMaxPriorityFeePerGas: "100000000",
		})
		if err != nil || adv.Class != execution.OutcomeRefusedBasis ||
			!strings.Contains(adv.Basis, string(ClassReplacementNoFeeChange)) {
			t.Fatalf("010 anchor check = %+v %v, want refused_basis naming %s", adv, err, ClassReplacementNoFeeChange)
		}
		if j.attemptCount(jj.intentID) != attemptsBefore {
			t.Fatal("no-fee-change refusal wrote an attempt")
		}
	})

	t.Run("over_scope_cap", func(t *testing.T) {
		out, err := worker.Driver.IssueAndAdvance(ctx, execution.StepRequest{
			IntentID: jj.intentID, RequestID: jj.requestID, CallerID: 1,
			OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, Action: execution.ActionReplace,
			AnchorAttemptID:                    jj.attemptID,
			ReplacementFeeMaxPerGas:            "2000000001",
			ReplacementFeeMaxPriorityFeePerGas: "100000000",
		})
		if err != nil {
			t.Fatalf("driver replace: %v", err)
		}
		if out.OutcomeClass != execution.OutcomeRefusedBasis || out.FinalStepState != execution.StepConverged {
			t.Fatalf("over-cap = %+v, want refused_basis/converged", out)
		}
		adv, err := adapter.Advance(ctx, execution.AdvanceRequest{
			IntentID: jj.intentID, RequestID: jj.requestID, CallerID: 1,
			OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, StepID: "step-replace-overcap",
			Action: execution.ActionReplace, AnchorAttemptID: jj.attemptID,
			ReplacementFeeMaxPerGas:            "2000000001",
			ReplacementFeeMaxPriorityFeePerGas: "100000000",
		})
		if err != nil || adv.Class != execution.OutcomeRefusedBasis ||
			!strings.Contains(adv.Basis, string(ClassFeeScopeExceeded)) {
			t.Fatalf("010 fee scope = %+v %v, want refused_basis naming %s", adv, err, ClassFeeScopeExceeded)
		}
		if j.attemptCount(jj.intentID) != attemptsBefore {
			t.Fatal("over-cap refusal wrote an attempt")
		}
	})

	t.Run("reuse_forbidden", func(t *testing.T) {
		j.mustExec(`UPDATE withdrawal_authorization_scopes SET allows_fee_replacement = FALSE
			WHERE authorization_id = $1`, jj.authorizationID)
		defer j.mustExec(`UPDATE withdrawal_authorization_scopes SET allows_fee_replacement = TRUE
			WHERE authorization_id = $1`, jj.authorizationID)

		stepsBefore := j.stepCount(jj.intentID)
		gated, err := worker.Driver.IssueAndAdvance(ctx, execution.StepRequest{
			IntentID: jj.intentID, RequestID: jj.requestID, CallerID: 1,
			OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, Action: execution.ActionReplace,
			AnchorAttemptID:                    jj.attemptID,
			ReplacementFeeMaxPerGas:            "1100000000",
			ReplacementFeeMaxPriorityFeePerGas: "100000000",
		})
		if err != nil {
			t.Fatalf("driver replace: %v", err)
		}
		if gated.Refusal != "" || !strings.Contains(gated.Basis, "allows_fee_replacement=false") {
			t.Fatalf("011 scope gate = %+v, want a zero-write basis refusal", gated)
		}
		if got := j.stepCount(jj.intentID); got != stepsBefore {
			t.Fatalf("011 scope gate wrote a step: %d -> %d", stepsBefore, got)
		}

		adv, err := adapter.Advance(ctx, execution.AdvanceRequest{
			IntentID: jj.intentID, RequestID: jj.requestID, CallerID: 1,
			OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, StepID: "step-replace-forbidden",
			Action: execution.ActionReplace, AnchorAttemptID: jj.attemptID,
			ReplacementFeeMaxPerGas:            "1100000000",
			ReplacementFeeMaxPriorityFeePerGas: "100000000",
		})
		if err != nil || adv.Class != execution.OutcomeRefusedBasis ||
			!strings.Contains(adv.Basis, string(ClassScopeReuseForbidden)) {
			t.Fatalf("010 reuse branch = %+v %v, want refused_basis naming %s", adv, err, ClassScopeReuseForbidden)
		}
		if j.attemptCount(jj.intentID) != attemptsBefore {
			t.Fatal("reuse-forbidden refusal wrote an attempt")
		}
	})
}
