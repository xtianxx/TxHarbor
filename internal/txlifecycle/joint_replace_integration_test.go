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
// The fresh-grant leg remains covered by TestJointReplaceFeeBump (a new scope
// whose request_id is the new signing identity). The same-grant conditional
// reuse (V13-4b) is covered by TestJointReplaceFeeBumpSameGrant: the anchor's
// own scope binds the anchor's originating request identity, and the
// replacement is a new signing identity admitted against the anchor under that
// one immutable carrier.
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

// jointSignerRow probes one 009-side request identity (the signer tables live
// in the same isolated database): its row id, terminal state, grant and the
// replacement linkage.
type jointSignerRow struct {
	rowID         int64
	state         string
	refusalClass  string
	authzID       string
	replacementOf *int64
	signingID     string
	intentID      string
	nonce         string
	bindingRef    string
}

func (j *jointEnv) signerRow(signingRequestID string) jointSignerRow {
	j.t.Helper()
	var r jointSignerRow
	if err := j.pool.QueryRow(j.ctx, `SELECT id, state, refusal_class, authorization_id,
		replacement_of, signing_request_id, intent_id, nonce::text, binding_ref
		FROM signing_requests WHERE caller_id = $1 AND signing_request_id = $2`,
		j.callerID, signingRequestID).Scan(&r.rowID, &r.state, &r.refusalClass, &r.authzID,
		&r.replacementOf, &r.signingID, &r.intentID, &r.nonce, &r.bindingRef); err != nil {
		j.t.Fatalf("read 009 signing_requests %s: %v", signingRequestID, err)
	}
	return r
}

func (j *jointEnv) signerResultCount(rowID int64) int {
	j.t.Helper()
	var n int
	if err := j.pool.QueryRow(j.ctx,
		`SELECT count(*) FROM signature_results WHERE signing_request_row = $1`, rowID).Scan(&n); err != nil {
		j.t.Fatal(err)
	}
	return n
}

// TestJointReplaceFeeBumpSameGrant is V13-4b's conditional-reuse (same-grant)
// leg on the production worker path: the anchor's own PB grant — whose carrier
// binds the anchor's originating request identity and explicitly permits fee
// replacement within its caps — backs a NEW attempt + signing identity raised
// inside those caps. The worker Driver drives the replacement through the real
// 010 adapter, the real 009 signer-serve over HTTP and the real Anvil node: the
// replacement supersedes the pending anchor on the same nonce, the sibling is
// revised to replaced only from the observed chain fact, the same-step retry is
// idempotent, and the 009-side history keeps both identities under the one
// immutable grant.
func TestJointReplaceFeeBumpSameGrant(t *testing.T) {
	ctx := context.Background()
	j := newJointEnv(t)
	j.setAutomine(false)
	j.installTransferEmit(1000)

	// The anchor is the grant's first legal use, signed at 1 gwei/0.1 gwei —
	// below the carrier caps (2 gwei / 1.5 gwei tip) so the replacement can
	// raise fees inside the same carrier.
	jj := j.admit()
	anchorRes, err := j.store.Send(ctx, &SendRequest{AttemptID: jj.attemptID, Kind: SendInitial, Claim: j.claim(jj)})
	if err != nil || anchorRes.Outcome != "accepted" {
		t.Fatalf("anchor send = %+v %v", anchorRes, err)
	}
	anchorHash := anchorRes.TxHash
	anchorSR := j.signerRow(jj.signingRequestID)
	if anchorSR.state != "signed" || anchorSR.replacementOf != nil || anchorSR.authzID != jj.authorizationID {
		t.Fatalf("anchor 009 row = %+v, want signed/non-replacement on %s", anchorSR, jj.authorizationID)
	}

	j.markClaimed(jj)
	worker, adapter := j.productionWorker()

	// Same grant, new identity: no ReplacementAuthorizationID, so 010 reuses
	// the anchor's grant; the caller-supplied signing identity keeps the
	// V13-4b identity split explicit.
	const replacementSR = "sr-samegrant-rep-1"
	replaceReq := execution.StepRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: 1,
		OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, Action: execution.ActionReplace,
		AnchorAttemptID:                    jj.attemptID,
		ReplacementFeeMaxPerGas:            "2000000000",
		ReplacementFeeMaxPriorityFeePerGas: "1500000000",
		ReplacementSigningRequestID:        replacementSR,
	}
	out, err := worker.Driver.IssueAndAdvance(ctx, replaceReq)
	if err != nil {
		t.Fatalf("driver same-grant replace: %v", err)
	}
	if out.OutcomeClass != execution.OutcomeSent || out.FinalStepState != execution.StepConverged ||
		out.AttemptID == "" || out.AttemptID == jj.attemptID || strings.EqualFold(out.TxHash, anchorHash) {
		t.Fatalf("driver same-grant replace = %+v, want sent/converged with a new attempt/hash", out)
	}

	// The replacement preserves the anchor's identity and payment semantics:
	// same intent, binding, nonce, sender and economics; a new attempt +
	// signing identity; and it stays under the anchor's own grant.
	repl, err := j.store.AttemptByID(ctx, out.AttemptID)
	if err != nil {
		t.Fatalf("read replacement: %v", err)
	}
	if repl.ReplacementOf != jj.attemptID || repl.IntentID != jj.intentID || repl.BindingRef != jj.bindingID ||
		repl.Nonce != jj.nonce || repl.Sender != j.sender || repl.State != "sent" {
		t.Fatalf("replacement identity/semantics = %+v", repl)
	}
	if repl.AuthorizationID != jj.authorizationID {
		t.Fatalf("replacement grant = %s, want the anchor's own grant %s (same-grant reuse)", repl.AuthorizationID, jj.authorizationID)
	}
	if repl.SigningRequestID != replacementSR {
		t.Fatalf("replacement signing identity = %s, want %s", repl.SigningRequestID, replacementSR)
	}
	if repl.MaxFeePerGas != "2000000000" || repl.MaxPriorityFeePerGas != "1500000000" {
		t.Fatalf("replacement fees = %s/%s, want the caller candidate", repl.MaxFeePerGas, repl.MaxPriorityFeePerGas)
	}
	anchor, err := j.store.AttemptByID(ctx, jj.attemptID)
	if err != nil || anchor.State != "sent" {
		t.Fatalf("anchor state = %+v %v, want sent (sibling not pre-declared)", anchor, err)
	}

	// 009-side history: two signed identities under one immutable grant, the
	// replacement linked to the anchor, both carrying the anchor's
	// intent/binding/nonce, each with exactly one persisted result.
	replSR := j.signerRow(replacementSR)
	if replSR.state != "signed" || replSR.replacementOf == nil || *replSR.replacementOf != anchorSR.rowID {
		t.Fatalf("replacement 009 row = %+v, want signed replacement of %d", replSR, anchorSR.rowID)
	}
	if replSR.authzID != jj.authorizationID || replSR.intentID != jj.intentID ||
		replSR.nonce != anchorSR.nonce || replSR.bindingRef != anchorSR.bindingRef {
		t.Fatalf("replacement 009 identity = %+v, want the anchor's grant/intent/nonce/binding", replSR)
	}
	if anchorSR.state != "signed" || anchorSR.replacementOf != nil {
		t.Fatalf("anchor 009 row changed after the reuse: %+v", anchorSR)
	}
	if a, r := j.signerResultCount(anchorSR.rowID), j.signerResultCount(replSR.rowID); a != 1 || r != 1 {
		t.Fatalf("009 results = anchor %d replacement %d, want 1/1", a, r)
	}
	var authVersion int64
	if err := j.pool.QueryRow(j.ctx,
		`SELECT authorization_version FROM withdrawal_authorization_scopes WHERE authorization_id = $1`,
		jj.authorizationID).Scan(&authVersion); err != nil || authVersion != 1 {
		t.Fatalf("carrier version = %d (%v), want the immutable 1", authVersion, err)
	}

	// Same-step retry converges on the recorded outcome: no second attempt,
	// send or signature.
	retry, err := adapter.Advance(ctx, execution.AdvanceRequest{
		IntentID: jj.intentID, RequestID: jj.requestID, CallerID: 1,
		OwnerID: jj.ownerID, LeaseVersion: jj.claimVersion, StepID: out.StepID,
		Action: execution.ActionReplace, AnchorAttemptID: jj.attemptID,
		ReplacementFeeMaxPerGas:            "2000000000",
		ReplacementFeeMaxPriorityFeePerGas: "1500000000",
		ReplacementSigningRequestID:        replacementSR,
	})
	if err != nil || retry.Class != execution.OutcomeSent || retry.AttemptID != out.AttemptID || retry.TxHash != out.TxHash {
		t.Fatalf("same-step retry = %+v / %v, want the recorded sent outcome for %s", retry, err, out.AttemptID)
	}
	if j.attemptCount(jj.intentID) != 2 || j.sendRowCount(out.AttemptID) != 1 {
		t.Fatalf("retry wrote a second attempt/send: attempts=%d sends=%d",
			j.attemptCount(jj.intentID), j.sendRowCount(out.AttemptID))
	}
	if a, r := j.signerResultCount(anchorSR.rowID), j.signerResultCount(replSR.rowID); a != 1 || r != 1 {
		t.Fatalf("retry re-signed: 009 results = anchor %d replacement %d, want 1/1", a, r)
	}

	// Broadcast + chain reconciliation: the replacement is included, the
	// superseded anchor is not, and the sibling is revised to replaced only
	// from the observed chain fact.
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
	t.Logf("same-grant replacement: grant=%s anchor=%s(009 row %d) replacement=%s(009 row %d) same nonce=%s; tx=%s; sibling revised only after chain fact",
		jj.authorizationID, jj.signingRequestID, anchorSR.rowID, replacementSR, replSR.rowID, jj.nonce, out.TxHash)
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
