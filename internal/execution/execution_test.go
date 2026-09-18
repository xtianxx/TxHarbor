package execution

import (
	"encoding/hex"
	"testing"
)

func TestClassifyTransition(t *testing.T) {
	sendEnabling := [][2]string{
		{IntentAdmitted, IntentClaimed},
		{IntentClaimed, IntentExecuting},
		{IntentReconciling, IntentExecuting},
		{IntentRevised, IntentExecuting},
	}
	for _, e := range sendEnabling {
		if got := ClassifyTransition(e[0], e[1]); got != TransitionSendEnabling {
			t.Errorf("ClassifyTransition(%s,%s) = %v, want send-enabling", e[0], e[1], got)
		}
	}

	facts := [][2]string{
		{IntentExecuting, IntentReconciling},
		{IntentExecuting, IntentCompleted},
		{IntentReconciling, IntentCompleted},
		{IntentExecuting, IntentFailed},
		{IntentReconciling, IntentFailed},
		{IntentCompleted, IntentRevised},
		{IntentFailed, IntentRevised},
	}
	for _, e := range facts {
		if got := ClassifyTransition(e[0], e[1]); got != TransitionFact {
			t.Errorf("ClassifyTransition(%s,%s) = %v, want fact", e[0], e[1], got)
		}
	}

	illegal := [][2]string{
		{IntentAdmitted, IntentCompleted},
		{IntentAdmitted, IntentExecuting},
		{IntentAdmitted, IntentRevised},
		{IntentClaimed, IntentCompleted},
		{IntentFailed, IntentCompleted},
		{IntentFailed, IntentExecuting},
		{IntentCompleted, IntentExecuting},
		{IntentRevised, IntentCompleted},
		{"bogus", IntentClaimed},
	}
	for _, e := range illegal {
		if got := ClassifyTransition(e[0], e[1]); got != TransitionNone {
			t.Errorf("ClassifyTransition(%s,%s) = %v, want none (refusal)", e[0], e[1], got)
		}
	}
}

func TestStepStateForOutcome(t *testing.T) {
	converged := []string{OutcomeSent, OutcomeRefusedGate, OutcomeRefusedBasis}
	for _, class := range converged {
		state, ok := StepStateForOutcome(class)
		if !ok || state != StepConverged {
			t.Errorf("StepStateForOutcome(%s) = %q,%v want converged,true", class, state, ok)
		}
		if OutcomeRequiresReconcile(class) {
			t.Errorf("%s must not require reconcile", class)
		}
	}
	for _, class := range []string{OutcomePendingUnknown, OutcomeReconcileRequired} {
		state, ok := StepStateForOutcome(class)
		if !ok || state != StepUnknown {
			t.Errorf("StepStateForOutcome(%s) = %q,%v want unknown,true", class, state, ok)
		}
		if !OutcomeRequiresReconcile(class) {
			t.Errorf("%s must require reconcile (never failure)", class)
		}
	}
	if state, ok := StepStateForOutcome(OutcomeUnavailable); !ok || state != StepIssued {
		t.Errorf("StepStateForOutcome(unavailable) = %q,%v want issued,true", state, ok)
	}
	if _, ok := StepStateForOutcome("bogus"); ok {
		t.Error("unknown outcome class must not map")
	}
}

func TestRefusalRetryability(t *testing.T) {
	cases := map[RefusalClass]bool{
		ClassExecutionPermissionDenied: false,
		ClassAuthorizationExpired:      false,
		ClassAuthorizationRevoked:      false,
		ClassAuthorizationInvalid:      true,
		ClassRecoveryPaused:            true,
		ClassGateReadFailed:            true,
		ClassBindingPaused:             true,
		ClassBindingReadFailed:         true,
		ClassBindingTerminal:           false,
		ClassClaimLost:                 true,
		ClassStepOpenUnreconciled:      true,
		ClassOperationConflict:         false,
	}
	for class, want := range cases {
		if got := class.Retryable(); got != want {
			t.Errorf("%s.Retryable() = %v, want %v", class, got, want)
		}
	}
	if RefusalClass("not_a_class").Retryable() {
		t.Error("an unknown refusal class must not be retryable")
	}
}

func TestBindingResultMapping(t *testing.T) {
	if !BindingMatches.PermitsSend() || BindingMatches.RefusalClass() != "" {
		t.Error("only BindingMatches may permit a send-class step")
	}
	cases := map[BindingResult]RefusalClass{
		BindingAbsent:     ClassBindingAbsent,
		BindingConflict:   ClassBindingConflict,
		BindingPaused:     ClassBindingPaused,
		BindingTerminal:   ClassBindingTerminal,
		BindingReadFailed: ClassBindingReadFailed,
	}
	for res, want := range cases {
		if res.PermitsSend() {
			t.Errorf("%v must not permit send", res)
		}
		if got := res.RefusalClass(); got != want {
			t.Errorf("%v.RefusalClass() = %s, want %s", res, got, want)
		}
	}
}

func TestEventKindsClosedList(t *testing.T) {
	if len(EventKinds) != 16 {
		t.Fatalf("EventKinds has %d kinds, want 16", len(EventKinds))
	}
	for _, kind := range EventKinds {
		if !ValidEventKind(kind) {
			t.Errorf("ValidEventKind(%q) = false", kind)
		}
	}
	if ValidEventKind("bogus_kind") {
		t.Error("ValidEventKind accepted an out-of-list kind")
	}
}

func TestNewStepID(t *testing.T) {
	first, err := NewStepID()
	if err != nil {
		t.Fatalf("NewStepID: %v", err)
	}
	raw, err := hex.DecodeString(first)
	if err != nil || len(raw) != StepIDBytes {
		t.Fatalf("NewStepID() = %q, want %d-byte hex", first, StepIDBytes)
	}
	second, err := NewStepID()
	if err != nil {
		t.Fatalf("NewStepID: %v", err)
	}
	if first == second {
		t.Error("NewStepID returned the same id twice")
	}
}
