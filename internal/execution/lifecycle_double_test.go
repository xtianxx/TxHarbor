//go:build integration

// lifecycle_double_test.go is the TEST-ONLY contract-shape double for the
// 011→010 boundary (T030, contracts/lifecycle.md §2). It is keyed by
// (intent_id, step_id) and carries deterministic fault switches. It is NEVER
// joint verification and MUST NOT be cited as joint evidence (C10; FR-14;
// R12). The real 010 acceptance is owned by 010 (J1-J5).
package execution

import (
	"context"
	"strings"
	"sync"
)

// lifecycleDouble is the labeled test-only LifecycleAdvancer/LifecycleReader.
type lifecycleDouble struct {
	mu sync.Mutex

	calls       []AdvanceRequest
	attempts    map[string]string // (intent|step) -> attempt identity
	nextAttempt int

	resultClass      string
	forceErr         error
	noSendResult     bool
	unavailableTimes int

	readFacts map[string]LifecycleFacts
	readErr   error
}

func newLifecycleDouble() *lifecycleDouble {
	return &lifecycleDouble{attempts: make(map[string]string), readFacts: make(map[string]LifecycleFacts)}
}

func stepKey(intentID, stepID string) string { return intentID + "|" + stepID }

// Advance converges on (intent_id, step_id): a retry with the same key returns
// the recorded attempt and never creates a second one.
func (d *lifecycleDouble) Advance(_ context.Context, req AdvanceRequest) (AdvanceOutcome, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, req)
	key := stepKey(req.IntentID, req.StepID)
	if id, ok := d.attempts[key]; ok {
		return AdvanceOutcome{Class: d.classOrDefault(), AttemptID: id, TxHash: txHashOf(id)}, nil
	}
	if d.forceErr != nil {
		return AdvanceOutcome{}, d.forceErr
	}
	if d.noSendResult {
		return AdvanceOutcome{}, ErrAdvanceNoSendResult
	}
	if d.unavailableTimes > 0 {
		d.unavailableTimes--
		return AdvanceOutcome{Class: OutcomeUnavailable}, nil
	}
	d.nextAttempt++
	id := "attempt-" + itoa(d.nextAttempt)
	d.attempts[key] = id
	return AdvanceOutcome{Class: d.classOrDefault(), AttemptID: id, TxHash: txHashOf(id), RevisionVersion: 1}, nil
}

func (d *lifecycleDouble) Read(_ context.Context, intentID string) (LifecycleFacts, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.readErr != nil {
		return LifecycleFacts{}, d.readErr
	}
	return d.readFacts[intentID], nil
}

func (d *lifecycleDouble) classOrDefault() string {
	if d.resultClass == "" {
		return OutcomeSent
	}
	return d.resultClass
}

func (d *lifecycleDouble) callCount(stepID string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, c := range d.calls {
		if c.StepID == stepID {
			n++
		}
	}
	return n
}

func txHashOf(attemptID string) string {
	sum := len(attemptID)
	return "0x" + strings.Repeat("a", 63) + string(rune('0'+sum%10))
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// bindingDouble is the TEST-ONLY 008 observation double (gates.md §3). It is
// never joint evidence either.
type bindingDouble struct {
	result BindingResult
	err    error
}

func (b bindingDouble) ReadBinding(_ context.Context, _ string) (BindingResult, error) {
	if b.err != nil {
		return BindingResult(0), b.err
	}
	return b.result, nil
}
