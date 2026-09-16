// observe_redact_test.go pins the T020 observability funnel for allocation
// events (FR-21/SC-09): the structured fields required by
// contracts/observation.md §5 are emitted, every value passes through
// logx.Redact, and the metrics seam receives the fixed machine vocabulary
// only — never a request-derived value.
package nonce

import (
	"bytes"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/logx"
)

// recordingSink captures what the allocation metrics seam observed.
type recordingSink struct {
	results         []string
	replays         int
	classifications []string
}

func (s *recordingSink) ObserveNonceAllocation(result string) { s.results = append(s.results, result) }
func (s *recordingSink) ObserveNonceReplay()                  { s.replays++ }
func (s *recordingSink) ObserveNonceObservation(c string) {
	s.classifications = append(s.classifications, c)
}

const allocRedactSender = "0x1111111111111111111111111111111111111111"

// TestAllocationEmissionRedactsSecrets drives the emission funnel with
// credential-shaped values in every string field and proves the log line
// carries the required fields, no raw credential, and the logx placeholder.
func TestAllocationEmissionRedactsSecrets(t *testing.T) {
	var buf bytes.Buffer
	sink := &recordingSink{}
	a := (&Allocator{}).WithObservability(slog.New(slog.NewTextHandler(&buf, nil)), sink)

	const (
		token = "sk-live-1234567890"
		pass  = "hunter2"
	)
	a.emitAllocation(allocationObservation{
		chainID:        31337,
		sender:         allocRedactSender,
		nonce:          big.NewInt(7),
		bindingID:      "nb-00000000000000000000000000000001",
		intentID:       "authorization=Bearer " + token,
		holdID:         "nh-00000000000000000000000000000001",
		cause:          "password=" + pass,
		classification: ClassificationDivergence,
		registrySeq:    3,
		result:         OutcomeScopeHeld,
	})

	line := buf.String()
	for _, secret := range []string{token, pass} {
		if strings.Contains(line, secret) {
			t.Fatalf("allocation log leaked %q: %s", secret, line)
		}
	}
	if !strings.Contains(line, logx.Redacted) {
		t.Fatalf("allocation log has no %s marker: %s", logx.Redacted, line)
	}
	for _, field := range []string{
		"chain_id", "sender", "nonce", "binding_id", "intent_id",
		"hold_id", "cause", "classification", "registry_seq",
	} {
		if !strings.Contains(line, field+"=") {
			t.Errorf("allocation log missing structured field %q: %s", field, line)
		}
	}

	// The metrics seam saw exactly the fixed vocabulary values.
	if len(sink.results) != 1 || sink.results[0] != string(OutcomeScopeHeld) {
		t.Fatalf("sink results = %v, want [%s]", sink.results, OutcomeScopeHeld)
	}
	if len(sink.classifications) != 1 || sink.classifications[0] != ClassificationDivergence {
		t.Fatalf("sink classifications = %v, want [%s]", sink.classifications, ClassificationDivergence)
	}
	if sink.replays != 0 {
		t.Fatalf("sink replays = %d on a refusal, want 0", sink.replays)
	}

	// Direct logx.Redact coverage for the same credential shapes.
	for _, raw := range []string{
		"dsn=postgres://txharbor:" + pass + "@db:5432/txharbor",
		"token=" + token,
		"Authorization: Bearer " + token,
	} {
		if got := logx.Redact(raw); strings.Contains(got, token) || strings.Contains(got, pass) {
			t.Fatalf("logx.Redact(%q) leaked a credential: %q", raw, got)
		}
	}
}

// TestAllocationEmissionCountsEveryMachineReason proves every refusal outcome
// reaches the allocation counter with its own machine reason, replays also
// increment the dedicated replay counter, and an unclassified attempt is
// emitted nowhere.
func TestAllocationEmissionCountsEveryMachineReason(t *testing.T) {
	var buf bytes.Buffer
	sink := &recordingSink{}
	a := (&Allocator{}).WithObservability(slog.New(slog.NewTextHandler(&buf, nil)), sink)

	refusals := []Outcome{
		OutcomeAllocationConflict,
		OutcomeChainViewUnavailable,
		OutcomeScopeHeld,
		OutcomeSenderNotRegistered,
		OutcomeSenderDisabled,
		OutcomeAuthorizationInvalid,
		OutcomeRebuildIncomplete,
		OutcomeRecoveryActive,
		OutcomeTemporarilyUnavailable,
	}
	for _, refusal := range refusals {
		a.emitAllocation(allocationObservation{
			chainID: 31337, sender: allocRedactSender, result: refusal, cause: string(refusal),
		})
	}
	a.emitAllocation(allocationObservation{
		chainID: 31337, sender: allocRedactSender, result: OutcomeAllocated,
		classification: ClassificationConsistent,
	})
	a.emitAllocation(allocationObservation{
		chainID: 31337, sender: allocRedactSender, result: OutcomeReplayed,
		classification: ClassificationConsistent,
	})

	want := map[string]bool{string(OutcomeAllocated): true, string(OutcomeReplayed): true}
	for _, refusal := range refusals {
		want[string(refusal)] = true
	}
	if len(sink.results) != len(want) {
		t.Fatalf("allocation results = %v, want %d entries", sink.results, len(want))
	}
	for _, got := range sink.results {
		if !want[got] {
			t.Fatalf("unexpected allocation result %q (want one of the machine vocabulary)", got)
		}
		delete(want, got)
	}
	if len(want) != 0 {
		t.Fatalf("machine reasons never counted: %v", want)
	}
	if sink.replays != 1 {
		t.Fatalf("replays = %d, want 1", sink.replays)
	}

	before := len(sink.results)
	a.emitAllocation(allocationObservation{chainID: 31337, sender: allocRedactSender})
	if len(sink.results) != before || sink.replays != 1 {
		t.Fatalf("an unclassified attempt was emitted: results=%v replays=%d", sink.results, sink.replays)
	}
}
