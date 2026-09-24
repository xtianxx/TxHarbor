// consumer_test.go is the T041 unit layer: envelope parsing/routing fail-closed
// branches, the pure version-guard decision, the bounded gap wait, the retry
// classification/backoff and the quarantine-reason mapping. The transactional
// T4 branches (inbox dedup, effect + progress atomicity, real gap windows)
// run in the T046–T048 integration layer; no database is used here.
package events

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// envelopeForEvent renders the transport envelope of one event the way the
// publisher does, so parse tests exercise the real wire shape.
func envelopeForEvent(t *testing.T, ev Event, version int64) []byte {
	t.Helper()
	prepared, err := ev.prepare()
	if err != nil {
		t.Fatalf("prepare event: %v", err)
	}
	wire := envelopeJSON{
		EventID:          NewEventID(naturalKeyFor(prepared, version)).String(),
		EventType:        prepared.EventType,
		SchemaVersion:    prepared.SchemaVersion,
		IdentityKind:     string(prepared.IdentityKind),
		AggregateType:    prepared.AggregateType,
		AggregateID:      prepared.AggregateID,
		AggregateVersion: version,
		OccurredAt:       prepared.OccurredAt,
		Payload:          json.RawMessage(prepared.PayloadBytes()),
	}
	if prepared.ChainID > 0 {
		wire.ChainID = &prepared.ChainID
	}
	if prepared.BlockNumber > 0 {
		wire.BlockNumber = &prepared.BlockNumber
	}
	if prepared.BlockHash != "" {
		wire.BlockHash = &prepared.BlockHash
	}
	if prepared.TxHash != "" {
		wire.TxHash = &prepared.TxHash
	}
	if prepared.IdentityKind == IdentityKindEVMLog {
		index := prepared.LogIndex
		wire.LogIndex = &index
	}
	if prepared.RecoveryVersion > 0 {
		wire.RecoveryVersion = &prepared.RecoveryVersion
	}
	if prepared.RevisesEventID != [16]byte{} {
		revises := prepared.RevisesEventID.String()
		wire.RevisesEventID = &revises
	}
	body, err := json.Marshal(wire)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return body
}

// TestConsumerParseEnvelopeRoutesEveryCatalogType walks every catalog v1 type
// through the wire shape: each one parses, routes and validates.
func TestConsumerParseEnvelopeRoutesEveryCatalogType(t *testing.T) {
	for _, spec := range CatalogV1() {
		ev := validEvent(t, spec.Type)
		env, err := ParseEnvelope(envelopeForEvent(t, ev, 1))
		if err != nil {
			t.Fatalf("%s: ParseEnvelope error = %v", spec.Type, err)
		}
		if env.EventType != spec.Type || env.IdentityKind != spec.IdentityKind || env.AggregateVersion != 1 {
			t.Fatalf("%s: parsed envelope = %+v", spec.Type, env)
		}
		if env.Raw() == nil {
			t.Fatalf("%s: raw bytes were not preserved", spec.Type)
		}
	}
}

// TestConsumerParseEnvelopeFailClosed covers the parse/routing refusals: each
// one maps to the closed quarantine reason the consumer uses.
func TestConsumerParseEnvelopeFailClosed(t *testing.T) {
	base := envelopeForEvent(t, validEvent(t, EventTypeDepositObservationStatusChanged), 2)

	cases := []struct {
		name   string
		mutate func(wire map[string]any)
		wantIs error
	}{
		{"unknown event type", func(w map[string]any) { w["event_type"] = "unknown.type" }, ErrContract},
		{"unsupported schema version", func(w map[string]any) { w["schema_version"] = 2 }, ErrSchemaUnsupported},
		{"identity kind mismatch", func(w map[string]any) { w["identity_kind"] = "evm_log" }, ErrContract},
		{"missing aggregate id", func(w map[string]any) { w["aggregate_id"] = "" }, ErrContract},
		{"missing aggregate version", func(w map[string]any) { w["aggregate_version"] = 0 }, ErrContract},
		{"missing occurred_at", func(w map[string]any) { w["occurred_at"] = time.Time{} }, ErrContract},
		{"missing payload key", func(w map[string]any) {
			w["payload"] = map[string]any{"from_state": "pending"}
		}, ErrContract},
		{"forbidden payload material", func(w map[string]any) {
			w["payload"] = map[string]any{"from_state": "a", "to_state": "b", "reason": "c", "private_key": "x"}
		}, ErrContract},
		{"bad event id", func(w map[string]any) { w["event_id"] = "not-a-uuid" }, ErrContract},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var wire map[string]any
			if err := json.Unmarshal(base, &wire); err != nil {
				t.Fatalf("decode base envelope: %v", err)
			}
			tc.mutate(wire)
			body, err := json.Marshal(wire)
			if err != nil {
				t.Fatalf("encode mutated envelope: %v", err)
			}
			if _, err := ParseEnvelope(body); !errors.Is(err, tc.wantIs) {
				t.Fatalf("ParseEnvelope = %v, want %v", err, tc.wantIs)
			}
		})
	}

	if _, err := ParseEnvelope([]byte("{not json")); !errors.Is(err, ErrContract) {
		t.Fatalf("invalid JSON = %v, want ErrContract", err)
	}
}

// TestConsumerParseEnvelopeIgnoresUnknownOptionalFields covers FR-12: a
// backward-compatible optional addition keeps the same schema_version and is
// ignored by the consumer.
func TestConsumerParseEnvelopeIgnoresUnknownOptionalFields(t *testing.T) {
	var wire map[string]any
	if err := json.Unmarshal(envelopeForEvent(t, validEvent(t, EventTypeDepositObservationStatusChanged), 2), &wire); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	wire["future_optional_field"] = map[string]any{"nested": true}
	payload := wire["payload"].(map[string]any)
	payload["future_payload_field"] = "ignored"
	body, err := json.Marshal(wire)
	if err != nil {
		t.Fatalf("encode envelope: %v", err)
	}
	env, err := ParseEnvelope(body)
	if err != nil {
		t.Fatalf("ParseEnvelope with unknown optional fields = %v", err)
	}
	if env.SchemaVersion != 1 {
		t.Fatalf("schema_version = %d, want 1", env.SchemaVersion)
	}
}

// TestConsumerChainIdentity covers contracts/events.md §6: a chain mismatch or
// a missing chain identity on a chain-derived event is identity_mismatch.
func TestConsumerChainIdentity(t *testing.T) {
	evmLog := validEvent(t, EventTypeDepositObservationCreated)
	env, err := ParseEnvelope(envelopeForEvent(t, evmLog, 1))
	if err != nil {
		t.Fatalf("parse evm_log envelope: %v", err)
	}
	if err := env.CheckChainIdentity(1); err != nil {
		t.Fatalf("matching chain refused: %v", err)
	}
	if err := env.CheckChainIdentity(31337); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("chain mismatch = %v, want ErrIdentityMismatch", err)
	}
	if err := env.CheckChainIdentity(0); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("unconfigured chain = %v, want ErrIdentityMismatch", err)
	}

	// A business_object envelope without chain fields is acceptable when the
	// local chain is configured; with chain fields it must match.
	object := validEvent(t, EventTypeWithdrawalRequestReceived)
	objectEnv, err := ParseEnvelope(envelopeForEvent(t, object, 1))
	if err != nil {
		t.Fatalf("parse object envelope: %v", err)
	}
	if err := objectEnv.CheckChainIdentity(1); err != nil {
		t.Fatalf("object envelope without chain identity refused: %v", err)
	}
}

// TestConsumerRevisionEnvelopeValidation covers the T054 revision-field
// validation: a revision-required event MUST carry revises_event_id,
// recovery_version and the chain identity of the revised fact; each missing
// or zero field fails closed with ErrContract (quarantined non_retryable by
// Process), and a parsed revision envelope reports its revision role. The
// FR-08 revival is a distinct, non-revision type and is never conflated with
// created.
func TestConsumerRevisionEnvelopeValidation(t *testing.T) {
	base := envelopeForEvent(t, validEvent(t, EventTypeDepositRevisionApplied), 2)

	cases := []struct {
		name   string
		mutate func(wire map[string]any)
	}{
		{"missing revises_event_id", func(w map[string]any) { delete(w, "revises_event_id") }},
		{"empty revises_event_id", func(w map[string]any) { w["revises_event_id"] = "" }},
		{"missing recovery_version", func(w map[string]any) { delete(w, "recovery_version") }},
		{"zero recovery_version", func(w map[string]any) { w["recovery_version"] = 0 }},
		{"missing chain identity", func(w map[string]any) { delete(w, "block_hash") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var wire map[string]any
			if err := json.Unmarshal(base, &wire); err != nil {
				t.Fatalf("decode base envelope: %v", err)
			}
			tc.mutate(wire)
			body, err := json.Marshal(wire)
			if err != nil {
				t.Fatalf("encode mutated envelope: %v", err)
			}
			if _, err := ParseEnvelope(body); !errors.Is(err, ErrContract) {
				t.Fatalf("ParseEnvelope = %v, want ErrContract", err)
			}
		})
	}

	// The complete revision envelope parses and preserves its revision fields.
	env, err := ParseEnvelope(base)
	if err != nil {
		t.Fatalf("complete revision envelope = %v", err)
	}
	if !env.IsRevision() {
		t.Fatal("revision-required envelope did not report IsRevision")
	}
	if env.RevisesEventID == nil || env.RecoveryVersion == nil || *env.RecoveryVersion != 1 {
		t.Fatalf("revision fields not preserved: revises=%v recovery=%v", env.RevisesEventID, env.RecoveryVersion)
	}

	// The FR-08 revival is not revision-required and never reads as created:
	// distinct catalog type and distinct identity kind.
	revivalEnv, err := ParseEnvelope(envelopeForEvent(t, validEvent(t, EventTypeDepositObservationReinstated), 4))
	if err != nil {
		t.Fatalf("reinstated envelope = %v", err)
	}
	if revivalEnv.IsRevision() {
		t.Fatal("reinstated must not be a revision-required event")
	}
	if !revivalEnv.IsRevival() {
		t.Fatal("reinstated envelope did not report IsRevival")
	}
	if revivalEnv.EventType == EventTypeDepositObservationCreated || revivalEnv.IdentityKind == IdentityKindEVMLog {
		t.Fatalf("revival was conflated with created: %+v", revivalEnv)
	}
}

// TestConsumerVersionGuardDecisions pins the pure guard: no baseline applies,
// newer applies, equal/older skips, a jump beyond max+1 gaps.
func TestConsumerVersionGuardDecisions(t *testing.T) {
	cases := []struct {
		name              string
		found             bool
		maxVersion, event int64
		want              guardDecision
	}{
		{"no baseline applies at any version", false, 0, 7, guardApply},
		{"first event applies", true, 0, 1, guardApply},
		{"newer applies", true, 3, 4, guardApply},
		{"equal skips", true, 4, 4, guardSkip},
		{"older skips", true, 9, 4, guardSkip},
		{"gap at max+2", true, 3, 5, guardGap},
		{"gap far ahead", true, 1, 9, guardGap},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := decideVersionGuard(tc.found, tc.maxVersion, tc.event); got != tc.want {
				t.Fatalf("decideVersionGuard(%v, %d, %d) = %v, want %v",
					tc.found, tc.maxVersion, tc.event, got, tc.want)
			}
		})
	}
}

// TestConsumerGapWaitClosesAndExpires covers the bounded gap wait: it returns
// nil as soon as an earlier version arrives and fails closed when the window
// expires (never an infinite block).
func TestConsumerGapWaitClosesAndExpires(t *testing.T) {
	env := Envelope{AggregateType: "withdrawal_intent", AggregateID: "intent-gap", AggregateVersion: 4}

	closed := &Consumer{
		GapWait: 200 * time.Millisecond,
		Sleep:   func(context.Context, time.Duration) error { return nil },
		ProbeGap: func(context.Context, Envelope) (int64, bool, error) {
			return 3, true, nil
		},
	}
	if err := closed.waitForGap(context.Background(), env); err != nil {
		t.Fatalf("closed gap returned %v, want nil", err)
	}

	open := &Consumer{
		GapWait: 20 * time.Millisecond,
		Sleep:   func(context.Context, time.Duration) error { return nil },
		ProbeGap: func(context.Context, Envelope) (int64, bool, error) {
			return 1, true, nil
		},
	}
	err := open.waitForGap(context.Background(), env)
	if !errors.Is(err, ErrVersionGap) {
		t.Fatalf("open gap returned %v, want ErrVersionGap", err)
	}

	// A missing baseline is never a gap: the first-seen version is the
	// baseline (contracts/consumer.md §3).
	baseline := &Consumer{
		GapWait: 20 * time.Millisecond,
		Sleep:   func(context.Context, time.Duration) error { return nil },
		ProbeGap: func(context.Context, Envelope) (int64, bool, error) {
			return 0, false, nil
		},
	}
	if err := baseline.waitForGap(context.Background(), env); err != nil {
		t.Fatalf("missing baseline returned %v, want nil", err)
	}
}

// TestConsumerQuarantineReasonMapping pins the closed failure-class mapping of
// the parse/identity branches.
func TestConsumerQuarantineReasonMapping(t *testing.T) {
	cases := []struct {
		err  error
		want QuarantineReason
	}{
		{ErrSchemaUnsupported, QuarantineSchemaUnsupported},
		{ErrIdentityMismatch, QuarantineIdentityMismatch},
		{contractErrorf("unknown event_type"), QuarantineNonRetryable},
		{ErrContract, QuarantineNonRetryable},
	}
	for _, tc := range cases {
		got, _ := quarantineReasonFor(tc.err)
		if got != tc.want {
			t.Fatalf("quarantineReasonFor(%v) = %s, want %s", tc.err, got, tc.want)
		}
	}
}

// TestConsumerRetryClassificationAndBackoff covers the bounded retry policy:
// only transient failures retry, contract/unknown failures quarantine, and the
// backoff is bounded by the configured cap.
func TestConsumerRetryClassificationAndBackoff(t *testing.T) {
	if !ConsumerRetryable(Transient(errors.New("broker unavailable"))) {
		t.Fatal("transient wrapper must be retryable")
	}
	if ConsumerRetryable(Permanent(errors.New("serialization failed"))) {
		t.Fatal("permanent wrapper must not be retryable")
	}
	if ConsumerRetryable(ErrContract) {
		t.Fatal("contract failure must not be retryable")
	}
	if ConsumerRetryable(errors.New("unclassified")) {
		t.Fatal("unclassified failures must be non-retryable (fail-closed)")
	}

	base, max := 500*time.Millisecond, 30*time.Second
	previous := time.Duration(0)
	for attempt := 1; attempt <= 12; attempt++ {
		delay := RetryDelay(attempt, base, max, func() float64 { return 0 })
		if delay > max {
			t.Fatalf("attempt %d delay %s exceeds the cap %s", attempt, delay, max)
		}
		if delay < previous {
			t.Fatalf("attempt %d delay %s is below the previous %s", attempt, delay, previous)
		}
		previous = delay
	}
	if got := RetryDelay(1, base, max, func() float64 { return 0 }); got != base {
		t.Fatalf("first delay = %s, want base %s", got, base)
	}
	if got := RetryDelay(3, base, max, func() float64 { return 0 }); got != 2*time.Second {
		t.Fatalf("third delay = %s, want 2s", got)
	}
}

// TestQuarantineIdentityIsDeterministic covers the unparseable-message
// identity: distinct bytes never conflate, identical bytes converge on one
// open entry on redelivery.
func TestQuarantineIdentityIsDeterministic(t *testing.T) {
	a := quarantineIdentity([]byte("{bad json A"))
	b := quarantineIdentity([]byte("{bad json A"))
	c := quarantineIdentity([]byte("{bad json B"))
	if a != b {
		t.Fatalf("identical bytes derived different identities: %s != %s", a, b)
	}
	if a == c {
		t.Fatalf("distinct bytes derived the same identity: %s", a)
	}
	if a == uuid.Nil {
		t.Fatal("unparseable bytes derived the nil identity")
	}
}

// TestConsumerProcessNeverSilentlySkipsWithoutDatabase pins the fail-closed
// shape of Process: a parse failure reaches a durable quarantine attempt
// instead of being dropped. The database is unavailable here, so the error is
// surfaced rather than swallowed.
func TestConsumerProcessNeverSilentlySkipsWithoutDatabase(t *testing.T) {
	consumer := &Consumer{Name: "unit-consumer", GapWait: time.Second, BackoffBase: time.Second, BackoffMax: time.Second, RetryLimit: 2}
	if _, err := consumer.Process(context.Background(), Message{Value: []byte("{not json")}); err == nil {
		t.Fatal("Process without a pool must surface the failure, not report success")
	}
}

// TestConsumerNewValidatesBounds covers the fail-closed constructor: missing
// bounds would mean unbounded behavior and are refused.
func TestConsumerNewValidatesBounds(t *testing.T) {
	effect := EffectFunc(func(context.Context, pgx.Tx, Envelope) error { return nil })
	valid := ConsumerOptions{GapWait: time.Second, BackoffBase: time.Second, BackoffMax: time.Second, RetryLimit: 1, ChainID: 1}

	if _, err := NewConsumer(nil, "c", effect, valid); err == nil {
		t.Fatal("nil pool accepted")
	}
	if _, err := NewConsumer(&pgxpool.Pool{}, "", effect, valid); err == nil {
		t.Fatal("empty consumer name accepted")
	}
	if _, err := NewConsumer(&pgxpool.Pool{}, "c", nil, valid); err == nil {
		t.Fatal("nil effect accepted")
	}
	badBounds := []ConsumerOptions{
		{GapWait: 0, BackoffBase: time.Second, BackoffMax: time.Second, RetryLimit: 1, ChainID: 1},
		{GapWait: time.Second, BackoffBase: 0, BackoffMax: time.Second, RetryLimit: 1, ChainID: 1},
		{GapWait: time.Second, BackoffBase: time.Second, BackoffMax: time.Millisecond, RetryLimit: 1, ChainID: 1},
		{GapWait: time.Second, BackoffBase: time.Second, BackoffMax: time.Second, RetryLimit: 0, ChainID: 1},
		{GapWait: time.Second, BackoffBase: time.Second, BackoffMax: time.Second, RetryLimit: 1, ChainID: 0},
	}
	for i, opts := range badBounds {
		if _, err := NewConsumer(&pgxpool.Pool{}, "c", effect, opts); err == nil {
			t.Fatalf("bad bounds case %d accepted", i)
		}
	}
	if !strings.Contains(ReferenceBoundaryStatement, "not a production ledger") ||
		!strings.Contains(ReferenceBoundaryStatement, "external real ledger") {
		t.Fatalf("boundary statement misses the FR-16 declaration: %q", ReferenceBoundaryStatement)
	}
}
