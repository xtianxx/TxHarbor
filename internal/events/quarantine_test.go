// quarantine_test.go is the T042 unit layer: the quarantine status machine,
// the snapshot discipline, the replay scope validation and the deterministic
// operation-id derivation. The transactional quarantine/replay paths
// (persistent isolation, audited replay, operation-id convergence) run in the
// T048/T049 integration layer.
package events

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestQuarantineStatusTransitions pins the state machine: only open moves,
// and only to replayed/superseded.
func TestQuarantineStatusTransitions(t *testing.T) {
	if !QuarantineStatusOpen.CanTransition(QuarantineStatusReplayed) {
		t.Fatal("open -> replayed must be allowed")
	}
	if !QuarantineStatusOpen.CanTransition(QuarantineStatusSuperseded) {
		t.Fatal("open -> superseded must be allowed")
	}
	for _, from := range []QuarantineStatus{QuarantineStatusOpen, QuarantineStatusReplayed, QuarantineStatusSuperseded} {
		for _, to := range []QuarantineStatus{QuarantineStatusOpen, QuarantineStatusReplayed, QuarantineStatusSuperseded} {
			allowed := from == QuarantineStatusOpen && (to == QuarantineStatusReplayed || to == QuarantineStatusSuperseded)
			if got := from.CanTransition(to); got != allowed {
				t.Fatalf("CanTransition(%s, %s) = %v, want %v", from, to, got, allowed)
			}
		}
	}
	if QuarantineStatus("unknown").Valid() {
		t.Fatal("unknown quarantine status reported valid")
	}
}

// TestQuarantineReasonsAreClosed pins the failure-class vocabulary to the
// migration CHECK (data-model Table 5).
func TestQuarantineReasonsAreClosed(t *testing.T) {
	want := []QuarantineReason{
		QuarantineRetryExhausted, QuarantineNonRetryable, QuarantineVersionGap,
		QuarantineSchemaUnsupported, QuarantineIdentityMismatch,
	}
	got := QuarantineReasons()
	if len(got) != len(want) {
		t.Fatalf("quarantine reasons = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("quarantine reasons = %v, want %v", got, want)
		}
	}
	if QuarantineReason("made_up").Valid() {
		t.Fatal("unknown reason reported valid")
	}
}

// TestQuarantineSnapshotPreservesBytes covers the snapshot discipline: a valid
// JSON envelope is stored byte-identically (replays feed the same bytes) and
// unparseable bytes are preserved forensically.
func TestQuarantineSnapshotPreservesBytes(t *testing.T) {
	raw := []byte(`{"event_id":"x","event_type":"deposit.observation.status_changed"}`)
	got := quarantineSnapshot(raw, Envelope{}, "parse failed")
	if string(got) != string(raw) {
		t.Fatalf("snapshot = %s, want the exact delivered bytes %s", got, raw)
	}

	bad := []byte("{not json")
	forensic := quarantineSnapshot(bad, Envelope{}, "envelope is not valid JSON")
	var decoded map[string]any
	if err := json.Unmarshal(forensic, &decoded); err != nil {
		t.Fatalf("forensic snapshot is not JSON: %v", err)
	}
	if decoded["parse_error"] == nil || decoded["raw_hex"] == nil {
		t.Fatalf("forensic snapshot misses the raw bytes or the parse error: %s", forensic)
	}
	if !strings.Contains(decoded["raw_hex"].(string), "7b6e6f74206a736f6e") {
		t.Fatalf("forensic raw_hex does not carry the delivered bytes: %s", forensic)
	}
}

// TestQuarantineEnqueueValidates covers the fail-closed enqueue guards; the
// persistent path runs against a real database in T048.
func TestQuarantineEnqueueValidates(t *testing.T) {
	store := &QuarantineStore{}
	if _, _, err := store.Enqueue(context.Background(), nil, QuarantineEntry{}); err == nil {
		t.Fatal("enqueue without a pool/tx accepted")
	}
	withTx := &QuarantineStore{}
	if _, _, err := withTx.Enqueue(context.Background(), nil, QuarantineEntry{
		ConsumerName: "c", FailureClass: QuarantineNonRetryable, EventSnapshot: []byte(`{}`),
	}); err == nil {
		t.Fatal("enqueue without a transaction accepted")
	}
}

// TestReplayScopeValidation covers the fail-closed scope shapes.
func TestReplayScopeValidation(t *testing.T) {
	valid := []ReplayScope{
		{Kind: ReplayScopeEventIDs, EventIDs: []uuid.UUID{uuid.New()}},
		{Kind: ReplayScopeAggregate, AggregateType: "withdrawal_intent", AggregateID: "i1"},
		{Kind: ReplayScopeTimeRange, From: time.Now().Add(-time.Hour), To: time.Now()},
		{Kind: ReplayScopeOffsetRange, Topic: "t", Partition: 0, FromOffset: 1, ToOffset: 2},
	}
	for _, scope := range valid {
		if err := scope.Validate(); err != nil {
			t.Fatalf("valid scope %+v refused: %v", scope, err)
		}
	}
	invalid := []ReplayScope{
		{Kind: ReplayScopeEventIDs},
		{Kind: ReplayScopeAggregate, AggregateType: "x"},
		{Kind: ReplayScopeTimeRange},
		{Kind: ReplayScopeTimeRange, From: time.Now(), To: time.Now().Add(-time.Hour)},
		{Kind: ReplayScopeOffsetRange, Topic: "", Partition: 0, FromOffset: 0, ToOffset: 1},
		{Kind: ReplayScopeOffsetRange, Topic: "t", Partition: -1, FromOffset: 0, ToOffset: 1},
		{Kind: ReplayScopeOffsetRange, Topic: "t", Partition: 0, FromOffset: 5, ToOffset: 1},
		{Kind: "unknown"},
	}
	for _, scope := range invalid {
		if err := scope.Validate(); err == nil {
			t.Fatalf("invalid scope %+v accepted", scope)
		}
	}
}

// TestDeriveOperationIDIsDeterministic pins the CLI idempotency key: identical
// parameters converge, any parameter change derives a new operation.
func TestDeriveOperationIDIsDeterministic(t *testing.T) {
	a := deriveOperationID("replay", "consumer", "scope", "reason", "operator")
	b := deriveOperationID("replay", "consumer", "scope", "reason", "operator")
	if a != b {
		t.Fatalf("operation ids differ for identical parameters: %s != %s", a, b)
	}
	if !strings.HasPrefix(a, "replay-") {
		t.Fatalf("operation id %q does not carry its kind", a)
	}
	c := deriveOperationID("replay", "consumer", "scope", "reason", "other-operator")
	if a == c {
		t.Fatal("operation id does not change with the operator")
	}
}

// TestReplayOptionsFailClosed covers the operator guards that must refuse
// before any event is touched.
func TestReplayOptionsFailClosed(t *testing.T) {
	consumer := &Consumer{Name: "ref"}
	if _, err := Replay(context.Background(), nil, consumer, ReplayOptions{}); err == nil {
		t.Fatal("replay without a pool accepted")
	}
	if _, err := Replay(context.Background(), nil, nil, ReplayOptions{}); err == nil {
		t.Fatal("replay without a consumer accepted")
	}
	if _, err := Replay(context.Background(), &pgxpool.Pool{}, consumer, ReplayOptions{
		ConsumerName: "other", Scope: ReplayScope{Kind: ReplayScopeEventIDs, EventIDs: []uuid.UUID{uuid.New()}},
		Reason: "r", Operator: "o",
	}); err == nil {
		t.Fatal("replay for a consumer that is not wired accepted")
	}
	if _, err := Replay(context.Background(), &pgxpool.Pool{}, consumer, ReplayOptions{
		ConsumerName: "ref", Scope: ReplayScope{Kind: ReplayScopeEventIDs, EventIDs: []uuid.UUID{uuid.New()}},
		Reason: "", Operator: "o",
	}); err == nil {
		t.Fatal("replay without a reason accepted")
	}
	if _, err := Replay(context.Background(), &pgxpool.Pool{}, consumer, ReplayOptions{
		ConsumerName: "ref", Scope: ReplayScope{Kind: ReplayScopeEventIDs, EventIDs: []uuid.UUID{uuid.New()}},
		Reason: "r", Operator: "",
	}); err == nil {
		t.Fatal("replay without an operator accepted")
	}
}
