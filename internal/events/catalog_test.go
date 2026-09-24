package events

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// validEvent builds a catalog-valid event for any catalog v1 type. Aggregate
// version stays zero (the Append-derived form).
func validEvent(t *testing.T, eventType string) Event {
	t.Helper()
	spec, ok := LookupEventSpec(eventType)
	if !ok {
		t.Fatalf("event type %q is not in catalog v1", eventType)
	}
	payload := make(map[string]any, len(spec.RequiredPayloadKeys))
	for _, key := range spec.RequiredPayloadKeys {
		payload[key] = "value"
	}
	ev := Event{
		EventType:     eventType,
		SchemaVersion: SchemaVersionV1,
		IdentityKind:  spec.IdentityKind,
		AggregateType: "deposit_observation",
		AggregateID:   "obs-1",
		Payload:       payload,
		OccurredAt:    time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
	}
	if spec.IdentityKind == IdentityKindEVMLog {
		ev.ChainID = 1
		ev.BlockNumber = 100
		ev.BlockHash = "0xblock"
		ev.TxHash = "0xtx"
		ev.LogIndex = 0
	}
	if spec.RevisionRequired {
		ev.ChainID = 1
		ev.BlockNumber = 100
		ev.BlockHash = "0xblock"
		ev.RecoveryVersion = 1
		ev.RevisesEventID = uuid.New()
	}
	return ev
}

func mustNewEvent(t *testing.T, ev Event) Event {
	t.Helper()
	prepared, err := NewEvent(ev)
	if err != nil {
		t.Fatalf("NewEvent(%s) error = %v", ev.EventType, err)
	}
	return prepared
}

// cloneEvent copies an event so table-driven mutations never share the payload
// map with the base case.
func cloneEvent(ev Event) Event {
	if ev.Payload != nil {
		payload := make(map[string]any, len(ev.Payload))
		for key, value := range ev.Payload {
			payload[key] = value
		}
		ev.Payload = payload
	}
	return ev
}

func TestCatalogV1Declaration(t *testing.T) {
	want := []EventSpec{
		{
			Type:                EventTypeDepositObservationCreated,
			IdentityKind:        IdentityKindEVMLog,
			RequiredPayloadKeys: []string{"observation_id", "state"},
			SchemaVersions:      []int{SchemaVersionV1},
		},
		{
			Type:                EventTypeDepositObservationStatusChanged,
			IdentityKind:        IdentityKindBusinessObject,
			RequiredPayloadKeys: []string{"from_state", "to_state", "reason"},
			SchemaVersions:      []int{SchemaVersionV1},
		},
		{
			Type:                EventTypeDepositObservationReinstated,
			IdentityKind:        IdentityKindBusinessObject,
			RequiredPayloadKeys: []string{"observation_id", "reason"},
			SchemaVersions:      []int{SchemaVersionV1},
		},
		{
			Type:                EventTypeDepositConfirmationConfirmed,
			IdentityKind:        IdentityKindBusinessObject,
			RequiredPayloadKeys: []string{"policy_version", "confirmed_block_number", "confirmed_block_hash"},
			SchemaVersions:      []int{SchemaVersionV1},
		},
		{
			Type:                EventTypeDepositRevisionApplied,
			IdentityKind:        IdentityKindBusinessObject,
			RequiredPayloadKeys: []string{"superseded_identity", "to_state", "reason"},
			RevisionRequired:    true,
			SchemaVersions:      []int{SchemaVersionV1},
		},
		{
			Type:                EventTypeWithdrawalRequestReceived,
			IdentityKind:        IdentityKindBusinessObject,
			RequiredPayloadKeys: []string{"request_id", "caller", "state"},
			SchemaVersions:      []int{SchemaVersionV1},
		},
		{
			Type:                EventTypeWithdrawalExecutionStateChanged,
			IdentityKind:        IdentityKindBusinessObject,
			RequiredPayloadKeys: []string{"from_state", "to_state", "intent_id"},
			SchemaVersions:      []int{SchemaVersionV1},
		},
		{
			Type:                EventTypeWithdrawalExecutionRevised,
			IdentityKind:        IdentityKindBusinessObject,
			RequiredPayloadKeys: []string{"superseded_identity", "to_state", "reason"},
			RevisionRequired:    true,
			SchemaVersions:      []int{SchemaVersionV1},
		},
	}
	got := CatalogV1()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CatalogV1() = %#v, want %#v", got, want)
	}
	if len(got) != 8 {
		t.Fatalf("catalog v1 has %d types, want 8 (contracts/events.md §3)", len(got))
	}
	if CatalogVersionV1 != 1 {
		t.Fatalf("CatalogVersionV1 = %d, want 1", CatalogVersionV1)
	}
	seen := map[string]bool{}
	for _, spec := range got {
		if seen[spec.Type] {
			t.Fatalf("duplicate catalog type %q", spec.Type)
		}
		seen[spec.Type] = true
		if !spec.IdentityKind.Valid() {
			t.Fatalf("type %s has invalid identity kind %q", spec.Type, spec.IdentityKind)
		}
		if len(spec.RequiredPayloadKeys) == 0 {
			t.Fatalf("type %s declares no required payload keys", spec.Type)
		}
		if !spec.SupportsSchemaVersion(SchemaVersionV1) {
			t.Fatalf("type %s does not declare schema version 1", spec.Type)
		}
		for _, key := range spec.RequiredPayloadKeys {
			if isForbiddenPayloadKey(key) {
				t.Fatalf("type %s requires forbidden payload key %q", spec.Type, key)
			}
		}
	}

	// CatalogV1 returns copies: mutating the result must not corrupt the
	// frozen table.
	got[0].RequiredPayloadKeys[0] = "mutated"
	got[0].SchemaVersions[0] = 99
	fresh := CatalogV1()
	if fresh[0].RequiredPayloadKeys[0] != "observation_id" || fresh[0].SchemaVersions[0] != SchemaVersionV1 {
		t.Fatalf("CatalogV1() leaked mutable state: %#v", fresh[0])
	}
}

func TestCatalogLookupAndRoute(t *testing.T) {
	for _, spec := range CatalogV1() {
		got, ok := LookupEventSpec(spec.Type)
		if !ok || !reflect.DeepEqual(got, spec) {
			t.Fatalf("LookupEventSpec(%s) = (%#v, %v), want the declaration", spec.Type, got, ok)
		}
		routed, ok := ResolveRoute(spec.Type, SchemaVersionV1)
		if !ok || routed.Type != spec.Type {
			t.Fatalf("ResolveRoute(%s, 1) = (%#v, %v), want the declaration", spec.Type, routed, ok)
		}
		if _, ok := ResolveRoute(spec.Type, SchemaVersionV1+1); ok {
			t.Fatalf("ResolveRoute(%s, 2) resolved an undeclared version", spec.Type)
		}
		if _, err := RouteEvent(spec.Type, SchemaVersionV1); err != nil {
			t.Fatalf("RouteEvent(%s, 1) error = %v", spec.Type, err)
		}
		if _, err := RouteEvent(spec.Type, SchemaVersionV1+1); !errors.Is(err, ErrSchemaUnsupported) {
			t.Fatalf("RouteEvent(%s, 2) error = %v, want ErrSchemaUnsupported", spec.Type, err)
		}
	}
	if _, ok := LookupEventSpec("deposit.unknown.fact"); ok {
		t.Fatal("LookupEventSpec resolved an unknown type")
	}
	if _, ok := ResolveRoute("deposit.unknown.fact", 1); ok {
		t.Fatal("ResolveRoute resolved an unknown type")
	}
	if _, err := RouteEvent("deposit.unknown.fact", 1); !errors.Is(err, ErrContract) {
		t.Fatalf("RouteEvent(unknown) error = %v, want ErrContract", err)
	}
}

func TestEventFamily(t *testing.T) {
	cases := map[string]string{
		EventTypeDepositObservationCreated:       "deposit",
		EventTypeWithdrawalExecutionStateChanged: "withdrawal",
		"malformed":                              "malformed",
		"":                                       "",
	}
	for eventType, want := range cases {
		if got := EventFamily(eventType); got != want {
			t.Fatalf("EventFamily(%q) = %q, want %q", eventType, got, want)
		}
	}
}

func TestEventIdentityDerivation(t *testing.T) {
	logKey := EVMLogNaturalKey(1, "0xblock", "0xtx", 3)
	if logKey != "evm_log|1|0xblock|0xtx|3" {
		t.Fatalf("EVMLogNaturalKey() = %q", logKey)
	}
	objectKey := BusinessObjectNaturalKey("withdrawal_request", "req-1", 2)
	if objectKey != "business_object|withdrawal_request|req-1|2" {
		t.Fatalf("BusinessObjectNaturalKey() = %q", objectKey)
	}

	id := NewEventID(logKey)
	if want := uuid.NewSHA1(uuid.NameSpaceURL, []byte(logKey)); id != want {
		t.Fatalf("NewEventID(%q) = %s, want UUIDv5 %s", logKey, id, want)
	}
	if NewEventID(logKey) != id {
		t.Fatal("NewEventID is not deterministic for the same natural key")
	}
	if NewEventID(objectKey) == id {
		t.Fatal("different natural keys derived the same event_id")
	}
	if NewEventID(BusinessObjectNaturalKey("withdrawal_request", "req-1", 3)) == NewEventID(objectKey) {
		t.Fatal("business object version must participate in the event identity")
	}
	if NewEventID(EVMLogNaturalKey(1, "0xblock", "0xtx", 3)) != id {
		t.Fatal("evm_log identity must not depend on call context, only the triple")
	}
}

func TestCanonicalPayloadAndHash(t *testing.T) {
	a := map[string]any{
		"b": 2,
		"a": map[string]any{"y": []any{int64(1), int64(2)}, "x": "s"},
	}
	b := map[string]any{
		"a": map[string]any{"x": "s", "y": []any{int64(1), int64(2)}},
		"b": 2,
	}
	ba, err := CanonicalPayload(a)
	if err != nil {
		t.Fatalf("CanonicalPayload(a) error = %v", err)
	}
	bb, err := CanonicalPayload(b)
	if err != nil {
		t.Fatalf("CanonicalPayload(b) error = %v", err)
	}
	if !bytes.Equal(ba, bb) {
		t.Fatalf("canonical bytes differ for equal payloads:\n%s\n%s", ba, bb)
	}
	if bytes.Contains(ba, []byte(" ")) {
		t.Fatalf("canonical payload carries whitespace: %s", ba)
	}
	hash := PayloadHash(ba)
	if len(hash) != 64 || strings.ToLower(hash) != hash {
		t.Fatalf("PayloadHash() = %q, want 64 lowercase hex characters", hash)
	}
	if PayloadHash(bb) != hash {
		t.Fatal("PayloadHash is not stable for equal payloads")
	}
	different, err := CanonicalPayload(map[string]any{"b": 3, "a": map[string]any{"x": "s", "y": []any{int64(1), int64(2)}}})
	if err != nil {
		t.Fatalf("CanonicalPayload(different) error = %v", err)
	}
	if PayloadHash(different) == hash {
		t.Fatal("different payloads hashed identically")
	}
	if _, err := CanonicalPayload(nil); !errors.Is(err, ErrContract) {
		t.Fatalf("CanonicalPayload(nil) error = %v, want ErrContract", err)
	}
}

func TestForbiddenPayloadScan(t *testing.T) {
	forbidden := []map[string]any{
		{"private_key": "0xdeadbeef"},
		{"nested": map[string]any{"apiKey": "abc"}},
		{"list": []any{map[string]any{"mnemonic": "words"}}},
		{"signed_tx": "0x00"},
		{"blob": "-----BEGIN PRIVATE KEY-----"},
	}
	for _, payload := range forbidden {
		if _, found := ForbiddenPayload(payload); !found {
			t.Fatalf("ForbiddenPayload(%v) found nothing", payload)
		}
	}
	allowed := []map[string]any{
		{"block_hash": "0xabc", "tx_hash": "0xdef", "log_index": 3},
		{"attempt_ref": "attempt-1", "superseded_identity": map[string]any{"event_id": "x"}},
		{"observation_id": "obs-1", "state": "pending", "reason": "reorg"},
	}
	for _, payload := range allowed {
		if key, found := ForbiddenPayload(payload); found {
			t.Fatalf("ForbiddenPayload(%v) rejected allowed key %q", payload, key)
		}
	}
}

func TestValidateEnvelopeMatrix(t *testing.T) {
	for _, spec := range CatalogV1() {
		ev := validEvent(t, spec.Type)
		ev.AggregateVersion = 1
		if err := ValidateEnvelope(ev); err != nil {
			t.Fatalf("ValidateEnvelope(%s) error = %v", spec.Type, err)
		}
	}

	base := validEvent(t, EventTypeDepositObservationCreated)
	base.AggregateVersion = 1
	mutations := []struct {
		name   string
		mutate func(*Event)
		want   error
	}{
		{"unknown type", func(ev *Event) { ev.EventType = "nope.nope.nope" }, ErrContract},
		{"unsupported schema", func(ev *Event) { ev.SchemaVersion = 2 }, ErrContract},
		{"wrong identity kind", func(ev *Event) { ev.IdentityKind = IdentityKindBusinessObject }, ErrContract},
		{"empty aggregate id", func(ev *Event) { ev.AggregateID = "" }, ErrContract},
		{"zero aggregate version", func(ev *Event) { ev.AggregateVersion = 0 }, ErrContract},
		{"zero occurred_at", func(ev *Event) { ev.OccurredAt = time.Time{} }, ErrContract},
		{"nil payload", func(ev *Event) { ev.Payload = nil }, ErrContract},
		{"missing required payload key", func(ev *Event) { delete(ev.Payload, "state") }, ErrContract},
		{"nil required payload value", func(ev *Event) { ev.Payload["state"] = nil }, ErrContract},
		{"forbidden payload key", func(ev *Event) { ev.Payload["private_key"] = "x" }, ErrContract},
		{"evm_log missing chain identity", func(ev *Event) { ev.ChainID = 0 }, ErrContract},
		{"evm_log negative log index", func(ev *Event) { ev.LogIndex = -1 }, ErrContract},
	}
	for _, tc := range mutations {
		ev := cloneEvent(base)
		tc.mutate(&ev)
		if err := ValidateEnvelope(ev); !errors.Is(err, tc.want) {
			t.Fatalf("%s: ValidateEnvelope error = %v, want %v", tc.name, err, tc.want)
		}
	}

	revision := validEvent(t, EventTypeDepositRevisionApplied)
	revision.AggregateVersion = 2
	if err := ValidateEnvelope(revision); err != nil {
		t.Fatalf("ValidateEnvelope(revision) error = %v", err)
	}
	noRevises := revision
	noRevises.RevisesEventID = uuid.Nil
	if err := ValidateEnvelope(noRevises); !errors.Is(err, ErrContract) {
		t.Fatalf("revision without revises_event_id error = %v, want ErrContract", err)
	}
	noRecovery := revision
	noRecovery.RecoveryVersion = 0
	if err := ValidateEnvelope(noRecovery); !errors.Is(err, ErrContract) {
		t.Fatalf("revision without recovery_version error = %v, want ErrContract", err)
	}
	noChain := revision
	noChain.ChainID = 0
	if err := ValidateEnvelope(noChain); !errors.Is(err, ErrContract) {
		t.Fatalf("chain-derived revision without chain identity error = %v, want ErrContract", err)
	}

	// A non-revision event carrying revises_event_id must still carry the
	// recovery version (outbox revision shape).
	statusChanged := validEvent(t, EventTypeDepositObservationStatusChanged)
	statusChanged.AggregateVersion = 2
	statusChanged.RevisesEventID = uuid.New()
	if err := ValidateEnvelope(statusChanged); !errors.Is(err, ErrContract) {
		t.Fatalf("revises_event_id without recovery_version error = %v, want ErrContract", err)
	}
}

func TestNewEventFreezesCanonicalPayload(t *testing.T) {
	ev := mustNewEvent(t, validEvent(t, EventTypeDepositObservationCreated))
	if len(ev.PayloadBytes()) == 0 {
		t.Fatal("NewEvent did not freeze canonical payload bytes")
	}
	if len(ev.PayloadHash()) != 64 {
		t.Fatalf("NewEvent payload hash = %q, want 64 hex characters", ev.PayloadHash())
	}
	frozen := string(ev.PayloadBytes())
	ev.Payload["state"] = "mutated-after-freeze"
	if string(ev.PayloadBytes()) != frozen {
		t.Fatal("payload bytes changed after NewEvent froze them")
	}
	if ev.PayloadHash() != PayloadHash([]byte(frozen)) {
		t.Fatal("payload hash no longer matches the frozen bytes")
	}

	// prepare() canonicalizes struct-literal events that skipped NewEvent.
	raw := validEvent(t, EventTypeDepositObservationCreated)
	prepared, err := raw.prepare()
	if err != nil {
		t.Fatalf("prepare() error = %v", err)
	}
	if string(prepared.PayloadBytes()) != frozen {
		t.Fatalf("prepare() bytes = %s, want %s", prepared.PayloadBytes(), frozen)
	}
}
