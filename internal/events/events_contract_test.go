//go:build contract

// events_contract_test.go is the T014 contract layer: envelope required
// fields, the two identity natural keys, schema_version compatibility and
// routing, payload minimization/forbidden-field scanning and the frozen
// catalog v1 declaration. It runs through `make test-contract` (contract tag,
// no middleware, no Docker; FR-28; verification.md §3) and is one member of
// the T009 x T014 x T015 x T016 merge gate.
package events

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestContractCatalogV1IsFrozen pins the catalog declaration to the
// contracts/events.md §3 table. Changing a type, kind, required payload key or
// compatibility window here is a catalog contract change and needs a new
// schema version (FR-12).
func TestContractCatalogV1IsFrozen(t *testing.T) {
	want := map[string]struct {
		kind     IdentityKind
		required []string
		revision bool
	}{
		EventTypeDepositObservationCreated:       {IdentityKindEVMLog, []string{"observation_id", "state"}, false},
		EventTypeDepositObservationStatusChanged: {IdentityKindBusinessObject, []string{"from_state", "to_state", "reason"}, false},
		EventTypeDepositObservationReinstated:    {IdentityKindBusinessObject, []string{"observation_id", "reason"}, false},
		EventTypeDepositConfirmationConfirmed:    {IdentityKindBusinessObject, []string{"policy_version", "confirmed_block_number", "confirmed_block_hash"}, false},
		EventTypeDepositRevisionApplied:          {IdentityKindBusinessObject, []string{"superseded_identity", "to_state", "reason"}, true},
		EventTypeWithdrawalRequestReceived:       {IdentityKindBusinessObject, []string{"request_id", "caller", "state"}, false},
		EventTypeWithdrawalExecutionStateChanged: {IdentityKindBusinessObject, []string{"from_state", "to_state", "intent_id"}, false},
		EventTypeWithdrawalExecutionRevised:      {IdentityKindBusinessObject, []string{"superseded_identity", "to_state", "reason"}, true},
	}
	catalog := CatalogV1()
	if len(catalog) != len(want) {
		t.Fatalf("catalog v1 has %d types, want %d", len(catalog), len(want))
	}
	for _, spec := range catalog {
		expected, ok := want[spec.Type]
		if !ok {
			t.Fatalf("catalog declares unexpected type %q", spec.Type)
		}
		if spec.IdentityKind != expected.kind {
			t.Fatalf("%s identity_kind = %s, want %s", spec.Type, spec.IdentityKind, expected.kind)
		}
		if len(spec.RequiredPayloadKeys) != len(expected.required) {
			t.Fatalf("%s required keys = %v, want %v", spec.Type, spec.RequiredPayloadKeys, expected.required)
		}
		for i := range expected.required {
			if spec.RequiredPayloadKeys[i] != expected.required[i] {
				t.Fatalf("%s required keys = %v, want %v", spec.Type, spec.RequiredPayloadKeys, expected.required)
			}
		}
		if spec.RevisionRequired != expected.revision {
			t.Fatalf("%s revision flag = %v, want %v", spec.Type, spec.RevisionRequired, expected.revision)
		}
		if len(spec.SchemaVersions) != 1 || spec.SchemaVersions[0] != SchemaVersionV1 {
			t.Fatalf("%s schema window = %v, want [1]", spec.Type, spec.SchemaVersions)
		}
	}
}

// TestContractEnvelopeRequiredFields walks the envelope matrix: every catalog
// type has a valid envelope, and each required field or identity invariant is
// individually enforced.
func TestContractEnvelopeRequiredFields(t *testing.T) {
	for _, spec := range CatalogV1() {
		ev := validEvent(t, spec.Type)
		ev.AggregateVersion = 1
		if err := ValidateEnvelope(ev); err != nil {
			t.Fatalf("%s: valid envelope rejected: %v", spec.Type, err)
		}
	}

	base := validEvent(t, EventTypeDepositObservationCreated)
	base.AggregateVersion = 1
	cases := []struct {
		name   string
		mutate func(*Event)
	}{
		{"unknown event type", func(ev *Event) { ev.EventType = "unknown" }},
		{"schema_version zero", func(ev *Event) { ev.SchemaVersion = 0 }},
		{"schema_version outside window", func(ev *Event) { ev.SchemaVersion = SchemaVersionV1 + 1 }},
		{"identity kind mismatch", func(ev *Event) { ev.IdentityKind = IdentityKindBusinessObject }},
		{"missing aggregate_type", func(ev *Event) { ev.AggregateType = "" }},
		{"missing aggregate_id", func(ev *Event) { ev.AggregateID = "" }},
		{"missing aggregate_version", func(ev *Event) { ev.AggregateVersion = 0 }},
		{"missing occurred_at", func(ev *Event) { ev.OccurredAt = time.Time{} }},
		{"missing payload", func(ev *Event) { ev.Payload = nil }},
		{"missing chain_id", func(ev *Event) { ev.ChainID = 0 }},
		{"missing block_hash", func(ev *Event) { ev.BlockHash = "" }},
		{"missing tx_hash", func(ev *Event) { ev.TxHash = "" }},
	}
	for _, tc := range cases {
		ev := cloneEvent(base)
		tc.mutate(&ev)
		if err := ValidateEnvelope(ev); !errors.Is(err, ErrContract) {
			t.Fatalf("%s: ValidateEnvelope = %v, want ErrContract", tc.name, err)
		}
	}
}

// TestContractIdentityNaturalKeys covers the two documented natural keys and
// the UUIDv5 derivation (FR-09; contracts/events.md §2).
func TestContractIdentityNaturalKeys(t *testing.T) {
	logKey := EVMLogNaturalKey(56, "0xblock", "0xtx", 7)
	if logKey != "evm_log|56|0xblock|0xtx|7" {
		t.Fatalf("evm_log natural key = %q", logKey)
	}
	objectKey := BusinessObjectNaturalKey("withdrawal_intent", "intent-9", 4)
	if objectKey != "business_object|withdrawal_intent|intent-9|4" {
		t.Fatalf("business_object natural key = %q", objectKey)
	}
	if NewEventID(logKey) != uuid.NewSHA1(uuid.NameSpaceURL, []byte(logKey)) {
		t.Fatal("evm_log event_id is not the UUIDv5 of its natural key")
	}
	if NewEventID(objectKey) != uuid.NewSHA1(uuid.NameSpaceURL, []byte(objectKey)) {
		t.Fatal("business_object event_id is not the UUIDv5 of its natural key")
	}
	if NewEventID(logKey) == NewEventID(objectKey) {
		t.Fatal("the two identity kinds must not collide")
	}
	if NewEventID(objectKey) == NewEventID(BusinessObjectNaturalKey("withdrawal_intent", "intent-9", 5)) {
		t.Fatal("business object version must participate in the identity")
	}
}

// TestContractSchemaVersionCompatibility covers FR-12: backward-compatible
// additions keep the version, unknown fields are ignored, undeclared versions
// are fail-closed, and a compatibility window can hold several versions.
func TestContractSchemaVersionCompatibility(t *testing.T) {
	// Unknown optional fields are ignored: the payload still validates.
	ev := validEvent(t, EventTypeDepositObservationStatusChanged)
	ev.AggregateVersion = 2
	ev.Payload["optional_new_field"] = map[string]any{"future": true}
	if err := ValidateEnvelope(ev); err != nil {
		t.Fatalf("payload with an unknown optional field rejected: %v", err)
	}

	// Removing a required field at the same version is the breaking change
	// the version guard refuses (a new version would be required).
	delete(ev.Payload, "to_state")
	if err := ValidateEnvelope(ev); !errors.Is(err, ErrContract) {
		t.Fatalf("payload missing a required field = %v, want ErrContract", err)
	}

	// Every catalog type routes at schema_version 1 and refuses undeclared
	// versions fail-closed.
	for _, spec := range CatalogV1() {
		if _, err := RouteEvent(spec.Type, SchemaVersionV1); err != nil {
			t.Fatalf("RouteEvent(%s, 1) error = %v", spec.Type, err)
		}
		if _, err := RouteEvent(spec.Type, SchemaVersionV1+1); !errors.Is(err, ErrSchemaUnsupported) {
			t.Fatalf("RouteEvent(%s, 2) error = %v, want ErrSchemaUnsupported", spec.Type, err)
		}
	}

	// The compatibility window is a set: adding a version keeps the old one
	// routable until it is deliberately dropped.
	window := EventSpec{Type: "x.y.z", SchemaVersions: []int{SchemaVersionV1, SchemaVersionV1 + 1}}
	if !window.SupportsSchemaVersion(1) || !window.SupportsSchemaVersion(2) || window.SupportsSchemaVersion(3) {
		t.Fatalf("compatibility window semantics broken: %v", window.SchemaVersions)
	}
}

// TestContractPayloadMinimization scans for forbidden payload material
// (data-model §9; research R18) and pins the canonical hash shape.
func TestContractPayloadMinimization(t *testing.T) {
	for _, spec := range CatalogV1() {
		for _, key := range spec.RequiredPayloadKeys {
			if isForbiddenPayloadKey(key) {
				t.Fatalf("%s requires forbidden payload key %q", spec.Type, key)
			}
		}
	}

	forbidden := []map[string]any{
		{"private_key": "x"},
		{"privateKey": "x"},
		{"private-key": "x"},
		{"mnemonic": "x"},
		{"api_key": "x"},
		{"credential": "x"},
		{"signature": "x"},
		{"signed_tx": "x"},
		{"nested": map[string]any{"raw_tx": "x"}},
		{"list": []any{map[string]any{"seed_phrase": "x"}}},
		{"pem": "-----BEGIN EC PRIVATE KEY-----"},
	}
	for _, payload := range forbidden {
		if key, found := ForbiddenPayload(payload); !found {
			t.Fatalf("forbidden payload %v passed the scan", payload)
		} else if key == "" {
			t.Fatalf("forbidden payload %v reported an empty key", payload)
		}
	}
	allowed := []map[string]any{
		{"block_hash": "0xabc", "tx_hash": "0xdef", "log_index": 1},
		{"superseded_identity": map[string]any{"event_id": "x", "block_hash": "0xabc"}},
		{"attempt_ref": "attempt-1", "from_state": "broadcast", "to_state": "unknown"},
	}
	for _, payload := range allowed {
		if key, found := ForbiddenPayload(payload); found {
			t.Fatalf("allowed payload %v rejected on %q", payload, key)
		}
	}

	canonical, err := CanonicalPayload(map[string]any{"a": 1, "b": []any{"x"}})
	if err != nil {
		t.Fatalf("CanonicalPayload error = %v", err)
	}
	hash := PayloadHash(canonical)
	if len(hash) != 64 || strings.Trim(hash, "0123456789abcdef") != "" {
		t.Fatalf("PayloadHash = %q, want 64 lowercase hex characters", hash)
	}
}
