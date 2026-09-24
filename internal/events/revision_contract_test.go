//go:build contract

// revision_contract_test.go is the B6/T058 contract layer: the revision event
// payload/envelope requirements (superseded_identity, new canonical or
// Orphaned disposition, reason, recovery_version, revises_event_id) are
// asserted field by field on both the producer validator and the consumer
// parser; the two revision types MUST NOT change their semantics in place
// (FR-12); and the catalog v1 declaration/emission closure produces the 8/8
// matrix (merged with T032's five non-revision rows). It runs through
// `make test-contract` (contract tag, no middleware, no Docker; FR-28;
// verification.md §3). Real-database emission conformance is T032 (five
// non-revision types) and T055/T056 (the three revision types); this layer
// pins declarations and producer wiring statically.
package events

import (
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// revisionTypes are the three catalog v1 revision/revival types and their
// required envelope references. The two RevisionRequired types carry
// revises_event_id + recovery_version + the chain identity; the revival type
// is a distinct, non-revision-required type whose payload carries the
// original observation id, the revival basis and the chain identity
// (contracts/events.md §3/§5).
var revisionTypes = []struct {
	eventType        string
	requiredPayload  []string
	revisionRequired bool
}{
	{EventTypeDepositObservationReinstated, []string{"observation_id", "reason"}, false},
	{EventTypeDepositRevisionApplied, []string{"superseded_identity", "to_state", "reason"}, true},
	{EventTypeWithdrawalExecutionRevised, []string{"superseded_identity", "to_state", "reason"}, true},
}

// TestContractRevisionPayloadFields walks every revision type through both
// sides of the contract: a complete envelope passes, and each missing
// revision field (payload keys on the producer, envelope references on the
// producer and consumer) is refused fail-closed.
func TestContractRevisionPayloadFields(t *testing.T) {
	for _, rt := range revisionTypes {
		rt := rt
		t.Run(rt.eventType, func(t *testing.T) {
			spec, ok := LookupEventSpec(rt.eventType)
			if !ok {
				t.Fatalf("catalog has no spec for %s", rt.eventType)
			}
			if spec.RevisionRequired != rt.revisionRequired {
				t.Fatalf("%s revision flag = %v, want %v", rt.eventType, spec.RevisionRequired, rt.revisionRequired)
			}
			for _, key := range rt.requiredPayload {
				if !slices.Contains(spec.RequiredPayloadKeys, key) {
					t.Fatalf("%s required payload keys %v miss %q", rt.eventType, spec.RequiredPayloadKeys, key)
				}
			}

			// Producer side: the complete envelope is valid; each missing
			// payload key or envelope reference refuses.
			base := validEvent(t, rt.eventType)
			base.AggregateVersion = 1
			if err := ValidateEnvelope(base); err != nil {
				t.Fatalf("%s: complete envelope refused: %v", rt.eventType, err)
			}
			for _, key := range rt.requiredPayload {
				ev := cloneEvent(base)
				delete(ev.Payload, key)
				if err := ValidateEnvelope(ev); !errors.Is(err, ErrContract) {
					t.Fatalf("%s: missing payload %q = %v, want ErrContract", rt.eventType, key, err)
				}
			}
			if rt.revisionRequired {
				noRevises := cloneEvent(base)
				noRevises.RevisesEventID = uuid.Nil
				if err := ValidateEnvelope(noRevises); !errors.Is(err, ErrContract) {
					t.Fatalf("%s: missing revises_event_id = %v, want ErrContract", rt.eventType, err)
				}
				noRecovery := cloneEvent(base)
				noRecovery.RecoveryVersion = 0
				if err := ValidateEnvelope(noRecovery); !errors.Is(err, ErrContract) {
					t.Fatalf("%s: missing recovery_version = %v, want ErrContract", rt.eventType, err)
				}
				noChain := cloneEvent(base)
				noChain.BlockHash = ""
				if err := ValidateEnvelope(noChain); !errors.Is(err, ErrContract) {
					t.Fatalf("%s: missing chain identity = %v, want ErrContract", rt.eventType, err)
				}
				// The envelope mirror: revises_event_id always implies a
				// recovery_version (data-model §2 CHECK shape).
				mirror := cloneEvent(base)
				mirror.EventType = EventTypeDepositObservationStatusChanged
				mirror.IdentityKind = IdentityKindBusinessObject
				mirror.Payload = map[string]any{"from_state": "a", "to_state": "b", "reason": "c"}
				mirror.RecoveryVersion = 0
				if err := ValidateEnvelope(mirror); !errors.Is(err, ErrContract) {
					t.Fatalf("revises/recovery mirror accepted a revision without a recovery version: %v", err)
				}
			}

			// Consumer side: the same field requirements on the real parse
			// path (the consumer quarantines these fail-closed).
			wire := envelopeForEvent(t, rt2Event(t, rt.eventType), 2)
			if _, err := ParseEnvelope(wire); err != nil {
				t.Fatalf("%s: complete wire envelope refused: %v", rt.eventType, err)
			}
			if rt.revisionRequired {
				mutations := []struct {
					name   string
					mutate func(map[string]any)
				}{
					{"missing revises_event_id", func(w map[string]any) { delete(w, "revises_event_id") }},
					{"missing recovery_version", func(w map[string]any) { delete(w, "recovery_version") }},
					{"missing chain identity", func(w map[string]any) { delete(w, "block_hash") }},
					{"missing superseded_identity", func(w map[string]any) { delete(w["payload"].(map[string]any), "superseded_identity") }},
					{"missing to_state", func(w map[string]any) { delete(w["payload"].(map[string]any), "to_state") }},
					{"missing reason", func(w map[string]any) { delete(w["payload"].(map[string]any), "reason") }},
				}
				for _, tc := range mutations {
					t.Run(tc.name, func(t *testing.T) {
						if _, err := ParseEnvelope(mutateContractEnvelope(t, wire, tc.mutate)); !errors.Is(err, ErrContract) {
							t.Fatalf("ParseEnvelope = %v, want ErrContract", err)
						}
					})
				}
			}
		})
	}
}

// TestContractRevisionSemanticsFrozen pins the two revision types and the
// revival type to their frozen catalog v1 semantics: changing a required key,
// identity kind or revision flag in place is a catalog contract change that
// needs a new schema version (FR-12).
func TestContractRevisionSemanticsFrozen(t *testing.T) {
	for _, rt := range revisionTypes {
		spec, ok := LookupEventSpec(rt.eventType)
		if !ok {
			t.Fatalf("%s is not routable", rt.eventType)
		}
		if spec.IdentityKind != IdentityKindBusinessObject {
			t.Fatalf("%s identity kind = %s, want business_object", rt.eventType, spec.IdentityKind)
		}
		if len(spec.RequiredPayloadKeys) != len(rt.requiredPayload) {
			t.Fatalf("%s required keys = %v, want %v", rt.eventType, spec.RequiredPayloadKeys, rt.requiredPayload)
		}
		for i := range rt.requiredPayload {
			if spec.RequiredPayloadKeys[i] != rt.requiredPayload[i] {
				t.Fatalf("%s required keys = %v, want %v", rt.eventType, spec.RequiredPayloadKeys, rt.requiredPayload)
			}
		}
		if len(spec.SchemaVersions) != 1 || spec.SchemaVersions[0] != SchemaVersionV1 {
			t.Fatalf("%s schema window = %v, want [1]", rt.eventType, spec.SchemaVersions)
		}
		if _, err := RouteEvent(rt.eventType, SchemaVersionV1+1); !errors.Is(err, ErrSchemaUnsupported) {
			t.Fatalf("%s schema_version 2 route = %v, want ErrSchemaUnsupported", rt.eventType, err)
		}
	}
	// The revival is NOT a revision-required event and NOT a created event:
	// distinct type, distinct identity kind, distinct semantics.
	revival, _ := LookupEventSpec(EventTypeDepositObservationReinstated)
	if revival.RevisionRequired {
		t.Fatal("reinstated must not be revision-required (the dedicated revival semantics carry the original observation)")
	}
	created, _ := LookupEventSpec(EventTypeDepositObservationCreated)
	if created.IdentityKind != IdentityKindEVMLog || revival.IdentityKind == created.IdentityKind {
		t.Fatal("reinstated and created must keep distinct identity kinds (revival of the original vs new source observation)")
	}
}

// TestContractCatalog8of8Closure produces the merged 8/8 catalog matrix: every
// catalog v1 type with its declaration and the producer emission site in the
// repository. The static wiring check complements T032/T055/T056, which prove
// the real database emission for the same types.
func TestContractCatalog8of8Closure(t *testing.T) {
	wiring := map[string]struct{ file, call string }{
		EventTypeDepositObservationCreated:       {"../indexer/depositcommit.go", "appendDepositObservationCreatedEvent(ctx, tx"},
		EventTypeDepositObservationStatusChanged: {"../indexer/reorgcommit.go", "appendDepositObservationStatusChangedEvent(ctx, tx"},
		EventTypeDepositObservationReinstated:    {"../indexer/reorgcommit.go", "appendDepositObservationReinstatedEvent(ctx, tx"},
		EventTypeDepositConfirmationConfirmed:    {"../indexer/confirmcommit.go", "appendDepositConfirmationConfirmedEvent(ctx, tx"},
		EventTypeDepositRevisionApplied:          {"../indexer/reorgcommit.go", "appendDepositRevisionAppliedEvent(ctx, tx"},
		EventTypeWithdrawalRequestReceived:       {"../withdrawal/intake.go", "appendWithdrawalRequestReceivedEvent(ctx, tx"},
		EventTypeWithdrawalExecutionStateChanged: {"../execution/intent.go", "appendIntentStateChangedEvent(ctx, tx"},
		EventTypeWithdrawalExecutionRevised:      {"../execution/revision.go", "appendWithdrawalExecutionRevisedEvent(ctx, tx"},
	}
	bodies := map[string]string{}
	readFile := func(path string) string {
		if body, ok := bodies[path]; ok {
			return body
		}
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		bodies[path] = string(body)
		return bodies[path]
	}

	catalog := CatalogV1()
	if len(catalog) != 8 {
		t.Fatalf("catalog v1 declares %d types, want 8", len(catalog))
	}
	for _, spec := range catalog {
		site, ok := wiring[spec.Type]
		if !ok {
			t.Fatalf("catalog type %s has no producer wiring entry", spec.Type)
		}
		if !strings.Contains(readFile(site.file), site.call) {
			t.Fatalf("%s has no emission call %q", site.file, site.call)
		}
		t.Logf("matrix 8/8 type=%s identity=%s revision=%v schema_version=%d producer=%s",
			spec.Type, spec.IdentityKind, spec.RevisionRequired, spec.SchemaVersions[0], site.file)
	}
	// The three B6 revision/revival types are exactly the ones this contract
	// layer covers; the five non-revision types are T032's matrix.
	if len(revisionTypes) != 3 {
		t.Fatalf("revision type count = %d, want 3", len(revisionTypes))
	}
}

// rt2Event builds a catalog-valid event for a revision type with the complete
// envelope references (the wire shape the consumer must accept).
func rt2Event(t *testing.T, eventType string) Event {
	t.Helper()
	ev := validEvent(t, eventType)
	ev.AggregateVersion = 2
	return ev
}
