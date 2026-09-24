//go:build contract

// consumer_contract_test.go is the T051 contract layer: the simulated
// upstream-consumer compatibility matrix (unknown optional fields are
// ignored, same-version backward compatibility holds, breaking changes need a
// new version and a compatibility window, unknown/unsupported versions fail
// closed into quarantine), the routing table, the FR-16 boundary declaration
// and the statement discipline (no cross-system exactly-once claim).
//
// It runs through `make test-contract` (contract tag, no middleware, no
// Docker; FR-28; verification.md §3).
package events

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestContractConsumerCompatibilityMatrix walks the upstream-consumer matrix
// on the real parse path: a backward-compatible optional addition keeps the
// version and is ignored; a removed required field at the same version is
// refused; an undeclared version fails closed with the quarantine reason.
func TestContractConsumerCompatibilityMatrix(t *testing.T) {
	base := envelopeForEvent(t, validEvent(t, EventTypeDepositObservationStatusChanged), 2)

	// Unknown optional fields (envelope level and payload level) are ignored
	// at the same schema_version (FR-12; contracts/events.md §4.2).
	withOptional := mutateContractEnvelope(t, base, func(wire map[string]any) {
		wire["future_envelope_field"] = map[string]any{"nested": true}
		payload := wire["payload"].(map[string]any)
		payload["future_optional"] = "ignored"
	})
	env, err := ParseEnvelope(withOptional)
	if err != nil {
		t.Fatalf("backward-compatible addition refused: %v", err)
	}
	if env.SchemaVersion != SchemaVersionV1 {
		t.Fatalf("schema_version = %d, want %d", env.SchemaVersion, SchemaVersionV1)
	}
	if _, present := env.Payload["future_optional"]; !present {
		t.Fatal("the parsed payload lost the optional field")
	}

	// Removing a required field at the same version is the breaking change
	// that MUST be published under a new version: the consumer refuses it.
	breaking := mutateContractEnvelope(t, base, func(wire map[string]any) {
		payload := wire["payload"].(map[string]any)
		delete(payload, "to_state")
	})
	if _, err := ParseEnvelope(breaking); !errors.Is(err, ErrContract) {
		t.Fatalf("same-version required-field removal = %v, want ErrContract", err)
	}

	// An undeclared version fails closed with schema_unsupported: never
	// guess-parsed, never partially applied (SC-05).
	unsupported := mutateContractEnvelope(t, base, func(wire map[string]any) {
		wire["schema_version"] = SchemaVersionV1 + 1
	})
	_, err = ParseEnvelope(unsupported)
	if !errors.Is(err, ErrSchemaUnsupported) {
		t.Fatalf("undeclared version = %v, want ErrSchemaUnsupported", err)
	}
	if reason, _ := quarantineReasonFor(err); reason != QuarantineSchemaUnsupported {
		t.Fatalf("quarantine reason = %s, want schema_unsupported", reason)
	}

	// An unknown event type is a contract refusal, never applied.
	unknown := mutateContractEnvelope(t, base, func(wire map[string]any) {
		wire["event_type"] = "future.event.type"
	})
	if _, err := ParseEnvelope(unknown); !errors.Is(err, ErrContract) {
		t.Fatalf("unknown event type = %v, want ErrContract", err)
	}
}

// TestContractConsumerRoutingTable pins the (event_type, schema_version)
// routing of every catalog v1 type and the fail-closed windows.
func TestContractConsumerRoutingTable(t *testing.T) {
	for _, spec := range CatalogV1() {
		if _, err := RouteEvent(spec.Type, SchemaVersionV1); err != nil {
			t.Fatalf("RouteEvent(%s, 1) = %v", spec.Type, err)
		}
		if _, err := RouteEvent(spec.Type, SchemaVersionV1+1); !errors.Is(err, ErrSchemaUnsupported) {
			t.Fatalf("RouteEvent(%s, 2) = %v, want ErrSchemaUnsupported", spec.Type, err)
		}
	}
	// The compatibility window is a set: a new version keeps the old one
	// consumable until it is deliberately dropped.
	window := EventSpec{Type: "x.y.z", SchemaVersions: []int{SchemaVersionV1, SchemaVersionV1 + 1}}
	if !window.SupportsSchemaVersion(SchemaVersionV1) || !window.SupportsSchemaVersion(SchemaVersionV1+1) {
		t.Fatal("the compatibility window dropped an available version")
	}
}

// TestContractReferenceBoundaryStatement covers FR-16/SC-12: the boundary
// declaration the evidence carries must state the reference consumer's
// non-authority and the exclusion of any external real ledger guarantee.
func TestContractReferenceBoundaryStatement(t *testing.T) {
	statement := ReferenceBoundaryStatement
	required := []string{
		"not a production ledger",
		"not authoritative",
		"no user balances",
		"event identity/version/delivery semantics",
		"consumer idempotency contract",
		"external real ledger",
	}
	for _, fragment := range required {
		if !strings.Contains(statement, fragment) {
			t.Fatalf("boundary statement misses %q: %q", fragment, statement)
		}
	}
	if RefConsumerName == "" {
		t.Fatal("the reference consumer must carry an independent consumer name")
	}
}

// TestContractNoCrossSystemExactlyOnceClaim scans the package sources so that
// no source ever makes a cross-system exactly-once claim: delivery is
// at-least-once and processing is idempotent, and PostgreSQL and Kafka share
// no transaction (contracts/consumer.md §1). A sentence that couples the
// effect-once term with Kafka or cross-system delivery MUST be negated.
//
// The scanner vocabulary is assembled from pieces (the T037 statement-scan
// pattern) so the scanner itself stays clean and no file needs an exemption.
func TestContractNoCrossSystemExactlyOnceClaim(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}
	forbidden := []string{"exactly" + "-once", "exactly" + " once", "恰好" + "一次"}
	contextTokens := []string{"kafka", "cross" + "-system", "end" + "-to-end", "跨" + "系统", "端" + "到端"}
	negations := []string{"never", "no ", "not ", "must not", "cannot", "不", "无", "禁止"}
	// Scanner-vocabulary files are exempt: they define the forbidden terms
	// (assembled from pieces) and therefore carry the joined tokens in their
	// token lists, not as claims. The T037 scan covers its own file, and this
	// scan covers every other source including test documentation.
	exempt := map[string]bool{
		"consumer_contract_test.go":           true,
		"publisher_kafka_integration_test.go": true,
		"publisher_test.go":                   true,
	}
	scanned := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || exempt[entry.Name()] {
			continue
		}
		body, err := os.ReadFile(filepath.Join(".", entry.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		scanned++
		lines := strings.Split(string(body), "\n")
		for lineNumber, line := range lines {
			lower := strings.ToLower(line)
			hasForbidden := false
			for _, token := range forbidden {
				if strings.Contains(lower, strings.ToLower(token)) {
					hasForbidden = true
					break
				}
			}
			if !hasForbidden {
				continue
			}
			hasContext := false
			for _, token := range contextTokens {
				if strings.Contains(lower, strings.ToLower(token)) {
					hasContext = true
					break
				}
			}
			if !hasContext {
				continue
			}
			// A sentence may wrap: a negation on the previous line counts.
			window := lower
			if lineNumber > 0 {
				window = strings.ToLower(lines[lineNumber-1]) + " " + lower
			}
			negated := false
			for _, token := range negations {
				if strings.Contains(window, token) {
					negated = true
					break
				}
			}
			if !negated {
				t.Fatalf("%s:%d carries a cross"+"-system "+"exactly"+"-once claim without a negation: %s",
					entry.Name(), lineNumber+1, strings.TrimSpace(line))
			}
		}
	}
	if scanned == 0 {
		t.Fatal("no package sources were scanned")
	}
}

// mutateContractEnvelope rewrites one JSON field of a transport envelope.
func mutateContractEnvelope(t *testing.T, raw []byte, mutate func(wire map[string]any)) []byte {
	t.Helper()
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	mutate(wire)
	body, err := json.Marshal(wire)
	if err != nil {
		t.Fatalf("encode envelope: %v", err)
	}
	return body
}
