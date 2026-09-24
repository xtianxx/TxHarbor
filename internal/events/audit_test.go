// audit_test.go is the T035 unit layer: the read-only probe constructor
// guards, the audit constructor validation and the pure watermark
// classification (missing / behind / unverifiable). The database-backed gap
// detection lives in audit_integration_test.go.
package events

import (
	"testing"
)

func TestNewSQLProbeRejectsWritesAndEmptyKind(t *testing.T) {
	if _, err := NewSQLProbe("", "SELECT 1"); err == nil {
		t.Fatal("NewSQLProbe(empty kind) succeeded")
	}
	for _, query := range []string{
		"UPDATE outbox_events SET publish_state = 'published'",
		"DELETE FROM outbox_events",
		"  insert into x values (1)",
		"",
	} {
		if _, err := NewSQLProbe("kind", query); err == nil {
			t.Fatalf("NewSQLProbe accepted a non-SELECT query %q", query)
		}
	}
	probe, err := NewSQLProbe("deposit_observer", "  select source_id, version from t limit $1")
	if err != nil {
		t.Fatalf("NewSQLProbe(valid) error = %v", err)
	}
	if probe.SourceKind() != "deposit_observer" {
		t.Fatalf("SourceKind() = %q", probe.SourceKind())
	}
}

func TestNewReconciliationAuditValidation(t *testing.T) {
	probe, err := NewSQLProbe("kind", "SELECT 1")
	if err != nil {
		t.Fatalf("NewSQLProbe: %v", err)
	}
	if _, err := NewReconciliationAudit([]SourceProbe{probe}, 0, nil); err == nil {
		t.Fatal("NewReconciliationAudit(limit 0) succeeded")
	}
	if _, err := NewReconciliationAudit([]SourceProbe{nil}, 10, nil); err == nil {
		t.Fatal("NewReconciliationAudit(nil probe) succeeded")
	}
	if _, err := NewReconciliationAudit([]SourceProbe{SQLProbe{Kind: " ", Query: "SELECT 1"}}, 10, nil); err == nil {
		t.Fatal("NewReconciliationAudit(empty probe kind) succeeded")
	}
	if _, err := NewReconciliationAudit([]SourceProbe{probe}, 10, nil); err != nil {
		t.Fatalf("NewReconciliationAudit(valid) error = %v", err)
	}
}

// TestClassifyWatermark pins the pure gap decision: no emitted row is a
// "missing" gap; emitted rows without a recorded source version are
// unverifiable (never a fabricated gap); a recorded source version ahead of
// the emitted one is a "behind" gap; equal or older versions are clean.
func TestClassifyWatermark(t *testing.T) {
	mark := SourceWatermark{SourceID: "src-1", SourceVersion: 3}

	gap, isGap, unverifiable := classifyWatermark("kind", mark, 0, nil)
	if !isGap || unverifiable || gap.Reason != "missing" || gap.SourceID != "src-1" || gap.SourceKind != "kind" {
		t.Fatalf("missing decision = (%+v, %v, %v)", gap, isGap, unverifiable)
	}

	if _, isGap, unverifiable := classifyWatermark("kind", mark, 1, nil); isGap || !unverifiable {
		t.Fatalf("unverifiable decision = (isGap=%v, unverifiable=%v)", isGap, unverifiable)
	}

	behind := int64(2)
	gap, isGap, unverifiable = classifyWatermark("kind", mark, 1, &behind)
	if !isGap || unverifiable || gap.Reason != "behind" || gap.SourceVersion != 3 {
		t.Fatalf("behind decision = (%+v, %v, %v)", gap, isGap, unverifiable)
	}

	equal := int64(3)
	if _, isGap, unverifiable := classifyWatermark("kind", mark, 1, &equal); isGap || unverifiable {
		t.Fatal("equal version reported a gap")
	}
	ahead := int64(4)
	if _, isGap, unverifiable := classifyWatermark("kind", mark, 1, &ahead); isGap || unverifiable {
		t.Fatal("emitted version ahead of the source reported a gap")
	}

	// A source without a version is presence-only: emitted rows are clean.
	noVersion := SourceWatermark{SourceID: "src-2", SourceVersion: 0}
	if _, isGap, unverifiable := classifyWatermark("kind", noVersion, 1, nil); isGap || unverifiable {
		t.Fatal("presence-only watermark reported a gap/unverifiable")
	}
	if _, isGap, _ := classifyWatermark("kind", noVersion, 0, nil); !isGap {
		t.Fatal("presence-only watermark with no emitted row did not report a gap")
	}
}
