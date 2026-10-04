//go:build integration

package recovery

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// Exercise the public persistence paths against PostgreSQL. These checks are
// deliberately about absence of writes, not just the shared validator.
func TestGapCredentialCanariesRejectBeforePersistence(t *testing.T) {
	f, _, gaps := gapFixture(t)
	gap := gapOpenValid(t, f, gaps, "credential-canary-gap", []Capability{CapabilityQuery})
	var auditsBefore, generationBefore int64
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM recovery_audit WHERE instance_id=$1`, f.instanceID).Scan(&auditsBefore); err != nil {
		t.Fatal(err)
	}
	if err := f.pool.QueryRow(f.ctx, `SELECT evidence_generation FROM recovery_instance WHERE instance_id=$1`, f.instanceID).Scan(&generationBefore); err != nil {
		t.Fatal(err)
	}

	const canary = "pg-credential-canary-7f08d2"
	secretText := "postgres://operator:" + canary + "@db/recovery"
	closeCases := []CloseGapRequest{
		{Actor: secretText},
		{OperationID: secretText},
		{Reason: secretText},
		{ClosureEvidence: []byte(`{"password":"` + canary + `"}`)},
	}
	for _, candidate := range closeCases {
		req := CloseGapRequest{InstanceID: f.instanceID, GapID: gap.GapID, Actor: "deploy:operator", OperationID: "canary-close", ClosureEvidence: []byte(`{"receipt":"public-reference"}`), Reason: "verified operational evidence"}
		if candidate.Actor != "" {
			req.Actor = candidate.Actor
		}
		if candidate.OperationID != "" {
			req.OperationID = candidate.OperationID
		}
		if candidate.Reason != "" {
			req.Reason = candidate.Reason
		}
		if candidate.ClosureEvidence != nil {
			req.ClosureEvidence = candidate.ClosureEvidence
		}
		_, err := gaps.Close(f.ctx, req)
		assertGapCanaryRejected(t, "close", err, canary)
	}

	escalateCases := []EscalateGapRequest{
		{Actor: secretText}, {OperationID: secretText}, {EscalationRef: secretText}, {Reason: secretText},
	}
	for _, candidate := range escalateCases {
		req := EscalateGapRequest{InstanceID: f.instanceID, GapID: gap.GapID, Actor: "deploy:operator", OperationID: "canary-escalate", EscalationRef: "ticket-123", Reason: "external review requested"}
		if candidate.Actor != "" {
			req.Actor = candidate.Actor
		}
		if candidate.OperationID != "" {
			req.OperationID = candidate.OperationID
		}
		if candidate.EscalationRef != "" {
			req.EscalationRef = candidate.EscalationRef
		}
		if candidate.Reason != "" {
			req.Reason = candidate.Reason
		}
		_, err := gaps.Escalate(f.ctx, req)
		assertGapCanaryRejected(t, "escalate", err, canary)
	}

	noteCases := []GapNoteRequest{{Actor: secretText}, {OperationID: secretText}, {Reason: secretText}}
	for _, candidate := range noteCases {
		req := GapNoteRequest{InstanceID: f.instanceID, GapID: gap.GapID, Kind: GapNoteAcknowledged, Actor: "deploy:operator", OperationID: "canary-note", Reason: "operator acknowledged"}
		if candidate.Actor != "" {
			req.Actor = candidate.Actor
		}
		if candidate.OperationID != "" {
			req.OperationID = candidate.OperationID
		}
		if candidate.Reason != "" {
			req.Reason = candidate.Reason
		}
		_, err := gaps.Note(f.ctx, req)
		assertGapCanaryRejected(t, "note", err, canary)
	}

	// Also hit the shared evidence-audit sink directly; JSON detail, actor and
	// operation id are all durable fields on this authoritative boundary.
	for _, rec := range []controlstore.AuditRecord{
		{Actor: secretText, Action: "credential_canary", Result: controlstore.AuditOK},
		{Actor: "deploy:operator", Action: "credential_canary", OperationID: secretText, Result: controlstore.AuditOK},
		{Actor: "deploy:operator", Action: "credential_canary", Detail: []byte(`{"reason":"` + secretText + `"}`), Result: controlstore.AuditOK},
	} {
		err := controlstore.WriteAudit(f.ctx, f.pool, rec)
		assertGapCanaryRejected(t, "audit", err, canary)
	}

	var auditsAfter, generationAfter int64
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM recovery_audit WHERE instance_id=$1`, f.instanceID).Scan(&auditsAfter); err != nil {
		t.Fatal(err)
	}
	if err := f.pool.QueryRow(f.ctx, `SELECT evidence_generation FROM recovery_instance WHERE instance_id=$1`, f.instanceID).Scan(&generationAfter); err != nil {
		t.Fatal(err)
	}
	if auditsAfter != auditsBefore || generationAfter != generationBefore {
		t.Fatalf("rejected credential inputs mutated persistence: audits %d -> %d; generation %d -> %d", auditsBefore, auditsAfter, generationBefore, generationAfter)
	}
	var persisted int64
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM recovery_audit WHERE instance_id=$1 AND (coalesce(target::text,'') LIKE '%'||$2||'%' OR coalesce(detail::text,'') LIKE '%'||$2||'%' OR actor LIKE '%'||$2||'%' OR coalesce(operation_id,'') LIKE '%'||$2||'%')`, f.instanceID, canary).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != 0 {
		t.Fatalf("credential canary found in %d audit rows", persisted)
	}

	// Positive operational references and hashed evidence remain accepted.
	if err := controlstore.ValidateCredentialJSON("evidence", mustJSON(t, map[string]any{"status": "authorization_recheck", "evidence_token": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "reference": "ticket-123"})); err != nil {
		t.Fatalf("ordinary operational evidence rejected: %v", err)
	}
}

func assertGapCanaryRejected(t *testing.T, operation string, err error, canary string) {
	t.Helper()
	if err == nil || strings.Contains(err.Error(), canary) {
		t.Fatalf("%s accepted or echoed credential canary: %v", operation, err)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
