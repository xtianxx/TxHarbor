package recovery

import (
	"context"
	"strings"
	"testing"
)

func TestExecuteVerifyBackupRejectsCallerTargetWithoutOpaqueBinding(t *testing.T) {
	_, err := ExecuteVerifyBackup(context.Background(), VerifyBackupOptions{
		ManifestPath: "unused.json", TargetDSN: testIsolatedDSN,
	})
	if err == nil || !strings.Contains(err.Error(), "opaque isolated target binding") {
		t.Fatalf("expected missing opaque binding refusal, got %v", err)
	}
}

func TestExecuteVerifyBackupRejectsWrongTargetAssertion(t *testing.T) {
	binding, err := BindIsolatedTarget(testAuthorityDSN, testControlDSN, testIsolatedDSN, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ExecuteVerifyBackup(context.Background(), VerifyBackupOptions{
		ManifestPath: "unused.json", Binding: binding,
		AuthoritativeDSN: testAuthorityDSN, ControlDSN: testControlDSN, ObserverDSN: testIsolatedDSN,
		TargetDSN: "postgres://other:secret@db.internal:5432/verify?sslmode=disable",
	})
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("expected target assertion mismatch refusal, got %v", err)
	}
}
