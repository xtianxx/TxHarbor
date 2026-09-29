package recovery

import (
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

const (
	testAuthorityDSN = "postgres://authority:secret@db.internal:5432/production?sslmode=disable"
	testControlDSN   = "postgres://control:secret@db.internal:5432/control?sslmode=disable"
	testIsolatedDSN  = "postgres://verifier:secret@db.internal:5432/verification?sslmode=disable"
)

func TestBindIsolatedTargetSameEndpointDistinctDatabase(t *testing.T) {
	binding, err := BindIsolatedTarget(testAuthorityDSN, testControlDSN, testIsolatedDSN, nil)
	if err != nil {
		t.Fatal(err)
	}
	if binding.BoundDSN() != testIsolatedDSN {
		t.Fatal("binding did not retain configured isolated DSN")
	}
	if err := AssertIsolatedTarget(binding, testIsolatedDSN); err != nil {
		t.Fatalf("configured target assertion rejected: %v", err)
	}
}

func TestBindIsolatedTargetRejectsEqualAuthorityDatabase(t *testing.T) {
	_, err := BindIsolatedTarget(testAuthorityDSN, testControlDSN,
		"postgres://verifier:secret@db.internal:5432/production?sslmode=disable", nil)
	if err == nil {
		t.Fatal("equal authoritative/isolated database accepted")
	}
}

func TestBindIsolatedTargetRejectsControlDatabase(t *testing.T) {
	_, err := BindIsolatedTarget(testAuthorityDSN, testControlDSN,
		"postgres://verifier:secret@db.internal:5432/control?sslmode=disable", nil)
	if err == nil || !strings.Contains(err.Error(), "control-store") {
		t.Fatalf("control database error = %v", err)
	}
}

func TestAssertIsolatedTargetRejectsRoleMismatch(t *testing.T) {
	binding, err := BindIsolatedTarget(testAuthorityDSN, testControlDSN, testIsolatedDSN, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = AssertIsolatedTarget(binding,
		"postgres://different-role:secret@db.internal:5432/verification?sslmode=disable")
	if err == nil || !strings.Contains(err.Error(), "endpoint and role") {
		t.Fatalf("role mismatch error = %v", err)
	}
}

func TestBindIsolatedTargetRejectsOpenInstanceCollision(t *testing.T) {
	target, err := controlstore.ParseDSNTarget(testIsolatedDSN)
	if err != nil {
		t.Fatal(err)
	}
	key, err := controlstore.TargetGuardKey(target)
	if err != nil {
		t.Fatal(err)
	}
	_, err = BindIsolatedTarget(testAuthorityDSN, testControlDSN, testIsolatedDSN, []IsolatedInstanceBinding{{
		TargetGuardKey: key, TargetRoleFingerprint: target.DataTargetFingerprint().RoleFingerprint,
	}})
	if err == nil || !strings.Contains(err.Error(), "collides") {
		t.Fatalf("collision error = %v", err)
	}
}

func TestBindIsolatedTargetRejectsLegacyNullOpenInstance(t *testing.T) {
	_, err := BindIsolatedTarget(testAuthorityDSN, testControlDSN, testIsolatedDSN, []IsolatedInstanceBinding{{}})
	if err == nil || !strings.Contains(err.Error(), "legacy-null") {
		t.Fatalf("legacy binding error = %v", err)
	}
}

func TestBindIsolatedTargetRejectsDifferentHostAliasRisk(t *testing.T) {
	_, err := BindIsolatedTarget(testAuthorityDSN, testControlDSN,
		"postgres://verifier:secret@db-alias.internal:5432/verification?sslmode=disable", nil)
	if err == nil || !strings.Contains(err.Error(), "aliases") {
		t.Fatalf("host alias error = %v", err)
	}
}

func TestIsolatedTargetDoesNotExposeSecretInFingerprints(t *testing.T) {
	binding, err := BindIsolatedTarget(testAuthorityDSN, testControlDSN, testIsolatedDSN, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{binding.TargetFingerprint(), binding.TargetGuardKey(), binding.RoleFingerprint()} {
		if strings.Contains(value, "secret") || strings.Contains(value, "verifier") {
			t.Fatalf("fingerprint contains DSN material: %q", value)
		}
	}
}
