// store_unit_test.go covers the pure logic of the control store: the schema
// version guard decisions (T069), DSN target identity/fingerprints, the
// credential-free data_target rule and request canonicalization. Database
// behavior (row locks, idempotency, commit-order reads, audit) is covered by
// store_integration_test.go against a real PostgreSQL.
package controlstore

import (
	"strings"
	"testing"
)

func TestSchemaStateCheckCompatible(t *testing.T) {
	known := func() SchemaState {
		return SchemaState{
			VersionTable: true,
			Known:        []int64{1},
			Target:       1,
			Applied:      []int64{1},
			Current:      1,
		}
	}

	t.Run("at target passes", func(t *testing.T) {
		if err := known().CheckCompatible(); err != nil {
			t.Fatalf("expected compatible, got %v", err)
		}
	})

	t.Run("missing version table refuses", func(t *testing.T) {
		state := known()
		state.VersionTable = false
		state.Applied = nil
		state.Current = 0
		state.Pending = []int64{1}
		err := state.CheckCompatible()
		if err == nil {
			t.Fatal("expected refusal for missing version table")
		}
		if !IsControlStoreUnavailable(err) {
			t.Fatalf("expected control_store_unavailable, got %v", err)
		}
		if !strings.Contains(err.Error(), "version table is missing") {
			t.Fatalf("refusal must name the missing version table: %v", err)
		}
	})

	t.Run("unknown version refuses with observed version", func(t *testing.T) {
		state := known()
		state.Applied = []int64{999}
		state.Current = 999
		state.Unknown = []int64{999}
		state.Pending = nil
		err := state.CheckCompatible()
		if err == nil {
			t.Fatal("expected refusal for unknown version")
		}
		if !IsControlStoreUnavailable(err) {
			t.Fatalf("expected control_store_unavailable, got %v", err)
		}
		if !strings.Contains(err.Error(), "observed_version=999") || !strings.Contains(err.Error(), "target_version=1") {
			t.Fatalf("refusal must carry observed/target versions: %v", err)
		}
	})

	t.Run("newer than known refuses", func(t *testing.T) {
		state := known()
		state.Applied = []int64{2}
		state.Current = 2
		state.Unknown = []int64{2}
		state.Pending = nil
		err := state.CheckCompatible()
		if err == nil || !IsControlStoreUnavailable(err) {
			t.Fatalf("expected control_store_unavailable for a newer version, got %v", err)
		}
	})

	t.Run("pending migration is not compatible", func(t *testing.T) {
		state := SchemaState{VersionTable: true, Known: []int64{1}, Target: 1, Pending: []int64{1}}
		err := state.CheckCompatible()
		if err == nil || !IsControlStoreUnavailable(err) {
			t.Fatalf("expected control_store_unavailable for pending migrations, got %v", err)
		}
		if !strings.Contains(err.Error(), "not at this binary's version") {
			t.Fatalf("refusal must explain the pending state: %v", err)
		}
	})
}

func TestSchemaStateCheckMigratable(t *testing.T) {
	t.Run("pristine empty database is migratable", func(t *testing.T) {
		state := SchemaState{Known: []int64{1}, Target: 1, Pending: []int64{1}}
		if err := state.CheckMigratable(); err != nil {
			t.Fatalf("expected pristine database to be migratable, got %v", err)
		}
	})

	t.Run("known applied prefix with pending is migratable", func(t *testing.T) {
		state := SchemaState{
			VersionTable: true, Known: []int64{1, 2}, Target: 2,
			Applied: []int64{1}, Current: 1, Pending: []int64{2},
		}
		if err := state.CheckMigratable(); err != nil {
			t.Fatalf("expected known prefix to be migratable, got %v", err)
		}
	})

	t.Run("unknown version refuses", func(t *testing.T) {
		state := SchemaState{
			VersionTable: true, Known: []int64{1}, Target: 1,
			Applied: []int64{999}, Current: 999, Unknown: []int64{999}, Pending: []int64{1},
		}
		err := state.CheckMigratable()
		if err == nil || !IsControlStoreUnavailable(err) {
			t.Fatalf("expected control_store_unavailable, got %v", err)
		}
	})

	t.Run("recovery objects without version table refuse", func(t *testing.T) {
		state := SchemaState{
			Known: []int64{1}, Target: 1, Pending: []int64{1},
			RecoveryObjects: []string{"recovery_instance", "recovery_audit"},
		}
		err := state.CheckMigratable()
		if err == nil || !IsControlStoreUnavailable(err) {
			t.Fatalf("expected control_store_unavailable, got %v", err)
		}
		if !strings.Contains(err.Error(), "without a goose_db_version version table") {
			t.Fatalf("refusal must name the unknown state: %v", err)
		}
	})

	t.Run("newer than known refuses", func(t *testing.T) {
		state := SchemaState{VersionTable: true, Known: []int64{1}, Target: 1, Applied: []int64{2}, Current: 2}
		if err := state.CheckMigratable(); err == nil || !IsControlStoreUnavailable(err) {
			t.Fatalf("expected control_store_unavailable, got %v", err)
		}
	})
}

func TestParseDSNTargetSameDatabaseAndFingerprint(t *testing.T) {
	target, err := ParseDSNTarget("postgres://ops:supersecret@db.example:5433/txharbor_control?sslmode=disable")
	if err != nil {
		t.Fatalf("parse url dsn: %v", err)
	}
	if target.Host != "db.example" || target.Port != 5433 || target.Database != "txharbor_control" || target.Role != "ops" {
		t.Fatalf("unexpected target: %+v", target)
	}

	keyword, err := ParseDSNTarget("host=db.example port=5433 user=ops password=another dbname=txharbor_control sslmode=disable")
	if err != nil {
		t.Fatalf("parse keyword dsn: %v", err)
	}
	if !target.SameDatabase(keyword) {
		t.Fatalf("same host/port/database with different credentials must compare equal: %+v vs %+v", target, keyword)
	}

	otherDB, err := ParseDSNTarget("postgres://ops:supersecret@db.example:5433/txharbor_data?sslmode=disable")
	if err != nil {
		t.Fatalf("parse other db: %v", err)
	}
	if target.SameDatabase(otherDB) {
		t.Fatal("different database names must not compare equal")
	}
	otherPort, _ := ParseDSNTarget("postgres://ops@db.example:5434/txharbor_control")
	if target.SameDatabase(otherPort) {
		t.Fatal("different ports must not compare equal")
	}

	fp := target.DataTargetFingerprint()
	if fp.Kind != "pg" {
		t.Fatalf("kind = %q, want pg", fp.Kind)
	}
	if fp.DatabaseFingerprint != target.DataTargetFingerprint().DatabaseFingerprint ||
		fp.TargetFingerprint != keyword.DataTargetFingerprint().TargetFingerprint {
		t.Fatal("fingerprints must be deterministic for equal targets")
	}
	if otherDB.DataTargetFingerprint().DatabaseFingerprint == fp.DatabaseFingerprint {
		t.Fatal("different databases must fingerprint differently")
	}
	for _, field := range []string{fp.DatabaseFingerprint, fp.RoleFingerprint, fp.TargetFingerprint} {
		for _, secret := range []string{"supersecret", "db.example", "txharbor_control", "ops"} {
			if strings.Contains(field, secret) {
				t.Fatalf("fingerprint %q leaks %q", field, secret)
			}
		}
		if !strings.HasPrefix(field, "sha256:") {
			t.Fatalf("fingerprint %q must be a sha256 digest", field)
		}
	}

	if _, err := ParseDSNTarget("   "); err == nil {
		t.Fatal("blank DSN must be refused")
	}
}

func TestValidateDataTarget(t *testing.T) {
	if err := ValidateDataTarget(nil); err != nil {
		t.Fatalf("absent data_target is allowed: %v", err)
	}
	ok := []byte(`{"kind":"pg","database_fingerprint":"sha256:aa","role_fingerprint":"sha256:bb","target_fingerprint":"sha256:cc"}`)
	if err := ValidateDataTarget(ok); err != nil {
		t.Fatalf("fingerprint payload must be accepted: %v", err)
	}
	refused := map[string][]byte{
		"plain dsn field":     []byte(`{"dsn":"postgres://u:p@h/db"}`),
		"database_url":        []byte(`{"database_url":"postgres://u:p@h/db"}`),
		"nested password":     []byte(`{"a":{"password":"hunter2"}}`),
		"dsn-like value":      []byte(`{"note":"postgres://u:p@h/db"}`),
		"credential-less dsn": []byte(`{"note":"postgres://localhost//db"}`),
		"password value":      []byte(`{"note":"password=hunter2"}`),
		"array credential":    []byte(`[{"pgpassword":"x"}]`),
		"scalar payload":      []byte(`"postgres://u:p@h/db"`),
		"invalid json":        []byte(`{`),
	}
	for name, payload := range refused {
		if err := ValidateDataTarget(payload); err == nil {
			t.Fatalf("%s must be refused", name)
		}
	}
}

func TestReleaseDecisionCanonical(t *testing.T) {
	instanceID := "11111111-1111-4111-8111-111111111111"
	id1 := "22222222-2222-4222-8222-222222222222"
	id2 := "33333333-3333-4333-8333-333333333333"
	req := ReleaseDecisionRequest{
		InstanceID: instanceID, Capability: "query", ScopeHash: "scope-1", Decision: "release",
		EvidenceGeneration: 2, EvidenceHash: "sha256:abc",
		ApprovalRefs: []string{id2, id1, id2}, Actor: "deploy:ops", OperationID: "op-1",
	}
	canonical, err := req.canonical()
	if err != nil {
		t.Fatalf("canonical release: %v", err)
	}
	if len(canonical.ApprovalRefs) != 2 || canonical.ApprovalRefs[0] != id1 || canonical.ApprovalRefs[1] != id2 {
		t.Fatalf("approval_refs must be sorted and de-duplicated: %v", canonical.ApprovalRefs)
	}

	bad := []ReleaseDecisionRequest{
		{InstanceID: "not-a-uuid", Capability: "query", ScopeHash: "s", Decision: "release", EvidenceHash: "h", Actor: "a", OperationID: "o"},
		{InstanceID: instanceID, Capability: "mint_money", ScopeHash: "s", Decision: "release", EvidenceHash: "h", Actor: "a", OperationID: "o"},
		{InstanceID: instanceID, Capability: "query", ScopeHash: "", Decision: "release", EvidenceHash: "h", Actor: "a", OperationID: "o"},
		{InstanceID: instanceID, Capability: "query", ScopeHash: "s", Decision: "approved", EvidenceHash: "h", Actor: "a", OperationID: "o"},
		{InstanceID: instanceID, Capability: "query", ScopeHash: "s", Decision: "release", EvidenceGeneration: -1, EvidenceHash: "h", Actor: "a", OperationID: "o"},
		{InstanceID: instanceID, Capability: "query", ScopeHash: "s", Decision: "release", EvidenceHash: "", Actor: "a", OperationID: "o"},
		{InstanceID: instanceID, Capability: "query", ScopeHash: "s", Decision: "release", EvidenceHash: "h", Actor: "", OperationID: "o"},
		{InstanceID: instanceID, Capability: "query", ScopeHash: "s", Decision: "release", EvidenceHash: "h", Actor: "a"},
		{InstanceID: instanceID, Capability: "query", ScopeHash: "s", Decision: "release", EvidenceHash: "h", Actor: "a", OperationID: "o", ApprovalRefs: []string{"nope"}},
	}
	for i, candidate := range bad {
		if _, err := candidate.canonical(); err == nil {
			t.Fatalf("bad release request %d must be refused", i)
		}
	}
}

func TestApprovalDecisionCanonical(t *testing.T) {
	instanceID := "11111111-1111-4111-8111-111111111111"
	req := ApprovalDecisionRequest{
		InstanceID: instanceID, Capability: "new_withdrawal_creation", ScopeHash: "scope-1",
		Decision: "approve", ApprovalClassSnapshot: "dual_non_executor",
		Principal: "deploy:alice", PersonID: "person-1", EvidenceGeneration: 1,
		EvidenceHash: "sha256:abc", OperationID: "op-1",
	}
	if _, err := req.canonical(); err != nil {
		t.Fatalf("canonical approval: %v", err)
	}
	bad := []ApprovalDecisionRequest{
		{InstanceID: instanceID, Capability: "query", ScopeHash: "s", Decision: "approve", ApprovalClassSnapshot: "single_non_executor", Principal: "Alice", PersonID: "p", EvidenceHash: "h", OperationID: "o"},
		{InstanceID: instanceID, Capability: "query", ScopeHash: "s", Decision: "approve", ApprovalClassSnapshot: "single_non_executor", Principal: "deploy:alice", PersonID: "", EvidenceHash: "h", OperationID: "o"},
		{InstanceID: instanceID, Capability: "query", ScopeHash: "s", Decision: "approve", ApprovalClassSnapshot: "anyone", Principal: "deploy:alice", PersonID: "p", EvidenceHash: "h", OperationID: "o"},
		{InstanceID: instanceID, Capability: "query", ScopeHash: "s", Decision: "release", ApprovalClassSnapshot: "single_non_executor", Principal: "deploy:alice", PersonID: "p", EvidenceHash: "h", OperationID: "o"},
	}
	for i, candidate := range bad {
		if _, err := candidate.canonical(); err == nil {
			t.Fatalf("bad approval request %d must be refused", i)
		}
	}
}
