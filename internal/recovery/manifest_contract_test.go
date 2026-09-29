//go:build contract

// manifest_contract_test.go is T014: the manifest contract layer (tags:
// contract; no Docker; FR-001-FR-004/FR-006/FR-036; contracts/backup-manifest.md
// §1-§5; data-model.md §2/§4; ADR-002).
//
// It pins, case by case, the rules that B6/T017 must implement in
// internal/recovery/manifest.go:
//
//   - missing required field / unknown manifest_version / digest mismatch ->
//     refused; artifacts[] entries must carry path/bytes/sha256;
//   - verification.state != verified is never usable for restore or for
//     service resumption, and the four restore checks
//     (readable/structure_constraints/business_state_probes/
//     verification_executable) are individually required: a missing check can
//     never be `verified`, and "file exists / can be listed" is not restore
//     verification (target must be isolated and evidence_ref present);
//   - selection is deterministic over manifest fields only:
//     recovery_point.wal_lsn decides, backup_id breaks ties, created_at is
//     display-only, and file name / mtime / human memory play no part
//     (SelectBackup receives manifests only);
//   - RPO can never be proven by backup frequency, business-table
//     MAX(created_at) or the backup file mtime; only the exported snapshot
//     tuple (+ export-time wal_lsn upper bound, wall clock, server/database)
//     is a valid recovery-point proof, and an incomplete recovery point or a
//     bare timestamp is refused;
//   - backup_lag / uncovered_interval may be the explicit value "unknown" but
//     MUST be present (RecoveryMeasurement).
//   - coverage.excluded must declare both signer_private_keys and
//     real_credentials; these declarations do not prove the artifact itself is
//     free of secret content.
//
// TDD-first: this file references the B6/T017 API (Manifest and friends) that
// does not exist yet, so the contract layer fails to build until T017 lands.
// The expected API surface is documented at each use site. Every assertion is
// pure Go: no Docker, no database, no filesystem.
package recovery

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// manifestValidJSON is the canonical wire fixture. Its key names, nesting and
// value shapes pin contracts/backup-manifest.md §1 and data-model.md §2: a
// change here is a manifest contract change.
const manifestValidJSON = `{
  "manifest_version": "015.1",
  "backup_id": "6f6d1c2e-4a77-4a1e-9d7c-2f5b8c0a1b2c",
  "created_at": "2026-09-28T12:00:00Z",
  "created_by": "deploy:executor",
  "carrier": {
    "kind": "pg_dump_custom",
    "pg_server_version": "18.6",
    "pg_dump_version": "18.6"
  },
  "coverage": {
    "authoritative": [
      "chain_blocks",
      "erc20_transfer_logs",
      "withdrawal_requests",
      "payment_intents",
      "signing_requests",
      "nonce_bindings",
      "outbox_events",
      "consumer_inbox",
      "recon_task"
    ],
    "excluded": [
      "redis_non_authoritative",
      "kafka_non_authoritative",
      "signer_private_keys",
      "real_credentials"
    ]
  },
  "recovery_point": {
    "snapshot": {"xmin": 769, "xip": [], "xmax": 769},
    "wal_lsn": "0/1C26D90",
    "wall_clock": "2026-09-28T12:00:00Z",
    "server": "txharbor-pg",
    "database": "txharbor"
  },
  "schema": {
    "goose_db_version": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]
  },
  "program": {"version": "018.0", "min_compatible": "018.0"},
  "artifacts": [
    {"path": "backup.pgcustom", "bytes": 2048, "sha256": "sha256:4f1e2d3c4b5a69788796a5b4c3d2e1f04f1e2d3c4b5a69788796a5b4c3d2e1f0"}
  ],
  "verification": {
    "state": "verified",
    "verified_at": "2026-09-28T12:05:00Z",
    "verifier": "auth:verifier",
    "target": "isolated",
    "checks": {
      "readable": true,
      "structure_constraints": true,
      "business_state_probes": true,
      "verification_executable": true
    },
    "evidence_ref": "docs/evidence/015/s2.json"
  },
  "retention_class": "test_inputs",
  "note": "local drill input, not a production threshold"
}`

// manifestValidation is the running program's compatibility target used by
// every Validate/CheckUsable/SelectBackup call in this file.
func manifestValidation() ManifestValidation {
	return ManifestValidation{
		PGServerMajor:  18,
		TargetSchema:   []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18},
		ProgramVersion: "018.0",
	}
}

// manifestJSON returns the wire fixture with optional mutations applied before
// encoding, so tests exercise the parser the way a real manifest file would.
func manifestJSON(t *testing.T, mutate func(doc map[string]any)) []byte {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(manifestValidJSON), &doc); err != nil {
		t.Fatalf("fixture is not valid JSON: %v", err)
	}
	if mutate != nil {
		mutate(doc)
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("encode fixture: %v", err)
	}
	return encoded
}

// manifestObject decodes the wire fixture into a typed *Manifest.
func manifestObject(t *testing.T, mutate func(doc map[string]any)) *Manifest {
	t.Helper()
	m, err := ParseManifest(manifestJSON(t, mutate))
	if err != nil {
		t.Fatalf("ParseManifest(valid fixture) = %v, want nil", err)
	}
	return m
}

// nested returns the object at doc[path...] or fails the test.
func nested(t *testing.T, doc map[string]any, path ...string) map[string]any {
	t.Helper()
	current := doc
	for _, key := range path {
		child, ok := current[key].(map[string]any)
		if !ok {
			t.Fatalf("fixture path %v: key %q is not an object", path, key)
		}
		current = child
	}
	return current
}

// TestManifestContractParsesValidDocument pins the wire field names and the
// typed accessors of data-model §2. The fixture is the contract document; if
// ParseManifest cannot round-trip it, the wire model drifted.
func TestManifestContractParsesValidDocument(t *testing.T) {
	m := manifestObject(t, nil)

	if m.ManifestVersion != ManifestVersionV1 {
		t.Fatalf("ManifestVersion = %q, want %q", m.ManifestVersion, ManifestVersionV1)
	}
	if m.BackupID != "6f6d1c2e-4a77-4a1e-9d7c-2f5b8c0a1b2c" {
		t.Fatalf("BackupID = %q", m.BackupID)
	}
	if m.CreatedAt == "" || m.CreatedBy != "deploy:executor" {
		t.Fatalf("CreatedAt/CreatedBy = %q/%q", m.CreatedAt, m.CreatedBy)
	}
	if m.Carrier.Kind != "pg_dump_custom" || m.Carrier.PGServerVersion != "18.6" || m.Carrier.PGDumpVersion != "18.6" {
		t.Fatalf("Carrier = %+v", m.Carrier)
	}
	if len(m.Coverage.Authoritative) == 0 || len(m.Coverage.Excluded) == 0 {
		t.Fatalf("Coverage must declare both authoritative objects and exclusions: %+v", m.Coverage)
	}
	for _, required := range []string{CoverageExcludedSignerPrivateKeys, CoverageExcludedRealCredentials} {
		if !containsString(m.Coverage.Excluded, required) {
			t.Fatalf("Coverage.Excluded = %v, must declare %s excluded", m.Coverage.Excluded, required)
		}
	}
	if m.RecoveryPoint.Snapshot.Xmin != 769 || m.RecoveryPoint.Snapshot.Xmax != 769 {
		t.Fatalf("RecoveryPoint.Snapshot = %+v", m.RecoveryPoint.Snapshot)
	}
	if m.RecoveryPoint.LSN != "0/1C26D90" || m.RecoveryPoint.WallClock == "" ||
		m.RecoveryPoint.Server == "" || m.RecoveryPoint.Database != "txharbor" {
		t.Fatalf("RecoveryPoint = %+v, want the full snapshot tuple + wal_lsn + wall clock + server/database", m.RecoveryPoint)
	}
	if len(m.Schema.GooseDBVersion) != 18 {
		t.Fatalf("Schema.GooseDBVersion = %v", m.Schema.GooseDBVersion)
	}
	if m.Program.Version != "018.0" || m.Program.MinCompatible != "018.0" {
		t.Fatalf("Program = %+v", m.Program)
	}
	if len(m.Artifacts) != 1 || m.Artifacts[0].Path == "" || m.Artifacts[0].Bytes != 2048 || m.Artifacts[0].SHA256 == "" {
		t.Fatalf("Artifacts = %+v", m.Artifacts)
	}
	if m.Verification.State != VerificationVerified || m.Verification.Target != "isolated" ||
		m.Verification.Verifier == "" || m.Verification.EvidenceRef == "" {
		t.Fatalf("Verification = %+v", m.Verification)
	}
	if !m.Verification.Checks.Readable || !m.Verification.Checks.StructureConstraints ||
		!m.Verification.Checks.BusinessStateProbes || !m.Verification.Checks.VerificationExecutable {
		t.Fatalf("Verification.Checks = %+v, want all four true", m.Verification.Checks)
	}
}

// TestManifestContractRequiresBothSecretExclusions ensures omission of either
// declaration is rejected independently of digest integrity. Recomputing a
// digest only binds the modified document; it cannot make an invalid coverage
// declaration acceptable or prove the artifact contents are secret-free.
func TestManifestContractRequiresBothSecretExclusions(t *testing.T) {
	valid := manifestObject(t, nil)
	if err := valid.Validate(manifestValidation()); err != nil {
		t.Fatalf("complete exclusion declarations rejected: %v", err)
	}
	if !containsString(valid.Coverage.Excluded, CoverageExcludedSignerPrivateKeys) ||
		!containsString(valid.Coverage.Excluded, CoverageExcludedRealCredentials) {
		t.Fatalf("complete fixture exclusions = %v", valid.Coverage.Excluded)
	}

	for _, omitted := range []string{CoverageExcludedSignerPrivateKeys, CoverageExcludedRealCredentials} {
		t.Run("missing_"+omitted, func(t *testing.T) {
			m, err := ParseManifest(manifestJSON(t, func(doc map[string]any) {
				coverage := nested(t, doc, "coverage")
				excluded := coverage["excluded"].([]any)
				filtered := make([]any, 0, len(excluded)-1)
				for _, entry := range excluded {
					if entry != omitted {
						filtered = append(filtered, entry)
					}
				}
				coverage["excluded"] = filtered
			}))
			if err != nil {
				t.Fatalf("ParseManifest: %v", err)
			}

			// The changed body has a valid, freshly recomputed digest. Contract
			// validation must still reject the missing required declaration.
			digest, err := m.Digest()
			if err != nil {
				t.Fatalf("Digest(tampered manifest): %v", err)
			}
			if err := CheckManifestDigest(m, digest); err != nil {
				t.Fatalf("CheckManifestDigest(recomputed) = %v, want nil", err)
			}
			if err := m.Validate(manifestValidation()); !errors.Is(err, ErrManifestInvalid) {
				t.Fatalf("Validate(missing %s) = %v, want ErrManifestInvalid", omitted, err)
			}
		})
	}
}

// TestManifestContractMissingRequiredFieldRefused walks every required field
// of contracts/backup-manifest.md §1: removing any one of them makes the
// manifest unusable (fail-closed; no zero-value default may stand in).
func TestManifestContractMissingRequiredFieldRefused(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(doc map[string]any)
	}{
		{"manifest_version", func(doc map[string]any) { delete(doc, "manifest_version") }},
		{"backup_id", func(doc map[string]any) { delete(doc, "backup_id") }},
		{"created_at", func(doc map[string]any) { delete(doc, "created_at") }},
		{"created_by", func(doc map[string]any) { delete(doc, "created_by") }},
		{"carrier", func(doc map[string]any) { delete(doc, "carrier") }},
		{"carrier.kind", func(doc map[string]any) { delete(nested(t, doc, "carrier"), "kind") }},
		{"carrier.pg_server_version", func(doc map[string]any) { delete(nested(t, doc, "carrier"), "pg_server_version") }},
		{"carrier.pg_dump_version", func(doc map[string]any) { delete(nested(t, doc, "carrier"), "pg_dump_version") }},
		{"coverage", func(doc map[string]any) { delete(doc, "coverage") }},
		{"coverage.authoritative", func(doc map[string]any) { delete(nested(t, doc, "coverage"), "authoritative") }},
		{"coverage.excluded", func(doc map[string]any) { delete(nested(t, doc, "coverage"), "excluded") }},
		{"recovery_point", func(doc map[string]any) { delete(doc, "recovery_point") }},
		{"recovery_point.snapshot", func(doc map[string]any) { delete(nested(t, doc, "recovery_point"), "snapshot") }},
		{"recovery_point.snapshot.xmin", func(doc map[string]any) { delete(nested(t, doc, "recovery_point", "snapshot"), "xmin") }},
		{"recovery_point.snapshot.xmax", func(doc map[string]any) { delete(nested(t, doc, "recovery_point", "snapshot"), "xmax") }},
		{"recovery_point.wal_lsn", func(doc map[string]any) { delete(nested(t, doc, "recovery_point"), "wal_lsn") }},
		{"recovery_point.wall_clock", func(doc map[string]any) { delete(nested(t, doc, "recovery_point"), "wall_clock") }},
		{"recovery_point.server", func(doc map[string]any) { delete(nested(t, doc, "recovery_point"), "server") }},
		{"recovery_point.database", func(doc map[string]any) { delete(nested(t, doc, "recovery_point"), "database") }},
		{"schema", func(doc map[string]any) { delete(doc, "schema") }},
		{"schema.goose_db_version", func(doc map[string]any) { delete(nested(t, doc, "schema"), "goose_db_version") }},
		{"program", func(doc map[string]any) { delete(doc, "program") }},
		{"program.version", func(doc map[string]any) { delete(nested(t, doc, "program"), "version") }},
		{"program.min_compatible", func(doc map[string]any) { delete(nested(t, doc, "program"), "min_compatible") }},
		{"artifacts", func(doc map[string]any) { delete(doc, "artifacts") }},
		{"verification", func(doc map[string]any) { delete(doc, "verification") }},
		{"verification.state", func(doc map[string]any) { delete(nested(t, doc, "verification"), "state") }},
		{"verification.checks", func(doc map[string]any) { delete(nested(t, doc, "verification"), "checks") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseManifest(manifestJSON(t, tc.mutate)); err == nil {
				t.Fatalf("ParseManifest without %s = nil, want refusal (fail-closed)", tc.name)
			}
		})
	}
	// The refusal for a structurally broken document is classifiable.
	if _, err := ParseManifest(manifestJSON(t, func(doc map[string]any) { delete(doc, "carrier") })); !errors.Is(err, ErrManifestInvalid) {
		t.Fatalf("missing carrier error = %v, want errors.Is ErrManifestInvalid", err)
	}
}

// TestManifestContractArtifactEntries pins artifacts[]: at least one artifact,
// and each entry carries a path, a positive byte count and a sha256.
func TestManifestContractArtifactEntries(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(doc map[string]any)
		wantErr bool
	}{
		{"empty list", func(doc map[string]any) { doc["artifacts"] = []any{} }, true},
		{"missing path", func(doc map[string]any) { doc["artifacts"] = []any{map[string]any{"bytes": 1, "sha256": "sha256:aa"}} }, true},
		{"missing bytes", func(doc map[string]any) { doc["artifacts"] = []any{map[string]any{"path": "d", "sha256": "sha256:aa"}} }, true},
		{"zero bytes", func(doc map[string]any) {
			doc["artifacts"] = []any{map[string]any{"path": "d", "bytes": 0, "sha256": "sha256:aa"}}
		}, true},
		{"missing sha256", func(doc map[string]any) { doc["artifacts"] = []any{map[string]any{"path": "d", "bytes": 1}} }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := ParseManifest(manifestJSON(t, tc.mutate))
			if err == nil {
				err = m.Validate(manifestValidation())
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("artifact case %s: err = %v, wantErr = %v", tc.name, err, tc.wantErr)
			}
		})
	}
}

// TestManifestContractUnknownVersionRefused pins manifest_version: an unknown
// version is refused, and the known version constant is the contract.
func TestManifestContractUnknownVersionRefused(t *testing.T) {
	_, err := ParseManifest(manifestJSON(t, func(doc map[string]any) {
		doc["manifest_version"] = "999.0"
	}))
	if !errors.Is(err, ErrManifestVersionUnknown) {
		t.Fatalf("unknown manifest_version error = %v, want ErrManifestVersionUnknown", err)
	}
	for _, raw := range []string{"", "1", "015", "015.1 ", "v015.1"} {
		if _, err := ParseManifest(manifestJSON(t, func(doc map[string]any) {
			doc["manifest_version"] = raw
		})); !errors.Is(err, ErrManifestVersionUnknown) {
			t.Fatalf("manifest_version %q error = %v, want ErrManifestVersionUnknown", raw, err)
		}
	}
}

// TestManifestContractVerificationStateClosedSet pins the closed state set and
// that no unknown state can masquerade as verified.
func TestManifestContractVerificationStateClosedSet(t *testing.T) {
	for _, raw := range []string{"", "approved", "verified ", "VERIFIED", "ok"} {
		if _, err := ParseManifest(manifestJSON(t, func(doc map[string]any) {
			nested(t, doc, "verification")["state"] = raw
		})); err == nil {
			t.Fatalf("verification.state %q = nil error, want refusal (closed set)", raw)
		}
	}
}

// TestManifestContractDigestStabilityAndMismatch pins the canonical JSON hash:
// the digest is stable across key order and whitespace, and a recorded digest
// that does not match the current body is refused.
func TestManifestContractDigestStabilityAndMismatch(t *testing.T) {
	m1 := manifestObject(t, nil)
	// The same semantic document with a different byte layout (map marshaling
	// sorts keys alphabetically and drops the fixture's formatting).
	m2 := manifestObject(t, func(doc map[string]any) {
		doc["note"] = "local drill input, not a production threshold"
	})

	c1, err := m1.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	c2, err := m2.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	if !bytes.Equal(c1, c2) {
		t.Fatalf("canonical JSON differs for the same document:\n%s\n%s", c1, c2)
	}
	d1, err := m1.Digest()
	if err != nil {
		t.Fatalf("Digest: %v", err)
	}
	d2, err := m2.Digest()
	if err != nil {
		t.Fatalf("Digest: %v", err)
	}
	if d1 != d2 || !strings.HasPrefix(d1, "sha256:") {
		t.Fatalf("Digest = %q / %q, want one stable sha256 digest", d1, d2)
	}
	if err := CheckManifestDigest(m1, d1); err != nil {
		t.Fatalf("CheckManifestDigest(match) = %v, want nil", err)
	}
	m1.RecoveryPoint.LSN = "0/DEADBEEF"
	if _, err := m1.Digest(); err != nil {
		t.Fatalf("Digest after mutation: %v", err)
	}
	if err := CheckManifestDigest(m1, d1); !errors.Is(err, ErrManifestDigestMismatch) {
		t.Fatalf("CheckManifestDigest(tampered) = %v, want ErrManifestDigestMismatch", err)
	}
	if err := CheckManifestDigest(m1, "sha256:not-hex"); !errors.Is(err, ErrManifestDigestMismatch) {
		t.Fatalf("CheckManifestDigest(malformed) = %v, want ErrManifestDigestMismatch", err)
	}
	if err := CheckManifestDigest(m1, ""); !errors.Is(err, ErrManifestDigestMismatch) {
		t.Fatalf("CheckManifestDigest(empty) = %v, want ErrManifestDigestMismatch", err)
	}
}

// TestManifestContractValidateCompatibility pins FR-004: carrier kind and
// major versions must match the target PG, the recorded goose set must be
// exactly the program's target set (unknown versions refused), and the
// program's minimum-compatible floor must be satisfied by the running
// program.
func TestManifestContractValidateCompatibility(t *testing.T) {
	valid := manifestValidation()
	if err := manifestObject(t, nil).Validate(valid); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}

	cases := []struct {
		name    string
		mutate  func(doc map[string]any)
		wantErr bool
	}{
		{"wrong carrier kind", func(doc map[string]any) {
			nested(t, doc, "carrier")["kind"] = "pg_basebackup"
		}, true},
		{"server major mismatch", func(doc map[string]any) {
			nested(t, doc, "carrier")["pg_server_version"] = "17.6"
		}, true},
		{"pg_dump major mismatch", func(doc map[string]any) {
			nested(t, doc, "carrier")["pg_dump_version"] = "17.6"
		}, true},
		{"unparsable server version", func(doc map[string]any) {
			nested(t, doc, "carrier")["pg_server_version"] = "eighteen"
		}, true},
		{"missing target migration", func(doc map[string]any) {
			nested(t, doc, "schema")["goose_db_version"] = []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}
		}, true},
		{"unknown extra migration", func(doc map[string]any) {
			nested(t, doc, "schema")["goose_db_version"] = []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 9999}
		}, true},
		{"empty schema set", func(doc map[string]any) {
			nested(t, doc, "schema")["goose_db_version"] = []any{}
		}, true},
		{"program floor newer than running", func(doc map[string]any) {
			nested(t, doc, "program")["min_compatible"] = "999.0"
		}, true},
		{"running program newer than floor", func(doc map[string]any) {
			nested(t, doc, "program")["min_compatible"] = "017.9"
		}, false},
		{"bogus recovery point lsn", func(doc map[string]any) {
			nested(t, doc, "recovery_point")["wal_lsn"] = "not-an-lsn"
		}, true},
		{"bogus wall clock", func(doc map[string]any) {
			nested(t, doc, "recovery_point")["wall_clock"] = "yesterday"
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A refusal may come from ParseManifest or from Validate; the
			// contract only fixes the outcome (incompatible is refused).
			m, err := ParseManifest(manifestJSON(t, tc.mutate))
			if err == nil {
				err = m.Validate(valid)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate(%s) = %v, wantErr = %v", tc.name, err, tc.wantErr)
			}
		})
	}
}

// TestManifestContractProgramCompatible pins the dotted-numeric comparison
// behind the program.min_compatible floor (FR-004, fail-closed): equal and
// newer pass, older and unparsable refuse, and "015.10" is newer than
// "015.9" (numeric segment order, never string order).
func TestManifestContractProgramCompatible(t *testing.T) {
	cases := []struct {
		running, floor string
		want           bool
	}{
		{"018.0", "018.0", true},
		{"018.1", "018.0", true},
		{"019.0", "018.9", true},
		{"015.10", "015.9", true},
		{"017.9", "018.0", false},
		{"018", "018.0", false},
		{"018.0", "018.0.1", false},
		{"", "018.0", false},
		{"018.0", "", false},
		{"v018.0", "018.0", false},
		{"018.x", "018.0", false},
	}
	for _, tc := range cases {
		if got := ProgramCompatible(tc.running, tc.floor); got != tc.want {
			t.Fatalf("ProgramCompatible(%q, %q) = %v, want %v", tc.running, tc.floor, got, tc.want)
		}
	}
}

// TestManifestContractNotVerifiedNeverUsable pins contracts/backup-manifest.md
// §5: verification.state != verified is not usable for restore or resumption,
// and an unknown state never slips through.
func TestManifestContractNotVerifiedNeverUsable(t *testing.T) {
	uses := []ManifestUse{ManifestUseRestore, ManifestUseResumption}
	valid := manifestValidation()

	for _, use := range uses {
		for _, state := range []VerificationState{VerificationUnverified, VerificationRejected} {
			m := manifestObject(t, func(doc map[string]any) {
				verification := nested(t, doc, "verification")
				verification["state"] = string(state)
				delete(verification, "verified_at")
				delete(verification, "verifier")
				delete(verification, "evidence_ref")
				checks := nested(t, doc, "verification", "checks")
				for _, key := range []string{"readable", "structure_constraints", "business_state_probes", "verification_executable"} {
					checks[key] = false
				}
			})
			if err := m.CheckUsable(use, valid); !errors.Is(err, ErrManifestNotVerified) {
				t.Fatalf("CheckUsable(%s, state=%s) = %v, want ErrManifestNotVerified", use, state, err)
			}
		}
	}
	if err := manifestObject(t, nil).CheckUsable(ManifestUseRestore, valid); err != nil {
		t.Fatalf("verified manifest refused for restore: %v", err)
	}
	if err := manifestObject(t, nil).CheckUsable(ManifestUseResumption, valid); err != nil {
		t.Fatalf("verified manifest refused for resumption: %v", err)
	}
}

// TestManifestContractFourChecksRequiredForVerified pins backups §3: each of
// the four restore checks is individually required; a `verified` state that
// lacks any one of them is refused (and "readable alone" is never
// verification).
func TestManifestContractFourChecksRequiredForVerified(t *testing.T) {
	valid := manifestValidation()
	checkKeys := []string{"readable", "structure_constraints", "business_state_probes", "verification_executable"}
	for _, key := range checkKeys {
		t.Run(key, func(t *testing.T) {
			m, err := ParseManifest(manifestJSON(t, func(doc map[string]any) {
				nested(t, doc, "verification", "checks")[key] = false
			}))
			if err != nil {
				// Parse-level refusal also satisfies "can never be verified".
				return
			}
			if err := m.Validate(valid); err == nil {
				t.Fatalf("Validate with %s=false = nil, want refusal (a missing check can never be verified)", key)
			}
			if err := m.CheckUsable(ManifestUseRestore, valid); err == nil {
				t.Fatalf("CheckUsable with %s=false = nil, want refusal", key)
			}
		})
	}

	// "readable alone" must not be verification: all other checks stay false.
	readableOnly, err := ParseManifest(manifestJSON(t, func(doc map[string]any) {
		checks := nested(t, doc, "verification", "checks")
		checks["structure_constraints"] = false
		checks["business_state_probes"] = false
		checks["verification_executable"] = false
	}))
	if err == nil && readableOnly.CheckUsable(ManifestUseRestore, valid) == nil {
		t.Fatal("readable-only manifest accepted, want refusal (file exists / can be listed is not restore verification)")
	}

	// A verified state must point at an isolated target and an evidence ref.
	for _, mutate := range []func(doc map[string]any){
		func(doc map[string]any) { nested(t, doc, "verification")["target"] = "production" },
		func(doc map[string]any) { nested(t, doc, "verification")["target"] = "" },
		func(doc map[string]any) { nested(t, doc, "verification")["evidence_ref"] = "" },
		func(doc map[string]any) { nested(t, doc, "verification")["verifier"] = "" },
		func(doc map[string]any) { nested(t, doc, "verification")["verified_at"] = "" },
	} {
		m, err := ParseManifest(manifestJSON(t, mutate))
		if err == nil {
			err = m.CheckUsable(ManifestUseRestore, valid)
		}
		if err == nil {
			t.Fatalf("verified manifest without isolation/evidence metadata accepted")
		}
	}
}

// TestManifestContractSelectionDeterministic pins contracts/backup-manifest.md
// §2 selection: only verified+valid manifests are eligible; wal_lsn decides
// (created_at is display-only), backup_id breaks ties, order/files/paths do
// not matter, and no eligible candidate is a refusal - never "best effort".
func TestManifestContractSelectionDeterministic(t *testing.T) {
	valid := manifestValidation()
	verified := func(mutate func(doc map[string]any)) *Manifest {
		return manifestObject(t, mutate)
	}

	// created_at is display-only: the older manifest with the higher LSN wins.
	older := verified(func(doc map[string]any) {
		doc["created_at"] = "2026-09-01T00:00:00Z"
		nested(t, doc, "recovery_point")["wal_lsn"] = "0/00000FFF"
		doc["backup_id"] = "aaaaaaaa-0000-0000-0000-000000000000"
	})
	newer := verified(func(doc map[string]any) {
		doc["created_at"] = "2026-09-28T23:59:59Z"
		nested(t, doc, "recovery_point")["wal_lsn"] = "0/00000F00"
		doc["backup_id"] = "bbbbbbbb-0000-0000-0000-000000000000"
	})
	winner, err := SelectBackup([]*Manifest{newer, older}, valid)
	if err != nil {
		t.Fatalf("SelectBackup = %v", err)
	}
	if winner.BackupID != older.BackupID {
		t.Fatalf("selected %s, want the higher wal_lsn %s (created_at must be display-only)", winner.BackupID, older.BackupID)
	}

	// wal_lsn is compared as a position, not as a string.
	lexical := verified(func(doc map[string]any) {
		nested(t, doc, "recovery_point")["wal_lsn"] = "0/1F"
		doc["backup_id"] = "cccccccc-0000-0000-0000-000000000000"
	})
	canonical := verified(func(doc map[string]any) {
		nested(t, doc, "recovery_point")["wal_lsn"] = "0/2"
		doc["backup_id"] = "dddddddd-0000-0000-0000-000000000000"
	})
	winner, err = SelectBackup([]*Manifest{lexical, canonical}, valid)
	if err != nil {
		t.Fatalf("SelectBackup = %v", err)
	}
	if winner.BackupID != lexical.BackupID {
		t.Fatalf("selected %s for 0/1F vs 0/2, want numeric LSN order (0/1F > 0/2)", winner.BackupID)
	}

	// Equal LSN -> backup_id decides, deterministically and order-free.
	first := verified(func(doc map[string]any) {
		nested(t, doc, "recovery_point")["wal_lsn"] = "0/10"
		doc["backup_id"] = "11111111-0000-0000-0000-000000000000"
	})
	second := verified(func(doc map[string]any) {
		nested(t, doc, "recovery_point")["wal_lsn"] = "0/10"
		doc["backup_id"] = "22222222-0000-0000-0000-000000000000"
	})
	winner, err = SelectBackup([]*Manifest{second, first}, valid)
	if err != nil {
		t.Fatalf("SelectBackup = %v", err)
	}
	if winner.BackupID != first.BackupID {
		t.Fatalf("tie on wal_lsn selected %s, want deterministic backup_id order (%s)", winner.BackupID, first.BackupID)
	}
	winnerReordered, err := SelectBackup([]*Manifest{first, second}, valid)
	if err != nil {
		t.Fatalf("SelectBackup(reordered) = %v", err)
	}
	if winnerReordered.BackupID != winner.BackupID {
		t.Fatal("selection depends on candidate order")
	}

	// Path/artifact name and created_at changes must not move the winner.
	renamed := verified(func(doc map[string]any) {
		nested(t, doc, "recovery_point")["wal_lsn"] = "0/00000F00"
		doc["backup_id"] = "bbbbbbbb-0000-0000-0000-000000000000"
		doc["artifacts"] = []any{map[string]any{
			"path": "zz-latest-looking.dump", "bytes": 10, "sha256": "sha256:aa",
		}}
	})
	winner, err = SelectBackup([]*Manifest{renamed, older}, valid)
	if err != nil {
		t.Fatalf("SelectBackup = %v", err)
	}
	if winner.BackupID != older.BackupID {
		t.Fatalf("artifact file path changed the selection to %s; only manifest fields may decide", winner.BackupID)
	}

	// Unverified/rejected candidates are never eligible, even with a higher LSN.
	unverified := verified(func(doc map[string]any) {
		nested(t, doc, "recovery_point")["wal_lsn"] = "FFFFFFFF/FFFFFFFF"
		doc["backup_id"] = "eeeeeeee-0000-0000-0000-000000000000"
		verification := nested(t, doc, "verification")
		verification["state"] = string(VerificationUnverified)
		delete(verification, "verified_at")
		delete(verification, "verifier")
		delete(verification, "evidence_ref")
		nested(t, doc, "verification", "checks")["readable"] = false
		nested(t, doc, "verification", "checks")["structure_constraints"] = false
		nested(t, doc, "verification", "checks")["business_state_probes"] = false
		nested(t, doc, "verification", "checks")["verification_executable"] = false
	})
	winner, err = SelectBackup([]*Manifest{unverified, older}, valid)
	if err != nil {
		t.Fatalf("SelectBackup = %v", err)
	}
	if winner.BackupID != older.BackupID {
		t.Fatalf("selected unverified %s, want only verified+valid candidates eligible", winner.BackupID)
	}

	// An incompatible candidate is not eligible either (Validate context).
	incompatible := verified(func(doc map[string]any) {
		nested(t, doc, "recovery_point")["wal_lsn"] = "FFFFFFFF/FFFFFFFE"
		doc["backup_id"] = "ffffffff-0000-0000-0000-000000000000"
		nested(t, doc, "carrier")["pg_server_version"] = "17.6"
	})
	winner, err = SelectBackup([]*Manifest{incompatible, older}, valid)
	if err != nil {
		t.Fatalf("SelectBackup = %v", err)
	}
	if winner.BackupID != older.BackupID {
		t.Fatalf("selected incompatible %s, want only validated candidates eligible", winner.BackupID)
	}

	// Nothing eligible is a refusal: no fallback to name/mtime/memory.
	if _, err := SelectBackup([]*Manifest{unverified}, valid); !errors.Is(err, ErrNoEligibleBackup) {
		t.Fatalf("SelectBackup(all ineligible) = %v, want ErrNoEligibleBackup", err)
	}
	if _, err := SelectBackup(nil, valid); !errors.Is(err, ErrNoEligibleBackup) {
		t.Fatalf("SelectBackup(no candidates) = %v, want ErrNoEligibleBackup", err)
	}
}

// TestManifestContractRPOProofKinds pins FR-036: only the exported snapshot
// tuple (with the export-time wal_lsn upper bound, wall clock and
// server/database identity) can prove the recovery point; backup frequency,
// business-table MAX(created_at) and backup-file mtime are forbidden proofs,
// and a bare wall-clock timestamp is not a recovery point at all.
func TestManifestContractRPOProofKinds(t *testing.T) {
	full := manifestObject(t, nil).RecoveryPoint

	if err := ValidateRPOProof(RPOProofSnapshotTuple, full); err != nil {
		t.Fatalf("ValidateRPOProof(snapshot tuple) = %v, want nil", err)
	}
	for _, forbidden := range []RPOProofKind{
		RPOProofBackupFrequency,
		RPOProofBusinessMaxCreatedAt,
		RPOProofFileMTime,
		"",
		"latest_file_mtime",
	} {
		err := ValidateRPOProof(forbidden, full)
		if forbidden == "" {
			// An empty kind is not a proof either; any refusal is fine.
			if err == nil {
				t.Fatal("ValidateRPOProof(empty kind) = nil, want refusal")
			}
			continue
		}
		if !errors.Is(err, ErrRPOProofForbidden) {
			t.Fatalf("ValidateRPOProof(%q) = %v, want ErrRPOProofForbidden", forbidden, err)
		}
	}

	// A wall clock alone is not a recovery point; nor is a partial tuple.
	timestampOnly := full
	timestampOnly.Snapshot = ManifestSnapshot{}
	timestampOnly.LSN = ""
	if err := ValidateRPOProof(RPOProofSnapshotTuple, timestampOnly); !errors.Is(err, ErrRecoveryPointIncomplete) {
		t.Fatalf("ValidateRPOProof(timestamp only) = %v, want ErrRecoveryPointIncomplete", err)
	}
	missingLSN := full
	missingLSN.LSN = ""
	if err := ValidateRPOProof(RPOProofSnapshotTuple, missingLSN); !errors.Is(err, ErrRecoveryPointIncomplete) {
		t.Fatalf("ValidateRPOProof(missing wal_lsn) = %v, want ErrRecoveryPointIncomplete", err)
	}
	missingDatabase := full
	missingDatabase.Database = ""
	if err := ValidateRPOProof(RPOProofSnapshotTuple, missingDatabase); !errors.Is(err, ErrRecoveryPointIncomplete) {
		t.Fatalf("ValidateRPOProof(missing database) = %v, want ErrRecoveryPointIncomplete", err)
	}
	bogusLSN := full
	bogusLSN.LSN = "not-an-lsn"
	if err := ValidateRPOProof(RPOProofSnapshotTuple, bogusLSN); !errors.Is(err, ErrRecoveryPointIncomplete) {
		t.Fatalf("ValidateRPOProof(bogus wal_lsn) = %v, want ErrRecoveryPointIncomplete", err)
	}

	// The same rule through manifest validation: a recovery point that is only
	// a timestamp is refused. Like the compatibility cases above, a
	// parse-level refusal also satisfies the rule (the contract fixes the
	// outcome, and a required key deleted from the wire document may already
	// fail the structural parse - see TestManifestContractMissingRequiredFieldRefused).
	m, parseErr := ParseManifest(manifestJSON(t, func(doc map[string]any) {
		rp := nested(t, doc, "recovery_point")
		delete(rp, "snapshot")
		delete(rp, "wal_lsn")
	}))
	if parseErr == nil {
		if err := m.Validate(manifestValidation()); err == nil {
			t.Fatal("manifest with a timestamp-only recovery point accepted, want refusal")
		}
	}
}

// TestManifestContractRecoveryMeasurement pins contracts/backup-manifest.md §4:
// backup_lag and uncovered_interval are always explicitly recorded (they may
// be "unknown", but omitting either key is refused); a known interval must be
// a non-negative duration.
func TestManifestContractRecoveryMeasurement(t *testing.T) {
	rp := manifestValidJSONRecoveryPointAttribute(t)
	base := `{
	  "recovery_point": ` + rp + `,
	  "backup_lag": "unknown",
	  "uncovered_interval": "unknown"
	}`

	if _, err := ParseRecoveryMeasurement([]byte(base)); err != nil {
		t.Fatalf("ParseRecoveryMeasurement(unknown/unknown) = %v, want nil (unknown is allowed, omission is not)", err)
	}

	missingLag := `{"recovery_point": ` + rp + `, "uncovered_interval": "unknown"}`
	if _, err := ParseRecoveryMeasurement([]byte(missingLag)); err == nil {
		t.Fatal("omitting backup_lag accepted, want refusal (must be present even when unknown)")
	}
	missingInterval := `{"recovery_point": ` + rp + `, "backup_lag": "unknown"}`
	if _, err := ParseRecoveryMeasurement([]byte(missingInterval)); err == nil {
		t.Fatal("omitting uncovered_interval accepted, want refusal (must be present even when unknown)")
	}
	missingPoint := `{"backup_lag": "unknown", "uncovered_interval": "unknown"}`
	if _, err := ParseRecoveryMeasurement([]byte(missingPoint)); err == nil {
		t.Fatal("omitting recovery_point accepted, want refusal")
	}

	known := `{
	  "recovery_point": ` + rp + `,
	  "backup_lag": {"state": "known", "seconds": 12.5},
	  "uncovered_interval": {"state": "known", "seconds": 0}
	}`
	measurement, err := ParseRecoveryMeasurement([]byte(known))
	if err != nil {
		t.Fatalf("ParseRecoveryMeasurement(known) = %v", err)
	}
	if measurement.BackupLag.State != MeasurementKnown || measurement.BackupLag.Seconds != 12.5 {
		t.Fatalf("BackupLag = %+v", measurement.BackupLag)
	}
	if measurement.UncoveredInterval.State != MeasurementKnown || measurement.UncoveredInterval.Seconds != 0 {
		t.Fatalf("UncoveredInterval = %+v", measurement.UncoveredInterval)
	}
	if err := measurement.Validate(); err != nil {
		t.Fatalf("Validate(known) = %v", err)
	}

	// "unknown" never carries a measured value; a known value must be
	// non-negative; unknown/garbage states are refused.
	for _, bad := range []string{
		`{"recovery_point": ` + rp + `, "backup_lag": {"state": "unknown", "seconds": 5}, "uncovered_interval": "unknown"}`,
		`{"recovery_point": ` + rp + `, "backup_lag": {"state": "known", "seconds": -1}, "uncovered_interval": "unknown"}`,
		`{"recovery_point": ` + rp + `, "backup_lag": "", "uncovered_interval": "unknown"}`,
		`{"recovery_point": ` + rp + `, "backup_lag": {"state": "maybe"}, "uncovered_interval": "unknown"}`,
	} {
		parsed, err := ParseRecoveryMeasurement([]byte(bad))
		if err == nil {
			err = parsed.Validate()
		}
		if err == nil {
			t.Fatalf("invalid measurement accepted: %s", bad)
		}
	}

	unknownOnly, err := ParseRecoveryMeasurement([]byte(base))
	if err != nil {
		t.Fatalf("ParseRecoveryMeasurement: %v", err)
	}
	if unknownOnly.BackupLag.State != MeasurementUnknown || unknownOnly.UncoveredInterval.State != MeasurementUnknown {
		t.Fatalf("unknown measurement states = %+v / %+v", unknownOnly.BackupLag, unknownOnly.UncoveredInterval)
	}
	if err := unknownOnly.Validate(); err != nil {
		t.Fatalf("Validate(unknown/unknown) = %v, want nil (explicit unknown is valid input)", err)
	}
	// An explicit unknown is present but is not an RPO proof.
	if err := ValidateRPOProof(RPOProofSnapshotTuple, unknownOnly.RecoveryPoint); err != nil {
		t.Fatalf("recovery point of an unknown-lag measurement must still be a valid snapshot tuple: %v", err)
	}
}

// manifestValidJSONRecoveryPointAttribute extracts the raw recovery_point JSON
// object from the canonical fixture.
func manifestValidJSONRecoveryPointAttribute(t *testing.T) string {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(manifestValidJSON), &doc); err != nil {
		t.Fatalf("fixture is not valid JSON: %v", err)
	}
	raw, err := json.Marshal(doc["recovery_point"])
	if err != nil {
		t.Fatalf("encode recovery_point: %v", err)
	}
	return string(raw)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
