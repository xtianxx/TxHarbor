// secrecy_test.go is T016: the FR-007 secrecy unit layer (no Docker, no
// database, no build tag - it runs in the ordinary `make test` unit layer).
//
// It pins the credential boundaries the recovery feature must hold:
//
//   - internal/logx/redact.go:34 Redact masks DSN passwords, keyword secrets,
//     bearer tokens and private key material while keeping the structural
//     context (scheme/user/host/database) diagnosable;
//   - RedactedDSN (internal/recovery/restore.go, B6/T019) is the only DSN form
//     that may reach logs/audit: structure preserved, credential values gone;
//   - ScanManifestSecrets (internal/recovery/manifest.go, B6/T017) refuses a
//     manifest whose free-text fields embed a DSN, credential values or PEM
//     private key material - the manifest is stored next to the backup and
//     is never a secret carrier; a coverage declaration that merely NAMES
//     signer_private_keys is not secret material and must not be refused;
//   - CheckSignerBoundary (internal/recovery/restore.go, B6/T019) proves
//     reachability only: its result carries a redacted endpoint and a boolean,
//     never key material, and an empty endpoint is refused by name;
//   - controlstore.ValidateDataTarget (existing) refuses audit/data_target
//     payloads that embed DSNs or credential keys at any depth, and the real
//     DataTargetFingerprint payload stays credential-free.
//
// TDD-first: the three B6-owned symbols above do not exist yet, so the unit
// candidate fails to build until T017/T019 land. All canary values below are
// obviously fictional test strings; none is a real credential.
package recovery

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

const (
	secrecyCanaryPassword = "canary-passphrase-9f1c-do-not-log"
	secrecyCanaryToken    = "canary-bearer-token-7a2b-do-not-log"
	secrecyCanaryKeyHex   = "canary-private-key-material-deadbeef-do-not-log"
	secrecyCanaryPEM      = "-----BEGIN PRIVATE KEY-----\nY2FuYXJ5LXByaXZhdGUta2V5LW1hdGVyaWFs\n-----END PRIVATE KEY-----"
	secrecyCanaryUser     = "canary_user"
)

// TestSecrecyRedactMasksCredentialCanaries pins the existing redaction
// primitive every recovery log/audit path must reuse (internal/logx/redact.go:34).
func TestSecrecyRedactMasksCredentialCanaries(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		canary  string
		keep    []string
		redacts bool
	}{
		{
			name:    "url dsn password",
			input:   "connect failed: postgres://" + secrecyCanaryUser + ":" + secrecyCanaryPassword + "@db.internal:5432/txharbor?sslmode=disable",
			canary:  secrecyCanaryPassword,
			keep:    []string{"postgres://", "db.internal:5432", "txharbor"},
			redacts: true,
		},
		{
			name:    "url dsn query token",
			input:   "dsn=postgres://txharbor@db.internal:5432/txharbor?token=" + secrecyCanaryToken,
			canary:  secrecyCanaryToken,
			keep:    []string{"db.internal", "txharbor"},
			redacts: true,
		},
		{
			name:    "keyword dsn password",
			input:   "host=db.internal port=5432 user=txharbor password=" + secrecyCanaryPassword + " dbname=txharbor",
			canary:  secrecyCanaryPassword,
			keep:    []string{"host=db.internal", "dbname=txharbor"},
			redacts: true,
		},
		{
			name:    "keyword dsn private key",
			input:   "private_key=" + secrecyCanaryKeyHex + " signer=boundary",
			canary:  secrecyCanaryKeyHex,
			keep:    []string{"signer=boundary"},
			redacts: true,
		},
		{
			name:    "bearer authorization",
			input:   "request failed: Authorization: Bearer " + secrecyCanaryToken,
			canary:  secrecyCanaryToken,
			keep:    []string{"Authorization:", "Bearer"},
			redacts: true,
		},
		{
			name:    "plain diagnostic untouched",
			input:   "backup artifact sha256 verified for txharbor",
			canary:  secrecyCanaryPassword,
			keep:    []string{"backup artifact", "txharbor"},
			redacts: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := logx.Redact(tc.input)
			if strings.Contains(out, tc.canary) {
				t.Fatalf("Redact left the canary in %q", out)
			}
			for _, fragment := range tc.keep {
				if !strings.Contains(out, fragment) {
					t.Fatalf("Redact(%q) = %q, lost diagnosable context %q", tc.input, out, fragment)
				}
			}
			if tc.redacts && !strings.Contains(out, logx.Redacted) {
				t.Fatalf("Redact(%q) = %q, want the %s placeholder", tc.input, out, logx.Redacted)
			}
			if again := logx.Redact(out); again != out {
				t.Fatalf("Redact is not idempotent: %q -> %q", out, again)
			}
		})
	}
}

// TestSecrecyRedactedDSNKeepsStructureWithoutCredentials pins RedactedDSN:
// the only DSN form allowed into logs/audit (FR-007/FR-008/T022).
func TestSecrecyRedactedDSNKeepsStructureWithoutCredentials(t *testing.T) {
	urlDSN := "postgres://" + secrecyCanaryUser + ":" + secrecyCanaryPassword + "@db.internal:5432/txharbor?sslmode=disable"
	out := RedactedDSN(urlDSN)
	if strings.Contains(out, secrecyCanaryPassword) || strings.Contains(out, secrecyCanaryUser+":"+secrecyCanaryPassword) {
		t.Fatalf("RedactedDSN left the credential in %q", out)
	}
	if !strings.Contains(out, "db.internal:5432") || !strings.Contains(out, "txharbor") {
		t.Fatalf("RedactedDSN(%q) = %q, want host/database context preserved", urlDSN, out)
	}
	if !strings.Contains(out, logx.Redacted) {
		t.Fatalf("RedactedDSN(%q) = %q, want the %s placeholder", urlDSN, out, logx.Redacted)
	}

	keywordDSN := "host=db.internal port=5432 user=txharbor password=" + secrecyCanaryPassword + " dbname=txharbor"
	out = RedactedDSN(keywordDSN)
	if strings.Contains(out, secrecyCanaryPassword) {
		t.Fatalf("RedactedDSN left the keyword credential in %q", out)
	}

	if got := RedactedDSN(""); got != "" {
		t.Fatalf("RedactedDSN(empty) = %q, want empty", got)
	}
	// The result must itself be safe to redact again (stable for logging).
	if again := logx.Redact(RedactedDSN(urlDSN)); strings.Contains(again, secrecyCanaryPassword) {
		t.Fatalf("double redaction leaked: %q", again)
	}
}

// TestSecrecyManifestScanRefusesEmbeddedSecrets pins ScanManifestSecrets:
// a manifest never carries a DSN, credential value or private key material in
// its free-text fields, while a coverage declaration that only NAMES
// signer_private_keys is legitimate (it declares exclusion, not key bytes).
func TestSecrecyManifestScanRefusesEmbeddedSecrets(t *testing.T) {
	clean := &Manifest{
		BackupID: "6f6d1c2e-4a77-4a1e-9d7c-2f5b8c0a1b2c",
		Note:     "local drill input; see contracts/backup-manifest.md",
		Coverage: ManifestCoverage{
			Authoritative: []string{"chain_blocks", "withdrawal_requests"},
			Excluded:      []string{"redis_non_authoritative", "kafka_non_authoritative", "signer_private_keys", "real_credentials"},
		},
		Verification: ManifestVerification{
			State:       VerificationUnverified,
			EvidenceRef: "docs/evidence/015/s2.json",
		},
	}
	if err := ScanManifestSecrets(clean); err != nil {
		t.Fatalf("clean manifest refused: %v", err)
	}

	mutations := []struct {
		name   string
		mutate func(m *Manifest)
		want   string
	}{
		{"note with url dsn", func(m *Manifest) {
			m.Note = "source was postgres://" + secrecyCanaryUser + ":" + secrecyCanaryPassword + "@db.internal/txharbor"
		}, secrecyCanaryPassword},
		{"note with keyword password", func(m *Manifest) {
			m.Note = "restore used password=" + secrecyCanaryPassword
		}, secrecyCanaryPassword},
		{"note with private key hex", func(m *Manifest) {
			m.Note = "private_key=" + secrecyCanaryKeyHex
		}, secrecyCanaryKeyHex},
		{"note with pem block", func(m *Manifest) {
			m.Note = "signer material: " + secrecyCanaryPEM
		}, "PRIVATE KEY"},
		{"evidence ref with bearer token", func(m *Manifest) {
			m.Verification.EvidenceRef = "Authorization: Bearer " + secrecyCanaryToken
		}, secrecyCanaryToken},
		{"created_by with credential", func(m *Manifest) {
			m.CreatedBy = "deploy:executor token=" + secrecyCanaryToken
		}, secrecyCanaryToken},
		{"artifact path with dsn", func(m *Manifest) {
			m.Artifacts = []ManifestArtifact{{Path: "postgres://x:" + secrecyCanaryPassword + "@h/db", Bytes: 1, SHA256: "sha256:aa"}}
		}, secrecyCanaryPassword},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			m := *clean
			tc.mutate(&m)
			err := ScanManifestSecrets(&m)
			if err == nil {
				t.Fatalf("manifest with %s accepted, want refusal (FR-007)", tc.name)
			}
			if strings.Contains(err.Error(), tc.want) {
				t.Fatalf("refusal error echoes the secret: %v", err)
			}
		})
	}
}

// TestSecrecySignerBoundaryReachableWithoutKeyMaterial pins FR-007's Signer
// boundary rule: recovery checks reachability only and its result type can
// never carry private key material.
func TestSecrecySignerBoundaryReachableWithoutKeyMaterial(t *testing.T) {
	if _, err := CheckSignerBoundary(context.Background(), ""); err == nil {
		t.Fatal("CheckSignerBoundary(empty endpoint) = nil error, want refusal by name")
	}
	if _, err := CheckSignerBoundary(context.Background(), "   "); err == nil {
		t.Fatal("CheckSignerBoundary(blank endpoint) = nil error, want refusal by name")
	}

	endpoint := "http://" + secrecyCanaryUser + ":" + secrecyCanaryPassword + "@127.0.0.1:1"
	boundary, err := CheckSignerBoundary(context.Background(), endpoint)
	if err != nil {
		t.Fatalf("CheckSignerBoundary(unreachable endpoint) = %v, want a reachability=false result (not an error)", err)
	}
	if boundary.Reachable {
		t.Fatalf("boundary %+v reports reachable for a closed port", boundary)
	}
	if strings.Contains(boundary.Endpoint, secrecyCanaryPassword) || strings.Contains(boundary.Endpoint, secrecyCanaryUser+":"+secrecyCanaryPassword) {
		t.Fatalf("SignerBoundary.Endpoint leaked the credential: %q", boundary.Endpoint)
	}
	if !strings.Contains(boundary.Endpoint, "127.0.0.1:1") {
		t.Fatalf("SignerBoundary.Endpoint = %q, want the redacted endpoint context", boundary.Endpoint)
	}

	// The result type has no key-material field: adding one is a secrecy
	// contract break, not an implementation detail.
	typ := reflect.TypeOf(SignerBoundary{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		name := strings.ToLower(field.Name + " " + field.Tag.Get("json"))
		for _, forbidden := range []string{"private", "secret", "credential", "key_pem", "pem", "keypair", "mnemonic"} {
			if strings.Contains(name, forbidden) {
				t.Fatalf("SignerBoundary field %q would carry key material (matched %q)", field.Name, forbidden)
			}
		}
	}
	// CheckSignerBoundary takes no key argument either.
	checkType := reflect.TypeOf(CheckSignerBoundary)
	for i := 0; i < checkType.NumIn(); i++ {
		name := strings.ToLower(checkType.In(i).String())
		if strings.Contains(name, "privatekey") || strings.Contains(name, "keymaterial") {
			t.Fatalf("CheckSignerBoundary accepts %q; recovery tools must never receive private keys", checkType.In(i))
		}
	}
}

// TestSecrecyAuditPayloadRefusesDSNOrCredentials pins the existing
// controlstore boundary reused by audit/data_target writes: credential keys
// and DSN-shaped values are refused at any depth, and the real fingerprint
// payload stays credential-free.
func TestSecrecyAuditPayloadRefusesDSNOrCredentials(t *testing.T) {
	bad := []struct {
		name    string
		payload string
		wantErr bool
	}{
		{"top-level dsn key", `{"dsn": "postgres://u:p@h/db"}`, true},
		{"nested password", `{"scope": {"target": {"password": "` + secrecyCanaryPassword + `"}}}`, true},
		{"url value", `{"artifact_ref": "postgres://u:` + secrecyCanaryPassword + `@h/db"}`, true},
		{"credential key in nested object", `{"evidence": {"token": "` + secrecyCanaryToken + `"}}`, true},
		{"private key key", `{"private_key": "` + secrecyCanaryKeyHex + `"}`, true},
		{"clean fingerprint", `{"kind":"pg","database_fingerprint":"sha256:aa","role_fingerprint":"sha256:bb","target_fingerprint":"sha256:cc"}`, false},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			err := controlstore.ValidateDataTarget([]byte(tc.payload))
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateDataTarget(%s) = %v, wantErr = %v", tc.payload, err, tc.wantErr)
			}
			if err != nil && strings.Contains(err.Error(), secrecyCanaryPassword) {
				t.Fatalf("refusal error echoes the credential: %v", err)
			}
		})
	}

	// The real fingerprint payload derived from a DSN never contains the DSN,
	// its password or its user name.
	target, err := controlstore.ParseDSNTarget("postgres://" + secrecyCanaryUser + ":" + secrecyCanaryPassword + "@db.internal:5432/txharbor")
	if err != nil {
		t.Fatalf("ParseDSNTarget: %v", err)
	}
	raw, err := json.Marshal(target.DataTargetFingerprint())
	if err != nil {
		t.Fatalf("marshal fingerprint: %v", err)
	}
	for _, secret := range []string{secrecyCanaryPassword, secrecyCanaryUser, "db.internal", "txharbor"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("fingerprint payload leaked %q: %s", secret, raw)
		}
	}
	if err := controlstore.ValidateDataTarget(raw); err != nil {
		t.Fatalf("fingerprint payload refused by its own validator: %v", err)
	}
}
