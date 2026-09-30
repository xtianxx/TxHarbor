package recovery

import (
	"errors"
	"slices"
	"testing"
)

func validManifestForUnitTest() *Manifest {
	return &Manifest{
		ManifestVersion: ManifestVersionV1,
		BackupID:        "6f6d1c2e-4a77-4a1e-9d7c-2f5b8c0a1b2c",
		CreatedAt:       "2026-09-28T12:00:00Z",
		CreatedBy:       "deploy:executor",
		Carrier:         ManifestCarrier{Kind: CarrierKindPGDumpCustom, PGServerVersion: "18.6", PGDumpVersion: "18.6"},
		Coverage:        ManifestCoverage{Authoritative: CanonicalAuthoritativeObjects(), Excluded: CanonicalExcludedObjects()},
		RecoveryPoint: ManifestRecoveryPoint{
			Snapshot: ManifestSnapshot{Xmin: 769, Xmax: 770, Xip: []int64{769}},
			LSN:      "0/1C26D90", WallClock: "2026-09-28T12:00:00Z", Server: "txharbor-pg", Database: "txharbor",
		},
		Schema:  ManifestSchema{GooseDBVersion: []int64{1, 2, 3}},
		Program: ManifestProgram{Version: "018.0", MinCompatible: "018.0"},
		Artifacts: []ManifestArtifact{{
			Path: "backup.pgcustom", Bytes: 2048,
			SHA256: "sha256:4f1e2d3c4b5a69788796a5b4c3d2e1f04f1e2d3c4b5a69788796a5b4c3d2e1f0",
		}},
		Verification: ManifestVerification{
			State: VerificationVerified, VerifiedAt: "2026-09-28T12:05:00Z", Verifier: "auth:verifier",
			Target: VerificationTargetIsolated, EvidenceRef: "evidence/backup.json",
			Checks: ManifestChecks{Readable: true, StructureConstraints: true, BusinessStateProbes: true, VerificationExecutable: true},
		},
	}
}

func manifestUnitValidation() ManifestValidation {
	return ManifestValidation{PGServerMajor: 18, TargetSchema: []int64{1, 2, 3}, ProgramVersion: "018.0"}
}

func TestManifestUnitRequiresCompleteNineCategoryCoverage(t *testing.T) {
	// These are the existing object labels in generated manifests, grouped by
	// the nine FR-002 categories. Do not introduce a parallel category-label
	// vocabulary in the manifest wire format.
	categories := map[string][]string{
		"chain identity/cursor":                   {"chain_blocks", "indexer_checkpoint"},
		"events":                                  {"erc20_transfer_logs"},
		"deposit confirmation":                    {"deposit_checkpoint", "deposit_observation_transitions", "confirmation_policy_history"},
		"withdrawal requests/payment intents":     {"withdrawal_requests", "payment_intents"},
		"outbound transactions/signing/broadcast": {"signing_requests", "tx_send_attempts", "tx_receipts"},
		"nonce allocation/occupancy":              {"nonce_bindings", "nonce_observations"},
		"Outbox/obligation markers":               {"outbox_events", "event_obligation"},
		"consumer idempotency/progress":           {"consumer_inbox", "consumer_progress"},
		"audit/permissions/014 differences":       {"recon_task", "recon_audit", "withdrawal_request_audit"},
	}
	if len(categories) != 9 {
		t.Fatalf("coverage taxonomy contains %d categories, want 9", len(categories))
	}

	base := validManifestForUnitTest()
	if err := base.Validate(manifestUnitValidation()); err != nil {
		t.Fatalf("complete canonical coverage rejected: %v", err)
	}
	for category, objects := range categories {
		for _, object := range objects {
			t.Run(category+"/missing_"+object, func(t *testing.T) {
				m := validManifestForUnitTest()
				m.Coverage.Authoritative = slices.DeleteFunc(m.Coverage.Authoritative, func(value string) bool {
					return value == object
				})
				if err := m.Validate(manifestUnitValidation()); !errors.Is(err, ErrManifestInvalid) {
					t.Fatalf("Validate without %s = %v, want ErrManifestInvalid", object, err)
				}
			})
		}
	}

	for _, object := range []string{"unknown_object", "chain_blocks"} {
		t.Run("extra_or_duplicate_"+object, func(t *testing.T) {
			m := validManifestForUnitTest()
			m.Coverage.Authoritative = append(m.Coverage.Authoritative, object)
			if err := m.Validate(manifestUnitValidation()); !errors.Is(err, ErrManifestInvalid) {
				t.Fatalf("Validate with extra/duplicate %s = %v, want ErrManifestInvalid", object, err)
			}
		})
	}
}

func TestManifestUnitRequiresAllCanonicalExclusions(t *testing.T) {
	for _, omitted := range CanonicalExcludedObjects() {
		t.Run("missing_"+omitted, func(t *testing.T) {
			m := validManifestForUnitTest()
			m.Coverage.Excluded = slices.DeleteFunc(m.Coverage.Excluded, func(value string) bool { return value == omitted })
			if err := m.Validate(manifestUnitValidation()); !errors.Is(err, ErrManifestInvalid) {
				t.Fatalf("Validate without exclusion %s = %v, want ErrManifestInvalid", omitted, err)
			}
		})
	}
}

func TestManifestUnitRequiresCanonicalArtifactSHA256(t *testing.T) {
	for _, sha := range []string{"", "sha256:aa", "sha256:" + string(make([]byte, 64)), "sha256:4F1E2D3C4B5A69788796A5B4C3D2E1F04F1E2D3C4B5A69788796A5B4C3D2E1F0"} {
		t.Run("invalid_sha", func(t *testing.T) {
			m := validManifestForUnitTest()
			m.Artifacts[0].SHA256 = sha
			if err := m.Validate(manifestUnitValidation()); !errors.Is(err, ErrManifestInvalid) {
				t.Fatalf("Validate with SHA %q = %v, want ErrManifestInvalid", sha, err)
			}
		})
	}
}
