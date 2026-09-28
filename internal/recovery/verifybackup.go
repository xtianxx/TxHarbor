// verifybackup.go implements T020: isolated restore verification
// (`recovery-admin verify-backup`), quickstart S2 and contracts/
// backup-manifest.md §3.
//
// A backup is `verified` only after at least one real isolated restore: a real
// pg_restore --clean --if-exists into the target plus the four checks
// (readable / structure_constraints / business_state_probes /
// verification_executable). Listing a file or re-initializing an empty
// database is never verification, and "the backup command exited 0" never
// proves a restore either.
//
// Evidence binding (DG-2, data-model §1.4):
//
//   - with a recovery instance: the conclusion is accepted through the
//     data-model §5 generation protocol as a backup_manifest evidence row
//     bound to the open instance, and the manifest write-back records
//     verified_at/verifier/target=isolated/evidence_ref;
//   - without an instance: the manifest-level conclusion is recorded with
//     instance_id NULL (generation 0) and audited; a later controlled command
//     may explicitly sync/bind it - this row must never pretend to be
//     instance-bound;
//   - the evidence records the digest of the final written manifest, so a
//     copied or edited manifest (including hand-flipped verification flags)
//     can never inherit an existing verification.
package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// VerifyBackupOptions is one isolated verification execution.
type VerifyBackupOptions struct {
	ManifestPath string
	TargetDSN    string
	// Verifier is the authenticated principal that ran the verification.
	Verifier string
	// InstanceID is optional: when set, the conclusion is bound to that open
	// instance; when empty, a manifest-level conclusion is recorded.
	InstanceID string
	// ControlStore is required: a verification without a recorded control
	// conclusion is not verification.
	ControlStore *controlstore.Store
	// ControlDSN, when set, is parsed only to prove the isolated target is
	// not the control-store database.
	ControlDSN string
	// ProgramVersion is the running program identity.
	ProgramVersion string
	// OperationID is the idempotency key recorded on accepted evidence.
	OperationID string
	// PG executes pg_restore (real tool chain only).
	PG PGCommand
}

// VerifyBackupResult reports the verification conclusion. State is
// unverified/rejected/verified; verified is only returned when all four
// checks passed on a real restore.
type VerifyBackupResult struct {
	State          VerificationState `json:"state"`
	Checks         ManifestChecks    `json:"checks"`
	EvidenceRef    string            `json:"evidence_ref,omitempty"`
	ManifestPath   string            `json:"manifest_path"`
	ManifestDigest string            `json:"manifest_digest,omitempty"`
}

// ExecuteVerifyBackup runs the real isolated verification of one manifest.
// A structurally broken document is an error; an integrity/compatibility
// refusal is recorded as `rejected` (write-back, no evidence); a successful
// real restore with all four checks records `verified` plus the control-store
// evidence.
func ExecuteVerifyBackup(ctx context.Context, opts VerifyBackupOptions) (VerifyBackupResult, error) {
	result := VerifyBackupResult{ManifestPath: opts.ManifestPath}
	if strings.TrimSpace(opts.ManifestPath) == "" {
		return result, errors.New("verify-backup requires a manifest path")
	}
	if strings.TrimSpace(opts.TargetDSN) == "" {
		return result, errors.New("verify-backup requires an isolated target DSN")
	}
	if strings.TrimSpace(opts.Verifier) == "" {
		return result, errors.New("verify-backup requires the verifying principal")
	}
	if strings.TrimSpace(opts.ProgramVersion) == "" {
		return result, errors.New("verify-backup requires the running program version")
	}
	if opts.PG == nil {
		return result, errors.New("verify-backup requires a PGCommand (the real pg_restore path); refusing to fake verification")
	}
	if opts.ControlStore == nil {
		return result, errors.New("verify-backup requires the control store; a verification without a recorded conclusion is not verification")
	}
	if strings.TrimSpace(opts.ControlDSN) != "" {
		controlTarget, err := controlstore.ParseDSNTarget(opts.ControlDSN)
		if err != nil {
			return result, fmt.Errorf("control DSN is invalid: %s", logx.Redact(err.Error()))
		}
		target, err := controlstore.ParseDSNTarget(opts.TargetDSN)
		if err != nil {
			return result, fmt.Errorf("target DSN is invalid: %s", logx.Redact(err.Error()))
		}
		if controlTarget.SameDatabase(target) {
			return result, errors.New("the verification target addresses the control-store database; refusing")
		}
	}

	m, err := readManifestFile(opts.ManifestPath)
	if err != nil {
		return result, err
	}
	result.State = m.Verification.State

	// Instance-bound verification needs the open instance token captured
	// before the result is derived (data-model §5); a closed/missing instance
	// refuses before any restore.
	var token EvidenceToken
	if strings.TrimSpace(opts.InstanceID) != "" {
		token, err = CaptureEvidenceToken(ctx, opts.ControlStore.Pool(), opts.InstanceID)
		if err != nil {
			return result, fmt.Errorf("verify-backup requires an open recovery instance: %w", err)
		}
		if token.State != "open" {
			return result, fmt.Errorf("verify-backup instance %s is not open (state=%s)", token.InstanceID, token.State)
		}
	}

	// Integrity first: a corrupt/truncated/hash-mismatched artifact is
	// `rejected` with a manifest write-back and no evidence.
	digest, integrityErr := checkManifestIntegrity(opts.ManifestPath, m)
	if integrityErr != nil {
		rejected := *m
		rejected.Verification = ManifestVerification{State: VerificationRejected, Verifier: opts.Verifier}
		if writeErr := writeVerifiedManifest(opts.ManifestPath, &rejected); writeErr != nil {
			return result, fmt.Errorf("verification refused (%v) and the rejected conclusion could not be recorded: %w", integrityErr, writeErr)
		}
		result.State = VerificationRejected
		return result, nil
	}
	result.ManifestDigest = digest

	// Compatibility: a schema/program/carrier mismatch is refused without
	// touching the target (F3: zero silent downgrade/rewrite).
	serverVersion, err := serverVersionOf(ctx, opts.TargetDSN)
	if err != nil {
		return result, fmt.Errorf("verification target is unreachable: %s", logx.Redact(err.Error()))
	}
	serverMajor, err := majorVersion(serverVersion)
	if err != nil {
		return result, fmt.Errorf("verification target server version %q is unparsable", serverVersion)
	}
	targetSchema, err := RepositoryTargetSchema()
	if err != nil {
		return result, err
	}
	if err := m.Validate(ManifestValidation{
		PGServerMajor:  serverMajor,
		TargetSchema:   targetSchema,
		ProgramVersion: opts.ProgramVersion,
	}); err != nil {
		rejected := *m
		rejected.Verification = ManifestVerification{State: VerificationRejected, Verifier: opts.Verifier}
		if writeErr := writeVerifiedManifest(opts.ManifestPath, &rejected); writeErr != nil {
			return result, fmt.Errorf("verification refused (%v) and the rejected conclusion could not be recorded: %w", err, writeErr)
		}
		result.State = VerificationRejected
		return result, nil
	}

	// Real isolated restore.
	archivePath := resolveArtifactPath(opts.ManifestPath, m.Artifacts[0].Path)
	archive, err := os.Open(archivePath)
	if err != nil {
		return result, fmt.Errorf("open artifact: %w", err)
	}
	restoreErr := pgRestoreInto(ctx, opts.PG, archive, opts.TargetDSN)
	_ = archive.Close()
	if restoreErr != nil {
		rejected := *m
		rejected.Verification = ManifestVerification{State: VerificationRejected, Verifier: opts.Verifier}
		if writeErr := writeVerifiedManifest(opts.ManifestPath, &rejected); writeErr != nil {
			return result, fmt.Errorf("verification refused (%v) and the rejected conclusion could not be recorded: %w", restoreErr, writeErr)
		}
		result.State = VerificationRejected
		return result, nil
	}

	// Four checks on the real restored target.
	outcome := probeRestoredTarget(ctx, opts.PG, opts.ManifestPath, m, opts.TargetDSN)
	result.Checks = outcome.Checks
	if !outcome.Checks.AllTrue() {
		rejected := *m
		rejected.Verification = ManifestVerification{
			State: VerificationRejected, Verifier: opts.Verifier, Checks: outcome.Checks,
		}
		if writeErr := writeVerifiedManifest(opts.ManifestPath, &rejected); writeErr != nil {
			return result, fmt.Errorf("verification probes failed (%s) and the rejected conclusion could not be recorded: %w",
				strings.Join(outcome.Problems, "; "), writeErr)
		}
		result.State = VerificationRejected
		return result, nil
	}

	// Accept: build the final manifest (with the evidence reference), compute
	// its digest, record the control-store evidence first and publish the
	// manifest only after the evidence is durable. A failure in either step
	// leaves a manifest that restore still refuses (no verified flag and/or no
	// matching evidence).
	evidenceID := newUUIDString()
	evidenceRef := "control:recovery_evidence/" + evidenceID
	verified := *m
	verified.Verification = ManifestVerification{
		State:       VerificationVerified,
		VerifiedAt:  time.Now().UTC().Format(time.RFC3339),
		Verifier:    opts.Verifier,
		Target:      VerificationTargetIsolated,
		Checks:      outcome.Checks,
		EvidenceRef: evidenceRef,
	}
	canonical, err := verified.CanonicalJSON()
	if err != nil {
		return result, err
	}
	finalDigest, err := verified.Digest()
	if err != nil {
		return result, err
	}
	targetFingerprint := ""
	if target, err := controlstore.ParseDSNTarget(opts.TargetDSN); err == nil {
		targetFingerprint = target.DataTargetFingerprint().TargetFingerprint
	}
	scope, err := json.Marshal(map[string]any{
		"backup_id":             verified.BackupID,
		"manifest_version":      verified.ManifestVersion,
		"manifest_digest":       finalDigest,
		"verifier":              opts.Verifier,
		"target_fingerprint":    targetFingerprint,
		"target_role":           VerificationTargetIsolated,
		"checks":                outcome.Checks,
		"business_coverage":     outcome.Business,
		"verification_coverage": outcome.Verification,
		"coverage_boundary":     "sampled per FR-002 category over the declared authoritative objects; unprovable categories are recorded unknown and never counted as verified",
	})
	if err != nil {
		return result, fmt.Errorf("encode verification evidence scope: %w", err)
	}

	if token.InstanceID != "" {
		written, err := CommitEvidenceWrite(ctx, opts.ControlStore, EvidenceWriteRequest{
			InstanceID:   token.InstanceID,
			Token:        token,
			Kind:         MutationEvidenceSnapshotAccepted,
			Actor:        opts.Verifier,
			Reason:       "isolated restore verification passed; backup_manifest evidence",
			OperationID:  verifyOperationID(opts, &verified),
			ResultDigest: []byte(finalDigest),
			Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
				return insertEvidence(ctx, tx, evidenceID, accepted.InstanceID, accepted.Generation,
					"backup_manifest", scope, finalDigest, evidenceRef, opts.Verifier)
			},
		})
		if err != nil {
			return result, fmt.Errorf("verification evidence could not be recorded: %w", err)
		}
		if written.Discarded {
			return result, fmt.Errorf("verification evidence was discarded: evidence changed during the verification (%s); re-run verify-backup", written.DiscardReason)
		}
	} else {
		// DG-2 manifest-level conclusion: explicitly unbound (instance_id
		// NULL, generation 0) and audited; it never pretends to be
		// instance-bound and a later controlled command must sync-bind it.
		if err := insertUnboundVerifyEvidence(ctx, opts.ControlStore, evidenceID, scope, finalDigest, evidenceRef, opts.Verifier); err != nil {
			return result, err
		}
	}

	if err := writeFileAtomic(opts.ManifestPath, canonical); err != nil {
		return result, err
	}
	result.State = VerificationVerified
	result.Checks = outcome.Checks
	result.EvidenceRef = evidenceRef
	result.ManifestDigest = finalDigest
	return result, nil
}

// writeVerifiedManifest publishes the (possibly rejected) manifest body.
func writeVerifiedManifest(path string, m *Manifest) error {
	canonical, err := m.CanonicalJSON()
	if err != nil {
		return err
	}
	return writeFileAtomic(path, canonical)
}

// insertUnboundVerifyEvidence records a manifest-level (not instance-bound)
// verification conclusion: instance_id NULL, generation 0, plus an audit row
// stating the unbound state explicitly (DG-2; never claims instance binding).
func insertUnboundVerifyEvidence(ctx context.Context, store *controlstore.Store,
	evidenceID string, scope []byte, digest, evidenceRef, verifier string) error {
	const sql = `
INSERT INTO recovery_evidence
    (evidence_id, instance_id, generation, kind, scope, artifact_hash, artifact_ref, observed_at, collected_by)
VALUES ($1, NULL, 0, 'backup_manifest', $2, $3, $4, now(), $5)`
	if _, err := store.Pool().Exec(ctx, sql, evidenceID, scope, digest, evidenceRef, verifier); err != nil {
		return fmt.Errorf("insert manifest-level backup_manifest evidence: %w", err)
	}
	detail, err := json.Marshal(map[string]any{
		"evidence_id":  evidenceID,
		"evidence_ref": evidenceRef,
		"binding":      "manifest_level_unbound",
		"note":         "recorded before an instance was open; must be explicitly sync-bound and audited later (DG-2)",
	})
	if err != nil {
		return err
	}
	return controlstore.WriteAudit(ctx, store.Pool(), controlstore.AuditRecord{
		Actor:  verifier,
		Action: "verify_backup",
		Detail: detail,
		Result: controlstore.AuditOK,
	})
}

// verifyOperationID derives a bounded operation id when the caller did not
// supply one.
func verifyOperationID(opts VerifyBackupOptions, m *Manifest) string {
	if strings.TrimSpace(opts.OperationID) != "" {
		return strings.TrimSpace(opts.OperationID)
	}
	return "verify:" + m.BackupID
}
