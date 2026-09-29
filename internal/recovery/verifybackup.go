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
	// Binding is the deployment-derived opaque isolated target. TargetDSN is
	// only an optional assertion and is never used for connections.
	Binding   IsolatedTarget
	TargetDSN string
	// Verifier is the authenticated principal that ran the verification.
	Verifier string
	// InstanceID is optional: when set, the conclusion is bound to that open
	// instance; when empty, a manifest-level conclusion is recorded.
	InstanceID string
	// ControlStore is required: a verification without a recorded control
	// conclusion is not verification.
	ControlStore *controlstore.Store
	// ControlDSN and AuthoritativeDSN establish the strict deployment topology.
	ControlDSN       string
	AuthoritativeDSN string
	// ObserverDSN is a privileged observer on the target PostgreSQL cluster.
	ObserverDSN string
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
	boundDSN := opts.Binding.BoundDSN()
	if strings.TrimSpace(boundDSN) == "" || strings.TrimSpace(opts.AuthoritativeDSN) == "" || strings.TrimSpace(opts.ControlDSN) == "" || strings.TrimSpace(opts.ObserverDSN) == "" {
		return result, errors.New("verify-backup requires an opaque isolated target binding, authoritative target, control DSN, and privileged observer DSN")
	}
	if strings.TrimSpace(opts.TargetDSN) != "" {
		if err := AssertIsolatedTarget(opts.Binding, opts.TargetDSN); err != nil {
			return result, err
		}
	}
	if strings.TrimSpace(opts.Verifier) == "" {
		return result, errors.New("verify-backup requires the verifying principal")
	}
	if strings.TrimSpace(opts.ProgramVersion) == "" {
		return result, errors.New("verify-backup requires the running program version")
	}
	if opts.PG == nil {
		return result, errors.New("verify-backup requires PGCommand for the four read-only probes")
	}
	if opts.ControlStore == nil {
		return result, errors.New("verify-backup requires the control store; a verification without a recorded conclusion is not verification")
	}
	openBindings, err := readOpenIsolatedBindings(ctx, opts.ControlStore)
	if err != nil {
		return result, fmt.Errorf("re-read open recovery target bindings: %w", err)
	}
	if _, err := BindIsolatedTarget(opts.AuthoritativeDSN, opts.ControlDSN, boundDSN, openBindings); err != nil {
		return result, err
	}
	trustedTarget, err := strictTarget(boundDSN)
	if err != nil {
		return result, errors.New("bound isolated target identity is invalid")
	}
	if trustedTarget.DataTargetFingerprint().TargetFingerprint != opts.Binding.TargetFingerprint() || trustedTarget.DataTargetFingerprint().RoleFingerprint != opts.Binding.RoleFingerprint() {
		return result, errors.New("opaque isolated target binding identity is inconsistent")
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
	serverVersion, err := serverVersionOf(ctx, boundDSN)
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

	// The target-writer coordinator refuses a missing/non-clean guard, supervises
	// and drains the child, runs the required probes, then accepts evidence and
	// guard state in one transaction. Its only DSN is the opaque binding.
	archivePath := resolveArtifactPath(opts.ManifestPath, m.Artifacts[0].Path)
	archive, err := os.Open(archivePath)
	if err != nil {
		return result, fmt.Errorf("open artifact: %w", err)
	}
	defer archive.Close()
	evidenceID := newUUIDString()
	evidenceRef := "control:recovery_evidence/" + evidenceID
	verified := *m
	var outcome probeOutcome
	var finalDigest string
	writer, err := runTargetWriter(ctx, TargetWriterOptions{
		OperationKind: TargetWriterOperationVerifyBackup,
		Store:         opts.ControlStore, ControlDSN: opts.ControlDSN, TargetDSN: boundDSN,
		ObserverDSN: opts.ObserverDSN, TrustedTarget: trustedTarget, IsolatedBinding: opts.Binding,
		AuthoritativeDSN: opts.AuthoritativeDSN, InstanceID: token.InstanceID,
		OperationID: verifyOperationID(opts, &verified), Archive: archive,
		Probe: func(probeCtx context.Context, proof TargetWriterProof) (TargetWriterProbeResult, error) {
			outcome = probeRestoredTarget(probeCtx, opts.PG, opts.ManifestPath, m, boundDSN)
			if !outcome.Checks.AllTrue() {
				return TargetWriterProbeResult{}, errors.New("four verification probes did not all pass")
			}
			evidence, _ := json.Marshal(map[string]any{"checks": outcome.Checks, "business": outcome.Business, "verification": outcome.Verification})
			return TargetWriterProbeResult{Outcome: TargetWriterProbePassed, ApplicationName: proof.Application, Evidence: evidence}, nil
		},
		Acceptance: func(ctx context.Context, tx pgx.Tx, proof TargetWriterProof) (TargetWriterAcceptance, error) {
			verified.Verification = ManifestVerification{State: VerificationVerified,
				VerifiedAt: time.Now().UTC().Format(time.RFC3339), Verifier: opts.Verifier,
				Target: VerificationTargetIsolated, Checks: outcome.Checks, EvidenceRef: evidenceRef}
			canonical, err := verified.CanonicalJSON()
			if err != nil {
				return TargetWriterAcceptance{}, err
			}
			finalDigest, err = verified.Digest()
			if err != nil {
				return TargetWriterAcceptance{}, err
			}
			scope, err := json.Marshal(map[string]any{
				"backup_id": verified.BackupID, "manifest_version": verified.ManifestVersion,
				"manifest_digest": finalDigest, "verifier": opts.Verifier,
				"target_fingerprint": opts.Binding.TargetFingerprint(), "target_role": VerificationTargetIsolated,
				"checks": outcome.Checks, "business_coverage": outcome.Business,
				"verification_coverage": outcome.Verification,
				"coverage_boundary":     "sampled per FR-002 category over the declared authoritative objects; unprovable categories are recorded unknown and never counted as verified",
			})
			if err != nil {
				return TargetWriterAcceptance{}, fmt.Errorf("encode verification evidence scope: %w", err)
			}
			// Publication intentionally precedes DB acceptance. A later failure
			// leaves the guard dirty, so a verified bit alone is non-authoritative.
			if err := writeFileAtomic(opts.ManifestPath, canonical); err != nil {
				return TargetWriterAcceptance{}, err
			}
			var accepted EvidenceToken
			if token.InstanceID != "" {
				write, err := CommitEvidenceWriteTx(ctx, tx, EvidenceWriteRequest{InstanceID: token.InstanceID,
					Token: token, Kind: MutationEvidenceSnapshotAccepted, Actor: opts.Verifier,
					Reason: "isolated restore verification passed; backup_manifest evidence", OperationID: verifyOperationID(opts, &verified),
					ResultDigest: []byte(finalDigest), Apply: func(ctx context.Context, tx pgx.Tx, next EvidenceToken) error {
						if err := insertEvidence(ctx, tx, evidenceID, next.InstanceID, next.Generation, "backup_manifest", scope, finalDigest, evidenceRef, opts.Verifier); err != nil {
							return err
						}
						accepted = next
						return nil
					}})
				if err != nil || write.Discarded {
					return TargetWriterAcceptance{}, errors.New("instance-bound verification evidence was not accepted")
				}
			} else {
				if err := insertUnboundVerifyEvidenceTx(ctx, tx, evidenceID, scope, finalDigest, evidenceRef, opts.Verifier); err != nil {
					return TargetWriterAcceptance{}, err
				}
			}
			var txid int64
			if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&txid); err != nil {
				return TargetWriterAcceptance{}, err
			}
			return TargetWriterAcceptance{AcceptedRowRefs: []string{evidenceRef}, TransactionID: txid, AcceptedEvidenceToken: accepted}, nil
		},
	})
	if err != nil {
		return result, fmt.Errorf("isolated verification was not accepted: %w", err)
	}
	result.State, result.Checks, result.EvidenceRef, result.ManifestDigest = VerificationVerified, outcome.Checks, evidenceRef, finalDigest
	_ = writer
	return result, nil
}

func readOpenIsolatedBindings(ctx context.Context, store *controlstore.Store) ([]IsolatedInstanceBinding, error) {
	rows, err := store.Pool().Query(ctx, `SELECT target_guard_key, target_role_fingerprint FROM recovery_instance WHERE state='open'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var bindings []IsolatedInstanceBinding
	for rows.Next() {
		var b IsolatedInstanceBinding
		if err := rows.Scan(&b.TargetGuardKey, &b.TargetRoleFingerprint); err != nil {
			return nil, err
		}
		bindings = append(bindings, b)
	}
	return bindings, rows.Err()
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
	tx, err := store.Pool().Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := insertUnboundVerifyEvidenceTx(ctx, tx, evidenceID, scope, digest, evidenceRef, verifier); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func insertUnboundVerifyEvidenceTx(ctx context.Context, tx pgx.Tx,
	evidenceID string, scope []byte, digest, evidenceRef, verifier string) error {
	const sql = `
INSERT INTO recovery_evidence
    (evidence_id, instance_id, generation, kind, scope, artifact_hash, artifact_ref, observed_at, collected_by)
VALUES ($1, NULL, 0, 'backup_manifest', $2, $3, $4, now(), $5)`
	if _, err := tx.Exec(ctx, sql, evidenceID, scope, digest, evidenceRef, verifier); err != nil {
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
	return controlstore.WriteAudit(ctx, tx, controlstore.AuditRecord{
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
