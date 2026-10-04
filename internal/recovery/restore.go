// restore.go implements T019: the restore executor and its preconditions,
// target guards, probes and interruption discipline (contracts/
// backup-manifest.md §2/§3, data-model.md §7; FR-004/005/008).
//
// Preconditions (any missing item is a blocked state with an explicit list,
// never a claimed recovery):
//
//   - the named recovery instance exists and is open;
//   - the manifest is verified AND validates against the running program
//     (carrier/schema/program compatibility, exact goose set, full recovery
//     point);
//   - the control store holds a verified evidence row bound to this instance
//     and this backup_id whose recorded digest/scope equal the current
//     manifest body (F7/DG-2: a manifest-file flag alone is never proof, and
//     copying/editing a manifest does not inherit verification);
//   - artifact integrity (size + sha256 + pg_restore -l) holds;
//   - the target DSN is reachable and is not the control store; the default
//     declaration is isolated-only, production_main requires an explicit
//     recorded reason.
//
// Execution: an accepted pre-write invalidation marker (restore_started,
// through the same data-model §5 generation protocol) advances the evidence
// generation before the first target write, so every release/approval bound to
// the pre-restore generation stops being usable for admission even if the
// restore then fails or is interrupted. Then a real pg_restore --clean
// --if-exists into the target, then the four probes (readable /
// structure_constraints / business_state_probes / verification_executable).
// Only when all four pass is a restore_probe evidence row accepted and
// Restored reported. An interruption is never marked restored and writes no
// restored evidence (the marker stays: the old authorization basis does not
// come back); the documented retry is: rebuild the target database, rerun.
//
// Signer boundary: the tool checks reachability only and never receives,
// reads or exports key material (FR-007).
package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// TargetDeclaration is the closed set of restore target declarations. The
// default is isolated-only; a production main database requires the explicit
// declaration and a recorded reason.
type TargetDeclaration string

// The restore target declarations of T019.
const (
	TargetIsolated       TargetDeclaration = "isolated"
	TargetProductionMain TargetDeclaration = "production_main"
)

// RestoreOptions is one restore execution.
type RestoreOptions struct {
	ManifestPath string
	InstanceID   string
	// ControlStore is the opened control store; its evidence is required
	// (a manifest-file flag is never proof).
	ControlStore *controlstore.Store
	// ControlDSN is parsed only, to prove the target is not the control
	// store database (the control store is outside the data restore set).
	ControlDSN string
	// ObserverDSN is an explicit connection with visibility into tagged target
	// sessions. It is never inferred from TargetDSN.
	ObserverDSN string
	// TargetDSN is the explicit restore target.
	TargetDSN string
	// GateDSN is the R3 protected-lane conninfo (passwordless local socket,
	// restricted recovery role). Empty keeps the historical TargetDSN route
	// (unprotected lane); non-empty switches the supervised child transport
	// to it while every comparator stays on TargetDSN. Never audit or log.
	GateDSN string
	// TargetDeclaration defaults to isolated.
	TargetDeclaration TargetDeclaration
	// TargetReason is required for a production_main declaration and recorded.
	TargetReason string
	// Actor is the authenticated principal.
	Actor string
	// ProgramVersion is the running program identity used for compatibility.
	ProgramVersion string
	// OperationID is the idempotency key recorded on the accepted evidence.
	OperationID string
	// PG executes pg_restore. Required (real tool chain only).
	PG PGCommand
	// Optional dependency probes (V9): when configured and unreachable the
	// restore is blocked; when not configured the check is reported as
	// not_configured, never as passed.
	SignerEndpoint string
	RPCURL         string
	BrokerDSN      string
}

// DependencyCheck is one dependency-probe result of the restore preconditions
// (FR-005/V9). state is one of ok|failed|not_configured|unknown.
type DependencyCheck struct {
	Name   string `json:"name"`
	State  string `json:"state"`
	Detail string `json:"detail,omitempty"`
}

// RestoreResult reports one restore attempt. Blocked carries the missing
// preconditions of a refusal; it is never empty on a refusal.
type RestoreResult struct {
	Restored          bool              `json:"restored"`
	Blocked           []string          `json:"blocked,omitempty"`
	Declaration       TargetDeclaration `json:"declaration"`
	ManifestDigest    string            `json:"manifest_digest,omitempty"`
	TargetFingerprint string            `json:"target_fingerprint,omitempty"`
	EvidenceRef       string            `json:"evidence_ref,omitempty"`
	Checks            ManifestChecks    `json:"checks"`
	Dependencies      []DependencyCheck `json:"dependencies,omitempty"`
}

// restoreRun accumulates the blocked list of one attempt.
type restoreRun struct {
	opts    RestoreOptions
	result  RestoreResult
	blocked []string
}

func (r *restoreRun) require(ok bool, reason string) bool {
	if !ok {
		r.blocked = append(r.blocked, reason)
	}
	return ok
}

func (r *restoreRun) fail() (RestoreResult, error) {
	r.result.Blocked = append([]string(nil), r.blocked...)
	return r.result, fmt.Errorf("restore refused (%d missing precondition(s)): %s",
		len(r.blocked), strings.Join(r.blocked, "; "))
}

// ExecuteRestore restores one verified manifest into the explicit target,
// binding the result to the open recovery instance. Every refusal returns a
// non-nil error and a Restored=false result with the blocked list populated;
// the target is never touched before all preconditions hold.
func ExecuteRestore(ctx context.Context, opts RestoreOptions) (RestoreResult, error) {
	result, _, err := executeRestoreWithTargetWriter(ctx, opts, defaultTargetWriterDriver())
	return result, err
}

type targetWriterDriver func(context.Context, TargetWriterOptions) (TargetWriterResult, error)

func defaultTargetWriterDriver() targetWriterDriver { return runTargetWriter }

// executeRestoreWithBorrowedTargetLock is a package-private drill seam. The
// supplied lock remains caller-owned; it is never released by this helper.
// It adds no target-cleanliness, authorization, or acceptance bypass.
func executeRestoreWithBorrowedTargetLock(ctx context.Context, opts RestoreOptions, lock *TargetLock) (RestoreResult, TargetWriterResult, error) {
	return executeRestoreWithTargetWriter(ctx, opts, func(runCtx context.Context, writerOpts TargetWriterOptions) (TargetWriterResult, error) {
		writerOpts.borrowedLock = lock
		return runTargetWriter(runCtx, writerOpts)
	})
}

func executeRestoreWithTargetWriter(ctx context.Context, opts RestoreOptions, driver targetWriterDriver) (RestoreResult, TargetWriterResult, error) {
	run := &restoreRun{opts: opts}
	var writerResult TargetWriterResult
	fail := func() (RestoreResult, TargetWriterResult, error) {
		result, err := run.fail()
		return result, writerResult, err
	}
	if driver == nil {
		run.blocked = append(run.blocked, "target writer driver is required")
		return fail()
	}
	run.result.Declaration = opts.TargetDeclaration
	if run.result.Declaration == "" {
		run.result.Declaration = TargetIsolated
	}
	// TargetReason is persisted in restore audit/evidence detail. Reject
	// credential-shaped annotations before any precondition work can write an
	// audit row, advance evidence, or touch the target. Keep the refusal generic:
	// the supplied text must never enter an error, log, or audit record.
	if err := ValidateRestoreTargetReason(opts.TargetReason); err != nil {
		run.blocked = append(run.blocked, err.Error())
		return fail()
	}

	// Structural preconditions.
	run.require(strings.TrimSpace(opts.ManifestPath) != "", "manifest path is required")
	run.require(strings.TrimSpace(opts.TargetDSN) != "", "target DSN is required")
	run.require(strings.TrimSpace(opts.ControlDSN) != "", "control DSN is required")
	run.require(strings.TrimSpace(opts.ObserverDSN) != "", "explicit target observer DSN is required")
	run.require(strings.TrimSpace(opts.InstanceID) != "", "recovery instance id is required")
	run.require(strings.TrimSpace(opts.Actor) != "", "restore actor (authenticated principal) is required")
	run.require(strings.TrimSpace(opts.ProgramVersion) != "", "running program version is required")
	run.require(opts.ControlStore != nil, "control store is required (manifest-file flags are not verification proof)")
	run.require(opts.PG != nil, "a PGCommand (the real pg_restore path) is required")
	switch run.result.Declaration {
	case TargetIsolated:
	case TargetProductionMain:
		run.require(strings.TrimSpace(opts.TargetReason) != "",
			"production_main requires an explicit recorded reason (--reason)")
	default:
		run.require(false, fmt.Sprintf("unknown target declaration %q (want isolated|production_main)", opts.TargetDeclaration))
	}
	if len(run.blocked) > 0 {
		return fail()
	}

	// Target guard: the control store database can never be a restore target.
	controlTarget, err := controlstore.ParseDSNTarget(opts.ControlDSN)
	if err != nil {
		run.blocked = append(run.blocked, "control DSN is invalid: "+logx.Redact(err.Error()))
		return fail()
	}
	targetTarget, err := controlstore.ParseDSNTarget(opts.TargetDSN)
	if err != nil {
		run.blocked = append(run.blocked, "target DSN is invalid: "+logx.Redact(err.Error()))
		return fail()
	}
	if !run.require(!controlTarget.SameDatabase(targetTarget),
		"the target DSN addresses the control-store database; the control store is outside the data restore set") {
		return fail()
	}
	run.result.TargetFingerprint = targetTarget.DataTargetFingerprint().TargetFingerprint

	// Manifest: parse, integrity, compatibility, verification.
	m, err := readManifestFile(opts.ManifestPath)
	if err != nil {
		run.blocked = append(run.blocked, logx.Redact(err.Error()))
		return fail()
	}
	digest, err := checkManifestIntegrity(opts.ManifestPath, m)
	if err != nil {
		run.blocked = append(run.blocked, err.Error())
		return fail()
	}
	run.result.ManifestDigest = digest

	serverVersion, err := serverVersionOf(ctx, opts.TargetDSN)
	if err != nil {
		run.blocked = append(run.blocked, "target DSN is unreachable: "+logx.Redact(err.Error()))
		return fail()
	}
	serverMajor, err := majorVersion(serverVersion)
	if err != nil {
		run.blocked = append(run.blocked, fmt.Sprintf("target server version %q is unparsable", serverVersion))
		return fail()
	}
	targetSchema, err := RepositoryTargetSchema()
	if err != nil {
		run.blocked = append(run.blocked, err.Error())
		return fail()
	}
	validation := ManifestValidation{
		PGServerMajor:  serverMajor,
		TargetSchema:   targetSchema,
		ProgramVersion: opts.ProgramVersion,
	}
	if err := m.CheckUsable(ManifestUseRestore, validation); err != nil {
		run.blocked = append(run.blocked, "manifest is not usable for restore: "+err.Error())
		return fail()
	}

	// Open instance + control-store evidence bound to this backup_id.
	token, err := CaptureEvidenceToken(ctx, opts.ControlStore.Pool(), opts.InstanceID)
	if err != nil {
		run.blocked = append(run.blocked, "recovery instance is not usable: "+err.Error())
		return fail()
	}
	if !run.require(token.State == "open",
		"recovery instance "+token.InstanceID+" is not open (state="+token.State+")") {
		return fail()
	}
	verificationBinding, evidenceMatches, epochErr := restoreBackupVerificationBinding(ctx, opts.ControlStore, token.InstanceID, m, digest, run)
	if epochErr != nil {
		run.blocked = append(run.blocked, "control-store verification receipt lookup failed: "+logx.Redact(epochErr.Error()))
		return fail()
	}
	if !run.require(evidenceMatches,
		"control store has no current verified receipt bound to this instance and backup_id matching the manifest digest; re-run verify-backup (F7/DG-2)") {
		return fail()
	}

	// Optional dependency probes (configured-and-unreachable blocks).
	if !run.dependencies(ctx) {
		return fail()
	}

	// Open the artifact before the marker: a missing artifact is refused
	// without invalidating anything.
	archivePath := resolveArtifactPath(opts.ManifestPath, m.Artifacts[0].Path)
	archive, err := os.Open(archivePath)
	if err != nil {
		run.blocked = append(run.blocked, "artifact cannot be opened: "+err.Error())
		return fail()
	}

	defer archive.Close()
	trustedTarget, err := controlstore.ParseDSNTarget(opts.TargetDSN)
	if err != nil {
		run.blocked = append(run.blocked, "target identity is invalid")
		return fail()
	}
	operationID := restoreOperationID(opts, m)
	evidenceID := newUUIDString()
	evidenceRef := "control:recovery_evidence/" + evidenceID
	var outcome probeOutcome
	writerResult, err = driver(ctx, TargetWriterOptions{
		OperationKind: TargetWriterOperationRestore,
		Store:         opts.ControlStore, ControlDSN: opts.ControlDSN, TargetDSN: opts.TargetDSN,
		ObserverDSN: opts.ObserverDSN, TrustedTarget: trustedTarget, InstanceID: token.InstanceID,
		OperationID: operationID, Archive: archive,
		RecoveryRoute: opts.GateDSN,
		Prelaunch: func(ctx context.Context, tx pgx.Tx, locked controlstore.InstanceToken) (EvidenceToken, error) {
			// TargetWriter has locked the instance row before this callback.
			// Recheck under the per-backup epoch lock before taking the target
			// guard or writing the restore-start marker.
			if _, err := controlstore.LockInstance(ctx, tx, locked.InstanceID); err != nil {
				return EvidenceToken{}, err
			}
			if err := lockBackupVerificationEpoch(ctx, tx, m.BackupID); err != nil {
				return EvidenceToken{}, err
			}
			latest, err := latestBackupVerificationEpoch(ctx, tx, m.BackupID)
			if err != nil {
				return EvidenceToken{}, err
			}
			if verificationBinding.Legacy {
				if latest != 0 {
					return EvidenceToken{}, errors.New("legacy backup verification was revoked by a verification epoch")
				}
			} else if latest != verificationBinding.EpochID {
				return EvidenceToken{}, errors.New("backup verification receipt was superseded before restore launch")
			}
			write, err := CommitEvidenceWriteTx(ctx, tx, EvidenceWriteRequest{
				InstanceID: locked.InstanceID, Token: LockedEvidenceToken(locked), Kind: MutationRestoreStarted,
				Actor: opts.Actor, Reason: "restore started: pre-write invalidation of the previous evidence generation",
				OperationID: operationID, ResultDigest: []byte(digest),
				Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
					return insertRestoreStartedMarker(ctx, tx, accepted, m, digest,
						run.result.TargetFingerprint, string(run.result.Declaration), opts)
				},
			})
			if err != nil {
				return EvidenceToken{}, err
			}
			if write.Discarded {
				return EvidenceToken{}, errors.New("restore start marker discarded")
			}
			return write.Token, nil
		},
		Probe: func(ctx context.Context, proof TargetWriterProof) (TargetWriterProbeResult, error) {
			probeDSN, err := ConninfoWithAttemptApplicationName(opts.TargetDSN, proof.Application)
			if err != nil {
				return TargetWriterProbeResult{}, err
			}
			outcome = probeRestoredTarget(ctx, opts.PG, opts.ManifestPath, m, probeDSN)
			if !outcome.Checks.AllTrue() {
				return TargetWriterProbeResult{}, errors.New("restore probes did not all pass")
			}
			basis, err := json.Marshal(outcome)
			if err != nil {
				return TargetWriterProbeResult{}, err
			}
			return TargetWriterProbeResult{Outcome: TargetWriterProbePassed, ApplicationName: proof.Application, Evidence: basis}, nil
		},
		Acceptance: func(ctx context.Context, tx pgx.Tx, proof TargetWriterProof) (TargetWriterAcceptance, error) {
			if _, err := controlstore.LockInstance(ctx, tx, proof.MarkerToken.InstanceID); err != nil {
				return TargetWriterAcceptance{}, err
			}
			if err := lockBackupVerificationEpoch(ctx, tx, m.BackupID); err != nil {
				return TargetWriterAcceptance{}, err
			}
			latest, err := latestBackupVerificationEpoch(ctx, tx, m.BackupID)
			if err != nil {
				return TargetWriterAcceptance{}, err
			}
			if verificationBinding.Legacy {
				if latest != 0 {
					return TargetWriterAcceptance{}, errors.New("legacy backup verification was revoked before restore acceptance")
				}
			} else if latest != verificationBinding.EpochID {
				return TargetWriterAcceptance{}, errors.New("backup verification receipt was superseded before restore acceptance")
			}
			scope, err := json.Marshal(map[string]any{
				"backup_id": m.BackupID, "manifest_version": m.ManifestVersion, "manifest_digest": digest,
				"target_fingerprint": run.result.TargetFingerprint, "target_role": string(run.result.Declaration),
				"target_reason": strings.TrimSpace(opts.TargetReason), "checks": outcome.Checks,
				"business_coverage": outcome.Business, "verification_coverage": outcome.Verification,
				"coverage_boundary": "sampled per FR-002 category over the declared authoritative objects; unprovable categories are recorded unknown and never counted as restored",
				"writer_probe":      json.RawMessage(proof.Probe.Evidence),
			})
			if err != nil {
				return TargetWriterAcceptance{}, err
			}
			write, err := CommitEvidenceWriteTx(ctx, tx, EvidenceWriteRequest{
				InstanceID: proof.MarkerToken.InstanceID, Token: proof.MarkerToken,
				Kind: MutationRestoreProbeAccepted, Actor: opts.Actor, Reason: "restore probes passed; restored evidence",
				OperationID: operationID, ResultDigest: []byte(digest),
				Apply: func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error {
					return insertEvidence(ctx, tx, evidenceID, accepted.InstanceID, accepted.Generation,
						"restore_probe", scope, digest, evidenceRef, opts.Actor)
				},
			})
			if err != nil {
				return TargetWriterAcceptance{}, err
			}
			if write.Discarded {
				return TargetWriterAcceptance{}, errors.New("restore evidence token is stale")
			}
			var txid int64
			if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&txid); err != nil {
				return TargetWriterAcceptance{}, err
			}
			return TargetWriterAcceptance{AcceptedRowRefs: []string{evidenceRef}, TransactionID: txid, AcceptedEvidenceToken: write.Token}, nil
		},
	})
	run.result.Checks = outcome.Checks
	if err != nil {
		run.blocked = append(run.blocked, "supervised target restore was not accepted: "+err.Error())
		return fail()
	}

	run.result.Restored = true
	run.result.EvidenceRef = evidenceRef
	return run.result, writerResult, nil
}

// ValidateRestoreTargetReason rejects values that the shared redaction
// boundary recognizes as credential-shaped. This is a pattern-based guard,
// not arbitrary secret recognition. The returned error is deliberately
// generic and never includes the supplied value.
func ValidateRestoreTargetReason(reason string) error {
	if secretShaped(reason) {
		return errors.New("target reason contains credential-shaped material")
	}
	return nil
}

// dependencies probes the configured V9 dependencies. A configured-but-
// unreachable dependency blocks; an unconfigured one is reported as
// not_configured (never as passed).
func (r *restoreRun) dependencies(ctx context.Context) bool {
	add := func(check DependencyCheck) {
		r.result.Dependencies = append(r.result.Dependencies, check)
	}
	ok := func(check DependencyCheck, blocking bool) bool {
		add(check)
		if blocking && check.State != "ok" {
			r.blocked = append(r.blocked, "dependency not satisfied: "+check.Name+" ("+check.Detail+")")
			return false
		}
		return true
	}
	all := true
	all = ok(DependencyCheck{Name: "target_dsn", State: "ok", Detail: "reachable (restore precondition)"}, false) && all
	all = ok(DependencyCheck{Name: "control_store", State: "ok", Detail: "opened and queried for evidence"}, false) && all
	if strings.TrimSpace(r.opts.SignerEndpoint) == "" {
		add(DependencyCheck{Name: "signer_boundary", State: "not_configured", Detail: "no signer endpoint configured; reachability not proven"})
	} else {
		boundary, err := CheckSignerBoundary(ctx, r.opts.SignerEndpoint)
		switch {
		case err != nil:
			all = ok(DependencyCheck{Name: "signer_boundary", State: "failed", Detail: err.Error()}, true) && all
		case !boundary.Reachable:
			all = ok(DependencyCheck{Name: "signer_boundary", State: "failed", Detail: "unreachable at " + boundary.Endpoint}, true) && all
		default:
			add(DependencyCheck{Name: "signer_boundary", State: "ok", Detail: "reachable at " + boundary.Endpoint + " (reachability only; no key material touched)"})
		}
	}
	if strings.TrimSpace(r.opts.RPCURL) == "" {
		add(DependencyCheck{Name: "rpc_fact_source", State: "not_configured", Detail: "no RPC URL configured; reachability not proven"})
	} else {
		all = ok(DependencyCheck{Name: "rpc_fact_source", State: tcpProbe(ctx, r.opts.RPCURL), Detail: "tcp reachability only"}, true) && all
	}
	if strings.TrimSpace(r.opts.BrokerDSN) == "" {
		add(DependencyCheck{Name: "broker", State: "not_configured", Detail: "no broker DSN configured; reachability not proven"})
	} else {
		all = ok(DependencyCheck{Name: "broker", State: tcpProbe(ctx, r.opts.BrokerDSN), Detail: "tcp reachability only"}, true) && all
	}
	return all
}

// tcpProbe is bounded tcp reachability (no protocol, no write).
func tcpProbe(ctx context.Context, endpoint string) string {
	dialTarget := endpoint
	if parsed, err := url.Parse(endpoint); err == nil && parsed.Host != "" {
		dialTarget = parsed.Host
	}
	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", dialTarget)
	if err != nil {
		return "failed: " + logx.Redact(err.Error())
	}
	_ = conn.Close()
	return "ok"
}

// restoreOperationID derives a bounded operation id for restored evidence
// when the caller did not supply one: it identifies the target probe, not an
// authorization.
func restoreOperationID(opts RestoreOptions, m *Manifest) string {
	if strings.TrimSpace(opts.OperationID) != "" {
		return strings.TrimSpace(opts.OperationID)
	}
	return "restore:" + m.BackupID
}

// checkManifestIntegrity verifies the artifact bindings and archive
// readability, and returns the canonical manifest digest.
func checkManifestIntegrity(manifestPath string, m *Manifest) (string, error) {
	if len(m.Artifacts) == 0 {
		return "", definiteManifestIntegrityError(errors.New("manifest contains no artifacts"))
	}
	for i, artifact := range m.Artifacts {
		path := resolveArtifactPath(manifestPath, artifact.Path)
		info, err := os.Stat(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return "", definiteManifestIntegrityError(fmt.Errorf("artifacts[%d] is missing: %w", i, err))
			}
			return "", indeterminateManifestIntegrityError(fmt.Errorf("inspect artifacts[%d]: %w", i, err))
		}
		if info.Size() != artifact.Bytes {
			return "", definiteManifestIntegrityError(fmt.Errorf("artifacts[%d] size %d does not match the manifest %d", i, info.Size(), artifact.Bytes))
		}
		actual, err := fileSHA256(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return "", definiteManifestIntegrityError(fmt.Errorf("artifacts[%d] is missing: %w", i, err))
			}
			return "", indeterminateManifestIntegrityError(err)
		}
		if actual != artifact.SHA256 {
			return "", definiteManifestIntegrityError(fmt.Errorf("artifacts[%d] sha256 does not match the manifest", i))
		}
	}
	digest, err := m.Digest()
	if err != nil {
		return "", definiteManifestIntegrityError(err)
	}
	if err := CheckManifestDigest(m, digest); err != nil {
		return "", definiteManifestIntegrityError(err)
	}
	return digest, nil
}

type manifestIntegrityError struct {
	determinate bool
	err         error
}

func (e *manifestIntegrityError) Error() string { return e.err.Error() }
func (e *manifestIntegrityError) Unwrap() error { return e.err }

func definiteManifestIntegrityError(err error) error {
	return &manifestIntegrityError{determinate: true, err: err}
}

func indeterminateManifestIntegrityError(err error) error {
	return &manifestIntegrityError{determinate: false, err: err}
}

func definiteIntegrityRefusal(err error) bool {
	var integrityErr *manifestIntegrityError
	return errors.As(err, &integrityErr) && integrityErr.determinate
}

// controlEvidenceMatches reports whether the control store holds a row bound
// to the instance and backup_id whose recorded digest/scope equal the current
// manifest body (F7/DG-2).
func controlEvidenceMatches(ctx context.Context, store *controlstore.Store,
	instanceID string, m *Manifest, digest string, run *restoreRun) bool {
	if store == nil {
		return false
	}
	_, matches, err := restoreBackupVerificationBinding(ctx, store, instanceID, m, digest, run)
	return err == nil && matches
}

func restoreBackupVerificationBinding(ctx context.Context, store *controlstore.Store, instanceID string,
	m *Manifest, digest string, run *restoreRun) (backupVerificationBinding, bool, error) {
	if store == nil {
		return backupVerificationBinding{}, false, nil
	}
	binding, accepted, err := currentBackupVerificationBinding(ctx, store.Pool(), instanceID, m, digest)
	if err != nil {
		return backupVerificationBinding{}, false, err
	}
	if !binding.Legacy {
		return binding, accepted, nil
	}
	if !accepted {
		return binding, false, nil
	}
	rows, err := store.Pool().Query(ctx, `
SELECT artifact_hash, COALESCE(scope, '{}'::jsonb)::text
FROM recovery_evidence
WHERE kind = 'backup_manifest' AND instance_id = $1 AND scope->>'backup_id' = $2
ORDER BY created_at DESC, evidence_id`, instanceID, m.BackupID)
	if err != nil {
		run.blocked = append(run.blocked, "control-store evidence lookup failed: "+logx.Redact(err.Error()))
		return binding, false, nil
	}
	defer rows.Close()
	available := false
	for rows.Next() {
		var artifactHash, scopeRaw string
		if err := rows.Scan(&artifactHash, &scopeRaw); err != nil {
			run.blocked = append(run.blocked, "control-store evidence read failed: "+logx.Redact(err.Error()))
			return binding, false, nil
		}
		available = true
		if artifactHash != digest {
			continue
		}
		var scope map[string]any
		if err := json.Unmarshal([]byte(scopeRaw), &scope); err != nil {
			continue
		}
		scopeDigest, _ := scope["manifest_digest"].(string)
		scopeVersion, _ := scope["manifest_version"].(string)
		if scopeDigest == digest && scopeVersion == m.ManifestVersion {
			return binding, true, nil
		}
	}
	if err := rows.Err(); err != nil {
		run.blocked = append(run.blocked, "control-store evidence read failed: "+logx.Redact(err.Error()))
		return binding, false, nil
	}
	if available {
		run.blocked = append(run.blocked, "control-store evidence for backup_id "+m.BackupID+
			" does not match the current manifest digest (manifest copied/edited?); re-run verify-backup")
	}
	return binding, false, nil
}

// ActionRestoreStarted is the recovery_audit action of the pre-write restore
// invalidation marker (MutationRestoreStarted). It records the credential-free
// start facts; the generation advance itself is written by the protocol.
const ActionRestoreStarted = "restore_started"

// insertRestoreStartedMarker writes the operator-facing marker row of the
// pre-write invalidation transition inside the marker's protocol transaction.
// It carries only credential-free facts (target fingerprint, never a DSN).
func insertRestoreStartedMarker(ctx context.Context, tx pgx.Tx, accepted EvidenceToken,
	m *Manifest, manifestDigest, targetFingerprint, declaration string, opts RestoreOptions) error {
	target, err := json.Marshal(map[string]any{
		"backup_id":          m.BackupID,
		"manifest_digest":    manifestDigest,
		"target_fingerprint": targetFingerprint,
		"declaration":        declaration,
	})
	if err != nil {
		return fmt.Errorf("encode restore_started target: %w", err)
	}
	detail, err := json.Marshal(map[string]any{
		"reason": boundedEvidenceDetail(opts.TargetReason),
		"note": "pre-write invalidation marker: releases/approvals bound to the previous generation " +
			"are stale before the first target write and stay stale if the restore fails",
	})
	if err != nil {
		return fmt.Errorf("encode restore_started detail: %w", err)
	}
	generation := accepted.Generation
	return controlstore.WriteAudit(ctx, tx, controlstore.AuditRecord{
		InstanceID:         accepted.InstanceID,
		Actor:              opts.Actor,
		Action:             ActionRestoreStarted,
		Target:             target,
		Detail:             detail,
		Result:             controlstore.AuditOK,
		EvidenceGeneration: &generation,
		OperationID:        restoreOperationID(opts, m),
	})
}

// insertEvidence writes one recovery_evidence row inside the caller's
// transaction (the generation protocol owns the instance lock and advance).
func insertEvidence(ctx context.Context, tx pgx.Tx, evidenceID, instanceID string, generation int64,
	kind string, scope []byte, artifactHash, artifactRef, collectedBy string) error {
	const sql = `
INSERT INTO recovery_evidence
    (evidence_id, instance_id, generation, kind, scope, artifact_hash, artifact_ref, observed_at, collected_by)
VALUES ($1, $2, $3, $4, $5, $6, $7, now(), $8)`
	if _, err := tx.Exec(ctx, sql, evidenceID, instanceID, generation, kind, scope, artifactHash, artifactRef, collectedBy); err != nil {
		return fmt.Errorf("insert %s evidence: %w", kind, err)
	}
	return nil
}

// serverVersionOf reads the server version of a DSN (a real connection; no
// write).
func serverVersionOf(ctx context.Context, dsn string) (string, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close(ctx) }()
	var version string
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version')").Scan(&version); err != nil {
		return "", err
	}
	return version, nil
}

// ---------------------------------------------------------------------------
// Restore probes (FR-002 nine categories, FR-006)
// ---------------------------------------------------------------------------

// businessStateCategory is one FR-002 authoritative category with its
// representative objects (data-model §7).
type businessStateCategory struct {
	Category string
	Objects  []string
}

// businessStateCategories is the fixed FR-002 enumeration the probe walks.
// A category whose declared objects cannot be queried is recorded unknown and
// never counted as restored (F9).
var businessStateCategories = []businessStateCategory{
	{"chain_identity_cursor", []string{"chain_blocks", "indexer_checkpoint"}},
	{"events", []string{"erc20_transfer_logs"}},
	{"deposit_confirmation", []string{"deposit_checkpoint", "confirmation_policy_history"}},
	{"withdrawal_requests_and_payment_intents", []string{"withdrawal_requests", "payment_intents"}},
	{"outbound_tx_signing_broadcast", []string{"signing_requests", "tx_send_attempts", "tx_receipts"}},
	{"nonce_allocation", []string{"nonce_bindings", "nonce_observations"}},
	{"outbox_and_obligations", []string{"outbox_events", "event_obligation"}},
	{"consumer_idempotency_progress", []string{"consumer_inbox", "consumer_progress"}},
	{"audit_permissions_014", []string{"recon_task", "recon_audit", "withdrawal_request_audit"}},
}

// probeItem records one sampled coverage item of a probe.
type probeItem struct {
	Category string `json:"category"`
	Object   string `json:"object"`
	State    string `json:"state"`
	Reason   string `json:"reason,omitempty"`
}

// probeOutcome is the four-check probe result plus its declared coverage.
type probeOutcome struct {
	Checks       ManifestChecks `json:"checks"`
	Business     []probeItem    `json:"business_state_coverage"`
	Verification []probeItem    `json:"verification_coverage"`
	Problems     []string       `json:"problems,omitempty"`
}

var relationNamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// probeRestoredTarget runs the four probes against the restored target:
//
//   - readable: the archive lists without truncation/checksum errors;
//   - structure_constraints: the goose set equals the manifest exactly,
//     CheckCompatibility passes, the authoritative objects exist and foreign
//     keys are present;
//   - business_state_probes: one representative read-only query per FR-002
//     category, over the objects the manifest declares; a category that
//     cannot be proven is unknown and not counted;
//   - verification_executable: a read-only transaction can execute the V1-V9
//     object-access path (transaction_read_only=on, every category relation
//     resolves, the goose set is readable).
func probeRestoredTarget(ctx context.Context, pg PGCommand, manifestPath string, m *Manifest, targetDSN string) probeOutcome {
	var out probeOutcome
	declared := make(map[string]bool, len(m.Coverage.Authoritative))
	for _, object := range m.Coverage.Authoritative {
		declared[object] = true
	}

	archivePath := resolveArtifactPath(manifestPath, m.Artifacts[0].Path)
	if archive, err := os.Open(archivePath); err != nil {
		out.Problems = append(out.Problems, "readable: artifact cannot be opened: "+err.Error())
	} else {
		if err := pgListArchive(ctx, pg, archive); err != nil {
			out.Problems = append(out.Problems, "readable: "+err.Error())
		} else {
			out.Checks.Readable = true
		}
		_ = archive.Close()
	}

	conn, err := pgx.Connect(ctx, targetDSN)
	if err != nil {
		out.Problems = append(out.Problems, "target is unreachable for probes: "+logx.Redact(err.Error()))
		return out
	}
	defer func() { _ = conn.Close(ctx) }()

	// structure_constraints.
	structureProblems := []string{}
	state, err := db.Inspect(ctx, db.MigrateOptions{DSN: targetDSN, ConnectTimeout: 5 * time.Second})
	if err != nil {
		structureProblems = append(structureProblems, "read migration state: "+logx.Redact(err.Error()))
	} else if !equalInt64Set(state.Applied, m.Schema.GooseDBVersion) {
		structureProblems = append(structureProblems,
			fmt.Sprintf("goose set %v does not equal the manifest set %v", state.Applied, m.Schema.GooseDBVersion))
	}
	if _, err := db.CheckCompatibility(ctx, db.MigrateOptions{DSN: targetDSN, ConnectTimeout: 5 * time.Second}); err != nil {
		structureProblems = append(structureProblems, "CheckCompatibility: "+err.Error())
	}
	for _, category := range businessStateCategories {
		for _, object := range category.Objects {
			exists, err := relationExists(ctx, conn, object)
			if err != nil {
				structureProblems = append(structureProblems, object+": "+err.Error())
				continue
			}
			if !exists {
				structureProblems = append(structureProblems, "required table "+object+" is missing")
			}
		}
	}
	var foreignKeys int
	if err := conn.QueryRow(ctx,
		`SELECT count(*) FROM pg_constraint WHERE contype = 'f' AND connamespace = 'public'::regnamespace`).
		Scan(&foreignKeys); err != nil {
		structureProblems = append(structureProblems, "read constraint catalog: "+err.Error())
	} else if foreignKeys == 0 {
		structureProblems = append(structureProblems, "no foreign key constraints found in the public schema")
	}
	if len(structureProblems) == 0 {
		out.Checks.StructureConstraints = true
	} else {
		out.Problems = append(out.Problems, "structure_constraints: "+strings.Join(structureProblems, "; "))
	}

	// business_state_probes: per FR-002 category over the declared objects.
	businessOK := true
	for _, category := range businessStateCategories {
		item := probeItem{Category: category.Category, Object: strings.Join(category.Objects, ","), State: "proven"}
		for _, object := range category.Objects {
			switch {
			case !relationNamePattern.MatchString(object):
				item.State, item.Reason = "unknown", "object name is not a plain relation identifier"
			case !declared[object]:
				item.State, item.Reason = "unknown", "object is not declared in the manifest coverage"
			default:
				var count int64
				if err := conn.QueryRow(ctx, `SELECT count(*) FROM `+object).Scan(&count); err != nil {
					item.State = "unknown"
					item.Reason = object + ": " + logx.Redact(err.Error())
				}
			}
		}
		if item.State != "proven" {
			businessOK = false
			out.Problems = append(out.Problems, "business_state_probes: category "+category.Category+" is unknown ("+item.Reason+")")
		}
		out.Business = append(out.Business, item)
	}
	out.Checks.BusinessStateProbes = businessOK

	// verification_executable: the read-only V1-V9 access path executes.
	verificationOK := true
	verificationTx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		verificationOK = false
		out.Problems = append(out.Problems, "verification_executable: cannot open a read-only transaction: "+logx.Redact(err.Error()))
	} else {
		var readOnly string
		if err := verificationTx.QueryRow(ctx, "SHOW transaction_read_only").Scan(&readOnly); err != nil || readOnly != "on" {
			verificationOK = false
			out.Problems = append(out.Problems, "verification_executable: the probe transaction is not read-only")
		}
		for _, category := range businessStateCategories {
			item := probeItem{Category: category.Category, Object: strings.Join(category.Objects, ","), State: "proven"}
			for _, object := range category.Objects {
				exists, err := relationExistsTx(ctx, verificationTx, object)
				switch {
				case err != nil:
					item.State, item.Reason = "unknown", logx.Redact(err.Error())
				case !exists:
					item.State, item.Reason = "unknown", "relation does not resolve"
				}
			}
			if item.State != "proven" {
				verificationOK = false
				out.Problems = append(out.Problems, "verification_executable: category "+category.Category+" is unknown ("+item.Reason+")")
			}
			out.Verification = append(out.Verification, item)
		}
		var gooseCount int64
		if err := verificationTx.QueryRow(ctx, `SELECT count(*) FROM goose_db_version`).Scan(&gooseCount); err != nil {
			verificationOK = false
			out.Problems = append(out.Problems, "verification_executable: goose set is not readable: "+logx.Redact(err.Error()))
		}
		_ = verificationTx.Rollback(ctx)
	}
	out.Checks.VerificationExecutable = verificationOK
	return out
}

func relationExists(ctx context.Context, conn *pgx.Conn, object string) (bool, error) {
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, "public."+object).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func relationExistsTx(ctx context.Context, tx pgx.Tx, object string) (bool, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, "public."+object).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// ---------------------------------------------------------------------------
// DSN redaction and signer boundary (FR-007/FR-008)
// ---------------------------------------------------------------------------

// RedactedDSN is the only DSN form allowed into logs, audit rows or evidence:
// structural context (scheme/user/host/database) stays diagnosable, credential
// values are gone. It reuses the shared logx.Redact boundary.
func RedactedDSN(dsn string) string {
	if strings.TrimSpace(dsn) == "" {
		return ""
	}
	return logx.Redact(dsn)
}

// SignerBoundary is the reachability-only result of a signer boundary check.
// It deliberately has no field that could carry key material.
type SignerBoundary struct {
	Reachable bool   `json:"reachable"`
	Endpoint  string `json:"endpoint"`
}

// CheckSignerBoundary probes whether the signer boundary endpoint accepts a
// TCP connection. Recovery tools only prove reachability; they never request,
// receive, read or export key material (FR-007). An empty endpoint is refused
// by name; an unreachable endpoint is a result, not an error.
func CheckSignerBoundary(ctx context.Context, endpoint string) (SignerBoundary, error) {
	if strings.TrimSpace(endpoint) == "" {
		return SignerBoundary{}, errors.New("signer boundary endpoint is required (refusing by name)")
	}
	dialTarget := endpoint
	if parsed, err := url.Parse(endpoint); err == nil && parsed.Host != "" {
		dialTarget = parsed.Host
	}
	dialer := net.Dialer{Timeout: 1500 * time.Millisecond}
	conn, err := dialer.DialContext(ctx, "tcp", dialTarget)
	reachable := err == nil
	if reachable {
		_ = conn.Close()
	}
	return SignerBoundary{Reachable: reachable, Endpoint: logx.Redact(endpoint)}, nil
}
