// manifest.go implements T017: the backup manifest model, validation and
// deterministic selection of contracts/backup-manifest.md §1-§3 and
// data-model.md §2 (FR-001-FR-004/FR-006/FR-036).
//
// The manifest is the only backup identity and the only selection input:
// file names, directory listings, mtimes and human memory play no part in
// selection (SelectBackup receives manifests only). A manifest is usable for
// restore or resumption only when verification.state=verified AND every
// validation rule holds; an unknown document, an unknown version, a missing
// required field or a digest mismatch is refused fail-closed and never
// "best-effort" defaulted.
//
// Recovery-point discipline (FR-036): only the exported snapshot tuple
// (pg_current_snapshot xmin/xip/xmax + the export-time pg_current_wal_lsn
// upper bound + wall clock + server/database identity) can prove a recovery
// point. Backup frequency, a business table MAX(created_at), the backup file
// mtime and a bare timestamp are forbidden proofs; backup_lag and
// uncovered_interval are always explicitly recorded, may be "unknown", but
// may never be omitted (RecoveryMeasurement).
//
// T017 does not touch a database, a Docker daemon or the filesystem: it is
// the pure contract layer behind the T018-T020 executors.
package recovery

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/xtianxx/txharbor/internal/logx"
)

// ManifestVersionV1 is the only known manifest contract version. An unknown
// value is refused; a widening of the contract is a new version constant and
// a specification change, never a silent acceptance.
const ManifestVersionV1 = "015.1"

// CarrierKindPGDumpCustom is the only supported carrier kind (ADR-002:
// logical backup via pg_dump --format=custom).
const CarrierKindPGDumpCustom = "pg_dump_custom"

// VerificationTargetIsolated is the only verification target recorded for a
// usable backup (`target=isolated`, contracts/backup-manifest.md §3).
const VerificationTargetIsolated = "isolated"

// Required backup coverage declarations. The excluded list is not decoration:
// signer private keys and real credentials must both be declared excluded, and
// a manifest that fails to declare either is refused (T017, FR-002). These
// declarations describe intended coverage; they do not prove the artifact's
// actual contents are secret-free.
const (
	CoverageExcludedSignerPrivateKeys = "signer_private_keys"
	CoverageExcludedRealCredentials   = "real_credentials"
	CoverageExcludedRedis             = "redis_non_authoritative"
	CoverageExcludedKafka             = "kafka_non_authoritative"
)

// canonicalExcludedObjects is the exclusion declaration every manifest this
// program produces carries: non-authoritative Redis/Kafka state, signer private
// keys and real credentials are declared excluded from a data-DB backup.
var canonicalExcludedObjects = []string{
	CoverageExcludedRedis,
	CoverageExcludedKafka,
	CoverageExcludedSignerPrivateKeys,
	CoverageExcludedRealCredentials,
}

// canonicalAuthoritativeObjects is the coverage declaration of the data-DB
// authoritative object set this program backs up (data-model §2). The
// business-state probe of T019 maps each FR-002 category onto these objects.
var canonicalAuthoritativeObjects = []string{
	"chain_blocks",
	"indexer_checkpoint",
	"erc20_transfer_logs",
	"deposit_checkpoint",
	"deposit_observation_transitions",
	"confirmation_policy_history",
	"withdrawal_requests",
	"payment_intents",
	"signing_requests",
	"tx_send_attempts",
	"tx_receipts",
	"nonce_bindings",
	"nonce_observations",
	"outbox_events",
	"event_obligation",
	"consumer_inbox",
	"consumer_progress",
	"recon_task",
	"recon_audit",
	"withdrawal_request_audit",
}

// CanonicalAuthoritativeObjects returns the authoritative coverage objects of
// a manifest this program creates. The caller receives a fresh slice.
func CanonicalAuthoritativeObjects() []string {
	return slices.Clone(canonicalAuthoritativeObjects)
}

// CanonicalExcludedObjects returns the exclusion declaration of a manifest
// this program creates. The caller receives a fresh slice.
func CanonicalExcludedObjects() []string {
	return slices.Clone(canonicalExcludedObjects)
}

// Typed contract errors. Callers classify refusals with errors.Is; the
// wrapped text never echoes credential material.
var (
	// ErrManifestInvalid: the document violates the manifest contract
	// (missing/malformed required field, broken coverage declaration, ...).
	ErrManifestInvalid = errors.New("backup manifest is invalid")
	// ErrManifestVersionUnknown: manifest_version is not a version this
	// program knows. Unknown versions are refused, never read best-effort.
	ErrManifestVersionUnknown = errors.New("unknown backup manifest version")
	// ErrManifestDigestMismatch: the recorded canonical-JSON digest does not
	// match the document body (or is malformed).
	ErrManifestDigestMismatch = errors.New("backup manifest digest mismatch")
	// ErrManifestNotVerified: verification.state is not `verified`; the
	// manifest is not usable for restore or resumption (FR-006).
	ErrManifestNotVerified = errors.New("backup manifest is not verified")
	// ErrManifestSecrets: a manifest free-text field embeds credential
	// material; the manifest is stored next to the backup and is never a
	// secret carrier (FR-007).
	ErrManifestSecrets = errors.New("backup manifest contains credential material")
	// ErrNoEligibleBackup: no candidate is verified+valid; selection is a
	// refusal, never a "best effort" fallback to names or mtimes.
	ErrNoEligibleBackup = errors.New("no eligible verified backup")
	// ErrRPOProofForbidden: the requested proof kind can never prove a
	// recovery point (FR-036).
	ErrRPOProofForbidden = errors.New("forbidden RPO proof kind")
	// ErrRecoveryPointIncomplete: the recovery point is not the full exported
	// snapshot tuple; a bare timestamp is not a recovery point.
	ErrRecoveryPointIncomplete = errors.New("recovery point is incomplete")
)

// ---------------------------------------------------------------------------
// Wire model (contracts/backup-manifest.md §1, data-model.md §2)
// ---------------------------------------------------------------------------

// Manifest is the external backup identity document. The JSON field names are
// the wire contract: renaming one is a specification change.
type Manifest struct {
	ManifestVersion string                `json:"manifest_version"`
	BackupID        string                `json:"backup_id"`
	CreatedAt       string                `json:"created_at"`
	CreatedBy       string                `json:"created_by"`
	Carrier         ManifestCarrier       `json:"carrier"`
	Coverage        ManifestCoverage      `json:"coverage"`
	RecoveryPoint   ManifestRecoveryPoint `json:"recovery_point"`
	Schema          ManifestSchema        `json:"schema"`
	Program         ManifestProgram       `json:"program"`
	Artifacts       []ManifestArtifact    `json:"artifacts"`
	Verification    ManifestVerification  `json:"verification"`
	RetentionClass  string                `json:"retention_class,omitempty"`
	Note            string                `json:"note,omitempty"`
}

// ManifestCarrier records the backup carrier and the client/server versions
// that produced it.
type ManifestCarrier struct {
	Kind            string `json:"kind"`
	PGServerVersion string `json:"pg_server_version"`
	PGDumpVersion   string `json:"pg_dump_version"`
}

// ManifestCoverage is the coverage declaration (authoritative data-DB objects)
// plus the exclusion declaration (non-authoritative state and key material).
type ManifestCoverage struct {
	Authoritative []string `json:"authoritative"`
	Excluded      []string `json:"excluded"`
}

// ManifestRecoveryPoint is the only valid recovery-point proof: the exported
// snapshot tuple plus the export-time WAL LSN upper bound, wall clock and
// server/database identity (research §2).
type ManifestRecoveryPoint struct {
	Snapshot  ManifestSnapshot `json:"snapshot"`
	LSN       string           `json:"wal_lsn"`
	WallClock string           `json:"wall_clock"`
	Server    string           `json:"server"`
	Database  string           `json:"database"`
}

// ManifestSnapshot is the pg_current_snapshot() tuple as exported by
// pg_export_snapshot().
type ManifestSnapshot struct {
	Xmin int64   `json:"xmin"`
	Xip  []int64 `json:"xip"`
	Xmax int64   `json:"xmax"`
}

// ManifestSchema is the exact applied goose_db_version set at backup time.
type ManifestSchema struct {
	GooseDBVersion []int64 `json:"goose_db_version"`
}

// ManifestProgram is the generating program version and its minimum
// compatible floor.
type ManifestProgram struct {
	Version       string `json:"version"`
	MinCompatible string `json:"min_compatible"`
}

// ManifestArtifact is one backup artifact with its byte-level binding.
type ManifestArtifact struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// ManifestVerification is the verification record of contracts/backup-manifest
// §3. state != verified is never usable for restore or resumption.
type ManifestVerification struct {
	State       VerificationState `json:"state"`
	VerifiedAt  string            `json:"verified_at,omitempty"`
	Verifier    string            `json:"verifier,omitempty"`
	Target      string            `json:"target,omitempty"`
	Checks      ManifestChecks    `json:"checks"`
	EvidenceRef string            `json:"evidence_ref,omitempty"`
}

// ManifestChecks is the four independently required restore checks of
// contracts/backup-manifest.md §3; "file exists / can be listed" alone is not
// verification.
type ManifestChecks struct {
	Readable               bool `json:"readable"`
	StructureConstraints   bool `json:"structure_constraints"`
	BusinessStateProbes    bool `json:"business_state_probes"`
	VerificationExecutable bool `json:"verification_executable"`
}

// AllTrue reports whether every restore check passed.
func (c ManifestChecks) AllTrue() bool {
	return c.Readable && c.StructureConstraints && c.BusinessStateProbes && c.VerificationExecutable
}

// VerificationState is the closed verification-state set.
type VerificationState string

// The closed verification-state set of contracts/backup-manifest.md §1.
const (
	VerificationUnverified VerificationState = "unverified"
	VerificationVerified   VerificationState = "verified"
	VerificationRejected   VerificationState = "rejected"
)

var knownVerificationStates = []VerificationState{
	VerificationUnverified, VerificationVerified, VerificationRejected,
}

// Known reports whether s is one of the closed verification states.
func (s VerificationState) Known() bool { return slices.Contains(knownVerificationStates, s) }

// ParseVerificationState maps s onto the closed set; unknown, empty,
// differently-cased or whitespace-padded input is refused.
func ParseVerificationState(s string) (VerificationState, error) {
	state := VerificationState(s)
	if !state.Known() {
		return "", fmt.Errorf("%w: verification.state %q is not in the closed set", ErrManifestInvalid, s)
	}
	return state, nil
}

// ManifestUse is the closed set of manifest consumers; a manifest must prove
// verification for both.
type ManifestUse string

// The manifest uses of contracts/backup-manifest.md §5.
const (
	ManifestUseRestore    ManifestUse = "restore"
	ManifestUseResumption ManifestUse = "resumption"
)

// ManifestValidation is the running program's compatibility target used by
// Validate/CheckUsable/SelectBackup (FR-004).
type ManifestValidation struct {
	// PGServerMajor is the major version of the PostgreSQL the manifest will
	// be used against; carrier server and pg_dump majors must match it.
	PGServerMajor int
	// TargetSchema is the exact goose_db_version set of the running program;
	// the manifest set must equal it (no missing, no unknown version).
	TargetSchema []int64
	// ProgramVersion is the running program identity; the manifest's
	// min_compatible floor must be satisfied by it.
	ProgramVersion string
}

// ---------------------------------------------------------------------------
// Parsing (fail-closed)
// ---------------------------------------------------------------------------

var uuidPattern = `^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`

// lsnPattern is the PostgreSQL WAL position form "HEX/HEX".
var lsnPattern = `^[0-9A-Fa-f]+/[0-9A-Fa-f]+$`

// ParseManifest decodes and structurally validates a manifest document.
// Required fields must be present with the right shape; an unknown
// manifest_version or an unknown verification state is refused. Semantic
// compatibility (carrier/schema/program) belongs to Validate, but the parse
// itself is already fail-closed: a missing field never becomes a zero-value
// default that Validate could later miss.
func ParseManifest(data []byte) (*Manifest, error) {
	top, err := jsonObject(data, "manifest")
	if err != nil {
		return nil, err
	}
	if err := requireKeys(top, "manifest",
		"manifest_version", "backup_id", "created_at", "created_by", "carrier",
		"coverage", "recovery_point", "schema", "program", "artifacts", "verification"); err != nil {
		return nil, err
	}

	version, err := requireString(top, "manifest_version", "manifest_version", false)
	if err != nil {
		return nil, err
	}
	if version != ManifestVersionV1 {
		return nil, fmt.Errorf("%w: %q", ErrManifestVersionUnknown, version)
	}

	backupID, err := requireString(top, "backup_id", "backup_id", true)
	if err != nil {
		return nil, err
	}
	createdAt, err := requireString(top, "created_at", "created_at", true)
	if err != nil {
		return nil, err
	}
	createdBy, err := requireString(top, "created_by", "created_by", true)
	if err != nil {
		return nil, err
	}

	carrier, err := parseCarrier(top["carrier"])
	if err != nil {
		return nil, err
	}
	coverage, err := parseCoverage(top["coverage"])
	if err != nil {
		return nil, err
	}
	recoveryPoint, err := parseRecoveryPoint(top["recovery_point"], "recovery_point")
	if err != nil {
		return nil, err
	}
	schema, err := parseSchema(top["schema"])
	if err != nil {
		return nil, err
	}
	program, err := parseProgram(top["program"])
	if err != nil {
		return nil, err
	}
	artifacts, err := parseArtifacts(top["artifacts"])
	if err != nil {
		return nil, err
	}
	verification, err := parseVerification(top["verification"])
	if err != nil {
		return nil, err
	}

	m := &Manifest{
		ManifestVersion: version,
		BackupID:        backupID,
		CreatedAt:       createdAt,
		CreatedBy:       createdBy,
		Carrier:         carrier,
		Coverage:        coverage,
		RecoveryPoint:   recoveryPoint,
		Schema:          schema,
		Program:         program,
		Artifacts:       artifacts,
		Verification:    verification,
	}
	if raw, ok := top["retention_class"]; ok {
		value, err := rawString(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: retention_class: %v", ErrManifestInvalid, err)
		}
		m.RetentionClass = value
	}
	if raw, ok := top["note"]; ok {
		value, err := rawString(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: note: %v", ErrManifestInvalid, err)
		}
		m.Note = value
	}
	return m, nil
}

func parseCarrier(raw json.RawMessage) (ManifestCarrier, error) {
	obj, err := jsonObject(raw, "carrier")
	if err != nil {
		return ManifestCarrier{}, err
	}
	if err := requireKeys(obj, "carrier", "kind", "pg_server_version", "pg_dump_version"); err != nil {
		return ManifestCarrier{}, err
	}
	kind, err := requireString(obj, "kind", "carrier.kind", true)
	if err != nil {
		return ManifestCarrier{}, err
	}
	server, err := requireString(obj, "pg_server_version", "carrier.pg_server_version", true)
	if err != nil {
		return ManifestCarrier{}, err
	}
	dump, err := requireString(obj, "pg_dump_version", "carrier.pg_dump_version", true)
	if err != nil {
		return ManifestCarrier{}, err
	}
	return ManifestCarrier{Kind: kind, PGServerVersion: server, PGDumpVersion: dump}, nil
}

func parseCoverage(raw json.RawMessage) (ManifestCoverage, error) {
	obj, err := jsonObject(raw, "coverage")
	if err != nil {
		return ManifestCoverage{}, err
	}
	if err := requireKeys(obj, "coverage", "authoritative", "excluded"); err != nil {
		return ManifestCoverage{}, err
	}
	authoritative, err := requireStringArray(obj, "authoritative", "coverage.authoritative", false)
	if err != nil {
		return ManifestCoverage{}, err
	}
	excluded, err := requireStringArray(obj, "excluded", "coverage.excluded", false)
	if err != nil {
		return ManifestCoverage{}, err
	}
	return ManifestCoverage{Authoritative: authoritative, Excluded: excluded}, nil
}

func parseRecoveryPoint(raw json.RawMessage, path string) (ManifestRecoveryPoint, error) {
	obj, err := jsonObject(raw, path)
	if err != nil {
		return ManifestRecoveryPoint{}, err
	}
	if err := requireKeys(obj, path, "snapshot", "wal_lsn", "wall_clock", "server", "database"); err != nil {
		return ManifestRecoveryPoint{}, err
	}
	snapshot, err := parseSnapshot(obj["snapshot"], path+".snapshot")
	if err != nil {
		return ManifestRecoveryPoint{}, err
	}
	lsn, err := requireString(obj, "wal_lsn", path+".wal_lsn", true)
	if err != nil {
		return ManifestRecoveryPoint{}, err
	}
	wallClock, err := requireString(obj, "wall_clock", path+".wall_clock", true)
	if err != nil {
		return ManifestRecoveryPoint{}, err
	}
	server, err := requireString(obj, "server", path+".server", true)
	if err != nil {
		return ManifestRecoveryPoint{}, err
	}
	database, err := requireString(obj, "database", path+".database", true)
	if err != nil {
		return ManifestRecoveryPoint{}, err
	}
	return ManifestRecoveryPoint{Snapshot: snapshot, LSN: lsn, WallClock: wallClock, Server: server, Database: database}, nil
}

func parseSnapshot(raw json.RawMessage, path string) (ManifestSnapshot, error) {
	obj, err := jsonObject(raw, path)
	if err != nil {
		return ManifestSnapshot{}, err
	}
	if err := requireKeys(obj, path, "xmin", "xip", "xmax"); err != nil {
		return ManifestSnapshot{}, err
	}
	xmin, err := requireInt64(obj, "xmin", path+".xmin")
	if err != nil {
		return ManifestSnapshot{}, err
	}
	xmax, err := requireInt64(obj, "xmax", path+".xmax")
	if err != nil {
		return ManifestSnapshot{}, err
	}
	xip, err := requireInt64Array(obj, "xip", path+".xip")
	if err != nil {
		return ManifestSnapshot{}, err
	}
	return ManifestSnapshot{Xmin: xmin, Xip: xip, Xmax: xmax}, nil
}

func parseSchema(raw json.RawMessage) (ManifestSchema, error) {
	obj, err := jsonObject(raw, "schema")
	if err != nil {
		return ManifestSchema{}, err
	}
	if err := requireKeys(obj, "schema", "goose_db_version"); err != nil {
		return ManifestSchema{}, err
	}
	versions, err := requireInt64Array(obj, "goose_db_version", "schema.goose_db_version")
	if err != nil {
		return ManifestSchema{}, err
	}
	return ManifestSchema{GooseDBVersion: versions}, nil
}

func parseProgram(raw json.RawMessage) (ManifestProgram, error) {
	obj, err := jsonObject(raw, "program")
	if err != nil {
		return ManifestProgram{}, err
	}
	if err := requireKeys(obj, "program", "version", "min_compatible"); err != nil {
		return ManifestProgram{}, err
	}
	version, err := requireString(obj, "version", "program.version", true)
	if err != nil {
		return ManifestProgram{}, err
	}
	floor, err := requireString(obj, "min_compatible", "program.min_compatible", true)
	if err != nil {
		return ManifestProgram{}, err
	}
	return ManifestProgram{Version: version, MinCompatible: floor}, nil
}

func parseArtifacts(raw json.RawMessage) ([]ManifestArtifact, error) {
	var entries []json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("%w: artifacts must be an array: %v", ErrManifestInvalid, err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: artifacts must declare at least one artifact", ErrManifestInvalid)
	}
	artifacts := make([]ManifestArtifact, 0, len(entries))
	for i, entry := range entries {
		path := fmt.Sprintf("artifacts[%d]", i)
		obj, err := jsonObject(entry, path)
		if err != nil {
			return nil, err
		}
		if err := requireKeys(obj, path, "path", "bytes", "sha256"); err != nil {
			return nil, err
		}
		artifactPath, err := requireString(obj, "path", path+".path", true)
		if err != nil {
			return nil, err
		}
		bytes, err := requireInt64(obj, "bytes", path+".bytes")
		if err != nil {
			return nil, err
		}
		if bytes <= 0 {
			return nil, fmt.Errorf("%w: %s.bytes must be positive, got %d", ErrManifestInvalid, path, bytes)
		}
		sha, err := requireString(obj, "sha256", path+".sha256", true)
		if err != nil {
			return nil, err
		}
		if !regexpMatch(digestPattern, sha) {
			return nil, fmt.Errorf("%w: %s.sha256 must be sha256:<64 lowercase hex characters>", ErrManifestInvalid, path)
		}
		artifacts = append(artifacts, ManifestArtifact{Path: artifactPath, Bytes: bytes, SHA256: sha})
	}
	return artifacts, nil
}

func parseVerification(raw json.RawMessage) (ManifestVerification, error) {
	obj, err := jsonObject(raw, "verification")
	if err != nil {
		return ManifestVerification{}, err
	}
	if err := requireKeys(obj, "verification", "state", "checks"); err != nil {
		return ManifestVerification{}, err
	}
	rawState, err := requireString(obj, "state", "verification.state", true)
	if err != nil {
		return ManifestVerification{}, err
	}
	state, err := ParseVerificationState(rawState)
	if err != nil {
		return ManifestVerification{}, err
	}
	checksRaw, err := jsonObject(obj["checks"], "verification.checks")
	if err != nil {
		return ManifestVerification{}, err
	}
	if err := requireKeys(checksRaw, "verification.checks",
		"readable", "structure_constraints", "business_state_probes", "verification_executable"); err != nil {
		return ManifestVerification{}, err
	}
	checks := ManifestChecks{}
	for key, target := range map[string]*bool{
		"readable":                &checks.Readable,
		"structure_constraints":   &checks.StructureConstraints,
		"business_state_probes":   &checks.BusinessStateProbes,
		"verification_executable": &checks.VerificationExecutable,
	} {
		value, err := requireBool(checksRaw, key, "verification.checks."+key)
		if err != nil {
			return ManifestVerification{}, err
		}
		*target = value
	}
	verification := ManifestVerification{State: state, Checks: checks}
	for key, target := range map[string]*string{
		"verified_at":  &verification.VerifiedAt,
		"verifier":     &verification.Verifier,
		"target":       &verification.Target,
		"evidence_ref": &verification.EvidenceRef,
	} {
		rawValue, ok := obj[key]
		if !ok {
			continue
		}
		value, err := rawString(rawValue)
		if err != nil {
			return ManifestVerification{}, fmt.Errorf("%w: verification.%s: %v", ErrManifestInvalid, key, err)
		}
		*target = value
	}
	return verification, nil
}

// ---------------------------------------------------------------------------
// Validation (FR-004, fail-closed)
// ---------------------------------------------------------------------------

// Validate checks one manifest against the running program's compatibility
// target: carrier kind and major versions, the exact goose set, the program
// floor, the full recovery point, artifact bindings, the verification record
// and the secrecy scan. A nil return means internally consistent and
// compatible; every error is a refusal.
func (m *Manifest) Validate(v ManifestValidation) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrManifestInvalid)
	}
	if m.ManifestVersion != ManifestVersionV1 {
		return fmt.Errorf("%w: %q", ErrManifestVersionUnknown, m.ManifestVersion)
	}
	if !validUUID(m.BackupID) {
		return fmt.Errorf("%w: backup_id %q is not a UUID", ErrManifestInvalid, m.BackupID)
	}
	if _, err := time.Parse(time.RFC3339, m.CreatedAt); err != nil {
		return fmt.Errorf("%w: created_at %q is not RFC3339", ErrManifestInvalid, m.CreatedAt)
	}
	if strings.TrimSpace(m.CreatedBy) == "" {
		return fmt.Errorf("%w: created_by is required", ErrManifestInvalid)
	}
	if m.Carrier.Kind != CarrierKindPGDumpCustom {
		return fmt.Errorf("%w: carrier.kind %q is not %s", ErrManifestInvalid, m.Carrier.Kind, CarrierKindPGDumpCustom)
	}
	serverMajor, err := majorVersion(m.Carrier.PGServerVersion)
	if err != nil {
		return fmt.Errorf("%w: carrier.pg_server_version %q: %v", ErrManifestInvalid, m.Carrier.PGServerVersion, err)
	}
	if serverMajor != v.PGServerMajor {
		return fmt.Errorf("%w: carrier.pg_server_version %q major %d does not match the target PostgreSQL major %d",
			ErrManifestInvalid, m.Carrier.PGServerVersion, serverMajor, v.PGServerMajor)
	}
	dumpMajor, err := majorVersion(m.Carrier.PGDumpVersion)
	if err != nil {
		return fmt.Errorf("%w: carrier.pg_dump_version %q: %v", ErrManifestInvalid, m.Carrier.PGDumpVersion, err)
	}
	if dumpMajor != v.PGServerMajor {
		return fmt.Errorf("%w: carrier.pg_dump_version %q major %d does not match the target PostgreSQL major %d",
			ErrManifestInvalid, m.Carrier.PGDumpVersion, dumpMajor, v.PGServerMajor)
	}
	if err := validateCoverage(m.Coverage); err != nil {
		return err
	}
	if err := ValidateRPOProof(RPOProofSnapshotTuple, m.RecoveryPoint); err != nil {
		return err
	}
	if len(m.Schema.GooseDBVersion) == 0 {
		return fmt.Errorf("%w: schema.goose_db_version must be the exact applied set", ErrManifestInvalid)
	}
	if !equalInt64Set(m.Schema.GooseDBVersion, v.TargetSchema) {
		return fmt.Errorf("%w: schema.goose_db_version %v does not equal the program target set %v",
			ErrManifestInvalid, m.Schema.GooseDBVersion, v.TargetSchema)
	}
	if strings.TrimSpace(m.Program.Version) == "" {
		return fmt.Errorf("%w: program.version is required", ErrManifestInvalid)
	}
	if !ProgramCompatible(v.ProgramVersion, m.Program.MinCompatible) {
		return fmt.Errorf("%w: program.min_compatible %q is not satisfied by the running program %q",
			ErrManifestInvalid, m.Program.MinCompatible, v.ProgramVersion)
	}
	if len(m.Artifacts) == 0 {
		return fmt.Errorf("%w: artifacts must declare at least one artifact", ErrManifestInvalid)
	}
	for i, artifact := range m.Artifacts {
		if strings.TrimSpace(artifact.Path) == "" || artifact.Bytes <= 0 || strings.TrimSpace(artifact.SHA256) == "" {
			return fmt.Errorf("%w: artifacts[%d] must carry path/bytes/sha256", ErrManifestInvalid, i)
		}
		if !regexpMatch(digestPattern, artifact.SHA256) {
			return fmt.Errorf("%w: artifacts[%d].sha256 must be sha256:<64 lowercase hex characters>", ErrManifestInvalid, i)
		}
	}
	switch m.Verification.State {
	case VerificationUnverified, VerificationRejected:
		// Not usable, but a valid record: rejected/unverified manifests are
		// legitimate documents (they simply never pass CheckUsable).
	case VerificationVerified:
		if !allChecksTrue(m.Verification.Checks) {
			return fmt.Errorf("%w: verification.state=verified with an incomplete check record is refused", ErrManifestInvalid)
		}
		if m.Verification.Target != VerificationTargetIsolated {
			return fmt.Errorf("%w: verification.target %q must be %s for a verified manifest",
				ErrManifestInvalid, m.Verification.Target, VerificationTargetIsolated)
		}
		if _, err := time.Parse(time.RFC3339, m.Verification.VerifiedAt); err != nil {
			return fmt.Errorf("%w: verification.verified_at %q is not RFC3339", ErrManifestInvalid, m.Verification.VerifiedAt)
		}
		if strings.TrimSpace(m.Verification.Verifier) == "" || strings.TrimSpace(m.Verification.EvidenceRef) == "" {
			return fmt.Errorf("%w: a verified manifest must record verifier and evidence_ref", ErrManifestInvalid)
		}
	default:
		return fmt.Errorf("%w: verification.state %q is not in the closed set", ErrManifestInvalid, m.Verification.State)
	}
	if err := ScanManifestSecrets(m); err != nil {
		return err
	}
	return nil
}

// validateCoverage makes the manifest's declaration complete rather than
// merely non-empty. Authoritative labels intentionally remain the canonical
// data-model object names; the FR-002 nine categories are represented by the
// complete object inventory, not by introducing a second incompatible label
// vocabulary.
func validateCoverage(coverage ManifestCoverage) error {
	if !sameUniqueStrings(coverage.Authoritative, canonicalAuthoritativeObjects) {
		return fmt.Errorf("%w: coverage.authoritative must declare the complete canonical data-DB object set", ErrManifestInvalid)
	}
	for _, required := range canonicalExcludedObjects {
		if !slices.Contains(coverage.Excluded, required) {
			return fmt.Errorf("%w: coverage.excluded must declare %s", ErrManifestInvalid, required)
		}
	}
	if hasDuplicateStrings(coverage.Excluded) {
		return fmt.Errorf("%w: coverage.excluded must not contain duplicate declarations", ErrManifestInvalid)
	}
	return nil
}

func sameUniqueStrings(actual, expected []string) bool {
	if len(actual) != len(expected) || hasDuplicateStrings(actual) {
		return false
	}
	for _, value := range expected {
		if !slices.Contains(actual, value) {
			return false
		}
	}
	return true
}

func hasDuplicateStrings(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}

func allChecksTrue(c ManifestChecks) bool {
	return c.Readable && c.StructureConstraints && c.BusinessStateProbes && c.VerificationExecutable
}

// CheckUsable enforces contracts/backup-manifest.md §5: only a verified
// manifest that validates may be used for restore or resumption. The state
// check comes first, so a not-verified document is always classified as
// ErrManifestNotVerified regardless of how incomplete its record is.
func (m *Manifest) CheckUsable(use ManifestUse, v ManifestValidation) error {
	switch use {
	case ManifestUseRestore, ManifestUseResumption:
	default:
		return fmt.Errorf("%w: unknown manifest use %q", ErrManifestInvalid, use)
	}
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrManifestInvalid)
	}
	if m.Verification.State != VerificationVerified {
		return fmt.Errorf("%w: verification.state=%s (use=%s)", ErrManifestNotVerified, m.Verification.State, use)
	}
	return m.Validate(v)
}

// ---------------------------------------------------------------------------
// Canonical JSON and digest
// ---------------------------------------------------------------------------

// CanonicalJSON renders the manifest as its canonical JSON document: the
// typed field order is stable, so key order and whitespace of the input never
// influence the digest. Array order is preserved because it is part of the
// 015.1 wire encoding (in particular, consumers may use the first artifact).
func (m *Manifest) CanonicalJSON() ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: nil manifest", ErrManifestInvalid)
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("encode canonical manifest: %w", err)
	}
	return encoded, nil
}

// Digest returns the canonical "sha256:<hex>" digest of the manifest body.
// The digest binds a manifest copy to the control-store evidence recorded for
// it: any edit (including flipping the verification fields by hand) changes
// the digest and is refused at restore (F7/DG-2).
func (m *Manifest) Digest() (string, error) {
	canonical, err := m.CanonicalJSON()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

var digestPattern = `^sha256:[0-9a-f]{64}$`

// CheckManifestDigest compares a recorded canonical digest with the current
// manifest body. A malformed, missing or mismatching digest is
// ErrManifestDigestMismatch - never "accept anyway".
func CheckManifestDigest(m *Manifest, recorded string) error {
	actual, err := m.Digest()
	if err != nil {
		return err
	}
	if !regexpMatch(digestPattern, recorded) || recorded != actual {
		return fmt.Errorf("%w: recorded digest does not match the manifest body", ErrManifestDigestMismatch)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Selection (deterministic over manifest fields only)
// ---------------------------------------------------------------------------

// SelectBackup returns the eligible backup with the highest recovery_point
// position (wal_lsn compared numerically, never as a string); ties are broken
// by the lexicographically smallest backup_id. created_at is display-only and
// candidate order, artifact paths, file names and mtimes play no part. No
// eligible candidate (including an empty list) is ErrNoEligibleBackup.
func SelectBackup(candidates []*Manifest, v ManifestValidation) (*Manifest, error) {
	var winner *Manifest
	var winnerPosition walPosition
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		if err := candidate.CheckUsable(ManifestUseRestore, v); err != nil {
			continue
		}
		position, ok := parseWALPosition(candidate.RecoveryPoint.LSN)
		if !ok {
			continue
		}
		switch {
		case winner == nil:
			winner, winnerPosition = candidate, position
		case position.greaterThan(winnerPosition):
			winner, winnerPosition = candidate, position
		case position.equal(winnerPosition) && candidate.BackupID < winner.BackupID:
			winner = candidate
		}
	}
	if winner == nil {
		return nil, ErrNoEligibleBackup
	}
	return winner, nil
}

type walPosition struct {
	high uint64
	low  uint64
}

func (p walPosition) greaterThan(other walPosition) bool {
	if p.high != other.high {
		return p.high > other.high
	}
	return p.low > other.low
}

func (p walPosition) equal(other walPosition) bool {
	return p.high == other.high && p.low == other.low
}

func parseWALPosition(lsn string) (walPosition, bool) {
	parts := strings.Split(lsn, "/")
	if len(parts) != 2 {
		return walPosition{}, false
	}
	high, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return walPosition{}, false
	}
	low, err := strconv.ParseUint(parts[1], 16, 64)
	if err != nil {
		return walPosition{}, false
	}
	return walPosition{high: high, low: low}, true
}

// ---------------------------------------------------------------------------
// Recovery point proofs (FR-036)
// ---------------------------------------------------------------------------

// RPOProofKind is the closed set of candidate recovery-point proof kinds.
type RPOProofKind string

// The candidate proof kinds. Only the exported snapshot tuple is a proof; the
// other three are explicitly forbidden by FR-036.
const (
	RPOProofSnapshotTuple        RPOProofKind = "snapshot_tuple"
	RPOProofBackupFrequency      RPOProofKind = "backup_frequency"
	RPOProofBusinessMaxCreatedAt RPOProofKind = "business_max_created_at"
	RPOProofFileMTime            RPOProofKind = "file_mtime"
)

// ValidateRPOProof refuses every proof kind but the exported snapshot tuple
// and refuses a snapshot tuple that is not complete. Backup frequency, a
// business-table MAX(created_at), the backup file mtime and an empty/unknown
// kind are forbidden proofs.
func ValidateRPOProof(kind RPOProofKind, point ManifestRecoveryPoint) error {
	if kind != RPOProofSnapshotTuple {
		if kind == "" {
			return fmt.Errorf("%w: an empty proof kind proves nothing", ErrRPOProofForbidden)
		}
		return fmt.Errorf("%w: %q", ErrRPOProofForbidden, kind)
	}
	if point.Snapshot.Xmin <= 0 || point.Snapshot.Xmax <= 0 {
		return fmt.Errorf("%w: the exported snapshot tuple (xmin/xip/xmax) is required", ErrRecoveryPointIncomplete)
	}
	if !regexpMatch(lsnPattern, point.LSN) {
		return fmt.Errorf("%w: wal_lsn %q is not a WAL position", ErrRecoveryPointIncomplete, point.LSN)
	}
	if _, err := time.Parse(time.RFC3339, point.WallClock); err != nil {
		return fmt.Errorf("%w: wall_clock %q is not RFC3339", ErrRecoveryPointIncomplete, point.WallClock)
	}
	if strings.TrimSpace(point.Server) == "" {
		return fmt.Errorf("%w: server identity is required", ErrRecoveryPointIncomplete)
	}
	if strings.TrimSpace(point.Database) == "" {
		return fmt.Errorf("%w: database identity is required", ErrRecoveryPointIncomplete)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Recovery measurement (contracts/backup-manifest.md §4)
// ---------------------------------------------------------------------------

// MeasurementState is the closed measurement state: known or explicitly
// unknown. An unknown measurement is valid input but is never an RPO proof.
type MeasurementState string

// The closed measurement states.
const (
	MeasurementUnknown MeasurementState = "unknown"
	MeasurementKnown   MeasurementState = "known"
)

// RecoveryMeasurementValue is one backup_lag/uncovered_interval value: the
// explicit string "unknown", or {"state":"known","seconds":N} with a
// non-negative N. An unknown value never carries a measured number.
type RecoveryMeasurementValue struct {
	State   MeasurementState
	Seconds float64
}

// MarshalJSON renders the wire form.
func (v RecoveryMeasurementValue) MarshalJSON() ([]byte, error) {
	switch v.State {
	case MeasurementUnknown:
		return json.Marshal("unknown")
	case MeasurementKnown:
		return json.Marshal(map[string]any{"state": "known", "seconds": v.Seconds})
	default:
		return nil, fmt.Errorf("%w: measurement state %q is not in the closed set", ErrManifestInvalid, v.State)
	}
}

// UnmarshalJSON parses the wire form. An unknown state with a measured value,
// a known state without a non-negative seconds value and any unrecognized
// shape are refused.
func (v *RecoveryMeasurementValue) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		if text != string(MeasurementUnknown) {
			return fmt.Errorf("%w: measurement %q is not %q", ErrManifestInvalid, text, MeasurementUnknown)
		}
		*v = RecoveryMeasurementValue{State: MeasurementUnknown}
		return nil
	}
	obj, err := jsonObject(data, "measurement")
	if err != nil {
		return err
	}
	rawState, ok := obj["state"]
	if !ok {
		return fmt.Errorf("%w: measurement.state is required", ErrManifestInvalid)
	}
	state, err := rawString(rawState)
	if err != nil {
		return fmt.Errorf("%w: measurement.state: %v", ErrManifestInvalid, err)
	}
	switch MeasurementState(state) {
	case MeasurementKnown:
		rawSeconds, ok := obj["seconds"]
		if !ok {
			return fmt.Errorf("%w: a known measurement must carry seconds", ErrManifestInvalid)
		}
		var seconds float64
		if err := json.Unmarshal(rawSeconds, &seconds); err != nil {
			return fmt.Errorf("%w: measurement.seconds: %v", ErrManifestInvalid, err)
		}
		if seconds < 0 {
			return fmt.Errorf("%w: measurement.seconds must be non-negative, got %v", ErrManifestInvalid, seconds)
		}
		*v = RecoveryMeasurementValue{State: MeasurementKnown, Seconds: seconds}
		return nil
	case MeasurementUnknown:
		if _, present := obj["seconds"]; present {
			return fmt.Errorf("%w: an unknown measurement must not carry a measured value", ErrManifestInvalid)
		}
		*v = RecoveryMeasurementValue{State: MeasurementUnknown}
		return nil
	default:
		return fmt.Errorf("%w: measurement state %q is not in the closed set", ErrManifestInvalid, state)
	}
}

// RecoveryMeasurement is the explicit recovery-point measurement record:
// backup_lag and uncovered_interval may be unknown, but they may never be
// omitted (contracts/backup-manifest.md §4).
type RecoveryMeasurement struct {
	RecoveryPoint     ManifestRecoveryPoint    `json:"recovery_point"`
	BackupLag         RecoveryMeasurementValue `json:"backup_lag"`
	UncoveredInterval RecoveryMeasurementValue `json:"uncovered_interval"`
}

// ParseRecoveryMeasurement parses a measurement document. Both measurement
// keys and the recovery point are required; "unknown" is a valid value, an
// omitted key is not.
func ParseRecoveryMeasurement(data []byte) (*RecoveryMeasurement, error) {
	top, err := jsonObject(data, "recovery_measurement")
	if err != nil {
		return nil, err
	}
	if err := requireKeys(top, "recovery_measurement", "recovery_point", "backup_lag", "uncovered_interval"); err != nil {
		return nil, err
	}
	point, err := parseRecoveryPoint(top["recovery_point"], "recovery_point")
	if err != nil {
		return nil, err
	}
	var lag, interval RecoveryMeasurementValue
	if err := json.Unmarshal(top["backup_lag"], &lag); err != nil {
		return nil, fmt.Errorf("%w: backup_lag: %v", ErrManifestInvalid, err)
	}
	if err := json.Unmarshal(top["uncovered_interval"], &interval); err != nil {
		return nil, fmt.Errorf("%w: uncovered_interval: %v", ErrManifestInvalid, err)
	}
	measurement := &RecoveryMeasurement{RecoveryPoint: point, BackupLag: lag, UncoveredInterval: interval}
	if err := measurement.Validate(); err != nil {
		return nil, err
	}
	return measurement, nil
}

// Validate refuses a measurement whose known values are negative or whose
// states are unknown. Explicit "unknown" is valid input.
func (m *RecoveryMeasurement) Validate() error {
	if m == nil {
		return fmt.Errorf("%w: nil recovery measurement", ErrManifestInvalid)
	}
	for name, value := range map[string]RecoveryMeasurementValue{
		"backup_lag": m.BackupLag, "uncovered_interval": m.UncoveredInterval,
	} {
		switch value.State {
		case MeasurementUnknown:
			if value.Seconds != 0 {
				return fmt.Errorf("%w: %s state=unknown must not carry seconds", ErrManifestInvalid, name)
			}
		case MeasurementKnown:
			if value.Seconds < 0 {
				return fmt.Errorf("%w: %s seconds must be non-negative", ErrManifestInvalid, name)
			}
		default:
			return fmt.Errorf("%w: %s state %q is not in the closed set", ErrManifestInvalid, name, value.State)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Program version compatibility (FR-004)
// ---------------------------------------------------------------------------

// ProgramCompatible reports whether the running program satisfies the
// manifest's minimum-compatible floor. Versions are dotted numeric with
// exactly two segments; comparison is numeric per segment, so "015.10" is
// newer than "015.9". Anything unparsable is refused (false).
func ProgramCompatible(running, floor string) bool {
	runningParts, ok := versionSegments(running)
	if !ok {
		return false
	}
	floorParts, ok := versionSegments(floor)
	if !ok {
		return false
	}
	for i := range runningParts {
		if runningParts[i] != floorParts[i] {
			return runningParts[i] > floorParts[i]
		}
	}
	return true
}

func versionSegments(value string) ([2]int64, bool) {
	var out [2]int64
	parts := strings.Split(value, ".")
	if len(parts) != 2 {
		return out, false
	}
	for i, part := range parts {
		if part == "" {
			return out, false
		}
		number, err := strconv.ParseInt(part, 10, 64)
		if err != nil || number < 0 {
			return out, false
		}
		out[i] = number
	}
	return out, true
}

// ---------------------------------------------------------------------------
// Secrecy scan (FR-007)
// ---------------------------------------------------------------------------

// ScanManifestSecrets refuses a manifest whose free-text fields embed a DSN,
// a credential value or PEM private-key material. The manifest travels next
// to the backup and is never a secret carrier. A coverage declaration that
// merely NAMES signer_private_keys is a declaration of exclusion, not secret
// material, and is not refused.
func ScanManifestSecrets(m *Manifest) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrManifestSecrets)
	}
	fields := []struct {
		field string
		value string
	}{
		{"manifest_version", m.ManifestVersion},
		{"backup_id", m.BackupID},
		{"created_at", m.CreatedAt},
		{"created_by", m.CreatedBy},
		{"carrier.kind", m.Carrier.Kind},
		{"carrier.pg_server_version", m.Carrier.PGServerVersion},
		{"carrier.pg_dump_version", m.Carrier.PGDumpVersion},
		{"recovery_point.wal_lsn", m.RecoveryPoint.LSN},
		{"recovery_point.wall_clock", m.RecoveryPoint.WallClock},
		{"recovery_point.server", m.RecoveryPoint.Server},
		{"recovery_point.database", m.RecoveryPoint.Database},
		{"program.version", m.Program.Version},
		{"program.min_compatible", m.Program.MinCompatible},
		{"verification.state", string(m.Verification.State)},
		{"verification.verified_at", m.Verification.VerifiedAt},
		{"verification.verifier", m.Verification.Verifier},
		{"verification.target", m.Verification.Target},
		{"verification.evidence_ref", m.Verification.EvidenceRef},
		{"retention_class", m.RetentionClass},
		{"note", m.Note},
	}
	for i, value := range m.Coverage.Authoritative {
		fields = append(fields, struct {
			field string
			value string
		}{fmt.Sprintf("coverage.authoritative[%d]", i), value})
	}
	for i, value := range m.Coverage.Excluded {
		fields = append(fields, struct {
			field string
			value string
		}{fmt.Sprintf("coverage.excluded[%d]", i), value})
	}
	for i, artifact := range m.Artifacts {
		fields = append(fields,
			struct {
				field string
				value string
			}{fmt.Sprintf("artifacts[%d].path", i), artifact.Path},
			struct {
				field string
				value string
			}{fmt.Sprintf("artifacts[%d].sha256", i), artifact.SHA256},
		)
	}
	for _, candidate := range fields {
		if secretShaped(candidate.value) {
			return fmt.Errorf("%w: field %s is not allowed to carry credential material", ErrManifestSecrets, candidate.field)
		}
	}
	return nil
}

// secretShaped reports whether a field value would be redacted (the shared
// logx.Redact boundary) or contains a PEM private-key marker. The check never
// echoes the value.
func secretShaped(value string) bool {
	if value == "" {
		return false
	}
	if logx.Redact(value) != value {
		return true
	}
	return strings.Contains(strings.ToUpper(value), "PRIVATE KEY")
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

func jsonObject(raw []byte, what string) (map[string]json.RawMessage, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("%w: %s must be a JSON object: %v", ErrManifestInvalid, what, err)
	}
	if obj == nil {
		return nil, fmt.Errorf("%w: %s must be a JSON object", ErrManifestInvalid, what)
	}
	return obj, nil
}

func requireKeys(obj map[string]json.RawMessage, path string, keys ...string) error {
	for _, key := range keys {
		if _, ok := obj[key]; !ok {
			return fmt.Errorf("%w: %s.%s is required", ErrManifestInvalid, path, key)
		}
	}
	return nil
}

func rawString(raw json.RawMessage) (string, error) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("must be a string: %v", err)
	}
	return value, nil
}

func requireString(obj map[string]json.RawMessage, key, path string, nonEmpty bool) (string, error) {
	value, err := rawString(obj[key])
	if err != nil {
		return "", fmt.Errorf("%w: %s: %v", ErrManifestInvalid, path, err)
	}
	if nonEmpty && strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%w: %s must not be empty", ErrManifestInvalid, path)
	}
	return value, nil
}

func requireInt64(obj map[string]json.RawMessage, key, path string) (int64, error) {
	var value int64
	if err := json.Unmarshal(obj[key], &value); err != nil {
		return 0, fmt.Errorf("%w: %s must be an integer: %v", ErrManifestInvalid, path, err)
	}
	return value, nil
}

func requireBool(obj map[string]json.RawMessage, key, path string) (bool, error) {
	var value bool
	if err := json.Unmarshal(obj[key], &value); err != nil {
		return false, fmt.Errorf("%w: %s must be a boolean: %v", ErrManifestInvalid, path, err)
	}
	return value, nil
}

func requireStringArray(obj map[string]json.RawMessage, key, path string, nonEmpty bool) ([]string, error) {
	var values []string
	if err := json.Unmarshal(obj[key], &values); err != nil {
		return nil, fmt.Errorf("%w: %s must be an array of strings: %v", ErrManifestInvalid, path, err)
	}
	if values == nil {
		return nil, fmt.Errorf("%w: %s must be an array", ErrManifestInvalid, path)
	}
	if nonEmpty && len(values) == 0 {
		return nil, fmt.Errorf("%w: %s must not be empty", ErrManifestInvalid, path)
	}
	return values, nil
}

func requireInt64Array(obj map[string]json.RawMessage, key, path string) ([]int64, error) {
	var values []int64
	if err := json.Unmarshal(obj[key], &values); err != nil {
		return nil, fmt.Errorf("%w: %s must be an array of integers: %v", ErrManifestInvalid, path, err)
	}
	if values == nil {
		// An explicit JSON null is not an array declaration.
		return nil, fmt.Errorf("%w: %s must be an array", ErrManifestInvalid, path)
	}
	return values, nil
}

func validUUID(value string) bool {
	return regexpMatch(uuidPattern, value)
}

// regexpMatch is a tiny helper for the anchored contract patterns; the
// patterns are constants, so a compile error is impossible.
func regexpMatch(pattern, value string) bool {
	ok, err := regexp.MatchString(pattern, value)
	return err == nil && ok
}

func equalInt64Set(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	left := slices.Clone(a)
	right := slices.Clone(b)
	slices.Sort(left)
	slices.Sort(right)
	return slices.Equal(left, right)
}

func majorVersion(version string) (int, error) {
	head := strings.SplitN(strings.TrimSpace(version), ".", 2)[0]
	major, err := strconv.Atoi(head)
	if err != nil || major <= 0 {
		return 0, fmt.Errorf("not a PostgreSQL version")
	}
	return major, nil
}
