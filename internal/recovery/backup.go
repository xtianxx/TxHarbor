// backup.go implements T018: the backup executor of quickstart S1 and
// ADR-002/FR-002/003/006/007.
//
// One backup is a transactionally consistent pg_dump of the data DB:
//
//	BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY
//	SELECT pg_current_wal_lsn()::text, pg_current_snapshot()::text,
//	       pg_export_snapshot(), now()
//
// The exporting transaction stays open while pg_dump runs with
// --snapshot=<id> against the SAME DSN that exported the snapshot. The
// manifest recovery point is exactly the exported snapshot tuple plus the
// export-time WAL LSN upper bound, wall clock and server/database identity -
// never a business-table timestamp, a backup frequency or a file mtime
// (FR-036). A snapshot invalidation, an export failure or a dump failure is
// an explicit failure that must not produce a success manifest and must not
// mix with pre-existing artifacts: the dump streams into a private temp file
// and only a fully written artifact plus manifest are published.
//
// Safety boundary (doc.go, contracts/resumption-gate.md): the backup path
// reads and dumps only. It publishes no event, signs nothing, broadcasts
// nothing and calls no business RPC.
package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
)

// PGCommand runs one PostgreSQL client binary (pg_dump/pg_restore). It is the
// injection point that keeps the executors' tool path real and testable: the
// integration layer executes the pinned image's binaries in the database
// container, the operator CLI runs the local binaries. There is no in-process
// dumper substitute anywhere.
type PGCommand interface {
	Run(ctx context.Context, name string, args []string, stdin io.Reader, stdout, stderr io.Writer) error
}

// LocalPGCommand runs the PostgreSQL client binaries found on PATH: the
// production operator path of `recovery-admin`.
type LocalPGCommand struct{}

// Run executes name with args, wiring the streams through. A non-zero exit is
// an error; stdout/stderr are owned by the caller.
func (LocalPGCommand) Run(ctx context.Context, name string, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("run %s: %w", name, err)
	}
	return nil
}

// SnapshotExport is what the exporting transaction observed: the exported
// snapshot id and the recovery point that will be recorded in the manifest.
// The barrier callback of BackupOptions receives it while the export
// transaction is still open (and therefore still importable).
type SnapshotExport struct {
	SnapshotID    string                `json:"snapshot_id"`
	RecoveryPoint ManifestRecoveryPoint `json:"recovery_point"`
}

// BackupOptions is one backup execution. DSN is both the snapshot-export DSN
// and the pg_dump DSN: the dump must run against exactly the exported
// snapshot, on exactly the same database.
type BackupOptions struct {
	// DSN is the data-DB DSN (snapshot export + pg_dump).
	DSN string
	// ArtifactDir is the backup product directory (dump + manifest).
	ArtifactDir string
	// ChainID is the scope annotation recorded in the audit trail (optional
	// at the library layer; the CLI requires it by name).
	ChainID string
	// CreatedBy is the authenticated principal recorded on the manifest.
	CreatedBy string
	// ProgramVersion is the generating program identity.
	ProgramVersion string
	// ProgramMinCompatible is the minimum-compatible program floor.
	ProgramMinCompatible string
	// PG executes pg_dump. Required: a backup without a real dumper is
	// refused, never simulated.
	PG PGCommand
	// SnapshotBarrier, when set, runs while the export transaction is open
	// and after the recovery point was observed but before pg_dump starts. It
	// exists for real concurrency checks (an independent session can import
	// the exported snapshot; a write committed after the export must never
	// appear in the dump). An error aborts the backup with no manifest.
	SnapshotBarrier func(ctx context.Context, export SnapshotExport) error
}

func (o BackupOptions) validate() error {
	if strings.TrimSpace(o.DSN) == "" {
		return fmt.Errorf("backup requires a data DSN")
	}
	if strings.TrimSpace(o.ArtifactDir) == "" {
		return fmt.Errorf("backup requires an artifact directory")
	}
	if strings.TrimSpace(o.CreatedBy) == "" {
		return fmt.Errorf("backup requires the creating principal (created_by)")
	}
	if strings.TrimSpace(o.ProgramVersion) == "" {
		return fmt.Errorf("backup requires the generating program version")
	}
	if strings.TrimSpace(o.ProgramMinCompatible) == "" {
		return fmt.Errorf("backup requires the minimum-compatible program version")
	}
	if o.PG == nil {
		return fmt.Errorf("backup requires a PGCommand (the real pg_dump path); refusing to fake a backup")
	}
	return nil
}

// BackupResult is one successful backup: the manifest, its path and the
// published artifact path.
type BackupResult struct {
	Manifest     *Manifest `json:"manifest"`
	ManifestPath string    `json:"manifest_path"`
	ArtifactPath string    `json:"artifact_path"`
}

// ExecuteBackup produces one backup artifact plus its manifest. The artifact
// bytes and sha256 are computed from the real file and bound into
// artifacts[]; a failure produces no success manifest and leaves the
// artifact directory without partial products.
func ExecuteBackup(ctx context.Context, opts BackupOptions) (BackupResult, error) {
	if err := opts.validate(); err != nil {
		return BackupResult{}, err
	}
	backupID := uuid.NewString()

	conn, err := pgx.Connect(ctx, opts.DSN)
	if err != nil {
		return BackupResult{}, fmt.Errorf("connect backup source: %s", logx.Redact(err.Error()))
	}
	defer func() { _ = conn.Close(ctx) }()

	var serverVersion string
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version')").Scan(&serverVersion); err != nil {
		return BackupResult{}, fmt.Errorf("read source server version: %s", logx.Redact(err.Error()))
	}
	serverMajor, err := majorVersion(serverVersion)
	if err != nil {
		return BackupResult{}, fmt.Errorf("source server version %q: %w", serverVersion, err)
	}
	gooseVersions, err := readGooseVersions(ctx, conn)
	if err != nil {
		return BackupResult{}, err
	}
	dumpVersion, err := pgToolVersion(ctx, opts.PG, "pg_dump")
	if err != nil {
		return BackupResult{}, err
	}

	// Consistency snapshot: the exporting transaction stays open until the
	// dump finished, so pg_dump's --snapshot import cannot race an
	// invalidated export.
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return BackupResult{}, fmt.Errorf("begin snapshot export transaction: %s", logx.Redact(err.Error()))
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		lsn          string
		snapshotText string
		snapshotID   string
		wallClock    time.Time
	)
	if err := tx.QueryRow(ctx, `
SELECT pg_current_wal_lsn()::text, pg_current_snapshot()::text, pg_export_snapshot(), now()`).
		Scan(&lsn, &snapshotText, &snapshotID, &wallClock); err != nil {
		return BackupResult{}, fmt.Errorf("export consistency snapshot: %s", logx.Redact(err.Error()))
	}
	if strings.TrimSpace(snapshotID) == "" {
		return BackupResult{}, fmt.Errorf("exported snapshot id is empty; refusing to take a non-snapshot dump")
	}
	snapshot, err := parseSnapshotText(snapshotText)
	if err != nil {
		return BackupResult{}, fmt.Errorf("exported snapshot tuple %q: %w", snapshotText, err)
	}

	var serverIdentity string
	if err := conn.QueryRow(ctx,
		`SELECT COALESCE(inet_server_addr()::text, 'local') || ':' || COALESCE(inet_server_port()::text, '0')`).
		Scan(&serverIdentity); err != nil {
		return BackupResult{}, fmt.Errorf("read source server identity: %s", logx.Redact(err.Error()))
	}
	var database string
	if err := conn.QueryRow(ctx, `SELECT current_database()`).Scan(&database); err != nil {
		return BackupResult{}, fmt.Errorf("read source database identity: %s", logx.Redact(err.Error()))
	}

	recoveryPoint := ManifestRecoveryPoint{
		Snapshot:  snapshot,
		LSN:       lsn,
		WallClock: wallClock.UTC().Format(time.RFC3339),
		Server:    serverIdentity,
		Database:  database,
	}
	export := SnapshotExport{SnapshotID: snapshotID, RecoveryPoint: recoveryPoint}
	if opts.SnapshotBarrier != nil {
		if err := opts.SnapshotBarrier(ctx, export); err != nil {
			return BackupResult{}, fmt.Errorf("snapshot barrier refused the backup: %w", err)
		}
	}

	if err := os.MkdirAll(opts.ArtifactDir, 0o700); err != nil {
		return BackupResult{}, fmt.Errorf("prepare artifact directory: %w", err)
	}
	partialPath := filepath.Join(opts.ArtifactDir, ".txharbor-"+backupID+".partial")
	file, err := os.OpenFile(partialPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return BackupResult{}, fmt.Errorf("create partial artifact: %w", err)
	}
	var dumpStderr bytes.Buffer
	runErr := opts.PG.Run(ctx, "pg_dump",
		[]string{"--format=custom", "--snapshot=" + snapshotID, "--dbname=" + opts.DSN},
		nil, file, &dumpStderr)
	closeErr := file.Close()
	if runErr != nil {
		_ = os.Remove(partialPath)
		return BackupResult{}, fmt.Errorf("pg_dump failed: %w: %s", runErr, logx.Redact(strings.TrimSpace(dumpStderr.String())))
	}
	if closeErr != nil {
		_ = os.Remove(partialPath)
		return BackupResult{}, fmt.Errorf("finalize artifact: %w", closeErr)
	}

	info, err := os.Stat(partialPath)
	if err != nil {
		_ = os.Remove(partialPath)
		return BackupResult{}, fmt.Errorf("stat artifact: %w", err)
	}
	if info.Size() <= 0 {
		_ = os.Remove(partialPath)
		return BackupResult{}, fmt.Errorf("pg_dump produced an empty artifact; refusing to publish a backup")
	}
	checksum, err := fileSHA256(partialPath)
	if err != nil {
		_ = os.Remove(partialPath)
		return BackupResult{}, err
	}
	artifactPath := filepath.Join(opts.ArtifactDir, "dump-"+backupID+".pgcustom")
	if err := os.Rename(partialPath, artifactPath); err != nil {
		_ = os.Remove(partialPath)
		return BackupResult{}, fmt.Errorf("publish artifact: %w", err)
	}

	m := &Manifest{
		ManifestVersion: ManifestVersionV1,
		BackupID:        backupID,
		CreatedAt:       recoveryPoint.WallClock,
		CreatedBy:       opts.CreatedBy,
		Carrier: ManifestCarrier{
			Kind:            CarrierKindPGDumpCustom,
			PGServerVersion: serverVersion,
			PGDumpVersion:   dumpVersion,
		},
		Coverage: ManifestCoverage{
			Authoritative: CanonicalAuthoritativeObjects(),
			Excluded:      CanonicalExcludedObjects(),
		},
		RecoveryPoint: recoveryPoint,
		Schema:        ManifestSchema{GooseDBVersion: gooseVersions},
		Program: ManifestProgram{
			Version:       opts.ProgramVersion,
			MinCompatible: opts.ProgramMinCompatible,
		},
		Artifacts: []ManifestArtifact{{
			Path:   filepath.Base(artifactPath),
			Bytes:  info.Size(),
			SHA256: checksum,
		}},
		Verification: ManifestVerification{State: VerificationUnverified},
	}
	if err := m.Validate(ManifestValidation{
		PGServerMajor:  serverMajor,
		TargetSchema:   gooseVersions,
		ProgramVersion: opts.ProgramVersion,
	}); err != nil {
		_ = os.Remove(artifactPath)
		return BackupResult{}, fmt.Errorf("refusing to publish an invalid manifest: %w", err)
	}
	canonical, err := m.CanonicalJSON()
	if err != nil {
		_ = os.Remove(artifactPath)
		return BackupResult{}, err
	}
	manifestPath := filepath.Join(opts.ArtifactDir, "manifest-"+backupID+".json")
	if err := writeFileAtomic(manifestPath, canonical); err != nil {
		_ = os.Remove(artifactPath)
		return BackupResult{}, err
	}
	return BackupResult{Manifest: m, ManifestPath: manifestPath, ArtifactPath: artifactPath}, nil
}

// ---------------------------------------------------------------------------
// Shared real-tool helpers
// ---------------------------------------------------------------------------

// readGooseVersions reads the exact applied goose set of a data database.
func readGooseVersions(ctx context.Context, conn *pgx.Conn) ([]int64, error) {
	rows, err := conn.Query(ctx,
		`SELECT version_id FROM goose_db_version WHERE is_applied AND version_id > 0 ORDER BY version_id`)
	if err != nil {
		return nil, fmt.Errorf("read goose_db_version set: %s", logx.Redact(err.Error()))
	}
	defer rows.Close()
	var versions []int64
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("scan goose version: %w", err)
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read goose_db_version set: %w", err)
	}
	if len(versions) == 0 {
		return nil, fmt.Errorf("source database has no applied migrations; refusing to back up an unmigrated database")
	}
	return versions, nil
}

var pgToolVersionPattern = regexp.MustCompile(`PostgreSQL\)\s*([0-9][0-9.]*)`)

// pgToolVersion runs `<tool> --version` and extracts the version. A tool that
// cannot report its version refuses the operation: the manifest must record
// the real client version that produced the artifact.
func pgToolVersion(ctx context.Context, pg PGCommand, tool string) (string, error) {
	var stdout, stderr bytes.Buffer
	if err := pg.Run(ctx, tool, []string{"--version"}, nil, &stdout, &stderr); err != nil {
		return "", fmt.Errorf("read %s version: %w: %s", tool, err, logx.Redact(strings.TrimSpace(stderr.String())))
	}
	match := pgToolVersionPattern.FindStringSubmatch(stdout.String())
	if match == nil {
		return "", fmt.Errorf("%s --version output did not name a PostgreSQL version", tool)
	}
	return match[1], nil
}

// pgListArchive runs `pg_restore --list` over the archive on stdin: the
// readability check of contracts/backup-manifest.md §3 ("dump can be listed,
// no truncation/checksum error").
func pgListArchive(ctx context.Context, pg PGCommand, archive io.Reader) error {
	var stdout, stderr bytes.Buffer
	if err := pg.Run(ctx, "pg_restore", []string{"--list"}, archive, &stdout, &stderr); err != nil {
		return fmt.Errorf("pg_restore --list refused the archive: %w: %s", err, logx.Redact(strings.TrimSpace(stderr.String())))
	}
	if strings.TrimSpace(stdout.String()) == "" {
		return fmt.Errorf("pg_restore --list produced no table of contents; refusing an unreadable archive")
	}
	return nil
}

// pgRestoreInto runs a real pg_restore of the archive (stdin) into the target
// DSN. --clean --if-exists make the rerun after a rebuild idempotent on a
// fresh/rebuild database (T019, F2).
func pgRestoreInto(ctx context.Context, pg PGCommand, archive io.Reader, targetDSN string) error {
	var stdout, stderr bytes.Buffer
	if err := pg.Run(ctx, "pg_restore",
		[]string{"--clean", "--if-exists", "--dbname=" + targetDSN}, archive, &stdout, &stderr); err != nil {
		return fmt.Errorf("pg_restore failed: %w: %s", err, logx.Redact(strings.TrimSpace(stderr.String())))
	}
	return nil
}

// parseSnapshotText parses the "xmin:xmax:xip_list" text form of
// pg_current_snapshot().
func parseSnapshotText(text string) (ManifestSnapshot, error) {
	parts := strings.Split(strings.TrimSpace(text), ":")
	if len(parts) < 2 {
		return ManifestSnapshot{}, fmt.Errorf("not a snapshot tuple")
	}
	xmin, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil {
		return ManifestSnapshot{}, fmt.Errorf("xmin %q: %w", parts[0], err)
	}
	xmax, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil {
		return ManifestSnapshot{}, fmt.Errorf("xmax %q: %w", parts[1], err)
	}
	snapshot := ManifestSnapshot{Xmin: xmin, Xip: []int64{}, Xmax: xmax}
	if len(parts) >= 3 && strings.TrimSpace(parts[2]) != "" {
		for _, raw := range strings.Split(parts[2], ",") {
			xid, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
			if err != nil {
				return ManifestSnapshot{}, fmt.Errorf("xip entry %q: %w", raw, err)
			}
			snapshot.Xip = append(snapshot.Xip, xid)
		}
	}
	return snapshot, nil
}

// newUUIDString returns a fresh v4 UUID (backup ids, evidence ids).
func newUUIDString() string { return uuid.NewString() }

// fileSHA256 computes the canonical "sha256:<hex>" digest of a file.
func fileSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read artifact: %w", err)
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// resolveArtifactPath resolves a manifest artifact path against the manifest's
// own directory (a relative path never escapes into guesswork).
func resolveArtifactPath(manifestPath, artifactPath string) string {
	if filepath.IsAbs(artifactPath) {
		return artifactPath
	}
	return filepath.Join(filepath.Dir(manifestPath), artifactPath)
}

// writeFileAtomic publishes a file by writing a sibling temp file and
// renaming it, so a reader never sees a half-written manifest.
func writeFileAtomic(path string, data []byte) error {
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("publish %s: %w", filepath.Base(path), err)
	}
	return nil
}

// readManifestFile reads and parses one manifest from disk.
func readManifestFile(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	return ParseManifest(data)
}

// RepositoryTargetSchema returns the exact goose set of the running program's
// embedded data-DB migrations: the schema a manifest must equal to be
// compatible (FR-004).
func RepositoryTargetSchema() ([]int64, error) {
	files, err := db.MigrationFiles(db.Migrations)
	if err != nil {
		return nil, fmt.Errorf("list repository migrations: %w", err)
	}
	versions := make([]int64, 0, len(files))
	for _, file := range files {
		versions = append(versions, file.Version)
	}
	return versions, nil
}

// CurrentProgramVersion derives the running program identity from the
// embedded migration target ("018.0" for the 18-migration schema). The
// manifest's program.version/min_compatible use this identity; the CLI layer
// passes it to ExecuteBackup/ExecuteRestore/ExecuteVerifyBackup.
func CurrentProgramVersion() (string, error) {
	versions, err := RepositoryTargetSchema()
	if err != nil {
		return "", err
	}
	if len(versions) == 0 {
		return "", fmt.Errorf("repository has no migrations; cannot derive a program version")
	}
	return fmt.Sprintf("%03d.0", versions[len(versions)-1]), nil
}
