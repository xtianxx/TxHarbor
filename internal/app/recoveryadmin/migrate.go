// recovery-admin migrate: control-store initialization and trust boundary
// (T008), with the schema version guard shared with the store (T069).
//
// `txharbor recovery-admin migrate up|status` acts ONLY on the independent
// control store reached through TXHARBOR_RECOVERY_CONTROL_DSN. The embedded
// control-store goose filesystem (internal/recovery/controlstore/schema) has
// its own version sequence; the data DB's migrations/ package and its
// goose_db_version table are never touched by this command (015 makes zero
// data-DB schema changes).
//
// Trust boundary and operating discipline:
//
//  1. No valid configuration means default deny. The control DSN and the data
//     DSN (TXHARBOR_PG_DSN) must both be configured. The data DSN is parsed
//     only — never opened — and exists here solely to prove the two targets
//     differ: a control DSN addressing the same database target as the data
//     DSN is refused before any connection is made, and an unset data DSN is
//     refused because independence would be unprovable.
//  2. A missing, malformed, unreachable or unauthenticated control DSN is
//     refused (non-zero exit) with a redacted message. There is no fallback to
//     the data DSN and no partial wiring.
//  3. Least privilege: the control database is a separate database. Its
//     migration role should own only the recovery_* DDL of this sequence; the
//     runtime role should hold only the DML it needs on recovery_* tables (no
//     DDL, no data-DB objects, no superuser). This command never creates roles
//     or databases and never touches data-DB objects.
//  4. Independent retention domain: control-store facts are not part of the
//     data-DB backup/restore set, and restoring the data DB never rewrites the
//     control store. The control store's own retention/backup is managed
//     separately, and blind restore of the control store is forbidden —
//     recovery of the control store itself is a stop-isolation + explicit
//     rebuild/supersede + audit path (ADR-001, data-model §4.4), never a plain
//     restore. data_target rows carry credential-free fingerprints only.
//  5. Unknown or incompatible control-store schema versions refuse fail-closed
//     as control_store_unavailable with the observed version annotated
//     (audited when the audit table exists); there is no best-effort read, no
//     silent downgrade and no new allow path. Pending migrations are the only
//     state `up` may advance; `status` reports them.
//  6. Plaintext DSNs never reach the control store, logs or evidence: every
//     message passes through logx.Redact, and audit rows carry only
//     credential-free target fingerprints.
package recoveryadmin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
)

// Operational bounds of the control-store migration path. These are local
// command bounds, not production thresholds: they bound the migration lock
// wait and the connect/ping attempt.
const (
	recoveryMigrateLockTimeout    = 30 * time.Second
	recoveryMigrateConnectTimeout = 5 * time.Second
)

// getenv is the process environment lookup of the command surface.
func (d Deps) getenv() func(string) (string, bool) {
	if d.Getenv == nil {
		return nil
	}
	return d.Getenv
}

// recoveryAdminMigrate implements `recovery-admin migrate up|status`.
func recoveryAdminMigrate(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	if len(args) != 1 {
		recoveryAdminMigrateUsage(stderr)
		return 2
	}
	phase := strings.TrimSpace(args[0])
	if phase != "up" && phase != "status" {
		fmt.Fprintf(stderr, "txharbor recovery-admin migrate: unknown action %q (want up|status)\n", args[0])
		recoveryAdminMigrateUsage(stderr)
		return 2
	}
	getenv := d.getenv()
	if getenv == nil {
		fmt.Fprintln(stderr, "txharbor recovery-admin migrate: environment lookup is not wired; refusing")
		return 1
	}

	controlDSN, ok := getenv(config.EnvRecoveryControlDSN)
	if !ok || strings.TrimSpace(controlDSN) == "" {
		fmt.Fprintf(stderr, "txharbor recovery-admin migrate: %s is required (not configured); refusing\n",
			config.EnvRecoveryControlDSN)
		return 1
	}
	dataDSN, ok := getenv(config.EnvPGDSN)
	if !ok || strings.TrimSpace(dataDSN) == "" {
		fmt.Fprintf(stderr,
			"txharbor recovery-admin migrate: %s is required to prove the control store is a different database target (not configured); refusing\n",
			config.EnvPGDSN)
		return 1
	}

	controlTarget, err := controlstore.ParseDSNTarget(controlDSN)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin migrate: %s is invalid: %s\n",
			config.EnvRecoveryControlDSN, logx.Redact(err.Error()))
		return 1
	}
	dataTarget, err := controlstore.ParseDSNTarget(dataDSN)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin migrate: %s is invalid: %s\n",
			config.EnvPGDSN, logx.Redact(err.Error()))
		return 1
	}
	if controlTarget.SameDatabase(dataTarget) {
		fmt.Fprintf(stderr,
			"txharbor recovery-admin migrate: %s and %s address the same database target; the control store must be an independent database (refusing)\n",
			config.EnvRecoveryControlDSN, config.EnvPGDSN)
		return 1
	}

	// Only the control DSN is ever opened; the data DSN above is parsed only.
	pool, err := db.OpenPool(ctx, controlDSN, recoveryMigrateConnectTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin migrate: control store unavailable: %s\n", logx.Redact(err.Error()))
		return 1
	}
	defer pool.Close()

	state, err := controlstore.InspectSchema(ctx, pool)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin migrate: control store unavailable: %s\n", logx.Redact(err.Error()))
		return 1
	}

	actor := migrateActor(getenv)
	fingerprint := controlTarget.DataTargetFingerprint().TargetFingerprint
	if phase == "status" {
		return recoveryAdminMigrateStatus(ctx, pool, state, phase, actor, fingerprint, stdout, stderr)
	}
	return recoveryAdminMigrateUp(ctx, pool, state, controlDSN, actor, fingerprint, stdout, stderr)
}

// recoveryAdminMigrateStatus prints the control-store schema state. Unknown
// or incompatible states (unknown version, newer-than-known version, recovery
// objects without a version table) refuse with control_store_unavailable and
// an audit note of the observed version; a pristine or known-prefix database
// reports its honest current/pending state.
func recoveryAdminMigrateStatus(ctx context.Context, pool *pgxpool.Pool, state controlstore.SchemaState,
	phase, actor, fingerprint string, stdout, stderr io.Writer) int {
	if err := state.CheckMigratable(); err != nil {
		return recoveryAdminMigrateVersionRefusal(ctx, pool, state, err, phase, actor, stderr)
	}
	scope := "uninitialized"
	if state.VersionTable {
		scope = "initialized"
	}
	fmt.Fprintf(stdout,
		"txharbor recovery-admin migrate: status control_store=%s current_version=%d target_version=%d pending=%d control_target_fingerprint=%s\n",
		scope, state.Current, state.Target, len(state.Pending), fingerprint)
	if len(state.Pending) == 0 {
		fmt.Fprintln(stdout, "pending=none")
		return 0
	}
	names := recoveryMigrationNames()
	for _, version := range state.Pending {
		fmt.Fprintf(stdout, "pending_version=%d name=%s\n", version, names[version])
	}
	return 0
}

// recoveryAdminMigrateUp applies the embedded control-store migrations after
// the version guard, re-inspects the result and refuses unless the post-state
// is exactly this binary's known version.
func recoveryAdminMigrateUp(ctx context.Context, pool *pgxpool.Pool, state controlstore.SchemaState,
	controlDSN, actor, fingerprint string, stdout, stderr io.Writer) int {
	if err := state.CheckMigratable(); err != nil {
		return recoveryAdminMigrateVersionRefusal(ctx, pool, state, err, "up", actor, stderr)
	}
	if state.VersionTable && state.Current == state.Target && len(state.Pending) == 0 {
		fmt.Fprintf(stdout,
			"txharbor recovery-admin migrate: up already_current=true applied=0 current_version=%d target_version=%d control_target_fingerprint=%s\n",
			state.Current, state.Target, fingerprint)
		return 0
	}

	appliedBefore := len(state.Applied)
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN:            controlDSN,
		LockTimeout:    recoveryMigrateLockTimeout,
		ConnectTimeout: recoveryMigrateConnectTimeout,
		FS:             schema.FS,
	}, stdout); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin migrate: up refused: %s\n", logx.Redact(err.Error()))
		return 1
	}

	after, err := controlstore.InspectSchema(ctx, pool)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin migrate: up applied but the post-migration check could not read the control store: %s\n",
			logx.Redact(err.Error()))
		return 1
	}
	if err := after.CheckCompatible(); err != nil {
		// The migration ran but the store is not exactly at the program's
		// known version (e.g. a concurrent migrator): fail closed, never claim
		// success over an unknown state.
		return recoveryAdminMigrateVersionRefusal(ctx, pool, after, err, "up", actor, stderr)
	}

	applied := len(after.Applied) - appliedBefore
	if applied > 0 {
		if err := recordMigrateSuccessAudit(ctx, pool, actor, applied, fingerprint); err != nil {
			fmt.Fprintf(stderr, "txharbor recovery-admin migrate: up applied %d migration(s) but the audit note could not be recorded: %s\n",
				applied, logx.Redact(err.Error()))
		}
	}
	fmt.Fprintf(stdout,
		"txharbor recovery-admin migrate: up applied=%d current_version=%d target_version=%d control_target_fingerprint=%s\n",
		applied, after.Current, after.Target, fingerprint)
	return 0
}

// recoveryAdminMigrateVersionRefusal is the fail-closed T069 exit for an
// unknown or incompatible control-store schema version: the refusal is
// expressed as control_store_unavailable, the observed version is annotated
// in a best-effort audit row (when the audit table exists), and no migration
// or read continues.
func recoveryAdminMigrateVersionRefusal(ctx context.Context, pool *pgxpool.Pool, state controlstore.SchemaState,
	cause error, phase, actor string, stderr io.Writer) int {
	var schemaErr *controlstore.SchemaError
	if !errors.As(cause, &schemaErr) {
		fmt.Fprintf(stderr, "txharbor recovery-admin migrate: control store unavailable (phase=%s): %s\n",
			phase, logx.Redact(cause.Error()))
		return 1
	}

	auditNote := "not_attempted"
	if slices.Contains(state.RecoveryObjects, "recovery_audit") {
		auditNote = "unrecorded"
		detail, err := json.Marshal(map[string]any{
			"phase":            phase,
			"observed_version": schemaErr.ObservedVersion,
			"target_version":   schemaErr.TargetVersion,
			"unknown_versions": schemaErr.UnknownVersions,
			"pending_versions": state.Pending,
			"version_table":    schemaErr.VersionTable,
			"reason":           schemaErr.Reason,
		})
		if err == nil {
			if auditErr := controlstore.WriteAudit(ctx, pool, controlstore.AuditRecord{
				Actor:        actor,
				Action:       "migrate_" + phase,
				Detail:       detail,
				Result:       controlstore.AuditRefused,
				RefusalClass: controlstore.RefusalControlStoreUnavailable,
			}); auditErr == nil {
				auditNote = "recorded"
			}
		}
	}

	fmt.Fprintf(stderr,
		"txharbor recovery-admin migrate: refused refusal_class=%s phase=%s observed_version=%d target_version=%d version_table=%t audit_note=%s reason=%q\n",
		controlstore.RefusalControlStoreUnavailable, phase, schemaErr.ObservedVersion,
		schemaErr.TargetVersion, schemaErr.VersionTable, auditNote, schemaErr.Reason)
	return 1
}

// recordMigrateSuccessAudit appends the best-effort provisioning note of an
// applied migration (fingerprint only; never the DSN).
func recordMigrateSuccessAudit(ctx context.Context, pool *pgxpool.Pool, actor string, applied int, fingerprint string) error {
	target, err := json.Marshal(map[string]any{"control_target_fingerprint": fingerprint})
	if err != nil {
		return err
	}
	detail, err := json.Marshal(map[string]any{"applied": applied})
	if err != nil {
		return err
	}
	return controlstore.WriteAudit(ctx, pool, controlstore.AuditRecord{
		Actor:  actor,
		Action: "migrate_up",
		Target: target,
		Detail: detail,
		Result: controlstore.AuditOK,
	})
}

// migrateActor binds the audit actor: the deployment-controlled principal
// when configured, else the command identity. It is an audit annotation, not
// an authorization (T008 does not require a principal for provisioning).
func migrateActor(getenv func(string) (string, bool)) string {
	if raw, ok := getenv(config.EnvRecoveryPrincipal); ok {
		if principal := strings.TrimSpace(raw); principal != "" {
			return principal
		}
	}
	return "recovery-admin:migrate"
}

// recoveryMigrationNames maps embedded versions to file names for the pending
// report.
func recoveryMigrationNames() map[int64]string {
	files, err := db.MigrationFiles(schema.FS)
	if err != nil {
		return nil
	}
	names := make(map[int64]string, len(files))
	for _, file := range files {
		names[file.Version] = file.Name
	}
	return names
}

// recoveryAdminMigrateUsage prints the migrate argument surface and its trust
// boundary.
func recoveryAdminMigrateUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: txharbor recovery-admin migrate up|status")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "  up      apply the embedded control-store migrations (control DSN only)")
	fmt.Fprintln(w, "  status  show the control-store schema version and pending migrations")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Only TXHARBOR_RECOVERY_CONTROL_DSN is migrated; TXHARBOR_PG_DSN is read only to prove the two")
	fmt.Fprintln(w, "targets differ (same database target -> refused, unset -> refused). The control store is an")
	fmt.Fprintln(w, "independent database outside the data-DB backup/restore set; it carries recovery governance")
	fmt.Fprintln(w, "facts only and never a plaintext DSN. Unknown or incompatible control-store schema versions are")
	fmt.Fprintln(w, "refused fail-closed as control_store_unavailable with the observed version annotated; there is no")
	fmt.Fprintln(w, "best-effort read, no downgrade and no new allow path.")
}
