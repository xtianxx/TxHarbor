// permissionadmin.go implements the T023 local-privileged 014 permission
// management surface (`permission-grant`/`permission-revoke`/`permission-show`)
// following the 011 withdrawalexec pattern (contracts/auth-matrix.md
// Management; management authorization ruling of 2026-09-26):
//
//   - local execution only; the actor is bound from the authenticated caller
//     (TXHARBOR_RECON_PRINCIPAL) and the manageable scope/identity comes from
//     the controlled deployment config (TXHARBOR_RECON_MANAGEMENT_TRUST, the
//     T009 ManagementConfig trust root). Without a valid trust root every
//     management operation is denied and audited by default.
//   - a single authenticated manager executes grants/revokes/queries; this
//     phase has no second approver, and no admin role is created. Management
//     authority is orthogonal to recon_permission: no grant row can authorize
//     a management action, so ordinary 014 holders can never self-grant.
//   - only the five 014 permissions are grantable. Withdrawal, signing,
//     payment and existing-recovery authority is never granted, created or
//     expanded here; calls into those capabilities still satisfy their own
//     authorization and gates elsewhere.
//   - --operator/--reason/--operation-id are required audit/idempotency
//     carriage and never authorization inputs. Grants and revokes record
//     before/after state, operator and result; refusals are audited by the
//     T009 evaluator; operation_id is deduplicated (same input reports the
//     recorded outcome, different input is operation_conflict with zero
//     writes).
//   - permission-show is strictly read-only; an allowed query writes no audit
//     row.
package reconcileadmin

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/reconciliation"
)

// reconcileManagementTrust loads the controlled deployment-config management
// trust root (TXHARBOR_RECON_MANAGEMENT_TRUST: identity binding + manageable
// scope). An absent or invalid configuration returns a nil trust root: the
// evaluator then denies management by default and audits the refusal.
func reconcileManagementTrust(cfg *config.Config) (*reconciliation.ManagementTrust, error) {
	raw := strings.TrimSpace(cfg.Recon.ManagementTrustRaw)
	if raw == "" {
		return nil, fmt.Errorf("%s is not configured; management is denied by default", config.EnvReconManagementTrust)
	}
	var managementConfig reconciliation.ManagementConfig
	if err := json.Unmarshal([]byte(raw), &managementConfig); err != nil {
		return nil, fmt.Errorf("%s is not a valid management trust config: %v", config.EnvReconManagementTrust, err)
	}
	trust, err := managementConfig.Trust()
	if err != nil {
		return nil, fmt.Errorf("%s is invalid: %v", config.EnvReconManagementTrust, err)
	}
	return &trust, nil
}

// printReconcileManagementTrustHint prints the trust-root configuration error
// next to a management refusal (never its content).
func printReconcileManagementTrustHint(stderr io.Writer, trustErr error) {
	if trustErr == nil {
		return
	}
	fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(trustErr.Error()))
}

// parseReconcileManagementScope parses the required target scope of a
// grant/revoke. Height scopes may be range-unbounded or carry an explicit
// range; time scopes are range-unbounded in the height-typed AuthScope carrier
// (the T018/T022 convention), so --from/--to are refused for them.
func parseReconcileManagementScope(chainID, scopeKind, businessTypes, from, to string) (reconciliation.AuthScope, error) {
	if strings.TrimSpace(chainID) == "" || strings.TrimSpace(scopeKind) == "" || strings.TrimSpace(businessTypes) == "" {
		return reconciliation.AuthScope{}, errors.New("--chain-id, --scope-kind and --business-types are required")
	}
	return buildReconcileManagementScope(chainID, scopeKind, businessTypes, from, to)
}

// parseReconcileOptionalManagementScope parses an optional scope filter for
// permission-show: all scope flags absent means no explicit filter (nil).
func parseReconcileOptionalManagementScope(chainID, scopeKind, businessTypes, from, to string) (*reconciliation.AuthScope, error) {
	if strings.TrimSpace(chainID) == "" && strings.TrimSpace(scopeKind) == "" &&
		strings.TrimSpace(businessTypes) == "" && strings.TrimSpace(from) == "" && strings.TrimSpace(to) == "" {
		return nil, nil
	}
	scope, err := parseReconcileManagementScope(chainID, scopeKind, businessTypes, from, to)
	if err != nil {
		return nil, err
	}
	return &scope, nil
}

// buildReconcileManagementScope builds and validates one AuthScope from CLI
// text. Unknown business types, inverted or one-sided ranges and non-height
// time ranges are refused before any authorization evaluation.
func buildReconcileManagementScope(chainID, scopeKind, businessTypes, from, to string) (reconciliation.AuthScope, error) {
	kind := reconciliation.ScopeKind(strings.TrimSpace(scopeKind))
	if !kind.Known() {
		return reconciliation.AuthScope{}, fmt.Errorf("--scope-kind must be %q or %q",
			reconciliation.ScopeHeight, reconciliation.ScopeTime)
	}
	types, err := parseReconBusinessTypes(businessTypes)
	if err != nil {
		return reconciliation.AuthScope{}, err
	}
	scope := reconciliation.AuthScope{ChainID: strings.TrimSpace(chainID), Kind: kind, BusinessTypes: types}
	rangeFrom, rangeTo := strings.TrimSpace(from), strings.TrimSpace(to)
	switch {
	case kind == reconciliation.ScopeTime:
		if rangeFrom != "" || rangeTo != "" {
			return reconciliation.AuthScope{}, errors.New("time-kind auth scopes are range-unbounded; omit --from/--to")
		}
	case rangeFrom == "" && rangeTo == "":
		// Range-unbounded height grant: covers any request in the scope.
	case rangeFrom == "" || rangeTo == "":
		return reconciliation.AuthScope{}, errors.New("--from and --to must be provided together")
	default:
		start, err := strconv.ParseInt(rangeFrom, 10, 64)
		if err != nil || start < 0 {
			return reconciliation.AuthScope{}, fmt.Errorf("--from %q is not a non-negative block height", from)
		}
		end, err := strconv.ParseInt(rangeTo, 10, 64)
		if err != nil || end < start {
			return reconciliation.AuthScope{}, fmt.Errorf("--to %q is not a block height >= --from", to)
		}
		scope.RangeStart, scope.RangeEnd = &start, &end
	}
	if err := scope.Validate(); err != nil {
		return reconciliation.AuthScope{}, err
	}
	return scope, nil
}

// parseReconcileTargetPrincipal binds an explicit grant/revoke/show target (or
// filter) principal from its canonical <kind>:<id> form. The actor identity
// itself is never accepted from a flag; this is the subject the manager
// manages. Identities the recon_permission registry cannot store (> 128
// characters) are refused.
func parseReconcileTargetPrincipal(raw string) (reconciliation.Principal, error) {
	principal, err := reconciliation.ConfigPrincipal(raw)
	if err != nil {
		return reconciliation.Principal{}, err
	}
	if len(principal.String()) > 128 {
		return reconciliation.Principal{}, fmt.Errorf("principal %q exceeds the conservative 128-byte cap for registry rows (the recon_permission columns allow up to 128 characters)", principal.String())
	}
	return principal, nil
}

// reconcileAdminPermissionGrant single-executes one 014 permission grant.
func reconcileAdminPermissionGrant(ctx context.Context, args []string, d Deps) int {
	return reconcileAdminPermissionMutation(ctx, args, d, reconciliation.ManagementGrant, "grant")
}

// reconcileAdminPermissionRevoke single-executes one 014 permission revoke
// (effective immediately after commit; the audit trail is retained).
func reconcileAdminPermissionRevoke(ctx context.Context, args []string, d Deps) int {
	return reconcileAdminPermissionMutation(ctx, args, d, reconciliation.ManagementRevoke, "revoke")
}

// reconcileAdminPermissionMutation implements permission-grant/permission-revoke:
// local execution only, actor bound from the authenticated caller, valid
// deployment trust root required (default deny + audited refusal otherwise),
// target principal/action/scope/reason named, before/after state and result
// audited, operation_id deduplicated. Only the five 014 permissions are
// grantable; the recon_permission registry itself never authorizes management,
// so ordinary 014 holders cannot self-grant.
func reconcileAdminPermissionMutation(ctx context.Context, args []string, d Deps,
	kind reconciliation.ManagementKind, command string) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("reconcile-admin permission-"+command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	targetRaw := fs.String("target-principal", "", "managed principal <kind>:<id> (required)")
	permissionRaw := fs.String("permission", "", "014 permission: scan_manage|exception_handle|dispose_ack|dispose_reuse|close (required)")
	chainID := fs.String("chain-id", "", "grant scope chain identity (required)")
	scopeKind := fs.String("scope-kind", "", "grant scope dimension: height|time (required)")
	businessTypes := fs.String("business-types", "", "comma-separated grant scope business types (required)")
	from := fs.String("from", "", "scope range start height (optional, must accompany --to)")
	to := fs.String("to", "", "scope range end height (optional, must accompany --from)")
	operationID := fs.String("operation-id", "", "idempotent operation identity (required)")
	operator := fs.String("operator", "", "declared operator identity (required, audit annotation only)")
	reason := fs.String("reason", "", "operator reason (required, audit annotation only)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*targetRaw) == "" || strings.TrimSpace(*permissionRaw) == "" ||
		strings.TrimSpace(*chainID) == "" || strings.TrimSpace(*scopeKind) == "" ||
		strings.TrimSpace(*businessTypes) == "" || strings.TrimSpace(*operationID) == "" ||
		strings.TrimSpace(*operator) == "" || strings.TrimSpace(*reason) == "" {
		reconcileAdminUsage(stderr)
		return 2
	}
	operatorName, reasonText := strings.TrimSpace(*operator), strings.TrimSpace(*reason)
	if len(operatorName) > 128 || len(reasonText) > 1024 {
		fmt.Fprintln(stderr, "txharbor reconcile-admin: --operator must be 1..128 characters and --reason 1..1024 characters")
		return 2
	}
	operation, err := parseReconcileOperationID(*operationID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}
	scope, err := parseReconcileManagementScope(*chainID, *scopeKind, *businessTypes, *from, *to)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}
	// A malformed target is passed through as an unauthenticated identity so
	// the evaluator owns the denial (and audits it), never a silent skip.
	target, targetErr := parseReconcileTargetPrincipal(*targetRaw)
	permission := reconciliation.Permission(strings.TrimSpace(*permissionRaw))

	env, code := reconcileAdminOpen(ctx, d)
	if env == nil {
		return code
	}
	defer env.pool.Close()

	if len(env.principal.String()) > 128 {
		fmt.Fprintln(stderr, "txharbor reconcile-admin: the authenticated principal's canonical identity exceeds the conservative 128-byte cap for registry/audit rows (the columns themselves allow up to 128 characters)")
		return 1
	}

	trust, trustErr := reconcileManagementTrust(env.cfg)
	decision, err := env.evaluator.AuthorizeManagement(ctx, trust, env.principal, reconciliation.ManagementRequest{
		Kind:            kind,
		TargetPrincipal: target,
		Permission:      permission,
		Scope:           scope,
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: authorization failed: %s\n", logx.Redact(err.Error()))
		printReconcileManagementTrustHint(stderr, trustErr)
		return 1
	}
	if !decision.Allowed {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: refused permission-%s: %s (%s)\n",
			command, decision.Reason, decision.Detail)
		printReconcileManagementTrustHint(stderr, trustErr)
		return 1
	}
	if targetErr != nil {
		// Unreachable with a valid trust root: EvaluateManagement denies an
		// unparsable target before any write. Defensive fail-closed.
		fmt.Fprintf(stderr, "txharbor reconcile-admin: invalid --target-principal: %s\n", logx.Redact(targetErr.Error()))
		return 1
	}

	result, err := env.applyPermissionMutation(ctx, kind, target, permission, scope,
		operatorName, reasonText, operation)
	switch {
	case errors.Is(err, errReconcileOperationConflict):
		fmt.Fprintf(stderr, "txharbor reconcile-admin: operation_conflict operation_id=%s\n", operation)
		return 1
	case err != nil:
		fmt.Fprintf(stderr, "txharbor reconcile-admin: permission-%s failed: %s\n", command, logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout,
		"txharbor reconcile-admin: permission-%s %s target_principal=%s permission=%s scope_hash=%s operation_id=%s recorded=%t principal=%s single_approver=true\n",
		command, result.Outcome, target, permission, result.ScopeHash, operation, result.Recorded, env.principal)
	return 0
}

// reconcileManagementResult reports the applied management mutation.
type reconcileManagementResult struct {
	Outcome   string
	Recorded  bool
	ScopeHash string
}

// reconcileManagementOperation is one recorded management mutation, read back
// for operation_id dedup and replay. ActorPrincipal is the full canonical
// principal recorded in the audit target (never the truncated actor column):
// replay/conflict decisions compare full identities only, and a row without a
// recoverable full identity is conservatively refused, never attributed by
// guess.
type reconcileManagementOperation struct {
	ActorPrincipal   string
	ManagementAction string
	TargetPrincipal  string
	Permission       string
	ScopeHash        string
	Outcome          string
}

// readManagementOperationSQL reads the recorded management change for one
// operation_id (recon_audit is the append-only 014 trail and the only
// 014-owned carrier of management operations). The identity check uses
// target.actor_principal, the untruncated Principal.String(), so two legal
// principals sharing the 128-byte actor truncation can never be conflated; a
// row without it (legacy/foreign shape) is a conservative conflict.
const readManagementOperationSQL = `
SELECT COALESCE(target->>'actor_principal', ''), COALESCE(target->>'management_action', ''),
       COALESCE(target->>'target_principal', ''), COALESCE(target->>'permission', ''),
       COALESCE(target->>'scope_hash', ''), result
FROM recon_audit
WHERE target->>'operation_id' = $1 AND target ? 'management_action'
ORDER BY audit_id DESC
LIMIT 1`

// insertManagementAuditSQL appends one management change record. The frozen
// recon_audit.action CHECK (migration 000016) has no management token, so the
// row uses the nearest existing bucket (grant -> start, revoke -> close) while
// target.management_action carries the authoritative operation; before/after
// state, operator and result are recorded on the same row.
const insertManagementAuditSQL = `
INSERT INTO recon_audit (actor, action, target, reason, evidence, result)
VALUES ($1, $2, $3::jsonb, $4, $5, $6)`

// applyPermissionMutation executes one grant/revoke atomically with its audit
// row. operation_id dedup runs under a transaction-scoped advisory lock (no
// schema carrier exists for management operations): the same input re-reads
// the recorded outcome with zero writes, different input is
// operation_conflict with zero writes (the 011/013 shape). The caller's
// identity is compared on the full target.actor_principal, never on the
// truncated actor column, and the mutation itself only runs after the current
// management authorization has passed (the caller decides that before this
// function).
func (e *reconAdminEnv) applyPermissionMutation(ctx context.Context, kind reconciliation.ManagementKind,
	target reconciliation.Principal, permission reconciliation.Permission, scope reconciliation.AuthScope,
	operator, reason, operation string) (reconcileManagementResult, error) {
	result := reconcileManagementResult{}
	scopeJSON, err := scope.CanonicalJSON()
	if err != nil {
		return result, fmt.Errorf("encode permission scope: %w", err)
	}
	scopeHash, err := scope.Digest()
	if err != nil {
		return result, fmt.Errorf("digest permission scope: %w", err)
	}
	result.ScopeHash = scopeHash
	actor := reconcileAuditActor(e.principal)

	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return result, fmt.Errorf("begin permission management: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	// Serialize equal operation ids across processes; distinct ids only
	// contend on a hash collision and stay correct.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, operation); err != nil {
		return result, fmt.Errorf("lock permission operation: %w", err)
	}

	var recorded reconcileManagementOperation
	err = tx.QueryRow(ctx, readManagementOperationSQL, operation).Scan(
		&recorded.ActorPrincipal, &recorded.ManagementAction, &recorded.TargetPrincipal,
		&recorded.Permission, &recorded.ScopeHash, &recorded.Outcome)
	switch {
	case err == nil:
		if recorded.ActorPrincipal != e.principal.String() || recorded.ManagementAction != string(kind) ||
			recorded.TargetPrincipal != target.String() || recorded.Permission != string(permission) ||
			recorded.ScopeHash != scopeHash {
			return result, errReconcileOperationConflict
		}
		result.Outcome, result.Recorded = recorded.Outcome, true
		return result, nil
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return result, fmt.Errorf("read permission operation record: %w", err)
	}

	beforeState := map[string]any{"present": false}
	afterState := map[string]any{"present": false}
	var outcome string
	switch kind {
	case reconciliation.ManagementGrant:
		beforePresent, beforeBy, beforeAt, err := readPermissionGrant(ctx, tx, target.String(), string(permission), scopeHash)
		if err != nil {
			return result, err
		}
		if beforePresent {
			beforeState = permissionGrantState(beforeBy, beforeAt)
		}
		var grantedAt time.Time
		insertErr := tx.QueryRow(ctx, `
INSERT INTO recon_permission (principal, action, scope, scope_hash, granted_by)
VALUES ($1, $2, $3::jsonb, $4, $5)
ON CONFLICT (principal, action, scope_hash) DO NOTHING
RETURNING granted_at`, target.String(), string(permission), string(scopeJSON), scopeHash, actor).Scan(&grantedAt)
		switch {
		case insertErr == nil:
			outcome = "granted"
			afterState = permissionGrantState(actor, grantedAt)
		case errors.Is(insertErr, pgx.ErrNoRows):
			outcome = "nop"
			// Re-read the recorded row: a concurrent grant may have committed
			// between the pre-read and this conflict, so the after state is
			// never rendered from a stale snapshot.
			present, by, at, err := readPermissionGrant(ctx, tx, target.String(), string(permission), scopeHash)
			if err != nil {
				return result, err
			}
			switch {
			case present:
				afterState = permissionGrantState(by, at)
			case beforePresent:
				afterState = permissionGrantState(beforeBy, beforeAt)
			}
		default:
			return result, fmt.Errorf("record permission grant: %w", insertErr)
		}
	case reconciliation.ManagementRevoke:
		beforePresent, beforeBy, beforeAt, err := readPermissionGrant(ctx, tx, target.String(), string(permission), scopeHash)
		if err != nil {
			return result, err
		}
		if beforePresent {
			beforeState = permissionGrantState(beforeBy, beforeAt)
		}
		tag, err := tx.Exec(ctx, `
DELETE FROM recon_permission
WHERE principal = $1 AND action = $2 AND scope_hash = $3`,
			target.String(), string(permission), scopeHash)
		if err != nil {
			return result, fmt.Errorf("revoke permission: %w", err)
		}
		if tag.RowsAffected() > 0 {
			outcome = "revoked"
		} else {
			outcome = "nop"
			// A concurrent revoke may have removed the row after the pre-read;
			// record the actual after state, never a stale snapshot.
			present, by, at, err := readPermissionGrant(ctx, tx, target.String(), string(permission), scopeHash)
			if err != nil {
				return result, err
			}
			if present {
				afterState = permissionGrantState(by, at)
			}
		}
	default:
		return result, fmt.Errorf("unknown management operation %q", kind)
	}

	action := reconciliation.AuditActionStart
	if kind == reconciliation.ManagementRevoke {
		// The frozen recon_audit.action vocabulary has no management token;
		// block permission changes are the nearest bucket and the target
		// carries the authoritative management_action.
		action = reconciliation.AuditActionClose
	}
	targetDoc, err := json.Marshal(map[string]any{
		"management_action": string(kind),
		"actor_principal":   e.principal.String(),
		"target_principal":  target.String(),
		"permission":        string(permission),
		"scope":             scope,
		"scope_hash":        scopeHash,
		"operation_id":      operation,
		"state_before":      beforeState,
		"state_after":       afterState,
	})
	if err != nil {
		return result, fmt.Errorf("encode permission management audit: %w", err)
	}
	if _, err := tx.Exec(ctx, insertManagementAuditSQL, actor, string(action), string(targetDoc),
		reason, "operator="+operator+" single_approver=true", outcome); err != nil {
		return result, fmt.Errorf("record permission management audit: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit permission management: %w", err)
	}
	result.Outcome = outcome
	return result, nil
}

// readPermissionGrant reads the current state of one grant row (read-only
// inside the caller's transaction).
func readPermissionGrant(ctx context.Context, tx pgx.Tx, principal, action, scopeHash string) (bool, string, time.Time, error) {
	var grantedBy string
	var grantedAt time.Time
	err := tx.QueryRow(ctx, `
SELECT granted_by, granted_at FROM recon_permission
WHERE principal = $1 AND action = $2 AND scope_hash = $3`, principal, action, scopeHash).
		Scan(&grantedBy, &grantedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "", time.Time{}, nil
	}
	if err != nil {
		return false, "", time.Time{}, fmt.Errorf("read permission grant: %w", err)
	}
	return true, grantedBy, grantedAt, nil
}

// permissionGrantState renders the before/after state snapshot of one grant.
func permissionGrantState(grantedBy string, grantedAt time.Time) map[string]any {
	return map[string]any{
		"present":    true,
		"granted_by": grantedBy,
		"granted_at": grantedAt.UTC().Format(time.RFC3339Nano),
	}
}

// reconcileAdminPermissionShow lists 014 grants inside the manager's
// manageable scope. It is read-only: an allowed query writes no audit row and
// refusals are audited by the evaluator.
func reconcileAdminPermissionShow(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("reconcile-admin permission-show", flag.ContinueOnError)
	fs.SetOutput(stderr)
	principalRaw := fs.String("principal", "", "filter by principal <kind>:<id> (optional)")
	permissionRaw := fs.String("permission", "", "filter by 014 permission: scan_manage|exception_handle|dispose_ack|dispose_reuse|close (optional)")
	chainID := fs.String("chain-id", "", "exact scope filter chain identity (optional, with --scope-kind/--business-types)")
	scopeKind := fs.String("scope-kind", "", "exact scope filter dimension: height|time (optional)")
	businessTypes := fs.String("business-types", "", "exact scope filter business types (optional)")
	from := fs.String("from", "", "exact scope filter range start height (optional)")
	to := fs.String("to", "", "exact scope filter range end height (optional)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		reconcileAdminUsage(stderr)
		return 2
	}
	var principalFilter reconciliation.Principal
	if strings.TrimSpace(*principalRaw) != "" {
		principal, err := parseReconcileTargetPrincipal(*principalRaw)
		if err != nil {
			fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
			return 2
		}
		principalFilter = principal
	}
	scopeFilter, err := parseReconcileOptionalManagementScope(*chainID, *scopeKind, *businessTypes, *from, *to)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}
	permissionFilter := reconciliation.Permission(strings.TrimSpace(*permissionRaw))

	env, code := reconcileAdminOpen(ctx, d)
	if env == nil {
		return code
	}
	defer env.pool.Close()

	trust, trustErr := reconcileManagementTrust(env.cfg)
	// Without an explicit scope filter the management request scope is the
	// configured manageable scope itself; with one, the exact requested scope.
	effectiveScope := reconciliation.AuthScope{}
	if scopeFilter != nil {
		effectiveScope = *scopeFilter
	} else if trust != nil {
		effectiveScope = trust.Scope
	}
	decision, err := env.evaluator.AuthorizeManagement(ctx, trust, env.principal, reconciliation.ManagementRequest{
		Kind:            reconciliation.ManagementQuery,
		TargetPrincipal: principalFilter,
		Permission:      permissionFilter,
		Scope:           effectiveScope,
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: authorization failed: %s\n", logx.Redact(err.Error()))
		printReconcileManagementTrustHint(stderr, trustErr)
		return 1
	}
	if !decision.Allowed {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: refused permission-show: %s (%s)\n",
			decision.Reason, decision.Detail)
		printReconcileManagementTrustHint(stderr, trustErr)
		return 1
	}

	shown, excluded, malformed, err := env.listReconPermissions(ctx, principalFilter, permissionFilter,
		scopeFilter, effectiveScope, stdout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: permission-show failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout,
		"txharbor reconcile-admin: permission-show scope=%s shown=%d excluded=%d malformed=%d principal=%s\n",
		reconcileAuthScopeText(effectiveScope), shown, excluded, malformed, env.principal)
	return 0
}

// listReconPermissions lists the grants visible to the manager (read-only):
// rows are filtered by the optional principal/permission filters and then
// conservatively kept only when their stored scope is the exact requested
// scope (explicit filter) or inside the manageable scope (no explicit filter).
// Rows whose stored scope cannot be decoded are counted as malformed and never
// partially trusted; out-of-scope rows are counted but their contents are not
// rendered.
func (e *reconAdminEnv) listReconPermissions(ctx context.Context, principalFilter reconciliation.Principal,
	permissionFilter reconciliation.Permission, scopeFilter *reconciliation.AuthScope,
	effectiveScope reconciliation.AuthScope, stdout io.Writer) (int, int, int, error) {
	principalText, permissionText := "", ""
	if principalFilter.Valid() {
		principalText = principalFilter.String()
	}
	if permissionFilter != "" {
		permissionText = string(permissionFilter)
	}
	filterHash := ""
	if scopeFilter != nil {
		hash, err := scopeFilter.Digest()
		if err != nil {
			return 0, 0, 0, fmt.Errorf("digest scope filter: %w", err)
		}
		filterHash = hash
	}
	rows, err := e.pool.Query(ctx, `
SELECT principal, action, scope, scope_hash, granted_by, granted_at
FROM recon_permission
WHERE ($1 = '' OR principal = $1) AND ($2 = '' OR action = $2)
ORDER BY principal, action, scope_hash`, principalText, permissionText)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("list %s: %w", reconciliation.ReconPermissionTable, err)
	}
	defer rows.Close()

	shown, excluded, malformed := 0, 0, 0
	for rows.Next() {
		var (
			principal, action, scopeHash, grantedBy string
			scopeJSON                               []byte
			grantedAt                               time.Time
		)
		if err := rows.Scan(&principal, &action, &scopeJSON, &scopeHash, &grantedBy, &grantedAt); err != nil {
			return 0, 0, 0, fmt.Errorf("scan %s: %w", reconciliation.ReconPermissionTable, err)
		}
		var rowScope reconciliation.AuthScope
		if err := json.Unmarshal(scopeJSON, &rowScope); err != nil {
			malformed++
			continue
		}
		if err := rowScope.Validate(); err != nil {
			malformed++
			continue
		}
		if scopeFilter != nil {
			if scopeHash != filterHash {
				excluded++
				continue
			}
		} else if !effectiveScope.Covers(rowScope) {
			excluded++
			continue
		}
		fmt.Fprintf(stdout, "grant principal=%s action=%s scope=%s scope_hash=%s granted_by=%s granted_at=%s\n",
			principal, action, reconcileAuthScopeText(rowScope), scopeHash,
			grantedBy, grantedAt.UTC().Format(time.RFC3339))
		shown++
	}
	if err := rows.Err(); err != nil {
		return 0, 0, 0, fmt.Errorf("iterate %s: %w", reconciliation.ReconPermissionTable, err)
	}
	return shown, excluded, malformed, nil
}
