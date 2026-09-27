//go:build integration

// us2admin_integration_test.go is the T023 US2 acceptance layer for the
// `reconcile-admin claim` replay path (FR-011/012; contracts/auth-matrix.md;
// Q2) over the real 000016 schema:
//
//   - first claim success; an authorized same-operation/same-input replay
//     succeeds with zero writes (audit/discrepancy/disposition counts
//     unchanged);
//   - a revoked exception_handle refuses the replay: non-zero exit, no
//     replay=true and no from=/to=/owner= content, one new no_grant refusal
//     audit, ticket and recorded claim row unchanged;
//   - the same operation with another ticket or another principal is
//     operation_conflict with zero writes and the target ticket untouched;
//   - same-128-byte-prefix principals (multi-byte ids) never conflate: the
//     actor-column truncation collides while the full target.owner comparison
//     refuses; a claim audit row without a recoverable target.owner is a
//     conservative conflict, never attributed by guess;
//   - refusals never carry the operation suffix and, after restoring the
//     grant, the replay hits the single original claim row;
//   - concurrent claims through the real CLI keep the single-owner CAS:
//     exactly one winner.
//
// Every case drives the real Run entry (principal from
// TXHARBOR_RECON_PRINCIPAL, T022 Store/ClaimDiscrepancy and the T009
// evaluator behind it). PostgreSQL comes from testcontainers. When no Docker
// provider is healthy the package reports NOT RUN (t.Skip), never a pass.
package reconcileadmin

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/reconciliation"
)

const (
	us2ITRangeFrom = int64(100)
	us2ITRangeTo   = int64(200)
	us2ITOperator  = "it-operator"
)

// us2ITInt64 points at v for the *int64 AuthScope range API.
func us2ITInt64(v int64) *int64 { return &v }

// us2ITAuthScope is the concrete ticket/grant scope shared by every T023 case.
func us2ITAuthScope() reconciliation.AuthScope {
	return reconciliation.AuthScope{
		ChainID: recAdminChainID, Kind: reconciliation.ScopeHeight,
		BusinessTypes: []reconciliation.BusinessType{reconciliation.BusinessWithdrawal},
		RangeStart:    us2ITInt64(us2ITRangeFrom), RangeEnd: us2ITInt64(us2ITRangeTo),
	}
}

// us2ITIdentityScope mirrors us2ITAuthScope in identity form: the persisted
// scope marker the production authorization seam parses.
func us2ITIdentityScope() reconciliation.IdentityScope {
	return reconciliation.IdentityScope{
		ChainID: recAdminChainID, Kind: reconciliation.ScopeHeight,
		From: us2ITRangeFrom, To: us2ITRangeTo,
		BusinessTypes: []reconciliation.BusinessType{reconciliation.BusinessWithdrawal},
	}
}

// us2ITEnv returns a valid command environment bound to one principal.
func us2ITEnv(dsn, principal string) map[string]string {
	env := recAdminEnv(dsn, nil, 0)
	env[config.EnvReconPrincipal] = principal
	return env
}

// us2ITTrustEnv additionally configures the single-manager deployment trust
// root so the real permission-grant/permission-revoke path can run.
func us2ITTrustEnv(dsn, manager string) map[string]string {
	env := us2ITEnv(dsn, manager)
	env[config.EnvReconManagementTrust] = fmt.Sprintf(
		`{"principal":%q,"scope":{"chain_id":%q,"kind":"height","business_types":["withdrawal"],"range_start":%d,"range_end":%d}}`,
		manager, recAdminChainID, us2ITRangeFrom, us2ITRangeTo)
	return env
}

// us2ITSeedDiscrepancy inserts one ticket carrying the real scope marker the
// T022 authorization seam reads from evidence_version_domain.
func us2ITSeedDiscrepancy(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	id string, state reconciliation.DiscrepancyState, claimOwner string) {
	t.Helper()
	if !state.Valid() {
		t.Fatalf("seed discrepancy with unknown state %q", state)
	}
	var owner *string
	var claimedAt *time.Time
	if state == reconciliation.DiscrepancyStateClaimed || state == reconciliation.DiscrepancyStateDisposing {
		if strings.TrimSpace(claimOwner) == "" {
			t.Fatalf("state %s requires a claim owner", state)
		}
		owner = &claimOwner
		at := time.Now().UTC()
		claimedAt = &at
	}
	scope := us2ITIdentityScope()
	domain, err := reconciliation.PersistedEvidenceDomainJSON(reconciliation.VersionDomain{
		BlockNumber: 101, BlockHash: "0xus2admin", EvidenceAt: time.Now().UTC(),
	}, &scope)
	if err != nil {
		t.Fatalf("persist evidence domain of %s: %v", id, err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO discrepancy (
		    discrepancy_id, category, business_key, content_hash, evidence_version_domain,
		    state, claim_owner, claimed_at)
		VALUES ($1::uuid, 'missing', $2, $3, $4::jsonb, $5, $6, $7)`,
		id, "us2-t023-"+id, []byte{0x02, 0x03}, string(domain),
		string(state), owner, claimedAt); err != nil {
		t.Fatalf("seed discrepancy %s: %v", id, err)
	}
}

// us2ITGrant inserts one exact-scope grant row (idempotent).
func us2ITGrant(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	principal string, permission reconciliation.Permission, scope reconciliation.AuthScope) {
	t.Helper()
	if err := scope.Validate(); err != nil {
		t.Fatalf("grant scope invalid: %v", err)
	}
	canonical, err := scope.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical grant scope: %v", err)
	}
	digest, err := scope.Digest()
	if err != nil {
		t.Fatalf("grant scope digest: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recon_permission (principal, action, scope, scope_hash, granted_by)
		VALUES ($1, $2, $3::jsonb, $4, 'it-grantor')
		ON CONFLICT (principal, action, scope_hash) DO NOTHING`,
		principal, string(permission), string(canonical), digest); err != nil {
		t.Fatalf("grant %s/%s: %v", principal, permission, err)
	}
}

// us2ITState reads the observable ticket lifecycle row.
func us2ITState(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	id string) (reconciliation.DiscrepancyState, string, int64) {
	t.Helper()
	var state, owner string
	var reopen int64
	if err := pool.QueryRow(ctx, `
		SELECT state, COALESCE(claim_owner, ''), reopen_count::bigint
		FROM discrepancy
		WHERE discrepancy_id = $1::uuid`, id).Scan(&state, &owner, &reopen); err != nil {
		t.Fatalf("read discrepancy %s: %v", id, err)
	}
	return reconciliation.DiscrepancyState(state), owner, reopen
}

// us2ITCount runs one scalar count query.
func us2ITCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

// us2ITOperationSuffix is the exact claim-audit reason carriage of one
// operation id (the suffix T022 appends to the recorded claim reason).
func us2ITOperationSuffix(operation string) string {
	return reconcileClaimOperationSuffix + operation
}

// us2ITAuditRows counts the audit rows of one action whose reason carries the
// operation suffix: only a committed operation ever matches.
func us2ITAuditRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, action, operation string) int64 {
	t.Helper()
	return us2ITCount(t, ctx, pool, `
		SELECT count(*)::bigint FROM recon_audit
		WHERE action = $1 AND right(reason, length($2)) = $2`,
		action, us2ITOperationSuffix(operation))
}

// us2ITSuffixRows counts every audit row (any action) carrying the suffix.
func us2ITSuffixRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, operation string) int64 {
	t.Helper()
	return us2ITCount(t, ctx, pool, `
		SELECT count(*)::bigint FROM recon_audit
		WHERE right(reason, length($1)) = $1`, us2ITOperationSuffix(operation))
}

// us2ITClaimArgs renders the real claim invocation for one ticket/operation.
func us2ITClaimArgs(id, operation string) []string {
	return []string{"claim",
		"--discrepancy-id", id,
		"--operator", us2ITOperator,
		"--reason", "it claim",
		"--operation-id", operation}
}

// us2ITPermissionArgs renders one permission-grant/revoke invocation for the
// shared ticket scope.
func us2ITPermissionArgs(command, target string, operation string) []string {
	return []string{command,
		"--target-principal", target,
		"--permission", string(reconciliation.PermissionExceptionHandle),
		"--chain-id", recAdminChainID,
		"--scope-kind", "height",
		"--business-types", "withdrawal",
		"--from", fmt.Sprintf("%d", us2ITRangeFrom),
		"--to", fmt.Sprintf("%d", us2ITRangeTo),
		"--operation-id", operation,
		"--operator", "it-manager",
		"--reason", "it " + command}
}

func TestIntegrationReconcileAdminClaimReplayAcceptance(t *testing.T) {
	ctx := context.Background()
	pool, dsn := recAdminPG(t)
	scope := us2ITAuthScope()
	env := us2ITEnv(dsn, recAdminPrincipal)

	t.Run("first_claim_succeeds_and_authorized_replay_is_zero_write", func(t *testing.T) {
		id := uuid.NewString()
		us2ITSeedDiscrepancy(t, ctx, pool, id, reconciliation.DiscrepancyStateOpenClaimable, "")
		us2ITGrant(t, ctx, pool, recAdminPrincipal, reconciliation.PermissionExceptionHandle, scope)
		operation := "it-replay-" + uuid.NewString()
		args := us2ITClaimArgs(id, operation)

		code, stdout, stderr := recAdminRun(ctx, env, args...)
		if code != 0 {
			t.Fatalf("first claim exit = %d, stderr=%q", code, stderr)
		}
		for field, want := range map[string]string{
			"replay": "false", "from": "open_claimable", "to": "claimed",
			"owner": recAdminPrincipal, "principal": recAdminPrincipal,
			"operation_id": operation, "disposal_right": "false",
		} {
			if got := recAdminField(t, stdout, field); got != want {
				t.Fatalf("first claim %s = %q, want %q (stdout %q)", field, got, want, stdout)
			}
		}
		if state, owner, _ := us2ITState(t, ctx, pool, id); state != reconciliation.DiscrepancyStateClaimed || owner != recAdminPrincipal {
			t.Fatalf("persisted claim = %s/%q, want claimed/%s", state, owner, recAdminPrincipal)
		}
		if n := us2ITAuditRows(t, ctx, pool, "claim", operation); n != 1 {
			t.Fatalf("claim audit rows carrying the operation = %d, want the single recorded row", n)
		}

		auditBefore := recAdminCount(t, ctx, pool, "recon_audit")
		discrepancyBefore := recAdminCount(t, ctx, pool, "discrepancy")
		dispositionBefore := recAdminCount(t, ctx, pool, "disposition")

		// The authorized same-operation replay converges on the recorded row
		// with zero writes.
		code, stdout, stderr = recAdminRun(ctx, env, args...)
		if code != 0 {
			t.Fatalf("authorized replay exit = %d, stderr=%q", code, stderr)
		}
		for field, want := range map[string]string{
			"replay": "true", "from": "open_claimable", "to": "claimed",
			"owner": recAdminPrincipal, "operation_id": operation,
		} {
			if got := recAdminField(t, stdout, field); got != want {
				t.Fatalf("authorized replay %s = %q, want %q (stdout %q)", field, got, want, stdout)
			}
		}
		if n := recAdminCount(t, ctx, pool, "recon_audit"); n != auditBefore {
			t.Fatalf("audit rows after the authorized replay = %d, want %d (zero writes)", n, auditBefore)
		}
		if n := recAdminCount(t, ctx, pool, "discrepancy"); n != discrepancyBefore {
			t.Fatalf("discrepancy rows after the authorized replay = %d, want %d (zero writes)", n, discrepancyBefore)
		}
		if n := recAdminCount(t, ctx, pool, "disposition"); n != dispositionBefore {
			t.Fatalf("disposition rows after the authorized replay = %d, want %d (zero writes)", n, dispositionBefore)
		}
		if n := us2ITAuditRows(t, ctx, pool, "claim", operation); n != 1 {
			t.Fatalf("claim audit rows after the replay = %d, want the single recorded row", n)
		}
	})

	t.Run("revoked_permission_refuses_replay_without_content_and_audits", func(t *testing.T) {
		id := uuid.NewString()
		us2ITSeedDiscrepancy(t, ctx, pool, id, reconciliation.DiscrepancyStateOpenClaimable, "")
		us2ITGrant(t, ctx, pool, recAdminPrincipal, reconciliation.PermissionExceptionHandle, scope)
		operation := "it-revoked-" + uuid.NewString()
		args := us2ITClaimArgs(id, operation)
		if code, _, stderr := recAdminRun(ctx, env, args...); code != 0 {
			t.Fatalf("first claim exit = %d, stderr=%q", code, stderr)
		}

		// Revoke through the real permission-revoke path. The management
		// dedup identity carrier is target.actor_principal; the dedup replay
		// below reads the recorded operation back with zero writes.
		trustEnv := us2ITTrustEnv(dsn, recAdminPrincipal)
		revokeOperation := "it-revoke-" + uuid.NewString()
		revokeArgs := us2ITPermissionArgs("permission-revoke", recAdminPrincipal, revokeOperation)
		code, stdout, stderr := recAdminRun(ctx, trustEnv, revokeArgs...)
		if code != 0 {
			t.Fatalf("permission-revoke exit = %d, stderr=%q", code, stderr)
		}
		if got := recAdminField(t, stdout, "recorded"); got != "false" {
			t.Fatalf("first permission-revoke recorded = %q, want false (a real mutation)", got)
		}
		if n := us2ITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM recon_permission
			WHERE principal = $1 AND action = $2`,
			recAdminPrincipal, string(reconciliation.PermissionExceptionHandle)); n != 0 {
			t.Fatalf("grant rows after the revoke = %d, want 0", n)
		}
		// Same operation, same input: the management dedup replay converges
		// with zero writes.
		auditBeforeRevokeReplay := recAdminCount(t, ctx, pool, "recon_audit")
		code, stdout, stderr = recAdminRun(ctx, trustEnv, revokeArgs...)
		if code != 0 {
			t.Fatalf("permission-revoke dedup exit = %d, stderr=%q", code, stderr)
		}
		if got := recAdminField(t, stdout, "recorded"); got != "true" {
			t.Fatalf("dedup permission-revoke recorded = %q, want true (read-back)", got)
		}
		if n := recAdminCount(t, ctx, pool, "recon_audit"); n != auditBeforeRevokeReplay {
			t.Fatalf("audit rows after the dedup revoke = %d, want %d (zero writes)", n, auditBeforeRevokeReplay)
		}

		refuseBefore := us2ITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM recon_audit
			WHERE action = 'refuse' AND actor = $1 AND target->>'action' = 'claim'`, recAdminPrincipal)
		auditBefore := recAdminCount(t, ctx, pool, "recon_audit")

		code, stdout, stderr = recAdminRun(ctx, env, args...)
		if code != 1 {
			t.Fatalf("revoked replay exit = %d, want 1 (stderr=%q)", code, stderr)
		}
		if strings.Contains(stdout, "replay=true") || strings.Contains(stdout, "claim discrepancy_id=") {
			t.Fatalf("refused replay stdout %q reports a claim/replay", stdout)
		}
		for _, field := range []string{"from=", "to=", "owner="} {
			if strings.Contains(stdout, field) {
				t.Fatalf("refused replay stdout %q leaks %s", stdout, field)
			}
		}
		if !strings.Contains(stderr, "claim replay refused") || !strings.Contains(stderr, string(reconciliation.DenyNoGrant)) {
			t.Fatalf("refused replay stderr = %q, want the no_grant replay refusal", stderr)
		}
		refuseAfter := us2ITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM recon_audit
			WHERE action = 'refuse' AND actor = $1 AND target->>'action' = 'claim'`, recAdminPrincipal)
		if refuseAfter != refuseBefore+1 {
			t.Fatalf("claim refusal audits = %d, want %d (one new refusal)", refuseAfter, refuseBefore+1)
		}
		if n := us2ITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM recon_audit
			WHERE action = 'refuse' AND actor = $1 AND target->>'action' = 'claim'
			  AND target->>'permission' = $2 AND reason = $3`,
			recAdminPrincipal, string(reconciliation.PermissionExceptionHandle),
			string(reconciliation.DenyNoGrant)); n != 1 {
			t.Fatalf("no_grant claim refusal rows = %d, want 1", n)
		}
		if n := recAdminCount(t, ctx, pool, "recon_audit"); n != auditBefore+1 {
			t.Fatalf("audit rows after the refused replay = %d, want %d (only the refusal)", n, auditBefore+1)
		}
		if state, owner, _ := us2ITState(t, ctx, pool, id); state != reconciliation.DiscrepancyStateClaimed || owner != recAdminPrincipal {
			t.Fatalf("ticket after the refused replay = %s/%q, want claimed/%s", state, owner, recAdminPrincipal)
		}
		if n := us2ITAuditRows(t, ctx, pool, "claim", operation); n != 1 {
			t.Fatalf("claim audit rows after the refused replay = %d, want the single recorded row", n)
		}
	})

	t.Run("same_operation_with_another_ticket_or_principal_is_operation_conflict", func(t *testing.T) {
		us2ITGrant(t, ctx, pool, recAdminPrincipal, reconciliation.PermissionExceptionHandle, scope)
		idA := uuid.NewString()
		us2ITSeedDiscrepancy(t, ctx, pool, idA, reconciliation.DiscrepancyStateOpenClaimable, "")
		operation := "it-conflict-" + uuid.NewString()
		code, stdout, stderr := recAdminRun(ctx, env, us2ITClaimArgs(idA, operation)...)
		if code != 0 {
			t.Fatalf("first claim exit = %d, stderr=%q", code, stderr)
		}
		if got := recAdminField(t, stdout, "replay"); got != "false" {
			t.Fatalf("first claim replay = %q, want false", got)
		}
		stateA, ownerA, _ := us2ITState(t, ctx, pool, idA)

		// (a) Another principal reuses the operation: conflict before any
		// authorization/write, target ticket untouched.
		auditBefore := recAdminCount(t, ctx, pool, "recon_audit")
		code, stdout, stderr = recAdminRun(ctx, us2ITEnv(dsn, "deploy:it-other"), us2ITClaimArgs(idA, operation)...)
		if code != 1 {
			t.Fatalf("other-principal conflict exit = %d, want 1 (stderr=%q)", code, stderr)
		}
		if strings.Contains(stdout, "replay=true") {
			t.Fatalf("other-principal conflict stdout %q reports replay=true", stdout)
		}
		if !strings.Contains(stderr, "operation_conflict") || !strings.Contains(stderr, "another principal") {
			t.Fatalf("other-principal conflict stderr = %q, want an operation_conflict naming the principal", stderr)
		}
		if n := recAdminCount(t, ctx, pool, "recon_audit"); n != auditBefore {
			t.Fatalf("audit rows after the principal conflict = %d, want %d (zero writes)", n, auditBefore)
		}
		if state, owner, _ := us2ITState(t, ctx, pool, idA); state != stateA || owner != ownerA {
			t.Fatalf("ticket after the principal conflict = %s/%q, want %s/%q", state, owner, stateA, ownerA)
		}
		if n := us2ITAuditRows(t, ctx, pool, "claim", operation); n != 1 {
			t.Fatalf("claim audit rows after the principal conflict = %d, want the single recorded row", n)
		}

		// (b) The same principal reuses the operation on another ticket:
		// conflict, and the target ticket stays open/unclaimed.
		idB := uuid.NewString()
		us2ITSeedDiscrepancy(t, ctx, pool, idB, reconciliation.DiscrepancyStateOpenClaimable, "")
		auditBefore = recAdminCount(t, ctx, pool, "recon_audit")
		code, stdout, stderr = recAdminRun(ctx, env, us2ITClaimArgs(idB, operation)...)
		if code != 1 {
			t.Fatalf("other-ticket conflict exit = %d, want 1 (stderr=%q)", code, stderr)
		}
		if strings.Contains(stdout, "replay=true") {
			t.Fatalf("other-ticket conflict stdout %q reports replay=true", stdout)
		}
		if !strings.Contains(stderr, "operation_conflict") || !strings.Contains(stderr, idA) {
			t.Fatalf("other-ticket conflict stderr = %q, want an operation_conflict naming the recorded ticket", stderr)
		}
		if n := recAdminCount(t, ctx, pool, "recon_audit"); n != auditBefore {
			t.Fatalf("audit rows after the ticket conflict = %d, want %d (zero writes)", n, auditBefore)
		}
		if state, owner, _ := us2ITState(t, ctx, pool, idB); state != reconciliation.DiscrepancyStateOpenClaimable || owner != "" {
			t.Fatalf("target ticket after the ticket conflict = %s/%q, want open_claimable/<none>", state, owner)
		}
		if n := us2ITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM recon_audit
			WHERE action = 'claim' AND target->>'discrepancy_id' = $1`, idB); n != 0 {
			t.Fatalf("claim audit rows of the untouched ticket = %d, want 0", n)
		}
		if state, owner, _ := us2ITState(t, ctx, pool, idA); state != stateA || owner != ownerA {
			t.Fatalf("recorded ticket changed: %s/%q, want %s/%q", state, owner, stateA, ownerA)
		}
	})

	t.Run("same_prefix_principals_and_missing_owner_are_conservative_conflicts", func(t *testing.T) {
		// 128 bytes of valid UTF-8: 60 two-byte runes plus the separating
		// byte; the full identities differ only past the truncation boundary.
		prefix := "deploy:" + strings.Repeat("é", 60) + "x"
		if len(prefix) != 128 {
			t.Fatalf("premise: prefix is %d bytes, want exactly 128", len(prefix))
		}
		p1, p2 := prefix+"A", prefix+"B"
		bound1, err := reconciliation.ConfigPrincipal(p1)
		if err != nil {
			t.Fatalf("ConfigPrincipal(p1): %v", err)
		}
		bound2, err := reconciliation.ConfigPrincipal(p2)
		if err != nil {
			t.Fatalf("ConfigPrincipal(p2): %v", err)
		}
		if bound1.String() == bound2.String() || len(bound1.String()) <= 128 {
			t.Fatalf("premise: full identities must differ past the 128-byte truncation")
		}
		if reconcileAuditActor(bound1) != reconcileAuditActor(bound2) {
			t.Fatalf("premise: the 128-byte actor truncation must collide")
		}
		envP1, envP2 := us2ITEnv(dsn, p1), us2ITEnv(dsn, p2)

		id := uuid.NewString()
		us2ITSeedDiscrepancy(t, ctx, pool, id, reconciliation.DiscrepancyStateOpenClaimable, "")
		us2ITGrant(t, ctx, pool, p1, reconciliation.PermissionExceptionHandle, scope)
		operation := "it-prefix-" + uuid.NewString()
		code, stdout, stderr := recAdminRun(ctx, envP1, us2ITClaimArgs(id, operation)...)
		if code != 0 {
			t.Fatalf("first claim by p1 exit = %d, stderr=%q", code, stderr)
		}
		if got := recAdminField(t, stdout, "owner"); got != p1 {
			t.Fatalf("first claim owner = %q, want the full identity %q", got, p1)
		}
		var actor, recordedOwner string
		if err := pool.QueryRow(ctx, `
			SELECT actor, COALESCE(target->>'owner', '') FROM recon_audit
			WHERE action = 'claim' AND right(reason, length($1)) = $1`,
			us2ITOperationSuffix(operation)).Scan(&actor, &recordedOwner); err != nil {
			t.Fatalf("read the recorded claim row: %v", err)
		}
		if actor != prefix {
			t.Fatalf("claim audit actor = %q (%d bytes), want the 128-byte truncation %q", actor, len(actor), prefix)
		}
		if recordedOwner != p1 {
			t.Fatalf("claim audit target.owner = %q, want the full untruncated identity %q", recordedOwner, p1)
		}

		// P2 shares the truncated actor column with P1 but is a different
		// principal: the full-identity comparison refuses, never replays.
		auditBefore := recAdminCount(t, ctx, pool, "recon_audit")
		code, stdout, stderr = recAdminRun(ctx, envP2, us2ITClaimArgs(id, operation)...)
		if code != 1 {
			t.Fatalf("same-prefix replay exit = %d, want 1 (stderr=%q)", code, stderr)
		}
		if strings.Contains(stdout, "replay=true") {
			t.Fatalf("same-prefix replay stdout %q reports replay=true", stdout)
		}
		if !strings.Contains(stderr, "operation_conflict") || !strings.Contains(stderr, "another principal") {
			t.Fatalf("same-prefix replay stderr = %q, want an operation_conflict naming the principal", stderr)
		}
		if n := recAdminCount(t, ctx, pool, "recon_audit"); n != auditBefore {
			t.Fatalf("audit rows after the same-prefix conflict = %d, want %d (zero writes)", n, auditBefore)
		}
		if state, owner, _ := us2ITState(t, ctx, pool, id); state != reconciliation.DiscrepancyStateClaimed || owner != p1 {
			t.Fatalf("ticket after the same-prefix conflict = %s/%q, want claimed/%q", state, owner, p1)
		}

		// A claim audit row without a recoverable target.owner (legacy/foreign
		// shape) is a conservative conflict: ownership is never guessed from
		// the truncated actor column.
		idLegacy := uuid.NewString()
		us2ITSeedDiscrepancy(t, ctx, pool, idLegacy, reconciliation.DiscrepancyStateOpenClaimable, "")
		legacyOperation := "it-legacy-" + uuid.NewString()
		if _, err := pool.Exec(ctx, `
			INSERT INTO recon_audit (actor, action, target, reason, evidence, result)
			VALUES ($1, 'claim', $2::jsonb, $3, '', 'claimed')`,
			reconcileAuditActor(bound2), fmt.Sprintf(`{"discrepancy_id":%q}`, idLegacy),
			"legacy claim"+us2ITOperationSuffix(legacyOperation)); err != nil {
			t.Fatalf("seed the legacy claim row: %v", err)
		}
		auditBefore = recAdminCount(t, ctx, pool, "recon_audit")
		code, stdout, stderr = recAdminRun(ctx, envP2, us2ITClaimArgs(idLegacy, legacyOperation)...)
		if code != 1 {
			t.Fatalf("legacy-owner conflict exit = %d, want 1 (stderr=%q)", code, stderr)
		}
		if strings.Contains(stdout, "replay=true") {
			t.Fatalf("legacy-owner conflict stdout %q reports replay=true", stdout)
		}
		if !strings.Contains(stderr, "operation_conflict") || !strings.Contains(stderr, "no recoverable actor identity") {
			t.Fatalf("legacy-owner conflict stderr = %q, want a conservative operation_conflict", stderr)
		}
		if n := recAdminCount(t, ctx, pool, "recon_audit"); n != auditBefore {
			t.Fatalf("audit rows after the legacy-owner conflict = %d, want %d (zero writes)", n, auditBefore)
		}
		if state, owner, _ := us2ITState(t, ctx, pool, idLegacy); state != reconciliation.DiscrepancyStateOpenClaimable || owner != "" {
			t.Fatalf("ticket after the legacy-owner conflict = %s/%q, want open_claimable/<none>", state, owner)
		}
		if n := us2ITSuffixRows(t, ctx, pool, legacyOperation); n != 1 {
			t.Fatalf("operation suffix rows after the legacy-owner conflict = %d, want the single seeded row", n)
		}
	})

	t.Run("refusals_never_carry_the_suffix_and_replay_hits_the_original_row", func(t *testing.T) {
		us2ITGrant(t, ctx, pool, recAdminPrincipal, reconciliation.PermissionExceptionHandle, scope)
		id := uuid.NewString()
		us2ITSeedDiscrepancy(t, ctx, pool, id, reconciliation.DiscrepancyStateOpenClaimable, "")
		operation := "it-suffix-" + uuid.NewString()
		args := us2ITClaimArgs(id, operation)

		code, stdout, stderr := recAdminRun(ctx, env, args...)
		if code != 0 {
			t.Fatalf("first claim exit = %d, stderr=%q", code, stderr)
		}
		from, to, owner := recAdminField(t, stdout, "from"), recAdminField(t, stdout, "to"), recAdminField(t, stdout, "owner")

		// Withdraw the grant directly (the real CLI revoke is covered above):
		// the replay is refused and no refusal audit ever carries the suffix.
		if _, err := pool.Exec(ctx, `
			DELETE FROM recon_permission WHERE principal = $1 AND action = $2`,
			recAdminPrincipal, string(reconciliation.PermissionExceptionHandle)); err != nil {
			t.Fatalf("remove the grant: %v", err)
		}
		code, _, stderr = recAdminRun(ctx, env, args...)
		if code != 1 {
			t.Fatalf("replay after the grant removal exit = %d, want 1 (stderr=%q)", code, stderr)
		}
		if n := us2ITAuditRows(t, ctx, pool, "refuse", operation); n != 0 {
			t.Fatalf("refusal audit rows carrying the operation suffix = %d, want 0", n)
		}

		// Restore the grant: the replay hits the single original claim row.
		us2ITGrant(t, ctx, pool, recAdminPrincipal, reconciliation.PermissionExceptionHandle, scope)
		auditBefore := recAdminCount(t, ctx, pool, "recon_audit")
		code, stdout, stderr = recAdminRun(ctx, env, args...)
		if code != 0 {
			t.Fatalf("restored replay exit = %d, stderr=%q", code, stderr)
		}
		for field, want := range map[string]string{
			"replay": "true", "from": from, "to": to, "owner": owner,
		} {
			if got := recAdminField(t, stdout, field); got != want {
				t.Fatalf("restored replay %s = %q, want the original recorded %q (stdout %q)", field, got, want, stdout)
			}
		}
		if n := recAdminCount(t, ctx, pool, "recon_audit"); n != auditBefore {
			t.Fatalf("audit rows after the restored replay = %d, want %d (zero writes)", n, auditBefore)
		}
		if n := us2ITAuditRows(t, ctx, pool, "claim", operation); n != 1 {
			t.Fatalf("claim audit rows after the restored replay = %d, want the single original row", n)
		}
		if n := us2ITSuffixRows(t, ctx, pool, operation); n != 1 {
			t.Fatalf("audit rows carrying the operation suffix = %d, want the single original claim row", n)
		}
	})

	t.Run("concurrent_claims_produce_exactly_one_owner", func(t *testing.T) {
		us2ITGrant(t, ctx, pool, recAdminPrincipal, reconciliation.PermissionExceptionHandle, scope)
		id := uuid.NewString()
		us2ITSeedDiscrepancy(t, ctx, pool, id, reconciliation.DiscrepancyStateOpenClaimable, "")

		type claimOutcome struct {
			code   int
			stdout string
			stderr string
		}
		operations := []string{"it-conc-a-" + uuid.NewString(), "it-conc-b-" + uuid.NewString()}
		start := make(chan struct{})
		outcomes := make(chan claimOutcome, len(operations))
		var wg sync.WaitGroup
		for _, operation := range operations {
			wg.Add(1)
			go func(operation string) {
				defer wg.Done()
				<-start
				code, stdout, stderr := recAdminRun(ctx, env, us2ITClaimArgs(id, operation)...)
				outcomes <- claimOutcome{code: code, stdout: stdout, stderr: stderr}
			}(operation)
		}
		close(start)
		wg.Wait()
		close(outcomes)

		var winners int
		for out := range outcomes {
			switch out.code {
			case 0:
				winners++
				if got := recAdminField(t, out.stdout, "replay"); got != "false" {
					t.Fatalf("concurrent winner replay = %q, want false (stdout %q)", got, out.stdout)
				}
				if got := recAdminField(t, out.stdout, "owner"); got != recAdminPrincipal {
					t.Fatalf("concurrent winner owner = %q, want %q", got, recAdminPrincipal)
				}
			case 1:
				if !strings.Contains(out.stderr, "already claimed") {
					t.Fatalf("concurrent loser stderr = %q, want the single-owner CAS refusal", out.stderr)
				}
			default:
				t.Fatalf("unexpected concurrent claim exit = %d (stdout=%q stderr=%q)", out.code, out.stdout, out.stderr)
			}
		}
		if winners != 1 {
			t.Fatalf("concurrent winners = %d, want exactly one", winners)
		}
		if state, owner, _ := us2ITState(t, ctx, pool, id); state != reconciliation.DiscrepancyStateClaimed || owner != recAdminPrincipal {
			t.Fatalf("persisted concurrent claim = %s/%q, want claimed/%s", state, owner, recAdminPrincipal)
		}
		if n := us2ITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM recon_audit
			WHERE action = 'claim' AND target->>'discrepancy_id' = $1`, id); n != 1 {
			t.Fatalf("claim audit rows after the concurrent claims = %d, want exactly one", n)
		}
	})
}
