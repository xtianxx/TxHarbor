//go:build integration

// lifecycle_integration_test.go is the T020 integration layer for US2
// (quickstart §2/5/6/9; contracts/discrepancy-lifecycle.md Transitions;
// contracts/auth-matrix.md; data-model.md §1.4–1.10/§2–§3):
//
//   - claim mutual exclusion: after A holds the single-owner CAS claim, a
//     competing claim (sequential or concurrent) is refused and audited with
//     the observed ownership, and can never overwrite the owner;
//   - dispose idempotency: a repeated dispose with the same idempotency_key
//     converges on the recorded row through the UNIQUE read-back and produces
//     zero side effects;
//   - claim is ownership only: an exception_handle grant authorizes the claim
//     and nothing else (no dispose, no close, no execution right);
//   - missing permission, cross-scope requests and forged identities are
//     refused and audited, including the no-self-grant rule for ordinary 014
//     holders;
//   - a close is refused without a fresh consistent reverify (unverified),
//     with expired evidence, and when the evidence verdict cannot be
//     consistent because scan coverage still has an open gap.
//
// T022 (the production claim/dispose paths) is wired: the claim cases drive
// ClaimDiscrepancy (single-owner CAS; a loser gets ErrDiscrepancyClaimTaken
// with the observed ownership hint) and the dispose cases drive
// DisposeDiscrepancy (UNIQUE idempotency_key + guarded disposing ->
// pending_verify hand-over), each behind the T009 DiscrepancyAuthorizer seam.
// The assertions pin the observable behavior: zero side effects on a replay,
// refusal audits, and unchanged state. The store paths claim no permission
// check inside the T007 transition itself; authorization runs before the
// write in T022.
//
// PostgreSQL comes from testcontainers through the shared reconIT /
// startReconPostgres helpers of scan_integration_test.go. When no Docker
// provider is healthy the package reports NOT RUN (t.Skip), never a pass.
package reconciliation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// lifecycleITInt64 points at v for the *int64 AuthScope range API.
func lifecycleITInt64(v int64) *int64 { return &v }

// lifecycleITAuthScope is the concrete authorization scope every IT ticket
// records in its evidence_version_domain and every seeded grant mirrors, so
// the T022 authorization seam derives exactly the grant-matching scope.
func lifecycleITAuthScope() AuthScope {
	return AuthScope{
		ChainID: reconITChainID, Kind: ScopeHeight,
		BusinessTypes: []BusinessType{BusinessWithdrawal},
		RangeStart:    lifecycleITInt64(100), RangeEnd: lifecycleITInt64(200),
	}
}

// lifecycleITIdentityScope is lifecycleITAuthScope in identity form: the
// persisted scope marker T022 parses to derive the authorization scope.
func lifecycleITIdentityScope() IdentityScope {
	return IdentityScope{
		ChainID: reconITChainID, Kind: ScopeHeight, From: 100, To: 200,
		BusinessTypes: []BusinessType{BusinessWithdrawal},
	}
}

// lifecycleITPrincipal binds one API-key caller id into a 014 principal.
func lifecycleITPrincipal(t *testing.T, id int64) Principal {
	t.Helper()
	p, err := APIKeyPrincipal(id)
	if err != nil {
		t.Fatalf("APIKeyPrincipal(%d): %v", id, err)
	}
	return p
}

// lifecycleITSeedDiscrepancy inserts one discrepancy in the requested state,
// carrying the real scope marker T022 reads from evidence_version_domain.
// claimed/disposing require an owner and a claim time
// (discrepancy_claim_states_check).
func lifecycleITSeedDiscrepancy(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	id string, state DiscrepancyState, claimOwner string) {
	t.Helper()
	if !state.Valid() {
		t.Fatalf("seed discrepancy with unknown state %q", state)
	}
	var owner *string
	var claimedAt *time.Time
	if state == DiscrepancyStateClaimed || state == DiscrepancyStateDisposing {
		if strings.TrimSpace(claimOwner) == "" {
			t.Fatalf("state %s requires a claim owner", state)
		}
		owner = &claimOwner
		at := time.Now().UTC()
		claimedAt = &at
	}
	scope := lifecycleITIdentityScope()
	domain, err := PersistedEvidenceDomainJSON(VersionDomain{
		BlockNumber: 101, BlockHash: "0xabc", EvidenceAt: time.Now().UTC(),
	}, &scope)
	if err != nil {
		t.Fatalf("persist evidence domain of %s: %v", id, err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO discrepancy (
		    discrepancy_id, category, business_key, content_hash, evidence_version_domain,
		    state, claim_owner, claimed_at)
		VALUES ($1::uuid, 'missing', $2, $3, $4::jsonb, $5, $6, $7)`,
		id, "lifecycle-it-"+id, []byte{0x01, 0x02, 0x03},
		string(domain),
		string(state), owner, claimedAt); err != nil {
		t.Fatalf("seed discrepancy %s: %v", id, err)
	}
}

// lifecycleITDiscrepancyState reads the observable row state.
func lifecycleITDiscrepancyState(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	id string) (DiscrepancyState, string, int64, time.Time) {
	t.Helper()
	var state, owner string
	var reopen int64
	var updatedAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT state, COALESCE(claim_owner, ''), reopen_count::bigint, updated_at
		FROM discrepancy
		WHERE discrepancy_id = $1::uuid`, id).Scan(&state, &owner, &reopen, &updatedAt); err != nil {
		t.Fatalf("read discrepancy %s: %v", id, err)
	}
	return DiscrepancyState(state), owner, reopen, updatedAt
}

// lifecycleITCount runs one scalar count query.
func lifecycleITCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

// lifecycleITMustClaim drives one legal open_claimable -> claimed claim
// through the T022 production path and fails the test on any other outcome.
func lifecycleITMustClaim(t *testing.T, ctx context.Context, store *Store,
	id string, principal Principal, authorizer DiscrepancyAuthorizer) DiscrepancyClaimResult {
	t.Helper()
	res, err := store.ClaimDiscrepancy(ctx, DiscrepancyClaimRequest{
		DiscrepancyID: id, Principal: principal, Authorizer: authorizer, Reason: "claim",
	})
	if err != nil {
		t.Fatalf("ClaimDiscrepancy(%s) by %s: %v", id, principal, err)
	}
	if !res.Claimed || res.From != DiscrepancyStateOpenClaimable || res.To != DiscrepancyStateClaimed {
		t.Fatalf("ClaimDiscrepancy(%s) = %+v, want open_claimable -> claimed", id, res)
	}
	if res.Owner != principal.String() {
		t.Fatalf("ClaimDiscrepancy(%s) owner = %q, want the authenticated principal %q", id, res.Owner, principal.String())
	}
	return res
}

// lifecycleITAuthzAudit persists authorization refusals the same way the
// production writer does: append-only recon_audit rows; a write failure never
// turns a denial into an allow.
type lifecycleITAuthzAudit struct {
	pool *pgxpool.Pool
}

// RecordAuthzRefusal implements AuthzAuditWriter.
func (w lifecycleITAuthzAudit) RecordAuthzRefusal(ctx context.Context, rec AuthzAuditRecord) error {
	target, err := json.Marshal(map[string]any{
		"action":     string(rec.Action),
		"permission": string(rec.Permission),
		"scope":      rec.Scope,
	})
	if err != nil {
		return err
	}
	actor := strings.TrimSpace(rec.Principal)
	if actor == "" {
		actor = "unauthenticated"
	}
	_, err = w.pool.Exec(ctx, `
		INSERT INTO recon_audit (actor, action, target, reason, evidence, result)
		VALUES ($1, 'refuse', $2::jsonb, $3, $4, 'refused')`,
		actor, string(target), string(rec.Reason), rec.Detail)
	return err
}

// lifecycleITSeedPermission inserts one recon_permission grant row with the
// canonical scope digest the registry PK requires.
func lifecycleITSeedPermission(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	principal string, permission Permission, scope AuthScope, grantedBy string) {
	t.Helper()
	if err := scope.Validate(); err != nil {
		t.Fatalf("seed permission scope invalid: %v", err)
	}
	canonical, err := scope.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical permission scope: %v", err)
	}
	digest, err := scope.Digest()
	if err != nil {
		t.Fatalf("digest permission scope: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recon_permission (principal, action, scope, scope_hash, granted_by)
		VALUES ($1, $2, $3::jsonb, $4, $5)`,
		principal, string(permission), string(canonical), digest, grantedBy); err != nil {
		t.Fatalf("seed permission %s/%s: %v", principal, permission, err)
	}
}

// lifecycleITRefusalAudit returns the refusal count and the last recorded
// deny reason for one (actor, attempted action) pair.
func lifecycleITRefusalAudit(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	actor string, action Action) (int64, string) {
	t.Helper()
	n := lifecycleITCount(t, ctx, pool, `
		SELECT count(*)::bigint FROM recon_audit
		WHERE action = 'refuse' AND actor = $1 AND target->>'action' = $2`,
		actor, string(action))
	if n == 0 {
		return 0, ""
	}
	var reason string
	if err := pool.QueryRow(ctx, `
		SELECT reason FROM recon_audit
		WHERE action = 'refuse' AND actor = $1 AND target->>'action' = $2
		ORDER BY audit_id DESC LIMIT 1`, actor, string(action)).Scan(&reason); err != nil {
		t.Fatalf("read refusal reason of %s/%s: %v", actor, action, err)
	}
	return n, reason
}

func TestIntegrationDiscrepancyClaimMutualExclusion(t *testing.T) {
	ctx, pool, store := reconIT(t)
	evaluator, err := NewEvaluator(pool, lifecycleITAuthzAudit{pool: pool})
	if err != nil {
		t.Fatalf("NewEvaluator: %v", err)
	}
	scope := lifecycleITAuthScope()

	t.Run("second_claimer_is_refused_with_the_owner_and_audited", func(t *testing.T) {
		id := uuid.NewString()
		lifecycleITSeedDiscrepancy(t, ctx, pool, id, DiscrepancyStateOpenClaimable, "")
		principalA, principalB := lifecycleITPrincipal(t, 5001), lifecycleITPrincipal(t, 5002)
		ownerA, ownerB := principalA.String(), principalB.String()
		lifecycleITSeedPermission(t, ctx, pool, ownerA, PermissionExceptionHandle, scope, "deploy:it-manager")
		lifecycleITSeedPermission(t, ctx, pool, ownerB, PermissionExceptionHandle, scope, "deploy:it-manager")

		first := lifecycleITMustClaim(t, ctx, store, id, principalA, evaluator)
		if first.ReopenCount != 0 {
			t.Fatalf("first claim reopen_count = %d, want 0", first.ReopenCount)
		}
		state, owner, reopen, _ := lifecycleITDiscrepancyState(t, ctx, pool, id)
		if state != DiscrepancyStateClaimed || owner != ownerA || reopen != 0 {
			t.Fatalf("persisted claim = state %s owner %q reopen %d, want claimed/%s/0", state, owner, reopen, ownerA)
		}
		if n := lifecycleITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM recon_audit
			WHERE action = 'claim' AND target->>'discrepancy_id' = $1`, id); n != 1 {
			t.Errorf("claim audit rows = %d, want 1", n)
		}

		// B's ClaimDiscrepancy loses the single-owner CAS: the refusal carries
		// ErrDiscrepancyClaimTaken plus the observed ownership hint (Owner), is
		// audited, and B can never overwrite A's owner (claim is single-owner).
		res, err := store.ClaimDiscrepancy(ctx, DiscrepancyClaimRequest{
			DiscrepancyID: id, Principal: principalB, Authorizer: evaluator, Reason: "competing claim",
		})
		if !errors.Is(err, ErrDiscrepancyClaimTaken) {
			t.Fatalf("competing claim err = %v, want ErrDiscrepancyClaimTaken", err)
		}
		if !errors.Is(err, ErrIllegalDiscrepancyTransition) {
			t.Fatalf("competing claim err = %v, want it to wrap ErrIllegalDiscrepancyTransition", err)
		}
		if res.Claimed || res.From != DiscrepancyStateClaimed || res.Owner != ownerA {
			t.Fatalf("competing claim result = %+v, want unclaimed from claimed with the ownership hint %q", res, ownerA)
		}
		state, owner, _, _ = lifecycleITDiscrepancyState(t, ctx, pool, id)
		if state != DiscrepancyStateClaimed || owner != ownerA {
			t.Fatalf("state after refused claim = %s/%q, want claimed/%q (the owner never changes)", state, owner, ownerA)
		}
		n := lifecycleITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM recon_audit
			WHERE action = 'refuse' AND target->>'discrepancy_id' = $1`, id)
		if n != 1 {
			t.Fatalf("refuse audit rows = %d, want 1", n)
		}
		var actor, reason string
		if err := pool.QueryRow(ctx, `
			SELECT actor, reason FROM recon_audit
			WHERE action = 'refuse' AND target->>'discrepancy_id' = $1`, id).Scan(&actor, &reason); err != nil {
			t.Fatalf("read the refusal audit: %v", err)
		}
		if actor != ownerB || !strings.Contains(reason, ownerA) || !strings.Contains(reason, string(DiscrepancyStateClaimed)) {
			t.Fatalf("refusal audit = actor %q reason %q, want actor %q naming the observed owner %q on the claimed ticket",
				actor, reason, ownerB, ownerA)
		}
	})

	t.Run("concurrent_claimers_produce_exactly_one_owner", func(t *testing.T) {
		id := uuid.NewString()
		lifecycleITSeedDiscrepancy(t, ctx, pool, id, DiscrepancyStateOpenClaimable, "")
		principals := []Principal{lifecycleITPrincipal(t, 5101), lifecycleITPrincipal(t, 5102)}
		owners := make([]string, len(principals))
		for i, principal := range principals {
			owners[i] = principal.String()
			lifecycleITSeedPermission(t, ctx, pool, owners[i], PermissionExceptionHandle, scope, "deploy:it-manager")
		}

		type outcome struct {
			owner   string
			claimed bool
			hinted  string
			err     error
		}
		start := make(chan struct{})
		outcomes := make(chan outcome, len(owners))
		var wg sync.WaitGroup
		for i := range principals {
			wg.Add(1)
			go func(principal Principal, owner string) {
				defer wg.Done()
				<-start
				res, err := store.ClaimDiscrepancy(ctx, DiscrepancyClaimRequest{
					DiscrepancyID: id, Principal: principal, Authorizer: evaluator, Reason: "concurrent claim",
				})
				outcomes <- outcome{owner: owner, claimed: res.Claimed, hinted: res.Owner, err: err}
			}(principals[i], owners[i])
		}
		close(start)
		wg.Wait()
		close(outcomes)

		var winners []string
		for out := range outcomes {
			switch {
			case out.err == nil:
				if !out.claimed {
					t.Fatalf("concurrent claim by %s returned no error without a claim", out.owner)
				}
				winners = append(winners, out.owner)
			case errors.Is(out.err, ErrDiscrepancyClaimTaken):
				if out.claimed || out.hinted == "" {
					t.Fatalf("losing claim by %s = %+v, want unclaimed with the observed ownership hint", out.owner, out)
				}
			default:
				t.Fatalf("unexpected concurrent claim error for %s: %v", out.owner, out.err)
			}
		}
		if len(winners) != 1 {
			t.Fatalf("concurrent winners = %v, want exactly one", winners)
		}
		_, owner, _, _ := lifecycleITDiscrepancyState(t, ctx, pool, id)
		if owner != winners[0] {
			t.Fatalf("persisted owner = %q, want the single winner %q", owner, winners[0])
		}
	})
}

func TestIntegrationDisposeIdempotencyZeroSideEffects(t *testing.T) {
	ctx, pool, store := reconIT(t)
	evaluator, err := NewEvaluator(pool, lifecycleITAuthzAudit{pool: pool})
	if err != nil {
		t.Fatalf("NewEvaluator: %v", err)
	}
	id := uuid.NewString()
	principal := lifecycleITPrincipal(t, 5201)
	owner := principal.String()
	lifecycleITSeedPermission(t, ctx, pool, owner, PermissionDisposeAck, lifecycleITAuthScope(), "deploy:it-manager")
	lifecycleITSeedDiscrepancy(t, ctx, pool, id, DiscrepancyStateDisposing, owner)

	// Dispose without a recorded disposition: disposed ≠ reverified, so a
	// recordless hand-over to verification is refused and audited.
	if _, err := store.TransitionDiscrepancy(ctx, DiscrepancyTransitionRequest{
		DiscrepancyID: id, To: DiscrepancyStatePendingVerify, Actor: owner, Reason: "dispose without a record",
	}); !errors.Is(err, ErrDispositionRequired) {
		t.Fatalf("dispose without a record err = %v, want ErrDispositionRequired", err)
	}
	if state, _, _, _ := lifecycleITDiscrepancyState(t, ctx, pool, id); state != DiscrepancyStateDisposing {
		t.Fatalf("state after the refused dispose = %s, want disposing", state)
	}
	if n := lifecycleITCount(t, ctx, pool, `
		SELECT count(*)::bigint FROM recon_audit
		WHERE action = 'refuse' AND target->>'discrepancy_id' = $1`, id); n != 1 {
		t.Fatalf("refuse audit rows after the recordless dispose = %d, want 1", n)
	}

	// First dispose through the T022 path: record the disposition row and apply
	// the guarded disposing -> pending_verify hand-over in one call.
	key := uuid.NewString()
	dispose := DiscrepancyDisposeRequest{
		DiscrepancyID: id, Kind: DispositionAckOnly,
		Operator: owner, Reason: "dispose ack_only",
		IdempotencyKey: key, Principal: principal, Authorizer: evaluator,
	}
	first, err := store.DisposeDiscrepancy(ctx, dispose)
	if err != nil {
		t.Fatalf("DisposeDiscrepancy: %v", err)
	}
	if first.IdempotentReplay || !first.Transitioned ||
		first.StateBefore != DiscrepancyStateDisposing || first.StateAfter != DiscrepancyStatePendingVerify {
		t.Fatalf("first dispose = %+v, want a disposing -> pending_verify hand-over", first)
	}
	if first.DispositionID == "" || first.Kind != DispositionAckOnly || first.Result != DispositionDone {
		t.Fatalf("first dispose recorded %+v, want one ack_only/done disposition", first)
	}
	state, _, reopen, updatedAt := lifecycleITDiscrepancyState(t, ctx, pool, id)
	if state != DiscrepancyStatePendingVerify {
		t.Fatalf("state after the dispose = %s, want pending_verify", state)
	}
	if n := lifecycleITCount(t, ctx, pool,
		`SELECT count(*)::bigint FROM disposition WHERE discrepancy_id = $1::uuid`, id); n != 1 {
		t.Fatalf("disposition rows = %d, want 1", n)
	}
	baseAudit := lifecycleITCount(t, ctx, pool, `SELECT count(*)::bigint FROM recon_audit`)

	// Repeat the exact dispose: the UNIQUE key converges on the recorded row
	// through the replay path, never a second insert or state change.
	second, err := store.DisposeDiscrepancy(ctx, dispose)
	if err != nil {
		t.Fatalf("repeated DisposeDiscrepancy: %v", err)
	}
	if !second.IdempotentReplay {
		t.Fatalf("repeated dispose = %+v, want IdempotentReplay", second)
	}
	if second.DispositionID != first.DispositionID {
		t.Fatalf("replay disposition_id = %q, want the recorded row %q", second.DispositionID, first.DispositionID)
	}
	if second.Kind != first.Kind || second.Result != first.Result || second.ActionRef != first.ActionRef {
		t.Fatalf("replay = %+v, want the recorded row's kind/result/action_ref %s/%s/%q",
			second, first.Kind, first.Result, first.ActionRef)
	}
	if second.Transitioned || second.StateBefore != DiscrepancyStatePendingVerify || second.StateAfter != DiscrepancyStatePendingVerify {
		t.Fatalf("replay state = %s -> %s (transitioned %v), want pending_verify unchanged",
			second.StateBefore, second.StateAfter, second.Transitioned)
	}
	if second.ReopenCount != reopen || !second.RecordedAt.Equal(first.RecordedAt) {
		t.Fatalf("replay reopen/recorded_at = %d/%v, want the recorded %d/%v",
			second.ReopenCount, second.RecordedAt, reopen, first.RecordedAt)
	}
	if n := lifecycleITCount(t, ctx, pool,
		`SELECT count(*)::bigint FROM disposition WHERE idempotency_key = $1`, key); n != 1 {
		t.Fatalf("disposition rows for the key = %d, want the single recorded row", n)
	}

	// SC-004 / quickstart §9: ten more replays of the same recorded dispose
	// converge on the recorded row with zero new side effects (the loop runs
	// before the zero-side-effect assertions below, so they cover all ten).
	for i := 1; i <= 10; i++ {
		replay, err := store.DisposeDiscrepancy(ctx, dispose)
		if err != nil {
			t.Fatalf("replay %d of the recorded dispose: %v", i, err)
		}
		if !replay.IdempotentReplay || replay.DispositionID != first.DispositionID ||
			replay.Transitioned || replay.Kind != first.Kind || replay.Result != first.Result {
			t.Fatalf("replay %d = %+v, want the recorded row replayed with no transition", i, replay)
		}
	}

	// Zero side effects: no second disposition, no new audit row, no reverify
	// row, no state/reopen/updated_at movement.
	if n := lifecycleITCount(t, ctx, pool,
		`SELECT count(*)::bigint FROM disposition WHERE discrepancy_id = $1::uuid`, id); n != 1 {
		t.Errorf("disposition rows after the repeat = %d, want 1", n)
	}
	if n := lifecycleITCount(t, ctx, pool, `SELECT count(*)::bigint FROM recon_audit`); n != baseAudit {
		t.Errorf("audit rows after the repeat = %d, want %d", n, baseAudit)
	}
	if n := lifecycleITCount(t, ctx, pool,
		`SELECT count(*)::bigint FROM reverify WHERE discrepancy_id = $1::uuid`, id); n != 0 {
		t.Errorf("reverify rows after the repeat = %d, want 0", n)
	}
	state, _, reopenAgain, updatedAtAgain := lifecycleITDiscrepancyState(t, ctx, pool, id)
	if state != DiscrepancyStatePendingVerify || reopenAgain != reopen || !updatedAtAgain.Equal(updatedAt) {
		t.Errorf("state after the repeat = %s (reopen %d, updated %v), want pending_verify/%d/%v",
			state, reopenAgain, updatedAtAgain, reopen, updatedAt)
	}

	// Even a mis-wired repeat transition cannot re-dispose: the lifecycle has
	// no pending_verify -> disposing edge, and the refusal is audited.
	if _, err := store.TransitionDiscrepancy(ctx, DiscrepancyTransitionRequest{
		DiscrepancyID: id, To: DiscrepancyStatePendingVerify, Actor: owner, Reason: "repeat dispose transition",
		Guard: TransitionGuard{Disposition: &DispositionSnapshot{Kind: DispositionAckOnly, Result: DispositionDone}},
	}); !errors.Is(err, ErrIllegalDiscrepancyTransition) {
		t.Fatalf("repeat dispose transition err = %v, want ErrIllegalDiscrepancyTransition", err)
	}
	if state, _, _, _ := lifecycleITDiscrepancyState(t, ctx, pool, id); state != DiscrepancyStatePendingVerify {
		t.Fatalf("state after the repeat transition = %s, want pending_verify", state)
	}
}

func TestIntegrationClaimOwnershipIsNotADisposalRight(t *testing.T) {
	ctx, pool, store := reconIT(t)
	scope := lifecycleITAuthScope()
	principal, err := APIKeyPrincipal(7301)
	if err != nil {
		t.Fatalf("APIKeyPrincipal: %v", err)
	}
	lifecycleITSeedPermission(t, ctx, pool, principal.String(), PermissionExceptionHandle, scope, "deploy:it-manager")
	evaluator, err := NewEvaluator(pool, lifecycleITAuthzAudit{pool: pool})
	if err != nil {
		t.Fatalf("NewEvaluator: %v", err)
	}

	claim, err := evaluator.Authorize(ctx, principal, ActionClaim, scope)
	if err != nil || !claim.Allowed {
		t.Fatalf("claim authorization = %+v (err %v), want allowed", claim, err)
	}
	if claim.ExecutionRight() {
		t.Fatalf("an allowed claim carries an execution right; a claim must be ownership only")
	}
	if n, _ := lifecycleITRefusalAudit(t, ctx, pool, principal.String(), ActionClaim); n != 0 {
		t.Errorf("the allowed claim wrote %d refusal audits, want 0", n)
	}

	// The same exception_handle holder has no dispose or close right: claim ≠
	// execute (auth-matrix claim row; FR-020).
	for _, action := range []Action{ActionDisposeAck, ActionDisposeReuse, ActionVerifyClose} {
		decision, err := evaluator.Authorize(ctx, principal, action, scope)
		if err != nil {
			t.Fatalf("authorize %s: %v", action, err)
		}
		if decision.Allowed || decision.Reason != DenyNoGrant {
			t.Fatalf("authorize %s = allowed %v reason %q, want denied no_grant", action, decision.Allowed, decision.Reason)
		}
		if decision.ExecutionRight() {
			t.Fatalf("the denied %s decision carries an execution right", action)
		}
	}
	if n, reason := lifecycleITRefusalAudit(t, ctx, pool, principal.String(), ActionDisposeAck); n != 1 || reason != string(DenyNoGrant) {
		t.Errorf("dispose refusal audit = %d rows reason %q, want 1 row reason %q", n, reason, DenyNoGrant)
	}
	if n, _ := lifecycleITRefusalAudit(t, ctx, pool, principal.String(), ActionDisposeReuse); n != 1 {
		t.Errorf("dispose_reuse refusal audits = %d, want 1", n)
	}
	if n, _ := lifecycleITRefusalAudit(t, ctx, pool, principal.String(), ActionVerifyClose); n != 1 {
		t.Errorf("verify_close refusal audits = %d, want 1", n)
	}

	// The claim itself is recorded as ownership and nothing else.
	id := uuid.NewString()
	lifecycleITSeedDiscrepancy(t, ctx, pool, id, DiscrepancyStateOpenClaimable, "")
	lifecycleITMustClaim(t, ctx, store, id, principal, evaluator)
	if _, owner, _, _ := lifecycleITDiscrepancyState(t, ctx, pool, id); owner != principal.String() {
		t.Fatalf("persisted owner = %q, want %q", owner, principal.String())
	}
}

func TestIntegrationAuthorizationRefusalsAreAudited(t *testing.T) {
	ctx, pool, _ := reconIT(t)
	evaluator, err := NewEvaluator(pool, lifecycleITAuthzAudit{pool: pool})
	if err != nil {
		t.Fatalf("NewEvaluator: %v", err)
	}
	request := AuthScope{
		ChainID: reconITChainID, Kind: ScopeHeight,
		BusinessTypes: []BusinessType{BusinessWithdrawal},
		RangeStart:    lifecycleITInt64(100), RangeEnd: lifecycleITInt64(200),
	}
	principalFor := func(t *testing.T, id int64) Principal {
		t.Helper()
		p, err := APIKeyPrincipal(id)
		if err != nil {
			t.Fatalf("APIKeyPrincipal(%d): %v", id, err)
		}
		return p
	}

	t.Run("missing_grant_is_denied_by_default_and_audited", func(t *testing.T) {
		principal := principalFor(t, 7401)
		decision, err := evaluator.Authorize(ctx, principal, ActionClaim, request)
		if err != nil || decision.Allowed || decision.Reason != DenyNoGrant {
			t.Fatalf("no-grant claim = %+v (err %v), want denied no_grant", decision, err)
		}
		if n, reason := lifecycleITRefusalAudit(t, ctx, pool, principal.String(), ActionClaim); n != 1 || reason != string(DenyNoGrant) {
			t.Fatalf("refusal audit = %d rows reason %q, want 1 row reason %q", n, reason, DenyNoGrant)
		}
	})

	t.Run("cross_chain_grant_is_out_of_scope_and_audited", func(t *testing.T) {
		principal := principalFor(t, 7402)
		grant := request
		grant.ChainID = "recon-it-other-chain"
		lifecycleITSeedPermission(t, ctx, pool, principal.String(), PermissionExceptionHandle, grant, "deploy:it-manager")
		decision, err := evaluator.Authorize(ctx, principal, ActionClaim, request)
		if err != nil || decision.Allowed || decision.Reason != DenyOutOfScope {
			t.Fatalf("cross-chain claim = %+v (err %v), want denied out_of_scope", decision, err)
		}
		if n, reason := lifecycleITRefusalAudit(t, ctx, pool, principal.String(), ActionClaim); n != 1 || reason != string(DenyOutOfScope) {
			t.Fatalf("refusal audit = %d rows reason %q, want 1 row reason %q", n, reason, DenyOutOfScope)
		}
	})

	t.Run("range_outside_the_grant_is_out_of_scope_and_audited", func(t *testing.T) {
		principal := principalFor(t, 7403)
		grant := request
		grant.RangeStart, grant.RangeEnd = lifecycleITInt64(100), lifecycleITInt64(150)
		lifecycleITSeedPermission(t, ctx, pool, principal.String(), PermissionExceptionHandle, grant, "deploy:it-manager")
		decision, err := evaluator.Authorize(ctx, principal, ActionClaim, request)
		if err != nil || decision.Allowed || decision.Reason != DenyOutOfScope {
			t.Fatalf("out-of-range claim = %+v (err %v), want denied out_of_scope", decision, err)
		}
		if n, _ := lifecycleITRefusalAudit(t, ctx, pool, principal.String(), ActionClaim); n != 1 {
			t.Fatalf("refusal audits = %d, want 1", n)
		}
	})

	t.Run("business_type_outside_the_grant_is_out_of_scope_and_audited", func(t *testing.T) {
		principal := principalFor(t, 7404)
		grant := request
		grant.BusinessTypes = []BusinessType{BusinessDeposit}
		lifecycleITSeedPermission(t, ctx, pool, principal.String(), PermissionExceptionHandle, grant, "deploy:it-manager")
		decision, err := evaluator.Authorize(ctx, principal, ActionClaim, request)
		if err != nil || decision.Allowed || decision.Reason != DenyOutOfScope {
			t.Fatalf("deposit-only grant claim = %+v (err %v), want denied out_of_scope", decision, err)
		}
		if n, _ := lifecycleITRefusalAudit(t, ctx, pool, principal.String(), ActionClaim); n != 1 {
			t.Fatalf("refusal audits = %d, want 1", n)
		}
	})

	t.Run("forged_identity_is_denied_unauthenticated_and_audited", func(t *testing.T) {
		decision, err := evaluator.Authorize(ctx, Principal{}, ActionClaim, request)
		if err != nil || decision.Allowed || decision.Reason != DenyUnauthenticated {
			t.Fatalf("forged claim = %+v (err %v), want denied unauthenticated", decision, err)
		}
		if n, reason := lifecycleITRefusalAudit(t, ctx, pool, "unauthenticated", ActionClaim); n != 1 || reason != string(DenyUnauthenticated) {
			t.Fatalf("refusal audit = %d rows reason %q, want 1 row reason %q", n, reason, DenyUnauthenticated)
		}
		// Free-text/CLI-style inputs can never mint a principal.
		if _, err := APIKeyPrincipal(0); !errors.Is(err, ErrInvalidPrincipal) {
			t.Errorf("APIKeyPrincipal(0) err = %v, want ErrInvalidPrincipal", err)
		}
		for _, raw := range []string{"", "alice", "operator:alice", "apikey:free text", "apikey:"} {
			if _, err := ConfigPrincipal(raw); !errors.Is(err, ErrInvalidPrincipal) {
				t.Errorf("ConfigPrincipal(%q) err = %v, want ErrInvalidPrincipal", raw, err)
			}
		}
	})

	t.Run("unknown_action_is_denied_and_audited", func(t *testing.T) {
		principal := principalFor(t, 7405)
		decision, err := evaluator.Authorize(ctx, principal, Action("grant_self"), request)
		if err != nil || decision.Allowed || decision.Reason != DenyUnknownAction {
			t.Fatalf("unknown action = %+v (err %v), want denied unknown_action", decision, err)
		}
		if n, _ := lifecycleITRefusalAudit(t, ctx, pool, principal.String(), Action("grant_self")); n != 1 {
			t.Fatalf("refusal audits = %d, want 1", n)
		}
	})

	t.Run("ordinary_holders_cannot_self_grant", func(t *testing.T) {
		principal := principalFor(t, 7406)
		for _, permission := range KnownPermissions() {
			lifecycleITSeedPermission(t, ctx, pool, principal.String(), permission, request, "deploy:it-manager")
		}
		before := lifecycleITCount(t, ctx, pool,
			`SELECT count(*)::bigint FROM recon_permission WHERE principal = $1`, principal.String())
		decision, err := evaluator.Authorize(ctx, principal, ActionPermissionGrant, request)
		if err != nil || decision.Allowed || decision.Reason != DenyUnapprovedAction {
			t.Fatalf("self-grant = %+v (err %v), want denied unapproved_action", decision, err)
		}
		after := lifecycleITCount(t, ctx, pool,
			`SELECT count(*)::bigint FROM recon_permission WHERE principal = $1`, principal.String())
		if after != before {
			t.Fatalf("self-grant changed recon_permission rows: %d -> %d", before, after)
		}
		if n, reason := lifecycleITRefusalAudit(t, ctx, pool, principal.String(), ActionPermissionGrant); n != 1 || reason != string(DenyUnapprovedAction) {
			t.Fatalf("refusal audit = %d rows reason %q, want 1 row reason %q", n, reason, DenyUnapprovedAction)
		}
	})
}

func TestIntegrationCloseRequiresFreshConsistentUngappedEvidence(t *testing.T) {
	ctx, pool, store := reconIT(t)
	scope := AuthScope{
		ChainID: reconITChainID, Kind: ScopeHeight,
		BusinessTypes: []BusinessType{BusinessWithdrawal},
		RangeStart:    lifecycleITInt64(100), RangeEnd: lifecycleITInt64(200),
	}
	closer, err := APIKeyPrincipal(7501)
	if err != nil {
		t.Fatalf("APIKeyPrincipal: %v", err)
	}
	lifecycleITSeedPermission(t, ctx, pool, closer.String(), PermissionClose, scope, "deploy:it-manager")
	evaluator, err := NewEvaluator(pool, lifecycleITAuthzAudit{pool: pool})
	if err != nil {
		t.Fatalf("NewEvaluator: %v", err)
	}
	closeBasis := func() []byte {
		return []byte(fmt.Sprintf(`{"range":"100..105","block":105,"version":"v1","observed_at":%q}`,
			time.Now().UTC().Format(time.RFC3339)))
	}

	// The permission gate is satisfied for the closer below; the evidence gate
	// is what refuses.
	if decision, err := evaluator.Authorize(ctx, closer, ActionVerifyClose, scope); err != nil || !decision.Allowed {
		t.Fatalf("verify_close authorization = %+v (err %v), want allowed", decision, err)
	}

	t.Run("unverified_evidence_is_refused", func(t *testing.T) {
		id := uuid.NewString()
		lifecycleITSeedDiscrepancy(t, ctx, pool, id, DiscrepancyStatePendingVerify, "")
		// The schema cannot even record a consistent verdict without its
		// evidence reference and freshness (reverify_consistent_evidence_check).
		if _, err := pool.Exec(ctx, `
			INSERT INTO reverify (discrepancy_id, verdict, evidence_ref)
			VALUES ($1::uuid, 'consistent', '')`, id); err == nil {
			t.Fatalf("a consistent verdict without evidence was recorded")
		}
		if _, err := store.TransitionDiscrepancy(ctx, DiscrepancyTransitionRequest{
			DiscrepancyID: id, To: DiscrepancyStateClosed, Actor: closer.String(), Reason: "force close",
			Guard: TransitionGuard{Now: time.Now().UTC(), ReverifyTolerance: time.Hour, CloseBasis: closeBasis()},
		}); !errors.Is(err, ErrReverifyRequired) {
			t.Fatalf("close without a reverify err = %v, want ErrReverifyRequired", err)
		}
		if state, _, _, _ := lifecycleITDiscrepancyState(t, ctx, pool, id); state != DiscrepancyStatePendingVerify {
			t.Fatalf("state after the refused close = %s, want pending_verify", state)
		}
		if n := lifecycleITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM discrepancy
			WHERE discrepancy_id = $1::uuid AND close_basis IS NULL`, id); n != 1 {
			t.Fatalf("the refused close wrote a close_basis")
		}
		if n := lifecycleITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM recon_audit
			WHERE action = 'refuse' AND target->>'discrepancy_id' = $1`, id); n != 1 {
			t.Fatalf("refuse audit rows = %d, want 1", n)
		}
	})

	t.Run("expired_evidence_is_refused", func(t *testing.T) {
		id := uuid.NewString()
		lifecycleITSeedDiscrepancy(t, ctx, pool, id, DiscrepancyStatePendingVerify, "")
		expiredAt := time.Now().UTC().Add(-2 * time.Hour)
		if _, err := pool.Exec(ctx, `
			INSERT INTO reverify (discrepancy_id, verdict, evidence_ref, freshness_at)
			VALUES ($1::uuid, 'consistent', 'evidence-expired', $2)`, id, expiredAt); err != nil {
			t.Fatalf("insert expired reverify: %v", err)
		}
		_, err := store.TransitionDiscrepancy(ctx, DiscrepancyTransitionRequest{
			DiscrepancyID: id, To: DiscrepancyStateClosed, Actor: closer.String(), Reason: "close on expired evidence",
			Guard: TransitionGuard{
				LatestReverify: &ReverifyEvidence{
					Verdict: ReverifyConsistent, EvidenceRef: "evidence-expired", FreshnessAt: expiredAt,
				},
				Now:               time.Now().UTC(),
				ReverifyTolerance: time.Hour,
				CloseBasis:        closeBasis(),
			},
		})
		if !errors.Is(err, ErrReverifyRequired) {
			t.Fatalf("close on expired evidence err = %v, want ErrReverifyRequired", err)
		}
		if state, _, _, _ := lifecycleITDiscrepancyState(t, ctx, pool, id); state != DiscrepancyStatePendingVerify {
			t.Fatalf("state after the expired close = %s, want pending_verify", state)
		}
	})

	t.Run("gapped_coverage_evidence_is_refused", func(t *testing.T) {
		id := uuid.NewString()
		lifecycleITSeedDiscrepancy(t, ctx, pool, id, DiscrepancyStatePendingVerify, "")
		// A running scan whose coverage still has an open query_failed gap:
		// the only honest verdict for that evidence is unknown, and unknown
		// can never close (Q5-4; data-model.md §6: a traversal cursor over a
		// gap never claims verified completeness).
		taskID := uuid.NewString()
		reconSeedHeightTask(t, ctx, pool, taskID, 100, 200, "running", "")
		if _, err := pool.Exec(ctx, `
			INSERT INTO recon_gap (task_id, range_start, range_end, reason)
			VALUES ($1::uuid, 100, 105, 'query_failed')`, taskID); err != nil {
			t.Fatalf("insert gap: %v", err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO reverify (discrepancy_id, verdict, evidence_ref, freshness_at)
			VALUES ($1::uuid, 'unknown', 'scan-gap:query_failed', now())`, id); err != nil {
			t.Fatalf("insert gap-limited reverify: %v", err)
		}
		_, err := store.TransitionDiscrepancy(ctx, DiscrepancyTransitionRequest{
			DiscrepancyID: id, To: DiscrepancyStateClosed, Actor: closer.String(), Reason: "close over an open gap",
			Guard: TransitionGuard{
				LatestReverify: &ReverifyEvidence{
					Verdict: ReverifyUnknown, EvidenceRef: "scan-gap:query_failed", FreshnessAt: time.Now().UTC(),
				},
				Now:               time.Now().UTC(),
				ReverifyTolerance: time.Hour,
				CloseBasis:        closeBasis(),
			},
		})
		if !errors.Is(err, ErrReverifyRequired) {
			t.Fatalf("close over a gap err = %v, want ErrReverifyRequired", err)
		}
		if n := lifecycleITCount(t, ctx, pool,
			`SELECT count(*)::bigint FROM recon_gap WHERE task_id = $1::uuid`, taskID); n != 1 {
			t.Fatalf("gap rows after the refused close = %d, want 1 (the gap stays open)", n)
		}
		if state, _, _, _ := lifecycleITDiscrepancyState(t, ctx, pool, id); state != DiscrepancyStatePendingVerify {
			t.Fatalf("state after the gapped close = %s, want pending_verify", state)
		}
	})

	t.Run("missing_close_basis_is_refused", func(t *testing.T) {
		id := uuid.NewString()
		lifecycleITSeedDiscrepancy(t, ctx, pool, id, DiscrepancyStatePendingVerify, "")
		evidence := &ReverifyEvidence{
			Verdict: ReverifyConsistent, EvidenceRef: "evidence-1", FreshnessAt: time.Now().UTC(),
		}
		_, err := store.TransitionDiscrepancy(ctx, DiscrepancyTransitionRequest{
			DiscrepancyID: id, To: DiscrepancyStateClosed, Actor: closer.String(), Reason: "close without a basis",
			Guard: TransitionGuard{LatestReverify: evidence, Now: time.Now().UTC(), ReverifyTolerance: time.Hour},
		})
		if !errors.Is(err, ErrContract) {
			t.Fatalf("close without close_basis err = %v, want ErrContract", err)
		}
		if state, _, _, _ := lifecycleITDiscrepancyState(t, ctx, pool, id); state != DiscrepancyStatePendingVerify {
			t.Fatalf("state after the basis-less close = %s, want pending_verify", state)
		}
	})

	t.Run("fresh_consistent_evidence_closes_with_the_basis", func(t *testing.T) {
		id := uuid.NewString()
		lifecycleITSeedDiscrepancy(t, ctx, pool, id, DiscrepancyStatePendingVerify, "")
		freshness := time.Now().UTC()
		if _, err := pool.Exec(ctx, `
			INSERT INTO reverify (discrepancy_id, verdict, evidence_ref, freshness_at)
			VALUES ($1::uuid, 'consistent', 'evidence-fresh', $2)`, id, freshness); err != nil {
			t.Fatalf("insert fresh reverify: %v", err)
		}
		res, err := store.TransitionDiscrepancy(ctx, DiscrepancyTransitionRequest{
			DiscrepancyID: id, To: DiscrepancyStateClosed, Actor: closer.String(), Reason: "verified consistent",
			Guard: TransitionGuard{
				LatestReverify: &ReverifyEvidence{
					Verdict: ReverifyConsistent, EvidenceRef: "evidence-fresh", FreshnessAt: freshness,
				},
				Now:               time.Now().UTC(),
				ReverifyTolerance: time.Hour,
				CloseBasis:        closeBasis(),
			},
		})
		if err != nil || res.From != DiscrepancyStatePendingVerify || res.To != DiscrepancyStateClosed {
			t.Fatalf("verified close = %+v (err %v), want pending_verify -> closed", res, err)
		}
		if state, _, _, _ := lifecycleITDiscrepancyState(t, ctx, pool, id); state != DiscrepancyStateClosed {
			t.Fatalf("state after the verified close = %s, want closed", state)
		}
		var rangeText, blockText string
		if err := pool.QueryRow(ctx, `
			SELECT close_basis->>'range', close_basis->>'block'
			FROM discrepancy WHERE discrepancy_id = $1::uuid`, id).Scan(&rangeText, &blockText); err != nil {
			t.Fatalf("read the recorded close_basis: %v", err)
		}
		if rangeText != "100..105" || blockText != "105" {
			t.Fatalf("close_basis = range %q block %q, want the recorded range/block snapshot", rangeText, blockText)
		}
		if n := lifecycleITCount(t, ctx, pool, `
			SELECT count(*)::bigint FROM recon_audit
			WHERE action = 'close' AND target->>'discrepancy_id' = $1`, id); n != 1 {
			t.Fatalf("close audit rows = %d, want 1", n)
		}
	})
}
