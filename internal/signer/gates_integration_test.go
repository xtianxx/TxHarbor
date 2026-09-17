//go:build integration

// gates_integration_test.go owns spec task T026 for 009-signer-service: the V6
// gate-consumption matrix on a real, isolated PostgreSQL (quickstart V6;
// FR-17/FR-18/FR-19; contracts/gates.md; SC-06). It exercises the 006
// pause/recovery/version gate, the 008 five-class binding mapping through
// contract-shape doubles (retired at T028, never here), the 007 FOR SHARE
// revoke ordering, the scopeless-grant fail-closed rule (research R11/Q-B),
// and can_sign off — then proves every upstream 006/007 read left the tables
// byte-identical. Fixtures are raw SQL: no internal/indexer or
// internal/withdrawal writer is imported.
package signer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	gateChainID   = int64(31337)
	gateCallerID  = int64(4001)
	gateAuthID    = "wa-gate-1"
	gateRequestID = "sr-gate-1"
	gateAsset     = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	gateRecipient = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	gateAmount    = "1000000"
)

// gateHash is a syntactically valid 32-byte 0x hash for 006 fixtures.
func gateHash(fill string) string { return "0x" + strings.Repeat(fill, 32) }

// gateExec runs one fixture statement, failing the test on error.
func gateExec(t *testing.T, pool *pgxpool.Pool, query string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// gateReset006 removes every harness-inserted 006 fixture. Only the harness
// writes these tables; 009 reads them.
func gateReset006(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	for _, tbl := range []string{
		"indexer_pause", "log_pause", "deposit_pause",
		"reorg_recovery", "reorg_recovery_events",
	} {
		gateExec(t, pool, "DELETE FROM "+tbl)
	}
}

// gateSeedPolicy installs the reorg policy row reorg_recovery's FK needs.
func gateSeedPolicy(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	gateExec(t, pool, `INSERT INTO reorg_policy_history (chain_id, policy_seq, max_depth, operator)
		VALUES ($1, 1, 12, 'bootstrap') ON CONFLICT (chain_id, policy_seq) DO NOTHING`, gateChainID)
}

// gateSeedRecovery inserts an active recovery instance in one phase.
func gateSeedRecovery(t *testing.T, pool *pgxpool.Pool, recoveryID, phase string, seq int64) {
	t.Helper()
	gateSeedPolicy(t, pool)
	gateExec(t, pool, `INSERT INTO reorg_recovery
		(chain_id, recovery_id, phase, policy_seq, max_depth, bound_old_number, bound_old_hash, recovery_seq)
		VALUES ($1, $2, $3, 1, 12, 100, $4, $5)`,
		gateChainID, recoveryID, phase, gateHash("aa"), seq)
}

// gateSeedGrant inserts the 007 caller and grant the FK and gate read need.
func gateSeedGrant(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	gateExec(t, pool, `INSERT INTO caller (caller_id, can_create) VALUES ($1, TRUE)
		ON CONFLICT (caller_id) DO NOTHING`, gateCallerID)
	gateExec(t, pool, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state, expires_at, supplied_by)
		VALUES ($1, $2, $3, $4, $5, $6, 'active', now() + interval '1 hour', 'gate-test')
		ON CONFLICT (authorization_id) DO UPDATE
		SET state = 'active', expires_at = EXCLUDED.expires_at`,
		gateAuthID, gateCallerID, gateChainID, gateAsset, gateRecipient, gateAmount)
}

// gateRead006 runs the 009 read sequence — gate-table SHARE lock then the one
// statement snapshot — and returns the observed basis.
func gateRead006(t *testing.T, pool *pgxpool.Pool) RecoveryGate {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin gate read: %v", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, GateLockSQL); err != nil {
		t.Fatalf("gate SHARE lock: %v", err)
	}
	var (
		g          RecoveryGate
		recoveryID *string
		phase      *string
		seq        *int64
		eventsMax  *int64
	)
	if err := tx.QueryRow(ctx, GateReadSQL, gateChainID).Scan(
		&g.IndexerPaused, &g.LogPaused, &g.DepositPaused,
		&recoveryID, &phase, &seq, &eventsMax); err != nil {
		t.Fatalf("gate read: %v", err)
	}
	if recoveryID != nil {
		g.HasRecovery = true
	}
	if phase != nil {
		g.RecoveryPhase = *phase
	}
	if seq != nil {
		g.RecoverySeq = *seq
	}
	if eventsMax != nil {
		g.HasEventsMax = true
		g.EventsMax = *eventsMax
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit gate read: %v", err)
	}
	return g
}

// gateReadGrant reads one grant FOR SHARE and reports absence as found=false.
func gateReadGrant(t *testing.T, pool *pgxpool.Pool, authID string) (AuthzGrant, bool) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin grant read: %v", err)
	}
	defer tx.Rollback(ctx)
	var (
		g       AuthzGrant
		amount  string
		expires *time.Time
	)
	err = tx.QueryRow(ctx, GrantReadSQL, authID).Scan(
		&g.CallerID, &g.ChainID, &g.Asset, &g.Recipient, &amount, &g.State, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return AuthzGrant{}, false
	}
	if err != nil {
		t.Fatalf("grant read: %v", err)
	}
	g.Amount, _ = new(big.Int).SetString(amount, 10)
	g.ExpiresAt = expires
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit grant read: %v", err)
	}
	return g, true
}

// gateRecordAuthorizationRefusal appends the pre-transaction audit row the
// serving layer writes for a refusal (data-model Table 5), pinning the class
// the gate returned rather than a caller-chosen string.
func gateRecordAuthorizationRefusal(t *testing.T, pool *pgxpool.Pool, class RefusalClass, detail string) {
	t.Helper()
	gateExec(t, pool, `INSERT INTO signing_request_audit
		(signing_request_id, caller_id, action, reason_class, detail)
		VALUES ($1, $2, 'authorization_refused', $3, $4)`,
		gateRequestID, gateCallerID, string(class), detail)
}

// gateSignatureCount counts persisted signing results across the scenario.
func gateSignatureCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM signature_results`).Scan(&n); err != nil {
		t.Fatalf("count signature_results: %v", err)
	}
	return n
}

// gateBindingDouble is the contract-shape 008 adapter double (T028 retires it
// for the live adapter): it returns the scripted read class per intent id.
type gateBindingDouble struct {
	results map[string]BindingResult
	errs    map[string]error
	calls   int
}

func (d *gateBindingDouble) ReadBinding(_ context.Context, intentID, _ string) (BindingResult, error) {
	d.calls++
	if err := d.errs[intentID]; err != nil {
		return BindingReadFailed, err
	}
	return d.results[intentID], nil
}

// gateUpstreamTables are every 006/007 relation the gate reads touch.
var gateUpstreamTables = []string{
	"indexer_pause", "log_pause", "deposit_pause",
	"reorg_recovery", "reorg_recovery_events", "withdrawal_authorizations",
}

// gateUpstreamDigest hashes each upstream table's full row set, so a
// before/after comparison detects any write the gate reads performed.
func gateUpstreamDigest(t *testing.T, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, tbl := range gateUpstreamTables {
		query := fmt.Sprintf(
			"SELECT COALESCE(md5(string_agg(x::text, E'\\n' ORDER BY x::text)), '') FROM %s x", tbl)
		var digest string
		if err := pool.QueryRow(context.Background(), query).Scan(&digest); err != nil {
			t.Fatalf("digest %s: %v", tbl, err)
		}
		out[tbl] = digest
	}
	return out
}

// TestSignerGateV6ReadOnly is the T026 acceptance surface: one isolated
// database drives every V6 branch, then asserts the upstream 006/007 tables
// are byte-identical.
func TestSignerGateV6ReadOnly(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()

	t.Run("006 pause rows refuse recovery_paused", func(t *testing.T) {
		seeds := []struct {
			name  string
			query string
			args  []any
		}{
			{"indexer_pause", `INSERT INTO indexer_pause
				(chain_id, height, expected_hash, actual_hash, kind) VALUES ($1, 10, $2, $3, 'hash_mismatch')`,
				[]any{gateChainID, gateHash("aa"), gateHash("bb")}},
			{"log_pause", `INSERT INTO log_pause (chain_id, height, kind) VALUES ($1, 10, 'chain_view_changed')`,
				[]any{gateChainID}},
			{"deposit_pause", `INSERT INTO deposit_pause (chain_id, height, kind) VALUES ($1, 10, 'upstream_gap')`,
				[]any{gateChainID}},
		}
		for _, s := range seeds {
			t.Run(s.name, func(t *testing.T) {
				gateReset006(t, pool)
				gateExec(t, pool, s.query, s.args...)
				g := gateRead006(t, pool)
				if got := g.Evaluate(0); got != ClassRecoveryPaused {
					t.Fatalf("single %s pause evaluated %q, want %q", s.name, got, ClassRecoveryPaused)
				}
				if bases := g.PauseBases(); len(bases) != 1 || bases[0] != s.name {
					t.Fatalf("pause bases = %v, want [%s]", bases, s.name)
				}
			})
		}
		gateReset006(t, pool)
		for _, s := range seeds {
			gateExec(t, pool, s.query, s.args...)
		}
		g := gateRead006(t, pool)
		if got := g.Evaluate(0); got != ClassRecoveryPaused {
			t.Fatalf("multi-pause evaluated %q, want %q (releasing one MUST NOT bypass another)", got, ClassRecoveryPaused)
		}
		if bases := g.PauseBases(); len(bases) != 3 {
			t.Fatalf("multi-pause bases = %v, want all three", bases)
		}
		gateReset006(t, pool)
	})

	t.Run("active recovery refuses recovery_active", func(t *testing.T) {
		gateReset006(t, pool)
		gateSeedRecovery(t, pool, "rec-gate-1", "replaying", 4)
		g := gateRead006(t, pool)
		if !g.HasRecovery || g.RecoveryPhase != "replaying" || g.RecoverySeq != 4 {
			t.Fatalf("observed recovery = %+v, want phase replaying seq 4", g)
		}
		if got := g.Evaluate(4); got != ClassRecoveryActive {
			t.Fatalf("active recovery evaluated %q, want %q", got, ClassRecoveryActive)
		}
	})

	t.Run("version change refuses recovery_version_changed", func(t *testing.T) {
		gateReset006(t, pool)
		gateExec(t, pool, `INSERT INTO reorg_recovery_events
			(chain_id, recovery_id, recovery_seq, event_seq, event) VALUES ($1, 'rec-old', 9, 1, 'established')`,
			gateChainID)
		g := gateRead006(t, pool)
		if g.HasRecovery || g.CurrentVersion() != 9 {
			t.Fatalf("observed version basis = %+v, want no active row and current version 9", g)
		}
		if got := g.Evaluate(7); got != ClassRecoveryVersionChanged {
			t.Fatalf("stale version evaluated %q, want %q", got, ClassRecoveryVersionChanged)
		}
		if got := g.Evaluate(9); got != "" {
			t.Fatalf("current version evaluated %q, want pass", got)
		}
		gateReset006(t, pool)
	})

	t.Run("008 binding five classes", func(t *testing.T) {
		errRead := errors.New("008 read unavailable")
		double := &gateBindingDouble{
			results: map[string]BindingResult{
				"pi-match":    BindingMatches,
				"pi-absent":   BindingAbsent,
				"pi-conflict": BindingConflict,
				"pi-paused":   BindingPaused,
				"pi-terminal": BindingTerminal,
			},
			errs: map[string]error{"pi-fail": errRead},
		}
		cases := []struct {
			intent string
			want   RefusalClass
		}{
			{"pi-match", ""},
			{"pi-absent", ClassBindingAbsent},
			{"pi-conflict", ClassBindingConflict},
			{"pi-paused", ClassBindingPaused},
			{"pi-terminal", ClassBindingTerminal},
			{"pi-fail", ClassBindingReadFailed},
		}
		admitted := 0
		for _, tc := range cases {
			res, err := double.ReadBinding(ctx, tc.intent, "at-gate")
			got := BindingRefusal(res, err)
			if got != tc.want {
				t.Fatalf("binding %s -> %q, want %q", tc.intent, got, tc.want)
			}
			if got == "" {
				admitted++
			}
		}
		if admitted != 1 {
			t.Fatalf("%d binding classes admitted, want exactly 1 (only BindingMatches)", admitted)
		}
		if err := double.errs["pi-fail"]; err == nil {
			t.Fatal("read-failure fixture lost its error")
		}
		if err := double.errs["pi-fail"]; BindingRefusal(BindingMatches, err) != ClassBindingReadFailed {
			t.Fatal("reader error did not fail closed to binding_read_failed")
		}
		if RetryabilityOf(ClassBindingTerminal) != RetryNever {
			t.Fatalf("binding_terminal retryability = %v, want RetryNever (never signable)", RetryabilityOf(ClassBindingTerminal))
		}
		if double.calls != len(cases) {
			t.Fatalf("binding reader calls = %d, want %d (read-only double, no writer call)", double.calls, len(cases))
		}
		if got := gateSignatureCount(t, pool); got != 0 {
			t.Fatalf("binding refusals produced %d signature result(s), want 0", got)
		}
	})

	t.Run("007 FOR SHARE revoke ordering", func(t *testing.T) {
		gateSeedGrant(t, pool)
		req := mustDecode(t, validBody())

		// Revoke commits first: the share read observes it.
		gateExec(t, pool, `UPDATE withdrawal_authorizations SET state = 'revoked' WHERE authorization_id = $1`, gateAuthID)
		grant, found := gateReadGrant(t, pool, gateAuthID)
		if !found || grant.State != "revoked" {
			t.Fatalf("pre-read revoke: found=%v state=%q, want revoked", found, grant.State)
		}
		if got := EvaluateGrant(found, &grant, gateCallerID, &req, time.Now().UTC()); got != ClassAuthorizationRevoked {
			t.Fatalf("revoked grant evaluated %q, want %q", got, ClassAuthorizationRevoked)
		}

		// Share lock first: the revoke waits until 009's decision commits.
		gateSeedGrant(t, pool)
		rtx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin share read: %v", err)
		}
		defer rtx.Rollback(ctx)
		var (
			observed AuthzGrant
			amount   string
			expires  *time.Time
		)
		if err := rtx.QueryRow(ctx, GrantReadSQL, gateAuthID).Scan(
			&observed.CallerID, &observed.ChainID, &observed.Asset, &observed.Recipient,
			&amount, &observed.State, &expires); err != nil {
			t.Fatalf("share read: %v", err)
		}
		if observed.State != "active" {
			t.Fatalf("share read observed %q, want active", observed.State)
		}

		started := make(chan struct{})
		revoked := make(chan error, 1)
		go func() {
			utx, err := pool.Begin(ctx)
			if err != nil {
				revoked <- err
				return
			}
			defer utx.Rollback(ctx)
			close(started)
			if _, err := utx.Exec(ctx,
				`UPDATE withdrawal_authorizations SET state = 'revoked' WHERE authorization_id = $1`,
				gateAuthID); err != nil {
				revoked <- err
				return
			}
			revoked <- utx.Commit(ctx)
		}()
		<-started
		select {
		case err := <-revoked:
			t.Fatalf("revoke did not wait for the FOR SHARE read (finished with %v)", err)
		case <-time.After(500 * time.Millisecond):
		}
		if err := rtx.Commit(ctx); err != nil {
			t.Fatalf("commit share read: %v", err)
		}
		select {
		case err := <-revoked:
			if err != nil {
				t.Fatalf("revoke after the share lock was released: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("revoke still blocked after the share lock was released")
		}
		grant, found = gateReadGrant(t, pool, gateAuthID)
		if !found || grant.State != "revoked" {
			t.Fatalf("post-ordering grant: found=%v state=%q, want revoked", found, grant.State)
		}
	})

	t.Run("scopeless grant refuses authorization_unverifiable", func(t *testing.T) {
		gateSeedGrant(t, pool)
		req := mustDecode(t, validBody())
		grant, found := gateReadGrant(t, pool, gateAuthID)
		if !found || grant.State != "active" {
			t.Fatalf("scopeless fixture: found=%v state=%q, want active", found, grant.State)
		}
		if got := EvaluateGrant(found, &grant, gateCallerID, &req, time.Now().UTC()); got != "" {
			t.Fatalf("007 row checks refused a valid grant as %q; the scopeless branch must be what refuses", got)
		}
		// The PB carrier row is read in the same FOR SHARE sequence as the
		// grant; this seeded grant has no scope row: scopeless stock.
		class := EvaluateGrantScope(GrantScope{Present: false}, &req)
		if class != ClassAuthorizationUnverifiable {
			t.Fatalf("scopeless grant evaluated %q, want %q", class, ClassAuthorizationUnverifiable)
		}
		carrier := GrantScope{
			Present:         true,
			AuthorizationID: req.AuthorizationID,
			IntentID:        req.IntentID,
			RequestID:       req.SigningRequestID,
			Sender:          req.Sender,
		}
		if got := EvaluateGrantScope(carrier, &req); got != "" {
			t.Fatalf("present-and-verifiable grant refused as %q; the refusal must be the absent carrier, not a blanket refuse", got)
		}
		if RetryabilityOf(class) != RetryNever {
			t.Fatalf("authorization_unverifiable retryability = %v, want RetryNever (fail closed)", RetryabilityOf(class))
		}
		gateRecordAuthorizationRefusal(t, pool, class, "authorization_id="+gateAuthID+" scope_carrier=absent")

		var action, reason, detail string
		if err := pool.QueryRow(ctx,
			`SELECT action, reason_class, detail FROM signing_request_audit
			  WHERE signing_request_id = $1 ORDER BY audit_id DESC LIMIT 1`,
			gateRequestID).Scan(&action, &reason, &detail); err != nil {
			t.Fatalf("read recorded refusal: %v", err)
		}
		if action != "authorization_refused" || reason != string(class) || detail == "" {
			t.Fatalf("recorded refusal = action %q reason %q detail %q, want authorization_refused/%s with a basis",
				action, reason, detail, class)
		}
		if got := gateSignatureCount(t, pool); got != 0 {
			t.Fatalf("scopeless-grant refusal produced %d signature result(s), want 0", got)
		}
		var signed int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM signing_request_audit WHERE action = 'signed'`).Scan(&signed); err != nil {
			t.Fatalf("count signed audits: %v", err)
		}
		if signed != 0 {
			t.Fatalf("refusal path recorded %d signed audit row(s), want 0", signed)
		}
	})

	t.Run("can_sign off refuses", func(t *testing.T) {
		gateExec(t, pool, `INSERT INTO signer_caller (caller_id, label, can_sign) VALUES ($1, 'gate-off', FALSE)
			ON CONFLICT (caller_id) DO UPDATE SET can_sign = FALSE`, gateCallerID)
		var canSign bool
		if err := pool.QueryRow(ctx,
			`SELECT can_sign FROM signer_caller WHERE caller_id = $1`, gateCallerID).Scan(&canSign); err != nil {
			t.Fatalf("read can_sign: %v", err)
		}
		if canSign {
			t.Fatal("fixture can_sign=false read back true")
		}
		var re *RefusalError
		if !errors.As(PermitSigning(Caller{ID: gateCallerID, CanSign: canSign}), &re) || re.Class != ClassSigningNotPermitted {
			t.Fatalf("can_sign=false permitted signing; got %v", re)
		}
		if got := gateSignatureCount(t, pool); got != 0 {
			t.Fatalf("can_sign off produced %d signature result(s), want 0", got)
		}
	})

	t.Run("upstream 006/007 tables stay byte-identical", func(t *testing.T) {
		gateReset006(t, pool)
		gateSeedRecovery(t, pool, "rec-gate-keep", "reconcile_required", 11)
		gateExec(t, pool, `INSERT INTO reorg_recovery_events
			(chain_id, recovery_id, recovery_seq, event_seq, event) VALUES ($1, 'rec-gate-keep', 11, 1, 'established')`,
			gateChainID)
		gateExec(t, pool, `INSERT INTO indexer_pause
			(chain_id, height, expected_hash, actual_hash, kind) VALUES ($1, 10, $2, $3, 'hash_mismatch')`,
			gateChainID, gateHash("aa"), gateHash("bb"))
		gateSeedGrant(t, pool)

		before := gateUpstreamDigest(t, pool)

		// Every 006/007 read the gate path issues, in the fixed order.
		g := gateRead006(t, pool)
		if !g.HasRecovery || len(g.PauseBases()) != 1 {
			t.Fatalf("scenario read observed %+v, want one pause and the active recovery", g)
		}
		if g.Evaluate(11) != ClassRecoveryPaused {
			t.Fatalf("scenario read evaluated %q, want %q (a pause precedes the recovery branch)", g.Evaluate(11), ClassRecoveryPaused)
		}
		grant, found := gateReadGrant(t, pool, gateAuthID)
		if !found {
			t.Fatal("scenario grant read returned absence")
		}
		req := mustDecode(t, validBody())
		_ = EvaluateGrant(found, &grant, gateCallerID, &req, time.Now().UTC())
		double := &gateBindingDouble{results: map[string]BindingResult{"pi-match": BindingMatches}}
		if _, err := double.ReadBinding(ctx, "pi-match", "at-gate"); err != nil {
			t.Fatalf("binding double: %v", err)
		}
		gateRecordAuthorizationRefusal(t, pool, ClassAuthorizationUnverifiable, "read-only probe")

		after := gateUpstreamDigest(t, pool)
		for _, tbl := range gateUpstreamTables {
			if before[tbl] != after[tbl] {
				t.Fatalf("%s changed across gate reads: before=%s after=%s (009 MUST NOT write upstream state)",
					tbl, before[tbl], after[tbl])
			}
		}
		gateReset006(t, pool)
		gateExec(t, pool, `DELETE FROM withdrawal_authorizations WHERE authorization_id = $1`, gateAuthID)
	})
}
