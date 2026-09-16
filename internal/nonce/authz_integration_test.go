//go:build integration

// authz_integration_test.go owns spec task T037 for 008-nonce-manager
// (V11, FR-04/FR-16/FR-17, R10, SC-05/SC-08): the authorization-binding
// fail-closed acceptance over a real PostgreSQL.
//
// Scenarios:
//   - fail-closed: missing / inactive / time-expired / chain-mismatched /
//     unreadable (terminal 'expired' carrier state and a shape-invalid id)
//     authorization → no binding, zero new 008 rows, zero 007 writes;
//   - valid: an active, unexpired, chain-matched authorization binds and the
//     binding stores authorization_id + the deterministic version digest;
//   - boundary: 008 writes zero 007 rows on every path (withdrawal_
//     authorizations snapshot byte-identity) and a retry neither consumes nor
//     extends the authorization (state/expiry/supplied metadata untouched);
//   - FR-16 / SC-08 guard: the presence of only 007 Accepted rows allocates
//     nothing and infers no payment intent.
//
// The admission serializes authorization validity with 007 supply/revoke by
// taking the 007 row under SELECT … FOR SHARE after the scope row and before
// its own-row writes (allocate.go readAuthorizationForShareSQL). The
// revoke-vs-admission race itself is T044's scope and is intentionally NOT
// exercised here; this file only records that the serialization point exists.
//
// 007 rows are seeded with raw SQL (never a withdrawal-writer import); the
// only 008 surface touched is the exported nonce.NewAllocator/Allocate API.
// The PostgreSQL container pattern is reused from migration_integration_test.go.
package nonce_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

const (
	azChain      = int64(80011)
	azOtherChain = int64(80012)
	azCaller     = int64(7)
)

// az* authorization ids: each names one fail-closed or binding scenario.
const (
	azAuthValid         = "az-valid-1"        // active, no expiry, chain match
	azAuthFuture        = "az-valid-future"   // active, future expiry
	azAuthMissing       = "az-absent-1"       // never seeded
	azAuthRevoked       = "az-revoked-1"      // state='revoked'
	azAuthTimeExpired   = "az-expired-time-1" // active, expires_at in the past
	azAuthChainMismatch = "az-mismatch-1"     // seeded on azOtherChain
	azAuthCarrierGone   = "az-unreadable-c-1" // state='expired' (terminal)
	azAuthAccepted      = "az-accepted-1"     // backs the Accepted 007 request
)

var (
	azSender        = nonceAddr("31")
	azAuthAsset     = nonceAddr("a1")
	azAuthRecipient = nonceAddr("b2")
	azHeadHash      = "0x" + strings.Repeat("ab", 32)
)

// azRPC is the scripted JSON-RPC double: a frozen healthy view (latest =
// pending = 0, head 0) so a valid admission classifies consistent at nonce 0.
// The call counter proves malformed input is rejected before any RPC.
type azRPC struct {
	calls int64
}

func (r *azRPC) CallContext(_ context.Context, result any, method string, args ...any) error {
	atomic.AddInt64(&r.calls, 1)
	switch method {
	case "eth_getTransactionCount":
		return json.Unmarshal([]byte(`"0x0"`), result)
	case "eth_getBlockByNumber":
		payload := fmt.Sprintf(`{"number":"0x0","hash":%q}`, azHeadHash)
		return json.Unmarshal([]byte(payload), result)
	default:
		return fmt.Errorf("azRPC: unexpected method %q", method)
	}
}

func azAllocator(t *testing.T, pool *pgxpool.Pool) (*nonce.Allocator, *azRPC) {
	t.Helper()
	rpc := &azRPC{}
	observer := nonce.NewObserver(rpc, nonce.ObserverConfig{
		RPCTimeout:   2 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	})
	return nonce.NewAllocator(pool, observer), rpc
}

func azOpenPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// azSeedAuth inserts one 007 authorization row via raw SQL; expiresSQL is a
// literal SQL expression (NULL / now() ± interval), never a caller value.
func azSeedAuth(t *testing.T, sqlDB *sql.DB, id string, chain int64, state, expiresSQL string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state, expires_at)
		VALUES ($1, $2, $3, $4, $5, 1, $6, `+expiresSQL+`)`,
		id, azCaller, chain, azAuthAsset, azAuthRecipient, state)
}

// azCensus is the comparable whole-008 row census (zero value = empty).
type azCensus struct {
	bindings     int
	events       int
	observations int
	holds        int
	scopes       int
}

func azCensusOf(t *testing.T, sqlDB *sql.DB) azCensus {
	t.Helper()
	var c azCensus
	if err := sqlDB.QueryRow(`SELECT
		(SELECT count(*) FROM nonce_bindings),
		(SELECT count(*) FROM nonce_binding_events),
		(SELECT count(*) FROM nonce_observations),
		(SELECT count(*) FROM nonce_scope_holds),
		(SELECT count(*) FROM nonce_scope_state)`).
		Scan(&c.bindings, &c.events, &c.observations, &c.holds, &c.scopes); err != nil {
		t.Fatalf("count 008 rows: %v", err)
	}
	return c
}

// azAuthSnapshot is the byte-identity projection of every 007 authorization
// row (all columns, timezone-stable, ordered); any 008 write to 007 changes it.
func azAuthSnapshot(t *testing.T, sqlDB *sql.DB) string {
	t.Helper()
	var digest string
	if err := sqlDB.QueryRow(`SELECT coalesce(md5(string_agg(
		authorization_id || '|' || caller_id || '|' || chain_id || '|' || asset || '|' ||
		recipient || '|' || amount::text || '|' || state || '|' ||
		coalesce(extract(epoch from expires_at)::text, '-') || '|' ||
		coalesce(extract(epoch from supplied_at)::text, '-') || '|' || supplied_by,
		E'\n' ORDER BY authorization_id)), '')
		FROM withdrawal_authorizations`).Scan(&digest); err != nil {
		t.Fatalf("snapshot 007 authorizations: %v", err)
	}
	return digest
}

// azRequestSnapshot is the byte-identity projection of every 007 Accepted
// request row.
func azRequestSnapshot(t *testing.T, sqlDB *sql.DB) string {
	t.Helper()
	var digest string
	if err := sqlDB.QueryRow(`SELECT coalesce(md5(string_agg(
		id || '|' || request_id || '|' || caller_id || '|' || idempotency_key || '|' ||
		authorization_id || '|' || chain_id || '|' || asset || '|' || recipient || '|' ||
		amount::text || '|' || status,
		E'\n' ORDER BY id)), '')
		FROM withdrawal_requests`).Scan(&digest); err != nil {
		t.Fatalf("snapshot 007 requests: %v", err)
	}
	return digest
}

type azBindingRow struct {
	bindingID   string
	intentID    string
	nonce       string
	state       string
	authID      string
	authVersion string
}

func azBindingByIntent(t *testing.T, sqlDB *sql.DB, intentID string) azBindingRow {
	t.Helper()
	var b azBindingRow
	if err := sqlDB.QueryRow(`SELECT binding_id, intent_id, nonce::text, state,
		authorization_id, authorization_version
		FROM nonce_bindings WHERE intent_id = $1`, intentID).
		Scan(&b.bindingID, &b.intentID, &b.nonce, &b.state, &b.authID, &b.authVersion); err != nil {
		t.Fatalf("read binding by intent %q: %v", intentID, err)
	}
	return b
}

func azIntentBindingCount(t *testing.T, sqlDB *sql.DB, intentID string) int {
	t.Helper()
	var n int
	if err := sqlDB.QueryRow(`SELECT count(*) FROM nonce_bindings WHERE intent_id = $1`, intentID).
		Scan(&n); err != nil {
		t.Fatalf("count bindings for %q: %v", intentID, err)
	}
	return n
}

// azAuthDigest independently re-derives the documented R10 authorization
// version (domain-tagged v1, newline-separated, SHA-256 lowercase hex) so the
// recorded binding version is pinned to the contract, not to itself.
func azAuthDigest(authorizationID string, chainID int64, asset, recipient, amount, state, expires string) string {
	parts := []string{
		"txharbor:nonce:authorization:v1",
		authorizationID,
		strconv.FormatInt(chainID, 10),
		asset,
		recipient,
		amount,
		state,
		expires,
	}
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// TestNonceAuthzFailClosedAndBindingIntegration is the T037 V11 acceptance.
func TestNonceAuthzFailClosedAndBindingIntegration(t *testing.T) {
	dsn := nonceStartPostgres(t)
	ctx := context.Background()

	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)

	nonceMustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES ($1, 'az-it')`, azCaller)
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry
		(chain_id, sender, state, registry_seq) VALUES ($1, $2, 'active', 1)`, azChain, azSender)

	azSeedAuth(t, sqlDB, azAuthValid, azChain, "active", "NULL")
	azSeedAuth(t, sqlDB, azAuthFuture, azChain, "active", "now() + interval '1 hour'")
	azSeedAuth(t, sqlDB, azAuthRevoked, azChain, "revoked", "NULL")
	azSeedAuth(t, sqlDB, azAuthTimeExpired, azChain, "active", "now() - interval '1 hour'")
	azSeedAuth(t, sqlDB, azAuthChainMismatch, azOtherChain, "active", "NULL")
	azSeedAuth(t, sqlDB, azAuthCarrierGone, azChain, "expired", "NULL")
	azSeedAuth(t, sqlDB, azAuthAccepted, azChain, "active", "NULL")

	// The FR-16 / SC-08 guard subject: an Accepted 007 request linked to an
	// active authorization, seeded raw (no 007 writer imported).
	nonceMustExec(t, sqlDB, `INSERT INTO withdrawal_requests
		(request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 1, 'accepted')`,
		"az-req-1", azCaller, "az-idem-1", azAuthAccepted, azChain, azAuthAsset, azAuthRecipient)

	authBefore := azAuthSnapshot(t, sqlDB)
	reqBefore := azRequestSnapshot(t, sqlDB)

	pool := azOpenPool(t, dsn)
	alloc, rpc := azAllocator(t, pool)

	// --- FR-16 / SC-08: Accepted-only leaves 008 untouched ---------------
	if got := azCensusOf(t, sqlDB); got != (azCensus{}) {
		t.Fatalf("Accepted-only seeding left 008 rows: %+v, want none", got)
	}
	var acceptedBindings int
	if err := sqlDB.QueryRow(`SELECT count(*) FROM nonce_bindings WHERE authorization_id = $1`, azAuthAccepted).
		Scan(&acceptedBindings); err != nil {
		t.Fatalf("count bindings for the Accepted authorization: %v", err)
	}
	if acceptedBindings != 0 {
		t.Fatalf("Accepted authorization produced %d bindings, want 0", acceptedBindings)
	}
	if got := azAuthSnapshot(t, sqlDB); got != authBefore {
		t.Fatal("007 authorizations changed before any admission")
	}
	if got := azRequestSnapshot(t, sqlDB); got != reqBefore {
		t.Fatal("007 requests changed before any admission")
	}
	if got := atomic.LoadInt64(&rpc.calls); got != 0 {
		t.Fatalf("Accepted-only state made %d RPC calls, want 0", got)
	}

	// --- fail-closed: no valid authorization, no binding, zero rows ------
	invalid := []struct {
		name        string
		authID      string
		wantOutcome bool
	}{
		{"missing", azAuthMissing, true},
		{"inactive-revoked", azAuthRevoked, true},
		{"expired-time", azAuthTimeExpired, true},
		{"mismatched-chain", azAuthChainMismatch, true},
		{"unreadable-carrier-expired", azAuthCarrierGone, true},
		{"unreadable-id-shape", "az-unreadable\nid", false},
	}
	for i, tc := range invalid {
		t.Run("fail-closed-"+tc.name, func(t *testing.T) {
			before := azCensusOf(t, sqlDB)
			rpcBefore := atomic.LoadInt64(&rpc.calls)
			req := nonce.AllocationRequest{
				IntentID:        fmt.Sprintf("az-intent-invalid-%02d", i),
				ChainID:         azChain,
				Sender:          azSender,
				AuthorizationID: tc.authID,
			}
			binding, outcome, err := alloc.Allocate(ctx, req)
			if binding != nil {
				t.Fatalf("%s returned binding %+v, want none", tc.name, binding)
			}
			if tc.wantOutcome {
				if outcome != nonce.OutcomeAuthorizationInvalid || !nonce.IsOutcome(err, nonce.OutcomeAuthorizationInvalid) {
					t.Fatalf("%s = (%q, %v), want authorization_invalid", tc.name, outcome, err)
				}
			} else {
				if err == nil || outcome != "" {
					t.Fatalf("%s = (%q, %v), want a plain validation error", tc.name, outcome, err)
				}
				if got := atomic.LoadInt64(&rpc.calls); got != rpcBefore {
					t.Fatalf("%s made an RPC call before validation failed", tc.name)
				}
			}
			if got := azCensusOf(t, sqlDB); got != before {
				t.Fatalf("%s wrote 008 rows: %+v, want %+v", tc.name, got, before)
			}
			if got := azAuthSnapshot(t, sqlDB); got != authBefore {
				t.Fatalf("%s mutated 007 authorizations", tc.name)
			}
		})
	}

	// --- valid: binding stores authorization_id + version digest ---------
	validReq := nonce.AllocationRequest{
		IntentID:        "az-intent-valid",
		ChainID:         azChain,
		Sender:          azSender,
		AuthorizationID: azAuthValid,
	}
	binding, outcome, err := alloc.Allocate(ctx, validReq)
	if err != nil || outcome != nonce.OutcomeAllocated || binding == nil {
		t.Fatalf("valid allocate = (%+v, %q, %v), want a new binding", binding, outcome, err)
	}
	if binding.AuthorizationID != azAuthValid {
		t.Fatalf("binding authorization_id = %q, want %q", binding.AuthorizationID, azAuthValid)
	}
	wantVersion := azAuthDigest(azAuthValid, azChain, azAuthAsset, azAuthRecipient, "1", "active", "")
	if binding.AuthorizationVersion != wantVersion {
		t.Fatalf("binding authorization_version = %q, want %q", binding.AuthorizationVersion, wantVersion)
	}
	persisted := azBindingByIntent(t, sqlDB, validReq.IntentID)
	if persisted.authID != azAuthValid || persisted.authVersion != wantVersion {
		t.Fatalf("persisted authorization = %q/%q, want %q/%q",
			persisted.authID, persisted.authVersion, azAuthValid, wantVersion)
	}
	if persisted.state != nonce.StateAllocated || persisted.nonce != "0" {
		t.Fatalf("persisted binding = state %s/nonce %s, want allocated/0", persisted.state, persisted.nonce)
	}
	if got := azAuthSnapshot(t, sqlDB); got != authBefore {
		t.Fatal("valid allocation mutated 007 authorizations")
	}

	// --- retry neither consumes nor extends the authorization ------------
	replayed, outcome, err := alloc.Allocate(ctx, validReq)
	if err != nil || outcome != nonce.OutcomeReplayed {
		t.Fatalf("retry = (%+v, %q, %v), want replayed", replayed, outcome, err)
	}
	if replayed == nil || replayed.BindingID != binding.BindingID || replayed.Nonce.Cmp(binding.Nonce) != 0 {
		t.Fatalf("retry binding = %+v, want the original %s/%s", replayed, binding.BindingID, binding.Nonce)
	}
	if n := azIntentBindingCount(t, sqlDB, validReq.IntentID); n != 1 {
		t.Fatalf("intent has %d bindings after retry, want 1", n)
	}
	if got := azAuthSnapshot(t, sqlDB); got != authBefore {
		t.Fatal("retry consumed or extended the authorization")
	}

	// --- valid future-expiry authorization also binds --------------------
	futureReq := nonce.AllocationRequest{
		IntentID:        "az-intent-future",
		ChainID:         azChain,
		Sender:          azSender,
		AuthorizationID: azAuthFuture,
	}
	futureBinding, outcome, err := alloc.Allocate(ctx, futureReq)
	if err != nil || outcome != nonce.OutcomeAllocated || futureBinding == nil {
		t.Fatalf("future-expiry allocate = (%+v, %q, %v), want allocated", futureBinding, outcome, err)
	}
	if v := futureBinding.AuthorizationVersion; len(v) != 64 || strings.ToLower(v) != v {
		t.Fatalf("future-expiry version = %q, want 64 lowercase hex", v)
	}
	if got := azAuthSnapshot(t, sqlDB); got != authBefore {
		t.Fatal("future-expiry allocation mutated 007 authorizations")
	}

	// --- 008 wrote zero 007 rows over the whole run ----------------------
	if got := azAuthSnapshot(t, sqlDB); got != authBefore {
		t.Fatal("008 wrote 007 authorization rows during the run")
	}
	if got := azRequestSnapshot(t, sqlDB); got != reqBefore {
		t.Fatal("008 wrote 007 request rows during the run")
	}
}
