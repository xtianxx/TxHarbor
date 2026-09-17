//go:build integration

// delivery_integration_test.go owns spec task T027 for 009-signer-service: the
// V7 delivery/unknown matrix on a real, isolated PostgreSQL (quickstart V7;
// FR-17/FR-23; contracts/persistence.md §§2/5/6, contracts/gates.md §3;
// SC-06/SC-08). It drives the T034 `delivery.go` protocol directly:
//
//   - fixed lock order: the 008 scope-row FOR SHARE is taken before any gate
//     read (observable with a scripted scope-locker + binding-double recorder);
//   - one protected region: admission INSERT + bytes write + delivered marker
//     UPDATE commit together — a failed write leaves no admitted row behind
//     (no admit-now-write-later split);
//   - write failure / partial write + RST / session loss → `unknown`, never
//     "nothing delivered" (the bytes may be out);
//   - post-write pre-COMMIT crash (injected marker failure) → `unknown_reconcile`
//     with the bytes already written;
//   - byte-identical redelivery re-passes current gates: revoked/expired grant
//     and an 006 pause block even the same bytes; can_sign off does too;
//   - V7-step-6: missing-marker restart re-gates and redelivers identical
//     bytes; a committed marker is never re-gated/un-delivered; overlap
//     accounting records a gate that flipped after the region; audit honesty
//     (unknown stays unknown, no backfilled "delivered"); and no re-signing.
//
// The delivery "bytes write" is an abstract byte sink; 009 never broadcasts,
// so a source assertion pins that delivery.go reaches no RPC/dial package.
package signer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// dlvOrder records the order of lock-taking steps across the transaction the
// delivery protocol runs (scope locker vs binding read).
type dlvOrder struct {
	mu     sync.Mutex
	events []string
}

func (o *dlvOrder) add(e string) {
	o.mu.Lock()
	o.events = append(o.events, e)
	o.mu.Unlock()
}

func (o *dlvOrder) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.events...)
}

// dlvScope returns a scripted ScopeLocker that records when the 008 scope-row
// FOR SHARE is taken; it never touches the DB (nonce_scope_state is 008-owned
// and absent in this tree until T028 wires the live adapter).
type dlvScope struct {
	order *dlvOrder
	calls int
}

func (s *dlvScope) LockScope(_ context.Context, _ pgx.Tx, _ int64, _ string) error {
	if s.order != nil {
		s.order.add("scope")
	}
	s.calls++
	return nil
}

// dlvBinding is a scripted contract-shape 008 double: it returns results[i] on
// the i-th read (clamped to the last entry) and records each read's position.
type dlvBinding struct {
	mu      sync.Mutex
	results []BindingResult
	idx     int
	order   *dlvOrder
}

func (d *dlvBinding) ReadBinding(_ context.Context, _, _ string) (BindingResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.order != nil {
		d.order.add("binding")
	}
	i := d.idx
	d.idx++
	if len(d.results) == 0 {
		return BindingMatches, nil
	}
	if i >= len(d.results) {
		i = len(d.results) - 1
	}
	return d.results[i], nil
}

// dlvSink is the abstract byte sink (the local transport): it records every
// payload and optionally runs a scripted fault on write.
type dlvSink struct {
	mu       sync.Mutex
	payloads [][]byte
	fn       func(ctx context.Context, payload []byte) error
}

func (s *dlvSink) WriteDelivery(ctx context.Context, payload []byte) error {
	s.mu.Lock()
	s.payloads = append(s.payloads, append([]byte(nil), payload...))
	s.mu.Unlock()
	if s.fn != nil {
		return s.fn(ctx, payload)
	}
	return nil
}

func (s *dlvSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.payloads)
}

func (s *dlvSink) last() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.payloads) == 0 {
		return nil
	}
	return s.payloads[len(s.payloads)-1]
}

// dlvFixture is one durable post-COMMIT signable-and-deliverable request: a
// committed `signed` row + `signature_results` row, its own caller and 007
// grant (unique authorization per fixture so the anchor index and FK scope
// never collide across subtests).
type dlvFixture struct {
	callerID  int64
	authzID   string
	requestID string
	rowID     int64
	signature string
	txHash    string
}

// dlvSeed builds a fresh, isolated fixture. n only varies the identifiers.
func dlvSeed(t *testing.T, pool *pgxpool.Pool, n int) *dlvFixture {
	t.Helper()
	ctx := context.Background()
	callerID := int64(8000 + n)
	authzID := "wa-dlv-" + strconv.Itoa(n)
	requestID := "sr-dlv-" + strconv.Itoa(n)

	gateExec(t, pool, `INSERT INTO caller (caller_id, can_create) VALUES ($1, TRUE)
		ON CONFLICT (caller_id) DO NOTHING`, callerID)
	gateExec(t, pool, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state, expires_at, supplied_by)
		VALUES ($1, $2, $3, $4, $5, $6, 'active', now() + interval '1 hour', 'dlv-test')`,
		authzID, callerID, gateChainID, gateAsset, gateRecipient, gateAmount)
	if _, err := IssueCredential(ctx, pool, callerID, "dlv-"+strconv.Itoa(n)); err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}

	body := strings.Replace(submitGrantBody(), `"authorization_id": "`+gateAuthID+`"`,
		`"authorization_id": "`+authzID+`"`, 1)
	body = strings.Replace(body, `"signing_request_id": "sr-7f3a"`,
		`"signing_request_id": "`+requestID+`"`, 1)
	body = strings.Replace(body, `"attempt_id": "at-9c02"`,
		`"attempt_id": "at-dlv-`+strconv.Itoa(n)+`"`, 1)

	rowID := submitSeedRow(t, pool, callerID, body, string(StateSigned))
	signature := signerMigrationSignature("ab")
	txHash := signerMigrationHash(fmt.Sprintf("%02x", n))
	gateExec(t, pool, `INSERT INTO signature_results (signing_request_row, signature, tx_hash)
		VALUES ($1, $2, $3)`, rowID, signature, txHash)
	dlvSyncFingerprint(t, pool, rowID, authzID)

	return &dlvFixture{callerID: callerID, authzID: authzID, requestID: requestID,
		rowID: rowID, signature: signature, txHash: txHash}
}

// dlvSyncFingerprint pins the row's stored `authz:v1` fingerprint to the real
// observed grant, so the delivery fingerprint equality is a real check (the
// generic seed writes a placeholder digest).
func dlvSyncFingerprint(t *testing.T, pool *pgxpool.Pool, rowID int64, authzID string) {
	t.Helper()
	var (
		callerID, chainID        int64
		asset, recipient, amount string
		state                    string
		expires                  *time.Time
	)
	if err := pool.QueryRow(context.Background(),
		`SELECT caller_id, chain_id, asset, recipient, amount::text, state, expires_at
		   FROM withdrawal_authorizations WHERE authorization_id = $1`, authzID).Scan(
		&callerID, &chainID, &asset, &recipient, &amount, &state, &expires); err != nil {
		t.Fatalf("read grant for fingerprint: %v", err)
	}
	g := &AuthzGrant{CallerID: callerID, ChainID: chainID, Asset: asset, Recipient: recipient,
		State: state, ExpiresAt: expires}
	if v, ok := new(big.Int).SetString(amount, 10); ok {
		g.Amount = v
	}
	fp := strings.TrimPrefix(g.Fingerprint(), AuthzFingerprintDomain+":")
	gateExec(t, pool, `UPDATE signing_requests SET authorization_fingerprint = $1, authorization_state = $2
		WHERE id = $3`, fp, state, rowID)
}

// dlvSeedScope inserts the PB carrier row for a fixture at the given version,
// deriving identity columns from the seeded request so the row is coherent
// (delivery re-checks the version, not the scope content).
func dlvSeedScope(t *testing.T, pool *pgxpool.Pool, f *dlvFixture, version int64) {
	t.Helper()
	gateExec(t, pool, `INSERT INTO withdrawal_authorization_scopes
		(authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas,
		 fee_max_priority, allows_fee_replacement, authorization_version, attested_by)
		SELECT authorization_id, intent_id, signing_request_id, sender, 0, 0, 0, TRUE, $2, 'dlv-test'
		  FROM signing_requests WHERE id = $1`, f.rowID, version)
}

func dlvDeps(pool *pgxpool.Pool, binding BindingReader, scope ScopeLocker) DeliveryDeps {
	return DeliveryDeps{DB: pool, Binding: binding, ScopeLock: scope}
}

// dlvAdmissionCount counts the persisted admission rows for a request.
func dlvAdmissionCount(t *testing.T, pool *pgxpool.Pool, rowID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM delivery_admissions WHERE signing_request_row = $1`, rowID).Scan(&n); err != nil {
		t.Fatalf("count delivery_admissions: %v", err)
	}
	return n
}

// dlvDeliveredMarker reports whether a committed `delivered` marker exists.
func dlvDeliveredMarker(t *testing.T, pool *pgxpool.Pool, rowID int64) bool {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM delivery_admissions
		   WHERE signing_request_row = $1 AND verdict = 'delivered')`, rowID).Scan(&exists); err != nil {
		t.Fatalf("read delivered marker: %v", err)
	}
	return exists
}

// dlvAssertNoResign asserts the persisted signature for the fixture is
// unchanged: delivery never re-signs (signature_results_pkey untouched).
func dlvAssertNoResign(t *testing.T, pool *pgxpool.Pool, f *dlvFixture) {
	t.Helper()
	var sig, txHash string
	var rows int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*), min(signature), min(tx_hash) FROM signature_results WHERE signing_request_row = $1`,
		f.rowID).Scan(&rows, &sig, &txHash); err != nil {
		t.Fatalf("read persisted result: %v", err)
	}
	if rows != 1 || sig != f.signature || txHash != f.txHash {
		t.Fatalf("persisted result = rows %d sig %q tx %q, want the original %q/%q",
			rows, sig, txHash, f.signature, f.txHash)
	}
}

// dlvLastAudit returns the most recent audit row for the request.
func dlvLastAudit(t *testing.T, pool *pgxpool.Pool, requestID string) (action, reason, detail string) {
	t.Helper()
	if err := pool.QueryRow(context.Background(),
		`SELECT action, reason_class, detail FROM signing_request_audit
		  WHERE signing_request_id = $1 ORDER BY audit_id DESC LIMIT 1`, requestID).Scan(
		&action, &reason, &detail); err != nil {
		t.Fatalf("read audit: %v", err)
	}
	return action, reason, detail
}

func dlvPayloadFacts(t *testing.T, payload []byte) map[string]string {
	t.Helper()
	var m map[string]string
	if err := json.Unmarshal(payload, &m); err != nil {
		t.Fatalf("delivery payload is not JSON: %v", err)
	}
	return m
}

// dlvAssertUnknown is the V7 "never nothing delivered" assertion: the outcome
// is reported unknown, no admitted row was left behind (no split), the durable
// result stands, and the delivered marker was never written.
func dlvAssertUnknown(t *testing.T, pool *pgxpool.Pool, f *dlvFixture, res *DeliveryResult, err error) {
	t.Helper()
	if res == nil || res.Verdict != VerdictUnknownReconcile || res.Class != ClassOutcomeUnknown {
		t.Fatalf("outcome = %+v, want unknown_reconcile/outcome_unknown", res)
	}
	signerAuthRefusal(t, err, ClassOutcomeUnknown)
	if got := dlvAdmissionCount(t, pool, f.rowID); got != 0 {
		t.Fatalf("unknown outcome left %d admission row(s), want 0 (the admitted insert rolled back)", got)
	}
	if dlvDeliveredMarker(t, pool, f.rowID) {
		t.Fatal("unknown outcome wrote a delivered marker")
	}
	action, reason, _ := dlvLastAudit(t, pool, f.requestID)
	if action != "delivery_unknown" || reason != string(ClassOutcomeUnknown) {
		t.Fatalf("audit = %s/%s, want delivery_unknown/outcome_unknown", action, reason)
	}
	dlvAssertNoResign(t, pool, f)
}

// TestSignerDeliveryV7 drives the T027 acceptance surface against T034.
func TestSignerDeliveryV7(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	n := 0
	next := func() *dlvFixture { n++; return dlvSeed(t, pool, n) }

	t.Run("admission insert bytes write and marker commit in one region", func(t *testing.T) {
		gateReset006(t, pool)
		f := next()
		sink := &dlvSink{}
		order := &dlvOrder{}
		deps := dlvDeps(pool,
			&dlvBinding{results: []BindingResult{BindingMatches}, order: order},
			&dlvScope{order: order})

		res, err := Deliver(ctx, deps, Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
		if err != nil {
			t.Fatalf("Deliver: %v", err)
		}
		if res.Verdict != VerdictDelivered || res.AttemptSeq != 1 {
			t.Fatalf("result = %+v, want delivered/attempt 1", res)
		}
		if sink.count() != 1 {
			t.Fatalf("sink received %d payloads, want 1", sink.count())
		}
		facts := dlvPayloadFacts(t, sink.last())
		if facts["signature"] != f.signature || facts["tx_hash"] != f.txHash ||
			facts["signing_request_id"] != f.requestID || facts["delivery"] != "delivered" {
			t.Fatalf("payload facts = %v, want the persisted signature/hash facts", facts)
		}

		var verdict, bindingClass, pauseBasis string
		var canSign bool
		var deliveredAt *time.Time
		if err := pool.QueryRow(ctx,
			`SELECT verdict, binding_class, pause_basis, can_sign, delivered_at
			   FROM delivery_admissions WHERE signing_request_row = $1`, f.rowID).Scan(
			&verdict, &bindingClass, &pauseBasis, &canSign, &deliveredAt); err != nil {
			t.Fatalf("read admission: %v", err)
		}
		if verdict != string(VerdictDelivered) || bindingClass != "matches" || pauseBasis != "none" ||
			!canSign || deliveredAt == nil {
			t.Fatalf("admission = %s/%s/%s can_sign=%v delivered_at=%v", verdict, bindingClass, pauseBasis, canSign, deliveredAt)
		}
		if action, _, _ := dlvLastAudit(t, pool, f.requestID); action != "delivery_admitted" {
			t.Fatalf("audit action = %s, want delivery_admitted", action)
		}
		if ev := order.snapshot(); len(ev) < 2 || ev[0] != "scope" || ev[1] != "binding" {
			t.Fatalf("lock order = %v, want the 008 scope-row FOR SHARE before any gate read", ev)
		}
		dlvAssertNoResign(t, pool, f)
	})

	t.Run("write failure is unknown never nothing delivered", func(t *testing.T) {
		gateReset006(t, pool)
		f := next()
		sink := &dlvSink{fn: func(context.Context, []byte) error { return errors.New("write failed") }}
		deps := dlvDeps(pool, &dlvBinding{results: []BindingResult{BindingMatches}}, nil)

		res, err := Deliver(ctx, deps, Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
		if sink.count() != 1 {
			t.Fatalf("sink received %d payloads, want the bytes to have been attempted", sink.count())
		}
		dlvAssertUnknown(t, pool, f, res, err)
	})

	t.Run("partial write then RST is unknown", func(t *testing.T) {
		gateReset006(t, pool)
		f := next()
		sink := &dlvSink{fn: func(_ context.Context, payload []byte) error {
			if len(payload) < 2 {
				return errors.New("short")
			}
			return errors.New("connection reset by peer")
		}}
		deps := dlvDeps(pool, &dlvBinding{results: []BindingResult{BindingMatches}}, nil)

		res, err := Deliver(ctx, deps, Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
		if sink.count() != 1 {
			t.Fatalf("sink received %d payloads, want a partial write attempt", sink.count())
		}
		dlvAssertUnknown(t, pool, f, res, err)
	})

	t.Run("session loss mid-region is unknown", func(t *testing.T) {
		gateReset006(t, pool)
		f := next()
		cancelCtx, cancel := context.WithCancel(ctx)
		sink := &dlvSink{fn: func(context.Context, []byte) error {
			// Kill the session after the bytes leave: the marker UPDATE fails
			// and the region rolls back — the outcome is unknown, not "nothing".
			cancel()
			return nil
		}}
		deps := dlvDeps(pool, &dlvBinding{results: []BindingResult{BindingMatches}}, nil)

		res, err := Deliver(cancelCtx, deps, Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
		dlvAssertUnknown(t, pool, f, res, err)
	})

	t.Run("post-write pre-commit crash is unknown_reconcile", func(t *testing.T) {
		gateReset006(t, pool)
		f := next()
		gateExec(t, pool, `CREATE OR REPLACE FUNCTION dlv_fail_marker() RETURNS trigger AS $$
			BEGIN RAISE EXCEPTION 'injected pre-COMMIT failure'; END; $$ LANGUAGE plpgsql`)
		gateExec(t, pool, `CREATE TRIGGER dlv_fail_marker_trg BEFORE UPDATE ON delivery_admissions
			FOR EACH ROW EXECUTE FUNCTION dlv_fail_marker()`)
		t.Cleanup(func() {
			gateExec(t, pool, `DROP TRIGGER IF EXISTS dlv_fail_marker_trg ON delivery_admissions`)
			gateExec(t, pool, `DROP FUNCTION IF EXISTS dlv_fail_marker()`)
		})

		sink := &dlvSink{}
		deps := dlvDeps(pool, &dlvBinding{results: []BindingResult{BindingMatches}}, nil)
		res, err := Deliver(ctx, deps, Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
		if sink.count() != 1 {
			t.Fatalf("sink received %d payloads; bytes may be out and must still be reported unknown", sink.count())
		}
		dlvAssertUnknown(t, pool, f, res, err)
	})

	t.Run("missing-marker restart regates and committed marker never regates", func(t *testing.T) {
		gateReset006(t, pool)
		f := next()
		sink := &dlvSink{}
		deps := dlvDeps(pool, &dlvBinding{results: []BindingResult{BindingMatches}}, nil)

		// Signed + result, no delivered marker: identified as unknown by durable
		// rows only; same-identity retry re-gates and redelivers identical bytes.
		res, err := Deliver(ctx, deps, Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
		if err != nil || res.Verdict != VerdictDelivered {
			t.Fatalf("missing-marker restart = %+v / %v, want delivered", res, err)
		}
		first := append([]byte(nil), sink.last()...)

		// Revoke the grant; an already-committed marker is never re-gated, so the
		// idempotent redelivery stays delivered with byte-identical bytes.
		gateExec(t, pool, `UPDATE withdrawal_authorizations SET state = 'revoked' WHERE authorization_id = $1`, f.authzID)
		res, err = Deliver(ctx, deps, Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
		if err != nil || res.Verdict != VerdictDelivered {
			t.Fatalf("already-delivered redelivery = %+v / %v, want delivered (never re-gated)", res, err)
		}
		if sink.count() != 2 || string(sink.last()) != string(first) {
			t.Fatalf("redelivery bytes differ or sink count %d; want byte-identical re-delivery", sink.count())
		}
		if got := dlvAdmissionCount(t, pool, f.rowID); got != 1 {
			t.Fatalf("already-delivered redelivery recorded %d admissions, want the original 1", got)
		}
		dlvAssertNoResign(t, pool, f)
	})

	t.Run("revoked grant blocks byte-identical redelivery", func(t *testing.T) {
		gateReset006(t, pool)
		f := next()
		// First attempt wins a write failure: unknown, no marker.
		failSink := &dlvSink{fn: func(context.Context, []byte) error { return errors.New("lost") }}
		deps := dlvDeps(pool, &dlvBinding{results: []BindingResult{BindingMatches}}, nil)
		if res, err := Deliver(ctx, deps, Caller{ID: f.callerID, CanSign: true}, f.requestID, failSink); res.Verdict != VerdictUnknownReconcile {
			t.Fatalf("first attempt = %+v / %v, want unknown", res, err)
		}
		gateExec(t, pool, `UPDATE withdrawal_authorizations SET state = 'revoked' WHERE authorization_id = $1`, f.authzID)

		sink := &dlvSink{}
		res, err := Deliver(ctx, deps, Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
		if res == nil || res.Verdict != VerdictBlocked {
			t.Fatalf("revoked redelivery = %+v / %v, want blocked", res, err)
		}
		signerAuthRefusal(t, err, ClassSignatureWithheld)
		if sink.count() != 0 {
			t.Fatalf("blocked attempt wrote %d payload(s), want 0 bytes", sink.count())
		}
		var verdict, reason, authState string
		if err := pool.QueryRow(ctx,
			`SELECT verdict, reason, authorization_state FROM delivery_admissions
			   WHERE signing_request_row = $1`, f.rowID).Scan(&verdict, &reason, &authState); err != nil {
			t.Fatalf("read blocked admission: %v", err)
		}
		if verdict != string(VerdictBlocked) || reason != string(ClassSignatureWithheld) || authState != "revoked" {
			t.Fatalf("blocked admission = %s/%s auth=%s, want blocked/signature_withheld/revoked", verdict, reason, authState)
		}
		if action, _, detail := dlvLastAudit(t, pool, f.requestID); action != "delivery_blocked" || !strings.Contains(detail, "authorization_revoked") {
			t.Fatalf("audit = %s %q, want delivery_blocked recording authorization_revoked", action, detail)
		}
		dlvAssertNoResign(t, pool, f)
	})

	t.Run("scope version change between submit and delivery blocks", func(t *testing.T) {
		gateReset006(t, pool)
		deps := dlvDeps(pool, &dlvBinding{results: []BindingResult{BindingMatches}}, nil)

		// The persisted submit-time snapshot and the live scope agree: the H2
		// path admits exactly as the fingerprint-only path did.
		matching := next()
		gateExec(t, pool, `UPDATE signing_requests SET authorization_version = 1 WHERE id = $1`, matching.rowID)
		dlvSeedScope(t, pool, matching, 1)
		sink := &dlvSink{}
		res, err := Deliver(ctx, deps, Caller{ID: matching.callerID, CanSign: true}, matching.requestID, sink)
		if err != nil || res.Verdict != VerdictDelivered || sink.count() != 1 {
			t.Fatalf("matching-version delivery = %+v / %v sink=%d, want delivered", res, err, sink.count())
		}
		dlvAssertNoResign(t, pool, matching)

		// Revoke-then-resupply between submit and delivery bumps the scope
		// version while the grant row (and its fingerprint) is untouched: only
		// the H2 equality can block. The grant state/validity re-check still
		// ran first and independently.
		bumped := next()
		gateExec(t, pool, `UPDATE signing_requests SET authorization_version = 1 WHERE id = $1`, bumped.rowID)
		dlvSeedScope(t, pool, bumped, 1)
		gateExec(t, pool, `UPDATE withdrawal_authorization_scopes SET authorization_version = 2
			WHERE authorization_id = $1`, bumped.authzID)
		sink = &dlvSink{}
		res, err = Deliver(ctx, deps, Caller{ID: bumped.callerID, CanSign: true}, bumped.requestID, sink)
		if res == nil || res.Verdict != VerdictBlocked {
			t.Fatalf("version-bumped redelivery = %+v / %v, want blocked", res, err)
		}
		signerAuthRefusal(t, err, ClassSignatureWithheld)
		if sink.count() != 0 {
			t.Fatalf("version-bumped attempt wrote %d payload(s), want 0 bytes", sink.count())
		}
		var verdict, reason string
		if err := pool.QueryRow(ctx,
			`SELECT verdict, reason FROM delivery_admissions WHERE signing_request_row = $1`,
			bumped.rowID).Scan(&verdict, &reason); err != nil {
			t.Fatalf("read version-blocked admission: %v", err)
		}
		if verdict != string(VerdictBlocked) || reason != string(ClassSignatureWithheld) {
			t.Fatalf("version-blocked admission = %s/%s, want blocked/signature_withheld", verdict, reason)
		}
		if _, _, detail := dlvLastAudit(t, pool, bumped.requestID); !strings.Contains(detail, "authorization_invalid") {
			t.Fatalf("audit detail %q missing authorization_invalid", detail)
		}
		dlvAssertNoResign(t, pool, bumped)

		// A scope appearing against a NULL submit-time snapshot never compares
		// equal: the default-version trap is closed.
		appeared := next()
		dlvSeedScope(t, pool, appeared, 1)
		sink = &dlvSink{}
		res, err = Deliver(ctx, deps, Caller{ID: appeared.callerID, CanSign: true}, appeared.requestID, sink)
		if res == nil || res.Verdict != VerdictBlocked || sink.count() != 0 {
			t.Fatalf("appearing-scope delivery = %+v / %v sink=%d, want blocked with 0 bytes", res, err, sink.count())
		}
		signerAuthRefusal(t, err, ClassSignatureWithheld)
		dlvAssertNoResign(t, pool, appeared)
	})

	t.Run("expired grant blocks byte-identical redelivery", func(t *testing.T) {
		gateReset006(t, pool)
		f := next()
		gateExec(t, pool, `UPDATE withdrawal_authorizations
			SET expires_at = now() - interval '1 minute' WHERE authorization_id = $1`, f.authzID)

		sink := &dlvSink{}
		deps := dlvDeps(pool, &dlvBinding{results: []BindingResult{BindingMatches}}, nil)
		res, err := Deliver(ctx, deps, Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
		if res == nil || res.Verdict != VerdictBlocked {
			t.Fatalf("expired redelivery = %+v / %v, want blocked", res, err)
		}
		signerAuthRefusal(t, err, ClassSignatureWithheld)
		if sink.count() != 0 {
			t.Fatalf("expired attempt wrote %d payload(s), want 0 bytes", sink.count())
		}
		if _, _, detail := dlvLastAudit(t, pool, f.requestID); !strings.Contains(detail, "authorization_expired") {
			t.Fatalf("audit detail %q missing authorization_expired", detail)
		}
		dlvAssertNoResign(t, pool, f)
	})

	t.Run("006 pause blocks byte-identical redelivery", func(t *testing.T) {
		gateReset006(t, pool)
		gateExec(t, pool, `INSERT INTO indexer_pause
			(chain_id, height, expected_hash, actual_hash, kind) VALUES ($1, 10, $2, $3, 'hash_mismatch')`,
			gateChainID, gateHash("aa"), gateHash("bb"))
		t.Cleanup(func() { gateReset006(t, pool) })

		f := next()
		sink := &dlvSink{}
		deps := dlvDeps(pool, &dlvBinding{results: []BindingResult{BindingMatches}}, nil)
		res, err := Deliver(ctx, deps, Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
		if res == nil || res.Verdict != VerdictBlocked {
			t.Fatalf("paused redelivery = %+v / %v, want blocked", res, err)
		}
		signerAuthRefusal(t, err, ClassSignatureWithheld)
		var pauseBasis string
		if err := pool.QueryRow(ctx,
			`SELECT pause_basis FROM delivery_admissions WHERE signing_request_row = $1`, f.rowID).Scan(&pauseBasis); err != nil {
			t.Fatalf("read pause basis: %v", err)
		}
		if !strings.Contains(pauseBasis, "indexer_pause") {
			t.Fatalf("pause basis = %q, want indexer_pause", pauseBasis)
		}
		if _, _, detail := dlvLastAudit(t, pool, f.requestID); !strings.Contains(detail, "recovery_paused") {
			t.Fatalf("audit detail %q missing recovery_paused", detail)
		}
		dlvAssertNoResign(t, pool, f)
	})

	t.Run("can_sign off blocks local delivery", func(t *testing.T) {
		gateReset006(t, pool)
		f := next()
		if err := SetCanSign(ctx, pool, f.callerID, false); err != nil {
			t.Fatalf("SetCanSign(false): %v", err)
		}
		t.Cleanup(func() { _ = SetCanSign(context.Background(), pool, f.callerID, true) })

		sink := &dlvSink{}
		deps := dlvDeps(pool, &dlvBinding{results: []BindingResult{BindingMatches}}, nil)
		res, err := Deliver(ctx, deps, Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
		if res == nil || res.Verdict != VerdictBlocked {
			t.Fatalf("can_sign-off delivery = %+v / %v, want blocked", res, err)
		}
		signerAuthRefusal(t, err, ClassSignatureWithheld)
		if sink.count() != 0 {
			t.Fatalf("can_sign-off attempt wrote %d payload(s), want 0 bytes", sink.count())
		}
		var canSign bool
		if err := pool.QueryRow(ctx,
			`SELECT can_sign FROM delivery_admissions WHERE signing_request_row = $1`, f.rowID).Scan(&canSign); err != nil {
			t.Fatalf("read admission can_sign: %v", err)
		}
		if canSign {
			t.Fatal("admission recorded can_sign=true despite the observed false")
		}
		if _, _, detail := dlvLastAudit(t, pool, f.requestID); !strings.Contains(detail, "signing_not_permitted") {
			t.Fatalf("audit detail %q missing signing_not_permitted", detail)
		}
		dlvAssertNoResign(t, pool, f)
	})

	t.Run("overlap accounting records a gate that flipped after the region", func(t *testing.T) {
		gateReset006(t, pool)
		f := next()
		sink := &dlvSink{}
		// Region read matches; the post-COMMIT overlap re-read observes a pause
		// that committed after the handoff (accounting only, never un-delivery).
		deps := dlvDeps(pool, &dlvBinding{results: []BindingResult{BindingMatches, BindingPaused}}, nil)

		res, err := Deliver(ctx, deps, Caller{ID: f.callerID, CanSign: true}, f.requestID, sink)
		if err != nil || res.Verdict != VerdictDelivered {
			t.Fatalf("overlap delivery = %+v / %v, want delivered (accounting never un-delivers)", res, err)
		}
		if !dlvDeliveredMarker(t, pool, f.rowID) {
			t.Fatal("overlap delivery lost its delivered marker")
		}
		action, _, detail := dlvLastAudit(t, pool, f.requestID)
		if action != "delivery_admitted" || !strings.Contains(detail, "overlap=binding_paused") {
			t.Fatalf("overlap audit = %s %q, want delivery_admitted overlap=binding_paused", action, detail)
		}
		dlvAssertNoResign(t, pool, f)
	})
}

// TestSignerDeliveryNoRPCDialImports pins the "009 never broadcasts" boundary
// directly on the delivery source: the bytes write is an abstract sink and the
// file reaches no RPC/dial/broadcast package.
func TestSignerDeliveryNoRPCDialImports(t *testing.T) {
	body, err := os.ReadFile("delivery.go")
	if err != nil {
		t.Fatalf("read delivery.go: %v", err)
	}
	src := string(body)
	for _, forbidden := range []string{
		"github.com/ethereum/go-ethereum/rpc",
		"github.com/ethereum/go-ethereum/ethclient",
		"github.com/ethereum/go-ethereum/accounts/abi/bind",
		"net/rpc",
		"net.Dial",
	} {
		if strings.Contains(src, forbidden) {
			t.Errorf("delivery.go references forbidden RPC/dial package %q", forbidden)
		}
	}
}
