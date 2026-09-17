//go:build integration

// registry_lifecycle_integration_test.go owns spec task T036 for
// 008-nonce-manager (US5; FR-01; R9; V10; SC-05): the controlled-wallet
// registry lifecycle over a real PostgreSQL, driven through the operator
// carrier (`txharbor nonce-admin register|disable`).
//
// What it proves, per the T036 acceptance text:
//   - `register → disable → re-register` through the real carrier, each
//     committing an `applied` audit and bumping the registry_seq in order
//     (1 -> 2 -> 3) with the final state active;
//   - an active sender's admission binds once, then a disabled sender's NEW
//     admission is refused `sender_disabled` (and even the existing intent's
//     admission is refused, never replayed) while the existing binding facts
//     stay byte-identical;
//   - R9 non-retroactivity: the binding keeps the registry_seq in force at its
//     admission (1) across the later registry changes;
//   - the operation-id replay/conflict semantics hold: an equal op-input
//     returns the recorded outcome with zero writes (the in-tx disable is
//     rolled back), a differing op-input is operation_conflict with zero writes.
//
// The chain view is the scripted JSON-RPC double the sibling integration tests
// own. Every helper this file owns is `rg`-prefixed so it cannot collide with a
// sibling integration test.
package nonce_test

import (
	"bytes"
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

const (
	// rgChain is the test's deployment chain id; rgAuth is the seeded 007
	// authorization the admission binds; rgSender is the controlled wallet.
	rgChain = int64(80041)
	rgAuth  = "wa-rg-1"
)

var rgSender = nonceAddr("e1")

// rgCarrier drives the real `nonce-admin` carrier with the serve env the
// registry actions require (a DSN; the registry path never dials RPC).
type rgCarrier struct {
	deps   app.Deps
	stdout *bytes.Buffer
	stderr *bytes.Buffer
}

// rgNewCarrier builds the carrier over the scratch database. The RPC URL is a
// well-formed placeholder: register/disable take no chain observation.
func rgNewCarrier(dsn string) *rgCarrier {
	env := map[string]string{
		config.EnvPGDSN:                 dsn,
		config.EnvRPCURL:                "http://127.0.0.1:8545",
		config.EnvChainID:               strconv.FormatInt(rgChain, 10),
		config.EnvStartHeight:           "0",
		config.EnvLogStartHeight:        "0",
		config.EnvLogContracts:          nonceAddr("ee"),
		config.EnvDepositStartHeight:    "0",
		config.EnvDepositContracts:      nonceAddr("ee"),
		config.EnvDepositWatchAddresses: nonceAddr("ee"),
		config.EnvConfirmationDepth:     "12",
	}
	var out, errOut bytes.Buffer
	return &rgCarrier{
		deps: app.Deps{
			Getenv: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
			Stdout: &out,
			Stderr: &errOut,
		},
		stdout: &out,
		stderr: &errOut,
	}
}

// run executes one carrier attempt with fresh output buffers.
func (c *rgCarrier) run(t *testing.T, args ...string) int {
	t.Helper()
	c.stdout.Reset()
	c.stderr.Reset()
	return app.NonceAdmin(context.Background(), args, c.deps)
}

// rgRegistry is the comparable registry row fact set.
type rgRegistry struct {
	state string
	seq   int64
}

func rgReadRegistry(t *testing.T, sqlDB *sql.DB) rgRegistry {
	t.Helper()
	var r rgRegistry
	if err := sqlDB.QueryRow(`SELECT state, registry_seq FROM nonce_wallet_registry
		WHERE chain_id = $1 AND sender = $2`, rgChain, rgSender).Scan(&r.state, &r.seq); err != nil {
		t.Fatalf("read registry: %v", err)
	}
	return r
}

// rgBinding is the comparable binding fact set (the sender/nonce/seq identity).
type rgBinding struct {
	bindingID   string
	intentID    string
	chainID     int64
	sender      string
	nonce       string
	state       string
	registrySeq int64
}

func rgReadBinding(t *testing.T, sqlDB *sql.DB, intentID string) rgBinding {
	t.Helper()
	var b rgBinding
	if err := sqlDB.QueryRow(`SELECT binding_id, intent_id, chain_id, sender,
		nonce::text, state, registry_seq FROM nonce_bindings WHERE intent_id = $1`, intentID).
		Scan(&b.bindingID, &b.intentID, &b.chainID, &b.sender, &b.nonce, &b.state, &b.registrySeq); err != nil {
		t.Fatalf("read binding by intent %q: %v", intentID, err)
	}
	return b
}

func rgCountAudits(t *testing.T, sqlDB *sql.DB) int {
	t.Helper()
	var n int
	if err := sqlDB.QueryRow(`SELECT count(*) FROM nonce_ops_audit`).Scan(&n); err != nil {
		t.Fatalf("count audits: %v", err)
	}
	return n
}

// rgWantAudit asserts the committed audit row for one operation id: the
// carrier reports the action/outcome and the audit names the sender subject.
func rgWantAudit(t *testing.T, sqlDB *sql.DB, opID, action string) {
	t.Helper()
	var gotAction, gotOutcome, gotSubject string
	if err := sqlDB.QueryRow(`SELECT action, outcome, subject_id FROM nonce_ops_audit
		WHERE operation_id = $1`, opID).Scan(&gotAction, &gotOutcome, &gotSubject); err != nil {
		t.Fatalf("read audit %q: %v", opID, err)
	}
	if gotAction != action || gotOutcome != string(nonce.AdminApplied) || gotSubject != rgSender {
		t.Fatalf("audit %s = (%s, %s, %s), want (%s, applied, %s)",
			opID, gotAction, gotOutcome, gotSubject, action, rgSender)
	}
}

// rgSeedCallerAuth seeds the 007 caller/authorization the admission binds; the
// registry row is materialized by the carrier's register action.
func rgSeedCallerAuth(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES (1, 'rg-it')`)
	nonceMustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1, 1, $2, $3, $4, 1, 'active')`, rgAuth, rgChain, nonceAddr("cc"), nonceAddr("dd"))
}

// TestNonceRegistryLifecycleIntegration is the T036 V10 acceptance over one
// migrated scratch database and one sender scope.
func TestNonceRegistryLifecycleIntegration(t *testing.T) {
	ctx := context.Background()
	dsn := nonceStartPostgres(t)

	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)
	rgSeedCallerAuth(t, sqlDB)

	pool := replayOpenPool(t, dsn)
	alloc := replayAllocator(t, pool)
	carrier := rgNewCarrier(dsn)

	const (
		rgOpRegister   = "rg-op-register-1"
		rgOpDisable    = "rg-op-disable-1"
		rgOpReregister = "rg-op-reregister-1"
		rgReason       = "registry lifecycle"
	)

	// --- register: fresh insert at registry_seq 1, through the carrier -----
	if code := carrier.run(t, "register", "--operation-id", rgOpRegister,
		"--chain-id", strconv.FormatInt(rgChain, 10), "--sender", rgSender,
		"--operator", "rg-operator", "--reason", rgReason); code != 0 {
		t.Fatalf("nonce-admin register exit = %d, want 0 (stderr %q)", code, carrier.stderr.String())
	}
	if !strings.Contains(carrier.stdout.String(), "outcome=applied") ||
		!strings.Contains(carrier.stdout.String(), "action=registry_register") {
		t.Fatalf("register stdout = %q, want an applied registry_register report", carrier.stdout.String())
	}
	if reg := rgReadRegistry(t, sqlDB); reg.state != nonce.RegistryActive || reg.seq != 1 {
		t.Fatalf("registry after register = %+v, want active/seq 1", reg)
	}

	// --- admission while active: exactly one binding at nonce 0 ------------
	base := nonce.AllocationRequest{
		IntentID: "rg-intent-keep", ChainID: rgChain, Sender: rgSender, AuthorizationID: rgAuth,
	}
	first, outcome, err := alloc.Allocate(ctx, base)
	if err != nil || outcome != nonce.OutcomeAllocated || first == nil {
		t.Fatalf("active admission = (%+v, %q, %v), want a new binding", first, outcome, err)
	}
	pinned := rgReadBinding(t, sqlDB, base.IntentID)
	if pinned.nonce != "0" || pinned.state != nonce.StateAllocated || pinned.registrySeq != 1 {
		t.Fatalf("admitted binding = %+v, want nonce 0/allocated/registry_seq 1", pinned)
	}

	// --- disable: state flips, seq bumps to 2, through the carrier ---------
	if code := carrier.run(t, "disable", "--operation-id", rgOpDisable,
		"--chain-id", strconv.FormatInt(rgChain, 10), "--sender", rgSender,
		"--operator", "rg-operator", "--reason", rgReason); code != 0 {
		t.Fatalf("nonce-admin disable exit = %d, want 0 (stderr %q)", code, carrier.stderr.String())
	}
	if !strings.Contains(carrier.stdout.String(), "outcome=applied") ||
		!strings.Contains(carrier.stdout.String(), "action=registry_disable") {
		t.Fatalf("disable stdout = %q, want an applied registry_disable report", carrier.stdout.String())
	}
	if reg := rgReadRegistry(t, sqlDB); reg.state != nonce.RegistryDisabled || reg.seq != 2 {
		t.Fatalf("registry after disable = %+v, want disabled/seq 2", reg)
	}

	// --- disabled sender: a NEW admission is refused sender_disabled --------
	newIntent := base
	newIntent.IntentID = "rg-intent-new"
	if b, o, e := alloc.Allocate(ctx, newIntent); !nonce.IsOutcome(e, nonce.OutcomeSenderDisabled) ||
		o != nonce.OutcomeSenderDisabled || b != nil {
		t.Fatalf("disabled new admission = (%+v, %q, %v), want sender_disabled with no binding", b, o, e)
	}
	// The registry gate precedes the intent re-read: the EXISTING intent's
	// admission is refused too, never replayed.
	if b, o, e := alloc.Allocate(ctx, base); !nonce.IsOutcome(e, nonce.OutcomeSenderDisabled) ||
		o != nonce.OutcomeSenderDisabled || b != nil {
		t.Fatalf("disabled existing-intent admission = (%+v, %q, %v), want sender_disabled", b, o, e)
	}
	// Existing binding facts are byte-identical; the refused attempts wrote no
	// second binding and no new observation/scope rows.
	if got := rgReadBinding(t, sqlDB, base.IntentID); got != pinned {
		t.Fatalf("binding changed during disable refusals: %+v -> %+v", pinned, got)
	}
	var bindings int
	if err := sqlDB.QueryRow(`SELECT count(*) FROM nonce_bindings WHERE chain_id = $1 AND sender = $2`,
		rgChain, rgSender).Scan(&bindings); err != nil {
		t.Fatalf("count scope bindings: %v", err)
	}
	if bindings != 1 {
		t.Fatalf("scope bindings after refusals = %d, want exactly 1", bindings)
	}

	// --- re-register: active again, seq bumps to 3 --------------------------
	if code := carrier.run(t, "register", "--operation-id", rgOpReregister,
		"--chain-id", strconv.FormatInt(rgChain, 10), "--sender", rgSender,
		"--operator", "rg-operator", "--reason", rgReason); code != 0 {
		t.Fatalf("nonce-admin re-register exit = %d, want 0 (stderr %q)", code, carrier.stderr.String())
	}
	if !strings.Contains(carrier.stdout.String(), "outcome=applied") ||
		!strings.Contains(carrier.stdout.String(), "action=registry_register") {
		t.Fatalf("re-register stdout = %q, want an applied registry_register report", carrier.stdout.String())
	}
	if reg := rgReadRegistry(t, sqlDB); reg.state != nonce.RegistryActive || reg.seq != 3 {
		t.Fatalf("registry after re-register = %+v, want active/seq 3", reg)
	}

	// --- R9 non-retroactivity: the binding stays pinned at its admission seq 1
	if got := rgReadBinding(t, sqlDB, base.IntentID); got != pinned {
		t.Fatalf("binding moved with the registry: %+v -> %+v", pinned, got)
	}

	// --- registry_seq history + audit rows complete -------------------------
	rgWantAudit(t, sqlDB, rgOpRegister, nonce.AdminActionRegistryRegister)
	rgWantAudit(t, sqlDB, rgOpDisable, nonce.AdminActionRegistryDisable)
	rgWantAudit(t, sqlDB, rgOpReregister, nonce.AdminActionRegistryRegister)
	if got := rgCountAudits(t, sqlDB); got != 3 {
		t.Fatalf("audit rows = %d, want exactly 3 (one per recorded attempt)", got)
	}

	// --- operation-id replay: equal input returns the recorded outcome ------
	runner := nonce.NewAdminRunner(pool, nil)
	beforeRows := replayCountRows(t, sqlDB)
	beforeAudits := rgCountAudits(t, sqlDB)
	disableReq := nonce.AdminRequest{
		Action:      nonce.AdminActionRegistryDisable,
		OperationID: rgOpDisable,
		ChainID:     rgChain,
		Sender:      rgSender,
		Operator:    "rg-operator",
		Reason:      rgReason,
	}
	res, err := runner.Run(ctx, disableReq)
	if err != nil || res.Outcome != nonce.AdminApplied || res.Action != nonce.AdminActionRegistryDisable {
		t.Fatalf("equal-input replay = (%+v, %v), want the recorded applied registry_disable", res, err)
	}
	// The replay re-ran the disable UPDATE in-tx but the operation-id 23505
	// rolled the whole transaction back: the registry stays active/seq 3.
	if reg := rgReadRegistry(t, sqlDB); reg.state != nonce.RegistryActive || reg.seq != 3 {
		t.Fatalf("replay double-applied: registry = %+v, want active/seq 3", reg)
	}
	if got := replayCountRows(t, sqlDB); got != beforeRows {
		t.Fatalf("replay wrote rows: %+v, want %+v", got, beforeRows)
	}
	if got := rgCountAudits(t, sqlDB); got != beforeAudits {
		t.Fatalf("replay wrote an audit row: %d, want %d", got, beforeAudits)
	}

	// --- operation-id conflict: differing input, zero writes ----------------
	conflictReq := disableReq
	conflictReq.Action = nonce.AdminActionRegistryRegister
	conflictReq.Reason = "a differing op-input"
	if _, err := runner.Run(ctx, conflictReq); !nonce.IsOutcome(err, nonce.OutcomeOperationConflict) {
		t.Fatalf("differing-input attempt = %v, want operation_conflict", err)
	}
	if reg := rgReadRegistry(t, sqlDB); reg.state != nonce.RegistryActive || reg.seq != 3 {
		t.Fatalf("conflict changed the registry: %+v, want active/seq 3", reg)
	}
	if got := replayCountRows(t, sqlDB); got != beforeRows {
		t.Fatalf("conflict wrote rows: %+v, want %+v", got, beforeRows)
	}
	if got := rgCountAudits(t, sqlDB); got != beforeAudits {
		t.Fatalf("conflict wrote an audit row: %d, want %d", got, beforeAudits)
	}
	if got := rgReadBinding(t, sqlDB, base.IntentID); got != pinned {
		t.Fatalf("binding changed across replay/conflict: %+v -> %+v", pinned, got)
	}
}
