//go:build integration

// restart_integration_test.go owns spec task T020 for 009-signer-service: the
// restart/crash recovery surface on a real, isolated PostgreSQL (quickstart V4
// step 4 / V8 step 3; FR-14/FR-15; contracts/persistence.md §5; SC-03/SC-08).
//
// The crash is simulated without killing the test process. A DB whose Begin
// yields a transaction whose Commit durably commits and then reports failure
// models the commit-unknown window (§5): the durable evidence exists, the
// caller never saw success. A fresh pool against the same database then stands
// in for the restarted process, and a same-identity retry must converge on the
// durable outcome — never re-sign, never create a second signature
// (signature_results_pkey), never a different result row.
//
// PB-gate: as in T016/T019, the legal signing path is unreachable until the 007
// scope/version carrier lands (submit.go refuses at EvaluateGrantScope). The
// committed result is therefore produced through the wrapped pool's
// transaction using submit.go's own durable SQL; the retry still runs the real
// Submit path. Nothing here is a legal-path sign-off.
package signer

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// errRstCommitUnknown is the injected post-commit failure that models the
// caller-side unknown outcome: the commit is durable, the response is not.
var errRstCommitUnknown = errors.New("rst: commit outcome unknown")

// rstCommitUnknownDB wraps a DB so every transaction it begins commits durably
// and then reports errRstCommitUnknown (commit-unknown).
type rstCommitUnknownDB struct {
	DB
	err error
}

func (d rstCommitUnknownDB) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := d.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return rstCommitUnknownTx{Tx: tx, err: d.err}, nil
}

// rstCommitUnknownTx forwards every pgx.Tx method to the real transaction and
// overrides only Commit: the commit is durable, the error is injected.
type rstCommitUnknownTx struct {
	pgx.Tx
	err error
}

func (t rstCommitUnknownTx) Commit(ctx context.Context) error {
	if err := t.Tx.Commit(ctx); err != nil {
		return err
	}
	return t.err
}

// rstFreshPool opens a second pool against the same database, standing in for
// the restarted process: fresh connections, no retained state.
func rstFreshPool(t *testing.T, pool *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	fresh, err := pgxpool.New(context.Background(), pool.Config().ConnString())
	if err != nil {
		t.Fatalf("rst: open fresh pool: %v", err)
	}
	t.Cleanup(fresh.Close)
	return fresh
}

// rstSeedRequestTx binds one identity+content row inside an open transaction,
// mirroring submitSeedRow (T016) but on the caller's tx so the commit-unknown
// wrapper owns the COMMIT. It reuses submitSeedRequestSQL so the persisted
// canonical_envelope is exactly what a real retry compares against.
func rstSeedRequestTx(t *testing.T, tx pgx.Tx, callerID int64, body, state string) int64 {
	t.Helper()
	req := mustDecode(t, body)
	env, err := req.CanonicalEnvelope()
	if err != nil {
		t.Fatalf("rst: CanonicalEnvelope: %v", err)
	}
	ch, err := req.ContentHash()
	if err != nil {
		t.Fatalf("rst: ContentHash: %v", err)
	}
	var id int64
	err = tx.QueryRow(context.Background(), submitSeedRequestSQL,
		callerID, req.SigningRequestID, req.AttemptID, req.IntentID, req.BindingRef, int64(req.RecoveryVersion),
		int64(req.ChainID), strings.ToLower(req.Sender), req.Nonce, int(req.TxType),
		strings.ToLower(req.To), req.Value, common.FromHex(req.Data), req.GasLimit,
		req.MaxFeePerGas, req.MaxPriorityFeePerGas, strings.ToLower(req.Asset),
		strings.ToLower(req.Recipient), req.Amount,
		envelopeText(env), ch.Hex(), req.AuthorizationID, strings.Repeat("d", 64),
		testPolicy(t).Version(), state).Scan(&id)
	if err != nil {
		t.Fatalf("rst: seed signing_requests row in tx: %v", err)
	}
	return id
}

// rstSignaturePKey reads the single persisted signature_results row: the
// signature_results_pkey (the request row it is keyed by) and its bytes.
func rstSignaturePKey(t *testing.T, pool *pgxpool.Pool) (int64, string) {
	t.Helper()
	var rowID int64
	var signature string
	if err := pool.QueryRow(context.Background(),
		`SELECT signing_request_row, signature FROM signature_results`).Scan(&rowID, &signature); err != nil {
		t.Fatalf("rst: read signature_results_pkey: %v", err)
	}
	return rowID, signature
}

// rstConstraint reports whether err is a PostgreSQL violation on the named
// constraint — the storage-level proof a second signature was refused by name,
// not by application memory.
func rstConstraint(err error, want string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.ConstraintName == want
}

// TestSignerRestartCommitUnknownConverges is the crash-after-result-COMMIT case
// (§5/§6): the signing transaction commits durably, the committing process
// reports failure (commit-unknown) and its response is lost, then a restarted
// process retries the same identity on a fresh pool. The retry must return the
// identical persisted result row — same signature_results_pkey, one signature,
// never a second set of bytes.
func TestSignerRestartCommitUnknownConverges(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(8101)

	cred, err := IssueCredential(ctx, pool, callerID, "restart-commit-unknown")
	if err != nil {
		t.Fatalf("rst: IssueCredential: %v", err)
	}
	body := submitGrantBody()

	// The submit transaction's durable tail (row → result → state → audit)
	// commits, then the COMMIT reports failure: durable evidence unknown to the
	// caller.
	wrapped := rstCommitUnknownDB{DB: pool, err: errRstCommitUnknown}
	tx, err := wrapped.Begin(ctx)
	if err != nil {
		t.Fatalf("rst: begin commit-unknown tx: %v", err)
	}
	rowID := rstSeedRequestTx(t, tx, callerID, body, string(StateSigned))
	signature, txHash := signerMigrationSignature("ab"), signerMigrationHash("e1")
	if _, err := tx.Exec(ctx, submitResultSQL, rowID, signature, txHash); err != nil {
		t.Fatalf("rst: persist result: %v", err)
	}
	if _, err := tx.Exec(ctx, submitSignedSQL, rowID); err != nil {
		t.Fatalf("rst: mark signed: %v", err)
	}
	if _, err := tx.Exec(ctx, submitAuditSQL, "sr-7f3a", callerID, "signed", "", "restart-post-result-commit"); err != nil {
		t.Fatalf("rst: append audit: %v", err)
	}
	if err := tx.Commit(ctx); !errors.Is(err, errRstCommitUnknown) {
		t.Fatalf("rst: commit error = %v, want the injected commit-unknown sentinel", err)
	}

	// Commit-unknown left the durable result behind: one pkey row, one signature.
	pkey, persisted := rstSignaturePKey(t, pool)
	if pkey != rowID {
		t.Fatalf("rst: signature_results_pkey = %d, want the committed row %d", pkey, rowID)
	}
	if persisted != signature {
		t.Fatalf("rst: persisted signature = %q, want %q", persisted, signature)
	}
	if got := signerAuthSignatureCount(t, pool); got != 1 {
		t.Fatalf("rst: signature_results rows = %d, want 1", got)
	}

	// Restarted process: retry the same identity on a fresh pool against the
	// same database. The dropped response is recovered, never re-signed.
	resp, err := Submit(ctx, submitTestDeps(t, rstFreshPool(t, pool)), cred, []byte(body))
	if err != nil {
		t.Fatalf("rst: same-identity retry after commit-unknown: %v", err)
	}
	if resp.Signature != signature || resp.TxHash != txHash {
		t.Fatalf("rst: retry = %q/%q, want the persisted %q/%q", resp.Signature, resp.TxHash, signature, txHash)
	}

	// Identical result row, one signature, no second bytes.
	afterID, afterSig := rstSignaturePKey(t, pool)
	if afterID != pkey || afterSig != signature {
		t.Fatalf("rst: retry changed the result row: pkey %d→%d signature %q→%q", pkey, afterID, signature, afterSig)
	}
	if got := signerAuthSignatureCount(t, pool); got != 1 {
		t.Fatalf("rst: retry left %d signature_results rows, want 1", got)
	}

	// signature_results_pkey is the storage-level guard: a second signature for
	// the same identity is refused by name.
	_, err = pool.Exec(ctx, `INSERT INTO signature_results (signing_request_row, signature, tx_hash) VALUES ($1, $2, $3)`,
		rowID, signerMigrationSignature("cd"), signerMigrationHash("e2"))
	if err == nil {
		t.Fatal("rst: a second signature_results row for the same identity was accepted")
	}
	if !rstConstraint(err, "signature_results_pkey") {
		t.Fatalf("rst: second-result error = %v, want signature_results_pkey", err)
	}
}

// TestSignerRestartCommitUnknownRefusalConverges exercises the same
// commit-unknown window through the real Submit path on the branch reachable
// today (the PB-gated terminal refusal): the rejecting transaction commits
// durably, COMMIT reports failure, and a same-identity retry on a fresh pool
// must converge on that persisted refusal — never re-drive into a signature.
func TestSignerRestartCommitUnknownRefusalConverges(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(gateCallerID)

	cred, err := IssueCredential(ctx, pool, callerID, "restart-refusal-commit-unknown")
	if err != nil {
		t.Fatalf("rst: IssueCredential: %v", err)
	}
	gateReset006(t, pool)
	gateSeedGrant(t, pool)
	body := submitGrantBody()

	deps := submitTestDeps(t, pool)
	deps.DB = rstCommitUnknownDB{DB: pool, err: errRstCommitUnknown}
	_, err = Submit(ctx, deps, cred, []byte(body))
	signerAuthRefusal(t, err, ClassStorageUnavailable) // unknown commit → retry same identity

	// The terminal refusal committed despite the reported failure.
	var state, class string
	if err := pool.QueryRow(ctx,
		`SELECT state, refusal_class FROM signing_requests WHERE caller_id = $1 AND signing_request_id = $2`,
		callerID, "sr-7f3a").Scan(&state, &class); err != nil {
		t.Fatalf("rst: read persisted refusal: %v", err)
	}
	if state != string(StateRejected) || class != string(ClassAuthorizationUnverifiable) {
		t.Fatalf("rst: persisted row = %q/%q, want rejected/%s", state, class, ClassAuthorizationUnverifiable)
	}

	// Same-identity retry on a fresh pool converges on the persisted refusal.
	_, err = Submit(ctx, submitTestDeps(t, rstFreshPool(t, pool)), cred, []byte(body))
	signerAuthRefusal(t, err, ClassAuthorizationUnverifiable)

	if got := signerAuthSignatureCount(t, pool); got != 0 {
		t.Fatalf("rst: commit-unknown refusal path produced %d signature result(s), want 0", got)
	}
	if got := submitRowCount(t, pool, callerID, "sr-7f3a"); got != 1 {
		t.Fatalf("rst: retry created %d identity rows, want the persisted 1", got)
	}
}

// TestSignerRestartCrashBeforeResultCommit is the crash-before-COMMIT case
// (§5): the signing transaction wrote its result but the process died before
// COMMIT, so nothing durable survives. A same-identity retry must not fabricate
// or re-sign a result: with the identity row committed and no visible result,
// it reports outcome_not_yet_visible and signature count stays 0.
func TestSignerRestartCrashBeforeResultCommit(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(8201)

	cred, err := IssueCredential(ctx, pool, callerID, "restart-crash-pre-commit")
	if err != nil {
		t.Fatalf("rst: IssueCredential: %v", err)
	}
	body := submitGrantBody()
	rowID := submitSeedRow(t, pool, callerID, body, string(StateReceived))

	// The in-flight signing transaction persisted its result, then the process
	// died before COMMIT: the transaction rolls back and no result survives.
	wrapped := rstCommitUnknownDB{DB: pool, err: errRstCommitUnknown}
	tx, err := wrapped.Begin(ctx)
	if err != nil {
		t.Fatalf("rst: begin pre-commit tx: %v", err)
	}
	if _, err := tx.Exec(ctx, submitResultSQL, rowID, signerMigrationSignature("ab"), signerMigrationHash("e1")); err != nil {
		t.Fatalf("rst: persist in-flight result: %v", err)
	}
	if _, err := tx.Exec(ctx, submitSignedSQL, rowID); err != nil {
		t.Fatalf("rst: mark in-flight signed: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rst: rollback pre-commit crash: %v", err)
	}

	// No partial success: no result survives the rolled-back crash.
	if got := signerAuthSignatureCount(t, pool); got != 0 {
		t.Fatalf("rst: crash before COMMIT left %d signature result(s), want 0", got)
	}
	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM signing_requests WHERE id = $1`, rowID).Scan(&state); err != nil {
		t.Fatalf("rst: re-read request row: %v", err)
	}
	if state != string(StateReceived) {
		t.Fatalf("rst: rolled-back crash changed the request state to %q, want received", state)
	}

	// Restarted process retries the same identity: no durable result is visible,
	// so it must not claim success or persist a second signature.
	_, err = Submit(ctx, submitTestDeps(t, rstFreshPool(t, pool)), cred, []byte(body))
	re := signerAuthRefusal(t, err, ClassOutcomeNotYetVisible)
	if RetryabilityOf(re.Class) != RetryNow {
		t.Fatalf("rst: outcome_not_yet_visible retryability = %v, want RetryNow", RetryabilityOf(re.Class))
	}
	if got := signerAuthSignatureCount(t, pool); got != 0 {
		t.Fatalf("rst: pre-commit retry produced %d signature result(s), want 0", got)
	}
	if got := submitRowCount(t, pool, callerID, "sr-7f3a"); got != 1 {
		t.Fatalf("rst: pre-commit retry created %d identity rows, want the original 1", got)
	}
}
