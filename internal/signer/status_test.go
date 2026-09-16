// status_test.go owns spec task T029 for 009-signer-service: the desensitized
// status path (contracts/api.md §3, FR-17). The unit cases pin the pure rules —
// tx_hash surfaces only for an admitted/delivered latest admission, a blocked
// admission carries its observed basis, and the view never carries signature
// material. The PostgreSQL case is the T-status read on a real, isolated
// database: own-request scoping (a foreign id is the identical not_found as a
// nonexistent one), latest-admission selection, and status-only responses for
// withheld/no-admission requests. The database case skips (never passes) when
// no container provider is available.
package signer

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
)

// statusWantNotFound asserts err is the single identical not_found outcome of
// the status path (api.md §3): no distinction between nonexistent and foreign.
func statusWantNotFound(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrStatusNotFound) {
		t.Fatalf("error = %v, want ErrStatusNotFound", err)
	}
	if err.Error() != ErrStatusNotFound.Error() {
		t.Fatalf("error text = %q, want the identical %q", err.Error(), ErrStatusNotFound.Error())
	}
}

// TestVerdictAllowsTxHash pins api.md §3: only an admission already cleared for
// delivery (admitted/delivered) unlocks tx_hash; blocked/unknown_reconcile are
// status-only (OC-6, the inversion of the 007 still-200 replay).
func TestVerdictAllowsTxHash(t *testing.T) {
	cases := map[Verdict]bool{
		VerdictAdmitted:         true,
		VerdictDelivered:        true,
		VerdictBlocked:          false,
		VerdictUnknownReconcile: false,
	}
	for verdict, want := range cases {
		if got := verdict.AllowsTxHash(); got != want {
			t.Errorf("Verdict(%q).AllowsTxHash() = %v, want %v", verdict, got, want)
		}
	}
}

// TestStatusCarriesNoSignatureMaterial asserts at the type and query level that
// the status view can never expose signature bytes: no Status/Delivery field
// names signature material, and the read never selects the signature column.
func TestStatusCarriesNoSignatureMaterial(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeOf(Status{}), reflect.TypeOf(Delivery{})} {
		for i := 0; i < typ.NumField(); i++ {
			name := typ.Field(i).Name
			if strings.Contains(strings.ToLower(name), "signature") {
				t.Errorf("%s.%s exposes signature material; status must never carry it", typ.Name(), name)
			}
		}
	}
	for _, forbidden := range []string{"s.signature", "raw"} {
		if strings.Contains(statusSelectSQL, forbidden) {
			t.Errorf("statusSelectSQL contains %q; status reads recorded facts and tx_hash only", forbidden)
		}
	}
}

// TestStatusNotFoundIsStable pins the wire value the serving layer maps to 404.
func TestStatusNotFoundIsStable(t *testing.T) {
	if ErrStatusNotFound.Error() != "not_found" {
		t.Fatalf("ErrStatusNotFound = %q, want %q", ErrStatusNotFound.Error(), "not_found")
	}
}

// statusStartPG boots an isolated scratch PostgreSQL named per the 009
// isolation scheme and migrates it to the full schema. It skips when no
// container provider is healthy so the default unit run needs no Docker.
func statusStartPG(t *testing.T) *pgxpool.Pool {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
		postgres.WithDatabase(isolationDBName),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	var out strings.Builder
	opts := db.MigrateOptions{DSN: dsn, LockTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second}
	if err := db.MigrateUp(ctx, opts, &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// statusInsertRequest seeds one signing_requests row and returns its id. Every
// request gets its own authorization id so the anchor unique index never trips.
func statusInsertRequest(t *testing.T, pool *pgxpool.Pool, callerID int64, requestID, attemptID, state, refusalClass string) int64 {
	t.Helper()
	const sql = `INSERT INTO signing_requests
		(caller_id, signing_request_id, attempt_id, intent_id, binding_ref, chain_id,
		 sender, nonce, tx_type, to_addr, value, data, gas_limit, gas_price,
		 asset, recipient, amount, canonical_envelope, content_hash,
		 authorization_id, authorization_fingerprint, authorization_state,
		 policy_version, state, refusal_class)
		VALUES ($1, $2, $3, $4, 'bind-1', 31337,
		 '0x1111111111111111111111111111111111111111', '0', 0,
		 '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '0', $5, '21000', '1',
		 '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
		 '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', '100',
		 'envelope', $6, $7, $8, 'active', 'policy-v1', $9, $10)
		RETURNING id`
	var id int64
	err := pool.QueryRow(context.Background(), sql,
		callerID, requestID, attemptID, "intent-"+requestID, []byte{},
		"0x"+strings.Repeat("cc", 32), "wa-"+requestID, strings.Repeat("dd", 32),
		state, refusalClass,
	).Scan(&id)
	if err != nil {
		t.Fatalf("seed request %s: %v", requestID, err)
	}
	return id
}

// statusInsertAdmission appends one delivery_admissions row for a request.
func statusInsertAdmission(t *testing.T, pool *pgxpool.Pool, rowID int64, attemptSeq int, verdict, reason, pauseBasis, recoveryBasis, authState string, delivered bool) {
	t.Helper()
	var deliveredAt *time.Time
	if delivered {
		now := time.Now()
		deliveredAt = &now
	}
	const sql = `INSERT INTO delivery_admissions
		(signing_request_row, attempt_seq, verdict, authorization_id, authorization_fingerprint,
		 authorization_state, binding_class, can_sign, recovery_version, pause_basis, recovery_basis,
		 reason, delivered_at)
		VALUES ($1, $2, $3, 'wa-x', $4, $5, 'matches', TRUE, 0, $6, $7, $8, $9)`
	if _, err := pool.Exec(context.Background(), sql,
		rowID, attemptSeq, verdict, strings.Repeat("dd", 32), authState,
		pauseBasis, recoveryBasis, reason, deliveredAt,
	); err != nil {
		t.Fatalf("seed admission seq %d: %v", attemptSeq, err)
	}
}

// statusInsertResult seeds the persisted result for a request.
func statusInsertResult(t *testing.T, pool *pgxpool.Pool, rowID int64, txHash string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO signature_results (signing_request_row, signature, tx_hash) VALUES ($1, $2, $3)`,
		rowID, "0x"+strings.Repeat("ab", 65), txHash,
	); err != nil {
		t.Fatalf("seed signature result: %v", err)
	}
}

// TestStatusOwnRequestDesensitized is the T-status read on a real database: an
// owner reads recorded facts plus the latest admission; a foreign or
// nonexistent id is the identical not_found; tx_hash appears only for an
// admitted/delivered latest admission; a blocked/no-admission request is
// status-only with its observed basis.
func TestStatusOwnRequestDesensitized(t *testing.T) {
	pool := statusStartPG(t)
	ctx := context.Background()
	const owner, other = int64(1001), int64(2001)

	if _, err := pool.Exec(ctx,
		`INSERT INTO signer_caller (caller_id, label) VALUES ($1, 'owner'), ($2, 'other')`,
		owner, other); err != nil {
		t.Fatalf("seed callers: %v", err)
	}

	deliveredID := statusInsertRequest(t, pool, owner, "sr-delivered", "att-delivered", "signed", "")
	statusInsertResult(t, pool, deliveredID, "0x"+strings.Repeat("e1", 32))
	statusInsertAdmission(t, pool, deliveredID, 1, "delivered", "", "none", "none", "active", true)

	blockedID := statusInsertRequest(t, pool, owner, "sr-blocked", "att-blocked", "signed", "")
	statusInsertResult(t, pool, blockedID, "0x"+strings.Repeat("e2", 32))
	statusInsertAdmission(t, pool, blockedID, 1, "admitted", "", "none", "none", "active", false)
	statusInsertAdmission(t, pool, blockedID, 2, "blocked", "signature_withheld", "indexer_pause", "active", "revoked", false)

	noAdmissionID := statusInsertRequest(t, pool, owner, "sr-no-admission", "att-no-admission", "signed", "")
	statusInsertResult(t, pool, noAdmissionID, "0x"+strings.Repeat("e3", 32))

	statusInsertRequest(t, pool, owner, "sr-rejected", "att-rejected", "rejected", "validation_failed")
	foreignID := statusInsertRequest(t, pool, other, "sr-foreign", "att-foreign", "signed", "")
	statusInsertResult(t, pool, foreignID, "0x"+strings.Repeat("e4", 32))
	statusInsertAdmission(t, pool, foreignID, 1, "delivered", "", "none", "none", "active", true)

	t.Run("delivered exposes tx_hash", func(t *testing.T) {
		st, err := LookupStatus(ctx, pool, owner, "sr-delivered")
		if err != nil {
			t.Fatalf("LookupStatus: %v", err)
		}
		if st.SigningRequestID != "sr-delivered" || st.State != StateSigned || st.AttemptID != "att-delivered" {
			t.Fatalf("facts = %+v, want sr-delivered/signed/att-delivered", st)
		}
		if st.IntentID != "intent-sr-delivered" || st.PolicyVersion != "policy-v1" {
			t.Fatalf("intent/policy = %q/%q", st.IntentID, st.PolicyVersion)
		}
		if st.ContentHash != "0x"+strings.Repeat("cc", 32) {
			t.Fatalf("content_hash = %q", st.ContentHash)
		}
		if st.CreatedAt.IsZero() || st.UpdatedAt.IsZero() {
			t.Fatalf("timestamps not read: %+v", st)
		}
		if st.Delivery == nil || st.Delivery.Verdict != VerdictDelivered || st.Delivery.AttemptSeq != 1 {
			t.Fatalf("delivery = %+v, want delivered/attempt 1", st.Delivery)
		}
		if st.Delivery.DeliveredAt == nil {
			t.Fatal("delivered admission carries no delivered_at")
		}
		if want := "0x" + strings.Repeat("e1", 32); st.TxHash != want {
			t.Fatalf("tx_hash = %q, want %q", st.TxHash, want)
		}
		if st.RefusalClass != "" {
			t.Fatalf("delivered status carries refusal class %q", st.RefusalClass)
		}
	})

	t.Run("blocked is status-only with observed basis", func(t *testing.T) {
		st, err := LookupStatus(ctx, pool, owner, "sr-blocked")
		if err != nil {
			t.Fatalf("LookupStatus: %v", err)
		}
		if st.Delivery == nil || st.Delivery.Verdict != VerdictBlocked || st.Delivery.AttemptSeq != 2 {
			t.Fatalf("delivery = %+v, want the latest blocked admission at attempt 2", st.Delivery)
		}
		if st.TxHash != "" {
			t.Fatalf("blocked latest admission still exposed tx_hash %q (OC-6 requires status-only)", st.TxHash)
		}
		if st.RefusalClass != ClassSignatureWithheld {
			t.Fatalf("refusal class = %q, want %q", st.RefusalClass, ClassSignatureWithheld)
		}
		if st.Delivery.Reason != "signature_withheld" || st.Delivery.PauseBasis != "indexer_pause" ||
			st.Delivery.RecoveryBasis != "active" || st.Delivery.AuthorizationState != "revoked" {
			t.Fatalf("blocked basis = %+v, want the observed pause/recovery/authorization basis", st.Delivery)
		}
	})

	t.Run("signed without admission is status-only", func(t *testing.T) {
		st, err := LookupStatus(ctx, pool, owner, "sr-no-admission")
		if err != nil {
			t.Fatalf("LookupStatus: %v", err)
		}
		if st.Delivery != nil {
			t.Fatalf("delivery = %+v, want none before any admission", st.Delivery)
		}
		if st.TxHash != "" {
			t.Fatalf("tx_hash %q exposed without an admitted/delivered admission", st.TxHash)
		}
	})

	t.Run("rejected carries its recorded refusal", func(t *testing.T) {
		st, err := LookupStatus(ctx, pool, owner, "sr-rejected")
		if err != nil {
			t.Fatalf("LookupStatus: %v", err)
		}
		if st.State != StateRejected || st.RefusalClass != ClassValidationFailed {
			t.Fatalf("state/refusal = %q/%q, want rejected/validation_failed", st.State, st.RefusalClass)
		}
		if st.Delivery != nil || st.TxHash != "" {
			t.Fatalf("rejected status exposed delivery/tx_hash: %+v", st)
		}
	})

	t.Run("foreign and nonexistent ids are identical not_found", func(t *testing.T) {
		_, foreignErr := LookupStatus(ctx, pool, owner, "sr-foreign")
		_, missingErr := LookupStatus(ctx, pool, owner, "sr-does-not-exist")
		statusWantNotFound(t, foreignErr)
		statusWantNotFound(t, missingErr)
		if foreignErr.Error() != missingErr.Error() {
			t.Fatalf("foreign %q != nonexistent %q", foreignErr, missingErr)
		}
		// The row exists for its owner; the miss is ownership, not absence.
		if _, err := LookupStatus(ctx, pool, other, "sr-foreign"); err != nil {
			t.Fatalf("owner of sr-foreign cannot read it: %v", err)
		}
	})

	t.Run("storage failure is never a false not_found", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		_, err := LookupStatus(canceled, pool, owner, "sr-delivered")
		if err == nil || errors.Is(err, ErrStatusNotFound) {
			t.Fatalf("canceled read = %v, want a storage refusal, never not_found", err)
		}
		var re *RefusalError
		if !errors.As(err, &re) || re.Class != ClassStorageUnavailable {
			t.Fatalf("canceled read class = %v, want %s", err, ClassStorageUnavailable)
		}
	})
}
