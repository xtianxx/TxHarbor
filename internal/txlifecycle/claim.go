package txlifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ClaimRef is the execution qualification the caller currently holds
// (send-api.md §2.2).
type ClaimRef struct {
	IntentID     string
	WorkerID     string
	LeaseVersion int64
}

// ClaimBasis is the observed claim row (evidence, never a permission).
type ClaimBasis struct {
	Present      bool
	IntentID     string
	WorkerID     string
	LeaseVersion int64
	ExpiresAt    time.Time
	Now          time.Time
	Basis        string
}

// claimColumns absorbs 011's concrete execution_claims column names in ONE
// place (T012; T045 swapped these for the real 011 columns in
// migrations/000012_withdrawal_execution.sql:58-82). The frozen J2 semantics
// are fixed by contract; only the names are 011-owned.
type claimColumns struct {
	Table        string
	IntentID     string
	WorkerID     string
	LeaseVersion string
	ExpiresAt    string
	State        string
}

// j2Columns is the real 011 mapping (T045): the claim owner is `owner_id`,
// and "revoked/ended" is carried by `state` ('active'|'released'|'revoked')
// under CONSTRAINT execution_claims_state_consistency. The contract-shaped
// fixture for the 010-independent suite is provisioned test-only with the
// same column names (testfixture_test.go), never as a migration.
var j2Columns = claimColumns{
	Table:        "execution_claims",
	IntentID:     "intent_id",
	WorkerID:     "owner_id",
	LeaseVersion: "lease_version",
	ExpiresAt:    "expires_at",
	State:        "state",
}

// ClaimAdapter reads the J2 claim row. Pre-011 the table does not exist: the
// read fails closed with claim_absent and never degrades into an unqualified
// send (G-010-4; R-010-11).
type ClaimAdapter struct {
	cols claimColumns
}

// NewClaimAdapter returns the adapter over the frozen J2 mapping.
func NewClaimAdapter() *ClaimAdapter { return &ClaimAdapter{cols: j2Columns} }

// Read performs the one FOR SHARE read keyed by intent_id and evaluates the
// frozen J2 semantics: intent-unique row, worker identity equality, monotonic
// lease_version equality, expiry on the DB clock, active/not-revoked. A
// natural expiry and an explicit revocation are recorded with distinct basis
// strings but identical refusal effect (no grace, no TTL).
func (a *ClaimAdapter) Read(ctx context.Context, tx pgx.Tx, ref ClaimRef) (ClaimBasis, *RefusalError) {
	var b ClaimBasis
	var state string
	b.IntentID = ref.IntentID
	query := fmt.Sprintf(
		`SELECT %s, %s, %s, %s, clock_timestamp() FROM %s WHERE %s = $1 FOR SHARE`,
		a.cols.WorkerID, a.cols.LeaseVersion, a.cols.ExpiresAt, a.cols.State,
		a.cols.Table, a.cols.IntentID)
	err := tx.QueryRow(ctx, query, ref.IntentID).
		Scan(&b.WorkerID, &b.LeaseVersion, &b.ExpiresAt, &state, &b.Now)
	if err != nil {
		if isUndefinedTable(err) || isUndefinedColumn(err) {
			return b, &RefusalError{Class: ClassClaimAbsent, Basis: "execution_claims table absent"}
		}
		if err == pgx.ErrNoRows {
			return b, &RefusalError{Class: ClassClaimAbsent, Basis: "no claim row for intent_id"}
		}
		return b, &RefusalError{Class: ClassGateReadFailed, Basis: "claim read failed"}
	}
	b.Present = true
	b.ExpiresAt = b.ExpiresAt.UTC()
	b.Now = b.Now.UTC()

	switch {
	case state != "active":
		b.Basis = "state=" + state
		return b, &RefusalError{Class: ClassClaimRevoked, Basis: "claim state=" + state}
	case !b.ExpiresAt.After(b.Now):
		b.Basis = "expired"
		return b, &RefusalError{Class: ClassClaimExpired, Basis: "claim expired on the database clock"}
	case b.WorkerID != ref.WorkerID:
		b.Basis = "worker_mismatch"
		return b, &RefusalError{Class: ClassClaimVersionMismatch, Basis: "claim worker identity differs"}
	case b.LeaseVersion != ref.LeaseVersion:
		b.Basis = "lease_version_mismatch"
		return b, &RefusalError{Class: ClassClaimVersionMismatch, Basis: "claim lease_version is not current"}
	}
	b.Basis = "active"
	return b, nil
}

// isUndefinedTable reports SQLSTATE 42P01 (relation does not exist).
func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

// isUndefinedColumn reports SQLSTATE 42703 (column does not exist): the
// contract-shaped fixture on the 011-absent lane fails closed as claim_absent
// rather than degrading into an unqualified send (G-010-4).
func isUndefinedColumn(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42703"
}

// isInFailedTransaction reports SQLSTATE 25P02: a failed statement (e.g. the
// undefined-table claim read) poisoned the region transaction, so the refusal
// evidence must be recorded in a fresh transaction.
func isInFailedTransaction(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "25P02"
}
