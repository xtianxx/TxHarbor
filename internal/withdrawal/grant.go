// grant.go owns the upstream grant-supply core (T008, research R9): the
// attempt-key mint, the op-input (8-field) binding, and the supply/revoke
// statement scripts with their 23505 and commit-unknown recovery branches
// (data-model.md Table 6 + supply pseudocode, locked 六轮定点).
//
// Transaction ownership: SupplyGrant and RevokeGrant own their BEGIN..COMMIT.
// T010 (intake) MUST NOT wrap them in an outer transaction — pgx has no nested
// transactions. The package-level SQL constants and helpers below are the
// reusable statement scripts; the grant read shape is documented for T010's own
// FOR SHARE receipt path (deliberately NOT defined here — Table 6: the grant
// supply/revoke path takes FOR UPDATE).
//
// Chain binding split: these functions reject chain_id <= 0 as a shape error
// but do NOT compare the chain against the deployment chain. That equality is
// the subcommand/T010 caller's job (config.ChainID via validate.go's
// ValidateChainID); grant.go has no deployment-chain parameter by design.
//
// API immutability: the frozen T008 surface is
//
//	SupplyGrant(ctx, pool, op, operator, reason)   // owns BEGIN..COMMIT
//	RevokeGrant(ctx, pool, operationID, authorizationID, operator, reason)
//	ReadAttempt(ctx, pool, operationID)            // pool read, no tx
//	MintOperationID()
//
// SupplyGrant's signature carries no separate operation-id parameter, so the
// attempt key travels in OpInput.OperationID. It is the compare KEY, never
// compared CONTENT: it is excluded from the bound op-input (the eight fields
// of data-model Table 6) and from the audit `detail` snapshot.
package withdrawal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Grant-supply vocabulary. The request verbs are op-input `action` values; the
// outcome names mirror the Table 6 audit `action` CHECK vocabulary exactly.
const (
	grantSupplyAction = "supply"
	grantRevokeAction = "revoke"

	grantOutcomeSupplied      = "supplied"
	grantOutcomeResupplied    = "resupplied"
	grantOutcomeSupplyRefused = "supply_refused"
	grantOutcomeRevoked       = "revoked"
	grantOutcomeRevokeNop     = "revoke_nop"
)

// The EXACT PostgreSQL constraint names matched after a 23505 (R8). Prose
// shorthands ("operation_id_uniq", "grant pkey") are never match values.
const (
	grantAuditOperationIDUniq = "withdrawal_grant_audit_operation_id_uniq"
	grantAuthorizationsPK     = "withdrawal_authorizations_pkey"
)

// grantWriteGuard bounds every statement inside a supply/revoke transaction
// (per-statement timeout only — never a tx total or a recovery gate). Same
// literal as the indexer's unexported writeGuard (scanner.go).
const grantWriteGuard = "SET LOCAL statement_timeout = '5s'"

// SQL statement scripts. Package-level so T010 can reuse the shapes.
const (
	// grantSelectForUpdateSQL locks the one grant row being supplied/revoked.
	grantSelectForUpdateSQL = `
SELECT caller_id, chain_id, asset, recipient, amount::text, state, expires_at
FROM withdrawal_authorizations
WHERE authorization_id = $1
FOR UPDATE`

	// grantInsertSQL writes the first supply of a grant (Table 4).
	grantInsertSQL = `
INSERT INTO withdrawal_authorizations
    (authorization_id, caller_id, chain_id, asset, recipient, amount, state, expires_at, supplied_by)
VALUES ($1, $2, $3, $4, $5, $6::numeric, 'active', $7::timestamptz, $8)`

	// grantUnchangedSQL is the equal-re-supply check: it fires only when a
	// business parameter differs, so an equal re-supply asserts
	// RowsAffected()==0 and leaves the row untouched (Table 6 'resupplied').
	// A non-zero count means the client-side equality was wrong; the caller
	// rolls back and reports operation_conflict.
	grantUnchangedSQL = `
UPDATE withdrawal_authorizations
SET caller_id = $2, chain_id = $3, asset = $4, recipient = $5,
    amount = $6::numeric, expires_at = $7::timestamptz
WHERE authorization_id = $1
  AND (caller_id IS DISTINCT FROM $2::bigint
       OR chain_id IS DISTINCT FROM $3::bigint
       OR asset IS DISTINCT FROM $4::text
       OR recipient IS DISTINCT FROM $5::text
       OR amount IS DISTINCT FROM $6::numeric
       OR expires_at IS DISTINCT FROM $7::timestamptz)`

	// grantRevokeSQL flips an active grant to revoked.
	grantRevokeSQL = `
UPDATE withdrawal_authorizations SET state = 'revoked' WHERE authorization_id = $1`

	// grantAuditInsertSQL appends one attempt row (Table 6). operator/reason
	// are retry metadata columns, never compared.
	grantAuditInsertSQL = `
INSERT INTO withdrawal_grant_audit
    (operation_id, authorization_id, caller_id, action, operator, reason, detail)
VALUES ($1, $2, $3, $4, $5, $6, $7)`

	// grantAttemptSelectSQL reads one attempt by its operation id.
	grantAttemptSelectSQL = `
SELECT action, authorization_id, detail FROM withdrawal_grant_audit WHERE operation_id = $1`

	// grantSelectReadOnlySQL reads a grant without locking (PK-race recovery).
	grantSelectReadOnlySQL = `
SELECT caller_id, chain_id, asset, recipient, amount::text, state, expires_at
FROM withdrawal_authorizations WHERE authorization_id = $1`
)

// OpInput is one supply op-input — the eight fields bound by data-model
// Table 6 (action, authorization_id, caller_id, chain_id, asset, recipient,
// amount, expires_at) — plus the attempt key OperationID.
//
// OperationID is the caller-minted, durably-captured attempt identity (R9):
// it is the compare key, not compared content, so operator/reason retry
// metadata never reopens an attempt. SupplyGrant's frozen signature has no
// separate operation-id parameter, so it travels here.
type OpInput struct {
	OperationID     string
	Action          string
	AuthorizationID string
	CallerID        int64
	ChainID         int64
	Asset           string
	Recipient       string
	Amount          string
	ExpiresAt       *time.Time
}

// GrantOutcome is the recorded audit action for one attempt plus the grant id
// it names. It is what ReadAttempt and the recovery branches report; it proves
// THIS attempt's outcome (never the grant row's current state).
type GrantOutcome struct {
	Action          string // Table 6 audit action ('supplied', 'resupplied', ...)
	AuthorizationID string // grant id the attempt operated on
}

// MintOperationID returns a fresh opaque attempt id: 16 bytes from crypto/rand
// as 32 lowercase hex characters. The caller MUST durably capture it BEFORE
// invoking supply/revoke (R9: no reusable operation id ⇒ no DB side effects).
func MintOperationID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("mint operation id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// opInputDetail renders the canonical, redacted op-input snapshot stored in
// the audit `detail` column and used for same-operation comparison. It binds
// exactly the eight op-input fields; OperationID is the key and operator/reason
// are retry metadata, so none of the three appear here. expiring values are
// normalized to UTC microseconds (PostgreSQL timestamptz precision).
func opInputDetail(op OpInput) string {
	expires := ""
	if op.ExpiresAt != nil {
		expires = op.ExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	return strings.Join([]string{
		"action=" + strconv.Quote(op.Action),
		"authorization_id=" + strconv.Quote(op.AuthorizationID),
		"caller_id=" + strconv.FormatInt(op.CallerID, 10),
		"chain_id=" + strconv.FormatInt(op.ChainID, 10),
		"asset=" + strconv.Quote(op.Asset),
		"recipient=" + strconv.Quote(op.Recipient),
		"amount=" + strconv.Quote(op.Amount),
		"expires_at=" + strconv.Quote(expires),
	}, ";")
}

// grantRow is one locked grant row (Table 4) as read on the supply/revoke path.
type grantRow struct {
	callerID  int64
	chainID   int64
	asset     string
	recipient string
	amount    string
	state     string
	expiresAt *time.Time
}

// matchesOp reports whether the stored grant binds exactly the presented
// op-input business params. state is not part of op-input.
func (g grantRow) matchesOp(op OpInput) bool {
	return g.callerID == op.CallerID && g.chainID == op.ChainID &&
		g.asset == op.Asset && g.recipient == op.Recipient &&
		g.amount == op.Amount && equalOptionalTime(g.expiresAt, op.ExpiresAt)
}

// attemptRow is one recorded audit attempt read by operation id.
type attemptRow struct {
	action          string
	authorizationID string
	detail          string
}

// opInputMatches reports whether the stored attempt binds the presented
// op-input. operator/reason differ → still equal (retry metadata).
func (r attemptRow) opInputMatches(op OpInput) bool {
	return r.detail == opInputDetail(op)
}

// equalOptionalTime compares two nullable instants by value.
func equalOptionalTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

// canonicalAddressField reuses CanonicalAddress (FR-07 EIP-55 + lowercase)
// while relabeling the offending field, since CanonicalAddress always names
// the "recipient" field.
func canonicalAddressField(raw, field string) (string, error) {
	canon, err := CanonicalAddress(raw)
	if err != nil {
		var e *Error
		if errors.As(err, &e) {
			return "", e.WithField(field)
		}
		return "", err
	}
	return canon, nil
}

// validateSupplyOpInput validates the supply op-input BEFORE any pool use and
// returns the normalized form (lowercase addresses, microseconds-truncated
// expires_at). Chain equality is intentionally not checked here (see file doc).
func validateSupplyOpInput(op OpInput, now time.Time) (OpInput, error) {
	if op.OperationID == "" {
		return OpInput{}, New(CodeValidationFailed,
			"operation_id is required; mint one and capture it before supply/revoke").
			WithField("operation_id")
	}
	if op.Action != grantSupplyAction {
		return OpInput{}, New(CodeValidationFailed,
			"action must be \"supply\" for SupplyGrant").
			WithField("action")
	}
	if op.AuthorizationID == "" {
		return OpInput{}, New(CodeValidationFailed, "authorization_id is required").
			WithField("authorization_id")
	}
	if op.CallerID <= 0 {
		return OpInput{}, New(CodeValidationFailed, "caller_id must be positive").
			WithField("caller_id")
	}
	if op.ChainID <= 0 {
		return OpInput{}, New(CodeValidationFailed,
			"chain_id must be positive; deployment-chain equality is enforced by the withdrawal-authz caller").
			WithField("chain_id")
	}
	asset, err := canonicalAddressField(op.Asset, "asset")
	if err != nil {
		return OpInput{}, err
	}
	recipient, err := canonicalAddressField(op.Recipient, "recipient")
	if err != nil {
		return OpInput{}, err
	}
	if _, err := ValidateAmount(op.Amount); err != nil {
		return OpInput{}, err
	}
	if op.ExpiresAt != nil {
		t := op.ExpiresAt.UTC().Truncate(time.Microsecond)
		if !t.After(now) {
			return OpInput{}, New(CodeValidationFailed, "expires_at must be in the future").
				WithField("expires_at")
		}
		op.ExpiresAt = &t
	}
	op.Asset = asset
	op.Recipient = recipient
	return op, nil
}

// validateRevokeInput validates a revoke attempt before any pool use.
func validateRevokeInput(operationID, authorizationID string) error {
	if operationID == "" {
		return New(CodeValidationFailed,
			"operation_id is required; mint one and capture it before supply/revoke").
			WithField("operation_id")
	}
	if authorizationID == "" {
		return New(CodeValidationFailed, "authorization_id is required").
			WithField("authorization_id")
	}
	return nil
}

// grantCommitUnknownError marks a failed COMMIT whose outcome is indeterminate;
// the caller must recover by re-reading the attempt with the SAME operation id.
type grantCommitUnknownError struct{ err error }

func (e *grantCommitUnknownError) Error() string { return "commit outcome unknown: " + e.err.Error() }
func (e *grantCommitUnknownError) Unwrap() error { return e.err }

// grantPgError extracts the *pgconn.PgError from a (possibly wrapped) error.
func grantPgError(err error) *pgconn.PgError {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr
	}
	return nil
}

// grantRetryable maps an internal failure to the retryable code, preserving a
// classified *Error (e.g. operation_conflict) that already reached the caller.
func grantRetryable(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return New(CodeTemporarilyUnavailable,
		"grant operation outcome is unknown or storage is unavailable; retry with the same operation_id")
}

// readGrant locks and reads one grant row inside tx (nil when absent).
func readGrant(ctx context.Context, tx pgx.Tx, authorizationID string) (*grantRow, error) {
	var g grantRow
	err := tx.QueryRow(ctx, grantSelectForUpdateSQL, authorizationID).
		Scan(&g.callerID, &g.chainID, &g.asset, &g.recipient, &g.amount, &g.state, &g.expiresAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("read grant for update: %w", err)
	}
	return &g, nil
}

// readGrantReadOnly reads one grant row without locking (PK-race recovery).
func readGrantReadOnly(ctx context.Context, pool *pgxpool.Pool, authorizationID string) (*grantRow, error) {
	var g grantRow
	err := pool.QueryRow(ctx, grantSelectReadOnlySQL, authorizationID).
		Scan(&g.callerID, &g.chainID, &g.asset, &g.recipient, &g.amount, &g.state, &g.expiresAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("read grant: %w", err)
	}
	return &g, nil
}

// insertAuditTx appends one audit row inside tx and asserts exactly one row.
func insertAuditTx(ctx context.Context, tx pgx.Tx, op OpInput, callerID int64, operator, reason, action string) error {
	tag, err := tx.Exec(ctx, grantAuditInsertSQL, op.OperationID, op.AuthorizationID,
		callerID, action, operator, reason, opInputDetail(op))
	if err != nil {
		return fmt.Errorf("insert grant audit: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("insert grant audit affected %d rows, want 1", tag.RowsAffected())
	}
	return nil
}

// runSupplyTx owns BEGIN..COMMIT for one supply attempt.
func runSupplyTx(ctx context.Context, pool *pgxpool.Pool, op OpInput, operator, reason string) (*GrantOutcome, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin supply transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after COMMIT

	if _, err := tx.Exec(ctx, grantWriteGuard); err != nil {
		return nil, fmt.Errorf("supply transaction statement guard: %w", err)
	}
	grant, err := readGrant(ctx, tx, op.AuthorizationID)
	if err != nil {
		return nil, err
	}

	var action string
	switch {
	case grant == nil: // miss + supply → first supply
		tag, err := tx.Exec(ctx, grantInsertSQL, op.AuthorizationID, op.CallerID, op.ChainID,
			op.Asset, op.Recipient, op.Amount, op.ExpiresAt, operator)
		if err != nil {
			return nil, fmt.Errorf("insert grant: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return nil, fmt.Errorf("insert grant affected %d rows, want 1", tag.RowsAffected())
		}
		action = grantOutcomeSupplied
	case grant.matchesOp(op): // present + equal + supply → resupply
		tag, err := tx.Exec(ctx, grantUnchangedSQL, op.AuthorizationID, op.CallerID, op.ChainID,
			op.Asset, op.Recipient, op.Amount, op.ExpiresAt)
		if err != nil {
			return nil, fmt.Errorf("verify grant equality: %w", err)
		}
		if tag.RowsAffected() != 0 {
			return nil, New(CodeOperationConflict,
				"grant parameters changed under the equality check").
				WithField("authorization_id")
		}
		action = grantOutcomeResupplied
	default: // present + differ (or no match) → refused, zero grant mutation
		action = grantOutcomeSupplyRefused
	}

	if err := insertAuditTx(ctx, tx, op, op.CallerID, operator, reason, action); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, &grantCommitUnknownError{err: err}
	}
	return &GrantOutcome{Action: action, AuthorizationID: op.AuthorizationID}, nil
}

// runRevokeTx owns BEGIN..COMMIT for one revoke attempt.
func runRevokeTx(ctx context.Context, pool *pgxpool.Pool, op OpInput, operator, reason string) (*GrantOutcome, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin revoke transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, grantWriteGuard); err != nil {
		return nil, fmt.Errorf("revoke transaction statement guard: %w", err)
	}
	grant, err := readGrant(ctx, tx, op.AuthorizationID)
	if err != nil {
		return nil, err
	}

	var (
		action   string
		callerID int64
	)
	switch {
	case grant == nil: // missing grant on revoke → refused, nothing to attribute
		action = grantOutcomeSupplyRefused
	case grant.state == "active":
		tag, err := tx.Exec(ctx, grantRevokeSQL, op.AuthorizationID)
		if err != nil {
			return nil, fmt.Errorf("revoke grant: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return nil, fmt.Errorf("revoke grant affected %d rows, want 1", tag.RowsAffected())
		}
		action = grantOutcomeRevoked
		callerID = grant.callerID
	default: // already revoked/expired → idempotent no-op, own row
		action = grantOutcomeRevokeNop
		callerID = grant.callerID
	}

	if err := insertAuditTx(ctx, tx, op, callerID, operator, reason, action); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, &grantCommitUnknownError{err: err}
	}
	return &GrantOutcome{Action: action, AuthorizationID: op.AuthorizationID}, nil
}

// readAttemptPool reads one attempt row via the pool (no transaction).
func readAttemptPool(ctx context.Context, pool *pgxpool.Pool, operationID string) (attemptRow, bool, error) {
	var r attemptRow
	err := pool.QueryRow(ctx, grantAttemptSelectSQL, operationID).
		Scan(&r.action, &r.authorizationID, &r.detail)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return attemptRow{}, false, nil
	case err != nil:
		return attemptRow{}, false, fmt.Errorf("read attempt: %w", err)
	}
	return r, true, nil
}

// resolveByOperationID is the shared recovery for a 23505 on
// withdrawal_grant_audit_operation_id_uniq and for an uncertain COMMIT: read the
// recorded attempt by O, compare the full op-input, and report. A miss is an
// unknown/retryable outcome with the SAME O ("双缺→新O" is deleted); a refusal
// stays a refusal and is never upgraded.
func resolveByOperationID(ctx context.Context, pool *pgxpool.Pool, op OpInput) (*GrantOutcome, error) {
	row, found, err := readAttemptPool(ctx, pool, op.OperationID)
	if err != nil {
		return nil, grantRetryable(err)
	}
	if !found {
		return nil, New(CodeTemporarilyUnavailable,
			"operation outcome is not yet visible; retry with the same operation_id")
	}
	if !row.opInputMatches(op) {
		return nil, New(CodeOperationConflict,
			"operation_id was already recorded with a different op-input").
			WithField("operation_id")
	}
	return &GrantOutcome{Action: row.action, AuthorizationID: row.authorizationID}, nil
}

// boundedAudit attempts the single bounded re-execution of the first-supply
// PK-race branch: append one audit row (SAME O, SAME op-input) in its own
// statement tx. A second operation-id conflict converges on the twin's recorded
// row; any other failure reports retryable with the same O (never a loop).
func boundedAudit(ctx context.Context, pool *pgxpool.Pool, op OpInput, callerID int64, operator, reason, action string) (*GrantOutcome, error) {
	_, err := pool.Exec(ctx, grantAuditInsertSQL, op.OperationID, op.AuthorizationID,
		callerID, action, operator, reason, opInputDetail(op))
	if err == nil {
		return &GrantOutcome{Action: action, AuthorizationID: op.AuthorizationID}, nil
	}
	if pgErr := grantPgError(err); pgErr != nil &&
		pgErr.Code == "23505" && pgErr.ConstraintName == grantAuditOperationIDUniq {
		return resolveByOperationID(ctx, pool, op)
	}
	return nil, grantRetryable(err)
}

// resolveGrantPKRace handles a 23505 on withdrawal_authorizations_pkey (two
// concurrent first-supplies). N1 order: O first; a recorded equal attempt
// converges, a recorded different attempt is operation_conflict. Otherwise the
// read-only grant re-read decides resupply (equal, same O, ONE bounded retry)
// or refusal. The grant-PK conflict is NEVER mapped to operation_conflict.
func resolveGrantPKRace(ctx context.Context, pool *pgxpool.Pool, op OpInput, operator, reason string) (*GrantOutcome, error) {
	row, found, err := readAttemptPool(ctx, pool, op.OperationID)
	if err != nil {
		return nil, grantRetryable(err)
	}
	if found {
		if row.opInputMatches(op) {
			return &GrantOutcome{Action: row.action, AuthorizationID: row.authorizationID}, nil
		}
		return nil, New(CodeOperationConflict,
			"operation_id was already recorded with a different op-input").
			WithField("operation_id")
	}
	grant, err := readGrantReadOnly(ctx, pool, op.AuthorizationID)
	if err != nil {
		return nil, grantRetryable(err)
	}
	if grant != nil && grant.matchesOp(op) {
		return boundedAudit(ctx, pool, op, op.CallerID, operator, reason, grantOutcomeResupplied)
	}
	return boundedAudit(ctx, pool, op, op.CallerID, operator, reason, grantOutcomeSupplyRefused)
}

// SupplyGrant runs one supply attempt. It owns BEGIN..COMMIT (do not nest it in
// another transaction). The operation id is op.OperationID and MUST be non-empty
// and durably captured first; invalid op-input fails with CodeValidationFailed
// before any database access. 23505/commit recovery per data-model Table 6.
func SupplyGrant(ctx context.Context, pool *pgxpool.Pool, op OpInput, operator, reason string) (*GrantOutcome, error) {
	norm, err := validateSupplyOpInput(op, time.Now())
	if err != nil {
		return nil, err
	}
	out, err := runSupplyTx(ctx, pool, norm, operator, reason)
	if err == nil {
		return out, nil
	}
	var unknown *grantCommitUnknownError
	switch {
	case errors.As(err, &unknown):
		return resolveByOperationID(ctx, pool, norm)
	case grantPgError(err) != nil:
		pgErr := grantPgError(err)
		if pgErr.Code == "23505" {
			switch pgErr.ConstraintName {
			case grantAuditOperationIDUniq:
				return resolveByOperationID(ctx, pool, norm)
			case grantAuthorizationsPK:
				return resolveGrantPKRace(ctx, pool, norm, operator, reason)
			}
		}
		if pgErr.Code == "23503" { // caller_id FK: deterministic bad input
			return nil, New(CodeValidationFailed, "caller_id does not exist").
				WithField("caller_id")
		}
	}
	return nil, grantRetryable(err)
}

// RevokeGrant runs one revoke attempt. It owns BEGIN..COMMIT (do not nest it).
// operationID MUST be non-empty and durably captured first. An active grant is
// revoked; an already-inactive grant records an idempotent 'revoke_nop' row.
func RevokeGrant(ctx context.Context, pool *pgxpool.Pool, operationID, authorizationID, operator, reason string) (*GrantOutcome, error) {
	if err := validateRevokeInput(operationID, authorizationID); err != nil {
		return nil, err
	}
	op := OpInput{OperationID: operationID, Action: grantRevokeAction, AuthorizationID: authorizationID}
	out, err := runRevokeTx(ctx, pool, op, operator, reason)
	if err == nil {
		return out, nil
	}
	var unknown *grantCommitUnknownError
	switch {
	case errors.As(err, &unknown):
		return resolveByOperationID(ctx, pool, op)
	case grantPgError(err) != nil:
		pgErr := grantPgError(err)
		if pgErr.Code == "23505" && pgErr.ConstraintName == grantAuditOperationIDUniq {
			return resolveByOperationID(ctx, pool, op)
		}
	}
	return nil, grantRetryable(err)
}

// ReadAttempt reads one recorded grant attempt by operation id. It returns the
// recorded outcome and true when present, or (nil, false, nil) when the
// operation id has no row. It never writes: recovery/retry paths use it to
// converge same-operation retries, and a missing row never proves rollback.
func ReadAttempt(ctx context.Context, pool *pgxpool.Pool, operationID string) (*GrantOutcome, bool, error) {
	row, found, err := readAttemptPool(ctx, pool, operationID)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	return &GrantOutcome{Action: row.action, AuthorizationID: row.authorizationID}, true, nil
}
