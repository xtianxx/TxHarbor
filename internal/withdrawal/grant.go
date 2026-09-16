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

	// grantOldSelectSQL reads the OLD grant a T-reissue links to. Plain read, no
	// lock: T-reissue holds new-row locks only (data-model.md lock matrix), so
	// nothing about the old row is read under a lock or ever written.
	grantOldSelectSQL = `
SELECT state FROM withdrawal_authorizations WHERE authorization_id = $1`

	// scopeSelectSQL reads the 1:1 scope row for a grant (T012). The supply/revoke
	// tx holds the grant row FOR UPDATE, so all scope writers are serialized on
	// the grant and no separate lock is taken here (data-model.md lock matrix:
	// "fresh scope insert (no lock)"). A missing row is pre-extension stock.
	scopeSelectSQL = `
SELECT authorization_id, intent_id, request_id, sender, fee_max_total,
       fee_max_per_gas, fee_max_priority, allows_fee_replacement,
       authorization_version, attested_by
FROM withdrawal_authorization_scopes WHERE authorization_id = $1`

	// scopeInsertSQL writes the scope row in the same tx as the first supply
	// (T012). authorization_version starts at 1 (data-model.md).
	scopeInsertSQL = `
INSERT INTO withdrawal_authorization_scopes
    (authorization_id, intent_id, request_id, sender, fee_max_total,
     fee_max_per_gas, fee_max_priority, allows_fee_replacement,
     authorization_version, attested_by)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

	// scopeBumpVersionSQL advances the monotonic version and nothing else:
	// applied scope CONTENT is immutable once written (data-model.md,
	// T-revoke-sync / T-reissue "never rewrite applied rows"). A revoke bumps
	// the version (scope follows grant state) and a revoke-then-re-supply cycle
	// bumps it again, so a version persisted before the change never matches
	// afterwards (009 delivery re-check).
	scopeBumpVersionSQL = `
UPDATE withdrawal_authorization_scopes
SET authorization_version = authorization_version + 1
WHERE authorization_id = $1`

	// keyAuthoritySelectSQL re-reads the presented key row FOR SHARE inside the
	// supply tx (T015, R-PB10). Matching the intake lookup predicate exactly
	// means a revocation committed before our SHARE is observed and one later
	// blocks until our COMMIT.
	keyAuthoritySelectSQL = `
SELECT caller_id FROM api_key
WHERE key_hash = $1 AND (revoked_at IS NULL OR revoked_at > now())
FOR SHARE`

	// callerAuthoritySelectSQL re-reads the caller row FOR SHARE (T015,
	// defense in depth: no UPDATE path exists in-tree, but a direct operator
	// write must still be observed).
	callerAuthoritySelectSQL = `
SELECT can_create FROM caller WHERE caller_id = $1 FOR SHARE`
)

// OpInput is one supply op-input — the eight fields bound by data-model
// Table 6 (action, authorization_id, caller_id, chain_id, asset, recipient,
// amount, expires_at) — plus the attempt key OperationID, plus the optional
// PB authorization-scope payload (PB-FR-01: intent_id, request_id, sender, the
// PB-C2 fee triple, the purpose token, and the server-resolved attested_by).
//
// OperationID is the caller-minted, durably-captured attempt identity (R9):
// it is the compare key, not compared content, so operator/reason retry
// metadata never reopens an attempt. SupplyGrant's frozen signature has no
// separate operation-id parameter, so it travels here.
//
// A scopeless OpInput leaves every scope field zero: that is the stock/OPEN
// path, which stays byte-for-byte unchanged. Sender is the scope's mandatory
// identity anchor, so any non-zero scope field makes the scope present and
// Sender/AttestedBy are then required. AttestedBy is always the principal the
// carrier resolved server-side (never caller-supplied) and joins op-input
// equality, so the same operation id from a different principal conflicts.
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

	// Optional PB authorization scope (absent = stock/OPEN).
	IntentID             string
	RequestID            string
	Sender               string
	FeeMaxTotal          int64
	FeeMaxPerGas         int64
	FeeMaxPriority       int64
	AllowsFeeReplacement bool
	AttestedBy           string
}

// scoped reports whether the op-input carries an authorization scope. Sender
// is mandatory inside a scope, so any non-zero scope field marks the scope
// present and the sender/attested_by rules apply.
func (op OpInput) scoped() bool {
	return op.IntentID != "" || op.RequestID != "" || op.Sender != "" ||
		op.FeeMaxTotal != 0 || op.FeeMaxPerGas != 0 || op.FeeMaxPriority != 0 ||
		op.AllowsFeeReplacement
}

// GrantOutcome is the recorded audit action for one attempt plus the grant id
// it names. It is what ReadAttempt and the recovery branches report; it proves
// THIS attempt's outcome (never the grant row's current state).
type GrantOutcome struct {
	Action          string // Table 6 audit action ('supplied', 'resupplied', ...)
	AuthorizationID string // grant id the attempt operated on
}

// In-tx authority check names (T015, R-PB10): a refusal records the failed
// check in the audit reason so the cause is durable, not just returned.
const (
	authorityCheckAPIKey  = "api_key"
	authorityCheckCaller  = "caller"
	authorityCheckIssuers = "issuer_allowlist"
)

// SupplyAuthority is the in-tx authority re-verification input of
// SupplyGrantAuthorized (T015, R-PB10). The presented key is hashed for the
// `FOR SHARE` re-read and is never persisted or logged; the allowlist is
// deployment config. It is not op-input: it never joins convergence comparison
// and never enters the audit snapshot.
type SupplyAuthority struct {
	PresentedKey string
	Issuers      *IssuerAllowlist
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
// the eight Table 6 op-input fields plus the PB scope payload (PB-FR-01);
// OperationID is the key and operator/reason are retry metadata, so none of the
// three appear here. expiring values are normalized to UTC microseconds
// (PostgreSQL timestamptz precision).
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
		"intent_id=" + strconv.Quote(op.IntentID),
		"request_id=" + strconv.Quote(op.RequestID),
		"sender=" + strconv.Quote(op.Sender),
		"fee_max_total=" + strconv.FormatInt(op.FeeMaxTotal, 10),
		"fee_max_per_gas=" + strconv.FormatInt(op.FeeMaxPerGas, 10),
		"fee_max_priority=" + strconv.FormatInt(op.FeeMaxPriority, 10),
		"allows_fee_replacement=" + strconv.FormatBool(op.AllowsFeeReplacement),
		"attested_by=" + strconv.Quote(op.AttestedBy),
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

// scopeRow is one scope row (data-model.md) as read/written by the supply and
// revoke transactions.
type scopeRow struct {
	authorizationID      string
	intentID             string
	requestID            string
	sender               string
	feeMaxTotal          int64
	feeMaxPerGas         int64
	feeMaxPriority       int64
	allowsFeeReplacement bool
	authorizationVersion int64
	attestedBy           string
}

// matchesOp reports whether the stored scope content binds exactly the
// presented scope payload. authorization_version is a monotonic counter, not
// content, so it is excluded (T012 guarded resupply comparison).
func (s scopeRow) matchesOp(op OpInput) bool {
	return s.intentID == op.IntentID && s.requestID == op.RequestID &&
		s.sender == op.Sender && s.feeMaxTotal == op.FeeMaxTotal &&
		s.feeMaxPerGas == op.FeeMaxPerGas && s.feeMaxPriority == op.FeeMaxPriority &&
		s.allowsFeeReplacement == op.AllowsFeeReplacement && s.attestedBy == op.AttestedBy
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

	// PB scope payload (PB-FR-01). The scopeless stock/OPEN path skips this
	// entirely, so pre-extension supply behavior is unchanged.
	if op.scoped() {
		sender, err := canonicalAddressField(op.Sender, "sender")
		if err != nil {
			return OpInput{}, err
		}
		if op.AttestedBy == "" {
			return OpInput{}, New(CodeValidationFailed,
				"attested_by is required on a scoped supply; it is the server-resolved issuance principal, never caller-supplied nor the operator").
				WithField("attested_by")
		}
		op.Sender = sender
	}
	if err := validateFeeScope(op); err != nil {
		return OpInput{}, err
	}
	return op, nil
}

// validateFeeScope enforces the PB-C2 fee triple (data-model.md fee domains +
// the frozen priority <= max_fee cross-check). Amounts are native-coin
// smallest-unit integers, so a negative cap is illegal. A scope that carries
// any fee dimension must carry the applicable caps: a missing fee_max_total or
// fee_max_per_gas is refused, never read as "unlimited". fee_max_priority == 0
// selects the legacy gas_price path (only the per-gas cap applies); a positive
// priority MUST stay within fee_max_per_gas. An all-zero triple is a scope with
// no fee constraint and keeps the pre-extension shape.
func validateFeeScope(op OpInput) error {
	for _, fee := range []struct {
		value int64
		field string
	}{
		{op.FeeMaxTotal, "fee_max_total"},
		{op.FeeMaxPerGas, "fee_max_per_gas"},
		{op.FeeMaxPriority, "fee_max_priority"},
	} {
		if fee.value < 0 {
			return New(CodeValidationFailed, fee.field+" must not be negative").
				WithField(fee.field)
		}
	}
	if op.FeeMaxTotal == 0 && op.FeeMaxPerGas == 0 && op.FeeMaxPriority == 0 {
		return nil
	}
	if op.FeeMaxTotal == 0 {
		return New(CodeValidationFailed,
			"fee_max_total is required once a scope carries a fee dimension; a missing applicable cap is refused").
			WithField("fee_max_total")
	}
	if op.FeeMaxPerGas == 0 {
		return New(CodeValidationFailed,
			"fee_max_per_gas is required once a scope carries a fee dimension; a missing applicable cap is refused").
			WithField("fee_max_per_gas")
	}
	if op.FeeMaxPriority > op.FeeMaxPerGas {
		return New(CodeValidationFailed,
			"fee_max_priority must not exceed fee_max_per_gas (the EIP-1559 priority cap stays within the max_fee cap); use 0 for the legacy gas_price path").
			WithField("fee_max_priority")
	}
	return nil
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

// ReissueInput is one T-reissue (PB-FR-04) attempt: a NEW grant identity Op
// carrying the same business intent as an explicit old grant, with an audit
// detail link to the old grant/request ids. Op is the new grant's op-input and
// MUST be scoped (T-reissue writes a new scope row in the same tx); Old* are
// link metadata only and never become an op-input field of the old row.
//
// Deleted-update rule: the old grant is read (no lock) and then NEVER written —
// re-issue mints a new identity, it does not rewrite. The business intent lives
// in the new scope's intent_id, re-declared by the operator after off-system
// re-verification; no independent intent table exists in this tree, so intent
// and signing-request linkage are re-checked by the 009 lane under H5 (full
// legal-path acceptance post-PB-merge). This procedure invents no such table.
type ReissueInput struct {
	Op                 OpInput
	OldAuthorizationID string
	OldRequestID       string
}

// reissueDetail is the persisted audit `detail` for a T-reissue attempt: the
// ordinary redacted op-input snapshot plus the traceability link to the old
// grant/request ids. It is the same string recovery compares on a same-O retry,
// so the link is part of the recorded attempt, not decoration.
func reissueDetail(op OpInput, oldAuthorizationID, oldRequestID string) string {
	return opInputDetail(op) +
		";reissued_from_authorization_id=" + strconv.Quote(oldAuthorizationID) +
		";reissued_from_request_id=" + strconv.Quote(oldRequestID)
}

// validateReissueInput validates a T-reissue attempt before any pool use and
// returns the normalized new-grant op-input. Re-issue MUST mint a distinct new
// authorization_id (equal ids would mean an UPDATE of the old row) and MUST
// carry a scope payload (the new scope row is written in the same tx).
func validateReissueInput(in ReissueInput) (ReissueInput, error) {
	if in.OldAuthorizationID == "" {
		return ReissueInput{}, New(CodeValidationFailed,
			"reissue requires the old authorization_id so the attempt can link it").
			WithField("reissue_from_authorization_id")
	}
	if in.Op.AuthorizationID == in.OldAuthorizationID {
		return ReissueInput{}, New(CodeValidationFailed,
			"re-issue must mint a NEW authorization_id; rewriting the old grant row is forbidden").
			WithField("authorization_id")
	}
	op, err := validateSupplyOpInput(in.Op, time.Now())
	if err != nil {
		return ReissueInput{}, err
	}
	if !op.scoped() {
		return ReissueInput{}, New(CodeValidationFailed,
			"re-issue writes a new authorization scope; the scope payload is required").
			WithField("sender")
	}
	in.Op = op
	return in, nil
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

// readScopeTx reads the scope row for a grant inside tx (nil when absent =
// pre-extension stock).
func readScopeTx(ctx context.Context, tx pgx.Tx, authorizationID string) (*scopeRow, error) {
	var s scopeRow
	err := tx.QueryRow(ctx, scopeSelectSQL, authorizationID).
		Scan(&s.authorizationID, &s.intentID, &s.requestID, &s.sender,
			&s.feeMaxTotal, &s.feeMaxPerGas, &s.feeMaxPriority,
			&s.allowsFeeReplacement, &s.authorizationVersion, &s.attestedBy)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("read scope: %w", err)
	}
	return &s, nil
}

// readScopeReadOnly reads the scope row via the pool (PK-race recovery, where
// the winner tx has already committed).
func readScopeReadOnly(ctx context.Context, pool *pgxpool.Pool, authorizationID string) (*scopeRow, error) {
	var s scopeRow
	err := pool.QueryRow(ctx, scopeSelectSQL, authorizationID).
		Scan(&s.authorizationID, &s.intentID, &s.requestID, &s.sender,
			&s.feeMaxTotal, &s.feeMaxPerGas, &s.feeMaxPriority,
			&s.allowsFeeReplacement, &s.authorizationVersion, &s.attestedBy)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("read scope: %w", err)
	}
	return &s, nil
}

// insertScopeTx writes the scope row in the same tx as the grant (T012) and
// asserts exactly one row.
func insertScopeTx(ctx context.Context, tx pgx.Tx, op OpInput, version int64) error {
	tag, err := tx.Exec(ctx, scopeInsertSQL, op.AuthorizationID, op.IntentID,
		op.RequestID, op.Sender, op.FeeMaxTotal, op.FeeMaxPerGas, op.FeeMaxPriority,
		op.AllowsFeeReplacement, version, op.AttestedBy)
	if err != nil {
		return fmt.Errorf("insert scope: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("insert scope affected %d rows, want 1", tag.RowsAffected())
	}
	return nil
}

// bumpScopeVersionTx advances the scope version in the revoke/re-supply tx
// (T021). Stock grants carry no scope, so zero rows updated is expected and not
// an error.
func bumpScopeVersionTx(ctx context.Context, tx pgx.Tx, authorizationID string) error {
	if _, err := tx.Exec(ctx, scopeBumpVersionSQL, authorizationID); err != nil {
		return fmt.Errorf("bump scope version: %w", err)
	}
	return nil
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

// insertReissueAuditTx appends the T-reissue audit row: same append-only shape
// as insertAuditTx, but its detail carries the old grant/request link.
func insertReissueAuditTx(ctx context.Context, tx pgx.Tx, in ReissueInput, operator, reason, action string) error {
	tag, err := tx.Exec(ctx, grantAuditInsertSQL, in.Op.OperationID, in.Op.AuthorizationID,
		in.Op.CallerID, action, operator, reason,
		reissueDetail(in.Op, in.OldAuthorizationID, in.OldRequestID))
	if err != nil {
		return fmt.Errorf("insert grant audit: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("insert grant audit affected %d rows, want 1", tag.RowsAffected())
	}
	return nil
}

// runSupplyTx owns BEGIN..COMMIT for one supply attempt. auth is the optional
// in-tx authority re-check (T015); nil keeps the transport-free library path.
func runSupplyTx(ctx context.Context, pool *pgxpool.Pool, op OpInput, auth *SupplyAuthority, operator, reason string) (*GrantOutcome, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin supply transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after COMMIT

	if _, err := tx.Exec(ctx, grantWriteGuard); err != nil {
		return nil, fmt.Errorf("supply transaction statement guard: %w", err)
	}
	// T015 / R-PB10 lock order: authority shares before the grant FOR UPDATE.
	// A failure records the refusal and writes no grant/scope row.
	if auth != nil {
		check, err := verifySupplyAuthority(ctx, tx, *auth)
		if err != nil {
			return nil, err
		}
		if check != "" {
			if err := insertAuditTx(ctx, tx, op, op.CallerID, operator,
				"supply authority: "+check, grantOutcomeSupplyRefused); err != nil {
				return nil, err
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, &grantCommitUnknownError{err: err}
			}
			return nil, authorityRefused(check)
		}
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
		// T012: a scoped first supply writes grant + scope + audit atomically
		// (data-model.md T-supply+scope); scopeless stock stays unchanged.
		if op.scoped() {
			if err := insertScopeTx(ctx, tx, op, 1); err != nil {
				return nil, err
			}
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
		// T012: the guarded resupply comparison extends to the scope content;
		// a differing or absent scope is a conflict with zero writes. T021: a
		// re-supply of a non-active grant is the revoke-then-re-supply cycle
		// and bumps the monotonic version.
		if op.scoped() {
			scope, err := readScopeTx(ctx, tx, op.AuthorizationID)
			if err != nil {
				return nil, err
			}
			if scope == nil || !scope.matchesOp(op) {
				return nil, New(CodeOperationConflict,
					"grant scope parameters changed under the equality check").
					WithField("authorization_id")
			}
			if grant.state != "active" {
				if err := bumpScopeVersionTx(ctx, tx, op.AuthorizationID); err != nil {
					return nil, err
				}
			}
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
	case grant == nil: // missing grant on revoke → refused; caller_id stays 0 = honest unattributable marker (no grant to attribute and no approved caller input; never operator-as-caller)
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

// runReissueTx owns BEGIN..COMMIT for one T-reissue attempt. It writes only the
// three new rows (grant, scope, audit) and never touches the old grant: the old
// grant is read without a lock purely to refuse linking to a nonexistent row.
// auth is the optional in-tx authority re-check, identical to the supply path.
func runReissueTx(ctx context.Context, pool *pgxpool.Pool, in ReissueInput, auth *SupplyAuthority, operator, reason string) (*GrantOutcome, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin reissue transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, grantWriteGuard); err != nil {
		return nil, fmt.Errorf("reissue transaction statement guard: %w", err)
	}
	if auth != nil {
		check, err := verifySupplyAuthority(ctx, tx, *auth)
		if err != nil {
			return nil, err
		}
		if check != "" {
			if err := insertReissueAuditTx(ctx, tx, in, operator,
				"supply authority: "+check, grantOutcomeSupplyRefused); err != nil {
				return nil, err
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, &grantCommitUnknownError{err: err}
			}
			return nil, authorityRefused(check)
		}
	}

	var oldState string
	err = tx.QueryRow(ctx, grantOldSelectSQL, in.OldAuthorizationID).Scan(&oldState)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, New(CodeValidationFailed,
			"reissue_from_authorization_id does not exist; re-issue links an existing grant").
			WithField("reissue_from_authorization_id")
	case err != nil:
		return nil, fmt.Errorf("read old grant: %w", err)
	}

	tag, err := tx.Exec(ctx, grantInsertSQL, in.Op.AuthorizationID, in.Op.CallerID, in.Op.ChainID,
		in.Op.Asset, in.Op.Recipient, in.Op.Amount, in.Op.ExpiresAt, operator)
	if err != nil {
		return nil, fmt.Errorf("insert grant: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("insert grant affected %d rows, want 1", tag.RowsAffected())
	}
	if err := insertScopeTx(ctx, tx, in.Op, 1); err != nil {
		return nil, err
	}
	if err := insertReissueAuditTx(ctx, tx, in, operator, reason, grantOutcomeSupplied); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, &grantCommitUnknownError{err: err}
	}
	return &GrantOutcome{Action: grantOutcomeSupplied, AuthorizationID: in.Op.AuthorizationID}, nil
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

// resolveReissueByOperationID is the T-reissue recovery for a 23505 on the
// audit operation-id UNIQUE and for an uncertain COMMIT: read the recorded
// attempt, compare it against the same reissue detail (op-input + old link),
// and report. A miss is retryable with the SAME operation id; a different
// recorded detail is operation_conflict, and a recorded refusal is never
// upgraded.
func resolveReissueByOperationID(ctx context.Context, pool *pgxpool.Pool, in ReissueInput) (*GrantOutcome, error) {
	row, found, err := readAttemptPool(ctx, pool, in.Op.OperationID)
	if err != nil {
		return nil, grantRetryable(err)
	}
	if !found {
		return nil, New(CodeTemporarilyUnavailable,
			"operation outcome is not yet visible; retry with the same operation_id")
	}
	if row.detail != reissueDetail(in.Op, in.OldAuthorizationID, in.OldRequestID) {
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
	matched := grant != nil && grant.matchesOp(op)
	// T012: the recovery re-read includes the scope row, so a scopeless winner
	// never reports a scoped attempt as resupplied (and vice versa).
	if matched && op.scoped() {
		scope, err := readScopeReadOnly(ctx, pool, op.AuthorizationID)
		if err != nil {
			return nil, grantRetryable(err)
		}
		matched = scope != nil && scope.matchesOp(op)
	}
	if matched {
		return boundedAudit(ctx, pool, op, op.CallerID, operator, reason, grantOutcomeResupplied)
	}
	return boundedAudit(ctx, pool, op, op.CallerID, operator, reason, grantOutcomeSupplyRefused)
}

// verifySupplyAuthority performs the T015 in-tx authority re-verification
// (R-PB10): the api_key row FOR SHARE, then the caller row FOR SHARE, then
// PermitIssue against the loaded mapping. It returns the name of the failed
// check, or "" when authorized. An empty presented key can never match a row,
// so it refuses rather than skipping the check.
func verifySupplyAuthority(ctx context.Context, tx pgx.Tx, auth SupplyAuthority) (string, error) {
	var callerID int64
	err := tx.QueryRow(ctx, keyAuthoritySelectSQL, HashKey(auth.PresentedKey)).Scan(&callerID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return authorityCheckAPIKey, nil
	case err != nil:
		return "", fmt.Errorf("re-read authority api key: %w", err)
	}
	var canCreate bool
	err = tx.QueryRow(ctx, callerAuthoritySelectSQL, callerID).Scan(&canCreate)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return authorityCheckCaller, nil
	case err != nil:
		return "", fmt.Errorf("re-read authority caller: %w", err)
	}
	if !auth.Issuers.PermitIssue(callerID) {
		return authorityCheckIssuers, nil
	}
	return "", nil
}

// authorityRefused is the classified refusal returned after the in-tx authority
// check commits its `supply_refused` audit row; the check name is both durable
// (audit reason) and reported to the carrier.
func authorityRefused(check string) *Error {
	return New(CodeUnauthorized, "supply refused: authority check failed: "+check)
}

// supply runs the shared validation + transaction + recovery path; auth is nil
// for SupplyGrant and non-nil for SupplyGrantAuthorized.
func supply(ctx context.Context, pool *pgxpool.Pool, op OpInput, auth *SupplyAuthority, operator, reason string) (*GrantOutcome, error) {
	norm, err := validateSupplyOpInput(op, time.Now())
	if err != nil {
		return nil, err
	}
	out, err := runSupplyTx(ctx, pool, norm, auth, operator, reason)
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

// SupplyGrant runs one supply attempt. It owns BEGIN..COMMIT (do not nest it in
// another transaction). The operation id is op.OperationID and MUST be non-empty
// and durably captured first; invalid op-input fails with CodeValidationFailed
// before any database access. 23505/commit recovery per data-model Table 6.
// It performs no authority check: the caller asserts it is authorized
// (R-PB9 transport-free library surface); use SupplyGrantAuthorized for the
// in-tx re-verification.
func SupplyGrant(ctx context.Context, pool *pgxpool.Pool, op OpInput, operator, reason string) (*GrantOutcome, error) {
	return supply(ctx, pool, op, nil, operator, reason)
}

// SupplyGrantAuthorized is SupplyGrant plus the T015 in-tx authority
// re-verification: before any grant/scope write it re-reads the presented key
// and caller rows FOR SHARE and re-evaluates PermitIssue. A failed check
// records `supply_refused` naming the check and writes zero grant/scope rows.
func SupplyGrantAuthorized(ctx context.Context, pool *pgxpool.Pool, op OpInput, auth SupplyAuthority, operator, reason string) (*GrantOutcome, error) {
	return supply(ctx, pool, op, &auth, operator, reason)
}

// reissue runs the shared validation + transaction + recovery path for a
// T-reissue; auth is nil for ReissueGrant and non-nil for ReissueGrantAuthorized.
func reissue(ctx context.Context, pool *pgxpool.Pool, in ReissueInput, auth *SupplyAuthority, operator, reason string) (*GrantOutcome, error) {
	norm, err := validateReissueInput(in)
	if err != nil {
		return nil, err
	}
	out, err := runReissueTx(ctx, pool, norm, auth, operator, reason)
	if err == nil {
		return out, nil
	}
	var unknown *grantCommitUnknownError
	switch {
	case errors.As(err, &unknown):
		return resolveReissueByOperationID(ctx, pool, norm)
	case grantPgError(err) != nil:
		pgErr := grantPgError(err)
		if pgErr.Code == "23505" {
			switch pgErr.ConstraintName {
			case grantAuditOperationIDUniq:
				return resolveReissueByOperationID(ctx, pool, norm)
			case grantAuthorizationsPK:
				// The NEW id already carries a grant: re-issue must mint a fresh
				// identity, so this is a conflict, never an adopted resupply.
				return nil, New(CodeOperationConflict,
					"authorization_id already has a grant; re-issue must mint a NEW grant id").
					WithField("authorization_id")
			}
		}
		if pgErr.Code == "23503" {
			return nil, New(CodeValidationFailed, "caller_id does not exist").
				WithField("caller_id")
		}
	}
	return nil, grantRetryable(err)
}

// ReissueGrant runs one T-reissue attempt (PB-FR-04): a NEW grant id + NEW
// scope row in one supply tx, with an audit detail linking the old
// grant/request ids. The old rows are never updated — this mints a new identity
// rather than rewriting one. It owns BEGIN..COMMIT (do not nest it) and
// performs no authority check; use ReissueGrantAuthorized for the in-tx
// re-verification.
func ReissueGrant(ctx context.Context, pool *pgxpool.Pool, in ReissueInput, operator, reason string) (*GrantOutcome, error) {
	return reissue(ctx, pool, in, nil, operator, reason)
}

// ReissueGrantAuthorized is ReissueGrant plus the in-tx authority
// re-verification, exactly as SupplyGrantAuthorized adds it to SupplyGrant.
func ReissueGrantAuthorized(ctx context.Context, pool *pgxpool.Pool, in ReissueInput, auth SupplyAuthority, operator, reason string) (*GrantOutcome, error) {
	return reissue(ctx, pool, in, &auth, operator, reason)
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
