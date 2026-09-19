// submit.go owns the submit path (T016; FR-13/FR-14/FR-15): the fixed request
// processing order (api.md §4), insert-first identity binding, 23505 classify,
// own-row lock, 006/008/007 gates, policy, KeyProvider.SignTx, result persist,
// and the same-identity replay path (persistence.md §§1/3). One transaction
// owns the decision: signature_results is committed before any response byte,
// and a failure rolls the whole transaction back (no partial success). 009
// reads upstream gates read-only and never broadcasts.
package signer

import (
	"context"
	"encoding/hex"
	"errors"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// submitWriteGuard bounds every statement of the submit transaction (the
// repo's existing 5s guard literal), the LOCK TABLE wait included.
const submitWriteGuard = "SET LOCAL statement_timeout = '5s'"

// submitClockSQL evaluates grant expiry on the database clock (gates.md §2:
// no application clock).
const submitClockSQL = `SELECT clock_timestamp()`

// submitAnchorSQL returns the anchor (non-replacement) row a fee-replacement
// request attaches to: same caller, intent_id and binding_ref on the same
// authorization, a different request identity (OC-4; research R7). No row means
// no anchor: the request is the first use of its grant. The row carries the
// anchor's request identity (the identity the carrier's request_id binds) and
// the payment binding a legitimate replacement must preserve (V13-4b); the
// anchor's authorization_id is never updated ("旧请求换绑禁止"). Pure SELECT.
const submitAnchorSQL = `SELECT id, signing_request_id, chain_id, sender, nonce::text, tx_type,
	to_addr, value::text, data, gas_limit::text, asset, recipient, amount::text
	FROM signing_requests
	WHERE caller_id = $1 AND intent_id = $2 AND binding_ref = $3
	  AND authorization_id = $4 AND replacement_of IS NULL
	  AND signing_request_id <> $5
	LIMIT 1`

// submitInsertSQL binds identity + full content before any gate read
// (persistence.md §1: identity and envelope first). $26 is the anchor row id
// for an OC-5 fee replacement, NULL for an anchor (H3/T038). authorization_state
// / authorization_fingerprint are provisional until the 007 grant read fills
// them; policy_version is known up front from the frozen policy.
const submitInsertSQL = `INSERT INTO signing_requests (
	caller_id, signing_request_id, attempt_id, replacement_of, intent_id, binding_ref,
	recovery_version, chain_id, sender, nonce, tx_type, to_addr, value, data,
	gas_limit, gas_price, max_fee_per_gas, max_priority_fee_per_gas,
	asset, recipient, amount, canonical_envelope, content_hash,
	authorization_id, authorization_fingerprint, authorization_state, policy_version
) VALUES ($1,$2,$3,$26,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,'unknown',$25)
RETURNING id`

// submitLockOwnSQL serializes concurrent same-identity attempts on the row the
// insert-first path just created (persistence.md §1).
const submitLockOwnSQL = `SELECT id FROM signing_requests WHERE id = $1 FOR UPDATE`

// submitRejectSQL records a terminal refusal on the owned row so a
// same-identity retry converges on the same class instead of re-driving.
const submitRejectSQL = `UPDATE signing_requests
	SET state = 'rejected', refusal_class = $1, updated_at = clock_timestamp()
	WHERE id = $2`

// submitFingerprintSQL fills the observed 007 grant fingerprint/state and the
// observed PB scope version over the provisional insert values once the grant
// read has run. authorization_version stays NULL when no scope was observed, so
// a carrier appearing before delivery can never compare equal (H2/T037).
const submitFingerprintSQL = `UPDATE signing_requests
	SET authorization_fingerprint = $1, authorization_state = $2, authorization_version = $3, updated_at = clock_timestamp()
	WHERE id = $4`

// submitResultSQL is THE durable signing result (FR-13), written inside the
// signing transaction before any response byte.
const submitResultSQL = `INSERT INTO signature_results (signing_request_row, signature, tx_hash)
	VALUES ($1, $2, $3)`

// submitSignedSQL is the terminal success transition (same transaction as the
// result row).
const submitSignedSQL = `UPDATE signing_requests
	SET state = 'signed', refusal_class = '', updated_at = clock_timestamp()
	WHERE id = $1`

// submitAuditSQL appends one decision/refusal audit row (Table 5).
const submitAuditSQL = `INSERT INTO signing_request_audit
	(signing_request_id, caller_id, action, reason_class, detail)
	VALUES ($1, $2, $3, $4, $5)`

// SubmitDeps are the submit transaction's collaborators: the pool, the frozen
// policy, the key provider, the 008 binding reader, and the 008 scope-row
// FOR SHARE locker. ScopeLock is mandatory: a missing locker refuses before
// BEGIN rather than silently skipping the submit-side lock.
type SubmitDeps struct {
	DB        DB
	Policy    *Policy
	Provider  KeyProvider
	Binding   BindingReader
	ScopeLock ScopeLocker
}

// SubmitResponse carries the persisted signing result facts (api.md §2): the
// signature value and transaction hash, never raw signed-transaction bytes.
// Delivery admission (T034) shapes the wire response later.
type SubmitResponse struct {
	Signature string
	TxHash    string
}

// Submit runs the fixed processing order (api.md §4): authenticate (generic
// 401), permission (403), strict body shape (400/422), field semantics (422),
// then the submit transaction. Refusals are *RefusalError; the serving layer
// maps Class to HTTP. A same-identity replay returns the persisted result.
func Submit(ctx context.Context, deps SubmitDeps, presentedCredential string, body []byte) (*SubmitResponse, error) {
	auth, err := Authenticate(ctx, deps.DB, presentedCredential)
	if err != nil {
		return nil, err
	}
	if err := PermitSigning(auth.Caller); err != nil {
		return nil, err
	}
	req, err := DecodeRequest(body)
	if err != nil {
		return nil, err
	}
	if err := Validate(&req); err != nil {
		return nil, err
	}
	return submitFirst(ctx, deps, auth.Caller, &req)
}

// submitFirst owns BEGIN..COMMIT for the first receipt of an identity
// (persistence.md §1): statement guard → 008 scope-row SHARE lock → 006
// gate-table SHARE lock → plain INSERT (23505 → replay) → own row FOR UPDATE →
// 006 → 008 → 007 → policy → sign → result → state → audit → COMMIT.
func submitFirst(ctx context.Context, deps SubmitDeps, caller Caller, req *Request) (*SubmitResponse, error) {
	if deps.DB == nil || deps.Policy == nil || deps.Provider == nil || deps.Binding == nil || deps.ScopeLock == nil {
		return nil, refuse(ClassStorageUnavailable, "", "submit dependencies incomplete")
	}
	envelope, err := req.CanonicalEnvelope()
	if err != nil {
		return nil, err
	}
	contentHash, err := req.ContentHash()
	if err != nil {
		return nil, err
	}
	dataBytes, err := hexutil.Decode(req.Data)
	if err != nil {
		return nil, refuse(ClassValidationFailed, "data", "data must be 0x-hex")
	}

	tx, err := deps.DB.Begin(ctx)
	if err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, submitWriteGuard); err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}

	// Lock order (persistence.md §1, R6; gates.md §3): the 008 scope-row FOR
	// SHARE comes FIRST, before the 006 gate-table lock and every gate read, and
	// is held to COMMIT — a racing 008 pause/registry/release writer waits, and
	// one that committed first is observed by the reads below. The delivery-side
	// re-check does not substitute for this submit-side lock. The sender is
	// canonicalized the same way the request row is, so the lock hits the
	// lowercase nonce_scope_state row.
	if err := deps.ScopeLock.LockScope(ctx, tx, int64(req.ChainID), lowerAddr(req.Sender)); err != nil {
		return nil, refuse(ClassGateReadFailed, "", "scope lock failed")
	}
	if _, err := tx.Exec(ctx, GateLockSQL); err != nil {
		return nil, refuse(ClassGateReadFailed, "", "gate lock failed")
	}

	// H3 (T038): a fee-replacement request is a new identity (new
	// signing_request_id + attempt_id) over the same caller + intent_id +
	// binding_ref (OC-4). When it names its anchor's grant it is inserted as
	// that anchor's replacement, so the partial anchor index keeps exactly one
	// non-replacement request per grant; the reuse itself is gated on the
	// scope's purpose token + fee range + the anchor's preserved payment
	// binding after the grant read below (V13-4b). The lookup is a plain read
	// of 009's own rows: no new lock object.
	anchor, err := findAnchorRow(ctx, tx, caller.ID, req)
	if err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}

	rowID, err := insertRequestRow(ctx, tx, caller.ID, anchor, req, deps.Policy.Version(), envelope, contentHash, dataBytes)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			_ = tx.Rollback(ctx)
			return classifyDuplicate(ctx, deps, caller, req, envelope, pgErr.ConstraintName)
		}
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}

	var lockedID int64
	if err := tx.QueryRow(ctx, submitLockOwnSQL, rowID).Scan(&lockedID); err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}

	gate, err := readRecoveryGate(ctx, tx, int64(req.ChainID))
	if err != nil {
		return nil, submitRefusal(ctx, tx, deps, rowID, caller.ID, req, ClassGateReadFailed, "", "006 gate read failed", "006 gate read failed")
	}
	if class := gate.Evaluate(int64(req.RecoveryVersion)); class != "" {
		return nil, submitRefusal(ctx, tx, deps, rowID, caller.ID, req, class, "", string(class), gateRefusalDetail(gate, class, req.RecoveryVersion))
	}

	binding, berr := deps.Binding.ReadBinding(ctx, req.IntentID, req.AttemptID)
	if class := BindingRefusal(binding, berr); class != "" {
		return nil, submitRefusal(ctx, tx, deps, rowID, caller.ID, req, class, "", string(class), "binding_class="+bindingClassName(binding))
	}

	grant, scope, found, err := readGrantForShare(ctx, tx, req.AuthorizationID)
	if err != nil {
		return nil, submitRefusal(ctx, tx, deps, rowID, caller.ID, req, ClassGateReadFailed, "", "007 grant read failed", "007 grant read failed")
	}
	var now time.Time
	if err := tx.QueryRow(ctx, submitClockSQL).Scan(&now); err != nil {
		return nil, submitRefusal(ctx, tx, deps, rowID, caller.ID, req, ClassGateReadFailed, "", "database clock read failed", "007 grant read failed")
	}
	if class := EvaluateGrant(found, grant, caller.ID, req, now); class != "" {
		return nil, submitRefusal(ctx, tx, deps, rowID, caller.ID, req, class, "authorization_id", string(class), "authorization_id="+req.AuthorizationID+" state="+grantState(grant, found))
	}
	fingerprint, state := "unknown", "unknown"
	var authVersion *int64
	if found && grant != nil {
		// Persist the bare digest; the column CHECK admits hex only.
		fingerprint, state = strings.TrimPrefix(grant.Fingerprint(), AuthzFingerprintDomain+":"), grant.State
		if scope.Present {
			// H2: snapshot the observed PB scope version alongside the
			// fingerprint. A scope absent at submit stays NULL so a carrier
			// appearing later is never silently adopted.
			version := scope.AuthorizationVersion
			authVersion = &version
		}
	}
	if _, err := tx.Exec(ctx, submitFingerprintSQL, fingerprint, state, authVersion, rowID); err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}

	// H3 (T038): a fee replacement sharing its anchor's grant is admitted only
	// under OC-5's conditional rule — the carrier explicitly permits the
	// fee-replacement purpose and the fee stays in range (PB-FR-05, R7/R11).
	// Otherwise the caller must obtain a fresh authorization and re-issue via
	// PB; the refusal is recorded on the new identity, zero signatures, and the
	// anchor row is never rebound.
	//
	// V13-4b identity split: the replacement's new signing identity is NOT the
	// carrier's bound request identity — the carrier binds the anchor's
	// originating request (request_id). The reuse therefore preserves the
	// anchor's payment binding (chain/sender/nonce/tx shape/payload/
	// gas_limit/asset/recipient/amount; the fee dimensions are the one free
	// axis) and the carrier coverage below is evaluated against the anchor's
	// signing_request_id, never the replacement's own new identity.
	boundRequestID := req.SigningRequestID
	if anchor != nil {
		if class := EvaluateGrantReuse(scope, req); class != "" {
			return nil, submitRefusal(ctx, tx, deps, rowID, caller.ID, req, class, "authorization_id", string(class), reuseRefusalDetail(anchor.RowID, scope))
		}
		if class := EvaluateAnchorBinding(*anchor, req); class != "" {
			return nil, submitRefusal(ctx, tx, deps, rowID, caller.ID, req, class, "authorization_id", string(class), anchorBindingRefusalDetail(*anchor, req))
		}
		boundRequestID = anchor.SigningRequestID
	}

	// H4 (T039): the decision consumes the carrier read in the same FOR SHARE
	// sequence above, never a hardcoded literal. A scopeless stock grant is
	// refused authorization_unverifiable per request (PB-FR-04, R11); only a
	// present-and-verifiable scope passes. The detail records the carrier as
	// actually observed.
	if class := EvaluateGrantScopeBoundIdentity(scope, req, boundRequestID); class != "" {
		return nil, submitRefusal(ctx, tx, deps, rowID, caller.ID, req, class, "authorization_id", string(class), "authorization_id="+req.AuthorizationID+" scope_carrier="+scopeCarrierState(scope))
	}

	if err := deps.Policy.Check(req); err != nil {
		re := asRefusal(err)
		return nil, submitRefusal(ctx, tx, deps, rowID, caller.ID, req, re.Class, re.Field, re.Msg, "policy_version="+deps.Policy.Version())
	}

	built, err := req.Transaction()
	if err != nil {
		re := asRefusal(err)
		return nil, submitRefusal(ctx, tx, deps, rowID, caller.ID, req, re.Class, re.Field, re.Msg, "transaction reconstruction failed")
	}
	signed, err := deps.Provider.SignTx(ctx, common.HexToAddress(req.Sender), new(big.Int).SetUint64(req.ChainID), built)
	if err != nil {
		re := asRefusal(err)
		return nil, submitRefusal(ctx, tx, deps, rowID, caller.ID, req, re.Class, re.Field, re.Msg, "key_provider")
	}
	sig := signatureBytes(signed, req.ChainID)
	sigHex := "0x" + hex.EncodeToString(sig)
	if _, _, err := req.VerifySignature(sig); err != nil {
		re := asRefusal(err)
		return nil, submitRefusal(ctx, tx, deps, rowID, caller.ID, req, re.Class, re.Field, re.Msg, "independent verification failed")
	}
	txHash := signed.Hash().Hex()

	if _, err := tx.Exec(ctx, submitResultSQL, rowID, sigHex, txHash); err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}
	if _, err := tx.Exec(ctx, submitSignedSQL, rowID); err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}
	if _, err := tx.Exec(ctx, submitAuditSQL, req.SigningRequestID, caller.ID, "signed", "", "policy_version="+deps.Policy.Version()); err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}
	return &SubmitResponse{Signature: sigHex, TxHash: txHash}, nil
}

// findAnchorRow reads the anchor a fee-replacement request attaches to; nil
// means no anchor (the request is the first use of its grant, or a fresh-grant
// replacement that is its own anchor — research R7). The snapshot carries the
// anchor's request identity (the carrier's bound request_id) and the payment
// binding the replacement must preserve (V13-4b).
func findAnchorRow(ctx context.Context, tx pgx.Tx, callerID int64, req *Request) (*ReplacementAnchor, error) {
	var a ReplacementAnchor
	err := tx.QueryRow(ctx, submitAnchorSQL,
		callerID, req.IntentID, req.BindingRef, req.AuthorizationID, req.SigningRequestID).Scan(
		&a.RowID, &a.SigningRequestID, &a.ChainID, &a.Sender, &a.Nonce, &a.TxType,
		&a.To, &a.Value, &a.Data, &a.GasLimit, &a.Asset, &a.Recipient, &a.Amount)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// insertRequestRow is the plain identity+content INSERT (state='received');
// anchor non-nil marks an OC-5 replacement (H3/T038).
func insertRequestRow(ctx context.Context, tx pgx.Tx, callerID int64, anchor *ReplacementAnchor, req *Request, policyVersion string, envelope []byte, contentHash common.Hash, data []byte) (int64, error) {
	var anchorRowID any
	if anchor != nil {
		anchorRowID = anchor.RowID
	}
	var rowID int64
	err := tx.QueryRow(ctx, submitInsertSQL,
		callerID, req.SigningRequestID, req.AttemptID, req.IntentID, req.BindingRef,
		int64(req.RecoveryVersion), int64(req.ChainID), lowerAddr(req.Sender), req.Nonce, int(req.TxType),
		lowerAddr(req.To), req.Value, data, req.GasLimit,
		nullIfEmpty(req.GasPrice), nullIfEmpty(req.MaxFeePerGas), nullIfEmpty(req.MaxPriorityFeePerGas),
		lowerAddr(req.Asset), lowerAddr(req.Recipient), req.Amount,
		envelopeText(envelope), contentHash.Hex(), req.AuthorizationID, strings.Repeat("0", 64), policyVersion,
		anchorRowID,
	).Scan(&rowID)
	return rowID, err
}

// envelopeText renders the canonical envelope for storage. The envelope carries
// a NUL domain separator that PostgreSQL TEXT cannot hold, so it is persisted
// as 0x-hex: exact, lossless, and still the byte artifact compared on
// retry/conflict (never re-derived from parsed columns).
func envelopeText(envelope []byte) string {
	return "0x" + hex.EncodeToString(envelope)
}

// classifyDuplicate handles a 23505 from the insert-first path
// (persistence.md §3 T-submit-replay). The exact constraint name decides: only
// the caller+request identity uniqueness is a replay; a grant or attempt
// collision is a terminal client conflict.
func classifyDuplicate(ctx context.Context, deps SubmitDeps, caller Caller, req *Request, envelope []byte, constraint string) (*SubmitResponse, error) {
	switch constraint {
	case "signing_requests_caller_request_uniq":
		return replayExisting(ctx, deps, caller, req, envelope)
	case "signing_requests_authorization_anchor_uniq":
		return nil, refuse(ClassAuthorizationInvalid, "authorization_id", "authorization already backs another request identity")
	case "signing_requests_attempt_uniq":
		return nil, refuse(ClassRequestConflict, "attempt_id", "attempt identity already belongs to another request")
	default:
		return nil, refuse(ClassStorageUnavailable, "", "unexpected duplicate identity")
	}
}

// replayExisting converges a same-identity duplicate on durable state: equal
// envelope → the persisted result (or the persisted refusal); different
// envelope → 409 request_conflict; no result yet → outcome_not_yet_visible,
// with a same-identity retry instruction and no second signature.
func replayExisting(ctx context.Context, deps SubmitDeps, caller Caller, req *Request, envelope []byte) (*SubmitResponse, error) {
	var (
		rowID          int64
		storedEnvelope string
		state          string
		refusalClass   string
	)
	err := deps.DB.QueryRow(ctx,
		`SELECT id, canonical_envelope, state, refusal_class FROM signing_requests
		  WHERE caller_id = $1 AND signing_request_id = $2`,
		caller.ID, req.SigningRequestID).Scan(&rowID, &storedEnvelope, &state, &refusalClass)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, refuse(ClassOutcomeNotYetVisible, "", "identity not yet visible; retry the same identity")
	}
	if err != nil {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}
	if storedEnvelope != envelopeText(envelope) {
		bestEffortAudit(ctx, deps.DB, req.SigningRequestID, caller.ID, "conflict", ClassRequestConflict, "envelope differs from the persisted identity")
		return nil, refuse(ClassRequestConflict, "", "signing_request_id is bound to a different envelope")
	}

	var signature, txHash string
	err = deps.DB.QueryRow(ctx,
		`SELECT signature, tx_hash FROM signature_results WHERE signing_request_row = $1`,
		rowID).Scan(&signature, &txHash)
	if err == nil {
		return &SubmitResponse{Signature: signature, TxHash: txHash}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, refuse(ClassStorageUnavailable, "", "storage unavailable")
	}
	if state == string(StateRejected) && refusalClass != "" {
		return nil, &RefusalError{Class: RefusalClass(refusalClass), Msg: "replay of the persisted refusal"}
	}
	return nil, refuse(ClassOutcomeNotYetVisible, "", "result not yet durably visible; retry the same identity")
}

// submitRefusal records a refusal inside the open transaction. A terminal
// class (RetryNever) commits state='rejected' + audit so a same-identity retry
// converges on the same refusal; a transient class rolls back so the retry
// re-drives the gates afresh, with a best-effort audit append (research R4,
// persistence.md §3/§4).
func submitRefusal(ctx context.Context, tx pgx.Tx, deps SubmitDeps, rowID, callerID int64, req *Request, class RefusalClass, field, msg, detail string) error {
	re := refuse(class, field, msg)
	action := auditActionFor(class)
	if RetryabilityOf(class) == RetryNever {
		if _, err := tx.Exec(ctx, submitRejectSQL, string(class), rowID); err != nil {
			return refuse(ClassStorageUnavailable, "", "storage unavailable")
		}
		if _, err := tx.Exec(ctx, submitAuditSQL, req.SigningRequestID, callerID, action, string(class), detail); err != nil {
			return refuse(ClassStorageUnavailable, "", "storage unavailable")
		}
		if err := tx.Commit(ctx); err != nil {
			return refuse(ClassStorageUnavailable, "", "storage unavailable")
		}
		return re
	}
	_ = tx.Rollback(ctx)
	bestEffortAudit(ctx, deps.DB, req.SigningRequestID, callerID, action, class, detail)
	return re
}

// bestEffortAudit appends one refusal attribution outside any transaction;
// Table 5 allows a best-effort single-statement append when no request
// transaction survives.
func bestEffortAudit(ctx context.Context, db DB, requestID string, callerID int64, action string, class RefusalClass, detail string) {
	_, _ = db.Exec(ctx, submitAuditSQL, requestID, callerID, action, string(class), detail)
}

// readRecoveryGate is the one-statement 006 snapshot read (gates.md §1); the
// caller holds the gate-table SHARE lock.
func readRecoveryGate(ctx context.Context, tx pgx.Tx, chainID int64) (RecoveryGate, error) {
	var (
		g          RecoveryGate
		recoveryID *string
		phase      *string
		seq        *int64
		eventsMax  *int64
	)
	if err := tx.QueryRow(ctx, GateReadSQL, chainID).Scan(
		&g.IndexerPaused, &g.LogPaused, &g.DepositPaused,
		&recoveryID, &phase, &seq, &eventsMax); err != nil {
		return g, err
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
	return g, nil
}

// readGrantForShare reads the 007 grant FOR SHARE and, in the same statement
// sequence of the same transaction, the 1:1 PB carrier row for the same
// authorization_id FOR SHARE (gates.md §2; PB data-model "grant + scope
// FOR SHARE in one sequence"). The scope read adds no lock object: it rides
// the grant-row coordination, so a revoke/re-supply committing before the
// grant share lock is observed and one racing it waits for this transaction.
// Grant absence is found=false; scope absence is Present=false (pre-extension
// stock).
func readGrantForShare(ctx context.Context, tx pgx.Tx, authorizationID string) (*AuthzGrant, GrantScope, bool, error) {
	var (
		g       AuthzGrant
		amount  string
		expires *time.Time
	)
	err := tx.QueryRow(ctx, GrantReadSQL, authorizationID).Scan(
		&g.CallerID, &g.ChainID, &g.Asset, &g.Recipient, &amount, &g.State, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, GrantScope{}, false, nil
	}
	if err != nil {
		return nil, GrantScope{}, false, err
	}
	if v, ok := new(big.Int).SetString(amount, 10); ok {
		g.Amount = v
	}
	g.ExpiresAt = expires

	scope, err := readGrantScopeForShare(ctx, tx, authorizationID)
	if err != nil {
		return nil, GrantScope{}, false, err
	}
	return &g, scope, true, nil
}

// readGrantScopeForShare reads the PB carrier row FOR SHARE immediately after
// the caller's grant read. Absence is Present=false, never an error; the read
// is pure SELECT.
func readGrantScopeForShare(ctx context.Context, tx pgx.Tx, authorizationID string) (GrantScope, error) {
	var s GrantScope
	err := tx.QueryRow(ctx, GrantScopeReadSQL, authorizationID).Scan(
		&s.AuthorizationID, &s.IntentID, &s.RequestID, &s.Sender,
		&s.FeeMaxTotal, &s.FeeMaxPerGas, &s.FeeMaxPriority, &s.AllowsFeeReplacement,
		&s.AuthorizationVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, nil
	}
	if err != nil {
		return GrantScope{}, err
	}
	s.Present = true
	return s, nil
}

// signatureBytes renders the 65-byte [R||S||V] signature the content layer
// verifies: V is the recovery id, normalized out of EIP-155's chain-bound
// legacy value.
func signatureBytes(signed *types.Transaction, chainID uint64) []byte {
	v, r, s := signed.RawSignatureValues()
	sig := make([]byte, 65)
	r.FillBytes(sig[0:32])
	s.FillBytes(sig[32:64])
	recid := v.Uint64()
	if signed.Type() == types.LegacyTxType && chainID != 0 {
		if offset := 2*chainID + 35; recid >= offset {
			recid -= offset
		}
	}
	sig[64] = byte(recid)
	return sig
}

// auditActionFor maps a refusal class to the Table 5 action vocabulary.
func auditActionFor(class RefusalClass) string {
	switch class {
	case ClassRecoveryPaused, ClassRecoveryActive, ClassRecoveryVersionChanged, ClassGateReadFailed:
		return "gate_refused"
	case ClassBindingAbsent, ClassBindingConflict, ClassBindingPaused, ClassBindingTerminal, ClassBindingReadFailed:
		return "binding_refused"
	case ClassAuthorizationInvalid, ClassAuthorizationExpired, ClassAuthorizationRevoked, ClassAuthorizationUnverifiable:
		return "authorization_refused"
	case ClassRequestConflict:
		return "conflict"
	case ClassKeyProviderUnavailable, ClassKeyProviderTimeout, ClassStorageUnavailable:
		return "failed"
	default:
		return "rejected"
	}
}

// gateRefusalDetail records the observed 006 basis without secrets.
func gateRefusalDetail(gate RecoveryGate, class RefusalClass, requestVersion uint64) string {
	if bases := gate.PauseBases(); len(bases) > 0 {
		return "pause_basis=" + strings.Join(bases, ",") + " recovery_version=" + strconv.FormatUint(requestVersion, 10)
	}
	if gate.HasRecovery {
		return "recovery_phase=" + gate.RecoveryPhase + " recovery_seq=" + strconv.FormatInt(gate.RecoverySeq, 10)
	}
	if class == ClassRecoveryVersionChanged {
		return "current_version=" + strconv.FormatInt(gate.CurrentVersion(), 10) + " request_version=" + strconv.FormatUint(requestVersion, 10)
	}
	return string(class)
}

// grantState renders the observed grant state for the audit detail.
func grantState(grant *AuthzGrant, found bool) string {
	if !found || grant == nil {
		return "absent"
	}
	return grant.State
}

// scopeCarrierState renders the observed PB carrier presence for the audit
// detail.
func scopeCarrierState(scope GrantScope) string {
	if scope.Present {
		return "present"
	}
	return "absent"
}

// reuseRefusalDetail records the observed OC-5 reuse basis (the anchor row and
// the carrier's purpose token) without secrets.
func reuseRefusalDetail(anchorID int64, scope GrantScope) string {
	return "anchor=" + strconv.FormatInt(anchorID, 10) + " scope_carrier=" + scopeCarrierState(scope) +
		" allows_fee_replacement=" + strconv.FormatBool(scope.AllowsFeeReplacement) + " fresh_authorization_required"
}

// anchorBindingRefusalDetail records which anchor-bound field the replacement
// tried to change (V13-4b: fees are the only free dimension), without secrets.
func anchorBindingRefusalDetail(anchor ReplacementAnchor, req *Request) string {
	return "anchor=" + strconv.FormatInt(anchor.RowID, 10) + " anchor_request_id=" + anchor.SigningRequestID +
		" anchor_identity_mismatch=" + anchorRebindingField(anchor, req)
}

// bindingClassName maps a BindingResult to the recorded binding_class.
func bindingClassName(res BindingResult) string {
	switch res {
	case BindingMatches:
		return "matches"
	case BindingAbsent:
		return "absent"
	case BindingConflict:
		return "conflict"
	case BindingPaused:
		return "paused"
	case BindingTerminal:
		return "terminal"
	default:
		return "read_failed"
	}
}

// asRefusal extracts a *RefusalError, failing closed to storage_unavailable.
func asRefusal(err error) *RefusalError {
	var re *RefusalError
	if errors.As(err, &re) {
		return re
	}
	return refuse(ClassStorageUnavailable, "", "internal failure")
}

// lowerAddr canonicalizes a validated hex address to lowercase.
func lowerAddr(s string) string {
	return strings.ToLower(common.HexToAddress(s).Hex())
}

// nullIfEmpty passes SQL NULL for an absent fee-shape column.
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
