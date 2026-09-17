package txlifecycle

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DB is the minimal pgx surface the store needs; *pgxpool.Pool satisfies it.
type DB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// writeGuard bounds every statement of a 010 transaction (persistence.md §1).
const writeGuard = "SET LOCAL statement_timeout = '5s'"

// lockGuard additionally bounds every lock wait inside the send region T3 so a
// contended gate fails closed instead of hanging (send-gate.md §2).
const lockGuard = "SET LOCAL lock_timeout = '5s'"

// ErrRevisionMoved means the version guard saw RowsAffected <> 1: the attempt
// moved since it was read. Callers roll back with zero dispatch (persistence.md §1).
var ErrRevisionMoved = errors.New("attempt revision moved")

// lockAttemptRow locks the attempt row FOR UPDATE and returns its current
// revision and state. event_seq allocation and every guarded state change in
// the same transaction run under this lock (T015; data-model Table 6).
func lockAttemptRow(ctx context.Context, tx pgx.Tx, attemptID string) (int64, string, error) {
	var revision int64
	var state string
	err := tx.QueryRow(ctx,
		`SELECT revision_seq, state FROM tx_attempts WHERE attempt_id = $1 FOR UPDATE`, attemptID).
		Scan(&revision, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", Refuse(ClassAttemptNotFound, "attempt_id", "no such attempt")
	}
	if err != nil {
		return 0, "", err
	}
	return revision, state, nil
}

// appendEventTx appends one tx_attempt_events row with event_seq allocated
// under the attempt row lock held by the caller. The log is append-only.
func appendEventTx(ctx context.Context, tx pgx.Tx, attemptID, event, reasonClass string, recoveryVersion *int64, detail string) error {
	if !ValidEvent(event) {
		return Refuse(ClassCoordinationUnavailable, "event", "unknown event "+event)
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO tx_attempt_events (attempt_id, event_seq, event, reason_class, recovery_version, detail)
		 VALUES ($1, (SELECT COALESCE(MAX(event_seq), 0) + 1 FROM tx_attempt_events WHERE attempt_id = $1), $2, $3, $4, $5)`,
		attemptID, event, reasonClass, recoveryVersion, detail)
	return err
}

// applyStateTx performs the version-guarded state mutation shared by every
// path: WHERE attempt_id = $1 AND revision_seq = $2 (plus optional from-state
// set) and revision_seq + 1. RowsAffected <> 1 is ErrRevisionMoved. extraSet
// is a constant fragment supplied by callers (e.g. "effective_at = now()").
func applyStateTx(ctx context.Context, tx pgx.Tx, attemptID string, expectedRevision int64, fromStates []string, toState, extraSet string) (int64, error) {
	query := `UPDATE tx_attempts SET state = $3, revision_seq = revision_seq + 1, updated_at = now()`
	args := []any{attemptID, expectedRevision, toState}
	if extraSet != "" {
		query += ", " + extraSet
	}
	query += ` WHERE attempt_id = $1 AND revision_seq = $2`
	if len(fromStates) > 0 {
		query += ` AND state = ANY($4)`
		args = append(args, fromStates)
	}
	tag, err := tx.Exec(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	if tag.RowsAffected() != 1 {
		return 0, ErrRevisionMoved
	}
	return expectedRevision + 1, nil
}

// recoveryVersionPtr adapts an optional version for appendEventTx.
func recoveryVersionPtr(v int64) *int64 { return &v }

// Store owns 010's durable state (T1/T2/T4) and the gated send region (T3).
type Store struct {
	db           DB
	rpc          ChainRPC
	signer       *SignerClient
	sendTimeout  time.Duration
	releaseToken string
}

// NewStore returns a store over db.
func NewStore(db DB) *Store { return &Store{db: db} }

// AttemptRef is the identity result of a T1 persist.
type AttemptRef struct {
	AttemptID        string
	SigningRequestID string
	ContentHash      string
	RevisionSeq      int64
	Converged        bool
}

// Attempt is one full tx_attempts row (identity/content plus current state).
type Attempt struct {
	AttemptID            string
	SigningRequestID     string
	ReplacementOf        string
	IntentID             string
	BindingRef           string
	AuthorizationID      string
	AuthorizationVersion int64
	RecoveryVersion      int64
	ChainID              int64
	Sender               string
	Nonce                string
	TxType               int
	To                   string
	Value                string
	Data                 []byte
	GasLimit             string
	GasPrice             string
	MaxFeePerGas         string
	MaxPriorityFeePerGas string
	Asset                string
	Recipient            string
	Amount               string
	CanonicalEnvelope    string
	ContentHash          string
	State                string
	RevisionSeq          int64
	UpdatedAt            time.Time
	TxHash               string
}

const attemptSelectColumns = `attempt_id, signing_request_id, COALESCE(replacement_of, ''), intent_id, binding_ref,
	authorization_id, authorization_version, recovery_version, chain_id, sender, nonce, tx_type,
	to_addr, value, data, gas_limit, COALESCE(gas_price, ''), COALESCE(max_fee_per_gas, ''),
	COALESCE(max_priority_fee_per_gas, ''), asset, recipient, amount, canonical_envelope, content_hash,
	state, revision_seq, updated_at`

const prepareInsertSQL = `INSERT INTO tx_attempts (
	attempt_id, signing_request_id, replacement_of, intent_id, binding_ref,
	authorization_id, authorization_version, recovery_version, chain_id, sender,
	nonce, tx_type, to_addr, value, data, gas_limit, gas_price,
	max_fee_per_gas, max_priority_fee_per_gas, asset, recipient, amount,
	canonical_envelope, content_hash
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)`

// PrepareAttempt is T1: persist attempt identity + immutable content before
// any 009 call (FR-01). Idempotent by attempt_id — same identity + identical
// canonical envelope converges on the existing row; same identity + different
// envelope refuses attempt_conflict with zero writes and the original row
// untouched; the signing-request identity is 1:1 with the attempt.
func (s *Store) PrepareAttempt(ctx context.Context, req *PrepareRequest) (AttemptRef, error) {
	if s == nil || s.db == nil {
		return AttemptRef{}, Refuse(ClassCoordinationUnavailable, "", "store has no database")
	}
	if err := req.Validate(); err != nil {
		return AttemptRef{}, err
	}
	envelope, err := req.CanonicalEnvelope()
	if err != nil {
		return AttemptRef{}, err
	}
	contentHash, err := req.ContentHash()
	if err != nil {
		return AttemptRef{}, err
	}
	if _, err := decimalUint64("nonce", req.Nonce); err != nil {
		return AttemptRef{}, err
	}
	asset, _ := lowerAddress("asset", req.Asset)
	recipient, _ := lowerAddress("recipient", req.Recipient)
	amount, _ := new(big.Int).SetString(req.Amount, 10)
	data := TransferCalldata(common.HexToAddress(recipient), amount)

	if req.ReplacementOf != "" {
		if err := s.validateReplacementAnchor(ctx, req); err != nil {
			return AttemptRef{}, err
		}
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return AttemptRef{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return AttemptRef{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	_, err = tx.Exec(ctx, prepareInsertSQL,
		req.AttemptID, req.SigningRequestID, nullIfEmpty(req.ReplacementOf), req.IntentID, req.BindingRef,
		req.AuthorizationID, req.AuthorizationVersion, int64(req.RecoveryVersion), int64(req.ChainID), strings.ToLower(req.Sender),
		req.Nonce, int(req.TxType), asset, "0", data, req.GasLimit, nullIfEmpty(req.GasPrice),
		nullIfEmpty(req.MaxFeePerGas), nullIfEmpty(req.MaxPriorityFeePerGas), asset, recipient, req.Amount,
		string(envelope), contentHash,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			_ = tx.Rollback(ctx)
			return s.converge(ctx, req, string(envelope), pgErr.ConstraintName)
		}
		return AttemptRef{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	if err := tx.Commit(ctx); err != nil {
		return AttemptRef{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	return AttemptRef{
		AttemptID: req.AttemptID, SigningRequestID: req.SigningRequestID,
		ContentHash: contentHash, RevisionSeq: 1,
	}, nil
}

// converge classifies a 23505 by exact ConstraintName only (repo convention).
func (s *Store) converge(ctx context.Context, req *PrepareRequest, envelope, constraint string) (AttemptRef, error) {
	switch constraint {
	case "tx_attempts_pkey":
		ref, storedEnvelope, err := s.fetchRef(ctx, "attempt_id", req.AttemptID)
		if err != nil {
			return AttemptRef{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
		}
		if storedEnvelope != envelope {
			return AttemptRef{}, Refuse(ClassAttemptConflict, "attempt_id",
				"attempt_id is bound to a different canonical envelope")
		}
		ref.Converged = true
		return ref, nil
	case "tx_attempts_signing_request_uniq":
		return AttemptRef{}, Refuse(ClassAttemptConflict, "signing_request_id",
			"signing_request_id already belongs to another attempt")
	default:
		return AttemptRef{}, Refuse(ClassCoordinationUnavailable, "", "unexpected duplicate identity")
	}
}

func (s *Store) fetchRef(ctx context.Context, column, value string) (AttemptRef, string, error) {
	var ref AttemptRef
	var envelope string
	err := s.db.QueryRow(ctx,
		`SELECT attempt_id, signing_request_id, content_hash, revision_seq, canonical_envelope
		   FROM tx_attempts WHERE `+column+` = $1`, value).
		Scan(&ref.AttemptID, &ref.SigningRequestID, &ref.ContentHash, &ref.RevisionSeq, &envelope)
	if err != nil {
		return AttemptRef{}, "", err
	}
	return ref, envelope, nil
}

// AttemptByID loads one full attempt row. A missing row is attempt_not_found.
func (s *Store) AttemptByID(ctx context.Context, attemptID string) (*Attempt, error) {
	if s == nil || s.db == nil {
		return nil, Refuse(ClassCoordinationUnavailable, "", "store has no database")
	}
	a := &Attempt{}
	err := s.db.QueryRow(ctx, `SELECT `+attemptSelectColumns+`, COALESCE(s.tx_hash, '')
		  FROM tx_attempts a LEFT JOIN tx_attempt_signings s ON s.attempt_id = a.attempt_id
		 WHERE a.attempt_id = $1`, attemptID).
		Scan(&a.AttemptID, &a.SigningRequestID, &a.ReplacementOf, &a.IntentID, &a.BindingRef,
			&a.AuthorizationID, &a.AuthorizationVersion, &a.RecoveryVersion, &a.ChainID, &a.Sender,
			&a.Nonce, &a.TxType, &a.To, &a.Value, &a.Data, &a.GasLimit, &a.GasPrice,
			&a.MaxFeePerGas, &a.MaxPriorityFeePerGas, &a.Asset, &a.Recipient, &a.Amount,
			&a.CanonicalEnvelope, &a.ContentHash, &a.State, &a.RevisionSeq, &a.UpdatedAt, &a.TxHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, Refuse(ClassAttemptNotFound, "attempt_id", "no such attempt")
	}
	if err != nil {
		return nil, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	return a, nil
}

// validateReplacementAnchor enforces FR-05/FR-12: ReplacementOf must name an
// existing attempt with equal intent/binding/chain/sender/nonce/asset/
// recipient/amount (else replacement_mismatch) and fee dimensions that differ
// (else replacement_no_fee_change). The anchor row itself is never touched.
func (s *Store) validateReplacementAnchor(ctx context.Context, req *PrepareRequest) error {
	anchor, err := s.AttemptByID(ctx, req.ReplacementOf)
	if err != nil {
		if ref, ok := err.(*RefusalError); ok && ref.Class == ClassAttemptNotFound {
			return Refuse(ClassReplacementMismatch, "replacement_of", "anchor attempt does not exist")
		}
		return err
	}
	inNonce, _ := decimalUint64("nonce", req.Nonce)
	anchorNonce, _ := decimalUint64("nonce", anchor.Nonce)
	reqAmount := new(big.Int)
	reqAmount.SetString(req.Amount, 10)
	anchorAmount := new(big.Int)
	anchorAmount.SetString(anchor.Amount, 10)
	reqSender, _ := lowerAddress("sender", req.Sender)
	reqAsset, _ := lowerAddress("asset", req.Asset)
	reqRecipient, _ := lowerAddress("recipient", req.Recipient)
	if req.IntentID != anchor.IntentID || req.BindingRef != anchor.BindingRef ||
		int64(req.ChainID) != anchor.ChainID || reqSender != anchor.Sender ||
		inNonce != anchorNonce || reqAsset != anchor.Asset || reqRecipient != anchor.Recipient ||
		reqAmount.Cmp(anchorAmount) != 0 {
		return Refuse(ClassReplacementMismatch, "replacement_of",
			"anchor names a different intent/binding/sender/nonce/asset/recipient/amount")
	}
	if !feeDimensionsDiffer(req, anchor) {
		return Refuse(ClassReplacementNoFeeChange, "replacement_of",
			"replacement does not change any fee dimension")
	}
	return nil
}

// feeDimensionsDiffer reports whether the request changes at least one fee
// dimension (tx type, gas limit, or any fee value) relative to the anchor.
func feeDimensionsDiffer(req *PrepareRequest, anchor *Attempt) bool {
	gasLimit := canonicalDecimal(req.GasLimit)
	if int(req.TxType) != anchor.TxType || gasLimit != canonicalDecimal(anchor.GasLimit) {
		return true
	}
	switch int(req.TxType) {
	case int(TxTypeLegacy):
		return canonicalDecimal(req.GasPrice) != canonicalDecimal(anchor.GasPrice)
	default:
		return canonicalDecimal(req.MaxFeePerGas) != canonicalDecimal(anchor.MaxFeePerGas) ||
			canonicalDecimal(req.MaxPriorityFeePerGas) != canonicalDecimal(anchor.MaxPriorityFeePerGas)
	}
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
