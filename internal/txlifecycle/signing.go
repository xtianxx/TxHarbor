package txlifecycle

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pgx/v5/pgconn"
)

// 009 submit boundary constants. The endpoint is fixed by signer-call.md §1;
// the credential is a secret and never reaches a log, error or metric.
const (
	signerSubmitPath = "/signer/v1/signing-requests"

	// Bounded same-identity retry for 009 delivery-unknown (503): no new
	// attempt/identity, no chain effect, never marked failed (signer-call §3).
	signerRetryAttempts = 3
	signerRetryInitial  = 200 * time.Millisecond
	signerRetryMax      = 2 * time.Second
)

// SignerConfig is the 010 caller configuration (T001 knobs).
type SignerConfig struct {
	BaseURL    string
	Credential string
	Timeout    time.Duration
}

// SigningResult are the 009 facts consumed by 010 (api.md §2): signature +
// tx_hash, never raw signed bytes.
type SigningResult struct {
	Signature string
	TxHash    string
}

// SignerError is a 009-boundary failure. DeliveryUnknown marks the 503
// indeterminate case: the attempt stays prepared and the same identity +
// envelope are retried (never re-signed under a new identity).
type SignerError struct {
	Class           RefusalClass
	Basis           string
	DeliveryUnknown bool
}

func (e *SignerError) Error() string { return fmt.Sprintf("%s: %s", e.Class, e.Basis) }

// SignerClient submits byte-identical canonical envelopes to 009.
type SignerClient struct {
	cfg  SignerConfig
	http *http.Client
}

// NewSignerClient validates the caller configuration fail-closed: a missing
// URL or credential refuses construction rather than sending unauthenticated.
func NewSignerClient(cfg SignerConfig) (*SignerClient, error) {
	if cfg.BaseURL == "" {
		return nil, Refuse(ClassCoordinationUnavailable, "", "TXHARBOR_TX_SIGNER_URL is not configured")
	}
	if cfg.Credential == "" {
		return nil, Refuse(ClassCoordinationUnavailable, "", "TXHARBOR_TX_SIGNER_CREDENTIAL is not configured")
	}
	return &SignerClient{cfg: cfg, http: &http.Client{}}, nil
}

// Submit sends the persisted canonical envelope for an attempt. The bytes are
// never re-serialized; same-identity retry is byte-identical.
func (c *SignerClient) Submit(ctx context.Context, attempt *Attempt) (SigningResult, error) {
	if attempt == nil {
		return SigningResult{}, Refuse(ClassAttemptNotFound, "attempt_id", "no attempt")
	}
	return c.submit(ctx, []byte(attempt.CanonicalEnvelope))
}

func (c *SignerClient) submit(ctx context.Context, envelope []byte) (SigningResult, error) {
	backoff := signerRetryInitial
	var last *SignerError
	for attempt := 0; attempt < signerRetryAttempts; attempt++ {
		result, err := c.submitOnce(ctx, envelope)
		if err == nil {
			return result, nil
		}
		var se *SignerError
		if !errors.As(err, &se) || !se.DeliveryUnknown {
			return SigningResult{}, err
		}
		last = se
		if attempt == signerRetryAttempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return SigningResult{}, &SignerError{Class: ClassCoordinationUnavailable, Basis: "context canceled during 009 delivery-unknown retry", DeliveryUnknown: true}
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > signerRetryMax {
			backoff = signerRetryMax
		}
	}
	return SigningResult{}, last
}

// submitOnce performs one HTTP submit and maps the api.md §2 response exactly
// onto the §3 handling table (signer-call.md §3).
func (c *SignerClient) submitOnce(ctx context.Context, envelope []byte) (SigningResult, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
		strings.TrimRight(c.cfg.BaseURL, "/")+signerSubmitPath, bytes.NewReader(envelope))
	if err != nil {
		return SigningResult{}, &SignerError{Class: ClassCoordinationUnavailable, Basis: "request build failed"}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.cfg.Credential)

	resp, err := c.http.Do(req)
	if err != nil {
		return SigningResult{}, &SignerError{Class: ClassCoordinationUnavailable, Basis: "009 transport failure", DeliveryUnknown: true}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return SigningResult{}, &SignerError{Class: ClassCoordinationUnavailable, Basis: "009 response read failed", DeliveryUnknown: resp.StatusCode == http.StatusServiceUnavailable}
	}
	return mapSignerResponse(resp.StatusCode, body)
}

// signerSuccessBody is the 009 200 body (api.md §2): signature + tx_hash.
type signerSuccessBody struct {
	State     string `json:"state"`
	Signature string `json:"signature"`
	TxHash    string `json:"tx_hash"`
}

// signerErrorBody is the 009 error body (api.md §2): machine code + observed
// basis, no secrets.
type signerErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field"`
}

func mapSignerResponse(status int, body []byte) (SigningResult, error) {
	if status == http.StatusOK {
		var ok signerSuccessBody
		if err := json.Unmarshal(body, &ok); err != nil || ok.Signature == "" || ok.TxHash == "" {
			return SigningResult{}, &SignerError{Class: ClassSignatureRefused, Basis: "009 200 body is not a valid signed result"}
		}
		return SigningResult{Signature: ok.Signature, TxHash: ok.TxHash}, nil
	}
	var e signerErrorBody
	_ = json.Unmarshal(body, &e)
	code := e.Code
	if code == "" {
		code = fmt.Sprintf("http_%d", status)
	}
	switch {
	case code == "request_conflict":
		// Same identity, different envelope: never overwrite (signer-call §3).
		return SigningResult{}, Refuse(ClassAttemptConflict, "", "009 request_conflict for this signing identity")
	case status == http.StatusServiceUnavailable:
		// key_provider_*/storage_*/outcome_unknown/gate_read_failed:
		// delivery-unknown; deterministic same-identity retry.
		return SigningResult{}, &SignerError{Class: ClassCoordinationUnavailable, Basis: "009 " + code, DeliveryUnknown: true}
	case status == http.StatusUnauthorized,
		status == http.StatusBadRequest:
		// Credential/permission or schema drift: fail closed, operator fix.
		return SigningResult{}, &SignerError{Class: ClassSignatureRefused, Basis: "009 " + code}
	default:
		// 409 signature_withheld/recovery_*/binding_*/authorization_*,
		// 422 policy_refused, 403 grant classes, and any unrecognized code:
		// refused, zero send, attempt stays prepared.
		return SigningResult{}, &SignerError{Class: ClassSignatureRefused, Basis: "009 " + code}
	}
}

// recordSignatureMismatch appends the fail-closed evidence for a tampered or
// mismatched 009 result, so zero-dispatch refusals are auditable. Best-effort:
// the refusal itself is already fail-closed.
func (s *Store) recordSignatureMismatch(ctx context.Context, attempt *Attempt, basis string) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return
	}
	if _, _, err := lockAttemptRow(ctx, tx, attempt.AttemptID); err != nil {
		return
	}
	if err := appendEventTx(ctx, tx, attempt.AttemptID, EventSignatureMismatch, "", recoveryVersionPtr(attempt.RecoveryVersion), basis); err != nil {
		return
	}
	_ = tx.Commit(ctx)
}

// SignedRecord is the durable T2 result.
type SignedRecord struct {
	TxHash      string
	SignedBytes []byte
	RevisionSeq int64
}

// SignAndPersist verifies 009's result locally and commits T2: signature +
// signed bytes + tx_hash + guarded prepared→signed + signature_persisted
// event, all before any dispatch (FR-02; signer-call §4).
func (s *Store) SignAndPersist(ctx context.Context, attempt *Attempt, result SigningResult) (SignedRecord, error) {
	if s == nil || s.db == nil {
		return SignedRecord{}, Refuse(ClassCoordinationUnavailable, "", "store has no database")
	}
	req, err := prepareRequestFromAttempt(attempt)
	if err != nil {
		return SignedRecord{}, err
	}
	signedBytes, localHash, err := reconstructSignedBytes(req, result.Signature)
	if err != nil {
		if ref, ok := err.(*RefusalError); ok && ref.Class == ClassSignatureMismatch {
			s.recordSignatureMismatch(ctx, attempt, ref.Basis)
		}
		return SignedRecord{}, err
	}
	if localHash.Hex() != strings.ToLower(result.TxHash) {
		s.recordSignatureMismatch(ctx, attempt, "locally reconstructed hash differs from the 009 tx_hash")
		return SignedRecord{}, Refuse(ClassSignatureMismatch, "tx_hash",
			"locally reconstructed hash differs from the 009 tx_hash")
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return SignedRecord{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return SignedRecord{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	revisionSeq, _, err := lockAttemptRow(ctx, tx, attempt.AttemptID)
	if err != nil {
		return SignedRecord{}, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO tx_attempt_signings (attempt_id, signature, signed_tx_bytes, tx_hash) VALUES ($1,$2,$3,$4)`,
		attempt.AttemptID, strings.ToLower(result.Signature), signedBytes, strings.ToLower(result.TxHash)); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "tx_attempt_signings_tx_hash_uniq" {
			return SignedRecord{}, Refuse(ClassHashConflict, "tx_hash", "these signed bytes already exist under another attempt")
		}
		return SignedRecord{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	if _, err := applyStateTx(ctx, tx, attempt.AttemptID, revisionSeq, []string{"prepared"}, "signed", ""); err != nil {
		if errors.Is(err, ErrRevisionMoved) {
			return SignedRecord{}, Refuse(ClassSendStale, "", "attempt revision moved; retry from current state")
		}
		return SignedRecord{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	if err := appendEventTx(ctx, tx, attempt.AttemptID, EventSignaturePersisted, "", recoveryVersionPtr(attempt.RecoveryVersion), "tx_hash="+strings.ToLower(result.TxHash)); err != nil {
		return SignedRecord{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	if err := tx.Commit(ctx); err != nil {
		return SignedRecord{}, Refuse(ClassCoordinationUnavailable, "", "storage unavailable")
	}
	return SignedRecord{TxHash: strings.ToLower(result.TxHash), SignedBytes: signedBytes, RevisionSeq: revisionSeq + 1}, nil
}

// reconstructSignedBytes rebuilds the transaction from the persisted content,
// attaches the 65-byte signature with the chain-bound signer, marshals, and
// returns the bytes plus keccak256(bytes). The recovered sender is checked
// against the attempt sender (signer-call §4 step 4).
func reconstructSignedBytes(req *PrepareRequest, signatureHex string) ([]byte, common.Hash, error) {
	raw, err := hex.DecodeString(strings.TrimPrefix(strings.ToLower(signatureHex), "0x"))
	if err != nil || len(raw) != 65 {
		return nil, common.Hash{}, Refuse(ClassSignatureMismatch, "signature", "signature must be 65 bytes [R||S||V]")
	}
	unsigned, err := req.Transaction()
	if err != nil {
		return nil, common.Hash{}, err
	}
	signer := types.LatestSignerForChainID(new(big.Int).SetUint64(req.ChainID))
	signed, err := unsigned.WithSignature(signer, raw)
	if err != nil {
		return nil, common.Hash{}, Refuse(ClassSignatureMismatch, "signature", "signature does not fit this transaction")
	}
	from, err := types.Sender(signer, signed)
	if err != nil || from != common.HexToAddress(req.Sender) {
		return nil, common.Hash{}, Refuse(ClassSignatureMismatch, "sender", "recovered sender does not match the attempt sender")
	}
	signedBytes, err := signed.MarshalBinary()
	if err != nil {
		return nil, common.Hash{}, Refuse(ClassCoordinationUnavailable, "", "signed transaction marshal failed")
	}
	return signedBytes, crypto.Keccak256Hash(signedBytes), nil
}

// prepareRequestFromAttempt projects the persisted attempt content back into
// the request shape used for local reconstruction.
func prepareRequestFromAttempt(a *Attempt) (*PrepareRequest, error) {
	if a == nil {
		return nil, Refuse(ClassAttemptNotFound, "attempt_id", "no attempt")
	}
	return &PrepareRequest{
		AttemptID:            a.AttemptID,
		SigningRequestID:     a.SigningRequestID,
		ReplacementOf:        a.ReplacementOf,
		IntentID:             a.IntentID,
		BindingRef:           a.BindingRef,
		AuthorizationID:      a.AuthorizationID,
		AuthorizationVersion: a.AuthorizationVersion,
		RecoveryVersion:      uint64(a.RecoveryVersion),
		ChainID:              uint64(a.ChainID),
		Sender:               a.Sender,
		Nonce:                a.Nonce,
		TxType:               uint8(a.TxType),
		GasLimit:             a.GasLimit,
		GasPrice:             a.GasPrice,
		MaxFeePerGas:         a.MaxFeePerGas,
		MaxPriorityFeePerGas: a.MaxPriorityFeePerGas,
		Asset:                a.Asset,
		Recipient:            a.Recipient,
		Amount:               a.Amount,
	}, nil
}
