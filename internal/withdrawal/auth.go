// Package withdrawal implements receive-only withdrawal intake: API-key
// authenticated, grant-authorized, idempotent request receipt plus
// owner-restricted query. It never allocates nonces, signs, or broadcasts
// (008+ boundary); persistence truth lives in PostgreSQL (Tables 1-6,
// migrations/000007_withdrawal_creation.sql).
//
// auth.go owns API-key credentials (T007, research R1-R5): server-issued
// high-entropy secrets, SHA-256 hex digests at rest, a single-statement
// revocation predicate, and caller-identity derivation. It carries no HTTP
// coupling; the operator subcommand (T034) and the receipt path (T010) call
// these functions.
package withdrawal

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// keyPrefix is the literal typed prefix every generated plaintext carries
	// (research R1/R5). It is part of the token, never of the stored lookup.
	keyPrefix = "txh_"
	// keyEntropyBytes is the CSPRNG secret length behind every issued key
	// (R1/R5): 32 bytes of crypto/rand, base64url without padding.
	keyEntropyBytes = 32
	// keyPrefixLength is how many leading plaintext characters are stored for
	// operator display and audit (R2). The stored key_prefix is never an
	// authentication input.
	keyPrefixLength = 8
)

// Caller is the stable, non-secret business identity a key resolves to
// (Table 1). CanCreate is the fixed interface permission (FR-03b): it is
// exposed for the caller (T010) to enforce — Authenticate never decides
// CodeUnauthorized itself.
type Caller struct {
	ID        int64
	Label     string
	CanCreate bool
}

// KeyRef identifies one api_key credential (Table 2) without carrying secret
// material: KeyID is the credential identity, CallerID the business identity
// it resolves to, and Prefix the display-only key fragment (R2/R3).
type KeyRef struct {
	KeyID    int64
	CallerID int64
	Prefix   string
}

// AuthResult is a successful authentication: the caller identity plus the
// credential reference it was derived from.
type AuthResult struct {
	Caller Caller
	Key    KeyRef
}

// GenerateKey mints one credential: keyEntropyBytes CSPRNG bytes, base64url
// without padding, behind the literal keyPrefix (R1/R5). It returns the
// plaintext exactly once (the caller MUST display it once and persist
// nothing), the lowercase-hex SHA-256 digest to store, and the display-only
// prefix. A CSPRNG failure is the only error.
func GenerateKey() (plaintext string, hash string, prefix string, err error) {
	entropy := make([]byte, keyEntropyBytes)
	if _, err := rand.Read(entropy); err != nil {
		return "", "", "", fmt.Errorf("generate api key: %w", err)
	}
	plaintext = keyPrefix + base64.RawURLEncoding.EncodeToString(entropy)
	return plaintext, HashKey(plaintext), plaintext[:keyPrefixLength], nil
}

// HashKey returns the lowercase hex SHA-256 digest of plaintext: the exact
// value stored in api_key.key_hash and the sole lookup key (R1). It is
// deterministic, so the same plaintext always yields the same digest and the
// digest is safe to compare and index; no salt or KDF is applied by design
// (keys are high-entropy, so a slow KDF buys nothing and cannot be indexed).
func HashKey(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}

// parsePresented reports whether presented has the shape of a generated
// credential: the literal keyPrefix followed by base64url that decodes to
// exactly keyEntropyBytes. A token that fails this can never address a stored
// digest, so it is rejected as unauthenticated without a database round trip.
func parsePresented(presented string) bool {
	if !strings.HasPrefix(presented, keyPrefix) {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(presented[len(keyPrefix):])
	return err == nil && len(decoded) == keyEntropyBytes
}

// Authenticate verifies one presented API key and resolves it to its caller.
//
// Verification is a single indexed read (R4):
//
//	SELECT key_id, caller_id, key_hash FROM api_key
//	WHERE key_hash = $1 AND (revoked_at IS NULL OR revoked_at > now())
//
// followed by a constant-time comparison of the stored digest against the
// recomputed one (defense in depth even though the unique index already
// matched) and a read of the caller row. There is no cache: every call
// re-reads, so a revocation committed before the call is observed by that
// call and by every later attempt (R4 startpoint semantics).
//
// A missing, malformed, or revoked key returns a generic
// *Error{Code: CodeUnauthenticated} naming no key material; a storage failure
// returns *Error{Code: CodeTemporarilyUnavailable}. Authenticate never decides
// CodeUnauthorized — Caller.CanCreate is exposed for the caller (T010) to
// enforce.
//
// pool may be nil when presented fails the pre-flight shape check (empty or
// malformed), because that path returns before the pool is used; a well-formed
// key with a nil pool is an internal miswiring and returns
// *Error{Code: CodeTemporarilyUnavailable} instead of panicking.
func Authenticate(ctx context.Context, pool *pgxpool.Pool, presented string) (*AuthResult, error) {
	if !parsePresented(presented) {
		return nil, New(CodeUnauthenticated, "missing or invalid API key")
	}
	if pool == nil {
		return nil, storageUnavailable("authenticate api key", errors.New("nil pool"))
	}
	hash := HashKey(presented)

	var keyID, callerID int64
	var storedHash string
	err := pool.QueryRow(ctx, `
SELECT key_id, caller_id, key_hash FROM api_key
WHERE key_hash = $1 AND (revoked_at IS NULL OR revoked_at > now())`, hash).
		Scan(&keyID, &callerID, &storedHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, New(CodeUnauthenticated, "missing or invalid API key")
	}
	if err != nil {
		return nil, storageUnavailable("authenticate api key", err)
	}
	if subtle.ConstantTimeCompare([]byte(storedHash), []byte(hash)) != 1 {
		return nil, New(CodeUnauthenticated, "missing or invalid API key")
	}

	var caller Caller
	err = pool.QueryRow(ctx,
		`SELECT caller_id, label, can_create FROM caller WHERE caller_id = $1`, callerID).
		Scan(&caller.ID, &caller.Label, &caller.CanCreate)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, New(CodeUnauthenticated, "missing or invalid API key")
	}
	if err != nil {
		return nil, storageUnavailable("load api key caller", err)
	}

	return &AuthResult{
		Caller: caller,
		Key:    KeyRef{KeyID: keyID, CallerID: callerID, Prefix: presented[:keyPrefixLength]},
	}, nil
}

// IssueKey creates one API key for callerID, creating the stable caller row on
// first use and reusing it (label set only on insert) thereafter (R3/R5). It
// returns the plaintext exactly once plus the stored reference; only the
// SHA-256 digest is persisted. The whole issue is one transaction, so a
// failed key insert never leaves a caller row behind.
func IssueKey(ctx context.Context, pool *pgxpool.Pool, callerID int64, label string) (plaintext string, ref KeyRef, err error) {
	if callerID <= 0 {
		return "", KeyRef{}, New(CodeValidationFailed, "caller_id must be positive").WithField("caller_id")
	}
	plaintext, hash, prefix, err := GenerateKey()
	if err != nil {
		return "", KeyRef{}, storageUnavailable("issue api key", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", KeyRef{}, storageUnavailable("issue api key", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
INSERT INTO caller (caller_id, label) VALUES ($1, $2)
ON CONFLICT ON CONSTRAINT caller_pkey DO NOTHING`, callerID, label); err != nil {
		return "", KeyRef{}, storageUnavailable("insert api key caller", err)
	}
	var reusedID int64
	if err := tx.QueryRow(ctx,
		`SELECT caller_id FROM caller WHERE caller_id = $1`, callerID).Scan(&reusedID); err != nil {
		return "", KeyRef{}, storageUnavailable("read api key caller", err)
	}

	var keyID int64
	err = tx.QueryRow(ctx, `
INSERT INTO api_key (caller_id, key_hash, key_prefix)
VALUES ($1, $2, $3) RETURNING key_id`, callerID, hash, prefix).Scan(&keyID)
	if err != nil {
		return "", KeyRef{}, storageUnavailable("insert api key", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", KeyRef{}, storageUnavailable("commit api key issue", err)
	}
	return plaintext, KeyRef{KeyID: keyID, CallerID: callerID, Prefix: prefix}, nil
}

// RotateKey mints a successor credential for callerID, then stamps the
// caller's currently active predecessor credentials revoked_at = now() plus
// graceSeconds (clamped at zero, so graceSeconds <= 0 revokes immediately). A
// positive grace leaves a predecessor accepted until the stamp passes, giving
// clients a dual-accept window (R4). caller_id is never mutated (R3): identity
// and idempotency scope survive rotation. The successor's plaintext is
// returned exactly once and only its digest is persisted. A missing caller
// fails the foreign key, which maps to CodeTemporarilyUnavailable.
func RotateKey(ctx context.Context, pool *pgxpool.Pool, callerID int64, graceSeconds int64) (plaintext string, ref KeyRef, err error) {
	if callerID <= 0 {
		return "", KeyRef{}, New(CodeValidationFailed, "caller_id must be positive").WithField("caller_id")
	}
	plaintext, hash, prefix, err := GenerateKey()
	if err != nil {
		return "", KeyRef{}, storageUnavailable("rotate api key", err)
	}
	grace := graceSeconds
	if grace < 0 {
		grace = 0
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", KeyRef{}, storageUnavailable("rotate api key", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var keyID int64
	err = tx.QueryRow(ctx, `
INSERT INTO api_key (caller_id, key_hash, key_prefix)
VALUES ($1, $2, $3) RETURNING key_id`, callerID, hash, prefix).Scan(&keyID)
	if err != nil {
		return "", KeyRef{}, storageUnavailable("insert rotated api key", err)
	}
	if _, err := tx.Exec(ctx, `
UPDATE api_key SET revoked_at = now() + make_interval(secs => $2::double precision)
WHERE caller_id = $1 AND key_id <> $3 AND revoked_at IS NULL`, callerID, grace, keyID); err != nil {
		return "", KeyRef{}, storageUnavailable("revoke rotated predecessor", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", KeyRef{}, storageUnavailable("commit api key rotation", err)
	}
	return plaintext, KeyRef{KeyID: keyID, CallerID: callerID, Prefix: prefix}, nil
}

// RevokeKey immediately revokes one still-effective credential
// (revoked_at = now()). A row with revoked_at IS NULL or in the future
// (rotation grace window) is still accepted by Authenticate, so an explicit
// revoke terminates it — the revoke always wins over a grace stamp. Only an
// already-terminated credential (revoked_at <= now()) or a missing key_id
// affects zero rows and returns *Error{Code: CodeNotFound}. A storage failure
// returns *Error{Code: CodeTemporarilyUnavailable}.
func RevokeKey(ctx context.Context, pool *pgxpool.Pool, keyID int64) error {
	tag, err := pool.Exec(ctx,
		`UPDATE api_key SET revoked_at = now() WHERE key_id = $1 AND (revoked_at IS NULL OR revoked_at > now())`, keyID)
	if err != nil {
		return storageUnavailable("revoke api key", err)
	}
	if tag.RowsAffected() == 0 {
		return New(CodeNotFound, "API key not found")
	}
	return nil
}

// storageUnavailable maps an internal storage failure to the stable 503 code.
// The cause is retained for errors.Is/As but is never rendered by Error(),
// which prints only the code and message; no key material is included.
func storageUnavailable(op string, cause error) *Error {
	err := New(CodeTemporarilyUnavailable, "withdrawal storage unavailable")
	err.cause = fmt.Errorf("%s: %w", op, cause)
	return err
}
