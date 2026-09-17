// auth.go owns credential verification and caller load (T010; FR-04/FR-05,
// contracts/api.md §1/§5): sha256-hashed bearer credentials with
// constant-time compare, revoked_at enforcement, signer_caller load
// including can_sign, and the operator credential lifecycle (issue/rotate/
// revoke + can_sign flip) behind the signer-auth subcommand. Authenticate
// never decides signing_not_permitted; the permission half is the separate
// PermitSigning gate the serving layer runs right after it (identity is not
// permission).
package signer

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// CredentialPrefix marks signer bearer credentials. The prefix is display
// and routing only, never auth input.
const CredentialPrefix = "txs_"

// credentialRandomBytes is the entropy per credential (32 bytes, base64url).
const credentialRandomBytes = 32

// DB is the minimal database surface auth needs. *pgx.Conn and
// *pgxpool.Pool both satisfy it; rotation runs inside Begin.
type DB interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Caller is the stable caller identity: who this is and whether it may sign.
type Caller struct {
	ID      int64
	Label   string
	CanSign bool
}

// AuthResult is a successful authentication: the caller plus the credential
// row that proved it (for audit).
type AuthResult struct {
	Caller       Caller
	CredentialID int64
}

// GenerateCredential mints one plaintext credential: prefix + 32 random
// bytes in base64url. The plaintext is shown once at issuance and never
// stored; only its sha256 is persisted.
func GenerateCredential() (string, error) {
	raw := make([]byte, credentialRandomBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", refuse(ClassStorageUnavailable, "", "credential entropy unavailable")
	}
	return CredentialPrefix + base64.RawURLEncoding.EncodeToString(raw), nil
}

// HashCredential is sha256(plaintext) lowercase hex, the stored form.
func HashCredential(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return fmt.Sprintf("%x", sum)
}

// displayPrefix is the audit/display prefix: the credential prefix plus a
// short non-secret head. It is never auth input.
func displayPrefix(plaintext string) string {
	head := plaintext
	if len(head) > len(CredentialPrefix)+6 {
		head = head[:len(CredentialPrefix)+6]
	}
	return head
}

// checkCredentialShape rejects anything that cannot be a credential before
// any database read: wrong prefix, bad base64url, or wrong length.
func checkCredentialShape(plaintext string) error {
	if !strings.HasPrefix(plaintext, CredentialPrefix) {
		return refuse(ClassUnauthenticated, "", "credential rejected")
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(plaintext, CredentialPrefix))
	if err != nil || len(raw) != credentialRandomBytes {
		return refuse(ClassUnauthenticated, "", "credential rejected")
	}
	return nil
}

func unauthenticated() error {
	return refuse(ClassUnauthenticated, "", "credential rejected")
}

// Authenticate verifies a presented credential and loads its caller: hash
// lookup restricted to unrevoked rows, constant-time hash compare, then the
// signer_caller row. Every failure — malformed, unknown, revoked, orphaned,
// storage — reports the same generic class; only the serving layer's audit
// records which (never the secret).
func Authenticate(ctx context.Context, db DB, presented string) (AuthResult, error) {
	var zero AuthResult
	if err := checkCredentialShape(presented); err != nil {
		return zero, err
	}
	want := HashCredential(presented)
	var credentialID, callerID int64
	var stored string
	err := db.QueryRow(ctx,
		`SELECT credential_id, caller_id, secret_hash FROM signer_credential
		  WHERE secret_hash = $1 AND revoked_at IS NULL`, want).Scan(&credentialID, &callerID, &stored)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return zero, unauthenticated()
		}
		return zero, refuse(ClassStorageUnavailable, "", "credential store unavailable")
	}
	if subtle.ConstantTimeCompare([]byte(stored), []byte(want)) != 1 {
		return zero, unauthenticated()
	}
	var caller Caller
	err = db.QueryRow(ctx,
		`SELECT caller_id, label, can_sign FROM signer_caller WHERE caller_id = $1`,
		callerID).Scan(&caller.ID, &caller.Label, &caller.CanSign)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return zero, unauthenticated()
		}
		return zero, refuse(ClassStorageUnavailable, "", "credential store unavailable")
	}
	return AuthResult{Caller: caller, CredentialID: credentialID}, nil
}

// PermitSigning is the permission half of the request gate (api.md §4 step 1).
// A proven identity with can_sign=false is refused as signing_not_permitted
// (403), never as unauthenticated (401): identity is not permission. The
// serving layer runs it immediately after Authenticate, before any body read.
func PermitSigning(c Caller) error {
	if !c.CanSign {
		return refuse(ClassSigningNotPermitted, "", "caller may not sign")
	}
	return nil
}

// IssueCredential ensures the caller row exists (operator-assigned identity,
// label refreshed) and inserts one credential, returning its plaintext once.
func IssueCredential(ctx context.Context, db DB, callerID int64, label string) (string, error) {
	if callerID <= 0 {
		return "", refuse(ClassValidationFailed, "caller_id", "caller id must be positive")
	}
	plaintext, err := GenerateCredential()
	if err != nil {
		return "", err
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return "", refuse(ClassStorageUnavailable, "", "credential store unavailable")
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx,
		`INSERT INTO signer_caller (caller_id, label) VALUES ($1, $2)
		  ON CONFLICT (caller_id) DO UPDATE SET label = EXCLUDED.label, updated_at = clock_timestamp()`,
		callerID, label); err != nil {
		return "", refuse(ClassStorageUnavailable, "", "credential store unavailable")
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO signer_credential (caller_id, secret_hash, secret_prefix)
		  VALUES ($1, $2, $3)`, callerID, HashCredential(plaintext), displayPrefix(plaintext)); err != nil {
		return "", refuse(ClassStorageUnavailable, "", "credential store unavailable")
	}
	if err := tx.Commit(ctx); err != nil {
		return "", refuse(ClassStorageUnavailable, "", "credential store unavailable")
	}
	return plaintext, nil
}

// RotateCredential inserts a successor credential and revokes the named
// predecessor atomically, returning the successor plaintext once. The
// predecessor must belong to the caller and be unrevoked.
func RotateCredential(ctx context.Context, db DB, callerID, predecessorID int64) (string, error) {
	plaintext, err := GenerateCredential()
	if err != nil {
		return "", err
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return "", refuse(ClassStorageUnavailable, "", "credential store unavailable")
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx,
		`UPDATE signer_credential SET revoked_at = clock_timestamp()
		  WHERE credential_id = $1 AND caller_id = $2 AND revoked_at IS NULL`,
		predecessorID, callerID)
	if err != nil {
		return "", refuse(ClassStorageUnavailable, "", "credential store unavailable")
	}
	if tag.RowsAffected() != 1 {
		return "", refuse(ClassValidationFailed, "credential_id", "predecessor credential not found or already revoked")
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO signer_credential (caller_id, secret_hash, secret_prefix)
		  VALUES ($1, $2, $3)`, callerID, HashCredential(plaintext), displayPrefix(plaintext)); err != nil {
		return "", refuse(ClassStorageUnavailable, "", "credential store unavailable")
	}
	if err := tx.Commit(ctx); err != nil {
		return "", refuse(ClassStorageUnavailable, "", "credential store unavailable")
	}
	return plaintext, nil
}

// RevokeCredential sets revoked_at on the credential; a revoked credential
// fails the next request's startpoint check with no cache in between.
func RevokeCredential(ctx context.Context, db DB, credentialID int64) error {
	tag, err := db.Exec(ctx,
		`UPDATE signer_credential SET revoked_at = clock_timestamp()
		  WHERE credential_id = $1 AND revoked_at IS NULL`, credentialID)
	if err != nil {
		return refuse(ClassStorageUnavailable, "", "credential store unavailable")
	}
	if tag.RowsAffected() != 1 {
		return refuse(ClassValidationFailed, "credential_id", "credential not found or already revoked")
	}
	return nil
}

// SetCanSign flips the caller's signing permission. False means the caller
// still authenticates but every signing attempt is refused with
// signing_not_permitted by the serving layer.
func SetCanSign(ctx context.Context, db DB, callerID int64, canSign bool) error {
	tag, err := db.Exec(ctx,
		`UPDATE signer_caller SET can_sign = $1, updated_at = clock_timestamp()
		  WHERE caller_id = $2`, canSign, callerID)
	if err != nil {
		return refuse(ClassStorageUnavailable, "", "credential store unavailable")
	}
	if tag.RowsAffected() != 1 {
		return refuse(ClassValidationFailed, "caller_id", "caller not found")
	}
	return nil
}
