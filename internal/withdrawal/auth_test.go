// auth_test.go owns the credential unit surface (T007): key shape and entropy
// round-trip, the pinned SHA-256 digest vector, and the pre-flight
// unauthenticated rejects that must never touch a database.
package withdrawal

import (
	"context"
	"encoding/base64"
	"errors"
	"regexp"
	"strings"
	"testing"
)

// hashShape is the storage contract for api_key.key_hash: 64 lowercase hex
// characters (migrations/000007_withdrawal_creation.sql, api_key_key_hash_check).
var hashShape = regexp.MustCompile(`^[0-9a-f]{64}$`)

// TestGenerateKeyShape pins the generated credential format: the literal
// "txh_" prefix, base64url-no-pad of 32 CSPRNG bytes, a 64-char lowercase hex
// digest, and prefix = the first 8 plaintext characters.
func TestGenerateKeyShape(t *testing.T) {
	plaintext, hash, prefix, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	if !strings.HasPrefix(plaintext, "txh_") {
		t.Fatalf("plaintext %q lacks the txh_ prefix", plaintext)
	}
	entropy, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(plaintext, "txh_"))
	if err != nil {
		t.Fatalf("plaintext body is not base64url: %v", err)
	}
	if len(entropy) != 32 {
		t.Fatalf("decoded entropy = %d bytes, want 32", len(entropy))
	}
	if !hashShape.MatchString(hash) {
		t.Fatalf("hash %q is not 64 lowercase hex characters", hash)
	}
	if want := HashKey(plaintext); hash != want {
		t.Fatalf("GenerateKey hash = %q, want HashKey(plaintext) = %q", hash, want)
	}
	if prefix != plaintext[:8] {
		t.Fatalf("prefix = %q, want the first 8 plaintext characters %q", prefix, plaintext[:8])
	}
}

// TestHashKeyKnownVector pins HashKey to SHA-256 (not a salted KDF or a
// truncated digest), using the canonical NIST vector for "abc".
func TestHashKeyKnownVector(t *testing.T) {
	const want = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got := HashKey("abc"); got != want {
		t.Fatalf("HashKey(\"abc\") = %q, want %q", got, want)
	}
}

// TestHashKeyDeterministicAndDistinct locks the deterministic, collision-free
// behavior the unique index relies on: identical input always hashes the same,
// and one character of difference changes the digest.
func TestHashKeyDeterministicAndDistinct(t *testing.T) {
	if HashKey("txh_same") != HashKey("txh_same") {
		t.Fatal("HashKey is not deterministic for identical input")
	}
	if HashKey("txh_one") == HashKey("txh_two") {
		t.Fatal("HashKey collided for distinct inputs")
	}
}

// TestGenerateKeyUniqueOver100Issues proves the CSPRNG path yields distinct
// plaintexts and distinct digests across many mints.
func TestGenerateKeyUniqueOver100Issues(t *testing.T) {
	plaintexts := make(map[string]bool, 100)
	hashes := make(map[string]bool, 100)
	for i := 0; i < 100; i++ {
		plaintext, hash, _, err := GenerateKey()
		if err != nil {
			t.Fatalf("GenerateKey() #%d error = %v", i, err)
		}
		if plaintexts[plaintext] {
			t.Fatalf("GenerateKey() repeated plaintext %q at #%d", plaintext, i)
		}
		if hashes[hash] {
			t.Fatalf("GenerateKey() repeated hash %q at #%d", hash, i)
		}
		plaintexts[plaintext] = true
		hashes[hash] = true
	}
}

// TestAuthenticateRejectsMalformedWithoutDB ensures the pre-flight shape check
// rejects empty or malformed credentials as CodeUnauthenticated before any
// pool access. The pool is deliberately nil: a regression that touches it on
// this path panics instead of silently passing.
func TestAuthenticateRejectsMalformedWithoutDB(t *testing.T) {
	shortEntropy := keyPrefix + base64.RawURLEncoding.EncodeToString(make([]byte, 31))
	cases := []struct {
		name      string
		presented string
	}{
		{"empty", ""},
		{"garbage", "garbage"},
		{"no prefix", strings.Repeat("a", 47)},
		{"prefix only", keyPrefix},
		{"non-base64 body", keyPrefix + "!!!!"},
		{"wrong entropy length", shortEntropy},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := Authenticate(context.Background(), nil, tc.presented)
			if result != nil {
				t.Fatalf("Authenticate(%q) result = %+v, want nil", tc.presented, result)
			}
			var typed *Error
			if !errors.As(err, &typed) {
				t.Fatalf("Authenticate(%q) error = %v (%T), want *Error", tc.presented, err, err)
			}
			if typed.Code != CodeUnauthenticated {
				t.Fatalf("Authenticate(%q) code = %q, want %q", tc.presented, typed.Code, CodeUnauthenticated)
			}
		})
	}
}

// TestAuthenticateWellFormedKeyWithNilPoolIs503 proves a well-formed credential
// against a miswired nil pool is a retryable storage defect, not a panic: the
// returned error is a *Error classified CodeTemporarilyUnavailable.
func TestAuthenticateWellFormedKeyWithNilPoolIs503(t *testing.T) {
	wellFormed := keyPrefix + base64.RawURLEncoding.EncodeToString(make([]byte, keyEntropyBytes))

	result, err := Authenticate(context.Background(), nil, wellFormed)
	if result != nil {
		t.Fatalf("Authenticate(well-formed, nil pool) result = %+v, want nil", result)
	}
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("Authenticate(well-formed, nil pool) error = %v (%T), want *Error", err, err)
	}
	if typed.Code != CodeTemporarilyUnavailable {
		t.Fatalf("code = %q, want %q", typed.Code, CodeTemporarilyUnavailable)
	}
}
