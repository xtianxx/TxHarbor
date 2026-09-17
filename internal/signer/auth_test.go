package signer

import (
	"context"
	"strings"
	"testing"
)

func TestCredentialShape(t *testing.T) {
	good, err := GenerateCredential()
	if err != nil {
		t.Fatalf("GenerateCredential: %v", err)
	}
	if !strings.HasPrefix(good, CredentialPrefix) {
		t.Fatalf("credential %q lacks prefix", good)
	}
	again, err := GenerateCredential()
	if err != nil {
		t.Fatalf("GenerateCredential: %v", err)
	}
	if good == again {
		t.Fatalf("two credentials identical")
	}
	h1, h2 := HashCredential(good), HashCredential(good)
	if h1 != h2 || len(h1) != 64 {
		t.Fatalf("hash not deterministic 64-hex: %q", h1)
	}
	for _, bad := range []string{"", "txh_abc", CredentialPrefix, CredentialPrefix + "!!!", CredentialPrefix + displayPrefix(good)} {
		if err := checkCredentialShape(bad); err == nil {
			t.Fatalf("shape %q accepted", bad)
		}
	}
	if err := checkCredentialShape(good); err != nil {
		t.Fatalf("good credential rejected: %v", err)
	}
	// The display prefix alone is never a credential.
	if err := checkCredentialShape(displayPrefix(good)); err == nil {
		t.Fatalf("display prefix accepted as credential")
	}
}

func TestAuthenticateMalformedNeverTouchesDB(t *testing.T) {
	for _, bad := range []string{"", "bearer abc", "txs_short"} {
		if _, err := Authenticate(context.Background(), nil, bad); err == nil {
			t.Fatalf("malformed %q accepted", bad)
		} else if re, ok := err.(*RefusalError); !ok || re.Class != ClassUnauthenticated {
			t.Fatalf("malformed %q class = %v, want unauthenticated", bad, err)
		}
	}
}
