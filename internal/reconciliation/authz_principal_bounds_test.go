// authz_principal_bounds_test.go pins the byte-level principal identity
// bounds the T023 claim replay and management dedup comparisons rely on
// (FR-011/012, Q2):
//
//   - validatePrincipalID caps the carrier-scoped id at 200 BYTES (not runes),
//     so the canonical "<kind>:<id>" identity tops out at 207 bytes and fits
//     the recon_permission/audit column caps by construction;
//   - the 128-byte audit actor truncation is a display annotation only: two
//     legal multi-byte identities can share it, which is why every
//     replay/conflict decision compares the untruncated Principal.String().
//
// No database, no Docker.
package reconciliation

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestValidatePrincipalIDCountsBytesNotRunes(t *testing.T) {
	// 200 bytes of ASCII is the boundary; 201 is refused.
	if err := validatePrincipalID(strings.Repeat("a", 200)); err != nil {
		t.Fatalf("200-byte id refused: %v", err)
	}
	if err := validatePrincipalID(strings.Repeat("a", 201)); !errors.Is(err, ErrInvalidPrincipal) {
		t.Fatalf("201-byte id err = %v, want ErrInvalidPrincipal", err)
	}

	// The cap counts BYTES: 100 two-byte runes are exactly at the boundary,
	// one extra byte is refused even though the rune count is far below 200.
	multi := strings.Repeat("é", 100)
	if len(multi) != 200 || utf8.RuneCountInString(multi) != 100 {
		t.Fatalf("premise: multi-byte id is %d bytes / %d runes", len(multi), utf8.RuneCountInString(multi))
	}
	if err := validatePrincipalID(multi); err != nil {
		t.Fatalf("200-byte multi-byte id refused: %v", err)
	}
	if err := validatePrincipalID(multi + "x"); !errors.Is(err, ErrInvalidPrincipal) {
		t.Fatalf("201-byte multi-byte id err = %v, want ErrInvalidPrincipal", err)
	}
	// 200 multi-byte runes are 400 bytes: over the byte cap.
	if err := validatePrincipalID(strings.Repeat("é", 200)); !errors.Is(err, ErrInvalidPrincipal) {
		t.Fatalf("400-byte multi-byte id err = %v, want ErrInvalidPrincipal", err)
	}
}

func TestPrincipalStringUpperBoundIs207Bytes(t *testing.T) {
	for _, kind := range []string{"apikey", "deploy"} {
		id := strings.Repeat("a", 200)
		principal, err := ConfigPrincipal(kind + ":" + id)
		if err != nil {
			t.Fatalf("ConfigPrincipal(%s, 200-byte id) refused: %v", kind, err)
		}
		if got := len(principal.String()); got != 207 {
			t.Fatalf("%s principal String() = %d bytes, want 207 (7-byte kind prefix + 200-byte id)", kind, got)
		}
		if string(principal.Kind()) != kind || principal.ID() != id {
			t.Fatalf("principal round-trip = %s/%d-byte id, want %s", principal.Kind(), len(principal.ID()), kind)
		}
	}
	// The bound is bytes, so a 100-rune multi-byte id fills it exactly.
	multi := strings.Repeat("é", 100)
	principal, err := ConfigPrincipal("deploy:" + multi)
	if err != nil {
		t.Fatalf("ConfigPrincipal(200-byte multi-byte id) refused: %v", err)
	}
	if got := len(principal.String()); got != 207 {
		t.Fatalf("multi-byte principal String() = %d bytes, want 207", got)
	}
}

// principalBoundsSamePrefix builds two distinct principals whose canonical
// strings share the first 128 bytes (past the actor-column truncation).
func principalBoundsSamePrefix(t *testing.T) (Principal, Principal, string) {
	t.Helper()
	prefix := "deploy:" + strings.Repeat("é", 60) + "x" // exactly 128 bytes
	if len(prefix) != 128 {
		t.Fatalf("premise: prefix is %d bytes, want exactly 128", len(prefix))
	}
	p1, err := ConfigPrincipal(prefix + "A")
	if err != nil {
		t.Fatalf("ConfigPrincipal(p1): %v", err)
	}
	p2, err := ConfigPrincipal(prefix + "B")
	if err != nil {
		t.Fatalf("ConfigPrincipal(p2): %v", err)
	}
	if p1.String() == p2.String() || len(p1.String()) <= 128 || len(p2.String()) <= 128 {
		t.Fatalf("premise: the identities must differ past a shared 128-byte prefix")
	}
	return p1, p2, prefix
}

func TestDiscrepancyAuditActorTruncationNeverConflatesFullIdentity(t *testing.T) {
	p1, p2, prefix := principalBoundsSamePrefix(t)

	actor1, actor2 := discrepancyAuditActor(p1), discrepancyAuditActor(p2)
	if actor1 != actor2 {
		t.Fatalf("truncated actors = %q vs %q, want the same 128-byte prefix", actor1, actor2)
	}
	if actor1 != prefix || len(actor1) != 128 {
		t.Fatalf("truncated actor = %q (%d bytes), want the exact 128-byte prefix %q", actor1, len(actor1), prefix)
	}
	if !utf8.ValidString(actor1) {
		t.Fatalf("truncated actor %q is not valid UTF-8", actor1)
	}
	if actor1 == p1.String() || actor2 == p2.String() {
		t.Fatalf("a >128-byte identity was not truncated at all")
	}

	// A short identity is never touched.
	short, err := APIKeyPrincipal(42)
	if err != nil {
		t.Fatalf("APIKeyPrincipal: %v", err)
	}
	if got := discrepancyAuditActor(short); got != short.String() {
		t.Fatalf("short actor = %q, want the canonical identity %q", got, short.String())
	}
}
