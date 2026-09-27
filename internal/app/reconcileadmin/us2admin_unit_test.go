// us2admin_unit_test.go pins the identity-boundary helpers of the T023 US2
// claim/permission surface without a database or Docker:
//
//   - the 128-byte audit actor truncation is a display annotation: two legal
//     principals with a colliding truncation stay distinct by their full
//     canonical identity, which is why the replay/conflict comparisons use
//     the untruncated target.owner / target.actor_principal carriers.
//
// The byte-level principal bounds themselves (validatePrincipalID /
// Principal.String()) live in internal/reconciliation with their owner.
package reconcileadmin

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/xtianxx/txharbor/internal/reconciliation"
)

// us2UnitSamePrefixPrincipals builds two principals whose canonical strings
// share the first 128 bytes but differ in full.
func us2UnitSamePrefixPrincipals(t *testing.T) (reconciliation.Principal, reconciliation.Principal, string) {
	t.Helper()
	prefix := "deploy:" + strings.Repeat("é", 60) + "x" // exactly 128 bytes
	if len(prefix) != 128 {
		t.Fatalf("premise: prefix is %d bytes, want exactly 128", len(prefix))
	}
	p1, err := reconciliation.ConfigPrincipal(prefix + "A")
	if err != nil {
		t.Fatalf("ConfigPrincipal(p1): %v", err)
	}
	p2, err := reconciliation.ConfigPrincipal(prefix + "B")
	if err != nil {
		t.Fatalf("ConfigPrincipal(p2): %v", err)
	}
	if p1.String() == p2.String() {
		t.Fatalf("premise: the two principals must be distinct")
	}
	if len(p1.String()) <= 128 || len(p2.String()) <= 128 {
		t.Fatalf("premise: both identities must exceed the 128-byte truncation")
	}
	return p1, p2, prefix
}

func TestReconcileAuditActorTruncationNeverConflatesFullIdentity(t *testing.T) {
	p1, p2, prefix := us2UnitSamePrefixPrincipals(t)

	actor1, actor2 := reconcileAuditActor(p1), reconcileAuditActor(p2)
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
}

func TestReconcileAuditActorLeavesShortIdentitiesIntact(t *testing.T) {
	short, err := reconciliation.ConfigPrincipal("apikey:1234")
	if err != nil {
		t.Fatalf("ConfigPrincipal: %v", err)
	}
	if got := reconcileAuditActor(short); got != short.String() {
		t.Fatalf("short actor = %q, want the canonical identity %q", got, short.String())
	}
	// Exactly 128 bytes is the boundary: no truncation.
	boundary := "deploy:" + strings.Repeat("a", 121)
	if len(boundary) != 128 {
		t.Fatalf("premise: boundary identity is %d bytes, want 128", len(boundary))
	}
	principal, err := reconciliation.ConfigPrincipal(boundary)
	if err != nil {
		t.Fatalf("ConfigPrincipal(boundary): %v", err)
	}
	if got := reconcileAuditActor(principal); got != boundary {
		t.Fatalf("128-byte actor = %q, want it preserved", got)
	}
}
