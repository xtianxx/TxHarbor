// reorgpolicy_test.go pins the 006 depth math and config refusals (T018,
// V1): exact bound-ancestor arithmetic, closed-boundary allow/refuse,
// MaxInt64-tip exactness, Q1 invalid configs, and the validity annotation
// matrix. No database, no chain, no Docker — `make test` covers it.
package indexer

import (
	"testing"
)

func TestReorgDepthExact(t *testing.T) {
	d, err := reorgDepth(100, 95)
	if err != nil || d != 5 {
		t.Fatalf("reorgDepth(100, 95) = %d, %v; want 5, nil", d, err)
	}
	d, err = reorgDepth(50, 50)
	if err != nil || d != 0 {
		t.Fatalf("reorgDepth(50, 50) = %d, %v; want 0, nil", d, err)
	}
}

func TestReorgDepthMaxInt64Tip(t *testing.T) {
	const maxInt64 = int64(1<<63 - 1)
	d, err := reorgDepth(maxInt64, 0)
	if err != nil || d != maxInt64 {
		t.Fatalf("reorgDepth(MaxInt64, 0) = %d, %v; want exact MaxInt64 (no saturation)", d, err)
	}
}

func TestReorgDepthRefusals(t *testing.T) {
	if _, err := reorgDepth(95, 100); err == nil {
		t.Fatal("reorgDepth(95, 100) = nil error, want ancestor-above-bound refusal")
	}
	if _, err := reorgDepth(-1, 0); err == nil {
		t.Fatal("reorgDepth(-1, 0) = nil error, want negative-input refusal")
	}
	if _, err := reorgDepth(10, -2); err == nil {
		t.Fatal("reorgDepth(10, -2) = nil error, want negative-input refusal")
	}
}

func TestReorgDepthClosedBoundary(t *testing.T) {
	const bound = int64(60)
	// Closed bound: depth == max_depth allows auto-recovery (the transaction
	// refuses only on depth > max_depth — both sides pin the exact math).
	allow, err := reorgDepth(bound, 50)
	if err != nil || allow != 10 {
		t.Fatalf("boundary depth = %d, %v; want exactly 10 (allow side)", allow, err)
	}
	refuse, err := reorgDepth(bound, 49)
	if err != nil || refuse != 11 {
		t.Fatalf("boundary depth = %d, %v; want exactly 11 (refuse side)", refuse, err)
	}
}

func TestParseReorgMaxDepthValid(t *testing.T) {
	n, err := ParseReorgMaxDepth("25")
	if err != nil || n != 25 {
		t.Fatalf("ParseReorgMaxDepth(25) = %d, %v", n, err)
	}
	n, err = ParseReorgMaxDepth("9223372036854775807")
	if err != nil || n != 1<<63-1 {
		t.Fatalf("ParseReorgMaxDepth(MaxInt64) = %d, %v", n, err)
	}
}

func TestParseReorgMaxDepthRefusals(t *testing.T) {
	for _, bad := range []string{"", "0", "-3", "abc", "1.5", "0x10", "9223372036854775808", "18446744073709551615"} {
		if _, err := ParseReorgMaxDepth(bad); err == nil {
			t.Fatalf("ParseReorgMaxDepth(%q) accepted, want Q1 refusal", bad)
		}
	}
}

func TestVerifyReorgPolicyIdentity(t *testing.T) {
	if err := VerifyReorgPolicyIdentity(nil, 25); err != nil {
		t.Fatalf("nil policy refused: %v (pre-bootstrap is ok)", err)
	}
	if err := VerifyReorgPolicyIdentity(&reorgPolicyRow{policySeq: 2, maxDepth: 25}, 25); err != nil {
		t.Fatalf("matching policy refused: %v", err)
	}
	if err := VerifyReorgPolicyIdentity(&reorgPolicyRow{policySeq: 2, maxDepth: 25}, 26); err == nil {
		t.Fatal("drifted policy accepted, want loud refusal")
	}
}

func TestAnnotateRecoveryHeight(t *testing.T) {
	st, v := AnnotateRecoveryHeight(nil, false, 100)
	if st != RecoveryStateNone || v != ValidityUnaffected {
		t.Fatalf("no row = (%s, %s); want (none, valid_unaffected)", st, v)
	}
	st, v = AnnotateRecoveryHeight(nil, true, 100)
	if st != RecoveryStateReleased || v != ValidityUnaffected {
		t.Fatalf("released = (%s, %s); want (released, valid_unaffected)", st, v)
	}
	ancestor := int64(90)
	row := &RecoveryRow{Phase: reorgPhaseReplaying, AncestorNumber: &ancestor}
	st, v = AnnotateRecoveryHeight(row, false, 90)
	if st != RecoveryStateRecovering || v != ValidityUnaffected {
		t.Fatalf("ancestor-side = (%s, %s); want (recovering, valid_unaffected)", st, v)
	}
	st, v = AnnotateRecoveryHeight(row, false, 91)
	if st != RecoveryStateRecovering || v != ValidityProvisionalReplaying {
		t.Fatalf("replay-side = (%s, %s); want (recovering, provisional_replaying)", st, v)
	}
	row.Phase = reorgPhaseReconcileRequired
	st, v = AnnotateRecoveryHeight(row, false, 90)
	if st != RecoveryStatePausedReconcile || v != ValidityUnknownPaused {
		t.Fatalf("reconcile-held = (%s, %s); want (paused_reconcile, unknown_paused)", st, v)
	}
	// No ancestor yet (detected phase): everything is provisional, never
	// labeled complete-or-valid by height.
	row.Phase = reorgPhaseDetected
	row.AncestorNumber = nil
	if st, v := AnnotateRecoveryHeight(row, false, 1); v != ValidityProvisionalReplaying {
		t.Fatalf("pre-ancestor height = (%s, %s); want provisional_replaying", st, v)
	}
}

func TestCheckSuffixContinuity(t *testing.T) {
	local := map[int64][2]string{
		10: {"h10", "h9"}, 11: {"h11", "h10"}, 12: {"h12", "h11"},
	}
	chain := map[int64][2]string{
		10: {"h10", "h9"}, 11: {"h11", "h10"}, 12: {"h12", "h11"},
	}
	if err := checkSuffixContinuity(local, chain, 11, 12); err != nil {
		t.Fatalf("continuous suffix refused: %v", err)
	}
	broken := map[int64][2]string{
		10: {"h10", "h9"}, 11: {"h11", "WRONG"}, 12: {"h12", "h11"},
	}
	if err := checkSuffixContinuity(broken, chain, 11, 12); err == nil {
		t.Fatal("local parent break accepted")
	}
	if err := checkSuffixContinuity(local, broken, 11, 12); err == nil {
		t.Fatal("chain parent break accepted")
	}
	delete(local, 11)
	if err := checkSuffixContinuity(local, chain, 11, 12); err == nil {
		t.Fatal("suffix gap accepted")
	}
}

func TestParseSnapshotEntries(t *testing.T) {
	out := map[string]uint64{}
	if err := parseSnapshotEntries("0xAbC:10\n0xdef:3\n", out); err != nil {
		t.Fatalf("valid snapshot refused: %v", err)
	}
	if out["0xabc"] != 10 || out["0xdef"] != 3 {
		t.Fatalf("snapshot parsed as %v; want lowercased entries", out)
	}
	if err := parseSnapshotEntries("", map[string]uint64{}); err != nil {
		t.Fatalf("empty snapshot refused: %v", err)
	}
	for _, bad := range []string{"0xabc", "0xabc:x", "0xabc:1:2"} {
		if err := parseSnapshotEntries(bad, map[string]uint64{}); err == nil {
			t.Fatalf("snapshot %q accepted, want refusal", bad)
		}
	}
}
