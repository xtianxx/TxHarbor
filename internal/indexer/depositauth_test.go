package indexer

import (
	"strconv"
	"strings"
	"testing"
)

// TestDepositAuthIdentityVector pins the R3 canonical encoding used by the
// authorization transaction: the layout must reproduce the standard
// deposit:v1 test vector, or H' would diverge from the T002 identity.
func TestDepositAuthIdentityVector(t *testing.T) {
	asset := strings.Repeat("11", 20)
	watch := strings.Repeat("aa", 20)
	assets, err := parseAuthSnapshot("0x"+asset+":0", "assets")
	if err != nil {
		t.Fatalf("parseAuthSnapshot(assets): %v", err)
	}
	watches, err := parseAuthSnapshot("0x"+watch+":0", "watches")
	if err != nil {
		t.Fatalf("parseAuthSnapshot(watches): %v", err)
	}
	if got := depositAuthIdentity(0, assets, watches); got != "31822b65a6444c91bdaaa04a86582f4db25f35d5dd8ee02b7c2c12a4cf6600f0" {
		t.Fatalf("depositAuthIdentity = %s, want the R3 standard vector 31822b65…cf6600f0", got)
	}
}

func TestParseAuthSnapshot(t *testing.T) {
	good, err := parseAuthSnapshot("0x"+strings.Repeat("11", 20)+":10\n0x"+strings.Repeat("22", 20)+":12", "assets")
	if err != nil {
		t.Fatalf("parseAuthSnapshot(): %v", err)
	}
	if len(good) != 2 || good[0].effective != 10 || good[1].address != "0x"+strings.Repeat("22", 20) {
		t.Fatalf("parsed = %+v, want two ordered entries", good)
	}
	for _, tc := range []struct {
		name string
		text string
	}{
		{"blank", ""},
		{"whitespace", "  \n "},
		{"no colon", "0x" + strings.Repeat("11", 20)},
		{"bad address", "0x1234:10"},
		{"bad effective", "0x" + strings.Repeat("11", 20) + ":abc"},
		{"negative effective", "0x" + strings.Repeat("11", 20) + ":-1"},
		{"unsorted", "0x" + strings.Repeat("22", 20) + ":10\n0x" + strings.Repeat("11", 20) + ":10"},
		{"duplicate", "0x" + strings.Repeat("11", 20) + ":10\n0x" + strings.Repeat("11", 20) + ":10"},
	} {
		if _, err := parseAuthSnapshot(tc.text, "assets"); err == nil {
			t.Fatalf("%s: parseAuthSnapshot() = nil, want an error", tc.name)
		}
	}
	// Same address with different effective heights stays distinct. Note the
	// canonical order is lexicographic by full line (T002 sorts by String()):
	// ":10" precedes ":5" because '1' < '5'.
	distinct, err := parseAuthSnapshot("0x"+strings.Repeat("11", 20)+":10\n0x"+strings.Repeat("11", 20)+":5", "assets")
	if err != nil {
		t.Fatalf("parseAuthSnapshot(distinct heights): %v", err)
	}
	if len(distinct) != 2 {
		t.Fatalf("distinct heights = %d entries, want 2", len(distinct))
	}
}

func TestDepositAuthReplayFrom(t *testing.T) {
	entry := func(addr string, eff uint64) authSnapshotEntry {
		return authSnapshotEntry{address: addr, effective: eff, line: addr + ":" + strconv.FormatUint(eff, 10)}
	}
	a := "0x" + strings.Repeat("aa", 20)
	b := "0x" + strings.Repeat("bb", 20)
	w := "0x" + strings.Repeat("cc", 20)

	// Pure shrink (same combos, same start): replay stays at the position.
	replay, aff := depositAuthReplayFrom(10, 10,
		[]authSnapshotEntry{entry(a, 10)}, []authSnapshotEntry{entry(w, 10)},
		[]authSnapshotEntry{entry(a, 10)}, []authSnapshotEntry{entry(w, 10)},
		15, []string{a})
	if replay != 15 || len(aff.assets) != 0 {
		t.Fatalf("shrink replay = %d affected %v, want 15 with no affected assets", replay, aff.assets)
	}
	// New asset effective inside the consumed range pulls replay back.
	replay, aff = depositAuthReplayFrom(10, 10,
		[]authSnapshotEntry{entry(a, 10)}, []authSnapshotEntry{entry(w, 10)},
		[]authSnapshotEntry{entry(a, 10), entry(b, 12)}, []authSnapshotEntry{entry(w, 10)},
		15, []string{a, b})
	if replay != 12 {
		t.Fatalf("new-asset replay = %d, want 12", replay)
	}
	if len(aff.assets) != 1 || aff.assets[0] != b {
		t.Fatalf("affected = %v, want [%s]", aff.assets, b)
	}
	if _, ok := aff.whitelist[strings.ToLower(b)]; !ok {
		t.Fatalf("whitelist %v misses the affected asset", aff.whitelist)
	}
	// Lowered global start puts every new combination in scope.
	replay, _ = depositAuthReplayFrom(5, 10,
		[]authSnapshotEntry{entry(a, 10)}, []authSnapshotEntry{entry(w, 10)},
		[]authSnapshotEntry{entry(a, 10)}, []authSnapshotEntry{entry(w, 10)},
		15, []string{a})
	if replay != 10 {
		t.Fatalf("lowered-start replay = %d, want 10", replay)
	}
	// Postponed effective height never pulls replay below the position.
	replay, _ = depositAuthReplayFrom(10, 10,
		[]authSnapshotEntry{entry(a, 10)}, []authSnapshotEntry{entry(w, 10)},
		[]authSnapshotEntry{entry(a, 18)}, []authSnapshotEntry{entry(w, 10)},
		15, []string{a})
	if replay != 15 {
		t.Fatalf("postponed replay = %d, want 15", replay)
	}
}

func TestParseGapRange(t *testing.T) {
	from, to, ok := parseGapRange("deposit upstream gap: class=structural gap=10-14 cause=below_upstream_start config=aa")
	if !ok || from != 10 || to != 14 {
		t.Fatalf("parseGapRange = (%d,%d,%v), want (10,14,true)", from, to, ok)
	}
	for _, detail := range []string{"no range here", "gap=14-10", "gap=abc", "gap=-1-5"} {
		if _, _, ok := parseGapRange(detail); ok {
			t.Fatalf("parseGapRange(%q) = true, want false", detail)
		}
	}
}
