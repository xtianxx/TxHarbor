package nonce

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestCoordGuardDriftGuard pins the local statement-guard literal to the
// repo's shared 5s value declared in internal/indexer/scanner.go (writeGuard).
// The local copy is deliberate (the indexer constant is package-private and
// internal/indexer is out of 008's change scope); this test is the drift
// guard that makes silent divergence impossible.
func TestCoordGuardDriftGuard(t *testing.T) {
	src, err := os.ReadFile("../indexer/scanner.go")
	if err != nil {
		t.Fatalf("read indexer scanner source: %v", err)
	}
	re := regexp.MustCompile(`(?m)^\s*writeGuard\s*=\s*"([^"]*)"`)
	m := re.FindSubmatch(src)
	if m == nil {
		t.Fatal("indexer writeGuard literal not found in scanner.go; drift guard needs updating")
	}
	if string(m[1]) != localWriteGuard {
		t.Fatalf("coordination guard drift: indexer %q != nonce %q", m[1], localWriteGuard)
	}
	if !strings.Contains(localWriteGuard, "5s") {
		t.Fatalf("local guard %q is not the 5s statement guard", localWriteGuard)
	}
}

// TestLockChainFraming pins the 5-step coordination framing order: statement
// guard → ensure coordination row → SELECT … FOR UPDATE. No lease
// acquisition/renewal and no indexer_lease write exist on the path.
func TestLockChainFraming(t *testing.T) {
	f := &fakeQuerier{handler: func(sql string, _ []any) pgx.Row {
		return rows("nonce-008", int64(0), true)
	}}
	if err := lockChain(context.Background(), f, 31337); err != nil {
		t.Fatalf("lockChain error = %v", err)
	}
	execs := f.recordedExecs()
	if len(execs) != 2 || execs[0] != localWriteGuard || !strings.Contains(execs[1], "INSERT INTO indexer_lease") {
		t.Fatalf("lockChain execs = %q, want guard then ensure-row", execs)
	}
	queries := f.recordedQueries()
	if len(queries) != 1 || !strings.Contains(queries[0], "FOR UPDATE") {
		t.Fatalf("lockChain queries = %q, want one FOR UPDATE", queries)
	}
	for _, q := range append(execs, queries...) {
		if strings.Contains(q, "UPDATE indexer_lease") || strings.Contains(q, "fencing_token = fencing_token") {
			t.Fatalf("lockChain touches lease state: %q", q)
		}
	}
}

// TestGateHitRefusalZeroWrites pins the 006 gate refusal: any active pause /
// recovery row refuses with the recovery_active cause and the gate path
// itself issues zero writes.
func TestGateHitRefusalZeroWrites(t *testing.T) {
	t.Run("indexer pause", func(t *testing.T) {
		f := &fakeQuerier{handler: func(sql string, _ []any) pgx.Row {
			switch {
			case strings.Contains(sql, "indexer_pause"):
				return rows(int64(1))
			default:
				return noRows()
			}
		}}
		g, err := readGateStateTx(context.Background(), f, 31337)
		if err != nil {
			t.Fatalf("readGateStateTx error = %v", err)
		}
		if !g.Any() || g.Refusal() != OutcomeRecoveryActive {
			t.Fatalf("gate = %+v refusal = %q", g, g.Refusal())
		}
		if got := f.execCount(); got != 0 {
			t.Fatalf("gate read issued %d writes, want 0", got)
		}
		if causes := g.Causes(); len(causes) != 1 || causes[0] != "indexer_pause" {
			t.Fatalf("causes = %v", causes)
		}
	})

	t.Run("active recovery", func(t *testing.T) {
		f := &fakeQuerier{handler: func(sql string, _ []any) pgx.Row {
			switch {
			case strings.Contains(sql, "FROM reorg_recovery WHERE"):
				return rows("reorg-1", "replaying")
			default:
				return noRows()
			}
		}}
		g, err := readGateStateTx(context.Background(), f, 31337)
		if err != nil {
			t.Fatalf("readGateStateTx error = %v", err)
		}
		if g.Refusal() != OutcomeRecoveryActive || g.RecoveryID != "reorg-1" {
			t.Fatalf("gate = %+v", g)
		}
		if got := f.execCount(); got != 0 {
			t.Fatalf("gate read issued %d writes, want 0", got)
		}
	})

	t.Run("clear gates", func(t *testing.T) {
		f := &fakeQuerier{handler: func(string, []any) pgx.Row { return noRows() }}
		g, err := readGateStateTx(context.Background(), f, 31337)
		if err != nil {
			t.Fatalf("readGateStateTx error = %v", err)
		}
		if g.Any() || g.Refusal() != "" {
			t.Fatalf("clear gates reported as active: %+v", g)
		}
	})
}

// TestRebuildGate pins the fail-closed readiness gate: closed by default,
// only a successful verification opens it, a failure keeps it closed with the
// structured reason.
func TestRebuildGate(t *testing.T) {
	g := NewRebuildGate()
	if g.IsOpen() {
		t.Fatal("fresh gate is open")
	}
	g.KeepClosed("bindings unreadable")
	open, reason := g.State()
	if open || reason != "bindings unreadable" {
		t.Fatalf("State = %v, %q", open, reason)
	}
	g.Open()
	if !g.IsOpen() {
		t.Fatal("gate did not open")
	}
	if _, reason := g.State(); reason != "" {
		t.Fatalf("open gate still carries reason %q", reason)
	}
}
