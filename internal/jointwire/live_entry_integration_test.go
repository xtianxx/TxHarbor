//go:build integration

package jointwire

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/app"
)

// TestWithdrawalWorkerCommandLiveEntry runs the real command entry against a
// migrated PostgreSQL and a real Anvil node: config load, assembly, joint
// construction, then the Run loop with the real 010/008 participants. It is
// cancelled once the startup line proves the loop was entered, and the raw
// output is written to the evidence directory by the invoking script.
func TestWithdrawalWorkerCommandLiveEntry(t *testing.T) {
	dsn := startMigratedPG(t)
	anvilURL := startAnvil(t)

	// The worker's slog goes to the process default; capturing it lets the
	// test prove the real startup catch-up ran without warnings.
	var workerLog syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&workerLog, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	var stdout, stderr syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	code := make(chan int, 1)
	go func() {
		code <- app.WithdrawalWorkerCommand(ctx, nil, app.Deps{
			Getenv:      fakeEnv(entryEnv(dsn, anvilURL)),
			Stdout:      &stdout,
			Stderr:      &stderr,
			JointWiring: Worker,
		})
	}()

	waitForText(t, &stdout, "joint wiring ready", 30*time.Second)
	// Let the startup catch-up and at least one scan cycle complete against
	// the real schema, then stop the process.
	time.Sleep(2500 * time.Millisecond)
	cancel()
	select {
	case c := <-code:
		if c != 0 {
			t.Fatalf("command exit = %d; stderr=%s", c, stderr.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatal("command did not stop after cancellation")
	}

	t.Logf("command stdout:\n%s", stdout.String())
	t.Logf("command stderr:\n%s", stderr.String())
	t.Logf("worker slog:\n%s", workerLog.String())
	for _, bad := range []string{"startup reconcile failed", "startup projection catch-up failed", "level=ERROR"} {
		if strings.Contains(workerLog.String(), bad) {
			t.Fatalf("worker startup encountered %q:\n%s", bad, workerLog.String())
		}
	}
}
