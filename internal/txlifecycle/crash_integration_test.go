//go:build integration

package txlifecycle

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/xtianxx/txharbor/internal/eth"
)

// crashBoundaries are the data-model §2 T-boundaries, each driven to its point
// and then hard-killed (os.Exit) in a helper subprocess.
var crashBoundaries = []string{
	"before_t1",
	"after_t1",
	"after_009_before_t2",
	"after_t2_before_region",
	"mid_region_before_dispatch",
	"after_dispatch_before_commit",
	"after_commit_before_response",
	"after_t4",
}

// TestCrashHelper is the kill-point subprocess body. It only runs when the
// parent supplies a migrated DSN and a boundary; it then exits hard so no
// cleanup or deferred commit can run.
func TestCrashHelper(t *testing.T) {
	dsn := os.Getenv(txCrashDSNEnv)
	point := os.Getenv(txCrashPointEnv)
	if dsn == "" || point == "" {
		t.Skip("not a crash-helper run")
	}
	e := newEnvWithDSN(t, dsn)
	driveCrashBoundary(t, e, point)
	os.Exit(137)
}

// driveCrashBoundary advances the real 010 code to one T-boundary.
func driveCrashBoundary(t *testing.T, e *env, point string) {
	t.Helper()
	switch point {
	case "before_t1":
		e.seedUpstream()
	case "after_t1":
		e.seed()
	case "after_009_before_t2":
		f := e.seed()
		_ = f.signOnly()
	case "after_t2_before_region":
		f := e.seed()
		f.sign()
	case "mid_region_before_dispatch":
		f := e.seed()
		f.sign()
		e.exec(`DROP TABLE nonce_scope_holds`)
		_, _ = e.send(f, SendInitial, nil)
	case "after_dispatch_before_commit":
		f := e.seed()
		f.sign()
		e.setDispatchErr(&eth.Error{Kind: eth.KindTimeout, Op: "crash"})
		_, _ = e.send(f, SendInitial, nil)
	case "after_commit_before_response":
		f := e.seed()
		f.sign()
		_, _ = e.send(f, SendInitial, nil)
	case "after_t4":
		f := e.seed()
		e.reconcileIncluded(t, f, synthReceipt(f.sender, 100))
	default:
		t.Fatalf("unknown crash point %q", point)
	}
}

// TestV9bCrashMatrix is T038: kill the process at each T-boundary and assert
// the documented recovery action is determined by the surviving durable state.
// The lane is whitelisted to a dedicated container per boundary (startPGDedicated):
// its helper subprocess is hard-killed and must never share the package-wide
// container's lifecycle.
func TestV9bCrashMatrix(t *testing.T) {
	for _, point := range crashBoundaries {
		t.Run(point, func(t *testing.T) {
			dsn := startPGDedicated(t)
			cmd := exec.Command(os.Args[0], "-test.run=TestCrashHelper", "-test.v")
			cmd.Env = append(os.Environ(), txCrashDSNEnv+"="+dsn, txCrashPointEnv+"="+point)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("crash helper did not exit hard; output=%s", out)
			}
			assertCrashRecovery(t, dsn, point)
		})
	}
}

func assertCrashRecovery(t *testing.T, dsn, point string) {
	t.Helper()
	ctx := context.Background()
	e := newEnvWithDSN(t, dsn)
	const (
		intentID  = "int-1"
		attemptID = "att-1"
	)
	var state string
	var signing, sends int
	_ = e.pool.QueryRow(ctx, `SELECT COALESCE((SELECT state FROM tx_attempts WHERE attempt_id=$1),'')`, attemptID).Scan(&state)
	_ = e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempt_signings WHERE attempt_id=$1`, attemptID).Scan(&signing)
	_ = e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_send_attempts WHERE attempt_id=$1`, attemptID).Scan(&sends)
	claim := ClaimRef{IntentID: intentID, WorkerID: "worker-1", LeaseVersion: 1}

	switch point {
	case "before_t1":
		var attempts int
		if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id=$1`, intentID).Scan(&attempts); err != nil {
			t.Fatal(err)
		}
		if attempts != 0 || signing != 0 || sends != 0 {
			t.Fatalf("before_t1 left partial identity: attempts=%d signing=%d sends=%d", attempts, signing, sends)
		}
		// Recovery: no durable attempt -> caller re-drives T1.
		var bindings int
		if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM nonce_bindings WHERE intent_id=$1`, intentID).Scan(&bindings); err != nil {
			t.Fatal(err)
		}
		if bindings != 1 {
			t.Fatalf("upstream fixture missing: bindings=%d", bindings)
		}
	case "after_t1", "after_009_before_t2":
		if state != "prepared" || signing != 0 || sends != 0 {
			t.Fatalf("%s: state=%s signing=%d sends=%d, want prepared/0/0 (re-do T2 on the same identity)", point, state, signing, sends)
		}
	case "after_t2_before_region":
		if state != "signed" || signing != 1 || sends != 0 {
			t.Fatalf("after_t2: state=%s signing=%d sends=%d, want signed/1/0", state, signing, sends)
		}
		// Recovery: reconcile-before-dispatch, then dispatch the persisted bytes.
		res, err := e.store.Send(ctx, &SendRequest{AttemptID: attemptID, Kind: SendInitial, Claim: claim})
		if err != nil || res.Outcome != "accepted" {
			t.Fatalf("recovery dispatch = %+v %v, want accepted", res, err)
		}
	case "mid_region_before_dispatch":
		if state != "signed" || sends != 0 {
			t.Fatalf("mid-region: state=%s sends=%d, want signed/0", state, sends)
		}
		var aborts int
		if err := e.pool.QueryRow(ctx,
			`SELECT count(*) FROM tx_attempt_events WHERE attempt_id=$1 AND event='region_aborted_no_dispatch'`, attemptID).Scan(&aborts); err != nil {
			t.Fatal(err)
		}
		if aborts == 0 {
			t.Fatal("no region_aborted_no_dispatch evidence (recovery: retry under gates, effect unknown)")
		}
	case "after_dispatch_before_commit":
		if state != "unknown" || sends != 1 {
			t.Fatalf("after-dispatch: state=%s sends=%d, want unknown/1", state, sends)
		}
		var outcome string
		if err := e.pool.QueryRow(ctx, `SELECT outcome FROM tx_send_attempts WHERE attempt_id=$1`, attemptID).Scan(&outcome); err != nil {
			t.Fatal(err)
		}
		if outcome != "unknown" {
			t.Fatalf("durable outcome=%s, want unknown", outcome)
		}
		// Recovery: probe-first; never unknown -> failed.
		e.rpc.mu.Lock()
		e.rpc.txFound = true
		e.rpc.txPending = false
		e.rpc.receipt = synthReceipt("", 100)
		e.rpc.mu.Unlock()
		if _, err := e.store.Reconcile(ctx, attemptID, ""); err != nil {
			t.Fatalf("probe-first recovery: %v", err)
		}
		att, err := e.store.AttemptByID(ctx, attemptID)
		if err != nil || att.State == "failed" {
			t.Fatalf("unknown converted to failure: %+v %v", att, err)
		}
	case "after_commit_before_response":
		if state != "sent" || sends != 1 {
			t.Fatalf("after-commit: state=%s sends=%d, want sent/1", state, sends)
		}
		// Recovery: the caller retry sees already_accepted, no duplicate attempt.
		res, err := e.store.Send(ctx, &SendRequest{AttemptID: attemptID, Kind: SendInitial, Claim: claim})
		if refusalClass(err) != ClassAlreadyAccepted {
			t.Fatalf("caller retry = %v (%s), want already_accepted", err, refusalClass(err))
		}
		_ = res
		var attempts int
		if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id=$1`, intentID).Scan(&attempts); err != nil {
			t.Fatal(err)
		}
		if attempts != 1 {
			t.Fatalf("duplicate attempt after retry: %d", attempts)
		}
	case "after_t4":
		var effect string
		if err := e.pool.QueryRow(ctx, `SELECT effect FROM tx_receipts WHERE attempt_id=$1`, attemptID).Scan(&effect); err != nil {
			t.Fatal(err)
		}
		if effect != "effective" {
			t.Fatalf("after_t4 effect=%s, want effective (no recovery needed)", effect)
		}
	}
}
