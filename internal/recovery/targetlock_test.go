//go:build linux

package recovery

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// Process supervision is Linux-specific; this test file is built with the
// package's Linux runner on supported deployments.
func TestTargetProcessRunnerCancellationKillsProcessGroupAndWaits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	err := (TargetProcessRunner{DrainTimeout: 2 * time.Second, PollInterval: 10 * time.Millisecond}).Run(ctx, "/bin/sh", "-c", "sleep 30 & wait")
	cancel()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled process group result = %v, want context.Canceled", err)
	}
}

func TestTargetProcessRunnerWaitsForNormalChild(t *testing.T) {
	if err := (TargetProcessRunner{}).Run(context.Background(), "/bin/true"); err != nil {
		t.Fatalf("normal child: %v", err)
	}
}

func TestRunPGCommandStreamsInputAndOutput(t *testing.T) {
	var stdout, stderr bytes.Buffer
	runner := TargetProcessRunner{DrainTimeout: time.Second}
	result, err := runner.RunPGCommand(context.Background(), "/bin/sh",
		[]string{"-c", "cat; printf diagnostic >&2"}, strings.NewReader("restore-artifact"), &stdout, &stderr,
		func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("RunPGCommand: %v", err)
	}
	if result.Outcome != PGCommandSucceeded || !result.Started || !result.ProcessGroupDrained {
		t.Fatalf("unexpected process result: %+v", result)
	}
	if stdout.String() != "restore-artifact" || stderr.String() != "diagnostic" {
		t.Fatalf("stream output mismatch: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestRunPGCommandLockLossKillsAndDrainsGrandchild(t *testing.T) {
	var calls atomic.Int32
	runner := TargetProcessRunner{
		DrainTimeout:        2 * time.Second,
		PollInterval:        10 * time.Millisecond,
		HealthCheckInterval: 20 * time.Millisecond,
		HealthCheckTimeout:  100 * time.Millisecond,
	}
	result, err := runner.RunPGCommand(context.Background(), "/bin/sh", []string{"-c", "sleep 30 & wait"}, nil, nil, nil,
		func(context.Context) error {
			if calls.Add(1) > 1 {
				return errors.New("lock lost: secret command/DSN must not be surfaced")
			}
			return nil
		})
	if err == nil || result.Outcome != PGCommandLockLost || !result.Started || !result.ProcessGroupDrained {
		t.Fatalf("lock loss must terminate and drain the whole group: result=%+v err=%v", result, err)
	}
	if strings.Contains(err.Error(), "secret command") || strings.Contains(err.Error(), "DSN") {
		t.Fatalf("error exposed callback command/DSN detail: %v", err)
	}
}

func TestRunPGCommandCancellationReportsGrandchildDrain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	result, err := (TargetProcessRunner{DrainTimeout: 2 * time.Second, PollInterval: 10 * time.Millisecond}).RunPGCommand(
		ctx, "/bin/sh", []string{"-c", "sleep 30 & wait"}, nil, nil, nil,
		func(context.Context) error { return nil })
	cancel()
	if !errors.Is(err, context.Canceled) || result.Outcome != PGCommandCanceled || !result.ProcessGroupDrained {
		t.Fatalf("cancellation did not prove child group drain: result=%+v err=%v", result, err)
	}
}

func TestRunPGCommandErrorDoesNotEchoCommandOrArguments(t *testing.T) {
	result, err := (TargetProcessRunner{}).RunPGCommand(context.Background(), "/no/such/secret-command",
		[]string{"postgres://user:secret@host/db"}, nil, nil, nil, func(context.Context) error { return nil })
	if err == nil || result.Started || result.Outcome != PGCommandNotStarted {
		t.Fatalf("expected sanitized not-started failure, result=%+v err=%v", result, err)
	}
	if strings.Contains(err.Error(), "secret-command") || strings.Contains(err.Error(), "postgres://") || strings.Contains(err.Error(), "secret@") {
		t.Fatalf("error exposed command or conninfo: %v", err)
	}
}

func TestCanonicalTargetKeyRoleAndOperationIndependent(t *testing.T) {
	a, err := CanonicalTargetKey(controlstore.DSNTarget{Host: "DB.EXAMPLE", Port: 5432, Database: "app", Role: "restore"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalTargetKey(controlstore.DSNTarget{Host: "db.example", Port: 5432, Database: "app", Role: "verify"})
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("role/operation variation changed shared target key")
	}
	c, err := CanonicalTargetKey(controlstore.DSNTarget{Host: "db.example", Port: 5432, Database: "other", Role: "restore"})
	if err != nil {
		t.Fatal(err)
	}
	if a == c {
		t.Fatal("different database unexpectedly shared target key")
	}
	d, err := CanonicalTargetKey(controlstore.DSNTarget{Host: "db.example", Port: 5433, Database: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if a == d {
		t.Fatal("different port unexpectedly shared target key")
	}
}

func TestCanonicalTargetKeyMatchesDurableGuardKey(t *testing.T) {
	target := controlstore.DSNTarget{Host: "DB.EXAMPLE", Port: 5432, Database: "app", Role: "restore"}
	lockKey, err := CanonicalTargetKey(target)
	if err != nil {
		t.Fatal(err)
	}
	guardKey, err := controlstore.TargetGuardKey(target)
	if err != nil {
		t.Fatal(err)
	}
	if lockKey.String() != guardKey {
		t.Fatalf("lock key %q != durable guard key %q", lockKey.String(), guardKey)
	}
}

func TestCanonicalTargetKeyFailsClosedForUnknown(t *testing.T) {
	for _, target := range []controlstore.DSNTarget{
		{}, {Host: "db", Database: "app"},
		{Host: "db", Port: 5432}, {Host: "db\x00bad", Port: 5432, Database: "app"},
	} {
		if _, err := CanonicalTargetKey(target); err == nil {
			t.Fatalf("accepted incomplete/invalid target: %#v", target)
		}
	}
}

func TestTargetLockHealthDetectsLoss(t *testing.T) {
	conn := &fakeAdvisoryConn{}
	lock := &TargetLock{conn: conn, key1: 11, key2: 22}
	conn.held.Store(true)
	if err := lock.Health(context.Background()); err != nil {
		t.Fatalf("healthy lock: %v", err)
	}
	conn.held.Store(false)
	if err := lock.Health(context.Background()); err == nil || !strings.Contains(err.Error(), "ownership was lost") {
		t.Fatalf("lock loss should fail closed, got %v", err)
	}
	conn.pingErr = errors.New("connection unavailable")
	if err := lock.Health(context.Background()); err == nil {
		t.Fatal("unhealthy control connection accepted")
	}
}

func TestTargetLockHealthUnknownFailsClosed(t *testing.T) {
	if err := (*TargetLock)(nil).Health(context.Background()); err == nil {
		t.Fatal("unknown lock accepted")
	}
}

func TestAttemptApplicationNameValidation(t *testing.T) {
	valid := "txh015_" + strings.Repeat("a", 56)
	if err := ValidateAttemptApplicationName(valid); err != nil {
		t.Fatalf("63-byte application name rejected: %v", err)
	}
	for _, name := range []string{"", strings.Repeat("x", 64), "x\x00y", "x\ny", string([]byte{0xff})} {
		if err := ValidateAttemptApplicationName(name); err == nil {
			t.Errorf("invalid app name accepted: %q", name)
		}
	}
	first, err := NewAttemptApplicationName()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewAttemptApplicationName()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("attempt application names were not unique")
	}
	conninfo, err := ConninfoWithAttemptApplicationName("postgres://user:pw@localhost/db?application_name=old", first)
	if err != nil {
		t.Fatal(err)
	}
	config, err := pgx.ParseConfig(conninfo)
	if err != nil {
		t.Fatalf("generated conninfo does not parse: %v", err)
	}
	if got := config.RuntimeParams["application_name"]; got != first {
		t.Fatalf("conninfo application_name = %q, want %q", got, first)
	}
	keywordConninfo, err := ConninfoWithAttemptApplicationName("host=localhost dbname=app application_name=old", first)
	if err != nil {
		t.Fatal(err)
	}
	keywordConfig, err := pgx.ParseConfig(keywordConninfo)
	if err != nil {
		t.Fatalf("generated keyword conninfo does not parse: %v", err)
	}
	if got := keywordConfig.RuntimeParams["application_name"]; got != first {
		t.Fatalf("keyword conninfo application_name = %q, want %q", got, first)
	}
}

type fakeAdvisoryConn struct {
	held    atomic.Bool
	pingErr error
}

func (f *fakeAdvisoryConn) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (f *fakeAdvisoryConn) QueryRow(context.Context, string, ...any) pgx.Row {
	return fakeAdvisoryRow{held: f.held.Load()}
}
func (f *fakeAdvisoryConn) Ping(context.Context) error  { return f.pingErr }
func (f *fakeAdvisoryConn) Close(context.Context) error { return nil }

type fakeAdvisoryRow struct{ held bool }

func (r fakeAdvisoryRow) Scan(dest ...any) error {
	if len(dest) != 1 {
		return errors.New("unexpected scan shape")
	}
	b, ok := dest[0].(*bool)
	if !ok {
		return errors.New("unexpected scan destination")
	}
	*b = r.held
	return nil
}

// Compile-time confirmation that the fake has the same row/connection shape.
var _ pgx.Row = fakeAdvisoryRow{}
var _ advisoryConn = (*fakeAdvisoryConn)(nil)

func TestWaitTargetQuiescentRequiresBound(t *testing.T) {
	if err := WaitTargetQuiescent(context.Background(), "", "", 0, time.Millisecond); err == nil {
		t.Fatal("unbounded/unknown observer request accepted")
	}
	if err := WaitTargetQuiescent(nil, "target", "txh015_tag", time.Second, time.Millisecond); err == nil {
		t.Fatal("unknown observer context accepted")
	}
}
