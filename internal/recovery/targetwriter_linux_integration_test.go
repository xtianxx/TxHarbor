//go:build linux && integration

package recovery

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

func TestTargetWriterFailedAndCanceledDrainedRunsBecomeRebuildRequired(t *testing.T) {
	for _, mode := range []string{"nonzero", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			f := newTargetWriterFixture(t)
			dsn := f.createTarget(t)
			target, err := controlstore.ParseDSNTarget(dsn)
			if err != nil {
				t.Fatal(err)
			}
			key, err := CanonicalTargetKey(target)
			if err != nil {
				t.Fatal(err)
			}
			initCleanWriterGuard(t, f.ctrl, key)
			marker := t.TempDir() + "/started"
			executable := "/bin/false"
			if mode == "canceled" {
				// This test helper launches a real target connection under the
				// supervised process group; the group must drain before cleanup.
				executable = blockingWriterScript(t, marker)
			}
			opts := writerOptions(f, dsn, target, "failure-cleanup-"+mode, executable)
			opts.QuiescenceTimeout = 5 * time.Second
			if mode == "canceled" {
				// Avoid racing caller cancellation with the runner's periodic lock
				// health query; cleanup still independently proves lock ownership.
				opts.Runner.HealthCheckInterval = 10 * time.Second
			}
			probeCalled := false
			opts.Probe = func(context.Context, TargetWriterProof) (TargetWriterProbeResult, error) {
				probeCalled = true
				return TargetWriterProbeResult{}, nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				result, runErr := runTargetWriter(ctx, opts)
				if mode == "canceled" && (result.Command.Outcome != PGCommandCanceled || !result.Command.ProcessGroupDrained) {
					runErr = fmt.Errorf("unexpected canceled command result %+v (run err %v)", result.Command, runErr)
				}
				done <- runErr
			}()
			if mode == "canceled" {
				waitForFile(t, marker, 5*time.Second)
				cancel()
			}
			var runErr error
			select {
			case runErr = <-done:
				if runErr == nil {
					t.Fatal("failed/canceled writer unexpectedly succeeded")
				}
			case <-time.After(15 * time.Second):
				t.Fatal("target writer did not finish bounded failure finalization")
			}
			cancel()
			if probeCalled {
				t.Fatal("failure path invoked target probe")
			}
			guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
			if err != nil || !found || guard.State != controlstore.TargetGuardRebuildRequired || guard.ActiveWriter {
				var remaining string
				_ = f.admin.QueryRow(f.ctx, `SELECT coalesce(string_agg(pid::text || ':' || state || ':' || wait_event, ','), '') FROM pg_stat_activity WHERE application_name=$1`, guard.AttemptAppName).Scan(&remaining)
				t.Fatalf("failed drained attempt not finalized safely: runErr=%v remaining=%v guard=%+v found=%v err=%v", runErr, remaining, guard, found, err)
			}
			next := opts
			next.OperationID += "-retry"
			next.executable = "/bin/true"
			if _, err := runTargetWriter(context.Background(), next); err == nil {
				t.Fatal("successor passed unresolved rebuild-required guard")
			}
		})
	}
}

func blockingWriterScript(t *testing.T, marker string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "blocking-writer")
	script := "#!/bin/sh\ntouch " + marker + "\nexec /bin/sleep 30\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestTargetWriterFailureCleanupRequiresRealTaggedSessionQuiescence(t *testing.T) {
	for _, mode := range []string{"tagged-session-timeout", "tagged-session-disappears"} {
		t.Run(mode, func(t *testing.T) {
			f := newTargetWriterFixture(t)
			dsn := f.createTarget(t)
			target, err := controlstore.ParseDSNTarget(dsn)
			if err != nil {
				t.Fatal(err)
			}
			key, _ := CanonicalTargetKey(target)
			initCleanWriterGuard(t, f.ctrl, key)
			opened, err := f.store.OpenInstance(f.ctx, controlstore.OpenInstanceRequest{
				Kind: "recovery", OpenedBy: "deploy:failure-cleanup",
				TargetGuardKey: key.String(), TargetRoleFingerprint: target.DataTargetFingerprint().RoleFingerprint,
			})
			if err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(t.TempDir(), "writer-started")
			opts := writerOptions(f, dsn, target, "failure-cleanup-"+mode, blockingWriterScript(t, marker))
			opts.InstanceID = opened.InstanceID
			if mode == "tagged-session-timeout" {
				opts.QuiescenceTimeout = 300 * time.Millisecond
			} else {
				opts.QuiescenceTimeout = 3 * time.Second
			}
			opts.Runner.HealthCheckInterval = 10 * time.Second
			var markerToken EvidenceToken
			opts.Prelaunch = func(ctx context.Context, tx pgx.Tx, locked controlstore.InstanceToken) (EvidenceToken, error) {
				write, err := CommitEvidenceWriteTx(ctx, tx, EvidenceWriteRequest{
					InstanceID: locked.InstanceID, Token: LockedEvidenceToken(locked),
					Kind: MutationRestoreStarted, Actor: "deploy:failure-cleanup", OperationID: opts.OperationID,
					Apply: func(context.Context, pgx.Tx, EvidenceToken) error { return nil },
				})
				if err != nil || write.Discarded {
					return EvidenceToken{}, fmt.Errorf("write restore marker: %w", err)
				}
				markerToken = write.Token
				return write.Token, nil
			}
			var probes, acceptances int
			opts.Probe = func(context.Context, TargetWriterProof) (TargetWriterProbeResult, error) {
				probes++
				return successfulWriterProbe(context.Background(), TargetWriterProof{Application: "wrong"})
			}
			opts.Acceptance = func(context.Context, pgx.Tx, TargetWriterProof) (TargetWriterAcceptance, error) {
				acceptances++
				return TargetWriterAcceptance{}, nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { _, err := runTargetWriter(ctx, opts); done <- err }()
			waitForFile(t, marker, 5*time.Second)
			guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
			if err != nil || !found || guard.AttemptAppName == "" {
				t.Fatalf("attempt tag was not durably recorded before child start: guard=%+v found=%v err=%v", guard, found, err)
			}
			taggedDSN, err := ConninfoWithAttemptApplicationName(dsn, guard.AttemptAppName)
			if err != nil {
				t.Fatal(err)
			}
			tagged, err := pgx.Connect(f.ctx, taggedDSN)
			if err != nil {
				t.Fatalf("open genuine tagged target backend: %v", err)
			}
			defer tagged.Close(context.Background())
			quiet, err := CheckTargetQuiescent(f.ctx, dsn, guard.AttemptAppName)
			if err != nil || quiet {
				t.Fatalf("observer did not see the exact live tagged backend: quiet=%v err=%v", quiet, err)
			}
			cancel()
			if mode == "tagged-session-disappears" {
				time.AfterFunc(250*time.Millisecond, func() { _ = tagged.Close(context.Background()) })
			}
			select {
			case runErr := <-done:
				if runErr == nil {
					t.Fatal("canceled writer unexpectedly succeeded")
				}
			case <-time.After(15 * time.Second):
				t.Fatal("writer did not finish bounded failure finalization")
			}
			cancel()
			guard, found, err = controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
			if err != nil || !found {
				t.Fatalf("read post-cleanup guard: found=%v err=%v", found, err)
			}
			if mode == "tagged-session-timeout" {
				if guard.State != controlstore.TargetGuardUnknown || !guard.ActiveWriter || !guard.LaunchIntent {
					t.Fatalf("live tagged backend did not preserve unknown/active guard: %+v", guard)
				}
			} else if guard.State != controlstore.TargetGuardRebuildRequired || guard.ActiveWriter {
				t.Fatalf("disappearing tagged backend was not finalized only after quiescence: %+v", guard)
			}
			if probes != 0 || acceptances != 0 {
				t.Fatalf("failure cleanup invoked success callbacks: probes=%d acceptances=%d", probes, acceptances)
			}
			var acceptedRows, probeEvidence, acceptedAudits int
			if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM target_writer_test_acceptance WHERE row_ref LIKE 'test:target_writer_acceptance/%'`).Scan(&acceptedRows); err != nil {
				t.Fatal(err)
			}
			if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM recovery_evidence WHERE instance_id=$1 AND kind='restore_probe'`, opened.InstanceID).Scan(&probeEvidence); err != nil {
				t.Fatal(err)
			}
			if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM recovery_audit WHERE instance_id=$1 AND action=$2 AND target->>'kind'=$3`, opened.InstanceID, ActionEvidenceWrite, string(MutationRestoreProbeAccepted)).Scan(&acceptedAudits); err != nil {
				t.Fatal(err)
			}
			var generation int64
			if err := f.ctrl.QueryRow(f.ctx, `SELECT evidence_generation FROM recovery_instance WHERE instance_id=$1`, opened.InstanceID).Scan(&generation); err != nil {
				t.Fatal(err)
			}
			if acceptedRows != 0 || probeEvidence != 0 || acceptedAudits != 0 || generation != markerToken.Generation {
				t.Fatalf("failure path changed acceptance evidence/generation: rows=%d probe_evidence=%d accepted_audits=%d generation=%d marker_generation=%d", acceptedRows, probeEvidence, acceptedAudits, generation, markerToken.Generation)
			}
		})
	}
}

func TestTargetWriterFailureCleanupObserverFailuresPreserveUnknownActive(t *testing.T) {
	for _, mode := range []string{"no-visibility", "connect-failure", "query-failure"} {
		t.Run(mode, func(t *testing.T) {
			f := newTargetWriterFixture(t)
			dsn := f.createTarget(t)
			target, err := controlstore.ParseDSNTarget(dsn)
			if err != nil {
				t.Fatal(err)
			}
			key, _ := CanonicalTargetKey(target)
			initCleanWriterGuard(t, f.ctrl, key)
			opts := writerOptions(f, dsn, target, "observer-failure-"+mode, "/bin/false")
			if mode == "connect-failure" {
				u, err := url.Parse(dsn)
				if err != nil {
					t.Fatal(err)
				}
				u.User = url.UserPassword("missing_observer", "wrong-password")
				opts.ObserverDSN = u.String()
			} else if mode == "no-visibility" {
				if _, err := f.admin.Exec(f.ctx, `CREATE ROLE target_writer_limited_observer LOGIN PASSWORD 'limited-password'`); err != nil {
					t.Fatal(err)
				}
				u, err := url.Parse(dsn)
				if err != nil {
					t.Fatal(err)
				}
				u.User = url.UserPassword("target_writer_limited_observer", "limited-password")
				opts.ObserverDSN = u.String()
			} else {
				observerSetup, err := pgx.Connect(f.ctx, dsn)
				if err != nil {
					t.Fatal(err)
				}
				_, err = observerSetup.Exec(f.ctx, `CREATE SCHEMA observer_shadow;
CREATE FUNCTION observer_shadow.fail_activity_lookup() RETURNS text LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'controlled observer query failure'; END $$;
CREATE VIEW observer_shadow.pg_stat_activity AS SELECT observer_shadow.fail_activity_lookup() AS application_name`)
				_ = observerSetup.Close(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				u, err := url.Parse(dsn)
				if err != nil {
					t.Fatal(err)
				}
				query := u.Query()
				query.Set("options", "-csearch_path=observer_shadow,pg_catalog")
				u.RawQuery = query.Encode()
				opts.ObserverDSN = u.String()
			}
			if _, err := CheckTargetQuiescent(f.ctx, opts.ObserverDSN, "txh015_visibility_test"); err == nil {
				t.Fatal("observer failure control did not fail closed")
			}
			if _, err := runTargetWriter(f.ctx, opts); err == nil {
				t.Fatal("writer unexpectedly succeeded despite observer failure")
			}
			guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
			if err != nil || !found || guard.State != controlstore.TargetGuardUnknown || !guard.ActiveWriter || !guard.LaunchIntent {
				t.Fatalf("observer failure incorrectly cleared active unknown attempt: guard=%+v found=%v err=%v", guard, found, err)
			}
		})
	}
}

func TestTargetWriterFailureCleanupSecondMarkFailureRollsBackDrain(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := CanonicalTargetKey(target)
	initCleanWriterGuard(t, f.ctrl, key)
	if _, err := f.admin.Exec(f.ctx, `CREATE FUNCTION target_writer_rebuild_reject() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.disposition = 'rebuild_required' THEN RAISE EXCEPTION 'controlled second-mark failure'; END IF;
  RETURN NEW;
END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.admin.Exec(f.ctx, `CREATE TRIGGER target_writer_rebuild_reject BEFORE UPDATE ON recovery_target_guard FOR EACH ROW EXECUTE FUNCTION target_writer_rebuild_reject()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = f.admin.Exec(context.Background(), `DROP TRIGGER IF EXISTS target_writer_rebuild_reject ON recovery_target_guard`)
		_, _ = f.admin.Exec(context.Background(), `DROP FUNCTION IF EXISTS target_writer_rebuild_reject()`)
	})
	marker := filepath.Join(t.TempDir(), "failed-child-started")
	opts := writerOptions(f, dsn, target, "second-mark-failure", blockingWriterScript(t, marker))
	opts.Runner.HealthCheckInterval = 10 * time.Second
	opts.QuiescenceTimeout = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := runTargetWriter(ctx, opts); done <- err }()
	waitForFile(t, marker, 5*time.Second)
	cancel()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "finalization is uncertain") {
			t.Fatalf("second failure-mark error was not surfaced: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("writer did not finish after controlled second-mark failure")
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || guard.State != controlstore.TargetGuardUnknown || !guard.ActiveWriter || !guard.LaunchIntent {
		t.Fatalf("failed second mark did not roll back first drain mark atomically: guard=%+v found=%v err=%v", guard, found, err)
	}
}
