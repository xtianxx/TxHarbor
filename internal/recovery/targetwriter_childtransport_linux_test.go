//go:build linux && integration

// targetwriter_childtransport_linux_test.go proves the package-private child
// argument transport hook: nil hooks keep the exact existing coordinator
// behavior, preflight refusals happen before any marker or launch intent, and
// a late launch refusal after durable intent leaves the guard unknown/active
// with nothing reset or cleaned up.
package recovery

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

func TestTargetWriterChildTransportNilHookUnchanged(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := CanonicalTargetKey(target)
	initCleanWriterGuard(t, f.ctrl, key)
	opts := writerOptions(f, dsn, target, "writer-transport-nil", "/bin/true")
	result, err := runTargetWriter(f.ctx, opts)
	if err != nil {
		t.Fatalf("nil-hook coordinator run: %v", err)
	}
	guard, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || guard.State != controlstore.TargetGuardClean || guard.OperationID != opts.OperationID || guard.AttemptAppName != result.Application {
		t.Fatalf("nil-hook behavior changed: guard=%+v found=%v err=%v", guard, found, err)
	}
}

func TestTargetWriterChildTransportPreflightRefusalBeforeIntent(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := CanonicalTargetKey(target)
	initCleanWriterGuard(t, f.ctrl, key)
	before, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || before.State != controlstore.TargetGuardClean {
		t.Fatalf("initial guard: %+v found=%v err=%v", before, found, err)
	}
	marker := t.TempDir() + "/must-not-launch"
	opts := writerOptions(f, dsn, target, "writer-transport-preflight", writerScript(t, 0, marker))
	var stages []targetWriterChildArgStage
	opts.prepareChildArgs = func(stage targetWriterChildArgStage, executable string, args []string) ([]string, error) {
		stages = append(stages, stage)
		return nil, &targetWriterChildTransportRefusal{stage: stage, reason: "controlled preflight refusal"}
	}
	result, err := runTargetWriter(f.ctx, opts)
	if err == nil || !strings.Contains(err.Error(), "refused at preflight") || !strings.Contains(err.Error(), "controlled preflight refusal") {
		t.Fatalf("preflight refusal missing or unsafe: %v", err)
	}
	if len(stages) != 1 || stages[0] != targetWriterChildArgPreflight {
		t.Fatalf("preflight hook call stages = %v", stages)
	}
	assertWriterChildNotStarted(t, marker)
	after, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || !reflect.DeepEqual(after, before) {
		t.Fatalf("preflight refusal changed the prior clean guard: before=%+v after=%+v found=%v err=%v", before, after, found, err)
	}
	var audits int
	if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM recovery_audit WHERE operation_id=$1`, opts.OperationID).Scan(&audits); err != nil || audits != 0 {
		t.Fatalf("preflight refusal wrote audit rows: count=%d err=%v", audits, err)
	}
	if result.Application != "" {
		var accepted int
		if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM target_writer_test_acceptance WHERE row_ref=$1`, "test:target_writer_acceptance/"+result.Application).Scan(&accepted); err != nil || accepted != 0 {
			t.Fatalf("preflight refusal wrote acceptance rows: count=%d err=%v", accepted, err)
		}
	}
}

func TestTargetWriterChildTransportLaunchRefusalLeavesIntentUnknown(t *testing.T) {
	f := newTargetWriterFixture(t)
	dsn := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := CanonicalTargetKey(target)
	initCleanWriterGuard(t, f.ctrl, key)
	before, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found || before.State != controlstore.TargetGuardClean {
		t.Fatalf("initial guard: %+v found=%v err=%v", before, found, err)
	}
	marker := t.TempDir() + "/must-not-launch"
	opts := writerOptions(f, dsn, target, "writer-transport-launch", writerScript(t, 0, marker))
	var (
		stages        []targetWriterChildArgStage
		originalArgs  [][]string
		preflightArgs []string
	)
	opts.prepareChildArgs = func(stage targetWriterChildArgStage, executable string, args []string) ([]string, error) {
		stages = append(stages, stage)
		originalArgs = append(originalArgs, append([]string(nil), args...))
		if stage == targetWriterChildArgPreflight {
			preflightArgs = append([]string(nil), args...)
			return args, nil
		}
		return nil, &targetWriterChildTransportRefusal{stage: stage, reason: "controlled launch refusal"}
	}
	result, err := runTargetWriter(f.ctx, opts)
	if err == nil || !strings.Contains(err.Error(), "refused at launch") || !strings.Contains(err.Error(), "controlled launch refusal") {
		t.Fatalf("launch refusal missing or unsafe: %v", err)
	}
	wantStages := []targetWriterChildArgStage{targetWriterChildArgPreflight, targetWriterChildArgLaunch}
	if !reflect.DeepEqual(stages, wantStages) {
		t.Fatalf("hook stages = %v, want %v", stages, wantStages)
	}
	if len(originalArgs) != 2 || !reflect.DeepEqual(originalArgs[0], originalArgs[1]) || !reflect.DeepEqual(originalArgs[0], preflightArgs) {
		t.Fatalf("hook did not receive the original canonical vector at both stages: %q", originalArgs)
	}
	canonical := originalArgs[0]
	if len(canonical) != 5 || canonical[0] != "--clean" || canonical[1] != "--if-exists" || canonical[2] != "--no-owner" || canonical[3] != "--no-privileges" || !strings.HasPrefix(canonical[4], "--dbname=") || !strings.Contains(canonical[4], "application_name=") {
		t.Fatalf("canonical child vector is not the exact coordinator invocation: %q", canonical)
	}
	assertWriterChildNotStarted(t, marker)
	after, found, err := controlstore.ReadTargetGuard(f.ctx, f.ctrl, key.String())
	if err != nil || !found {
		t.Fatalf("read guard after launch refusal: found=%v err=%v", found, err)
	}
	if after.State != controlstore.TargetGuardUnknown || !after.LaunchIntent || after.OperationID != opts.OperationID {
		t.Fatalf("late refusal did not conservatively leave intent unknown/active: %+v", after)
	}
	if after.State == controlstore.TargetGuardClean {
		t.Fatal("late refusal reset the guard to clean")
	}
	var accepted int
	if err := f.ctrl.QueryRow(f.ctx, `SELECT count(*) FROM target_writer_test_acceptance WHERE row_ref=$1`, "test:target_writer_acceptance/"+result.Application).Scan(&accepted); err != nil || accepted != 0 {
		t.Fatalf("late refusal wrote acceptance rows: count=%d err=%v", accepted, err)
	}
	// The intent stays durable: a second attempt on the same target must not be
	// able to proceed from a clean state.
	second := writerOptions(f, dsn, target, "writer-transport-after-refusal", "/bin/true")
	if _, err := runTargetWriter(context.Background(), second); err == nil {
		t.Fatal("successor after conservative intent loss was accepted")
	}
}
