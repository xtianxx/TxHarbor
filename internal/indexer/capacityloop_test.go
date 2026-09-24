// capacityloop_test.go is the T090 unit layer: the loop-facing capacity pause
// controller (evaluation matrix, recovery wait, fail-safe observation) and the
// scanner wiring rules. The real loop behavior at a hard boundary with a
// failed persistence is exercised by the B9 fault drills.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/events"
)

// fakeCapacityGate is a scripted CapacityPauseGate (no database).
type fakeCapacityGate struct {
	snapshots []events.CapacitySnapshot
	errs      []error
	calls     int
}

func (f *fakeCapacityGate) Observe(context.Context) (events.CapacitySnapshot, error) {
	index := f.calls
	f.calls++
	if index >= len(f.snapshots) {
		index = len(f.snapshots) - 1
	}
	snapshot := f.snapshots[index]
	if len(f.errs) > 0 {
		errIndex := index
		if errIndex >= len(f.errs) {
			errIndex = len(f.errs) - 1
		}
		if f.errs[errIndex] != nil {
			return events.CapacitySnapshot{}, f.errs[errIndex]
		}
	}
	return snapshot, nil
}

// scriptedRow is one pgx.Row over fixed values.
type scriptedRow struct {
	values []any
	err    error
}

func (r scriptedRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return fmt.Errorf("scripted row: %d destinations for %d values", len(dest), len(r.values))
	}
	for i, d := range dest {
		switch out := d.(type) {
		case *uint64:
			v, ok := r.values[i].(uint64)
			if !ok {
				return fmt.Errorf("scripted row: value %d is %T, want uint64", i, r.values[i])
			}
			*out = v
		case *string:
			v, ok := r.values[i].(string)
			if !ok {
				return fmt.Errorf("scripted row: value %d is %T, want string", i, r.values[i])
			}
			*out = v
		default:
			return fmt.Errorf("scripted row: unsupported destination %T", d)
		}
	}
	return nil
}

// fakeProgressQuerier returns one scripted row.
type fakeProgressQuerier struct {
	row     pgx.Row
	queried int
}

func (f *fakeProgressQuerier) QueryRow(context.Context, string, ...any) pgx.Row {
	f.queried++
	return f.row
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNewCapacityPauseControllerRefusesNilGate(t *testing.T) {
	if _, err := NewCapacityPauseController(nil, time.Second); err == nil {
		t.Fatal("nil gate must refuse construction")
	}
	controller, err := NewCapacityPauseController(&fakeCapacityGate{}, 0)
	if err != nil {
		t.Fatalf("NewCapacityPauseController: %v", err)
	}
	if controller.poll != DefaultCapacityPausePoll {
		t.Fatalf("poll = %s, want the default %s", controller.poll, DefaultCapacityPausePoll)
	}
}

func TestCapacityPauseControllerEvaluateMatrix(t *testing.T) {
	progressRow := scriptedRow{values: []any{uint64(101), uint64(10), "cfg-hash"}}
	persistErr := errors.New("commit: persistence not safe")

	cases := []struct {
		name      string
		level     events.CapacityLevel
		persist   error
		gateErr   error
		progress  error
		wantPause bool
		wantErr   bool
	}{
		{name: "normal_never_pauses", level: events.CapacityNormal, persist: persistErr},
		{name: "soft_never_pauses", level: events.CapacitySoft, persist: persistErr},
		{name: "hard_safe_persistence_continues", level: events.CapacityHard},
		{name: "hard_failed_persistence_pauses", level: events.CapacityHard, persist: persistErr, wantPause: true},
		{name: "unreadable_observation_errors", level: events.CapacityHard, persist: persistErr, gateErr: errors.New("pg down"), wantErr: true},
		{name: "unreadable_progress_errors", level: events.CapacityHard, persist: persistErr, progress: errors.New("progress unreadable"), wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gate := &fakeCapacityGate{
				snapshots: []events.CapacitySnapshot{{Level: tc.level, PendingTotal: 5}},
				errs:      []error{tc.gateErr},
			}
			controller, err := NewCapacityPauseController(gate, time.Millisecond)
			if err != nil {
				t.Fatalf("controller: %v", err)
			}
			querier := &fakeProgressQuerier{row: progressRow}
			if tc.progress != nil {
				querier.row = scriptedRow{err: tc.progress}
			}
			record, paused, err := controller.Evaluate(context.Background(), querier,
				31337, ReliableProgressDepositSource, tc.persist)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got paused=%v record=%+v", paused, record)
				}
				if paused {
					t.Fatal("an unreadable observation must never pause")
				}
				return
			}
			if err != nil {
				t.Fatalf("Evaluate: %v", err)
			}
			if paused != tc.wantPause {
				t.Fatalf("paused = %v, want %v (record %+v)", paused, tc.wantPause, record)
			}
			if !tc.wantPause {
				return
			}
			if record.Decision.Reason != CapacityPauseReasonNotPersistable {
				t.Fatalf("reason = %q, want %q", record.Decision.Reason, CapacityPauseReasonNotPersistable)
			}
			if record.Decision.Progress.ResumeHeight != 101 || record.Decision.Progress.LastHeight != 100 {
				t.Fatalf("progress = %+v, want last=100 resume=101", record.Decision.Progress)
			}
			plan := record.Plan
			if !plan.HasProgress || plan.ResumeFrom != 101 || plan.ReprocessAll {
				t.Fatalf("rescan plan = %+v, want resume from 101", plan)
			}
			if querier.queried != 1 {
				t.Fatalf("progress reads = %d, want exactly 1", querier.queried)
			}
		})
	}
}

func TestCapacityPauseControllerWaitRecovery(t *testing.T) {
	gate := &fakeCapacityGate{snapshots: []events.CapacitySnapshot{
		{Level: events.CapacityHard},
		{Level: events.CapacityHard},
		{Level: events.CapacitySoft},
	}}
	controller, err := NewCapacityPauseController(gate, time.Millisecond)
	if err != nil {
		t.Fatalf("controller: %v", err)
	}
	if !controller.WaitRecovery(context.Background()) {
		t.Fatal("capacity leaving the hard boundary must report recovery")
	}
	if gate.calls != 3 {
		t.Fatalf("observations = %d, want 3 (hard, hard, soft)", gate.calls)
	}

	// An observation error is never recovery: the wait stays paused until the
	// context ends.
	stuck := &fakeCapacityGate{
		snapshots: []events.CapacitySnapshot{{Level: events.CapacityHard}},
		errs:      []error{errors.New("pg down")},
	}
	stuckController, err := NewCapacityPauseController(stuck, time.Millisecond)
	if err != nil {
		t.Fatalf("controller: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if stuckController.WaitRecovery(ctx) {
		t.Fatal("an unreadable observation must not be treated as recovery")
	}
}

func TestScannersCapacityPauseWiring(t *testing.T) {
	logScanner := &LogScanner{chainID: 31337, logger: testLogger()}
	depositScanner := &DepositScanner{cfg: DepositConfig{ChainID: 31337}}

	// Nil gate: the pre-013 retry path stays in place and no status is
	// recorded.
	for name, scanner := range map[string]interface {
		SetCapacityPauseGate(CapacityPauseGate)
		CapacityPauseStatus() (CapacityPauseStatus, bool)
	}{
		"log":     logScanner,
		"deposit": depositScanner,
	} {
		scanner.SetCapacityPauseGate(nil)
		if _, ok := scanner.CapacityPauseStatus(); ok {
			t.Fatalf("%s scanner: status recorded without a gate", name)
		}
	}

	// A non-hard observation never pauses and never records evidence; the
	// decision ignores a persistence error below the hard boundary.
	logScanner.SetCapacityPauseGate(&fakeCapacityGate{snapshots: []events.CapacitySnapshot{{Level: events.CapacitySoft}}})
	if handled, alive := logScanner.capacityPauseIfNeeded(context.Background(), errors.New("write failed")); handled || !alive {
		t.Fatalf("soft level = handled %v alive %v, want false/true", handled, alive)
	}
	if _, ok := logScanner.CapacityPauseStatus(); ok {
		t.Fatal("soft level recorded a capacity pause")
	}

	depositScanner.SetCapacityPauseGate(&fakeCapacityGate{snapshots: []events.CapacitySnapshot{{Level: events.CapacityNormal}}})
	if handled, alive := depositScanner.capacityPauseIfNeeded(context.Background(), nil); handled || !alive {
		t.Fatalf("normal level = handled %v alive %v, want false/true", handled, alive)
	}
	if _, ok := depositScanner.CapacityPauseStatus(); ok {
		t.Fatal("normal level recorded a capacity pause")
	}
}
