// capacitypause_test.go is the T071 unit layer: the pause decision matrix
// (soft/normal never pause, hard with safe persistence never pauses, hard with
// a failed persistence pauses from the reliable progress), the rescan
// continuity window, and the structural constraint that the capacity pause
// never creates payment intents and changes no upstream gate.
package indexer

import (
	"errors"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/events"
)

func TestEvaluateCapacityPauseMatrix(t *testing.T) {
	progress := ReliableProgress{
		ChainID: 31337, Source: ReliableProgressDepositSource,
		LastHeight: 100, ResumeHeight: 101, Hash: "cfg", HasProgress: true,
	}
	persistErr := errors.New("append: disk full")

	cases := []struct {
		name       string
		level      events.CapacityLevel
		persistErr error
		wantPause  bool
	}{
		{"normal_continues", events.CapacityNormal, persistErr, false},
		{"soft_continues", events.CapacitySoft, persistErr, false},
		{"hard_safe_persistence_continues", events.CapacityHard, nil, false},
		{"hard_failed_persistence_pauses", events.CapacityHard, persistErr, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dec := EvaluateCapacityPause(tc.level, tc.persistErr, progress)
			if dec.Pause != tc.wantPause {
				t.Fatalf("Pause = %v, want %v (decision %+v)", dec.Pause, tc.wantPause, dec)
			}
			if !tc.wantPause {
				return
			}
			if dec.Reason != CapacityPauseReasonNotPersistable {
				t.Fatalf("Reason = %q, want %q", dec.Reason, CapacityPauseReasonNotPersistable)
			}
			if dec.Progress != progress {
				t.Fatalf("Progress = %+v, want %+v", dec.Progress, progress)
			}
			if !strings.Contains(dec.Detail, "resume=101") || !strings.Contains(dec.Detail, "disk full") {
				t.Fatalf("Detail %q must carry the cause and the resume height", dec.Detail)
			}
		})
	}
}

func TestRescanFromAndContinuity(t *testing.T) {
	progress := ReliableProgress{
		ChainID: 31337, Source: ReliableProgressLogSource,
		LastHeight: 50, ResumeHeight: 51, HasProgress: true,
	}
	dec := EvaluateCapacityPause(events.CapacityHard, errors.New("write failed"), progress)
	plan := RescanFrom(dec)
	if plan.ResumeFrom != 51 || !plan.HasProgress || plan.ReprocessAll {
		t.Fatalf("plan = %+v, want resume from 51 with durable progress", plan)
	}

	// A clean rescan (51..55) has no gap and no duplicate.
	if missing, dup := RescanContinuity(plan, []uint64{51, 52, 53, 54, 55}); len(missing) != 0 || len(dup) != 0 {
		t.Fatalf("clean rescan reported missing=%v duplicates=%v", missing, dup)
	}
	// A skipped height is a missing observation.
	missing, dup := RescanContinuity(plan, []uint64{51, 53, 54})
	if len(missing) != 1 || missing[0] != 52 || len(dup) != 0 {
		t.Fatalf("skip detection = missing %v, duplicates %v; want missing [52]", missing, dup)
	}
	// A re-applied height is a duplicate observation.
	missing, dup = RescanContinuity(plan, []uint64{51, 52, 52, 53})
	if len(missing) != 0 || len(dup) != 1 || dup[0] != 52 {
		t.Fatalf("duplicate detection = missing %v, duplicates %v; want duplicates [52]", missing, dup)
	}

	// No durable checkpoint: the caller reproduces from its configured start
	// height; the helper asserts nothing about a window it cannot see.
	empty := EvaluateCapacityPause(events.CapacityHard, errors.New("write failed"), ReliableProgress{ChainID: 1, Source: ReliableProgressDepositSource})
	emptyPlan := RescanFrom(empty)
	if emptyPlan.HasProgress || !emptyPlan.ReprocessAll {
		t.Fatalf("empty plan = %+v, want ReprocessAll", emptyPlan)
	}
	if missing, dup := RescanContinuity(emptyPlan, []uint64{1, 2, 3}); missing != nil || dup != nil {
		t.Fatalf("empty plan continuity = %v/%v, want nil/nil", missing, dup)
	}
}

// TestCapacityPauseNoNewIntentOrUpstreamGateChange pins the safety boundary:
// capacitypause.go must not import the execution/nonce/txlifecycle/withdrawal
// writers, must not create payment intents and carries no SQL write shape.
func TestCapacityPauseNoNewIntentOrUpstreamGateChange(t *testing.T) {
	body, err := os.ReadFile("capacitypause.go")
	if err != nil {
		t.Fatalf("ReadFile capacitypause.go: %v", err)
	}
	src := string(body)

	parsed, perr := parser.ParseFile(token.NewFileSet(), "capacitypause.go", body, parser.ImportsOnly)
	if perr != nil {
		t.Fatalf("parse capacitypause.go: %v", perr)
	}
	for _, spec := range parsed.Imports {
		path := strings.Trim(spec.Path.Value, `"`)
		for _, forbidden := range []string{
			"github.com/xtianxx/txharbor/internal/execution",
			"github.com/xtianxx/txharbor/internal/withdrawal",
			"github.com/xtianxx/txharbor/internal/txlifecycle",
			"github.com/xtianxx/txharbor/internal/nonce",
		} {
			if path == forbidden {
				t.Errorf("capacitypause.go imports %q; the capacity pause must not create payment work", path)
			}
		}
	}
	for _, token := range []string{
		"payment_intents", "INSERT INTO", "UPDATE ", "DELETE ", "TRUNCATE",
	} {
		if strings.Contains(src, token) {
			t.Errorf("capacitypause.go carries %q; the capacity pause only decides and reads progress", token)
		}
	}
}
