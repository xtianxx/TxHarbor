// drill_unit_test.go covers the pre-connection trust boundary of the T057
// `recovery-admin drill` wiring without Docker: help, usage errors (missing
// flags, unknown scenario, invalid chain id/local inputs), the refused
// pre-connection paths and the required-configuration refusal by exact key
// name. The real restore/record flow runs in drill_integration_test.go.
package recoveryadmin

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/recovery"
)

func drillTestArgs() []string {
	return []string{"drill",
		"--manifest", "manifest.json",
		"--target-dsn", "postgres://u:d@127.0.0.1:1/drill_env?sslmode=disable",
		"--instance", "11111111-1111-4111-8111-111111111111",
		"--chain-id", "1",
	}
}

func TestDrillFullRecoveryRequiresObservedCompleteStages(t *testing.T) {
	seconds := make(map[recovery.Capability]float64)
	observations := make(map[string]string)
	for i, capability := range recovery.KnownCapabilities() {
		seconds[capability] = float64(i + 1)
		for _, chain := range []uint64{1, 2} {
			observations["chain="+strconv.FormatUint(chain, 10)+";capability="+string(capability)] = "released"
		}
	}
	complete := func(recordOnly bool, restore, verification string, configured bool,
		measured map[recovery.Capability]float64, observed map[string]string, observationErr error, gaps int) bool {
		return recoveryDrillFullRecoveryComplete(recordOnly, restore, verification, configured,
			measured, observed, observationErr, gaps, []uint64{1, 2})
	}
	if !complete(false, "executed", "executed", true, seconds, observations, nil, 0) {
		t.Fatal("all required observed stages should classify a full recovery as complete")
	}
	if complete(false, "executed", "not_configured", false, seconds, observations, nil, 0) {
		t.Fatal("an unconfigured verification phase must not classify a full recovery as complete")
	}
	if complete(true, "skipped_record_only", "skipped_record_only", false, seconds, observations, nil, 0) {
		t.Fatal("record-only must never claim full execution")
	}
	if complete(false, "executed", "not_configured", true, seconds, observations, nil, 0) {
		t.Fatal("configured verification must have executed")
	}
	if complete(false, "executed", "executed", true, seconds, observations, errors.New("gate unavailable"), 0) {
		t.Fatal("release observation errors must prevent positive classification")
	}
	if complete(false, "executed", "executed", true, seconds, observations, nil, 1) {
		t.Fatal("open evidence gaps must prevent positive classification")
	}
	delete(seconds, recovery.KnownCapabilities()[0])
	if complete(false, "executed", "executed", true, seconds, observations, nil, 0) {
		t.Fatal("missing capability release measurement must prevent positive classification")
	}
	seconds[recovery.KnownCapabilities()[0]] = 1
	observations["chain=1;capability="+string(recovery.KnownCapabilities()[0])] = "not_released"
	if complete(false, "executed", "executed", true, seconds, observations, nil, 0) {
		t.Fatal("a non-released capability must prevent positive classification")
	}
	observations["chain=1;capability="+string(recovery.KnownCapabilities()[0])] = "released"
	delete(observations, "chain=2;capability="+string(recovery.KnownCapabilities()[0]))
	if complete(false, "executed", "executed", true, seconds, observations, nil, 0) {
		t.Fatal("every capability on every bound chain must be observed")
	}
	if recoveryDrillFullRecoveryComplete(false, "executed", "executed", true, seconds, observations, nil, 0, nil) {
		t.Fatal("missing bound inventory must never classify complete")
	}
}

func TestDrillFailureInjectionRequiresMatchingCause(t *testing.T) {
	if recoveryDrillInjectionObserved("F1", "refused", []string{"target database unavailable"}, "not_run_restore_refused", 0, nil) {
		t.Fatal("unrelated restore failure must not count as F1")
	}
	if recoveryDrillInjectionObserved("F3", "refused", []string{"manifest is not usable for restore"}, "not_run_restore_refused", 0, nil) {
		t.Fatal("F1-class failure must not count as F3")
	}
	if recoveryDrillInjectionObserved("F6", "executed", nil, "executed", 1, map[string]string{"scope": "approval_stale"}) {
		t.Fatal("an unrelated refusal alongside a gap is not the F6 causal class")
	}
	if !recoveryDrillInjectionObserved("F6", "executed", nil, "executed", 1, map[string]string{"scope": "evidence_gap_pending"}) {
		t.Fatal("an open gap with an observed gap refusal is the F6 causal class")
	}
}

func TestDrillStartedOperationMarkerRefusesReplay(t *testing.T) {
	var stdout, stderr strings.Builder
	replayed, code := recoveryDrillReplay(&stderr, &stdout, &recoveryOpRecorded{
		Result: "refused", Detail: []byte(`{"input_digest":"digest","stage":"started"}`),
	}, "digest", "op")
	if !replayed || code != 1 || !strings.Contains(stderr.String(), "started but non-terminal") {
		t.Fatalf("started operation must refuse ambiguous retry: replayed=%v code=%d stderr=%q", replayed, code, stderr.String())
	}
}

func TestDrillHelpAndUsageErrors(t *testing.T) {
	env := controlTestEnv("postgres://u:c@127.0.0.1:1/control?sslmode=disable",
		"postgres://u:d@127.0.0.1:1/drill_env?sslmode=disable", "deploy:executor")

	code, stdout, _ := runRecoveryAdmin(t, []string{"drill", "-h"}, env)
	if code != 0 || !strings.Contains(stdout, "--record-only") || !strings.Contains(stdout, "--chain-id") {
		t.Fatalf("drill -h must print the full surface: exit=%d stdout=%q", code, stdout)
	}

	cases := []struct {
		name string
		args []string
	}{
		{"no flags", []string{"drill"}},
		{"missing chain id", []string{"drill", "--manifest", "m.json", "--target-dsn", "postgres://u:d@127.0.0.1:1/x?sslmode=disable", "--instance", "11111111-1111-4111-8111-111111111111"}},
		{"unknown scenario", append(drillTestArgs(), "--scenario", "f8")},
		{"case-folded scenario", append(drillTestArgs(), "--scenario", "FULL_RECOVERY")},
		{"bare failure injection id", append(drillTestArgs(), "--scenario", "F1")},
		{"invalid chain id", []string{"drill", "--manifest", "m.json", "--target-dsn", "postgres://u:d@127.0.0.1:1/x?sslmode=disable", "--instance", "11111111-1111-4111-8111-111111111111", "--chain-id", "zero"}},
		{"negative lag input", append(drillTestArgs(), "--backup-lag-seconds", "-5")},
		{"unexpected positional", append(drillTestArgs(), "extra")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, stderr := runRecoveryAdmin(t, tc.args, env)
			if code != 2 {
				t.Fatalf("exit=%d want 2 (stderr=%q)", code, stderr)
			}
			if strings.Contains(stderr, "NOT IMPLEMENTED") {
				t.Fatalf("drill is wired; it must never answer NOT IMPLEMENTED: %q", stderr)
			}
		})
	}
}

func TestDrillRefusesBeforeConnecting(t *testing.T) {
	// Missing control DSN: refused by exact key name before anything opens.
	env := controlTestEnv("", "postgres://u:d@127.0.0.1:1/drill_env?sslmode=disable", "deploy:executor")
	env[config.EnvRecoveryObserverDSN] = "postgres://u:o@127.0.0.1:1/drill_env?sslmode=disable"
	code, _, stderr := runRecoveryAdmin(t, drillTestArgs(), env)
	if code != 1 || !strings.Contains(stderr, config.EnvRecoveryControlDSN) {
		t.Fatalf("missing control DSN must refuse by key name: exit=%d stderr=%q", code, stderr)
	}

	// A --instance that disagrees with the deployment binding never reaches
	// the database.
	env = controlTestEnv("postgres://u:c@127.0.0.1:1/control?sslmode=disable",
		"postgres://u:d@127.0.0.1:1/drill_env?sslmode=disable", "deploy:executor")
	env[config.EnvRecoveryInstance] = "22222222-2222-4222-8222-222222222222"
	code, _, stderr = runRecoveryAdmin(t, drillTestArgs(), env)
	if code != 1 || !strings.Contains(stderr, "instance bound execution") {
		t.Fatalf("a mismatched instance binding must refuse: exit=%d stderr=%q", code, stderr)
	}

	// A real drill requires the explicit deployment observer before opening the
	// control store or attempting any restore side effect.
	env = controlTestEnv("postgres://u:c@127.0.0.1:1/control?sslmode=disable",
		"postgres://u:d@127.0.0.1:1/drill_env?sslmode=disable", "deploy:executor")
	delete(env, config.EnvRecoveryObserverDSN)
	code, _, stderr = runRecoveryAdmin(t, drillTestArgs(), env)
	if code != 1 || !strings.Contains(stderr, config.EnvRecoveryObserverDSN+" is required") {
		t.Fatalf("missing observer must refuse before connecting: exit=%d stderr=%q", code, stderr)
	}

	// Without the executor participant configuration the command refuses
	// fail-closed at the control store (unreachable here), never with success.
	env = controlTestEnv("postgres://u:c@127.0.0.1:1/control?sslmode=disable",
		"postgres://u:d@127.0.0.1:1/drill_env?sslmode=disable", "deploy:executor")
	code, stdout, stderr := runRecoveryAdmin(t, append(drillTestArgs(), "--record-only"), env)
	if code == 0 {
		t.Fatalf("an unreachable control store must refuse: exit=%d stdout=%q", code, stdout)
	}
	if strings.Contains(stdout, "drill_id=") || strings.Contains(stdout, "result=ok") {
		t.Fatalf("a refused drill must never print a recorded run: %q", stdout)
	}
}
