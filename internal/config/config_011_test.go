package config

import (
	"strings"
	"testing"
	"time"
)

// TestLoadWorkerDefaults pins the approved initial configuration (2026-09-17,
// research R14) and that the prior 008/009 knobs are untouched by the 011
// addition.
func TestLoadWorkerDefaults(t *testing.T) {
	cfg, err := Load(fakeEnv(baseEnv()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.WorkerTTL != DefaultWorkerTTL {
		t.Errorf("WorkerTTL = %s, want %s", cfg.WorkerTTL, DefaultWorkerTTL)
	}
	if cfg.WorkerHeartbeat != DefaultWorkerHeartbeat {
		t.Errorf("WorkerHeartbeat = %s, want %s", cfg.WorkerHeartbeat, DefaultWorkerHeartbeat)
	}
	if cfg.WorkerStall != DefaultWorkerStall {
		t.Errorf("WorkerStall = %s, want %s", cfg.WorkerStall, DefaultWorkerStall)
	}
	if cfg.WorkerStall != 300*time.Second {
		t.Errorf("WorkerStall = %s, want the approved independent 300s value", cfg.WorkerStall)
	}
	if cfg.WorkerBackoffBase != DefaultWorkerBackoffBase || cfg.WorkerBackoffMax != DefaultWorkerBackoffMax {
		t.Errorf("backoff = %s/%s, want %s/%s",
			cfg.WorkerBackoffBase, cfg.WorkerBackoffMax, DefaultWorkerBackoffBase, DefaultWorkerBackoffMax)
	}
	if cfg.WorkerScanInterval != DefaultWorkerScanInterval {
		t.Errorf("WorkerScanInterval = %s, want %s", cfg.WorkerScanInterval, DefaultWorkerScanInterval)
	}
	for _, want := range []string{"worker_ttl=30s", "worker_stall=5m0s", "worker_label=\"\""} {
		if !strings.Contains(cfg.Summary(), want) {
			t.Errorf("summary %q lacks %q", cfg.Summary(), want)
		}
	}
}

// TestLoadWorkerCustomValues proves the *_SECONDS/_MS knobs parse and that the
// stall window is accepted independently of the TTL (it is never derived).
func TestLoadWorkerCustomValues(t *testing.T) {
	env := baseEnv()
	env[EnvWorkerTTLSeconds] = "60"
	env[EnvWorkerHeartbeatSeconds] = "20"
	env[EnvWorkerStallSeconds] = "90"
	env[EnvWorkerBackoffBaseMS] = "250"
	env[EnvWorkerBackoffMaxMS] = "5000"
	env[EnvWorkerScanIntervalMS] = "750"
	env[EnvWorkerLabel] = "worker-a"

	cfg, err := Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.WorkerTTL != 60*time.Second || cfg.WorkerHeartbeat != 20*time.Second || cfg.WorkerStall != 90*time.Second {
		t.Errorf("worker durations = %s/%s/%s, want 1m0s/20s/1m30s",
			cfg.WorkerTTL, cfg.WorkerHeartbeat, cfg.WorkerStall)
	}
	if cfg.WorkerBackoffBase != 250*time.Millisecond || cfg.WorkerBackoffMax != 5*time.Second {
		t.Errorf("worker backoff = %s/%s, want 250ms/5s", cfg.WorkerBackoffBase, cfg.WorkerBackoffMax)
	}
	if cfg.WorkerScanInterval != 750*time.Millisecond {
		t.Errorf("WorkerScanInterval = %s, want 750ms", cfg.WorkerScanInterval)
	}
	if cfg.WorkerLabel != "worker-a" {
		t.Errorf("WorkerLabel = %q, want worker-a", cfg.WorkerLabel)
	}
}

// TestLoadWorkerFailClosed covers the T001 refusal rule: heartbeat < TTL and
// stall > TTL, all positive; a bad combination must refuse startup.
func TestLoadWorkerFailClosed(t *testing.T) {
	cases := []struct {
		name string
		knob string
		val  string
	}{
		{"heartbeat equal to ttl", EnvWorkerHeartbeatSeconds, "30"},
		{"heartbeat above ttl", EnvWorkerHeartbeatSeconds, "31"},
		{"stall equal to ttl", EnvWorkerStallSeconds, "30"},
		{"stall below ttl", EnvWorkerStallSeconds, "1"},
		{"zero ttl", EnvWorkerTTLSeconds, "0"},
		{"negative heartbeat", EnvWorkerHeartbeatSeconds, "-1"},
		{"non-integer backoff base", EnvWorkerBackoffBaseMS, "fast"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := baseEnv()
			env[tc.knob] = tc.val
			if _, err := Load(fakeEnv(env)); err == nil {
				t.Fatalf("Load() = nil, want refusal for %s=%s", tc.knob, tc.val)
			}
		})
	}
}
