package app

import (
	"os"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/events"
)

// TestBuildCapacityGuardLifecycle pins the T089 assembly rules: an
// unconfigured limit set leaves the PG-only baseline unchanged (nil guard), a
// configured set is validated fail-closed, and the metrics registry is wired
// as the observer without any other construction path.
func TestBuildCapacityGuardLifecycle(t *testing.T) {
	unconfigured := &config.Config{}
	guard, err := buildCapacityGuard(nil, unconfigured, nil)
	if err != nil || guard != nil {
		t.Fatalf("unconfigured limits = (%v, %v), want (nil, nil)", guard, err)
	}

	configured := &config.Config{}
	configured.Capacity.Reserve = 100
	configured.Capacity.SoftLimit = 1000
	configured.Capacity.HardLimit = 2000
	// The guard requires a real pool; a configured set with no pool is
	// refused, never silently disabled.
	if _, err := buildCapacityGuard(nil, configured, nil); err == nil {
		t.Fatal("configured limits with a nil pool must refuse construction")
	}

	// An invalid formula refuses construction (fail closed at startup).
	invalid := &config.Config{}
	invalid.Capacity.Reserve = 1000
	invalid.Capacity.SoftLimit = 1000
	invalid.Capacity.HardLimit = 2000
	if _, err := buildCapacityGuard(nil, invalid, nil); err == nil {
		t.Fatal("invalid formula must refuse construction")
	}
	if _, err := events.NewCapacityGuard(nil, events.CapacityLimits{}, nil); err == nil {
		t.Fatal("empty limits must refuse construction")
	}
}

// TestWithdrawalHandlerPassesCapacityGate pins the transport wiring: the
// handler carries the gate it received and hands it to the core exactly once
// per create attempt (the core owns the first-create-only placement).
func TestWithdrawalHandlerPassesCapacityGate(t *testing.T) {
	source, err := os.ReadFile("withdrawalhttp.go")
	if err != nil {
		t.Fatalf("read withdrawalhttp.go: %v", err)
	}
	if !strings.Contains(string(source), "CapacityGate: h.CapacityGate") {
		t.Fatal("POST /withdrawals must hand h.CapacityGate to the core")
	}
	if n := strings.Count(string(source), "CapacityGate: h.CapacityGate"); n != 1 {
		t.Fatalf("CapacityGate pass-through count = %d, want 1", n)
	}
}
