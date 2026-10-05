//go:build integration && linux

// Production executable topology verification layer: these cases resolve and
// reject the real pg_restore executable (direct ELF required) and exercise the
// controlled test seam, so they run in the integration channel that pins the
// native PostgreSQL 18.6 clients — never in the unit channel, where a
// pg_restore that merely happens to sit on the runner PATH (e.g. a wrapper
// script) must not decide a unit result. Assertions are unchanged by this
// move; a missing native tool is a fatal NOT RUN under CI (fail closed) and a
// skip otherwise.
package recovery

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestTargetWriterRejectsPATHScriptBeforeGuardWork(t *testing.T) {
	binDir := t.TempDir()
	shim := filepath.Join(binDir, "pg_restore")
	if err := os.WriteFile(shim, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)

	_, err := runTargetWriter(nil, TargetWriterOptions{})
	if err == nil || !strings.Contains(err.Error(), "direct executable pg_restore ELF") {
		t.Fatalf("expected production executable topology rejection, got %v", err)
	}
	if strings.Contains(err.Error(), shim) {
		t.Fatalf("error exposed executable path: %v", err)
	}
}

func TestTargetWriterProductionExecutableDiscovery(t *testing.T) {
	if _, err := exec.LookPath("pg_restore"); err != nil {
		requireNativePG18Tool(t, "native pg_restore unavailable")
	}
	resolved, err := resolveTargetWriterExecutable("")
	if err != nil {
		t.Fatalf("installed pg_restore was not accepted: %v", err)
	}
	if !filepath.IsAbs(resolved) {
		t.Fatalf("resolved executable is not absolute: %q", resolved)
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		t.Fatalf("resolved executable is not an executable regular file: %v", err)
	}
}

func TestTargetWriterExecutableTestSeamAllowsControlledScript(t *testing.T) {
	script := filepath.Join(t.TempDir(), "pg-restore-test-script")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	resolved, err := resolveTargetWriterExecutable(script)
	if err != nil {
		t.Fatalf("test executable seam rejected controlled script: %v", err)
	}
	if resolved != script {
		t.Fatalf("test executable seam changed path: got %q, want %q", resolved, script)
	}
}
