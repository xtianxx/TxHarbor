//go:build ignore

// borrowed-auth-staging-statprobe_linux_testhelper.go is the small standalone
// stdlib-only read probe for the borrowed-auth staging harness. It is compiled
// by the Linux drill test and copied ONLY into its own disposable PostgreSQL
// fixture container at a private path distinct from every other helper/probe
// (never the shared fixture census path and never the entry-private probe).
//
// Usage: borrowed-auth-staging-statprobe <pid> [proc-root]
//
// It reads <proc-root>/<pid>/stat and writes the raw bytes to stdout with exit
// 0. Only a real fs.ErrNotExist read failure prints the exact
// STATPROBE_ABSENT sentinel and exits 44: that sentinel combined with the
// probe's own exit 44 is the sole absence discriminator. Every other read
// failure (EACCES, EIO, ELOOP, EISDIR, ENOTDIR, ...) prints
// STATPROBE_UNKNOWN with the numeric errno only and exits 45; a
// malformed/truncated document is detected by the harness's strict Go-side
// PID/state/start parser, never here. The probe performs no write, no signal,
// no parse and grants no authority; it never prints raw error text, stderr,
// DSN or secret material.
package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
)

const (
	statProbeExitOK      = 0
	statProbeExitAbsent  = 44
	statProbeExitUnknown = 45

	statProbeAbsentSentinel = "STATPROBE_ABSENT"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout))
}

// run is the testable entry point: it returns the process exit code and writes
// only the absent sentinel, the unknown sentinel or the raw stat document to
// stdout.
func run(args []string, stdout io.Writer) int {
	if len(args) != 1 && len(args) != 2 {
		return statProbeUnknown(stdout, 0)
	}
	pid, err := strconv.Atoi(args[0])
	if err != nil || pid < 2 {
		return statProbeUnknown(stdout, 0)
	}
	procRoot := "/proc"
	if len(args) == 2 {
		procRoot = args[1]
	}
	raw, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "stat"))
	if err != nil {
		if classifyStatError(err) == statProbeExitAbsent {
			fmt.Fprintln(stdout, statProbeAbsentSentinel)
			return statProbeExitAbsent
		}
		return statProbeUnknown(stdout, statProbeErrno(err))
	}
	if _, err := stdout.Write(raw); err != nil {
		return statProbeExitUnknown
	}
	return statProbeExitOK
}

// classifyStatError preserves the errno distinction instead of collapsing
// every failure into a shell-style boolean: nil is OK, only a real
// fs.ErrNotExist is the absence discriminator and every other error is
// UNKNOWN.
func classifyStatError(err error) int {
	switch {
	case err == nil:
		return statProbeExitOK
	case errors.Is(err, fs.ErrNotExist):
		return statProbeExitAbsent
	default:
		return statProbeExitUnknown
	}
}

// statProbeErrno preserves the numeric errno for safe diagnostics only; it
// never carries error text and reports 0 for a non-errno error.
func statProbeErrno(err error) uintptr {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return uintptr(errno)
	}
	return 0
}

func statProbeUnknown(stdout io.Writer, errno uintptr) int {
	fmt.Fprintf(stdout, "STATPROBE_UNKNOWN errno=%d\n", errno)
	return statProbeExitUnknown
}
