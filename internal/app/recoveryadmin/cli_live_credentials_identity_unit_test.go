//go:build linux

package recoveryadmin

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Process identity inspection for the live-credential CLI test.
//
// Disappearance evidence is asymmetric on purpose: only an authoritative
// ENOENT or a successfully read, different start identity proves that the
// observed process is gone. EACCES, IO errors, transient failures, malformed
// stat data and every other unreadable state are inspection_unknown and must
// be refused as evidence. A zombie keeps the same start identity observable
// until it is reaped, so it classifies as alive here, never gone.
// ---------------------------------------------------------------------------

// linuxTestReadStat is the narrow injection seam used by the deterministic
// unit tests below. Its default is the real /proc reader; the live native
// canary test still observes real kernel state.
var linuxTestReadStat = os.ReadFile

type linuxTestPresence int

const (
	// linuxTestPresenceUnknown: the identity could not be inspected. The
	// caller must refuse this as evidence, never read it as gone.
	linuxTestPresenceUnknown linuxTestPresence = iota
	// linuxTestPresenceAlive: the same start identity is observable,
	// including zombie state Z (an unreaped process).
	linuxTestPresenceAlive
	// linuxTestPresenceGone: authoritative disappearance: ENOENT or a
	// different start identity (this PID no longer names that process).
	linuxTestPresenceGone
)

func (p linuxTestPresence) String() string {
	switch p {
	case linuxTestPresenceAlive:
		return "alive"
	case linuxTestPresenceGone:
		return "gone"
	default:
		return "inspection_unknown"
	}
}

type linuxTestProcStat struct {
	state string
	ppid  int
	start uint64
}

// parseLinuxTestProcStat parses the fields needed from /proc/<pid>/stat. comm
// is parenthesized and may itself contain spaces or ')'; fields after its
// final ')' begin at field 3, so starttime (field 22) is index 19.
func parseLinuxTestProcStat(data []byte) (linuxTestProcStat, error) {
	closeIndex := strings.LastIndexByte(string(data), ')')
	if closeIndex < 0 {
		return linuxTestProcStat{}, errors.New("malformed process stat: no command terminator")
	}
	fields := strings.Fields(string(data[closeIndex+1:]))
	if len(fields) <= 19 {
		return linuxTestProcStat{}, errors.New("malformed process stat: fewer than 20 fields")
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return linuxTestProcStat{}, fmt.Errorf("malformed process stat ppid: %w", err)
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return linuxTestProcStat{}, fmt.Errorf("malformed process stat starttime: %w", err)
	}
	return linuxTestProcStat{state: fields[0], ppid: ppid, start: start}, nil
}

// linuxTestProcessPresence classifies one /proc observation of pid. A
// start of zero is an existence-only probe (any start identity). Only ENOENT
// or a different successfully-read start identity classify as gone; anything
// unreadable is inspection_unknown.
func linuxTestProcessPresence(pid int, start uint64) (linuxTestPresence, error) {
	data, err := linuxTestReadStat(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return linuxTestPresenceGone, nil
		}
		return linuxTestPresenceUnknown, err
	}
	stat, err := parseLinuxTestProcStat(data)
	if err != nil {
		return linuxTestPresenceUnknown, err
	}
	if start != 0 && stat.start != start {
		return linuxTestPresenceGone, nil
	}
	return linuxTestPresenceAlive, nil
}

// awaitLinuxTestProcessGone polls up to timeout and reports only authoritative
// states: gone on ENOENT/different start identity, alive while the same start
// identity stays observable, inspection_unknown (with the cause) when /proc
// cannot be read. Timeout expiry is never by itself drain evidence.
func awaitLinuxTestProcessGone(pid int, start uint64, timeout time.Duration) (linuxTestPresence, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		presence, err := linuxTestProcessPresence(pid, start)
		if presence == linuxTestPresenceGone {
			return presence, nil
		}
		if err != nil {
			lastErr = err
		}
		if !time.Now().Before(deadline) {
			if presence == linuxTestPresenceUnknown {
				return presence, lastErr
			}
			return presence, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForProcessGone is the boolean compatibility form used by cleanup. An
// inconclusive inspection is false (not drained), never a success.
func waitForProcessGone(pid int, start uint64, timeout time.Duration) bool {
	presence, _ := awaitLinuxTestProcessGone(pid, start, timeout)
	return presence == linuxTestPresenceGone
}

// requireLinuxTestProcessGone fails the test unless the exact identity is
// authoritatively gone. A refused inspection is a failure, never a silent
// pass: it is not disappearance evidence.
func requireLinuxTestProcessGone(t *testing.T, pid int, start uint64, subject string) {
	t.Helper()
	presence, err := linuxTestProcessPresence(pid, start)
	switch presence {
	case linuxTestPresenceGone:
		return
	case linuxTestPresenceAlive:
		t.Fatalf("%s: process identity remains alive (pid=%d start=%d)", subject, pid, start)
	default:
		t.Fatalf("%s: process inspection refused (pid=%d start=%d): %v; disappearance is not proven", subject, pid, start, err)
	}
}

func linuxTestStat(pid int) (uint64, int, error) {
	data, err := linuxTestReadStat(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, 0, err
	}
	stat, err := parseLinuxTestProcStat(data)
	if err != nil {
		return 0, 0, err
	}
	return stat.start, stat.ppid, nil
}

func linuxTestStartIdentity(pid int) (uint64, error) {
	start, _, err := linuxTestStat(pid)
	return start, err
}

func linuxTestProcessObservation(pid int, start uint64) (string, int, int) {
	data, err := linuxTestReadStat(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "gone", 0, 0
		}
		return "inspection_unknown", 0, 0
	}
	stat, err := parseLinuxTestProcStat(data)
	if err != nil {
		return "malformed_stat", 0, 0
	}
	if stat.start != start {
		return "reused", 0, 0
	}
	return stat.state, stat.ppid, 1
}

func linuxTestParentPID(pid int) (int, error) {
	_, parent, err := linuxTestStat(pid)
	return parent, err
}

// ---------------------------------------------------------------------------
// Deterministic inspection-class unit tests (no real process involved).
// These run in any build of this package on Linux, including without the
// integration tag or a container runtime.
// ---------------------------------------------------------------------------

func linuxTestStatLine(pid int, comm, state string, ppid int, start uint64) []byte {
	fields := make([]string, 20)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0] = state
	fields[1] = strconv.Itoa(ppid)
	fields[19] = strconv.FormatUint(start, 10)
	return []byte(fmt.Sprintf("%d (%s) %s\n", pid, comm, strings.Join(fields, " ")))
}

func stubLinuxTestReadStat(t *testing.T, read func(string) ([]byte, error)) {
	t.Helper()
	previous := linuxTestReadStat
	linuxTestReadStat = read
	t.Cleanup(func() { linuxTestReadStat = previous })
}

func TestCLIProcessIdentityClassificationIsStrictAboutUnreadableState(t *testing.T) {
	const (
		pid   = 6206
		start = 246813579
	)
	cases := []struct {
		name    string
		build   func() ([]byte, error)
		want    linuxTestPresence
		wantErr bool
	}{
		{"enoent is authoritative disappearance", func() ([]byte, error) {
			return nil, &fs.PathError{Op: "open", Path: fmt.Sprintf("/proc/%d/stat", pid), Err: fs.ErrNotExist}
		}, linuxTestPresenceGone, false},
		{"eacces refuses evidence", func() ([]byte, error) {
			return nil, &fs.PathError{Op: "open", Path: fmt.Sprintf("/proc/%d/stat", pid), Err: fs.ErrPermission}
		}, linuxTestPresenceUnknown, true},
		{"io error refuses evidence", func() ([]byte, error) {
			return nil, syscall.EIO
		}, linuxTestPresenceUnknown, true},
		{"same start identity is alive", func() ([]byte, error) {
			return linuxTestStatLine(pid, "pg_dump", "S", 1, start), nil
		}, linuxTestPresenceAlive, false},
		{"zombie is not reaped and stays alive", func() ([]byte, error) {
			return linuxTestStatLine(pid, "pg_dump", "Z", 1, start), nil
		}, linuxTestPresenceAlive, false},
		{"different start identity is gone (pid reuse)", func() ([]byte, error) {
			return linuxTestStatLine(pid, "pg_dump", "S", 1, start+1), nil
		}, linuxTestPresenceGone, false},
		{"malformed stat refuses evidence", func() ([]byte, error) {
			return []byte(fmt.Sprintf("%d (pg_dump) S 1 2 3", pid)), nil
		}, linuxTestPresenceUnknown, true},
		{"non-numeric starttime refuses evidence", func() ([]byte, error) {
			line := linuxTestStatLine(pid, "pg_dump", "S", 1, start)
			replaced := strings.NewReplacer(strconv.FormatUint(start, 10), "not-a-number").Replace(string(line))
			return []byte(replaced), nil
		}, linuxTestPresenceUnknown, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stubLinuxTestReadStat(t, func(string) ([]byte, error) { return tc.build() })
			got, err := linuxTestProcessPresence(pid, start)
			if got != tc.want {
				t.Fatalf("presence = %s, want %s", got, tc.want)
			}
			if tc.wantErr && err == nil {
				t.Fatal("unreadable state was classified as evidence without an error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("authoritative state reported an unexpected error: %v", err)
			}
		})
	}
}

func TestCLIAwaitProcessGoneDistinguishesUnknownFromGone(t *testing.T) {
	const (
		pid   = 7307
		start = 135792468
	)
	const shortWait = 80 * time.Millisecond

	t.Run("enoent is gone", func(t *testing.T) {
		stubLinuxTestReadStat(t, func(string) ([]byte, error) { return nil, fs.ErrNotExist })
		presence, err := awaitLinuxTestProcessGone(pid, start, shortWait)
		if presence != linuxTestPresenceGone || err != nil {
			t.Fatalf("presence=%s err=%v, want gone", presence, err)
		}
	})
	t.Run("unreadable state is refused, never inferred as drained", func(t *testing.T) {
		stubLinuxTestReadStat(t, func(string) ([]byte, error) { return nil, fs.ErrPermission })
		presence, err := awaitLinuxTestProcessGone(pid, start, shortWait)
		if presence != linuxTestPresenceUnknown || err == nil {
			t.Fatalf("presence=%s err=%v, want inspection_unknown with its cause", presence, err)
		}
	})
	t.Run("transient refusal recovers to a later authoritative read", func(t *testing.T) {
		calls := 0
		stubLinuxTestReadStat(t, func(string) ([]byte, error) {
			calls++
			if calls < 3 {
				return nil, fs.ErrPermission
			}
			return nil, fs.ErrNotExist
		})
		presence, err := awaitLinuxTestProcessGone(pid, start, time.Second)
		if presence != linuxTestPresenceGone || err != nil {
			t.Fatalf("presence=%s err=%v, want gone after a transient refusal", presence, err)
		}
	})
	t.Run("zombie same-start identity stays alive at timeout", func(t *testing.T) {
		stubLinuxTestReadStat(t, func(string) ([]byte, error) {
			return linuxTestStatLine(pid, "pg_dump", "Z", 1, start), nil
		})
		presence, err := awaitLinuxTestProcessGone(pid, start, shortWait)
		if presence != linuxTestPresenceAlive || err != nil {
			t.Fatalf("presence=%s err=%v, want alive: timeout expiry is not drain evidence", presence, err)
		}
	})
	t.Run("same-start live identity is alive at timeout", func(t *testing.T) {
		stubLinuxTestReadStat(t, func(string) ([]byte, error) {
			return linuxTestStatLine(pid, "pg_dump", "S", 1, start), nil
		})
		presence, err := awaitLinuxTestProcessGone(pid, start, shortWait)
		if presence != linuxTestPresenceAlive || err != nil {
			t.Fatalf("presence=%s err=%v, want alive", presence, err)
		}
	})
	t.Run("different start identity is gone on first read", func(t *testing.T) {
		stubLinuxTestReadStat(t, func(string) ([]byte, error) {
			return linuxTestStatLine(pid, "pg_dump", "S", 1, start+7), nil
		})
		presence, err := awaitLinuxTestProcessGone(pid, start, shortWait)
		if presence != linuxTestPresenceGone || err != nil {
			t.Fatalf("presence=%s err=%v, want gone", presence, err)
		}
	})
	t.Run("malformed stat is refused", func(t *testing.T) {
		stubLinuxTestReadStat(t, func(string) ([]byte, error) { return []byte("garbage"), nil })
		presence, err := awaitLinuxTestProcessGone(pid, start, shortWait)
		if presence != linuxTestPresenceUnknown || err == nil {
			t.Fatalf("presence=%s err=%v, want inspection_unknown", presence, err)
		}
	})
}

// TestCLIProcessIdentityDefaultReaderObservesRealProc is a mini canary for the
// default seam: the deterministic unit tests must not have replaced the real
// /proc reader, and the test process itself must classify as alive. The live
// credential test still inspects real native pg_dump state.
func TestCLIProcessIdentityDefaultReaderObservesRealProc(t *testing.T) {
	self := os.Getpid()
	data, err := linuxTestReadStat(fmt.Sprintf("/proc/%d/stat", self))
	if err != nil || !strings.HasPrefix(string(data), fmt.Sprintf("%d (", self)) {
		t.Fatalf("default process reader is not the real /proc reader (err=%v)", err)
	}
	start, err := linuxTestStartIdentity(self)
	if err != nil {
		t.Fatalf("read own start identity: %v", err)
	}
	presence, err := linuxTestProcessPresence(self, start)
	if presence != linuxTestPresenceAlive || err != nil {
		t.Fatalf("own process presence=%s err=%v, want alive", presence, err)
	}
	observation, ppid, alive := linuxTestProcessObservation(self, start)
	if observation == "" || ppid <= 0 || alive != 1 {
		t.Fatalf("real proc observation is not usable (state=%q ppid=%d alive=%d)", observation, ppid, alive)
	}
}
