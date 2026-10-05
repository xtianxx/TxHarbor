//go:build linux

// origin-supervisor-identity_test.go is the strict Linux /proc identity
// classifier used by the supervised-origin gate tests, with a narrow injected
// read seam so its classes are proven deterministically as unit tests.
//
// Disappearance evidence is asymmetric on purpose: only an authoritative
// ENOENT or a successfully read, different start identity proves that the
// observed process is gone. EACCES, IO errors, transient read failures,
// partial or malformed stat data and every other unreadable state are
// inspection_unknown and must be refused as evidence, never read as gone. A
// zombie keeps the same start identity observable until it is reaped, so it
// classifies as alive here.
package recovery_test

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

// originSupervisorReadStat is the injection seam used only by the
// deterministic unit tests below. Its default is the real /proc reader; the
// native pg_restore supervision test still observes real kernel state.
var originSupervisorReadStat = os.ReadFile

type originSupervisorPresence int

const (
	// originSupervisorPresenceUnknown: the identity could not be inspected.
	// The caller must refuse this as evidence, never read it as gone/reaped.
	originSupervisorPresenceUnknown originSupervisorPresence = iota
	// originSupervisorPresenceAlive: the same start identity is observable,
	// including zombie state Z (an unreaped process).
	originSupervisorPresenceAlive
	// originSupervisorPresenceGone: authoritative disappearance: ENOENT or a
	// different start identity (this PID no longer names that process).
	originSupervisorPresenceGone
)

func (p originSupervisorPresence) String() string {
	switch p {
	case originSupervisorPresenceAlive:
		return "alive"
	case originSupervisorPresenceGone:
		return "gone"
	default:
		return "inspection_unknown"
	}
}

type originSupervisorProcStat struct {
	state string
	ppid  int
	start uint64
}

// parseOriginSupervisorProcStat parses the fields needed from
// /proc/<pid>/stat. comm is parenthesized and may itself contain spaces or
// ')'; fields after its final ')' begin at field 3, so starttime (field 22) is
// index 19.
func parseOriginSupervisorProcStat(data []byte) (originSupervisorProcStat, error) {
	closeIndex := strings.LastIndexByte(string(data), ')')
	if closeIndex < 0 {
		return originSupervisorProcStat{}, errors.New("malformed process stat: no command terminator")
	}
	fields := strings.Fields(string(data[closeIndex+1:]))
	if len(fields) <= 19 {
		return originSupervisorProcStat{}, errors.New("malformed process stat: fewer than 20 fields")
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return originSupervisorProcStat{}, fmt.Errorf("malformed process stat ppid: %w", err)
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return originSupervisorProcStat{}, fmt.Errorf("malformed process stat starttime: %w", err)
	}
	return originSupervisorProcStat{state: fields[0], ppid: ppid, start: start}, nil
}

// originSupervisorPresenceOf classifies one /proc observation of pid against
// the expected start identity. Only ENOENT or a different successfully-read
// start identity classify as gone; anything unreadable is inspection_unknown.
func originSupervisorPresenceOf(pid int, startID uint64) (originSupervisorPresence, error) {
	data, err := originSupervisorReadStat(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return originSupervisorPresenceGone, nil
		}
		return originSupervisorPresenceUnknown, err
	}
	stat, err := parseOriginSupervisorProcStat(data)
	if err != nil {
		return originSupervisorPresenceUnknown, err
	}
	if stat.start != startID {
		return originSupervisorPresenceGone, nil
	}
	return originSupervisorPresenceAlive, nil
}

// awaitOriginSupervisorGone polls up to timeout and reports only authoritative
// states: gone on ENOENT/different start identity, alive while the same start
// identity stays observable, inspection_unknown (with the cause) when /proc
// cannot be read. Timeout expiry is never by itself reaping/drain evidence.
func awaitOriginSupervisorGone(pid int, startID uint64, timeout time.Duration) (originSupervisorPresence, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		presence, err := originSupervisorPresenceOf(pid, startID)
		if presence == originSupervisorPresenceGone {
			return presence, nil
		}
		if err != nil {
			lastErr = err
		}
		if !time.Now().Before(deadline) {
			if presence == originSupervisorPresenceUnknown {
				return presence, lastErr
			}
			return presence, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// originSupervisorReapError is the exact verdict used by the native
// supervised-child assertion: it returns nil only when the exact start
// identity is authoritatively gone, and otherwise returns the refusal reason
// (still observable vs inspection refused) that the assertion reports.
func originSupervisorReapError(pid int, startID uint64, timeout time.Duration) error {
	presence, err := awaitOriginSupervisorGone(pid, startID, timeout)
	switch presence {
	case originSupervisorPresenceGone:
		return nil
	case originSupervisorPresenceAlive:
		return fmt.Errorf("supervised child PID %d start %d was not reaped: the same start identity is still observable (%s); timeout expiry is not reaping evidence", pid, startID, presence)
	default:
		return fmt.Errorf("supervised child PID %d start %d reaping could not be proven: process inspection refused (%s): %w", pid, startID, presence, err)
	}
}

// ---------------------------------------------------------------------------
// Deterministic inspection-class unit tests (no real process involved).
// ---------------------------------------------------------------------------

// TestOriginSupervisorReapErrorCausalVerdict pins the exact regression: the
// former helper treated any /proc read error as "reaped", so EACCES, IO and
// malformed reads falsely proved the native child gone. The verdict must only
// be nil for authoritative ENOENT/different-start evidence.
func TestOriginSupervisorReapErrorCausalVerdict(t *testing.T) {
	const (
		pid     = 1113
		startID = 161803398
	)
	const shortWait = 60 * time.Millisecond

	cases := []struct {
		name       string
		build      func() ([]byte, error)
		wantNil    bool
		wantReason string
	}{
		{"enoent is authoritative reaping", func() ([]byte, error) {
			return nil, fs.ErrNotExist
		}, true, ""},
		{"different start identity is authoritative reaping", func() ([]byte, error) {
			return originSupervisorStatLine(pid, "pg_restore", "S", 1, startID+1), nil
		}, true, ""},
		{"eacces must not prove reaping", func() ([]byte, error) {
			return nil, fs.ErrPermission
		}, false, "inspection refused"},
		{"io error must not prove reaping", func() ([]byte, error) {
			return nil, syscall.EIO
		}, false, "inspection refused"},
		{"malformed stat must not prove reaping", func() ([]byte, error) {
			return []byte("partial"), nil
		}, false, "inspection refused"},
		{"zombie same-start must not prove reaping", func() ([]byte, error) {
			return originSupervisorStatLine(pid, "pg_restore", "Z", 1, startID), nil
		}, false, "still observable"},
		{"live same-start must not prove reaping", func() ([]byte, error) {
			return originSupervisorStatLine(pid, "pg_restore", "S", 1, startID), nil
		}, false, "still observable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stubOriginSupervisorReadStat(t, func(string) ([]byte, error) { return tc.build() })
			err := originSupervisorReapError(pid, startID, shortWait)
			if tc.wantNil {
				if err != nil {
					t.Fatalf("verdict = %v, want authoritative reaping", err)
				}
				return
			}
			if err == nil {
				t.Fatal("non-authoritative state was accepted as reaping evidence")
			}
			if !strings.Contains(err.Error(), tc.wantReason) {
				t.Fatalf("verdict reason %q does not report %q", err, tc.wantReason)
			}
		})
	}
}

func originSupervisorStatLine(pid int, comm, state string, ppid int, start uint64) []byte {
	fields := make([]string, 20)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0] = state
	fields[1] = strconv.Itoa(ppid)
	fields[19] = strconv.FormatUint(start, 10)
	return []byte(fmt.Sprintf("%d (%s) %s\n", pid, comm, strings.Join(fields, " ")))
}

func stubOriginSupervisorReadStat(t *testing.T, read func(string) ([]byte, error)) {
	t.Helper()
	previous := originSupervisorReadStat
	originSupervisorReadStat = read
	t.Cleanup(func() { originSupervisorReadStat = previous })
}

func TestOriginSupervisorIdentityClassificationStrict(t *testing.T) {
	const (
		pid     = 8105
		startID = 481516234
	)
	cases := []struct {
		name    string
		build   func() ([]byte, error)
		want    originSupervisorPresence
		wantErr bool
	}{
		{"enoent is authoritative disappearance", func() ([]byte, error) {
			return nil, &fs.PathError{Op: "open", Path: fmt.Sprintf("/proc/%d/stat", pid), Err: fs.ErrNotExist}
		}, originSupervisorPresenceGone, false},
		{"eacces refuses evidence", func() ([]byte, error) {
			return nil, &fs.PathError{Op: "open", Path: fmt.Sprintf("/proc/%d/stat", pid), Err: fs.ErrPermission}
		}, originSupervisorPresenceUnknown, true},
		{"io error refuses evidence", func() ([]byte, error) {
			return nil, syscall.EIO
		}, originSupervisorPresenceUnknown, true},
		{"same start identity is alive", func() ([]byte, error) {
			return originSupervisorStatLine(pid, "pg_restore", "S", 1, startID), nil
		}, originSupervisorPresenceAlive, false},
		{"zombie is not reaped and stays alive", func() ([]byte, error) {
			return originSupervisorStatLine(pid, "pg_restore", "Z", 1, startID), nil
		}, originSupervisorPresenceAlive, false},
		{"different start identity is gone (pid reuse)", func() ([]byte, error) {
			return originSupervisorStatLine(pid, "pg_restore", "S", 1, startID+1), nil
		}, originSupervisorPresenceGone, false},
		{"malformed stat refuses evidence", func() ([]byte, error) {
			return []byte(fmt.Sprintf("%d (pg_restore) S 1 2 3", pid)), nil
		}, originSupervisorPresenceUnknown, true},
		{"partial stat without command terminator refuses evidence", func() ([]byte, error) {
			return []byte(fmt.Sprintf("%d pg_restore", pid)), nil
		}, originSupervisorPresenceUnknown, true},
		{"non-numeric starttime refuses evidence", func() ([]byte, error) {
			line := originSupervisorStatLine(pid, "pg_restore", "S", 1, startID)
			replaced := strings.NewReplacer(strconv.FormatUint(startID, 10), "not-a-number").Replace(string(line))
			return []byte(replaced), nil
		}, originSupervisorPresenceUnknown, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stubOriginSupervisorReadStat(t, func(string) ([]byte, error) { return tc.build() })
			got, err := originSupervisorPresenceOf(pid, startID)
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

func TestOriginSupervisorIdentityAwaitStrict(t *testing.T) {
	const (
		pid     = 9107
		startID = 362880111
	)
	const shortWait = 80 * time.Millisecond

	t.Run("enoent is gone", func(t *testing.T) {
		stubOriginSupervisorReadStat(t, func(string) ([]byte, error) { return nil, fs.ErrNotExist })
		presence, err := awaitOriginSupervisorGone(pid, startID, shortWait)
		if presence != originSupervisorPresenceGone || err != nil {
			t.Fatalf("presence=%s err=%v, want gone", presence, err)
		}
	})
	t.Run("unreadable state is refused, never inferred as reaped", func(t *testing.T) {
		stubOriginSupervisorReadStat(t, func(string) ([]byte, error) { return nil, fs.ErrPermission })
		presence, err := awaitOriginSupervisorGone(pid, startID, shortWait)
		if presence != originSupervisorPresenceUnknown || err == nil {
			t.Fatalf("presence=%s err=%v, want inspection_unknown with its cause", presence, err)
		}
	})
	t.Run("transient refusal recovers to a later authoritative read", func(t *testing.T) {
		calls := 0
		stubOriginSupervisorReadStat(t, func(string) ([]byte, error) {
			calls++
			if calls < 3 {
				return nil, fs.ErrPermission
			}
			return nil, fs.ErrNotExist
		})
		presence, err := awaitOriginSupervisorGone(pid, startID, time.Second)
		if presence != originSupervisorPresenceGone || err != nil {
			t.Fatalf("presence=%s err=%v, want gone after a transient refusal", presence, err)
		}
	})
	t.Run("zombie same-start identity stays alive at timeout", func(t *testing.T) {
		stubOriginSupervisorReadStat(t, func(string) ([]byte, error) {
			return originSupervisorStatLine(pid, "pg_restore", "Z", 1, startID), nil
		})
		presence, err := awaitOriginSupervisorGone(pid, startID, shortWait)
		if presence != originSupervisorPresenceAlive || err != nil {
			t.Fatalf("presence=%s err=%v, want alive: timeout expiry is not reaping evidence", presence, err)
		}
	})
	t.Run("same-start live identity is alive at timeout", func(t *testing.T) {
		stubOriginSupervisorReadStat(t, func(string) ([]byte, error) {
			return originSupervisorStatLine(pid, "pg_restore", "S", 1, startID), nil
		})
		presence, err := awaitOriginSupervisorGone(pid, startID, shortWait)
		if presence != originSupervisorPresenceAlive || err != nil {
			t.Fatalf("presence=%s err=%v, want alive", presence, err)
		}
	})
	t.Run("different start identity is gone on first read", func(t *testing.T) {
		stubOriginSupervisorReadStat(t, func(string) ([]byte, error) {
			return originSupervisorStatLine(pid, "pg_restore", "S", 1, startID+7), nil
		})
		presence, err := awaitOriginSupervisorGone(pid, startID, shortWait)
		if presence != originSupervisorPresenceGone || err != nil {
			t.Fatalf("presence=%s err=%v, want gone", presence, err)
		}
	})
	t.Run("malformed stat is refused", func(t *testing.T) {
		stubOriginSupervisorReadStat(t, func(string) ([]byte, error) { return []byte("garbage"), nil })
		presence, err := awaitOriginSupervisorGone(pid, startID, shortWait)
		if presence != originSupervisorPresenceUnknown || err == nil {
			t.Fatalf("presence=%s err=%v, want inspection_unknown", presence, err)
		}
	})
}

// TestOriginSupervisorIdentityDefaultReaderRealProc is a mini canary for the
// default seam: the deterministic unit tests must not have replaced the real
// /proc reader, and the test process itself must classify as alive.
func TestOriginSupervisorIdentityDefaultReaderRealProc(t *testing.T) {
	self := os.Getpid()
	data, err := originSupervisorReadStat(fmt.Sprintf("/proc/%d/stat", self))
	if err != nil || !strings.HasPrefix(string(data), fmt.Sprintf("%d (", self)) {
		t.Fatalf("default process reader is not the real /proc reader (err=%v)", err)
	}
	stat, err := parseOriginSupervisorProcStat(data)
	if err != nil {
		t.Fatalf("real proc stat did not parse: %v", err)
	}
	if stat.start == 0 || stat.ppid <= 0 {
		t.Fatalf("real proc stat is not usable: %+v", stat)
	}
	presence, err := originSupervisorPresenceOf(self, stat.start)
	if presence != originSupervisorPresenceAlive || err != nil {
		t.Fatalf("own process presence=%s err=%v, want alive", presence, err)
	}
}
