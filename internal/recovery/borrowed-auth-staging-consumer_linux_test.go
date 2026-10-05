//go:build linux && drill

// borrowed-auth-staging-consumer_linux_test.go is the PURE strict consumer for
// the borrowed-auth staging census producer plus the pure lifecycle/identity
// proofs shared with the harness. It has no producer dependency: the negative
// unit data is deliberately synthetic (impossible-producer shapes) to prove
// consumer-only refusal and can never mint a positive capability. The schema is
// the current producer contract (kind/gone_reason/after_start/uncertain) and an
// older report without those mandatory fields is refused.
package recovery_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
)

const borrowedAuthStagingRecognizedProcStates = "RSDTZtXxKWPI"

// borrowedAuthStagingTCPStates are the kernel /proc/net/tcp state codes.
var borrowedAuthStagingTCPStates = map[string]bool{
	"01": true, "02": true, "03": true, "04": true, "05": true, "06": true,
	"07": true, "08": true, "09": true, "0A": true, "0B": true, "0C": true,
}

// borrowedAuthStagingTerminalQuit / ServerEOF / Unknown are the only terminal
// markers of the staging producer protocol; a managed quit is never EOF
// evidence.
const (
	borrowedAuthStagingTerminalQuit      = "CLIENT_QUIT"
	borrowedAuthStagingTerminalServerEOF = "CLIENT_SERVER_EOF"
	borrowedAuthStagingTerminalUnknown   = "CLIENT_UNKNOWN"
)

// borrowedAuthStagingCensusSocket is one socket entry of the strict census.
type borrowedAuthStagingCensusSocket struct {
	Inode  string `json:"inode"`
	Kind   string `json:"kind"`
	TCP    bool   `json:"tcp"`
	Local  string `json:"local,omitempty"`
	Remote string `json:"remote,omitempty"`
	State  string `json:"state,omitempty"`
}

// borrowedAuthStagingCensusChild is one direct postmaster child entry.
type borrowedAuthStagingCensusChild struct {
	PID         int                               `json:"pid"`
	Start       uint64                            `json:"start"`
	PPID        int                               `json:"ppid"`
	StateBefore string                            `json:"state_before"`
	StateAfter  string                            `json:"state_after,omitempty"`
	Gone        bool                              `json:"gone"`
	GoneReason  string                            `json:"gone_reason,omitempty"`
	AfterStart  uint64                            `json:"after_start,omitempty"`
	AfterPPID   int                               `json:"after_ppid,omitempty"`
	Uncertain   bool                              `json:"uncertain,omitempty"`
	FDTotal     int                               `json:"fd_total"`
	FDReadable  int                               `json:"fd_readable"`
	FDENOENT    int                               `json:"fd_enoent"`
	FDUnknown   int                               `json:"fd_unknown"`
	Sockets     []borrowedAuthStagingCensusSocket `json:"sockets"`
}

// borrowedAuthStagingCensus is the immutable report. The postmaster state
// fields are validated when the producer emits them; the current producer
// attests the live before/after postmaster through Complete + zero errors and
// fails the whole report on Z/X/x, which the live test additionally brackets
// with a strict container stat.
type borrowedAuthStagingCensus struct {
	Complete              bool                              `json:"complete"`
	Errors                []string                          `json:"errors"`
	PostmasterPID         int                               `json:"postmaster_pid"`
	StartBefore           uint64                            `json:"start_before"`
	StartAfter            uint64                            `json:"start_after"`
	ChildENOENT           int                               `json:"child_enoent"`
	FDUnknown             int                               `json:"fd_unknown_total"`
	Sockets               []borrowedAuthStagingCensusSocket `json:"sockets"`
	Children              []borrowedAuthStagingCensusChild  `json:"children"`
	PostmasterStateBefore string                            `json:"postmaster_state_before,omitempty"`
	PostmasterStateAfter  string                            `json:"postmaster_state_after,omitempty"`
}

// borrowedAuthStagingRecognizedState reports a kernel-recognized single-char
// process state.
func borrowedAuthStagingRecognizedState(state string) bool {
	return len(state) == 1 && strings.ContainsRune(borrowedAuthStagingRecognizedProcStates, rune(state[0]))
}

// borrowedAuthStagingLiveState reports a recognized state that is not a dead
// state (Z/X/x rejected; T is allowed for a future stopped process).
func borrowedAuthStagingLiveState(state string) bool {
	return borrowedAuthStagingRecognizedState(state) && state != "Z" && state != "X" && state != "x"
}

// borrowedAuthStagingCanonicalEndpoint requires a canonical ip:port string with
// a parseable IP (canonical form), an explicit port and the range 0..65535.
func borrowedAuthStagingCanonicalEndpoint(raw string) error {
	host, portText, err := net.SplitHostPort(raw)
	if err != nil || host == "" || portText == "" {
		return errors.New("endpoint is not host:port")
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.String() != host {
		return errors.New("endpoint address is not canonical")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 0 || port > 65535 {
		return errors.New("endpoint port is out of range")
	}
	return nil
}

// borrowedAuthStagingValidateSocket enforces the mandatory typed socket
// contract: the inode must be a positive canonical decimal uint64 (ParseUint
// round-trips through FormatUint, so empty/zero/signed/garbage/overflow and
// leading-zero aliases are refused); TCP sockets need canonical endpoints and a
// recognized state; unix sockets are only accepted with the producer's actual
// unix proof and no TCP metadata; unresolved kinds are refused (a missing TCP
// table is never "known non-TCP").
func borrowedAuthStagingValidateSocket(socket borrowedAuthStagingCensusSocket) error {
	inode, err := strconv.ParseUint(socket.Inode, 10, 64)
	if err != nil || inode == 0 || strconv.FormatUint(inode, 10) != socket.Inode {
		return errors.New("socket inode is not a positive canonical decimal")
	}
	switch socket.Kind {
	case "tcp":
		if !socket.TCP {
			return errors.New("tcp socket lacks the tcp flag")
		}
		if err := borrowedAuthStagingCanonicalEndpoint(socket.Local); err != nil {
			return fmt.Errorf("tcp local endpoint invalid: %w", err)
		}
		if err := borrowedAuthStagingCanonicalEndpoint(socket.Remote); err != nil {
			return fmt.Errorf("tcp remote endpoint invalid: %w", err)
		}
		if !borrowedAuthStagingTCPStates[socket.State] {
			return errors.New("tcp state is not recognized")
		}
	case "unix":
		if socket.TCP {
			return errors.New("unix socket carries the tcp flag")
		}
		if socket.Local != "" || socket.Remote != "" || socket.State != "" {
			return errors.New("unix socket carries TCP metadata")
		}
	default:
		return errors.New("socket kind is unresolved")
	}
	return nil
}

// parseBorrowedAuthStagingCensus is the strict consumer. It validates the
// expected positive postmaster identity upfront, the same expected postmaster
// before/after (state non-dead when emitted), complete-with-zero-errors, zero
// unknown FDs (top level and per child), exact per-child FD accounting with the
// child socket count bounded by the readable FD count (len(sockets) <=
// fd_readable <= fd_total; readable pipes/files make strict equality wrong),
// unique positive child PIDs directly parented by the postmaster, recognized
// non-dead states, strict gone proofs (enoent or validated pid-reused final
// start with the retained initial identity), the typed socket contract with
// positive canonical decimal inodes, unique child socket inodes and an exact
// top-level socket set (unique inodes, identical metadata, count equal to both
// the summed child socket count and the unique inode count). Any deviation
// refuses.
func parseBorrowedAuthStagingCensus(raw []byte, expectedPID int, expectedStart uint64) (borrowedAuthStagingCensus, error) {
	var report borrowedAuthStagingCensus
	if expectedPID < 2 || expectedStart == 0 {
		return report, errors.New("expected postmaster identity is not positive")
	}
	if err := json.Unmarshal(raw, &report); err != nil {
		return report, errors.New("census report is malformed")
	}
	if !report.Complete {
		return report, errors.New("census report is incomplete")
	}
	if len(report.Errors) != 0 {
		return report, errors.New("census report carries errors despite complete")
	}
	if report.PostmasterPID != expectedPID || report.StartBefore != expectedStart || report.StartAfter != report.StartBefore {
		return report, errors.New("census postmaster identity is inconsistent")
	}
	if report.PostmasterStateBefore != "" && !borrowedAuthStagingLiveState(report.PostmasterStateBefore) {
		return report, errors.New("census postmaster state before is not a recognized live state")
	}
	if report.PostmasterStateAfter != "" && !borrowedAuthStagingLiveState(report.PostmasterStateAfter) {
		return report, errors.New("census postmaster state after is not a recognized live state")
	}
	if report.PostmasterStateBefore != "" && report.PostmasterStateAfter != "" && report.PostmasterStateBefore != report.PostmasterStateAfter {
		return report, errors.New("census postmaster state changed")
	}
	if report.FDUnknown != 0 {
		return report, errors.New("census reports unknown FD entries")
	}
	if report.ChildENOENT < 0 {
		return report, errors.New("census child ENOENT counter is negative")
	}
	childPIDs := make(map[int]bool)
	childSockets := make(map[string]borrowedAuthStagingCensusSocket)
	childSocketTotal := 0
	for _, child := range report.Children {
		if child.PID <= 1 || child.Start == 0 || child.PPID != expectedPID {
			return report, errors.New("census child identity is invalid")
		}
		if childPIDs[child.PID] {
			return report, errors.New("census child PID is duplicated")
		}
		childPIDs[child.PID] = true
		if !borrowedAuthStagingLiveState(child.StateBefore) {
			return report, errors.New("census child state before is not a recognized live state")
		}
		if child.Uncertain {
			return report, errors.New("census child is uncertain")
		}
		if child.FDTotal < 0 || child.FDReadable < 0 || child.FDENOENT < 0 || child.FDUnknown < 0 {
			return report, errors.New("census child FD counters are negative")
		}
		if child.FDUnknown != 0 {
			return report, errors.New("census child has unknown FD entries")
		}
		if child.FDTotal != child.FDReadable+child.FDENOENT+child.FDUnknown {
			return report, errors.New("census child FD accounting is inconsistent")
		}
		if len(child.Sockets) > child.FDReadable || child.FDReadable > child.FDTotal {
			return report, errors.New("census child socket count exceeds readable FDs")
		}
		if child.Gone {
			switch child.GoneReason {
			case "enoent":
				if child.AfterStart != 0 || child.AfterPPID != 0 || child.StateAfter != "" {
					return report, errors.New("census enoent gone child carries final identity")
				}
			case "pid-reused":
				if child.AfterStart == 0 || child.AfterStart == child.Start {
					return report, errors.New("census pid-reused gone child has no validated different final start")
				}
				if !borrowedAuthStagingLiveState(child.StateAfter) {
					return report, errors.New("census pid-reused gone child has no recognized final state")
				}
			default:
				return report, errors.New("census gone child has no strict disappearance proof")
			}
		} else {
			if !borrowedAuthStagingLiveState(child.StateAfter) {
				return report, errors.New("census live child has no recognized non-dead final state")
			}
			if child.AfterStart != 0 && child.AfterStart != child.Start {
				return report, errors.New("census live child changed start without a gone proof")
			}
		}
		for _, socket := range child.Sockets {
			if err := borrowedAuthStagingValidateSocket(socket); err != nil {
				return report, err
			}
			if _, duplicate := childSockets[socket.Inode]; duplicate {
				return report, errors.New("census socket inode is duplicated")
			}
			childSockets[socket.Inode] = socket
		}
		childSocketTotal += len(child.Sockets)
	}
	if len(report.Sockets) != len(childSockets) || len(report.Sockets) != childSocketTotal {
		return report, errors.New("census top-level socket count is inconsistent")
	}
	topInodes := make(map[string]bool)
	for _, top := range report.Sockets {
		if topInodes[top.Inode] {
			return report, errors.New("census top-level socket inode is duplicated")
		}
		topInodes[top.Inode] = true
		matched, ok := childSockets[top.Inode]
		if !ok || matched != top {
			return report, errors.New("census top-level socket does not match a child entry")
		}
	}
	for inode := range childSockets {
		if !topInodes[inode] {
			return report, errors.New("census top-level socket set does not cover every child socket")
		}
	}
	return report, nil
}

// borrowedAuthStagingParseProcStat strictly parses one /proc/<pid>/stat
// document: the leading integer must equal the expected PID, the command field
// must be delimited by the first '(' and the last ')', the field list must be
// sufficient, the state must be kernel-recognized and the start field must be a
// positive numeric identity. Garbage can never prove disappearance.
func borrowedAuthStagingParseProcStat(expectedPID int, raw []byte) (string, uint64, int, error) {
	if expectedPID <= 0 {
		return "", 0, 0, errors.New("expected process PID is invalid")
	}
	text := string(raw)
	open := strings.IndexByte(text, '(')
	if open <= 0 || open > 20 {
		return "", 0, 0, errors.New("stat command delimiter is not recognized")
	}
	parsedPID, err := strconv.Atoi(strings.TrimSpace(text[:open]))
	if err != nil || parsedPID != expectedPID {
		return "", 0, 0, errors.New("stat leading PID does not match")
	}
	last := strings.LastIndexByte(text, ')')
	if last < open {
		return "", 0, 0, errors.New("stat command delimiter is malformed")
	}
	fields := strings.Fields(text[last+1:])
	if len(fields) <= 19 {
		return "", 0, 0, errors.New("stat fields are truncated")
	}
	state := fields[0]
	if !borrowedAuthStagingRecognizedState(state) {
		return "", 0, 0, errors.New("stat state is not recognized")
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return "", 0, 0, errors.New("stat ppid is invalid")
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || start == 0 {
		return "", 0, 0, errors.New("stat start is not a positive identity")
	}
	return state, start, ppid, nil
}

// borrowedAuthStagingTerminalMarkers returns the terminal markers among the
// given output lines; the producer protocol emits at most one.
func borrowedAuthStagingTerminalMarkers(lines []string) []string {
	var markers []string
	for _, line := range lines {
		switch strings.TrimSpace(line) {
		case borrowedAuthStagingTerminalQuit, borrowedAuthStagingTerminalServerEOF, borrowedAuthStagingTerminalUnknown:
			markers = append(markers, strings.TrimSpace(line))
		}
	}
	return markers
}

// borrowedAuthStagingSoleTerminal requires exactly one terminal marker and
// returns it. Zero or multiple terminal markers are refused.
func borrowedAuthStagingSoleTerminal(lines []string) (string, error) {
	markers := borrowedAuthStagingTerminalMarkers(lines)
	if len(markers) != 1 {
		return "", fmt.Errorf("expected exactly one terminal marker, found %d", len(markers))
	}
	return markers[0], nil
}

// TestBorrowedAuthStagingCensusConsumerInvariants proves the strict consumer
// accepts only the exact mandatory schema and refuses every deviation. The
// negative data is synthetic and consumer-only: it can never mint a positive
// capability.
func TestBorrowedAuthStagingCensusConsumerInvariants(t *testing.T) {
	baseline := borrowedAuthStagingCensus{
		Complete: true, PostmasterPID: 7, StartBefore: 100, StartAfter: 100,
		PostmasterStateBefore: "S", PostmasterStateAfter: "S",
		Children: []borrowedAuthStagingCensusChild{{
			PID: 9, Start: 200, PPID: 7, StateBefore: "S", StateAfter: "S",
			FDTotal: 2, FDReadable: 2,
			Sockets: []borrowedAuthStagingCensusSocket{
				{Inode: "1", Kind: "tcp", TCP: true, Local: "127.0.0.1:5432", Remote: "127.0.0.1:5555", State: "01"},
				{Inode: "2", Kind: "unix"},
			},
		}},
		Sockets: []borrowedAuthStagingCensusSocket{
			{Inode: "1", Kind: "tcp", TCP: true, Local: "127.0.0.1:5432", Remote: "127.0.0.1:5555", State: "01"},
			{Inode: "2", Kind: "unix"},
		},
	}
	encode := func(report borrowedAuthStagingCensus) []byte {
		raw, err := json.Marshal(report)
		if err != nil {
			t.Fatalf("marshal census: %v", err)
		}
		return raw
	}
	clone := func() borrowedAuthStagingCensus {
		mutated := baseline
		mutated.Errors = append([]string(nil), baseline.Errors...)
		mutated.Children = append([]borrowedAuthStagingCensusChild(nil), baseline.Children...)
		for index := range mutated.Children {
			mutated.Children[index].Sockets = append([]borrowedAuthStagingCensusSocket(nil), mutated.Children[index].Sockets...)
		}
		mutated.Sockets = append([]borrowedAuthStagingCensusSocket(nil), baseline.Sockets...)
		return mutated
	}
	if _, err := parseBorrowedAuthStagingCensus(encode(baseline), 7, 100); err != nil {
		t.Fatalf("baseline census refused: %v", err)
	}
	// Positive variants: a strict enoent gone child and a validated pid-reused
	// gone child are accepted; a unix socket is accepted only with its proof.
	goneENOENT := clone()
	goneENOENT.Children = append(goneENOENT.Children, borrowedAuthStagingCensusChild{
		PID: 11, Start: 300, PPID: 7, StateBefore: "S", Gone: true, GoneReason: "enoent",
	})
	if _, err := parseBorrowedAuthStagingCensus(encode(goneENOENT), 7, 100); err != nil {
		t.Fatalf("strict enoent gone child refused: %v", err)
	}
	goneReused := clone()
	goneReused.Children = append(goneReused.Children, borrowedAuthStagingCensusChild{
		PID: 12, Start: 400, PPID: 7, StateBefore: "S", Gone: true, GoneReason: "pid-reused",
		AfterStart: 401, AfterPPID: 7, StateAfter: "S",
	})
	if _, err := parseBorrowedAuthStagingCensus(encode(goneReused), 7, 100); err != nil {
		t.Fatalf("strict pid-reused gone child refused: %v", err)
	}
	// Positive variants: the top-level socket list is a set contract, so a
	// reordering of the exact child sockets is accepted; readable non-socket FDs
	// (pipes/files) are legitimate, so fd_readable may exceed the socket count;
	// and a maximal canonical uint64 inode is accepted.
	topReordered := clone()
	topReordered.Sockets[0], topReordered.Sockets[1] = topReordered.Sockets[1], topReordered.Sockets[0]
	if _, err := parseBorrowedAuthStagingCensus(encode(topReordered), 7, 100); err != nil {
		t.Fatalf("reordered top-level socket set refused: %v", err)
	}
	readableSurplus := clone()
	readableSurplus.Children[0].FDTotal = 3
	readableSurplus.Children[0].FDReadable = 3
	if _, err := parseBorrowedAuthStagingCensus(encode(readableSurplus), 7, 100); err != nil {
		t.Fatalf("readable non-socket FDs refused: %v", err)
	}
	maximalInode := clone()
	maximalInode.Children[0].Sockets[1].Inode = "18446744073709551615"
	maximalInode.Sockets[1].Inode = "18446744073709551615"
	if _, err := parseBorrowedAuthStagingCensus(encode(maximalInode), 7, 100); err != nil {
		t.Fatalf("maximal canonical socket inode refused: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*borrowedAuthStagingCensus)
	}{
		{"malformed", nil},
		{"incomplete", func(r *borrowedAuthStagingCensus) { r.Complete = false }},
		{"errors-despite-complete", func(r *borrowedAuthStagingCensus) { r.Errors = []string{"x"} }},
		{"wrong-postmaster-pid", func(r *borrowedAuthStagingCensus) { r.PostmasterPID = 8 }},
		{"unstable-incarnation", func(r *borrowedAuthStagingCensus) { r.StartAfter = 101 }},
		{"dead-postmaster-state-before", func(r *borrowedAuthStagingCensus) { r.PostmasterStateBefore = "Z" }},
		{"dead-postmaster-state-after", func(r *borrowedAuthStagingCensus) { r.PostmasterStateAfter = "X" }},
		{"unrecognized-postmaster-state", func(r *borrowedAuthStagingCensus) { r.PostmasterStateAfter = "?" }},
		{"unknown-fd-total", func(r *borrowedAuthStagingCensus) { r.FDUnknown = 1 }},
		{"negative-enoent", func(r *borrowedAuthStagingCensus) { r.ChildENOENT = -1 }},
		{"count-mismatch", func(r *borrowedAuthStagingCensus) { r.Children[0].FDTotal = 3 }},
		{"negative-count", func(r *borrowedAuthStagingCensus) { r.Children[0].FDReadable = -1 }},
		{"per-child-unknown-fd", func(r *borrowedAuthStagingCensus) {
			r.Children[0].FDUnknown = 1
			r.Children[0].FDTotal = 3
		}},
		{"child-sockets-without-readable-fds", func(r *borrowedAuthStagingCensus) {
			r.Children[0].FDTotal = 0
			r.Children[0].FDReadable = 0
		}},
		{"child-sockets-exceed-readable-fds", func(r *borrowedAuthStagingCensus) {
			r.Children[0].FDTotal = 1
			r.Children[0].FDReadable = 1
		}},
		{"child-pid-nonpositive", func(r *borrowedAuthStagingCensus) { r.Children[0].PID = 1 }},
		{"child-ppid-mismatch", func(r *borrowedAuthStagingCensus) { r.Children[0].PPID = 8 }},
		{"child-start-zero", func(r *borrowedAuthStagingCensus) { r.Children[0].Start = 0 }},
		{"duplicate-child-pid", func(r *borrowedAuthStagingCensus) {
			second := r.Children[0]
			second.Sockets = append([]borrowedAuthStagingCensusSocket(nil), second.Sockets...)
			r.Children = append(r.Children, second)
		}},
		{"state-before-dead", func(r *borrowedAuthStagingCensus) { r.Children[0].StateBefore = "Z" }},
		{"state-after-dead", func(r *borrowedAuthStagingCensus) { r.Children[0].StateAfter = "Z" }},
		{"state-after-unrecognized", func(r *borrowedAuthStagingCensus) { r.Children[0].StateAfter = "?" }},
		{"uncertain-child", func(r *borrowedAuthStagingCensus) { r.Children[0].Uncertain = true }},
		{"missing-kind", func(r *borrowedAuthStagingCensus) { r.Children[0].Sockets[0].Kind = "" }},
		{"unknown-kind", func(r *borrowedAuthStagingCensus) { r.Children[0].Sockets[0].Kind = "unknown" }},
		{"tcp-without-tcp-flag", func(r *borrowedAuthStagingCensus) { r.Children[0].Sockets[0].TCP = false }},
		{"unix-with-tcp-flag", func(r *borrowedAuthStagingCensus) { r.Children[0].Sockets[1].TCP = true }},
		{"unix-with-tcp-metadata", func(r *borrowedAuthStagingCensus) { r.Children[0].Sockets[1].Local = "127.0.0.1:1" }},
		{"missing-inode", func(r *borrowedAuthStagingCensus) { r.Children[0].Sockets[0].Inode = "" }},
		{"inode-zero", func(r *borrowedAuthStagingCensus) {
			r.Children[0].Sockets[1].Inode = "0"
			r.Sockets[1].Inode = "0"
		}},
		{"inode-negative", func(r *borrowedAuthStagingCensus) {
			r.Children[0].Sockets[1].Inode = "-1"
			r.Sockets[1].Inode = "-1"
		}},
		{"inode-garbage", func(r *borrowedAuthStagingCensus) {
			r.Children[0].Sockets[1].Inode = "abc"
			r.Sockets[1].Inode = "abc"
		}},
		{"inode-overflow", func(r *borrowedAuthStagingCensus) {
			r.Children[0].Sockets[1].Inode = "18446744073709551616"
			r.Sockets[1].Inode = "18446744073709551616"
		}},
		{"inode-leading-zero-alias", func(r *borrowedAuthStagingCensus) {
			r.Children[0].Sockets[1].Inode = "01"
			r.Sockets[1].Inode = "01"
		}},
		{"duplicate-inode", func(r *borrowedAuthStagingCensus) {
			r.Children[0].Sockets[1].Inode = r.Children[0].Sockets[0].Inode
		}},
		{"noncanonical-address", func(r *borrowedAuthStagingCensus) { r.Children[0].Sockets[0].Local = "localhost:5432" }},
		{"port-out-of-range", func(r *borrowedAuthStagingCensus) { r.Children[0].Sockets[0].Remote = "127.0.0.1:65536" }},
		{"partial-tcp", func(r *borrowedAuthStagingCensus) { r.Children[0].Sockets[0].Remote = "" }},
		{"unrecognized-tcp-state", func(r *borrowedAuthStagingCensus) { r.Children[0].Sockets[0].State = "0D" }},
		{"top-sockets-count-mismatch", func(r *borrowedAuthStagingCensus) { r.Sockets = r.Sockets[:1] }},
		{"top-socket-mismatch", func(r *borrowedAuthStagingCensus) { r.Sockets[0].Remote = "127.0.0.1:5556" }},
		{"top-duplicate-inode", func(r *borrowedAuthStagingCensus) { r.Sockets[1] = r.Sockets[0] }},
		{"gone-without-reason", func(r *borrowedAuthStagingCensus) {
			r.Children[0].Gone = true
			r.Children[0].GoneReason = ""
		}},
		{"gone-enoent-without-initial-identity", func(r *borrowedAuthStagingCensus) {
			r.Children[0].Gone = true
			r.Children[0].GoneReason = "enoent"
			r.Children[0].Start = 0
		}},
		{"gone-enoent-with-final-identity", func(r *borrowedAuthStagingCensus) {
			r.Children[0].Gone = true
			r.Children[0].GoneReason = "enoent"
			r.Children[0].AfterStart = 201
		}},
		{"gone-reused-without-final-start", func(r *borrowedAuthStagingCensus) {
			r.Children[0].Gone = true
			r.Children[0].GoneReason = "pid-reused"
		}},
		{"gone-reused-same-start", func(r *borrowedAuthStagingCensus) {
			r.Children[0].Gone = true
			r.Children[0].GoneReason = "pid-reused"
			r.Children[0].AfterStart = r.Children[0].Start
		}},
		{"gone-reused-dead-final-state", func(r *borrowedAuthStagingCensus) {
			r.Children[0].Gone = true
			r.Children[0].GoneReason = "pid-reused"
			r.Children[0].AfterStart = 201
			r.Children[0].StateAfter = "Z"
		}},
		{"live-child-with-changed-start", func(r *borrowedAuthStagingCensus) { r.Children[0].AfterStart = 201 }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			raw := []byte("not-json")
			if testCase.mutate != nil {
				mutated := clone()
				testCase.mutate(&mutated)
				raw = encode(mutated)
			}
			if _, err := parseBorrowedAuthStagingCensus(raw, 7, 100); err == nil {
				t.Fatalf("%s was accepted", testCase.name)
			}
		})
	}
	// The expected positive postmaster identity is validated upfront.
	if _, err := parseBorrowedAuthStagingCensus(encode(baseline), 0, 100); err == nil {
		t.Fatal("nonpositive expected postmaster PID was accepted")
	}
	if _, err := parseBorrowedAuthStagingCensus(encode(baseline), 7, 0); err == nil {
		t.Fatal("nonpositive expected postmaster start was accepted")
	}
}

// TestBorrowedAuthStagingPureLifecycleInvariants proves the pure identity and
// terminal helpers: strict stat parsing refuses garbage (never proving
// disappearance), dead states are recognized but not live, canonical endpoints
// are enforced and the terminal markers are exactly classified with a sole
// marker requirement.
func TestBorrowedAuthStagingPureLifecycleInvariants(t *testing.T) {
	valid := "4242 (pg helper (x)) S 7 4242 4242 0 -1 4194304 0 0 0 0 0 0 0 0 20 0 1 0 12345 0 0"
	state, start, ppid, err := borrowedAuthStagingParseProcStat(4242, []byte(valid))
	if err != nil || state != "S" || start != 12345 || ppid != 7 {
		t.Fatalf("valid stat refused: state=%q start=%d ppid=%d err=%v", state, start, ppid, err)
	}
	for _, bad := range []struct {
		name string
		pid  int
		raw  string
	}{
		{"wrong-pid", 4243, valid},
		{"malformed-delimiter", 4242, "4242 pg helper S 7 4242 4242 0 -1 4194304 0 0 0 0 0 0 0 0 20 0 1 0 12345 0 0"},
		{"truncated", 4242, "4242 (pg) S 7 4242 4242 0 -1 1"},
		{"unrecognized-state", 4242, "4242 (pg) ? 7 4242 4242 0 -1 4194304 0 0 0 0 0 0 0 0 20 0 1 0 12345 0 0"},
		{"zero-start", 4242, "4242 (pg) S 7 4242 4242 0 -1 4194304 0 0 0 0 0 0 0 0 20 0 1 0 0 0 0"},
		{"empty", 4242, ""},
	} {
		if _, _, _, err := borrowedAuthStagingParseProcStat(bad.pid, []byte(bad.raw)); err == nil {
			t.Fatalf("bad stat %s was accepted", bad.name)
		}
	}
	if !borrowedAuthStagingLiveState("T") || !borrowedAuthStagingLiveState("S") {
		t.Fatal("recognized live states were refused")
	}
	for _, dead := range []string{"Z", "X", "x", "?", "", "SS"} {
		if borrowedAuthStagingLiveState(dead) {
			t.Fatalf("dead/unrecognized state %q was accepted as live", dead)
		}
	}
	for _, good := range []string{"127.0.0.1:0", "127.0.0.1:65535", "[::1]:5432", "0.0.0.0:1"} {
		if err := borrowedAuthStagingCanonicalEndpoint(good); err != nil {
			t.Fatalf("canonical endpoint %q refused: %v", good, err)
		}
	}
	for _, bad := range []string{"", "localhost:5432", "127.0.0.1", "127.0.0.1:", "127.0.0.1:65536", "127.0.0.1:-1", "127.000.000.001:5432"} {
		if err := borrowedAuthStagingCanonicalEndpoint(bad); err == nil {
			t.Fatalf("noncanonical endpoint %q was accepted", bad)
		}
	}
	lines := []string{"HELPER_STARTED pid=1 start=2", "READY mode=x", borrowedAuthStagingTerminalServerEOF}
	marker, err := borrowedAuthStagingSoleTerminal(lines)
	if err != nil || marker != borrowedAuthStagingTerminalServerEOF {
		t.Fatalf("sole terminal marker: %q err=%v", marker, err)
	}
	for _, bad := range [][]string{
		{"READY mode=x"},
		{borrowedAuthStagingTerminalQuit, borrowedAuthStagingTerminalServerEOF},
		{borrowedAuthStagingTerminalUnknown, borrowedAuthStagingTerminalQuit},
	} {
		if _, err := borrowedAuthStagingSoleTerminal(bad); err == nil {
			t.Fatalf("non-sole terminal lines were accepted: %v", bad)
		}
	}
}
