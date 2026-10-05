//go:build ignore

// borrowed-auth-staging-client_linux_testhelper.go is the standalone static
// staging client/producer for the borrowed-auth prerequisite lane. It is
// compiled by the Linux drill test and copied only into its disposable
// PostgreSQL container. Modes:
//
//	prestartup-hold <role> <database> <serverIP> <port>
//	scram-hold      <role> <database> <serverIP> <port>
//	census          <postmasterPID> <postmasterStart>
//
// The W secret is read exclusively from the private stdin first line and is
// never an argument or environment value. Public stdout carries only the
// immutable HELPER_STARTED/READY phase markers, the SASLContinue verifier
// shape, and exactly one terminal marker: CLIENT_QUIT (managed stdin quit),
// CLIENT_SERVER_EOF (genuine server io.EOF) or CLIENT_UNKNOWN (any other read
// error or an explicit stdin end after READY, safe constant reason, nonzero
// exit). Exactly one completion owner decides the terminal, emits that one
// marker and completes the process; main is the single exit point. No password,
// DSN or raw network error text is emitted. The census mode is a strict
// read-only producer: immutable report data, no live capability and no
// os.Process return.
package main

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const recognizedProcStates = "RSDTZtXxKWPI"

var recognizedTCPStates = map[string]bool{
	"01": true, "02": true, "03": true, "04": true, "05": true, "06": true,
	"07": true, "08": true, "09": true, "0A": true, "0B": true, "0C": true,
}

func main() {
	if len(os.Args) < 2 {
		fail("usage")
	}
	switch os.Args[1] {
	case "prestartup-hold", "scram-hold":
		if len(os.Args) != 6 {
			fail("hold-args")
		}
		pid, start := processStart()
		fmt.Fprintf(os.Stdout, "HELPER_STARTED pid=%d start=%d\n", pid, start)
		os.Exit(runHold(os.Args[2:], os.Args[1] == "scram-hold"))
	case "census":
		runCensus(os.Args[2:])
	default:
		fail("unknown mode")
	}
}

func fail(reason string) {
	fmt.Fprintf(os.Stdout, "HELPER_ERROR %s\n", reason)
	os.Exit(2)
}

func processStart() (int, uint64) {
	pid := os.Getpid()
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		fail("self-stat-unreadable")
	}
	state, start, _, err := parseProcStat(pid, raw)
	if err != nil || state == "Z" || state == "X" || state == "x" {
		fail("self-stat-invalid")
	}
	return pid, start
}

// terminalCoordinator is the single completion owner of one staging helper
// process. Exactly one caller wins the terminal decision and that same winner
// emits the exactly-one terminal marker and publishes process completion, so a
// marker can never be lost between decision and process exit. A managed QUIT
// suppresses any later read outcome, and a genuine server EOF that linearizes
// first wins over a later managed quit. No goroutine calls os.Exit: main is the
// only process exit point and it exits only after the owner completed.
type terminalCoordinator struct {
	mu      sync.Mutex
	decided bool
	managed bool
	code    int
	out     io.Writer
	done    chan struct{}
}

func newTerminalCoordinator(out io.Writer) *terminalCoordinator {
	return &terminalCoordinator{out: out, done: make(chan struct{})}
}

// resolve claims the terminal outcome exactly once. The first caller that
// linearizes owns the decision, the marker emission and the completion: it
// writes exactly one marker through the private output and then closes done.
// Every loser observes false and must wait for the owner's completion instead
// of exiting on its own; it can never emit a second marker or complete the
// process first. The marker is written before completion is published, so the
// single main exit point can never overtake the winner's emission.
func (t *terminalCoordinator) resolve(marker string, code int) bool {
	t.mu.Lock()
	if t.decided {
		t.mu.Unlock()
		return false
	}
	t.decided = true
	t.managed = marker == "CLIENT_QUIT"
	t.code = code
	t.mu.Unlock()
	out := t.out
	if out == nil {
		out = os.Stdout
	}
	fmt.Fprintln(out, marker)
	close(t.done)
	return true
}

func (t *terminalCoordinator) isManaged() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.managed
}

// exitCode returns the winning terminal exit code. It is only meaningful after
// done is closed; before that it is the zero value and not a decision.
func (t *terminalCoordinator) exitCode() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.code
}

// classifyReadTerminal is the pure terminal classification for a server read
// error: a managed local termination suppresses every marker; a genuine io.EOF
// is CLIENT_SERVER_EOF with exit 0; any other read error (timeout, reset,
// local close) is CLIENT_UNKNOWN with a nonzero exit. It never contains raw
// error text.
func classifyReadTerminal(err error, managed bool) (string, int) {
	if managed {
		return "", 0
	}
	if errors.Is(err, io.EOF) {
		return "CLIENT_SERVER_EOF", 0
	}
	return "CLIENT_UNKNOWN", 3
}

func runHold(args []string, scram bool) int {
	role, database, host, port := args[0], args[1], args[2], args[3]
	secret := ""
	stdin := bufio.NewReader(os.Stdin)
	if line, err := stdin.ReadString('\n'); err == nil {
		secret = strings.TrimRight(line, "\r\n")
	} else if err != io.EOF {
		fail("stdin-secret")
	}
	conn, err := net.Dial("tcp", net.JoinHostPort(host, port))
	if err != nil {
		fail("dial-failed")
	}
	defer conn.Close()
	pid, start := processStart()
	fmt.Fprintf(os.Stdout, "READY mode=%s pid=%d start=%d local=%s remote=%s\n",
		map[bool]string{false: "prestartup-hold", true: "scram-hold"}[scram],
		pid, start, conn.LocalAddr().String(), conn.RemoteAddr().String())

	terminal := newTerminalCoordinator(os.Stdout)
	if scram {
		if err := scramUpToContinue(conn, role, database, secret); err != nil {
			fmt.Fprintf(os.Stdout, "SCRAM_FAILED %s\n", err.Error())
		}
	}
	// The server-EOF reader starts only after the SCRAM hold point, so it can
	// never race the auth reads.
	go readerLoop(conn, terminal)
	go stdinLoop(stdin, terminal)
	// Main waits for the single terminal owner to complete: a losing managed
	// QUIT and an explicit stdin end after an already-owned terminal wait here
	// instead of returning main early without a marker.
	<-terminal.done
	if terminal.isManaged() {
		// Preserve the managed QUIT decision before the local close: the reader
		// may only observe a suppressed local-close error afterwards.
		_ = conn.Close()
	}
	return terminal.exitCode()
}

// readerLoop is the actual server-reader path of the helper. It starts only
// after the SCRAM hold point (or immediately in the prestartup hold, where no
// auth read happens), so it can never race the auth reads. A genuine io.EOF
// resolves CLIENT_SERVER_EOF with exit 0; any other read error resolves
// CLIENT_UNKNOWN with the safe constant reason and a nonzero exit; a managed
// termination suppresses the read outcome and can never emit a second marker.
func readerLoop(conn net.Conn, terminal *terminalCoordinator) {
	buf := make([]byte, 4096)
	for {
		if _, err := conn.Read(buf); err != nil {
			marker, code := classifyReadTerminal(err, terminal.isManaged())
			if marker != "" {
				terminal.resolve(marker, code)
			}
			return
		}
	}
}

// stdinLoop is the actual private-stdin path after the secret line. A managed
// quit line resolves CLIENT_QUIT with exit 0 before any local connection close;
// an explicit stdin end (EOF or a local read failure) resolves CLIENT_UNKNOWN
// with the safe constant nonzero exit unless another terminal owner already
// completed, so it can never be a silent exit 0 and can never replace the
// winner's marker or exit code.
func stdinLoop(stdin *bufio.Reader, terminal *terminalCoordinator) {
	for {
		line, err := stdin.ReadString('\n')
		if err != nil {
			terminal.resolve("CLIENT_UNKNOWN", 3)
			return
		}
		if strings.TrimSpace(line) == "quit" {
			terminal.resolve("CLIENT_QUIT", 0)
			return
		}
	}
}

func scramUpToContinue(conn net.Conn, role, database, secret string) error {
	_ = secret // hold point needs no proof; the secret stays private
	startup := make([]byte, 0, 64)
	startup = append(startup, 0, 0, 0, 0)
	startup = binary.BigEndian.AppendUint32(startup, 196608)
	startup = append(startup, []byte("user\x00"+role+"\x00database\x00"+database+"\x00\x00")...)
	binary.BigEndian.PutUint32(startup[:4], uint32(len(startup)))
	if _, err := conn.Write(startup); err != nil {
		return fmt.Errorf("startup-write")
	}
	nonceRaw := make([]byte, 18)
	if _, err := rand.Read(nonceRaw); err != nil {
		return fmt.Errorf("nonce")
	}
	clientFirst := "n,,n=,r=" + hex.EncodeToString(nonceRaw)
	initial := make([]byte, 0, 128)
	initial = append(initial, 'p', 0, 0, 0, 0)
	initial = append(initial, []byte("SCRAM-SHA-256\x00")...)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(clientFirst)))
	initial = append(initial, lenBuf[:]...)
	initial = append(initial, []byte(clientFirst)...)
	binary.BigEndian.PutUint32(initial[1:5], uint32(len(initial)-1))
	if _, err := conn.Write(initial); err != nil {
		return fmt.Errorf("sasl-initial-write")
	}
	for {
		header := make([]byte, 5)
		if _, err := io.ReadFull(conn, header); err != nil {
			return fmt.Errorf("auth-read")
		}
		length := int(binary.BigEndian.Uint32(header[1:5]))
		if length < 4 || length > 1<<20 {
			return fmt.Errorf("auth-length")
		}
		body := make([]byte, length-4)
		if _, err := io.ReadFull(conn, body); err != nil {
			return fmt.Errorf("auth-body")
		}
		switch header[0] {
		case 'R':
			if len(body) < 4 {
				return fmt.Errorf("auth-code")
			}
			code := binary.BigEndian.Uint32(body[:4])
			switch code {
			case 10:
				continue
			case 11:
				iterations, saltLen, nonceLen := parseServerFirst(string(body[4:]))
				fmt.Fprintf(os.Stdout, "SASL_CONTINUE iterations=%d salt_len=%d nonce_len=%d\n", iterations, saltLen, nonceLen)
				return nil
			case 0:
				fmt.Fprintln(os.Stdout, "AUTH_OK_UNEXPECTED")
				return nil
			default:
				return fmt.Errorf("auth-code-%d", code)
			}
		case 'E':
			return fmt.Errorf("server-error")
		default:
			continue
		}
	}
}

func parseServerFirst(text string) (iterations, saltLen, nonceLen int) {
	for _, part := range strings.Split(text, ",") {
		switch {
		case strings.HasPrefix(part, "i="):
			iterations, _ = strconv.Atoi(strings.TrimPrefix(part, "i="))
		case strings.HasPrefix(part, "s="):
			saltLen = len(strings.TrimPrefix(part, "s="))
		case strings.HasPrefix(part, "r="):
			nonceLen = len(strings.TrimPrefix(part, "r="))
		}
	}
	return iterations, saltLen, nonceLen
}

// --- strict census producer ---

type censusSocket struct {
	Inode  string `json:"inode"`
	Kind   string `json:"kind"`
	TCP    bool   `json:"tcp"`
	Local  string `json:"local,omitempty"`
	Remote string `json:"remote,omitempty"`
	State  string `json:"state,omitempty"`
}

type censusChild struct {
	PID         int            `json:"pid"`
	Start       uint64         `json:"start"`
	PPID        int            `json:"ppid"`
	StateBefore string         `json:"state_before"`
	StateAfter  string         `json:"state_after,omitempty"`
	Gone        bool           `json:"gone"`
	GoneReason  string         `json:"gone_reason,omitempty"`
	AfterStart  uint64         `json:"after_start,omitempty"`
	AfterPPID   int            `json:"after_ppid,omitempty"`
	Uncertain   bool           `json:"uncertain,omitempty"`
	FDTotal     int            `json:"fd_total"`
	FDReadable  int            `json:"fd_readable"`
	FDENOENT    int            `json:"fd_enoent"`
	FDUnknown   int            `json:"fd_unknown"`
	Sockets     []censusSocket `json:"sockets"`
}

type censusReport struct {
	Complete      bool           `json:"complete"`
	Errors        []string       `json:"errors"`
	PostmasterPID int            `json:"postmaster_pid"`
	StartBefore   uint64         `json:"start_before"`
	StartAfter    uint64         `json:"start_after"`
	ChildENOENT   int            `json:"child_enoent"`
	FDUnknown     int            `json:"fd_unknown_total"`
	Sockets       []censusSocket `json:"sockets"`
	Children      []censusChild  `json:"children"`
}

// parseProcStat strictly parses one /proc/<pid>/stat document: the leading
// integer must equal the expected PID, the command field must be delimited by
// the first '(' and the last ')', the field list must be sufficient, the state
// must be a kernel-recognized single character and the start field must be a
// valid positive numeric identity. Garbage can never prove disappearance.
func parseProcStat(expectedPID int, raw []byte) (string, uint64, int, error) {
	if expectedPID <= 0 {
		return "", 0, 0, fmt.Errorf("expected-pid")
	}
	text := string(raw)
	open := strings.IndexByte(text, '(')
	if open <= 0 || open > 20 {
		return "", 0, 0, fmt.Errorf("delimiter")
	}
	parsedPID, err := strconv.Atoi(strings.TrimSpace(text[:open]))
	if err != nil || parsedPID != expectedPID {
		return "", 0, 0, fmt.Errorf("pid")
	}
	last := strings.LastIndexByte(text, ')')
	if last < open {
		return "", 0, 0, fmt.Errorf("delimiter")
	}
	fields := strings.Fields(text[last+1:])
	if len(fields) <= 19 {
		return "", 0, 0, fmt.Errorf("truncated")
	}
	state := fields[0]
	if len(state) != 1 || !strings.ContainsRune(recognizedProcStates, rune(state[0])) {
		return "", 0, 0, fmt.Errorf("state")
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return "", 0, 0, fmt.Errorf("ppid")
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || start == 0 {
		return "", 0, 0, fmt.Errorf("start")
	}
	return state, start, ppid, nil
}

func readProcStat(pid int) (string, uint64, int, error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", 0, 0, err
	}
	return parseProcStat(pid, raw)
}

func runCensus(args []string) {
	if len(args) != 2 {
		fail("census-args")
	}
	pmPID, err := strconv.Atoi(args[0])
	if err != nil || pmPID < 2 {
		fail("census-postmaster-pid")
	}
	pmStart, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil || pmStart == 0 {
		fail("census-postmaster-start")
	}
	report := censusReport{Errors: []string{}, PostmasterPID: pmPID}
	pmState, pmStartObserved, pmPPID, err := readProcStat(pmPID)
	if err != nil || pmStartObserved != pmStart {
		fail("census-postmaster-identity")
	}
	if pmState == "Z" || pmState == "X" || pmState == "x" {
		fail("census-postmaster-not-live")
	}
	report.StartBefore = pmStartObserved

	childrenRaw, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%d/children", pmPID, pmPID))
	if err != nil {
		fail("census-children-unreadable")
	}
	table, err := readTCPTables()
	if err != nil {
		fail("census-net-table")
	}
	unixInodes, err := readUnixInodes()
	if err != nil {
		fail("census-unix-table")
	}
	var childPIDs []int
	for _, field := range strings.Fields(string(childrenRaw)) {
		pid, err := strconv.Atoi(field)
		if err != nil || pid < 2 {
			fail("census-child-pid")
		}
		childPIDs = append(childPIDs, pid)
	}
	sort.Ints(childPIDs)
	for _, pid := range childPIDs {
		child := censusChild{PID: pid}
		beforeState, beforeStart, beforePPID, err := readProcStat(pid)
		if err != nil {
			if os.IsNotExist(err) {
				// The initial identity was never captured: this cannot prove a
				// known disappearance.
				child.Uncertain = true
				report.ChildENOENT++
				report.Errors = append(report.Errors, "child-initial-identity-unavailable")
				report.Children = append(report.Children, child)
				continue
			}
			report.Errors = append(report.Errors, "child-stat-unreadable")
			continue
		}
		if beforePPID != pmPID {
			report.Errors = append(report.Errors, "child-ppid-mismatch")
			continue
		}
		child.Start, child.PPID, child.StateBefore = beforeStart, beforePPID, beforeState
		entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
		if err != nil {
			if os.IsNotExist(err) {
				// FD dir gone is not proof by itself: the retained identity must
				// be re-proven as ENOENT or a validated different start.
				afterState, afterStart, afterPPID, afterErr := readProcStat(pid)
				switch {
				case afterErr != nil && os.IsNotExist(afterErr):
					child.Gone, child.GoneReason = true, "enoent"
				case afterErr == nil && afterStart != beforeStart:
					child.Gone, child.GoneReason = true, "pid-reused"
					child.AfterStart, child.AfterPPID, child.StateAfter = afterStart, afterPPID, afterState
				case afterErr == nil:
					child.Uncertain = true
					report.Errors = append(report.Errors, "child-fd-dir-gone-but-process-live")
				default:
					child.Uncertain = true
					report.Errors = append(report.Errors, "child-final-stat-unreadable")
				}
				report.Children = append(report.Children, child)
				continue
			}
			report.Errors = append(report.Errors, "child-fd-table-unreadable")
			continue
		}
		child.FDTotal = len(entries)
		var socketInodes []string
		for _, entry := range entries {
			target, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(pid), "fd", entry.Name()))
			if err != nil {
				if os.IsNotExist(err) {
					child.FDENOENT++
					continue
				}
				child.FDUnknown++
				report.FDUnknown++
				continue
			}
			child.FDReadable++
			if index := strings.Index(target, "socket:["); index >= 0 {
				inode := strings.TrimSuffix(target[index+len("socket:["):], "]")
				if inode == "" {
					child.FDUnknown++
					report.FDUnknown++
					continue
				}
				socketInodes = append(socketInodes, inode)
			}
		}
		for _, inode := range socketInodes {
			socket := censusSocket{Inode: inode}
			switch {
			case table[inode].known:
				row := table[inode]
				socket.Kind, socket.TCP = "tcp", true
				socket.Local, socket.Remote, socket.State = row.local, row.remote, row.state
			case unixInodes[inode]:
				socket.Kind = "unix"
			default:
				socket.Kind = "unknown"
				report.Errors = append(report.Errors, "socket-kind-unresolved")
			}
			child.Sockets = append(child.Sockets, socket)
			report.Sockets = append(report.Sockets, socket)
		}
		afterState, afterStart, afterPPID, afterErr := readProcStat(pid)
		switch {
		case afterErr == nil && afterStart == beforeStart && afterPPID == beforePPID:
			child.StateAfter = afterState
		case afterErr == nil && afterStart != beforeStart:
			child.Gone, child.GoneReason = true, "pid-reused"
			child.AfterStart, child.AfterPPID, child.StateAfter = afterStart, afterPPID, afterState
		case afterErr != nil && os.IsNotExist(afterErr):
			child.Gone, child.GoneReason = true, "enoent"
		default:
			child.Uncertain = true
			report.Errors = append(report.Errors, "child-final-stat-uncertain")
		}
		if child.FDTotal != child.FDReadable+child.FDENOENT+child.FDUnknown {
			report.Errors = append(report.Errors, "child-fd-accounting")
		}
		report.Children = append(report.Children, child)
	}
	afterState, afterStart, afterPPID, err := readProcStat(pmPID)
	if err != nil {
		report.Errors = append(report.Errors, "postmaster-stat-unreadable-after")
	} else {
		if afterStart != pmStartObserved {
			report.Errors = append(report.Errors, "postmaster-start-changed")
		}
		if afterPPID != pmPPID {
			report.Errors = append(report.Errors, "postmaster-parent-changed")
		}
		if afterState == "Z" || afterState == "X" || afterState == "x" {
			report.Errors = append(report.Errors, "postmaster-not-live-after")
		}
		report.StartAfter = afterStart
	}
	report.Complete = len(report.Errors) == 0 && report.FDUnknown == 0
	encoded, err := json.Marshal(report)
	if err != nil {
		fail("census-marshal")
	}
	fmt.Fprintln(os.Stdout, string(encoded))
}

type tcpRow struct {
	known  bool
	local  string
	remote string
	state  string
}

func readTCPTables() (map[string]tcpRow, error) {
	table := make(map[string]tcpRow)
	for _, path := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
		for _, line := range lines[1:] {
			if strings.TrimSpace(line) == "" {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 10 {
				return nil, fmt.Errorf("net-line")
			}
			local, err := decodeTCPEndpoint(fields[1])
			if err != nil {
				return nil, err
			}
			remote, err := decodeTCPEndpoint(fields[2])
			if err != nil {
				return nil, err
			}
			if !recognizedTCPStates[fields[3]] {
				return nil, fmt.Errorf("net-state")
			}
			table[fields[9]] = tcpRow{known: true, local: local, remote: remote, state: fields[3]}
		}
	}
	return table, nil
}

func readUnixInodes() (map[string]bool, error) {
	raw, err := os.ReadFile("/proc/net/unix")
	if err != nil {
		return nil, err
	}
	inodes := make(map[string]bool)
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	for _, line := range lines[1:] {
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 7 {
			return nil, fmt.Errorf("unix-line")
		}
		inodes[fields[6]] = true
	}
	return inodes, nil
}

// decodeTCPEndpoint decodes the kernel /proc/net/tcp hex endpoint form into a
// canonical ip:port string. IPv4 is little-endian; IPv6 stores four
// little-endian 32-bit words and is swapped per word. Ports must be 0..65535
// and addresses must parse canonically.
func decodeTCPEndpoint(raw string) (string, error) {
	parts := strings.Split(raw, ":")
	if len(parts) != 2 {
		return "", fmt.Errorf("endpoint")
	}
	port, err := strconv.ParseInt(parts[1], 16, 32)
	if err != nil || port < 0 || port > 65535 {
		return "", fmt.Errorf("port")
	}
	addressBytes, err := hex.DecodeString(parts[0])
	if err != nil {
		return "", fmt.Errorf("address")
	}
	var ip net.IP
	switch len(addressBytes) {
	case 4:
		ip = net.IPv4(addressBytes[3], addressBytes[2], addressBytes[1], addressBytes[0])
	case 16:
		canonical := make([]byte, 16)
		for word := 0; word < 4; word++ {
			canonical[word*4+0] = addressBytes[word*4+3]
			canonical[word*4+1] = addressBytes[word*4+2]
			canonical[word*4+2] = addressBytes[word*4+1]
			canonical[word*4+3] = addressBytes[word*4+0]
		}
		ip = net.IP(canonical)
	default:
		return "", fmt.Errorf("width")
	}
	if ip == nil || ip.String() == "<nil>" {
		return "", fmt.Errorf("address")
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	return net.JoinHostPort(ip.String(), strconv.Itoa(int(port))), nil
}
