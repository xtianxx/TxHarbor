//go:build linux && drill

// borrowed-auth-staging-producer_linux_test.go is the producer-only unit lane:
// it compiles the staging helper source together with generated package-main
// tests in a temporary directory (explicit-file go test, ignore-tag safe) and
// exercises the strict stat parser, the pure terminal classification, the
// canonical TCP/tcp6 endpoint decoder and the single terminal completion owner
// (decision + marker emission + process completion) with deterministic
// decision/emission barriers. It then spawns the separately built static helper
// and proves the real stdout markers and process exit codes. All injected
// negatives are labeled as such; no real-permission claim is made and no
// fixture/PG process is touched.
package recovery_test

import (
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const generatedProducerUnitTests = `package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProducerUnitStatParser(t *testing.T) {
	valid := []byte("4242 (pg restore (x)) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 99")
	state, start, ppid, err := parseProcStat(4242, valid)
	if err != nil || state != "S" || start != 99 || ppid != 0 {
		t.Fatalf("valid stat refused: state=%q start=%d ppid=%d err=%v", state, start, ppid, err)
	}
	if _, _, _, err := parseProcStat(1, valid); err == nil {
		t.Fatal("wrong leading PID was accepted")
	}
	if _, _, _, err := parseProcStat(4242, []byte(strings.Replace(string(valid), " S ", " ? ", 1))); err == nil {
		t.Fatal("unrecognized state was accepted")
	}
	for _, bad := range []string{"abc", "0", "999999999999999999999999"} {
		raw := strings.Replace(string(valid), " 99", " "+bad, 1)
		if _, _, _, err := parseProcStat(4242, []byte(raw)); err == nil {
			t.Fatalf("garbage start %q was accepted", bad)
		}
	}
	if _, _, _, err := parseProcStat(4242, []byte("4242 (pg) S 1 2 3")); err == nil {
		t.Fatal("truncated stat was accepted")
	}
	if _, _, _, err := parseProcStat(4242, []byte("4242 pg_restore S 1 2 3")); err == nil {
		t.Fatal("malformed command delimiter was accepted")
	}
	if _, _, _, err := parseProcStat(0, valid); err == nil {
		t.Fatal("zero expected PID was accepted")
	}
}

func TestProducerUnitTerminalClassification(t *testing.T) {
	if marker, code := classifyReadTerminal(io.EOF, false); marker != "CLIENT_SERVER_EOF" || code != 0 {
		t.Fatalf("genuine EOF classification: %q/%d", marker, code)
	}
	if marker, code := classifyReadTerminal(errors.New("connection reset"), false); marker != "CLIENT_UNKNOWN" || code == 0 {
		t.Fatalf("network error classification: %q/%d", marker, code)
	}
	if marker, code := classifyReadTerminal(io.EOF, true); marker != "" || code != 0 {
		t.Fatalf("managed EOF must be suppressed: %q/%d", marker, code)
	}
	if marker, code := classifyReadTerminal(errors.New("timeout"), true); marker != "" || code != 0 {
		t.Fatalf("managed error must be suppressed: %q/%d", marker, code)
	}
}

func TestProducerUnitTCPDecoder(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{"0100007F:1F90", "127.0.0.1:8080"},
		{"00000000000000000000000001000000:1538", "[::1]:5432"},
		{"0000000000000000ffff00000100007f:1F90", "127.0.0.1:8080"},
	}
	for _, testCase := range cases {
		got, err := decodeTCPEndpoint(testCase.raw)
		if err != nil || got != testCase.want {
			t.Fatalf("decode %q = %q err=%v, want %q", testCase.raw, got, err, testCase.want)
		}
	}
	for _, bad := range []string{"0100007F:10000", "zz:1F90", "0100007F", "0100007F:1F90:00"} {
		if _, err := decodeTCPEndpoint(bad); err == nil {
			t.Fatalf("malformed endpoint %q was accepted", bad)
		}
	}
	if !recognizedTCPStates["01"] || recognizedTCPStates["99"] {
		t.Fatal("recognized TCP state set is wrong")
	}
}

var unitTerminalMarkers = map[string]bool{
	"CLIENT_QUIT": true, "CLIENT_SERVER_EOF": true, "CLIENT_UNKNOWN": true,
}

func unitMarkersIn(text string) []string {
	var markers []string
	for _, line := range strings.Split(text, "\n") {
		if unitTerminalMarkers[strings.TrimSpace(line)] {
			markers = append(markers, strings.TrimSpace(line))
		}
	}
	return markers
}

// unitTerminalOutput is the unit-only decision/emission barrier: the terminal
// owner notifies that it decided and reached emission, then blocks before any
// marker byte is retained, so the lane can deterministically inject a losing
// competing terminal. It is a notification barrier only: it never supplies,
// changes or fabricates a terminal, EOF or auth decision.
type unitTerminalOutput struct {
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	buf     bytes.Buffer
}

func newUnitTerminalOutput() *unitTerminalOutput {
	return &unitTerminalOutput{started: make(chan struct{}, 16), release: make(chan struct{})}
}

func (o *unitTerminalOutput) Write(p []byte) (int, error) {
	select {
	case o.started <- struct{}{}:
	default:
	}
	<-o.release
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.Write(p)
}

func (o *unitTerminalOutput) text() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.String()
}

func (o *unitTerminalOutput) markers() []string {
	return unitMarkersIn(o.text())
}

func unitAwaitBoundary(t *testing.T, out *unitTerminalOutput) {
	t.Helper()
	select {
	case <-out.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the terminal owner never reached the decision/emission boundary")
	}
}

func unitAssertNotCompleted(t *testing.T, terminal *terminalCoordinator, stage string) {
	t.Helper()
	select {
	case <-terminal.done:
		t.Fatalf("the process completed at %s before the owner emitted its marker", stage)
	default:
	}
}

func unitAwaitCompletion(t *testing.T, terminal *terminalCoordinator) {
	t.Helper()
	select {
	case <-terminal.done:
	case <-time.After(10 * time.Second):
		t.Fatal("the terminal owner never published process completion")
	}
}

func unitSoleMarker(t *testing.T, out *unitTerminalOutput) string {
	t.Helper()
	markers := out.markers()
	if len(markers) != 1 {
		t.Fatalf("terminal markers are not exactly one: %v", markers)
	}
	return markers[0]
}

// TestProducerUnitTerminalEOFBeatsQuit deterministically pauses the actual
// reader after it decided a genuine in-process io.EOF and before its marker was
// emitted, injects the actual managed stdin QUIT path as a loser, and proves
// the EOF owner still emits exactly one CLIENT_SERVER_EOF and completes with
// exit 0 while the losing QUIT can never complete the process or emit.
func TestProducerUnitTerminalEOFBeatsQuit(t *testing.T) {
	out := newUnitTerminalOutput()
	terminal := newTerminalCoordinator(out)
	server, client := net.Pipe()
	defer server.Close()
	_ = client.Close() // the in-process reader observes a genuine io.EOF
	readerDone := make(chan struct{})
	go func() {
		readerLoop(server, terminal)
		close(readerDone)
	}()
	unitAwaitBoundary(t, out)
	unitAssertNotCompleted(t, terminal, "the reader decision")
	if terminal.isManaged() {
		t.Fatal("a server EOF must never be classified as a managed termination")
	}
	stdinLoop(bufio.NewReader(strings.NewReader("quit\n")), terminal) // actual losing QUIT path
	unitAssertNotCompleted(t, terminal, "the losing managed QUIT")
	close(out.release)
	select {
	case <-readerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the winning reader did not complete after emission")
	}
	unitAwaitCompletion(t, terminal)
	if marker := unitSoleMarker(t, out); marker != "CLIENT_SERVER_EOF" {
		t.Fatalf("winning marker is not the genuine server EOF: %q", marker)
	}
	if terminal.isManaged() || terminal.exitCode() != 0 {
		t.Fatalf("EOF winner state: managed=%v code=%d, want managed=false code=0", terminal.isManaged(), terminal.exitCode())
	}
}

// TestProducerUnitTerminalQuitBeatsEOF deterministically pauses the actual
// managed QUIT owner before emission, then runs the actual reader path over a
// genuine in-process io.EOF as the loser: exactly one CLIENT_QUIT, no server
// EOF, managed state preserved and exit 0.
func TestProducerUnitTerminalQuitBeatsEOF(t *testing.T) {
	out := newUnitTerminalOutput()
	terminal := newTerminalCoordinator(out)
	quitDone := make(chan struct{})
	go func() {
		stdinLoop(bufio.NewReader(strings.NewReader("quit\n")), terminal)
		close(quitDone)
	}()
	unitAwaitBoundary(t, out)
	unitAssertNotCompleted(t, terminal, "the managed QUIT decision")
	if !terminal.isManaged() {
		t.Fatal("the managed QUIT decision did not claim the managed state")
	}
	server, client := net.Pipe()
	defer server.Close()
	_ = client.Close() // a genuine EOF that loses to the owned managed terminal
	readerDone := make(chan struct{})
	go func() {
		readerLoop(server, terminal)
		close(readerDone)
	}()
	select {
	case <-readerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the losing reader never observed the managed suppression")
	}
	unitAssertNotCompleted(t, terminal, "the losing reader EOF")
	close(out.release)
	select {
	case <-quitDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the managed QUIT owner did not complete")
	}
	unitAwaitCompletion(t, terminal)
	if marker := unitSoleMarker(t, out); marker != "CLIENT_QUIT" {
		t.Fatalf("winning marker is not the managed QUIT: %q", marker)
	}
	if !terminal.isManaged() || terminal.exitCode() != 0 {
		t.Fatalf("QUIT winner state: managed=%v code=%d, want managed=true code=0", terminal.isManaged(), terminal.exitCode())
	}
}

// unitErrorConn is an injected unit-only read error source. It exercises the
// non-EOF classification path only and is explicitly NOT an auth or server-EOF
// proof; real server EOF evidence belongs to the owned harness lane.
type unitErrorConn struct{ net.Conn }

func (c unitErrorConn) Read([]byte) (int, error) {
	return 0, errors.New("injected unit read error")
}

// TestProducerUnitTerminalUnknownBeatsQuit pauses the actual reader after it
// decided an injected non-EOF read error, injects the actual managed QUIT path
// as a loser, and proves exactly one CLIENT_UNKNOWN with a nonzero exit and no
// QUIT marker: a losing QUIT can never convert an UNKNOWN terminal into a
// silent exit 0.
func TestProducerUnitTerminalUnknownBeatsQuit(t *testing.T) {
	out := newUnitTerminalOutput()
	terminal := newTerminalCoordinator(out)
	readerDone := make(chan struct{})
	go func() {
		readerLoop(unitErrorConn{}, terminal)
		close(readerDone)
	}()
	unitAwaitBoundary(t, out)
	unitAssertNotCompleted(t, terminal, "the injected read error decision")
	if terminal.isManaged() {
		t.Fatal("an injected read error must not be a managed termination")
	}
	stdinLoop(bufio.NewReader(strings.NewReader("quit\n")), terminal) // actual losing QUIT path
	unitAssertNotCompleted(t, terminal, "the losing managed QUIT")
	close(out.release)
	select {
	case <-readerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the winning reader did not complete after emission")
	}
	unitAwaitCompletion(t, terminal)
	if marker := unitSoleMarker(t, out); marker != "CLIENT_UNKNOWN" {
		t.Fatalf("winning marker is not the safe UNKNOWN terminal: %q", marker)
	}
	if terminal.isManaged() || terminal.exitCode() == 0 {
		t.Fatalf("UNKNOWN winner must stay non-managed and nonzero: managed=%v code=%d", terminal.isManaged(), terminal.exitCode())
	}
}

// TestProducerUnitTerminalStdinEnd proves the explicit stdin end after READY
// semantics on the actual stdin path: a quit line owns CLIENT_QUIT exit 0, a
// bare stdin end with no owner owns CLIENT_UNKNOWN nonzero (never a silent exit
// 0), and a stdin end after an owned terminal can neither replace the winner's
// marker nor its exit code.
func TestProducerUnitTerminalStdinEnd(t *testing.T) {
	quitBuf := &bytes.Buffer{}
	quitTerminal := newTerminalCoordinator(quitBuf)
	stdinLoop(bufio.NewReader(strings.NewReader("quit\n")), quitTerminal)
	unitAwaitCompletion(t, quitTerminal)
	if markers := unitMarkersIn(quitBuf.String()); len(markers) != 1 || markers[0] != "CLIENT_QUIT" {
		t.Fatalf("managed quit line markers = %v, want exactly [CLIENT_QUIT]", markers)
	}
	if !quitTerminal.isManaged() || quitTerminal.exitCode() != 0 {
		t.Fatalf("managed quit line state: managed=%v code=%d", quitTerminal.isManaged(), quitTerminal.exitCode())
	}

	eofBuf := &bytes.Buffer{}
	eofTerminal := newTerminalCoordinator(eofBuf)
	stdinLoop(bufio.NewReader(strings.NewReader("")), eofTerminal)
	unitAwaitCompletion(t, eofTerminal)
	if markers := unitMarkersIn(eofBuf.String()); len(markers) != 1 || markers[0] != "CLIENT_UNKNOWN" {
		t.Fatalf("explicit stdin end markers = %v, want exactly [CLIENT_UNKNOWN]", markers)
	}
	if eofTerminal.isManaged() || eofTerminal.exitCode() == 0 {
		t.Fatalf("explicit stdin end must be a nonzero safe UNKNOWN: managed=%v code=%d", eofTerminal.isManaged(), eofTerminal.exitCode())
	}

	ownedBuf := &bytes.Buffer{}
	ownedTerminal := newTerminalCoordinator(ownedBuf)
	if !ownedTerminal.resolve("CLIENT_SERVER_EOF", 0) {
		t.Fatal("genuine EOF did not claim an unowned terminal")
	}
	stdinLoop(bufio.NewReader(strings.NewReader("")), ownedTerminal)
	if markers := unitMarkersIn(ownedBuf.String()); len(markers) != 1 || markers[0] != "CLIENT_SERVER_EOF" {
		t.Fatalf("stdin end after an owned terminal replaced the winner: %v", markers)
	}
	if ownedTerminal.exitCode() != 0 || ownedTerminal.isManaged() {
		t.Fatalf("stdin end after an owned terminal changed the winner state: managed=%v code=%d", ownedTerminal.isManaged(), ownedTerminal.exitCode())
	}
}

// TestProducerUnitTerminalOwnerExactlyOnce races every terminal candidate
// through the same owner and proves exactly one decision wins, exactly one
// marker is emitted, completion follows the winner and the exit code and
// managed state belong to the winner alone.
func TestProducerUnitTerminalOwnerExactlyOnce(t *testing.T) {
	out := newUnitTerminalOutput()
	close(out.release) // arbitration proof, not the emission boundary
	terminal := newTerminalCoordinator(out)
	candidates := []struct {
		marker string
		code   int
	}{
		{"CLIENT_SERVER_EOF", 0}, {"CLIENT_UNKNOWN", 3}, {"CLIENT_QUIT", 0},
		{"CLIENT_SERVER_EOF", 0}, {"CLIENT_UNKNOWN", 3}, {"CLIENT_QUIT", 0},
		{"CLIENT_SERVER_EOF", 0}, {"CLIENT_QUIT", 0},
	}
	wins := make(chan string, len(candidates))
	var wg sync.WaitGroup
	for _, candidate := range candidates {
		wg.Add(1)
		go func(marker string, code int) {
			defer wg.Done()
			if terminal.resolve(marker, code) {
				wins <- marker
			}
		}(candidate.marker, candidate.code)
	}
	wg.Wait()
	close(wins)
	var winners []string
	for marker := range wins {
		winners = append(winners, marker)
	}
	if len(winners) != 1 {
		t.Fatalf("terminal decisions are not exactly one: %v", winners)
	}
	unitAwaitCompletion(t, terminal)
	if marker := unitSoleMarker(t, out); marker != winners[0] {
		t.Fatalf("emitted marker %q is not the winning decision %q", marker, winners[0])
	}
	wantCode := 0
	if winners[0] == "CLIENT_UNKNOWN" {
		wantCode = 3
	}
	if terminal.exitCode() != wantCode || terminal.isManaged() != (winners[0] == "CLIENT_QUIT") {
		t.Fatalf("winner state does not belong to %q: managed=%v code=%d", winners[0], terminal.isManaged(), terminal.exitCode())
	}
}
`

func TestBorrowedAuthStagingProducerUnit(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate producer unit test source")
	}
	source := filepath.Join(filepath.Dir(testFile), "borrowed-auth-staging-client_linux_testhelper.go")
	helperSource, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("read staging helper source: %v", err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "producer.go"), helperSource, 0o600); err != nil {
		t.Fatalf("write producer copy: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "producer_unit_test.go"), []byte(generatedProducerUnitTests), 0o600); err != nil {
		t.Fatalf("write generated unit tests: %v", err)
	}
	cmd := exec.Command("go", "test", "-count=1", "-run", "TestProducerUnit", "producer.go", "producer_unit_test.go")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated producer unit tests failed: %v\n%s", err, strings.TrimSpace(string(out)))
	}
}

// borrowedAuthStagingTerminalOutput is the synchronized stdout sink of the
// actual-process terminal proof. It is drained by os/exec (output completion is
// coordinated before the sole Wait) and exposes immutable line snapshots plus
// bounded prefix waits; raw content is never printed.
type borrowedAuthStagingTerminalOutput struct {
	mu      sync.Mutex
	pending string
	lines   []string
	notify  chan struct{}
}

func newBorrowedAuthStagingTerminalOutput() *borrowedAuthStagingTerminalOutput {
	return &borrowedAuthStagingTerminalOutput{notify: make(chan struct{}, 1)}
}

func (o *borrowedAuthStagingTerminalOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	o.pending += string(p)
	for {
		index := strings.IndexByte(o.pending, '\n')
		if index < 0 {
			break
		}
		o.lines = append(o.lines, strings.TrimRight(o.pending[:index], "\r"))
		o.pending = o.pending[index+1:]
	}
	o.mu.Unlock()
	select {
	case o.notify <- struct{}{}:
	default:
	}
	return len(p), nil
}

func (o *borrowedAuthStagingTerminalOutput) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.lines...)
}

func (o *borrowedAuthStagingTerminalOutput) await(prefix string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		for _, line := range o.snapshot() {
			if strings.HasPrefix(line, prefix) {
				return line, nil
			}
		}
		if time.Now().After(deadline) {
			return "", errors.New("staging helper output prefix was not observed in time")
		}
		select {
		case <-o.notify:
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (o *borrowedAuthStagingTerminalOutput) markers() []string {
	var markers []string
	for _, line := range o.snapshot() {
		switch strings.TrimSpace(line) {
		case "CLIENT_QUIT", "CLIENT_SERVER_EOF", "CLIENT_UNKNOWN":
			markers = append(markers, strings.TrimSpace(line))
		}
	}
	return markers
}

// borrowedAuthStagingTerminalProcess owns one spawned static helper process
// with its private stdin, the os/exec-drained stdout sink, one listener-side
// connection and exactly one sole Wait.
type borrowedAuthStagingTerminalProcess struct {
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	stdout   *borrowedAuthStagingTerminalOutput
	listener net.Listener
	conn     net.Conn
	wait     chan struct{}
	waitErr  error
}

// startBorrowedAuthStagingTerminalProcess starts the separately built static
// helper in the prestartup hold against a private listener, writes the private
// secret line and awaits the READY phase marker.
func startBorrowedAuthStagingTerminalProcess(t *testing.T, helper, secret string) *borrowedAuthStagingTerminalProcess {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("staging terminal listener: %v", err)
	}
	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatal("staging terminal listener is not TCP")
	}
	cmd := exec.Command(helper, "prestartup-hold", "terminal_unit_role", "terminal_unit_db", "127.0.0.1", strconv.Itoa(tcpAddr.Port))
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("staging helper private stdin: %v", err)
	}
	stdout := newBorrowedAuthStagingTerminalOutput()
	cmd.Stdout = stdout
	cmd.Stderr = io.Discard
	process := &borrowedAuthStagingTerminalProcess{cmd: cmd, stdin: stdin, stdout: stdout, listener: listener, wait: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start staging helper: %v", err)
	}
	t.Cleanup(func() {
		_ = process.stdin.Close()
		if process.conn != nil {
			_ = process.conn.Close()
		}
		_ = process.listener.Close()
		select {
		case <-process.wait:
		case <-time.After(10 * time.Second):
			_ = process.cmd.Process.Kill()
			<-process.wait
		}
	})
	go func() {
		process.waitErr = cmd.Wait() // sole Wait; the os/exec output copy completed first
		close(process.wait)
	}()
	if _, err := io.WriteString(stdin, secret+"\n"); err != nil {
		t.Fatalf("write private staging secret: %v", err)
	}
	if tcpListener, ok := listener.(*net.TCPListener); ok {
		_ = tcpListener.SetDeadline(time.Now().Add(30 * time.Second))
	}
	conn, err := listener.Accept()
	if err != nil {
		t.Fatalf("staging helper never dialed the terminal listener: %v", err)
	}
	process.conn = conn
	if _, err := stdout.await("READY ", 30*time.Second); err != nil {
		t.Fatalf("staging helper never reached READY: %v", err)
	}
	return process
}

func (p *borrowedAuthStagingTerminalProcess) awaitWait(t *testing.T, timeout time.Duration) int {
	t.Helper()
	select {
	case <-p.wait:
	case <-time.After(timeout):
		t.Fatalf("staging helper process did not exit")
	}
	if p.waitErr == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(p.waitErr, &exitErr) {
		return exitErr.ExitCode()
	}
	t.Fatalf("staging helper Wait failed: %v", p.waitErr)
	return -1
}

// TestBorrowedAuthStagingProducerTerminalProcess spawns the separately built
// static helper and proves the real stdout markers and process exit codes:
// managed QUIT wins with exactly one CLIENT_QUIT exit 0 and no EOF, a genuine
// server close wins with exactly one CLIENT_SERVER_EOF exit 0, an explicit
// stdin end after READY is exactly one CLIENT_UNKNOWN with the safe constant
// nonzero exit (never a silent exit 0), and a bounded EOF/QUIT interleaving
// shake always publishes exactly one terminal marker with the matching exit
// code. The secret line is never printed.
func TestBorrowedAuthStagingProducerTerminalProcess(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate producer unit test source")
	}
	source := filepath.Join(filepath.Dir(testFile), "borrowed-auth-staging-client_linux_testhelper.go")
	helper := filepath.Join(t.TempDir(), "borrowed-auth-staging-client")
	build := exec.Command("go", "build", "-trimpath", "-o", helper, source)
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("static staging helper build refused: %v\n%s", err, strings.TrimSpace(string(out)))
	}
	secret := "terminal-unit-secret-never-emitted"

	// Managed QUIT wins: exactly one CLIENT_QUIT, exit 0, never a server EOF.
	quit := startBorrowedAuthStagingTerminalProcess(t, helper, secret)
	if _, err := io.WriteString(quit.stdin, "quit\n"); err != nil {
		t.Fatalf("managed quit write refused: %v", err)
	}
	if code := quit.awaitWait(t, 30*time.Second); code != 0 {
		t.Fatalf("managed QUIT exit code = %d, want 0", code)
	}
	if markers := quit.stdout.markers(); len(markers) != 1 || markers[0] != "CLIENT_QUIT" {
		t.Fatalf("managed QUIT terminal markers = %v, want exactly [CLIENT_QUIT]", markers)
	}

	// Genuine server close wins: exactly one CLIENT_SERVER_EOF, exit 0.
	eof := startBorrowedAuthStagingTerminalProcess(t, helper, secret)
	if err := eof.conn.Close(); err != nil {
		t.Fatalf("close staging listener connection: %v", err)
	}
	if code := eof.awaitWait(t, 30*time.Second); code != 0 {
		t.Fatalf("server EOF exit code = %d, want 0", code)
	}
	if markers := eof.stdout.markers(); len(markers) != 1 || markers[0] != "CLIENT_SERVER_EOF" {
		t.Fatalf("server EOF terminal markers = %v, want exactly [CLIENT_SERVER_EOF]", markers)
	}

	// Explicit stdin end after READY: exactly one CLIENT_UNKNOWN, nonzero.
	stdinEnd := startBorrowedAuthStagingTerminalProcess(t, helper, secret)
	if err := stdinEnd.stdin.Close(); err != nil {
		t.Fatalf("close staging helper stdin: %v", err)
	}
	code := stdinEnd.awaitWait(t, 30*time.Second)
	if code == 0 {
		t.Fatal("explicit stdin end after READY exited 0 silently")
	}
	if code != 3 {
		t.Fatalf("explicit stdin end exit code = %d, want the safe constant nonzero 3", code)
	}
	if markers := stdinEnd.stdout.markers(); len(markers) != 1 || markers[0] != "CLIENT_UNKNOWN" {
		t.Fatalf("explicit stdin end terminal markers = %v, want exactly [CLIENT_UNKNOWN]", markers)
	}

	// Bounded EOF/QUIT interleaving shake: the old flag-only decision could
	// lose both markers and exit 0 silently; the owner must always publish
	// exactly one terminal marker with the matching exit code.
	for attempt := 0; attempt < 8; attempt++ {
		race := startBorrowedAuthStagingTerminalProcess(t, helper, secret)
		if err := race.conn.Close(); err != nil {
			t.Fatalf("interleaving %d close connection: %v", attempt, err)
		}
		_, _ = io.WriteString(race.stdin, "quit\n") // may race an already completed exit
		raceCode := race.awaitWait(t, 30*time.Second)
		markers := race.stdout.markers()
		if len(markers) != 1 {
			t.Fatalf("interleaving %d published %v terminal markers, want exactly one", attempt, markers)
		}
		wantCode := 0
		switch markers[0] {
		case "CLIENT_QUIT", "CLIENT_SERVER_EOF":
			wantCode = 0
		default:
			t.Fatalf("interleaving %d published an unexpected terminal marker %q", attempt, markers[0])
		}
		if raceCode != wantCode {
			t.Fatalf("interleaving %d marker %q exit code = %d, want %d", attempt, markers[0], raceCode, wantCode)
		}
	}

	// No secret and no raw failure text on stdout.
	for _, out := range []*borrowedAuthStagingTerminalOutput{quit.stdout, eof.stdout, stdinEnd.stdout} {
		for _, line := range out.snapshot() {
			if strings.Contains(line, secret) || strings.HasPrefix(line, "HELPER_ERROR") {
				t.Fatal("staging helper stdout leaked non-protocol content")
			}
		}
	}
}
