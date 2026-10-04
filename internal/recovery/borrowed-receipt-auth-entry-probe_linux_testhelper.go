//go:build ignore

// borrowed-receipt-auth-entry-probe_linux_testhelper.go is the ENTRY-PRIVATE
// strict container probe for the borrowed receipt auth entry. It is compiled by
// the entry test and copied only into the owned disposable PostgreSQL fixture
// container at a private path. It is deliberately independent of the shared
// auth/gate census helper: every process fact is parsed with the Go standard
// library (no shell/awk), the expected PID is validated against the leading
// stat field, the command delimiter must be the first '(' and the last ')' with
// the kernel's space separator, the state must be one of the kernel-known
// single-character states, and the start identity must be a positive numeric
// value. Only ENOENT is disappearance; a structurally valid different numeric
// start is PID reuse; a same-identity Z is an unreaped zombie (not gone); every
// other read/parse condition (EACCES, EIO, ELOOP, EISDIR, ENOTDIR, malformed or
// truncated documents) is an explicit UNKNOWN refusal with the errno named.
//
// The census mode re-validates the postmaster before and after enumerating the
// full direct-child scope, classifies EVERY fd readlink, refuses UNKNOWN on ANY
// unreconciled fd directory or readlink ENOENT (an fd enumerated a moment ago
// can be renumbered/duplicated away before it is classified, so a vanished fd
// or fd directory never proves absence or process disappearance), refuses on
// every other read error with the errno named, strictly parses both TCP tables
// (no silently skipped line), and reports whether the exact registered
// PID/start and socket inode remain owned anywhere in that scope. It performs
// no write, no signal and grants no authority.
//
// The unexported fd scan I/O wrappers and the census notification hook exist
// only for the generated program unit test: they drive deterministic fd
// renumbering/vanish races and injected negative errno branches. Their
// nonempty configurations are validated completely before any scan, and every
// configuration, marker I/O or ack-deadline failure is an explicit UNKNOWN
// refusal, never a continued scan. They are inert unless the wrapper test sets
// their private environment variables, can only produce refusals, and never
// declare validity or grant authority; the entry path never sets them.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const probeStateSet = "RSDTZtXxKWPI"

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "pidstat":
		runPIDStat(os.Args[2:])
	case "census":
		runCensus(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Println("PROBE ERROR usage")
	os.Exit(2)
}

// runPIDStat classifies one expected PID/start identity. The optional third
// argument is a test-only proc-root override used by the unit classification
// test; the entry path never passes it and the default is the real /proc.
func runPIDStat(args []string) {
	if len(args) != 2 && len(args) != 3 {
		usage()
	}
	pid, err := strconv.Atoi(args[0])
	if err != nil || pid < 2 {
		fmt.Println("STAGE UNKNOWN reason=expected-pid-invalid")
		return
	}
	expectedStart, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil || expectedStart == 0 {
		fmt.Println("STAGE UNKNOWN reason=expected-start-invalid")
		return
	}
	procroot := "/proc"
	if len(args) == 3 {
		procroot = args[2]
	}
	result, err := readProcessStat(procroot, pid)
	if err != nil {
		fmt.Printf("STAGE UNKNOWN errno=%s reason=stat-read-error\n", errnoName(err))
		return
	}
	switch {
	case result.gone:
		fmt.Println("STAGE GONE")
	case result.malformed:
		fmt.Println("STAGE MALFORMED reason=stat-malformed")
	case result.start == expectedStart:
		if result.state == "Z" {
			fmt.Printf("STAGE ZOMBIE %d\n", result.start)
		} else {
			fmt.Printf("STAGE ALIVE %d\n", result.start)
		}
	default:
		fmt.Printf("STAGE REUSED %d\n", result.start)
	}
}

type processStat struct {
	gone      bool
	malformed bool
	state     string
	start     uint64
}

func readProcessStat(procroot string, pid int) (processStat, error) {
	raw, err := os.ReadFile(filepath.Join(procroot, strconv.Itoa(pid), "stat"))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return processStat{gone: true}, nil
		}
		return processStat{}, err
	}
	state, start, parseErr := parseProcessStat(pid, raw)
	if parseErr != nil {
		return processStat{malformed: true}, nil
	}
	return processStat{state: state, start: start}, nil
}

// parseProcessStat validates the expected PID, the command delimiter, a
// recognized kernel state and a positive numeric start identity (post-delimiter
// field index 19, the kernel's field 22). Any deviation is an UNKNOWN/malformed
// refusal, never evidence of disappearance.
func parseProcessStat(expectedPID int, raw []byte) (string, uint64, error) {
	if expectedPID < 2 {
		return "", 0, errors.New("expected PID is invalid")
	}
	text := string(raw)
	open := strings.IndexByte(text, '(')
	if open <= 0 || open > 20 {
		return "", 0, errors.New("command delimiter is not recognized")
	}
	leading, err := strconv.Atoi(strings.TrimSpace(text[:open]))
	if err != nil || leading != expectedPID {
		return "", 0, errors.New("leading PID does not match the expected process")
	}
	last := strings.LastIndexByte(text, ')')
	if last <= open || last+1 >= len(text) || text[last+1] != ' ' {
		return "", 0, errors.New("command delimiter is malformed")
	}
	fields := strings.Fields(text[last+1:])
	if len(fields) <= 19 {
		return "", 0, errors.New("stat fields are truncated")
	}
	state := fields[0]
	if len(state) != 1 || !strings.ContainsRune(probeStateSet, rune(state[0])) {
		return "", 0, errors.New("state is not a recognized Linux state")
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || start == 0 {
		return "", 0, errors.New("start field is not a valid positive identity")
	}
	return state, start, nil
}

func errnoName(err error) string {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.EACCES:
			return "EACCES"
		case syscall.EIO:
			return "EIO"
		case syscall.ENOENT:
			return "ENOENT"
		case syscall.EISDIR:
			return "EISDIR"
		case syscall.ELOOP:
			return "ELOOP"
		case syscall.ENOTDIR:
			return "ENOTDIR"
		default:
			return fmt.Sprintf("ERRNO_%d", uintptr(errno))
		}
	}
	return "UNKNOWN"
}

// runCensus proves, inside the owned container PID namespace, that the exact
// registered PID/start instance and socket inode no longer appear anywhere in
// the protected postmaster's direct-child scope, with the postmaster
// incarnation strictly bound before and after.
func runCensus(args []string) {
	if len(args) != 5 {
		usage()
	}
	postmasterPID, err := strconv.Atoi(args[0])
	if err != nil || postmasterPID < 2 {
		censusUnknown("postmaster-pid-invalid")
		return
	}
	postmasterStart, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil || postmasterStart == 0 {
		censusUnknown("postmaster-start-invalid")
		return
	}
	expectedPID, err := strconv.Atoi(args[2])
	if err != nil || expectedPID < 2 {
		censusUnknown("expected-pid-invalid")
		return
	}
	expectedStart, err := strconv.ParseUint(args[3], 10, 64)
	if err != nil || expectedStart == 0 {
		censusUnknown("expected-start-invalid")
		return
	}
	expectedInode := args[4]
	if expectedInode == "" {
		censusUnknown("expected-inode-invalid")
		return
	}
	if _, err := strconv.ParseUint(expectedInode, 10, 64); err != nil {
		censusUnknown("expected-inode-invalid")
		return
	}

	// The test-only fault/hook seams are validated COMPLETELY before any scan:
	// a nonempty malformed specification can never silently fall back to
	// normal reads or to an unrefused handshake, even when the requested
	// operation never occurs.
	fault, err := parseProbeFdFault(os.Getenv("TXHARBOR_PROBE_FD_FAULT"))
	if err != nil {
		censusUnknown("probe-fd-fault-invalid")
		return
	}
	hook, err := parseProbeFdHook(os.Getenv("TXHARBOR_PROBE_FD_HOOK_DIR"), os.Getenv("TXHARBOR_PROBE_FD_HOOK_PID"))
	if err != nil {
		censusUnknown("probe-fd-hook-invalid")
		return
	}

	postmaster, err := readProcessStat("/proc", postmasterPID)
	if err != nil {
		censusUnknownErrno("postmaster-stat-read-error", err)
		return
	}
	if postmaster.gone || postmaster.malformed || postmaster.start != postmasterStart || postmaster.state == "Z" {
		censusUnknown("postmaster-identity-changed")
		return
	}

	childrenRaw, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%d/children", postmasterPID, postmasterPID))
	if err != nil {
		censusUnknownErrno("postmaster-children-read-error", err)
		return
	}
	childFields := strings.Fields(string(childrenRaw))
	if len(childFields) == 0 {
		censusUnknown("postmaster-child-scope-empty")
		return
	}

	pidAbsent := true
	inodeAbsent := true
	liveChildren := 0
	ownedSockets := 0
	for _, field := range childFields {
		childPID, err := strconv.Atoi(field)
		if err != nil || childPID < 2 {
			censusUnknown("child-pid-malformed")
			return
		}
		child, err := readProcessStat("/proc", childPID)
		if err != nil {
			censusUnknownErrno("child-stat-read-error", err)
			return
		}
		if child.gone {
			continue
		}
		if child.malformed {
			censusUnknown("child-stat-malformed")
			return
		}
		if childPID == expectedPID && child.start == expectedStart {
			// The exact registered instance is still inside the protected
			// scope, whatever its transient state: the old backend is present.
			pidAbsent = false
		}
		if child.state == "Z" {
			continue
		}
		liveChildren++
		// The child identity was just read as live; the notification hook lets
		// the generated program unit test deterministically interleave a
		// process/fd mutation at this exact scan point. It is inert in the
		// entry path (no hook environment) and never influences the verdict.
		// An activated-seam handshake failure is an explicit UNKNOWN refusal,
		// never a continued scan.
		if err := probeFdCensusHook(childPID, "after-stat", hook); err != nil {
			probeFdHookUnknown("after-stat", err)
			return
		}
		entries, err := probeFdReadDir(fmt.Sprintf("/proc/%d/fd", childPID), fault)
		if err != nil {
			// A vanished fd directory is not proof that the child is gone:
			// the identity was read moments ago, so skipping it would
			// silently shrink the ownership scan. Any errno here, including
			// ENOENT, is an explicit UNKNOWN refusal.
			censusUnknownErrno("child-fd-table-read-error", err)
			return
		}
		if err := probeFdCensusHook(childPID, "after-readdir", hook); err != nil {
			probeFdHookUnknown("after-readdir", err)
			return
		}
		for _, entry := range entries {
			target, err := probeFdReadlink(filepath.Join("/proc", strconv.Itoa(childPID), "fd", entry.Name()), fault)
			if err != nil {
				// An fd enumerated a moment ago can be renumbered/duplicated
				// away before this readlink; skipping it would report an
				// incomplete ownership scan as absence. Any errno here,
				// including ENOENT, is an explicit UNKNOWN refusal.
				censusUnknownErrno("child-fd-readlink-error", err)
				return
			}
			index := strings.Index(target, "socket:[")
			if index < 0 {
				continue
			}
			inode := strings.TrimSuffix(target[index+len("socket:["):], "]")
			if inode == "" {
				censusUnknown("child-fd-socket-inode-malformed")
				return
			}
			ownedSockets++
			if inode == expectedInode {
				inodeAbsent = false
			}
		}
	}

	// Strict TCP table parse: every non-header line must be structurally valid.
	for _, path := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		if err := validateTCPTable(path); err != nil {
			censusUnknownErrno("net-table-invalid", err)
			return
		}
	}

	// Final postmaster bound check: the same incarnation must still hold.
	postmasterAfter, err := readProcessStat("/proc", postmasterPID)
	if err != nil {
		censusUnknownErrno("postmaster-stat-read-error-after", err)
		return
	}
	if postmasterAfter.gone || postmasterAfter.malformed || postmasterAfter.start != postmasterStart || postmasterAfter.state == "Z" {
		censusUnknown("postmaster-identity-changed-after")
		return
	}

	// Reaching this point means every live child's fd directory was enumerated
	// and every enumerated fd was classified without any refusal: only a
	// complete ownership accounting can report absence.
	verdict := "OK"
	if !pidAbsent || !inodeAbsent {
		verdict = "PRESENT"
	}
	fmt.Printf("CENSUS %s pid_absent=%d inode_absent=%d children=%d live=%d sockets=%d postmaster_start_before=%d postmaster_start_after=%d\n",
		verdict, boolInt(pidAbsent), boolInt(inodeAbsent), len(childFields), liveChildren, ownedSockets, postmasterStart, postmasterAfter.start)
}

func validateTCPTable(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) < 1 {
		return errors.New("net table is empty")
	}
	for _, line := range lines[1:] {
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 10 {
			return errors.New("net table line is malformed")
		}
		if _, err := decodeEndpoint(fields[1]); err != nil {
			return errors.New("net table local endpoint is malformed")
		}
		if _, err := decodeEndpoint(fields[2]); err != nil {
			return errors.New("net table remote endpoint is malformed")
		}
		if _, err := strconv.ParseUint(fields[9], 10, 64); err != nil {
			return errors.New("net table inode is malformed")
		}
	}
	return nil
}

func censusUnknown(reason string) {
	fmt.Printf("CENSUS UNKNOWN reason=%s\n", reason)
}

func censusUnknownErrno(reason string, err error) {
	fmt.Printf("CENSUS UNKNOWN errno=%s reason=%s\n", errnoName(err), reason)
}

// probeFdFaultConfig is the validated test-only negative fault configuration.
// The zero value is inert. A nonempty configuration is always the result of
// the complete strict parse below; there is no permissive fallback and no
// default errno.
type probeFdFaultConfig struct {
	operation string
	errno     error
}

// parseProbeFdFault validates the COMPLETE nonempty fault specification before
// any scan. Only the exact form <readdir|readlink>:<EACCES|EIO> is accepted;
// a missing delimiter, an empty or unknown operation, an unsupported errno and
// any trailing garbage are refused. The empty specification stays inert.
func parseProbeFdFault(spec string) (probeFdFaultConfig, error) {
	if spec == "" {
		return probeFdFaultConfig{}, nil
	}
	operation, value, ok := strings.Cut(spec, ":")
	if !ok {
		return probeFdFaultConfig{}, errors.New("fault operation delimiter is missing")
	}
	switch operation {
	case "readdir", "readlink":
	default:
		return probeFdFaultConfig{}, errors.New("fault operation is not supported")
	}
	var errno error
	switch value {
	case "EACCES":
		errno = syscall.EACCES
	case "EIO":
		errno = syscall.EIO
	default:
		return probeFdFaultConfig{}, errors.New("fault errno is not supported")
	}
	return probeFdFaultConfig{operation: operation, errno: errno}, nil
}

// injectedErrno returns the validated fault for the named operation only; a
// valid fault for another operation leaves this operation untouched.
func (c probeFdFaultConfig) injectedErrno(op string) error {
	if c.operation == "" || c.operation != op {
		return nil
	}
	return c.errno
}

// probeFdReadDir is the unexported I/O operation dependency for fd directory
// enumeration. The generated program unit test can inject a validated negative
// errno (TXHARBOR_PROBE_FD_FAULT=readdir:EACCES); the injection can only
// produce a refusal, and the entry path always performs the real read.
func probeFdReadDir(path string, fault probeFdFaultConfig) ([]fs.DirEntry, error) {
	if err := fault.injectedErrno("readdir"); err != nil {
		return nil, err
	}
	return os.ReadDir(path)
}

// probeFdReadlink is the unexported I/O operation dependency for fd target
// classification with the same validated test-only negative errno injection
// (TXHARBOR_PROBE_FD_FAULT=readlink:EIO).
func probeFdReadlink(path string, fault probeFdFaultConfig) (string, error) {
	if err := fault.injectedErrno("readlink"); err != nil {
		return "", err
	}
	return os.Readlink(path)
}

// probeFdHookAckDeadline bounds the whole activated handshake from inside the
// probe. It is finite and clipped inside the wrapper's owned probe wait
// budget; an arbitrary hung kernel filesystem is NOT claimed to be
// hard-bounded here (the outer owned host process context and its sole Wait
// own orphan cleanup), while the specified FIFO-open block is prevented
// locally by the exclusive marker create and the nonblocking ack probe.
const probeFdHookAckDeadline = 10 * time.Second

// errProbeFdHookAckTimeout marks the bounded missing-ack deadline.
var errProbeFdHookAckTimeout = errors.New("hook ack deadline expired")

// probeFdHookConfig is the validated test-only notification hook
// configuration. The zero value is inert.
type probeFdHookConfig struct {
	dir    string
	pid    int
	active bool
}

// parseProbeFdHook validates a nonempty hook configuration before any scan. A
// nonempty directory requires a valid positive target PID; an empty directory
// leaves the seam inert.
func parseProbeFdHook(dir, pidText string) (probeFdHookConfig, error) {
	if dir == "" {
		return probeFdHookConfig{}, nil
	}
	pid, err := strconv.Atoi(pidText)
	if err != nil || pid < 2 {
		return probeFdHookConfig{}, errors.New("hook target pid is invalid")
	}
	return probeFdHookConfig{dir: dir, pid: pid, active: true}, nil
}

// probeFdCensusHook is the test-only census notification seam. When the
// generated program unit test sets TXHARBOR_PROBE_FD_HOOK_DIR and names the
// target child in TXHARBOR_PROBE_FD_HOOK_PID, the census publishes an
// exclusively created ready marker carrying a fresh nonce for the named phase
// and waits, bounded, for the wrapper's nonce echo before continuing. Every
// configuration, marker I/O or ack-deadline failure is returned as an error
// and becomes an explicit UNKNOWN refusal; the seam is inert when the hook
// environment is absent (the entry path never sets it). The created marker is
// left in place for the wrapper-owned hook directory lifecycle: the probe
// never removes, truncates or unblocks any path during or after the
// handshake, so a replacement object can never be unlinked by the helper.
func probeFdCensusHook(childPID int, phase string, hook probeFdHookConfig) error {
	if !hook.active || hook.pid != childPID {
		return nil
	}
	ready := filepath.Join(hook.dir, fmt.Sprintf("%d.%s.ready", childPID, phase))
	ack := filepath.Join(hook.dir, fmt.Sprintf("%d.%s.ack", childPID, phase))
	nonce, err := probeFdHookNonce()
	if err != nil {
		return err
	}
	// Exclusive no-follow creation never opens or truncates a preexisting
	// regular file, FIFO or symlink (EEXIST instead), so the specified
	// FIFO-open block cannot occur and the preexisting object is untouched.
	marker, err := os.OpenFile(ready, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	// The created marker is deliberately NOT removed here: the marker and any
	// replacement object belong to the wrapper-owned hook directory lifecycle.
	// The probe never unlinks, truncates or unblocks a path during or after
	// the handshake.
	if _, err := io.WriteString(marker, nonce); err != nil {
		_ = marker.Close()
		return err
	}
	if err := marker.Close(); err != nil {
		return err
	}
	deadline := time.Now().Add(probeFdHookAckDeadline)
	for time.Now().Before(deadline) {
		matched, err := probeFdHookAckMatches(ack, nonce)
		if err != nil {
			return err
		}
		if matched {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return errProbeFdHookAckTimeout
}

// probeFdHookUnknown maps an activated-seam failure to the strict census
// refusal protocol: preexisting marker objects, the bounded ack deadline and
// every other handshake I/O failure are explicit UNKNOWN, never a continued
// scan.
func probeFdHookUnknown(phase string, err error) {
	switch {
	case errors.Is(err, fs.ErrExist):
		fmt.Printf("CENSUS UNKNOWN reason=child-fd-hook-marker-preexisting phase=%s\n", phase)
	case errors.Is(err, errProbeFdHookAckTimeout):
		fmt.Printf("CENSUS UNKNOWN reason=child-fd-hook-ack-timeout phase=%s\n", phase)
	default:
		fmt.Printf("CENSUS UNKNOWN errno=%s reason=child-fd-hook-error phase=%s\n", errnoName(err), phase)
	}
}

// probeFdHookAckMatches reports whether the ACTUAL opened ack descriptor is a
// regular file whose content exactly echoes the fresh ready nonce. The type
// proof is the fstat of the opened descriptor, never a pathname check: a FIFO
// or other non-regular object substituted before the open is refused before
// any byte is read, so a stream supplying the nonce can never be accepted. A
// symlink is refused by O_NOFOLLOW, ENOENT simply means the ack is not there
// yet under the bounded loop, and the no-follow/nonblocking open means a
// hostile path cannot block the probe.
func probeFdHookAckMatches(path, nonce string) (bool, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, nil
	}
	raw, err := io.ReadAll(io.LimitReader(file, int64(len(nonce)+1)))
	if err != nil {
		return false, err
	}
	return string(raw) == nonce, nil
}

// probeFdHookNonce returns a fresh unpredictable nonce so a preexisting
// arbitrary object can never be mistaken for the wrapper's acknowledgement.
func probeFdHookNonce() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

// decodeEndpoint decodes the kernel /proc/net/tcp hex endpoint form with
// IPv4-mapped normalization. Port 0 is a valid encoding and never equals a
// concrete anchor port.
func decodeEndpoint(raw string) (string, error) {
	parts := strings.Split(raw, ":")
	if len(parts) != 2 {
		return "", errors.New("malformed endpoint")
	}
	port, err := strconv.ParseInt(parts[1], 16, 32)
	if err != nil || port < 0 {
		return "", errors.New("malformed port")
	}
	addressBytes, err := hexDecode(parts[0])
	if err != nil {
		return "", err
	}
	var ip net.IP
	switch len(addressBytes) {
	case 4:
		ip = net.IPv4(addressBytes[3], addressBytes[2], addressBytes[1], addressBytes[0])
	case 16:
		mapped := true
		for _, b := range addressBytes[:10] {
			if b != 0 {
				mapped = false
				break
			}
		}
		if mapped && addressBytes[10] == 0xff && addressBytes[11] == 0xff {
			ip = net.IPv4(addressBytes[15], addressBytes[14], addressBytes[13], addressBytes[12])
		} else {
			ip = net.IP(addressBytes)
		}
	default:
		return "", errors.New("unsupported address width")
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	return net.JoinHostPort(ip.String(), strconv.Itoa(int(port))), nil
}

func hexDecode(raw string) ([]byte, error) {
	if len(raw)%2 != 0 {
		return nil, errors.New("odd hex length")
	}
	out := make([]byte, len(raw)/2)
	for i := 0; i < len(out); i++ {
		value, err := strconv.ParseUint(raw[2*i:2*i+2], 16, 8)
		if err != nil {
			return nil, errors.New("malformed hex")
		}
		out[i] = byte(value)
	}
	return out, nil
}
