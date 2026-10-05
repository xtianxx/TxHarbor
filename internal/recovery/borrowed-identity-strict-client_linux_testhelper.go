//go:build ignore

// This standalone strict namespace inspector is compiled by the borrowed
// identity prefix test helper and copied only into the owned disposable
// PostgreSQL fixture container at its private prefix path. It runs inside the
// fixture PID/network namespace and emits one JSON census for the EXACT
// required control backend: OS start identity before all fd/readlink/net table
// reads and again after them, direct postmaster parenthood, non-zombie state,
// every fd readlink classified (ENOENT counted, any other error is an explicit
// UNKNOWN reason), all TCP socket metadata joined by inode, and an optional
// required socket inode that must still be present. It performs no write, no
// signal and no authority grant; every incomplete or uncertain read produces a
// complete=false report with fixed safe reasons instead of a weak skip.
package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type endpoint struct {
	Inode  string `json:"inode"`
	TCP    bool   `json:"tcp"`
	Local  string `json:"local,omitempty"`
	Remote string `json:"remote,omitempty"`
	State  string `json:"state,omitempty"`
}

type processInfo struct {
	PID         int    `json:"pid"`
	StartBefore string `json:"start_before"`
	StartAfter  string `json:"start_after"`
	State       string `json:"state"`
	PPID        int    `json:"ppid"`
}

type report struct {
	OK                   bool        `json:"ok"`
	Complete             bool        `json:"complete"`
	Errors               []string    `json:"errors"`
	Postmaster           processInfo `json:"postmaster"`
	Backend              processInfo `json:"backend"`
	FDTotal              int         `json:"fd_total"`
	FDReadable           int         `json:"fd_readable"`
	FDENOENT             int         `json:"fd_enoent"`
	FDUnknown            int         `json:"fd_unknown"`
	Sockets              []endpoint  `json:"sockets"`
	RequiredInode        string      `json:"required_inode,omitempty"`
	RequiredInodePresent bool        `json:"required_inode_present"`
}

func main() {
	result := inspect(os.Args)
	encoded, err := json.Marshal(result)
	if err != nil {
		fmt.Fprintln(os.Stdout, `{"ok":false,"complete":false,"errors":["census-marshal-failed"]}`)
		return
	}
	fmt.Fprintln(os.Stdout, string(encoded))
}

func inspect(args []string) report {
	result := report{Errors: []string{}}
	if len(args) < 5 || args[1] != "inspect" {
		return refuse(result, "usage-invalid")
	}
	postmasterPID, err := strconv.Atoi(args[2])
	if err != nil || postmasterPID < 2 {
		return refuse(result, "postmaster-pid-invalid")
	}
	postmasterStart := args[3]
	backendPID, err := strconv.Atoi(args[4])
	if err != nil || backendPID < 2 {
		return refuse(result, "backend-pid-invalid")
	}
	if len(args) >= 6 {
		result.RequiredInode = args[5]
	}

	// OS start identity of the postmaster and the required backend BEFORE any
	// fd table, readlink, status or network table field is read.
	pmStart, pmState, pmPPID, err := statProcess(postmasterPID)
	if err != nil {
		return refuse(result, "postmaster-stat-unreadable")
	}
	if pmStart != postmasterStart {
		return refuse(result, "postmaster-start-mismatch")
	}
	result.Postmaster = processInfo{PID: postmasterPID, StartBefore: pmStart, State: pmState, PPID: pmPPID}

	children, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%d/children", postmasterPID, postmasterPID))
	if err != nil {
		return refuse(result, "postmaster-children-unreadable")
	}
	listed := false
	for _, field := range strings.Fields(string(children)) {
		if field == strconv.Itoa(backendPID) {
			listed = true
		}
	}
	if !listed {
		return refuse(result, "backend-not-direct-postmaster-child")
	}

	backendStart, backendState, backendPPID, err := statProcess(backendPID)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return refuse(result, "backend-gone-before-inspection")
		}
		return refuse(result, "backend-stat-unreadable")
	}
	if backendPPID != postmasterPID {
		return refuse(result, "backend-parent-mismatch")
	}
	if backendState == "Z" || backendState == "" {
		return refuse(result, "backend-not-live")
	}
	if backendStart == "" || backendStart == "0" {
		return refuse(result, "backend-start-invalid")
	}
	result.Backend = processInfo{PID: backendPID, StartBefore: backendStart, State: backendState, PPID: backendPPID}

	// Strict fd table: every entry must be classified. ENOENT reconciles a
	// disappeared entry; any other readlink error is an explicit UNKNOWN.
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", backendPID))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return refuse(result, "backend-gone-during-fd-enumeration")
		}
		return refuse(result, "backend-fd-table-unreadable")
	}
	result.FDTotal = len(entries)
	var socketInodes []string
	for _, entry := range entries {
		target, readErr := os.Readlink(filepath.Join("/proc", strconv.Itoa(backendPID), "fd", entry.Name()))
		if readErr != nil {
			if errors.Is(readErr, fs.ErrNotExist) {
				result.FDENOENT++
				continue
			}
			result.FDUnknown++
			return refuse(result, "backend-fd-readlink-error")
		}
		result.FDReadable++
		if index := strings.Index(target, "socket:["); index >= 0 {
			inode := strings.TrimSuffix(target[index+len("socket:["):], "]")
			if inode == "" {
				return refuse(result, "backend-fd-socket-inode-malformed")
			}
			socketInodes = append(socketInodes, inode)
		}
	}

	table, err := readTCPTables()
	if err != nil {
		return refuse(result, err.Error())
	}
	for _, inode := range socketInodes {
		row, found := table[inode]
		if !found {
			// A socket fd absent from the TCP tables is a non-TCP socket; it
			// is reported without TCP metadata and never treated as a match.
			result.Sockets = append(result.Sockets, endpoint{Inode: inode})
			continue
		}
		result.Sockets = append(result.Sockets, endpoint{Inode: inode, TCP: true, Local: row.local, Remote: row.remote, State: row.state})
	}
	if result.RequiredInode != "" {
		for _, socket := range result.Sockets {
			if socket.Inode == result.RequiredInode {
				result.RequiredInodePresent = true
				break
			}
		}
		if !result.RequiredInodePresent {
			return refuse(result, "required-socket-inode-absent")
		}
	}

	// OS start identity AFTER every fd/readlink/network field read. The exact
	// backend and postmaster incarnation must be unchanged; otherwise the
	// bracket did not hold and the census is UNKNOWN.
	backendStartAfter, backendStateAfter, backendPPIDAfter, err := statProcess(backendPID)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return refuse(result, "backend-gone-after-inspection")
		}
		return refuse(result, "backend-stat-unreadable-after")
	}
	// Start identity and parenthood are the immutable tokens; the transient
	// scheduler state may legitimately change (S/R) and is only required to be
	// non-zombie at the end.
	if backendStartAfter != backendStart || backendPPIDAfter != backendPPID {
		return refuse(result, "backend-start-changed-across-inspection")
	}
	if backendStateAfter == "Z" || backendStateAfter == "" {
		return refuse(result, "backend-not-live-after-inspection")
	}
	pmStartAfter, _, _, err := statProcess(postmasterPID)
	if err != nil {
		return refuse(result, "postmaster-stat-unreadable-after")
	}
	if pmStartAfter != pmStart {
		return refuse(result, "postmaster-start-changed-across-inspection")
	}
	result.Backend.StartAfter = backendStartAfter
	result.Postmaster.StartAfter = pmStartAfter
	result.OK = true
	result.Complete = true
	return result
}

func refuse(result report, reason string) report {
	result.OK = false
	result.Complete = false
	result.Errors = append(result.Errors, reason)
	return result
}

func statProcess(pid int) (start string, state string, ppid int, err error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", "", 0, err
	}
	end := strings.LastIndexByte(string(raw), ')')
	if end < 0 {
		return "", "", 0, errors.New("malformed process stat")
	}
	fields := strings.Fields(string(raw[end+1:]))
	if len(fields) <= 19 {
		return "", "", 0, errors.New("truncated process stat")
	}
	parsedPPID, err := strconv.Atoi(fields[1])
	if err != nil {
		return "", "", 0, errors.New("malformed parent pid")
	}
	return fields[19], fields[0], parsedPPID, nil
}

type tcpRow struct {
	local  string
	remote string
	state  string
}

func readTCPTables() (map[string]tcpRow, error) {
	table := make(map[string]tcpRow)
	for _, path := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, errors.New("net-table-unreadable")
		}
		lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
		if len(lines) < 1 {
			return nil, errors.New("net-table-empty")
		}
		for _, line := range lines[1:] {
			if strings.TrimSpace(line) == "" {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 10 {
				return nil, errors.New("net-table-line-malformed")
			}
			local, err := decodeNetEndpoint(fields[1])
			if err != nil {
				return nil, errors.New("net-table-local-endpoint-malformed")
			}
			remote, err := decodeNetEndpoint(fields[2])
			if err != nil {
				return nil, errors.New("net-table-remote-endpoint-malformed")
			}
			table[fields[9]] = tcpRow{local: local, remote: remote, state: fields[3]}
		}
	}
	return table, nil
}

// decodeNetEndpoint decodes the kernel /proc/net/tcp hex endpoint form with
// IPv4-mapped normalization and returns a canonical ip:port string. Port 0 is
// a valid encoding (listening or unconnected sockets) and can never equal a
// concrete anchor port.
func decodeNetEndpoint(raw string) (string, error) {
	parts := strings.Split(raw, ":")
	if len(parts) != 2 {
		return "", errors.New("malformed endpoint")
	}
	port, err := strconv.ParseInt(parts[1], 16, 32)
	if err != nil || port < 0 {
		return "", errors.New("malformed port")
	}
	addressBytes, err := hex.DecodeString(parts[0])
	if err != nil {
		return "", errors.New("malformed address")
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
