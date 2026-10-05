//go:build ignore

// This standalone helper is compiled by the Linux drill test and copied only
// into its disposable PostgreSQL container. It runs as a client sibling, not
// as a postmaster child, so its localhost socket tuple is observable in the
// server PID/network namespace without Docker host-NAT inference.
package main

import (
	"bufio"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stdout, "HELPER_ERROR missing mode")
		os.Exit(2)
	}
	mode := os.Args[1]
	if mode == "census" {
		if len(os.Args) != 4 {
			fmt.Fprintln(os.Stdout, "HELPER_ERROR invalid census args")
			os.Exit(2)
		}
		if err := census(os.Args[2], os.Args[3]); err != nil {
			fmt.Fprintf(os.Stdout, "HELPER_ERROR census: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if mode == "authcheck" {
		runAuthCheck(os.Args)
		return
	}
	if mode == "queued" {
		runQueuedClient(os.Args)
		return
	}
	role := ""
	serverIP := ""
	if mode == "scram" {
		if len(os.Args) != 4 {
			fmt.Fprintln(os.Stdout, "HELPER_ERROR invalid scram args")
			os.Exit(2)
		}
		role = os.Args[2]
		serverIP = os.Args[3]
	} else if mode == "prestartup" {
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stdout, "HELPER_ERROR invalid prestartup args")
			os.Exit(2)
		}
		serverIP = os.Args[2]
	}
	var password string
	if mode == "scram" {
		line, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil {
			fmt.Fprintln(os.Stdout, "HELPER_ERROR password input missing")
			os.Exit(2)
		}
		password = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	}
	if mode != "scram" && mode != "prestartup" {
		fmt.Fprintln(os.Stdout, "HELPER_ERROR invalid mode")
		os.Exit(2)
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(serverIP, "5432"), 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stdout, "HELPER_ERROR dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	local := conn.LocalAddr().(*net.TCPAddr)
	localPort := local.Port
	localIP := local.IP.String()
	if mode == "prestartup" {
		fmt.Printf("CLIENT_READY state=PRESTARTUP pid=%d local_port=%d local_ip=%s\n", os.Getpid(), localPort, localIP)
	} else {
		if err := beginSCRAM(conn, role, password); err != nil {
			fmt.Fprintf(os.Stdout, "HELPER_ERROR scram: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("CLIENT_READY state=SASLContinue pid=%d local_port=%d local_ip=%s\n", os.Getpid(), localPort, localIP)
	}

	commands := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			commands <- scanner.Text()
		}
		commands <- "stdin-closed"
	}()
	reader := bufio.NewReader(conn)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		_, _, err := readBackend(reader)
		if err == nil {
			fmt.Fprintln(os.Stdout, "HELPER_ERROR unexpected server message before proof")
			return
		}
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			select {
			case command := <-commands:
				if command == "quit" || command == "stdin-closed" {
					fmt.Fprintln(os.Stdout, "CLIENT_EXIT state=QUIT")
					return
				}
				fmt.Fprintln(os.Stdout, "HELPER_ERROR unsupported control command")
				return
			default:
			}
			continue
		}
		if err == io.EOF || strings.Contains(err.Error(), "reset by peer") || strings.Contains(err.Error(), "broken pipe") {
			fmt.Fprintln(os.Stdout, "CLIENT_EXIT state=SERVER_EOF")
			return
		}
		fmt.Fprintf(os.Stdout, "CLIENT_EXIT state=SERVER_ERROR error=%q\n", err.Error())
		return
	}
}

func census(postmasterPIDText, expectedStart string) error {
	postmasterPID, err := strconv.Atoi(postmasterPIDText)
	if err != nil || postmasterPID < 2 {
		return fmt.Errorf("invalid postmaster PID")
	}
	start, _, err := processIdentity(postmasterPID)
	if err != nil || start != expectedStart {
		return fmt.Errorf("postmaster start identity changed")
	}
	childrenBytes, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%d/children", postmasterPID, postmasterPID))
	if err != nil {
		return err
	}
	var netRows []tcpRow
	for _, path := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		rows, readErr := readTCPTable(path)
		if readErr != nil {
			return readErr
		}
		netRows = append(netRows, rows...)
	}
	for _, row := range netRows {
		if row.LocalPort == 5432 {
			fmt.Printf("N %s %s %s %s\n", row.Local, row.Remote, row.State, row.Inode)
		}
	}
	for _, field := range strings.Fields(string(childrenBytes)) {
		pid, err := strconv.Atoi(field)
		if err != nil {
			return fmt.Errorf("invalid child PID in proc list")
		}
		start, state, err := processIdentity(pid)
		if err != nil {
			continue
		}
		fmt.Printf("P %d %s %s\n", pid, start, state)
		entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
		if err != nil {
			return fmt.Errorf("read fd table for child PID %d: %w", pid, err)
		}
		owned := make(map[string]struct{})
		readable := 0
		for _, entry := range entries {
			target, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(pid), "fd", entry.Name()))
			if err != nil {
				continue
			}
			readable++
			if !strings.Contains(target, "socket:[") {
				continue
			}
			inode := target[strings.Index(target, "socket:[")+len("socket:["):]
			inode = strings.TrimSuffix(inode, "]")
			if inode == "" {
				continue
			}
			owned[inode] = struct{}{}
			fmt.Printf("F %d %s\n", pid, inode)
		}
		fmt.Printf("X %d %d %d\n", pid, len(entries), readable)
		for _, row := range netRows {
			if row.LocalPort != 5432 || row.State != "01" {
				continue
			}
			if _, ok := owned[row.Inode]; ok {
				fmt.Printf("S %d %s %s %s %s\n", pid, start, row.Inode, row.Local, row.Remote)
			}
		}
	}
	return nil
}

type tcpRow struct {
	Local     string
	Remote    string
	LocalPort int
	State     string
	Inode     string
}

func readTCPTable(path string) ([]tcpRow, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return nil, scanner.Err()
	}
	var rows []tcpRow
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 10 {
			continue
		}
		localParts := strings.Split(fields[1], ":")
		if len(localParts) != 2 {
			continue
		}
		port, err := strconv.ParseInt(localParts[1], 16, 32)
		if err != nil {
			continue
		}
		rows = append(rows, tcpRow{Local: fields[1], Remote: fields[2], LocalPort: int(port), State: fields[3], Inode: fields[9]})
	}
	return rows, scanner.Err()
}

func processIdentity(pid int) (string, string, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", "", err
	}
	line := string(data)
	end := strings.LastIndex(line, ") ")
	if end < 0 || end+2 >= len(line) {
		return "", "", fmt.Errorf("malformed stat for PID %d", pid)
	}
	fields := strings.Fields(line[end+2:])
	if len(fields) < 20 {
		return "", "", fmt.Errorf("short stat for PID %d", pid)
	}
	return fields[19], fields[0], nil
}

func beginSCRAM(conn net.Conn, role, password string) error {
	startup := []byte("user\x00" + role + "\x00database\x00txharbor\x00client_encoding\x00UTF8\x00\x00")
	packet := make([]byte, 8+len(startup))
	binary.BigEndian.PutUint32(packet[:4], uint32(len(packet)))
	binary.BigEndian.PutUint32(packet[4:8], 196608)
	copy(packet[8:], startup)
	if _, err := conn.Write(packet); err != nil {
		return err
	}
	reader := bufio.NewReader(conn)
	typ, body, err := readBackend(reader)
	if err != nil {
		return err
	}
	if typ != 'R' || len(body) < 4 || binary.BigEndian.Uint32(body[:4]) != 10 {
		code := uint32(0)
		if len(body) >= 4 {
			code = binary.BigEndian.Uint32(body[:4])
		}
		return fmt.Errorf("expected AuthenticationSASL, got type=%q auth_code=%d body_bytes=%d", typ, code, len(body))
	}
	if !strings.Contains(string(body[4:]), "SCRAM-SHA-256\x00") {
		return fmt.Errorf("SCRAM-SHA-256 not offered")
	}
	var nonce [18]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	clientFirstBare := "n=,r=" + base64.RawStdEncoding.EncodeToString(nonce[:])
	initial := append([]byte("SCRAM-SHA-256\x00"), make([]byte, 4)...)
	binary.BigEndian.PutUint32(initial[len("SCRAM-SHA-256\x00"):], uint32(len("n,,"+clientFirstBare)))
	initial = append(initial, []byte("n,,"+clientFirstBare)...)
	if err := writeFrontend(conn, 'p', initial); err != nil {
		return err
	}
	typ, body, err = readBackend(reader)
	if err != nil {
		return err
	}
	if typ != 'R' || len(body) < 4 || binary.BigEndian.Uint32(body[:4]) != 11 {
		return fmt.Errorf("expected actual AuthenticationSASLContinue, got %q", typ)
	}
	// P0 is received privately from stdin and is intentionally not used to
	// manufacture any authentication result; this client stops before proof.
	_ = password
	return nil
}

func readBackend(reader *bufio.Reader) (byte, []byte, error) {
	typ, err := reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n < 4 || n > 1<<20 {
		return 0, nil, fmt.Errorf("invalid PostgreSQL message length %d", n)
	}
	body := make([]byte, int(n)-4)
	if _, err := io.ReadFull(reader, body); err != nil {
		return 0, nil, err
	}
	return typ, body, nil
}

func writeFrontend(conn net.Conn, typ byte, body []byte) error {
	packet := make([]byte, 5+len(body))
	packet[0] = typ
	binary.BigEndian.PutUint32(packet[1:5], uint32(4+len(body)))
	copy(packet[5:], body)
	_, err := conn.Write(packet)
	return err
}

// runAuthCheck performs a full SCRAM-SHA-256 exchange against one transport
// with a credential supplied privately on stdin and prints a public outcome
// line. It never prints the credential and selects no data.
func runAuthCheck(args []string) {
	var transport, serverIP, role string
	switch len(args) {
	case 4:
		transport, role = args[2], args[3]
	case 5:
		transport, serverIP, role = args[2], args[3], args[4]
	default:
		fmt.Fprintln(os.Stdout, "HELPER_ERROR invalid authcheck args")
		os.Exit(2)
	}
	reader := bufio.NewReader(os.Stdin)
	password, err := reader.ReadString('\n')
	if err != nil {
		fmt.Fprintln(os.Stdout, "HELPER_ERROR authcheck credential input missing")
		os.Exit(2)
	}
	password = strings.TrimSuffix(strings.TrimSuffix(password, "\n"), "\r")
	conn, err := dialTransport(transport, serverIP)
	if err != nil {
		fmt.Fprintf(os.Stdout, "AUTHCHECK state=TRANSPORT_ERROR error=%q\n", err.Error())
		return
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	outcome, sqlstate, err := authenticate(conn, role, password)
	if err != nil {
		fmt.Fprintf(os.Stdout, "AUTHCHECK state=ERROR error=%q\n", err.Error())
		return
	}
	fmt.Printf("AUTHCHECK state=%s sqlstate=%s\n", outcome, sqlstate)
}

// runQueuedClient opens a TCP connection that can sit in the listen backlog
// while the postmaster is frozen, prints CLIENT_READY state=QUEUED, then waits
// for a "go" line on stdin before sending StartupMessage and completing a real
// SCRAM exchange with the supplied credential.
func runQueuedClient(args []string) {
	if len(args) != 4 {
		fmt.Fprintln(os.Stdout, "HELPER_ERROR invalid queued args")
		os.Exit(2)
	}
	role, serverIP := args[2], args[3]
	reader := bufio.NewReader(os.Stdin)
	password, err := reader.ReadString('\n')
	if err != nil {
		fmt.Fprintln(os.Stdout, "HELPER_ERROR queued credential input missing")
		os.Exit(2)
	}
	password = strings.TrimSuffix(strings.TrimSuffix(password, "\n"), "\r")
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(serverIP, "5432"), 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stdout, "HELPER_ERROR queued dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	local := conn.LocalAddr().(*net.TCPAddr)
	fmt.Printf("CLIENT_READY state=QUEUED pid=%d local_port=%d local_ip=%s\n", os.Getpid(), local.Port, local.IP.String())
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Fprintln(os.Stdout, "QUEUED_NEG state=CONTROL_CLOSED")
			return
		}
		if strings.TrimSpace(line) == "go" {
			break
		}
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	outcome, sqlstate, err := authenticate(conn, role, password)
	if err != nil {
		fmt.Fprintf(os.Stdout, "QUEUED_NEG state=ERROR error=%q\n", err.Error())
		return
	}
	fmt.Printf("QUEUED_NEG state=%s sqlstate=%s\n", outcome, sqlstate)
}

func dialTransport(transport, serverIP string) (net.Conn, error) {
	switch transport {
	case "unix":
		return net.DialTimeout("unix", "/var/run/postgresql/.s.PGSQL.5432", 5*time.Second)
	case "loopback":
		return net.DialTimeout("tcp", "127.0.0.1:5432", 5*time.Second)
	case "serverip":
		if serverIP == "" {
			return nil, fmt.Errorf("serverip transport requires an address")
		}
		return net.DialTimeout("tcp", net.JoinHostPort(serverIP, "5432"), 5*time.Second)
	default:
		return nil, fmt.Errorf("unsupported transport %q", transport)
	}
}

// authenticate performs a full SCRAM-SHA-256 exchange for the given credential
// and classifies the outcome. A trust-style immediate AuthenticationOk is
// reported as NO_SASL_AUTH_OK for the caller to judge; it is never silently
// treated as success.
func authenticate(conn net.Conn, role, password string) (string, string, error) {
	startup := []byte("user\x00" + role + "\x00database\x00txharbor\x00client_encoding\x00UTF8\x00\x00")
	packet := make([]byte, 8+len(startup))
	binary.BigEndian.PutUint32(packet[:4], uint32(len(packet)))
	binary.BigEndian.PutUint32(packet[4:8], 196608)
	copy(packet[8:], startup)
	if _, err := conn.Write(packet); err != nil {
		return "", "", err
	}
	reader := bufio.NewReader(conn)
	typ, body, err := readBackend(reader)
	if err != nil {
		return "", "", err
	}
	if typ == 'E' {
		return "REJECT", parseErrorFields(body)['C'], nil
	}
	if typ != 'R' || len(body) < 4 {
		return "", "", fmt.Errorf("unexpected first backend message type %q", typ)
	}
	authCode := binary.BigEndian.Uint32(body[:4])
	if authCode == 0 {
		return "NO_SASL_AUTH_OK", "", nil
	}
	if authCode != 10 {
		return fmt.Sprintf("NON_SCRAM_AUTH_%d", authCode), "", nil
	}
	if !strings.Contains(string(body[4:]), "SCRAM-SHA-256\x00") {
		return "SCRAM_NOT_OFFERED", "", nil
	}
	var nonce [18]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", "", err
	}
	clientNonce := base64.RawStdEncoding.EncodeToString(nonce[:])
	clientBare := "n=,r=" + clientNonce
	initial := append([]byte("SCRAM-SHA-256\x00"), make([]byte, 4)...)
	binary.BigEndian.PutUint32(initial[len("SCRAM-SHA-256\x00"):], uint32(len("n,,"+clientBare)))
	initial = append(initial, []byte("n,,"+clientBare)...)
	if err := writeFrontend(conn, 'p', initial); err != nil {
		return "", "", err
	}
	typ, body, err = readBackend(reader)
	if err != nil {
		return "", "", err
	}
	if typ == 'E' {
		return "REJECT", parseErrorFields(body)['C'], nil
	}
	if typ != 'R' || len(body) < 4 || binary.BigEndian.Uint32(body[:4]) != 11 {
		return "", "", fmt.Errorf("expected real AuthenticationSASLContinue, got type=%q", typ)
	}
	serverFirst := string(body[4:])
	salt, err := base64.StdEncoding.DecodeString(scramAttribute(serverFirst, 's'))
	if err != nil {
		return "", "", fmt.Errorf("decode SCRAM salt: %w", err)
	}
	iterations, err := strconv.Atoi(scramAttribute(serverFirst, 'i'))
	if err != nil || iterations < 1 || iterations > 1_000_000 {
		return "", "", fmt.Errorf("invalid SCRAM iteration count")
	}
	nonceAttribute := scramAttribute(serverFirst, 'r')
	if !strings.HasPrefix(nonceAttribute, clientNonce) || nonceAttribute == clientNonce {
		return "", "", fmt.Errorf("invalid SCRAM server nonce")
	}
	withoutProof := "c=biws,r=" + nonceAttribute
	authMessage := clientBare + "," + serverFirst + "," + withoutProof
	salted := pbkdf2SHA256([]byte(password), salt, iterations, sha256.Size)
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	clientSignature := hmacSHA256(storedKey[:], []byte(authMessage))
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ clientSignature[i]
	}
	final := withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)
	if err := writeFrontend(conn, 'p', []byte(final)); err != nil {
		return "", "", err
	}
	for {
		typ, body, err = readBackend(reader)
		if err != nil {
			return "", "", err
		}
		if typ == 'E' {
			return "REJECT", parseErrorFields(body)['C'], nil
		}
		if typ == 'R' && len(body) >= 4 {
			switch binary.BigEndian.Uint32(body[:4]) {
			case 12:
				continue
			case 0:
				return "AUTH_OK", "", nil
			}
		}
	}
}

func parseErrorFields(body []byte) map[byte]string {
	fields := make(map[byte]string)
	for i := 0; i < len(body); {
		code := body[i]
		i++
		if code == 0 {
			break
		}
		j := i
		for j < len(body) && body[j] != 0 {
			j++
		}
		fields[code] = string(body[i:j])
		i = j + 1
	}
	return fields
}

func hmacSHA256(key, message []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(message)
	return h.Sum(nil)
}

func scramAttribute(message string, key byte) string {
	for _, item := range strings.Split(message, ",") {
		if len(item) > 2 && item[0] == key && item[1] == '=' {
			return item[2:]
		}
	}
	return ""
}

func pbkdf2SHA256(password, salt []byte, iterations, length int) []byte {
	var block [4]byte
	binary.BigEndian.PutUint32(block[:], 1)
	u := hmacSHA256(password, append(append([]byte(nil), salt...), block[:]...))
	out := append([]byte(nil), u...)
	for i := 1; i < iterations; i++ {
		u = hmacSHA256(password, u)
		for j := range out {
			out[j] ^= u[j]
		}
	}
	return out[:length]
}
