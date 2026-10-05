//go:build ignore

// origin_gate_client_linux_testhelper.go is the standalone frontend client for
// the bounded continuous-origin gate prototype (fix101 lane). The drill test
// compiles this file explicitly (go build <file>) into a disposable binary and
// spawns it as a genuine owned OS process with the exec.Cmd retained. It speaks
// the raw PostgreSQL frontend/backend protocol over TCP:
//
//   - optional SSLRequest / GSSENCRequest negotiation, with the real reply byte
//     reported verbatim (never interpreted as success);
//   - StartupMessage, then a genuine SCRAM-SHA-256 exchange in which the only
//     credential is read privately from stdin (never argv, never env);
//   - for the "write" mode, the first Query frame (INSERT) is sent immediately
//     after the real AuthenticationOk, before ReadyForQuery, so the gate under
//     test must hold an executable frontend frame until its origin/backend
//     registration completes;
//   - public metadata only on stdout: PID, loopback tuple, protocol state and
//     SQLSTATE verdicts. No password or SCRAM material is ever printed.
//
// Exit codes: 0 for a scripted outcome (including a genuine server rejection),
// 3 for a transport/protocol error. The process never manufactures an
// AuthenticationOk: it only reports what the real server sent.
package main

import (
	"bufio"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	helperMaxMessage    = 1 << 20
	helperStartupProto  = 196608
	helperNegotiateSSL  = 80877103
	helperNegotiateGSS  = 80877104
	helperCancelRequest = 80877102
	helperDatabase      = "txharbor"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "write":
		runWrite(os.Args)
	case "auth":
		runAuth(os.Args)
	case "negotiate":
		runNegotiate(os.Args)
	case "idle":
		runIdle(os.Args)
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: origin-gate-client write <addr> <role> <table> <ref> <neg:none|ssl|gss> <appname>")
	fmt.Fprintln(os.Stderr, "       origin-gate-client auth <addr> <role> <neg:none|ssl|gss> <appname>")
	fmt.Fprintln(os.Stderr, "       origin-gate-client negotiate <addr> <kind:ssl|gss>")
	fmt.Fprintln(os.Stderr, "       origin-gate-client idle")
	os.Exit(2)
}

// readSecret reads the lone credential from private stdin. The credential is
// deliberately absent from argv and from the environment.
func readSecret() (string, error) {
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("credential unavailable on stdin: %w", err)
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}

type helperClient struct {
	conn     net.Conn
	reader   *bufio.Reader
	role     string
	password string
	appname  string
}

func dialHelper(addr string) (*helperClient, error) {
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	local, ok := conn.LocalAddr().(*net.TCPAddr)
	if !ok {
		_ = conn.Close()
		return nil, errors.New("unexpected local address type")
	}
	fmt.Printf("ORIGIN_CLIENT_READY pid=%d local_ip=%s local_port=%d\n", os.Getpid(), local.IP.String(), local.Port)
	return &helperClient{conn: conn, reader: bufio.NewReader(conn)}, nil
}

func (c *helperClient) deadline(d time.Duration) { _ = c.conn.SetDeadline(time.Now().Add(d)) }

func (c *helperClient) startup() error {
	params := []byte("user\x00" + c.role + "\x00database\x00" + helperDatabase +
		"\x00application_name\x00" + c.appname + "\x00client_encoding\x00UTF8\x00\x00")
	packet := make([]byte, 8+len(params))
	binary.BigEndian.PutUint32(packet[:4], uint32(len(packet)))
	binary.BigEndian.PutUint32(packet[4:8], helperStartupProto)
	copy(packet[8:], params)
	c.deadline(30 * time.Second)
	_, err := c.conn.Write(packet)
	return err
}

// negotiate sends one SSLRequest/GSSENCRequest and reports the single real
// reply byte the peer sent. It never treats 'N'/'G' as authentication success.
func (c *helperClient) negotiate(kind string) error {
	var code uint32
	switch kind {
	case "ssl":
		code = helperNegotiateSSL
	case "gss":
		code = helperNegotiateGSS
	default:
		return fmt.Errorf("unsupported negotiation kind %q", kind)
	}
	request := make([]byte, 8)
	binary.BigEndian.PutUint32(request[:4], 8)
	binary.BigEndian.PutUint32(request[4:8], code)
	c.deadline(15 * time.Second)
	if _, err := c.conn.Write(request); err != nil {
		return err
	}
	reply := make([]byte, 1)
	if _, err := io.ReadFull(c.reader, reply); err != nil {
		return fmt.Errorf("negotiation reply read: %w", err)
	}
	fmt.Printf("ORIGIN_CLIENT_NEG kind=%s reply=%c\n", kind, reply[0])
	return nil
}

func (c *helperClient) writeFrontend(typ byte, body []byte) error {
	packet := make([]byte, 5+len(body))
	packet[0] = typ
	binary.BigEndian.PutUint32(packet[1:5], uint32(4+len(body)))
	copy(packet[5:], body)
	c.deadline(30 * time.Second)
	_, err := c.conn.Write(packet)
	return err
}

func (c *helperClient) readBackend() (byte, []byte, error) {
	typ, err := c.reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	var header [4]byte
	if _, err := io.ReadFull(c.reader, header[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n < 4 || n > helperMaxMessage {
		return 0, nil, fmt.Errorf("invalid backend message length %d", n)
	}
	body := make([]byte, int(n)-4)
	if _, err := io.ReadFull(c.reader, body); err != nil {
		return 0, nil, err
	}
	return typ, body, nil
}

// scram performs a genuine SCRAM-SHA-256 exchange. It returns ok=true only when
// the real server sent AuthenticationOk (auth code 0); a server ErrorResponse
// yields the SQLSTATE from the real error, and a trust-style immediate
// AuthenticationOk without SASL is reported as NO_SASL_AUTH_OK for the caller
// to reject.
func (c *helperClient) scram() (ok bool, sqlstate string, err error) {
	c.deadline(45 * time.Second)
	typ, body, err := c.readBackend()
	if err != nil {
		return false, "", err
	}
	if typ == 'E' {
		return false, errorSQLState(body), nil
	}
	if typ != 'R' || len(body) < 4 {
		return false, "", fmt.Errorf("unexpected first backend message type %q", typ)
	}
	code := binary.BigEndian.Uint32(body[:4])
	if code == 0 {
		return false, "NO_SASL_AUTH_OK", nil
	}
	if code != 10 || !strings.Contains(string(body[4:]), "SCRAM-SHA-256\x00") {
		return false, fmt.Sprintf("NON_SCRAM_AUTH_%d", code), nil
	}
	fmt.Fprintln(os.Stdout, "ORIGIN_CLIENT_EVENT state=AUTH_OFFER code=10")
	var nonce [18]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return false, "", err
	}
	clientNonce := base64.RawStdEncoding.EncodeToString(nonce[:])
	clientBare := "n=,r=" + clientNonce
	clientFirst := "n,," + clientBare
	initial := append([]byte("SCRAM-SHA-256\x00"), make([]byte, 4)...)
	binary.BigEndian.PutUint32(initial[len("SCRAM-SHA-256\x00"):], uint32(len(clientFirst)))
	initial = append(initial, []byte(clientFirst)...)
	if err := c.writeFrontend('p', initial); err != nil {
		return false, "", err
	}
	typ, body, err = c.readBackend()
	if err != nil {
		return false, "", err
	}
	if typ == 'E' {
		return false, errorSQLState(body), nil
	}
	if typ != 'R' || len(body) < 4 || binary.BigEndian.Uint32(body[:4]) != 11 {
		return false, "", fmt.Errorf("expected real AuthenticationSASLContinue, got type=%q", typ)
	}
	serverFirst := string(body[4:])
	serverNonce := scramAttribute(serverFirst, 'r')
	if !strings.HasPrefix(serverNonce, clientNonce) || serverNonce == clientNonce {
		return false, "", errors.New("invalid SCRAM server nonce")
	}
	salt, err := base64.StdEncoding.DecodeString(scramAttribute(serverFirst, 's'))
	if err != nil {
		return false, "", fmt.Errorf("decode SCRAM salt: %w", err)
	}
	iterations, err := strconv.Atoi(scramAttribute(serverFirst, 'i'))
	if err != nil || iterations < 1 || iterations > 1_000_000 {
		return false, "", errors.New("invalid SCRAM iteration count")
	}
	withoutProof := "c=biws,r=" + serverNonce
	authMessage := clientBare + "," + serverFirst + "," + withoutProof
	salted := pbkdf2SHA256([]byte(c.password), salt, iterations, sha256.Size)
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	clientSignature := hmacSHA256(storedKey[:], []byte(authMessage))
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ clientSignature[i]
	}
	final := withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)
	if err := c.writeFrontend('p', []byte(final)); err != nil {
		return false, "", err
	}
	verifiedFinal := false
	for {
		typ, body, err = c.readBackend()
		if err != nil {
			return false, "", err
		}
		if typ == 'E' {
			return false, errorSQLState(body), nil
		}
		if typ != 'R' || len(body) < 4 {
			continue
		}
		switch binary.BigEndian.Uint32(body[:4]) {
		case 12:
			serverKey := hmacSHA256(salted, []byte("Server Key"))
			expected := base64.StdEncoding.EncodeToString(hmacSHA256(serverKey, []byte(authMessage)))
			if scramAttribute(string(body[4:]), 'v') != expected {
				return false, "", errors.New("server SCRAM signature mismatch")
			}
			verifiedFinal = true
		case 0:
			if !verifiedFinal {
				return false, "", errors.New("AuthenticationOk before verified SCRAM final")
			}
			return true, "", nil
		}
	}
}

func (c *helperClient) sendTerminate() {
	_ = c.writeFrontend('X', nil)
}

func runWrite(args []string) {
	if len(args) != 8 {
		usage()
	}
	addr, role, table, ref, neg, appname := args[2], args[3], args[4], args[5], args[6], args[7]
	password, err := readSecret()
	if err != nil {
		transportFail(err)
	}
	client, err := dialHelper(addr)
	if err != nil {
		transportFail(err)
	}
	defer client.conn.Close()
	client.role, client.password, client.appname = role, password, appname
	if neg != "none" {
		if err := client.negotiate(neg); err != nil {
			transportFail(err)
		}
	}
	if err := client.startup(); err != nil {
		transportFail(err)
	}
	ok, sqlstate, err := client.scram()
	if err != nil {
		transportFail(err)
	}
	if !ok {
		fmt.Printf("ORIGIN_CLIENT_EXIT state=REJECT sqlstate=%s\n", sqlstate)
		os.Exit(0)
	}
	fmt.Fprintln(os.Stdout, "ORIGIN_CLIENT_EVENT state=AUTH_OK")
	statement := "INSERT INTO " + table + "(ref) VALUES ('" + strings.ReplaceAll(ref, "'", "''") + "')"
	if err := client.writeFrontend('Q', append([]byte(statement), 0)); err != nil {
		transportFail(err)
	}
	fmt.Fprintln(os.Stdout, "ORIGIN_CLIENT_EVENT state=QUERY_SENT")
	// The blocking backend read below keeps detecting protected-channel loss
	// (server EOF after a latch) while the stdin goroutine owns only the
	// graceful terminate path.
	go func() {
		reader := bufio.NewReader(os.Stdin)
		line, err := reader.ReadString('\n')
		if err != nil || strings.TrimSpace(line) == "quit" {
			client.sendTerminate()
			fmt.Fprintln(os.Stdout, "ORIGIN_CLIENT_EXIT state=QUIT")
			os.Exit(0)
		}
	}()
	client.deadline(10 * time.Minute)
	for {
		typ, body, err := client.readBackend()
		if err != nil {
			if isPeerClose(err) {
				fmt.Fprintln(os.Stdout, "ORIGIN_CLIENT_EXIT state=SERVER_EOF")
				os.Exit(0)
			}
			transportFail(err)
		}
		switch typ {
		case 'E':
			fmt.Printf("ORIGIN_CLIENT_EVENT state=SERVER_ERROR sqlstate=%s\n", errorSQLState(body))
			fmt.Fprintln(os.Stdout, "ORIGIN_CLIENT_EXIT state=SERVER_EOF")
			os.Exit(0)
		case 'C':
			fmt.Printf("ORIGIN_CLIENT_EVENT state=COMMAND tag=%q\n", commandTag(body))
		case 'Z':
			fmt.Fprintln(os.Stdout, "ORIGIN_CLIENT_EVENT state=SERVER_READY")
		}
	}
}

func runAuth(args []string) {
	if len(args) != 6 {
		usage()
	}
	addr, role, neg, appname := args[2], args[3], args[4], args[5]
	password, err := readSecret()
	if err != nil {
		transportFail(err)
	}
	client, err := dialHelper(addr)
	if err != nil {
		transportFail(err)
	}
	defer client.conn.Close()
	client.role, client.password, client.appname = role, password, appname
	if neg != "none" {
		if err := client.negotiate(neg); err != nil {
			transportFail(err)
		}
	}
	if err := client.startup(); err != nil {
		transportFail(err)
	}
	ok, sqlstate, err := client.scram()
	if err != nil {
		transportFail(err)
	}
	if !ok {
		fmt.Printf("ORIGIN_CLIENT_EVENT state=REJECT sqlstate=%s\n", sqlstate)
		fmt.Println("ORIGIN_CLIENT_EXIT state=REJECT")
		os.Exit(0)
	}
	fmt.Fprintln(os.Stdout, "ORIGIN_CLIENT_EVENT state=AUTH_OK")
	client.sendTerminate()
	fmt.Fprintln(os.Stdout, "ORIGIN_CLIENT_EXIT state=AUTH_OK")
	os.Exit(0)
}

func runNegotiate(args []string) {
	if len(args) != 4 {
		usage()
	}
	client, err := dialHelper(args[2])
	if err != nil {
		transportFail(err)
	}
	defer client.conn.Close()
	if err := client.negotiate(args[3]); err != nil {
		transportFail(err)
	}
	os.Exit(0)
}

func runIdle(args []string) {
	if len(args) != 2 {
		usage()
	}
	fmt.Printf("ORIGIN_CLIENT_READY pid=%d mode=idle\n", os.Getpid())
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Fprintln(os.Stdout, "ORIGIN_CLIENT_EXIT state=QUIT")
			os.Exit(0)
		}
		if strings.TrimSpace(line) == "quit" {
			fmt.Fprintln(os.Stdout, "ORIGIN_CLIENT_EXIT state=QUIT")
			os.Exit(0)
		}
	}
}

func transportFail(err error) {
	fmt.Printf("ORIGIN_CLIENT_EXIT state=TRANSPORT_ERROR detail=%q\n", err.Error())
	os.Exit(3)
}

func isPeerClose(err error) bool {
	if errors.Is(err, io.EOF) {
		return true
	}
	message := err.Error()
	return strings.Contains(message, "reset by peer") || strings.Contains(message, "broken pipe") || strings.Contains(message, "EOF")
}

func errorSQLState(body []byte) string {
	fields := parseErrorFields(body)
	if state := fields['C']; state != "" {
		return state
	}
	return "unknown"
}

func commandTag(body []byte) string { return string(body) }

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
