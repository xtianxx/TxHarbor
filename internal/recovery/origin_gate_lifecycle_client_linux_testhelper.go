//go:build ignore

// origin_gate_lifecycle_client_linux_testhelper.go is the standalone lifecycle
// client for the origin-gate hardening tests (OG02/OG03/OG04). It is compiled
// explicitly by the drill test and spawned as a genuine owned OS process:
//
//   - sequence: a genuine SCRAM frontend that sends one INSERT per stdin line,
//     letting the test drive several executable frames across the registration
//     release transition to prove exact-once ordered forwarding;
//   - fork: dials the endpoint and then forks a child that inherits the exact
//     connected socket descriptor, so the gate's authoritative FD-owner census
//     must refuse the copied process;
//   - holdfd: the inherited-descriptor child; it holds the descriptor until
//     killed.
//
// The credential is only ever read from private stdin; public stdout carries
// PIDs, refs and protocol state, never credential material.
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
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	lifecycleMaxMessage = 1 << 20
	lifecycleStartup    = 196608
	lifecycleDatabase   = "txharbor"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: lifecycle-client sequence|fork|holdfd ...")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "sequence":
		runSequence(os.Args)
	case "fork":
		runFork(os.Args)
	case "holdfd":
		runHoldFD(os.Args)
	case "forkmid":
		runForkMid()
	default:
		os.Exit(2)
	}
}

func readSecret() (string, error) {
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}

type lifecycleClient struct {
	conn     net.Conn
	reader   *bufio.Reader
	role     string
	password string
	appname  string
}

func (c *lifecycleClient) deadline(d time.Duration) { _ = c.conn.SetDeadline(time.Now().Add(d)) }

func (c *lifecycleClient) writeFrontend(typ byte, body []byte) error {
	packet := make([]byte, 5+len(body))
	packet[0] = typ
	binary.BigEndian.PutUint32(packet[1:5], uint32(4+len(body)))
	copy(packet[5:], body)
	c.deadline(30 * time.Second)
	_, err := c.conn.Write(packet)
	return err
}

func (c *lifecycleClient) readBackend() (byte, []byte, error) {
	typ, err := c.reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	var header [4]byte
	if _, err := io.ReadFull(c.reader, header[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n < 4 || n > lifecycleMaxMessage {
		return 0, nil, fmt.Errorf("invalid backend length %d", n)
	}
	body := make([]byte, int(n)-4)
	if _, err := io.ReadFull(c.reader, body); err != nil {
		return 0, nil, err
	}
	return typ, body, nil
}

func (c *lifecycleClient) startup() error {
	params := []byte("user\x00" + c.role + "\x00database\x00" + lifecycleDatabase +
		"\x00application_name\x00" + c.appname + "\x00client_encoding\x00UTF8\x00\x00")
	packet := make([]byte, 8+len(params))
	binary.BigEndian.PutUint32(packet[:4], uint32(len(packet)))
	binary.BigEndian.PutUint32(packet[4:8], lifecycleStartup)
	copy(packet[8:], params)
	c.deadline(30 * time.Second)
	_, err := c.conn.Write(packet)
	return err
}

func (c *lifecycleClient) scram() error {
	c.deadline(45 * time.Second)
	typ, body, err := c.readBackend()
	if err != nil {
		return err
	}
	if typ != 'R' || len(body) < 4 || binary.BigEndian.Uint32(body[:4]) != 10 {
		return fmt.Errorf("expected AuthenticationSASL, got type=%q", typ)
	}
	var nonce [18]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	clientNonce := base64.RawStdEncoding.EncodeToString(nonce[:])
	clientBare := "n=,r=" + clientNonce
	clientFirst := "n,," + clientBare
	initial := append([]byte("SCRAM-SHA-256\x00"), make([]byte, 4)...)
	binary.BigEndian.PutUint32(initial[len("SCRAM-SHA-256\x00"):], uint32(len(clientFirst)))
	initial = append(initial, []byte(clientFirst)...)
	if err := c.writeFrontend('p', initial); err != nil {
		return err
	}
	typ, body, err = c.readBackend()
	if err != nil {
		return err
	}
	if typ != 'R' || len(body) < 4 || binary.BigEndian.Uint32(body[:4]) != 11 {
		return fmt.Errorf("expected AuthenticationSASLContinue, got type=%q", typ)
	}
	serverFirst := string(body[4:])
	serverNonce := scramAttribute(serverFirst, 'r')
	salt, err := base64.StdEncoding.DecodeString(scramAttribute(serverFirst, 's'))
	if err != nil {
		return err
	}
	iterations, err := strconv.Atoi(scramAttribute(serverFirst, 'i'))
	if err != nil || iterations < 1 || iterations > 1_000_000 {
		return fmt.Errorf("invalid SCRAM iteration count")
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
		return err
	}
	for {
		typ, body, err = c.readBackend()
		if err != nil {
			return err
		}
		if typ != 'R' || len(body) < 4 {
			continue
		}
		if binary.BigEndian.Uint32(body[:4]) == 0 {
			return nil
		}
	}
}

func runSequence(args []string) {
	if len(args) != 6 {
		fmt.Fprintln(os.Stderr, "usage: lifecycle-client sequence <addr> <role> <table> <appname>")
		os.Exit(2)
	}
	address, role, table, appname := args[2], args[3], args[4], args[5]
	password, err := readSecret()
	if err != nil {
		fmt.Println("ORIGIN_LIFECYCLE_EXIT state=CREDENTIAL_ERROR")
		os.Exit(3)
	}
	conn, err := net.DialTimeout("tcp", address, 10*time.Second)
	if err != nil {
		fmt.Printf("ORIGIN_LIFECYCLE_EXIT state=TRANSPORT_ERROR detail=%q\n", err.Error())
		os.Exit(3)
	}
	defer conn.Close()
	local := conn.LocalAddr().(*net.TCPAddr)
	fmt.Printf("ORIGIN_LIFECYCLE_READY pid=%d local_ip=%s local_port=%d\n", os.Getpid(), local.IP.String(), local.Port)
	client := &lifecycleClient{conn: conn, reader: bufio.NewReader(conn), role: role, password: password, appname: appname}
	if err := client.startup(); err != nil {
		fmt.Printf("ORIGIN_LIFECYCLE_EXIT state=TRANSPORT_ERROR detail=%q\n", err.Error())
		os.Exit(3)
	}
	if err := client.scram(); err != nil {
		fmt.Printf("ORIGIN_LIFECYCLE_EXIT state=TRANSPORT_ERROR detail=%q\n", err.Error())
		os.Exit(3)
	}
	fmt.Println("ORIGIN_LIFECYCLE_AUTH ok=1")
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			ref := scanner.Text()
			if strings.TrimSpace(ref) == "quit" {
				_ = client.writeFrontend('X', nil)
				fmt.Println("ORIGIN_LIFECYCLE_EXIT state=QUIT")
				os.Exit(0)
			}
			if strings.TrimSpace(ref) == "forkfd" {
				if err := forkLiveSocket(client); err != nil {
					fmt.Printf("ORIGIN_LIFECYCLE_ERROR forkfd=%q\n", err.Error())
				}
				continue
			}
			if strings.TrimSpace(ref) == "orphanfd" {
				if err := orphanForkLiveSocket(client); err != nil {
					fmt.Printf("ORIGIN_LIFECYCLE_ERROR orphanfd=%q\n", err.Error())
				}
				continue
			}
			statement := "INSERT INTO " + table + "(ref) VALUES ('" + strings.ReplaceAll(ref, "'", "''") + "')"
			if err := client.writeFrontend('Q', append([]byte(statement), 0)); err != nil {
				fmt.Printf("ORIGIN_LIFECYCLE_EXIT state=TRANSPORT_ERROR detail=%q\n", err.Error())
				os.Exit(3)
			}
			fmt.Printf("ORIGIN_LIFECYCLE_SENT ref=%s\n", ref)
		}
	}()
	for {
		typ, body, err := client.readBackend()
		if err != nil {
			fmt.Println("ORIGIN_LIFECYCLE_EXIT state=SERVER_EOF")
			os.Exit(0)
		}
		switch typ {
		case 'C':
			fmt.Printf("ORIGIN_LIFECYCLE_COMMAND tag=%q\n", string(body))
		case 'E':
			fmt.Printf("ORIGIN_LIFECYCLE_ERROR sqlstate=%s\n", errorSQLState(body))
		}
	}
}

func runFork(args []string) {
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: lifecycle-client fork <addr>")
		os.Exit(2)
	}
	conn, err := net.DialTimeout("tcp", args[2], 10*time.Second)
	if err != nil {
		fmt.Printf("ORIGIN_LIFECYCLE_EXIT state=TRANSPORT_ERROR detail=%q\n", err.Error())
		os.Exit(3)
	}
	file, err := conn.(*net.TCPConn).File()
	if err != nil {
		fmt.Printf("ORIGIN_LIFECYCLE_EXIT state=TRANSPORT_ERROR detail=%q\n", err.Error())
		os.Exit(3)
	}
	child := exec.Command(os.Args[0], "holdfd")
	child.ExtraFiles = []*os.File{file}
	child.Stdin = nil
	child.Stdout = nil
	child.Stderr = nil
	if err := child.Start(); err != nil {
		fmt.Printf("ORIGIN_LIFECYCLE_EXIT state=FORK_ERROR detail=%q\n", err.Error())
		os.Exit(3)
	}
	_ = file.Close()
	fmt.Printf("ORIGIN_LIFECYCLE_FORK parent=%d child=%d\n", os.Getpid(), child.Process.Pid)
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil || strings.TrimSpace(line) == "quit" {
		_ = child.Process.Kill()
		_, _ = child.Process.Wait()
		fmt.Println("ORIGIN_LIFECYCLE_EXIT state=QUIT")
		os.Exit(0)
	}
}

func runHoldFD(args []string) {
	if len(args) > 2 && args[2] == "dumpable" {
		// PR_SET_DUMPABLE 0 makes this process's fd table unreadable to any
		// reader without CAP_SYS_PTRACE. The setter result and the read-back
		// are both checked and reported explicitly; a failure is fatal.
		if _, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, uintptr(4), 0, 0, 0, 0, 0); errno != 0 {
			fmt.Printf("ORIGIN_LIFECYCLE_DUMPABLE_ERROR phase=set errno=%d\n", int(errno))
			os.Exit(3)
		}
		got, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, uintptr(3), 0, 0, 0, 0, 0) // PR_GET_DUMPABLE = 3
		if errno != 0 || int(got) != 0 {
			fmt.Printf("ORIGIN_LIFECYCLE_DUMPABLE_ERROR phase=get errno=%d value=%d\n", int(errno), int(got))
			os.Exit(3)
		}
		fmt.Printf("ORIGIN_LIFECYCLE_DUMPABLE pid=%d set=0 get=%d\n", os.Getpid(), int(got))
	}
	for {
		time.Sleep(time.Hour)
	}
}

// orphanForkLiveSocket is the double-fork orphan scenario: an intermediate
// child receives a duplicated live socket descriptor and exits (reaped by its
// original parent with a captured Wait), while the grandchild keeps the
// descriptor and is reparented, with dumpable=0 so its fd table is
// authentically unreadable.
func orphanForkLiveSocket(client *lifecycleClient) error {
	tcp, ok := client.conn.(*net.TCPConn)
	if !ok {
		return fmt.Errorf("connection is not TCP")
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return err
	}
	dupFD := -1
	if err := raw.Control(func(fd uintptr) {
		dupFD, _ = syscall.Dup(int(fd))
	}); err != nil {
		return err
	}
	if dupFD < 0 {
		return fmt.Errorf("duplicate socket descriptor failed")
	}
	inherited := os.NewFile(uintptr(dupFD), "inherited-socket")
	intermediate := exec.Command(os.Args[0], "forkmid")
	intermediate.ExtraFiles = []*os.File{inherited}
	intermediate.Stdout = os.Stdout
	intermediate.Stderr = os.Stderr
	if err := intermediate.Start(); err != nil {
		_ = inherited.Close()
		return err
	}
	_ = inherited.Close()
	fmt.Printf("ORIGIN_LIFECYCLE_ORPHAN parent=%d intermediate=%d\n", os.Getpid(), intermediate.Process.Pid)
	go func() {
		err := intermediate.Wait() // the original parent captures the intermediate Wait
		if err != nil {
			fmt.Printf("ORIGIN_LIFECYCLE_MID_EXIT pid=%d status=error detail=%q\n", intermediate.Process.Pid, err.Error())
			return
		}
		fmt.Printf("ORIGIN_LIFECYCLE_MID_EXIT pid=%d status=0\n", intermediate.Process.Pid)
	}()
	return nil
}

// runForkMid is the exiting intermediate: it passes the inherited live socket
// descriptor to its own child and exits immediately, so the child is
// reparented while the original owner stays alive.
func runForkMid() {
	inherited := os.NewFile(3, "inherited-socket")
	if inherited == nil {
		fmt.Println("ORIGIN_LIFECYCLE_ERROR forkmid=no-inherited-fd")
		os.Exit(3)
	}
	grandchild := exec.Command(os.Args[0], "holdfd", "dumpable")
	grandchild.ExtraFiles = []*os.File{inherited}
	grandchild.Stdin = nil
	grandchild.Stdout = os.Stdout
	grandchild.Stderr = os.Stderr
	if err := grandchild.Start(); err != nil {
		fmt.Printf("ORIGIN_LIFECYCLE_ERROR forkmid=%q\n", err.Error())
		os.Exit(3)
	}
	_ = inherited.Close()
	start := ""
	if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", grandchild.Process.Pid)); err == nil {
		line := string(data)
		if end := strings.LastIndex(line, ") "); end >= 0 && end+2 < len(line) {
			fields := strings.Fields(line[end+2:])
			if len(fields) >= 20 {
				start = fields[19]
			}
		}
	}
	fmt.Printf("ORIGIN_LIFECYCLE_GRANDCHILD pid=%d start=%s\n", grandchild.Process.Pid, start)
	os.Exit(0)
}

// forkLiveSocket duplicates the live connected socket descriptor into a child
// process while the parent keeps serving, modelling a post-registration
// inheritance transfer that the gate's FD-owner census must refuse.
func forkLiveSocket(client *lifecycleClient) error {
	tcp, ok := client.conn.(*net.TCPConn)
	if !ok {
		return fmt.Errorf("connection is not TCP")
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return err
	}
	dupFD := -1
	if err := raw.Control(func(fd uintptr) {
		dupFD, _ = syscall.Dup(int(fd))
	}); err != nil {
		return err
	}
	if dupFD < 0 {
		return fmt.Errorf("duplicate socket descriptor failed")
	}
	inherited := os.NewFile(uintptr(dupFD), "inherited-socket")
	child := exec.Command(os.Args[0], "holdfd")
	child.ExtraFiles = []*os.File{inherited}
	child.Stdin, child.Stdout, child.Stderr = nil, nil, nil
	if err := child.Start(); err != nil {
		_ = inherited.Close()
		return err
	}
	_ = inherited.Close()
	fmt.Printf("ORIGIN_LIFECYCLE_FORK parent=%d child=%d trigger=post-registration\n", os.Getpid(), child.Process.Pid)
	return nil
}

func errorSQLState(body []byte) string {
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
	if state := fields['C']; state != "" {
		return state
	}
	return "unknown"
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
