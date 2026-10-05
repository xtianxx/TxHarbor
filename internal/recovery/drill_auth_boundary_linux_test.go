//go:build linux && drill

package recovery_test

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	authBoundaryPGImage = "postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280"
	authBoundaryPGPort  = "5432/tcp"
	passwordP0          = "drill-only-P0-9f7a1c"
	passwordP1          = "drill-only-P1-2b8e4d"
	// Staged two-stage own-auth prototype evidence. The first prototype runs
	// are archived immutably under docs/evidence/015/restart-auth-prototype;
	// this path is the fresh staged run ledger and is never used to rewrite
	// those archived records.
	boundaryEvidence = "/tmp/opencode/015-e504462/staged-auth-feasibility.jsonl"
	// Explicit protected init HBA: every transport an old writer could reach
	// (local socket, loopback IPv4/IPv6, container-address TCP) requires
	// scram-sha-256. No auth fallback exists and the actual rules are asserted
	// from pg_hba_file_rules.
	authBoundaryProtectedHBA = "local all all scram-sha-256\nhost all all 127.0.0.1/32 scram-sha-256\nhost all all ::1/128 scram-sha-256\nhost all all all scram-sha-256\n"
)

type authBoundaryEvidenceRecord struct {
	Time   string `json:"time"`
	Test   string `json:"test"`
	Result string `json:"result"`
	Detail string `json:"detail,omitempty"`
}

func recordAuthBoundaryResult(t *testing.T, name string, detail *string) {
	t.Helper()
	t.Cleanup(func() {
		if err := os.MkdirAll(filepath.Dir(boundaryEvidence), 0o700); err != nil {
			t.Errorf("preserve auth-boundary run evidence: %v", err)
			return
		}
		f, err := os.OpenFile(boundaryEvidence, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			t.Errorf("preserve auth-boundary run evidence: %v", err)
			return
		}
		result := "PASS"
		if t.Failed() {
			result = "FAIL"
		}
		text := ""
		if detail != nil {
			text = *detail
		}
		_ = json.NewEncoder(f).Encode(authBoundaryEvidenceRecord{
			Time: time.Now().UTC().Format(time.RFC3339Nano), Test: name,
			Result: result, Detail: text,
		})
		if err := f.Close(); err != nil {
			t.Errorf("close auth-boundary evidence: %v", err)
		}
	})
}

func startAuthBoundaryPG(t *testing.T) (string, string) {
	t.Helper()
	// All auth-boundary fixtures, including the rotation prototype, use the
	// explicit protected HBA with no trust fallback.
	ctr, dsn := startAuthBoundaryPGWithPostmasterChildContainer(t)
	return ctr.GetContainerID(), dsn
}

// startAuthBoundaryPGWithPostmasterChild leaves a tiny shell as container PID
// 1 and runs the official PostgreSQL entrypoint as its child. Linux prevents
// SIGSTOP from stopping namespace PID 1; this arrangement lets the test stop
// only the real postmaster PID while keeping the container and Docker exec
// control path alive.
func startAuthBoundaryPGWithPostmasterChild(t *testing.T) (string, string) {
	ctr, dsn := startAuthBoundaryPGWithPostmasterChildContainer(t)
	return ctr.GetContainerID(), dsn
}

func startAuthBoundaryPGWithPostmasterChildContainer(t *testing.T) (testcontainers.Container, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        authBoundaryPGImage,
			CapAdd:       []string{"SYS_PTRACE"},
			Entrypoint:   []string{"/bin/sh", "-c"},
			Cmd:          []string{"printf '%s' '" + authBoundaryProtectedHBA + "' > /tmp/protected_pg_hba.conf && exec /usr/local/bin/docker-entrypoint.sh postgres -c hba_file=/tmp/protected_pg_hba.conf & wait"},
			Env:          map[string]string{"POSTGRES_DB": "txharbor", "POSTGRES_USER": "txharbor", "POSTGRES_PASSWORD": "txharbor", "POSTGRES_HOST_AUTH_METHOD": "scram-sha-256"},
			ExposedPorts: []string{authBoundaryPGPort},
			WaitingFor:   wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start disposable PostgreSQL 18.6 with non-PID1 postmaster: %v", err)
	}
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanCancel()
		if err := ctr.Terminate(cleanCtx); err != nil {
			t.Errorf("terminate disposable PostgreSQL: %v", err)
		}
	})
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("get disposable PostgreSQL host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, authBoundaryPGPort)
	if err != nil {
		t.Fatalf("get disposable PostgreSQL port: %v", err)
	}
	dsn := "postgres://txharbor:txharbor@" + net.JoinHostPort(host, port.Port()) + "/txharbor?sslmode=disable"
	return ctr, dsn
}

func requireNativePG18(t *testing.T, dsn string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect native PostgreSQL fixture: %v", err)
	}
	defer conn.Close(context.Background())
	var version int
	if err := conn.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&version); err != nil {
		t.Fatalf("read PostgreSQL version: %v", err)
	}
	if version < 180000 || version >= 190000 {
		t.Fatalf("native PostgreSQL version number %d is not PG18", version)
	}
}

func sqlLiteral(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }

func TestDrillSCRAMLoadedVerifierSurvivesPasswordRotation(t *testing.T) {
	detail := "attempting raw PostgreSQL SCRAM handshake paused at AuthenticationSASLContinue"
	recordAuthBoundaryResult(t, "TestDrillSCRAMLoadedVerifierSurvivesPasswordRotation", &detail)
	_, adminDSN := startAuthBoundaryPG(t)
	requireNativePG18(t, adminDSN)
	adminCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(adminCtx, adminDSN)
	if err != nil {
		t.Fatalf("connect PostgreSQL administrator: %v", err)
	}
	defer admin.Close(context.Background())
	role := fmt.Sprintf("drill_scram_%d", time.Now().UnixNano())
	if _, err := admin.Exec(adminCtx, `CREATE ROLE `+pgx.Identifier{role}.Sanitize()+` LOGIN PASSWORD `+sqlLiteral(passwordP0)); err != nil {
		t.Fatalf("create SCRAM role: %v", err)
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+pgx.Identifier{role}.Sanitize())
	}()
	var verifier string
	if err := admin.QueryRow(adminCtx, `SELECT rolpassword FROM pg_authid WHERE rolname=$1`, role).Scan(&verifier); err != nil {
		t.Fatalf("read fixture verifier: %v", err)
	}
	if !strings.HasPrefix(verifier, "SCRAM-SHA-256$") {
		t.Fatalf("server did not store a SCRAM verifier (prefix %q)", verifierPrefix(verifier))
	}

	client, err := beginRawSCRAM(adminCtx, adminDSN, role, passwordP0)
	if err != nil {
		t.Fatalf("pause raw client at AuthenticationSASLContinue: %v", err)
	}
	defer client.conn.Close()
	if client.phase != "SASLContinue" || client.serverFirst == "" {
		t.Fatalf("raw SCRAM client did not stop after real SASLContinue: phase=%s", client.phase)
	}
	if _, err := admin.Exec(adminCtx, `ALTER ROLE `+pgx.Identifier{role}.Sanitize()+` PASSWORD `+sqlLiteral(passwordP1)); err != nil {
		t.Fatalf("rotate SCRAM role to P1: %v", err)
	}

	value, err := client.finishAndSelectOne(adminCtx)
	if err != nil {
		t.Fatalf("original P0 SCRAM exchange did not finish against loaded verifier: %v", err)
	}
	if value != "1" {
		t.Fatalf("P0 authenticated query returned %q, want 1", value)
	}
	if err := assertRawPasswordResult(adminCtx, adminDSN, role, passwordP0, false); err != nil {
		t.Fatalf("new P0 connection was not rejected after rotation: %v", err)
	}
	if err := assertRawPasswordResult(adminCtx, adminDSN, role, passwordP1, true); err != nil {
		t.Fatalf("new P1 connection did not authenticate and SELECT 1: %v", err)
	}
	detail = "native PG18 accepted the original in-flight P0 SCRAM proof after committed P0-to-P1 rotation; fresh P0 rejected and fresh P1 succeeded (prototype only, not a drain/fence proof)"
}

func verifierPrefix(value string) string {
	if len(value) > 16 {
		return value[:16]
	}
	return value
}

type rawSCRAM struct {
	conn        net.Conn
	reader      *bufio.Reader
	role        string
	password    string
	clientBare  string
	serverFirst string
	phase       string
	authOK      bool
	backendPID  int
}

func beginRawSCRAM(ctx context.Context, dsn, role, password string) (*rawSCRAM, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", u.Host)
	if err != nil {
		return nil, err
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = conn.Close()
		}
	}()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	}
	startup := []byte("user\x00" + role + "\x00database\x00" + pathDB(u) + "\x00client_encoding\x00UTF8\x00\x00")
	packet := make([]byte, 8+len(startup))
	binary.BigEndian.PutUint32(packet[:4], uint32(len(packet)))
	binary.BigEndian.PutUint32(packet[4:8], 196608)
	copy(packet[8:], startup)
	if _, err := conn.Write(packet); err != nil {
		return nil, err
	}
	r := &rawSCRAM{conn: conn, reader: bufio.NewReader(conn), role: role, password: password}
	typ, body, err := r.readMessage()
	if err != nil {
		return nil, err
	}
	if typ != 'R' || len(body) < 4 || binary.BigEndian.Uint32(body[:4]) != 10 {
		return nil, fmt.Errorf("expected AuthenticationSASL, received type=%q body=%x", typ, body)
	}
	if !bytesHasMechanism(body[4:], "SCRAM-SHA-256") {
		return nil, errors.New("server did not offer SCRAM-SHA-256")
	}
	var nonce [18]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	clientNonce := base64.RawStdEncoding.EncodeToString(nonce[:])
	r.clientBare = "n=,r=" + clientNonce
	first := "n,," + r.clientBare
	if err := r.writePasswordMessage(append([]byte("SCRAM-SHA-256\x00"), appendUint32AndBytes(0, []byte(first))...)); err != nil {
		return nil, err
	}
	typ, body, err = r.readMessage()
	if err != nil {
		return nil, err
	}
	if typ != 'R' || len(body) < 4 || binary.BigEndian.Uint32(body[:4]) != 11 {
		return nil, fmt.Errorf("expected AuthenticationSASLContinue, received type=%q body=%x", typ, body)
	}
	r.serverFirst = string(body[4:])
	serverNonce := scramAttribute(r.serverFirst, 'r')
	if !strings.HasPrefix(serverNonce, clientNonce) || serverNonce == clientNonce {
		return nil, errors.New("invalid SCRAM server nonce")
	}
	r.phase = "SASLContinue"
	closeOnError = false
	return r, nil
}

func pathDB(u *url.URL) string {
	name := strings.TrimPrefix(u.Path, "/")
	if name == "" {
		return "postgres"
	}
	return name
}

func bytesHasMechanism(raw []byte, wanted string) bool {
	for _, item := range strings.Split(string(raw), "\x00") {
		if item == wanted {
			return true
		}
	}
	return false
}

func appendUint32AndBytes(_ uint32, raw []byte) []byte {
	out := make([]byte, 4+len(raw))
	binary.BigEndian.PutUint32(out[:4], uint32(len(raw)))
	copy(out[4:], raw)
	return out
}

func scramAttribute(message string, key byte) string {
	for _, item := range strings.Split(message, ",") {
		if len(item) > 2 && item[0] == key && item[1] == '=' {
			return item[2:]
		}
	}
	return ""
}

func (r *rawSCRAM) finishAndSelectOne(ctx context.Context) (string, error) {
	salt, err := base64.StdEncoding.DecodeString(scramAttribute(r.serverFirst, 's'))
	if err != nil {
		return "", fmt.Errorf("decode SCRAM salt: %w", err)
	}
	iterations, err := strconv.Atoi(scramAttribute(r.serverFirst, 'i'))
	if err != nil || iterations < 1 || iterations > 1_000_000 {
		return "", errors.New("invalid SCRAM iteration count")
	}
	nonce := scramAttribute(r.serverFirst, 'r')
	withoutProof := "c=biws,r=" + nonce
	authMessage := r.clientBare + "," + r.serverFirst + "," + withoutProof
	salted := pbkdf2SHA256([]byte(r.password), salt, iterations, sha256.Size)
	clientKey := scramHMAC(salted, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	clientSignature := scramHMAC(storedKey[:], []byte(authMessage))
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ clientSignature[i]
	}
	final := withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)
	if err := r.writePasswordMessage([]byte(final)); err != nil {
		return "", err
	}
	typ, body, err := r.readMessage()
	if err != nil {
		return "", err
	}
	if typ != 'R' || len(body) < 4 || binary.BigEndian.Uint32(body[:4]) != 12 {
		if typ == 'E' {
			return "", serverAuthErrorFrom(body)
		}
		return "", fmt.Errorf("expected AuthenticationSASLFinal, received type=%q", typ)
	}
	serverFinal := string(body[4:])
	serverKey := scramHMAC(salted, []byte("Server Key"))
	expected := base64.StdEncoding.EncodeToString(scramHMAC(serverKey, []byte(authMessage)))
	if scramAttribute(serverFinal, 'v') != expected {
		return "", errors.New("server SCRAM signature mismatch")
	}
	for {
		typ, body, err = r.readMessage()
		if err != nil {
			return "", err
		}
		if typ == 'R' && len(body) >= 4 {
			switch binary.BigEndian.Uint32(body[:4]) {
			case 0:
				r.authOK = true
				continue
			case 12:
				continue
			}
		}
		if typ == 'K' && len(body) >= 8 {
			r.backendPID = int(binary.BigEndian.Uint32(body[:4]))
			continue
		}
		if typ == 'Z' {
			break
		}
		if typ == 'E' {
			return "", serverAuthErrorFrom(body)
		}
	}
	if err := r.writeFrontend('Q', append([]byte("SELECT 1"), 0)); err != nil {
		return "", err
	}
	var value string
	for {
		typ, body, err = r.readMessage()
		if err != nil {
			return "", err
		}
		switch typ {
		case 'D':
			if len(body) < 6 || binary.BigEndian.Uint16(body[:2]) != 1 {
				return "", errors.New("unexpected SELECT 1 data row")
			}
			n := int32(binary.BigEndian.Uint32(body[2:6]))
			if n < 0 || int(n) > len(body)-6 {
				return "", errors.New("invalid SELECT 1 data length")
			}
			value = string(body[6 : 6+n])
		case 'E':
			return "", fmt.Errorf("SELECT 1 failed: %s", serverError(body))
		case 'Z':
			return value, nil
		}
	}
}

func (r *rawSCRAM) writePasswordMessage(body []byte) error { return r.writeFrontend('p', body) }
func (r *rawSCRAM) writeFrontend(typ byte, body []byte) error {
	packet := make([]byte, 5+len(body))
	packet[0] = typ
	binary.BigEndian.PutUint32(packet[1:5], uint32(4+len(body)))
	copy(packet[5:], body)
	_, err := r.conn.Write(packet)
	return err
}

func (r *rawSCRAM) readMessage() (byte, []byte, error) {
	typ, err := r.reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	var lenbuf [4]byte
	if _, err := io.ReadFull(r.reader, lenbuf[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(lenbuf[:])
	if n < 4 || n > 1<<20 {
		return 0, nil, fmt.Errorf("invalid PostgreSQL message length %d", n)
	}
	body := make([]byte, int(n)-4)
	if _, err := io.ReadFull(r.reader, body); err != nil {
		return 0, nil, err
	}
	return typ, body, nil
}

func scramHMAC(key, message []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(message)
	return h.Sum(nil)
}

func pbkdf2SHA256(password, salt []byte, iterations, length int) []byte {
	var block [4]byte
	binary.BigEndian.PutUint32(block[:], 1)
	u := scramHMAC(password, append(append([]byte(nil), salt...), block[:]...))
	out := append([]byte(nil), u...)
	for i := 1; i < iterations; i++ {
		u = scramHMAC(password, u)
		for j := range out {
			out[j] ^= u[j]
		}
	}
	return out[:length]
}

// serverAuthError is a typed PostgreSQL ErrorResponse for authentication
// outcomes. Only this type counts as a credential verdict; transport,
// protocol and query errors are deliberately not classified as rejection.
type serverAuthError struct {
	SQLState string
	Message  string
}

func (e *serverAuthError) Error() string {
	return fmt.Sprintf("server authentication rejection: sqlstate=%s message=%q", e.SQLState, e.Message)
}

func serverErrorFields(body []byte) map[byte]string {
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

func serverAuthErrorFrom(body []byte) *serverAuthError {
	fields := serverErrorFields(body)
	return &serverAuthError{SQLState: fields['C'], Message: fields['M']}
}

func serverError(body []byte) string {
	fields := serverErrorFields(body)
	if fields['M'] == "" {
		return fields['S']
	}
	return fields['M']
}

// assertRawPasswordResult rejects a wrong credential only when the actual
// server returned a genuine authentication ErrorResponse with SQLSTATE 28P01
// and no AuthenticationOk was ever observed. A transport or query error is
// reported as such and never counted as a credential rejection.
func assertRawPasswordResult(ctx context.Context, dsn, role, password string, wantSuccess bool) error {
	client, err := beginRawSCRAM(ctx, dsn, role, password)
	if err != nil {
		return fmt.Errorf("raw SCRAM handshake did not reach SASLContinue (transport/protocol class, not a credential verdict): %w", err)
	}
	defer client.conn.Close()
	_, err = client.finishAndSelectOne(ctx)
	if wantSuccess {
		if err != nil {
			return fmt.Errorf("expected accepted credential: %w", err)
		}
		if !client.authOK {
			return errors.New("accepted credential path never observed AuthenticationOk")
		}
		return nil
	}
	var rejection *serverAuthError
	if !errors.As(err, &rejection) {
		if err == nil {
			return errors.New("wrong credential authenticated and SELECT 1 succeeded")
		}
		return fmt.Errorf("wrong credential was not rejected by server authentication (transport/query/protocol error is not a credential verdict): %w", err)
	}
	if rejection.SQLState != "28P01" {
		return fmt.Errorf("wrong credential rejection had SQLSTATE %q, want 28P01: %v", rejection.SQLState, rejection)
	}
	if client.authOK {
		return errors.New("server issued AuthenticationOk before rejecting the wrong credential")
	}
	return nil
}

// generateSCRAMVerifier derives a complete SCRAM-SHA-256 verifier string on
// the client side so the staged prototype can assert the exact stored
// rolpassword after ALTER ROLE instead of treating "it changed" as proof. The
// verifier is never logged.
func generateSCRAMVerifier(password string, iterations int) (string, error) {
	if iterations < 1 {
		return "", errors.New("invalid SCRAM iteration count")
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	salted := pbkdf2SHA256([]byte(password), salt, iterations, sha256.Size)
	clientKey := scramHMAC(salted, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	serverKey := scramHMAC(salted, []byte("Server Key"))
	return fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s", iterations,
		base64.StdEncoding.EncodeToString(salt),
		base64.StdEncoding.EncodeToString(storedKey[:]),
		base64.StdEncoding.EncodeToString(serverKey)), nil
}

// assertProtectedScramOnlyHBA reads the actual pg_hba_file_rules of the
// protected fixture and fails unless every rule requires scram-sha-256 and the
// local socket plus loopback IPv4/IPv6 transports are explicitly present. The
// fixture starts with an explicit hba_file override; no trust fallback may
// exist on any transport an old writer could reach.
func assertProtectedScramOnlyHBA(t *testing.T, ctx context.Context, admin *pgx.Conn) {
	t.Helper()
	rows, err := admin.Query(ctx, `SELECT line_number, type, coalesce(address,''), auth_method FROM pg_hba_file_rules ORDER BY line_number`)
	if err != nil {
		t.Fatalf("read protected fixture actual HBA rules: %v", err)
	}
	defer rows.Close()
	var lines []string
	localSeen, loopback4, loopback6 := false, false, false
	for rows.Next() {
		var line int
		var typ, address, method string
		if err := rows.Scan(&line, &typ, &address, &method); err != nil {
			t.Fatalf("scan protected fixture HBA rule: %v", err)
		}
		lines = append(lines, fmt.Sprintf("line=%d type=%s address=%q auth=%s", line, typ, address, method))
		if method != "scram-sha-256" {
			t.Fatalf("protected fixture HBA line %d does not require SCRAM (auth=%s); all reachable transports must require SCRAM: %s", line, method, strings.Join(lines, "; "))
		}
		switch {
		case typ == "local":
			localSeen = true
		case typ == "host" && strings.HasPrefix(address, "127.0.0.1"):
			loopback4 = true
		case typ == "host" && strings.HasPrefix(address, "::1"):
			loopback6 = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read protected fixture actual HBA rules: %v", err)
	}
	if !localSeen || !loopback4 || !loopback6 {
		t.Fatalf("protected fixture actual HBA rules are missing an explicit local/loopback rule: %s", strings.Join(lines, "; "))
	}
	t.Logf("protected fixture actual HBA rules (all SCRAM, no trust fallback): %s", strings.Join(lines, "; "))
}

func TestDrillPostmasterStopResumeRetainsControlLock(t *testing.T) {
	detail := "attempting native postmaster stop/resume while existing control backend owns session advisory lock"
	recordAuthBoundaryResult(t, "TestDrillPostmasterStopResumeRetainsControlLock", &detail)
	container, dsn := startAuthBoundaryPGWithPostmasterChildContainer(t)
	containerID := container.GetContainerID()
	requireNativePG18(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	controlDSN := dsnForDB(t, dsn, "txharbor")
	control, err := pgx.Connect(ctx, controlDSN)
	if err != nil {
		t.Fatalf("open retained control backend: %v", err)
	}
	defer control.Close(context.Background())
	const key1, key2 = 71001, 71002
	var ownerPID int
	if err := control.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&ownerPID); err != nil {
		t.Fatalf("read lock owner PID: %v", err)
	}
	if _, err := control.Exec(ctx, `SELECT pg_advisory_lock($1::int, $2::int)`, key1, key2); err != nil {
		t.Fatalf("acquire real control advisory lock: %v", err)
	}
	locked := true
	defer func() {
		if locked {
			unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer unlockCancel()
			_, _ = control.Exec(unlockCtx, `SELECT pg_advisory_unlock($1::int, $2::int)`, key1, key2)
		}
	}()
	if err := assertOwnedAdvisoryLock(ctx, control, ownerPID, key1, key2); err != nil {
		t.Fatal(err)
	}

	postmasterPID, startIdentity, err := inspectPostmaster(ctx, containerID)
	if err != nil {
		t.Fatalf("inspect actual postmaster identity: %v", err)
	}
	if err := signalPostmaster(ctx, containerID, postmasterPID, startIdentity, "STOP"); err != nil {
		t.Fatalf("SIGSTOP exact postmaster: %v", err)
	}
	resumed := false
	resume := func() error {
		if resumed {
			return nil
		}
		resumeCtx, resumeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer resumeCancel()
		err := signalPostmaster(resumeCtx, containerID, postmasterPID, startIdentity, "CONT")
		if err == nil {
			resumed = true
		}
		return err
	}
	defer func() {
		if err := resume(); err != nil {
			t.Errorf("guaranteed postmaster resume failed: %v", err)
		}
	}()

	if _, err := awaitProcessState(ctx, containerID, postmasterPID, startIdentity, "T", "t"); err != nil {
		t.Fatalf("confirm postmaster stopped: %v", err)
	}
	var observedPID int
	queryCtx, queryCancel := context.WithTimeout(ctx, 5*time.Second)
	err = control.QueryRow(queryCtx, `SELECT pg_backend_pid()`).Scan(&observedPID)
	queryCancel()
	if err != nil {
		t.Fatalf("existing control backend did not respond while postmaster stopped: %v", err)
	}
	if observedPID != ownerPID {
		t.Fatalf("control backend PID changed from %d to %d", ownerPID, observedPID)
	}
	if err := assertOwnedAdvisoryLock(ctx, control, ownerPID, key1, key2); err != nil {
		t.Fatalf("lock not retained while postmaster stopped: %v", err)
	}
	state, stateErr := processState(ctx, containerID, postmasterPID, startIdentity)
	if stateErr != nil || (state != "T" && state != "t") {
		t.Fatalf("postmaster identity/state changed during observer query: state=%q err=%v", state, stateErr)
	}
	if err := resume(); err != nil {
		t.Fatalf("resume exact postmaster: %v", err)
	}
	if err := awaitNativePGReady(ctx, dsn); err != nil {
		t.Fatalf("native server did not recover after SIGCONT: %v", err)
	}
	if err := assertOwnedAdvisoryLock(ctx, control, ownerPID, key1, key2); err != nil {
		t.Fatalf("same lock owner/key did not survive resume: %v", err)
	}
	if err := assertRawPasswordResult(ctx, dsn, "txharbor", "txharbor", true); err != nil {
		t.Fatalf("new native connection failed after resume: %v", err)
	}
	if _, err := control.Exec(ctx, `SELECT pg_advisory_unlock($1::int, $2::int)`, key1, key2); err != nil {
		t.Fatalf("release original lock: %v", err)
	}
	locked = false
	detail = fmt.Sprintf("native PG18 postmaster PID %d (start identity %s) stopped and resumed; original backend PID %d answered during stop and retained advisory key (%d,%d) throughout; prototype only, not a complete child census or rebuild proof", postmasterPID, startIdentity, ownerPID, key1, key2)
}

type authBoundaryActivity struct {
	PID          int
	BackendType  string
	BackendStart time.Time
	ClientPort   *int
}

type authBoundaryProcess struct {
	PID   int
	Start string
	State string
}

type authBoundarySocket struct {
	PID    int
	Inode  string
	Local  string
	Remote string
}

// TestDrillFrozenAdmissionDrainsLoadedSCRAMAndPrestartupClients implements the
// approved staged own-auth prototype on the pinned native PG18 fixture.
//
// Calibrated scope: stage 1 (exit before rotation) requires, while the exact
// postmaster is frozen, that both socket-identified old-auth server backends
// actually exited to state Z with unchanged PID/start identities, empty FD
// tables (helper X census), zero live socket records, and that both owned
// helpers observed server EOF and were actually reaped as container processes
// (docker exec wait gathered, in-container PID/start gone). Only then may the
// preregistered admin rotate P0->P1, and only under a held catalog SHARE fence
// with exact client-generated verifier, role OID, privilege and membership
// validation. Stage 2 (reap before DDL) requires, after exact SIGCONT, that
// both old PID/start pairs are authoritatively gone and that the registered
// control backend, advisory lock and catalog observer identities are
// unchanged; an immediate refreeze then reconciles the new admission set.
// No destructive target DDL or restore happens in this prototype.
//
// This is not the protected successor admission gate, not a rebuild witness,
// and not atomic acceptance; it does not replace the separate supervised
// RESTORE writer receipt/reap requirement. The earlier strict all-PIDs-gone-
// while-frozen failure stays immutable under
// docs/evidence/015/restart-auth-prototype; this staged narrowing is not an
// erasure of that historical result.
func TestDrillFrozenAdmissionDrainsLoadedSCRAMAndPrestartupClients(t *testing.T) {
	detail := "starting staged native PG18 exit-before-rotation / reap-before-DDL prototype"
	recordAuthBoundaryResult(t, "TestDrillFrozenAdmissionDrainsLoadedSCRAMAndPrestartupClients", &detail)
	setFailure := func(format string, args ...any) {
		detail = fmt.Sprintf(format, args...)
		t.Fatalf("%s", detail)
	}
	container, dsn := startAuthBoundaryPGWithPostmasterChildContainer(t)
	containerID := container.GetContainerID()
	requireNativePG18(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		setFailure("open registered administrator backend: %v", err)
	}
	defer admin.Close(context.Background())
	control, err := pgx.Connect(ctx, dsn)
	if err != nil {
		setFailure("open registered control-owner backend: %v", err)
	}
	defer control.Close(context.Background())
	catalogObserver, err := pgx.Connect(ctx, dsn)
	if err != nil {
		setFailure("open registered catalog-observer backend: %v", err)
	}
	defer catalogObserver.Close(context.Background())
	if _, err := control.Exec(ctx, `SET application_name='drill-protected-control-origin'`); err != nil {
		setFailure("set registered control application_name: %v", err)
	}

	const lockA, lockB = 71003, 71004
	var ownerPID int
	if err := control.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&ownerPID); err != nil {
		setFailure("identify registered control backend: %v", err)
	}
	if _, err := control.Exec(ctx, `SELECT pg_advisory_lock($1::int,$2::int)`, lockA, lockB); err != nil {
		setFailure("acquire registered control advisory lock: %v", err)
	}
	lockHeld := true
	defer func() {
		if lockHeld {
			unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer unlockCancel()
			_, _ = control.Exec(unlockCtx, `SELECT pg_advisory_unlock($1::int,$2::int)`, lockA, lockB)
		}
	}()
	if err := assertOwnedAdvisoryLock(ctx, control, ownerPID, lockA, lockB); err != nil {
		setFailure("registered control owner did not retain original advisory lock: %v", err)
	}

	role := fmt.Sprintf("drill_frozen_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE ROLE `+pgx.Identifier{role}.Sanitize()+` LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD `+sqlLiteral(passwordP0)); err != nil {
		setFailure("create P0 SCRAM fixture role: %v", err)
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+pgx.Identifier{role}.Sanitize())
	}()
	var originalVerifier string
	if err := admin.QueryRow(ctx, `SELECT rolpassword FROM pg_authid WHERE rolname=$1`, role).Scan(&originalVerifier); err != nil || !strings.HasPrefix(originalVerifier, "SCRAM-SHA-256$") {
		setFailure("fixture P0 verifier is not actual SCRAM: prefix=%q err=%v", verifierPrefix(originalVerifier), err)
	}
	var roleOID int64
	if err := admin.QueryRow(ctx, `SELECT oid::int8 FROM pg_authid WHERE rolname=$1`, role).Scan(&roleOID); err != nil {
		setFailure("capture fixture role OID: %v", err)
	}
	assertProtectedScramOnlyHBA(t, ctx, admin)

	helperPath := buildAuthBoundaryClientHelper(t)
	if err := container.CopyFileToContainer(ctx, helperPath, "/tmp/drill-auth-boundary-client", 0o700); err != nil {
		setFailure("copy static owned-client helper into protected server namespace: %v", err)
	}
	serverIP, err := container.ContainerIP(ctx)
	if err != nil {
		setFailure("read disposable server's direct container endpoint: %v", err)
	}
	// Bounded transport negatives against the explicit protected HBA: the
	// credential used here (P1) is not active for the fixture role yet, so a
	// trust fallback would still admit it. Each transport an old writer could
	// reach must instead return a genuine 28P01 server rejection.
	for _, transport := range []string{"unix", "loopback", "serverip"} {
		line := runHelperAuthCheck(t, ctx, containerID, transport, serverIP, role, passwordP1)
		if !strings.Contains(line, "state=REJECT") || !strings.Contains(line, "sqlstate=28P01") {
			setFailure("protected transport %s did not reject an inactive credential with genuine SQLSTATE 28P01: %s", transport, line)
		}
	}
	heldClient := startOwnedContainerClient(t, ctx, containerID, "scram", role, passwordP0, serverIP)
	defer heldClient.close()
	if !strings.Contains(heldClient.ready, "state=SASLContinue") {
		setFailure("owned raw SCRAM helper did not receive actual SASLContinue: %q", heldClient.ready)
	}
	t.Logf("held auth client public metadata: %s", heldClient.ready)
	heldPort := heldClient.port
	preClient := startOwnedContainerClient(t, ctx, containerID, "prestartup", "", "", serverIP)
	defer preClient.close()
	if !strings.Contains(preClient.ready, "state=PRESTARTUP") {
		setFailure("owned bare TCP helper did not stop before StartupMessage: %q", preClient.ready)
	}
	t.Logf("pre-startup client public metadata: %s", preClient.ready)
	preStartupPort := preClient.port

	postmasterPID, postmasterStart, err := inspectPostmaster(ctx, containerID)
	if err != nil {
		setFailure("bind fixture container/postmaster identity: %v", err)
	}
	if postmasterPID == 1 {
		setFailure("postmaster is namespace PID 1; SIGSTOP cannot safely freeze it in this fixture")
	}

	// Register the actual backend PID/backend_start and Linux start identity for
	// each persistent fixture connection before freezing admission.
	registered := map[int]time.Time{}
	registeredOS := map[int]string{}
	for name, conn := range map[string]*pgx.Conn{"admin": admin, "control-owner": control, "catalog-observer": catalogObserver} {
		pid, backendStart, err := backendIdentity(ctx, admin, conn)
		if err != nil {
			setFailure("register %s backend identity: %v", name, err)
		}
		registered[pid] = backendStart
		registeredOS[pid], err = processStartIdentity(ctx, containerID, pid)
		if err != nil {
			setFailure("register %s Linux process start identity: %v", name, err)
		}
	}
	if _, ok := registered[ownerPID]; !ok {
		setFailure("original control owner PID %d was not registered", ownerPID)
	}

	clientSourceIPs := map[int]string{heldPort: heldClient.localIP, preStartupPort: preClient.localIP}
	preFreeze, err := awaitOwnedConnectionSockets(ctx, containerID, postmasterPID, postmasterStart,
		admin, heldPort, clientSourceIPs)
	if err != nil {
		setFailure("associate held clients with actual server socket/process identities: %v", err)
	}
	if preFreeze[heldPort].PID == preFreeze[preStartupPort].PID {
		setFailure("held SCRAM and pre-startup clients unexpectedly map to one backend PID")
	}
	clientOS, _, err := snapshotPostmasterChildren(ctx, containerID, postmasterPID, postmasterStart)
	if err != nil {
		setFailure("verify helper processes are outside the postmaster child set: %v", err)
	}
	clientOSStart := make(map[int]string, 2)
	for _, client := range []*ownedContainerClient{heldClient, preClient} {
		if _, isPostmasterChild := clientOS[client.pid]; isPostmasterChild {
			setFailure("owned client helper PID %d is unexpectedly a postmaster child", client.pid)
		}
		start, err := processStartIdentity(ctx, containerID, client.pid)
		if err != nil {
			setFailure("owned client helper PID %d has no server-namespace start identity: %v", client.pid, err)
		}
		if start == "" || client.port == 0 {
			setFailure("owned client helper PID %d identity/source port incomplete", client.pid)
		}
		clientOSStart[client.pid] = start
	}
	for pid, start := range registered {
		currentStart, err := processStartIdentity(ctx, containerID, pid)
		if err != nil || currentStart != registeredOS[pid] {
			setFailure("registered fixture backend PID %d has no stable process identity: %v", pid, err)
		}
		var currentBackendStart time.Time
		if err := admin.QueryRow(ctx, `SELECT backend_start FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&currentBackendStart); err != nil || !currentBackendStart.Equal(start) {
			setFailure("registered fixture backend PID %d backend_start changed: was=%s now=%s err=%v", pid, start.UTC().Format(time.RFC3339Nano), currentBackendStart.UTC().Format(time.RFC3339Nano), err)
		}
	}

	// The staged prototype freezes the exact postmaster more than once; every
	// failure path must still guarantee CONT on this same postmaster. No
	// SIGKILL of server backends, no restart, no replacement control lock.
	frozen := false
	freeze := func() error {
		if frozen {
			return nil
		}
		if err := signalPostmaster(ctx, containerID, postmasterPID, postmasterStart, "STOP"); err != nil {
			return err
		}
		if _, err := awaitProcessState(ctx, containerID, postmasterPID, postmasterStart, "T", "t"); err != nil {
			return err
		}
		frozen = true
		return nil
	}
	resume := func() error {
		if !frozen {
			return nil
		}
		resumeCtx, resumeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer resumeCancel()
		if err := signalPostmaster(resumeCtx, containerID, postmasterPID, postmasterStart, "CONT"); err != nil {
			return err
		}
		frozen = false
		return nil
	}
	defer func() {
		if err := resume(); err != nil {
			t.Errorf("guaranteed CONT of exact protected postmaster during cleanup: %v", err)
		}
	}()
	if err := freeze(); err != nil {
		setFailure("SIGSTOP exact protected postmaster PID %d: %v", postmasterPID, err)
	}
	if err := assertOwnedAdvisoryLock(ctx, control, ownerPID, lockA, lockB); err != nil {
		setFailure("control owner/lock unavailable after postmaster stop: %v", err)
	}
	for pid, expectedStart := range registeredOS {
		actualStart, err := processStartIdentity(ctx, containerID, pid)
		if err != nil || actualStart != expectedStart {
			setFailure("registered control/admin/catalog backend PID %d identity changed during freeze: expected=%s actual=%s err=%v", pid, expectedStart, actualStart, err)
		}
	}
	for pid, expectedStart := range clientOSStart {
		actualStart, err := processStartIdentity(ctx, containerID, pid)
		if err != nil || actualStart != expectedStart {
			setFailure("owned helper PID %d identity changed while held at auth boundary: expected=%s actual=%s err=%v", pid, expectedStart, actualStart, err)
		}
	}

	activities, err := readActivityInventory(ctx, admin)
	if err != nil {
		setFailure("read actual PG18 backend_type inventory through preregistered admin backend: %v", err)
	}
	processes, sockets, err := snapshotPostmasterChildren(ctx, containerID, postmasterPID, postmasterStart)
	if err != nil {
		setFailure("enumerate frozen postmaster children and process/socket identities: %v", err)
	}
	if err := reconcileProtectedCensus(processes, sockets, activities, registered, preFreeze, heldPort, preStartupPort); err != nil {
		setFailure("frozen server child census is incomplete or unexplained: %v", err)
	}
	for _, port := range []int{heldPort, preStartupPort} {
		if !socketMappingMatches(sockets, preFreeze[port]) {
			setFailure("frozen census lost authentic socket/process relationship for client source port %d", port)
		}
	}

	// Terminate only the two socket-identified fixture-owned server processes.
	// Use pg_terminate_backend when the real server inventory exposes the PID;
	// otherwise signal the retained PID/start-identity pair directly.
	for _, port := range []int{heldPort, preStartupPort} {
		proc := preFreeze[port]
		if proc.PID == postmasterPID || proc.PID == ownerPID {
			setFailure("refusing to terminate non-client/postmaster process PID %d", proc.PID)
		}
		if _, visible := activities[proc.PID]; visible {
			var terminated bool
			if err := control.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, proc.PID).Scan(&terminated); err != nil || !terminated {
				setFailure("pg_terminate_backend on socket-identified client PID %d failed: terminated=%t err=%v", proc.PID, terminated, err)
			}
		} else if err := signalProcess(ctx, containerID, proc.PID, proc.Start, "TERM"); err != nil {
			setFailure("SIGTERM socket-identified pre-startup PID %d failed: %v", proc.PID, err)
		}
	}
	for _, client := range []*ownedContainerClient{heldClient, preClient} {
		event, err := client.nextEvent(ctx)
		if err != nil || !strings.Contains(event, "state=SERVER_EOF") {
			setFailure("terminated owned client did not report server-side EOF before AuthenticationOk/first write: event=%q err=%v", event, err)
		}
	}

	if err := assertOwnedAdvisoryLock(ctx, control, ownerPID, lockA, lockB); err != nil {
		setFailure("original control lock lost while auth children were terminated: %v", err)
	}
	if _, err := admin.Exec(ctx, `SELECT 1`); err != nil {
		setFailure("registered administrator backend died during frozen drain: %v", err)
	}
	if _, err := catalogObserver.Exec(ctx, `SELECT 1`); err != nil {
		setFailure("registered catalog observer backend died during frozen drain: %v", err)
	}

	// Stage gate 1 (exit before rotation): both socket-identified old-auth
	// server backends must actually exit to state Z with unchanged PID/start
	// identities, empty FD tables (helper X census, not merely no active
	// sockets), and both owned helpers must have reported server EOF and be
	// reaped as container processes. Only then may rotation proceed.
	var drainBarriers []string
	terminated := make(map[int]bool, 2)
	for _, port := range []int{heldPort, preStartupPort} {
		terminated[preFreeze[port].PID] = true
	}
	for _, client := range []*ownedContainerClient{heldClient, preClient} {
		client.close()
		if err := client.requireExited(ctx, containerID, "pre-rotation exit gate"); err != nil {
			setFailure("owned client exit gate: %v", err)
		}
	}
	awaitExited := func() (map[int]authBoundaryProcess, []authBoundarySocket, error) {
		deadline := time.NewTimer(10 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		last := "no census yet"
		for {
			processes, sockets, err := snapshotPostmasterChildren(ctx, containerID, postmasterPID, postmasterStart)
			if err == nil {
				ready := 0
				var pending []string
				for _, port := range []int{heldPort, preStartupPort} {
					old := preFreeze[port]
					current, present := processes[old.PID]
					if !present || current.Start != old.Start || current.State != "Z" {
						pending = append(pending, fmt.Sprintf("pid=%d present=%t census_state=%q start=%s", old.PID, present, current.State, current.Start))
						continue
					}
					entries, readable, ok := processFDTable(sockets, old.PID)
					if !ok || entries != 0 || readable != 0 {
						pending = append(pending, fmt.Sprintf("pid=%d fd_entries=%d fd_readable=%d fd_census=%t", old.PID, entries, readable, ok))
						continue
					}
					if live := countLiveProcessSockets(sockets, old.PID); live != 0 {
						pending = append(pending, fmt.Sprintf("pid=%d live_socket_records=%d", old.PID, live))
						continue
					}
					ready++
				}
				if ready == 2 {
					return processes, sockets, nil
				}
				last = strings.Join(pending, "; ")
			} else {
				last = "census error: " + err.Error()
			}
			select {
			case <-ctx.Done():
				return nil, nil, fmt.Errorf("context ended: %s", last)
			case <-deadline.C:
				return nil, nil, fmt.Errorf("old-auth exit gate not established while postmaster stopped: %s", last)
			case <-ticker.C:
			}
		}
	}
	postDrain, postDrainSockets, err := awaitExited()
	if err != nil {
		setFailure("exit-before-rotation gate: %v", err)
	}
	for _, port := range []int{heldPort, preStartupPort} {
		old := preFreeze[port]
		entries, readable, _ := processFDTable(postDrainSockets, old.PID)
		drainBarriers = append(drainBarriers, fmt.Sprintf("old-auth PID %d exited to state Z (start %s) with empty FD table (entries=%d readable=%d) and no live socket records while postmaster PID %d is stopped; reaped only after CONT", old.PID, old.Start, entries, readable, postmasterPID))
	}
	postDrainActivities, err := readActivityInventory(ctx, admin)
	if err != nil {
		setFailure("read post-drain backend inventory: %v", err)
	}
	for pid, process := range postDrain {
		activity, visible := postDrainActivities[pid]
		if visible {
			if activity.BackendType == "" {
				setFailure("post-drain child PID %d lacks backend_type", pid)
			}
			switch {
			case activity.BackendType == "client backend":
				if _, ok := registered[pid]; !ok {
					setFailure("post-drain frozen census has unexpected authenticated client backend PID %d start=%s; backend_type class alone is not authorization", pid, process.Start)
				}
			case protectedFixtureBackgroundTypes[activity.BackendType]:
			default:
				setFailure("post-drain frozen census has unexpected backend_type %q for PID %d start=%s", activity.BackendType, pid, process.Start)
			}
		}
		if !visible && !(terminated[pid] && process.State == "Z") {
			setFailure("post-drain frozen census has unexplained child PID %d state=%s start=%s", pid, process.State, process.Start)
		}
		if expected, ok := registered[pid]; ok && (!visible || !activity.BackendStart.Equal(expected)) {
			setFailure("registered control/admin/catalog backend PID %d missing or changed in frozen post-drain inventory", pid)
		}
	}
	for pid := range postDrainActivities {
		if _, ok := postDrain[pid]; !ok {
			setFailure("post-drain pg_stat_activity PID %d has no matching OS child identity", pid)
		}
	}

	// Stage gate 2 (rotation allowed only after the exit gate): rotate P0->P1
	// through the preregistered admin using a client-generated SCRAM verifier,
	// then fence the catalog with the already-registered observer and assert
	// the exact verifier, role OID, privilege flags and membership closure.
	// The fence is held until the prototype completes.
	generatedP1Verifier, err := generateSCRAMVerifier(passwordP1, 4096)
	if err != nil {
		setFailure("generate client-side P1 SCRAM verifier: %v", err)
	}
	if _, err := admin.Exec(ctx, `ALTER ROLE `+pgx.Identifier{role}.Sanitize()+` PASSWORD `+sqlLiteral(generatedP1Verifier)); err != nil {
		setFailure("rotate SCRAM role P0->P1 through registered admin while frozen: %v", err)
	}
	tx, err := catalogObserver.Begin(ctx)
	if err != nil {
		setFailure("begin preregistered catalog-observer transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `LOCK TABLE pg_catalog.pg_authid, pg_catalog.pg_auth_members IN SHARE MODE`); err != nil {
		setFailure("acquire same-cluster SHARE catalog fence: %v", err)
	}
	var fencedOID int64
	var fencedVerifier string
	var login, superuser, createdb, createrole, inherit, replication, bypass bool
	if err := tx.QueryRow(ctx, `SELECT oid::int8, rolpassword, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolinherit, rolreplication, rolbypassrls FROM pg_authid WHERE rolname=$1`, role).
		Scan(&fencedOID, &fencedVerifier, &login, &superuser, &createdb, &createrole, &inherit, &replication, &bypass); err != nil {
		setFailure("verify rotated credential under catalog SHARE fence: %v", err)
	}
	if fencedOID != roleOID {
		setFailure("fixture role OID changed across frozen rotation: was=%d now=%d", roleOID, fencedOID)
	}
	if fencedVerifier != generatedP1Verifier {
		setFailure("catalog SHARE fence observed a rolpassword that is not the exact client-generated P1 verifier (observed_length=%d generated_length=%d)", len(fencedVerifier), len(generatedP1Verifier))
	}
	if fencedVerifier == originalVerifier {
		setFailure("catalog SHARE fence observed the original P0 verifier after rotation")
	}
	if !login || superuser || createdb || createrole || inherit || replication || bypass {
		setFailure("catalog SHARE observer saw unexpected privilege flags: login=%t super=%t createdb=%t createrole=%t inherit=%t replication=%t bypass=%t", login, superuser, createdb, createrole, inherit, replication, bypass)
	}
	var membershipCount int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM pg_auth_members m JOIN pg_authid r ON r.oid=m.member WHERE r.rolname=$1`, role).Scan(&membershipCount); err != nil {
		setFailure("verify role membership state under catalog SHARE fence: %v", err)
	}
	if membershipCount != 0 {
		setFailure("restricted fixture role unexpectedly has %d memberships", membershipCount)
	}

	// Stage gate 3 (reap before DDL): CONT the exact postmaster and require the
	// old PID/start pairs to be authoritatively gone, while the registered
	// control backend, advisory lock and catalog observer identities are
	// unchanged. No DDL is actually performed by this prototype.
	if err := resume(); err != nil {
		setFailure("resume exact postmaster after frozen drain gate: %v", err)
	}
	if err := awaitNativePGReady(ctx, dsn); err != nil {
		setFailure("native server did not recover after exact postmaster resume: %v", err)
	}
	if err := assertOwnedAdvisoryLock(ctx, control, ownerPID, lockA, lockB); err != nil {
		setFailure("original control backend/advisory lock changed across reap: %v", err)
	}
	postReap, _, err := snapshotPostmasterChildren(ctx, containerID, postmasterPID, postmasterStart)
	if err != nil {
		setFailure("enumerate postmaster children after reap: %v", err)
	}
	for _, port := range []int{heldPort, preStartupPort} {
		old := preFreeze[port]
		if err := awaitProcessGone(ctx, containerID, old.PID, old.Start); err != nil {
			setFailure("reap-before-DDL gate: old-auth %v", err)
		}
		if current, stillPresent := postReap[old.PID]; stillPresent && current.Start == old.Start {
			setFailure("old-auth PID %d still present after CONT with unchanged start %s", old.PID, old.Start)
		}
	}
	for pid, expectedStart := range registeredOS {
		actualStart, err := processStartIdentity(ctx, containerID, pid)
		if err != nil || actualStart != expectedStart {
			setFailure("registered control/admin/catalog backend PID %d identity changed before any further action: expected=%s actual=%s err=%v", pid, expectedStart, actualStart, err)
		}
	}
	for pid, expected := range registered {
		var currentBackendStart time.Time
		if err := admin.QueryRow(ctx, `SELECT backend_start FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&currentBackendStart); err != nil || !currentBackendStart.Equal(expected) {
			setFailure("registered backend PID %d backend_start changed across reap: was=%s now=%s err=%v", pid, expected.UTC().Format(time.RFC3339Nano), currentBackendStart.UTC().Format(time.RFC3339Nano), err)
		}
	}
	if _, err := tx.Exec(ctx, `SELECT 1`); err != nil {
		setFailure("catalog SHARE transaction did not remain usable after reap: %v", err)
	}

	// Stage gate 4 (immediate refreeze): refreeze the exact postmaster and
	// reconcile the new admission set before any further probe. The drained
	// client origins are gone, so only registered protected origins and known
	// immutable fixture backgrounds may appear.
	if err := freeze(); err != nil {
		setFailure("immediate refreeze after reap: %v", err)
	}
	refreezeProcesses, refreezeSockets, err := snapshotPostmasterChildren(ctx, containerID, postmasterPID, postmasterStart)
	if err != nil {
		setFailure("enumerate refrozen postmaster children: %v", err)
	}
	refreezeActivities, err := readActivityInventory(ctx, admin)
	if err != nil {
		setFailure("read refrozen backend inventory: %v", err)
	}
	if err := reconcileProtectedCensus(refreezeProcesses, refreezeSockets, refreezeActivities, registered, nil, 0, 0); err != nil {
		setFailure("refrozen new-admission census is incomplete or unexplained: %v", err)
	}
	for pid, expectedStart := range registeredOS {
		actualStart, err := processStartIdentity(ctx, containerID, pid)
		if err != nil || actualStart != expectedStart {
			setFailure("registered backend PID %d identity changed during refreeze: expected=%s actual=%s err=%v", pid, expectedStart, actualStart, err)
		}
	}
	if err := assertOwnedAdvisoryLock(ctx, control, ownerPID, lockA, lockB); err != nil {
		setFailure("original control backend/advisory lock changed during refreeze: %v", err)
	}

	// Bounded negative: a TCP/startup client queued in the listen backlog
	// around CONT while the postmaster is frozen must receive a genuine SASL
	// server rejection once released, not a hang or a trust accept.
	queuedClient := startOwnedContainerClient(t, ctx, containerID, "queued", role, passwordP0, serverIP)
	if !strings.Contains(queuedClient.ready, "state=QUEUED") {
		setFailure("queued TCP client did not report a backloged connection: %q", queuedClient.ready)
	}
	if err := resume(); err != nil {
		setFailure("resume exact postmaster after refreeze: %v", err)
	}
	if _, err := fmt.Fprintln(queuedClient.stdin, "go"); err != nil {
		setFailure("release queued TCP/startup client after CONT: %v", err)
	}
	queuedEvent, err := queuedClient.nextEvent(ctx)
	if err != nil || !strings.Contains(queuedEvent, "state=REJECT") || !strings.Contains(queuedEvent, "sqlstate=28P01") {
		setFailure("queued TCP/startup P0 client around CONT did not receive a genuine SASL server rejection: event=%q err=%v", queuedEvent, err)
	}
	queuedClient.close()
	if err := queuedClient.requireExited(ctx, containerID, "post-CONT queued negative"); err != nil {
		setFailure("queued negative helper exit: %v", err)
	}
	if err := awaitNativePGReady(ctx, dsn); err != nil {
		setFailure("native server readiness after final resume: %v", err)
	}

	// Final probes: fresh P0 must be rejected with a typed 28P01 server error
	// (never AuthenticationOk), and a fresh P1 startup fork must succeed after
	// all old capability holders exited. This still cannot claim the complete
	// protected admission gate.
	if err := assertRawPasswordResult(ctx, dsn, role, passwordP0, false); err != nil {
		setFailure("fresh direct server P0 authentication was not rejected with a genuine 28P01 server error: %v", err)
	}
	p1Client, err := beginRawSCRAM(ctx, dsn, role, passwordP1)
	if err != nil {
		setFailure("open fresh P1 startup-fork exchange: %v", err)
	}
	defer p1Client.conn.Close()
	value, err := p1Client.finishAndSelectOne(ctx)
	if err != nil || value != "1" {
		setFailure("fresh direct server P1 authentication failed after reap: value=%q err=%v", value, err)
	}
	if p1Client.backendPID <= 0 {
		setFailure("fresh P1 exchange did not expose server BackendKeyData PID")
	}
	if terminated[p1Client.backendPID] {
		setFailure("fresh P1 startup fork reused old-auth backend PID %d", p1Client.backendPID)
	}
	t.Logf("fresh P1 startup fork after all old capability holders exited uses the current P1 fence (backend PID %d); this is not a complete admission gate", p1Client.backendPID)
	if state, err := processState(ctx, containerID, postmasterPID, postmasterStart); err != nil || state == "Z" {
		setFailure("postmaster identity changed/restarted during staged prototype: state=%q err=%v", state, err)
	}
	if _, err := tx.Exec(ctx, `SELECT 1`); err != nil {
		setFailure("catalog SHARE transaction did not remain usable across staged prototype: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		setFailure("commit catalog observer transaction: %v", err)
	}
	if _, err := control.Exec(ctx, `SELECT pg_advisory_unlock($1::int,$2::int)`, lockA, lockB); err != nil {
		setFailure("release original control lock: %v", err)
	}
	lockHeld = false
	detail = "staged native PG18 prototype completed: exit-before-rotation gate (Z with unchanged PID/start, empty helper-X FD table, zero live sockets, genuine client EOF, helper wait + container PID/start gone), exact client-generated P1 verifier/role OID/privileges/membership closure under held catalog SHARE, reap-before-DDL gate (authoritative post-CONT reap with registered identities unchanged), immediate refreeze reconciliation, queued TCP/startup P0 28P01 rejection, fresh P1 startup fork; " + strings.Join(drainBarriers, "; ") + "; prototype only: not a protected successor admission gate, rebuild, witness, or atomic acceptance; separate supervised RESTORE writer receipt/reap unchanged"
}

type ownedServerSocket struct {
	PID    int
	Start  string
	Inode  string
	Local  string
	Remote string
}

type ownedContainerClient struct {
	cmd          *exec.Cmd
	stdin        io.WriteCloser
	events       chan string
	ready        string
	pid          int
	start        string
	port         int
	localIP      string
	closed       bool
	waited       bool
	waitErr      error
	waitTimedOut bool
}

func buildAuthBoundaryClientHelper(t *testing.T) string {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate auth-boundary test source for static client helper")
	}
	source := filepath.Join(filepath.Dir(testFile), "drill_auth_boundary_client_linux_testhelper.go")
	output := filepath.Join(t.TempDir(), "drill-auth-boundary-client")
	cmd := exec.Command("go", "build", "-trimpath", "-o", output, source)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if result, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build static owned-client helper: %v: %s", err, strings.TrimSpace(string(result)))
	}
	return output
}

func startOwnedContainerClient(t *testing.T, ctx context.Context, containerID, mode, role, password, serverIP string) *ownedContainerClient {
	t.Helper()
	args := []string{"exec", "-i", containerID, "/tmp/drill-auth-boundary-client", mode}
	switch mode {
	case "scram", "queued":
		args = append(args, role)
	}
	args = append(args, serverIP)
	cmd := exec.CommandContext(ctx, "docker", args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("open owned-client private stdin: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("open owned-client public metadata stream: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start owned client in server PID namespace: %v", err)
	}
	client := &ownedContainerClient{cmd: cmd, stdin: stdin, events: make(chan string, 4)}
	if mode == "scram" || mode == "queued" {
		if _, err := fmt.Fprintln(stdin, password); err != nil {
			t.Fatalf("send private canary credential on helper stdin: %v", err)
		}
	}
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			client.events <- scanner.Text()
		}
		close(client.events)
	}()
	line, err := client.nextEvent(ctx)
	if err != nil {
		t.Fatalf("wait for owned-client public readiness record: %v", err)
	}
	if strings.HasPrefix(line, "HELPER_ERROR") {
		t.Fatalf("owned client failed: %s", line)
	}
	client.ready = line
	for _, field := range strings.Fields(line) {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch key {
		case "pid":
			client.pid, _ = strconv.Atoi(value)
		case "local_port":
			client.port, _ = strconv.Atoi(value)
		case "local_ip":
			client.localIP = value
		}
	}
	if client.pid < 2 || client.port < 1 || client.localIP == "" {
		t.Fatalf("invalid owned-client metadata %q", line)
	}
	client.start, err = processStartIdentity(ctx, containerID, client.pid)
	if err != nil {
		t.Fatalf("read owned helper PID %d Linux start identity: %v", client.pid, err)
	}
	return client
}

func (c *ownedContainerClient) nextEvent(ctx context.Context) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case line, ok := <-c.events:
		if !ok {
			return "", errors.New("owned-client output closed before event")
		}
		return line, nil
	}
}

// close releases the owned helper and gathers the docker exec wait result.
// A timeout kills only the local docker exec process; the caller must still
// prove the in-container helper PID/start is gone before treating this as
// helper exit.
func (c *ownedContainerClient) close() {
	if c.closed {
		return
	}
	c.closed = true
	_, _ = fmt.Fprintln(c.stdin, "quit")
	_ = c.stdin.Close()
	done := make(chan error, 1)
	go func() { done <- c.cmd.Wait() }()
	select {
	case c.waitErr = <-done:
	case <-time.After(5 * time.Second):
		c.waitTimedOut = true
		if c.cmd.Process != nil {
			_ = c.cmd.Process.Kill()
		}
		c.waitErr = <-done
	}
	c.waited = true
}

// requireExited verifies the owned helper actually completed: the docker exec
// wait result was gathered and the helper PID/start identity is gone from the
// protected server namespace. Killing the local docker exec process alone is
// not accepted as helper exit.
func (c *ownedContainerClient) requireExited(ctx context.Context, containerID, stage string) error {
	if !c.closed || !c.waited {
		return fmt.Errorf("owned helper PID %d was not closed/waited before %s", c.pid, stage)
	}
	if c.waitTimedOut {
		return fmt.Errorf("owned helper PID %d docker exec wait timed out during %s; local docker exec kill is not helper exit", c.pid, stage)
	}
	if c.waitErr != nil {
		return fmt.Errorf("owned helper PID %d docker exec wait failed during %s: %w", c.pid, stage, c.waitErr)
	}
	gone, detail, err := containerProcessGone(ctx, containerID, c.pid, c.start)
	if err != nil {
		return fmt.Errorf("inspect owned helper PID %d after %s: %w", c.pid, stage, err)
	}
	if !gone {
		return fmt.Errorf("owned helper PID %d (start %s) still alive after %s: %s", c.pid, c.start, stage, detail)
	}
	return nil
}

// containerProcessGone reports whether the PID/start-identity pair is gone
// from the protected server namespace. Docker-exec failures and timeouts are
// returned as errors; they are never counted as process exit.
func containerProcessGone(ctx context.Context, containerID string, pid int, startIdentity string) (bool, string, error) {
	cmd := fmt.Sprintf(`pid=%d; if [ ! -e "/proc/$pid/stat" ]; then echo GONE; exit 0; fi; stat=$(cat "/proc/$pid/stat") || exit 23; actual=$(printf '%%s\n' "$stat" | awk '{ sub(/^.*\) /, ""); print $20 }'); test -n "$actual" || exit 24; if [ "$actual" = %s ]; then echo ALIVE; else printf 'REUSED %%s\n' "$actual"; fi`, pid, startIdentity)
	out, err := dockerExec(ctx, containerID, "sh", "-ec", cmd)
	if err != nil {
		return false, "", err
	}
	line := strings.TrimSpace(string(out))
	switch {
	case line == "GONE":
		return true, line, nil
	case line == "ALIVE":
		return false, line, nil
	case strings.HasPrefix(line, "REUSED "):
		return true, line, nil
	default:
		return false, "", fmt.Errorf("unexpected process exit probe output %q", line)
	}
}

// awaitProcessGone bounds the wait for an authoritative post-CONT reap. A
// permission error, timeout or docker-exec failure is never counted as gone.
func awaitProcessGone(ctx context.Context, containerID string, pid int, startIdentity string) error {
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	last := "not probed"
	for {
		gone, detail, err := containerProcessGone(ctx, containerID, pid, startIdentity)
		if err == nil && gone {
			return nil
		}
		if err != nil {
			last = "probe error: " + err.Error()
		} else {
			last = detail
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("context ended waiting for PID %d (start %s): %s: %w", pid, startIdentity, last, ctx.Err())
		case <-deadline.C:
			return fmt.Errorf("PID %d (start %s) was not authoritatively reaped before deadline: %s", pid, startIdentity, last)
		case <-ticker.C:
		}
	}
}

// runHelperAuthCheck executes the standalone helper authcheck mode with a
// private stdin credential and returns its public outcome line. The credential
// never appears in returned diagnostics.
func runHelperAuthCheck(t *testing.T, ctx context.Context, containerID, transport, serverIP, role, credential string) string {
	t.Helper()
	args := []string{"exec", "-i", containerID, "/tmp/drill-auth-boundary-client", "authcheck", transport}
	if transport == "serverip" {
		args = append(args, serverIP)
	}
	args = append(args, role)
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = strings.NewReader(credential + "\n")
	out, err := cmd.CombinedOutput()
	line := strings.TrimSpace(string(out))
	if err != nil {
		t.Fatalf("owned helper %s authcheck failed: %v: %s", transport, err, line)
	}
	if strings.HasPrefix(line, "HELPER_ERROR") {
		t.Fatalf("owned helper %s authcheck error: %s", transport, line)
	}
	return line
}

func setFailureUnless(t *testing.T, detail *string, format string, args ...any) {
	t.Helper()
	*detail = fmt.Sprintf(format, args...)
	t.Fatalf("%s", *detail)
}

func dsnForDB(t *testing.T, dsn, database string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + database
	return u.String()
}

func assertOwnedAdvisoryLock(ctx context.Context, conn *pgx.Conn, pid, key1, key2 int) error {
	var actualPID, count int
	err := conn.QueryRow(ctx, `SELECT pg_backend_pid(), count(*) FROM pg_locks
		WHERE pid=pg_backend_pid() AND locktype='advisory' AND granted
		AND classid=$1::oid AND objid=$2::oid`, key1, key2).Scan(&actualPID, &count)
	if err != nil {
		return err
	}
	if actualPID != pid || count != 1 {
		return fmt.Errorf("same-session lock proof mismatch: pid=%d want=%d lockrows=%d", actualPID, pid, count)
	}
	return nil
}

func dockerExec(ctx context.Context, containerID string, args ...string) ([]byte, error) {
	full := append([]string{"exec", containerID}, args...)
	cmd := exec.CommandContext(ctx, "docker", full...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("docker exec %v: %w: %s", args, err, strings.TrimSpace(string(out)))
	}
	return out, nil
}

func inspectPostmaster(ctx context.Context, containerID string) (int, string, error) {
	// Read the PID from PostgreSQL's actual postmaster.pid and Linux starttime
	// from /proc inside that same container PID namespace. No process-name scan.
	cmd := `pid=$(head -n 1 "$PGDATA/postmaster.pid"); case "$pid" in ''|*[!0-9]*) exit 21;; esac; start=$(awk 'NR==1 { sub(/^.*\) /, ""); print $20 }' "/proc/$pid/stat"); printf '%s %s\n' "$pid" "$start"`
	out, err := dockerExec(ctx, containerID, "sh", "-ec", cmd)
	if err != nil {
		return 0, "", err
	}
	fields := strings.Fields(string(out))
	if len(fields) != 2 {
		return 0, "", fmt.Errorf("unexpected postmaster identity response")
	}
	pid, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, "", err
	}
	if fields[1] == "" || fields[1] == "0" {
		return 0, "", errors.New("invalid Linux start identity")
	}
	return pid, fields[1], nil
}

func processState(ctx context.Context, containerID string, pid int, startIdentity string) (string, error) {
	cmd := fmt.Sprintf(`pid=%d; start=$(awk 'NR==1 { sub(/^.*\) /, ""); print $20 }' "/proc/$pid/stat"); state=$(awk 'NR==1 { sub(/^.*\) /, ""); print $1 }' "/proc/$pid/stat"); test "$start" = %s || exit 22; printf '%%s %%s\n' "$state" "$start"`, pid, startIdentity)
	out, err := dockerExec(ctx, containerID, "sh", "-ec", cmd)
	if err != nil {
		return "", err
	}
	fields := strings.Fields(string(out))
	if len(fields) != 2 || fields[1] != startIdentity {
		return "", errors.New("postmaster start identity changed")
	}
	return fields[0], nil
}

func awaitProcessState(ctx context.Context, containerID string, pid int, startIdentity string, wanted ...string) (string, error) {
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := processState(ctx, containerID, pid, startIdentity)
		if err != nil {
			return "", err
		}
		for _, candidate := range wanted {
			if state == candidate {
				return state, nil
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline.C:
			return state, fmt.Errorf("timed out waiting for postmaster state %v (last observed %q)", wanted, state)
		case <-ticker.C:
		}
	}
}

func signalPostmaster(ctx context.Context, containerID string, pid int, startIdentity, signal string) error {
	if signal != "STOP" && signal != "CONT" {
		return errors.New("unsupported postmaster signal")
	}
	cmd := fmt.Sprintf(`pid=%d; start=$(awk 'NR==1 { sub(/^.*\) /, ""); print $20 }' "/proc/$pid/stat"); test "$start" = %s || exit 22; kill -%s "$pid"; after=$(awk 'NR==1 { sub(/^.*\) /, ""); print $20 }' "/proc/$pid/stat"); test "$after" = %s`, pid, startIdentity, signal, startIdentity)
	_, err := dockerExec(ctx, containerID, "sh", "-ec", cmd)
	return err
}

func awaitNativePGReady(ctx context.Context, dsn string) error {
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		probeCtx, cancel := context.WithTimeout(ctx, time.Second)
		conn, err := pgx.Connect(probeCtx, dsn)
		if err == nil {
			err = conn.Close(probeCtx)
		}
		cancel()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("server readiness deadline exceeded: %w", err)
		case <-ticker.C:
		}
	}
}

func backendIdentity(ctx context.Context, observer, conn *pgx.Conn) (int, time.Time, error) {
	var pid int
	if err := conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		return 0, time.Time{}, err
	}
	var started time.Time
	if err := observer.QueryRow(ctx, `SELECT backend_start FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&started); err != nil {
		return 0, time.Time{}, err
	}
	return pid, started, nil
}

func processStartIdentity(ctx context.Context, containerID string, pid int) (string, error) {
	cmd := fmt.Sprintf(`pid=%d; stat=$(cat "/proc/$pid/stat") || exit 23; start=$(printf '%%s\n' "$stat" | awk '{ sub(/^.*\) /, ""); print $20 }'); test -n "$start" || exit 24; printf '%%s\n' "$start"`, pid)
	out, err := dockerExec(ctx, containerID, "sh", "-ec", cmd)
	if err != nil {
		return "", err
	}
	start := strings.TrimSpace(string(out))
	if start == "" || start == "0" {
		return "", errors.New("empty Linux process start identity")
	}
	return start, nil
}

func readActivityInventory(ctx context.Context, conn *pgx.Conn) (map[int]authBoundaryActivity, error) {
	rows, err := conn.Query(ctx, `SELECT pid, backend_type, backend_start, client_port FROM pg_stat_activity WHERE pid IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make(map[int]authBoundaryActivity)
	for rows.Next() {
		var item authBoundaryActivity
		if err := rows.Scan(&item.PID, &item.BackendType, &item.BackendStart, &item.ClientPort); err != nil {
			return nil, err
		}
		items[item.PID] = item
	}
	return items, rows.Err()
}

func snapshotPostmasterChildren(ctx context.Context, containerID string, postmasterPID int, postmasterStart string) (map[int]authBoundaryProcess, []authBoundarySocket, error) {
	// Revalidate the exact postmaster before enumerating its direct children.
	// Socket inodes are joined to the actual owning process fd table; cmdline,
	// role, appname and idle state are deliberately absent from this census.
	out, err := dockerExec(ctx, containerID, "/tmp/drill-auth-boundary-client", "census", strconv.Itoa(postmasterPID), postmasterStart)
	if err != nil {
		return nil, nil, err
	}
	processes := make(map[int]authBoundaryProcess)
	var sockets []authBoundarySocket
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "N":
			if len(fields) != 5 {
				return nil, nil, fmt.Errorf("malformed network-table line %q", line)
			}
			sockets = append(sockets, authBoundarySocket{PID: 0, Local: fields[1], Remote: fields[2], Inode: fields[4]})
		case "P":
			if len(fields) != 4 {
				return nil, nil, fmt.Errorf("malformed process census line %q", line)
			}
			pid, err := strconv.Atoi(fields[1])
			if err != nil {
				return nil, nil, err
			}
			processes[pid] = authBoundaryProcess{PID: pid, Start: fields[2], State: fields[3]}
		case "F":
			if len(fields) != 3 {
				return nil, nil, fmt.Errorf("malformed process-fd line %q", line)
			}
			pid, err := strconv.Atoi(fields[1])
			if err != nil {
				return nil, nil, err
			}
			sockets = append(sockets, authBoundarySocket{PID: pid, Inode: fields[2], Local: "fd"})
		case "X":
			if len(fields) != 4 {
				return nil, nil, fmt.Errorf("malformed fd-count line %q", line)
			}
			pid, err := strconv.Atoi(fields[1])
			if err != nil {
				return nil, nil, err
			}
			sockets = append(sockets, authBoundarySocket{PID: pid, Inode: fields[2] + "/" + fields[3], Local: "fdcount"})
		case "S":
			if len(fields) != 6 {
				return nil, nil, fmt.Errorf("malformed socket census line %q", line)
			}
			pid, err := strconv.Atoi(fields[1])
			if err != nil {
				return nil, nil, err
			}
			process, found := processes[pid]
			if !found || process.Start != fields[2] {
				return nil, nil, fmt.Errorf("socket owner PID %d start identity does not match child census", pid)
			}
			sockets = append(sockets, authBoundarySocket{PID: pid, Inode: fields[3], Local: fields[4], Remote: fields[5]})
		default:
			return nil, nil, fmt.Errorf("unknown census record %q", line)
		}
	}
	return processes, sockets, nil
}

func awaitOwnedConnectionSockets(ctx context.Context, containerID string, postmasterPID int, postmasterStart string, admin *pgx.Conn, heldPort int, sourceIPs map[int]string) (map[int]ownedServerSocket, error) {
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	last := "no census"
	for {
		processes, sockets, err := snapshotPostmasterChildren(ctx, containerID, postmasterPID, postmasterStart)
		if err == nil {
			found := make(map[int]ownedServerSocket)
			var diagnostics []string
			activities, activityErr := readActivityInventory(ctx, admin)
			if activityErr != nil {
				err = activityErr
			}
			for port, sourceIP := range sourceIPs {
				if err != nil {
					found = nil
					break
				}
				candidates := findOwnedSockets(sockets, port, sourceIP)
				var observed []string
				for _, socket := range sockets {
					if strings.HasSuffix(strings.ToUpper(socket.Remote), fmt.Sprintf(":%04X", port)) {
						observed = append(observed, fmt.Sprintf("pid=%d inode=%s %s>%s", socket.PID, socket.Inode, socket.Local, socket.Remote))
					}
					if socket.PID > 0 && socket.Local == "fd" {
						observed = append(observed, fmt.Sprintf("fd-owner=%d inode=%s", socket.PID, socket.Inode))
					}
					if socket.PID > 0 && socket.Local == "fdcount" {
						observed = append(observed, fmt.Sprintf("fd-count=%d:%s", socket.PID, socket.Inode))
					}
				}
				diagnostics = append(diagnostics, fmt.Sprintf("client=%s:%d matches=%d observed=[%s]", sourceIP, port, len(candidates), strings.Join(observed, ",")))
				if len(candidates) != 1 {
					found = nil
					break
				}
				candidate := candidates[0]
				proc, ok := processes[candidate.PID]
				if !ok || proc.State == "Z" {
					found = nil
					break
				}
				start, startErr := processStartIdentity(ctx, containerID, candidate.PID)
				if startErr != nil || start != proc.Start {
					found = nil
					break
				}
				activity, visible := activities[candidate.PID]
				if port == heldPort && (!visible || activity.BackendType != "client backend" || activity.ClientPort == nil || *activity.ClientPort != port) {
					found = nil
					break
				}
				if visible && (activity.ClientPort == nil || *activity.ClientPort != port) {
					found = nil
					break
				}
				found[port] = ownedServerSocket{PID: candidate.PID, Start: proc.Start, Inode: candidate.Inode, Local: candidate.Local, Remote: candidate.Remote}
			}
			if len(found) == len(sourceIPs) {
				return found, nil
			}
			var childPIDs, fdOwners, activityRows []string
			for pid, process := range processes {
				childPIDs = append(childPIDs, fmt.Sprintf("%d/%s/%s", pid, process.Start, process.State))
			}
			for _, socket := range sockets {
				if socket.PID > 0 && socket.Local == "fd" {
					fdOwners = append(fdOwners, fmt.Sprintf("%d:%s", socket.PID, socket.Inode))
				}
			}
			for pid, activity := range activities {
				port := "null"
				if activity.ClientPort != nil {
					port = strconv.Itoa(*activity.ClientPort)
				}
				activityRows = append(activityRows, fmt.Sprintf("%d:%s:%s:%s", pid, activity.BackendType, activity.BackendStart.UTC().Format(time.RFC3339Nano), port))
			}
			last = fmt.Sprintf("%s child_processes=[%s] fd_owners=[%s] backend_inventory=[%s]", strings.Join(diagnostics, "; "), strings.Join(childPIDs, ","), strings.Join(fdOwners, ","), strings.Join(activityRows, ","))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, fmt.Errorf("could not uniquely map all owned client four-tuples to PG child socket inodes before deadline (observed: %s; last census error: %v)", last, err)
		case <-ticker.C:
		}
	}
}

func findOwnedSockets(sockets []authBoundarySocket, sourcePort int, sourceIP string) []authBoundarySocket {
	address, err := procIPv4(sourceIP)
	if err != nil {
		return nil
	}
	wantRemote := fmt.Sprintf("%s:%04X", address, sourcePort)
	var matches []authBoundarySocket
	for _, socket := range sockets {
		if socket.PID > 0 && strings.EqualFold(socket.Local, address+":1538") && strings.EqualFold(socket.Remote, wantRemote) {
			matches = append(matches, socket)
		}
	}
	return matches
}

func procIPv4(value string) (string, error) {
	ip := net.ParseIP(value).To4()
	if ip == nil {
		return "", fmt.Errorf("not an IPv4 endpoint")
	}
	return fmt.Sprintf("%02X%02X%02X%02X", ip[3], ip[2], ip[1], ip[0]), nil
}

// protectedFixtureBackgroundTypes are the known immutable PG18 fixture
// background origins. Any other backend_type is rejected; a non-empty
// backend_type is a class label, not authorization, and an unexpected
// authenticated client backend is only authorized here when it is one of the
// preregistered protected admin/control/catalog origins or a socket-identified
// owned prototype client. SQL shape, idle state and application_name are
// deliberately not consulted.
var protectedFixtureBackgroundTypes = map[string]bool{
	"checkpointer":                 true,
	"background writer":            true,
	"walwriter":                    true,
	"autovacuum launcher":          true,
	"logical replication launcher": true,
	"io worker":                    true,
	"archiver":                     true,
	"walsender":                    true,
	"walreceiver":                  true,
	"startup":                      true,
	"autovacuum worker":            true,
}

func reconcileProtectedCensus(processes map[int]authBoundaryProcess, sockets []authBoundarySocket, activities map[int]authBoundaryActivity, registered map[int]time.Time, owned map[int]ownedServerSocket, heldPort, preStartupPort int) error {
	for pid, process := range processes {
		activity, visible := activities[pid]
		if visible && activity.BackendType == "" {
			return fmt.Errorf("child PID %d has empty backend_type inventory", pid)
		}
		ownedClient := false
		for _, port := range []int{heldPort, preStartupPort} {
			if origin, ok := owned[port]; ok && origin.PID == pid && origin.Start == process.Start && socketMappingMatches(sockets, origin) {
				ownedClient = true
			}
		}
		if !visible && !ownedClient {
			return fmt.Errorf("unexplained postmaster child PID %d start=%s state=%s absent from pg_stat_activity backend_type and registered socket origins", pid, process.Start, process.State)
		}
		if visible {
			switch {
			case activity.BackendType == "client backend":
				_, isRegistered := registered[pid]
				if !isRegistered && !ownedClient {
					return fmt.Errorf("unexpected authenticated client backend PID %d start=%s state=%s is not a registered protected admin/control/catalog origin or socket-owned prototype client; backend_type is a class, not authorization", pid, process.Start, process.State)
				}
			case protectedFixtureBackgroundTypes[activity.BackendType]:
				// authorized immutable fixture background origin
			default:
				return fmt.Errorf("unexpected backend_type %q for postmaster child PID %d start=%s", activity.BackendType, pid, process.Start)
			}
		}
		if expected, ok := registered[pid]; ok {
			if !visible {
				return fmt.Errorf("registered control/admin/catalog PID %d absent from real backend_type inventory", pid)
			}
			if !activity.BackendStart.Equal(expected) {
				return fmt.Errorf("registered PID %d backend_start changed: inventory=%s expected=%s", pid, activity.BackendStart, expected)
			}
		}
	}
	for pid := range activities {
		if _, ok := processes[pid]; !ok {
			return fmt.Errorf("pg_stat_activity PID %d has no matching direct postmaster child start identity", pid)
		}
	}
	return nil
}

// processFDTable decodes the helper census X record (all directory entries
// and readable entries) for one postmaster child.
func processFDTable(sockets []authBoundarySocket, pid int) (entries, readable int, ok bool) {
	for _, socket := range sockets {
		if socket.PID != pid || socket.Local != "fdcount" {
			continue
		}
		parts := strings.Split(socket.Inode, "/")
		if len(parts) != 2 {
			return 0, 0, false
		}
		parsedEntries, errE := strconv.Atoi(parts[0])
		parsedReadable, errR := strconv.Atoi(parts[1])
		if errE != nil || errR != nil {
			return 0, 0, false
		}
		return parsedEntries, parsedReadable, true
	}
	return 0, 0, false
}

func socketMappingMatches(sockets []authBoundarySocket, origin ownedServerSocket) bool {
	for _, socket := range sockets {
		if socket.PID == origin.PID && socket.Inode == origin.Inode && socket.Local == origin.Local && socket.Remote == origin.Remote {
			return true
		}
	}
	return false
}

func signalProcess(ctx context.Context, containerID string, pid int, startIdentity, signal string) error {
	if signal != "TERM" {
		return errors.New("unsupported child signal")
	}
	cmd := fmt.Sprintf(`pid=%d; start=$(awk 'NR==1 { sub(/^.*\) /, ""); print $20 }' "/proc/$pid/stat") || exit 22; test "$start" = %s || exit 23; kill -TERM "$pid"; sleep 0.02; if test -r "/proc/$pid/stat"; then after=$(awk 'NR==1 { sub(/^.*\) /, ""); print $20 }' "/proc/$pid/stat"); test "$after" = %s || exit 24; fi`, pid, startIdentity, startIdentity)
	_, err := dockerExec(ctx, containerID, "sh", "-ec", cmd)
	return err
}

func countProcessSockets(sockets []authBoundarySocket, pid int) int {
	count := 0
	for _, socket := range sockets {
		if socket.PID == pid {
			count++
		}
	}
	return count
}

func countLiveProcessSockets(sockets []authBoundarySocket, pid int) int {
	count := 0
	for _, socket := range sockets {
		if socket.PID == pid && socket.Local != "fd" && socket.Local != "fdcount" {
			count++
		}
	}
	return count
}

// TestAuthStagedTransportRequiresSCRAM is a bounded negative for the explicit
// protected HBA: wrong credentials must receive a genuine 28P01 server
// rejection on every transport an old writer could reach (unix socket,
// loopback TCP, container-address TCP), and the actual pg_hba_file_rules must
// show no trust fallback. It is not an admission-gate proof.
func TestAuthStagedTransportRequiresSCRAM(t *testing.T) {
	detail := "checking protected fixture actual HBA rules and wrong-credential rejection over unix, loopback and container-address transports"
	recordAuthBoundaryResult(t, "TestAuthStagedTransportRequiresSCRAM", &detail)
	container, dsn := startAuthBoundaryPGWithPostmasterChildContainer(t)
	containerID := container.GetContainerID()
	requireNativePG18(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect registered administrator backend: %v", err)
	}
	defer admin.Close(context.Background())
	assertProtectedScramOnlyHBA(t, ctx, admin)

	serverIP, err := container.ContainerIP(ctx)
	if err != nil {
		t.Fatalf("read disposable server's direct container endpoint: %v", err)
	}
	helperPath := buildAuthBoundaryClientHelper(t)
	if err := container.CopyFileToContainer(ctx, helperPath, "/tmp/drill-auth-boundary-client", 0o700); err != nil {
		t.Fatalf("copy static owned-client helper into protected server namespace: %v", err)
	}
	// Positive control: the real fixture credential authenticates over the
	// container-address transport, proving the helper path is exercised.
	okLine := runHelperAuthCheck(t, ctx, containerID, "serverip", serverIP, "txharbor", "txharbor")
	if !strings.Contains(okLine, "state=AUTH_OK") {
		t.Fatalf("real fixture credential did not authenticate over container-address TCP: %s", okLine)
	}
	// Negative: an inactive credential must get a genuine server rejection on
	// every reachable transport; a trust fallback would admit it instead.
	for _, transport := range []string{"unix", "loopback", "serverip"} {
		line := runHelperAuthCheck(t, ctx, containerID, transport, serverIP, "txharbor", passwordP0)
		if !strings.Contains(line, "state=REJECT") || !strings.Contains(line, "sqlstate=28P01") {
			t.Fatalf("protected transport %s did not reject an inactive credential with genuine SQLSTATE 28P01: %s", transport, line)
		}
	}
	detail = "protected fixture actual HBA rules require SCRAM on local/loopback/container TCP with no trust fallback; inactive credential got genuine 28P01 over unix, loopback and container-address transports; real fixture credential authenticated over container-address TCP"
}

// TestAuthStagedUnregisteredClientRejectedByCensus is a bounded negative:
// an authenticated client backend that is neither a preregistered protected
// origin nor a socket-owned prototype client must be rejected by the census,
// even when it uses the same role and a matching application_name. The check
// never consults SQL shape, idle state or application_name.
func TestAuthStagedUnregisteredClientRejectedByCensus(t *testing.T) {
	detail := "bounded negative: an unregistered authenticated client backend with same role and matching application_name must not be authorized by the census"
	recordAuthBoundaryResult(t, "TestAuthStagedUnregisteredClientRejectedByCensus", &detail)
	container, dsn := startAuthBoundaryPGWithPostmasterChildContainer(t)
	containerID := container.GetContainerID()
	requireNativePG18(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect registered administrator backend: %v", err)
	}
	defer admin.Close(context.Background())
	helperPath := buildAuthBoundaryClientHelper(t)
	if err := container.CopyFileToContainer(ctx, helperPath, "/tmp/drill-auth-boundary-client", 0o700); err != nil {
		t.Fatalf("copy static owned-client helper into protected server namespace: %v", err)
	}
	postmasterPID, postmasterStart, err := inspectPostmaster(ctx, containerID)
	if err != nil {
		t.Fatalf("bind fixture container/postmaster identity: %v", err)
	}
	if postmasterPID == 1 {
		t.Fatalf("postmaster is namespace PID 1; census fixture requires a non-PID1 postmaster")
	}
	registered := map[int]time.Time{}
	adminPID, adminStart, err := backendIdentity(ctx, admin, admin)
	if err != nil {
		t.Fatalf("register administrator backend identity: %v", err)
	}
	registered[adminPID] = adminStart

	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse fixture DSN for unregistered client: %v", err)
	}
	cfg.RuntimeParams["application_name"] = "drill-protected-control-origin"
	rogue, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("open unregistered authenticated client: %v", err)
	}
	defer rogue.Close(context.Background())
	var roguePID int
	if err := rogue.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&roguePID); err != nil {
		t.Fatalf("identify unregistered client backend: %v", err)
	}
	if roguePID == adminPID {
		t.Fatalf("unregistered client reused administrator backend PID %d", roguePID)
	}

	var processes map[int]authBoundaryProcess
	var sockets []authBoundarySocket
	var activities map[int]authBoundaryActivity
	converged := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		processes, sockets, err = snapshotPostmasterChildren(ctx, containerID, postmasterPID, postmasterStart)
		if err == nil {
			activities, err = readActivityInventory(ctx, admin)
		}
		if err == nil {
			if _, present := processes[roguePID]; present {
				if activity, visible := activities[roguePID]; visible && activity.BackendType == "client backend" {
					converged = true
					break
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !converged {
		t.Fatalf("unregistered client backend PID %d did not appear in the frozen-class census and inventory: %v", roguePID, err)
	}
	err = reconcileProtectedCensus(processes, sockets, activities, registered, nil, 0, 0)
	if err == nil {
		t.Fatalf("census authorized unregistered authenticated client backend PID %d with matching role/application_name", roguePID)
	}
	if !strings.Contains(err.Error(), "unexpected authenticated client backend") {
		t.Fatalf("census rejected unregistered client for an unexpected reason: %v", err)
	}

	if err := rogue.Close(context.Background()); err != nil {
		t.Fatalf("close unregistered client: %v", err)
	}
	clean := false
	lastErr := error(nil)
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		processes, sockets, err = snapshotPostmasterChildren(ctx, containerID, postmasterPID, postmasterStart)
		if err == nil {
			activities, err = readActivityInventory(ctx, admin)
		}
		if err == nil {
			if err = reconcileProtectedCensus(processes, sockets, activities, registered, nil, 0, 0); err == nil {
				clean = true
				break
			}
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	if !clean {
		t.Fatalf("registered-only census did not converge after the unregistered client exited: %v", lastErr)
	}
	detail = "unregistered authenticated client backend (same role, matching application_name) was rejected by the census registration/class check with no SQL-shape/idle/appname whitelist; registered-only census reconciled after its exit; bounded negative, not a complete admission gate"
}
