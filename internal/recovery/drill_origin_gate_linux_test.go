//go:build linux && drill

// drill_origin_gate_linux_test.go is the fix101 bounded TEST-ONLY proof for the
// continuous-origin gate primitive on the protected native PG18 fixture.
//
// The five focused drill cases share the real forwarding gate implemented in
// origin_gate_proxy_linux_test.go and the standalone frontend client in
// origin_gate_client_linux_testhelper.go:
//
//  1. an owned client process (real exec.Cmd retained by the launcher, private
//     one-use capability, accepted peer bound to the process fd/socket inode)
//     forwards a genuine SCRAM exchange to the protected server; the first
//     executable frontend frame is held until the real BackendKeyData PID is
//     registered through the protected observer; the target row stays 0 while
//     held and only the known owned process INSERT passes;
//  2. caller-supplied PID/role/JSON identity and one-use capability replay are
//     denied;
//  3. SSL/GSS negotiation and credential verdicts are relayed byte-for-byte
//     from the real server (no gate-fabricated AuthenticationOk, no query
//     whitelist: classification is by protocol frame type only);
//  4. losing the protected observer or the registered server backend latches
//     the gate forever, closes both directions and refuses new writes;
//  5. a refused source or a PID/start identity mismatch latches the same way.
//
// Calibrated scope: this primitive proves real PG effects of origin admission
// and first-write holding. It does NOT integrate the supervised writer
// receipt/reap, rebuild or atomic acceptance (still OPEN), does not touch the
// drill bridge/harness/F2/T060 lanes, and is not a complete witness claim.
package recovery_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

func originGateWaitFor(ctx context.Context, describe string, condition func() bool) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for %s: %w", describe, ctx.Err())
		case <-ticker.C:
		}
	}
}

func originGateWaitRegistration(ctx context.Context, session *originGateSession) (*originGateRegistration, error) {
	var registration *originGateRegistration
	err := originGateWaitFor(ctx, "protected origin registration", func() bool {
		value, ok := session.Registration()
		if !ok {
			return false
		}
		registration = value
		return true
	})
	return registration, err
}

func originGateWaitLatch(ctx context.Context, gate *originGate) (string, error) {
	var reason string
	err := originGateWaitFor(ctx, "origin gate loss latch", func() bool {
		latched, current := gate.Latched()
		if !latched {
			return false
		}
		reason = current
		return true
	})
	return reason, err
}

func originGateWaitSessionEnded(ctx context.Context, gate *originGate, session *originGateSession) error {
	return originGateWaitFor(ctx, "admitted session end", func() bool {
		return gate.CurrentSession() != session
	})
}

// TestDrillOriginGateOwnedOriginFirstWriteBarrier is the positive primitive:
// the only INSERT that lands is the one carried by the connection whose
// accepted peer four-tuple is bound to the launcher-owned client process, and
// it lands only after the real server BackendKeyData PID is registered.
func TestDrillOriginGateOwnedOriginFirstWriteBarrier(t *testing.T) {
	detail := "declaring the stable gate endpoint before spawning the owned client; forwarding genuine SCRAM to the protected native PG18 fixture; holding the first executable frame until BackendKeyData registration"
	recordOriginGateResult(t, "TestDrillOriginGateOwnedOriginFirstWriteBarrier", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	gate := newOriginGate(t, ctx, fx)
	gate.HoldRegistration()
	launcher := newOriginGateLauncher(t)
	ref := fmt.Sprintf("origin-gate-barrier-%d", time.Now().UnixNano())
	client, cap, err := launcher.launch(t, ctx, gate,
		[]string{"write", gate.Endpoint(), fx.role, fx.table, ref, "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn owned frontend client: %v", err)
	}
	for _, arg := range client.cmd.Args {
		if strings.Contains(arg, fx.password) {
			t.Fatalf("private credential leaked into helper argv: %v", client.cmd.Args)
		}
	}
	for _, env := range client.cmd.Environ() {
		if strings.Contains(env, fx.password) {
			t.Fatalf("private credential leaked into helper environment")
		}
	}
	t.Logf("owned frontend client Cmd PID %d start identity %s; gate endpoint %s was declared before the client was spawned", client.pid, client.start, gate.Endpoint())

	session, err := gate.Admit(ctx, cap)
	if err != nil {
		t.Fatalf("admit owned origin through the gate: %v", err)
	}
	if cap.pid != client.pid || cap.start != client.start {
		t.Fatalf("issued capability does not bind the real client Cmd identity: cap=%d/%s client=%d/%s", cap.pid, cap.start, client.pid, client.start)
	}
	ready, err := client.waitEvent(ctx, "owned client ready record", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_CLIENT_READY")
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
	readyFields := originGateEventFields(ready)
	if _, err := client.waitEvent(ctx, "real server AuthenticationSASL offer", func(line string) bool {
		return strings.Contains(line, "state=AUTH_OFFER")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := client.waitEvent(ctx, "real server AuthenticationOk", func(line string) bool {
		return strings.Contains(line, "state=AUTH_OK")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := client.waitEvent(ctx, "pipelined first Query frame", func(line string) bool {
		return strings.Contains(line, "state=QUERY_SENT")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := originGateWaitFor(ctx, "gate to hold the first executable frontend frame", func() bool {
		return session.BufferedFrames() >= 1
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if !session.ServerAuthOK() {
		t.Fatalf("gate never observed a real server AuthenticationOk frame")
	}
	if _, registered := session.Registration(); registered {
		t.Fatalf("registration completed before the test released it")
	}
	if rows := fx.mustRowCount(ctx, ref); rows != 0 {
		t.Fatalf("target table row %q became %d while origin registration was held; the first write was admitted too early", ref, rows)
	}
	if client.exitedAlready() {
		t.Fatalf("owned client exited while its first write was held")
	}
	t.Logf("held-state barrier: frontend frame buffered=%d, server AuthenticationOk=%t, target rows=%d", session.BufferedFrames(), session.ServerAuthOK(), fx.mustRowCount(ctx, ref))

	gate.ReleaseRegistration()
	registration, err := originGateWaitRegistration(ctx, session)
	if err != nil {
		t.Fatalf("%v", err)
	}
	// Independent re-verification over the administrator connection: the
	// registered BackendKeyData PID must be a real authenticated client backend
	// with the same backend_start, OS process identity and socket inode.
	var backendStart time.Time
	var backendType, clientAddr string
	var clientPort *int
	if err := fx.admin.QueryRow(ctx,
		`SELECT backend_start, backend_type, coalesce(host(client_addr),''), client_port FROM pg_stat_activity WHERE pid=$1`,
		registration.PID).Scan(&backendStart, &backendType, &clientAddr, &clientPort); err != nil {
		t.Fatalf("independent administrator view of registered backend PID %d: %v", registration.PID, err)
	}
	if backendType != "client backend" || !backendStart.Equal(registration.BackendStart) {
		t.Fatalf("registered backend PID %d is not the same authenticated session: type=%q start=%s registered=%s", registration.PID, backendType, backendStart.UTC().Format(time.RFC3339Nano), registration.BackendStart.UTC().Format(time.RFC3339Nano))
	}
	if clientPort == nil || *clientPort != registration.ClientPort || clientAddr != registration.ClientAddr {
		t.Fatalf("registered backend PID %d client tuple changed: admin=%s:%v registered=%s:%d", registration.PID, clientAddr, clientPort, registration.ClientAddr, registration.ClientPort)
	}
	osStart, err := processStartIdentity(ctx, fx.containerID, registration.PID)
	if err != nil || osStart != registration.OSStart {
		t.Fatalf("registered backend PID %d OS process identity mismatch: os=%s registered=%s err=%v", registration.PID, osStart, registration.OSStart, err)
	}
	processes, sockets, err := snapshotPostmasterChildren(ctx, fx.containerID, fx.postmasterPID, fx.postmasterStr)
	if err != nil {
		t.Fatalf("enumerate protected postmaster children: %v", err)
	}
	process, ok := processes[registration.PID]
	if !ok || process.Start != registration.OSStart || process.State == "Z" {
		t.Fatalf("registered backend PID %d is not a live direct postmaster child with the registered start identity: %+v", registration.PID, process)
	}
	socketFound := false
	for _, socket := range sockets {
		if socket.PID == registration.PID && socket.Inode == registration.SocketInode {
			socketFound = true
			break
		}
	}
	if !socketFound {
		t.Fatalf("registered backend PID %d socket inode %s is not in the protected census", registration.PID, registration.SocketInode)
	}
	report := session.Report()
	if report.PeerIP != readyFields["local_ip"] || strconv.Itoa(report.PeerPort) != readyFields["local_port"] {
		t.Fatalf("accepted peer tuple %s:%d is not the real Cmd socket %s:%s", report.PeerIP, report.PeerPort, readyFields["local_ip"], readyFields["local_port"])
	}
	if report.OriginInode == "" || report.CapPID != client.pid || report.CapStart != client.start {
		t.Fatalf("session is not bound to the real Cmd origin: inode=%q cap=%d/%s", report.OriginInode, report.CapPID, report.CapStart)
	}
	t.Logf("registered backend PID %d backend_start %s os_start %s socket_inode %s client %s:%d accepted origin inode %s",
		registration.PID, registration.BackendStart.UTC().Format(time.RFC3339Nano), registration.OSStart,
		registration.SocketInode, registration.ClientAddr, registration.ClientPort, report.OriginInode)

	if _, err := client.waitEvent(ctx, "first INSERT command completion", func(line string) bool {
		return strings.Contains(line, "state=COMMAND") && strings.Contains(line, "INSERT 0 1")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := originGateWaitFor(ctx, "target row for the admitted first write", func() bool {
		return fx.mustRowCount(ctx, ref) == 1
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := client.sendLine("quit"); err != nil {
		t.Fatalf("request graceful client terminate: %v", err)
	}
	if err := client.waitExit(ctx); err != nil {
		t.Fatalf("owned client did not exit cleanly: %v (events: %s)", err, client.observed())
	}
	if strings.Contains(client.observed(), fx.password) {
		t.Fatalf("private credential leaked into helper public output")
	}
	if err := originGateWaitSessionEnded(ctx, gate, session); err != nil {
		t.Fatalf("%v", err)
	}
	// One-use capability: a struct copy of a consumed capability is replay and
	// must be denied without touching the network.
	replay := *cap
	if _, err := gate.Admit(ctx, &replay); err == nil || !strings.Contains(err.Error(), "consumed") {
		t.Fatalf("replayed origin capability was not denied as consumed: %v", err)
	}
	if latched, reason := gate.Latched(); latched {
		t.Fatalf("gate latched during a clean admitted session: %s", reason)
	}
	if rows := fx.mustRowCount(ctx, ref); rows != 1 {
		t.Fatalf("target row count changed after the clean session: %d", rows)
	}
	detail = fmt.Sprintf("stable gate endpoint declared before client spawn; owned Cmd PID %d start %s bound to accepted peer %s:%s via its own socket inode %s; genuine SCRAM forwarded to protected native PG18 backend PID %d (backend_start %s, os_start %s, socket inode %s) registered through protected observer; first INSERT held while row count stayed 0 and released to 1 only after registration; capability replay denied; no observer/backend loss occurred",
		client.pid, client.start, report.PeerIP, readyFields["local_port"], report.OriginInode,
		registration.PID, registration.BackendStart.UTC().Format(time.RFC3339Nano), registration.OSStart, registration.SocketInode)
}

// TestDrillOriginGateDeniesCallerSuppliedIdentityAndReplay proves that the
// private capability cannot be forged from caller-supplied PID/role/JSON data
// and cannot be reused after its one-use connection binding.
func TestDrillOriginGateDeniesCallerSuppliedIdentityAndReplay(t *testing.T) {
	detail := "attempting forged caller-supplied PID/role/JSON capabilities and one-use capability replay against the gate"
	recordOriginGateResult(t, "TestDrillOriginGateDeniesCallerSuppliedIdentityAndReplay", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	gate := newOriginGate(t, ctx, fx)
	launcher := newOriginGateLauncher(t)

	forged := &originGateCap{pid: 4242, start: "999999"}
	if _, err := gate.Admit(ctx, forged); err == nil || !strings.Contains(err.Error(), "not issued") {
		t.Fatalf("caller-supplied PID/start capability was not denied: %v", err)
	}
	if _, err := gate.Admit(ctx, &originGateCap{}); err == nil || !strings.Contains(err.Error(), "not issued") {
		t.Fatalf("zero-value capability was not denied: %v", err)
	}
	if latched, reason := gate.Latched(); latched {
		t.Fatalf("API-misuse denial latched the gate: %s", reason)
	}

	client, cap, err := launcher.launch(t, ctx, gate,
		[]string{"auth", gate.Endpoint(), fx.role, "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn owned auth client: %v", err)
	}
	serialized, err := json.Marshal(cap)
	if err != nil {
		t.Fatalf("marshal private capability: %v", err)
	}
	if string(serialized) != "{}" {
		t.Fatalf("private capability serialized identity material: %s", serialized)
	}
	var decoded originGateCap
	if err := json.Unmarshal([]byte(`{"token":"AAAA","pid":4242,"start":"999999","role":"`+fx.role+`"}`), &decoded); err != nil {
		t.Fatalf("unmarshal forged capability document: %v", err)
	}
	if decoded.pid != 0 || decoded.start != "" {
		t.Fatalf("JSON document constructed capability identity material: %+v", decoded)
	}
	if _, err := gate.Admit(ctx, &decoded); err == nil || !strings.Contains(err.Error(), "not issued") {
		t.Fatalf("JSON-forged capability was not denied: %v", err)
	}

	session, err := gate.Admit(ctx, cap)
	if err != nil {
		t.Fatalf("admit the legitimately issued capability: %v", err)
	}
	if _, err := client.waitEvent(ctx, "real AuthenticationOk through the gate", func(line string) bool {
		return strings.Contains(line, "state=AUTH_OK")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := client.waitExit(ctx); err != nil {
		t.Fatalf("owned auth client did not exit cleanly: %v (events: %s)", err, client.observed())
	}
	if err := originGateWaitSessionEnded(ctx, gate, session); err != nil {
		t.Fatalf("%v", err)
	}
	replay := *cap
	if _, err := gate.Admit(ctx, &replay); err == nil || !strings.Contains(err.Error(), "consumed") {
		t.Fatalf("replayed one-use capability was not denied: %v", err)
	}
	if latched, reason := gate.Latched(); latched {
		t.Fatalf("gate latched during capability denial checks: %s", reason)
	}
	if total, err := fx.totalRows(ctx); err != nil || total != 0 {
		t.Fatalf("denied/authentication-only clients executed SQL: total rows=%d err=%v", total, err)
	}
	detail = "caller-supplied PID/start capability denied (not issued); JSON marshal is {} and JSON-forged identity cannot construct a usable capability; the legitimately issued one-use capability served exactly one real authenticated session; struct-copy replay denied as consumed; no SQL executed and no latch"
}

// TestDrillOriginGateStrictProtocolNegotiationNoFabricatedAuth proves the
// bootstrap forwarding is strict protocol relay, not a query whitelist and not
// a manufactured verdict: SSL/GSS negotiation bytes through the gate are
// byte-identical to a direct control connection, a wrong credential receives
// the real server 28P01 with no AuthenticationOk, and a good credential is
// accepted only when the real server sent AuthenticationOk.
func TestDrillOriginGateStrictProtocolNegotiationNoFabricatedAuth(t *testing.T) {
	detail := "comparing gate-relayed SSL/GSS negotiation replies with a direct control connection and checking that credential verdicts are the real server's"
	recordOriginGateResult(t, "TestDrillOriginGateStrictProtocolNegotiationNoFabricatedAuth", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	gate := newOriginGate(t, ctx, fx)
	launcher := newOriginGateLauncher(t)

	directReplies := make(map[string]string)
	for _, kind := range []string{"ssl", "gss"} {
		client, err := launcher.spawn(t, ctx, []string{"negotiate", fx.serverAddr, kind}, "")
		if err != nil {
			t.Fatalf("spawn direct control negotiation client: %v", err)
		}
		line, err := client.waitEvent(ctx, "direct control negotiation reply", func(line string) bool {
			return strings.Contains(line, "ORIGIN_CLIENT_NEG")
		})
		if err != nil {
			t.Fatalf("%v", err)
		}
		directReplies[kind] = originGateEventFields(line)["reply"]
		if err := client.waitExit(ctx); err != nil {
			t.Fatalf("direct control negotiation client did not exit: %v", err)
		}
	}
	if directReplies["ssl"] != "N" {
		t.Fatalf("fixture ssl=off did not answer N to the direct control SSLRequest: %q", directReplies["ssl"])
	}
	if directReplies["gss"] != "N" && directReplies["gss"] != "G" {
		t.Fatalf("unexpected direct control GSSENCRequest reply: %q", directReplies["gss"])
	}
	t.Logf("direct control real negotiation replies: ssl=%q gss=%q", directReplies["ssl"], directReplies["gss"])

	for _, kind := range []string{"ssl", "gss"} {
		client, cap, err := launcher.launch(t, ctx, gate, []string{"negotiate", gate.Endpoint(), kind}, "")
		if err != nil {
			t.Fatalf("spawn gate negotiation client: %v", err)
		}
		session, err := gate.Admit(ctx, cap)
		if err != nil {
			t.Fatalf("admit negotiation-only origin: %v", err)
		}
		line, err := client.waitEvent(ctx, "gate-relayed negotiation reply", func(line string) bool {
			return strings.Contains(line, "ORIGIN_CLIENT_NEG")
		})
		if err != nil {
			t.Fatalf("%v", err)
		}
		if fields := originGateEventFields(line); fields["reply"] != directReplies[kind] {
			t.Fatalf("gate-relayed %s reply %q is not the direct real server reply %q", kind, fields["reply"], directReplies[kind])
		}
		if err := client.waitExit(ctx); err != nil {
			t.Fatalf("gate negotiation client did not exit: %v", err)
		}
		if err := originGateWaitSessionEnded(ctx, gate, session); err != nil {
			t.Fatalf("%v", err)
		}
	}

	wrongCredential := fmt.Sprintf("origin-gate-wrong-%d", time.Now().UnixNano())
	clientWrong, capWrong, err := launcher.launch(t, ctx, gate,
		[]string{"auth", gate.Endpoint(), fx.role, "none", originGateAppName}, wrongCredential)
	if err != nil {
		t.Fatalf("spawn wrong-credential client: %v", err)
	}
	sessionWrong, err := gate.Admit(ctx, capWrong)
	if err != nil {
		t.Fatalf("admit wrong-credential origin: %v", err)
	}
	rejectionLine, err := clientWrong.waitEvent(ctx, "genuine server rejection", func(line string) bool {
		return strings.Contains(line, "state=REJECT")
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !strings.Contains(rejectionLine, "sqlstate=28P01") {
		t.Fatalf("wrong credential was not rejected by genuine server authentication: %s", rejectionLine)
	}
	if sessionWrong.ServerAuthOK() {
		t.Fatalf("gate observed AuthenticationOk for a credential the real server rejected")
	}
	if err := clientWrong.waitExit(ctx); err != nil {
		t.Fatalf("wrong-credential client did not exit: %v", err)
	}
	if err := originGateWaitSessionEnded(ctx, gate, sessionWrong); err != nil {
		t.Fatalf("%v", err)
	}

	clientOK, capOK, err := launcher.launch(t, ctx, gate,
		[]string{"auth", gate.Endpoint(), fx.role, "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn valid-credential client: %v", err)
	}
	sessionOK, err := gate.Admit(ctx, capOK)
	if err != nil {
		t.Fatalf("admit valid-credential origin: %v", err)
	}
	if _, err := clientOK.waitEvent(ctx, "real AuthenticationOk", func(line string) bool {
		return strings.Contains(line, "state=AUTH_OK")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if !sessionOK.ServerAuthOK() {
		t.Fatalf("valid credential authenticated without a real server AuthenticationOk frame")
	}
	if err := clientOK.waitExit(ctx); err != nil {
		t.Fatalf("valid-credential client did not exit: %v", err)
	}
	if err := originGateWaitSessionEnded(ctx, gate, sessionOK); err != nil {
		t.Fatalf("%v", err)
	}
	if total, err := fx.totalRows(ctx); err != nil || total != 0 {
		t.Fatalf("authentication-only clients executed SQL: total rows=%d err=%v", total, err)
	}
	detail = fmt.Sprintf("gate SSL/GSS negotiation replies byte-identical to direct control connection (ssl=%q gss=%q); wrong credential got genuine server 28P01 with no AuthenticationOk frame; valid credential accepted only with a real server AuthenticationOk; no SQL executed; classification is by protocol frame type, not query text", directReplies["ssl"], directReplies["gss"])
}

// TestDrillOriginGateObserverAndBackendLossLatchRefusesWrites proves the
// permanent loss latch for both protected liveness sources: killing the
// registered observer closes the admitted channel, and killing the registered
// server backend closes it; in both cases new writes are refused and no
// success fallback occurs.
func TestDrillOriginGateObserverAndBackendLossLatchRefusesWrites(t *testing.T) {
	detail := "terminating the protected observer and then a registered server backend to prove both loss latches close the admitted channel and refuse new writes"
	recordOriginGateResult(t, "TestDrillOriginGateObserverAndBackendLossLatchRefusesWrites", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	launcher := newOriginGateLauncher(t)

	// Phase 1: protected observer termination.
	gateObserverLoss := newOriginGate(t, ctx, fx)
	refObserverLoss := fmt.Sprintf("origin-gate-observer-loss-%d", time.Now().UnixNano())
	clientObserverLoss, capObserverLoss, err := launcher.launch(t, ctx, gateObserverLoss,
		[]string{"write", gateObserverLoss.Endpoint(), fx.role, fx.table, refObserverLoss, "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn observer-loss client: %v", err)
	}
	sessionObserverLoss, err := gateObserverLoss.Admit(ctx, capObserverLoss)
	if err != nil {
		t.Fatalf("admit observer-loss origin: %v", err)
	}
	if _, err := clientObserverLoss.waitEvent(ctx, "observer-loss first INSERT completion", func(line string) bool {
		return strings.Contains(line, "state=COMMAND") && strings.Contains(line, "INSERT 0 1")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if rows := fx.mustRowCount(ctx, refObserverLoss); rows != 1 {
		t.Fatalf("first admitted write did not land before observer loss: %d", rows)
	}
	var terminated bool
	if err := fx.admin.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, gateObserverLoss.observerPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate protected observer backend PID %d: terminated=%t err=%v", gateObserverLoss.observerPID, terminated, err)
	}
	observerLossReason, err := originGateWaitLatch(ctx, gateObserverLoss)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !strings.Contains(observerLossReason, "liveness") || !strings.Contains(observerLossReason, "observer") {
		t.Fatalf("observer loss did not latch protected liveness: %s", observerLossReason)
	}
	if _, err := clientObserverLoss.waitEvent(ctx, "observer-loss channel close", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_CLIENT_EXIT")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := clientObserverLoss.waitExit(ctx); err != nil {
		t.Fatalf("observer-loss client did not exit after the channel closed: %v", err)
	}
	if rows := fx.mustRowCount(ctx, refObserverLoss); rows != 1 {
		t.Fatalf("observer-loss latch changed admitted rows: %d", rows)
	}
	refObserverRefused := fmt.Sprintf("origin-gate-observer-refused-%d", time.Now().UnixNano())
	clientObserverRefused, err := launcher.spawn(t, ctx,
		[]string{"write", gateObserverLoss.Endpoint(), fx.role, fx.table, refObserverRefused, "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn refused-after-observer-loss client: %v", err)
	}
	if _, err := clientObserverRefused.waitEvent(ctx, "refused connection after observer loss", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_CLIENT_EXIT")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := clientObserverRefused.waitRefusalExit(ctx); err != nil {
		t.Fatalf("refused-after-observer-loss client did not exit: %v", err)
	}
	if rows := fx.mustRowCount(ctx, refObserverRefused); rows != 0 {
		t.Fatalf("write was admitted after observer loss latch: %d", rows)
	}
	t.Logf("observer loss latched: %s", observerLossReason)
	_ = sessionObserverLoss

	// Phase 2: registered server backend termination on a fresh gate.
	gateBackendLoss := newOriginGate(t, ctx, fx)
	refBackendLoss := fmt.Sprintf("origin-gate-backend-loss-%d", time.Now().UnixNano())
	clientBackendLoss, capBackendLoss, err := launcher.launch(t, ctx, gateBackendLoss,
		[]string{"write", gateBackendLoss.Endpoint(), fx.role, fx.table, refBackendLoss, "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn backend-loss client: %v", err)
	}
	sessionBackendLoss, err := gateBackendLoss.Admit(ctx, capBackendLoss)
	if err != nil {
		t.Fatalf("admit backend-loss origin: %v", err)
	}
	if _, err := clientBackendLoss.waitEvent(ctx, "backend-loss first INSERT completion", func(line string) bool {
		return strings.Contains(line, "state=COMMAND") && strings.Contains(line, "INSERT 0 1")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	registration, err := originGateWaitRegistration(ctx, sessionBackendLoss)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if rows := fx.mustRowCount(ctx, refBackendLoss); rows != 1 {
		t.Fatalf("first admitted write did not land before backend loss: %d", rows)
	}
	if err := fx.admin.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, registration.PID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate registered server backend PID %d: terminated=%t err=%v", registration.PID, terminated, err)
	}
	backendLossReason, err := originGateWaitLatch(ctx, gateBackendLoss)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !strings.Contains(backendLossReason, "lost") && !strings.Contains(backendLossReason, "liveness") && !strings.Contains(backendLossReason, "terminating") && !strings.Contains(backendLossReason, "FATAL") {
		t.Fatalf("backend loss did not latch the registered channel: %s", backendLossReason)
	}
	if _, err := clientBackendLoss.waitEvent(ctx, "backend-loss channel close", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_CLIENT_EXIT")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := clientBackendLoss.waitExit(ctx); err != nil {
		t.Fatalf("backend-loss client did not exit after the channel closed: %v", err)
	}
	if rows := fx.mustRowCount(ctx, refBackendLoss); rows != 1 {
		t.Fatalf("backend-loss latch changed admitted rows: %d", rows)
	}
	refBackendRefused := fmt.Sprintf("origin-gate-backend-refused-%d", time.Now().UnixNano())
	clientBackendRefused, err := launcher.spawn(t, ctx,
		[]string{"write", gateBackendLoss.Endpoint(), fx.role, fx.table, refBackendRefused, "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn refused-after-backend-loss client: %v", err)
	}
	if _, err := clientBackendRefused.waitEvent(ctx, "refused connection after backend loss", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_CLIENT_EXIT")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := clientBackendRefused.waitRefusalExit(ctx); err != nil {
		t.Fatalf("refused-after-backend-loss client did not exit: %v", err)
	}
	if rows := fx.mustRowCount(ctx, refBackendRefused); rows != 0 {
		t.Fatalf("write was admitted after backend loss latch: %d", rows)
	}
	detail = fmt.Sprintf("protected observer termination latched liveness (%s) and closed the admitted channel; registered server backend PID %d termination latched the channel (%s); both latches refused new writes with no success fallback", observerLossReason, registration.PID, backendLossReason)
}

// TestDrillOriginGateRefusedSourceAndProcessIdentityMismatchLatch proves the
// loss latch for refused origins: a foreign genuine client with the same W role
// and the same application_name is refused when its accepted tuple is not owned
// by the capability process, and a capability whose origin process identity is
// gone is refused as well. Neither refusal may execute SQL.
func TestDrillOriginGateRefusedSourceAndProcessIdentityMismatchLatch(t *testing.T) {
	detail := "refusing a foreign genuine client whose accepted tuple is not owned by the capability process and a capability whose origin process is gone; both must latch loss"
	recordOriginGateResult(t, "TestDrillOriginGateRefusedSourceAndProcessIdentityMismatchLatch", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	launcher := newOriginGateLauncher(t)

	// Phase 1: refused source (socket/FD mismatch).
	gateRefusedSource := newOriginGate(t, ctx, fx)
	_, capIdle, err := launcher.launch(t, ctx, gateRefusedSource, []string{"idle"}, "")
	if err != nil {
		t.Fatalf("spawn idle capability process: %v", err)
	}
	refForeign := fmt.Sprintf("origin-gate-foreign-%d", time.Now().UnixNano())
	foreign, err := launcher.spawn(t, ctx, []string{"write", gateRefusedSource.Endpoint(), fx.role, fx.table, refForeign, "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn foreign genuine client: %v", err)
	}
	if _, err := foreign.waitEvent(ctx, "foreign client socket binding", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_CLIENT_READY")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := gateRefusedSource.Admit(ctx, capIdle); err == nil {
		t.Fatalf("gate admitted a foreign source not owned by the capability process")
	}
	reason, err := originGateWaitLatch(ctx, gateRefusedSource)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !strings.Contains(reason, "does not own") {
		t.Fatalf("foreign source refusal did not latch the accepted-origin loss: %s", reason)
	}
	if _, err := foreign.waitEvent(ctx, "foreign client channel close", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_CLIENT_EXIT")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := foreign.waitRefusalExit(ctx); err != nil {
		t.Fatalf("foreign client did not exit after refusal: %v", err)
	}
	if rows := fx.mustRowCount(ctx, refForeign); rows != 0 {
		t.Fatalf("foreign genuine client with copied role/application_name executed SQL: %d rows", rows)
	}
	refRefused := fmt.Sprintf("origin-gate-refused-%d", time.Now().UnixNano())
	refusedAfterLatch, err := launcher.spawn(t, ctx,
		[]string{"write", gateRefusedSource.Endpoint(), fx.role, fx.table, refRefused, "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn refused-after-mismatch client: %v", err)
	}
	if _, err := refusedAfterLatch.waitEvent(ctx, "refused connection after source mismatch", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_CLIENT_EXIT")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := refusedAfterLatch.waitRefusalExit(ctx); err != nil {
		t.Fatalf("refused-after-mismatch client did not exit: %v", err)
	}
	if rows := fx.mustRowCount(ctx, refRefused); rows != 0 {
		t.Fatalf("write was admitted after source-mismatch latch: %d", rows)
	}
	t.Logf("refused source latched: %s", reason)

	// Phase 2: PID/start identity mismatch (capability process gone).
	gateIdentityLoss := newOriginGate(t, ctx, fx)
	capProcessGone, capGone, err := launcher.launch(t, ctx, gateIdentityLoss, []string{"idle"}, "")
	if err != nil {
		t.Fatalf("spawn capability process for identity loss: %v", err)
	}
	if err := capProcessGone.killAndWait(ctx); err != nil {
		t.Fatalf("terminate capability process before admission: %v", err)
	}
	refIdentity := fmt.Sprintf("origin-gate-identity-%d", time.Now().UnixNano())
	foreignIdentity, err := launcher.spawn(t, ctx, []string{"write", gateIdentityLoss.Endpoint(), fx.role, fx.table, refIdentity, "none", originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn client during identity-loss admission: %v", err)
	}
	if _, err := foreignIdentity.waitEvent(ctx, "identity-loss client socket binding", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_CLIENT_READY")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := gateIdentityLoss.Admit(ctx, capGone); err == nil {
		t.Fatalf("gate admitted a capability whose origin process identity is gone")
	}
	reasonIdentity, err := originGateWaitLatch(ctx, gateIdentityLoss)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !strings.Contains(reasonIdentity, "identity") {
		t.Fatalf("process identity mismatch did not latch the origin loss: %s", reasonIdentity)
	}
	if _, err := foreignIdentity.waitEvent(ctx, "identity-loss channel close", func(line string) bool {
		return strings.HasPrefix(line, "ORIGIN_CLIENT_EXIT")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := foreignIdentity.waitRefusalExit(ctx); err != nil {
		t.Fatalf("identity-loss client did not exit after refusal: %v", err)
	}
	if rows := fx.mustRowCount(ctx, refIdentity); rows != 0 {
		t.Fatalf("SQL executed during PID/start identity refusal: %d rows", rows)
	}
	detail = fmt.Sprintf("foreign genuine client (same restricted W role, same application_name) refused as not owned by the capability process, latch: %s; capability whose process identity was gone refused, latch: %s; no SQL/row for either refused source and all later writes refused", reason, reasonIdentity)
}
