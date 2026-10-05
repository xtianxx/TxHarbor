//go:build linux && drill

// borrowed-receipt-auth-entry_linux_test.go is the NON-AUTHORIZING integration
// between the actual restricted-W receipt handoff and the actual protected
// origin gate: a private one-use auth-stage checkpoint that requires the real
// coordinator run to have finished, the original child sole wait to have
// succeeded, the gate owner receipt to be a completed clean success on the
// ORIGINAL bound handle, the gate clean retirement to be published, and the
// registered native server backend to be authoritatively gone through the
// entry's OWN strict in-container proof (validated expected PID, recognized
// kernel state, positive numeric start, correct command delimiter, explicit
// errno classification and a full protected-scope socket census) before the
// opaque handoff token may be consumed exactly once.
//
// RE01-RE04 hardening in this file:
//   - RE01: Recheck publishes its final decision under the shared state mutex
//     (invalidated first, then the caller context), with a test-only
//     notification/barrier seam after the genuine Health call and before the
//     publication decision.
//   - RE02: Enter classifies invalidation/reservation/consumption under the
//     shared state mutex BEFORE the caller context, so concurrent copies and
//     replays (including nil-context ones) refuse without invalidating the
//     winner or consuming the shared token. The fixture-owned one-time shared
//     entry slot binds the checkpoint to the original fixture/run-bound
//     capability, so a genuine verification failure is a permanent shared loss
//     that copies and reconstructed entries cannot rehabilitate.
//   - RE03: the entry adapter path uses caller-derived bounded contexts and
//     context-aware (TryLock + bounded backoff) mutex acquisition for the
//     protected control/observer serialization; no I/O is performed under the
//     shared state mutex and caller cancellation is never reported healthy.
//   - RE04: the entry retirement proof is independent of the gate's shared
//     shell probe and uses the entry-private strict container probe helper.
//
// Failed retirement, context, observer or owner facts permanently invalidate
// the shared checkpoint and reserve no auth stage. It grants no clean/DDL/
// acceptance authority, adds no production API, and never reconstructs
// authority from diagnostics, PIDs, keys or caller booleans.
package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
)

// borrowedReceiptAuthEntryProbePath is the private per-container path of the
// entry-private strict probe; it never overwrites the shared auth/gate census
// helper or the identity-prefix helper.
const borrowedReceiptAuthEntryProbePath = "/tmp/borrowed-receipt-auth-entry-probe"

// borrowedReceiptAuthEntryStage names the narrow test-only pause points of the
// entry adapter. They are notification/barrier seams only: they declare
// nothing and grant no authority.
type borrowedReceiptAuthEntryStage string

const (
	// borrowedReceiptAuthEntryStageEnterPublish pauses Enter after every
	// genuine verification I/O (including the real health checks and the
	// one-time token consumption) and before the shared publication decision.
	borrowedReceiptAuthEntryStageEnterPublish borrowedReceiptAuthEntryStage = "enter-publish"
	// borrowedReceiptAuthEntryStageRecheckPublish pauses Recheck after the
	// genuine Health call and before the shared publication decision.
	borrowedReceiptAuthEntryStageRecheckPublish borrowedReceiptAuthEntryStage = "recheck-publish"
)

// borrowedReceiptAuthEntry is the private one-use auth-stage checkpoint. Copies
// share the state pointer, so permanent invalidation and the one-time entry
// are shared; it exposes no public clean/DDL/acceptance authority.
type borrowedReceiptAuthEntry struct {
	state *borrowedReceiptAuthEntryState
}

// borrowedReceiptAuthEntryState retains the concrete fixture, gate session,
// actual run/prefix and opaque handoff as one shared checkpoint. All identity
// fields are immutable safe copies; no conn, cmd or caller boolean.
type borrowedReceiptAuthEntryState struct {
	mu            sync.Mutex
	invalidated   bool
	invalidReason string
	entering      bool
	entered       bool

	fixture *borrowedAuthHandoffFixture
	gate    *originGate
	session *originGateSession
	run     *recovery.DrillBorrowedWriterRun
	prefix  *borrowedIdentityPrefix
	handoff recovery.OpaqueDrillReceiptHandoff

	originalKey     recovery.TargetKey
	controlKey      recovery.TargetKey
	roleFingerprint string
	operationID     string
	instanceID      string
	writerRoleOID   uint32
	childPID        int
	childStartID    uint64
	registeredPID   int
	registeredStart time.Time
	postmasterStart time.Time
	systemID        string

	// stage is the optional unexported test-only notification/barrier seam. It
	// is invoked after the genuine Health verification and before the shared
	// publication mutex; it declares nothing and is nil in real runs.
	stage func(borrowedReceiptAuthEntryStage)
}

// borrowedReceiptAuthEntrySlot is the fixture-owned one-time shared storage of
// the auth-stage checkpoint. The original fixture initializes exactly one slot
// before the run/entry construction and shallow fixture copies share the
// pointer, so a genuine failure permanently latches the original
// fixture/run-bound capability instead of a per-constructor copy. The slot is
// unexported, holds no authority of its own, is never reachable through a
// global registry and is only ever populated with a fully validated state; a
// nil slot refuses and has no allocate-fresh fallback.
type borrowedReceiptAuthEntrySlot struct {
	mu    sync.Mutex
	state *borrowedReceiptAuthEntryState
}

// newBorrowedReceiptAuthEntry binds the opaque handoff token to the concrete
// fixture, the actual gate session issued by f.gate's authentic borrowed
// admission, and the immutable run/prefix identity. It refuses an absent or
// invalidated token, a session from another gate, a foreign borrowed origin, a
// mismatched claimed/observed/frozen child identity, an incomplete gate
// registration and a session whose bound role/database is not the actual W
// target. No diagnostic projection, PID, key or boolean can construct it.
func newBorrowedReceiptAuthEntry(f *borrowedAuthHandoffFixture, session *originGateSession, handoff recovery.OpaqueDrillReceiptHandoff) (*borrowedReceiptAuthEntry, error) {
	if f == nil || f.gate == nil || f.run == nil || f.prefix == nil || session == nil {
		return nil, errors.New("receipt auth entry requires the concrete fixture, gate session and run")
	}
	facts := handoff.Diagnostics()
	if !facts.Present || facts.Invalidated {
		return nil, errors.New("receipt auth entry requires the opaque handoff token produced by the actual run")
	}
	if session.g != f.gate {
		return nil, errors.New("gate session was not issued by this fixture's gate")
	}
	g := f.gate
	g.mu.Lock()
	borrowed := g.borrowedOrigin
	g.mu.Unlock()
	if borrowed == nil || borrowed.run != f.run || borrowed.prefix != f.prefix {
		return nil, errors.New("gate borrowed origin is not this actual run/prefix")
	}
	binding := f.run.Binding()
	if facts.OriginalKey != binding.OriginalTargetKey() || facts.ControlKey != binding.ControlTargetKey() ||
		facts.RoleFingerprint != binding.OriginalRoleFingerprint() || facts.OperationID != binding.OriginalOperationID() ||
		facts.WriterRoleOID != binding.WriterRoleOID() || facts.InstanceID != binding.OriginalInstanceID() {
		return nil, errors.New("handoff token does not retain the immutable original binding")
	}
	if f.prefix.capturedTargetDBOID == 0 || f.prefix.capturedRoleOID == 0 ||
		f.prefix.capturedTargetDBOID != binding.TargetDatabaseOID() || f.prefix.capturedRoleOID != binding.WriterRoleOID() {
		return nil, errors.New("prefix capture does not match the immutable target/role catalog binding")
	}
	anchor := f.prefix.capturedFacts
	if !anchor.Present || anchor.Invalidated || anchor.PostmasterStart.IsZero() || anchor.SystemIdentifier == "" ||
		anchor.OriginalTargetKey != binding.OriginalTargetKey() || anchor.ControlTargetKey != binding.ControlTargetKey() {
		return nil, errors.New("prefix control anchor capture is incomplete or foreign")
	}
	if session.supervisor == nil {
		return nil, errors.New("gate session has no claimed borrowed origin")
	}
	claimed := session.supervisor.StartedIdentity()
	observed := f.run.Observation().StartedIdentity()
	if !claimed.Started || !observed.Started || claimed.PID != observed.PID || claimed.StartID != observed.StartID ||
		claimed.PID != facts.ChildPID || claimed.StartID != facts.ChildStartID || claimed.PID <= 0 || claimed.StartID == 0 {
		return nil, errors.New("gate claimed origin does not match the run observation and frozen child identity")
	}
	registration, ok := session.Registration()
	if !ok || registration.PID <= 0 || registration.OSStart == "" || registration.SocketInode == "" || registration.BackendStart.IsZero() {
		return nil, errors.New("gate registration is unavailable or incomplete")
	}
	report := session.Report()
	if !report.ServerAuthOK || report.BoundRole != f.writerRole || report.BoundDatabase != f.targetDB {
		return nil, errors.New("gate session bound identity is not the actual W target")
	}
	state := &borrowedReceiptAuthEntryState{
		fixture: f, gate: g, session: session, run: f.run, prefix: f.prefix, handoff: handoff,
		originalKey:     facts.OriginalKey,
		controlKey:      facts.ControlKey,
		roleFingerprint: facts.RoleFingerprint,
		operationID:     facts.OperationID,
		instanceID:      facts.InstanceID,
		writerRoleOID:   facts.WriterRoleOID,
		childPID:        facts.ChildPID,
		childStartID:    facts.ChildStartID,
		registeredPID:   registration.PID,
		registeredStart: registration.BackendStart,
		postmasterStart: anchor.PostmasterStart,
		systemID:        anchor.SystemIdentifier,
	}
	// The fully validated state is installed into the fixture-owned shared slot
	// at most once; every later construction over the same original
	// run/session/handoff reuses it (and therefore shares its permanent loss),
	// while a different capability can never borrow the closed slot. A nil slot
	// refuses: there is no allocate-fresh fallback.
	return f.authEntrySlot.bindOrReuse(state, f, session, handoff)
}

// bindOrReuse installs the fully validated state into the fixture-owned shared
// slot at most once, or reuses the existing state for the same original
// run/session/handoff. It never resets or replaces an invalidated, in-progress
// or entered state, and a different run/session/handoff can never borrow the
// closed slot. The critical section is a short in-memory publication only (no
// I/O, no conn, no cmd).
func (s *borrowedReceiptAuthEntrySlot) bindOrReuse(state *borrowedReceiptAuthEntryState, f *borrowedAuthHandoffFixture, session *originGateSession, handoff recovery.OpaqueDrillReceiptHandoff) (*borrowedReceiptAuthEntry, error) {
	if s == nil {
		return nil, errors.New("receipt auth entry requires the initialized fixture shared entry slot")
	}
	if state == nil || f == nil || session == nil {
		return nil, errors.New("receipt auth entry slot requires the validated fixture binding")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.state; existing != nil {
		if existing.run != f.run || existing.session != session || existing.handoff != handoff {
			return nil, errors.New("fixture shared entry slot is bound to another original run/session/handoff")
		}
		return &borrowedReceiptAuthEntry{state: existing}, nil
	}
	s.state = state
	return &borrowedReceiptAuthEntry{state: state}, nil
}

// Enter performs the one-time auth-stage entry: genuine gate owner sole-wait
// success and published clean retirement, the entry's OWN strict authoritative
// disappearance of the registered native server backend (protected control row,
// in-container PID/start parser and full protected-scope socket census, plus
// the protected observer pid/start/datOID row and the unchanged cluster
// incarnation), a fresh whole-window prefix/control-anchor recheck, and only
// then the bounded one-time handoff.ConsumeForAuth.
//
// The in-progress reservation is taken atomically BEFORE any I/O: concurrent
// copies and replays refuse without invalidating the winner and without
// touching the one-use token. The classification (permanent invalidation,
// active reservation, consumed entry) is decided under the shared mutex BEFORE
// the caller context is examined, so a concurrent or replay loser - even one
// with a nil context - can never poison the live winner. Any genuine
// verification failure permanently invalidates the shared checkpoint without
// reserving the auth stage, and there is no reset or retry rehabilitation.
func (e *borrowedReceiptAuthEntry) Enter(ctx context.Context) error {
	if e == nil || e.state == nil {
		return errors.New("receipt auth entry is absent")
	}
	state := e.state
	// Classification first: invalidation, an active reservation and a consumed
	// entry all refuse before the context is examined.
	state.mu.Lock()
	if state.invalidated {
		state.mu.Unlock()
		return errors.New("receipt auth entry is permanently invalidated")
	}
	if state.entered {
		state.mu.Unlock()
		return errors.New("receipt auth entry was already consumed")
	}
	if state.entering {
		state.mu.Unlock()
		return errors.New("receipt auth entry is already entering the auth stage")
	}
	if ctx == nil {
		// Only the newly classified genuine attempt can latch the permanent
		// context fault; a nil-context loser was already refused above.
		state.invalidated = true
		state.invalidReason = "entry context is missing"
		state.mu.Unlock()
		return errors.New("receipt auth entry requires a bounded context")
	}
	state.entering = true
	state.mu.Unlock()

	if err := state.verifyRetirement(ctx); err != nil {
		state.invalidate("retirement verification refused")
		return err
	}
	if err := state.verifyPrefixWindow(ctx); err != nil {
		state.invalidate("prefix whole-window recheck refused")
		return err
	}
	if err := state.handoff.ConsumeForAuth(ctx); err != nil {
		state.invalidate("receipt handoff auth consumption refused")
		return err
	}
	// Publication linearizes with invalidation/loss under the shared mutex; no
	// I/O is performed while holding it.
	state.pauseAt(borrowedReceiptAuthEntryStageEnterPublish)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.invalidated {
		return errors.New("receipt auth entry was permanently invalidated concurrently")
	}
	if state.entered {
		return errors.New("receipt auth entry was already consumed")
	}
	if err := ctx.Err(); err != nil {
		state.invalidated = true
		state.invalidReason = "context ended before auth entry publication"
		return errors.New("receipt auth entry context ended before publication")
	}
	state.entering = false
	state.entered = true
	return nil
}

// Recheck re-verifies the retained controller after (or before) the one-time
// entry: the dynamic original control anchor recheck, the fresh whole-window
// prefix recheck, the still-authoritative native backend retirement and the
// genuine original control-owner health. Its final publication decision is
// taken under the shared state mutex: a concurrent copy-completed invalidation
// and the caller's own ended context both refuse and permanently invalidate the
// shared checkpoint. It remains usable after the auth consumption for a future
// staged driver; any failure permanently invalidates the shared checkpoint.
func (e *borrowedReceiptAuthEntry) Recheck(ctx context.Context) error {
	if e == nil || e.state == nil {
		return errors.New("receipt auth entry is absent")
	}
	state := e.state
	if reason := state.invalidReasonNow(); reason != "" {
		return errors.New("receipt auth entry is permanently invalidated")
	}
	if ctx == nil {
		state.invalidate("recheck context is missing")
		return errors.New("receipt auth entry recheck requires a bounded context")
	}
	if err := state.handoff.Recheck(ctx); err != nil {
		state.invalidate("retained control anchor recheck refused")
		return err
	}
	if err := state.verifyPrefixWindow(ctx); err != nil {
		state.invalidate("prefix whole-window recheck refused")
		return err
	}
	if err := state.verifyRetirement(ctx); err != nil {
		state.invalidate("retirement verification refused")
		return err
	}
	healthCtx, cancel := context.WithTimeout(ctx, time.Second)
	err := state.fixture.lock.Health(healthCtx)
	cancel()
	if err != nil {
		state.invalidate("original control owner health lost")
		return errors.New("original control owner health lost")
	}
	// Test-only notification/barrier seam: after the genuine Health call and
	// before the shared publication decision.
	state.pauseAt(borrowedReceiptAuthEntryStageRecheckPublish)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.invalidated {
		return errors.New("receipt auth entry was permanently invalidated concurrently")
	}
	if err := ctx.Err(); err != nil {
		state.invalidated = true
		state.invalidReason = "context ended before recheck publication"
		return errors.New("receipt auth entry recheck context ended")
	}
	return nil
}

// stageNow reports the private checkpoint state for diagnostics only: it
// grants no authority and prints no credential material.
func (e *borrowedReceiptAuthEntry) stageNow() (bool, string) {
	if e == nil || e.state == nil {
		return false, "absent"
	}
	state := e.state
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.entered, state.invalidReason
}

// verifyRetirement establishes the actual native backend retirement from the
// gate's genuine owner facts plus the entry's OWN independent strict drain
// proof: completed clean owner sole-wait receipt on the ORIGINAL bound handle,
// published clean retirement, the unchanged gate registration, the strict
// protected control-session row check, the strict in-container PID/start
// parser, the strict full protected-scope socket census, and the protected
// observer row/datOID plus cluster incarnation. It never delegates to the
// gate's shared shell-based drain probe.
func (s *borrowedReceiptAuthEntryState) verifyRetirement(ctx context.Context) error {
	// Gate owner facts are short, non-I/O critical sections: the owner receipt
	// (sole cmd.Wait terminal facts) and the published clean verdict.
	receipt := s.gate.ownerReceipt(s.session)
	if !receipt.Completed || receipt.Pending || !receipt.Clean {
		return errors.New("genuine gate owner receipt is not a completed clean sole-wait success")
	}
	if clean, _ := s.gate.CleanRetired(); !clean {
		return errors.New("gate clean retirement is not published")
	}
	// The session registration is an immutable copy under the session mutex; no
	// I/O is performed while holding it.
	registration, ok := s.session.Registration()
	if !ok {
		return errors.New("gate registration is unavailable")
	}
	if registration.PID != s.registeredPID || !registration.BackendStart.Equal(s.registeredStart) {
		return errors.New("gate registration changed after entry construction")
	}
	if err := s.verifyRegisteredBackendDrain(ctx, registration); err != nil {
		return err
	}
	return s.verifyObserverBackendRowAbsent(ctx)
}

// verifyRegisteredBackendDrain is the entry's independent strict drain proof:
// the exact registered backend row must be gone from the protected control
// session, the in-container PID/start parser must classify the original
// instance as gone or reused (never alive/zombie/unknown), and the strict
// protected-scope socket census must prove the registered socket inode absent.
// Every context is caller-derived and bounded, every mutex acquisition is
// context-aware, and any unknown/error condition refuses.
func (s *borrowedReceiptAuthEntryState) verifyRegisteredBackendDrain(ctx context.Context, registration *originGateRegistration) error {
	if registration.OSStart == "" || registration.SocketInode == "" {
		return errors.New("registered backend OS/socket identity is incomplete")
	}
	// Protected control-session row check: context-aware serialization with a
	// caller-derived bound; no I/O under any shared state mutex.
	controlCtx, cancelControl := context.WithTimeout(ctx, 3*time.Second)
	defer cancelControl()
	if err := borrowedReceiptAuthAcquireMu(controlCtx, &s.gate.controlMu); err != nil {
		return errors.New("strict control-session serialization was not acquired")
	}
	defer s.gate.controlMu.Unlock()
	if s.gate.control == nil {
		return errors.New("independent protected control session is unavailable")
	}
	var (
		backendStart time.Time
		backendType  string
	)
	err := s.gate.control.QueryRow(controlCtx, `SELECT backend_start, backend_type FROM pg_stat_activity WHERE pid=$1`, registration.PID).
		Scan(&backendStart, &backendType)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// The exact registered row is gone.
	case err != nil:
		return errors.New("strict control backend-row query refused")
	default:
		if backendType == "client backend" && backendStart.Equal(registration.BackendStart) {
			return errors.New("registered native server backend row is still present")
		}
		// A validated different backend_start (or a non-client backend type) is
		// a PID-reused replacement: the registered instance is gone.
	}
	if err := controlCtx.Err(); err != nil {
		return errors.New("strict control backend-row context ended")
	}

	// Strict in-container PID/start parser: only ENOENT (gone) or a valid
	// different numeric start (reuse) authorizes disappearance; a same-identity
	// zombie is explicitly NOT reaped and any error/malformed document refuses.
	stage, err := s.strictContainerProcessStage(ctx, registration.PID, registration.OSStart)
	if err != nil {
		return err
	}
	switch stage {
	case borrowedReceiptAuthEntryProcessGone, borrowedReceiptAuthEntryProcessReused:
	case borrowedReceiptAuthEntryProcessAlive:
		return errors.New("registered native server backend OS process is still alive")
	case borrowedReceiptAuthEntryProcessZombie:
		return errors.New("registered native server backend OS process is an unreaped zombie")
	default:
		return errors.New("registered native server backend OS process identity is unknown")
	}

	return s.strictContainerSocketCensus(ctx, registration.PID, registration.OSStart, registration.SocketInode)
}

// borrowedReceiptAuthEntryProcessStage is the strict in-container process
// classification of the registered native server backend.
type borrowedReceiptAuthEntryProcessStage int

const (
	borrowedReceiptAuthEntryProcessUnknown borrowedReceiptAuthEntryProcessStage = iota
	borrowedReceiptAuthEntryProcessGone
	borrowedReceiptAuthEntryProcessReused
	borrowedReceiptAuthEntryProcessAlive
	borrowedReceiptAuthEntryProcessZombie
)

// strictContainerProcessStage executes the entry-private probe in the owned
// container under the caller-derived bounded context and classifies the exact
// registered PID/start identity. The expected start token must itself be a
// positive numeric identity; the probe output must be an explicit stage with a
// validated numeric start; anything else (probe error, unknown stage,
// malformed report, explicit errno) refuses.
func (s *borrowedReceiptAuthEntryState) strictContainerProcessStage(ctx context.Context, pid int, osStart string) (borrowedReceiptAuthEntryProcessStage, error) {
	if pid <= 0 || osStart == "" {
		return borrowedReceiptAuthEntryProcessUnknown, errors.New("registered backend PID/start identity is missing")
	}
	if start, err := strconv.ParseUint(osStart, 10, 64); err != nil || start == 0 {
		return borrowedReceiptAuthEntryProcessUnknown, errors.New("registered backend OS start token is not a positive numeric identity")
	}
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out, err := dockerExec(probeCtx, s.fixture.fx.containerID, borrowedReceiptAuthEntryProbePath, "pidstat", strconv.Itoa(pid), osStart)
	if err != nil {
		return borrowedReceiptAuthEntryProcessUnknown, errors.New("strict container PID/start probe refused")
	}
	fields := strings.Fields(strings.TrimSpace(string(out)))
	if len(fields) < 2 || fields[0] != "STAGE" {
		return borrowedReceiptAuthEntryProcessUnknown, errors.New("strict container PID/start probe report is unrecognized")
	}
	switch fields[1] {
	case "GONE":
		if len(fields) != 2 {
			return borrowedReceiptAuthEntryProcessUnknown, errors.New("strict container PID/start probe GONE report is malformed")
		}
		return borrowedReceiptAuthEntryProcessGone, nil
	case "REUSED", "ALIVE", "ZOMBIE":
		if len(fields) != 3 {
			return borrowedReceiptAuthEntryProcessUnknown, errors.New("strict container PID/start probe identity report is malformed")
		}
		start, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil || start == 0 {
			return borrowedReceiptAuthEntryProcessUnknown, errors.New("strict container PID/start probe returned a non-numeric start identity")
		}
		switch fields[1] {
		case "REUSED":
			return borrowedReceiptAuthEntryProcessReused, nil
		case "ALIVE":
			return borrowedReceiptAuthEntryProcessAlive, nil
		default:
			return borrowedReceiptAuthEntryProcessZombie, nil
		}
	case "MALFORMED", "UNKNOWN":
		return borrowedReceiptAuthEntryProcessUnknown, errors.New("strict container PID/start probe reported an unknown identity")
	default:
		return borrowedReceiptAuthEntryProcessUnknown, errors.New("strict container PID/start probe returned an unrecognized stage")
	}
}

// strictContainerSocketCensus executes the entry-private strict census in the
// owned container under the caller-derived bounded context. The probe
// re-validates the postmaster before and after the full direct-child scan,
// classifies every fd, strictly parses both TCP tables and only reports OK when
// the exact registered PID/start instance and socket inode are absent from the
// protected scope. The retained postmaster start token must match the
// before/after census tokens.
func (s *borrowedReceiptAuthEntryState) strictContainerSocketCensus(ctx context.Context, pid int, osStart, inode string) error {
	fx := s.fixture.fx
	if fx.postmasterPID <= 0 || fx.postmasterStr == "" || inode == "" {
		return errors.New("strict socket census requires the postmaster and registered socket tokens")
	}
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := dockerExec(probeCtx, fx.containerID, borrowedReceiptAuthEntryProbePath, "census",
		strconv.Itoa(fx.postmasterPID), fx.postmasterStr, strconv.Itoa(pid), osStart, inode)
	if err != nil {
		return errors.New("strict container socket census refused")
	}
	fields := strings.Fields(strings.TrimSpace(string(out)))
	if len(fields) < 2 || fields[0] != "CENSUS" {
		return errors.New("strict container socket census report is unrecognized")
	}
	switch fields[1] {
	case "OK":
		report := make(map[string]string, len(fields)-2)
		for _, field := range fields[2:] {
			key, value, ok := strings.Cut(field, "=")
			if !ok || key == "" {
				return errors.New("strict container socket census report is malformed")
			}
			report[key] = value
		}
		if report["pid_absent"] != "1" || report["inode_absent"] != "1" {
			return errors.New("strict container socket census did not prove the registered backend drain")
		}
		wantStart, err := strconv.ParseUint(fx.postmasterStr, 10, 64)
		before, errBefore := strconv.ParseUint(report["postmaster_start_before"], 10, 64)
		after, errAfter := strconv.ParseUint(report["postmaster_start_after"], 10, 64)
		if err != nil || errBefore != nil || errAfter != nil || before != wantStart || after != wantStart {
			return errors.New("strict container socket census postmaster incarnation changed")
		}
		return nil
	case "PRESENT":
		return errors.New("registered native server backend socket identity is still present in the protected census")
	case "UNKNOWN":
		return errors.New("strict container socket census reported UNKNOWN")
	default:
		return errors.New("strict container socket census returned an unrecognized verdict")
	}
}

// verifyObserverBackendRowAbsent re-reads the exact registered backend row
// through the retained protected observer under a caller-derived bounded
// context and context-aware serialization acquisition: the registered
// pid/start/datOID instance must be gone (row absent or a validated different
// backend_start), and the protected cluster SQL incarnation must still match
// the prefix/control-anchor capture. Any query/probe/context error refuses; it
// is never treated as disappearance, and caller cancellation is never reported
// temporarily healthy.
func (s *borrowedReceiptAuthEntryState) verifyObserverBackendRowAbsent(ctx context.Context) error {
	g := s.gate
	observerCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := borrowedReceiptAuthAcquireMu(observerCtx, &g.observerMu); err != nil {
		return errors.New("protected observer serialization was not acquired")
	}
	defer g.observerMu.Unlock()
	if g.observer == nil {
		return errors.New("protected observer session is unavailable")
	}
	var (
		backendStart time.Time
		databaseOID  uint32
		backendType  string
	)
	err := g.observer.QueryRow(observerCtx, `SELECT backend_start, datid::oid, backend_type FROM pg_stat_activity WHERE pid=$1`, s.registeredPID).
		Scan(&backendStart, &databaseOID, &backendType)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// The exact registered row is gone; there is no datOID instance left.
	case err != nil:
		return errors.New("protected observer backend row query refused")
	default:
		if backendStart.Equal(s.registeredStart) {
			return errors.New("registered native backend row is still present")
		}
		// A validated different backend_start is a PID-reused replacement:
		// the registered instance (its pid/start/datOID) is gone.
	}
	if err := observerCtx.Err(); err != nil {
		return errors.New("protected observer backend row context ended")
	}
	var (
		postmasterStart time.Time
		systemID        string
	)
	if err := g.observer.QueryRow(observerCtx, `SELECT pg_postmaster_start_time(), system_identifier::text FROM pg_control_system()`).
		Scan(&postmasterStart, &systemID); err != nil {
		return errors.New("protected cluster incarnation query refused")
	}
	if !postmasterStart.Equal(s.postmasterStart) || systemID == "" || systemID != s.systemID {
		return errors.New("protected cluster incarnation changed")
	}
	return nil
}

// verifyPrefixWindow runs the fresh bounded whole-window prefix inspection
// (independent SQL, strict OS census, postmaster incarnation and retained
// control-anchor recheck); any loss, mismatch or context end refuses.
func (s *borrowedReceiptAuthEntryState) verifyPrefixWindow(ctx context.Context) error {
	bounded, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if err := s.prefix.inspect(bounded); err != nil {
		return errors.New("prefix whole-window recheck refused")
	}
	if err := bounded.Err(); err != nil {
		return errors.New("prefix whole-window recheck context ended")
	}
	return nil
}

func (s *borrowedReceiptAuthEntryState) invalidate(reason string) {
	s.mu.Lock()
	if !s.invalidated {
		s.invalidated = true
		s.invalidReason = reason
	}
	s.mu.Unlock()
}

func (s *borrowedReceiptAuthEntryState) invalidReasonNow() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.invalidated {
		return ""
	}
	return s.invalidReason
}

// pauseAt invokes the optional test-only notification/barrier seam. It is
// called outside every shared mutex and performs no I/O of its own.
func (s *borrowedReceiptAuthEntryState) pauseAt(stage borrowedReceiptAuthEntryStage) {
	if s.stage != nil {
		s.stage(stage)
	}
}

// borrowedReceiptAuthAcquireMu acquires a shared adapter mutex within the
// caller-bounded context using TryLock plus a bounded backoff, so a held
// serialization lock can never strand a goroutine and caller cancellation
// always wins. It mirrors the existing identity-prefix acquisition.
func borrowedReceiptAuthAcquireMu(ctx context.Context, mu *sync.Mutex) error {
	if ctx == nil || mu == nil {
		return errors.New("bounded mutex acquisition requires a context and mutex")
	}
	if mu.TryLock() {
		return nil
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if mu.TryLock() {
				return nil
			}
		}
	}
}

// buildBorrowedReceiptAuthEntryProbe compiles the entry-private strict probe
// from the ignore-tagged helper source as a static Go binary.
func buildBorrowedReceiptAuthEntryProbe(t *testing.T) string {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate entry test source for the strict container probe")
	}
	source := filepath.Join(filepath.Dir(testFile), "borrowed-receipt-auth-entry-probe_linux_testhelper.go")
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod strict probe build directory: %v", err)
	}
	output := filepath.Join(dir, "borrowed-receipt-auth-entry-probe")
	cmd := exec.Command("go", "build", "-trimpath", "-o", output, source)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if result, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build strict container probe: %v: %s", err, strings.TrimSpace(string(result)))
	}
	return output
}

// installBorrowedReceiptAuthEntryProbe copies the compiled strict probe into
// the owned disposable fixture container at its private path.
func installBorrowedReceiptAuthEntryProbe(t *testing.T, f *borrowedAuthHandoffFixture) {
	t.Helper()
	if f == nil || f.fx == nil || f.fx.container == nil {
		t.Fatal("strict container probe installation requires the owned fixture container")
	}
	probe := buildBorrowedReceiptAuthEntryProbe(t)
	if err := f.fx.container.CopyFileToContainer(context.Background(), probe, borrowedReceiptAuthEntryProbePath, 0o700); err != nil {
		t.Fatalf("copy strict container probe into the owned fixture: %v", err)
	}
}

// waitBorrowedGateRetirement waits, bounded, for either a published clean gate
// retirement or a latch. It reports the safe reason only.
func waitBorrowedGateRetirement(f *borrowedAuthHandoffFixture, timeout time.Duration) (bool, string) {
	deadline := time.Now().Add(timeout)
	for {
		if clean, report := f.gate.CleanRetired(); clean {
			return true, report
		}
		if latched, reason := f.gate.Latched(); latched {
			return false, "latched: " + reason
		}
		if time.Now().After(deadline) {
			return false, "deadline waiting for gate retirement"
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitBorrowedOwnerReceipt waits, bounded, for the gate's genuine owner
// terminal verdict on the actual session.
func waitBorrowedOwnerReceipt(g *originGate, session *originGateSession, timeout time.Duration) originGateOwnerReceipt {
	deadline := time.Now().Add(timeout)
	for {
		receipt := g.ownerReceipt(session)
		if receipt.Completed {
			return receipt
		}
		if time.Now().After(deadline) {
			return receipt
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// borrowedReceiptHandoffOutcome is one RunForReceiptHandoff result tuple.
type borrowedReceiptHandoffOutcome struct {
	result  recovery.TargetWriterResult
	handoff recovery.OpaqueDrillReceiptHandoff
	err     error
}

// borrowedReceiptAuthEntrySeam is the test-only blocking notification barrier
// installed on one entry state. It blocks the reaching goroutine at the named
// publication stage until released, and it is released exactly once at cleanup
// so a fatal can never strand the goroutine.
type borrowedReceiptAuthEntrySeam struct {
	stage       borrowedReceiptAuthEntryStage
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func installBorrowedReceiptAuthEntrySeam(t *testing.T, entry *borrowedReceiptAuthEntry, stage borrowedReceiptAuthEntryStage) *borrowedReceiptAuthEntrySeam {
	t.Helper()
	seam := &borrowedReceiptAuthEntrySeam{stage: stage, entered: make(chan struct{}), release: make(chan struct{})}
	entry.state.stage = func(current borrowedReceiptAuthEntryStage) {
		if current != seam.stage {
			return
		}
		seam.enteredOnce.Do(func() { close(seam.entered) })
		<-seam.release
	}
	t.Cleanup(seam.releaseSeam)
	return seam
}

func (s *borrowedReceiptAuthEntrySeam) releaseSeam() {
	s.releaseOnce.Do(func() { close(s.release) })
}

// TestBorrowedReceiptAuthEntryNativeRetirement proves the whole positive chain:
// the actual restricted-W run held at its first executable frame with zero
// effects, the released native restore with the deliberate Probe rejection and
// no acceptance, the genuine opaque handoff token with the original binding,
// the actual gate clean retirement and strict registered-backend drain, and the
// private one-use auth entry (atomic concurrent reservation, shared
// invalidation, single consumption, post-consumption recheck, original control
// health and W-bound session).
func TestBorrowedReceiptAuthEntryNativeRetirement(t *testing.T) {
	f := newBorrowedAuthHandoffFixture(t)
	installBorrowedReceiptAuthEntryProbe(t, f)
	ctx := context.Background()
	binding := f.run.Binding()

	f.gate.HoldRegistration()
	resultCh := make(chan borrowedReceiptHandoffOutcome, 1)
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go func() {
		result, handoff, err := f.run.RunForReceiptHandoff(runCtx)
		resultCh <- borrowedReceiptHandoffOutcome{result, handoff, err}
	}()
	admitCtx, cancelAdmit := context.WithTimeout(ctx, 90*time.Second)
	defer cancelAdmit()
	session, err := f.gate.AdmitBorrowed(admitCtx, f.run, f.prefix)
	if err != nil {
		cancelRun()
		t.Fatalf("actual borrowed admission: %v", err)
	}
	heldDeadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(heldDeadline) && !(session.BackendPID() > 0 && session.BufferedFrames() > 0) {
		time.Sleep(20 * time.Millisecond)
	}
	if !(session.BackendPID() > 0 && session.BufferedFrames() > 0) {
		cancelRun()
		t.Fatal("actual native child never reached the held first executable frame")
	}
	if atomic.LoadInt32(&f.probeCalls) != 0 || atomic.LoadInt32(&f.acceptanceCalls) != 0 {
		cancelRun()
		t.Fatalf("held first frame already ran probe/acceptance: probe=%d acceptance=%d", f.probeCalls, f.acceptanceCalls)
	}
	targetConn, err := pgx.Connect(ctx, f.writerTargetDSN)
	if err != nil {
		cancelRun()
		t.Fatalf("connect W target during hold (sqlstate=%s, W role OID %d)", borrowedAuthSQLState(err), f.writerRoleOID)
	}
	var absent bool
	if err := targetConn.QueryRow(ctx, `SELECT to_regclass($1) IS NULL`, "public."+f.table).Scan(&absent); err != nil || !absent {
		_ = targetConn.Close(ctx)
		cancelRun()
		t.Fatalf("target relation present during hold: absent=%t err=%v", absent, err)
	}
	f.gate.ReleaseRegistration()
	releaseDeadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(releaseDeadline) && !session.WatcherReady() {
		time.Sleep(20 * time.Millisecond)
	}
	if !session.WatcherReady() {
		_ = targetConn.Close(ctx)
		cancelRun()
		t.Fatal("actual watcher/admission checks never became ready")
	}
	var outcome borrowedReceiptHandoffOutcome
	select {
	case outcome = <-resultCh:
	case <-time.After(150 * time.Second):
		_ = targetConn.Close(ctx)
		cancelRun()
		t.Fatal("actual receipt-handoff run did not return")
	}
	if outcome.err == nil {
		t.Fatal("deliberate Probe rejection was converted to a nil error")
	}
	facts := outcome.handoff.Diagnostics()
	if !facts.Present || facts.Invalidated {
		t.Fatalf("actual native success produced no handoff token: %+v err=%v", facts, outcome.err)
	}
	if facts.OriginalKey != binding.OriginalTargetKey() || facts.ControlKey != binding.ControlTargetKey() ||
		facts.RoleFingerprint != binding.OriginalRoleFingerprint() || facts.OperationID != binding.OriginalOperationID() ||
		facts.WriterRoleOID != f.writerRoleOID || facts.InstanceID != "" || binding.OriginalInstanceID() != "" {
		t.Fatalf("handoff token lost the immutable original binding (empty instance disclosed): %+v", facts)
	}
	if facts.ChildPID <= 0 || facts.ChildStartID == 0 {
		t.Fatalf("handoff token has no frozen child identity: %+v", facts)
	}
	if stage := outcome.handoff.GateRetirementStage(); stage != recovery.DrillReceiptHandoffGateRetirementStage {
		t.Fatalf("handoff token claimed gate retirement instead of the fixed external requirement: %q", stage)
	}
	var rows int
	if err := targetConn.QueryRow(ctx, `SELECT count(*) FROM public.`+pgx.Identifier{f.table}.Sanitize()).Scan(&rows); err != nil || rows != 3 {
		_ = targetConn.Close(ctx)
		t.Fatalf("actual native restore rows=%d err=%v, want 3", rows, err)
	}
	_ = targetConn.Close(ctx)
	if atomic.LoadInt32(&f.probeCalls) != 1 || atomic.LoadInt32(&f.acceptanceCalls) != 0 {
		t.Fatalf("probe/acceptance counts: probe=%d acceptance=%d", f.probeCalls, f.acceptanceCalls)
	}
	var disposition string
	if err := f.controlPool.QueryRow(ctx, `SELECT disposition FROM recovery_target_guard WHERE target_guard_key=$1`, f.guardKey).Scan(&disposition); err != nil {
		t.Fatalf("guard state: %v", err)
	}
	if disposition == "clean" {
		t.Fatal("rejected probe left a clean guard")
	}
	if err := f.lock.Health(ctx); err != nil {
		t.Fatalf("original control lock no longer healthy after the native run: %v", err)
	}

	// Actual gate retirement: completed clean owner sole-wait receipt on the
	// ORIGINAL bound handle plus authoritative registered-backend drain.
	clean, cleanReport := waitBorrowedGateRetirement(f, 30*time.Second)
	if !clean {
		t.Fatalf("actual gate did not publish clean retirement: %s", cleanReport)
	}
	receipt := waitBorrowedOwnerReceipt(f.gate, session, 10*time.Second)
	if !receipt.Completed || receipt.Pending || !receipt.Clean {
		t.Fatalf("gate owner receipt is not a completed clean sole-wait success: %+v", receipt)
	}
	registration, ok := session.Registration()
	if !ok {
		t.Fatal("gate registration is unavailable after retirement")
	}
	gone, err := f.gate.authoritativeBackendGone(registration.PID, registration.OSStart, registration.SocketInode, registration.BackendStart)
	if err != nil {
		t.Fatalf("strict gate backend-drain inspection refused: %v", err)
	}
	if !gone {
		t.Fatalf("registered native server backend %d is not authoritatively gone", registration.PID)
	}

	// Private one-use auth entry with the deterministic concurrent reservation
	// proof: the winner is held after every genuine verification I/O (including
	// the real health checks) at the pre-publication seam while a copy attempts
	// the same entry. The copy must refuse without invalidating the winner or
	// consuming the shared token; exactly one entry is published.
	entry, err := newBorrowedReceiptAuthEntry(f, session, outcome.handoff)
	if err != nil {
		t.Fatalf("receipt auth entry construction: %v", err)
	}
	enterCtx, cancelEnter := context.WithTimeout(ctx, 15*time.Second)
	defer cancelEnter()
	seam := installBorrowedReceiptAuthEntrySeam(t, entry, borrowedReceiptAuthEntryStageEnterPublish)
	winnerCh := make(chan error, 1)
	go func() { winnerCh <- entry.Enter(enterCtx) }()
	select {
	case <-seam.entered:
	case <-time.After(60 * time.Second):
		t.Fatal("first auth-stage entry never reached the pre-publication seam")
	}
	copied := *entry
	if err := copied.Enter(enterCtx); err == nil {
		t.Fatal("a concurrent copy entered the auth stage while the winner held the reservation")
	}
	// RE02 P2: a nil-context replay loser must be classified as a loser (the
	// live reservation wins) and must never invalidate the winner.
	if err := copied.Enter(nil); err == nil {
		t.Fatal("a nil-context copy entered the auth stage while the winner held the reservation")
	}
	if valid, reason := outcome.handoff.Valid(); !valid || reason != "" {
		t.Fatalf("concurrent entry loser invalidated the shared handoff token: valid=%t reason=%q", valid, reason)
	}
	if entered, reason := entry.stageNow(); entered || reason != "" {
		t.Fatalf("concurrent entry loser changed the winner checkpoint: entered=%t reason=%q", entered, reason)
	}
	seam.releaseSeam()
	select {
	case winnerErr := <-winnerCh:
		if winnerErr != nil {
			t.Fatalf("first auth-stage entry: %v", winnerErr)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("reservation winner did not publish the auth entry")
	}
	if entered, reason := entry.stageNow(); !entered || reason != "" {
		t.Fatalf("checkpoint state after first entry: entered=%t reason=%q", entered, reason)
	}
	if !outcome.handoff.Diagnostics().Consumed {
		t.Fatal("the one-use handoff token was not consumed exactly once by the winner")
	}
	if err := copied.Enter(enterCtx); err == nil {
		t.Fatal("a copy consumed the one-use auth entry again")
	}
	// RE02 P2: a nil-context replay after consumption must be refused WITHOUT
	// poisoning the consumed checkpoint, so the winner's post-consumption
	// Recheck stays usable.
	if err := copied.Enter(nil); err == nil {
		t.Fatal("a nil-context copy consumed the one-use auth entry again")
	}
	if entered, reason := entry.stageNow(); !entered || reason != "" {
		t.Fatalf("nil-context post-consumption replay poisoned the winner checkpoint: entered=%t reason=%q", entered, reason)
	}
	if err := entry.Enter(enterCtx); err == nil {
		t.Fatal("the same checkpoint entered the auth stage twice")
	}
	if err := entry.Recheck(enterCtx); err != nil {
		t.Fatalf("post-consumption checkpoint recheck: %v", err)
	}
	if err := f.lock.Health(ctx); err != nil {
		t.Fatalf("original control session health after entry: %v", err)
	}
	report := session.Report()
	if !report.ServerAuthOK || report.BoundRole != f.writerRole || report.BoundDatabase != f.targetDB {
		t.Fatalf("gate session bound identity is not the actual W target: %+v", report)
	}
	if f.prefix.capturedRoleOID != f.writerRoleOID || f.prefix.capturedTargetDBOID != binding.TargetDatabaseOID() {
		t.Fatal("prefix capture does not bind the actual W role/target catalog OIDs")
	}
	if f.prefix.capturedFacts.OriginalTargetKey != binding.OriginalTargetKey() ||
		f.prefix.capturedFacts.ControlTargetKey != binding.ControlTargetKey() {
		t.Fatal("prefix control anchor does not retain the original/control keys")
	}

	// Actual foreign-gate session with the authentic completed-run token: the
	// constructor must refuse a session issued by another actual gate; the
	// token itself is never a manually constructed structure.
	foreignGate := newOriginGate(t, ctx, f.fx)
	launcher := newOriginGateLauncher(t)
	_, cap, err := launcher.launch(t, ctx, foreignGate,
		[]string{"auth", foreignGate.Endpoint(), f.fx.role, "none", originGateAppName}, f.fx.password)
	if err != nil {
		t.Fatalf("spawn foreign-gate owned client: %v", err)
	}
	foreignSession, err := foreignGate.Admit(ctx, cap)
	if err != nil {
		t.Fatalf("foreign-gate actual admission: %v", err)
	}
	if foreignSession.g == f.gate {
		t.Fatal("foreign-gate session is not actually foreign")
	}
	if foreignEntry, err := newBorrowedReceiptAuthEntry(f, foreignSession, outcome.handoff); err == nil || foreignEntry != nil {
		t.Fatal("constructor accepted an actual foreign-gate session with the authentic token")
	}

	// Context contention at the observer serialization: a held observer mutex
	// must make the context-aware acquisition refuse at the caller deadline and
	// the recheck must permanently invalidate its shared checkpoint, with no
	// hanging goroutine and without stranding the gate's own cleanup. The
	// reconstructed checkpoint reuses the fixture-owned shared state, so the
	// winner's consumption is shared and the later invalidation reaches the
	// original entry too.
	contentionEntry, err := newBorrowedReceiptAuthEntry(f, session, outcome.handoff)
	if err != nil {
		t.Fatalf("contention checkpoint construction: %v", err)
	}
	if contentionEntry.state != entry.state {
		t.Fatal("reconstructed checkpoint did not reuse the original fixture-bound state")
	}
	var observerHeld atomic.Bool
	lockObserver := func() {
		f.gate.observerMu.Lock()
		observerHeld.Store(true)
	}
	releaseObserver := func() {
		if observerHeld.CompareAndSwap(true, false) {
			f.gate.observerMu.Unlock()
		}
	}
	// The cleanup is registered before the first acquisition so a fatal can
	// never strand the gate's own cleanup on the observer mutex; it releases
	// exactly the currently held acquisition.
	t.Cleanup(releaseObserver)
	lockObserver()
	directCtx, cancelDirect := context.WithTimeout(ctx, 700*time.Millisecond)
	directCh := make(chan error, 1)
	go func() { directCh <- contentionEntry.state.verifyObserverBackendRowAbsent(directCtx) }()
	var directErr error
	select {
	case directErr = <-directCh:
	case <-time.After(15 * time.Second):
		cancelDirect()
		releaseObserver()
		t.Fatal("observer serialization acquisition ignored the bounded context")
	}
	cancelDirect()
	releaseObserver()
	if directErr == nil {
		t.Fatal("observer row check succeeded while the observer mutex was held")
	}
	if entered, reason := contentionEntry.stageNow(); !entered || reason != "" {
		t.Fatalf("raw observer contention changed the shared checkpoint: entered=%t reason=%q", entered, reason)
	}
	recheckCtx, cancelRecheck := context.WithTimeout(ctx, 30*time.Second)
	defer cancelRecheck()
	if err := contentionEntry.Recheck(recheckCtx); err != nil {
		t.Fatalf("checkpoint was not usable after the raw contention probe: %v", err)
	}
	lockObserver()
	shortCtx, cancelShort := context.WithTimeout(ctx, 5*time.Second)
	shortCh := make(chan error, 1)
	go func() { shortCh <- contentionEntry.Recheck(shortCtx) }()
	var shortErr error
	select {
	case shortErr = <-shortCh:
	case <-time.After(30 * time.Second):
		cancelShort()
		releaseObserver()
		t.Fatal("short-deadline recheck hung on observer serialization")
	}
	cancelShort()
	releaseObserver()
	if shortErr == nil {
		t.Fatal("short-deadline recheck succeeded while the observer mutex was held")
	}
	contentionEntered, contentionReason := contentionEntry.stageNow()
	if !contentionEntered || contentionReason == "" {
		t.Fatalf("short-deadline observer contention did not permanently invalidate the shared checkpoint: entered=%t reason=%q", contentionEntered, contentionReason)
	}
	if _, originalReason := entry.stageNow(); originalReason == "" {
		t.Fatal("short-deadline observer contention loss is not shared with the original entry")
	}
	t.Logf("observer contention refusal stage: %s", contentionReason)
}

// TestBorrowedReceiptAuthEntryHeldCancelRefusal proves the held-cancellation
// refusal: the actual run is canceled at its held first executable frame
// WITHOUT releasing the registration barrier, so it produces no token and zero
// writes, the genuine child sole wait completes as a drained cancellation, the
// gate never publishes clean retirement, and the private entry refuses the
// absent token even though the gate drain may be verified.
func TestBorrowedReceiptAuthEntryHeldCancelRefusal(t *testing.T) {
	f := newBorrowedAuthHandoffFixture(t)
	ctx := context.Background()

	f.gate.HoldRegistration()
	defer f.gate.ReleaseRegistration()
	resultCh := make(chan borrowedReceiptHandoffOutcome, 1)
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go func() {
		result, handoff, err := f.run.RunForReceiptHandoff(runCtx)
		resultCh <- borrowedReceiptHandoffOutcome{result, handoff, err}
	}()
	admitCtx, cancelAdmit := context.WithTimeout(ctx, 90*time.Second)
	defer cancelAdmit()
	session, err := f.gate.AdmitBorrowed(admitCtx, f.run, f.prefix)
	if err != nil {
		cancelRun()
		t.Fatalf("actual borrowed admission: %v", err)
	}
	heldDeadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(heldDeadline) && !(session.BackendPID() > 0 && session.BufferedFrames() > 0) {
		time.Sleep(20 * time.Millisecond)
	}
	if !(session.BackendPID() > 0 && session.BufferedFrames() > 0) {
		cancelRun()
		t.Fatal("actual native child never reached the held first executable frame")
	}
	// Cancel the actual run context while the first executable frame is still
	// held and the registration barrier is still installed.
	cancelRun()
	var outcome borrowedReceiptHandoffOutcome
	select {
	case outcome = <-resultCh:
	case <-time.After(150 * time.Second):
		t.Fatal("canceled held receipt-handoff run did not return")
	}
	if outcome.err == nil {
		t.Fatal("held cancellation was converted to a nil error")
	}
	if outcome.handoff.Diagnostics().Present {
		t.Fatal("held cancellation produced a handoff token")
	}
	if outcome.result.Command.Outcome != recovery.PGCommandCanceled || !outcome.result.Command.ProcessGroupDrained {
		t.Fatalf("held cancellation is not a genuine drained cancel: %+v", outcome.result.Command)
	}
	targetConn, err := pgx.Connect(ctx, f.writerTargetDSN)
	if err != nil {
		t.Fatalf("connect W target after held cancel (sqlstate=%s, W role OID %d)", borrowedAuthSQLState(err), f.writerRoleOID)
	}
	var absent bool
	if err := targetConn.QueryRow(ctx, `SELECT to_regclass($1) IS NULL`, "public."+f.table).Scan(&absent); err != nil || !absent {
		_ = targetConn.Close(ctx)
		t.Fatalf("held cancellation wrote to the target: absent=%t err=%v", absent, err)
	}
	_ = targetConn.Close(ctx)
	if atomic.LoadInt32(&f.probeCalls) != 0 || atomic.LoadInt32(&f.acceptanceCalls) != 0 {
		t.Fatalf("held cancellation ran probe/acceptance: probe=%d acceptance=%d", f.probeCalls, f.acceptanceCalls)
	}
	receipt := waitBorrowedOwnerReceipt(f.gate, session, 30*time.Second)
	if !receipt.Completed {
		t.Fatalf("gate never observed the canceled child sole wait: %+v", receipt)
	}
	if receipt.Clean {
		t.Fatal("canceled held child was reported as a clean success")
	}
	// The cancellation happened while the first executable frame was still
	// held and no frame was ever delivered to the protected server (zero
	// writes above). Releasing the held barrier now only lets the gate observe
	// the already-dead client and publish its terminal verdict: it must latch
	// (non-clean terminal loss) and must never clean-retire.
	f.gate.ReleaseRegistration()
	latchDeadline := time.Now().Add(20 * time.Second)
	latched := false
	for time.Now().Before(latchDeadline) {
		if clean, _ := f.gate.CleanRetired(); clean {
			t.Fatal("canceled held run published clean gate retirement")
		}
		if l, _ := f.gate.Latched(); l {
			latched = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !latched {
		t.Fatal("canceled held run did not latch after the held barrier was released")
	}
	// The gate drain may be verified, but an absent token can never grant the
	// private entry: the constructor refuses without a token.
	entry, entryErr := newBorrowedReceiptAuthEntry(f, session, outcome.handoff)
	if entryErr == nil || entry != nil {
		t.Fatal("absent handoff token constructed an auth entry")
	}
	t.Logf("held-cancel refusal: token absent, owner receipt completed=%t clean=%t, gate latched=%t; entry refused absent token",
		receipt.Completed, receipt.Clean, latched)
}

// TestBorrowedReceiptAuthEntryControlOwnerLoss proves the owner-loss refusal:
// a genuine positive run and retirement first, then the actual captured control
// owner is terminated through the control role BEFORE entry. The entry must
// refuse, the underlying token must fail its recheck and become permanently
// invalid for every copy, and no P0 rotation, DDL, extra target write or
// acceptance may occur.
func TestBorrowedReceiptAuthEntryControlOwnerLoss(t *testing.T) {
	f := newBorrowedAuthHandoffFixture(t)
	installBorrowedReceiptAuthEntryProbe(t, f)
	ctx := context.Background()

	f.gate.HoldRegistration()
	resultCh := make(chan borrowedReceiptHandoffOutcome, 1)
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go func() {
		result, handoff, err := f.run.RunForReceiptHandoff(runCtx)
		resultCh <- borrowedReceiptHandoffOutcome{result, handoff, err}
	}()
	admitCtx, cancelAdmit := context.WithTimeout(ctx, 90*time.Second)
	defer cancelAdmit()
	session, err := f.gate.AdmitBorrowed(admitCtx, f.run, f.prefix)
	if err != nil {
		cancelRun()
		t.Fatalf("actual borrowed admission: %v", err)
	}
	heldDeadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(heldDeadline) && !(session.BackendPID() > 0 && session.BufferedFrames() > 0) {
		time.Sleep(20 * time.Millisecond)
	}
	if !(session.BackendPID() > 0 && session.BufferedFrames() > 0) {
		cancelRun()
		t.Fatal("actual native child never reached the held first executable frame")
	}
	f.gate.ReleaseRegistration()
	releaseDeadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(releaseDeadline) && !session.WatcherReady() {
		time.Sleep(20 * time.Millisecond)
	}
	if !session.WatcherReady() {
		cancelRun()
		t.Fatal("actual watcher/admission checks never became ready")
	}
	var outcome borrowedReceiptHandoffOutcome
	select {
	case outcome = <-resultCh:
	case <-time.After(150 * time.Second):
		cancelRun()
		t.Fatal("actual receipt-handoff run did not return")
	}
	if outcome.err == nil || !outcome.handoff.Diagnostics().Present {
		t.Fatalf("owner-loss positive token missing: err=%v facts=%+v", outcome.err, outcome.handoff.Diagnostics())
	}
	clean, cleanReport := waitBorrowedGateRetirement(f, 30*time.Second)
	if !clean {
		t.Fatalf("actual gate did not publish clean retirement before owner loss: %s", cleanReport)
	}

	// Terminate the actual captured control owner through the control role.
	controlPID := f.run.Binding().ControlBackendPID()
	if controlPID <= 0 {
		t.Fatal("captured control owner PID is missing")
	}
	var terminated bool
	if err := f.controlPool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, controlPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate captured control owner: terminated=%t err=%v", terminated, err)
	}
	ownerDeadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(ownerDeadline) {
		var present int
		if err := f.controlPool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE pid=$1`, controlPID).Scan(&present); err != nil {
			break
		}
		if present == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	var verifierBefore string
	if err := f.fx.admin.QueryRow(ctx, `SELECT coalesce(rolpassword,'') FROM pg_authid WHERE rolname=$1`, f.writerRole).Scan(&verifierBefore); err != nil {
		t.Fatalf("read W verifier before owner loss entry: %v", err)
	}

	entry, err := newBorrowedReceiptAuthEntry(f, session, outcome.handoff)
	if err != nil {
		t.Fatalf("receipt auth entry construction before owner loss: %v", err)
	}
	enterCtx, cancelEnter := context.WithTimeout(ctx, 15*time.Second)
	defer cancelEnter()
	if err := entry.Enter(enterCtx); err == nil {
		t.Fatal("auth entry was accepted after real control-owner loss")
	}
	entered, reason := entry.stageNow()
	if entered || reason == "" {
		t.Fatalf("owner-loss refusal did not permanently invalidate the checkpoint: entered=%t reason=%q", entered, reason)
	}
	t.Logf("owner-loss entry refusal stage: %s", reason)
	// The underlying opaque token must also refuse and permanently invalidate.
	if err := outcome.handoff.ConsumeForAuth(enterCtx); err == nil {
		t.Fatal("owner-loss token accepted auth consumption")
	}
	if valid, invalidReason := outcome.handoff.Valid(); valid || invalidReason == "" {
		t.Fatalf("owner-loss token invalidation is not shared: valid=%t reason=%q", valid, invalidReason)
	}
	if err := outcome.handoff.Recheck(enterCtx); err == nil {
		t.Fatal("owner-loss token rechecked")
	}
	copied := *entry
	if err := copied.Enter(enterCtx); err == nil {
		t.Fatal("owner-loss checkpoint copy entered the auth stage")
	}
	if err := copied.Recheck(enterCtx); err == nil {
		t.Fatal("owner-loss checkpoint copy rechecked")
	}

	// No P0 rotation, DDL, extra target write or acceptance.
	var verifierAfter string
	if err := f.fx.admin.QueryRow(ctx, `SELECT coalesce(rolpassword,'') FROM pg_authid WHERE rolname=$1`, f.writerRole).Scan(&verifierAfter); err != nil {
		t.Fatalf("read W verifier after owner loss entry: %v", err)
	}
	if verifierBefore == "" || verifierAfter != verifierBefore {
		t.Fatal("W verifier changed during the owner-loss entry attempt (P0 rotation must not occur)")
	}
	targetConn, err := pgx.Connect(ctx, f.writerTargetDSN)
	if err != nil {
		t.Fatalf("connect W target after owner loss (sqlstate=%s, W role OID %d)", borrowedAuthSQLState(err), f.writerRoleOID)
	}
	var rows int
	if err := targetConn.QueryRow(ctx, `SELECT count(*) FROM public.`+pgx.Identifier{f.table}.Sanitize()).Scan(&rows); err != nil || rows != 3 {
		_ = targetConn.Close(ctx)
		t.Fatalf("target rows after owner-loss refusal=%d err=%v, want the original 3", rows, err)
	}
	_ = targetConn.Close(ctx)
	if atomic.LoadInt32(&f.probeCalls) != 1 || atomic.LoadInt32(&f.acceptanceCalls) != 0 {
		t.Fatalf("owner-loss probe/acceptance counts: probe=%d acceptance=%d", f.probeCalls, f.acceptanceCalls)
	}
	var disposition string
	if err := f.controlPool.QueryRow(ctx, `SELECT disposition FROM recovery_target_guard WHERE target_guard_key=$1`, f.guardKey).Scan(&disposition); err != nil {
		t.Fatalf("guard state after owner loss: %v", err)
	}
	if disposition == "clean" {
		t.Fatal("owner-loss attempt cleared the guard to clean")
	}
}

// TestBorrowedReceiptAuthEntryRetirementPublicationHeldRefusal proves the
// authentic-token held-retirement refusal and the RE02 P1 permanent
// fixture-bound loss against the ACTUAL fixture:
//
//   - a genuine completed run mints the opaque token while the gate's existing
//     deterministic publication barrier holds the clean-retirement verdict
//     AFTER the authoritative drain was verified: the entry must refuse the
//     authentic token, permanently invalidate its shared checkpoint, and never
//     consume the token;
//   - releasing the barrier publishes the genuine clean retirement, but the
//     loss is permanent at the original fixture/run-bound capability: the
//     original entry, its shallow copies and a reconstructed entry over the
//     same authentic token/session/run must all refuse Recheck AND Enter, the
//     authentic token stays unconsumed, and no probe, acceptance or guard
//     effect may occur. There is no fresh-entry rehabilitation.
func TestBorrowedReceiptAuthEntryRetirementPublicationHeldRefusal(t *testing.T) {
	f := newBorrowedAuthHandoffFixture(t)
	installBorrowedReceiptAuthEntryProbe(t, f)
	ctx := context.Background()

	// Install the gate's existing deterministic test-only publication barrier
	// BEFORE the run: the gate verifies the authoritative drain, then parks
	// before publishing the clean verdict.
	barrier := make(chan struct{})
	var barrierOnce sync.Once
	releaseBarrier := func() { barrierOnce.Do(func() { close(barrier) }) }
	t.Cleanup(releaseBarrier)
	f.gate.mu.Lock()
	f.gate.publishBarrier = barrier
	f.gate.mu.Unlock()

	f.gate.HoldRegistration()
	resultCh := make(chan borrowedReceiptHandoffOutcome, 1)
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go func() {
		result, handoff, err := f.run.RunForReceiptHandoff(runCtx)
		resultCh <- borrowedReceiptHandoffOutcome{result, handoff, err}
	}()
	admitCtx, cancelAdmit := context.WithTimeout(ctx, 90*time.Second)
	defer cancelAdmit()
	session, err := f.gate.AdmitBorrowed(admitCtx, f.run, f.prefix)
	if err != nil {
		cancelRun()
		t.Fatalf("actual borrowed admission: %v", err)
	}
	heldDeadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(heldDeadline) && !(session.BackendPID() > 0 && session.BufferedFrames() > 0) {
		time.Sleep(20 * time.Millisecond)
	}
	if !(session.BackendPID() > 0 && session.BufferedFrames() > 0) {
		cancelRun()
		t.Fatal("actual native child never reached the held first executable frame")
	}
	f.gate.ReleaseRegistration()
	releaseDeadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(releaseDeadline) && !session.WatcherReady() {
		time.Sleep(20 * time.Millisecond)
	}
	if !session.WatcherReady() {
		cancelRun()
		t.Fatal("actual watcher/admission checks never became ready")
	}
	var outcome borrowedReceiptHandoffOutcome
	select {
	case outcome = <-resultCh:
	case <-time.After(150 * time.Second):
		cancelRun()
		t.Fatal("actual receipt-handoff run did not return")
	}
	if outcome.err == nil || !outcome.handoff.Diagnostics().Present {
		t.Fatalf("held-retirement positive token missing: err=%v facts=%+v", outcome.err, outcome.handoff.Diagnostics())
	}

	// Wait until the gate verified the authoritative drain while still parked
	// before the clean-verdict publication.
	drainDeadline := time.Now().Add(60 * time.Second)
	drainVerified := false
	for time.Now().Before(drainDeadline) {
		if clean, _ := f.gate.CleanRetired(); clean {
			t.Fatal("gate published clean retirement although the publication barrier is held")
		}
		if latched, reason := f.gate.Latched(); latched {
			t.Fatalf("gate latched before the held publication: %s", reason)
		}
		if f.gate.DrainState().Verified {
			drainVerified = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !drainVerified {
		t.Fatal("gate never verified the authoritative drain before publication")
	}

	// The authentic completed-run token must be refused while retirement is
	// held: this is not an absent-token construction refusal.
	entry, err := newBorrowedReceiptAuthEntry(f, session, outcome.handoff)
	if err != nil {
		t.Fatalf("held-retirement entry construction: %v", err)
	}
	enterCtx, cancelEnter := context.WithTimeout(ctx, 15*time.Second)
	defer cancelEnter()
	if err := entry.Enter(enterCtx); err == nil {
		t.Fatal("auth entry accepted the authentic token although clean retirement was not published")
	}
	entered, reason := entry.stageNow()
	if entered || reason == "" {
		t.Fatalf("held-retirement refusal did not permanently invalidate the checkpoint: entered=%t reason=%q", entered, reason)
	}
	t.Logf("held-retirement entry refusal stage: %s", reason)
	if clean, _ := f.gate.CleanRetired(); clean {
		t.Fatal("gate clean retirement was published during the held refusal")
	}
	if valid, tokenReason := outcome.handoff.Valid(); !valid || tokenReason != "" {
		t.Fatalf("held-retirement refusal invalidated the authentic token: valid=%t reason=%q", valid, tokenReason)
	}
	if outcome.handoff.Diagnostics().Consumed {
		t.Fatal("held-retirement refusal consumed the authentic token")
	}

	// Release the barrier: the genuine clean retirement publishes, but the
	// held-retirement failure already latched the original fixture/run-bound
	// capability permanently.
	releaseBarrier()
	publishDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(publishDeadline) {
		if clean, _ := f.gate.CleanRetired(); clean {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if clean, report := f.gate.CleanRetired(); !clean {
		t.Fatalf("gate did not publish clean retirement after the barrier release: %s", report)
	}
	// RE02 P1: the genuine held-retirement failure is permanent at the
	// original fixture/run-bound capability. Releasing the barrier publishes
	// the genuine clean retirement, but the original entry, its shallow copies
	// and a reconstructed entry over the same authentic token/session/run must
	// all refuse Recheck AND Enter; the authentic token stays unconsumed and
	// no probe, acceptance or guard effect may occur.
	refusalCtx, cancelRefusal := context.WithTimeout(ctx, 30*time.Second)
	defer cancelRefusal()
	if err := entry.Recheck(refusalCtx); err == nil {
		t.Fatal("original entry rechecked after its genuine retirement failure")
	}
	if err := entry.Enter(refusalCtx); err == nil {
		t.Fatal("original entry entered after its genuine retirement failure")
	}
	copied := *entry
	if err := copied.Recheck(refusalCtx); err == nil {
		t.Fatal("entry copy rechecked after the genuine retirement failure")
	}
	if err := copied.Enter(refusalCtx); err == nil {
		t.Fatal("entry copy entered after the genuine retirement failure")
	}
	reconstructed, err := newBorrowedReceiptAuthEntry(f, session, outcome.handoff)
	if err != nil || reconstructed == nil {
		t.Fatalf("reconstructed entry over the same original capability: %v", err)
	}
	if reconstructed.state != entry.state {
		t.Fatal("reconstructed entry did not reuse the original fixture-bound checkpoint")
	}
	if err := reconstructed.Recheck(refusalCtx); err == nil {
		t.Fatal("reconstructed entry rechecked after the genuine retirement failure")
	}
	if err := reconstructed.Enter(refusalCtx); err == nil {
		t.Fatal("reconstructed entry entered after the genuine retirement failure")
	}
	if entered, reason := entry.stageNow(); entered || reason == "" {
		t.Fatalf("permanent loss did not invalidate the original checkpoint: entered=%t reason=%q", entered, reason)
	}
	if valid, tokenReason := outcome.handoff.Valid(); !valid || tokenReason != "" {
		t.Fatalf("permanent loss invalidated the authentic token: valid=%t reason=%q", valid, tokenReason)
	}
	if outcome.handoff.Diagnostics().Consumed {
		t.Fatal("permanent loss consumed the authentic token")
	}
	if atomic.LoadInt32(&f.probeCalls) != 1 || atomic.LoadInt32(&f.acceptanceCalls) != 0 {
		t.Fatalf("permanent-loss probe/acceptance counts: probe=%d acceptance=%d", f.probeCalls, f.acceptanceCalls)
	}
	var disposition string
	if err := f.controlPool.QueryRow(ctx, `SELECT disposition FROM recovery_target_guard WHERE target_guard_key=$1`, f.guardKey).Scan(&disposition); err != nil {
		t.Fatalf("guard state after permanent loss: %v", err)
	}
	if disposition == "clean" {
		t.Fatal("permanent loss cleared the guard to clean")
	}
}

// runBorrowedReceiptAuthEntryPositiveChain establishes one INDEPENDENT genuine
// positive fixture for the RE01 recheck-loss tests: the real held run, the
// released native restore with the deliberate Probe rejection and no
// acceptance, the authentic opaque handoff token, the actual gate clean
// retirement and the entry-private strict probe installed. It grants no
// authority; the caller's permanent loss latch is never shared with another
// fixture.
func runBorrowedReceiptAuthEntryPositiveChain(t *testing.T) (*borrowedAuthHandoffFixture, *originGateSession, borrowedReceiptHandoffOutcome) {
	t.Helper()
	return runBorrowedReceiptAuthEntryPositiveChainWith(t, newBorrowedAuthHandoffFixture)
}

// runBorrowedReceiptAuthEntryPositiveChainWith is the SAME genuine positive
// chain with an explicit fixture constructor, so bound callers can reuse the
// identical orchestration without copying it. The default constructor keeps
// every unbound caller unchanged.
func runBorrowedReceiptAuthEntryPositiveChainWith(t *testing.T, newFixture func(*testing.T) *borrowedAuthHandoffFixture) (*borrowedAuthHandoffFixture, *originGateSession, borrowedReceiptHandoffOutcome) {
	t.Helper()
	f := newFixture(t)
	installBorrowedReceiptAuthEntryProbe(t, f)
	ctx := context.Background()

	f.gate.HoldRegistration()
	t.Cleanup(f.gate.ReleaseRegistration)
	resultCh := make(chan borrowedReceiptHandoffOutcome, 1)
	runCtx, cancelRun := context.WithCancel(ctx)
	t.Cleanup(cancelRun)
	go func() {
		result, handoff, err := f.run.RunForReceiptHandoff(runCtx)
		resultCh <- borrowedReceiptHandoffOutcome{result, handoff, err}
	}()
	admitCtx, cancelAdmit := context.WithTimeout(ctx, 90*time.Second)
	t.Cleanup(cancelAdmit)
	session, err := f.gate.AdmitBorrowed(admitCtx, f.run, f.prefix)
	if err != nil {
		cancelRun()
		t.Fatalf("actual borrowed admission: %v", err)
	}
	heldDeadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(heldDeadline) && !(session.BackendPID() > 0 && session.BufferedFrames() > 0) {
		time.Sleep(20 * time.Millisecond)
	}
	if !(session.BackendPID() > 0 && session.BufferedFrames() > 0) {
		cancelRun()
		t.Fatal("actual native child never reached the held first executable frame")
	}
	f.gate.ReleaseRegistration()
	releaseDeadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(releaseDeadline) && !session.WatcherReady() {
		time.Sleep(20 * time.Millisecond)
	}
	if !session.WatcherReady() {
		cancelRun()
		t.Fatal("actual watcher/admission checks never became ready")
	}
	var outcome borrowedReceiptHandoffOutcome
	select {
	case outcome = <-resultCh:
	case <-time.After(150 * time.Second):
		cancelRun()
		t.Fatal("actual receipt-handoff run did not return")
	}
	if outcome.err == nil || !outcome.handoff.Diagnostics().Present {
		t.Fatalf("positive chain produced no authentic token: err=%v facts=%+v", outcome.err, outcome.handoff.Diagnostics())
	}
	if atomic.LoadInt32(&f.probeCalls) != 1 || atomic.LoadInt32(&f.acceptanceCalls) != 0 {
		t.Fatalf("positive chain probe/acceptance counts: probe=%d acceptance=%d", f.probeCalls, f.acceptanceCalls)
	}
	clean, report := waitBorrowedGateRetirement(f, 30*time.Second)
	if !clean {
		t.Fatalf("actual gate did not publish clean retirement: %s", report)
	}
	return f, session, outcome
}

// TestBorrowedReceiptAuthEntryRecheckOwnCancelLoss proves the RE01 own-context
// cancellation at the recheck publication seam against an INDEPENDENT genuine
// positive fixture (never the permanently lost held-retirement fixture): the
// decision is taken under the shared mutex, so the ended caller context is
// recorded as permanent shared loss instead of a nil success, and the loss is
// shared with copies.
func TestBorrowedReceiptAuthEntryRecheckOwnCancelLoss(t *testing.T) {
	f, session, outcome := runBorrowedReceiptAuthEntryPositiveChainWith(t, newBorrowedAuthHandoffFixture)
	ctx := context.Background()
	entry, err := newBorrowedReceiptAuthEntry(f, session, outcome.handoff)
	if err != nil {
		t.Fatalf("own-cancel checkpoint construction: %v", err)
	}
	seam := installBorrowedReceiptAuthEntrySeam(t, entry, borrowedReceiptAuthEntryStageRecheckPublish)
	seamCtx, cancelSeam := context.WithCancel(ctx)
	seamCh := make(chan error, 1)
	go func() { seamCh <- entry.Recheck(seamCtx) }()
	select {
	case <-seam.entered:
	case <-time.After(60 * time.Second):
		cancelSeam()
		t.Fatal("recheck never reached the pre-publication seam")
	}
	cancelSeam()
	seam.releaseSeam()
	select {
	case seamErr := <-seamCh:
		if seamErr == nil {
			t.Fatal("recheck published success after its own context was canceled at the publication seam")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("canceled seam recheck did not return")
	}
	seamEntered, seamReason := entry.stageNow()
	if seamEntered || seamReason == "" {
		t.Fatalf("own-cancel refusal did not permanently invalidate the checkpoint: entered=%t reason=%q", seamEntered, seamReason)
	}
	seamCopy := *entry
	seamCopyEntered, seamCopyReason := seamCopy.stageNow()
	if seamCopyEntered || seamCopyReason == "" {
		t.Fatal("own-cancel permanent loss is not shared with copies")
	}
	t.Logf("recheck own-cancel refusal stage: %s", seamReason)
}

// TestBorrowedReceiptAuthEntryRecheckCopyInvalidationLoss proves the RE01
// copy-completed permanent invalidation while the winner is held at the
// publication seam against an INDEPENDENT genuine positive fixture (never the
// permanently lost held-retirement fixture): the held recheck must refuse
// instead of publishing a nil success over the shared loss, and the loss is
// shared with the original entry.
func TestBorrowedReceiptAuthEntryRecheckCopyInvalidationLoss(t *testing.T) {
	f, session, outcome := runBorrowedReceiptAuthEntryPositiveChainWith(t, newBorrowedAuthHandoffFixture)
	ctx := context.Background()
	entry, err := newBorrowedReceiptAuthEntry(f, session, outcome.handoff)
	if err != nil {
		t.Fatalf("copy-invalidation checkpoint construction: %v", err)
	}
	copySeam := installBorrowedReceiptAuthEntrySeam(t, entry, borrowedReceiptAuthEntryStageRecheckPublish)
	copyCh := make(chan error, 1)
	copyWinnerCtx, cancelCopyWinner := context.WithTimeout(ctx, 60*time.Second)
	t.Cleanup(cancelCopyWinner)
	go func() { copyCh <- entry.Recheck(copyWinnerCtx) }()
	select {
	case <-copySeam.entered:
	case <-time.After(60 * time.Second):
		t.Fatal("winner recheck never reached the pre-publication seam")
	}
	copied := *entry
	canceledCtx, cancelCanceled := context.WithCancel(ctx)
	cancelCanceled()
	if err := copied.Recheck(canceledCtx); err == nil {
		t.Fatal("copy recheck with an ended context succeeded")
	}
	if entered, copyReason := copied.stageNow(); entered || copyReason == "" {
		t.Fatalf("copy-completed invalidation did not permanently invalidate the shared checkpoint: entered=%t reason=%q", entered, copyReason)
	}
	copySeam.releaseSeam()
	select {
	case heldErr := <-copyCh:
		if heldErr == nil {
			t.Fatal("held recheck published success although a copy completed a permanent invalidation")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("held recheck did not return after the copy invalidation")
	}
	winnerEntered, winnerReason := entry.stageNow()
	if winnerEntered || winnerReason == "" {
		t.Fatalf("held recheck did not retain the shared permanent invalidation: entered=%t reason=%q", winnerEntered, winnerReason)
	}
	t.Logf("copy-invalidation refusal stage: %s", winnerReason)
}

// TestBorrowedReceiptAuthEntryStateClassificationUnit is a UNIT-ONLY proof of
// the RE02 classification order and the one-time shared slot. It constructs
// bare test-only states (no fixture, no I/O, no authority): a nil-context
// loser while an entry is reserved or already consumed refuses without
// invalidating the live state, a fresh genuine attempt latches the permanent
// context fault, and the slot installs at most once and never lets a
// different session reuse a closed slot. It grants no auth authority.
func TestBorrowedReceiptAuthEntryStateClassificationUnit(t *testing.T) {
	// A fresh genuine attempt with a nil context permanently latches.
	fresh := &borrowedReceiptAuthEntry{state: &borrowedReceiptAuthEntryState{}}
	if err := fresh.Enter(nil); err == nil {
		t.Fatal("fresh entry with a nil context was accepted")
	}
	if entered, reason := fresh.stageNow(); entered || reason == "" {
		t.Fatalf("fresh nil-context attempt did not latch permanent loss: entered=%t reason=%q", entered, reason)
	}

	// A copy losing to a live reservation refuses without invalidating the
	// winner even with a nil context.
	reserved := &borrowedReceiptAuthEntry{state: &borrowedReceiptAuthEntryState{entering: true}}
	if err := reserved.Enter(nil); err == nil {
		t.Fatal("nil-context replay entered a reserved checkpoint")
	}
	if entered, reason := reserved.stageNow(); entered || reason != "" {
		t.Fatalf("nil-context replay poisoned the reserved checkpoint: entered=%t reason=%q", entered, reason)
	}
	if !reserved.state.entering {
		t.Fatal("nil-context replay cleared the live reservation")
	}

	// A copy losing to a consumed entry refuses without invalidating it.
	consumed := &borrowedReceiptAuthEntry{state: &borrowedReceiptAuthEntryState{entered: true}}
	if err := consumed.Enter(nil); err == nil {
		t.Fatal("nil-context replay entered a consumed checkpoint")
	}
	if entered, reason := consumed.stageNow(); !entered || reason != "" {
		t.Fatalf("nil-context replay poisoned the consumed checkpoint: entered=%t reason=%q", entered, reason)
	}

	// An invalidated checkpoint reports the permanent invalidation.
	dead := &borrowedReceiptAuthEntry{state: &borrowedReceiptAuthEntryState{invalidated: true, invalidReason: "unit"}}
	if err := dead.Enter(nil); err == nil {
		t.Fatal("nil-context entry accepted an invalidated checkpoint")
	}
	if entered, reason := dead.stageNow(); entered || reason != "unit" {
		t.Fatalf("invalidated checkpoint stage: entered=%t reason=%q", entered, reason)
	}

	// Slot semantics: a nil slot refuses, the first validated state installs
	// at most once, the same original run/session/handoff reuses it, and a
	// different session can never borrow the closed slot.
	fixture := &borrowedAuthHandoffFixture{}
	session := &originGateSession{}
	handoff := recovery.OpaqueDrillReceiptHandoff{}
	var absent *borrowedReceiptAuthEntrySlot
	if entry, err := absent.bindOrReuse(&borrowedReceiptAuthEntryState{}, fixture, session, handoff); err == nil || entry != nil {
		t.Fatal("a nil fixture slot constructed an entry")
	}
	slot := &borrowedReceiptAuthEntrySlot{}
	state := &borrowedReceiptAuthEntryState{session: session, handoff: handoff}
	first, err := slot.bindOrReuse(state, fixture, session, handoff)
	if err != nil || first == nil || first.state != state {
		t.Fatalf("first slot binding: entry=%v err=%v", first, err)
	}
	reused, err := slot.bindOrReuse(&borrowedReceiptAuthEntryState{}, fixture, session, handoff)
	if err != nil || reused == nil || reused.state != state {
		t.Fatalf("same-capability reuse did not return the bound state: err=%v", err)
	}
	if _, err := slot.bindOrReuse(state, fixture, &originGateSession{}, handoff); err == nil {
		t.Fatal("a different session borrowed the closed slot")
	}
	if slot.state != state {
		t.Fatal("a refused binding replaced the closed slot state")
	}
}

// TestBorrowedReceiptAuthEntryStrictProbeUnitStatNegatives is a UNIT-ONLY
// classification test for the entry-private strict probe stat parser and its
// explicit errno reporting. It runs the compiled probe against a synthetic
// proc-root and grants no auth authority: it is not part of the real-PG
// retirement proof.
func TestBorrowedReceiptAuthEntryStrictProbeUnitStatNegatives(t *testing.T) {
	built := buildBorrowedReceiptAuthEntryProbe(t)
	// The EACCES case executes the probe as an unprivileged user when the test
	// runs as root; the compiled probe therefore needs a world-traversable
	// private directory (the test temp tree itself is 0700).
	execDir, err := os.MkdirTemp("", "borrowed-receipt-auth-entry-probe-unit")
	if err != nil {
		t.Fatalf("create world-traversable probe dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(execDir) })
	if err := os.Chmod(execDir, 0o755); err != nil {
		t.Fatalf("chmod world-traversable probe dir: %v", err)
	}
	helper := filepath.Join(execDir, "borrowed-receipt-auth-entry-probe")
	probeBytes, err := os.ReadFile(built)
	if err != nil {
		t.Fatalf("read compiled strict probe: %v", err)
	}
	if err := os.WriteFile(helper, probeBytes, 0o755); err != nil {
		t.Fatalf("install world-executable strict probe: %v", err)
	}
	root := t.TempDir()
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatalf("chmod synthetic proc root: %v", err)
	}
	const pid = 4242
	statLine := func(pid int, comm, state string, start int) string {
		fields := []string{state, "1"}
		for len(fields) < 20 {
			fields = append(fields, "0")
		}
		fields[19] = strconv.Itoa(start)
		return fmt.Sprintf("%d (%s) %s", pid, comm, strings.Join(fields, " "))
	}
	writeStat := func(content string) string {
		t.Helper()
		dir := filepath.Join(root, strconv.Itoa(pid))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("create synthetic pid dir: %v", err)
		}
		path := filepath.Join(dir, "stat")
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write synthetic stat: %v", err)
		}
		return path
	}
	expect := func(want string, args ...string) {
		t.Helper()
		if out := runBorrowedReceiptAuthEntryProbeUnit(t, helper, false, args...); out != want {
			t.Fatalf("strict probe %v classified %q, want %q", args, out, want)
		}
	}

	statPath := writeStat(statLine(pid, "postgres", "S", 987654))
	expect("STAGE ALIVE 987654", "pidstat", "4242", "987654", root)
	writeStat(statLine(pid, "postgres", "Z", 987654))
	expect("STAGE ZOMBIE 987654", "pidstat", "4242", "987654", root)
	writeStat(statLine(pid, "postgres", "S", 111111))
	expect("STAGE REUSED 111111", "pidstat", "4242", "987654", root)
	if err := os.Remove(statPath); err != nil {
		t.Fatalf("remove synthetic stat: %v", err)
	}
	expect("STAGE GONE", "pidstat", "4242", "987654", root)

	// Wrong leading PID, garbage, unrecognized state and truncation are all
	// malformed UNKNOWN refusals, never disappearance or aliveness.
	writeStat(statLine(9999, "postgres", "S", 987654))
	expect("STAGE MALFORMED reason=stat-malformed", "pidstat", "4242", "987654", root)
	writeStat("this is not a process stat document")
	expect("STAGE MALFORMED reason=stat-malformed", "pidstat", "4242", "987654", root)
	writeStat(statLine(pid, "postgres", "Q", 987654))
	expect("STAGE MALFORMED reason=stat-malformed", "pidstat", "4242", "987654", root)
	writeStat("4242 (postgres) S 1 0")
	expect("STAGE MALFORMED reason=stat-malformed", "pidstat", "4242", "987654", root)

	// Explicit errno reporting: a directory in place of the stat document is
	// EISDIR, never GONE.
	if err := os.Remove(statPath); err != nil {
		t.Fatalf("remove synthetic stat: %v", err)
	}
	if err := os.Mkdir(statPath, 0o755); err != nil {
		t.Fatalf("replace synthetic stat with a directory: %v", err)
	}
	expect("STAGE UNKNOWN errno=EISDIR reason=stat-read-error", "pidstat", "4242", "987654", root)

	// EACCES: the stat document is unreadable. When the test runs as root the
	// probe is executed as an unprivileged user so the permission denial is a
	// real kernel EACCES, not a synthesized report.
	if err := os.RemoveAll(statPath); err != nil {
		t.Fatalf("remove synthetic stat directory: %v", err)
	}
	writeStat(statLine(pid, "postgres", "S", 987654))
	if err := os.Chmod(statPath, 0); err != nil {
		t.Fatalf("chmod synthetic stat unreadable: %v", err)
	}
	if out := runBorrowedReceiptAuthEntryProbeUnit(t, helper, true, "pidstat", "4242", "987654", root); out != "STAGE UNKNOWN errno=EACCES reason=stat-read-error" {
		t.Fatalf("strict probe EACCES classification %q, want explicit EACCES UNKNOWN", out)
	}
}

func runBorrowedReceiptAuthEntryProbeUnit(t *testing.T, helper string, asNobody bool, args ...string) string {
	t.Helper()
	cmd := exec.Command(helper, args...)
	if asNobody && os.Geteuid() == 0 {
		cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534}}
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("strict probe unit invocation %v failed: %v: %s", args, err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out))
}
