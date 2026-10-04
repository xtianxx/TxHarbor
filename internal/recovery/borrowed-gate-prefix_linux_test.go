//go:build linux && drill

// borrowed-gate-prefix_linux_test.go is the private borrowed-writer adapter for
// the protected origin gate. It does NOT replace or weaken the normal observed
// admission path: it adds a separate borrowed mode that admits the concrete
// coordinator run's supervised child through the gate's EXACT endpoint
// capability and requires the fresh identity prefix (control owner + immutable
// original binding) plus the native backend catalog/incarnation evidence
// before any executable frame is released. Everything here is drill-only,
// non-authorizing and grant-free.
package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xtianxx/txharbor/internal/recovery"
)

// originGateBorrowedOrigin is the gate-private borrowed registration record:
// the concrete coordinator run and the fresh identity prefix captured against
// the same actual fixture. It carries no caller scalar, JSON projection or
// validity callback.
type originGateBorrowedOrigin struct {
	run    *recovery.DrillBorrowedWriterRun
	prefix *borrowedIdentityPrefix
	// stage is the optional test-only notification seam inside the borrowed
	// check (never a callback that declares identity or success). It is nil in
	// real runs.
	stage func(stage string)
}

// borrowedNativeControlDistinct classifies the registered native backend
// against the actual control backend: identity is the PID + captured OS start
// of the exact same instance, never a globally compared tick value. Two
// different PIDs with equal Linux start ticks are legitimately distinct
// processes; only the same PID (or an empty native start) is a conflation.
func borrowedNativeControlDistinct(nativePID int, nativeOSStart string, controlPID int, controlOSStart string) error {
	if nativePID <= 0 || controlPID <= 0 {
		return errors.New("native/control PID identity is missing")
	}
	if nativePID == controlPID {
		return errors.New("native backend PID equals the control backend PID (same-instance conflation)")
	}
	if nativeOSStart == "" {
		return errors.New("native backend OS start identity is missing")
	}
	if controlOSStart == "" {
		return errors.New("control backend OS start identity is missing")
	}
	// Equal tick values across different PIDs are explicitly allowed: Linux
	// start ticks are per-boot and repeat across processes.
	return nil
}

// UseBorrowedOrigin registers one concrete run and its fresh identity prefix
// for borrowed admission and installs the pre-release check. The gate mode is
// frozen under g.mu before admission: ordinary arm/admission freezes ordinary
// mode, and a borrowed install is refused once any ordinary arm/admission has
// happened, while admitting/active or an already-installed borrowed origin.
func (g *originGate) UseBorrowedOrigin(run *recovery.DrillBorrowedWriterRun, prefix *borrowedIdentityPrefix) error {
	if run == nil || prefix == nil {
		return errors.New("borrowed origin registration requires a concrete run and a fresh identity prefix")
	}
	if prefix.run != run {
		return errors.New("borrowed origin registration prefix does not belong to the concrete run")
	}
	if invalid, reason := prefix.Invalid(); invalid {
		return fmt.Errorf("borrowed origin registration refused: prefix is permanently invalid (%s)", reason)
	}
	facts := prefix.anchor.Diagnostics()
	if !facts.Present || facts.Invalidated {
		return errors.New("borrowed origin registration requires a live anchor capture")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.mode != gateModeFree {
		return errors.New("borrowed origin registration refused: gate mode is already frozen (ordinary/borrowed)")
	}
	if g.admitting || g.active != nil || g.armedRestore != nil || g.borrowedOrigin != nil {
		return errors.New("borrowed origin registration refused: an admission/arm is already selected on this gate")
	}
	g.mode = gateModeBorrowed
	g.borrowedOrigin = &originGateBorrowedOrigin{run: run, prefix: prefix}
	g.borrowedCheck = g.borrowedRegistrationCheck
	return nil
}

// AdmitBorrowed admits one connection whose origin is the concrete coordinator
// run's supervised child. It claims the run's observation with the gate's exact
// endpoint capability, requires the immutable bound operation (original target
// key/role/operation) to match the run binding and the fresh prefix capture,
// and then reuses the normal admission primitives (live PID/start identity,
// accepted peer tuple ownership, server startup identity binding).
func (g *originGate) AdmitBorrowed(ctx context.Context, run *recovery.DrillBorrowedWriterRun, prefix *borrowedIdentityPrefix) (*originGateSession, error) {
	if !g.endpoint.Valid() {
		return nil, errors.New("borrowed admission requires the gate's factory endpoint capability")
	}
	if run == nil || prefix == nil {
		return nil, errors.New("borrowed admission requires the concrete run and its fresh identity prefix")
	}
	g.mu.Lock()
	borrowed := g.borrowedOrigin
	g.mu.Unlock()
	if borrowed == nil || borrowed.run != run || borrowed.prefix != prefix {
		return nil, errors.New("borrowed admission requires the borrowed origin registered on this gate")
	}
	if invalid, reason := prefix.Invalid(); invalid {
		g.latch("borrowed admission prefix is permanently invalid: " + reason)
		return nil, fmt.Errorf("borrowed admission prefix is permanently invalid: %s", reason)
	}
	facts := prefix.anchor.Diagnostics()
	if !facts.Present || facts.Invalidated {
		return nil, errors.New("borrowed admission requires a live anchor capture")
	}
	handle := run.Observation()
	if !handle.Valid() {
		return nil, errors.New("borrowed admission requires the run's live observation")
	}
	g.mu.Lock()
	if g.latched {
		reason := g.latchReason
		g.mu.Unlock()
		return nil, fmt.Errorf("origin gate is latched forever: %s", reason)
	}
	if g.active != nil {
		g.mu.Unlock()
		return nil, errors.New("origin gate already has an active admitted session")
	}
	if g.admitting {
		g.mu.Unlock()
		return nil, errors.New("origin gate admission is already in progress")
	}
	g.admitting = true
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.admitting = false
		g.mu.Unlock()
	}()

	if err := handle.AwaitStarted(ctx); err != nil {
		g.latch("borrowed owned origin did not start: " + err.Error())
		return nil, fmt.Errorf("borrowed owned origin did not start: %w", err)
	}
	claimed, err := handle.ClaimForEndpoint(g.endpoint)
	if err != nil {
		return nil, fmt.Errorf("borrowed endpoint-bound origin claim refused: %w", err)
	}
	if !claimed.MatchesEndpoint(g.endpoint) {
		g.latch("borrowed endpoint claim swap: claimed origin does not match the gate's exact endpoint capability")
		return nil, errors.New("borrowed claimed origin does not match the gate's exact endpoint capability")
	}
	bound, err := claimed.BoundOperation()
	if err != nil {
		return nil, err
	}
	binding := run.Binding()
	if bound.TargetKey != binding.OriginalTargetKey() || bound.TargetKey != facts.OriginalTargetKey ||
		bound.RoleFingerprint != binding.OriginalRoleFingerprint() || bound.RoleFingerprint != facts.OriginalRoleFingerprint ||
		bound.OperationID != binding.OriginalOperationID() || bound.OperationID != facts.OriginalOperationID ||
		facts.OriginalInstanceID != binding.OriginalInstanceID() ||
		bound.Executable != "pg_restore" {
		g.latch("borrowed admission operation identity does not match the immutable run binding and prefix capture")
		return nil, errors.New("borrowed admission operation identity does not match the immutable run binding and prefix capture")
	}
	if bound.Database != prefix.capturedTargetDB || bound.Role != prefix.capturedTargetRole {
		g.latch("borrowed admission startup identity does not match the declared fixture target")
		return nil, errors.New("borrowed admission startup identity does not match the declared fixture target")
	}
	identity := claimed.StartedIdentity()
	currentStartID, err := hostProcessStartID(identity.PID)
	if err != nil {
		g.latch(fmt.Sprintf("borrowed owned origin process %d identity unavailable before admission: %v", identity.PID, err))
		return nil, fmt.Errorf("borrowed owned origin process %d identity unavailable before admission: %w", identity.PID, err)
	}
	if currentStartID != identity.StartID {
		g.latch(fmt.Sprintf("borrowed owned origin process %d start identity changed before admission: observed=%d current=%d", identity.PID, identity.StartID, currentStartID))
		return nil, errors.New("borrowed owned origin start identity changed before admission")
	}
	conn, peer, err := g.acceptAdmitted(ctx)
	if err != nil {
		return nil, err
	}
	inode, err := g.verifyObservedOrigin(identity, peer)
	if err != nil {
		_ = conn.Close()
		g.latch("accepted borrowed frontend origin rejected: " + err.Error())
		return nil, fmt.Errorf("accepted borrowed frontend origin rejected: %w", err)
	}
	return g.startSession(ctx, conn, peer, inode, 0, "", nil, &claimed, identity.PID, identity.StartID, bound.Role, bound.Database)
}

// borrowedRegistrationCheck is the borrowed pre-release check: the newly
// registered native backend must match the declared fixture target/role
// catalog OIDs and the protected cluster incarnation of the prefix capture,
// the fresh prefix must pass a full window recheck, and the genuine control
// owner health must hold. It runs AFTER registration and the first successful
// watcher pass and BEFORE any executable frame is released; any failure latches
// the gate. The control-backend identity alone is not the native backend proof.
func (g *originGate) borrowedRegistrationCheck(ctx context.Context, registration *originGateRegistration) error {
	g.mu.Lock()
	borrowed := g.borrowedOrigin
	g.mu.Unlock()
	if borrowed == nil {
		return errors.New("no borrowed origin is registered on this gate")
	}
	prefix := borrowed.prefix
	if invalid, reason := prefix.Invalid(); invalid {
		return fmt.Errorf("borrowed prefix is permanently invalid: %s", reason)
	}
	facts := prefix.capturedFacts
	if !facts.Present {
		return errors.New("borrowed prefix has no retained capture")
	}

	// Native backend catalog identity through the protected observer: the real
	// server backend behind BackendKeyData must name the declared target
	// database OID and role OID of this fixture.
	g.observerMu.Lock()
	var (
		backendStart    time.Time
		backendType     string
		databaseOID     uint32
		roleOID         uint32
		postmasterStart time.Time
		systemID        string
	)
	catalogCtx, catalogCancel := context.WithTimeout(ctx, 3*time.Second)
	err := g.observer.QueryRow(catalogCtx,
		`SELECT backend_start, backend_type, datid::oid, usesysid::oid FROM pg_stat_activity WHERE pid=$1`,
		registration.PID).Scan(&backendStart, &backendType, &databaseOID, &roleOID)
	catalogCancel()
	if err != nil {
		g.observerMu.Unlock()
		return fmt.Errorf("native backend catalog row is unreadable: %w", err)
	}
	incarnationCtx, incarnationCancel := context.WithTimeout(ctx, 3*time.Second)
	err = g.observer.QueryRow(incarnationCtx, `SELECT pg_postmaster_start_time(), system_identifier::text FROM pg_control_system()`).Scan(&postmasterStart, &systemID)
	incarnationCancel()
	g.observerMu.Unlock()
	if err != nil {
		return fmt.Errorf("protected cluster incarnation is unreadable: %w", err)
	}
	if backendType != "client backend" || !backendStart.Equal(registration.BackendStart) {
		return errors.New("native backend catalog identity changed during registration")
	}
	if databaseOID != prefix.capturedTargetDBOID || roleOID != prefix.capturedRoleOID {
		return errors.New("native backend database/role catalog OIDs do not match the declared fixture target")
	}
	if !postmasterStart.Equal(facts.PostmasterStart) || systemID == "" || systemID != facts.SystemIdentifier {
		return errors.New("protected cluster incarnation does not match the prefix capture")
	}
	if prefix.backendOSStart == "" || prefix.serverSocketInode == "" {
		return errors.New("borrowed prefix has no retained OS/socket token")
	}
	if err := borrowedNativeControlDistinct(registration.PID, registration.OSStart, facts.BackendPID, prefix.backendOSStart); err != nil {
		return err
	}
	// Test-only negative notification seam: it may not declare identity and the
	// normal borrowed check still executes the real prefix recheck and genuine
	// control health below.
	if borrowed.stage != nil {
		borrowed.stage("after-catalog")
	}

	// Fresh full-window prefix recheck (independent SQL + strict OS census +
	// anchor) and genuine control-owner health, both bounded, before release.
	recheckCtx, recheckCancel := context.WithTimeout(ctx, 6*time.Second)
	err = prefix.inspect(recheckCtx)
	recheckCancel()
	if err != nil {
		return fmt.Errorf("fresh prefix recheck refused before frame release: %w", err)
	}
	healthCtx, healthCancel := context.WithTimeout(ctx, time.Second)
	err = borrowed.run.Health(healthCtx)
	healthCancel()
	if err != nil {
		return fmt.Errorf("genuine control-owner health lost before frame release: %w", err)
	}
	return nil
}
