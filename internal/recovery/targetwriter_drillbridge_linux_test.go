//go:build linux && drill

package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// DrillTargetProcessReceipt is an opaque, one-use handle to facts retained by
// the real process supervisor. Its private pointer cannot be forged by the
// external recovery_test package or represented as ordinary JSON.
type DrillTargetProcessReceipt struct {
	receipt *targetProcessReceipt
}

// DrillTargetProcessFacts is a one-time diagnostic projection, not an
// acceptance or cleanliness result.
type DrillTargetProcessFacts struct {
	Cmd             *exec.Cmd
	Process         *os.Process
	PID             int
	StartIdentity   uint64
	Started         bool
	Terminal        bool
	Command         PGCommandResult
	TargetKey       TargetKey
	RoleFingerprint string
	OperationID     string
}

func (r DrillTargetProcessReceipt) ConsumeFacts() (DrillTargetProcessFacts, error) {
	if r.receipt == nil {
		return DrillTargetProcessFacts{}, errors.New("process receipt is absent")
	}
	r.receipt.mu.Lock()
	defer r.receipt.mu.Unlock()
	if r.receipt.consumed {
		return DrillTargetProcessFacts{}, errors.New("process receipt was already consumed")
	}
	r.receipt.consumed = true
	s := r.receipt.snapshot
	return DrillTargetProcessFacts{
		Cmd: s.cmd, Process: s.process, PID: s.pid, StartIdentity: s.startID,
		Started: s.started, Terminal: s.terminal, Command: s.result,
		TargetKey: r.receipt.targetKey, RoleFingerprint: r.receipt.roleFingerprint,
		OperationID: r.receipt.operationID,
	}, nil
}

func DrillRunTargetWriterBorrowingLock(ctx context.Context, opts TargetWriterOptions, lock *TargetLock) (TargetWriterResult, DrillTargetProcessReceipt, error) {
	observation := &targetProcessObservation{}
	opts.borrowedLock = lock
	opts.Runner.observation = observation
	result, err := runTargetWriter(ctx, opts)
	receipt := DrillTargetProcessReceipt{receipt: result.processReceipt}
	return result, receipt, err
}

func DrillExecuteRestoreBorrowingLock(ctx context.Context, opts RestoreOptions, lock *TargetLock) (RestoreResult, TargetWriterResult, DrillTargetProcessReceipt, error) {
	result, writerResult, err := executeRestoreWithBorrowedTargetLock(ctx, opts, lock)
	return result, writerResult, DrillTargetProcessReceipt{receipt: writerResult.processReceipt}, err
}

// ---------------------------------------------------------------------------
// Observed-origin handle (fix101 supervisor binding, drill-only).
//
// This is the minimal TEST-ONLY adapter that lets the external recovery_test
// gate primitive adopt a live origin backed only by the real production
// targetProcessObservation owned by the pinned launch+soleWait owner:
//
//   - no exported *exec.Cmd/*os.Process pointer is ever projected, so the
//     external package can neither Wait, cancel nor mutate the child;
//   - the zero value and any JSON round trip are inert: authority exists only
//     through the private observation pointer recorded by that owner;
//   - started identity, bounded runner disposition and actual wait terminal are
//     projected as three separate facts, mirroring CORE05 exactly;
//   - an origin cannot be claimed before the operation was bound once from
//     validated options and the pinned owner captured a real Linux start ID.
//
// The terminal consumed receipt above is unchanged diagnostics; it is not, and
// must not become, a live admission authority.
// ---------------------------------------------------------------------------

// DrillStartedIdentity is the scalar-only start projection.
type DrillStartedIdentity struct {
	Started  bool
	PID      int
	StartID  uint64
	StartErr error
}

// DrillRunnerDisposition is the bounded disposition the runner returned to its
// caller. It is never terminal evidence.
type DrillRunnerDisposition struct {
	Returned bool
	Result   PGCommandResult
}

// DrillWaitTerminal is the actual sole cmd.Wait completion for the exact child.
type DrillWaitTerminal struct {
	ChildWaitCompleted bool
	WaitErr            error
	WaitExitCode       int
	Terminal           bool
}

// DrillBoundOperation is the immutable operation identity bound exactly once
// at invocation from actually validated options.
type DrillBoundOperation struct {
	TargetKey       TargetKey
	RoleFingerprint string
	Host            string
	Port            uint16
	Database        string
	Role            string
	OperationID     string
	Executable      string
}

type drillObservedState struct {
	mu         sync.Mutex
	bound      *DrillBoundOperation
	claimed    bool
	armed      *drillArmedRestore
	runInvoked bool
}

// drillArmedRestore is the private, one-time arm snapshot: the actual factory
// listener object (pointer identity, not a scalar address), its captured
// kernel socket inode, the sealed private tool identities and the canonical
// single-endpoint DSN constructed from the validated input.
type drillArmedRestore struct {
	endpointListener net.Listener
	endpointAddr     string
	endpointInode    uint64
	tools            drillSealedPGTools
	dsn              string
}

// DrillTargetProcessObservation is the opaque live handle.
type DrillTargetProcessObservation struct {
	observation *targetProcessObservation
	state       *drillObservedState
}

// DrillNewTargetProcessObservation creates an inert placeholder. It issues no
// origin: a claim is refused until the real pinned owner records a started
// child with a nonzero Linux start ID and the operation was bound once.
func DrillNewTargetProcessObservation() DrillTargetProcessObservation {
	return DrillTargetProcessObservation{observation: &targetProcessObservation{}, state: &drillObservedState{}}
}

// Valid reports whether this value is backed by a real observation.
func (o DrillTargetProcessObservation) Valid() bool {
	return o.observation != nil && o.state != nil
}

func (o DrillTargetProcessObservation) StartedIdentity() DrillStartedIdentity {
	if !o.Valid() {
		return DrillStartedIdentity{}
	}
	snapshot := o.observation.snapshot()
	return DrillStartedIdentity{Started: snapshot.started, PID: snapshot.pid, StartID: snapshot.startID, StartErr: snapshot.startErr}
}

func (o DrillTargetProcessObservation) RunnerDisposition() DrillRunnerDisposition {
	if !o.Valid() {
		return DrillRunnerDisposition{}
	}
	snapshot := o.observation.snapshot()
	return DrillRunnerDisposition{Returned: snapshot.runnerReturned, Result: snapshot.result}
}

func (o DrillTargetProcessObservation) WaitTerminal() DrillWaitTerminal {
	if !o.Valid() {
		return DrillWaitTerminal{}
	}
	snapshot := o.observation.snapshot()
	return DrillWaitTerminal{
		ChildWaitCompleted: snapshot.childWaitCompleted,
		WaitErr:            snapshot.waitErr,
		WaitExitCode:       snapshot.waitExitCode,
		Terminal:           snapshot.terminal,
	}
}

// BoundOperation returns the immutable operation identity once the run variant
// has bound it. The boolean is false for an inert handle.
func (o DrillTargetProcessObservation) BoundOperation() (DrillBoundOperation, bool) {
	if !o.Valid() {
		return DrillBoundOperation{}, false
	}
	o.state.mu.Lock()
	defer o.state.mu.Unlock()
	if o.state.bound == nil {
		return DrillBoundOperation{}, false
	}
	return *o.state.bound, true
}

// AwaitStarted blocks until the pinned owner captured the real Linux start
// identity. A captured startErr, a zero start ID or an invalid PID fails
// closed; a context end is returned as such and never defaulted into an
// identity.
func (o DrillTargetProcessObservation) AwaitStarted(ctx context.Context) error {
	if !o.Valid() {
		return errors.New("observation handle is not backed by a real supervised process")
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		identity := o.StartedIdentity()
		if identity.Started {
			if identity.StartErr != nil {
				return fmt.Errorf("owned child start identity was not captured: %w", identity.StartErr)
			}
			if identity.PID <= 0 || identity.StartID == 0 {
				return errors.New("owned child start identity is incomplete")
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("owned child start was not observed: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// AwaitBound waits until the invocation bound the immutable operation
// identity. It is the synchronization point between launching the supervised
// run and admitting its origin.
func (o DrillTargetProcessObservation) AwaitBound(ctx context.Context) error {
	if !o.Valid() {
		return errors.New("observation handle is not backed by a real supervised process")
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, bound := o.BoundOperation(); bound {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation identity was never bound: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// DrillClaimedOrigin is the one-use origin claim. It retains the private
// observation pointer itself, so a copied claim is the same claim and a second
// ClaimOrigin is denied.
type DrillClaimedOrigin struct {
	observation *targetProcessObservation
	state       *drillObservedState
	bound       DrillBoundOperation
}

func (c DrillClaimedOrigin) Valid() bool { return c.observation != nil && c.state != nil }

func (c DrillClaimedOrigin) StartedIdentity() DrillStartedIdentity {
	if !c.Valid() {
		return DrillStartedIdentity{}
	}
	handle := DrillTargetProcessObservation{observation: c.observation, state: c.state}
	return handle.StartedIdentity()
}

func (c DrillClaimedOrigin) RunnerDisposition() DrillRunnerDisposition {
	if !c.Valid() {
		return DrillRunnerDisposition{}
	}
	handle := DrillTargetProcessObservation{observation: c.observation, state: c.state}
	return handle.RunnerDisposition()
}

func (c DrillClaimedOrigin) WaitTerminal() DrillWaitTerminal {
	if !c.Valid() {
		return DrillWaitTerminal{}
	}
	handle := DrillTargetProcessObservation{observation: c.observation, state: c.state}
	return handle.WaitTerminal()
}

func (c DrillClaimedOrigin) BoundOperation() (DrillBoundOperation, error) {
	if !c.Valid() {
		return DrillBoundOperation{}, errors.New("claimed origin is absent")
	}
	return c.bound, nil
}

// ClaimOrigin consumes the handle exactly once. It can only succeed after the
// operation was bound from validated options and the pinned owner recorded a
// real started child with a nonzero Linux start ID.
func (o DrillTargetProcessObservation) ClaimOrigin() (DrillClaimedOrigin, error) {
	if !o.Valid() {
		return DrillClaimedOrigin{}, errors.New("observation handle is not backed by a real supervised process")
	}
	state := o.state
	state.mu.Lock()
	defer state.mu.Unlock()
	return o.claimOriginLocked(state)
}

// ClaimForEndpoint consumes the handle exactly once, but only when the caller
// presents the actual armed endpoint capability. The comparison is private
// pointer identity against the factory listener retained by the arm snapshot:
// an equal-looking address string or JSON round trip can never satisfy it.
func (o DrillTargetProcessObservation) ClaimForEndpoint(endpoint DrillOriginEndpoint) (DrillClaimedOrigin, error) {
	if !o.Valid() {
		return DrillClaimedOrigin{}, errors.New("observation handle is not backed by a real supervised process")
	}
	state := o.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.armed == nil {
		return DrillClaimedOrigin{}, errors.New("origin was not armed with a live endpoint capability")
	}
	if endpoint.listener == nil || endpoint.listener != state.armed.endpointListener {
		return DrillClaimedOrigin{}, errors.New("endpoint is not the armed origin capability")
	}
	return o.claimOriginLocked(state)
}

// MatchesEndpoint reports whether the claimed origin was armed with exactly
// this endpoint capability (private pointer identity).
func (c DrillClaimedOrigin) MatchesEndpoint(endpoint DrillOriginEndpoint) bool {
	if !c.Valid() {
		return false
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	return c.state.armed != nil && endpoint.listener != nil && endpoint.listener == c.state.armed.endpointListener
}

func (o DrillTargetProcessObservation) claimOriginLocked(state *drillObservedState) (DrillClaimedOrigin, error) {
	if state.claimed {
		return DrillClaimedOrigin{}, errors.New("origin was already claimed (one use, connection binding)")
	}
	if state.bound == nil {
		return DrillClaimedOrigin{}, errors.New("operation identity was not bound before claiming the origin")
	}
	identity := o.StartedIdentity()
	if !identity.Started {
		return DrillClaimedOrigin{}, errors.New("origin was not started")
	}
	if identity.StartErr != nil {
		return DrillClaimedOrigin{}, fmt.Errorf("origin start identity is not usable: %w", identity.StartErr)
	}
	if identity.PID <= 0 || identity.StartID == 0 {
		return DrillClaimedOrigin{}, errors.New("origin start identity is incomplete")
	}
	state.claimed = true
	return DrillClaimedOrigin{observation: o.observation, state: state, bound: *state.bound}, nil
}

// DrillObservedRunnerOptions is retained only so the pending external call
// sites still compile while proxy adoption is not yet terminal. The old
// command is permanently closed and this type carries no authority.
type DrillObservedRunnerOptions struct {
	TargetDSN    string
	ExpectedRole string
	OperationID  string
	Executable   string
}

// DrillRunObservedPGCommand is permanently fail-closed: it previously accepted
// caller DSN/args/executable inputs and could start a native child outside the
// armed endpoint+tools authority. It is kept only for compilation of pending
// call sites; use DrillArmObservedPGRestore + DrillRunObservedPGRestore.
func DrillRunObservedPGCommand(
	ctx context.Context,
	runner TargetProcessRunner,
	handle DrillTargetProcessObservation,
	opts DrillObservedRunnerOptions,
	args []string,
	stdin io.Reader,
	stdout, stderr io.Writer,
) (PGCommandResult, error) {
	_, _, _, _, _, _, _, _ = ctx, runner, handle, opts, args, stdin, stdout, stderr
	return PGCommandResult{Outcome: PGCommandNotStarted, ExitCode: -1},
		errors.New("DrillRunObservedPGCommand is permanently closed: arm an endpoint and sealed tools with DrillArmObservedPGRestore, then run DrillRunObservedPGRestore (proxy adoption pending)")
}

// ---------------------------------------------------------------------------
// OG01 Phase 1: armed endpoint + sealed native tool authority.
//
// The factory-created listener and the privately provisioned pinned-client
// ELFs are the only sources of endpoint/tool authority. Callers can never
// supply bytes, paths, hashes or trust flags: they can only obtain the opaque
// values from the factories below, and the arm/run APIs revalidate them.
//
// Scope limit: the binding here is transport endpoint + process/tool
// provenance only. It does not bind, claim or close the original durable
// target/control namespace; those gate concerns remain open for the proxy
// adoption phase.
// ---------------------------------------------------------------------------

// Carrier image identity is store-dependent. The pinned reference resolves to
// an OCI image index; a containerd-snapshotter store reports that index digest
// as the image identity, while the classic overlay2 graph driver reports the
// config digest. The linux/amd64 platform manifest digest is not any store's
// image identity, so it is deliberately not part of the accepted set.
// Evidence: docs/evidence/016-carrier-image-identity/.
const (
	drillCarrierImage                  = "postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280"
	drillCarrierIndexDigest            = "sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280"
	drillCarrierPlatformManifestDigest = "sha256:7341002d2b8c7c5bdd7542a671a95b36196c0b5b888daf454ae4fc33ba5346d7"
	drillCarrierConfigDigest           = "sha256:a6638641707cdf047e5d5c2781f437e2e809323cab22c70b280be8389fbb7878"
	drillCarrierRestore                = "/usr/lib/postgresql/18/bin/pg_restore"
	drillCarrierDump                   = "/usr/lib/postgresql/18/bin/pg_dump"
	drillCarrierLibpq                  = "/usr/lib/x86_64-linux-gnu/libpq.so.5.18"
	drillCarrierRestoreSHA             = "06115b93c3d1bf9d7c62563abb595792ea90acb9083233fd293eac1b34695840"
	drillCarrierDumpSHA                = "66115325f4e49f7f9c79a83cfc89d2c9a1698858a1ae9786ae7595cd7a9f2de9"
	drillCarrierLibpqSHA               = "9cce9bfae9405a71e0d642457d9d8748093cea9fdc0f785b5d2ed9575aec168d"
	drillToolsVersion                  = "(PostgreSQL) 18.6"
)

// DrillOriginEndpoint is an opaque origin endpoint capability: it retains the
// actual factory-created net.Listener and its captured kernel socket inode.
// There is no constructor from an address string and no JSON representation
// that carries authority.
type DrillOriginEndpoint struct {
	listener net.Listener
	addr     string
	inode    uint64
}

// DrillOpenOriginEndpoint creates the actual loopback TCP listener and
// captures its kernel socket identity.
func DrillOpenOriginEndpoint() (DrillOriginEndpoint, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return DrillOriginEndpoint{}, fmt.Errorf("open origin endpoint: %w", err)
	}
	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	if !ok || !tcpAddr.IP.IsLoopback() || tcpAddr.Port <= 0 {
		_ = listener.Close()
		return DrillOriginEndpoint{}, errors.New("origin endpoint is not a loopback TCP listener")
	}
	inode, err := drillListenerSocketInode(listener)
	if err != nil {
		_ = listener.Close()
		return DrillOriginEndpoint{}, err
	}
	return DrillOriginEndpoint{listener: listener, addr: listener.Addr().String(), inode: inode}, nil
}

func (e DrillOriginEndpoint) Valid() bool {
	return e.listener != nil && e.addr != "" && e.inode != 0
}

// Addr is the actual bound listener address (diagnostics only; it carries no
// authority).
func (e DrillOriginEndpoint) Addr() string { return e.addr }

// Listener exposes the actual factory listener for the owning test/adapter. A
// closed listener is detected by revalidation and refuses arm/run.
func (e DrillOriginEndpoint) Listener() net.Listener { return e.listener }

func (e DrillOriginEndpoint) revalidate() error {
	if !e.Valid() {
		return errors.New("origin endpoint capability is absent")
	}
	current := ""
	if e.listener.Addr() != nil {
		current = e.listener.Addr().String()
	}
	if current != e.addr {
		return errors.New("origin endpoint address changed")
	}
	inode, err := drillListenerSocketInode(e.listener)
	if err != nil {
		return fmt.Errorf("origin endpoint listener is no longer alive: %w", err)
	}
	if inode != e.inode {
		return errors.New("origin endpoint kernel socket identity changed")
	}
	return nil
}

func drillListenerSocketInode(listener net.Listener) (uint64, error) {
	conn, ok := listener.(syscall.Conn)
	if !ok {
		return 0, errors.New("origin endpoint listener does not expose a kernel connection")
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("origin endpoint listener is not alive: %w", err)
	}
	var (
		inode   uint64
		statErr error
	)
	if err := raw.Control(func(fd uintptr) {
		var stat syscall.Stat_t
		if err := syscall.Fstat(int(fd), &stat); err != nil {
			statErr = err
			return
		}
		inode = stat.Ino
	}); err != nil {
		return 0, fmt.Errorf("origin endpoint listener is not alive: %w", err)
	}
	if statErr != nil {
		return 0, fmt.Errorf("origin endpoint socket identity: %w", statErr)
	}
	if inode == 0 {
		return 0, errors.New("origin endpoint socket identity is zero")
	}
	return inode, nil
}

// drillPGFileIdentity is the private file identity captured at seal time.
type drillPGFileIdentity struct {
	path string
	dev  uint64
	ino  uint64
	size int64
	sha  [sha256.Size]byte
}

type drillSealedPGTools struct {
	dir     string
	restore drillPGFileIdentity
	dump    drillPGFileIdentity
}

// DrillNativePGTools is the opaque, privately provisioned and sealed pinned
// client tool set. Only DumpPath is exposed, as a diagnostic for archive
// creation; there is no restore-path authority derived from a basename.
type DrillNativePGTools struct {
	sealed *drillSealedPGTools
}

func (t DrillNativePGTools) Valid() bool { return t.sealed != nil }

// DumpPath returns the provisioned pg_dump path for archive creation. It is a
// diagnostic input, not restore authority.
func (t DrillNativePGTools) DumpPath() (string, bool) {
	if !t.Valid() {
		return "", false
	}
	return t.sealed.dump.path, true
}

func (t DrillNativePGTools) revalidate() error {
	if !t.Valid() {
		return errors.New("native PostgreSQL tools are not sealed")
	}
	if info, err := os.Stat(t.sealed.dir); err != nil || info.Mode().Perm() != 0o700 {
		return errors.New("native PostgreSQL tools directory is not private 0700")
	}
	if err := t.sealed.restore.revalidate(); err != nil {
		return fmt.Errorf("restore tool: %w", err)
	}
	if err := t.sealed.dump.revalidate(); err != nil {
		return fmt.Errorf("dump tool: %w", err)
	}
	return nil
}

func (id drillPGFileIdentity) revalidate() error {
	if id.path == "" {
		return errors.New("tool path is absent")
	}
	lstat, err := os.Lstat(id.path)
	if err != nil {
		return errors.New("tool is not present")
	}
	if lstat.Mode()&os.ModeSymlink != 0 || !lstat.Mode().IsRegular() {
		return errors.New("tool is not a regular non-symlink file")
	}
	if lstat.Mode().Perm() != 0o700 {
		return errors.New("tool mode is not 0700")
	}
	resolved, err := filepath.EvalSymlinks(id.path)
	if err != nil || resolved != id.path {
		return errors.New("tool path does not resolve to itself")
	}
	info, err := os.Stat(id.path)
	if err != nil {
		return errors.New("tool cannot be stat'ed")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint64(stat.Dev) != id.dev || stat.Ino != id.ino || info.Size() != id.size {
		return errors.New("tool file identity changed")
	}
	if !isDrillELF64LE(id.path) {
		return errors.New("tool is not a 64-bit little-endian ELF")
	}
	data, err := os.ReadFile(id.path)
	if err != nil {
		return errors.New("tool content cannot be read")
	}
	if sha256.Sum256(data) != id.sha {
		return errors.New("tool content digest changed")
	}
	return nil
}

// DrillProvisionNativePGTools provisions the genuine pinned PostgreSQL 18.6
// pg_restore/pg_dump ELFs from a pristine carrier container created from the
// fixed image digest (never an existing mutable server container), applies the
// narrow libpq DT_NEEDED transform inside this authority, and seals the
// privately computed expected result digest plus the file identity of each
// tool. It accepts no caller bytes, paths, hashes or trust flags.
func DrillProvisionNativePGTools(t *testing.T) (DrillNativePGTools, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		return DrillNativePGTools{}, errors.New("cannot restrict the tool directory to 0700")
	}
	if info, err := os.Stat(dir); err != nil || info.Mode().Perm() != 0o700 {
		return DrillNativePGTools{}, errors.New("tool directory is not private 0700")
	}
	binDir := filepath.Join(dir, "bin")
	libDir := filepath.Join(dir, "lib")
	for _, path := range []string{binDir, libDir} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return DrillNativePGTools{}, errors.New("cannot create the private tool directories")
		}
	}
	carrierID, removeCarrier, err := drillCreatePristineCarrier(ctx)
	if err != nil {
		return DrillNativePGTools{}, fmt.Errorf("pristine carrier unavailable: %w", err)
	}
	defer removeCarrier()
	libpqPath := filepath.Join(libDir, "libpq.so.5.18")
	if err := drillDockerCopy(ctx, carrierID, drillCarrierLibpq, libpqPath); err != nil {
		return DrillNativePGTools{}, errors.New("cannot copy the carrier libpq")
	}
	if err := drillVerifyFileDigest(libpqPath, drillCarrierLibpqSHA); err != nil {
		return DrillNativePGTools{}, fmt.Errorf("carrier libpq provenance: %w", err)
	}
	identities := make(map[string]drillPGFileIdentity, 2)
	for _, spec := range []struct {
		carrier string
		name    string
		digest  string
	}{
		{drillCarrierRestore, "pg_restore", drillCarrierRestoreSHA},
		{drillCarrierDump, "pg_dump", drillCarrierDumpSHA},
	} {
		pristinePath := filepath.Join(binDir, spec.name+".pristine")
		if err := drillDockerCopy(ctx, carrierID, spec.carrier, pristinePath); err != nil {
			return DrillNativePGTools{}, fmt.Errorf("cannot copy the carrier %s", spec.name)
		}
		if err := drillVerifyFileDigest(pristinePath, spec.digest); err != nil {
			return DrillNativePGTools{}, fmt.Errorf("carrier %s provenance: %w", spec.name, err)
		}
		pristine, err := os.ReadFile(pristinePath)
		if err != nil {
			return DrillNativePGTools{}, fmt.Errorf("cannot read the pristine %s", spec.name)
		}
		patched, err := patchDrillPGClientLibpqDependency(pristine, libpqPath)
		if err != nil {
			return DrillNativePGTools{}, fmt.Errorf("%s libpq transform refused: %w", spec.name, err)
		}
		expected := sha256.Sum256(patched)
		target := filepath.Join(binDir, spec.name)
		if err := os.WriteFile(target, patched, 0o700); err != nil {
			return DrillNativePGTools{}, fmt.Errorf("cannot write the provisioned %s", spec.name)
		}
		onDisk, err := os.ReadFile(target)
		if err != nil || !bytes.Equal(onDisk, patched) {
			return DrillNativePGTools{}, fmt.Errorf("provisioned %s does not match the private expected result", spec.name)
		}
		if sha256.Sum256(onDisk) != expected {
			return DrillNativePGTools{}, fmt.Errorf("provisioned %s result digest does not match the private expected digest", spec.name)
		}
		if err := os.Remove(pristinePath); err != nil {
			return DrillNativePGTools{}, fmt.Errorf("cannot remove the private pristine copy of %s", spec.name)
		}
		version, err := exec.CommandContext(ctx, target, "--version").CombinedOutput()
		if err != nil || !strings.Contains(string(version), drillToolsVersion) {
			return DrillNativePGTools{}, fmt.Errorf("provisioned %s is not the genuine pinned PostgreSQL 18.6 client", spec.name)
		}
		identity, err := drillCaptureFileIdentity(target)
		if err != nil {
			return DrillNativePGTools{}, fmt.Errorf("cannot seal %s: %w", spec.name, err)
		}
		identities[spec.name] = identity
	}
	sealed := &drillSealedPGTools{dir: dir, restore: identities["pg_restore"], dump: identities["pg_dump"]}
	tools := DrillNativePGTools{sealed: sealed}
	if err := tools.revalidate(); err != nil {
		return DrillNativePGTools{}, fmt.Errorf("sealed tools failed self revalidation: %w", err)
	}
	return tools, nil
}

func drillCaptureFileIdentity(path string) (drillPGFileIdentity, error) {
	if !isDrillELF64LE(path) {
		return drillPGFileIdentity{}, errors.New("captured tool is not a 64-bit little-endian ELF")
	}
	info, err := os.Stat(path)
	if err != nil {
		return drillPGFileIdentity{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return drillPGFileIdentity{}, errors.New("tool stat is not a kernel stat")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return drillPGFileIdentity{}, err
	}
	return drillPGFileIdentity{
		path: path, dev: uint64(stat.Dev), ino: stat.Ino, size: info.Size(), sha: sha256.Sum256(data),
	}, nil
}

func isDrillELF64LE(path string) bool {
	header := make([]byte, 6)
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	if _, err := io.ReadFull(file, header); err != nil {
		return false
	}
	return header[0] == 0x7f && header[1] == 'E' && header[2] == 'L' && header[3] == 'F' && header[4] == 2 && header[5] == 1
}

func drillVerifyFileDigest(path, expected string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return errors.New("file cannot be read")
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != expected {
		return errors.New("content digest does not match the pinned carrier content")
	}
	return nil
}

// drillCarrierStoreClass names the docker image store semantics that decide
// which digest is the image identity.
type drillCarrierStoreClass string

const (
	drillCarrierStoreClassic    drillCarrierStoreClass = "classic-overlay2"
	drillCarrierStoreContainerd drillCarrierStoreClass = "containerd-snapshotter"
)

// drillCarrierStoreClassify derives the store class from docker info driver
// facts. The containerd snapshotter marker is checked first because the c8d
// store also reports a driver whose status carries driver-type.
func drillCarrierStoreClassify(driver, driverStatusJSON string) (drillCarrierStoreClass, error) {
	if strings.Contains(driverStatusJSON, "io.containerd.snapshotter.v1") {
		return drillCarrierStoreContainerd, nil
	}
	if strings.TrimSpace(driver) == "overlay2" {
		return drillCarrierStoreClassic, nil
	}
	return "", fmt.Errorf("docker image store driver %q is not a recognized carrier store class", strings.TrimSpace(driver))
}

// drillVerifyCarrierIdentity checks the store-specific image identity, the
// pinned platform and the exact repository digest provenance of the carrier.
func drillVerifyCarrierIdentity(store drillCarrierStoreClass, imageID, imagePlatform, repoDigestsJSON string) error {
	var expected string
	switch store {
	case drillCarrierStoreClassic:
		expected = drillCarrierConfigDigest
	case drillCarrierStoreContainerd:
		expected = drillCarrierIndexDigest
	default:
		return fmt.Errorf("carrier store class %q is unrecognized", store)
	}
	if strings.TrimSpace(imageID) != expected {
		return fmt.Errorf("carrier image content digest does not match the pinned identity (store %s, want %s, got %s)", store, expected, strings.TrimSpace(imageID))
	}
	if strings.TrimSpace(imagePlatform) != "linux/amd64" {
		return fmt.Errorf("carrier image platform is not linux/amd64 (got %q)", strings.TrimSpace(imagePlatform))
	}
	var repoDigests []string
	if err := json.Unmarshal([]byte(repoDigestsJSON), &repoDigests); err != nil {
		return errors.New("carrier image repository digests are not a JSON string array")
	}
	for _, digest := range repoDigests {
		if digest == drillCarrierImage {
			return nil
		}
	}
	return errors.New("carrier image provenance is not the pinned repository digest")
}

// drillVerifyCarrierContainerImage checks that the created container still
// points at the verified carrier image identity.
func drillVerifyCarrierContainerImage(imageID, containerImage string) error {
	if strings.TrimSpace(containerImage) != strings.TrimSpace(imageID) {
		return fmt.Errorf("pristine carrier container image ID is not the pinned content digest (want %s, got %s)", strings.TrimSpace(imageID), strings.TrimSpace(containerImage))
	}
	return nil
}

// drillCarrierStore reads the live docker info store facts.
func drillCarrierStore(ctx context.Context) (drillCarrierStoreClass, error) {
	driver, err := drillDockerOutput(ctx, "info", "--format", "{{.Driver}}")
	if err != nil {
		return "", err
	}
	driverStatus, err := drillDockerOutput(ctx, "info", "--format", "{{json .DriverStatus}}")
	if err != nil {
		return "", err
	}
	return drillCarrierStoreClassify(driver, driverStatus)
}

func drillCreatePristineCarrier(ctx context.Context) (string, func(), error) {
	store, err := drillCarrierStore(ctx)
	if err != nil {
		return "", nil, err
	}
	imageID, err := drillDockerOutput(ctx, "image", "inspect", "--format", "{{.Id}}", drillCarrierImage)
	if err != nil {
		return "", nil, err
	}
	repoDigests, err := drillDockerOutput(ctx, "image", "inspect", "--format", "{{json .RepoDigests}}", drillCarrierImage)
	if err != nil {
		return "", nil, err
	}
	imagePlatform, err := drillDockerOutput(ctx, "image", "inspect", "--format", "{{.Os}}/{{.Architecture}}", drillCarrierImage)
	if err != nil {
		return "", nil, err
	}
	if err := drillVerifyCarrierIdentity(store, imageID, imagePlatform, repoDigests); err != nil {
		return "", nil, err
	}
	containerID, err := drillDockerOutput(ctx, "create", drillCarrierImage, "/bin/true")
	if err != nil {
		return "", nil, err
	}
	remove := func() {
		_ = exec.Command("docker", "rm", "-f", containerID).Run()
	}
	inspected, err := drillDockerOutput(ctx, "inspect", "--format", "{{.Image}}", containerID)
	if err != nil {
		remove()
		return "", nil, err
	}
	if err := drillVerifyCarrierContainerImage(imageID, inspected); err != nil {
		remove()
		return "", nil, err
	}
	return containerID, remove, nil
}

func drillDockerCopy(ctx context.Context, containerID, source, target string) error {
	_, err := drillDockerOutput(ctx, "cp", containerID+":"+source, target)
	return err
}

func drillDockerOutput(ctx context.Context, args ...string) (string, error) {
	if _, err := exec.LookPath("docker"); err != nil {
		return "", errors.New("docker CLI is unavailable")
	}
	out, err := exec.CommandContext(ctx, "docker", args...).Output()
	if err != nil {
		return "", fmt.Errorf("docker %s failed", args[0])
	}
	return strings.TrimSpace(string(out)), nil
}

// patchDrillPGClientLibpqDependency is the narrow, private ELF transform: it
// repurposes one PT_NOTE program header as a read-only PT_LOAD carrying the
// private libpq path and repoints the existing DT_NEEDED "libpq.so.5" entry at
// it, extending DT_STRSZ to cover it. Program code and the dynamic string
// bytes are not rewritten. The returned bytes are the private expected result
// that the authority compares against the written tool.
func patchDrillPGClientLibpqDependency(data []byte, libpqPath string) ([]byte, error) {
	if len(data) < 64 || data[0] != 0x7f || string(data[1:4]) != "ELF" || data[4] != 2 || data[5] != 1 {
		return nil, errors.New("client binary is not a 64-bit little-endian ELF")
	}
	out := append([]byte(nil), data...)
	readU64 := func(offset int) uint64 { return binary.LittleEndian.Uint64(out[offset:]) }
	readU16 := func(offset int) uint16 { return binary.LittleEndian.Uint16(out[offset:]) }
	writeU32 := func(offset int, value uint32) { binary.LittleEndian.PutUint32(out[offset:], value) }
	writeU64 := func(offset int, value uint64) { binary.LittleEndian.PutUint64(out[offset:], value) }
	alignUp := func(value, alignment uint64) uint64 { return (value + alignment - 1) / alignment * alignment }

	type programHeader struct {
		offset            int
		typ               uint32
		fileOffset, vaddr uint64
		fileSize, memSize uint64
	}
	phoff, phentsize, phnum := readU64(0x20), readU16(0x36), readU16(0x38)
	if phentsize == 0 || phnum == 0 || int(phoff)+int(phnum)*int(phentsize) > len(out) {
		return nil, errors.New("client binary program headers are unreadable")
	}
	var headers []programHeader
	for index := 0; index < int(phnum); index++ {
		offset := int(phoff) + index*int(phentsize)
		headers = append(headers, programHeader{
			offset: offset, typ: binary.LittleEndian.Uint32(out[offset:]),
			fileOffset: readU64(offset + 8), vaddr: readU64(offset + 16),
			fileSize: readU64(offset + 32), memSize: readU64(offset + 40),
		})
	}
	vaddrToOffset := func(vaddr uint64) (int, bool) {
		for _, header := range headers {
			if header.typ == 1 && vaddr >= header.vaddr && vaddr < header.vaddr+header.fileSize {
				return int(header.fileOffset + (vaddr - header.vaddr)), true
			}
		}
		return 0, false
	}
	var dynamic, note *programHeader
	for index := range headers {
		header := &headers[index]
		if header.typ == 2 {
			dynamic = header
		}
		if header.typ == 4 && note == nil {
			note = header
		}
	}
	if dynamic == nil || note == nil {
		return nil, errors.New("client binary lacks PT_DYNAMIC or a repurposable PT_NOTE")
	}
	var strtabVaddr uint64
	strszOffset := -1
	dynamicEnd := int(dynamic.fileOffset) + int(dynamic.fileSize)
	if dynamicEnd > len(out) {
		return nil, errors.New("client binary dynamic segment is unreadable")
	}
	for offset := int(dynamic.fileOffset); offset+16 <= dynamicEnd; offset += 16 {
		tag := int64(readU64(offset))
		if tag == 0 {
			break
		}
		switch tag {
		case 5:
			strtabVaddr = readU64(offset + 8)
		case 10:
			strszOffset = offset + 8
		}
	}
	if strtabVaddr == 0 {
		return nil, errors.New("client binary has no DT_STRTAB")
	}
	neededOffset := -1
	for offset := int(dynamic.fileOffset); offset+16 <= dynamicEnd; offset += 16 {
		tag := int64(readU64(offset))
		if tag == 0 {
			break
		}
		if tag != 1 {
			continue
		}
		stringOffset, ok := vaddrToOffset(strtabVaddr + readU64(offset+8))
		if !ok {
			continue
		}
		end := bytes.IndexByte(out[stringOffset:], 0)
		if end < 0 {
			continue
		}
		if string(out[stringOffset:stringOffset+end]) == "libpq.so.5" {
			neededOffset = offset
			break
		}
	}
	if neededOffset < 0 {
		return nil, errors.New("client binary has no DT_NEEDED libpq.so.5 entry")
	}
	payload := []byte(libpqPath + "\x00")
	fileOffset := alignUp(uint64(len(out)), 0x1000)
	var highestVaddr uint64
	for _, header := range headers {
		if header.typ == 1 && header.vaddr+header.memSize > highestVaddr {
			highestVaddr = header.vaddr + header.memSize
		}
	}
	newVaddr := alignUp(highestVaddr+0x1000, 0x1000)
	for uint64(len(out)) < fileOffset {
		out = append(out, 0)
	}
	out = append(out, payload...)
	headerOffset := note.offset
	writeU32(headerOffset, 1)   // PT_LOAD
	writeU32(headerOffset+4, 4) // R
	writeU64(headerOffset+8, fileOffset)
	writeU64(headerOffset+16, newVaddr)
	writeU64(headerOffset+24, newVaddr)
	writeU64(headerOffset+32, uint64(len(payload)))
	writeU64(headerOffset+40, uint64(len(payload)))
	writeU64(headerOffset+48, 0x1000)
	// DT_NEEDED names are read at DT_STRTAB + d_val.
	writeU64(neededOffset+8, newVaddr-strtabVaddr)
	if strszOffset >= 0 {
		writeU64(strszOffset, newVaddr+uint64(len(payload))-strtabVaddr)
	}
	return out, nil
}

// DrillPGRestoreArmOptions is the arm input: the desired writer DSN and the
// operation identity. There is no independent role, executable, args or
// routing authority here; the role/database come from the validated DSN only.
type DrillPGRestoreArmOptions struct {
	TargetDSN   string
	OperationID string
}

// DrillArmObservedPGRestore binds the handle exactly once to the actual
// endpoint capability and the sealed tool set. It validates the operation id,
// the DSN role/database, the single-TCP-endpoint routing equality against the
// exact armed listener, and refuses service/passfile/multi-host/unix or any
// unsupported routing. A second arm on the same handle is refused (no swap).
func DrillArmObservedPGRestore(handle DrillTargetProcessObservation, endpoint DrillOriginEndpoint, tools DrillNativePGTools, opts DrillPGRestoreArmOptions) error {
	if !handle.Valid() {
		return errors.New("arm requires a live supervision handle")
	}
	if err := endpoint.revalidate(); err != nil {
		return fmt.Errorf("arm refused endpoint: %w", err)
	}
	if err := tools.revalidate(); err != nil {
		return fmt.Errorf("arm refused tools: %w", err)
	}
	operationID := strings.TrimSpace(opts.OperationID)
	if operationID == "" || strings.ContainsAny(operationID, "\x00\n\r") {
		return errors.New("arm requires a nonempty operation identity")
	}
	dsn, target, err := drillValidateArmedRestoreDSN(opts.TargetDSN, endpoint)
	if err != nil {
		return err
	}
	key, err := CanonicalTargetKey(target)
	if err != nil {
		return errors.New("arm target identity is unknown")
	}
	state := handle.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.armed != nil {
		return errors.New("arm refused: handle is already armed (no swap or replay)")
	}
	if state.claimed {
		return errors.New("arm refused: handle was already claimed")
	}
	bound := DrillBoundOperation{
		TargetKey: key, RoleFingerprint: target.DataTargetFingerprint().RoleFingerprint,
		Host: target.Host, Port: target.Port, Database: target.Database, Role: target.Role,
		OperationID: operationID, Executable: "pg_restore",
	}
	state.bound = &bound
	state.armed = &drillArmedRestore{
		endpointListener: endpoint.listener, endpointAddr: endpoint.addr, endpointInode: endpoint.inode,
		tools: *tools.sealed, dsn: dsn,
	}
	return nil
}

// DrillRunObservedPGRestore starts the native restore from the private arm
// configuration only: a closed hardcoded flag set plus the canonical --dbname
// derived from the validated DSN. There are no caller args, no host/port/user/
// database or positional override, and no executable input. Immediately before
// start it revalidates the live listener socket identity and the sealed tool
// provenance (path, file id, content hash).
//
// Exactly one actual start and sole wait is allowed per handle: the run
// reservation is taken atomically under the same state mutex as arm/claim
// BEFORE any launch. Repeated, concurrent or post-terminal invocations are
// refused without touching the observation facts, and any failure after the
// reservation (including a refused pre-start revalidation) permanently
// consumes the attempt. A retry requires a new handle. The legitimate gate
// claim is not forbidden by the reservation; it runs after the child started.
func DrillRunObservedPGRestore(
	ctx context.Context,
	runner TargetProcessRunner,
	handle DrillTargetProcessObservation,
	archive io.Reader,
	stdout, stderr io.Writer,
) (PGCommandResult, error) {
	notStarted := PGCommandResult{Outcome: PGCommandNotStarted, ExitCode: -1}
	if !handle.Valid() {
		return notStarted, errors.New("observed restore requires a live supervision handle")
	}
	state := handle.state
	state.mu.Lock()
	if state.armed == nil {
		state.mu.Unlock()
		return notStarted, errors.New("observed restore requires an armed endpoint and sealed tools")
	}
	if state.runInvoked {
		state.mu.Unlock()
		return notStarted, errors.New("observed restore was already invoked on this handle: one actual start and sole wait per handle")
	}
	if state.claimed {
		state.mu.Unlock()
		return notStarted, errors.New("observed restore refused: the handle was already claimed")
	}
	armed := state.armed
	state.runInvoked = true
	state.mu.Unlock()
	endpoint := DrillOriginEndpoint{listener: armed.endpointListener, addr: armed.endpointAddr, inode: armed.endpointInode}
	if err := endpoint.revalidate(); err != nil {
		return notStarted, fmt.Errorf("observed restore refused before start: %w", err)
	}
	tools := DrillNativePGTools{sealed: &armed.tools}
	if err := tools.revalidate(); err != nil {
		return notStarted, fmt.Errorf("observed restore refused before start: %w", err)
	}
	args := append([]string(nil), drillRestoreFlags...)
	args = append(args, "--dbname="+armed.dsn)
	configured := runner
	configured.observation = handle.observation
	return configured.RunPGCommand(ctx, armed.tools.restore.path, args, archive, stdout, stderr, nil)
}

// drillRestoreFlags is the closed hardcoded flag set of the existing native
// positive; nothing here is caller-controlled.
var drillRestoreFlags = []string{"--no-owner", "--no-privileges"}

func drillValidateArmedRestoreDSN(raw string, endpoint DrillOriginEndpoint) (string, controlstore.DSNTarget, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", controlstore.DSNTarget{}, errors.New("arm requires a target DSN")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", controlstore.DSNTarget{}, errors.New("arm target DSN is unparsable")
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return "", controlstore.DSNTarget{}, errors.New("arm target DSN must be a postgres URI (unsupported routing refused)")
	}
	if parsed.Opaque != "" || parsed.Fragment != "" {
		return "", controlstore.DSNTarget{}, errors.New("arm target DSN uses unsupported routing")
	}
	if parsed.User == nil || strings.TrimSpace(parsed.User.Username()) == "" {
		return "", controlstore.DSNTarget{}, errors.New("arm target DSN requires a nonempty role")
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	if err != nil || host == "" || port == "" || strings.Contains(host, ",") || strings.HasPrefix(host, "/") {
		return "", controlstore.DSNTarget{}, errors.New("arm target DSN must be exactly one TCP host:port endpoint")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return "", controlstore.DSNTarget{}, errors.New("arm target DSN port is invalid")
	}
	if net.JoinHostPort(host, port) != endpoint.addr {
		return "", controlstore.DSNTarget{}, errors.New("arm target DSN endpoint does not equal the exact armed origin listener")
	}
	database := strings.TrimPrefix(parsed.Path, "/")
	if database == "" || strings.Contains(database, "/") {
		return "", controlstore.DSNTarget{}, errors.New("arm target DSN requires exactly one database")
	}
	// Parse the raw query strictly: every malformed percent-encoding, bare
	// semicolon separator or otherwise unparsable pair is refused instead of
	// being silently discarded, and each supported key must appear exactly
	// once with a nonempty value. Duplicates (including percent-encoded
	// spellings that decode to the same name) are refused. No raw DSN text is
	// echoed in any refusal.
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return "", controlstore.DSNTarget{}, errors.New("arm target DSN query is malformed")
	}
	for key, values := range query {
		if key != "sslmode" && key != "gssencmode" {
			return "", controlstore.DSNTarget{}, errors.New("arm target DSN uses unsupported routing parameters")
		}
		if len(values) != 1 || values[0] == "" {
			return "", controlstore.DSNTarget{}, errors.New("arm target DSN duplicates or empties a supported routing parameter")
		}
	}
	canonical := url.URL{Scheme: "postgres", User: parsed.User, Host: endpoint.addr, Path: "/" + database}
	canonicalQuery := url.Values{}
	if values, ok := query["sslmode"]; ok {
		canonicalQuery.Set("sslmode", values[0])
	}
	if values, ok := query["gssencmode"]; ok {
		canonicalQuery.Set("gssencmode", values[0])
	}
	canonical.RawQuery = canonicalQuery.Encode()
	target, err := controlstore.ParseDSNTarget(canonical.String())
	if err != nil || target.Role == "" || target.Database == "" {
		return "", controlstore.DSNTarget{}, errors.New("arm target DSN identity is invalid")
	}
	return canonical.String(), target, nil
}
