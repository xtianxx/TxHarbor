//go:build linux && drill

package recovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// ---------------------------------------------------------------------------
// OG01/coordination Phase 1: borrowed-writer transport bridge (drill only).
//
// This is the minimal private arm mode for the real coordinator: it never
// feeds a proxy DSN into DrillArmObservedPGRestore. Instead it retains the
// caller's real borrowed *TargetLock, the original validated target key /
// role fingerprint / operation / instance, the real control and observer
// identities, the actual factory listener capability, the sealed tools and
// the private process observation. The only production change it drives is
// the package-private child-argument preparation hook: it validates the
// coordinator's exact three-argument invocation, resolves the executable
// against the sealed private tool cap, rewrites ONLY the host/port to the
// actual armed listener, and refuses everything else before Start.
// ---------------------------------------------------------------------------

// DrillBorrowedSQLCapture is the read-only SQL capture gate101 may consume. It
// carries no clean-issued authority: data/system_identifier facts alone are
// never sufficient for a later release.
type DrillBorrowedSQLCapture interface {
	ClusterSystemIdentifier() string
	PostmasterStartTime() time.Time
	TargetDatabaseOID() uint32
	WriterRoleOID() uint32
	ControlBackendPID() int
	ControlBackendStart() time.Time
	ControlDatabaseOID() uint32
	// ControlPostmasterStart is the control-lock-owner session's own
	// pg_postmaster_start_time, captured under the lock's mutex from the same
	// session that holds the advisory lock. It is the SQL control-incarnation
	// fact the factory compares with the observer before publishing any
	// facts-complete capture; it is not an OS binding.
	ControlPostmasterStart() time.Time
	ControlTargetKey() TargetKey
	OriginalTargetKey() TargetKey
	OriginalRoleFingerprint() string
	OriginalOperationID() string
	OriginalInstanceID() string
	SQLFactsComplete() bool
	MissingSQLFacts() []string
	// Phase2OSBindingState is always "OPEN" in phase 1: the exact protected
	// OS postmaster/server-child/socket binding is captured by the phase-2
	// gate factory from the actual fixture, never accepted as a caller scalar.
	Phase2OSBindingState() string
}

// DrillBorrowedControlBinding is the phase-1 SQL-runtime capture of the
// original cluster, incarnation, database/role identities and the actual
// borrowed control lock owner. All fields are private; only read-only getters
// leave this package.
type DrillBorrowedControlBinding struct {
	clusterSystemIdentifier  string
	postmasterStartTime      time.Time
	targetDatabaseOID        uint32
	writerRoleOID            uint32
	controlBackendPID        int
	controlBackendStart      time.Time
	controlDatabaseOID       uint32
	controlPostmasterStart   time.Time
	controlClusterIdentifier string
	controlTargetKey         TargetKey
	originalTargetKey        TargetKey
	originalRoleFingerprint  string
	originalOperationID      string
	originalInstanceID       string
	transportTargetKey       TargetKey
	capturedAt               time.Time
	missing                  []string
}

func (b DrillBorrowedControlBinding) ClusterSystemIdentifier() string {
	return b.clusterSystemIdentifier
}
func (b DrillBorrowedControlBinding) PostmasterStartTime() time.Time { return b.postmasterStartTime }
func (b DrillBorrowedControlBinding) TargetDatabaseOID() uint32      { return b.targetDatabaseOID }
func (b DrillBorrowedControlBinding) WriterRoleOID() uint32          { return b.writerRoleOID }
func (b DrillBorrowedControlBinding) ControlBackendPID() int         { return b.controlBackendPID }
func (b DrillBorrowedControlBinding) ControlBackendStart() time.Time { return b.controlBackendStart }
func (b DrillBorrowedControlBinding) ControlDatabaseOID() uint32     { return b.controlDatabaseOID }
func (b DrillBorrowedControlBinding) ControlPostmasterStart() time.Time {
	return b.controlPostmasterStart
}
func (b DrillBorrowedControlBinding) ControlTargetKey() TargetKey  { return b.controlTargetKey }
func (b DrillBorrowedControlBinding) OriginalTargetKey() TargetKey { return b.originalTargetKey }
func (b DrillBorrowedControlBinding) OriginalRoleFingerprint() string {
	return b.originalRoleFingerprint
}
func (b DrillBorrowedControlBinding) OriginalOperationID() string   { return b.originalOperationID }
func (b DrillBorrowedControlBinding) OriginalInstanceID() string    { return b.originalInstanceID }
func (b DrillBorrowedControlBinding) TransportTargetKey() TargetKey { return b.transportTargetKey }
func (b DrillBorrowedControlBinding) CapturedAt() time.Time         { return b.capturedAt }
func (b DrillBorrowedControlBinding) Phase2OSBindingState() string  { return "OPEN" }
func (b DrillBorrowedControlBinding) SQLFactsComplete() bool {
	// A zero value, a JSON round trip and any incomplete capture must never
	// read as all-true: completeness requires every fixed fact to be positive
	// in addition to the recorded missing list being empty.
	return len(b.missing) == 0 &&
		b.clusterSystemIdentifier != "" && !b.postmasterStartTime.IsZero() &&
		b.targetDatabaseOID != 0 && b.writerRoleOID != 0 &&
		b.controlBackendPID > 0 && !b.controlBackendStart.IsZero() &&
		b.controlDatabaseOID != 0 && !b.controlPostmasterStart.IsZero() &&
		b.controlClusterIdentifier != "" &&
		b.controlTargetKey != (TargetKey{}) && b.originalTargetKey != (TargetKey{}) &&
		b.transportTargetKey != (TargetKey{}) && b.originalRoleFingerprint != "" &&
		b.originalOperationID != ""
}
func (b DrillBorrowedControlBinding) MissingSQLFacts() []string {
	return append([]string(nil), b.missing...)
}

// drillBorrowedRunLifetime is the single shared lifetime reservation of a
// factory-created borrowed run. It is allocated ONLY by the authentic factory
// and is retained by private pointer, so every struct copy of one run shares
// exactly one reservation: a second (or concurrent) Run through any copy is
// refused. A zero value, a JSON round trip or any hand-built struct has no
// lifetime pointer and is permanently inert.
type drillBorrowedRunLifetime struct {
	mu       sync.Mutex
	reserved bool
}

// DrillBorrowedWriterRun is the opaque one-use borrowed-transport run. It
// cannot be copied into authority: JSON and zero values carry none, and all
// copies of a factory-created run share the same private lifetime pointer.
type DrillBorrowedWriterRun struct {
	life         *drillBorrowedRunLifetime
	opts         TargetWriterOptions
	lock         *TargetLock
	endpoint     DrillOriginEndpoint
	tools        DrillNativePGTools
	observed     *targetProcessObservation
	state        *drillObservedState
	binding      DrillBorrowedControlBinding
	transportKey TargetKey
}

// DrillNewBorrowedWriterRun captures the real borrowed identities from the
// caller's lock session and observer DSN and the actual endpoint/tools
// capabilities. No caller hash, PID, JSON projection or same-server boolean is
// accepted as authority.
func DrillNewBorrowedWriterRun(ctx context.Context, opts TargetWriterOptions, lock *TargetLock, endpoint DrillOriginEndpoint, tools DrillNativePGTools) (*DrillBorrowedWriterRun, error) {
	if ctx == nil {
		return nil, errors.New("borrowed transport requires a context")
	}
	if lock == nil {
		return nil, errors.New("borrowed transport requires the original caller-owned target lock")
	}
	if err := endpoint.revalidate(); err != nil {
		return nil, fmt.Errorf("borrowed transport endpoint refused: %w", err)
	}
	if err := tools.revalidate(); err != nil {
		return nil, fmt.Errorf("borrowed transport sealed tools refused: %w", err)
	}
	// The factory is the only place the restore executable is selected: the
	// private sealed tool path is chosen here and vetted by the revalidation
	// above; a caller that already selected a conflicting private executable is
	// refused instead of letting the coordinator PATH-resolve an ordinary,
	// different ELF. An owner that selected the same sealed path is accepted.
	if opts.executable != "" && opts.executable != tools.sealed.restore.path {
		return nil, errors.New("borrowed transport refuses a conflicting private restore executable")
	}
	opts.executable = tools.sealed.restore.path
	controlTarget, err := controlstore.ParseDSNTarget(opts.ControlDSN)
	if err != nil {
		return nil, errors.New("borrowed transport control DSN identity is invalid")
	}
	controlKey, err := CanonicalTargetKey(controlTarget)
	if err != nil {
		return nil, errors.New("borrowed transport control identity is unknown")
	}
	originalKey, err := CanonicalTargetKey(opts.TrustedTarget)
	if err != nil {
		return nil, errors.New("borrowed transport original target identity is unknown")
	}
	// The canonical observer identity (host/port/database, role may be a
	// different privileged observer) is validated BEFORE any observer
	// connection is attempted, so a wrong observer can never publish facts.
	observerTarget, err := controlstore.ParseDSNTarget(opts.ObserverDSN)
	if err != nil {
		return nil, errors.New("borrowed transport observer DSN identity is invalid")
	}
	observerKey, err := CanonicalTargetKey(observerTarget)
	if err != nil || observerKey != originalKey {
		return nil, errors.New("borrowed transport observer identity does not match the original trusted target")
	}
	transportKey, err := drillTransportTargetKey(endpoint.addr, opts.TrustedTarget)
	if err != nil {
		return nil, err
	}
	if transportKey == originalKey {
		return nil, errors.New("borrowed transport key must differ from the original direct target key")
	}
	if err := drillValidateBorrowedLock(ctx, lock, controlKey, originalKey); err != nil {
		return nil, err
	}
	controlFacts, err := drillCaptureControlSessionFacts(ctx, lock)
	if err != nil {
		return nil, fmt.Errorf("borrowed control session SQL identity: %w", err)
	}
	observerFacts, err := drillCaptureObserverFacts(ctx, opts.ObserverDSN, opts.TrustedTarget)
	if err != nil {
		return nil, fmt.Errorf("original observer SQL identity: %w", err)
	}
	// Before publishing any facts-complete capture, a known control cluster or
	// postmaster incarnation mismatch with the observer is refused: the SQL
	// system_identifier alone is not accepted as a same-cluster proof, and a
	// mismatched pair must never be published as coherent binding facts.
	if controlFacts.systemIdentifier != "" && observerFacts.systemIdentifier != "" &&
		controlFacts.systemIdentifier != observerFacts.systemIdentifier {
		return nil, errors.New("borrowed transport control and observer sessions are not the same PostgreSQL cluster")
	}
	if !controlFacts.postmasterStart.IsZero() && !observerFacts.postmasterStart.IsZero() &&
		!controlFacts.postmasterStart.Equal(observerFacts.postmasterStart) {
		return nil, errors.New("borrowed transport control and observer sessions are not the same postmaster incarnation")
	}
	missing := append([]string(nil), observerFacts.missing...)
	missing = append(missing, controlFacts.missing...)
	bound := DrillBoundOperation{
		TargetKey: originalKey, RoleFingerprint: opts.TrustedTarget.DataTargetFingerprint().RoleFingerprint,
		Host: opts.TrustedTarget.Host, Port: opts.TrustedTarget.Port,
		Database: opts.TrustedTarget.Database, Role: opts.TrustedTarget.Role,
		OperationID: opts.OperationID, Executable: "pg_restore",
	}
	observed := &targetProcessObservation{}
	state := &drillObservedState{
		bound: &bound,
		armed: &drillArmedRestore{
			endpointListener: endpoint.listener, endpointAddr: endpoint.addr, endpointInode: endpoint.inode,
			tools: *tools.sealed, dsn: opts.TargetDSN,
		},
	}
	run := &DrillBorrowedWriterRun{
		life: &drillBorrowedRunLifetime{},
		opts: opts, lock: lock, endpoint: endpoint, tools: tools,
		observed: observed, state: state, transportKey: transportKey,
		binding: DrillBorrowedControlBinding{
			clusterSystemIdentifier:  observerFacts.systemIdentifier,
			postmasterStartTime:      observerFacts.postmasterStart,
			targetDatabaseOID:        observerFacts.databaseOID,
			writerRoleOID:            observerFacts.roleOID,
			controlBackendPID:        controlFacts.backendPID,
			controlBackendStart:      controlFacts.backendStart,
			controlDatabaseOID:       controlFacts.databaseOID,
			controlPostmasterStart:   controlFacts.postmasterStart,
			controlClusterIdentifier: controlFacts.systemIdentifier,
			controlTargetKey:         controlKey,
			originalTargetKey:        originalKey,
			originalRoleFingerprint:  opts.TrustedTarget.DataTargetFingerprint().RoleFingerprint,
			originalOperationID:      opts.OperationID,
			originalInstanceID:       opts.InstanceID,
			transportTargetKey:       transportKey,
			capturedAt:               time.Now().UTC(),
			missing:                  missing,
		},
	}
	return run, nil
}

// Binding returns the immutable phase-1 SQL capture.
func (r *DrillBorrowedWriterRun) Binding() DrillBorrowedControlBinding { return r.binding }

// TransportTargetKey is the distinct transport-route key (listener endpoint +
// original database/role), demonstrably different from the original direct
// target key. It is a recorded fact, not a new guard namespace.
func (r *DrillBorrowedWriterRun) TransportTargetKey() TargetKey { return r.transportKey }

// Observation is the opaque private handle connected to the actual process
// observation. It grants no *exec.Cmd/*os.Process; after a real Started it can
// be consumed with ClaimForEndpoint using the same actual endpoint capability.
func (r *DrillBorrowedWriterRun) Observation() DrillTargetProcessObservation {
	return DrillTargetProcessObservation{observation: r.observed, state: r.state}
}

// Run reserves the run exactly once and executes the real coordinator with the
// borrowed lock, the private observation and the transport hook. It returns
// the frozen opaque receipt even on failure or cancellation; there is no
// second Wait and no clean-retirement proof.
//
// The reservation lives behind the private lifetime pointer allocated only by
// the authentic factory, so every copy of one run shares the same once state:
// the reservation is taken atomically under that shared mutex BEFORE any
// coordinator entry, any invocation (including one with a context canceled
// before Start) consumes the single attempt, and repeated/concurrent/terminal
// invocations through any copy are refused without touching the real
// observation or the pinned owner's Wait facts. A zero value, a JSON round
// trip or any hand-built struct has no lifetime and is permanently inert.
func (r *DrillBorrowedWriterRun) Run(ctx context.Context) (TargetWriterResult, DrillTargetProcessReceipt, error) {
	if r == nil || r.life == nil {
		return TargetWriterResult{}, DrillTargetProcessReceipt{}, errors.New("borrowed writer run was not created by the factory (inert value)")
	}
	life := r.life
	life.mu.Lock()
	if life.reserved {
		life.mu.Unlock()
		return TargetWriterResult{}, DrillTargetProcessReceipt{}, errors.New("borrowed writer run was already reserved (one actual coordinator run per handle)")
	}
	life.reserved = true
	life.mu.Unlock()
	opts := r.opts
	opts.borrowedLock = r.lock
	opts.Runner.observation = r.observed
	opts.prepareChildArgs = r.prepareChildArgs
	result, err := runTargetWriter(ctx, opts)
	return result, DrillTargetProcessReceipt{receipt: result.processReceipt}, err
}

// prepareChildArgs is the private hook the coordinator invokes. It validates
// the EXACT canonical three-argument invocation against the original trusted
// DSN and the sealed tool/endpoint capabilities and rewrites ONLY host/port.
func (r *DrillBorrowedWriterRun) prepareChildArgs(stage targetWriterChildArgStage, executable string, args []string) ([]string, error) {
	refuse := func(reason string) ([]string, error) {
		return nil, &targetWriterChildTransportRefusal{stage: stage, reason: reason}
	}
	if r.tools.sealed == nil || executable != r.tools.sealed.restore.path {
		return refuse("executable is not the sealed pinned client")
	}
	if err := r.tools.revalidate(); err != nil {
		return refuse("sealed tool provenance")
	}
	if err := r.endpoint.revalidate(); err != nil {
		return refuse("origin endpoint capability")
	}
	if len(args) != 5 || args[0] != "--clean" || args[1] != "--if-exists" || args[2] != "--no-owner" || args[3] != "--no-privileges" || !strings.HasPrefix(args[4], "--dbname=") {
		return refuse("child argument shape")
	}
	rewritten, ok := r.rewriteTransportDSN(strings.TrimPrefix(args[4], "--dbname="))
	if !ok {
		return refuse("child argument routing")
	}
	return []string{"--clean", "--if-exists", "--no-owner", "--no-privileges", "--dbname=" + rewritten}, nil
}

// rewriteTransportDSN validates the coordinator's tagged canonical DSN against
// the original trusted target and rewrites only the host/port to the actual
// armed listener. Role, database, password, application tag and settings are
// preserved; independent routing (service/passfile/hostaddr/multi-host/unix/
// unknown options) is refused.
func (r *DrillBorrowedWriterRun) rewriteTransportDSN(tagged string) (string, bool) {
	original, err := url.Parse(r.opts.TargetDSN)
	if err != nil || original.Scheme == "" || original.User == nil {
		return "", false
	}
	taggedURL, err := url.Parse(tagged)
	if err != nil || taggedURL.Scheme != original.Scheme || taggedURL.Opaque != "" || taggedURL.Fragment != "" {
		return "", false
	}
	if taggedURL.User == nil || taggedURL.User.String() != original.User.String() {
		return "", false
	}
	if taggedURL.Path != original.Path {
		return "", false
	}
	if strings.Contains(taggedURL.Host, ",") || strings.HasPrefix(taggedURL.Host, "/") {
		return "", false
	}
	if _, _, err := net.SplitHostPort(taggedURL.Host); err != nil {
		return "", false
	}
	if !strings.EqualFold(taggedURL.Host, original.Host) {
		return "", false
	}
	originalQuery := original.Query()
	taggedQuery := taggedURL.Query()
	applicationTag := taggedQuery.Get("application_name")
	if applicationTag == "" || ValidateAttemptApplicationName(applicationTag) != nil {
		return "", false
	}
	for key, values := range taggedQuery {
		switch key {
		case "application_name":
			if len(values) != 1 {
				return "", false
			}
		case "sslmode", "gssencmode":
			if len(values) != 1 || originalQuery.Get(key) != values[0] {
				return "", false
			}
		default:
			return "", false
		}
	}
	for key := range originalQuery {
		if key != "sslmode" && key != "gssencmode" {
			return "", false
		}
	}
	transport := *taggedURL
	transport.Host = r.endpoint.addr
	return transport.String(), true
}

func drillTransportTargetKey(endpointAddr string, trusted controlstore.DSNTarget) (TargetKey, error) {
	host, port, err := net.SplitHostPort(endpointAddr)
	if err != nil {
		return TargetKey{}, errors.New("borrowed transport endpoint address is invalid")
	}
	portNumber, err := net.LookupPort("tcp", port)
	if err != nil {
		return TargetKey{}, errors.New("borrowed transport endpoint port is invalid")
	}
	key, err := CanonicalTargetKey(controlstore.DSNTarget{
		Host: host, Port: uint16(portNumber), Database: trusted.Database, Role: trusted.Role,
	})
	if err != nil {
		return TargetKey{}, errors.New("borrowed transport endpoint identity is unknown")
	}
	return key, nil
}

// drillValidateBorrowedLock mirrors the coordinator's borrowed identity gate
// without bypassing it: the real runTargetWriter still performs the same
// checks. Errors here are additive capture refusals.
func drillValidateBorrowedLock(ctx context.Context, lock *TargetLock, controlKey, originalKey TargetKey) error {
	if lock.controlKey == (TargetKey{}) || lock.controlKey != controlKey {
		return errors.New("borrowed target lock control-store identity does not match configured control store")
	}
	k1, k2 := originalKey.AdvisoryLockKey()
	if lock.key1 != k1 || lock.key2 != k2 {
		return errors.New("borrowed target lock does not match the original trusted target")
	}
	if err := lock.Health(ctx); err != nil {
		return errors.New("borrowed target lock health is uncertain")
	}
	return nil
}

// drillControlSessionFacts is the control-lock-owner SQL runtime identity
// captured under the lock's own mutex: backend pid/start, control database
// OID, postmaster start, cluster system_identifier and whether this exact
// session still holds the target namespace advisory key.
type drillControlSessionFacts struct {
	backendPID       int
	backendStart     time.Time
	databaseOID      uint32
	postmasterStart  time.Time
	systemIdentifier string
	missing          []string
}

// drillCaptureControlSessionFacts reads the actual control lock owner SQL
// identity, including its own pg_postmaster_start_time and the held
// namespace advisory key, under the lock's existing mutex. It never calls
// Health (no recursive lock) and never accepts a caller scalar.
func drillCaptureControlSessionFacts(ctx context.Context, lock *TargetLock) (drillControlSessionFacts, error) {
	lock.mu.Lock()
	defer lock.mu.Unlock()
	if lock.conn == nil || lock.closed {
		return drillControlSessionFacts{}, errors.New("borrowed control session is closed")
	}
	var facts drillControlSessionFacts
	var namespaceKeyHeld bool
	if err := lock.conn.QueryRow(ctx, `
SELECT pid::int, backend_start, pg_postmaster_start_time(),
       (SELECT oid FROM pg_database WHERE datname = current_database()),
       EXISTS (SELECT 1 FROM pg_locks
               WHERE locktype = 'advisory' AND granted AND pid = pg_backend_pid()
                 AND objsubid = 2 AND classid = $1::oid AND objid = $2::oid)
FROM pg_stat_activity WHERE pid = pg_backend_pid()`, uint32(lock.key1), uint32(lock.key2)).
		Scan(&facts.backendPID, &facts.backendStart, &facts.postmasterStart, &facts.databaseOID, &namespaceKeyHeld); err != nil {
		return drillControlSessionFacts{}, errors.New("control lock owner query failed")
	}
	// Best effort under the same mutex: unreadable cluster identity is a
	// missing fact, never a name-only acceptance.
	if err := lock.conn.QueryRow(ctx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&facts.systemIdentifier); err != nil {
		facts.systemIdentifier = ""
	}
	if facts.backendPID <= 0 {
		facts.missing = append(facts.missing, "control backend PID")
	}
	if facts.backendStart.IsZero() {
		facts.missing = append(facts.missing, "control backend start time")
	}
	if facts.databaseOID == 0 {
		facts.missing = append(facts.missing, "control database OID")
	}
	if facts.postmasterStart.IsZero() {
		facts.missing = append(facts.missing, "control postmaster start time")
	}
	if facts.systemIdentifier == "" {
		facts.missing = append(facts.missing, "control cluster system_identifier")
	}
	if !namespaceKeyHeld {
		facts.missing = append(facts.missing, "control namespace advisory key")
	}
	return facts, nil
}

type drillObserverSQLFacts struct {
	currentDatabase  string
	systemIdentifier string
	postmasterStart  time.Time
	databaseOID      uint32
	roleOID          uint32
	missing          []string
}

// drillCaptureObserverFacts reads the ORIGINAL target cluster SQL identity
// through the original trusted observer DSN (never a proxy route). The
// database OID is taken from the session's actual current_database(); a
// connection that landed in any other database is refused instead of looking
// the OID up by the asserted name.
func drillCaptureObserverFacts(ctx context.Context, observerDSN string, trusted controlstore.DSNTarget) (drillObserverSQLFacts, error) {
	conn, err := pgx.Connect(ctx, observerDSN)
	if err != nil {
		return drillObserverSQLFacts{}, errors.New("original observer is not reachable")
	}
	defer conn.Close(context.Background())
	var facts drillObserverSQLFacts
	if err := conn.QueryRow(ctx, `
SELECT current_database(), pg_postmaster_start_time(),
       (SELECT oid FROM pg_database WHERE datname = current_database()),
       (SELECT oid FROM pg_roles WHERE rolname = $1)`, trusted.Role).
		Scan(&facts.currentDatabase, &facts.postmasterStart, &facts.databaseOID, &facts.roleOID); err != nil {
		return drillObserverSQLFacts{}, errors.New("original observer SQL identity query failed")
	}
	if facts.currentDatabase != trusted.Database {
		return drillObserverSQLFacts{}, errors.New("original observer connection database does not match the original trusted target")
	}
	if err := conn.QueryRow(ctx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&facts.systemIdentifier); err != nil {
		facts.systemIdentifier = ""
	}
	if facts.systemIdentifier == "" {
		facts.missing = append(facts.missing, "target cluster system_identifier")
	}
	if facts.postmasterStart.IsZero() {
		facts.missing = append(facts.missing, "postmaster start time")
	}
	if facts.databaseOID == 0 {
		facts.missing = append(facts.missing, "target database OID")
	}
	if facts.roleOID == 0 {
		facts.missing = append(facts.missing, "writer role OID")
	}
	return facts, nil
}
