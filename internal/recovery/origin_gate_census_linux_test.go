//go:build linux && drill

// origin_gate_census_linux_test.go contains the strict-census findings:
// double-fork orphan inheritance (original owner alive, intermediate reaped,
// grandchild reparented with an authentically unreadable fd table), a start
// identity change after the ownership scan, a latch racing clean-verdict
// publication, and deterministic permission/IO seam refusals.
package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestDrillOriginGateCensusStrictOrphanRefused proves the double-fork orphan
// holder is never hidden: the intermediate exits and is reaped by the original
// parent while the reparented grandchild keeps the live socket with dumpable=0.
// The strict census must refuse (UNKNOWN), latch and drain, never CleanRetired.
func TestDrillOriginGateCensusStrictOrphanRefused(t *testing.T) {
	detail := "double-fork orphan FD holder after registration: original owner alive, intermediate reaped, grandchild reparented dumpable=0; strict census must refuse"
	recordOriginGateResult(t, "TestDrillOriginGateCensusStrictOrphanRefused", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	table := fmt.Sprintf("origin_gate_census_orphan_%d", time.Now().UnixNano())
	if _, err := fx.admin.Exec(ctx, `CREATE TABLE `+pgx.Identifier{table}.Sanitize()+` (seq bigserial PRIMARY KEY, ref text NOT NULL UNIQUE)`); err != nil {
		t.Fatalf("create orphan sequence table: %v", err)
	}
	if _, err := fx.admin.Exec(ctx, `GRANT INSERT, SELECT ON `+pgx.Identifier{table}.Sanitize()+` TO `+pgx.Identifier{fx.role}.Sanitize()); err != nil {
		t.Fatalf("grant orphan table access: %v", err)
	}
	if _, err := fx.admin.Exec(ctx, `GRANT USAGE, SELECT ON SEQUENCE `+pgx.Identifier{table + "_seq_seq"}.Sanitize()+` TO `+pgx.Identifier{fx.role}.Sanitize()); err != nil {
		t.Fatalf("grant orphan sequence usage: %v", err)
	}
	gate := newOriginGate(t, ctx, fx)
	launcher := lifecycleLauncher(t)
	ref := fmt.Sprintf("census-orphan-%d", time.Now().UnixNano())
	client, cap, err := launcher.launch(t, ctx, gate,
		[]string{"sequence", gate.Endpoint(), fx.role, table, originGateAppName}, fx.password)
	if err != nil {
		t.Fatalf("spawn orphan census client: %v", err)
	}
	session, err := gate.Admit(ctx, cap)
	if err != nil {
		t.Fatalf("admit orphan census origin: %v", err)
	}
	if _, err := client.waitEvent(ctx, "orphan census authentication", func(line string) bool {
		return strings.Contains(line, "ORIGIN_LIFECYCLE_AUTH")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := originGateWaitRegistration(ctx, session); err != nil {
		t.Fatalf("%v", err)
	}
	if err := client.sendLine(ref); err != nil {
		t.Fatalf("send pre-orphan ref: %v", err)
	}
	if _, err := client.waitEvent(ctx, "pre-orphan command completion", func(line string) bool {
		return strings.Contains(line, "ORIGIN_LIFECYCLE_COMMAND")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	// Install the ownership-census pause under g.mu and await the watcher's
	// entered ack before triggering the fork: this pins the causal window to
	// the actual watcher blocking at the real census wait point, not merely a
	// channel assignment.
	censusBarrier, censusEntered := gate.holdCensusBarrier()
	barrierReleased := false
	defer func() {
		if !barrierReleased {
			close(censusBarrier)
		}
	}()
	select {
	case <-censusEntered:
	case <-time.After(15 * time.Second):
		t.Fatalf("watcher never entered the installed ownership-census barrier")
	}
	originalStart := cap.start
	if err := client.sendLine("orphanfd"); err != nil {
		t.Fatalf("request double-fork orphan transfer: %v", err)
	}
	grandchildLine, err := client.waitEvent(ctx, "reparented grandchild record", func(line string) bool {
		return strings.Contains(line, "ORIGIN_LIFECYCLE_GRANDCHILD")
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
	grandchildFields := originGateEventFields(grandchildLine)
	grandchildPID, _ := strconv.Atoi(grandchildFields["pid"])
	grandchildStart := grandchildFields["start"]
	if grandchildPID <= 1 || grandchildStart == "" {
		t.Fatalf("grandchild identity record is incomplete: %q", grandchildLine)
	}
	t.Cleanup(func() {
		if current, err := hostProcessStartID(grandchildPID); err == nil && strconv.FormatUint(current, 10) == grandchildStart {
			if process, err := os.FindProcess(grandchildPID); err == nil {
				_ = process.Kill()
			}
		}
	})
	if _, err := client.waitEvent(ctx, "intermediate reaped by original parent", func(line string) bool {
		return strings.Contains(line, "ORIGIN_LIFECYCLE_MID_EXIT") && strings.Contains(line, "status=0")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := client.waitEvent(ctx, "grandchild dumpable verification", func(line string) bool {
		return strings.Contains(line, "ORIGIN_LIFECYCLE_DUMPABLE") && strings.Contains(line, "get=0")
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := originGateWaitFor(ctx, "grandchild identity stable", func() bool {
		current, err := hostProcessStartID(grandchildPID)
		return err == nil && strconv.FormatUint(current, 10) == grandchildStart
	}); err != nil {
		t.Fatalf("grandchild identity changed (reused/reaped) before the readlink proof: %v", err)
	}
	// Authentic unreadability and real orphan namespace facts, asserted by the
	// observing test itself while the census is still paused. The census's
	// authoritative read is the per-fd readlink; kernel behaviour here allows
	// the fd directory listing for root but denies every readlink for a
	// dumpable=0 process, and a readable readlink is a FAIL, never a skip.
	if _, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/0", grandchildPID)); err == nil || !errors.Is(err, os.ErrPermission) {
		t.Fatalf("grandchild fd readlink was readable from the gate credentials (err=%v); authentic denial not established", err)
	}
	t.Logf("grandchild fd readlink denied with EACCES under the gate credentials (directory listing may remain permitted; the census reads per-fd readlinks)")
	ppid := readCensusPPid(t, grandchildPID)
	if ppid != 1 {
		t.Fatalf("grandchild PPid is %d, want the namespace init 1 for an authoritative orphan", ppid)
	}
	originalStartID, err := strconv.ParseUint(originalStart, 10, 64)
	if err != nil {
		t.Fatalf("parse original start identity: %v", err)
	}
	if current, err := hostProcessStartID(cap.pid); err != nil || current != originalStartID {
		t.Fatalf("original owner PID %d is no longer alive with its captured start identity (err=%v)", cap.pid, err)
	}
	close(censusBarrier)
	barrierReleased = true
	if err := originGateWaitFor(ctx, "strict-census orphan refusal", func() bool {
		latched, _ := gate.Latched()
		return latched
	}); err != nil {
		t.Fatalf("%v", err)
	}
	_, reason := gate.Latched()
	if !strings.Contains(reason, "owner census") || !strings.Contains(reason, strconv.Itoa(grandchildPID)) {
		t.Fatalf("strict census did not refuse uniquely on the grandchild fd denial: %s", reason)
	}
	if strings.Contains(reason, "no longer owned solely") {
		t.Fatalf("census refused on the visible double-owner path instead of the authentic unreadable orphan: %s", reason)
	}
	if clean, report := gate.CleanRetired(); clean {
		t.Fatalf("orphan transfer produced a clean retirement: %s", report)
	}
	if err := originGateWaitFor(ctx, "orphan refusal drain", func() bool {
		return gate.DrainState().Verified || gate.DrainState().Unknown
	}); err != nil {
		t.Fatalf("%v", err)
	}
	observation, err := pgx.Connect(ctx, fx.dsn)
	if err != nil {
		t.Fatalf("connect orphan observation session: %v", err)
	}
	defer observation.Close(context.Background())
	var rows int
	if err := observation.QueryRow(ctx, `SELECT count(*) FROM `+pgx.Identifier{table}.Sanitize()+` WHERE ref=$1`, ref).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("pre-orphan completed write changed: rows=%d err=%v", rows, err)
	}
	detail = fmt.Sprintf("grandchild PID %d reparented with dumpable=0 holding the live socket; intermediate reaped by the original owner; strict census latched (%s), no clean retirement, completed write retained", grandchildPID, reason)
}

// TestDrillOriginGateCensusStartChangeAfterOwnershipScanRefused proves the
// captured admitted start identity is re-verified after the FD/owner scan, via
// the deterministic seam, and that a change after the scan latches.
func TestDrillOriginGateCensusStartChangeAfterOwnershipScanRefused(t *testing.T) {
	detail := "start identity change after the ownership scan must refuse"
	recordOriginGateResult(t, "TestDrillOriginGateCensusStartChangeAfterOwnershipScanRefused", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	gate := newOriginGate(t, ctx, fx)
	var armed atomic.Bool
	var calls atomic.Int32
	gate.seam = &originGateInspectionSeam{
		processStart: func(pid int) (uint64, error) {
			if !armed.Load() {
				return hostProcessStartID(pid)
			}
			if calls.Add(1) >= 3 {
				return 999999999, nil
			}
			return hostProcessStartID(pid)
		},
	}
	launcher := newOriginGateLauncher(t)
	ref := fmt.Sprintf("origin-gate-scan-window-%d", time.Now().UnixNano())
	client, session, observation := retirementFixtureSession(t, ctx, gate, launcher, ref)
	defer observation.Close(context.Background())
	armed.Store(true)
	if err := originGateWaitFor(ctx, "post-ownership-scan start change latch", func() bool {
		latched, _ := gate.Latched()
		return latched
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, reason := gate.Latched(); !strings.Contains(reason, "after ownership scan") {
		t.Fatalf("start change was not refused after the ownership scan: %s", reason)
	}
	if clean, _ := gate.CleanRetired(); clean {
		t.Fatalf("post-scan start change produced a clean retirement")
	}
	_ = session
	_ = client
	detail = fmt.Sprintf("deterministic start change at the post-ownership-scan bracket latched (%s) with no clean retirement", reasonOf(gate))
}

// TestDrillOriginGateCensusLatchDuringPublicationNotClean proves a latch that
// wins between the authoritative drain observation and clean-verdict
// publication cannot publish clean evidence.
func TestDrillOriginGateCensusLatchDuringPublicationNotClean(t *testing.T) {
	detail := "latch racing clean-verdict publication must not publish a clean retirement"
	recordOriginGateResult(t, "TestDrillOriginGateCensusLatchDuringPublicationNotClean", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	ref := fmt.Sprintf("origin-gate-publish-race-%d", time.Now().UnixNano())
	gate := newOriginGate(t, ctx, fx)
	barrier := make(chan struct{})
	gate.publishBarrier = barrier
	launcher := newOriginGateLauncher(t)
	client, session, observation := retirementFixtureSession(t, ctx, gate, launcher, ref)
	defer observation.Close(context.Background())
	if err := client.sendLine("quit"); err != nil {
		t.Fatalf("request client terminate: %v", err)
	}
	if err := originGateWaitFor(ctx, "verified drain before clean publication", func() bool {
		return gate.DrainState().Verified
	}); err != nil {
		t.Fatalf("%v", err)
	}
	gate.latch("deterministic publication race")
	close(barrier)
	select {
	case <-gate.retirementPublishDone():
	case <-time.After(5 * time.Second):
		t.Fatalf("clean-publication attempt did not reach its completion signal")
	}
	if clean, report := gate.CleanRetired(); clean {
		t.Fatalf("clean retirement was published although the latch won: %s", report)
	}
	if latched, _ := gate.Latched(); !latched {
		t.Fatalf("gate did not remain latched after the publication race")
	}
	if err := originGateWaitFor(ctx, "latched drain settled after the publication race", func() bool {
		return !gate.DrainState().Pending
	}); err != nil {
		t.Fatalf("%v", err)
	}
	_ = session
	_ = client
	detail = fmt.Sprintf("latch won between the verified drain and publication: latched=%s, CleanRetired=false, no contradictory evidence", reasonOf(gate))
}

// TestDrillOriginGateCensusSeamPermissionRefuses proves deterministic
// permission/IO seam errors on status/start facts refuse instead of being read
// as vanished or ignored.
func TestDrillOriginGateCensusSeamPermissionRefuses(t *testing.T) {
	detail := "deterministic permission/IO seam errors on start and owner facts must refuse"
	recordOriginGateResult(t, "TestDrillOriginGateCensusSeamPermissionRefuses", &detail)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	fx := newOriginGateFixture(t)
	launcher := newOriginGateLauncher(t)

	permissionGate := newOriginGate(t, ctx, fx)
	var permissionArmed atomic.Bool
	permissionGate.seam = &originGateInspectionSeam{
		processStart: func(pid int) (uint64, error) {
			if permissionArmed.Load() {
				return 0, os.ErrPermission
			}
			return hostProcessStartID(pid)
		},
	}
	permissionRef := fmt.Sprintf("origin-gate-seam-perm-%d", time.Now().UnixNano())
	permissionClient, permissionSession, permissionObservation := retirementFixtureSession(t, ctx, permissionGate, launcher, permissionRef)
	defer permissionObservation.Close(context.Background())
	permissionArmed.Store(true)
	if err := originGateWaitFor(ctx, "permission seam refusal", func() bool {
		latched, _ := permissionGate.Latched()
		return latched
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, reason := permissionGate.Latched(); !strings.Contains(reason, "identity unavailable") && !strings.Contains(reason, "start identity") {
		t.Fatalf("permission error was not refused as an identity fact: %s", reason)
	}
	if clean, _ := permissionGate.CleanRetired(); clean {
		t.Fatalf("permission error produced a clean retirement")
	}

	ioGate := newOriginGate(t, ctx, fx)
	var ioArmed atomic.Bool
	ioGate.seam = &originGateInspectionSeam{
		socketOwners: func(inode string) ([]int, error) {
			if ioArmed.Load() {
				return nil, fmt.Errorf("owner census incomplete: deterministic IO error")
			}
			return hostSocketInodeOwners(inode)
		},
	}
	ioRef := fmt.Sprintf("origin-gate-seam-io-%d", time.Now().UnixNano())
	ioClient, ioSession, ioObservation := retirementFixtureSession(t, ctx, ioGate, launcher, ioRef)
	defer ioObservation.Close(context.Background())
	ioArmed.Store(true)
	if err := originGateWaitFor(ctx, "IO seam refusal", func() bool {
		latched, _ := ioGate.Latched()
		return latched
	}); err != nil {
		t.Fatalf("%v", err)
	}
	if _, reason := ioGate.Latched(); !strings.Contains(reason, "owner census failed") {
		t.Fatalf("IO error was not refused as an incomplete census: %s", reason)
	}
	if clean, _ := ioGate.CleanRetired(); clean {
		t.Fatalf("IO error produced a clean retirement")
	}
	_ = permissionClient
	_ = permissionSession
	_ = ioClient
	_ = ioSession
	detail = fmt.Sprintf("permission error latched (%s) and IO error latched (%s); neither produced clean evidence and neither was treated as vanished", reasonOf(permissionGate), reasonOf(ioGate))
}

func readCensusPPid(t *testing.T, pid int) int {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		t.Fatalf("read grandchild status: %v", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "PPid:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				value, err := strconv.Atoi(fields[1])
				if err != nil {
					t.Fatalf("parse PPid: %v", err)
				}
				return value
			}
		}
	}
	t.Fatalf("grandchild status has no PPid line")
	return -1
}

// TestDrillOriginGateCensusReadOrderIncarnationChangeRefused proves the
// per-process read order: the start identity is captured before the fd table
// and recaptured after; an incarnation change between the reads is UNKNOWN,
// while ENOENT is the only authoritative vanished outcome.
func TestDrillOriginGateCensusReadOrderIncarnationChangeRefused(t *testing.T) {
	detail := "start captured before fd enumeration and recaptured after; an incarnation change between the reads must be UNKNOWN"
	recordOriginGateResult(t, "TestDrillOriginGateCensusReadOrderIncarnationChangeRefused", &detail)
	originalStartReader, originalDirReader := hostCensusStartReader, hostCensusDirReader
	defer func() { hostCensusStartReader, hostCensusDirReader = originalStartReader, originalDirReader }()
	calls := 0
	hostCensusStartReader = func(pid int) (uint64, error) {
		calls++
		if calls == 1 {
			return 111, nil
		}
		return 222, nil
	}
	hostCensusDirReader = func(name string) ([]os.DirEntry, error) { return nil, nil }
	if _, err := hostSocketInodeOwners("4242"); err == nil || !strings.Contains(err.Error(), "start identity changed") {
		t.Fatalf("incarnation change between fd enumeration and the after-scan recapture was not UNKNOWN: %v", err)
	}
	calls = 0
	hostCensusStartReader = func(pid int) (uint64, error) { return 0, os.ErrNotExist }
	owners, err := hostSocketInodeOwners("4242")
	if err != nil || len(owners) != 0 {
		t.Fatalf("ENOENT was not the only authoritative vanished outcome: owners=%v err=%v", owners, err)
	}
	detail = "read-order seam: old-start/new-start around one fd enumeration yielded UNKNOWN; ENOENT alone reconciled to vanished"
}
