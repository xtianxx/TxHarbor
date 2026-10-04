//go:build linux && drill

// targetwriter_receipthandoff_drillbridge_linux_test.go is the STEP1 opaque
// receipt -> retained-controller handoff bridge. It is drill-only and adds no
// production API.
//
// The handoff wraps the ACTUAL factory-created borrowed run and the frozen
// private process receipt produced by that exact coordinator invocation. The
// original control anchor is captured BEFORE the run; after the entire Run
// returned (sole wait, drain and health monitors finished) the private receipt
// is consumed exactly once internally, cross-checked against the retained
// observation and the immutable factory binding, and only a frozen
// success/zero-exit/drained child whose original Linux PID/start identity is
// strictly gone (or PID-reused) yields a token. A frozen ambiguous or
// canceled/failed disposition yields NO token even if a late wait would have
// exited zero. The token can only recheck the retained anchor (same control
// lock connection/session/namespace and SQL incarnation binding) and be
// consumed once for a later auth step; it never grants clean/DDL/acceptance
// authority and it never claims the external gate's server-backend drain.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	receiptHandoffRecheckBound = 2 * time.Second
	// DrillReceiptHandoffGateRetirementStage is the fixed, non-clean statement
	// that the external protected origin gate's native server-backend drain
	// remains a separate requirement. The receipt bridge never claims it.
	DrillReceiptHandoffGateRetirementStage = "gate-retirement-required"
)

// drillReceiptHandoffInvariant is the immutable retained identity copy of one
// qualified handoff. It contains no conn, cmd, process or caller scalar.
type drillReceiptHandoffInvariant struct {
	originalTargetKey TargetKey
	roleFingerprint   string
	operationID       string
	instanceID        string
	controlTargetKey  TargetKey
	writerRoleOID     uint32
	childPID          int
	childStartID      uint64
}

// drillReceiptHandoffState is the shared private state of one handoff. Copies
// share this pointer, so permanent invalidation and the one-time consumption
// capability are shared. No exported field, no JSON representation.
type drillReceiptHandoffState struct {
	mu            sync.Mutex
	invalidated   bool
	invalidReason string
	consumed      bool
	// stage is the optional unexported test-only notification seam invoked
	// after a successful retained anchor recheck and before the shared-state
	// publication mutex in Recheck and ConsumeForAuth. It declares nothing.
	stage func(stage string)

	run     *DrillBorrowedWriterRun
	anchor  DrillControlAnchor
	receipt *targetProcessReceipt
	pfs     drillReceiptHandoffInvariant
}

// OpaqueDrillReceiptHandoff is the opaque retained-controller token. The zero
// value and any JSON round trip are inert; there is no constructor from
// receipt facts, PIDs, booleans or projections.
type OpaqueDrillReceiptHandoff struct {
	state *drillReceiptHandoffState
}

// DrillReceiptHandoffFacts is the read-only diagnostic projection. It cannot
// reconstruct a token.
type DrillReceiptHandoffFacts struct {
	Present         bool
	Invalidated     bool
	InvalidReason   string
	Consumed        bool
	OriginalKey     TargetKey
	ControlKey      TargetKey
	RoleFingerprint string
	OperationID     string
	InstanceID      string
	WriterRoleOID   uint32
	ChildPID        int
	ChildStartID    uint64
}

// RunForReceiptHandoff captures the original control anchor BEFORE the actual
// coordinator run, executes the real r.Run (the concrete coordinator; never an
// opts wrapper or another restore), and returns the frozen result plus the
// opaque handoff token when the frozen receipt qualifies. The original
// coordinator error is returned unchanged (a deliberate Probe rejection is
// never converted to nil success).
func (r *DrillBorrowedWriterRun) RunForReceiptHandoff(ctx context.Context) (TargetWriterResult, OpaqueDrillReceiptHandoff, error) {
	if r == nil || r.life == nil {
		return TargetWriterResult{}, OpaqueDrillReceiptHandoff{}, errors.New("receipt handoff requires the factory-created borrowed run")
	}
	if ctx == nil {
		return TargetWriterResult{}, OpaqueDrillReceiptHandoff{}, errors.New("receipt handoff requires a bounded context")
	}
	anchor, err := r.CaptureControlAnchor(ctx)
	if err != nil {
		return TargetWriterResult{}, OpaqueDrillReceiptHandoff{}, err
	}
	if err := ctx.Err(); err != nil {
		return TargetWriterResult{}, OpaqueDrillReceiptHandoff{}, err
	}
	result, _, runErr := r.Run(ctx)
	if ctxErr := ctx.Err(); ctxErr != nil {
		// A context that ended after the run began must never surface as a
		// silent nil error when no token could be produced: the coordinator
		// error stays authoritative when present, otherwise the context error
		// is returned.
		return result, OpaqueDrillReceiptHandoff{}, receiptHandoffAfterRunError(ctxErr, runErr)
	}
	forged, qerr := r.buildReceiptHandoff(ctx, anchor, result)
	if qerr != nil {
		if runErr != nil {
			// The original coordinator error is authoritative; the handoff is
			// simply absent.
			return result, OpaqueDrillReceiptHandoff{}, runErr
		}
		return result, OpaqueDrillReceiptHandoff{}, qerr
	}
	return result, forged, runErr
}

// receiptHandoffAfterRunError is the pure post-run error decision: the original
// coordinator error is authoritative when present, otherwise the ended context
// error is returned instead of a silent nil.
func receiptHandoffAfterRunError(ctxErr, runErr error) error {
	if runErr != nil {
		return runErr
	}
	return ctxErr
}

// buildReceiptHandoff consumes the frozen private receipt exactly once and
// qualifies the whole identity/terminal chain. Any missing, unreadable or
// mismatched fact refuses without a token.
func (r *DrillBorrowedWriterRun) buildReceiptHandoff(ctx context.Context, anchor DrillControlAnchor, result TargetWriterResult) (OpaqueDrillReceiptHandoff, error) {
	receipt := result.processReceipt
	if receipt == nil {
		return OpaqueDrillReceiptHandoff{}, errors.New("no frozen process receipt was produced")
	}
	receipt.mu.Lock()
	if receipt.consumed {
		receipt.mu.Unlock()
		return OpaqueDrillReceiptHandoff{}, errors.New("frozen process receipt was already consumed")
	}
	// Consume the private receipt once, internally: the external receipt
	// projection can no longer be consumed.
	receipt.consumed = true
	snapshot := receipt.snapshot
	receiptKey := receipt.targetKey
	receiptRole := receipt.roleFingerprint
	receiptOperation := receipt.operationID
	receipt.mu.Unlock()

	binding := r.binding
	if binding.originalTargetKey == (TargetKey{}) || receiptKey != binding.originalTargetKey {
		return OpaqueDrillReceiptHandoff{}, errors.New("frozen receipt target key does not match the immutable original binding")
	}
	if receiptRole != binding.originalRoleFingerprint || receiptOperation != binding.originalOperationID {
		return OpaqueDrillReceiptHandoff{}, errors.New("frozen receipt role/operation does not match the immutable original binding")
	}
	if binding.controlTargetKey == (TargetKey{}) || binding.writerRoleOID == 0 {
		return OpaqueDrillReceiptHandoff{}, errors.New("immutable control namespace/role OID binding is incomplete")
	}
	if !snapshot.started || !snapshot.terminal || !snapshot.childWaitCompleted || snapshot.waitErr != nil {
		return OpaqueDrillReceiptHandoff{}, errors.New("frozen receipt has no successful terminal sole wait")
	}
	if !snapshot.runnerReturned || snapshot.result.Outcome != PGCommandSucceeded ||
		snapshot.result.ExitCode != 0 || !snapshot.result.ProcessGroupDrained {
		return OpaqueDrillReceiptHandoff{}, errors.New("frozen command disposition is not success/zero/drained")
	}
	if r.observed == nil {
		return OpaqueDrillReceiptHandoff{}, errors.New("retained observation is absent")
	}
	observed := r.observed.snapshot()
	if observed.pid != snapshot.pid || observed.startID != snapshot.startID || observed.pid <= 0 || observed.startID == 0 {
		return OpaqueDrillReceiptHandoff{}, errors.New("frozen receipt does not match the retained observation identity")
	}
	if err := receiptHandoffProcessReaped(snapshot.pid, snapshot.startID); err != nil {
		return OpaqueDrillReceiptHandoff{}, err
	}
	facts := anchor.Diagnostics()
	if !facts.Present || facts.Invalidated ||
		facts.OriginalTargetKey != binding.originalTargetKey ||
		facts.ControlTargetKey != binding.controlTargetKey ||
		facts.OriginalRoleFingerprint != binding.originalRoleFingerprint ||
		facts.OriginalOperationID != binding.originalOperationID ||
		facts.OriginalInstanceID != binding.originalInstanceID {
		return OpaqueDrillReceiptHandoff{}, errors.New("original control anchor does not match the immutable binding")
	}
	recheckCtx, cancelRecheck := context.WithTimeout(ctx, receiptHandoffRecheckBound)
	err := anchor.Recheck(recheckCtx)
	cancelRecheck()
	if err != nil {
		return OpaqueDrillReceiptHandoff{}, errors.New("bounded original anchor recheck refused")
	}
	if err := ctx.Err(); err != nil {
		return OpaqueDrillReceiptHandoff{}, err
	}
	state := &drillReceiptHandoffState{
		run: r, anchor: anchor, receipt: receipt,
		pfs: drillReceiptHandoffInvariant{
			originalTargetKey: binding.originalTargetKey,
			roleFingerprint:   binding.originalRoleFingerprint,
			operationID:       binding.originalOperationID,
			instanceID:        binding.originalInstanceID,
			controlTargetKey:  binding.controlTargetKey,
			writerRoleOID:     binding.writerRoleOID,
			childPID:          snapshot.pid,
			childStartID:      snapshot.startID,
		},
	}
	// Publication linearizes with invalidation/loss under the shared mutex;
	// no I/O is performed while holding it.
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := ctx.Err(); err != nil {
		state.invalidated = true
		state.invalidReason = "context ended before handoff publication"
		return OpaqueDrillReceiptHandoff{}, err
	}
	return OpaqueDrillReceiptHandoff{state: state}, nil
}

// receiptHandoffProcessStage is the strict child-identity stage.
type receiptHandoffProcessStage int

const (
	receiptHandoffStageUnknown receiptHandoffProcessStage = iota
	receiptHandoffStageLive
	receiptHandoffStageZombie
	receiptHandoffStageGone
)

// parseReceiptHandoffProcessStat parses one /proc/<pid>/stat document with the
// expected PID: the leading integer must equal the expected PID, the command
// field must be structurally delimited by the first '(' and the LAST ')' (the
// comm may contain spaces and parentheses), the field list must be sufficient,
// the state must be one of the kernel-known single-character states and the
// start field must be a valid positive numeric identity. Any deviation is an
// UNKNOWN refusal, never evidence of disappearance.
func parseReceiptHandoffProcessStat(expectedPID int, raw []byte) (string, uint64, error) {
	if expectedPID <= 0 {
		return "", 0, errors.New("expected process PID is invalid (unknown)")
	}
	text := string(raw)
	open := strings.IndexByte(text, '(')
	if open <= 0 || open > 20 {
		return "", 0, errors.New("stat command delimiter is not recognized (unknown)")
	}
	parsedPID, err := strconv.Atoi(strings.TrimSpace(text[:open]))
	if err != nil || parsedPID != expectedPID {
		return "", 0, errors.New("stat leading PID does not match the expected process (unknown)")
	}
	last := strings.LastIndexByte(text, ')')
	if last < open {
		return "", 0, errors.New("stat command delimiter is malformed (unknown)")
	}
	fields := strings.Fields(text[last+1:])
	if len(fields) <= 19 {
		return "", 0, errors.New("stat fields are truncated (unknown)")
	}
	state := fields[0]
	if len(state) != 1 || !strings.ContainsRune("RSDTZtXxKWPI", rune(state[0])) {
		return "", 0, errors.New("stat state is not a recognized Linux state (unknown)")
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || start == 0 {
		return "", 0, errors.New("stat start field is not a valid positive identity (unknown)")
	}
	return state, start, nil
}

func defaultReceiptHandoffReadStat(pid int) ([]byte, error) {
	return os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
}

// receiptHandoffProcessIdentityStage classifies the expected process with an
// injectable stat reader (nil selects the real /proc reader). ENOENT is the
// only disappearance; a structurally valid different numeric start means the
// original process is gone (PID reuse); a validated same PID/start in state Z
// means exited-but-not-reaped; a validated same live start means still running;
// any read/parse failure is UNKNOWN.
func receiptHandoffProcessIdentityStage(expectedPID int, expectedStart uint64, readStat func(int) ([]byte, error)) (receiptHandoffProcessStage, error) {
	if expectedPID <= 0 || expectedStart == 0 {
		return receiptHandoffStageUnknown, errors.New("expected process PID/start identity is missing")
	}
	if readStat == nil {
		readStat = defaultReceiptHandoffReadStat
	}
	raw, err := readStat(expectedPID)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return receiptHandoffStageGone, nil
		}
		return receiptHandoffStageUnknown, errors.New("original child identity is unreadable (unknown)")
	}
	state, start, err := parseReceiptHandoffProcessStat(expectedPID, raw)
	if err != nil {
		return receiptHandoffStageUnknown, err
	}
	// A validated PID with a valid different numeric start is the original
	// process gone (PID reuse), even when the new process is a zombie. Only
	// the SAME validated PID/start in state Z is the exited-but-not-reaped
	// case; the same start in any other recognized state is still Live.
	if start != expectedStart {
		return receiptHandoffStageGone, nil
	}
	if state == "Z" {
		return receiptHandoffStageZombie, nil
	}
	return receiptHandoffStageLive, nil
}

// receiptHandoffProcessReaped classifies the original child's Linux identity
// after the entire run returned. Only ENOENT or a validated different numeric
// start (PID reuse) means the original process is gone; a validated zombie or
// same live start means it was not reaped; any other read/parse error refuses.
func receiptHandoffProcessReaped(pid int, startID uint64) error {
	stage, err := receiptHandoffProcessIdentityStage(pid, startID, nil)
	if err != nil {
		return err
	}
	switch stage {
	case receiptHandoffStageZombie:
		return errors.New("original child is a zombie and was not reaped")
	case receiptHandoffStageLive:
		return errors.New("original child is still running with the same start identity")
	}
	return nil
}

// Recheck re-verifies the retained controller: the bounded original anchor
// recheck (same control lock connection/session/namespace and SQL incarnation
// binding, including control-owner health) and the caller's own context. Any
// failure permanently invalidates the shared state.
func (h *OpaqueDrillReceiptHandoff) Recheck(ctx context.Context) error {
	if h == nil || h.state == nil {
		return errors.New("receipt handoff is absent")
	}
	state := h.state
	if reason := state.invalidReasonNow(); reason != "" {
		return errors.New("receipt handoff is permanently invalidated")
	}
	if ctx == nil {
		// A nil recheck context is a caller fault and permanently invalidates
		// the shared state instead of returning a recoverable error.
		state.invalidate("recheck context is missing")
		return errors.New("receipt handoff recheck requires a bounded context")
	}
	bounded, cancel := context.WithTimeout(ctx, receiptHandoffRecheckBound)
	defer cancel()
	if err := state.anchor.Recheck(bounded); err != nil {
		state.invalidate("retained control anchor recheck refused")
		return errors.New("retained control anchor recheck refused")
	}
	if state.stage != nil {
		state.stage("recheck-publish")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.invalidated {
		return errors.New("receipt handoff was permanently invalidated concurrently")
	}
	if err := bounded.Err(); err != nil {
		state.invalidated = true
		state.invalidReason = "context ended before recheck publication"
		return errors.New("receipt handoff context ended before recheck publication")
	}
	return nil
}

// ConsumeForAuth consumes the one-time handoff capability for a later auth
// step. It requires a bounded context, re-runs the real retained control-anchor
// recheck (same control connection/session/namespace and SQL incarnation,
// including genuine owner health), and publishes the consumption under the
// shared state mutex with a final own-context check. Any failure permanently
// invalidates the shared state; it never accepts a callback boolean and grants
// no clean/DDL/acceptance authority.
func (h *OpaqueDrillReceiptHandoff) ConsumeForAuth(ctx context.Context) error {
	if h == nil || h.state == nil {
		return errors.New("receipt handoff is absent")
	}
	state := h.state
	if reason := state.invalidReasonNow(); reason != "" {
		return errors.New("receipt handoff is permanently invalidated")
	}
	if ctx == nil {
		state.invalidate("auth consumption context is missing")
		return errors.New("receipt handoff auth consumption requires a bounded context")
	}
	bounded, cancel := context.WithTimeout(ctx, receiptHandoffRecheckBound)
	defer cancel()
	if err := state.anchor.Recheck(bounded); err != nil {
		state.invalidate("retained control anchor recheck refused before auth consumption")
		return errors.New("retained control anchor recheck refused before auth consumption")
	}
	if state.stage != nil {
		state.stage("consume-publish")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.invalidated {
		return errors.New("receipt handoff was permanently invalidated concurrently")
	}
	if state.consumed {
		return errors.New("receipt handoff was already consumed for auth")
	}
	if err := bounded.Err(); err != nil {
		state.invalidated = true
		state.invalidReason = "context ended before auth consumption publication"
		return errors.New("receipt handoff context ended before auth consumption publication")
	}
	state.consumed = true
	return nil
}

// Valid reports the shared permanent invalidation state (copy-shared).
func (h *OpaqueDrillReceiptHandoff) Valid() (bool, string) {
	if h == nil || h.state == nil {
		return false, "absent"
	}
	reason := h.state.invalidReasonNow()
	return reason == "", reason
}

// Diagnostics is the read-only projection; it cannot reconstruct a token.
func (h *OpaqueDrillReceiptHandoff) Diagnostics() DrillReceiptHandoffFacts {
	if h == nil || h.state == nil {
		return DrillReceiptHandoffFacts{}
	}
	state := h.state
	state.mu.Lock()
	invalidated, reason, consumed := state.invalidated, state.invalidReason, state.consumed
	state.mu.Unlock()
	return DrillReceiptHandoffFacts{
		Present: true, Invalidated: invalidated, InvalidReason: reason, Consumed: consumed,
		OriginalKey: state.pfs.originalTargetKey, ControlKey: state.pfs.controlTargetKey,
		RoleFingerprint: state.pfs.roleFingerprint, OperationID: state.pfs.operationID,
		InstanceID: state.pfs.instanceID, WriterRoleOID: state.pfs.writerRoleOID,
		ChildPID: state.pfs.childPID, ChildStartID: state.pfs.childStartID,
	}
}

// GateRetirementStage states, as a fixed non-clean string, that the external
// protected origin gate's native server-backend drain remains a separate
// requirement; the receipt bridge retains no protected fixture and cannot
// claim that drain.
func (h *OpaqueDrillReceiptHandoff) GateRetirementStage() string {
	return DrillReceiptHandoffGateRetirementStage
}

func (s *drillReceiptHandoffState) invalidate(reason string) {
	s.mu.Lock()
	if !s.invalidated {
		s.invalidated = true
		s.invalidReason = reason
	}
	s.mu.Unlock()
}

func (s *drillReceiptHandoffState) invalidReasonNow() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.invalidated {
		return ""
	}
	return s.invalidReason
}
