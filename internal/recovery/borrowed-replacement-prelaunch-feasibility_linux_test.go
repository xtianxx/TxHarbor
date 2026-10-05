//go:build linux && drill

// borrowed-replacement-prelaunch-feasibility_linux_test.go is the bounded
// native-ready replacement attempt -> dirty-guard refusal lane. It starts from
// a GENUINE replacement-bound capture (the actual bounded
// borrowedReplacementBindingOrchestrate path with stage counters, never an
// OID projection) and retains that source capture as a real dependency of the
// prepared attempt: the shared source validity and the caller context are
// rechecked before the preparation is published, before the borrowed origin is
// registered and before the coordinator Run is dispatched, and an observed
// source loss permanently invalidates the prepared attempt. ONE prerequisite
// replacement session is exercised and retired (owner health retained, guard
// unresolved, zero added acceptance), the lane opens its OWN fresh archive
// reader over the real fixture archive at offset zero (descriptor-bound,
// offset-independent complete digest; the original coordinator reader is never
// rewound or reused), prepares a fresh native-ready attempt on a NEW gate
// (real control store, exact private P1 target DSN, observer, fresh archive,
// unique attempt identity, rejecting probe/acceptance, short caller-derived
// child contexts for store/factory/capture), registers its fresh prefix
// against THAT run through UseBorrowedOrigin, and then invokes the actual
// coordinator Run EXACTLY once for the refusal control. The deliberately dirty
// replacement guard must refuse the attempt with the guard-specific refusal
// before any child launch: no Started identity, no executable frames, no
// callbacks, the replacement catalog unchanged and the guard still blocking.
// Replay refusals, cancellation through the common preparation helper, source
// loss during/after preparation, wrong endpoint-target identity, an
// unavailable archive, and the partial/UNKNOWN/contaminated preparation stops
// are exercised pre-launch. There is no guard bypass, no restore, no probe
// execution, no receipt authority, no continuous exclusion, no clean
// transition, no acceptance, no manifest, no downstream and no Gate1
// authority; no admission waiter expecting a child is ever installed and no
// child is ever launched. Secrets, verifiers and DSNs are never logged.
package recovery_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// borrowedReplacementPrelaunch*Budgets are the explicit short caller-derived
// child budgets of the preparation steps; a sooner caller deadline still clips
// them.
const (
	borrowedReplacementPrelaunchStoreBudget   = 30 * time.Second
	borrowedReplacementPrelaunchFactoryBudget = 30 * time.Second
	borrowedReplacementPrelaunchCaptureBudget = 60 * time.Second
)

// borrowedReplacementPrelaunchArchiveLimit is the explicit supported archive
// size bound; a larger archive is refused explicitly and never silently
// truncated.
const borrowedReplacementPrelaunchArchiveLimit = 512 << 20

// borrowedReplacementPrelaunchGuardDisposition reads the real target guard
// disposition of the fixture's bound guard key.
func borrowedReplacementPrelaunchGuardDisposition(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture) string {
	t.Helper()
	var disposition string
	guardCtx, cancelGuard := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	guardErr := f.controlPool.QueryRow(guardCtx, `SELECT disposition FROM recovery_target_guard WHERE target_guard_key=$1`, f.guardKey).Scan(&disposition)
	cancelGuard()
	if guardErr != nil {
		t.Fatalf("replacement prelaunch guard disposition read refused: %v", guardErr)
	}
	return disposition
}

// borrowedReplacementPrelaunchArchiveDigest hashes the COMPLETE descriptor
// content with offset-independent reads (ReadAt): it never reopens the
// pathname, never moves the descriptor position and never truncates silently.
// A descriptor whose size exceeds the explicit limit is refused before any
// read.
func borrowedReplacementPrelaunchArchiveDigest(reader *os.File) (string, error) {
	if reader == nil {
		return "", errors.New("archive digest requires the concrete descriptor")
	}
	info, err := reader.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("archive digest descriptor is not a regular file")
	}
	if info.Size() > borrowedReplacementPrelaunchArchiveLimit {
		return "", errors.New("archive exceeds the supported size limit")
	}
	h := sha256.New()
	buffer := make([]byte, 1<<20)
	var offset int64
	for offset < info.Size() {
		chunk := int64(len(buffer))
		if remaining := info.Size() - offset; remaining < chunk {
			chunk = remaining
		}
		n, readErr := reader.ReadAt(buffer[:chunk], offset)
		if n > 0 {
			_, _ = h.Write(buffer[:n])
			offset += int64(n)
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) && offset == info.Size() {
				break
			}
			return "", errors.New("archive digest read refused")
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// borrowedReplacementPrelaunchArchiveReader independently reopens the real
// fixture archive as a NEW reader and verifies the descriptor identity (same
// regular file, same size via os.SameFile), the COMPLETE descriptor-bound
// content digest of the fixture and the fresh descriptors, and that the fresh
// reader still starts at offset zero. The original coordinator archive reader
// is never rewound or reused. It is error-returning and publishes no reader on
// refusal.
func borrowedReplacementPrelaunchArchiveReader(f *borrowedAuthHandoffFixture) (*os.File, error) {
	if f == nil || f.archive == nil {
		return nil, errors.New("replacement prelaunch archive requires the concrete fixture archive")
	}
	path := f.archive.Name()
	fixtureInfo, err := f.archive.Stat()
	if err != nil || !fixtureInfo.Mode().IsRegular() || fixtureInfo.Size() == 0 {
		return nil, errors.New("replacement prelaunch fixture archive identity refused")
	}
	reader, err := os.Open(path)
	if err != nil {
		return nil, errors.New("replacement prelaunch fresh archive open refused")
	}
	freshInfo, err := reader.Stat()
	if err != nil || !freshInfo.Mode().IsRegular() || !os.SameFile(fixtureInfo, freshInfo) || freshInfo.Size() != fixtureInfo.Size() {
		_ = reader.Close()
		return nil, errors.New("replacement prelaunch fresh archive is not the fixture archive file")
	}
	originalDigest, err := borrowedReplacementPrelaunchArchiveDigest(f.archive)
	if err != nil {
		_ = reader.Close()
		return nil, errors.New("replacement prelaunch fixture archive digest refused")
	}
	freshDigest, err := borrowedReplacementPrelaunchArchiveDigest(reader)
	if err != nil {
		_ = reader.Close()
		return nil, errors.New("replacement prelaunch fresh archive digest refused")
	}
	if originalDigest != freshDigest {
		_ = reader.Close()
		return nil, errors.New("replacement prelaunch fresh archive content differs from the fixture archive")
	}
	if offset, err := reader.Seek(0, io.SeekCurrent); err != nil || offset != 0 {
		_ = reader.Close()
		return nil, errors.New("replacement prelaunch fresh archive does not start at offset zero")
	}
	return reader, nil
}

// borrowedReplacementPrelaunchArchive is the fatal wrapper around the actual
// reader helper for positive preparation.
func borrowedReplacementPrelaunchArchive(t *testing.T, f *borrowedAuthHandoffFixture) *os.File {
	t.Helper()
	reader, err := borrowedReplacementPrelaunchArchiveReader(f)
	if err != nil {
		t.Fatalf("replacement prelaunch archive reader refused: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	return reader
}

// borrowedReplacementPrelaunchArchiveControls proves the archive verification
// is descriptor-bound and offset-independent: a pathname substitution is
// refused by the actual reader helper, an in-place content change is tracked
// descriptor-bound by the retained descriptor (never a pathname cache),
// beyond-limit content is refused explicitly, and the retained descriptor
// stays at offset zero.
func borrowedReplacementPrelaunchArchiveControls(t *testing.T, f *borrowedAuthHandoffFixture) {
	t.Helper()
	retained := borrowedReplacementPrelaunchArchive(t, f)
	retainedDigest, err := borrowedReplacementPrelaunchArchiveDigest(retained)
	if err != nil || retainedDigest == "" {
		t.Fatalf("retained archive descriptor digest refused: %v", err)
	}
	path := f.archive.Name()
	backup := path + ".prelaunch-backup"
	if err := os.Rename(path, backup); err != nil {
		t.Fatalf("pathname substitution backup rename refused: %v", err)
	}
	restored := false
	t.Cleanup(func() {
		if restored {
			return
		}
		_ = os.Remove(path)
		_ = os.Rename(backup, path)
	})
	if err := os.WriteFile(path, []byte("borrowed-replacement-prelaunch-substitute-content"), 0o600); err != nil {
		t.Fatalf("pathname substitution write refused: %v", err)
	}
	if substituted, subErr := borrowedReplacementPrelaunchArchiveReader(f); subErr == nil {
		_ = substituted.Close()
		t.Fatal("pathname-substituted archive content was accepted by the reader helper")
	}
	retainedAfter, err := borrowedReplacementPrelaunchArchiveDigest(retained)
	if err != nil || retainedAfter != retainedDigest {
		t.Fatalf("retained archive descriptor changed after the pathname substitution: err=%v", err)
	}
	if offset, err := retained.Seek(0, io.SeekCurrent); err != nil || offset != 0 {
		t.Fatalf("retained archive descriptor position moved: offset=%d err=%v", offset, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("pathname substitution cleanup refused: %v", err)
	}
	if err := os.Rename(backup, path); err != nil {
		t.Fatalf("pathname substitution restore refused: %v", err)
	}
	restored = true
	originalContent, err := os.ReadFile(path)
	if err != nil || len(originalContent) == 0 {
		t.Fatalf("archive content reference refused: err=%v", err)
	}
	changed := append([]byte(nil), originalContent...)
	for i := range changed {
		changed[i] ^= 0x5a
	}
	if err := os.WriteFile(path, changed, 0o600); err != nil {
		t.Fatalf("content-change write refused: %v", err)
	}
	contentRestored := false
	t.Cleanup(func() {
		if contentRestored {
			return
		}
		_ = os.WriteFile(path, originalContent, 0o600)
	})
	// In-place content change (same inode): the descriptor-bound digest tracks
	// the LIVE content of the retained descriptor (never a pathname cache) and
	// a fresh reader of the same file still verifies consistently.
	changedDigest, err := borrowedReplacementPrelaunchArchiveDigest(retained)
	if err != nil || changedDigest == retainedDigest {
		t.Fatalf("in-place content change was not reflected by the descriptor-bound digest: err=%v", err)
	}
	if sameFileReader, sameErr := borrowedReplacementPrelaunchArchiveReader(f); sameErr != nil {
		t.Fatalf("in-place content change of the same file was refused by the reader helper: %v", sameErr)
	} else {
		_ = sameFileReader.Close()
	}
	if err := os.WriteFile(path, originalContent, 0o600); err != nil {
		t.Fatalf("content-change restore refused: %v", err)
	}
	contentRestored = true
	restoredDigest, err := borrowedReplacementPrelaunchArchiveDigest(retained)
	if err != nil || restoredDigest != retainedDigest {
		t.Fatalf("restored content was not reflected by the descriptor-bound digest: err=%v", err)
	}
	restoredReader := borrowedReplacementPrelaunchArchive(t, f)
	_ = restoredReader.Close()
	oversized, err := os.CreateTemp(t.TempDir(), "prelaunch-oversize")
	if err != nil {
		t.Fatalf("oversize archive fixture refused: %v", err)
	}
	t.Cleanup(func() { _ = oversized.Close() })
	if err := oversized.Truncate(borrowedReplacementPrelaunchArchiveLimit + 1); err != nil {
		t.Fatalf("oversize archive truncate refused: %v", err)
	}
	if _, overErr := borrowedReplacementPrelaunchArchiveDigest(oversized); overErr == nil ||
		!strings.Contains(overErr.Error(), "exceeds the supported size limit") {
		t.Fatalf("beyond-limit archive content was not refused explicitly: %v", overErr)
	}
	if offset, err := oversized.Seek(0, io.SeekCurrent); err != nil || offset != 0 {
		t.Fatalf("oversize descriptor position moved: offset=%d err=%v", offset, err)
	}
	t.Logf("archive controls: pathname-substituted content was refused by the actual reader helper, an in-place content change was tracked descriptor-bound by the retained descriptor, beyond-limit content was refused explicitly, and the retained descriptor stayed at offset zero")
}

// borrowedReplacementPrelaunchProvenanceCheck requires the genuine shared
// replacement capture provenance: the SHARED replacement-prefix state must not
// be permanently invalidated and the capture must be replacement-bound before
// any native-ready attempt is prepared. It is error-returning and performs no
// I/O.
func borrowedReplacementPrelaunchProvenanceCheck(fresh *borrowedReplacementBinding) error {
	if fresh == nil || fresh.prefix == nil || fresh.prefix.state == nil {
		return errors.New("replacement prelaunch provenance requires the concrete replacement capture")
	}
	if reason := fresh.prefix.state.invalidReason(); reason != "" {
		return errors.New("replacement prelaunch provenance refused: the shared replacement prefix is permanently invalidated")
	}
	if fresh.replacementOID == 0 || fresh.replacementOID == fresh.oldOID || fresh.binding.TargetDatabaseOID() != fresh.replacementOID {
		return errors.New("replacement prelaunch provenance refused: the capture is not replacement-bound")
	}
	return nil
}

// borrowedReplacementPrelaunchPreparationHook is the test-only mid-preparation
// seam consulted after the prefix capture and before the preparation is
// published. It is read atomically and is nil in normal runs.
var borrowedReplacementPrelaunchPreparationHook atomic.Pointer[func()]

func borrowedReplacementPrelaunchPauseAtPreparation() {
	if hook := borrowedReplacementPrelaunchPreparationHook.Load(); hook != nil {
		(*hook)()
	}
}

// borrowedReplacementPrelaunchPreparationSeam is the test-only blocking
// mid-preparation barrier; it is released exactly once at cleanup so a fatal
// can never strand the preparation goroutine.
type borrowedReplacementPrelaunchPreparationSeam struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func installBorrowedReplacementPrelaunchPreparationSeam(t *testing.T) *borrowedReplacementPrelaunchPreparationSeam {
	t.Helper()
	seam := &borrowedReplacementPrelaunchPreparationSeam{entered: make(chan struct{}), release: make(chan struct{})}
	hook := func() {
		seam.enteredOnce.Do(func() { close(seam.entered) })
		<-seam.release
	}
	borrowedReplacementPrelaunchPreparationHook.Store(&hook)
	t.Cleanup(func() {
		borrowedReplacementPrelaunchPreparationHook.Store(nil)
		seam.releaseSeam()
	})
	return seam
}

func (s *borrowedReplacementPrelaunchPreparationSeam) releaseSeam() {
	s.releaseOnce.Do(func() { close(s.release) })
}

// borrowedReplacementPrelaunchAttemptState is the copy-shared permanent
// invalidation state of one prepared attempt: copies share this pointer, so an
// observed source loss permanently invalidates every copy.
type borrowedReplacementPrelaunchAttemptState struct {
	mu            sync.Mutex
	invalidated   bool
	invalidReason string
}

func (s *borrowedReplacementPrelaunchAttemptState) invalidate(reason string) {
	s.mu.Lock()
	if !s.invalidated {
		s.invalidated = true
		s.invalidReason = reason
	}
	s.mu.Unlock()
}

func (s *borrowedReplacementPrelaunchAttemptState) invalidReasonNow() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.invalidated {
		return ""
	}
	return s.invalidReason
}

// borrowedReplacementPrelaunchAttempt is one prepared native-ready replacement
// attempt: the NEW gate, the actual factory run, the fresh prefix captured
// against THAT run and the RETAINED genuine source capture it depends on.
type borrowedReplacementPrelaunchAttempt struct {
	gate   *originGate
	run    *recovery.DrillBorrowedWriterRun
	prefix *borrowedIdentityPrefix
	source *borrowedReplacementBinding
	state  *borrowedReplacementPrelaunchAttemptState
}

// verifySourceAndContext rechecks the caller context and the retained genuine
// source capture validity with no I/O. An observed source loss (or caller
// context end) permanently invalidates the prepared attempt and refuses.
func (a *borrowedReplacementPrelaunchAttempt) verifySourceAndContext(ctx context.Context) error {
	if a == nil || a.state == nil || a.source == nil || a.source.prefix == nil || a.source.prefix.state == nil {
		return errors.New("replacement prelaunch attempt requires the retained genuine source capture")
	}
	if a.state.invalidReasonNow() != "" {
		return errors.New("replacement prelaunch attempt is permanently invalidated")
	}
	if ctx == nil {
		a.state.invalidate("replacement prelaunch attempt caller context is missing")
		return errors.New("replacement prelaunch attempt requires a bounded caller context")
	}
	if err := ctx.Err(); err != nil {
		a.state.invalidate("replacement prelaunch attempt caller context ended before publication")
		return errors.New("replacement prelaunch attempt caller context ended before publication")
	}
	if reason := a.source.prefix.state.invalidReason(); reason != "" {
		a.state.invalidate("replacement prelaunch attempt observed genuine source capture loss: " + reason)
		return errors.New("replacement prelaunch attempt genuine source capture was permanently invalidated")
	}
	return nil
}

// newBorrowedReplacementPrelaunchAttempt prepares the fresh attempt: the shared
// replacement provenance check, a NEW gate on the owned fixture, the real
// control store on an explicit short child budget, the complete fixture
// options with the exact private P1 target DSN, the observer, the supplied
// archive, a unique attempt identity and rejecting probe/acceptance callbacks
// on an explicit short factory child budget, a fresh prefix captured on an
// explicit short child budget against THAT run, and the final source/context
// recheck before the preparation is published. The borrowed origin is
// registered separately so pre-launch refusals can be exercised before
// registration. Every meaningful refusal is error-returning and publishes no
// attempt.
// borrowedReplacementPrelaunchBoundOptions carries the BOUND-only attempt
// plumbing: the authentic instance id and the transactional
// restore_started marker hook.
type borrowedReplacementPrelaunchBoundOptions struct {
	InstanceID string
	Prelaunch  func(context.Context, pgx.Tx, controlstore.InstanceToken) (recovery.EvidenceToken, error)
}

func newBorrowedReplacementPrelaunchAttempt(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, archive io.Reader, attemptID string, probeCalls, acceptanceCalls *int32) (*borrowedReplacementPrelaunchAttempt, error) {
	t.Helper()
	return newBorrowedReplacementPrelaunchAttemptWith(t, ctx, b, fresh, archive, attemptID, probeCalls, acceptanceCalls, nil)
}

func newBorrowedReplacementPrelaunchAttemptWith(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, archive io.Reader, attemptID string, probeCalls, acceptanceCalls *int32, bound *borrowedReplacementPrelaunchBoundOptions) (*borrowedReplacementPrelaunchAttempt, error) {
	t.Helper()
	if b == nil || b.fixture == nil || fresh == nil || attemptID == "" {
		return nil, errors.New("replacement prelaunch attempt requires the concrete baseline, fresh capture and attempt identity")
	}
	if err := borrowedReplacementPrelaunchProvenanceCheck(fresh); err != nil {
		return nil, err
	}
	f := b.fixture
	p1TargetDSN := borrowedReplacementP1TargetDSN(t, b)
	targetTarget, err := controlstore.ParseDSNTarget(p1TargetDSN)
	if err != nil {
		return nil, errors.New("replacement prelaunch trusted target identity refused")
	}
	gate := newOriginGate(t, ctx, f.fx)
	storeCtx, cancelStore := context.WithTimeout(ctx, borrowedReplacementPrelaunchStoreBudget)
	store, err := controlstore.NewStore(storeCtx, f.controlPool)
	cancelStore()
	if err != nil {
		return nil, errors.New("replacement prelaunch control store refused")
	}
	factoryCtx, cancelFactory := context.WithTimeout(ctx, borrowedReplacementPrelaunchFactoryBudget)
	if bound != nil && (bound.InstanceID == "" || bound.Prelaunch == nil) {
		cancelFactory()
		return nil, errors.New("replacement prelaunch bound options require an instance id and a transactional marker hook")
	}
	writerOptions := recovery.TargetWriterOptions{
		OperationKind: recovery.TargetWriterOperationRestore,
		Store:         store,
		ControlDSN:    f.controlDSN,
		TargetDSN:     p1TargetDSN,
		ObserverDSN:   f.observerTargetDSN,
		TrustedTarget: targetTarget,
		OperationID:   attemptID,
		Archive:       archive,
		Probe: func(context.Context, recovery.TargetWriterProof) (recovery.TargetWriterProbeResult, error) {
			atomic.AddInt32(probeCalls, 1)
			return recovery.TargetWriterProbeResult{}, errors.New("replacement prelaunch probe intentionally rejects")
		},
		Acceptance: func(context.Context, pgx.Tx, recovery.TargetWriterProof) (recovery.TargetWriterAcceptance, error) {
			atomic.AddInt32(acceptanceCalls, 1)
			return recovery.TargetWriterAcceptance{}, errors.New("replacement prelaunch acceptance must never run")
		},
		QuiescenceTimeout: 5 * time.Second, QuiescenceInterval: 50 * time.Millisecond,
	}
	if bound != nil {
		writerOptions.InstanceID = bound.InstanceID
		writerOptions.Prelaunch = bound.Prelaunch
	}
	run, err := recovery.DrillNewBorrowedWriterRun(factoryCtx, writerOptions, f.lock, gate.endpoint, f.tools)
	cancelFactory()
	if err != nil {
		return nil, errors.New("replacement prelaunch factory refused")
	}
	setup := newBorrowedIdentityCandidateSetup(t, f.fx, p1TargetDSN)
	captureCtx, cancelCapture := context.WithTimeout(ctx, borrowedReplacementPrelaunchCaptureBudget)
	prefix, err := setup.capture(captureCtx, run)
	cancelCapture()
	if err != nil {
		return nil, errors.New("replacement prelaunch prefix capture refused")
	}
	attempt := &borrowedReplacementPrelaunchAttempt{
		gate: gate, run: run, prefix: prefix, source: fresh,
		state: &borrowedReplacementPrelaunchAttemptState{},
	}
	borrowedReplacementPrelaunchPauseAtPreparation()
	if err := attempt.verifySourceAndContext(ctx); err != nil {
		return nil, err
	}
	return attempt, nil
}

// registerOrigin rechecks the source/context and registers the attempt's fresh
// prefix against THAT run through the gate borrowed-origin seam. It is
// error-returning; an observed source loss permanently invalidates the attempt.
func (a *borrowedReplacementPrelaunchAttempt) registerOrigin(ctx context.Context) error {
	if a == nil || a.gate == nil || a.run == nil || a.prefix == nil {
		return errors.New("replacement prelaunch registration requires the concrete attempt")
	}
	if err := a.verifySourceAndContext(ctx); err != nil {
		return err
	}
	return a.gate.UseBorrowedOrigin(a.run, a.prefix)
}

// dispatchRun rechecks the source/context and dispatches the one actual
// coordinator Run. A source loss or an ended caller context refuses before the
// run is ever invoked.
func (a *borrowedReplacementPrelaunchAttempt) dispatchRun(ctx context.Context) (recovery.TargetWriterResult, recovery.DrillTargetProcessReceipt, error) {
	if a == nil || a.run == nil {
		return recovery.TargetWriterResult{}, recovery.DrillTargetProcessReceipt{}, errors.New("replacement prelaunch attempt is absent")
	}
	if err := a.verifySourceAndContext(ctx); err != nil {
		return recovery.TargetWriterResult{}, recovery.DrillTargetProcessReceipt{}, err
	}
	return a.run.Run(ctx)
}

// borrowedReplacementPrelaunchRunRefusal is the refusal control: the actual
// coordinator Run is dispatched EXACTLY once on the native-ready attempt and
// must be refused by the deliberately dirty replacement guard, before any child
// launch, with the guard-specific refusal (never a malformed-option error).
func borrowedReplacementPrelaunchRunRefusal(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, attempt *borrowedReplacementPrelaunchAttempt, probeCalls, acceptanceCalls *int32) {
	t.Helper()
	f := b.fixture
	beforeDisposition := borrowedReplacementPrelaunchGuardDisposition(t, ctx, f)
	if beforeDisposition == "clean" {
		t.Fatal("replacement prelaunch requires the deliberately dirty replacement guard")
	}
	beforeOID, err := borrowedOwnerDDLTargetOID(ctx, b)
	if err != nil || beforeOID != fresh.replacementOID {
		t.Fatalf("replacement prelaunch replacement catalog identity before the attempt: oid=%d err=%v", beforeOID, err)
	}
	runCtx, cancelRun := context.WithTimeout(ctx, 60*time.Second)
	_, receipt, runErr := attempt.dispatchRun(runCtx)
	cancelRun()
	if runErr == nil {
		t.Fatal("native-ready replacement attempt was accepted with a dirty guard")
	}
	if !strings.Contains(runErr.Error(), "guard") || !strings.Contains(runErr.Error(), "not clean") {
		t.Fatalf("replacement prelaunch refusal is not the guard-specific dirty-guard refusal: %v", runErr)
	}
	identity := attempt.run.Observation().StartedIdentity()
	if identity.Started || identity.PID != 0 || identity.StartID != 0 {
		t.Fatalf("replacement prelaunch refusal started a child: %+v", identity)
	}
	if facts, factsErr := receipt.ConsumeFacts(); factsErr == nil && (facts.Started || facts.Cmd != nil || facts.Process != nil || facts.PID != 0) {
		t.Fatalf("replacement prelaunch refusal published executable frames: %+v", facts)
	}
	if got := atomic.LoadInt32(probeCalls); got != 0 {
		t.Fatalf("replacement prelaunch refusal ran the probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(acceptanceCalls); got != 0 {
		t.Fatalf("replacement prelaunch refusal ran acceptance %d times, want 0", got)
	}
	afterOID, err := borrowedOwnerDDLTargetOID(ctx, b)
	if err != nil || afterOID != beforeOID {
		t.Fatalf("replacement prelaunch refusal changed the replacement catalog: oid=%d err=%v", afterOID, err)
	}
	afterDisposition := borrowedReplacementPrelaunchGuardDisposition(t, ctx, f)
	if afterDisposition == "clean" {
		t.Fatal("replacement prelaunch refusal resolved the dirty guard")
	}
	t.Logf("native-ready replacement attempt refused by the dirty guard: disposition %q -> %q, no Started, no executable frames, no callbacks, replacement catalog unchanged (OID %d)", beforeDisposition, afterDisposition, afterOID)
}

// borrowedReplacementPrelaunchReplayRefuse proves a copied run handle cannot
// replay the one-use coordinator attempt.
func borrowedReplacementPrelaunchReplayRefuse(t *testing.T, ctx context.Context, attempt *borrowedReplacementPrelaunchAttempt, probeCalls, acceptanceCalls *int32) {
	t.Helper()
	replayCtx, cancelReplay := context.WithTimeout(ctx, 30*time.Second)
	copied := *attempt.run
	_, _, copyErr := copied.Run(replayCtx)
	cancelReplay()
	if copyErr == nil || !strings.Contains(copyErr.Error(), "already reserved") {
		t.Fatalf("copied replacement prelaunch run replay was not refused as already reserved: %v", copyErr)
	}
	if identity := attempt.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("copied replacement prelaunch run replay started a child: %+v", identity)
	}
	if got := atomic.LoadInt32(probeCalls); got != 0 {
		t.Fatalf("copied replacement prelaunch run replay ran the probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(acceptanceCalls); got != 0 {
		t.Fatalf("copied replacement prelaunch run replay ran acceptance %d times, want 0", got)
	}
	t.Logf("copied replacement prelaunch run replay refused as already reserved")
}

// borrowedReplacementPrelaunchConcurrentReplayRefuse proves two concurrent
// dispatches through one prepared run resolve to exactly one reservation and
// one dirty-guard refusal, with no child ever started.
func borrowedReplacementPrelaunchConcurrentReplayRefuse(t *testing.T, ctx context.Context, attempt *borrowedReplacementPrelaunchAttempt, probeCalls, acceptanceCalls *int32) {
	t.Helper()
	runCtx, cancelRun := context.WithTimeout(ctx, 60*time.Second)
	results := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			_, _, err := attempt.dispatchRun(runCtx)
			results <- err
		}()
	}
	joined := make(chan struct{})
	go func() { wg.Wait(); close(joined) }()
	joinedNow := false
	t.Cleanup(func() {
		cancelRun()
		if joinedNow {
			return
		}
		select {
		case <-joined:
		case <-time.After(30 * time.Second):
			t.Errorf("concurrent replacement prelaunch run join is unknown after the bounded cleanup join")
		}
	})
	select {
	case <-joined:
		joinedNow = true
	case <-time.After(60 * time.Second):
		t.Errorf("concurrent replacement prelaunch run did not complete within the bounded join; outcome unknown")
		return
	}
	cancelRun()
	var reserved, guard int
	for i := 0; i < 2; i++ {
		err := <-results
		if err == nil {
			t.Fatal("concurrent replacement prelaunch run was accepted")
		}
		switch {
		case strings.Contains(err.Error(), "already reserved"):
			reserved++
		case strings.Contains(err.Error(), "guard") && strings.Contains(err.Error(), "not clean"):
			guard++
		default:
			t.Fatalf("concurrent replacement prelaunch run refused with an unexpected error: %v", err)
		}
	}
	if reserved != 1 || guard != 1 {
		t.Fatalf("concurrent replacement prelaunch run outcomes: reserved=%d guard=%d, want exactly one each", reserved, guard)
	}
	if identity := attempt.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("concurrent replacement prelaunch run started a child: %+v", identity)
	}
	if got := atomic.LoadInt32(probeCalls); got != 0 {
		t.Fatalf("concurrent replacement prelaunch run ran the probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(acceptanceCalls); got != 0 {
		t.Fatalf("concurrent replacement prelaunch run ran acceptance %d times, want 0", got)
	}
	t.Logf("concurrent replacement prelaunch run replay refused: exactly one reservation and one dirty-guard refusal, no child")
}

// borrowedReplacementPrelaunchPreparationCancelRefuse exercises cancellation
// through the COMMON preparation helper: the mid-preparation seam holds the
// preparation, the caller context is canceled, and the common helper must
// publish nothing, with a bounded cancel-and-join cleanup for the spawned
// preparation goroutine.
func borrowedReplacementPrelaunchPreparationCancelRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, probeCalls, acceptanceCalls *int32) {
	t.Helper()
	archive := borrowedReplacementPrelaunchArchive(t, b.fixture)
	seam := installBorrowedReplacementPrelaunchPreparationSeam(t)
	prepCtx, cancelPrep := context.WithCancel(ctx)
	type prepOutcome struct {
		attempt *borrowedReplacementPrelaunchAttempt
		err     error
	}
	done := make(chan prepOutcome, 1)
	go func() {
		attempt, err := newBorrowedReplacementPrelaunchAttempt(t, prepCtx, b, fresh, archive,
			fmt.Sprintf("borrowed-replacement-prelaunch-canceled-common-%d", time.Now().UnixNano()), probeCalls, acceptanceCalls)
		done <- prepOutcome{attempt: attempt, err: err}
	}()
	joined := false
	t.Cleanup(func() {
		cancelPrep()
		seam.releaseSeam()
		if joined {
			return
		}
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Errorf("canceled common preparation join is unknown after the bounded cleanup join")
		}
	})
	select {
	case <-seam.entered:
	case <-time.After(60 * time.Second):
		t.Errorf("common preparation never reached the mid-preparation seam; completion unknown")
		return
	}
	cancelPrep()
	seam.releaseSeam()
	select {
	case got := <-done:
		joined = true
		if got.err == nil || got.attempt != nil {
			t.Fatalf("canceled common preparation published an attempt: attempt=%v err=%v", got.attempt, got.err)
		}
	case <-time.After(30 * time.Second):
		t.Errorf("canceled common preparation did not complete within the bounded join; outcome unknown")
		return
	}
	if got := atomic.LoadInt32(probeCalls); got != 0 {
		t.Fatalf("canceled common preparation ran the probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(acceptanceCalls); got != 0 {
		t.Fatalf("canceled common preparation ran acceptance %d times, want 0", got)
	}
	t.Logf("canceled common preparation published nothing through the mid-preparation seam")
}

// borrowedReplacementPrelaunchCanceledFactoryRefuse proves an already-canceled
// caller context makes the factory refuse and publish no run.
func borrowedReplacementPrelaunchCanceledFactoryRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, probeCalls, acceptanceCalls *int32) {
	t.Helper()
	f := b.fixture
	p1TargetDSN := borrowedReplacementP1TargetDSN(t, b)
	targetTarget, err := controlstore.ParseDSNTarget(p1TargetDSN)
	if err != nil {
		t.Fatalf("replacement prelaunch canceled factory trusted target refused: %v", err)
	}
	gate := newOriginGate(t, ctx, f.fx)
	storeCtx, cancelStore := context.WithTimeout(ctx, borrowedReplacementPrelaunchStoreBudget)
	store, err := controlstore.NewStore(storeCtx, f.controlPool)
	cancelStore()
	if err != nil {
		t.Fatalf("replacement prelaunch canceled factory control store refused: %v", err)
	}
	cancelCtx, cancelPrep := context.WithCancel(ctx)
	cancelPrep()
	run, prepErr := recovery.DrillNewBorrowedWriterRun(cancelCtx, recovery.TargetWriterOptions{
		OperationKind: recovery.TargetWriterOperationRestore,
		Store:         store,
		ControlDSN:    f.controlDSN,
		TargetDSN:     p1TargetDSN,
		ObserverDSN:   f.observerTargetDSN,
		TrustedTarget: targetTarget,
		OperationID:   fmt.Sprintf("borrowed-replacement-prelaunch-canceled-%d", time.Now().UnixNano()),
		Archive:       borrowedReplacementPrelaunchArchive(t, f),
		Probe: func(context.Context, recovery.TargetWriterProof) (recovery.TargetWriterProbeResult, error) {
			atomic.AddInt32(probeCalls, 1)
			return recovery.TargetWriterProbeResult{}, errors.New("replacement prelaunch canceled probe must never run")
		},
		Acceptance: func(context.Context, pgx.Tx, recovery.TargetWriterProof) (recovery.TargetWriterAcceptance, error) {
			atomic.AddInt32(acceptanceCalls, 1)
			return recovery.TargetWriterAcceptance{}, errors.New("replacement prelaunch canceled acceptance must never run")
		},
	}, f.lock, gate.endpoint, f.tools)
	if prepErr == nil || run != nil {
		t.Fatalf("canceled replacement prelaunch factory published a run: run=%v err=%v", run, prepErr)
	}
	t.Logf("canceled replacement prelaunch factory published nothing")
}

// borrowedReplacementPrelaunchWrongIdentityRefuse proves a wrong
// endpoint-target identity (observer on the W-owned source database) is refused
// by the actual factory before any run is published, on explicit short child
// budgets.
func borrowedReplacementPrelaunchWrongIdentityRefuse(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, probeCalls, acceptanceCalls *int32) {
	t.Helper()
	f := b.fixture
	p1TargetDSN := borrowedReplacementP1TargetDSN(t, b)
	targetTarget, err := controlstore.ParseDSNTarget(p1TargetDSN)
	if err != nil {
		t.Fatalf("replacement prelaunch wrong-identity trusted target refused: %v", err)
	}
	gate := newOriginGate(t, ctx, f.fx)
	storeCtx, cancelStore := context.WithTimeout(ctx, borrowedReplacementPrelaunchStoreBudget)
	store, err := controlstore.NewStore(storeCtx, f.controlPool)
	cancelStore()
	if err != nil {
		t.Fatalf("replacement prelaunch wrong-identity control store refused: %v", err)
	}
	wrongObserver := borrowedAuthRoleDSN(t, f.writerSourceDSN, f.observerRole, f.observerPass)
	factoryCtx, cancelFactory := context.WithTimeout(ctx, borrowedReplacementPrelaunchFactoryBudget)
	run, prepErr := recovery.DrillNewBorrowedWriterRun(factoryCtx, recovery.TargetWriterOptions{
		OperationKind: recovery.TargetWriterOperationRestore,
		Store:         store,
		ControlDSN:    f.controlDSN,
		TargetDSN:     p1TargetDSN,
		ObserverDSN:   wrongObserver,
		TrustedTarget: targetTarget,
		OperationID:   fmt.Sprintf("borrowed-replacement-prelaunch-wrong-%d", time.Now().UnixNano()),
		Archive:       borrowedReplacementPrelaunchArchive(t, f),
		Probe: func(context.Context, recovery.TargetWriterProof) (recovery.TargetWriterProbeResult, error) {
			atomic.AddInt32(probeCalls, 1)
			return recovery.TargetWriterProbeResult{}, errors.New("replacement prelaunch wrong-identity probe must never run")
		},
		Acceptance: func(context.Context, pgx.Tx, recovery.TargetWriterProof) (recovery.TargetWriterAcceptance, error) {
			atomic.AddInt32(acceptanceCalls, 1)
			return recovery.TargetWriterAcceptance{}, errors.New("replacement prelaunch wrong-identity acceptance must never run")
		},
	}, f.lock, gate.endpoint, f.tools)
	cancelFactory()
	if prepErr == nil || run != nil {
		t.Fatalf("wrong endpoint-target identity published a run: run=%v err=%v", run, prepErr)
	}
	if !strings.Contains(prepErr.Error(), "observer identity does not match the original trusted target") {
		t.Fatalf("wrong endpoint-target identity was not refused with the intended factory refusal: %v", prepErr)
	}
	t.Logf("wrong endpoint-target identity refused pre-launch by the actual factory")
}

// borrowedReplacementPrelaunchUnavailableArchiveRefuse proves an unavailable
// archive is refused pre-launch with the distinct archive-requirement refusal
// (never the dirty-guard refusal), so the guard-specific refusal of the control
// cannot be confused with a malformed-option failure.
func borrowedReplacementPrelaunchUnavailableArchiveRefuse(t *testing.T, ctx context.Context, attempt *borrowedReplacementPrelaunchAttempt, probeCalls, acceptanceCalls *int32) {
	t.Helper()
	runCtx, cancelRun := context.WithTimeout(ctx, 60*time.Second)
	_, receipt, runErr := attempt.dispatchRun(runCtx)
	cancelRun()
	if runErr == nil {
		t.Fatal("unavailable archive was accepted as a native-ready attempt")
	}
	if !strings.Contains(runErr.Error(), "archive") {
		t.Fatalf("unavailable archive refusal is not the archive requirement: %v", runErr)
	}
	if strings.Contains(runErr.Error(), "guard") {
		t.Fatalf("unavailable archive refusal was misreported as the guard refusal: %v", runErr)
	}
	if identity := attempt.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("unavailable archive attempt started a child: %+v", identity)
	}
	if facts, factsErr := receipt.ConsumeFacts(); factsErr == nil && (facts.Started || facts.Cmd != nil || facts.Process != nil) {
		t.Fatalf("unavailable archive attempt published executable frames: %+v", facts)
	}
	if got := atomic.LoadInt32(probeCalls); got != 0 {
		t.Fatalf("unavailable archive attempt ran the probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(acceptanceCalls); got != 0 {
		t.Fatalf("unavailable archive attempt ran acceptance %d times, want 0", got)
	}
	t.Logf("unavailable archive refused pre-launch with the distinct archive-requirement refusal (not the dirty-guard refusal)")
}

// borrowedReplacementPrelaunchSourceLossControls proves the retained genuine
// source capture is a real dependency of the prepared attempt: a source loss
// observed mid-preparation refuses the common preparation, a prepared-before-
// loss attempt is permanently invalidated and refuses registerOrigin and
// dispatchRun, copies share that loss, and reuse/reconstruction cannot
// rehabilitate. The loss is an actual copied-prefix canceled-ctx recheck. This
// is observed permanent invalidation, not continuous exclusion.
func borrowedReplacementPrelaunchSourceLossControls(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, probeCalls, acceptanceCalls *int32) {
	t.Helper()
	// Prepared before loss: a valid attempt retained without registration.
	prepared, prepErr := newBorrowedReplacementPrelaunchAttempt(t, ctx, b, fresh, borrowedReplacementPrelaunchArchive(t, b.fixture),
		fmt.Sprintf("borrowed-replacement-prelaunch-prepared-%d", time.Now().UnixNano()), probeCalls, acceptanceCalls)
	if prepErr != nil {
		t.Fatalf("pre-loss attempt preparation refused: %v", prepErr)
	}
	// Loss during preparation: the mid-preparation hook injects the actual
	// copied-prefix loss and the common helper must refuse publication.
	lossHook := func() { borrowedReplacementSessionCopiedPrefixLoss(t, fresh) }
	borrowedReplacementPrelaunchPreparationHook.Store(&lossHook)
	midLossAttempt, midLossErr := newBorrowedReplacementPrelaunchAttempt(t, ctx, b, fresh, borrowedReplacementPrelaunchArchive(t, b.fixture),
		fmt.Sprintf("borrowed-replacement-prelaunch-midloss-%d", time.Now().UnixNano()), probeCalls, acceptanceCalls)
	borrowedReplacementPrelaunchPreparationHook.Store(nil)
	if midLossErr == nil || midLossAttempt != nil {
		t.Fatalf("source loss during preparation published an attempt: attempt=%v err=%v", midLossAttempt, midLossErr)
	}
	// Prepared before loss: registerOrigin and dispatchRun refuse, the run is
	// never dispatched, and copies share the permanent invalidation.
	if err := prepared.registerOrigin(ctx); err == nil {
		t.Fatal("prepared-before-loss attempt registered its origin after the source loss")
	}
	runCtx, cancelRun := context.WithTimeout(ctx, 30*time.Second)
	_, _, dispatchErr := prepared.dispatchRun(runCtx)
	cancelRun()
	if dispatchErr == nil || !strings.Contains(dispatchErr.Error(), "permanently invalidated") {
		t.Fatalf("prepared-before-loss attempt dispatched the coordinator run after the source loss: %v", dispatchErr)
	}
	if identity := prepared.run.Observation().StartedIdentity(); identity.Started {
		t.Fatalf("prepared-before-loss attempt started a child: %+v", identity)
	}
	if prepared.state.invalidReasonNow() == "" {
		t.Fatal("source loss did not permanently invalidate the prepared attempt")
	}
	copied := *prepared
	if err := copied.registerOrigin(ctx); err == nil {
		t.Fatal("copy of the source-lost attempt rehabilitated the registration")
	}
	runCopyCtx, cancelRunCopy := context.WithTimeout(ctx, 30*time.Second)
	_, _, copiedErr := copied.dispatchRun(runCopyCtx)
	cancelRunCopy()
	if copiedErr == nil {
		t.Fatal("copy of the source-lost attempt dispatched the coordinator run")
	}
	// Reconstruction and a new session registration refuse after the loss.
	if attempt, reconErr := newBorrowedReplacementPrelaunchAttempt(t, ctx, b, fresh, nil,
		fmt.Sprintf("borrowed-replacement-prelaunch-recon-%d", time.Now().UnixNano()), probeCalls, acceptanceCalls); reconErr == nil || attempt != nil {
		t.Fatalf("post-loss reconstruction minted a prepared attempt: attempt=%v err=%v", attempt, reconErr)
	}
	conn := borrowedReplacementSessionConnectP1(t, ctx, b)
	if _, regErr := newBorrowedReplacementSessionRegistration(t, ctx, b, fresh, conn); regErr == nil {
		t.Fatal("shared-prefix loss did not refuse a new replacement session registration")
	}
	borrowedSuccessorCloseConn(t, conn)
	if err := borrowedReplacementPrelaunchProvenanceCheck(fresh); err == nil {
		t.Fatal("shared-prefix loss did not refuse the replacement prelaunch provenance")
	}
	t.Logf("source-loss controls: mid-preparation loss refused the common preparation; the prepared-before-loss attempt was permanently invalidated and refused registerOrigin/dispatchRun; copies shared the loss; reconstruction and a new session registration refused")
}

// TestBorrowedReplacementPrelaunchRefusal is the bounded native-ready
// replacement attempt -> dirty-guard refusal lane described in the file header.
func TestBorrowedReplacementPrelaunchRefusal(t *testing.T) {
	ctx := t.Context()

	// Genuine replacement capture: the actual bounded shared orchestration with
	// stage counters; never a projection.
	b := newBorrowedSuccessorBaseline(t, ctx)
	originalTargetDSN := b.fixture.writerTargetDSN
	originalObserverDSN := b.fixture.observerTargetDSN
	var stages int32
	orchestrateCtx, cancelOrchestrate := borrowedReplacementOrchestrationContext(ctx)
	fresh, err := borrowedReplacementBindingOrchestrate(orchestrateCtx, t, b, &stages)
	cancelOrchestrate()
	if err != nil {
		t.Fatalf("replacement prelaunch genuine capture refused: %v", err)
	}
	if fresh == nil {
		t.Fatal("replacement prelaunch genuine capture produced no capture")
	}
	if b.fixture.writerTargetDSN != originalTargetDSN || b.fixture.observerTargetDSN != originalObserverDSN {
		t.Fatal("replacement prelaunch lane rewrote an original fixture DSN")
	}
	if got := atomic.LoadInt32(&stages); got != borrowedReplacementStagePrefixCaptured {
		t.Fatalf("replacement prelaunch pipeline stages=%d, want %d", got, borrowedReplacementStagePrefixCaptured)
	}
	if fresh.binding.TargetDatabaseOID() != fresh.replacementOID || fresh.replacementOID == fresh.oldOID {
		t.Fatalf("replacement prelaunch capture is not replacement-bound: replacement=%d old=%d binding=%d", fresh.replacementOID, fresh.oldOID, fresh.binding.TargetDatabaseOID())
	}
	t.Logf("replacement prelaunch genuine capture: old OID %d -> replacement OID %d", fresh.oldOID, fresh.replacementOID)

	// Descriptor-bound archive controls (pathname substitution, content change,
	// beyond-limit) with the retained reader unaffected.
	borrowedReplacementPrelaunchArchiveControls(t, b.fixture)

	// Prerequisite session: exercise exactly one use and retire it; owner
	// health retained, guard unresolved, zero added acceptance.
	sessionConn, sessionReg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	sessionCtx, cancelSession := context.WithTimeout(ctx, 60*time.Second)
	sessionErr := sessionReg.Use(sessionCtx)
	cancelSession()
	if sessionErr != nil {
		t.Fatalf("replacement prelaunch prerequisite session use refused: %v", sessionErr)
	}
	if got := atomic.LoadInt32(&sessionReg.state.probeExecutions); got != 1 {
		t.Fatalf("replacement prelaunch prerequisite session probe executions=%d, want exactly 1", got)
	}
	borrowedReplacementSessionRetire(t, ctx, b, sessionConn, sessionReg.state)

	var probeCalls, acceptanceCalls int32

	// Control: native-ready replacement attempt -> dirty-guard refusal (the one
	// actual Run dispatch).
	attempt, prepErr := newBorrowedReplacementPrelaunchAttempt(t, ctx, b, fresh, borrowedReplacementPrelaunchArchive(t, b.fixture),
		fmt.Sprintf("borrowed-replacement-prelaunch-%d", time.Now().UnixNano()), &probeCalls, &acceptanceCalls)
	if prepErr != nil {
		t.Fatalf("replacement prelaunch attempt preparation refused: %v", prepErr)
	}
	if err := attempt.registerOrigin(ctx); err != nil {
		t.Fatalf("replacement prelaunch origin registration refused: %v", err)
	}
	borrowedReplacementPrelaunchRunRefusal(t, ctx, b, fresh, attempt, &probeCalls, &acceptanceCalls)

	// Replay negatives.
	borrowedReplacementPrelaunchReplayRefuse(t, ctx, attempt, &probeCalls, &acceptanceCalls)
	concurrentAttempt, prepErr := newBorrowedReplacementPrelaunchAttempt(t, ctx, b, fresh, borrowedReplacementPrelaunchArchive(t, b.fixture),
		fmt.Sprintf("borrowed-replacement-prelaunch-concurrent-%d", time.Now().UnixNano()), &probeCalls, &acceptanceCalls)
	if prepErr != nil {
		t.Fatalf("replacement prelaunch concurrent attempt preparation refused: %v", prepErr)
	}
	if err := concurrentAttempt.registerOrigin(ctx); err != nil {
		t.Fatalf("replacement prelaunch concurrent origin registration refused: %v", err)
	}
	borrowedReplacementPrelaunchConcurrentReplayRefuse(t, ctx, concurrentAttempt, &probeCalls, &acceptanceCalls)

	// Pre-launch preparation refusals.
	borrowedReplacementPrelaunchPreparationCancelRefuse(t, ctx, b, fresh, &probeCalls, &acceptanceCalls)
	borrowedReplacementPrelaunchCanceledFactoryRefuse(t, ctx, b, &probeCalls, &acceptanceCalls)
	borrowedReplacementPrelaunchWrongIdentityRefuse(t, ctx, b, &probeCalls, &acceptanceCalls)
	unavailableAttempt, prepErr := newBorrowedReplacementPrelaunchAttempt(t, ctx, b, fresh, nil,
		fmt.Sprintf("borrowed-replacement-prelaunch-unavailable-%d", time.Now().UnixNano()), &probeCalls, &acceptanceCalls)
	if prepErr != nil {
		t.Fatalf("replacement prelaunch unavailable-archive attempt preparation refused: %v", prepErr)
	}
	if err := unavailableAttempt.registerOrigin(ctx); err != nil {
		t.Fatalf("replacement prelaunch unavailable-archive origin registration refused: %v", err)
	}
	borrowedReplacementPrelaunchUnavailableArchiveRefuse(t, ctx, unavailableAttempt, &probeCalls, &acceptanceCalls)

	// Source-loss controls; LAST on this fixture because the loss permanently
	// invalidates the shared replacement prefix.
	borrowedReplacementPrelaunchSourceLossControls(t, ctx, b, fresh, &probeCalls, &acceptanceCalls)

	// Guard non-clean and acceptance zero across the whole lane.
	borrowedOwnerDDLGuard(t, ctx, b)
	if got := atomic.LoadInt32(&probeCalls); got != 0 {
		t.Fatalf("replacement prelaunch lane ran the probe %d times, want 0", got)
	}
	if got := atomic.LoadInt32(&acceptanceCalls); got != 0 {
		t.Fatalf("replacement prelaunch lane ran acceptance %d times, want 0", got)
	}

	// Partial/UNKNOWN/contaminated preparation stops pre-launch: the shared
	// binding-lane negatives stop the orchestration before any fresh capture,
	// so no attempt can ever be prepared or launched.
	borrowedReplacementPartialDDLNegative(t, ctx)
	borrowedReplacementContaminatedNegative(t, ctx)
}
