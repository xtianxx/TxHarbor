package recovery

// admission.go implements the ADR-004 §2.1 R1/R5 limited ruling: a
// controller-owned single-attempt recovery admission handle, distinct from
// authentication credentials, readiness facts, process receipts and
// service-resumption approvals.
//
// Invariants:
//   - Mint happens only after the coordinator's existing prelaunch checks
//     (authoritative comparator, guard prepare, launch-intent commit) — never
//     from readiness, journal, audit or receipt facts alone.
//   - The handle is copy-shared: every copy sees the same once-state. Exactly
//     one consume() wins; the loser gets a fixed refusal without touching the
//     winner's observation or Wait facts.
//   - consume() refuses further LAUNCHES only; the minted attempt's evidence
//     chain continues inside the same coordinator run (acceptance is not a
//     launch and does not need a second handle).
//   - invalidate() is permanent: it latches the attempt into a refused state
//     on every loss path (lock health loss, observer failure, marker/token
//     staleness, cancellation). An invalidated handle can never mint,
//     consume, or re-validate again; a new attempt requires a fresh mint
//     under the R1 conditions.
//   - The handle carries no DSNs, passwords or reconstructable authority
//     material. Durable audit rows describe its lifecycle; they are
//     references only, never re-mintable authority.

import (
	"errors"
	"sync"
)

// liminal fixed refusal classes (safe denial strings only; never credentials).
var (
	errAdmissionInvalidated   = errors.New("admission invalidated: the attempt lost its protection or proof and cannot continue")
	errAdmissionAlreadyMinted = errors.New("admission was already minted for this attempt")
	errAdmissionNotMinted     = errors.New("admission was never minted for this attempt")
	errAdmissionConsumed      = errors.New("admission already consumed: one launch per attempt")
)

// admissionState is the copy-shared one-shot admission lifecycle for one
// minted restore attempt (ADR-004 §2.1 R1/R5; mirrors the proven test lanes).
type admissionState struct {
	mu          sync.Mutex
	minted      bool
	consumed    bool
	invalidated bool
	// reason is a fixed safe denial string for the refusal classification. It
	// never carries DSN or credential material.
	reason string
}

// newAdmission allocates a not-yet-minted state. The zero value is inert and
// forever invalid — only the coordinator's mint path produces a live state.
func newAdmission() *admissionState { return &admissionState{} }

// mint transitions an unused state into minted IS valid (single time). It
// refuses when the state was minted, consumed or invalidated before. A failed
// mint leaves the state permanently unable to become live (fail-closed); the
// coordinator's prelaunch error path keeps the guard disposition unchanged.
func (a *admissionState) mint() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case a.invalidated:
		return errAdmissionInvalidated
	case a.minted:
		return errAdmissionAlreadyMinted
	default:
		a.minted = true
		a.reason = ""
		return nil
	}
}

// consume spends the single attempt at a launch boundary. It refuses when the
// state was never minted, already consumed, or invalidated. Consumption
// refuses further launches only; acceptance continues inside the same run.
func (a *admissionState) consume() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case a.invalidated:
		return errAdmissionInvalidated
	case !a.minted:
		return errAdmissionNotMinted
	case a.consumed:
		return errAdmissionConsumed
	default:
		a.consumed = true
		return nil
	}
}

// invalidate permanently latches the attempt as refused (any loss path).
// It is idempotent and always wins over mint/re-consume.
func (a *admissionState) invalidate() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.invalidated = true
	if a.reason == "" {
		a.reason = "invalidated"
	}
}

// usableForLaunch reports whether a launch boundary may still run (used by
// the acceptance boundary to refuse when the attempt lost its protection).
func (a *admissionState) usableForLaunch() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.minted && !a.consumed && !a.invalidated
}

// barrierForAcceptance refuses acceptance for an attempt whose admission was
// consumed then invalidated (no valid launch basis), or never minted.
func (a *admissionState) barrierForAcceptance() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case !a.minted:
		return errAdmissionNotMinted
	case a.invalidated:
		return errAdmissionInvalidated
	default:
		return nil
	}
}
