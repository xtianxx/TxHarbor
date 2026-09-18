// withdrawalworker_joint.go owns the joint-deployment construction of the 011
// worker. NewWithdrawalWorker stays the standalone constructor: its
// Reconciler/Driver are nil, so a 011-only run reconciles nothing and sends
// nothing. Joint deployment needs the real 010 participants, and the adapter
// is 010-owned, so this constructor takes the boundary interfaces (the app
// package must not import internal/txlifecycle: the txlifecycle joint tests
// import app, and a package import would close a test-build cycle) and builds
// the real Reconciler/StepDriver around them.
package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/metrics"
)

// JointWiringFunc assembles the joint-deployment participants from the process
// configuration and pool. The binary entrypoint supplies it through
// Deps.JointWiring because internal/app must not import internal/txlifecycle
// (the txlifecycle joint tests import app, closing a test-build cycle).
type JointWiringFunc func(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool) (JointDeps, error)

// JointDeps carries the joint-deployment boundary participants. All four are
// required: a nil half would silently degrade the worker to standalone
// behavior — exactly what joint wiring exists to prevent.
type JointDeps struct {
	// Advancer drives 010 (the 010-owned txlifecycle.LifecycleLive in the
	// joint workspace).
	Advancer execution.LifecycleAdvancer
	// Reader reads 010's authority facts for the reconcile loop (the same
	// adapter value as Advancer carries both interfaces).
	Reader execution.LifecycleReader
	// Binding is 011's 008 observation seam (execution.BindingReader over
	// 008's read provider).
	Binding execution.BindingReader
	// AllocBinding provisions the 008 binding for one intent BEFORE the
	// driver's step-issue gate observes it (the issue gate refuses an absent
	// binding with zero writes, so an unsupplied allocator would loop the
	// worker silently). The implementation lives in the joint wiring
	// composition root (the 008 domain); app holds only the closure so its
	// import graph stays free of 008's RPC-bearing packages. The 008
	// allocator is idempotent for one intent (allocated first, replayed for
	// the identical re-request), so a repeat call is a no-op by contract.
	AllocBinding func(ctx context.Context, intentID string) error
	// ConfirmAttempt runs 010's receipt/confirmation scan for the intent's
	// current attempt (V13-1: receipt/confirmation BEFORE completed). The
	// implementation lives in the joint wiring composition root; it is a
	// no-op when nothing sendable exists and is safe to repeat (the scan
	// advances with the chain).
	ConfirmAttempt func(ctx context.Context, intentID string) error
}

// NewJointWithdrawalWorker returns the production-wired worker: the standalone
// core plus the real 010 participants, so Driver and Reconciler are non-nil
// and a joint run executes through 010 instead of silently no-op'ing. A
// missing participant refuses construction rather than minting a worker that
// only looks wired.
func NewJointWithdrawalWorker(pool *pgxpool.Pool, cfg *config.Config, m *metrics.Metrics, log *slog.Logger, deps JointDeps) (*WithdrawalWorker, error) {
	if deps.Advancer == nil || deps.Reader == nil || deps.Binding == nil {
		return nil, fmt.Errorf("joint withdrawal worker requires advancer, reader and binding")
	}
	if deps.AllocBinding == nil {
		return nil, fmt.Errorf("joint withdrawal worker requires the 008 binding allocator (the step-issue gate refuses an absent binding with zero writes)")
	}
	if deps.ConfirmAttempt == nil {
		return nil, fmt.Errorf("joint withdrawal worker requires the 010 confirmation scanner (receipt/confirmation must precede completion)")
	}
	w, err := NewWithdrawalWorker(pool, cfg, m, log)
	if err != nil {
		return nil, err
	}
	w.Reconciler = &execution.Reconciler{Pool: pool, Reader: deps.Reader}
	w.Driver = &execution.StepDriver{Pool: pool, Claims: w.Claims, Binding: deps.Binding, Advancer: deps.Advancer}
	w.AllocBinding = deps.AllocBinding
	w.ConfirmAttempt = deps.ConfirmAttempt
	return w, nil
}
