// us3admin.go implements the T028 US3 operator surface of reconcile-admin:
// `reverify` (one bounded history-revalidation slice over a running task) and
// `close` (the evidence-gated closure of one pending_verify ticket). It is a
// thin wiring layer over the T027 production paths (Store.RunReverifySweep /
// Store.CloseDiscrepancy) and the T009 authorization evaluator; no lifecycle
// rule is reimplemented here.
//
// Boundaries (FR-005/010/017/018/019, Q1/Q5; contracts/auth-matrix.md;
// contracts/discrepancy-lifecycle.md History Revalidation Sweep):
//
//   - `reverify` authorizes as a scan-management budgeted operation:
//     principal × ActionScanStart (scan_manage) × the task's recorded scope.
//     contracts/auth-matrix.md marks `reverify` system-only (a bounded
//     read-only re-check) and the T009 evaluator structurally denies
//     ActionReverify for every operator grant, so the operator invocation
//     reuses the same authority a `scan` invocation uses rather than
//     inventing a permission. That authority only starts a bounded slice: it
//     grants no close, dispose or execution right, and no env key is added.
//   - the slice bounds are required positive values supplied per invocation
//     (`--max-items`, `--max-pg-requests`, `--max-item-attempts`; an optional
//     non-negative `--max-item-duration`) and are refused by name when
//     missing/illegal: no default is invented and a local value is never
//     claimed as a production threshold (FR-019). The slice's PG charges also
//     consume the task's configured total budget through a ParentBudget built
//     from the T018 keys, so revalidation is never unaccounted.
//   - the evidence re-read goes through the production EventStateReverifyEvaluator
//     built exactly like the scan path's event evidence (T015 read-only event
//     adapter + reference-consumer registration + read-only quarantine store +
//     T036 height->chain-time window resolver). The command never runs, replays
//     or unblocks the consumer, and the sweep writes only 014-owned records
//     (reverify/gap/audit rows and the task's history_sweep_through traversal
//     cursor). The traversal cursor is progress, never verified completeness:
//     the output's verified_complete field is the conservative
//     ReverifySweepResult.VerifiedComplete predicate, and no daemon, serve or
//     worker auto-start ever runs here (ADR-001).
//   - `close` is evidence-gated end to end: the ticket's recorded scope
//     authorizes principal × ActionVerifyClose (the close permission; default
//     deny, refusals audited), and Store.CloseDiscrepancy always reads the
//     latest reverify row from the database, so a caller can never substitute
//     fresher or fabricated evidence. The required --close-basis JSON object
//     is the recorded range/block/version/timestamp snapshot; the tolerance
//     defaults to TXHARBOR_RECON_FRESHNESS_TOLERANCE (an explicit override must
//     stay positive). Incomplete, expired, unknown or divergent evidence is
//     refused and audited; no payment, signature or broadcast path is involved.
package reconcileadmin

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/reconciliation"
)

// parseReconcileSweepBound parses one required positive integer slice bound.
// A missing, non-integer, zero or oversized value is refused by name: there is
// no default, and a local test value is never claimed as a production
// threshold. The upper bound is a representability guard only.
func parseReconcileSweepBound(raw, name string) (int, error) {
	n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s %q is not a decimal integer in [1, %d] (required, no default)", name, raw, math.MaxInt32)
	}
	return int(n), nil
}

// parseReconcileItemDuration parses the optional per-item duration bound: an
// empty value (or an explicit zero) leaves the parent context as the only
// bound, a negative value is refused by name.
func parseReconcileItemDuration(raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, nil
	}
	itemDuration, err := time.ParseDuration(trimmed)
	if err != nil || itemDuration < 0 {
		return 0, fmt.Errorf("--max-item-duration %q is not a non-negative duration (zero means the parent context bounds one item)", raw)
	}
	return itemDuration, nil
}

// parseReconcileCloseBasis validates the required close snapshot: a JSON
// object carrying the range/block/version/timestamp basis (the migration
// discrepancy_close_basis_shape CHECK only accepts an object, and a close
// without a basis is never valid). It performs no I/O.
func parseReconcileCloseBasis(raw string) ([]byte, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, errors.New("--close-basis is required (a JSON object snapshot of range/block/version/timestamp)")
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(trimmed), &obj); err != nil || obj == nil {
		return nil, errors.New("--close-basis must be a JSON object (the recorded range/block/version/timestamp snapshot)")
	}
	return []byte(trimmed), nil
}

// parseReconcileCloseTolerance resolves the close evidence tolerance: the
// explicit flag when given (must stay positive), otherwise the configured
// TXHARBOR_RECON_FRESHNESS_TOLERANCE. A non-positive default is refused by
// name: the close never invents a tolerance of its own.
func parseReconcileCloseTolerance(cfg *config.Config, raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed != "" {
		tolerance, err := time.ParseDuration(trimmed)
		if err != nil || tolerance <= 0 {
			return 0, fmt.Errorf("--reverify-tolerance %q is not a positive duration", raw)
		}
		return tolerance, nil
	}
	if cfg == nil || cfg.Recon.FreshnessTolerance <= 0 {
		return 0, fmt.Errorf("close requires a positive evidence tolerance: configure %s or pass --reverify-tolerance (no default)",
			config.EnvReconFreshnessTolerance)
	}
	return cfg.Recon.FreshnessTolerance, nil
}

// reconcileSweepCursorText renders the persisted traversal cursor for operator
// output ("none" before the sweep starts). It is a position, never a
// completeness claim.
func reconcileSweepCursorText(cursor *reconciliation.HistorySweepCursor) string {
	if cursor == nil || !cursor.Valid() {
		return "none"
	}
	return cursor.EvidenceAt.UTC().Format(time.RFC3339Nano) + "|" + cursor.DiscrepancyID
}

// reconcileAdminReverify implements `reverify`: one bounded
// history-revalidation slice over the closed items of a running task
// (T027 executor; contracts/discrepancy-lifecycle.md History Revalidation
// Sweep, quickstart §11):
//
//   - authorization is the same scan-management budgeted operation `scan`
//     runs (principal × ActionScanStart × the task's recorded scope): the
//     auth-matrix reverify row is system-only and ActionReverify is never
//     operator-grantable, so no reverify permission is invented or needed for
//     a bounded read-only re-check;
//   - the required slice bounds are refused by name when missing (no
//     defaults), and the slice's charges also consume the task's configured
//     total budget (ParentBudget), so repeat invocations stay budgeted;
//   - the evidence evaluator is the production read-only event-state seam
//     with the reference-consumer/quarantine/window-resolver construction of
//     the scan path; a divergent re-read enters only the approved reverify
//     flow (pending_verify) and never auto-disposes, auto-closes, recovers,
//     replays or pays;
//   - output is one honest line: rechecked/consistent/divergent/pending/
//     failed/gaps, the traversal cursor, the stop reason, and
//     verified_complete computed only from the conservative predicate (the
//     cursor != verified completeness).
func reconcileAdminReverify(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("reconcile-admin reverify", flag.ContinueOnError)
	fs.SetOutput(stderr)
	taskID := fs.String("task-id", "", "running task whose closed items are revalidated (required)")
	maxItems := fs.String("max-items", "", "bounded closed items per slice (required, integer >= 1; no default)")
	maxPGRequests := fs.String("max-pg-requests", "", "bounded PostgreSQL requests per slice (required, integer >= 1; no default)")
	maxItemAttempts := fs.String("max-item-attempts", "", "bounded evidence re-read attempts per item (required, integer >= 1; no default)")
	maxItemDuration := fs.String("max-item-duration", "", "optional per-item evidence re-read timeout (zero/omitted: the parent context bounds it)")
	reason := fs.String("reason", "", "audit annotation (optional)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*taskID) == "" || strings.TrimSpace(*maxItems) == "" ||
		strings.TrimSpace(*maxPGRequests) == "" || strings.TrimSpace(*maxItemAttempts) == "" {
		reconcileAdminUsage(stderr)
		return 2
	}
	id, err := reconciliation.RequireTaskID(*taskID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}
	sliceItems, err := parseReconcileSweepBound(*maxItems, "--max-items")
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", err)
		return 2
	}
	slicePGRequests, err := parseReconcileSweepBound(*maxPGRequests, "--max-pg-requests")
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", err)
		return 2
	}
	sliceAttempts, err := parseReconcileSweepBound(*maxItemAttempts, "--max-item-attempts")
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", err)
		return 2
	}
	itemDuration, err := parseReconcileItemDuration(*maxItemDuration)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", err)
		return 2
	}
	slice := reconciliation.ReverifySlice{
		MaxItems:        sliceItems,
		MaxPGRequests:   slicePGRequests,
		MaxItemAttempts: sliceAttempts,
		MaxItemDuration: itemDuration,
	}

	env, code := reconcileAdminOpen(ctx, d)
	if env == nil {
		return code
	}
	defer env.pool.Close()

	// The operator invocation reuses the scan-management authority of the
	// task scope. ActionReverify itself is system-only (the T009 evaluator
	// denies it for every grant), so no reverify permission exists to demand.
	task, ok := env.authorizeTaskAction(ctx, stderr, id, reconciliation.ActionScanStart)
	if !ok {
		return 1
	}
	limits, err := reconcileScanLimits(env.cfg)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", err)
		return 1
	}
	// Freshness feeds the read-only event adapter; the same required bounds as
	// `scan` are reused (no separate default is invented for reverify).
	_, freshness, _, _, maxEventRows, err := reconcileScanBounds(env.cfg)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", err)
		return 1
	}
	probes, err := reconcileWindowProbes(env.cfg)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", err)
		return 1
	}
	parentBudget, err := reconciliation.NewBudget(limits)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 1
	}

	// The evidence seam is the production read-only event-state evaluator,
	// wired with the same reference-consumer/quarantine/window-resolver
	// construction as the scan path (reused, not reimplemented).
	eventState, err := reconciliation.NewEventStateAdapter(env.pool, reconciliation.EventStateConfig{
		FreshnessTolerance: freshness,
		MaxEntries:         maxEventRows,
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: event-state adapter refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	windowClient, err := eth.Dial(ctx, env.cfg.RPCURL, env.cfg.IndexRPCTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: window resolver RPC refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	defer windowClient.Close()
	blockTimeReader, err := reconciliation.NewHeaderBlockTimeReader(env.pool, windowClient, env.cfg.IndexRPCTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: block-time reader refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	windowResolver, err := reconciliation.NewTimeHeightResolver(blockTimeReader, reconciliation.TimeHeightResolverConfig{
		MaxProbes:  probes,
		RPCTimeout: env.cfg.IndexRPCTimeout,
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: window resolver refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	reference, err := events.NewReferenceConsumer(env.pool, events.ConsumerOptions{
		GapWait:     env.cfg.Events.Consumer.GapWait,
		BackoffBase: env.cfg.Events.Consumer.BackoffBase,
		BackoffMax:  env.cfg.Events.Consumer.BackoffMax,
		RetryLimit:  env.cfg.Events.Consumer.RetryLimit,
		ChainID:     int64(env.cfg.ChainID),
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: event consumer registration refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	evaluator := &reconciliation.EventStateReverifyEvaluator{
		Reader:               eventState,
		Consumers:            []reconciliation.EventConsumerRegistration{{Name: events.RefConsumerName, Progress: reference.Consumer}},
		Quarantine:           &events.QuarantineStore{Pool: env.pool},
		UpstreamReceipts:     task.UpstreamReceipts,
		ChainID:              int64(env.cfg.ChainID),
		HeightWindowResolver: windowResolver,
	}

	result, sweepErr := env.store.RunReverifySweep(ctx, reconciliation.ReverifySweepRequest{
		TaskID:       id,
		Actor:        env.principal.String(),
		Slice:        slice,
		ParentBudget: parentBudget,
		Evaluator:    evaluator,
		Reason:       strings.TrimSpace(*reason),
	})
	parentUsage := parentBudget.Usage()
	fmt.Fprintf(stdout,
		"txharbor reconcile-admin: reverify task_id=%s rechecked=%d consistent=%d divergent=%d pending=%d failed=%d gaps=%d open_gaps=%d open_reverify_gaps=%d retry_exhausted=%d cursor=%s cursor_advanced=%t stop=%s verified_complete=%t budget_pg=%d/%d parent_pg=%d/%d principal=%s\n",
		result.TaskID, result.Rechecked, result.Consistent, result.Divergent, result.Pending, result.Failed,
		result.GapsWritten, result.OpenGaps, result.OpenReverifyGaps, result.RetryExhausted,
		reconcileSweepCursorText(result.Cursor), result.CursorAdvanced, result.Stop, result.VerifiedComplete(),
		result.PGUsed, slice.MaxPGRequests, parentUsage.PGUsed, limits.MaxPGRequests, env.principal)
	if sweepErr != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: reverify stopped with error: %s\n", logx.Redact(sweepErr.Error()))
		return 1
	}
	return 0
}

// reconcileAdminClose implements `close`: the evidence-gated closure of one
// ticket through Store.CloseDiscrepancy (FR-010/016, Q5;
// contracts/discrepancy-lifecycle.md Transitions):
//
//   - the ticket's recorded scope authorizes principal × ActionVerifyClose ×
//     scope (the close permission; default deny; refusals audited by the
//     evaluator). A ticket without a derivable scope fails closed;
//   - the close guard always reads the latest reverify row from the database:
//     incomplete, expired, unknown or divergent evidence — and any gap-limited
//     result — is refused and audited (disposed ≠ reverified ≠ closed), and
//     the caller can never substitute fresher evidence;
//   - --close-basis is the required JSON object snapshot of the basis being
//     recorded (range/block/version/timestamp); the tolerance defaults to
//     TXHARBOR_RECON_FRESHNESS_TOLERANCE with an optional positive override;
//   - no payment, signature or broadcast path is involved.
func reconcileAdminClose(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("reconcile-admin close", flag.ContinueOnError)
	fs.SetOutput(stderr)
	discrepancyID := fs.String("discrepancy-id", "", "discrepancy to close (required)")
	closeBasis := fs.String("close-basis", "", "range/block/version/timestamp snapshot as a JSON object (required)")
	reason := fs.String("reason", "", "operator reason (required, recorded)")
	toleranceRaw := fs.String("reverify-tolerance", "", "maximum evidence age the close accepts (defaults to "+config.EnvReconFreshnessTolerance+"; an explicit value must be positive)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*discrepancyID) == "" ||
		strings.TrimSpace(*closeBasis) == "" || strings.TrimSpace(*reason) == "" {
		reconcileAdminUsage(stderr)
		return 2
	}
	id, err := parseReconcileDiscrepancyID(*discrepancyID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}
	basis, err := parseReconcileCloseBasis(*closeBasis)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", err)
		return 2
	}

	env, code := reconcileAdminOpen(ctx, d)
	if env == nil {
		return code
	}
	defer env.pool.Close()

	tolerance, err := parseReconcileCloseTolerance(env.cfg, *toleranceRaw)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", err)
		return 1
	}

	// The authorization scope is exactly the ticket's recorded detection
	// scope; authorization runs before any write and never under a row lock.
	row, err := env.readDiscrepancyView(ctx, id)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: close failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	scope, scopeErr := reconcileDiscrepancyAuthScope(id, row.EvidenceDomain)
	if scopeErr != nil {
		// No derivable scope: fail closed through the evaluator so the
		// refusal is audited as an invalid-scope denial, never guessed around.
		if _, err := env.evaluator.Authorize(ctx, env.principal, reconciliation.ActionVerifyClose, reconciliation.AuthScope{}); err != nil {
			fmt.Fprintf(stderr, "txharbor reconcile-admin: authorization failed: %s\n", logx.Redact(err.Error()))
			return 1
		}
		fmt.Fprintf(stderr, "txharbor reconcile-admin: close refused: %s\n", logx.Redact(scopeErr.Error()))
		return 1
	}
	decision, err := env.evaluator.Authorize(ctx, env.principal, reconciliation.ActionVerifyClose, scope)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: authorization failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if !decision.Allowed {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: refused %s: %s (%s)\n",
			reconciliation.ActionVerifyClose, decision.Reason, decision.Detail)
		return 1
	}

	result, err := env.store.CloseDiscrepancy(ctx, reconciliation.DiscrepancyCloseRequest{
		DiscrepancyID:     id,
		Actor:             env.principal.String(),
		Reason:            strings.TrimSpace(*reason),
		CloseBasis:        basis,
		ReverifyTolerance: tolerance,
	})
	if err != nil {
		switch {
		case errors.Is(err, reconciliation.ErrReverifyRequired):
			fmt.Fprintf(stderr,
				"txharbor reconcile-admin: close refused (a fresh consistent reverify row is required; incomplete/expired/unknown/divergent evidence can never close): %s\n",
				logx.Redact(err.Error()))
		default:
			fmt.Fprintf(stderr, "txharbor reconcile-admin: close refused: %s\n", logx.Redact(err.Error()))
		}
		return 1
	}
	fmt.Fprintf(stdout,
		"txharbor reconcile-admin: close discrepancy_id=%s from=%s to=%s reopen_count=%d close_basis=recorded principal=%s\n",
		result.DiscrepancyID, result.From, result.To, result.ReopenCount, env.principal)
	return 0
}
