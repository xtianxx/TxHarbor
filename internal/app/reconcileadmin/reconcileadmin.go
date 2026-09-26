// Package reconcileadmin implements the thin 014 operator command surface
// (`txharbor reconcile-admin`, T018): start/scan/pause/resume/cancel/show
// (contracts/task-lifecycle.md Operations; contracts/auth-matrix.md).
//
// It lives in its own package (rather than internal/app) because it imports
// internal/reconciliation, which transitively imports internal/txlifecycle;
// internal/txlifecycle's integration tests import internal/app, so an
// internal/app -> internal/reconciliation import would close a test-build
// cycle (the same constraint documented on internal/app's Deps.JointWiring).
//
// Boundaries (FR-001/003/011/012/019/023/024, Q2/Q3):
//
//   - Every action evaluates the authenticated principal × action × scope
//     against the 014 recon_permission registry (default deny: zero grants
//     are seeded; unconfigured rows, unknown actions and out-of-scope
//     requests are refused and audited). The principal is bound from
//     controlled deployment configuration (TXHARBOR_RECON_PRINCIPAL, the
//     ConfigPrincipal carrier), never from a CLI flag; free-text fields can
//     never authorize anything (operator/reason are audit annotations only).
//   - `pause`/`cancel` act only on 014 recon_task rows, settle the in-flight
//     attempts within the configured bounded limit, and leave the uncovered
//     remainder visible as gap rows; no upstream funds flow is ever paused.
//   - `start` records the task (state `created`) with the required confirm
//     policy depth N (--confirm-threshold-n, N >= 1; no default) snapshotted
//     into policy_refs, so the scan's chain-evidence confirmation basis is
//     explicit and reproducible. `resume` activates it (created/paused/
//     suspended_budget -> running). `scan` claims the next budgeted interval
//     and drives the T016 ScanOnce loop; the budget knobs are required
//     positive values (missing/illegal is refused by name, never defaulted:
//     no local value is a production threshold). Height-scoped enumeration is
//     chain-first aware: the durable indexed transfer-log facts of the
//     interval are reverse checked against the authoritative withdrawal
//     execution rows (read-only) and a chain transaction without any PG row
//     becomes a missing candidate (FR-009, US1 missing detection). Time
//     scopes deliberately do not map time->heights and keep the chain party
//     unknown/pending (pending-by-design; a dedicated window-resolver task
//     owns that mapping).
//   - `show` is strictly read-only and reports coverage honestly (open gaps /
//     uncovered remainder are never rendered as "fully consistent").
//
// Deliberately absent from this batch: the T023 permission grant/revoke/query
// management commands (management trust-root naming is carried through
// configuration only; ordinary holders cannot self-grant because no
// recon_permission row can authorize a management action) and the US2/US3
// discrepancy lifecycle/reverify commands. Audit rows are append-only.
package reconcileadmin

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/reconciliation"
	"github.com/xtianxx/txharbor/internal/txlifecycle"
)

// Deps carries the process dependencies so the command is testable in-process
// (the same shape as internal/app.Deps, reduced to what this package needs).
type Deps struct {
	Getenv func(string) (string, bool)
	Stdout io.Writer
	Stderr io.Writer
}

func (d Deps) stdout() io.Writer {
	if d.Stdout == nil {
		return io.Discard
	}
	return d.Stdout
}

func (d Deps) stderr() io.Writer {
	if d.Stderr == nil {
		return io.Discard
	}
	return d.Stderr
}

func (d Deps) getenv() func(string) (string, bool) {
	if d.Getenv == nil {
		return os.LookupEnv
	}
	return d.Getenv
}

// Run runs the 014 operator subcommand. It is the only CLI that mutates 014
// task rows; every mutation is authorized, audited, and bounded.
func Run(ctx context.Context, args []string, d Deps) int {
	if len(args) == 0 {
		reconcileAdminUsage(d.stderr())
		return 2
	}
	switch args[0] {
	case "start":
		return reconcileAdminStart(ctx, args[1:], d)
	case "scan":
		return reconcileAdminScan(ctx, args[1:], d)
	case "pause":
		return reconcileAdminPause(ctx, args[1:], d)
	case "resume":
		return reconcileAdminResume(ctx, args[1:], d)
	case "cancel":
		return reconcileAdminCancel(ctx, args[1:], d)
	case "show":
		return reconcileAdminShow(ctx, args[1:], d)
	default:
		stderr := d.stderr()
		fmt.Fprintf(stderr, "txharbor reconcile-admin: unknown action %q\n", args[0])
		reconcileAdminUsage(stderr)
		return 2
	}
}

// reconAdminEnv is the opened command context: configuration, pool, store,
// the bound principal and the authorization evaluator. Opening any of these
// failing is a hard refusal (no partial wiring).
type reconAdminEnv struct {
	cfg       *config.Config
	pool      *pgxpool.Pool
	store     *reconciliation.Store
	evaluator *reconciliation.Evaluator
	principal reconciliation.Principal
}

// reconcileAdminOpen loads the configuration, binds the authenticated
// principal and opens the pool/store/evaluator. Every failure is fail-closed;
// the caller closes env.pool when non-nil.
func reconcileAdminOpen(ctx context.Context, d Deps) (*reconAdminEnv, int) {
	stderr := d.stderr()
	cfg, err := config.Load(d.getenv())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: configuration error: %s\n", logx.Redact(err.Error()))
		return nil, 1
	}
	if strings.TrimSpace(cfg.Recon.Principal) == "" {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s is required (authenticated principal binding; default deny)\n",
			config.EnvReconPrincipal)
		return nil, 1
	}
	principal, err := reconciliation.ConfigPrincipal(cfg.Recon.Principal)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s is invalid: %s\n",
			config.EnvReconPrincipal, logx.Redact(err.Error()))
		return nil, 1
	}
	pool, err := db.OpenPool(ctx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return nil, 1
	}
	store, err := reconciliation.NewStore(pool)
	if err != nil {
		pool.Close()
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return nil, 1
	}
	evaluator, err := reconciliation.NewEvaluator(pool, reconcileAuthzAuditWriter{pool: pool})
	if err != nil {
		pool.Close()
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return nil, 1
	}
	return &reconAdminEnv{cfg: cfg, pool: pool, store: store, evaluator: evaluator, principal: principal}, 0
}

// reconcileAuthzAuditWriter persists authorization refusals into the
// append-only recon_audit trail (data-model.md §1.8: refusals are rows,
// including the ownership hint). It never writes anything else and a write
// failure never turns a denial into an allow (the caller still refuses).
type reconcileAuthzAuditWriter struct {
	pool *pgxpool.Pool
}

const insertReconAuthzRefusalSQL = `
INSERT INTO recon_audit (actor, action, target, reason, evidence, result)
VALUES ($1, 'refuse', $2::jsonb, $3, $4, 'refused')`

// RecordAuthzRefusal implements reconciliation.AuthzAuditWriter.
func (w reconcileAuthzAuditWriter) RecordAuthzRefusal(ctx context.Context, rec reconciliation.AuthzAuditRecord) error {
	if w.pool == nil {
		return errors.New("reconciliation authorization audit writer is not wired")
	}
	target := map[string]any{
		"action":     string(rec.Action),
		"permission": string(rec.Permission),
		"scope":      rec.Scope,
	}
	if rec.TargetPrincipal != "" {
		target["target_principal"] = rec.TargetPrincipal
	}
	encoded, err := json.Marshal(target)
	if err != nil {
		return fmt.Errorf("marshal refusal audit target: %w", err)
	}
	actor := strings.TrimSpace(rec.Principal)
	if actor == "" {
		actor = "unauthenticated"
	}
	if len(actor) > 128 {
		actor = actor[:128]
	}
	evidence := rec.Detail
	if len(evidence) > 1024 {
		evidence = evidence[:1024]
	}
	if _, err := w.pool.Exec(ctx, insertReconAuthzRefusalSQL,
		actor, string(encoded), string(rec.Reason), evidence); err != nil {
		return fmt.Errorf("record authorization refusal: %w", err)
	}
	return nil
}

// reconcileTaskAuthScope builds the authorization scope of one existing task
// from its recorded scope. Height tasks carry their concrete range; time tasks
// are range-unbounded in the height-typed scope carrier (a time task can only
// be covered by an unbounded grant).
func reconcileTaskAuthScope(task *reconciliation.Task) (reconciliation.AuthScope, error) {
	scope := reconciliation.AuthScope{
		ChainID:       task.ScopeChainID,
		Kind:          task.ScopeKind,
		BusinessTypes: task.BusinessTypes,
	}
	if task.ScopeKind == reconciliation.ScopeHeight {
		start, end := task.ScopeStart.Height, task.ScopeEnd.Height
		scope.RangeStart, scope.RangeEnd = &start, &end
	}
	if err := scope.Validate(); err != nil {
		return scope, err
	}
	return scope, nil
}

// authorizeTaskAction loads the task, evaluates the authorized scope of the
// requested action and audits refusals. It returns the task only on an allow;
// any refusal or read failure is already reported on stderr.
func (e *reconAdminEnv) authorizeTaskAction(ctx context.Context, stderr io.Writer, taskID string,
	action reconciliation.Action) (*reconciliation.Task, bool) {
	task, err := e.store.TaskByID(ctx, taskID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return nil, false
	}
	scope, err := reconcileTaskAuthScope(task)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: task %s has an invalid authorization scope: %s\n",
			taskID, logx.Redact(err.Error()))
		return nil, false
	}
	decision, err := e.evaluator.Authorize(ctx, e.principal, action, scope)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: authorization failed: %s\n", logx.Redact(err.Error()))
		return nil, false
	}
	if !decision.Allowed {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: refused %s: %s (%s)\n",
			action, decision.Reason, decision.Detail)
		return nil, false
	}
	return task, true
}

// reconcileScanLimits returns the required scan budget limits. Every value
// must be configured positive; a missing or malformed value is refused by
// name (FR-019). No default is invented: a local value is never claimed as a
// production threshold.
func reconcileScanLimits(cfg *config.Config) (reconciliation.BudgetLimits, error) {
	var missing []string
	note := func(value int64, name string) {
		if value <= 0 {
			missing = append(missing, name)
		}
	}
	note(int64(cfg.Recon.Concurrency), config.EnvReconConcurrency)
	note(cfg.Recon.MaxSpanPerClaim, config.EnvReconMaxSpanPerClaim)
	note(int64(cfg.Recon.MaxPGRequests), config.EnvReconMaxPGRequests)
	note(int64(cfg.Recon.MaxRPCRequests), config.EnvReconMaxRPCRequests)
	if cfg.Recon.MaxDuration <= 0 {
		missing = append(missing, config.EnvReconMaxDuration)
	}
	if len(missing) > 0 {
		return reconciliation.BudgetLimits{}, fmt.Errorf(
			"scan budget is required and must be a positive value: %s", strings.Join(missing, ", "))
	}
	limits := reconciliation.BudgetLimits{
		MaxConcurrency:  cfg.Recon.Concurrency,
		MaxSpanPerClaim: cfg.Recon.MaxSpanPerClaim,
		MaxDuration:     cfg.Recon.MaxDuration,
		MaxPGRequests:   cfg.Recon.MaxPGRequests,
		MaxRPCRequests:  cfg.Recon.MaxRPCRequests,
	}
	if err := limits.Validate(); err != nil {
		return reconciliation.BudgetLimits{}, err
	}
	return limits, nil
}

// reconcileScanBounds returns the remaining required scan inputs (lease TTL,
// freshness tolerance, chain tip lag, per-interval candidate/event-row
// bounds). Missing/invalid values are refused by name.
func reconcileScanBounds(cfg *config.Config) (leaseTTL, freshness time.Duration, tipLag int64, maxCandidates, maxEventRows int, err error) {
	var missing []string
	leaseTTL, freshness, tipLag = cfg.Recon.LeaseTTL, cfg.Recon.FreshnessTolerance, cfg.Recon.MaxTipLag
	maxCandidates, maxEventRows = cfg.Recon.MaxCandidates, cfg.Recon.MaxEventRows
	if leaseTTL <= 0 {
		missing = append(missing, config.EnvReconLeaseTTL)
	}
	if freshness <= 0 {
		missing = append(missing, config.EnvReconFreshnessTolerance)
	}
	if tipLag <= 0 {
		missing = append(missing, config.EnvReconMaxTipLag)
	}
	if maxCandidates <= 0 {
		missing = append(missing, config.EnvReconMaxCandidates)
	}
	if maxEventRows <= 0 {
		missing = append(missing, config.EnvReconMaxEventRows)
	}
	if len(missing) > 0 {
		err = fmt.Errorf("scan bounds are required and must be positive: %s", strings.Join(missing, ", "))
		return
	}
	return
}

// reconcileSettleLimit returns the required bounded in-flight settle limit
// (pause/cancel settle at most this many attempts per pass; there is no
// unbounded settle).
func reconcileSettleLimit(cfg *config.Config) (int, error) {
	if cfg.Recon.SettleLimit <= 0 {
		return 0, fmt.Errorf("the bounded settle limit is required and must be positive: %s", config.EnvReconSettleLimit)
	}
	return cfg.Recon.SettleLimit, nil
}

// reconcileAdminStart implements `start(scope, budget) -> created`.
func reconcileAdminStart(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("reconcile-admin start", flag.ContinueOnError)
	fs.SetOutput(stderr)
	taskID := fs.String("task-id", "", "task UUID (generated when omitted)")
	chainID := fs.String("chain-id", "", "scope chain identity (required)")
	kind := fs.String("scope-kind", "", "scope dimension: height|time (required)")
	from := fs.String("from", "", "inclusive start: block height (height) or RFC3339 time (time) (required)")
	to := fs.String("to", "", "inclusive end: block height (height) or RFC3339 time (time) (required)")
	businessTypes := fs.String("business-types", "", "comma-separated scope business types (required)")
	upstream := fs.String("upstream-receipts", "", "comma-separated business_type=source:connected|unconnected (a missing declaration counts as unconnected)")
	confirmThresholdN := fs.String("confirm-threshold-n", "", "confirm policy depth N (required, integer >= 1; recorded in the task's policy_refs snapshot; no default)")
	reason := fs.String("reason", "", "audit annotation (optional)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*chainID) == "" || strings.TrimSpace(*kind) == "" ||
		strings.TrimSpace(*from) == "" || strings.TrimSpace(*to) == "" || strings.TrimSpace(*businessTypes) == "" ||
		strings.TrimSpace(*confirmThresholdN) == "" {
		reconcileAdminUsage(stderr)
		return 2
	}

	scopeKind := reconciliation.ScopeKind(strings.TrimSpace(*kind))
	start, end, err := parseReconRange(scopeKind, *from, *to)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}
	types, err := parseReconBusinessTypes(*businessTypes)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}
	receipts, err := parseReconUpstreamReceipts(*upstream)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}
	thresholdN, err := parseReconConfirmThresholdN(*confirmThresholdN)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}

	env, code := reconcileAdminOpen(ctx, d)
	if env == nil {
		return code
	}
	defer env.pool.Close()

	// The requested scope itself must be authorized before any task row is
	// written (default deny; refusals audited).
	authScope := reconciliation.AuthScope{ChainID: strings.TrimSpace(*chainID), Kind: scopeKind, BusinessTypes: types}
	if scopeKind == reconciliation.ScopeHeight {
		authScope.RangeStart, authScope.RangeEnd = &start.Height, &end.Height
	}
	decision, err := env.evaluator.Authorize(ctx, env.principal, reconciliation.ActionScanStart, authScope)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: authorization failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if !decision.Allowed {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: refused %s: %s (%s)\n",
			reconciliation.ActionScanStart, decision.Reason, decision.Detail)
		return 1
	}

	limits, err := reconcileScanLimits(env.cfg)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", err)
		return 1
	}
	budget, err := json.Marshal(map[string]any{
		"max_concurrency":    limits.MaxConcurrency,
		"max_span_per_claim": limits.MaxSpanPerClaim,
		"max_duration":       limits.MaxDuration.String(),
		"max_pg_requests":    limits.MaxPGRequests,
		"max_rpc_requests":   limits.MaxRPCRequests,
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: encode budget snapshot: %s\n", logx.Redact(err.Error()))
		return 1
	}

	// The confirm policy depth is snapshotted per task (FR-001): the scan
	// reads it back as the chain-evidence confirmation basis. It comes from
	// the required operator flag only; no default exists, and a local test
	// value is never claimed as a production threshold.
	policyRefs, err := json.Marshal(map[string]any{"confirm_threshold_n": thresholdN})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: encode policy_refs snapshot: %s\n", logx.Redact(err.Error()))
		return 1
	}

	result, err := env.store.CreateTask(ctx, reconciliation.TaskCreateRequest{
		TaskID:           *taskID,
		ChainID:          strings.TrimSpace(*chainID),
		Kind:             scopeKind,
		Start:            start,
		End:              end,
		BusinessTypes:    types,
		UpstreamReceipts: receipts,
		PolicyRefs:       policyRefs,
		Budget:           budget,
		CreatedBy:        env.principal.String(),
		Reason:           *reason,
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: start refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout, "txharbor reconcile-admin: start created task_id=%s state=%s scope_kind=%s business_types=%s principal=%s\n",
		result.TaskID, result.State, scopeKind, strings.Join(businessTypeNames(types), ","), env.principal)
	return 0
}

// reconcileAdminScan implements one budgeted `scan` invocation: it claims the
// next interval of a running task and drives the T016 ScanOnce compare loop
// with the real read-only adapters (T013-T015) and the read-only candidate
// enumeration of this command (PG-anchored rows plus the chain-first
// missing-fact discovery for height scopes).
func reconcileAdminScan(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("reconcile-admin scan", flag.ContinueOnError)
	fs.SetOutput(stderr)
	taskID := fs.String("task-id", "", "task to scan (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*taskID) == "" {
		reconcileAdminUsage(stderr)
		return 2
	}
	id, err := reconciliation.RequireTaskID(*taskID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}

	env, code := reconcileAdminOpen(ctx, d)
	if env == nil {
		return code
	}
	defer env.pool.Close()

	task, ok := env.authorizeTaskAction(ctx, stderr, id, reconciliation.ActionScanStart)
	if !ok {
		return 1
	}
	limits, err := reconcileScanLimits(env.cfg)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", err)
		return 1
	}
	leaseTTL, freshness, tipLag, maxCandidates, maxEventRows, err := reconcileScanBounds(env.cfg)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", err)
		return 1
	}

	chainFacts, err := reconciliation.NewChainFactsAdapter(env.pool, reconciliation.ChainFactsConfig{
		MaxSourceAge: freshness,
		MaxTipLag:    tipLag,
		MaxRangeSpan: limits.MaxSpanPerClaim,
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: chain-facts adapter refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	pgState, err := reconciliation.NewPGStateAdapter(env.pool, txlifecycle.NewStore(env.pool), reconciliation.PGStateOptions{
		FreshnessWindow: freshness,
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: PG-state adapter refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	eventState, err := reconciliation.NewEventStateAdapter(env.pool, reconciliation.EventStateConfig{
		FreshnessTolerance: freshness,
		MaxEntries:         maxEventRows,
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: event-state adapter refused: %s\n", logx.Redact(err.Error()))
		return 1
	}

	result, scanErr := env.store.ScanOnce(ctx, reconciliation.ScanOnceRequest{
		TaskID:             id,
		Owner:              env.principal.String(),
		LeaseTTL:           leaseTTL,
		Limits:             limits,
		FreshnessTolerance: freshness,
		Sources: reconciliation.ScanSources{
			Chain:  chainFacts,
			PG:     pgState,
			Events: eventState,
		},
		Candidates: reconcileScanCandidates{
			pool:          env.pool,
			chainID:       task.ScopeChainID,
			limit:         maxCandidates,
			chain:         chainFacts,
			businessTypes: task.BusinessTypes,
		},
	})

	fmt.Fprintf(stdout,
		"txharbor reconcile-admin: scan task_id=%s stop=%s stopped_state=%s attempts=%d committed=%d discarded=%d candidates=%d tickets=%d merged=%d occurrences=%d pending=%d absorbed=%d gaps=%d suspended=%t",
		result.TaskID, result.Stop, result.StoppedState, result.Attempts, result.Committed,
		result.Discarded, result.Candidates, result.Tickets, result.Merged, result.Occurrences,
		result.Pending, result.Absorbed, result.Gaps, result.Suspended)
	if result.LastCheckpoint != nil {
		fmt.Fprintf(stdout, " checkpoint_seq=%d persisted_through=%s",
			result.LastCheckpoint.Seq, reconcileBoundText(result.LastCheckpoint.ResultPersistedThrough))
	}
	if result.Suspended {
		fmt.Fprintf(stdout, " suspend_resource=%s suspend_detail=%q", result.SuspendResource, result.SuspendDetail)
	}
	if result.DiscardReason != "" {
		fmt.Fprintf(stdout, " discard_reason=%s", result.DiscardReason)
	}
	fmt.Fprintf(stdout, " budget_pg=%d/%d budget_rpc=%d/%d task_scope=%s\n",
		result.BudgetUsage.PGUsed, limits.MaxPGRequests, result.BudgetUsage.RPCUsed, limits.MaxRPCRequests,
		reconcileTaskScopeText(task))
	if scanErr != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: scan stopped with error: %s\n", logx.Redact(scanErr.Error()))
		return 1
	}
	return 0
}

// reconcileAdminPause pauses a 014 task (running -> paused) and settles the
// in-flight attempts within the configured bounded limit. No upstream funds
// flow is paused: only the 014 task row and its attempts are touched.
func reconcileAdminPause(ctx context.Context, args []string, d Deps) int {
	return reconcileAdminStop(ctx, args, d, "pause", reconciliation.TaskStatePaused,
		reconciliation.SettleModePause, reconciliation.ActionScanPause)
}

// reconcileAdminCancel cancels a 014 task (running -> cancelled; the pointer
// never moves past an unfinished interval) and settles in-flight attempts as
// cancelled. The action vocabulary has no separate scan_cancel token, so the
// scan-management permission governs it (contracts/auth-matrix.md).
func reconcileAdminCancel(ctx context.Context, args []string, d Deps) int {
	return reconcileAdminStop(ctx, args, d, "cancel", reconciliation.TaskStateCancelled,
		reconciliation.SettleModeCancel, reconciliation.ActionScanPause)
}

// reconcileAdminStop is the shared pause/cancel implementation: authorize,
// transition the task with the operator reason, then settle the claimed
// attempts boundedly (each settled attempt leaves a gap row).
func reconcileAdminStop(ctx context.Context, args []string, d Deps, action string,
	to reconciliation.TaskState, mode reconciliation.SettleMode, authAction reconciliation.Action) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("reconcile-admin "+action, flag.ContinueOnError)
	fs.SetOutput(stderr)
	taskID := fs.String("task-id", "", "task to "+action+" (required)")
	reason := fs.String("reason", "", "operator reason (required, recorded)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*taskID) == "" || strings.TrimSpace(*reason) == "" {
		reconcileAdminUsage(stderr)
		return 2
	}
	id, err := reconciliation.RequireTaskID(*taskID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}

	env, code := reconcileAdminOpen(ctx, d)
	if env == nil {
		return code
	}
	defer env.pool.Close()

	if _, ok := env.authorizeTaskAction(ctx, stderr, id, authAction); !ok {
		return 1
	}
	settleLimit, err := reconcileSettleLimit(env.cfg)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", err)
		return 1
	}

	transition, err := env.store.TransitionTask(ctx, reconciliation.TaskTransitionRequest{
		TaskID: id,
		To:     to,
		Reason: *reason,
		Actor:  env.principal.String(),
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s refused: %s\n", action, logx.Redact(err.Error()))
		return 1
	}
	settled, err := env.store.SettleInFlight(ctx, reconciliation.SettleInFlightRequest{
		TaskID: id,
		Mode:   mode,
		Limit:  settleLimit,
		Reason: *reason,
		Actor:  env.principal.String(),
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s transitioned %s -> %s but in-flight settle failed: %s\n",
			action, transition.From, transition.To, logx.Redact(err.Error()))
		return 1
	}
	checkpoint := "none"
	if transition.Checkpoint != nil {
		checkpoint = reconcileBoundText(transition.Checkpoint.ResultPersistedThrough)
	}
	fmt.Fprintf(stdout,
		"txharbor reconcile-admin: %s task_id=%s from=%s to=%s settled_attempts=%d persisted_through=%s principal=%s\n",
		action, id, transition.From, transition.To, len(settled.Settled), checkpoint, env.principal)
	return 0
}

// reconcileAdminResume activates a task (created/paused/suspended_budget ->
// running). Intervals already persisted are never re-reported; uncovered
// ranges continue (FR-003).
func reconcileAdminResume(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("reconcile-admin resume", flag.ContinueOnError)
	fs.SetOutput(stderr)
	taskID := fs.String("task-id", "", "task to resume (required)")
	reason := fs.String("reason", "", "audit annotation (optional)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*taskID) == "" {
		reconcileAdminUsage(stderr)
		return 2
	}
	id, err := reconciliation.RequireTaskID(*taskID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}

	env, code := reconcileAdminOpen(ctx, d)
	if env == nil {
		return code
	}
	defer env.pool.Close()

	if _, ok := env.authorizeTaskAction(ctx, stderr, id, reconciliation.ActionScanResume); !ok {
		return 1
	}
	transition, err := env.store.TransitionTask(ctx, reconciliation.TaskTransitionRequest{
		TaskID: id,
		To:     reconciliation.TaskStateRunning,
		Reason: *reason,
		Actor:  env.principal.String(),
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: resume refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	checkpoint := "none"
	if transition.Checkpoint != nil {
		checkpoint = reconcileBoundText(transition.Checkpoint.ResultPersistedThrough)
	}
	fmt.Fprintf(stdout, "txharbor reconcile-admin: resume task_id=%s from=%s to=%s persisted_through=%s principal=%s\n",
		id, transition.From, transition.To, checkpoint, env.principal)
	return 0
}

// reconcileAdminShow prints the read-only task/coverage view. It never writes
// (an allowed query writes no audit row; refusals are audited by the
// evaluator) and never renders an incomplete task as fully consistent.
func reconcileAdminShow(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("reconcile-admin show", flag.ContinueOnError)
	fs.SetOutput(stderr)
	taskID := fs.String("task-id", "", "task to inspect (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*taskID) == "" {
		reconcileAdminUsage(stderr)
		return 2
	}
	id, err := reconciliation.RequireTaskID(*taskID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}

	env, code := reconcileAdminOpen(ctx, d)
	if env == nil {
		return code
	}
	defer env.pool.Close()

	task, ok := env.authorizeTaskAction(ctx, stderr, id, reconciliation.ActionQuery)
	if !ok {
		return 1
	}
	coverage, err := env.store.TaskCoverageByID(ctx, id)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: show failed: %s\n", logx.Redact(err.Error()))
		return 1
	}

	checkpoint := "none"
	if coverage.Head != nil {
		checkpoint = fmt.Sprintf("seq=%d covered_through=%s persisted_through=%s",
			coverage.Head.Seq, reconcileBoundText(coverage.Head.CoveredThrough),
			reconcileBoundText(coverage.Head.ResultPersistedThrough))
	}
	uncovered := "scope_end"
	if coverage.UncoveredFrom != nil {
		uncovered = reconcileBoundText(*coverage.UncoveredFrom)
	}
	consistent := coverage.Task.State == reconciliation.TaskStateRunning &&
		coverage.OpenGaps == 0 && coverage.UncoveredFrom == nil
	fmt.Fprintf(stdout,
		"txharbor reconcile-admin: show task_id=%s state=%s scope_chain=%s scope_kind=%s range=%s business_types=%s pause_reason=%q\n",
		task.TaskID, task.State, task.ScopeChainID, task.ScopeKind,
		reconcileTaskScopeText(task), businessTypeNames(task.BusinessTypes), task.PauseReason)
	for _, receipt := range task.UpstreamReceipts {
		fmt.Fprintf(stdout, "  upstream business_type=%s connected=%t source=%q\n",
			receipt.BusinessType, receipt.Connected, receipt.Source)
	}
	fmt.Fprintf(stdout, "  checkpoint %s open_gaps=%d uncovered_from=%s scan_complete=%t\n",
		checkpoint, coverage.OpenGaps, uncovered, consistent)
	return 0
}

// reconcileScanCandidates enumerates the read-only business candidates of one
// claimed interval. It is the caller-supplied enumeration seam of ScanOnce
// (T016): it never writes, never opens a transaction, and its error is
// committed as a query_failed gap by ScanOnce (an interval can never close
// silently over a failed or truncated enumeration).
//
// Height intervals combine two bounded discovery passes:
//
//   - PG-anchored: the canonical 011 receipts whose block lies in the
//     interval; the candidate carries the receipt's chain anchor
//     (block/hash/tx hash).
//   - chain-first (US1 missing detection, FR-009): the durable indexed
//     transfer-log facts of the interval (the same erc20_transfer_logs read
//     path the T013 chain-facts adapter uses) are enumerated through the
//     chain-facts reader, reverse checked against the authoritative
//     withdrawal execution rows by tx hash (tx_attempt_signings/tx_receipts,
//     read-only), and every chain transaction with no PG row becomes a
//     missing candidate carrying the stable chain identity (tx_hash business
//     key + block/log evidence reference). Classification stays with T017 and
//     ScanOnce: this layer only reports the candidate and its evidence.
//
// Time intervals deliberately do not map time->heights: no chain anchor is
// invented and the chain party stays unknown/pending (pending-by-design; a
// dedicated window-resolver task owns that mapping, so time scopes stay
// PG-anchored until then).
//
// Bounds and honesty (no silent truncation):
//
//   - every pass is bounded by the scope's MAX_CANDIDATES; a pass that would
//     exceed the bound, or a combined list beyond it, fails the enumeration so
//     the interval carries a query_failed gap row instead of an unnoticed
//     subset;
//   - a chain read failure, an uncovered durable log stream, or orphaned block
//     evidence fails the same way: chain-first discovery never claims a
//     complete discovery set over evidence it could not prove complete;
//   - chain-first candidates are attributed to the withdrawal business type
//     only (the reverse-checked 011 rows are the withdrawal execution side); a
//     scope without that business type keeps the PG-anchored enumeration. The
//     pass deliberately does not attempt transfer-direction or asset
//     attribution: each candidate carries its durable block/tx evidence
//     reference and stays an alert-only signal for operator triage (no
//     automatic disposition, FR-014);
//   - only read-only SQL runs here. No business table is written, and no
//     recovery, replay/unblock or payment path is ever invoked (FR-014/015/
//     023).
type reconcileScanCandidates struct {
	pool    *pgxpool.Pool
	chainID string
	limit   int
	// chain is the read-only chain-facts surface (T013). The chain-first pass
	// consumes its canonical transfer-log facts and its durable log-stream
	// coverage only; the confirmation gate stays with the compare loop.
	chain reconciliation.ChainFactsReader
	// businessTypes is the task scope's closed business-type set; chain-first
	// candidates must stay inside it.
	businessTypes []reconciliation.BusinessType
}

const reconcileCandidatesTimeSQL = `
SELECT rq.request_id
FROM withdrawal_requests rq
WHERE rq.chain_id = $1 AND rq.created_at >= $2 AND rq.created_at <= $3
ORDER BY rq.created_at, rq.request_id
LIMIT $4`

const reconcileCandidatesHeightSQL = `
SELECT a.attempt_id, a.intent_id, COALESCE(i.request_id, ''),
       rc.block_number, rc.block_hash, rc.tx_hash
FROM tx_receipts rc
JOIN tx_attempts a ON a.attempt_id = rc.attempt_id
LEFT JOIN payment_intents i ON i.intent_id = a.intent_id
WHERE a.chain_id = $1 AND rc.block_number >= $2 AND rc.block_number <= $3
  AND rc.canonicality = 'canonical'
ORDER BY rc.block_number, a.attempt_id
LIMIT $4`

// reconcilePGTxHashLookupSQL is the chain-first reverse check: does any
// authoritative withdrawal execution row exist for the transaction hash?
// tx_attempt_signings (unique tx hash) and tx_receipts both bind a hash to its
// attempt. Read-only; no row means no PG business record exists for the chain
// fact.
const reconcilePGTxHashLookupSQL = `
SELECT resolved.tx_hash
FROM (
    SELECT s.tx_hash
    FROM tx_attempt_signings s
    JOIN tx_attempts a ON a.attempt_id = s.attempt_id
    WHERE a.chain_id = $1 AND s.tx_hash = ANY($2::text[])
    UNION
    SELECT r.tx_hash
    FROM tx_receipts r
    JOIN tx_attempts a ON a.attempt_id = r.attempt_id
    WHERE a.chain_id = $1 AND r.tx_hash = ANY($2::text[])
) resolved`

// ScanCandidates implements reconciliation.ScanCandidateSource.
func (s reconcileScanCandidates) ScanCandidates(ctx context.Context, interval reconciliation.ScanInterval) ([]reconciliation.ScanCandidate, error) {
	if s.pool == nil {
		return nil, errors.New("candidate enumeration is not wired")
	}
	if s.limit <= 0 {
		return nil, errors.New("candidate enumeration limit must be positive")
	}
	chainID, err := strconv.ParseInt(strings.TrimSpace(s.chainID), 10, 64)
	if err != nil || chainID <= 0 {
		return nil, fmt.Errorf("candidate enumeration requires a numeric chain id, got %q", s.chainID)
	}
	switch interval.From.Kind {
	case reconciliation.ScopeHeight:
		return s.scanHeightCandidates(ctx, chainID, interval.From.Height, interval.To.Height)
	case reconciliation.ScopeTime:
		return s.scanTimeCandidates(ctx, chainID, interval.From.Time, interval.To.Time)
	default:
		return nil, fmt.Errorf("candidate enumeration does not know scope kind %q", interval.From.Kind)
	}
}

// scanHeightCandidates combines the PG-anchored and chain-first passes under
// the shared MAX_CANDIDATES bound.
func (s reconcileScanCandidates) scanHeightCandidates(ctx context.Context, chainID, from, to int64) ([]reconciliation.ScanCandidate, error) {
	pgCandidates, err := s.scanPGHeightCandidates(ctx, chainID, from, to)
	if err != nil {
		return nil, err
	}
	chainCandidates, err := s.scanChainFirstHeightCandidates(ctx, chainID, from, to)
	if err != nil {
		return nil, err
	}
	if len(pgCandidates)+len(chainCandidates) > s.limit {
		return nil, fmt.Errorf(
			"candidate enumeration %d..%d exceeds the bound of %d candidates (%d PG-anchored + %d chain-first); refusing a silent truncation",
			from, to, s.limit, len(pgCandidates), len(chainCandidates))
	}
	return append(pgCandidates, chainCandidates...), nil
}

// scanPGHeightCandidates enumerates canonical receipts inside the height
// interval. One extra row is read so an overflow past the bound is detected
// instead of silently truncated.
func (s reconcileScanCandidates) scanPGHeightCandidates(ctx context.Context, chainID, from, to int64) ([]reconciliation.ScanCandidate, error) {
	rows, err := s.pool.Query(ctx, reconcileCandidatesHeightSQL, chainID, from, to, s.limit+1)
	if err != nil {
		return nil, fmt.Errorf("enumerate height candidates: %w", err)
	}
	defer rows.Close()
	var out []reconciliation.ScanCandidate
	for rows.Next() {
		var (
			attemptID, intentID, requestID string
			blockNumber                    int64
			blockHash, txHash              string
		)
		if err := rows.Scan(&attemptID, &intentID, &requestID, &blockNumber, &blockHash, &txHash); err != nil {
			return nil, fmt.Errorf("scan height candidate: %w", err)
		}
		key, ok := reconcileCandidateKey(intentID, requestID)
		if !ok {
			continue
		}
		out = append(out, reconciliation.ScanCandidate{
			BusinessType: reconciliation.BusinessWithdrawal,
			BusinessKey:  key,
			ChainFact: reconciliation.ChainFactRef{
				BlockNumber: blockNumber,
				BlockHash:   blockHash,
				TxHash:      txHash,
			},
			EvidenceRef: "scan:candidates:receipt attempt_id=" + attemptID,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("enumerate height candidates: %w", err)
	}
	if len(out) > s.limit {
		return nil, fmt.Errorf("PG-anchored candidate enumeration %d..%d exceeds the bound of %d candidates",
			from, to, s.limit)
	}
	return out, nil
}

// scanChainFirstHeightCandidates enumerates the durable indexed transfer-log
// facts of one height interval and reports every chain transaction with no
// authoritative PG row as a missing candidate. The chain-facts reader supplies
// the canonical log facts and the log-stream coverage proof; the reverse check
// confirms the absence inside PostgreSQL.
func (s reconcileScanCandidates) scanChainFirstHeightCandidates(ctx context.Context, chainID, from, to int64) ([]reconciliation.ScanCandidate, error) {
	if !reconcileHasBusinessType(s.businessTypes, reconciliation.BusinessWithdrawal) {
		// The reverse-checked 011 rows are the withdrawal execution side;
		// without the withdrawal business type a chain-only fact has no
		// attributable scope identity, so this pass is skipped (the
		// PG-anchored pass still covers the scope's own business types).
		return nil, nil
	}
	if s.chain == nil {
		return nil, errors.New("chain-first enumeration requires the chain-facts reader (no silent skip)")
	}
	bundle, err := s.chain.Observe(ctx, reconciliation.ChainFactsQuery{
		ChainID:          chainID,
		From:             from,
		To:               to,
		NeedTransferLogs: true,
	})
	if err != nil {
		return nil, fmt.Errorf("chain-first enumeration %d..%d: %w", from, to, err)
	}
	if bundle.Orphaned {
		return nil, fmt.Errorf("chain-first enumeration %d..%d: block evidence is orphaned; refusing to derive permanent missing candidates", from, to)
	}
	if !bundle.Coverage.LogRangeCovered {
		return nil, fmt.Errorf("chain-first enumeration %d..%d: the durable log stream does not cover the interval; refusing to claim a complete discovery set", from, to)
	}
	facts, overflow := reconcileChainFirstTxFacts(bundle.Logs, s.limit)
	if overflow {
		return nil, fmt.Errorf("chain-first enumeration %d..%d: more than %d distinct chain transactions; refusing a silent truncation",
			from, to, s.limit)
	}
	if len(facts) == 0 {
		return nil, nil
	}
	pgTxHashes, err := s.pgTxHashes(ctx, chainID, facts)
	if err != nil {
		return nil, err
	}
	out := make([]reconciliation.ScanCandidate, 0, len(facts))
	for i := range facts {
		if _, covered := pgTxHashes[facts[i].txHash]; covered {
			// The transaction already has an authoritative PG row; the
			// PG-anchored pass owns it (no duplicate coverage here).
			continue
		}
		out = append(out, reconciliation.ScanCandidate{
			BusinessType: reconciliation.BusinessWithdrawal,
			BusinessKey: reconciliation.BusinessKey{
				Kind:  reconciliation.BusinessKeyTxHash,
				Value: facts[i].txHash,
			},
			ChainFact: reconciliation.ChainFactRef{
				BlockNumber: facts[i].blockNumber,
				BlockHash:   facts[i].blockHash,
				TxHash:      facts[i].txHash,
			},
			EvidenceRef: fmt.Sprintf("scan:candidates:chain tx_hash=%s block=%d", facts[i].txHash, facts[i].blockNumber),
		})
	}
	return out, nil
}

// reconcileChainFirstTxFact is one distinct chain transaction discovered by
// the chain-first pass: the first (lowest block/log index) durable transfer
// fact of the transaction anchors the candidate's chain identity.
type reconcileChainFirstTxFact struct {
	txHash      string
	blockNumber int64
	blockHash   string
}

// reconcileChainFirstTxFacts groups the ordered transfer-log facts by
// transaction hash (first occurrence wins; the log order is deterministic),
// normalizes the hash, and reports an overflow past the bound instead of
// truncating silently.
func reconcileChainFirstTxFacts(logs []reconciliation.ChainFactLog, limit int) ([]reconcileChainFirstTxFact, bool) {
	out := make([]reconcileChainFirstTxFact, 0, len(logs))
	seen := make(map[string]struct{}, len(logs))
	for i := range logs {
		txHash := strings.ToLower(strings.TrimSpace(logs[i].TxHash))
		if txHash == "" {
			continue
		}
		if _, dup := seen[txHash]; dup {
			continue
		}
		seen[txHash] = struct{}{}
		out = append(out, reconcileChainFirstTxFact{
			txHash:      txHash,
			blockNumber: logs[i].BlockNumber,
			blockHash:   logs[i].BlockHash,
		})
		if len(out) > limit {
			return nil, true
		}
	}
	return out, false
}

// pgTxHashes returns the subset of the given tx hashes that already has an
// authoritative withdrawal execution row on the scoped chain.
func (s reconcileScanCandidates) pgTxHashes(ctx context.Context, chainID int64, facts []reconcileChainFirstTxFact) (map[string]struct{}, error) {
	hashes := make([]string, 0, len(facts))
	for i := range facts {
		hashes = append(hashes, facts[i].txHash)
	}
	rows, err := s.pool.Query(ctx, reconcilePGTxHashLookupSQL, chainID, hashes)
	if err != nil {
		return nil, fmt.Errorf("chain-first PG reverse lookup: %w", err)
	}
	defer rows.Close()
	out := make(map[string]struct{}, len(hashes))
	for rows.Next() {
		var txHash string
		if err := rows.Scan(&txHash); err != nil {
			return nil, fmt.Errorf("chain-first PG reverse lookup: %w", err)
		}
		out[strings.ToLower(strings.TrimSpace(txHash))] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("chain-first PG reverse lookup: %w", err)
	}
	return out, nil
}

// reconcileHasBusinessType reports whether the scope's closed set contains the
// given business type.
func reconcileHasBusinessType(types []reconciliation.BusinessType, want reconciliation.BusinessType) bool {
	for _, businessType := range types {
		if businessType == want {
			return true
		}
	}
	return false
}

// scanTimeCandidates enumerates withdrawal requests created inside the time
// interval. One extra row is read so an overflow past the bound is detected
// instead of silently truncated; no time->height mapping is attempted here
// (pending-by-design until a window resolver exists).
func (s reconcileScanCandidates) scanTimeCandidates(ctx context.Context, chainID int64, from, to time.Time) ([]reconciliation.ScanCandidate, error) {
	rows, err := s.pool.Query(ctx, reconcileCandidatesTimeSQL, chainID, from, to, s.limit+1)
	if err != nil {
		return nil, fmt.Errorf("enumerate time candidates: %w", err)
	}
	defer rows.Close()
	var out []reconciliation.ScanCandidate
	for rows.Next() {
		var requestID string
		if err := rows.Scan(&requestID); err != nil {
			return nil, fmt.Errorf("scan time candidate: %w", err)
		}
		if strings.TrimSpace(requestID) == "" {
			continue
		}
		out = append(out, reconciliation.ScanCandidate{
			BusinessType: reconciliation.BusinessWithdrawal,
			BusinessKey: reconciliation.BusinessKey{
				Kind:  reconciliation.BusinessKeyRequestID,
				Value: requestID,
			},
			EvidenceRef: "scan:candidates:request request_id=" + requestID,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("enumerate time candidates: %w", err)
	}
	if len(out) > s.limit {
		return nil, fmt.Errorf("PG-anchored time enumeration %s..%s exceeds the bound of %d candidates",
			from.UTC().Format(time.RFC3339Nano), to.UTC().Format(time.RFC3339Nano), s.limit)
	}
	return out, nil
}

// reconcileCandidateKey prefers the 007 request identity and falls back to
// the 012 intent identity. A row with neither cannot anchor an identity.
func reconcileCandidateKey(intentID, requestID string) (reconciliation.BusinessKey, bool) {
	if trimmed := strings.TrimSpace(requestID); trimmed != "" {
		return reconciliation.BusinessKey{Kind: reconciliation.BusinessKeyRequestID, Value: trimmed}, true
	}
	if trimmed := strings.TrimSpace(intentID); trimmed != "" {
		return reconciliation.BusinessKey{Kind: reconciliation.BusinessKeyIntentID, Value: trimmed}, true
	}
	return reconciliation.BusinessKey{}, false
}

// reconcileBoundText renders a range bound for operator output.
func reconcileBoundText(bound reconciliation.RangeBound) string {
	if bound.Kind == reconciliation.ScopeTime {
		return bound.Time.UTC().Format(time.RFC3339Nano)
	}
	return strconv.FormatInt(bound.Height, 10)
}

// reconcileTaskScopeText renders the task's scope range.
func reconcileTaskScopeText(task *reconciliation.Task) string {
	return reconcileBoundText(task.ScopeStart) + ".." + reconcileBoundText(task.ScopeEnd)
}

// businessTypeNames renders business types for operator output.
func businessTypeNames(types []reconciliation.BusinessType) []string {
	out := make([]string, len(types))
	for i, t := range types {
		out[i] = string(t)
	}
	return out
}

// parseReconRange parses the inclusive scope bounds from CLI text for the
// given kind: decimal heights or RFC3339 times.
func parseReconRange(kind reconciliation.ScopeKind, from, to string) (reconciliation.RangeBound, reconciliation.RangeBound, error) {
	switch kind {
	case reconciliation.ScopeHeight:
		startHeight, err := strconv.ParseInt(strings.TrimSpace(from), 10, 64)
		if err != nil || startHeight < 0 {
			return reconciliation.RangeBound{}, reconciliation.RangeBound{},
				fmt.Errorf("--from %q is not a non-negative block height", from)
		}
		endHeight, err := strconv.ParseInt(strings.TrimSpace(to), 10, 64)
		if err != nil || endHeight < 0 {
			return reconciliation.RangeBound{}, reconciliation.RangeBound{},
				fmt.Errorf("--to %q is not a non-negative block height", to)
		}
		start, end := reconciliation.HeightBound(startHeight), reconciliation.HeightBound(endHeight)
		if cmp, err := start.Compare(end); err != nil || cmp > 0 {
			return reconciliation.RangeBound{}, reconciliation.RangeBound{},
				fmt.Errorf("scope range %s..%s is not ascending", from, to)
		}
		return start, end, nil
	case reconciliation.ScopeTime:
		startAt, err := time.Parse(time.RFC3339, strings.TrimSpace(from))
		if err != nil {
			return reconciliation.RangeBound{}, reconciliation.RangeBound{},
				fmt.Errorf("--from %q is not RFC3339", from)
		}
		endAt, err := time.Parse(time.RFC3339, strings.TrimSpace(to))
		if err != nil {
			return reconciliation.RangeBound{}, reconciliation.RangeBound{},
				fmt.Errorf("--to %q is not RFC3339", to)
		}
		start, end := reconciliation.TimeBound(startAt), reconciliation.TimeBound(endAt)
		if cmp, err := start.Compare(end); err != nil || cmp > 0 {
			return reconciliation.RangeBound{}, reconciliation.RangeBound{},
				fmt.Errorf("scope range %s..%s is not ascending", from, to)
		}
		return start, end, nil
	default:
		return reconciliation.RangeBound{}, reconciliation.RangeBound{},
			fmt.Errorf("--scope-kind must be %q or %q", reconciliation.ScopeHeight, reconciliation.ScopeTime)
	}
}

// parseReconConfirmThresholdN parses the required confirm policy depth N of a
// start request: a positive decimal integer in [1, MaxInt64], the same domain
// as the 005 confirmation policy and the BIGINT receipt storage. There is no
// default: a missing, non-integer, zero or out-of-range value is refused by
// name. A local test value is never claimed as a production threshold.
func parseReconConfirmThresholdN(raw string) (uint64, error) {
	n, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil || n < 1 || n > math.MaxInt64 {
		return 0, fmt.Errorf("--confirm-threshold-n %q is not a decimal integer in [1, %d]", raw, math.MaxInt64)
	}
	return n, nil
}

// parseReconBusinessTypes parses the comma-separated closed business-type set.
func parseReconBusinessTypes(raw string) ([]reconciliation.BusinessType, error) {
	parts := strings.Split(raw, ",")
	if len(parts) == 0 {
		return nil, errors.New("--business-types must name at least one business type")
	}
	seen := make(map[string]struct{}, len(parts))
	out := make([]reconciliation.BusinessType, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name == "" {
			return nil, errors.New("--business-types contains a blank entry")
		}
		businessType := reconciliation.BusinessType(name)
		if !businessType.Known() {
			return nil, fmt.Errorf("unknown business type %q (known: withdrawal, deposit, event-delivery)", name)
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("duplicate business type %q", name)
		}
		seen[name] = struct{}{}
		out = append(out, businessType)
	}
	return out, nil
}

// parseReconUpstreamReceipts parses comma-separated declarations of the shape
// `business_type=source:connected|unconnected`. A business type without a
// declaration is unconnected (never an external-credit claim).
func parseReconUpstreamReceipts(raw string) ([]reconciliation.ChainUpstreamReceiptSource, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	parts := strings.Split(trimmed, ",")
	out := make([]reconciliation.ChainUpstreamReceiptSource, 0, len(parts))
	for _, part := range parts {
		decl := strings.TrimSpace(part)
		if decl == "" {
			return nil, errors.New("--upstream-receipts contains a blank entry")
		}
		name, rest, ok := strings.Cut(decl, "=")
		businessType := reconciliation.BusinessType(strings.TrimSpace(name))
		if !ok || !businessType.Known() {
			return nil, fmt.Errorf("upstream receipt %q is not business_type=source:connected|unconnected", decl)
		}
		source, state, hasState := strings.Cut(rest, ":")
		connected := true
		if hasState {
			switch state {
			case "connected":
				connected = true
			case "unconnected":
				connected = false
			case "":
				return nil, fmt.Errorf("upstream receipt %q has an empty connection state", decl)
			default:
				return nil, fmt.Errorf("upstream receipt %q connection state must be connected or unconnected", decl)
			}
		}
		receipt := reconciliation.ChainUpstreamReceiptSource{
			BusinessType: businessType,
			Source:       source,
			Connected:    connected,
		}
		if err := receipt.Validate(); err != nil {
			return nil, err
		}
		out = append(out, receipt)
	}
	return out, nil
}

// reconcileAdminUsage prints the accepted action forms.
func reconcileAdminUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: txharbor reconcile-admin start --chain-id C --scope-kind height|time --from B --to B --business-types withdrawal[,deposit,event-delivery] --confirm-threshold-n N [--task-id UUID] [--upstream-receipts withdrawal=ledger:connected] [--reason R]")
	fmt.Fprintln(w, "       txharbor reconcile-admin scan --task-id UUID")
	fmt.Fprintln(w, "       txharbor reconcile-admin pause --task-id UUID --reason R")
	fmt.Fprintln(w, "       txharbor reconcile-admin resume --task-id UUID [--reason R]")
	fmt.Fprintln(w, "       txharbor reconcile-admin cancel --task-id UUID --reason R")
	fmt.Fprintln(w, "       txharbor reconcile-admin show --task-id UUID")
	fmt.Fprintln(w, "start requires --confirm-threshold-n N (integer >= 1, no default): the confirm policy depth snapshotted into the task's policy_refs as the scan's chain-evidence confirmation basis; a local test value is never a production threshold")
	fmt.Fprintln(w, "scan enumerates candidates from the authoritative PG rows (007 requests by time window; 011 canonical receipts by height window) and, for height scopes, chain-first enumerates the durable indexed transfer facts (bounded read-only) and reports chain transactions with no PG execution row as missing candidates; time scopes stay PG-anchored (no time->height mapping: pending-by-design until a window resolver exists)")
	fmt.Fprintln(w, "budget keys (required positive; no defaults): "+strings.Join([]string{
		config.EnvReconPrincipal,
		config.EnvReconConcurrency,
		config.EnvReconMaxSpanPerClaim,
		config.EnvReconMaxDuration,
		config.EnvReconMaxPGRequests,
		config.EnvReconMaxRPCRequests,
		config.EnvReconLeaseTTL,
		config.EnvReconFreshnessTolerance,
		config.EnvReconMaxTipLag,
		config.EnvReconMaxCandidates,
		config.EnvReconMaxEventRows,
		config.EnvReconSettleLimit,
	}, " "))
}
