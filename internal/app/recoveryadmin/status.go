// status.go implements T051 [US4]: the `recovery-admin status` bounded
// read-only review (FR-019/F13, FR-022/026).
//
// What the command reports, per capability, is the three derived states of
// data-model §4.2 — never a stored boolean and never an invented verdict:
//
//   - restored: an accepted restore-probe evidence row exists for the instance
//     (recovery_evidence.kind='restore_probe'). It implies neither verified
//     nor released and is displayed with its generation and observation time.
//   - verified: the capability's applicable V1–V9 categories (the directly
//     blocking sets of contracts/verification-items.md §1) have a
//     current-generation verification conclusion. unknown/stale/divergent are
//     allowed as conclusions and are displayed verbatim — a non-consistent
//     conclusion is never shown as normal and never counts as a pass.
//   - released: release_valid is re-derived by the single gate evaluation
//     (gate.go Admit) for every recorded (capability, scope) stream. The
//     review re-implements no rule: the released state, the blocking reason
//     and the closed refusal_class are exactly the gate's derived output. An
//     approval without a valid release stays `released=false` (F16: approved
//     is not a state).
//
// Bounded read-only review (F13):
//
//  1. Readable range: rows of the reviewed instance only — its evidence,
//     verification items, gaps, approvals/releases and audit census — with an
//     optional `--scope` narrowing the release evaluation to one canonical
//     scope. Every read carries an instance predicate and a LIMIT; no
//     unbounded full-table scan is issued and the review writes no evidence,
//     gap, instance, approval or release state.
//  2. Budgets come from deployment configuration and are refused by exact key
//     name when missing/invalid (no default, local values are test inputs
//     only): TXHARBOR_RECOVERY_STATUS_TIMEOUT (time), ..._MAX_READS (number of
//     bounded reads/evaluations one pass may issue), ..._MAX_ROWS (rows one
//     bounded read may return). Exhausting any bound refuses the rest of the
//     review, appends one refused audit row and changes no state; a timeout,
//     an exhausted budget and human knowledge are never gap closure or a
//     resumption permission.
//  3. The derived evaluation is the gate's own audited evaluation: each
//     Admit call records one recovery_audit row with request_action=
//     status_review. A review admits no action, grants no release and starts
//     no signing/broadcast/payment/delivery. Phase two (the existing fund
//     gates) is not evaluated here and is never substituted by this review or
//     by any health signal; phase_two_evaluated=false is reported explicitly.
//  4. The caller must be provable (an active identity mapping) and bound to
//     the instance under some participant role; the review reads only the
//     instance it is bound to. TXHARBOR_RECOVERY_GATE_TTL is required for the
//     derived evaluation (the gate has no default TTL) and the control store
//     is opened through the T069 version guard (unknown/incompatible =>
//     control_store_unavailable).
//
// Exit codes: 0 review completed (regardless of the state mix), 1
// refusal/config/budget, 2 usage.
package recoveryadmin

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

const (
	// recoveryActionStatusReview is the recovery_audit action token of the
	// status review and of its gate evaluations (GateRequest.Action).
	recoveryActionStatusReview = "status_review"

	// recoveryStatusAuditFallbackActor is the audit actor label of a refusal
	// whose deployment principal is not configured. It is an audit label only
	// and never authorizes anything.
	recoveryStatusAuditFallbackActor = "system:recovery-status"
)

// errRecoveryStatusBudget marks a review refused because one of its configured
// bounds is exhausted (reads, rows or time). The refusal is audited and changes
// no state; it is never gap closure or a resumption permission.
var errRecoveryStatusBudget = errors.New("recovery status review budget is exhausted")

// recoveryAdminStatus implements `recovery-admin status`.
func recoveryAdminStatus(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			recoveryAdminActionUsage(stdout, recoveryAdminActionByNameOrZero("status"))
			return 0
		}
	}

	fs := flag.NewFlagSet("txharbor recovery-admin status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	instanceFlag := fs.String("instance", "", "recovery instance id (optional; defaults to the bound or single open instance)")
	scopeFlag := fs.String("scope", "", "canonical capability scope filter (optional)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		recoveryAdminActionUsage(stderr, recoveryAdminActionByNameOrZero("status"))
		return 2
	}

	maxReads, maxRows, timeout, err := recoveryStatusResolveBudget(d)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin status: %s\n", logx.Redact(err.Error()))
		return 1
	}

	// The deployment binding never silently overrides --instance, and a bound
	// process reviews the bound instance.
	instanceArg := strings.TrimSpace(*instanceFlag)
	if instanceArg != "" {
		if bound, ok := d.getenvValue(config.EnvRecoveryInstance); ok && strings.TrimSpace(bound) != "" &&
			!recoveryVerifyInstanceBindingMatches(instanceArg, bound) {
			fmt.Fprintf(stderr,
				"txharbor recovery-admin status: --instance %s does not match %s=%s (instance bound execution; refusing)\n",
				instanceArg, config.EnvRecoveryInstance, strings.TrimSpace(bound))
			return 1
		}
		instanceID, err := recoveryVerifyInstanceID(instanceArg)
		if err != nil {
			fmt.Fprintf(stderr, "txharbor recovery-admin status: %s\n", logx.Redact(err.Error()))
			return 2
		}
		instanceArg = instanceID
	} else if bound, ok := d.getenvValue(config.EnvRecoveryInstance); ok && strings.TrimSpace(bound) != "" {
		instanceID, err := recoveryVerifyInstanceID(strings.TrimSpace(bound))
		if err != nil {
			fmt.Fprintf(stderr, "txharbor recovery-admin status: %s=%q is not a UUID\n",
				config.EnvRecoveryInstance, strings.TrimSpace(bound))
			return 1
		}
		instanceArg = instanceID
	}

	env, code := recoveryOpOpen(ctx, d, "status")
	if env == nil {
		return code
	}
	defer env.pool.Close()

	// Without a named/bound instance the review follows the single open
	// instance; when none is open the system is in normal mode and there is
	// nothing to review (no read, no write, no claim).
	if instanceArg == "" {
		openID, ok, err := recoveryOpOpenInstance(ctx, env)
		if err != nil {
			fmt.Fprintf(stderr, "txharbor recovery-admin status: %s\n", logx.Redact(err.Error()))
			return 1
		}
		if !ok {
			fmt.Fprintln(stdout,
				"txharbor recovery-admin status: no open recovery instance; normal mode (the daily runtime is unchanged; zero reads, zero writes, no state claimed)")
			return 0
		}
		instanceArg = openID
	}

	var (
		scopeFilter           string
		scopeFilterCapability recovery.Capability
	)
	if raw := strings.TrimSpace(*scopeFlag); raw != "" {
		scopeFilter, err = recovery.CanonicalScopeHash(raw)
		if err != nil {
			reason := fmt.Sprintf("--scope %q is not a canonical capability scope: %v", raw, err)
			recoveryStatusRefuse(ctx, env, instanceArg, "", string(recovery.RefusalScopeMismatch), reason)
			fmt.Fprintf(stderr, "txharbor recovery-admin status: refused: refusal_class=%s %s (no state changed)\n",
				recovery.RefusalScopeMismatch, logx.Redact(reason))
			return 1
		}
		filterScope, parseErr := recovery.ParseScope(scopeFilter)
		if parseErr != nil {
			// Unreachable: CanonicalScopeHash parsed the same expression.
			reason := fmt.Sprintf("--scope %q cannot be attributed to a capability: %v", raw, parseErr)
			recoveryStatusRefuse(ctx, env, instanceArg, "", string(recovery.RefusalScopeMismatch), reason)
			fmt.Fprintf(stderr, "txharbor recovery-admin status: refused: refusal_class=%s %s (no state changed)\n",
				recovery.RefusalScopeMismatch, logx.Redact(reason))
			return 1
		}
		scopeFilterCapability = filterScope.Capability
	}

	if refusalClass, err := recoveryStatusRequireParticipant(ctx, env, instanceArg); err != nil {
		recoveryStatusRefuse(ctx, env, instanceArg, "", refusalClass, err.Error())
		fmt.Fprintf(stderr, "txharbor recovery-admin status: refused: %s (no state changed)\n", logx.Redact(err.Error()))
		return 1
	}

	ttl, code := recoveryGateTTL(d, "status")
	if code != 0 {
		return code
	}
	ruling, code := recoveryGateEffectClassRuling(d, "status")
	if code != 0 {
		return code
	}
	gate, err := recovery.NewGate(env.store, recovery.GateOptions{
		TTL: ttl, EffectClassRuling: ruling,
		TrustedTarget: recovery.GateTargetBinding{
			TargetGuardKey:        env.targetGuardKey,
			TargetRoleFingerprint: env.targetRoleFingerprint,
		},
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin status: %s\n", logx.Redact(err.Error()))
		return 1
	}

	reviewCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	review := &recoveryStatusReview{
		pool:                  env.pool,
		instanceID:            instanceArg,
		scopeFilter:           scopeFilter,
		scopeFilterCapability: scopeFilterCapability,
		principal:             env.principal,
		operation:             recoveryActionStatusReview + ":" + uuid.NewString(),
		maxReads:              maxReads,
		maxRows:               maxRows,
	}
	if err := review.run(reviewCtx, gate); err != nil {
		refusalClass := ""
		switch {
		case errors.Is(err, recovery.ErrGateControlStoreUnavailable), controlstore.IsControlStoreUnavailable(err):
			refusalClass = string(recovery.RefusalControlStoreUnavailable)
		}
		reason := err.Error()
		if errors.Is(err, errRecoveryStatusBudget) || reviewCtx.Err() != nil {
			reason = "the bounded read-only review budget is exhausted: " + reason +
				" (a timeout, an exhausted budget and human knowledge are never gap closure or a resumption permission)"
		}
		recoveryStatusRefuse(ctx, env, instanceArg, review.operation, refusalClass, reason)
		if refusalClass != "" {
			fmt.Fprintf(stderr, "txharbor recovery-admin status: refused: refusal_class=%s close_ready=unknown %s (no gap/instance/approval/release state changed)\n",
				refusalClass, logx.Redact(reason))
		} else {
			fmt.Fprintf(stderr, "txharbor recovery-admin status: refused: close_ready=unknown %s (no gap/instance/approval/release state changed)\n",
				logx.Redact(reason))
		}
		return 1
	}
	review.render(stdout)
	return 0
}

// recoveryStatusResolveBudget resolves the three deployment-configured bounds
// of one bounded read-only review pass (T003 keys, optional at Load). A
// missing/blank/invalid value is refused by exact key name: there is no
// default and no unbounded review.
func recoveryStatusResolveBudget(d Deps) (maxReads, maxRows int, timeout time.Duration, err error) {
	timeout, err = recoveryStatusPositiveDuration(d, config.EnvRecoveryStatusTimeout)
	if err != nil {
		return 0, 0, 0, err
	}
	maxReads, err = recoveryStatusPositiveInt(d, config.EnvRecoveryStatusMaxReads)
	if err != nil {
		return 0, 0, 0, err
	}
	maxRows, err = recoveryStatusPositiveInt(d, config.EnvRecoveryStatusMaxRows)
	if err != nil {
		return 0, 0, 0, err
	}
	return maxReads, maxRows, timeout, nil
}

// recoveryStatusPositiveDuration refuses a missing or non-positive duration by
// exact key name.
func recoveryStatusPositiveDuration(d Deps, key string) (time.Duration, error) {
	raw, ok := d.getenvValue(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return 0, fmt.Errorf("%s is required for the bounded read-only review (not configured); refusing", key)
	}
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s=%q is not a positive duration; refusing", key, raw)
	}
	return value, nil
}

// recoveryStatusPositiveInt refuses a missing or non-positive integer by exact
// key name.
func recoveryStatusPositiveInt(d Deps, key string) (int, error) {
	raw, ok := d.getenvValue(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return 0, fmt.Errorf("%s is required for the bounded read-only review (not configured); refusing", key)
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s=%q is not a positive decimal integer; refusing", key, raw)
	}
	return value, nil
}

// recoveryStatusRequireParticipant enforces the read trust boundary: the
// authenticated principal must be provable (an active identity mapping) and
// bound to the reviewed instance under some participant role. The returned
// class is the closed-set class for the audited refusal (a caller that cannot
// be attributed is a control-store-unavailable condition when the store read
// itself failed).
func recoveryStatusRequireParticipant(ctx context.Context, env *recoveryOpEnv, instanceID string) (string, error) {
	if _, ok, err := env.store.ActivePersonID(ctx, env.principal); err != nil {
		return string(recovery.RefusalControlStoreUnavailable), fmt.Errorf("resolve the principal identity mapping: %w", err)
	} else if !ok {
		return "", fmt.Errorf("%s has no active identity mapping; register the mapping first (a principal without a mapping cannot prove a person)",
			env.principal)
	}
	bindings, err := env.store.ParticipantBindings(ctx, instanceID)
	if err != nil {
		return string(recovery.RefusalControlStoreUnavailable), fmt.Errorf("read participant bindings: %w", err)
	}
	for _, binding := range bindings {
		if binding.Principal == env.principal {
			return "", nil
		}
	}
	return "", fmt.Errorf("%s is not bound to instance %s under any participant role; the review reads only the instance it is bound to",
		env.principal, instanceID)
}

// recoveryStatusWriteRefuse appends one refused audit row of the status review
// (the only write path of this command; it changes no gap/instance/approval/
// release state). Best effort: the refusal is already decided.
func recoveryStatusRefuse(ctx context.Context, env *recoveryOpEnv, instanceID, operation, refusalClass, reason string) {
	if env == nil || env.pool == nil {
		return
	}
	detail, err := json.Marshal(map[string]any{
		"reason":        boundedRecoveryCLIDetail(reason),
		"operation_id":  operation,
		"state_changed": false,
	})
	if err != nil {
		return
	}
	actor := env.principal
	if strings.TrimSpace(actor) == "" {
		actor = recoveryStatusAuditFallbackActor
	}
	_ = controlstore.WriteAudit(ctx, env.pool, controlstore.AuditRecord{
		InstanceID:   instanceID,
		Actor:        actor,
		Action:       recoveryActionStatusReview,
		Detail:       detail,
		Result:       controlstore.AuditRefused,
		RefusalClass: refusalClass,
		OperationID:  operation,
	})
}

// recoveryStatusInstance is the read instance row of one review.
type recoveryStatusInstance struct {
	kind              string
	state             string
	generation        int64
	evidenceHash      string
	openedBy          string
	entryChains       []uint64
	entryChainVersion *int32
}

// recoveryStatusRestoreFacts is the restore-probe census of one review.
type recoveryStatusRestoreFacts struct {
	probeCount  int64
	generation  int64
	artifactRef string
	observedAt  time.Time
}

// recoveryStatusGap is one open/escalated gap line (escalation is not closure).
type recoveryStatusGap struct {
	gapID     string
	objectKey string
	state     string
	affected  string
}

// recoveryStatusScopeLine is the derived gate evaluation of one
// (capability, scope) stream.
type recoveryStatusScopeLine struct {
	scopeHash    string
	marker       bool
	allowed      bool
	releaseID    string
	phaseTwo     bool
	refusalClass string
	reason       string
}

// recoveryStatusCapability is the per-capability tri-state of one review.
type recoveryStatusCapability struct {
	capability   recovery.Capability
	restored     bool
	verified     bool
	divergent    bool
	verification string
	released     bool
	refusalClass string
	reason       string
	scopes       []recoveryStatusScopeLine
}

// recoveryStatusReview is one bounded read-only review pass over one instance.
// It holds the configured bounds and the collected facts and never writes
// state.
type recoveryStatusReview struct {
	pool        *pgxpool.Pool
	instanceID  string
	scopeFilter string
	// scopeFilterCapability is the capability named by a configured --scope
	// filter: a canonical scope names exactly one capability stream (T050).
	scopeFilterCapability recovery.Capability
	principal             string
	operation             string

	maxReads int
	maxRows  int
	reads    int

	instance     recoveryStatusInstance
	restore      recoveryStatusRestoreFacts
	verification map[string]map[string]int64
	gaps         []recoveryStatusGap
	scopes       []string
	// unattributable holds recorded scopes outside the canonical capability
	// form (legacy opaque strings, non-canonical expressions). They match no
	// canonical stream, are never translated or guessed, and are surfaced so
	// the operator re-approves/re-releases at the canonical scope.
	unattributable []string
	auditCensus    map[string]int64
	capabilities   []recoveryStatusCapability
}

// spend accounts one bounded read/evaluation against the 次数 budget.
func (r *recoveryStatusReview) spend() error {
	r.reads++
	if r.reads > r.maxReads {
		return fmt.Errorf(
			"%w: the review issued %d reads/evaluations, above %s=%d; narrow --scope/--instance and retry",
			errRecoveryStatusBudget, r.reads, config.EnvRecoveryStatusMaxReads, r.maxReads)
	}
	return nil
}

// checkRows enforces the 资源 budget of one bounded read (each read is issued
// with LIMIT maxRows+1, so an overflow is detectable without an unbounded
// scan).
func (r *recoveryStatusReview) checkRows(rows int) error {
	if rows > r.maxRows {
		return fmt.Errorf(
			"%w: a bounded read returned %d rows, above %s=%d; narrow the range and retry",
			errRecoveryStatusBudget, rows, config.EnvRecoveryStatusMaxRows, r.maxRows)
	}
	return nil
}

// run performs the bounded reads and the derived release evaluations.
func (r *recoveryStatusReview) run(ctx context.Context, gate *recovery.Gate) error {
	if err := r.loadInstance(ctx); err != nil {
		return err
	}
	if err := r.loadRestore(ctx); err != nil {
		return err
	}
	if err := r.loadVerification(ctx); err != nil {
		return err
	}
	if err := r.loadGaps(ctx); err != nil {
		return err
	}
	if err := r.loadScopes(ctx); err != nil {
		return err
	}
	if err := r.loadAuditCensus(ctx); err != nil {
		return err
	}
	return r.evaluate(ctx, gate)
}

// loadInstance reads the reviewed instance row (one bounded read).
func (r *recoveryStatusReview) loadInstance(ctx context.Context) error {
	if err := r.spend(); err != nil {
		return err
	}
	err := r.pool.QueryRow(ctx,
		`SELECT kind, state, evidence_generation, evidence_hash, opened_by,
                entry_chain_inventory, entry_chain_inventory_version
		   FROM recovery_instance WHERE instance_id = $1`, r.instanceID).
		Scan(&r.instance.kind, &r.instance.state, &r.instance.generation,
			&r.instance.evidenceHash, &r.instance.openedBy, &r.instance.entryChains,
			&r.instance.entryChainVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("recovery instance %s does not exist; the status review reads one existing instance", r.instanceID)
	}
	if err != nil {
		return fmt.Errorf("read recovery instance %s: %w", r.instanceID, err)
	}
	return nil
}

// loadRestore reads the accepted restore-probe evidence of the instance (the
// `restored` state). A restore probe proves the restored data set passed its
// probes; it never implies verified or released.
func (r *recoveryStatusReview) loadRestore(ctx context.Context) error {
	if err := r.spend(); err != nil {
		return err
	}
	var probeCount int64
	if err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM recovery_evidence WHERE instance_id = $1 AND kind = 'restore_probe'`,
		r.instanceID).Scan(&probeCount); err != nil {
		return fmt.Errorf("read the restore-probe census: %w", err)
	}
	if probeCount == 0 {
		return nil
	}
	if err := r.spend(); err != nil {
		return err
	}
	var (
		generation  int64
		artifactRef string
		observedAt  time.Time
	)
	if err := r.pool.QueryRow(ctx, `
SELECT generation, artifact_ref, observed_at
  FROM recovery_evidence
 WHERE instance_id = $1 AND kind = 'restore_probe'
 ORDER BY generation DESC, created_at DESC, evidence_id DESC
 LIMIT 1`, r.instanceID).Scan(&generation, &artifactRef, &observedAt); err != nil {
		return fmt.Errorf("read the latest restore-probe evidence: %w", err)
	}
	r.restore = recoveryStatusRestoreFacts{
		probeCount: probeCount, generation: generation, artifactRef: artifactRef, observedAt: observedAt,
	}
	return nil
}

// loadVerification reads the current-generation verification conclusions of
// the instance, grouped by category and conclusion (one bounded aggregate
// read). Older-generation conclusions are stale after any accepted evidence
// write and are never displayed as current.
func (r *recoveryStatusReview) loadVerification(ctx context.Context) error {
	if err := r.spend(); err != nil {
		return err
	}
	rows, err := r.pool.Query(ctx, `
SELECT category, conclusion, count(*)
  FROM recovery_verification_item
 WHERE instance_id = $1 AND generation = $2
 GROUP BY category, conclusion
 ORDER BY category, conclusion
 LIMIT $3`, r.instanceID, r.instance.generation, r.maxRows+1)
	if err != nil {
		return fmt.Errorf("read the current-generation verification conclusions: %w", err)
	}
	defer rows.Close()
	counts := make(map[string]map[string]int64)
	read := 0
	for rows.Next() {
		var category, conclusion string
		var count int64
		if err := rows.Scan(&category, &conclusion, &count); err != nil {
			return fmt.Errorf("scan the current-generation verification conclusions: %w", err)
		}
		if counts[category] == nil {
			counts[category] = make(map[string]int64)
		}
		counts[category][conclusion] = count
		read++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read the current-generation verification conclusions: %w", err)
	}
	if err := r.checkRows(read); err != nil {
		return err
	}
	r.verification = counts
	return nil
}

// loadGaps reads the open/escalated gaps of the instance (one bounded read).
// Escalation is not closure: an escalated gap keeps blocking its capabilities.
func (r *recoveryStatusReview) loadGaps(ctx context.Context) error {
	if err := r.spend(); err != nil {
		return err
	}
	rows, err := r.pool.Query(ctx, `
SELECT gap_id::text, object_key, state, affected_capabilities
  FROM recovery_gap
 WHERE instance_id = $1 AND state IN ('open', 'escalated')
 ORDER BY object_key, gap_id
 LIMIT $2`, r.instanceID, r.maxRows+1)
	if err != nil {
		return fmt.Errorf("read the open/escalated gap list: %w", err)
	}
	defer rows.Close()
	var gaps []recoveryStatusGap
	for rows.Next() {
		var (
			gap      recoveryStatusGap
			affected []string
		)
		if err := rows.Scan(&gap.gapID, &gap.objectKey, &gap.state, &affected); err != nil {
			return fmt.Errorf("scan the open/escalated gap list: %w", err)
		}
		gap.affected = strings.Join(affected, ",")
		gaps = append(gaps, gap)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read the open/escalated gap list: %w", err)
	}
	if err := r.checkRows(len(gaps)); err != nil {
		return err
	}
	r.gaps = gaps
	return nil
}

// loadScopes reads the distinct decision scopes recorded for the instance
// (release and approval streams, one bounded read). These are the streams the
// derived release evaluation runs against; `--scope` narrows the range.
func (r *recoveryStatusReview) loadScopes(ctx context.Context) error {
	if err := r.spend(); err != nil {
		return err
	}
	rows, err := r.pool.Query(ctx, `
SELECT scope_hash FROM (
    SELECT scope_hash FROM recovery_release WHERE instance_id = $1
    UNION
    SELECT scope_hash FROM recovery_approval WHERE instance_id = $1
) streams
WHERE ($2 = '' OR scope_hash = $2)
ORDER BY scope_hash
LIMIT $3`, r.instanceID, r.scopeFilter, r.maxRows+1)
	if err != nil {
		return fmt.Errorf("read the recorded decision scopes: %w", err)
	}
	defer rows.Close()
	var scopes []string
	for rows.Next() {
		var scope string
		if err := rows.Scan(&scope); err != nil {
			return fmt.Errorf("scan the recorded decision scopes: %w", err)
		}
		scopes = append(scopes, scope)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read the recorded decision scopes: %w", err)
	}
	if err := r.checkRows(len(scopes)); err != nil {
		return err
	}
	r.scopes = scopes
	return nil
}

// loadAuditCensus reads the instance's audit-result census (one bounded
// aggregate read). It is a review fact only: no control decision reads it.
func (r *recoveryStatusReview) loadAuditCensus(ctx context.Context) error {
	if err := r.spend(); err != nil {
		return err
	}
	rows, err := r.pool.Query(ctx, `
SELECT result, count(*) FROM recovery_audit
 WHERE instance_id = $1
 GROUP BY result
 ORDER BY result
 LIMIT $2`, r.instanceID, r.maxRows+1)
	if err != nil {
		return fmt.Errorf("read the audit census: %w", err)
	}
	defer rows.Close()
	census := make(map[string]int64)
	read := 0
	for rows.Next() {
		var result string
		var count int64
		if err := rows.Scan(&result, &count); err != nil {
			return fmt.Errorf("scan the audit census: %w", err)
		}
		census[result] = count
		read++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read the audit census: %w", err)
	}
	if err := r.checkRows(read); err != nil {
		return err
	}
	r.auditCensus = census
	return nil
}

// evaluate runs the single derived release evaluation for every capability at
// every recorded canonical scope of that capability. A recorded scope outside
// the canonical capability form (T050) is never translated or evaluated: it
// matches no canonical stream and is surfaced as unattributable for
// re-approval/re-release. When a capability records no scope and the instance
// records canonical scopes of exactly one chain, the review probes that
// capability's canonical scope (a value never written) so the derived class is
// exactly the gate's evaluation order (dependencies at their mapped scopes,
// isolation, gaps, then no_release). Each evaluation is audited by the gate as
// request_action=status_review; the review itself writes no decision.
func (r *recoveryStatusReview) evaluate(ctx context.Context, gate *recovery.Gate) error {
	byCapability, chains := r.canonicalScopes()
	var probeChain uint64
	if len(chains) == 1 {
		for chain := range chains {
			probeChain = chain
		}
	}
	for _, capability := range recovery.KnownCapabilities() {
		line := recoveryStatusCapability{
			capability: capability,
			restored:   r.restore.probeCount > 0,
		}
		line.verified, line.verification, line.divergent = r.capabilityVerified(capability)

		scopes := byCapability[capability]
		marker := false
		switch {
		case r.scopeFilter != "":
			// A configured --scope is the authoritative range: evaluate exactly
			// the single capability stream it names, even when no decision
			// stream is recorded at it.
			scopes = nil
			if r.scopeFilterCapability == capability {
				scopes = []string{r.scopeFilter}
			}
		case len(scopes) == 0 && probeChain != 0:
			// No stream is recorded for this capability: probe its canonical
			// capability scope on the deployment chain the instance recorded.
			probe, err := recovery.CapabilityScope(probeChain, capability)
			if err != nil {
				return err
			}
			scopes = []string{probe}
			marker = true
		}

		for _, scope := range scopes {
			if err := r.spend(); err != nil {
				return err
			}
			decision, err := gate.Admit(ctx, recovery.GateRequest{
				InstanceID:  r.instanceID,
				Capability:  capability,
				ScopeHash:   scope,
				Actor:       r.principal,
				OperationID: r.operation,
				Action:      recoveryActionStatusReview,
			})
			if err != nil {
				return err
			}
			scopeLine := recoveryStatusScopeLine{
				scopeHash:    scope,
				marker:       marker,
				allowed:      decision.Allowed,
				releaseID:    decision.DecisionRef,
				phaseTwo:     decision.PhaseTwoEvaluated,
				refusalClass: string(decision.RefusalClass),
				reason:       logx.Redact(decision.Reason),
			}
			line.scopes = append(line.scopes, scopeLine)
			if decision.Allowed {
				line.released = true
			}
		}
		if !line.released {
			switch {
			case len(line.scopes) > 0:
				line.refusalClass = line.scopes[0].refusalClass
				line.reason = line.scopes[0].reason
			case r.scopeFilter != "":
				line.refusalClass = string(recovery.RefusalNoRelease)
				line.reason = fmt.Sprintf(
					"the configured --scope names capability %s; capability %s was not evaluated at a scope it was never granted at",
					r.scopeFilterCapability, capability)
			default:
				line.refusalClass = string(recovery.RefusalNoRelease)
				line.reason = "no release decision exists for this capability at any canonical scope (no canonical scope was recorded and none can be probed)"
			}
		}
		r.capabilities = append(r.capabilities, line)
	}
	return nil
}

// canonicalScopes attributes the recorded decision scopes to the capability
// they canonically name. A scope that does not parse, or that parses but is not
// in canonical form, is never mapped onto a capability: it is recorded as
// unattributable (re-approval/re-release required) and matches no canonical
// stream. The returned chains set bounds the probe scope: a probe is only
// defensible when the instance recorded scopes of exactly one chain.
func (r *recoveryStatusReview) canonicalScopes() (map[recovery.Capability][]string, map[uint64]bool) {
	byCapability := make(map[recovery.Capability][]string)
	chains := make(map[uint64]bool)
	for _, raw := range r.scopes {
		scope, err := recovery.ParseScope(raw)
		if err != nil {
			r.unattributable = append(r.unattributable, raw)
			continue
		}
		canonical, err := scope.Canonical()
		if err != nil || canonical != raw {
			r.unattributable = append(r.unattributable, raw)
			continue
		}
		byCapability[scope.Capability] = append(byCapability[scope.Capability], raw)
		chains[scope.ChainID] = true
	}
	return byCapability, chains
}

// statusCapabilityCategories is the display-side map of the V1-V9 categories
// whose current-generation conclusion the `verified` state of each capability
// reports (contracts/verification-items.md §1 "直接阻塞能力"). It is
// presentation only: it is never a release verdict, and the gate remains the
// single judgment.
var statusCapabilityCategories = map[recovery.Capability][]recovery.VerificationCategory{
	recovery.CapabilityQuery: {
		recovery.VerificationV9,
	},
	recovery.CapabilityChainScan: {
		recovery.VerificationV1, recovery.VerificationV9,
	},
	recovery.CapabilityDepositConfirmation: {
		recovery.VerificationV1, recovery.VerificationV9,
	},
	recovery.CapabilityExistingWithdrawalRecovery: {
		recovery.VerificationV2, recovery.VerificationV3, recovery.VerificationV4,
		recovery.VerificationV7, recovery.VerificationV8, recovery.VerificationV9,
	},
	recovery.CapabilityNewWithdrawalCreation: {
		recovery.VerificationV2, recovery.VerificationV7, recovery.VerificationV8, recovery.VerificationV9,
	},
	recovery.CapabilityEventPublishing: {
		recovery.VerificationV5, recovery.VerificationV7, recovery.VerificationV8, recovery.VerificationV9,
	},
	recovery.CapabilityEventConsuming: {
		recovery.VerificationV6, recovery.VerificationV7, recovery.VerificationV8, recovery.VerificationV9,
	},
}

// capabilityVerified derives the capability's `verified` state from the
// current-generation conclusions: every applicable category must have at least
// one conclusion; unknown/stale are allowed (they just cannot be released) and
// a divergent conclusion is surfaced as a contradiction, never as normal.
func (r *recoveryStatusReview) capabilityVerified(capability recovery.Capability) (verified bool, detail string, divergent bool) {
	categories := statusCapabilityCategories[capability]
	parts := make([]string, 0, len(categories))
	verified = true
	for _, category := range categories {
		counts := r.verification[string(category)]
		if len(counts) == 0 {
			verified = false
			parts = append(parts, fmt.Sprintf("%s:none@generation=%d", category, r.instance.generation))
			continue
		}
		conclusions := make([]string, 0, len(counts))
		for conclusion := range counts {
			conclusions = append(conclusions, conclusion)
		}
		sort.Strings(conclusions)
		rendered := make([]string, 0, len(conclusions))
		for _, conclusion := range conclusions {
			rendered = append(rendered, fmt.Sprintf("%s=%d", conclusion, counts[conclusion]))
			if conclusion == string(recovery.ConclusionDivergent) {
				divergent = true
			}
		}
		parts = append(parts, fmt.Sprintf("%s:%s", category, strings.Join(rendered, "+")))
	}
	return verified, strings.Join(parts, ","), divergent
}

// render prints the completed review: the instance summary, the per-capability
// tri-state with the derived blocking reason and refusal class, the gap list
// and the honesty notes. Nothing here writes.
func (r *recoveryStatusReview) render(w io.Writer) {
	scopeFilter := "(all recorded scopes)"
	if r.scopeFilter != "" {
		scopeFilter = r.scopeFilter
	}
	fmt.Fprintf(w,
		"txharbor recovery-admin status: instance=%s kind=%s state=%s generation=%d evidence_hash=%s opened_by=%s principal=%s review_operation_id=%s scope_filter=%s reads=%d/%d max_rows=%d mode=bounded_read_only_review close_ready=unknown (status is not an atomic close guard)\n",
		r.instanceID, r.instance.kind, r.instance.state, r.instance.generation, r.instance.evidenceHash,
		r.instance.openedBy, r.principal, recoveryOpDisplayID(r.operation), scopeFilter,
		r.reads, r.maxReads, r.maxRows)

	if r.restore.probeCount > 0 {
		fmt.Fprintf(w,
			"  restored=true restore_probe_count=%d latest_probe_generation=%d latest_probe_observed_at=%s latest_probe_artifact=%s (restored proves the restore probes passed; it does not imply verified or released)\n",
			r.restore.probeCount, r.restore.generation,
			r.restore.observedAt.UTC().Format(time.RFC3339), logx.Redact(r.restore.artifactRef))
	} else {
		fmt.Fprintln(w,
			"  restored=false reason=\"no accepted restore-probe evidence for this instance; nothing proves the data set was restored (restored is never inferred from connectivity or a status field)\"")
	}
	if r.instance.entryChainVersion != nil && *r.instance.entryChainVersion == 1 && controlstore.ValidateEntryChainInventory(r.instance.entryChains) == nil {
		fmt.Fprintf(w, "  bound_entry_chain_inventory_version=1 bound_entry_chains=%v (deployment-complete snapshot; status scope results below remain partial observations, not close_ready)\n", r.instance.entryChains)
	} else {
		fmt.Fprintln(w, "  bound_entry_chain_inventory=unknown (legacy-null or malformed; no inventory is inferred from release rows); close_ready=unknown")
	}

	for _, capability := range r.capabilities {
		refusalClass := displayOrNone(capability.refusalClass)
		if capability.released {
			refusalClass = "(none)"
		}
		fmt.Fprintf(w,
			"  capability=%s restored=%t verified=%t released=%t refusal_class=%s verification=%s reason=%s\n",
			capability.capability, capability.restored, capability.verified, capability.released,
			refusalClass, capability.verification, displayOrNone(capability.reason))
		for _, scope := range capability.scopes {
			marker := ""
			if scope.marker {
				marker = " unrecorded_marker_scope=true"
			}
			fmt.Fprintf(w,
				"    release_scope=%s%s released=%t refusal_class=%s scope_partial=true release_id=%s phase_two_evaluated=%t reason=%s\n",
				scope.scopeHash, marker, scope.allowed, displayOrNone(scope.refusalClass),
				displayOrNone(scope.releaseID), scope.phaseTwo, displayOrNone(scope.reason))
		}
		if capability.divergent {
			fmt.Fprintf(w,
				"    warning: capability=%s has divergent current-generation verification conclusion(s); a divergent conclusion is never a pass and is never displayed as normally consistent\n",
				capability.capability)
		}
	}

	for _, gap := range r.gaps {
		fmt.Fprintf(w,
			"  gap gap_id=%s object_key=%s state=%s affected_capabilities=%s (escalation is not closure; only new evidence closes a gap)\n",
			gap.gapID, gap.objectKey, gap.state, displayOrNone(gap.affected))
	}

	results := make([]string, 0, len(r.auditCensus))
	for result := range r.auditCensus {
		results = append(results, result)
	}
	sort.Strings(results)
	for _, result := range results {
		fmt.Fprintf(w, "  audit result=%s count=%d\n", result, r.auditCensus[result])
	}

	fmt.Fprintln(w,
		"  note: restored/verified/released are derived states of this instance at generation "+strconv.FormatInt(r.instance.generation, 10)+
			"; `approved` is not a state (an approval is a decision record, never a release). This review writes no gap/instance/approval/release state and grants nothing.")
	fmt.Fprintln(w,
		"  note: released is the phase-one recovery evaluation only. The existing fund gates stay independently enforced at the real action site (phase two) and are never substituted by this review or by any health signal; phase_two_evaluated=false means phase two remains the action site's obligation.")
	if len(r.unattributable) > 0 {
		fmt.Fprintf(w,
			"  note: %d recorded scope(s) are not canonical capability scopes; they match no canonical stream (T050) and are never translated or matched by guessing — re-approval and re-release at the canonical capability scope are required.\n",
			len(r.unattributable))
		for i, scope := range r.unattributable {
			if i >= r.maxRows {
				fmt.Fprintf(w, "  unattributable_scope=... (%d more not shown)\n", len(r.unattributable)-i)
				break
			}
			fmt.Fprintf(w, "  unattributable_scope=%s\n", logx.Redact(scope))
		}
	}
	if r.scopeFilter == "" && len(r.scopes) == 0 {
		fmt.Fprintln(w,
			"  note: no release/approval scope is recorded for this instance; no canonical scope exists to evaluate, so every capability reports released=false with no_release.")
	}
	if r.scopeFilter != "" {
		fmt.Fprintf(w,
			"  note: the review evaluated only the %s stream named by --scope %s; other capabilities are not evaluated at a scope they were never granted at.\n",
			r.scopeFilterCapability, r.scopeFilter)
	}
	fmt.Fprintln(w,
		"  note: the bounded review bounds (time/reads/rows) come from deployment configuration; exhausting any of them refuses the rest of the review, is audited, and changes no gap/instance/approval/release state. A timeout, an exhausted budget and human knowledge are never gap closure or a resumption permission.")
}
