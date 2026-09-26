// us2admin.go implements the T023 US2 operator surface of reconcile-admin:
// `claim`, `dispose` and the discrepancy half of `show`. It is a thin wiring
// layer over the T022 production paths (Store.ClaimDiscrepancy /
// Store.DisposeDiscrepancy) and the T009 authorization evaluator; no lifecycle
// rule is reimplemented here.
//
// Boundaries (FR-010/011/012/013/020/023, Q2):
//
//   - the principal is always the authenticated caller binding
//     (TXHARBOR_RECON_PRINCIPAL, the ConfigPrincipal carrier); --operator,
//     --reason and --operation-id are audit/idempotency carriage only and can
//     never authorize anything;
//   - a claim records responsibility only. It never conveys a disposal,
//     closure or downstream execution right; the specific dispose permission
//     is what gates dispose;
//   - dispose carries the UNIQUE idempotency_key (the persistent idempotency
//     of one disposition): a repeat reads the recorded row back with zero side
//     effects, a key reused with different input is operation_conflict with
//     zero writes;
//   - reuse_recovery only records a reference to an existing entry point
//     (014 never executes it and never substitutes its gates); new_fix_rule
//     stays dry_run-only and its T009 action remains unapproved, so the
//     evaluator refuses it rather than this CLI bypassing that decision;
//   - `show` is strictly read-only: an allowed query writes no audit row,
//     refusals are audited by the evaluator, and the ticket is never rendered
//     as verified or closed by disposal (disposed != reverified != closed).
package reconcileadmin

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/reconciliation"
)

// errReconcileOperationConflict means an operation_id was reused with
// different input; no write was performed (the 011/013 shape).
var errReconcileOperationConflict = errors.New("operation_conflict")

// parseReconcileDiscrepancyID validates a CLI-supplied discrepancy id and
// returns its canonical form. It performs no I/O.
func parseReconcileDiscrepancyID(raw string) (string, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("discrepancy id %q is not a UUID", raw)
	}
	return parsed.String(), nil
}

// parseReconcileOperationID validates the required operation_id carriage: a
// bounded, control-free identity used for persistent idempotency and audit.
// It is never an authorization input.
func parseReconcileOperationID(raw string) (string, error) {
	operation := strings.TrimSpace(raw)
	if operation == "" {
		return "", errors.New("--operation-id is required")
	}
	if len(operation) > 128 || strings.ContainsAny(operation, "\x00\n\r\t") {
		return "", errors.New("--operation-id must be 1..128 characters without control characters")
	}
	return operation, nil
}

// reconcileAuditActor renders the authenticated principal for the recon_audit
// actor column (1..128 bytes; the T018 writer convention). It is an audit
// annotation only; permission evaluation always uses the full identity.
func reconcileAuditActor(principal reconciliation.Principal) string {
	actor := principal.String()
	if len(actor) > 128 {
		actor = actor[:128]
	}
	return actor
}

// reconcileAuthScopeText renders a compact, secret-free scope description for
// operator output.
func reconcileAuthScopeText(scope reconciliation.AuthScope) string {
	types := make([]string, len(scope.BusinessTypes))
	for i, businessType := range scope.BusinessTypes {
		types[i] = string(businessType)
	}
	rangeText := "unbounded"
	if scope.RangeStart != nil && scope.RangeEnd != nil {
		rangeText = fmt.Sprintf("%d..%d", *scope.RangeStart, *scope.RangeEnd)
	}
	return fmt.Sprintf("chain=%s kind=%s range=%s business_types=%s",
		scope.ChainID, scope.Kind, rangeText, strings.Join(types, ","))
}

// reconcileClaimOperationSuffix is the audit-carriage marker of one claim
// operation inside the claim audit row written by T022. T022's claim request
// has no idempotency-key field (its durable safety property is the single-owner
// state CAS), so the local-privileged caller appends the bounded operation_id
// to the recorded reason exactly once; that suffix is the persistent
// idempotency key of the claim operation.
const reconcileClaimOperationSuffix = " operation_id="

// reconcileClaimAuditReason renders the claim audit reason with the operation
// carriage appended, refusing when the bounded audit reason limit would be
// exceeded (T022 validates reason <= 1024).
func reconcileClaimAuditReason(reason, operationID string) (string, error) {
	combined := strings.TrimSpace(reason) + reconcileClaimOperationSuffix + operationID
	if len(combined) > 1024 {
		return "", errors.New("claim reason plus the operation_id carriage exceeds the 1024-character audit reason limit")
	}
	return combined, nil
}

// reconcileClaimOperation is the recorded claim operation read back on a
// repeated operation_id.
type reconcileClaimOperation struct {
	Actor         string
	DiscrepancyID string
	Owner         string
	From          string
	To            string
}

// readClaimOperationSQL finds the claim audit row carrying one operation
// carriage. The suffix match is exact (the operation id is control-free), and a
// hit is proof the claim committed: refused attempts never carry the suffix.
const readClaimOperationSQL = `
SELECT actor, COALESCE(target->>'discrepancy_id', ''), COALESCE(target->>'owner', ''),
       COALESCE(target->>'from', ''), COALESCE(target->>'to', '')
FROM recon_audit
WHERE action = 'claim' AND right(reason, length($1)) = $1
ORDER BY audit_id DESC
LIMIT 1`

// readClaimOperation reads the recorded claim operation of one operation_id.
func (e *reconAdminEnv) readClaimOperation(ctx context.Context, operationID string) (reconcileClaimOperation, bool, error) {
	var recorded reconcileClaimOperation
	err := e.pool.QueryRow(ctx, readClaimOperationSQL, reconcileClaimOperationSuffix+operationID).
		Scan(&recorded.Actor, &recorded.DiscrepancyID, &recorded.Owner, &recorded.From, &recorded.To)
	if errors.Is(err, pgx.ErrNoRows) {
		return recorded, false, nil
	}
	if err != nil {
		return recorded, false, fmt.Errorf("read claim operation record: %w", err)
	}
	return recorded, true, nil
}

// reconcileAdminClaim implements `claim(discrepancy_id)` for the authenticated
// principal through the T022 production path (US2-1/US2-2, FR-011/012):
//
//   - --operator/--reason/--operation-id are required audit carriage; the
//     principal (and therefore the claim owner) comes only from
//     TXHARBOR_RECON_PRINCIPAL, never from a flag;
//   - T022's single-owner CAS refuses a competing claimer with the observed
//     owner and audits every refusal; a claim is responsibility only and never
//     an execution/disposal right;
//   - the operation_id is the persistent idempotency key of one claim
//     operation: a repeat converges on the recorded claim with zero writes
//     (same ticket) or is an operation_conflict (different input or a
//     different caller/ticket).
func reconcileAdminClaim(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("reconcile-admin claim", flag.ContinueOnError)
	fs.SetOutput(stderr)
	discrepancyID := fs.String("discrepancy-id", "", "discrepancy to claim (required)")
	operator := fs.String("operator", "", "declared operator identity (required, audit annotation only)")
	reason := fs.String("reason", "", "operator reason (required, audit annotation only)")
	operationID := fs.String("operation-id", "", "idempotent operation identity (required, audit carriage only)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*discrepancyID) == "" || strings.TrimSpace(*operator) == "" ||
		strings.TrimSpace(*reason) == "" || strings.TrimSpace(*operationID) == "" {
		reconcileAdminUsage(stderr)
		return 2
	}
	id, err := parseReconcileDiscrepancyID(*discrepancyID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}
	operation, err := parseReconcileOperationID(*operationID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}
	operatorName := strings.TrimSpace(*operator)
	if len(operatorName) > 128 {
		fmt.Fprintln(stderr, "txharbor reconcile-admin: --operator must be 1..128 characters")
		return 2
	}
	auditReason, err := reconcileClaimAuditReason(*reason, operation)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}

	env, code := reconcileAdminOpen(ctx, d)
	if env == nil {
		return code
	}
	defer env.pool.Close()

	recorded, found, err := env.readClaimOperation(ctx, operation)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if found {
		switch {
		case recorded.Actor != reconcileAuditActor(env.principal):
			fmt.Fprintf(stderr, "txharbor reconcile-admin: operation_conflict operation_id=%s was already used by another principal\n", operation)
			return 1
		case recorded.DiscrepancyID != id:
			fmt.Fprintf(stderr, "txharbor reconcile-admin: operation_conflict operation_id=%s already recorded discrepancy_id=%s\n",
				operation, recorded.DiscrepancyID)
			return 1
		}
		fmt.Fprintf(stdout,
			"txharbor reconcile-admin: claim discrepancy_id=%s from=%s to=%s owner=%s principal=%s disposal_right=false operation_id=%s replay=true\n",
			recorded.DiscrepancyID, recorded.From, recorded.To, recorded.Owner, env.principal, operation)
		return 0
	}

	result, err := env.store.ClaimDiscrepancy(ctx, reconciliation.DiscrepancyClaimRequest{
		DiscrepancyID: id,
		Principal:     env.principal,
		Authorizer:    env.evaluator,
		Operator:      operatorName,
		Reason:        auditReason,
	})
	if err != nil {
		switch {
		case errors.Is(err, reconciliation.ErrDiscrepancyUnauthorized):
			fmt.Fprintf(stderr, "txharbor reconcile-admin: claim refused (unauthorized): %s\n", logx.Redact(err.Error()))
		case errors.Is(err, reconciliation.ErrDiscrepancyClaimTaken):
			fmt.Fprintf(stderr, "txharbor reconcile-admin: claim refused (already claimed): %s\n", logx.Redact(err.Error()))
		default:
			fmt.Fprintf(stderr, "txharbor reconcile-admin: claim refused: %s\n", logx.Redact(err.Error()))
		}
		return 1
	}
	fmt.Fprintf(stdout,
		"txharbor reconcile-admin: claim discrepancy_id=%s from=%s to=%s owner=%s reopen_count=%d principal=%s disposal_right=false operation_id=%s replay=false\n",
		result.DiscrepancyID, result.From, result.To, result.Owner, result.ReopenCount, env.principal, operation)
	return 0
}

// reconcileAdminDispose implements `dispose(discrepancy_id, kind, action_ref,
// result)` through the T022 production path (FR-010/013/016/020/023):
//
//   - authorization runs first inside T022 (T009 evaluator, default deny) and
//     claim ownership never grants it: the specific dispose permission of the
//     kind is what gates the path;
//   - --operation-id is the persistent idempotency carriage (T022's UNIQUE
//     idempotency_key): a repeat reads the recorded disposition back with zero
//     side effects, a reused key with different input is operation_conflict;
//   - reuse_recovery only records a reference to an existing entry point
//     (e.g. "txlifecycle.UnknownRecovery", "events-admin replay"): 014 never
//     executes it and never substitutes its gates (FR-020);
//   - new_fix_rule stays dry_run-only in this phase (FR-023) and its T009
//     action remains unapproved, so the evaluator refuses it: this CLI never
//     bypasses that decision.
func reconcileAdminDispose(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("reconcile-admin dispose", flag.ContinueOnError)
	fs.SetOutput(stderr)
	discrepancyID := fs.String("discrepancy-id", "", "discrepancy to dispose (required)")
	kindRaw := fs.String("kind", "", "disposition kind: ack_only|reuse_recovery|new_fix_rule (required)")
	actionRef := fs.String("action-ref", "", "existing entry point the disposition references (required for reuse_recovery/new_fix_rule; 014 never executes it)")
	resultRaw := fs.String("result", "", "optional recorded outcome: done|refused|failed (new_fix_rule is dry_run-only)")
	evidenceRef := fs.String("evidence-ref", "", "optional bounded evidence annotation")
	operator := fs.String("operator", "", "declared operator identity (required, audit annotation only)")
	reason := fs.String("reason", "", "operator reason (required, audit annotation only)")
	operationID := fs.String("operation-id", "", "idempotent operation identity (required, persistent idempotency)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*discrepancyID) == "" || strings.TrimSpace(*kindRaw) == "" ||
		strings.TrimSpace(*operator) == "" || strings.TrimSpace(*reason) == "" || strings.TrimSpace(*operationID) == "" {
		reconcileAdminUsage(stderr)
		return 2
	}
	id, err := parseReconcileDiscrepancyID(*discrepancyID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}
	kind := reconciliation.DispositionKind(strings.TrimSpace(*kindRaw))
	if !kind.Valid() {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: --kind must be %q, %q or %q\n",
			reconciliation.DispositionAckOnly, reconciliation.DispositionReuseRecovery, reconciliation.DispositionNewFixRule)
		return 2
	}
	reference := strings.TrimSpace(*actionRef)
	if kind != reconciliation.DispositionAckOnly && reference == "" {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: --action-ref is required for disposition kind %q (an existing entry point; 014 never executes it)\n", kind)
		return 2
	}
	result := reconciliation.DispositionResult(strings.TrimSpace(*resultRaw))
	if result != "" && !result.Valid() {
		fmt.Fprintln(stderr, "txharbor reconcile-admin: --result must be done|refused|failed (new_fix_rule is dry_run-only)")
		return 2
	}
	if kind == reconciliation.DispositionNewFixRule && result != "" && result != reconciliation.DispositionDryRun {
		fmt.Fprintln(stderr, "txharbor reconcile-admin: new_fix_rule is dry_run-only in this phase (FR-023)")
		return 2
	}
	if kind != reconciliation.DispositionNewFixRule && result == reconciliation.DispositionDryRun {
		fmt.Fprintln(stderr, "txharbor reconcile-admin: --result dry_run is only valid for new_fix_rule")
		return 2
	}
	operation, err := parseReconcileOperationID(*operationID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: %s\n", logx.Redact(err.Error()))
		return 2
	}

	env, code := reconcileAdminOpen(ctx, d)
	if env == nil {
		return code
	}
	defer env.pool.Close()

	disposition, err := env.store.DisposeDiscrepancy(ctx, reconciliation.DiscrepancyDisposeRequest{
		DiscrepancyID:  id,
		Kind:           kind,
		ActionRef:      reference,
		Result:         result,
		Operator:       strings.TrimSpace(*operator),
		Reason:         strings.TrimSpace(*reason),
		EvidenceRef:    strings.TrimSpace(*evidenceRef),
		IdempotencyKey: operation,
		Principal:      env.principal,
		Authorizer:     env.evaluator,
	})
	if err != nil {
		switch {
		case errors.Is(err, reconciliation.ErrIdempotencyKeyReused):
			fmt.Fprintf(stderr, "txharbor reconcile-admin: operation_conflict operation_id=%s\n", operation)
		case errors.Is(err, reconciliation.ErrDiscrepancyUnauthorized):
			fmt.Fprintf(stderr, "txharbor reconcile-admin: dispose refused (unauthorized): %s\n", logx.Redact(err.Error()))
		default:
			fmt.Fprintf(stderr, "txharbor reconcile-admin: dispose refused: %s\n", logx.Redact(err.Error()))
		}
		return 1
	}
	fmt.Fprintf(stdout,
		"txharbor reconcile-admin: dispose discrepancy_id=%s disposition_id=%s kind=%s result=%s action_ref=%q state_before=%s state_after=%s transitioned=%t idempotent_replay=%t target_gates_required=%t executed_by_014=false reopen_count=%d operation_id=%s\n",
		disposition.DiscrepancyID, disposition.DispositionID, disposition.Kind, disposition.Result,
		disposition.ActionRef, disposition.StateBefore, disposition.StateAfter, disposition.Transitioned,
		disposition.IdempotentReplay, disposition.TargetGatesRequired, disposition.ReopenCount, operation)
	if disposition.IdempotentReplay {
		fmt.Fprintln(stdout, "txharbor reconcile-admin: dispose replay read the recorded disposition back; no new side effects occurred")
	}
	if disposition.TargetGatesRequired {
		fmt.Fprintf(stdout,
			"txharbor reconcile-admin: dispose references the existing entry point %q; its own authorization and gates remain mandatory and were NOT executed or substituted by 014 (FR-020)\n",
			disposition.ActionRef)
	}
	if disposition.Result == reconciliation.DispositionRefused || disposition.Result == reconciliation.DispositionFailed {
		fmt.Fprintf(stdout,
			"txharbor reconcile-admin: disposition recorded with result=%s; the item stays with the operator and is neither verified nor closed (disposed != reverified != closed)\n",
			disposition.Result)
	}
	return 0
}

// reconcileDiscrepancyView is the read-only pre-authorization view of one
// discrepancy row. It carries the recorded scope marker so the query
// authorization uses exactly the scope the ticket was detected under.
type reconcileDiscrepancyView struct {
	DiscrepancyID  string
	Category       string
	BusinessKey    string
	ContentHash    string
	State          reconciliation.DiscrepancyState
	ClaimOwner     string
	ReopenCount    int64
	LinkedTo       string
	CloseBasis     bool
	EvidenceDomain []byte
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// readDiscrepancyViewSQL reads one discrepancy row without a lock: the
// authorization evaluation must never run while a row lock is held.
const readDiscrepancyViewSQL = `
SELECT discrepancy_id::text, category, business_key, encode(content_hash, 'hex'),
       state, COALESCE(claim_owner, ''), reopen_count::bigint,
       COALESCE(linked_to::text, ''), (close_basis IS NOT NULL),
       evidence_version_domain, created_at, updated_at
FROM discrepancy
WHERE discrepancy_id = $1`

// readDiscrepancyView loads one discrepancy row (read-only, no lock).
func (e *reconAdminEnv) readDiscrepancyView(ctx context.Context, id string) (reconcileDiscrepancyView, error) {
	var row reconcileDiscrepancyView
	err := e.pool.QueryRow(ctx, readDiscrepancyViewSQL, id).Scan(
		&row.DiscrepancyID, &row.Category, &row.BusinessKey, &row.ContentHash,
		&row.State, &row.ClaimOwner, &row.ReopenCount, &row.LinkedTo,
		&row.CloseBasis, &row.EvidenceDomain, &row.CreatedAt, &row.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return row, fmt.Errorf("%w: discrepancy_id %s", reconciliation.ErrDiscrepancyNotFound, id)
	}
	if err != nil {
		return row, fmt.Errorf("read discrepancy: %w", err)
	}
	if !row.State.Valid() {
		return row, fmt.Errorf("discrepancy %s has unknown state %q", id, row.State)
	}
	return row, nil
}

// countDiscrepancyOccurrences returns the ticket's reappearance row count.
func (e *reconAdminEnv) countDiscrepancyOccurrences(ctx context.Context, id string) (int64, error) {
	var count int64
	if err := e.pool.QueryRow(ctx,
		`SELECT count(*)::bigint FROM discrepancy_occurrence WHERE discrepancy_id = $1`, id).Scan(&count); err != nil {
		return 0, fmt.Errorf("count discrepancy occurrences: %w", err)
	}
	return count, nil
}

// reconcileDispositionView is one recent disposition row of the ticket.
type reconcileDispositionView struct {
	DispositionID  string
	Kind           reconciliation.DispositionKind
	Result         reconciliation.DispositionResult
	ActionRef      string
	Operator       string
	IdempotencyKey string
	CreatedAt      time.Time
}

// readDiscrepancyDispositions reads the ticket's most recent dispositions
// (bounded, newest first), read-only.
func (e *reconAdminEnv) readDiscrepancyDispositions(ctx context.Context, id string) ([]reconcileDispositionView, error) {
	rows, err := e.pool.Query(ctx, `
SELECT disposition_id::text, kind, result, action_ref, operator, idempotency_key, created_at
FROM disposition
WHERE discrepancy_id = $1
ORDER BY created_at DESC, disposition_id
LIMIT 5`, id)
	if err != nil {
		return nil, fmt.Errorf("read discrepancy dispositions: %w", err)
	}
	defer rows.Close()
	var out []reconcileDispositionView
	for rows.Next() {
		var (
			view         reconcileDispositionView
			kind, result string
		)
		if err := rows.Scan(&view.DispositionID, &kind, &result, &view.ActionRef,
			&view.Operator, &view.IdempotencyKey, &view.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan discrepancy disposition: %w", err)
		}
		view.Kind, view.Result = reconciliation.DispositionKind(kind), reconciliation.DispositionResult(result)
		out = append(out, view)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate discrepancy dispositions: %w", err)
	}
	return out, nil
}

// reconcileDiscrepancyAuthScope derives the authorization scope of one
// discrepancy ticket from its recorded scope marker. A ticket without a scope
// marker cannot be authorized: the caller must deny, never guess. The
// derivation mirrors the T022 discrepancyAuthScope rule; height tickets carry
// their concrete range while time tickets are range-unbounded in the
// height-typed AuthScope carrier.
func reconcileDiscrepancyAuthScope(id string, raw []byte) (reconciliation.AuthScope, error) {
	doc, err := reconciliation.ParsePersistedEvidenceDomain(raw)
	if err != nil {
		return reconciliation.AuthScope{}, fmt.Errorf("discrepancy %s: %v", id, err)
	}
	if doc.Scope == nil {
		return reconciliation.AuthScope{}, fmt.Errorf("discrepancy %s has no recorded scope marker", id)
	}
	if err := doc.Scope.Validate(); err != nil {
		return reconciliation.AuthScope{}, fmt.Errorf("discrepancy %s scope marker: %v", id, err)
	}
	scope := reconciliation.AuthScope{
		ChainID:       doc.Scope.ChainID,
		Kind:          doc.Scope.Kind,
		BusinessTypes: append([]reconciliation.BusinessType(nil), doc.Scope.BusinessTypes...),
	}
	if doc.Scope.Kind == reconciliation.ScopeHeight {
		from, to := doc.Scope.From, doc.Scope.To
		scope.RangeStart, scope.RangeEnd = &from, &to
	}
	if err := scope.Validate(); err != nil {
		return reconciliation.AuthScope{}, err
	}
	return scope, nil
}

// reconcileAdminShowDiscrepancy prints the read-only lifecycle view of one
// discrepancy: state/ownership, identity fields, recorded scope, occurrence
// count and the most recent dispositions. It is authorized through the T009
// evaluator with the ticket's recorded scope (ActionQuery), writes no audit
// row on an allow, and never renders a disposed item as verified/closed.
func reconcileAdminShowDiscrepancy(ctx context.Context, env *reconAdminEnv, id string, stdout, stderr io.Writer) int {
	row, err := env.readDiscrepancyView(ctx, id)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: show failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	scope, scopeErr := reconcileDiscrepancyAuthScope(id, row.EvidenceDomain)
	if scopeErr != nil {
		// No derivable scope: fail closed through the evaluator so the refusal
		// is audited as an invalid-scope denial, never guessed around (the T022
		// discrepancyAuthScope rule).
		if _, err := env.evaluator.Authorize(ctx, env.principal, reconciliation.ActionQuery, reconciliation.AuthScope{}); err != nil {
			fmt.Fprintf(stderr, "txharbor reconcile-admin: authorization failed: %s\n", logx.Redact(err.Error()))
			return 1
		}
		fmt.Fprintf(stderr, "txharbor reconcile-admin: discrepancy show refused: %s\n", logx.Redact(scopeErr.Error()))
		return 1
	}
	decision, err := env.evaluator.Authorize(ctx, env.principal, reconciliation.ActionQuery, scope)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: authorization failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if !decision.Allowed {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: refused %s: %s (%s)\n",
			reconciliation.ActionQuery, decision.Reason, decision.Detail)
		return 1
	}

	occurrences, err := env.countDiscrepancyOccurrences(ctx, id)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: show failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	dispositions, err := env.readDiscrepancyDispositions(ctx, id)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor reconcile-admin: show failed: %s\n", logx.Redact(err.Error()))
		return 1
	}

	linkedTo := "none"
	if row.LinkedTo != "" {
		linkedTo = row.LinkedTo
	}
	owner := "none"
	if row.ClaimOwner != "" {
		owner = row.ClaimOwner
	}
	fmt.Fprintf(stdout,
		"txharbor reconcile-admin: show discrepancy_id=%s state=%s owner=%s reopen_count=%d category=%s business_key=%s content_hash=%s linked_to=%s close_basis=%t scope=%s created_at=%s updated_at=%s\n",
		row.DiscrepancyID, row.State, owner, row.ReopenCount, row.Category, row.BusinessKey,
		row.ContentHash, linkedTo, row.CloseBasis, reconcileAuthScopeText(scope),
		row.CreatedAt.UTC().Format(time.RFC3339), row.UpdatedAt.UTC().Format(time.RFC3339))
	fmt.Fprintf(stdout, "  occurrences=%d\n", occurrences)
	if len(dispositions) == 0 {
		fmt.Fprintln(stdout, "  disposition none")
	}
	for _, disposition := range dispositions {
		fmt.Fprintf(stdout, "  disposition disposition_id=%s kind=%s result=%s action_ref=%q operator=%s idempotency_key=%s created_at=%s\n",
			disposition.DispositionID, disposition.Kind, disposition.Result, disposition.ActionRef,
			disposition.Operator, disposition.IdempotencyKey, disposition.CreatedAt.UTC().Format(time.RFC3339))
	}
	return 0
}
