// approve.go implements T051 [US4]: the `recovery-admin approve` and
// `recovery-admin release` operator wiring — the four decision commands
// approve / revoke / release / revoke-release of FR-021–FR-024.
//
// Trust boundary and operating discipline:
//
//  1. The authenticated subject is TXHARBOR_RECOVERY_PRINCIPAL (an
//     authenticated <kind>:<id> principal) and never a flag; free text never
//     authorizes. `--reason` is recorded audit annotation only.
//  2. This is wiring, not judgment: the decision rules stay in
//     internal/recovery (approvals.go / release.go / scope.go) and the single
//     derived judgment stays in gate.go. The CLI canonicalizes `--scope`
//     through recovery.CanonicalScopeHash (T050) and calls the decision
//     writer; it re-implements no validity rule, writes no decision row itself
//     and fabricates no refusal class. A refusal returns the class the library
//     derived (executor self-approval, missing role/mapping, stale evidence,
//     missing or insufficient approval basis, ...).
//  3. No valid approval basis means no release: Release derives the current
//     approve decisions of the instance's approvers and judges them through
//     gate.approvalsValid; an unsatisfied basis refuses, is audited and writes
//     zero release rows. There is no flag that names approvals, no
//     force/override flag and no writable release boolean (INV-2).
//  4. `--operation-id` is the persistent idempotency key of the decision
//     writer: the same input reads the recorded decision back with zero
//     writes (recorded=true), a different input under the same id refuses with
//     controlstore.ErrOperationConflict and zero writes. Without it the
//     invocation derives a unique id and prints it: a real decision
//     (non-replayable), never a silent repeat or a false replay.
//  5. The instance binding discipline is inherited: `--instance` must be a
//     UUID and must agree with TXHARBOR_RECOVERY_INSTANCE; the library locks
//     the instance row and refuses a missing/closed/non-recovery instance.
//  6. Missing required configuration is refused by exact key name;
//     TXHARBOR_RECOVERY_GATE_TTL is required for a release (the derived
//     evaluation has no default TTL), and a malformed
//     TXHARBOR_RECOVERY_EFFECT_CLASS_RULING refuses by key name (the ruling
//     resolves the scope's effect-class dimension and is never a flag). Exit
//     codes: 0 recorded/replayed, 1 refusal/config, 2 usage. DSNs never reach
//     stdout/stderr (logx.Redact; only the credential-free fingerprint is
//     carried in audit detail).
//
// The control store is reached only through recoveryOpOpen
// (controlstore.NewStore), so the T069 schema-version guard (unknown or
// incompatible version => control_store_unavailable) is inherited and no
// unguarded decision path exists.
package recoveryadmin

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// The audit action tokens of the decision commands. Accepted decisions are
// audited by the decision writers themselves (approval_decision /
// release_decision, paired with the decision row a single transaction); these
// tokens carry the CLI-level refusals that never reach the writer (for example
// a non-canonical --scope).
const (
	recoveryActionApprove = "approve"
	recoveryActionRelease = "release"
)

// recoveryAdminApprove implements `recovery-admin approve` (grant, or
// `--revoke` for the matching revoke decision).
func recoveryAdminApprove(ctx context.Context, args []string, d Deps) int {
	return recoveryAdminDecision(ctx, args, d, true)
}

// recoveryAdminRelease implements `recovery-admin release` (derived release,
// or `--revoke` for the explicit stream revoke).
func recoveryAdminRelease(ctx context.Context, args []string, d Deps) int {
	return recoveryAdminDecision(ctx, args, d, false)
}

// recoveryAdminDecision is the shared approve/revoke/release/revoke-release
// body: parse, canonicalize, call the library decision writer, render.
func recoveryAdminDecision(ctx context.Context, args []string, d Deps, approval bool) int {
	command, action := "release", recoveryActionRelease
	if approval {
		command, action = "approve", recoveryActionApprove
	}
	stdout, stderr := d.stdout(), d.stderr()
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			recoveryAdminActionUsage(stdout, recoveryAdminActionByNameOrZero(command))
			return 0
		}
	}

	fs := flag.NewFlagSet("txharbor recovery-admin "+command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	instanceFlag := fs.String("instance", "", "recovery instance id (required)")
	capabilityFlag := fs.String("capability", "", "capability name (required)")
	scopeFlag := fs.String("scope", "", "canonical capability scope (required)")
	revokeFlag := fs.Bool("revoke", false, "record the matching revoke decision instead of the grant")
	reasonFlag := fs.String("reason", "", "audit annotation (optional; never an authorization)")
	operationID := fs.String("operation-id", "", "idempotency key (optional)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*instanceFlag) == "" ||
		strings.TrimSpace(*capabilityFlag) == "" || strings.TrimSpace(*scopeFlag) == "" {
		recoveryAdminActionUsage(stderr, recoveryAdminActionByNameOrZero(command))
		return 2
	}
	instanceID, err := recoveryVerifyInstanceID(*instanceFlag)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: %s\n", command, logx.Redact(err.Error()))
		return 2
	}
	capability := recovery.Capability(strings.TrimSpace(*capabilityFlag))
	if !capability.Known() {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: capability %q is outside the closed 7-capability set %v\n",
			command, *capabilityFlag, recovery.KnownCapabilities())
		return 2
	}
	operation, err := recoveryOpOperationID(*operationID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: %s\n", command, logx.Redact(err.Error()))
		return 2
	}
	derived := false
	if operation == "" {
		// A derived unique id makes the invocation a real, non-replayable
		// decision; it is printed so the operator can make the next one
		// replayable with --operation-id.
		derived = true
		operation = command + ":" + uuid.NewString()
	}

	// The deployment binding never silently overrides --instance (the same
	// canonical comparison the verify path uses).
	if bound, ok := d.getenvValue(config.EnvRecoveryInstance); ok && strings.TrimSpace(bound) != "" &&
		!recoveryVerifyInstanceBindingMatches(instanceID, bound) {
		fmt.Fprintf(stderr,
			"txharbor recovery-admin %s: --instance %s does not match %s=%s (instance bound execution; refusing)\n",
			command, instanceID, config.EnvRecoveryInstance, strings.TrimSpace(bound))
		return 1
	}

	env, code := recoveryOpOpen(ctx, d, command)
	if env == nil {
		return code
	}
	defer env.pool.Close()

	// T050 owns the canonical scope vocabulary: the CLI accepts only a
	// canonical capability scope and refuses everything else as
	// scope_mismatch (fail-closed, audited) instead of recording a stream
	// under a non-canonical key.
	scopeHash, scopeErr := recovery.CanonicalScopeHash(*scopeFlag)
	if scopeErr != nil {
		reason := fmt.Sprintf("--scope %q is not a canonical capability scope: %v", strings.TrimSpace(*scopeFlag), scopeErr)
		recoveryDecisionRefuse(ctx, env, action, instanceID, operation, string(recovery.RefusalScopeMismatch), reason)
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: refused: refusal_class=%s %s (zero writes)\n",
			command, recovery.RefusalScopeMismatch, logx.Redact(reason))
		return 1
	}

	reason := strings.TrimSpace(*reasonFlag)
	if approval {
		var out recovery.ApprovalOutcome
		req := recovery.ApprovalRequest{
			InstanceID:  instanceID,
			Capability:  capability,
			ScopeHash:   scopeHash,
			Principal:   env.principal,
			Reason:      reason,
			OperationID: operation,
		}
		if *revokeFlag {
			out, err = recovery.RevokeApproval(ctx, env.store, req)
		} else {
			out, err = recovery.Approve(ctx, env.store, req)
		}
		return recoveryApproveRender(stdout, stderr, out, err, instanceID, capability, scopeHash, operation, derived)
	}

	// A release evaluates the derived release_valid on every call, so the
	// gate (with the configured TTL, no default) is required; no fund-gate
	// checker is injected here (phase two stays the action site's obligation).
	// The trusted deployment effect-class ruling (T050) resolves the scope's
	// effect-class dimension at this evaluation: an absent key means "not
	// configured"; a malformed value refuses by key name, never guessing a
	// class. The approval snapshot stays conservative and is never a request
	// input.
	ttl, code := recoveryGateTTL(d, command)
	if code != 0 {
		return code
	}
	ruling, code := recoveryGateEffectClassRuling(d, command)
	if code != 0 {
		return code
	}
	gate, gateErr := recovery.NewGate(env.store, recovery.GateOptions{
		TTL: ttl, EffectClassRuling: ruling,
		TrustedTarget: recovery.GateTargetBinding{
			TargetGuardKey:        env.targetGuardKey,
			TargetRoleFingerprint: env.targetRoleFingerprint,
		},
	})
	if gateErr != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: %s\n", command, logx.Redact(gateErr.Error()))
		return 1
	}
	var out recovery.ReleaseOutcome
	req := recovery.ReleaseRequest{
		InstanceID:  instanceID,
		Capability:  capability,
		ScopeHash:   scopeHash,
		Principal:   env.principal,
		Reason:      reason,
		OperationID: operation,
	}
	if *revokeFlag {
		out, err = recovery.RevokeRelease(ctx, env.store, gate, req)
	} else {
		out, err = recovery.Release(ctx, env.store, gate, req)
	}
	return recoveryReleaseRender(stdout, stderr, out, err, instanceID, capability, scopeHash, operation, derived)
}

// recoveryApproveRender renders one approve/revoke outcome. The library
// already audited the refusal under approval_decision; the CLI prints the
// class it derived and never substitutes one.
func recoveryApproveRender(stdout, stderr io.Writer, out recovery.ApprovalOutcome, err error,
	instanceID string, capability recovery.Capability, scopeHash, operation string, derived bool) int {
	switch {
	case err == nil:
		fmt.Fprintf(stdout,
			"txharbor recovery-admin approve: decision=%s recorded=%t approval_id=%s approval_class=%s principal=%s person_id=%s instance=%s capability=%s scope_hash=%s operation_id=%s\n",
			out.Decision, out.Recorded, out.ApprovalID, out.ApprovalClass, out.Principal, out.PersonID,
			instanceID, capability, scopeHash, recoveryOpDisplayID(operation))
		if derived {
			fmt.Fprintf(stdout,
				"  derived_operation_id=%s (a real decision; pass --operation-id to make the next one replayable)\n", operation)
		}
		if out.Decision == "revoke" {
			fmt.Fprintln(stdout,
				"  note: the revoke covers only this principal's earlier approve rows of this (instance, capability, scope) stream; re-approval plus re-release is required to converge.")
		} else {
			fmt.Fprintln(stdout,
				"  note: an approval is a decision record, not a release (approved is not a state); release validity is re-derived on every admission and the existing fund gates remain the action site's obligation.")
		}
		return 0
	case errors.Is(err, controlstore.ErrOperationConflict):
		fmt.Fprintf(stderr,
			"txharbor recovery-admin approve: operation_conflict operation_id=%s (the operation id was recorded with a different input; zero writes)\n", operation)
		return 1
	case errors.Is(err, recovery.ErrApprovalRefused):
		fmt.Fprintf(stderr,
			"txharbor recovery-admin approve: refused: refusal_class=%s reason=%s instance=%s capability=%s scope_hash=%s operation_id=%s (zero writes)\n",
			displayOrNone(string(out.RefusalClass)), logx.Redact(out.Reason),
			instanceID, capability, scopeHash, operation)
		return 1
	case errors.Is(err, recovery.ErrApprovalRequest):
		fmt.Fprintf(stderr, "txharbor recovery-admin approve: %s\n", logx.Redact(err.Error()))
		return 2
	default:
		fmt.Fprintf(stderr, "txharbor recovery-admin approve: %s\n", logx.Redact(err.Error()))
		return 1
	}
}

// recoveryReleaseRender renders one release/revoke outcome.
func recoveryReleaseRender(stdout, stderr io.Writer, out recovery.ReleaseOutcome, err error,
	instanceID string, capability recovery.Capability, scopeHash, operation string, derived bool) int {
	switch {
	case err == nil:
		fmt.Fprintf(stdout,
			"txharbor recovery-admin release: decision=%s recorded=%t release_id=%s approval_refs=%s instance=%s capability=%s scope_hash=%s operation_id=%s\n",
			out.Decision, out.Recorded, out.ReleaseID, displayOrNone(strings.Join(out.ApprovalRefs, ",")),
			instanceID, capability, scopeHash, recoveryOpDisplayID(operation))
		if derived {
			fmt.Fprintf(stdout,
				"  derived_operation_id=%s (a real decision; pass --operation-id to make the next one replayable)\n", operation)
		}
		if out.Decision == "revoke" {
			fmt.Fprintln(stdout,
				"  note: the next evaluation of this (instance, capability, scope) stream refuses with release_revoked until a new release is recorded; the revoke needs no approval basis (withdrawing a release is fail-safe).")
		} else {
			fmt.Fprintln(stdout,
				"  note: a release decision is not a resumption: the gate re-derives every condition on each admission (no writable release boolean), and the existing fund gates stay independently enforced at the action site (phase two, never replaced by this command).")
		}
		return 0
	case errors.Is(err, controlstore.ErrOperationConflict):
		fmt.Fprintf(stderr,
			"txharbor recovery-admin release: operation_conflict operation_id=%s (the operation id was recorded with a different input; zero writes)\n", operation)
		return 1
	case errors.Is(err, recovery.ErrReleaseRefused):
		fmt.Fprintf(stderr,
			"txharbor recovery-admin release: refused: refusal_class=%s reason=%s instance=%s capability=%s scope_hash=%s operation_id=%s (zero writes; the capability stays closed)\n",
			displayOrNone(string(out.RefusalClass)), logx.Redact(out.Reason),
			instanceID, capability, scopeHash, operation)
		return 1
	case errors.Is(err, recovery.ErrReleaseRequest):
		fmt.Fprintf(stderr, "txharbor recovery-admin release: %s\n", logx.Redact(err.Error()))
		return 2
	default:
		fmt.Fprintf(stderr, "txharbor recovery-admin release: %s\n", logx.Redact(err.Error()))
		return 1
	}
}

// recoveryDecisionRefuse appends one CLI-level refusal row (a request refused
// before the library decision path, for example a non-canonical scope). Best
// effort: the refusal is already decided and the annotation never changes it.
// The class comes from the closed gate set; the operation id is the request's
// idempotency annotation.
func recoveryDecisionRefuse(ctx context.Context, env *recoveryOpEnv, action, instanceID, operation, refusalClass, reason string) {
	if env == nil || env.pool == nil {
		return
	}
	detail, err := json.Marshal(map[string]any{
		"reason":       boundedRecoveryCLIDetail(reason),
		"operation_id": operation,
	})
	if err != nil {
		return
	}
	actor := env.principal
	if strings.TrimSpace(actor) == "" {
		actor = "system:recovery-decision"
	}
	_ = controlstore.WriteAudit(ctx, env.pool, controlstore.AuditRecord{
		InstanceID:   instanceID,
		Actor:        actor,
		Action:       action,
		Detail:       detail,
		Result:       controlstore.AuditRefused,
		RefusalClass: refusalClass,
		OperationID:  operation,
	})
}

// boundedRecoveryCLIDetail bounds one free-text audit annotation (the reason
// is never an authorization input).
func boundedRecoveryCLIDetail(text string) string {
	text = strings.TrimSpace(text)
	if len(text) <= 512 {
		return text
	}
	return text[:504] + " [bound]"
}
