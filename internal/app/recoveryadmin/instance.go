// instance.go implements T027's CLI wiring: `recovery-admin instance-open`
// (plain open or the explicit, two-person-approved supersede) and
// `recovery-admin instance-close`.
//
// Trust boundary and operating discipline:
//
//  1. The authenticated subject is TXHARBOR_RECOVERY_PRINCIPAL and never a
//     flag; free text never authorizes. instance-open requires an active
//     identity mapping (the recorded executor's person must be provable).
//     instance-close additionally requires a participant binding on the
//     instance being closed (any role), and the two-person basis of a
//     supersede is validated against the control-store approval rows.
//  2. TXHARBOR_RECOVERY_INSTANCE binds the process to one instance: a bound
//     process refuses to open another instance, and a supersede must name
//     exactly the bound instance (instance_mismatch discipline). The close
//     binding must match --instance.
//  3. `baseline` is documentation-only deployment state and never arms the
//     production gate (`recovery` does); the help text states this so the
//     operator cannot read kind=baseline as an armed recovery.
//  4. instance-close evaluates the derived release of all seven capabilities
//     with the configured TXHARBOR_RECOVERY_GATE_TTL (required by name, no
//     default). An open gap refuses the close, keeps the instance open and
//     escalates; no risk acceptance is ever delivered.
//  5. --operation-id is the persistent idempotency key: a recorded outcome
//     replays with zero side effects; a different input conflicts with zero
//     writes. Exit codes: 0 success, 1 refusal/config, 2 usage.
package recoveryadmin

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// The audit action tokens of the lifecycle commands.
const (
	recoveryActionInstanceOpen      = "instance_open"
	recoveryActionInstanceClose     = "instance_close"
	recoveryActionInstanceSupersede = "instance_supersede"
)

// recoveryAdminInstanceOpen implements `recovery-admin instance-open`.
func recoveryAdminInstanceOpen(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			recoveryAdminActionUsage(stdout, recoveryAdminActionByNameOrZero("instance-open"))
			return 0
		}
	}
	fs := flag.NewFlagSet("txharbor recovery-admin instance-open", flag.ContinueOnError)
	fs.SetOutput(stderr)
	kind := fs.String("kind", "", "recovery|baseline (required); only recovery arms the production gate")
	supersede := fs.String("supersede", "", "explicit supersede: the open instance id this new instance replaces (requires --approval-refs)")
	approvalRefs := fs.String("approval-refs", "", "two approval ids (comma-separated) by two distinct non-executor people, required with --supersede")
	reason := fs.String("reason", "", "audit annotation (optional; never an authorization)")
	operationID := fs.String("operation-id", "", "idempotent operation identity (optional)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	instanceKind := strings.TrimSpace(*kind)
	if fs.NArg() > 0 || instanceKind == "" {
		recoveryAdminActionUsage(stderr, recoveryAdminActionByNameOrZero("instance-open"))
		return 2
	}
	switch instanceKind {
	case "recovery", "baseline":
	default:
		fmt.Fprintf(stderr, "txharbor recovery-admin instance-open: unknown --kind %q (want recovery|baseline)\n", *kind)
		return 2
	}
	supersedeID := strings.TrimSpace(*supersede)
	refs := parseApprovalRefs(*approvalRefs)
	if supersedeID == "" && len(refs) > 0 {
		fmt.Fprintln(stderr, "txharbor recovery-admin instance-open: --approval-refs is only meaningful with --supersede")
		return 2
	}
	if supersedeID != "" && len(refs) < 2 {
		fmt.Fprintln(stderr, "txharbor recovery-admin instance-open: --supersede requires --approval-refs with two approval ids by two distinct non-executor people")
		return 2
	}
	operation, err := recoveryOpOperationID(*operationID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin instance-open: %s\n", logx.Redact(err.Error()))
		return 2
	}

	action := recoveryActionInstanceOpen
	if supersedeID != "" {
		action = recoveryActionInstanceSupersede
		instanceID, bound, code := recoveryOpInstanceContext(d, supersedeID, "instance-open")
		if code != 0 {
			return code
		}
		if !bound {
			fmt.Fprintf(stderr, "txharbor recovery-admin instance-open: --supersede requires an instance id\n")
			return 2
		}
		supersedeID = instanceID
	} else if env, ok := d.getenvValue(config.EnvRecoveryInstance); ok && strings.TrimSpace(env) != "" {
		fmt.Fprintf(stderr,
			"txharbor recovery-admin instance-open: %s is bound to %s; a bound process cannot open another instance (refusing; use --supersede for the audited replacement path)\n",
			config.EnvRecoveryInstance, strings.TrimSpace(env))
		return 1
	}

	env, code := recoveryOpOpen(ctx, d, "instance-open")
	if env == nil {
		return code
	}
	defer env.pool.Close()

	// The executor's person must be provable before it is recorded as
	// opened_by (the gate resolves the executor identity from it).
	if err := recoveryOpRequireParticipant(ctx, env, "", "executor", "instance-open"); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin instance-open: refused: %s\n", logx.Redact(err.Error()))
		return 1
	}

	inputDigest := recoveryOpInputDigest(action, map[string]string{
		"kind":        instanceKind,
		"supersede":   supersedeID,
		"approvals":   strings.Join(refs, ","),
		"opened_by":   env.principal,
		"reason_hash": recoveryOpInputDigest("reason", map[string]string{"value": strings.TrimSpace(*reason)}),
	})
	release, recorded, err := recoveryOpBegin(ctx, env.pool, action, operation)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin instance-open: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if release != nil {
		defer release()
	}
	if replayed, code := recoveryOpReplayGeneric(stderr, stdout, recorded, inputDigest, operation,
		fmt.Sprintf("kind=%s supersede=%s", instanceKind, displayOrNone(supersedeID))); replayed {
		return code
	}

	if supersedeID == "" {
		result, err := recovery.OpenInstance(ctx, env.store, recovery.OpenInstanceRequest{
			Kind:     instanceKind,
			OpenedBy: env.principal,
			Reason:   strings.TrimSpace(*reason),
		})
		if err != nil {
			_ = recoveryOpRecordInstance(ctx, env.pool, "", action, operation, env.principal, controlstore.AuditRefused,
				map[string]any{"input_digest": inputDigest, "reason": logx.Redact(err.Error())},
				map[string]any{"kind": instanceKind, "opened_by": env.principal})
			fmt.Fprintf(stderr, "txharbor recovery-admin instance-open: refused: %s\n", logx.Redact(err.Error()))
			return 1
		}
		_ = recoveryOpRecordInstance(ctx, env.pool, result.InstanceID, action, operation, env.principal, controlstore.AuditOK,
			map[string]any{"input_digest": inputDigest, "kind": result.Kind, "opened_by": result.OpenedBy},
			map[string]any{"instance_id": result.InstanceID, "kind": result.Kind})
		fmt.Fprintf(stdout,
			"txharbor recovery-admin instance-open: instance_id=%s kind=%s opened_by=%s arming=%s operation_id=%s principal=%s\n",
			result.InstanceID, result.Kind, result.OpenedBy,
			map[bool]string{true: "recovery (gate armed)", false: "baseline (documentation-only; the gate is not armed)"}[result.Kind == "recovery"],
			recoveryOpDisplayID(operation), env.principal)
		return 0
	}

	result, err := recovery.SupersedeInstance(ctx, env.store, recovery.SupersedeInstanceRequest{
		InstanceID:   supersedeID,
		OpenedBy:     env.principal,
		Reason:       strings.TrimSpace(*reason),
		ApprovalRefs: refs,
		OperationID:  operation,
	})
	if err != nil {
		_ = recoveryOpRecordInstance(ctx, env.pool, supersedeID, action, operation, env.principal, controlstore.AuditRefused,
			map[string]any{"input_digest": inputDigest, "reason": logx.Redact(err.Error()), "approval_refs": refs},
			map[string]any{"superseded_instance_id": supersedeID, "opened_by": env.principal})
		fmt.Fprintf(stderr, "txharbor recovery-admin instance-open: supersede refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	_ = recoveryOpRecordInstance(ctx, env.pool, result.NewInstanceID, action, operation, env.principal, controlstore.AuditOK,
		map[string]any{"input_digest": inputDigest, "superseded_instance_id": result.SupersededInstanceID, "new_instance_id": result.NewInstanceID, "opened_by": env.principal},
		map[string]any{"superseded_instance_id": result.SupersededInstanceID, "new_instance_id": result.NewInstanceID})
	fmt.Fprintf(stdout,
		"txharbor recovery-admin instance-open: superseded_instance=%s new_instance=%s kind=recovery opened_by=%s operation_id=%s principal=%s\n",
		result.SupersededInstanceID, result.NewInstanceID, env.principal, recoveryOpDisplayID(operation), env.principal)
	return 0
}

// recoveryAdminInstanceClose implements `recovery-admin instance-close`.
func recoveryAdminInstanceClose(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			recoveryAdminActionUsage(stdout, recoveryAdminActionByNameOrZero("instance-close"))
			return 0
		}
	}
	fs := flag.NewFlagSet("txharbor recovery-admin instance-close", flag.ContinueOnError)
	fs.SetOutput(stderr)
	instanceFlag := fs.String("instance", "", "open recovery instance id (required)")
	reason := fs.String("reason", "", "audit annotation (optional; never an authorization)")
	operationID := fs.String("operation-id", "", "idempotent operation identity (optional)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*instanceFlag) == "" {
		recoveryAdminActionUsage(stderr, recoveryAdminActionByNameOrZero("instance-close"))
		return 2
	}
	operation, err := recoveryOpOperationID(*operationID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin instance-close: %s\n", logx.Redact(err.Error()))
		return 2
	}
	instanceID, bound, code := recoveryOpInstanceContext(d, *instanceFlag, "instance-close")
	if code != 0 {
		return code
	}
	if !bound {
		fmt.Fprintf(stderr, "txharbor recovery-admin instance-close: --instance is required (instance bound execution)\n")
		return 1
	}

	env, code := recoveryOpOpen(ctx, d, "instance-close")
	if env == nil {
		return code
	}
	defer env.pool.Close()

	if err := instanceRequireParticipant(ctx, env, instanceID, "instance-close"); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin instance-close: refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	ttl, code := recoveryGateTTL(d, "instance-close")
	if code != 0 {
		return code
	}
	ruling, code := recoveryGateEffectClassRuling(d, "instance-close")
	if code != 0 {
		return code
	}
	gate, err := recovery.NewGate(env.store, recovery.GateOptions{TTL: ttl, EffectClassRuling: ruling})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin instance-close: %s\n", logx.Redact(err.Error()))
		return 1
	}

	inputDigest := recoveryOpInputDigest(recoveryActionInstanceClose, map[string]string{
		"instance_id": instanceID,
		"reason_hash": recoveryOpInputDigest("reason", map[string]string{"value": strings.TrimSpace(*reason)}),
	})
	release, recorded, err := recoveryOpBegin(ctx, env.pool, recoveryActionInstanceClose, operation)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin instance-close: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if release != nil {
		defer release()
	}
	if replayed, code := recoveryOpReplayGeneric(stderr, stdout, recorded, inputDigest, operation,
		fmt.Sprintf("instance=%s", instanceID)); replayed {
		return code
	}

	result, err := recovery.CloseInstance(ctx, env.store, gate, recovery.CloseInstanceRequest{
		InstanceID:  instanceID,
		Actor:       env.principal,
		Reason:      strings.TrimSpace(*reason),
		OperationID: operation,
	})
	if err != nil {
		_ = recoveryOpRecordInstance(ctx, env.pool, instanceID, recoveryActionInstanceClose, operation, env.principal, controlstore.AuditRefused,
			map[string]any{"input_digest": inputDigest, "reason": logx.Redact(err.Error()), "blocked": result.Blocked, "escalation_required": result.EscalationRequired},
			map[string]any{"instance_id": instanceID})
		fmt.Fprintf(stderr, "txharbor recovery-admin instance-close: refused: %s\n", logx.Redact(err.Error()))
		for _, blocker := range result.Blocked {
			fmt.Fprintf(stderr, "  blocker=%s\n", blocker)
		}
		fmt.Fprintf(stderr,
			"  closed=false escalation_required=%t risk_acceptance_delivered=false instance=%s operation_id=%s principal=%s\n",
			result.EscalationRequired, instanceID, recoveryOpDisplayID(operation), env.principal)
		return 1
	}
	_ = recoveryOpRecordInstance(ctx, env.pool, result.InstanceID, recoveryActionInstanceClose, operation, env.principal, controlstore.AuditOK,
		map[string]any{"input_digest": inputDigest, "baseline": result.Baseline, "risk_acceptance_delivered": false},
		map[string]any{"instance_id": result.InstanceID})
	fmt.Fprintf(stdout,
		"txharbor recovery-admin instance-close: instance=%s closed=%t baseline=%t escalation_required=%t risk_acceptance_delivered=false operation_id=%s principal=%s\n",
		result.InstanceID, result.Closed, result.Baseline, result.EscalationRequired,
		recoveryOpDisplayID(operation), env.principal)
	return 0
}

// instanceRequireParticipant enforces the close subject binding: the
// authenticated principal must have an active identity mapping and a
// participant binding of any role on the instance being closed.
func instanceRequireParticipant(ctx context.Context, env *recoveryOpEnv, instanceID, command string) error {
	if _, ok, err := env.store.ActivePersonID(ctx, env.principal); err != nil {
		return fmt.Errorf("resolve the principal identity mapping: %w", err)
	} else if !ok {
		return fmt.Errorf("%s %s has no active identity mapping; register the mapping first (cannot prove a person)",
			command, env.principal)
	}
	bindings, err := env.store.ParticipantBindings(ctx, instanceID)
	if err != nil {
		return fmt.Errorf("read participant bindings: %w", err)
	}
	for _, binding := range bindings {
		if binding.Principal == env.principal {
			return nil
		}
	}
	return fmt.Errorf("%s %s is not bound to instance %s under any participant role; register the participant first",
		command, env.principal, instanceID)
}

// recoveryGateEffectClassRuling resolves the trusted deployment effect-class
// ruling (T050) for the derived evaluation. An absent/blank key means "not
// configured" (nil: every effect class unknown, the event capabilities stay
// conservatively dual); a malformed value refuses by exact key name, because a
// deployment configuration error is never guessed around.
func recoveryGateEffectClassRuling(d Deps, command string) (recovery.EffectClassRuling, int) {
	raw, ok := d.getenvValue(recovery.EffectClassRulingConfigKey)
	if !ok || strings.TrimSpace(raw) == "" {
		return nil, 0
	}
	ruling, err := recovery.ParseEffectClassRuling(strings.TrimSpace(raw))
	if err != nil {
		fmt.Fprintf(d.stderr(), "txharbor recovery-admin %s: %s: %s\n",
			command, recovery.EffectClassRulingConfigKey, logx.Redact(err.Error()))
		return nil, 1
	}
	return ruling, 0
}

// recoveryGateTTL reads the required gate cache TTL for the close guard. The
// gate has no default TTL, so a missing/invalid value refuses by key name.
func recoveryGateTTL(d Deps, command string) (time.Duration, int) {
	raw, ok := d.getenvValue(config.EnvRecoveryGateTTL)
	if !ok || strings.TrimSpace(raw) == "" {
		fmt.Fprintf(d.stderr(),
			"txharbor recovery-admin %s: %s is required to evaluate the derived release (no default TTL); refusing\n",
			command, config.EnvRecoveryGateTTL)
		return 0, 1
	}
	ttl, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || ttl <= 0 {
		fmt.Fprintf(d.stderr(),
			"txharbor recovery-admin %s: %s must be a positive duration: %s\n",
			command, config.EnvRecoveryGateTTL, logx.Redact(errString(err)))
		return 0, 1
	}
	return ttl, 0
}

func errString(err error) string {
	if err == nil {
		return "value is not a positive duration"
	}
	return err.Error()
}

// parseApprovalRefs splits a comma-separated approval-id list. Empty entries
// are dropped; the library validates existence, currency and the two-person
// condition against the control store.
func parseApprovalRefs(raw string) []string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	refs := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			refs = append(refs, trimmed)
		}
	}
	return refs
}

func displayOrNone(value string) string {
	if strings.TrimSpace(value) == "" {
		return "(none)"
	}
	return value
}

// recoveryOpRecordInstance appends one command-outcome audit row bound to an
// instance (the generic recoveryOpRecord carries no instance id). Best effort:
// the command result is already decided; an audit failure never changes it.
func recoveryOpRecordInstance(ctx context.Context, pool *pgxpool.Pool, instanceID, action, operation, actor, result string,
	detail, target map[string]any) error {
	detailJSON, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	targetJSON, err := json.Marshal(target)
	if err != nil {
		return err
	}
	return controlstore.WriteAudit(ctx, pool, controlstore.AuditRecord{
		InstanceID:  instanceID,
		Actor:       actor,
		Action:      action,
		Target:      targetJSON,
		Detail:      detailJSON,
		Result:      result,
		OperationID: operation,
	})
}

// recoveryOpReplayGeneric replays a recorded command outcome under an
// operation id shared with the dedicated per-command replay helpers: same
// input replays with zero side effects, a different input conflicts with zero
// writes.
func recoveryOpReplayGeneric(stderr, stdout io.Writer, recorded *recoveryOpRecorded, inputDigest, operation, subject string) (bool, int) {
	if recorded == nil {
		return false, 0
	}
	var detail map[string]any
	_ = json.Unmarshal(recorded.Detail, &detail)
	if digest, _ := detail["input_digest"].(string); digest != inputDigest {
		fmt.Fprintf(stderr, "txharbor recovery-admin: operation_conflict operation_id=%s (the operation id was recorded with a different input; zero writes)\n", operation)
		return true, 1
	}
	if recorded.Result == controlstore.AuditOK {
		fmt.Fprintf(stdout, "txharbor recovery-admin: replayed=true operation_id=%s result=ok %s\n", operation, subject)
		return true, 0
	}
	fmt.Fprintf(stderr, "txharbor recovery-admin: replayed=true operation_id=%s result=%s (the recorded refusal replays; zero side effects)\n",
		operation, recorded.Result)
	return true, 1
}
