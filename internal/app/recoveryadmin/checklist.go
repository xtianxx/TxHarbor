// checklist.go implements T029's CLI wiring: `recovery-admin checklist-set`
// (evidence collection by this instance's executor) and
// `recovery-admin checklist-verify` (confirmation by a non-executor verifier).
//
// Trust boundary and operating discipline (T028/T029; FR-010/011/012):
//
//  1. The authenticated subject is TXHARBOR_RECOVERY_PRINCIPAL; free text
//     never authorizes. checklist-set requires the executor binding of the
//     instance, checklist-verify the verifier binding, each with an active
//     identity mapping. The library re-derives the person behind the
//     principal and refuses self-verification (the same person under another
//     principal included) with an audited refusal.
//  2. TXHARBOR_RECOVERY_INSTANCE binding: a process bound to another instance
//     is refused before any connection (instance_mismatch discipline).
//  3. Missing/blank evidence refuses and is audited: a checkpoint summary or
//     state record alone is never proof. An unknown item key is a usage error.
//  4. --operation-id is the persistent idempotency key: a recorded outcome
//     replays with zero side effects; a different input conflicts with zero
//     writes. Exit codes: 0 success, 1 refusal/config, 2 usage.
//  5. Program boundary (T028/T064, stated in the help text and NOT a runtime
//     enforcement claim): reconcile-admin/events-admin/withdraw-exec operator
//     paths and external schedulers have no 015 runtime gate in this tree;
//     only checklist signing does not constitute a runtime isolation proof.
//     The old_writers_stopped evidence must name the executable stop/
//     permission-removal measures plus the gate audit, and
//     no_pre_release_effects is checked against admitted externally visible
//     actions, never by a status field.
package recoveryadmin

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// The audit action tokens of the checklist commands.
const (
	recoveryActionChecklistSet    = "checklist_set"
	recoveryActionChecklistVerify = "checklist_verify"
)

// recoveryAdminChecklistSet implements `recovery-admin checklist-set`.
func recoveryAdminChecklistSet(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			recoveryAdminActionUsage(stdout, recoveryAdminActionByNameOrZero("checklist-set"))
			return 0
		}
	}
	fs := flag.NewFlagSet("txharbor recovery-admin checklist-set", flag.ContinueOnError)
	fs.SetOutput(stderr)
	instanceFlag := fs.String("instance", "", "recovery instance id (required)")
	itemFlag := fs.String("item", "", "checklist item key (required)")
	evidenceRef := fs.String("evidence-ref", "", "external evidence reference (required; a summary alone is never proof)")
	checkpointSummary := fs.String("checkpoint-summary", "", "optional JSON status/context summary (never a verification basis)")
	reason := fs.String("reason", "", "audit annotation (optional; never an authorization)")
	operationID := fs.String("operation-id", "", "idempotent operation identity (optional)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*instanceFlag) == "" || strings.TrimSpace(*itemFlag) == "" {
		recoveryAdminActionUsage(stderr, recoveryAdminActionByNameOrZero("checklist-set"))
		return 2
	}
	item, err := recovery.ParseIsolationItemKey(strings.TrimSpace(*itemFlag))
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin checklist-set: %s\n", logx.Redact(err.Error()))
		return 2
	}
	summary := strings.TrimSpace(*checkpointSummary)
	if summary != "" && !json.Valid([]byte(summary)) {
		fmt.Fprintln(stderr, "txharbor recovery-admin checklist-set: --checkpoint-summary must be valid JSON (it is stored as JSONB)")
		return 2
	}
	if err := recovery.ValidateChecklistText("evidence_ref", strings.TrimSpace(*evidenceRef)); err != nil {
		fmt.Fprintln(stderr, "txharbor recovery-admin checklist-set: refused: evidence_ref contains credential-shaped material")
		return 1
	}
	if err := recovery.ValidateChecklistSummary([]byte(summary)); err != nil {
		fmt.Fprintln(stderr, "txharbor recovery-admin checklist-set: refused: checkpoint_summary contains credential-shaped material")
		return 1
	}
	if err := recovery.ValidateChecklistText("reason", strings.TrimSpace(*reason)); err != nil {
		fmt.Fprintln(stderr, "txharbor recovery-admin checklist-set: refused: reason contains credential-shaped material")
		return 1
	}
	operation, err := recoveryOpOperationID(*operationID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin checklist-set: %s\n", logx.Redact(err.Error()))
		return 2
	}
	instanceID, bound, code := recoveryOpInstanceContext(d, *instanceFlag, "checklist-set")
	if code != 0 {
		return code
	}
	if !bound {
		fmt.Fprintf(stderr, "txharbor recovery-admin checklist-set: --instance is required (instance bound execution)\n")
		return 1
	}

	env, code := recoveryOpOpen(ctx, d, "checklist-set")
	if env == nil {
		return code
	}
	defer env.pool.Close()

	if err := recoveryOpRequireParticipant(ctx, env, instanceID, "executor", "checklist-set"); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin checklist-set: refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	checklist, err := recovery.NewChecklist(env.store)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin checklist-set: %s\n", logx.Redact(err.Error()))
		return 1
	}

	inputDigest := recoveryOpInputDigest(recoveryActionChecklistSet, map[string]string{
		"instance_id":     instanceID,
		"item_key":        string(item),
		"evidence_ref":    strings.TrimSpace(*evidenceRef),
		"summary_present": fmt.Sprintf("%t", summary != ""),
		"summary_hash":    recoveryOpInputDigest("summary", map[string]string{"value": summary}),
	})
	release, recorded, err := recoveryOpBegin(ctx, env.pool, recoveryActionChecklistSet, operation)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin checklist-set: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if release != nil {
		defer release()
	}
	if replayed, code := recoveryOpReplayGeneric(stderr, stdout, recorded, inputDigest, operation,
		fmt.Sprintf("instance=%s item=%s", instanceID, item)); replayed {
		return code
	}

	record, err := checklist.Set(ctx, recovery.ChecklistEvidenceRequest{
		InstanceID:        instanceID,
		ItemKey:           item,
		State:             recovery.ChecklistStateEvidenced,
		EvidenceRef:       strings.TrimSpace(*evidenceRef),
		CheckpointSummary: nullableSummary(summary),
		Actor:             env.principal,
		OperationID:       operation,
	})
	if err != nil {
		_ = recoveryOpRecordInstance(ctx, env.pool, instanceID, recoveryActionChecklistSet, operation, env.principal, controlstore.AuditRefused,
			map[string]any{"input_digest": inputDigest, "reason": logx.Redact(err.Error()), "note": strings.TrimSpace(*reason), "item_key": string(item)},
			map[string]any{"instance_id": instanceID, "item_key": string(item)})
		fmt.Fprintf(stderr, "txharbor recovery-admin checklist-set: refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	_ = recoveryOpRecordInstance(ctx, env.pool, instanceID, recoveryActionChecklistSet, operation, env.principal, controlstore.AuditOK,
		map[string]any{"input_digest": inputDigest, "state": string(record.State), "evidence_ref": record.EvidenceRef, "note": strings.TrimSpace(*reason), "item_key": string(item)},
		map[string]any{"instance_id": instanceID, "item_key": string(item)})
	fmt.Fprintf(stdout,
		"txharbor recovery-admin checklist-set: instance=%s item=%s state=%s evidence_ref=%s checked_by=%s operation_id=%s principal=%s\n",
		record.InstanceID, record.ItemKey, record.State, record.EvidenceRef, record.CheckedBy,
		recoveryOpDisplayID(operation), env.principal)
	fmt.Fprintf(stdout,
		"  note: the item is evidenced only; a non-executor verifier must confirm it (checklist-verify) before its isolation dependency set counts as verified.\n")
	return 0
}

// recoveryAdminChecklistVerify implements `recovery-admin checklist-verify`.
func recoveryAdminChecklistVerify(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			recoveryAdminActionUsage(stdout, recoveryAdminActionByNameOrZero("checklist-verify"))
			return 0
		}
	}
	fs := flag.NewFlagSet("txharbor recovery-admin checklist-verify", flag.ContinueOnError)
	fs.SetOutput(stderr)
	instanceFlag := fs.String("instance", "", "recovery instance id (required)")
	itemFlag := fs.String("item", "", "checklist item key (required)")
	reject := fs.Bool("reject", false, "reject the item (insufficient/superseded evidence; re-collection required)")
	reason := fs.String("reason", "", "reason (required with --reject; audit annotation)")
	operationID := fs.String("operation-id", "", "idempotent operation identity (optional)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*instanceFlag) == "" || strings.TrimSpace(*itemFlag) == "" {
		recoveryAdminActionUsage(stderr, recoveryAdminActionByNameOrZero("checklist-verify"))
		return 2
	}
	item, err := recovery.ParseIsolationItemKey(strings.TrimSpace(*itemFlag))
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin checklist-verify: %s\n", logx.Redact(err.Error()))
		return 2
	}
	if *reject && strings.TrimSpace(*reason) == "" {
		fmt.Fprintln(stderr, "txharbor recovery-admin checklist-verify: --reject requires --reason")
		return 2
	}
	if err := recovery.ValidateChecklistText("reason", strings.TrimSpace(*reason)); err != nil {
		fmt.Fprintln(stderr, "txharbor recovery-admin checklist-verify: refused: reason contains credential-shaped material")
		return 1
	}
	operation, err := recoveryOpOperationID(*operationID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin checklist-verify: %s\n", logx.Redact(err.Error()))
		return 2
	}
	instanceID, bound, code := recoveryOpInstanceContext(d, *instanceFlag, "checklist-verify")
	if code != 0 {
		return code
	}
	if !bound {
		fmt.Fprintf(stderr, "txharbor recovery-admin checklist-verify: --instance is required (instance bound execution)\n")
		return 1
	}

	env, code := recoveryOpOpen(ctx, d, "checklist-verify")
	if env == nil {
		return code
	}
	defer env.pool.Close()

	if err := recoveryOpRequireParticipant(ctx, env, instanceID, "verifier", "checklist-verify"); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin checklist-verify: refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	checklist, err := recovery.NewChecklist(env.store)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin checklist-verify: %s\n", logx.Redact(err.Error()))
		return 1
	}

	inputDigest := recoveryOpInputDigest(recoveryActionChecklistVerify, map[string]string{
		"instance_id": instanceID,
		"item_key":    string(item),
		"reject":      fmt.Sprintf("%t", *reject),
		"reason_hash": recoveryOpInputDigest("reason", map[string]string{"value": strings.TrimSpace(*reason)}),
	})
	release, recorded, err := recoveryOpBegin(ctx, env.pool, recoveryActionChecklistVerify, operation)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin checklist-verify: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if release != nil {
		defer release()
	}
	if replayed, code := recoveryOpReplayGeneric(stderr, stdout, recorded, inputDigest, operation,
		fmt.Sprintf("instance=%s item=%s", instanceID, item)); replayed {
		return code
	}

	record, err := checklist.Verify(ctx, recovery.ChecklistVerifyRequest{
		InstanceID:  instanceID,
		ItemKey:     item,
		Actor:       env.principal,
		OperationID: operation,
		Reject:      *reject,
		Reason:      strings.TrimSpace(*reason),
	})
	if err != nil {
		_ = recoveryOpRecordInstance(ctx, env.pool, instanceID, recoveryActionChecklistVerify, operation, env.principal, controlstore.AuditRefused,
			map[string]any{"input_digest": inputDigest, "reason": logx.Redact(err.Error()), "item_key": string(item), "reject": *reject},
			map[string]any{"instance_id": instanceID, "item_key": string(item)})
		fmt.Fprintf(stderr, "txharbor recovery-admin checklist-verify: refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	_ = recoveryOpRecordInstance(ctx, env.pool, instanceID, recoveryActionChecklistVerify, operation, env.principal, controlstore.AuditOK,
		map[string]any{"input_digest": inputDigest, "state": string(record.State), "evidence_ref": record.EvidenceRef, "item_key": string(item)},
		map[string]any{"instance_id": instanceID, "item_key": string(item)})
	verdict := "verified"
	if record.State == recovery.ChecklistStateRejected {
		verdict = "rejected (re-collection required)"
	}
	fmt.Fprintf(stdout,
		"txharbor recovery-admin checklist-verify: instance=%s item=%s state=%s verdict=%s evidence_ref=%s verified_by=%s operation_id=%s principal=%s\n",
		record.InstanceID, record.ItemKey, record.State, verdict, record.EvidenceRef, record.VerifiedBy,
		recoveryOpDisplayID(operation), env.principal)
	if record.State == recovery.ChecklistStateVerified {
		fmt.Fprintln(stdout,
			"  note: the verification advanced the evidence generation; releases/approvals bound to the previous generation are invalid and must be re-issued.")
	}
	return 0
}

// nullableSummary converts the optional summary text for the storage layer.
func nullableSummary(summary string) []byte {
	if summary == "" {
		return nil
	}
	return []byte(summary)
}
