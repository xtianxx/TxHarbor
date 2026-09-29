// Package recoveryadmin implements the thin 015 operator command surface
// (`txharbor recovery-admin`): migrate/control/backup/verify-backup/restore/
// instance-open/instance-close/checklist-set/checklist-verify/verify/approve/
// release/status/drill all converge here.
//
// B0 setup skeleton (T004): this batch registers the fixed action surface and
// leaves every action at help/argument parsing. The behavior lands batch by
// batch (T008/T069 migrate, T010 control, T020/T021 verify-backup/backup,
// T022 restore); an action without an implementation still reports NOT
// IMPLEMENTED and exits non-zero. A stub never returns a false success, never
// substitutes a default for a required argument and never starts a backup,
// restore, verification, approval or release flow. Missing arguments and
// unknown actions are usage errors (exit 2); help goes to stdout, refusals and
// usage errors go to stderr.
//
// T008/T069 implement `migrate up|status` in migrate.go, scoped to the
// independent control store (TXHARBOR_RECOVERY_CONTROL_DSN) with the same
// trust-boundary and schema version guard the store uses. T010 implements the
// `control participant-register|identity-map-set|identity-map-show` management
// surface in control.go (deployment-privilege path: subject from
// TXHARBOR_RECOVERY_PRINCIPAL, single subject + audit, no preset principal,
// mapping changes invalidating affected approvals). B6/T021-T022 implement
// `backup`/`verify-backup` in backup.go and `restore` in restore.go (real
// pg_dump/pg_restore only, principal bound to a registered participant,
// persistent operation_id idempotency, explicit blocked states). B8/T027-T029
// implement the instance lifecycle in instance.go (`instance-open` with the
// explicit supersede path, release-guarded `instance-close`) and the
// isolation checklist in checklist.go (`checklist-set`/`checklist-verify`
// with audited refusals and operation_id idempotency). T042 implements
// `verify` in verify.go: one bounded read-only V1-V9 step through the T038
// orchestrator and the T039/T040 source adapters, bound to the open recovery
// instance. T051 implements `approve`/`release` in approve.go (the append-only
// approve/revoke/release/revoke-release wiring over the T048/T049 decision
// writers, no direct row writes) and `status` in status.go (the bounded
// read-only per-capability restored/verified/released review). T057 implements
// `drill` in drill.go: the isolated-environment real recovery flow (real
// pg_restore, never a re-initialized empty database) with the S12 measurements
// recorded separately (T055 RTO evaluation, T056 drill model) and archived
// under docs/evidence/015/. Every action of the fixed surface is wired; the B0
// stub helper remains for historical reference only and is never reached by a
// known action.
//
// It lives in its own package (rather than internal/app) for the same reason as
// internal/app/reconcileadmin: the delivered command imports internal/recovery
// and its source adapters, which keeps that build/test graph independent of
// internal/app. `internal/app` stays the assembly point for the capability gate
// wiring (T030-T034/T070); this package owns the command surface only.
//
// Boundary notes for the owning batches that follow: T004 is the single
// registration owner (later CLI tasks add their own files on top of the fixed
// action set); the authenticated principal comes from the deployment-controlled
// TXHARBOR_RECOVERY_PRINCIPAL binding and never from a flag; --operator/--reason
// style free text is audit carriage only and can never authorize; and every
// required configuration value is refused by its exact key name ("not
// configured") rather than defaulted.
package recoveryadmin

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
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

// recoveryAdminFlag is one parsed flag of an action. Every value is carried as
// text in B0; typed parsing belongs to the owning batch.
type recoveryAdminFlag struct {
	name  string
	usage string
}

// recoveryAdminAction is one action of the fixed surface. In B0 it carries the
// help/parse metadata only; behavior lands with the owning batch and is added
// in its own file.
type recoveryAdminAction struct {
	// name is the action token (`txharbor recovery-admin <name>`).
	name string
	// summary is the one-line help text.
	summary string
	// usage is the accepted argument form without the command prefix.
	usage string
	// flags are the accepted flags.
	flags []recoveryAdminFlag
	// required lists flag names that must be present and non-blank. A missing
	// required flag is refused by name; no default is invented (production
	// thresholds and authorization material never come from this stub).
	required []string
	// positional, when non-empty, is the closed set of required positional
	// tokens (e.g. migrate's up|status).
	positional []string
	// notes, when non-empty, are operator-facing boundaries rendered under the
	// flag list (for example the restore preconditions and evidence-timing
	// facts). They are help text only and never a runtime enforcement claim.
	notes []string
}

// recoveryAdminActions is the fixed action surface, in help order.
var recoveryAdminActions = []recoveryAdminAction{
	{
		name:       "migrate",
		summary:    "apply/show the control-store schema versions (control DSN only)",
		usage:      "migrate up|status",
		positional: []string{"up", "status"},
	},
	{
		name:       "control",
		summary:    "manage participants and identity mappings (deployment-controlled; T010)",
		usage:      "control participant-register|identity-map-set|identity-map-show [flags]",
		positional: []string{"participant-register", "identity-map-set", "identity-map-show"},
	},
	{
		name:    "backup",
		summary: "produce a backup artifact and its manifest (unverified until verify-backup)",
		usage:   "backup --chain-id CHAIN [--out DIR] [--operation-id ID]",
		flags: []recoveryAdminFlag{
			{name: "chain-id", usage: "scope chain identity (required)"},
			{name: "out", usage: "artifact output directory override (defaults to the configured artifact dir)"},
			{name: "operation-id", usage: "idempotent operation identity (optional)"},
		},
		required: []string{"chain-id"},
	},
	{
		name:    "verify-backup",
		summary: "restore a backup into an isolated target and verify it",
		usage:   "verify-backup --manifest M --target-dsn TARGET [--instance ID] [--operation-id ID]",
		flags: []recoveryAdminFlag{
			{name: "manifest", usage: "manifest path (required)"},
			{name: "target-dsn", usage: "isolated target DSN (required)"},
			{name: "instance", usage: "recovery instance id that binds the conclusion (optional)"},
			{name: "operation-id", usage: "idempotency key (optional); same id+input replays with zero side effects, a changed input conflicts with zero writes; omitted = a real rerun (non-replay)"},
		},
		required: []string{"manifest", "target-dsn"},
	},
	{
		name:    "restore",
		summary: "restore a verified manifest into the bound recovery instance (pre-write evidence invalidation)",
		usage:   "restore --manifest M --target-dsn TARGET --instance ID [--declaration isolated|production_main] [--reason R] [--operation-id ID]",
		flags: []recoveryAdminFlag{
			{name: "manifest", usage: "manifest path (required); the control store must hold verified evidence bound to this instance and backup_id"},
			{name: "target-dsn", usage: "target DSN (required; isolated unless explicitly declared production_main)"},
			{name: "instance", usage: "recovery instance id (required)"},
			{name: "declaration", usage: "isolated|production_main (default isolated)"},
			{name: "reason", usage: "explicit recorded reason (required for production_main); audit annotation only, never an authorization"},
			{name: "operation-id", usage: "idempotency key (optional); same id+input replays with zero side effects, a changed input conflicts with zero writes; omitted = a real rerun (non-replay) that appends new evidence"},
			{name: "signer-endpoint", usage: "signer boundary endpoint for reachability probing (optional)"},
			{name: "rpc-url", usage: "RPC fact-source URL for reachability probing (optional)"},
			{name: "broker-dsn", usage: "broker DSN for reachability probing (optional)"},
		},
		required: []string{"manifest", "target-dsn", "instance"},
		// Operator-facing boundaries of T022. These are help text: they state
		// preconditions and limits, never a runtime enforcement claim.
		notes: []string{
			"production_main preconditions: verified manifest + a control-store evidence chain matching the current manifest + a target that is not the control store + all four restore probes passing. This proves the restored data set is usable; it does NOT prove the old writers stopped or that the old instance is isolated.",
			"Authority comes from TXHARBOR_RECOVERY_PRINCIPAL with an active mapping and the executor participant binding on the instance; --reason is recorded audit annotation only and free text never authorizes.",
			"Evidence timing: an accepted pre-write marker advances the evidence generation before the first target write, so releases/approvals bound to the previous generation stop being usable for admission; an interrupted restore keeps them stale and never brings the old permission back.",
			"T028/T064 are not delivered: reconcile-admin/events-admin/withdraw-exec operator paths and external schedulers have no 015 runtime gate in this tree. Isolation relies on the documented stop/permission-removal discipline plus audit and no_pre_release_effects evidence; no runtime enforcement is claimed.",
		},
	},
	{
		name:    "instance-open",
		summary: "open a recovery instance (executor recorded; at most one open; explicit supersede replaces one)",
		usage:   "instance-open --kind recovery|baseline [--supersede ID --approval-refs A,B] [--reason R] [--operation-id ID]",
		flags: []recoveryAdminFlag{
			{name: "kind", usage: "recovery|baseline (required); only recovery arms the production gate, baseline is documentation-only state"},
			{name: "supersede", usage: "explicit supersede: the open instance id this new instance replaces (requires --approval-refs)"},
			{name: "approval-refs", usage: "two approval ids (comma-separated) by two distinct non-executor people, required with --supersede"},
			{name: "reason", usage: "audit annotation (optional)"},
			{name: "operation-id", usage: "idempotency key (optional); same id+input replays with zero side effects, a changed input conflicts with zero writes"},
		},
		required: []string{"kind"},
		notes: []string{
			"The recorded opened_by is the executor: the gate excludes its person from approvals and checklist verification. It must resolve to an active identity mapping.",
			"supersede is explicit only (the old instance id must be named), requires two current approvals by two distinct non-executor people, closes the old instance and opens a new one in one audited transaction; a row written directly into the control store is not this path and grants nothing.",
			"A process bound through " + "TXHARBOR_RECOVERY_INSTANCE" + " cannot open another instance; use --supersede for the audited replacement path (the binding must then match the instance being replaced).",
		},
	},
	{
		name:    "instance-close",
		summary: "close the open recovery instance (only when every capability is validly released)",
		usage:   "instance-close --instance ID [--reason R] [--operation-id ID]",
		flags: []recoveryAdminFlag{
			{name: "instance", usage: "open recovery instance id (required)"},
			{name: "reason", usage: "audit annotation (optional)"},
			{name: "operation-id", usage: "idempotency key (optional); same id+input replays with zero side effects, a changed input conflicts with zero writes"},
		},
		required: []string{"instance"},
		notes: []string{
			"Close requires all seven capabilities to be currently release-valid (derived evaluation re-run under the instance lock, including the dual two-person approval condition when a funds/delivery capability was ever released).",
			"An open evidence gap refuses the close: the instance stays open, is escalated for manual handling, and no risk acceptance is delivered.",
			"TXHARBOR_RECOVERY_GATE_TTL is required (the close guard has no default TTL); the bound principal needs a participant binding on the instance.",
		},
	},
	{
		name:    "checklist-set",
		summary: "record isolation-checklist evidence (executor)",
		usage:   "checklist-set --instance ID --item ITEM [--evidence-ref REF] [--checkpoint-summary JSON] [--reason R] [--operation-id ID]",
		flags: []recoveryAdminFlag{
			{name: "instance", usage: "recovery instance id (required)"},
			{name: "item", usage: "checklist item key (required): " + "old_writers_stopped|writer_fencing_observed|network_isolation|version_compatible|no_pre_release_effects|authorization_recheck"},
			{name: "evidence-ref", usage: "external evidence reference (required; a checkpoint summary or state record alone is never proof)"},
			{name: "checkpoint-summary", usage: "optional JSON status/context summary (stored, never a verification basis)"},
			{name: "reason", usage: "audit annotation (optional)"},
			{name: "operation-id", usage: "idempotency key (optional); same id+input replays with zero side effects, a changed input conflicts with zero writes"},
		},
		required: []string{"instance", "item"},
		notes: []string{
			"Evidence is collected by this instance's executor and confirmed by a non-executor verifier; a checkpoint summary alone is never proof and collection does not advance the evidence generation.",
			"Program boundary (T028/T064): reconcile-admin/events-admin/withdraw-exec operator paths and external schedulers have no 015 runtime gate in this tree. old_writers_stopped evidence must name the executable stop/permission-removal measures with time and subject plus the gate audit; checklist signing alone does not constitute a runtime isolation proof (runbook T064).",
			"no_pre_release_effects is verified against the gate audit (zero admitted externally visible actions), never by a status field; an action admitted before the instance opened and still in flight is outside that audit check and must be drained or awaited by the isolation procedure.",
		},
	},
	{
		name:    "checklist-verify",
		summary: "confirm a checklist item (non-executor verifier)",
		usage:   "checklist-verify --instance ID --item ITEM [--reject --reason R] [--operation-id ID]",
		flags: []recoveryAdminFlag{
			{name: "instance", usage: "recovery instance id (required)"},
			{name: "item", usage: "checklist item key (required)"},
			{name: "reject", usage: "reject the item (insufficient/superseded evidence; re-collection required); a verified item may be rejected by a later verdict"},
			{name: "reason", usage: "reason (required with --reject; audit annotation)"},
			{name: "operation-id", usage: "idempotency key (optional); same id+input replays with zero side effects, a changed input conflicts with zero writes"},
		},
		required: []string{"instance", "item"},
		notes: []string{
			"The verifier must be registered on the instance with the verifier role and must not resolve to the executor's person (the same person under another principal is refused and audited).",
			"An accepted verification/rejection advances the evidence generation by exactly one, invalidating releases/approvals bound to the previous generation.",
		},
	},
	{
		name:    "verify",
		summary: "run one bounded read-only V1-V9 fact verification step for the instance",
		usage:   "verify --instance ID --scope SCOPE [--operation-id ID]",
		flags: []recoveryAdminFlag{
			{name: "instance", usage: "recovery instance id (required); must be the open recovery instance and agree with " + "TXHARBOR_RECOVERY_INSTANCE"},
			{name: "scope", usage: "verification scope (required): \"all\" or a JSON object; canonicalized and recorded as scope_hash (empty/invalid refuses as scope_mismatch)"},
			{name: "operation-id", usage: "idempotency key (optional); same id+input reads the recorded outcome back with zero side effects, a changed input conflicts with zero writes; omitted = a real rerun (non-replay) that appends a new bounded step"},
		},
		required: []string{"instance", "scope"},
		notes: []string{
			"One bounded step over the full V1-V9 catalogue: the (state, generation, hash) token is captured and re-validated under the instance row lock on every call (no in-memory state), repeated calls append new verdict rows instead of flipping history, and the batch bound is TXHARBOR_RECOVERY_VERIFICATION_BATCH_LIMIT (no default; a bound overrun refuses with zero writes).",
			"Conclusions are consistent/divergent/unknown/stale; unknown and stale are never a pass and an open/escalated gap keeps its affected capabilities blocked. Verification never auto-fixes a gap and never triggers payment, signature, broadcast, replay or downstream delivery.",
			"Authority: TXHARBOR_RECOVERY_PRINCIPAL must resolve to an active identity mapping and a participant binding on the instance; TXHARBOR_RPC_URL, TXHARBOR_CHAIN_ID and the control-store schema guard (T069) are refused by exact key name when missing or incompatible.",
			"Output lists each V1-V9 conclusion and the open/escalated gap list (affected capabilities, required evidence, owner, escalation); the bound capabilities remain closed until the gap is closed by new evidence (T041) and released through the approval path.",
		},
	},
	{
		name:    "approve",
		summary: "record an approval decision (approve/revoke; executor excluded)",
		usage:   "approve --instance ID --capability CAP --scope SCOPE [--revoke] [--reason R] [--operation-id ID]",
		flags: []recoveryAdminFlag{
			{name: "instance", usage: "open recovery instance id (required)"},
			{name: "capability", usage: "capability name (required)"},
			{name: "scope", usage: "canonical capability scope (required); canonicalized with T050, a non-canonical --scope refuses as scope_mismatch"},
			{name: "revoke", usage: "record the revoke of this principal's earlier approve decision of the same (instance, capability, scope) stream"},
			{name: "reason", usage: "audit annotation (optional; never an authorization)"},
			{name: "operation-id", usage: "idempotency key (optional); same id+input reads the recorded decision back with zero writes, a changed input conflicts with zero writes; omitted = a real derived id (non-replayable)"},
		},
		required: []string{"instance", "capability", "scope"},
		notes: []string{
			"Validity is derived (T048): the recorded person comes from the current active mapping, the conservative approval class from the capability matrix, and the evidence (generation, hash) token from the instance row lock; approving is not releasing (F16: approved is not a state).",
			"The executor (opened_by or an executor-role participant, directly or through the current mapping) can never approve its own instance; a mapping change invalidates the earlier approval (re-approval is required).",
		},
	},
	{
		name:    "release",
		summary: "record a release/revoke decision (derived evaluation; nothing is writable)",
		usage:   "release --instance ID --capability CAP --scope SCOPE [--revoke] [--reason R] [--operation-id ID]",
		flags: []recoveryAdminFlag{
			{name: "instance", usage: "open recovery instance id (required)"},
			{name: "capability", usage: "capability name (required)"},
			{name: "scope", usage: "canonical capability scope (required); canonicalized with T050, a non-canonical --scope refuses as scope_mismatch"},
			{name: "revoke", usage: "record the explicit revoke of this stream (fail-safe; needs no approval basis)"},
			{name: "reason", usage: "audit annotation (optional; never an authorization)"},
			{name: "operation-id", usage: "idempotency key (optional); same id+input reads the recorded decision back with zero writes, a changed input conflicts with zero writes; omitted = a real derived id (non-replayable)"},
		},
		required: []string{"instance", "capability", "scope"},
		notes: []string{
			"No valid approval basis means no release: the writer derives the current approve decisions of the instance's approvers and judges them with the single derived judgment; an unsatisfied basis refuses, is audited and writes zero release rows. There is no flag that names approvals and no force/override entry.",
			"The recorded basis binds the authoritative (evidence_generation, evidence_hash) token; any later accepted evidence write invalidates the release (release_invalidated_generation). The gate re-derives every condition on each admission (INV-2: no writable release boolean) and the existing fund gates remain phase two at the action site.",
			"A release/revoke requires the instance's recovery_execute binding (opened_by or an executor-role participant) and TXHARBOR_RECOVERY_GATE_TTL (no default).",
		},
	},
	{
		name:    "status",
		summary: "show the read-only per-capability restored/verified/released review",
		usage:   "status [--instance ID] [--scope SCOPE]",
		flags: []recoveryAdminFlag{
			{name: "instance", usage: "recovery instance id (optional; defaults to the bound or single open instance)"},
			{name: "scope", usage: "canonical capability scope filter (optional); canonicalized with T050"},
		},
		notes: []string{
			"Per capability the three derived states of data-model §4.2 are shown with the blocking reason and the closed refusal class: restored = accepted restore-probe evidence (never implies verified/released), verified = current-generation V1-V9 conclusions for the applicable categories (unknown allowed and displayed; never a pass), released = the derived gate evaluation re-run per (capability, scope) stream (approved alone is not a release).",
			"Bounded read-only review (F13): the range is this instance (plus --scope) with instance-predicated LIMITed reads and no unbounded scan; the bounds TXHARBOR_RECOVERY_STATUS_TIMEOUT / TXHARBOR_RECOVERY_STATUS_MAX_READS / TXHARBOR_RECOVERY_STATUS_MAX_ROWS are required by exact key name (no default; local values are test inputs only). Exhausting any bound refuses the rest of the review, appends one refused audit row and changes no gap/instance/approval/release state.",
			"The review grants nothing: the gate evaluates phase one only (phase_two_evaluated=false), health signals never substitute the existing fund gates, and a timeout/exhausted budget/human knowledge is never gap closure or a resumption permission.",
			"Requires an active identity mapping and a participant binding on the reviewed instance, and TXHARBOR_RECOVERY_GATE_TTL for the derived evaluation.",
		},
	},
	{
		name:    "drill",
		summary: "run the isolated recovery drill and record the S12 measurements",
		usage:   "drill --manifest M --target-dsn TARGET --instance ID --chain-id CHAIN [--scenario S] [--out DIR] [--record-only] [--operation-id ID] [--backup-lag-seconds N] [--uncovered-interval-seconds N] [--signer-endpoint URL] [--rpc-url URL] [--broker-dsn DSN]",
		flags: []recoveryAdminFlag{
			{name: "manifest", usage: "manifest path (required); the control store must hold verified evidence bound to this instance and backup_id"},
			{name: "target-dsn", usage: "isolated recovery-environment DSN (required; must address " + "TXHARBOR_PG_DSN" + ", never the control store)"},
			{name: "instance", usage: "open recovery instance id (required)"},
			{name: "chain-id", usage: "scope chain id (required; the per-capability observation scope dimension)"},
			{name: "scenario", usage: "drill scenario (default full_recovery): full_recovery or one of the seven failure injections (f1_backup_unusable_or_unverified, f2_restore_interrupted_or_partial, f3_version_or_schema_incompatible, f4_external_fact_ahead, f5_old_instance_not_isolated, f6_unprovable_evidence_gap, f7_unauthorized_or_stale_approval)"},
			{name: "out", usage: "archive directory (default docs/evidence/015/drill)"},
			{name: "record-only", usage: "record from the current control-store facts without running a restore (requires accepted restore_probe evidence)"},
			{name: "operation-id", usage: "idempotency key (optional); same id+input replays the recorded outcome with zero side effects, a changed input conflicts with zero writes; omitted = a real rerun"},
			{name: "backup-lag-seconds", usage: "operator-provided backup-lag local input (optional; recorded as an explicit test input, never a production RPO measurement)"},
			{name: "uncovered-interval-seconds", usage: "operator-provided uncovered-interval local input (optional; same rule)"},
			{name: "signer-endpoint", usage: "signer boundary endpoint for reachability probing (optional)"},
			{name: "rpc-url", usage: "RPC fact-source URL for the verification phase (optional; missing = verification not_configured, never a pass)"},
			{name: "broker-dsn", usage: "broker DSN for reachability probing (optional)"},
		},
		required: []string{"manifest", "target-dsn", "instance", "chain-id"},
		notes: []string{
			"The drill executes the real recovery flow (real pg_restore into the environment's own isolated data database plus the four probes); a re-initialized empty database is never a drill. --record-only records from the current facts and requires accepted restore_probe evidence.",
			"Timing scopes are recorded separately: recovery point, db_restore_seconds, verification_seconds, per-capability release seconds, backup_lag, uncovered_interval and gap counts/dispositions. A reachable database or a completed restore is never an RTO measurement; RTO is judged end-to-end only when all seven capabilities are observed released, and an exceeded configured target is recorded not-met + alerted + escalated without permanently blocking later safe resumption.",
			"Required constraints (TXHARBOR_RECOVERY_RPO_TARGET/_RTO_TARGET/_BACKUP_FREQUENCY/_RETENTION) that are not configured are recorded as an explicit unconfigured state with their purpose; local drill values are test inputs only, never production thresholds (T000-P stays OPEN).",
			"No approval or release is created: per-capability release state is observed through the derived gate evaluation at the canonical scope chain=<chain>;capability=<capability>, and refusals are recorded as such. Evidence gaps, isolation and approvals remain the only blockers. Per-capability release times are therefore measured by a --record-only run after the real approve/release commands; a restore-mode run records the restore/verification scopes and the fail-closed observations of that moment.",
			"Requires TXHARBOR_RECOVERY_CONTROL_DSN, TXHARBOR_PG_DSN, TXHARBOR_RECOVERY_PRINCIPAL, TXHARBOR_RECOVERY_GATE_TTL and an executor participant binding; archive files are written under docs/evidence/015/ (or --out) and referenced by log_ref.",
		},
	},
}

// recoveryAdminActionByName returns the action with the given token.
func recoveryAdminActionByName(name string) (recoveryAdminAction, bool) {
	for _, action := range recoveryAdminActions {
		if action.name == name {
			return action, true
		}
	}
	return recoveryAdminAction{}, false
}

// Run runs the 015 operator command surface. `migrate up|status` (T008) is
// implemented in migrate.go, the `control ...` subcommands (T010) in
// control.go, `backup`/`verify-backup` (T021/T020) in backup.go, `restore`
// (T022) in restore.go, the instance lifecycle (T027) in instance.go, the
// isolation checklist (T028/T029) in checklist.go and `verify` (T042) in
// verify.go; every other action is still the B0 stub (help and argument
// parsing only, no behavior).
func Run(ctx context.Context, args []string, d Deps) int {
	if len(args) == 0 {
		recoveryAdminUsage(d.stderr())
		return 2
	}
	switch args[0] {
	case "help", "-h", "--help":
		return recoveryAdminHelp(args[1:], d)
	case "migrate":
		// T008/T069: the control-store provisioning path is implemented in
		// migrate.go.
		return recoveryAdminMigrate(ctx, args[1:], d)
	case "control":
		// T010: the deployment-privilege identity management path is
		// implemented in control.go; every other action remains the B0
		// help/parse stub.
		return recoveryAdminControl(ctx, args[1:], d)
	case "backup":
		// T021: produce a real backup artifact + manifest (artifact-bound,
		// unverified until verify-backup).
		return recoveryAdminBackup(ctx, args[1:], d)
	case "verify-backup":
		// T021/T020: real isolated restore verification + control-store
		// conclusion.
		return recoveryAdminVerifyBackup(ctx, args[1:], d)
	case "restore":
		// T022: precondition-gated restore bound to the open instance.
		return recoveryAdminRestore(ctx, args[1:], d)
	case "instance-open":
		// T027: open/supersede a recovery instance (executor recorded).
		return recoveryAdminInstanceOpen(ctx, args[1:], d)
	case "instance-close":
		// T027: release-guarded close of the open instance.
		return recoveryAdminInstanceClose(ctx, args[1:], d)
	case "checklist-set":
		// T029: isolation-checklist evidence collection (executor).
		return recoveryAdminChecklistSet(ctx, args[1:], d)
	case "checklist-verify":
		// T029: isolation-checklist confirmation (non-executor verifier).
		return recoveryAdminChecklistVerify(ctx, args[1:], d)
	case "verify":
		// T042: one bounded read-only V1-V9 verification step over the T038
		// orchestrator and the T039/T040 real source adapters, bound to the
		// open recovery instance.
		return recoveryAdminVerify(ctx, args[1:], d)
	case "approve":
		// T051: append-only approval decisions (approve/revoke) through the
		// T048 writer; the executor is excluded and no release is derived here.
		return recoveryAdminApprove(ctx, args[1:], d)
	case "release":
		// T051: append-only release/revoke decisions whose release basis is
		// derived and judged by the T049 writer / T012 gate.
		return recoveryAdminRelease(ctx, args[1:], d)
	case "status":
		// T051: bounded read-only per-capability reconstructed state
		// (restored/verified/released + blocking reason + refusal class).
		return recoveryAdminStatus(ctx, args[1:], d)
	case "drill":
		// T057: the isolated-environment real recovery drill and the S12
		// measurement record (T055 RTO evaluation + T056 drill model).
		return recoveryAdminDrill(ctx, args[1:], d)
	}
	action, ok := recoveryAdminActionByName(args[0])
	if !ok {
		stderr := d.stderr()
		fmt.Fprintf(stderr, "txharbor recovery-admin: unknown action %q\n", args[0])
		recoveryAdminUsage(stderr)
		return 2
	}
	return recoveryAdminStub(ctx, action, args[1:], d)
}

// recoveryAdminHelp prints the full action surface, or one action's usage when
// a known action is named.
func recoveryAdminHelp(args []string, d Deps) int {
	if len(args) == 0 {
		recoveryAdminUsage(d.stdout())
		return 0
	}
	action, ok := recoveryAdminActionByName(args[0])
	if !ok || len(args) > 1 {
		stderr := d.stderr()
		fmt.Fprintf(stderr, "txharbor recovery-admin: unknown action %q\n", args[0])
		recoveryAdminUsage(stderr)
		return 2
	}
	recoveryAdminActionUsage(d.stdout(), action)
	return 0
}

// recoveryAdminStub parses one action's arguments and refuses to act. Parse
// failures, unexpected arguments and missing required arguments are usage
// errors (2); a complete invocation reports NOT IMPLEMENTED and exits 1 — a
// stub must never return a false success and never starts a recovery flow.
func recoveryAdminStub(ctx context.Context, action recoveryAdminAction, args []string, d Deps) int {
	_ = ctx // B0: no behavior, no control-store or artifact access.
	stdout, stderr := d.stdout(), d.stderr()

	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			recoveryAdminActionUsage(stdout, action)
			return 0
		}
	}

	fs := flag.NewFlagSet("txharbor recovery-admin "+action.name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { recoveryAdminActionUsage(stderr, action) }
	values := make(map[string]*string, len(action.flags))
	for _, f := range action.flags {
		values[f.name] = fs.String(f.name, "", f.usage)
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			recoveryAdminActionUsage(stdout, action)
			return 0
		}
		return 2
	}

	if len(action.positional) > 0 {
		if fs.NArg() != 1 {
			recoveryAdminActionUsage(stderr, action)
			return 2
		}
		token := strings.TrimSpace(fs.Arg(0))
		known := false
		for _, want := range action.positional {
			if token == want {
				known = true
				break
			}
		}
		if !known {
			fmt.Fprintf(stderr, "txharbor recovery-admin %s: unknown argument %q (want %s)\n",
				action.name, fs.Arg(0), strings.Join(action.positional, "|"))
			recoveryAdminActionUsage(stderr, action)
			return 2
		}
	} else if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: unexpected argument %q\n", action.name, fs.Arg(0))
		recoveryAdminActionUsage(stderr, action)
		return 2
	}

	var missing []string
	for _, name := range action.required {
		value, ok := values[name]
		if !ok || strings.TrimSpace(*value) == "" {
			missing = append(missing, "--"+name)
		}
	}
	if len(missing) > 0 {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: missing required %s\n", action.name, strings.Join(missing, ", "))
		recoveryAdminActionUsage(stderr, action)
		return 2
	}

	fmt.Fprintf(stderr,
		"txharbor recovery-admin %s: NOT IMPLEMENTED in the B0 setup skeleton; no action taken (no backup/restore/verification/release flow is started and no success is claimed)\n",
		action.name)
	return 1
}

// recoveryAdminUsage prints the full fixed action surface.
func recoveryAdminUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: txharbor recovery-admin <action> [args]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "actions:")
	for _, action := range recoveryAdminActions {
		fmt.Fprintf(w, "  %-16s %s\n", action.name, action.summary)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Wired today: `migrate up|status` (T008), `control participant-register|identity-map-set|identity-map-show` (T010), `backup`/`verify-backup` (T020/T021), `restore` (T022), `instance-open`/`instance-close` (T027), `checklist-set`/`checklist-verify` (T028/T029), `verify` (T042), `approve`/`release`/`status` (T051) and `drill` (T057). Missing arguments and unknown actions exit 2. Required configuration values are refused by name and never defaulted; the authenticated subject comes from TXHARBOR_RECOVERY_PRINCIPAL and free text never authorizes.")
}

// recoveryAdminActionUsage prints one action's accepted argument form.
func recoveryAdminActionUsage(w io.Writer, action recoveryAdminAction) {
	fmt.Fprintf(w, "usage: txharbor recovery-admin %s\n", action.usage)
	for _, f := range action.flags {
		fmt.Fprintf(w, "  --%s\t%s\n", f.name, f.usage)
	}
	if len(action.notes) > 0 {
		fmt.Fprintln(w, "notes:")
		for _, note := range action.notes {
			fmt.Fprintf(w, "  - %s\n", note)
		}
	}
}
