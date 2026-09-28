// restore.go implements T022: the `recovery-admin restore` operator wiring.
//
// Trust boundary and operating discipline:
//
//  1. `--manifest M --target-dsn TARGET --instance ID`; the deployment
//     binding TXHARBOR_RECOVERY_INSTANCE, when present, must equal --instance
//     or the command refuses before any connection (instance_mismatch
//     discipline).
//  2. The authenticated subject is TXHARBOR_RECOVERY_PRINCIPAL; it must have
//     an active identity mapping and hold the `executor` participant role on
//     the target instance. Free text never authorizes.
//  3. A failed precondition produces an explicit blocked state with the
//     missing items listed on stderr and exit 1; the command never claims a
//     recovery. The library additionally refuses the control-store database
//     as a target, requires an explicit reason for production_main and
//     refuses targets that are not reachable.
//  4. The target DSN is never echoed: output carries the credential-free
//     target fingerprint only, and every message passes through logx.Redact
//     (FR-008).
//  5. Interruption is safe to re-enter: rebuild the target database, then
//     rerun with the same manifest (idempotent; no double/mixed state).
package recoveryadmin

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// recoveryAdminRestore implements `recovery-admin restore`.
func recoveryAdminRestore(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("txharbor recovery-admin restore", flag.ContinueOnError)
	fs.SetOutput(stderr)
	manifestPath := fs.String("manifest", "", "manifest path (required)")
	targetDSN := fs.String("target-dsn", "", "target DSN (required; isolated unless explicitly declared)")
	instanceFlag := fs.String("instance", "", "recovery instance id (required)")
	declaration := fs.String("declaration", string(recovery.TargetIsolated), "target declaration: isolated|production_main")
	reason := fs.String("reason", "", "explicit recorded reason (required for production_main; audit annotation only)")
	operationID := fs.String("operation-id", "", "idempotent operation identity (optional)")
	signerEndpoint := fs.String("signer-endpoint", "", "signer boundary endpoint for reachability probing (optional)")
	rpcURL := fs.String("rpc-url", "", "RPC fact-source URL for reachability probing (optional)")
	brokerDSN := fs.String("broker-dsn", "", "broker DSN for reachability probing (optional)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*manifestPath) == "" || strings.TrimSpace(*targetDSN) == "" ||
		strings.TrimSpace(*instanceFlag) == "" {
		recoveryAdminActionUsage(stderr, recoveryAdminActionByNameOrZero("restore"))
		return 2
	}
	decl := recovery.TargetDeclaration(strings.TrimSpace(*declaration))
	switch decl {
	case recovery.TargetIsolated, recovery.TargetProductionMain:
	default:
		fmt.Fprintf(stderr, "txharbor recovery-admin restore: unknown --declaration %q (want isolated|production_main)\n", *declaration)
		return 2
	}
	if decl == recovery.TargetProductionMain && strings.TrimSpace(*reason) == "" {
		fmt.Fprintln(stderr, "txharbor recovery-admin restore: production_main requires an explicit --reason (refusing)")
		return 1
	}
	operation, err := recoveryOpOperationID(*operationID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin restore: %s\n", logx.Redact(err.Error()))
		return 2
	}

	// The deployment binding must agree with --instance before anything is
	// opened (a mismatched invocation never reaches the database).
	instanceID, bound, code := recoveryOpInstanceContext(d, *instanceFlag, "restore")
	if code != 0 {
		return code
	}
	if !bound {
		fmt.Fprintf(stderr, "txharbor recovery-admin restore: %s is required (instance bound execution)\n", config.EnvRecoveryInstance)
		return 1
	}

	env, code := recoveryOpOpen(ctx, d, "restore")
	if env == nil {
		return code
	}
	defer env.pool.Close()

	if err := recoveryOpRequireParticipant(ctx, env, instanceID, "executor", "restore"); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin restore: refused: %s\n", logx.Redact(err.Error()))
		return 1
	}

	inputDigest := recoveryOpInputDigest(recoveryActionRestore, map[string]string{
		"manifest":    strings.TrimSpace(*manifestPath),
		"target":      recoveryTargetFingerprint(*targetDSN),
		"instance_id": instanceID,
		"declaration": string(decl),
	})
	release, recorded, err := recoveryOpBegin(ctx, env.pool, recoveryActionRestore, operation)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin restore: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if release != nil {
		defer release()
	}
	if replayed, code := recoveryOpReplay(stderr, stdout, recorded, inputDigest, operation); replayed {
		return code
	}

	programVersion, err := recovery.CurrentProgramVersion()
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin restore: %s\n", logx.Redact(err.Error()))
		return 1
	}
	result, err := recovery.ExecuteRestore(ctx, recovery.RestoreOptions{
		ManifestPath:      strings.TrimSpace(*manifestPath),
		InstanceID:        instanceID,
		ControlStore:      env.store,
		ControlDSN:        env.controlDSN,
		TargetDSN:         *targetDSN,
		TargetDeclaration: decl,
		TargetReason:      strings.TrimSpace(*reason),
		Actor:             env.principal,
		ProgramVersion:    programVersion,
		OperationID:       operation,
		PG:                recovery.LocalPGCommand{},
		SignerEndpoint:    strings.TrimSpace(*signerEndpoint),
		RPCURL:            strings.TrimSpace(*rpcURL),
		BrokerDSN:         strings.TrimSpace(*brokerDSN),
	})
	if err != nil {
		// Explicit blocked state with the missing items; no success claim.
		fmt.Fprintf(stderr, "txharbor recovery-admin restore: restored=false blocked=%d operation_id=%s principal=%s instance=%s\n",
			len(result.Blocked), recoveryOpDisplayID(operation), env.principal, instanceID)
		for _, item := range result.Blocked {
			fmt.Fprintf(stderr, "  missing_precondition=%s\n", logx.Redact(item))
		}
		if target := result.TargetFingerprint; target != "" {
			fmt.Fprintf(stderr, "  target_fingerprint=%s\n", target)
		}
		_ = recoveryOpRecord(ctx, env.pool, recoveryActionRestore, operation, env.principal, controlstore.AuditRefused,
			map[string]any{"input_digest": inputDigest, "blocked": result.Blocked, "reason": logx.Redact(err.Error())},
			map[string]any{
				"manifest_path":      strings.TrimSpace(*manifestPath),
				"target_fingerprint": result.TargetFingerprint,
				"instance_id":        instanceID,
				"declaration":        string(decl),
			})
		return 1
	}

	detail := map[string]any{
		"input_digest":       inputDigest,
		"restored":           result.Restored,
		"manifest_digest":    result.ManifestDigest,
		"target_fingerprint": result.TargetFingerprint,
		"declaration":        string(result.Declaration),
		"evidence_ref":       result.EvidenceRef,
		"checks": map[string]bool{
			"readable":                result.Checks.Readable,
			"structure_constraints":   result.Checks.StructureConstraints,
			"business_state_probes":   result.Checks.BusinessStateProbes,
			"verification_executable": result.Checks.VerificationExecutable,
		},
	}
	_ = recoveryOpRecord(ctx, env.pool, recoveryActionRestore, operation, env.principal, controlstore.AuditOK, detail,
		map[string]any{
			"manifest_path":      strings.TrimSpace(*manifestPath),
			"target_fingerprint": result.TargetFingerprint,
			"instance_id":        instanceID,
			"declaration":        string(result.Declaration),
		})
	fmt.Fprintf(stdout,
		"txharbor recovery-admin restore: restored=%t declaration=%s target_fingerprint=%s manifest=%s evidence_ref=%s readable=%t structure_constraints=%t business_state_probes=%t verification_executable=%t operation_id=%s principal=%s instance=%s\n",
		result.Restored, result.Declaration, result.TargetFingerprint, strings.TrimSpace(*manifestPath),
		result.EvidenceRef, result.Checks.Readable, result.Checks.StructureConstraints,
		result.Checks.BusinessStateProbes, result.Checks.VerificationExecutable,
		recoveryOpDisplayID(operation), env.principal, instanceID)
	for _, dep := range result.Dependencies {
		if dep.State != "ok" {
			fmt.Fprintf(stdout, "  dependency=%s state=%s detail=%s\n", dep.Name, dep.State, logx.Redact(dep.Detail))
		}
	}
	if !result.Restored {
		return 1
	}
	return 0
}
