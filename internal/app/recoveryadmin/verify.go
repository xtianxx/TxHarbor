// verify.go implements T042: the `recovery-admin verify` operator wiring.
//
// The command runs one bounded read-only V1-V9 fact-verification step through
// the T038 orchestrator (internal/recovery/verification.go) and prints the
// step's conclusions separately from the evidence-gap list.
//
// Trust boundary and operating discipline (FR-014/015/018/019/020; T038):
//
//  1. `--instance ID --scope SCOPE` are both required. The deployment binding
//     TXHARBOR_RECOVERY_INSTANCE, when present, must address the same instance
//     and the instance must be the open `recovery` instance; anything else
//     refuses as instance_mismatch (the gate's own vocabulary) and is audited.
//  2. The authenticated subject is TXHARBOR_RECOVERY_PRINCIPAL; it must have
//     an active identity mapping (a person is provable) and a participant
//     binding on the instance. Free text never authorizes; the verification is
//     read-only and grants no approval by itself.
//  3. One step reads the configured sources and commits through the T038
//     generation protocol: the (state, generation, hash) token is captured and
//     re-validated under the instance row lock on every call, so the command
//     holds no in-memory state and repeated calls append new verdict rows
//     (latest-row-wins only behind the token check) instead of flipping
//     history. The batch bound is deployment configuration with no default.
//  4. Output separates the V1-V9 conclusions (consistent/divergent/unknown/
//     stale) from the gap list (affected capabilities, required evidence,
//     owner, escalation). unknown/stale/divergent are never displayed as a
//     pass, and an open/escalated gap keeps its affected capabilities blocked.
//  5. Verification never fixes a gap and never triggers a payment, signature,
//     broadcast, replay or real downstream delivery: the source surface has no
//     effectful method, the assembled adapters are read-only, and this command
//     touches only the control store plus SELECT reads.
//  6. A missing required configuration value is refused by exact key name
//     (TXHARBOR_RPC_URL, TXHARBOR_CHAIN_ID,
//     TXHARBOR_RECOVERY_VERIFICATION_BATCH_LIMIT); a configured but invalid
//     value is refused by its key too, while a missing per-category freshness
//     tolerance stays conservative (unknown) and is never replaced by a
//     default. The control-store schema guard (T069) refuses an unknown or
//     incompatible store as control_store_unavailable before any read.
//  7. --operation-id is the persistent idempotency key: the same id with the
//     same input reads the recorded outcome back with zero side effects; a
//     different input conflicts with zero writes; omitted = a real rerun
//     (non-replay) that appends a new bounded step and converges through
//     repetition.
//  8. Exit codes: 0 = the step was accepted and recorded (regardless of the
//     conclusion mix), 1 = refusal/config/discarded, 2 = usage. Plaintext DSNs
//     never reach stdout/stderr; messages pass through logx.Redact and targets
//     are reported as credential-free fingerprints.
//
// Dependencies: T038 (internal/recovery/verification.go), T039/T040
// (internal/recovery/sources/). T041 (internal/recovery/gaps.go) owns gap
// closure; this command never closes, escalates or otherwise changes a gap.
package recoveryadmin

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
	"github.com/xtianxx/txharbor/internal/recovery/sources"
)

// recoveryActionVerify is the recovery_audit action token of
// `recovery-admin verify`. The T038 orchestrator records its own rows under
// recovery.ActionVerification; this token is the command-level outcome.
const recoveryActionVerify = "verify"

// recoveryVerifyProbeTimeout bounds one V9 boundary reachability probe. It is
// a local command bound, not a production threshold.
const recoveryVerifyProbeTimeout = 2 * time.Second

// recoveryAdminVerify implements `recovery-admin verify`.
func recoveryAdminVerify(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			recoveryAdminActionUsage(stdout, recoveryAdminActionByNameOrZero("verify"))
			return 0
		}
	}

	fs := flag.NewFlagSet("txharbor recovery-admin verify", flag.ContinueOnError)
	fs.SetOutput(stderr)
	instanceFlag := fs.String("instance", "", "recovery instance id (required)")
	scopeFlag := fs.String("scope", "", "verification scope (required): \"all\" or a JSON object")
	operationID := fs.String("operation-id", "", "idempotent operation identity (optional)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	scopeGiven := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "scope" {
			scopeGiven = true
		}
	})
	if fs.NArg() > 0 || strings.TrimSpace(*instanceFlag) == "" || !scopeGiven {
		recoveryAdminActionUsage(stderr, recoveryAdminActionByNameOrZero("verify"))
		return 2
	}
	instanceID, err := recoveryVerifyInstanceID(*instanceFlag)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify: %s\n", logx.Redact(err.Error()))
		return 2
	}
	operation, err := recoveryOpOperationID(*operationID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify: %s\n", logx.Redact(err.Error()))
		return 2
	}

	// Required deployment configuration is refused by exact key name before
	// anything is opened; no value is ever defaulted.
	getenv := d.getenv()
	if getenv == nil {
		fmt.Fprintln(stderr, "txharbor recovery-admin verify: environment lookup is not wired; refusing")
		return 1
	}
	rpcURL, ok := getenv(config.EnvRPCURL)
	if !ok || strings.TrimSpace(rpcURL) == "" {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify: %s is required (not configured); refusing\n", config.EnvRPCURL)
		return 1
	}
	chainID, err := recoveryVerifyChainID(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify: %s\n", logx.Redact(err.Error()))
		return 1
	}
	batchLimit, err := recovery.ResolveVerificationBatchLimit(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify: %s; refusing\n", logx.Redact(err.Error()))
		return 1
	}
	// A missing per-category tolerance is allowed (it keeps every conclusion
	// conservative); a configured but invalid value refuses by key name.
	for _, category := range recovery.KnownVerificationCategories() {
		if _, err := recovery.ResolveVerificationFreshness(getenv, category); err != nil {
			fmt.Fprintf(stderr, "txharbor recovery-admin verify: refusing: %s\n", logx.Redact(err.Error()))
			return 1
		}
	}

	env, code := recoveryOpOpen(ctx, d, "verify")
	if env == nil {
		return code
	}
	defer env.pool.Close()

	// The scope is canonicalized first: it is part of the operation's input
	// identity (scope_hash) and its refusal must carry the same digest a later
	// replay compares against.
	scope, scopeHash, scopeErr := recoveryVerifyNormalizeScope(*scopeFlag)
	if scopeErr != nil {
		scopeHash = recoveryVerifyRawScopeHash(strings.TrimSpace(*scopeFlag))
	}
	dataFingerprint := recoveryTargetFingerprint(env.dataDSN)
	inputDigest := recoveryOpInputDigest(recoveryActionVerify, map[string]string{
		"instance_id": instanceID,
		"scope_hash":  scopeHash,
		"actor":       env.principal,
		"chain_id":    strconv.FormatUint(chainID, 10),
		"data_target": dataFingerprint,
	})
	target := map[string]any{
		"instance_id": instanceID,
		"scope_hash":  scopeHash,
		"chain_id":    chainID,
	}
	refuse := func(refusalClass string, reason string, extra map[string]any) int {
		detail := map[string]any{"input_digest": inputDigest, "reason": logx.Redact(reason)}
		for key, value := range extra {
			detail[key] = value
		}
		recoveryVerifyWriteAudit(ctx, env.pool, recoveryVerifyAudit{
			instanceID: instanceID, operation: operation, actor: env.principal,
			result: controlstore.AuditRefused, refusalClass: refusalClass,
			detail: detail, target: target,
		})
		if refusalClass != "" {
			fmt.Fprintf(stderr, "txharbor recovery-admin verify: refused: refusal_class=%s %s\n",
				refusalClass, logx.Redact(reason))
		} else {
			fmt.Fprintf(stderr, "txharbor recovery-admin verify: refused: %s\n", logx.Redact(reason))
		}
		return 1
	}

	// The deployment binding never silently overrides --instance.
	if bound, ok := d.getenvValue(config.EnvRecoveryInstance); ok && strings.TrimSpace(bound) != "" &&
		!recoveryVerifyInstanceBindingMatches(instanceID, bound) {
		return refuse(string(recovery.RefusalInstanceMismatch),
			fmt.Sprintf("--instance %s does not match %s=%s (instance bound execution; refusing)",
				instanceID, config.EnvRecoveryInstance, strings.TrimSpace(bound)),
			map[string]any{"bound_env": strings.TrimSpace(bound)})
	}

	// The instance must exist and be the open recovery instance (a stale or
	// wrong binding refuses exactly like a mismatched deployment binding).
	kind, state, found, err := recoveryVerifyInstanceRow(ctx, env.pool, instanceID)
	if err != nil {
		return refuse(string(recovery.RefusalControlStoreUnavailable), err.Error(), nil)
	}
	if !found || kind != "recovery" || state != "open" {
		return refuse(string(recovery.RefusalInstanceMismatch),
			fmt.Sprintf("instance %s is not the open recovery instance (found=%t kind=%s state=%s); verification binds to the open recovery instance",
				instanceID, found, kind, state),
			map[string]any{"found": found, "kind": kind, "state": state})
	}

	// The principal must be provable and registered on the instance. Any
	// participant role qualifies (verification is read-only and grants no
	// approval), but a principal without a binding cannot act at all.
	refusalClass, err := recoveryVerifyRequireParticipant(ctx, env, instanceID)
	if err != nil {
		return refuse(refusalClass, err.Error(), nil)
	}

	if scopeErr != nil {
		return refuse(string(recovery.RefusalScopeMismatch), scopeErr.Error(), nil)
	}

	// Persistent idempotency: a recorded outcome under the same operation id
	// replays (same input) or conflicts (different input) with zero writes.
	release, recorded, err := recoveryOpBegin(ctx, env.pool, recoveryActionVerify, operation)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if release != nil {
		defer release()
	}
	if replayed, code := recoveryVerifyReplay(stderr, stdout, recorded, inputDigest, operation, instanceID, scopeHash); replayed {
		return code
	}

	// Read-only dependencies and the real T039/T040 adapters.
	deps, err := recoveryVerifyOpenDependencies(ctx, env, getenv, rpcURL, chainID, dataFingerprint)
	if err != nil {
		// A dependency failure has no dedicated closed-set class; the reason
		// names the failing boundary and the audit still records the refusal.
		return refuse("", err.Error(), nil)
	}
	defer deps.close()

	// One bounded read-only step. The step operation id is the operator's
	// --operation-id, or a derived unique id for a real rerun (non-replay).
	stepOperation := operation
	if stepOperation == "" {
		stepOperation = "verify:" + uuid.NewString()
	}
	verification, err := recovery.NewVerification(env.store, recovery.VerificationOptions{
		BatchLimit: batchLimit,
		Freshness: func(category recovery.VerificationCategory) (time.Duration, error) {
			return recovery.ResolveVerificationFreshness(getenv, category)
		},
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify: %s\n", logx.Redact(err.Error()))
		return 1
	}

	batch, err := verification.Verify(ctx, recovery.VerificationRequest{
		InstanceID:  instanceID,
		Actor:       env.principal,
		Scope:       scope,
		OperationID: stepOperation,
		Sources:     deps.sources,
	})
	if err != nil {
		refusalClass := recoveryVerifyRefusalClass(err)
		recoveryVerifyWriteAudit(ctx, env.pool, recoveryVerifyAudit{
			instanceID: instanceID, operation: stepOperation, actor: env.principal,
			result: controlstore.AuditRefused, refusalClass: refusalClass,
			detail: map[string]any{"input_digest": inputDigest, "reason": logx.Redact(err.Error()), "scope_hash": scopeHash},
			target: target,
		})
		if refusalClass != "" {
			fmt.Fprintf(stderr, "txharbor recovery-admin verify: refused: refusal_class=%s %s (no verification items were accepted for this step)\n",
				refusalClass, logx.Redact(err.Error()))
		} else {
			fmt.Fprintf(stderr, "txharbor recovery-admin verify: refused: %s (no verification items were accepted for this step)\n",
				logx.Redact(err.Error()))
		}
		return 1
	}
	if batch.Discarded {
		// A concurrent evidence write committed first: the protocol persisted
		// only its own discard audit row. No command outcome is recorded under
		// the operation id, so a rerun re-reads the authoritative state and
		// converges instead of replaying the discard forever.
		fmt.Fprintf(stderr,
			"txharbor recovery-admin verify: discarded=true instance=%s generation=%d operation_id=%s (a concurrent evidence write committed first; nothing from this step was persisted — rerun to converge)\n",
			instanceID, batch.Generation, recoveryOpDisplayID(operation))
		return 1
	}

	counts := map[recovery.VerificationConclusion]int{}
	for _, item := range batch.Items {
		counts[item.Conclusion]++
	}
	recoveryVerifyWriteAudit(ctx, env.pool, recoveryVerifyAudit{
		instanceID: instanceID, operation: stepOperation, actor: env.principal,
		result: controlstore.AuditOK,
		detail: map[string]any{
			"input_digest":      inputDigest,
			"batch_id":          batch.BatchID,
			"generation":        batch.Generation,
			"scope_hash":        scopeHash,
			"item_count":        len(batch.Items),
			"consistent":        counts[recovery.ConclusionConsistent],
			"divergent":         counts[recovery.ConclusionDivergent],
			"unknown":           counts[recovery.ConclusionUnknown],
			"stale":             counts[recovery.ConclusionStale],
			"gaps":              len(batch.Gaps),
			"step_operation_id": stepOperation,
		},
		target: target,
	})

	return recoveryVerifyPrint(ctx, stdout, stderr, env, batch, counts, instanceID, scopeHash, chainID, operation, stepOperation, dataFingerprint)
}

// recoveryVerifyDeps is the opened read-only dependency set of one invocation
// (data DB pool, canonical chain client, raw quantity surface, assembled
// V1-V9 adapters). It is closed before the command returns; no state survives
// an invocation.
type recoveryVerifyDeps struct {
	dataPool *pgxpool.Pool
	rpc      *eth.Client
	quantity *gethrpc.Client
	sources  []recovery.VerificationSource
}

func (d *recoveryVerifyDeps) close() {
	if d == nil {
		return
	}
	if d.quantity != nil {
		d.quantity.Close()
		d.quantity = nil
	}
	if d.rpc != nil {
		d.rpc.Close()
		d.rpc = nil
	}
	if d.dataPool != nil {
		d.dataPool.Close()
		d.dataPool = nil
	}
}

// recoveryVerifyOpenDependencies opens the read-only verification dependencies
// and assembles the real T039/T040 source adapters. Every failure leaves a
// clean refusal: nothing was verified and no evidence row is written.
func recoveryVerifyOpenDependencies(ctx context.Context, env *recoveryOpEnv, getenv func(string) (string, bool),
	rpcURL string, chainID uint64, dataFingerprint string) (*recoveryVerifyDeps, error) {
	dataPool, err := db.OpenPool(ctx, env.dataDSN, recoveryOpConnectTimeout)
	if err != nil {
		return nil, fmt.Errorf("blocked: the data DB is unreachable: %s", logx.Redact(err.Error()))
	}
	rpc, err := eth.Dial(ctx, rpcURL, recoveryOpConnectTimeout)
	if err != nil {
		dataPool.Close()
		return nil, fmt.Errorf("blocked: %s is unusable: %s", config.EnvRPCURL, logx.Redact(err.Error()))
	}
	if err := rpc.CheckChainID(ctx, new(big.Int).SetUint64(chainID), recoveryOpConnectTimeout); err != nil {
		rpc.Close()
		dataPool.Close()
		return nil, fmt.Errorf("blocked: the canonical endpoint does not serve %s=%d: %s",
			config.EnvChainID, chainID, logx.Redact(err.Error()))
	}
	dialCtx, cancel := context.WithTimeout(ctx, recoveryOpConnectTimeout)
	quantity, err := gethrpc.DialContext(dialCtx, strings.TrimSpace(rpcURL))
	cancel()
	if err != nil {
		rpc.Close()
		dataPool.Close()
		return nil, fmt.Errorf("blocked: the raw chain-quantity surface is unavailable: %s", logx.Redact(err.Error()))
	}
	chainSources, err := sources.NewChainSources(sources.ChainOptions{
		Data:       dataPool,
		RPC:        rpc,
		ChainID:    chainID,
		DataTarget: dataFingerprint,
		Quantity:   quantity,
	})
	if err != nil {
		quantity.Close()
		rpc.Close()
		dataPool.Close()
		return nil, fmt.Errorf("blocked: assemble the V1-V4 read-only sources: %s", logx.Redact(err.Error()))
	}
	opsSources, err := sources.NewOpsSources(sources.OpsOptions{
		Data:    dataPool,
		Control: env.store,
		RPC:     rpc,
		// No broker reader exists in this tree (T053 wires the event layer):
		// V6's offset comparison stays unprovable (unknown) rather than a
		// fabricated zero.
		BrokerOffsets: nil,
		Dependencies:  recoveryVerifyProbes(getenv),
		DataTarget:    dataFingerprint,
	})
	if err != nil {
		quantity.Close()
		rpc.Close()
		dataPool.Close()
		return nil, fmt.Errorf("blocked: assemble the V5-V9 read-only sources: %s", logx.Redact(err.Error()))
	}
	return &recoveryVerifyDeps{
		dataPool: dataPool,
		rpc:      rpc,
		quantity: quantity,
		sources:  append(chainSources, opsSources...),
	}, nil
}

// recoveryVerifyPrint renders the accepted step: the V1-V9 conclusions, the
// open/escalated gap list, and the no-pass note. Nothing here writes.
func recoveryVerifyPrint(ctx context.Context, stdout, stderr io.Writer, env *recoveryOpEnv,
	batch recovery.VerificationBatch, counts map[recovery.VerificationConclusion]int,
	instanceID, scopeHash string, chainID uint64, operation, stepOperation, dataFingerprint string) int {
	items := append([]recovery.VerificationItem(nil), batch.Items...)
	sort.Slice(items, func(i, j int) bool {
		if items[i].Category != items[j].Category {
			return items[i].Category < items[j].Category
		}
		if items[i].ObjectKey != items[j].ObjectKey {
			return items[i].ObjectKey < items[j].ObjectKey
		}
		return items[i].ItemID < items[j].ItemID
	})

	gaps, gapErr := recoveryVerifyOpenGaps(ctx, env.pool, instanceID)
	if gapErr != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin verify: warning: the open-gap list could not be read: %s\n",
			logx.Redact(gapErr.Error()))
		gaps = make([]recoveryVerifyGap, 0, len(batch.Gaps))
		for _, gap := range batch.Gaps {
			gaps = append(gaps, recoveryVerifyGapFromRecord(gap))
		}
	}
	openedByBatch := make(map[string]bool, len(batch.Gaps))
	for _, gap := range batch.Gaps {
		openedByBatch[gap.GapID] = true
	}
	openGapCount := strconv.Itoa(len(gaps))
	if gapErr != nil {
		openGapCount = "unreadable"
	}

	fmt.Fprintf(stdout,
		"txharbor recovery-admin verify: instance=%s scope_hash=%s chain_id=%d data_target=%s operation_id=%s principal=%s batch_id=%s generation=%d items=%d consistent=%d divergent=%d unknown=%d stale=%d open_gaps=%s\n",
		instanceID, scopeHash, chainID, dataFingerprint, recoveryOpDisplayID(operation), env.principal,
		batch.BatchID, batch.Generation, len(items),
		counts[recovery.ConclusionConsistent], counts[recovery.ConclusionDivergent],
		counts[recovery.ConclusionUnknown], counts[recovery.ConclusionStale], openGapCount)
	if operation == "" {
		fmt.Fprintf(stdout,
			"  derived_step_operation_id=%s (a real rerun; pass --operation-id to make a step replayable)\n",
			stepOperation)
	}
	for _, item := range items {
		fmt.Fprintf(stdout, "  conclusion category=%s object_key=%s conclusion=%s observed_at=%s evidence_refs=%s reason=%s\n",
			item.Category, item.ObjectKey, item.Conclusion,
			item.ObservedAt.UTC().Format(time.RFC3339),
			displayOrNone(strings.Join(item.EvidenceRefs, ",")),
			displayOrNone(logx.Redact(item.Reason)))
	}
	for _, gap := range gaps {
		opened := openedByBatch[gap.gapID]
		if gapErr != nil {
			// The batch list is the fallback source: those gaps were just
			// established or confirmed by this step.
			opened = true
		}
		fmt.Fprintf(stdout,
			"  gap gap_id=%s object_key=%s state=%s affected_capabilities=%s required_evidence=%s owner=%s escalation_ref=%s opened_by_this_step=%t\n",
			gap.gapID, gap.objectKey, gap.state, displayOrNone(gap.affectedCapabilities),
			logx.Redact(gap.requiredEvidence), displayOrNone(gap.owner), displayOrNone(gap.escalationRef), opened)
	}
	nonConsistent := len(items) - counts[recovery.ConclusionConsistent]
	switch {
	case gapErr != nil:
		fmt.Fprintf(stdout,
			"  note: %d item(s) are not consistent; the full open/escalated gap list is unreadable (see the warning) and the %d gap(s) above are the gaps of this step. Divergent/unknown/stale are not a pass; verification never auto-fixes a gap and never triggers payment, signature, broadcast, replay or downstream delivery.\n",
			nonConsistent, len(gaps))
	case nonConsistent > 0 || len(gaps) > 0:
		fmt.Fprintf(stdout,
			"  note: %d item(s) are not consistent and %d gap(s) are open/escalated; divergent/unknown/stale are not a pass and an open gap keeps its affected capabilities blocked. Verification never auto-fixes a gap and never triggers payment, signature, broadcast, replay or downstream delivery.\n",
			nonConsistent, len(gaps))
	default:
		fmt.Fprintln(stdout,
			"  note: every item is consistent and no open/escalated gap exists for this instance; the step remains read-only and grants no release by itself.")
	}
	return 0
}

// recoveryVerifyReplay reports a recorded outcome for an already-seen
// operation. The same input replays exactly (zero side effects); a different
// input conflicts with zero writes.
func recoveryVerifyReplay(stderr, stdout io.Writer, recorded *recoveryOpRecorded,
	inputDigest, operation, instanceID, scopeHash string) (bool, int) {
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
		fmt.Fprintf(stdout,
			"txharbor recovery-admin verify: replayed=true operation_id=%s result=ok instance=%s scope_hash=%s batch_id=%v generation=%v items=%v consistent=%v divergent=%v unknown=%v stale=%v open_gaps=%v (read back from the recorded outcome; zero writes)\n",
			operation, instanceID, scopeHash,
			detail["batch_id"], detail["generation"], detail["item_count"],
			detail["consistent"], detail["divergent"], detail["unknown"], detail["stale"], detail["gaps"])
		return true, 0
	}
	fmt.Fprintf(stderr,
		"txharbor recovery-admin verify: replayed=true operation_id=%s result=%s instance=%s (the recorded refusal replays; zero side effects)\n",
		operation, recorded.Result, instanceID)
	return true, 1
}

// recoveryVerifyRefusalClass maps a T038 refusal onto the closed refusal-class
// set. A bound/observation refusal has no dedicated class: the reason text
// carries the detail and the class stays empty.
func recoveryVerifyRefusalClass(err error) string {
	switch {
	case errors.Is(err, controlstore.ErrInstanceNotFound), errors.Is(err, controlstore.ErrInstanceNotOpen):
		return string(recovery.RefusalInstanceMismatch)
	case controlstore.IsControlStoreUnavailable(err):
		return string(recovery.RefusalControlStoreUnavailable)
	default:
		return ""
	}
}

// recoveryVerifyRequireParticipant enforces the T042 subject rule: the
// authenticated principal must be provable (an active identity mapping) and
// must hold a participant binding on the instance. Any participant role
// qualifies; a principal without a binding cannot act at all. The returned
// refusal class is the closed-set class for the audited refusal.
func recoveryVerifyRequireParticipant(ctx context.Context, env *recoveryOpEnv, instanceID string) (string, error) {
	if _, ok, err := env.store.ActivePersonID(ctx, env.principal); err != nil {
		return string(recovery.RefusalControlStoreUnavailable), fmt.Errorf("resolve the principal identity mapping: %w", err)
	} else if !ok {
		return string(recovery.RefusalApprovalIdentityUnverified), fmt.Errorf(
			"%s has no active identity mapping; register the mapping first (cannot prove a person)", env.principal)
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
	return string(recovery.RefusalApprovalIdentityUnverified), fmt.Errorf(
		"%s is not registered as a participant on instance %s; register the participant first", env.principal, instanceID)
}

// recoveryVerifyInstanceRow reads the instance kind/state once, for the
// open-recovery-instance guard.
func recoveryVerifyInstanceRow(ctx context.Context, pool *pgxpool.Pool, instanceID string) (kind, state string, found bool, err error) {
	err = pool.QueryRow(ctx, `SELECT kind, state FROM recovery_instance WHERE instance_id = $1`, instanceID).
		Scan(&kind, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, fmt.Errorf("read recovery instance %s: %w", instanceID, err)
	}
	return kind, state, true, nil
}

// recoveryVerifyGap is one gap line of the output.
type recoveryVerifyGap struct {
	gapID                string
	objectKey            string
	state                string
	affectedCapabilities string
	requiredEvidence     string
	owner                string
	escalationRef        string
}

// recoveryVerifyGapFromRecord renders one already-loaded gap as an output
// line (the fallback when the direct read is unavailable).
func recoveryVerifyGapFromRecord(gap recovery.Gap) recoveryVerifyGap {
	capabilities := make([]string, 0, len(gap.AffectedCapabilities))
	for _, capability := range gap.AffectedCapabilities {
		capabilities = append(capabilities, string(capability))
	}
	return recoveryVerifyGap{
		gapID:                gap.GapID,
		objectKey:            gap.ObjectKey,
		state:                string(gap.State),
		affectedCapabilities: strings.Join(capabilities, ","),
		requiredEvidence:     string(gap.RequiredEvidence),
		owner:                gap.Owner,
		escalationRef:        gap.EscalationRef,
	}
}

// recoveryVerifyOpenGaps reads the instance's open and escalated gaps (both
// states block their affected capabilities; escalation is not closure). The
// read is strictly read-only: this command never closes, escalates or
// otherwise changes a gap.
func recoveryVerifyOpenGaps(ctx context.Context, pool *pgxpool.Pool, instanceID string) ([]recoveryVerifyGap, error) {
	const sql = `
SELECT gap_id::text, object_key, state, affected_capabilities, required_evidence::text,
       COALESCE(owner, ''), COALESCE(escalation_ref, '')
  FROM recovery_gap
 WHERE instance_id = $1 AND state IN ('open', 'escalated')
 ORDER BY object_key, gap_id`
	rows, err := pool.Query(ctx, sql, instanceID)
	if err != nil {
		return nil, fmt.Errorf("read the open gap list: %w", err)
	}
	defer rows.Close()
	var gaps []recoveryVerifyGap
	for rows.Next() {
		var (
			gap          recoveryVerifyGap
			capabilities []string
		)
		if err := rows.Scan(&gap.gapID, &gap.objectKey, &gap.state, &capabilities,
			&gap.requiredEvidence, &gap.owner, &gap.escalationRef); err != nil {
			return nil, fmt.Errorf("scan the open gap list: %w", err)
		}
		gap.affectedCapabilities = strings.Join(capabilities, ",")
		gaps = append(gaps, gap)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the open gap list: %w", err)
	}
	return gaps, nil
}

// recoveryVerifyAudit is one command-outcome audit row of `verify`.
type recoveryVerifyAudit struct {
	instanceID   string
	operation    string
	actor        string
	result       string
	refusalClass string
	detail       map[string]any
	target       map[string]any
}

// recoveryVerifyWriteAudit appends the command-outcome audit row (best
// effort: the result is already decided, and an audit failure never changes
// it). A row bound to a non-existent instance falls back to an instance-less
// row so the refusal still leaves a trace; the evaluated instance id stays in
// the target payload.
func recoveryVerifyWriteAudit(ctx context.Context, pool *pgxpool.Pool, rec recoveryVerifyAudit) {
	detailJSON, err := json.Marshal(rec.detail)
	if err != nil {
		return
	}
	targetJSON, err := json.Marshal(rec.target)
	if err != nil {
		return
	}
	write := func(instanceID string) error {
		return controlstore.WriteAudit(ctx, pool, controlstore.AuditRecord{
			InstanceID:   instanceID,
			Actor:        rec.actor,
			Action:       recoveryActionVerify,
			Target:       targetJSON,
			Detail:       detailJSON,
			Result:       rec.result,
			RefusalClass: rec.refusalClass,
			OperationID:  rec.operation,
		})
	}
	if err := write(rec.instanceID); err == nil {
		return
	}
	if rec.instanceID != "" {
		_ = write("")
	}
}

// recoveryVerifyInstanceID canonicalizes --instance.
func recoveryVerifyInstanceID(raw string) (string, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("--instance %q is not a UUID", raw)
	}
	return parsed.String(), nil
}

// recoveryVerifyInstanceBindingMatches compares --instance with the
// deployment binding; UUIDs compare canonically, any other value exactly.
func recoveryVerifyInstanceBindingMatches(requested, bound string) bool {
	bound = strings.TrimSpace(bound)
	if bound == "" {
		return true
	}
	if parsed, err := uuid.Parse(bound); err == nil {
		if req, err := uuid.Parse(requested); err == nil {
			return req.String() == parsed.String()
		}
	}
	return bound == requested
}

// recoveryVerifyChainID resolves the required TXHARBOR_CHAIN_ID.
func recoveryVerifyChainID(getenv func(string) (string, bool)) (uint64, error) {
	raw, ok := getenv(config.EnvChainID)
	if !ok || strings.TrimSpace(raw) == "" {
		return 0, fmt.Errorf("%s is required (not configured); refusing", config.EnvChainID)
	}
	value, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil || value == 0 || value > 1<<62 {
		return 0, fmt.Errorf("%s=%q is not a supported chain id", config.EnvChainID, raw)
	}
	return value, nil
}

// recoveryVerifyNormalizeScope canonicalizes --scope: the literal "all" (the
// full V1-V9 catalogue) or a JSON object (canonicalized, numbers kept exact).
// Everything else, including empty, is refused (the caller maps it to
// scope_mismatch); no scope is ever invented.
func recoveryVerifyNormalizeScope(raw string) ([]byte, string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil, "", errors.New("--scope is required and is never defaulted")
	}
	if value == "all" {
		scope := []byte(`{"scope":"all"}`)
		return scope, recoveryVerifyScopeHash(scope), nil
	}
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		return nil, "", fmt.Errorf("--scope %q is neither \"all\" nor a JSON object: %v", raw, err)
	}
	if object == nil {
		return nil, "", fmt.Errorf("--scope %q is neither \"all\" nor a JSON object", raw)
	}
	if decoder.More() {
		return nil, "", fmt.Errorf("--scope %q carries trailing JSON after the first object", raw)
	}
	scope, err := json.Marshal(object)
	if err != nil {
		return nil, "", fmt.Errorf("normalize --scope: %v", err)
	}
	return scope, recoveryVerifyScopeHash(scope), nil
}

// recoveryVerifyScopeHash is the canonical scope digest ("scope_hash")
// recorded in the output and in the audited operation identity.
func recoveryVerifyScopeHash(scope []byte) string {
	return recoveryOpInputDigest("verify_scope", map[string]string{"scope": string(scope)})
}

// recoveryVerifyRawScopeHash is the digest of a refused scope input: the
// refusal must be replayable under the same operation id and the same input.
func recoveryVerifyRawScopeHash(raw string) string {
	return recoveryOpInputDigest("verify_scope_raw", map[string]string{"scope": raw})
}

// recoveryVerifyProbes assembles the V9 readiness probes from the
// deployment's declared boundary configuration: the signer boundary
// (TXHARBOR_TX_SIGNER_URL) and the broker endpoints
// (TXHARBOR_KAFKA_BROKERS). A boundary that is not configured is not part of
// the deployed surface and is not probed; the V9 source reports an empty
// probe set as unproven by itself. The probes perform reachability checks
// only: no credential, key or signer session is ever acquired.
func recoveryVerifyProbes(getenv func(string) (string, bool)) []sources.DependencyProbe {
	var probes []sources.DependencyProbe
	if endpoint := strings.TrimSpace(recoveryVerifyEnv(getenv, config.EnvTxSignerURL)); endpoint != "" {
		probes = append(probes, recoveryVerifyBoundaryProbe{
			name: "signer-boundary",
			check: func(ctx context.Context) error {
				boundary, err := recovery.CheckSignerBoundary(ctx, endpoint)
				if err != nil {
					return err
				}
				if !boundary.Reachable {
					return fmt.Errorf("the signer boundary is unreachable at %s", boundary.Endpoint)
				}
				return nil
			},
		})
	}
	if brokers := strings.TrimSpace(recoveryVerifyEnv(getenv, config.EnvKafkaBrokers)); brokers != "" {
		for i, broker := range strings.Split(brokers, ",") {
			endpoint := strings.TrimSpace(broker)
			if endpoint == "" {
				continue
			}
			probes = append(probes, recoveryVerifyBoundaryProbe{
				name: fmt.Sprintf("kafka-broker-%d", i+1),
				check: func(ctx context.Context) error {
					return recoveryVerifyTCPReachable(ctx, endpoint)
				},
			})
		}
	}
	return probes
}

// recoveryVerifyEnv reads one optional deployment key.
func recoveryVerifyEnv(getenv func(string) (string, bool), key string) string {
	if getenv == nil {
		return ""
	}
	value, _ := getenv(key)
	return value
}

// recoveryVerifyBoundaryProbe is one read-only V9 reachability probe.
type recoveryVerifyBoundaryProbe struct {
	name  string
	check func(context.Context) error
}

// Name implements sources.DependencyProbe.
func (p recoveryVerifyBoundaryProbe) Name() string { return p.name }

// Check implements sources.DependencyProbe.
func (p recoveryVerifyBoundaryProbe) Check(ctx context.Context) error { return p.check(ctx) }

// recoveryVerifyTCPReachable is a bounded TCP reachability check (no
// protocol, no write).
func recoveryVerifyTCPReachable(ctx context.Context, endpoint string) error {
	target := endpoint
	if parsed, err := url.Parse(endpoint); err == nil && parsed.Host != "" {
		target = parsed.Host
	}
	dialCtx, cancel := context.WithTimeout(ctx, recoveryVerifyProbeTimeout)
	defer cancel()
	dialer := net.Dialer{Timeout: recoveryVerifyProbeTimeout}
	conn, err := dialer.DialContext(dialCtx, "tcp", target)
	if err != nil {
		return fmt.Errorf("tcp reachability failed: %w", err)
	}
	_ = conn.Close()
	return nil
}
