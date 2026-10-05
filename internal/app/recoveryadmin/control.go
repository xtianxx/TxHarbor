// control.go implements T010: the 015 control-plane management CLI
// (`txharbor recovery-admin control participant-register|identity-map-set|
// identity-map-show`), the `recovery_control_manage` deployment-privilege path
// of approval-matrix.md §1/§2.
//
// Trust boundary and operating discipline:
//
//  1. No valid configuration means default deny. The independent control DSN
//     (TXHARBOR_RECOVERY_CONTROL_DSN) and the data DSN (TXHARBOR_PG_DSN) must
//     both be configured, and their targets must differ: if the control store
//     addressed the data DB, restoring the data DB would roll the identity
//     mappings back — exactly what F19 forbids. Only the control DSN is ever
//     opened; the data DSN is parsed only to prove the two targets differ.
//  2. The authenticated subject is bound from the deployment-controlled
//     TXHARBOR_RECOVERY_PRINCIPAL in the canonical <kind>:<id> form and never
//     from a flag. No principal is preset (a pristine control store has no
//     mappings, participants or approvers); every write records the subject as
//     its audit actor. No second approver is required in this phase (a single
//     controlled deployment subject), and a single-privilege maintenance
//     cannot prove two-person truth by itself — F19 handles that by
//     invalidating the approvals a mapping change touches.
//  3. --operator and --reason are free-text audit annotations only; they never
//     authorize anything. --operation-id is the persistent idempotency key of
//     every write: the same input replays the recorded outcome
//     (recorded=true, zero writes), a different input refuses with
//     operation_conflict and zero writes.
//  4. person_id is never accepted on participant-register: it is resolved from
//     the active recovery_identity mapping inside the store transaction. A
//     principal without an active mapping cannot prove a person, so the
//     registration refuses and the refusal is audited.
//  5. identity-map-set/identity-map-revoke immediately invalidate the existing
//     approvals that relied on the mapping (F19): each affected open-instance
//     approve row gets a refusal-path audit row
//     (approval_identity_unverified) and must be re-approved against the
//     current mapping. identity-map-show is strictly read-only and writes no
//     audit rows.
//  6. Plaintext DSNs never reach the output: every failure message passes
//     through logx.Redact and target identity is reported as fingerprints.
package recoveryadmin

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// recoveryControlConnectTimeout bounds the control-store connect/ping of the
// management path (a local command bound, not a production threshold).
const recoveryControlConnectTimeout = 5 * time.Second

// controlEnv is the opened management context: the authenticated principal,
// the control-store pool/store and the credential-free control target
// fingerprint.
type controlEnv struct {
	principal   string
	fingerprint string
	pool        *pgxpool.Pool
	store       *controlstore.Store
}

// recoveryControlOpen binds the management trust boundary and opens the
// version-guarded control store. Every failure is fail-closed and reports by
// exact configuration key name, never by DSN value.
func recoveryControlOpen(ctx context.Context, d Deps) (*controlEnv, int) {
	stderr := d.stderr()
	getenv := d.getenv()
	if getenv == nil {
		fmt.Fprintln(stderr, "txharbor recovery-admin control: environment lookup is not wired; refusing")
		return nil, 1
	}

	controlDSN, ok := getenv(config.EnvRecoveryControlDSN)
	if !ok || strings.TrimSpace(controlDSN) == "" {
		fmt.Fprintf(stderr, "txharbor recovery-admin control: %s is required (not configured); refusing\n",
			config.EnvRecoveryControlDSN)
		return nil, 1
	}
	dataDSN, ok := getenv(config.EnvPGDSN)
	if !ok || strings.TrimSpace(dataDSN) == "" {
		fmt.Fprintf(stderr,
			"txharbor recovery-admin control: %s is required to prove the identity store is not the data DB (not configured); refusing\n",
			config.EnvPGDSN)
		return nil, 1
	}

	controlTarget, err := controlstore.ParseDSNTarget(controlDSN)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin control: %s is invalid: %s\n",
			config.EnvRecoveryControlDSN, logx.Redact(err.Error()))
		return nil, 1
	}
	dataTarget, err := controlstore.ParseDSNTarget(dataDSN)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin control: %s is invalid: %s\n",
			config.EnvPGDSN, logx.Redact(err.Error()))
		return nil, 1
	}
	if controlTarget.SameDatabase(dataTarget) {
		fmt.Fprintf(stderr,
			"txharbor recovery-admin control: %s and %s address the same database target; identity data must never be recoverable from the data DB (refusing)\n",
			config.EnvRecoveryControlDSN, config.EnvPGDSN)
		return nil, 1
	}

	principalRaw, ok := getenv(config.EnvRecoveryPrincipal)
	if !ok || strings.TrimSpace(principalRaw) == "" {
		fmt.Fprintf(stderr, "txharbor recovery-admin control: %s is required (authenticated management subject; default deny)\n",
			config.EnvRecoveryPrincipal)
		return nil, 1
	}
	if principalRaw != strings.TrimSpace(principalRaw) {
		fmt.Fprintf(stderr, "txharbor recovery-admin control: %s must not carry surrounding whitespace\n",
			config.EnvRecoveryPrincipal)
		return nil, 1
	}
	principal, err := controlstore.NormalizePrincipal(principalRaw)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin control: %s is invalid: %s\n",
			config.EnvRecoveryPrincipal, logx.Redact(err.Error()))
		return nil, 1
	}

	pool, err := db.OpenPool(ctx, controlDSN, recoveryControlConnectTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin control: control store unavailable: %s\n", logx.Redact(err.Error()))
		return nil, 1
	}
	store, err := controlstore.NewStore(ctx, pool)
	if err != nil {
		pool.Close()
		fmt.Fprintf(stderr, "txharbor recovery-admin control: control store unavailable: %s\n", logx.Redact(err.Error()))
		return nil, 1
	}
	return &controlEnv{
		principal:   principal,
		fingerprint: controlTarget.DataTargetFingerprint().TargetFingerprint,
		pool:        pool,
		store:       store,
	}, 0
}

// recoveryAdminControl dispatches the management subcommands.
func recoveryAdminControl(ctx context.Context, args []string, d Deps) int {
	if len(args) == 0 {
		recoveryControlUsage(d.stderr())
		return 2
	}
	switch args[0] {
	case "help", "-h", "--help":
		recoveryControlUsage(d.stdout())
		return 0
	case "participant-register":
		return controlParticipantRegister(ctx, args[1:], d)
	case "identity-map-set":
		return controlIdentityMapSet(ctx, args[1:], d)
	case "identity-map-show":
		return controlIdentityMapShow(ctx, args[1:], d)
	default:
		stderr := d.stderr()
		fmt.Fprintf(stderr, "txharbor recovery-admin control: unknown subcommand %q\n", args[0])
		recoveryControlUsage(stderr)
		return 2
	}
}

// controlParticipantRegister implements `control participant-register`: bind
// one authenticated principal to an open recovery instance under one role,
// with person_id resolved from the active identity mapping (never supplied).
func controlParticipantRegister(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("recovery-admin control participant-register", flag.ContinueOnError)
	fs.SetOutput(stderr)
	instanceID := fs.String("instance", "", "open recovery instance UUID (required)")
	principalRaw := fs.String("principal", "", "registered principal <kind>:<id> (required)")
	role := fs.String("role", "", "participant role: executor|verifier|approver (required)")
	bindingSource := fs.String("binding-source", controlstore.BindingSourceDeployConfig, "binding source: deploy_config|auth_principal")
	proofRef := fs.String("proof-ref", "", "deployment proof/config reference (audit carriage, optional)")
	operator := fs.String("operator", "", "declared operator identity (required, audit annotation only)")
	reason := fs.String("reason", "", "operator reason (required, audit annotation only)")
	operationID := fs.String("operation-id", "", "idempotent operation identity (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*instanceID) == "" || strings.TrimSpace(*principalRaw) == "" ||
		strings.TrimSpace(*role) == "" || strings.TrimSpace(*operator) == "" ||
		strings.TrimSpace(*reason) == "" || strings.TrimSpace(*operationID) == "" {
		recoveryControlUsage(stderr)
		return 2
	}
	principal, err := controlstore.NormalizePrincipal(*principalRaw)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin control: --principal: %s\n", logx.Redact(err.Error()))
		return 2
	}
	operation, err := controlParseOperationID(*operationID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin control: %s\n", logx.Redact(err.Error()))
		return 2
	}
	if err := controlParseAuditAnnotations(*operator, *reason); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin control: %s\n", logx.Redact(err.Error()))
		return 2
	}

	env, code := recoveryControlOpen(ctx, d)
	if env == nil {
		return code
	}
	defer env.pool.Close()

	result, err := env.store.RegisterParticipant(ctx, controlstore.RegisterParticipantRequest{
		InstanceID:    strings.TrimSpace(*instanceID),
		Principal:     principal,
		Role:          strings.TrimSpace(*role),
		BindingSource: strings.TrimSpace(*bindingSource),
		ProofRef:      strings.TrimSpace(*proofRef),
		Actor:         env.principal,
		Operator:      strings.TrimSpace(*operator),
		Reason:        strings.TrimSpace(*reason),
		OperationID:   operation,
	})
	switch {
	case errors.Is(err, controlstore.ErrOperationConflict):
		fmt.Fprintf(stderr, "txharbor recovery-admin control: operation_conflict operation_id=%s\n", operation)
		return 1
	case errors.Is(err, controlstore.ErrIdentityMappingMissing):
		fmt.Fprintf(stderr, "txharbor recovery-admin control: refused participant-register: principal %s has no identity mapping (cannot prove a person); register the mapping first\n", principal)
		return 1
	case errors.Is(err, controlstore.ErrIdentityMappingRevoked):
		fmt.Fprintf(stderr, "txharbor recovery-admin control: refused participant-register: principal %s has a revoked identity mapping (cannot prove a person); re-activate the mapping first\n", principal)
		return 1
	case errors.Is(err, controlstore.ErrParticipantAlreadyRegistered):
		fmt.Fprintf(stderr, "txharbor recovery-admin control: refused participant-register: binding already exists for instance=%s principal=%s role=%s\n",
			strings.TrimSpace(*instanceID), principal, strings.TrimSpace(*role))
		return 1
	case err != nil:
		fmt.Fprintf(stderr, "txharbor recovery-admin control: participant-register refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout,
		"txharbor recovery-admin: control participant-register instance=%s principal=%s role=%s person_id=%s binding_source=%s recorded=%t operation_id=%s operator=%s principal_actor=%s\n",
		result.Binding.InstanceID, result.Binding.Principal, result.Binding.Role, result.Binding.PersonID,
		result.Binding.BindingSource, result.Recorded, operation, strings.TrimSpace(*operator), env.principal)
	return 0
}

// controlIdentityMapSet implements `control identity-map-set`: set/refresh one
// person<->principal mapping or revoke it (--revoke, keeping the row). A
// mapping change immediately invalidates the approvals that relied on it
// (F19), recorded as refusal-path audit rows.
func controlIdentityMapSet(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("recovery-admin control identity-map-set", flag.ContinueOnError)
	fs.SetOutput(stderr)
	principalRaw := fs.String("principal", "", "mapped principal <kind>:<id> (required)")
	personID := fs.String("person-id", "", "person identity to map (required unless --revoke)")
	source := fs.String("source", controlstore.MappingSourceDeployConfig, "mapping source: deploy_config|identity_source")
	revoke := fs.Bool("revoke", false, "revoke the mapping (row kept, active=false)")
	operator := fs.String("operator", "", "declared operator identity (required, audit annotation only)")
	reason := fs.String("reason", "", "operator reason (required, audit annotation only)")
	operationID := fs.String("operation-id", "", "idempotent operation identity (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	person := strings.TrimSpace(*personID)
	if fs.NArg() > 0 || strings.TrimSpace(*principalRaw) == "" || strings.TrimSpace(*operator) == "" ||
		strings.TrimSpace(*reason) == "" || strings.TrimSpace(*operationID) == "" ||
		(*revoke && person != "") || (!*revoke && person == "") {
		recoveryControlUsage(stderr)
		return 2
	}
	principal, err := controlstore.NormalizePrincipal(*principalRaw)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin control: --principal: %s\n", logx.Redact(err.Error()))
		return 2
	}
	operation, err := controlParseOperationID(*operationID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin control: %s\n", logx.Redact(err.Error()))
		return 2
	}
	if err := controlParseAuditAnnotations(*operator, *reason); err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin control: %s\n", logx.Redact(err.Error()))
		return 2
	}

	env, code := recoveryControlOpen(ctx, d)
	if env == nil {
		return code
	}
	defer env.pool.Close()

	var (
		change  controlstore.IdentityMappingChange
		outcome string
	)
	if *revoke {
		change, err = env.store.RevokeIdentityMapping(ctx, controlstore.RevokeIdentityMappingRequest{
			Principal:   principal,
			RecordedBy:  env.principal,
			Operator:    strings.TrimSpace(*operator),
			Reason:      strings.TrimSpace(*reason),
			OperationID: operation,
		})
		switch {
		case err == nil && change.Recorded:
			outcome = "recorded"
		case err == nil && change.AlreadyRevoked:
			outcome = "already_revoked"
		case err == nil:
			outcome = "revoked"
		}
	} else {
		change, err = env.store.SetIdentityMapping(ctx, controlstore.SetIdentityMappingRequest{
			Principal:   principal,
			PersonID:    person,
			Source:      strings.TrimSpace(*source),
			RecordedBy:  env.principal,
			Operator:    strings.TrimSpace(*operator),
			Reason:      strings.TrimSpace(*reason),
			OperationID: operation,
		})
		switch {
		case err == nil && change.Recorded:
			outcome = "recorded"
		case err == nil && change.Created:
			outcome = "created"
		case err == nil && change.Changed:
			outcome = "changed"
		case err == nil:
			outcome = "unchanged"
		}
	}
	switch {
	case errors.Is(err, controlstore.ErrOperationConflict):
		fmt.Fprintf(stderr, "txharbor recovery-admin control: operation_conflict operation_id=%s\n", operation)
		return 1
	case errors.Is(err, controlstore.ErrIdentityMappingMissing):
		fmt.Fprintf(stderr, "txharbor recovery-admin control: refused identity-map-set: principal %s has no identity mapping to revoke\n", principal)
		return 1
	case err != nil:
		fmt.Fprintf(stderr, "txharbor recovery-admin control: identity-map-set refused: %s\n", logx.Redact(err.Error()))
		return 1
	}

	fmt.Fprintf(stdout,
		"txharbor recovery-admin: control identity-map-set outcome=%s active=%t recorded=%t principal=%s person_id=%s source=%s recorded_by=%s invalidated_approvals=%d operation_id=%s operator=%s\n",
		outcome, change.Mapping.Active, change.Recorded, change.Mapping.Principal, change.Mapping.PersonID,
		change.Mapping.Source, change.Mapping.RecordedBy, len(change.InvalidatedApprovals), operation,
		strings.TrimSpace(*operator))
	for _, item := range change.InvalidatedApprovals {
		fmt.Fprintf(stdout,
			"  invalidated_approval approval_id=%s instance=%s capability=%s scope_hash=%s recorded_person_id=%s current_person_id=%s mapping_state=%s refusal_class=%s\n",
			item.ApprovalID, item.InstanceID, item.Capability, item.ScopeHash, item.RecordedPersonID,
			item.CurrentPersonID, item.MappingState, controlstore.RefusalApprovalIdentityUnverified)
	}
	return 0
}

// controlIdentityMapShow is the strictly read-only mapping/participant view
// (no audit writes; refusals are usage/config refusals on stderr).
func controlIdentityMapShow(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("recovery-admin control identity-map-show", flag.ContinueOnError)
	fs.SetOutput(stderr)
	principalFilter := fs.String("principal", "", "filter by principal <kind>:<id> (optional)")
	personFilter := fs.String("person-id", "", "filter by person_id (optional)")
	instanceFilter := fs.String("instance", "", "also list this instance's participant bindings (optional UUID)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		recoveryControlUsage(stderr)
		return 2
	}

	env, code := recoveryControlOpen(ctx, d)
	if env == nil {
		return code
	}
	defer env.pool.Close()

	mappings, err := env.store.IdentityMappings(ctx, *principalFilter, *personFilter)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor recovery-admin control: identity-map-show refused: %s\n", logx.Redact(err.Error()))
		return 1
	}
	for _, mapping := range mappings {
		fmt.Fprintf(stdout, "mapping principal=%s person_id=%s active=%t source=%s recorded_by=%s recorded_at=%s\n",
			mapping.Principal, mapping.PersonID, mapping.Active, mapping.Source, mapping.RecordedBy,
			mapping.RecordedAt.UTC().Format(time.RFC3339))
	}

	participants := 0
	if strings.TrimSpace(*instanceFilter) != "" {
		bindings, err := env.store.ParticipantBindings(ctx, *instanceFilter)
		if err != nil {
			fmt.Fprintf(stderr, "txharbor recovery-admin control: identity-map-show refused: %s\n", logx.Redact(err.Error()))
			return 1
		}
		for _, binding := range bindings {
			fmt.Fprintf(stdout, "participant instance=%s principal=%s person_id=%s role=%s binding_source=%s bound_at=%s\n",
				binding.InstanceID, binding.Principal, binding.PersonID, binding.Role, binding.BindingSource,
				binding.BoundAt.UTC().Format(time.RFC3339))
		}
		participants = len(bindings)
	}

	active := 0
	for _, mapping := range mappings {
		if mapping.Active {
			active++
		}
	}
	fmt.Fprintf(stdout,
		"txharbor recovery-admin: control identity-map-show mappings=%d active=%d participants=%d principal=%s control_target_fingerprint=%s\n",
		len(mappings), active, participants, env.principal, env.fingerprint)
	return 0
}

// controlParseOperationID validates the required operation_id carriage: a
// bounded, control-free identity used for persistent idempotency and audit. It
// is never an authorization input.
func controlParseOperationID(raw string) (string, error) {
	operation := strings.TrimSpace(raw)
	if operation == "" {
		return "", errors.New("--operation-id is required")
	}
	if len(operation) > 128 || strings.ContainsAny(operation, "\x00\n\r\t") {
		return "", errors.New("--operation-id must be 1..128 characters without control characters")
	}
	return operation, nil
}

// controlParseAuditAnnotations bounds the free-text audit annotations. They
// are recorded, never evaluated as authorization.
func controlParseAuditAnnotations(operator, reason string) error {
	if n := len(strings.TrimSpace(operator)); n == 0 || n > 128 {
		return errors.New("--operator must be 1..128 characters (audit annotation only)")
	}
	if n := len(strings.TrimSpace(reason)); n == 0 || n > 1024 {
		return errors.New("--reason must be 1..1024 characters (audit annotation only)")
	}
	return nil
}

// recoveryControlUsage prints the management subcommand surface and its trust
// boundary.
func recoveryControlUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: txharbor recovery-admin control participant-register|identity-map-set|identity-map-show [flags]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "  participant-register  bind a principal to an open instance under one role")
	fmt.Fprintln(w, "      --instance UUID --principal <kind>:<id> --role executor|verifier|approver")
	fmt.Fprintln(w, "      [--binding-source deploy_config|auth_principal] [--proof-ref TEXT]")
	fmt.Fprintln(w, "      --operator TEXT --reason TEXT --operation-id TEXT")
	fmt.Fprintln(w, "    person_id is resolved from the active identity mapping (never supplied).")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "  identity-map-set  set/refresh a person<->principal mapping (F19: invalidates affected approvals)")
	fmt.Fprintln(w, "      --principal <kind>:<id> --person-id TEXT [--source deploy_config|identity_source]")
	fmt.Fprintln(w, "      [--revoke] --operator TEXT --reason TEXT --operation-id TEXT")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "  identity-map-show  read-only mapping view [--principal P] [--person-id ID] [--instance UUID]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Trust boundary: the subject is bound from TXHARBOR_RECOVERY_PRINCIPAL (no principal is preset),")
	fmt.Fprintln(w, "the control store is reached only through TXHARBOR_RECOVERY_CONTROL_DSN and must differ from")
	fmt.Fprintln(w, "TXHARBOR_PG_DSN (identity data is never recoverable from the data DB); unknown/incompatible control")
	fmt.Fprintln(w, "stores are refused. --operator/--reason are audit annotations only and never authorize anything;")
	fmt.Fprintln(w, "operation_id is the idempotency key (same input replays with zero writes, different input conflicts).")
	fmt.Fprintln(w, "A mapping change immediately invalidates the approvals that relied on it (refusal_class="+
		controlstore.RefusalApprovalIdentityUnverified+"); they must be re-approved.")
}
