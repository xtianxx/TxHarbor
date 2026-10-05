//go:build integration

// control_integration_test.go runs the T010 management CLI against a real
// PostgreSQL 18.6 (testcontainers): the local operable identity path (mapping
// set/show, participant registration resolved through the mapping, no preset
// subject), missing/revoked mapping refusals with audit, operation_id
// idempotency and conflict zero-write, the F19 mapping-change invalidation
// recorded as approval_identity_unverified, and the T069 version guard on the
// management open path.
//
// Docker provider missing: the package fails under CI=true or
// TXHARBOR_REQUIRE_DOCKER=1 and reports NOT RUN locally (exit 0).
package recoveryadmin

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// controlIntegrationEnv migrates one fresh control store and returns the
// command environment (control DSN != data DSN), its pool and a bound store.
func controlIntegrationEnv(t *testing.T) (map[string]string, *pgxpool.Pool, *controlstore.Store) {
	t.Helper()
	baseDSN := startMigratePostgres(t)
	env := map[string]string{
		config.EnvRecoveryControlDSN: baseDSN,
		config.EnvPGDSN:              migrateWithDatabase(t, baseDSN, "txharbor_data"),
		config.EnvRecoveryPrincipal:  "deploy:manager",
	}
	if code, _, stderr := runRecoveryAdmin(t, []string{"migrate", "up"}, env); code != 0 {
		t.Fatalf("migrate up: exit=%d stderr=%q", code, stderr)
	}
	pool := migrateTestPool(t, baseDSN)
	store, err := controlstore.NewStore(context.Background(), pool)
	if err != nil {
		t.Fatalf("NewStore over the migrated control store: %v", err)
	}
	return env, pool, store
}

func TestControlIntegrationIdentityPath(t *testing.T) {
	ctx := context.Background()
	env, pool, store := controlIntegrationEnv(t)
	var outputs []string

	// A pristine control store presets no subject.
	code, out, errOut := runControl(t, []string{"identity-map-show"}, env)
	outputs = append(outputs, out, errOut)
	if code != 0 || !strings.Contains(out, "mappings=0") {
		t.Fatalf("pristine identity view: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}

	// 1) identity-map-set creates the mapping (deployment-controlled source).
	code, out, errOut = runControl(t, []string{"identity-map-set",
		"--principal", "deploy:alice", "--person-id", "person-a",
		"--operator", "ops", "--reason", "bootstrap", "--operation-id", "op-map-1"}, env)
	outputs = append(outputs, out, errOut)
	if code != 0 {
		t.Fatalf("identity-map-set: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	for _, want := range []string{"outcome=created", "active=true", "recorded=false",
		"person_id=person-a", "recorded_by=deploy:manager", "invalidated_approvals=0"} {
		if !strings.Contains(out, want) {
			t.Fatalf("identity-map-set output must contain %q: %q", want, out)
		}
	}
	if got := migrateCountRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_identity WHERE principal = 'deploy:alice' AND person_id = 'person-a' AND active"); got != 1 {
		t.Fatalf("mapping row must be active, found %d", got)
	}

	// 2) identity-map-show renders it.
	code, out, errOut = runControl(t, []string{"identity-map-show", "--principal", "deploy:alice"}, env)
	outputs = append(outputs, out, errOut)
	if code != 0 || !strings.Contains(out, "mapping principal=deploy:alice person_id=person-a active=true source=deploy_config") {
		t.Fatalf("identity-map-show: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}

	// 3) Same person, second account: the show view identifies the person.
	code, out, errOut = runControl(t, []string{"identity-map-set",
		"--principal", "sso:alice", "--person-id", "person-a", "--source", "identity_source",
		"--operator", "ops", "--reason", "second account", "--operation-id", "op-map-2"}, env)
	outputs = append(outputs, out, errOut)
	if code != 0 {
		t.Fatalf("second-account mapping: exit=%d stderr=%q", code, errOut)
	}
	code, out, errOut = runControl(t, []string{"identity-map-show", "--person-id", "person-a"}, env)
	outputs = append(outputs, out, errOut)
	if code != 0 || !strings.Contains(out, "mappings=2") {
		t.Fatalf("same-person accounts must both show: exit=%d stdout=%q", code, out)
	}
	if person, found, err := store.ActivePersonID(ctx, "sso:alice"); err != nil || !found || person != "person-a" {
		t.Fatalf("sso:alice must resolve to person-a: %q found=%t err=%v", person, found, err)
	}

	// 4) participant-register resolves person_id from the mapping.
	target := cliTargetBinding(t, env[config.EnvPGDSN])
	instance, err := store.OpenInstance(ctx, controlstore.OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:manager",
		TargetGuardKey: target.TargetGuardKey, TargetRoleFingerprint: target.TargetRoleFingerprint,
	})
	if err != nil {
		t.Fatalf("open instance: %v", err)
	}
	code, out, errOut = runControl(t, []string{"participant-register",
		"--instance", instance.InstanceID, "--principal", "deploy:alice", "--role", "approver",
		"--operator", "ops", "--reason", "onboard", "--operation-id", "op-reg-1"}, env)
	outputs = append(outputs, out, errOut)
	if code != 0 {
		t.Fatalf("participant-register: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	for _, want := range []string{"person_id=person-a", "role=approver", "binding_source=deploy_config", "recorded=false"} {
		if !strings.Contains(out, want) {
			t.Fatalf("participant-register output must contain %q: %q", want, out)
		}
	}
	if got := migrateCountRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_participant WHERE principal = 'deploy:alice' AND person_id = 'person-a'"); got != 1 {
		t.Fatalf("participant binding must resolve the mapping person, found %d", got)
	}

	// 5) operation_id replay: recorded outcome, zero writes.
	code, out, errOut = runControl(t, []string{"participant-register",
		"--instance", instance.InstanceID, "--principal", "deploy:alice", "--role", "approver",
		"--operator", "ops", "--reason", "onboard", "--operation-id", "op-reg-1"}, env)
	outputs = append(outputs, out, errOut)
	if code != 0 || !strings.Contains(out, "recorded=true") {
		t.Fatalf("registration replay: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if got := migrateCountRows(t, ctx, pool, "SELECT count(*) FROM recovery_participant"); got != 1 {
		t.Fatalf("replay must write no participant rows, got %d", got)
	}
	if got := migrateCountRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE action = 'participant_register' AND operation_id = 'op-reg-1'"); got != 1 {
		t.Fatalf("replay must not duplicate the audit row, got %d", got)
	}

	// 6) A principal without a mapping cannot be registered (zero rows,
	// audited refusal).
	code, out, errOut = runControl(t, []string{"participant-register",
		"--instance", instance.InstanceID, "--principal", "deploy:carol", "--role", "verifier",
		"--operator", "ops", "--reason", "onboard", "--operation-id", "op-reg-2"}, env)
	outputs = append(outputs, out, errOut)
	if code != 1 || !strings.Contains(errOut, "no identity mapping") {
		t.Fatalf("unmapped principal must refuse: exit=%d stderr=%q", code, errOut)
	}
	if got := migrateCountRows(t, ctx, pool, "SELECT count(*) FROM recovery_participant"); got != 1 {
		t.Fatalf("refused registration must write nothing, got %d bindings", got)
	}
	if got := migrateCountRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE action = 'participant_register' AND result = 'refused'"); got != 1 {
		t.Fatalf("refused registration must be audited, got %d", got)
	}

	// 7) A mapping change immediately invalidates existing approvals and
	// records the approval_identity_unverified refusal path (F19).
	approval, err := store.AppendApprovalDecision(ctx, controlstore.ApprovalDecisionRequest{
		InstanceID: instance.InstanceID, Capability: "query", ScopeHash: "scope-1",
		Decision: "approve", ApprovalClassSnapshot: "single_non_executor",
		Principal: "deploy:alice", PersonID: "person-a",
		EvidenceGeneration: 0, EvidenceHash: controlstore.EmptyEvidenceHash, OperationID: "op-appr-1",
	})
	if err != nil {
		t.Fatalf("append approval: %v", err)
	}
	code, out, errOut = runControl(t, []string{"identity-map-set",
		"--principal", "deploy:alice", "--person-id", "person-b",
		"--operator", "ops", "--reason", "identity corrected", "--operation-id", "op-map-3"}, env)
	outputs = append(outputs, out, errOut)
	if code != 0 {
		t.Fatalf("mapping change: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	for _, want := range []string{"outcome=changed", "invalidated_approvals=1",
		"recorded_person_id=person-a", "current_person_id=person-b", "mapping_state=changed",
		"refusal_class=approval_identity_unverified"} {
		if !strings.Contains(out, want) {
			t.Fatalf("mapping change output must contain %q: %q", want, out)
		}
	}
	if got := migrateCountRows(t, ctx, pool, `
SELECT count(*) FROM recovery_audit
WHERE action = 'approval_identity_invalidated' AND result = 'refused'
  AND refusal_class = 'approval_identity_unverified' AND target->>'approval_id' = $1`, approval.DecisionID); got != 1 {
		t.Fatalf("exactly one recorded invalidation refusal expected, got %d", got)
	}
	invalidated, err := store.InvalidatedApprovals(ctx, "deploy:alice")
	if err != nil || len(invalidated) != 1 || invalidated[0].ApprovalID != approval.DecisionID {
		t.Fatalf("invalidated approvals read helper: %+v err=%v", invalidated, err)
	}

	// 8) A conflicting reuse of op-map-3 writes nothing.
	code, out, errOut = runControl(t, []string{"identity-map-set",
		"--principal", "deploy:alice", "--person-id", "person-c",
		"--operator", "ops", "--reason", "conflict", "--operation-id", "op-map-3"}, env)
	outputs = append(outputs, out, errOut)
	if code != 1 || !strings.Contains(errOut, "operation_conflict") {
		t.Fatalf("operation_id reuse with different input must conflict: exit=%d stderr=%q", code, errOut)
	}
	if got := migrateCountRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_identity WHERE principal = 'deploy:alice' AND person_id = 'person-b'"); got != 1 {
		t.Fatalf("conflict must not rewrite the mapping, found %d", got)
	}

	// 9) Revocation keeps the row and refuses later registration.
	code, out, errOut = runControl(t, []string{"identity-map-set",
		"--principal", "deploy:alice", "--revoke",
		"--operator", "ops", "--reason", "offboard", "--operation-id", "op-map-4"}, env)
	outputs = append(outputs, out, errOut)
	if code != 0 || !strings.Contains(out, "outcome=revoked") || !strings.Contains(out, "active=false") {
		t.Fatalf("revoke: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if got := migrateCountRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_identity WHERE principal = 'deploy:alice'"); got != 1 {
		t.Fatalf("revoked mapping row must be kept, got %d", got)
	}
	if got := migrateCountRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_identity WHERE principal = 'deploy:alice' AND active"); got != 0 {
		t.Fatalf("revoked mapping must be inactive, got %d active rows", got)
	}
	code, out, errOut = runControl(t, []string{"participant-register",
		"--instance", instance.InstanceID, "--principal", "deploy:alice", "--role", "verifier",
		"--operator", "ops", "--reason", "retry", "--operation-id", "op-reg-3"}, env)
	outputs = append(outputs, out, errOut)
	if code != 1 || !strings.Contains(errOut, "revoked identity mapping") {
		t.Fatalf("registration over a revoked mapping must refuse: exit=%d stderr=%q", code, errOut)
	}

	// 10) identity-map-show --instance lists the participant bindings.
	code, out, errOut = runControl(t, []string{"identity-map-show", "--instance", instance.InstanceID}, env)
	outputs = append(outputs, out, errOut)
	if code != 0 || !strings.Contains(out, "participant instance=") || !strings.Contains(out, "participants=1") {
		t.Fatalf("instance participant view: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}

	// No command output may carry a plaintext DSN or its credentials.
	parsed, err := url.Parse(env[config.EnvRecoveryControlDSN])
	if err != nil {
		t.Fatalf("parse control DSN: %v", err)
	}
	password, _ := parsed.User.Password()
	for _, text := range outputs {
		if strings.Contains(text, "postgres://") || (password != "" && strings.Contains(text, ":"+password+"@")) {
			t.Fatalf("command output must never carry a plaintext DSN: %q", text)
		}
	}
}

func TestControlIntegrationRefusesUnknownControlVersion(t *testing.T) {
	ctx := context.Background()
	env, pool, _ := controlIntegrationEnv(t)
	if _, err := pool.Exec(ctx, "UPDATE goose_db_version SET version_id = 999 WHERE is_applied"); err != nil {
		t.Fatalf("forge unknown version: %v", err)
	}

	code, stdout, stderr := runControl(t, []string{"identity-map-set",
		"--principal", "deploy:alice", "--person-id", "person-a",
		"--operator", "ops", "--reason", "bootstrap", "--operation-id", "op-map-1"}, env)
	if code != 1 || !strings.Contains(stderr, controlstore.RefusalControlStoreUnavailable) {
		t.Fatalf("an unknown control-store version must refuse: exit=%d stderr=%q", code, stderr)
	}
	if strings.Contains(stdout, "outcome=") {
		t.Fatalf("no mapping write may be reported over an unknown version: %q", stdout)
	}
	if got := migrateCountRows(t, ctx, pool, "SELECT count(*) FROM recovery_identity"); got != 0 {
		t.Fatalf("an unknown control store must accept zero identity writes, got %d rows", got)
	}
	if got := migrateCountRows(t, ctx, pool,
		"SELECT count(*) FROM recovery_audit WHERE action = 'store_connect' AND result = 'refused' AND refusal_class = 'control_store_unavailable'"); got != 1 {
		t.Fatalf("the version refusal must be annotated once, got %d", got)
	}
}
