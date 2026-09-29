//go:build integration

// verify_approve_status_integration_test.go is the T042/T051 real-PG entry
// evidence for quickstart S6/S7/S10/S11 (tags: integration; FR-014/018/019/020,
// FR-021/022/023/024/026; T042, T051).
//
// The file drives the actual `txharbor recovery-admin` entry functions of this
// package (Run -> recoveryAdminVerify / recoveryAdminApprove /
// recoveryAdminRelease / recoveryAdminStatus / recoveryAdminInstanceClose /
// recoveryAdminInstanceOpen) against a real PostgreSQL 18.6 fixture, a real
// Anvil chain and the real T039/T040 read-only source adapters. No release row
// is ever written directly: every approval/release arrives through the T048/
// T049 decision writers behind the CLI, and the derived states are read back
// through the T051 status review (the gate's own evaluation).
//
// Scenario walk (one fixture, sequential phases):
//
//  1. open recovery instance (fixture write path) with explicit identity
//     mappings for executor / verifier / two approvers, explicit participant
//     bindings, and the isolation checklist double-sign through the real
//     checklist-set (executor) + checklist-verify (non-executor verifier)
//     entries; V1 chain facts are seeded so the restored frontier is provable
//     and the chain does not lead it;
//  2. real backup -> verify-backup -> restore, so `restored` evidence exists
//     through a real entry (pg_dump/pg_restore inside the pinned container via
//     the PATH shim of the existing fixture);
//  3. S6: `verify --scope all` twice (bounded stepping) plus one
//     operation-id replay; conclusions and gap list are asserted separately;
//  4. S7/S10: `approve`/`release` through the real entries (single and dual
//     two-person bases), a no-approval release refusal, then `status` showing
//     restored/verified/released per capability with refusal_class (gap_open
//     for the gapped capability, isolation_unproven for the funds capability
//     whose approval basis is valid but the hard gate still refuses),
//     `revoke` of one release stream (release_revoked), the bounded review
//     budget exhaustion (refused + audited, zero state change);
//  5. S11 partial: `instance-close` refuses over the open gap (gap_open
//     audit, instance stays open) and `instance-open --supersede` refuses a
//     two-reference same-person basis (approval_missing audit).
//
// Docker provider missing: the package TestMain (migrate_integration_test.go)
// fails the package under CI=true / TXHARBOR_REQUIRE_DOCKER=1 and reports
// NOT RUN locally (exit 0).
package recoveryadmin

import (
	"context"
	"fmt"
	"math/big"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/recovery"
)

const (
	// raChainIDText is the Anvil chain id (the compose default), mirrored in
	// every TXHARBOR_CHAIN_ID below.
	raChainIDText = "31337"
	// raChainID is the numeric form used by the V1 chain seeding.
	raChainID = uint64(31337)
	// raAnvilImage is the pinned Anvil of the repo's other integration suites.
	raAnvilImage = "ghcr.io/foundry-rs/foundry:v1.8.1"
)

// raStartAnvil starts the pinned Anvil and returns its HTTP endpoint.
func raStartAnvil(t *testing.T) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        raAnvilImage,
			ExposedPorts: []string{"8545/tcp"},
			// The image entrypoint is /bin/sh -c; without the override anvil
			// never receives --host and binds 127.0.0.1 inside the container.
			Entrypoint: []string{"anvil"},
			Cmd:        []string{"--host", "0.0.0.0", "--port", "8545", "--chain-id", raChainIDText},
			WaitingFor: wait.ForLog("Listening on"),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start anvil: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("anvil host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "8545/tcp")
	if err != nil {
		t.Fatalf("anvil port: %v", err)
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

// raSeedV1ChainFacts writes the restored indexer frontier and the log
// checkpoint of the real chain at its current tip: the checkpointed block hash
// is the canonical block hash the chain actually serves, so V1 can prove the
// frontier (a fabricated or stale hash would be divergent/unknown by design).
// The deposit checkpoint is deliberately left absent: V1 then establishes the
// deposit_confirmation gap used by S7.
func raSeedV1ChainFacts(t *testing.T, f *cliFixture, rpcURL string) {
	t.Helper()
	client, err := eth.Dial(context.Background(), rpcURL, 10*time.Second)
	if err != nil {
		t.Fatalf("dial anvil: %v", err)
	}
	defer client.Close()
	tip, err := client.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf("chain tip: %v", err)
	}
	header, err := client.HeaderByNumber(context.Background(), new(big.Int).SetUint64(tip))
	if err != nil {
		t.Fatalf("header %d: %v", tip, err)
	}
	hash := strings.ToLower(header.Hash().Hex())
	parent := strings.ToLower(header.ParentHash.Hex())
	if _, err := f.data.Exec(f.ctx,
		`INSERT INTO chain_blocks (chain_id, number, hash, parent_hash) VALUES ($1, $2, $3, $4)`,
		raChainID, tip, hash, parent); err != nil {
		t.Fatalf("seed chain_blocks: %v", err)
	}
	if _, err := f.data.Exec(f.ctx,
		`INSERT INTO indexer_checkpoint (chain_id, height, block_hash, start_height) VALUES ($1, $2, $3, 0)`,
		raChainID, tip, hash); err != nil {
		t.Fatalf("seed indexer_checkpoint: %v", err)
	}
	// next_block = tip+1 is the closed log coverage of the restored frontier.
	if _, err := f.data.Exec(f.ctx,
		`INSERT INTO log_checkpoint (chain_id, start_block, config_hash, next_block) VALUES ($1, 0, $2, $3)`,
		raChainID, strings.Repeat("a", 64), tip+1); err != nil {
		t.Fatalf("seed log_checkpoint: %v", err)
	}
}

// raEnv builds the full deployment environment of one command invocation: the
// fixture environment (control/data DSN, principal, artifact dir) plus the
// T042/T051 deployment configuration. Every value is a local test input; the
// DSN is the restored data DB for the post-restore phases.
func (f *cliFixture) raEnv(principal, dataDSN, rpcURL string) map[string]string {
	env := f.env(principal)
	env[config.EnvPGDSN] = dataDSN
	env[config.EnvRPCURL] = rpcURL
	env[config.EnvChainID] = raChainIDText
	env[config.EnvRecoveryGateTTL] = "5m"
	env[config.EnvRecoveryStatusTimeout] = "30s"
	env[config.EnvRecoveryStatusMaxReads] = "200"
	env[config.EnvRecoveryStatusMaxRows] = "200"
	env[recovery.VerificationBatchLimitConfigKey] = "500"
	for _, category := range recovery.KnownVerificationCategories() {
		env[recovery.VerificationFreshnessConfigPrefix+string(category)] = "24h"
	}
	// A reachable readiness boundary (the V9 probe only checks TCP
	// reachability); without one the V9 gap would conservatively close all
	// seven capabilities.
	env[config.EnvKafkaBrokers] = strings.TrimPrefix(rpcURL, "http://")
	return env
}

// raRunChecklist double-signs the four isolation items the query/chain_scan
// dependency sets require: checklist-set by the executor, checklist-verify by
// the non-executor verifier (authorization_recheck stays deliberately
// unverified so the V8 funds/delivery blocker exists for S7/S10).
func raRunChecklist(t *testing.T, f *cliFixture, env func(string) map[string]string) {
	t.Helper()
	items := []recovery.IsolationItemKey{
		recovery.IsolationItemOldWritersStopped,
		recovery.IsolationItemWriterFencingObserved,
		recovery.IsolationItemNetworkIsolation,
		recovery.IsolationItemVersionCompatible,
	}
	for i, item := range items {
		code, out, errOut := runRecoveryAdmin(t, []string{
			"checklist-set", "--instance", f.instanceID, "--item", string(item),
			"--evidence-ref", "evidence:" + string(item) + ":ra",
			"--operation-id", fmt.Sprintf("ra-chk-set-%d", i+1),
		}, env("deploy:executor"))
		if code != 0 || !strings.Contains(out, "state=evidenced") {
			t.Fatalf("checklist-set %s: exit=%d stdout=%q stderr=%q", item, code, out, errOut)
		}
		code, out, errOut = runRecoveryAdmin(t, []string{
			"checklist-verify", "--instance", f.instanceID, "--item", string(item),
			"--operation-id", fmt.Sprintf("ra-chk-verify-%d", i+1),
		}, env("auth:verifier"))
		if code != 0 || !strings.Contains(out, "state=verified") {
			t.Fatalf("checklist-verify %s: exit=%d stdout=%q stderr=%q", item, code, out, errOut)
		}
	}
}

// raScope is the canonical T050 scope of one capability stream.
func raScope(capability recovery.Capability) string {
	return fmt.Sprintf("chain=%s;capability=%s", raChainIDText, capability)
}

// raMustContain asserts every needle is present in the collected output.
func raMustContain(t *testing.T, label, output string, needles ...string) {
	t.Helper()
	for _, needle := range needles {
		if !strings.Contains(output, needle) {
			t.Fatalf("%s: output must contain %q:\n%s", label, needle, output)
		}
	}
}

// raAuditRows counts the audit rows of one action bound to the fixture
// instance.
func raAuditRows(t *testing.T, f *cliFixture, action string) int {
	t.Helper()
	return migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = $2`, f.instanceID, action)
}

// raRefusalRows counts the refused audit rows of one action with one closed
// refusal class.
func raRefusalRows(t *testing.T, f *cliFixture, action, class string) int {
	t.Helper()
	return migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_audit
		  WHERE instance_id = $1 AND action = $2 AND result = 'refused' AND refusal_class = $3`,
		f.instanceID, action, class)
}

// raInstanceState reads the reviewed instance row.
func raInstanceState(t *testing.T, f *cliFixture) (kind, state string) {
	t.Helper()
	if err := f.control.QueryRow(f.ctx,
		`SELECT kind, state FROM recovery_instance WHERE instance_id = $1`, f.instanceID).
		Scan(&kind, &state); err != nil {
		t.Fatalf("read instance state: %v", err)
	}
	return kind, state
}

func TestRecoveryAdminVerifyApproveReleaseStatusRealEntry(t *testing.T) {
	requireNativePGRestore(t)
	f := newCLIFixture(t)
	rpcURL := raStartAnvil(t)
	raSeedV1ChainFacts(t, f, rpcURL)

	// Explicit identity mappings and participant bindings: executor, verifier
	// and two approvers with two distinct persons (dual bases must be provable
	// by two people, never by two accounts of one person).
	f.mapIdentity(t, "auth:approver-1", "person-approver-1")
	f.mapIdentity(t, "auth:approver-2", "person-approver-2")
	f.register(t, "auth:approver-1", "approver")
	f.register(t, "auth:approver-2", "approver")

	checklistEnv := func(principal string) map[string]string {
		return f.raEnv(principal, f.dataDSN, rpcURL)
	}
	raRunChecklist(t, f, checklistEnv)

	// Real backup -> verify-backup -> real restore: `restored` exists because
	// the real entry wrote restore_probe evidence, not because a row was
	// inserted.
	manifestPath, backupID := f.verifiedBackup(t)
	restoredDSN := f.createDB(t, "restored")
	code, restoredOut, restoredErr := f.cliRestore(t, manifestPath, restoredDSN, "ra-restore-1")
	if code != 0 || !strings.Contains(restoredOut, "restored=true") {
		t.Fatalf("CLI restore: exit=%d stdout=%q stderr=%q", code, restoredOut, restoredErr)
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 1 {
		t.Fatalf("restore_probe evidence rows after the real restore = %d, want 1", got)
	}
	t.Logf("S4 restore (real entry): %s", strings.TrimSpace(restoredOut))

	env := func(principal string) map[string]string {
		return f.raEnv(principal, restoredDSN, rpcURL)
	}

	// ------------------------------------------------------------------
	// S6: bounded read-only V1-V9 stepping through the real `verify` entry.
	// ------------------------------------------------------------------
	code, step1, step1Err := runRecoveryAdmin(t, []string{
		"verify", "--instance", f.instanceID, "--scope", "all", "--operation-id", "ra-verify-1",
	}, env("deploy:executor"))
	if code != 0 {
		t.Fatalf("verify step 1: exit=%d stdout=%q stderr=%q", code, step1, step1Err)
	}
	t.Logf("S6 verify step 1:\n%s", step1)

	// Every V1-V9 category reported (conclusions rendered separately from the
	// gap list), the provable V1 frontier items are consistent, and the
	// non-consistent items are not displayed as a pass.
	for _, category := range recovery.KnownVerificationCategories() {
		raMustContain(t, "verify step 1 categories", step1, "category="+string(category))
	}
	raMustContain(t, "verify step 1 conclusions", step1,
		"object_key=v1/indexer_checkpoint?chain_id=31337 conclusion=consistent",
		"object_key=v1/log_checkpoint?chain_id=31337 conclusion=consistent",
		"category=V9 object_key=v9/dependency?name=data-db conclusion=consistent",
		"category=V9 object_key=v9/dependency?name=control-store conclusion=consistent",
		"category=V9 object_key=v9/dependency?name=chain-rpc conclusion=consistent",
		"open_gaps=",
		"  gap gap_id=",
		"affected_capabilities=",
		"divergent/unknown/stale are not a pass",
	)
	if openGaps := cliField(step1, "open_gaps"); openGaps == "" || openGaps == "0" {
		t.Fatalf("verify step 1 must establish/confirm at least one open gap, got open_gaps=%q:\n%s", openGaps, step1)
	}
	step1Items := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_verification_item WHERE instance_id = $1`, f.instanceID)
	generation1 := migrateCountRows(t, f.ctx, f.control,
		`SELECT evidence_generation FROM recovery_instance WHERE instance_id = $1`, f.instanceID)

	// operation_id replay: recorded outcome, zero writes, no second batch.
	code, replayOut, replayErr := runRecoveryAdmin(t, []string{
		"verify", "--instance", f.instanceID, "--scope", "all", "--operation-id", "ra-verify-1",
	}, env("deploy:executor"))
	if code != 0 || !strings.Contains(replayOut, "replayed=true") {
		t.Fatalf("verify replay: exit=%d stdout=%q stderr=%q", code, replayOut, replayErr)
	}
	if got := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_verification_item WHERE instance_id = $1`, f.instanceID); got != step1Items {
		t.Fatalf("verify replay appended items: %d -> %d (want zero writes)", step1Items, got)
	}

	// A real rerun (new operation id) appends a new bounded step instead of
	// flipping history: the item rows grow and the generation advances by one.
	code, step2, step2Err := runRecoveryAdmin(t, []string{
		"verify", "--instance", f.instanceID, "--scope", "all", "--operation-id", "ra-verify-2",
	}, env("deploy:executor"))
	if code != 0 {
		t.Fatalf("verify step 2: exit=%d stdout=%q stderr=%q", code, step2, step2Err)
	}
	t.Logf("S6 verify step 2 (bounded stepping):\n%s", step2)
	step2Items := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_verification_item WHERE instance_id = $1`, f.instanceID)
	generation2 := migrateCountRows(t, f.ctx, f.control,
		`SELECT evidence_generation FROM recovery_instance WHERE instance_id = $1`, f.instanceID)
	if step2Items <= step1Items {
		t.Fatalf("bounded stepping did not append verdict rows: %d -> %d", step1Items, step2Items)
	}
	if generation2 != generation1+1 {
		t.Fatalf("one accepted step must advance the generation by exactly one: %d -> %d", generation1, generation2)
	}
	// The current generation carries all nine categories as accepted rows.
	if got := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(DISTINCT category) FROM recovery_verification_item WHERE instance_id = $1 AND generation = $2`,
		f.instanceID, generation2); got != len(recovery.KnownVerificationCategories()) {
		t.Fatalf("current-generation categories = %d, want %d", got, len(recovery.KnownVerificationCategories()))
	}

	// ------------------------------------------------------------------
	// S10: approve / release through the real entries.
	// ------------------------------------------------------------------
	approve := func(principal string, capability recovery.Capability, operationID string) (int, string, string) {
		return runRecoveryAdmin(t, []string{
			"approve", "--instance", f.instanceID, "--capability", string(capability),
			"--scope", raScope(capability), "--operation-id", operationID,
		}, env(principal))
	}
	release := func(principal string, capability recovery.Capability, revoke bool, operationID string) (int, string, string) {
		args := []string{
			"release", "--instance", f.instanceID, "--capability", string(capability),
			"--scope", raScope(capability), "--operation-id", operationID,
		}
		if revoke {
			args = append(args, "--revoke")
		}
		return runRecoveryAdmin(t, args, env(principal))
	}

	// chain_scan first (query/chain_scan are the dependency roots), single
	// non-executor basis.
	code, out, errOut := approve("auth:approver-1", recovery.CapabilityChainScan, "ra-approve-chain-scan-1")
	if code != 0 || !strings.Contains(out, "decision=approve") || !strings.Contains(out, "approval_class=single_non_executor") {
		t.Fatalf("approve chain_scan: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	chainScanApproval := cliField(out, "approval_id")
	if chainScanApproval == "" {
		t.Fatalf("approve chain_scan printed no approval_id: %q", out)
	}
	code, out, errOut = release("deploy:executor", recovery.CapabilityChainScan, false, "ra-release-chain-scan-1")
	if code != 0 || !strings.Contains(out, "decision=release") {
		t.Fatalf("release chain_scan: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	t.Logf("S10 release chain_scan: %s", strings.TrimSpace(out))

	code, out, errOut = approve("auth:approver-1", recovery.CapabilityQuery, "ra-approve-query-1")
	if code != 0 {
		t.Fatalf("approve query: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	queryApproval := cliField(out, "approval_id")
	code, out, errOut = release("deploy:executor", recovery.CapabilityQuery, false, "ra-release-query-1")
	if code != 0 || !strings.Contains(out, "decision=release") {
		t.Fatalf("release query: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	t.Logf("S10 release query: %s", strings.TrimSpace(out))

	// No valid approval basis means no release: event_consuming has no
	// approval at all and must refuse with zero release rows.
	beforeEventConsuming := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_release WHERE instance_id = $1 AND capability = 'event_consuming'`, f.instanceID)
	code, out, errOut = release("deploy:executor", recovery.CapabilityEventConsuming, false, "ra-release-event-consuming-1")
	if code != 1 || !strings.Contains(errOut, "refusal_class=approval_missing") {
		t.Fatalf("release without approvals must refuse approval_missing: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if got := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_release WHERE instance_id = $1 AND capability = 'event_consuming'`, f.instanceID); got != beforeEventConsuming {
		t.Fatalf("refused release wrote %d row(s)", got-beforeEventConsuming)
	}
	t.Logf("S10 release without approval (refused): %s", strings.TrimSpace(errOut))

	// Dual capability: one approval is not enough; two different people are.
	code, out, errOut = approve("auth:approver-1", recovery.CapabilityExistingWithdrawalRecovery, "ra-approve-evr-1")
	if code != 0 {
		t.Fatalf("approve EVR by approver-1: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, out, errOut = release("deploy:executor", recovery.CapabilityExistingWithdrawalRecovery, false, "ra-release-evr-1")
	if code != 1 || !strings.Contains(errOut, "refusal_class=approval_missing") {
		t.Fatalf("dual release with one approval must refuse approval_missing: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, out, errOut = approve("auth:approver-2", recovery.CapabilityExistingWithdrawalRecovery, "ra-approve-evr-2")
	if code != 0 || !strings.Contains(out, "person_id=person-approver-2") {
		t.Fatalf("approve EVR by approver-2: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, out, errOut = release("deploy:executor", recovery.CapabilityExistingWithdrawalRecovery, false, "ra-release-evr-2")
	if code != 0 || !strings.Contains(out, "decision=release") {
		t.Fatalf("dual release EVR: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	t.Logf("S10 dual release existing_withdrawal_recovery: %s", strings.TrimSpace(out))

	// S7 attempt: the deposit_confirmation capability carries the V1 deposit
	// gap. The release decision may be recorded (a release row is a decision
	// record; hard gates stay evaluation-time, data-model §3), but the derived
	// admission must refuse gap_open (asserted through `status` and
	// `instance-close` below).
	code, out, errOut = approve("auth:approver-1", recovery.CapabilityDepositConfirmation, "ra-approve-deposit-1")
	if code != 0 {
		t.Fatalf("approve deposit_confirmation: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, out, errOut = release("deploy:executor", recovery.CapabilityDepositConfirmation, false, "ra-release-deposit-1")
	if code != 0 {
		t.Fatalf("release deposit_confirmation (decision record): exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	t.Logf("S7 release attempt over the gap (recorded decision): %s", strings.TrimSpace(out))

	// ------------------------------------------------------------------
	// S10: status shows the derived per-capability tri-state + refusal class.
	// ------------------------------------------------------------------
	runStatus := func(principal string, extra map[string]string) (int, string, string) {
		env := f.raEnv(principal, restoredDSN, rpcURL)
		for key, value := range extra {
			env[key] = value
		}
		return runRecoveryAdmin(t, []string{"status", "--instance", f.instanceID}, env)
	}
	code, status1, status1Err := runStatus("deploy:executor", nil)
	if code != 0 {
		t.Fatalf("status: exit=%d stdout=%q stderr=%q", code, status1, status1Err)
	}
	t.Logf("S10 status (derived tri-state):\n%s", status1)

	raMustContain(t, "status tri-state", status1,
		"restored=true restore_probe_count=1",
		"capability=query restored=true verified=true released=true",
		"capability=chain_scan restored=true verified=true released=true",
		"capability=deposit_confirmation",
		"refusal_class=gap_open",
		"phase_two_evaluated=false",
		"`approved` is not a state",
	)
	if got := strings.Count(status1, "\n  capability="); got != len(recovery.KnownCapabilities()) {
		t.Fatalf("status must render all %d capabilities, got %d:\n%s", len(recovery.KnownCapabilities()), got, status1)
	}
	// The funds capability whose release basis is valid (two approvals) still
	// refuses the hard isolation gate: an approval never covers it. The
	// applicable V2/V3/V4/V8 categories carry current-generation unknown
	// conclusions (shown verbatim, never hidden) and the derived class is
	// exactly isolation_unproven.
	raMustContain(t, "status hard gate", status1,
		"capability=existing_withdrawal_recovery restored=true verified=true released=false refusal_class=isolation_unproven",
		"verification=V2:unknown=1,V3:unknown=1,V4:unknown=1,V7:consistent=3,V8:unknown=1,V9:consistent=4")
	// The gapped capability is not displayed as normal: exactly the derived
	// gap_open class, never released.
	depositLine := ""
	consistencyLine := ""
	for _, line := range strings.Split(status1, "\n") {
		if strings.HasPrefix(line, "  capability=deposit_confirmation") {
			depositLine = line
		}
		if strings.HasPrefix(line, "  capability=chain_scan") {
			consistencyLine = line
		}
	}
	if !strings.Contains(depositLine, "released=false") || !strings.Contains(depositLine, "refusal_class=gap_open") {
		t.Fatalf("deposit_confirmation must show released=false refusal_class=gap_open: %q", depositLine)
	}
	if !strings.Contains(consistencyLine, "released=true") {
		t.Fatalf("chain_scan must show released=true after a valid release: %q", consistencyLine)
	}

	// The status review never writes state: it appends the gate's own
	// audited status_review evaluations (action=gate_admission,
	// target.request_action=status_review).
	statusAudits := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_audit
		  WHERE instance_id = $1 AND action = 'gate_admission' AND target->>'request_action' = 'status_review'`,
		f.instanceID)
	if statusAudits == 0 {
		t.Fatalf("the status review recorded no audited status_review gate evaluation")
	}

	// ------------------------------------------------------------------
	// Revocation: an explicit release revoke refuses the next evaluation.
	// ------------------------------------------------------------------
	code, out, errOut = release("deploy:executor", recovery.CapabilityQuery, true, "ra-revoke-query-release-1")
	if code != 0 || !strings.Contains(out, "decision=revoke") {
		t.Fatalf("revoke release query: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, status2, status2Err := runStatus("deploy:executor", nil)
	if code != 0 {
		t.Fatalf("status after revoke: exit=%d stdout=%q stderr=%q", code, status2, status2Err)
	}
	// The explicit revoke refuses the next evaluation of the very stream it
	// covered (the other streams never had a release and stay no_release).
	raMustContain(t, "status after revoke", status2,
		"release_scope=capability=query;chain=31337 released=false refusal_class=release_revoked")
	queryLine := ""
	for _, line := range strings.Split(status2, "\n") {
		if strings.HasPrefix(line, "  capability=query") {
			queryLine = line
		}
	}
	if !strings.Contains(queryLine, "released=false") {
		t.Fatalf("query after the explicit revoke must show released=false: %q", queryLine)
	}
	t.Logf("S10 status after revoke: %s", strings.TrimSpace(queryLine))

	// ------------------------------------------------------------------
	// F13: the bounded read-only review refuses on budget exhaustion, is
	// audited, and changes no state.
	// ------------------------------------------------------------------
	before := map[string]int{
		"open_gaps": migrateCountRows(t, f.ctx, f.control,
			`SELECT count(*) FROM recovery_gap WHERE instance_id = $1 AND state IN ('open','escalated')`, f.instanceID),
		"approvals": migrateCountRows(t, f.ctx, f.control,
			`SELECT count(*) FROM recovery_approval WHERE instance_id = $1`, f.instanceID),
		"releases": migrateCountRows(t, f.ctx, f.control,
			`SELECT count(*) FROM recovery_release WHERE instance_id = $1`, f.instanceID),
		"items": migrateCountRows(t, f.ctx, f.control,
			`SELECT count(*) FROM recovery_verification_item WHERE instance_id = $1`, f.instanceID),
	}
	_, beforeState := raInstanceState(t, f)
	code, out, errOut = runStatus("deploy:executor", map[string]string{
		config.EnvRecoveryStatusMaxReads: "2",
	})
	if code != 1 || !strings.Contains(errOut, "budget is exhausted") || !strings.Contains(errOut, "no gap/instance/approval/release state changed") {
		t.Fatalf("budget-exhausted status must refuse with the audited bound: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	t.Logf("F13 status budget exhaustion (refused): %s", strings.TrimSpace(errOut))
	if got := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_audit
		  WHERE instance_id = $1 AND action = 'status_review' AND result = 'refused'
		    AND detail->>'reason' LIKE '%budget%'`, f.instanceID); got != 1 {
		t.Fatalf("budget-exhausted review must append exactly one refused audit row, got %d", got)
	}
	after := map[string]int{
		"open_gaps": migrateCountRows(t, f.ctx, f.control,
			`SELECT count(*) FROM recovery_gap WHERE instance_id = $1 AND state IN ('open','escalated')`, f.instanceID),
		"approvals": migrateCountRows(t, f.ctx, f.control,
			`SELECT count(*) FROM recovery_approval WHERE instance_id = $1`, f.instanceID),
		"releases": migrateCountRows(t, f.ctx, f.control,
			`SELECT count(*) FROM recovery_release WHERE instance_id = $1`, f.instanceID),
		"items": migrateCountRows(t, f.ctx, f.control,
			`SELECT count(*) FROM recovery_verification_item WHERE instance_id = $1`, f.instanceID),
	}
	_, afterState := raInstanceState(t, f)
	for key, want := range before {
		if after[key] != want {
			t.Fatalf("budget exhaustion changed %s: %d -> %d", key, want, after[key])
		}
	}
	if afterState != beforeState {
		t.Fatalf("budget exhaustion changed the instance state: %s -> %s", beforeState, afterState)
	}

	// ------------------------------------------------------------------
	// S11 (partial): close refuses over the open gap; supersede refuses a
	// two-reference same-person basis.
	// ------------------------------------------------------------------
	closeEnv := env("deploy:executor")
	// Close the instance against the authoritative data target bound at open;
	// the restored database is a separate artifact target, not a rebinding.
	closeEnv[config.EnvPGDSN] = f.dataDSN
	code, out, errOut = runRecoveryAdmin(t, []string{
		"instance-close", "--instance", f.instanceID, "--operation-id", "ra-close-1",
	}, closeEnv)
	if code != 1 || !strings.Contains(errOut, "an open evidence gap blocks at least one capability") ||
		!strings.Contains(errOut, "escalation_required=true") {
		t.Fatalf("instance-close over an open gap: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	t.Logf("S11 instance-close refused over the gap: %s", strings.TrimSpace(errOut))
	if got := raRefusalRows(t, f, "instance_close", string(recovery.RefusalGapOpen)); got < 1 {
		t.Fatalf("instance-close refusal must be audited with refusal_class=gap_open, got %d", got)
	}
	if _, state := raInstanceState(t, f); state != "open" {
		t.Fatalf("refused close must keep the instance open, got %q", state)
	}

	supersedeEnv := env("deploy:executor")
	supersedeEnv[config.EnvPGDSN] = f.dataDSN
	code, out, errOut = runRecoveryAdmin(t, []string{
		"instance-open", "--kind", "recovery", "--supersede", f.instanceID,
		"--approval-refs", chainScanApproval + "," + queryApproval,
		"--operation-id", "ra-supersede-1",
	}, supersedeEnv)
	if code != 1 || !strings.Contains(errOut, "supersede refused") {
		t.Fatalf("supersede with two refs of one person: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if got := raRefusalRows(t, f, "instance_supersede", string(recovery.RefusalApprovalMissing)); got < 1 {
		t.Fatalf("supersede refusal must be audited with refusal_class=approval_missing, got %d", got)
	}
	if _, state := raInstanceState(t, f); state != "open" {
		t.Fatalf("refused supersede must not close the instance, got %q", state)
	}

	// ------------------------------------------------------------------
	// Evidence census: every positive record came through a real entry.
	// ------------------------------------------------------------------
	approvals := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_approval WHERE instance_id = $1`, f.instanceID)
	releases := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_release WHERE instance_id = $1`, f.instanceID)
	gaps := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_gap WHERE instance_id = $1 AND state = 'open'`, f.instanceID)
	if approvals != 5 { // chain_scan, query, deposit, EVR x2
		t.Fatalf("approval records through the real entry = %d, want 5", approvals)
	}
	if releases != 5 { // chain_scan, query, deposit, EVR releases + query revoke
		t.Fatalf("release decisions through the real entry = %d, want 5", releases)
	}
	if gaps == 0 {
		t.Fatalf("the verification must have established open gaps")
	}
	t.Logf("evidence census: approvals=%d releases=%d open_gaps=%d verify_audit=%d verification_audit=%d approval_audit=%d release_audit=%d status_gate_audits=%d status_refused=%d close_refused_gap_open=%d supersede_refused=%d",
		approvals, releases, gaps,
		raAuditRows(t, f, "verify"),
		raAuditRows(t, f, "verification"),
		raAuditRows(t, f, "approval_decision"),
		raAuditRows(t, f, "release_decision"),
		migrateCountRows(t, f.ctx, f.control,
			`SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = 'gate_admission' AND target->>'request_action' = 'status_review'`, f.instanceID),
		migrateCountRows(t, f.ctx, f.control,
			`SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = 'status_review' AND result = 'refused'`, f.instanceID),
		raRefusalRows(t, f, "instance_close", string(recovery.RefusalGapOpen)),
		raRefusalRows(t, f, "instance_supersede", string(recovery.RefusalApprovalMissing)),
	)

	// No command output may carry a plaintext DSN or its credentials: the
	// control and data DSN passwords are compared verbatim.
	for _, dsn := range []string{f.controlDSN, restoredDSN} {
		parsed, err := url.Parse(dsn)
		if err != nil {
			t.Fatalf("parse DSN: %v", err)
		}
		password, _ := parsed.User.Password()
		for _, text := range []string{restoredOut, step1, step2, status1, status2, out, errOut} {
			if strings.Contains(text, "postgres://") || (password != "" && strings.Contains(text, ":"+password+"@")) {
				t.Fatalf("command output must never carry a plaintext DSN: %q", text)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// S11 positive: every capability released, then the instance closes
// ---------------------------------------------------------------------------

// raSeedPositiveDepositCoverage seeds the restored deposit frontier and the
// confirmation policy so V1's deposit coverage is provable (the negative test
// deliberately leaves the checkpoint absent and establishes the S7 gap).
func raSeedPositiveDepositCoverage(t *testing.T, f *cliFixture, rpcURL string) {
	t.Helper()
	client, err := eth.Dial(context.Background(), rpcURL, 10*time.Second)
	if err != nil {
		t.Fatalf("dial anvil: %v", err)
	}
	defer client.Close()
	tip, err := client.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf("chain tip: %v", err)
	}
	if _, err := f.data.Exec(f.ctx,
		`INSERT INTO deposit_checkpoint (chain_id, start_block, config_hash, next_block)
		 VALUES ($1, 0, $2, $3)`,
		raChainID, strings.Repeat("b", 64), tip+1); err != nil {
		t.Fatalf("seed deposit_checkpoint: %v", err)
	}
	if _, err := f.data.Exec(f.ctx,
		`INSERT INTO confirmation_policy_history (chain_id, policy_seq, threshold, operator, reason)
		 VALUES ($1, 1, 1, 'bootstrap', 'S11 positive close fixture')`,
		raChainID); err != nil {
		t.Fatalf("seed confirmation_policy_history: %v", err)
	}
}

// raRunAllChecklist double-signs every isolation item of the closed set: the
// S11 positive close needs all seven capabilities' isolation dependency sets
// verified, and authorization_recheck (the V8 surface) is deliberately left
// out of the negative test.
func raRunAllChecklist(t *testing.T, f *cliFixture, env func(string) map[string]string) {
	t.Helper()
	for i, item := range recovery.KnownIsolationItemKeys() {
		code, out, errOut := runRecoveryAdmin(t, []string{
			"checklist-set", "--instance", f.instanceID, "--item", string(item),
			"--evidence-ref", "evidence:" + string(item) + ":ra-positive",
			"--operation-id", fmt.Sprintf("ra-pos-chk-set-%d", i+1),
		}, env("deploy:executor"))
		if code != 0 || !strings.Contains(out, "state=evidenced") {
			t.Fatalf("checklist-set %s: exit=%d stdout=%q stderr=%q", item, code, out, errOut)
		}
		code, out, errOut = runRecoveryAdmin(t, []string{
			"checklist-verify", "--instance", f.instanceID, "--item", string(item),
			"--operation-id", fmt.Sprintf("ra-pos-chk-verify-%d", i+1),
		}, env("auth:verifier"))
		if code != 0 || !strings.Contains(out, "state=verified") {
			t.Fatalf("checklist-verify %s: exit=%d stdout=%q stderr=%q", item, code, out, errOut)
		}
	}
}

// raCloseAllOpenGaps closes every open gap of the fixture instance through the
// real T041 gap service with new external evidence: FR-019 says only new
// evidence closes a gap, and the instance-close guard refuses while any gap is
// open. This is the operator evidence path — no gap/instance/approval/release
// success record is inserted directly, and no approval or release row is
// written by this helper.
func raCloseAllOpenGaps(t *testing.T, f *cliFixture) int {
	t.Helper()
	gaps, err := recovery.NewGaps(f.store)
	if err != nil {
		t.Fatalf("NewGaps: %v", err)
	}
	all, err := gaps.List(f.ctx, f.instanceID)
	if err != nil {
		t.Fatalf("list gaps: %v", err)
	}
	closed := 0
	for _, gap := range all {
		if gap.State == recovery.GapStateClosed {
			continue
		}
		evidence := []byte(`{"kind":"operator_reconciliation","object_key":"` + gap.ObjectKey +
			`","source":"trusted copy of the data DB plus canonical chain facts at the restore point","recorded_by":"deploy:executor"}`)
		if _, err := gaps.Close(f.ctx, recovery.CloseGapRequest{
			InstanceID:      f.instanceID,
			GapID:           gap.GapID,
			ClosureEvidence: evidence,
			Actor:           "deploy:executor",
			OperationID:     "ra-pos-gap-close-" + gap.GapID,
			Reason:          "S11 positive fixture: reconciliation evidence for the restored coverage",
		}); err != nil {
			t.Fatalf("close gap %s (%s): %v", gap.GapID, gap.ObjectKey, err)
		}
		closed++
	}
	if closed == 0 {
		t.Fatalf("the real verify entry established no open gap to close; the S11 positive fixture is not exercising the gap guard")
	}
	return closed
}

// raApprovalClass is the conservative class the T050 scope layer requires for
// a capability at the fixture chain (no effect-class ruling configured): the
// same class the CLI records in the approval snapshot.
func raApprovalClass(t *testing.T, capability recovery.Capability) recovery.ApprovalClass {
	t.Helper()
	scope, err := recovery.Scope{ChainID: raChainID, Capability: capability}.Canonical()
	if err != nil {
		t.Fatalf("canonical scope of %s: %v", capability, err)
	}
	parsed, err := recovery.ParseCapabilityScope(scope, capability)
	if err != nil {
		t.Fatalf("parse capability scope of %s: %v", capability, err)
	}
	class, err := recovery.RequiredApprovalClassForScope(parsed, recovery.ScopeEffectClass(parsed), nil)
	if err != nil {
		t.Fatalf("required class of %s: %v", capability, err)
	}
	return class
}

// TestRecoveryAdminS11PositiveCloseRealEntry drives the whole 015 US4 path
// through the real entries to a successful instance close: real verify
// (V1–V9), every open evidence gap closed through the real T041 evidence path,
// approve/release of all seven capabilities at their canonical scopes (single
// and dual, with the one-approval dual refusal), an explicit release revoke
// and re-release, an explicit approval revoke with re-approval and
// re-release, scope equivalence and scope out-of-bounds refusals, an
// evidence change that invalidates an old approval, an operation-id replay,
// status before the close, and `instance-close`.
//
// What close means here: the operator returns the deployment to daily
// operation. It is only allowed while every one of the seven capabilities is
// currently release-valid (derived, re-evaluated under the instance lock), no
// evidence gap is open, and the funds/delivery releases carry two distinct
// non-executor people. No approval, release or close success row is inserted
// by the test: every state change arrives through the CLI entry functions.
func TestRecoveryAdminS11PositiveCloseRealEntry(t *testing.T) {
	requireNativePGRestore(t)
	f := newCLIFixture(t)
	rpcURL := raStartAnvil(t)
	raSeedV1ChainFacts(t, f, rpcURL)
	raSeedPositiveDepositCoverage(t, f, rpcURL)

	f.mapIdentity(t, "auth:approver-1", "person-approver-1")
	f.mapIdentity(t, "auth:approver-2", "person-approver-2")
	f.register(t, "auth:approver-1", "approver")
	f.register(t, "auth:approver-2", "approver")

	checklistEnv := func(principal string) map[string]string {
		return f.raEnv(principal, f.dataDSN, rpcURL)
	}
	raRunAllChecklist(t, f, checklistEnv)

	manifestPath, backupID := f.verifiedBackup(t)
	restoredDSN := f.createDB(t, "restored")
	code, restoredOut, restoredErr := f.cliRestore(t, manifestPath, restoredDSN, "ra-pos-restore-1")
	if code != 0 || !strings.Contains(restoredOut, "restored=true") {
		t.Fatalf("CLI restore: exit=%d stdout=%q stderr=%q", code, restoredOut, restoredErr)
	}
	if got := f.evidenceCount(t, "restore_probe", backupID); got != 1 {
		t.Fatalf("restore_probe evidence rows after the real restore = %d, want 1", got)
	}

	env := func(principal string) map[string]string {
		return f.raEnv(principal, restoredDSN, rpcURL)
	}
	approve := func(principal string, capability recovery.Capability, operationID string) (int, string, string) {
		return runRecoveryAdmin(t, []string{
			"approve", "--instance", f.instanceID, "--capability", string(capability),
			"--scope", raScope(capability), "--operation-id", operationID,
		}, env(principal))
	}
	release := func(capability recovery.Capability, revoke bool, operationID string) (int, string, string) {
		args := []string{
			"release", "--instance", f.instanceID, "--capability", string(capability),
			"--scope", raScope(capability), "--operation-id", operationID,
		}
		if revoke {
			args = append(args, "--revoke")
		}
		return runRecoveryAdmin(t, args, env("deploy:executor"))
	}
	// approveRevoke is the revoke arm of the approve entry: it withdraws the
	// caller principal's own earlier approve rows of the same canonical
	// (instance, capability, scope) stream (the approval-side twin of the
	// release --revoke above).
	approveRevoke := func(principal string, capability recovery.Capability, operationID string) (int, string, string) {
		return runRecoveryAdmin(t, []string{
			"approve", "--instance", f.instanceID, "--capability", string(capability),
			"--scope", raScope(capability), "--operation-id", operationID, "--revoke",
		}, env(principal))
	}
	status := func() (int, string, string) {
		return runRecoveryAdmin(t, []string{"status", "--instance", f.instanceID}, env("deploy:executor"))
	}

	// --- S6: the real verify entry runs; its unknown observations establish
	// open gaps (the fixture data cannot prove V2–V8), which is exactly the
	// state S11 must refuse to close over. ---
	code, verifyOut, verifyErr := runRecoveryAdmin(t, []string{
		"verify", "--instance", f.instanceID, "--scope", "all", "--operation-id", "ra-pos-verify-1",
	}, env("deploy:executor"))
	if code != 0 {
		t.Fatalf("verify: exit=%d stdout=%q stderr=%q", code, verifyOut, verifyErr)
	}
	t.Logf("S11 positive: real verify step:\n%s", verifyOut)

	// --- Evidence change invalidates an old approval: approve chain_scan at
	// the current generation, advance the generation with a real verify step,
	// then the release must refuse approval_stale and write zero rows. ---
	code, out, errOut := approve("auth:approver-1", recovery.CapabilityChainScan, "ra-pos-approve-chain-scan-old")
	if code != 0 {
		t.Fatalf("approve chain_scan before the evidence change: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, verify2, verify2Err := runRecoveryAdmin(t, []string{
		"verify", "--instance", f.instanceID, "--scope", "all", "--operation-id", "ra-pos-verify-2",
	}, env("deploy:executor"))
	if code != 0 {
		t.Fatalf("verify step 2: exit=%d stdout=%q stderr=%q", code, verify2, verify2Err)
	}
	releaseRowsBefore := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_release WHERE instance_id = $1`, f.instanceID)
	code, out, errOut = release(recovery.CapabilityChainScan, false, "ra-pos-release-chain-scan-stale")
	if code != 1 || !strings.Contains(errOut, "refusal_class=approval_stale") {
		t.Fatalf("release over a stale approval must refuse approval_stale: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if got := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_release WHERE instance_id = $1`, f.instanceID); got != releaseRowsBefore {
		t.Fatalf("stale-approval release wrote %d row(s)", got-releaseRowsBefore)
	}

	// --- S11 precondition: on this run the instance still has open gaps, so
	// the close must refuse (a positive close is never granted over an open
	// gap). Every gap is then closed through the real T041 evidence path. ---
	closeEnv := env("deploy:executor")
	closeEnv[config.EnvPGDSN] = f.dataDSN
	code, out, errOut = runRecoveryAdmin(t, []string{
		"instance-close", "--instance", f.instanceID, "--operation-id", "ra-pos-close-open-gap",
	}, closeEnv)
	if code != 1 || !strings.Contains(errOut, "an open evidence gap blocks at least one capability") {
		t.Fatalf("close over the open gaps must refuse: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	closedGaps := raCloseAllOpenGaps(t, f)
	t.Logf("S11 positive: closed %d open evidence gap(s) through the real gap service", closedGaps)

	// --- Scope equivalence: an equivalent representation of the canonical
	// scope canonicalizes onto the same stream. ---
	code, out, errOut = runRecoveryAdmin(t, []string{
		"approve", "--instance", f.instanceID, "--capability", "query",
		"--scope", "Chain=031337;Capability=QUERY", "--operation-id", "ra-pos-approve-query-equiv",
	}, env("auth:approver-1"))
	if code != 0 || !strings.Contains(out, "scope_hash=capability=query;chain=31337") {
		t.Fatalf("equivalent scope approval: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}

	// --- Scope out-of-bounds: a scope naming another capability, and a legacy
	// opaque pre-T050 scope, both refuse as scope_mismatch with zero writes. ---
	approvalsBefore := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_approval WHERE instance_id = $1`, f.instanceID)
	code, out, errOut = runRecoveryAdmin(t, []string{
		"approve", "--instance", f.instanceID, "--capability", "query",
		"--scope", raScope(recovery.CapabilityChainScan), "--operation-id", "ra-pos-approve-cross-capability",
	}, env("auth:approver-1"))
	if code != 1 || !strings.Contains(errOut, "refusal_class=scope_mismatch") {
		t.Fatalf("cross-capability scope approval must refuse scope_mismatch: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, out, errOut = runRecoveryAdmin(t, []string{
		"approve", "--instance", f.instanceID, "--capability", "query",
		"--scope", "chain=31337;surface=serve", "--operation-id", "ra-pos-approve-legacy-opaque",
	}, env("auth:approver-1"))
	if code != 1 || !strings.Contains(errOut, "refusal_class=scope_mismatch") {
		t.Fatalf("legacy opaque scope approval must refuse scope_mismatch: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	// Both refusals wrote zero approval rows (the equivalent-scope approval
	// already counted in approvalsBefore).
	if got := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_approval WHERE instance_id = $1`, f.instanceID); got != approvalsBefore {
		t.Fatalf("refused scopes wrote approval rows: %d -> %d", approvalsBefore, got)
	}

	// --- Release query (single, the equivalent-scope approval) and replay the
	// decision under the same operation id: zero writes, recorded=true. ---
	code, out, errOut = release(recovery.CapabilityQuery, false, "ra-pos-release-query-1")
	if code != 0 || !strings.Contains(out, "decision=release") {
		t.Fatalf("release query: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	releaseRowsAfterQuery := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_release WHERE instance_id = $1`, f.instanceID)
	code, out, errOut = release(recovery.CapabilityQuery, false, "ra-pos-release-query-1")
	if code != 0 || !strings.Contains(out, "recorded=true") {
		t.Fatalf("release query replay: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if got := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_release WHERE instance_id = $1`, f.instanceID); got != releaseRowsAfterQuery {
		t.Fatalf("release replay appended %d row(s)", got-releaseRowsAfterQuery)
	}

	// --- Explicit revoke and re-release: the revoke refuses the next
	// evaluation; re-releasing (the approvals are untouched) converges. ---
	code, out, errOut = release(recovery.CapabilityQuery, true, "ra-pos-revoke-query")
	if code != 0 || !strings.Contains(out, "decision=revoke") {
		t.Fatalf("revoke query release: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, statusRevoked, statusRevokedErr := status()
	if code != 0 {
		t.Fatalf("status after revoke: exit=%d stdout=%q stderr=%q", code, statusRevoked, statusRevokedErr)
	}
	raMustContain(t, "status after revoke", statusRevoked,
		"release_scope=capability=query;chain=31337 released=false refusal_class=release_revoked")
	code, out, errOut = release(recovery.CapabilityQuery, false, "ra-pos-release-query-2")
	if code != 0 || !strings.Contains(out, "decision=release") {
		t.Fatalf("re-release query: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}

	// --- Approval revoke (approve --revoke) and convergence: the approval
	// side of the same four-decision-state cycle (approve, approve --revoke,
	// release, release --revoke). Revoking approver-1's own approve decision
	// collapses the release basis of the stream: the recorded release row
	// still stands (this is not a release revoke), but the next derived
	// evaluation refuses approval_missing; a re-approval plus a new release
	// converge. ---
	code, out, errOut = approveRevoke("auth:approver-1", recovery.CapabilityQuery, "ra-pos-revoke-approve-query")
	if code != 0 || !strings.Contains(out, "decision=revoke") ||
		!strings.Contains(out, "approval_class=single_non_executor") ||
		!strings.Contains(out, "person_id=person-approver-1") {
		t.Fatalf("approve --revoke query: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	raMustContain(t, "approve --revoke note", out,
		"the revoke covers only this principal's earlier approve rows",
		"re-approval plus re-release is required to converge.")
	if got := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_approval
		  WHERE instance_id = $1 AND capability = 'query' AND scope_hash = 'capability=query;chain=31337'
		    AND decision = 'revoke' AND principal = 'auth:approver-1'
		    AND operation_id = 'ra-pos-revoke-approve-query'`, f.instanceID); got != 1 {
		t.Fatalf("approve --revoke must append exactly one revoke decision row through the real entry, got %d", got)
	}
	code, statusApprovalRevoked, statusApprovalRevokedErr := status()
	if code != 0 {
		t.Fatalf("status after the approval revoke: exit=%d stdout=%q stderr=%q", code, statusApprovalRevoked, statusApprovalRevokedErr)
	}
	raMustContain(t, "status after the approval revoke", statusApprovalRevoked,
		"release_scope=capability=query;chain=31337 released=false refusal_class=approval_missing")
	code, out, errOut = approve("auth:approver-1", recovery.CapabilityQuery, "ra-pos-approve-query-2")
	if code != 0 || !strings.Contains(out, "decision=approve") ||
		!strings.Contains(out, "person_id=person-approver-1") {
		t.Fatalf("re-approve query after the approval revoke: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	reApprovalQuery := cliField(out, "approval_id")
	if reApprovalQuery == "" {
		t.Fatalf("re-approve query printed no approval_id: %q", out)
	}
	code, out, errOut = release(recovery.CapabilityQuery, false, "ra-pos-release-query-3")
	if code != 0 || !strings.Contains(out, "decision=release") ||
		!strings.Contains(out, "approval_refs="+reApprovalQuery) {
		t.Fatalf("re-release query after the approval revoke: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, statusApprovalRegranted, statusApprovalRegrantedErr := status()
	if code != 0 {
		t.Fatalf("status after the re-approval and re-release: exit=%d stdout=%q stderr=%q", code, statusApprovalRegranted, statusApprovalRegrantedErr)
	}
	raMustContain(t, "status after the re-approval and re-release", statusApprovalRegranted,
		"release_scope=capability=query;chain=31337 released=true")

	// --- Release all seven capabilities at their canonical scopes. Dual
	// capabilities require two distinct non-executor people: the one-approval
	// attempt refuses with zero rows first. ---
	for _, capability := range recovery.KnownCapabilities() {
		if capability == recovery.CapabilityQuery {
			continue // already released above
		}
		class := raApprovalClass(t, capability)
		code, out, errOut = approve("auth:approver-1", capability, "ra-pos-approve-1-"+string(capability))
		if code != 0 {
			t.Fatalf("approve %s by approver-1: exit=%d stdout=%q stderr=%q", capability, code, out, errOut)
		}
		if class == recovery.ApprovalClassDualNonExecutor {
			rowsBefore := migrateCountRows(t, f.ctx, f.control,
				`SELECT count(*) FROM recovery_release WHERE instance_id = $1 AND capability = $2`, f.instanceID, string(capability))
			code, out, errOut = release(capability, false, "ra-pos-release-single-"+string(capability))
			if code != 1 || !strings.Contains(errOut, "refusal_class=approval_missing") {
				t.Fatalf("dual capability %s with one approval must refuse approval_missing: exit=%d stdout=%q stderr=%q", capability, code, out, errOut)
			}
			if got := migrateCountRows(t, f.ctx, f.control,
				`SELECT count(*) FROM recovery_release WHERE instance_id = $1 AND capability = $2`, f.instanceID, string(capability)); got != rowsBefore {
				t.Fatalf("one-approval release of %s wrote %d row(s)", capability, got-rowsBefore)
			}
			code, out, errOut = approve("auth:approver-2", capability, "ra-pos-approve-2-"+string(capability))
			if code != 0 || !strings.Contains(out, "person_id=person-approver-2") {
				t.Fatalf("approve %s by approver-2: exit=%d stdout=%q stderr=%q", capability, code, out, errOut)
			}
		}
		code, out, errOut = release(capability, false, "ra-pos-release-"+string(capability))
		if code != 0 || !strings.Contains(out, "decision=release") {
			t.Fatalf("release %s: exit=%d stdout=%q stderr=%q", capability, code, out, errOut)
		}
	}

	// --- Status before the close: all seven capabilities released, no open
	// gap, no writable state changed by the review. ---
	code, statusOut, statusErr := status()
	if code != 0 {
		t.Fatalf("status before close: exit=%d stdout=%q stderr=%q", code, statusOut, statusErr)
	}
	for _, capability := range recovery.KnownCapabilities() {
		line := ""
		for _, candidate := range strings.Split(statusOut, "\n") {
			if strings.HasPrefix(candidate, "  capability="+string(capability)+" ") {
				line = candidate
			}
		}
		if !strings.Contains(line, "released=true") {
			t.Fatalf("status must show capability %s released=true before the close: %q\n%s", capability, line, statusOut)
		}
	}
	if got := strings.Count(statusOut, "\n  capability="); got != len(recovery.KnownCapabilities()) {
		t.Fatalf("status must render all %d capabilities, got %d", len(recovery.KnownCapabilities()), got)
	}

	// --- S11 positive: close. Every capability is release-valid and no gap is
	// open, so the instance returns to daily operation. ---
	closeEnv = env("deploy:executor")
	closeEnv[config.EnvPGDSN] = f.dataDSN
	code, out, errOut = runRecoveryAdmin(t, []string{
		"instance-close", "--instance", f.instanceID, "--operation-id", "ra-pos-close-1",
	}, closeEnv)
	if code != 0 || !strings.Contains(out, "closed=true") {
		t.Fatalf("S11 positive close: exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if _, state := raInstanceState(t, f); state != "closed" {
		t.Fatalf("instance after the close = %q, want closed (back to daily operation)", state)
	}
	// The close is recorded: refusal rows for the failed attempts and exactly
	// two ok rows — the library close guard's lifecycle row plus the CLI
	// operation row of the same command.
	if got := raRefusalRows(t, f, "instance_close", string(recovery.RefusalGapOpen)); got < 1 {
		t.Fatalf("the refused close over the open gap must be audited with refusal_class=gap_open, got %d", got)
	}
	closeAudits := migrateCountRows(t, f.ctx, f.control,
		`SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = 'instance_close' AND result = 'ok'`, f.instanceID)
	if closeAudits != 2 {
		t.Fatalf("instance_close ok audit rows = %d, want 2 (library lifecycle row + CLI operation row)", closeAudits)
	}

	// The evidence census: every release row arrived through the CLI, and the
	// dual capabilities carry two-person bases.
	dualMissing := 0
	for _, capability := range recovery.KnownCapabilities() {
		if raApprovalClass(t, capability) != recovery.ApprovalClassDualNonExecutor {
			continue
		}
		if got := migrateCountRows(t, f.ctx, f.control,
			`SELECT count(*) FROM recovery_release WHERE instance_id = $1 AND capability = $2 AND decision = 'release' AND cardinality(approval_refs) = 2`,
			f.instanceID, string(capability)); got < 1 {
			dualMissing++
		}
	}
	if dualMissing != 0 {
		t.Fatalf("%d dual capability(ies) closed without a two-person release basis", dualMissing)
	}

	// --- Legacy pre-T050 scopes: a recorded opaque scope matches no canonical
	// stream; the review surfaces it for re-approval/re-release instead of
	// guessing a translation. The row is a negative fixture (an old-format
	// stream), inserted only after the close so it cannot mask the positive
	// path. ---
	if _, err := f.control.Exec(f.ctx, `
INSERT INTO recovery_release (release_id, instance_id, capability, scope_hash, decision,
       evidence_generation, evidence_hash, operation_id, reason)
SELECT gen_random_uuid(), instance_id, 'query', 'chain=31337;surface=serve', 'release',
       evidence_generation, evidence_hash, 'ra-pos-legacy-scope-fixture', 'pre-T050 opaque scope fixture'
FROM recovery_instance WHERE instance_id = $1`, f.instanceID); err != nil {
		t.Fatalf("seed legacy scope fixture: %v", err)
	}
	code, statusLegacy, statusLegacyErr := status()
	if code != 0 {
		t.Fatalf("status with the legacy fixture: exit=%d stdout=%q stderr=%q", code, statusLegacy, statusLegacyErr)
	}
	raMustContain(t, "status legacy scope", statusLegacy,
		"unattributable_scope=chain=31337;surface=serve",
		"re-approval and re-release at the canonical capability scope are required")

	// No command output may carry a plaintext DSN or its credentials.
	parsed, err := url.Parse(restoredDSN)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	password, _ := parsed.User.Password()
	for _, text := range []string{restoredOut, verifyOut, verify2, out, errOut, statusOut, statusLegacy} {
		if strings.Contains(text, "postgres://") || (password != "" && strings.Contains(text, ":"+password+"@")) {
			t.Fatalf("command output must never carry a plaintext DSN: %q", text)
		}
	}
}
