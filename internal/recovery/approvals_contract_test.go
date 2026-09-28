//go:build contract

// approvals_contract_test.go is T043 [US4]: the approval-validity contract
// layer (tags: contract; no Docker; FR-023/024; data-model.md §1.2/§1.3/
// §1.8/§1.9/§3.1; contracts/approval-matrix.md §1–§5; tasks.md T043).
//
// What this layer can and cannot execute without a database. Approval
// validity is derived inside the gate's instance-row-lock transaction
// (gate.go approvalsValid), so the real behavior — dual = two different
// principals whose person_id values differ, executor exclusion resolved
// through this instance's recorded participants/mappings, role/registration/
// mapping refusals, generation/hash staleness, per-principal revoke coverage
// and the hard gates that approvals can never cover — is executed against a
// real PostgreSQL control store in release_integration_test.go (T044) and
// identity_path_integration_test.go (T046). This contract file pins the
// surface those layers depend on:
//
//   - the closed approval-class set of recovery_approval
//     (single_non_executor / dual_non_executor) and the conservative
//     required class of every one of the 7 capabilities: the 4 high-impact
//     capabilities (existing_withdrawal_recovery, new_withdrawal_creation,
//     event_publishing, event_consuming) are dual — the event effect-class
//     ruling is not available yet, so an unknown effect class is never
//     silently narrowed to single;
//   - the approval refusal classes of the closed gate set
//     (approval_missing, approval_identity_unverified,
//     approval_executor_excluded, approval_stale) and their controlstore
//     aliases;
//   - the storage contract of the append-only decision tables: recovery_approval
//     records approve/revoke decisions with principal+person_id, generation/
//     hash binding and operation_id idempotency; recovery_release records
//     release/revoke decisions with deterministic approval_refs; neither
//     table carries a writable "released"/"valid" boolean (INV-2); the only
//     write direction in the control-store source is INSERT;
//   - no bypass surface: the package API and the deployment configuration
//     carry no emergency/force/override/disable entry for approvals, and no
//     direct approval write path is exposed beyond the control store's
//     append-only decision writers.
//
// Every assertion here is pure Go: no Docker, no database, no filesystem
// beyond reading the embedded control-store schema and the package sources.
package recovery

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
)

// t043BypassName matches every identifier/key that would look like an
// approval or release bypass. 015 has no such entry: FR-023 closes the
// approval path and the quickstart §4 anti-cheat discipline forbids an
// emergency force-resume path.
var t043BypassName = regexp.MustCompile(`(?i)(disable|bypass|skip|waive|override|force|ignore|unsafe|emergency)`)

// t043ControlSchema reads the embedded control-store initial migration.
func t043ControlSchema(t *testing.T) string {
	t.Helper()
	raw, err := schema.FS.ReadFile("0001_control_init.sql")
	if err != nil {
		t.Fatalf("read embedded control-store schema: %v", err)
	}
	return string(raw)
}

// t043TableBlock extracts one CREATE TABLE body (up to the closing ");" at
// the start of a line) and returns it whitespace-normalized, so column and
// constraint spellings can be matched independent of formatting.
func t043TableBlock(t *testing.T, sqlText, table string) string {
	t.Helper()
	pattern := regexp.MustCompile(`(?ms)^CREATE TABLE ` + regexp.QuoteMeta(table) + ` \((.*?)^\);`)
	m := pattern.FindStringSubmatch(sqlText)
	if m == nil {
		t.Fatalf("control-store schema has no CREATE TABLE %s block", table)
	}
	return strings.Join(strings.Fields(m[1]), " ")
}

// t043RequireRE asserts that pattern matches the whitespace-normalized block.
func t043RequireRE(t *testing.T, block, pattern, what string) {
	t.Helper()
	if !regexp.MustCompile(pattern).MatchString(block) {
		t.Fatalf("%s: %s is not recorded (pattern %q)", what, what, pattern)
	}
}

// t043PackageSources returns the non-test Go sources of dir. A contract
// failure (not a skip) when the directory is unreadable: the anti-bypass
// contract cannot be verified without its input.
func t043PackageSources(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read package sources in %s: %v", dir, err)
	}
	files := make(map[string]string)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %s: %v", filepath.Join(dir, name), err)
		}
		files[name] = string(raw)
	}
	if len(files) == 0 {
		t.Fatalf("no non-test Go sources found in %s", dir)
	}
	return files
}

// TestT043ApprovalClassIsSingleDualAndConservative pins the closed two-member
// approval-class set and the conservative required class of all 7
// capabilities: single for the read/scan/confirmation capabilities, dual for
// every high-impact capability — including the two event capabilities, whose
// real-downstream effect class is a pending deployment ruling and therefore
// never defaulted to single (FR-023; data-model §1.9; T050 tightens, never
// relaxes).
func TestT043ApprovalClassIsSingleDualAndConservative(t *testing.T) {
	if ApprovalClassSingleNonExecutor != "single_non_executor" || ApprovalClassDualNonExecutor != "dual_non_executor" {
		t.Fatalf("approval-class vocabulary = (%q, %q), want (single_non_executor, dual_non_executor)",
			ApprovalClassSingleNonExecutor, ApprovalClassDualNonExecutor)
	}

	wantClass := map[Capability]ApprovalClass{
		CapabilityQuery:                      ApprovalClassSingleNonExecutor,
		CapabilityChainScan:                  ApprovalClassSingleNonExecutor,
		CapabilityDepositConfirmation:        ApprovalClassSingleNonExecutor,
		CapabilityExistingWithdrawalRecovery: ApprovalClassDualNonExecutor,
		CapabilityNewWithdrawalCreation:      ApprovalClassDualNonExecutor,
		CapabilityEventPublishing:            ApprovalClassDualNonExecutor,
		CapabilityEventConsuming:             ApprovalClassDualNonExecutor,
	}
	used := make(map[ApprovalClass]bool)
	for _, capability := range KnownCapabilities() {
		class, err := RequiredApprovalClass(capability)
		if err != nil {
			t.Fatalf("RequiredApprovalClass(%s): %v", capability, err)
		}
		if class != wantClass[capability] {
			t.Fatalf("required class of %s = %q, want %q (conservative dual for high impact)", capability, class, wantClass[capability])
		}
		used[class] = true
	}
	if len(used) != 2 {
		t.Fatalf("capability matrix uses %d approval classes, want exactly the 2-member closed set", len(used))
	}
	if !used[ApprovalClassSingleNonExecutor] || !used[ApprovalClassDualNonExecutor] {
		t.Fatalf("both approval classes must be reachable, got %v", used)
	}

	// The compiled-in table covers exactly the closed 7 capabilities; an
	// extra or missing entry would make the required class of a capability
	// depend on nothing but a map default.
	if len(requiredApprovalClasses) != len(KnownCapabilities()) {
		t.Fatalf("requiredApprovalClasses has %d entries, want %d (one per known capability)",
			len(requiredApprovalClasses), len(KnownCapabilities()))
	}
	for capability := range requiredApprovalClasses {
		if !capability.Known() {
			t.Fatalf("requiredApprovalClasses contains unknown capability %q", capability)
		}
	}
	if _, err := RequiredApprovalClass("order_book"); err == nil {
		t.Fatal("an unknown capability must be refused, never mapped to a default class")
	}
}

// TestT043DualDistinctnessAndRevokeSemanticsPinnedBySchema pins the storage
// contract behind the derived rules: the dual class is a row-level snapshot,
// person identity is a NOT NULL column resolved from recovery_identity (whose
// PK is the principal, so one person may hold several principals and
// distinctness can only be derived), a revoke is a per-principal decision of
// the same stream, and generation/hash binding is present on every decision.
func TestT043DualDistinctnessAndRevokeSemanticsPinnedBySchema(t *testing.T) {
	sqlText := t043ControlSchema(t)
	approval := t043TableBlock(t, sqlText, "recovery_approval")
	release := t043TableBlock(t, sqlText, "recovery_release")
	identity := t043TableBlock(t, sqlText, "recovery_identity")
	participant := t043TableBlock(t, sqlText, "recovery_participant")

	// recovery_approval: approve/revoke decisions, class snapshot, resolved
	// person, generation/hash binding, idempotent operation_id.
	t043RequireRE(t, approval, `decision\s+TEXT\s+NOT NULL`, "recovery_approval.decision")
	t043RequireRE(t, approval, `CHECK\s*\(\s*decision\s+IN\s*\(\s*'approve',\s*'revoke'\s*\)\s*\)`, "approval decision closed set")
	t043RequireRE(t, approval, `approval_class_snapshot\s+TEXT\s+NOT NULL`, "recovery_approval.approval_class_snapshot")
	t043RequireRE(t, approval, `CHECK\s*\(\s*approval_class_snapshot\s+IN\s*\(\s*'single_non_executor',\s*'dual_non_executor'\s*\)\s*\)`, "approval class closed set")
	t043RequireRE(t, approval, `principal\s+TEXT\s+NOT NULL`, "recovery_approval.principal")
	t043RequireRE(t, approval, `person_id\s+TEXT\s+NOT NULL`, "recovery_approval.person_id")
	t043RequireRE(t, approval, `evidence_generation\s+BIGINT\s+NOT NULL`, "recovery_approval.evidence_generation")
	t043RequireRE(t, approval, `evidence_hash\s+TEXT\s+NOT NULL`, "recovery_approval.evidence_hash")
	t043RequireRE(t, approval, `UNIQUE\s*\(\s*operation_id\s*\)`, "approval operation_id idempotency")
	t043RequireRE(t, approval, `capability\s+IN\s*\(`, "approval capability closed set")

	// recovery_release: release/revoke decisions, deterministic approval
	// refs, generation/hash binding, idempotent operation_id.
	t043RequireRE(t, release, `CHECK\s*\(\s*decision\s+IN\s*\(\s*'release',\s*'revoke'\s*\)\s*\)`, "release decision closed set")
	t043RequireRE(t, release, `approval_refs\s+UUID\[\]\s+NOT NULL`, "recovery_release.approval_refs")
	t043RequireRE(t, release, `evidence_generation\s+BIGINT\s+NOT NULL`, "recovery_release.evidence_generation")
	t043RequireRE(t, release, `evidence_hash\s+TEXT\s+NOT NULL`, "recovery_release.evidence_hash")
	t043RequireRE(t, release, `UNIQUE\s*\(\s*operation_id\s*\)`, "release operation_id idempotency")

	// Identity: PK(principal), so the person<->principal mapping is many-to-one
	// and "two people" can only be derived from person_id distinctness; a
	// missing mapping is "cannot prove a distinct person" (approval-matrix §2).
	t043RequireRE(t, identity, `PRIMARY KEY\s*\(\s*principal\s*\)`, "recovery_identity PK(principal)")
	// Participant: PK(instance_id, principal, role) — one person may hold
	// several roles/principals, executor exclusion is per instance record and
	// never inferred from a different user name.
	t043RequireRE(t, participant, `PRIMARY KEY\s*\(\s*instance_id,\s*principal,\s*role\s*\)`, "recovery_participant PK")

	// The approval refs recorded on a release are written deterministically
	// (ascending, de-duplicated): pin the shared helper.
	got := dedupeApprovalRefs([]string{" b ", "a", "", "a", "c"})
	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dedupeApprovalRefs = %v, want deterministic sorted %v", got, want)
	}
	if !slices.IsSorted(got) {
		t.Fatalf("approval refs must be sorted, got %v", got)
	}
	if maxGateApprovalRefs <= 0 {
		t.Fatalf("approval reference bound must be positive, got %d", maxGateApprovalRefs)
	}

	// A revoke is a per-principal decision: the store's approval-stream read
	// is principal-scoped, so one approver's revoke can never cover another
	// approver's approve.
	storeSource, err := os.ReadFile("controlstore/store.go")
	if err != nil {
		t.Fatalf("read controlstore/store.go: %v", err)
	}
	principalScoped := regexp.MustCompile(`(?m)WHERE d\.instance_id = \$1 AND d\.capability = \$2 AND d\.scope_hash = \$3 AND d\.principal = \$4`)
	if !principalScoped.Match(storeSource) {
		t.Fatal("the approval decision read must be scoped by principal: a revoke covers only that principal's earlier approve (data-model §1.8)")
	}
}

// TestT043DecisionTablesAreAppendOnlyWithoutValidityBoolean pins INV-2 and the
// append-only write direction: neither decision table carries a writable
// "released"/"valid"/"approved" boolean, and the control-store sources only
// ever INSERT into them (a later revoke is a new row, never an update).
func TestT043DecisionTablesAreAppendOnlyWithoutValidityBoolean(t *testing.T) {
	sqlText := t043ControlSchema(t)
	for _, table := range []string{"recovery_approval", "recovery_release"} {
		block := t043TableBlock(t, sqlText, table)
		booleanFlag := regexp.MustCompile(`(?i)\b(released|valid|is_valid|approved|active|enabled)\s+(BOOL|BOOLEAN)\b`)
		if loc := booleanFlag.FindString(block); loc != "" {
			t.Fatalf("%s carries a writable validity boolean %q; release validity must be derived at every evaluation (INV-2)", table, loc)
		}
	}
	release := t043TableBlock(t, sqlText, "recovery_release")
	if strings.Contains(strings.ToLower(release), " released ") {
		t.Fatalf("recovery_release must not carry a released flag: %s", release)
	}

	writeDirection := regexp.MustCompile(`(?i)\b(UPDATE|DELETE\s+FROM)\s+recovery_(approval|release)\b`)
	insertCount := 0
	for _, dir := range []string{".", "controlstore"} {
		for name, source := range t043PackageSources(t, dir) {
			if loc := writeDirection.FindString(source); loc != "" {
				t.Fatalf("%s/%s contains a mutation of an append-only decision table (%q); only INSERT is allowed", dir, name, loc)
			}
			insertCount += strings.Count(source, "INSERT INTO recovery_approval")
			insertCount += strings.Count(source, "INSERT INTO recovery_release")
		}
	}
	if insertCount == 0 {
		t.Fatal("no INSERT into recovery_approval/recovery_release found in the control-store sources; the append-only write path cannot be verified")
	}
	// The operation_id idempotency constraints are the write path's zero-flip
	// protection; they must exist by name in the sources and the schema.
	storeSource, err := os.ReadFile("controlstore/store.go")
	if err != nil {
		t.Fatalf("read controlstore/store.go: %v", err)
	}
	for _, marker := range []string{"recovery_approval_operation_id_uniq", "recovery_release_operation_id_uniq"} {
		if !strings.Contains(string(storeSource), marker) {
			t.Fatalf("control-store write path does not reference %s; operation_id idempotency is not enforced", marker)
		}
		if !strings.Contains(sqlText, marker) {
			t.Fatalf("control-store schema does not declare %s", marker)
		}
	}
}

// TestT043ApprovalRefusalClosedSetAndAliases pins the refusal classes the
// approval path is allowed to produce. Every member is part of the closed
// gate set (the recovery_audit.refusal_class CHECK), and
// approval_identity_unverified keeps its shared controlstore constant — the
// F19 mapping-change path and the gate must not drift apart.
func TestT043ApprovalRefusalClosedSetAndAliases(t *testing.T) {
	for _, class := range []RefusalClass{
		RefusalApprovalMissing,
		RefusalApprovalIdentityUnverified,
		RefusalApprovalExecutorExcluded,
		RefusalApprovalStale,
	} {
		if !class.Known() {
			t.Fatalf("approval refusal class %q is outside the closed set", class)
		}
		if !slices.Contains(KnownRefusalClasses(), class) {
			t.Fatalf("approval refusal class %q is missing from the canonical closed set", class)
		}
	}
	if RefusalApprovalIdentityUnverified != controlstore.RefusalApprovalIdentityUnverified {
		t.Fatal("approval_identity_unverified must alias the controlstore constant (F19 mapping-change and gate paths share it)")
	}
	// A class outside the closed set never becomes an approval outcome: the
	// audit writer refuses unknown refusal_class values (controlstore).
	bad := RefusalClass("emergency_allow")
	if bad.Known() {
		t.Fatalf("%q must never be a known class: risk acceptance and forced resumption have no entry in 015", bad)
	}

	// Hard gates stay independent of approvals: the classes expressing a
	// missing-evidence / unproven-isolation / open-gap / active fund-gate
	// condition are part of the same closed set, so an approval can never be
	// read as an override (FR-023 item 5).
	for _, class := range []RefusalClass{
		RefusalIsolationUnproven,
		RefusalGapOpen,
		RefusalHardGateActive,
	} {
		if !class.Known() || !slices.Contains(KnownRefusalClasses(), class) {
			t.Fatalf("hard-gate refusal class %q must be in the closed set; approvals never cover hard gates", class)
		}
	}
}

// TestT043ApprovalPathsHaveNoBypassEntry scans the real sources for an
// emergency/force/override/disable approval or release entry: the 015
// approval path applies only to post-disaster resumption, adds no bypass over
// the existing fund gates, and must never grow a "close the gate" switch
// (FR-023; quickstart §4).
func TestT043ApprovalPathsHaveNoBypassEntry(t *testing.T) {
	funcOrType := regexp.MustCompile(`(?m)^func\s+(?:\([^)]*\)\s*)?([A-Z][A-Za-z0-9_]*)\b|^type\s+([A-Z][A-Za-z0-9_]*)\b`)
	fieldName := regexp.MustCompile(`(?m)^\s+([A-Z][A-Za-z0-9_]*)\s+(?:[A-Za-z_][A-Za-z0-9_.\[\]\*]*)\s*$`)
	scanned := 0
	for _, dir := range []string{".", "controlstore"} {
		for name, source := range t043PackageSources(t, dir) {
			for _, m := range funcOrType.FindAllStringSubmatch(source, -1) {
				identifier := m[1]
				if identifier == "" {
					identifier = m[2]
				}
				scanned++
				if t043BypassName.MatchString(identifier) {
					t.Fatalf("%s/%s exports %q, which looks like an approval/release bypass; 015 has no force/override entry", dir, name, identifier)
				}
			}
			for _, m := range fieldName.FindAllStringSubmatch(source, -1) {
				scanned++
				if t043BypassName.MatchString(m[1]) {
					t.Fatalf("%s/%s declares field %q, which looks like an approval/release bypass", dir, name, m[1])
				}
			}
		}
	}
	if scanned == 0 {
		t.Fatal("anti-bypass scan inspected no identifiers; the contract cannot be verified")
	}

	// Deployment configuration: no approval/release bypass key may exist; an
	// unrecognized knob would be a forced-resumption entry.
	src, err := os.ReadFile("../config/config.go")
	if err != nil {
		t.Fatalf("read config source: %v", err)
	}
	keyRE := regexp.MustCompile(`"(TXHARBOR_RECOVERY_[A-Z0-9_]*)"`)
	matches := keyRE.FindAllStringSubmatch(string(src), -1)
	if len(matches) == 0 {
		t.Fatal("config source scan found no TXHARBOR_RECOVERY_* keys")
	}
	for _, m := range matches {
		if t043BypassName.MatchString(m[1]) {
			t.Fatalf("config key %s looks like an approval/release bypass; the 015 approval path has no force-resume knob", m[1])
		}
	}
}
