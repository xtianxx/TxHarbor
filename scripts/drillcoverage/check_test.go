package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func event(name, action string) string {
	return fmt.Sprintf(`{"Action":%q,"Test":%q,"Package":"github.com/xtianxx/txharbor/internal/recovery"}`+"\n", action, name)
}

func completeEvents() string {
	var lines strings.Builder
	for _, name := range requiredTests {
		lines.WriteString(event(name, "pass"))
	}
	for _, name := range requiredSubtests {
		lines.WriteString(event(name, "pass"))
	}
	return lines.String()
}

func TestMissingDependencySkipIsNamedNotRun(t *testing.T) {
	name := requiredTests[1]
	input := strings.Replace(completeEvents(), event(name, "pass"), event(name, "skip"), 1)
	passed, diagnostics := checkEvents(strings.NewReader(input))
	if passed || !hasDiagnostic(diagnostics, "NOT RUN: "+name) {
		t.Fatalf("checkEvents() = (%v, %v), want named NOT RUN", passed, diagnostics)
	}
}

func TestNestedSkipFailsEvenWhenParentPasses(t *testing.T) {
	name := requiredSubtests[0]
	input := completeEvents() + event(name, "skip")
	passed, diagnostics := checkEvents(strings.NewReader(input))
	if passed || !hasDiagnostic(diagnostics, "NOT RUN: "+name) {
		t.Fatalf("checkEvents() = (%v, %v), want nested NOT RUN", passed, diagnostics)
	}
}

func TestMissingRequiredCaseIsNotRun(t *testing.T) {
	missing := requiredTests[3]
	input := strings.Replace(completeEvents(), event(missing, "pass"), "", 1)
	passed, diagnostics := checkEvents(strings.NewReader(input))
	if passed || !hasDiagnostic(diagnostics, "NOT RUN: "+missing) {
		t.Fatalf("checkEvents() = (%v, %v), want missing-case NOT RUN", passed, diagnostics)
	}
}

func TestAllRequiredPassAllowsUnrelatedOptionalSkip(t *testing.T) {
	input := completeEvents() + event("TestOptionalUnrelatedCase", "skip")
	passed, diagnostics := checkEvents(strings.NewReader(input))
	if !passed || len(diagnostics) != 0 {
		t.Fatalf("checkEvents() = (%v, %v), want pass with no diagnostics", passed, diagnostics)
	}
}

func TestEvidenceReportsPerCaseAndOptionalSkipWithoutSecrets(t *testing.T) {
	t.Setenv("TXHARBOR_TESTED_TREE_FINGERPRINT", "sha256:test")
	input := completeEvents() + event("TestOptionalUnrelatedCase", "skip")
	statuses, packages, passed, diagnostics := inspectEvents(strings.NewReader(input))
	report := buildEvidence(statuses, packages, diagnostics, passed, 0, "./...", "run-1", "now")
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	if report.OverallResult != "PASS" || len(report.Required) != len(requiredTests)+len(requiredSubtests) || len(report.OptionalSkips) != 1 {
		t.Fatalf("incomplete report: %+v", report)
	}
	if !strings.Contains(text, `"status":"PASS"`) || !strings.Contains(text, "TestOptionalUnrelatedCase") {
		t.Fatalf("report lacks case statuses/optional skip: %s", text)
	}
	for _, secret := range []string{"TXHARBOR_PG_DSN", "postgres://", "private-key", "TXHARBOR_RPC_URL"} {
		if strings.Contains(text, secret) {
			t.Fatalf("secret field/value leaked into report: %s", secret)
		}
	}
}

func TestEvidenceMarksMissingSkippedAndFailedCasesAndPreservesGoExit(t *testing.T) {
	t.Setenv("TXHARBOR_TESTED_TREE_FINGERPRINT", "sha256:test")
	input := completeEvents()
	input = strings.Replace(input, event(requiredTests[0], "pass"), "", 1)
	input = strings.Replace(input, event(requiredTests[1], "pass"), event(requiredTests[1], "skip"), 1)
	input = strings.Replace(input, event(requiredTests[2], "pass"), event(requiredTests[2], "fail"), 1)
	statuses, packages, passed, diagnostics := inspectEvents(strings.NewReader(input))
	report := buildEvidence(statuses, packages, diagnostics, passed, 7, "./...", "run-2", "now")
	if report.GoTestExit != 7 || report.CheckerResult != "FAIL" || report.OverallResult != "FAIL" {
		t.Fatalf("failure state lost: %+v", report)
	}
	want := map[string]string{requiredTests[0]: "NOTRUN", requiredTests[1]: "SKIP", requiredTests[2]: "FAIL"}
	for _, c := range report.Required {
		if status, ok := want[c.Name]; ok && c.Status != status {
			t.Errorf("%s: status %s, want %s", c.Name, c.Status, status)
		}
	}
}

func TestCommitDoesNotSubstituteForTestedTreeFingerprint(t *testing.T) {
	t.Setenv("TXHARBOR_COMMIT", "0123456789abcdef")
	t.Setenv("TXHARBOR_TESTED_TREE_FINGERPRINT", "")
	statuses, packages, passed, diagnostics := inspectEvents(strings.NewReader(completeEvents()))
	report := buildEvidence(statuses, packages, diagnostics, passed, 0, "./...", "run-commit", "now")
	if report.Commit == "" || report.TreeFingerprint != "" || report.OverallResult != "FAIL" || report.CheckerResult != "FAIL" {
		t.Fatalf("commit substituted for tree identity: %+v", report)
	}
}

func TestRequiredWitnessFromWrongPackageFails(t *testing.T) {
	input := strings.Replace(completeEvents(), `"Package":"github.com/xtianxx/txharbor/internal/recovery"`, `"Package":"example.com/unrelated"`, 1)
	passed, diagnostics := checkEvents(strings.NewReader(input))
	if passed || !hasDiagnostic(diagnostics, "unexpected package") {
		t.Fatalf("wrong-package witness accepted: %v", diagnostics)
	}
}

func TestRuntimeFingerprintIgnoresDocsButTracksRuntimeAndTestInputs(t *testing.T) {
	root := t.TempDir()
	write := func(name, body string) {
		t.Helper()
		path := root + "/" + name
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	write("internal/recovery/a.go", "package recovery\n")
	write("testdata/schema.sql", "create table a;\n")
	write("docs/guide.md", "before\n")
	first, err := runtimeFingerprint(root)
	if err != nil {
		t.Fatal(err)
	}
	write("docs/guide.md", "after docs-only edit\n")
	second, err := runtimeFingerprint(root)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("docs edit changed runtime fingerprint: %s != %s", first, second)
	}
	write("internal/recovery/a.go", "package recovery // changed\n")
	third, err := runtimeFingerprint(root)
	if err != nil {
		t.Fatal(err)
	}
	if second == third {
		t.Fatal("runtime source edit did not change fingerprint")
	}
	write("internal/recovery/a.go", "package recovery\n")
	write("testdata/schema.sql", "create table b;\n")
	fourth, err := runtimeFingerprint(root)
	if err != nil {
		t.Fatal(err)
	}
	if first == fourth {
		t.Fatal("test schema edit did not change fingerprint")
	}
}

func TestRuntimeFingerprintFailsWhenInputsUnreadableOrAbsent(t *testing.T) {
	empty := t.TempDir()
	if _, err := runtimeFingerprint(empty); err == nil {
		t.Fatal("expected missing runtime inputs to fail")
	}
	root := t.TempDir()
	path := root + "/input.go"
	if err := os.WriteFile(path, []byte("package p"), 0000); err != nil {
		t.Fatal(err)
	}
	if _, err := runtimeFingerprint(root); err == nil {
		t.Fatal("expected unreadable runtime input to fail")
	}
}

func TestWriteEvidenceCreatesReport(t *testing.T) {
	path := t.TempDir() + "/coverage.json"
	if err := writeEvidence(path, runEvidence{RunID: "test", OverallResult: "PASS"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func hasDiagnostic(diagnostics []string, fragment string) bool {
	for _, diagnostic := range diagnostics {
		if strings.Contains(diagnostic, fragment) {
			return true
		}
	}
	return false
}
