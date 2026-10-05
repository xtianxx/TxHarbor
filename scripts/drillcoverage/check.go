package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var requiredTests = []string{
	"TestDrillArmRefusesReplacedELFAndPreStartTamper",
	"TestDrillTargetWitnessOldReconnectRejected",
	"TestDrillTargetWitnessOldRoleLoginDuringRestoreAcceptanceFailsClosed",
	"TestDrillTargetWitnessObserverTerminationDuringRestoreAcceptanceFailsClosed",
	"TestT058DrillE2EBackupAdvanceRestoreRefuseThenRelease",
	"TestT058DrillGapCannotBeClosedStaysPaused",
	"TestT058DrillEventLayerKafkaBrokerOffsetDivergence",
	"TestT058DrillEventEffectRecovery",
	"TestT058WithdrawalEffectRestoreRetry",
	"TestT059F1BackupUnusableCorruptTruncatedUnverified",
	"TestT059F2RestoreInterruptionNotRestoredRebuildRerunIdempotent",
	"TestT059F3IncompatibleProgramRefusedNoSilentDowngrade",
	"TestT059F4ExternalLeadZeroReplayZeroAuthorityWrites",
	"TestT059F5IsolationUnprovenRefusedAndNotInferred",
	"TestT059F6UnprovableGapStaysUnknownWithPackageAndEscalation",
	"TestT059F7UnauthorizedInsufficientStaleApprovalsRefused",
	"TestT060DrillRepeatedVerifyApproveReleaseCloseNoFlips",
	"TestT060DrillRestoreReentryConvergesWithSingleState",
	"TestT060DrillInstanceCloseOnlyWhenAllReleasedAndIdempotent",
}

var requiredSubtests = []string{
	"TestT059F1BackupUnusableCorruptTruncatedUnverified/corrupt-verification",
	"TestT059F1BackupUnusableCorruptTruncatedUnverified/edited-verified-manifest",
	"TestT059F3IncompatibleProgramRefusedNoSilentDowngrade/incompatible-restore",
}

type testEvent struct {
	Action  string `json:"Action"`
	Test    string `json:"Test"`
	Package string `json:"Package"`
}

type caseEvidence struct {
	Name    string `json:"name"`
	Package string `json:"package,omitempty"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
}
type runEvidence struct {
	RunID           string         `json:"run_id"`
	StartedAt       string         `json:"started_at"`
	Commit          string         `json:"commit,omitempty"`
	TreeFingerprint string         `json:"tree_fingerprint,omitempty"`
	TreeStable      *bool          `json:"tree_stable,omitempty"`
	Package         string         `json:"package"`
	GoTestExit      int            `json:"go_test_exit_status"`
	CheckerResult   string         `json:"checker_result"`
	CheckerExit     int            `json:"checker_exit_status"`
	OverallResult   string         `json:"overall_result"`
	Required        []caseEvidence `json:"required_cases"`
	OptionalSkips   []string       `json:"optional_skips"`
}

func checkEvents(input io.Reader) (bool, []string) {
	_, _, passed, diagnostics := inspectEvents(input)
	return passed, diagnostics
}

func inspectEvents(input io.Reader) (map[string][]string, map[string]string, bool, []string) {
	statuses := make(map[string][]string)
	packages := make(map[string]string)
	scanner := bufio.NewScanner(input)
	// Test output can contain large JSON strings; don't retain Scanner's small default limit.
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		line := scanner.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		var event testEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return statuses, nil, false, []string{fmt.Sprintf("invalid go test JSON at line %d: %v", lineNumber, err)}
		}
		switch event.Action {
		case "pass", "fail", "skip":
			if event.Test != "" {
				statuses[event.Test] = append(statuses[event.Test], event.Action)
				if event.Package != "" {
					packages[event.Test] = event.Package
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return statuses, packages, false, []string{fmt.Sprintf("reading go test JSON: %v", err)}
	}

	var errors []string
	for _, name := range requiredTests {
		if err := statusError(name, statuses[name]); err != "" {
			errors = append(errors, err)
		}
		if pkg := packages[name]; pkg != "github.com/xtianxx/txharbor/internal/recovery" {
			errors = append(errors, fmt.Sprintf("FAIL: %s reported from unexpected package %q", name, pkg))
		}
	}
	// A parent can report PASS even if a nested case was skipped.
	observed := make([]string, 0, len(statuses))
	for name := range statuses {
		observed = append(observed, name)
	}
	sort.Strings(observed)
	for _, name := range observed {
		for _, required := range requiredTests {
			if strings.HasPrefix(name, required+"/") && contains(statuses[name], "skip") {
				errors = append(errors, fmt.Sprintf("NOT RUN: %s (SKIP beneath required %s)", name, required))
				break
			}
		}
	}
	for _, name := range requiredSubtests {
		if err := statusError(name, statuses[name]); err != "" {
			errors = append(errors, err)
		}
		if pkg := packages[name]; pkg != "github.com/xtianxx/txharbor/internal/recovery" {
			errors = append(errors, fmt.Sprintf("FAIL: %s reported from unexpected package %q", name, pkg))
		}
	}
	return statuses, packages, len(errors) == 0, errors
}

func statusError(name string, actions []string) string {
	if contains(actions, "skip") {
		return fmt.Sprintf("NOT RUN: %s (SKIP)", name)
	}
	if contains(actions, "fail") {
		return fmt.Sprintf("FAIL: %s", name)
	}
	if !contains(actions, "pass") {
		return fmt.Sprintf("NOT RUN: %s (missing from go test -json output)", name)
	}
	return ""
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--fingerprint" {
		fingerprint, err := runtimeFingerprint(".")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Println(fingerprint)
		return
	}
	if len(os.Args) != 2 && len(os.Args) != 7 {
		fmt.Fprintf(os.Stderr, "usage: %s GO-TEST-JSON [REPORT.json GO-EXIT PACKAGE RUN-ID STARTED-AT] | --fingerprint\n", os.Args[0])
		os.Exit(2)
	}
	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot read go test JSON: %v\n", err)
		os.Exit(2)
	}
	statuses, packages, passed, diagnostics := inspectEvents(file)
	_ = file.Close()
	if len(os.Args) == 7 {
		if os.Getenv("TXHARBOR_TESTED_TREE_FINGERPRINT") == "" {
			diagnostics = append(diagnostics, "NOT RUN: runtime/test source fingerprint unavailable")
			passed = false
		}
		goExit := 0
		if _, err := fmt.Sscan(os.Args[3], &goExit); err != nil {
			fmt.Fprintf(os.Stderr, "invalid Go exit status: %v\n", err)
			os.Exit(2)
		}
		packagePath, runID, started := os.Args[4], os.Args[5], os.Args[6]
		report := buildEvidence(statuses, packages, diagnostics, passed, goExit, packagePath, runID, started)
		if err := writeEvidence(os.Args[2], report); err != nil {
			fmt.Fprintf(os.Stderr, "cannot write drill evidence: %v\n", err)
			os.Exit(2)
		}
	}
	if len(diagnostics) != 0 {
		fmt.Fprintln(os.Stderr, "Drill scenario coverage incomplete:")
		for _, diagnostic := range diagnostics {
			fmt.Fprintf(os.Stderr, "- %s\n", diagnostic)
		}
		os.Exit(1)
	}
	if !passed {
		os.Exit(1)
	}
	fmt.Println("All required drill scenarios and restore-dependent subtests passed.")
}

func buildEvidence(statuses map[string][]string, packages map[string]string, diagnostics []string, passed bool, goExit int, packagePath, runID, started string) runEvidence {
	report := runEvidence{RunID: runID, StartedAt: started, Commit: os.Getenv("TXHARBOR_COMMIT"), TreeFingerprint: os.Getenv("TXHARBOR_TESTED_TREE_FINGERPRINT"), Package: packagePath, GoTestExit: goExit, CheckerResult: "PASS", OverallResult: "PASS", OptionalSkips: []string{}}
	for _, name := range requiredTests {
		report.Required = append(report.Required, evidenceFor(name, statuses, packages))
	}
	for _, name := range requiredSubtests {
		report.Required = append(report.Required, evidenceFor(name, statuses, packages))
	}
	observedNested := make([]string, 0)
	for name := range statuses {
		if strings.Contains(name, "/") && isRequired(name) && !contains(requiredSubtests, name) {
			observedNested = append(observedNested, name)
		}
	}
	sort.Strings(observedNested)
	for _, name := range observedNested {
		report.Required = append(report.Required, evidenceFor(name, statuses, packages))
	}
	for name, actions := range statuses {
		if contains(actions, "skip") && !isRequired(name) {
			report.OptionalSkips = append(report.OptionalSkips, name)
		}
	}
	sort.Strings(report.OptionalSkips)
	if !passed || len(diagnostics) != 0 || report.TreeFingerprint == "" {
		report.CheckerResult = "FAIL"
		report.CheckerExit = 1
	}
	if goExit != 0 || !passed || len(diagnostics) != 0 || report.TreeFingerprint == "" {
		report.OverallResult = "FAIL"
	}
	return report
}

func evidenceFor(name string, statuses map[string][]string, packages map[string]string) caseEvidence {
	result := caseEvidence{Name: name}
	actions := statuses[name]
	switch {
	case contains(actions, "fail"):
		result.Status = "FAIL"
		result.Reason = "go test reported failure"
	case contains(actions, "skip"):
		result.Status = "SKIP"
		result.Reason = "go test reported skip"
	case contains(actions, "pass"):
		result.Status = "PASS"
	default:
		result.Status = "NOTRUN"
		result.Reason = "missing from go test -json output"
	}
	result.Package = packages[name]
	if len(actions) > 0 && result.Package != "github.com/xtianxx/txharbor/internal/recovery" {
		result.Status = "FAIL"
		result.Reason = fmt.Sprintf("unexpected package %q; required github.com/xtianxx/txharbor/internal/recovery", result.Package)
	}
	return result
}

func isRequired(name string) bool {
	for _, n := range requiredTests {
		if name == n || strings.HasPrefix(name, n+"/") {
			return true
		}
	}
	for _, n := range requiredSubtests {
		if name == n || strings.HasPrefix(name, n+"/") {
			return true
		}
	}
	return false
}

func writeEvidence(path string, report runEvidence) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0600)
}

// runtimeFingerprint hashes only runtime/test inputs, independent of git and docs.
func runtimeFingerprint(root string) (string, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	var files []string
	excluded := map[string]bool{".git": true, ".slim": true, "docs": true, "specs": true, ".evidence": true, "evidence": true, "artifacts": true, "dist": true, "build": true}
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if rel != "." && excluded[entry.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if isRuntimeInput(rel) && entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("cannot fingerprint symlink runtime/test input %s", rel)
		}
		if isRuntimeInput(rel) {
			files = append(files, rel)
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("walking runtime/test inputs: %w", err)
	}
	sort.Strings(files)
	if len(files) == 0 {
		return "", fmt.Errorf("no runtime/test inputs found under %s", root)
	}
	h := sha256.New()
	for _, name := range files {
		path := filepath.Join(root, name)
		info, err := os.Stat(path)
		if err != nil {
			return "", fmt.Errorf("stat runtime/test input %s: %w", name, err)
		}
		if info.Mode().Perm()&0444 == 0 {
			return "", fmt.Errorf("runtime/test input is unreadable: %s", name)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read runtime/test input %s: %w", name, err)
		}
		fmt.Fprintf(h, "%s\x00%o\x00", filepath.ToSlash(name), info.Mode().Perm())
		_, _ = h.Write(body)
		_, _ = h.Write([]byte{0})
	}
	return fmt.Sprintf("sha256:%x", h.Sum(nil)), nil
}

func isRuntimeInput(path string) bool {
	ext := filepath.Ext(path)
	if ext == ".go" || ext == ".sql" {
		return true
	}
	if filepath.Base(path) == "go.mod" || filepath.Base(path) == "go.sum" || filepath.Base(path) == "Makefile" {
		return true
	}
	if strings.HasPrefix(filepath.ToSlash(path), "testdata/") {
		return true
	}
	if strings.HasPrefix(filepath.ToSlash(path), "scripts/") && ext == ".sh" {
		return true
	}
	base := filepath.Base(path)
	if strings.HasPrefix(base, "docker-compose") && (ext == ".yml" || ext == ".yaml") {
		return true
	}
	return filepath.ToSlash(path) == ".github/workflows/drill.yml"
}
