//go:build linux && drill

// borrowed-identity-contract_linux_test.go is the IP01-IP04 contract group for
// the identity prefix: verification is error-returning with shared permanent
// invalidation on any failed fact, contexts are always component-bounded,
// prefix SQL is serialized through the shared per-prefix admin mutex on
// independent owned-fixture connections, and the strict required-backend
// namespace census refuses UNKNOWN on any non-ENOENT/malformed/absent
// metadata. IP04a extends the window to end-of-window rechecks against the
// retained immutable tokens; negatives use the error-returning capture/inspect
// routine, never the outer t.Fatal wrappers.
package recovery_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func installBorrowedIdentityPublishHook(t *testing.T, hook func(borrowedIdentityStage)) {
	t.Helper()
	borrowedIdentityStageHook.Store(&hook)
	t.Cleanup(func() { borrowedIdentityStageHook.Store(nil) })
}

func borrowedIdentityWaitStage(t *testing.T, entered <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatalf("borrowed identity prefix never reached %s", name)
	}
}

func borrowedIdentityRequireStage(t *testing.T, err error, want ...borrowedIdentityReason) borrowedIdentityReason {
	t.Helper()
	if err == nil {
		t.Fatal("verification unexpectedly succeeded")
	}
	stage, ok := borrowedIdentityRefusalStage(err)
	if !ok {
		t.Fatalf("refusal is not a typed verification predicate: %v", err)
	}
	for _, candidate := range want {
		if stage == candidate {
			return stage
		}
	}
	t.Fatalf("refusal stage %s is not one of %v", stage, want)
	return ""
}

// TestBorrowedIdentityContractStrictHelperUnavailableInvalidates covers the
// required-backend/helper absence path: a real disposable fixture metadata
// failure (the private strict helper binary removed from the owned container)
// must make verification return an error and permanently invalidate the shared
// state observed by copies, with no positive result and no weak skip.
func TestBorrowedIdentityContractStrictHelperUnavailableInvalidates(t *testing.T) {
	ctx := context.Background()
	f := newBorrowedIdentityFixture(t)
	prefix, err := f.capture(ctx)
	if err != nil {
		t.Fatalf("positive identity capture: %v", err)
	}
	if invalid, reason := prefix.Invalid(); invalid {
		t.Fatalf("fresh prefix is invalid: %s", reason)
	}
	if _, err := dockerExec(ctx, f.fixture.containerID, "rm", "-f", f.strictHelperPath); err != nil {
		t.Fatalf("remove strict helper from the owned fixture: %v", err)
	}
	if err := prefix.inspect(ctx); err == nil {
		t.Fatal("verification succeeded after the strict helper was removed")
	}
	copied := *prefix
	if invalid, reason := copied.Invalid(); !invalid || reason == "" {
		t.Fatalf("strict helper failure is not shared permanent invalidation: invalid=%t reason=%q", invalid, reason)
	}
	if err := prefix.inspect(ctx); err == nil {
		t.Fatal("permanently invalid prefix rechecked successfully")
	}
}

// TestBorrowedIdentityContractMalformedStrictOutputIsUnknown proves malformed
// or otherwise unclassifiable producer output is UNKNOWN, never a positive
// identity: the private helper is replaced by a deterministic malformed
// producer inside the owned container.
func TestBorrowedIdentityContractMalformedStrictOutputIsUnknown(t *testing.T) {
	ctx := context.Background()
	f := newBorrowedIdentityFixture(t)
	prefix, err := f.capture(ctx)
	if err != nil {
		t.Fatalf("positive identity capture: %v", err)
	}
	malformed := filepath.Join(t.TempDir(), "malformed-strict-client")
	if err := os.WriteFile(malformed, []byte("#!/bin/sh\nprintf 'not-a-census'\n"), 0o700); err != nil {
		t.Fatalf("write malformed strict producer: %v", err)
	}
	if err := f.fixture.container.CopyFileToContainer(ctx, malformed, f.strictHelperPath, 0o700); err != nil {
		t.Fatalf("copy malformed strict producer into the owned fixture: %v", err)
	}
	if err := prefix.inspect(ctx); err == nil {
		t.Fatal("malformed strict census was accepted as a positive identity")
	}
	copied := *prefix
	if invalid, reason := copied.Invalid(); !invalid || reason == "" {
		t.Fatalf("malformed strict census is not shared permanent invalidation: invalid=%t reason=%q", invalid, reason)
	}
}

// TestBorrowedIdentityWindowInvalidReportPathInvalidates proves the IP04b
// consumer at the actual verification path: a structurally valid JSON report
// claiming ok/complete while its FD accounting carries an unknown entry must
// be refused with the fixed predicate and permanently invalidate the shared
// state (never a weak skip or a positive identity).
func TestBorrowedIdentityWindowInvalidReportPathInvalidates(t *testing.T) {
	ctx := context.Background()
	f := newBorrowedIdentityFixture(t)
	prefix, err := f.capture(ctx)
	if err != nil {
		t.Fatalf("positive identity capture: %v", err)
	}
	script := filepath.Join(t.TempDir(), "invalid-report-strict-client")
	body := `#!/bin/sh
printf '{"ok":true,"complete":true,"errors":[],"postmaster":{"pid":%s,"start_before":"%s","start_after":"%s","state":"S","ppid":1},"backend":{"pid":%s,"start_before":"7","start_after":"7","state":"S","ppid":%s},"fd_total":2,"fd_readable":1,"fd_enoent":0,"fd_unknown":1,"sockets":[],"required_inode":"","required_inode_present":false}' "$2" "$3" "$3" "$4" "$2"
`
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatalf("write invalid strict report producer: %v", err)
	}
	if err := f.fixture.container.CopyFileToContainer(ctx, script, f.strictHelperPath, 0o700); err != nil {
		t.Fatalf("copy invalid strict report producer into the owned fixture: %v", err)
	}
	if err := prefix.inspect(ctx); err == nil {
		t.Fatal("inconsistent strict report was accepted as a positive identity")
	} else {
		borrowedIdentityRequireStage(t, err, reasonStrictReportFDUnk)
	}
	copied := *prefix
	if invalid, reason := copied.Invalid(); !invalid || reason == "" {
		t.Fatalf("invalid report refusal is not shared permanent invalidation: invalid=%t reason=%q", invalid, reason)
	}
}

// TestBorrowedIdentityWindowStrictReportInvariantsRejected is the pure
// consumer-invariant proof over injected valid JSON documents: wrong PIDs,
// negative counters, broken FD accounting, nonzero unknown entries, changed
// identities, mismatched required inodes and malformed bytes are all refused
// with the fixed predicate enum. The data is a negative verification seam, not
// an authorization factory.
func TestBorrowedIdentityWindowStrictReportInvariantsRejected(t *testing.T) {
	baseline := borrowedIdentityStrictReportJSON(nil)
	census, err := borrowedIdentityParseStrictReport(baseline)
	if err != nil {
		t.Fatalf("baseline strict report did not parse: %v", err)
	}
	if err := borrowedIdentityValidateStrictReport(census, 5, "100", 9, ""); err != nil {
		t.Fatalf("baseline strict report refused: %v", err)
	}
	backend := func(pid int, startBefore, startAfter, state string, ppid int) map[string]any {
		return map[string]any{"pid": pid, "start_before": startBefore, "start_after": startAfter, "state": state, "ppid": ppid}
	}
	cases := []struct {
		name     string
		raw      []byte
		required string
		want     borrowedIdentityReason
	}{
		{"malformed-bytes", []byte("not-a-census"), "", reasonStrictMalformed},
		{"incomplete", borrowedIdentityStrictReportJSON(map[string]any{"complete": false}), "", reasonStrictIncomplete},
		{"wrong-backend-pid", borrowedIdentityStrictReportJSON(map[string]any{"backend": backend(10, "200", "200", "S", 5)}), "", reasonStrictReportPID},
		{"negative-counter", borrowedIdentityStrictReportJSON(map[string]any{"fd_total": -1, "fd_readable": -1}), "", reasonStrictReportFD},
		{"broken-accounting", borrowedIdentityStrictReportJSON(map[string]any{"fd_total": 1, "fd_readable": 0, "fd_enoent": 0, "fd_unknown": 0}), "", reasonStrictReportFD},
		{"nonzero-unknown", borrowedIdentityStrictReportJSON(map[string]any{"fd_total": 3, "fd_readable": 2, "fd_unknown": 1}), "", reasonStrictReportFDUnk},
		{"start-changed", borrowedIdentityStrictReportJSON(map[string]any{"backend": backend(9, "200", "201", "S", 5)}), "", reasonStrictReportBackend},
		{"zombie", borrowedIdentityStrictReportJSON(map[string]any{"backend": backend(9, "200", "200", "Z", 5)}), "", reasonStrictReportState},
		{"wrong-ppid", borrowedIdentityStrictReportJSON(map[string]any{"backend": backend(9, "200", "200", "S", 6)}), "", reasonStrictReportPPID},
		{"postmaster-start", borrowedIdentityStrictReportJSON(map[string]any{"postmaster": map[string]any{"pid": 5, "start_before": "101", "start_after": "101", "state": "S", "ppid": 1}}), "", reasonStrictReportPost},
		{"required-inode-absent", borrowedIdentityStrictReportJSON(nil), "777", reasonStrictRequiredInode},
		{"required-inode-mismatch", borrowedIdentityStrictReportJSON(map[string]any{"required_inode": "888", "required_inode_present": true}), "888", reasonStrictRequiredInode},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			parsed, parseErr := borrowedIdentityParseStrictReport(testCase.raw)
			if parseErr != nil {
				borrowedIdentityRequireStage(t, parseErr, testCase.want)
				return
			}
			validationErr := borrowedIdentityValidateStrictReport(parsed, 5, "100", 9, testCase.required)
			borrowedIdentityRequireStage(t, validationErr, testCase.want)
		})
	}
}

// borrowedIdentityStrictReportJSON builds one structurally valid census
// document with optional overrides.
func borrowedIdentityStrictReportJSON(overrides map[string]any) []byte {
	document := map[string]any{
		"ok": true, "complete": true, "errors": []string{},
		"postmaster": map[string]any{"pid": 5, "start_before": "100", "start_after": "100", "state": "S", "ppid": 1},
		"backend":    map[string]any{"pid": 9, "start_before": "200", "start_after": "200", "state": "S", "ppid": 5},
		"fd_total":   2, "fd_readable": 2, "fd_enoent": 0, "fd_unknown": 0,
		"sockets": []map[string]any{
			{"inode": "777", "tcp": true, "local": "10.0.0.1:1000", "remote": "10.0.0.2:2000", "state": "01"},
		},
		"required_inode": "", "required_inode_present": false,
	}
	for key, value := range overrides {
		document[key] = value
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		panic(err)
	}
	return encoded
}

// TestBorrowedIdentityWindowEndCatalogMutationRefused proves IP04a's real
// end-of-window catalog check: after the initial checks and the final anchor
// recheck, a real catalog change to the fixture-owned declared target database
// (rename + recreate with the same name, hence a new OID) must be refused with
// the end catalog predicate and permanently invalidate the shared state.
func TestBorrowedIdentityWindowEndCatalogMutationRefused(t *testing.T) {
	ctx := context.Background()
	f := newBorrowedIdentityFixture(t)
	prefix, err := f.capture(ctx)
	if err != nil {
		t.Fatalf("positive identity capture: %v", err)
	}
	declaredDatabase := databaseOf(t, f.directTargetDSN)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	installBorrowedIdentityPublishHook(t, func(stage borrowedIdentityStage) {
		if stage != borrowedIdentityStageEndWindow {
			return
		}
		once.Do(func() { close(entered) })
		<-release
	})
	result := make(chan error, 1)
	go func() { result <- prefix.inspect(ctx) }()
	borrowedIdentityWaitStage(t, entered, "the end-of-window stage")
	moved := declaredDatabase + "_moved"
	if _, err := f.fixture.admin.Exec(ctx, `ALTER DATABASE `+pgx.Identifier{declaredDatabase}.Sanitize()+` RENAME TO `+pgx.Identifier{moved}.Sanitize()); err != nil {
		t.Fatalf("rename declared target database: %v", err)
	}
	if _, err := f.fixture.admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{declaredDatabase}.Sanitize()); err != nil {
		t.Fatalf("recreate declared target database: %v", err)
	}
	close(release)
	select {
	case inspectErr := <-result:
		borrowedIdentityRequireStage(t, inspectErr, reasonEndSQLTargetCatalog)
	case <-time.After(20 * time.Second):
		t.Fatal("end-window verification did not complete")
	}
	if invalid, reason := prefix.Invalid(); !invalid || reason == "" {
		t.Fatalf("end catalog mutation is not shared permanent invalidation: invalid=%t reason=%q", invalid, reason)
	}
}

// TestBorrowedIdentityWindowEndInjectedTokenChangeRefused injects a
// structurally valid but changed end-of-window census through the deterministic
// negative report seam: a changed retained backend OS start and a changed
// server socket inode must each be refused by the retained-token comparison,
// never accepted as positive.
func TestBorrowedIdentityWindowEndInjectedTokenChangeRefused(t *testing.T) {
	ctx := context.Background()

	// Changed retained backend OS start.
	first := newBorrowedIdentityFixture(t)
	prefixStart, err := first.capture(ctx)
	if err != nil {
		t.Fatalf("positive identity capture: %v", err)
	}
	retainedStart := prefixStart.backendOSStart
	var startHook func(*borrowedIdentityStrictCensus, borrowedIdentityStrictPhase) = func(census *borrowedIdentityStrictCensus, phase borrowedIdentityStrictPhase) {
		if phase != borrowedIdentityStrictPhaseEnd {
			return
		}
		census.Backend.StartBefore = retainedStart + "9"
		census.Backend.StartAfter = retainedStart + "9"
	}
	borrowedIdentityStrictReportHook.Store(&startHook)
	t.Cleanup(func() { borrowedIdentityStrictReportHook.Store(nil) })
	if err := prefixStart.inspect(ctx); err == nil {
		t.Fatal("changed retained backend start was accepted")
	} else {
		borrowedIdentityRequireStage(t, err, reasonEndOSBackendStart)
	}
	if invalid, reason := prefixStart.Invalid(); !invalid || reason == "" {
		t.Fatalf("injected start change is not shared permanent invalidation: invalid=%t reason=%q", invalid, reason)
	}
	borrowedIdentityStrictReportHook.Store(nil)

	// Changed retained server socket inode.
	second := newBorrowedIdentityFixture(t)
	prefixInode, err := second.capture(ctx)
	if err != nil {
		t.Fatalf("positive identity capture: %v", err)
	}
	retainedInode := prefixInode.serverSocketInode
	var inodeHook func(*borrowedIdentityStrictCensus, borrowedIdentityStrictPhase) = func(census *borrowedIdentityStrictCensus, phase borrowedIdentityStrictPhase) {
		if phase != borrowedIdentityStrictPhaseEnd {
			return
		}
		for index := range census.Sockets {
			if census.Sockets[index].TCP && census.Sockets[index].Inode == retainedInode {
				census.Sockets[index].Inode = retainedInode + "-changed"
			}
		}
		// Keep the required inode structurally present so the token comparison,
		// not the presence invariant, is the refusing predicate.
		census.Sockets = append(census.Sockets, borrowedIdentityStrictEndpoint{Inode: retainedInode})
	}
	borrowedIdentityStrictReportHook.Store(&inodeHook)
	if err := prefixInode.inspect(ctx); err == nil {
		t.Fatal("changed retained server socket inode was accepted")
	} else {
		borrowedIdentityRequireStage(t, err, reasonEndOSServerInode)
	}
	if invalid, reason := prefixInode.Invalid(); !invalid || reason == "" {
		t.Fatalf("injected inode change is not shared permanent invalidation: invalid=%t reason=%q", invalid, reason)
	}
}

// TestBorrowedIdentityContractOwnCancelBeforePublicationInvalidates proves the
// IP02 publication contract: a verification paused after all facts and the
// final anchor recheck must fail closed with the fixed safe reason when its
// OWN parent context is canceled before the shared-state decision, recording
// permanent invalidation shared with copies.
func TestBorrowedIdentityContractOwnCancelBeforePublicationInvalidates(t *testing.T) {
	ctx := context.Background()
	f := newBorrowedIdentityFixture(t)
	prefix, err := f.capture(ctx)
	if err != nil {
		t.Fatalf("positive identity capture: %v", err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	installBorrowedIdentityPublishHook(t, func(stage borrowedIdentityStage) {
		if stage != borrowedIdentityStageInspectPublish {
			return
		}
		once.Do(func() { close(entered) })
		<-release
	})
	parent, cancelParent := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() { result <- prefix.inspect(parent) }()
	borrowedIdentityWaitStage(t, entered, "the pre-publication stage of a healthy verification")
	cancelParent()
	close(release)
	select {
	case inspectErr := <-result:
		borrowedIdentityRequireStage(t, inspectErr, reasonContextEnded)
	case <-time.After(20 * time.Second):
		t.Fatal("verification did not complete after its own parent cancellation")
	}
	copied := *prefix
	if invalid, reason := copied.Invalid(); !invalid || reason != borrowedIdentityContextEndedReason {
		t.Fatalf("own-parent cancellation is not shared permanent invalidation: invalid=%t reason=%q", invalid, reason)
	}
	if err := prefix.inspect(ctx); err == nil {
		t.Fatal("permanently invalid prefix rechecked successfully")
	}
}

// TestBorrowedIdentityContractCopyInvalidationRacesPublication proves the
// publication cannot grant success after a copy has invalidated the shared
// state (the CA01-style mistake): a context fault through a copied handle
// permanently invalidates the shared state while a healthy verification is
// paused before publication, which must then fail instead of publishing.
func TestBorrowedIdentityContractCopyInvalidationRacesPublication(t *testing.T) {
	ctx := context.Background()
	f := newBorrowedIdentityFixture(t)
	prefix, err := f.capture(ctx)
	if err != nil {
		t.Fatalf("positive identity capture: %v", err)
	}
	copied := *prefix
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	installBorrowedIdentityPublishHook(t, func(stage borrowedIdentityStage) {
		if stage != borrowedIdentityStageInspectPublish {
			return
		}
		once.Do(func() { close(entered) })
		<-release
	})
	result := make(chan error, 1)
	go func() { result <- prefix.inspect(ctx) }()
	borrowedIdentityWaitStage(t, entered, "the pre-publication stage of a healthy verification")

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := copied.inspect(canceled); err == nil {
		t.Fatal("context fault through the copied handle did not fail verification")
	}
	if invalid, reason := copied.Invalid(); !invalid || reason == "" {
		t.Fatalf("copy context fault did not share permanent invalidation: invalid=%t reason=%q", invalid, reason)
	}
	close(release)
	select {
	case inspectErr := <-result:
		if inspectErr == nil {
			t.Fatal("paused healthy verification published success after a copy invalidated the shared state")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("paused verification did not complete after the copy invalidation")
	}
	if invalid, reason := prefix.Invalid(); !invalid || reason == "" {
		t.Fatalf("copy invalidation is not shared with the original prefix: invalid=%t reason=%q", invalid, reason)
	}
}

// TestBorrowedIdentityContractConcurrentCopiesSerializeSQL runs several copies
// of one prefix concurrently: the shared per-prefix admin mutex serializes the
// SQL phases (independent owned-fixture connections, no shared fixture admin
// connection), every verification succeeds, and the shared state stays valid.
// Run with -race to prove the shared serialization has no data race.
func TestBorrowedIdentityContractConcurrentCopiesSerializeSQL(t *testing.T) {
	ctx := context.Background()
	f := newBorrowedIdentityFixture(t)
	prefix, err := f.capture(ctx)
	if err != nil {
		t.Fatalf("positive identity capture: %v", err)
	}
	const workers = 4
	bounded, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for index := 0; index < workers; index++ {
		wg.Add(1)
		go func(copyPrefix borrowedIdentityPrefix) {
			defer wg.Done()
			results <- copyPrefix.inspect(bounded)
		}(*prefix)
	}
	wg.Wait()
	close(results)
	failures := 0
	for inspectErr := range results {
		if inspectErr != nil {
			failures++
			t.Logf("concurrent copy verification failed: %v", inspectErr)
		}
	}
	if failures != 0 {
		t.Fatalf("concurrent copy verifications failed: %d/%d", failures, workers)
	}
	if invalid, reason := prefix.Invalid(); invalid {
		t.Fatalf("concurrent copy verifications invalidated the shared state: %s", reason)
	}
}
