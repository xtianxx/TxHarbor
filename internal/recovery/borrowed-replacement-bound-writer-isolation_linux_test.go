//go:build linux && drill

// borrowed-replacement-bound-writer-isolation_linux_test.go is the bounded
// bound-writer-path isolation fence lane (NON-AUTHORIZING). It composes ONLY
// existing, unchanged primitives: the genuine bound baseline with the
// single-consumed receipt/entry, the same-owner committed P1 rotation and the
// unchanged five-stage replacement capture/comparator, the existing bound
// dirty-guard refusal with its closed output contract, the unchanged
// owner-window/owner-tail readiness machinery (whose single-use NON-AUTHORIZING
// readiness witness is consumed ONCE as lineage facts, never as authority), the
// strict registered-P1 retirement, and the existing fixture HBA/reload/
// effective-rule and standalone-client authcheck primitives. On that disposable
// fixture it establishes an ACTUAL writer-ingress fence: writer-role-specific
// reject rules for EVERY enumerated connection scope (local socket, loopback
// IPv4/IPv6, container-address TCP), a successful reload, independent
// pg_hba_file_rules verification, and external rejection of fresh genuine P1
// connections on every supported transport at the intended server admission
// stage (SQLSTATE 28000, never timeout/bad-credential/routing). P0 stays
// refused, every retained writer incarnation is strictly gone, the original
// advisory owner, the authentic InstanceID/key/fingerprint and the actual
// replacement catalog stay valid, and the protected observer/control routes
// stay usable. Only then does the lane publish at most a SINGLE-USE, lane-local,
// fixture-scoped isolation fact set bound to the actual fence, the enumerated
// route coverage and the consumed lineage; it is never a reusable admission
// handle. The fence is disposed by restoring the original HBA, and disposal
// never authorizes recovery or rehabilitates consumed/lost facts. There is no
// guard clean transition, no ResolveTargetGuardClean invocation, no fabricated
// isolation/rebuild evidence, no direct SQL authority, no second instance, no
// owner reacquisition, no native replacement launch, no successful restore
// probe, no atomic acceptance, no manifest, no capability release, no
// downstream and no Gate1 authority; rebuildTargetWithWitness is unmodified and
// intentionally refusing. This is NOT continuous exclusion and NOT universal
// external isolation: it is one fixture-scoped writer-role fence with explicit
// scope/route coverage. Secrets, verifiers, DSNs and archive contents are never
// logged.
package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Bound writer-isolation refusal stages: every control captures and asserts the
// ACTUAL real stage instead of any non-nil error.
var (
	errWriterIsolationLineage     = errors.New("bound writer isolation: the consumed lineage facts refused")
	errWriterIsolationPrefix      = errors.New("bound writer isolation: the shared replacement prefix refused")
	errWriterIsolationFenceAbsent = errors.New("bound writer isolation: no effective writer-reject fence")
	errWriterIsolationCoverage    = errors.New("bound writer isolation: writer-route coverage is incomplete")
	errWriterIsolationReload      = errors.New("bound writer isolation: the HBA reload/effective-rule verification refused")
	errWriterIsolationRoute       = errors.New("bound writer isolation: a supported writer route was not rejected at the intended admission stage")
	errWriterIsolationCensus      = errors.New("bound writer isolation: the retained-writer census refused")
	errWriterIsolationOwner       = errors.New("bound writer isolation: the original owner/anchor refused")
	errWriterIsolationReplacement = errors.New("bound writer isolation: the replacement catalog revalidation refused")
	errWriterIsolationProof       = errors.New("bound writer isolation: the fence was not effective through the bounded proof interval")
	errWriterIsolationCancelled   = errors.New("bound writer isolation: the lane context ended")
	errWriterIsolationPublished   = errors.New("bound writer isolation: publication refused")
)

// borrowedBoundWriterIsolationIssuance is the copy-shared one-shot issuance
// latch bound to the consumed lineage: the source proofs can issue at most ONE
// fact set, so a fresh destination object can never replay an already-issued
// source. Copies of the lineage share this pointer.
type borrowedBoundWriterIsolationIssuance struct {
	mu     sync.Mutex
	issued bool
	nonce  int64
}

func (l *borrowedBoundWriterIsolationIssuance) issuedNow() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.issued
}

// claimAndPublish atomically claims the one-shot issuance only when the
// publication actually succeeds; a refused publication leaves the source
// unclaimed so an honest retry remains possible.
func (l *borrowedBoundWriterIsolationIssuance) claimAndPublish(publish func() error) error {
	if l == nil {
		return errors.New("bound writer isolation lineage has no issuance latch")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.issued {
		return errors.New("bound writer isolation lineage already issued its one fact set")
	}
	if err := publish(); err != nil {
		return err
	}
	l.issued = true
	l.nonce++
	return nil
}

// borrowedBoundWriterIsolationLineage is the consumed lineage fact set: either
// the genuine single-use readiness witness (positive chain) or the equivalent
// facts over a strictly retired P1 incarnation (controls). The issuance latch
// is copy-shared with the lineage, so a fresh destination can never re-issue
// the same source.
type borrowedBoundWriterIsolationLineage struct {
	consumed        bool
	readinessBacked bool
	instanceID      string
	targetKey       string
	fingerprint     string
	replacementOID  uint32
	ownerPID        int
	issuance        *borrowedBoundWriterIsolationIssuance
}

// borrowedBoundWriterIsolationFacts is the plain, NON-AUTHORIZING observed fact
// set: no handle, no capability, no admission.
type borrowedBoundWriterIsolationFacts struct {
	InstanceID      string
	TargetKey       string
	RoleFingerprint string
	ReplacementOID  uint32
	OwnerPID        int
	ReadinessBacked bool
	CoveredScopes   []string
	ExercisedRoutes []string
	ProofRounds     int
	Nonce           int64
}

// borrowedBoundWriterIsolationState is the copy-shared single-use state.
type borrowedBoundWriterIsolationState struct {
	mu        sync.Mutex
	published bool
	consumed  bool
	nonce     int64
	facts     borrowedBoundWriterIsolationFacts
}

// borrowedBoundWriterIsolation is the lane-local NON-AUTHORIZING isolation fact
// set. Copies share the same state pointer; consume is one-shot.
type borrowedBoundWriterIsolation struct {
	state *borrowedBoundWriterIsolationState
}

func newBorrowedBoundWriterIsolation() *borrowedBoundWriterIsolation {
	return &borrowedBoundWriterIsolation{state: &borrowedBoundWriterIsolationState{}}
}

func (r *borrowedBoundWriterIsolation) publish(facts borrowedBoundWriterIsolationFacts) error {
	if r == nil || r.state == nil {
		return errors.New("bound writer isolation fact set is absent")
	}
	if facts.InstanceID == "" || facts.TargetKey == "" || facts.RoleFingerprint == "" ||
		facts.ReplacementOID == 0 || facts.OwnerPID <= 0 || len(facts.CoveredScopes) == 0 ||
		len(facts.ExercisedRoutes) == 0 || facts.ProofRounds <= 0 {
		return errors.New("bound writer isolation publication requires the complete observed fence/coverage/lineage facts")
	}
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	if r.state.published {
		return errors.New("bound writer isolation fact set was already published")
	}
	if r.state.consumed {
		return errors.New("bound writer isolation fact set was already consumed")
	}
	r.state.nonce++
	facts.Nonce = r.state.nonce
	r.state.facts = facts
	r.state.published = true
	return nil
}

func (r *borrowedBoundWriterIsolation) consume() (borrowedBoundWriterIsolationFacts, bool) {
	if r == nil || r.state == nil {
		return borrowedBoundWriterIsolationFacts{}, false
	}
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	if !r.state.published || r.state.consumed {
		return borrowedBoundWriterIsolationFacts{}, false
	}
	r.state.consumed = true
	return r.state.facts, true
}

func (r *borrowedBoundWriterIsolation) publishedNow() bool {
	if r == nil || r.state == nil {
		return false
	}
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	return r.state.published
}

// borrowedBoundWriterIsolationRule is one effective pg_hba_file_rules row.
type borrowedBoundWriterIsolationRule struct {
	line     int
	kind     string
	database string
	user     string
	address  string
	method   string
	errText  string
}

// borrowedBoundWriterIsolationScope is one enumerated connection scope that
// must be fenced for the writer role.
type borrowedBoundWriterIsolationScope struct {
	kind    string
	address string
	line    int
}

func borrowedBoundWriterIsolationReadRules(ctx context.Context, admin *pgx.Conn) ([]borrowedBoundWriterIsolationRule, error) {
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelRead()
	rows, err := admin.Query(readCtx, `
SELECT line_number, coalesce(type, ''), coalesce(array_to_string(database, ','), ''),
       coalesce(array_to_string(user_name, ','), ''), coalesce(address, ''), coalesce(auth_method, ''), coalesce(error, '')
FROM pg_hba_file_rules ORDER BY line_number`)
	if err != nil {
		return nil, fmt.Errorf("read effective pg_hba_file_rules: %w", err)
	}
	defer rows.Close()
	var rules []borrowedBoundWriterIsolationRule
	for rows.Next() {
		var rule borrowedBoundWriterIsolationRule
		if err := rows.Scan(&rule.line, &rule.kind, &rule.database, &rule.user, &rule.address, &rule.method, &rule.errText); err != nil {
			return nil, fmt.Errorf("scan effective pg_hba_file_rules: %w", err)
		}
		rules = append(rules, rule)
	}
	return rules, rows.Err()
}

func borrowedBoundWriterIsolationScopes(rules []borrowedBoundWriterIsolationRule) []borrowedBoundWriterIsolationScope {
	var scopes []borrowedBoundWriterIsolationScope
	for _, rule := range rules {
		if rule.errText != "" || rule.method != "scram-sha-256" || rule.user != "all" || rule.database != "all" {
			continue
		}
		scopes = append(scopes, borrowedBoundWriterIsolationScope{kind: rule.kind, address: rule.address, line: rule.line})
	}
	return scopes
}

func borrowedBoundWriterIsolationScopeName(scope borrowedBoundWriterIsolationScope) string {
	if scope.kind == "local" {
		return "local"
	}
	return scope.address
}

// borrowedBoundWriterIsolationFenceAddress renders the exact HBA address token:
// the effective-rule view reports addresses WITHOUT a CIDR mask, and a bare
// IPv4/IPv6 address would be parsed as a hostname, so the full-length mask is
// restored explicitly (the all-addresses scope stays "all").
func borrowedBoundWriterIsolationFenceAddress(scope borrowedBoundWriterIsolationScope) string {
	if scope.address == "all" || strings.Contains(scope.address, "/") {
		return scope.address
	}
	if strings.Contains(scope.address, ":") {
		return scope.address + "/128"
	}
	return scope.address + "/32"
}

func borrowedBoundWriterIsolationFencedContent(original string, scopes []borrowedBoundWriterIsolationScope, writer string) string {
	var builder strings.Builder
	for _, scope := range scopes {
		if scope.kind == "local" {
			fmt.Fprintf(&builder, "local all %s reject\n", writer)
		} else {
			fmt.Fprintf(&builder, "host all %s %s reject\n", writer, borrowedBoundWriterIsolationFenceAddress(scope))
		}
	}
	builder.WriteString(original)
	return builder.String()
}

func borrowedBoundWriterIsolationAddressCovers(ruleAddress, scopeAddress string) bool {
	if ruleAddress == scopeAddress {
		return true
	}
	return ruleAddress == "all"
}

// borrowedBoundWriterIsolationFenceStateCheck verifies the effective rules:
// every enumerated scope has a PRECEDING writer-reject rule, no rule carries a
// parse error and no unexpected writer-reject rule exists.
func borrowedBoundWriterIsolationFenceStateCheck(rules []borrowedBoundWriterIsolationRule, scopes []borrowedBoundWriterIsolationScope, writer string) error {
	for _, rule := range rules {
		if rule.errText != "" {
			return fmt.Errorf("%w: pg_hba_file_rules reports an error on line %d (%s)", errWriterIsolationReload, rule.line, rule.errText)
		}
	}
	rejectCount := 0
	for _, rule := range rules {
		if rule.method == "reject" && rule.user == writer {
			rejectCount++
		}
	}
	if rejectCount != len(scopes) {
		return fmt.Errorf("%w: %d writer-reject rules for %d enumerated scopes", errWriterIsolationCoverage, rejectCount, len(scopes))
	}
	for _, scope := range scopes {
		covered := false
		originalLine := -1
		for _, rule := range rules {
			if rule.method == "scram-sha-256" && rule.user == "all" && rule.kind == scope.kind && borrowedBoundWriterIsolationAddressCovers(rule.address, scope.address) {
				if originalLine < 0 || rule.line < originalLine {
					originalLine = rule.line
				}
			}
		}
		for _, rule := range rules {
			if rule.method != "reject" || rule.user != writer || rule.kind != scope.kind {
				continue
			}
			if !borrowedBoundWriterIsolationAddressCovers(rule.address, scope.address) {
				continue
			}
			if originalLine > 0 && rule.line < originalLine {
				covered = true
				break
			}
		}
		if !covered {
			return fmt.Errorf("%w: scope %s/%q has no preceding writer-reject rule", errWriterIsolationCoverage, scope.kind, scope.address)
		}
	}
	return nil
}

func borrowedBoundWriterIsolationHBAPath(ctx context.Context, admin *pgx.Conn) (string, error) {
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelRead()
	var path string
	if err := admin.QueryRow(readCtx, `SELECT current_setting('hba_file')`).Scan(&path); err != nil || path == "" {
		return "", fmt.Errorf("read effective hba_file path refused: %w", err)
	}
	return path, nil
}

func borrowedBoundWriterIsolationReadHBA(ctx context.Context, f *borrowedAuthHandoffFixture, path string) (string, error) {
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelRead()
	out, err := dockerExec(readCtx, f.fx.containerID, "cat", path)
	if err != nil {
		return "", fmt.Errorf("read effective HBA content refused: %w", err)
	}
	return string(out), nil
}

func borrowedBoundWriterIsolationWriteHBA(ctx context.Context, t *testing.T, f *borrowedAuthHandoffFixture, path, content string) error {
	t.Helper()
	hostFile := filepath.Join(t.TempDir(), "fenced_pg_hba.conf")
	if err := os.WriteFile(hostFile, []byte(content), 0o600); err != nil {
		return fmt.Errorf("write local fenced HBA staging file refused: %w", err)
	}
	writeCtx, cancelWrite := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	defer cancelWrite()
	if err := f.fx.container.CopyFileToContainer(writeCtx, hostFile, path, 0o644); err != nil {
		return fmt.Errorf("copy fenced HBA into the disposable fixture refused: %w", err)
	}
	return nil
}

func borrowedBoundWriterIsolationReload(ctx context.Context, admin *pgx.Conn) error {
	reloadCtx, cancelReload := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelReload()
	var reloaded bool
	if err := admin.QueryRow(reloadCtx, `SELECT pg_reload_conf()`).Scan(&reloaded); err != nil || !reloaded {
		return fmt.Errorf("%w: pg_reload_conf refused (err=%v)", errWriterIsolationReload, err)
	}
	return nil
}

func borrowedBoundWriterIsolationContains(line, state, sqlstate string) bool {
	return strings.Contains(line, "state="+state) && strings.Contains(line, "sqlstate="+sqlstate)
}

func borrowedBoundWriterIsolationAwaitState(t *testing.T, ctx context.Context, f *borrowedAuthHandoffFixture, serverIP, role, credential, state, sqlstate string, bound time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(bound)
	var last string
	for {
		line := runHelperAuthCheck(t, ctx, f.fx.containerID, "serverip", serverIP, role, credential)
		last = line
		if sqlstate == "" {
			if strings.Contains(line, "state="+state) {
				return nil
			}
		} else if borrowedBoundWriterIsolationContains(line, state, sqlstate) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: last admission line %q", errWriterIsolationRoute, last)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", errWriterIsolationCancelled, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func borrowedBoundWriterIsolationCensus(ctx context.Context, conn *pgx.Conn, role string) (int, error) {
	readCtx, cancelRead := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	defer cancelRead()
	var count int
	if err := conn.QueryRow(readCtx, `SELECT count(*)::int FROM pg_stat_activity WHERE usename=$1`, role).Scan(&count); err != nil {
		return 0, fmt.Errorf("%w: %v", errWriterIsolationCensus, err)
	}
	return count, nil
}

func borrowedBoundWriterIsolationAwaitCensusZero(t *testing.T, ctx context.Context, conn *pgx.Conn, role string, bound time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(bound)
	var last int
	for {
		count, err := borrowedBoundWriterIsolationCensus(ctx, conn, role)
		if err != nil {
			return err
		}
		last = count
		if count == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: %d writer sessions retained", errWriterIsolationCensus, last)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", errWriterIsolationCancelled, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func borrowedBoundWriterIsolationObserverOK(ctx context.Context, f *borrowedAuthHandoffFixture) error {
	checkCtx, cancelCheck := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	defer cancelCheck()
	conn, err := pgx.Connect(checkCtx, f.observerTargetDSN)
	if err != nil {
		return fmt.Errorf("protected observer route refused: %w", err)
	}
	defer func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = conn.Close(closeCtx)
		cancelClose()
	}()
	var one int
	if err := conn.QueryRow(checkCtx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		return fmt.Errorf("protected observer route query refused: %v", err)
	}
	return nil
}

func borrowedBoundWriterIsolationControlOK(ctx context.Context, f *borrowedAuthHandoffFixture) error {
	checkCtx, cancelCheck := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
	defer cancelCheck()
	conn, err := pgx.Connect(checkCtx, f.controlDSN)
	if err != nil {
		return fmt.Errorf("protected control route refused: %w", err)
	}
	defer func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = conn.Close(closeCtx)
		cancelClose()
	}()
	var one int
	if err := conn.QueryRow(checkCtx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		return fmt.Errorf("protected control route query refused: %v", err)
	}
	return nil
}

// borrowedBoundWriterIsolationFlowState is the observed result container of one
// fence flow.
type borrowedBoundWriterIsolationFlowState struct {
	hbaPath         string
	originalContent string
	fencedContent   string
	originalRules   []borrowedBoundWriterIsolationRule
	requiredScopes  []borrowedBoundWriterIsolationScope
	fenceWritten    bool
	fenceApplied    bool
	coverageOK      bool
	routeLines      map[string]string
	proofRounds     int
	proofEffective  bool
	p0PreFence      string
	p0PostFence     string
	p1PreFence      string
	observerOK      bool
	controlOK       bool
	censusPre       int
	censusPost      int
	ownerOK         bool
	catalogOK       bool
	cleanupRestored bool
	cleanupErr      error
	flowErr         error
	censusConn      *pgx.Conn
}

// borrowedBoundWriterIsolationHooks is the lane-local injection plan. skipFence
// verifies the CURRENT effective rules without writing (missing/partial fence
// controls); partialScopes applies only the first N enumerated scopes.
type borrowedBoundWriterIsolationHooks struct {
	skipFence      bool
	partialScopes  int
	atFenceApplied func(context.Context, *borrowedBoundWriterIsolationFlowState) error
	duringProof    func(context.Context, *borrowedBoundWriterIsolationFlowState) error
	beforeCensus   func(context.Context, *borrowedBoundWriterIsolationFlowState) error
}

// borrowedBoundWriterIsolationRun is the common fence flow: lineage gate, the
// pre-fence genuine-P1/P0 proof, observer/control/owner/catalog validity, the
// actual writer-reject HBA fence with reload and effective-rule coverage, the
// external rejection of every supported writer transport at the intended
// admission stage, the bounded proof interval, the final fence-state
// verification and the post-fence census/owner/catalog checks. The original HBA
// is ALWAYS restored by the deferred disposal, even after cancellation.
func borrowedBoundWriterIsolationRun(ctx context.Context, t *testing.T, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, lineage *borrowedBoundWriterIsolationLineage, hooks *borrowedBoundWriterIsolationHooks) (state *borrowedBoundWriterIsolationFlowState, retErr error) {
	t.Helper()
	if hooks == nil {
		hooks = &borrowedBoundWriterIsolationHooks{}
	}
	state = &borrowedBoundWriterIsolationFlowState{routeLines: map[string]string{}}
	defer func() {
		if retErr != nil {
			state.flowErr = retErr
		}
	}()
	f := b.fixture
	admin := f.fx.admin
	if lineage == nil || !lineage.consumed || lineage.instanceID != f.instanceID || lineage.targetKey != f.guardKey ||
		lineage.fingerprint != fresh.binding.OriginalRoleFingerprint() || lineage.replacementOID != fresh.replacementOID ||
		lineage.ownerPID != b.owner.BackendPID {
		return state, fmt.Errorf("%w: the consumed lineage does not match the bound fixture provenance", errWriterIsolationLineage)
	}
	if invalid, reason := fresh.prefix.Invalid(); invalid {
		return state, fmt.Errorf("%w: the shared replacement prefix is permanently invalidated: %s", errWriterIsolationPrefix, reason)
	}
	serverIP, err := f.fx.container.ContainerIP(ctx)
	if err != nil || serverIP == "" {
		return state, fmt.Errorf("%w: the fixture container endpoint is unknown: %v", errWriterIsolationRoute, err)
	}
	censusConn, err := pgx.Connect(ctx, f.adminDSN)
	if err != nil {
		return state, fmt.Errorf("%w: the independent census connection refused: %v", errWriterIsolationCensus, err)
	}
	state.censusConn = censusConn
	defer func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
		_ = censusConn.Close(closeCtx)
		cancelClose()
	}()
	defer func() {
		if !state.fenceWritten {
			return
		}
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancelCleanup()
		if err := borrowedBoundWriterIsolationWriteHBA(cleanupCtx, t, f, state.hbaPath, state.originalContent); err != nil {
			state.cleanupErr = err
			return
		}
		if err := borrowedBoundWriterIsolationReload(cleanupCtx, admin); err != nil {
			state.cleanupErr = err
			return
		}
		state.cleanupRestored = true
	}()

	// Pre-fence: the genuine P1 credential authenticates and P0 is refused.
	state.p1PreFence = runHelperAuthCheck(t, ctx, f.fx.containerID, "serverip", serverIP, f.writerRole, b.passwordP1)
	if !strings.Contains(state.p1PreFence, "state=AUTH_OK") {
		return state, fmt.Errorf("%w: the genuine P1 credential did not authenticate before fencing: %s", errWriterIsolationRoute, state.p1PreFence)
	}
	state.p0PreFence = runHelperAuthCheck(t, ctx, f.fx.containerID, "serverip", serverIP, f.writerRole, f.writerPassword)
	if !borrowedBoundWriterIsolationContains(state.p0PreFence, "REJECT", "28P01") {
		return state, fmt.Errorf("%w: P0 was not refused as an inactive credential before fencing: %s", errWriterIsolationRoute, state.p0PreFence)
	}
	healthCtx, cancelHealth := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr := f.lock.Health(healthCtx)
	cancelHealth()
	anchorCtx, cancelAnchor := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchorErr := b.anchor.Recheck(anchorCtx)
	cancelAnchor()
	if healthErr != nil || anchorErr != nil {
		return state, fmt.Errorf("%w: owner health=%v anchor=%v", errWriterIsolationOwner, healthErr, anchorErr)
	}
	state.ownerOK = true
	oid, oidErr := borrowedOwnerDDLTargetOID(ctx, b)
	if oidErr != nil || oid != fresh.replacementOID {
		return state, fmt.Errorf("%w: actual catalog oid=%d err=%v", errWriterIsolationReplacement, oid, oidErr)
	}
	state.catalogOK = true
	if err := borrowedBoundWriterIsolationObserverOK(ctx, f); err != nil {
		return state, err
	}
	state.observerOK = true
	if err := borrowedBoundWriterIsolationControlOK(ctx, f); err != nil {
		return state, err
	}
	state.controlOK = true
	state.censusPre, err = borrowedBoundWriterIsolationCensus(ctx, censusConn, f.writerRole)
	if err != nil {
		return state, err
	}

	// Read the actual effective HBA and enumerate every supported scope.
	state.hbaPath, err = borrowedBoundWriterIsolationHBAPath(ctx, admin)
	if err != nil {
		return state, err
	}
	state.originalContent, err = borrowedBoundWriterIsolationReadHBA(ctx, f, state.hbaPath)
	if err != nil {
		return state, err
	}
	state.originalRules, err = borrowedBoundWriterIsolationReadRules(ctx, admin)
	if err != nil {
		return state, err
	}
	state.requiredScopes = borrowedBoundWriterIsolationScopes(state.originalRules)
	if len(state.requiredScopes) == 0 {
		return state, fmt.Errorf("%w: no supported writer scopes were enumerated", errWriterIsolationCoverage)
	}

	if hooks.skipFence {
		// Verify the CURRENT rules only: a missing or partial fence must refuse.
		if err := borrowedBoundWriterIsolationFenceStateCheck(state.originalRules, state.requiredScopes, f.writerRole); err != nil {
			return state, err
		}
		state.fenceApplied = true
		state.coverageOK = true
	} else {
		scopes := state.requiredScopes
		if hooks.partialScopes > 0 && hooks.partialScopes < len(scopes) {
			scopes = scopes[:hooks.partialScopes]
		}
		state.fencedContent = borrowedBoundWriterIsolationFencedContent(state.originalContent, scopes, f.writerRole)
		if err := borrowedBoundWriterIsolationWriteHBA(ctx, t, f, state.hbaPath, state.fencedContent); err != nil {
			return state, err
		}
		state.fenceWritten = true
		if err := borrowedBoundWriterIsolationReload(ctx, admin); err != nil {
			return state, err
		}
		effective, err := borrowedBoundWriterIsolationReadRules(ctx, admin)
		if err != nil {
			return state, err
		}
		if err := borrowedBoundWriterIsolationFenceStateCheck(effective, state.requiredScopes, f.writerRole); err != nil {
			return state, err
		}
		state.fenceApplied = true
		state.coverageOK = true
		if err := borrowedBoundWriterIsolationAwaitState(t, ctx, f, serverIP, f.writerRole, b.passwordP1, "REJECT", "28000", 10*time.Second); err != nil {
			return state, err
		}
	}
	if hooks.atFenceApplied != nil {
		if err := hooks.atFenceApplied(ctx, state); err != nil {
			return state, err
		}
	}
	if err := ctx.Err(); err != nil {
		return state, fmt.Errorf("%w: %v", errWriterIsolationCancelled, err)
	}

	// External route coverage: every supported transport must be rejected at
	// the intended server admission stage, never timeout/bad-credential/routing.
	for _, transport := range []string{"unix", "loopback", "serverip"} {
		line := runHelperAuthCheck(t, ctx, f.fx.containerID, transport, serverIP, f.writerRole, b.passwordP1)
		state.routeLines[transport] = line
		if !borrowedBoundWriterIsolationContains(line, "REJECT", "28000") {
			return state, fmt.Errorf("%w: transport %s admission line %q", errWriterIsolationRoute, transport, line)
		}
	}

	// Bounded held proof interval: the fence stays effective over wall time.
	select {
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		return state, fmt.Errorf("%w: %v", errWriterIsolationCancelled, ctx.Err())
	}
	if hooks.duringProof != nil {
		if err := hooks.duringProof(ctx, state); err != nil {
			return state, err
		}
	}
	if err := ctx.Err(); err != nil {
		return state, fmt.Errorf("%w: %v", errWriterIsolationCancelled, err)
	}
	proofLine := runHelperAuthCheck(t, ctx, f.fx.containerID, "serverip", serverIP, f.writerRole, b.passwordP1)
	if !borrowedBoundWriterIsolationContains(proofLine, "REJECT", "28000") {
		return state, fmt.Errorf("%w: proof-interval admission line %q", errWriterIsolationProof, proofLine)
	}
	state.proofRounds++
	state.proofEffective = true

	// P0 stays refused under the fence (the reject rule precedes authentication).
	state.p0PostFence = runHelperAuthCheck(t, ctx, f.fx.containerID, "serverip", serverIP, f.writerRole, f.writerPassword)
	if !borrowedBoundWriterIsolationContains(state.p0PostFence, "REJECT", "28000") {
		return state, fmt.Errorf("%w: P0 was not refused under the fence: %s", errWriterIsolationRoute, state.p0PostFence)
	}

	// FINAL fence-state verification (detects a corrupted/invalid HBA file even
	// while the postmaster keeps the old in-memory rules).
	finalRules, err := borrowedBoundWriterIsolationReadRules(ctx, admin)
	if err != nil {
		return state, err
	}
	if err := borrowedBoundWriterIsolationFenceStateCheck(finalRules, state.requiredScopes, f.writerRole); err != nil {
		return state, err
	}

	// Post-fence independent census, owner, catalog and protected routes.
	if hooks.beforeCensus != nil {
		if err := hooks.beforeCensus(ctx, state); err != nil {
			return state, err
		}
	}
	state.censusPost, err = borrowedBoundWriterIsolationCensus(ctx, censusConn, f.writerRole)
	if err != nil {
		return state, err
	}
	healthCtx2, cancelHealth2 := context.WithTimeout(ctx, borrowedOwnerRotationHealthBudget)
	healthErr2 := f.lock.Health(healthCtx2)
	cancelHealth2()
	anchorCtx2, cancelAnchor2 := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
	anchorErr2 := b.anchor.Recheck(anchorCtx2)
	cancelAnchor2()
	if healthErr2 != nil || anchorErr2 != nil {
		return state, fmt.Errorf("%w: post-fence owner health=%v anchor=%v", errWriterIsolationOwner, healthErr2, anchorErr2)
	}
	state.ownerOK = state.ownerOK && healthErr2 == nil && anchorErr2 == nil
	oid2, oidErr2 := borrowedOwnerDDLTargetOID(ctx, b)
	if oidErr2 != nil || oid2 != fresh.replacementOID {
		return state, fmt.Errorf("%w: post-fence actual catalog oid=%d err=%v", errWriterIsolationReplacement, oid2, oidErr2)
	}
	if err := borrowedBoundWriterIsolationObserverOK(ctx, f); err != nil {
		return state, err
	}
	if err := borrowedBoundWriterIsolationControlOK(ctx, f); err != nil {
		return state, err
	}
	return state, nil
}

// borrowedBoundWriterIsolationTryPublish evaluates EVERY fence/coverage/lineage
// proof and only then publishes the single-use NON-AUTHORIZING fact set.
func borrowedBoundWriterIsolationTryPublish(t *testing.T, label string, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, lineage *borrowedBoundWriterIsolationLineage, state *borrowedBoundWriterIsolationFlowState, isolation *borrowedBoundWriterIsolation) error {
	t.Helper()
	if isolation == nil || isolation.state == nil {
		return fmt.Errorf("%w: %s has no isolation fact set", errWriterIsolationPublished, label)
	}
	if lineage == nil || !lineage.consumed {
		return fmt.Errorf("%w: %s has no consumed lineage facts", errWriterIsolationLineage, label)
	}
	if state == nil {
		return fmt.Errorf("%w: %s has no fence flow state", errWriterIsolationPublished, label)
	}
	if state.flowErr != nil {
		return fmt.Errorf("%w: %s fence flow refused: %v", errWriterIsolationPublished, label, state.flowErr)
	}
	if !state.fenceApplied || !state.coverageOK || !state.proofEffective || state.proofRounds <= 0 {
		return fmt.Errorf("%w: %s fence/coverage/proof is incomplete", errWriterIsolationPublished, label)
	}
	for _, transport := range []string{"unix", "loopback", "serverip"} {
		if !borrowedBoundWriterIsolationContains(state.routeLines[transport], "REJECT", "28000") {
			return fmt.Errorf("%w: %s transport %s admission %q", errWriterIsolationRoute, label, transport, state.routeLines[transport])
		}
	}
	if !borrowedBoundWriterIsolationContains(state.p0PreFence, "REJECT", "28P01") ||
		!borrowedBoundWriterIsolationContains(state.p0PostFence, "REJECT", "28000") {
		return fmt.Errorf("%w: %s P0 admission pre=%q post=%q", errWriterIsolationPublished, label, state.p0PreFence, state.p0PostFence)
	}
	if state.censusPre < 0 || state.censusPost != 0 {
		return fmt.Errorf("%w: %s retained-writer census post=%d", errWriterIsolationCensus, label, state.censusPost)
	}
	if !state.ownerOK || !state.catalogOK || !state.observerOK || !state.controlOK {
		return fmt.Errorf("%w: %s owner=%t catalog=%t observer=%t control=%t", errWriterIsolationPublished, label, state.ownerOK, state.catalogOK, state.observerOK, state.controlOK)
	}
	if !state.cleanupRestored || state.cleanupErr != nil {
		return fmt.Errorf("%w: %s fence disposal was not proven clean", errWriterIsolationPublished, label)
	}
	covered := make([]string, 0, len(state.requiredScopes))
	for _, scope := range state.requiredScopes {
		covered = append(covered, borrowedBoundWriterIsolationScopeName(scope))
	}
	if lineage.issuance == nil {
		return fmt.Errorf("%w: %s lineage has no one-shot issuance latch", errWriterIsolationPublished, label)
	}
	return lineage.issuance.claimAndPublish(func() error {
		return isolation.publish(borrowedBoundWriterIsolationFacts{
			InstanceID:      b.fixture.instanceID,
			TargetKey:       b.fixture.guardKey,
			RoleFingerprint: fresh.binding.OriginalRoleFingerprint(),
			ReplacementOID:  fresh.replacementOID,
			OwnerPID:        b.owner.BackendPID,
			ReadinessBacked: lineage.readinessBacked,
			CoveredScopes:   covered,
			ExercisedRoutes: []string{"unix", "loopback", "serverip"},
			ProofRounds:     state.proofRounds,
		})
	})
}

// borrowedBoundWriterIsolationAssertUnpublished requires a refusal control to
// have published nothing and to have no consumable fact set (including through
// a shared-state copy).
func borrowedBoundWriterIsolationAssertUnpublished(t *testing.T, label string, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding, lineage *borrowedBoundWriterIsolationLineage, state *borrowedBoundWriterIsolationFlowState, isolation *borrowedBoundWriterIsolation) {
	t.Helper()
	if err := borrowedBoundWriterIsolationTryPublish(t, label, b, fresh, lineage, state, isolation); err == nil {
		t.Fatalf("%s: the isolation fact set was published without every fence/coverage/lineage proof", label)
	}
	if isolation.publishedNow() {
		t.Fatalf("%s: the isolation fact set reports published after a refusal", label)
	}
	if _, ok := isolation.consume(); ok {
		t.Fatalf("%s: the isolation fact set was consumable after a refusal", label)
	}
	copied := *isolation
	if _, ok := copied.consume(); ok {
		t.Fatalf("%s: a shared-state isolation copy was consumable after a refusal", label)
	}
}

// borrowedBoundWriterIsolationRetireP1 is the lane-local lighter prerequisite
// of the fence controls: the exact genuine P1 registration, single use and
// strict guarded retirement over the authentic bound capture.
func borrowedBoundWriterIsolationRetireP1(t *testing.T, ctx context.Context, b *borrowedSuccessorBaseline, fresh *borrowedReplacementBinding) *borrowedBoundWriterIsolationLineage {
	t.Helper()
	conn, reg := borrowedReplacementSessionRegisterP1(t, ctx, b, fresh)
	useCtx, cancelUse := context.WithTimeout(ctx, 60*time.Second)
	useErr := reg.Use(useCtx)
	cancelUse()
	if useErr != nil {
		t.Fatalf("bound writer isolation prerequisite P1 use refused: %v", useErr)
	}
	if got := atomic.LoadInt32(&reg.state.probeExecutions); got != 1 {
		t.Fatalf("bound writer isolation prerequisite probe executions=%d, want exactly 1", got)
	}
	borrowedReplacementSessionRetire(t, ctx, b, conn, reg.state)
	return &borrowedBoundWriterIsolationLineage{
		consumed: true, readinessBacked: false,
		instanceID: b.fixture.instanceID, targetKey: b.fixture.guardKey,
		fingerprint:    fresh.binding.OriginalRoleFingerprint(),
		replacementOID: fresh.replacementOID, ownerPID: b.owner.BackendPID,
		issuance: &borrowedBoundWriterIsolationIssuance{},
	}
}

// TestBorrowedReplacementBoundWriterIsolation is the bounded bound-writer-path
// isolation fence lane described in the file header.
func TestBorrowedReplacementBoundWriterIsolation(t *testing.T) {
	ctx := t.Context()

	// P: the single chain over ONE authentic bound fixture: bound capture ->
	// existing dirty-guard refusal -> unchanged readiness machinery whose
	// genuine single-use readiness witness is consumed ONCE as lineage facts ->
	// strict registered-P1 retirement -> the actual writer-ingress fence with
	// full route coverage and external admission-stage proof -> the single-use
	// NON-AUTHORIZING isolation fact set -> clean fence disposal.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		f := b.fixture
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)

		// Existing bound dirty-guard refusal with the closed output contract.
		var probeCalls, acceptanceCalls int32
		prepAttemptID := fmt.Sprintf("borrowed-replacement-bound-writer-isolation-refusal-%d", time.Now().UnixNano())
		marker := newBorrowedBoundNativeReadyMarker(prepAttemptID, f.adminRole)
		attempt := prepareBorrowedBoundNativeReadyAttempt(t, ctx, b, fresh, prepAttemptID, marker, &probeCalls, &acceptanceCalls)
		if err := attempt.registerOrigin(ctx); err != nil {
			t.Fatalf("bound writer isolation refusal origin registration refused: %v", err)
		}
		result, receipt, runErr := dispatchBorrowedBoundNativeReady(t, ctx, attempt)
		if runErr == nil || runErr.Error() != "bound target guard is not clean" {
			t.Fatalf("bound writer isolation refusal is not the exact production bound dirty-guard refusal: %v", runErr)
		}
		if got := marker.callsNow(); got != 1 {
			t.Fatalf("bound writer isolation refusal marker calls=%d, want exactly 1", got)
		}
		borrowedBoundNativeReadyRequireNoProbeOutput(t, "bound writer isolation refusal", result)
		borrowedBoundNativeReadyRequireAbsentReceipt(t, "bound writer isolation refusal", receipt)
		borrowedBoundReentryAssertOutcomeBoundaries(t, "bound writer isolation refusal", attempt, result, receipt, &probeCalls, &acceptanceCalls)
		borrowedBoundReentryReadinessAssertBaseline(t, "bound writer isolation refusal", ctx, b, base)

		// The unchanged readiness machinery; its single-use witness is consumed
		// ONCE as lineage facts, never authorization.
		stage := &borrowedBoundReentryReadinessStage{
			key: f.guardKey, expectedOperation: f.operation,
			journalAttemptID: prepAttemptID, journalFacts: &borrowedReplacementOwnerJournalFacts{},
		}
		outcome := borrowedBoundReentryReadinessRun(ctx, t, b, fresh, stage, nil)
		readiness := newBorrowedBoundReentryReadiness()
		if err := borrowedBoundReentryReadinessTryPublish(t, "bound writer isolation readiness", b, fresh, stage, outcome, readiness); err != nil {
			t.Fatalf("bound writer isolation readiness did not satisfy the completion proofs: %v", err)
		}
		readinessFacts, ok := readiness.consume()
		if !ok {
			t.Fatal("bound writer isolation readiness witness was not consumable once")
		}
		if _, again := readiness.consume(); again {
			t.Fatal("bound writer isolation readiness witness was consumable twice")
		}
		readinessCopy := *readiness
		if _, copyOK := readinessCopy.consume(); copyOK {
			t.Fatal("a shared-state readiness copy was consumable")
		}
		lineage := &borrowedBoundWriterIsolationLineage{
			consumed: true, readinessBacked: true,
			instanceID: readinessFacts.InstanceID, targetKey: readinessFacts.TargetKey,
			fingerprint: readinessFacts.RoleFingerprint, replacementOID: readinessFacts.ReplacementOID,
			ownerPID: readinessFacts.OwnerPID, issuance: &borrowedBoundWriterIsolationIssuance{},
		}
		if err := borrowedReplacementOwnerTailRetireGuarded(t, ctx, b, outcome); err != nil {
			t.Fatalf("bound writer isolation registered-P1 retirement refused: %v", err)
		}

		// The actual writer-ingress fence over every enumerated route.
		flowState, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, lineage, nil)
		if flowErr != nil {
			t.Fatalf("bound writer isolation fence flow refused: %v", flowErr)
		}
		isolation := newBorrowedBoundWriterIsolation()
		if err := borrowedBoundWriterIsolationTryPublish(t, "bound writer isolation positive", b, fresh, lineage, flowState, isolation); err != nil {
			t.Fatalf("bound writer isolation positive did not satisfy the fence/coverage/lineage proofs: %v", err)
		}
		isoFacts, ok := isolation.consume()
		if !ok {
			t.Fatal("bound writer isolation fact set was not consumable once")
		}
		if isoFacts.InstanceID != f.instanceID || isoFacts.TargetKey != f.guardKey || isoFacts.ReplacementOID != fresh.replacementOID ||
			isoFacts.OwnerPID != b.owner.BackendPID || !isoFacts.ReadinessBacked || isoFacts.Nonce != 1 ||
			len(isoFacts.CoveredScopes) == 0 || len(isoFacts.ExercisedRoutes) != 3 || isoFacts.ProofRounds <= 0 {
			t.Fatalf("bound writer isolation facts are not the observed fence/coverage/lineage: %+v", isoFacts)
		}
		if _, again := isolation.consume(); again {
			t.Fatal("bound writer isolation fact set was consumable twice")
		}
		isolationCopy := *isolation
		if _, copyOK := isolationCopy.consume(); copyOK {
			t.Fatal("a shared-state isolation copy was consumable")
		}

		// Post-disposal: the original HBA is restored, P1 authenticates again,
		// P0 stays refused, the refused journal row is preserved append-only and
		// the guard stays unresolved.
		if !flowState.cleanupRestored || flowState.cleanupErr != nil {
			t.Fatalf("bound writer isolation fence disposal was not proven clean: restored=%t err=%v", flowState.cleanupRestored, flowState.cleanupErr)
		}
		serverIP, ipErr := f.fx.container.ContainerIP(ctx)
		if ipErr != nil {
			t.Fatalf("bound writer isolation container endpoint refused: %v", ipErr)
		}
		if err := borrowedBoundWriterIsolationAwaitState(t, ctx, f, serverIP, f.writerRole, b.passwordP1, "AUTH_OK", "", 10*time.Second); err != nil {
			t.Fatalf("post-disposal P1 authentication did not recover: %v", err)
		}
		restoredP0 := runHelperAuthCheck(t, ctx, f.fx.containerID, "serverip", serverIP, f.writerRole, f.writerPassword)
		if !borrowedBoundWriterIsolationContains(restoredP0, "REJECT", "28P01") {
			t.Fatalf("P0 was rehabilitated after fence disposal: %s", restoredP0)
		}
		if count := borrowedReplacementOwnerJournalCount(t, ctx, f.controlPool, prepAttemptID); count != 1 {
			t.Fatalf("bound writer isolation durable refused journal rows=%d, want exactly 1", count)
		}
		borrowedOwnerDDLGuard(t, ctx, b)
		t.Logf("bound writer isolation positive: genuine readiness consumed once as lineage; writer-reject fence applied to %d enumerated scopes with reload and effective-rule verification; fresh P1 rejected with 28000 over unix/loopback/serverip through the held proof interval; P0 refused pre-fence (28P01) and under the fence (28000); census zero; owner/catalog/observer/control valid; single-use NON-AUTHORIZING isolation facts consumed once; HBA disposed cleanly and the guard stayed unresolved", len(isoFacts.CoveredScopes))
	}()

	// N1: missing/unapplied fence: no writer-reject rules exist, the coverage
	// verification refuses, and a fresh P1 still authenticates (proving the
	// fence is genuinely absent), with no publication.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		lineage := borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		state, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, lineage, &borrowedBoundWriterIsolationHooks{skipFence: true})
		isolation := newBorrowedBoundWriterIsolation()
		borrowedBoundWriterIsolationAssertUnpublished(t, "bound writer isolation missing fence", b, fresh, lineage, state, isolation)
		if !errors.Is(flowErr, errWriterIsolationCoverage) {
			t.Fatalf("missing-fence refusal is not the real coverage stage: %v", flowErr)
		}
		if !strings.Contains(state.p1PreFence, "state=AUTH_OK") {
			t.Fatalf("missing-fence control did not prove the fence was absent: %s", state.p1PreFence)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound writer isolation missing fence", ctx, b, base)
		t.Logf("bound writer isolation missing-fence negative: with no writer-reject rules the real coverage verification refused, a fresh genuine P1 still authenticated (fence genuinely absent) and nothing was published")
	}()

	// N2: incomplete route coverage: a partial fence covers only the local
	// scope; the TCP routes still admit P1, the coverage verification refuses
	// and nothing is published.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		lineage := borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		f := b.fixture
		admin := f.fx.admin
		hbaPath, pathErr := borrowedBoundWriterIsolationHBAPath(ctx, admin)
		if pathErr != nil {
			t.Fatalf("partial-coverage HBA path refused: %v", pathErr)
		}
		originalContent, contentErr := borrowedBoundWriterIsolationReadHBA(ctx, f, hbaPath)
		if contentErr != nil {
			t.Fatalf("partial-coverage HBA read refused: %v", contentErr)
		}
		rules, rulesErr := borrowedBoundWriterIsolationReadRules(ctx, admin)
		if rulesErr != nil {
			t.Fatalf("partial-coverage rules read refused: %v", rulesErr)
		}
		scopes := borrowedBoundWriterIsolationScopes(rules)
		if len(scopes) < 2 {
			t.Fatalf("partial-coverage control requires at least two scopes, got %d", len(scopes))
		}
		partial := borrowedBoundWriterIsolationFencedContent(originalContent, scopes[:1], f.writerRole)
		if err := borrowedBoundWriterIsolationWriteHBA(ctx, t, f, hbaPath, partial); err != nil {
			t.Fatalf("partial-coverage fence write refused: %v", err)
		}
		// Block-local disposal: the partial fence is restored before any later
		// control runs.
		defer func() {
			cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancelCleanup()
			_ = borrowedBoundWriterIsolationWriteHBA(cleanupCtx, t, f, hbaPath, originalContent)
			_ = borrowedBoundWriterIsolationReload(cleanupCtx, admin)
		}()
		if err := borrowedBoundWriterIsolationReload(ctx, admin); err != nil {
			t.Fatalf("partial-coverage reload refused: %v", err)
		}
		serverIP, ipErr := f.fx.container.ContainerIP(ctx)
		if ipErr != nil {
			t.Fatalf("partial-coverage endpoint refused: %v", ipErr)
		}
		unfenced := runHelperAuthCheck(t, ctx, f.fx.containerID, "serverip", serverIP, f.writerRole, b.passwordP1)
		if !strings.Contains(unfenced, "state=AUTH_OK") {
			t.Fatalf("partial-coverage control did not prove the uncovered route admits P1: %s", unfenced)
		}
		state, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, lineage, &borrowedBoundWriterIsolationHooks{skipFence: true})
		isolation := newBorrowedBoundWriterIsolation()
		borrowedBoundWriterIsolationAssertUnpublished(t, "bound writer isolation incomplete coverage", b, fresh, lineage, state, isolation)
		if !errors.Is(flowErr, errWriterIsolationCoverage) {
			t.Fatalf("incomplete-coverage refusal is not the real coverage stage: %v", flowErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound writer isolation incomplete coverage", ctx, b, base)
		t.Logf("bound writer isolation incomplete-coverage negative: the partial fence left the container-address route admitting a fresh genuine P1, the real coverage verification refused and nothing was published")
	}()

	// N3: pre-existing retained writer session: ingress denial alone must NOT
	// pass. A genuine P1 session opened before fencing survives the fence (HBA
	// affects new connections only), the census refuses publication, and only
	// after the retained incarnation is closed does the census reach zero.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		lineage := borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		f := b.fixture
		retainCtx, cancelRetain := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
		retained, retainErr := pgx.Connect(retainCtx, borrowedReplacementP1TargetDSN(t, b))
		cancelRetain()
		if retainErr != nil {
			t.Fatalf("retained-writer control P1 connect refused: %v", retainErr)
		}
		retainedClosed := false
		t.Cleanup(func() {
			if retainedClosed {
				return
			}
			borrowedSuccessorCloseConn(t, retained)
		})
		var one int
		queryCtx, cancelQuery := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		if err := retained.QueryRow(queryCtx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
			cancelQuery()
			t.Fatalf("retained-writer control pre-fence query refused: %v", err)
		}
		cancelQuery()
		state, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, lineage, nil)
		isolation := newBorrowedBoundWriterIsolation()
		borrowedBoundWriterIsolationAssertUnpublished(t, "bound writer isolation retained writer", b, fresh, lineage, state, isolation)
		if flowErr != nil {
			t.Fatalf("retained-writer control flow refused outside the census stage: %v", flowErr)
		}
		if state.censusPost != 1 {
			t.Fatalf("retained-writer control census post=%d, want exactly the retained session", state.censusPost)
		}
		queryCtx2, cancelQuery2 := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		if err := retained.QueryRow(queryCtx2, `SELECT 1`).Scan(&one); err != nil || one != 1 {
			cancelQuery2()
			t.Fatalf("retained-writer session did not survive the ingress fence: %v", err)
		}
		cancelQuery2()
		borrowedSuccessorCloseConn(t, retained)
		retainedClosed = true
		censusConn, connErr := pgx.Connect(ctx, f.adminDSN)
		if connErr != nil {
			t.Fatalf("retained-writer control census reconnect refused: %v", connErr)
		}
		defer func() {
			closeCtx, cancelClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
			_ = censusConn.Close(closeCtx)
			cancelClose()
		}()
		censusCtx, cancelCensus := context.WithTimeout(ctx, 30*time.Second)
		censusErr := borrowedBoundWriterIsolationAwaitCensusZero(t, censusCtx, censusConn, f.writerRole, 15*time.Second)
		cancelCensus()
		if censusErr != nil {
			t.Fatalf("retained-writer control did not reach an empty census after closing: %v", censusErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound writer isolation retained writer", ctx, b, base)
		t.Logf("bound writer isolation retained-writer negative: the pre-existing genuine P1 session survived the ingress fence (new connections rejected), the real census refused publication while it remained, and the census reached zero only after the retained incarnation was closed")
	}()

	// N4: reload/removal uncertainty: an invalid HBA written during removal
	// leaves the postmaster on the old fenced rules with an error-bearing file;
	// the FINAL fence-state verification refuses publication and the fence stays
	// externally effective until the original HBA is restored.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		lineage := borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		f := b.fixture
		state, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, lineage, &borrowedBoundWriterIsolationHooks{
			atFenceApplied: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				if err := borrowedBoundWriterIsolationWriteHBA(hookCtx, t, f, flow.hbaPath, "this is not a valid pg_hba line\n"); err != nil {
					return err
				}
				return borrowedBoundWriterIsolationReload(hookCtx, f.fx.admin)
			},
		})
		isolation := newBorrowedBoundWriterIsolation()
		borrowedBoundWriterIsolationAssertUnpublished(t, "bound writer isolation reload uncertainty", b, fresh, lineage, state, isolation)
		if !errors.Is(flowErr, errWriterIsolationReload) {
			t.Fatalf("reload-uncertainty refusal is not the real reload/effective-rule stage: %v", flowErr)
		}
		if !state.cleanupRestored || state.cleanupErr != nil {
			t.Fatalf("reload-uncertainty disposal was not proven clean: restored=%t err=%v", state.cleanupRestored, state.cleanupErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound writer isolation reload uncertainty", ctx, b, base)
		t.Logf("bound writer isolation reload-uncertainty negative: the invalid HBA file was detected by the final effective-rule verification (postmaster retained the old fenced rules), publication was refused and the original HBA was restored")
	}()

	// N5: observer/census visibility loss: the independent census connection is
	// lost before the census; the real visibility stage refuses publication.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		lineage := borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		state, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, lineage, &borrowedBoundWriterIsolationHooks{
			beforeCensus: func(hookCtx context.Context, flow *borrowedBoundWriterIsolationFlowState) error {
				if flow.censusConn == nil {
					return errors.New("bound writer isolation census connection is absent")
				}
				closeCtx, cancelClose := context.WithTimeout(context.Background(), borrowedOwnerRotationCleanupBudget)
				closeErr := flow.censusConn.Close(closeCtx)
				cancelClose()
				if closeErr != nil {
					return closeErr
				}
				return nil
			},
		})
		isolation := newBorrowedBoundWriterIsolation()
		borrowedBoundWriterIsolationAssertUnpublished(t, "bound writer isolation census visibility loss", b, fresh, lineage, state, isolation)
		if !errors.Is(flowErr, errWriterIsolationCensus) {
			t.Fatalf("census-visibility refusal is not the real independent-census stage: %v", flowErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound writer isolation census visibility loss", ctx, b, base)
		t.Logf("bound writer isolation census-visibility negative: the independent census connection was lost before the census, the real visibility stage refused and nothing was published")
	}()

	// N6: owner loss: the original anchored owner session is terminated; the
	// real owner/anchor validity stage refuses publication.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		lineage := borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		var terminated bool
		termCtx, cancelTerm := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		termErr := b.fixture.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, b.owner.BackendPID).Scan(&terminated)
		cancelTerm()
		if termErr != nil || !terminated {
			t.Fatalf("owner-loss termination refused: terminated=%t err=%v", terminated, termErr)
		}
		state, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, lineage, nil)
		isolation := newBorrowedBoundWriterIsolation()
		borrowedBoundWriterIsolationAssertUnpublished(t, "bound writer isolation owner loss", b, fresh, lineage, state, isolation)
		if !errors.Is(flowErr, errWriterIsolationOwner) {
			t.Fatalf("owner-loss refusal is not the real owner/anchor stage: %v", flowErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound writer isolation owner loss", ctx, b, base)
		t.Logf("bound writer isolation owner-loss negative: the original anchored owner loss refused the real owner/anchor stage and nothing was published")
	}()

	// N7: replacement change: the actual target catalog name/OID no longer
	// matches the captured replacement; the real catalog revalidation refuses
	// publication.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		lineage := borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		f := b.fixture
		changedName := f.targetDB + "_changed"
		// Make the rename deterministic: no target-database session may remain
		// (the exact P1 incarnation was already strictly retired).
		termCtx, cancelTerm := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
		_, termErr := f.fx.admin.Exec(termCtx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid <> pg_backend_pid()`, f.targetDB)
		cancelTerm()
		if termErr != nil {
			t.Fatalf("replacement-change target-session termination refused: %v", termErr)
		}
		renameCtx, cancelRename := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
		_, renameErr := f.fx.admin.Exec(renameCtx, `ALTER DATABASE `+pgx.Identifier{f.targetDB}.Sanitize()+` RENAME TO `+pgx.Identifier{changedName}.Sanitize())
		cancelRename()
		if renameErr != nil {
			t.Fatalf("replacement-change rename refused: %v", renameErr)
		}
		renamedBack := false
		t.Cleanup(func() {
			if renamedBack {
				return
			}
			backCtx, cancelBack := context.WithTimeout(context.Background(), borrowedOwnerRotationAuthBudget)
			_, _ = f.fx.admin.Exec(backCtx, `ALTER DATABASE `+pgx.Identifier{changedName}.Sanitize()+` RENAME TO `+pgx.Identifier{f.targetDB}.Sanitize())
			cancelBack()
		})
		state, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, lineage, nil)
		isolation := newBorrowedBoundWriterIsolation()
		borrowedBoundWriterIsolationAssertUnpublished(t, "bound writer isolation replacement change", b, fresh, lineage, state, isolation)
		if !errors.Is(flowErr, errWriterIsolationReplacement) {
			t.Fatalf("replacement-change refusal is not the real catalog revalidation stage: %v", flowErr)
		}
		backCtx, cancelBack := context.WithTimeout(ctx, borrowedOwnerRotationAuthBudget)
		_, backErr := f.fx.admin.Exec(backCtx, `ALTER DATABASE `+pgx.Identifier{changedName}.Sanitize()+` RENAME TO `+pgx.Identifier{f.targetDB}.Sanitize())
		cancelBack()
		if backErr != nil {
			t.Fatalf("replacement-change rename-back refused: %v", backErr)
		}
		renamedBack = true
		borrowedBoundReentryReadinessAssertBaseline(t, "bound writer isolation replacement change", ctx, b, base)
		t.Logf("bound writer isolation replacement-change negative: the actual replacement catalog no longer matched the captured OID, the real catalog revalidation refused and nothing was published")
	}()

	// N8: shared-prefix loss: the real copied-prefix loss permanently
	// invalidates the shared replacement prefix; the real prefix stage refuses
	// publication.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
		lineage := borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		borrowedReplacementSessionCopiedPrefixLoss(t, fresh)
		state, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, lineage, nil)
		isolation := newBorrowedBoundWriterIsolation()
		borrowedBoundWriterIsolationAssertUnpublished(t, "bound writer isolation shared-prefix loss", b, fresh, lineage, state, isolation)
		if !errors.Is(flowErr, errWriterIsolationPrefix) {
			t.Fatalf("shared-prefix-loss refusal is not the real prefix stage: %v", flowErr)
		}
		borrowedBoundReentryReadinessAssertBaseline(t, "bound writer isolation shared-prefix loss", ctx, b, base)
		t.Logf("bound writer isolation shared-prefix-loss negative: the real copied-prefix loss refused the real prefix stage and nothing was published")
	}()

	// N9: cancellation during fence establishment and during the held proof
	// interval: no publication, and cancel -> release -> bounded join restores
	// the original HBA.
	for _, cancelAt := range []string{"establishment", "proof-interval"} {
		cancelAt := cancelAt
		func() {
			b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
			base := borrowedBoundReentryReadinessCaptureBaseline(t, ctx, b, fresh)
			lineage := borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
			laneCtx, cancelLane := context.WithCancel(ctx)
			defer cancelLane()
			var canceled atomic.Bool
			hooks := &borrowedBoundWriterIsolationHooks{}
			if cancelAt == "establishment" {
				hooks.atFenceApplied = func(_ context.Context, _ *borrowedBoundWriterIsolationFlowState) error {
					canceled.Store(true)
					cancelLane()
					return nil
				}
			} else {
				hooks.duringProof = func(_ context.Context, _ *borrowedBoundWriterIsolationFlowState) error {
					canceled.Store(true)
					cancelLane()
					return nil
				}
			}
			// Synchronous immediate cancel -> release -> bounded disposal: the
			// hook cancels the lane context, the flow observes it at the next
			// boundary and the deferred disposal restores the HBA before return.
			state, flowErr := borrowedBoundWriterIsolationRun(laneCtx, t, b, fresh, lineage, hooks)
			if !canceled.Load() {
				t.Fatalf("bound writer isolation %s cancellation was not exercised", cancelAt)
			}
			isolation := newBorrowedBoundWriterIsolation()
			borrowedBoundWriterIsolationAssertUnpublished(t, "bound writer isolation cancellation "+cancelAt, b, fresh, lineage, state, isolation)
			if !errors.Is(flowErr, errWriterIsolationCancelled) {
				t.Fatalf("bound writer isolation %s cancellation refusal is not the real cancellation stage: %v", cancelAt, flowErr)
			}
			if state == nil || !state.cleanupRestored || state.cleanupErr != nil {
				t.Fatalf("bound writer isolation %s cancellation disposal was not proven clean: %+v", cancelAt, state)
			}
			serverIP, ipErr := b.fixture.fx.container.ContainerIP(ctx)
			if ipErr != nil {
				t.Fatalf("bound writer isolation %s cancellation endpoint refused: %v", cancelAt, ipErr)
			}
			if err := borrowedBoundWriterIsolationAwaitState(t, ctx, b.fixture, serverIP, b.fixture.writerRole, b.passwordP1, "AUTH_OK", "", 10*time.Second); err != nil {
				t.Fatalf("bound writer isolation %s cancellation did not restore the HBA: %v", cancelAt, err)
			}
			borrowedBoundReentryReadinessAssertBaseline(t, "bound writer isolation cancellation "+cancelAt, ctx, b, base)
			t.Logf("bound writer isolation %s cancellation negative: the cancel was observed, no isolation fact was published, cancel->release->bounded join completed and the original HBA was restored", cancelAt)
		}()
	}

	// N10: replay/copy plus post-publication loss: the consumed isolation fact
	// set is never reusable and a post-publication owner loss leaves no
	// reusable positive result while the fence disposal never authorizes
	// recovery or rehabilitates refused facts.
	func() {
		b, fresh := newBorrowedBoundNativeReadyCapture(t, ctx)
		lineage := borrowedBoundWriterIsolationRetireP1(t, ctx, b, fresh)
		state, flowErr := borrowedBoundWriterIsolationRun(ctx, t, b, fresh, lineage, nil)
		if flowErr != nil {
			t.Fatalf("post-publication-loss control fence flow refused: %v", flowErr)
		}
		isolation := newBorrowedBoundWriterIsolation()
		if err := borrowedBoundWriterIsolationTryPublish(t, "bound writer isolation post-publication loss", b, fresh, lineage, state, isolation); err != nil {
			t.Fatalf("post-publication-loss control did not publish: %v", err)
		}
		if _, ok := isolation.consume(); !ok {
			t.Fatal("post-publication-loss control fact set was not consumable once")
		}
		isolationCopy := *isolation
		if _, copyOK := isolationCopy.consume(); copyOK {
			t.Fatal("post-publication-loss control copy was consumable")
		}
		if !lineage.issuance.issuedNow() {
			t.Fatal("the source one-shot issuance latch was not claimed by the successful publication")
		}
		// Fresh-destination replay BEFORE the owner loss: the source lineage's
		// copy-shared one-shot issuance latch refuses a second fact set even
		// though the fresh destination has neither published nor consumed.
		freshBefore := newBorrowedBoundWriterIsolation()
		if err := borrowedBoundWriterIsolationTryPublish(t, "post-publication replay before loss", b, fresh, lineage, state, freshBefore); err == nil {
			t.Fatal("fresh-destination replay before the owner loss published a second fact set")
		}
		if freshBefore.publishedNow() {
			t.Fatal("fresh-destination replay before the owner loss reported published")
		}
		if _, ok := freshBefore.consume(); ok {
			t.Fatal("fresh-destination replay before the owner loss left a consumable fact set")
		}
		lineageCopyBefore := *lineage
		freshCopyBefore := newBorrowedBoundWriterIsolation()
		if err := borrowedBoundWriterIsolationTryPublish(t, "post-publication lineage-copy replay before loss", b, fresh, &lineageCopyBefore, state, freshCopyBefore); err == nil {
			t.Fatal("lineage-copy replay before the owner loss published a second fact set")
		}
		if freshCopyBefore.publishedNow() {
			t.Fatal("lineage-copy replay before the owner loss reported published")
		}
		if _, ok := freshCopyBefore.consume(); ok {
			t.Fatal("lineage-copy replay before the owner loss left a consumable fact set")
		}
		var terminated bool
		termCtx, cancelTerm := context.WithTimeout(ctx, borrowedOwnerRotationQueryBudget)
		termErr := b.fixture.controlPool.QueryRow(termCtx, `SELECT pg_terminate_backend($1)`, b.owner.BackendPID).Scan(&terminated)
		cancelTerm()
		if termErr != nil || !terminated {
			t.Fatalf("post-publication-loss owner termination refused: terminated=%t err=%v", terminated, termErr)
		}
		if isolation.publishedNow() != true {
			t.Fatal("post-publication loss changed the consumed fact set state")
		}
		if _, again := isolation.consume(); again {
			t.Fatal("post-publication loss left a reusable isolation result")
		}
		if err := isolation.publish(borrowedBoundWriterIsolationFacts{
			InstanceID: b.fixture.instanceID, TargetKey: b.fixture.guardKey,
			RoleFingerprint: fresh.binding.OriginalRoleFingerprint(), ReplacementOID: fresh.replacementOID,
			OwnerPID: b.owner.BackendPID, CoveredScopes: []string{"local"}, ExercisedRoutes: []string{"unix"}, ProofRounds: 1,
		}); err == nil {
			t.Fatal("post-publication loss allowed a second publication")
		}
		// Fresh-destination replay AFTER the owner loss: the copy-shared source
		// latch still refuses, so no reusable positive result exists anywhere.
		freshAfter := newBorrowedBoundWriterIsolation()
		if err := borrowedBoundWriterIsolationTryPublish(t, "post-publication replay after loss", b, fresh, lineage, state, freshAfter); err == nil {
			t.Fatal("fresh-destination replay after the owner loss published a second fact set")
		}
		if freshAfter.publishedNow() {
			t.Fatal("fresh-destination replay after the owner loss reported published")
		}
		if _, ok := freshAfter.consume(); ok {
			t.Fatal("fresh-destination replay after the owner loss left a consumable fact set")
		}
		lineageCopyAfter := *lineage
		freshCopyAfter := newBorrowedBoundWriterIsolation()
		if err := borrowedBoundWriterIsolationTryPublish(t, "post-publication lineage-copy replay after loss", b, fresh, &lineageCopyAfter, state, freshCopyAfter); err == nil {
			t.Fatal("lineage-copy replay after the owner loss published a second fact set")
		}
		if freshCopyAfter.publishedNow() {
			t.Fatal("lineage-copy replay after the owner loss reported published")
		}
		if _, ok := freshCopyAfter.consume(); ok {
			t.Fatal("lineage-copy replay after the owner loss left a consumable fact set")
		}
		serverIP, ipErr := b.fixture.fx.container.ContainerIP(ctx)
		if ipErr != nil {
			t.Fatalf("post-publication-loss endpoint refused: %v", ipErr)
		}
		if err := borrowedBoundWriterIsolationAwaitState(t, ctx, b.fixture, serverIP, b.fixture.writerRole, b.passwordP1, "AUTH_OK", "", 10*time.Second); err != nil {
			t.Fatalf("post-publication-loss disposal did not restore the HBA: %v", err)
		}
		restoredP0 := runHelperAuthCheck(t, ctx, b.fixture.fx.containerID, "serverip", serverIP, b.fixture.writerRole, b.fixture.writerPassword)
		if !borrowedBoundWriterIsolationContains(restoredP0, "REJECT", "28P01") {
			t.Fatalf("post-publication-loss disposal rehabilitated P0: %s", restoredP0)
		}
		borrowedOwnerDDLGuard(t, ctx, b)
		t.Logf("bound writer isolation post-publication-loss negative: the consumed single-use fact set was never reusable, the post-publication owner loss left no reusable positive result, the HBA disposal restored access for the genuine P1 only and P0 stayed refused")
	}()
}
