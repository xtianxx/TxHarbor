//go:build integration_dualproc

// publisher_dualproc_integration_test.go is the 013 supplement layer for
// T034's "dual instance" evidence: TWO REAL, INDEPENDENT OS
// `txharbor event-publisher` processes on ONE local host, sharing one isolated
// PostgreSQL + Kafka pair (testcontainers), running for the configured window
// (default ~10 minutes, shortened only by TXHARBOR_DUALPROC_DURATION).
//
// It exists because the merged evidence for T034/T037 is a SAME-PROCESS
// multi-instance drill (two *Publisher values in goroutines:
// TestPublisherKafkaRuntime/multi_instance_drain) plus same-process PG
// crash/lease tests. This layer adds process-level evidence that the
// documented protocol (FOR UPDATE SKIP LOCKED + claim_owner/claim_expires_at
// lease + attempt_count increment, outbox.go) actually shards work across two
// separate OS processes:
//
//  1. two distinct instance identities (owner=<32hex> on stdout, PIDs);
//  2. BOTH processes actually claim and publish (per-process stderr cycle
//     logs cross-checked against outbox attempt/publish counters);
//  3. mutually exclusive partitioning (every claim is owned by exactly one of
//     the two identities; a row's owner never changes before its lease
//     expires);
//  4. lease-expiry takeover after one process is killed (crash) while holding
//     claims: the surviving process reclaims the orphaned rows only after
//     claim_expires_at and publishes them (attempt_count +1);
//  5. final drain (0 pending / 0 blocked / 0 unowned claims) and graceful stop
//     of the survivor (SIGTERM -> exit 0, "stopped");
//  6. consumption effect: every event reaches the broker and the reference
//     consumer's persistent inbox yields EXACTLY ONE effective application
//     (ledger rows = 1) per event, with zero upstream payment-side-effect
//     rows. The reference consumer boundary (refconsumer.go) applies
//     verbatim: it is a simulated ledger, not production, not authoritative.
//
// Statement discipline (global): delivery is at-least-once and processing is
// idempotent; this layer never claims cross-system exactly-once.
//
// Scope limit (MUST travel with any reference to this evidence): SINGLE
// LOCAL HOST, one Kafka broker, one PostgreSQL container, synthetic load.
// It is NOT a multi-host / distributed / production result.
//
// Layer discipline: this file carries its own build tag
// (integration_dualproc) and is deliberately NOT collected by
// `make test-integration-kafka` or any PR-required CI job (a ~10 minute
// two-process drill would contend for Docker and blow the PR budget). Run it
// explicitly:
//
//	go test -tags integration_dualproc -count=1 -timeout 25m \
//	    -run TestPublisherDualProcessSupplement -v ./internal/events
//
// TXHARBOR_DUALPROC_DURATION (default 10m) shortens the steady-state window
// for smoke validation only; the committed acceptance run uses the default.
// TXHARBOR_DUALPROC_EVIDENCE_DIR (optional) writes the structured evidence
// lines to dualproc-test-evidence.txt for the /tmp evidence index.
//
// Skip convention: testcontainers.SkipIfProviderIsNotHealthy(t) skips (never
// passes) without a Docker provider. t.Cleanup terminates both containers and
// kills every child process even on failure; failures fail the test (non-zero
// exit), never a skip.
package events

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/testutil"
)

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

const (
	// dualprocDurationEnv shortens the steady-state window (smoke only).
	dualprocDurationEnv = "TXHARBOR_DUALPROC_DURATION"
	// dualprocEvidenceDirEnv optionally receives the structured evidence file.
	dualprocEvidenceDirEnv = "TXHARBOR_DUALPROC_EVIDENCE_DIR"

	dualprocDefaultDuration = 10 * time.Minute
	dualprocMinDuration     = 15 * time.Second

	// dualprocChainID is the synthetic chain identity carried by every event.
	dualprocChainID = int64(31337)

	// Publisher bounds shared by both processes. batch/poll keep a live claim
	// window observable at the sampling cadence; the lease is derived from the
	// run duration so takeover happens well inside the test.
	dualprocBatch        = 100
	dualprocPollInterval = 250 * time.Millisecond
	dualprocBackoffBase  = 200 * time.Millisecond
	dualprocBackoffMax   = 2 * time.Second

	dualprocSampleInterval   = 25 * time.Millisecond
	dualprocProducerInterval = 500 * time.Millisecond
	dualprocProducerPerTick  = 3
	dualprocInitialSeed      = 600
	dualprocFreezeBurst      = 300
	dualprocHashBase         = int64(0xD0A100000000)
)

var (
	dualprocOwnerRe     = regexp.MustCompile(`owner=([0-9a-fA-F]+)`)
	dualprocCycleRe     = regexp.MustCompile(`publish cycle.*?claimed=(\d+) acked=(\d+)`)
	dualprocPublishedRe = regexp.MustCompile(`outbox published count=(\d+)`)
)

// dualprocUpstreamTables are the 007/008/009/010/011 authority tables that
// must gain zero rows: the consumption effect of this drill is the reference
// consumer's simulated ledger only, never a payment action.
var dualprocUpstreamTables = []string{
	"withdrawal_requests",
	"payment_intents",
	"nonce_bindings",
	"tx_attempts",
	"tx_attempt_signings",
	"tx_send_attempts",
	"signing_requests",
}

// ---------------------------------------------------------------------------
// Evidence log
// ---------------------------------------------------------------------------

// dualprocEvidence collects stable, greppable evidence lines. Lines are logged
// with the DUALPROC-EVIDENCE prefix and optionally persisted to
// TXHARBOR_DUALPROC_EVIDENCE_DIR. add is called from the test goroutine only.
type dualprocEvidence struct {
	mu    sync.Mutex
	lines []string
}

func (e *dualprocEvidence) add(t *testing.T, format string, args ...any) {
	t.Helper()
	line := fmt.Sprintf(format, args...)
	t.Logf("DUALPROC-EVIDENCE %s", line)
	e.mu.Lock()
	e.lines = append(e.lines, line)
	e.mu.Unlock()
}

func (e *dualprocEvidence) writeFile(t *testing.T) {
	t.Helper()
	dir := strings.TrimSpace(os.Getenv(dualprocEvidenceDirEnv))
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create evidence dir: %v", err)
	}
	e.mu.Lock()
	body := strings.Join(e.lines, "\n") + "\n"
	e.mu.Unlock()
	if err := os.WriteFile(filepath.Join(dir, "dualproc-test-evidence.txt"), []byte(body), 0o644); err != nil {
		t.Fatalf("write evidence file: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Infrastructure
// ---------------------------------------------------------------------------

// dualprocDuration reads the smoke-only duration override.
func dualprocDuration(t *testing.T) time.Duration {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(dualprocDurationEnv))
	if raw == "" {
		return dualprocDefaultDuration
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < dualprocMinDuration {
		t.Fatalf("%s=%q is not a duration >= %s", dualprocDurationEnv, raw, dualprocMinDuration)
	}
	return d
}

// dualprocLease derives the lease TTL: long enough to be realistic, short
// enough that a killed process's orphaned claims are taken over inside the
// test window.
func dualprocLease(duration time.Duration) time.Duration {
	lease := duration / 120
	if lease < 2*time.Second {
		lease = 2 * time.Second
	}
	if lease > 5*time.Second {
		lease = 5 * time.Second
	}
	return lease
}

// dualprocPostgres boots a real PostgreSQL container with the embedded
// migrations applied. Skips (never passes) when no Docker provider exists.
func dualprocPostgres(t *testing.T) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
		postgres.WithDatabase("txharbor"),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	opts := db.MigrateOptions{DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 10 * time.Second}
	if err := db.MigrateUp(ctx, opts, io.Discard); err != nil {
		t.Fatalf("MigrateUp() error = %v", err)
	}
	return dsn
}

func dualprocPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// dualprocBuildBinary builds the real single binary once into the test dir.
// The drill runs the actual `txharbor event-publisher` command, not an
// in-process substitute.
func dualprocBuildBinary(t *testing.T, dir string) string {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	bin := filepath.Join(dir, "txharbor")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/txharbor")
	cmd.Dir = repoRoot
	cmd.Env = os.Environ()
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("go build ./cmd/txharbor: %v; stderr=%s", err, stderr.String())
	}
	return bin
}

// dualprocPublisherEnv is the complete, fail-closed 013 configuration for the
// event-publisher command (same shape as the app command tests, with the
// publisher bounds pinned for observability).
func dualprocPublisherEnv(dsn string, brokers []string, lease time.Duration) map[string]string {
	return map[string]string{
		config.EnvPGDSN:                 dsn,
		config.EnvRPCURL:                "http://127.0.0.1:8545",
		config.EnvChainID:               "31337",
		config.EnvStartHeight:           "0",
		config.EnvLogStartHeight:        "0",
		config.EnvLogContracts:          "0x1111111111111111111111111111111111111111",
		config.EnvDepositStartHeight:    "0",
		config.EnvDepositContracts:      "0x1111111111111111111111111111111111111111",
		config.EnvDepositWatchAddresses: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		config.EnvConfirmationDepth:     "10",
		config.EnvReorgMaxDepth:         "100",
		config.EnvHTTPAddr:              "127.0.0.1:0",

		config.EnvEventsEnabled: "true",
		config.EnvKafkaBrokers:  strings.Join(brokers, ","),
		config.EnvRedisAddr:     "127.0.0.1:6379",

		config.EnvRateLimitNewWithdrawal: "10/20",
		config.EnvRateLimitWrite:         "50/100",
		config.EnvRateLimitQuery:         "100/200",
		config.EnvRateLimitOperator:      "5/10",
		config.EnvRateLimitRPC:           "20/40",

		config.EnvEventsCapacitySoftLimit:   "1000",
		config.EnvEventsCapacityHardLimit:   "2000",
		config.EnvEventsCapacityReserve:     "100",
		config.EnvEventsCapacityRetention:   "24h",
		config.EnvEventsCapacityMaxShutdown: "1h",
		config.EnvEventsCapacityDrainTarget: "30m",

		config.EnvEventsPublisherBatch:        strconv.Itoa(dualprocBatch),
		config.EnvEventsPublisherPollInterval: dualprocPollInterval.String(),
		config.EnvEventsPublisherLeaseTTL:     lease.String(),
		config.EnvEventsPublisherBackoffBase:  dualprocBackoffBase.String(),
		config.EnvEventsPublisherBackoffMax:   dualprocBackoffMax.String(),
	}
}

// dualprocChildEnv builds a deterministic child environment: only what the
// binary needs plus the explicit 013 configuration (the parent TXHARBOR_* env
// never leaks in and can never make the drill pass on the wrong config).
func dualprocChildEnv(extra map[string]string) []string {
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"TZ=UTC",
	}
	for k, v := range extra {
		env = append(env, k+"="+v)
	}
	return env
}

// ---------------------------------------------------------------------------
// Publisher process
// ---------------------------------------------------------------------------

// dualprocProcess is one real OS publisher process with captured stdout
// (identity line) and stderr (structured publish-cycle logs).
type dualprocProcess struct {
	name       string
	owner      string
	pid        int
	cmd        *exec.Cmd
	done       chan error
	stdoutPath string
	stderrPath string

	mu      sync.Mutex
	exited  bool
	exitErr error
}

func dualprocStartProcess(t *testing.T, bin string, env map[string]string, dir, name string) *dualprocProcess {
	t.Helper()
	p := &dualprocProcess{
		name:       name,
		stdoutPath: filepath.Join(dir, name+".stdout.log"),
		stderrPath: filepath.Join(dir, name+".stderr.log"),
	}
	stdout, err := os.Create(p.stdoutPath)
	if err != nil {
		t.Fatalf("%s: create stdout: %v", name, err)
	}
	stderr, err := os.Create(p.stderrPath)
	if err != nil {
		_ = stdout.Close()
		t.Fatalf("%s: create stderr: %v", name, err)
	}
	p.cmd = exec.Command(bin, "event-publisher")
	p.cmd.Env = dualprocChildEnv(env)
	p.cmd.Stdout = stdout
	p.cmd.Stderr = stderr
	if err := p.cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		t.Fatalf("start %s: %v", name, err)
	}
	_ = stdout.Close()
	_ = stderr.Close()
	p.pid = p.cmd.Process.Pid
	p.done = make(chan error, 1)
	go func() { p.done <- p.cmd.Wait() }()
	t.Cleanup(func() { p.killIfAlive() })
	return p
}

func (p *dualprocProcess) pollExit() (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.exited {
		return true, p.exitErr
	}
	select {
	case err := <-p.done:
		p.exited, p.exitErr = true, err
	default:
	}
	return p.exited, p.exitErr
}

func (p *dualprocProcess) waitExit(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if exited, err := p.pollExit(); exited {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s did not exit within %s", p.name, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (p *dualprocProcess) killIfAlive() {
	if exited, _ := p.pollExit(); exited {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGCONT)
	_ = p.cmd.Process.Kill()
	_ = p.waitExit(15 * time.Second)
}

func (p *dualprocProcess) signal(sig syscall.Signal) error {
	return p.cmd.Process.Signal(sig)
}

func (p *dualprocProcess) stdoutText() string {
	body, _ := os.ReadFile(p.stdoutPath)
	return string(body)
}

func (p *dualprocProcess) stderrText() string {
	body, _ := os.ReadFile(p.stderrPath)
	return string(body)
}

// waitOwner waits for the startup identity line and returns the owner id.
func (p *dualprocProcess) waitOwner(t *testing.T, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if exited, err := p.pollExit(); exited {
			t.Fatalf("%s exited before startup (err=%v); stderr tail:\n%s",
				p.name, err, dualprocFileTail(p.stderrPath, 4000))
		}
		if m := dualprocOwnerRe.FindStringSubmatch(p.stdoutText()); m != nil {
			p.owner = m[1]
			return p.owner
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s did not report its owner within %s; stdout=%q stderr tail:\n%s",
		p.name, timeout, p.stdoutText(), dualprocFileTail(p.stderrPath, 4000))
	return ""
}

func dualprocFileTail(path string, n int) string {
	body, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("<read %s: %v>", path, err)
	}
	if len(body) > n {
		body = body[len(body)-n:]
	}
	return string(body)
}

// dualprocLogTotals sums the per-process publish-cycle logs: claimed rows,
// acknowledged rows and observer-published counts. These are the per-process
// attribution evidence.
func dualprocLogTotals(path string) (claimed, acked, published int64) {
	body, _ := os.ReadFile(path)
	text := string(body)
	for _, m := range dualprocCycleRe.FindAllStringSubmatch(text, -1) {
		c, _ := strconv.ParseInt(m[1], 10, 64)
		a, _ := strconv.ParseInt(m[2], 10, 64)
		claimed += c
		acked += a
	}
	for _, m := range dualprocPublishedRe.FindAllStringSubmatch(text, -1) {
		n, _ := strconv.ParseInt(m[1], 10, 64)
		published += n
	}
	return claimed, acked, published
}

// ---------------------------------------------------------------------------
// Event producer (the verifier's authoritative load)
// ---------------------------------------------------------------------------

type dualprocProducer struct {
	pool *pgxpool.Pool
	mu   sync.Mutex
	seq  int
	ids  []uuid.UUID
	err  error
	stop chan struct{}
	done chan struct{}
}

func newDualprocProducer(pool *pgxpool.Pool) *dualprocProducer {
	return &dualprocProducer{pool: pool, stop: make(chan struct{}), done: make(chan struct{})}
}

// appendBatch commits n deposit.observation.created events through the real
// Append path (evm_log identity, unique per event).
func (p *dualprocProducer) appendBatch(n int) error {
	for i := 0; i < n; i++ {
		p.mu.Lock()
		seq := p.seq
		p.seq++
		p.mu.Unlock()

		obsID := fmt.Sprintf("dualproc-obs-%d", seq)
		ev, err := NewEvent(Event{
			EventType:     EventTypeDepositObservationCreated,
			SchemaVersion: SchemaVersionV1,
			IdentityKind:  IdentityKindEVMLog,
			AggregateType: "deposit_observation",
			AggregateID:   obsID,
			Payload: map[string]any{
				"observation_id": obsID,
				"state":          "pending",
			},
			OccurredAt:  time.Now().UTC(),
			ChainID:     dualprocChainID,
			BlockNumber: int64(9_000_000 + seq),
			BlockHash:   fmt.Sprintf("0x%064x", dualprocHashBase+int64(seq)),
			TxHash:      fmt.Sprintf("0x%064x", dualprocHashBase+int64(1_000_000)+int64(seq)),
			LogIndex:    seq,
		})
		if err != nil {
			return fmt.Errorf("build event %d: %w", seq, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		tx, err := p.pool.Begin(ctx)
		if err != nil {
			cancel()
			return fmt.Errorf("begin append %d: %w", seq, err)
		}
		res, err := Append(ctx, tx, ev)
		if err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
			cancel()
			return fmt.Errorf("append %d: %w", seq, err)
		}
		if err := tx.Commit(ctx); err != nil {
			cancel()
			return fmt.Errorf("commit append %d: %w", seq, err)
		}
		cancel()
		p.mu.Lock()
		p.ids = append(p.ids, res.EventID)
		p.mu.Unlock()
	}
	return nil
}

func (p *dualprocProducer) start() {
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(dualprocProducerInterval)
		defer ticker.Stop()
		for {
			select {
			case <-p.stop:
				return
			case <-ticker.C:
				if err := p.appendBatch(dualprocProducerPerTick); err != nil {
					p.setErr(err)
					return
				}
			}
		}
	}()
}

func (p *dualprocProducer) stopProducing() {
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
	<-p.done
}

func (p *dualprocProducer) burst(t *testing.T, n int) {
	t.Helper()
	if err := p.appendBatch(n); err != nil {
		t.Fatalf("producer burst(%d): %v", n, err)
	}
}

func (p *dualprocProducer) setErr(err error) {
	p.mu.Lock()
	if p.err == nil {
		p.err = err
	}
	p.mu.Unlock()
}

func (p *dualprocProducer) firstErr() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *dualprocProducer) eventIDs() []uuid.UUID {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]uuid.UUID(nil), p.ids...)
}

func (p *dualprocProducer) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.ids)
}

// ---------------------------------------------------------------------------
// Claim sampler
// ---------------------------------------------------------------------------

// dualprocContested is one row owned by the killed process at kill time.
type dualprocContested struct {
	id          int64
	preAttempt  int
	expires     time.Time
	clearedAt   time.Time // first observation where the row left owner B
	publishedAt time.Time // first observation of publish_state = published
	postAttempt int
}

// dualprocSampler samples all live claims at a fixed cadence: per-owner
// counts, the simultaneous-both-owners counter, unknown-owner detection and
// the orphaned-claim takeover timeline.
type dualprocSampler struct {
	pool   *pgxpool.Pool
	ownerA string
	ownerB string

	mu           sync.Mutex
	samples      int
	ownerSamples map[string]int
	both         int
	unknown      map[string]int
	contested    map[int64]*dualprocContested
	firstErr     error

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

func newDualprocSampler(pool *pgxpool.Pool, ownerA, ownerB string) *dualprocSampler {
	return &dualprocSampler{
		pool:         pool,
		ownerA:       ownerA,
		ownerB:       ownerB,
		ownerSamples: map[string]int{},
		unknown:      map[string]int{},
		contested:    map[int64]*dualprocContested{},
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
}

func (s *dualprocSampler) run(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(dualprocSampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sample(ctx)
		}
	}
}

func (s *dualprocSampler) stopSampling() {
	s.stopOnce.Do(func() { close(s.stop) })
	<-s.done
}

func (s *dualprocSampler) sample(ctx context.Context) {
	rows, err := s.pool.Query(ctx,
		`SELECT claim_owner, count(*)::bigint FROM outbox_events WHERE claim_owner IS NOT NULL GROUP BY 1`)
	if err != nil {
		if ctx.Err() == nil {
			s.setErr(fmt.Errorf("sample query: %w", err))
		}
		return
	}
	counts := map[string]int{}
	for rows.Next() {
		var owner string
		var n int64
		if err := rows.Scan(&owner, &n); err != nil {
			rows.Close()
			s.setErr(fmt.Errorf("sample scan: %w", err))
			return
		}
		counts[owner] = int(n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		if ctx.Err() == nil {
			s.setErr(fmt.Errorf("sample rows: %w", err))
		}
		return
	}

	s.mu.Lock()
	s.samples++
	for owner, n := range counts {
		if n > 0 {
			s.ownerSamples[owner]++
		}
		if owner != s.ownerA && owner != s.ownerB {
			s.unknown[owner]++
		}
	}
	if counts[s.ownerA] > 0 && counts[s.ownerB] > 0 {
		s.both++
	}
	ids := make([]int64, 0, len(s.contested))
	for id := range s.contested {
		ids = append(ids, id)
	}
	s.mu.Unlock()

	if len(ids) > 0 {
		s.sampleContested(ctx, ids)
	}
}

func (s *dualprocSampler) sampleContested(ctx context.Context, ids []int64) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, publish_state, coalesce(claim_owner, ''), attempt_count
		 FROM outbox_events WHERE id = ANY($1)`, ids)
	if err != nil {
		if ctx.Err() == nil {
			s.setErr(fmt.Errorf("contested query: %w", err))
		}
		return
	}
	defer rows.Close()
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for rows.Next() {
		var id, attempt int64
		var state, owner string
		if err := rows.Scan(&id, &state, &owner, &attempt); err != nil {
			s.setErrHoldLock(fmt.Errorf("contested scan: %w", err))
			return
		}
		c := s.contested[id]
		if c == nil {
			continue
		}
		if owner != s.ownerB && c.clearedAt.IsZero() {
			c.clearedAt = now
		}
		if state == string(PublishStatePublished) {
			c.postAttempt = int(attempt)
			if c.publishedAt.IsZero() {
				c.publishedAt = now
			}
		}
	}
	if err := rows.Err(); err != nil && ctx.Err() == nil {
		s.setErrHoldLock(fmt.Errorf("contested rows: %w", err))
	}
}

func (s *dualprocSampler) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setErrHoldLock(err)
}

// setErrHoldLock records the first sampler error; the caller holds s.mu.
func (s *dualprocSampler) setErrHoldLock(err error) {
	if s.firstErr == nil {
		s.firstErr = err
	}
}

func (s *dualprocSampler) firstError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstErr
}

// trackContested registers the rows owned by the process that is about to be
// killed (the process must already be SIGSTOPped so the set is stable).
func (s *dualprocSampler) trackContested(rows []dualprocClaim) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range rows {
		s.contested[r.id] = &dualprocContested{id: r.id, preAttempt: r.attempt, expires: r.expires}
	}
}

func (s *dualprocSampler) contestedViews() map[int64]dualprocContested {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int64]dualprocContested, len(s.contested))
	for id, c := range s.contested {
		out[id] = *c
	}
	return out
}

type dualprocSamplerSummary struct {
	samples       int
	seenA         int
	seenB         int
	both          int
	unknownOwners int
}

func (s *dualprocSampler) summary() dualprocSamplerSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	return dualprocSamplerSummary{
		samples:       s.samples,
		seenA:         s.ownerSamples[s.ownerA],
		seenB:         s.ownerSamples[s.ownerB],
		both:          s.both,
		unknownOwners: len(s.unknown),
	}
}

// dualprocClaim is one pending claim read from PostgreSQL.
type dualprocClaim struct {
	id      int64
	owner   string
	attempt int
	expires time.Time
}

// dualprocPendingClaims reads the pending claims of one owner.
func dualprocPendingClaims(ctx context.Context, pool *pgxpool.Pool, owner string) ([]dualprocClaim, error) {
	rows, err := pool.Query(ctx,
		`SELECT id, attempt_count, claim_expires_at FROM outbox_events
		 WHERE publish_state = 'pending' AND claim_owner = $1 ORDER BY id`, owner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dualprocClaim
	for rows.Next() {
		var c dualprocClaim
		var expires *time.Time
		if err := rows.Scan(&c.id, &c.attempt, &expires); err != nil {
			return nil, err
		}
		if expires != nil {
			c.expires = *expires
		}
		c.owner = owner
		out = append(out, c)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

// TestPublisherDualProcessSupplement is the T034 supplement acceptance drill:
// two real OS publisher processes, one isolated PG/Kafka pair, mutual
// exclusion, lease-expiry takeover after a kill, final drain, graceful stop
// and exactly-one consumer effect.
func TestPublisherDualProcessSupplement(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	duration := dualprocDuration(t)

	ctx, cancel := context.WithTimeout(context.Background(), duration+7*time.Minute)
	defer cancel()

	dsn := dualprocPostgres(t)
	kafka, err := testutil.StartKafka(ctx)
	if err != nil {
		t.Fatalf("StartKafka: %v", err)
	}
	t.Cleanup(func() { _ = kafka.Close(context.Background()) })
	if err := kafka.EnsureTopic(ctx, testutil.KafkaTopic, testutil.KafkaPartitions); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	pool := dualprocPool(t, dsn)

	dir := t.TempDir()
	bin := dualprocBuildBinary(t, dir)
	lease := dualprocLease(duration)
	env := dualprocPublisherEnv(dsn, kafka.Brokers(), lease)

	producer := newDualprocProducer(pool)
	producer.burst(t, dualprocInitialSeed)

	// Two real OS processes with independent identities.
	procA := dualprocStartProcess(t, bin, env, dir, "publisher-a")
	ownerA := procA.waitOwner(t, 3*time.Minute)
	procB := dualprocStartProcess(t, bin, env, dir, "publisher-b")
	ownerB := procB.waitOwner(t, 3*time.Minute)
	if ownerA == ownerB {
		t.Fatalf("both processes report the same owner id %q; identities must be independent", ownerA)
	}

	sampler := newDualprocSampler(pool, ownerA, ownerB)
	go sampler.run(ctx)
	t.Cleanup(sampler.stopSampling)
	producer.start()
	t.Cleanup(producer.stopProducing)

	ev := &dualprocEvidence{}
	ev.add(t, "scope=single-host; publisher_processes=2; duration=%s; lease=%s; batch=%d; poll=%s",
		duration, lease, dualprocBatch, dualprocPollInterval)
	ev.add(t, "identity A: owner=%s pid=%d log=%s", ownerA, procA.pid, filepath.Base(procA.stdoutPath))
	ev.add(t, "identity B: owner=%s pid=%d log=%s", ownerB, procB.pid, filepath.Base(procB.stdoutPath))

	// Phase 1: both processes must actually claim and publish before any
	// failure is injected. The per-process cycle logs are the attribution.
	dualprocWaitBothPublish(t, procA, procB, producer, sampler)
	claimedA, ackedA, pubA := dualprocLogTotals(procA.stderrPath)
	claimedB, ackedB, pubB := dualprocLogTotals(procB.stderrPath)
	ev.add(t, "phase=both-publishing A(claimed=%d acked=%d observer_published=%d) B(claimed=%d acked=%d observer_published=%d)",
		claimedA, ackedA, pubA, claimedB, ackedB, pubB)

	// Phase 2 window: keep both processes publishing until freezeAt.
	lead := lease + 45*time.Second
	if lead > duration/3 {
		lead = duration / 3
	}
	start := time.Now()
	freezeAt := start.Add(duration - lead)
	dualprocWaitUntil(t, freezeAt, sampler, procA, procB)

	// Phase 3: freeze B while it owns claims, capture the simultaneous
	// partition snapshot, then kill B (crash) and observe lease-expiry
	// takeover by A.
	contested, partitionA, partitionB := dualprocFreezeAndCapturePartition(t, pool, producer, procA, procB)
	ev.add(t, "phase=partition-snapshot ownerA_claims=%d ownerB_claims=%d (disjoint rows, both owners pending simultaneously)",
		len(partitionA), len(partitionB))
	for _, c := range partitionA {
		ev.add(t, "partition owner=%s row=%d attempt=%d expires=%s", ownerA, c.id, c.attempt, c.expires.UTC().Format(time.RFC3339Nano))
	}
	for _, c := range partitionB {
		ev.add(t, "partition owner=%s row=%d attempt=%d expires=%s", ownerB, c.id, c.attempt, c.expires.UTC().Format(time.RFC3339Nano))
	}
	sampler.trackContested(contested)
	if err := procB.signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL B: %v", err)
	}
	if err := procB.waitExit(30 * time.Second); err == nil {
		t.Fatalf("killed process B exited without an error; want the crash signal")
	}
	ev.add(t, "phase=crash-kill owner=%s pid=%d contested_rows=%d killed=true", ownerB, procB.pid, len(contested))

	dualprocWaitTakeover(t, ctx, pool, sampler, contested)
	views := sampler.contestedViews()
	for _, c := range contested {
		v := views[c.id]
		ev.add(t, "takeover row=%d pre_attempt=%d post_attempt=%d cleared_at=%s published_at=%s expires=%s",
			c.id, v.preAttempt, v.postAttempt,
			v.clearedAt.UTC().Format(time.RFC3339Nano), v.publishedAt.UTC().Format(time.RFC3339Nano),
			v.expires.UTC().Format(time.RFC3339Nano))
	}

	// Phase 4: the survivor keeps publishing until the window ends, then the
	// verifier stops the load and the survivor drains.
	dualprocWaitUntil(t, start.Add(duration), sampler, procA)
	producer.stopProducing()
	if err := producer.firstErr(); err != nil {
		t.Fatalf("producer failed: %v", err)
	}
	dualprocDrain(t, ctx, pool, duration)
	ev.add(t, "phase=drained pending=0 blocked=0 unowned_claims=0")

	// Graceful stop of the surviving process: SIGTERM -> clean exit 0.
	if err := procA.signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM A: %v", err)
	}
	if err := procA.waitExit(90 * time.Second); err != nil {
		t.Fatalf("process A did not stop cleanly: %v; stderr tail:\n%s", err, dualprocFileTail(procA.stderrPath, 4000))
	}
	if !strings.Contains(procA.stdoutText(), "stopped") {
		t.Fatalf("process A printed no clean-stop line; stdout=%q stderr tail:\n%s",
			procA.stdoutText(), dualprocFileTail(procA.stderrPath, 4000))
	}
	ev.add(t, "phase=graceful-stop owner=%s pid=%d exit=0 stdout=stopped", ownerA, procA.pid)

	sampler.stopSampling()
	if err := sampler.firstError(); err != nil {
		t.Fatalf("sampler observed an error: %v", err)
	}
	summary := sampler.summary()
	if summary.unknownOwners > 0 {
		t.Fatalf("sampler observed %d unknown claim owner(s); only the two drill processes may own rows", summary.unknownOwners)
	}
	if summary.seenA == 0 || summary.seenB == 0 {
		t.Fatalf("sampler saw owner samples A=%d B=%d; both processes must be observed holding claims", summary.seenA, summary.seenB)
	}
	if summary.both == 0 {
		t.Fatalf("sampler never observed both owners holding claims at the same instant (samples=%d)", summary.samples)
	}
	ev.add(t, "mutual-exclusion samples=%d ownerA_seen=%d ownerB_seen=%d both_simultaneous=%d unknown_owners=0",
		summary.samples, summary.seenA, summary.seenB, summary.both)

	// Phase 5: final outbox state. Every row published exactly once (published
	// is a terminal state); no pending/blocked/unowned claims remain.
	expected := int64(producer.count())
	var total, published, pending, blocked, attempts, owned int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE publish_state = 'published'),
		       count(*) FILTER (WHERE publish_state = 'pending'),
		       count(*) FILTER (WHERE publish_state = 'blocked'),
		       coalesce(sum(attempt_count), 0),
		       count(claim_owner)
		FROM outbox_events`).
		Scan(&total, &published, &pending, &blocked, &attempts, &owned); err != nil {
		t.Fatalf("final outbox state: %v", err)
	}
	if total != expected {
		t.Fatalf("outbox rows = %d, want %d (one per appended event)", total, expected)
	}
	if published != expected || pending != 0 || blocked != 0 || owned != 0 {
		t.Fatalf("final state published=%d pending=%d blocked=%d owned=%d, want %d/0/0/0",
			published, pending, blocked, owned, expected)
	}
	if attempts < expected {
		t.Fatalf("sum(attempt_count) = %d < %d; every row must be claimed at least once", attempts, expected)
	}
	ev.add(t, "final-outbox events=%d published=%d pending=0 blocked=0 attempts=%d", expected, published, attempts)

	// Per-process attribution cross-check: each process's own logs must show
	// claims and acks, and the logs must account for the durable counters
	// within one batch (a process killed mid-cycle logs at most one batch
	// less than it durably claimed).
	claimedA, ackedA, pubA = dualprocLogTotals(procA.stderrPath)
	claimedB, ackedB, pubB = dualprocLogTotals(procB.stderrPath)
	if claimedA == 0 || ackedA == 0 || claimedB == 0 || ackedB == 0 {
		t.Fatalf("per-process publish evidence missing: A(claimed=%d acked=%d) B(claimed=%d acked=%d)",
			claimedA, ackedA, claimedB, ackedB)
	}
	if claimedA+claimedB > attempts || attempts > claimedA+claimedB+dualprocBatch {
		t.Fatalf("logged claims (A=%d + B=%d) do not account for durable attempts %d within one batch",
			claimedA, claimedB, attempts)
	}
	if ackedA+ackedB > published || published > ackedA+ackedB+dualprocBatch {
		t.Fatalf("logged acks (A=%d + B=%d) do not account for published rows %d within one batch",
			ackedA, ackedB, published)
	}
	ev.add(t, "attribution A(claimed=%d acked=%d published=%d) B(claimed=%d acked=%d published=%d) durable_attempts=%d published_rows=%d",
		claimedA, ackedA, pubA, claimedB, ackedB, pubB, attempts, published)

	// Contested rows: takeover by the survivor only after the lease expired,
	// with the attempt counter advanced.
	for _, c := range contested {
		v := views[c.id]
		if v.postAttempt < v.preAttempt+1 {
			t.Fatalf("contested row %d attempt %d -> %d; the surviving owner must reclaim it (attempt +1)",
				c.id, v.preAttempt, v.postAttempt)
		}
		if v.clearedAt.IsZero() || v.publishedAt.IsZero() {
			t.Fatalf("contested row %d has no observed takeover timeline", c.id)
		}
		if v.clearedAt.Before(v.expires.Add(-1 * time.Second)) {
			t.Fatalf("contested row %d left its owner at %s, before lease expiry %s: the live lease was stolen",
				c.id, v.clearedAt, v.expires)
		}
		if v.publishedAt.Before(v.expires.Add(-1 * time.Second)) {
			t.Fatalf("contested row %d was published at %s, before lease expiry %s",
				c.id, v.publishedAt, v.expires)
		}
	}
	ev.add(t, "lease-takeover verified: %d orphaned claims reclaimed only after claim_expires_at, all published", len(contested))

	// Phase 6: consumption effect. Every event reaches the broker; the
	// reference consumer's persistent inbox yields exactly one simulated
	// ledger row per event (no duplicate payment-side effect). The reference
	// consumer boundary statement applies verbatim.
	want := map[uuid.UUID]bool{}
	for _, id := range producer.eventIDs() {
		want[id] = true
	}
	stats := dualprocConsumeAndApply(t, ctx, pool, kafka.Brokers(), want)
	ev.add(t, "consume records=%d applied=%d duplicates_absorbed=%d expected_events=%d",
		stats.records, stats.applied, stats.duplicates, len(want))
	ev.add(t, "effect ledger_rows_per_event=1 for all %d events (min=%d max=%d); upstream_payment_rows=0",
		len(want), stats.minEffect, stats.maxEffect)
	ev.add(t, "boundary %s", ReferenceBoundaryStatement)
	ev.writeFile(t)
}

// dualprocWaitBothPublish blocks until both processes' own logs prove they
// claimed and acknowledged at least one record.
func dualprocWaitBothPublish(t *testing.T, procA, procB *dualprocProcess, producer *dualprocProducer, sampler *dualprocSampler) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		claimedA, ackedA, _ := dualprocLogTotals(procA.stderrPath)
		claimedB, ackedB, _ := dualprocLogTotals(procB.stderrPath)
		if claimedA > 0 && ackedA > 0 && claimedB > 0 && ackedB > 0 {
			return
		}
		for _, p := range []*dualprocProcess{procA, procB} {
			if exited, err := p.pollExit(); exited {
				t.Fatalf("%s exited before proving a claim+publish: %v; stderr tail:\n%s",
					p.name, err, dualprocFileTail(p.stderrPath, 4000))
			}
		}
		if err := sampler.firstError(); err != nil {
			t.Fatalf("sampler observed an error: %v", err)
		}
		producer.burst(t, 100)
		time.Sleep(300 * time.Millisecond)
	}
	claimedAtFail, ackedAtFail, _ := dualprocLogTotals(procA.stderrPath)
	t.Fatalf("both processes did not claim and publish within the window; A(claimed=%d acked=%d) stderr tail:\n%s\nB stderr tail:\n%s",
		claimedAtFail, ackedAtFail, dualprocFileTail(procA.stderrPath, 3000), dualprocFileTail(procB.stderrPath, 3000))
}

// dualprocWaitUntil sleeps until deadline while checking process health and
// the sampler.
func dualprocWaitUntil(t *testing.T, deadline time.Time, sampler *dualprocSampler, procs ...*dualprocProcess) {
	t.Helper()
	for time.Now().Before(deadline) {
		for _, p := range procs {
			if exited, err := p.pollExit(); exited {
				t.Fatalf("%s exited prematurely: %v; stderr tail:\n%s",
					p.name, err, dualprocFileTail(p.stderrPath, 4000))
			}
		}
		if err := sampler.firstError(); err != nil {
			t.Fatalf("sampler observed an error: %v", err)
		}
		sleep := time.Second
		if remaining := time.Until(deadline); remaining < sleep {
			sleep = remaining
		}
		if sleep > 0 {
			time.Sleep(sleep)
		}
	}
}

// dualprocFreezeAndCapturePartition stops process B while it owns committed
// pending claims, then captures the simultaneous two-owner partition. To make
// B's short claim window observable, A is SIGSTOPped first (B becomes the only
// drainer), B is stop-polled until it owns claims, and only then is A resumed
// to claim concurrently against the frozen B. B stays frozen on return; A is
// running again.
func dualprocFreezeAndCapturePartition(t *testing.T, pool *pgxpool.Pool, producer *dualprocProducer,
	procA, procB *dualprocProcess) ([]dualprocClaim, []dualprocClaim, []dualprocClaim) {
	t.Helper()
	ctx := context.Background()

	// A stops draining: every burst stays claimable by B, whose claim cycles
	// (batch rows held across Sink.Publish) become observable at a 3ms poll.
	if err := procA.signal(syscall.SIGSTOP); err != nil {
		t.Fatalf("SIGSTOP A: %v", err)
	}
	aResumed := false
	resumeA := func() {
		if !aResumed {
			if err := procA.signal(syscall.SIGCONT); err != nil {
				t.Fatalf("SIGCONT A: %v", err)
			}
			aResumed = true
		}
	}
	defer resumeA()

	var contested []dualprocClaim
attempts:
	for attempt := 1; attempt <= 12; attempt++ {
		producer.burst(t, dualprocFreezeBurst)
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			bClaims, err := dualprocPendingClaims(ctx, pool, procB.owner)
			if err != nil {
				t.Fatalf("read B claims: %v", err)
			}
			if len(bClaims) == 0 {
				time.Sleep(3 * time.Millisecond)
				continue
			}
			if err := procB.signal(syscall.SIGSTOP); err != nil {
				t.Fatalf("SIGSTOP B: %v", err)
			}
			time.Sleep(30 * time.Millisecond)
			stable, err := dualprocPendingClaims(ctx, pool, procB.owner)
			if err != nil {
				t.Fatalf("re-read B claims: %v", err)
			}
			if len(stable) == 0 {
				// B completed its cycle before the stop landed; retry.
				if err := procB.signal(syscall.SIGCONT); err != nil {
					t.Fatalf("SIGCONT B: %v", err)
				}
				break
			}
			contested = stable
			break attempts
		}
	}
	if len(contested) == 0 {
		t.Fatalf("could not freeze process B while it owns pending claims after 12 attempts")
	}

	// A resumes against the frozen B: the two owners now hold disjoint claim
	// sets at the same instant (the partition snapshot). Both are frozen
	// briefly so the snapshot and the sampler observe one common instant.
	resumeA()
	var partitionA, partitionB []dualprocClaim
	for attempt := 1; attempt <= 4 && len(partitionA) == 0; attempt++ {
		producer.burst(t, dualprocFreezeBurst)
		deadline := time.Now().Add(2 * time.Second)
		foundA := false
		for time.Now().Before(deadline) {
			aClaims, err := dualprocPendingClaims(ctx, pool, procA.owner)
			if err != nil {
				t.Fatalf("read A claims: %v", err)
			}
			if len(aClaims) > 0 {
				foundA = true
				break
			}
			time.Sleep(3 * time.Millisecond)
		}
		if !foundA {
			continue
		}
		if err := procA.signal(syscall.SIGSTOP); err != nil {
			t.Fatalf("SIGSTOP A: %v", err)
		}
		time.Sleep(80 * time.Millisecond)
		aFrozen, err := dualprocPendingClaims(ctx, pool, procA.owner)
		if err != nil {
			t.Fatalf("frozen A claims: %v", err)
		}
		bFrozen, err := dualprocPendingClaims(ctx, pool, procB.owner)
		if err != nil {
			t.Fatalf("frozen B claims: %v", err)
		}
		if len(aFrozen) > 0 && len(bFrozen) > 0 {
			partitionA, partitionB = aFrozen, bFrozen
		}
		if err := procA.signal(syscall.SIGCONT); err != nil {
			t.Fatalf("SIGCONT A: %v", err)
		}
	}
	if len(partitionA) == 0 {
		t.Fatalf("process A held no claims while B was frozen; no simultaneous partition snapshot")
	}
	if len(partitionB) == 0 {
		t.Fatalf("process B lost its frozen claims before the partition snapshot completed")
	}
	return contested, partitionA, partitionB
}

// dualprocWaitTakeover waits until every orphaned claim is published and
// asserts each left its dead owner only after the lease expired.
func dualprocWaitTakeover(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sampler *dualprocSampler, contested []dualprocClaim) {
	t.Helper()
	ids := make([]int64, 0, len(contested))
	for _, c := range contested {
		ids = append(ids, c.id)
	}
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		var pending int64
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM outbox_events WHERE id = ANY($1) AND publish_state = 'pending'`, ids).
			Scan(&pending); err != nil {
			t.Fatalf("count orphaned claims: %v", err)
		}
		if pending == 0 {
			return
		}
		if err := sampler.firstError(); err != nil {
			t.Fatalf("sampler observed an error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("orphaned claims were not taken over and published within the deadline")
}

// dualprocDrain waits until the outbox has no pending or blocked rows and no
// live claim.
func dualprocDrain(t *testing.T, ctx context.Context, pool *pgxpool.Pool, duration time.Duration) {
	t.Helper()
	budget := duration / 5
	if budget < 90*time.Second {
		budget = 90 * time.Second
	}
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		var pending, blocked, owned int64
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE publish_state = 'pending'),
			       count(*) FILTER (WHERE publish_state = 'blocked'),
			       count(claim_owner)
			FROM outbox_events`).Scan(&pending, &blocked, &owned); err != nil {
			t.Fatalf("drain query: %v", err)
		}
		if pending == 0 && blocked == 0 && owned == 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("outbox did not drain within %s", budget)
}

// dualprocConsumeStats is the consumption-effect evidence.
type dualprocConsumeStats struct {
	records    int64
	applied    int64
	duplicates int64
	minEffect  int64
	maxEffect  int64
}

// dualprocConsumeAndApply consumes the whole topic from the start, processes
// every record through the reference consumer (persistent inbox idempotency)
// and asserts exactly one simulated ledger row per event. It returns the
// stats; every assertion failure is fatal.
func dualprocConsumeAndApply(t *testing.T, ctx context.Context, pool *pgxpool.Pool, brokers []string, want map[uuid.UUID]bool) dualprocConsumeStats {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(testutil.KafkaTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("kafka consumer: %v", err)
	}
	defer cl.Close()

	reference, err := NewReferenceConsumer(pool, ConsumerOptions{
		GapWait:     500 * time.Millisecond,
		BackoffBase: 10 * time.Millisecond,
		BackoffMax:  100 * time.Millisecond,
		RetryLimit:  4,
		ChainID:     dualprocChainID,
		Jitter:      func() float64 { return 0 },
	})
	if err != nil {
		t.Fatalf("NewReferenceConsumer: %v", err)
	}
	if err := reference.EnsureLedgerSchema(ctx); err != nil {
		t.Fatalf("EnsureLedgerSchema: %v", err)
	}

	seen := map[uuid.UUID]int64{}
	stats := dualprocConsumeStats{}
	deadline := time.Now().Add(10 * time.Minute)
	quietSince := time.Now()
	for {
		allSeen := true
		for id := range want {
			if seen[id] == 0 {
				allSeen = false
				break
			}
		}
		if allSeen && time.Since(quietSince) > 2*time.Second {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("consume timeout: saw %d/%d expected events (%d records)", len(seen), len(want), stats.records)
		}
		fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := cl.PollFetches(fetchCtx)
		cancel()
		fetches.EachError(func(topic string, partition int32, err error) {
			if err != context.DeadlineExceeded && ctx.Err() == nil {
				t.Fatalf("PollFetches(%s/%d): %v", topic, partition, err)
			}
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			stats.records++
			id := dualprocRecordEventID(rec)
			if id == uuid.Nil {
				t.Fatalf("record %s/%d@%d carries no event_id header", rec.Topic, rec.Partition, rec.Offset)
			}
			if !want[id] {
				t.Fatalf("unexpected event id %s on the topic", id)
			}
			if seen[id] == 0 {
				quietSince = time.Now()
			}
			seen[id]++
			result, err := reference.Process(ctx, Message{
				Topic: rec.Topic, Partition: int(rec.Partition), Offset: rec.Offset, Value: rec.Value,
			})
			if err != nil {
				t.Fatalf("reference consumer Process(%s): %v", id, err)
			}
			switch result.Outcome {
			case OutcomeApplied:
				stats.applied++
			case OutcomeDuplicate:
				stats.duplicates++
			default:
				t.Fatalf("event %s outcome = %s (reason=%s); want applied or duplicate",
					id, result.Outcome, result.Reason)
			}
		})
	}
	if stats.applied != int64(len(want)) {
		t.Fatalf("applied outcomes = %d, want %d (each event applied once)", stats.applied, len(want))
	}

	// Exactly one simulated ledger row per event: the effective application
	// count is 1 under duplicates/replay.
	stats.minEffect = int64(len(want)) + 1
	for id := range want {
		n, err := reference.LedgerApplications(ctx, id)
		if err != nil {
			t.Fatalf("LedgerApplications(%s): %v", id, err)
		}
		if n != 1 {
			t.Fatalf("event %s ledger rows = %d, want exactly 1", id, n)
		}
		if n < stats.minEffect {
			stats.minEffect = n
		}
		if n > stats.maxEffect {
			stats.maxEffect = n
		}
	}
	if stats.minEffect != 1 || stats.maxEffect != 1 {
		t.Fatalf("ledger rows per event = [%d..%d], want exactly 1", stats.minEffect, stats.maxEffect)
	}

	// Zero payment-side-effect rows: this drill never reaches 007/008/009/
	// 010/011.
	for _, table := range dualprocUpstreamTables {
		var n int64
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if n != 0 {
			t.Fatalf("%s has %d rows; the drill must produce zero payment-side effects", table, n)
		}
	}
	return stats
}

// dualprocRecordEventID extracts the event_id header written by the publisher.
func dualprocRecordEventID(rec *kgo.Record) uuid.UUID {
	for _, h := range rec.Headers {
		if h.Key == "event_id" {
			if id, err := uuid.Parse(string(h.Value)); err == nil {
				return id
			}
			return uuid.Nil
		}
	}
	return uuid.Nil
}
