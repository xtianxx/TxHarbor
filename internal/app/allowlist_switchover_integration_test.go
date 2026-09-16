//go:build integration

// allowlist_switchover_integration_test.go owns spec task T043
// (012-007-authorization-carrier): the R-PB10 §1–5 allowlist switchover
// rehearsal, run against a real scratch PostgreSQL 18 container (testcontainers)
// and the real T013 CLI carrier.
//
// R-PB10 §1–5 (no hot reload, no key revocation):
//  1. editing the config file is not effective — halt new supply invocations
//     (entry control; concurrent CLI invocations all covered);
//  2. drain: let running invocations exit, or terminate them and confirm via
//     process table + pg_stat_activity that no supply tx remains open (an
//     idle-in-transaction from a killed CLI rolls back on disconnect);
//  3. atomically swap the config (rename, not in-place edit) and record its
//     checksum + swap timestamp as the verifiable effective point;
//  4. re-enable the entry only after (2)-(3) are confirmed — a new invocation
//     then loads the new mapping by construction;
//  5. failure at any step keeps the entry closed; never declare the switchover
//     complete while an old executor is unaccounted for.
//
// Precondition: every host that can invoke supply must swap its config
// atomically; a host that cannot meet (1)-(5) is a deployment gap, not waved
// through. The rehearsal swaps every declared host config file and reports a
// replica gap when only the scratch single host is present.
package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// switchoverSupplyApp tags the rehearsal's simulated supply executors so the
// drain check can find them in pg_stat_activity. A real deployment identifies
// supply serializers by its own process/connection naming.
const switchoverSupplyApp = "txharbor-supply"

// switchoverDeployment models one host's deployment env file for the issuance
// allowlist. getenv re-reads the file on every call, which is exactly the
// R-PB10 model: supply runs as short-lived CLI processes, each loading config at
// start, so a swapped file is observed by the next invocation by construction.
type switchoverDeployment struct {
	path string
	base map[string]string
}

func newSwitchoverDeployment(t *testing.T, path string, base map[string]string) *switchoverDeployment {
	t.Helper()
	return &switchoverDeployment{path: path, base: base}
}

// writeConfig writes content in place (setup only; the effective swap is the
// atomic rename in switchoverSwap).
func (d *switchoverDeployment) writeConfig(t *testing.T, content string) {
	t.Helper()
	if err := os.WriteFile(d.path, []byte(content), 0o600); err != nil {
		t.Fatalf("write deployment config %s: %v", d.path, err)
	}
}

func (d *switchoverDeployment) getenv() func(string) (string, bool) {
	return func(key string) (string, bool) {
		if key == withdrawal.EnvIssuerCallers {
			raw, err := os.ReadFile(d.path)
			if err != nil {
				return "", false
			}
			return switchoverFileValue(string(raw), key)
		}
		v, ok := d.base[key]
		return v, ok
	}
}

// switchoverFileValue extracts KEY=value from a deployment env file; absent is
// reported as not-set, which LoadIssuerAllowlist treats as deny-all.
func switchoverFileValue(raw, key string) (string, bool) {
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if ok && strings.TrimSpace(k) == key {
			return strings.TrimSpace(v), true
		}
	}
	return "", false
}

// switchoverEntry is the operational entry control of R-PB10 §1/§4: while
// halted, no new supply invocation is admitted; it is reopened only after the
// drain and swap are confirmed.
type switchoverEntry struct {
	closed   bool
	refusals int
}

// supply admits one supply invocation through the entry and drives the real
// T013 carrier when the entry is open.
func (e *switchoverEntry) supply(ctx context.Context, d *switchoverDeployment, args ...string) (int, string, string) {
	if e.closed {
		e.refusals++
		return 1, "", "entry halted: no new supply invocations admitted (R-PB10 §1)"
	}
	var stdout, stderr bytes.Buffer
	code := WithdrawalAuthz(ctx, args, Deps{
		Getenv: d.getenv(),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

// switchoverSwap performs the atomic config swap (R-PB10 §3): write the new
// content to a temp file in the same directory, fsync it, then rename over the
// live path. It returns the read-back checksum and the recorded timestamp of the
// effective point.
func switchoverSwap(t *testing.T, host, content string) (checksum string, ts time.Time, err error) {
	t.Helper()
	dir := filepath.Dir(host)
	tmp, err := os.CreateTemp(dir, ".switchover-*.tmp")
	if err != nil {
		return "", time.Time{}, fmt.Errorf("create temp config for %s: %w", host, err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.WriteString(content); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return "", time.Time{}, fmt.Errorf("write temp config: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return "", time.Time{}, fmt.Errorf("fsync temp config: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return "", time.Time{}, fmt.Errorf("close temp config: %w", err)
	}
	if err := os.Rename(tmpName, host); err != nil {
		os.Remove(tmpName)
		return "", time.Time{}, fmt.Errorf("atomic rename onto %s: %w", host, err)
	}
	back, err := os.ReadFile(host)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("read back swapped config %s: %w", host, err)
	}
	sum := sha256.Sum256(back)
	return hex.EncodeToString(sum[:]), time.Now().UTC(), nil
}

// switchoverSupplyPIDs reports the PIDs of open supply transactions: backends on
// this database tagged as supply whose transaction is not closed (a killed CLI
// leaves nothing behind — disconnect rolls the tx back, which is the drain
// signal). No sleeps as proof: the state is read from pg_stat_activity.
func switchoverSupplyPIDs(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[int32]struct{} {
	t.Helper()
	rows, err := pool.Query(ctx, `
SELECT pid FROM pg_stat_activity
WHERE datname = current_database()
  AND application_name = $1
  AND state <> 'idle'`, switchoverSupplyApp)
	if err != nil {
		t.Fatalf("poll pg_stat_activity for supply tx: %v", err)
	}
	defer rows.Close()
	out := make(map[int32]struct{})
	for rows.Next() {
		var pid int32
		if err := rows.Scan(&pid); err != nil {
			t.Fatalf("scan supply pid: %v", err)
		}
		out[pid] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate supply pids: %v", err)
	}
	return out
}

// switchoverWaitDrained polls pg_stat_activity until no supply tx is open, or
// the timeout elapses. Declared PIDs are the process-table accounting: an open
// backend whose PID is not declared is an UNACCOUNTED old executor (R-PB10 §5)
// and is named in the failure.
func switchoverWaitDrained(t *testing.T, ctx context.Context, pool *pgxpool.Pool, declared []int32, timeout time.Duration) error {
	t.Helper()
	declaredSet := make(map[int32]struct{}, len(declared))
	for _, pid := range declared {
		declaredSet[pid] = struct{}{}
	}
	deadline := time.Now().Add(timeout)
	var last map[int32]struct{}
	for {
		last = switchoverSupplyPIDs(t, ctx, pool)
		if len(last) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			var unaccounted []int32
			for pid := range last {
				if _, ok := declaredSet[pid]; !ok {
					unaccounted = append(unaccounted, pid)
				}
			}
			return fmt.Errorf("drain not confirmed: %d supply tx still open, unaccounted pids=%v", len(last), unaccounted)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// switchoverOutcome is the rehearsal's recorded evidence.
type switchoverOutcome struct {
	Declared      bool
	EntryClosed   bool
	FailureStep   string
	ReplicaGap    string
	Checksums     map[string]string
	HostsSwapped  []string
	SwapTimestamp time.Time
}

// runSwitchoverRehearsal executes R-PB10 §1–5 against the declared host config
// files. It halts the entry, confirms drain, atomically swaps every host, and
// only then reopens the entry; any failure leaves the entry closed.
func runSwitchoverRehearsal(ctx context.Context, t *testing.T, pool *pgxpool.Pool, entry *switchoverEntry, hosts []string, newConfig string, declared []int32, drainTimeout time.Duration) (switchoverOutcome, error) {
	t.Helper()

	// §1 — halt the entry.
	entry.closed = true
	out := switchoverOutcome{EntryClosed: true, Checksums: make(map[string]string, len(hosts))}

	// §2 — drain, confirmed by pg_stat_activity against the process table.
	if err := switchoverWaitDrained(t, ctx, pool, declared, drainTimeout); err != nil {
		out.FailureStep = "drain"
		return out, err
	}

	// §3 — atomic per-host config swap with recorded checksum/timestamp.
	for _, host := range hosts {
		checksum, ts, err := switchoverSwap(t, host, newConfig)
		if err != nil {
			out.FailureStep = "swap"
			out.ReplicaGap = fmt.Sprintf("host %s did not swap: %v", host, err)
			return out, err
		}
		out.Checksums[host] = checksum
		out.HostsSwapped = append(out.HostsSwapped, host)
		if ts.After(out.SwapTimestamp) {
			out.SwapTimestamp = ts
		}
	}

	// §4 — re-enable only after (2)-(3) are confirmed.
	entry.closed = false
	out.EntryClosed = false
	out.Declared = true
	return out, nil
}

// switchoverScopedSupplyArgs builds one scoped supply invocation by the issuer
// whose credential is stored at keyPath. A scoped supply is the only path that
// consults the allowlist; the file input keeps the key out of argv, so a real
// switchover command line carries no secret.
func switchoverScopedSupplyArgs(opID, authID, keyPath string, callerID int64) []string {
	return []string{
		"supply",
		"--operation-id", opID,
		"--authorization-id", authID,
		"--api-key-file", keyPath,
		"--caller-id", strconv.FormatInt(callerID, 10),
		"--chain-id", "31337",
		"--asset", "0x1111111111111111111111111111111111111111",
		"--recipient", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"--amount", "100",
		"--operator", "op-switchover",
		"--reason", "switchover rehearsal",
		"--intent-id", "intent-" + authID,
		"--request-id", "request-" + authID,
		"--sender", "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"--fee-max-total", "21000",
		"--fee-max-per-gas", "2",
		"--fee-max-priority", "1",
		"--allows-fee-replacement",
	}
}

// switchoverKeyFile writes one credential to a 0600 file for the argv-free
// supply invocations.
func switchoverKeyFile(t *testing.T, key, name string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(key+"\n"), 0o600); err != nil {
		t.Fatalf("write key file %s: %v", name, err)
	}
	return path
}

// switchoverOpenSupplyTx opens a real PG backend that has BEGIN'd and executed
// the first supply-tx authority read (R-PB10 fixed order), leaving it idle in
// transaction: a live in-flight supply executor. Close() disconnects it, which
// rolls the tx back — the drain signal.
func switchoverOpenSupplyTx(t *testing.T, ctx context.Context, dsn, appName, apiKey string) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.RuntimeParams["application_name"] = appName
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect executor backend: %v", err)
	}
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		conn.Close(ctx)
		t.Fatalf("begin executor tx: %v", err)
	}
	if _, err := conn.Exec(ctx,
		`SELECT caller_id FROM api_key WHERE key_hash = $1 AND (revoked_at IS NULL OR revoked_at > now()) FOR SHARE`,
		withdrawal.HashKey(apiKey)); err != nil {
		conn.Close(ctx)
		t.Fatalf("executor authority read: %v", err)
	}
	return conn
}

// TestAllowlistSwitchoverRehearsal is T043. It rehearses the happy path with a
// two-host replica config: halt -> drain confirm -> atomic per-host swap with
// checksums -> re-enable -> old principal refused, new principal allowed.
func TestAllowlistSwitchoverRehearsal(t *testing.T) {
	dsn := startConfirmAuthPostgres(t)
	ctx := context.Background()

	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	const (
		oldCaller = int64(8401)
		newCaller = int64(8402)
	)
	oldKey, _, err := withdrawal.IssueKey(ctx, pool, oldCaller, "switchover-old")
	if err != nil {
		t.Fatalf("issue old key: %v", err)
	}
	newKey, _, err := withdrawal.IssueKey(ctx, pool, newCaller, "switchover-new")
	if err != nil {
		t.Fatalf("issue new key: %v", err)
	}
	oldKeyFile := switchoverKeyFile(t, oldKey, "old.key")
	newKeyFile := switchoverKeyFile(t, newKey, "new.key")

	base := withdrawalAuthzEnv(dsn)
	oldConfig := fmt.Sprintf("%s=%d\n", withdrawal.EnvIssuerCallers, oldCaller)
	newConfig := fmt.Sprintf("%s=%d\n", withdrawal.EnvIssuerCallers, newCaller)

	dir := t.TempDir()
	hosts := []string{filepath.Join(dir, "host1.env"), filepath.Join(dir, "host2.env")}
	for _, host := range hosts {
		if err := os.WriteFile(host, []byte(oldConfig), 0o600); err != nil {
			t.Fatalf("write config %s: %v", host, err)
		}
	}
	// Both hosts share one deployment model (scratch); the file path varies.
	d := newSwitchoverDeployment(t, hosts[0], base)
	// Point host2's deployment at its own file too, exercising a second host.
	d2 := newSwitchoverDeployment(t, hosts[1], base)
	entry := &switchoverEntry{}
	oldSupply := func(authID string) (int, string, string) {
		return entry.supply(ctx, d, switchoverScopedSupplyArgs(newOpID(t), authID, oldKeyFile, oldCaller)...)
	}

	// Precondition: with the entry open, the OLD principal is allowed.
	if code, _, stderr := oldSupply("switchover-pre-old"); code != 0 {
		t.Fatalf("pre-swap old-principal supply exit = %d, want 0; stderr=%s", code, stderr)
	}

	outcome, err := runSwitchoverRehearsal(ctx, t, pool, entry, hosts, newConfig, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("rehearsal = %+v, error = %v", outcome, err)
	}
	if !outcome.Declared || outcome.EntryClosed {
		t.Fatalf("outcome = %+v, want declared with entry open", outcome)
	}
	if len(outcome.HostsSwapped) != len(hosts) {
		t.Fatalf("hosts swapped = %v, want every declared host %v (per-host replica precondition)", outcome.HostsSwapped, hosts)
	}
	if outcome.SwapTimestamp.IsZero() {
		t.Fatal("swap timestamp was not recorded")
	}
	for _, host := range hosts {
		back, err := os.ReadFile(host)
		if err != nil {
			t.Fatalf("read swapped config %s: %v", host, err)
		}
		sum := sha256.Sum256(back)
		if got := hex.EncodeToString(sum[:]); got != outcome.Checksums[host] {
			t.Fatalf("host %s checksum = %s, recorded = %s", host, got, outcome.Checksums[host])
		}
		if string(back) != newConfig {
			t.Fatalf("host %s content = %q, want %q", host, back, newConfig)
		}
	}
	// Atomicity observable effect: no temp file left behind.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read config dir: %v", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".switchover-") {
			t.Fatalf("temp swap file %q left behind (swap must be an atomic rename)", e.Name())
		}
	}
	// Per-host replica precondition: single-host scratch reports a gap.
	t.Logf("replica precondition: %d host config(s) swapped atomically; multi-host replicas outside the scratch rehearsal are a deployment gap (R-PB10 precondition)", len(hosts))

	// Post-swap: OLD principal refused by the new mapping, NEW principal allowed.
	// The old principal is a fresh invocation against host1's swapped file.
	code, stdout, stderr := entry.supply(ctx, d, switchoverScopedSupplyArgs(newOpID(t), "switchover-post-old", oldKeyFile, oldCaller)...)
	if code != 1 {
		t.Fatalf("post-swap old-principal supply exit = %d, want 1 (refused); stdout=%s stderr=%s", code, stdout, stderr)
	}
	if strings.Contains(stdout, "action=supplied") {
		t.Fatalf("post-swap old-principal stdout %q reports an accepted supply", stdout)
	}
	code, stdout, stderr = entry.supply(ctx, d2, switchoverScopedSupplyArgs(newOpID(t), "switchover-post-new", newKeyFile, newCaller)...)
	if code != 0 {
		t.Fatalf("post-swap new-principal supply exit = %d, want 0; stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "action=supplied") {
		t.Fatalf("post-swap new-principal stdout %q lacks action=supplied", stdout)
	}
	if entry.refusals != 0 {
		t.Fatalf("entry refusals = %d, want 0 (entry was open for the post-swap invocations)", entry.refusals)
	}

	t.Logf("T043 switchover rehearsal declared: hosts=%v checksums=%v swap_ts=%s",
		outcome.HostsSwapped, outcome.Checksums, outcome.SwapTimestamp.Format(time.RFC3339Nano))
}

// TestAllowlistSwitchoverRehearsalUnaccountedExecutorKeepsEntryClosed is the
// T043 failure path (R-PB10 §2/§5): an old supply executor that the process
// table does not account for blocks the drain confirmation, so the switchover is
// NOT declared and the entry stays closed.
func TestAllowlistSwitchoverRehearsalUnaccountedExecutorKeepsEntryClosed(t *testing.T) {
	dsn := startConfirmAuthPostgres(t)
	ctx := context.Background()

	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	const (
		oldCaller = int64(8403)
		newCaller = int64(8404)
	)
	oldKey, _, err := withdrawal.IssueKey(ctx, pool, oldCaller, "switchover-unaccounted-old")
	if err != nil {
		t.Fatalf("issue old key: %v", err)
	}
	oldKeyFile := switchoverKeyFile(t, oldKey, "old.key")

	base := withdrawalAuthzEnv(dsn)
	oldConfig := fmt.Sprintf("%s=%d\n", withdrawal.EnvIssuerCallers, oldCaller)
	newConfig := fmt.Sprintf("%s=%d\n", withdrawal.EnvIssuerCallers, newCaller)
	host := filepath.Join(t.TempDir(), "host1.env")
	if err := os.WriteFile(host, []byte(oldConfig), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	d := newSwitchoverDeployment(t, host, base)
	entry := &switchoverEntry{}

	// A live in-flight supply executor, NOT in the process table.
	executor := switchoverOpenSupplyTx(t, ctx, dsn, switchoverSupplyApp, oldKey)
	closed := false
	defer func() {
		if !closed {
			_ = executor.Close(ctx)
		}
	}()

	// The process table declares no in-flight PID: the open backend is
	// unaccounted and the drain must fail.
	outcome, err := runSwitchoverRehearsal(ctx, t, pool, entry, []string{host}, newConfig, nil, time.Second)
	if err == nil {
		t.Fatalf("rehearsal = %+v, want a drain failure while the executor is unaccounted", outcome)
	}
	if !strings.Contains(err.Error(), "unaccounted") {
		t.Fatalf("drain error %q does not name the unaccounted executor", err)
	}
	if outcome.Declared || !outcome.EntryClosed || outcome.FailureStep != "drain" {
		t.Fatalf("outcome = %+v, want declared=false entry closed at the drain step", outcome)
	}
	// The switchover was not declared: the config was never swapped and the
	// entry stays closed (no new invocations).
	if back, _ := os.ReadFile(host); string(back) != oldConfig {
		t.Fatalf("config changed while entry closed: %q", back)
	}
	if code, _, stderr := entry.supply(ctx, d, switchoverScopedSupplyArgs(newOpID(t), "switchover-halted", oldKeyFile, oldCaller)...); code != 1 || !strings.Contains(stderr, "entry halted") {
		t.Fatalf("halted entry supply = (%d, %q), want refusal naming the halt", code, stderr)
	}

	// Drain the executor, then the same procedure declares the switchover.
	if err := executor.Close(ctx); err != nil {
		t.Fatalf("close executor: %v", err)
	}
	closed = true
	outcome, err = runSwitchoverRehearsal(ctx, t, pool, entry, []string{host}, newConfig, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("rehearsal after drain = %+v, error = %v", outcome, err)
	}
	if !outcome.Declared || outcome.EntryClosed {
		t.Fatalf("outcome after drain = %+v, want declared with entry open", outcome)
	}
}

// newOpID mints one operation id for a rehearsal invocation.
func newOpID(t *testing.T) string {
	t.Helper()
	id, err := withdrawal.MintOperationID()
	if err != nil {
		t.Fatalf("mint operation id: %v", err)
	}
	return id
}
