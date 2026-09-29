//go:build integration

// verification_integration_test.go is T036 [US3]: the V1–V9 real-adapter
// verification integration layer (tags: integration; real PostgreSQL + real
// Anvil; FR-014–FR-020; data-model.md §1.4/§1.5/§1.6/§5/§8;
// contracts/verification-items.md §1; tasks.md T036).
//
// TDD-first. B10 lands the tests before the B11 implementation, so this file
// references the planned T038/T039/T040 API (internal/recovery/verification.go
// and internal/recovery/sources/) plus the T041 gaps API that do not exist
// yet: the integration candidate fails to build until B11 lands. Expected API
// surface (documented at every use site):
//
//	// internal/recovery/verification.go (T038)
//	type VerificationCategory string
//	const (VerificationV1 ... VerificationV9 VerificationCategory)
//	func KnownVerificationCategories() []VerificationCategory
//
//	type VerificationConclusion string
//	const (
//	    ConclusionConsistent VerificationConclusion = "consistent"
//	    ConclusionDivergent  VerificationConclusion = "divergent"
//	    ConclusionUnknown    VerificationConclusion = "unknown"
//	    ConclusionStale      VerificationConclusion = "stale"
//	)
//	func (c VerificationConclusion) Known() bool
//
//	type SourceObservation struct {
//	    ObjectKey            string
//	    Scope                []byte
//	    Sources              []byte
//	    Conclusion           VerificationConclusion
//	    Reason               string
//	    EvidenceRefs         []string
//	    ObservedAt           time.Time
//	    GapEvidence          *GapEvidence // non-nil asks for an evidence gap
//	}
//	type GapEvidence struct {
//	    Timeline, ExistingEvidence, RequiredEvidence, Risk []byte
//	    AffectedCapabilities []Capability
//	}
//	type VerificationSource interface {
//	    Category() VerificationCategory
//	    Observe(ctx context.Context) ([]SourceObservation, error)
//	}
//	type VerificationOptions struct {
//	    Tolerance  time.Duration // freshness tolerance; <= 0 stays conservative
//	    BatchLimit int           // per-batch item bound; <= 0 refuses (no default)
//	    Now        func() time.Time
//	}
//	func NewVerification(store *controlstore.Store, opts VerificationOptions) (*Verification, error)
//	type VerificationRequest struct {
//	    InstanceID  string
//	    Actor       string
//	    Scope       []byte
//	    OperationID string
//	    Sources     []VerificationSource
//	}
//	type VerificationItem struct {
//	    ItemID       string
//	    InstanceID   string
//	    Generation   int64
//	    Category     VerificationCategory
//	    ObjectKey    string
//	    Scope        []byte
//	    Sources      []byte
//	    Conclusion   VerificationConclusion
//	    Reason       string
//	    EvidenceRefs []string
//	    ObservedAt   time.Time
//	    CreatedAt    time.Time
//	}
//	type VerificationBatch struct {
//	    InstanceID string
//	    BatchID    string
//	    Generation int64
//	    Discarded  bool
//	    Items      []VerificationItem
//	    Gaps       []Gap
//	}
//	func (v *Verification) Verify(ctx context.Context, req VerificationRequest) (VerificationBatch, error)
//	const ActionVerification = "verification"
//	var ErrVerificationBound error
//
//	// internal/recovery/sources/chain.go (T039), ops.go (T040)
//	type ChainOptions struct {
//	    Data       *pgxpool.Pool // data DB; the pool may be a SELECT-only role
//	    RPC        *eth.Client   // canonical chain reads
//	    ChainID    uint64
//	    DataTarget string
//	    Now        func() time.Time
//	}
//	func NewChainSources(opts ChainOptions) ([]recovery.VerificationSource, error)
//	type DependencyProbe interface {
//	    Name() string
//	    Check(ctx context.Context) error
//	}
//	type BrokerOffsetReader interface {
//	    CommittedOffset(ctx context.Context, topic string, partition int) (int64, error)
//	}
//	type OpsOptions struct {
//	    Data          *pgxpool.Pool
//	    Control       *controlstore.Store
//	    RPC           *eth.Client
//	    BrokerOffsets BrokerOffsetReader // nil = broker unreadable -> unknown
//	    Dependencies  []DependencyProbe  // V9 reachability probes
//	    DataTarget    string
//	    Now           func() time.Time
//	}
//	func NewOpsSources(opts OpsOptions) ([]recovery.VerificationSource, error)
//
// Pinned behavior:
//
//   - every one of V1–V9 is observed through a real read-only adapter over the
//     real data DB and the real chain; the chain-ahead case (external lead) is
//     never consistent, a missing record is unknown plus a gap, and no
//     verification item may be fabricated from defaults;
//   - verification performs zero data-DB writes: R1 layer ① diffs the full
//     content fingerprint of the static V1–V9 read-chain table list
//     (migrations/-derived names, not dynamically collected) around a batch in
//     an isolated fixture; layer ② asserts the expected control-store writes
//     (append-only items, generation binding) separately; layer ③ observes a
//     concurrent business writer and asserts only zero verification-attributed
//     writes (a SELECT-only data role plus the writer's own convergence);
//   - historical replay is refused: signed/broadcast/payment-intent/outbox
//     history is never re-sent, re-broadcast, re-delivered or re-created, and
//     the verification surface exposes no effectful method at all;
//   - batch size is bounded by configuration (no default), the freshness
//     tolerance missing stays conservative (unknown), refusals are audited and
//     change no state.
//
// This file is in package recovery_test (external test package) because it
// imports internal/recovery/sources, which imports internal/recovery for the
// VerificationSource contract; an internal test file would close an import
// cycle. It therefore builds its own fixture from exported APIs. Docker
// discipline: the package TestMain (generation_integration_test.go) reports
// NOT RUN locally when no Docker provider is healthy and fails the package
// under CI=true or TXHARBOR_REQUIRE_DOCKER=1. Helpers are vfy-prefixed.
package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
	"github.com/xtianxx/txharbor/internal/recovery/sources"
)

const (
	// vfyPGImage is the pinned PostgreSQL carrier of ADR-002.
	vfyPGImage = "postgres:18.6-trixie"
	// vfyAnvilImage is the pinned Anvil used by the repo's other integration
	// suites (nonce/txlifecycle).
	vfyAnvilImage = "ghcr.io/foundry-rs/foundry:v1.8.1"
	// vfyChainID is the Anvil chain id (the compose default).
	vfyChainID = uint64(31337)
	// vfyDataTarget is the credential-free target fingerprint recorded in
	// verification scopes.
	vfyDataTarget = "database=vfy_data;role=vfy_owner"
)

// vfyReadOnlyTables is the R1-reduced static manifest: every data-DB table the
// V1–V9 read chain may touch. The list is frozen here (T036) and never
// collected dynamically; T039/T040 must extend it in the same commit if an
// adapter starts reading a new table (tasks.md T036). Every name is validated
// against the real migrations/ schema by
// vfyAssertStaticManifestMatchesMigrations so a typo or a removed table fails
// loudly.
//
// Known tables deliberately outside this list (classification guard, not
// dynamic collection): the existing gate/lease tables (indexer_pause,
// log_pause, deposit_pause, deposit_pause_audit, indexer_lease) are covered by
// their own gates, and reorg_policy_history / signer_credential are not in the
// T036 manifest — if T039/T040 reads them, they must be added here.
var vfyReadOnlyTables = []string{
	// V1 chain facts vs PG.
	"chain_blocks", "erc20_transfer_logs", "indexer_checkpoint", "log_checkpoint",
	"deposit_observations", "deposit_observation_transitions", "deposit_checkpoint",
	"deposit_config_history", "confirmation_policy_history", "reorg_recovery",
	"reorg_recovery_events",
	// V2 withdrawal requests / payment intents / authorizations.
	"withdrawal_requests", "payment_intents", "withdrawal_authorizations",
	"withdrawal_authorization_scopes", "request_status_projection",
	"withdrawal_request_audit", "withdrawal_grant_audit",
	// V3 nonce allocation / occupancy.
	"nonce_bindings", "nonce_observations", "nonce_scope_state",
	"nonce_scope_holds", "nonce_wallet_registry", "nonce_binding_events",
	"nonce_ops_audit",
	// V4 signing / broadcast / tx lifecycle / execution chain.
	"signing_requests", "signature_results", "signing_request_audit",
	"delivery_admissions", "signer_caller", "tx_attempts", "tx_attempt_signings",
	"tx_send_attempts", "tx_receipts", "tx_reconciliations", "tx_attempt_events",
	"tx_intent_freezes", "execution_claims", "execution_steps",
	"execution_events", "execution_ops_audit", "execution_caller_permission",
	// V5 outbox and obligations.
	"outbox_events", "event_obligation", "event_system_state", "event_ops_audit",
	// V6 consumer idempotency / progress / quarantine.
	"consumer_inbox", "consumer_versions", "consumer_progress",
	"consumer_quarantine",
	// V7/V8 reconciliation, disposition, permission and authorization-surface
	// evidence.
	"recon_task", "recon_checkpoint", "recon_gap", "discrepancy",
	"discrepancy_occurrence", "disposition", "reverify", "recon_audit",
	"recon_scan_attempt", "recon_permission", "caller", "api_key",
}

// ---------------------------------------------------------------------------
// Anvil harness: the real chain truth (same pinned container as the nonce
// suite; the package TestMain owns the Docker NOT RUN discipline).
// ---------------------------------------------------------------------------

type vfyAnvil struct {
	url    string
	client *eth.Client     // the adapters' canonical read surface
	rpc    *gethrpc.Client // raw JSON-RPC for anvil_mine / txpool_status
}

func vfyStartAnvil(t *testing.T) *vfyAnvil {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        vfyAnvilImage,
			ExposedPorts: []string{"8545/tcp"},
			// The image entrypoint is /bin/sh -c; without the override anvil
			// never receives --host and binds 127.0.0.1 inside the container.
			Entrypoint: []string{"anvil"},
			Cmd:        []string{"--host", "0.0.0.0", "--port", "8545", "--chain-id", "31337"},
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
	rawURL := fmt.Sprintf("http://%s:%s", host, port.Port())
	rpc, err := gethrpc.DialContext(ctx, rawURL)
	if err != nil {
		t.Fatalf("dial anvil rpc: %v", err)
	}
	t.Cleanup(rpc.Close)
	client, err := eth.Dial(ctx, rawURL, 5*time.Second)
	if err != nil {
		t.Fatalf("dial anvil eth client: %v", err)
	}
	t.Cleanup(client.Close)
	return &vfyAnvil{url: rawURL, client: client, rpc: rpc}
}

func (n *vfyAnvil) mustCall(t *testing.T, out any, method string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.rpc.CallContext(ctx, out, method, args...); err != nil {
		t.Fatalf("%s: %v", method, err)
	}
}

// mine mines real blocks on Anvil; the PG frontier is seeded separately, so
// the chain genuinely leads the restore point.
func (n *vfyAnvil) mine(t *testing.T, blocks uint64) {
	t.Helper()
	var ignored any
	n.mustCall(t, &ignored, "anvil_mine", hexutil.EncodeUint64(blocks))
}

func (n *vfyAnvil) tip(t *testing.T) uint64 {
	t.Helper()
	tip, err := n.client.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf("chain tip: %v", err)
	}
	return tip
}

func (n *vfyAnvil) pendingTxCount(t *testing.T) uint64 {
	t.Helper()
	var status struct {
		Pending hexutil.Uint64 `json:"pending"`
		Queued  hexutil.Uint64 `json:"queued"`
	}
	n.mustCall(t, &status, "txpool_status")
	return uint64(status.Pending)
}

// ---------------------------------------------------------------------------
// Fixture: one PostgreSQL container with one data DB (repo migrations) and one
// independent control DB (control-store migrations), one open recovery
// instance and the identity/participant baseline.
// ---------------------------------------------------------------------------

type vfyFixture struct {
	t   *testing.T
	ctx context.Context

	adminDSN string
	admin    *pgxpool.Pool
	dataDSN  string
	data     *pgxpool.Pool
	ctrlDSN  string
	ctrl     *pgxpool.Pool
	store    *controlstore.Store

	instanceID string
	seq        int
}

func vfyNewFixture(t *testing.T) *vfyFixture {
	t.Helper()
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, vfyPGImage,
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
	adminDSN, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	f := &vfyFixture{t: t, ctx: ctx, adminDSN: adminDSN}
	f.admin = vfyOpenPool(t, adminDSN)

	// Data database: the real repository migrations (the V1-V9 read chain).
	f.dataDSN = f.createDatabase("data")
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: f.dataDSN, LockTimeout: 30 * time.Second, ConnectTimeout: 5 * time.Second,
	}, io.Discard); err != nil {
		t.Fatalf("migrate data database: %v", err)
	}
	f.data = vfyOpenPool(t, f.dataDSN)

	// Control store: independent database with its own goose sequence.
	f.ctrlDSN = f.createDatabase("ctrl")
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: f.ctrlDSN, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS,
	}, io.Discard); err != nil {
		t.Fatalf("migrate control database: %v", err)
	}
	f.ctrl = vfyOpenPool(t, f.ctrlDSN)
	store, err := controlstore.NewStore(ctx, f.ctrl)
	if err != nil {
		t.Fatalf("controlstore.NewStore: %v", err)
	}
	f.store = store

	// One open recovery instance with executor/verifier/approver through the
	// real write paths. Bind it to the actual data database identity: deriving
	// these immutable guard values from the authoritative DSN ensures normal
	// control-store open creates the matching target-guard inventory.
	dataTarget, err := controlstore.ParseDSNTarget(f.dataDSN)
	if err != nil {
		t.Fatalf("parse authoritative data target: %v", err)
	}
	targetGuardKey, err := controlstore.TargetGuardKey(dataTarget)
	if err != nil {
		t.Fatalf("derive authoritative target guard key: %v", err)
	}
	targetRoleFingerprint := dataTarget.DataTargetFingerprint().RoleFingerprint
	opened, err := recovery.OpenInstance(ctx, store, recovery.OpenInstanceRequest{
		Kind:                  "recovery",
		OpenedBy:              "deploy:executor",
		EntryChains:           []uint64{1},
		TargetGuardKey:        targetGuardKey,
		TargetRoleFingerprint: targetRoleFingerprint,
	})
	if err != nil {
		t.Fatalf("open recovery instance: %v", err)
	}
	f.instanceID = opened.InstanceID
	f.mapIdentity("deploy:admin", "person-admin")
	f.mapIdentity("deploy:executor", "person-executor")
	f.mapIdentity("auth:verifier", "person-verifier")
	f.mapIdentity("auth:approver", "person-approver")
	f.register("deploy:executor", "executor")
	f.register("auth:verifier", "verifier")
	f.register("auth:approver", "approver")
	return f
}

func (f *vfyFixture) operation(prefix string) string {
	f.seq++
	return fmt.Sprintf("vfy-test-%s-%d", prefix, f.seq)
}

func (f *vfyFixture) mapIdentity(principal, person string) {
	f.t.Helper()
	if _, err := f.store.SetIdentityMapping(f.ctx, controlstore.SetIdentityMappingRequest{
		Principal: principal, PersonID: person, RecordedBy: "deploy:admin",
		OperationID: f.operation("map"),
	}); err != nil {
		f.t.Fatalf("map identity %s -> %s: %v", principal, person, err)
	}
}

func (f *vfyFixture) register(principal, role string) {
	f.t.Helper()
	if _, err := f.store.RegisterParticipant(f.ctx, controlstore.RegisterParticipantRequest{
		InstanceID: f.instanceID, Principal: principal, Role: role,
		Actor: "deploy:admin", OperationID: f.operation("reg"),
	}); err != nil {
		f.t.Fatalf("register %s as %s: %v", principal, role, err)
	}
}

func (f *vfyFixture) createDatabase(label string) string {
	f.t.Helper()
	name := fmt.Sprintf("vfy_t_%s_%d", label, f.seq)
	f.seq++
	if _, err := f.admin.Exec(f.ctx, "CREATE DATABASE "+quoteIdent(name)); err != nil {
		f.t.Fatalf("create database %s: %v", name, err)
	}
	return vfyDSNFor(f.t, f.adminDSN, name)
}

// seedIndexedFrontier writes one canonical block plus its checkpoint: PG has
// indexed block 1 while Anvil is mined past it, the controlled "external lead"
// of F4.
func (f *vfyFixture) seedIndexedFrontier(height uint64) {
	f.t.Helper()
	if _, err := f.data.Exec(f.ctx,
		`INSERT INTO chain_blocks (chain_id, number, hash, parent_hash)
		 VALUES ($1, $2, $3, $4)`,
		vfyChainID, height, vfyBlockHash(height), vfyBlockHash(height-1)); err != nil {
		f.t.Fatalf("seed chain block %d: %v", height, err)
	}
	if _, err := f.data.Exec(f.ctx,
		`INSERT INTO indexer_checkpoint (chain_id, height, block_hash, start_height)
		 VALUES ($1, $2, $3, 0)`,
		vfyChainID, height, vfyBlockHash(height)); err != nil {
		f.t.Fatalf("seed indexer checkpoint: %v", err)
	}
}

// seedWithdrawalRequest writes one real withdrawal request so V2 faces a
// restore point with a record but no provable payment intent.
func (f *vfyFixture) seedWithdrawalRequest() {
	f.t.Helper()
	if _, err := f.data.Exec(f.ctx,
		`INSERT INTO caller (caller_id, label) VALUES (1, 'vfy-fixture')`); err != nil {
		f.t.Fatalf("seed caller: %v", err)
	}
	asset := "0x" + strings.Repeat("a", 40)
	recipient := "0x" + strings.Repeat("b", 40)
	if _, err := f.data.Exec(f.ctx,
		`INSERT INTO withdrawal_requests
		 (request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
		 VALUES ('vfy-req-1', 1, 'vfy-idem-1', 'vfy-auth-1', $1, $2, $3, 1000)`,
		vfyChainID, asset, recipient); err != nil {
		f.t.Fatalf("seed withdrawal request: %v", err)
	}
}

// seedPendingOutbox writes one historical, unpublished outbox event: the
// verification must never publish it (F10 history).
func (f *vfyFixture) seedPendingOutbox() {
	f.t.Helper()
	if _, err := f.data.Exec(f.ctx,
		`INSERT INTO outbox_events
		 (event_id, identity_kind, event_type, schema_version, aggregate_type,
		  aggregate_id, aggregate_version, payload, payload_hash, occurred_at, publish_state)
		 VALUES (gen_random_uuid(), 'business_object', 'withdrawal_requested', 1,
		         'withdrawal_request', 'vfy-req-1', 1, '{}'::jsonb, 'vfy-payload-hash-1', now(), 'pending')`); err != nil {
		f.t.Fatalf("seed pending outbox: %v", err)
	}
}

// seedConsumerProgress writes one historical consumer progress row.
func (f *vfyFixture) seedConsumerProgress(nextOffset int64) {
	f.t.Helper()
	if _, err := f.data.Exec(f.ctx,
		`INSERT INTO consumer_progress (consumer_name, topic, partition, next_offset)
		 VALUES ('vfy-consumer', 'vfy-topic', 0, $1)`, nextOffset); err != nil {
		f.t.Fatalf("seed consumer progress: %v", err)
	}
}

func vfyBlockHash(number uint64) string {
	return fmt.Sprintf("0x%064x", number)
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func vfyOpenPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := db.OpenPool(context.Background(), dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func vfyDSNFor(t *testing.T, baseDSN, dbName string) string {
	t.Helper()
	u, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatalf("parse base dsn: %v", err)
	}
	u.Path = "/" + dbName
	return u.String()
}

func vfyDSNWithUser(t *testing.T, baseDSN, user, password string) string {
	t.Helper()
	u, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatalf("parse base dsn: %v", err)
	}
	u.User = url.UserPassword(user, password)
	return u.String()
}

// readOnlyPool creates a SELECT-only login role over the data DB (R1 layer
// ③: verification-attributed writes are attributed through the connection's
// role, which cannot write at all).
func (f *vfyFixture) readOnlyPool() *pgxpool.Pool {
	f.t.Helper()
	role := "vfy_ro"
	if _, err := f.data.Exec(f.ctx,
		`CREATE ROLE `+role+` LOGIN PASSWORD 'vfy_ro' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT`); err != nil {
		f.t.Fatalf("create read-only role: %v", err)
	}
	if _, err := f.data.Exec(f.ctx, `GRANT USAGE ON SCHEMA public TO `+role); err != nil {
		f.t.Fatalf("grant schema usage: %v", err)
	}
	if _, err := f.data.Exec(f.ctx, `GRANT SELECT ON ALL TABLES IN SCHEMA public TO `+role); err != nil {
		f.t.Fatalf("grant select: %v", err)
	}
	pool := vfyOpenPool(f.t, vfyDSNWithUser(f.t, f.dataDSN, role, "vfy_ro"))
	if _, err := pool.Exec(f.ctx, `UPDATE consumer_progress SET next_offset = next_offset`); err == nil {
		f.t.Fatal("the read-only verification role must not be able to write")
	}
	return pool
}

// ---------------------------------------------------------------------------
// Sources assembly and verification helpers.
// ---------------------------------------------------------------------------

type vfyProbe struct {
	name  string
	check func(ctx context.Context) error
}

func (p vfyProbe) Name() string                    { return p.name }
func (p vfyProbe) Check(ctx context.Context) error { return p.check(ctx) }

// vfySources assembles the real T039/T040 adapters; no test double stands in
// for a source. data is the pool handed to the adapters (the fixture's data
// pool, or the SELECT-only role of R1 layer ③); deps are the V9 reachability
// probes (nil = not configured, stays conservative).
func vfySources(t *testing.T, f *vfyFixture, anvil *vfyAnvil, data *pgxpool.Pool, deps []sources.DependencyProbe) []recovery.VerificationSource {
	t.Helper()
	chain, err := sources.NewChainSources(sources.ChainOptions{
		Data:       data,
		RPC:        anvil.client,
		ChainID:    vfyChainID,
		DataTarget: vfyDataTarget,
	})
	if err != nil {
		t.Fatalf("NewChainSources: %v", err)
	}
	ops, err := sources.NewOpsSources(sources.OpsOptions{
		Data:          data,
		Control:       f.store,
		RPC:           anvil.client,
		BrokerOffsets: nil, // broker unreadable: V5/V6 stay conservative
		Dependencies:  deps,
		DataTarget:    vfyDataTarget,
	})
	if err != nil {
		t.Fatalf("NewOpsSources: %v", err)
	}
	all := append(chain, ops...)
	if len(all) == 0 {
		t.Fatal("the sources assembly returned no V-sources")
	}
	return all
}

func vfyVerifier(t *testing.T, f *vfyFixture, tolerance time.Duration, limit int) *recovery.Verification {
	t.Helper()
	v, err := recovery.NewVerification(f.store, recovery.VerificationOptions{
		Tolerance: tolerance, BatchLimit: limit,
	})
	if err != nil {
		t.Fatalf("NewVerification: %v", err)
	}
	return v
}

func vfyRequest(f *vfyFixture, anvil *vfyAnvil, data *pgxpool.Pool, deps []sources.DependencyProbe, operationID string) recovery.VerificationRequest {
	return recovery.VerificationRequest{
		InstanceID:  f.instanceID,
		Actor:       "deploy:executor",
		Scope:       []byte(`{"chain_id":31337,"target":"vfy-test-target"}`),
		OperationID: operationID,
		Sources:     vfySources(f.t, f, anvil, data, deps),
	}
}

func vfyGate(t *testing.T, f *vfyFixture) *recovery.Gate {
	t.Helper()
	target, err := recovery.GateTargetBindingFromDSN(f.dataDSN)
	if err != nil {
		t.Fatalf("GateTargetBindingFromDSN: %v", err)
	}
	gate, err := recovery.NewGate(f.store, recovery.GateOptions{TTL: time.Minute, TrustedTarget: target})
	if err != nil {
		t.Fatalf("NewGate: %v", err)
	}
	return gate
}

func vfyAdmit(t *testing.T, f *vfyFixture, gate *recovery.Gate, capability recovery.Capability) recovery.GateDecision {
	t.Helper()
	// The canonical capability scope of the fixture chain (T050); the same
	// constructor the package-internal fixtures use.
	scope, err := recovery.Scope{ChainID: 31337, Asset: "usdc", Kind: "deposit", Capability: capability}.Canonical()
	if err != nil {
		t.Fatalf("canonical scope of %s: %v", capability, err)
	}
	decision, err := gate.Admit(f.ctx, recovery.GateRequest{
		InstanceID: f.instanceID, Capability: capability, ScopeHash: scope,
		Actor: "deploy:executor", OperationID: f.operation("admit"),
	})
	if err != nil {
		t.Fatalf("gate.Admit(%s): %v", capability, err)
	}
	return decision
}

func vfyItemCount(t *testing.T, f *vfyFixture) int {
	t.Helper()
	var n int
	if err := f.ctrl.QueryRow(f.ctx,
		`SELECT count(*) FROM recovery_verification_item WHERE instance_id = $1`,
		f.instanceID).Scan(&n); err != nil {
		t.Fatalf("count verification items: %v", err)
	}
	return n
}

func vfyItemRow(t *testing.T, f *vfyFixture, itemID string) string {
	t.Helper()
	var row string
	if err := f.ctrl.QueryRow(f.ctx,
		`SELECT to_jsonb(i)::text FROM recovery_verification_item i WHERE item_id = $1`,
		itemID).Scan(&row); err != nil {
		t.Fatalf("read verification item %s: %v", itemID, err)
	}
	return row
}

func vfyAuditCount(t *testing.T, f *vfyFixture, action, result string) int {
	t.Helper()
	var n int
	if err := f.ctrl.QueryRow(f.ctx,
		`SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = $2 AND result = $3`,
		f.instanceID, action, result).Scan(&n); err != nil {
		t.Fatalf("count audit rows (%s/%s): %v", action, result, err)
	}
	return n
}

func vfyGeneration(t *testing.T, f *vfyFixture) int64 {
	t.Helper()
	var generation int64
	if err := f.ctrl.QueryRow(f.ctx,
		`SELECT evidence_generation FROM recovery_instance WHERE instance_id = $1`,
		f.instanceID).Scan(&generation); err != nil {
		t.Fatalf("read instance generation: %v", err)
	}
	return generation
}

// vfyFingerprintData serializes the full current content of every static
// read-chain table (all rows via to_jsonb, deterministic order) into a digest
// map — an in-place UPDATE that keeps row counts identical is still caught.
func vfyFingerprintData(t *testing.T, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	out := make(map[string]string, len(vfyReadOnlyTables))
	for _, table := range vfyReadOnlyTables {
		var fp string
		if err := pool.QueryRow(context.Background(),
			`SELECT COALESCE(string_agg(row_text, E'\n' ORDER BY row_text), '')
			   FROM (SELECT to_jsonb(x)::text AS row_text FROM `+table+` x) s`).Scan(&fp); err != nil {
			t.Fatalf("fingerprint %s: %v", table, err)
		}
		out[table] = fp
	}
	return out
}

// vfyAssertStaticManifestMatchesMigrations pins the static manifest against
// the real migrations/ schema (names must exist; the excluded gate/lease
// tables must not be present) without ever collecting the fingerprint list
// dynamically (tasks.md T036).
func vfyAssertStaticManifestMatchesMigrations(t *testing.T) {
	t.Helper()
	created := make(map[string]bool)
	entries, err := fs.ReadDir(db.Migrations, ".")
	if err != nil {
		t.Fatalf("read migrations FS: %v", err)
	}
	createTable := regexp.MustCompile(`(?i)CREATE TABLE(?: IF NOT EXISTS)?\s+([a-z0-9_]+)`)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		raw, err := fs.ReadFile(db.Migrations, entry.Name())
		if err != nil {
			t.Fatalf("read migration %s: %v", entry.Name(), err)
		}
		for _, match := range createTable.FindAllStringSubmatch(string(raw), -1) {
			created[strings.ToLower(match[1])] = true
		}
	}
	if len(created) == 0 {
		t.Fatal("no CREATE TABLE found in the migrations FS")
	}
	seen := make(map[string]bool, len(vfyReadOnlyTables))
	for _, table := range vfyReadOnlyTables {
		if seen[table] {
			t.Fatalf("static manifest lists %s twice", table)
		}
		seen[table] = true
		if !created[table] {
			t.Fatalf("static manifest lists %s, which no migration creates", table)
		}
	}
	// The gate/lease tables belong to their own gates, not the V1-V9 read chain.
	for _, excluded := range []string{
		"indexer_pause", "log_pause", "deposit_pause", "deposit_pause_audit", "indexer_lease",
	} {
		if slices.Contains(vfyReadOnlyTables, excluded) {
			t.Fatalf("%s is not part of the V1-V9 read chain and must stay out of the zero-write manifest", excluded)
		}
	}
}

// vfyAssertAllCategoriesObserved is the "V1–V9 全部经真实适配器" assertion:
// every closed category is observed by a real source and ends up persisted.
func vfyAssertAllCategoriesObserved(t *testing.T, batch recovery.VerificationBatch) {
	t.Helper()
	seen := make(map[recovery.VerificationCategory]int)
	for _, item := range batch.Items {
		if !item.Conclusion.Known() {
			t.Fatalf("persisted item %s has conclusion %q outside the closed set", item.ItemID, item.Conclusion)
		}
		if item.Conclusion != recovery.ConclusionConsistent && item.Reason == "" {
			t.Fatalf("item %s (%s) is %s without a reason", item.ItemID, item.Category, item.Conclusion)
		}
		seen[item.Category]++
	}
	for _, category := range recovery.KnownVerificationCategories() {
		if seen[category] == 0 {
			t.Fatalf("no %s verification item was produced from the real adapters (seen: %v)", category, seen)
		}
	}
}

// ---------------------------------------------------------------------------
// T036 tests.
// ---------------------------------------------------------------------------

// TestT036VerificationV1ToV9RealAdaptersExternalLead is the core F4/F6 case:
// with a real Anvil chain leading the restored PG frontier and records missing
// after the restore point, every V1–V9 conclusion comes from a real adapter,
// the external lead is never reported consistent, a missing record is unknown
// plus an open gap, and the affected capabilities stay closed.
func TestT036VerificationV1ToV9RealAdaptersExternalLead(t *testing.T) {
	f := vfyNewFixture(t)
	anvil := vfyStartAnvil(t)
	f.seedIndexedFrontier(1)
	f.seedWithdrawalRequest()
	anvil.mine(t, 12)

	deps := []sources.DependencyProbe{
		vfyProbe{name: "data-db", check: func(ctx context.Context) error {
			if err := f.data.Ping(ctx); err != nil {
				return fmt.Errorf("data database unreachable: %w", err)
			}
			return nil
		}},
		vfyProbe{name: "chain-rpc", check: func(ctx context.Context) error {
			if _, err := anvil.client.BlockNumber(ctx); err != nil {
				return fmt.Errorf("chain RPC unreachable: %w", err)
			}
			return nil
		}},
	}

	// Every source is a real adapter that observes at least one object.
	all := vfySources(t, f, anvil, f.data, deps)
	seen := make(map[recovery.VerificationCategory]bool)
	for _, source := range all {
		category := source.Category()
		if !slices.Contains(recovery.KnownVerificationCategories(), category) {
			t.Fatalf("adapter reports category %q outside the closed V1-V9 set", category)
		}
		observations, err := source.Observe(context.Background())
		if err != nil {
			t.Fatalf("Observe(%s): %v", category, err)
		}
		if len(observations) == 0 {
			t.Fatalf("adapter %s observed nothing", category)
		}
		for _, observation := range observations {
			if !observation.Conclusion.Known() {
				t.Fatalf("adapter %s observation conclusion %q is outside the closed set", category, observation.Conclusion)
			}
		}
		seen[category] = true
	}
	for _, category := range recovery.KnownVerificationCategories() {
		if !seen[category] {
			t.Fatalf("no real adapter covers category %s", category)
		}
	}

	verification := vfyVerifier(t, f, time.Hour, 1000)
	batch, err := verification.Verify(f.ctx, vfyRequest(f, anvil, f.data, deps, f.operation("verify-external-lead")))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if batch.Discarded {
		t.Fatalf("uncontended batch was discarded: %+v", batch)
	}
	if batch.InstanceID != f.instanceID {
		t.Fatalf("batch instance %q, want %q", batch.InstanceID, f.instanceID)
	}
	vfyAssertAllCategoriesObserved(t, batch)

	// F4: the chain leads the restored frontier; V1 must report the difference,
	// never consistency.
	var v1 []recovery.VerificationItem
	for _, item := range batch.Items {
		if item.Category == recovery.VerificationV1 {
			v1 = append(v1, item)
		}
	}
	if len(v1) == 0 {
		t.Fatal("no V1 item for the external-lead case")
	}
	for _, item := range v1 {
		if item.Conclusion == recovery.ConclusionConsistent {
			t.Fatalf("V1 reported consistent while the chain leads the restored PG frontier: %+v", item)
		}
	}

	// F6: the missing records are unknown (not "never happened") and establish
	// an open evidence gap naming the affected capabilities.
	unknownV2 := false
	for _, item := range batch.Items {
		if item.Category == recovery.VerificationV2 && item.Conclusion == recovery.ConclusionUnknown {
			unknownV2 = true
		}
	}
	if !unknownV2 {
		t.Fatalf("V2 must be unknown when the restore point has records with no provable external evidence: %+v", batch.Items)
	}
	var gapCount int
	if err := f.ctrl.QueryRow(f.ctx,
		`SELECT count(*) FROM recovery_gap
		  WHERE instance_id = $1 AND state = 'open'
		    AND ('new_withdrawal_creation' = ANY(affected_capabilities)
		         OR 'existing_withdrawal_recovery' = ANY(affected_capabilities))`,
		f.instanceID).Scan(&gapCount); err != nil {
		t.Fatalf("count open gaps: %v", err)
	}
	if gapCount == 0 {
		t.Fatal("a missing-record verification must establish an open evidence gap for the affected capabilities")
	}

	// The affected capabilities stay closed through the real gate.
	gate := vfyGate(t, f)
	for _, capability := range []recovery.Capability{
		recovery.CapabilityNewWithdrawalCreation,
		recovery.CapabilityExistingWithdrawalRecovery,
	} {
		if d := vfyAdmit(t, f, gate, capability); d.Allowed {
			t.Fatalf("capability %s admitted although verification left an open gap", capability)
		}
	}

	// Repeated bounded stepping converges: the same operation_id reads back
	// without appending a second batch.
	before := vfyItemCount(t, f)
	replay := vfyRequest(f, anvil, f.data, deps, "vfy-idempotent-step")
	if _, err := verification.Verify(f.ctx, replay); err != nil {
		t.Fatalf("first bounded step: %v", err)
	}
	afterFirst := vfyItemCount(t, f)
	if afterFirst == before {
		t.Fatal("the bounded step persisted no items")
	}
	if _, err := verification.Verify(f.ctx, replay); err != nil {
		t.Fatalf("idempotent read-back: %v", err)
	}
	if after := vfyItemCount(t, f); after != afterFirst {
		t.Fatalf("re-running the same operation_id appended items: %d -> %d", afterFirst, after)
	}
}

// TestT036VerificationReadOnlyStaticTableFingerprint is R1 layer ①: in an
// isolated fixture (no concurrent business writer) the full content
// fingerprint of every static V1–V9 read-chain table is byte-identical before
// and after a verification batch, so a single in-place UPDATE would be caught.
func TestT036VerificationReadOnlyStaticTableFingerprint(t *testing.T) {
	f := vfyNewFixture(t)
	anvil := vfyStartAnvil(t)
	f.seedIndexedFrontier(1)
	f.seedWithdrawalRequest()
	f.seedPendingOutbox()
	f.seedConsumerProgress(5)
	anvil.mine(t, 12)

	vfyAssertStaticManifestMatchesMigrations(t)

	// The static manifest must be a real probe of the data DB, not an empty
	// sweep: at least one seeded table must have content.
	before := vfyFingerprintData(t, f.data)
	nonEmpty := 0
	for _, fp := range before {
		if fp != "" {
			nonEmpty++
		}
	}
	if nonEmpty == 0 {
		t.Fatal("the fingerprint sweep saw no seeded data; it cannot prove anything")
	}

	verification := vfyVerifier(t, f, time.Hour, 1000)
	batch, err := verification.Verify(f.ctx, vfyRequest(f, anvil, f.data, nil, f.operation("verify-fingerprint")))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if len(batch.Items) == 0 {
		t.Fatal("verification persisted no items; nothing was actually verified")
	}

	after := vfyFingerprintData(t, f.data)
	for _, table := range vfyReadOnlyTables {
		if before[table] != after[table] {
			t.Errorf("data table %s changed during verification (zero-write boundary violated)\nbefore: %s\nafter:  %s",
				table, before[table], after[table])
		}
	}
}

// TestT036VerificationHistoricalReplayNegativeZeroSideEffects is F10: over a
// fixture carrying historical signed/broadcast/pending-delivery state, the
// verification triggers no payment, signature, broadcast, replay or real
// downstream delivery — zero new intents, zero sends, an untouched pending
// outbox row, an unchanged chain tip and no admission for the effectful
// capability.
func TestT036VerificationHistoricalReplayNegativeZeroSideEffects(t *testing.T) {
	f := vfyNewFixture(t)
	anvil := vfyStartAnvil(t)
	f.seedIndexedFrontier(1)
	f.seedWithdrawalRequest()
	f.seedPendingOutbox()
	f.seedConsumerProgress(5)
	anvil.mine(t, 12)

	effectTables := []string{
		"payment_intents", "signing_requests", "signature_results", "tx_attempts",
		"tx_send_attempts", "tx_receipts", "execution_claims", "event_obligation",
	}
	before := make(map[string]int, len(effectTables))
	for _, table := range effectTables {
		var n int
		if err := f.data.QueryRow(f.ctx, `SELECT count(*) FROM `+table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		before[table] = n
	}
	var outboxBefore string
	if err := f.data.QueryRow(f.ctx,
		`SELECT to_jsonb(o)::text FROM outbox_events o`).Scan(&outboxBefore); err != nil {
		t.Fatalf("snapshot outbox: %v", err)
	}
	var progressBefore int64
	if err := f.data.QueryRow(f.ctx,
		`SELECT next_offset FROM consumer_progress WHERE consumer_name = 'vfy-consumer'`).Scan(&progressBefore); err != nil {
		t.Fatalf("snapshot consumer progress: %v", err)
	}
	tipBefore := anvil.tip(t)

	// The read-only surface exposes exactly the two observation methods: there
	// is no replay/send/publish/recreate entry point to call.
	iface := reflect.TypeOf((*recovery.VerificationSource)(nil)).Elem()
	allowed := map[string]bool{"Category": true, "Observe": true}
	for i := 0; i < iface.NumMethod(); i++ {
		if name := iface.Method(i).Name; !allowed[name] {
			t.Fatalf("VerificationSource exposes %s; a read-only source may only Category/Observe", name)
		}
	}

	verification := vfyVerifier(t, f, time.Hour, 1000)
	batch, err := verification.Verify(f.ctx, vfyRequest(f, anvil, f.data, nil, f.operation("verify-history")))
	if err != nil {
		t.Fatalf("Verify over historical state: %v", err)
	}
	if len(batch.Items) == 0 {
		t.Fatal("verification persisted no items")
	}

	for _, table := range effectTables {
		var n int
		if err := f.data.QueryRow(f.ctx, `SELECT count(*) FROM `+table).Scan(&n); err != nil {
			t.Fatalf("recount %s: %v", table, err)
		}
		if n != before[table] {
			t.Fatalf("verification changed %s: %d -> %d (no replay/re-send/re-create)", table, before[table], n)
		}
	}
	var outboxAfter string
	if err := f.data.QueryRow(f.ctx,
		`SELECT to_jsonb(o)::text FROM outbox_events o`).Scan(&outboxAfter); err != nil {
		t.Fatalf("re-snapshot outbox: %v", err)
	}
	if outboxBefore != outboxAfter {
		t.Fatalf("the pending outbox event changed during verification:\nbefore: %s\nafter:  %s", outboxBefore, outboxAfter)
	}
	var progressAfter int64
	if err := f.data.QueryRow(f.ctx,
		`SELECT next_offset FROM consumer_progress WHERE consumer_name = 'vfy-consumer'`).Scan(&progressAfter); err != nil {
		t.Fatalf("re-snapshot consumer progress: %v", err)
	}
	if progressAfter != progressBefore {
		t.Fatalf("verification advanced consumer progress: %d -> %d", progressBefore, progressAfter)
	}
	if tipAfter := anvil.tip(t); tipAfter != tipBefore {
		t.Fatalf("verification mined/broadcast a block: tip %d -> %d", tipBefore, tipAfter)
	}
	if pending := anvil.pendingTxCount(t); pending != 0 {
		t.Fatalf("verification left %d pending transactions on the chain", pending)
	}

	// The effectful capability has no admission path: historical state does not
	// authorize a replay.
	if d := vfyAdmit(t, f, vfyGate(t, f), recovery.CapabilityExistingWithdrawalRecovery); d.Allowed {
		t.Fatalf("existing_withdrawal_recovery admitted over historical state: %+v", d)
	}
}

// TestT036VerificationBatchBoundAndConservativeMissingTolerance pins the
// bounded batch (no default) and the conservative missing tolerance: an
// unconfigured freshness tolerance never produces a consistent conclusion,
// and an over-limit batch is refused, audited and writes nothing.
func TestT036VerificationBatchBoundAndConservativeMissingTolerance(t *testing.T) {
	f := vfyNewFixture(t)
	anvil := vfyStartAnvil(t)
	f.seedIndexedFrontier(1)
	anvil.mine(t, 6)

	if _, err := recovery.NewVerification(nil, recovery.VerificationOptions{Tolerance: time.Hour, BatchLimit: 10}); err == nil {
		t.Fatal("NewVerification(nil store) must refuse")
	}
	if _, err := recovery.NewVerification(f.store, recovery.VerificationOptions{Tolerance: time.Hour, BatchLimit: 0}); err == nil {
		t.Fatal("NewVerification without a batch bound must refuse: there is no default bound")
	}

	// Over-limit: refused before anything is persisted.
	before := vfyItemCount(t, f)
	generationBefore := vfyGeneration(t, f)
	refusedBefore := vfyAuditCount(t, f, recovery.ActionVerification, "refused")
	bound := vfyVerifier(t, f, time.Hour, 2)
	if _, err := bound.Verify(f.ctx, vfyRequest(f, anvil, f.data, nil, f.operation("verify-over-bound"))); !errors.Is(err, recovery.ErrVerificationBound) {
		t.Fatalf("over-limit batch error = %v, want ErrVerificationBound", err)
	}
	if got := vfyItemCount(t, f); got != before {
		t.Fatalf("refused batch persisted items: %d -> %d", before, got)
	}
	if got := vfyGeneration(t, f); got != generationBefore {
		t.Fatalf("refused batch advanced the generation: %d -> %d", generationBefore, got)
	}
	if got := vfyAuditCount(t, f, recovery.ActionVerification, "refused"); got <= refusedBefore {
		t.Fatalf("refused batch must be audited: %d -> %d", refusedBefore, got)
	}

	// Missing tolerance: the batch runs but stays conservative.
	lax := vfyVerifier(t, f, 0, 1000)
	batch, err := lax.Verify(f.ctx, vfyRequest(f, anvil, f.data, nil, f.operation("verify-no-tolerance")))
	if err != nil {
		t.Fatalf("Verify without a freshness tolerance: %v", err)
	}
	vfyAssertAllCategoriesObserved(t, batch)
	for _, item := range batch.Items {
		if item.Conclusion == recovery.ConclusionConsistent {
			t.Fatalf("item %s is consistent although the freshness tolerance is unconfigured (缺失→保守 unknown)", item.ItemID)
		}
	}
}

// TestT036VerificationControlStoreAppendOnlyGenerationBound is R1 layer ②:
// the verification's own control-store writes are the expected output and are
// asserted separately — item rows are append-only, bound to the accepted
// generation, and the same operation_id converges without a second batch.
func TestT036VerificationControlStoreAppendOnlyGenerationBound(t *testing.T) {
	f := vfyNewFixture(t)
	anvil := vfyStartAnvil(t)
	f.seedIndexedFrontier(1)
	anvil.mine(t, 6)

	verification := vfyVerifier(t, f, time.Hour, 1000)
	generationBefore := vfyGeneration(t, f)
	countBefore := vfyItemCount(t, f)

	firstOperation := f.operation("verify-append-1")
	first, err := verification.Verify(f.ctx, vfyRequest(f, anvil, f.data, nil, firstOperation))
	if err != nil {
		t.Fatalf("first batch: %v", err)
	}
	if first.Discarded || first.BatchID == "" || first.Generation <= generationBefore {
		t.Fatalf("first batch = %+v, want an accepted generation-bound batch (before=%d)", first, generationBefore)
	}
	countAfterFirst := vfyItemCount(t, f)
	if countAfterFirst <= countBefore {
		t.Fatalf("first batch appended no items: %d -> %d", countBefore, countAfterFirst)
	}
	for _, item := range first.Items {
		if item.Generation != first.Generation {
			t.Fatalf("item %s carries generation %d, want the accepted batch generation %d",
				item.ItemID, item.Generation, first.Generation)
		}
	}
	firstID := first.Items[0].ItemID
	firstRow := vfyItemRow(t, f, firstID)

	// Same operation_id: idempotent read-back, no second batch.
	if _, err := verification.Verify(f.ctx, vfyRequest(f, anvil, f.data, nil, firstOperation)); err != nil {
		t.Fatalf("idempotent read-back: %v", err)
	}
	if got := vfyItemCount(t, f); got != countAfterFirst {
		t.Fatalf("same operation_id appended items: %d -> %d", countAfterFirst, got)
	}

	// New operation_id: append-only — new rows, old rows byte-identical.
	second, err := verification.Verify(f.ctx, vfyRequest(f, anvil, f.data, nil, f.operation("verify-append-2")))
	if err != nil {
		t.Fatalf("second batch: %v", err)
	}
	if second.Discarded || second.Generation <= first.Generation {
		t.Fatalf("second batch = %+v, want a later accepted generation (first=%d)", second, first.Generation)
	}
	if got := vfyItemCount(t, f); got <= countAfterFirst {
		t.Fatalf("second batch appended no items: %d -> %d", countAfterFirst, got)
	}
	if row := vfyItemRow(t, f, firstID); row != firstRow {
		t.Fatalf("append-only violated: item %s changed between batches\nbefore: %s\nafter:  %s", firstID, firstRow, row)
	}
}

// TestT036VerificationConcurrentAttributionReadOnlyRole is R1 layer ③: with a
// concurrent business writer there is no whole-table fingerprint assertion;
// instead the verification runs over a SELECT-only data role and the writer's
// table converges to exactly the writer's own writes — zero verification-
// attributed data writes.
func TestT036VerificationConcurrentAttributionReadOnlyRole(t *testing.T) {
	f := vfyNewFixture(t)
	anvil := vfyStartAnvil(t)
	f.seedIndexedFrontier(1)
	anvil.mine(t, 6)

	roPool := f.readOnlyPool()

	const writes = 40
	var wg sync.WaitGroup
	writerErr := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1); i <= writes; i++ {
			if _, err := f.data.Exec(f.ctx, `
INSERT INTO consumer_progress (consumer_name, topic, partition, next_offset)
VALUES ('vfy-concurrent', 'vfy-topic', 0, $1)
ON CONFLICT (consumer_name, topic, partition)
DO UPDATE SET next_offset = EXCLUDED.next_offset, updated_at = now()`, i); err != nil {
				writerErr <- fmt.Errorf("writer at %d: %w", i, err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Verification reads the data DB only through the SELECT-only role; a
	// verification write would fail the connection, so a successful batch is
	// the database-side attribution that the writes below belong to the writer.
	verification := vfyVerifier(t, f, time.Hour, 1000)
	batch, err := verification.Verify(f.ctx, vfyRequest(f, anvil, roPool, nil, f.operation("verify-concurrent")))
	wg.Wait()
	select {
	case err := <-writerErr:
		t.Fatalf("concurrent writer failed: %v", err)
	default:
	}
	if err != nil {
		t.Fatalf("Verify over the SELECT-only role: %v", err)
	}
	if len(batch.Items) == 0 {
		t.Fatal("verification persisted no items while observing the concurrent writer")
	}

	// The writer's table matches exactly the writer's own sequence: nothing
	// else wrote to it.
	var rows int
	var nextOffset int64
	if err := f.data.QueryRow(f.ctx,
		`SELECT count(*), max(next_offset) FROM consumer_progress WHERE consumer_name = 'vfy-concurrent'`).
		Scan(&rows, &nextOffset); err != nil {
		t.Fatalf("read concurrent progress: %v", err)
	}
	if rows != 1 || nextOffset != writes {
		t.Fatalf("concurrent consumer_progress = rows %d offset %d, want 1 row at %d (writer-attributed only)",
			rows, nextOffset, writes)
	}
}
