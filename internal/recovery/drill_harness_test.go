//go:build drill

// drill_harness_test.go is the shared fixture of the B17 independent drill
// channel (T058-T060): one real PostgreSQL 18.6 container (pinned
// postgres:18.6-trixie) carrying an independent control store, a real Anvil
// (pinned ghcr.io/foundry-rs/foundry:v1.8.1) and a container-backed
// PGCommand that executes the real pg_dump/pg_restore binaries of the pinned
// image. There is no in-process dumper, no hand-written archive and no
// verification double anywhere in this channel (quickstart §4).
//
// Package placement: this file is in package recovery_test (external test
// package) because the drill assembles the real T039/T040 verification
// adapters, and internal/recovery/sources imports internal/recovery — an
// internal test file would close an import cycle. Everything below therefore
// uses the exported 015 API only; nothing writes control-store decision rows
// directly (no approval/release/gap/isolation state is ever inserted by SQL —
// quickstart §4 anti-cheat discipline).
//
// Docker discipline (drill.yml + quickstart §3): the drill layer never runs on
// ordinary pull requests. Without a healthy Docker provider the package
// reports NOT RUN locally (exit 0) and fails under CI=true or
// TXHARBOR_REQUIRE_DOCKER=1 — an unrun drill is never a pass. Kafka-dependent
// event scenarios report NOT RUN individually via t.Skip when no broker can be
// provisioned.
//
// Evidence archive: when TXHARBOR_DRILL_EVIDENCE_DIR is set (drill.yml points
// it at the runner's temp directory) each drill test writes its S12-structured
// run record there; the workflow uploads the directory as the durable run
// artifact. Without the variable the archive lands in the test's temp dir —
// never inside the repository.
package recovery_test

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5"
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
	// drillPGImage is the pinned carrier image (ADR-002); it also supplies the
	// real pg_dump/pg_restore clients executed through docker exec.
	drillPGImage = "postgres:18.6-trixie"
	// drillAnvilImage is the pinned Anvil used by the repo's other
	// integration suites.
	drillAnvilImage = "ghcr.io/foundry-rs/foundry:v1.8.1"
	// drillChainID is the Anvil chain id (the compose default).
	drillChainID = uint64(31337)
	// drillProgramVersion is the program identity of the drill backup/restore
	// pair; the manifest floor is the same value, so the fixture manifest is
	// compatible with the drill's restore.
	drillProgramVersion = "018.0"
	// drillInternalPort is the PostgreSQL port inside the pinned container.
	drillInternalPort = "5432"
	// drillEvidenceDirEnv overrides the drill evidence archive directory
	// (drill.yml sets it to the runner temp dir). It never defaults to a
	// repository path.
	drillEvidenceDirEnv = "TXHARBOR_DRILL_EVIDENCE_DIR"

	// drillDevKeyHex is account #0 of the standard Anvil mnemonic (a public
	// test key; it holds only Anvil's disposable test ether). The drill uses
	// it to produce one real confirmed payment transaction on the local chain
	// after the backup recovery point.
	drillDevKeyHex = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

	// drillLatestConfigKey is the deployment binding checked by the
	// recovery-admin CLI; the library-level drill does not require it, but the
	// fixture names the values so an accidental future CLI coupling fails
	// loudly instead of silently reading an unconfigured state.
	drillDataTargetFingerprint = "drill-data-target"
)

// TestMain owns the drill package's Docker NOT RUN discipline. Locally without
// a provider the package reports NOT RUN and exits 0 (the Makefile guard
// cannot see this: the layer is "not run", never green — see drill.yml). Under
// CI=true or TXHARBOR_REQUIRE_DOCKER=1 the package must fail so a scheduled
// run can never be read as green without a real drill.
func TestMain(m *testing.M) {
	os.Exit(runDrillMain(m))
}

func runDrillMain(m *testing.M) int {
	ctx := context.Background()
	if !drillDockerHealthy(ctx) {
		if os.Getenv("CI") == "true" || os.Getenv("TXHARBOR_REQUIRE_DOCKER") == "1" {
			fmt.Fprintln(os.Stderr, "recovery drill: docker provider unavailable; failing the drill package (CI=true or TXHARBOR_REQUIRE_DOCKER=1): NOT RUN is not a pass (drill.yml; quickstart §3)")
			return 1
		}
		fmt.Fprintln(os.Stderr, "recovery drill: docker provider unavailable; NOT RUN (exit 0) — this is not a pass; re-run with a Docker daemon for drill evidence")
		return 0
	}
	return m.Run()
}

func drillDockerHealthy(ctx context.Context) (healthy bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "recovery drill: docker provider check panicked: %v\n", r)
			healthy = false
		}
	}()
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		fmt.Fprintf(os.Stderr, "recovery drill: docker provider: %v\n", err)
		return false
	}
	if err := provider.Health(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "recovery drill: docker health: %v\n", err)
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// Anvil harness: the real chain truth.
// ---------------------------------------------------------------------------

type drillAnvil struct {
	url    string
	client *eth.Client
	rpc    *gethrpc.Client
}

func drillStartAnvil(t *testing.T) *drillAnvil {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        drillAnvilImage,
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
	return &drillAnvil{url: rawURL, client: client, rpc: rpc}
}

func (n *drillAnvil) mustCall(t *testing.T, out any, method string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.rpc.CallContext(ctx, out, method, args...); err != nil {
		t.Fatalf("%s: %v", method, err)
	}
}

// mine mines real blocks; the PG frontier is seeded separately, so the chain
// genuinely leads the restore point.
func (n *drillAnvil) mine(t *testing.T, blocks uint64) {
	t.Helper()
	var ignored any
	n.mustCall(t, &ignored, "anvil_mine", hexutil.EncodeUint64(blocks))
}

func (n *drillAnvil) tip(t *testing.T) uint64 {
	t.Helper()
	tip, err := n.client.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf("chain tip: %v", err)
	}
	return tip
}

// sendConfirmedPayment signs and broadcasts one real (disposable, Anvil-only)
// ether transfer from the standard dev account and waits for its receipt. The
// returned hash and receipt are the external "a payment was confirmed after
// the recovery point" fact of the drill.
func (n *drillAnvil) sendConfirmedPayment(t *testing.T) (common.Hash, *types.Receipt) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key, err := crypto.HexToECDSA(drillDevKeyHex)
	if err != nil {
		t.Fatalf("parse anvil dev key: %v", err)
	}
	var nonceHex hexutil.Uint64
	n.mustCall(t, &nonceHex, "eth_getTransactionCount", crypto.PubkeyToAddress(key.PublicKey), "latest")

	to := crypto.PubkeyToAddress(drillSecondDevKey(t).PublicKey)
	chainID := new(big.Int).SetUint64(drillChainID)
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     uint64(nonceHex),
		GasTipCap: big.NewInt(1_000_000_000),
		GasFeeCap: big.NewInt(2_000_000_000),
		Gas:       21_000,
		To:        &to,
		Value:     big.NewInt(1),
	})
	signed, err := types.SignTx(tx, types.LatestSignerForChainID(chainID), key)
	if err != nil {
		t.Fatalf("sign payment tx: %v", err)
	}
	raw, err := signed.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal payment tx: %v", err)
	}
	if _, err := n.client.SendSignedTransaction(ctx, raw, signed.Hash()); err != nil {
		t.Fatalf("broadcast payment tx: %v", err)
	}
	// Anvil automines, but a fresh receipt may still race the RPC; poll
	// bounded instead of sleeping a fixed amount.
	deadline := time.Now().Add(15 * time.Second)
	for {
		receipt, err := n.client.TransactionReceipt(ctx, signed.Hash())
		if err == nil {
			if receipt.Status != types.ReceiptStatusSuccessful {
				t.Fatalf("payment tx %s reverted (status=%d)", signed.Hash(), receipt.Status)
			}
			return signed.Hash(), receipt
		}
		if time.Now().After(deadline) {
			t.Fatalf("payment tx %s receipt not available: %v", signed.Hash(), err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func drillSecondDevKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	// Account #1 of the standard Anvil mnemonic (public test key).
	key, err := crypto.HexToECDSA("59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d")
	if err != nil {
		t.Fatalf("parse second anvil dev key: %v", err)
	}
	return key
}

// ---------------------------------------------------------------------------
// Container-backed PGCommand (real pg_dump/pg_restore from the pinned image).
// ---------------------------------------------------------------------------

type drillPGCommand struct {
	t     *testing.T
	ctrID string
}

func (c *drillPGCommand) Run(ctx context.Context, name string, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	dockerArgs := append([]string{"exec", "-i", c.ctrID, name}, drillRewriteDSNArgs(args)...)
	cmd := exec.CommandContext(ctx, "docker", dockerArgs...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("run %s in pinned container: %w", name, err)
	}
	return nil
}

func drillRewriteDSNArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		out = append(out, drillRewriteDSNArg(arg))
	}
	return out
}

func drillRewriteDSNArg(arg string) string {
	for _, scheme := range []string{"postgres://", "postgresql://"} {
		idx := strings.Index(arg, scheme)
		if idx < 0 {
			continue
		}
		u, err := url.Parse(arg[idx:])
		if err != nil {
			return arg
		}
		u.Host = net.JoinHostPort("127.0.0.1", drillInternalPort)
		return arg[:idx] + u.String()
	}
	return arg
}

// ---------------------------------------------------------------------------
// Fixture: one PG container, one live/source data DB, one control DB, one
// open recovery instance with executor/verifier/approver identities.
// ---------------------------------------------------------------------------

type drillEnv struct {
	t   *testing.T
	ctx context.Context

	ctrID    string
	adminDSN string
	admin    *pgxpool.Pool

	// data is the live (source) data database: the environment whose recovery
	// point the drill backs up. It is never written by the recovery flow.
	dataDSN string
	data    *pgxpool.Pool

	ctrlDSN string
	ctrl    *pgxpool.Pool
	store   *controlstore.Store

	instanceID string
	artDir     string
	pg         recovery.PGCommand

	anvil *drillAnvil
	// broker carries the optional real broker committed-offset reader (the
	// event-layer scenarios); nil keeps V5/V6 conservative (unknown).
	broker sources.BrokerOffsetReader
	seq    int
}

// newDrillEnv boots the fixture. withAnvil starts the real chain; tests that
// do not exercise chain facts (F1/F2/F3 backup/restore failures) pass false so
// the drill layer stays as fast as it is allowed to be.
func newDrillEnv(t *testing.T, withAnvil bool) *drillEnv {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, drillPGImage,
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
	e := &drillEnv{
		t:        t,
		ctx:      ctx,
		ctrID:    ctr.GetContainerID(),
		adminDSN: adminDSN,
		artDir:   t.TempDir(),
	}
	e.admin = e.openPool(adminDSN)
	e.pg = &drillPGCommand{t: t, ctrID: e.ctrID}

	// Live data database: the real repository migrations (the manifest records
	// the exact goose set; restore probes compare against it).
	e.dataDSN = e.createDatabase("data")
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: e.dataDSN, LockTimeout: 30 * time.Second, ConnectTimeout: 5 * time.Second,
	}, io.Discard); err != nil {
		t.Fatalf("migrate data database: %v", err)
	}
	e.data = e.openPool(e.dataDSN)

	// Control store: an independent database with its own goose sequence
	// (ADR-001); the data backup never contains recovery_* tables.
	e.ctrlDSN = e.createDatabase("ctrl")
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: e.ctrlDSN, LockTimeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, FS: schema.FS,
	}, io.Discard); err != nil {
		t.Fatalf("migrate control database: %v", err)
	}
	e.ctrl = e.openPool(e.ctrlDSN)
	store, err := controlstore.NewStore(ctx, e.ctrl)
	if err != nil {
		t.Fatalf("controlstore.NewStore: %v", err)
	}
	e.store = store

	// One open recovery instance through the real entry point.
	opened, err := recovery.OpenInstance(ctx, store, recovery.OpenInstanceRequest{
		Kind:     "recovery",
		OpenedBy: "deploy:executor",
		Reason:   "drill: isolated recovery environment (local test input only)",
	})
	if err != nil {
		t.Fatalf("open recovery instance: %v", err)
	}
	e.instanceID = opened.InstanceID
	e.mapIdentity("deploy:admin", "person-admin")
	e.mapIdentity("deploy:executor", "person-executor")
	e.mapIdentity("auth:verifier", "person-verifier")
	e.mapIdentity("auth:approver", "person-approver")
	e.mapIdentity("auth:approver2", "person-approver-2")
	e.register("deploy:executor", "executor")
	e.register("auth:verifier", "verifier")
	e.register("auth:approver", "approver")
	e.register("auth:approver2", "approver")

	if withAnvil {
		e.anvil = drillStartAnvil(t)
	}
	return e
}

func (e *drillEnv) operation(prefix string) string {
	e.seq++
	return fmt.Sprintf("drill-%s-%d", prefix, e.seq)
}

func (e *drillEnv) mapIdentity(principal, person string) {
	e.t.Helper()
	if _, err := e.store.SetIdentityMapping(e.ctx, controlstore.SetIdentityMappingRequest{
		Principal: principal, PersonID: person, RecordedBy: "deploy:admin",
		OperationID: e.operation("map"),
	}); err != nil {
		e.t.Fatalf("map identity %s -> %s: %v", principal, person, err)
	}
}

func (e *drillEnv) register(principal, role string) {
	e.t.Helper()
	if _, err := e.store.RegisterParticipant(e.ctx, controlstore.RegisterParticipantRequest{
		InstanceID: e.instanceID, Principal: principal, Role: role,
		Actor: "deploy:admin", OperationID: e.operation("reg"),
	}); err != nil {
		e.t.Fatalf("register %s as %s: %v", principal, role, err)
	}
}

func (e *drillEnv) createDatabase(label string) string {
	e.t.Helper()
	e.seq++
	name := fmt.Sprintf("drill_%s_%d", label, e.seq)
	if _, err := e.admin.Exec(e.ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		e.t.Fatalf("create database %s: %v", name, err)
	}
	return drillDSNFor(e.t, e.adminDSN, name)
}

func (e *drillEnv) openPool(dsn string) *pgxpool.Pool {
	e.t.Helper()
	pool, err := db.OpenPool(context.Background(), dsn, 5*time.Second)
	if err != nil {
		e.t.Fatalf("open pool: %v", err)
	}
	e.t.Cleanup(pool.Close)
	return pool
}

// ---------------------------------------------------------------------------
// Controlled data preparation (labeled test data, never verification doubles).
// ---------------------------------------------------------------------------

// seedLiveBusinessState writes the small controlled business data set the
// backup will capture: one indexed chain frontier, one withdrawal request
// without a provable payment intent, one historical unpublished outbox event
// and one consumer progress row. These are data-preparation rows only; no
// verification source reads them through anything but its real adapter.
func (e *drillEnv) seedLiveBusinessState() {
	e.t.Helper()
	if _, err := e.data.Exec(e.ctx,
		`INSERT INTO chain_blocks (chain_id, number, hash, parent_hash)
		 VALUES ($1, 1, $2, $3)`,
		drillChainID, drillBlockHash(1), drillBlockHash(0)); err != nil {
		e.t.Fatalf("seed chain block 1: %v", err)
	}
	if _, err := e.data.Exec(e.ctx,
		`INSERT INTO indexer_checkpoint (chain_id, height, block_hash, start_height)
		 VALUES ($1, 1, $2, 0)`,
		drillChainID, drillBlockHash(1)); err != nil {
		e.t.Fatalf("seed indexer checkpoint: %v", err)
	}
	if _, err := e.data.Exec(e.ctx,
		`INSERT INTO caller (caller_id, label) VALUES (1, 'drill-fixture')`); err != nil {
		e.t.Fatalf("seed caller: %v", err)
	}
	if _, err := e.data.Exec(e.ctx,
		`INSERT INTO withdrawal_requests
		 (request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
		 VALUES ('drill-req-1', 1, 'drill-idem-1', 'drill-auth-1', $1, $2, $3, 1000)`,
		drillChainID, drillAddress("a"), drillAddress("b")); err != nil {
		e.t.Fatalf("seed withdrawal request: %v", err)
	}
	if _, err := e.data.Exec(e.ctx,
		`INSERT INTO outbox_events
		 (event_id, identity_kind, event_type, schema_version, aggregate_type,
		  aggregate_id, aggregate_version, payload, payload_hash, occurred_at, publish_state)
		 VALUES (gen_random_uuid(), 'business_object', 'withdrawal_requested', 1,
		         'withdrawal_request', 'drill-req-1', 1, '{}'::jsonb, 'drill-payload-hash-1', now(), 'pending')`); err != nil {
		e.t.Fatalf("seed pending outbox: %v", err)
	}
	if _, err := e.data.Exec(e.ctx,
		`INSERT INTO consumer_progress (consumer_name, topic, partition, next_offset)
		 VALUES ('drill-consumer', 'txharbor.events.v1', 0, 5)`); err != nil {
		e.t.Fatalf("seed consumer progress: %v", err)
	}
}

// advanceExternally is the post-recovery-point external progression: one real
// confirmed payment on Anvil, the live instance's observed transfer log and
// indexed frontier, one signed/broadcast withdrawal action and one downstream
// consumption. Every row here is a controlled data-preparation fact; none of
// them may ever be re-created or re-executed by the recovery flow.
func (e *drillEnv) advanceExternally() drillExternalAdvance {
	e.t.Helper()
	if e.anvil == nil {
		e.t.Fatal("advanceExternally requires the anvil fixture")
	}
	txHash, receipt := e.anvil.sendConfirmedPayment(e.t)
	e.anvil.mine(e.t, 5)

	// The live indexer observed the confirmed transfer and moved its frontier
	// (block 2 is a real mined block; the row is the controlled indexed view).
	if _, err := e.data.Exec(e.ctx,
		`INSERT INTO chain_blocks (chain_id, number, hash, parent_hash)
		 VALUES ($1, 2, $2, $3)`,
		drillChainID, drillBlockHash(2), drillBlockHash(1)); err != nil {
		e.t.Fatalf("seed chain block 2: %v", err)
	}
	if _, err := e.data.Exec(e.ctx,
		`INSERT INTO erc20_transfer_logs
		 (chain_id, block_number, block_hash, tx_hash, log_index, contract, topic0, topic1, topic2, data)
		 VALUES ($1, 2, $2, $3, 0, $4, $5, $6, $7, $8)`,
		drillChainID, drillBlockHash(2), txHash.Hex(), drillAddress("c"),
		drillTransferTopic(), drillPaddedAddress("d"), drillPaddedAddress("e"), drillPaddedWord(500)); err != nil {
		e.t.Fatalf("seed transfer log: %v", err)
	}
	if _, err := e.data.Exec(e.ctx,
		`UPDATE indexer_checkpoint SET height = 2, block_hash = $2 WHERE chain_id = $1`,
		drillChainID, drillBlockHash(2)); err != nil {
		e.t.Fatalf("advance indexer checkpoint: %v", err)
	}

	// A signed/broadcast withdrawal action of the live instance.
	if _, err := e.data.Exec(e.ctx,
		`INSERT INTO signer_caller (caller_id, label) VALUES (1, 'drill-live-instance')`); err != nil {
		e.t.Fatalf("seed signer caller: %v", err)
	}
	if _, err := e.data.Exec(e.ctx, `
INSERT INTO signing_requests
    (caller_id, signing_request_id, attempt_id, intent_id, binding_ref, chain_id, sender, nonce,
     tx_type, to_addr, value, data, gas_limit, max_fee_per_gas, max_priority_fee_per_gas,
     asset, recipient, amount, canonical_envelope, content_hash, authorization_id,
     authorization_fingerprint, authorization_state, policy_version, state)
VALUES (1, 'drill-sr-1', 'drill-attempt-1', 'drill-intent-1', 'drill-binding-1', $1, $2, 0,
        2, $3, 0, '\x'::bytea, 21000, 2000000000, 1000000000,
        $4, $3, 500, '{"kind":"drill"}', $5, 'drill-auth-1',
        $6, 'valid', 'drill-policy-v1', 'signed')`,
		drillChainID, drillAddress("f"), drillAddress("b"), drillAddress("a"),
		drillHex64("1"), drillFingerprintHex("2")); err != nil {
		e.t.Fatalf("seed signing request: %v", err)
	}
	if _, err := e.data.Exec(e.ctx,
		`INSERT INTO signature_results (signing_request_row, signature, tx_hash)
		 SELECT id, $1, $2 FROM signing_requests WHERE signing_request_id = 'drill-sr-1'`,
		drillSignatureHex(), drillHex64("3")); err != nil {
		e.t.Fatalf("seed signature result: %v", err)
	}

	// Downstream consumption happened after the recovery point.
	if _, err := e.data.Exec(e.ctx, `
INSERT INTO consumer_inbox
    (consumer_name, event_id, aggregate_type, aggregate_id, aggregate_version, topic, partition, "offset")
VALUES ('drill-consumer', gen_random_uuid(), 'withdrawal_request', 'drill-req-1', 1,
        'txharbor.events.v1', 0, 5)`); err != nil {
		e.t.Fatalf("seed consumer inbox: %v", err)
	}
	if _, err := e.data.Exec(e.ctx,
		`UPDATE consumer_progress SET next_offset = 6
		  WHERE consumer_name = 'drill-consumer' AND topic = 'txharbor.events.v1' AND partition = 0`); err != nil {
		e.t.Fatalf("advance consumer progress: %v", err)
	}
	return drillExternalAdvance{TxHash: txHash.Hex(), BlockNumber: receipt.BlockNumber.Uint64()}
}

// drillExternalAdvance identifies the real external payment fact for evidence.
type drillExternalAdvance struct {
	TxHash      string `json:"tx_hash"`
	BlockNumber uint64 `json:"block_number"`
}

// ---------------------------------------------------------------------------
// Verification source assembly (real T039/T040 adapters).
// ---------------------------------------------------------------------------

type drillProbe struct {
	name  string
	check func(ctx context.Context) error
}

func (p drillProbe) Name() string                    { return p.name }
func (p drillProbe) Check(ctx context.Context) error { return p.check(ctx) }

func (e *drillEnv) dependencyProbes() []sources.DependencyProbe {
	e.t.Helper()
	probes := []sources.DependencyProbe{
		drillProbe{name: "control-store", check: func(ctx context.Context) error {
			if err := e.ctrl.Ping(ctx); err != nil {
				return fmt.Errorf("control store unreachable: %w", err)
			}
			return nil
		}},
	}
	if e.anvil != nil {
		probes = append(probes, drillProbe{name: "chain-rpc", check: func(ctx context.Context) error {
			if _, err := e.anvil.client.BlockNumber(ctx); err != nil {
				return fmt.Errorf("chain RPC unreachable: %w", err)
			}
			return nil
		}})
	}
	return probes
}

// sourcesFor assembles the real adapters over the given data pool (the live
// database or the recovered target — the drill verifies the environment it
// restored). A nil RPC leaves the chain-dependent categories conservative.
func (e *drillEnv) sourcesFor(data *pgxpool.Pool) []recovery.VerificationSource {
	e.t.Helper()
	var rpc *eth.Client
	if e.anvil != nil {
		rpc = e.anvil.client
	}
	chain, err := sources.NewChainSources(sources.ChainOptions{
		Data:       data,
		RPC:        rpc,
		ChainID:    drillChainID,
		DataTarget: drillDataTargetFingerprint,
	})
	if err != nil {
		e.t.Fatalf("NewChainSources: %v", err)
	}
	ops, err := sources.NewOpsSources(sources.OpsOptions{
		Data:          data,
		Control:       e.store,
		RPC:           rpc,
		BrokerOffsets: e.broker, // nil = broker unreadable: V5/V6 stay conservative
		Dependencies:  e.dependencyProbes(),
		DataTarget:    drillDataTargetFingerprint,
	})
	if err != nil {
		e.t.Fatalf("NewOpsSources: %v", err)
	}
	all := append(chain, ops...)
	if len(all) == 0 {
		e.t.Fatal("the drill source assembly returned no V-sources")
	}
	return all
}

// verify runs one bounded real verification step and returns the persisted
// batch; the caller measures the elapsed time.
func (e *drillEnv) verify(data *pgxpool.Pool, operationID string) recovery.VerificationBatch {
	e.t.Helper()
	verification, err := recovery.NewVerification(e.store, recovery.VerificationOptions{
		Tolerance: time.Hour, BatchLimit: 1000,
	})
	if err != nil {
		e.t.Fatalf("NewVerification: %v", err)
	}
	batch, err := verification.Verify(e.ctx, recovery.VerificationRequest{
		InstanceID:  e.instanceID,
		Actor:       "deploy:executor",
		Scope:       []byte(`{"purpose":"drill verification scope (local test input only)"}`),
		OperationID: operationID,
		Sources:     e.sourcesFor(data),
	})
	if err != nil {
		e.t.Fatalf("Verify: %v", err)
	}
	if batch.Discarded {
		e.t.Fatalf("uncontended drill verification was discarded: %+v", batch)
	}
	return batch
}

// ---------------------------------------------------------------------------
// Real backup / verify-backup / restore entry points (the recovery flow).
// ---------------------------------------------------------------------------

// backup runs the real backup executor (real pg_dump through the pinned
// container) over the live data database.
func (e *drillEnv) backup() recovery.BackupResult {
	e.t.Helper()
	result, err := recovery.ExecuteBackup(e.ctx, recovery.BackupOptions{
		DSN:                  e.dataDSN,
		ArtifactDir:          e.artDir,
		ChainID:              fmt.Sprintf("%d", drillChainID),
		CreatedBy:            "deploy:executor",
		ProgramVersion:       drillProgramVersion,
		ProgramMinCompatible: drillProgramVersion,
		PG:                   e.pg,
	})
	if err != nil {
		e.t.Fatalf("ExecuteBackup: %v", err)
	}
	return result
}

// verifyBackup runs the real isolated verify-backup path and requires the
// verified conclusion (a rejected/unverified result fails the caller).
func (e *drillEnv) verifyBackup(manifestPath, targetDSN, operationID string) recovery.VerifyBackupResult {
	e.t.Helper()
	result, err := recovery.ExecuteVerifyBackup(e.ctx, recovery.VerifyBackupOptions{
		ManifestPath:   manifestPath,
		TargetDSN:      targetDSN,
		Verifier:       "auth:verifier",
		InstanceID:     e.instanceID,
		ControlStore:   e.store,
		ControlDSN:     e.ctrlDSN,
		ProgramVersion: drillProgramVersion,
		OperationID:    operationID,
		PG:             e.pg,
	})
	if err != nil {
		e.t.Fatalf("ExecuteVerifyBackup: %v", err)
	}
	if result.State != recovery.VerificationVerified {
		e.t.Fatalf("verify-backup state = %s, want verified (checks=%+v)", result.State, result.Checks)
	}
	return result
}

// restore runs the real restore executor (real pg_restore through the pinned
// container) into the explicit isolated target.
func (e *drillEnv) restore(manifestPath, targetDSN, operationID string) recovery.RestoreResult {
	e.t.Helper()
	result, err := recovery.ExecuteRestore(e.ctx, recovery.RestoreOptions{
		ManifestPath:      manifestPath,
		InstanceID:        e.instanceID,
		ControlStore:      e.store,
		ControlDSN:        e.ctrlDSN,
		TargetDSN:         targetDSN,
		TargetDeclaration: recovery.TargetIsolated,
		TargetReason:      "drill: isolated recovery environment",
		Actor:             "deploy:executor",
		ProgramVersion:    drillProgramVersion,
		OperationID:       operationID,
		PG:                e.pg,
	})
	if err != nil {
		e.t.Fatalf("ExecuteRestore: %v", err)
	}
	if !result.Restored {
		e.t.Fatalf("ExecuteRestore restored=false: %+v", result)
	}
	return result
}

// refusedRestore runs a restore that must be refused: non-nil error,
// Restored=false, and the caller inspects the blocked preconditions. The
// running program identity is parameterized so the F3 compatibility refusal
// can be exercised without touching the manifest bytes.
func (e *drillEnv) refusedRestore(manifestPath, targetDSN, programVersion, operationID string) (recovery.RestoreResult, error) {
	e.t.Helper()
	result, err := recovery.ExecuteRestore(e.ctx, recovery.RestoreOptions{
		ManifestPath:      manifestPath,
		InstanceID:        e.instanceID,
		ControlStore:      e.store,
		ControlDSN:        e.ctrlDSN,
		TargetDSN:         targetDSN,
		TargetDeclaration: recovery.TargetIsolated,
		Actor:             "deploy:executor",
		ProgramVersion:    programVersion,
		OperationID:       operationID,
		PG:                e.pg,
	})
	if err == nil {
		e.t.Fatal("ExecuteRestore succeeded, want a fail-closed refusal")
	}
	if result.Restored {
		e.t.Fatal("a refused restore reports Restored=true")
	}
	if len(result.Blocked) == 0 {
		e.t.Fatalf("a refused restore carried no blocked preconditions: %+v", result)
	}
	return result, err
}

// evidenceCount counts evidence rows of one kind for the instance.
func (e *drillEnv) evidenceCount(kind string) int {
	e.t.Helper()
	var n int
	if err := e.ctrl.QueryRow(e.ctx,
		`SELECT count(*) FROM recovery_evidence WHERE instance_id = $1 AND kind = $2`,
		e.instanceID, kind).Scan(&n); err != nil {
		e.t.Fatalf("count evidence (%s): %v", kind, err)
	}
	return n
}

// advanceEvidence records one real generation-advancing write through the gap
// entry point (a fresh, fully specified handling package). Tests use it to
// make an earlier approval/release stale; nothing here writes decision state.
func (e *drillEnv) advanceEvidence(label string) recovery.Gap {
	e.t.Helper()
	gaps, err := recovery.NewGaps(e.store)
	if err != nil {
		e.t.Fatalf("NewGaps: %v", err)
	}
	gap, err := gaps.Open(e.ctx, recovery.OpenGapRequest{
		InstanceID:       e.instanceID,
		ObjectKey:        "drill:evidence-advance:" + label,
		Scope:            []byte(`{"chain_id":31337,"object":"drill:evidence-advance"}`),
		Timeline:         []byte(`{"observed_at":"local drill"}`),
		ExistingEvidence: []byte(`{"local":"test input"}`),
		RequiredEvidence: []byte(`{"external":["local drill test input only"]}`),
		Risk:             []byte(`{"unproven_external_effect":true}`),
		AffectedCapabilities: []recovery.Capability{
			recovery.CapabilityEventConsuming,
		},
		Owner:       "person-owner-1",
		Actor:       "deploy:executor",
		OperationID: e.operation("advance"),
	})
	if err != nil {
		e.t.Fatalf("advanceEvidence(%s): %v", label, err)
	}
	return gap
}

// checklistSetOnly collects evidence for one item without a non-executor
// verdict: the item is evidenced, never verified.
func (e *drillEnv) checklistSetOnly(capability recovery.Capability, item recovery.IsolationItemKey) {
	e.t.Helper()
	checklist, err := recovery.NewChecklist(e.store)
	if err != nil {
		e.t.Fatalf("NewChecklist: %v", err)
	}
	if _, err := checklist.Set(e.ctx, recovery.ChecklistEvidenceRequest{
		InstanceID: e.instanceID, ItemKey: item,
		State:       recovery.ChecklistStateEvidenced,
		EvidenceRef: fmt.Sprintf("drill:isolation-evidence-only/%s", item),
		Actor:       "deploy:executor",
		OperationID: e.operation("checklist-set-only"),
	}); err != nil {
		e.t.Fatalf("checklist.Set(%s/%s): %v", capability, item, err)
	}
}

// remapIdentity changes one principal's active person mapping through the
// real entry point (the old approvals bound to the old person become
// unverifiable; F19).
func (e *drillEnv) remapIdentity(principal, person string) controlstore.IdentityMappingChange {
	e.t.Helper()
	change, err := e.store.SetIdentityMapping(e.ctx, controlstore.SetIdentityMappingRequest{
		Principal: principal, PersonID: person, RecordedBy: "deploy:admin",
		OperationID: e.operation("remap"),
	})
	if err != nil {
		e.t.Fatalf("remap identity %s -> %s: %v", principal, person, err)
	}
	return change
}

// dbNameOf extracts the database name of a DSN on the fixture server.
func (e *drillEnv) dbNameOf(dsn string) string {
	e.t.Helper()
	base, err := url.Parse(e.adminDSN)
	if err != nil {
		e.t.Fatalf("parse base dsn: %v", err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		e.t.Fatalf("parse dsn: %v", err)
	}
	if parsed.Host != base.Host {
		e.t.Fatalf("dsn %s is not on the fixture server", parsed.Host)
	}
	return strings.TrimPrefix(parsed.Path, "/")
}

// rebuildDatabase drops (FORCE) and recreates one target database empty: the
// documented retry after an interrupted restore (F2).
func (e *drillEnv) rebuildDatabase(dsn string) string {
	e.t.Helper()
	name := e.dbNameOf(dsn)
	if _, err := e.admin.Exec(e.ctx,
		"DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
		e.t.Fatalf("drop database %s: %v", name, err)
	}
	if _, err := e.admin.Exec(e.ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		e.t.Fatalf("recreate database %s: %v", name, err)
	}
	return dsn
}

// userTableCount counts ordinary user tables in the public schema: a refused
// restore must leave it at zero (no best-effort recovery).
func (e *drillEnv) userTableCount(dsn string) int {
	e.t.Helper()
	conn, err := pgx.Connect(e.ctx, dsn)
	if err != nil {
		e.t.Fatalf("connect %s: %v", dsn, err)
	}
	defer func() { _ = conn.Close(e.ctx) }()
	var n int
	if err := conn.QueryRow(e.ctx,
		`SELECT count(*) FROM pg_class
		  WHERE relkind = 'r' AND relnamespace = 'public'::regnamespace
		    AND relname NOT LIKE 'pg_%'`).Scan(&n); err != nil {
		e.t.Fatalf("user table count: %v", err)
	}
	return n
}

// gooseVersions reads the exact applied goose set of a database.
func (e *drillEnv) gooseVersions(dsn string) []int64 {
	e.t.Helper()
	conn, err := pgx.Connect(e.ctx, dsn)
	if err != nil {
		e.t.Fatalf("connect %s: %v", dsn, err)
	}
	defer func() { _ = conn.Close(e.ctx) }()
	rows, err := conn.Query(e.ctx,
		`SELECT version_id FROM goose_db_version WHERE is_applied AND version_id > 0 ORDER BY version_id`)
	if err != nil {
		e.t.Fatalf("read goose versions: %v", err)
	}
	defer rows.Close()
	var versions []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			e.t.Fatalf("scan goose version: %v", err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		e.t.Fatalf("read goose versions: %v", err)
	}
	return versions
}

// controlEvidenceCount counts evidence rows of one kind bound to one backup.
func (e *drillEnv) controlEvidenceCount(kind, backupID string) int {
	e.t.Helper()
	var n int
	if err := e.ctrl.QueryRow(e.ctx,
		`SELECT count(*) FROM recovery_evidence
		  WHERE instance_id = $1 AND kind = $2 AND scope->>'backup_id' = $3`,
		e.instanceID, kind, backupID).Scan(&n); err != nil {
		e.t.Fatalf("count control evidence (%s/%s): %v", kind, backupID, err)
	}
	return n
}

// openGaps lists the open/escalated gaps of the drill instance through the
// real gap service (no SQL state writes).
func (e *drillEnv) openGaps() []recovery.Gap {
	e.t.Helper()
	gaps, err := recovery.NewGaps(e.store)
	if err != nil {
		e.t.Fatalf("NewGaps: %v", err)
	}
	all, err := gaps.List(e.ctx, e.instanceID)
	if err != nil {
		e.t.Fatalf("Gaps.List: %v", err)
	}
	var blocking []recovery.Gap
	for _, gap := range all {
		if gap.State == recovery.GapStateOpen || gap.State == recovery.GapStateEscalated {
			blocking = append(blocking, gap)
		}
	}
	return blocking
}

// ---------------------------------------------------------------------------
// Control-store entry points (no direct decision writes).
// ---------------------------------------------------------------------------

func (e *drillEnv) scope(capability recovery.Capability) string {
	e.t.Helper()
	scope, err := recovery.CapabilityScope(drillChainID, capability)
	if err != nil {
		e.t.Fatalf("capability scope %s: %v", capability, err)
	}
	return scope
}

func (e *drillEnv) gate() *recovery.Gate {
	e.t.Helper()
	gate, err := recovery.NewGate(e.store, recovery.GateOptions{TTL: time.Minute})
	if err != nil {
		e.t.Fatalf("NewGate: %v", err)
	}
	return gate
}

func (e *drillEnv) admit(gate *recovery.Gate, capability recovery.Capability) recovery.GateDecision {
	e.t.Helper()
	decision, err := gate.Admit(e.ctx, recovery.GateRequest{
		InstanceID:  e.instanceID,
		Capability:  capability,
		ScopeHash:   e.scope(capability),
		Actor:       "deploy:executor",
		OperationID: e.operation("admit"),
		Action:      "drill",
	})
	if err != nil {
		e.t.Fatalf("gate.Admit(%s): %v", capability, err)
	}
	return decision
}

// checklistVerified collects evidence (executor) and records the non-executor
// verdict for every isolation item of the capability, through the real
// checklist entry points.
func (e *drillEnv) checklistVerified(capability recovery.Capability) {
	e.t.Helper()
	checklist, err := recovery.NewChecklist(e.store)
	if err != nil {
		e.t.Fatalf("NewChecklist: %v", err)
	}
	items, err := recovery.IsolationDependencySet(capability)
	if err != nil {
		e.t.Fatalf("IsolationDependencySet(%s): %v", capability, err)
	}
	for _, item := range items {
		// Items shared between capabilities (for example old_writers_stopped)
		// are collected once: a verified item is never re-collected silently.
		current, found, err := checklist.Item(e.ctx, e.instanceID, item)
		if err != nil {
			e.t.Fatalf("checklist.Item(%s/%s): %v", capability, item, err)
		}
		if !found || current.State != recovery.ChecklistStateEvidenced {
			if found && current.State == recovery.ChecklistStateVerified {
				continue
			}
			if _, err := checklist.Set(e.ctx, recovery.ChecklistEvidenceRequest{
				InstanceID: e.instanceID, ItemKey: item,
				State:       recovery.ChecklistStateEvidenced,
				EvidenceRef: fmt.Sprintf("drill:isolation/%s/%s", capability, item),
				Actor:       "deploy:executor",
				OperationID: e.operation("checklist-set"),
			}); err != nil {
				e.t.Fatalf("checklist.Set(%s/%s): %v", capability, item, err)
			}
		}
		if _, err := checklist.Verify(e.ctx, recovery.ChecklistVerifyRequest{
			InstanceID: e.instanceID, ItemKey: item,
			Actor:       "auth:verifier",
			OperationID: e.operation("checklist-verify"),
		}); err != nil {
			e.t.Fatalf("checklist.Verify(%s/%s): %v", capability, item, err)
		}
	}
}

func (e *drillEnv) approve(capability recovery.Capability, principal string) recovery.ApprovalOutcome {
	e.t.Helper()
	outcome, err := recovery.Approve(e.ctx, e.store, recovery.ApprovalRequest{
		InstanceID: e.instanceID, Capability: capability, ScopeHash: e.scope(capability),
		Principal: principal, Reason: "drill approval (local test input only)",
		OperationID: e.operation("approve"),
	})
	if err != nil {
		e.t.Fatalf("Approve(%s by %s): %v", capability, principal, err)
	}
	return outcome
}

func (e *drillEnv) release(gate *recovery.Gate, capability recovery.Capability) recovery.ReleaseOutcome {
	e.t.Helper()
	outcome, err := recovery.Release(e.ctx, e.store, gate, recovery.ReleaseRequest{
		InstanceID: e.instanceID, Capability: capability, ScopeHash: e.scope(capability),
		Principal: "deploy:executor", Reason: "drill release (local test input only)",
		OperationID: e.operation("release"),
	})
	if err != nil {
		e.t.Fatalf("Release(%s): %v", capability, err)
	}
	return outcome
}

// tryApprove runs one approval attempt without asserting its outcome: refusal
// tests inspect the closed class and the audited zero-write behavior.
func (e *drillEnv) tryApprove(capability recovery.Capability, principal, operationID string) (recovery.ApprovalOutcome, error) {
	e.t.Helper()
	return recovery.Approve(e.ctx, e.store, recovery.ApprovalRequest{
		InstanceID: e.instanceID, Capability: capability, ScopeHash: e.scope(capability),
		Principal: principal, Reason: "drill refusal probe (local test input only)",
		OperationID: operationID,
	})
}

// tryRelease runs one release attempt without asserting its outcome.
func (e *drillEnv) tryRelease(gate *recovery.Gate, capability recovery.Capability, operationID string) (recovery.ReleaseOutcome, error) {
	e.t.Helper()
	return recovery.Release(e.ctx, e.store, gate, recovery.ReleaseRequest{
		InstanceID: e.instanceID, Capability: capability, ScopeHash: e.scope(capability),
		Principal: "deploy:executor", Reason: "drill refusal probe (local test input only)",
		OperationID: operationID,
	})
}

// tryRevokeRelease runs one release revocation attempt without asserting its
// outcome (the caller checks authorization and auditability).
func (e *drillEnv) tryRevokeRelease(gate *recovery.Gate, capability recovery.Capability, principal, operationID string) (recovery.ReleaseOutcome, error) {
	e.t.Helper()
	return recovery.RevokeRelease(e.ctx, e.store, gate, recovery.ReleaseRequest{
		InstanceID: e.instanceID, Capability: capability, ScopeHash: e.scope(capability),
		Principal: principal, Reason: "drill revoke probe (local test input only)",
		OperationID: operationID,
	})
}

// decisionRowCount counts append-only decision rows of one stream.
func (e *drillEnv) decisionRowCount(table string, capability recovery.Capability, principal string) int {
	e.t.Helper()
	if table != "recovery_approval" && table != "recovery_release" {
		e.t.Fatalf("unsupported decision table %q", table)
	}
	query := `SELECT count(*) FROM ` + table + ` WHERE instance_id = $1 AND capability = $2`
	args := []any{e.instanceID, string(capability)}
	if table == "recovery_approval" {
		query += ` AND principal = $3`
		args = append(args, principal)
	}
	var n int
	if err := e.ctrl.QueryRow(e.ctx, query, args...).Scan(&n); err != nil {
		e.t.Fatalf("count %s rows: %v", table, err)
	}
	return n
}

// dbNow reads the fixture database server's clock (the clock every persisted
// timestamp is written with). Timing comparisons across recorded facts use
// this clock, never the host's, so a host/container clock offset can never
// fabricate a negative measurement.
func (e *drillEnv) dbNow(dsn string) time.Time {
	e.t.Helper()
	conn, err := pgx.Connect(e.ctx, dsn)
	if err != nil {
		e.t.Fatalf("connect %s: %v", dsn, err)
	}
	defer func() { _ = conn.Close(e.ctx) }()
	var now time.Time
	if err := conn.QueryRow(e.ctx, `SELECT now()`).Scan(&now); err != nil {
		e.t.Fatalf("read database clock: %v", err)
	}
	return now
}

// restoreStartedAt reads the latest accepted restore-start marker (the real
// restore's timing origin, written by the restore entry point on the control
// store's clock).
func (e *drillEnv) restoreStartedAt() time.Time {
	e.t.Helper()
	var at time.Time
	if err := e.ctrl.QueryRow(e.ctx,
		`SELECT created_at FROM recovery_audit
		  WHERE instance_id = $1 AND action = $2 AND result = 'ok'
		  ORDER BY audit_id DESC LIMIT 1`,
		e.instanceID, recovery.ActionRestoreStarted).Scan(&at); err != nil {
		e.t.Fatalf("read restore start marker: %v", err)
	}
	return at
}

// releaseSecondsOf reads the persisted release row's created_at and returns
// its distance from the real restore-start marker — a recorded fact pair on
// one database clock, never a host wall-clock guess.
func (e *drillEnv) releaseSecondsOf(releaseID string, restoreStarted time.Time) float64 {
	e.t.Helper()
	seconds := e.releaseCreatedAt(releaseID).Sub(restoreStarted).Seconds()
	if seconds < 0 {
		e.t.Fatalf("release %s predates the restore start marker (%.3fs); refusing a negative safe-resumption measurement", releaseID, seconds)
	}
	return seconds
}

// countWhere runs a read-only count query on one pool.
func (e *drillEnv) countWhere(pool *pgxpool.Pool, query string) int {
	e.t.Helper()
	var n int
	if err := pool.QueryRow(e.ctx, query).Scan(&n); err != nil {
		e.t.Fatalf("count (%s): %v", query, err)
	}
	return n
}

// auditCount counts audit rows for one action/result on the instance.
func (e *drillEnv) auditCount(action, result string) int {
	e.t.Helper()
	var n int
	if err := e.ctrl.QueryRow(e.ctx,
		`SELECT count(*) FROM recovery_audit WHERE instance_id = $1 AND action = $2 AND result = $3`,
		e.instanceID, action, result).Scan(&n); err != nil {
		e.t.Fatalf("count audit %s/%s: %v", action, result, err)
	}
	return n
}

// ---------------------------------------------------------------------------
// Effect-side assertions.
// ---------------------------------------------------------------------------

// drillEffectTables is the drill's own frozen effect-side guard: every table
// whose row content would change if the recovery flow re-executed a payment,
// re-signed/broadcast an intent, re-published an event or re-consumed one.
// The drill layer keeps its own copy (a drill-tagged file cannot reuse the
// integration-tagged manifest) and only asserts equality — it never collects
// the list dynamically.
var drillEffectTables = []string{
	"withdrawal_requests", "payment_intents", "withdrawal_authorizations",
	"withdrawal_authorization_scopes", "request_status_projection",
	"nonce_bindings", "nonce_observations", "nonce_scope_state",
	"signing_requests", "signature_results", "signer_caller",
	"tx_attempts", "tx_send_attempts", "tx_receipts", "tx_reconciliations",
	"execution_claims", "execution_steps",
	"outbox_events", "event_obligation",
	"consumer_inbox", "consumer_progress", "consumer_versions", "consumer_quarantine",
	"deposit_observations", "deposit_observation_transitions",
	"chain_blocks", "erc20_transfer_logs", "indexer_checkpoint",
}

// fingerprint serializes the full current content of every effect table (all
// rows via to_jsonb, deterministic order) into a digest map: an in-place
// UPDATE that keeps row counts identical is still caught.
func drillFingerprint(t *testing.T, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	out := make(map[string]string, len(drillEffectTables))
	for _, table := range drillEffectTables {
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

func drillAssertFingerprintEqual(t *testing.T, before, after map[string]string, label string) {
	t.Helper()
	for _, table := range drillEffectTables {
		if before[table] != after[table] {
			t.Errorf("%s: effect table %s changed (a recovery flow wrote an external-effect row)\nbefore: %s\nafter:  %s",
				label, table, before[table], after[table])
		}
	}
}

// ---------------------------------------------------------------------------
// Evidence archive.
// ---------------------------------------------------------------------------

// drillEvidenceDir resolves the archive directory: the workflow override when
// set (never a repository path), otherwise the test's temp dir.
func drillEvidenceDir(t *testing.T) string {
	t.Helper()
	if dir := strings.TrimSpace(os.Getenv(drillEvidenceDirEnv)); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("create drill evidence dir %s: %v", dir, err)
		}
		return dir
	}
	return t.TempDir()
}

// drillWriteEvidence persists one drill run record and returns its path. The
// record is the archived artifact a claim cites; it carries local inputs only
// and never a credential or a DSN.
func drillWriteEvidence(t *testing.T, name string, payload any) string {
	t.Helper()
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		t.Fatalf("encode drill evidence %s: %v", name, err)
	}
	path := filepath.Join(drillEvidenceDir(t), "drill-"+name+".json")
	if err := os.WriteFile(path, append(encoded, '\n'), 0o644); err != nil {
		t.Fatalf("write drill evidence %s: %v", name, err)
	}
	return path
}

// ---------------------------------------------------------------------------
// Mid-flight interruption (F2).
// ---------------------------------------------------------------------------

// drillProbeTable is created in the live database before the backup and is
// therefore part of the archive; the interruption test creates a conflicting
// copy in the target and parks the real pg_restore on its DROP TABLE.
const drillProbeTable = "drill_f2_probe"

func (e *drillEnv) seedProbeTable() {
	e.t.Helper()
	if _, err := e.data.Exec(e.ctx,
		`CREATE TABLE `+drillProbeTable+` (id bigserial PRIMARY KEY, note text NOT NULL)`); err != nil {
		e.t.Fatalf("create probe table: %v", err)
	}
	if _, err := e.data.Exec(e.ctx,
		`INSERT INTO `+drillProbeTable+` (note) VALUES ('drill')`); err != nil {
		e.t.Fatalf("seed probe row: %v", err)
	}
}

func (e *drillEnv) probeCount(dsn string) int {
	e.t.Helper()
	conn, err := pgx.Connect(e.ctx, dsn)
	if err != nil {
		e.t.Fatalf("connect %s: %v", dsn, err)
	}
	defer func() { _ = conn.Close(e.ctx) }()
	var n int
	if err := conn.QueryRow(e.ctx, `SELECT count(*) FROM `+drillProbeTable).Scan(&n); err != nil {
		e.t.Fatalf("probe count: %v", err)
	}
	return n
}

// waitForLockWaiter waits until a pg_restore backend is parked on the lock the
// test holds in the target database, and returns its pid(s).
func (e *drillEnv) waitForLockWaiter(targetDB string, timeout time.Duration) []int32 {
	e.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		rows, err := e.admin.Query(e.ctx,
			`SELECT pid FROM pg_stat_activity
			  WHERE datname = $1 AND wait_event_type = 'Lock' AND pid <> pg_backend_pid()
			  ORDER BY pid`, targetDB)
		if err != nil {
			e.t.Fatalf("look up lock waiter: %v", err)
		}
		var pids []int32
		for rows.Next() {
			var pid int32
			if err := rows.Scan(&pid); err != nil {
				rows.Close()
				e.t.Fatalf("scan lock waiter: %v", err)
			}
			pids = append(pids, pid)
		}
		rows.Close()
		if len(pids) > 0 {
			return pids
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("no pg_restore lock waiter in %s after %s (the restore never reached the target)", targetDB, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// terminateBackends server-side terminates the given backends: the real
// mid-flight interruption of the real pg_restore.
func (e *drillEnv) terminateBackends(pids []int32) {
	e.t.Helper()
	for _, pid := range pids {
		var ok bool
		if err := e.admin.QueryRow(e.ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&ok); err != nil {
			e.t.Fatalf("terminate backend %d: %v", pid, err)
		}
		if !ok {
			e.t.Fatalf("pg_terminate_backend(%d) = false", pid)
		}
	}
}

// ---------------------------------------------------------------------------
// Small helpers.
// ---------------------------------------------------------------------------

func drillDSNFor(t *testing.T, baseDSN, dbName string) string {
	t.Helper()
	u, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatalf("parse base dsn: %v", err)
	}
	u.Path = "/" + dbName
	return u.String()
}

func drillBlockHash(number uint64) string {
	return fmt.Sprintf("0x%064x", number)
}

func drillAddress(seed string) string {
	return "0x" + strings.Repeat(seed, 40)
}

func drillPaddedAddress(seed string) string {
	return "0x" + strings.Repeat("0", 24) + strings.Repeat(seed, 40)
}

func drillPaddedWord(value uint64) string {
	return fmt.Sprintf("0x%064x", value)
}

func drillHex64(seed string) string {
	return "0x" + strings.Repeat(seed, 64)
}

// drillFingerprintHex renders a bare (unprefixed) 64-hex fingerprint, the
// authorization_fingerprint shape of the signer schema.
func drillFingerprintHex(seed string) string {
	return strings.Repeat(seed, 64)
}

// drillSignatureHex renders a shape-valid 65-byte signature value.
func drillSignatureHex() string {
	return "0x" + strings.Repeat("0", 130)
}

func drillTransferTopic() string {
	// keccak256("Transfer(address,address,uint256)")
	return "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
}
