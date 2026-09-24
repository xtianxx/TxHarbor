//go:build fault

// matrix_contract_test.go is the T020 structural contract of the B9 fault
// layer (FR-02/05/06; contracts/consumer.md §9): the funding decision path
// never imports the cache/rate-limit/health carriers, the event runtime never
// imports a send-side writer, delivery status is never interpreted as an
// authorization, and the duplicate-delivery path contains no
// intent/nonce/signature/broadcast call. It also pins the T089/T090 production
// assembly itself: serve constructs the guard, the withdrawal receive path
// receives it and the 003/004 loops evaluate the pause decision.
//
// This test is structural and needs no containers; it lives in the fault layer
// because it guards the fault-matrix preconditions.
package faultdrill

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// scanImports returns the import paths of one source file.
func scanImports(t *testing.T, path string) []string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), path, body, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var imports []string
	for _, spec := range parsed.Imports {
		imports = append(imports, strings.Trim(spec.Path.Value, `"`))
	}
	return imports
}

// assertNoForbiddenImports fails when a file imports a forbidden package.
func assertNoForbiddenImports(t *testing.T, path string, forbidden []string) {
	t.Helper()
	for _, imported := range scanImports(t, path) {
		for _, bad := range forbidden {
			if imported == bad {
				t.Fatalf("%s imports %q; forbidden by the fault-matrix contract", filepath.Base(path), imported)
			}
		}
	}
}

// assertNoTokens fails when a source file contains any forbidden token.
func assertNoTokens(t *testing.T, path string, tokens []string) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	for _, token := range tokens {
		if strings.Contains(string(body), token) {
			t.Fatalf("%s contains %q; forbidden by the fault-matrix contract", filepath.Base(path), token)
		}
	}
}

// assertTokens fails when a source file misses a required token.
func assertTokens(t *testing.T, path string, tokens []string) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	for _, token := range tokens {
		if !strings.Contains(string(body), token) {
			t.Fatalf("%s is missing %q; the T089/T090 assembly is not wired", filepath.Base(path), token)
		}
	}
}

func TestMatrixContractFundingPathIsCarrierFree(t *testing.T) {
	fundingFiles := []string{
		filepath.Join("..", "withdrawal", "intake.go"),
		filepath.Join("..", "execution", "intent.go"),
		filepath.Join("..", "execution", "advance.go"),
		filepath.Join("..", "execution", "revision.go"),
		filepath.Join("..", "indexer", "capacitypause.go"),
	}
	forbidden := []string{
		"github.com/xtianxx/txharbor/internal/cache",
		"github.com/xtianxx/txharbor/internal/ratelimit",
		"github.com/xtianxx/txharbor/internal/health",
	}
	for _, file := range fundingFiles {
		assertNoForbiddenImports(t, file, forbidden)
	}

	// The capacity gate is PostgreSQL-only: no Redis/Kafka client in the
	// decision files.
	for _, file := range fundingFiles {
		assertNoTokens(t, file, []string{"redis.", "kgo.", "redisClient", "KafkaAvailable"})
	}

	// Delivery status is never read by a send-side decision: the funding
	// decision files must not reference the outbox/consumer tables.
	sendSide := []string{
		filepath.Join("..", "withdrawal", "intake.go"),
		filepath.Join("..", "execution", "intent.go"),
		filepath.Join("..", "execution", "advance.go"),
	}
	deliveryTokens := []string{
		"outbox_events", "consumer_progress", "consumer_inbox",
		"consumer_quarantine", "publish_state", "events.Delivery",
	}
	for _, file := range sendSide {
		assertNoTokens(t, file, deliveryTokens)
	}
}

func TestMatrixContractEventRuntimeHasNoSendPath(t *testing.T) {
	eventFiles := []string{
		filepath.Join("..", "events", "append.go"),
		filepath.Join("..", "events", "consumer.go"),
		filepath.Join("..", "events", "quarantine.go"),
		filepath.Join("..", "events", "publisher.go"),
		filepath.Join("..", "events", "capacity.go"),
		filepath.Join("..", "events", "refconsumer.go"),
	}
	forbidden := []string{
		"github.com/xtianxx/txharbor/internal/execution",
		"github.com/xtianxx/txharbor/internal/withdrawal",
		"github.com/xtianxx/txharbor/internal/nonce",
		"github.com/xtianxx/txharbor/internal/signer",
		"github.com/xtianxx/txharbor/internal/txlifecycle",
	}
	for _, file := range eventFiles {
		assertNoForbiddenImports(t, file, forbidden)
	}

	// The duplicate-delivery / replay path creates no payment work.
	sendTables := []string{
		"INSERT INTO payment_intents", "INSERT INTO nonce_bindings",
		"INSERT INTO tx_attempts", "INSERT INTO tx_attempt_signings",
		"INSERT INTO tx_send_attempts", "eth_sendRawTransaction",
	}
	for _, file := range eventFiles {
		assertNoTokens(t, file, sendTables)
	}
}

func TestMatrixContractCapacityAssemblyIsWired(t *testing.T) {
	// T089: serve constructs the guard from configuration, hands it to the
	// withdrawal receive path and to both chain streams; the observation is
	// refreshed on the serve lifecycle.
	assertTokens(t, filepath.Join("..", "app", "capacity.go"), []string{
		"events.NewCapacityGuard", "cfg.Capacity.Reserve", "cfg.Capacity.SoftLimit", "cfg.Capacity.HardLimit",
	})
	assertTokens(t, filepath.Join("..", "app", "serve.go"), []string{
		"buildCapacityGuard(pool, cfg, m)",
		"logScanner.SetCapacityPauseGate(capacityGuard)",
		"depositScanner.SetCapacityPauseGate(capacityGuard)",
		"CapacityGate: capacityGuard",
		"capacityGuard.Observe(ctx)",
	})
	assertTokens(t, filepath.Join("..", "app", "withdrawalhttp.go"), []string{
		"CapacityGate: h.CapacityGate",
	})

	// T090: the 003/004 loops evaluate the T071 pause decision at a
	// persistence failure.
	for _, file := range []string{"logscanner.go", "depositscanner.go"} {
		assertTokens(t, filepath.Join("..", "indexer", file), []string{
			"capacityPauseIfNeeded(ctx, err)",
			"SetCapacityPauseGate",
		})
	}
	assertTokens(t, filepath.Join("..", "indexer", "capacitypause.go"), []string{
		"CapacityPauseController", "WaitRecovery", "ReliableProgressLogSource", "ReliableProgressDepositSource",
	})
}
