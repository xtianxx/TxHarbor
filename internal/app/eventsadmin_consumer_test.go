// eventsadmin_consumer_test.go is the T045 CLI smoke layer: the audited
// operator actions' argument grammar (events-admin replay / unblock /
// retention-prune) and the fail-closed refusals that must happen before any
// database access. The transactional audit/replay paths run in the T048/T049
// integration layer.
package app

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/xtianxx/txharbor/internal/events"
)

// eventsAdminGetenv adapts a map to the Deps.Getenv shape (the integration
// helper of the same purpose is not compiled without its build tag).
func eventsAdminGetenv(env map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}

// eventsAdminCLIEnv is a complete, valid 013 configuration for the CLI smoke.
func eventsAdminCLIEnv() map[string]string {
	return map[string]string{
		"TXHARBOR_PG_DSN":                              "postgres://txharbor:txharbor@127.0.0.1:5432/txharbor?sslmode=disable",
		"TXHARBOR_RPC_URL":                             "http://127.0.0.1:8545",
		"TXHARBOR_CHAIN_ID":                            "31337",
		"TXHARBOR_START_HEIGHT":                        "0",
		"TXHARBOR_LOG_START_HEIGHT":                    "0",
		"TXHARBOR_LOG_CONTRACTS":                       "0x1111111111111111111111111111111111111111",
		"TXHARBOR_DEPOSIT_START_HEIGHT":                "0",
		"TXHARBOR_DEPOSIT_CONTRACTS":                   "0x1111111111111111111111111111111111111111:0",
		"TXHARBOR_DEPOSIT_WATCH_ADDRESSES":             "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
		"TXHARBOR_CONFIRMATION_DEPTH":                  "10",
		"TXHARBOR_REORG_MAX_DEPTH":                     "100",
		"TXHARBOR_HTTP_ADDR":                           "127.0.0.1:0",
		"TXHARBOR_EVENTS_ENABLED":                      "true",
		"TXHARBOR_KAFKA_BROKERS":                       "127.0.0.1:9092",
		"TXHARBOR_REDIS_ADDR":                          "127.0.0.1:6379",
		"TXHARBOR_RATELIMIT_NEW_WITHDRAWAL":            "50/10",
		"TXHARBOR_RATELIMIT_WRITE":                     "50/10",
		"TXHARBOR_RATELIMIT_QUERY":                     "50/10",
		"TXHARBOR_RATELIMIT_OPERATOR":                  "50/10",
		"TXHARBOR_RATELIMIT_RPC":                       "50/10",
		"TXHARBOR_EVENTS_CAPACITY_SOFT_LIMIT":          "100000",
		"TXHARBOR_EVENTS_CAPACITY_HARD_LIMIT":          "200000",
		"TXHARBOR_EVENTS_CAPACITY_RESERVE":             "1000",
		"TXHARBOR_EVENTS_CAPACITY_RETENTION":           "168h",
		"TXHARBOR_EVENTS_CAPACITY_MAX_SHUTDOWN_WINDOW": "1h",
		"TXHARBOR_EVENTS_CAPACITY_DRAIN_TARGET_WINDOW": "5m",
	}
}

// TestEventsAdminOperatorUsageRefusals pins the fail-closed usage grammar: an
// incomplete operator action is a usage error (exit 2) and never reaches the
// database.
func TestEventsAdminOperatorUsageRefusals(t *testing.T) {
	env := eventsAdminCLIEnv()
	cases := [][]string{
		{"replay"},
		{"replay", "--consumer", events.RefConsumerName},
		{"replay", "--consumer", events.RefConsumerName, "--scope", "event-ids:" + uuid.NewString()},
		{"replay", "--consumer", events.RefConsumerName, "--scope", "event-ids:" + uuid.NewString(), "--reason", "r"},
		{"unblock"},
		{"unblock", "--outbox-id", "7"},
		{"retention-prune"},
		{"retention-prune", "--retention", "1h"},
	}
	for _, args := range cases {
		var stdout, stderr bytes.Buffer
		code := EventsAdmin(context.Background(), args, Deps{
			Getenv: eventsAdminGetenv(env),
			Stdout: &stdout,
			Stderr: &stderr,
		})
		if code != 2 {
			t.Fatalf("EventsAdmin(%v) = %d, want 2 (usage); stderr=%s", args, code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "usage:") {
			t.Fatalf("EventsAdmin(%v) did not print usage: %s", args, stderr.String())
		}
	}
}

// TestEventsAdminReplayRefusesUnwiredConsumer pins the fail-closed wiring:
// a replay for a consumer this batch does not run is refused before any
// database access (no pool is reachable in this test).
func TestEventsAdminReplayRefusesUnwiredConsumer(t *testing.T) {
	env := eventsAdminCLIEnv()
	var stdout, stderr bytes.Buffer
	code := EventsAdmin(context.Background(), []string{
		"replay",
		"--consumer", "some-other-consumer",
		"--scope", "event-ids:" + uuid.NewString(),
		"--reason", "should refuse",
		"--operator", "smoke-operator",
	}, Deps{
		Getenv: eventsAdminGetenv(env),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if code != 1 {
		t.Fatalf("replay for an unwired consumer = %d, want 1; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "not wired") {
		t.Fatalf("refusal does not name the wiring: %s", stderr.String())
	}
}

// TestEventsAdminReplayScopeGrammar covers the operator scope grammar: every
// documented form parses and malformed input is refused.
func TestEventsAdminReplayScopeGrammar(t *testing.T) {
	id := uuid.New()
	valid := []struct {
		raw  string
		kind events.ReplayScopeKind
	}{
		{"event-ids:" + id.String(), events.ReplayScopeEventIDs},
		{"aggregate:withdrawal_intent:intent-1", events.ReplayScopeAggregate},
		{"time-range:2026-09-24T00:00:00Z,2026-09-25T00:00:00Z", events.ReplayScopeTimeRange},
		{"offset-range:txharbor.events.v1:0:1-9", events.ReplayScopeOffsetRange},
	}
	for _, tc := range valid {
		scope, err := parseReplayScope(tc.raw)
		if err != nil {
			t.Fatalf("parseReplayScope(%q) error = %v", tc.raw, err)
		}
		if scope.Kind != tc.kind {
			t.Fatalf("parseReplayScope(%q) kind = %s, want %s", tc.raw, scope.Kind, tc.kind)
		}
		if err := scope.Validate(); err != nil {
			t.Fatalf("parsed scope %q invalid: %v", tc.raw, err)
		}
	}
	invalid := []string{
		"", "event-ids", "event-ids:not-a-uuid", "aggregate:only-type",
		"time-range:not-a-time,2026-09-25T00:00:00Z", "offset-range:topic:0",
		"offset-range:topic:-1:1-9", "offset-range:topic:0:9-1", "unknown:value",
	}
	for _, raw := range invalid {
		if _, err := parseReplayScope(raw); err == nil {
			t.Fatalf("parseReplayScope(%q) accepted an invalid scope", raw)
		}
	}
}

// TestEventsAdminUnimplementedActionsAreGone guards the batch boundary: the
// operator actions are implemented (their usage paths respond), never the old
// "not implemented in this batch" refusal.
func TestEventsAdminUnimplementedActionsAreGone(t *testing.T) {
	env := eventsAdminCLIEnv()
	for _, action := range []string{"replay", "unblock", "retention-prune"} {
		var stdout, stderr bytes.Buffer
		code := EventsAdmin(context.Background(), []string{action, "--help"}, Deps{
			Getenv: eventsAdminGetenv(env),
			Stdout: &stdout,
			Stderr: &stderr,
		})
		// --help is answered by the flag set with the action usage, never
		// with the removed placeholder.
		if code != 2 || !strings.Contains(stderr.String(), "usage:") {
			t.Fatalf("%s --help = %d (stdout=%q stderr=%q)", action, code, stdout.String(), stderr.String())
		}
		if strings.Contains(stderr.String(), "not implemented in this batch") {
			t.Fatalf("%s still refuses as unimplemented: %s", action, stderr.String())
		}
	}
}

// TestEventsAdminHelpListsOperatorActions pins the documented operator
// surface.
func TestEventsAdminHelpListsOperatorActions(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := EventsAdmin(context.Background(), []string{"--help"}, Deps{
		Getenv: eventsAdminGetenv(eventsAdminCLIEnv()),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if code != 0 {
		t.Fatalf("EventsAdmin --help = %d, want 0; stderr=%s", code, stderr.String())
	}
	for _, fragment := range []string{"replay --consumer", "unblock --outbox-id", "retention-prune --retention"} {
		if !strings.Contains(stdout.String(), fragment) {
			t.Fatalf("help does not document %q:\n%s", fragment, stdout.String())
		}
	}
}
