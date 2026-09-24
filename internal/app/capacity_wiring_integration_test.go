//go:build integration

// capacity_wiring_integration_test.go is the T089 acceptance (Integration-PG):
// the production assembly constructed from the T001 configuration gates the
// real 007 receive path. With a real migrated PostgreSQL and one
// events.CapacityGuard over it, POST /withdrawals is refused with the
// retryable 503 channel when the pending backlog is at/above the soft
// boundary, admitted below it, and a replay of an already accepted request is
// never refused by capacity. Redis and Kafka do not participate in any of
// these paths.
package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

const (
	t089ChainID = int64(31337)
	t089Asset   = "0x2222222222222222222222222222222222222222"
	t089Watch   = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	t089AuthID  = "authz-t089-1"
	t089Caller  = int64(89)
)

// t089AppendPending commits one pending outbox event through the real Append
// path, so the guard observes a real PostgreSQL backlog.
func t089AppendPending(t *testing.T, pool *pgxpool.Pool, n int) {
	t.Helper()
	ctx := context.Background()
	obsID := fmt.Sprintf("t089-obs-%d-%d", time.Now().UnixNano(), n)
	ev, err := events.NewEvent(events.Event{
		EventType:     events.EventTypeDepositObservationCreated,
		SchemaVersion: events.SchemaVersionV1,
		IdentityKind:  events.IdentityKindEVMLog,
		AggregateType: "deposit_observation",
		AggregateID:   obsID,
		Payload:       map[string]any{"observation_id": obsID, "state": "pending"},
		OccurredAt:    time.Now().UTC(),
		ChainID:       t089ChainID,
		BlockNumber:   int64(7000 + n),
		BlockHash:     fmt.Sprintf("0x%064x", 0x7100+n),
		TxHash:        fmt.Sprintf("0x%064x", 0x7200+n),
		LogIndex:      n,
	})
	if err != nil {
		t.Fatalf("build event: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin append: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := events.Append(ctx, tx, ev); err != nil {
		t.Fatalf("append event: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit append: %v", err)
	}
}

// t089SeedWithdrawalPrerequisites seeds caller, API key, allowlist bootstrap
// row and an active supply grant for the real 007 path.
func t089SeedWithdrawalPrerequisites(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO caller (caller_id, label, can_create) VALUES ($1, 't089', TRUE)
		ON CONFLICT (caller_id) DO UPDATE SET can_create = TRUE`, t089Caller); err != nil {
		t.Fatalf("seed caller: %v", err)
	}
	apiKey, _, err := withdrawal.IssueKey(ctx, pool, t089Caller, "t089")
	if err != nil {
		t.Fatalf("IssueKey: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO deposit_config_history
		(chain_id, version_seq, config_hash, prev_seq, start_block, assets, watches, replay_from, operator, reason)
		VALUES ($1, 1, repeat('a', 64), NULL, 0, $2, $3, 0, 'bootstrap', 'bootstrap')
		ON CONFLICT (chain_id, version_seq) DO NOTHING`, t089ChainID, t089Asset+":0", t089Watch+":0"); err != nil {
		t.Fatalf("seed allowlist: %v", err)
	}
	if _, err := withdrawal.SupplyGrant(ctx, pool, withdrawal.OpInput{
		OperationID:     "op-t089-supply",
		Action:          "supply",
		AuthorizationID: t089AuthID,
		CallerID:        t089Caller,
		ChainID:         t089ChainID,
		Asset:           t089Asset,
		Recipient:       "0x3333333333333333333333333333333333333333",
		Amount:          "1000",
	}, "t089", "controlled t089 supply"); err != nil {
		t.Fatalf("SupplyGrant: %v", err)
	}
	return apiKey
}

// t089Post drives one authenticated POST /withdrawals against the real
// handler.
func t089Post(t *testing.T, handler http.Handler, apiKey, idemKey string) (int, []byte) {
	t.Helper()
	body := fmt.Sprintf(`{"idempotency_key":%q,"chain_id":%d,"asset":%q,"recipient":"0x3333333333333333333333333333333333333333","amount":"1000","authorization_id":%q}`,
		idemKey, t089ChainID, t089Asset, t089AuthID)
	req := httptest.NewRequest(http.MethodPost, "/withdrawals", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	raw, _ := io.ReadAll(rec.Result().Body)
	return rec.Code, raw
}

// TestCapacityGateWiredIntoWithdrawalReceivePath is the T089 acceptance.
func TestCapacityGateWiredIntoWithdrawalReceivePath(t *testing.T) {
	ctx := context.Background()
	ctr := startPostgresContainer(t)
	dsn := postgresDSN(t, ctr)
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 10 * time.Second}, io.Discard); err != nil {
		t.Fatalf("MigrateUp: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	apiKey := t089SeedWithdrawalPrerequisites(t, ctx, pool)

	// The guard is built exactly like serve builds it: from the configuration
	// values, over the real pool. 2 pending rows sit at soft_limit=2.
	t089AppendPending(t, pool, 0)
	t089AppendPending(t, pool, 1)
	refusing, err := events.NewCapacityGuard(pool, events.CapacityLimits{Reserve: 1, SoftLimit: 2, HardLimit: 3}, nil)
	if err != nil {
		t.Fatalf("NewCapacityGuard: %v", err)
	}
	generous, err := events.NewCapacityGuard(pool, events.CapacityLimits{Reserve: 1, SoftLimit: 1000, HardLimit: 2000}, nil)
	if err != nil {
		t.Fatalf("NewCapacityGuard: %v", err)
	}

	refusingHandler := &WithdrawalHandler{Pool: pool, ChainID: t089ChainID, CapacityGate: refusing}
	generousHandler := &WithdrawalHandler{Pool: pool, ChainID: t089ChainID, CapacityGate: generous}

	// 1. Soft boundary: the first create is refused with the retryable 503
	// channel and nothing is persisted.
	status, raw := t089Post(t, refusingHandler, apiKey, "idem-t089-refused")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("create at soft boundary status = %d body=%s, want 503", status, raw)
	}
	var refusal struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &refusal); err != nil {
		t.Fatalf("decode refusal: %v (%s)", err, raw)
	}
	if refusal.Code != string(withdrawal.CodeTemporarilyUnavailable) {
		t.Fatalf("refusal code = %q, want %q", refusal.Code, withdrawal.CodeTemporarilyUnavailable)
	}
	if !strings.Contains(strings.ToLower(refusal.Message), "capacity") {
		t.Fatalf("refusal message %q does not explain the capacity boundary", refusal.Message)
	}
	var refusedRows int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM withdrawal_requests WHERE caller_id = $1`, t089Caller).Scan(&refusedRows); err != nil {
		t.Fatalf("count refused rows: %v", err)
	}
	if refusedRows != 0 {
		t.Fatalf("refused create left %d request rows, want 0", refusedRows)
	}

	// 2. Below the boundary the same real path admits and emits its event.
	status, raw = t089Post(t, generousHandler, apiKey, "idem-t089-admitted")
	if status != http.StatusCreated {
		t.Fatalf("create below soft boundary status = %d body=%s, want 201", status, raw)
	}
	var created struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(raw, &created); err != nil || created.RequestID == "" {
		t.Fatalf("decode admitted create: %v (%s)", err, raw)
	}
	var requestRows, requestEvents int64
	if err := pool.QueryRow(ctx, `
SELECT (SELECT count(*) FROM withdrawal_requests WHERE caller_id = $1 AND idempotency_key = 'idem-t089-admitted'),
       (SELECT count(*) FROM outbox_events WHERE event_type = 'withdrawal.request.received' AND aggregate_id = $2)`,
		t089Caller, created.RequestID).Scan(&requestRows, &requestEvents); err != nil {
		t.Fatalf("count admitted rows/events: %v", err)
	}
	if requestRows != 1 || requestEvents != 1 {
		t.Fatalf("admitted create rows=%d events=%d, want (1, 1)", requestRows, requestEvents)
	}

	// 3. The capacity gate never refuses a replay of an accepted request: the
	// replay fast path returns before the gate.
	status, raw = t089Post(t, refusingHandler, apiKey, "idem-t089-admitted")
	if status != http.StatusOK {
		t.Fatalf("replay under refusal config status = %d body=%s, want 200", status, raw)
	}
	var replay struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(raw, &replay); err != nil || replay.RequestID != created.RequestID {
		t.Fatalf("replay request_id = %q (err %v), want %s", replay.RequestID, err, created.RequestID)
	}

	// 4. The refusal channel counts capacity_refusals_total{op_class} through
	// the observer surface (the T089 production wiring passes the registry).
	observer := &t089Observer{}
	observed, err := events.NewCapacityGuard(pool, events.CapacityLimits{Reserve: 1, SoftLimit: 2, HardLimit: 3}, observer)
	if err != nil {
		t.Fatalf("NewCapacityGuard: %v", err)
	}
	observedHandler := &WithdrawalHandler{Pool: pool, ChainID: t089ChainID, CapacityGate: observed}
	status, _ = t089Post(t, observedHandler, apiKey, "idem-t089-counted")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("observed refusal status = %d, want 503", status)
	}
	if observer.refusals[events.CapacityOpWithdrawalCreate] != 1 {
		t.Fatalf("capacity refusals = %v, want one %q", observer.refusals, events.CapacityOpWithdrawalCreate)
	}
}

// t089Observer records the CapacityObserver calls without a metrics registry.
type t089Observer struct {
	soft, hard int
	refusals   map[string]int
}

func (o *t089Observer) SetOutboxPending(string, int)              {}
func (o *t089Observer) SetOutboxPendingOldestAge(string, float64) {}
func (o *t089Observer) ObserveCapacitySoftBreach()                { o.soft++ }
func (o *t089Observer) ObserveCapacityHardBreach()                { o.hard++ }

func (o *t089Observer) ObserveCapacityRefusal(opClass string) {
	if o.refusals == nil {
		o.refusals = map[string]int{}
	}
	o.refusals[opClass]++
}
