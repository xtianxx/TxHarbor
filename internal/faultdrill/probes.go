//go:build fault

// probes.go holds the shared seven-class probes the five-state matrix tasks
// (T021–T025, T080) assert against. Every probe goes through the real service
// entry (HTTP handler started by serve, scanner loops started by serve, the
// command runtimes) and returns the reduced matrix verdict plus the durable
// facts the caller records as evidence.
package faultdrill

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// doFull issues one authenticated request and returns status, body and headers.
func (s *Scene) doFull(method, path, body string) (int, []byte, http.Header) {
	s.T.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, s.BaseURL+path, reader)
	if err != nil {
		s.T.Fatalf("build %s %s: %v", method, path, err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+s.APIKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.T.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		s.T.Fatalf("read %s %s: %v", method, path, err)
	}
	return resp.StatusCode, raw, resp.Header.Clone()
}

// DegradationNow reads the status surface without waiting.
func (s *Scene) DegradationNow() map[string]any {
	s.T.Helper()
	status, raw := s.do(http.MethodGet, "/status/degradation", "")
	if status != http.StatusOK {
		s.T.Fatalf("GET /status/degradation = %d (%s)", status, raw)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		s.T.Fatalf("decode degradation body: %v (%s)", err, raw)
	}
	return body
}

// WaitDependency waits until /status/degradation reports the dependency's
// expected reachability (the serve probe cadence is a few seconds).
func (s *Scene) WaitDependency(name string, up bool) {
	s.T.Helper()
	want := "down"
	if up {
		want = "up"
	}
	if err := WaitFor(s.Ctx, 60*time.Second, func() (bool, error) {
		body := s.DegradationNow()
		deps, _ := body["dependencies"].(map[string]any)
		return deps[name] == want, nil
	}); err != nil {
		s.T.Fatalf("dependency %s never reported %s: %v", name, want, err)
	}
}

// Degraded reports the non-critical degradation verdict from the status body.
func Degraded(body map[string]any) bool {
	degraded, _ := body["non_critical_degraded"].(bool)
	return degraded
}

// NonCriticalVerdict maps the status surface onto the matrix verdict.
func NonCriticalVerdict(body map[string]any) string {
	if Degraded(body) {
		return VerdictDegraded
	}
	return VerdictContinue
}

// ProbeDeposit sends one real deposit and waits for the 004 observation and
// the 005 confirmation. It returns the transaction hash and height; the
// processing and confirmation/reorg classes both observe "continue".
func (s *Scene) ProbeDeposit(amount int64) (common.Hash, uint64) {
	s.T.Helper()
	txHash, height := s.SendDeposit(amount)
	s.WaitObservation(txHash)
	s.WaitConfirmed(txHash)
	return txHash, height
}

// CreateExpect drives one first create and requires the accepted outcome
// (201). It returns the request id.
func (s *Scene) CreateExpect(idemKey string) string {
	s.T.Helper()
	return s.CreateWithAuth(idemKey, SceneAuthID)
}

// CreateWithAuth drives one first create under a specific authorization id.
func (s *Scene) CreateWithAuth(idemKey, authID string) string {
	s.T.Helper()
	status, raw := s.do(http.MethodPost, "/withdrawals", s.createBodyWithAuth(idemKey, authID))
	if status != http.StatusCreated {
		s.T.Fatalf("POST /withdrawals (%s) = %d, want 201; body=%s%s", idemKey, status, raw, s.diagnostics())
	}
	var body struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(raw, &body); err != nil || body.RequestID == "" {
		s.T.Fatalf("decode create body: %v (%s)", err, raw)
	}
	return body.RequestID
}

// CreateRefused requires the clear retryable refusal (503 +
// temporarily_unavailable) and asserts nothing was persisted for that
// idempotency key. It returns the refusal body message for the evidence.
func (s *Scene) CreateRefused(idemKey string) string {
	s.T.Helper()
	status, raw, _ := s.doFull(http.MethodPost, "/withdrawals", s.createBody(idemKey))
	if status != http.StatusServiceUnavailable {
		s.T.Fatalf("POST /withdrawals (%s) = %d, want the retryable 503 refusal; body=%s%s",
			idemKey, status, raw, s.diagnostics())
	}
	var body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		s.T.Fatalf("decode refusal: %v (%s)", err, raw)
	}
	if body.Code != string(withdrawal.CodeTemporarilyUnavailable) {
		s.T.Fatalf("refusal code = %q, want %q", body.Code, withdrawal.CodeTemporarilyUnavailable)
	}
	if n := s.Count(`SELECT count(*) FROM withdrawal_requests WHERE caller_id = $1 AND idempotency_key = $2`,
		SceneCaller, idemKey); n != 0 {
		s.T.Fatalf("refused create persisted %d rows for %q, want 0 (0 unlimited admission)", n, idemKey)
	}
	return body.Message
}

// createBody renders one canonical create request body (default grant).
func (s *Scene) createBody(idemKey string) string {
	return s.createBodyWithAuth(idemKey, SceneAuthID)
}

// createBodyWithAuth renders one canonical create request body under a
// specific authorization id.
func (s *Scene) createBodyWithAuth(idemKey, authID string) string {
	return fmt.Sprintf(`{"idempotency_key":%q,"chain_id":%d,"asset":%q,"recipient":%q,"amount":%q,"authorization_id":%q}`,
		idemKey, SceneChainID, SceneAsset, SceneRecipient, SceneAmount, authID)
}

// FreshRequest is one accepted withdrawal request plus the authorization id
// it was accepted under (each request needs its own grant: an authorization
// is bound to exactly one request).
type FreshRequest struct {
	ID     string
	AuthID string
}

// CreateFresh seeds a new grant and creates one accepted request under it,
// through the real supply operation and the real POST /withdrawals.
func (s *Scene) CreateFresh(name string) FreshRequest {
	s.T.Helper()
	authID := "authz-scene-" + name
	s.SeedGrant(authID, "op-drill-supply-"+name)
	return FreshRequest{ID: s.CreateWithAuth(name+"-create", authID), AuthID: authID}
}

// CreateFreshDrained creates one accepted request and waits for its event to
// be published and the pending backlog to clear. It is used while the
// capacity boundary is small: the next create must not race the publisher.
func (s *Scene) CreateFreshDrained(name string) FreshRequest {
	s.T.Helper()
	req := s.CreateFresh(name)
	s.WaitOutbox(120*time.Second, "event drain after "+name, func(totals OutboxTotals) bool {
		return totals.Pending == 0
	})
	return req
}

// ProbeExecution seeds the 011 scope for one grant and drives the real
// execution admission for an already accepted request. It returns the admitted
// intent id.
func (s *Scene) ProbeExecution(requestID, intentID, authID string) string {
	s.T.Helper()
	s.SeedExecutionScope(requestID, intentID, authID)
	status, raw := s.AdmitExecution(requestID)
	if status != http.StatusCreated {
		s.T.Fatalf("POST /withdrawals/%s/execution = %d, want 201; body=%s%s", requestID, status, raw, s.diagnostics())
	}
	var body struct {
		IntentID string `json:"intent_id"`
	}
	if err := json.Unmarshal(raw, &body); err != nil || body.IntentID != intentID {
		s.T.Fatalf("admission body = %s (err %v), want intent %s", raw, err, intentID)
	}
	return body.IntentID
}

// QueryVerdict drives the real query path and returns the matrix verdict:
// continue when the answer is served without a degradation annotation,
// degraded when the non-critical surface (and the response header) says so.
func (s *Scene) QueryVerdict(requestID string) string {
	s.T.Helper()
	status, raw, headers := s.doFull(http.MethodGet, "/withdrawals/"+requestID, "")
	if status != http.StatusOK {
		s.T.Fatalf("GET /withdrawals/%s = %d, want 200; body=%s", requestID, status, raw)
	}
	var body struct {
		RequestID string `json:"request_id"`
		Status    string `json:"status"`
	}
	if err := json.Unmarshal(raw, &body); err != nil || body.RequestID != requestID {
		s.T.Fatalf("query body = %s (err %v), want request %s", raw, err, requestID)
	}
	if headers.Get("X-TXHarbor-Degraded") == "true" || Degraded(s.DegradationNow()) {
		return VerdictDegraded
	}
	return VerdictContinue
}

// EventDeliveryState is the observed event-channel posture.
type EventDeliveryState struct {
	Delivery  string `json:"delivery"`
	Pending   int64  `json:"pending"`
	Published int64  `json:"published"`
	Ledger    int64  `json:"ledger"`
}

// EventDelivery reads the delivery posture from the status surface plus the
// durable outbox/ledger counts. Pending is always the durable PostgreSQL
// count: the status surface reports "degraded" without a count while Kafka is
// down (no fabricated liveness).
func (s *Scene) EventDelivery() EventDeliveryState {
	s.T.Helper()
	body := s.DegradationNow()
	delivery, _ := body["event_delivery"].(string)
	totals, err := s.Env.OutboxTotals(s.Ctx)
	if err != nil {
		s.T.Fatalf("outbox totals: %v", err)
	}
	return EventDeliveryState{
		Delivery:  delivery,
		Pending:   totals.Pending,
		Published: totals.Published,
		Ledger:    s.LedgerRows(),
	}
}

// WaitEventDelivered waits until the reference consumer applied every
// published event and the pending backlog is empty (the catch-up verdict).
func (s *Scene) WaitEventDelivered() {
	s.T.Helper()
	s.WaitOutbox(240*time.Second, "outbox drain", func(totals OutboxTotals) bool {
		return totals.Pending == 0
	})
	if err := WaitFor(s.Ctx, 240*time.Second, func() (bool, error) {
		if err := s.checkRuntimes(); err != nil {
			return false, err
		}
		totals, err := s.Env.OutboxTotals(s.Ctx)
		if err != nil {
			return false, err
		}
		ledger := s.LedgerRows()
		return ledger == totals.Published && totals.Pending == 0, nil
	}); err != nil {
		s.T.Fatalf("consumer catch-up: %v%s", err, s.diagnostics())
	}
}
