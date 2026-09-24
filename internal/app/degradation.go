// degradation.go owns the T018 non-critical degradation surface: dependency
// health drives a degradation state that is annotated onto query responses
// and exposed on GET /status/degradation. The state is informational only —
// it is never a readiness verdict and never a funding gate. Critical paths
// (deposit, confirmation, reorg recovery, in-flight and accepted withdrawals)
// are unaffected by non-critical degradation; queries keep serving from
// PostgreSQL with an explicit annotation instead of pretending to be live.
//
// T063 extends the same state with the cache/limiter/RPC-budget posture; the
// fields are attached once at startup before the listener opens.
package app

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/cache"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/health"
	"github.com/xtianxx/txharbor/internal/ratelimit"
)

// degradationState carries the non-authoritative dependency signals and the
// attached 013 middleware posture.
type degradationState struct {
	enabled      bool
	signals      *health.DependencySignals
	dependencies []string

	// Attached by the 013 wiring (T063); nil when the events feature is off.
	cache     *cache.Client
	policy    *ratelimit.Policy
	rpcBudget *ratelimit.RPCBudget
}

// newDegradationState builds the state for the configured dependency names.
func newDegradationState(enabled bool, signals *health.DependencySignals, dependencies ...string) *degradationState {
	return &degradationState{enabled: enabled, signals: signals, dependencies: dependencies}
}

// Degraded reports whether any observed non-authoritative dependency is
// unavailable. Unknown dependencies are not degraded (no invented failure),
// and a disabled feature never degrades.
func (d *degradationState) Degraded() bool {
	if d == nil || !d.enabled || d.signals == nil {
		return false
	}
	return d.signals.Degraded()
}

// dependencyStates renders "up"/"down"/"unknown" per configured dependency.
func (d *degradationState) dependencyStates() map[string]string {
	out := make(map[string]string, len(d.dependencies))
	for _, name := range d.dependencies {
		if d.signals == nil {
			out[name] = "unknown"
			continue
		}
		available, known := d.signals.Availability(name)
		switch {
		case !known:
			out[name] = "unknown"
		case available:
			out[name] = "up"
		default:
			out[name] = "down"
		}
	}
	return out
}

// eventDelivery annotates the event delivery state without faking real-time:
// a down Kafka is degraded, an uncleared pending backlog is "backlog" (with
// its observed count, no invented threshold), and an unconfirmable state is
// "unknown".
func (d *degradationState) eventDelivery(ctx context.Context, pool *pgxpool.Pool) (string, int64) {
	if d == nil || !d.enabled {
		return "disabled", 0
	}
	if available, known := d.signals.Availability("kafka"); known && !available {
		return "degraded", 0
	}
	if pool == nil {
		return "unknown", 0
	}
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := pool.Query(queryCtx, events.CapacitySQL)
	if err != nil {
		return "unknown", 0
	}
	defer rows.Close()
	var pending int64
	for rows.Next() {
		var family string
		var count int64
		var oldest float64
		if err := rows.Scan(&family, &count, &oldest); err != nil {
			return "unknown", 0
		}
		pending += count
	}
	if err := rows.Err(); err != nil {
		return "unknown", 0
	}
	if pending > 0 {
		return "backlog", pending
	}
	if available, known := d.signals.Availability("kafka"); known && available {
		return "live", 0
	}
	return "unknown", 0
}

// degradationStatusBody is the GET /status/degradation response. It is a
// non-critical observability surface: no credential, no business identity.
type degradationStatusBody struct {
	Status              string            `json:"status"`
	NonCriticalDegraded bool              `json:"non_critical_degraded"`
	Dependencies        map[string]string `json:"dependencies"`
	EventDelivery       string            `json:"event_delivery"`
	PendingEvents       int64             `json:"pending_events"`
	Cache               string            `json:"cache"`
	RateLimit           string            `json:"rate_limit"`
	RPCBudget           map[string]string `json:"rpc_budget,omitempty"`
	Note                string            `json:"note"`
}

// degradationStatusHandler serves the non-critical status surface.
type degradationStatusHandler struct {
	state *degradationState
	pool  *pgxpool.Pool
}

func (h *degradationStatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	delivery, pending := h.state.eventDelivery(r.Context(), h.pool)
	body := degradationStatusBody{
		Status:              "ok",
		NonCriticalDegraded: h.state.Degraded(),
		Dependencies:        h.state.dependencyStates(),
		EventDelivery:       delivery,
		PendingEvents:       pending,
		Cache:               h.state.cacheStatus(),
		RateLimit:           h.state.rateLimitStatus(),
		RPCBudget:           h.state.rpcBudgetStatus(),
		Note: "non-authoritative status: authoritative reads continue from PostgreSQL;" +
			" degraded non-critical features never change funding gates",
	}
	if body.NonCriticalDegraded {
		body.Status = "degraded"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(body)
}

// cacheStatus renders the cache posture (T063): "disabled" without the 013
// wiring, "unknown" before the first Redis probe, "bypass" when Redis is
// unavailable and reads go straight to PostgreSQL, otherwise "enabled".
func (d *degradationState) cacheStatus() string {
	if d == nil || d.cache == nil {
		return "disabled"
	}
	available, known := d.signals.Availability("redis")
	switch {
	case !known:
		return "unknown"
	case !available:
		return "bypass"
	default:
		return "enabled"
	}
}

// rateLimitStatus renders the limiter posture (T063).
func (d *degradationState) rateLimitStatus() string {
	if d == nil || d.policy == nil {
		return "disabled"
	}
	if d.policy.Degraded() {
		return "unavailable"
	}
	if d.policy.Recovering() {
		return "recovering"
	}
	return "available"
}

// rpcBudgetStatus renders the RPC budget posture (T064) when attached.
func (d *degradationState) rpcBudgetStatus() map[string]string {
	if d == nil || d.rpcBudget == nil {
		return nil
	}
	return d.rpcBudget.Status()
}

// degradationMiddleware annotates query responses while the non-critical
// state is degraded: the request still runs unchanged (critical paths are
// never blocked), and the client learns the answer may be degraded instead of
// being presented as real-time.
type degradationMiddleware struct {
	state *degradationState
	next  http.Handler
}

func (m *degradationMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if m.state.Degraded() {
		w.Header().Set("X-TXHarbor-Degraded", "true")
	}
	if r.Method == http.MethodGet {
		// Authority reads never resolve through the cache; the annotation
		// makes the bypass explicit (T063; contracts/redis.md §2.1).
		w.Header().Set("X-TXHarbor-Cache", "bypass")
	}
	m.next.ServeHTTP(w, r)
}
