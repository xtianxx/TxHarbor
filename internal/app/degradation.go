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
//
// T063 (015) adds the recovery-period honesty block: when recovery mode is
// configured the surface reports the in-process posture and explicitly states
// what it does NOT attest — no restored/verified/released state, no cached
// release decision, no funding-gate substitution. The surface performs zero
// control-store reads and zero writes: the bounded read-only review with
// time/reads/rows budgets stays with `recovery-admin status` (F13), and an
// exhausted budget, a timeout or human knowledge is never gap closure or a
// resumption permission.
package app

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
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

	// recovery is the 015 T063 recovery-period posture. It is derived from
	// the serve-side assembly at startup and holds deployment binding facts
	// only: the surface never reads the control store, so a release decision
	// can never be displayed here as current.
	recovery *recoveryStatusPosture
}

// recoveryStatusPosture is the in-process recovery posture of the status
// surface (T063/FR-026): normal, configured (control store armed, process
// unbound) or bound (TXHARBOR_RECOVERY_INSTANCE names one instance).
type recoveryStatusPosture struct {
	mode     string
	instance string
}

const (
	recoveryStatusModeNormal     = "normal"
	recoveryStatusModeConfigured = "configured"
	recoveryStatusModeBound      = "bound"
)

// newRecoveryStatusPosture derives the posture from the serve-side recovery
// assembly: a nil wiring means normal mode — no recovery instance can exist
// and every entry keeps its pre-015 behavior. The posture carries instance
// identity only; it never carries a capability verdict.
func newRecoveryStatusPosture(wiring *serveRecoveryWiring) *recoveryStatusPosture {
	if wiring == nil {
		return nil
	}
	instance := strings.TrimSpace(wiring.instance)
	mode := recoveryStatusModeConfigured
	if instance != "" {
		mode = recoveryStatusModeBound
	}
	return &recoveryStatusPosture{mode: mode, instance: instance}
}

// recoveryStatusBody is the recovery section of GET /status/degradation. It
// reports the posture and the explicit non-attestations; it deliberately
// reports no per-capability released state, because that state is derived by
// the gate per admission and reviewed (with F13 bounds) by
// `recovery-admin status` — a cached copy here could present a stale or
// rolled-back view as normally consistent.
type recoveryStatusBody struct {
	Mode       string `json:"mode"`
	InstanceID string `json:"instance_id,omitempty"`
	// Attestation names what this surface does NOT prove.
	Attestation string `json:"attestation"`
	// Enforcement says how gated actions are admitted.
	Enforcement string `json:"enforcement"`
	// States names the authoritative derived-state review.
	States string `json:"states"`
	// FundingGate keeps the two-phase boundary explicit.
	FundingGate string `json:"funding_gate"`
}

// recoveryStatus renders the recovery section. Zero control-store reads and
// zero writes happen here, so an unbounded review is structurally impossible;
// the bounded review (range + time/reads/rows budgets, refusal audited with
// no state change) belongs to `recovery-admin status` (T051/F13).
func (d *degradationState) recoveryStatus() *recoveryStatusBody {
	if d == nil || d.recovery == nil {
		return &recoveryStatusBody{
			Mode:        recoveryStatusModeNormal,
			Attestation: "no recovery instance is configured; this surface attests no restored/verified/released state",
			Enforcement: "entries keep their pre-015 behavior; a recovery instance can only exist while the control store is configured",
			States:      "restored/verified/released are derived states of a recovery instance and are not reported here; authoritative review: recovery-admin status",
			FundingGate: "health and degradation signals never adjudicate the resumption gate or the existing fund gates",
		}
	}
	return &recoveryStatusBody{
		Mode:       d.recovery.mode,
		InstanceID: d.recovery.instance,
		Attestation: "this surface caches no release decision and attests no restored/verified/released state; " +
			"queried data may be restored-but-unverified until its capability is actually released",
		Enforcement: "every gated action is admitted per request through the resumption gate against the control store; " +
			"a refusal is returned at the entry as HTTP 503 with its refusal_class and is never cached here as available",
		States: "restored, verified and released are derived states and never impersonate one another; " +
			"authoritative review: recovery-admin status (bounded read-only review, F13: time/reads/rows budgets; " +
			"exhaustion refuses and audits without changing any state, and a timeout or an exhausted budget is never gap closure or a resumption permission)",
		FundingGate: "health and degradation signals never substitute the resumption gate or the existing fund gates; " +
			"the real action site keeps enforcing phase two independently",
	}
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
	Status              string              `json:"status"`
	NonCriticalDegraded bool                `json:"non_critical_degraded"`
	Dependencies        map[string]string   `json:"dependencies"`
	EventDelivery       string              `json:"event_delivery"`
	PendingEvents       int64               `json:"pending_events"`
	Cache               string              `json:"cache"`
	RateLimit           string              `json:"rate_limit"`
	RPCBudget           map[string]string   `json:"rpc_budget,omitempty"`
	Recovery            *recoveryStatusBody `json:"recovery"`
	Note                string              `json:"note"`
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
		Recovery:            h.state.recoveryStatus(),
		Note: "non-authoritative status: authoritative reads continue from PostgreSQL;" +
			" degraded non-critical features never change funding gates;" +
			" recovery and health signals never attest restored/verified/released state and never present" +
			" unverified or rolled-back data as normally consistent",
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
