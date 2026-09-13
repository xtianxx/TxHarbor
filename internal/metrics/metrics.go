// Package metrics wires the minimal Prometheus surface for 001/002: process/go
// collectors plus readiness, dependency probe and chain indexer metrics. No
// business metrics, no histograms, no backlog reservations (FR-019).
package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/xtianxx/txharbor/internal/logx"
)

// Custom metric names (contracts/observability.md freezes the indexer and log
// names).
const (
	ReadyMetricName = "txharbor_ready"
	ProbeMetricName = "txharbor_probe_total"

	IndexerCheckpointMetricName = "txharbor_indexer_checkpoint_height"
	IndexerStateMetricName      = "txharbor_indexer_state"
	IndexerRPCMetricName        = "txharbor_indexer_rpc_total"
	IndexerPauseMetricName      = "txharbor_indexer_pause_total"

	LogCheckpointNextMetricName = "txharbor_log_checkpoint_next"
	LogLagMetricName            = "txharbor_log_lag_blocks"
	LogStateMetricName          = "txharbor_log_state"
	LogRPCMetricName            = "txharbor_log_rpc_total"
	LogPauseMetricName          = "txharbor_log_pause_total"

	DepositNextMetricName         = "txharbor_deposit_next"
	DepositLagMetricName          = "txharbor_deposit_lag_blocks"
	DepositStateMetricName        = "txharbor_deposit_state"
	DepositObservationsMetricName = "txharbor_deposit_observations_total"
	DepositPauseMetricName        = "txharbor_deposit_pause_total"
	DepositTransitionMetricName   = "txharbor_deposit_transition_total"
)

// Metrics owns a private registry so multiple instances (tests, restarts of
// config) never collide.
type Metrics struct {
	registry      *prometheus.Registry
	probeTotal    *prometheus.CounterVec
	indexerHeight *prometheus.GaugeVec
	indexerState  *prometheus.GaugeVec
	indexerRPC    *prometheus.CounterVec
	indexerPause  *prometheus.CounterVec
	logNext       *prometheus.GaugeVec
	logLag        *prometheus.GaugeVec
	logState      *prometheus.GaugeVec
	logRPC        *prometheus.CounterVec
	logPause      *prometheus.CounterVec

	depositNext         *prometheus.GaugeVec
	depositLag          *prometheus.GaugeVec
	depositState        *prometheus.GaugeVec
	depositObservations *prometheus.CounterVec
	depositPause        *prometheus.CounterVec
	depositTransition   *prometheus.CounterVec

	handler http.Handler
}

// New builds the registry and the /metrics handler. ready is evaluated on
// every scrape (GaugeFunc).
func New(ready func() bool) *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	readyGauge := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: ReadyMetricName,
		Help: "Current readiness of the service: 1=ready, 0=not-ready.",
	}, func() float64 {
		if ready() {
			return 1
		}
		return 0
	})

	probeTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: ProbeMetricName,
		Help: "Total dependency probes by dependency and result.",
	}, []string{"dep", "result"})

	indexerHeight := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: IndexerCheckpointMetricName,
		Help: "Indexer checkpoint height per chain; absent while progress is empty.",
	}, []string{"chain"})

	indexerState := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: IndexerStateMetricName,
		Help: "Indexer state per chain: 0=running, 1=waiting, 2=retrying, 3=paused.",
	}, []string{"chain"})

	indexerRPC := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: IndexerRPCMetricName,
		Help: "Indexer chain RPC outcomes by failure class and result; not-found/ok is the wait polarity.",
	}, []string{"kind", "result"})

	indexerPause := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: IndexerPauseMetricName,
		Help: "Indexer pauses observed per chain; monotonic and independent of pause rows.",
	}, []string{"chain"})

	logNext := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: LogCheckpointNextMetricName,
		Help: "Log scan next block per chain; absent while progress is empty.",
	}, []string{"chain"})

	logLag := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: LogLagMetricName,
		Help: "Log lag in blocks per chain; absent while either checkpoint is empty.",
	}, []string{"chain"})

	logState := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: LogStateMetricName,
		Help: "Log scanner state per chain: 0=running, 1=waiting, 2=retrying, 3=paused.",
	}, []string{"chain"})

	logRPC := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: LogRPCMetricName,
		Help: "Log RPC outcomes by failure class and result; incomplete means completeness is suspect.",
	}, []string{"kind", "result"})

	logPause := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: LogPauseMetricName,
		Help: "Log pauses observed per chain; monotonic and independent of pause rows.",
	}, []string{"chain"})

	depositNext := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: DepositNextMetricName,
		Help: "Deposit scan next block per chain; absent while progress is empty.",
	}, []string{"chain"})

	depositLag := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: DepositLagMetricName,
		Help: "Deposit lag in blocks per chain; absent while either checkpoint is empty.",
	}, []string{"chain"})

	depositState := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: DepositStateMetricName,
		Help: "Deposit scanner state per chain: 0=running, 1=waiting for upstream coverage, 2=retrying, 3=paused, 4=structural gap halt.",
	}, []string{"chain"})

	depositObservations := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: DepositObservationsMetricName,
		Help: "Deposit processing results per chain; matched generates an observation, nomatch/zero/invalid do not.",
	}, []string{"chain", "result"})

	depositPause := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: DepositPauseMetricName,
		Help: "Deposit pauses observed per chain; monotonic and independent of pause rows.",
	}, []string{"chain"})

	depositTransition := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: DepositTransitionMetricName,
		Help: "Authorised deposit config transitions per chain; details live in deposit_config_history.",
	}, []string{"chain", "result"})

	registry.MustRegister(readyGauge, probeTotal,
		indexerHeight, indexerState, indexerRPC, indexerPause,
		logNext, logLag, logState, logRPC, logPause,
		depositNext, depositLag, depositState, depositObservations, depositPause, depositTransition)
	return &Metrics{
		registry:            registry,
		probeTotal:          probeTotal,
		indexerHeight:       indexerHeight,
		indexerState:        indexerState,
		indexerRPC:          indexerRPC,
		indexerPause:        indexerPause,
		logNext:             logNext,
		logLag:              logLag,
		logState:            logState,
		logRPC:              logRPC,
		logPause:            logPause,
		depositNext:         depositNext,
		depositLag:          depositLag,
		depositState:        depositState,
		depositObservations: depositObservations,
		depositPause:        depositPause,
		depositTransition:   depositTransition,
		handler:             promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	}
}

// Handler serves the Prometheus exposition format.
func (m *Metrics) Handler() http.Handler { return m.handler }

// Gatherer exposes the registry for tests and diagnostics.
func (m *Metrics) Gatherer() prometheus.Gatherer { return m.registry }

// ObserveProbe counts one DB/RPC probe result.
func (m *Metrics) ObserveProbe(dep string, ok bool) {
	result := "failure"
	if ok {
		result = "success"
	}
	m.probeTotal.WithLabelValues(dep, result).Inc()
}

// ObserveIndexerState records the scanner state for chain: 0 running,
// 1 waiting, 2 retrying, 3 paused (contracts/observability.md).
func (m *Metrics) ObserveIndexerState(chain int64, state int) {
	m.indexerState.WithLabelValues(chainLabel(chain)).Set(float64(state))
}

// ObserveIndexerCheckpoint records the checkpoint height for chain. ok=false
// means empty progress: the series is removed rather than zeroed.
func (m *Metrics) ObserveIndexerCheckpoint(chain int64, height uint64, ok bool) {
	if !ok {
		m.indexerHeight.DeleteLabelValues(chainLabel(chain))
		return
	}
	m.indexerHeight.WithLabelValues(chainLabel(chain)).Set(float64(height))
}

// ObserveIndexerRPC counts one classified chain RPC outcome. ok is true only
// for the not-found wait polarity; every other outcome is a failure class.
func (m *Metrics) ObserveIndexerRPC(kind string, ok bool) {
	result := "error"
	if ok {
		result = "ok"
	}
	m.indexerRPC.WithLabelValues(kind, result).Inc()
}

// ObserveIndexerPause counts one observed pause for chain.
func (m *Metrics) ObserveIndexerPause(chain int64) {
	m.indexerPause.WithLabelValues(chainLabel(chain)).Inc()
}

// ObserveLogCheckpointNext records the next block to scan for chain. ok=false
// means empty progress: the series is removed rather than zeroed.
func (m *Metrics) ObserveLogCheckpointNext(chain int64, next uint64, ok bool) {
	if !ok {
		m.logNext.DeleteLabelValues(chainLabel(chain))
		return
	}
	m.logNext.WithLabelValues(chainLabel(chain)).Set(float64(next))
}

// ObserveLogLag records the log lag in blocks for chain. ok=false means either
// checkpoint is empty: the series is removed rather than zeroed.
func (m *Metrics) ObserveLogLag(chain int64, lag uint64, ok bool) {
	if !ok {
		m.logLag.DeleteLabelValues(chainLabel(chain))
		return
	}
	m.logLag.WithLabelValues(chainLabel(chain)).Set(float64(lag))
}

// ObserveLogState records the log scanner state for chain: 0 running,
// 1 waiting, 2 retrying, 3 paused (contracts/observability.md). Pausing must
// not flip readyz.
func (m *Metrics) ObserveLogState(chain int64, state int) {
	m.logState.WithLabelValues(chainLabel(chain)).Set(float64(state))
}

// ObserveLogRPC counts one classified log RPC outcome. ok is true only for the
// not-found wait polarity and successful calls.
func (m *Metrics) ObserveLogRPC(kind string, ok bool) {
	result := "error"
	if ok {
		result = "ok"
	}
	m.logRPC.WithLabelValues(kind, result).Inc()
}

// ObserveLogPause counts one observed log pause for chain.
func (m *Metrics) ObserveLogPause(chain int64) {
	m.logPause.WithLabelValues(chainLabel(chain)).Inc()
}

// ObserveDepositNext records the next deposit block to process for chain.
// ok=false means empty progress: the series is removed rather than zeroed.
func (m *Metrics) ObserveDepositNext(chain int64, next uint64, ok bool) {
	if !ok {
		m.depositNext.DeleteLabelValues(chainLabel(chain))
		return
	}
	m.depositNext.WithLabelValues(chainLabel(chain)).Set(float64(next))
}

// ObserveDepositLag records the deposit lag in blocks for chain. ok=false
// means either the deposit or the 003 log checkpoint is empty: the series is
// removed rather than zeroed.
func (m *Metrics) ObserveDepositLag(chain int64, lag uint64, ok bool) {
	if !ok {
		m.depositLag.DeleteLabelValues(chainLabel(chain))
		return
	}
	m.depositLag.WithLabelValues(chainLabel(chain)).Set(float64(lag))
}

// ObserveDepositState records the deposit scanner state for chain: 0 running,
// 1 waiting for upstream coverage, 2 retrying, 3 paused, 4 structural gap
// halt (contracts/observability.md). Pausing or halting must not flip readyz.
func (m *Metrics) ObserveDepositState(chain int64, state int) {
	m.depositState.WithLabelValues(chainLabel(chain)).Set(float64(state))
}

// ObserveDepositObservation counts one processed log by result:
// matched|nomatch|zero|invalid (contracts/observability.md).
func (m *Metrics) ObserveDepositObservation(chain int64, result string) {
	m.depositObservations.WithLabelValues(chainLabel(chain), result).Inc()
}

// ObserveDepositPause counts one observed deposit pause for chain.
func (m *Metrics) ObserveDepositPause(chain int64) {
	m.depositPause.WithLabelValues(chainLabel(chain)).Inc()
}

// ObserveDepositTransition counts one authorised config transition by
// result: ok|error|rejected (contracts/observability.md). Audit details live
// in the deposit_config_history rows, not in this counter.
func (m *Metrics) ObserveDepositTransition(chain int64, result string) {
	m.depositTransition.WithLabelValues(chainLabel(chain), result).Inc()
}

// Deposit log events and their frozen structured field lists
// (contracts/observability.md). The scanner (T005+) logs exactly these fields
// per event; the T019 audit walks a captured record against this manifest and
// DepositLogRedact.
const (
	DepositLogAdvance      = "advance"
	DepositLogWait         = "wait"
	DepositLogRetry        = "retry"
	DepositLogGap          = "gap"
	DepositLogPause        = "pause"
	DepositLogRelease      = "release"
	DepositLogConfigReject = "config_reject"
	DepositLogTransition   = "transition"
)

// DepositLogFields is the frozen field list per deposit log event. Renaming a
// field or an event here is an observability contract change.
var DepositLogFields = map[string][]string{
	DepositLogAdvance:      {"chain_id", "from_block", "to_block", "matched", "nomatch", "zero", "attempt"},
	DepositLogWait:         {"chain_id", "next_block", "reason"},
	DepositLogRetry:        {"chain_id", "from_block", "to_block", "kind", "attempt", "retry_in"},
	DepositLogGap:          {"chain_id", "gap_from", "gap_to", "class", "cause", "config"},
	DepositLogPause:        {"chain_id", "pause_id", "revision", "height", "kind", "detail"},
	DepositLogRelease:      {"chain_id", "pause_id", "revision", "operator", "reason", "result"},
	DepositLogConfigReject: {"chain_id", "reason", "detail"},
	DepositLogTransition:   {"chain_id", "request_id", "operator", "old_config", "new_config", "replay_from", "result", "reason"},
}

// DepositLogRedact scrubs a deposit log value before it reaches slog
// (SC-09): error and detail strings go through this hook, amounts are logged
// as decimal strings only, and raw contract data is never dumped. It wraps
// logx.Redact so the scanner (T005+) and the contract tests share one funnel.
func DepositLogRedact(s string) string { return logx.Redact(s) }

func chainLabel(chain int64) string { return strconv.FormatInt(chain, 10) }
