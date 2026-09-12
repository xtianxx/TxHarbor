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
)

// Custom metric names (contracts/observability.md freezes the indexer names).
const (
	ReadyMetricName = "txharbor_ready"
	ProbeMetricName = "txharbor_probe_total"

	IndexerCheckpointMetricName = "txharbor_indexer_checkpoint_height"
	IndexerStateMetricName      = "txharbor_indexer_state"
	IndexerRPCMetricName        = "txharbor_indexer_rpc_total"
	IndexerPauseMetricName      = "txharbor_indexer_pause_total"
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
	handler       http.Handler
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

	registry.MustRegister(readyGauge, probeTotal, indexerHeight, indexerState, indexerRPC, indexerPause)
	return &Metrics{
		registry:      registry,
		probeTotal:    probeTotal,
		indexerHeight: indexerHeight,
		indexerState:  indexerState,
		indexerRPC:    indexerRPC,
		indexerPause:  indexerPause,
		handler:       promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
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

func chainLabel(chain int64) string { return strconv.FormatInt(chain, 10) }
