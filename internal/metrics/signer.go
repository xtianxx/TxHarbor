package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// 009 signer series (specs/009-signer-service, T012; FR-24). Counters and
// gauges only. Label values are fixed low-cardinality vocabularies — machine
// refusal classes, gate names (006/007/008), delivery verdicts, commit
// results — never caller/request/key/asset/amount content and never secrets.
const (
	SignerRequestsMetricName     = "txharbor_signer_requests_total"
	SignerSignedMetricName       = "txharbor_signer_signed_total"
	SignerRefusalsMetricName     = "txharbor_signer_refusals_total"
	SignerGateRefusalsMetricName = "txharbor_signer_gate_refusals_total"
	SignerAdmissionsMetricName   = "txharbor_signer_admissions_total"
	SignerCommitsMetricName      = "txharbor_signer_commits_total"
)

// registerSigner builds the signer series, registers them, and wires them
// into m. Called from New; kept here so metrics.go only gains fields.
func (m *Metrics) registerSigner(registry *prometheus.Registry) {
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: SignerRequestsMetricName,
		Help: "Signer submit requests received; one per accepted HTTP body.",
	}, nil)
	signed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: SignerSignedMetricName,
		Help: "Signatures produced; one per persisted signing result.",
	}, nil)
	refusals := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: SignerRefusalsMetricName,
		Help: "Signer refusals by machine class (fixed taxonomy vocabulary).",
	}, []string{"class"})
	gateRefusals := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: SignerGateRefusalsMetricName,
		Help: "Signer gate refusals by gate (006, 007, or 008).",
	}, []string{"gate"})
	admissions := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: SignerAdmissionsMetricName,
		Help: "Delivery admissions by verdict (fixed verdict vocabulary).",
	}, []string{"verdict"})
	commits := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: SignerCommitsMetricName,
		Help: "Sign/delivery commit results (fixed result vocabulary).",
	}, []string{"result"})
	registry.MustRegister(requests, signed, refusals, gateRefusals, admissions, commits)
	m.signerRequests = requests
	m.signerSigned = signed
	m.signerRefusals = refusals
	m.signerGateRefusals = gateRefusals
	m.signerAdmissions = admissions
	m.signerCommits = commits
}

// ObserveSignerRequest records one received submit request.
func (m *Metrics) ObserveSignerRequest() {
	m.signerRequests.WithLabelValues().Inc()
}

// ObserveSignerSigned records one produced signature (persisted result).
func (m *Metrics) ObserveSignerSigned() {
	m.signerSigned.WithLabelValues().Inc()
}

// ObserveSignerRefusal records one refusal by machine class. The class must
// be a taxonomy value from the signer's refusal vocabulary; request-derived
// content must never reach this label.
func (m *Metrics) ObserveSignerRefusal(class string) {
	m.signerRefusals.WithLabelValues(class).Inc()
}

// ObserveSignerGateRefusal records one gate refusal by gate name
// ("006", "007" or "008").
func (m *Metrics) ObserveSignerGateRefusal(gate string) {
	m.signerGateRefusals.WithLabelValues(gate).Inc()
}

// ObserveSignerAdmission records one delivery admission by verdict.
func (m *Metrics) ObserveSignerAdmission(verdict string) {
	m.signerAdmissions.WithLabelValues(verdict).Inc()
}

// ObserveSignerCommit records one commit result.
func (m *Metrics) ObserveSignerCommit(result string) {
	m.signerCommits.WithLabelValues(result).Inc()
}
