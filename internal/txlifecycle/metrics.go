package txlifecycle

import (
	"github.com/xtianxx/txharbor/internal/metrics"
)

// metrics.go wires the 010 transaction-lifecycle observability surface
// (T003/R-010-13; constitution XII) onto the Store. Every ObserveTx* call
// passes a fixed-vocabulary value only (send outcome, refusal class,
// reconcile classification, receipt effect): no tx_hash, signature, signed
// byte or credential can ever become a metric label. Observability is
// best-effort and never alters an outcome; a nil Metrics records nothing.

// WithMetrics wires the shared observability surface into the store.
func (s *Store) WithMetrics(m *metrics.Metrics) *Store {
	s.metrics = m
	return s
}

// observeSend records one completed send-region result on the fixed-vocabulary
// series: dispatch counters carry accepted/rejected/unknown send facts, the
// unknown gauge marks an attempt whose business effect is undetermined, and a
// blocked result counts exactly one zero-dispatch gate refusal by class
// (FR-08; FR-21). Errors that carry no outcome (e.g. a 009 boundary refusal)
// are not dispatches and add no counter here.
func (s *Store) observeSend(res SendResult, err error) {
	if s == nil || s.metrics == nil {
		return
	}
	switch res.Outcome {
	case "blocked":
		if err != nil && res.RefusalClass != "" {
			s.metrics.ObserveTxGateRefusal(res.RefusalClass)
		}
	case "unknown":
		s.metrics.ObserveTxDispatch("unknown")
		s.metrics.ObserveTxUnknown(true)
	case "accepted", "rejected":
		s.metrics.ObserveTxDispatch(res.Outcome)
	}
}
