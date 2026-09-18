package txlifecycle

import "context"

// TxMetrics is 010's observability sink (R-010-13; constitution XII).
// internal/metrics.Metrics satisfies it; every label is a fixed vocabulary, so
// no tx_hash, signature, signed byte or credential can become a label.
type TxMetrics interface {
	ObserveTxDispatch(outcome string)
	ObserveTxGateRefusal(class string)
	ObserveTxUnknown(unknown bool)
	ObserveTxReconcile(classification string)
	ObserveTxReceiptEffect(effect string)
	ObserveTxRevision()
}

// WithMetrics wires the observability sink; nil disables emission.
func (s *Store) WithMetrics(m TxMetrics) *Store {
	s.metrics = m
	return s
}

// refuse funnels every zero-dispatch refusal through the gate-refusal counter
// and returns the blocked result.
func (s *Store) refuse(a *Attempt, ref *RefusalError) (SendResult, error) {
	if s != nil && s.metrics != nil {
		s.metrics.ObserveTxGateRefusal(string(ref.Class))
	}
	return blocked(a, ref)
}

func (s *Store) observeDispatch(outcome string) {
	if s != nil && s.metrics != nil {
		s.metrics.ObserveTxDispatch(outcome)
	}
}

func (s *Store) observeReconcile(classification string) {
	if s != nil && s.metrics != nil {
		s.metrics.ObserveTxReconcile(classification)
	}
}

func (s *Store) observeReceiptEffect(effect string) {
	if s != nil && s.metrics != nil {
		s.metrics.ObserveTxReceiptEffect(effect)
	}
}

func (s *Store) observeRevision() {
	if s != nil && s.metrics != nil {
		s.metrics.ObserveTxRevision()
	}
}

// observeUnknown refreshes the global unknown gauge: 1 while any attempt's
// business effect is unknown, else 0.
func (s *Store) observeUnknown(ctx context.Context) {
	if s == nil || s.metrics == nil {
		return
	}
	var unknown bool
	if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM tx_attempts WHERE state = 'unknown')`).Scan(&unknown); err != nil {
		return
	}
	s.metrics.ObserveTxUnknown(unknown)
}
