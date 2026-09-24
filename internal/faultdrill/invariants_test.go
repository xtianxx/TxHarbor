//go:build fault

// invariants_test.go holds the FR-06 invariant collection for the drill suites.
// It is a test file (not a test-support source file) because the upstream 004
// confinement rule allows only internal/indexer to reference the
// deposit_observations table outside tests.
package faultdrill

import (
	"context"
	"fmt"

	"github.com/xtianxx/txharbor/internal/events"
)

// CollectInvariants evaluates the FR-06 invariant queries. Every failure
// count must be zero; the totals are recorded for the evidence package.
func (e *Env) CollectInvariants(ctx context.Context) (InvariantReport, error) {
	var report InvariantReport
	queries := []struct {
		name  string
		query string
		dest  *int64
	}{
		{"duplicate intents", `
SELECT count(*) FROM (
  SELECT request_id FROM payment_intents GROUP BY request_id HAVING count(*) > 1
) d`, &report.DuplicateIntents},
		{"duplicate nonces", `
SELECT count(*) FROM (
  SELECT chain_id, sender, nonce FROM nonce_bindings GROUP BY 1, 2, 3 HAVING count(*) > 1
) d`, &report.DuplicateNonces},
		{"duplicate send slots", `
SELECT count(*) FROM (
  SELECT attempt_id, send_seq FROM tx_send_attempts GROUP BY 1, 2 HAVING count(*) > 1
) d`, &report.DuplicateSendSlots},
		{"orphaned ledger credits", `
SELECT count(*) FROM ` + events.RefLedgerTable + ` l
JOIN outbox_events e ON e.event_id = l.event_id
JOIN deposit_observations o
  ON o.chain_id = e.chain_id AND o.block_hash = e.block_hash
 AND o.tx_hash = e.tx_hash AND o.log_index = e.log_index
WHERE e.event_type = 'deposit.observation.created' AND o.status = 'orphaned'`, &report.OrphanedLedgerCredits},
		{"committed units without their event", `
SELECT
  (SELECT count(*) FROM deposit_observations o
   WHERE NOT EXISTS (
     SELECT 1 FROM outbox_events e
     WHERE e.event_type = 'deposit.observation.created'
       AND e.chain_id = o.chain_id AND e.block_hash = o.block_hash
       AND e.tx_hash = o.tx_hash AND e.log_index = o.log_index))
+ (SELECT count(*) FROM withdrawal_requests w
   WHERE NOT EXISTS (
     SELECT 1 FROM outbox_events e
     WHERE e.event_type = 'withdrawal.request.received'
       AND e.aggregate_id = w.request_id))`, &report.MissingEventsForUnits},
		{"blocked rows", `SELECT count(*) FROM outbox_events WHERE publish_state = 'blocked'`, &report.BlockedRows},
		{"open quarantine", `SELECT count(*) FROM consumer_quarantine WHERE status = 'open'`, &report.OpenQuarantine},
		{"outbox total", `SELECT count(*) FROM outbox_events`, &report.OutboxTotal},
		{"submitted requests", `SELECT count(*) FROM withdrawal_requests`, &report.SubmittedRequests},
		{"submitted observations", `SELECT count(*) FROM deposit_observations`, &report.SubmittedObservations},
	}
	for _, q := range queries {
		if err := e.Pool.QueryRow(ctx, q.query).Scan(q.dest); err != nil {
			return InvariantReport{}, fmt.Errorf("invariant %q: %w", q.name, err)
		}
	}
	return report, nil
}
