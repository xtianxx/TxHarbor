//go:build integration

package txlifecycle

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/metrics"
)

// txMetricValue reads one counter or gauge series from a real registry; an
// absent series reads as 0.
func txMetricValue(t *testing.T, m *metrics.Metrics, name string, want map[string]string) float64 {
	t.Helper()
	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, met := range fam.GetMetric() {
			got := make(map[string]string, len(met.GetLabel()))
			for _, pair := range met.GetLabel() {
				got[pair.GetName()] = pair.GetValue()
			}
			match := true
			for k, v := range want {
				if got[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			if c := met.GetCounter(); c != nil {
				return c.GetValue()
			}
			if g := met.GetGauge(); g != nil {
				return g.GetValue()
			}
		}
	}
	return 0
}

func txMetricExists(t *testing.T, m *metrics.Metrics, name string) bool {
	t.Helper()
	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() == name {
			return true
		}
	}
	return false
}

// TestV11ObservabilityCounters is T055/V11: the 010 counters exist and are
// recorded on the V-scenarios (accepted/unknown dispatch, gate refusal, unknown
// gauge, reconcile class, receipt effect, reorg revision) (constitution XII;
// R-010-13).
func TestV11ObservabilityCounters(t *testing.T) {
	m := metrics.New(func() bool { return true })
	e := newEnv(t)
	e.store = e.store.WithMetrics(m)
	ctx := context.Background()

	// V8/V9 path: accepted dispatch, effective receipt, reorg revision.
	e.addBlock(98, blockHashHex(98), true)
	f := e.seed()
	f.sign()
	if res, err := e.send(f, SendInitial, nil); err != nil || res.Outcome != "accepted" {
		t.Fatalf("send = %+v %v", res, err)
	}
	e.rpc.mu.Lock()
	e.rpc.txFound = true
	e.rpc.txPending = false
	e.rpc.receipt = receiptAt(98, blockHashHex(98), 1, []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)})
	e.rpc.mu.Unlock()
	if res, err := e.store.Reconcile(ctx, f.attemptID, ""); err != nil || res.ReceiptEffect != "effective" {
		t.Fatalf("receipt reconcile = %+v %v", res, err)
	}
	e.exec(`UPDATE chain_blocks SET canonical = FALSE WHERE chain_id = $1 AND number = 98`, e.chainID)
	e.rpc.mu.Lock()
	e.rpc.receipt = receiptAt(100, blockHashHex(100), 1, []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)})
	e.rpc.mu.Unlock()
	if _, err := e.store.Reconcile(ctx, f.attemptID, ""); err != nil {
		t.Fatalf("reorg reconcile: %v", err)
	}

	// V4/FR-03 path: a dispatch timeout lands unknown.
	e.rpc.mu.Lock()
	e.rpc.txFound = false
	e.rpc.txPending = false
	e.rpc.receipt = nil
	e.rpc.mu.Unlock()
	u := e.seed()
	u.sign()
	e.rpc.mu.Lock()
	e.rpc.txErr = &eth.Error{Kind: eth.KindTimeout, Op: "stub"}
	e.rpc.mu.Unlock()
	if res, err := e.send(u, SendInitial, nil); err != nil || res.Outcome != "unknown" {
		t.Fatalf("timeout send = %+v %v", res, err)
	}
	e.rpc.mu.Lock()
	e.rpc.txErr = nil
	e.rpc.mu.Unlock()
	if got := txMetricValue(t, m, metrics.TxUnknownMetricName, nil); got != 1 {
		t.Fatalf("unknown gauge = %v, want 1", got)
	}

	// V6 path: a zero-dispatch gate refusal.
	g := e.seed()
	g.sign()
	e.exec(`DELETE FROM execution_claims WHERE intent_id = $1`, g.intentID)
	before := e.rpc.dispatchCount()
	res, err := e.send(g, SendInitial, nil)
	if res.RefusalClass != string(ClassClaimAbsent) {
		t.Fatalf("claim refusal class = %s (%v), want claim_absent", res.RefusalClass, err)
	}
	if e.rpc.dispatchCount() != before {
		t.Fatal("gate refusal dispatched")
	}

	for _, name := range []string{
		metrics.TxDispatchMetricName, metrics.TxGateRefusalMetricName, metrics.TxUnknownMetricName,
		metrics.TxReconcileMetricName, metrics.TxReceiptEffectMetricName, metrics.TxRevisionMetricName,
	} {
		if !txMetricExists(t, m, name) {
			t.Errorf("metric family %s missing", name)
		}
	}
	if got := txMetricValue(t, m, metrics.TxDispatchMetricName, map[string]string{"outcome": "accepted"}); got < 1 {
		t.Errorf("dispatch accepted = %v, want >= 1", got)
	}
	if got := txMetricValue(t, m, metrics.TxDispatchMetricName, map[string]string{"outcome": "unknown"}); got < 1 {
		t.Errorf("dispatch unknown = %v, want >= 1", got)
	}
	if got := txMetricValue(t, m, metrics.TxGateRefusalMetricName, map[string]string{"class": string(ClassClaimAbsent)}); got < 1 {
		t.Errorf("gate refusal claim_absent = %v, want >= 1", got)
	}
	if got := txMetricValue(t, m, metrics.TxReconcileMetricName, map[string]string{"classification": "included"}); got < 1 {
		t.Errorf("reconcile included = %v, want >= 1", got)
	}
	if got := txMetricValue(t, m, metrics.TxReconcileMetricName, map[string]string{"classification": "not_found_yet"}); got < 1 {
		t.Errorf("reconcile not_found_yet = %v, want >= 1", got)
	}
	if got := txMetricValue(t, m, metrics.TxReceiptEffectMetricName, map[string]string{"effect": "effective"}); got < 1 {
		t.Errorf("receipt effect effective = %v, want >= 1", got)
	}
	if got := txMetricValue(t, m, metrics.TxRevisionMetricName, nil); got < 1 {
		t.Errorf("revision counter = %v, want >= 1", got)
	}
}
