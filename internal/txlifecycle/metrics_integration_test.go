//go:build integration

package txlifecycle

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/metrics"
)

// metrics_integration_test.go executes quickstart V11's observability
// acceptance (T055; constitution XII; R-010-13): the six fixed-vocabulary 010
// series — dispatch outcomes, gate refusals by class, the unknown gauge,
// reconcile classifications, receipt effects and revisions — exist on the real
// registry and are recorded by the REAL store paths on the V-scenarios
// (pause refusal, accepted dispatch, timeout unknown, reconcile + effective
// receipt, reorg orphan + reconfirm). A final scan proves no metric label
// ever carries a signature, a signed byte or a credential.

// scrapeMetrics renders the exposition text through the same handler serve
// mounts at /metrics.
func scrapeMetrics(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

// metricSeries is one parsed exposition series line.
type metricSeries struct {
	name   string
	labels string
	value  float64
}

// parseSeries extracts every numeric series line (TYPE/HELP excluded).
func parseSeries(body string) []metricSeries {
	var out []metricSeries
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		s := metricSeries{value: v}
		if i := strings.Index(fields[0], "{"); i >= 0 {
			s.name = fields[0][:i]
			s.labels = strings.Trim(fields[0][i+1:], "{}")
		} else {
			s.name = fields[0]
		}
		out = append(out, s)
	}
	return out
}

// seriesValue sums the series named `name` whose labels contain `label`
// ("" matches the unlabeled series only). An absent series reads as 0: a
// counter has no series until its first observation.
func seriesValue(body string, name, label string) float64 {
	total := 0.0
	for _, s := range parseSeries(body) {
		if s.name != name {
			continue
		}
		if label == "" && s.labels != "" {
			continue
		}
		if label != "" && !strings.Contains(s.labels, label) {
			continue
		}
		total += s.value
	}
	return total
}

// expectSeriesDelta asserts one scenario moved exactly `want` on one series.
func expectSeriesDelta(t *testing.T, before, after string, name, label string, want float64) {
	t.Helper()
	got := seriesValue(after, name, label) - seriesValue(before, name, label)
	if got != want {
		t.Fatalf("series %s{%s} moved by %v, want %v", name, label, got, want)
	}
}

func TestV11TxMetricsRecorded(t *testing.T) {
	m := metrics.New(func() bool { return true })
	e := newEnv(t)
	e.store = e.store.WithMetrics(m)
	ctx := context.Background()

	names := []string{
		metrics.TxDispatchMetricName,
		metrics.TxGateRefusalMetricName,
		metrics.TxUnknownMetricName,
		metrics.TxReconcileMetricName,
		metrics.TxReceiptEffectMetricName,
		metrics.TxRevisionMetricName,
	}

	base := scrapeMetrics(t, m)

	var secretSamples []string

	// V6: a present pause refuses with zero dispatch and counts exactly one
	// gate refusal under the fixed pause_present class.
	f0 := e.seed()
	f0.sign()
	e.exec(`INSERT INTO indexer_pause (chain_id, height, expected_hash, actual_hash, kind)
		VALUES ($1,100,$2,$3,'hash_mismatch')`, e.chainID, blockHashHex(100), blockHashHex(101))
	before := e.rpc.dispatchCount()
	res, err := e.send(f0, SendInitial, nil)
	assertBlocked(t, e, f0.attemptID, res, err, ClassPausePresent, before)
	e.exec(`DELETE FROM indexer_pause WHERE chain_id = $1`, e.chainID)
	after := scrapeMetrics(t, m)
	expectSeriesDelta(t, base, after, metrics.TxGateRefusalMetricName, `class="pause_present"`, 1)
	expectSeriesDelta(t, base, after, metrics.TxDispatchMetricName, "", 0)

	// V3: one accepted dispatch counts one accepted send fact.
	f1 := e.seed()
	f1.sign()
	secretSamples = append(secretSamples, f1.signOnly().Signature)
	res, err = e.send(f1, SendInitial, nil)
	if err != nil || res.Outcome != "accepted" {
		t.Fatalf("send f1 = %+v %v, want accepted", res, err)
	}
	secretSamples = append(secretSamples, res.TxHash)
	after = scrapeMetrics(t, m)
	expectSeriesDelta(t, base, after, metrics.TxDispatchMetricName, `outcome="accepted"`, 1)

	// V4: a timeout dispatch is a send fact (unknown outcome) and marks the
	// unknown gauge: the business effect is undetermined.
	f2 := e.seed()
	f2.sign()
	e.rpc.mu.Lock()
	e.rpc.txErr = &eth.Error{Kind: eth.KindTimeout, Op: "stub"}
	e.rpc.mu.Unlock()
	res, err = e.send(f2, SendInitial, nil)
	if err != nil || res.Outcome != "unknown" {
		t.Fatalf("send f2 = %+v %v, want unknown", res, err)
	}
	e.rpc.mu.Lock()
	e.rpc.txErr = nil
	e.rpc.mu.Unlock()
	secretSamples = append(secretSamples, res.TxHash)
	after = scrapeMetrics(t, m)
	expectSeriesDelta(t, base, after, metrics.TxDispatchMetricName, `outcome="unknown"`, 1)
	if got := seriesValue(after, metrics.TxUnknownMetricName, ""); got != 1 {
		t.Fatalf("unknown gauge = %v, want 1", got)
	}

	// V5/V7: reconcile observes f3 included; the effective receipt counts its
	// effect on the receipt-effect series.
	f3 := e.seed()
	f3.sign()
	if _, err := e.send(f3, SendInitial, nil); err != nil {
		t.Fatalf("send f3: %v", err)
	}
	e.rpc.mu.Lock()
	e.rpc.txFound = true
	e.rpc.txPending = false
	e.rpc.receipt = synthReceipt(f3.sender, 100)
	e.rpc.mu.Unlock()
	t.Cleanup(func() {
		e.rpc.mu.Lock()
		e.rpc.txFound = false
		e.rpc.receipt = nil
		e.rpc.mu.Unlock()
	})
	rec, err := e.store.Reconcile(ctx, f3.attemptID, "")
	if err != nil || rec.Classification != "included" || rec.ReceiptEffect != "effective" {
		t.Fatalf("reconcile f3 = %+v %v, want included/effective", rec, err)
	}
	after = scrapeMetrics(t, m)
	expectSeriesDelta(t, base, after, metrics.TxReconcileMetricName, `classification="included"`, 1)
	expectSeriesDelta(t, base, after, metrics.TxReceiptEffectMetricName, `effect="effective"`, 1)

	// V9: a reorg revises the revision chain twice — the orphaned receipt and
	// the re-inclusion reconfirm each count one revision.
	e.exec(`UPDATE chain_blocks SET canonical = FALSE WHERE chain_id = $1 AND number = 100`, e.chainID)
	e.addBlock(101, blockHashHex(101), true)
	e.rpc.mu.Lock()
	e.rpc.receipt = synthReceipt(f3.sender, 101)
	e.rpc.mu.Unlock()
	if _, err := e.store.Reconcile(ctx, f3.attemptID, ""); err != nil {
		t.Fatalf("reconcile after reorg: %v", err)
	}
	e.rpc.mu.Lock()
	e.rpc.receipt = synthReceipt(f3.sender, 101)
	e.rpc.mu.Unlock()
	if _, err := e.store.Reconcile(ctx, f3.attemptID, ""); err != nil {
		t.Fatalf("reconcile after re-inclusion: %v", err)
	}
	after = scrapeMetrics(t, m)
	expectSeriesDelta(t, base, after, metrics.TxRevisionMetricName, "", 2)

	// The secrecy scan: across the whole scrape, no label value may carry a
	// signature, a signed byte, a tx hash or a credential — every label is a
	// fixed vocabulary (FR-21/SC-09; constitution XII). After the scenarios
	// every series has at least one child, so all six names must be exposed.
	final := scrapeMetrics(t, m)
	for _, name := range names {
		if !strings.Contains(final, "# HELP "+name+" ") {
			t.Fatalf("scrape missing metric name %q — the six 010 series must exist", name)
		}
	}
	for _, s := range parseSeries(final) {
		for _, bad := range secretSamples {
			if bad == "" {
				continue
			}
			if strings.Contains(s.labels, bad) {
				t.Fatalf("metric %s{%s} leaks a secret-bearing value", s.name, s.labels)
			}
		}
	}
	if strings.Contains(final, "test-credential") {
		t.Fatal("scrape contains a credential string")
	}
}
