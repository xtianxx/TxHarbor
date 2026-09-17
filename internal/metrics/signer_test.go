package metrics

import (
	"testing"
)

// counterValue sums a family by name from a gather.
func signerCounterValue(t *testing.T, m *Metrics, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	var sum float64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, metric := range f.GetMetric() {
			match := true
			for _, lp := range metric.GetLabel() {
				if want, ok := labels[lp.GetName()]; !ok || want != lp.GetValue() {
					match = false
				}
			}
			if match {
				sum += metric.GetCounter().GetValue()
			}
		}
	}
	return sum
}

func TestSignerMetricsContract(t *testing.T) {
	m := New(func() bool { return true })
	m.ObserveSignerRequest()
	m.ObserveSignerRequest()
	m.ObserveSignerSigned()
	m.ObserveSignerRefusal("validation_failed")
	m.ObserveSignerRefusal("authorization_unverifiable")
	m.ObserveSignerGateRefusal("008")
	m.ObserveSignerAdmission("delivered")
	m.ObserveSignerAdmission("withheld")
	m.ObserveSignerCommit("committed")
	m.ObserveSignerCommit("unknown")

	if got := signerCounterValue(t, m, SignerRequestsMetricName, nil); got != 2 {
		t.Fatalf("requests = %v, want 2", got)
	}
	if got := signerCounterValue(t, m, SignerSignedMetricName, nil); got != 1 {
		t.Fatalf("signed = %v, want 1", got)
	}
	if got := signerCounterValue(t, m, SignerRefusalsMetricName, map[string]string{"class": "validation_failed"}); got != 1 {
		t.Fatalf("refusals{validation_failed} = %v, want 1", got)
	}
	if got := signerCounterValue(t, m, SignerGateRefusalsMetricName, map[string]string{"gate": "008"}); got != 1 {
		t.Fatalf("gate refusals{008} = %v, want 1", got)
	}
	if got := signerCounterValue(t, m, SignerAdmissionsMetricName, map[string]string{"verdict": "withheld"}); got != 1 {
		t.Fatalf("admissions{withheld} = %v, want 1", got)
	}
	if got := signerCounterValue(t, m, SignerCommitsMetricName, map[string]string{"result": "unknown"}); got != 1 {
		t.Fatalf("commits{unknown} = %v, want 1", got)
	}

	// Label values stay inside fixed vocabularies: nothing request-derived
	// (caller, sender, amount, key material) may appear.
	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	allowedLabel := func(name, value string) bool {
		switch name {
		case "class":
			switch value {
			case "validation_failed", "authorization_unverifiable":
				return true
			}
		case "gate":
			return value == "006" || value == "007" || value == "008"
		case "verdict":
			return value == "delivered" || value == "withheld"
		case "result":
			return value == "committed" || value == "unknown"
		}
		return false
	}
	for _, f := range families {
		switch f.GetName() {
		case SignerRefusalsMetricName, SignerGateRefusalsMetricName,
			SignerAdmissionsMetricName, SignerCommitsMetricName:
			for _, metric := range f.GetMetric() {
				for _, lp := range metric.GetLabel() {
					if !allowedLabel(lp.GetName(), lp.GetValue()) {
						t.Fatalf("series %s carries out-of-vocabulary label %s=%q",
							f.GetName(), lp.GetName(), lp.GetValue())
					}
				}
			}
		}
	}
}
