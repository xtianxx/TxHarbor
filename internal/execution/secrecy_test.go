// secrecy_test.go executes the quickstart V12.4/V12.5 observability, secrecy and
// integer-discipline assertions (T049) without a database: the V-scenario rows
// and events carry the structured identity/version fields FR-13 names, no
// credential/key/raw-signed-bytes token appears in the 011 sources, amounts,
// fees and nonces are never carried through a float type, and the T002 worker
// metrics are recorded on the shared registry. The shared Prometheus surface
// (internal/metrics) is deliberately out of scan scope: float64 there is the
// client library's API, never a money or chain value.
package execution

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/metrics"
)

// fieldNames returns the exported Go field names of a struct carrier.
func fieldNames(v any) []string {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	var out []string
	for i := 0; i < t.NumField(); i++ {
		out = append(out, t.Field(i).Name)
	}
	return out
}

// TestV12StructuredFieldsPresent pins the FR-13 / persistence.md §9 field list
// onto the durable carriers. scope_version is carried by 011 as
// authorization_version (the version of the authorization/scope the intent is
// bound to); every other field is a literal field name.
func TestV12StructuredFieldsPresent(t *testing.T) {
	carriers := []any{
		Intent{}, Claim{}, Step{}, StepIssue{}, StepConverge{}, Event{},
		StepRequest{}, StepOutcome{}, AdvanceRequest{}, AdvanceOutcome{},
		AttemptRef{}, UnknownRef{}, LifecycleFacts{}, RevisionFact{}, RevisionResult{},
	}
	present := map[string]bool{}
	for _, c := range carriers {
		for _, name := range fieldNames(c) {
			present[name] = true
		}
	}

	// required maps the persistence.md §9 structured field to its carrier.
	required := map[string]string{
		"request_id":       "RequestID",
		"intent_id":        "IntentID",
		"caller_id":        "CallerID",
		"sender":           "Sender",
		"owner_id":         "OwnerID",
		"lease_version":    "LeaseVersion",
		"step_id":          "StepID",
		"action":           "Action",
		"attempt_id":       "AttemptID",
		"tx_hash":          "TxHash",
		"authorization_id": "AuthorizationID",
		"scope_version":    "AuthorizationVersion",
		"recovery_version": "RecoveryVersion",
		"revision_version": "RevisionVersion",
		"outcome_class":    "OutcomeClass",
	}
	for field, goName := range required {
		if !present[goName] {
			t.Errorf("structured field %s (%s) is not carried by any 011 row/event struct", field, goName)
		}
	}
}

// 011 business/value sources: the execution package, the 011 app carriers and
// the 011 config. Test files and the shared metrics registry are excluded (see
// the package comment).
func featureSources(t *testing.T) []string {
	t.Helper()
	var files []string
	dirs := []struct{ dir, glob string }{
		{".", "*.go"},
		{"../app", "withdrawal*.go"},
	}
	for _, d := range dirs {
		matches, err := filepath.Glob(filepath.Join(d.dir, d.glob))
		if err != nil {
			t.Fatalf("glob %s/%s: %v", d.dir, d.glob, err)
		}
		for _, m := range matches {
			if strings.HasSuffix(m, "_test.go") {
				continue
			}
			files = append(files, m)
		}
	}
	files = append(files, "../config/config.go")
	if len(files) < 10 {
		t.Fatalf("feature source scan found only %d files", len(files))
	}
	return files
}

func TestV12NoFloatIn011Paths(t *testing.T) {
	for _, path := range featureSources(t) {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", path, err)
		}
		for _, token := range []string{"float64", "float32"} {
			if strings.Contains(string(body), token) {
				t.Errorf("%s carries a %s value path; amounts/fees/nonces must be integer decimal strings", path, token)
			}
		}
	}
}

func TestV12NoSecretsIn011Paths(t *testing.T) {
	keyLiteral := regexp.MustCompile(`\b[0-9a-fA-F]{64}\b`)
	forbidden := []string{
		"PRIVATE KEY", "PrivateKey", "privateKey",
		"SignTx", "SignHash", "SignMessage", "ecdsa", "rlp.",
		"RawTransaction", "rawTransaction", "signedTx", "SignedTx",
		"eth_sendRawTransaction",
	}
	for _, path := range featureSources(t) {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", path, err)
		}
		if m := keyLiteral.Find(body); m != nil {
			t.Errorf("%s embeds a 64-hex key-shaped literal: %s", path, m)
		}
		for _, token := range forbidden {
			if strings.Contains(string(body), token) {
				t.Errorf("%s contains credential/signing/raw-bytes token %q", path, token)
			}
		}
	}
}

// TestV12WorkerMetricsRecorded drives every T002 worker metric and asserts it is
// exposed on the shared registry, label values being fixed vocabularies only.
func TestV12WorkerMetricsRecorded(t *testing.T) {
	m := metrics.New(func() bool { return true })
	m.ObserveWorkerClaimAcquisition("acquired")
	m.ObserveWorkerClaimTakeover()
	m.ObserveWorkerClaimRevocation()
	m.ObserveWorkerStallFlag()
	m.SetWorkerStepsOpen(1)
	m.SetWorkerUnknownPending(1)
	m.ObserveWorkerReconcile("converged")
	m.ObserveWorkerGateRefusal(string(ClassClaimLost))
	m.SetWorkerProjectionStale(1)
	m.ObserveWorkerAdvance("sent")
	m.SetWorkerAdvanceSeconds(0.5)

	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	got := map[string]bool{}
	for _, f := range families {
		got[f.GetName()] = true
	}
	for _, name := range []string{
		metrics.WorkerClaimAcquisitionsMetricName,
		metrics.WorkerClaimTakeoversMetricName,
		metrics.WorkerClaimRevocationsMetricName,
		metrics.WorkerStallFlagsMetricName,
		metrics.WorkerStepsOpenMetricName,
		metrics.WorkerUnknownPendingMetricName,
		metrics.WorkerReconcileMetricName,
		metrics.WorkerGateRefusalsMetricName,
		metrics.WorkerProjectionStaleMetricName,
		metrics.WorkerAdvanceMetricName,
		metrics.WorkerAdvanceSecondsMetricName,
	} {
		if !got[name] {
			t.Errorf("worker metric %s is not recorded on the registry", name)
		}
	}
}
