// us3admin_unit_test.go is the T031 evidence pin for the `reconcile-admin
// close` input rules (quickstart §call-path step 6; tasks T031 special
// verification; FR-010/019; contracts/discrepancy-lifecycle.md Transitions):
//
//   - the evidence tolerance comes from exactly two legal sources: an explicit
//     `--reverify-tolerance` that MUST stay positive, or the configured
//     TXHARBOR_RECON_FRESHNESS_TOLERANCE. There is no invented default: a
//     missing/non-positive configuration is refused by name;
//   - `--close-basis` is a required JSON object snapshot (range/block/version/
//     timestamp). A scalar, array, null or empty basis is never a snapshot and
//     is refused before any database access.
//
// The end-to-end DB behavior (the refusal is audited, nothing is written, the
// latest DB reverify row always wins) is proven by
// internal/reconciliation/lifecycle_evidence_honesty_integration_test.go and
// the `close` cases of us3admin_integration_test.go. These tests are pure
// input-unit level: no database and no Docker.
package reconcileadmin

import (
	"strings"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/config"
)

// TestParseReconcileCloseToleranceSourceAndRange pins the legal tolerance
// sources and the fail-closed refusal of illegal values.
func TestParseReconcileCloseToleranceSourceAndRange(t *testing.T) {
	configured := &config.Config{}
	configured.Recon.FreshnessTolerance = 90 * time.Minute
	zeroConfigured := &config.Config{}

	for _, tc := range []struct {
		name    string
		cfg     *config.Config
		raw     string
		want    time.Duration
		wantErr string
	}{
		{name: "explicit positive value is used", cfg: configured, raw: "2h", want: 2 * time.Hour},
		{name: "explicit value overrides the configured default", cfg: configured, raw: "5m", want: 5 * time.Minute},
		{name: "empty value takes the configured tolerance", cfg: configured, raw: "", want: 90 * time.Minute},
		{name: "explicit zero is refused", cfg: configured, raw: "0s", wantErr: "--reverify-tolerance"},
		{name: "explicit negative is refused", cfg: configured, raw: "-1m", wantErr: "--reverify-tolerance"},
		{name: "non-duration is refused", cfg: configured, raw: "soon", wantErr: "--reverify-tolerance"},
		{name: "unconfigured default is refused by key name", cfg: zeroConfigured, raw: "", wantErr: config.EnvReconFreshnessTolerance},
		{name: "nil config is refused by key name", cfg: nil, raw: "", wantErr: config.EnvReconFreshnessTolerance},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseReconcileCloseTolerance(tc.cfg, tc.raw)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("parseReconcileCloseTolerance(%q) err = nil, want a refusal naming %s", tc.raw, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not name %s", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseReconcileCloseTolerance(%q) err = %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("tolerance = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestParseReconcileCloseBasisRequiresObjectSnapshot pins that only a JSON
// object is a close-basis snapshot; everything else is refused by name.
func TestParseReconcileCloseBasisRequiresObjectSnapshot(t *testing.T) {
	valid := `{"range":"100..200","block":101,"version":"v1","observed_at":"2026-09-27T00:00:00Z"}`
	if got, err := parseReconcileCloseBasis(valid); err != nil || string(got) != valid {
		t.Fatalf("valid basis = %q (err %v), want the snapshot accepted verbatim", got, err)
	}
	for _, raw := range []string{"", "   ", "null", "[]", `"100..200"`, "101", "not-json"} {
		if _, err := parseReconcileCloseBasis(raw); err == nil {
			t.Fatalf("parseReconcileCloseBasis(%q) err = nil, want a refusal", raw)
		} else if !strings.Contains(err.Error(), "close-basis") {
			t.Fatalf("refusal for %q = %q, want it to name --close-basis", raw, err.Error())
		}
	}
}
