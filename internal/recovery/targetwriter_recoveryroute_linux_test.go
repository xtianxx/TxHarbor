//go:build linux

package recovery

// targetwriter_recoveryroute_test.go (2026-10-04 implement round, T019 R2/R3
// closed acceptance): unit negatives for the protected peer-entry discipline —
// passwords, TCP/IP hosts, TLS params, identity overrides and the original
// writer role itself are refused BEFORE any guard, marker or child work; the
// wrong_lane role (equivalent grants, not the recovery identity) is the
// protected-entry zero-write negative companion (full integration evidence:
// docs/evidence/015/r2-grant-inventory/.

import (
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

func TestValidateRecoveryRouteRefusals(t *testing.T) {
	trusted := controlstore.DSNTarget{
		Host: "/var/run/postgresql", Port: 5432, Database: "txharbor", Role: "writer_owner",
	}
	cases := []struct {
		name  string
		route string
		want  string
	}{
		{"empty", "", "recovery route is empty"},
		{"password_uri", "postgres://recovery_r:secret@/txharbor", "not parseable"},
		{"keyword_with_password_field", "host=/var/run/postgresql port=5432 dbname=txharbor user=recovery_r password=secret", "not parseable"},
		{"tcp_loopback_host", "host=127.0.0.1 port=5432 dbname=txharbor user=recovery_r", "socket directory"},
		{"ip_host", "host=10.0.0.1 port=5432 dbname=txharbor user=recovery_r", "socket directory"},
		{"wrong_database", "host=/var/run/postgresql port=5432 dbname=otherdb user=recovery_r", "does not match the trusted target"},
		{"original_writer_role", "host=/var/run/postgresql port=5432 dbname=txharbor user=writer_owner", "original writer role"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRecoveryRoute(tc.route, trusted)
			if err == nil {
				t.Fatalf("route %q was accepted", tc.route)
			}
			if !strings.Contains(err.Error(), tc.want) && !(tc.want == "not parseable" && strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("refusal %q does not match expected %q", err.Error(), tc.want)
			}
		})
	}
	t.Run("valid_peer_route_accepted", func(t *testing.T) {
		if err := validateRecoveryRoute("host=/var/run/postgresql port=5432 dbname=txharbor user=recovery_r", trusted); err != nil {
			t.Fatalf("valid peer route refused: %v", err)
		}
	})
}
