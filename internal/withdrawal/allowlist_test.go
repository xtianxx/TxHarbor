// allowlist_test.go owns spec task T005 (012-007-authorization-carrier):
// unit coverage for the issuance allowlist loader and the pure PermitIssue
// predicate (R-PB9/R-PB10, contract supply-scope.md precondition 2). No
// database is involved; the CLI wiring that maps a load error to exit 2 lives
// in the T013 carrier, not here.
package withdrawal

import (
	"strings"
	"testing"
)

// allowlistEnv is a LookupEnv-shaped getter over a fixed map.
func allowlistEnv(vars map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := vars[k]
		return v, ok
	}
}

func TestLoadIssuerAllowlistPermitsMappedCallers(t *testing.T) {
	// Given the env carries caller_ids 7 and 42 with stray whitespace and
	// empty entries
	allow, err := LoadIssuerAllowlist(allowlistEnv(map[string]string{
		EnvIssuerCallers: " 7 , ,42,  ",
	}))
	if err != nil {
		t.Fatalf("LoadIssuerAllowlist() error = %v, want nil", err)
	}
	// Then each mapped caller is permitted and any other is denied (deny-by-default)
	for _, id := range []int64{7, 42} {
		if !allow.PermitIssue(id) {
			t.Errorf("PermitIssue(%d) = false, want true", id)
		}
	}
	if allow.PermitIssue(8) {
		t.Error("PermitIssue(8) = true, want false (unmapped)")
	}
}

func TestLoadIssuerAllowlistDeniesAllWhenUnsetOrEmpty(t *testing.T) {
	cases := []struct {
		name string
		vars map[string]string
	}{
		{"missing", map[string]string{}},
		{"empty", map[string]string{EnvIssuerCallers: ""}},
		{"whitespace", map[string]string{EnvIssuerCallers: "   "}},
		{"separators only", map[string]string{EnvIssuerCallers: " , , "}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given the variable is absent or carries no entries
			allow, err := LoadIssuerAllowlist(allowlistEnv(tc.vars))
			// Then loading succeeds with a deny-all mapping
			if err != nil {
				t.Fatalf("LoadIssuerAllowlist() error = %v, want nil", err)
			}
			if allow.PermitIssue(1) {
				t.Fatal("PermitIssue(1) = true, want false (deny-all)")
			}
		})
	}
}

func TestLoadIssuerAllowlistRejectsIllegalValues(t *testing.T) {
	// Given a value with any entry that is not a positive decimal caller_id
	illegal := []string{
		"abc", "7,abc", "0", "-1", "+7", "1.5", "0x7", "7;8",
		"9223372036854775808", // int64 overflow
	}
	for _, raw := range illegal {
		t.Run(raw, func(t *testing.T) {
			// When it is loaded
			allow, err := LoadIssuerAllowlist(allowlistEnv(map[string]string{
				EnvIssuerCallers: raw,
			}))
			// Then it is a startup configuration error naming the variable,
			// and no usable mapping is returned
			if err == nil {
				t.Fatalf("LoadIssuerAllowlist(%q) = nil, want startup error", raw)
			}
			if !strings.Contains(err.Error(), EnvIssuerCallers) {
				t.Errorf("error %q does not name %s", err, EnvIssuerCallers)
			}
			if allow != nil {
				t.Fatalf("LoadIssuerAllowlist(%q) = %+v with error, want nil", raw, allow)
			}
		})
	}
}

func TestLoadIssuerAllowlistNilGetterIsError(t *testing.T) {
	// A nil getter is a miswiring, not a deny-all configuration.
	if _, err := LoadIssuerAllowlist(nil); err == nil {
		t.Fatal("LoadIssuerAllowlist(nil) = nil, want error")
	}
}

func TestNilIssuerAllowlistDenies(t *testing.T) {
	// The predicate is nil-safe: a nil mapping denies rather than panicking.
	var allow *IssuerAllowlist
	if allow.PermitIssue(1) {
		t.Fatal("(*IssuerAllowlist)(nil).PermitIssue(1) = true, want false")
	}
}
