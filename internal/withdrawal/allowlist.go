// allowlist.go owns the issuance permission mapping (T005, R-PB9/R-PB10):
// the deployment allowlist of caller_ids permitted to issue upstream
// authorizations, loaded once at process start, plus the pure predicate the
// supply carrier (T013) and the in-tx authority re-check (T015) consult.
//
// The mapping lives in deployment config, never in code constants, and the
// predicates here never touch the database — permission is checked before the
// pool is used, so "can reach the DB" is not permission (R-PB1/R-PB2).
package withdrawal

import (
	"fmt"
	"strconv"
	"strings"
)

// EnvIssuerCallers is the deployment environment variable carrying the
// issuance allowlist: a comma-separated list of decimal caller_ids (R-PB10,
// contract supply-scope.md precondition 2). Whitespace around entries is
// trimmed and empty entries are ignored; missing or empty means deny-all
// (fail-closed). Any other entry shape is a startup configuration error.
const EnvIssuerCallers = "TXHARBOR_AUTHZ_ISSUER_CALLERS"

// IssuerAllowlist is the loaded issuance permission mapping. The zero value and
// a nil pointer both permit nothing, so a failed or absent load can never
// degrade into allow-all.
type IssuerAllowlist struct {
	callers map[int64]struct{}
}

// LoadIssuerAllowlist reads EnvIssuerCallers through getenv (os.LookupEnv
// compatible) and parses it. A missing or empty value yields a deny-all
// mapping and a nil error; a duplicate entry is harmless. Each remaining entry
// must be a positive decimal caller_id (the same positive-int64 domain
// IssueKey and the caller table enforce), so a sign, empty-and-not-trimmed
// token, non-digit, zero, or int64 overflow is a startup configuration error
// the CLI carrier reports with exit code 2. A nil getenv is a miswiring and
// returns an error rather than panicking.
//
// A non-nil error means the process must not start: the returned mapping is
// nil and MUST NOT be used.
func LoadIssuerAllowlist(getenv func(string) (string, bool)) (*IssuerAllowlist, error) {
	if getenv == nil {
		return nil, fmt.Errorf("%s: no environment getter", EnvIssuerCallers)
	}
	raw, _ := getenv(EnvIssuerCallers)
	allow := &IssuerAllowlist{callers: make(map[int64]struct{})}
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			// Missing, empty, and all-empty values all fall through to the
			// empty mapping: deny-all, never allow-all.
			continue
		}
		id, err := strconv.ParseUint(entry, 10, 63)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("%s: %q is not a positive decimal caller_id", EnvIssuerCallers, entry)
		}
		allow.callers[int64(id)] = struct{}{}
	}
	return allow, nil
}

// PermitIssue reports whether callerID may issue an upstream authorization.
// It is pure (no database, no environment, no caching) and nil-safe: a nil
// allowlist, like an unmapped caller, always denies. The caller_id is matched
// by exact integer value against the loaded mapping.
func (a *IssuerAllowlist) PermitIssue(callerID int64) bool {
	if a == nil {
		return false
	}
	_, ok := a.callers[callerID]
	return ok
}
