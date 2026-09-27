// authz_range_test.go is the directed positive/negative layer for the T009
// scope-range evaluation (authz.go): authzRangeCovered and AuthScope.Covers,
// plus the same semantics as observed through Evaluate (default deny). It pins
// the range rules of contracts/auth-matrix.md and data-model.md §1.10:
//
//   - a range-unbounded grant (both bounds absent) covers a request with no
//     stated range (a time-scoped task legitimately carries no height range)
//     and every bounded request;
//   - a bounded grant only ever covers a bounded request fully inside it and
//     never an unbounded request; partial overlap, out-of-bounds and
//     cross-chain requests are covered by nothing;
//   - inverted grant bounds never match, and an inverted request is refused as
//     an invalid scope before any grant lookup;
//   - chain, kind and business-type isolation are all part of coverage, not
//     just the numeric range.
//
// The file is intentionally untagged so the ordinary PR unit path (make test)
// and both vet invocations compile and run it.
package reconciliation

import (
	"strconv"
	"testing"
)

// authzBound points at v for the *int64 range-bound API.
func authzBound(v int64) *int64 { return &v }

func TestAuthzRangeCoveredDirectedCases(t *testing.T) {
	cases := []struct {
		name         string
		gStart, gEnd *int64
		rStart, rEnd *int64
		want         bool
	}{
		{
			name: "unbounded grant covers absent request range",
			want: true,
		},
		{
			name:   "unbounded grant covers bounded request",
			rStart: authzBound(100), rEnd: authzBound(200),
			want: true,
		},
		{
			name:   "bounded grant contains bounded request",
			gStart: authzBound(100), gEnd: authzBound(200),
			rStart: authzBound(150), rEnd: authzBound(175),
			want: true,
		},
		{
			name:   "bounded grant covers exact request bounds",
			gStart: authzBound(100), gEnd: authzBound(200),
			rStart: authzBound(100), rEnd: authzBound(200),
			want: true,
		},
		{
			name:   "prefix grant covers a suffix request",
			gStart: authzBound(100),
			rStart: authzBound(150), rEnd: authzBound(300),
			want: true,
		},
		{
			name:   "suffix grant covers a prefix request",
			gEnd:   authzBound(200),
			rStart: authzBound(50), rEnd: authzBound(150),
			want: true,
		},
		{
			name:   "bounded grant cannot cover unbounded request",
			gStart: authzBound(100), gEnd: authzBound(200),
			want: false,
		},
		{
			name:   "partial overlap is not coverage",
			gStart: authzBound(100), gEnd: authzBound(200),
			rStart: authzBound(150), rEnd: authzBound(250),
			want: false,
		},
		{
			name:   "request below the grant start is out of range",
			gStart: authzBound(100), gEnd: authzBound(200),
			rStart: authzBound(50), rEnd: authzBound(150),
			want: false,
		},
		{
			name:   "request above the grant end is out of range",
			gStart: authzBound(100), gEnd: authzBound(200),
			rStart: authzBound(175), rEnd: authzBound(250),
			want: false,
		},
		{
			name:   "request before a one-sided prefix grant start",
			gStart: authzBound(150),
			rStart: authzBound(100), rEnd: authzBound(200),
			want: false,
		},
		{
			name:   "request past a one-sided suffix grant end",
			gEnd:   authzBound(150),
			rStart: authzBound(100), rEnd: authzBound(175),
			want: false,
		},
		{
			name:   "inverted grant bounds never match",
			gStart: authzBound(200), gEnd: authzBound(100),
			rStart: authzBound(150), rEnd: authzBound(175),
			want: false,
		},
		{
			name:   "inverted request bounds never match",
			gStart: authzBound(100), gEnd: authzBound(200),
			rStart: authzBound(175), rEnd: authzBound(150),
			want: false,
		},
		{
			name:   "unbounded grant rejects an inverted request",
			rStart: authzBound(200), rEnd: authzBound(100),
			want: false,
		},
		{
			name:   "point request on the grant boundaries",
			gStart: authzBound(100), gEnd: authzBound(200),
			rStart: authzBound(100), rEnd: authzBound(100),
			want: true,
		},
		{
			name:   "point request past the grant end",
			gStart: authzBound(100), gEnd: authzBound(200),
			rStart: authzBound(201), rEnd: authzBound(201),
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := authzRangeCovered(tc.gStart, tc.gEnd, tc.rStart, tc.rEnd); got != tc.want {
				t.Fatalf("authzRangeCovered(grant %v..%v, request %v..%v) = %v, want %v",
					authzRangeRef(tc.gStart), authzRangeRef(tc.gEnd),
					authzRangeRef(tc.rStart), authzRangeRef(tc.rEnd), got, tc.want)
			}
		})
	}
}

// authzRangeRef renders a nullable bound for failure messages.
func authzRangeRef(v *int64) string {
	if v == nil {
		return "unbounded"
	}
	return strconv.FormatInt(*v, 10)
}

func TestAuthzScopeCoversIsolatesChainKindAndBusinessTypes(t *testing.T) {
	withdrawal := []BusinessType{BusinessWithdrawal}
	both := []BusinessType{BusinessWithdrawal, BusinessDeposit}

	cases := []struct {
		name    string
		grant   AuthScope
		request AuthScope
		want    bool
	}{
		{
			name:    "range-unbounded grant covers an unbounded request",
			grant:   AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal},
			request: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal},
			want:    true,
		},
		{
			name:  "range-unbounded grant covers a bounded request",
			grant: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal},
			request: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal,
				RangeStart: authzBound(10), RangeEnd: authzBound(20)},
			want: true,
		},
		{
			name: "bounded grant contains an inner request",
			grant: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal,
				RangeStart: authzBound(10), RangeEnd: authzBound(20)},
			request: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal,
				RangeStart: authzBound(12), RangeEnd: authzBound(18)},
			want: true,
		},
		{
			name: "bounded grant never covers an unbounded request",
			grant: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal,
				RangeStart: authzBound(10), RangeEnd: authzBound(20)},
			request: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal},
			want:    false,
		},
		{
			name: "partial overlap is not coverage",
			grant: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal,
				RangeStart: authzBound(10), RangeEnd: authzBound(20)},
			request: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal,
				RangeStart: authzBound(15), RangeEnd: authzBound(25)},
			want: false,
		},
		{
			name:    "cross-chain grant does not cover",
			grant:   AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal},
			request: AuthScope{ChainID: "2", Kind: ScopeHeight, BusinessTypes: withdrawal},
			want:    false,
		},
		{
			name:    "empty grant chain never covers",
			grant:   AuthScope{Kind: ScopeHeight, BusinessTypes: withdrawal},
			request: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal},
			want:    false,
		},
		{
			name:    "scope kind mismatch does not cover",
			grant:   AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal},
			request: AuthScope{ChainID: "1", Kind: ScopeTime, BusinessTypes: withdrawal},
			want:    false,
		},
		{
			name:    "unknown grant kind never covers",
			grant:   AuthScope{ChainID: "1", Kind: ScopeKind("epoch"), BusinessTypes: withdrawal},
			request: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal},
			want:    false,
		},
		{
			name:    "missing request business types never covered",
			grant:   AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal},
			request: AuthScope{ChainID: "1", Kind: ScopeHeight},
			want:    false,
		},
		{
			name:  "unrequested business type is never covered",
			grant: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal},
			request: AuthScope{ChainID: "1", Kind: ScopeHeight,
				BusinessTypes: []BusinessType{BusinessDeposit}},
			want: false,
		},
		{
			name:  "request must be inside the granted business-type set",
			grant: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal},
			request: AuthScope{ChainID: "1", Kind: ScopeHeight,
				BusinessTypes: both},
			want: false,
		},
		{
			name:  "superset grant covers a business-type subset",
			grant: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: both},
			request: AuthScope{ChainID: "1", Kind: ScopeHeight,
				BusinessTypes: withdrawal},
			want: true,
		},
		{
			name: "unknown grant business type never covers",
			grant: AuthScope{ChainID: "1", Kind: ScopeHeight,
				BusinessTypes: []BusinessType{BusinessType("ledger")}},
			request: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal},
			want:    false,
		},
		{
			name:  "unknown request business type never covered",
			grant: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal},
			request: AuthScope{ChainID: "1", Kind: ScopeHeight,
				BusinessTypes: []BusinessType{BusinessType("ledger")}},
			want: false,
		},
		{
			name: "inverted grant bounds never cover",
			grant: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal,
				RangeStart: authzBound(20), RangeEnd: authzBound(10)},
			request: AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal,
				RangeStart: authzBound(12), RangeEnd: authzBound(18)},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.grant.Covers(tc.request); got != tc.want {
				t.Fatalf("grant %+v Covers request %+v = %v, want %v", tc.grant, tc.request, got, tc.want)
			}
		})
	}
}

func TestAuthzEvaluateRangeCoverage(t *testing.T) {
	principal, err := APIKeyPrincipal(42)
	if err != nil {
		t.Fatalf("APIKeyPrincipal: %v", err)
	}
	withdrawal := []BusinessType{BusinessWithdrawal}

	grantUnbounded := AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal}
	grantBounded := AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal,
		RangeStart: authzBound(100), RangeEnd: authzBound(200)}
	grantCrossChain := AuthScope{ChainID: "2", Kind: ScopeHeight, BusinessTypes: withdrawal}
	grantInverted := AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal,
		RangeStart: authzBound(200), RangeEnd: authzBound(100)}
	grantTimeUnbounded := AuthScope{ChainID: "1", Kind: ScopeTime, BusinessTypes: withdrawal}

	requestBounded := AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal,
		RangeStart: authzBound(150), RangeEnd: authzBound(175)}
	requestPartial := AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal,
		RangeStart: authzBound(150), RangeEnd: authzBound(250)}
	requestUnbounded := AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal}
	requestCrossChain := AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal}
	requestTime := AuthScope{ChainID: "1", Kind: ScopeTime, BusinessTypes: withdrawal}
	requestInverted := AuthScope{ChainID: "1", Kind: ScopeHeight, BusinessTypes: withdrawal,
		RangeStart: authzBound(175), RangeEnd: authzBound(150)}

	cases := []struct {
		name    string
		grant   AuthScope
		request AuthScope
		allowed bool
		reason  DenyReason
	}{
		{"unbounded grant covers a bounded request", grantUnbounded, requestBounded, true, ""},
		{"bounded grant covers a contained request", grantBounded, requestBounded, true, ""},
		{"partial overlap is denied out of scope", grantBounded, requestPartial, false, DenyOutOfScope},
		{"bounded grant cannot cover an unbounded request", grantBounded, requestUnbounded, false, DenyOutOfScope},
		{"cross-chain grant is denied out of scope", grantCrossChain, requestCrossChain, false, DenyOutOfScope},
		{"inverted grant bounds are denied out of scope", grantInverted, requestBounded, false, DenyOutOfScope},
		{"inverted request is an invalid scope", grantUnbounded, requestInverted, false, DenyInvalidScope},
		{"grant kind mismatch is denied out of scope", grantUnbounded, requestTime, false, DenyOutOfScope},
		{"unbounded time grant covers an absent range request", grantTimeUnbounded, requestTime, true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			grants := []Grant{{
				Principal:  principal.String(),
				Permission: PermissionScanManage,
				Scope:      tc.grant,
			}}
			decision := Evaluate(principal, ActionScanStart, tc.request, grants)
			if decision.Allowed != tc.allowed {
				t.Fatalf("Evaluate allowed = %v (reason %q, detail %q), want %v",
					decision.Allowed, decision.Reason, decision.Detail, tc.allowed)
			}
			if !tc.allowed && decision.Reason != tc.reason {
				t.Fatalf("Evaluate reason = %q, want %q", decision.Reason, tc.reason)
			}
			if tc.allowed && decision.Reason != "" {
				t.Fatalf("allowed decision carries refusal reason %q", decision.Reason)
			}
		})
	}
}
