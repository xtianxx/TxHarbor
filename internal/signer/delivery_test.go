package signer

import "testing"

func TestScopeVersionClass(t *testing.T) {
	persisted := int64(1)
	cases := []struct {
		name      string
		persisted *int64
		scope     GrantScope
		want      RefusalClass
	}{
		{"no snapshot and no scope (pre-extension stock)", nil, GrantScope{}, ""},
		{"snapshot equals the live scope version", &persisted, GrantScope{Present: true, AuthorizationVersion: 1}, ""},
		{"bumped scope version", &persisted, GrantScope{Present: true, AuthorizationVersion: 2}, ClassAuthorizationInvalid},
		{"scope vanished after a snapshot", &persisted, GrantScope{}, ClassAuthorizationInvalid},
		{"scope appeared against a NULL snapshot", nil, GrantScope{Present: true, AuthorizationVersion: 1}, ClassAuthorizationInvalid},
	}
	for _, tc := range cases {
		if got := scopeVersionClass(tc.persisted, tc.scope); got != tc.want {
			t.Fatalf("scopeVersionClass(%s) = %q, want %q", tc.name, got, tc.want)
		}
	}
}
