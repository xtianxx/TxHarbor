package signer

import "testing"

// TestErrors pins every machine class string of contracts/persistence.md §4
// and its retryability. It fails on a rename, a dropped class, a class without
// a retryability mapping, or retryability drift.
func TestErrors(t *testing.T) {
	cases := []struct {
		class RefusalClass
		want  string
		retry Retryability
	}{
		{ClassUnauthenticated, "unauthenticated", RetryNever},
		{ClassSigningNotPermitted, "signing_not_permitted", RetryNever},
		{ClassMalformedRequest, "malformed_request", RetryNever},
		{ClassArbitraryDigestRejected, "arbitrary_digest_rejected", RetryNever},
		{ClassValidationFailed, "validation_failed", RetryNever},
		{ClassPolicyRefused, "policy_refused", RetryNever},
		{ClassRequestConflict, "request_conflict", RetryNever},
		{ClassBindingAbsent, "binding_absent", RetryNever},
		{ClassBindingConflict, "binding_conflict", RetryNever},
		{ClassBindingPaused, "binding_paused", RetryAfterRelease},
		{ClassBindingTerminal, "binding_terminal", RetryNever},
		{ClassBindingReadFailed, "binding_read_failed", RetryNow},
		{ClassAuthorizationInvalid, "authorization_invalid", RetryNever},
		{ClassAuthorizationExpired, "authorization_expired", RetryNever},
		{ClassAuthorizationRevoked, "authorization_revoked", RetryNever},
		{ClassAuthorizationUnverifiable, "authorization_unverifiable", RetryNever},
		{ClassRecoveryPaused, "recovery_paused", RetryAfterRelease},
		{ClassRecoveryActive, "recovery_active", RetryAfterRelease},
		{ClassRecoveryVersionChanged, "recovery_version_changed", RetryNever},
		{ClassGateReadFailed, "gate_read_failed", RetryNow},
		{ClassKeyProviderUnavailable, "key_provider_unavailable", RetryNow},
		{ClassKeyProviderTimeout, "key_provider_timeout", RetryNow},
		{ClassStorageUnavailable, "storage_unavailable", RetryNow},
		{ClassOutcomeNotYetVisible, "outcome_not_yet_visible", RetryNow},
		{ClassSignatureWithheld, "signature_withheld", RetryAfterRelease},
		{ClassOutcomeUnknown, "outcome_unknown", RetryReconcile},
	}

	seen := make(map[RefusalClass]bool, len(cases))
	for _, tc := range cases {
		if seen[tc.class] {
			t.Errorf("duplicate class %q", tc.class)
		}
		seen[tc.class] = true
		if got := string(tc.class); got != tc.want {
			t.Errorf("class string = %q, want %q", got, tc.want)
		}
		if got := RetryabilityOf(tc.class); got != tc.retry {
			t.Errorf("RetryabilityOf(%q) = %d, want %d", tc.class, got, tc.retry)
		}
	}
	if len(retryabilityByClass) != len(cases) {
		t.Fatalf("retryability map has %d entries, want %d (an unmapped class fails closed to RetryNever)",
			len(retryabilityByClass), len(cases))
	}
	if got := RetryabilityOf("not_a_class"); got != RetryNever {
		t.Errorf("RetryabilityOf(unknown) = %d, want RetryNever", got)
	}
}
