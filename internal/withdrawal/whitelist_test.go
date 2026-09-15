// whitelist_test.go owns spec task T020 (007-withdrawal-creation): unit
// coverage for the FR-05 whitelist hook and the 004 policy-snapshot parser.
// No database is involved; the live reader (ResolveAssetAllowlist) is exercised
// against real PostgreSQL in whitelist_integration_test.go.
package withdrawal

import "testing"

const (
	whitelistMember = "0x1111111111111111111111111111111111111111"
	whitelistOther  = "0x2222222222222222222222222222222222222222"
)

func TestWhitelistHookEnforcement(t *testing.T) {
	t.Run("member is accepted", func(t *testing.T) {
		// Given an allowlist containing the asset
		// When the asset is validated
		// Then it is accepted
		if err := ValidateAssetWhitelisted(whitelistMember, []string{whitelistOther, whitelistMember}); err != nil {
			t.Fatalf("ValidateAssetWhitelisted(member) = %v, want nil", err)
		}
	})

	t.Run("non-member is rejected", func(t *testing.T) {
		requireValidationError(t, ValidateAssetWhitelisted(whitelistMember, []string{whitelistOther}), "asset")
	})

	t.Run("empty allowlist rejects", func(t *testing.T) {
		// Never a full-chain fallback: an empty source rejects every asset.
		requireValidationError(t, ValidateAssetWhitelisted(whitelistMember, nil), "asset")
		requireValidationError(t, ValidateAssetWhitelisted(whitelistMember, []string{}), "asset")
	})
}

func TestParseAssetAllowlist(t *testing.T) {
	t.Run("canonical snapshot is lowercased sorted and deduped", func(t *testing.T) {
		// Given a 004 assets snapshot where one address appears twice with
		// different effective heights and the lines are not sorted
		// When it is parsed
		// Then the canonical address set is lowercase, sorted and deduplicated
		got, err := parseAssetAllowlist(whitelistOther + ":10\n" + whitelistMember + ":12\n" + whitelistMember + ":3")
		if err != nil {
			t.Fatalf("parseAssetAllowlist = %v, want nil", err)
		}
		want := []string{whitelistMember, whitelistOther}
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("got %v, want %v", got, want)
			}
		}
	})

	t.Run("uppercase addresses are canonicalized", func(t *testing.T) {
		got, err := parseAssetAllowlist("0x111111111111111111111111111111111111111A:1")
		if err != nil {
			t.Fatalf("parseAssetAllowlist = %v, want nil", err)
		}
		if len(got) != 1 || got[0] != "0x111111111111111111111111111111111111111a" {
			t.Fatalf("got %v, want lowercase canonical address", got)
		}
	})

	t.Run("blank snapshot is an error", func(t *testing.T) {
		if _, err := parseAssetAllowlist(""); err == nil {
			t.Fatal("parseAssetAllowlist(blank) = nil, want error")
		}
		if _, err := parseAssetAllowlist("  \n "); err == nil {
			t.Fatal("parseAssetAllowlist(whitespace) = nil, want error")
		}
	})

	t.Run("malformed line is an error", func(t *testing.T) {
		if _, err := parseAssetAllowlist("not-an-address:1"); err == nil {
			t.Fatal("parseAssetAllowlist(malformed) = nil, want error")
		}
		if _, err := parseAssetAllowlist(whitelistMember); err == nil {
			t.Fatal("parseAssetAllowlist(no-effectiveness) = nil, want error")
		}
	})
}
