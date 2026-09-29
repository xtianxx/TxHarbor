package config

import "testing"

func TestParseRecoveryEntryChains(t *testing.T) {
	for _, raw := range []string{"1", "1,10,31337"} {
		if chains, err := ParseRecoveryEntryChains(raw); err != nil || len(chains) == 0 {
			t.Fatalf("ParseRecoveryEntryChains(%q) = %v, %v", raw, chains, err)
		}
	}
	for _, raw := range []string{"", " 1", "1,", "1,1", "2,1", "0", "+1", "01", "1, 2"} {
		if _, err := ParseRecoveryEntryChains(raw); err == nil {
			t.Errorf("ParseRecoveryEntryChains(%q) unexpectedly succeeded", raw)
		}
	}
}
