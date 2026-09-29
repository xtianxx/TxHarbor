package recoveryadmin

import (
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
)

func TestRecoveryOpEntryChainsRequiresRuntimeMembership(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{
			name: "valid single runtime in complete inventory",
			env:  map[string]string{config.EnvRecoveryEntryChains: "1,10", config.EnvChainID: "10"},
		},
		{
			name: "missing runtime chain",
			env:  map[string]string{config.EnvRecoveryEntryChains: "1,10"},
			want: config.EnvChainID,
		},
		{
			name: "runtime omitted from inventory",
			env:  map[string]string{config.EnvRecoveryEntryChains: "1,10", config.EnvChainID: "2"},
			want: "not in the trusted deployment",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := Deps{Getenv: func(key string) (string, bool) { v, ok := tc.env[key]; return v, ok }}
			chains, err := recoveryOpEntryChains(d, "test")
			if tc.want != "" {
				if err == nil || !contains(err.Error(), tc.want) {
					t.Fatalf("recoveryOpEntryChains error = %v, want substring %q", err, tc.want)
				}
				return
			}
			if err != nil || len(chains) != 2 || chains[0] != 1 || chains[1] != 10 {
				t.Fatalf("recoveryOpEntryChains = %v, %v; want [1 10], nil", chains, err)
			}
		})
	}
}

func contains(value, sub string) bool {
	for i := 0; i+len(sub) <= len(value); i++ {
		if value[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
