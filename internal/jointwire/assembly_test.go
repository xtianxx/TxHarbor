package jointwire

import (
	"context"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
)

// TestWorkerRefusesMissingDependenciesByName pins the fail-loud assembly
// contract: every missing required piece is refused with the environment
// variable named, before any participant is returned.
func TestWorkerRefusesMissingDependenciesByName(t *testing.T) {
	mutate := func(apply func(*config.Config)) *config.Config {
		cfg := &config.Config{
			RPCURL:             "http://127.0.0.1:8545",
			TxSignerURL:        "http://127.0.0.1:8091",
			TxSignerCredential: "credential",
		}
		apply(cfg)
		return cfg
	}
	cases := []struct {
		name string
		cfg  *config.Config
		want string
	}{
		{name: "nil configuration", cfg: nil, want: "configuration is required"},
		{name: "missing rpc url", cfg: mutate(func(c *config.Config) { c.RPCURL = "" }), want: config.EnvRPCURL},
		{name: "missing signer url", cfg: mutate(func(c *config.Config) { c.TxSignerURL = "" }), want: config.EnvTxSignerURL},
		{name: "missing signer credential", cfg: mutate(func(c *config.Config) { c.TxSignerCredential = "" }), want: config.EnvTxSignerCredential},
		{name: "missing pool", cfg: mutate(func(*config.Config) {}), want: "database pool is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Worker(context.Background(), tc.cfg, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Worker error = %v, want it to name %q", err, tc.want)
			}
		})
	}
}
