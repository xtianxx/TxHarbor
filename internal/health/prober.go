package health

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/xtianxx/txharbor/internal/eth"
)

// Result is the outcome of one named readiness check.
type Result struct {
	Name string
	Err  error
}

// ProbeFunc runs one dependency probe and returns a result per affected check.
type ProbeFunc func(ctx context.Context) []Result

// Pinger is the DB pool subset the prober needs (pgxpool.Pool satisfies it).
type Pinger interface {
	Ping(ctx context.Context) error
}

// DBProber probes PostgreSQL connectivity.
func DBProber(pool Pinger) ProbeFunc {
	return func(ctx context.Context) []Result {
		return []Result{{Name: "db", Err: pool.Ping(ctx)}}
	}
}

// RPCProber probes RPC connectivity and chain-id agreement in one call.
func RPCProber(client *eth.Client, expected *big.Int) ProbeFunc {
	return func(ctx context.Context) []Result {
		id, err := client.ChainID(ctx)
		switch {
		case err != nil:
			return []Result{{Name: "rpc", Err: err}, {Name: "chain", Err: err}}
		case expected != nil && id.Cmp(expected) != 0:
			mismatch := &eth.Error{
				Kind: eth.KindChainMismatch,
				Op:   "eth_chainId",
				Err:  fmt.Errorf("expected %s, actual %s", expected.String(), id.String()),
			}
			return []Result{{Name: "rpc"}, {Name: "chain", Err: mismatch}}
		default:
			return []Result{{Name: "rpc"}, {Name: "chain"}}
		}
	}
}

// Check pairs a probe function with a display name.
type Check struct {
	Name  string
	Probe ProbeFunc
}

// Runner probes dependencies on a fixed interval with a per-probe timeout and
// mirrors every result into the aggregate and the metrics hook.
type Runner struct {
	Interval time.Duration
	Timeout  time.Duration
	Checks   []Check
	Agg      *Aggregate
	Observe  func(dep string, ok bool)
}

// Run probes immediately, then on every tick until ctx is done.
func (r *Runner) Run(ctx context.Context) {
	r.ProbeOnce(ctx)
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.ProbeOnce(ctx)
		}
	}
}

// ProbeOnce runs every check exactly once with its own timeout.
func (r *Runner) ProbeOnce(ctx context.Context) {
	for _, check := range r.Checks {
		probeCtx, cancel := context.WithTimeout(ctx, r.Timeout)
		results := check.Probe(probeCtx)
		cancel()
		if len(results) == 0 {
			results = []Result{{Name: check.Name, Err: fmt.Errorf("probe %s returned no result", check.Name)}}
		}
		for _, res := range results {
			r.Agg.Set(res.Name, res.Err)
			if r.Observe != nil {
				r.Observe(res.Name, res.Err == nil)
			}
		}
	}
}
