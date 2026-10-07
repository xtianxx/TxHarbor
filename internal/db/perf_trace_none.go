//go:build !perf

package db

import "github.com/jackc/pgx/v5/pgxpool"

// perfAttachQueryTracer is the ordinary-build half of the perf query-trace
// seam (frozen API): nothing is attached and OpenPool keeps its exact
// pre-seam behavior. The perf-tagged half lives in perf_trace_perf.go and is
// compiled only under `-tags perf` (make test-perf).
func perfAttachQueryTracer(cfg *pgxpool.Config) {}
