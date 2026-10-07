//go:build !perf

package app

import (
	"context"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/xtianxx/txharbor/internal/ratelimit"
)

// This file is the ordinary-build half of the serve-side segmented-measurement
// seam (frozen API; the perf-tagged half lives in perf_seam_perf.go and is
// compiled only under `-tags perf`). Every symbol is the identity: the three
// call points in serve.go / ratelimit_middleware.go resolve to zero-logic
// functions, so a production build keeps its exact behavior with no branch
// and no allocation.

// perfWrapServeHandler is the identity in an ordinary build.
func perfWrapServeHandler(next http.Handler) http.Handler { return next }

// perfRegisterServeResources does nothing in an ordinary build.
func perfRegisterServeResources(pool *pgxpool.Pool, limiter *redis.Client) {}

// admitWithPerf is exactly policy.Admit in an ordinary build.
func admitWithPerf(ctx context.Context, policy *ratelimit.Policy, class ratelimit.Class, r *http.Request) error {
	return policy.Admit(ctx, class)
}

// perfWrapBelowAdmit is the identity in an ordinary build.
func perfWrapBelowAdmit(next http.Handler) http.Handler { return next }
