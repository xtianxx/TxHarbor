//go:build perf

package app

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/ratelimit"
)

// This file is the perf-tagged half of the serve-side segmented-measurement
// seam (frozen API; the ordinary build resolves every symbol here to the
// no-op in perf_seam_none.go). It compiles only under `-tags perf` and stays
// inert until PerfEnableSeg installs a sink, so even a perf-tagged binary
// behaves like production until the harness arms it.
//
// Timing discipline (frozen): every request under /withdrawals is minted one
// id (echoed in the X-TXHarbor-Perf-Id response header and carried in the
// request context) and observed as four nested segments — server_total ⊇
// {admit ⊕ below_admit ⊕ wrapper overhead}; below_admit covers the handler
// behind the admission decision. Segments are start-timestamped, so
// [at, at+dur] is the measured interval.
//
// State discipline: the sink and the query-limiter switch live in one
// immutable snapshot published by an atomic pointer, so every call point
// reads exactly one atomic and a request sees one consistent pair. The
// limiter switch is deliberately independent of collection: an uncollected
// arm (collect=false, no sink) still measures its limiter condition — it only
// writes no records.

// perfIDHeader is the response header that echoes the minted per-request id
// (the harness reads it next to its own copy of the frozen name).
const perfIDHeader = "X-TXHarbor-Perf-Id"

// perfSegPathPrefix scopes the serve wrapper: only the withdrawal surface is
// instrumented, every other path passes through untouched.
const perfSegPathPrefix = "/withdrawals"

// PerfSegSink receives one finished segment. Implementations must be safe for
// concurrent use: one call happens per segment of every instrumented request.
type PerfSegSink interface {
	Segment(id uint64, seg, class string, dur time.Duration, at time.Time, method, path string)
}

// perfSeamState is the immutable snapshot published by every knob change: the
// armed sink (nil = collection off) and the query-limiter switch (true =
// production behavior).
type perfSeamState struct {
	sink           PerfSegSink
	queryLimiterOn bool
}

var (
	// perfSeamPtr publishes the current perfSeamState.
	perfSeamPtr atomic.Pointer[perfSeamState]
	// perfSegIDCounter mints the per-request ids. It is monotonic across
	// arms, so ids stay unique process-wide and a client sample's echoed id
	// joins to its segments unambiguously.
	perfSegIDCounter atomic.Uint64
	// perfServePoolPtr / perfLimiterClientPtr are the serve resources the
	// harness reads pool statistics from (limiter nil when 013 is off).
	perfServePoolPtr     atomic.Pointer[pgxpool.Pool]
	perfLimiterClientPtr atomic.Pointer[redis.Client]
)

func init() {
	perfSeamPtr.Store(&perfSeamState{queryLimiterOn: true})
}

// perfSeamSnapshot returns the current state with one atomic read; callers
// hold it for the whole request so a knob change never splits a request.
func perfSeamSnapshot() *perfSeamState { return perfSeamPtr.Load() }

// perfSeamMutate publishes a copy of the current state with mutate applied.
// The copy-on-write CAS keeps concurrent knob changes race-free; knobs change
// between measurement windows, never per request.
func perfSeamMutate(mutate func(*perfSeamState)) {
	for {
		current := perfSeamPtr.Load()
		next := &perfSeamState{sink: current.sink, queryLimiterOn: current.queryLimiterOn}
		mutate(next)
		if perfSeamPtr.CompareAndSwap(current, next) {
			return
		}
	}
}

// PerfEnableSeg arms collection with sink; a nil sink disarms it (equivalent
// to PerfDisableSeg).
func PerfEnableSeg(sink PerfSegSink) {
	if sink == nil {
		PerfDisableSeg()
		return
	}
	perfSeamMutate(func(state *perfSeamState) { state.sink = sink })
}

// PerfDisableSeg disarms collection (the hot path returns to a single atomic
// read per call point and no record is written).
func PerfDisableSeg() {
	perfSeamMutate(func(state *perfSeamState) { state.sink = nil })
}

// PerfSetQueryLimiterEnabled selects whether the ClassQuery admission is
// consulted (true, the default) or skipped (false: no limiter call, and with
// collection armed an admit_skipped segment is recorded in its place). Every
// other class is untouched either way, and the switch applies whether or not
// collection is armed.
func PerfSetQueryLimiterEnabled(enabled bool) {
	perfSeamMutate(func(state *perfSeamState) { state.queryLimiterOn = enabled })
}

// perfRegisterServeResources saves the serve pool and the limiter's dedicated
// Redis client (nil when the 013 wiring is off) for the harness to read pool
// statistics from.
func perfRegisterServeResources(pool *pgxpool.Pool, limiter *redis.Client) {
	perfServePoolPtr.Store(pool)
	perfLimiterClientPtr.Store(limiter)
}

// PerfServePool returns the saved serve pool (nil before serve registers it).
func PerfServePool() *pgxpool.Pool { return perfServePoolPtr.Load() }

// PerfLimiterClient returns the saved limiter Redis client (nil when the 013
// wiring is off or before serve registers it).
func PerfLimiterClient() *redis.Client { return perfLimiterClientPtr.Load() }

// perfRequestClass derives the frozen class label of an instrumented request
// by the same method rule as the limiter middleware on these routes: GET/HEAD
// is a query, any other method on the withdrawal collection is new withdrawal
// creation. Every other shape (e.g. the execution route's write class)
// carries the empty label.
func perfRequestClass(r *http.Request) string {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return "query"
	}
	if r.URL.Path == "/withdrawals" || r.URL.Path == "/withdrawals/" {
		return "new_withdrawal"
	}
	return ""
}

// perfWrapServeHandler instruments the serve mux. Disarmed (nil sink) or on a
// non-withdrawal path it is a pass-through after one atomic read; armed it
// mints the request id, echoes it, carries it in the context and records
// server_total around the rest of the chain.
func perfWrapServeHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := perfSeamSnapshot()
		if state.sink == nil || !strings.HasPrefix(r.URL.Path, perfSegPathPrefix) {
			next.ServeHTTP(w, r)
			return
		}
		id := perfSegIDCounter.Add(1)
		w.Header().Set(perfIDHeader, strconv.FormatUint(id, 10))
		ctx := db.PerfWithID(r.Context(), id)
		start := time.Now()
		next.ServeHTTP(w, r.WithContext(ctx))
		state.sink.Segment(id, "server_total", perfRequestClass(r), time.Since(start), start, r.Method, r.URL.Path)
	})
}

// admitWithPerf wraps every limiter admission. With the query limiter off a
// query class is admitted without calling the policy (an armed, id-carrying
// request records admit_skipped with dur=0); every other case runs exactly
// policy.Admit and records admit when collection is armed and the context
// carries an id. Every other class keeps its exact semantics in both modes.
func admitWithPerf(ctx context.Context, policy *ratelimit.Policy, class ratelimit.Class, r *http.Request) error {
	state := perfSeamSnapshot()
	if class == ratelimit.ClassQuery && !state.queryLimiterOn {
		if state.sink != nil {
			if id, hasID := db.PerfIDFromContext(ctx); hasID {
				state.sink.Segment(id, "admit_skipped", perfRequestClass(r), 0, time.Now(), r.Method, r.URL.Path)
			}
		}
		return nil
	}
	if state.sink == nil {
		return policy.Admit(ctx, class)
	}
	id, hasID := db.PerfIDFromContext(ctx)
	if !hasID {
		return policy.Admit(ctx, class)
	}
	start := time.Now()
	err := policy.Admit(ctx, class)
	state.sink.Segment(id, "admit", perfRequestClass(r), time.Since(start), start, r.Method, r.URL.Path)
	return err
}

// perfWrapBelowAdmit instruments the handler behind the admission decision:
// disarmed or id-less it is a pass-through after one atomic read, otherwise
// it records below_admit (auth, query work and response writing) around the
// inner handler.
func perfWrapBelowAdmit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := perfSeamSnapshot()
		if state.sink == nil {
			next.ServeHTTP(w, r)
			return
		}
		id, hasID := db.PerfIDFromContext(r.Context())
		if !hasID {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		next.ServeHTTP(w, r)
		state.sink.Segment(id, "below_admit", perfRequestClass(r), time.Since(start), start, r.Method, r.URL.Path)
	})
}
