//go:build perf

package app

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/ratelimit"
)

// This file tests the perf-tagged serve-side seam offline (no Docker, no
// testutil): the wrap mints/echoes/injects the per-request id and records
// server_total, the admission seam skips ClassQuery without consulting the
// policy when the query limiter is disabled, below_admit is recorded only for
// id-carrying requests, and 50 concurrent requests record without races
// (`make test-perf` runs under -race; go test -tags perf -race
// ./internal/app runs these four).

// perfSeamSeg is one captured Segment call.
type perfSeamSeg struct {
	id     uint64
	seg    string
	class  string
	dur    time.Duration
	at     time.Time
	method string
	path   string
}

// perfSeamSink records every Segment call; it is the seam's concurrency
// contract in test form (one call per segment of every instrumented request).
type perfSeamSink struct {
	mu   sync.Mutex
	segs []perfSeamSeg
}

func (s *perfSeamSink) Segment(id uint64, seg, class string, dur time.Duration, at time.Time, method, path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.segs = append(s.segs, perfSeamSeg{id: id, seg: seg, class: class, dur: dur, at: at, method: method, path: path})
}

func (s *perfSeamSink) snapshot() []perfSeamSeg {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]perfSeamSeg, len(s.segs))
	copy(out, s.segs)
	return out
}

func (s *perfSeamSink) bySeg(seg string) []perfSeamSeg {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []perfSeamSeg
	for _, rec := range s.segs {
		if rec.seg == seg {
			out = append(out, rec)
		}
	}
	return out
}

// perfSeamArm arms the seam and pins the test teardown: the sink is disarmed
// and the query limiter returns to its default (enabled) state.
func perfSeamArm(t *testing.T, sink *perfSeamSink) {
	t.Helper()
	PerfEnableSeg(sink)
	t.Cleanup(func() {
		PerfDisableSeg()
		PerfSetQueryLimiterEnabled(true)
	})
}

// perfSeamCheckTiming asserts the record's interval is plausible against the
// wall-clock bounds the test observed around the request.
func perfSeamCheckTiming(t *testing.T, seg perfSeamSeg, before, after time.Time) {
	t.Helper()
	if seg.dur < 0 {
		t.Fatalf("%s dur = %s, want >= 0", seg.seg, seg.dur)
	}
	if seg.at.Before(before) || seg.at.After(after) {
		t.Fatalf("%s at = %s outside request window [%s, %s]", seg.seg, seg.at, before, after)
	}
	if end := seg.at.Add(seg.dur); end.After(after.Add(time.Second)) {
		t.Fatalf("%s interval end = %s implausibly past request end %s", seg.seg, end, after)
	}
}

// perfSeamScriptStore is the offline ScriptStore stub: every Eval is counted
// and answered with a trusted allow, so the policy never touches Redis.
type perfSeamScriptStore struct {
	mu sync.Mutex
	n  int
}

func (s *perfSeamScriptStore) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	s.mu.Lock()
	s.n++
	s.mu.Unlock()
	return []any{int64(1), int64(1), int64(0)}, nil
}

func (s *perfSeamScriptStore) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

// perfSeamPolicy builds a policy over the counting stub.
func perfSeamPolicy(t *testing.T) (*ratelimit.Policy, *perfSeamScriptStore) {
	t.Helper()
	store := &perfSeamScriptStore{}
	classes := map[ratelimit.Class]ratelimit.ClassConfig{}
	for _, class := range []ratelimit.Class{ratelimit.ClassQuery, ratelimit.ClassWrite, ratelimit.ClassNewWithdrawal} {
		classes[class] = ratelimit.ClassConfig{RatePerSecond: 100, Burst: 50}
	}
	limiter, err := ratelimit.NewLimiter(store, ratelimit.Config{
		Classes:        classes,
		Timeout:        100 * time.Millisecond,
		BucketTTL:      200 * time.Millisecond,
		RecoveryWindow: time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	policy, err := ratelimit.NewPolicy(limiter)
	if err != nil {
		t.Fatalf("NewPolicy: %v", err)
	}
	return policy, store
}

// TestPerfSeamServeHandlerMintsAndRecords covers the serve wrapper: id
// minting, header echo, context injection, server_total recording around the
// nested below_admit segment, the non-withdrawal pass-through and the
// disarmed pass-through.
func TestPerfSeamServeHandlerMintsAndRecords(t *testing.T) {
	sink := &perfSeamSink{}
	perfSeamArm(t, sink)

	var ctxID uint64
	var ctxOK bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctxID, ctxOK = db.PerfIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	handler := perfWrapServeHandler(perfWrapBelowAdmit(inner))

	before := time.Now()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/withdrawals/req-1", nil))
	after := time.Now()

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !ctxOK {
		t.Fatal("handler context carries no perf id")
	}
	header := rec.Header().Get(perfIDHeader)
	if header != strconv.FormatUint(ctxID, 10) {
		t.Fatalf("response %s = %q, want minted id %d", perfIDHeader, header, ctxID)
	}

	segs := sink.snapshot()
	if len(segs) != 2 {
		t.Fatalf("segments = %+v, want server_total + below_admit", segs)
	}
	var total, below *perfSeamSeg
	for i := range segs {
		switch segs[i].seg {
		case "server_total":
			total = &segs[i]
		case "below_admit":
			below = &segs[i]
		}
	}
	if total == nil || below == nil {
		t.Fatalf("segments = %+v, want one server_total and one below_admit", segs)
	}
	for _, seg := range []perfSeamSeg{*total, *below} {
		if seg.id != ctxID {
			t.Fatalf("%s id = %d, want %d", seg.seg, seg.id, ctxID)
		}
		if seg.class != "query" {
			t.Fatalf("%s class = %q, want query", seg.seg, seg.class)
		}
		if seg.method != http.MethodGet || seg.path != "/withdrawals/req-1" {
			t.Fatalf("%s method/path = %s %s", seg.seg, seg.method, seg.path)
		}
		perfSeamCheckTiming(t, seg, before, after)
	}
	// Nesting: below_admit ⊆ server_total.
	if below.at.Before(total.at) || below.at.Add(below.dur).After(total.at.Add(total.dur)) {
		t.Fatalf("below_admit [%s,%s] not nested in server_total [%s,%s]",
			below.at, below.at.Add(below.dur), total.at, total.at.Add(total.dur))
	}

	// A second withdrawal request mints a fresh, strictly larger id.
	firstID := ctxID
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/withdrawals/req-2", nil))
	second, err := strconv.ParseUint(rec.Header().Get(perfIDHeader), 10, 64)
	if err != nil {
		t.Fatalf("second request %s: %v", perfIDHeader, err)
	}
	if second <= firstID {
		t.Fatalf("second id = %d, want > %d", second, firstID)
	}

	// A non-withdrawal path is never instrumented.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if got := rec.Header().Get(perfIDHeader); got != "" {
		t.Fatalf("non-withdrawal path echoed %s = %q, want none", perfIDHeader, got)
	}
	if got := len(sink.snapshot()); got != 4 {
		t.Fatalf("segments after pass-through = %d, want 4", got)
	}

	// Disarmed: a withdrawal request passes through with no id and no record.
	PerfDisableSeg()
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/withdrawals/req-3", nil))
	if got := rec.Header().Get(perfIDHeader); got != "" {
		t.Fatalf("disarmed path echoed %s = %q, want none", perfIDHeader, got)
	}
	if got := len(sink.snapshot()); got != 4 {
		t.Fatalf("segments after disarm = %d, want 4", got)
	}
}

// TestPerfSeamAdmitSkipSemantics covers the admission seam: disabled query
// limiting records admit_skipped and never touches the policy; enabled query
// limiting runs the policy once and records admit; other classes keep their
// semantics in both modes; an id-less context records nothing.
func TestPerfSeamAdmitSkipSemantics(t *testing.T) {
	sink := &perfSeamSink{}
	perfSeamArm(t, sink)
	policy, store := perfSeamPolicy(t)

	const id = 4242
	ctx := db.PerfWithID(context.Background(), id)
	queryReq := httptest.NewRequest(http.MethodGet, "/withdrawals/req-9", nil)

	// Query limiter off: no policy call, nil outcome, admit_skipped recorded.
	PerfSetQueryLimiterEnabled(false)
	if err := admitWithPerf(ctx, policy, ratelimit.ClassQuery, queryReq); err != nil {
		t.Fatalf("skipped query admit = %v, want nil", err)
	}
	if got := store.calls(); got != 0 {
		t.Fatalf("policy consulted %d times with query limiting off, want 0", got)
	}
	skips := sink.bySeg("admit_skipped")
	if len(skips) != 1 {
		t.Fatalf("admit_skipped records = %d, want 1", len(skips))
	}
	if skips[0].id != id || skips[0].class != "query" || skips[0].dur != 0 || skips[0].method != http.MethodGet {
		t.Fatalf("admit_skipped record = %+v, want id %d class query dur 0 GET", skips[0], id)
	}

	// Query limiter on: one policy call, admit recorded with a real duration.
	PerfSetQueryLimiterEnabled(true)
	before := time.Now()
	if err := admitWithPerf(ctx, policy, ratelimit.ClassQuery, queryReq); err != nil {
		t.Fatalf("query admit = %v, want nil", err)
	}
	after := time.Now()
	if got := store.calls(); got != 1 {
		t.Fatalf("policy calls = %d, want 1", got)
	}
	admits := sink.bySeg("admit")
	if len(admits) != 1 {
		t.Fatalf("admit records = %d, want 1", len(admits))
	}
	if admits[0].id != id || admits[0].class != "query" {
		t.Fatalf("admit record = %+v, want id %d class query", admits[0], id)
	}
	perfSeamCheckTiming(t, admits[0], before, after)

	// Another class with query limiting off keeps its exact semantics: the
	// policy still runs and the record carries the class of the request.
	PerfSetQueryLimiterEnabled(false)
	postReq := httptest.NewRequest(http.MethodPost, "/withdrawals", strings.NewReader("{}"))
	if err := admitWithPerf(ctx, policy, ratelimit.ClassNewWithdrawal, postReq); err != nil {
		t.Fatalf("new_withdrawal admit = %v, want nil", err)
	}
	if got := store.calls(); got != 2 {
		t.Fatalf("policy calls = %d, want 2 (new withdrawal must keep its admission)", got)
	}
	admits = sink.bySeg("admit")
	last := admits[len(admits)-1]
	if last.class != "new_withdrawal" || last.method != http.MethodPost {
		t.Fatalf("new_withdrawal record = %+v, want class new_withdrawal POST", last)
	}

	// An id-less context keeps the exact admission path and records nothing.
	records := len(sink.snapshot())
	PerfSetQueryLimiterEnabled(true)
	if err := admitWithPerf(context.Background(), policy, ratelimit.ClassQuery, queryReq); err != nil {
		t.Fatalf("id-less admit = %v, want nil", err)
	}
	if got := store.calls(); got != 3 {
		t.Fatalf("policy calls = %d, want 3", got)
	}
	if got := len(sink.snapshot()); got != records {
		t.Fatalf("id-less admit added records: %d -> %d", records, got)
	}

	// The query-limiter switch is independent of collection: a disarmed sink
	// (an uncollected arm) still bypasses the query admission without calling
	// the policy, and writes no record.
	PerfDisableSeg()
	PerfSetQueryLimiterEnabled(false)
	if err := admitWithPerf(ctx, policy, ratelimit.ClassQuery, queryReq); err != nil {
		t.Fatalf("disarmed skipped admit = %v, want nil", err)
	}
	if got := store.calls(); got != 3 {
		t.Fatalf("disarmed skip consulted the policy: calls = %d, want 3", got)
	}
	if got := len(sink.snapshot()); got != records {
		t.Fatalf("disarmed skip added records: %d -> %d", records, got)
	}
	PerfSetQueryLimiterEnabled(true)
	if err := admitWithPerf(ctx, policy, ratelimit.ClassQuery, queryReq); err != nil {
		t.Fatalf("disarmed admit = %v, want nil", err)
	}
	if got := store.calls(); got != 4 {
		t.Fatalf("policy calls = %d, want 4", got)
	}
	if got := len(sink.snapshot()); got != records {
		t.Fatalf("disarmed admit added records: %d -> %d", records, got)
	}
}

// TestPerfSeamBelowAdmitRecords covers the below-admit wrapper: id-carrying
// requests are timed and recorded, id-less requests and disarmed requests
// pass through untouched.
func TestPerfSeamBelowAdmitRecords(t *testing.T) {
	sink := &perfSeamSink{}
	perfSeamArm(t, sink)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	handler := perfWrapBelowAdmit(inner)

	// No perf id: pass-through, no record.
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/withdrawals/req-1", nil))
	if got := len(sink.snapshot()); got != 0 {
		t.Fatalf("id-less below_admit records = %d, want 0", got)
	}

	// Perf id: one below_admit record.
	req := httptest.NewRequest(http.MethodGet, "/withdrawals/req-1", nil)
	req = req.WithContext(db.PerfWithID(req.Context(), 7))
	before := time.Now()
	handler.ServeHTTP(httptest.NewRecorder(), req)
	after := time.Now()
	segs := sink.snapshot()
	if len(segs) != 1 {
		t.Fatalf("below_admit records = %d, want 1", len(segs))
	}
	if segs[0].seg != "below_admit" || segs[0].id != 7 || segs[0].class != "query" {
		t.Fatalf("below_admit record = %+v, want id 7 class query", segs[0])
	}
	perfSeamCheckTiming(t, segs[0], before, after)

	// Disarmed: pass-through even with an id.
	PerfDisableSeg()
	handler.ServeHTTP(httptest.NewRecorder(), req)
	if got := len(sink.snapshot()); got != 1 {
		t.Fatalf("disarmed below_admit records = %d, want 1", got)
	}
}

// TestPerfSeamConcurrentRequests drives 50 concurrent instrumented requests
// through the real chain; every request must get its own id and exactly one
// server_total plus one below_admit record (race-detector coverage for the
// atomic seam and the sink contract).
func TestPerfSeamConcurrentRequests(t *testing.T) {
	sink := &perfSeamSink{}
	perfSeamArm(t, sink)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := db.PerfIDFromContext(r.Context()); !ok {
			t.Errorf("request %s carries no perf id", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	})
	handler := perfWrapServeHandler(perfWrapBelowAdmit(inner))

	const n = 50
	ids := make([]uint64, n)
	problems := make([]string, n)
	var wg sync.WaitGroup
	// One knob writer runs concurrently with the requests: the copy-on-write
	// state publisher must stay race-free and never split a request's state.
	var knobs sync.WaitGroup
	knobs.Add(1)
	go func() {
		defer knobs.Done()
		for i := range 20 {
			PerfSetQueryLimiterEnabled(i%2 == 0)
		}
		PerfSetQueryLimiterEnabled(true)
	}()
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/withdrawals/req-"+strconv.Itoa(i), nil))
			id, err := strconv.ParseUint(rec.Header().Get(perfIDHeader), 10, 64)
			if err != nil {
				problems[i] = fmt.Sprintf("request %d: id header %q: %v", i, rec.Header().Get(perfIDHeader), err)
				return
			}
			ids[i] = id
		}(i)
	}
	wg.Wait()
	knobs.Wait()
	for _, problem := range problems {
		if problem != "" {
			t.Error(problem)
		}
	}

	seen := map[uint64]bool{}
	for _, id := range ids {
		if id == 0 {
			t.Fatal("a concurrent request received no perf id")
		}
		if seen[id] {
			t.Fatalf("perf id %d minted twice", id)
		}
		seen[id] = true
	}

	type segKey struct {
		id  uint64
		seg string
	}
	counts := map[segKey]int{}
	for _, seg := range sink.snapshot() {
		if !seen[seg.id] {
			t.Errorf("segment %s carries unknown id %d", seg.seg, seg.id)
		}
		counts[segKey{id: seg.id, seg: seg.seg}]++
	}
	if got := len(sink.snapshot()); got != 2*n {
		t.Fatalf("segments = %d, want %d", got, 2*n)
	}
	for id := range seen {
		if counts[segKey{id: id, seg: "server_total"}] != 1 || counts[segKey{id: id, seg: "below_admit"}] != 1 {
			t.Fatalf("id %d: server_total=%d below_admit=%d, want 1/1", id,
				counts[segKey{id: id, seg: "server_total"}], counts[segKey{id: id, seg: "below_admit"}])
		}
	}
}
