// timingring.go is test support only (same charter as redis.go). It records
// stop-to-observation latency samples for the Redis failure-latency A/B/C
// experiment and aggregates them into the same percentile discipline the
// perf harness uses (linear interpolation; Sample/ClassStats/percentile are
// copied structurally from internal/perf/harness.go:1003-1087 because the
// perf build tag makes that package un-importable across layers).
package testutil

import (
	"math"
	"sort"
	"sync"
	"time"
)

// LatencySample is one Allow() observation. Fields mirror what the
// experiment must be able to prove about each attempt:
//
//   - ErrorClass is the coarse bucket the caller observed (see
//     ClassifyRedisError in the experiment test): "refused" (shape ①),
//     "dial_io_timeout" (② or a dial attempt that timed out inside
//     DialTimeout), "context_deadline_exceeded" (the caller budget cut
//     everything off), "read_io_timeout" (shape ③(b): cold HELLO read), or
//     "ok".
//   - ErrorText keeps the raw error string; unsent/unknown distinctions
//     stay explicit (the experiment never infers an unobserved stage).
type LatencySample struct {
	Variant    string    `json:"variant"` // A / B / C
	Shape      string    `json:"shape"`   // refused / dial_blackhole / conn_hold_cold / conn_hold_warm / normal
	Round      int       `json:"round"`   // 1 / 2
	Caller     int       `json:"caller"`
	DurationMS float64   `json:"duration_ms"`
	OK         bool      `json:"ok"`
	ErrorClass string    `json:"error_class,omitempty"`
	ErrorText  string    `json:"error_text,omitempty"`
	Unavail    bool      `json:"unavail"` // limiter.Unavailable() at observation time
	At         time.Time `json:"at"`
	// Unobservable stages (go-redis attempt counts, socket-level dial/send
	// breakdown) are NOT fields here: the caller-visible surface records
	// only what it genuinely saw. "Attempt tracking" fields were dropped
	// during fixture revision for exactly that reason.
}

// LatencyStats is the per-(variant,shape,round) aggregation.
type LatencyStats struct {
	Variant    string         `json:"variant"`
	Shape      string         `json:"shape"`
	Round      int            `json:"round"`
	Count      int            `json:"count"`
	Errors     int            `json:"errors"`
	ErrorRate  float64        `json:"error_rate"`
	ErrorWeeks map[string]int `json:"error_classes,omitempty"`
	P50MS      float64        `json:"p50_ms"`
	P95MS      float64        `json:"p95_ms"`
	P99MS      float64        `json:"p99_ms"`
	MaxMS      float64        `json:"max_ms"`
	MeanMS     float64        `json:"mean_ms"`
	StdDevMS   float64        `json:"stddev_ms"`
}

// TimingRing collects samples under a mutex; size is capped by the
// experiment (fixed variants × shapes × rounds × callers), so no eviction
// is needed: capacity is allocated once and overwritten.
type TimingRing struct {
	mu      sync.Mutex
	samples []LatencySample
}

// NewTimingRing pre-allocates capacity.
func NewTimingRing(capacity int) *TimingRing {
	return &TimingRing{samples: make([]LatencySample, 0, capacity)}
}

// Record appends one sample.
func (r *TimingRing) Record(s LatencySample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples = append(r.samples, s)
}

// Snapshot returns a copy (safe for aggregation after the run).
func (r *TimingRing) Snapshot() []LatencySample {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]LatencySample, len(r.samples))
	copy(out, r.samples)
	return out
}

// Select filters by variant/shape/round. Empty strings mean "any".
func Select(samples []LatencySample, variant, shape string, round int) []LatencySample {
	var out []LatencySample
	for _, s := range samples {
		if variant != "" && s.Variant != variant {
			continue
		}
		if shape != "" && s.Shape != shape {
			continue
		}
		if round != 0 && s.Round != round {
			continue
		}
		out = append(out, s)
	}
	return out
}

// ComputeLatencyStats aggregates durations of one selected sample set. The
// percentile method is identical to perf/harness.go (linear interpolation).
func ComputeLatencyStats(samples []LatencySample) LatencyStats {
	stats := LatencyStats{ErrorWeeks: map[string]int{}}
	var durations []float64
	for _, s := range samples {
		if len(stats.ErrorWeeks) == 0 {
			stats.Variant = s.Variant
			stats.Shape = s.Shape
			stats.Round = s.Round
		}
		stats.Count++
		if !s.OK {
			stats.Errors++
			stats.ErrorWeeks[s.ErrorClass]++
		}
		durations = append(durations, s.DurationMS)
	}
	if stats.Count == 0 {
		return stats
	}
	stats.ErrorRate = float64(stats.Errors) / float64(stats.Count)
	sort.Float64s(durations)
	stats.P50MS = latencyPercentile(durations, 50)
	stats.P95MS = latencyPercentile(durations, 95)
	stats.P99MS = latencyPercentile(durations, 99)
	stats.MaxMS = durations[len(durations)-1]
	var sum float64
	for _, d := range durations {
		sum += d
	}
	stats.MeanMS = sum / float64(len(durations))
	if len(durations) > 1 {
		var sq float64
		for _, d := range durations {
			sq += (d - stats.MeanMS) * (d - stats.MeanMS)
		}
		stats.StdDevMS = math.Sqrt(sq / float64(len(durations)-1))
	}
	return stats
}

// latencyPercentile is linear-interpolation (copied from perf/harness.go).
func latencyPercentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	rank := p / 100 * float64(len(sorted)-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return sorted[lo]
	}
	return sorted[lo] + (sorted[hi]-sorted[lo])*(rank-float64(lo))
}
