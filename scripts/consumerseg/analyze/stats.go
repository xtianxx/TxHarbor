// stats.go holds the deterministic, stdlib-only statistics primitives of the
// consumer-seg analyzer: the frozen six-number distribution, the
// linear-interpolation percentile, the strict-interval overlap counter and the
// tail-window clipping math. Everything works on integer microseconds coming
// from the evidence files; derived floats are rounded to 1e-6 resolution.
package main

import (
	"math"
	"sort"
)

// round6 rounds a derived float to 1e-6 resolution. Every input is an integer
// microsecond or an integer count, so 1e-6 keeps ratios exact to
// sub-nanosecond precision while keeping float noise out of the JSON and the
// markdown.
func round6(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return math.Round(v*1e6) / 1e6
}

// percentile is the linear-interpolation percentile of an ascending slice:
// rank = p*(n-1), interpolated between the two neighboring order statistics
// (the method documented in summary.md). Empty input returns 0; callers gate
// on the sample count first.
func percentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	switch {
	case n == 0:
		return 0
	case n == 1:
		return sorted[0]
	}
	rank := p * float64(n-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo < 0 {
		lo = 0
	}
	if hi > n-1 {
		hi = n - 1
	}
	if lo == hi {
		return sorted[lo]
	}
	frac := rank - float64(lo)
	return sorted[lo] + (sorted[hi]-sorted[lo])*frac
}

// dist is the frozen six-number summary of one value set (microseconds for
// every duration block). A nil *dist means "no sample" and renders as "-",
// never as 0.
type dist struct {
	N    int     `json:"n"`
	Min  float64 `json:"min"`
	Mean float64 `json:"mean"`
	P50  float64 `json:"p50"`
	P95  float64 `json:"p95"`
	P99  float64 `json:"p99"`
	Max  float64 `json:"max"`
}

// distFromInt64 summarizes one microsecond value set. The caller's slice is
// copied; the sort never touches it.
func distFromInt64(values []int64) *dist {
	if len(values) == 0 {
		return nil
	}
	sorted := make([]float64, len(values))
	var sum float64
	for i, v := range values {
		sorted[i] = float64(v)
		sum += float64(v)
	}
	sort.Float64s(sorted)
	return &dist{
		N:    len(values),
		Min:  round6(sorted[0]),
		Mean: round6(sum / float64(len(values))),
		P50:  round6(percentile(sorted, 0.50)),
		P95:  round6(percentile(sorted, 0.95)),
		P99:  round6(percentile(sorted, 0.99)),
		Max:  round6(sorted[len(sorted)-1]),
	}
}

// median is the standard median of the values (even counts average the two
// middle values). Empty input returns 0 and ok=false.
func median(values []float64) (float64, bool) {
	if len(values) == 0 {
		return 0, false
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2], true
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2, true
}

// span is one top-level loop span [Start, Start+Dur] in microseconds relative
// to the monotonic epoch.
type span struct {
	Kind  string
	Start int64
	Dur   int64
}

func (s span) end() int64 { return s.Start + s.Dur }

// overlapUS is the length of the intersection of [start, end] with the window
// [winStart, winEnd]; touching boundaries (end == winStart) count as zero.
func overlapUS(start, end, winStart, winEnd int64) int64 {
	lo := start
	if winStart > lo {
		lo = winStart
	}
	hi := end
	if winEnd < hi {
		hi = winEnd
	}
	if hi <= lo {
		return 0
	}
	return hi - lo
}

// overlapPairs counts pairwise overlaps of positive-length spans with strict
// interval semantics: a span that ends exactly where another starts does not
// overlap, and zero-length spans never overlap anything. It returns the pair
// count and the number of spans that start strictly inside an earlier span.
//
// The sweep is O(n log n): every span contributes one start edge (+1) and one
// end edge (-1); ends sort before starts at one position, so the "active"
// count at a start edge is exactly the number of earlier spans still running.
func overlapPairs(spans []span) (pairs, violating int) {
	type edge struct {
		pos   int64
		delta int
	}
	edges := make([]edge, 0, 2*len(spans))
	for _, s := range spans {
		if s.end() <= s.Start {
			continue
		}
		edges = append(edges, edge{s.Start, +1}, edge{s.end(), -1})
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].pos != edges[j].pos {
			return edges[i].pos < edges[j].pos
		}
		return edges[i].delta < edges[j].delta
	})
	active := 0
	for _, e := range edges {
		if e.delta > 0 {
			if active > 0 {
				pairs += active
				violating++
			}
			active++
			continue
		}
		active--
	}
	return pairs, violating
}

// clipTail computes the window clipping of every top-level span against the
// tail window [winStart, winEnd]:
//
//   - full:    the span lies entirely inside the window;
//   - partial: the span crosses a window boundary — only the in-window part
//     counts into the coverage;
//   - outside: no overlap at all.
//
// The residual is tail − Σ in-window coverage; the raw full-span sum is
// reported for audit only and must never be compared with the tail directly.
func clipTail(spans []span, winStart, winEnd int64) tailBlock {
	tb := tailBlock{
		Collected:     true,
		WindowStartUS: &winStart,
		WindowEndUS:   &winEnd,
		ByKind:        map[string]*kindClip{},
	}
	tail := winEnd - winStart
	tb.TailUS = new(tail)
	for _, s := range spans {
		kc := tb.ByKind[s.Kind]
		if kc == nil {
			kc = &kindClip{}
			tb.ByKind[s.Kind] = kc
		}
		kc.Spans++
		dur := s.end() - s.Start
		if dur < 0 {
			tb.NegativeDurations++
		}
		if dur == 0 {
			tb.ZeroDurations++
		}
		if s.Start < 0 {
			tb.PreEpochSpans++
		}
		ov := overlapUS(s.Start, s.end(), winStart, winEnd)
		switch {
		case s.Start >= winStart && s.end() <= winEnd:
			kc.Full++
			tb.FullSpans++
			kc.RawFullUS += dur
			tb.RawFullSpansUS += dur
			kc.CoverageUS += ov
			tb.CoverageUS += ov
		case ov > 0:
			kc.Partial++
			tb.PartialSpans++
			kc.CoverageUS += ov
			tb.CoverageUS += ov
		default:
			kc.Outside++
			tb.OutsideSpans++
		}
	}
	residual := tail - tb.CoverageUS
	tb.ResidualUS = new(residual)
	if tail != 0 {
		pct := round6(float64(residual) / float64(tail) * 100)
		tb.ResidualPct = new(pct)
	}
	return tb
}
