//go:build perf

package db

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This file tests the perf-tagged query-trace seam offline (no Docker, no
// testutil): the context id round-trip, the single-line bounded SQL rendering
// and the tracer contract (records only with a recorder and an id, failed
// statements included, the existing tracer chain forwarded).

// perfTraceFakeNext is a pgx.QueryTracer stand-in for the pre-existing chain.
type perfTraceFakeNext struct {
	starts int
	ends   int
	marker contextKey
}

type contextKey string

func (f *perfTraceFakeNext) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	f.starts++
	return context.WithValue(ctx, f.marker, "next")
}

func (f *perfTraceFakeNext) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	f.ends++
}

// perfTraceCollector captures every recorded statement.
type perfTraceCollector struct {
	records []PerfQueryRecord
}

func (c *perfTraceCollector) record(rec PerfQueryRecord) {
	c.records = append(c.records, rec)
}

// perfTraceEnd drives one start/end pair through the tracer.
func perfTraceEnd(tracer perfQueryTracer, ctx context.Context, err error) {
	started := tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "select 1"})
	tracer.TraceQueryEnd(started, nil, pgx.TraceQueryEndData{Err: err})
}

func TestPerfTraceContextRoundTrip(t *testing.T) {
	if _, ok := PerfIDFromContext(context.Background()); ok {
		t.Fatal("empty context reports a perf id")
	}
	ctx := PerfWithID(context.Background(), 17)
	id, ok := PerfIDFromContext(ctx)
	if !ok || id != 17 {
		t.Fatalf("PerfIDFromContext = (%d, %v), want (17, true)", id, ok)
	}
}

func TestPerfTraceCompactSQL(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want string
	}{
		{"single line", "select 1", "select 1"},
		{"newlines and tabs collapse", "select\n  id\tfrom withdrawals", "select id from withdrawals"},
		{"carriage return", "select\r\n1", "select 1"},
	}
	for _, tc := range cases {
		if got := perfCompactSQL(tc.sql); got != tc.want {
			t.Fatalf("%s: perfCompactSQL(%q) = %q, want %q", tc.name, tc.sql, got, tc.want)
		}
	}
	long := "select " + strings.Repeat("jsonb_build_object(a, b), ", 20) + "from withdrawals"
	got := perfCompactSQL(long)
	if len(got) > perfSQLMaxLen {
		t.Fatalf("perfCompactSQL(%d bytes) = %d bytes, want <= %d", len(long), len(got), perfSQLMaxLen)
	}
	if len(long) > perfSQLMaxLen && !strings.HasPrefix(long, got) {
		t.Fatalf("truncated SQL %q is not a prefix of the input", got)
	}
	multibyte := "x" + strings.Repeat("é", perfSQLMaxLen) + "tail"
	got = perfCompactSQL(multibyte)
	if len(got) > perfSQLMaxLen {
		t.Fatalf("multibyte truncation = %d bytes, want <= %d", len(got), perfSQLMaxLen)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("multibyte truncation split a rune: %q", got)
	}
}

func TestPerfQueryTracerRecords(t *testing.T) {
	collector := &perfTraceCollector{}
	PerfSetQueryRecorder(collector.record)
	defer PerfSetQueryRecorder(nil)

	tracer := perfQueryTracer{}
	ctx := PerfWithID(context.Background(), 9)

	before := time.Now()
	perfTraceEnd(tracer, ctx, nil)
	after := time.Now()

	if len(collector.records) != 1 {
		t.Fatalf("records = %d, want 1", len(collector.records))
	}
	rec := collector.records[0]
	if rec.ID != 9 || rec.SQL != "select 1" || rec.Err {
		t.Fatalf("record = %+v, want id 9 sql %q err false", rec, "select 1")
	}
	if rec.Dur < 0 {
		t.Fatalf("recorded dur = %s, want >= 0", rec.Dur)
	}
	if rec.At.Before(before) || rec.At.After(after) {
		t.Fatalf("recorded at = %s outside [%s, %s]", rec.At, before, after)
	}

	// A failed statement is recorded too, with Err set.
	perfTraceEnd(tracer, ctx, context.DeadlineExceeded)
	if len(collector.records) != 2 || !collector.records[1].Err {
		t.Fatalf("records = %+v, want a second record with Err", collector.records)
	}
}

func TestPerfQueryTracerSkipsWithoutIDOrRecorder(t *testing.T) {
	collector := &perfTraceCollector{}
	PerfSetQueryRecorder(collector.record)
	defer PerfSetQueryRecorder(nil)

	tracer := perfQueryTracer{}
	// No id in the context: nothing is recorded even with a recorder set.
	perfTraceEnd(tracer, context.Background(), nil)
	if len(collector.records) != 0 {
		t.Fatalf("id-less statement recorded %d times, want 0", len(collector.records))
	}

	// No recorder: an identified statement is still not recorded.
	PerfSetQueryRecorder(nil)
	perfTraceEnd(tracer, PerfWithID(context.Background(), 3), nil)
	if len(collector.records) != 0 {
		t.Fatalf("statement recorded %d times without a recorder, want 0", len(collector.records))
	}
}

func TestPerfQueryTracerForwardsExistingChain(t *testing.T) {
	collector := &perfTraceCollector{}
	PerfSetQueryRecorder(collector.record)
	defer PerfSetQueryRecorder(nil)

	next := &perfTraceFakeNext{marker: contextKey("next-marker")}
	tracer := perfQueryTracer{next: next}
	ctx := PerfWithID(context.Background(), 5)

	started := tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "select 2"})
	if next.starts != 1 {
		t.Fatalf("next.TraceQueryStart called %d times, want 1", next.starts)
	}
	if marker, _ := started.Value(next.marker).(string); marker != "next" {
		t.Fatal("tracer dropped the context returned by the existing tracer")
	}
	tracer.TraceQueryEnd(started, nil, pgx.TraceQueryEndData{})
	if next.ends != 1 {
		t.Fatalf("next.TraceQueryEnd called %d times, want 1", next.ends)
	}
	if len(collector.records) != 1 || collector.records[0].SQL != "select 2" {
		t.Fatalf("records = %+v, want the traced statement alongside the forwarded chain", collector.records)
	}
}

func TestPerfAttachQueryTracerGate(t *testing.T) {
	newConfig := func(t *testing.T) *pgxpool.Config {
		t.Helper()
		cfg, err := pgxpool.ParseConfig("postgres://txharbor:txharbor@127.0.0.1:5432/txharbor")
		if err != nil {
			t.Fatalf("ParseConfig: %v", err)
		}
		return cfg
	}

	// Gate off: nothing is attached and the config is untouched.
	t.Setenv(perfTraceEnvGate, "0")
	cfg := newConfig(t)
	perfAttachQueryTracer(cfg)
	if cfg.ConnConfig.Tracer != nil {
		t.Fatalf("tracer attached with %s=0: %T", perfTraceEnvGate, cfg.ConnConfig.Tracer)
	}

	// Gate on with no existing tracer: the seam tracer is attached.
	t.Setenv(perfTraceEnvGate, "1")
	cfg = newConfig(t)
	perfAttachQueryTracer(cfg)
	attached, ok := cfg.ConnConfig.Tracer.(perfQueryTracer)
	if !ok {
		t.Fatalf("tracer = %T, want perfQueryTracer", cfg.ConnConfig.Tracer)
	}
	if attached.next != nil {
		t.Fatalf("attached tracer forwarded to %T, want nil chain", attached.next)
	}

	// Gate on with an existing query tracer: it is wrapped and forwarded.
	next := &perfTraceFakeNext{marker: contextKey("chain")}
	cfg = newConfig(t)
	cfg.ConnConfig.Tracer = next
	perfAttachQueryTracer(cfg)
	attached, ok = cfg.ConnConfig.Tracer.(perfQueryTracer)
	if !ok {
		t.Fatalf("tracer = %T, want perfQueryTracer wrapping the existing chain", cfg.ConnConfig.Tracer)
	}
	if attached.next != next {
		t.Fatalf("attached tracer forwards to %v, want the existing tracer", attached.next)
	}
}
