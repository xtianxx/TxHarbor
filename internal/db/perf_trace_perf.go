//go:build perf

package db

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This file is the perf-tagged half of the segmented-measurement query-trace
// seam (frozen API; the ordinary build resolves every symbol here to the
// no-op in perf_trace_none.go). It exists so the measurement harness can time
// individual SQL statements per request without any change to production
// builds: the file only compiles under `-tags perf`, and even then the tracer
// is attached only when TXHARBOR_PERF_SEG_TRACE=1 names itself. The tracer
// never alters query semantics: it reads ctx, forwards the existing tracer
// chain unchanged, and all state is atomic.
//
// Trace discipline (frozen): a statement is recorded only when a recorder is
// installed AND the statement's context carries a perf id (PerfWithID). A
// TraceQueryStart without either still returns a valid context; a nil recorder
// or an id-less context simply yields no record.

// perfTraceEnvGate is the environment gate of the tracer. The seam stays
// disarmed unless it is exactly "1", so even a perf-tagged binary keeps the
// ordinary connection behavior by default.
const perfTraceEnvGate = "TXHARBOR_PERF_SEG_TRACE"

// perfSQLMaxLen bounds the recorded SQL text (runes) so one JSONL record
// stays a bounded line regardless of the statement.
const perfSQLMaxLen = 96

// PerfQueryRecord is one traced statement completion: the request id the
// statement ran under, the compacted SQL, the measured duration, the start
// time and whether the statement failed. Err stays a bool (the record is
// evidence of timing, not of error taxonomy).
type PerfQueryRecord struct {
	ID  uint64
	SQL string
	Dur time.Duration
	At  time.Time
	Err bool
}

// perfQueryRecorder is the active recorder; nil means tracing is off. The
// recorder is read on every traced statement end, so it is an atomic pointer
// (swapping it concurrently with in-flight statements is safe).
var perfQueryRecorder atomic.Pointer[func(PerfQueryRecord)]

// PerfSetQueryRecorder installs the recorder that receives every traced
// statement; nil disables recording (the tracer stays attached and inert).
func PerfSetQueryRecorder(rec func(PerfQueryRecord)) {
	if rec == nil {
		perfQueryRecorder.Store(nil)
		return
	}
	perfQueryRecorder.Store(&rec)
}

// perfTraceIDKey is the private context key of the request id.
type perfTraceIDKey struct{}

// PerfWithID returns a context carrying id, the seam the serve side uses to
// mint one id per request and hand it to every segment (including the
// database statements the request issues).
func PerfWithID(ctx context.Context, id uint64) context.Context {
	return context.WithValue(ctx, perfTraceIDKey{}, id)
}

// PerfIDFromContext reports the perf id carried by ctx, if any.
func PerfIDFromContext(ctx context.Context) (uint64, bool) {
	id, ok := ctx.Value(perfTraceIDKey{}).(uint64)
	return id, ok
}

// perfQueryStart is the per-statement state carried in the context returned
// by TraceQueryStart; TraceQueryEnd is invoked with exactly that context.
type perfQueryStart struct {
	sql   string
	start time.Time
}

type perfQueryStartKey struct{}

// perfQueryTracer implements pgx.QueryTracer. next is the pre-existing tracer
// of the pool config (nil when there was none): every call is forwarded to it
// unchanged so the existing chain keeps its exact behavior.
type perfQueryTracer struct {
	next pgx.QueryTracer
}

// TraceQueryStart stores the statement text and start time in the returned
// context.
func (t perfQueryTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if t.next != nil {
		ctx = t.next.TraceQueryStart(ctx, conn, data)
	}
	return context.WithValue(ctx, perfQueryStartKey{}, perfQueryStart{sql: data.SQL, start: time.Now()})
}

// TraceQueryEnd emits one record when a recorder is installed and the context
// carries a perf id, then forwards to the existing tracer chain. Failed
// statements are recorded too (Err=true).
func (t perfQueryTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if start, ok := ctx.Value(perfQueryStartKey{}).(perfQueryStart); ok {
		if rec := perfQueryRecorder.Load(); rec != nil {
			if id, hasID := PerfIDFromContext(ctx); hasID {
				(*rec)(PerfQueryRecord{
					ID:  id,
					SQL: perfCompactSQL(start.sql),
					Dur: time.Since(start.start),
					At:  start.start,
					Err: data.Err != nil,
				})
			}
		}
	}
	if t.next != nil {
		t.next.TraceQueryEnd(ctx, conn, data)
	}
}

// perfAttachQueryTracer attaches the tracer to cfg when, and only when, the
// environment gate says so. An existing pgx.QueryTracer is wrapped and
// forwarded; a tracer that does not implement pgx.QueryTracer is left
// untouched (replacing it would silently drop its other tracing
// capabilities). Nothing else about cfg is modified.
func perfAttachQueryTracer(cfg *pgxpool.Config) {
	if os.Getenv(perfTraceEnvGate) != "1" {
		return
	}
	if cfg == nil || cfg.ConnConfig == nil {
		return
	}
	next, _ := cfg.ConnConfig.Tracer.(pgx.QueryTracer)
	if cfg.ConnConfig.Tracer != nil && next == nil {
		return
	}
	cfg.ConnConfig.Tracer = perfQueryTracer{next: next}
}

// perfCompactSQL flattens a statement onto one line (whitespace runs collapse
// to single spaces) and truncates it to at most perfSQLMaxLen bytes without
// splitting a rune, so the recorded text is always a single valid-UTF-8 line.
func perfCompactSQL(sql string) string {
	flat := strings.Join(strings.Fields(sql), " ")
	if len(flat) <= perfSQLMaxLen {
		return flat
	}
	cut := perfSQLMaxLen
	for cut > 0 && !utf8.RuneStart(flat[cut]) {
		cut--
	}
	return flat[:cut]
}
