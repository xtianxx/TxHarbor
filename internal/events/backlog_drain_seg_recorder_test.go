//go:build integration_backlog && perf

// backlog_drain_seg_recorder_test.go is the offline half of the consumer
// segmented-measurement harness (013 verification supplement, V-CATCHUP
// supplement). It owns everything the instrumented drain needs but nothing it
// boots: the segment recorder that implements events.PerfConsumerSegSink, the
// pgx.QueryTracer that attributes SQL statements to the event sequence they
// ran under, the frozen CSV row formats (format v1) and the CSV writer.
//
// It carries no Docker dependency: TestBacklogDrainSegRecorder exercises the
// recorder with synthesized spans (pairing, sequence annotation, drop
// counting), the tracer under marked and unmarked contexts (recording vs.
// zero-allocation forwarding) and a CSV write/read round trip.
//
// Timing basis (frozen): the harness captures one monotonic epoch time.Time
// at test start; every t_us/dur_us in these rows is an integer microsecond
// offset from that epoch (monotonic clock), never a wall-clock timestamp.
//
// Attribution: the tracer records only statements whose context is marked
// with backlogSegMarked (the harness marks the consumer's run context, so
// every statement the consumer issues is attributed), and it attributes each
// statement to the event sequence the recorder last entered through
// ProcessStart. Every other statement (monitor, publisher, harness queries)
// is forwarded without allocation and without a record.
package events_test

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/events"
)

// backlogSegBase is the frozen timing basis: every recorded offset is a
// microsecond delta from epoch on the monotonic clock.
type backlogSegBase struct {
	epoch time.Time
}

// us returns t as integer microseconds since the epoch.
func (b backlogSegBase) us(t time.Time) int64 { return t.Sub(b.epoch).Microseconds() }

// dur returns d as integer microseconds.
func (b backlogSegBase) dur(d time.Duration) int64 { return d.Microseconds() }

// seconds converts a recorded microsecond offset back to seconds (the report
// keeps the original harness's float seconds).
func (b backlogSegBase) seconds(us int64) float64 { return float64(us) / 1e6 }

// backlogSegDrops counts observations the recorder refused to keep: a loop
// span whose start and done did not pair (a harness bug, asserted to be 0)
// and monitor samples beyond the frozen sample cap.
type backlogSegDrops struct {
	LoopUnpaired   int `json:"loop_unpaired"`
	SamplesDropped int `json:"samples_dropped"`
}

// backlogSegCounts is the frozen per-run cross-check tally (anchors.counts).
type backlogSegCounts struct {
	ObservedProcess     int `json:"observed_process"`
	ObservedApplied     int `json:"observed_applied"`
	ObservedDuplicate   int `json:"observed_duplicate"`
	ObservedVersionSkip int `json:"observed_version_skip"`
	ObservedQuarantined int `json:"observed_quarantined"`
	ObservedErrors      int `json:"observed_errors"`
	SQLSpans            int `json:"sql_spans"`
	SQLUnattributed     int `json:"sql_unattributed"`
	AppliedCB           int `json:"applied_cb"`
}

// Loop row kinds (loop.csv column kind).
const (
	backlogSegKindPoll      = "poll"
	backlogSegKindProcess   = "process"
	backlogSegKindMark      = "mark"
	backlogSegKindRebalance = "rebalance"
	backlogSegKindLag       = "lag"
	backlogSegKindAppliedCB = "applied_cb"
)

// SQL row kinds (sql.csv column kind). They are derived from the statement
// text of the consumer's own SQL constants (consumer.go / quarantine.go): the
// recorder never reads package internals, it classifies the traced text.
const (
	backlogSegSQLBegin            = "begin"
	backlogSegSQLCommit           = "commit"
	backlogSegSQLRollback         = "rollback"
	backlogSegSQLInboxInsert      = "inbox_insert"
	backlogSegSQLVersionRead      = "version_read"
	backlogSegSQLVersionProbe     = "version_probe"
	backlogSegSQLEffectInsert     = "effect_insert"
	backlogSegSQLVersionUpsert    = "version_upsert"
	backlogSegSQLProgressUpdate   = "progress_update"
	backlogSegSQLQuarantineInsert = "quarantine_insert"
	backlogSegSQLOther            = "other"
)

// backlogSegSQLMaxLen bounds the recorded SQL text (bytes) so one csv row
// stays bounded; the classifier always sees the full text.
const backlogSegSQLMaxLen = 96

// backlogSegLoopRow is one loop.csv row: a paired span (poll/process/mark
// start->done), a lag observation or a point event (rebalance, applied_cb).
// seq is the event sequence number for process/mark/applied_cb and 0 for the
// batch-level rows; i1/i2/s1 carry the kind's payload.
type backlogSegLoopRow struct {
	Kind  string
	Seq   int64
	TUs   int64
	DurUs int64
	I1    int64
	I2    int64
	S1    string
}

func (r backlogSegLoopRow) record() []string {
	return []string{
		r.Kind,
		strconv.FormatInt(r.Seq, 10),
		strconv.FormatInt(r.TUs, 10),
		strconv.FormatInt(r.DurUs, 10),
		strconv.FormatInt(r.I1, 10),
		strconv.FormatInt(r.I2, 10),
		r.S1,
	}
}

// backlogSegSQLRow is one sql.csv row: the traced statement with its start
// offset, duration, classified kind, compacted text and failure flag.
type backlogSegSQLRow struct {
	Seq   int64
	TUs   int64
	DurUs int64
	Kind  string
	SQL   string
	Err   int
}

func (r backlogSegSQLRow) record() []string {
	return []string{
		strconv.FormatInt(r.Seq, 10),
		strconv.FormatInt(r.TUs, 10),
		strconv.FormatInt(r.DurUs, 10),
		r.Kind,
		r.SQL,
		strconv.Itoa(r.Err),
	}
}

// backlogSegPublishRow is one publish.csv row: one bounded publish cycle
// (iterations are 1-based; the last row observes pending=0 and carries no
// publish outcome).
type backlogSegPublishRow struct {
	Iter        int
	TCountUs    int64
	CountDurUs  int64
	PendingSeen int64
	TPublishUs  int64
	PublishDur  int64
	Claimed     int
	Acked       int
	Released    int
	Blocked     int
}

func (r backlogSegPublishRow) record() []string {
	return []string{
		strconv.Itoa(r.Iter),
		strconv.FormatInt(r.TCountUs, 10),
		strconv.FormatInt(r.CountDurUs, 10),
		strconv.FormatInt(r.PendingSeen, 10),
		strconv.FormatInt(r.TPublishUs, 10),
		strconv.FormatInt(r.PublishDur, 10),
		strconv.Itoa(r.Claimed),
		strconv.Itoa(r.Acked),
		strconv.Itoa(r.Released),
		strconv.Itoa(r.Blocked),
	}
}

// backlogSegSampleRow is one samples.csv row: the monitor's 200ms tick
// (durable state + the pool's cumulative counters). The elapsed field feeds
// the original monitor's monotonic assertions and is not part of the CSV.
type backlogSegSampleRow struct {
	ElapsedSeconds       float64
	TUs                  int64
	Pending              int64
	Published            int64
	Applied              int64
	Ledger               int64
	Progress             int64
	AcquireCount         int64
	EmptyAcquireCount    int64
	AcquireDurationUs    int64
	CanceledAcquireCount int64
	TotalConns           int64
	IdleConns            int64
}

func (r backlogSegSampleRow) record() []string {
	return []string{
		strconv.FormatInt(r.TUs, 10),
		strconv.FormatInt(r.Pending, 10),
		strconv.FormatInt(r.Published, 10),
		strconv.FormatInt(r.Applied, 10),
		strconv.FormatInt(r.Ledger, 10),
		strconv.FormatInt(r.Progress, 10),
		strconv.FormatInt(r.AcquireCount, 10),
		strconv.FormatInt(r.EmptyAcquireCount, 10),
		strconv.FormatInt(r.AcquireDurationUs, 10),
		strconv.FormatInt(r.CanceledAcquireCount, 10),
		strconv.FormatInt(r.TotalConns, 10),
		strconv.FormatInt(r.IdleConns, 10),
	}
}

// backlogSegRecorder implements events.PerfConsumerSegSink plus the
// applied-callback hook. mu guards the consumer-side state (loop rows, SQL
// rows, span pairing, counters); the monitor and publish buffers have their
// own mutexes because they are written by other goroutines.
//
// collect=false (the OFF arm) keeps the whole observation path armed — the
// same calls, the same pairing checks and the same tallies — but stores no
// per-span row: the ON arm adds the row storage and the query tracer on top
// of an otherwise identical control.
type backlogSegRecorder struct {
	base    backlogSegBase
	collect bool

	mu      sync.Mutex
	loops   []backlogSegLoopRow
	sqls    []backlogSegSQLRow
	drops   backlogSegDrops
	counts  backlogSegCounts
	nextSeq int64
	lastSeq int64
	curSeq  int64

	pollOpen   bool
	pollStart  time.Time
	procOpen   bool
	procStart  time.Time
	procSeq    int64
	procPart   int32
	procOffset int64
	markOpen   bool
	markStart  time.Time
	markPart   int32
	markOffset int64
	lagOpen    bool
	lagStart   time.Time

	pubMu     sync.Mutex
	publishes []backlogSegPublishRow

	sampleMu      sync.Mutex
	samples       []backlogSegSampleRow
	sampleDrops   int
	sampleErrText []string
}

func newBacklogSegRecorder(epoch time.Time, collect bool) *backlogSegRecorder {
	return &backlogSegRecorder{base: backlogSegBase{epoch: epoch}, collect: collect, curSeq: -1}
}

// PollStart implements events.PerfConsumerSegSink.
func (r *backlogSegRecorder) PollStart() {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pollOpen {
		r.drops.LoopUnpaired++
	}
	r.pollOpen = true
	r.pollStart = now
}

// PollDone implements events.PerfConsumerSegSink. The batch's record count is
// not passed in (the seam never names a broker-client type), so the row starts
// without one: backlogSegFillPollCounts derives it from the process spans that
// fall into this batch's window when the archive is written.
func (r *backlogSegRecorder) PollDone() {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.pollOpen {
		r.drops.LoopUnpaired++
		return
	}
	r.pollOpen = false
	if !r.collect {
		return
	}
	r.loops = append(r.loops, backlogSegLoopRow{
		Kind:  backlogSegKindPoll,
		TUs:   r.base.us(r.pollStart),
		DurUs: r.base.dur(now.Sub(r.pollStart)),
	})
}

// ProcessStart implements events.PerfConsumerSegSink: it opens the span and
// mints the event sequence that every SQL statement of this record carries.
func (r *backlogSegRecorder) ProcessStart(partition int32, offset int64) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.procOpen {
		r.drops.LoopUnpaired++
	}
	r.nextSeq++
	r.procSeq = r.nextSeq
	r.lastSeq = r.nextSeq
	r.curSeq = r.nextSeq
	r.procOpen = true
	r.procStart = now
	r.procPart = partition
	r.procOffset = offset
}

// ProcessDone implements events.PerfConsumerSegSink: it closes the span,
// tallies the outcome (an empty outcome with a non-nil error is the transport
// error class) and leaves the event scope, so later statements are
// unattributed.
func (r *backlogSegRecorder) ProcessDone(result events.ProcessResult, err error) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.curSeq = -1
	if !r.procOpen {
		r.drops.LoopUnpaired++
		return
	}
	r.procOpen = false
	r.counts.ObservedProcess++
	outcome := ""
	if err == nil {
		outcome = string(result.Outcome)
	} else {
		r.counts.ObservedErrors++
	}
	switch outcome {
	case string(events.OutcomeApplied):
		r.counts.ObservedApplied++
	case string(events.OutcomeDuplicate):
		r.counts.ObservedDuplicate++
	case string(events.OutcomeVersionSkip):
		r.counts.ObservedVersionSkip++
	case string(events.OutcomeQuarantined):
		r.counts.ObservedQuarantined++
	}
	if !r.collect {
		return
	}
	r.loops = append(r.loops, backlogSegLoopRow{
		Kind:  backlogSegKindProcess,
		Seq:   r.procSeq,
		TUs:   r.base.us(r.procStart),
		DurUs: r.base.dur(now.Sub(r.procStart)),
		I1:    int64(r.procPart),
		I2:    r.procOffset,
		S1:    outcome,
	})
}

// MarkStart implements events.PerfConsumerSegSink.
func (r *backlogSegRecorder) MarkStart(partition int32, offset int64) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.markOpen {
		r.drops.LoopUnpaired++
	}
	r.markOpen = true
	r.markStart = now
	r.markPart = partition
	r.markOffset = offset
}

// MarkDone implements events.PerfConsumerSegSink. The mark itself is a Kafka
// client call; the offset it carries is the record just processed, so the row
// takes that record's sequence.
func (r *backlogSegRecorder) MarkDone() {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.markOpen {
		r.drops.LoopUnpaired++
		return
	}
	r.markOpen = false
	if !r.collect {
		return
	}
	r.loops = append(r.loops, backlogSegLoopRow{
		Kind:  backlogSegKindMark,
		Seq:   r.lastSeq,
		TUs:   r.base.us(r.markStart),
		DurUs: r.base.dur(now.Sub(r.markStart)),
		I1:    int64(r.markPart),
		I2:    r.markOffset,
	})
}

// RebalanceDone implements events.PerfConsumerSegSink: a point event at the
// end of a settled batch, never a span.
func (r *backlogSegRecorder) RebalanceDone() {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.collect {
		return
	}
	r.loops = append(r.loops, backlogSegLoopRow{Kind: backlogSegKindRebalance, TUs: r.base.us(now)})
}

// RefreshLagStart implements events.PerfConsumerSegSink.
func (r *backlogSegRecorder) RefreshLagStart() {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lagOpen {
		r.drops.LoopUnpaired++
	}
	r.lagOpen = true
	r.lagStart = now
}

// RefreshLagDone implements events.PerfConsumerSegSink.
func (r *backlogSegRecorder) RefreshLagDone() {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.lagOpen {
		r.drops.LoopUnpaired++
		return
	}
	r.lagOpen = false
	if !r.collect {
		return
	}
	r.loops = append(r.loops, backlogSegLoopRow{
		Kind:  backlogSegKindLag,
		TUs:   r.base.us(r.lagStart),
		DurUs: r.base.dur(now.Sub(r.lagStart)),
	})
}

// recordAppliedCB records one consumer applied observation. It is called from
// the consumer goroutine while the event scope is still open, so it carries
// the sequence of the record that was just applied.
func (r *backlogSegRecorder) recordAppliedCB() {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts.AppliedCB++
	if !r.collect {
		return
	}
	r.loops = append(r.loops, backlogSegLoopRow{
		Kind: backlogSegKindAppliedCB,
		Seq:  r.curSeq,
		TUs:  r.base.us(now),
	})
}

// recordSQL stores one traced statement completion.
func (r *backlogSegRecorder) recordSQL(start backlogSegQueryStart, failed bool) {
	if !r.collect {
		return
	}
	row := backlogSegSQLRow{
		Seq:   start.seq,
		TUs:   r.base.us(start.start),
		DurUs: r.base.dur(time.Since(start.start)),
		Kind:  backlogSegSQLKind(start.sql),
		SQL:   backlogSegCompactSQL(start.sql),
	}
	if failed {
		row.Err = 1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sqls = append(r.sqls, row)
}

// traceTarget returns the recorder when the statement tracer must be
// attached (the ON arm) and nil otherwise: the OFF arm opens its pool exactly
// like the pristine harness.
func (r *backlogSegRecorder) traceTarget() *backlogSegRecorder {
	if r.collect {
		return r
	}
	return nil
}

// currentSeq returns the event sequence currently being processed (-1
// outside an event).
func (r *backlogSegRecorder) currentSeq() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.curSeq
}

// recordPublish appends one publish-cycle row (main goroutine).
func (r *backlogSegRecorder) recordPublish(row backlogSegPublishRow) {
	r.pubMu.Lock()
	defer r.pubMu.Unlock()
	r.publishes = append(r.publishes, row)
}

// recordSample appends one monitor tick (monitor goroutine). Samples beyond
// the frozen cap are dropped and counted, never grown without bound.
func (r *backlogSegRecorder) recordSample(row backlogSegSampleRow) {
	r.sampleMu.Lock()
	defer r.sampleMu.Unlock()
	if len(r.samples) >= backlogMaxSamples {
		r.sampleDrops++
		return
	}
	r.samples = append(r.samples, row)
}

// recordSampleErr records one monitor sampling failure (bounded).
func (r *backlogSegRecorder) recordSampleErr(text string) {
	r.sampleMu.Lock()
	defer r.sampleMu.Unlock()
	if len(r.sampleErrText) < 20 {
		r.sampleErrText = append(r.sampleErrText, text)
	}
}

// sealPairs closes the pairing bookkeeping after the consumer stopped: a span
// still open at flush means Run returned mid-span and is counted as unpaired.
func (r *backlogSegRecorder) sealPairs() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pollOpen || r.procOpen || r.markOpen || r.lagOpen {
		r.drops.LoopUnpaired++
	}
	r.pollOpen, r.procOpen, r.markOpen, r.lagOpen = false, false, false, false
}

// snapshotDrops returns the drop counters (the unpaired count is sealed
// first, so an open span at flush is never silently dropped).
func (r *backlogSegRecorder) snapshotDrops() backlogSegDrops {
	r.sealPairs()
	r.mu.Lock()
	drops := r.drops
	r.mu.Unlock()
	r.sampleMu.Lock()
	drops.SamplesDropped = r.sampleDrops
	r.sampleMu.Unlock()
	return drops
}

// snapshotCounts returns the frozen cross-check tally. sql_spans and
// sql_unattributed are derived from the stored rows, so they are exact at
// flush time (which runs after Run returned).
func (r *backlogSegRecorder) snapshotCounts() backlogSegCounts {
	r.mu.Lock()
	defer r.mu.Unlock()
	counts := r.counts
	counts.SQLSpans = len(r.sqls)
	counts.SQLUnattributed = 0
	for _, row := range r.sqls {
		if row.Seq < 1 {
			counts.SQLUnattributed++
		}
	}
	return counts
}

// sqlEventSeqs counts the distinct event sequences that produced at least one
// statement: the SQL-side witness of how many records were processed.
func (r *backlogSegRecorder) sqlEventSeqs() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := make(map[int64]struct{}, len(r.sqls))
	for _, row := range r.sqls {
		if row.Seq >= 1 {
			seen[row.Seq] = struct{}{}
		}
	}
	return len(seen)
}

// loopRows returns a copy of the recorded loop rows.
func (r *backlogSegRecorder) loopRows() []backlogSegLoopRow {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]backlogSegLoopRow(nil), r.loops...)
}

// sqlRows returns a copy of the recorded SQL rows.
func (r *backlogSegRecorder) sqlRows() []backlogSegSQLRow {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]backlogSegSQLRow(nil), r.sqls...)
}

// publishRows returns a copy of the recorded publish rows.
func (r *backlogSegRecorder) publishRows() []backlogSegPublishRow {
	r.pubMu.Lock()
	defer r.pubMu.Unlock()
	return append([]backlogSegPublishRow(nil), r.publishes...)
}

// sampleRows returns a copy of the recorded monitor samples.
func (r *backlogSegRecorder) sampleRows() []backlogSegSampleRow {
	r.sampleMu.Lock()
	defer r.sampleMu.Unlock()
	return append([]backlogSegSampleRow(nil), r.samples...)
}

// sampleErrs returns a copy of the bounded monitor failure list.
func (r *backlogSegRecorder) sampleErrs() []string {
	r.sampleMu.Lock()
	defer r.sampleMu.Unlock()
	return append([]string(nil), r.sampleErrText...)
}

// backlogSegFillPollCounts returns a copy of rows with every poll row's batch
// record count backfilled — the frozen loop.csv i1 column of a poll row. Batch
// k owns exactly the process spans that started inside its window
// [pollDone_k, pollStart_k+1); the last batch runs to the end of the rows,
// which the caller seals after Run returned. The consumer loop is single
// goroutine and appends in order, so one ordered pass suffices; a poll row
// that observed no record keeps 0.
func backlogSegFillPollCounts(rows []backlogSegLoopRow) []backlogSegLoopRow {
	out := append([]backlogSegLoopRow(nil), rows...)
	open := -1
	windowStart := int64(0)
	for i := range out {
		switch out[i].Kind {
		case backlogSegKindPoll:
			open = i
			windowStart = out[i].TUs + out[i].DurUs
		case backlogSegKindProcess:
			if open >= 0 && out[i].TUs >= windowStart {
				out[open].I1++
			}
		}
	}
	return out
}

// backlogSegLoopHeader and friends are the frozen CSV headers (format v1).
const (
	backlogSegLoopHeader    = "kind,seq,t_us,dur_us,i1,i2,s1"
	backlogSegSQLHeader     = "seq,t_us,dur_us,kind,sql,err"
	backlogSegPublishHeader = "iter,t_count_us,count_dur_us,pending_seen,t_publish_us,publish_dur_us,claimed,acked,released,blocked"
	backlogSegSamplesHeader = "t_us,pending,published,applied,ledger,progress,acquire_count,empty_acquire_count,acquire_duration_us,canceled_acquire_count,total_conns,idle_conns"
)

// writeLoopCSV writes loop.csv with every poll row's batch record count
// backfilled (the recorded spans themselves are never mutated).
func (r *backlogSegRecorder) writeLoopCSV(path string) error {
	rows := backlogSegFillPollCounts(r.loopRows())
	out := make([][]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.record())
	}
	return backlogSegWriteCSV(path, backlogSegLoopHeader, out)
}

// writeSQLCSV writes sql.csv.
func (r *backlogSegRecorder) writeSQLCSV(path string) error {
	rows := r.sqlRows()
	out := make([][]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.record())
	}
	return backlogSegWriteCSV(path, backlogSegSQLHeader, out)
}

// writePublishCSV writes publish.csv.
func (r *backlogSegRecorder) writePublishCSV(path string) error {
	rows := r.publishRows()
	out := make([][]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.record())
	}
	return backlogSegWriteCSV(path, backlogSegPublishHeader, out)
}

// writeSamplesCSV writes samples.csv.
func (r *backlogSegRecorder) writeSamplesCSV(path string) error {
	rows := r.sampleRows()
	out := make([][]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.record())
	}
	return backlogSegWriteCSV(path, backlogSegSamplesHeader, out)
}

// backlogSegWriteCSV writes header plus rows as one RFC 4180 CSV file.
func backlogSegWriteCSV(path, header string, rows [][]string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	writer := csv.NewWriter(file)
	writeErr := writer.Write(strings.Split(header, ","))
	for _, row := range rows {
		if writeErr != nil {
			break
		}
		writeErr = writer.Write(row)
	}
	writer.Flush()
	if err := writer.Error(); err != nil && writeErr == nil {
		writeErr = err
	}
	if err := file.Close(); err != nil && writeErr == nil {
		writeErr = err
	}
	return writeErr
}

// backlogSegCompactSQL flattens a statement onto one line (whitespace runs
// collapse to single spaces) and truncates it to at most backlogSegSQLMaxLen
// bytes without splitting a rune.
func backlogSegCompactSQL(sql string) string {
	flat := backlogSegFlatten(sql)
	if len(flat) <= backlogSegSQLMaxLen {
		return flat
	}
	cut := backlogSegSQLMaxLen
	for cut > 0 && !utf8.RuneStart(flat[cut]) {
		cut--
	}
	return flat[:cut]
}

// backlogSegFlatten collapses every whitespace run to one space.
func backlogSegFlatten(sql string) string { return strings.Join(strings.Fields(sql), " ") }

// backlogSegSQLKind classifies one statement by its full compacted text
// against the consumer's own SQL constants (consumer.go, quarantine.go) and
// the harness effect table. Classification never uses the truncated column
// text, so a trailing "FOR UPDATE" stays visible.
func backlogSegSQLKind(sql string) string {
	upper := strings.ToUpper(backlogSegFlatten(sql))
	switch {
	case upper == "BEGIN":
		return backlogSegSQLBegin
	case upper == "COMMIT":
		return backlogSegSQLCommit
	case upper == "ROLLBACK":
		return backlogSegSQLRollback
	case strings.HasPrefix(upper, "INSERT INTO CONSUMER_INBOX"):
		return backlogSegSQLInboxInsert
	case strings.HasPrefix(upper, "SELECT MAX_VERSION FROM CONSUMER_VERSIONS"):
		if strings.Contains(upper, "FOR UPDATE") {
			return backlogSegSQLVersionRead
		}
		return backlogSegSQLVersionProbe
	case strings.HasPrefix(upper, "UPDATE CONSUMER_VERSIONS"),
		strings.HasPrefix(upper, "INSERT INTO CONSUMER_VERSIONS"):
		return backlogSegSQLVersionUpsert
	case strings.HasPrefix(upper, "INSERT INTO CONSUMER_PROGRESS"):
		return backlogSegSQLProgressUpdate
	case strings.HasPrefix(upper, "INSERT INTO CONSUMER_QUARANTINE"):
		return backlogSegSQLQuarantineInsert
	case strings.HasPrefix(upper, "INSERT INTO "+strings.ToUpper(backlogEffectTable)):
		return backlogSegSQLEffectInsert
	default:
		return backlogSegSQLOther
	}
}

// backlogSegTraceKey marks a context whose statements are attributed to the
// consumer's event sequence.
type backlogSegTraceKey struct{}

// backlogSegMarked returns ctx marked for statement tracing. Only the
// consumer's run context is marked; every other caller keeps an unmarked
// context and pays nothing.
func backlogSegMarked(ctx context.Context) context.Context {
	return context.WithValue(ctx, backlogSegTraceKey{}, true)
}

// backlogSegStartKey carries the per-statement start state.
type backlogSegStartKey struct{}

// backlogSegQueryStart is one statement's start state: the text, the start
// time and the event sequence it runs under.
type backlogSegQueryStart struct {
	sql   string
	start time.Time
	seq   int64
}

// backlogSegTracer implements pgx.QueryTracer for the harness pool. It never
// alters query semantics: an existing tracer (next) is forwarded unchanged
// and only marked statements are recorded.
type backlogSegTracer struct {
	rec  *backlogSegRecorder
	next pgx.QueryTracer
}

// TraceQueryStart forwards to the existing tracer chain, then stores the
// start state for a marked context. An unmarked context is returned exactly
// as it arrived: no value store, no allocation, no record.
func (t *backlogSegTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if t.next != nil {
		ctx = t.next.TraceQueryStart(ctx, conn, data)
	}
	if ctx.Value(backlogSegTraceKey{}) == nil {
		return ctx
	}
	return context.WithValue(ctx, backlogSegStartKey{}, backlogSegQueryStart{
		sql:   data.SQL,
		start: time.Now(),
		seq:   t.rec.currentSeq(),
	})
}

// TraceQueryEnd records one statement when the context came from
// TraceQueryStart (marked), then forwards to the existing tracer chain.
// Failed statements are recorded too (err=1).
func (t *backlogSegTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if start, ok := ctx.Value(backlogSegStartKey{}).(backlogSegQueryStart); ok {
		t.rec.recordSQL(start, data.Err != nil)
	}
	if t.next != nil {
		t.next.TraceQueryEnd(ctx, conn, data)
	}
}

// backlogSegObserver wraps the harness consumer observer so the applied
// callback is timestamped inside the event scope. Every method forwards to
// the wrapped observer unchanged: the wrapper only observes.
type backlogSegObserver struct {
	next events.ConsumerObserver
	rec  *backlogSegRecorder
}

func (o *backlogSegObserver) ObserveConsumerApplied() {
	o.rec.recordAppliedCB()
	o.next.ObserveConsumerApplied()
}

func (o *backlogSegObserver) ObserveConsumerRetry(failureClass string) {
	o.next.ObserveConsumerRetry(failureClass)
}

func (o *backlogSegObserver) ObserveConsumerQuarantine(failureClass string) {
	o.next.ObserveConsumerQuarantine(failureClass)
}

func (o *backlogSegObserver) SetConsumerLag(consumerName string, partition int, lagSeconds float64, lagMessages int64) {
	o.next.SetConsumerLag(consumerName, partition, lagSeconds, lagMessages)
}

func (o *backlogSegObserver) ObserveEventReplay(opKind, consumerName string) {
	o.next.ObserveEventReplay(opKind, consumerName)
}

func (o *backlogSegObserver) SetConsumerReplayClock(consumerName string, unixSeconds float64) {
	o.next.SetConsumerReplayClock(consumerName, unixSeconds)
}

// TestBacklogDrainSegRecorder pins the recorder and tracer contract without
// any Docker dependency: paired spans become one row each, sequence numbers
// are annotated on the statements that ran inside an event, unpaired spans
// are counted instead of silently accepted, the tracer records only marked
// contexts, and the CSV writer round-trips every frozen file.
func TestBacklogDrainSegRecorder(t *testing.T) {
	epoch := time.Now()
	rec := newBacklogSegRecorder(epoch, true)

	// --- paired spans: poll, applied callback, process, mark, lag, rebalance
	rec.PollStart()
	rec.PollDone()
	rec.ProcessStart(2, 41)
	rec.recordAppliedCB()
	rec.ProcessDone(events.ProcessResult{Outcome: events.OutcomeApplied}, nil)
	rec.ProcessStart(2, 42)
	rec.ProcessDone(events.ProcessResult{Outcome: events.OutcomeDuplicate}, nil)
	rec.ProcessStart(3, 7)
	rec.ProcessDone(events.ProcessResult{}, fmt.Errorf("db down"))
	rec.MarkStart(2, 41)
	rec.MarkDone()
	rec.RefreshLagStart()
	rec.RefreshLagDone()
	rec.RebalanceDone()

	rows := rec.loopRows()
	wantKinds := []string{
		backlogSegKindPoll, backlogSegKindAppliedCB, backlogSegKindProcess, backlogSegKindProcess,
		backlogSegKindProcess, backlogSegKindMark, backlogSegKindLag, backlogSegKindRebalance,
	}
	if len(rows) != len(wantKinds) {
		t.Fatalf("loop rows = %d, want %d (%+v)", len(rows), len(wantKinds), rows)
	}
	for i, want := range wantKinds {
		if rows[i].Kind != want {
			t.Fatalf("loop row %d kind = %q, want %q", i, rows[i].Kind, want)
		}
	}
	if rows[0].I1 != 0 || rows[0].DurUs < 0 {
		t.Fatalf("raw poll row = %+v, want no batch count before the archive backfill", rows[0])
	}
	if rows[1].Seq != 1 || rows[1].DurUs != 0 {
		t.Fatalf("applied_cb row = %+v, want seq=1 and dur=0", rows[1])
	}
	if rows[2].Seq != 1 || rows[2].I1 != 2 || rows[2].I2 != 41 || rows[2].S1 != string(events.OutcomeApplied) {
		t.Fatalf("first process row = %+v, want seq=1 partition=2 offset=41 outcome=applied", rows[2])
	}
	if rows[3].Seq != 2 || rows[3].S1 != string(events.OutcomeDuplicate) {
		t.Fatalf("second process row = %+v, want seq=2 outcome=duplicate", rows[3])
	}
	if rows[4].Seq != 3 || rows[4].S1 != "" {
		t.Fatalf("error process row = %+v, want seq=3 and empty outcome", rows[4])
	}
	if rows[5].Seq != 3 || rows[5].I1 != 2 || rows[5].I2 != 41 {
		t.Fatalf("mark row = %+v, want seq=3 partition=2 offset=41", rows[5])
	}
	if rows[6].Seq != 0 {
		t.Fatalf("lag row = %+v, want seq=0", rows[6])
	}
	if rows[7].Seq != 0 || rows[7].DurUs != 0 {
		t.Fatalf("rebalance row = %+v, want a point event", rows[7])
	}
	for i, row := range rows {
		if row.TUs < 0 || row.DurUs < 0 {
			t.Fatalf("loop row %d has a negative offset: %+v", i, row)
		}
	}

	want := backlogSegCounts{
		ObservedProcess: 3, ObservedApplied: 1, ObservedDuplicate: 1, ObservedErrors: 1, AppliedCB: 1,
	}
	if counts := rec.snapshotCounts(); counts != want {
		t.Fatalf("counts = %+v, want %+v", counts, want)
	}

	// --- unpaired spans are counted, never silently absorbed ----------------
	rec.PollStart()
	rec.PollStart() // a second start without a done
	rec.PollDone()
	rec.MarkDone()       // a done without a start
	rec.RefreshLagDone() // ditto
	rows = rec.loopRows()
	if len(rows) != len(wantKinds)+1 || rows[len(rows)-1].Kind != backlogSegKindPoll {
		t.Fatalf("loop rows after the extra poll = %d, want %d", len(rows), len(wantKinds)+1)
	}
	drops := rec.snapshotDrops()
	if drops.LoopUnpaired != 3 || drops.SamplesDropped != 0 {
		t.Fatalf("drops = %+v, want loop_unpaired=3 samples_dropped=0", drops)
	}

	// --- each poll row's batch record count is derived from its window ------
	// The seam passes no fetched batch (it names no broker-client type), so
	// the frozen loop.csv i1 column of a poll row is backfilled from the
	// process spans that started inside that batch's window.
	batches := newBacklogSegRecorder(epoch, true)
	batches.PollStart()
	batches.PollDone() // batch 1
	batches.ProcessStart(0, 0)
	batches.ProcessDone(events.ProcessResult{Outcome: events.OutcomeApplied}, nil)
	batches.ProcessStart(0, 1)
	batches.ProcessDone(events.ProcessResult{Outcome: events.OutcomeDuplicate}, nil)
	batches.PollStart()
	batches.PollDone() // batch 2
	batches.ProcessStart(0, 2)
	batches.ProcessDone(events.ProcessResult{Outcome: events.OutcomeApplied}, nil)
	batches.PollStart()
	batches.PollDone() // batch 3: no records
	rawRows := batches.loopRows()
	if rawRows[0].I1 != 0 {
		t.Fatalf("recorded poll span carries a batch count: %+v", rawRows[0])
	}
	filled := backlogSegFillPollCounts(rawRows)
	wantBatchCounts := []int64{2, 1, 0}
	var gotBatchCounts []int64
	for _, row := range filled {
		if row.Kind == backlogSegKindPoll {
			gotBatchCounts = append(gotBatchCounts, row.I1)
		}
	}
	if len(gotBatchCounts) != len(wantBatchCounts) {
		t.Fatalf("poll rows = %v, want %v", gotBatchCounts, wantBatchCounts)
	}
	for i, want := range wantBatchCounts {
		if gotBatchCounts[i] != want {
			t.Fatalf("poll batch %d records = %d, want %d (all: %v)", i+1, gotBatchCounts[i], want, gotBatchCounts)
		}
	}
	if rawRows[0].I1 != 0 || rawRows[1].I1 != 0 {
		t.Fatalf("the backfill mutated the recorded spans: %+v", rawRows)
	}

	// The main recorder's own batches: one poll holding all three process
	// spans, then the extra poll of the pairing case with none.
	mainFilled := backlogSegFillPollCounts(rows)
	var mainCounts []int64
	for _, row := range mainFilled {
		if row.Kind == backlogSegKindPoll {
			mainCounts = append(mainCounts, row.I1)
		}
	}
	if len(mainCounts) != 2 || mainCounts[0] != 3 || mainCounts[1] != 0 {
		t.Fatalf("main poll batch counts = %v, want [3 0]", mainCounts)
	}

	// --- the tracer records only marked contexts ----------------------------
	off := newBacklogSegRecorder(epoch, true)
	tracer := &backlogSegTracer{rec: off}
	plain := context.Background()
	if got := tracer.TraceQueryStart(plain, nil, pgx.TraceQueryStartData{SQL: "begin"}); got != plain {
		t.Fatalf("unmarked TraceQueryStart did not forward the context unchanged: %v", got)
	}
	tracer.TraceQueryEnd(plain, nil, pgx.TraceQueryEndData{})
	if rows := off.sqlRows(); len(rows) != 0 {
		t.Fatalf("unmarked statement was recorded: %+v", rows)
	}

	outside := tracer.TraceQueryStart(backlogSegMarked(context.Background()), nil,
		pgx.TraceQueryStartData{SQL: "SELECT 1"})
	tracer.TraceQueryEnd(outside, nil, pgx.TraceQueryEndData{})
	inside := tracer.TraceQueryStart(backlogSegMarked(context.Background()), nil, pgx.TraceQueryStartData{
		SQL: `SELECT max_version FROM consumer_versions
WHERE consumer_name = $1 AND aggregate_type = $2 AND aggregate_id = $3
FOR UPDATE`,
	})
	tracer.TraceQueryEnd(inside, nil, pgx.TraceQueryEndData{Err: fmt.Errorf("boom")})
	sqlRows := off.sqlRows()
	if len(sqlRows) != 2 {
		t.Fatalf("marked statements recorded = %d, want 2 (%+v)", len(sqlRows), sqlRows)
	}
	if sqlRows[0].Seq != -1 || sqlRows[0].Kind != backlogSegSQLOther || sqlRows[0].Err != 0 {
		t.Fatalf("unattributed row = %+v, want seq=-1 kind=other err=0", sqlRows[0])
	}
	if sqlRows[1].Kind != backlogSegSQLVersionRead || sqlRows[1].Seq != -1 || sqlRows[1].Err != 1 {
		t.Fatalf("version read row = %+v, want kind=version_read seq=-1 err=1", sqlRows[1])
	}
	if strings.Contains(sqlRows[1].SQL, "\n") || len(sqlRows[1].SQL) > backlogSegSQLMaxLen {
		t.Fatalf("sql column is not compacted/bounded: %q", sqlRows[1].SQL)
	}

	// A statement inside an event carries that event's sequence.
	off.ProcessStart(0, 0)
	inEvent := tracer.TraceQueryStart(backlogSegMarked(context.Background()), nil,
		pgx.TraceQueryStartData{SQL: "INSERT INTO consumer_inbox (consumer_name) VALUES ($1) ON CONFLICT DO NOTHING"})
	tracer.TraceQueryEnd(inEvent, nil, pgx.TraceQueryEndData{})
	off.ProcessDone(events.ProcessResult{Outcome: events.OutcomeApplied}, nil)
	sqlRows = off.sqlRows()
	last := sqlRows[len(sqlRows)-1]
	if last.Seq != 1 || last.Kind != backlogSegSQLInboxInsert {
		t.Fatalf("in-event statement = %+v, want seq=1 kind=%s", last, backlogSegSQLInboxInsert)
	}
	if got := off.sqlEventSeqs(); got != 1 {
		t.Fatalf("sql event sequences = %d, want 1", got)
	}
	if got := off.snapshotCounts(); got.SQLSpans != 3 || got.SQLUnattributed != 2 {
		t.Fatalf("sql counts = %+v, want sql_spans=3 sql_unattributed=2", got)
	}

	// --- the OFF arm keeps the tallies but stores no span row ---------------
	offArm := newBacklogSegRecorder(epoch, false)
	offArm.PollStart()
	offArm.PollDone()
	offArm.ProcessStart(0, 5)
	offArm.recordAppliedCB()
	offArm.ProcessDone(events.ProcessResult{Outcome: events.OutcomeApplied}, nil)
	offArm.MarkStart(0, 5)
	offArm.MarkDone()
	offArm.RebalanceDone()
	offArm.RefreshLagStart()
	offArm.RefreshLagDone()
	if rows := offArm.loopRows(); len(rows) != 0 {
		t.Fatalf("OFF arm stored loop rows: %+v", rows)
	}
	if got := offArm.snapshotCounts(); got.ObservedProcess != 1 || got.ObservedApplied != 1 || got.AppliedCB != 1 || got.SQLSpans != 0 {
		t.Fatalf("OFF arm counts = %+v, want process=1 applied=1 applied_cb=1 sql_spans=0", got)
	}
	if got := offArm.snapshotDrops(); got.LoopUnpaired != 0 {
		t.Fatalf("OFF arm unpaired = %d, want 0", got.LoopUnpaired)
	}

	// --- SQL kind classification over the consumer's own statement texts ----
	kinds := []struct {
		sql  string
		want string
	}{
		{"begin", backlogSegSQLBegin},
		{"COMMIT", backlogSegSQLCommit},
		{"rollback", backlogSegSQLRollback},
		{"\nINSERT INTO consumer_inbox (\n\tconsumer_name, event_id\n) VALUES ($1, $2)\nON CONFLICT (consumer_name, event_id) DO NOTHING", backlogSegSQLInboxInsert},
		{"\nSELECT max_version FROM consumer_versions\nWHERE consumer_name = $1\nFOR UPDATE", backlogSegSQLVersionRead},
		{"SELECT max_version FROM consumer_versions WHERE aggregate_id = $3", backlogSegSQLVersionProbe},
		{"\nUPDATE consumer_versions\nSET max_version = $4, updated_at = now()\nWHERE consumer_name = $1", backlogSegSQLVersionUpsert},
		{"INSERT INTO consumer_versions (consumer_name) VALUES ($1) ON CONFLICT DO NOTHING", backlogSegSQLVersionUpsert},
		{"\nINSERT INTO consumer_progress (consumer_name, topic, partition, next_offset)\nVALUES ($1, $2, $3, $4)", backlogSegSQLProgressUpdate},
		{"\nINSERT INTO consumer_quarantine (\n\tconsumer_name, event_id\n) VALUES ($1, $2)", backlogSegSQLQuarantineInsert},
		{"INSERT INTO " + backlogEffectTable + " (event_id, event_type) VALUES ($1, $2)", backlogSegSQLEffectInsert},
		{"SELECT pg_database_size(current_database())", backlogSegSQLOther},
	}
	for _, k := range kinds {
		if got := backlogSegSQLKind(k.sql); got != k.want {
			t.Errorf("backlogSegSQLKind(%q) = %q, want %q", backlogSegFlatten(k.sql), got, k.want)
		}
	}

	// --- the frozen record formats -----------------------------------------
	// The extended report embeds the pristine record: its field names must
	// stay at the top level (never nested) and the segmentation fields are
	// appended beside them.
	reportBody, err := json.Marshal(backlogSegReport{})
	if err != nil {
		t.Fatalf("marshal backlogSegReport: %v", err)
	}
	var flat map[string]json.RawMessage
	if err := json.Unmarshal(reportBody, &flat); err != nil {
		t.Fatalf("unmarshal backlogSegReport: %v", err)
	}
	for _, key := range []string{
		"harness", "n", "publish_cycles", "pool_stats_after", "pending_curve",
		"seg_enabled", "applied_at_publish_done", "publish_last_cycle_end_seconds",
		"pending_zero_observed_seconds", "consume_confirm_seconds", "last_commit_end_seconds",
		"last_applied_cb_seconds", "process_observed", "sql_span_count", "seg_drops",
		"lag_final", "pool_final",
	} {
		if _, ok := flat[key]; !ok {
			t.Errorf("report JSON misses the top-level key %q", key)
		}
	}
	if _, nested := flat["backlogReport"]; nested {
		t.Errorf("the embedded pristine report was nested instead of flattened")
	}

	// meta/anchors keep the frozen field names and never carry a credential:
	// the harness writes images and parameter bounds, never a DSN, broker
	// address or credential.
	anchorsBody, err := json.Marshal(backlogSegAnchors{})
	if err != nil {
		t.Fatalf("marshal backlogSegAnchors: %v", err)
	}
	metaBody, err := json.Marshal(backlogSegMeta{})
	if err != nil {
		t.Fatalf("marshal backlogSegMeta: %v", err)
	}
	var anchorsFlat map[string]json.RawMessage
	if err := json.Unmarshal(anchorsBody, &anchorsFlat); err != nil {
		t.Fatalf("unmarshal backlogSegAnchors: %v", err)
	}
	for _, key := range []string{"drain_start_us", "publish", "consume", "counts", "lag_final", "seg_enabled"} {
		if _, ok := anchorsFlat[key]; !ok {
			t.Errorf("anchors JSON misses the top-level key %q", key)
		}
	}
	var metaFlat map[string]json.RawMessage
	if err := json.Unmarshal(metaBody, &metaFlat); err != nil {
		t.Fatalf("unmarshal backlogSegMeta: %v", err)
	}
	for _, key := range []string{
		"harness", "mode", "started_at_wall", "finished_at_wall", "epoch_wall", "go_version",
		"gomaxprocs", "host_cpus", "vcs_revision", "vcs_modified", "n", "config", "seg_enabled", "drops",
	} {
		if _, ok := metaFlat[key]; !ok {
			t.Errorf("meta JSON misses the top-level key %q", key)
		}
	}
	lowered := strings.ToLower(string(reportBody) + string(anchorsBody) + string(metaBody))
	for _, forbidden := range []string{"dsn", "password", "passwd", "username", "brokers", "sslmode", "secret"} {
		if strings.Contains(lowered, forbidden) {
			t.Errorf("the evidence records carry the forbidden material %q", forbidden)
		}
	}

	// --- CSV round trip -----------------------------------------------------
	dir := t.TempDir()
	rec.recordPublish(backlogSegPublishRow{Iter: 1, PendingSeen: 250, Claimed: 250, Acked: 250})
	rec.recordPublish(backlogSegPublishRow{Iter: 2})
	rec.recordSample(backlogSegSampleRow{TUs: 1000, Pending: 0, Published: 250, Applied: 250, Ledger: 250, Progress: 250})
	paths := map[string]string{
		"loop.csv":    filepath.Join(dir, "loop.csv"),
		"sql.csv":     filepath.Join(dir, "sql.csv"),
		"publish.csv": filepath.Join(dir, "publish.csv"),
		"samples.csv": filepath.Join(dir, "samples.csv"),
	}
	if err := rec.writeLoopCSV(paths["loop.csv"]); err != nil {
		t.Fatalf("writeLoopCSV: %v", err)
	}
	if err := off.writeSQLCSV(paths["sql.csv"]); err != nil {
		t.Fatalf("writeSQLCSV: %v", err)
	}
	if err := rec.writePublishCSV(paths["publish.csv"]); err != nil {
		t.Fatalf("writePublishCSV: %v", err)
	}
	if err := rec.writeSamplesCSV(paths["samples.csv"]); err != nil {
		t.Fatalf("writeSamplesCSV: %v", err)
	}
	wantHeaders := map[string]string{
		"loop.csv":    backlogSegLoopHeader,
		"sql.csv":     backlogSegSQLHeader,
		"publish.csv": backlogSegPublishHeader,
		"samples.csv": backlogSegSamplesHeader,
	}
	wantRows := map[string]int{"loop.csv": len(wantKinds) + 1, "sql.csv": 3, "publish.csv": 2, "samples.csv": 1}
	written := map[string][][]string{}
	for name, path := range paths {
		written[name] = backlogSegReadCSV(t, path, wantHeaders[name], wantRows[name])
	}
	// The written loop.csv carries the backfilled batch record counts (i1 is
	// column 5 of the frozen header), never the raw zero of the span rows.
	var writtenPollCounts []string
	for _, row := range written["loop.csv"] {
		if row[0] == backlogSegKindPoll {
			writtenPollCounts = append(writtenPollCounts, row[4])
		}
	}
	if len(writtenPollCounts) != 2 || writtenPollCounts[0] != "3" || writtenPollCounts[1] != "0" {
		t.Fatalf("written loop.csv poll counts = %v, want [3 0]", writtenPollCounts)
	}
	// The last publish row is the boundary row: pending=0 and no outcome.
	publishRows := rec.publishRows()
	boundary := publishRows[len(publishRows)-1]
	if boundary.PendingSeen != 0 || boundary.Claimed != 0 || boundary.TPublishUs != 0 || boundary.PublishDur != 0 {
		t.Fatalf("boundary publish row = %+v, want pending=0 and zeroed publish fields", boundary)
	}
}

// backlogSegReadCSV asserts one written CSV file (header equality, row count
// and row widths) and returns its rows for cell-level assertions.
func backlogSegReadCSV(t *testing.T, path, header string, rows int) [][]string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer file.Close()
	got, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if len(got) == 0 {
		t.Fatalf("%s is empty", path)
	}
	if strings.Join(got[0], ",") != header {
		t.Fatalf("%s header = %q, want %q", path, strings.Join(got[0], ","), header)
	}
	if len(got)-1 != rows {
		t.Fatalf("%s rows = %d, want %d", path, len(got)-1, rows)
	}
	for i, row := range got {
		if len(row) != len(got[0]) {
			t.Fatalf("%s row %d width = %d, want %d", path, i, len(row), len(got[0]))
		}
	}
	return got[1:]
}
