//go:build perf

// seg_arms.go is the controlled four-condition, single-state query batch
// (perf/seg-normal): the arm vocabulary parsed from TXHARBOR_SEG_ARMS and the
// segment/statement collector that persists the frozen JSONL evidence shapes.
//
// The batch measures one normal query path under four controlled conditions
// (R = query-class limiter on/off, G = background runtimes on/off); it never
// implements performance tuning, never changes production defaults and never
// enables caching.
package perf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/db"
)

// segSegmentsFile is the frozen evidence file name for the segment/statement
// records of one arm directory.
const segSegmentsFile = "server_segments.jsonl"

// Arm is one controlled condition of the segment batch:
//
//	R{0|1} = query-class limiter off/on,
//	G{0|1} = background runtimes (event publisher + consumer) off/on,
//	":nocoll" = segment/statement collection off (overhead measurement).
type Arm struct {
	Name         string
	LimiterOn    bool
	BackgroundOn bool
	Collect      bool
}

// ParseSegArms parses the comma-separated arm list (for example
// "R0G0,R1G0,R0G1,R1G1:nocoll"). Array order is execution order. An invalid
// token is an error; an empty list is an error.
func ParseSegArms(raw string) ([]Arm, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("perf: empty arm list")
	}
	var arms []Arm
	for _, token := range strings.Split(raw, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			return nil, fmt.Errorf("perf: empty arm token in %q", raw)
		}
		name := token
		collect := true
		if idx := strings.IndexByte(token, ':'); idx >= 0 {
			switch token[idx+1:] {
			case "nocoll":
				collect = false
			default:
				return nil, fmt.Errorf("perf: invalid arm suffix in %q", token)
			}
			name = token[:idx]
		}
		if len(name) != 4 || name[0] != 'R' || name[2] != 'G' {
			return nil, fmt.Errorf("perf: invalid arm token %q (want R{0|1}G{0|1}[:nocoll])", token)
		}
		bit := func(b byte) (bool, error) {
			switch b {
			case '0':
				return false, nil
			case '1':
				return true, nil
			default:
				return false, fmt.Errorf("perf: invalid arm token %q (want R{0|1}G{0|1}[:nocoll])", token)
			}
		}
		limiterOn, err := bit(name[1])
		if err != nil {
			return nil, err
		}
		backgroundOn, err := bit(name[3])
		if err != nil {
			return nil, err
		}
		arms = append(arms, Arm{
			Name:         token,
			LimiterOn:    limiterOn,
			BackgroundOn: backgroundOn,
			Collect:      collect,
		})
	}
	return arms, nil
}

// segSinkLine is one frozen segment record (kind="seg"), written by the
// collector; the field order mirrors the frozen pattern.
type segSinkLine struct {
	Kind     string `json:"kind"`
	ID       uint64 `json:"id"`
	Seg      string `json:"seg"`
	Class    string `json:"class"`
	DurNS    int64  `json:"dur_ns"`
	AtUnixNS int64  `json:"at_unix_ns"`
	Method   string `json:"method"`
	Path     string `json:"path"`
}

// pgqSinkLine is one frozen statement record (kind="pgq").
type pgqSinkLine struct {
	Kind     string `json:"kind"`
	ID       uint64 `json:"id"`
	SQL      string `json:"sql"`
	DurNS    int64  `json:"dur_ns"`
	AtUnixNS int64  `json:"at_unix_ns"`
	Err      bool   `json:"err"`
}

// segCollector implements app.PerfSegSink and records the per-request segment
// timeline (server_total/admit/below_admit/...) and the PostgreSQL statement
// timeline of the traced pool. It is safe for concurrent use. Records keep
// arrival order so the seg/pgq interleaving is reconstructable.
type segCollector struct {
	mu    sync.Mutex
	lines []any
	segN  int
	pgN   int
}

// newSegCollector returns an empty collector.
func newSegCollector() *segCollector { return &segCollector{} }

// The collector is the perf segment sink the app seam publishes to.
var _ app.PerfSegSink = (*segCollector)(nil)

// Segment implements app.PerfSegSink.
func (c *segCollector) Segment(id uint64, seg, class string, dur time.Duration, at time.Time, method, path string) {
	line := segSinkLine{
		Kind:     "seg",
		ID:       id,
		Seg:      seg,
		Class:    class,
		DurNS:    int64(dur),
		AtUnixNS: at.UnixNano(),
		Method:   method,
		Path:     path,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, line)
	c.segN++
}

// RecordQuery receives one traced statement (db.PerfQueryRecord). The SQL text
// is written exactly as the tracer recorded it (already compacted by the db
// seam).
func (c *segCollector) RecordQuery(rec db.PerfQueryRecord) {
	line := pgqSinkLine{
		Kind:     "pgq",
		ID:       rec.ID,
		SQL:      rec.SQL,
		DurNS:    int64(rec.Dur),
		AtUnixNS: rec.At.UnixNano(),
		Err:      rec.Err,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, line)
	c.pgN++
}

// Counts returns the recorded segment/statement counts.
func (c *segCollector) Counts() (segs int, stmts int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.segN, c.pgN
}

// Flush writes server_segments.jsonl (one frozen record per line) into dir.
func (c *segCollector) Flush(dir string) error {
	c.mu.Lock()
	lines := make([]any, len(c.lines))
	copy(lines, c.lines)
	c.mu.Unlock()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("perf: create segment evidence dir: %w", err)
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for _, line := range lines {
		if err := encoder.Encode(line); err != nil {
			return fmt.Errorf("perf: encode segment record: %w", err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, segSegmentsFile), buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("perf: write %s: %w", segSegmentsFile, err)
	}
	return nil
}
