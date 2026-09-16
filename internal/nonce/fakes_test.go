// fakes_test.go holds the scripted querier used by the unit tests. These are
// EARLY-VALIDATION doubles only: no acceptance claim in 008 rests on them
// (quickstart §Environment; constitution XI).
package nonce

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeRow implements pgx.Row from scripted values (or an error).
type fakeRow struct {
	vals []any
	err  error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.vals) {
		return fmt.Errorf("fakeRow: %d destinations for %d values", len(dest), len(r.vals))
	}
	for i := range dest {
		dv := reflect.ValueOf(dest[i])
		if dv.Kind() != reflect.Ptr || dv.IsNil() {
			return fmt.Errorf("fakeRow: destination %d is not a non-nil pointer", i)
		}
		if r.vals[i] == nil {
			dv.Elem().Set(reflect.Zero(dv.Elem().Type()))
			continue
		}
		sv := reflect.ValueOf(r.vals[i])
		if !sv.Type().AssignableTo(dv.Elem().Type()) {
			if sv.Type().ConvertibleTo(dv.Elem().Type()) {
				sv = sv.Convert(dv.Elem().Type())
			} else {
				return fmt.Errorf("fakeRow: value %d type %s not assignable to %s", i, sv.Type(), dv.Elem().Type())
			}
		}
		dv.Elem().Set(sv)
	}
	return nil
}

// fakeQuerier records every statement and answers QueryRow through handler.
type fakeQuerier struct {
	mu      sync.Mutex
	execs   []string
	queries []string
	handler func(sql string, args []any) pgx.Row
	execErr func(sql string, args []any) error
}

func (f *fakeQuerier) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.mu.Lock()
	f.execs = append(f.execs, sql)
	f.mu.Unlock()
	if f.execErr != nil {
		if err := f.execErr(sql, args); err != nil {
			return pgconn.CommandTag{}, err
		}
	}
	return pgconn.NewCommandTag("SELECT 1"), nil
}

func (f *fakeQuerier) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	f.mu.Lock()
	f.queries = append(f.queries, sql)
	f.mu.Unlock()
	if f.handler == nil {
		return fakeRow{err: pgx.ErrNoRows}
	}
	return f.handler(sql, args)
}

// Query satisfies txQuerier; no current unit test scripts a multi-row query.
func (f *fakeQuerier) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return nil, fmt.Errorf("fakeQuerier: Query not scripted")
}

func (f *fakeQuerier) execCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.execs)
}

func (f *fakeQuerier) recordedExecs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.execs...)
}

func (f *fakeQuerier) recordedQueries() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.queries...)
}

// rows builds a fake row from literal scan values.
func rows(vals ...any) pgx.Row { return fakeRow{vals: vals} }

// noRows builds a pgx.ErrNoRows row.
func noRows() pgx.Row { return fakeRow{err: pgx.ErrNoRows} }
