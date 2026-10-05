//go:build linux && drill

// targetwriter_borrowedhealth_drillbridge_linux_test.go is the one small
// drill-only health adapter for the borrowed-writer run: a read-only wrapper
// that delegates to the ACTUAL borrowed lock's Health under the lock's own
// serialization. It adds no authority, no constructor, no health boolean, no
// JSON/Cmd/Wait grants and no production API.
package recovery

import (
	"context"
	"errors"
)

// Health is the read-only health adapter of a factory-created borrowed run. It
// requires a complete factory binding (the private immutable lock and the
// phase-1 SQL capture) and then delegates to the real TargetLock.Health with
// the caller's bounded context; the lock's own mutex owns serialization, so
// this wrapper never holds r.lock.mu itself and never recurses. Errors are
// returned unchanged (no context hiding, no raw DSN material) so a gate can
// apply its own short timeout before first release or continuous monitoring.
func (r *DrillBorrowedWriterRun) Health(ctx context.Context) error {
	if r == nil || r.lock == nil {
		return errors.New("borrowed writer run health is unavailable: not a factory-created run")
	}
	if !r.binding.SQLFactsComplete() {
		return errors.New("borrowed writer run health is unavailable: factory SQL capture is incomplete")
	}
	if ctx == nil {
		return errors.New("borrowed writer run health requires a bounded context")
	}
	return r.lock.Health(ctx)
}
