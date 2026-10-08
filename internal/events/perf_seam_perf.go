//go:build perf

package events

import "sync/atomic"

// This file is the perf-tagged half of the consumer-side segmented
// measurement seam (frozen API; the ordinary build resolves every symbol here
// to the no-op in perf_seam_none.go). It compiles only under `-tags perf` and
// stays inert until PerfEnableConsumerSeg installs a sink, so even a
// perf-tagged binary behaves like production until the harness arms it.
//
// Observation discipline (frozen): every seam call is observation only. A
// sink MUST NOT block the consumer, MUST NOT influence control flow and MUST
// NOT touch durable state; a slow or panicking sink is a harness bug, never a
// production concern. The interface is documented as safe for concurrent use
// because a future measurement may observe more than one goroutine; today
// every call point runs on the single consumer goroutine of
// KafkaConsumer.Run (and on the observer callbacks that goroutine invokes),
// so a sink may keep unsynchronized per-span state.
//
// Cost discipline: each call point is one atomic load plus a nil check. With
// no sink installed (the production-identical state) nothing else happens:
// the values the loop already holds are discarded, no allocation is made and
// no branch of the loop's own control flow is altered.
//
// Boundary discipline: like its ordinary-build twin, this file names no
// broker-client type and imports no broker package. The poll seam takes no
// fetched batch; the record count of each poll batch is derived by the
// measurement harness from the process spans it observes (T016/T085).

// PerfConsumerSegSink receives the consumer runtime's segment observations:
// one start/done pair per poll attempt, per processed record, per offset mark
// and per lag refresh, plus one completion event per settled batch. It is
// implemented by the measurement harness, never by production code.
type PerfConsumerSegSink interface {
	PollStart()
	PollDone()
	ProcessStart(partition int32, offset int64)
	ProcessDone(result ProcessResult, err error)
	MarkStart(partition int32, offset int64)
	MarkDone()
	RebalanceDone()
	RefreshLagStart()
	RefreshLagDone()
}

// perfConsumerSegSink is the armed sink; nil means collection is off. The
// whole switch is one atomic pointer, so arming or disarming never races with
// an in-flight call point.
var perfConsumerSegSink atomic.Pointer[PerfConsumerSegSink]

// PerfEnableConsumerSeg arms the seam with sink. A nil sink disarms it (the
// same state as PerfDisableConsumerSeg). The sink is called on the consumer
// goroutine and MUST be safe for concurrent use.
func PerfEnableConsumerSeg(sink PerfConsumerSegSink) {
	if sink == nil {
		perfConsumerSegSink.Store(nil)
		return
	}
	perfConsumerSegSink.Store(&sink)
}

// PerfDisableConsumerSeg disarms the seam: every call point returns to one
// atomic load and a nil check, and no further observation is recorded.
func PerfDisableConsumerSeg() { perfConsumerSegSink.Store(nil) }

// perfConsumerPollStart forwards the poll start to the armed sink.
func perfConsumerPollStart() {
	if sink := perfConsumerSegSink.Load(); sink != nil {
		(*sink).PollStart()
	}
}

// perfConsumerPollDone forwards the poll completion to the armed sink.
func perfConsumerPollDone() {
	if sink := perfConsumerSegSink.Load(); sink != nil {
		(*sink).PollDone()
	}
}

// perfConsumerProcessStart forwards the processed record's coordinates to the
// armed sink.
func perfConsumerProcessStart(partition int32, offset int64) {
	if sink := perfConsumerSegSink.Load(); sink != nil {
		(*sink).ProcessStart(partition, offset)
	}
}

// perfConsumerProcessDone forwards the record's outcome and transport error
// to the armed sink (the error may be nil; the result is meaningful only
// then).
func perfConsumerProcessDone(result ProcessResult, err error) {
	if sink := perfConsumerSegSink.Load(); sink != nil {
		(*sink).ProcessDone(result, err)
	}
}

// perfConsumerMarkStart forwards the marked record's coordinates to the armed
// sink.
func perfConsumerMarkStart(partition int32, offset int64) {
	if sink := perfConsumerSegSink.Load(); sink != nil {
		(*sink).MarkStart(partition, offset)
	}
}

// perfConsumerMarkDone forwards the offset mark completion to the armed sink.
func perfConsumerMarkDone() {
	if sink := perfConsumerSegSink.Load(); sink != nil {
		(*sink).MarkDone()
	}
}

// perfConsumerRebalanceDone forwards the settled-batch event (the rebalance
// gate was just released) to the armed sink.
func perfConsumerRebalanceDone() {
	if sink := perfConsumerSegSink.Load(); sink != nil {
		(*sink).RebalanceDone()
	}
}

// perfConsumerRefreshLagStart forwards the lag observation start to the armed
// sink.
func perfConsumerRefreshLagStart() {
	if sink := perfConsumerSegSink.Load(); sink != nil {
		(*sink).RefreshLagStart()
	}
}

// perfConsumerRefreshLagDone forwards the lag observation completion to the
// armed sink.
func perfConsumerRefreshLagDone() {
	if sink := perfConsumerSegSink.Load(); sink != nil {
		(*sink).RefreshLagDone()
	}
}
