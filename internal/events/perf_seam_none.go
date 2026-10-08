//go:build !perf

package events

// This file is the ordinary-build half of the consumer-side segmented
// measurement seam (frozen API; the perf-tagged half lives in
// perf_seam_perf.go and is compiled only under `-tags perf`). Every symbol is
// a no-op: the call points in KafkaConsumer.Run resolve to zero-logic
// functions, so an ordinary build keeps its exact behavior with no branch,
// no allocation and no evaluation beyond the values the loop already holds.
//
// The seam deliberately names no broker-client type and imports no broker
// package: it observes the loop through plain coordinates (partition, offset,
// outcome), which keeps the broker client confined to the two runtime files
// that are allowed to carry it (T016/T085). The poll seam therefore takes no
// fetched batch at all — the batch's record count is derived by the
// measurement harness from the spans it observes, never passed in from here.

// perfConsumerPollStart marks the start of one poll attempt.
func perfConsumerPollStart() {}

// perfConsumerPollDone marks the end of one poll attempt.
func perfConsumerPollDone() {}

// perfConsumerProcessStart marks the start of one record's processing.
func perfConsumerProcessStart(partition int32, offset int64) {}

// perfConsumerProcessDone marks the end of one record's processing.
func perfConsumerProcessDone(result ProcessResult, err error) {}

// perfConsumerMarkStart marks the start of one offset mark/commit step.
func perfConsumerMarkStart(partition int32, offset int64) {}

// perfConsumerMarkDone marks the end of one offset mark/commit step.
func perfConsumerMarkDone() {}

// perfConsumerRebalanceDone marks the end of one settled batch (the rebalance
// gate was just released).
func perfConsumerRebalanceDone() {}

// perfConsumerRefreshLagStart marks the start of one lag observation.
func perfConsumerRefreshLagStart() {}

// perfConsumerRefreshLagDone marks the end of one lag observation.
func perfConsumerRefreshLagDone() {}
