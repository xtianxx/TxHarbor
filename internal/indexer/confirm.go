// confirm.go implements 005's safe confirmation math (T003, research R1):
// pure uint64 in-memory predicates over the canonical local tip. No scanner
// loop, no commit, no wiring here (T010-T012, later).
package indexer

import (
	"errors"
	"fmt"
	"time"
)

// ConfirmationConfig is the frozen input of confirmation tracking. ThresholdN
// is the required confirmation depth N (N >= 1, N <= MaxInt64 so it fits the
// NUMERIC/BIGINT policy range); PollInterval/RetryInitial/RetryMax reuse the
// shared TXHARBOR_INDEX_* knob names (research R2: no new env vars; zero means
// the repository defaults, pacing waits/retries only).
type ConfirmationConfig struct {
	ChainID      int64
	ThresholdN   uint64
	PollInterval time.Duration
	RetryInitial time.Duration
	RetryMax     time.Duration
}

// maxConfirmationThreshold is MaxInt64 expressed as uint64 without casting any
// variable: the storage type (BIGINT/NUMERIC policy range) decides the system
// range, not a business cap (research R1).
const maxConfirmationThreshold = uint64(1<<63 - 1)

// ErrConfirmationsSaturated is the unreachable-defense refusal: the exact
// formula tip-h+1 overflowed uint64 (only tip==MaxUint64 && h==0), so there is
// no exact value to store or audit. Callers must refuse the commit as an
// internal error; the saturated value must never be persisted or displayed.
var ErrConfirmationsSaturated = errors.New("confirmation count saturated; refusing commit")

// NewConfirmationConfig validates the confirmation configuration without any
// I/O. N==0 is rejected (the gate form N-1 would underflow); N>MaxInt64 is
// rejected as out of the system integer range.
func NewConfirmationConfig(cfg ConfirmationConfig) (ConfirmationConfig, error) {
	if cfg.ChainID <= 0 {
		return ConfirmationConfig{}, fmt.Errorf("confirmation config: chain id %d must be > 0", cfg.ChainID)
	}
	if cfg.ThresholdN == 0 {
		return ConfirmationConfig{}, errors.New("confirmation config: threshold N must be >= 1")
	}
	if cfg.ThresholdN > maxConfirmationThreshold {
		return ConfirmationConfig{}, fmt.Errorf("confirmation config: threshold N %d exceeds system integer range (max %d)",
			cfg.ThresholdN, maxConfirmationThreshold)
	}
	return cfg, nil
}

// ConfirmationReached is the overflow-free gate predicate, mathematically
// equivalent to the spec formula confirmations=max(0,tip-h+1) >= N but with no
// +1 overflow point: it holds for every input including hypothetical MaxUint64
// tips. N must be >= 1 (constructor-guaranteed); N==0 defensively reports false
// so the N-1 subtraction can never underflow.
func ConfirmationReached(tip, h, n uint64) bool {
	if n == 0 {
		return false
	}
	return tip >= h && tip-h >= n-1
}

// ExactConfirmations evaluates the spec formula confirmations=max(0,tip-h+1),
// the measurement/AUDIT value (the gate above is the only comparison path).
// tip<h yields (0, nil): zero confirmations, not saturation. The d==MaxUint64
// saturation guard (only tip==MaxUint64 && h==0, unreachable: BIGINT CHECK(>=0)
// tips/h never reach MaxUint64) returns ErrConfirmationsSaturated so the caller
// refuses the commit instead of storing a defense value.
func ExactConfirmations(tip, h uint64) (uint64, error) {
	if tip < h {
		return 0, nil
	}
	d := tip - h
	if d == ^uint64(0) {
		return ^uint64(0), ErrConfirmationsSaturated
	}
	return d + 1, nil
}

// MaxEligibleHeight is the candidate-scan upper bound: only rows with
// block_number <= maxEligible can satisfy the gate, so the query stays an
// indexed <= comparison (research R1). It equals tip+1-N mathematically but is
// computed as tip-(N-1) to avoid the tip+1 overflow entirely; ok=false (empty)
// when tip+1<N, i.e. tip < N-1, meaning no candidate can be eligible yet.
// N==0 defensively reports empty.
func MaxEligibleHeight(tip, n uint64) (uint64, bool) {
	if n == 0 {
		return 0, false
	}
	if tip < n-1 {
		return 0, false
	}
	return tip - (n - 1), true
}
