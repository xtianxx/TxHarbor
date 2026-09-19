// pausestop.go owns the Lane-F5 stop classification: a durable business pause
// halts the affected indexer loop, but it must not terminate the serve
// process. Only the persisted-pause stop classes below are non-terminal; every
// other stop error (configuration refusal, upstream drift, corrupt state,
// unknown failure) keeps the existing fatal polarity, and context cancellation
// keeps the existing clean-exit polarity.
package indexer

import "errors"

// isPauseStop reports whether err is a persisted business-pause stop:
//
//   - the header/log scanner's durable pause sentinel (errPaused), persisted as
//     an indexer_pause/log_pause row before the loop returns;
//   - the deposit/confirmation stream gate's pause-row verdict
//     (*streamPauseError);
//   - the log/deposit chain-view divergence (*chainViewError), persisted as a
//     pause row before the loop stops;
//   - the deposit scanner's persisted stop evidence: a structural upstream gap,
//     a validation failure and an identity conflict, each of which persists a
//     deposit_pause row through pauseEvidenceForStop before returning.
//
// It deliberately does NOT match anything else: upstream drift, corrupt
// deposit state, configuration refusals, lease loss, unknown errors and
// context cancellation are not pause stops and keep their existing
// terminal/clean semantics.
func isPauseStop(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errPaused) {
		return true
	}
	var pause *streamPauseError
	if errors.As(err, &pause) {
		return true
	}
	var view *chainViewError
	if errors.As(err, &view) {
		return true
	}
	var gap *depositGap
	if errors.As(err, &gap) {
		return gap.class == depositGapStructural
	}
	var parse *depositParseError
	if errors.As(err, &parse) {
		return true
	}
	var conflict *depositIdentityConflictError
	if errors.As(err, &conflict) {
		return true
	}
	return false
}
