// confirmscan.go implements 005's candidate scan loop (T011, research R4/R7):
// the transaction-external half of the confirmation protocol, exactly as
// locked by specs/005-confirmation-tracking/ (FR-02/FR-06, research R4,
// data-model.md §候选分类 / §首确认协议).
//
// T011 scope: read the effective policy and the canonical tip each tick,
// compute maxEligible via confirm.go, fetch one ordered batch through the
// partial index, pre-check the gate per candidate and commit via
// ConfirmationCommitter.ConfirmDepositUnit with the captured (T, TH, S, N,
// h, bh, identity) basis. It performs no writes itself: the policy bootstrap
// row lands only inside the first conversion transaction (T010), so the
// empty state (F3) is zero candidates, zero policy row, zero business
// writes. Wiring into the coordinator is T012, not here.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/logx"
)

// confirmationCandidateBatchLimit bounds one tick's candidate fetch (research
// R4: no cursor; a LIMIT smaller than the eligible set is covered over
// multiple ticks, T019). It is a constant, not a knob: pacing reuses the
// shared INDEX timing knobs, no new env (research R2).
const confirmationCandidateBatchLimit = 500

// confirmationMetrics is the confirmation_* handle surface the scanner
// drives (contracts/observability.md). *metrics.Metrics satisfies it; a nil
// handle disables observation. The scanner never touches
// policy_transition_total (T024 owns it).
type confirmationMetrics interface {
	ObserveConfirmationPending(chain int64, pending uint64)
	ObserveConfirmationLag(chain int64, lag uint64, ok bool)
	ObserveConfirmationState(chain int64, state int)
	ObserveConfirmationPolicySeq(chain int64, seq uint64, ok bool)
	ObserveConfirmationConfirmed(chain int64)
	ObserveConfirmationSkipped(chain int64, reason string)
	ObserveConfirmationTransition(chain int64, result string)
}

// confirmationCommitter commits one captured candidate. It is satisfied by
// *ConfirmationCommitter; tests substitute a scripted fake.
type confirmationCommitter interface {
	ConfirmDepositUnit(ctx context.Context, lease *Lease, basis ConfirmBasis) error
}

// confirmationQuerier is satisfied by *pgxpool.Pool: the tick reads run
// outside any transaction (the commit re-adjudicates under the coordination
// lock, T010).
type confirmationQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// confirmationConfigMismatchError is the R7 refusal: the effective policy
// (MAX policy_seq row) threshold differs from this scanner's frozen env N.
// It mirrors 004's depositConfigMismatchError: refuse and loud-stop, never
// silently follow.
type confirmationConfigMismatchError struct{ detail string }

func (e *confirmationConfigMismatchError) Error() string {
	return "confirmation config changed: " + e.detail
}

// errConfirmationTipMissing / errConfirmationTipUntrusted are the FR-02 wait
// signals: no canonical tip row, or a tip row that cannot be trusted
// (negative height or blank hash; unreachable under the BIGINT CHECK, so
// defense only). Both wait for a trusted tip (state=1), never stop.
var (
	errConfirmationTipMissing   = errors.New("confirmation tip missing")
	errConfirmationTipUntrusted = errors.New("confirmation tip untrusted")
)

// ConfirmationScanner holds the frozen confirmation configuration plus the
// commit and observation handles. ServeLoop adapts to the shared ServeFunc
// shape with the coordinator-held lease (T012 wires it); confirmation
// authorization stays out of loop. The scanner performs no acquisition and
// no renewal: checkLost reports a lost lease and aborts before any commit.
type ConfirmationScanner struct {
	pool    *pgxpool.Pool
	cfg     ConfirmationConfig
	commit  confirmationCommitter
	metrics confirmationMetrics
	// db is the tick-read surface; it defaults to pool and is swapped only
	// by same-package unit tests (no DB).
	db confirmationQuerier
	// conState/conProgress/conHasProgress mirror the loop condition for the
	// confirmation_state gauge and the highest confirmed height
	// (contracts/observability.md): 0 running, 1 waiting on a trusted tip,
	// 2 backing off, 3 stopped. Atomics because serve samples them off-loop
	// on a ticker. Progress stays absent until the first conversion commits,
	// so the lag series stays absent on empty progress.
	conState       atomic.Int32
	conProgress    atomic.Uint64
	conHasProgress atomic.Bool
}

// NewConfirmationScanner validates the confirmation configuration without
// any I/O. A nil committer is a construction error; a nil metrics handle
// disables observation.
func NewConfirmationScanner(pool *pgxpool.Pool, cfg ConfirmationConfig, committer confirmationCommitter, m confirmationMetrics) (*ConfirmationScanner, error) {
	if pool == nil {
		return nil, errors.New("confirmation scanner: nil pool")
	}
	if committer == nil {
		return nil, errors.New("confirmation scanner: nil committer")
	}
	valid, err := NewConfirmationConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("confirmation scanner: %w", err)
	}
	return &ConfirmationScanner{pool: pool, cfg: valid, commit: committer, metrics: m, db: pool}, nil
}

// ConfirmationState reports the loop condition for the confirmation_state
// gauge (contracts/observability.md): 0 running, 1 waiting on a trusted
// tip, 2 backing off, 3 stopped. It mirrors the last loop decision;
// terminal stops keep their verdict so the final sample explains the exit.
func (s *ConfirmationScanner) ConfirmationState() int {
	return int(s.conState.Load())
}

// ConfirmationProgress reports the highest confirmed block height committed
// by this scanner run. ok is false until the first conversion commits, so
// the lag series stays absent on empty progress.
func (s *ConfirmationScanner) ConfirmationProgress() (uint64, bool) {
	return s.conProgress.Load(), s.conHasProgress.Load()
}

// confirmationPolicy is the read-only mirror of the effective policy (MAX
// policy_seq row). exists=false is the pre-bootstrap state: no row yet, so
// there is no effective policy and nothing to compare (data-model §首确认协议).
type confirmationPolicy struct {
	seq       int64
	threshold int64
	exists    bool
}

// confirmationTip is the read-only mirror of the canonical tip.
type confirmationTip struct {
	number uint64
	hash   string
}

// confirmationCandidate is one ordered pending candidate: the observation PK
// (chain_id is constant per scanner) plus its referenced height.
type confirmationCandidate struct {
	height    uint64
	blockHash string
	txHash    string
	logIndex  uint64
}

// confirmOutcome classifies one commit attempt for the batch driver.
type confirmOutcome int

const (
	// confirmCommitted: conversion landed (or converged); continue the batch.
	confirmCommitted confirmOutcome = iota
	// confirmWaitRow: below_depth row-level wait — leave Pending, count the
	// skip, continue the same batch. The only non-halt branch (§候选分类).
	confirmWaitRow
	// confirmRetryTick: stale basis — re-read tip/policy and continue the
	// outer loop with zero advance.
	confirmRetryTick
	// confirmHalt: whole-loop halt — zero further commits this tick and
	// after (until operator/006 resolves); the caller returns the error so
	// the coordinator fan-out stops the process loud.
	confirmHalt
	// confirmBackoff: transient failure — bounded backoff with zero advance.
	confirmBackoff
)

// classifyConfirmationOutcome maps a commit outcome to a batch verdict with
// the contracts/observability.md stop reason. below_depth (the pre-check or
// the re-computed gate refusal after a tip move) is the only wait branch;
// every other anomaly halts.
func classifyConfirmationOutcome(err error) (confirmOutcome, string) {
	if err == nil {
		return confirmCommitted, ""
	}
	if errors.Is(err, errStaleState) {
		return confirmRetryTick, ""
	}
	if errors.Is(err, ErrLeaseLost) {
		return confirmHalt, "lease_lost"
	}
	var drift *ConfirmationDriftError
	if errors.As(err, &drift) {
		return confirmHalt, "policy_drift"
	}
	var pause *streamPauseError
	if errors.As(err, &pause) {
		return confirmHalt, "pause_present"
	}
	var chainView *ConfirmationChainViewError
	if errors.As(err, &chainView) {
		switch d := chainView.detail; {
		case strings.Contains(d, "re-computed gate fails"):
			return confirmWaitRow, "below_depth"
		case strings.Contains(d, "canonical tip is missing"):
			return confirmHalt, "tip_missing"
		case strings.Contains(d, "differs from canonical tip"):
			return confirmHalt, "tip_untrusted"
		default:
			return confirmHalt, "reference_unverifiable"
		}
	}
	var mismatch *confirmationConfigMismatchError
	if errors.As(err, &mismatch) {
		return confirmHalt, "policy_drift"
	}
	return confirmBackoff, ""
}

// readConfirmationPolicy reads the effective policy (MAX policy_seq row).
// No row is the pre-bootstrap state (ok with exists=false), never an error.
func (s *ConfirmationScanner) readConfirmationPolicy(ctx context.Context, q confirmationQuerier) (confirmationPolicy, error) {
	var (
		seq, threshold int64
	)
	err := q.QueryRow(ctx, readConfirmationPolicySQL, s.cfg.ChainID).Scan(&seq, &threshold)
	switch {
	case err == nil:
		return confirmationPolicy{seq: seq, threshold: threshold, exists: true}, nil
	case errors.Is(err, pgx.ErrNoRows):
		return confirmationPolicy{}, nil
	default:
		return confirmationPolicy{}, fmt.Errorf("read confirmation policy: %w", err)
	}
}

// verifyPolicyIdentity applies the R7 startup comparison to the effective
// policy row: once the row exists its threshold is frozen and must equal
// this scanner's env N. A mismatch is a configuration change, reported and
// never continued silently. The comparison object is the row's threshold,
// never a candidate height.
func (s *ConfirmationScanner) verifyPolicyIdentity(p confirmationPolicy) error {
	if !p.exists {
		return nil
	}
	if p.threshold != int64(s.cfg.ThresholdN) {
		return &confirmationConfigMismatchError{detail: fmt.Sprintf(
			"chain %d effective policy (seq=%d threshold=%d) differs from configured N=%d",
			s.cfg.ChainID, p.seq, p.threshold, s.cfg.ThresholdN)}
	}
	return nil
}

// readConfirmationTip reads the current canonical tip. A missing row waits
// for coverage; an untrusted row waits for a trustworthy tip.
func (s *ConfirmationScanner) readConfirmationTip(ctx context.Context, q confirmationQuerier) (confirmationTip, error) {
	var (
		number int64
		hash   string
	)
	err := q.QueryRow(ctx, readConfirmationTipSQL, s.cfg.ChainID).Scan(&number, &hash)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return confirmationTip{}, errConfirmationTipMissing
	case err != nil:
		return confirmationTip{}, fmt.Errorf("read confirmation tip: %w", err)
	case number < 0 || hash == "":
		return confirmationTip{}, errConfirmationTipUntrusted
	default:
		return confirmationTip{number: uint64(number), hash: hash}, nil
	}
}

// rejectConfirmationPauses requires all three pause rows to be absent
// (data-model §提交协议 step 3). It is the pre-commit check; the commit
// re-reads the same statements under the coordination lock before any write.
func (s *ConfirmationScanner) rejectConfirmationPauses(ctx context.Context, q confirmationQuerier) error {
	for _, stream := range []struct {
		name string
		sql  string
	}{
		{"deposit_pause", depositPauseExistsSQL},
		{"log_pause", logPauseExistsSQL},
		{"indexer_pause", pauseExistsSQL},
	} {
		var one int
		err := q.QueryRow(ctx, stream.sql, s.cfg.ChainID).Scan(&one)
		switch {
		case err == nil:
			return &streamPauseError{stream: stream.name, chainID: s.cfg.ChainID}
		case errors.Is(err, pgx.ErrNoRows):
		default:
			return fmt.Errorf("read %s: %w", stream.name, err)
		}
	}
	return nil
}

// readConfirmationCandidates fetches one ordered pending batch at or below
// maxEligible through the partial index (contracts diagnostic candidate
// SQL): (chain_id, block_number) order, no cursor, so a lowered threshold
// automatically re-includes previously ineligible rows (research R4).
func (s *ConfirmationScanner) readConfirmationCandidates(ctx context.Context, q confirmationQuerier, maxEligible uint64) ([]confirmationCandidate, error) {
	rows, err := q.Query(ctx, readConfirmationCandidatesSQL, s.cfg.ChainID, int64(maxEligible), confirmationCandidateBatchLimit)
	if err != nil {
		return nil, fmt.Errorf("read confirmation candidates: %w", err)
	}
	defer rows.Close()
	var out []confirmationCandidate
	for rows.Next() {
		var (
			number, index int64
			c             confirmationCandidate
		)
		if err := rows.Scan(&number, &c.blockHash, &c.txHash, &index); err != nil {
			return nil, fmt.Errorf("scan confirmation candidate: %w", err)
		}
		if number < 0 || index < 0 {
			return nil, fmt.Errorf("confirmation candidate has negative height or index (block_number=%d log_index=%d)", number, index)
		}
		c.height, c.logIndex = uint64(number), uint64(index)
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read confirmation candidates: %w", err)
	}
	return out, nil
}

// countConfirmationPending reads the unconfirmed pending estimate for the
// pending gauge (empty exposes 0).
func (s *ConfirmationScanner) countConfirmationPending(ctx context.Context, q confirmationQuerier) (uint64, error) {
	var n int64
	if err := q.QueryRow(ctx, countConfirmationPendingSQL, s.cfg.ChainID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count confirmation pending: %w", err)
	}
	if n < 0 {
		return 0, fmt.Errorf("confirmation pending count %d is negative", n)
	}
	return uint64(n), nil
}

// loopTimings resolves the polling/backoff knobs with repository defaults
// (research R2: the INDEX_* knobs are reused, no new environment variable).
func (s *ConfirmationScanner) loopTimings() (poll, retryInitial, retryMax time.Duration) {
	poll = s.cfg.PollInterval
	if poll <= 0 {
		poll = depositDefaultPollInterval
	}
	retryInitial = s.cfg.RetryInitial
	if retryInitial <= 0 {
		retryInitial = depositDefaultRetryInitial
	}
	retryMax = s.cfg.RetryMax
	if retryMax <= 0 {
		retryMax = depositDefaultRetryMax
	}
	if retryMax < retryInitial {
		retryMax = retryInitial
	}
	return poll, retryInitial, retryMax
}

// wait sleeps for d and reports false when ctx is done, so shutdown aborts a
// waiting loop immediately.
func (s *ConfirmationScanner) wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// observeTick mirrors the current tick into the confirmation_* gauges:
// pending estimate, loop state, effective policy_seq (absent pre-bootstrap)
// and lag (absent while the tip is missing or nothing is confirmed).
func (s *ConfirmationScanner) observeTick(pending uint64, policy confirmationPolicy, tip confirmationTip, tipOK bool) {
	if s.metrics == nil {
		return
	}
	chain := s.cfg.ChainID
	s.metrics.ObserveConfirmationPending(chain, pending)
	s.metrics.ObserveConfirmationState(chain, int(s.conState.Load()))
	if policy.exists && policy.seq > 0 {
		s.metrics.ObserveConfirmationPolicySeq(chain, uint64(policy.seq), true)
	} else {
		s.metrics.ObserveConfirmationPolicySeq(chain, 0, false)
	}
	if progress, ok := s.conProgress.Load(), s.conHasProgress.Load(); ok && tipOK && tip.number >= progress {
		s.metrics.ObserveConfirmationLag(chain, tip.number-progress, true)
	} else {
		s.metrics.ObserveConfirmationLag(chain, 0, false)
	}
}

// ServeLoop is the T011 confirmation loop: while the coordinator holds the
// lease it repeatedly reads the effective policy and the canonical tip,
// sizes one ordered candidate batch at or below maxEligible and commits each
// convertible candidate with its captured basis. It returns nil on ctx
// cancellation, ErrLeaseLost when the lease is gone, and a stop error for
// every condition that must not continue (policy drift, pause, chain-view
// anomaly — state=3, zero further commits that tick and after).
//
// below_depth is the only row-level wait: the row stays Pending and the
// batch continues. A missing/untrusted tip waits (state=1); an empty batch
// idles running (state=0) with zero policy row and zero business writes
// (F3); unreadable state retries with bounded backoff (state=2).
func (s *ConfirmationScanner) ServeLoop(ctx context.Context, lease *Lease, checkLost func() error) error {
	if lease == nil {
		return errors.New("confirmation scanner: nil lease")
	}
	if checkLost == nil {
		checkLost = func() error { return nil }
	}
	poll, retryInitial, retryMax := s.loopTimings()
	back := newBackoff(retryInitial, retryMax)

outer:
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := checkLost(); err != nil {
			return err
		}

		policy, err := s.readConfirmationPolicy(ctx, s.db)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// The effective policy is unreadable: bounded retry with zero
			// advance. The commit re-adjudicates the policy on retry.
			s.conState.Store(2)
			s.observeTick(0, confirmationPolicy{}, confirmationTip{}, false)
			if !s.wait(ctx, back.next()) {
				return nil
			}
			continue
		}
		if err := s.verifyPolicyIdentity(policy); err != nil {
			// R7 loud stop: the env N is foreign to the effective policy.
			s.conState.Store(3)
			s.observeTick(0, policy, confirmationTip{}, false)
			if s.metrics != nil {
				s.metrics.ObserveConfirmationTransition(s.cfg.ChainID, "rejected")
			}
			slog.Error("confirmation loop stopped",
				"chain_id", s.cfg.ChainID, "reason", "policy_drift",
				"error", logx.Redact(err.Error()))
			return err
		}

		tip, err := s.readConfirmationTip(ctx, s.db)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, errConfirmationTipMissing) || errors.Is(err, errConfirmationTipUntrusted) {
				// FR-02: no trusted tip, stop confirming and wait for one.
				reason := "tip_missing"
				if errors.Is(err, errConfirmationTipUntrusted) {
					reason = "tip_untrusted"
				}
				s.conState.Store(1)
				s.observeTick(0, policy, confirmationTip{}, false)
				slog.Warn("confirmation waiting for trusted tip",
					"chain_id", s.cfg.ChainID, "reason", reason)
				if !s.wait(ctx, poll) {
					return nil
				}
				continue
			}
			s.conState.Store(2)
			s.observeTick(0, policy, confirmationTip{}, false)
			if !s.wait(ctx, back.next()) {
				return nil
			}
			continue
		}

		pending, err := s.countConfirmationPending(ctx, s.db)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			s.conState.Store(2)
			s.observeTick(0, policy, tip, true)
			if !s.wait(ctx, back.next()) {
				return nil
			}
			continue
		}

		maxEligible, ok := MaxEligibleHeight(tip.number, s.cfg.ThresholdN)
		if !ok {
			// tip+1 < N: no candidate can be eligible yet. Benign lag
			// wait, never an anomaly.
			s.conState.Store(0)
			s.observeTick(pending, policy, tip, true)
			if !s.wait(ctx, poll) {
				return nil
			}
			continue
		}

		if err := s.rejectConfirmationPauses(ctx, s.db); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			s.conState.Store(3)
			s.observeTick(pending, policy, tip, true)
			if s.metrics != nil {
				s.metrics.ObserveConfirmationTransition(s.cfg.ChainID, "rejected")
			}
			slog.Error("confirmation loop stopped",
				"chain_id", s.cfg.ChainID, "reason", "pause_present",
				"error", logx.Redact(err.Error()))
			return err
		}

		batch, err := s.readConfirmationCandidates(ctx, s.db, maxEligible)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			s.conState.Store(2)
			s.observeTick(pending, policy, tip, true)
			if !s.wait(ctx, back.next()) {
				return nil
			}
			continue
		}
		if len(batch) == 0 {
			// F3 empty-state: no policy row is created and no business
			// write is attempted; the state stays running-idle (never
			// stopped). The first eligible conversion bootstraps the
			// policy inside its own commit transaction (T010).
			s.conState.Store(0)
			s.observeTick(pending, policy, tip, true)
			if !s.wait(ctx, poll) {
				return nil
			}
			continue
		}

		seq := int64(1)
		if policy.exists {
			seq = policy.seq
		}
		for _, c := range batch {
			if !ConfirmationReached(tip.number, c.height, s.cfg.ThresholdN) {
				// Pre-check gate: below_depth row-level wait.
				s.observeSkipped(c)
				continue
			}
			basis := ConfirmBasis{
				BlockHash:  c.blockHash,
				TxHash:     c.txHash,
				LogIndex:   c.logIndex,
				Height:     c.height,
				TipNumber:  tip.number,
				TipHash:    tip.hash,
				PolicySeq:  seq,
				ThresholdN: s.cfg.ThresholdN,
			}
			err := s.commit.ConfirmDepositUnit(ctx, lease, basis)
			outcome, reason := classifyConfirmationOutcome(err)
			switch outcome {
			case confirmCommitted:
				s.observeConfirmed(c, tip, seq)
				back.reset()
			case confirmWaitRow:
				// below_depth after a tip move: row-level wait, same
				// batch continues.
				if s.metrics != nil {
					s.metrics.ObserveConfirmationTransition(s.cfg.ChainID, "stale")
				}
				s.observeSkipped(c)
			case confirmRetryTick:
				if s.metrics != nil {
					s.metrics.ObserveConfirmationTransition(s.cfg.ChainID, "stale")
				}
				continue outer
			case confirmHalt:
				if errors.Is(err, ErrLeaseLost) {
					slog.Warn("confirmation loop stopped",
						"chain_id", s.cfg.ChainID, "reason", reason,
						"error", logx.Redact(err.Error()))
					return fmt.Errorf("%w (confirmation commit: %v)", ErrLeaseLost, err)
				}
				s.conState.Store(3)
				s.observeTick(pending, policy, tip, true)
				if s.metrics != nil {
					if reason == "policy_drift" || reason == "pause_present" {
						s.metrics.ObserveConfirmationTransition(s.cfg.ChainID, "rejected")
					} else {
						s.metrics.ObserveConfirmationTransition(s.cfg.ChainID, "stale")
					}
				}
				slog.Error("confirmation loop stopped",
					"chain_id", s.cfg.ChainID, "reason", reason,
					"block_number", c.height, "block_hash", c.blockHash,
					"tx_hash", c.txHash, "log_index", c.logIndex,
					"error", logx.Redact(err.Error()))
				return err
			case confirmBackoff:
				s.conState.Store(2)
				s.observeTick(pending, policy, tip, true)
				slog.Warn("confirmation retrying",
					"chain_id", s.cfg.ChainID,
					"error", logx.Redact(err.Error()))
				if !s.wait(ctx, back.next()) {
					return nil
				}
				continue outer
			}
		}

		// A full batch may hide more eligible rows behind the LIMIT:
		// re-tick immediately for monotonic multi-tick coverage (T019).
		// Anything else waits one poll so an unchanged tip never hot-spins.
		if len(batch) == confirmationCandidateBatchLimit {
			continue
		}
		s.conState.Store(0)
		s.observeTick(pending, policy, tip, true)
		if !s.wait(ctx, poll) {
			return nil
		}
	}
}

// observeConfirmed records one successful conversion: the confirmed counter,
// the ok adjudication, the progress watermark and the conversion log line
// (contracts/observability.md fields).
func (s *ConfirmationScanner) observeConfirmed(c confirmationCandidate, tip confirmationTip, seq int64) {
	s.conState.Store(0)
	s.conProgress.Store(c.height)
	s.conHasProgress.Store(true)
	if s.metrics != nil {
		s.metrics.ObserveConfirmationConfirmed(s.cfg.ChainID)
		s.metrics.ObserveConfirmationTransition(s.cfg.ChainID, "ok")
	}
	confirmations, cerr := ExactConfirmations(tip.number, c.height)
	if cerr != nil {
		slog.Warn("confirmation converted with unmeasurable count",
			"chain_id", s.cfg.ChainID,
			"block_number", c.height, "block_hash", c.blockHash,
			"tx_hash", c.txHash, "log_index", c.logIndex,
			"error", logx.Redact(cerr.Error()))
		return
	}
	slog.Info("confirmation converted",
		"chain_id", s.cfg.ChainID,
		"block_number", c.height, "block_hash", c.blockHash,
		"tx_hash", c.txHash, "log_index", c.logIndex,
		"tip", tip.number, "threshold", s.cfg.ThresholdN,
		"confirmations", confirmations, "policy_seq", seq)
}

// observeSkipped records one benign below_depth re-estimate: the row stays
// Pending and only the skip counter moves (contracts/observability.md).
func (s *ConfirmationScanner) observeSkipped(c confirmationCandidate) {
	if s.metrics != nil {
		s.metrics.ObserveConfirmationSkipped(s.cfg.ChainID, "below_depth")
	}
	slog.Debug("confirmation candidate below depth",
		"chain_id", s.cfg.ChainID,
		"block_number", c.height, "block_hash", c.blockHash,
		"reason", "below_depth")
}

// Confirmation scan statements. All are chain-scoped reads; this file issues
// no UPDATE/INSERT/DELETE (the only 005 write path is the T010 commit
// transaction, which carries the status='pending' predicate).
const (
	// readConfirmationCandidatesSQL is the R4 ordered batch fetch through
	// the partial index (contracts diagnostic candidate SQL): pending rows
	// at or below maxEligible in (block_number, log_index) order.
	readConfirmationCandidatesSQL = `
SELECT block_number, block_hash, tx_hash, log_index
FROM deposit_observations
WHERE chain_id = $1 AND status = 'pending' AND block_number <= $2
ORDER BY block_number, log_index LIMIT $3`

	// countConfirmationPendingSQL reads the unconfirmed pending estimate
	// for the pending gauge (empty exposes 0).
	countConfirmationPendingSQL = `
SELECT COUNT(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'pending'`
)
