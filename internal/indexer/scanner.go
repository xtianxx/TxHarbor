// scanner.go implements the sequential block-header scanner: ChainID gate,
// lease acquisition, startup checkpoint verification and the one-height-per-
// transaction advance loop. It is the only component that advances
// indexer_checkpoint. Behavior is locked by specs/002-chain-indexer/spec.md
// (FR-01/02/03/05/06/10/11/12/13 plus the five clarifications) and
// specs/002-chain-indexer/data-model.md (write-transaction protocol,
// same-height hash re-read, pause protocol, frozen start_height).
package indexer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	mathrand "math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/logx"
)

// State is the scanner's runtime state; the values match the
// txharbor_indexer_state metric (contracts/observability.md).
type State int

const (
	StateRunning  State = 0
	StateWaiting  State = 1
	StateRetrying State = 2
	StatePaused   State = 3
)

// Pause kinds, matching the indexer_pause.kind CHECK constraint.
const (
	pauseHashMismatch      = "hash_mismatch"
	pauseParentMismatch    = "parent_mismatch"
	pauseCheckpointChanged = "checkpoint_changed"
)

// Config carries the scanner's runtime knobs (from config.Config:
// TXHARBOR_START_HEIGHT and TXHARBOR_INDEX_*).
type Config struct {
	StartHeight  uint64
	RPCTimeout   time.Duration
	PollInterval time.Duration
	RetryInitial time.Duration
	RetryMax     time.Duration
}

// HeaderClient is the minimal chain read surface the scanner needs;
// *eth.Client satisfies it.
type HeaderClient interface {
	ChainID(ctx context.Context) (*big.Int, error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
}

// Scanner advances one chain's header index. Run is the only entry point; it
// may be called once. State and Checkpoint are safe for concurrent readers
// (metrics).
type Scanner struct {
	pool   *pgxpool.Pool
	client HeaderClient
	lease  *Lease
	cfg    Config
	logger *slog.Logger

	chainID int64

	mu     sync.RWMutex
	state  State
	reason string
	cp     *progress
}

// progress is the in-memory mirror of the durable checkpoint row. The database
// stays authoritative: every write transaction re-decides under the
// coordination lock and a failed transaction forces a re-read.
type progress struct {
	height      uint64
	hash        string
	startHeight uint64
}

// pauseRow is a durable pause row (diagnostics only; row existence is truth).
type pauseRow struct {
	height   uint64
	expected string
	actual   string
	kind     string
	detail   string
}

// NewScanner validates the dependencies and builds a scanner. It performs no
// I/O; Run does all connecting.
func NewScanner(pool *pgxpool.Pool, client HeaderClient, lease *Lease, cfg Config, logger *slog.Logger) (*Scanner, error) {
	if pool == nil {
		return nil, errors.New("indexer scanner: nil pool")
	}
	if client == nil {
		return nil, errors.New("indexer scanner: nil chain client")
	}
	if lease == nil {
		return nil, errors.New("indexer scanner: nil lease")
	}
	if cfg.RPCTimeout <= 0 {
		return nil, fmt.Errorf("indexer scanner: rpc timeout %s must be > 0", cfg.RPCTimeout)
	}
	if cfg.PollInterval <= 0 {
		return nil, fmt.Errorf("indexer scanner: poll interval %s must be > 0", cfg.PollInterval)
	}
	if cfg.RetryInitial <= 0 {
		return nil, fmt.Errorf("indexer scanner: retry initial %s must be > 0", cfg.RetryInitial)
	}
	if cfg.RetryMax < cfg.RetryInitial {
		return nil, fmt.Errorf("indexer scanner: retry max %s must be >= retry initial %s", cfg.RetryMax, cfg.RetryInitial)
	}
	if cfg.StartHeight > math.MaxInt64 {
		return nil, fmt.Errorf("indexer scanner: start height %d exceeds the bigint column range", cfg.StartHeight)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Scanner{
		pool:    pool,
		client:  client,
		lease:   lease,
		cfg:     cfg,
		logger:  logger,
		chainID: lease.chainID,
	}, nil
}

// State returns the current scanner state.
func (s *Scanner) State() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// Checkpoint returns the current checkpoint snapshot; ok is false while no
// checkpoint exists (empty progress).
func (s *Scanner) Checkpoint() (height uint64, hash string, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.cp == nil {
		return 0, "", false
	}
	return s.cp.height, s.cp.hash, true
}

func (s *Scanner) setState(state State, reason string) {
	s.mu.Lock()
	s.state = state
	s.reason = reason
	s.mu.Unlock()
}

func (s *Scanner) setProgress(cp *progress) {
	s.mu.Lock()
	s.cp = cp
	s.mu.Unlock()
}

// Run gates on the configured chain id, then runs the scanner until ctx is
// cancelled or a stop condition is reached (chain mismatch, paused chain,
// start-height change, non-retryable error). While another instance holds the
// lease this instance stays a quiet bystander and keeps retrying.
func (s *Scanner) Run(ctx context.Context) error {
	if err := s.gateChainID(ctx); err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		won, _, err := s.lease.Acquire(ctx)
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return nil
			}
			s.setState(StateRetrying, "lease_acquire")
			s.logger.Warn("lease acquire failed; retrying",
				"chain_id", s.chainID, "error", logx.Redact(err.Error()))
			if !s.wait(ctx, s.cfg.PollInterval) {
				return nil
			}
			continue
		case !won:
			s.setState(StateWaiting, "lease_busy")
			s.logger.Debug("lease held by another instance; standing by", "chain_id", s.chainID)
			if !s.wait(ctx, s.cfg.PollInterval) {
				return nil
			}
			continue
		}

		s.logger.Info("lease acquired", "chain_id", s.chainID, "fencing_token", s.lease.Token())
		err = s.serve(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrLeaseLost) {
			// The lease is unconfirmed: re-acquire (possibly with a higher
			// fencing token) before any further write.
			s.logger.Warn("lease lost; re-acquiring", "chain_id", s.chainID, "error", logx.Redact(err.Error()))
			continue
		}
		return err
	}
}

// gateChainID refuses to scan when the endpoint chain id differs from the
// configured one (FR-01, zero writes). Retryable transport failures are
// retried with capped backoff; a mismatch is reported with both values.
func (s *Scanner) gateChainID(ctx context.Context) error {
	back := newBackoff(s.cfg.RetryInitial, s.cfg.RetryMax)
	for {
		if ctx.Err() != nil {
			return nil
		}
		rctx, cancel := context.WithTimeout(ctx, s.cfg.RPCTimeout)
		id, err := s.client.ChainID(rctx)
		cancel()
		if err == nil {
			if id == nil || id.Cmp(big.NewInt(s.chainID)) != 0 {
				return fmt.Errorf("chain id mismatch: configured %d, endpoint %v; refusing to scan", s.chainID, id)
			}
			return nil
		}
		if !isRetryable(err) {
			return fmt.Errorf("chain id gate: %w", err)
		}
		d := back.next()
		s.setState(StateRetrying, "chain_id_gate")
		s.logger.Warn("chain id check failed; retrying",
			"chain_id", s.chainID, "attempt", back.attempt, "retry_in", d,
			"error", logx.Redact(err.Error()))
		if !s.wait(ctx, d) {
			return nil
		}
	}
}

// serve holds the lease and drives the scan until a stop condition. It runs
// the lease heartbeat for as long as it is active; Run keeps this
// self-contained path so 002 behavior stays locked by its own tests.
func (s *Scanner) serve(ctx context.Context) error {
	hbCtx, cancelHB := context.WithCancel(ctx)
	defer cancelHB()
	lost := make(chan error, 1)
	go func() { lost <- s.lease.Heartbeat(hbCtx) }()
	checkLost := func() error {
		select {
		case err := <-lost:
			if err == nil {
				return nil // heartbeat stopped because its context is done
			}
			return fmt.Errorf("%w: %v", ErrLeaseLost, err)
		default:
			return nil
		}
	}
	return s.ServeLoop(ctx, checkLost)
}

// ServeLoop is the mechanical split of the old serve body: it runs the scan
// while the caller holds the lease and the heartbeat (the 003 coordinator owns
// both, research R1). checkLost reports a lost lease and is consulted before
// any write; nil means "never lost" for standalone callers.
func (s *Scanner) ServeLoop(ctx context.Context, checkLost func() error) error {
	if checkLost == nil {
		checkLost = func() error { return nil }
	}

	// Startup: read durable state and freeze the start height (FR-03).
	cp, paused, err := s.loadProgressRetry(ctx)
	if err != nil {
		return nil // ctx cancelled
	}
	if cp != nil {
		// Mirror durable truth for State()/Checkpoint() observers (metrics):
		// a scanner that starts already caught up may never commit, yet must
		// still report the checkpoint. Writes always re-decide under the
		// coordination lock; this mirror never authorizes anything.
		s.setProgress(cp)
	}
	if cp != nil && cp.startHeight != s.cfg.StartHeight {
		return fmt.Errorf("start height changed: configured %d but checkpoint was created with %d; refusing to scan (FR-03)",
			s.cfg.StartHeight, cp.startHeight)
	}
	if paused != nil {
		return s.reportPaused(paused)
	}

	// Startup checkpoint verification (FR-11, clarification #2): wait on
	// not-found/retryable, pause on a confirmed different chain hash, stop on
	// non-retryable errors. No advance happens before this succeeds.
	if cp != nil {
		if err := s.verifyCheckpoint(ctx, cp, checkLost); err != nil {
			return err
		}
	}

	back := newBackoff(s.cfg.RetryInitial, s.cfg.RetryMax)
	attempt := 0
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := checkLost(); err != nil {
			return err
		}

		// 006 loop gate (T013): capture the recovery version BEFORE batch
		// inputs and never start a batch under an active recovery row.
		rcap, active, err := captureRecoveryVersion(ctx, s.pool, s.chainID)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			d := back.next()
			s.setState(StateRetrying, "recovery_capture")
			s.logger.Warn("recovery capture failed; retrying",
				"chain_id", s.chainID, "retry_in", d, "error", logx.Redact(err.Error()))
			if !s.wait(ctx, d) {
				return nil
			}
			continue
		}
		if active {
			s.setState(StateWaiting, "recovery_active")
			s.logger.Debug("recovery active; ordinary indexing waits",
				"chain_id", s.chainID, "recovery_seq", rcap.Seq)
			if !s.wait(ctx, pairPollInterval) {
				return nil
			}
			continue
		}

		// Expected height and continuity anchor (FR-02/FR-05): the first
		// block after an empty progress is S (no S-1 read, no parent check);
		// afterwards it is checkpoint height + 1 and the parent must equal the
		// checkpoint hash.
		n := s.cfg.StartHeight
		first := true
		if cp != nil {
			n = cp.height + 1
			first = false
		}

		attempt++
		h, err := s.fetch(ctx, n)
		if err != nil {
			switch {
			case isNotFound(err):
				// Caught up (or S above the head): wait, not a fault (FR-10).
				reason := "wait_head"
				if first {
					reason = "wait_start"
				}
				s.setState(StateWaiting, reason)
				s.logger.Debug("waiting for next block",
					"chain_id", s.chainID, "height", n, "reason", reason)
				if !s.wait(ctx, s.cfg.PollInterval) {
					return nil
				}
			case isRetryable(err):
				d := back.next()
				s.setState(StateRetrying, "fetch_header")
				s.logger.Warn("header fetch failed; retrying",
					"chain_id", s.chainID, "height", n, "attempt", attempt,
					"retry_in", d, "error", logx.Redact(err.Error()))
				if !s.wait(ctx, d) {
					return nil
				}
			default:
				return fmt.Errorf("fetch header %d: %w", n, err)
			}
			continue
		}

		hash := hashHex(h.Hash())
		parentHash := hashHex(h.ParentHash)
		// Pre-transaction continuity check; the authoritative check is the
		// exact guard inside the write transaction.
		if cp != nil && parentHash != cp.hash {
			return s.pause(ctx, pauseInfo{
				kind:     pauseParentMismatch,
				height:   n,
				expected: cp.hash,
				actual:   parentHash,
				detail:   fmt.Sprintf("block %d parent %s does not link to stored predecessor %s", n, parentHash, cp.hash),
			})
		}

		err = s.commitBlock(ctx, blockWrite{number: n, hash: hash, parent: parentHash, first: first}, rcap)
		var hm *hashMismatchError
		switch {
		case err == nil:
			if first {
				cp = &progress{height: n, hash: hash, startHeight: s.cfg.StartHeight}
			} else {
				cp = &progress{height: n, hash: hash, startHeight: cp.startHeight}
			}
			s.setProgress(cp)
			s.setState(StateRunning, "")
			s.logger.Info("indexed block",
				"chain_id", s.chainID, "height", n, "hash", hash, "attempt", attempt)
			back.reset()
			attempt = 0
		case errors.As(err, &hm):
			// Same-height hash comparison failed inside the transaction: the
			// canonical record on disk differs from the chain. Pause, never
			// rewrite (FR-12).
			return s.pause(ctx, pauseInfo{
				kind:     pauseHashMismatch,
				height:   n,
				expected: hm.stored,
				actual:   hm.actual,
				detail:   fmt.Sprintf("stored block %d differs from chain hash", n),
			})
		case errors.Is(err, errPaused):
			_, p, rerr := s.loadProgressRetry(ctx)
			if rerr != nil {
				return nil
			}
			if p != nil {
				return s.reportPaused(p)
			}
			s.setState(StatePaused, "pause_row")
			return fmt.Errorf("%w: durable pause row present", errPaused)
		case errors.Is(err, ErrLeaseLost):
			return err
		case errors.Is(err, errStaleState) || isRecoveryGate(err):
			// A concurrent writer (or an uncertain commit from the previous
			// iteration) changed the durable state — or a recovery version
			// moved under us. Re-read and continue idempotently; the exact
			// guard (or a fresh capture upstream) decides what actually
			// committed. Recomputation starts a new batch, never re-labels
			// old results.
			old := cp
			ncp, np, rerr := s.loadProgressRetry(ctx)
			if rerr != nil {
				return nil
			}
			if np != nil {
				return s.reportPaused(np)
			}
			if ncp != nil && ncp.height == n && ncp.hash != hash {
				return s.pause(ctx, pauseInfo{
					kind:     pauseHashMismatch,
					height:   n,
					expected: ncp.hash,
					actual:   hash,
					detail:   fmt.Sprintf("stored block %d differs from chain hash", n),
				})
			}
			cp = ncp
			if sameProgress(old, cp) {
				// Nothing moved: avoid a hot loop against the database.
				if !s.wait(ctx, back.next()) {
					return nil
				}
			}
		default:
			// Database failure: stay on this height and retry (FR-09).
			d := back.next()
			s.setState(StateRetrying, "commit_block")
			s.logger.Warn("block transaction failed; retrying",
				"chain_id", s.chainID, "height", n, "attempt", attempt,
				"retry_in", d, "error", logx.Redact(err.Error()))
			if !s.wait(ctx, d) {
				return nil
			}
		}
	}
}

// verifyCheckpoint re-checks the chain hash at the stored checkpoint height
// (FR-11). Not-found waits on the poll interval, retryable errors back off,
// a confirmed different hash pauses and stops, anything else stops and
// reports.
func (s *Scanner) verifyCheckpoint(ctx context.Context, cp *progress, checkLost func() error) error {
	back := newBackoff(s.cfg.RetryInitial, s.cfg.RetryMax)
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := checkLost(); err != nil {
			return err
		}
		h, err := s.fetch(ctx, cp.height)
		if err == nil {
			actual := hashHex(h.Hash())
			if actual != cp.hash {
				return s.pause(ctx, pauseInfo{
					kind:     pauseCheckpointChanged,
					height:   cp.height,
					expected: cp.hash,
					actual:   actual,
					detail:   fmt.Sprintf("chain header at checkpoint height no longer matches stored hash"),
				})
			}
			return nil
		}
		switch {
		case isNotFound(err):
			s.setState(StateWaiting, "verify_retry")
			s.logger.Debug("checkpoint verify: height not found yet; waiting",
				"chain_id", s.chainID, "height", cp.height, "reason", "verify_retry")
			if !s.wait(ctx, s.cfg.PollInterval) {
				return nil
			}
		case isRetryable(err):
			d := back.next()
			s.setState(StateRetrying, "verify_retry")
			s.logger.Warn("checkpoint verify failed; retrying",
				"chain_id", s.chainID, "height", cp.height, "attempt", back.attempt,
				"retry_in", d, "error", logx.Redact(err.Error()))
			if !s.wait(ctx, d) {
				return nil
			}
		default:
			return fmt.Errorf("checkpoint verify at height %d: %w", cp.height, err)
		}
	}
}

// fetch reads one header with the configured RPC timeout (always outside any
// transaction) and validates its height and hash shapes.
func (s *Scanner) fetch(ctx context.Context, number uint64) (*types.Header, error) {
	rctx, cancel := context.WithTimeout(ctx, s.cfg.RPCTimeout)
	defer cancel()
	h, err := s.client.HeaderByNumber(rctx, new(big.Int).SetUint64(number))
	if err != nil {
		return nil, err
	}
	if err := checkHeader(h, number); err != nil {
		return nil, err
	}
	return h, nil
}

// loadProgressRetry reads the durable checkpoint and pause row, retrying
// database errors with capped backoff until success or ctx cancellation.
func (s *Scanner) loadProgressRetry(ctx context.Context) (*progress, *pauseRow, error) {
	back := newBackoff(s.cfg.RetryInitial, s.cfg.RetryMax)
	for {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		cp, p, err := s.loadProgress(ctx)
		if err == nil {
			return cp, p, nil
		}
		d := back.next()
		s.setState(StateRetrying, "load_progress")
		s.logger.Warn("read indexer state failed; retrying",
			"chain_id", s.chainID, "attempt", back.attempt, "retry_in", d,
			"error", logx.Redact(err.Error()))
		if !s.wait(ctx, d) {
			return nil, nil, ctx.Err()
		}
	}
}

// loadProgress reads the durable checkpoint (nil = empty progress) and the
// pause row (nil = not paused).
func (s *Scanner) loadProgress(ctx context.Context) (*progress, *pauseRow, error) {
	var (
		height, startHeight int64
		hash                string
	)
	cp := (*progress)(nil)
	err := s.pool.QueryRow(ctx, readCheckpointSQL, s.chainID).Scan(&height, &hash, &startHeight)
	switch {
	case err == nil:
		cp = &progress{height: uint64(height), hash: hash, startHeight: uint64(startHeight)}
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return nil, nil, fmt.Errorf("read checkpoint: %w", err)
	}

	var (
		pHeight          int64
		expected, actual string
		kind, detail     string
	)
	p := (*pauseRow)(nil)
	err = s.pool.QueryRow(ctx, readPauseSQL, s.chainID).Scan(&pHeight, &expected, &actual, &kind, &detail)
	switch {
	case err == nil:
		p = &pauseRow{
			height: uint64(pHeight), expected: expected, actual: actual,
			kind: kind, detail: detail,
		}
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return nil, nil, fmt.Errorf("read pause row: %w", err)
	}
	return cp, p, nil
}

// blockWrite is one block to persist.
type blockWrite struct {
	number uint64
	hash   string
	parent string
	first  bool // no checkpoint row exists yet; number must equal start height
}

// hashMismatchError reports that the same height is already stored with a
// different hash (FR-12 first arm). Post-006-migration a height may hold a
// non-canonical sibling; stored then names the canonical hash at the height
// (the view the chain diverged from), never an arbitrary sibling.
type hashMismatchError struct {
	stored string
	actual string
}

func (e *hashMismatchError) Error() string {
	return fmt.Sprintf("stored block hash %s differs from chain hash %s at the same height", e.stored, e.actual)
}

// commitBlock persists one header and advances (or creates) the checkpoint in
// a single short transaction following data-model.md's 5-step write protocol.
// There is no special case for an uncertain COMMIT: the next iteration
// re-fetches the same height and the exact guard settles what actually
// committed, making reprocessing idempotent (FR-07/FR-13).
//
// rc carries the loop's pre-inputs recovery capture (006 capture-first
// discipline); direct callers omit it and the commit captures at entry
// (see resolveRecoveryCapture).
func (s *Scanner) commitBlock(ctx context.Context, w blockWrite, rc ...RecoveryCapture) error {
	rcap, err := resolveRecoveryCapture(ctx, s.pool, s.chainID, rc)
	if err != nil {
		return err
	}
	if w.first && w.number != s.cfg.StartHeight {
		return errStaleState
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin block transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after COMMIT
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return fmt.Errorf("block transaction statement guard: %w", err)
	}

	// Step 2: idempotently ensure the coordination row exists.
	if _, err := tx.Exec(ctx, ensureLeaseSQL, s.chainID, s.lease.ownerID, s.lease.Token(), s.lease.ttl.Seconds()); err != nil {
		return fmt.Errorf("ensure coordination row: %w", err)
	}
	// Step 3: take the chain-wide coordination lock (held to COMMIT).
	var (
		owner string
		token int64
		valid bool
	)
	err = tx.QueryRow(ctx, lockCoordSQL, s.chainID).Scan(&owner, &token, &valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: coordination row missing", ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("lock coordination row: %w", err)
	}

	// Step 4: independent statements re-read and adjudicate. Read Committed
	// gives each statement a fresh snapshot that includes every transaction
	// committed while we waited for the lock.
	var one int
	err = tx.QueryRow(ctx, pauseExistsSQL, s.chainID).Scan(&one)
	if err == nil {
		return errPaused
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("pause verdict: %w", err)
	}
	err = tx.QueryRow(ctx, leaseVerdictSQL, s.chainID, s.lease.ownerID, s.lease.Token()).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: owner/fencing/expiry verdict failed", ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("lease verdict: %w", err)
	}
	// 006 recovery gate (T013): capture-first/commit-triple beside the
	// existing verdicts. An ordinary batch refuses on any active row or
	// version mismatch with zero writes and zero progress, even on full
	// content coincidence; recomputation starts a new batch upstream.
	if _, err := recheckRecoveryGate(ctx, tx, s.chainID, rcap); err != nil {
		return err
	}
	if w.first {
		// First block: no checkpoint row may exist at all, and the number is
		// S (checked above); no parent check for this boundary (FR-05).
		err = tx.QueryRow(ctx, checkpointExistsSQL, s.chainID).Scan(&one)
		if err == nil {
			return errStaleState
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("first-block verdict: %w", err)
		}
	} else {
		// Advance: checkpoint must be exactly (n-1, parent) (FR-05, I4/I6).
		err = tx.QueryRow(ctx, guardExactSQL, s.chainID, w.number-1, w.parent).Scan(&one)
		if errors.Is(err, pgx.ErrNoRows) {
			return errStaleState
		}
		if err != nil {
			return fmt.Errorf("checkpoint guard: %w", err)
		}
	}

	// Step 5: write the block sibling-aware (006 R2 linkage), re-read the
	// same height inside the transaction and never trust in-memory knowledge
	// (FR-07: idempotent convergence). Same number+hash is an idempotent
	// rescan (DO NOTHING, correct); same number + different hash inserts a
	// sibling row (no conflict) as fork evidence for the adjudication below.
	if _, err := tx.Exec(ctx, insertBlockSQL, s.chainID, w.number, w.hash, w.parent); err != nil {
		return fmt.Errorf("insert block: %w", err)
	}
	storedHashes, err := queryBlockHashes(ctx, tx, s.chainID, w.number)
	if err != nil {
		return fmt.Errorf("re-read block hashes: %w", err)
	}
	if len(storedHashes) != 1 || storedHashes[0] != w.hash {
		// Sibling fork evidence at this height (T008): the recovery gate
		// above already refused every batch under an active recovery, so
		// reaching here means the ordinary path owns this evidence — route
		// to the preserved hash_mismatch pause.
		stored, qerr := canonicalHashAtHeight(ctx, tx, s.chainID, w.number, storedHashes, w.hash)
		if qerr != nil {
			return qerr
		}
		return &hashMismatchError{stored: stored, actual: w.hash}
	}

	if w.first {
		if _, err := tx.Exec(ctx, insertCheckpointSQL, s.chainID, w.number, w.hash, s.cfg.StartHeight); err != nil {
			return fmt.Errorf("insert checkpoint: %w", err)
		}
	} else {
		tag, err := tx.Exec(ctx, advanceCheckpointSQL, s.chainID, w.number, w.hash, w.number-1, w.parent)
		if err != nil {
			return fmt.Errorf("advance checkpoint: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return errStaleState
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit block %d: %w", w.number, err)
	}
	return nil
}

// queryBlockHashes lists every stored hash at a height in hash order.
// Callers run it inside their write transaction so the snapshot includes
// the row just written above.
func queryBlockHashes(ctx context.Context, tx pgx.Tx, chainID int64, number uint64) ([]string, error) {
	rows, err := tx.Query(ctx, siblingHashesSQL, chainID, number)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// canonicalHashAtHeight names the stored hash a same-height divergence is
// reported against: the canonical hash when one is effective, otherwise the
// first foreign hash from the in-txn sibling list (no extra read needed).
func canonicalHashAtHeight(ctx context.Context, tx pgx.Tx, chainID int64, number uint64, siblings []string, actual string) (string, error) {
	var stored string
	err := tx.QueryRow(ctx, canonicalHashSQL, chainID, number).Scan(&stored)
	switch {
	case err == nil:
		return stored, nil
	case errors.Is(err, pgx.ErrNoRows):
		for _, h := range siblings {
			if h != actual {
				return h, nil
			}
		}
		return actual, nil
	default:
		return "", fmt.Errorf("re-read canonical hash: %w", err)
	}
}

// pauseInfo describes one divergence to persist.
type pauseInfo struct {
	kind     string
	height   uint64
	expected string
	actual   string
	detail   string
}

// pause persists the pause (best effort -- stopping is unconditional) and
// returns the stop error. Pause persistence follows the unified write
// protocol and re-verifies the divergence under the coordination lock
// (data-model.md Table 4).
func (s *Scanner) pause(ctx context.Context, p pauseInfo) error {
	s.setState(StatePaused, p.kind)
	s.logger.Error("chain divergence detected; pausing",
		"chain_id", s.chainID, "height", p.height, "kind", p.kind,
		"expected_hash", p.expected, "actual_hash", p.actual)
	switch err := s.commitPause(ctx, p); {
	case err == nil:
	case errors.Is(err, ErrLeaseLost):
		return fmt.Errorf("%w (persisting pause: %v)", ErrLeaseLost, err)
	case errors.Is(err, errPaused):
		// Another pause row already exists: the desired end state.
	case errors.Is(err, errStaleState):
		s.logger.Warn("pause evidence changed before it was persisted; stopping anyway",
			"chain_id", s.chainID, "height", p.height, "kind", p.kind)
	default:
		s.logger.Error("persist pause failed",
			"chain_id", s.chainID, "height", p.height, "kind", p.kind,
			"error", logx.Redact(err.Error()))
	}
	return fmt.Errorf("%w: %s at height %d: expected %s, actual %s",
		errPaused, p.kind, p.height, p.expected, p.actual)
}

// commitPause persists a pause row through the unified write protocol: ensure
// the coordination row, lock it, re-verify that the divergence evidence still
// holds, then insert. If the evidence is gone the pause is abandoned and
// errStaleState is returned.
func (s *Scanner) commitPause(ctx context.Context, p pauseInfo) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin pause transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return fmt.Errorf("pause transaction statement guard: %w", err)
	}
	if _, err := tx.Exec(ctx, ensureLeaseSQL, s.chainID, s.lease.ownerID, s.lease.Token(), s.lease.ttl.Seconds()); err != nil {
		return fmt.Errorf("ensure coordination row: %w", err)
	}
	var (
		owner string
		token int64
		valid bool
	)
	err = tx.QueryRow(ctx, lockCoordSQL, s.chainID).Scan(&owner, &token, &valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: coordination row missing", ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("lock coordination row: %w", err)
	}

	var one int
	err = tx.QueryRow(ctx, pauseExistsSQL, s.chainID).Scan(&one)
	if err == nil {
		return errPaused
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("pause verdict: %w", err)
	}
	err = tx.QueryRow(ctx, leaseVerdictSQL, s.chainID, s.lease.ownerID, s.lease.Token()).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: owner/fencing/expiry verdict failed", ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("lease verdict: %w", err)
	}

	// Re-verify the divergence still holds under the lock. The chain side was
	// observed outside the transaction; only the durable side is re-read here.
	var (
		height, startHeight int64
		hash                string
	)
	switch p.kind {
	case pauseHashMismatch:
		var stored string
		err := tx.QueryRow(ctx, canonicalHashSQL, s.chainID, p.height).Scan(&stored)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && (stored != p.expected || stored == p.actual)) {
			return errStaleState
		}
		if err != nil {
			return fmt.Errorf("pause re-read block: %w", err)
		}
	case pauseParentMismatch:
		if p.height == 0 {
			return errStaleState
		}
		err := tx.QueryRow(ctx, readCheckpointSQL, s.chainID).Scan(&height, &hash, &startHeight)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && (height != int64(p.height-1) || hash != p.expected)) {
			return errStaleState
		}
		if err != nil {
			return fmt.Errorf("pause re-read checkpoint: %w", err)
		}
	case pauseCheckpointChanged:
		err := tx.QueryRow(ctx, readCheckpointSQL, s.chainID).Scan(&height, &hash, &startHeight)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && (height != int64(p.height) || hash != p.expected)) {
			return errStaleState
		}
		if err != nil {
			return fmt.Errorf("pause re-read checkpoint: %w", err)
		}
	default:
		return fmt.Errorf("unknown pause kind %q", p.kind)
	}

	if _, err := tx.Exec(ctx, insertPauseSQL, s.chainID, p.height, p.expected, p.actual, p.kind, p.detail); err != nil {
		return fmt.Errorf("insert pause row: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit pause: %w", err)
	}
	return nil
}

// reportPaused logs the durable pause row and returns the stop error; it is
// used both at startup and when a pause row appears mid-run (FR-12).
func (s *Scanner) reportPaused(p *pauseRow) error {
	s.setState(StatePaused, p.kind)
	s.logger.Error("indexer is paused; refusing to advance",
		"chain_id", s.chainID, "height", p.height, "kind", p.kind,
		"expected_hash", p.expected, "actual_hash", p.actual, "detail", p.detail)
	return fmt.Errorf("%w: %s at height %d (expected %s, actual %s)",
		errPaused, p.kind, p.height, p.expected, p.actual)
}

// wait sleeps for d and reports false when ctx is done, so shutdown aborts
// every wait and backoff immediately.
func (s *Scanner) wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

var (
	// errPaused means the chain must not advance: a durable pause row exists
	// or divergence was just detected and persisted.
	errPaused = errors.New("indexer paused")
	// errStaleState means a write's preconditions changed under it; the
	// caller must re-read durable state and decide again.
	errStaleState = errors.New("indexer state changed")
)

// backoff is a capped exponential backoff with +-25% jitter. Its zero state is
// usable; reset arms it for a new run of failures.
type backoff struct {
	initial time.Duration
	max     time.Duration
	current time.Duration
	attempt int
}

func newBackoff(initial, max time.Duration) *backoff {
	return &backoff{initial: initial, max: max}
}

func (b *backoff) reset() {
	b.current = 0
	b.attempt = 0
}

// next returns the jittered wait for the current attempt and doubles the base
// for the next one, capped at max.
func (b *backoff) next() time.Duration {
	b.attempt++
	base := b.current
	if base <= 0 {
		base = b.initial
	}
	if base > b.max {
		base = b.max
	}
	next := base * 2
	if next > b.max {
		next = b.max
	}
	b.current = next
	return jitter(base)
}

// jitter applies +-25% to d; durations below the quantum are unchanged.
func jitter(d time.Duration) time.Duration {
	span := d / 4
	if span <= 0 {
		return d
	}
	return d - span + time.Duration(mathrand.Int64N(int64(2*span)+1))
}

// isRetryable reports whether err is a transient RPC failure that must be
// retried with backoff. Unknown and non-transient errors stop the scan
// (FR-09: config/auth errors are never retried forever).
func isRetryable(err error) bool {
	switch eth.KindOf(err) {
	case eth.KindTransport, eth.KindTimeout, eth.KindRateLimited:
		return true
	default:
		return false
	}
}

// isNotFound reports the wait polarity: the requested height has no block yet.
func isNotFound(err error) bool { return eth.KindOf(err) == eth.KindNotFound }

// checkHeader validates the fields the index relies on: the returned height
// must equal the requested one and both hashes must have the storage format.
// Genesis' all-zero parent is a valid hash.
func checkHeader(h *types.Header, number uint64) error {
	if h == nil {
		return errors.New("header is nil")
	}
	if h.Number == nil || !h.Number.IsUint64() || h.Number.Uint64() != number {
		return fmt.Errorf("header height does not match requested height %d", number)
	}
	if !validHash(hashHex(h.Hash())) {
		return errors.New("header hash is not 0x-prefixed 32-byte lowercase hex")
	}
	if !validHash(hashHex(h.ParentHash)) {
		return errors.New("header parent hash is not 0x-prefixed 32-byte lowercase hex")
	}
	return nil
}

// hashHex renders a hash in the storage format (lowercase, 0x-prefixed).
func hashHex(h common.Hash) string { return strings.ToLower(h.Hex()) }

// validHash matches the chain_blocks CHECK constraint format.
func validHash(s string) bool {
	if len(s) != 66 || !strings.HasPrefix(s, "0x") || s != strings.ToLower(s) {
		return false
	}
	_, err := hex.DecodeString(s[2:])
	return err == nil
}

// sameProgress reports whether two checkpoint snapshots describe the same
// durable row.
func sameProgress(a, b *progress) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return a.height == b.height && a.hash == b.hash
	}
}

// Write-transaction SQL, following data-model.md's 5-step protocol. RPC is
// always done outside these transactions.

const (
	// writeGuard bounds every statement inside a scanner write transaction.
	writeGuard = "SET LOCAL statement_timeout = '5s'"

	// ensureLeaseSQL is write-protocol step 2: idempotently make sure the
	// coordination row exists so step 3 always has a row to lock.
	ensureLeaseSQL = `
INSERT INTO indexer_lease (chain_id, owner_id, fencing_token, expires_at)
VALUES ($1, $2, $3, now() + make_interval(secs => $4))
ON CONFLICT (chain_id) DO NOTHING`

	// lockCoordSQL is write-protocol step 3: take the chain-wide coordination
	// lock and re-read the current owner under it.
	lockCoordSQL = `
SELECT owner_id, fencing_token, expires_at > now()
FROM indexer_lease WHERE chain_id = $1 FOR UPDATE`

	// leaseVerdictSQL is write-protocol step 4: the owner/token/expiry
	// verdict, issued as an independent statement after the lock.
	leaseVerdictSQL = `
SELECT 1 FROM indexer_lease
WHERE chain_id = $1 AND owner_id = $2 AND fencing_token = $3 AND expires_at > now()`

	// pauseExistsSQL is write-protocol step 4: a durable pause rejects every
	// advance.
	pauseExistsSQL = `SELECT 1 FROM indexer_pause WHERE chain_id = $1`

	// guardExactSQL is write-protocol step 4: exact height and continuity
	// guard for an advancing write (height = n-1, hash = parent).
	guardExactSQL = `
SELECT 1 FROM indexer_checkpoint
WHERE chain_id = $1 AND height = $2 AND block_hash = $3`

	// checkpointExistsSQL is write-protocol step 4 for the first block: no
	// checkpoint row may exist (n == S is asserted in application code).
	checkpointExistsSQL = `SELECT 1 FROM indexer_checkpoint WHERE chain_id = $1`

	insertBlockSQL = `
INSERT INTO chain_blocks (chain_id, number, hash, parent_hash)
VALUES ($1, $2, $3, $4)
ON CONFLICT (chain_id, number, hash) DO NOTHING`

	// siblingHashesSQL lists every stored hash at a height (post-006 the PK
	// holds both forks). Rows arrive in hash order so the single-row case
	// is deterministic.
	siblingHashesSQL = `SELECT hash FROM chain_blocks WHERE chain_id = $1 AND number = $2 ORDER BY hash`

	// canonicalHashSQL reads the single canonical hash at a height (the
	// partial UNIQUE guarantees at most one row; no row means no canonical
	// view is effective there).
	canonicalHashSQL = `SELECT hash FROM chain_blocks WHERE chain_id = $1 AND number = $2 AND canonical`

	insertCheckpointSQL = `
INSERT INTO indexer_checkpoint (chain_id, height, block_hash, start_height)
VALUES ($1, $2, $3, $4)`

	advanceCheckpointSQL = `
UPDATE indexer_checkpoint
SET height = $2, block_hash = $3, updated_at = now()
WHERE chain_id = $1 AND height = $4 AND block_hash = $5`

	readCheckpointSQL = `
SELECT height, block_hash, start_height FROM indexer_checkpoint WHERE chain_id = $1`

	readPauseSQL = `
SELECT height, expected_hash, actual_hash, kind, detail FROM indexer_pause WHERE chain_id = $1`

	insertPauseSQL = `
INSERT INTO indexer_pause (chain_id, height, expected_hash, actual_hash, kind, detail)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (chain_id) DO NOTHING`
)
