// reorg.go implements the 006 recovery executor (T010): the phase-machine
// driver over the reorgcommit.go transaction catalog, plus the read-only
// ancestor search (R5, OQ1-drive, Q1 depth anchor). It calls T011's
// transaction functions (T011 executes first by dependency); no forward
// reference escapes this batch.
//
// The search is read-only and holds no lock: it walks down from the bound
// old tip while depth <= max_depth, comparing local canonical (number, hash)
// against chain RPC (number, hash). Evidence classes reuse the 003 failure
// taxonomy — retryable waits bounded, contradictory/insufficient evidence
// waits and never invalidates. Timing reuses newBackoff + the deposit default
// triple (poll 1s / initial 200ms / max 30s, loopTimings clamp, startup-only)
// per the research R5 timing table — no new knob names.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/logx"
)

// Recovery timing (research R5 table): the deposit default triple, reused
// verbatim — poll 1s, retryInitial 200ms, retryMax 30s. Startup-only; the
// clamp mirrors loopTimings (non-positive → defaults, max < initial → max =
// initial), so an unchanged tip never hot-spins and no new knob is born.
const (
	recoveryPollInterval  = time.Second
	recoveryRetryInitial  = 200 * time.Millisecond
	recoveryRetryMax      = 30 * time.Second
	recoveryReplayHeights = 500
)

// reorgDepth computes the Q1 depth as the exact difference bound_old_tip −
// ancestor with a pre-assertion ancestor <= bound_old_tip (violation =
// internal error, refuse). Both inputs are BIGINT CHECK (>= 0) columns, so
// the result lies in [0, MaxInt64]: no underflow given the assertion, no
// overflow (result <= bound_old_tip <= MaxInt64). Saturation/clamping
// arithmetic is never the audit basis — the compared value is always exact.
func reorgDepth(boundOldTip, ancestor int64) (int64, error) {
	if ancestor < 0 || boundOldTip < 0 {
		return 0, fmt.Errorf("reorg depth: negative input bound=%d ancestor=%d", boundOldTip, ancestor)
	}
	if ancestor > boundOldTip {
		return 0, fmt.Errorf("reorg depth: ancestor %d above bound tip %d", ancestor, boundOldTip)
	}
	return boundOldTip - ancestor, nil
}

// RecoveryConfig wires one chain's executor. MaxDepthRaw is the Q1 value in
// raw form (parsed once at construction — invalid configs refuse startup).
// Intervals clamp like loopTimings and are startup-only. BlockStartHeight is
// the 002 scan start the executor falls back to when the header checkpoint
// row was deleted for a pre-start floor (an existing config value passed
// through, not a new knob).
type RecoveryConfig struct {
	ChainID          int64
	MaxDepthRaw      string
	PollInterval     time.Duration
	RetryInitial     time.Duration
	RetryMax         time.Duration
	ReplayHeights    int
	BlockStartHeight uint64
}

// RecoveryExecutor drives one chain's recovery instance (single deployment
// single chain; multi-executor contention converges in the transactions).
type RecoveryExecutor struct {
	pool   *pgxpool.Pool
	lease  *Lease
	header HeaderClient
	logs   LogsClient
	cfg    RecoveryConfig
	depth  int64
	poll   time.Duration
	retryI time.Duration
	retryM time.Duration
	batch  int
}

// NewRecoveryExecutor validates without I/O: Q1 depth rules apply here, so a
// missing/zero/negative/non-integer/out-of-representation depth refuses
// startup before any recovery can bind it.
func NewRecoveryExecutor(pool *pgxpool.Pool, lease *Lease, header HeaderClient, logs LogsClient, cfg RecoveryConfig) (*RecoveryExecutor, error) {
	if pool == nil {
		return nil, errors.New("recovery executor: nil pool")
	}
	if lease == nil {
		return nil, errors.New("recovery executor: nil lease")
	}
	if header == nil {
		return nil, errors.New("recovery executor: nil header client")
	}
	if logs == nil {
		return nil, errors.New("recovery executor: nil logs client")
	}
	if cfg.ChainID <= 0 {
		return nil, fmt.Errorf("recovery executor: chain id %d must be > 0", cfg.ChainID)
	}
	depth, err := ParseReorgMaxDepth(cfg.MaxDepthRaw)
	if err != nil {
		return nil, fmt.Errorf("recovery executor: %w: %v", ErrReorgPolicyRejected, err)
	}
	poll, retryI, retryM := cfg.PollInterval, cfg.RetryInitial, cfg.RetryMax
	if poll <= 0 {
		poll = recoveryPollInterval
	}
	if retryI <= 0 {
		retryI = recoveryRetryInitial
	}
	if retryM <= 0 {
		retryM = recoveryRetryMax
	}
	if retryM < retryI {
		retryM = retryI
	}
	batch := cfg.ReplayHeights
	if batch <= 0 {
		batch = recoveryReplayHeights
	}
	return &RecoveryExecutor{
		pool: pool, lease: lease, header: header, logs: logs, cfg: cfg,
		depth: int64(depth), poll: poll, retryI: retryI, retryM: retryM, batch: batch,
	}, nil
}

// NewRecoveryLoop adapts the executor to the coordinator ServeFunc shape
// (T017 wiring): the loop rides the existing RunQuatro discipline — same
// acquisition, same heartbeat, no new lock order.
func NewRecoveryLoop(pool *pgxpool.Pool, lease *Lease, header HeaderClient, logs LogsClient, cfg RecoveryConfig) (ServeFunc, error) {
	ex, err := NewRecoveryExecutor(pool, lease, header, logs, cfg)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, checkLost func() error) error {
		return ex.ServeLoop(ctx, checkLost)
	}, nil
}

// ancestorResult is one read-only search outcome.
type ancestorResult struct {
	// found: ancestor pinned with dual equality + continuities.
	number int64
	hash   string
	// evidence refs for the confirm transaction detail.
	evidence string
	// terminal: unrecoverable (over-deep / local-exhausted / below-start).
	terminalCause  string
	terminalDetail string
	// hold: evidence-insufficient — wait, never invalidate.
	hold bool
}

// searchAncestor walks down from the bound tip read-only and off-lock. It
// issues zero writes; every height costs at most two point reads (one local,
// one RPC) plus bounded backoff waits on retryable failures.
func (e *RecoveryExecutor) searchAncestor(ctx context.Context, boundNumber int64, boundHash string, scanStart int64) ancestorResult {
	back := newBackoff(e.retryI, e.retryM)
	local := map[int64][2]string{} // number -> (hash, parent)
	chain := map[int64][2]string{}
	for d := int64(0); ; d++ {
		if ctx.Err() != nil {
			return ancestorResult{hold: true}
		}
		if d > e.depth {
			return ancestorResult{
				terminalCause:  "over_depth",
				terminalDetail: fmt.Sprintf("walked=%d-%d bound=%d:%s max_depth=%d", boundNumber-e.depth, boundNumber, boundNumber, boundHash, e.depth),
			}
		}
		h := boundNumber - d
		if h < scanStart {
			return ancestorResult{
				terminalCause:  "below_scan_start",
				terminalDetail: fmt.Sprintf("walked=%d-%d start=%d bound=%d:%s", h+1, boundNumber, scanStart, boundNumber, boundHash),
			}
		}
		lh, lp, ok, err := e.localBlock(ctx, h)
		if err != nil {
			if ctx.Err() != nil {
				return ancestorResult{hold: true}
			}
			d := back.next()
			slog.Warn("ancestor search local read failed; retrying",
				"chain_id", e.cfg.ChainID, "height", h, "retry_in", d, "error", logx.Redact(err.Error()))
			if !waitCtx(ctx, d) {
				return ancestorResult{hold: true}
			}
			continue
		}
		if !ok {
			return ancestorResult{
				terminalCause:  "local_exhausted",
				terminalDetail: fmt.Sprintf("walked=%d-%d bound=%d:%s no_local_row_at=%d", h+1, boundNumber, boundNumber, boundHash, h),
			}
		}
		ch, cp, retry, holdChain, err := e.chainBlock(ctx, h)
		if err != nil {
			if ctx.Err() != nil {
				return ancestorResult{hold: true}
			}
			if retry {
				d := back.next()
				slog.Warn("ancestor search chain read failed; retrying",
					"chain_id", e.cfg.ChainID, "height", h, "retry_in", d, "error", logx.Redact(err.Error()))
				if !waitCtx(ctx, d) {
					return ancestorResult{hold: true}
				}
				continue
			}
			_ = holdChain
			// Evidence-insufficient (chain-mismatch and friends): hold the
			// position with a plain poll wait — nothing to back off, and the
			// manual reconcile path stays the escape hatch.
			if !waitCtx(ctx, e.poll) {
				return ancestorResult{hold: true}
			}
			continue
		}
		local[h] = [2]string{lh, lp}
		chain[h] = [2]string{ch, cp}
		back.reset()
		if lh == ch {
			// First match walking down is the convergence point — but only
			// with continuous parent linkage on both sides over the walked
			// suffix (a lone equality without linkage proves nothing).
			if err := checkSuffixContinuity(local, chain, h+1, boundNumber); err != nil {
				slog.Warn("ancestor suffix discontinuous; holding",
					"chain_id", e.cfg.ChainID, "ancestor", h, "error", logx.Redact(err.Error()))
				if !waitCtx(ctx, e.poll) {
					return ancestorResult{hold: true}
				}
				continue
			}
			depth, derr := reorgDepth(boundNumber, h)
			if derr != nil {
				return ancestorResult{hold: true}
			}
			return ancestorResult{
				number: h, hash: lh,
				evidence: fmt.Sprintf("bound=%d:%s ancestor=%d:%s depth=%d walked=%d-%d",
					boundNumber, boundHash, h, lh, depth, h, boundNumber),
			}
		}
	}
}

// checkSuffixContinuity requires every height in (from,to] to link to its
// predecessor on both the local and the chain side.
func checkSuffixContinuity(local, chain map[int64][2]string, from, to int64) error {
	for h := from; h <= to; h++ {
		l, lok := local[h]
		lp, pok := local[h-1]
		c, cok := chain[h]
		cp, qok := chain[h-1]
		if !lok || !pok || !cok || !qok {
			return fmt.Errorf("suffix gap at %d", h)
		}
		if l[1] != lp[0] {
			return fmt.Errorf("local parent break at %d", h)
		}
		if c[1] != cp[0] {
			return fmt.Errorf("chain parent break at %d", h)
		}
	}
	return nil
}

// localBlock reads one local canonical row (ok=false: no row).
func (e *RecoveryExecutor) localBlock(ctx context.Context, h int64) (hash, parent string, ok bool, err error) {
	rctx, cancel := context.WithTimeout(ctx, e.retryM)
	defer cancel()
	err = e.pool.QueryRow(rctx, `SELECT hash, parent_hash FROM chain_blocks WHERE chain_id = $1 AND number = $2 AND canonical`,
		e.cfg.ChainID, h).Scan(&hash, &parent)
	switch {
	case err == nil:
		return hash, parent, true, nil
	case errors.Is(err, pgx.ErrNoRows):
		return "", "", false, nil
	default:
		return "", "", false, err
	}
}

// chainBlock reads one chain header: retryable transport/timeout/
// rate-limit/parse failures ask for bounded backoff; chain-mismatch and
// anything else hold without backoff (nothing to back off).
func (e *RecoveryExecutor) chainBlock(ctx context.Context, h int64) (hash, parent string, retry, hold bool, err error) {
	rctx, cancel := context.WithTimeout(ctx, e.retryM)
	defer cancel()
	hd, err := e.header.HeaderByNumber(rctx, big.NewInt(h))
	if err != nil {
		if ctx.Err() != nil {
			return "", "", false, true, err
		}
		switch eth.KindOf(err) {
		case eth.KindTransport, eth.KindTimeout, eth.KindRateLimited, eth.KindInvalidResponse:
			return "", "", true, false, err
		default:
			return "", "", false, true, err
		}
	}
	return strings.ToLower(hd.Hash().Hex()), strings.ToLower(hd.ParentHash.Hex()), false, false, nil
}

// chainHead reads the live tip (tracked separately from the bound tip; it is
// never a completion input — FR-19).
func (e *RecoveryExecutor) chainHead(ctx context.Context) (int64, string, error) {
	rctx, cancel := context.WithTimeout(ctx, e.retryM)
	defer cancel()
	hd, err := e.header.HeaderByNumber(rctx, nil)
	if err != nil {
		return 0, "", err
	}
	if !hd.Number.IsInt64() {
		return 0, "", fmt.Errorf("chain head %s out of int64 range", hd.Number.String())
	}
	return hd.Number.Int64(), strings.ToLower(hd.Hash().Hex()), nil
}

// ServeLoop drives the persisted phase machine (T010): every tick re-reads
// the durable row and advances at most one stage per stream, so a kill -9
// between any two phases restarts from persisted phase + frontiers with no
// redone committed phase and no skipped phase.
func (e *RecoveryExecutor) ServeLoop(ctx context.Context, checkLost func() error) error {
	if checkLost == nil {
		checkLost = func() error { return nil }
	}
	back := newBackoff(e.retryI, e.retryM)
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := checkLost(); err != nil {
			return err
		}
		row, err := readRecoveryRow(ctx, e.pool, e.cfg.ChainID)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !waitCtx(ctx, back.next()) {
				return nil
			}
			continue
		}
		if row == nil {
			if done, rerr := e.tickIdle(ctx); rerr != nil {
				return rerr
			} else if done {
				back.reset()
			} else if !waitCtx(ctx, e.poll) {
				return nil
			}
			continue
		}
		cap := RecoveryCapture{Seq: row.Seq, Owned: &RecoveryOwned{RecoveryID: row.RecoveryID}}
		var terr error
		switch row.Phase {
		case reorgPhaseDetected:
			terr = e.tickDetected(ctx, row)
		case reorgPhaseAncestorConfirmed:
			terr = e.tickInvalidate(ctx, row, cap)
		case reorgPhaseInvalidated, reorgPhaseReplaying:
			terr = e.tickReplay(ctx, row, cap)
		case reorgPhaseCompletePending:
			terr = e.tickComplete(ctx, row, cap)
		case reorgPhaseReconcileRequired:
			// Manual path owns reconcile-held recoveries: hold with a plain
			// poll wait (auth_enter_repair / auth_release are operator
			// transactions, never loop actions).
			if !waitCtx(ctx, e.poll) {
				return nil
			}
			continue
		default:
			terr = fmt.Errorf("recovery %s in unknown phase %q", row.RecoveryID, row.Phase)
		}
		if terr != nil {
			var gate *RecoveryGateError
			if errors.As(terr, &gate) {
				// Version/phase moved under us (concurrent executor won the
				// race): re-read and continue — the loser fails safe.
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("recovery tick failed; retrying",
				"chain_id", e.cfg.ChainID, "recovery", row.RecoveryID,
				"phase", row.Phase, "error", logx.Redact(terr.Error()))
			if !waitCtx(ctx, back.next()) {
				return nil
			}
			continue
		}
		back.reset()
	}
}

// tickIdle establishes on fork evidence (an ordinary-path hash_mismatch
// pause) and idles otherwise. The bound tip is the persisted canonical tip;
// the live tip is tracked separately and never enters completion.
func (e *RecoveryExecutor) tickIdle(ctx context.Context) (done bool, err error) {
	var kind string
	var height int64
	err = e.pool.QueryRow(ctx, `SELECT kind, height FROM indexer_pause WHERE chain_id = $1`, e.cfg.ChainID).Scan(&kind, &height)
	if err != nil {
		// No pause row (or an unreadable one): idle. Either way ordinary
		// work is governed by its own paths; establishment needs evidence.
		return false, nil
	}
	if kind != "hash_mismatch" {
		return false, nil
	}
	var boundNumber int64
	var boundHash string
	if err := e.pool.QueryRow(ctx, readConfirmationTipSQL, e.cfg.ChainID).Scan(&boundNumber, &boundHash); err != nil {
		return false, nil // no trusted bound tip yet: idle
	}
	newNumber, newHash, herr := e.chainHead(ctx)
	if herr != nil {
		return false, nil // chain unreadable: idle (ordinary pause holds work)
	}
	res, err := EstablishRecovery(ctx, e.pool, e.lease, EstablishRequest{
		ChainID:      e.cfg.ChainID,
		OldTipNumber: boundNumber, OldTipHash: boundHash,
		NewTipNumber: newNumber, NewTipHash: newHash,
		DetectedHeight: height, EnvMaxDepthRaw: e.cfg.MaxDepthRaw,
	})
	if err != nil {
		var expired error = ErrReorgPolicyExpired
		var rejected error = ErrReorgPolicyRejected
		if errors.Is(err, expired) || errors.Is(err, rejected) {
			// Configuration/authorization refusal: loud stop, never silent.
			return false, err
		}
		return false, nil // contention/transient: idle-retry next tick
	}
	slog.Info("recovery established",
		"chain_id", e.cfg.ChainID, "recovery", res.RecoveryID, "converged", res.Converged)
	return true, nil
}

// tickDetected runs the read-only search and pins or holds.
func (e *RecoveryExecutor) tickDetected(ctx context.Context, row *RecoveryRow) error {
	start, err := e.scanStart(ctx)
	if err != nil {
		return nil // unreadable: idle-retry next tick
	}
	res := e.searchAncestor(ctx, row.BoundOldNumber, row.BoundOldHash, start)
	cap := RecoveryCapture{Seq: row.Seq, Owned: &RecoveryOwned{RecoveryID: row.RecoveryID}}
	switch {
	case res.hold:
		return nil // evidence-insufficient: hold position, never invalidate
	case res.terminalCause != "":
		return SignalRecoveryReconcile(ctx, e.pool, e.lease, e.cfg.ChainID, cap,
			res.terminalCause, res.terminalDetail, res.terminalDetail)
	default:
		if cerr := ConfirmRecoveryAncestor(ctx, e.pool, e.lease, e.cfg.ChainID, cap, res.number, res.hash, res.evidence); cerr != nil {
			var gate *RecoveryGateError
			if errors.As(cerr, &gate) && strings.Contains(gate.detail, "exceeds bound") {
				return SignalRecoveryReconcile(ctx, e.pool, e.lease, e.cfg.ChainID, cap,
					"over_depth", res.evidence, res.evidence)
			}
			return cerr
		}
		slog.Info("recovery ancestor confirmed",
			"chain_id", e.cfg.ChainID, "recovery", row.RecoveryID,
			"ancestor", res.number, "hash", res.hash)
		return nil
	}
}

// tickInvalidate runs the invalidate + rollback sequence (each step
// idempotent: re-runs converge on guards and rowcounts).
func (e *RecoveryExecutor) tickInvalidate(ctx context.Context, row *RecoveryRow, cap RecoveryCapture) error {
	if _, _, _, err := InvalidateRecoveryBlocks(ctx, e.pool, e.lease, e.cfg.ChainID, cap); err != nil {
		return err
	}
	if _, err := InvalidateRecoveryObservations(ctx, e.pool, e.lease, e.cfg.ChainID, cap); err != nil {
		return err
	}
	for _, stream := range []RecoveryStream{RecoveryStreamBlock, RecoveryStreamLog, RecoveryStreamDeposit} {
		if _, _, err := RollbackRecoveryCheckpoint(ctx, e.pool, e.lease, e.cfg.ChainID, cap, stream); err != nil {
			return err
		}
	}
	return nil
}

// tickReplay advances one bounded batch per stream, recanonicalizing
// same-hash heights (with observation revival) instead of re-inserting them.
func (e *RecoveryExecutor) tickReplay(ctx context.Context, row *RecoveryRow, cap RecoveryCapture) error {
	liveTip, err := e.persistedTip(ctx)
	if err != nil {
		return nil // unreadable: idle-retry
	}
	for _, stream := range []RecoveryStream{RecoveryStreamBlock, RecoveryStreamLog, RecoveryStreamDeposit} {
		if err := e.replayStream(ctx, row, cap, stream, liveTip); err != nil {
			return err
		}
		// Re-read the row: the batch may have moved the phase (or a
		// concurrent executor did) — later streams bind fresh state.
		if row, err = readRecoveryRow(ctx, e.pool, e.cfg.ChainID); err != nil || row == nil {
			return err
		}
		cap = RecoveryCapture{Seq: row.Seq, Owned: &RecoveryOwned{RecoveryID: row.RecoveryID}}
		if row.Phase != reorgPhaseInvalidated && row.Phase != reorgPhaseReplaying {
			return nil
		}
	}
	return nil
}

// tickComplete runs the auto release (re-verified FR-19判据 inside).
func (e *RecoveryExecutor) tickComplete(ctx context.Context, row *RecoveryRow, cap RecoveryCapture) error {
	if err := CompleteRecoveryVerify(ctx, e.pool, e.lease, e.cfg.ChainID, cap); err != nil {
		return err
	}
	slog.Info("recovery auto-completed",
		"chain_id", e.cfg.ChainID, "recovery", row.RecoveryID)
	return nil
}

// replayStream replays one bounded range of one stream.
func (e *RecoveryExecutor) replayStream(ctx context.Context, row *RecoveryRow, cap RecoveryCapture, stream RecoveryStream, liveTip int64) error {
	from, ok, err := e.streamResumeFrom(ctx, stream)
	if err != nil {
		return nil // unreadable checkpoint: idle-retry
	}
	if !ok {
		if stream != RecoveryStreamBlock {
			// No-progress stream: nothing was swept for it, nothing to
			// replay (the completion gate treats it as satisfied; its
			// ordinary bootstrap owns it post-release).
			return nil
		}
		// Header checkpoint deleted for a pre-start floor: resume at the
		// bound scan start, fail-closed below.
		from = int64(e.cfg.BlockStartHeight)
		if row.AncestorNumber == nil || from <= *row.AncestorNumber {
			return SignalRecoveryReconcile(ctx, e.pool, e.lease, e.cfg.ChainID, cap,
				"block_checkpoint_missing",
				fmt.Sprintf("no header checkpoint, start=%d ancestor=%v", from, row.AncestorNumber),
				fmt.Sprintf("start=%d", from))
		}
	}
	if row.AncestorNumber != nil {
		if floor := *row.AncestorNumber + 1; from < floor {
			from = floor
		}
	}
	if from > liveTip {
		return nil // nothing persisted to replay yet
	}
	to := from + int64(e.batch) - 1
	if to > liveTip {
		to = liveTip
	}
	// Per-height triage: same-hash non-canonical heights take the
	// recanonicalize + revive path; everything else assembles replay rows.
	var blocks []ReplayBlock
	var logs []ReplayLog
	var observations []ReplayObservation
	for h := from; h <= to; h++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		chainHash, chainParent, retry, _, err := e.chainBlock(ctx, h)
		if err != nil {
			if retry {
				return nil // bounded-wait next tick (backoff in caller)
			}
			// Evidence hold mid-replay: never falsely release; the phase
			// stays replaying and the next tick re-walks the range.
			return nil
		}
		_ = chainParent
		localHash, localCanon, ok, err := e.localBlockView(ctx, h)
		if err != nil {
			return nil
		}
		if ok && !localCanon {
			// A retained row at this height: same-hash → recanonicalize
			// path; foreign-hash → already-invalidated old fork, skip (its
			// replacement arrives as case "no row" only when... see below).
			if localHash == chainHash {
				if err := e.recanonicalizeHeight(ctx, row, cap, h, chainHash); err != nil {
					return err
				}
				continue
			}
			// Foreign non-canonical row: the new-chain height has no row
			// yet — assemble it below (the proof counts the new canonical).
		}
		if ok && localCanon && localHash == chainHash {
			continue // already effective: proof counts it, no write needed
		}
		if ok && localCanon {
			// A canonical foreign row at a replay height means the sweep
			// missed it (tip grew past the sweep, or a post-establish
			// Class-1 commit): hold for a re-sweep decision — never paper
			// over it with a duplicate canonical (partial UNIQUE would
			// refuse anyway; the explicit hold names the cause).
			return SignalRecoveryReconcile(ctx, e.pool, e.lease, e.cfg.ChainID, cap,
				"canon_diverged_mid_replay",
				fmt.Sprintf("range=%d-%d height=%d local=%s chain=%s", from, to, h, localHash, chainHash),
				fmt.Sprintf("height=%d local=%s chain=%s", h, localHash, chainHash))
		}
		blocks = append(blocks, ReplayBlock{Number: h, Hash: chainHash, ParentHash: e.chainParentOf(ctx, h, chainHash)})
	}
	// Log + deposit assembly over the same range (historical semantics).
	var err2 error
	logs, observations, err2 = e.assembleLogsAndDeposits(ctx, from, to)
	if err2 != nil {
		return err2
	}
	// Deposit stream carries no block rows (coverage proves on headers).
	if stream != RecoveryStreamBlock {
		blocks = nil
	}
	if stream == RecoveryStreamBlock {
		logs, observations = nil, nil
	}
	if stream == RecoveryStreamLog {
		observations = nil
	}
	if stream == RecoveryStreamDeposit {
		logs = nil
	}
	return ReplayRecoveryRange(ctx, e.pool, e.lease, e.cfg.ChainID, cap, stream, from, to, blocks, logs, observations)
}

// chainParentOf returns the already-fetched parent for an assembled header
// (re-reads are cheap point RPCs; the txn re-proves canonically anyway).
func (e *RecoveryExecutor) chainParentOf(ctx context.Context, h int64, _ string) string {
	_, parent, _, _, err := e.chainBlock(ctx, h)
	if err != nil {
		return ""
	}
	return parent
}

// recanonicalizeHeight flips one height and revives its orphaned
// observations (Q4 path: reuse, re-verify, audit; ex-Confirmed passes
// through Pending for post-release 005 reconfirmation).
func (e *RecoveryExecutor) recanonicalizeHeight(ctx context.Context, row *RecoveryRow, cap RecoveryCapture, h int64, oldHash string) error {
	if err := RecanonicalizeRecoveryBlock(ctx, e.pool, e.lease, e.cfg.ChainID, cap, h, oldHash); err != nil {
		return err
	}
	identities, err := e.orphansAtBlock(ctx, oldHash)
	if err != nil {
		return err
	}
	for _, id := range identities {
		evidence := fmt.Sprintf("height=%d block=%s canonical_reverified log_binding_reverified", h, oldHash)
		if err := ReviveRecoveryObservation(ctx, e.pool, e.lease, e.cfg.ChainID, cap, id.blockHash, id.txHash, id.logIndex, evidence); err != nil {
			return err
		}
	}
	return nil
}

// assembleLogsAndDeposits fetches new-chain logs over [from,to] (Transfer
// topic, no address pre-filter — historical matching decides) and
// re-identifies deposits under the per-height historical version semantics.
func (e *RecoveryExecutor) assembleLogsAndDeposits(ctx context.Context, from, to int64) ([]ReplayLog, []ReplayObservation, error) {
	raw, err := e.logs.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: big.NewInt(from),
		ToBlock:   big.NewInt(to),
		Topics:    [][]common.Hash{{eth.TransferSig}},
	})
	if err != nil {
		return nil, nil, nil // evidence hold: skip log/deposit assembly this tick
	}
	var logs []ReplayLog
	byHeight := map[int64][]depositSourceLog{}
	for _, l := range raw {
		if l.BlockNumber > uint64(math.MaxInt64) {
			continue
		}
		h := int64(l.BlockNumber)
		if h < from || h > to {
			continue
		}
		src := depositSourceLog{
			blockNumber: uint64(h),
			blockHash:   strings.ToLower(l.BlockHash.Hex()),
			txHash:      strings.ToLower(l.TxHash.Hex()),
			logIndex:    uint64(l.Index),
			contract:    strings.ToLower(l.Address.Hex()),
			topic0:      strings.ToLower(l.Topics[0].Hex()),
			topic1:      pickTopic(l.Topics, 1),
			topic2:      pickTopic(l.Topics, 2),
			data:        strings.ToLower(common.Bytes2Hex(l.Data)),
		}
		logs = append(logs, ReplayLog{
			BlockNumber: h, BlockHash: src.blockHash, TxHash: src.txHash, LogIndex: int64(l.Index),
			Contract: src.contract, Topic0: src.topic0, Topic1: src.topic1, Topic2: src.topic2, Data: src.data,
		})
		byHeight[h] = append(byHeight[h], src)
	}
	var observations []ReplayObservation
	for h, rows := range byHeight {
		match, versionSeq, ok, err := e.historicalMatch(ctx, h)
		if err != nil || !ok {
			continue // no version bound at h yet: skip (executor retries)
		}
		batch, err := parseDepositLogs(match, rows)
		if err != nil {
			return nil, nil, err // deterministic invalid row: fail the tick loudly
		}
		for _, m := range batch.matched {
			observations = append(observations, ReplayObservation{
				BlockHash: m.blockHash, TxHash: m.txHash, LogIndex: int64(m.logIndex),
				BlockNumber: int64(m.blockNumber), Contract: m.contract, Sender: m.sender,
				Recipient: m.recipient, Amount: m.amount, VersionSeq: versionSeq,
			})
		}
	}
	return logs, observations, nil
}

// historicalMatch rebuilds the match view in effect at height h from the
// version snapshot (FR-09: replay inherits historical semantics, never the
// current config silently).
func (e *RecoveryExecutor) historicalMatch(ctx context.Context, h int64) (depositMatchConfig, int64, bool, error) {
	var versionSeq, startBlock int64
	var assets, watches string
	err := e.pool.QueryRow(ctx, readDepositVersionAtSQL, e.cfg.ChainID, h).Scan(&versionSeq, &startBlock, &assets, &watches)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return depositMatchConfig{}, 0, false, nil
		}
		return depositMatchConfig{}, 0, false, err
	}
	match := depositMatchConfig{startBlock: uint64(startBlock), assets: map[string]uint64{}, watches: map[string]uint64{}}
	if err := parseSnapshotEntries(assets, match.assets); err != nil {
		return depositMatchConfig{}, 0, false, err
	}
	if err := parseSnapshotEntries(watches, match.watches); err != nil {
		return depositMatchConfig{}, 0, false, err
	}
	return match, versionSeq, true, nil
}

// parseSnapshotEntries decodes `<address>:<effective>` snapshot lines
// (config.DepositSnapshot form) back into a match set.
func parseSnapshotEntries(snapshot string, out map[string]uint64) error {
	if strings.TrimSpace(snapshot) == "" {
		return nil
	}
	for _, line := range strings.Split(snapshot, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			return fmt.Errorf("snapshot entry %q is not address:effective", line)
		}
		eff, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil {
			return fmt.Errorf("snapshot entry %q has a non-integer effective height", line)
		}
		out[strings.ToLower(strings.TrimSpace(parts[0]))] = eff
	}
	return nil
}

// Small durable reads shared by the ticks (off-lock planning inputs; every
// write re-proves under the coordination lock).

func (e *RecoveryExecutor) scanStart(ctx context.Context) (int64, error) {
	var start int64
	if err := e.pool.QueryRow(ctx, `SELECT start_height FROM indexer_checkpoint WHERE chain_id = $1`, e.cfg.ChainID).Scan(&start); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return start, nil
}

func (e *RecoveryExecutor) persistedTip(ctx context.Context) (int64, error) {
	var n int64
	if err := e.pool.QueryRow(ctx, `SELECT COALESCE(MAX(number), -1) FROM chain_blocks WHERE chain_id = $1`, e.cfg.ChainID).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (e *RecoveryExecutor) localBlockView(ctx context.Context, h int64) (hash string, canonical bool, ok bool, err error) {
	err = e.pool.QueryRow(ctx, `SELECT hash, canonical FROM chain_blocks WHERE chain_id = $1 AND number = $2 ORDER BY hash LIMIT 2`,
		e.cfg.ChainID, h).Scan(&hash, &canonical)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, false, nil
		}
		return "", false, false, err
	}
	// LIMIT 2 with one Scan: presence of a second row is checked by count.
	var n int64
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM chain_blocks WHERE chain_id = $1 AND number = $2`,
		e.cfg.ChainID, h).Scan(&n); err != nil {
		return "", false, false, err
	}
	if n > 1 {
		// Siblings at this height: name the canonical one (partial UNIQUE:
		// at most one). Non-canonical survivors exclude the height from the
		// "no row" path so the triage above routes correctly.
		if !canonical {
			if err := e.pool.QueryRow(ctx, canonicalHashAtHeightSQL, e.cfg.ChainID, h).Scan(&hash); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return "", false, true, nil // rows exist, none canonical
				}
				return "", false, false, err
			}
			return hash, true, true, nil
		}
	}
	return hash, canonical, true, nil
}

type orphanIdentity struct {
	blockHash string
	txHash    string
	logIndex  int64
}

func (e *RecoveryExecutor) orphansAtBlock(ctx context.Context, blockHash string) ([]orphanIdentity, error) {
	rows, err := e.pool.Query(ctx, `SELECT tx_hash, log_index FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND status = 'orphaned' ORDER BY tx_hash, log_index`, e.cfg.ChainID, blockHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []orphanIdentity
	for rows.Next() {
		var id orphanIdentity
		id.blockHash = blockHash
		if err := rows.Scan(&id.txHash, &id.logIndex); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (e *RecoveryExecutor) streamResumeFrom(ctx context.Context, stream RecoveryStream) (int64, bool, error) {
	switch stream {
	case RecoveryStreamBlock:
		var h int64
		if err := e.pool.QueryRow(ctx, `SELECT height FROM indexer_checkpoint WHERE chain_id = $1`, e.cfg.ChainID).Scan(&h); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return 0, false, nil
			}
			return 0, false, err
		}
		return h + 1, true, nil
	case RecoveryStreamLog:
		var n int64
		if err := e.pool.QueryRow(ctx, `SELECT next_block FROM log_checkpoint WHERE chain_id = $1`, e.cfg.ChainID).Scan(&n); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return 0, false, nil
			}
			return 0, false, err
		}
		return n, true, nil
	default:
		var n int64
		if err := e.pool.QueryRow(ctx, `SELECT next_block FROM deposit_checkpoint WHERE chain_id = $1`, e.cfg.ChainID).Scan(&n); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return 0, false, nil
			}
			return 0, false, err
		}
		return n, true, nil
	}
}

func pickTopic(topics []common.Hash, i int) string {
	if i < len(topics) {
		return strings.ToLower(topics[i].Hex())
	}
	return ""
}
