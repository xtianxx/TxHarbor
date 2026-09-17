// Package metrics wires the minimal Prometheus surface for 001/002: process/go
// collectors plus readiness, dependency probe and chain indexer metrics. No
// business metrics, no histograms, no backlog reservations (FR-019).
package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/xtianxx/txharbor/internal/logx"
)

// Custom metric names (contracts/observability.md freezes the indexer and log
// names).
const (
	ReadyMetricName = "txharbor_ready"
	ProbeMetricName = "txharbor_probe_total"

	IndexerCheckpointMetricName = "txharbor_indexer_checkpoint_height"
	IndexerStateMetricName      = "txharbor_indexer_state"
	IndexerRPCMetricName        = "txharbor_indexer_rpc_total"
	IndexerPauseMetricName      = "txharbor_indexer_pause_total"

	LogCheckpointNextMetricName = "txharbor_log_checkpoint_next"
	LogLagMetricName            = "txharbor_log_lag_blocks"
	LogStateMetricName          = "txharbor_log_state"
	LogRPCMetricName            = "txharbor_log_rpc_total"
	LogPauseMetricName          = "txharbor_log_pause_total"

	DepositNextMetricName         = "txharbor_deposit_next"
	DepositLagMetricName          = "txharbor_deposit_lag_blocks"
	DepositStateMetricName        = "txharbor_deposit_state"
	DepositObservationsMetricName = "txharbor_deposit_observations_total"
	DepositPauseMetricName        = "txharbor_deposit_pause_total"
	DepositTransitionMetricName   = "txharbor_deposit_transition_total"

	ConfirmationPendingMetricName          = "txharbor_confirmation_pending"
	ConfirmationLagMetricName              = "txharbor_confirmation_lag_blocks"
	ConfirmationStateMetricName            = "txharbor_confirmation_state"
	ConfirmationPolicySeqMetricName        = "txharbor_confirmation_policy_seq"
	ConfirmationConfirmedMetricName        = "txharbor_confirmation_confirmed_total"
	ConfirmationSkippedMetricName          = "txharbor_confirmation_skipped_total"
	ConfirmationTransitionMetricName       = "txharbor_confirmation_transition_total"
	ConfirmationPolicyTransitionMetricName = "txharbor_confirmation_policy_transition_total"

	ReorgActiveMetricName       = "txharbor_reorg_active"
	ReorgDepthMetricName        = "txharbor_reorg_depth"
	ReorgBoundMetricName        = "txharbor_reorg_bound"
	ReorgFrontierLagMetricName  = "txharbor_reorg_frontier_lag"
	ReorgOrphanedMetricName     = "txharbor_reorg_orphaned_total"
	ReorgRevivedMetricName      = "txharbor_reorg_revived_total"
	ReorgReconcileMetricName    = "txharbor_reorg_reconcile_required"
	ReorgEvidenceWaitMetricName = "txharbor_reorg_evidence_wait_total"

	// 007 withdrawal intake surface (specs/007-withdrawal-creation, T015). One
	// label-free counter per create outcome; the outcome is the metric name so
	// no caller/request/key/asset/amount can ever become a (high-cardinality or
	// secret-bearing) label value (FR-20/FR-21).
	WithdrawalAcceptedMetricName        = "txharbor_withdrawal_accepted_total"
	WithdrawalReplayedMetricName        = "txharbor_withdrawal_replayed_total"
	WithdrawalConflictMetricName        = "txharbor_withdrawal_conflict_total"
	WithdrawalRejectedMetricName        = "txharbor_withdrawal_rejected_total"
	WithdrawalUnavailableMetricName     = "txharbor_withdrawal_unavailable_total"
	WithdrawalUnauthenticatedMetricName = "txharbor_withdrawal_unauthenticated_total"

	// 008 nonce-manager surface (specs/008-nonce-manager/contracts/
	// observation.md §5; T020). The allocation result label carries the
	// admission's machine reason (allocated, replayed, or the refusal
	// Outcome); classification carries the nonce_observations vocabulary.
	// Both are fixed vocabularies, so no sender/nonce/intent/hold value can
	// ever become a label (FR-21/SC-09).
	NonceAllocationsMetricName  = "txharbor_nonce_allocations_total"
	NonceReplaysMetricName      = "txharbor_nonce_replays_total"
	NonceObservationsMetricName = "txharbor_nonce_observations_total"
	// NonceHolds counts holds established by admission classification
	// refusals. Label-free: the hold cause rides the redacted log line and
	// the hold row, never a label (FR-21/SC-09).
	NonceHoldsMetricName = "txharbor_nonce_holds_total"
	// NonceReconcileFailures counts per-scope reconcile-tick failures that
	// entered the bounded backoff. Label-free for the same reason; the
	// failing scope and error ride the redacted log line only.
	NonceReconcileFailuresMetricName = "txharbor_nonce_reconcile_failures_total"

	// 010 transaction-lifecycle observability surface (T003; R-010-13;
	// constitution XII; asserted by V11). Every label is a fixed vocabulary
	// (send outcome, refusal class, reconcile classification, receipt effect),
	// so no tx_hash, signature, signed byte or credential can become a label.
	TxDispatchMetricName      = "txharbor_tx_dispatch_total"
	TxGateRefusalMetricName   = "txharbor_tx_gate_refusal_total"
	TxUnknownMetricName       = "txharbor_tx_unknown"
	TxReconcileMetricName     = "txharbor_tx_reconcile_total"
	TxReceiptEffectMetricName = "txharbor_tx_receipt_effect_total"
	TxRevisionMetricName      = "txharbor_tx_revision_total"
	// 011 withdrawal execution worker surface (T002; FR-13/R13,
	// persistence.md §9). Every label is a fixed vocabulary (result/class),
	// so no intent/owner/step/attempt identity ever becomes a label.
	WorkerClaimAcquisitionsMetricName = "txharbor_worker_claim_acquisitions_total"
	WorkerClaimTakeoversMetricName    = "txharbor_worker_claim_takeovers_total"
	WorkerClaimRevocationsMetricName  = "txharbor_worker_claim_revocations_total"
	WorkerStallFlagsMetricName        = "txharbor_worker_stall_flags_total"
	WorkerStepsOpenMetricName         = "txharbor_worker_steps_open"
	WorkerUnknownPendingMetricName    = "txharbor_worker_unknown_pending"
	WorkerReconcileMetricName         = "txharbor_worker_reconcile_total"
	WorkerGateRefusalsMetricName      = "txharbor_worker_gate_refusals_total"
	WorkerProjectionStaleMetricName   = "txharbor_worker_projection_stale"
	WorkerAdvanceMetricName           = "txharbor_worker_advance_total"
	WorkerAdvanceSecondsMetricName    = "txharbor_worker_advance_seconds"
)

// Metrics owns a private registry so multiple instances (tests, restarts of
// config) never collide.
type Metrics struct {
	registry      *prometheus.Registry
	probeTotal    *prometheus.CounterVec
	indexerHeight *prometheus.GaugeVec
	indexerState  *prometheus.GaugeVec
	indexerRPC    *prometheus.CounterVec
	indexerPause  *prometheus.CounterVec
	logNext       *prometheus.GaugeVec
	logLag        *prometheus.GaugeVec
	logState      *prometheus.GaugeVec
	logRPC        *prometheus.CounterVec
	logPause      *prometheus.CounterVec

	depositNext         *prometheus.GaugeVec
	depositLag          *prometheus.GaugeVec
	depositState        *prometheus.GaugeVec
	depositObservations *prometheus.CounterVec
	depositPause        *prometheus.CounterVec
	depositTransition   *prometheus.CounterVec

	confirmationPending          *prometheus.GaugeVec
	confirmationLag              *prometheus.GaugeVec
	confirmationState            *prometheus.GaugeVec
	confirmationPolicySeq        *prometheus.GaugeVec
	confirmationConfirmed        *prometheus.CounterVec
	confirmationSkipped          *prometheus.CounterVec
	confirmationTransition       *prometheus.CounterVec
	confirmationPolicyTransition *prometheus.CounterVec

	// 006 reorg recovery surface (specs/006-reorg-recovery/contracts/
	// observability.md). Contract-name -> exposition-name mapping:
	//   reorg_active            -> txharbor_reorg_active
	//   reorg_depth_vs_bound    -> txharbor_reorg_depth + txharbor_reorg_bound
	//                              (two gauges; the depth>bound alert lives in
	//                              the runbook, not in code)
	//   reorg_frontier_lag      -> txharbor_reorg_frontier_lag
	//   reorg_orphaned_total    -> txharbor_reorg_orphaned_total
	//   reorg_revived_total     -> txharbor_reorg_revived_total
	//   reorg_reconcile_required -> txharbor_reorg_reconcile_required
	//   reorg_evidence_wait_total -> txharbor_reorg_evidence_wait_total
	reorgActive       *prometheus.GaugeVec
	reorgDepth        *prometheus.GaugeVec
	reorgBound        *prometheus.GaugeVec
	reorgFrontierLag  *prometheus.GaugeVec
	reorgOrphaned     *prometheus.CounterVec
	reorgRevived      *prometheus.CounterVec
	reorgReconcile    *prometheus.GaugeVec
	reorgEvidenceWait *prometheus.CounterVec

	// 007 withdrawal create outcomes; label-free (see the metric-name consts).
	withdrawalAccepted        *prometheus.CounterVec
	withdrawalReplayed        *prometheus.CounterVec
	withdrawalConflict        *prometheus.CounterVec
	withdrawalRejected        *prometheus.CounterVec
	withdrawalUnavailable     *prometheus.CounterVec
	withdrawalUnauthenticated *prometheus.CounterVec

	// 008 nonce-manager allocation surface (T020): result and classification
	// are fixed vocabularies, nothing request-derived is ever a label.
	nonceAllocations       *prometheus.CounterVec
	nonceReplays           *prometheus.CounterVec
	nonceObservations      *prometheus.CounterVec
	nonceHolds             *prometheus.CounterVec
	nonceReconcileFailures *prometheus.CounterVec
	// 009 signer surface (internal/metrics/signer.go; T012).
	signerRequests     *prometheus.CounterVec
	signerSigned       *prometheus.CounterVec
	signerRefusals     *prometheus.CounterVec
	signerGateRefusals *prometheus.CounterVec
	signerAdmissions   *prometheus.CounterVec
	signerCommits      *prometheus.CounterVec

	// 010 transaction-lifecycle surface (T003); fixed-vocabulary labels only.
	txDispatch      *prometheus.CounterVec
	txGateRefusal   *prometheus.CounterVec
	txUnknown       *prometheus.GaugeVec
	txReconcile     *prometheus.CounterVec
	txReceiptEffect *prometheus.CounterVec
	txRevision      *prometheus.CounterVec
	// 011 withdrawal execution worker (T002): claim/lease lifecycle, stall
	// flags, step/reconcile/unknown position, gate refusals by class,
	// projection staleness and advance outcomes.
	workerClaimAcquisitions *prometheus.CounterVec
	workerClaimTakeovers    *prometheus.CounterVec
	workerClaimRevocations  *prometheus.CounterVec
	workerStallFlags        *prometheus.CounterVec
	workerStepsOpen         *prometheus.GaugeVec
	workerUnknownPending    *prometheus.GaugeVec
	workerReconcile         *prometheus.CounterVec
	workerGateRefusals      *prometheus.CounterVec
	workerProjectionStale   *prometheus.GaugeVec
	workerAdvance           *prometheus.CounterVec
	workerAdvanceSeconds    *prometheus.GaugeVec

	handler http.Handler
}

// New builds the registry and the /metrics handler. ready is evaluated on
// every scrape (GaugeFunc).
func New(ready func() bool) *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	readyGauge := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: ReadyMetricName,
		Help: "Current readiness of the service: 1=ready, 0=not-ready.",
	}, func() float64 {
		if ready() {
			return 1
		}
		return 0
	})

	probeTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: ProbeMetricName,
		Help: "Total dependency probes by dependency and result.",
	}, []string{"dep", "result"})

	indexerHeight := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: IndexerCheckpointMetricName,
		Help: "Indexer checkpoint height per chain; absent while progress is empty.",
	}, []string{"chain"})

	indexerState := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: IndexerStateMetricName,
		Help: "Indexer state per chain: 0=running, 1=waiting, 2=retrying, 3=paused.",
	}, []string{"chain"})

	indexerRPC := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: IndexerRPCMetricName,
		Help: "Indexer chain RPC outcomes by failure class and result; not-found/ok is the wait polarity.",
	}, []string{"kind", "result"})

	indexerPause := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: IndexerPauseMetricName,
		Help: "Indexer pauses observed per chain; monotonic and independent of pause rows.",
	}, []string{"chain"})

	logNext := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: LogCheckpointNextMetricName,
		Help: "Log scan next block per chain; absent while progress is empty.",
	}, []string{"chain"})

	logLag := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: LogLagMetricName,
		Help: "Log lag in blocks per chain; absent while either checkpoint is empty.",
	}, []string{"chain"})

	logState := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: LogStateMetricName,
		Help: "Log scanner state per chain: 0=running, 1=waiting, 2=retrying, 3=paused.",
	}, []string{"chain"})

	logRPC := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: LogRPCMetricName,
		Help: "Log RPC outcomes by failure class and result; incomplete means completeness is suspect.",
	}, []string{"kind", "result"})

	logPause := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: LogPauseMetricName,
		Help: "Log pauses observed per chain; monotonic and independent of pause rows.",
	}, []string{"chain"})

	depositNext := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: DepositNextMetricName,
		Help: "Deposit scan next block per chain; absent while progress is empty.",
	}, []string{"chain"})

	depositLag := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: DepositLagMetricName,
		Help: "Deposit lag in blocks per chain; absent while either checkpoint is empty.",
	}, []string{"chain"})

	depositState := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: DepositStateMetricName,
		Help: "Deposit scanner state per chain: 0=running, 1=waiting for upstream coverage, 2=retrying, 3=paused, 4=structural gap halt.",
	}, []string{"chain"})

	depositObservations := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: DepositObservationsMetricName,
		Help: "Deposit processing results per chain; matched generates an observation, nomatch/zero/invalid do not.",
	}, []string{"chain", "result"})

	depositPause := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: DepositPauseMetricName,
		Help: "Deposit pauses observed per chain; monotonic and independent of pause rows.",
	}, []string{"chain"})

	depositTransition := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: DepositTransitionMetricName,
		Help: "Authorised deposit config transitions per chain; details live in deposit_config_history.",
	}, []string{"chain", "result"})

	confirmationPending := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: ConfirmationPendingMetricName,
		Help: "Unconfirmed pending estimate per chain; exposed as 0 while empty.",
	}, []string{"chain"})

	confirmationLag := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: ConfirmationLagMetricName,
		Help: "Confirmation lag in blocks per chain; absent while the tip is missing or nothing is confirmed.",
	}, []string{"chain"})

	confirmationState := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: ConfirmationStateMetricName,
		Help: "Confirmation scanner state per chain: 0=running, 1=waiting for trusted tip, 2=retrying, 3=stopped.",
	}, []string{"chain"})

	confirmationPolicySeq := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: ConfirmationPolicySeqMetricName,
		Help: "Effective confirmation policy_seq per chain; absent while no policy row exists.",
	}, []string{"chain"})

	confirmationConfirmed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: ConfirmationConfirmedMetricName,
		Help: "Successful pending-to-confirmed transitions per chain; details live in the observation rows.",
	}, []string{"chain"})

	confirmationSkipped := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: ConfirmationSkippedMetricName,
		Help: "Benign below-depth re-estimates per chain; the row stays pending.",
	}, []string{"chain", "reason"})

	confirmationTransition := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: ConfirmationTransitionMetricName,
		Help: "Confirmation commit adjudications per chain: ok=committed, stale=under-lock mismatch rollback, rejected=drift/pause refusal.",
	}, []string{"chain", "result"})

	confirmationPolicyTransition := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: ConfirmationPolicyTransitionMetricName,
		Help: "Authorised confirmation policy transitions per chain; details live in confirmation_policy_history.",
	}, []string{"chain", "result"})

	reorgActive := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: ReorgActiveMetricName,
		Help: "Reorg recovery active per chain: 1 while a recovery row exists, else 0.",
	}, []string{"chain"})

	reorgDepth := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: ReorgDepthMetricName,
		Help: "Reorg recovery depth per chain; alert when depth exceeds txharbor_reorg_bound (see runbook).",
	}, []string{"chain"})

	reorgBound := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: ReorgBoundMetricName,
		Help: "Reorg recovery bound per chain; alert when txharbor_reorg_depth exceeds bound (see runbook).",
	}, []string{"chain"})

	reorgFrontierLag := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: ReorgFrontierLagMetricName,
		Help: "Reorg frontier lag per chain and stream (block|log|deposit): swept_end minus frontier; absent while progress is empty.",
	}, []string{"chain", "stream"})

	reorgOrphaned := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: ReorgOrphanedMetricName,
		Help: "Reorg orphaned observations per chain; monotonic conversion counter.",
	}, []string{"chain"})

	reorgRevived := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: ReorgRevivedMetricName,
		Help: "Reorg revived observations per chain; monotonic conversion counter.",
	}, []string{"chain"})

	reorgReconcile := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: ReorgReconcileMetricName,
		Help: "Reorg reconcile required per chain: 1 while phase is reconcile_required, else 0.",
	}, []string{"chain"})

	// Evidence-wait class values are the executor's own cause classes
	// (observed in internal/indexer/reorg.go chainBlock via eth.KindOf plus
	// the hold/terminal paths, and internal/indexer/reorgcommit.go reconcile
	// causes): transport, timeout, rate-limited, invalid-response (parse),
	// chain-mismatch/contradictory, insufficient (hold), and the terminal
	// reconcile causes over_depth, below_scan_start, local_exhausted,
	// over-deep, ancestor-unobtainable, exhausted history. No new taxonomy
	// is introduced here; class is a pass-through label like other result
	// labels in this file.
	reorgEvidenceWait := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: ReorgEvidenceWaitMetricName,
		Help: "Reorg evidence-insufficient waits per chain by executor cause class; never invalidates.",
	}, []string{"chain", "class"})

	registry.MustRegister(readyGauge, probeTotal,
		indexerHeight, indexerState, indexerRPC, indexerPause,
		logNext, logLag, logState, logRPC, logPause,
		depositNext, depositLag, depositState, depositObservations, depositPause, depositTransition,
		confirmationPending, confirmationLag, confirmationState, confirmationPolicySeq,
		confirmationConfirmed, confirmationSkipped, confirmationTransition, confirmationPolicyTransition,
		reorgActive, reorgDepth, reorgBound, reorgFrontierLag,
		reorgOrphaned, reorgRevived, reorgReconcile, reorgEvidenceWait)

	// 007 withdrawal create outcomes. Label-free counters: each create attempt
	// lands in exactly one series by outcome, so cardinality stays O(1) and no
	// request-derived value can leak into a label (FR-20/FR-21). No label
	// dimensions means the series appears only once an outcome is observed.
	withdrawalAccepted := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: WithdrawalAcceptedMetricName,
		Help: "Accepted 007 withdrawal creates (HTTP 201); one per newly persisted request row.",
	}, nil)
	withdrawalReplayed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: WithdrawalReplayedMetricName,
		Help: "Replayed 007 withdrawal creates (HTTP 200): same key and parameters as the stored row.",
	}, nil)
	withdrawalConflict := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: WithdrawalConflictMetricName,
		Help: "Conflicting 007 withdrawal creates (HTTP 409): same key, different parameters.",
	}, nil)
	withdrawalRejected := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: WithdrawalRejectedMetricName,
		Help: "Rejected 007 withdrawal creates (HTTP 400/403/422): malformed, unauthorized or invalid.",
	}, nil)
	withdrawalUnavailable := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: WithdrawalUnavailableMetricName,
		Help: "007 withdrawal creates whose storage outcome was unavailable/unknown (HTTP 503).",
	}, nil)
	withdrawalUnauthenticated := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: WithdrawalUnauthenticatedMetricName,
		Help: "Unauthenticated 007 withdrawal create attempts (HTTP 401); no verifiable caller identity.",
	}, nil)
	registry.MustRegister(withdrawalAccepted, withdrawalReplayed, withdrawalConflict,
		withdrawalRejected, withdrawalUnavailable, withdrawalUnauthenticated)

	// 008 nonce-manager allocation surface (T020). result/classification are
	// fixed vocabularies; see the metric-name consts.
	nonceAllocations := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: NonceAllocationsMetricName,
		Help: "008 admission terminals by machine reason: allocated, replayed, or the refusal Outcome.",
	}, []string{"result"})
	nonceReplays := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: NonceReplaysMetricName,
		Help: "008 admissions that returned the original binding on full allocation-input equality.",
	}, nil)
	nonceObservations := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: NonceObservationsMetricName,
		Help: "008 persisted observation rows by classification (consistent, bootstrap_external_consumed, unattributed_consumption, unexplained_gap, divergence, unavailable).",
	}, []string{"classification"})
	nonceHolds := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: NonceHoldsMetricName,
		Help: "008 holds established by admission classification refusals.",
	}, nil)
	nonceReconcileFailures := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: NonceReconcileFailuresMetricName,
		Help: "008 per-scope reconcile-tick failures that entered the bounded backoff.",
	}, nil)
	registry.MustRegister(nonceAllocations, nonceReplays, nonceObservations, nonceHolds, nonceReconcileFailures)

	txDispatch := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: TxDispatchMetricName,
		Help: "010 dispatch outcomes by outcome (accepted/rejected/unknown); a send fact, never a payment verdict.",
	}, []string{"outcome"})
	txGateRefusal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: TxGateRefusalMetricName,
		Help: "010 zero-dispatch gate/send refusals by refusal class (fixed vocabulary from errors.go).",
	}, []string{"class"})
	txUnknown := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: TxUnknownMetricName,
		Help: "010 attempts whose business effect is currently unknown: 1 while any attempt is unknown, else 0.",
	}, nil)
	txReconcile := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: TxReconcileMetricName,
		Help: "010 reconcile observations by classification (found_pending/included/not_found_yet/unavailable).",
	}, []string{"classification"})
	txReceiptEffect := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: TxReceiptEffectMetricName,
		Help: "010 receipt effect verdicts by effect (effective/ineffective_*); mismatch never counts as paid.",
	}, []string{"effect"})
	txRevision := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: TxRevisionMetricName,
		Help: "010 reorg revisions applied (append-only revision chain); monotonic counter.",
	}, nil)
	registry.MustRegister(txDispatch, txGateRefusal, txUnknown, txReconcile, txReceiptEffect, txRevision)

	// 011 withdrawal execution worker (T002). result/class are fixed
	// vocabularies (see the metric-name consts); no identity value is ever a
	// label.
	workerClaimAcquisitions := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: WorkerClaimAcquisitionsMetricName,
		Help: "011 claim acquisitions by result: acquired or not_claimable.",
	}, []string{"result"})
	workerClaimTakeovers := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: WorkerClaimTakeoversMetricName,
		Help: "011 claim takeovers (generation advance) observed.",
	}, nil)
	workerClaimRevocations := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: WorkerClaimRevocationsMetricName,
		Help: "011 operator claim revocations applied.",
	}, nil)
	workerStallFlags := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: WorkerStallFlagsMetricName,
		Help: "011 stall flags recorded by the sweep without takeover.",
	}, nil)
	workerStepsOpen := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: WorkerStepsOpenMetricName,
		Help: "011 open (issued) execution steps at the last observation.",
	}, nil)
	workerUnknownPending := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: WorkerUnknownPendingMetricName,
		Help: "011 intents whose execution effect is unknown and pending reconcile.",
	}, nil)
	workerReconcile := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: WorkerReconcileMetricName,
		Help: "011 reconcile outcomes by result: converged, still_unknown or unavailable.",
	}, []string{"result"})
	workerGateRefusals := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: WorkerGateRefusalsMetricName,
		Help: "011 gate refusals by machine refusal class.",
	}, []string{"class"})
	workerProjectionStale := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: WorkerProjectionStaleMetricName,
		Help: "011 projections currently marked possibly_stale.",
	}, nil)
	workerAdvance := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: WorkerAdvanceMetricName,
		Help: "011->010 advance calls by result: the outcome class or an error class.",
	}, []string{"result"})
	workerAdvanceSeconds := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: WorkerAdvanceSecondsMetricName,
		Help: "Duration of the last 011->010 advance call in seconds; not an SLA (C11 unspecified).",
	}, nil)
	registry.MustRegister(workerClaimAcquisitions, workerClaimTakeovers,
		workerClaimRevocations, workerStallFlags, workerStepsOpen,
		workerUnknownPending, workerReconcile, workerGateRefusals,
		workerProjectionStale, workerAdvance, workerAdvanceSeconds)
	m := &Metrics{
		registry:                     registry,
		probeTotal:                   probeTotal,
		indexerHeight:                indexerHeight,
		indexerState:                 indexerState,
		indexerRPC:                   indexerRPC,
		indexerPause:                 indexerPause,
		logNext:                      logNext,
		logLag:                       logLag,
		logState:                     logState,
		logRPC:                       logRPC,
		logPause:                     logPause,
		depositNext:                  depositNext,
		depositLag:                   depositLag,
		depositState:                 depositState,
		depositObservations:          depositObservations,
		depositPause:                 depositPause,
		depositTransition:            depositTransition,
		confirmationPending:          confirmationPending,
		confirmationLag:              confirmationLag,
		confirmationState:            confirmationState,
		confirmationPolicySeq:        confirmationPolicySeq,
		confirmationConfirmed:        confirmationConfirmed,
		confirmationSkipped:          confirmationSkipped,
		confirmationTransition:       confirmationTransition,
		confirmationPolicyTransition: confirmationPolicyTransition,
		reorgActive:                  reorgActive,
		reorgDepth:                   reorgDepth,
		reorgBound:                   reorgBound,
		reorgFrontierLag:             reorgFrontierLag,
		reorgOrphaned:                reorgOrphaned,
		reorgRevived:                 reorgRevived,
		reorgReconcile:               reorgReconcile,
		reorgEvidenceWait:            reorgEvidenceWait,
		withdrawalAccepted:           withdrawalAccepted,
		withdrawalReplayed:           withdrawalReplayed,
		withdrawalConflict:           withdrawalConflict,
		withdrawalRejected:           withdrawalRejected,
		withdrawalUnavailable:        withdrawalUnavailable,
		withdrawalUnauthenticated:    withdrawalUnauthenticated,
		nonceAllocations:             nonceAllocations,
		nonceReplays:                 nonceReplays,
		nonceObservations:            nonceObservations,
		nonceHolds:                   nonceHolds,
		nonceReconcileFailures:       nonceReconcileFailures,
		txDispatch:                   txDispatch,
		txGateRefusal:                txGateRefusal,
		txUnknown:                    txUnknown,
		txReconcile:                  txReconcile,
		txReceiptEffect:              txReceiptEffect,
		txRevision:                   txRevision,
		workerClaimAcquisitions:      workerClaimAcquisitions,
		workerClaimTakeovers:         workerClaimTakeovers,
		workerClaimRevocations:       workerClaimRevocations,
		workerStallFlags:             workerStallFlags,
		workerStepsOpen:              workerStepsOpen,
		workerUnknownPending:         workerUnknownPending,
		workerReconcile:              workerReconcile,
		workerGateRefusals:           workerGateRefusals,
		workerProjectionStale:        workerProjectionStale,
		workerAdvance:                workerAdvance,
		workerAdvanceSeconds:         workerAdvanceSeconds,
		handler:                      promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	}
	m.registerSigner(registry)
	return m
}

// Handler serves the Prometheus exposition format.
func (m *Metrics) Handler() http.Handler { return m.handler }

// Gatherer exposes the registry for tests and diagnostics.
func (m *Metrics) Gatherer() prometheus.Gatherer { return m.registry }

// ObserveProbe counts one DB/RPC probe result.
func (m *Metrics) ObserveProbe(dep string, ok bool) {
	result := "failure"
	if ok {
		result = "success"
	}
	m.probeTotal.WithLabelValues(dep, result).Inc()
}

// ObserveIndexerState records the scanner state for chain: 0 running,
// 1 waiting, 2 retrying, 3 paused (contracts/observability.md).
func (m *Metrics) ObserveIndexerState(chain int64, state int) {
	m.indexerState.WithLabelValues(chainLabel(chain)).Set(float64(state))
}

// ObserveIndexerCheckpoint records the checkpoint height for chain. ok=false
// means empty progress: the series is removed rather than zeroed.
func (m *Metrics) ObserveIndexerCheckpoint(chain int64, height uint64, ok bool) {
	if !ok {
		m.indexerHeight.DeleteLabelValues(chainLabel(chain))
		return
	}
	m.indexerHeight.WithLabelValues(chainLabel(chain)).Set(float64(height))
}

// ObserveIndexerRPC counts one classified chain RPC outcome. ok is true only
// for the not-found wait polarity; every other outcome is a failure class.
func (m *Metrics) ObserveIndexerRPC(kind string, ok bool) {
	result := "error"
	if ok {
		result = "ok"
	}
	m.indexerRPC.WithLabelValues(kind, result).Inc()
}

// ObserveIndexerPause counts one observed pause for chain.
func (m *Metrics) ObserveIndexerPause(chain int64) {
	m.indexerPause.WithLabelValues(chainLabel(chain)).Inc()
}

// ObserveLogCheckpointNext records the next block to scan for chain. ok=false
// means empty progress: the series is removed rather than zeroed.
func (m *Metrics) ObserveLogCheckpointNext(chain int64, next uint64, ok bool) {
	if !ok {
		m.logNext.DeleteLabelValues(chainLabel(chain))
		return
	}
	m.logNext.WithLabelValues(chainLabel(chain)).Set(float64(next))
}

// ObserveLogLag records the log lag in blocks for chain. ok=false means either
// checkpoint is empty: the series is removed rather than zeroed.
func (m *Metrics) ObserveLogLag(chain int64, lag uint64, ok bool) {
	if !ok {
		m.logLag.DeleteLabelValues(chainLabel(chain))
		return
	}
	m.logLag.WithLabelValues(chainLabel(chain)).Set(float64(lag))
}

// ObserveLogState records the log scanner state for chain: 0 running,
// 1 waiting, 2 retrying, 3 paused (contracts/observability.md). Pausing must
// not flip readyz.
func (m *Metrics) ObserveLogState(chain int64, state int) {
	m.logState.WithLabelValues(chainLabel(chain)).Set(float64(state))
}

// ObserveLogRPC counts one classified log RPC outcome. ok is true only for the
// not-found wait polarity and successful calls.
func (m *Metrics) ObserveLogRPC(kind string, ok bool) {
	result := "error"
	if ok {
		result = "ok"
	}
	m.logRPC.WithLabelValues(kind, result).Inc()
}

// ObserveLogPause counts one observed log pause for chain.
func (m *Metrics) ObserveLogPause(chain int64) {
	m.logPause.WithLabelValues(chainLabel(chain)).Inc()
}

// ObserveDepositNext records the next deposit block to process for chain.
// ok=false means empty progress: the series is removed rather than zeroed.
func (m *Metrics) ObserveDepositNext(chain int64, next uint64, ok bool) {
	if !ok {
		m.depositNext.DeleteLabelValues(chainLabel(chain))
		return
	}
	m.depositNext.WithLabelValues(chainLabel(chain)).Set(float64(next))
}

// ObserveDepositLag records the deposit lag in blocks for chain. ok=false
// means either the deposit or the 003 log checkpoint is empty: the series is
// removed rather than zeroed.
func (m *Metrics) ObserveDepositLag(chain int64, lag uint64, ok bool) {
	if !ok {
		m.depositLag.DeleteLabelValues(chainLabel(chain))
		return
	}
	m.depositLag.WithLabelValues(chainLabel(chain)).Set(float64(lag))
}

// ObserveDepositState records the deposit scanner state for chain: 0 running,
// 1 waiting for upstream coverage, 2 retrying, 3 paused, 4 structural gap
// halt (contracts/observability.md). Pausing or halting must not flip readyz.
func (m *Metrics) ObserveDepositState(chain int64, state int) {
	m.depositState.WithLabelValues(chainLabel(chain)).Set(float64(state))
}

// ObserveDepositObservation counts one processed log by result:
// matched|nomatch|zero|invalid (contracts/observability.md).
func (m *Metrics) ObserveDepositObservation(chain int64, result string) {
	m.depositObservations.WithLabelValues(chainLabel(chain), result).Inc()
}

// ObserveDepositPause counts one observed deposit pause for chain.
func (m *Metrics) ObserveDepositPause(chain int64) {
	m.depositPause.WithLabelValues(chainLabel(chain)).Inc()
}

// ObserveDepositTransition counts one authorised config transition by
// result: ok|error|rejected (contracts/observability.md). Audit details live
// in the deposit_config_history rows, not in this counter.
func (m *Metrics) ObserveDepositTransition(chain int64, result string) {
	m.depositTransition.WithLabelValues(chainLabel(chain), result).Inc()
}

// ObserveConfirmationPending records the unconfirmed pending estimate for
// chain. Empty is exposed as 0 (contracts/observability.md), never removed.
func (m *Metrics) ObserveConfirmationPending(chain int64, pending uint64) {
	m.confirmationPending.WithLabelValues(chainLabel(chain)).Set(float64(pending))
}

// ObserveConfirmationLag records the confirmation lag in blocks for chain.
// ok=false means the tip is missing or nothing is confirmed yet: the series
// is removed rather than zeroed.
func (m *Metrics) ObserveConfirmationLag(chain int64, lag uint64, ok bool) {
	if !ok {
		m.confirmationLag.DeleteLabelValues(chainLabel(chain))
		return
	}
	m.confirmationLag.WithLabelValues(chainLabel(chain)).Set(float64(lag))
}

// ObserveConfirmationState records the confirmation scanner state for chain:
// 0 running, 1 waiting for trusted tip, 2 retrying, 3 stopped
// (contracts/observability.md). Stopping must not flip readyz.
func (m *Metrics) ObserveConfirmationState(chain int64, state int) {
	m.confirmationState.WithLabelValues(chainLabel(chain)).Set(float64(state))
}

// ObserveConfirmationPolicySeq records the effective policy_seq for chain.
// ok=false means no policy row exists yet: the series is removed rather than
// zeroed.
func (m *Metrics) ObserveConfirmationPolicySeq(chain int64, seq uint64, ok bool) {
	if !ok {
		m.confirmationPolicySeq.DeleteLabelValues(chainLabel(chain))
		return
	}
	m.confirmationPolicySeq.WithLabelValues(chainLabel(chain)).Set(float64(seq))
}

// ObserveConfirmationConfirmed counts one successful pending-to-confirmed
// transition for chain.
func (m *Metrics) ObserveConfirmationConfirmed(chain int64) {
	m.confirmationConfirmed.WithLabelValues(chainLabel(chain)).Inc()
}

// ObserveConfirmationSkipped counts one benign below-depth re-estimate for
// chain; the row stays pending.
func (m *Metrics) ObserveConfirmationSkipped(chain int64, reason string) {
	m.confirmationSkipped.WithLabelValues(chainLabel(chain), reason).Inc()
}

// ObserveConfirmationTransition counts one commit adjudication by result:
// ok|stale|rejected (contracts/observability.md).
func (m *Metrics) ObserveConfirmationTransition(chain int64, result string) {
	m.confirmationTransition.WithLabelValues(chainLabel(chain), result).Inc()
}

// ObserveConfirmationPolicyTransition counts one authorised policy transition
// by result: ok|rejected (contracts/observability.md). Audit details live in
// the confirmation_policy_history rows, not in this counter.
func (m *Metrics) ObserveConfirmationPolicyTransition(chain int64, result string) {
	m.confirmationPolicyTransition.WithLabelValues(chainLabel(chain), result).Inc()
}

// ObserveReorgActive records whether a recovery row exists for chain: 1 while
// present, else 0 (specs/006-reorg-recovery/contracts/observability.md).
func (m *Metrics) ObserveReorgActive(chain int64, active bool) {
	if !active {
		m.reorgActive.WithLabelValues(chainLabel(chain)).Set(0)
		return
	}
	m.reorgActive.WithLabelValues(chainLabel(chain)).Set(1)
}

// ObserveReorgDepthBound records the computed depth and bound for chain. The
// depth>bound alert lives in the runbook, not in code.
func (m *Metrics) ObserveReorgDepthBound(chain int64, depth, bound int64) {
	m.reorgDepth.WithLabelValues(chainLabel(chain)).Set(float64(depth))
	m.reorgBound.WithLabelValues(chainLabel(chain)).Set(float64(bound))
}

// ObserveReorgDepthPending records the bound while the ancestor search is
// still in progress: the bound gauge is set, the depth series is removed
// (unknown, never a zero that would read as an empty reorg).
func (m *Metrics) ObserveReorgDepthPending(chain int64, bound int64) {
	m.reorgBound.WithLabelValues(chainLabel(chain)).Set(float64(bound))
	m.reorgDepth.DeleteLabelValues(chainLabel(chain))
}

// ObserveReorgFrontierLag records swept_end minus frontier for chain and
// stream (block|log|deposit). ok=false means empty progress: the series is
// removed rather than zeroed.
func (m *Metrics) ObserveReorgFrontierLag(chain int64, stream string, lag uint64, ok bool) {
	if !ok {
		m.reorgFrontierLag.DeleteLabelValues(chainLabel(chain), stream)
		return
	}
	m.reorgFrontierLag.WithLabelValues(chainLabel(chain), stream).Set(float64(lag))
}

// ObserveReorgOrphaned counts one orphaned observation conversion for chain.
func (m *Metrics) ObserveReorgOrphaned(chain int64) {
	m.reorgOrphaned.WithLabelValues(chainLabel(chain)).Inc()
}

// ObserveReorgRevived counts one revived observation conversion for chain.
func (m *Metrics) ObserveReorgRevived(chain int64) {
	m.reorgRevived.WithLabelValues(chainLabel(chain)).Inc()
}

// ObserveReorgReconcile records whether chain sits in reconcile_required: 1
// while held, else 0.
func (m *Metrics) ObserveReorgReconcile(chain int64, required bool) {
	if !required {
		m.reorgReconcile.WithLabelValues(chainLabel(chain)).Set(0)
		return
	}
	m.reorgReconcile.WithLabelValues(chainLabel(chain)).Set(1)
}

// ObserveReorgEvidenceWait counts one evidence-insufficient wait for chain by
// executor cause class (see the reorgEvidenceWait comment at construction).
func (m *Metrics) ObserveReorgEvidenceWait(chain int64, class string) {
	m.reorgEvidenceWait.WithLabelValues(chainLabel(chain), class).Inc()
}

// Deposit log events and their frozen structured field lists
// (contracts/observability.md). The scanner (T005+) logs exactly these fields
// per event; the T019 audit walks a captured record against this manifest and
// DepositLogRedact.
const (
	DepositLogAdvance      = "advance"
	DepositLogWait         = "wait"
	DepositLogRetry        = "retry"
	DepositLogGap          = "gap"
	DepositLogPause        = "pause"
	DepositLogRelease      = "release"
	DepositLogConfigReject = "config_reject"
	DepositLogTransition   = "transition"
)

// DepositLogFields is the frozen field list per deposit log event. Renaming a
// field or an event here is an observability contract change.
var DepositLogFields = map[string][]string{
	DepositLogAdvance:      {"chain_id", "from_block", "to_block", "matched", "nomatch", "zero", "attempt"},
	DepositLogWait:         {"chain_id", "next_block", "reason"},
	DepositLogRetry:        {"chain_id", "from_block", "to_block", "kind", "attempt", "retry_in"},
	DepositLogGap:          {"chain_id", "gap_from", "gap_to", "class", "cause", "config"},
	DepositLogPause:        {"chain_id", "pause_id", "revision", "height", "kind", "detail"},
	DepositLogRelease:      {"chain_id", "pause_id", "revision", "operator", "reason", "result"},
	DepositLogConfigReject: {"chain_id", "reason", "detail"},
	DepositLogTransition:   {"chain_id", "request_id", "operator", "old_config", "new_config", "replay_from", "result", "reason"},
}

// DepositLogRedact scrubs a deposit log value before it reaches slog
// (SC-09): error and detail strings go through this hook, amounts are logged
// as decimal strings only, and raw contract data is never dumped. It wraps
// logx.Redact so the scanner (T005+) and the contract tests share one funnel.
func DepositLogRedact(s string) string { return logx.Redact(s) }

func chainLabel(chain int64) string { return strconv.FormatInt(chain, 10) }

// ObserveWithdrawalStatus counts one 007 withdrawal create attempt by the HTTP
// status the transport returned (T015, FR-20/FR-21). The counters are
// label-free, so no caller id, request id, key material, asset or amount ever
// reaches a label. Statuses with no business outcome (e.g. 405) are ignored.
func (m *Metrics) ObserveWithdrawalStatus(status int) {
	switch status {
	case http.StatusCreated:
		m.withdrawalAccepted.WithLabelValues().Inc()
	case http.StatusOK:
		m.withdrawalReplayed.WithLabelValues().Inc()
	case http.StatusConflict:
		m.withdrawalConflict.WithLabelValues().Inc()
	case http.StatusBadRequest, http.StatusForbidden, http.StatusUnprocessableEntity:
		m.withdrawalRejected.WithLabelValues().Inc()
	case http.StatusUnauthorized:
		m.withdrawalUnauthenticated.WithLabelValues().Inc()
	case http.StatusServiceUnavailable:
		m.withdrawalUnavailable.WithLabelValues().Inc()
	}
}

// ObserveNonceAllocation counts one 008 admission by its machine result: the
// allocated outcome, the replayed outcome, or the refusal Outcome (the
// refusal's machine reason). result is a fixed vocabulary value (R11/FR-21);
// no sender, nonce, intent or hold value is ever passed here.
func (m *Metrics) ObserveNonceAllocation(result string) {
	m.nonceAllocations.WithLabelValues(result).Inc()
}

// ObserveNonceReplay counts one 008 admission that returned the original
// binding on full allocation-input equality.
func (m *Metrics) ObserveNonceReplay() {
	m.nonceReplays.WithLabelValues().Inc()
}

// ObserveNonceObservation counts one persisted 008 observation by its
// classification (classify.go's fixed vocabulary).
func (m *Metrics) ObserveNonceObservation(classification string) {
	m.nonceObservations.WithLabelValues(classification).Inc()
}

// ObserveNonceHoldEstablished counts one hold row established by an admission
// classification refusal in this attempt.
func (m *Metrics) ObserveNonceHoldEstablished() {
	m.nonceHolds.WithLabelValues().Inc()
}

// ObserveNonceReconcileFailure counts one per-scope reconcile-tick failure
// that entered the bounded backoff.
func (m *Metrics) ObserveNonceReconcileFailure() {
	m.nonceReconcileFailures.WithLabelValues().Inc()
}

// ObserveTxDispatch counts one 010 dispatch action by outcome
// (accepted/rejected/unknown); the outcome is a send fact, not a verdict.
func (m *Metrics) ObserveTxDispatch(outcome string) {
	m.txDispatch.WithLabelValues(outcome).Inc()
}

// ObserveTxGateRefusal counts one 010 zero-dispatch refusal by refusal class.
func (m *Metrics) ObserveTxGateRefusal(class string) {
	m.txGateRefusal.WithLabelValues(class).Inc()
}

// ObserveTxUnknown records whether any attempt currently has an unknown
// business effect: 1 while true, else 0.
func (m *Metrics) ObserveTxUnknown(unknown bool) {
	if unknown {
		m.txUnknown.WithLabelValues().Set(1)
		return
	}
	m.txUnknown.WithLabelValues().Set(0)
}

// ObserveTxReconcile counts one reconcile observation by classification.
func (m *Metrics) ObserveTxReconcile(classification string) {
	m.txReconcile.WithLabelValues(classification).Inc()
}

// ObserveTxReceiptEffect counts one receipt effect verdict by effect.
func (m *Metrics) ObserveTxReceiptEffect(effect string) {
	m.txReceiptEffect.WithLabelValues(effect).Inc()
}

// ObserveTxRevision counts one reorg revision applied to the revision chain.
func (m *Metrics) ObserveTxRevision() {
	m.txRevision.WithLabelValues().Inc()
}

// ObserveWorkerClaimAcquisition counts one 011 claim attempt by result:
// acquired or not_claimable.
func (m *Metrics) ObserveWorkerClaimAcquisition(result string) {
	m.workerClaimAcquisitions.WithLabelValues(result).Inc()
}

// ObserveWorkerClaimTakeover counts one 011 claim generation advance.
func (m *Metrics) ObserveWorkerClaimTakeover() {
	m.workerClaimTakeovers.WithLabelValues().Inc()
}

// ObserveWorkerClaimRevocation counts one applied operator claim revocation.
func (m *Metrics) ObserveWorkerClaimRevocation() {
	m.workerClaimRevocations.WithLabelValues().Inc()
}

// ObserveWorkerStallFlag counts one stall flag recorded without takeover.
func (m *Metrics) ObserveWorkerStallFlag() {
	m.workerStallFlags.WithLabelValues().Inc()
}

// SetWorkerStepsOpen records the open (issued) execution-step count.
func (m *Metrics) SetWorkerStepsOpen(n int) {
	m.workerStepsOpen.WithLabelValues().Set(float64(n))
}

// SetWorkerUnknownPending records the unknown-pending intent count.
func (m *Metrics) SetWorkerUnknownPending(n int) {
	m.workerUnknownPending.WithLabelValues().Set(float64(n))
}

// ObserveWorkerReconcile counts one reconcile outcome by result.
func (m *Metrics) ObserveWorkerReconcile(result string) {
	m.workerReconcile.WithLabelValues(result).Inc()
}

// ObserveWorkerGateRefusal counts one gate refusal by machine class.
func (m *Metrics) ObserveWorkerGateRefusal(class string) {
	m.workerGateRefusals.WithLabelValues(class).Inc()
}

// SetWorkerProjectionStale records the possibly-stale projection count.
func (m *Metrics) SetWorkerProjectionStale(n int) {
	m.workerProjectionStale.WithLabelValues().Set(float64(n))
}

// ObserveWorkerAdvance counts one 011->010 advance call by result (outcome
// class or error class).
func (m *Metrics) ObserveWorkerAdvance(result string) {
	m.workerAdvance.WithLabelValues(result).Inc()
}

// SetWorkerAdvanceSeconds records the last advance call duration in seconds.
func (m *Metrics) SetWorkerAdvanceSeconds(seconds float64) {
	m.workerAdvanceSeconds.WithLabelValues().Set(seconds)
}
