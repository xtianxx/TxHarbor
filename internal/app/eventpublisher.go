package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/indexer"
	"github.com/xtianxx/txharbor/internal/logx"
)

// Event publisher loop initial values (to be calibrated after measurement).
const (
	// eventPublisherAuditInterval is the reconciliation-audit cadence.
	eventPublisherAuditInterval = time.Minute
	// eventPublisherAuditLimit bounds the recent source window examined per
	// audit probe.
	eventPublisherAuditLimit = 500
)

// EventPublisher is the 013 `event-publisher` entry point (T034): fail-closed
// configuration, the explicit topic creation (auto-create stays disabled), the
// bounded claim/publish loop, the periodic reconciliation audit (T035) and a
// graceful stop (stop claiming, finish the in-flight batch, release what was
// not confirmed).
//
// Guarantee statement (global, MUST NOT be weakened): delivery is
// at-least-once and processing is idempotent; this command never claims a
// stronger delivery guarantee.
//
// Exit codes: 0 clean stop, 1 configuration/feature gate/database/broker
// refusal, 2 usage error.
func EventPublisher(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	if wantsHelp(args) {
		eventPublisherUsage(stdout)
		return 0
	}
	if len(args) > 0 {
		fmt.Fprintf(stderr, "txharbor event-publisher: unexpected argument %q\n", args[0])
		eventPublisherUsage(stderr)
		return 2
	}

	cfg, err := config.Load(d.getenv())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor event-publisher: configuration error: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if !cfg.Events.Enabled {
		fmt.Fprintf(stderr, "txharbor event-publisher: events are disabled (%s=false); refusing to start\n",
			config.EnvEventsEnabled)
		return 1
	}
	fmt.Fprintf(stdout, "txharbor event-publisher: config %s\n", cfg.Summary())

	pool, err := db.OpenPool(ctx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor event-publisher: open database: %s\n", logx.Redact(err.Error()))
		return 1
	}
	defer pool.Close()

	owner, err := indexer.NewOwnerID()
	if err != nil {
		fmt.Fprintf(stderr, "txharbor event-publisher: instance identity: %s\n", logx.Redact(err.Error()))
		return 1
	}

	sink, err := events.NewKafkaSink(events.DefaultKafkaSinkConfig(cfg.Kafka.Brokers, cfg.Kafka.Topic))
	if err != nil {
		fmt.Fprintf(stderr, "txharbor event-publisher: kafka producer: %s\n", logx.Redact(err.Error()))
		return 1
	}
	defer sink.Close()
	// Explicit topic creation at startup (research R17): auto-create stays
	// disabled, so a missing topic is refused here instead of surfacing as
	// implicit broker defaults later.
	if err := sink.EnsureTopic(ctx, events.DefaultTopicPartitions); err != nil {
		fmt.Fprintf(stderr, "txharbor event-publisher: kafka topic ensure failed: %s\n", logx.Redact(err.Error()))
		return 1
	}

	log := slog.Default()
	pub, err := events.NewPublisher(pool, sink, owner, events.PublisherOptions{
		Batch:       cfg.Events.Publisher.Batch,
		LeaseTTL:    cfg.Events.Publisher.LeaseTTL,
		BackoffBase: cfg.Events.Publisher.BackoffBase,
		BackoffMax:  cfg.Events.Publisher.BackoffMax,
		Observer:    eventPublisherLogObserver{log: log},
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor event-publisher: %s\n", logx.Redact(err.Error()))
		return 1
	}

	audit, err := newEventPublisherAudit(pool, log)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor event-publisher: reconciliation audit: %s\n", logx.Redact(err.Error()))
		return 1
	}

	fmt.Fprintf(stdout, "txharbor event-publisher: owner=%s topic=%s batch=%d lease=%s\n",
		owner, cfg.Kafka.Topic, cfg.Events.Publisher.Batch, cfg.Events.Publisher.LeaseTTL)
	runEventPublisherLoop(ctx, pub, eventPublisherLoopOptions{
		PollInterval:  cfg.Events.Publisher.PollInterval,
		AuditInterval: eventPublisherAuditInterval,
		Audit:         audit,
		Log:           log,
	})
	fmt.Fprintln(stdout, "txharbor event-publisher: stopped")
	return 0
}

// outboxPublisher is the bounded runtime surface the loop drives.
type outboxPublisher interface {
	PublishOnce(ctx context.Context) (events.PublishOutcome, error)
	RefreshGauges(ctx context.Context) error
}

// auditEntry is the periodic reconciliation entry (T035).
type auditEntry interface {
	Run(ctx context.Context)
}

// eventPublisherLoopOptions bounds one publisher loop.
type eventPublisherLoopOptions struct {
	PollInterval  time.Duration
	AuditInterval time.Duration
	Audit         auditEntry
	Log           *slog.Logger
}

// runEventPublisherLoop drives the bounded claim/publish cadence until ctx is
// cancelled: at most one bounded batch per poll interval (the drain rate is
// bounded by batch/poll, never an unbounded broker/PG burst), a periodic
// reconciliation pass, gauge refresh, and a graceful stop — the in-flight
// batch settles and nothing new is claimed.
func runEventPublisherLoop(ctx context.Context, pub outboxPublisher, opts eventPublisherLoopOptions) {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	pollInterval := opts.PollInterval
	if pollInterval <= 0 {
		// config.Load refuses a non-positive cadence; this is the last-resort
		// bound so the loop can never spin unthrottled.
		pollInterval = time.Second
	}
	poll := time.NewTicker(pollInterval)
	defer poll.Stop()

	var lastAudit time.Time
	for {
		if ctx.Err() != nil {
			return
		}
		outcome, err := pub.PublishOnce(ctx)
		if err != nil && ctx.Err() == nil {
			log.Warn("publish cycle failed", "error", logx.Redact(err.Error()))
		}
		if err == nil && outcome.Claimed > 0 {
			log.Info("publish cycle", "claimed", outcome.Claimed, "acked", outcome.Acked,
				"released", outcome.Released, "blocked", outcome.Blocked)
		}
		if gerr := pub.RefreshGauges(ctx); gerr != nil && ctx.Err() == nil {
			log.Warn("outbox gauge refresh failed", "error", logx.Redact(gerr.Error()))
		}
		if opts.Audit != nil && opts.AuditInterval > 0 && time.Since(lastAudit) >= opts.AuditInterval {
			lastAudit = time.Now()
			opts.Audit.Run(ctx)
		}
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
		}
	}
}

// eventPublisherLogObserver turns publisher outcomes into structured log
// lines: transient failures are Debug (an outage repeats them every cycle),
// permanent/contract failures are Error (they must alert), published batches
// are Info. The gauge methods are no-ops here — the command exposes no metrics
// endpoint in this batch; drills and later batches wire the metric sink.
type eventPublisherLogObserver struct{ log *slog.Logger }

// ObserveOutboxPublishFailure logs one publish failure by class.
func (o eventPublisherLogObserver) ObserveOutboxPublishFailure(errorClass string) {
	if errorClass == string(events.ClassTransient) {
		o.log.Debug("outbox publish failure", "error_class", errorClass)
		return
	}
	o.log.Error("outbox publish failure", "error_class", errorClass)
}

// ObserveOutboxPublished logs one acknowledged batch.
func (o eventPublisherLogObserver) ObserveOutboxPublished(n int) {
	o.log.Info("outbox published", "count", n)
}

// ObserveOutboxAttempts logs one claimed batch.
func (o eventPublisherLogObserver) ObserveOutboxAttempts(n int) {
	o.log.Debug("outbox publish attempt", "count", n)
}

// SetOutboxBlocked is a no-op in this batch (no metrics endpoint).
func (o eventPublisherLogObserver) SetOutboxBlocked(int) {}

// SetOutboxPending is a no-op in this batch (no metrics endpoint).
func (o eventPublisherLogObserver) SetOutboxPending(string, int) {}

// SetOutboxPendingOldestAge is a no-op in this batch (no metrics endpoint).
func (o eventPublisherLogObserver) SetOutboxPendingOldestAge(string, float64) {}

// Source-domain reconciliation probes (data-model §4). Every probe is
// read-only, bounded to the recent window and filtered to the
// migration-seeded cutover: pre-cutover history legitimately has no emitted
// event (data-model §7; history is never fabricated). A detected gap is
// observed and logged; repair goes through the audited events-admin path,
// never an automatic writeback.
const (
	withdrawalIntakeProbeSQL = `
SELECT request_id AS source_id, 0::bigint AS source_version
FROM withdrawal_requests
WHERE created_at >= (SELECT cutover_at FROM event_system_state WHERE id = 1)
ORDER BY created_at DESC, request_id DESC
LIMIT $1`

	withdrawalExecutionProbeSQL = `
SELECT intent_id AS source_id, state_version AS source_version
FROM payment_intents
WHERE admitted_at >= (SELECT cutover_at FROM event_system_state WHERE id = 1)
ORDER BY admitted_at DESC, intent_id DESC
LIMIT $1`
)

// depositObserverProbe adapts the read-only internal/indexer reader to the
// audit probe interface: the 004-owned table stays inside internal/indexer
// (TestDepositWritePathConfinement), exactly like the T031 bootstrap export.
type depositObserverProbe struct{ pool *pgxpool.Pool }

// SourceKind returns the registered deposit source_kind.
func (depositObserverProbe) SourceKind() string { return "deposit_observer" }

// Latest returns the bounded recent observation watermarks. The audit's
// querier is ignored on purpose: this probe reads through the owning package's
// reader and its own pool.
func (p depositObserverProbe) Latest(ctx context.Context, _ events.Querier, limit int) ([]events.SourceWatermark, error) {
	marks, err := indexer.LatestObservationWatermarks(ctx, p.pool, limit)
	if err != nil {
		return nil, err
	}
	out := make([]events.SourceWatermark, 0, len(marks))
	for _, mark := range marks {
		out = append(out, events.SourceWatermark{SourceID: mark.SourceID, SourceVersion: mark.SourceVersion})
	}
	return out, nil
}

// eventPublisherProbes builds the three registered source-domain probes.
func eventPublisherProbes(pool *pgxpool.Pool) ([]events.SourceProbe, error) {
	probes := []events.SourceProbe{depositObserverProbe{pool: pool}}
	for _, spec := range []struct{ kind, query string }{
		{"withdrawal_intake", withdrawalIntakeProbeSQL},
		{"withdrawal_execution", withdrawalExecutionProbeSQL},
	} {
		probe, err := events.NewSQLProbe(spec.kind, spec.query)
		if err != nil {
			return nil, err
		}
		probes = append(probes, probe)
	}
	return probes, nil
}

// eventPublisherAudit wires the reconciliation audit probes for the three 013
// source domains and logs each pass.
type eventPublisherAudit struct {
	audit *events.ReconciliationAudit
	pool  *pgxpool.Pool
	log   *slog.Logger
}

// newEventPublisherAudit builds the audit over the three source domains.
func newEventPublisherAudit(pool *pgxpool.Pool, log *slog.Logger) (*eventPublisherAudit, error) {
	if pool == nil {
		return nil, errors.New("reconciliation audit requires a database pool")
	}
	if log == nil {
		log = slog.Default()
	}
	probes, err := eventPublisherProbes(pool)
	if err != nil {
		return nil, err
	}
	audit, err := events.NewReconciliationAudit(probes, eventPublisherAuditLimit, nil)
	if err != nil {
		return nil, err
	}
	return &eventPublisherAudit{audit: audit, pool: pool, log: log}, nil
}

// Run executes one read-only audit pass and logs the outcome. Gaps are
// detection-only: nothing is written back.
func (a *eventPublisherAudit) Run(ctx context.Context) {
	report, err := a.audit.Run(ctx, a.pool)
	if err != nil {
		a.log.Warn("reconciliation audit failed", "error", logx.Redact(err.Error()))
		return
	}
	for _, probeErr := range report.ProbeErrors {
		a.log.Warn("reconciliation probe failed", "error", logx.Redact(probeErr.Error()))
	}
	if len(report.Gaps) > 0 {
		a.log.Error("reconciliation gaps detected (detection only; repair is audited)",
			"checked", report.Checked, "gaps", len(report.Gaps), "unverifiable", report.Unverifiable)
		return
	}
	a.log.Debug("reconciliation audit clean", "checked", report.Checked, "unverifiable", report.Unverifiable)
}

// wantsHelp reports whether args request help (no other arguments allowed).
func wantsHelp(args []string) bool {
	return len(args) == 1 && (args[0] == "-h" || args[0] == "--help" || args[0] == "help")
}

// eventPublisherUsage documents the runtime surface.
func eventPublisherUsage(w io.Writer) {
	fmt.Fprint(w, `usage: txharbor event-publisher

Runs the 013 outbox publisher loop: claim (FOR UPDATE SKIP LOCKED plus a
bounded lease), publish outside any transaction (acks=all, idempotent
producer, bounded in-flight/delivery/request limits), ack/release/block
settlement with bounded backoff, a periodic reconciliation audit and a
graceful shutdown (stop claiming, finish the in-flight batch, release what
was not confirmed). Delivery is at-least-once and processing is idempotent.

Requires TXHARBOR_EVENTS_ENABLED=true and a complete, valid 013 configuration
(config.Load fails closed otherwise).

  --help  show this help
`)
}
