package app

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/logx"
)

// EventConsumer is the 013 `event-consumer` entry point (T044): fail-closed
// configuration, the reference consumer (the acceptance-evidence consumer;
// explicitly non-authoritative, contracts/consumer.md §8) and the franz-go
// group runtime — per-partition sequential processing, the durable PostgreSQL
// progress as the authoritative resume point, Kafka offset commits strictly
// after the effect transaction and bounded lag observation.
//
// Guarantee statement (global, MUST NOT be weakened): delivery is
// at-least-once and processing is idempotent; the same event is effectively
// applied exactly once inside the consumer's PostgreSQL effects. This command
// never claims cross-system exactly-once and never treats a consumed event as
// an authorization, a send permission or a reconciliation verdict.
//
// Exit codes: 0 clean stop, 1 configuration/feature gate/database/broker
// refusal, 2 usage error.
func EventConsumer(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	if wantsHelp(args) {
		eventConsumerUsage(stdout)
		return 0
	}
	if len(args) > 0 {
		fmt.Fprintf(stderr, "txharbor event-consumer: unexpected argument %q\n", args[0])
		eventConsumerUsage(stderr)
		return 2
	}

	cfg, err := config.Load(d.getenv())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor event-consumer: configuration error: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if !cfg.Events.Enabled {
		fmt.Fprintf(stderr, "txharbor event-consumer: events are disabled (%s=false); refusing to start\n",
			config.EnvEventsEnabled)
		return 1
	}
	fmt.Fprintf(stdout, "txharbor event-consumer: config %s\n", cfg.Summary())

	pool, err := db.OpenPool(ctx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor event-consumer: open database: %s\n", logx.Redact(err.Error()))
		return 1
	}
	defer pool.Close()

	log := slog.Default()
	reference, err := events.NewReferenceConsumer(pool, events.ConsumerOptions{
		GapWait:     cfg.Events.Consumer.GapWait,
		BackoffBase: cfg.Events.Consumer.BackoffBase,
		BackoffMax:  cfg.Events.Consumer.BackoffMax,
		RetryLimit:  cfg.Events.Consumer.RetryLimit,
		ChainID:     int64(cfg.ChainID),
		Observer:    eventConsumerLogObserver{log: log},
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor event-consumer: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if err := reference.EnsureLedgerSchema(ctx); err != nil {
		fmt.Fprintf(stderr, "txharbor event-consumer: reference ledger schema: %s\n", logx.Redact(err.Error()))
		return 1
	}

	commitInterval := cfg.Events.Consumer.PollInterval
	if commitInterval < 200*time.Millisecond {
		// The configured poll cadence is the commit cadence upper bound; a
		// very small cadence would hammer the group coordinator, so it is
		// floored at the documented initial bound (calibrate after
		// measurement).
		commitInterval = 200 * time.Millisecond
	}
	runtime, err := events.NewKafkaConsumer(events.KafkaConsumerConfig{
		Brokers:        cfg.Kafka.Brokers,
		Topic:          cfg.Kafka.Topic,
		GroupPrefix:    cfg.Kafka.ConsumerGroupPrefix,
		ConsumerName:   events.RefConsumerName,
		PollBatch:      cfg.Events.Consumer.Batch,
		CommitInterval: commitInterval,
	}, reference.Consumer)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor event-consumer: %s\n", logx.Redact(err.Error()))
		return 1
	}
	defer runtime.Client.Close()

	fmt.Fprintf(stdout, "txharbor event-consumer: consumer=%s group=%s topic=%s batch=%d gap_wait=%s\n",
		events.RefConsumerName, runtime.GroupID, cfg.Kafka.Topic, cfg.Events.Consumer.Batch, cfg.Events.Consumer.GapWait)
	fmt.Fprintln(stdout, "txharbor event-consumer: delivery is at-least-once and processing is idempotent;")
	fmt.Fprintf(stdout, "txharbor event-consumer: boundary: %s\n", reference.BoundaryStatement())

	if err := runtime.Run(ctx); err != nil && ctx.Err() == nil {
		fmt.Fprintf(stderr, "txharbor event-consumer: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintln(stdout, "txharbor event-consumer: stopped")
	return 0
}

// eventConsumerLogObserver turns consumer outcomes into structured log lines.
// Applied/duplicate outcomes are Info, retries Debug (an outage repeats them),
// quarantines Error (they must alert), lag observations Debug. The metric
// sink is wired by later batches; the durable audit trail lives in
// consumer_quarantine/consumer_progress regardless.
type eventConsumerLogObserver struct{ log *slog.Logger }

// ObserveConsumerApplied logs one exactly-once effect application.
func (o eventConsumerLogObserver) ObserveConsumerApplied() {
	o.log.Info("consumer applied event")
}

// ObserveConsumerRetry logs one bounded retry.
func (o eventConsumerLogObserver) ObserveConsumerRetry(failureClass string) {
	o.log.Debug("consumer retry", "failure_class", failureClass)
}

// ObserveConsumerQuarantine logs one quarantine decision (must alert).
func (o eventConsumerLogObserver) ObserveConsumerQuarantine(failureClass string) {
	o.log.Error("consumer quarantined event", "failure_class", failureClass)
}

// SetConsumerLag logs one lag observation.
func (o eventConsumerLogObserver) SetConsumerLag(consumerName string, partition int, lagSeconds float64, lagMessages int64) {
	o.log.Debug("consumer lag", "consumer", consumerName, "partition", partition,
		"lag_messages", lagMessages, "watermark_age_seconds", lagSeconds)
}

// ObserveEventReplay logs one audited manual replay (distinct from the
// automatic retries above; contracts/consumer.md §6).
func (o eventConsumerLogObserver) ObserveEventReplay(opKind, consumerName string) {
	o.log.Info("event replay operation", "op_kind", opKind, "consumer", consumerName)
}

// SetConsumerReplayClock logs the audited replay watermark.
func (o eventConsumerLogObserver) SetConsumerReplayClock(consumerName string, unixSeconds float64) {
	o.log.Debug("consumer replay clock", "consumer", consumerName, "unix_seconds", unixSeconds)
}

// eventConsumerUsage documents the runtime surface.
func eventConsumerUsage(w io.Writer) {
	fmt.Fprint(w, `usage: txharbor event-consumer

Runs the 013 reference consumer: a franz-go consumer group over the event
topic, per-partition sequential processing through the idempotent T4
transaction (persistent inbox dedup, version guard with a bounded gap wait,
the simulated-ledger effect, durable PostgreSQL progress), bounded retries,
persistent quarantine with audited replay and bounded lag observation.

Delivery is at-least-once and processing is idempotent; the consumer never
claims cross-system exactly-once. The reference consumer is not a production
ledger and is not authoritative.

Requires TXHARBOR_EVENTS_ENABLED=true and a complete, valid 013 configuration
(config.Load fails closed otherwise).

  --help  show this help
`)
}
