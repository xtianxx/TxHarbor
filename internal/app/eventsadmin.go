package app

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/indexer"
	"github.com/xtianxx/txharbor/internal/logx"
)

// EventsAdmin is the 013 `events-admin` operator entry point. T031 implements
// the cutover path: the read-only bootstrap-export snapshot, the cutover
// readiness report and the read-only rollback inventory. T045 adds the
// audited operator actions: replay (re-delivery/re-processing of historical
// events through the wired consumer's persistent idempotency and version
// guard), unblock (blocked -> pending) and retention-prune (published rows
// only). Every action records an event_ops_audit row with the operator, the
// scope, the reason and the result; the operation id is derived
// deterministically so a retried command converges on the stored result
// (PD-4; contracts/consumer.md §6).
//
// Replay never re-executes a chain payment and never creates a withdrawal
// intent; it never bypasses the persistent inbox, the version guard or any
// existing gate. The read-only actions never write a business table, never
// emit an event and never fabricate history (data-model §7; R14). The
// rollback procedure is a controlled full-feature rollback — stop the
// publisher/consumer, inventory or drain pending|blocked, run the down
// migration in a stopped-write window, remove the wiring. Stopping emission
// while business keeps running is NOT a compliant rollback (it violates
// FR-07) and is not offered by this command.
//
// Exit codes: 0 success/help, 1 configuration/feature-gate/database refusal or
// unimplemented action, 2 usage error.
func EventsAdmin(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	if len(args) == 0 {
		eventsAdminUsage(stderr)
		return 2
	}
	if wantsHelp(args) {
		eventsAdminUsage(stdout)
		return 0
	}

	cfg, err := config.Load(d.getenv())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor events-admin: configuration error: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if !cfg.Events.Enabled {
		fmt.Fprintf(stderr, "txharbor events-admin: events are disabled (%s=false); refusing to run\n",
			config.EnvEventsEnabled)
		return 1
	}

	switch args[0] {
	case "bootstrap-export":
		return eventsAdminBootstrapExport(ctx, args[1:], cfg, d)
	case "cutover":
		return eventsAdminCutover(ctx, args[1:], cfg, d)
	case "rollback-inventory":
		return eventsAdminRollbackInventory(ctx, args[1:], cfg, d)
	case "replay":
		return eventsAdminReplay(ctx, args[1:], cfg, d)
	case "unblock":
		return eventsAdminUnblock(ctx, args[1:], cfg, d)
	case "retention-prune":
		return eventsAdminRetentionPrune(ctx, args[1:], cfg, d)
	default:
		fmt.Fprintf(stderr, "txharbor events-admin: unknown action %q\n", args[0])
		eventsAdminUsage(stderr)
		return 2
	}
}

// openEventsAdminPool is the shared fail-closed database entry of every
// events-admin action.
func openEventsAdminPool(ctx context.Context, cfg *config.Config, stderr io.Writer) (*pgxpool.Pool, bool) {
	pool, err := db.OpenPool(ctx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor events-admin: open database: %s\n", logx.Redact(err.Error()))
		return nil, false
	}
	return pool, true
}

// eventsAdminBootstrapExport writes a read-only JSONL snapshot of the
// cutover-relevant business state so downstream consumers can initialise their
// historical baseline before consuming incremental events (quickstart Q0;
// data-model §7). It performs SELECTs only: business tables are untouched and
// no historical event is fabricated.
func eventsAdminBootstrapExport(ctx context.Context, args []string, cfg *config.Config, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("events-admin bootstrap-export", flag.ContinueOnError)
	fs.SetOutput(stderr)
	outPath := fs.String("out", "", "output file for the JSONL snapshot (required; use - for stdout)")
	if err := fs.Parse(args); err != nil {
		eventsAdminUsage(stderr)
		return 2
	}
	if fs.NArg() > 0 || *outPath == "" {
		eventsAdminUsage(stderr)
		return 2
	}

	pool, ok := openEventsAdminPool(ctx, cfg, stderr)
	if !ok {
		return 1
	}
	defer pool.Close()

	out, closeOut, code := eventsAdminOpenOutput(*outPath, stdout, stderr)
	if code != 0 {
		return code
	}
	defer closeOut()

	// The snapshot stream is written in a fixed order; each line is one
	// independent JSON object, so a consumer can replay it sequentially.
	written := 0
	writeLine := func(v any) error {
		body, err := json.Marshal(v)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "%s\n", body); err != nil {
			return err
		}
		written++
		return nil
	}

	if err := eventsAdminExportSystemState(ctx, pool, writeLine); err != nil {
		fmt.Fprintf(stderr, "txharbor events-admin: bootstrap-export: %s\n", logx.Redact(err.Error()))
		return 1
	}
	// Deposit observations are streamed through the 004-owned read-only
	// reader: the table stays inside internal/indexer's write-path
	// confinement boundary (the SELECT never crosses packages).
	if err := indexer.StreamObservationSnapshots(ctx, pool, func(s indexer.ObservationSnapshot) error {
		return writeLine(map[string]any{
			"snapshot":        "deposit_observation",
			"aggregate_type":  "deposit_observation",
			"aggregate_id":    fmt.Sprintf("%d/%s/%s/%d", s.ChainID, s.BlockHash, s.TxHash, s.LogIndex),
			"status":          s.Status,
			"chain_id":        s.ChainID,
			"block_number":    s.BlockNumber,
			"block_hash":      s.BlockHash,
			"tx_hash":         s.TxHash,
			"log_index":       s.LogIndex,
			"version_seq":     s.VersionSeq,
			"amount":          s.Amount,
			"historical_only": true,
			"observed_at":     s.ObservedAt.UTC().Format(time.RFC3339Nano),
		})
	}); err != nil {
		fmt.Fprintf(stderr, "txharbor events-admin: bootstrap-export: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if err := eventsAdminExportRows(ctx, pool, writeLine,
		`SELECT request_id, caller_id, chain_id, asset, recipient, amount::text, status, created_at
		 FROM withdrawal_requests
		 ORDER BY request_id`,
		func(rows pgx.Rows) (any, error) {
			var (
				requestID, asset, recipient, amount, status string
				callerID, chainID                           int64
				createdAt                                   time.Time
			)
			if err := rows.Scan(&requestID, &callerID, &chainID, &asset, &recipient,
				&amount, &status, &createdAt); err != nil {
				return nil, err
			}
			return map[string]any{
				"snapshot":        "withdrawal_request",
				"aggregate_type":  "withdrawal_request",
				"aggregate_id":    requestID,
				"state":           status,
				"caller":          callerID,
				"chain_id":        chainID,
				"asset":           asset,
				"recipient":       recipient,
				"amount":          amount,
				"historical_only": true,
				"created_at":      createdAt.UTC().Format(time.RFC3339Nano),
			}, nil
		}); err != nil {
		fmt.Fprintf(stderr, "txharbor events-admin: bootstrap-export: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if err := eventsAdminExportRows(ctx, pool, writeLine,
		`SELECT intent_id, request_id, chain_id, state, state_version, admitted_at
		 FROM payment_intents
		 ORDER BY intent_id`,
		func(rows pgx.Rows) (any, error) {
			var (
				intentID, requestID, state string
				chainID, stateVersion      int64
				admittedAt                 time.Time
			)
			if err := rows.Scan(&intentID, &requestID, &chainID, &state, &stateVersion, &admittedAt); err != nil {
				return nil, err
			}
			return map[string]any{
				"snapshot":        "withdrawal_intent",
				"aggregate_type":  "withdrawal_intent",
				"aggregate_id":    intentID,
				"request_id":      requestID,
				"state":           state,
				"state_version":   stateVersion,
				"chain_id":        chainID,
				"historical_only": true,
				"admitted_at":     admittedAt.UTC().Format(time.RFC3339Nano),
			}, nil
		}); err != nil {
		fmt.Fprintf(stderr, "txharbor events-admin: bootstrap-export: %s\n", logx.Redact(err.Error()))
		return 1
	}

	if *outPath != "-" {
		fmt.Fprintf(stdout, "txharbor events-admin: bootstrap-export wrote %d snapshot lines to %s\n", written, *outPath)
	}
	return 0
}

// eventsAdminExportSystemState writes the single event_system_state row (the
// cutover marker) as the first snapshot line. A missing row is a fail-closed
// refusal: the migration seeds it and emission without it has no baseline.
func eventsAdminExportSystemState(ctx context.Context, pool *pgxpool.Pool, writeLine func(any) error) error {
	var (
		cutoverAt time.Time
		catalog   int
	)
	err := pool.QueryRow(ctx, `SELECT cutover_at, catalog_version FROM event_system_state WHERE id = 1`).
		Scan(&cutoverAt, &catalog)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("event_system_state row is missing (migration 000015 not applied?)")
	}
	if err != nil {
		return fmt.Errorf("read event_system_state: %w", err)
	}
	return writeLine(map[string]any{
		"snapshot":        "event_system_state",
		"cutover_at":      cutoverAt.UTC().Format(time.RFC3339Nano),
		"catalog_version": catalog,
	})
}

// eventsAdminExportRows streams one SELECT result set through project.
func eventsAdminExportRows(ctx context.Context, pool *pgxpool.Pool, writeLine func(any) error, sql string, project func(pgx.Rows) (any, error)) error {
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return fmt.Errorf("query snapshot rows: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		line, err := project(rows)
		if err != nil {
			return fmt.Errorf("scan snapshot row: %w", err)
		}
		if err := writeLine(line); err != nil {
			return fmt.Errorf("write snapshot line: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate snapshot rows: %w", err)
	}
	return nil
}

// eventsAdminCutover reports the cutover readiness: the migration-seeded
// cutover_at and catalog_version must exist before code rollout starts
// emitting catalog transitions. It is read-only; emission begins when the
// producers are deployed, and consumers baseline on first-seen versions
// (contracts/events.md §7).
func eventsAdminCutover(ctx context.Context, args []string, cfg *config.Config, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	if len(args) > 0 {
		eventsAdminUsage(stderr)
		return 2
	}
	pool, ok := openEventsAdminPool(ctx, cfg, stderr)
	if !ok {
		return 1
	}
	defer pool.Close()

	var (
		cutoverAt time.Time
		catalog   int
		updatedAt time.Time
	)
	err := pool.QueryRow(ctx, `SELECT cutover_at, catalog_version, updated_at FROM event_system_state WHERE id = 1`).
		Scan(&cutoverAt, &catalog, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		fmt.Fprintln(stderr, "txharbor events-admin: cutover: event_system_state row is missing; refuse to declare cutover")
		return 1
	}
	if err != nil {
		fmt.Fprintf(stderr, "txharbor events-admin: cutover: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout, "txharbor events-admin: cutover cutover_at=%s catalog_version=%d updated_at=%s\n",
		cutoverAt.UTC().Format(time.RFC3339Nano), catalog, updatedAt.UTC().Format(time.RFC3339Nano))
	fmt.Fprintln(stdout, "txharbor events-admin: cutover baseline is migration-seeded; catalog transitions begin on rollout and consumers baseline on first-seen versions")
	return 0
}

// eventsAdminRollbackInventory is the read-only inventory step of the
// controlled rollback: it lists pending|blocked outbox rows (optionally
// exporting them to JSONL) so an operator can drain or account for every
// un-published event BEFORE the down migration drops the outbox. The
// rollback record is: stop publisher/consumer → inventory or drain → execute
// down in a stopped-write window → remove the wiring; stopping emission while
// business continues is a violation, not a rollback.
func eventsAdminRollbackInventory(ctx context.Context, args []string, cfg *config.Config, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("events-admin rollback-inventory", flag.ContinueOnError)
	fs.SetOutput(stderr)
	outPath := fs.String("out", "", "optional output file for the pending|blocked JSONL inventory")
	if err := fs.Parse(args); err != nil {
		eventsAdminUsage(stderr)
		return 2
	}
	if fs.NArg() > 0 {
		eventsAdminUsage(stderr)
		return 2
	}

	pool, ok := openEventsAdminPool(ctx, cfg, stderr)
	if !ok {
		return 1
	}
	defer pool.Close()

	var pending, blocked int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE publish_state = 'pending'),
	       count(*) FILTER (WHERE publish_state = 'blocked')
	  FROM outbox_events`).Scan(&pending, &blocked); err != nil {
		fmt.Fprintf(stderr, "txharbor events-admin: rollback-inventory: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout, "txharbor events-admin: rollback-inventory pending=%d blocked=%d\n", pending, blocked)

	if *outPath != "" {
		out, closeOut, code := eventsAdminOpenOutput(*outPath, stdout, stderr)
		if code != 0 {
			return code
		}
		defer closeOut()
		written := 0
		if err := eventsAdminExportRows(ctx, pool, func(v any) error {
			body, err := json.Marshal(v)
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintf(out, "%s\n", body); err != nil {
				return err
			}
			written++
			return nil
		},
			`SELECT id, event_id, event_type, schema_version, identity_kind, aggregate_type, aggregate_id,
			        aggregate_version, publish_state, attempt_count, created_at
			 FROM outbox_events
			 WHERE publish_state IN ('pending','blocked')
			 ORDER BY id`,
			func(rows pgx.Rows) (any, error) {
				var (
					id, aggregateVersion, attemptCount int64
					schemaVersion                      int32
					eventID, eventType, identityKind   string
					aggregateType, aggregateID         string
					publishState                       string
					createdAt                          time.Time
				)
				if err := rows.Scan(&id, &eventID, &eventType, &schemaVersion, &identityKind,
					&aggregateType, &aggregateID, &aggregateVersion, &publishState, &attemptCount, &createdAt); err != nil {
					return nil, err
				}
				return map[string]any{
					"outbox_id":         id,
					"event_id":          eventID,
					"event_type":        eventType,
					"schema_version":    schemaVersion,
					"identity_kind":     identityKind,
					"aggregate_type":    aggregateType,
					"aggregate_id":      aggregateID,
					"aggregate_version": aggregateVersion,
					"publish_state":     publishState,
					"attempt_count":     attemptCount,
					"created_at":        createdAt.UTC().Format(time.RFC3339Nano),
				}, nil
			}); err != nil {
			fmt.Fprintf(stderr, "txharbor events-admin: rollback-inventory: %s\n", logx.Redact(err.Error()))
			return 1
		}
		fmt.Fprintf(stdout, "txharbor events-admin: rollback-inventory wrote %d rows to %s\n", written, *outPath)
	}
	return 0
}

// eventsAdminReplay is the audited manual replay (T045; PD-4;
// contracts/consumer.md §6): the operator names the consumer and the scope,
// the reason and the operator id are recorded in event_ops_audit, and every
// replayed event goes through the wired consumer's T4 transaction — the
// persistent inbox, the version guard and the existing gates decide. A replay
// never re-executes a chain payment and never creates a withdrawal intent.
// The operation id is derived deterministically, so a retried command returns
// the stored result instead of replaying twice.
func eventsAdminReplay(ctx context.Context, args []string, cfg *config.Config, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("events-admin replay", flag.ContinueOnError)
	fs.SetOutput(stderr)
	consumerName := fs.String("consumer", "", "wired consumer name (required)")
	scopeRaw := fs.String("scope", "", "event-ids:<id,...> | aggregate:<type>:<id> | time-range:<from>,<to> (RFC3339) | offset-range:<topic>:<partition>:<from>-<to>")
	reason := fs.String("reason", "", "operator reason recorded in the audit (required)")
	operator := fs.String("operator", "", "authorized operator id (required)")
	operationID := fs.String("operation-id", "", "optional idempotency key; empty derives one from the parameters")
	if err := fs.Parse(args); err != nil {
		eventsAdminUsage(stderr)
		return 2
	}
	if fs.NArg() > 0 || *consumerName == "" || *scopeRaw == "" || *reason == "" || *operator == "" {
		eventsAdminUsage(stderr)
		return 2
	}
	if *consumerName != events.RefConsumerName {
		fmt.Fprintf(stderr, "txharbor events-admin: replay: consumer %q is not wired in this batch (wired: %s)\n",
			*consumerName, events.RefConsumerName)
		return 1
	}
	scope, err := parseReplayScope(*scopeRaw)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor events-admin: replay: scope: %s\n", logx.Redact(err.Error()))
		return 2
	}

	pool, ok := openEventsAdminPool(ctx, cfg, stderr)
	if !ok {
		return 1
	}
	defer pool.Close()

	reference, err := events.NewReferenceConsumer(pool, events.ConsumerOptions{
		GapWait:     cfg.Events.Consumer.GapWait,
		BackoffBase: cfg.Events.Consumer.BackoffBase,
		BackoffMax:  cfg.Events.Consumer.BackoffMax,
		RetryLimit:  cfg.Events.Consumer.RetryLimit,
		ChainID:     int64(cfg.ChainID),
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor events-admin: replay: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if err := reference.EnsureLedgerSchema(ctx); err != nil {
		fmt.Fprintf(stderr, "txharbor events-admin: replay: %s\n", logx.Redact(err.Error()))
		return 1
	}
	result, err := events.Replay(ctx, pool, reference.Consumer, events.ReplayOptions{
		ConsumerName: *consumerName,
		Scope:        scope,
		Reason:       *reason,
		Operator:     *operator,
		OperationID:  *operationID,
		Topic:        cfg.Kafka.Topic,
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor events-admin: replay: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if result.Deduplicated {
		fmt.Fprintf(stdout, "txharbor events-admin: replay operation_id=%s deduplicated=true stored_result=%q\n",
			result.OperationID, result.StoredResult)
		return 0
	}
	fmt.Fprintf(stdout, "txharbor events-admin: replay operation_id=%s operator=%s consumer=%s %s\n",
		result.OperationID, *operator, *consumerName, result.Result())
	fmt.Fprintf(stdout, "txharbor events-admin: replay boundary: %s\n", reference.BoundaryStatement())
	return 0
}

// eventsAdminUnblock returns one permanently blocked outbox row to pending
// under an audited operator decision (blocked -> pending only; PD-4).
func eventsAdminUnblock(ctx context.Context, args []string, cfg *config.Config, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("events-admin unblock", flag.ContinueOnError)
	fs.SetOutput(stderr)
	outboxID := fs.Int64("outbox-id", 0, "outbox_events.id of the blocked row (required)")
	reason := fs.String("reason", "", "operator reason recorded in the audit (required)")
	operator := fs.String("operator", "", "authorized operator id (required)")
	operationID := fs.String("operation-id", "", "optional idempotency key; empty derives one from the parameters")
	if err := fs.Parse(args); err != nil {
		eventsAdminUsage(stderr)
		return 2
	}
	if fs.NArg() > 0 || *outboxID <= 0 || *reason == "" || *operator == "" {
		eventsAdminUsage(stderr)
		return 2
	}

	pool, ok := openEventsAdminPool(ctx, cfg, stderr)
	if !ok {
		return 1
	}
	defer pool.Close()

	result, err := events.Unblock(ctx, pool, events.UnblockOptions{
		OutboxID:    *outboxID,
		Reason:      *reason,
		Operator:    *operator,
		OperationID: *operationID,
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor events-admin: unblock: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if result.Deduplicated {
		fmt.Fprintf(stdout, "txharbor events-admin: unblock operation_id=%s deduplicated=true stored_result=%q\n",
			result.OperationID, result.StoredResult)
		return 0
	}
	fmt.Fprintf(stdout, "txharbor events-admin: unblock operation_id=%s operator=%s outbox_id=%d %s\n",
		result.OperationID, *operator, *outboxID, result.Result())
	if !result.Unblocked {
		fmt.Fprintln(stdout, "txharbor events-admin: unblock: the row is not blocked; zero rows updated (no illegal transition)")
	}
	return 0
}

// eventsAdminRetentionPrune deletes published outbox rows older than the
// retention window under an audited operator decision. pending and blocked
// rows are never touched (data-model §5 T7).
func eventsAdminRetentionPrune(ctx context.Context, args []string, cfg *config.Config, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("events-admin retention-prune", flag.ContinueOnError)
	fs.SetOutput(stderr)
	retention := fs.Duration("retention", 0, "retention window of published rows (required, e.g. 168h)")
	reason := fs.String("reason", "", "operator reason recorded in the audit (required)")
	operator := fs.String("operator", "", "authorized operator id (required)")
	operationID := fs.String("operation-id", "", "optional idempotency key; empty derives one from the parameters")
	if err := fs.Parse(args); err != nil {
		eventsAdminUsage(stderr)
		return 2
	}
	if fs.NArg() > 0 || *retention <= 0 || *reason == "" || *operator == "" {
		eventsAdminUsage(stderr)
		return 2
	}

	pool, ok := openEventsAdminPool(ctx, cfg, stderr)
	if !ok {
		return 1
	}
	defer pool.Close()

	result, err := events.RetentionPrune(ctx, pool, events.PruneOptions{
		Retention:   *retention,
		Reason:      *reason,
		Operator:    *operator,
		OperationID: *operationID,
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor events-admin: retention-prune: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if result.Deduplicated {
		fmt.Fprintf(stdout, "txharbor events-admin: retention-prune operation_id=%s deduplicated=true stored_result=%q\n",
			result.OperationID, result.StoredResult)
		return 0
	}
	fmt.Fprintf(stdout, "txharbor events-admin: retention-prune operation_id=%s operator=%s %s\n",
		result.OperationID, *operator, result.Result())
	return 0
}

// parseReplayScope parses the operator scope grammar fail-closed. Formats:
//
//	event-ids:<uuid>,<uuid>,...
//	aggregate:<aggregate_type>:<aggregate_id>
//	time-range:<from-RFC3339>,<to-RFC3339>
//	offset-range:<topic>:<partition>:<from_offset>-<to_offset>
func parseReplayScope(raw string) (events.ReplayScope, error) {
	kind, rest, found := strings.Cut(raw, ":")
	if !found {
		return events.ReplayScope{}, fmt.Errorf("scope %q misses its kind prefix", raw)
	}
	switch events.ReplayScopeKind(kind) {
	case events.ReplayScopeEventIDs:
		var ids []uuid.UUID
		for _, part := range strings.Split(rest, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			id, err := uuid.Parse(part)
			if err != nil {
				return events.ReplayScope{}, fmt.Errorf("event id %q: %w", part, err)
			}
			ids = append(ids, id)
		}
		return events.ReplayScope{Kind: events.ReplayScopeEventIDs, EventIDs: ids}, nil
	case events.ReplayScopeAggregate:
		aggregateType, aggregateID, found := strings.Cut(rest, ":")
		if !found || aggregateType == "" || aggregateID == "" {
			return events.ReplayScope{}, fmt.Errorf("aggregate scope requires <type>:<id>")
		}
		return events.ReplayScope{Kind: events.ReplayScopeAggregate,
			AggregateType: aggregateType, AggregateID: aggregateID}, nil
	case events.ReplayScopeTimeRange:
		fromRaw, toRaw, found := strings.Cut(rest, ",")
		if !found {
			return events.ReplayScope{}, fmt.Errorf("time-range scope requires <from>,<to>")
		}
		from, err := time.Parse(time.RFC3339, strings.TrimSpace(fromRaw))
		if err != nil {
			return events.ReplayScope{}, fmt.Errorf("time-range from: %w", err)
		}
		to, err := time.Parse(time.RFC3339, strings.TrimSpace(toRaw))
		if err != nil {
			return events.ReplayScope{}, fmt.Errorf("time-range to: %w", err)
		}
		return events.ReplayScope{Kind: events.ReplayScopeTimeRange, From: from, To: to}, nil
	case events.ReplayScopeOffsetRange:
		topic, remainder, found := strings.Cut(rest, ":")
		if !found {
			return events.ReplayScope{}, fmt.Errorf("offset-range scope requires <topic>:<partition>:<from>-<to>")
		}
		partitionRaw, rangeRaw, found := strings.Cut(remainder, ":")
		if !found {
			return events.ReplayScope{}, fmt.Errorf("offset-range scope requires <topic>:<partition>:<from>-<to>")
		}
		partition, err := strconv.Atoi(strings.TrimSpace(partitionRaw))
		if err != nil || partition < 0 {
			return events.ReplayScope{}, fmt.Errorf("offset-range partition %q is invalid", partitionRaw)
		}
		fromRaw, toRaw, found := strings.Cut(rangeRaw, "-")
		if !found {
			return events.ReplayScope{}, fmt.Errorf("offset-range requires <from>-<to>")
		}
		from, err := strconv.ParseInt(strings.TrimSpace(fromRaw), 10, 64)
		if err != nil || from < 0 {
			return events.ReplayScope{}, fmt.Errorf("offset-range from %q is invalid", fromRaw)
		}
		to, err := strconv.ParseInt(strings.TrimSpace(toRaw), 10, 64)
		if err != nil || to < from {
			return events.ReplayScope{}, fmt.Errorf("offset-range to %q is invalid", toRaw)
		}
		return events.ReplayScope{Kind: events.ReplayScopeOffsetRange,
			Topic: topic, Partition: partition, FromOffset: from, ToOffset: to}, nil
	default:
		return events.ReplayScope{}, fmt.Errorf("unknown scope kind %q", kind)
	}
}

// eventsAdminOpenOutput opens the JSONL sink; "-" means stdout. The caller
// owns Close via the returned function.
func eventsAdminOpenOutput(path string, stdout io.Writer, stderr io.Writer) (io.Writer, func(), int) {
	if path == "-" {
		return stdout, func() {}, 0
	}
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor events-admin: open output %s: %s\n", path, logx.Redact(err.Error()))
		return nil, func() {}, 1
	}
	return f, func() { _ = f.Close() }, 0
}

// eventsAdminUsage documents the operator surface.
func eventsAdminUsage(w io.Writer) {
	fmt.Fprint(w, `usage: txharbor events-admin <action> [flags]

013 operator actions:

  bootstrap-export --out PATH|-     read-only cutover snapshot for downstream
                                    initialisation: event_system_state plus the
                                    deposit observation / withdrawal request /
                                    withdrawal intent object snapshots (JSONL).
                                    Writes no business table, emits no event.
  cutover                           report the migration-seeded cutover_at and
                                    catalog_version; refuses when the row is
                                    missing. Read-only.
  rollback-inventory [--out PATH]   read-only inventory of pending|blocked
                                    outbox rows for the controlled rollback
                                    (stop publisher/consumer -> inventory or
                                    drain -> down migration -> remove wiring;
                                    stopping emission while business continues
                                    is a violation, not a rollback).

  replay --consumer NAME --scope SCOPE --reason TEXT --operator ID
                                    [--operation-id ID]
                                    audited re-delivery/re-processing of
                                    historical events through the wired
                                    consumer (persistent inbox, version guard,
                                    existing gates; no new payment action).
                                    SCOPE: event-ids:<uuid,...> |
                                    aggregate:<type>:<id> |
                                    time-range:<from>,<to> (RFC3339) |
                                    offset-range:<topic>:<partition>:<from>-<to>
  unblock --outbox-id ID --reason TEXT --operator ID [--operation-id ID]
                                    audited blocked -> pending return of one
                                    outbox row (never a silent transition).
  retention-prune --retention DURATION --reason TEXT --operator ID
                                    [--operation-id ID]
                                    audited deletion of published rows older
                                    than the retention window; pending|blocked
                                    rows are never touched.

Requires TXHARBOR_EVENTS_ENABLED=true and a complete, valid 013 configuration.

  --help  show this help
`)
}
