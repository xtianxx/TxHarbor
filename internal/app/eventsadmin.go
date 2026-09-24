package app

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/indexer"
	"github.com/xtianxx/txharbor/internal/logx"
)

// EventsAdmin is the 013 `events-admin` operator entry point. T031 implements
// the cutover path: the read-only bootstrap-export snapshot, the cutover
// readiness report and the read-only rollback inventory. The replay /
// unblock / retention-prune actions stay T045 and refuse explicitly.
//
// Every implemented action is read-only: it never writes a business table,
// never emits an event and never fabricates history (data-model §7; R14).
// The rollback procedure is a controlled full-feature rollback — stop the
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
	case "replay", "unblock", "retention-prune":
		fmt.Fprintf(stderr, "txharbor events-admin: %s is not implemented in this batch (T045)\n", args[0])
		return 1
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

  replay / unblock / retention-prune   T045 (not implemented in this batch)

Requires TXHARBOR_EVENTS_ENABLED=true and a complete, valid 013 configuration.

  --help  show this help
`)
}
