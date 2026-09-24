package app

import (
	"context"
	"fmt"
	"io"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/logx"
)

// EventsAdmin is the 013 `events-admin` operator entry point (T007 skeleton):
// argument parsing, usage and fail-closed configuration loading. The operator
// actions (bootstrap-export/cutover with T031; replay/unblock/retention-prune
// with T045) are NOT implemented in this batch: every action refuses with a
// clear message instead of pretending to have done anything.
//
// Exit codes: 0 help, 1 configuration/feature gate refusal or unimplemented
// action, 2 usage error.
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
	case "bootstrap-export", "cutover":
		fmt.Fprintf(stderr, "txharbor events-admin: %s is not implemented in this batch (T031)\n", args[0])
		return 1
	case "replay", "unblock", "retention-prune":
		fmt.Fprintf(stderr, "txharbor events-admin: %s is not implemented in this batch (T045)\n", args[0])
		return 1
	default:
		fmt.Fprintf(stderr, "txharbor events-admin: unknown action %q\n", args[0])
		eventsAdminUsage(stderr)
		return 2
	}
}

// eventsAdminUsage documents the operator surface (actions land in T031/T045).
func eventsAdminUsage(w io.Writer) {
	fmt.Fprint(w, `usage: txharbor events-admin <action> [flags]

013 operator actions (not implemented in this batch; the command refuses
rather than pretending):

  bootstrap-export   read-only snapshot export for downstream initialisation (T031)
  cutover            cutover/maintenance path (T031)
  replay             audited manual replay through inbox/version guards (T045)
  unblock            audited unblock of permanently blocked outbox rows (T045)
  retention-prune    retention pruning of published rows, audited (T045)

Requires TXHARBOR_EVENTS_ENABLED=true and a complete, valid 013 configuration.

  --help  show this help
`)
}
