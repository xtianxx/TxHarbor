package app

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/logx"
)

// EventPublisher is the 013 `event-publisher` entry point (T007 skeleton):
// argument parsing, usage, fail-closed configuration loading and an empty
// runtime loop. The claim/publish runtime is wired by T033/T034; this batch
// must not pretend to publish anything.
//
// Fail-closed: config.Load refuses an incomplete 013 configuration and the
// command refuses to run while TXHARBOR_EVENTS_ENABLED is not true. Exit
// codes: 0 clean stop, 1 configuration/feature gate refusal, 2 usage error.
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
	fmt.Fprintln(stdout, "txharbor event-publisher: skeleton ready; outbox publishing lands with T033/T034")

	// Empty loop entry: no runtime work in this batch, but the process shape
	// (poll cadence, graceful stop on signal) is the one T034 fills in.
	return runEmptyLoop(ctx, cfg.Events.Publisher.PollInterval)
}

// runEmptyLoop blocks until ctx is cancelled, waking on the configured poll
// cadence. It performs no work and never reports progress it did not make.
func runEmptyLoop(ctx context.Context, poll time.Duration) int {
	for {
		select {
		case <-ctx.Done():
			return 0
		case <-time.After(poll):
		}
	}
}

// eventPublisherUsage documents the skeleton surface.
func eventPublisherUsage(w io.Writer) {
	fmt.Fprint(w, `usage: txharbor event-publisher

Runs the 013 outbox publisher loop (skeleton in this batch; claim/publish
runtime lands with T033/T034). Requires TXHARBOR_EVENTS_ENABLED=true and a
complete, valid 013 configuration (config.Load fails closed otherwise).

  --help  show this help
`)
}

// wantsHelp reports whether args request help (no other arguments allowed).
func wantsHelp(args []string) bool {
	return len(args) == 1 && (args[0] == "-h" || args[0] == "--help" || args[0] == "help")
}
