package app

import (
	"context"
	"fmt"
	"io"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/logx"
)

// EventConsumer is the 013 `event-consumer` entry point (T007 skeleton):
// argument parsing, usage, fail-closed configuration loading and an empty
// runtime loop. The idempotent consumer runtime is wired by T041/T044; this
// batch must not pretend to consume anything.
//
// Fail-closed: config.Load refuses an incomplete 013 configuration and the
// command refuses to run while TXHARBOR_EVENTS_ENABLED is not true. Exit
// codes: 0 clean stop, 1 configuration/feature gate refusal, 2 usage error.
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
	fmt.Fprintln(stdout, "txharbor event-consumer: skeleton ready; consumer runtime lands with T041/T044")

	return runEmptyLoop(ctx, cfg.Events.Consumer.PollInterval)
}

// eventConsumerUsage documents the skeleton surface.
func eventConsumerUsage(w io.Writer) {
	fmt.Fprint(w, `usage: txharbor event-consumer

Runs the 013 reference consumer loop (skeleton in this batch; idempotent
consumer runtime lands with T041/T044). Requires TXHARBOR_EVENTS_ENABLED=true
and a complete, valid 013 configuration (config.Load fails closed otherwise).

  --help  show this help
`)
}
