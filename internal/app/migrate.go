package app

import (
	"context"
	"fmt"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
)

// Migrate runs the independent migration command: `migrate up|status`
// (contracts/cli.md). Serve never migrates automatically.
func Migrate(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()

	cfg, err := config.Load(d.getenv())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor migrate: configuration error: %s\n", logx.Redact(err.Error()))
		return 1
	}

	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: txharbor migrate up|status")
		return 2
	}

	opts := db.MigrateOptions{
		DSN:            cfg.PGDSN,
		LockTimeout:    cfg.MigrateLockTimeout,
		ConnectTimeout: cfg.ProbeTimeout,
	}

	switch args[0] {
	case "up":
		if err := db.MigrateUp(ctx, opts, stdout); err != nil {
			fmt.Fprintf(stderr, "txharbor migrate: %s\n", logx.Redact(err.Error()))
			return 1
		}
		return 0
	case "status":
		if err := db.MigrateStatus(ctx, opts, stdout); err != nil {
			fmt.Fprintf(stderr, "txharbor migrate: %s\n", logx.Redact(err.Error()))
			return 1
		}
		return 0
	default:
		fmt.Fprintf(stderr, "txharbor migrate: unknown action %q (want up|status)\n", args[0])
		return 2
	}
}
