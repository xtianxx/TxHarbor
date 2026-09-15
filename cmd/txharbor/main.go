// Command txharbor is the single binary for the project foundation.
//
// Subcommands:
//
//	txharbor serve            start the readiness/liveness HTTP service
//	txharbor migrate up       apply pending database migrations
//	txharbor migrate status   show current/pending migration versions
//	txharbor confirm-auth     run one guarded confirmation-policy switch
//	txharbor withdrawal-authz supply an upstream grant (or mint/revoke)
//	txharbor apikey-auth      manage caller API keys (issue/rotate/revoke)
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/xtianxx/txharbor/internal/app"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	d := app.Deps{Getenv: os.LookupEnv, Stdout: os.Stdout, Stderr: os.Stderr}
	if len(args) == 0 {
		usage(os.Stderr)
		return 2
	}

	switch args[0] {
	case "serve":
		return app.Serve(ctx, d)
	case "migrate":
		return app.Migrate(ctx, args[1:], d)
	case "confirm-auth":
		return app.ConfirmAuth(ctx, args[1:], d)
	case "withdrawal-authz":
		return app.WithdrawalAuthz(ctx, args[1:], d)
	case "apikey-auth":
		return app.APIKeyAuth(ctx, args[1:], d)
	case "help", "-h", "--help":
		usage(os.Stdout)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "txharbor: unknown command %q\n\n", args[0])
		usage(os.Stderr)
		return 2
	}
}

func usage(w io.Writer) {
	fmt.Fprint(w, `usage: txharbor <command> [args]

commands:
  serve             start the readiness/liveness HTTP service
  migrate up        apply pending database migrations
  migrate status    show applied/pending migration versions
  confirm-auth      run one guarded confirmation-policy switch
  withdrawal-authz  supply an upstream grant (or mint/revoke)
  apikey-auth       manage caller API keys (issue/rotate/revoke)
  help              show this help
`)
}
