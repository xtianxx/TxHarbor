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
//	txharbor nonce-admin      operate 008 nonce holds/registry (mint/release/status)
//	txharbor withdrawal-exec  operate 011 execution permissions/claims/projection
//	txharbor withdrawal-worker run the 011 execution worker
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/jointwire"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	d := app.Deps{Getenv: os.LookupEnv, Stdout: os.Stdout, Stderr: os.Stderr, JointWiring: jointwire.Worker}
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
	case "signer-serve":
		return app.SignerServe(ctx, args[1:], d)
	case "signer-auth":
		return app.SignerAuth(ctx, args[1:], d)
	case "nonce-admin":
		return app.NonceAdmin(ctx, args[1:], d)
	case "withdrawal-exec":
		return app.WithdrawalExec(ctx, args[1:], d)
	case "withdrawal-worker":
		return app.WithdrawalWorkerCommand(ctx, args[1:], d)
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
  signer-serve      run the signer HTTP service (009, standalone listener)
  signer-auth       manage signer credentials (issue/rotate/revoke/set-can-sign)
  nonce-admin       operate 008 nonce holds/registry (mint/release/status)
  withdrawal-exec   operate 011 execution permissions/claims/projection
  withdrawal-worker run the 011 execution worker (claim/renew/advance/reconcile)
  help              show this help
`)
}
