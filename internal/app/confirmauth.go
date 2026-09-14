package app

import (
	"context"
	"flag"
	"fmt"
	"math"
	"strconv"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/indexer"
	"github.com/xtianxx/txharbor/internal/logx"
)

// ConfirmAuth runs the privileged confirmation-policy switch carrier:
// `confirm-auth --request-id ID --expected-old-seq S --new-threshold N
// --operator OP --reason R` (T029, the 005 operator entry point for
// indexer.AuthorizeConfirmationPolicy).
//
// Carrier shape (mirrors Migrate): config.Load for the DSN, a pgxpool
// connect, then the guarded transaction. The command only *uses* the DSN
// (plus ProbeTimeout for the connect ping and ChainID as the request
// chain), but config.Load validates the full serve environment exactly like
// migrate does — run it with the serve env file. The operator identity is
// the DB operator running this binary with DSN access (same trust as
// migrate); --operator is recorded verbatim as the declared identity, and
// --request-id is a caller-generated opaque UUID-ish string.
//
// Exit codes mirror Migrate: 0 the switch committed (or the same intent was
// already recorded and is read back), 1 the switch was refused or failed
// (stderr carries the redacted reason), 2 flag usage errors. A bare psql
// INSERT is forbidden: every guard (old-seq check, Q1 parse,
// request_id定性) lives inside AuthorizeConfirmationPolicy, and this is the
// only binary path that executes it.
func ConfirmAuth(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()

	fs := flag.NewFlagSet("confirm-auth", flag.ContinueOnError)
	fs.SetOutput(stderr)
	requestID := fs.String("request-id", "", "opaque caller-generated request identity (required)")
	oldSeqRaw := fs.String("expected-old-seq", "", "policy_seq this switch is based on (required)")
	newThreshold := fs.String("new-threshold", "", "new threshold N in Q1 form: positive decimal integer (required)")
	operator := fs.String("operator", "", "declared operator identity for the audit row (required)")
	reason := fs.String("reason", "", "audit reason for the switch (required)")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintln(stderr, "usage: txharbor confirm-auth --request-id ID --expected-old-seq S --new-threshold N --operator OP --reason R")
		return 2
	}
	if fs.NArg() > 0 || *requestID == "" || *oldSeqRaw == "" || *newThreshold == "" || *operator == "" || *reason == "" {
		fmt.Fprintln(stderr, "usage: txharbor confirm-auth --request-id ID --expected-old-seq S --new-threshold N --operator OP --reason R")
		return 2
	}
	oldSeq, err := strconv.ParseInt(*oldSeqRaw, 10, 64)
	if err != nil {
		fmt.Fprintln(stderr, "usage: txharbor confirm-auth --request-id ID --expected-old-seq S --new-threshold N --operator OP --reason R")
		fmt.Fprintf(stderr, "txharbor confirm-auth: invalid --expected-old-seq %q: not a decimal integer\n", *oldSeqRaw)
		return 2
	}

	cfg, err := config.Load(d.getenv())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor confirm-auth: configuration error: %s\n", logx.Redact(err.Error()))
		return 1
	}
	if cfg.ChainID > math.MaxInt64 {
		fmt.Fprintf(stderr, "txharbor confirm-auth: configuration error: %s out of system range\n", config.EnvChainID)
		return 1
	}

	pool, err := db.OpenPool(ctx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor confirm-auth: %s\n", logx.Redact(err.Error()))
		return 1
	}
	defer pool.Close()

	res, err := indexer.AuthorizeConfirmationPolicy(ctx, pool, indexer.ConfirmAuthRequest{
		ChainID:         int64(cfg.ChainID),
		RequestID:       *requestID,
		ExpectedOldSeq:  oldSeq,
		NewThresholdRaw: *newThreshold,
		Operator:        *operator,
		Reason:          *reason,
	})
	if err != nil {
		fmt.Fprintf(stderr, "txharbor confirm-auth: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout, "txharbor confirm-auth: ok policy_seq=%d threshold=%d recorded=%t\n",
		res.PolicySeq, res.Threshold, res.Recorded)
	return 0
}
