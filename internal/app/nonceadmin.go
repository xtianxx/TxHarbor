package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/nonce"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// NonceAdmin runs the 008 operator carrier (T015):
// `txharbor nonce-admin <mint|hold-release|binding-release|register|disable|status> [flags]`,
// the operator entry point for the nonce.AdminRunner transaction library
// (contracts/observation.md §3/§5).
//
// Carrier shape mirrors WithdrawalAuthz: flag parsing, then config.Load for
// the DSN (full serve-env validation, run with the serve env file), then an
// operator-connection pool, then the repository-owned transaction. The
// operator identity is the DB operator running this binary with DSN access
// (same trust as migrate/withdrawal-authz); --operator is recorded verbatim as
// the declared identity — an audit claim, not a cryptographic proof.
//
// Actions: `mint` prints one opaque operation id and is pure entropy — no
// configuration, no connection, nothing persisted (R9: the caller MUST durably
// capture the id first). Every mutating action REQUIRES --operation-id (no
// auto-mint, no echo-fallback) and binds --chain-id to this deployment's chain
// plus --sender to the scope before any connection opens. `status` is the
// read-only query surface of observation.md §5 (active holds, causes, evidence,
// scope state) and writes nothing.
//
// Exit codes mirror WithdrawalAuthz: 0 the attempt committed (applied/nop, or
// an equal retry converged on the recorded outcome), 1 refused (including
// operation_conflict) or failed, 2 flag usage errors — including a missing
// --operation-id, detected before any configuration or database access.
func NonceAdmin(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	if len(args) == 0 {
		nonceAdminUsage(stderr)
		return 2
	}
	switch args[0] {
	case "mint":
		return nonceAdminMint(stdout, stderr, args[1:])
	case "hold-release":
		return nonceAdminRelease(ctx, "hold-release", args[1:], d)
	case "binding-release":
		return nonceAdminRelease(ctx, "binding-release", args[1:], d)
	case "register":
		return nonceAdminRegistry(ctx, nonce.AdminActionRegistryRegister, "register", args[1:], d)
	case "disable":
		return nonceAdminRegistry(ctx, nonce.AdminActionRegistryDisable, "disable", args[1:], d)
	case "status":
		return nonceAdminStatus(ctx, args[1:], d)
	default:
		fmt.Fprintf(stderr, "txharbor nonce-admin: unknown action %q\n", args[0])
		nonceAdminUsage(stderr)
		return 2
	}
}

// nonceAdminUsage documents every action; each usage error routes here.
func nonceAdminUsage(w io.Writer) {
	fmt.Fprint(w, `usage: txharbor nonce-admin mint
       txharbor nonce-admin hold-release --operation-id O --hold-id H --chain-id N --sender 0x… --observation-id OB --evidence F --operator OP --reason R
       txharbor nonce-admin binding-release --operation-id O --binding-id B --chain-id N --sender 0x… --observation-id OB --evidence F --operator OP --reason R
       txharbor nonce-admin register --operation-id O --chain-id N --sender 0x… --operator OP --reason R
       txharbor nonce-admin disable  --operation-id O --chain-id N --sender 0x… --operator OP --reason R
       txharbor nonce-admin status   --chain-id N [--sender 0x…]

mint prints one opaque operation id; capture it durably before any operator action.
--operation-id is required on every mutating action (no auto-mint, no echo-fallback).
status is read-only: it lists active holds and scope state and writes nothing.
`)
}

// nonceAdminMint prints one fresh opaque operation id (32 lowercase hex). It is
// pure entropy: no config.Load, no pool, nothing persisted. The same mint the
// 007/008 carrier protocol uses (mint-first rule).
func nonceAdminMint(stdout, stderr io.Writer, args []string) int {
	if len(args) > 0 {
		nonceAdminUsage(stderr)
		return 2
	}
	id, err := withdrawal.MintOperationID()
	if err != nil {
		fmt.Fprintf(stderr, "txharbor nonce-admin mint: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintln(stdout, id)
	return 0
}

// nonceAdminRelease runs T-hold-release / T-binding-release: the observer
// samples the chain outside any DB tx (the runner owns that), the runner
// re-verifies inside the locked tx.
func nonceAdminRelease(ctx context.Context, action string, args []string, d Deps) int {
	stderr := d.stderr()

	fs := flag.NewFlagSet("nonce-admin "+action, flag.ContinueOnError)
	fs.SetOutput(stderr)
	operationID := fs.String("operation-id", "", "opaque caller-minted attempt id (required)")
	chainIDRaw := fs.String("chain-id", "", "scope chain id; must equal this deployment's chain")
	sender := fs.String("sender", "", "scope sender (lowercase 0x + 40 hex)")
	observationID := fs.String("observation-id", "", "operator observation reference (in scope)")
	evidence := fs.String("evidence", "", "operator finding text")
	operator := fs.String("operator", "", "declared operator identity for the audit row")
	reason := fs.String("reason", "", "audit reason")
	var holdID, bindingID *string
	if action == "hold-release" {
		holdID = fs.String("hold-id", "", "hold to release")
	} else {
		bindingID = fs.String("binding-id", "", "binding to dispose (non-terminal only)")
	}
	if err := fs.Parse(args); err != nil {
		nonceAdminUsage(stderr)
		return 2
	}
	if fs.NArg() > 0 || *operationID == "" || *chainIDRaw == "" || *sender == "" ||
		(action == "hold-release" && *holdID == "") || (action == "binding-release" && *bindingID == "") {
		nonceAdminUsage(stderr)
		return 2
	}
	chainID, ok := nonceAdminChainID(stderr, *chainIDRaw)
	if !ok {
		nonceAdminUsage(stderr)
		return 2
	}
	if !nonceAdminSender(*sender) {
		fmt.Fprintf(stderr, "txharbor nonce-admin: invalid --sender %q: want lowercase 0x + 40 hex\n", *sender)
		nonceAdminUsage(stderr)
		return 2
	}

	cfg, code := nonceAdminConfig(d, chainID)
	if code != 0 {
		return code
	}
	rpc, err := gethrpc.DialContext(ctx, cfg.RPCURL)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor nonce-admin: %s\n", logx.Redact(err.Error()))
		return 1
	}
	defer rpc.Close()
	pool, err := db.OpenPool(ctx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor nonce-admin: %s\n", logx.Redact(err.Error()))
		return 1
	}
	defer pool.Close()

	req := nonce.AdminRequest{
		Action:        nonce.AdminActionHoldRelease,
		OperationID:   *operationID,
		ChainID:       chainID,
		Sender:        *sender,
		ObservationID: *observationID,
		Evidence:      *evidence,
		Operator:      *operator,
		Reason:        *reason,
	}
	if action == "binding-release" {
		req.Action = nonce.AdminActionBindingRelease
		req.BindingID = *bindingID
	} else {
		req.HoldID = *holdID
	}

	runner := nonce.NewAdminRunner(pool, nonce.NewObserver(rpc, nonce.ObserverConfig{
		RPCTimeout:   cfg.IndexRPCTimeout,
		RetryInitial: cfg.IndexRetryInitial,
		RetryMax:     cfg.IndexRetryMax,
	}))
	res, err := runner.Run(ctx, req)
	return nonceAdminReport(d, *operationID, res, err)
}

// nonceAdminRegistry runs T-registry register/disable. These take no chain
// observation (no RPC), so the runner gets a nil observer.
func nonceAdminRegistry(ctx context.Context, action, name string, args []string, d Deps) int {
	stderr := d.stderr()

	fs := flag.NewFlagSet("nonce-admin "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	operationID := fs.String("operation-id", "", "opaque caller-minted attempt id (required)")
	chainIDRaw := fs.String("chain-id", "", "scope chain id; must equal this deployment's chain")
	sender := fs.String("sender", "", "controlled sender (lowercase 0x + 40 hex)")
	operator := fs.String("operator", "", "declared operator identity for the audit row")
	reason := fs.String("reason", "", "audit reason")
	if err := fs.Parse(args); err != nil {
		nonceAdminUsage(stderr)
		return 2
	}
	if fs.NArg() > 0 || *operationID == "" || *chainIDRaw == "" || *sender == "" {
		nonceAdminUsage(stderr)
		return 2
	}
	chainID, ok := nonceAdminChainID(stderr, *chainIDRaw)
	if !ok {
		nonceAdminUsage(stderr)
		return 2
	}
	if !nonceAdminSender(*sender) {
		fmt.Fprintf(stderr, "txharbor nonce-admin: invalid --sender %q: want lowercase 0x + 40 hex\n", *sender)
		nonceAdminUsage(stderr)
		return 2
	}

	cfg, code := nonceAdminConfig(d, chainID)
	if code != 0 {
		return code
	}
	pool, err := db.OpenPool(ctx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor nonce-admin: %s\n", logx.Redact(err.Error()))
		return 1
	}
	defer pool.Close()

	res, err := nonce.NewAdminRunner(pool, nil).Run(ctx, nonce.AdminRequest{
		Action:      action,
		OperationID: *operationID,
		ChainID:     chainID,
		Sender:      *sender,
		Operator:    *operator,
		Reason:      *reason,
	})
	return nonceAdminReport(d, *operationID, res, err)
}

// nonceAdminStatus reads the operator query surface of observation.md §5:
// active holds with their causes and evidence, plus the scope state row. It is
// SELECT-only and never writes; the nonce package keeps these readers
// unexported (T014's frozen interface), so the carrier reads the same tables
// directly. Deleting a hold is only ever done by hold-release.
func nonceAdminStatus(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()

	fs := flag.NewFlagSet("nonce-admin status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	chainIDRaw := fs.String("chain-id", "", "scope chain id; must equal this deployment's chain")
	senderRaw := fs.String("sender", "", "optional scope filter (lowercase 0x + 40 hex)")
	if err := fs.Parse(args); err != nil {
		nonceAdminUsage(stderr)
		return 2
	}
	if fs.NArg() > 0 || *chainIDRaw == "" {
		nonceAdminUsage(stderr)
		return 2
	}
	chainID, ok := nonceAdminChainID(stderr, *chainIDRaw)
	if !ok {
		nonceAdminUsage(stderr)
		return 2
	}
	sender := ""
	if *senderRaw != "" {
		if !nonceAdminSender(*senderRaw) {
			fmt.Fprintf(stderr, "txharbor nonce-admin: invalid --sender %q: want lowercase 0x + 40 hex\n", *senderRaw)
			nonceAdminUsage(stderr)
			return 2
		}
		sender = *senderRaw
	}

	cfg, code := nonceAdminConfig(d, chainID)
	if code != 0 {
		return code
	}
	pool, err := db.OpenPool(ctx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor nonce-admin: %s\n", logx.Redact(err.Error()))
		return 1
	}
	defer pool.Close()
	return nonceAdminStatusRead(ctx, stdout, stderr, pool, chainID, sender)
}

// nonceAdminStatusRead performs the read-only selects. A missing scope row
// prints scope=none; a scope with no active holds is not held.
func nonceAdminStatusRead(ctx context.Context, stdout, stderr io.Writer, pool *pgxpool.Pool, chainID int64, sender string) int {
	if sender != "" {
		var floor, latest, pending, lastObs *string
		err := pool.QueryRow(ctx, `
SELECT reconciled_floor::text, last_latest::text, last_pending::text, last_observation_id
FROM nonce_scope_state WHERE chain_id = $1 AND sender = $2`, chainID, sender).
			Scan(&floor, &latest, &pending, &lastObs)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			fmt.Fprintf(stdout, "txharbor nonce-admin: status chain_id=%d sender=%s scope=none\n", chainID, sender)
		case err != nil:
			fmt.Fprintf(stderr, "txharbor nonce-admin: status read failed: %s\n", logx.Redact(err.Error()))
			return 1
		default:
			fmt.Fprintf(stdout, "txharbor nonce-admin: status chain_id=%d sender=%s reconciled_floor=%s last_latest=%s last_pending=%s last_observation_id=%s\n",
				chainID, sender, nonceAdminText(floor), nonceAdminText(latest), nonceAdminText(pending), nonceAdminText(lastObs))
		}
	}

	query := `
SELECT hold_id, sender, cause, established_at, evidence_observation_id
FROM nonce_scope_holds WHERE chain_id = $1 AND status = 'active'`
	qargs := []any{chainID}
	if sender != "" {
		query += ` AND sender = $2`
		qargs = append(qargs, sender)
	}
	query += ` ORDER BY sender, established_at, hold_id`
	rows, err := pool.Query(ctx, query, qargs...)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor nonce-admin: status read failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	defer rows.Close()

	holdCount := 0
	for rows.Next() {
		var holdID, holdSender, cause, evidenceObservationID string
		var establishedAt time.Time
		if err := rows.Scan(&holdID, &holdSender, &cause, &establishedAt, &evidenceObservationID); err != nil {
			fmt.Fprintf(stderr, "txharbor nonce-admin: status read failed: %s\n", logx.Redact(err.Error()))
			return 1
		}
		fmt.Fprintf(stdout, "hold hold_id=%s sender=%s cause=%s established_at=%s evidence_observation_id=%s\n",
			holdID, holdSender, cause, establishedAt.UTC().Format(time.RFC3339), evidenceObservationID)
		holdCount++
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(stderr, "txharbor nonce-admin: status read failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout, "txharbor nonce-admin: status ok active_holds=%d\n", holdCount)
	return 0
}

// nonceAdminConfig runs the shared carrier gate: config.Load (full serve-env
// validation, run with the serve env file) plus the deployment chain bind. The
// bind is checked before any connection opens, so a foreign chain touches
// neither RPC nor database.
func nonceAdminConfig(d Deps, claimChainID int64) (*config.Config, int) {
	stderr := d.stderr()
	cfg, err := config.Load(d.getenv())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor nonce-admin: configuration error: %s\n", logx.Redact(err.Error()))
		return nil, 1
	}
	if cfg.ChainID > math.MaxInt64 {
		fmt.Fprintf(stderr, "txharbor nonce-admin: configuration error: %s out of system range\n", config.EnvChainID)
		return nil, 1
	}
	if int64(cfg.ChainID) != claimChainID {
		fmt.Fprintf(stderr, "txharbor nonce-admin: chain_id %d does not match deployment chain %d\n", claimChainID, cfg.ChainID)
		return nil, 1
	}
	return cfg, 0
}

// nonceAdminChainID parses --chain-id as a positive int64 (the nonce schema's
// system range); malformed and non-positive values are usage errors.
func nonceAdminChainID(stderr io.Writer, raw string) (int64, bool) {
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		fmt.Fprintf(stderr, "txharbor nonce-admin: invalid --chain-id %q: not a positive decimal integer\n", raw)
		return 0, false
	}
	return id, true
}

// nonceAdminSender accepts exactly the nonce scope form: lowercase 0x + 40 hex.
func nonceAdminSender(s string) bool {
	return strings.HasPrefix(s, "0x") && s == strings.ToLower(s) && common.IsHexAddress(s)
}

// nonceAdminReport maps one committed/failed attempt to the carrier exit code:
// applied/nop committed (0); refused and operation_conflict are business
// refusals (1) that never rotate the operation id.
func nonceAdminReport(d Deps, operationID string, res nonce.AdminResult, err error) int {
	stdout, stderr := d.stdout(), d.stderr()
	if err != nil {
		var e *nonce.Error
		if errors.As(err, &e) && e.Outcome == nonce.OutcomeTemporarilyUnavailable {
			fmt.Fprintf(stderr,
				"txharbor nonce-admin: %s; retry with the same --operation-id %s and the same parameters\n",
				logx.Redact(e.Error()), operationID)
			return 1
		}
		fmt.Fprintf(stderr, "txharbor nonce-admin: %s\n", logx.Redact(err.Error()))
		return 1
	}
	switch res.Outcome {
	case nonce.AdminApplied, nonce.AdminNop:
		fmt.Fprintf(stdout, "txharbor nonce-admin: ok action=%s subject_id=%s outcome=%s\n",
			res.Action, res.SubjectID, res.Outcome)
		return 0
	default:
		fmt.Fprintf(stderr, "txharbor nonce-admin: action=%s subject_id=%s outcome=%s detail=%s\n",
			res.Action, res.SubjectID, res.Outcome, logx.Redact(res.Detail))
		return 1
	}
}

// nonceAdminText renders a nullable numeric/text scan as "-" when absent.
func nonceAdminText(s *string) string {
	if s == nil {
		return "-"
	}
	return *s
}
